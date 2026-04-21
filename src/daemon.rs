use crate::cache::write_cache_file;
use crate::config::Config;
use crate::diagnostics::{emit_snapshot, summarize_gossip};
use crate::gossip::{GossipMessage, MemberEntry, MemberStatus};
use crate::invite::{add_invite, generate_token, now_ms};
use crate::peers::{load_peers, save_group_state, set_topic_id};
use crate::snapshot::{
    HashMapById, ReplicatedEntry, SnapshotEntry, apply_remote_entries, canonical_path,
    collect_file_send_list, process_local_event, reconcile_startup_state, should_accept_remote,
    snapshot_from_wire,
};
use crate::sync_tree::{TreeNodeInfo, get_nodes, root_hash};
use crate::transport::{FolderProtocol, fetch_entries, send_join_request, sync_peer};
use futures_lite::StreamExt;
use iroh::protocol::Router;
use iroh::{Endpoint, EndpointAddr, PublicKey, SecretKey, Watcher};
use iroh_gossip::{
    ALPN as GOSSIP_ALPN, Gossip, TopicId,
    api::{Event as GossipEvent, GossipSender},
};
use notify::{Config as NotifyConfig, Event, RecommendedWatcher, RecursiveMode, Watcher as _};
use signal_hook::consts::signal::SIGTERM;
use signal_hook::flag;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{RwLock, mpsc, oneshot};

pub(crate) const ALPN: &[u8] = b"tngl/folder/1";
const DAEMON_CACHE_VERSION: u32 = 8;
const MEMBERLIST_INTERVAL: Duration = Duration::from_secs(60);

pub(crate) enum Command {
    LocalFs(Event),
    ApplyRemote {
        entries: Vec<(ReplicatedEntry, Option<PathBuf>)>,
        respond_to: Option<oneshot::Sender<()>>,
    },
    GetLocalNodes {
        path_prefix: String,
        respond_to: oneshot::Sender<Vec<TreeNodeInfo>>,
    },
    CollectFileSendList {
        ids: Vec<String>,
        respond_to: oneshot::Sender<io::Result<Vec<(ReplicatedEntry, Option<PathBuf>)>>>,
    },
    SyncPeer(EndpointAddr),
    AddPeer {
        id: PublicKey,
    },
    GossipReceived(GossipMessage),
    PublishSyncState,
    PublishMemberList,
}

struct State {
    sync_dir: PathBuf,
    cache_path: PathBuf,
    peers_path: PathBuf,
    endpoint: Endpoint,
    local_origin: String,
    topic_id: TopicId,
    peers: Vec<EndpointAddr>,
    allowlist: Arc<RwLock<HashSet<PublicKey>>>,
    gossip_sender: GossipSender,
    members: HashMap<String, MemberEntry>,
    removed_from_group: bool,
    lamport: u64,
    entries: HashMapById,
    suppressed: HashMap<String, SnapshotEntry>,
}

pub fn run(config: Config) -> io::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(io::Error::other)?;
    runtime.block_on(run_async(config))
}

async fn run_async(config: Config) -> io::Result<()> {
    let secret_key = load_or_create_secret_key(&config.key_path)?;
    let endpoint = Endpoint::builder()
        .alpns(vec![ALPN.to_vec(), GOSSIP_ALPN.to_vec()])
        .secret_key(secret_key.clone())
        .bind()
        .await
        .map_err(io::Error::other)?;

    if config.show_id {
        println!("{}", endpoint.id());
        return Ok(());
    }

    let sync_dir = canonical_path(config.sync_dir);
    let cache_path = config.cache_path;

    // Acquire an exclusive OS lock on the state directory. Held for the
    // lifetime of the daemon; released automatically on exit or crash.
    let _lock = acquire_daemon_lock(&config.key_path.parent().unwrap_or(&sync_dir))?;

    print_node_info(&endpoint)?;

    let saved_peers_file = load_peers(&config.peers_path)?;
    let topic_id = ensure_topic_id(&config.peers_path, saved_peers_file.topic_id.as_deref())?;

    let mut peers: Vec<EndpointAddr> = saved_peers_file
        .peer_ids()
        .into_iter()
        .map(EndpointAddr::new)
        .collect();
    let local_origin = endpoint.id().to_string();
    peers.retain(|peer| peer.id != endpoint.id());
    dedupe_peers(&mut peers);

    let allowlist: HashSet<PublicKey> = peers.iter().map(|peer| peer.id).collect();

    let terminated = Arc::new(AtomicBool::new(false));
    flag::register(SIGTERM, Arc::clone(&terminated)).map_err(io::Error::other)?;

    if config.rescan {
        match fs::remove_file(&cache_path) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
    }

    let entries = reconcile_startup_state(&sync_dir, &cache_path, &local_origin, emit_snapshot)?;
    write_cache_file(&cache_path, &entries, DAEMON_CACHE_VERSION)?;

    let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel();

    let watch_tx = cmd_tx.clone();
    let mut watcher = RecommendedWatcher::new(
        move |result| match result {
            Ok(event) => {
                let _ = watch_tx.send(Command::LocalFs(event));
            }
            Err(err) => tracing::warn!(target: "tngl", "filesystem watch error: {err}"),
        },
        NotifyConfig::default(),
    )
    .map_err(io::Error::other)?;
    watcher
        .watch(&sync_dir, RecursiveMode::Recursive)
        .map_err(io::Error::other)?;

    let allowlist = Arc::new(RwLock::new(allowlist));
    let gossip = Gossip::builder().spawn(endpoint.clone());
    let router = Router::builder(endpoint.clone())
        .accept(
            ALPN,
            FolderProtocol {
                tx: cmd_tx.clone(),
                allowlist: Arc::clone(&allowlist),
                invites_path: config.invites_path.clone().into(),
                peers_path: config.peers_path.clone().into(),
                local_id: endpoint.id(),
            },
        )
        .accept(GOSSIP_ALPN, gossip.clone())
        .spawn();

    let bootstrap_peers: Vec<PublicKey> = peers.iter().map(|peer| peer.id).collect();
    let topic = gossip
        .subscribe(topic_id, bootstrap_peers)
        .await
        .map_err(io::Error::other)?;
    let (gossip_sender, mut gossip_receiver) = topic.split();

    let gossip_tx = cmd_tx.clone();
    tokio::spawn(async move {
        while let Some(event) = gossip_receiver.next().await {
            match event {
                Ok(GossipEvent::Received(message)) => {
                    if let Some(parsed) = GossipMessage::from_bytes(&message.content) {
                        let _ = gossip_tx.send(Command::GossipReceived(parsed));
                    }
                }
                Ok(_) => {}
                Err(err) => tracing::warn!(target: "tngl::gossip", "receive error: {err}"),
            }
        }
    });

    let member_tx = cmd_tx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(MEMBERLIST_INTERVAL);
        interval.tick().await;
        loop {
            interval.tick().await;
            let _ = member_tx.send(Command::PublishMemberList);
        }
    });

    let sync_state_tx = cmd_tx.clone();
    let sync_state_interval = Duration::from_secs(config.sync_state_interval_secs.max(1));
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(sync_state_interval);
        interval.tick().await;
        loop {
            interval.tick().await;
            let _ = sync_state_tx.send(Command::PublishSyncState);
        }
    });

    let mut members: HashMap<String, MemberEntry> = saved_peers_file
        .member_entries()
        .into_iter()
        .map(|entry| (entry.id.clone(), entry))
        .collect();
    for peer in &peers {
        members.entry(peer.id.to_string()).or_insert(MemberEntry {
            id: peer.id.to_string(),
            status: MemberStatus::Active,
            lamport: 0,
        });
    }
    members.entry(local_origin.clone()).or_insert(MemberEntry {
        id: local_origin.clone(),
        status: MemberStatus::Active,
        lamport: 0,
    });

    let mut state = State {
        sync_dir,
        cache_path,
        peers_path: config.peers_path.clone(),
        endpoint,
        local_origin,
        topic_id,
        peers,
        allowlist,
        gossip_sender,
        members,
        removed_from_group: false,
        lamport: entries
            .values()
            .map(|entry| entry.lamport)
            .max()
            .unwrap_or(0),
        entries,
        suppressed: HashMap::new(),
    };

    let _ = cmd_tx.send(Command::PublishSyncState);
    let _ = cmd_tx.send(Command::PublishMemberList);

    while !terminated.load(Ordering::Relaxed) {
        let Some(command) = cmd_rx.recv().await else {
            break;
        };
        match command {
            Command::LocalFs(event) => {
                let updates = process_local_event(
                    &state.sync_dir,
                    &mut state.lamport,
                    &mut state.entries,
                    &mut state.suppressed,
                    &state.local_origin,
                    event,
                )?;
                if updates.is_empty() {
                    continue;
                }
                for update in &updates {
                    emit_snapshot(&update.id, &snapshot_from_wire(update));
                    publish_gossip(
                        &state,
                        GossipMessage::FileChanged {
                            origin: state.local_origin.clone(),
                            id: update.id.clone(),
                            hash: update.hash,
                            lamport: update.lamport,
                            changed_at_ms: update.changed_at_ms,
                        },
                    )
                    .await;
                }
                write_cache_file(&state.cache_path, &state.entries, DAEMON_CACHE_VERSION)?;
            }
            Command::ApplyRemote {
                entries,
                respond_to,
            } => {
                let updates = apply_remote_entries(
                    &state.sync_dir,
                    &mut state.lamport,
                    &mut state.entries,
                    &mut state.suppressed,
                    &state.local_origin,
                    entries,
                    emit_snapshot,
                )?;
                if !updates.is_empty() {
                    write_cache_file(&state.cache_path, &state.entries, DAEMON_CACHE_VERSION)?;
                }
                if let Some(reply) = respond_to {
                    let _ = reply.send(());
                }
            }
            Command::GetLocalNodes {
                path_prefix,
                respond_to,
            } => {
                let nodes = get_nodes(&state.entries, &path_prefix);
                let _ = respond_to.send(nodes);
            }
            Command::CollectFileSendList { ids, respond_to } => {
                let result = collect_file_send_list(
                    &state.sync_dir,
                    &mut state.entries,
                    &state.local_origin,
                    &mut state.lamport,
                    &ids,
                );
                let _ = respond_to.send(result);
            }
            Command::AddPeer { id } => {
                state.lamport = state.lamport.saturating_add(1);
                state.members.insert(
                    id.to_string(),
                    MemberEntry {
                        id: id.to_string(),
                        status: MemberStatus::Active,
                        lamport: state.lamport,
                    },
                );
                let peer_addr = EndpointAddr::new(id);
                if !state.peers.iter().any(|peer| peer.id == id) {
                    state.peers.push(peer_addr.clone());
                }
                state.allowlist.write().await.insert(id);
                persist_members(&state.peers_path, state.topic_id, &state.members)?;
                if let Err(err) = state.gossip_sender.join_peers(vec![id]).await {
                    tracing::warn!(target: "tngl::gossip", %id, "join_peers failed: {err}");
                }
                let _ = cmd_tx.send(Command::SyncPeer(peer_addr));
                publish_gossip(
                    &state,
                    build_member_list_msg(&state.local_origin, &state.members),
                )
                .await;
            }
            Command::SyncPeer(peer) => {
                let tx = cmd_tx.clone();
                let endpoint = state.endpoint.clone();
                let sync_dir = state.sync_dir.clone();
                tokio::spawn(async move {
                    let tmp_dir = sync_dir.join(".tngl");
                    if let Err(err) = sync_peer(endpoint, peer, tx, tmp_dir).await {
                        tracing::warn!(target: "tngl", "peer sync error: {err}");
                    }
                });
            }
            Command::GossipReceived(message) => {
                handle_gossip_message(&mut state, &cmd_tx, message).await?;
                if state.removed_from_group {
                    tracing::info!(target: "tngl", "removed from group, leaving topic");
                    break;
                }
            }
            Command::PublishSyncState => {
                publish_gossip(
                    &state,
                    GossipMessage::SyncState {
                        origin: state.local_origin.clone(),
                        root_hash: root_hash(&state.entries),
                        lamport: state.lamport,
                    },
                )
                .await;
            }
            Command::PublishMemberList => {
                let message = build_member_list_msg(&state.local_origin, &state.members);
                publish_gossip(&state, message).await;
            }
        }
    }

    write_cache_file(&state.cache_path, &state.entries, DAEMON_CACHE_VERSION)?;
    router.shutdown().await.map_err(io::Error::other)?;
    gossip.shutdown().await.map_err(io::Error::other)?;
    Ok(())
}

fn build_member_list_msg(
    local_origin: &str,
    members: &HashMap<String, MemberEntry>,
) -> GossipMessage {
    let mut members: Vec<MemberEntry> = members.values().cloned().collect();
    members.sort_by(|left, right| left.id.cmp(&right.id));
    GossipMessage::MemberList {
        origin: local_origin.to_string(),
        members,
    }
}

pub fn run_invite(config: Config) -> io::Result<()> {
    let secret_key = load_or_create_secret_key(&config.key_path)?;
    let node_id = secret_key.public();
    let token = generate_token()?;
    let expires_at = now_ms()? + config.invite_expire_secs * 1000;
    add_invite(&config.invites_path, &token, expires_at)?;
    println!("{node_id}:{token}");
    tracing::info!(target: "tngl", expires_secs = config.invite_expire_secs, "invite token generated");
    Ok(())
}

pub fn run_join(config: Config, ticket: &str) -> io::Result<()> {
    let (host_id_str, token) = ticket
        .split_once(':')
        .ok_or_else(|| io::Error::other("invalid ticket: expected <node_id>:<token>"))?;
    let host_id: PublicKey = host_id_str
        .parse()
        .map_err(|_| io::Error::other("invalid node id in ticket"))?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(io::Error::other)?;
    runtime.block_on(async {
        let secret_key = load_or_create_secret_key(&config.key_path)?;
        let endpoint = Endpoint::builder()
            .alpns(vec![ALPN.to_vec()])
            .secret_key(secret_key)
            .bind()
            .await
            .map_err(io::Error::other)?;

        let (topic_id, members) = send_join_request(&endpoint, host_id, token).await?;
        save_group_state(&config.peers_path, topic_id.as_deref(), &members)?;
        tracing::info!(target: "tngl", "joined group successfully");
        Ok(())
    })
}

pub fn run_remove(config: Config, peer_id: PublicKey) -> io::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(io::Error::other)?;
    runtime.block_on(async move {
        let secret_key = load_or_create_secret_key(&config.key_path)?;
        let endpoint = Endpoint::builder()
            .alpns(vec![GOSSIP_ALPN.to_vec()])
            .secret_key(secret_key)
            .bind()
            .await
            .map_err(io::Error::other)?;

        let saved_peers_file = load_peers(&config.peers_path)?;
        let topic_id = ensure_topic_id(&config.peers_path, saved_peers_file.topic_id.as_deref())?;
        let local_origin = endpoint.id().to_string();
        let target_id = peer_id.to_string();

        let mut members: HashMap<String, MemberEntry> = saved_peers_file
            .member_entries()
            .into_iter()
            .map(|entry| (entry.id.clone(), entry))
            .collect();
        members.entry(local_origin.clone()).or_insert(MemberEntry {
            id: local_origin.clone(),
            status: MemberStatus::Active,
            lamport: 0,
        });
        let next_lamport = members
            .values()
            .map(|entry| entry.lamport)
            .max()
            .unwrap_or(0)
            + 1;
        members
            .entry(target_id.clone())
            .and_modify(|entry| {
                entry.status = MemberStatus::Removed;
                entry.lamport = next_lamport;
            })
            .or_insert(MemberEntry {
                id: target_id.clone(),
                status: MemberStatus::Removed,
                lamport: next_lamport,
            });
        persist_members(&config.peers_path, topic_id, &members)?;

        let gossip = Gossip::builder().spawn(endpoint.clone());
        let router = Router::builder(endpoint.clone())
            .accept(GOSSIP_ALPN, gossip.clone())
            .spawn();
        let bootstrap_peers: Vec<PublicKey> = members
            .values()
            .filter(|entry| entry.status == MemberStatus::Active && entry.id != local_origin)
            .filter_map(|entry| entry.id.parse().ok())
            .collect();
        let topic = gossip
            .subscribe(topic_id, bootstrap_peers)
            .await
            .map_err(io::Error::other)?;
        let (sender, _receiver) = topic.split();
        sender
            .broadcast(
                build_member_list_msg(&local_origin, &members)
                    .to_bytes()
                    .into(),
            )
            .await
            .map_err(io::Error::other)?;
        tokio::time::sleep(Duration::from_millis(500)).await;
        router.shutdown().await.map_err(io::Error::other)?;
        gossip.shutdown().await.map_err(io::Error::other)?;
        tracing::info!(target: "tngl", peer = target_id, "peer removed");
        Ok(())
    })
}

/// Open `.tngl/daemon.lock` and hold an exclusive non-blocking flock.
///
/// Returns the open `File` — the lock is released when it is dropped.
/// Returns an error immediately if another daemon instance holds the lock.
fn acquire_daemon_lock(state_dir: &Path) -> io::Result<fs::File> {
    let lock_path = state_dir.join("daemon.lock");
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)?;
    let ret = unsafe {
        libc::flock(
            std::os::unix::io::AsRawFd::as_raw_fd(&file),
            libc::LOCK_EX | libc::LOCK_NB,
        )
    };
    if ret != 0 {
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::WouldBlock {
            return Err(io::Error::other(
                "another tngl instance is already running on this folder",
            ));
        }
        return Err(err);
    }
    Ok(file)
}

fn load_or_create_secret_key(path: &Path) -> io::Result<SecretKey> {
    if let Ok(bytes) = fs::read(path) {
        let array: [u8; 32] = bytes
            .try_into()
            .map_err(|_| io::Error::other("invalid iroh key file length"))?;
        return Ok(SecretKey::from_bytes(&array));
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut bytes = [0u8; 32];
    fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    fs::write(path, bytes)?;
    Ok(SecretKey::from_bytes(&bytes))
}

fn print_node_info(endpoint: &Endpoint) -> io::Result<()> {
    let addr = endpoint.watch_addr().get();
    let addr_json = serde_json::to_string(&addr).map_err(io::Error::other)?;
    tracing::info!(target: "tngl", node_id = %endpoint.id(), "node started");
    tracing::info!(target: "tngl", addr = addr_json, "listening");
    Ok(())
}

fn dedupe_peers(peers: &mut Vec<EndpointAddr>) {
    let mut seen = HashSet::new();
    peers.retain(|peer| seen.insert(peer.id));
}

fn ensure_topic_id(path: &Path, topic_id: Option<&str>) -> io::Result<TopicId> {
    if let Some(existing) = topic_id {
        return existing
            .parse()
            .map_err(|err| io::Error::other(format!("invalid stored topic id: {err}")));
    }
    let mut bytes = [0u8; 32];
    fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    let topic_id = TopicId::from_bytes(bytes);
    set_topic_id(path, &encode_hex(topic_id.as_bytes()))?;
    Ok(topic_id)
}

fn encode_hex(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

async fn publish_gossip(state: &State, message: GossipMessage) {
    if state.removed_from_group {
        return;
    }
    tracing::debug!(target: "tngl::gossip", "send {}", summarize_gossip(&message));
    if let Err(err) = state
        .gossip_sender
        .broadcast(message.to_bytes().into())
        .await
    {
        tracing::warn!(target: "tngl::gossip", "publish error: {err}");
    }
}

fn is_active_member(state: &State, id: &str) -> bool {
    matches!(
        state.members.get(id).map(|entry| &entry.status),
        Some(MemberStatus::Active)
    )
}

fn persist_members(
    peers_path: &Path,
    topic_id: TopicId,
    members: &HashMap<String, MemberEntry>,
) -> io::Result<()> {
    let mut ledger: Vec<MemberEntry> = members.values().cloned().collect();
    ledger.sort_by(|left, right| left.id.cmp(&right.id));
    save_group_state(peers_path, Some(&encode_hex(topic_id.as_bytes())), &ledger)
}

struct MemberListUpdate {
    changed: bool,
    removed_from_group: bool,
    peers: Vec<EndpointAddr>,
}

fn apply_member_list_update(
    local_origin: &str,
    members: &mut HashMap<String, MemberEntry>,
    incoming: Vec<MemberEntry>,
) -> MemberListUpdate {
    let mut changed = false;
    let mut removed_from_group = false;
    for entry in incoming {
        let should_apply = members
            .get(&entry.id)
            .map(|current| entry.lamport > current.lamport)
            .unwrap_or(true);
        if !should_apply {
            continue;
        }
        if entry.id == local_origin && entry.status == MemberStatus::Removed {
            removed_from_group = true;
        }
        members.insert(entry.id.clone(), entry);
        changed = true;
    }

    let mut peers: Vec<EndpointAddr> = if changed {
        members
            .values()
            .filter(|entry| entry.id != local_origin && entry.status == MemberStatus::Active)
            .filter_map(|entry| entry.id.parse().ok().map(EndpointAddr::new))
            .collect()
    } else {
        Vec::new()
    };
    dedupe_peers(&mut peers);

    MemberListUpdate {
        changed,
        removed_from_group,
        peers,
    }
}

async fn handle_gossip_message(
    state: &mut State,
    cmd_tx: &mpsc::UnboundedSender<Command>,
    message: GossipMessage,
) -> io::Result<()> {
    if message.origin() == state.local_origin {
        return Ok(());
    }
    tracing::debug!(target: "tngl::gossip", "recv {}", summarize_gossip(&message));
    match message {
        GossipMessage::FileChanged {
            origin,
            id,
            hash,
            lamport,
            changed_at_ms,
        } => {
            if !is_active_member(state, &origin) {
                return Ok(());
            }
            let remote = SnapshotEntry {
                hash,
                lamport,
                changed_at_ms,
                origin: origin.clone(),
                mtime_ms: 0,
            };
            if !should_accept_remote(state.entries.get(&id), &remote) {
                return Ok(());
            }
            let Some(peer_id) = file_changed_fetch_peer(
                is_active_member(state, &origin),
                state.entries.get(&id),
                &origin,
                &remote,
            ) else {
                return Ok(());
            };
            let endpoint = state.endpoint.clone();
            let tx = cmd_tx.clone();
            let sync_dir = state.sync_dir.clone();
            tokio::spawn(async move {
                let tmp_dir = sync_dir.join(".tngl");
                match fetch_entries(&endpoint, &EndpointAddr::new(peer_id), vec![id], &tmp_dir)
                    .await
                {
                    Ok(entries) => {
                        let (reply_tx, reply_rx) = oneshot::channel();
                        let _ = tx.send(Command::ApplyRemote {
                            entries,
                            respond_to: Some(reply_tx),
                        });
                        let _ = reply_rx.await;
                    }
                    Err(err) => {
                        tracing::warn!(target: "tngl", %peer_id, "gossip fetch failed, falling back to full sync: {err}");
                        let _ = tx.send(Command::SyncPeer(EndpointAddr::new(peer_id)));
                    }
                }
            });
        }
        GossipMessage::SyncState {
            origin,
            root_hash: remote_root,
            ..
        } => {
            if !is_active_member(state, &origin) {
                return Ok(());
            }
            if remote_root != root_hash(&state.entries) {
                if let Ok(peer_id) = origin.parse() {
                    let _ = cmd_tx.send(Command::SyncPeer(EndpointAddr::new(peer_id)));
                }
            }
        }
        GossipMessage::MemberList { origin, members } => {
            if !is_active_member(state, &origin) {
                return Ok(());
            }
            let update = apply_member_list_update(&state.local_origin, &mut state.members, members);
            if !update.changed {
                return Ok(());
            }
            state.removed_from_group |= update.removed_from_group;
            let peers = update.peers;
            state.peers = peers.clone();
            let allowlist: HashSet<PublicKey> = peers.iter().map(|peer| peer.id).collect();
            *state.allowlist.write().await = allowlist;
            persist_members(&state.peers_path, state.topic_id, &state.members)?;
            if !peers.is_empty() {
                let peer_ids: Vec<PublicKey> = peers.iter().map(|peer| peer.id).collect();
                if let Err(err) = state.gossip_sender.join_peers(peer_ids).await {
                    tracing::warn!(target: "tngl::gossip", "join_peers failed: {err}");
                }
            }
            for peer in peers {
                let _ = cmd_tx.send(Command::SyncPeer(peer));
            }
        }
    }
    Ok(())
}

fn file_changed_fetch_peer(
    origin_is_active_member: bool,
    current: Option<&SnapshotEntry>,
    origin: &str,
    remote: &SnapshotEntry,
) -> Option<PublicKey> {
    if !origin_is_active_member {
        return None;
    }
    if !should_accept_remote(current, remote) {
        return None;
    }
    origin.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::TOMBSTONE_HASH;
    use std::collections::HashMap;

    fn snapshot(hash_byte: u8, lamport: u64, origin: &str) -> SnapshotEntry {
        SnapshotEntry {
            hash: [hash_byte; 32],
            lamport,
            changed_at_ms: lamport,
            origin: origin.to_string(),
            mtime_ms: 0,
        }
    }

    #[test]
    fn file_changed_fetch_peer_rejects_non_member() {
        let peer = "03b16e503664b5f13260af120da62bc793232fad5c425a513bb2f2ce675dbf2e".to_string();
        assert!(file_changed_fetch_peer(false, None, &peer, &snapshot(1, 1, &peer)).is_none());
    }

    #[test]
    fn file_changed_fetch_peer_rejects_stale_update() {
        let peer = "5371ffdc857cb51e0a9f2a1b0d432c73075893238c2cea971116fa8401c3b097".to_string();
        let current = snapshot(9, 5, "local");
        assert!(
            file_changed_fetch_peer(true, Some(&current), &peer, &snapshot(1, 4, &peer)).is_none()
        );
    }

    #[test]
    fn file_changed_fetch_peer_accepts_newer_member_update() {
        let peer_key = PublicKey::from_bytes(&[9; 32]).unwrap();
        let peer = peer_key.to_string();
        let current = SnapshotEntry {
            hash: TOMBSTONE_HASH,
            lamport: 1,
            changed_at_ms: 1,
            origin: "local".into(),
            mtime_ms: 0,
        };
        assert_eq!(
            file_changed_fetch_peer(true, Some(&current), &peer, &snapshot(7, 2, &peer)),
            Some(peer_key)
        );
    }

    fn member(id: &str, status: MemberStatus, lamport: u64) -> MemberEntry {
        MemberEntry {
            id: id.to_string(),
            status,
            lamport,
        }
    }

    #[test]
    fn member_list_merge_ignores_stale_entries() {
        let peer_a = "03b16e503664b5f13260af120da62bc793232fad5c425a513bb2f2ce675dbf2e".to_string();
        let peer_b = "5371ffdc857cb51e0a9f2a1b0d432c73075893238c2cea971116fa8401c3b097".to_string();
        let local = "local-node";
        let mut members = HashMap::from([
            (local.to_string(), member(local, MemberStatus::Active, 1)),
            (peer_a.clone(), member(&peer_a, MemberStatus::Active, 5)),
            (peer_b.clone(), member(&peer_b, MemberStatus::Active, 3)),
        ]);

        let update = apply_member_list_update(
            local,
            &mut members,
            vec![
                member(&peer_a, MemberStatus::Removed, 4),
                member(&peer_b, MemberStatus::Removed, 3),
            ],
        );

        assert!(!update.changed);
        assert!(!update.removed_from_group);
        assert!(update.peers.is_empty());
        assert_eq!(members.get(&peer_a).unwrap().status, MemberStatus::Active);
        assert_eq!(members.get(&peer_b).unwrap().status, MemberStatus::Active);
    }

    #[test]
    fn member_list_merge_marks_self_removed() {
        let local = "local-node";
        let peer = "03b16e503664b5f13260af120da62bc793232fad5c425a513bb2f2ce675dbf2e".to_string();
        let mut members = HashMap::from([
            (local.to_string(), member(local, MemberStatus::Active, 1)),
            (peer.clone(), member(&peer, MemberStatus::Active, 1)),
        ]);

        let update = apply_member_list_update(
            local,
            &mut members,
            vec![member(local, MemberStatus::Removed, 2)],
        );

        assert!(update.changed);
        assert!(update.removed_from_group);
        assert_eq!(members.get(local).unwrap().status, MemberStatus::Removed);
        assert_eq!(update.peers.len(), 1);
        assert_eq!(update.peers[0].id.to_string(), peer);
    }

    #[test]
    fn member_list_merge_routes_only_active_non_self_peers() {
        let local = "local-node";
        let peer_a = "03b16e503664b5f13260af120da62bc793232fad5c425a513bb2f2ce675dbf2e".to_string();
        let peer_b = "5371ffdc857cb51e0a9f2a1b0d432c73075893238c2cea971116fa8401c3b097".to_string();
        let peer_c = PublicKey::from_bytes(&[9; 32]).unwrap().to_string();
        let mut members = HashMap::from([
            (local.to_string(), member(local, MemberStatus::Active, 1)),
            (peer_a.clone(), member(&peer_a, MemberStatus::Active, 1)),
            (peer_b.clone(), member(&peer_b, MemberStatus::Removed, 1)),
        ]);

        let update = apply_member_list_update(
            local,
            &mut members,
            vec![
                member(&peer_b, MemberStatus::Active, 2),
                member(&peer_c, MemberStatus::Active, 3),
            ],
        );

        let mut routed: Vec<String> = update
            .peers
            .iter()
            .map(|peer| peer.id.to_string())
            .collect();
        routed.sort();
        let mut expected = vec![peer_a, peer_b, peer_c];
        expected.sort();
        assert!(update.changed);
        assert!(!update.removed_from_group);
        assert_eq!(routed, expected);
    }
}
