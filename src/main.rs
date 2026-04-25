mod group;
mod message;
mod protocol;
mod rpc;
mod state;
mod sync;
mod watcher;

use crate::group::GroupState;
use crate::message::{GossipMessage, WireChange};
use crate::state::{Change, FolderState, hex};
use clap::Parser;
use futures_lite::StreamExt;
use iroh::protocol::Router;
use iroh::{Endpoint, PublicKey, SecretKey};
use iroh_gossip::api::{ApiError, Event as GossipEvent, GossipSender};
use iroh_gossip::{ALPN as GOSSIP_ALPN, Gossip, TopicId};
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, mpsc};

const KEY_FILE: &str = "private.key";
const PEERS_FILE: &str = "peers.json";
const INVITES_FILE: &str = "invites.json";

#[derive(Parser, Debug)]
#[command(name = "tngl", about = "Monitor a folder and gossip tngl tree changes")]
struct Cli {
    #[arg(long, value_name = "PATH", help = "Folder to monitor")]
    folder: PathBuf,

    #[arg(long, help = "Create a one-time group join ticket and exit")]
    invite: bool,

    #[arg(
        long,
        value_name = "SECONDS",
        default_value = "3600",
        help = "Ticket lifetime when used with --invite"
    )]
    expire_secs: u64,

    #[arg(
        long,
        value_name = "TICKET",
        help = "Join a group using <node_id>:<secret>"
    )]
    ticket: Option<String>,

    #[arg(
        long,
        value_name = "MILLIS",
        default_value = "500",
        help = "Debounce delay for filesystem events, or scan interval with --poll"
    )]
    interval_ms: u64,

    #[arg(
        long,
        value_name = "SECONDS",
        default_value = "10",
        help = "How often to publish local root state"
    )]
    announce_interval_secs: u64,

    #[arg(long, help = "Use periodic polling instead of filesystem events")]
    poll: bool,
}

#[derive(Debug, Clone)]
struct JoinTicket {
    issuer: PublicKey,
    secret: String,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .without_time()
        .init();

    if let Err(err) = run().await {
        tracing::error!("{err}");
        std::process::exit(1);
    }
}

async fn run() -> io::Result<()> {
    let cli = Cli::parse();
    fs::create_dir_all(&cli.folder)?;

    let state_dir = cli.folder.join(".tngl");
    fs::create_dir_all(&state_dir)?;

    if cli.invite {
        return create_invite(&state_dir, cli.expire_secs);
    }

    let _lock = acquire_daemon_lock(&state_dir)?;

    let secret_key = load_or_create_secret_key(&state_dir.join(KEY_FILE))?;
    let endpoint = Endpoint::builder()
        .alpns(vec![GOSSIP_ALPN.to_vec(), rpc::ALPN.to_vec()])
        .secret_key(secret_key)
        .bind()
        .await
        .map_err(io::Error::other)?;

    let local_origin = endpoint.id().to_string();
    let peers_path = state_dir.join(PEERS_FILE);
    let invites_path = state_dir.join(INVITES_FILE);
    if let Some(ticket) = cli.ticket.as_deref() {
        join_group(&endpoint, &peers_path, ticket).await?;
    }
    let group = Arc::new(RwLock::new(GroupState::load_or_init(
        peers_path,
        endpoint.id(),
    )?));
    let topic_id = group.read().await.topic_id();
    let bootstrap = group.read().await.active_peers();
    let state = Arc::new(RwLock::new(FolderState::new(
        cli.folder.clone(),
        local_origin.clone(),
    )?));

    {
        let state = state.read().await;
        print_start(
            &state,
            &endpoint,
            topic_id,
            &bootstrap,
            cli.poll,
            cli.interval_ms,
        );
    }

    let (fs_tx, mut fs_rx) = mpsc::unbounded_channel::<Vec<PathBuf>>();
    let (rpc_event_tx, mut rpc_event_rx) = mpsc::unbounded_channel();
    let watch_root = state.read().await.root().to_path_buf();
    let _watcher = watcher::spawn(watch_root, cli.interval_ms, cli.poll, fs_tx)?;

    let gossip = Gossip::builder().spawn(endpoint.clone());
    let rpc_client = rpc::RpcClient::new(endpoint.clone());
    let _router = Router::builder(endpoint.clone())
        .accept(GOSSIP_ALPN, gossip.clone())
        .accept(
            rpc::ALPN,
            rpc::FolderRpc::new(
                Arc::clone(&state),
                Arc::clone(&group),
                invites_path,
                rpc_event_tx,
            ),
        )
        .spawn();

    let topic = gossip
        .subscribe(topic_id, bootstrap.clone())
        .await
        .map_err(io::Error::other)?;
    let (sender, mut receiver) = topic.split();
    if !bootstrap.is_empty() {
        if let Err(err) = sender.join_peers(bootstrap).await {
            tracing::warn!("join known peers failed: {err}");
        }
    }

    {
        let state = state.read().await;
        publish_sync_state(&sender, &StateSnapshot::from_state(&state), &local_origin).await;
    }
    publish_peers(&sender, Arc::clone(&group), &local_origin).await;

    let mut announce_interval =
        tokio::time::interval(Duration::from_secs(cli.announce_interval_secs.max(1)));
    announce_interval.tick().await;

    loop {
        tokio::select! {
            Some(paths) = fs_rx.recv() => {
                let update = {
                    let mut state = state.write().await;
                    let before_state = state.root_hash();
                    let before_live = state.live_root_hash();
                    let result = if paths.is_empty() {
                        state.rescan()
                    } else {
                        state.apply_paths(paths)
                    };
                    match result {
                        Ok(changes) if changes.is_empty() => None,
                        Ok(changes) => {
                            print_changes(before_state, before_live, &state, &changes);
                            Some((changes, StateSnapshot::from_state(&state)))
                        }
                        Err(err) => {
                            tracing::warn!("scan failed: {err}");
                            None
                        }
                    }
                };
                if let Some((changes, snapshot)) = update {
                    publish_filesystem_changed(&sender, &snapshot, &local_origin, &changes).await;
                }
            }
            event = receiver.next() => {
                let Some(event) = event else {
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, "gossip stream closed"));
                };
                handle_gossip_event(
                    event,
                    rpc_client.clone(),
                    Arc::clone(&state),
                    Arc::clone(&group),
                    &sender,
                    &local_origin,
                ).await?;
            }
            Some(event) = rpc_event_rx.recv() => {
                match event {
                    rpc::RpcEvent::PeerJoined { peer } => {
                        tracing::info!("peer joined {peer}");
                        if let Err(err) = sender.join_peers(vec![peer]).await {
                            tracing::warn!("join new peer failed: {err}");
                        }
                        publish_peers(&sender, Arc::clone(&group), &local_origin).await;
                    }
                }
            }
            _ = announce_interval.tick() => {
                let snapshot = {
                    let state = state.read().await;
                    StateSnapshot::from_state(&state)
                };
                publish_sync_state(&sender, &snapshot, &local_origin).await;
                publish_peers(&sender, Arc::clone(&group), &local_origin).await;
            }
        }
    }
}

#[derive(Clone, Copy)]
struct StateSnapshot {
    state_root: [u8; 32],
    live_root: [u8; 32],
    lamport: u64,
    state_nodes: usize,
    live_nodes: usize,
}

impl StateSnapshot {
    fn from_state(state: &FolderState) -> Self {
        Self {
            state_root: state.root_hash(),
            live_root: state.live_root_hash(),
            lamport: state.lamport(),
            state_nodes: state.tree().nodes.len(),
            live_nodes: state.live_tree().nodes.len(),
        }
    }
}

async fn publish_sync_state(sender: &GossipSender, state: &StateSnapshot, origin: &str) {
    let message = GossipMessage::SyncState {
        origin: origin.to_string(),
        state_root: state.state_root,
        live_root: state.live_root,
        lamport: state.lamport,
        state_nodes: state.state_nodes,
        live_nodes: state.live_nodes,
    };
    publish(sender, message).await;
}

async fn publish_filesystem_changed(
    sender: &GossipSender,
    state: &StateSnapshot,
    origin: &str,
    changes: &[Change],
) {
    let message = GossipMessage::FilesystemChanged {
        origin: origin.to_string(),
        state_root: state.state_root,
        live_root: state.live_root,
        lamport: state.lamport,
        changes: changes.iter().map(WireChange::from).collect(),
    };
    publish(sender, message).await;
}

async fn publish_peers(sender: &GossipSender, group: Arc<RwLock<GroupState>>, origin: &str) {
    let members = group.read().await.members();
    let message = GossipMessage::Peers {
        origin: origin.to_string(),
        members,
    };
    publish(sender, message).await;
}

async fn publish(sender: &GossipSender, message: GossipMessage) {
    tracing::debug!("gossip send {}", summarize_message(&message));
    if let Err(err) = sender.broadcast(message.to_bytes().into()).await {
        tracing::warn!("gossip publish failed: {err}");
    }
}

async fn handle_gossip_event(
    event: Result<GossipEvent, ApiError>,
    rpc_client: rpc::RpcClient,
    state: Arc<RwLock<FolderState>>,
    group: Arc<RwLock<GroupState>>,
    sender: &GossipSender,
    local_origin: &str,
) -> io::Result<()> {
    match event {
        Ok(GossipEvent::Received(message)) => {
            let Some(message) = GossipMessage::from_bytes(&message.content) else {
                tracing::debug!("ignored unknown gossip message");
                return Ok(());
            };
            if message.origin() == local_origin {
                return Ok(());
            }
            if !is_active_origin(&group, message.origin()).await {
                tracing::debug!("ignored gossip from non-member {}", message.origin());
                return Ok(());
            }
            let local = {
                let state = state.read().await;
                StateSnapshot::from_state(&state)
            };
            print_remote_message(&message, &local);
            match &message {
                GossipMessage::Peers { members, .. } => {
                    let update = group.write().await.merge_members(members.clone())?;
                    if update.changed {
                        tracing::info!("peers updated active={}", update.active_peers.len());
                        if let Err(err) = sender.join_peers(update.active_peers).await {
                            tracing::warn!("join peers from gossip failed: {err}");
                        }
                        if update.removed_self {
                            tracing::warn!("this node is marked removed from the group");
                        }
                    }
                }
                _ => maybe_probe_remote_rpc(rpc_client, message, state),
            }
        }
        Ok(GossipEvent::NeighborUp(endpoint_id)) => {
            tracing::info!("gossip neighbor up {endpoint_id}");
        }
        Ok(GossipEvent::NeighborDown(endpoint_id)) => {
            tracing::info!("gossip neighbor down {endpoint_id}");
        }
        Ok(GossipEvent::Lagged) => {
            tracing::warn!("gossip stream lagged");
        }
        Err(err) => tracing::warn!("gossip receive failed: {err}"),
    }
    Ok(())
}

async fn is_active_origin(group: &Arc<RwLock<GroupState>>, origin: &str) -> bool {
    let Ok(peer) = origin.parse::<PublicKey>() else {
        return false;
    };
    group.read().await.is_active_member(&peer)
}

fn print_remote_message(message: &GossipMessage, state: &StateSnapshot) {
    match message {
        GossipMessage::SyncState {
            origin,
            state_root,
            live_root,
            lamport,
            state_nodes,
            live_nodes,
        } => {
            tracing::info!(
                "peer state origin={} lamport={} state_root={} live_root={} state_nodes={} live_nodes={} state_match={} live_match={}",
                origin,
                lamport,
                hex(*state_root),
                hex(*live_root),
                state_nodes,
                live_nodes,
                *state_root == state.state_root,
                *live_root == state.live_root
            );
        }
        GossipMessage::FilesystemChanged {
            origin,
            state_root,
            live_root,
            lamport,
            changes,
        } => {
            tracing::info!(
                "peer filesystem origin={} lamport={} changes={} state_root={} live_root={} state_match={} live_match={}",
                origin,
                lamport,
                changes.len(),
                hex(*state_root),
                hex(*live_root),
                *state_root == state.state_root,
                *live_root == state.live_root
            );
            for change in changes {
                tracing::info!(
                    "peer change {} {} v{}:{}",
                    change.verb,
                    change.path,
                    change.version_lamport,
                    change.version_origin
                );
            }
        }
        GossipMessage::Peers { origin, members } => {
            tracing::info!("peer list origin={} members={}", origin, members.len());
        }
    }
}

fn maybe_probe_remote_rpc(
    rpc_client: rpc::RpcClient,
    message: GossipMessage,
    state: Arc<RwLock<FolderState>>,
) {
    let (origin, remote_state_root, remote_live_root) = match message {
        GossipMessage::SyncState {
            origin,
            state_root,
            live_root,
            ..
        }
        | GossipMessage::FilesystemChanged {
            origin,
            state_root,
            live_root,
            ..
        } => (origin, state_root, live_root),
        GossipMessage::Peers { .. } => return,
    };
    let Ok(peer) = origin.parse::<PublicKey>() else {
        tracing::warn!("cannot RPC probe peer with invalid origin {origin}");
        return;
    };

    tokio::spawn(async move {
        let local = {
            let state = state.read().await;
            StateSnapshot::from_state(&state)
        };
        if remote_state_root == local.state_root && remote_live_root == local.live_root {
            return;
        }

        match sync::reconcile_with_peer(rpc_client, state, peer).await {
            Ok(changes) if changes.is_empty() => {
                tracing::info!("sync peer={} no changes applied", peer);
            }
            Ok(changes) => {
                tracing::info!("sync peer={} applied {} changes", peer, changes.len());
            }
            Err(err) => {
                tracing::warn!("sync peer={} failed: {err}", peer);
            }
        }
    });
}

fn print_start(
    state: &FolderState,
    endpoint: &Endpoint,
    topic_id: TopicId,
    bootstrap: &[PublicKey],
    poll: bool,
    interval_ms: u64,
) {
    tracing::info!("node {}", endpoint.id());
    tracing::info!("topic {}", hex(*topic_id.as_bytes()));
    tracing::info!(
        "invite command: tngl --folder {} --invite",
        state.root().display()
    );
    tracing::info!("known peers {}", bootstrap.len());
    tracing::info!("watching {}", state.root().display());
    if poll {
        tracing::info!("mode poll interval={}ms", interval_ms.max(50));
    } else {
        tracing::info!("mode fs-events debounce={}ms", interval_ms.max(20));
    }
    tracing::info!("state root {}", hex(state.root_hash()));
    tracing::info!("live root {}", hex(state.live_root_hash()));
    tracing::info!(
        "nodes state={} live={}",
        state.tree().nodes.len(),
        state.live_tree().nodes.len()
    );
}

fn print_changes(
    before_state: [u8; 32],
    before_live: [u8; 32],
    state: &FolderState,
    changes: &[Change],
) {
    for change in changes {
        tracing::info!(
            "{} {} v{}:{}",
            change.verb(),
            change.path,
            change.new.version.lamport,
            change.new.version.origin
        );
    }

    tracing::info!(
        "state root {} -> {} nodes={}",
        hex(before_state),
        hex(state.root_hash()),
        state.tree().nodes.len()
    );
    tracing::info!(
        "live root {} -> {} nodes={}",
        hex(before_live),
        hex(state.live_root_hash()),
        state.live_tree().nodes.len()
    );
}

fn summarize_message(message: &GossipMessage) -> String {
    match message {
        GossipMessage::SyncState {
            origin,
            state_root,
            lamport,
            ..
        } => format!(
            "sync-state origin={origin} lamport={lamport} state_root={}",
            hex(*state_root)
        ),
        GossipMessage::FilesystemChanged {
            origin,
            lamport,
            changes,
            ..
        } => format!(
            "filesystem-changed origin={origin} lamport={lamport} changes={}",
            changes.len()
        ),
        GossipMessage::Peers { origin, members } => {
            format!("peers origin={origin} members={}", members.len())
        }
    }
}

fn create_invite(state_dir: &Path, expire_secs: u64) -> io::Result<()> {
    let secret_key = load_or_create_secret_key(&state_dir.join(KEY_FILE))?;
    let node_id = secret_key.public();
    let peers_path = state_dir.join(PEERS_FILE);
    let _group = GroupState::load_or_init(peers_path, node_id)?;
    let secret = group::generate_secret()?;
    let expires_at = group::now_ms()? + expire_secs.saturating_mul(1000);
    group::add_invite(&state_dir.join(INVITES_FILE), &secret, expires_at)?;
    println!("{node_id}:{secret}");
    tracing::info!("invite expires in {}s", expire_secs);
    Ok(())
}

async fn join_group(endpoint: &Endpoint, peers_path: &Path, ticket: &str) -> io::Result<()> {
    let ticket = parse_join_ticket(ticket)?;
    let rpc_client = rpc::RpcClient::new(endpoint.clone());
    let (topic_id, members) = rpc_client
        .join_group(ticket.issuer, ticket.secret, endpoint.id())
        .await?;
    let topic_id = topic_id
        .parse()
        .map_err(|err| io::Error::other(format!("invalid topic id from join response: {err}")))?;
    GroupState::replace(peers_path, topic_id, members)?;
    tracing::info!("joined group via {}", ticket.issuer);
    Ok(())
}

fn load_or_create_secret_key(path: &Path) -> io::Result<SecretKey> {
    if let Ok(bytes) = fs::read(path) {
        let bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| io::Error::other("invalid iroh key file length"))?;
        return Ok(SecretKey::from_bytes(&bytes));
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut bytes = [0u8; 32];
    fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    fs::write(path, bytes)?;
    Ok(SecretKey::from_bytes(&bytes))
}

#[cfg(unix)]
fn acquire_daemon_lock(state_dir: &Path) -> io::Result<fs::File> {
    use std::os::fd::AsRawFd;

    let lock_path = state_dir.join("daemon.lock");
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)?;
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result != 0 {
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

#[cfg(not(unix))]
fn acquire_daemon_lock(state_dir: &Path) -> io::Result<fs::File> {
    let lock_path = state_dir.join("daemon.lock");
    fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(lock_path)
        .map_err(|err| {
            if err.kind() == io::ErrorKind::AlreadyExists {
                io::Error::other("another tngl instance is already running on this folder")
            } else {
                err
            }
        })
}

fn parse_join_ticket(value: &str) -> io::Result<JoinTicket> {
    let (issuer, secret) = value
        .split_once(':')
        .ok_or_else(|| io::Error::other("invalid ticket: expected <node_id>:<secret>"))?;
    let issuer = issuer
        .parse()
        .map_err(|err| io::Error::other(format!("invalid node id in ticket: {err}")))?;
    if secret.is_empty() {
        return Err(io::Error::other("invalid ticket: empty secret"));
    }
    Ok(JoinTicket {
        issuer,
        secret: secret.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_ticket_parses_node_and_secret() {
        let issuer = SecretKey::from_bytes(&[7; 32]).public();
        let ticket = parse_join_ticket(&format!("{issuer}:secret")).unwrap();
        assert_eq!(ticket.issuer, issuer);
        assert_eq!(ticket.secret, "secret");
    }

    #[test]
    fn secret_key_is_reused_from_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let key_path = tmp.path().join(KEY_FILE);

        let first = load_or_create_secret_key(&key_path).unwrap();
        let second = load_or_create_secret_key(&key_path).unwrap();

        assert_eq!(first.public(), second.public());
        assert_eq!(fs::read(key_path).unwrap().len(), 32);
    }

    #[test]
    fn daemon_lock_rejects_second_holder() {
        let tmp = tempfile::tempdir().unwrap();

        let first = acquire_daemon_lock(tmp.path()).unwrap();
        let second = acquire_daemon_lock(tmp.path()).unwrap_err();

        assert_eq!(
            second.to_string(),
            "another tngl instance is already running on this folder"
        );
        drop(first);
        let _third = acquire_daemon_lock(tmp.path()).unwrap();
    }
}
