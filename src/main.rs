mod group;
mod message;
mod protocol;
mod rpc;
mod state;
mod sync;
mod watcher;

use crate::group::GroupState;
use crate::message::{GossipMessage, TreeHint};
use crate::state::{Change, FolderState, GcWatermark, hex};
use clap::{Parser, Subcommand};
use futures_lite::StreamExt;
use iroh::protocol::Router;
use iroh::{Endpoint, PublicKey, SecretKey};
use iroh_gossip::api::{ApiError, Event as GossipEvent, GossipSender};
use iroh_gossip::{ALPN as GOSSIP_ALPN, Gossip, TopicId};
use std::collections::BTreeSet;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, mpsc};

const KEY_FILE: &str = "private.key";
const PEERS_FILE: &str = "peers.json";
const INVITES_FILE: &str = "invites.json";
const MAX_FILESYSTEM_CHANGED_BYTES: usize = 3_000;

#[derive(Parser, Debug)]
#[command(name = "lil", about = "lilsync folder sync daemon")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the sync daemon for a folder
    Sync {
        /// Folder to monitor
        folder: PathBuf,
        /// Human-readable name for this node
        #[arg(long)]
        name: Option<String>,
        /// Use periodic polling instead of filesystem events
        #[arg(long)]
        poll: bool,
        /// Debounce delay for filesystem events, or scan interval with --poll
        #[arg(long, value_name = "MILLIS", default_value = "500")]
        interval_ms: u64,
        /// How often to publish local root state
        #[arg(long, value_name = "SECONDS", default_value = "10")]
        announce_interval_secs: u64,
    },
    /// Create a one-time join ticket and exit
    Invite {
        /// Folder whose group to invite into
        folder: PathBuf,
        /// Ticket lifetime in seconds
        #[arg(long, value_name = "SECONDS", default_value = "3600")]
        expire_secs: u64,
    },
    /// Join a group using a ticket
    Join {
        /// Folder to sync
        folder: PathBuf,
        /// 86-character base62 ticket from `lil invite`
        ticket: String,
        /// Human-readable name for this node
        #[arg(long)]
        name: Option<String>,
    },
    /// Remove a peer by node ID or name
    Remove {
        /// Folder whose group to modify
        folder: PathBuf,
        /// Node ID or name to remove
        target: String,
    },
    /// List known peers
    Peers {
        /// Folder whose group to inspect
        folder: PathBuf,
    },
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
        .with_level(true)
        .with_target(false)
        .init();

    if let Err(err) = run().await {
        tracing::error!("{err}");
        std::process::exit(1);
    }
}

async fn run() -> io::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Invite { folder, expire_secs } => {
            fs::create_dir_all(&folder)?;
            let state_dir = folder.join(".lil");
            fs::create_dir_all(&state_dir)?;
            return create_invite(&state_dir, expire_secs);
        }
        Command::Remove { folder, target } => {
            fs::create_dir_all(&folder)?;
            let state_dir = folder.join(".lil");
            fs::create_dir_all(&state_dir)?;
            return remove_peer_cmd(&state_dir, &target);
        }
        Command::Peers { folder } => {
            fs::create_dir_all(&folder)?;
            let state_dir = folder.join(".lil");
            fs::create_dir_all(&state_dir)?;
            return peers_cmd(&state_dir);
        }
        Command::Join { folder, ticket, name } => {
            fs::create_dir_all(&folder)?;
            let state_dir = folder.join(".lil");
            fs::create_dir_all(&state_dir)?;
            let secret_key = load_or_create_secret_key(&state_dir.join(KEY_FILE))?;
            let endpoint = Endpoint::builder()
                .alpns(vec![GOSSIP_ALPN.to_vec(), rpc::ALPN.to_vec()])
                .secret_key(secret_key)
                .bind()
                .await
                .map_err(io::Error::other)?;
            let peers_path = state_dir.join(PEERS_FILE);
            join_group(&endpoint, &peers_path, &ticket, name.clone()).await?;
            run_sync(folder, name, false, 500, 10).await?;
        }
        Command::Sync {
            folder,
            name,
            poll,
            interval_ms,
            announce_interval_secs,
        } => run_sync(folder, name, poll, interval_ms, announce_interval_secs).await?,
    }
    Ok(())
}

async fn run_sync(
    folder: PathBuf,
    name: Option<String>,
    poll: bool,
    interval_ms: u64,
    announce_interval_secs: u64,
) -> io::Result<()> {
    fs::create_dir_all(&folder)?;

    let state_dir = folder.join(".lil");
    fs::create_dir_all(&state_dir)?;

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
    let group = Arc::new(RwLock::new(GroupState::load_or_init(
        peers_path,
        endpoint.id(),
        name,
    )?));
    let topic_id = group.read().await.topic_id();
    let bootstrap = group.read().await.active_peers();
    let state = {
        let s = FolderState::new(folder.clone(), local_origin.clone())?;
        Arc::new(RwLock::new(s))
    };
    let root_reports = Arc::new(RwLock::new(RootReports::default()));

    {
        let state = state.read().await;
        print_start(
            &state,
            &endpoint,
            topic_id,
            &bootstrap,
            poll,
            interval_ms,
        );
    }

    let (fs_tx, mut fs_rx) = mpsc::unbounded_channel::<Vec<PathBuf>>();
    let (rpc_event_tx, mut rpc_event_rx) = mpsc::unbounded_channel();
    let watch_root = state.read().await.root().to_path_buf();
    let _watcher = watcher::spawn(watch_root, interval_ms, poll, fs_tx)?;

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
    maybe_gc_tombstones(
        Arc::clone(&state),
        Arc::clone(&group),
        Arc::clone(&root_reports),
        &sender,
        &local_origin,
    )
    .await;

    let mut announce_interval =
        tokio::time::interval(Duration::from_secs(announce_interval_secs.max(1)));
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
                            let snapshot = StateSnapshot::from_state(&state);
                            let hint = build_tree_hint(&state, &changes, &snapshot, &local_origin);
                            Some((changes, snapshot, hint))
                        }
                        Err(err) => {
                            tracing::warn!("scan failed: {err}");
                            None
                        }
                    }
                };
                if let Some((changes, snapshot, hint)) = update {
                    tracing::info!("filesystem changed paths={}", changes.len());
                    publish_filesystem_changed(&sender, &snapshot, &local_origin, hint).await;
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
                    Arc::clone(&root_reports),
                    &sender,
                    &local_origin,
                ).await?;
            }
            Some(event) = rpc_event_rx.recv() => {
                match event {
                    rpc::RpcEvent::PeerJoined { peer, name } => {
                        match name.as_deref() {
                            Some(n) => tracing::info!("peer joined {peer} ({n})"),
                            None    => tracing::info!("peer joined {peer}"),
                        };
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

#[derive(Clone)]
struct StateSnapshot {
    state_root: [u8; 32],
    live_root: [u8; 32],
    lamport: u64,
    state_nodes: usize,
    live_nodes: usize,
    gc_watermark: GcWatermark,
}

impl StateSnapshot {
    fn from_state(state: &FolderState) -> Self {
        Self {
            state_root: state.root_hash(),
            live_root: state.live_root_hash(),
            lamport: state.lamport(),
            state_nodes: state.tree().nodes.len(),
            live_nodes: state.live_tree().nodes.len(),
            gc_watermark: state.gc_watermark().clone(),
        }
    }
}

#[derive(Clone, Copy)]
struct RootReport {
    state_root: [u8; 32],
    lamport: u64,
}

#[derive(Default)]
struct RootReports {
    reports: std::collections::BTreeMap<String, RootReport>,
}

impl RootReports {
    fn update(&mut self, origin: String, report: RootReport) {
        let apply = self
            .reports
            .get(&origin)
            .is_none_or(|old| report.lamport >= old.lamport);
        if apply {
            self.reports.insert(origin, report);
        }
    }

    fn all_active_match(&self, local_root: [u8; 32], active_peer_ids: &[String]) -> bool {
        active_peer_ids.iter().all(|id| {
            self.reports
                .get(id)
                .is_some_and(|report| report.state_root == local_root)
        })
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
        gc_watermark: state.gc_watermark.clone(),
    };
    publish(sender, message).await;
}

async fn publish_filesystem_changed(
    sender: &GossipSender,
    state: &StateSnapshot,
    origin: &str,
    hint: Option<TreeHint>,
) {
    let message = GossipMessage::FilesystemChanged {
        origin: origin.to_string(),
        state_root: state.state_root,
        live_root: state.live_root,
        lamport: state.lamport,
        hint,
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

fn build_tree_hint(
    state: &FolderState,
    changes: &[Change],
    snapshot: &StateSnapshot,
    origin: &str,
) -> Option<TreeHint> {
    let mut prefixes = Vec::new();
    let mut seen = BTreeSet::new();
    push_hint_prefix(&mut prefixes, &mut seen, "");
    for change in changes {
        for prefix in path_prefixes(&change.path) {
            push_hint_prefix(&mut prefixes, &mut seen, &prefix);
        }
    }

    let mut hint = TreeHint {
        truncated: false,
        nodes: Vec::new(),
    };
    for prefix in prefixes {
        let Some(node) = state.node(&prefix) else {
            continue;
        };
        let mut candidate = hint.clone();
        candidate.nodes.push(node);
        if filesystem_changed_len(snapshot, origin, Some(&candidate))
            <= MAX_FILESYSTEM_CHANGED_BYTES
        {
            hint = candidate;
        } else {
            hint.truncated = true;
            break;
        }
    }

    if hint.nodes.is_empty() {
        None
    } else {
        Some(hint)
    }
}

fn filesystem_changed_len(
    snapshot: &StateSnapshot,
    origin: &str,
    hint: Option<&TreeHint>,
) -> usize {
    GossipMessage::FilesystemChanged {
        origin: origin.to_string(),
        state_root: snapshot.state_root,
        live_root: snapshot.live_root,
        lamport: snapshot.lamport,
        hint: hint.cloned(),
    }
    .to_bytes()
    .len()
}

fn push_hint_prefix(prefixes: &mut Vec<String>, seen: &mut BTreeSet<String>, prefix: &str) {
    let normalized = prefix.trim_matches('/').to_string();
    if seen.insert(normalized.clone()) {
        prefixes.push(normalized);
    }
}

fn path_prefixes(path: &str) -> Vec<String> {
    let parts: Vec<&str> = path
        .trim_matches('/')
        .split('/')
        .filter(|p| !p.is_empty())
        .collect();
    let mut out = Vec::new();
    for end in 1..=parts.len() {
        out.push(parts[..end].join("/"));
    }
    out
}

async fn publish(sender: &GossipSender, message: GossipMessage) {
    tracing::debug!("gossip send {}", summarize_message(&message));
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        sender.broadcast(message.to_bytes().into()),
    )
    .await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::warn!("gossip publish failed: {err}"),
        Err(_) => tracing::warn!("gossip publish timed out — connection likely dead"),
    }
}

async fn handle_gossip_event(
    event: Result<GossipEvent, ApiError>,
    rpc_client: rpc::RpcClient,
    state: Arc<RwLock<FolderState>>,
    group: Arc<RwLock<GroupState>>,
    root_reports: Arc<RwLock<RootReports>>,
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
            if let Some(snapshot) =
                merge_remote_gc_watermark(Arc::clone(&state), message_gc_watermark(&message)).await
            {
                publish_sync_state(sender, &snapshot, local_origin).await;
            }
            let local = {
                let state = state.read().await;
                StateSnapshot::from_state(&state)
            };
            print_remote_message(&message, &local);
            record_root_report(Arc::clone(&root_reports), &message).await;
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
                    maybe_gc_tombstones(
                        Arc::clone(&state),
                        Arc::clone(&group),
                        Arc::clone(&root_reports),
                        sender,
                        local_origin,
                    )
                    .await;
                }
                _ => maybe_probe_remote_rpc(
                    rpc_client,
                    message,
                    state,
                    Arc::clone(&group),
                    root_reports,
                    sender.clone(),
                    local_origin.to_string(),
                ),
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

fn message_gc_watermark(message: &GossipMessage) -> Option<&GcWatermark> {
    match message {
        GossipMessage::SyncState { gc_watermark, .. } => Some(gc_watermark),
        GossipMessage::FilesystemChanged { .. } | GossipMessage::Peers { .. } => None,
    }
}

async fn merge_remote_gc_watermark(
    state: Arc<RwLock<FolderState>>,
    gc_watermark: Option<&GcWatermark>,
) -> Option<StateSnapshot> {
    let gc_watermark = gc_watermark?;
    if gc_watermark.is_empty() {
        return None;
    }

    let mut state = state.write().await;
    let (changed, pruned) = state.merge_gc_watermark(gc_watermark);
    if changed {
        tracing::debug!("gc watermark updated origins={}", gc_watermark.len());
    }
    if pruned == 0 {
        return None;
    }
    tracing::info!("gc watermark pruned {pruned} tombstones");
    state.save_entries();
    Some(StateSnapshot::from_state(&state))
}

async fn record_root_report(root_reports: Arc<RwLock<RootReports>>, message: &GossipMessage) {
    let Some((origin, report)) = root_report_from_message(message) else {
        return;
    };
    root_reports.write().await.update(origin, report);
}

fn root_report_from_message(message: &GossipMessage) -> Option<(String, RootReport)> {
    match message {
        GossipMessage::SyncState {
            origin,
            state_root,
            lamport,
            ..
        }
        | GossipMessage::FilesystemChanged {
            origin,
            state_root,
            lamport,
            ..
        } => Some((
            origin.clone(),
            RootReport {
                state_root: *state_root,
                lamport: *lamport,
            },
        )),
        GossipMessage::Peers { .. } => None,
    }
}

async fn maybe_gc_tombstones(
    state: Arc<RwLock<FolderState>>,
    group: Arc<RwLock<GroupState>>,
    root_reports: Arc<RwLock<RootReports>>,
    sender: &GossipSender,
    local_origin: &str,
) {
    let (local_root, active_peer_ids) = {
        let state = state.read().await;
        let group = group.read().await;
        (state.root_hash(), group.active_peer_ids())
    };
    let converged = root_reports
        .read()
        .await
        .all_active_match(local_root, &active_peer_ids);
    if !converged {
        return;
    }

    let snapshot = {
        let mut state = state.write().await;
        if state.root_hash() != local_root {
            return;
        }
        let pruned = state.gc_tombstones_for_converged_root();
        if pruned == 0 {
            return;
        }
        tracing::info!("gc converged root pruned {pruned} tombstones");
        state.save_entries();
        StateSnapshot::from_state(&state)
    };
    publish_sync_state(sender, &snapshot, local_origin).await;
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
            gc_watermark,
        } => {
            tracing::info!(
                "peer state origin={} lamport={} state_root={} live_root={} state_nodes={} live_nodes={} gc_origins={} state_match={} live_match={}",
                origin,
                lamport,
                hex(*state_root),
                hex(*live_root),
                state_nodes,
                live_nodes,
                gc_watermark.len(),
                *state_root == state.state_root,
                *live_root == state.live_root
            );
        }
        GossipMessage::FilesystemChanged {
            origin,
            state_root,
            live_root,
            lamport,
            hint,
        } => {
            let hint_nodes = hint.as_ref().map(|hint| hint.nodes.len()).unwrap_or(0);
            let hint_truncated = hint.as_ref().map(|hint| hint.truncated).unwrap_or(false);
            tracing::info!(
                "peer filesystem origin={} lamport={} hint_nodes={} hint_truncated={} state_root={} live_root={} state_match={} live_match={}",
                origin,
                lamport,
                hint_nodes,
                hint_truncated,
                hex(*state_root),
                hex(*live_root),
                *state_root == state.state_root,
                *live_root == state.live_root
            );
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
    group: Arc<RwLock<GroupState>>,
    root_reports: Arc<RwLock<RootReports>>,
    sender: GossipSender,
    local_origin: String,
) {
    let (origin, remote_state_root, remote_live_root, remote_lamport, hint, use_advertised_root) =
        match message {
            GossipMessage::SyncState {
                origin,
                state_root,
                live_root,
                lamport,
                ..
            } => (origin, state_root, live_root, lamport, None, false),
            GossipMessage::FilesystemChanged {
                origin,
                state_root,
                live_root,
                lamport,
                hint,
                ..
            } => (origin, state_root, live_root, lamport, hint, true),
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
            maybe_gc_tombstones(
                Arc::clone(&state),
                Arc::clone(&group),
                Arc::clone(&root_reports),
                &sender,
                &local_origin,
            )
            .await;
            return;
        }

        let result = if use_advertised_root {
            sync::reconcile_with_advertised_root(
                rpc_client,
                Arc::clone(&state),
                peer,
                remote_state_root,
                remote_live_root,
                remote_lamport,
                hint,
            )
            .await
        } else {
            sync::reconcile_with_peer(rpc_client, Arc::clone(&state), peer).await
        };

        match result {
            Ok(changes) if changes.is_empty() => {
                tracing::info!("sync peer={} no changes applied", peer);
                maybe_gc_tombstones(
                    Arc::clone(&state),
                    Arc::clone(&group),
                    Arc::clone(&root_reports),
                    &sender,
                    &local_origin,
                )
                .await;
            }
            Ok(changes) => {
                tracing::info!("sync peer={} applied {} changes", peer, changes.len());
                let snapshot = {
                    let state = state.read().await;
                    StateSnapshot::from_state(&state)
                };
                if let Err(err) = group
                    .write()
                    .await
                    .update_sync_lamport(&peer.to_string(), snapshot.lamport)
                {
                    tracing::warn!("update sync_lamport failed: {err}");
                }
                publish_sync_state(&sender, &snapshot, &local_origin).await;
                maybe_gc_tombstones(
                    Arc::clone(&state),
                    Arc::clone(&group),
                    Arc::clone(&root_reports),
                    &sender,
                    &local_origin,
                )
                .await;
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
        "invite command: lil invite {}",
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
            hint,
            ..
        } => {
            let hint_nodes = hint.as_ref().map(|hint| hint.nodes.len()).unwrap_or(0);
            format!("filesystem-changed origin={origin} lamport={lamport} hint_nodes={hint_nodes}")
        }
        GossipMessage::Peers { origin, members } => {
            format!("peers origin={origin} members={}", members.len())
        }
    }
}

fn create_invite(state_dir: &Path, expire_secs: u64) -> io::Result<()> {
    let secret_key = load_or_create_secret_key(&state_dir.join(KEY_FILE))?;
    let node_id = secret_key.public();
    let peers_path = state_dir.join(PEERS_FILE);
    let _group = GroupState::load_or_init(peers_path, node_id, None)?;
    let secret_bytes = group::generate_secret()?;
    let secret_hex = hex(secret_bytes);
    let expires_at = group::now_ms()? + expire_secs.saturating_mul(1000);
    group::add_invite(&state_dir.join(INVITES_FILE), &secret_hex, expires_at)?;
    println!("{}", encode_ticket(node_id, &secret_bytes));
    tracing::info!("invite expires in {}s", expire_secs);
    Ok(())
}

fn peers_cmd(state_dir: &Path) -> io::Result<()> {
    let secret_key = load_or_create_secret_key(&state_dir.join(KEY_FILE))?;
    let node_id = secret_key.public();
    let peers_path = state_dir.join(PEERS_FILE);
    let group = GroupState::load_or_init(peers_path, node_id, None)?;
    for member in group.members() {
        let label = match member.name.as_deref() {
            Some(n) => format!("{} ({})", member.id, n),
            None => member.id.clone(),
        };
        let marker = if member.id == node_id.to_string() { " [self]" } else { "" };
        println!("{:?} {}{}", member.status, label, marker);
    }
    Ok(())
}

fn remove_peer_cmd(state_dir: &Path, target: &str) -> io::Result<()> {
    let secret_key = load_or_create_secret_key(&state_dir.join(KEY_FILE))?;
    let node_id = secret_key.public();
    let peers_path = state_dir.join(PEERS_FILE);
    let mut group = GroupState::load_or_init(peers_path, node_id, None)?;
    match group.remove_peer(target)? {
        Some(id) => {
            println!("removed {id}");
            println!("restart the daemon to broadcast the removal to other peers");
        }
        None => {
            eprintln!("no peer found matching {target:?}");
            std::process::exit(1);
        }
    }
    Ok(())
}

async fn join_group(endpoint: &Endpoint, peers_path: &Path, ticket: &str, name: Option<String>) -> io::Result<()> {
    let ticket = parse_join_ticket(ticket)?;
    let rpc_client = rpc::RpcClient::new(endpoint.clone());
    let (topic_id, members) = rpc_client
        .join_group(ticket.issuer, ticket.secret, endpoint.id(), name)
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
                "another lil instance is already running on this folder",
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
                io::Error::other("another lil instance is already running on this folder")
            } else {
                err
            }
        })
}

fn parse_join_ticket(value: &str) -> io::Result<JoinTicket> {
    let bytes = ticket_base62_decode(value)?;
    let issuer = PublicKey::from_bytes(&bytes[..32].try_into().unwrap())
        .map_err(|err| io::Error::other(format!("invalid node id in ticket: {err}")))?;
    let secret_arr: [u8; 32] = bytes[32..].try_into().unwrap();
    let secret = hex(secret_arr);
    Ok(JoinTicket { issuer, secret })
}

fn encode_ticket(node_id: PublicKey, secret_bytes: &[u8; 32]) -> String {
    let mut combined = [0u8; 64];
    combined[..32].copy_from_slice(node_id.as_bytes());
    combined[32..].copy_from_slice(secret_bytes);
    ticket_base62_encode(&combined)
}

const BASE62_ALPHABET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const TICKET_CHARS: usize = 86;

fn ticket_base62_encode(bytes: &[u8; 64]) -> String {
    let mut n = *bytes;
    let mut digits = Vec::with_capacity(TICKET_CHARS);
    loop {
        let rem = b62_divmod(&mut n);
        digits.push(BASE62_ALPHABET[rem as usize]);
        if n.iter().all(|&b| b == 0) {
            break;
        }
    }
    while digits.len() < TICKET_CHARS {
        digits.push(BASE62_ALPHABET[0]);
    }
    digits.reverse();
    String::from_utf8(digits).unwrap()
}

fn ticket_base62_decode(s: &str) -> io::Result<[u8; 64]> {
    if s.len() != TICKET_CHARS {
        return Err(io::Error::other(format!(
            "invalid ticket: expected {} chars, got {}",
            TICKET_CHARS,
            s.len()
        )));
    }
    let mut result = [0u8; 64];
    for &ch in s.as_bytes() {
        let digit = b62_char_to_digit(ch)?;
        b62_mul_add(&mut result, digit);
    }
    Ok(result)
}

fn b62_divmod(bytes: &mut [u8]) -> u8 {
    let mut rem = 0u32;
    for b in bytes.iter_mut() {
        let val = rem * 256 + *b as u32;
        *b = (val / 62) as u8;
        rem = val % 62;
    }
    rem as u8
}

fn b62_mul_add(bytes: &mut [u8], digit: u8) {
    let mut carry = digit as u32;
    for b in bytes.iter_mut().rev() {
        let val = *b as u32 * 62 + carry;
        *b = (val & 0xFF) as u8;
        carry = val >> 8;
    }
}

fn b62_char_to_digit(ch: u8) -> io::Result<u8> {
    match ch {
        b'0'..=b'9' => Ok(ch - b'0'),
        b'A'..=b'Z' => Ok(ch - b'A' + 10),
        b'a'..=b'z' => Ok(ch - b'a' + 36),
        _ => Err(io::Error::other(format!(
            "invalid ticket char: {}",
            ch as char
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_ticket_roundtrips() {
        let issuer = SecretKey::from_bytes(&[7; 32]).public();
        let secret_bytes = [0xABu8; 32];
        let ticket_str = encode_ticket(issuer, &secret_bytes);
        assert_eq!(ticket_str.len(), TICKET_CHARS);
        let parsed = parse_join_ticket(&ticket_str).unwrap();
        assert_eq!(parsed.issuer, issuer);
        assert_eq!(parsed.secret, hex(secret_bytes));
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
            "another lil instance is already running on this folder"
        );
        drop(first);
        let _third = acquire_daemon_lock(tmp.path()).unwrap();
    }

    #[test]
    fn filesystem_tree_hint_stays_under_budget() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();

        let dir = tmp.path().join("a").join("b");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("c.txt"), "hello").unwrap();

        let changes = state.apply_paths(vec![tmp.path().join("a")]).unwrap();
        let snapshot = StateSnapshot::from_state(&state);
        let hint = build_tree_hint(&state, &changes, &snapshot, "node-a");

        assert!(hint.as_ref().is_some_and(|hint| !hint.nodes.is_empty()));
        assert!(
            filesystem_changed_len(&snapshot, "node-a", hint.as_ref())
                <= MAX_FILESYSTEM_CHANGED_BYTES
        );
    }
}
