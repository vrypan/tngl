mod message;
mod state;
mod watcher;

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
use std::time::Duration;
use tokio::sync::mpsc;

const KEY_FILE: &str = "private.key";

#[derive(Parser, Debug)]
#[command(name = "tngl", about = "Monitor a folder and gossip tngl tree changes")]
struct Cli {
    #[arg(long, value_name = "PATH", help = "Folder to monitor")]
    folder: PathBuf,

    #[arg(long, value_name = "TICKET", help = "Join an existing gossip topic")]
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
struct Ticket {
    topic: TopicId,
    bootstrap: Vec<PublicKey>,
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
    let _lock = acquire_daemon_lock(&state_dir)?;

    let secret_key = load_or_create_secret_key(&state_dir.join(KEY_FILE))?;
    let endpoint = Endpoint::builder()
        .alpns(vec![GOSSIP_ALPN.to_vec()])
        .secret_key(secret_key)
        .bind()
        .await
        .map_err(io::Error::other)?;

    let local_origin = endpoint.id().to_string();
    let mut state = FolderState::new(cli.folder.clone(), local_origin.clone())?;
    let ticket = match cli.ticket.as_deref() {
        Some(ticket) => parse_ticket(ticket)?,
        None => Ticket {
            topic: new_topic_id()?,
            bootstrap: Vec::new(),
        },
    };

    print_start(&state, &endpoint, &ticket, cli.poll, cli.interval_ms);

    let (fs_tx, mut fs_rx) = mpsc::unbounded_channel();
    let _watcher = watcher::spawn(state.root().to_path_buf(), cli.interval_ms, cli.poll, fs_tx)?;

    let gossip = Gossip::builder().spawn(endpoint.clone());
    let _router = Router::builder(endpoint.clone())
        .accept(GOSSIP_ALPN, gossip.clone())
        .spawn();

    let topic = gossip
        .subscribe(ticket.topic, ticket.bootstrap.clone())
        .await
        .map_err(io::Error::other)?;
    let (sender, mut receiver) = topic.split();

    publish_sync_state(&sender, &state, &local_origin).await;

    let mut announce_interval =
        tokio::time::interval(Duration::from_secs(cli.announce_interval_secs.max(1)));
    announce_interval.tick().await;

    loop {
        tokio::select! {
            Some(()) = fs_rx.recv() => {
                let before_state = state.root_hash();
                let before_live = state.live_root_hash();
                match state.rescan() {
                    Ok(changes) if changes.is_empty() => {}
                    Ok(changes) => {
                        print_changes(before_state, before_live, &state, &changes);
                        publish_filesystem_changed(&sender, &state, &local_origin, &changes).await;
                    }
                    Err(err) => tracing::warn!("scan failed: {err}"),
                }
            }
            event = receiver.next() => {
                let Some(event) = event else {
                    return Err(io::Error::new(io::ErrorKind::BrokenPipe, "gossip stream closed"));
                };
                handle_gossip_event(event, &state, &local_origin)?;
            }
            _ = announce_interval.tick() => {
                publish_sync_state(&sender, &state, &local_origin).await;
            }
        }
    }
}

async fn publish_sync_state(sender: &GossipSender, state: &FolderState, origin: &str) {
    let message = GossipMessage::SyncState {
        origin: origin.to_string(),
        state_root: state.root_hash(),
        live_root: state.live_root_hash(),
        lamport: state.lamport(),
        state_nodes: state.tree().nodes.len(),
        live_nodes: state.live_tree().nodes.len(),
    };
    publish(sender, message).await;
}

async fn publish_filesystem_changed(
    sender: &GossipSender,
    state: &FolderState,
    origin: &str,
    changes: &[Change],
) {
    let message = GossipMessage::FilesystemChanged {
        origin: origin.to_string(),
        state_root: state.root_hash(),
        live_root: state.live_root_hash(),
        lamport: state.lamport(),
        changes: changes.iter().map(WireChange::from).collect(),
    };
    publish(sender, message).await;
}

async fn publish(sender: &GossipSender, message: GossipMessage) {
    tracing::debug!("gossip send {}", summarize_message(&message));
    if let Err(err) = sender.broadcast(message.to_bytes().into()).await {
        tracing::warn!("gossip publish failed: {err}");
    }
}

fn handle_gossip_event(
    event: Result<GossipEvent, ApiError>,
    state: &FolderState,
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
            print_remote_message(&message, state);
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

fn print_remote_message(message: &GossipMessage, state: &FolderState) {
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
                *state_root == state.root_hash(),
                *live_root == state.live_root_hash()
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
                *state_root == state.root_hash(),
                *live_root == state.live_root_hash()
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
    }
}

fn print_start(
    state: &FolderState,
    endpoint: &Endpoint,
    ticket: &Ticket,
    poll: bool,
    interval_ms: u64,
) {
    tracing::info!("node {}", endpoint.id());
    tracing::info!("ticket {}", format_ticket(ticket.topic, endpoint.id()));
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
    }
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

fn new_topic_id() -> io::Result<TopicId> {
    let mut bytes = [0_u8; 32];
    fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(TopicId::from_bytes(bytes))
}

fn parse_ticket(value: &str) -> io::Result<Ticket> {
    let (topic, peers) = value
        .split_once(':')
        .map_or((value, ""), |(topic, peers)| (topic, peers));
    let topic = topic
        .parse()
        .map_err(|err| io::Error::other(format!("invalid topic id in ticket: {err}")))?;
    let bootstrap = peers
        .split(',')
        .filter(|peer| !peer.is_empty())
        .map(|peer| {
            peer.parse()
                .map_err(|err| io::Error::other(format!("invalid peer id in ticket: {err}")))
        })
        .collect::<io::Result<Vec<_>>>()?;
    Ok(Ticket { topic, bootstrap })
}

fn format_ticket(topic: TopicId, local_id: PublicKey) -> String {
    format!("{}:{}", hex(*topic.as_bytes()), local_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ticket_roundtrips_topic_without_bootstrap() {
        let topic = new_topic_id().unwrap();
        let ticket = parse_ticket(&hex(*topic.as_bytes())).unwrap();
        assert_eq!(ticket.topic, topic);
        assert!(ticket.bootstrap.is_empty());
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
