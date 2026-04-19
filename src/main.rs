mod cache;
mod config;
mod daemon;
mod diagnostics;
mod gossip;
mod invite;
mod peers;
mod protocol;
mod snapshot;
mod sync_tree;
mod transport;

use clap::Parser;
use config::Config;
use daemon::{run, run_invite, run_join, run_remove};
use iroh::PublicKey;
use std::io;
use std::path::PathBuf;

#[derive(Parser, Debug, Clone)]
#[command(name = "tngl", about = "Sync a folder between nodes over iroh")]
struct Cli {
    #[arg(
        long,
        value_name = "PATH",
        help = "Folder to sync (state stored in <PATH>/.tngl/)"
    )]
    folder: PathBuf,

    #[arg(long = "show-id", help = "Print the node ID then exit")]
    show_id: bool,

    #[arg(
        long = "rescan",
        help = "Ignore the startup cache and rebuild folder state from disk"
    )]
    rescan: bool,

    #[arg(
        long = "sync-state-interval",
        value_name = "SECONDS",
        default_value = "10",
        help = "Broadcast SyncState every N seconds"
    )]
    sync_state_interval_secs: u64,

    #[arg(
        long = "invite",
        help = "Generate an invite ticket and exit (the running daemon will accept it)"
    )]
    invite: bool,

    #[arg(
        long = "expire",
        value_name = "SECONDS",
        default_value = "3600",
        help = "Invite token expiry in seconds (used with --invite)"
    )]
    expire: u64,

    #[arg(
        long = "join",
        value_name = "TICKET",
        help = "Join a peer using an invite ticket (<node_id>:<token>), then exit"
    )]
    join: Option<String>,

    #[arg(
        long = "remove-peer",
        value_name = "NODE_ID",
        help = "Mark a peer as removed, broadcast the updated member list, then exit"
    )]
    remove_peer: Option<PublicKey>,
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    if let Err(err) = run_cli() {
        tracing::error!("{err}");
        std::process::exit(1);
    }
}

fn run_cli() -> io::Result<()> {
    let cli = Cli::parse();
    let sync_dir = cli.folder;
    let state_dir = sync_dir.join(".tngl");
    std::fs::create_dir_all(&sync_dir)?;
    std::fs::create_dir_all(&state_dir)?;

    let config = Config {
        sync_dir,
        cache_path: state_dir.join("daemon.cache"),
        key_path: state_dir.join("iroh.key"),
        peers_path: state_dir.join("peers.json"),
        invites_path: state_dir.join("pending_invites.json"),
        show_id: cli.show_id,
        rescan: cli.rescan,
        sync_state_interval_secs: cli.sync_state_interval_secs,
        invite_expire_secs: cli.expire,
    };

    if cli.invite {
        return run_invite(config);
    }

    if let Some(peer_id) = cli.remove_peer {
        return run_remove(config, peer_id);
    }

    if let Some(ticket) = cli.join {
        run_join(config.clone(), &ticket)?;
        // Fall through to start the daemon after joining.
    }

    run(config)
}

