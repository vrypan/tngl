use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    pub sync_dir: PathBuf,
    pub cache_path: PathBuf,
    pub key_path: PathBuf,
    pub peers_path: PathBuf,
    pub invites_path: PathBuf,
    pub show_id: bool,
    pub rescan: bool,
    pub sync_state_interval_secs: u64,
    pub invite_expire_secs: u64,
}
