use crate::state::{Change, EntryKind};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum GossipMessage {
    SyncState {
        origin: String,
        state_root: [u8; 32],
        live_root: [u8; 32],
        lamport: u64,
        state_nodes: usize,
        live_nodes: usize,
    },
    FilesystemChanged {
        origin: String,
        state_root: [u8; 32],
        live_root: [u8; 32],
        lamport: u64,
        changes: Vec<WireChange>,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WireChange {
    pub path: String,
    pub verb: String,
    pub entry_kind: EntryKind,
    pub content_hash: Option<[u8; 32]>,
    pub size: u64,
    pub version_lamport: u64,
    pub version_origin: String,
}

impl GossipMessage {
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("gossip message serialization should not fail")
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        serde_json::from_slice(bytes).ok()
    }

    pub fn origin(&self) -> &str {
        match self {
            Self::SyncState { origin, .. } => origin,
            Self::FilesystemChanged { origin, .. } => origin,
        }
    }
}

impl From<&Change> for WireChange {
    fn from(change: &Change) -> Self {
        Self {
            path: change.path.clone(),
            verb: change.verb().to_string(),
            entry_kind: change.new.kind,
            content_hash: change.new.content_hash,
            size: change.new.size,
            version_lamport: change.new.version.lamport,
            version_origin: change.new.version.origin.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sync_state_roundtrips() {
        let message = GossipMessage::SyncState {
            origin: "node-a".to_string(),
            state_root: [1; 32],
            live_root: [2; 32],
            lamport: 7,
            state_nodes: 3,
            live_nodes: 2,
        };

        let parsed = GossipMessage::from_bytes(&message.to_bytes()).unwrap();
        assert!(matches!(
            parsed,
            GossipMessage::SyncState { lamport: 7, .. }
        ));
    }

    #[test]
    fn filesystem_change_roundtrips() {
        let message = GossipMessage::FilesystemChanged {
            origin: "node-a".to_string(),
            state_root: [1; 32],
            live_root: [2; 32],
            lamport: 8,
            changes: vec![WireChange {
                path: "a.txt".to_string(),
                verb: "file new".to_string(),
                entry_kind: EntryKind::File,
                content_hash: Some([3; 32]),
                size: 10,
                version_lamport: 8,
                version_origin: "node-a".to_string(),
            }],
        };

        let parsed = GossipMessage::from_bytes(&message.to_bytes()).unwrap();
        match parsed {
            GossipMessage::FilesystemChanged { changes, .. } => assert_eq!(changes.len(), 1),
            _ => panic!("wrong message variant"),
        }
    }
}
