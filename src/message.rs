use crate::group::MemberEntry;
use crate::state::{GcWatermark, TreeNode};

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
        #[serde(default)]
        gc_watermark: GcWatermark,
    },
    FilesystemChanged {
        origin: String,
        state_root: [u8; 32],
        live_root: [u8; 32],
        lamport: u64,
        hint: Option<TreeHint>,
    },
    Peers {
        origin: String,
        members: Vec<MemberEntry>,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TreeHint {
    pub truncated: bool,
    pub nodes: Vec<TreeNode>,
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
            Self::Peers { origin, .. } => origin,
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
            gc_watermark: GcWatermark::new(),
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
            hint: Some(TreeHint {
                truncated: false,
                nodes: Vec::new(),
            }),
        };

        let parsed = GossipMessage::from_bytes(&message.to_bytes()).unwrap();
        match parsed {
            GossipMessage::FilesystemChanged { lamport, hint, .. } => {
                assert_eq!(lamport, 8);
                assert!(hint.is_some());
            }
            _ => panic!("wrong message variant"),
        }
    }

    #[test]
    fn peers_roundtrips() {
        let message = GossipMessage::Peers {
            origin: "node-a".to_string(),
            members: vec![MemberEntry {
                id: "node-a".to_string(),
                name: Some("alice".to_string()),
                status: crate::group::MemberStatus::Active,
                lamport: 1,
                sync_lamport: 0,
            }],
        };

        let parsed = GossipMessage::from_bytes(&message.to_bytes()).unwrap();
        match parsed {
            GossipMessage::Peers { members, .. } => assert_eq!(members.len(), 1),
            _ => panic!("wrong message variant"),
        }
    }
}
