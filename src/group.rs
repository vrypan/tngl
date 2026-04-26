use crate::state::hex;
use iroh::PublicKey;
use iroh_gossip::TopicId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MemberStatus {
    Active,
    Removed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemberEntry {
    pub id: String,
    pub status: MemberStatus,
    pub lamport: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PeersFile {
    topic_id: String,
    #[serde(default)]
    members: Vec<MemberEntry>,
}

#[derive(Debug)]
pub struct GroupState {
    path: PathBuf,
    local_id: String,
    topic_id: TopicId,
    members: BTreeMap<String, MemberEntry>,
}

#[derive(Debug)]
pub struct MemberMerge {
    pub changed: bool,
    pub removed_self: bool,
    pub active_peers: Vec<PublicKey>,
}

impl GroupState {
    pub fn load_or_init(path: PathBuf, local_id: PublicKey, name: Option<String>) -> io::Result<Self> {
        let local_id = local_id.to_string();
        let loaded = match fs::read_to_string(&path) {
            Ok(contents) => Some(serde_json::from_str::<PeersFile>(&contents).map_err(|err| {
                io::Error::other(format!("invalid peers file {}: {err}", path.display()))
            })?),
            Err(err) if err.kind() == io::ErrorKind::NotFound => None,
            Err(err) => return Err(err),
        };

        let topic_id = match loaded.as_ref() {
            Some(file) => parse_topic_id(&file.topic_id)?,
            None => new_topic_id()?,
        };
        let mut members: BTreeMap<String, MemberEntry> = loaded
            .map(|file| {
                file.members
                    .into_iter()
                    .map(|entry| (entry.id.clone(), entry))
                    .collect()
            })
            .unwrap_or_default();

        let has_local = members.contains_key(&local_id);
        let local_name = members.get(&local_id).and_then(|e| e.name.clone());
        let name_changed = name.is_some() && local_name != name;
        if !has_local || name_changed {
            let lamport = next_lamport(&members);
            if !has_local {
                members.insert(
                    local_id.clone(),
                    MemberEntry {
                        id: local_id.clone(),
                        name,
                        status: MemberStatus::Active,
                        lamport,
                    },
                );
            } else {
                let entry = members.get_mut(&local_id).unwrap();
                entry.name = name;
                entry.lamport = lamport;
            }
        }

        let state = Self {
            path,
            local_id,
            topic_id,
            members,
        };
        state.persist()?;
        Ok(state)
    }

    pub fn replace(path: &Path, topic_id: TopicId, members: Vec<MemberEntry>) -> io::Result<()> {
        save_peers_file(path, topic_id, members)
    }

    pub fn topic_id(&self) -> TopicId {
        self.topic_id
    }

    pub fn members(&self) -> Vec<MemberEntry> {
        self.members.values().cloned().collect()
    }

    pub fn active_peers(&self) -> Vec<PublicKey> {
        active_peers_from_members(&self.local_id, self.members.values())
    }

    pub fn is_active_member(&self, peer: &PublicKey) -> bool {
        self.members
            .get(&peer.to_string())
            .is_some_and(|entry| entry.status == MemberStatus::Active)
    }

    pub fn add_active_peer(&mut self, peer: PublicKey, name: Option<String>) -> io::Result<bool> {
        let id = peer.to_string();
        let lamport = next_lamport(&self.members);
        let changed = match self.members.get_mut(&id) {
            Some(entry) if entry.status == MemberStatus::Active && (name.is_none() || entry.name == name) => false,
            Some(entry) => {
                entry.status = MemberStatus::Active;
                if name.is_some() {
                    entry.name = name;
                }
                entry.lamport = lamport;
                true
            }
            None => {
                self.members.insert(
                    id.clone(),
                    MemberEntry {
                        id,
                        name,
                        status: MemberStatus::Active,
                        lamport,
                    },
                );
                true
            }
        };
        if changed {
            self.persist()?;
        }
        Ok(changed)
    }

    pub fn remove_peer(&mut self, target: &str) -> io::Result<Option<String>> {
        let id = self
            .members
            .values()
            .find(|e| e.id == target || e.name.as_deref() == Some(target))
            .map(|e| e.id.clone());

        let Some(id) = id else {
            return Ok(None);
        };
        if id == self.local_id {
            return Err(io::Error::other("cannot remove self from the group"));
        }

        let lamport = next_lamport(&self.members);
        let entry = self.members.get_mut(&id).unwrap();
        entry.status = MemberStatus::Removed;
        entry.lamport = lamport;
        self.persist()?;
        Ok(Some(id))
    }

    pub fn merge_members(&mut self, incoming: Vec<MemberEntry>) -> io::Result<MemberMerge> {
        let mut changed = false;
        let mut removed_self = false;

        for entry in incoming {
            let apply = self
                .members
                .get(&entry.id)
                .is_none_or(|local| entry.lamport > local.lamport);
            if !apply {
                continue;
            }
            if entry.id == self.local_id && entry.status == MemberStatus::Removed {
                removed_self = true;
            }
            self.members.insert(entry.id.clone(), entry);
            changed = true;
        }

        if changed {
            self.persist()?;
        }

        Ok(MemberMerge {
            changed,
            removed_self,
            active_peers: self.active_peers(),
        })
    }

    fn persist(&self) -> io::Result<()> {
        save_peers_file(&self.path, self.topic_id, self.members())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct PendingInvite {
    secret_hash: String,
    expires_at_ms: u64,
}

pub fn add_invite(path: &Path, secret: &str, expires_at_ms: u64) -> io::Result<()> {
    let mut invites = load_invites(path)?;
    let now = now_ms()?;
    invites.retain(|invite| invite.expires_at_ms > now);
    invites.push(PendingInvite {
        secret_hash: hash_secret(secret),
        expires_at_ms,
    });
    save_invites(path, &invites)
}

pub fn consume_invite(path: &Path, secret: &str) -> io::Result<bool> {
    let mut invites = load_invites(path)?;
    let now = now_ms()?;
    let secret_hash = hash_secret(secret);
    let mut found = false;

    invites.retain(|invite| {
        if invite.expires_at_ms <= now {
            return false;
        }
        if invite.secret_hash == secret_hash {
            found = true;
            return false;
        }
        true
    });

    save_invites(path, &invites)?;
    Ok(found)
}

pub fn generate_secret() -> io::Result<[u8; 32]> {
    let mut bytes = [0u8; 32];
    fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(bytes)
}

pub fn now_ms() -> io::Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .map_err(io::Error::other)
}

fn active_peers_from_members<'a>(
    local_id: &str,
    members: impl IntoIterator<Item = &'a MemberEntry>,
) -> Vec<PublicKey> {
    members
        .into_iter()
        .filter(|entry| entry.id != local_id)
        .filter(|entry| entry.status == MemberStatus::Active)
        .filter_map(|entry| entry.id.parse().ok())
        .collect()
}

fn new_topic_id() -> io::Result<TopicId> {
    let mut bytes = [0_u8; 32];
    fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(TopicId::from_bytes(bytes))
}

fn parse_topic_id(value: &str) -> io::Result<TopicId> {
    value
        .parse()
        .map_err(|err| io::Error::other(format!("invalid stored topic id: {err}")))
}

fn save_peers_file(
    path: &Path,
    topic_id: TopicId,
    mut members: Vec<MemberEntry>,
) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    members.sort_by(|left, right| left.id.cmp(&right.id));
    let file = PeersFile {
        topic_id: hex(*topic_id.as_bytes()),
        members,
    };
    let json = serde_json::to_string_pretty(&file).map_err(io::Error::other)?;
    fs::write(path, json)
}

fn next_lamport(members: &BTreeMap<String, MemberEntry>) -> u64 {
    members
        .values()
        .map(|entry| entry.lamport)
        .max()
        .unwrap_or(0)
        + 1
}

fn load_invites(path: &Path) -> io::Result<Vec<PendingInvite>> {
    match fs::read_to_string(path) {
        Ok(contents) => serde_json::from_str(&contents).map_err(io::Error::other),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(err) => Err(err),
    }
}

fn save_invites(path: &Path, invites: &[PendingInvite]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_string_pretty(invites).map_err(io::Error::other)?;
    fs::write(path, json)
}

fn hash_secret(secret: &str) -> String {
    hex(*blake3::hash(secret.as_bytes()).as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invite_is_one_time() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("invites.json");

        add_invite(&path, "secret", now_ms().unwrap() + 10_000).unwrap();

        assert!(consume_invite(&path, "secret").unwrap());
        assert!(!consume_invite(&path, "secret").unwrap());
    }

    #[test]
    fn member_merge_keeps_newest_lamport() {
        let tmp = tempfile::tempdir().unwrap();
        let local = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let peer = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let mut state = GroupState::load_or_init(tmp.path().join("peers.json"), local, None).unwrap();

        let peer_id = peer.to_string();
        let update = state
            .merge_members(vec![MemberEntry {
                id: peer_id.clone(),
                name: None,
                status: MemberStatus::Active,
                lamport: 2,
            }])
            .unwrap();
        assert!(update.changed);
        assert_eq!(update.active_peers, vec![peer]);

        let update = state
            .merge_members(vec![MemberEntry {
                id: peer_id,
                name: None,
                status: MemberStatus::Removed,
                lamport: 1,
            }])
            .unwrap();
        assert!(!update.changed);
        assert_eq!(update.active_peers, vec![peer]);
    }

    #[test]
    fn name_propagates_via_load_or_init() {
        let tmp = tempfile::tempdir().unwrap();
        let local = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let path = tmp.path().join("peers.json");

        GroupState::load_or_init(path.clone(), local, Some("alice".to_string())).unwrap();
        let state = GroupState::load_or_init(path, local, None).unwrap();

        let entry = state.members().into_iter().find(|e| e.id == local.to_string()).unwrap();
        assert_eq!(entry.name.as_deref(), Some("alice"));
    }

    #[test]
    fn name_update_bumps_lamport() {
        let tmp = tempfile::tempdir().unwrap();
        let local = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let path = tmp.path().join("peers.json");

        let s1 = GroupState::load_or_init(path.clone(), local, Some("alice".to_string())).unwrap();
        let lamport1 = s1.members().into_iter().find(|e| e.id == local.to_string()).unwrap().lamport;

        let s2 = GroupState::load_or_init(path, local, Some("bob".to_string())).unwrap();
        let entry2 = s2.members().into_iter().find(|e| e.id == local.to_string()).unwrap();
        assert_eq!(entry2.name.as_deref(), Some("bob"));
        assert!(entry2.lamport > lamport1);
    }

    #[test]
    fn remove_peer_by_id_and_by_name() {
        let tmp = tempfile::tempdir().unwrap();
        let local = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let peer = iroh::SecretKey::from_bytes(&[2; 32]).public();
        let path = tmp.path().join("peers.json");
        let mut state = GroupState::load_or_init(path, local, None).unwrap();

        state.add_active_peer(peer, Some("carol".to_string())).unwrap();
        assert!(state.is_active_member(&peer));

        // remove by name
        let removed = state.remove_peer("carol").unwrap();
        assert_eq!(removed.as_deref(), Some(peer.to_string().as_str()));
        assert!(!state.is_active_member(&peer));

        // removing again returns None (already removed, but still finds the entry)
        state.add_active_peer(peer, None).unwrap();
        let removed = state.remove_peer(&peer.to_string()).unwrap();
        assert!(removed.is_some());
    }

    #[test]
    fn remove_self_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let local = iroh::SecretKey::from_bytes(&[1; 32]).public();
        let path = tmp.path().join("peers.json");
        let mut state = GroupState::load_or_init(path, local, None).unwrap();
        assert!(state.remove_peer(&local.to_string()).is_err());
    }
}
