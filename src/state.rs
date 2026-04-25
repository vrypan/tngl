use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EntryKind {
    File,
    Dir,
    Tombstone,
}

impl EntryKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Dir => "dir",
            Self::Tombstone => "tombstone",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Version {
    pub lamport: u64,
    pub origin: String,
}

impl Ord for Version {
    fn cmp(&self, other: &Self) -> Ordering {
        self.lamport
            .cmp(&other.lamport)
            .then_with(|| self.origin.cmp(&other.origin))
    }
}

impl PartialOrd for Version {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Entry {
    pub path: String,
    pub kind: EntryKind,
    pub content_hash: Option<[u8; 32]>,
    pub size: u64,
    pub mode: Option<u32>,
    pub version: Version,
}

#[derive(Debug, Clone)]
pub struct Change {
    pub path: String,
    pub old: Option<Entry>,
    pub new: Entry,
}

impl Change {
    pub fn verb(&self) -> &'static str {
        match (&self.old, self.new.kind) {
            (None, EntryKind::File) => "file new",
            (None, EntryKind::Dir) => "dir new",
            (None, EntryKind::Tombstone) => "tombstone new",
            (Some(_), EntryKind::Tombstone) => "delete",
            (Some(old), EntryKind::File) if old.kind == EntryKind::File => "file update",
            (Some(old), EntryKind::Dir) if old.kind == EntryKind::Dir => "dir update",
            (Some(_), EntryKind::File) => "file replace",
            (Some(_), EntryKind::Dir) => "dir replace",
        }
    }
}

#[derive(Debug, Clone)]
pub struct TreeNode {
    pub prefix: String,
    pub entries: BTreeMap<String, [u8; 32]>,
    pub children: BTreeMap<String, [u8; 32]>,
    pub hash: [u8; 32],
}

#[derive(Debug, Clone)]
pub struct TreeSnapshot {
    pub root_hash: [u8; 32],
    pub nodes: BTreeMap<String, TreeNode>,
}

#[derive(Debug)]
pub struct FolderState {
    root: PathBuf,
    origin: String,
    lamport: u64,
    entries: BTreeMap<String, Entry>,
    tree: TreeSnapshot,
    live_tree: TreeSnapshot,
}

impl FolderState {
    pub fn new(root: PathBuf, origin: String) -> io::Result<Self> {
        let root = fs::canonicalize(root)?;
        let mut state = Self {
            root,
            origin,
            lamport: 0,
            entries: BTreeMap::new(),
            tree: TreeSnapshot::empty(),
            live_tree: TreeSnapshot::empty(),
        };
        state.rescan()?;
        Ok(state)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn root_hash(&self) -> [u8; 32] {
        self.tree.root_hash
    }

    pub fn live_root_hash(&self) -> [u8; 32] {
        self.live_tree.root_hash
    }

    pub fn tree(&self) -> &TreeSnapshot {
        &self.tree
    }

    pub fn live_tree(&self) -> &TreeSnapshot {
        &self.live_tree
    }

    pub fn lamport(&self) -> u64 {
        self.lamport
    }

    pub fn rescan(&mut self) -> io::Result<Vec<Change>> {
        let live = scan_folder(&self.root)?;
        let mut changes = Vec::new();
        let mut seen = BTreeSet::new();

        for (path, live_entry) in live {
            seen.insert(path.clone());
            match self.entries.get(&path) {
                Some(old) if same_observed_state(old, &live_entry) => {}
                old => {
                    let old = old.cloned();
                    let mut new = live_entry;
                    new.version = self.next_version();
                    self.entries.insert(path.clone(), new.clone());
                    changes.push(Change { path, old, new });
                }
            }
        }

        let old_paths: Vec<String> = self.entries.keys().cloned().collect();
        for path in old_paths {
            if seen.contains(&path) {
                continue;
            }

            let Some(old) = self.entries.get(&path).cloned() else {
                continue;
            };
            if old.kind == EntryKind::Tombstone {
                continue;
            }

            let new = Entry {
                path: path.clone(),
                kind: EntryKind::Tombstone,
                content_hash: None,
                size: 0,
                mode: old.mode,
                version: self.next_version(),
            };
            self.entries.insert(path.clone(), new.clone());
            changes.push(Change {
                path,
                old: Some(old),
                new,
            });
        }

        self.tree = derive_tree(&self.entries);
        self.live_tree = derive_live_tree(&self.entries);
        Ok(changes)
    }

    fn next_version(&mut self) -> Version {
        self.lamport += 1;
        Version {
            lamport: self.lamport,
            origin: self.origin.clone(),
        }
    }
}

impl TreeSnapshot {
    fn empty() -> Self {
        let mut nodes = BTreeMap::new();
        let root = hash_node("/", &BTreeMap::new(), &BTreeMap::new());
        nodes.insert(
            String::new(),
            TreeNode {
                prefix: "/".to_string(),
                entries: BTreeMap::new(),
                children: BTreeMap::new(),
                hash: root,
            },
        );
        Self {
            root_hash: root,
            nodes,
        }
    }
}

fn scan_folder(root: &Path) -> io::Result<BTreeMap<String, Entry>> {
    let mut entries = BTreeMap::new();
    scan_dir(root, root, &mut entries)?;
    Ok(entries)
}

fn scan_dir(root: &Path, dir: &Path, entries: &mut BTreeMap<String, Entry>) -> io::Result<()> {
    let mut children = Vec::new();
    for child in fs::read_dir(dir)? {
        let child = child?;
        children.push(child.path());
    }
    children.sort();

    for path in children {
        let relative = relative_path(root, &path)?;
        if should_ignore(&relative) {
            continue;
        }

        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            continue;
        }

        let mode = mode(&metadata);
        if metadata.is_dir() {
            entries.insert(
                relative.clone(),
                Entry {
                    path: relative,
                    kind: EntryKind::Dir,
                    content_hash: None,
                    size: 0,
                    mode,
                    version: placeholder_version(),
                },
            );
            scan_dir(root, &path, entries)?;
        } else if metadata.is_file() {
            entries.insert(
                relative.clone(),
                Entry {
                    path: relative,
                    kind: EntryKind::File,
                    content_hash: Some(hash_file(&path)?),
                    size: metadata.len(),
                    mode,
                    version: placeholder_version(),
                },
            );
        }
    }

    Ok(())
}

fn should_ignore(relative: &str) -> bool {
    relative == ".tngl" || relative.starts_with(".tngl/")
}

fn relative_path(root: &Path, path: &Path) -> io::Result<String> {
    let relative = path
        .strip_prefix(root)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
    Ok(relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/"))
}

fn hash_file(path: &Path) -> io::Result<[u8; 32]> {
    let mut file = fs::File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(*hasher.finalize().as_bytes())
}

#[cfg(unix)]
fn mode(metadata: &fs::Metadata) -> Option<u32> {
    use std::os::unix::fs::PermissionsExt;
    Some(metadata.permissions().mode())
}

#[cfg(not(unix))]
fn mode(_metadata: &fs::Metadata) -> Option<u32> {
    None
}

fn placeholder_version() -> Version {
    Version {
        lamport: 0,
        origin: String::new(),
    }
}

fn same_observed_state(left: &Entry, right: &Entry) -> bool {
    left.path == right.path
        && left.kind == right.kind
        && left.content_hash == right.content_hash
        && left.size == right.size
        && left.mode == right.mode
}

fn derive_tree(entries: &BTreeMap<String, Entry>) -> TreeSnapshot {
    derive_tree_with(entries, |_| true)
}

fn derive_live_tree(entries: &BTreeMap<String, Entry>) -> TreeSnapshot {
    derive_tree_with(entries, |entry| entry.kind != EntryKind::Tombstone)
}

fn derive_tree_with(
    entries: &BTreeMap<String, Entry>,
    include: impl Fn(&Entry) -> bool,
) -> TreeSnapshot {
    let mut node_paths = BTreeSet::new();
    node_paths.insert(String::new());

    for entry in entries.values() {
        if !include(entry) {
            continue;
        }
        for ancestor in ancestors(&entry.path) {
            node_paths.insert(ancestor);
        }
        if entry.kind == EntryKind::Dir {
            node_paths.insert(entry.path.clone());
        }
    }

    let mut partials = BTreeMap::new();
    for path in &node_paths {
        partials.insert(
            path.clone(),
            TreeNode {
                prefix: display_prefix(path),
                entries: BTreeMap::new(),
                children: BTreeMap::new(),
                hash: [0; 32],
            },
        );
    }

    for entry in entries.values() {
        if !include(entry) {
            continue;
        }
        let parent = parent_path(&entry.path);
        let name = basename(&entry.path);
        if let Some(node) = partials.get_mut(&parent) {
            node.entries.insert(name, hash_entry(entry));
        }
    }

    let mut hashes = BTreeMap::new();
    for path in node_paths.iter().rev() {
        let child_paths: Vec<String> = node_paths
            .iter()
            .filter(|candidate| parent_path(candidate) == *path && !candidate.is_empty())
            .cloned()
            .collect();

        let mut children = BTreeMap::new();
        for child in child_paths {
            if let Some(hash) = hashes.get(&child) {
                children.insert(basename(&child), *hash);
            }
        }

        let node = partials
            .get_mut(path)
            .expect("node path came from the same set");
        node.children = children;
        node.hash = hash_node(&node.prefix, &node.entries, &node.children);
        hashes.insert(path.clone(), node.hash);
    }

    TreeSnapshot {
        root_hash: hashes.get("").copied().unwrap_or([0; 32]),
        nodes: partials,
    }
}

fn ancestors(path: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = parent_path(path);
    loop {
        out.push(current.clone());
        if current.is_empty() {
            break;
        }
        current = parent_path(&current);
    }
    out
}

fn parent_path(path: &str) -> String {
    path.rsplit_once('/')
        .map(|(parent, _)| parent.to_string())
        .unwrap_or_default()
}

fn basename(path: &str) -> String {
    path.rsplit_once('/')
        .map(|(_, name)| name.to_string())
        .unwrap_or_else(|| path.to_string())
}

fn display_prefix(path: &str) -> String {
    if path.is_empty() {
        "/".to_string()
    } else {
        format!("/{path}/")
    }
}

fn hash_entry(entry: &Entry) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    encode_str(&mut hasher, "tngl-entry-v1");
    encode_str(&mut hasher, &entry.path);
    encode_str(&mut hasher, entry.kind.as_str());
    encode_opt_hash(&mut hasher, entry.content_hash);
    hasher.update(&entry.size.to_be_bytes());
    encode_opt_u32(&mut hasher, entry.mode);
    hasher.update(&entry.version.lamport.to_be_bytes());
    encode_str(&mut hasher, &entry.version.origin);
    *hasher.finalize().as_bytes()
}

fn hash_node(
    prefix: &str,
    entries: &BTreeMap<String, [u8; 32]>,
    children: &BTreeMap<String, [u8; 32]>,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"tngl-node-v1");
    encode_str(&mut hasher, prefix);
    for (name, hash) in entries {
        encode_str(&mut hasher, "entry");
        encode_str(&mut hasher, name);
        hasher.update(hash);
    }
    for (name, hash) in children {
        encode_str(&mut hasher, "child");
        encode_str(&mut hasher, name);
        hasher.update(hash);
    }
    *hasher.finalize().as_bytes()
}

fn encode_str(hasher: &mut blake3::Hasher, value: &str) {
    let bytes = value.as_bytes();
    hasher.update(&(bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn encode_opt_hash(hasher: &mut blake3::Hasher, value: Option<[u8; 32]>) {
    match value {
        Some(value) => {
            hasher.update(&[1]);
            hasher.update(&value);
        }
        None => {
            hasher.update(&[0]);
        }
    };
}

fn encode_opt_u32(hasher: &mut blake3::Hasher, value: Option<u32>) {
    match value {
        Some(value) => {
            hasher.update(&[1]);
            hasher.update(&value.to_be_bytes());
        }
        None => {
            hasher.update(&[0]);
        }
    };
}

pub fn hex(hash: [u8; 32]) -> String {
    hash.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn records_create_update_delete_as_versions() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("a.txt");
        fs::write(&file, "one").unwrap();

        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        let initial_hash = state.root_hash();
        let initial_live_hash = state.live_root_hash();

        fs::write(&file, "two").unwrap();
        let changes = state.rescan().unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].verb(), "file update");
        assert_eq!(changes[0].new.version.lamport, 2);
        assert_ne!(state.root_hash(), initial_hash);
        assert_ne!(state.live_root_hash(), initial_live_hash);

        fs::remove_file(&file).unwrap();
        let changes = state.rescan().unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].verb(), "delete");
        assert_eq!(changes[0].new.kind, EntryKind::Tombstone);
        assert_eq!(changes[0].new.version.lamport, 3);
        assert_ne!(state.root_hash(), initial_hash);
    }

    #[test]
    fn derives_child_nodes_for_directories() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir(tmp.path().join("src")).unwrap();
        let mut file = fs::File::create(tmp.path().join("src/main.rs")).unwrap();
        writeln!(file, "fn main() {{}}").unwrap();

        let state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();

        let root = state.tree().nodes.get("").unwrap();
        assert!(root.entries.contains_key("src"));
        assert!(root.children.contains_key("src"));
        assert!(state.tree().nodes.contains_key("src"));
        assert_ne!(state.root_hash(), [0; 32]);
    }

    #[test]
    fn live_root_returns_to_initial_after_transient_path_is_removed() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("kept.txt"), "kept").unwrap();

        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        let initial_state_hash = state.root_hash();
        let initial_live_hash = state.live_root_hash();

        fs::create_dir(tmp.path().join("new")).unwrap();
        fs::write(tmp.path().join("new/a"), "temp").unwrap();
        state.rescan().unwrap();

        fs::remove_file(tmp.path().join("new/a")).unwrap();
        fs::remove_dir(tmp.path().join("new")).unwrap();
        state.rescan().unwrap();

        assert_ne!(state.root_hash(), initial_state_hash);
        assert_eq!(state.live_root_hash(), initial_live_hash);
    }
}
