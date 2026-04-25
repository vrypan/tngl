use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

const STATE_DIR: &str = ".tngl";
const IGNORE_FILE_NAME: &str = ".notngl";
const IGNORED_FILE_NAMES: &[&str] = &[".DS_Store", "Thumbs.db", "Desktop.ini"];
const IGNORED_FILE_PREFIXES: &[&str] = &["._"];
const IGNORED_DIR_NAMES: &[&str] = &[
    ".Spotlight-V100",
    ".Trashes",
    ".fseventsd",
    "$RECYCLE.BIN",
    "lost+found",
];

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

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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

    pub fn node(&self, prefix: &str) -> Option<TreeNode> {
        self.tree.nodes.get(&normalize_prefix(prefix)).cloned()
    }

    pub fn entry(&self, path: &str) -> Option<Entry> {
        self.entries.get(path.trim_matches('/')).cloned()
    }

    pub fn object_path(&self, content_hash: [u8; 32]) -> Option<PathBuf> {
        self.entries.values().find_map(|entry| {
            if entry.kind == EntryKind::File && entry.content_hash == Some(content_hash) {
                Some(self.root.join(&entry.path))
            } else {
                None
            }
        })
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
    let ignore_patterns = load_ignore_patterns(root)?;
    scan_dir(root, root, &ignore_patterns, &mut entries)?;
    Ok(entries)
}

fn scan_dir(
    root: &Path,
    dir: &Path,
    ignore_patterns: &[IgnorePattern],
    entries: &mut BTreeMap<String, Entry>,
) -> io::Result<()> {
    let mut children = Vec::new();
    for child in fs::read_dir(dir)? {
        let child = child?;
        children.push(child.path());
    }
    children.sort();

    for path in children {
        let relative = relative_path(root, &path)?;
        if should_ignore(&relative, ignore_patterns) {
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
            scan_dir(root, &path, ignore_patterns, entries)?;
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

fn should_ignore(relative: &str, ignore_patterns: &[IgnorePattern]) -> bool {
    if relative == IGNORE_FILE_NAME {
        return false;
    }

    Path::new(relative).components().any(|component| {
        let name = component.as_os_str().to_string_lossy();
        should_ignore_component(&name)
    }) || matches_ignore_patterns(ignore_patterns, relative)
}

fn should_ignore_component(name: &str) -> bool {
    name == STATE_DIR
        || IGNORED_FILE_NAMES.contains(&name)
        || IGNORED_FILE_PREFIXES
            .iter()
            .any(|prefix| name.starts_with(prefix))
        || IGNORED_DIR_NAMES.contains(&name)
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

#[derive(Clone, Debug)]
struct IgnorePattern {
    pattern: String,
    negated: bool,
    directory_only: bool,
    anchored: bool,
    has_slash: bool,
}

fn load_ignore_patterns(root: &Path) -> io::Result<Vec<IgnorePattern>> {
    match fs::read_to_string(root.join(IGNORE_FILE_NAME)) {
        Ok(contents) => Ok(parse_ignore_patterns(&contents)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(err) => Err(err),
    }
}

fn parse_ignore_patterns(contents: &str) -> Vec<IgnorePattern> {
    let mut patterns = Vec::new();
    for raw_line in contents.lines() {
        let mut line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let negated = line.starts_with('!');
        if negated {
            line = &line[1..];
        }

        let directory_only = line.ends_with('/');
        if directory_only {
            line = line.trim_end_matches('/');
        }

        let anchored = line.starts_with('/');
        if anchored {
            line = &line[1..];
        }

        if line.is_empty() {
            continue;
        }

        patterns.push(IgnorePattern {
            pattern: line.to_string(),
            negated,
            directory_only,
            anchored,
            has_slash: line.contains('/'),
        });
    }
    patterns
}

fn matches_ignore_patterns(patterns: &[IgnorePattern], relative: &str) -> bool {
    let mut ignored = false;
    for pattern in patterns {
        if matches_ignore_pattern(pattern, relative) {
            ignored = !pattern.negated;
        }
    }
    ignored
}

fn matches_ignore_pattern(pattern: &IgnorePattern, relative: &str) -> bool {
    let targets = if pattern.directory_only {
        directory_candidates(relative)
    } else {
        vec![relative.to_string()]
    };

    for target in targets {
        if pattern.anchored {
            if glob_match(&pattern.pattern, &target) {
                return true;
            }
            continue;
        }

        if pattern.has_slash {
            for suffix in path_suffix_candidates(&target) {
                if glob_match(&pattern.pattern, suffix) {
                    return true;
                }
            }
        } else {
            for component in target.split('/') {
                if glob_match(&pattern.pattern, component) {
                    return true;
                }
            }
        }
    }
    false
}

fn directory_candidates(relative: &str) -> Vec<String> {
    let parts: Vec<&str> = relative.split('/').collect();
    let mut out = Vec::with_capacity(parts.len());
    for i in 0..parts.len() {
        out.push(parts[..=i].join("/"));
    }
    out
}

fn path_suffix_candidates(relative: &str) -> Vec<&str> {
    let mut suffixes = Vec::new();
    let mut start = 0usize;
    suffixes.push(relative);
    while let Some(pos) = relative[start..].find('/') {
        start += pos + 1;
        suffixes.push(&relative[start..]);
    }
    suffixes
}

fn glob_match(pattern: &str, text: &str) -> bool {
    glob_match_bytes(pattern.as_bytes(), text.as_bytes())
}

fn glob_match_bytes(pattern: &[u8], text: &[u8]) -> bool {
    if pattern.is_empty() {
        return text.is_empty();
    }
    if pattern.starts_with(b"**") {
        let rest = &pattern[2..];
        if glob_match_bytes(rest, text) {
            return true;
        }
        return !text.is_empty() && glob_match_bytes(pattern, &text[1..]);
    }
    match pattern[0] {
        b'*' => {
            let rest = &pattern[1..];
            if glob_match_bytes(rest, text) {
                return true;
            }
            !text.is_empty() && text[0] != b'/' && glob_match_bytes(pattern, &text[1..])
        }
        b'?' => !text.is_empty() && text[0] != b'/' && glob_match_bytes(&pattern[1..], &text[1..]),
        ch => !text.is_empty() && ch == text[0] && glob_match_bytes(&pattern[1..], &text[1..]),
    }
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

fn normalize_prefix(prefix: &str) -> String {
    prefix.trim_matches('/').to_string()
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
    fn scan_ignores_state_and_os_metadata_paths() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir(tmp.path().join(".tngl")).unwrap();
        fs::write(tmp.path().join(".tngl/private.key"), "secret").unwrap();
        fs::write(tmp.path().join(".DS_Store"), "metadata").unwrap();
        fs::write(tmp.path().join("Thumbs.db"), "metadata").unwrap();
        fs::write(tmp.path().join("Desktop.ini"), "metadata").unwrap();
        fs::write(tmp.path().join("._file.txt"), "metadata").unwrap();
        fs::create_dir(tmp.path().join("lost+found")).unwrap();
        fs::write(tmp.path().join("lost+found/orphan"), "metadata").unwrap();
        fs::create_dir(tmp.path().join(".Spotlight-V100")).unwrap();
        fs::write(tmp.path().join(".Spotlight-V100/store"), "metadata").unwrap();
        fs::write(tmp.path().join("keep.txt"), "keep").unwrap();

        let state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();

        assert!(state.entries.contains_key("keep.txt"));
        assert!(!state.entries.contains_key(".tngl"));
        assert!(!state.entries.contains_key(".DS_Store"));
        assert!(!state.entries.contains_key("Thumbs.db"));
        assert!(!state.entries.contains_key("Desktop.ini"));
        assert!(!state.entries.contains_key("._file.txt"));
        assert!(
            !state
                .entries
                .keys()
                .any(|path| path.starts_with("lost+found"))
        );
        assert!(
            !state
                .entries
                .keys()
                .any(|path| path.starts_with(".Spotlight-V100"))
        );
    }

    #[test]
    fn scan_respects_notngl_patterns() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(
            tmp.path().join(".notngl"),
            "ignored.txt\nlogs/\n*.tmp\n/sub/exact.txt\n",
        )
        .unwrap();
        fs::write(tmp.path().join("ignored.txt"), "ignored").unwrap();
        fs::write(tmp.path().join("keep.txt"), "keep").unwrap();
        fs::write(tmp.path().join("note.tmp"), "tmp").unwrap();
        fs::create_dir(tmp.path().join("logs")).unwrap();
        fs::write(tmp.path().join("logs/a.txt"), "log").unwrap();
        fs::create_dir(tmp.path().join("sub")).unwrap();
        fs::write(tmp.path().join("sub/exact.txt"), "exact").unwrap();
        fs::write(tmp.path().join("sub/keep.txt"), "keep").unwrap();

        let state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();

        assert!(state.entries.contains_key(".notngl"));
        assert!(state.entries.contains_key("keep.txt"));
        assert!(state.entries.contains_key("sub"));
        assert!(state.entries.contains_key("sub/keep.txt"));
        assert!(!state.entries.contains_key("ignored.txt"));
        assert!(!state.entries.contains_key("note.tmp"));
        assert!(!state.entries.contains_key("logs"));
        assert!(!state.entries.contains_key("logs/a.txt"));
        assert!(!state.entries.contains_key("sub/exact.txt"));
    }

    #[test]
    fn notngl_negation_reincludes_path() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join(".notngl"), "*.tmp\n!keep.tmp\n").unwrap();
        fs::write(tmp.path().join("drop.tmp"), "drop").unwrap();
        fs::write(tmp.path().join("keep.tmp"), "keep").unwrap();

        let state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();

        assert!(!state.entries.contains_key("drop.tmp"));
        assert!(state.entries.contains_key("keep.tmp"));
    }

    #[test]
    fn newly_ignored_path_becomes_tombstone() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("keep.txt"), "keep").unwrap();

        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        assert!(state.entries.contains_key("keep.txt"));

        fs::write(tmp.path().join(".notngl"), "keep.txt\n").unwrap();
        let changes = state.rescan().unwrap();

        assert!(changes.iter().any(|change| change.path == ".notngl"));
        let ignored = state.entries.get("keep.txt").unwrap();
        assert_eq!(ignored.kind, EntryKind::Tombstone);
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
