use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

const STATE_DIR: &str = ".lil";
const IGNORE_FILE_NAME: &str = ".nolil";
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
        let state_dir = root.join(STATE_DIR);
        fs::create_dir_all(&state_dir)?;
        let saved_lamport = load_saved_lamport(&state_dir);
        let mut state = Self {
            root,
            origin,
            lamport: saved_lamport,
            entries: BTreeMap::new(),
            tree: TreeSnapshot::empty(),
            live_tree: TreeSnapshot::empty(),
        };
        state.rescan()?;
        state.save_lamport();
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

    pub fn should_accept_remote(&self, remote: &Entry) -> bool {
        self.entries
            .get(&remote.path)
            .map(|local| remote.version > local.version)
            .unwrap_or(true)
    }

    pub fn tmp_recv_path(&self, entry: &Entry) -> PathBuf {
        self.root.join(STATE_DIR).join(format!(
            "recv-{}-{}",
            entry.version.lamport,
            entry.path.replace('/', "_")
        ))
    }

    pub fn apply_remote_entry(
        &mut self,
        remote: Entry,
        object_tmp_path: Option<&Path>,
    ) -> io::Result<Option<Change>> {
        if !self.should_accept_remote(&remote) {
            if let Some(tmp) = object_tmp_path {
                let _ = fs::remove_file(tmp);
            }
            return Ok(None);
        }

        match remote.kind {
            EntryKind::File => {
                let tmp = object_tmp_path.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("missing tmp path for file {}", remote.path),
                    )
                })?;
                self.install_remote_file(&remote, tmp)?;
            }
            EntryKind::Dir => {
                fs::create_dir_all(self.root.join(&remote.path))?;
            }
            EntryKind::Tombstone => {
                remove_path_if_exists(&self.root.join(&remote.path))?;
            }
        }

        let path = remote.path.clone();
        self.lamport = self.lamport.max(remote.version.lamport);
        self.save_lamport();
        let old = self.entries.insert(path.clone(), remote.clone());

        // Tombstone all live descendants so the entries map stays consistent
        // with the filesystem after a directory deletion.
        let mut extra_changes = Vec::new();
        if remote.kind == EntryKind::Tombstone {
            let prefix = format!("{path}/");
            let children: Vec<String> = self
                .entries
                .keys()
                .filter(|p| p.starts_with(&prefix))
                .cloned()
                .collect();
            for child in children {
                let child_entry = self.entries.get(&child).cloned().unwrap();
                if child_entry.kind != EntryKind::Tombstone {
                    let version = self.next_version();
                    let new = tombstone_entry(&child, &child_entry, version);
                    self.entries.insert(child.clone(), new.clone());
                    extra_changes.push(Change { path: child, old: Some(child_entry), new });
                }
            }
        }

        let mut changed_paths: Vec<String> = vec![path.clone()];
        changed_paths.extend(extra_changes.iter().map(|c| c.path.clone()));
        update_tree_snapshot(&mut self.tree, &self.entries, &changed_paths, |_| true);
        update_tree_snapshot(&mut self.live_tree, &self.entries, &changed_paths, |e| {
            e.kind != EntryKind::Tombstone
        });

        let mut all_changes = vec![Change { path, old, new: remote }];
        all_changes.extend(extra_changes);
        Ok(Some(all_changes.remove(0)))
    }

    fn install_remote_file(&self, remote: &Entry, tmp_path: &Path) -> io::Result<()> {
        let dest = self.root.join(&remote.path);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::rename(tmp_path, &dest)?;

        #[cfg(unix)]
        if let Some(mode) = remote.mode {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&dest, fs::Permissions::from_mode(mode))?;
        }

        Ok(())
    }

    pub fn rescan(&mut self) -> io::Result<Vec<Change>> {
        let ignore_patterns = load_ignore_patterns(&self.root)?;
        let live = scan_folder(&self.root)?;
        let mut changes = Vec::new();
        let mut seen = BTreeSet::new();

        for (path, live_entry) in live {
            seen.insert(path.clone());
            self.update_entry(&path, live_entry, &mut changes);
        }

        let old_paths: Vec<String> = self.entries.keys().cloned().collect();
        for path in old_paths {
            if seen.contains(&path) {
                continue;
            }
            if should_ignore(&path, &ignore_patterns) {
                continue;
            }
            let Some(old) = self.entries.get(&path).cloned() else {
                continue;
            };
            if old.kind == EntryKind::Tombstone {
                continue;
            }
            let version = self.next_version();
            let new = tombstone_entry(&path, &old, version);
            self.entries.insert(path.clone(), new.clone());
            changes.push(Change { path, old: Some(old), new });
        }

        self.tree = derive_tree(&self.entries);
        self.live_tree = derive_live_tree(&self.entries);
        Ok(changes)
    }

    pub fn apply_paths(&mut self, abs_paths: Vec<PathBuf>) -> io::Result<Vec<Change>> {
        let canonical_paths: Vec<PathBuf> = abs_paths
            .into_iter()
            .filter_map(|p| {
                fs::canonicalize(&p).ok().or_else(|| {
                    p.parent()
                        .and_then(|parent| fs::canonicalize(parent).ok())
                        .map(|parent| parent.join(p.file_name().unwrap_or_default()))
                })
            })
            .collect();

        if canonical_paths.iter().any(|p| p == &self.root.join(IGNORE_FILE_NAME)) {
            return self.rescan();
        }

        let ignore_patterns = load_ignore_patterns(&self.root)?;
        let mut changes = Vec::new();

        for abs_path in canonical_paths {
            let relative = match relative_path(&self.root, &abs_path) {
                Ok(r) if !r.is_empty() => r,
                _ => continue,
            };
            self.apply_one_path(&abs_path, &relative, &ignore_patterns, &mut changes)?;
        }

        if !changes.is_empty() {
            let changed_paths: Vec<String> = changes.iter().map(|c| c.path.clone()).collect();
            update_tree_snapshot(&mut self.tree, &self.entries, &changed_paths, |_| true);
            update_tree_snapshot(&mut self.live_tree, &self.entries, &changed_paths, |e| {
                e.kind != EntryKind::Tombstone
            });
            self.save_lamport();
        }

        Ok(changes)
    }

    fn apply_one_path(
        &mut self,
        abs_path: &Path,
        relative: &str,
        ignore_patterns: &[IgnorePattern],
        changes: &mut Vec<Change>,
    ) -> io::Result<()> {
        if should_ignore(relative, ignore_patterns) {
            return Ok(());
        }

        match fs::symlink_metadata(abs_path) {
            Ok(metadata) if metadata.is_file() => {
                let live = Entry {
                    path: relative.to_string(),
                    kind: EntryKind::File,
                    content_hash: Some(hash_file(abs_path)?),
                    size: metadata.len(),
                    mode: mode(&metadata),
                    version: placeholder_version(),
                };
                self.update_entry(relative, live, changes);
            }
            Ok(metadata) if metadata.is_dir() => {
                let live = Entry {
                    path: relative.to_string(),
                    kind: EntryKind::Dir,
                    content_hash: None,
                    size: 0,
                    mode: mode(&metadata),
                    version: placeholder_version(),
                };
                self.update_entry(relative, live, changes);
                let mut dir_entries = BTreeMap::new();
                scan_dir(&self.root, abs_path, ignore_patterns, &mut dir_entries)?;
                for (path, live) in dir_entries {
                    self.update_entry(&path, live, changes);
                }
            }
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                self.tombstone_descendants(relative, changes);
            }
            Err(err) => return Err(err),
        }
        Ok(())
    }

    fn update_entry(&mut self, path: &str, live: Entry, changes: &mut Vec<Change>) {
        let existing = self.entries.get(path).cloned();
        if existing.as_ref().map_or(false, |o| same_observed_state(o, &live)) {
            return;
        }
        let mut new = live;
        new.version = self.next_version();
        self.entries.insert(path.to_string(), new.clone());
        changes.push(Change { path: path.to_string(), old: existing, new });
    }

    fn tombstone_descendants(&mut self, relative: &str, changes: &mut Vec<Change>) {
        let prefix = format!("{relative}/");
        let to_tombstone: Vec<String> = self
            .entries
            .keys()
            .filter(|p| **p == relative || p.starts_with(&prefix))
            .cloned()
            .collect();
        for path in to_tombstone {
            let old = self.entries.get(&path).cloned().unwrap();
            if old.kind == EntryKind::Tombstone {
                continue;
            }
            let version = self.next_version();
            let new = tombstone_entry(&path, &old, version);
            self.entries.insert(path.clone(), new.clone());
            changes.push(Change { path, old: Some(old), new });
        }
    }

    fn next_version(&mut self) -> Version {
        self.lamport += 1;
        Version {
            lamport: self.lamport,
            origin: self.origin.clone(),
        }
    }

    fn save_lamport(&self) {
        let path = self.root.join(STATE_DIR).join("lamport");
        let _ = fs::write(path, self.lamport.to_le_bytes());
    }
}

fn load_saved_lamport(state_dir: &Path) -> u64 {
    fs::read(state_dir.join("lamport"))
        .ok()
        .and_then(|b| b.try_into().ok())
        .map(u64::from_le_bytes)
        .unwrap_or(0)
}

fn remove_path_if_exists(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => fs::remove_dir_all(path),
        Ok(_) => fs::remove_file(path),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
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

const LEGACY_STATE_DIRS: &[&str] = &[".tngl"];

fn should_ignore_component(name: &str) -> bool {
    name == STATE_DIR || LEGACY_STATE_DIRS.contains(&name)
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

fn tombstone_entry(path: &str, old: &Entry, version: Version) -> Entry {
    Entry {
        path: path.to_string(),
        kind: EntryKind::Tombstone,
        content_hash: None,
        size: 0,
        mode: old.mode,
        version,
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

fn update_tree_snapshot(
    snapshot: &mut TreeSnapshot,
    entries: &BTreeMap<String, Entry>,
    changed_paths: &[String],
    include: impl Fn(&Entry) -> bool,
) {
    // Collect dirty node paths: ancestors of every changed path, plus the
    // changed paths themselves in case they are dir nodes.
    let mut dirty: BTreeSet<String> = BTreeSet::new();
    for path in changed_paths {
        dirty.insert(path.clone());
        dirty.extend(ancestors(path));
    }

    // When a dir node is removed, all its descendant nodes are also stale.
    for path in changed_paths {
        if !snapshot.nodes.contains_key(path) {
            continue;
        }
        let still_included = entries.get(path).map_or(false, |e| include(e));
        if still_included {
            continue;
        }
        let prefix = format!("{path}/");
        let descendants: Vec<String> = snapshot
            .nodes
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
            .collect();
        for desc in descendants {
            dirty.extend(ancestors(&desc));
            dirty.insert(desc);
        }
    }

    // Index entries by their parent node path for O(1) lookup per node.
    let mut by_parent: BTreeMap<String, Vec<&Entry>> = BTreeMap::new();
    for entry in entries.values() {
        if include(entry) {
            by_parent
                .entry(parent_path(&entry.path))
                .or_default()
                .push(entry);
        }
    }

    // Process dirty nodes bottom-up (reverse lex = deepest first) so that
    // when a parent reads its children hashes they are already up to date.
    for node_path in dirty.iter().rev() {
        let direct_entries: BTreeMap<String, [u8; 32]> = by_parent
            .get(node_path)
            .map(|es| {
                es.iter()
                    .map(|e| (basename(&e.path), hash_entry(e)))
                    .collect()
            })
            .unwrap_or_default();

        let direct_children: BTreeMap<String, [u8; 32]> = snapshot
            .nodes
            .iter()
            .filter(|(k, _)| !k.is_empty() && &parent_path(k) == node_path)
            .map(|(k, v)| (basename(k), v.hash))
            .collect();

        if node_path.is_empty() || !direct_entries.is_empty() || !direct_children.is_empty() {
            let prefix = display_prefix(node_path);
            let hash = hash_node(&prefix, &direct_entries, &direct_children);
            snapshot.nodes.insert(
                node_path.clone(),
                TreeNode {
                    prefix,
                    entries: direct_entries,
                    children: direct_children,
                    hash,
                },
            );
        } else {
            snapshot.nodes.remove(node_path);
        }
    }

    snapshot.root_hash = snapshot.nodes.get("").map(|n| n.hash).unwrap_or([0; 32]);
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
    encode_str(&mut hasher, "lil-entry-v1");
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
    hasher.update(b"lil-node-v1");
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
        fs::create_dir(tmp.path().join(".lil")).unwrap();
        fs::write(tmp.path().join(".lil/private.key"), "secret").unwrap();
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
        assert!(!state.entries.contains_key(".lil"));
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
    fn scan_respects_nolil_patterns() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(
            tmp.path().join(".nolil"),
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

        assert!(state.entries.contains_key(".nolil"));
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
    fn nolil_negation_reincludes_path() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join(".nolil"), "*.tmp\n!keep.tmp\n").unwrap();
        fs::write(tmp.path().join("drop.tmp"), "drop").unwrap();
        fs::write(tmp.path().join("keep.tmp"), "keep").unwrap();

        let state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();

        assert!(!state.entries.contains_key("drop.tmp"));
        assert!(state.entries.contains_key("keep.tmp"));
    }

    #[test]
    fn newly_ignored_path_stays_in_state() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("keep.txt"), "keep").unwrap();

        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        assert!(state.entries.contains_key("keep.txt"));

        fs::write(tmp.path().join(".nolil"), "keep.txt\n").unwrap();
        let changes = state.rescan().unwrap();

        // .nolil changed but keep.txt should remain live — ignore stops local
        // tracking without broadcasting a deletion to peers.
        assert!(changes.iter().any(|c| c.path == ".nolil"));
        let entry = state.entries.get("keep.txt").unwrap();
        assert_eq!(entry.kind, EntryKind::File);
    }

    #[test]
    fn applies_higher_version_remote_file() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        let bytes = b"remote\n";
        let remote = Entry {
            path: "remote.txt".to_string(),
            kind: EntryKind::File,
            content_hash: Some(*blake3::hash(bytes).as_bytes()),
            size: bytes.len() as u64,
            mode: None,
            version: Version {
                lamport: 10,
                origin: "node-b".to_string(),
            },
        };

        let tmp_path = state.tmp_recv_path(&remote);
        fs::write(&tmp_path, bytes).unwrap();
        let change = state
            .apply_remote_entry(remote, Some(&tmp_path))
            .unwrap()
            .unwrap();

        assert_eq!(change.verb(), "file new");
        assert_eq!(fs::read(tmp.path().join("remote.txt")).unwrap(), bytes);
        assert_eq!(state.entry("remote.txt").unwrap().version.lamport, 10);
        assert_eq!(state.lamport(), 10);
    }

    #[test]
    fn applies_higher_version_remote_tombstone() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("gone.txt"), "local").unwrap();
        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        let remote = Entry {
            path: "gone.txt".to_string(),
            kind: EntryKind::Tombstone,
            content_hash: None,
            size: 0,
            mode: None,
            version: Version {
                lamport: 10,
                origin: "node-b".to_string(),
            },
        };

        let change = state.apply_remote_entry(remote, None).unwrap().unwrap();

        assert_eq!(change.verb(), "delete");
        assert!(!tmp.path().join("gone.txt").exists());
        assert_eq!(state.entry("gone.txt").unwrap().kind, EntryKind::Tombstone);
    }

    #[test]
    fn apply_paths_handles_create_modify_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        let empty_hash = state.root_hash();

        let file = tmp.path().join("a.txt");
        fs::write(&file, "hello").unwrap();
        let changes = state.apply_paths(vec![file.clone()]).unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].verb(), "file new");
        assert_ne!(state.root_hash(), empty_hash);

        fs::write(&file, "world").unwrap();
        let changes = state.apply_paths(vec![file.clone()]).unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].verb(), "file update");

        fs::remove_file(&file).unwrap();
        let changes = state.apply_paths(vec![file]).unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].verb(), "delete");
        assert_eq!(changes[0].new.kind, EntryKind::Tombstone);
    }

    #[test]
    fn apply_paths_tombstones_dir_descendants_on_removal() {
        let tmp = tempfile::tempdir().unwrap();
        let sub = tmp.path().join("sub");
        fs::create_dir(&sub).unwrap();
        fs::write(sub.join("a.txt"), "a").unwrap();
        fs::write(sub.join("b.txt"), "b").unwrap();

        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();
        assert_eq!(state.entries.get("sub/a.txt").unwrap().kind, EntryKind::File);

        fs::remove_dir_all(&sub).unwrap();
        let changes = state.apply_paths(vec![sub]).unwrap();
        assert!(changes.len() >= 3); // sub, sub/a.txt, sub/b.txt
        for c in &changes {
            assert_eq!(c.new.kind, EntryKind::Tombstone);
        }
    }

    #[test]
    fn apply_paths_scans_new_directory_contents() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();

        let sub = tmp.path().join("sub");
        fs::create_dir(&sub).unwrap();
        fs::write(sub.join("c.txt"), "c").unwrap();

        let changes = state.apply_paths(vec![sub]).unwrap();
        let paths: Vec<&str> = changes.iter().map(|c| c.path.as_str()).collect();
        assert!(paths.contains(&"sub"));
        assert!(paths.contains(&"sub/c.txt"));
    }

    #[test]
    fn apply_paths_falls_back_to_rescan_when_nolil_changes() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("keep.txt"), "keep").unwrap();
        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();

        let notngl = tmp.path().join(".nolil");
        fs::write(&notngl, "keep.txt\n").unwrap();
        let changes = state.apply_paths(vec![notngl]).unwrap();

        assert!(changes.iter().any(|c| c.path == ".nolil"));
        // keep.txt is now ignored but stays live in state — no deletion broadcast
        assert_eq!(
            state.entries.get("keep.txt").unwrap().kind,
            EntryKind::File
        );
    }

    #[test]
    fn incremental_tree_matches_full_rebuild() {
        let tmp = tempfile::tempdir().unwrap();
        fs::create_dir(tmp.path().join("sub")).unwrap();
        fs::write(tmp.path().join("sub/a.txt"), "initial").unwrap();
        fs::write(tmp.path().join("b.txt"), "b").unwrap();

        let mut state = FolderState::new(tmp.path().to_path_buf(), "node-a".to_string()).unwrap();

        // Modify via apply_paths, then confirm rescan sees no further changes
        // (meaning incremental state is identical to a fresh scan).
        fs::write(tmp.path().join("sub/a.txt"), "modified").unwrap();
        state.apply_paths(vec![tmp.path().join("sub/a.txt")]).unwrap();
        let incremental_root = state.root_hash();
        let incremental_live = state.live_root_hash();

        let further_changes = state.rescan().unwrap();
        assert!(further_changes.is_empty(), "rescan found unexpected changes after apply_paths");
        assert_eq!(state.root_hash(), incremental_root);
        assert_eq!(state.live_root_hash(), incremental_live);

        // Delete a file, then verify the same way.
        fs::remove_file(tmp.path().join("b.txt")).unwrap();
        state.apply_paths(vec![tmp.path().join("b.txt")]).unwrap();
        let after_delete_root = state.root_hash();
        let after_delete_live = state.live_root_hash();

        let further_changes = state.rescan().unwrap();
        assert!(further_changes.is_empty(), "rescan found unexpected changes after delete");
        assert_eq!(state.root_hash(), after_delete_root);
        assert_eq!(state.live_root_hash(), after_delete_live);
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
