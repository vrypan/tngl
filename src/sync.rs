use crate::rpc::RpcClient;
use crate::state::{Change, EntryKind, FolderState, TreeNode, hex};
use iroh::PublicKey;
use std::collections::BTreeSet;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore};
use tokio::task::JoinSet;

const MAX_CONCURRENT_FETCHES: usize = 8;

pub async fn reconcile_with_peer(
    rpc: RpcClient,
    state: Arc<RwLock<FolderState>>,
    peer: PublicKey,
) -> io::Result<Vec<Change>> {
    let (remote_root, remote_live_root, remote_lamport) = rpc.get_root(peer).await?;
    tracing::info!(
        "sync peer={} remote_lamport={} state_root={} live_root={}",
        peer,
        remote_lamport,
        hex(remote_root),
        hex(remote_live_root)
    );

    let local_root = {
        let state = state.read().await;
        state.root_hash()
    };
    if local_root == remote_root {
        return Ok(Vec::new());
    }

    let mut changes = Vec::new();
    let mut stack = vec![("".to_string(), remote_root)];
    while let Some((prefix, remote_hash)) = stack.pop() {
        reconcile_node(
            &rpc,
            &state,
            peer,
            &prefix,
            remote_hash,
            &mut changes,
            &mut stack,
        )
        .await?;
    }
    Ok(changes)
}

async fn reconcile_node(
    rpc: &RpcClient,
    state: &Arc<RwLock<FolderState>>,
    peer: PublicKey,
    prefix: &str,
    expected_remote_hash: [u8; 32],
    changes: &mut Vec<Change>,
    stack: &mut Vec<(String, [u8; 32])>,
) -> io::Result<()> {
    let local_node = {
        let state = state.read().await;
        state.node(prefix)
    };
    if local_node
        .as_ref()
        .is_some_and(|node| node.hash == expected_remote_hash)
    {
        return Ok(());
    }

    let Some(remote_node) = rpc.get_node(peer, prefix).await? else {
        return Err(io::Error::other(format!(
            "peer {peer} did not return node for prefix {prefix:?}"
        )));
    };
    if remote_node.hash != expected_remote_hash {
        return Err(io::Error::other(format!(
            "peer {peer} returned stale node {}: expected {}, got {}",
            remote_node.prefix,
            hex(expected_remote_hash),
            hex(remote_node.hash)
        )));
    }

    reconcile_entries(
        rpc,
        state,
        peer,
        prefix,
        local_node.as_ref(),
        &remote_node,
        changes,
    )
    .await?;
    queue_changed_children(prefix, local_node.as_ref(), &remote_node, stack);
    Ok(())
}

async fn reconcile_entries(
    rpc: &RpcClient,
    state: &Arc<RwLock<FolderState>>,
    peer: PublicKey,
    prefix: &str,
    local_node: Option<&TreeNode>,
    remote_node: &TreeNode,
    changes: &mut Vec<Change>,
) -> io::Result<()> {
    // Phase 1: fetch metadata for all changed entries.
    let mut to_apply: Vec<crate::state::Entry> = Vec::new();
    for (name, remote_hash) in &remote_node.entries {
        if local_node
            .and_then(|node| node.entries.get(name))
            .is_some_and(|local_hash| local_hash == remote_hash)
        {
            continue;
        }
        let path = join_path(prefix, name);
        let Some(remote_entry) = rpc.get_entry(peer, &path).await? else {
            return Err(io::Error::other(format!(
                "peer {peer} did not return entry {path}"
            )));
        };
        if !state.read().await.should_accept_remote(&remote_entry) {
            continue;
        }
        to_apply.push(remote_entry);
    }

    // Phase 2: apply non-file entries immediately; download file entries in parallel.
    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_FETCHES));
    let mut join_set: JoinSet<io::Result<(crate::state::Entry, PathBuf)>> = JoinSet::new();

    for entry in to_apply {
        if entry.kind == EntryKind::File {
            let content_hash = entry.content_hash.ok_or_else(|| {
                io::Error::other(format!("remote file {} has no content hash", entry.path))
            })?;
            let tmp_path = state.read().await.tmp_recv_path(&entry);
            let rpc = rpc.clone();
            let sem = Arc::clone(&sem);
            let expected_size = entry.size;
            join_set.spawn(async move {
                let _permit = sem.acquire().await.map_err(io::Error::other)?;
                rpc.get_object_to_file(peer, content_hash, expected_size, &tmp_path)
                    .await?;
                Ok((entry, tmp_path))
            });
        } else {
            let mut state = state.write().await;
            if let Some(change) = state.apply_remote_entry(entry, None)? {
                log_applied(&change);
                changes.push(change);
            }
        }
    }

    // Phase 3: apply file entries as their downloads complete.
    while let Some(result) = join_set.join_next().await {
        let (entry, tmp_path) = result.map_err(io::Error::other)??;
        let mut state = state.write().await;
        if let Some(change) = state.apply_remote_entry(entry, Some(&tmp_path))? {
            log_applied(&change);
            changes.push(change);
        }
    }

    Ok(())
}

fn log_applied(change: &Change) {
    tracing::info!(
        "sync applied {} {} v{}:{}",
        change.verb(),
        change.path,
        change.new.version.lamport,
        change.new.version.origin
    );
}

fn queue_changed_children(
    prefix: &str,
    local_node: Option<&TreeNode>,
    remote_node: &TreeNode,
    stack: &mut Vec<(String, [u8; 32])>,
) {
    let child_names: BTreeSet<String> = remote_node.children.keys().cloned().collect();
    for name in child_names {
        let remote_hash = remote_node
            .children
            .get(&name)
            .copied()
            .expect("name came from remote children");
        if local_node
            .and_then(|node| node.children.get(&name))
            .is_some_and(|local_hash| *local_hash == remote_hash)
        {
            continue;
        }
        let child_prefix = join_path(prefix, &name);
        stack.push((child_prefix, remote_hash));
    }
}

fn join_path(prefix: &str, name: &str) -> String {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}/{name}")
    }
}
