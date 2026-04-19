
> [!WARNING]
> `tbgl` is experimental. **There may be bugs that could DELETE or EXPOSE
> your files.**

# tngl

`tngl` (pronounced *"tangle"*) syncs a folder between a small, **trusted** group of nodes. 

It is designed for direct peer-to-peer sync between your own machines or other
trusted peers. Each node keeps a full local copy of the synced folder. Any node
in the group can create a ticket that can be used by a new node to join.

`tngl` uses [iroh and iroh-gossip](https://www.iroh.computer) which allows end-t-end encrypted
connections over QUIC, and will work even if the nodes are behind a firewall: Iroh
offers relay nodes that facilitate the communication but are unable to see the
encrypted data passing through them. `tngl` could use private relays in the future, but this
is beyond the current scope of the project.

> [!NOTE]
> See [SECURITY.md](SECURITY.md) for a full threat model and known limitations.

## Build

Debug build:

```bash
cargo build
```

Release build:

```bash
cargo build --release
```

The binary name is `tngl`.

## Basic Usage

Start a node for a folder:

```bash
./target/release/tngl --folder /path/to/folder
```

State is stored inside the synced folder under:

```text
<folder>/.tngl/
```

This contains:

- `iroh.key`: node identity key
- `peers.json`: known peers
- `pending_invites.json`: invite tokens
- `daemon.cache`: startup cache

## Additional Node Setup

Generate an invite on node A:

```bash
./target/release/tngl --folder /tmp/node-a --invite
```

This prints a ticket of the form:

```text
<NODE_ID>:<TOKEN>
```

Join from node B:

```bash
./target/release/tngl --folder /tmp/node-b --join '<NODE_ID>:<TOKEN>'
```

`--join` performs the join handshake, saves the peer list to
`/tmp/node-b/.tngl/peers.json`, and then starts the daemon normally.

## Useful Flags

Print the local node ID and exit:

```bash
./target/release/tngl --folder /tmp/node-a --show-id
```

Force a full startup rescan by dropping the cache before startup:

```bash
./target/release/tngl --folder /tmp/node-a --rescan
```

Set a custom invite expiration in seconds:

```bash
./target/release/tngl --folder /tmp/node-a --invite --expire 600
```

Remove a peer from the membership ledger and broadcast the update:

```bash
./target/release/tngl --folder /tmp/node-a --remove-peer <NODE_ID>
```

> [!WARNING]
> A removed node can still listen to gossip messages exchanged by the rest
> of the nodes, but it can not read new file contents. This means a removed
> node may not be able to read new files, but will we aware of filenames
> and folder activity. `tngl` is designed to be used between **trusted**
> nodes.

Set how often `SyncState` is broadcast (default 10 seconds). Lower values
speed up repair at the cost of more gossip traffic:

```bash
./target/release/tngl --folder /tmp/node-a --sync-state-interval 30
```

## Ignore Rules

You can ignore paths by creating a file named:

```text
.notngl
```

inside the synced folder.

Example:

```text
files/
*.tmp
build/
!build/keep.txt
```

Supported rule features are intentionally gitignore-like:

- blank lines
- `#` comments
- `!` negation
- `*`, `?`, and `**`
- leading `/` for root-anchored patterns
- trailing `/` for directory patterns

When `.notngl` changes, `tngl` rescans the folder so newly ignored paths become
tombstones and newly unignored paths can reappear.

## Notes

- `.tngl/` is always ignored by sync; it also holds temporary files during
  in-flight transfers (`recv-*`), which are removed on completion or error.
- Live replication uses gossip announcements plus direct `GetFiles` pulls.
  File content is streamed over QUIC without buffering the whole file in memory;
  the blake3 hash is verified before the temp file is moved into place.
- If a targeted `GetFiles` fetch fails, the node falls back to a full Merkle
  tree sync (`GetNodes` walk) against the same peer.
- Repair is also driven by periodic `SyncState` broadcasts (default every
  10 seconds). Any node with a different root hash initiates a tree sync.
- Some OS metadata files are ignored automatically: `.DS_Store`, `Thumbs.db`,
  `Desktop.ini`, `._*`, `.Spotlight-V100`, and `$RECYCLE.BIN`.
- Empty directories are not tracked as first-class state. If the last file in a
  directory is deleted, the empty directory is removed on peers.

## Debugging

Log verbosity is controlled via the `RUST_LOG` environment variable.
The default level is `info`.

Show all operational events (default):

```bash
RUST_LOG=info ./target/release/tngl --folder /tmp/node-a
```

Show gossip and RPC traffic:

```bash
RUST_LOG=debug ./target/release/tngl --folder /tmp/node-a
```

Show only gossip or only RPC at debug level, everything else at info:

```bash
RUST_LOG=info,tngl::gossip=debug ./target/release/tngl --folder /tmp/node-a
RUST_LOG=info,tngl::rpc=debug   ./target/release/tngl --folder /tmp/node-a
```
