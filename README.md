# tngl

`tngl` syncs a folder between a small, trusted group of nodes over `iroh`.

It is designed for direct peer-to-peer sync between your own machines or other
trusted peers. Each node keeps a full local copy of the synced folder.

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

On startup, `tngl` prints:

- the local node ID
- a JSON `--peer` value you can use on another node

Example:

```bash
tngl node-id <NODE_ID>
tngl peer {"id":"<NODE_ID>","addrs":[...]}
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

## Two-Node Setup

Start the daemon on node A:

```bash
./target/release/tngl --folder /tmp/node-a
```

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
