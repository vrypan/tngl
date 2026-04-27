> [!WARNING]
> `lil` is **experimental**! There may be bugs that could DELETE or EXPOSE
> your files. I use it, but **use it at your own risk.**

# lil — a little tool to sync your files

`lil` syncs a folder between a small, **trusted** group of nodes.

- Designed for peer-to-peer sync between your own machines or other trusted
  peers.

- End-to-end encrypted: `lil` uses
  [iroh and iroh-gossip](https://www.iroh.computer) for end-to-end encrypted 
  connections over QUIC.

- Connections work even when nodes are behind NAT. Iroh relay servers
  facilitate the handshake but cannot see the encrypted data.

- No central node: every node is equal. Each keeps a full copy of the synced
  folder and each can invite new nodes.

> [!NOTE]
> See [SECURITY.md](SECURITY.md) for the full threat model and known limitations.

## Build

```bash
cargo build --release
```

The binary is `lil` (`target/release/lil`).

## Basic Usage

Start syncing a folder (creates a new single-node group on first run):

```bash
lil sync /path/to/folder
```

State is stored inside the synced folder under `.lil/`:

| File | Purpose |
|---|---|
| `private.key` | Node identity key |
| `daemon.lock` | Exclusive process lock |
| `peers.json` | Known peers and group membership |
| `invites.json` | Pending invite tokens |
| `entries.bin` | Persisted entry index (survives restarts) |
| `lamport` | Persisted Lamport clock |
| `gc-watermark.bin` | Persisted tombstone garbage-collection watermark |

## Adding a Second Node

Generate an invite on the first node:

```bash
lil invite /path/to/folder
```

This prints an 86-character base62 ticket. On the second node:

```bash
lil join /path/to/folder2 <ticket>
```

`join` completes the handshake and then starts the daemon.
Use `--exit` to join and exit instead, which is useful before starting `lil`
from a service manager.

## Subcommands

```
lil sync <folder> [--name <name>] [--poll] [--interval-ms <ms>]
                  [--announce-interval-secs <secs>]
lil invite <folder> [--expire-secs <secs>]
lil join <folder> <ticket> [--name <name>] [--exit]
lil peers <folder>
lil remove <folder> <id-or-name>
```

- `--name` sets a human-readable label for this node, visible to peers.
- `--poll` uses filesystem polling instead of native OS notifications.
- `--interval-ms` sets the watcher debounce window (default 500 ms).
- `--announce-interval-secs` sets how often `SyncState` is broadcast (default 10 s).
- `--expire-secs` sets invite lifetime (default 3600 s).
- `--exit` makes `join` stop after writing group state instead of starting the daemon.

## Ignore Rules

Create `.nolil` inside the synced folder to exclude paths:

```text
files/
*.tmp
build/
!build/keep.txt
```

Supported syntax (gitignore-like):

- blank lines and `#` comments are ignored
- `!` negates a rule
- `*`, `?`, `**` wildcards
- leading `/` anchors to the root
- trailing `/` matches directories only

When `.nolil` changes, `lil` rescans the folder. Newly ignored paths stop
being tracked locally; they are **not** deleted on remote peers.

## Notes

- `.lil/` is always excluded from sync. Temporary files for in-flight
  transfers (`recv-*`) are stored there and removed on completion or error.
- File content is streamed over QUIC without buffering the whole file in
  memory; BLAKE3 hash is verified before the temp file is renamed into place.
- Up to 8 file downloads run in parallel per reconciliation pass.
- Periodic `SyncState` broadcasts (default every 10 s) drive repair: any node
  with a different root hash initiates a Merkle tree sync. Filesystem-change
  gossip also includes a small bounded tree hint to reduce follow-up RPCs.
- Tombstones (records of deleted files) are persisted across restarts and
  garbage-collected after all active peers report the same state root. Nodes
  publish GC watermarks so stale tombstones are not accepted again later.
- Some OS metadata files are always ignored: `.DS_Store`, `Thumbs.db`,
  `Desktop.ini`, `._*`, `.Spotlight-V100`, `$RECYCLE.BIN`, `lost+found`.
- Empty directories are not tracked. If the last file in a directory is
  deleted, the empty directory is removed on peers.

## Debugging

Log verbosity is controlled via `RUST_LOG` (default: `info`):

```bash
RUST_LOG=info lil sync /tmp/node-a
RUST_LOG=debug lil sync /tmp/node-a
```
