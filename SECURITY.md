# Security Assessment

`tngl` is designed for syncing folders between a small group of trusted nodes
(e.g. your own devices). This document describes the threat model and known
limitations.

## Threat Model

`tngl` is **not** designed to defend against a compromised group member, a
compromised machine, or a sophisticated adversary with physical access to any
node. It is designed to be safe against outsiders who have only network access.

## Attack Surface

### File content — strong protection

An outsider cannot read your files.

All connections use QUIC with TLS, authenticated by Ed25519 node keys. Passive
network sniffing yields nothing. The `GetFiles` RPC is additionally gated by an
allowlist built from the membership ledger in `peers.json`. An outsider who
connects has their request silently dropped at the application layer.

The realistic paths to file content are:

- **Invite token interception**: tokens are single-use and time-limited, but if
  transmitted insecurely (e.g. over plaintext chat) an attacker could use one
  before the intended recipient. Share invite tokens over an already-secure
  channel.
- **Key file theft**: stealing `.tngl/iroh.key` from a compromised machine
  allows impersonating that node. This requires physical or OS-level access.

### File metadata — partial exposure

Anyone who knows the gossip `topic_id` and any member's node ID can subscribe
to the gossip topic and receive `FileChanged` messages (file paths and blake3
content hashes) and `MemberList` messages (all member node IDs). They cannot
retrieve file content, but they learn what files exist and when they change.

The `topic_id` is not publicly advertised — it is only distributed through the
join handshake — but it is stored in plaintext in `.tngl/peers.json`. A
compromised machine exposes it.

Removed members retain the `topic_id` they already know and can continue to
observe gossip metadata indefinitely. They cannot access file content because
they are removed from the allowlist on all remaining nodes.

### Relay server visibility — low, by design

`tngl` uses [iroh](https://www.iroh.computer) relay servers for NAT traversal.
Relay servers forward encrypted QUIC packets and cannot decrypt them, but they
can observe which node IDs are communicating with each other. This is a
metadata leak to iroh's infrastructure that is inherent to the current
architecture. Support for private relay servers is out of scope for now.

### Denial of service — not mitigated

The iroh endpoint accepts inbound QUIC connections from any peer. Non-member
connections are dropped after reading the first frame, but there is no rate
limiting or connection cap. A targeted flood could exhaust file descriptors or
CPU. This is unlikely against a personal sync tool but is not mitigated.

## Summary

| Threat | Risk | Status |
|---|---|---|
| Passive network sniffing | None | QUIC/TLS encryption |
| Unauthorized file download | Very low | Allowlist on all RPCs |
| Invite token interception | Low–medium | Single-use, time-limited token; depends on how it is shared |
| Key file theft (iroh.key) | Low | Requires physical or OS-level access |
| Gossip metadata snooping | Low–medium | topic_id secrecy; no cryptographic membership gate on gossip |
| Relay metadata observation | Low | Architectural trade-off; accepted |
| DoS via connection flood | Low | Not mitigated |

## Files to Protect

| File | Secret | Consequence if leaked |
|---|---|---|
| `.tngl/iroh.key` | Node identity key | Attacker can impersonate this node |
| `.tngl/peers.json` | Contains `topic_id` and member node IDs | Attacker can subscribe to gossip and observe file metadata |
| Invite tokens | Printed to stdout at generation time | Attacker can join the group if used before the intended recipient |
