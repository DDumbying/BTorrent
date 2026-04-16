# btorrent — Roadmap

## v1.1.0 — IPv6 + UDP connection ID caching

**IPv6 peer support**
- `AF_INET6` socket path in `tcp.c` and `dht.c`
- Compact6 format (18 bytes/peer) in tracker responses and PEX
- DHT bootstrap over IPv6
- `announce6` tracker parameter

**UDP tracker connection ID caching**
- Per-tracker `{ host, conn_id, expiry }` table
- Reuse IDs for 60 s per BEP 15 spec
- Eliminates the extra round-trip on every re-announce

---

## v1.2.0 — macOS / BSD port

- Replace `epoll` with a thin abstraction (`poll_add`, `poll_wait`, etc.)
- `kqueue` backend for macOS and BSD
- `epoll` backend for Linux (current behaviour unchanged)
- Homebrew formula

---

## v1.3.0 — Live status and scripting

**JSON/HTTP status API**
- Lightweight HTTP server (single extra thread)
- `GET /status` → JSON: progress, speed, ETA, peer list, ratio
- `GET /stop` → graceful shutdown
- Enables headless server monitoring and scripting

**`--magnet-to-torrent` flag**
- Fetch metadata only, write `.torrent` file, exit
- Useful for queueing downloads without starting them immediately

---

## v1.4.0 — TUI dashboard

- ncurses (or raw ANSI) live interface
- Peer table with per-peer speed and state
- Per-piece progress map
- Upload/download speed graph
- No ncurses dependency in non-TUI build (`--enable-tui` at compile time)

---

## Known limitations (all versions)

| Limitation | Impact |
|---|---|
| IPv4 only | Cannot connect to IPv6-only peers (~15% of swarm) |
| UDP conn ID not cached | Extra UDP round-trip per announce |
| Linux only (epoll) | Does not build on macOS or BSD |
| No DHT routing table persistence | Cold start on every run |
| No NAT-PMP / UPnP | Firewalled users need manual port forwarding |
| No encrypted transport (MSE/PE) | Some ISPs throttle unencrypted BitTorrent |
