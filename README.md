# BTorrent
<img src="./assets/images/BT.png" align="left" width="128" hspace="10" vspace="10">
<b>A BitTorrent Client Built From Scratch in C</b>.
<br/>A fully documented, educational BitTorrent client designed to teach you how peer-to-peer file sharing works at the lowest level. <br/><br/>

**Links:**  [Alsullam](https://github.com/alsullam/), ~[Doc](https://github.com/alsullam.github.io/)~

BTorrent is a from-scratch BitTorrent client written in C.
built to expose the inner workings of peer-to-peer systems at a low level. Instead of relying on high-level libraries, the project focuses on implementing the protocol manually - from parsing `.torrent` files and handling bencoded data, to managing TCP connections and exchanging pieces between peers.

The goal is not just to download files, but to understand how decentralized networks coordinate, verify data integrity using SHA-1, and distribute load across a swarm efficiently. Each component is intentionally designed to be simple, modular, and well-documented, making it easy to follow the flow from metadata to actual data transfer.

BTorrent serves as both a learning resource and a systems-level exercise, helping you build intuition around networking, protocol design, and distributed architecture by implementing a real-world protocol step by step.

## 🎯 What You'll Learn

By reading and running this code, you will understand:

1. **Bencoding** - BitTorrent's custom serialization format
2. **Torrent files** - How `.torrent` metadata is structured
3. **SHA-1 Hashing** - How file integrity is verified piece by piece
4. **Tracker Protocol** - How clients find peers via HTTP
5. **Peer Protocol** - The TCP handshake and message wire format
6. **Piece Management** - How a file is split, downloaded, and reassembled
7. **Sockets & Networking** - Raw TCP/UDP in C

---

## 📁 Project Structure

```
btorrent/
├── README.md               ← You are here
├── Makefile                ← Build system
├── include/
│   ├── bencode.h           ← Bencoding parser API
│   ├── torrent.h           ← Torrent metadata structures
│   ├── sha1.h              ← SHA-1 hash implementation
│   ├── tracker.h           ← Tracker communication
│   ├── peer.h              ← Peer wire protocol
│   ├── pieces.h            ← Piece manager
│   └── utils.h             ← Utility helpers
├── src/
│   ├── main.c              ← Entry point & orchestration
│   ├── bencode.c           ← Bencoding parser
│   ├── torrent.c           ← .torrent file parser
│   ├── sha1.c              ← SHA-1 implementation (RFC 3174)
│   ├── tracker.c           ← HTTP tracker requests
│   ├── peer.c              ← Peer handshake & messaging
│   ├── pieces.c            ← Piece download manager
│   └── utils.c             ← Utilities
├── docs/
│   ├── 01_bencoding.md     ← Deep dive: Bencoding
│   ├── 02_torrent_file.md  ← Deep dive: .torrent structure
│   ├── 03_sha1.md          ← Deep dive: SHA-1 hashing
│   ├── 04_tracker.md       ← Deep dive: Tracker protocol
│   ├── 05_peer_protocol.md ← Deep dive: Peer wire protocol
│   └── 06_pieces.md        ← Deep dive: Piece management
└── tests/
    └── test_bencode.c      ← Unit tests for bencoding
```

---

## 🛠 Building

```bash
make          # Build the client
make test     # Run unit tests
make clean    # Remove build artifacts
```

## 🚀 Running

```bash
./btorrent <path/to/file.torrent>
```

---

## 📖 Recommended Reading Order

Start with the docs, then read the source code alongside them:

1. `docs/01_bencoding.md`    → then `src/bencode.c`
2. `docs/02_torrent_file.md` → then `src/torrent.c`
3. `docs/03_sha1.md`         → then `src/sha1.c`
4. `docs/04_tracker.md`      → then `src/tracker.c`
5. `docs/05_peer_protocol.md`→ then `src/peer.c`
6. `docs/06_pieces.md`       → then `src/pieces.c`

---

## 🔗 BitTorrent Specification References

- [BEP 0003](https://www.bittorrent.org/beps/bep_0003.html) - The BitTorrent Protocol Specification
- [BEP 0020](https://www.bittorrent.org/beps/bep_0020.html) - Peer ID Conventions
- [Bencoding Wikipedia](https://en.wikipedia.org/wiki/Bencode)
- [RFC 3174](https://tools.ietf.org/html/rfc3174) - SHA-1

---

## ⚠️ Educational Purpose

This client is built for learning. It implements the core BitTorrent protocol
features without optimizations like:
- DHT (Distributed Hash Table)
- PEX (Peer Exchange)
- uTP (Micro Transport Protocol)
- Magnet links

These are excellent next steps after you understand the fundamentals here.
