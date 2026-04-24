# BTorrent
<img src="./assets/images/BTlow.png" align="left" width="128" hspace="10" vspace="10">

**A BitTorrent Client Built From Scratch in C**

A fully documented, educational BitTorrent client designed to teach you how peer-to-peer file sharing works at the lowest level.

**Links:** [GitHub](https://github.com/ddumbying/) · [Documentation](https://ddumbying.github.io/projects/btorrent/)

BTorrent is a from-scratch BitTorrent client written in C, built to expose the inner workings of peer-to-peer systems at a low level. Instead of relying on high-level libraries, the project implements the protocol manually - from parsing `.torrent` files and handling bencoded data, to managing concurrent TCP connections and exchanging pieces across a swarm.

The goal is not just to download files, but to understand how decentralized networks coordinate, verify data integrity using SHA-1, and distribute load across peers efficiently. Each component is intentionally simple, modular, and well-documented.

## Features

| Feature | Status | BEP |
|---------|--------|-----|
| Torrent file parsing | Done | BEP 3 |
| SHA-1 piece verification | Done | - |
| HTTP tracker | Done | BEP 3 |
| UDP tracker | Done | BEP 15 |
| Concurrent epoll scheduler | Done | - |
| Piece pipelining | Done | - |
| Rarest-first selection | Done | - |
| DHT peer discovery | Done | BEP 5 |
| Magnet links | Done | BEP 9/10 |
| Peer Exchange (PEX) | Done | BEP 11 |
| Seeding mode | Done | - |
| Rate limiting | Done | - |
| File locking | Done | - |

## Building

```bash
# Install dependency (Debian/Ubuntu)
sudo apt install libcurl4-openssl-dev

# Build
make          # Release build
make debug    # Debug build with AddressSanitizer
make test     # Run all tests
make clean    # Clean build artifacts
```

## Usage

```
btorrent 1.0.2 — BitTorrent client

Usage:
  btorrent -d <file.torrent> [options]   Download a torrent
  btorrent -i <file.torrent> [-j]        Inspect torrent metadata
  btorrent -c <file.torrent> [-o path]   Check an existing download
  btorrent -V                            Print version

Mode flags:
  -d, --download    Download the torrent
  -i, --inspect     Print metadata (no download)
  -c, --check       Verify an existing download
  -V, --version     Print version

Options:
  -o, --output <path>   Output directory  (default: torrent name)
  -p, --port   <N>      Listen port     (default: 6881)
  -n, --peers  <N>      Max peers       (default: 50)
  -t, --timeout <N>     Timeout (seconds) (default: 5)
  -P, --pipeline <N>    Pipeline depth   (default: 64)
  -S, --seed            Keep seeding after download
  -U, --ul <N>          Upload limit (KiB/s)
  -D, --dl <N>          Download limit (KiB/s)
  -q, --quiet           Minimal output (progress bar only)
  -v, --verbose         Verbose logging
  -l, --log <file>      Write log to file
  -j, --json            JSON output (inspect mode)
  -h, --help            Show this help
```

### Examples

```bash
# Download a torrent
btorrent -d ubuntu.torrent

# Download to specific directory with seeding
btorrent -d ubuntu.torrent -o ~/Downloads -S

# Inspect metadata
btorrent -i ubuntu.torrent

# Inspect with JSON
btorrent -i ubuntu.torrent -j | jq .

# Verify existing download
btorrent -c ubuntu.torrent -o ~/Downloads

# Download with verbose logging
btorrent -d ubuntu.torrent -v -l debug.log

# Magnet link
btorrent -d "magnet:?xt=urn:btih:..."
```

## Project Structure

```
btorrent/
├── include/
│   ├── cmd/cmd.h          Config & mode dispatch
│   ├── core/
│   │   ├── bencode.h      Bencoding parser
│   │   ├── magnet.h       Magnet link parser
│   │   ├── pieces.h       Piece manager
│   │   ├── sha1.h        SHA-1 implementation
│   │   └── torrent.h      Torrent metadata
│   ├── dht/dht.h         DHT peer discovery
│   ├── log.h             Structured logging
│   ├── net/tcp.h         Non-blocking TCP
│   ├── proto/
│   │   ├── ext.h         BEP 9/10 (metadata)
│   │   ├── peer.h        Peer wire protocol
│   │   └── tracker.h     Tracker communication
│   ├── result.h          Error types
│   ├── scheduler.h       Concurrent scheduler
│   └── utils.h           Utilities
├── src/
│   ├── cmd/              Mode implementations
│   ├── core/              Core BitTorrent logic
│   ├── dht/              DHT implementation
│   ├── net/              Network utilities
│   ├── proto/            Protocol implementations
│   ├── cmd_download.c    Download mode
│   ├── cmd_inspect.c     Inspect mode
│   ├── cmd_check.c       Check mode
│   ├── log.c             Logging
│   ├── main.c             Entry point
│   ├── result.c          Error messages
│   ├── scheduler.c       Peer scheduler
│   └── utils.c           Utilities
├── docs/                 Documentation
└── tests/                Test suites
```

## Documentation

Read the documentation in order:

1. `docs/chapters/01_bencoding.md` - BitTorrent's serialization format
2. `docs/chapters/02_torrent_file.md` - .torrent file structure
3. `docs/chapters/03_sha1.md` - SHA-1 hashing
4. `docs/chapters/04_tracker.md` - Tracker protocol
5. `docs/chapters/05_peer_protocol.md` - Peer wire protocol
6. `docs/chapters/06_pieces.md` - Piece management

## BitTorrent Specification References

- [BEP 3](https://www.bittorrent.org/beps/bep_0003.html) - The BitTorrent Protocol
- [BEP 5](https://www.bittorrent.org/beps/bep_0005.html) - DHT Protocol
- [BEP 9](https://www.bittorrent.org/beps/bep_0009.html) - Extension for Peers to Send Metadata Files
- [BEP 10](https://www.bittorrent.org/beps/bep_0010.html) - Extension Protocol
- [BEP 11](https://www.bittorrent.org/beps/bep_0011.html) - Peer Exchange (PEX)
- [BEP 15](https://www.bittorrent.org/beps/bep_0015.html) - UDP Tracker Protocol
- [BEP 20](https://www.bittorrent.org/beps/bep_0020.html) - Peer ID Conventions
