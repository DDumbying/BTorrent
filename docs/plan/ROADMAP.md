# BTorrent — Current State & Roadmap

## What we have right now

### Architecture (as-built)

```
btorrent/
├── include/
│   ├── core/        bencode.h  sha1.h  torrent.h  pieces.h
│   ├── proto/       peer.h  tracker.h
│   ├── log.h        structured logging (DEBUG/INFO/WARN/ERROR + timestamp)
│   ├── result.h     BtErr enum + BT_RESULT(T) typed returns
│   └── utils.h      xmalloc/xcalloc/xstrdup  random_bytes  byte-order helpers
├── src/
│   ├── core/        bencode.c  sha1.c  torrent.c  pieces.c
│   ├── proto/       peer.c  tracker.c
│   ├── log.c  result.c  utils.c  main.c
└── tests/unit/      test_sha1.c  test_peer.c  test_pieces.c
```

### What works correctly today

| Component | State |
|---|---|
| Bencode parser | Complete. Zero-copy string views, `bencode_parse_ex()` returns bytes consumed |
| SHA-1 | Complete. Passes all RFC 3174 vectors. Streaming and one-shot API |
| .torrent parsing | Complete. Single-file and multi-file. info_hash computed correctly |
| HTTP tracker | Complete. libcurl, compact peer list, failure reason handling |
| UDP tracker | Complete. BEP 15 two-step connect+announce, random transaction IDs from `/dev/urandom` |
| Multi-tracker fallback | Complete. BEP 12 tier iteration with exponential backoff retry |
| Peer wire protocol | Complete. Handshake, BITFIELD, HAVE, UNCHOKE, PIECE, REQUEST framing |
| Piece manager | Complete. SHA-1 verify, resume from disk, multi-file write via `pwrite()` |
| Logging | Complete. `LOG_*` macros, runtime level, optional log file |
| Error types | Complete. `BtErr` + `BT_RESULT(T)`, `bt_strerror()` |
| CLI | Partial. `--port --pipeline --timeout --verbose --log` work. Missing: `--max-peers`, `--seed`, `--check` |
| Tests | 40/40 passing. SHA-1 (6), peer wire (18), piece manager (16). Zero warnings |
| Build | Release (`-O2`) and debug (`-g3 -fsanitize=address,undefined`). `-MMD -MP` dep tracking |

### What does NOT work yet (honest gaps)

| Gap | Impact |
|---|---|
| **Serial peer loop** | One peer at a time. Throughput is capped at a single peer's upload speed. This is the #1 bottleneck |
| **No seeding** | After download completes the process exits. Cannot give back to the swarm |
| **No DHT** | Requires working tracker. Many modern torrents are tracker-less |
| **No magnet links** | Must have a .torrent file. Major usability gap |
| **No peer timeout config** | Hardcoded 5 s in `peer_connect`. `--timeout` flag parsed but not wired through |
| **No `--max-peers` flag** | Parsed but ignored. No concurrent peer limit |
| **Progress bar races with log lines** | `\r` progress overwritten by `LOG_INFO` lines during download |
| **No `btorrent inspect` subcommand** | Cannot examine a .torrent without downloading |
| **No install target** | `make install` absent. Binary stays in `build/` |

---

## Roadmap — ordered by impact on usability

---

### Phase 1 — CLI UX polish (1–2 days, no protocol changes)

**Goal:** feel like a real Unix tool before touching networking.

#### 1.1 Subcommand dispatch

Replace the single mode with proper subcommands:

```
btorrent download  file.torrent [--output DIR] [--port N] [--peers N] [--seed]
btorrent inspect   file.torrent
btorrent check     file.torrent [--output DIR]
btorrent version
```

Pattern in `main.c`:

```c
typedef struct { const char *name; int (*fn)(int, char**); } Subcommand;

static const Subcommand cmds[] = {
    { "download", cmd_download },
    { "inspect",  cmd_inspect  },
    { "check",    cmd_check    },
    { "version",  cmd_version  },
    { NULL, NULL }
};

int main(int argc, char *argv[]) {
    if (argc < 2) { usage(argv[0]); return 1; }
    for (int i = 0; cmds[i].name; i++)
        if (strcmp(argv[1], cmds[i].name) == 0)
            return cmds[i].fn(argc - 1, argv + 1);
    /* fallback: treat argv[1] as a .torrent file → download */
    return cmd_download(argc, argv);
}
```

This keeps `btorrent file.torrent` working (backward compatible) while enabling
`btorrent inspect file.torrent` as a first-class command.

#### 1.2 Wire `--timeout` and `--max-peers` through to peer_connect

`peer_connect` currently hardcodes 5 s. Add a timeout param:

```c
// peer.h
PeerConnResult peer_connect(const char *ip, uint16_t port,
                            const uint8_t *info_hash,
                            const uint8_t *peer_id,
                            int timeout_s);   // ← add this
```

#### 1.3 Separate progress bar from log lines

Log goes to stderr (or file). Progress bar goes to stdout only when stdout is a TTY:

```c
// In piece_manager_print_progress():
if (!isatty(STDOUT_FILENO)) return;   // skip in piped/redirected context
```

This makes `btorrent download f.torrent | tee log.txt` work cleanly.

#### 1.4 `btorrent inspect` — print torrent metadata without downloading

```c
static int cmd_inspect(int argc, char *argv[]) {
    // parse --json flag for machine-readable output
    TorrentInfo *t = torrent_parse(argv[1]);
    if (!t) return 1;
    torrent_print(t);   // existing function
    torrent_free(t);
    return 0;
}
```

Add a `--json` flag that emits a JSON object — useful for scripting:
```
btorrent inspect file.torrent --json | jq .name
```

#### 1.5 `btorrent check` — verify existing download without re-downloading

```c
static int cmd_check(int argc, char *argv[]) {
    // parse torrent + output path
    // create PieceManager (resume scan does the work)
    // print per-piece status: GOOD / MISSING
    // exit 0 if complete, 1 if incomplete
}
```

This is already implemented inside `piece_manager_new()` — expose it as a command.

#### 1.6 `make install` target

```makefile
PREFIX ?= /usr/local

install: all
	install -Dm755 build/btorrent $(DESTDIR)$(PREFIX)/bin/btorrent
	install -Dm644 README.md $(DESTDIR)$(PREFIX)/share/doc/btorrent/README.md

uninstall:
	rm -f $(DESTDIR)$(PREFIX)/bin/btorrent
```

---

### Phase 2 — Concurrent peers with epoll (3–5 days, biggest throughput win)

**Goal:** download from 30 peers simultaneously instead of 1.

The entire `download_from_peer` sequential loop in `main.c` is replaced by a
non-blocking event-driven scheduler. This is the single biggest change to make
the client usable on real torrents.

#### 2.1 Non-blocking connect

```c
// src/net/tcp.c  (new file)
int tcp_connect_nb(const char *ip, uint16_t port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    fcntl(sock, F_SETFL, O_NONBLOCK);
    // connect() returns -1 with errno==EINPROGRESS — that's fine
    struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = htons(port) };
    inet_pton(AF_INET, ip, &addr.sin_addr);
    connect(sock, (struct sockaddr*)&addr, sizeof(addr));  // EINPROGRESS OK
    return sock;
}
```

#### 2.2 Per-peer state machine

```c
// src/scheduler.c  (new file)
typedef enum {
    PS_CONNECTING,   // epoll EPOLLOUT fires when connect() completes
    PS_HANDSHAKE,    // sending/receiving 68-byte handshake
    PS_BITFIELD,     // waiting for initial messages
    PS_INTERESTED,   // sent INTERESTED, waiting for UNCHOKE
    PS_DOWNLOADING,  // actively requesting blocks
    PS_DONE,         // no more pieces for this peer, connection idle
    PS_DEAD,         // error — remove from epoll
} PeerPhase;

typedef struct {
    PeerConn  *conn;
    PeerPhase  phase;
    int        piece_idx;       // piece currently being downloaded
    int        blocks_sent;     // blocks requested
    int        blocks_recv;     // blocks received
    uint8_t   *peer_bitfield;
    time_t     last_active;
    int        fail_count;
} PeerSession;
```

#### 2.3 epoll event loop

```c
void scheduler_run(TorrentInfo *t, PieceManager *pm,
                   PeerList *peers, const uint8_t *peer_id,
                   const Config *cfg) {
    int epfd = epoll_create1(EPOLL_CLOEXEC);
    PeerSession *sessions = xcalloc(cfg->max_peers, sizeof(PeerSession));

    // Open non-blocking connections to min(max_peers, peers->count) peers
    int active = 0;
    for (int i = 0; i < peers->count && active < cfg->max_peers; i++) {
        int sock = tcp_connect_nb(peers->peers[i].ip, peers->peers[i].port);
        if (sock < 0) continue;
        sessions[active] = (PeerSession){
            .conn        = /* wrap sock */,
            .phase       = PS_CONNECTING,
            .last_active = time(NULL),
        };
        struct epoll_event ev = { .events = EPOLLOUT, .data.u32 = active };
        epoll_ctl(epfd, EPOLL_CTL_ADD, sock, &ev);
        active++;
    }

    struct epoll_event events[64];
    while (!piece_manager_is_complete(pm) && !g_interrupted) {
        int n = epoll_wait(epfd, events, 64, 1000 /* ms timeout for timers */);
        for (int i = 0; i < n; i++) {
            PeerSession *s = &sessions[events[i].data.u32];
            handle_peer_event(s, events[i].events, t, pm, peer_id, cfg, epfd);
        }
        // Cull idle sessions, open new connections, re-announce timer
        scheduler_housekeep(sessions, active, peers, epfd, t, pm, peer_id, cfg);
    }
    close(epfd);
}
```

`handle_peer_event` is a switch on `s->phase` — each case handles one step of
the handshake/download state machine and transitions to the next phase.

#### 2.4 Rarest-first piece selection

Once multiple peer sessions exist, pick the piece fewest peers have:

```c
int piece_manager_next_rarest(PieceManager *pm,
                              PeerSession *sessions, int n_sessions) {
    int *avail = xcalloc(pm->num_pieces, sizeof(int));
    for (int s = 0; s < n_sessions; s++) {
        if (!sessions[s].peer_bitfield) continue;
        for (int i = 0; i < pm->num_pieces; i++)
            if (bitfield_has_piece(sessions[s].peer_bitfield, i))
                avail[i]++;
    }
    int best = -1, best_n = INT_MAX;
    for (int i = 0; i < pm->num_pieces; i++) {
        if (pm->pieces[i].state != PIECE_EMPTY) continue;
        if (avail[i] > 0 && avail[i] < best_n) { best = i; best_n = avail[i]; }
    }
    free(avail);
    return best;
}
```

---

### Phase 3 — Seeding (1–2 days after Phase 2)

**Goal:** give back to the swarm after completing a download.

Seeding reuses the epoll scheduler. After `piece_manager_is_complete()`, instead
of exiting, the scheduler keeps listening for incoming connections and serves
`MSG_PIECE` responses.

Two additions:

**3.1** A listening socket for incoming connections:
```c
int listen_sock = socket(AF_INET, SOCK_STREAM, 0);
// bind to cfg->port, listen, add to epoll with EPOLLIN
// On EPOLLIN: accept(), add new PeerSession in PS_HANDSHAKE phase
```

**3.2** Handle `MSG_REQUEST` in the peer state machine:
```c
case MSG_REQUEST: {
    uint8_t *block = xmalloc(msg.length);
    piece_manager_read_piece(pm, msg.piece_index, block);
    // send MSG_PIECE with the slice [msg.begin, msg.begin+msg.length]
    free(block);
    break;
}
```

Add `--seed` and `--seed-ratio N` flags:
```
btorrent download file.torrent --seed          # seed until Ctrl-C
btorrent download file.torrent --seed-ratio 1.0 # seed until 1:1 ratio
```

---

### Phase 4 — DHT (3–7 days, enables tracker-less torrents)

**Goal:** find peers without a tracker. Required for most real-world use.

DHT is a Kademlia-based distributed hash table (BEP 5). The implementation
lives in `src/dht/` as a self-contained module.

Key pieces:
- `dht_node_id[20]` — our random node ID (saved to `~/.btorrent/dht_id`)
- Routing table: 160 buckets of up to 8 `(id, ip, port)` entries each
- Bootstrap: `dht_bootstrap("router.bittorrent.com", 6881)`
- Queries: `find_node`, `get_peers`, `announce_peer` (all over UDP)
- Responses feed peers directly into the scheduler

```c
// include/dht/dht.h
typedef struct DhtCtx DhtCtx;
DhtCtx  *dht_new(uint16_t port);
void     dht_bootstrap(DhtCtx *ctx, const char *host, int port);
void     dht_get_peers(DhtCtx *ctx, const uint8_t *info_hash,
                       peer_found_cb cb, void *user);
void     dht_tick(DhtCtx *ctx);   // call every second from scheduler loop
void     dht_free(DhtCtx *ctx);
```

Wire into `cmd_download`: if tracker returns 0 peers, fall back to DHT.

---

### Phase 5 — Magnet links (1 day after DHT)

**Goal:** `btorrent download magnet:?xt=urn:btih:...`

Magnet link parsing is simple string work:
```c
// src/core/magnet.c
typedef struct {
    uint8_t info_hash[20];
    char    name[256];
    char    trackers[MAX_TRACKERS][512];
    int     num_trackers;
} MagnetLink;

int magnet_parse(const char *uri, MagnetLink *out);
```

The hard part is fetching the `.torrent` metadata from peers once you have the
info_hash but no `.torrent` file. This requires BEP 9 (metadata extension) +
BEP 10 (extension protocol handshake). The handshake reserved bytes at offset 20
need bit 20 set to signal extension support. Once negotiated, request the
`ut_metadata` extension to receive the info dict in pieces.

---

### Phase 6 — Distribution packaging (1 day)

**Goal:** `brew install btorrent` / `apt install btorrent` / AUR.

#### 6.1 man page

```
docs/btorrent.1   (troff format)
```

```
btorrent(1)

NAME
    btorrent — BitTorrent client

SYNOPSIS
    btorrent download [options] file.torrent
    btorrent inspect  file.torrent [--json]
    btorrent check    file.torrent [--output dir]

...
```

Add to Makefile:
```makefile
install: all
    install -Dm755 build/btorrent    $(DESTDIR)$(PREFIX)/bin/btorrent
    install -Dm644 docs/btorrent.1   $(DESTDIR)$(PREFIX)/share/man/man1/btorrent.1
```

#### 6.2 Homebrew formula

```ruby
# Formula/btorrent.rb
class Btorrent < Formula
  desc "Educational BitTorrent client"
  homepage "https://github.com/alsullam/btorrent"
  url "https://github.com/alsullam/btorrent/archive/v1.0.tar.gz"
  depends_on "curl"
  def install
    system "make", "PREFIX=#{prefix}", "install"
  end
  test do
    system "#{bin}/btorrent", "version"
  end
end
```

#### 6.3 GitHub Actions CI

```yaml
# .github/workflows/ci.yml
name: CI
on: [push, pull_request]
jobs:
  build-and-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: sudo apt-get install -y libcurl4-openssl-dev
      - run: make all
      - run: make test
      - run: make debug  # ASan/UBSan pass
```

---

## Execution order recommendation

```
Phase 1  CLI UX          1–2 days   no risk, high user-visible impact
Phase 2  epoll scheduler 3–5 days   core throughput, most complex change
Phase 3  Seeding         1–2 days   depends on Phase 2
Phase 4  DHT             3–7 days   depends on Phase 2
Phase 5  Magnet links    1 day      depends on Phase 4
Phase 6  Packaging       1 day      any time after Phase 1
```

Total to a fully usable, distributable BitTorrent client: **~2–3 weeks** of
focused engineering.

---

## Files to create in Phase 1 (ready to implement)

```
src/cmd/download.c   download subcommand (extract from main.c)
src/cmd/inspect.c    inspect subcommand
src/cmd/check.c      check subcommand
include/cmd/cmd.h    int cmd_XXX(int argc, char **argv) prototypes
src/main.c           shrinks to ~40 lines: dispatch table only
docs/btorrent.1      man page
.github/workflows/ci.yml
```

## Files to create in Phase 2

```
src/net/tcp.c            non-blocking connect / accept helpers
include/net/tcp.h
src/scheduler.c          epoll event loop + PeerSession state machine
include/scheduler.h
```

## Files to create in Phase 4

```
src/dht/dht.c            Kademlia routing table + query engine
src/dht/dht_krpc.c       bencode-based KRPC message encode/decode
include/dht/dht.h
tests/unit/test_dht.c
```
