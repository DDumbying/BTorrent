# btorrent — Development Log

Complete record of every change, fix, and addition made across all development
sessions. Entries are grouped by phase and ordered from earliest to most recent.

---

## Phase 1 — Structured logging & error types

### New: `include/log.h` / `src/log.c`

Replaced every `printf` / `fprintf(stderr, ...)` in the codebase with a
structured logging system.

**Macros:** `LOG_DEBUG`, `LOG_INFO`, `LOG_WARN`, `LOG_ERROR`

Each call emits: `[HH:MM:SS] LEVEL  source_file:line  message`

**Runtime configuration via `log_init()`:**
- Minimum level (compile-time gate via `LOG_MIN_LEVEL`, runtime via `log_set_level()`)
- Optional log file (`log_set_file(path)`) — writes to file alongside stderr
- Release builds compiled with `LOG_MIN_LEVEL=1` to strip all `LOG_DEBUG` calls
  at compile time (zero runtime cost)

### New: `include/result.h` / `src/result.c`

Replaced `NULL` / `-1` error returns with a typed error system.

**`BtErr` enum values:**

| Code | Name | Meaning |
|---|---|---|
| 0 | `BT_OK` | Success |
| 1 | `BT_ERR_IO` | File / socket read-write failure |
| 2 | `BT_ERR_PARSE` | Malformed bencode or torrent data |
| 3 | `BT_ERR_HASH` | SHA-1 mismatch on a piece |
| 4 | `BT_ERR_NETWORK` | DNS, connect, or timeout failure |
| 5 | `BT_ERR_PROTOCOL` | Peer sent unexpected or invalid data |
| 6 | `BT_ERR_ALLOC` | Out of memory |
| 7 | `BT_ERR_ARGS` | Bad function arguments |
| 8 | `BT_ERR_CHOKED` | Peer choked us before we could request |
| 9 | `BT_ERR_NO_PIECES` | Peer has nothing we need |
| 10 | `BT_ERR_TRACKER` | Tracker returned error or zero peers |

**`BT_RESULT(T)` macro** — pairs a value with an error code, used by functions
that both return a value and can fail:

```c
typedef BT_RESULT(PeerConn *) PeerConnResult;
PeerConnResult r = peer_connect(...);
if (r.err) { LOG_WARN(...); return r.err; }
PeerConn *conn = r.value;
```

`bt_strerror(err)` returns a human-readable string for any `BtErr` code.

---

## Phase 2 — Bug fixes

### Fix: use-after-free in `main.c`

**Bug:** `piece_manager_is_complete(pm)` was called on the `return` line *after*
`piece_manager_free(pm)`. The freed memory was read to determine the exit code.

**Fix:** Save `int complete = piece_manager_is_complete(pm)` before freeing,
then use `complete` in the `return`.

### Fix: dead code in `torrent.c` — redundant bencode parse

**Bug:** `compute_info_hash()` called `bencode_parse()`, discarded the returned
tree, then re-walked the raw buffer a second time with a manual depth-counter
loop to find the info-dict boundaries. Two passes, one wasted.

**Fix:** Added `bencode_parse_ex()` to `bencode.c` — identical to
`bencode_parse()` but also returns the number of bytes consumed. The info-hash
computation now does one parse pass and uses the returned byte count to slice
the buffer for SHA-1 hashing. The depth-counter loop was deleted.

### Fix: weak randomness in `tracker.c`

**Bug:** Peer ID and UDP transaction IDs generated with `srand(time(NULL))` /
`rand()`. The sequence is fully predictable after the first call and produces
the same peer ID on rapid restarts.

**Fix:** Replaced with `random_bytes()` in `utils.c`, which reads from
`/dev/urandom`. Falls back to time-seeded `rand()` only if `/dev/urandom` is
unavailable (unusual environments). Used everywhere a random value is needed:
peer ID generation, UDP transaction IDs, port randomisation.

### Fix: wrong re-announce trigger in `main.c`

**Bug:** Re-announce fired when `peer_idx` (the cursor into the peer list)
reached the end of the list. A fast connection that exhausted the peer list in
10 seconds would re-announce at 10 s; a stalled session would never
re-announce at all. Neither matched the tracker-specified interval.

**Fix:** Re-announce now fires when `time(NULL) - last_announce >=
announce_interval`, where `announce_interval` comes from the tracker's response.
Defaults to 1800 s when the tracker omits the field.

### Fix: duplicated bitfield logic in `main.c`

**Bug:** The bit-setting logic for marking a piece as owned in a bitfield was
copy-pasted in two separate locations in `main.c`.

**Fix:** Extracted to `bitfield_set_piece(uint8_t *bitfield, int piece_index)`
in `peer.c` / `peer.h`. Both call sites replaced with the single function.
`bitfield_has_piece()` was already in `peer.c`; now both halves of the API
live in the same place.

### Fix: `MSG_REQUEST` / `MSG_CANCEL` payload not consumed in `peer_recv_msg()`

**Bug:** The `case MSG_REQUEST` and `case MSG_CANCEL` branches in
`peer_recv_msg()` read the message ID but did not consume the 12-byte payload.
Subsequent reads would start mid-message and corrupt the stream.

**Fix:** Both cases now call `recv_all(conn->sock, buf, 12)` to drain the
payload before returning.

---

## Phase 3 — Concurrent epoll scheduler

### New: `src/scheduler.c` / `include/scheduler.h`

Replaced the serial "one peer at a time" download loop with a concurrent,
non-blocking epoll-driven scheduler.

**Public API:**
```c
int scheduler_run(const TorrentInfo *torrent, PieceManager *pm,
                  PeerList *initial, const uint8_t *peer_id,
                  const Config *cfg, volatile sig_atomic_t *interrupted);
```

**State machine per peer session:**

```
CONNECTING → HANDSHAKE → BITFIELD_WAIT → INTERESTED → DOWNLOADING
                                                            |
          ←──────────── IDLE (re-unchoked) ────────────────┘
Any state → DEAD (error / peer closed)
```

**Per-session read buffer** — each session has a 1 MiB ring buffer
(`ReadBuf`). `rbuf_fill()` drains the socket non-blockingly;
`rbuf_consume()` slices complete messages from the front. This fixes the
original `nb_recv` approach which would return `EAGAIN` mid-message and lose
the partial state.

**Rarest-first piece selection** — `next_rarest()` counts how many connected
peers have each piece. Pieces owned by fewer peers are requested first.
Falls back to sequential if no rarity data is available.

**Request pipelining** — `send_requests()` keeps up to `cfg->pipeline_depth`
block requests outstanding per peer. Configurable via `--pipeline` flag.

**Keepalive** — sends a zero-length keepalive every 90 seconds to prevent
peer timeout (BEP 3 specifies a 2-minute idle limit).

**Re-announce** — the scheduler re-announces to the tracker at the
tracker-specified interval and merges fresh peer lists into the pool.

**Backup tracker scrape** — if the primary tracker returns fewer than 50 peers,
the scheduler immediately tries all backup trackers from `announce_list` and
deduplicates the results.

**Per-peer timeout** — configurable via `cfg->peer_timeout_s`. Dead sessions
are replaced immediately from the peer pool.

**`MSG_HAVE` broadcast** — when a piece is verified and written, the scheduler
sends `MSG_HAVE` to all connected peers (required by BEP 3).

**Re-interest after choke** — when a `MSG_CHOKE` is received, the scheduler
immediately re-sends `MSG_INTERESTED` so the peer knows to unchoke us when it
can.

---

## Phase 3 — CLI & config

### New: `Config` struct and `getopt_long` CLI

All hardcoded constants moved into `Config` (defined in `include/cmd/cmd.h`).

| Flag | Default | Meaning |
|---|---|---|
| `-d <file\|magnet>` | — | Download mode |
| `-i <file>` | — | Inspect a .torrent file |
| `-c <file>` | — | Check pieces of a completed download |
| `-o <dir>` | `.` | Output directory |
| `-p <port>` | 6881 | Listen / announce port |
| `--pipeline <n>` | 5 | Outstanding block requests per peer |
| `--max-peers <n>` | 50 | Maximum concurrent connections |
| `--timeout <s>` | 30 | Per-peer connection timeout |
| `--peer-timeout <s>` | 120 | Idle peer eviction timeout |
| `--verbose` / `-v` | off | Enable `LOG_DEBUG` output |
| `--log <file>` | — | Tee log output to a file |

### New: `cmd_inspect.c` — torrent inspection subcommand

`btorrent -i file.torrent` prints full metadata without downloading:
name, info-hash (hex), total size, piece count and size, tracker list,
and the full file list with sizes and offsets.

### New: `cmd_check.c` — piece integrity verification subcommand

`btorrent -c file.torrent -o <dir>` reads existing output files and
re-verifies each piece's SHA-1 against the .torrent hashes. Reports
good / bad / missing piece counts. Useful for detecting partial or
corrupted downloads.

---

## Phase 3 — Directory restructure

Sources reorganised from a flat layout into:

```
include/
  core/      bencode.h  sha1.h  torrent.h  pieces.h  magnet.h
  proto/     peer.h  tracker.h  ext.h
  net/       tcp.h
  cmd/       cmd.h
  dht/       dht.h
  log.h  result.h  scheduler.h  utils.h
src/
  core/      bencode.c  sha1.c  torrent.c  pieces.c  magnet.c
  proto/     peer.c  tracker.c  ext.c
  net/       tcp.c
  cmd/       cmd_download.c  cmd_inspect.c  cmd_check.c
  dht/       dht.c
  main.c  log.c  result.c  utils.c  scheduler.c
tests/unit/
  test_sha1.c  test_peer.c  test_pieces.c  test_magnet.c  test_ext.c
```

All headers use `#pragma once`. No circular includes.

---

## Phase 3 — I/O and socket improvements

### `pwrite(2)` for all disk writes in `pieces.c`

Replaced `fseek` + `fwrite` with `pwrite()` (positional, atomic write) and
`pread()`. Eliminated the `FILE *` buffering layer entirely. `ftruncate()` used
for file pre-allocation instead of seeking to the end and writing a zero byte.
Multi-file torrents use byte-offset arithmetic to map piece boundaries to file
boundaries correctly.

### `TCP_NODELAY` on all peer sockets

Set on every outgoing peer socket in both `peer.c` and `scheduler.c`. Disables
Nagle's algorithm, which was coalescing the 17-byte `MSG_REQUEST` messages and
introducing up to ~200 ms of artificial latency per block request.

### `tracker_announce_with_retry` in `tracker.c`

New wrapper around `tracker_announce()` with exponential backoff:

- Up to 5 attempts
- Delays: 1 s, 2 s, 4 s, 8 s, 16 s
- Returns on first success; returns empty peer list after all retries exhausted

Used for the initial announce in `cmd_download.c`.

---

## Phase 3 — Makefile

| Addition | Detail |
|---|---|
| `-MMD -MP` | Automatic header dependency tracking. Changing a `.h` rebuilds all `.c` files that include it |
| `release` target | `gcc -O2 -DNDEBUG -DLOG_MIN_LEVEL=1` |
| `debug` target | `gcc -g3 -O0 -fsanitize=address,undefined -DLOG_MIN_LEVEL=0` |
| `make install` | Copies binary to `$(PREFIX)/bin/btorrent` (default `/usr/local`) |
| Per-suite test targets | `make test_sha1`, `make test_peer`, `make test_pieces`, `make test_magnet`, `make test_ext` |
| `make test` | Runs all suites in sequence, fails on first non-zero exit |

---

## Phase 4 — DHT peer discovery

### New: `src/dht/dht.c` / `include/dht/dht.h`

Implements BEP 5 — Kademlia-based Distributed Hash Table for tracker-free peer
discovery.

**API:**
```c
DhtCtx  *dht_new(uint16_t port);
void      dht_bootstrap(DhtCtx *dht);
PeerList  dht_get_peers(DhtCtx *dht, const uint8_t *info_hash, int want);
void      dht_free(DhtCtx *dht);
```

**Bootstrap nodes** — contacts the standard public bootstrap nodes
(`router.bittorrent.com:6881`, `router.utorrent.com:6881`,
`dht.transmissionbt.com:6881`) to enter the DHT network.

**Integration in `cmd_download.c`:**
- If the primary tracker returns zero peers, DHT is tried automatically
- If fewer than 10 peers found from all trackers, DHT runs as a supplement
- DHT peers are merged and deduplicated against tracker peers before the
  scheduler starts

---

## Phase 5 — Magnet link parsing

### New: `src/core/magnet.c` / `include/core/magnet.h`

Parses `magnet:?xt=urn:btih:...` URIs per the informal magnet link spec.

**`MagnetLink` struct fields:**

| Field | Description |
|---|---|
| `info_hash[20]` | Binary 20-byte SHA-1 (decoded from hex or base32) |
| `info_hash_hex[41]` | Lowercase hex string |
| `name[256]` | Display name from `dn=` parameter (URL-decoded) |
| `trackers[16][512]` | Tracker URLs from `tr=` parameters (URL-decoded) |
| `num_trackers` | Count of parsed trackers |

**Encoding support:**
- Hex info-hash: `urn:btih:aabbcc...` (40 hex chars)
- Base32 info-hash: `urn:btih:AAABBB...` (32 base32 chars)
- URL-encoded `dn=` and `tr=` values fully decoded

---

## Phase 6 — CI and packaging skeleton

### New: `.github/workflows/ci.yml`

GitHub Actions workflow that runs on every push and pull request:

1. Install `libcurl4-openssl-dev`
2. `make all` (release build, zero warnings required)
3. `make test` (all unit test suites)

Fails the workflow on any compiler warning or test failure.

### `make install` target

```makefile
install: all
    install -Dm755 $(BIN) $(DESTDIR)$(PREFIX)/bin/btorrent
```

Supports `DESTDIR` for staged installs (packaging systems).
`PREFIX` defaults to `/usr/local`, overridable: `make install PREFIX=/usr`.

---

## Phase 7 — BEP 9/10 — Magnet metadata fetch

This phase completes magnet link support. Before this, `btorrent -d
magnet:?...` would discover peers via DHT and then exit — it had no way to
download without a `.torrent` file. After this phase, magnet links fully
download.

### New: `include/proto/ext.h` / `src/proto/ext.c` (533 lines)

Implements:
- **BEP 10** — Extension Protocol handshake
- **BEP 9** — `ut_metadata` exchange

**Public API (one function):**
```c
TorrentInfo *ext_fetch_metadata(const PeerList *peers,
                                const uint8_t  *info_hash,
                                int             max_peers);
```

Returns a heap-allocated `TorrentInfo` on success (free with `torrent_free()`),
or `NULL` if no peer could supply the metadata.

---

#### BEP 10 — Extension Protocol (implemented in `ext.c`)

The standard BitTorrent handshake has 8 reserved bytes. BEP 10 defines bit
`0x10` of reserved byte 5 (index from 0) as "I support the extension
protocol". We set this bit in our outgoing handshake and check for it in the
peer's handshake. Peers that don't set it are skipped immediately.

After the 68-byte handshake, both peers exchange a **BEP-10 extension
handshake** as a standard wire message with ID 20. The payload is a bencoded
dict:

```
d
  1:m d
    11:ut_metadata i1e   ← "I know ut_metadata as ext msg id 1"
  e
  1:q i1e                ← request queue depth hint
  1:v 14:btorrent/0.9.0  ← client version string
e
```

The peer's reply is parsed for:
- `m.ut_metadata` — their local extension message ID for `ut_metadata`
- `metadata_size` — total byte count of the info-dict

Peers that don't advertise `ut_metadata` or don't include `metadata_size` are
skipped.

**Bug found during implementation:** the version string `"btorrent/0.9.0"` is
14 bytes, not 15. The original format string said `"15:btorrent/0.9.0"`, which
caused the bencode parser to consume the outer dict's closing `e` as part of
the string value, leaving the dict unclosed. The parser returned `NULL` for
every handshake message, silently skipping every peer. Fixed to
`"14:btorrent/0.9.0"`.

---

#### BEP 9 — `ut_metadata` exchange (implemented in `ext.c`)

The torrent info-dict is split into 16 KiB blocks. To fetch block `N`, we send
a wire message with ID 20 and sub-ID equal to the peer's `ut_metadata` ext ID:

```
d 8:msg_type i0e 5:piece iNe e
```

The peer replies with:

```
d 8:msg_type i1e 5:piece iNe 10:total_size iMe e <raw block bytes>
```

The raw block bytes are appended directly after the bencoded dict.
`bencode_parse_ex()` (which returns bytes consumed) is used to find exactly
where the dict ends and the raw bytes begin.

**Reject handling:** if the peer sends `msg_type=2` (reject), we stop trying
that peer and move to the next one.

**Pipelining:** all block requests are sent upfront before waiting for replies,
rather than doing request/reply/request/reply sequentially. This avoids a
full round-trip per block.

---

#### SHA-1 verification in `torrent_info_from_raw_dict()`

After all blocks are assembled, the complete info-dict is SHA-1 hashed and
compared against the original `info_hash` from the magnet link. If the hashes
don't match — whether from data corruption or a malicious peer — the entire
result is discarded and the next peer is tried. A peer cannot inject arbitrary
torrent metadata.

```c
sha1_init(&ctx);
sha1_update(&ctx, dict, dict_len);
sha1_final(&ctx, actual_hash);
if (memcmp(actual_hash, expected_hash, 20) != 0) return NULL;
```

The verified dict is wrapped in a minimal `.torrent` envelope
(`d4:info<raw-dict>e`), written to a tmpfile, parsed by the existing
`torrent_parse()`, and the tmpfile deleted. This reuses all existing parsing
logic with no duplication.

---

### Modified: `src/cmd/cmd_download.c`

The magnet-link branch was a stub that ran DHT peer discovery and exited.
Replaced with a full 5-stage pipeline:

**Old behaviour:**
```
parse magnet → DHT discovery → print peer list → exit
```

**New behaviour:**
```
[1/5] parse magnet URI
[2/5] peer discovery: tracker announce + DHT, results merged and deduped
[3/5] BEP 9/10 metadata fetch via ext_fetch_metadata()
      → on success: falls through to normal download pipeline
      → on failure: LOG_ERROR and EXIT_FAILURE
[4/5] scheduler_run() — identical to .torrent path
[5/5] tracker completion/stopped event
```

The magnet and `.torrent` paths rejoin at a `magnet_resume:;` label placed
before the output-path resolution block, so all downstream logic — piece
manager creation, scheduler, progress reporting, tracker events — is shared
with zero duplication.

**Peer discovery detail:** tracker announce is attempted first (fastest when
the magnet includes `tr=` parameters) using a stub `TorrentInfo` built from
just the `info_hash`. DHT runs in parallel and results are merged. Both lists
are deduplicated by `IP:port` before being passed to `ext_fetch_metadata()`.

---

### New: `tests/unit/test_ext.c` (321 lines, 35 test cases)

Covers all pure, offline-testable logic in `ext.c`:

| Test group | Cases | What is covered |
|---|---|---|
| Ext handshake builder | 8 | Output is valid bencode; root is a dict; `m.ut_metadata` present and correct; version string present; truncated buffer returns error |
| Ext handshake parser | 6 | Peer with `ut_metadata=3` and `metadata_size=32768`; peer without `ut_metadata`; peer without `metadata_size`; garbage input returns error |
| Metadata request builder | 7 | Piece 0 request is valid bencode with correct fields; piece 7 request has correct index; truncated buffer returns error |
| Metadata data parser | 11 | Full data reply: correct `msg_type`, `piece`, `total_size`, block pointer, block length, block contents; reject reply (`msg_type=2`); request-type reply survives without crash |
| SHA-1 verification | 3 | Two independent hash computations agree; corrupted data produces different hash; real hash is non-zero |

Network-dependent code (`tcp_connect_timed`, `try_peer_metadata`,
`ext_fetch_metadata`) is covered by integration testing rather than unit tests.

---

### Modified: `Makefile`

- Added `src/proto/ext.c` to `SRCS`
- Added `test_ext` target:
  ```makefile
  test_ext: build/obj
      $(CC) $(TEST_FLAGS) tests/unit/test_ext.c \
          src/core/bencode.c src/core/sha1.c \
          $(TEST_COMMON) -o build/test_ext
      @echo "--- test_ext ---" && ./build/test_ext
  ```
- `test` target now depends on `test_ext`

---

## Test suite summary (all phases)

| Suite | File | Tests | What is covered |
|---|---|---|---|
| SHA-1 | `test_sha1.c` | 6 | RFC 3174 vectors; streaming API |
| Peer wire | `test_peer.c` | 18 | Message framing roundtrip via `socketpair(2)`; bitfield read/write |
| Piece manager | `test_pieces.c` | 16 | Verified piece accepted; corrupt piece rejected; two-block assembly; rarest-first selection; resume from disk |
| Magnet parser | `test_magnet.c` | 20 | Hex and base32 info-hash decoding; URL decoding; multiple trackers; invalid inputs |
| BEP 9/10 | `test_ext.c` | 35 | Bencode builders; handshake parser; block parser; SHA-1 verify path |
| **Total** | | **95** | **0 failures, 0 compiler warnings** |

---

## What is not yet implemented

| Feature | Notes |
|---|---|
| **Seeding** | After download completes the client exits. Needs a listen socket added to the epoll loop and `MSG_REQUEST` responses served from the piece manager. |
| **PEX — Peer Exchange** | BEP 11. Peers share their peer lists with each other. Reuses the BEP-10 extension protocol infrastructure already in `ext.c`. Small addition. |
| **Rate limiting** | No upload or download rate cap. A token bucket in the scheduler's send/recv paths would cover both. |
| **Man page** | `make install` copies the binary but there is no `btorrent.1` man page yet. |
