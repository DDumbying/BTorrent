# BTorrent — What Changed

## New files

| File | Purpose |
|---|---|
| `include/log.h` / `src/log.c` | Structured logging: `LOG_DEBUG/INFO/WARN/ERROR` macros with timestamp, source file, level. Replaces every `printf`/`fprintf` in the codebase. Runtime level and output file configurable via `log_init()`. |
| `include/result.h` / `src/result.c` | Unified error type `BtErr` (enum) and `BT_RESULT(T)` macro. `peer_connect` and `peer_handshake` now return typed results instead of `NULL`/`-1`. `bt_strerror()` gives human-readable error strings. |

## Bug fixes

| Location | Bug | Fix |
|---|---|---|
| `main.c` | **Use-after-free**: `piece_manager_is_complete(pm)` called on the `return` line after `piece_manager_free(pm)`. | Save `int complete = piece_manager_is_complete(pm)` before freeing. |
| `torrent.c` | **Dead code**: `compute_info_hash` called `bencode_parse()`, discarded the result, then re-walked the buffer manually with a redundant depth-counter loop. | Added `bencode_parse_ex()` that returns bytes consumed. One parse pass, no dead code. |
| `tracker.c` | **Weak randomness**: peer ID and UDP transaction IDs generated with `srand(time)`/`rand()`. Predictable after the first call. | Replaced with `random_bytes()` which reads from `/dev/urandom`, with a time-seeded `rand()` fallback only if the device is unavailable. |
| `main.c` | **Wrong re-announce trigger**: re-announce fired based on `peer_idx` count, not wall-clock time. A fast peer could exhaust the list before the interval elapsed; a stalled session would never re-announce. | Re-announce now fires when `time(NULL) - last_announce >= announce_interval`. |
| `main.c` | **Duplicated bitfield code**: `bitfield_set_piece` inline logic was copy-pasted in two places. | Extracted to `bitfield_set_piece()` in `peer.c`/`peer.h` and called from both sites. |

## Improvements

| What | Detail |
|---|---|
| **Directory structure** | Sources reorganised into `src/core/`, `src/proto/`, `src/net/` and `include/core/`, `include/proto/`. All headers use `#pragma once`. |
| **`pwrite(2)` for disk I/O** | `pieces.c` replaced `fseek + fwrite` with `pwrite` (positional, atomic write) and `pread`. No `FILE*` buffering layer. `ftruncate` used for pre-allocation instead of seeking and writing a single zero byte. |
| **`TCP_NODELAY`** | Set on every peer socket. Disables Nagle's algorithm, which was batching the 17-byte `REQUEST` messages and adding ~200 ms latency per request. |
| **`tracker_announce_with_retry`** | New wrapper around `tracker_announce` with exponential backoff: retries up to 5 times with 1 s, 2 s, 4 s, 8 s, 16 s delays. Used for the initial announce in `main.c`. |
| **CLI via `getopt_long`** | All hardcoded constants (`LISTEN_PORT`, `PIPELINE_DEPTH`, etc.) moved into a `Config` struct and exposed as flags: `--port`, `--pipeline`, `--timeout`, `--verbose`, `--log`. |
| **Makefile** | Added `-MMD -MP` automatic header dependency tracking. Separate `release` (`-O2 -DNDEBUG`) and `debug` (`-g3 -fsanitize=address,undefined`) targets. `LOG_MIN_LEVEL` compile-time gate. |

## Tests added

| Suite | Tests | What is covered |
|---|---|---|
| `test_sha1` | 6 | RFC 3174 vectors (empty, "abc", 56-byte, 55-byte boundary, 64-byte boundary); streaming API |
| `test_peer` | 18 | Message framing roundtrip via `socketpair(2)` for INTERESTED, REQUEST, HAVE, KEEPALIVE; bitfield read and write |
| `test_pieces` | 16 | Verified piece accepted and written; corrupt piece rejected and reset; two-block assembly; `next_needed` skip logic; peer-bitfield filter; resume from disk |

**Total: 40 tests, 0 failures, 0 compiler warnings.**
