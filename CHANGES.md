# btorrent — Changelog

## v1.0.2 (2026-04-15)

Bug-fix release. No behaviour changes for working downloads.

### Bug fixes

- **DHT finds 0 peers on first try** — `DHT_ALPHA` raised from 3 to 8;
  `DHT_MAX_NODES` raised from 256 to 512; round 1 now queries all bootstrap
  nodes in parallel instead of sequentially. Eliminates the cold-start lottery
  where `dht.transmissionbt.com` resolving to a slow node caused the lookup to
  exhaust the node table after 2 rounds.

- **Metadata fetch returns 0 blocks** — BEP 9 block requests are now sent
  one-at-a-time (request → wait → request) instead of blasting all blocks
  upfront. Many peers silently discard burst metadata requests but respond
  correctly when paced. `PEER_TIMEOUT_S` reduced 15 → 8 s so bad peers fail
  faster.

- **`kill <pid>` (SIGTERM) ignored during metadata fetch** — `ext_fetch_metadata`
  now accepts `volatile sig_atomic_t *interrupted` and checks it between peer
  attempts. Process now stops within one timeout cycle (≤ 8 s) on SIGTERM.

- **Lock file fails with "No such file or directory"** — parent directory of
  `<out_path>.btlock` is now created before the `open()` call. Previously the
  output directory didn't exist yet when the lock was attempted.

- **DHT log spam** — `dht: 1 peers from X` was printed once per 6-byte entry
  in the values list. Now prints one summary line per response node.

- **`realloc` without NULL check** — three sites in `cmd_download.c` assigned
  `realloc()` directly back to `peers.peers` without checking for NULL. On OOM
  this set the pointer to NULL and the next iteration would crash. Fixed to use
  a temp pointer with a graceful fallback.

- **Magnet `raw[]` buffer too small** — URL-encoded tracker URLs can be up to
  3× their decoded length. `raw[]` and `val[]` widened from 1024 to 2048 bytes;
  tracker slot in `MagnetLink` widened from 512 to 768 bytes.

- **Metadata tmpfile left on disk after SIGKILL** — tmpfile is now unlinked
  immediately after `mkstemp()` and accessed via `/proc/self/fd/<n>`. The
  directory entry disappears instantly; the inode stays alive via the open fd
  for the duration of the write and parse, then is reclaimed by the OS.

### Shell usage note

Magnet URIs contain `&` characters which the shell interprets as background
job separators. Always wrap magnet links in single quotes:

    btorrent -d 'magnet:?xt=urn:btih:HASH&tr=...'

Without quotes, every `tr=` parameter becomes a separate background shell job
and the client receives only the `xt=` hash with no trackers.

---

## v1.0.1 (2026-04-13)

Correctness and publish-readiness release.

### New features
- **Seeding** (`--seed`) — listen socket in the epoll loop serves verified
  pieces to incoming peers after download completes.
- **PEX — Peer Exchange** (BEP 11) — `ut_pex` advertised alongside
  `ut_metadata`; inbound peer lists merged into pool automatically;
  outbound broadcast every 60 s.
- **Rate limiting** (`--ul N`, `--dl N`) — token-bucket limiter in KiB/s.
- **Endgame mode** — broadcasts last pieces to all eligible peers; sends
  `MSG_CANCEL` to duplicates on completion.
- **Stuck-piece rescue** — pieces held for `3 × timeout` with zero blocks
  returned to pool without killing the peer.
- **Active-piece memory cap** — at most `max(max_peers/2, 8)` piece buffers
  allocated simultaneously.
- **Choke rotation** — tit-for-tat every 10 s; top 4 peers by `blocks_recv`
  stay unchoked.
- **File locking** — `<out_path>.btlock` prevents simultaneous instances.
- **Magnet link full download** — DHT + BEP 9/10 metadata fetch falls through
  to normal download pipeline.
- **Progress bar** — TTY: animated `[####....]` bar with speed and ETA.
  Non-TTY: plain `[progress] N%` log lines every 5%.
- **Man page** — `docs/btorrent.1` installed by `make install`.
- **Packaging** — AUR PKGBUILD, Debian control/rules/changelog, RPM spec,
  `.gitignore`, GitHub Actions CI + release workflow.

### Bug fixes
- DHT timeout raised from 10 s → 30 s; trigger threshold raised from
  `< 10` to `< 20` peers.
- Emergency re-announce in scheduler when `live_peers < 3`.
- Lock file directory created before lock attempt.
- DHT log spam fixed (one line per response, not per peer entry).
- `realloc` NULL-check added in scheduler `inject_peers`.
- Version string sourced from `Makefile VERSION` variable, not hardcoded.

---

## v1.0.0 (2026-04-11)

First working release.

### Core features
- Concurrent epoll-based scheduler with rarest-first piece selection and
  block-level pipelining (BEP 3).
- DHT peer discovery (BEP 5).
- Magnet URI parsing (hex + base32 info-hash, URL-decoded `dn=` and `tr=`).
- BEP 9 / BEP 10 metadata fetch (`ut_metadata`).
- Multi-tracker announce with exponential backoff (BEP 12).
- UDP tracker protocol (BEP 15).
- SHA-1 piece verification with resume from disk.
- Multi-file torrent support via `pwrite()`.
- Structured logging (`LOG_DEBUG/INFO/WARN/ERROR`) with runtime level control.
- CLI: `-d`, `-i`, `-c`, `-V`, `--output`, `--port`, `--peers`, `--timeout`,
  `--pipeline`, `--verbose`, `--log`, `--json`.
- 178 unit tests across 7 suites, all offline.
