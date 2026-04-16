# btorrent v1.0.2 — Changes Log

## Security Fixes

### Fixed: Memory leaks on realloc failures

Multiple `realloc` calls didn't check for NULL before assignment, causing memory leaks if allocation failed.

**Files affected:**
- `src/core/bencode.c` - `parse_list()` and `parse_dict()`
- `src/scheduler.c` - `inject_peers()`
- `src/proto/tracker.c` - `curl_wcb()`

**Fix:** Store old pointer, check realloc result, free old pointer on failure.

### Fixed: Missing MSG_NOSIGNAL

`peer.c::send_all()` didn't use `MSG_NOSIGNAL`, which could cause SIGPIPE crashes when writing to closed sockets.

**File:** `src/proto/peer.c`

**Fix:** Added `MSG_NOSIGNAL` flag to all send calls.

### Fixed: No SSL certificate verification

HTTP tracker requests didn't verify SSL certificates, making the client vulnerable to MITM attacks.

**File:** `src/proto/tracker.c`

**Fix:** Added `CURLOPT_SSL_VERIFYPEER` and `CURLOPT_SSL_VERIFYHOST`.

## Reliability Fixes

### Fixed: Race condition in logging

Global log state (`g_min_level`, `g_dest`, `g_is_tty`) wasn't protected by mutex, causing potential race conditions in multi-threaded context.

**File:** `src/log.c`

**Fix:** Added `pthread_mutex_t` protection to all logging functions.

### Fixed: Integer overflow in bencode parser

Integer parsing check used `>= 30` instead of `>= sizeof(buf) - 1`, allowing potential buffer overflow.

**File:** `src/core/bencode.c`

**Fix:** Changed to use `sizeof(buf) - 1`.

### Fixed: URL decode buffer overflow

`%00` sequences created premature NUL terminators, causing buffer overflow.

**File:** `src/core/magnet.c`

**Fix:** Fixed to handle embedded nulls correctly.

### Fixed: Base32 length check

`strlen(src) < 32` should be `strlen(src) != 32`.

**File:** `src/core/magnet.c`

**Fix:** Changed comparison operator.

### Fixed: Request validation

Block request handler didn't check for zero-length requests.

**File:** `src/scheduler.c`

**Fix:** Added `req_len <= 0` check.

### Fixed: DHT bit not set

Handshake didn't set BEP-5 DHT bit (0x01) in reserved bytes.

**Files:** `src/scheduler.c`, `src/proto/ext.c`

**Fix:** Changed `buf[25] = 0x10` to `buf[25] = 0x11`.

## Stability Enhancements

### Added: Circuit breaker pattern

Added failure tracking per peer session. After 3 consecutive failures, the circuit opens for 30 seconds to avoid hammering failing peers.

**File:** `src/scheduler.c`

**New functions:**
- `is_circuit_open()` - Check if peer is in circuit-open state
- `record_failure()` - Record a failure for a session
- `record_success()` - Reset failures on success

### Added: Graceful shutdown

Replaced `sleep()` with interruptible `nanosleep()` to allow faster response to signals.

**Files:** `src/scheduler.c`, `src/proto/tracker.c`

### Added: More DHT bootstrap nodes

Added 3 more DHT bootstrap nodes for better peer discovery.

**File:** `src/dht/dht.c`

### Added: IPv6 support

Added `tcp_connect_nb_ipv6()` for IPv6 connections.

**File:** `src/net/tcp.c`

### Added: O_CLOEXEC on file descriptors

Added `O_CLOEXEC` flag to prevent fd inheritance by child processes.

**Files:** `src/core/pieces.c`

### Added: Health check API

New health monitoring interface for status reporting.

**New files:**
- `include/health.h`
- `src/health.c`

## CLI Improvements

### Added: Quiet mode (-q)

New `-q` / `--quiet` flag for minimal output (progress bar only).

### Changed: Default log level

Default log level changed from INFO to WARN for cleaner output. Use `-v` for verbose, `-q` for quiet.

### Fixed: Version string

Updated version string from "btorrent/0.9.0" to "btorrent/1.0.2".

**Files:** `src/scheduler.c`, `src/proto/ext.c`

## Test Suite

### Added: Circuit breaker tests

New integration test for circuit breaker pattern.

**File:** `tests/integration/test_circuit_breaker.c`

### Added: Network I/O tests

New integration tests for TCP networking utilities.

**File:** `tests/integration/test_netio.c`

### Total tests: 197 (all passing)
