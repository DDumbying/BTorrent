## btorrent Makefile
## Targets: all  debug  test  install  uninstall  dist  clean  distclean
##
## Requirements: gcc (or clang), libcurl, pkg-config
## Platform: Linux only (epoll-based scheduler)
##
## Options (make VAR=value):
##   CFLAGS / CPPFLAGS / LDFLAGS  appended to ours, so distro build flags
##                                (dpkg-buildflags, makepkg, rpm) apply
##   HARDEN=0    drop our default hardening flags (use when the distro's
##               CFLAGS already provide them, to avoid duplicate defines)
##   WERROR=1    treat warnings as errors (CI)
##   SANITIZE=1  build the unit tests with ASan + UBSan (CI)

## ── Version (single source of truth) ──────────────────────────────────────
VERSION  = 1.1.0
TARNAME  = btorrent-$(VERSION)

## ── Toolchain ─────────────────────────────────────────────────────────────
CC      ?= gcc
CSTD     = -std=c11
WARN     = -Wall -Wextra -Wpedantic -Wshadow
IFLAGS   = -Iinclude
PREFIX  ?= /usr/local

ifeq ($(WERROR),1)
WARN    += -Werror
endif

HARDEN  ?= 1
ifeq ($(HARDEN),1)
HARDEN_CFLAGS  = -fstack-protector-strong -D_FORTIFY_SOURCE=2
HARDEN_LDFLAGS = -Wl,-z,relro,-z,now
endif

## ── pkg-config for libcurl (falls back to bare -lcurl if unavailable) ─────
CURL_CFLAGS := $(shell pkg-config --cflags libcurl 2>/dev/null)
CURL_LIBS   := $(shell pkg-config --libs   libcurl 2>/dev/null || echo -lcurl)
LIBS         = $(CURL_LIBS) -lpthread

## ── Sources ───────────────────────────────────────────────────────────────
SRCS = src/main.c             \
       src/cmd/cmd_download.c \
       src/cmd/cmd_inspect.c  \
       src/cmd/cmd_check.c    \
       src/scheduler.c        \
       src/net/tcp.c          \
       src/core/bencode.c     \
       src/core/torrent.c     \
       src/core/sha1.c        \
       src/core/pieces.c      \
       src/core/magnet.c      \
       src/proto/peer.c       \
       src/proto/ext.c        \
       src/proto/ext_handshake.c \
       src/proto/tracker.c    \
       src/dht/dht.c         \
       src/utils.c           \
       src/log.c             \
       src/result.c          \
       src/health.c

## Release and debug builds use separate object directories and binaries,
## so switching between them never links objects built with the other flags.
REL_OBJS = $(patsubst src/%.c, build/obj/release/%.o, $(SRCS))
DBG_OBJS = $(patsubst src/%.c, build/obj/debug/%.o,   $(SRCS))
BIN      = build/btorrent
DBG_BIN  = build/btorrent-debug

COMMON_CFLAGS = $(CSTD) $(WARN) $(IFLAGS) $(CURL_CFLAGS) \
                -DBT_VERSION=\"$(VERSION)\" -D_FILE_OFFSET_BITS=64
REL_CFLAGS    = -O2 -DNDEBUG -DLOG_MIN_LEVEL=1 $(HARDEN_CFLAGS)
SAN_FLAGS     = -fsanitize=address,undefined -fno-omit-frame-pointer
DBG_CFLAGS    = -g3 -O0 -DLOG_MIN_LEVEL=0 $(SAN_FLAGS)

## ── Release ───────────────────────────────────────────────────────────────
all: $(BIN)

$(BIN): $(REL_OBJS)
	$(CC) $(COMMON_CFLAGS) $(REL_CFLAGS) $(CFLAGS) -o $@ $^ \
	    $(HARDEN_LDFLAGS) $(LDFLAGS) $(LIBS)

build/obj/release/%.o: src/%.c
	@mkdir -p $(dir $@)
	$(CC) $(COMMON_CFLAGS) $(REL_CFLAGS) $(CPPFLAGS) $(CFLAGS) -MMD -MP -c $< -o $@

## ── Debug (AddressSanitizer + UBSan) → build/btorrent-debug ───────────────
debug: $(DBG_BIN)

$(DBG_BIN): $(DBG_OBJS)
	$(CC) $(COMMON_CFLAGS) $(DBG_CFLAGS) $(CFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)

build/obj/debug/%.o: src/%.c
	@mkdir -p $(dir $@)
	$(CC) $(COMMON_CFLAGS) $(DBG_CFLAGS) $(CPPFLAGS) $(CFLAGS) -MMD -MP -c $< -o $@

-include $(REL_OBJS:.o=.d) $(DBG_OBJS:.o=.d)

## ── Install ───────────────────────────────────────────────────────────────
install: all
	install -Dm755 $(BIN)          $(DESTDIR)$(PREFIX)/bin/btorrent
	install -Dm644 docs/btorrent.1 $(DESTDIR)$(PREFIX)/share/man/man1/btorrent.1
	@echo "Installed $(DESTDIR)$(PREFIX)/bin/btorrent"
	@echo "Man page  $(DESTDIR)$(PREFIX)/share/man/man1/btorrent.1"

uninstall:
	rm -f $(DESTDIR)$(PREFIX)/bin/btorrent
	rm -f $(DESTDIR)$(PREFIX)/share/man/man1/btorrent.1

## ── Source tarball ────────────────────────────────────────────────────────
## btorrent-<VERSION>.tar.gz from the committed tree (git archive): the
## output depends only on HEAD, never on untracked or modified local files.
dist:
	@git rev-parse --git-dir >/dev/null 2>&1 || \
	    { echo "make dist needs a git checkout" >&2; exit 1; }
	@git diff --quiet HEAD -- || \
	    echo "warning: uncommitted changes are NOT included in $(TARNAME).tar.gz" >&2
	git archive --format=tar.gz --prefix=$(TARNAME)/ -o $(TARNAME).tar.gz HEAD
	@echo "Created $(TARNAME).tar.gz"

## ── Tests ─────────────────────────────────────────────────────────────────
TEST_COMMON = src/utils.c src/log.c src/result.c
TEST_FLAGS  = $(CSTD) $(WARN) $(IFLAGS) -g3 -O0
ifeq ($(SANITIZE),1)
TEST_FLAGS += $(SAN_FLAGS) -fno-sanitize-recover=all
endif

build:
	@mkdir -p build

test_sha1: | build
	$(CC) $(TEST_FLAGS) tests/unit/test_sha1.c src/core/sha1.c \
	    $(TEST_COMMON) -o build/test_sha1
	@echo "--- test_sha1 ---" && ./build/test_sha1

test_peer: | build
	$(CC) $(TEST_FLAGS) tests/unit/test_peer.c src/proto/peer.c \
	    src/core/sha1.c $(TEST_COMMON) -o build/test_peer
	@echo "--- test_peer ---" && ./build/test_peer

test_pieces: | build
	$(CC) $(TEST_FLAGS) tests/unit/test_pieces.c src/core/pieces.c \
	    src/core/sha1.c src/core/torrent.c src/core/bencode.c \
	    src/proto/peer.c $(TEST_COMMON) -o build/test_pieces
	@echo "--- test_pieces ---" && ./build/test_pieces

test_magnet: | build
	$(CC) $(TEST_FLAGS) tests/unit/test_magnet.c src/core/magnet.c \
	    $(TEST_COMMON) -o build/test_magnet
	@echo "--- test_magnet ---" && ./build/test_magnet

test_ext: | build
	$(CC) $(TEST_FLAGS) tests/unit/test_ext.c \
	    src/core/bencode.c src/core/sha1.c src/proto/ext_handshake.c \
	    $(TEST_COMMON) -o build/test_ext
	@echo "--- test_ext ---" && ./build/test_ext

test_scheduler: | build
	$(CC) $(TEST_FLAGS) tests/unit/test_scheduler.c \
	    src/core/bencode.c src/core/sha1.c src/proto/ext_handshake.c \
	    $(TEST_COMMON) -o build/test_scheduler
	@echo "--- test_scheduler ---" && ./build/test_scheduler

test_metadata: | build
	$(CC) $(TEST_FLAGS) $(CURL_CFLAGS) tests/unit/test_metadata_serve.c \
	    src/core/pieces.c src/core/torrent.c src/core/bencode.c \
	    src/core/sha1.c src/proto/peer.c src/proto/tracker.c \
	    src/proto/ext_handshake.c src/net/tcp.c \
	    $(TEST_COMMON) $(LIBS) -o build/test_metadata
	@echo "--- test_metadata ---" && ./build/test_metadata

test_publish: | build
	$(CC) $(TEST_FLAGS) -DBT_VERSION=\"$(VERSION)\" tests/unit/test_publish.c \
	    src/core/pieces.c src/core/sha1.c src/core/torrent.c \
	    src/core/bencode.c src/proto/peer.c \
	    $(TEST_COMMON) -o build/test_publish
	@echo "--- test_publish ---" && ./build/test_publish

test_circuit: | build
	$(CC) $(TEST_FLAGS) tests/integration/test_circuit_breaker.c \
	    $(TEST_COMMON) -o build/test_circuit
	@echo "--- test_circuit ---" && ./build/test_circuit

test_netio: | build
	$(CC) $(TEST_FLAGS) tests/integration/test_netio.c \
	    src/net/tcp.c $(TEST_COMMON) -o build/test_netio
	@echo "--- test_netio ---" && ./build/test_netio

test_tracker_v6: | build
	$(CC) $(TEST_FLAGS) $(CURL_CFLAGS) tests/unit/test_tracker_v6.c \
	    src/proto/tracker.c src/core/bencode.c \
	    $(TEST_COMMON) $(LIBS) -o build/test_tracker_v6
	@echo "--- test_tracker_v6 ---" && ./build/test_tracker_v6

test: test_sha1 test_peer test_pieces test_magnet test_ext test_scheduler test_metadata test_publish test_circuit test_netio test_tracker_v6

## ── Fuzzing ───────────────────────────────────────────────────────────────
##   make fuzz                     build libFuzzer targets (needs clang)
##   make fuzz-run FUZZ_SECONDS=N  fuzz each target for N s (default 60),
##                                 starting from tests/fuzz/corpus/<target>
##   make fuzz-replay              replay the corpus with $(CC) as a regression
##                                 test (add SANITIZE=1 for ASan + UBSan)
## Crashes are written to build/fuzz/crash-<target>-*; to keep one as a
## regression input, copy it into tests/fuzz/corpus/<target>/.
FUZZ_TARGETS = bencode torrent magnet tracker dht ext wire
FUZZ_CC     ?= clang
FUZZ_SECONDS ?= 60
FUZZ_FLAGS   = $(CSTD) $(IFLAGS) -Itests/fuzz $(CURL_CFLAGS) -g -O1 \
               -DBT_VERSION=\"$(VERSION)\" -D_FILE_OFFSET_BITS=64
FUZZ_BASE    = src/utils.c src/log.c src/result.c

## Sources each target links. Targets that #include a .c file (dht, ext,
## wire — to reach static functions) must not link that file again.
FUZZ_SRCS_bencode = src/core/bencode.c
FUZZ_SRCS_torrent = src/core/torrent.c src/core/bencode.c src/core/sha1.c
FUZZ_SRCS_magnet  = src/core/magnet.c
FUZZ_SRCS_tracker = src/proto/tracker.c src/core/bencode.c
FUZZ_SRCS_dht     = src/core/bencode.c
FUZZ_SRCS_ext     = src/core/torrent.c src/core/bencode.c src/core/sha1.c \
                    src/proto/ext_handshake.c src/proto/tracker.c
FUZZ_SRCS_wire    = src/core/pieces.c src/core/torrent.c src/core/bencode.c \
                    src/core/sha1.c src/proto/peer.c src/proto/tracker.c \
                    src/proto/ext_handshake.c src/net/tcp.c

fuzz: $(addprefix build/fuzz/fuzz_,$(FUZZ_TARGETS))

build/fuzz/fuzz_%: tests/fuzz/fuzz_%.c FORCE | build
	@mkdir -p build/fuzz
	$(FUZZ_CC) $(FUZZ_FLAGS) -fsanitize=fuzzer,address,undefined \
	    -fno-sanitize-recover=all $< $(FUZZ_SRCS_$*) $(FUZZ_BASE) -o $@ $(LIBS)

build/fuzz/replay_%: tests/fuzz/fuzz_%.c tests/fuzz/replay_main.c FORCE | build
	@mkdir -p build/fuzz
	$(CC) $(TEST_FLAGS) -Itests/fuzz $(CURL_CFLAGS) -DBT_VERSION=\"$(VERSION)\" \
	    $< tests/fuzz/replay_main.c $(FUZZ_SRCS_$*) $(FUZZ_BASE) -o $@ $(LIBS)

fuzz-run: fuzz
	@for t in $(FUZZ_TARGETS); do \
	    echo "--- fuzz_$$t ($(FUZZ_SECONDS)s)"; \
	    mkdir -p build/fuzz/corpus/$$t && \
	    cp -n tests/fuzz/corpus/$$t/* build/fuzz/corpus/$$t/ 2>/dev/null; \
	    ./build/fuzz/fuzz_$$t -max_total_time=$(FUZZ_SECONDS) -print_final_stats=1 \
	        -artifact_prefix=build/fuzz/crash-$$t- \
	        build/fuzz/corpus/$$t tests/fuzz/corpus/$$t || exit 1; \
	done

fuzz-replay: $(addprefix build/fuzz/replay_,$(FUZZ_TARGETS))
	@for t in $(FUZZ_TARGETS); do \
	    printf 'fuzz_%-8s ' $$t; ./build/fuzz/replay_$$t tests/fuzz/corpus/$$t || exit 1; \
	done

FORCE:

## ── Clean ─────────────────────────────────────────────────────────────────
clean:
	rm -rf build

distclean: clean
	rm -f $(TARNAME).tar.gz

.PHONY: all debug install uninstall dist \
        test test_sha1 test_peer test_pieces test_magnet test_ext test_scheduler test_metadata test_publish \
        test_circuit test_netio test_tracker_v6 fuzz fuzz-run fuzz-replay FORCE \
        clean distclean
