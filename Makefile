## btorrent Makefile
## Targets: all  debug  test  install  uninstall  dist  clean
##
## Requirements: gcc (or clang), libcurl, pkg-config
## Platform: Linux only (epoll-based scheduler)

## ── Version (single source of truth) ──────────────────────────────────────
VERSION  = 1.0.2
TARNAME  = btorrent-$(VERSION)

## ── Toolchain ─────────────────────────────────────────────────────────────
CC      ?= gcc
CSTD     = -std=c11
WARN     = -Wall -Wextra -Wpedantic -Wshadow
IFLAGS   = -Iinclude
PREFIX  ?= /usr/local

## ── pkg-config for libcurl (falls back to bare -lcurl if unavailable) ─────
CURL_CFLAGS := $(shell pkg-config --cflags libcurl 2>/dev/null)
CURL_LIBS   := $(shell pkg-config --libs   libcurl 2>/dev/null || echo -lcurl)

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
       src/proto/tracker.c    \
       src/dht/dht.c         \
       src/utils.c           \
       src/log.c             \
       src/result.c          \
       src/health.c

OBJS = $(patsubst src/%.c, build/obj/%.o, $(SRCS))
BIN  = build/btorrent

## ── Version flag injected at compile time ─────────────────────────────────
VERFLAGS = -DBT_VERSION=\"$(VERSION)\"

## ── Release ───────────────────────────────────────────────────────────────
all: CFLAGS  = $(CSTD) $(WARN) $(IFLAGS) $(CURL_CFLAGS) $(VERFLAGS) \
               -O2 -DNDEBUG -DLOG_MIN_LEVEL=1 -D_FILE_OFFSET_BITS=64
all: LDFLAGS = $(CURL_LIBS)
all: $(BIN)

## ── Debug (AddressSanitizer + UBSan) ──────────────────────────────────────
debug: CFLAGS  = $(CSTD) $(WARN) $(IFLAGS) $(CURL_CFLAGS) $(VERFLAGS) \
                 -g3 -O0 -fsanitize=address,undefined \
                 -DLOG_MIN_LEVEL=0 -D_FILE_OFFSET_BITS=64
debug: LDFLAGS = $(CURL_LIBS) -fsanitize=address,undefined
debug: $(BIN)

$(BIN): $(OBJS) | build/obj
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

build/obj/%.o: src/%.c | build/obj
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -MMD -MP -c $< -o $@

-include $(OBJS:.o=.d)

build/obj:
	@mkdir -p build/obj/core build/obj/proto build/obj/net \
	           build/obj/cmd  build/obj/dht

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
## Produces btorrent-<VERSION>.tar.gz with a reproducible file list.
## Excludes build artefacts, lock files, and editor droppings.
dist:
	@echo "Creating $(TARNAME).tar.gz ..."
	@rm -rf /tmp/$(TARNAME)
	@mkdir -p /tmp/$(TARNAME)
	@git ls-files 2>/dev/null | tar -T - -c | tar -x -C /tmp/$(TARNAME) \
	    || { echo "Not a git repo — copying all tracked files manually"; \
	         cp -r . /tmp/$(TARNAME)/; \
	         rm -rf /tmp/$(TARNAME)/build; }
	@tar -czf $(TARNAME).tar.gz -C /tmp $(TARNAME)
	@rm -rf /tmp/$(TARNAME)
	@echo "Created $(TARNAME).tar.gz"

## ── Tests ─────────────────────────────────────────────────────────────────
TEST_COMMON = src/utils.c src/log.c src/result.c
TEST_FLAGS  = $(CSTD) $(WARN) $(IFLAGS) -g3 -O0

test_sha1: build/obj
	$(CC) $(TEST_FLAGS) tests/unit/test_sha1.c src/core/sha1.c \
	    $(TEST_COMMON) -o build/test_sha1
	@echo "--- test_sha1 ---" && ./build/test_sha1

test_peer: build/obj
	$(CC) $(TEST_FLAGS) tests/unit/test_peer.c src/proto/peer.c \
	    src/core/sha1.c $(TEST_COMMON) -o build/test_peer
	@echo "--- test_peer ---" && ./build/test_peer

test_pieces: build/obj
	$(CC) $(TEST_FLAGS) tests/unit/test_pieces.c src/core/pieces.c \
	    src/core/sha1.c src/core/torrent.c src/core/bencode.c \
	    src/proto/peer.c $(TEST_COMMON) -o build/test_pieces
	@echo "--- test_pieces ---" && ./build/test_pieces

test_magnet: build/obj
	$(CC) $(TEST_FLAGS) tests/unit/test_magnet.c src/core/magnet.c \
	    $(TEST_COMMON) -o build/test_magnet
	@echo "--- test_magnet ---" && ./build/test_magnet

test_ext: build/obj
	$(CC) $(TEST_FLAGS) tests/unit/test_ext.c \
	    src/core/bencode.c src/core/sha1.c \
	    $(TEST_COMMON) -o build/test_ext
	@echo "--- test_ext ---" && ./build/test_ext

test_scheduler: build/obj
	$(CC) $(TEST_FLAGS) tests/unit/test_scheduler.c \
	    src/core/bencode.c src/core/sha1.c \
	    $(TEST_COMMON) -o build/test_scheduler
	@echo "--- test_scheduler ---" && ./build/test_scheduler

test_publish: build/obj
	$(CC) $(TEST_FLAGS) -DBT_VERSION=\"$(VERSION)\" tests/unit/test_publish.c \
	    src/core/pieces.c src/core/sha1.c src/core/torrent.c \
	    src/core/bencode.c src/proto/peer.c \
	    $(TEST_COMMON) -o build/test_publish
	@echo "--- test_publish ---" && ./build/test_publish

test_circuit: build/obj
	$(CC) $(TEST_FLAGS) tests/integration/test_circuit_breaker.c \
	    $(TEST_COMMON) -o build/test_circuit
	@echo "--- test_circuit ---" && ./build/test_circuit

test_netio: build/obj
	$(CC) $(TEST_FLAGS) tests/integration/test_netio.c \
	    src/net/tcp.c $(TEST_COMMON) -o build/test_netio
	@echo "--- test_netio ---" && ./build/test_netio

test: test_sha1 test_peer test_pieces test_magnet test_ext test_scheduler test_publish test_circuit test_netio

## ── Clean ─────────────────────────────────────────────────────────────────
clean:
	rm -rf build

distclean: clean
	rm -f $(TARNAME).tar.gz

.PHONY: all debug install uninstall dist \
        test test_sha1 test_peer test_pieces test_magnet test_ext test_scheduler test_publish \
        test_circuit test_netio \
        clean distclean
