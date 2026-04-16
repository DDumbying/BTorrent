#define _POSIX_C_SOURCE 200809L
/**
 * scheduler.c — Concurrent peer scheduler (epoll-based)
 *
 * Handles both downloading and seeding in a single epoll loop.
 *
 * Download state machine (outgoing connections):
 *   CONNECTING → HANDSHAKE → INTERESTED → DOWNLOADING ⇄ IDLE
 *
 * Seed state machine (incoming connections on the listen socket):
 *   SEED_HANDSHAKE → SEED_READY ⇄ SEED_UPLOADING
 *
 * PEX (BEP 11) — peers exchange peer lists via the ut_pex extension message.
 * Inbound PEX data is parsed and injected into the peer pool automatically.
 *
 * Rate limiting — token-bucket per direction (upload / download).
 * Tokens refill at the configured KiB/s rate on each epoll tick.
 *
 * Any state → DEAD (error / peer closed)
 */

#include "scheduler.h"
#include "net/tcp.h"
#include "proto/peer.h"
#include "proto/tracker.h"
#include "core/pieces.h"
#include "utils.h"
#include "log.h"
#include "result.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <limits.h>

/* ── Token-bucket rate limiter ───────────────────────────────────────────── */

typedef struct {
    long long tokens;         /* available bytes */
    long long capacity;       /* max burst (2-second worth of rate) */
    long long rate_per_ms;    /* bytes added per millisecond; 0 = unlimited */
    long long last_refill_ms;
} TokenBucket;

static long long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
}

static void tb_init(TokenBucket *tb, int kbs) {
    if (kbs <= 0) {
        tb->rate_per_ms = 0;
        tb->tokens = tb->capacity = 0;
    } else {
        tb->rate_per_ms = (long long)kbs * 1024 / 1000;
        if (tb->rate_per_ms < 1) tb->rate_per_ms = 1;
        tb->capacity = tb->rate_per_ms * 2000;  /* 2-second burst cap */
        tb->tokens   = tb->capacity;
    }
    tb->last_refill_ms = now_ms();
}

static void tb_refill(TokenBucket *tb) {
    if (tb->rate_per_ms == 0) return;
    long long now  = now_ms();
    long long diff = now - tb->last_refill_ms;
    if (diff <= 0) return;
    tb->tokens += diff * tb->rate_per_ms;
    if (tb->tokens > tb->capacity) tb->tokens = tb->capacity;
    tb->last_refill_ms = now;
}

/* Returns how many bytes are allowed now; deducts from the bucket. */
static long long tb_consume(TokenBucket *tb, long long want) {
    if (tb->rate_per_ms == 0) return want;
    tb_refill(tb);
    if (tb->tokens <= 0) return 0;
    long long allowed = tb->tokens < want ? tb->tokens : want;
    tb->tokens -= allowed;
    return allowed;
}

/* ── Per-session read buffer ─────────────────────────────────────────────── */

#define RBUF_SIZE (1024 * 1024)

typedef struct {
    uint8_t *data;
    size_t   len;
    size_t   cap;
} ReadBuf;

static void rbuf_init(ReadBuf *b) {
    b->data = xmalloc(RBUF_SIZE);
    b->len  = 0;
    b->cap  = RBUF_SIZE;
}

static void rbuf_free(ReadBuf *b) {
    free(b->data);
    b->data = NULL;
    b->len  = 0;
}

static int rbuf_fill(ReadBuf *b, int sock) {
    while (b->len < b->cap) {
        ssize_t n = recv(sock, b->data + b->len, b->cap - b->len, 0);
        if (n > 0) { b->len += (size_t)n; continue; }
        if (n == 0) return -1;
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
        return -1;
    }
    return 0;
}

static int rbuf_consume(ReadBuf *b, size_t need, uint8_t *dst) {
    if (b->len < need) return 0;
    if (dst) memcpy(dst, b->data, need);
    memmove(b->data, b->data + need, b->len - need);
    b->len -= need;
    return 1;
}

/* ── Session state machine ───────────────────────────────────────────────── */

typedef enum {
    PS_CONNECTING,      /* outgoing TCP connect in progress */
    PS_HANDSHAKE,       /* sent our HS, waiting for theirs */
    PS_INTERESTED,      /* sent INTERESTED, waiting for UNCHOKE */
    PS_DOWNLOADING,     /* active download; requesting blocks */
    PS_IDLE,            /* unchoked but no piece to request right now */
    PS_SEED_HANDSHAKE,  /* incoming: waiting for peer's handshake */
    PS_SEED_READY,      /* incoming: unchoked, waiting for requests */
    PS_SEED_UPLOADING,  /* incoming: actively serving blocks */
    PS_DEAD,
} PeerPhase;

#define EXT_MSGID      20   /* BEP-10 extension wire message id */
#define META_LOCAL_ID   1   /* our local ext id for ut_metadata */
#define PEX_LOCAL_ID    2   /* our local ext id for ut_pex */

typedef struct {
    int        sock;
    PeerPhase  phase;
    char       ip[16];
    uint16_t   port;
    uint8_t    peer_id[20];

    int        am_choked;      /* download: are WE choked by peer? */
    int        peer_choked;    /* seed: have WE choked the peer? */

    /* BEP-10 ext IDs as advertised by the remote peer */
    int        peer_pex_id;   /* their ut_pex ext msg id (-1 = unsupported) */

    uint8_t   *peer_bitfield;
    int        bf_len;

    /* Download state */
    int        piece_idx;
    int        piece_len;
    int        num_blocks;
    int        blocks_sent;
    int        blocks_recv;

    time_t     last_active;
    time_t     last_keepalive;
    time_t     last_pex;
    time_t     piece_started;
    int        is_incoming;

    /* Circuit breaker: failure tracking */
    int        consecutive_failures;
    time_t     circuit_open_until;

    ReadBuf    rbuf;
} Session;

/* ── Circuit Breaker ────────────────────────────────────────────────────── */

#define CIRCUIT_BREAKER_THRESHOLD  3
#define CIRCUIT_BREAKER_TIMEOUT_S  30

static int is_circuit_open(Session *sessions, int max_s,
                            const char *ip, time_t now) {
    for (int i = 0; i < max_s; i++) {
        if (sessions[i].consecutive_failures >= CIRCUIT_BREAKER_THRESHOLD &&
            now < sessions[i].circuit_open_until) {
            if (strcmp(sessions[i].ip, ip) == 0) return 1;
        }
    }
    return 0;
}

static void record_failure(Session *s, time_t now) {
    s->consecutive_failures++;
    if (s->consecutive_failures >= CIRCUIT_BREAKER_THRESHOLD) {
        s->circuit_open_until = now + CIRCUIT_BREAKER_TIMEOUT_S;
        LOG_INFO("sched: circuit breaker OPEN for %s:%d (%d failures)",
                 s->ip, s->port, s->consecutive_failures);
    }
}

static void record_success(Session *s) {
    s->consecutive_failures = 0;
    s->circuit_open_until = 0;
}

/* ── Handshake ───────────────────────────────────────────────────────────── */

#define HANDSHAKE_LEN  68
#define PSTR           "BitTorrent protocol"
#define PSTRLEN        19

static void build_handshake(uint8_t *buf,
                             const uint8_t *info_hash,
                             const uint8_t *peer_id) {
    buf[0] = PSTRLEN;
    memcpy(buf + 1,  PSTR,      PSTRLEN);
    memset(buf + 20, 0,         8);
    buf[25] = 0x11;   /* BEP-10 extension bit + BEP-5 DHT bit */
    memcpy(buf + 28, info_hash, 20);
    memcpy(buf + 48, peer_id,   20);
}

/* Returns 0 on OK; sets *supports_ext if peer has BEP-10 bit */
static int verify_handshake(const uint8_t *buf, const uint8_t *info_hash,
                             int *supports_ext) {
    if (buf[0] != PSTRLEN)                         return -1;
    if (memcmp(buf + 1,  PSTR,      PSTRLEN) != 0) return -1;
    if (memcmp(buf + 28, info_hash, 20)      != 0) return -1;
    if (supports_ext) *supports_ext = (buf[25] & 0x10) ? 1 : 0;
    return 0;
}

/* ── Non-blocking send (with optional rate limiting) ─────────────────────── */

static int nb_send(int sock, const uint8_t *buf, size_t len) {
    size_t sent = 0;
    int retries = 0;
    while (sent < len) {
        ssize_t n = send(sock, buf + sent, len - sent, MSG_NOSIGNAL);
        if (n > 0)  { sent += (size_t)n; retries = 0; continue; }
        if (n == 0) return -1;
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            if (++retries > 50) return -1;
            struct timespec ts = { 0, 100000 };
            nanosleep(&ts, NULL);
            continue;
        }
        return -1;
    }
    return 0;
}

/* Send up to however many bytes the token bucket allows. */
static void nb_send_limited(int sock, const uint8_t *buf, size_t len,
                             TokenBucket *tb) {
    if (!tb || tb->rate_per_ms == 0) { nb_send(sock, buf, len); return; }
    long long allowed = tb_consume(tb, (long long)len);
    if (allowed > 0)
        nb_send(sock, buf, (size_t)allowed);
}

/* ── BEP-10 extension handshake ─────────────────────────────────────────── */
/*
 * Advertise both ut_metadata (id=1) and ut_pex (id=2).
 * The version string "btorrent/0.9.0" is exactly 14 bytes.
 */
static int build_ext_handshake(uint8_t *buf, size_t cap) {
    int n = snprintf((char *)buf, cap,
        "d"
            "1:m" "d"
                "11:ut_metadata" "i%de"
                "6:ut_pex"       "i%de"
            "e"
            "1:v" "15:btorrent/1.0.2"
        "e",
        META_LOCAL_ID, PEX_LOCAL_ID);
    return (n > 0 && (size_t)n < cap) ? n : -1;
}

static void send_ext_handshake(int sock) {
    uint8_t body[256];
    body[0] = 0;  /* sub-id 0 = BEP-10 handshake */
    int blen = build_ext_handshake(body + 1, sizeof(body) - 1);
    if (blen < 0) return;
    uint8_t hdr[5];
    write_uint32_be(hdr, (uint32_t)(1 + blen));
    hdr[4] = EXT_MSGID;
    nb_send(sock, hdr,  5);
    nb_send(sock, body, (size_t)(1 + blen));
}

/* ── PEX ─────────────────────────────────────────────────────────────────── */

/*
 * Build a compact ut_pex "added" list from currently-connected outgoing peers.
 * Format: d 5:added <N*6>:<compact-ipv4-peers> e
 */
static int build_pex_body(uint8_t *buf, size_t cap,
                           Session *sessions, int max_s, int self_idx) {
    uint8_t compact[50 * 6];
    int     count = 0;
    for (int i = 0; i < max_s && count < 50; i++) {
        if (i == self_idx || sessions[i].is_incoming) continue;
        Session *s = &sessions[i];
        if (s->phase == PS_DEAD || s->sock < 0) continue;
        struct in_addr addr;
        if (inet_pton(AF_INET, s->ip, &addr) != 1) continue;
        memcpy(compact + count * 6, &addr.s_addr, 4);
        compact[count * 6 + 4] = (uint8_t)(s->port >> 8);
        compact[count * 6 + 5] = (uint8_t)(s->port & 0xFF);
        count++;
    }
    if (count == 0) return 0;
    int n = snprintf((char *)buf, cap, "d5:added%d:", count * 6);
    if (n < 0 || (size_t)(n + count * 6 + 1) >= cap) return -1;
    memcpy(buf + n, compact, (size_t)(count * 6));
    n += count * 6;
    buf[n++] = 'e';
    return n;
}

static void send_pex(Session *s, Session *all, int max_s, int self_idx) {
    if (s->peer_pex_id <= 0 || s->sock < 0) return;
    uint8_t body[400];
    body[0] = (uint8_t)s->peer_pex_id;
    int blen = build_pex_body(body + 1, sizeof(body) - 1, all, max_s, self_idx);
    if (blen <= 0) return;
    uint8_t hdr[5];
    write_uint32_be(hdr, (uint32_t)(1 + blen));
    hdr[4] = EXT_MSGID;
    nb_send(s->sock, hdr,  5);
    nb_send(s->sock, body, (size_t)(1 + blen));
}

/* Parse ut_pex peer's advertised ut_pex ext ID from their ext handshake. */
static void parse_peer_ext_hs(const uint8_t *data, uint32_t len,
                               int *out_pex_id) {
    *out_pex_id = -1;
    const char *needle = "6:ut_pex";
    size_t nlen = strlen(needle);
    for (uint32_t i = 0; i + nlen + 3 < len; i++) {
        if (memcmp(data + i, needle, nlen) != 0) continue;
        uint32_t j = i + (uint32_t)nlen;
        if (data[j] != 'i') continue;
        j++;
        int val = 0;
        while (j < len && data[j] >= '0' && data[j] <= '9')
            val = val * 10 + (data[j++] - '0');
        if (j < len && data[j] == 'e') *out_pex_id = val;
        break;
    }
}

/* Parse the compact "added" field from a ut_pex data payload. */
static int parse_pex_peers(const uint8_t *payload, uint32_t plen,
                            Peer *out, int max_out) {
    const char *needle = "5:added";
    size_t nlen = strlen(needle);
    int found = 0;
    for (uint32_t i = 0; i + nlen + 2 < plen; i++) {
        if (memcmp(payload + i, needle, nlen) != 0) continue;
        uint32_t j = i + (uint32_t)nlen;
        int compact_len = 0;
        while (j < plen && payload[j] >= '0' && payload[j] <= '9')
            compact_len = compact_len * 10 + (payload[j++] - '0');
        if (j >= plen || payload[j] != ':') break;
        j++;
        for (int k = 0; k + 6 <= compact_len && found < max_out; k += 6) {
            if (j + (uint32_t)(k + 6) > plen) break;
            const uint8_t *p = payload + j + k;
            struct in_addr addr;
            memcpy(&addr.s_addr, p, 4);
            char ip[16];
            if (!inet_ntop(AF_INET, &addr, ip, sizeof(ip))) continue;
            uint16_t port = (uint16_t)((p[4] << 8) | p[5]);
            if (port == 0) continue;
            strncpy(out[found].ip, ip, 15);
            out[found].ip[15] = '\0';
            out[found].port   = port;
            found++;
        }
        break;
    }
    return found;
}

/* Merge new peers into the pool, deduplicating by IP:port. */
static int inject_peers(PeerList *peers, const Peer *new_peers, int count) {
    int added = 0;
    for (int i = 0; i < count; i++) {
        int dup = 0;
        for (int k = 0; k < peers->count; k++) {
            if (strcmp(peers->peers[k].ip, new_peers[i].ip) == 0 &&
                peers->peers[k].port == new_peers[i].port) { dup = 1; break; }
        }
        if (!dup) {
            void *tmp = realloc(peers->peers,
                (size_t)(peers->count + 1) * sizeof(Peer));
            if (!tmp) break;
            peers->peers = tmp;
            peers->peers[peers->count++] = new_peers[i];
            added++;
        }
    }
    return added;
}

/* ── Piece selection ─────────────────────────────────────────────────────── */

static int next_rarest(PieceManager *pm,
                        Session *sessions, int max_s,
                        const uint8_t *peer_bf, int num_pieces) {
    int *avail = xcalloc((size_t)pm->num_pieces, sizeof(int));
    for (int s = 0; s < max_s; s++) {
        if (sessions[s].phase == PS_DEAD || !sessions[s].peer_bitfield) continue;
        for (int i = 0; i < pm->num_pieces; i++)
            if (bitfield_has_piece(sessions[s].peer_bitfield, i)) avail[i]++;
    }
    int best = -1, best_n = INT_MAX;
    for (int i = 0; i < pm->num_pieces; i++) {
        if (pm->pieces[i].state != PIECE_EMPTY) continue;
        if (peer_bf && i < num_pieces && !bitfield_has_piece(peer_bf, i)) continue;
        if (avail[i] > 0 && avail[i] < best_n) { best = i; best_n = avail[i]; }
    }
    if (best == -1 && peer_bf) {
        for (int i = 0; i < pm->num_pieces; i++) {
            if (pm->pieces[i].state != PIECE_EMPTY) continue;
            if (i < num_pieces && bitfield_has_piece(peer_bf, i)) { best = i; break; }
        }
    }
    free(avail);
    return best;
}

/*
 * endgame_threshold — returns 1 if we are in endgame:
 * fewer than 1% of pieces remain AND at least one is still in-flight.
 * Once triggered, assign_piece broadcasts all remaining pieces to every
 * peer that has them rather than serialising one peer per piece.
 */
static int in_endgame(const PieceManager *pm) {
    int empty = 0, active = 0;
    for (int i = 0; i < pm->num_pieces; i++) {
        if (pm->pieces[i].state == PIECE_EMPTY)    empty++;
        if (pm->pieces[i].state == PIECE_ACTIVE ||
            pm->pieces[i].state == PIECE_ASSIGNED)  active++;
    }
    /* Endgame: nothing empty left, but some pieces still in flight */
    return (empty == 0 && active > 0);
}

/* ── Wire message helpers ────────────────────────────────────────────────── */

static void send_have_msg(int sock, int pi) {
    uint8_t buf[9];
    write_uint32_be(buf, 5); buf[4] = MSG_HAVE;
    write_uint32_be(buf + 5, (uint32_t)pi);
    send(sock, buf, 9, MSG_NOSIGNAL);
}
static void send_keepalive(int sock) {
    uint8_t buf[4] = {0,0,0,0};
    send(sock, buf, 4, MSG_NOSIGNAL);
}
static void send_unchoke(int sock) {
    uint8_t buf[5]; write_uint32_be(buf, 1); buf[4] = MSG_UNCHOKE;
    send(sock, buf, 5, MSG_NOSIGNAL);
}

/* Send MSG_PIECE block to a requesting peer. */
static int serve_block(Session *s, PieceManager *pm,
                        const TorrentInfo *torrent,
                        int pi, int begin, int length,
                        TokenBucket *ul_bucket) {
    int plen = torrent_get_piece_length(torrent, pi);
    if (begin < 0 || length <= 0 || begin + length > plen) return -1;
    if (pm->pieces[pi].state != PIECE_COMPLETE) return -1;

    uint8_t *piece_data = xmalloc((size_t)plen);
    if (!piece_manager_read_piece(pm, pi, piece_data)) {
        free(piece_data); return -1;
    }

    /* Header: [4:len=9+block][1:id=7][4:index][4:begin] */
    uint8_t hdr[13];
    write_uint32_be(hdr,     (uint32_t)(9 + length));
    hdr[4] = MSG_PIECE;
    write_uint32_be(hdr + 5,  (uint32_t)pi);
    write_uint32_be(hdr + 9,  (uint32_t)begin);
    nb_send(s->sock, hdr, 13);
    nb_send_limited(s->sock, piece_data + begin, (size_t)length, ul_bucket);

    free(piece_data);
    LOG_DEBUG("seed: %s:%d ← piece %d begin=%d len=%d",
              s->ip, s->port, pi, begin, length);
    return 0;
}

/* ── Session lifecycle ───────────────────────────────────────────────────── */

static void session_init(Session *s, int sock, const char *ip, uint16_t port,
                          int incoming) {
    s->sock          = sock;
    s->phase         = incoming ? PS_SEED_HANDSHAKE : PS_CONNECTING;
    s->piece_idx     = -1;
    s->am_choked     = 1;
    s->peer_choked   = 1;
    s->peer_pex_id   = -1;
    s->last_active   = time(NULL);
    s->last_keepalive= time(NULL);
    s->last_pex      = time(NULL);
    s->piece_started = 0;
    s->blocks_sent   = s->blocks_recv = 0;
    s->peer_bitfield = NULL;
    s->bf_len        = 0;
    s->is_incoming   = incoming;
    s->consecutive_failures = 0;
    s->circuit_open_until = 0;
    memset(s->peer_id, 0, 20);
    strncpy(s->ip, ip, 15); s->ip[15] = '\0';
    s->port = port;
    rbuf_init(&s->rbuf);
}

static void session_close(Session *s, int epfd) {
    if (s->sock >= 0) {
        epoll_ctl(epfd, EPOLL_CTL_DEL, s->sock, NULL);
        close(s->sock); s->sock = -1;
    }
    free(s->peer_bitfield); s->peer_bitfield = NULL;
    rbuf_free(&s->rbuf);
    s->phase = PS_DEAD;
}

/* ── epoll helper ────────────────────────────────────────────────────────── */

static void epoll_watch(int epfd, int fd, uint32_t ev, int idx) {
    struct epoll_event e = { .events = ev, .data.u32 = (uint32_t)idx };
    epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &e);
}

/* ── Block request pipeline ──────────────────────────────────────────────── */

static int send_requests(Session *s, const Config *cfg) {
    while (s->blocks_sent < s->num_blocks &&
           s->blocks_sent - s->blocks_recv < cfg->pipeline_depth) {
        int beg  = s->blocks_sent * BLOCK_SIZE;
        int blen = (beg + BLOCK_SIZE > s->piece_len)
                   ? s->piece_len - beg : BLOCK_SIZE;
        PeerConn tmp = { .sock = s->sock };
        if (peer_send_request(&tmp, (uint32_t)s->piece_idx,
                              (uint32_t)beg, (uint32_t)blen) < 0) return -1;
        s->blocks_sent++;
    }
    return 0;
}

static void assign_piece(Session *s, PieceManager *pm,
                          const TorrentInfo *torrent,
                          Session *all, int max_s, const Config *cfg) {
    if (s->phase != PS_DOWNLOADING || s->am_choked || s->piece_idx >= 0) return;

    /* ── Active-piece memory cap ───────────────────────────────────────────
     * Each PIECE_ACTIVE slot holds up to piece_length bytes (up to 4 MiB).
     * With 50 peers this could hit 200 MiB simultaneously.  Cap concurrent
     * active pieces at max_peers/2 (floor 8) to bound peak RAM usage. */
    int max_active = (cfg->max_peers / 2 < 8) ? 8 : cfg->max_peers / 2;
    int active_count = 0;
    for (int i = 0; i < pm->num_pieces; i++)
        if (pm->pieces[i].state == PIECE_ACTIVE) active_count++;
    if (active_count >= max_active) return; /* wait for a slot to free up */

    /* ── Endgame mode ──────────────────────────────────────────────────────
     * When every piece is either ASSIGNED/ACTIVE (in-flight) or COMPLETE,
     * and at least one is still in-flight, broadcast requests for all
     * remaining pieces to every peer that has them.  Duplicates are fine —
     * dispatch_msg sends MSG_CANCEL on completion. */
    if (in_endgame(pm)) {
        for (int i = 0; i < pm->num_pieces; i++) {
            if (pm->pieces[i].state == PIECE_COMPLETE) continue;
            if (!s->peer_bitfield) continue;
            if (!bitfield_has_piece(s->peer_bitfield, i)) continue;
            /* Request this piece from this peer too */
            s->piece_idx  = i;
            s->piece_len  = torrent_get_piece_length(torrent, i);
            s->num_blocks = (s->piece_len + BLOCK_SIZE - 1) / BLOCK_SIZE;
            s->blocks_sent = s->blocks_recv = 0;
            LOG_DEBUG("endgame: %s:%d → piece %d", s->ip, s->port, i);
            if (send_requests(s, cfg) < 0) s->phase = PS_DEAD;
            return;
        }
        s->phase = PS_IDLE;
        return;
    }
    /* ── Normal mode ─────────────────────────────────────────────────────── */
    int pi = next_rarest(pm, all, max_s, s->peer_bitfield, torrent->num_pieces);
    if (pi < 0) { s->phase = PS_IDLE; return; }
    s->piece_idx  = pi;
    s->piece_len  = torrent_get_piece_length(torrent, pi);
    s->num_blocks = (s->piece_len + BLOCK_SIZE - 1) / BLOCK_SIZE;
    s->blocks_sent = s->blocks_recv = 0;
    pm->pieces[pi].state = PIECE_ASSIGNED;
    s->piece_started     = time(NULL);
    LOG_DEBUG("sched: %s:%d → piece %d/%d", s->ip, s->port, pi, pm->num_pieces-1);
    if (send_requests(s, cfg) < 0) s->phase = PS_DEAD;
}

static void return_piece(Session *s, PieceManager *pm) {
    if (s->piece_idx < 0) return;
    int pi = s->piece_idx;
    if (pm->pieces[pi].state == PIECE_ACTIVE) {
        free(pm->pieces[pi].data); pm->pieces[pi].data = NULL;
        pm->pieces[pi].state = PIECE_EMPTY;
        memset(pm->pieces[pi].block_received, 0,
               (size_t)pm->pieces[pi].num_blocks);
        pm->pieces[pi].blocks_done = 0;
    } else if (pm->pieces[pi].state == PIECE_ASSIGNED) {
        pm->pieces[pi].state = PIECE_EMPTY;
    }
    s->piece_idx = -1;
    s->blocks_sent = s->blocks_recv = 0;
}

/* ── dispatch_msg ────────────────────────────────────────────────────────── */

static void dispatch_msg(Session *s, int sidx,
                          uint8_t id, uint8_t *payload, uint32_t plen,
                          int epfd,
                          const TorrentInfo *torrent, PieceManager *pm,
                          Session *all, int max_s,
                          const Config *cfg, PeerList *peers,
                          TokenBucket *ul_bucket) {
    int bf_bytes = (torrent->num_pieces + 7) / 8;
    (void)epfd;

    switch (id) {

    /* ── Standard download messages ── */

    case MSG_CHOKE:
        s->am_choked = 1;
        return_piece(s, pm);
        s->phase = PS_IDLE;
        { uint8_t m[5]; write_uint32_be(m,1); m[4]=MSG_INTERESTED; nb_send(s->sock,m,5); }
        break;

    case MSG_UNCHOKE:
        s->am_choked = 0;
        if (s->phase == PS_INTERESTED || s->phase == PS_IDLE) s->phase = PS_DOWNLOADING;
        break;

    case MSG_HAVE: {
        if (plen != 4) { s->phase = PS_DEAD; break; }
        uint32_t pi = read_uint32_be(payload);
        if ((int)pi < torrent->num_pieces && s->peer_bitfield)
            bitfield_set_piece(s->peer_bitfield, (int)pi);
        if (s->phase == PS_IDLE && !s->am_choked) s->phase = PS_DOWNLOADING;
        break;
    }

    case MSG_BITFIELD: {
        if (!s->peer_bitfield) s->peer_bitfield = xcalloc((size_t)bf_bytes, 1);
        s->bf_len = bf_bytes;
        uint32_t copy = plen < (uint32_t)bf_bytes ? plen : (uint32_t)bf_bytes;
        memcpy(s->peer_bitfield, payload, copy);
        LOG_INFO("peer %s:%d: BITFIELD", s->ip, s->port);
        break;
    }

    case MSG_PIECE: {
        if (plen < 8) { s->phase = PS_DEAD; break; }
        int pi    = (int)read_uint32_be(payload);
        int begin = (int)read_uint32_be(payload + 4);
        int dlen  = (int)(plen - 8);
        int result = piece_manager_on_block(pm, pi, begin, payload + 8, dlen);
        if (pi == s->piece_idx) s->blocks_recv++;
        if (result == 1) {
            /* Piece verified — broadcast HAVE and cancel any endgame duplicates */
            for (int i = 0; i < max_s; i++) {
                if (all[i].sock < 0 || all[i].phase == PS_DEAD) continue;
                send_have_msg(all[i].sock, pi);
                /* Cancel redundant endgame requests for this piece */
                if (i != sidx && all[i].piece_idx == pi) {
                    uint8_t cancel[17];
                    write_uint32_be(cancel,      13);
                    cancel[4] = MSG_CANCEL;
                    write_uint32_be(cancel + 5,  (uint32_t)pi);
                    write_uint32_be(cancel + 9,  0);
                    write_uint32_be(cancel + 13, (uint32_t)torrent_get_piece_length(torrent, pi));
                    nb_send(all[i].sock, cancel, 17);
                    all[i].piece_idx   = -1;
                    all[i].blocks_sent = all[i].blocks_recv = 0;
                }
            }
            s->piece_idx  = -1;
            s->blocks_sent = s->blocks_recv = 0;
            s->phase      = PS_DOWNLOADING;
        } else if (result == -1) {
            s->piece_idx  = -1;
            s->blocks_sent = s->blocks_recv = 0;
        } else {
            if (send_requests(s, cfg) < 0) s->phase = PS_DEAD;
        }
        break;
    }

    /* ── Seed: serve uploaded blocks ── */

    case MSG_REQUEST: {
        if (plen < 12) { s->phase = PS_DEAD; break; }
        int req_pi    = (int)read_uint32_be(payload);
        int req_begin = (int)read_uint32_be(payload + 4);
        int req_len   = (int)read_uint32_be(payload + 8);
        if (s->peer_choked) break;
        if (req_pi >= torrent->num_pieces || req_len <= 0 || req_len > 32768) break;
        if (serve_block(s, pm, torrent, req_pi, req_begin, req_len, ul_bucket) == 0)
            if (s->phase == PS_SEED_READY) s->phase = PS_SEED_UPLOADING;
        break;
    }

    case MSG_CANCEL:
        /* We serve synchronously, so CANCEL arrives after we've already sent */
        break;

    /* ── BEP-10 extension messages ── */

    case EXT_MSGID: {
        if (plen < 2) break;
        uint8_t sub = payload[0];

        if (sub == 0) {
            /* Extension handshake */
            parse_peer_ext_hs(payload + 1, plen - 1, &s->peer_pex_id);
            LOG_DEBUG("peer %s:%d: ext hs, pex_id=%d", s->ip, s->port, s->peer_pex_id);
        } else if (sub == PEX_LOCAL_ID) {
            /* ut_pex data */
            Peer new_peers[50];
            int n = parse_pex_peers(payload + 1, plen - 1, new_peers, 50);
            if (n > 0) {
                int added = inject_peers(peers, new_peers, n);
                if (added > 0)
                    LOG_INFO("pex: %s:%d → +%d peers (pool=%d)",
                             s->ip, s->port, added, peers->count);
            }
        }
        /* sub == META_LOCAL_ID handled in ext.c / metadata-fetch phase */
        break;
    }

    default: break;
    }

    (void)sidx; (void)bf_bytes;
}

/* ── handle_session ──────────────────────────────────────────────────────── */

static void handle_session(Session *s, uint32_t ev_flags,
                            int epfd, int idx,
                            const TorrentInfo *torrent,
                            PieceManager *pm,
                            const uint8_t *info_hash,
                            const uint8_t *our_peer_id,
                            Session *all, int max_s,
                            const Config *cfg, PeerList *peers,
                            TokenBucket *ul_bucket) {
    s->last_active = time(NULL);
    int bf_bytes   = (torrent->num_pieces + 7) / 8;

    /* ── Outgoing: finish TCP connect ── */
    if (s->phase == PS_CONNECTING) {
        if (!(ev_flags & EPOLLOUT)) { session_close(s, epfd); return; }
        if (tcp_finish_connect(s->sock) < 0) { session_close(s, epfd); return; }
        tcp_set_timeouts(s->sock, cfg->peer_timeout_s);
        uint8_t hs[HANDSHAKE_LEN];
        build_handshake(hs, info_hash, our_peer_id);
        if (nb_send(s->sock, hs, HANDSHAKE_LEN) < 0) { session_close(s,epfd); return; }
        LOG_INFO("peer %s:%d: connected", s->ip, s->port);
        s->phase         = PS_HANDSHAKE;
        s->peer_bitfield = xcalloc((size_t)bf_bytes, 1);
        s->bf_len        = bf_bytes;
        epoll_watch(epfd, s->sock, EPOLLIN | EPOLLET, idx);
        return;
    }

    /* ── Outgoing: receive peer's handshake ── */
    if (s->phase == PS_HANDSHAKE) {
        if (rbuf_fill(&s->rbuf, s->sock) < 0) { session_close(s,epfd); return; }
        uint8_t their_hs[HANDSHAKE_LEN];
        if (!rbuf_consume(&s->rbuf, HANDSHAKE_LEN, their_hs)) return;
        int supports_ext = 0;
        if (verify_handshake(their_hs, info_hash, &supports_ext) < 0) {
            record_failure(s, time(NULL));
            session_close(s, epfd); return;
        }
        record_success(s);
        memcpy(s->peer_id, their_hs + 48, 20);
        LOG_INFO("peer %s:%d: HS OK (ext=%d)", s->ip, s->port, supports_ext);

        if (pm->completed > 0) {
            uint8_t *bfmsg = xmalloc((size_t)(5 + pm->bf_len));
            write_uint32_be(bfmsg, (uint32_t)(1 + pm->bf_len));
            bfmsg[4] = MSG_BITFIELD;
            memcpy(bfmsg + 5, pm->our_bitfield, (size_t)pm->bf_len);
            nb_send(s->sock, bfmsg, (size_t)(5 + pm->bf_len));
            free(bfmsg);
        }
        if (supports_ext) send_ext_handshake(s->sock);

        uint8_t interested[5];
        write_uint32_be(interested, 1); interested[4] = MSG_INTERESTED;
        if (nb_send(s->sock, interested, 5) < 0) { session_close(s,epfd); return; }
        s->am_choked = 1;
        s->phase     = PS_INTERESTED;
        return;
    }

    /* ── Incoming seed: receive peer's handshake ── */
    if (s->phase == PS_SEED_HANDSHAKE) {
        if (rbuf_fill(&s->rbuf, s->sock) < 0) { session_close(s,epfd); return; }
        uint8_t their_hs[HANDSHAKE_LEN];
        if (!rbuf_consume(&s->rbuf, HANDSHAKE_LEN, their_hs)) return;
        int supports_ext = 0;
        if (verify_handshake(their_hs, info_hash, &supports_ext) < 0) {
            LOG_DEBUG("seed: bad HS from %s:%d", s->ip, s->port);
            session_close(s, epfd); return;
        }
        memcpy(s->peer_id, their_hs + 48, 20);

        /* Reply with our handshake */
        uint8_t hs[HANDSHAKE_LEN];
        build_handshake(hs, info_hash, our_peer_id);
        nb_send(s->sock, hs, HANDSHAKE_LEN);

        /* Send our BITFIELD */
        uint8_t *bfmsg = xmalloc((size_t)(5 + pm->bf_len));
        write_uint32_be(bfmsg, (uint32_t)(1 + pm->bf_len));
        bfmsg[4] = MSG_BITFIELD;
        memcpy(bfmsg + 5, pm->our_bitfield, (size_t)pm->bf_len);
        nb_send(s->sock, bfmsg, (size_t)(5 + pm->bf_len));
        free(bfmsg);

        if (supports_ext) send_ext_handshake(s->sock);

        /* Unchoke immediately — simple altruistic seeding policy */
        s->peer_choked = 0;
        send_unchoke(s->sock);

        s->peer_bitfield = xcalloc((size_t)bf_bytes, 1);
        s->bf_len        = bf_bytes;
        s->phase         = PS_SEED_READY;
        LOG_INFO("seed: %s:%d connected", s->ip, s->port);
        return;
    }

    /* ── All data-bearing states: read and dispatch messages ── */
    if (!(ev_flags & EPOLLIN)) return;
    if (rbuf_fill(&s->rbuf, s->sock) < 0) {
        LOG_INFO("peer %s:%d: disconnected (phase=%d)", s->ip, s->port, s->phase);
        session_close(s, epfd); return;
    }

    while (s->phase != PS_DEAD) {
        if (s->rbuf.len < 4) break;
        uint32_t msg_len = read_uint32_be(s->rbuf.data);
        if (msg_len == 0) { rbuf_consume(&s->rbuf, 4, NULL); continue; }
        if (msg_len > 16 * 1024 * 1024) { session_close(s, epfd); return; }
        if (s->rbuf.len < 4 + msg_len) break;

        rbuf_consume(&s->rbuf, 4, NULL);
        uint8_t wire_id = 0;
        rbuf_consume(&s->rbuf, 1, &wire_id);
        uint32_t plen   = msg_len - 1;
        uint8_t *payload = plen ? xmalloc(plen) : NULL;
        if (plen) rbuf_consume(&s->rbuf, plen, payload);

        dispatch_msg(s, idx, wire_id, payload, plen, epfd,
                     torrent, pm, all, max_s, cfg, peers, ul_bucket);
        free(payload);

        if (s->phase == PS_DOWNLOADING && !s->am_choked && s->piece_idx < 0)
            assign_piece(s, pm, torrent, all, max_s, cfg);
    }

    (void)bf_bytes;
}

/* ── Listen socket ───────────────────────────────────────────────────────── */

static int create_listen_sock(uint16_t port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;
    int one = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    int flags = fcntl(sock, F_GETFL, 0);
    if (flags >= 0) fcntl(sock, F_SETFL, flags | O_NONBLOCK);
    struct sockaddr_in addr = {0};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port);
    if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0 ||
        listen(sock, 16) < 0) { close(sock); return -1; }
    return sock;
}

static int accept_incoming(int lsock, int epfd, Session *sessions, int max_s) {
    struct sockaddr_in peer_addr;
    socklen_t addrlen = sizeof(peer_addr);
    int conn = accept(lsock, (struct sockaddr *)&peer_addr, &addrlen);
    if (conn < 0) return -1;
    int flags = fcntl(conn, F_GETFL, 0);
    if (flags >= 0) fcntl(conn, F_SETFL, flags | O_NONBLOCK);
    int one = 1;
    setsockopt(conn, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    int slot = -1;
    for (int i = 0; i < max_s; i++) {
        if (sessions[i].phase == PS_DEAD && sessions[i].sock < 0) { slot = i; break; }
    }
    if (slot < 0) { close(conn); return -1; }

    char ip[16] = "0.0.0.0";
    inet_ntop(AF_INET, &peer_addr.sin_addr, ip, sizeof(ip));
    uint16_t port = ntohs(peer_addr.sin_port);

    session_init(&sessions[slot], conn, ip, port, /*incoming=*/1);
    struct epoll_event ev = { .events = EPOLLIN | EPOLLET, .data.u32 = (uint32_t)slot };
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, conn, &ev) < 0) {
        session_close(&sessions[slot], epfd); return -1;
    }
    LOG_INFO("seed: accepted %s:%d → slot %d", ip, port, slot);
    return slot;
}

/* ── open_connection ─────────────────────────────────────────────────────── */

static int open_connection(Session *sessions, int max_s,
                            Session *s, int epfd, int idx,
                            const char *ip, uint16_t port, time_t now) {
    if (is_circuit_open(sessions, max_s, ip, now)) {
        LOG_DEBUG("sched: circuit breaker open for %s:%d — skipping", ip, port);
        return -1;
    }
    int sock = tcp_connect_nb(ip, port);
    if (sock < 0) return -1;
    session_init(s, sock, ip, port, /*incoming=*/0);
    struct epoll_event ev = { .events = EPOLLOUT | EPOLLET, .data.u32 = (uint32_t)idx };
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, sock, &ev) < 0) {
        close(sock); s->sock = -1; rbuf_free(&s->rbuf); return -1;
    }
    return 0;
}

/* ── scheduler_run ───────────────────────────────────────────────────────── */

int scheduler_run(const TorrentInfo *torrent,
                  PieceManager      *pm,
                  PeerList          *peers,
                  const uint8_t     *peer_id,
                  const Config      *cfg,
                  volatile sig_atomic_t *interrupted) {

    int max_s = cfg->max_peers > 0 ? cfg->max_peers : 50;

    TokenBucket ul_bucket;
    tb_init(&ul_bucket, cfg->upload_limit_kbs);

    Session *sessions = xcalloc((size_t)max_s, sizeof(Session));
    for (int i = 0; i < max_s; i++) {
        sessions[i].sock      = -1;
        sessions[i].phase     = PS_DEAD;
        sessions[i].rbuf.data = NULL;
    }

    int epfd = epoll_create1(EPOLL_CLOEXEC);
    if (epfd < 0) { free(sessions); return EXIT_FAILURE; }

    /*
     * Listen socket sentinel: we use index max_s in epoll data to
     * distinguish the listen fd from session fds (which are 0..max_s-1).
     */
    int listen_sock = -1;
    int downloading = !piece_manager_is_complete(pm);

    if (cfg->seed || !downloading) {
        listen_sock = create_listen_sock(cfg->port);
        if (listen_sock >= 0) {
            struct epoll_event lev = { .events   = EPOLLIN,
                                       .data.u32 = (uint32_t)max_s };
            epoll_ctl(epfd, EPOLL_CTL_ADD, listen_sock, &lev);
            LOG_INFO("seed: listening on port %d", cfg->port);
        }
    }

    int    peer_cursor    = 0;
    int    active         = 0;
    time_t last_announce  = time(NULL);
    int    announce_int   = peers->interval > 0 ? peers->interval : 1800;
    time_t last_progress  = time(NULL);
    time_t last_pex_bcast    = time(NULL);
    time_t last_choke_rotate = time(NULL);

    for (int i = 0; i < max_s && peer_cursor < peers->count; i++) {
        const Peer *p = &peers->peers[peer_cursor++];
        time_t now = time(NULL);
        if (open_connection(sessions, max_s, &sessions[i], epfd, i, p->ip, p->port, now) == 0) active++;
    }
    LOG_INFO("sched: %d connections opened (max %d)", active, max_s);

    struct epoll_event events[64];

    while (!(*interrupted)) {

        /* Transition: download just finished */
        if (downloading && piece_manager_is_complete(pm)) {
            downloading = 0;
            LOG_INFO("%s", "sched: download complete");
            if (!cfg->seed) break;
            if (listen_sock < 0) {
                listen_sock = create_listen_sock(cfg->port);
                if (listen_sock >= 0) {
                    struct epoll_event lev = { .events   = EPOLLIN,
                                               .data.u32 = (uint32_t)max_s };
                    epoll_ctl(epfd, EPOLL_CTL_ADD, listen_sock, &lev);
                }
            }
            LOG_INFO("seed: now seeding on port %d — Ctrl+C to stop", cfg->port);
        }

        /* Re-announce — normally at tracker interval, but immediately if
         * we are critically short on active peers (< 3 connected sessions).
         * This handles the case where a tracker returns only 1 peer and
         * that peer is slow — we need fresh peers right away, not in 30 min. */
        int live_peers = 0;
        for (int i = 0; i < max_s; i++) {
            if (sessions[i].sock >= 0 && sessions[i].phase != PS_DEAD &&
                sessions[i].phase != PS_CONNECTING && !sessions[i].is_incoming)
                live_peers++;
        }
        int announce_due = (time(NULL) - last_announce >= announce_int) ||
                           (downloading && live_peers < 3 &&
                            time(NULL) - last_announce >= 60);
        if (announce_due) {
            long dl = (long)pm->completed * torrent->piece_length;
            if (live_peers < 3)
                LOG_INFO("sched: only %d live peers — re-announcing now", live_peers);
            PeerList np = tracker_announce_with_retry(
                torrent, peer_id, cfg->port,
                dl, 0, torrent->total_length - dl,
                piece_manager_is_complete(pm) ? "completed" : NULL);
            if (np.count > 0) {
                int added = inject_peers(peers, np.peers, np.count);
                if (added > 0) LOG_INFO("sched: +%d tracker peers", added);
                if (np.interval > 0) announce_int = np.interval;
                peer_list_free(&np);
            }
            last_announce = time(NULL);
        }

        /* Assign pieces */
        if (downloading) {
            for (int i = 0; i < max_s; i++) {
                Session *s = &sessions[i];
                if (s->phase == PS_DOWNLOADING && !s->am_choked && s->piece_idx < 0)
                    assign_piece(s, pm, torrent, sessions, max_s, cfg);
            }
        }

        /* Keepalive */
        time_t now_ka = time(NULL);
        for (int i = 0; i < max_s; i++) {
            Session *s = &sessions[i];
            if (s->sock < 0 || s->phase == PS_DEAD || s->phase == PS_CONNECTING) continue;
            if (now_ka - s->last_keepalive >= 90) {
                send_keepalive(s->sock);
                s->last_keepalive = now_ka;
            }
        }

        /* PEX broadcast every 60 s */
        if (time(NULL) - last_pex_bcast >= 60) {
            last_pex_bcast = time(NULL);
            for (int i = 0; i < max_s; i++) {
                Session *s = &sessions[i];
                if (s->sock < 0 || s->peer_pex_id <= 0) continue;
                if (s->phase == PS_DOWNLOADING || s->phase == PS_IDLE ||
                    s->phase == PS_SEED_READY  || s->phase == PS_SEED_UPLOADING)
                    send_pex(s, sessions, max_s, i);
            }
        }

        /*
         * Choke rotation — every 10 s (BEP 3 recommendation).
         *
         * We maintain an altruistic unchoke for seeding (all peers unchoked)
         * and an optimistic unchoke during downloading:
         *   - Rank upload sessions by blocks received from them (tit-for-tat).
         *   - Keep the top 4 unchoked.
         *   - Rotate one "optimistic" slot every 30 s to give new peers a chance.
         *
         * This prevents leechers who never upload from draining our bandwidth.
         */
        if (time(NULL) - last_choke_rotate >= 10) {
            last_choke_rotate = time(NULL);

            /* Only apply tit-for-tat during downloading; seed generously */
            if (downloading) {
                /* Score each download session by blocks received from that peer */
                int   order[256];
                int   order_count = 0;
                for (int i = 0; i < max_s && order_count < 256; i++) {
                    Session *s = &sessions[i];
                    if (s->is_incoming || s->sock < 0 || s->phase == PS_DEAD) continue;
                    order[order_count++] = i;
                }
                /* Insertion sort by blocks_recv descending (small N, fine) */
                for (int a = 1; a < order_count; a++) {
                    int key = order[a];
                    int b   = a - 1;
                    while (b >= 0 &&
                           sessions[order[b]].blocks_recv <
                           sessions[key].blocks_recv) {
                        order[b + 1] = order[b]; b--;
                    }
                    order[b + 1] = key;
                }
                /* Top 4 stay unchoked; rest get choked */
                for (int r = 0; r < order_count; r++) {
                    Session *s = &sessions[order[r]];
                    if (r < 4) {
                        /* Unchoke if currently choked */
                        if (s->peer_choked) {
                            s->peer_choked = 0;
                            send_unchoke(s->sock);
                        }
                    } else {
                        /* Choke if currently unchoked */
                        if (!s->peer_choked) {
                            s->peer_choked = 1;
                            uint8_t choke[5];
                            write_uint32_be(choke, 1);
                            choke[4] = MSG_CHOKE;
                            send(s->sock, choke, 5, MSG_NOSIGNAL);
                        }
                    }
                }
            }
            /* Seed sessions stay permanently unchoked (already set at handshake) */
        }

        tb_refill(&ul_bucket);

        int n = epoll_wait(epfd, events, 64, 200);
        if (n < 0 && errno == EINTR) continue;

        for (int e = 0; e < n; e++) {
            uint32_t eidx = events[e].data.u32;

            if ((int)eidx == max_s) {
                accept_incoming(listen_sock, epfd, sessions, max_s);
                continue;
            }

            int idx = (int)eidx;
            if (idx < 0 || idx >= max_s) continue;
            Session *s = &sessions[idx];
            if (s->phase == PS_DEAD || s->sock < 0) continue;

            if (events[e].events & (EPOLLERR | EPOLLHUP)) {
                return_piece(s, pm);
                session_close(s, epfd);
                continue;
            }

            handle_session(s, events[e].events, epfd, idx,
                           torrent, pm, torrent->info_hash, peer_id,
                           sessions, max_s, cfg, peers, &ul_bucket);

            if (s->phase == PS_DOWNLOADING && !s->am_choked
                && s->piece_idx < 0 && s->sock >= 0)
                assign_piece(s, pm, torrent, sessions, max_s, cfg);
        }

        /* Housekeeping */
        time_t now_hk = time(NULL);
        int dead = 0;
        for (int i = 0; i < max_s; i++) {
            Session *s = &sessions[i];
            if (!s->is_incoming &&
                s->phase != PS_DEAD && s->phase != PS_CONNECTING &&
                s->sock >= 0 && cfg->peer_timeout_s > 0 &&
                (now_hk - s->last_active) > (cfg->peer_timeout_s * 3)) {
                LOG_DEBUG("sched: %s:%d timed out", s->ip, s->port);
                return_piece(s, pm);
                session_close(s, epfd);
            }
            /* Stuck-piece rescue: peer has not delivered a single block in
             * 3× timeout. Return the piece so another peer can take it.
             * Keep the connection — the peer may still be useful later. */
            if (!s->is_incoming && s->phase == PS_DOWNLOADING &&
                s->piece_idx >= 0 && s->piece_started > 0 &&
                s->blocks_recv == 0 && cfg->peer_timeout_s > 0 &&
                (now_hk - s->piece_started) > (cfg->peer_timeout_s * 3)) {
                LOG_INFO("sched: %s:%d stuck on piece %d — returning to pool",
                         s->ip, s->port, s->piece_idx);
                return_piece(s, pm);
                s->piece_started = 0;
            }
            if (s->phase != PS_DEAD) continue;
            dead++;
            if (downloading && peer_cursor < peers->count) {
                const Peer *p = &peers->peers[peer_cursor++];
                time_t now = time(NULL);
                if (open_connection(sessions, max_s, &sessions[i], epfd, i, p->ip, p->port, now) == 0)
                    dead--;
            }
        }
        active = max_s - dead;

        if (downloading && time(NULL) != last_progress) {
            last_progress = time(NULL);
            piece_manager_print_progress(pm);
        }

        if (downloading && active <= 0 && peer_cursor >= peers->count) {
            time_t now       = time(NULL);
            int    wait_cap  = (peers->count < 10) ? 30 : 120;
            int    wait_secs = (int)(announce_int - (now - last_announce));
            if (wait_secs > wait_cap) wait_secs = wait_cap;
            if (wait_secs > 0) {
                LOG_INFO("sched: all peers tried — waiting %ds", wait_secs);
                struct timespec ts = { (time_t)wait_secs, 0 };
                while (ts.tv_sec > 0 && !(*interrupted)) {
                    struct timespec rem = { 0, 0 };
                    if (nanosleep(&ts, &rem) == 0) break;
                    if (errno == EINTR) ts = rem;
                    else break;
                }
            }
            last_announce = 0;
        }
    }

    for (int i = 0; i < max_s; i++) {
        if (sessions[i].sock >= 0) session_close(&sessions[i], epfd);
        else if (sessions[i].rbuf.data) rbuf_free(&sessions[i].rbuf);
    }
    if (listen_sock >= 0) close(listen_sock);
    close(epfd);
    free(sessions);
    return piece_manager_is_complete(pm) ? EXIT_SUCCESS : EXIT_FAILURE;
}
