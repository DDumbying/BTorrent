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
#include "proto/ext_handshake.h"
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
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sys/ioctl.h>
#include <linux/sockios.h>

/* ── Token-bucket rate limiter ───────────────────────────────────────────── */

#define MAX_REQUEST_LEN (32 * 1024)   /* largest block request we serve */

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
        /* The bucket must be able to hold one whole block, or low limits
         * would never accumulate enough tokens to send anything. */
        if (tb->capacity < MAX_REQUEST_LEN) tb->capacity = MAX_REQUEST_LEN;
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

/* All-or-nothing: spend `want` tokens and return 1, or return 0 and spend
 * nothing. Whole messages are sent or deferred — never cut short, which
 * would desynchronise the peer's view of the byte stream. */
static int tb_try_consume(TokenBucket *tb, long long want) {
    if (tb->rate_per_ms == 0) return 1;
    tb_refill(tb);
    if (tb->tokens < want) return 0;
    tb->tokens -= want;
    return 1;
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

#define PENDING_MAX   128   /* queued upload requests per peer (BEP-10 reqq) */
typedef struct { int pi, begin, len; } UploadReq;
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
    time_t     last_block;     /* last accepted block for piece_idx (stall detection) */
    int        is_incoming;

    /* Upload: requests from this peer, served in order as the rate limit
     * and the socket's send buffer allow. */
    UploadReq  pending[PENDING_MAX];
    int        pend_count;

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
    buf[25] = 0x10;   /* reserved[5] & 0x10: BEP-10 extension protocol.
                       * The BEP-5 DHT bit (reserved[7] & 0x01) is left clear:
                       * we only query the DHT, we don't run a node. */
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

/* ── Non-blocking send ───────────────────────────────────────────────────── */

/*
 * nb_send — send the whole buffer or fail.
 *
 * There is no per-session write queue, so a message must go out completely:
 * a partial message corrupts the stream. If the socket buffer is full we
 * wait (bounded) for it to drain. On -1 the caller must drop the session,
 * because an unknown prefix of the message may already have been sent.
 */
#define SEND_WAIT_MS 1000

static int nb_send(int sock, const uint8_t *buf, size_t len) {
    size_t sent = 0;
    long long deadline = now_ms() + SEND_WAIT_MS;
    while (sent < len) {
        ssize_t n = send(sock, buf + sent, len - sent, MSG_NOSIGNAL);
        if (n > 0) { sent += (size_t)n; continue; }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            long long left = deadline - now_ms();
            if (left <= 0) return -1;
            struct pollfd pfd = { .fd = sock, .events = POLLOUT };
            if (poll(&pfd, 1, (int)left) < 0 && errno != EINTR) return -1;
            continue;
        }
        return -1;
    }
    return 0;
}

/* Bytes that can be queued on the socket right now without blocking. */
static long send_space(int sock) {
    int sndbuf = 0, queued = 0;
    socklen_t len = sizeof(sndbuf);
    if (getsockopt(sock, SOL_SOCKET, SO_SNDBUF, &sndbuf, &len) < 0) return 0;
    if (ioctl(sock, SIOCOUTQ, &queued) < 0) return 0;
    /* Linux doubles SO_SNDBUF for bookkeeping; halve it to be conservative. */
    return (long)sndbuf / 2 - queued;
}

/* ── BEP-10 extension handshake ─────────────────────────────────────────── */

/*
 * send_ext_msg — frame and send one BEP-10 message:
 *   <len:4> <id=20:1> <sub_id:1> <payload>      where len = 2 + payload_len
 * The length covers the message id AND the sub-id; counting only one of them
 * leaves a stray byte that desynchronises the peer's parser.
 */
static int send_ext_msg(int sock, uint8_t sub_id,
                        const uint8_t *payload, size_t payload_len) {
    uint8_t hdr[6];
    write_uint32_be(hdr, (uint32_t)(2 + payload_len));
    hdr[4] = EXT_MSGID;
    hdr[5] = sub_id;
    if (nb_send(sock, hdr, sizeof(hdr)) < 0) return -1;
    return nb_send(sock, payload, payload_len);
}

static int send_ext_handshake(int sock) {
    uint8_t body[256];
    int blen = ext_build_handshake(body, sizeof(body),
                                   META_LOCAL_ID, PEX_LOCAL_ID, PENDING_MAX);
    if (blen < 0) return 0;
    return send_ext_msg(sock, 0 /* handshake */, body, (size_t)blen);
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

static int send_pex(Session *s, Session *all, int max_s, int self_idx) {
    if (s->peer_pex_id <= 0 || s->peer_pex_id > 255 || s->sock < 0) return 0;
    uint8_t body[400];
    int blen = build_pex_body(body, sizeof(body), all, max_s, self_idx);
    if (blen <= 0) return 0;
    return send_ext_msg(s->sock, (uint8_t)s->peer_pex_id, body, (size_t)blen);
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

/* All return 0 on success, -1 if the session must be dropped. */
static int send_have_msg(int sock, int pi) {
    uint8_t buf[9];
    write_uint32_be(buf, 5); buf[4] = MSG_HAVE;
    write_uint32_be(buf + 5, (uint32_t)pi);
    return nb_send(sock, buf, 9);
}
static int send_keepalive(int sock) {
    uint8_t buf[4] = {0,0,0,0};
    return nb_send(sock, buf, 4);
}
static int send_simple(int sock, uint8_t id) {
    uint8_t buf[5]; write_uint32_be(buf, 1); buf[4] = id;
    return nb_send(sock, buf, 5);
}
static int send_unchoke(int sock) { return send_simple(sock, MSG_UNCHOKE); }
static int send_choke(int sock)   { return send_simple(sock, MSG_CHOKE); }

/* Cancel every block request we have sent for s->piece_idx. A CANCEL must
 * match the original REQUEST exactly (index, begin, length). */
static int send_cancels(Session *s) {
    for (int b = 0; b < s->blocks_sent; b++) {
        int beg  = b * BLOCK_SIZE;
        int blen = (beg + BLOCK_SIZE > s->piece_len) ? s->piece_len - beg : BLOCK_SIZE;
        uint8_t m[17];
        write_uint32_be(m, 13); m[4] = MSG_CANCEL;
        write_uint32_be(m + 5,  (uint32_t)s->piece_idx);
        write_uint32_be(m + 9,  (uint32_t)beg);
        write_uint32_be(m + 13, (uint32_t)blen);
        if (nb_send(s->sock, m, 17) < 0) return -1;
    }
    return 0;
}

/* Send one MSG_PIECE block.
 * Returns 0 on success, -1 for an unservable request (ignore it),
 * -2 if the connection broke (drop the session). */
static int serve_block(Session *s, PieceManager *pm,
                        const TorrentInfo *torrent,
                        int pi, int begin, int length) {
    if (pi < 0 || pi >= torrent->num_pieces) return -1;
    int plen = torrent_get_piece_length(torrent, pi);
    if (begin < 0 || length <= 0 || (long long)begin + length > plen) return -1;
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
    int rc = (nb_send(s->sock, hdr, 13) == 0 &&
              nb_send(s->sock, piece_data + begin, (size_t)length) == 0) ? 0 : -2;

    free(piece_data);
    if (rc < 0) return rc;
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
    s->last_block    = 0;
    s->pend_count    = 0;
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
    s->pend_count = 0;
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
            s->last_block  = time(NULL);
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
    s->last_block        = time(NULL);
    LOG_DEBUG("sched: %s:%d → piece %d/%d", s->ip, s->port, pi, pm->num_pieces-1);
    if (send_requests(s, cfg) < 0) s->phase = PS_DEAD;
}

/*
 * return_piece — release s's claim on its piece.
 *
 * In endgame several sessions fetch the same piece. If another live session
 * still holds it, leave the piece and its received blocks alone: wiping it
 * would discard blocks that session will never re-request (it has already
 * sent all its requests), and the download would hang at 99%.
 */
static void return_piece(Session *s, PieceManager *pm, Session *all, int max_s) {
    if (s->piece_idx < 0) return;
    int pi = s->piece_idx;
    s->piece_idx = -1;
    s->blocks_sent = s->blocks_recv = 0;
    for (int i = 0; i < max_s; i++) {
        if (&all[i] != s && all[i].sock >= 0 && all[i].phase != PS_DEAD &&
            all[i].piece_idx == pi)
            return;
    }
    if (pm->pieces[pi].state == PIECE_ACTIVE) {
        free(pm->pieces[pi].data); pm->pieces[pi].data = NULL;
        pm->pieces[pi].state = PIECE_EMPTY;
        memset(pm->pieces[pi].block_received, 0,
               (size_t)pm->pieces[pi].num_blocks);
        pm->pieces[pi].blocks_done = 0;
    } else if (pm->pieces[pi].state == PIECE_ASSIGNED) {
        pm->pieces[pi].state = PIECE_EMPTY;
    }
}

/* Release the session's piece, then close it. Use this — not a bare
 * session_close() — whenever a session that may hold a piece goes away. */
static void session_drop(Session *s, PieceManager *pm,
                         Session *all, int max_s, int epfd) {
    return_piece(s, pm, all, max_s);
    session_close(s, epfd);
}

/* ── dispatch_msg ────────────────────────────────────────────────────────── */

static void dispatch_msg(Session *s, int sidx,
                          uint8_t id, uint8_t *payload, uint32_t plen,
                          int epfd,
                          const TorrentInfo *torrent, PieceManager *pm,
                          Session *all, int max_s,
                          const Config *cfg, PeerList *peers) {
    int bf_bytes = (torrent->num_pieces + 7) / 8;
    (void)epfd;

    switch (id) {

    /* ── Standard download messages ── */

    case MSG_CHOKE:
        /* A choking peer discards our outstanding requests. */
        s->am_choked = 1;
        return_piece(s, pm, all, max_s);
        s->phase = PS_IDLE;
        if (send_simple(s->sock, MSG_INTERESTED) < 0) s->phase = PS_DEAD;
        break;

    case MSG_UNCHOKE:
        s->am_choked = 0;
        if (s->phase == PS_INTERESTED || s->phase == PS_IDLE) s->phase = PS_DOWNLOADING;
        break;

    case MSG_HAVE: {
        if (plen != 4) { s->phase = PS_DEAD; break; }
        uint32_t pi = read_uint32_be(payload);
        if (pi < (uint32_t)torrent->num_pieces && s->peer_bitfield)
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
        uint32_t wire_pi    = read_uint32_be(payload);
        uint32_t wire_begin = read_uint32_be(payload + 4);
        if (wire_pi >= (uint32_t)torrent->num_pieces ||
            wire_begin > (uint32_t)INT_MAX || plen - 8 > BLOCK_SIZE) {
            LOG_DEBUG("peer %s:%d: invalid PIECE (index=%u begin=%u len=%u)",
                      s->ip, s->port, wire_pi, wire_begin, plen - 8);
            break;
        }
        int pi    = (int)wire_pi;
        int begin = (int)wire_begin;
        int dlen  = (int)(plen - 8);
        /* Ignore unsolicited blocks: only pieces some session has been
         * assigned may allocate a buffer in the piece manager. */
        PieceState st = pm->pieces[pi].state;
        if (st != PIECE_ASSIGNED && st != PIECE_ACTIVE) break;
        int result = piece_manager_on_block(pm, pi, begin, payload + 8, dlen);
        if (pi == s->piece_idx) {
            s->blocks_recv++;
            s->last_block = time(NULL);
        }
        if (result == 1 || result == -1) {
            /* Piece finished: verified (broadcast HAVE) or failed its hash
             * (piece reset to EMPTY). Either way, every other session
             * fetching it in endgame must stop — cancel its requests. */
            for (int i = 0; i < max_s; i++) {
                Session *o = &all[i];
                if (o->sock < 0 || o->phase == PS_DEAD) continue;
                if (result == 1 && send_have_msg(o->sock, pi) < 0) {
                    o->phase = PS_DEAD;
                    continue;
                }
                if (i != sidx && o->piece_idx == pi) {
                    if (send_cancels(o) < 0) o->phase = PS_DEAD;
                    o->piece_idx   = -1;
                    o->blocks_sent = o->blocks_recv = 0;
                }
            }
            /* The block may be a late one for a piece s has already been
             * moved off; only clear s's claim if this is its current piece. */
            if (s->piece_idx == pi) {
                s->piece_idx  = -1;
                s->blocks_sent = s->blocks_recv = 0;
            }
            if (result == 1 && s->phase != PS_DEAD && !s->am_choked)
                s->phase = PS_DOWNLOADING;
        } else if (pi == s->piece_idx) {
            if (send_requests(s, cfg) < 0) s->phase = PS_DEAD;
        }
        break;
    }

    /* ── Seed: serve uploaded blocks ── */

    case MSG_REQUEST: {
        if (plen < 12) { s->phase = PS_DEAD; break; }
        uint32_t wire_pi    = read_uint32_be(payload);
        uint32_t wire_begin = read_uint32_be(payload + 4);
        uint32_t wire_len   = read_uint32_be(payload + 8);
        if (s->peer_choked) break;
        if (wire_pi >= (uint32_t)torrent->num_pieces ||
            wire_begin > (uint32_t)INT_MAX ||
            wire_len == 0 || wire_len > 32768) break;
        if (pm->pieces[wire_pi].state != PIECE_COMPLETE) break;
        if ((long long)wire_begin + wire_len >
            torrent_get_piece_length(torrent, (int)wire_pi)) break;
        /* Queue it; serve_pending() sends it when the upload rate limit and
         * the socket's send buffer allow. Beyond our advertised reqq the
         * peer is misbehaving, so the request is dropped. */
        if (s->pend_count >= PENDING_MAX) {
            LOG_DEBUG("seed: %s:%d request queue full", s->ip, s->port);
            break;
        }
        s->pending[s->pend_count++] = (UploadReq){
            .pi = (int)wire_pi, .begin = (int)wire_begin, .len = (int)wire_len };
        break;
    }

    case MSG_CANCEL: {
        if (plen < 12) break;
        int pi    = (int)read_uint32_be(payload);
        int begin = (int)read_uint32_be(payload + 4);
        int len   = (int)read_uint32_be(payload + 8);
        for (int i = 0; i < s->pend_count; i++) {
            UploadReq *r = &s->pending[i];
            if (r->pi == pi && r->begin == begin && r->len == len) {
                memmove(r, r + 1, (size_t)(s->pend_count - i - 1) * sizeof(*r));
                s->pend_count--;
                break;
            }
        }
        break;
    }

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
            Peer new_peers[50] = {0};
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
                            const Config *cfg, PeerList *peers) {
    s->last_active = time(NULL);
    int bf_bytes   = (torrent->num_pieces + 7) / 8;
#define DROP() do { \
        LOG_DEBUG("peer %s:%d: dropped (phase=%d, scheduler.c:%d)", \
                  s->ip, s->port, s->phase, __LINE__); \
        session_drop(s, pm, all, max_s, epfd); return; } while (0)

    /* ── Outgoing: finish TCP connect ── */
    if (s->phase == PS_CONNECTING) {
        if (!(ev_flags & EPOLLOUT)) DROP();
        if (tcp_finish_connect(s->sock) < 0) DROP();
        uint8_t hs[HANDSHAKE_LEN];
        build_handshake(hs, info_hash, our_peer_id);
        if (nb_send(s->sock, hs, HANDSHAKE_LEN) < 0) DROP();
        LOG_INFO("peer %s:%d: connected", s->ip, s->port);
        s->phase         = PS_HANDSHAKE;
        s->last_active   = time(NULL);   /* handshake timer starts now */
        s->peer_bitfield = xcalloc((size_t)bf_bytes, 1);
        s->bf_len        = bf_bytes;
        epoll_watch(epfd, s->sock, EPOLLIN | EPOLLET, idx);
        return;
    }

    /* With edge-triggered epoll we must consume everything that has arrived:
     * a peer often sends its handshake and first messages (bitfield, unchoke,
     * requests) in one packet, and no new event fires for bytes already read.
     * So after a handshake completes we fall through to message processing
     * instead of returning. */
    int filled = 0;

    /* ── Outgoing: receive peer's handshake ── */
    if (s->phase == PS_HANDSHAKE) {
        if (rbuf_fill(&s->rbuf, s->sock) < 0) DROP();
        filled = 1;
        uint8_t their_hs[HANDSHAKE_LEN];
        if (!rbuf_consume(&s->rbuf, HANDSHAKE_LEN, their_hs)) return;
        int supports_ext = 0;
        if (verify_handshake(their_hs, info_hash, &supports_ext) < 0) {
            record_failure(s, time(NULL));
            DROP();
        }
        record_success(s);
        memcpy(s->peer_id, their_hs + 48, 20);
        LOG_INFO("peer %s:%d: HS OK (ext=%d)", s->ip, s->port, supports_ext);

        if (pm->completed > 0) {
            uint8_t *bfmsg = xmalloc((size_t)(5 + pm->bf_len));
            write_uint32_be(bfmsg, (uint32_t)(1 + pm->bf_len));
            bfmsg[4] = MSG_BITFIELD;
            memcpy(bfmsg + 5, pm->our_bitfield, (size_t)pm->bf_len);
            int rc = nb_send(s->sock, bfmsg, (size_t)(5 + pm->bf_len));
            free(bfmsg);
            if (rc < 0) DROP();
        }
        if (supports_ext && send_ext_handshake(s->sock) < 0) DROP();
        if (send_simple(s->sock, MSG_INTERESTED) < 0) DROP();
        s->am_choked = 1;
        s->phase     = PS_INTERESTED;
    }

    /* ── Incoming seed: receive peer's handshake ── */
    else if (s->phase == PS_SEED_HANDSHAKE) {
        if (rbuf_fill(&s->rbuf, s->sock) < 0) DROP();
        filled = 1;
        uint8_t their_hs[HANDSHAKE_LEN];
        if (!rbuf_consume(&s->rbuf, HANDSHAKE_LEN, their_hs)) return;
        int supports_ext = 0;
        if (verify_handshake(their_hs, info_hash, &supports_ext) < 0) {
            LOG_DEBUG("seed: bad HS from %s:%d", s->ip, s->port);
            DROP();
        }
        memcpy(s->peer_id, their_hs + 48, 20);

        /* Reply with our handshake and BITFIELD */
        uint8_t hs[HANDSHAKE_LEN];
        build_handshake(hs, info_hash, our_peer_id);
        if (nb_send(s->sock, hs, HANDSHAKE_LEN) < 0) DROP();

        uint8_t *bfmsg = xmalloc((size_t)(5 + pm->bf_len));
        write_uint32_be(bfmsg, (uint32_t)(1 + pm->bf_len));
        bfmsg[4] = MSG_BITFIELD;
        memcpy(bfmsg + 5, pm->our_bitfield, (size_t)pm->bf_len);
        int rc = nb_send(s->sock, bfmsg, (size_t)(5 + pm->bf_len));
        free(bfmsg);
        if (rc < 0) DROP();

        if (supports_ext && send_ext_handshake(s->sock) < 0) DROP();

        /* Unchoke immediately — simple altruistic seeding policy */
        s->peer_choked = 0;
        if (send_unchoke(s->sock) < 0) DROP();

        s->peer_bitfield = xcalloc((size_t)bf_bytes, 1);
        s->bf_len        = bf_bytes;
        s->phase         = PS_SEED_READY;
        LOG_INFO("seed: %s:%d connected", s->ip, s->port);
    }

    /* ── All data-bearing states: read and dispatch messages ── */
    if (!filled) {
        if (!(ev_flags & EPOLLIN)) return;
        if (rbuf_fill(&s->rbuf, s->sock) < 0) {
            LOG_INFO("peer %s:%d: disconnected (phase=%d)", s->ip, s->port, s->phase);
            DROP();
        }
    }

    while (s->phase != PS_DEAD) {
        if (s->rbuf.len < 4) break;
        uint32_t msg_len = read_uint32_be(s->rbuf.data);
        if (msg_len == 0) { rbuf_consume(&s->rbuf, 4, NULL); continue; }
        if (msg_len > 16 * 1024 * 1024) DROP();
        if (s->rbuf.len < 4 + msg_len) break;

        rbuf_consume(&s->rbuf, 4, NULL);
        uint8_t wire_id = 0;
        rbuf_consume(&s->rbuf, 1, &wire_id);
        uint32_t plen   = msg_len - 1;
        uint8_t *payload = plen ? xmalloc(plen) : NULL;
        if (plen) rbuf_consume(&s->rbuf, plen, payload);

        dispatch_msg(s, idx, wire_id, payload, plen, epfd,
                     torrent, pm, all, max_s, cfg, peers);
        free(payload);

        if (s->phase == PS_DOWNLOADING && !s->am_choked && s->piece_idx < 0)
            assign_piece(s, pm, torrent, all, max_s, cfg);
    }
    /* A handler may have marked the session dead (protocol error or a
     * failed send); release its piece and socket now. */
    if (s->phase == PS_DEAD && s->sock >= 0) DROP();
#undef DROP
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
                            const char *ip, uint16_t port, int is_ipv6, time_t now) {
    if (is_circuit_open(sessions, max_s, ip, now)) {
        LOG_DEBUG("sched: circuit breaker open for %s:%d — skipping", ip, port);
        return -1;
    }
    int sock;
    if (is_ipv6) {
        sock = tcp_connect_nb_ipv6(ip, port);
    } else {
        sock = tcp_connect_nb(ip, port);
    }
    if (sock < 0) return -1;
    session_init(s, sock, ip, port, /*incoming=*/0);
    struct epoll_event ev = { .events = EPOLLOUT | EPOLLET, .data.u32 = (uint32_t)idx };
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, sock, &ev) < 0) {
        close(sock); s->sock = -1; rbuf_free(&s->rbuf);
        s->phase = PS_DEAD;   /* otherwise the slot is lost for good */
        return -1;
    }
    return 0;
}

/* ── Background tracker announce ─────────────────────────────────────────── */
/*
 * An announce can take a long time (HTTP timeouts, dead UDP trackers,
 * retry backoff), so it runs on a detached thread while the event loop keeps
 * serving peers. The job holds a private copy of the torrent metadata and is
 * reference-counted, so the worker may safely outlive scheduler_run() when
 * the user interrupts mid-announce.
 */
typedef struct {
    atomic_int   refs;          /* main loop + worker */
    atomic_int   done;          /* set by the worker after `result` is written */
    TorrentInfo  torrent;       /* copy; pieces_hash cleared (not needed) */
    uint8_t      peer_id[20];
    uint16_t     port;
    long         dl, ul, left;
    char         event[16];
    PeerList     result;
} AnnounceJob;

static void announce_release(AnnounceJob *job) {
    if (atomic_fetch_sub(&job->refs, 1) == 1) {
        peer_list_free(&job->result);
        free(job);
    }
}

static void *announce_worker(void *arg) {
    AnnounceJob *job = arg;
    job->result = tracker_announce_with_retry(
        &job->torrent, job->peer_id, job->port, job->dl, job->ul, job->left,
        job->event[0] ? job->event : NULL);
    atomic_store(&job->done, 1);
    announce_release(job);
    return NULL;
}

static AnnounceJob *announce_start(const TorrentInfo *torrent,
                                   const uint8_t *peer_id, uint16_t port,
                                   long dl, long ul, long left,
                                   const char *event) {
    AnnounceJob *job = xcalloc(1, sizeof(*job));
    job->torrent = *torrent;
    job->torrent.pieces_hash = NULL;
    memcpy(job->peer_id, peer_id, 20);
    job->port = port;
    job->dl = dl; job->ul = ul; job->left = left;
    snprintf(job->event, sizeof(job->event), "%s", event ? event : "");
    atomic_init(&job->refs, 2);
    atomic_init(&job->done, 0);

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_t tid;
    int rc = pthread_create(&tid, &attr, announce_worker, job);
    pthread_attr_destroy(&attr);
    if (rc != 0) {
        LOG_WARN("sched: cannot start announce thread: %s", strerror(rc));
        free(job);
        return NULL;
    }
    return job;
}

/* ── Upload queue ────────────────────────────────────────────────────────── */

#define MAX_SERVE_PER_TICK 256   /* bound the time spent serving per loop */
#define INCOMING_IDLE_S    180   /* peers keepalive every 120 s at most */

/*
 * serve_pending — send queued blocks round-robin across peers.
 *
 * A block is only sent when (a) the socket's send buffer can take all of it,
 * so the send never stalls, and (b) the upload token bucket covers it, so
 * the rate limit is honoured by deferring whole messages, never truncating.
 */
static void serve_pending(Session *all, int max_s, PieceManager *pm,
                          const TorrentInfo *torrent, TokenBucket *tb,
                          long long *uploaded) {
    int served = 0, progress = 1;
    while (progress && served < MAX_SERVE_PER_TICK) {
        progress = 0;
        for (int i = 0; i < max_s && served < MAX_SERVE_PER_TICK; i++) {
            Session *s = &all[i];
            if (s->pend_count == 0 || s->sock < 0 || s->phase == PS_DEAD) continue;
            if (s->peer_choked) { s->pend_count = 0; continue; }
            UploadReq r = s->pending[0];
            if (send_space(s->sock) < r.len + 13) continue;
            if (!tb_try_consume(tb, r.len)) return;
            s->pend_count--;
            memmove(s->pending, s->pending + 1,
                    (size_t)s->pend_count * sizeof(UploadReq));
            int rc = serve_block(s, pm, torrent, r.pi, r.begin, r.len);
            if (rc == -2) { s->phase = PS_DEAD; s->pend_count = 0; continue; }
            if (rc == 0) {
                *uploaded += r.len;
                if (s->phase == PS_SEED_READY) s->phase = PS_SEED_UPLOADING;
            }
            served++;
            progress = 1;
        }
    }
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
    AnnounceJob *announce_job     = NULL;
    int          completed_pending = 0;   /* "completed" event still owed */
    int          starved_logged    = 0;
    long long    uploaded          = 0;
    const int    timeout_s = cfg->peer_timeout_s > 0 ? cfg->peer_timeout_s : 5;

    for (int i = 0; i < max_s && peer_cursor < peers->count; i++) {
        const Peer *p = &peers->peers[peer_cursor++];
        time_t now = time(NULL);
        if (open_connection(sessions, max_s, &sessions[i], epfd, i, p->ip, p->port, p->is_ipv6, now) == 0) active++;
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
            completed_pending = 1;   /* tell the tracker once, right away */
        }

        /* Collect a finished background announce. */
        if (announce_job && atomic_load(&announce_job->done)) {
            PeerList *np = &announce_job->result;
            if (np->count > 0) {
                int added = inject_peers(peers, np->peers, np->count);
                if (added > 0) LOG_INFO("sched: +%d tracker peers", added);
                if (np->interval > 0) announce_int = np->interval;
                starved_logged = 0;
            }
            announce_release(announce_job);
            announce_job = NULL;
        }

        /* Re-announce — at the tracker interval; after 60 s if we are
         * critically short on peers (< 3 connected); after 30 s if every
         * known peer has been tried; and at once to report completion.
         * The announce runs on a background thread (see announce_start)
         * so slow or dead trackers never stall peer traffic. */
        int live_peers = 0, active_sessions = 0;
        for (int i = 0; i < max_s; i++) {
            if (sessions[i].sock < 0 || sessions[i].phase == PS_DEAD) continue;
            active_sessions++;
            if (sessions[i].phase != PS_CONNECTING && !sessions[i].is_incoming)
                live_peers++;
        }
        time_t since_announce = time(NULL) - last_announce;
        int starved = downloading && active_sessions == 0 &&
                      peer_cursor >= peers->count;
        if (starved && !starved_logged) {
            LOG_INFO("%s", "sched: all known peers tried — waiting for more "
                           "from the tracker");
            starved_logged = 1;
        }
        int announce_due = !announce_job &&
            (since_announce >= announce_int ||
             completed_pending ||
             (downloading && live_peers < 3 && since_announce >= 60) ||
             (starved && since_announce >= 30));
        if (announce_due) {
            long long have = pm->bytes_at_start + pm->bytes_downloaded;
            long long left = torrent->total_length - have;
            if (left < 0) left = 0;
            if (downloading && live_peers < 3)
                LOG_INFO("sched: only %d live peers — re-announcing now", live_peers);
            announce_job = announce_start(torrent, peer_id, cfg->port,
                                          (long)pm->bytes_downloaded,
                                          (long)uploaded, (long)left,
                                          completed_pending ? "completed" : NULL);
            completed_pending = 0;
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
                if (send_keepalive(s->sock) < 0) s->phase = PS_DEAD;
                s->last_keepalive = now_ka;
            }
        }

        /* PEX broadcast every 60 s */
        if (time(NULL) - last_pex_bcast >= 60) {
            last_pex_bcast = time(NULL);
            for (int i = 0; i < max_s; i++) {
                Session *s = &sessions[i];
                if (s->sock < 0 || s->peer_pex_id <= 0) continue;
                if ((s->phase == PS_DOWNLOADING || s->phase == PS_IDLE ||
                     s->phase == PS_SEED_READY  || s->phase == PS_SEED_UPLOADING) &&
                    send_pex(s, sessions, max_s, i) < 0)
                    s->phase = PS_DEAD;
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
                            if (send_unchoke(s->sock) < 0) s->phase = PS_DEAD;
                        }
                    } else {
                        /* Choke if currently unchoked */
                        if (!s->peer_choked) {
                            /* Choking discards the peer's queued requests. */
                            s->peer_choked = 1;
                            s->pend_count  = 0;
                            if (send_choke(s->sock) < 0) s->phase = PS_DEAD;
                        }
                    }
                }
            }
            /* Seed sessions stay permanently unchoked (already set at handshake) */
        }

        tb_refill(&ul_bucket);

        /* Poll faster while uploads are queued so they drain promptly. */
        int any_pending = 0;
        for (int i = 0; i < max_s && !any_pending; i++)
            any_pending = sessions[i].pend_count > 0;
        int n = epoll_wait(epfd, events, 64, any_pending ? 20 : 200);
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
                session_drop(s, pm, sessions, max_s, epfd);
                continue;
            }

            handle_session(s, events[e].events, epfd, idx,
                           torrent, pm, torrent->info_hash, peer_id,
                           sessions, max_s, cfg, peers);

            if (s->phase == PS_DOWNLOADING && !s->am_choked
                && s->piece_idx < 0 && s->sock >= 0)
                assign_piece(s, pm, torrent, sessions, max_s, cfg);
        }

        serve_pending(sessions, max_s, pm, torrent, &ul_bucket, &uploaded);

        /* Housekeeping: drop dead and idle sessions, rescue stalled pieces,
         * refill free slots from the peer pool. */
        time_t now_hk = time(NULL);
        for (int i = 0; i < max_s; i++) {
            Session *s = &sessions[i];
            time_t idle = now_hk - s->last_active;

            if (s->sock >= 0 && s->phase == PS_DEAD) {
                /* Marked dead by a handler (failed send, protocol error). */
                session_drop(s, pm, sessions, max_s, epfd);
            } else if (s->sock >= 0 && s->phase == PS_CONNECTING) {
                /* Non-blocking connect: enforce -t ourselves, otherwise a
                 * dead peer holds the slot for the kernel's ~2 min SYN timeout. */
                if (idle > timeout_s) {
                    LOG_DEBUG("sched: %s:%d connect timed out", s->ip, s->port);
                    session_drop(s, pm, sessions, max_s, epfd);
                }
            } else if (s->sock >= 0 && !s->is_incoming) {
                if (idle > timeout_s * 3) {
                    LOG_DEBUG("sched: %s:%d timed out", s->ip, s->port);
                    session_drop(s, pm, sessions, max_s, epfd);
                } else if (s->phase == PS_DOWNLOADING && s->piece_idx >= 0 &&
                           now_hk - s->last_block > timeout_s * 3) {
                    /* No block for this piece in 3× timeout: hand it back so
                     * another peer can take it; keep the connection. */
                    LOG_INFO("sched: %s:%d stalled on piece %d — returning to pool",
                             s->ip, s->port, s->piece_idx);
                    if (send_cancels(s) < 0) s->phase = PS_DEAD;
                    return_piece(s, pm, sessions, max_s);
                }
            } else if (s->sock >= 0 && s->is_incoming) {
                /* Incoming peers must finish the handshake promptly and
                 * then send something (at least keepalives, every 2 min);
                 * otherwise idle connections could hold every slot. */
                int limit = (s->phase == PS_SEED_HANDSHAKE) ? timeout_s * 3
                                                             : INCOMING_IDLE_S;
                if (idle > limit) {
                    LOG_DEBUG("seed: %s:%d idle — closing", s->ip, s->port);
                    session_drop(s, pm, sessions, max_s, epfd);
                }
            }

            if (s->phase != PS_DEAD || s->sock >= 0) continue;
            if (downloading && peer_cursor < peers->count) {
                const Peer *p = &peers->peers[peer_cursor++];
                open_connection(sessions, max_s, &sessions[i], epfd, i,
                                p->ip, p->port, p->is_ipv6, now_hk);
            }
        }

        if (downloading && time(NULL) != last_progress) {
            last_progress = time(NULL);
            piece_manager_print_progress(pm);
        }

    }

    /* A worker still waiting on a tracker keeps its own reference and
     * frees the job when it finishes. */
    if (announce_job) announce_release(announce_job);

    for (int i = 0; i < max_s; i++) {
        if (sessions[i].sock >= 0) session_close(&sessions[i], epfd);
        else if (sessions[i].rbuf.data) rbuf_free(&sessions[i].rbuf);
    }
    if (listen_sock >= 0) close(listen_sock);
    close(epfd);
    free(sessions);
    return piece_manager_is_complete(pm) ? EXIT_SUCCESS : EXIT_FAILURE;
}
