/**
 * test_metadata_serve.c — BEP 9 metadata serving and BEP 27 private torrents,
 * end to end through the scheduler.
 *
 * Includes scheduler.c (as tests/fuzz/fuzz_wire.c does) so a real session
 * handles real wire bytes: the "peer" writes to one end of a socketpair,
 * handle_session() reads the other, and the test reads back what we sent.
 *
 * Covers:
 *   1. torrent parsing: info_raw hashes to info_hash; 'private' flag
 *   2. our ext handshake: ut_metadata/ut_pex/metadata_size for public
 *      torrents, none of them for private ones
 *   3. ut_metadata requests: data for public torrents, reject for private
 *   4. inbound PEX is ignored for private torrents
 */
#include "../../src/scheduler.c"   /* first: it sets feature macros */
#include "core/bencode.h"
#include "core/sha1.h"
#include <sys/socket.h>

static int g_pass = 0, g_fail = 0;
#define EXPECT(cond, name) \
    do { if (cond) { printf("  PASS  %s\n", name); g_pass++; } \
         else      { printf("  FAIL  %s  (line %d)\n", name, __LINE__); g_fail++; } \
    } while (0)

#define PLEN 32768

static const uint8_t g_peer_id[21] = "-TS0001-000000000000";

/* A single-file, single-piece torrent of zeros, optionally private. The
 * name is padded so the info dict spans two 16 KiB metadata pieces. */
static TorrentInfo *make_torrent(int priv) {
    uint8_t *zeros = calloc(PLEN, 1);
    uint8_t hash[20];
    sha1(zeros, PLEN, hash);
    free(zeros);

    char name[200];
    memset(name, 'n', sizeof(name) - 1);
    name[sizeof(name) - 1] = '\0';

    /* Pad with an unknown key: parsers ignore it, but it is hashed. */
    size_t pad_len = META_BLOCK_SIZE + 1000;
    size_t cap = pad_len + 1024;
    char *buf = malloc(cap);
    int n = snprintf(buf, cap,
        "d8:announce22:http://127.0.0.1:1/ann4:info"
        "d6:lengthi%de4:name%zu:%s12:piece lengthi%de6:pieces20:",
        PLEN, strlen(name), name, PLEN);
    memcpy(buf + n, hash, 20);
    n += 20;
    n += snprintf(buf + n, cap - (size_t)n, "%s9:x-padding%zu:",
                  priv ? "7:privatei1e" : "", pad_len);
    memset(buf + n, 'p', pad_len);
    n += (int)pad_len;
    n += snprintf(buf + n, cap - (size_t)n, "ee");

    TorrentInfo *t = torrent_parse_buffer((uint8_t *)buf, (size_t)n);
    free(buf);
    return t;
}

/* Frame one BEP 10 message: <len> <20> <sub> <body>. */
static size_t ext_frame(uint8_t *out, uint8_t sub, const char *body, size_t blen) {
    write_uint32_be(out, (uint32_t)(2 + blen));
    out[4] = EXT_MSGID;
    out[5] = sub;
    memcpy(out + 6, body, blen);
    return 6 + blen;
}

/* Read everything we sent to the peer. */
static size_t drain(int fd, uint8_t *buf, size_t cap) {
    size_t got = 0;
    ssize_t n;
    while (got < cap && (n = recv(fd, buf + got, cap - got, MSG_DONTWAIT)) > 0)
        got += (size_t)n;
    return got;
}

/* Find the first ext message with sub-id `sub` in a stream of wire messages
 * (after `skip` bytes of handshake). Returns its body, or NULL. */
static const uint8_t *find_ext(const uint8_t *buf, size_t len, size_t skip,
                               uint8_t sub, size_t *body_len) {
    size_t pos = skip;
    while (pos + 4 <= len) {
        uint32_t mlen = read_uint32_be(buf + pos);
        if (pos + 4 + mlen > len) break;
        if (mlen >= 2 && buf[pos + 4] == EXT_MSGID && buf[pos + 5] == sub) {
            *body_len = mlen - 2;
            return buf + pos + 6;
        }
        pos += 4 + mlen;
    }
    return NULL;
}

typedef struct {
    int           sv[2];
    Session       sessions[2];
    PieceManager *pm;
    PeerList      peers;
    Config        cfg;
    int           epfd;
    char          path[64];
} Rig;

static void rig_open(Rig *r, TorrentInfo *t, PeerPhase phase) {
    memset(r, 0, sizeof(*r));
    socketpair(AF_UNIX, SOCK_STREAM, 0, r->sv);
    for (int i = 0; i < 2; i++)
        fcntl(r->sv[i], F_SETFL, fcntl(r->sv[i], F_GETFL) | O_NONBLOCK);
    snprintf(r->path, sizeof(r->path), "/tmp/btorrent_test_meta_%d.bin", (int)getpid());
    r->pm   = piece_manager_new(t, r->path, 0);
    r->cfg  = (Config){ .max_peers = 2, .pipeline_depth = 4, .peer_timeout_s = 5 };
    r->epfd = epoll_create1(EPOLL_CLOEXEC);
    for (int i = 0; i < 2; i++) { r->sessions[i].sock = -1; r->sessions[i].phase = PS_DEAD; }
    Session *s = &r->sessions[0];
    session_init(s, r->sv[0], "10.0.0.1", 6881, 1);
    if (phase == PS_SEED_READY) {
        int bf = (t->num_pieces + 7) / 8;
        s->peer_bitfield = xcalloc((size_t)bf, 1);
        s->bf_len        = bf;
        s->peer_choked   = 0;
        s->phase         = PS_SEED_READY;
    }
}

static void rig_run(Rig *r, TorrentInfo *t, const uint8_t *in, size_t len) {
    if (write(r->sv[1], in, len) != (ssize_t)len) { /* socketpair: fits */ }
    handle_session(&r->sessions[0], EPOLLIN, r->epfd, 0, t, r->pm,
                   t->info_hash, g_peer_id, r->sessions, 2, &r->cfg, &r->peers);
}

static void rig_close(Rig *r) {
    Session *s = &r->sessions[0];
    if (s->sock >= 0) session_drop(s, r->pm, r->sessions, 2, r->epfd);
    else if (s->rbuf.data) rbuf_free(&s->rbuf);
    free(s->peer_bitfield);
    close(r->sv[1]);
    close(r->epfd);
    piece_manager_free(r->pm);
    peer_list_free(&r->peers);
    char lock[80];
    snprintf(lock, sizeof(lock), "%s.btlock", r->path);
    unlink(r->path);
    unlink(lock);
}

/* ── Tests ────────────────────────────────────────────────────────────────── */

static void test_parse(void) {
    printf("\n--- torrent parsing: info_raw + private ---\n");

    TorrentInfo *pub = make_torrent(0), *priv = make_torrent(1);
    EXPECT(pub && priv, "both torrents parse");
    if (!pub || !priv) { torrent_free(pub); torrent_free(priv); return; }

    uint8_t h[20];
    sha1(pub->info_raw, pub->info_raw_len, h);
    EXPECT(memcmp(h, pub->info_hash, 20) == 0, "SHA-1(info_raw) == info_hash");
    EXPECT(pub->info_raw[0] == 'd' && pub->info_raw[pub->info_raw_len - 1] == 'e',
           "info_raw is the bencoded dict");
    EXPECT(pub->info_raw_len > META_BLOCK_SIZE, "info dict spans two metadata pieces");
    EXPECT(!pub->is_private, "public torrent: is_private == 0");
    EXPECT(priv->is_private, "private torrent: is_private == 1");
    EXPECT(memcmp(pub->info_hash, priv->info_hash, 20) != 0,
           "'private' key changes the info hash");

    torrent_free(pub);
    torrent_free(priv);
}

/* Incoming peer handshakes with the extension bit; check our ext handshake. */
static void check_our_handshake(int priv) {
    TorrentInfo *t = make_torrent(priv);
    Rig r;
    rig_open(&r, t, PS_SEED_HANDSHAKE);

    uint8_t hs[HANDSHAKE_LEN];
    build_handshake(hs, t->info_hash, (const uint8_t *)"-PE0001-000000000000");
    rig_run(&r, t, hs, sizeof(hs));

    static uint8_t out[65536];
    size_t n = drain(r.sv[1], out, sizeof(out));
    size_t blen = 0;
    const uint8_t *body = find_ext(out, n, HANDSHAKE_LEN, 0, &blen);
    EXPECT(body != NULL, priv ? "private: ext handshake sent"
                              : "public: ext handshake sent");
    BencodeNode *root = body ? bencode_parse(body, blen) : NULL;
    BencodeNode *m    = bencode_dict_get(root, "m");
    BencodeNode *meta = bencode_dict_get(m, "ut_metadata");
    BencodeNode *pex  = bencode_dict_get(m, "ut_pex");
    BencodeNode *size = bencode_dict_get(root, "metadata_size");
    if (priv) {
        EXPECT(!meta, "private: no ut_metadata");
        EXPECT(!pex,  "private: no ut_pex");
        EXPECT(!size, "private: no metadata_size");
    } else {
        EXPECT(meta && meta->integer == META_LOCAL_ID, "public: ut_metadata advertised");
        EXPECT(pex  && pex->integer  == PEX_LOCAL_ID,  "public: ut_pex advertised");
        EXPECT(size && size->integer == (long long)t->info_raw_len,
               "public: metadata_size == info_raw_len");
    }
    bencode_free(root);
    rig_close(&r);
    torrent_free(t);
}

static void test_handshake(void) {
    printf("\n--- our ext handshake ---\n");
    check_our_handshake(0);
    check_our_handshake(1);
}

/* Peer (ut_metadata id 3) asks for metadata piece `piece`; returns the
 * parsed reply's msg_type (-1 if none), and copies the block out. */
static int request_piece(TorrentInfo *t, int piece, uint8_t *block, size_t *block_len,
                         int *alive) {
    Rig r;
    rig_open(&r, t, PS_SEED_READY);

    uint8_t in[256];
    size_t n = 0;
    const char *hs = "d1:md11:ut_metadatai3eee";
    n += ext_frame(in + n, 0, hs, strlen(hs));
    char req[64];
    int rl = snprintf(req, sizeof(req), "d8:msg_typei0e5:piecei%dee", piece);
    n += ext_frame(in + n, META_LOCAL_ID, req, (size_t)rl);
    rig_run(&r, t, in, n);
    *alive = r.sessions[0].phase != PS_DEAD;

    static uint8_t out[65536];
    size_t got = drain(r.sv[1], out, sizeof(out));
    size_t blen = 0;
    const uint8_t *body = find_ext(out, got, 0, 3, &blen);
    int type = -1;
    *block_len = 0;
    if (body) {
        BencodeNode *root = NULL;
        size_t used = bencode_parse_ex(body, blen, &root);
        BencodeNode *mt = bencode_dict_get(root, "msg_type");
        BencodeNode *pc = bencode_dict_get(root, "piece");
        if (mt && pc && pc->integer == piece) type = (int)mt->integer;
        if (used && used < blen) {
            *block_len = blen - used;
            memcpy(block, body + used, *block_len);
        }
        bencode_free(root);
    }
    rig_close(&r);
    return type;
}

static void test_serve(void) {
    printf("\n--- serving ut_metadata requests ---\n");

    static uint8_t block[META_BLOCK_SIZE];
    size_t blen;
    int alive;

    TorrentInfo *t = make_torrent(0);
    EXPECT(request_piece(t, 0, block, &blen, &alive) == 1, "public piece 0: data");
    EXPECT(alive, "public piece 0: session stays up");
    EXPECT(blen == META_BLOCK_SIZE && memcmp(block, t->info_raw, blen) == 0,
           "public piece 0: first 16 KiB of info dict");
    EXPECT(request_piece(t, 1, block, &blen, &alive) == 1, "public piece 1: data");
    EXPECT(blen == t->info_raw_len - META_BLOCK_SIZE &&
           memcmp(block, t->info_raw + META_BLOCK_SIZE, blen) == 0,
           "public piece 1: rest of info dict");
    EXPECT(request_piece(t, 2, block, &blen, &alive) == 2, "public piece 2: reject");
    torrent_free(t);

    t = make_torrent(1);
    EXPECT(request_piece(t, 0, block, &blen, &alive) == 2, "private piece 0: reject");
    EXPECT(blen == 0, "private: no metadata bytes sent");
    torrent_free(t);
}

static void test_private_pex(void) {
    printf("\n--- inbound PEX ---\n");
    for (int priv = 0; priv <= 1; priv++) {
        TorrentInfo *t = make_torrent(priv);
        Rig r;
        rig_open(&r, t, PS_SEED_READY);
        uint8_t in[128];
        size_t n = 0;
        const char *hs = "d1:md6:ut_pexi5eee";
        n += ext_frame(in + n, 0, hs, strlen(hs));
        const char pex[] = "d5:added6:\x0a\x00\x00\x02\x1a\xe1" "e";
        n += ext_frame(in + n, PEX_LOCAL_ID, pex, sizeof(pex) - 1);
        rig_run(&r, t, in, n);
        if (priv) {
            EXPECT(r.peers.count == 0, "private: PEX peers ignored");
            EXPECT(r.sessions[0].peer_pex_id == -1, "private: peer's ut_pex not recorded");
        } else {
            EXPECT(r.peers.count == 1, "public: PEX peer added");
            EXPECT(r.sessions[0].peer_pex_id == 5, "public: peer's ut_pex recorded");
        }
        rig_close(&r);
        torrent_free(t);
    }
}

int main(void) {
    printf("=== BEP 9 metadata serving / BEP 27 private torrents ===\n");
    log_init(LOG_ERROR, stderr);

    test_parse();
    test_handshake();
    test_serve();
    test_private_pex();

    printf("\n%d passed, %d failed\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
