#define _POSIX_C_SOURCE 200809L
/**
 * ext.c — BEP 10 Extension Protocol + BEP 9 ut_metadata metadata fetch
 *
 * Protocol flow for a single peer connection
 * -------------------------------------------
 *  1. TCP connect (blocking, with SO_RCVTIMEO / SO_SNDTIMEO).
 *  2. Standard BitTorrent handshake (68 bytes each way).
 *     - We set reserved byte 20 bit 0x10 (extension protocol bit, BEP 10).
 *  3. BEP 10 extension handshake (MSG_ID 20).
 *     - We send: d8:ut_metadatai1ee  (we want ut_metadata as ext msg 1)
 *     - We parse their response for:
 *         "ut_metadata" → their local message ID for ut_metadata
 *         "metadata_size" → total info-dict byte count
 *  4. Request each 16 KiB piece of the metadata:
 *       d8:msg_typei0e5:piecei<N>ee
 *     peer replies with:
 *       d8:msg_typei1e5:piecei<N>e10:total_sizei<M>ee<raw-block-bytes>
 *  5. After all pieces arrive, SHA-1 the reassembled buffer and verify
 *     it equals info_hash.  If it matches, bencode-parse it as a
 *     TorrentInfo and return it.
 *
 * References
 * ----------
 *   BEP 10 — Extension Protocol
 *     https://www.bittorrent.org/beps/bep_0010.html
 *   BEP 9  — Extension for Peers to Send Metadata Files
 *     https://www.bittorrent.org/beps/bep_0009.html
 */

#include "proto/ext.h"
#include "core/torrent.h"
#include "core/bencode.h"
#include "core/sha1.h"
#include "utils.h"
#include "log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

/* ── Constants ────────────────────────────────────────────────────────────── */

#define HANDSHAKE_LEN     68
#define PSTR              "BitTorrent protocol"
#define PSTRLEN           19
#define EXT_MSGID         20            /* BEP-10: extension messages use id 20 */
#define UT_META_LOCAL_ID  1             /* our local ext msg id for ut_metadata  */
#define META_BLOCK_SIZE   (16 * 1024)   /* BEP 9: 16 KiB per metadata block     */
#define MAX_METADATA_SIZE (10 * 1024 * 1024) /* 10 MiB — sanity cap             */
#define PEER_TIMEOUT_S    8     /* reduced from 15 — fail fast, try next peer */

/* ── Low-level I/O ────────────────────────────────────────────────────────── */

static int send_all(int sock, const uint8_t *buf, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(sock, buf + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0) return -1;
        sent += (size_t)n;
    }
    return 0;
}

static int recv_all(int sock, uint8_t *buf, size_t len) {
    size_t got = 0;
    while (got < len) {
        ssize_t n = recv(sock, buf + got, len - got, 0);
        if (n <= 0) return -1;
        got += (size_t)n;
    }
    return 0;
}

/* ── TCP connect with timeout ─────────────────────────────────────────────── */

static int tcp_connect_timed(const char *ip, uint16_t port, int timeout_s) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return -1;

    struct timeval tv = { .tv_sec = timeout_s, .tv_usec = 0 };
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    int flag = 1;
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    struct sockaddr_in addr = {0};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(port);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) <= 0) { close(sock); return -1; }

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(sock); return -1;
    }
    return sock;
}

/* ── Handshake ────────────────────────────────────────────────────────────── */

/**
 * do_handshake — send and receive the 68-byte BitTorrent handshake.
 *
 * We set reserved byte index 5 (offset 25 from start, counting from 1) to
 * 0x10, which is the BEP-10 "extension protocol supported" bit.
 *
 * Returns 0 on success, -1 on failure.
 */
static int do_handshake(int sock,
                        const uint8_t *info_hash,
                        const uint8_t *our_peer_id,
                        uint8_t        their_reserved[8]) {
    uint8_t hs[HANDSHAKE_LEN];
    hs[0] = PSTRLEN;
    memcpy(hs + 1,  PSTR,       PSTRLEN);
    memset(hs + 20, 0,          8);
    hs[25] = 0x11;                  /* BEP-10 extension bit + BEP-5 DHT bit */
    memcpy(hs + 28, info_hash,  20);
    memcpy(hs + 48, our_peer_id, 20);

    if (send_all(sock, hs, HANDSHAKE_LEN) < 0) return -1;

    uint8_t their[HANDSHAKE_LEN];
    if (recv_all(sock, their, HANDSHAKE_LEN) < 0) return -1;

    if (their[0] != PSTRLEN || memcmp(their + 1, PSTR, PSTRLEN) != 0) return -1;
    if (memcmp(their + 28, info_hash, 20) != 0) return -1;

    if (their_reserved) memcpy(their_reserved, their + 20, 8);
    return 0;
}

/* ── Wire message helpers ─────────────────────────────────────────────────── */

/**
 * send_wire_msg — frame and send a standard peer-wire message.
 *   [4 bytes: length][1 byte: id][payload]
 */
static int send_wire_msg(int sock, uint8_t id,
                         const uint8_t *payload, uint32_t plen) {
    uint32_t total = 1 + plen;
    uint8_t  hdr[5];
    write_uint32_be(hdr, total);
    hdr[4] = id;
    if (send_all(sock, hdr, 5) < 0) return -1;
    if (plen && send_all(sock, payload, plen) < 0) return -1;
    return 0;
}

/**
 * recv_wire_msg — receive one peer-wire message.
 * Returns message id and fills *out_payload (caller must free).
 * Returns -1 on error.
 */
static int recv_wire_msg(int sock, uint8_t **out_payload, uint32_t *out_plen) {
    uint8_t lbuf[4];
    if (recv_all(sock, lbuf, 4) < 0) return -1;
    uint32_t len = read_uint32_be(lbuf);

    /* keepalive */
    if (len == 0) { *out_payload = NULL; *out_plen = 0; return 255; }

    if (len > 4 * 1024 * 1024) return -1;  /* sanity */

    uint8_t id;
    if (recv_all(sock, &id, 1) < 0) return -1;

    uint32_t plen = len - 1;
    uint8_t *payload = NULL;
    if (plen > 0) {
        payload = xmalloc(plen);
        if (recv_all(sock, payload, plen) < 0) { free(payload); return -1; }
    }
    *out_payload = payload;
    *out_plen    = plen;
    return (int)(unsigned char)id;
}

/* ── Bencode builder (minimal — just what we need for ext messages) ────────── */

/**
 * We need to produce two bencode dicts:
 *
 *   Extension handshake:
 *     d1:md11:ut_metadatai1ee1:v<version-string>e
 *   (tells the peer we support ut_metadata as ext msg id 1)
 *
 *   Metadata request for block N:
 *     d8:msg_typei0e5:pieceiNee
 *
 * Rather than writing a full encoder, we just build these strings directly —
 * the format is fixed enough to hardcode.
 */

static int build_ext_handshake(uint8_t *buf, size_t cap) {
    /* d1:md11:ut_metadatai1ee1:v15:btorrent/0.9.0e */
    int n = snprintf((char *)buf, cap,
        "d"
            "1:m"  "d"
                "11:ut_metadata" "i%de"
            "e"
            "1:q" "i%de"          /* queue depth hint (we'll request 1 at a time) */
            "1:v" "15:btorrent/1.0.2"
        "e",
        UT_META_LOCAL_ID,
        1);
    return (n > 0 && (size_t)n < cap) ? n : -1;
}

static int build_meta_request(uint8_t *buf, size_t cap, int piece) {
    /* d8:msg_typei0e5:piecei<N>ee */
    int n = snprintf((char *)buf, cap,
        "d8:msg_typei0e5:piecei%dee", piece);
    return (n > 0 && (size_t)n < cap) ? n : -1;
}

/* ── BEP-10 extension handshake parser ────────────────────────────────────── */

typedef struct {
    int      ut_metadata_id;   /* peer's local ext id for ut_metadata, -1 if none */
    int      metadata_size;    /* total info-dict bytes, 0 if not advertised        */
} ExtHandshake;

static int parse_ext_handshake(const uint8_t *data, uint32_t len,
                                ExtHandshake  *out) {
    out->ut_metadata_id = -1;
    out->metadata_size  = 0;

    BencodeNode *root = bencode_parse(data, (size_t)len);
    if (!root) return -1;
    if (root->type != BENCODE_DICT) { bencode_free(root); return -1; }

    /* Look for m.ut_metadata */
    BencodeNode *m = bencode_dict_get(root, "m");
    if (m && m->type == BENCODE_DICT) {
        BencodeNode *utmeta = bencode_dict_get(m, "ut_metadata");
        if (utmeta && utmeta->type == BENCODE_INT)
            out->ut_metadata_id = (int)utmeta->integer;
    }

    /* Look for metadata_size */
    BencodeNode *msize = bencode_dict_get(root, "metadata_size");
    if (msize && msize->type == BENCODE_INT)
        out->metadata_size = (int)msize->integer;

    bencode_free(root);
    return 0;
}

/* ── BEP-9 metadata data message parser ───────────────────────────────────── */

typedef struct {
    int msg_type;      /* 0=request, 1=data, 2=reject */
    int piece;
    int total_size;
    const uint8_t *block;      /* points into payload, not owned */
    int block_len;
} MetaMsg;

/**
 * parse_meta_msg — parse the bencoded header of a ut_metadata message.
 *
 * The wire format is:  <bencoded-dict><raw-block-bytes>
 * The dict ends where the raw block bytes begin; bencode_parse_ex() tells
 * us how many bytes the dict consumed.
 */
static int parse_meta_msg(const uint8_t *payload, uint32_t plen, MetaMsg *out) {
    out->msg_type   = -1;
    out->piece      = -1;
    out->total_size = 0;
    out->block      = NULL;
    out->block_len  = 0;

    BencodeNode *root = NULL;
    size_t dict_len = bencode_parse_ex(payload, (size_t)plen, &root);
    if (!root || dict_len == 0) { bencode_free(root); return -1; }
    if (root->type != BENCODE_DICT) { bencode_free(root); return -1; }

    BencodeNode *n;
    n = bencode_dict_get(root, "msg_type");
    if (n && n->type == BENCODE_INT) out->msg_type = (int)n->integer;
    n = bencode_dict_get(root, "piece");
    if (n && n->type == BENCODE_INT) out->piece = (int)n->integer;
    n = bencode_dict_get(root, "total_size");
    if (n && n->type == BENCODE_INT) out->total_size = (int)n->integer;

    if (dict_len < plen) {
        out->block     = payload + dict_len;
        out->block_len = (int)(plen - dict_len);
    }

    bencode_free(root);
    return 0;
}

/* ── torrent_info_from_raw_dict ────────────────────────────────────────────── */

/**
 * Build a TorrentInfo from a raw bencoded info-dict buffer.
 *
 * The info-dict is exactly what lives inside a .torrent file under the
 * "info" key.  torrent_parse() normally calls into this path internally;
 * here we wrap a fake .torrent around the raw dict so we can reuse the
 * existing parser.
 *
 * We prepend:  d4:info
 * and append:  e
 * to form a minimal valid .torrent, write it to a tmpfile, parse it, then
 * delete the tmpfile.
 */
static TorrentInfo *torrent_info_from_raw_dict(const uint8_t *dict,
                                               size_t         dict_len,
                                               const uint8_t *expected_hash) {
    /* Verify SHA-1 before we trust the data at all. */
    uint8_t actual_hash[20];
    SHA1_CTX ctx;
    sha1_init(&ctx);
    sha1_update(&ctx, dict, dict_len);
    sha1_final(&ctx, actual_hash);

    if (memcmp(actual_hash, expected_hash, 20) != 0) {
        LOG_WARN("%s", "ext: metadata SHA-1 mismatch — data corrupted or wrong peer");
        return NULL;
    }
    LOG_INFO("%s", "ext: metadata SHA-1 verified OK");

    /* Wrap in a minimal bencoded dict: d4:info<raw-dict>e */
    size_t wrapper_len = 2 /* "d" + "e" */ + 6 /* "4:info" */ + dict_len;
    uint8_t *fake_torrent = xmalloc(wrapper_len);
    size_t pos = 0;
    memcpy(fake_torrent + pos, "d4:info", 7); pos += 7;
    memcpy(fake_torrent + pos, dict, dict_len); pos += dict_len;
    fake_torrent[pos++] = 'e';

    /* Write to a temp file.
     * Unlink immediately after open — the fd keeps the inode alive for
     * writing and parsing, but the directory entry is gone instantly so
     * a SIGKILL leaves nothing behind on disk. */
    char tmppath[] = "/tmp/btorrent_meta_XXXXXX";
    int fd = mkstemp(tmppath);
    if (fd < 0) { free(fake_torrent); return NULL; }
    unlink(tmppath);   /* directory entry gone; fd still valid */

    ssize_t written = write(fd, fake_torrent, pos);
    free(fake_torrent);

    if (written != (ssize_t)pos) { close(fd); return NULL; }

    /* Re-open via /proc/self/fd/<n> so torrent_parse() can read it */
    char fdpath[64];
    snprintf(fdpath, sizeof(fdpath), "/proc/self/fd/%d", fd);
    TorrentInfo *ti = torrent_parse(fdpath);
    close(fd);
    return ti;
}

/* ── Per-peer metadata fetch ──────────────────────────────────────────────── */

/**
 * try_peer_metadata — connect to one peer and try to download the metadata.
 *
 * Returns a heap-allocated TorrentInfo on success, NULL on any failure.
 * The socket is always closed before returning.
 */
static TorrentInfo *try_peer_metadata(const char    *ip,
                                      uint16_t       port,
                                      const uint8_t *info_hash,
                                      const uint8_t *our_peer_id) {
    LOG_DEBUG("ext: trying %s:%d", ip, port);

    int sock = tcp_connect_timed(ip, port, PEER_TIMEOUT_S);
    if (sock < 0) {
        LOG_DEBUG("ext: %s:%d connect failed", ip, port);
        return NULL;
    }

    /* ── Step 1: standard handshake ── */
    uint8_t their_reserved[8] = {0};
    if (do_handshake(sock, info_hash, our_peer_id, their_reserved) < 0) {
        LOG_DEBUG("ext: %s:%d handshake failed", ip, port);
        close(sock); return NULL;
    }

    /* Check peer supports BEP-10 (reserved byte 5, bit 0x10) */
    if (!(their_reserved[5] & 0x10)) {
        LOG_DEBUG("ext: %s:%d does not support BEP-10", ip, port);
        close(sock); return NULL;
    }

    /* ── Step 2: send our BEP-10 extension handshake ── */
    uint8_t ext_hs_body[256];
    int ext_hs_len = build_ext_handshake(ext_hs_body, sizeof(ext_hs_body));
    if (ext_hs_len < 0 ||
        send_wire_msg(sock, EXT_MSGID, ext_hs_body, (uint32_t)ext_hs_len) < 0) {
        LOG_DEBUG("ext: %s:%d failed to send ext handshake", ip, port);
        close(sock); return NULL;
    }

    /* ── Step 3: receive peer's ext handshake ── */
    ExtHandshake eh = {0};
    for (int retries = 0; retries < 10; retries++) {
        uint8_t *payload = NULL; uint32_t plen = 0;
        int id = recv_wire_msg(sock, &payload, &plen);
        if (id < 0) { free(payload); close(sock); return NULL; }

        if (id == EXT_MSGID && plen > 0) {
            /* First byte of payload is the sub-message id; 0 = handshake */
            if (payload[0] == 0)
                parse_ext_handshake(payload + 1, plen - 1, &eh);
            free(payload);
            break;
        }
        free(payload);
    }

    if (eh.ut_metadata_id <= 0) {
        LOG_DEBUG("ext: %s:%d does not support ut_metadata", ip, port);
        close(sock); return NULL;
    }
    if (eh.metadata_size <= 0 || eh.metadata_size > MAX_METADATA_SIZE) {
        LOG_DEBUG("ext: %s:%d bad metadata_size=%d", ip, port, eh.metadata_size);
        close(sock); return NULL;
    }

    LOG_INFO("ext: %s:%d has ut_metadata (ext_id=%d, size=%d bytes)",
             ip, port, eh.ut_metadata_id, eh.metadata_size);

    /* ── Step 4: request and receive metadata blocks ── ─────────────────────
     * We request blocks one at a time (request → wait → request) rather than
     * blasting all requests upfront. Many peers rate-limit or ignore burst
     * requests, delivering 0 blocks when pipelined but responding fine when
     * paced. The overhead is negligible — metadata is at most a few hundred
     * KB and 21 blocks × 8s timeout is still only 168s worst case. */
    int num_blocks = (eh.metadata_size + META_BLOCK_SIZE - 1) / META_BLOCK_SIZE;
    uint8_t *metadata = xcalloc((size_t)eh.metadata_size, 1);
    int *received = xcalloc((size_t)num_blocks, sizeof(int));
    int blocks_done = 0;

    for (int blk = 0; blk < num_blocks && blocks_done < num_blocks; blk++) {
        if (received[blk]) continue;  /* already got this one */

        /* Send request for this block */
        uint8_t req[64];
        int req_len = build_meta_request(req, sizeof(req), blk);
        if (req_len < 0) continue;
        uint8_t msg[65];
        msg[0] = (uint8_t)eh.ut_metadata_id;
        memcpy(msg + 1, req, (size_t)req_len);
        if (send_wire_msg(sock, EXT_MSGID, msg, (uint32_t)(1 + req_len)) < 0)
            break;

        /* Wait for a response — accept any message, skip non-metadata ones */
        int got_response = 0;
        for (int attempt = 0; attempt < 30 && !got_response; attempt++) {
            uint8_t *payload = NULL; uint32_t plen = 0;
            int id = recv_wire_msg(sock, &payload, &plen);
            if (id < 0) { free(payload); goto peer_done; }

            if (id != EXT_MSGID || plen < 2) { free(payload); continue; }

            uint8_t sub_id = payload[0];
            if (sub_id != UT_META_LOCAL_ID) { free(payload); continue; }

            MetaMsg mm = {0};
            if (parse_meta_msg(payload + 1, plen - 1, &mm) < 0) {
                free(payload); continue;
            }

            if (mm.msg_type == 2) {
                /* reject — peer doesn't have this block */
                LOG_DEBUG("ext: %s:%d rejected block %d", ip, port, mm.piece);
                free(payload); goto peer_done;
            }

            if (mm.msg_type == 1 &&
                mm.piece >= 0 && mm.piece < num_blocks &&
                mm.block && mm.block_len > 0 &&
                !received[mm.piece]) {

                int off      = mm.piece * META_BLOCK_SIZE;
                int maxcopy  = eh.metadata_size - off;
                int copy     = mm.block_len < maxcopy ? mm.block_len : maxcopy;
                memcpy(metadata + off, mm.block, (size_t)copy);
                received[mm.piece] = 1;
                blocks_done++;
                got_response = 1;
                LOG_DEBUG("ext: block %d/%d received (%d bytes)",
                          mm.piece + 1, num_blocks, copy);
            }
            free(payload);
        }
        if (!got_response) {
            LOG_DEBUG("ext: %s:%d no response for block %d", ip, port, blk);
            break;
        }
    }
peer_done:

    close(sock);

    TorrentInfo *result = NULL;
    if (blocks_done == num_blocks) {
        LOG_INFO("ext: all %d metadata blocks received — verifying SHA-1", num_blocks);
        result = torrent_info_from_raw_dict(metadata, (size_t)eh.metadata_size,
                                           info_hash);
    } else {
        LOG_WARN("ext: %s:%d incomplete metadata (%d/%d blocks)",
                 ip, port, blocks_done, num_blocks);
    }

    free(metadata);
    free(received);
    return result;
}

/* ── Public API ───────────────────────────────────────────────────────────── */

TorrentInfo *ext_fetch_metadata(const PeerList *peers,
                                const uint8_t  *info_hash,
                                int             max_peers,
                                volatile sig_atomic_t *interrupted) {
    if (!peers || peers->count == 0) return NULL;

    /* Generate a stable peer_id for this session */
    uint8_t our_peer_id[20];
    generate_peer_id(our_peer_id);

    int limit = peers->count;
    if (max_peers > 0 && max_peers < limit) limit = max_peers;

    LOG_INFO("ext: trying up to %d peers for metadata", limit);

    for (int i = 0; i < limit; i++) {
        if (interrupted && *interrupted) {
            LOG_INFO("%s", "ext: interrupted — stopping metadata fetch");
            return NULL;
        }
        const Peer *p = &peers->peers[i];
        TorrentInfo *ti = try_peer_metadata(p->ip, p->port,
                                            info_hash, our_peer_id);
        if (ti) {
            LOG_INFO("ext: metadata fetched from %s:%d", p->ip, p->port);
            return ti;
        }
    }

    LOG_WARN("%s", "ext: exhausted all peers — could not fetch metadata");
    return NULL;
}
