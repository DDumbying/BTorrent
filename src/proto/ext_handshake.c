#define _POSIX_C_SOURCE 200809L
/**
 * ext_handshake.c — BEP 10 extension messages
 *
 * Builds our extension handshake and parses what the scheduler needs from
 * peers' messages. Shared by the metadata fetcher (ext.c) and the download
 * scheduler so each message is handled in exactly one place — and so the
 * unit tests and fuzzers exercise the real code rather than copies of it.
 *
 * Output (keys sorted, as bencode requires):
 *   d1:md11:ut_metadatai<M>e6:ut_pexi<P>ee13:metadata_sizei<S>e
 *    4:reqqi<Q>e1:v<len>:btorrent/<ver>e
 */

#include "proto/ext_handshake.h"
#include "core/bencode.h"
#include <arpa/inet.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#ifndef BT_VERSION
#  define BT_VERSION "dev"
#endif
#define CLIENT_VERSION "btorrent/" BT_VERSION

/* Append formatted text at *pos; returns -1 once the buffer is too small. */
__attribute__((format(printf, 4, 5)))
static int appendf(uint8_t *buf, size_t cap, size_t *pos, const char *fmt, ...) {
    if (*pos >= cap) return -1;
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf((char *)buf + *pos, cap - *pos, fmt, ap);
    va_end(ap);
    if (n < 0 || (size_t)n >= cap - *pos) return -1;
    *pos += (size_t)n;
    return 0;
}

int ext_build_handshake(uint8_t *buf, size_t cap,
                        int ut_metadata_id, int ut_pex_id,
                        size_t metadata_size, int reqq) {
    size_t pos = 0;
    if (appendf(buf, cap, &pos, "d1:md") < 0) return -1;
    if (ut_metadata_id > 0 &&
        appendf(buf, cap, &pos, "11:ut_metadatai%de", ut_metadata_id) < 0)
        return -1;
    if (ut_pex_id > 0 &&
        appendf(buf, cap, &pos, "6:ut_pexi%de", ut_pex_id) < 0)
        return -1;
    if (appendf(buf, cap, &pos, "e") < 0) return -1;
    if (metadata_size > 0 &&
        appendf(buf, cap, &pos, "13:metadata_sizei%zue", metadata_size) < 0)
        return -1;
    if (reqq > 0 && appendf(buf, cap, &pos, "4:reqqi%de", reqq) < 0)
        return -1;
    /* The length prefix is computed: a wrong one swallows the closing 'e'
     * and makes the whole dict unparseable for the remote peer. */
    if (appendf(buf, cap, &pos, "1:v%zu:%se",
                strlen(CLIENT_VERSION), CLIENT_VERSION) < 0)
        return -1;
    return (int)pos;
}

/* Peer messages are parsed with the real bencode parser: hand-rolled
 * substring scans matched inside unrelated values and overflowed on long
 * digit runs (found by fuzzing). */
static int parse_ext_id(const uint8_t *data, size_t len, const char *name) {
    BencodeNode *root = bencode_parse(data, len);
    BencodeNode *m    = bencode_dict_get(root, "m");
    BencodeNode *ext  = bencode_dict_get(m, name);
    int id = (ext && ext->type == BENCODE_INT &&
              ext->integer >= 1 && ext->integer <= 255) ? (int)ext->integer : -1;
    bencode_free(root);
    return id;
}

int ext_parse_pex_id(const uint8_t *data, size_t len) {
    return parse_ext_id(data, len, "ut_pex");
}

int ext_parse_metadata_id(const uint8_t *data, size_t len) {
    return parse_ext_id(data, len, "ut_metadata");
}

/* ── BEP 9: serving the info dict ────────────────────────────────────────── */

int meta_parse_request(const uint8_t *data, size_t len) {
    BencodeNode *root = NULL;
    size_t used = bencode_parse_ex(data, len, &root);
    BencodeNode *type  = bencode_dict_get(root, "msg_type");
    BencodeNode *piece = bencode_dict_get(root, "piece");
    int ok = used > 0 && root && root->type == BENCODE_DICT &&
             type  && type->type  == BENCODE_INT && type->integer == 0 &&
             piece && piece->type == BENCODE_INT &&
             piece->integer >= 0 && piece->integer <= INT_MAX;
    int result = ok ? (int)piece->integer : -1;
    bencode_free(root);
    return result;
}

int meta_build_response(uint8_t *buf, size_t cap, int piece,
                        const uint8_t *info, size_t info_len) {
    size_t pos = 0;
    size_t off = (size_t)piece * META_BLOCK_SIZE;
    if (!info || info_len == 0 || piece < 0 || off >= info_len) {
        /* Reject: we don't have (or won't share) that piece. */
        if (appendf(buf, cap, &pos, "d8:msg_typei2e5:piecei%dee", piece) < 0)
            return -1;
        return (int)pos;
    }
    size_t blen = info_len - off < META_BLOCK_SIZE ? info_len - off
                                                   : META_BLOCK_SIZE;
    if (appendf(buf, cap, &pos, "d8:msg_typei1e5:piecei%de10:total_sizei%zuee",
                piece, info_len) < 0)
        return -1;
    if (cap - pos < blen) return -1;
    memcpy(buf + pos, info + off, blen);
    return (int)(pos + blen);
}

int pex_parse_added(const uint8_t *data, size_t len, Peer *out, int max_out) {
    BencodeNode *root  = bencode_parse(data, len);
    BencodeNode *added = bencode_dict_get(root, "added");
    int found = 0;
    if (added && added->type == BENCODE_STR) {
        for (size_t k = 0; k + 6 <= added->str.len && found < max_out; k += 6) {
            const uint8_t *p = added->str.data + k;
            uint16_t port = (uint16_t)((p[4] << 8) | p[5]);
            if (port == 0) continue;
            memset(&out[found], 0, sizeof(out[found]));
            if (!inet_ntop(AF_INET, p, out[found].ip, sizeof(out[found].ip)))
                continue;
            out[found].port = port;
            found++;
        }
    }
    bencode_free(root);
    return found;
}
