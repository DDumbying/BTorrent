#define _POSIX_C_SOURCE 200809L
/**
 * ext_handshake.c — BEP 10 extension handshake builder
 *
 * Shared by the metadata fetcher (ext.c) and the download scheduler so the
 * message is built in exactly one place — and so the unit tests exercise the
 * real code rather than a copy of it.
 *
 * Output (keys sorted, as bencode requires):
 *   d1:md11:ut_metadatai<M>e6:ut_pexi<P>ee4:reqqi<Q>e1:v<len>:btorrent/<ver>e
 */

#include "proto/ext_handshake.h"
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
                        int ut_metadata_id, int ut_pex_id, int reqq) {
    size_t pos = 0;
    if (appendf(buf, cap, &pos, "d1:md") < 0) return -1;
    if (ut_metadata_id > 0 &&
        appendf(buf, cap, &pos, "11:ut_metadatai%de", ut_metadata_id) < 0)
        return -1;
    if (ut_pex_id > 0 &&
        appendf(buf, cap, &pos, "6:ut_pexi%de", ut_pex_id) < 0)
        return -1;
    if (appendf(buf, cap, &pos, "e") < 0) return -1;
    if (reqq > 0 && appendf(buf, cap, &pos, "4:reqqi%de", reqq) < 0)
        return -1;
    /* The length prefix is computed: a wrong one swallows the closing 'e'
     * and makes the whole dict unparseable for the remote peer. */
    if (appendf(buf, cap, &pos, "1:v%zu:%se",
                strlen(CLIENT_VERSION), CLIENT_VERSION) < 0)
        return -1;
    return (int)pos;
}
