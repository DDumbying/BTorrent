#define _POSIX_C_SOURCE 200809L
/**
 * magnet.c — Magnet URI parser (BEP 9)
 *
 * Handles:
 *   magnet:?xt=urn:btih:<40-hex or 32-base32>&dn=<name>&tr=<url>&tr=...
 *
 * Base32 alphabet: A-Z 2-7 (RFC 4648), case-insensitive.
 */

#include "core/magnet.h"
#include "log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

/* ── URL decode ──────────────────────────────────────────────────────────── */

static int hex_val(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

/* URL-decode src into dst (dst must be at least strlen(src)+1 bytes).
 * Returns number of bytes written (excluding NUL terminator). */
static int url_decode(const char *src, char *dst, size_t dst_cap) {
    size_t out = 0;
    for (size_t i = 0; src[i] && out + 1 < dst_cap; ) {
        if (src[i] == '%' && src[i+1] && src[i+2]) {
            int hi = hex_val(src[i+1]);
            int lo = hex_val(src[i+2]);
            if (hi >= 0 && lo >= 0) {
                dst[out++] = (char)((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        if (src[i] == '+') { dst[out++] = ' '; i++; continue; }
        dst[out++] = src[i++];
    }
    dst[out] = '\0';
    return (int)out;
}

/* ── Base32 decode ───────────────────────────────────────────────────────── */

static int base32_val(char c) {
    c = (char)toupper((unsigned char)c);
    if (c >= 'A' && c <= 'Z') return c - 'A';
    if (c >= '2' && c <= '7') return 26 + (c - '2');
    return -1;
}

/* Decode a 32-char base32 string into 20 bytes. Returns 0 on success. */
static int base32_decode_20(const char *src, uint8_t *out) {
    if (strlen(src) != 32) return -1;
    /* 8 base32 chars → 5 bytes */
    for (int group = 0; group < 4; group++) {
        int v[8];
        for (int k = 0; k < 8; k++) {
            v[k] = base32_val(src[group * 8 + k]);
            if (v[k] < 0) return -1;
        }
        uint8_t *o = out + group * 5;
        o[0] = (uint8_t)((v[0] << 3) | (v[1] >> 2));
        o[1] = (uint8_t)((v[1] << 6) | (v[2] << 1) | (v[3] >> 4));
        o[2] = (uint8_t)((v[3] << 4) | (v[4] >> 1));
        o[3] = (uint8_t)((v[4] << 7) | (v[5] << 2) | (v[6] >> 3));
        o[4] = (uint8_t)((v[6] << 5) | v[7]);
    }
    return 0;
}

/* ── Parameter iterator ──────────────────────────────────────────────────── */

/* Parse key=value from a query string. Advances *pos past the parameter.
 * key and val are NUL-terminated. Returns 1 if a param was read, 0 at end. */
static int next_param(const char *qs, size_t *pos,
                       char *key, size_t ksz,
                       char *raw_val, size_t vsz) {
    if (!qs[*pos]) return 0;
    /* Find '=' */
    size_t k = 0;
    while (qs[*pos] && qs[*pos] != '=' && qs[*pos] != '&') {
        if (k + 1 < ksz) key[k++] = qs[(*pos)++];
        else (*pos)++;
    }
    key[k] = '\0';
    if (qs[*pos] == '=') (*pos)++;

    /* Find next '&' or end */
    size_t v = 0;
    while (qs[*pos] && qs[*pos] != '&') {
        if (v + 1 < vsz) raw_val[v++] = qs[(*pos)++];
        else (*pos)++;
    }
    raw_val[v] = '\0';
    if (qs[*pos] == '&') (*pos)++;
    return 1;
}

/* ── Public API ──────────────────────────────────────────────────────────── */

int magnet_parse(const char *uri, MagnetLink *out) {
    memset(out, 0, sizeof(*out));

    /* Must start with "magnet:?" */
    if (strncmp(uri, "magnet:?", 8) != 0) {
        LOG_WARN("magnet: not a magnet URI: %.40s", uri);
        return -1;
    }

    const char *qs  = uri + 8;
    size_t      pos = 0;
    int         got_hash = 0;

    /* raw[] holds the URL-encoded value before decoding.
     * Tracker URLs can be ~200 chars decoded; URL-encoding triples the
     * length in the worst case (%XX per char), so 2048 is safe. */
    char key[64], raw[2048], val[2048];

    while (next_param(qs, &pos, key, sizeof(key), raw, sizeof(raw))) {
        url_decode(raw, val, sizeof(val));

        if (strcmp(key, "xt") == 0) {
            /* xt=urn:btih:<hash> */
            const char *prefix = "urn:btih:";
            if (strncmp(val, prefix, strlen(prefix)) != 0) continue;
            const char *hash_str = val + strlen(prefix);
            size_t hlen = strlen(hash_str);

            if (hlen == 40) {
                /* Hex-encoded info_hash */
                for (int i = 0; i < 20; i++) {
                    int hi = hex_val(hash_str[i * 2]);
                    int lo = hex_val(hash_str[i * 2 + 1]);
                    if (hi < 0 || lo < 0) { LOG_WARN("%s", "magnet: bad hex hash"); return -1; }
                    out->info_hash[i] = (uint8_t)((hi << 4) | lo);
                }
                snprintf(out->info_hash_hex, sizeof(out->info_hash_hex),
                         "%s", hash_str);
                /* Lowercase the hex */
                for (int i = 0; i < 40; i++)
                    out->info_hash_hex[i] = (char)tolower((unsigned char)out->info_hash_hex[i]);
                got_hash = 1;

            } else if (hlen >= 32) {
                /* Base32-encoded info_hash */
                if (base32_decode_20(hash_str, out->info_hash) < 0) {
                    LOG_WARN("%s", "magnet: bad base32 hash");
                    return -1;
                }
                for (int i = 0; i < 20; i++)
                    snprintf(out->info_hash_hex + i * 2, 3, "%02x", out->info_hash[i]);
                got_hash = 1;
            }

        } else if (strcmp(key, "dn") == 0) {
            /* val is url-decoded into a 1024-byte buffer; name is 256 — truncate safely */
            val[sizeof(out->name) - 1] = '\0';
            memcpy(out->name, val, sizeof(out->name) - 1);
            out->name[sizeof(out->name) - 1] = '\0';

        } else if (strcmp(key, "tr") == 0) {
            if (out->num_trackers < MAGNET_MAX_TRACKERS) {
                size_t tsz = sizeof(out->trackers[0]);
                val[tsz - 1] = '\0';
                memcpy(out->trackers[out->num_trackers], val, tsz - 1);
                out->trackers[out->num_trackers][tsz - 1] = '\0';
                out->num_trackers++;
            }
        }
    }

    if (!got_hash) {
        LOG_WARN("%s", "magnet: no xt=urn:btih: found");
        return -1;
    }

    LOG_INFO("magnet: parsed — hash=%s name='%s' trackers=%d",
             out->info_hash_hex, out->name, out->num_trackers);
    return 0;
}
