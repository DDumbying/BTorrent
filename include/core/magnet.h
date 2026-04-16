#pragma once
/**
 * magnet.h — Magnet link parser (BEP 9)
 *
 * Parses magnet URIs of the form:
 *   magnet:?xt=urn:btih:<info_hash>[&dn=<name>][&tr=<tracker>...]
 *
 * Usage:
 *   MagnetLink m;
 *   if (magnet_parse(uri, &m) == 0)
 *       // m.info_hash, m.name, m.trackers[] are populated
 */

#include <stdint.h>

#define MAGNET_MAX_TRACKERS 32

typedef struct {
    uint8_t info_hash[20];      /* binary info hash */
    char    info_hash_hex[41];  /* hex string */
    char    name[256];          /* dn= display name, may be empty */
    char    trackers[MAGNET_MAX_TRACKERS][768];  /* decoded tracker URLs */
    int     num_trackers;
} MagnetLink;

/**
 * magnet_parse — parse a magnet URI into a MagnetLink struct.
 *
 * Supports both hex (40-char) and base32 (32-char) info_hash encodings.
 * Returns 0 on success, -1 on error (malformed URI or missing xt=).
 */
int magnet_parse(const char *uri, MagnetLink *out);
