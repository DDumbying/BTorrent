#pragma once
/**
 * ext_handshake.h — BEP 10 extension messages: our handshake, and parsing
 * the parts of peers' messages the scheduler uses (ut_pex id, PEX peers).
 */

#include "proto/tracker.h"   /* Peer */
#include <stddef.h>
#include <stdint.h>

/**
 * ext_build_handshake — write our BEP 10 handshake dict into buf.
 *
 * @ut_metadata_id  our local id for ut_metadata (<= 0 to omit)
 * @ut_pex_id       our local id for ut_pex      (<= 0 to omit)
 * @reqq            request queue depth hint     (<= 0 to omit)
 *
 * Returns bytes written (no NUL counted), or -1 if cap is too small.
 */
int ext_build_handshake(uint8_t *buf, size_t cap,
                        int ut_metadata_id, int ut_pex_id, int reqq);

/**
 * ext_parse_pex_id — the peer's ut_pex message id from its BEP 10 handshake
 * dict (the payload after the sub-id byte). Returns 1..255, or -1 if the
 * peer does not support PEX or the data is malformed.
 */
int ext_parse_pex_id(const uint8_t *data, size_t len);

/**
 * pex_parse_added — IPv4 peers from the "added" field of a ut_pex message
 * (BEP 11). Fills up to max_out entries of out, skipping port 0; returns
 * how many were filled. Malformed data yields 0.
 */
int pex_parse_added(const uint8_t *data, size_t len, Peer *out, int max_out);
