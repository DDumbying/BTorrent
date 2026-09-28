#pragma once
/**
 * ext_handshake.h — BEP 10 extension handshake builder
 */

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
