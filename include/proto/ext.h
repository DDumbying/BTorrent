#pragma once
/**
 * ext.h — BEP 10 Extension Protocol + BEP 9 ut_metadata
 *
 * BEP 10 defines a generic extension handshake that peers exchange
 * immediately after the standard BitTorrent handshake.  It maps
 * extension names to local message IDs.
 *
 * BEP 9 defines "ut_metadata", an extension that lets peers share
 * the torrent info-dict without a .torrent file.  The info-dict is
 * split into 16 KiB blocks; a peer that has the full metadata can
 * serve individual blocks on request.
 *
 * Together these two BEPs are the minimum needed to make magnet links
 * actually download — you locate peers via DHT, shake hands, negotiate
 * the extension protocol, then fetch the metadata block-by-block and
 * reassemble it into a TorrentInfo.
 *
 * Public API
 * ----------
 *   ext_fetch_metadata()   — high-level: connect to a peer list and return
 *                            a heap-allocated TorrentInfo (or NULL).
 *
 * The implementation is self-contained in src/proto/ext.c.
 */

#include "core/torrent.h"
#include "proto/tracker.h"   /* PeerList, Peer */
#include <stdint.h>
#include <stddef.h>
#include <signal.h>

/**
 * ext_fetch_metadata - try to download the info-dict from the given peers.
 *
 * Tries each peer in turn (serial, with timeouts).  Returns a fully
 * populated TorrentInfo on success, or NULL if no peer could supply the
 * metadata or if interrupted is set.
 *
 * The returned pointer must be freed with torrent_free().
 *
 * @peers       list of peers to try (typically from DHT)
 * @info_hash   20-byte SHA-1 that identifies the torrent
 * @max_peers   cap on how many peers to try (0 = try all)
 * @interrupted pointer to signal flag; fetch stops if set non-zero (may be NULL)
 */
TorrentInfo *ext_fetch_metadata(const PeerList        *peers,
                                const uint8_t         *info_hash,
                                int                    max_peers,
                                volatile sig_atomic_t *interrupted);
