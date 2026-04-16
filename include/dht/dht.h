#pragma once
/**
 * dht.h — BEP 5 DHT (Kademlia-based peer discovery)
 *
 * Finds peers for a given info_hash without relying on a tracker.
 * Runs over UDP on the same port as the BitTorrent client.
 *
 * Usage:
 *   DhtCtx *dht = dht_new(6881);
 *   dht_bootstrap(dht);                          // contact well-known nodes
 *   PeerList peers = dht_get_peers(dht, info_hash, 10);  // 10s timeout
 *   dht_free(dht);
 */

#include "proto/tracker.h"   /* PeerList */
#include <stdint.h>
#include <stddef.h>

typedef struct DhtCtx DhtCtx;

/** Create a DHT context bound to the given UDP port. Returns NULL on error. */
DhtCtx  *dht_new(uint16_t port);

/** Contact well-known bootstrap nodes to join the DHT network. */
void     dht_bootstrap(DhtCtx *ctx);

/**
 * dht_get_peers — find peers for info_hash via the DHT.
 *
 * Performs an iterative Kademlia lookup, collecting peers from responding
 * nodes. Blocks for up to timeout_s seconds.
 *
 * Returns a PeerList (caller must peer_list_free() it).
 */
PeerList dht_get_peers(DhtCtx *ctx, const uint8_t *info_hash, int timeout_s);

/** Free a DHT context and close its socket. */
void     dht_free(DhtCtx *ctx);
