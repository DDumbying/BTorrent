#pragma once
/**
 * tracker.h — HTTP + UDP Tracker Communication
 */

#include "core/torrent.h"
#include <stdint.h>
#include <signal.h>

typedef struct {
    char     ip[46];
    uint16_t port;
    int      is_ipv6;
} Peer;

typedef struct {
    Peer  *peers;
    int    count;
    int    interval;
} PeerList;

typedef struct {
    char     host[256];
    uint64_t conn_id;
    int64_t  expires_at;
} UdpConnCache;

void udp_cache_init(UdpConnCache *cache);
uint64_t udp_cache_get(UdpConnCache *cache, const char *host, int64_t now);
void udp_cache_set(UdpConnCache *cache, const char *host, uint64_t conn_id, int64_t expires_at);

void     generate_peer_id(uint8_t *out);

/**
 * tracker_set_abort_flag — make announces interruptible.
 * Once *flag is non-zero, pending announces, retries and backoff sleeps
 * return early. "stopped" announces are exempt so they can still be sent
 * on shutdown (with a short timeout, stopping at the first tracker that
 * replies). Call once at startup.
 */
void     tracker_set_abort_flag(volatile sig_atomic_t *flag);

PeerList tracker_announce(const TorrentInfo *torrent,
                          const uint8_t     *peer_id,
                          uint16_t           port,
                          long               downloaded,
                          long               uploaded,
                          long               left,
                          const char        *event);

/**
 * tracker_announce_url — announce to a single specific tracker URL.
 * Used when iterating backup trackers manually.
 */
PeerList tracker_announce_url(const char        *url,
                               const TorrentInfo *torrent,
                               const uint8_t     *peer_id,
                               uint16_t           port,
                               long               downloaded,
                               long               uploaded,
                               long               left,
                               const char        *event);

/**
 * tracker_announce_with_retry — tracker_announce with exponential backoff.
 * Retries up to 5 times: 1s, 2s, 4s, 8s, 16s delays.
 */
PeerList tracker_announce_with_retry(const TorrentInfo *torrent,
                                     const uint8_t     *peer_id,
                                     uint16_t           port,
                                     long               downloaded,
                                     long               uploaded,
                                     long               left,
                                     const char        *event);

void peer_list_free(PeerList *pl);

PeerList compact_peers(const uint8_t *d, size_t len);
PeerList compact6_peers(const uint8_t *d, size_t len);

/** Parse an HTTP tracker reply: 0 on success (fills *out), -1 if invalid. */
int      tracker_parse_http_response(const uint8_t *data, size_t len, PeerList *out);
