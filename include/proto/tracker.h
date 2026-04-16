#pragma once
/**
 * tracker.h — HTTP + UDP Tracker Communication
 */

#include "core/torrent.h"
#include <stdint.h>

typedef struct {
    char     ip[16];
    uint16_t port;
} Peer;

typedef struct {
    Peer  *peers;
    int    count;
    int    interval;
} PeerList;

void     generate_peer_id(uint8_t *out);

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
