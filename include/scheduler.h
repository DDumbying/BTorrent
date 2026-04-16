#pragma once
/**
 * scheduler.h — Concurrent peer download scheduler (epoll-based)
 *
 * Replaces the serial "one peer at a time" loop in cmd_download.c.
 * Manages up to cfg->max_peers simultaneous non-blocking TCP connections,
 * each running through a per-peer state machine driven by epoll events.
 *
 * Public interface is a single function: scheduler_run().
 * It blocks until the download is complete, interrupted, or all peers
 * are exhausted.
 */

#include "core/torrent.h"
#include "core/pieces.h"
#include "proto/tracker.h"
#include "cmd/cmd.h"
#include <stdint.h>
#include <signal.h>

/**
 * scheduler_run — download torrent using concurrent non-blocking peers.
 *
 * @torrent      parsed torrent metadata
 * @pm           piece manager (already initialised, may have resumed pieces)
 * @initial      first peer list from tracker announce
 * @peer_id      our 20-byte peer ID
 * @cfg          runtime configuration (max_peers, pipeline_depth, etc.)
 * @interrupted  pointer to the signal flag set by SIGINT handler
 *
 * Returns EXIT_SUCCESS if download completed, EXIT_FAILURE otherwise.
 */
int scheduler_run(const TorrentInfo *torrent,
                  PieceManager      *pm,
                  PeerList          *initial,
                  const uint8_t     *peer_id,
                  const Config      *cfg,
                  volatile sig_atomic_t *interrupted);
