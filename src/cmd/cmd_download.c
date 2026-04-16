#define _POSIX_C_SOURCE 200809L
/**
 * cmd_download.c — "-d" download mode
 *
 * Owns the full download lifecycle:
 *   1. Parse torrent
 *   2. Announce to tracker (with retry + backoff)
 *   3. Create piece manager (handles resume automatically)
 *   4. Hand off to scheduler_run() — epoll concurrent peer download
 *   5. Send "completed" or "stopped" tracker event on finish/interrupt
 */

#include "cmd/cmd.h"
#include "scheduler.h"
#include "core/torrent.h"
#include "core/magnet.h"
#include "dht/dht.h"
#include "proto/tracker.h"
#include "proto/peer.h"
#include "proto/ext.h"
#include "core/pieces.h"
#include "utils.h"
#include "log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <unistd.h>

/* Signal flag declared in main.c, shared via extern */
extern volatile sig_atomic_t g_interrupted;

/* ── cmd_download ────────────────────────────────────────────────────────── */

int cmd_download(const Config *cfg) {
    time_t session_start = time(NULL);

    /* 1. Parse torrent or magnet link */
    TorrentInfo *torrent = NULL;

    if (cfg->is_magnet) {
        /*
         * Magnet link — full download pipeline:
         *
         *   [1/5] Parse magnet URI → extract info_hash + tracker hints
         *   [2/5] DHT peer discovery (+ tracker announce if trackers present)
         *   [3/5] BEP 9/10 metadata fetch — download the info-dict from peers
         *   [4/5] Hand off to the normal scheduler with the recovered TorrentInfo
         *   [5/5] Tracker completion event
         */
        LOG_INFO("[1/5] Parsing magnet: %s", cfg->torrent_path);
        MagnetLink mag;
        if (magnet_parse(cfg->torrent_path, &mag) < 0) {
            LOG_ERROR("Failed to parse magnet link: %s\n"
                      "       Expected format: magnet:?xt=urn:btih:<hash>&dn=<name>",
                      cfg->torrent_path);
            return EXIT_FAILURE;
        }
        LOG_INFO("magnet: info_hash=%s name='%s' trackers=%d",
                 mag.info_hash_hex,
                 mag.name[0] ? mag.name : "(unknown)",
                 mag.num_trackers);

        /* ── [2/5] Peer discovery ── */
        LOG_INFO("%s", "[2/5] Peer discovery (DHT + trackers)...");

        PeerList peers = { .peers = NULL, .count = 0, .interval = 1800 };

        /* Try trackers from the magnet first — fastest when available */
        if (mag.num_trackers > 0) {
            /* Build a temporary stub TorrentInfo just for tracker_announce */
            TorrentInfo stub = {0};
            memcpy(stub.info_hash, mag.info_hash, 20);
            snprintf(stub.announce, sizeof(stub.announce), "%s", mag.trackers[0]);
            for (int i = 0; i < mag.num_trackers && i < MAX_TRACKERS; i++) {
                snprintf(stub.announce_list[i], sizeof(stub.announce_list[i]), "%s", mag.trackers[i]);
                stub.num_trackers++;
            }
            stub.total_length = 1; /* non-zero so tracker sees us as a leecher */
            uint8_t tmp_peer_id[20];
            generate_peer_id(tmp_peer_id);
            peers = tracker_announce_with_retry(&stub, tmp_peer_id, cfg->port,
                                                0, 0, 1, "started");
            if (peers.count > 0)
                LOG_INFO("magnet: %d peers from tracker", peers.count);
        }

        /* DHT — always run it; merge results */
        DhtCtx *dht = dht_new(cfg->port);
        if (dht) {
            dht_bootstrap(dht);
            PeerList dht_peers = dht_get_peers(dht, mag.info_hash, 30);
            dht_free(dht);
            LOG_INFO("magnet: %d peers from DHT", dht_peers.count);
            if (dht_peers.count > 0) {
                if (peers.count == 0) {
                    peers = dht_peers;
                } else {
                    /* merge, dedup by IP:port */
                    for (int j = 0; j < dht_peers.count; j++) {
                        int dup = 0;
                        for (int k = 0; k < peers.count; k++) {
                            if (strcmp(peers.peers[k].ip, dht_peers.peers[j].ip) == 0 &&
                                peers.peers[k].port == dht_peers.peers[j].port) {
                                dup = 1; break;
                            }
                        }
                        if (!dup) {
                            Peer *tmp = realloc(peers.peers,
                                (size_t)(peers.count + 1) * sizeof(Peer));
                            if (!tmp) { LOG_WARN("%s", "out of memory merging DHT peers"); break; }
                            peers.peers = tmp;
                            peers.peers[peers.count++] = dht_peers.peers[j];
                        }
                    }
                    free(dht_peers.peers);
                }
            }
        }

        if (peers.count == 0) {
            LOG_ERROR("%s", "magnet: no peers found via DHT or trackers.\n"
                      "       The DHT may be unreachable on your network, or\n"
                      "       this torrent has no active peers right now.\n"
                      "       Try again later or add -v to see details.");
            return EXIT_FAILURE;
        }
        LOG_INFO("magnet: %d total unique peers for metadata fetch", peers.count);

        /* ── [3/5] BEP 9/10 metadata fetch ── */
        LOG_INFO("%s", "[3/5] Fetching torrent metadata via BEP 9/10 (ut_metadata)...");
        torrent = ext_fetch_metadata(&peers, mag.info_hash, 0, &g_interrupted);
        if (!torrent) {
            LOG_ERROR("%s", "magnet: could not fetch torrent metadata from any peer.\n"
                      "       None of the discovered peers support the ut_metadata\n"
                      "       extension (BEP 9), or all connections timed out.\n"
                      "       Try: obtain the .torrent file directly and use -d file.torrent");
            peer_list_free(&peers);
            return EXIT_FAILURE;
        }

        /* Copy tracker hints from the magnet into the recovered TorrentInfo
         * (the info-dict itself never contains tracker URLs). */
        if (torrent->announce[0] == '\0' && mag.num_trackers > 0)
            snprintf(torrent->announce, sizeof(torrent->announce), "%s", mag.trackers[0]);
        for (int i = 0; i < mag.num_trackers && i < MAX_TRACKERS; i++) {
            if (i >= torrent->num_trackers) {
                snprintf(torrent->announce_list[torrent->num_trackers], sizeof(torrent->announce_list[0]),
                         "%s", mag.trackers[i]);
                torrent->num_trackers++;
            }
        }

        LOG_INFO("magnet: metadata OK — '%s', %d pieces, %.1f MB",
                 torrent->name, torrent->num_pieces,
                 (double)torrent->total_length / (1024.0 * 1024.0));
        torrent_print(torrent);

        /* ── [4/5] + [5/5]: fall through to the normal download path ── */
        /*
         * From this point the magnet path is identical to the .torrent path.
         * We have a fully populated TorrentInfo, a peer list, and the rest of
         * cmd_download handles output path, piece manager, scheduler, and the
         * tracker completion event.
         *
         * Jump past the ".torrent file parse" block and continue.
         */
        goto magnet_resume;
    }

    LOG_INFO("[1/4] Parsing: %s", cfg->torrent_path);
    torrent = torrent_parse(cfg->torrent_path);
    if (!torrent) return EXIT_FAILURE;
    torrent_print(torrent);

magnet_resume:; /* semicolon: label must precede a statement, not a declaration */
    /* 2. Resolve output path.
     *    -o sets the DIRECTORY to save into (matching qBittorrent/Transmission).
     *    For single-file torrents: output = out_dir/torrent_name
     *    For multi-file torrents:  output = out_dir/torrent_name/ (directory)
     *    If -o is not given, default to torrent name in current directory. */
    char out_path[1024];
    if (cfg->output_path[0]) {
        int n = snprintf(out_path, sizeof(out_path) - 1, "%s/%s",
                         cfg->output_path, torrent->name);
        if (n < 0 || n >= (int)sizeof(out_path) - 1)
            strncpy(out_path, torrent->name, sizeof(out_path) - 1);
    } else {
        strncpy(out_path, torrent->name, sizeof(out_path) - 1);
    }
    out_path[sizeof(out_path) - 1] = '\0';

    /* 3. Peer ID + tracker announce */
    uint8_t peer_id[20];
    generate_peer_id(peer_id);

    LOG_INFO("%s", "[2/4] Announcing to tracker...");
    PeerList peers = tracker_announce_with_retry(
        torrent, peer_id, cfg->port,
        0, 0, torrent->total_length, "started");
    if (peers.count == 0) {
        LOG_WARN("%s", "No peers from tracker — trying DHT (30s)...");
        DhtCtx *dht = dht_new(cfg->port);
        if (dht) {
            dht_bootstrap(dht);
            peers = dht_get_peers(dht, torrent->info_hash, 30);
            dht_free(dht);
        }
        if (peers.count == 0) {
            LOG_ERROR("%s", "No peers found from tracker or DHT.\n"
                      "       Possible causes: firewall blocking UDP (DHT),\n"
                      "       tracker is down, or torrent has no active peers.\n"
                      "       Try: -v for details, or -p <alt-port> if port 6881 is blocked.");
            torrent_free(torrent);
            return EXIT_FAILURE;
        }
        LOG_INFO("dht: %d peers found", peers.count);
    }
    LOG_INFO("      %d peers (re-announce in %ds)",
             peers.count, peers.interval);

    /* If the primary tracker returned very few peers, scrape backup trackers
     * right now to build a larger pool before starting the scheduler. */
    if (peers.count < 50 && torrent->num_trackers > 0) {
        LOG_INFO("%s", "      few peers — trying backup trackers...");
        for (int i = 0; i < torrent->num_trackers; i++) {
            const char *url = torrent->announce_list[i];
            if (url[0] == '\0' || strcmp(url, torrent->announce) == 0) continue;
            PeerList extra = tracker_announce_url(url, torrent, peer_id, cfg->port,
                0, 0, torrent->total_length, "started");
            if (extra.count > 0) {
                /* Merge, deduplicating by IP:port */
                int added = 0;
                for (int j = 0; j < extra.count; j++) {
                    int dup = 0;
                    for (int k = 0; k < peers.count; k++) {
                        if (strcmp(peers.peers[k].ip, extra.peers[j].ip) == 0 &&
                            peers.peers[k].port == extra.peers[j].port) {
                            dup = 1; break;
                        }
                    }
                    if (!dup) {
                        Peer *tmp = realloc(peers.peers,
                            (size_t)(peers.count + 1) * sizeof(Peer));
                        if (!tmp) { LOG_WARN("%s", "out of memory merging tracker peers"); break; }
                        peers.peers = tmp;
                        peers.peers[peers.count++] = extra.peers[j];
                        added++;
                    }
                }
                LOG_INFO("      +%d peers (deduped) from %s", added, url);
                peer_list_free(&extra);
                if (peers.count >= 200) break;
            }
        }
        LOG_INFO("      total: %d unique peers", peers.count);
    }

    /* If still very few peers, try DHT to supplement.
     * Trigger at < 20 peers (not just < 10) — Ubuntu's tracker returns 1 peer
     * and the backup returns 0, so we'd stall on a single connection.
     * Use 30s timeout: DHT needs ~5 rounds × 2s each to converge, plus
     * resolution time for bootstrap nodes. */
    if (peers.count < 20) {
        LOG_INFO("      %d peers is too few — running DHT (30s)...", peers.count);
        DhtCtx *dht = dht_new((uint16_t)(cfg->port + 1));
        if (dht) {
            dht_bootstrap(dht);
            PeerList dht_peers = dht_get_peers(dht, torrent->info_hash, 30);
            dht_free(dht);
            if (dht_peers.count > 0) {
                /* Merge with dedup */
                int added = 0;
                for (int j = 0; j < dht_peers.count; j++) {
                    int dup = 0;
                    for (int k = 0; k < peers.count; k++) {
                        if (strcmp(peers.peers[k].ip, dht_peers.peers[j].ip) == 0 &&
                            peers.peers[k].port == dht_peers.peers[j].port) {
                            dup = 1; break;
                        }
                    }
                    if (!dup) {
                        Peer *tmp = realloc(peers.peers,
                            (size_t)(peers.count + 1) * sizeof(Peer));
                        if (!tmp) { LOG_WARN("%s", "out of memory merging DHT supplement"); break; }
                        peers.peers = tmp;
                        peers.peers[peers.count++] = dht_peers.peers[j];
                        added++;
                    }
                }
                LOG_INFO("      +%d DHT peers (total: %d)", added, peers.count);
                peer_list_free(&dht_peers);
            }
        }
    }

    /* 4. Piece manager (auto-resumes from existing output) */
    LOG_INFO("[3/4] Piece manager → %s", out_path);
    PieceManager *pm = piece_manager_new(torrent, out_path);
    if (!pm) {
        peer_list_free(&peers);
        torrent_free(torrent);
        return EXIT_FAILURE;
    }

    if (piece_manager_is_complete(pm)) {
        LOG_INFO("%s", "All pieces already on disk — nothing to download.");
        piece_manager_free(pm);
        peer_list_free(&peers);
        torrent_free(torrent);
        return EXIT_SUCCESS;
    }

    /* 5. Download — concurrent epoll scheduler */
    LOG_INFO("[4/4] Downloading %d pieces via up to %d peers...",
             torrent->num_pieces, cfg->max_peers);

    int rc = scheduler_run(torrent, pm, &peers, peer_id, cfg, &g_interrupted);

    /* 6. Tracker completion / stopped event */
    time_t elapsed = time(NULL) - session_start;

    if (g_interrupted) {
        LOG_INFO("%s", "Interrupted — sending stopped to tracker");
        long dl = (long)pm->completed * torrent->piece_length;
        PeerList tmp = tracker_announce(torrent, peer_id, cfg->port,
                                        dl, 0, torrent->total_length - dl,
                                        "stopped");
        peer_list_free(&tmp);
    }

    int complete = piece_manager_is_complete(pm);

    if (complete) {
        LOG_INFO("%s", "Download complete!");
        LOG_INFO("  Output : %s", out_path);
        LOG_INFO("  Size   : %.2f MB",
                 (double)torrent->total_length / (1024.0 * 1024.0));
        LOG_INFO("  Time   : %lds", (long)elapsed);
        if (elapsed > 0)
            LOG_INFO("  Avg    : %.1f KB/s",
                     (double)torrent->total_length / 1024.0 / (double)elapsed);

        long dl = torrent->total_length;
        PeerList tmp = tracker_announce(torrent, peer_id, cfg->port,
                                        dl, 0, 0, "completed");
        peer_list_free(&tmp);
    } else {
        long long written = (long long)pm->completed * torrent->piece_length;
        LOG_WARN("Incomplete: %d/%d pieces (%.1f MB written, %.1f MB remaining)",
                 pm->completed, pm->num_pieces,
                 (double)written / (1024.0 * 1024.0),
                 (double)(torrent->total_length - written) / (1024.0 * 1024.0));
        LOG_WARN("Output file is sparse — reported size is %.1f GB but only %.1f MB on disk",
                 (double)torrent->total_length / (1024.0 * 1024.0 * 1024.0),
                 (double)written / (1024.0 * 1024.0));
        LOG_WARN("Re-run the same command to resume: btorrent -d %s -o ...",
                 cfg->torrent_path);
    }

    piece_manager_free(pm);
    peer_list_free(&peers);
    torrent_free(torrent);
    (void)rc;
    return complete ? EXIT_SUCCESS : EXIT_FAILURE;
}
