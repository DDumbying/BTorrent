#define _POSIX_C_SOURCE 200809L
/**
 * cmd_check.c — "-c" check mode
 *
 * Verifies an existing download against the .torrent piece hashes without
 * re-downloading anything. Uses the same resume logic already in the piece
 * manager — it SHA-1 verifies every piece on disk and reports which are
 * good and which are missing or corrupt.
 *
 * Usage:
 *   btorrent -c ubuntu.torrent                 # checks ./ubuntu/
 *   btorrent -c ubuntu.torrent -o /tmp/ubuntu  # checks /tmp/ubuntu/
 *
 * Exit codes:
 *   0  all pieces verified
 *   1  one or more pieces missing or corrupt
 */

#include "cmd/cmd.h"
#include "core/torrent.h"
#include "core/pieces.h"
#include "core/sha1.h"
#include "proto/peer.h"
#include "utils.h"
#include "log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* ── cmd_check ───────────────────────────────────────────────────────────── */

int cmd_check(const Config *cfg) {
    /* 1. Parse torrent */
    TorrentInfo *torrent = torrent_parse(cfg->torrent_path);
    if (!torrent) return EXIT_FAILURE;

    /* 2. Resolve output path — mirror cmd_download: -o sets the directory */
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

    printf("\nChecking: %s\n", torrent->name);
    printf("Path    : %s\n", out_path);
    printf("Pieces  : %d × %d KiB\n\n",
           torrent->num_pieces, torrent->piece_length / 1024);

    PieceManager *pm = piece_manager_new(torrent, out_path);
    if (!pm) {
        LOG_ERROR("%s", "Failed to open output files — has the download been started?");
        torrent_free(torrent);
        return EXIT_FAILURE;
    }

    int good    = pm->completed;
    int total   = pm->num_pieces;
    int missing = total - good;
    int pct     = total > 0 ? (good * 100) / total : 0;

    /* 4. Report per-piece status (only print bad pieces to keep output short) */
    if (missing > 0) {
        printf("  Bad / missing pieces:\n");
        int shown = 0;
        for (int i = 0; i < total && shown < 20; i++) {
            if (pm->pieces[i].state != PIECE_COMPLETE) {
                printf("    piece %d\n", i);
                shown++;
            }
        }
        if (missing > 20)
            printf("    ... and %d more\n", missing - 20);
        printf("\n");
    }

    /* 5. Summary */
    printf("  Good    : %d / %d  (%d%%)\n", good, total, pct);
    printf("  Missing : %d\n", missing);

    if (missing == 0) {
        printf("  Status  : OK — download is complete and verified\n\n");
    } else {
        double missing_mb = (double)missing * torrent->piece_length
                            / (1024.0 * 1024.0);
        printf("  Status  : INCOMPLETE — %.1f MB missing\n", missing_mb);
        printf("            Run:  btorrent -d %s\n\n", cfg->torrent_path);
    }

    piece_manager_free(pm);
    torrent_free(torrent);
    return (missing == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
