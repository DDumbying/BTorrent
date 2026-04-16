#define _POSIX_C_SOURCE 200809L
/**
 * cmd_inspect.c — "-i" inspect mode
 *
 * Parses a .torrent file and prints its metadata without downloading anything.
 * With --json / -j it emits a machine-readable JSON object so the output
 * can be piped into jq, scripts, or other tools.
 *
 * Human output (default):
 *   btorrent -i ubuntu.torrent
 *
 * JSON output:
 *   btorrent -i ubuntu.torrent --json
 *   btorrent -i ubuntu.torrent -j | jq .name
 */

#include "cmd/cmd.h"
#include "core/torrent.h"
#include "utils.h"
#include "log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* ── Human-readable output ───────────────────────────────────────────────── */

static void print_human(const TorrentInfo *t) {
    char hash[41];
    hex_to_str(t->info_hash, 20, hash);

    /* Use stdout directly — this is user-facing content, not a log line */
    printf("\n");
    printf("  Name        : %s\n", t->name);
    printf("  Info hash   : %s\n", hash);
    printf("  Size        : %ld bytes (%.2f MB)\n",
           t->total_length, (double)t->total_length / (1024.0 * 1024.0));
    printf("  Piece length: %d bytes (%d KiB)\n",
           t->piece_length, t->piece_length / 1024);
    printf("  Pieces      : %d\n", t->num_pieces);
    printf("  Files       : %d\n", t->num_files);

    if (t->comment[0])
        printf("  Comment     : %s\n", t->comment);
    if (t->created_by[0])
        printf("  Created by  : %s\n", t->created_by);

    printf("\n  Trackers:\n");
    if (t->announce[0])
        printf("    [primary] %s\n", t->announce);
    for (int i = 0; i < t->num_trackers; i++)
        printf("    [%d] %s\n", i, t->announce_list[i]);

    if (t->num_files > 1) {
        printf("\n  Files (%d):\n", t->num_files);
        int show = t->num_files > 50 ? 50 : t->num_files;
        for (int i = 0; i < show; i++)
            printf("    %6.2f MB  %s\n",
                   (double)t->files[i].length / (1024.0 * 1024.0),
                   t->files[i].path);
        if (t->num_files > show)
            printf("    ... and %d more files\n", t->num_files - show);
    }
    printf("\n");
}

/* ── JSON output ─────────────────────────────────────────────────────────── */

/* Minimal JSON string escape — handles the characters that actually appear
   in torrent metadata (no full Unicode needed). */
static void json_str(FILE *f, const char *s) {
    fputc('"', f);
    for (; *s; s++) {
        if      (*s == '"')  fputs("\\\"", f);
        else if (*s == '\\') fputs("\\\\", f);
        else if (*s == '\n') fputs("\\n",  f);
        else if (*s == '\r') fputs("\\r",  f);
        else if (*s == '\t') fputs("\\t",  f);
        else                 fputc(*s, f);
    }
    fputc('"', f);
}

static void print_json(const TorrentInfo *t) {
    char hash[41];
    hex_to_str(t->info_hash, 20, hash);

    printf("{\n");
    printf("  \"name\": ");        json_str(stdout, t->name);       printf(",\n");
    printf("  \"info_hash\": \"%s\",\n", hash);
    printf("  \"size\": %ld,\n",   t->total_length);
    printf("  \"piece_length\": %d,\n", t->piece_length);
    printf("  \"num_pieces\": %d,\n",   t->num_pieces);
    printf("  \"num_files\": %d,\n",    t->num_files);
    printf("  \"multi_file\": %s,\n",   t->is_multi_file ? "true" : "false");

    if (t->comment[0]) {
        printf("  \"comment\": ");  json_str(stdout, t->comment);   printf(",\n");
    }
    if (t->created_by[0]) {
        printf("  \"created_by\": "); json_str(stdout, t->created_by); printf(",\n");
    }

    /* Trackers array */
    printf("  \"trackers\": [");
    int first = 1;
    if (t->announce[0]) {
        printf("\n    "); json_str(stdout, t->announce);
        first = 0;
    }
    for (int i = 0; i < t->num_trackers; i++) {
        if (!first) printf(",");
        printf("\n    "); json_str(stdout, t->announce_list[i]);
        first = 0;
    }
    printf("\n  ],\n");

    /* Files array */
    printf("  \"files\": [\n");
    for (int i = 0; i < t->num_files; i++) {
        printf("    { \"path\": ");
        json_str(stdout, t->files[i].path);
        printf(", \"size\": %ld }", t->files[i].length);
        if (i < t->num_files - 1) printf(",");
        printf("\n");
    }
    printf("  ]\n");
    printf("}\n");
}

/* ── cmd_inspect ─────────────────────────────────────────────────────────── */

int cmd_inspect(const Config *cfg) {
    TorrentInfo *t = torrent_parse(cfg->torrent_path);
    if (!t) return EXIT_FAILURE;

    if (cfg->json_output)
        print_json(t);
    else
        print_human(t);

    torrent_free(t);
    return EXIT_SUCCESS;
}
