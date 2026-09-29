/* fuzz_torrent.c — .torrent / magnet metadata parsing and validation. */
#include "fuzz.h"
#include "core/torrent.h"
#include <string.h>

int LLVMFuzzerInitialize(int *argc, char ***argv) {
    (void)argc; (void)argv;
    fuzz_quiet_logs();
    return 0;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TorrentInfo *t = torrent_parse_buffer(data, size);
    if (!t) return 0;

    /* Anything accepted must be internally consistent. */
    long long sum = 0;
    for (int i = 0; i < t->num_pieces; i++) {
        int len = torrent_get_piece_length(t, i);
        if (len <= 0 || len > t->piece_length) __builtin_trap();
        sum += len;
        (void)torrent_get_piece_hash(t, i)[19];
    }
    if (sum != t->total_length) __builtin_trap();
    for (int i = 0; i < t->num_files; i++) {
        const char *p = t->files[i].path;
        if (p[0] == '/' || strstr(p, "../") || strcmp(p, "..") == 0) __builtin_trap();
    }
    torrent_free(t);
    return 0;
}
