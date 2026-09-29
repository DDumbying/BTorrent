/* fuzz_magnet.c — magnet URI parsing. */
#include "fuzz.h"
#include "core/magnet.h"
#include <stdlib.h>
#include <string.h>

int LLVMFuzzerInitialize(int *argc, char ***argv) {
    (void)argc; (void)argv;
    fuzz_quiet_logs();
    return 0;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    char *uri = malloc(size + 1);
    memcpy(uri, data, size);
    uri[size] = '\0';
    MagnetLink m;
    if (magnet_parse(uri, &m) == 0) {
        if (strlen(m.info_hash_hex) != 40) __builtin_trap();
        if (m.num_trackers < 0 || m.num_trackers > MAGNET_MAX_TRACKERS) __builtin_trap();
    }
    free(uri);
    return 0;
}
