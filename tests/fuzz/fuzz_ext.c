/*
 * fuzz_ext.c — BEP 10 extension handshake and BEP 9 ut_metadata messages
 * as received from peers during a magnet metadata fetch.
 * Includes ext.c to reach its static parsers.
 */
#include "../../src/proto/ext.c"   /* first: it sets feature macros */
#include "fuzz.h"

int LLVMFuzzerInitialize(int *argc, char ***argv) {
    (void)argc; (void)argv;
    fuzz_quiet_logs();
    return 0;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    ExtHandshake eh;
    (void)parse_ext_handshake(data, (uint32_t)size, &eh);

    MetaMsg mm;
    if (parse_meta_msg(data, (uint32_t)size, &mm) == 0 && mm.block) {
        if (mm.block < data || mm.block + mm.block_len > data + size) __builtin_trap();
    }
    return 0;
}
