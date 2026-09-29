/*
 * fuzz_ext.c — BEP 10 extension handshake, BEP 11 PEX, and BEP 9
 * ut_metadata messages as received from peers.
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
    /* What the scheduler parses from peers' BEP 10 / BEP 11 messages. */
    int pex_id = ext_parse_pex_id(data, size);
    if (pex_id != -1 && (pex_id < 1 || pex_id > 255)) __builtin_trap();
    Peer peers[50];
    int n = pex_parse_added(data, size, peers, 50);
    if (n < 0 || n > 50) __builtin_trap();
    for (int i = 0; i < n; i++)
        if (peers[i].port == 0 || peers[i].is_ipv6) __builtin_trap();

    ExtHandshake eh;
    (void)parse_ext_handshake(data, (uint32_t)size, &eh);

    MetaMsg mm;
    if (parse_meta_msg(data, (uint32_t)size, &mm) == 0 && mm.block) {
        if (mm.block < data || mm.block + mm.block_len > data + size) __builtin_trap();
    }
    return 0;
}
