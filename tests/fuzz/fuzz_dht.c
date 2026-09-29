/*
 * fuzz_dht.c — DHT (KRPC) reply handling.
 *
 * Includes dht.c to reach its static reply handler. Input layout:
 *   [2 bytes: transaction id of our pending query][KRPC datagram]
 * The fake node is registered as queried with that id, so fuzzed replies
 * get past the anti-spoofing check and into the peer/node parsing.
 */
#include "../../src/dht/dht.c"   /* first: it sets feature macros */
#include "fuzz.h"

int LLVMFuzzerInitialize(int *argc, char ***argv) {
    (void)argc; (void)argv;
    fuzz_quiet_logs();
    return 0;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size < 2) return 0;
    DhtCtx *ctx = calloc(1, sizeof(*ctx));
    ctx->sock = -1;
    add_node(ctx, NULL, "10.0.0.1", 6881);
    ctx->nodes[0].queried = 1;
    memcpy(ctx->nodes[0].tid, data, 2);

    Peer found[64];
    int  num_found = 0;
    process_reply(ctx, data + 2, size - 2, &ctx->nodes[0].addr,
                  found, 64, &num_found);
    if (num_found < 0 || num_found > 64) __builtin_trap();
    if (ctx->num_nodes > DHT_MAX_NODES) __builtin_trap();
    dht_free(ctx);
    return 0;
}
