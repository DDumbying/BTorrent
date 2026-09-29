/* fuzz_tracker.c — HTTP tracker replies and compact peer lists. */
#include "fuzz.h"
#include "proto/tracker.h"

int LLVMFuzzerInitialize(int *argc, char ***argv) {
    (void)argc; (void)argv;
    fuzz_quiet_logs();
    return 0;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    PeerList pl;
    if (tracker_parse_http_response(data, size, &pl) == 0) {
        for (int i = 0; i < pl.count; i++) (void)pl.peers[i].ip[0];
        peer_list_free(&pl);
    }
    pl = compact_peers(data, size);
    peer_list_free(&pl);
    pl = compact6_peers(data, size);
    peer_list_free(&pl);
    return 0;
}
