/**
 * test_tracker_v6.c — IPv6 peer parsing and UDP connection ID caching tests
 *
 * Build:
 *   gcc -Iinclude -std=c11 tests/unit/test_tracker_v6.c \
 *       src/proto/tracker.c src/utils.c src/log.c src/result.c \
 *       -o build/test_tracker_v6
 */

#include "proto/tracker.h"
#include "utils.h"
#include "log.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static int passed = 0, failed = 0;

#define ASSERT(cond, label) \
    do { if (cond) { printf("  PASS  %s\n", label); passed++; } \
         else { printf("  FAIL  %s  (line %d)\n", label, __LINE__); failed++; } } while(0)

/* ── IPv6 Peer Parsing Tests ─────────────────────────────────────────────── */

static void test_compact_ipv4_peers(void) {
    uint8_t data[] = {
        127, 0, 0, 1, 0x1B, 0xB5,  /* 127.0.0.1:7093 */
        192, 168, 1, 1, 0x22, 0xB8,  /* 192.168.1.1:8888 */
    };
    PeerList pl;
    pl.peers = NULL;
    pl.count = 0;

    pl = compact_peers(data, sizeof(data));
    ASSERT(pl.count == 2, "ipv4: count correct");
    ASSERT(strcmp(pl.peers[0].ip, "127.0.0.1") == 0, "ipv4: peer[0] IP");
    ASSERT(pl.peers[0].port == 0x1BB5, "ipv4: peer[0] port");
    ASSERT(pl.peers[0].is_ipv6 == 0, "ipv4: peer[0] is_ipv6=0");
    ASSERT(strcmp(pl.peers[1].ip, "192.168.1.1") == 0, "ipv4: peer[1] IP");
    ASSERT(pl.peers[1].port == 0x22B8, "ipv4: peer[1] port");
    ASSERT(pl.peers[1].is_ipv6 == 0, "ipv4: peer[1] is_ipv6=0");
    free(pl.peers);
}

static void test_compact_ipv6_peers(void) {
    uint8_t data[36] = {0};
    data[0]  = 0x20; data[1]  = 0x01; data[2]  = 0x0d; data[3]  = 0xb8;
    data[4]  = 0x00; data[5]  = 0x00; data[6]  = 0x00; data[7]  = 0x00;
    data[8]  = 0x00; data[9]  = 0x00; data[10] = 0x00; data[11] = 0x00;
    data[12] = 0x00; data[13] = 0x00; data[14] = 0x00; data[15] = 0x01;
    data[16] = 0x1B; data[17] = 0xB5;

    PeerList pl;
    pl.peers = NULL;
    pl.count = 0;

    pl = compact6_peers(data, sizeof(data));
    ASSERT(pl.count == 2, "ipv6: count correct");
    ASSERT(pl.peers[0].is_ipv6 == 1, "ipv6: peer[0] is_ipv6=1");
    ASSERT(pl.peers[0].port == 0x1BB5, "ipv6: peer[0] port");
    ASSERT(pl.peers[1].is_ipv6 == 1, "ipv6: peer[1] is_ipv6=1");
    free(pl.peers);
}

/* Regression: an 18-byte compact "peers" string is three IPv4 peers, not
 * one IPv6 peer. Length-based auto-detection misparsed any peer count that
 * was a multiple of 3; IPv6 peers only ever arrive in "peers6". */
static void test_compact_18_bytes_is_three_ipv4_peers(void) {
    uint8_t data[18] = {
        10, 0, 0, 1, 0x1A, 0xE1,
        10, 0, 0, 2, 0x1A, 0xE1,
        10, 0, 0, 3, 0x1A, 0xE1,
    };
    PeerList pl = compact_peers(data, sizeof(data));
    ASSERT(pl.count == 3, "compact: 18 bytes → 3 IPv4 peers");
    ASSERT(pl.count == 3 && strcmp(pl.peers[2].ip, "10.0.0.3") == 0,
           "compact: third peer IP correct");
    ASSERT(pl.count == 3 && pl.peers[0].is_ipv6 == 0, "compact: marked IPv4");
    free(pl.peers);
}

/* ── UDP Cache Tests ──────────────────────────────────────────────────────── */

static void test_udp_cache_init(void) {
    UdpConnCache cache = {0};
    udp_cache_init(&cache);
    ASSERT(cache.host[0] == '\0', "cache init: host empty");
}

static void test_udp_cache_miss_empty(void) {
    UdpConnCache cache = {0};
    udp_cache_init(&cache);
    uint64_t id = udp_cache_get(&cache, "tracker.example.com", 1000);
    ASSERT(id == 0, "cache miss: empty cache returns 0");
}

static void test_udp_cache_set_and_get(void) {
    UdpConnCache cache = {0};
    udp_cache_init(&cache);

    udp_cache_set(&cache, "tracker.example.com", 0x1234567890ABCDEFULL, 2000);

    uint64_t id = udp_cache_get(&cache, "tracker.example.com", 1500);
    ASSERT(id == 0x1234567890ABCDEFULL, "cache hit: returns stored conn_id");

    int not_found = udp_cache_get(&cache, "other.tracker.com", 1500);
    ASSERT(not_found == 0, "cache miss: different host returns 0");
}

static void test_udp_cache_expired(void) {
    UdpConnCache cache = {0};
    udp_cache_init(&cache);

    udp_cache_set(&cache, "tracker.example.com", 0xDEADBEEFULL, 1000);

    uint64_t id = udp_cache_get(&cache, "tracker.example.com", 999);
    ASSERT(id == 0xDEADBEEFULL, "cache hit: before expiry");

    uint64_t expired = udp_cache_get(&cache, "tracker.example.com", 1000);
    ASSERT(expired == 0, "cache miss: at expiry time");
}

static void test_udp_cache_overwrite(void) {
    UdpConnCache cache = {0};
    udp_cache_init(&cache);

    udp_cache_set(&cache, "tracker.example.com", 0x11111111, 2000);
    uint64_t id1 = udp_cache_get(&cache, "tracker.example.com", 1500);

    udp_cache_set(&cache, "tracker.example.com", 0x22222222, 3000);
    uint64_t id2 = udp_cache_get(&cache, "tracker.example.com", 2500);

    ASSERT(id1 == 0x11111111, "cache overwrite: first id correct");
    ASSERT(id2 == 0x22222222, "cache overwrite: second id correct");
}

/* ── Main ────────────────────────────────────────────────────────────────── */

int main(void) {
    log_init(LOG_ERROR, NULL);
    printf("=== IPv6 Peer Parsing + UDP Cache Tests ===\n\n");

    printf("--- IPv6 peer parsing ---\n");
    test_compact_ipv4_peers();
    test_compact_ipv6_peers();
    test_compact_18_bytes_is_three_ipv4_peers();

    printf("\n--- UDP connection ID cache ---\n");
    test_udp_cache_init();
    test_udp_cache_miss_empty();
    test_udp_cache_set_and_get();
    test_udp_cache_expired();
    test_udp_cache_overwrite();

    printf("\n%d passed, %d failed\n", passed, failed);
    return failed ? EXIT_FAILURE : EXIT_SUCCESS;
}
