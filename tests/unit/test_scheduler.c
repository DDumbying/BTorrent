#define _POSIX_C_SOURCE 200809L
/**
 * test_scheduler.c — Unit tests for scheduler.c pure logic
 *
 * Covers the offline-testable parts added in Phase 8:
 *   1. Token bucket — init, consume, refill, unlimited mode
 *   2. PEX body builder — valid bencode, compact peer format
 *   3. PEX peer parser — extracts peers from "5:added..." payload
 *   4. Ext handshake builder — advertises both ut_metadata and ut_pex
 *   5. Peer ext-handshake parser — reads ut_pex ext id from peer's hs
 *
 * Network-dependent code (epoll loop, listen socket, accept, seeding)
 * is tested by integration rather than unit tests.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <time.h>
#include <arpa/inet.h>

#include "core/bencode.h"
#include "proto/ext_handshake.h"
#include "proto/tracker.h"   /* Peer, PeerList */
#include "utils.h"
#include "log.h"

/* ── Minimal test framework ───────────────────────────────────────────────── */

static int g_pass = 0, g_fail = 0;
#define EXPECT(cond, name) \
    do { if (cond) { printf("  PASS  %s\n", name); g_pass++; } \
         else      { printf("  FAIL  %s  (line %d)\n", name, __LINE__); g_fail++; } \
    } while (0)

/* ── Replicated token-bucket from scheduler.c (static there) ─────────────── */

typedef struct {
    long long tokens;
    long long capacity;
    long long rate_per_ms;
    long long last_refill_ms;
} TokenBucket;

static long long now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
}

static void tb_init(TokenBucket *tb, int kbs) {
    if (kbs <= 0) {
        tb->rate_per_ms = 0; tb->tokens = tb->capacity = 0;
    } else {
        tb->rate_per_ms = (long long)kbs * 1024 / 1000;
        if (tb->rate_per_ms < 1) tb->rate_per_ms = 1;
        tb->capacity = tb->rate_per_ms * 2000;
        tb->tokens   = tb->capacity;
    }
    tb->last_refill_ms = now_ms();
}

static void tb_refill(TokenBucket *tb) {
    if (tb->rate_per_ms == 0) return;
    long long now  = now_ms();
    long long diff = now - tb->last_refill_ms;
    if (diff <= 0) return;
    tb->tokens += diff * tb->rate_per_ms;
    if (tb->tokens > tb->capacity) tb->tokens = tb->capacity;
    tb->last_refill_ms = now;
}

static long long tb_consume(TokenBucket *tb, long long want) {
    if (tb->rate_per_ms == 0) return want;
    tb_refill(tb);
    if (tb->tokens <= 0) return 0;
    long long allowed = tb->tokens < want ? tb->tokens : want;
    tb->tokens -= allowed;
    return allowed;
}

/* ── Replicated ext/PEX helpers from scheduler.c ────────────────────────── */

#define META_LOCAL_ID 1
#define PEX_LOCAL_ID  2

/* Thin wrapper so the tests exercise the real builder, not a copy. */
static int build_ext_handshake(uint8_t *buf, size_t cap) {
    return ext_build_handshake(buf, cap, META_LOCAL_ID, PEX_LOCAL_ID, 0, 0);
}

/* Build a PEX "added" payload from an explicit peer list. */
static int build_pex_from_list(uint8_t *buf, size_t cap,
                                const char **ips, const uint16_t *ports,
                                int count) {
    if (count == 0) return 0;
    uint8_t compact[50 * 6];
    int actual = count < 50 ? count : 50;
    for (int i = 0; i < actual; i++) {
        struct in_addr addr;
        if (inet_pton(AF_INET, ips[i], &addr) != 1) return -1;
        memcpy(compact + i * 6, &addr.s_addr, 4);
        compact[i * 6 + 4] = (uint8_t)(ports[i] >> 8);
        compact[i * 6 + 5] = (uint8_t)(ports[i] & 0xFF);
    }
    int n = snprintf((char *)buf, cap, "d5:added%d:", actual * 6);
    if (n < 0 || (size_t)(n + actual * 6 + 1) >= cap) return -1;
    memcpy(buf + n, compact, (size_t)(actual * 6));
    n += actual * 6;
    buf[n++] = 'e';
    return n;
}

/* Parse ut_pex peer's ut_pex ext id from their extension handshake. */
/* Thin wrappers so the tests exercise the real parsers, not copies. */
static void parse_peer_ext_hs(const uint8_t *data, uint32_t len, int *out_pex_id) {
    *out_pex_id = ext_parse_pex_id(data, len);
}

static int parse_pex_peers(const uint8_t *payload, uint32_t plen,
                            Peer *out, int max_out) {
    return pex_parse_added(payload, plen, out, max_out);
}

/* ── Tests ────────────────────────────────────────────────────────────────── */

static void test_token_bucket(void) {
    printf("\n--- token bucket ---\n");

    /* Unlimited mode (kbs=0) */
    TokenBucket unlimited;
    tb_init(&unlimited, 0);
    EXPECT(unlimited.rate_per_ms == 0, "unlimited: rate_per_ms == 0");
    long long got = tb_consume(&unlimited, 1000000LL);
    EXPECT(got == 1000000LL, "unlimited: consume returns full request");

    /* 100 KiB/s bucket */
    TokenBucket tb;
    tb_init(&tb, 100);
    long long expected_rate = 100LL * 1024 / 1000;  /* bytes per ms */
    EXPECT(tb.rate_per_ms == expected_rate, "100kbs: correct rate_per_ms");
    EXPECT(tb.capacity == expected_rate * 2000, "100kbs: 2-second burst capacity");
    EXPECT(tb.tokens == tb.capacity, "100kbs: starts full");

    /* Consume less than available */
    long long before = tb.tokens;
    long long allowed = tb_consume(&tb, 1024);
    EXPECT(allowed == 1024, "100kbs: consume 1024 bytes allowed in full");
    EXPECT(tb.tokens == before - 1024, "100kbs: tokens decremented correctly");

    /* Drain completely */
    tb.tokens = 500;
    long long a2 = tb_consume(&tb, 10000);
    EXPECT(a2 == 500, "100kbs: consume capped at available tokens");
    EXPECT(tb.tokens == 0, "100kbs: tokens reach zero");

    /* Empty bucket returns 0 */
    tb.tokens = 0;
    long long a3 = tb_consume(&tb, 1);
    EXPECT(a3 == 0, "100kbs: empty bucket returns 0");

    /* Refill: wait 10 ms worth of tokens (simulated by back-dating last_refill) */
    tb.last_refill_ms -= 10;
    tb_refill(&tb);
    EXPECT(tb.tokens >= expected_rate * 10, "100kbs: refill adds correct tokens");
    EXPECT(tb.tokens <= tb.capacity, "100kbs: refill capped at capacity");

    /* 1 KiB/s bucket — minimum rate */
    TokenBucket tiny;
    tb_init(&tiny, 1);
    EXPECT(tiny.rate_per_ms >= 1, "1kbs: rate_per_ms at least 1");
}

static void test_ext_handshake_builder(void) {
    printf("\n--- scheduler ext handshake builder ---\n");

    uint8_t buf[256];
    int len = build_ext_handshake(buf, sizeof(buf));
    EXPECT(len > 0, "builder: positive length");

    BencodeNode *root = bencode_parse(buf, (size_t)len);
    EXPECT(root != NULL, "builder: valid bencode");
    EXPECT(root && root->type == BENCODE_DICT, "builder: root is dict");

    if (root) {
        BencodeNode *m = bencode_dict_get(root, "m");
        EXPECT(m && m->type == BENCODE_DICT, "builder: has 'm' dict");
        if (m) {
            BencodeNode *meta = bencode_dict_get(m, "ut_metadata");
            BencodeNode *pex  = bencode_dict_get(m, "ut_pex");
            EXPECT(meta && meta->type == BENCODE_INT, "builder: ut_metadata present");
            EXPECT(meta && meta->integer == META_LOCAL_ID, "builder: ut_metadata id correct");
            EXPECT(pex  && pex->type  == BENCODE_INT, "builder: ut_pex present");
            EXPECT(pex  && pex->integer == PEX_LOCAL_ID, "builder: ut_pex id correct");
        }
        BencodeNode *v = bencode_dict_get(root, "v");
        EXPECT(v && v->type == BENCODE_STR, "builder: version key present");
        bencode_free(root);
    }

    /* Truncated buffer must fail gracefully */
    EXPECT(build_ext_handshake(buf, 5) < 0, "builder: truncated buffer fails");
}

static void test_peer_ext_hs_parser(void) {
    printf("\n--- peer ext handshake parser (ut_pex id) ---\n");

    /* Peer advertising ut_pex=4 */
    const char *hs = "d1:md6:ut_pexi4e11:ut_metadatai1ee13:metadata_sizei65536ee";
    int pex_id = -1;
    parse_peer_ext_hs((const uint8_t *)hs, (uint32_t)strlen(hs), &pex_id);
    EXPECT(pex_id == 4, "parser: ut_pex id == 4");

    /* Peer with no ut_pex */
    const char *no_pex = "d1:md11:ut_metadatai1eee";
    int pex_id2 = -1;
    parse_peer_ext_hs((const uint8_t *)no_pex, (uint32_t)strlen(no_pex), &pex_id2);
    EXPECT(pex_id2 == -1, "parser: no ut_pex stays -1");

    /* Garbage input */
    int pex_id3 = -1;
    parse_peer_ext_hs((const uint8_t *)"garbage", 7, &pex_id3);
    EXPECT(pex_id3 == -1, "parser: garbage stays -1");

    /* ut_pex id > 9 (multi-digit) */
    const char *big = "d1:md6:ut_pexi12eee";
    int pex_id4 = -1;
    parse_peer_ext_hs((const uint8_t *)big, (uint32_t)strlen(big), &pex_id4);
    EXPECT(pex_id4 == 12, "parser: multi-digit ut_pex id == 12");

    /* Regression (found by fuzzing): a long digit run overflowed int. */
    const char *huge = "d1:md6:ut_pexi21244444444444444444444444eee";
    int pex_id5 = 0;
    parse_peer_ext_hs((const uint8_t *)huge, (uint32_t)strlen(huge), &pex_id5);
    EXPECT(pex_id5 == -1, "parser: out-of-range ut_pex id rejected");

    /* 0 means "disabled" (BEP 10); ids must fit the 1-byte message id. */
    int pex_id6 = 0, pex_id7 = 0;
    parse_peer_ext_hs((const uint8_t *)"d1:md6:ut_pexi0eee", 18, &pex_id6);
    parse_peer_ext_hs((const uint8_t *)"d1:md6:ut_pexi256eee", 20, &pex_id7);
    EXPECT(pex_id6 == -1 && pex_id7 == -1, "parser: ut_pex 0 and 256 rejected");

    /* The old substring scan also matched inside unrelated values. */
    const char *decoy = "d1:v12:6:ut_pexi7ee";
    int pex_id8 = 0;
    parse_peer_ext_hs((const uint8_t *)decoy, (uint32_t)strlen(decoy), &pex_id8);
    EXPECT(pex_id8 == -1, "parser: ut_pex text inside a value ignored");
}

static void test_pex_roundtrip(void) {
    printf("\n--- PEX build + parse roundtrip ---\n");

    const char *ips[]   = { "1.2.3.4", "10.20.30.40", "192.168.1.100" };
    const uint16_t ports[] = { 6881, 51413, 12345 };
    int count = 3;

    uint8_t buf[256];
    int len = build_pex_from_list(buf, sizeof(buf), ips, ports, count);
    EXPECT(len > 0, "pex build: positive length");

    /* Parse it back */
    Peer out[10];
    int found = parse_pex_peers(buf, (uint32_t)len, out, 10);
    EXPECT(found == count, "pex parse: correct peer count");

    if (found == count) {
        EXPECT(strcmp(out[0].ip, "1.2.3.4")        == 0, "peer[0] IP correct");
        EXPECT(out[0].port == 6881,                       "peer[0] port correct");
        EXPECT(strcmp(out[1].ip, "10.20.30.40")    == 0, "peer[1] IP correct");
        EXPECT(out[1].port == 51413,                      "peer[1] port correct");
        EXPECT(strcmp(out[2].ip, "192.168.1.100")  == 0, "peer[2] IP correct");
        EXPECT(out[2].port == 12345,                      "peer[2] port correct");
    }

    /* Empty list */
    int len0 = build_pex_from_list(buf, sizeof(buf), ips, ports, 0);
    EXPECT(len0 == 0, "pex build: empty list returns 0");

    /* Parse payload with no 'added' key */
    const char *no_added = "d7:deletedlee";
    Peer out2[10];
    int f2 = parse_pex_peers((const uint8_t *)no_added,
                              (uint32_t)strlen(no_added), out2, 10);
    EXPECT(f2 == 0, "pex parse: missing 'added' returns 0 peers");

    /* Truncated compact data */
    const char *truncated = "d5:added3:abce";  /* 3 bytes, need 6 per peer */
    Peer out3[10];
    int f3 = parse_pex_peers((const uint8_t *)truncated,
                              (uint32_t)strlen(truncated), out3, 10);
    EXPECT(f3 == 0, "pex parse: truncated compact (< 6 bytes) yields 0 peers");

    /* Truncated buffer forces build failure */
    int small_len = build_pex_from_list(buf, 10, ips, ports, count);
    EXPECT(small_len < 0, "pex build: small buffer returns error");
}

static void test_pex_port_encoding(void) {
    printf("\n--- PEX port big-endian encoding ---\n");

    /* Port 0x1234 = 4660 — verify byte order in compact format */
    const char *ip = "127.0.0.1";
    uint16_t port  = 0x1234;
    uint8_t buf[64];
    int len = build_pex_from_list(buf, sizeof(buf), &ip, &port, 1);
    EXPECT(len > 0, "port encoding: build succeeds");

    Peer out[4];
    int found = parse_pex_peers(buf, (uint32_t)len, out, 4);
    EXPECT(found == 1, "port encoding: 1 peer parsed");
    EXPECT(found == 1 && out[0].port == 0x1234, "port encoding: big-endian correct");

    /* Port 0x6881 (6881) */
    uint16_t port2 = 6881;
    len = build_pex_from_list(buf, sizeof(buf), &ip, &port2, 1);
    found = parse_pex_peers(buf, (uint32_t)len, out, 4);
    EXPECT(found == 1 && out[0].port == 6881, "port encoding: port 6881 correct");
}

/* ── main ─────────────────────────────────────────────────────────────────── */

int main(void) {
    printf("=== Scheduler / Seeding / PEX / Rate-Limit Tests ===\n");

    test_token_bucket();
    test_ext_handshake_builder();
    test_peer_ext_hs_parser();
    test_pex_roundtrip();
    test_pex_port_encoding();

    printf("\n%d passed, %d failed\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
