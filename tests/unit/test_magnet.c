/**
 * test_magnet.c — Unit tests for magnet link parser
 */

#include "core/magnet.h"
#include "log.h"
#include <stdio.h>
#include <string.h>
#include <stdint.h>

static int passed = 0;
static int failed = 0;

#define CHECK(desc, expr) do { \
    if (expr) { printf("  PASS  %s\n", desc); passed++; } \
    else       { printf("  FAIL  %s  (line %d)\n", desc, __LINE__); failed++; } \
} while(0)

int main(void) {
    log_init(LOG_ERROR, NULL);   /* suppress noise in test output */
    printf("=== Magnet Parser Tests ===\n");

    MagnetLink m;

    /* ── Test 1: hex info_hash ─────────────────────────────────────────── */
    const char *uri1 =
        "magnet:?xt=urn:btih:d984f67af9917b214cd8b6048ab5624c7df6a07a"
        "&dn=test_folder"
        "&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337%2Fannounce";

    CHECK("hex: parse succeeds",
          magnet_parse(uri1, &m) == 0);
    CHECK("hex: info_hash byte 0",
          m.info_hash[0] == 0xd9);
    CHECK("hex: info_hash byte 1",
          m.info_hash[1] == 0x84);
    CHECK("hex: info_hash_hex matches",
          strcmp(m.info_hash_hex, "d984f67af9917b214cd8b6048ab5624c7df6a07a") == 0);
    CHECK("hex: display name",
          strcmp(m.name, "test_folder") == 0);
    CHECK("hex: tracker count",
          m.num_trackers == 1);
    CHECK("hex: tracker URL decoded",
          strcmp(m.trackers[0], "udp://tracker.opentrackr.org:1337/announce") == 0);

    /* ── Test 2: base32 info_hash ─────────────────────────────────────── */
    /* "d984f67af9917b214cd8b6048ab5624c7df6a07a" in base32 is:
     * 3GBPM7UZSH3SCTHYX4CIKWLCJR6XUBZQ  (32 chars)  */
    const char *uri2 =
        "magnet:?xt=urn:btih:3GBPM7UZSH3SCTHYX4CIKWLCJR6XUBZQ";

    CHECK("b32: parse succeeds",
          magnet_parse(uri2, &m) == 0);
    CHECK("b32: info_hash byte 0",
          m.info_hash[0] == 0xd9);
    CHECK("b32: info_hash_hex set",
          m.info_hash_hex[0] != '\0');

    /* ── Test 3: multiple trackers ────────────────────────────────────── */
    const char *uri3 =
        "magnet:?xt=urn:btih:d984f67af9917b214cd8b6048ab5624c7df6a07a"
        "&tr=http%3A%2F%2Ftracker1.com%2Fannounce"
        "&tr=udp%3A%2F%2Ftracker2.org%3A1337";

    CHECK("multi-tr: parse succeeds",
          magnet_parse(uri3, &m) == 0);
    CHECK("multi-tr: 2 trackers",
          m.num_trackers == 2);
    CHECK("multi-tr: tracker[0] decoded",
          strcmp(m.trackers[0], "http://tracker1.com/announce") == 0);
    CHECK("multi-tr: tracker[1] decoded",
          strcmp(m.trackers[1], "udp://tracker2.org:1337") == 0);

    /* ── Test 4: no display name ──────────────────────────────────────── */
    const char *uri4 =
        "magnet:?xt=urn:btih:d984f67af9917b214cd8b6048ab5624c7df6a07a";

    CHECK("no-dn: parse succeeds",
          magnet_parse(uri4, &m) == 0);
    CHECK("no-dn: name empty",
          m.name[0] == '\0');
    CHECK("no-dn: no trackers",
          m.num_trackers == 0);

    /* ── Test 5: invalid URIs ─────────────────────────────────────────── */
    CHECK("invalid: not a magnet",
          magnet_parse("http://example.com/file.torrent", &m) == -1);
    CHECK("invalid: missing xt",
          magnet_parse("magnet:?dn=foo", &m) == -1);
    CHECK("invalid: bad hash",
          magnet_parse("magnet:?xt=urn:btih:ZZZZ", &m) == -1);

    printf("\n%d passed, %d failed\n", passed, failed);
    return failed > 0 ? 1 : 0;
}
