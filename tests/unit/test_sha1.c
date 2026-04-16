/**
 * test_sha1.c — SHA-1 unit tests using RFC 3174 test vectors
 *
 * Build:
 *   gcc -Iinclude -std=c11 tests/unit/test_sha1.c src/core/sha1.c -o build/test_sha1
 *   ./build/test_sha1
 */

#include "core/sha1.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static int passed = 0, failed = 0;

static void check(const char *label, const char *input, size_t len,
                  const char *expected_hex) {
    uint8_t digest[20];
    sha1((const uint8_t *)input, len, digest);

    char got[41];
    for (int i = 0; i < 20; i++)
        sprintf(got + i*2, "%02x", digest[i]);
    got[40] = '\0';

    if (strcmp(got, expected_hex) == 0) {
        printf("  PASS  %s\n", label);
        passed++;
    } else {
        printf("  FAIL  %s\n         got:      %s\n         expected: %s\n",
               label, got, expected_hex);
        failed++;
    }
}

int main(void) {
    printf("=== SHA-1 Tests ===\n");

    /* RFC 3174 test vectors */
    check("empty string",
          "", 0,
          "da39a3ee5e6b4b0d3255bfef95601890afd80709");

    check("\"abc\"",
          "abc", 3,
          "a9993e364706816aba3e25717850c26c9cd0d89d");

    check("\"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq\"",
          "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", 56,
          "84983e441c3bd26ebaae4aa1f95129e5e54670f1");

    /* Boundary: exactly 55 bytes → padding fits in one block */
    check("55-byte input (single-block padding boundary)",
          "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 55,
          "c1c8bbdc22796e28c0e15163d20899b65621d65a");

    /* Boundary: exactly 64 bytes → padding spills into a second block */
    check("64-byte input (two-block padding boundary)",
          "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 64,
          "0098ba824b5c16427bd7a1122a5a442a25ec644d");

    /* Streaming API: same result when fed in chunks */
    {
        SHA1_CTX ctx;
        sha1_init(&ctx);
        sha1_update(&ctx, (const uint8_t *)"ab", 2);
        sha1_update(&ctx, (const uint8_t *)"c",  1);
        uint8_t digest[20]; char got[41];
        sha1_final(&ctx, digest);
        for (int i = 0; i < 20; i++) sprintf(got + i*2, "%02x", digest[i]);
        got[40] = '\0';
        const char *want = "a9993e364706816aba3e25717850c26c9cd0d89d";
        if (strcmp(got, want) == 0) { printf("  PASS  streaming API (abc in 2 chunks)\n"); passed++; }
        else { printf("  FAIL  streaming API\n"); failed++; }
    }

    printf("\n%d passed, %d failed\n", passed, failed);
    return failed ? EXIT_FAILURE : EXIT_SUCCESS;
}
