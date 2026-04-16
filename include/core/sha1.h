#pragma once
/**
 * sha1.h — SHA-1 Hash Implementation
 *
 * Implements the SHA-1 (Secure Hash Algorithm 1) as defined in RFC 3174.
 * Produces a 160-bit (20-byte) hash from any input.
 *
 * BitTorrent uses SHA-1 for:
 *   1. The info_hash (torrent's unique identifier)
 *   2. Piece verification (each piece has a stored SHA-1 in the torrent file)
 *
 * See docs/03_sha1.md for a detailed explanation of the algorithm.
 *
 * Usage (one-shot):
 *   uint8_t hash[20];
 *   sha1(data, len, hash);
 *
 * Usage (streaming):
 *   SHA1_CTX ctx;
 *   sha1_init(&ctx);
 *   sha1_update(&ctx, chunk1, len1);
 *   sha1_update(&ctx, chunk2, len2);
 *   sha1_final(&ctx, hash);
 */


#include <stdint.h>
#include <stddef.h>

#define SHA1_DIGEST_SIZE 20    /* SHA-1 output is 20 bytes = 160 bits */
#define SHA1_BLOCK_SIZE  64    /* SHA-1 processes data in 64-byte blocks */

/* ── Context structure ───────────────────────────────────────────────────── */

/**
 * SHA1_CTX holds the running state of a SHA-1 computation.
 *
 * h[0..4]   - The 5 state words (each 32 bits), initialized to magic constants
 * count     - Number of bits processed so far (used for final padding)
 * buffer    - Partial block buffer (accumulates bytes until we have 64)
 */
typedef struct {
    uint32_t h[5];              /* 5 × 32-bit state words = 160 bits */
    uint64_t count;             /* total bit count (for length padding) */
    uint8_t  buffer[SHA1_BLOCK_SIZE]; /* partial block buffer */
    uint32_t buf_used;          /* bytes currently in buffer */
} SHA1_CTX;

/* ── API ─────────────────────────────────────────────────────────────────── */

/**
 * sha1_init - initialize a SHA-1 context.
 * Must be called before sha1_update().
 *
 * Sets h[] to the magic initialization constants from RFC 3174:
 *   h[0] = 0x67452301
 *   h[1] = 0xEFCDAB89
 *   h[2] = 0x98BADCFE
 *   h[3] = 0x10325476
 *   h[4] = 0xC3D2E1F0
 */
void sha1_init(SHA1_CTX *ctx);

/**
 * sha1_update - feed bytes into the SHA-1 computation.
 * Can be called multiple times for streaming input.
 *
 * @ctx   the SHA-1 context (must be initialized with sha1_init)
 * @data  bytes to hash
 * @len   number of bytes
 */
void sha1_update(SHA1_CTX *ctx, const uint8_t *data, size_t len);

/**
 * sha1_final - finalize the hash and write the 20-byte digest.
 *
 * Applies padding (appends 0x80, zeros, then the message length),
 * processes the final block(s), and writes the result.
 *
 * @ctx     the SHA-1 context
 * @digest  output buffer — must be at least SHA1_DIGEST_SIZE (20) bytes
 */
void sha1_final(SHA1_CTX *ctx, uint8_t *digest);

/**
 * sha1 - convenience function: hash a complete buffer at once.
 *
 * @data    input bytes
 * @len     number of bytes
 * @digest  output: 20-byte SHA-1 hash
 */
void sha1(const uint8_t *data, size_t len, uint8_t *digest);

/* ── Internal: process one 64-byte block ────────────────────────────────── */

/**
 * sha1_process_block - the core SHA-1 compression function.
 * Runs 80 rounds on a single 64-byte block and updates ctx->h[].
 *
 * You don't call this directly — sha1_update() calls it internally.
 */
void sha1_process_block(SHA1_CTX *ctx, const uint8_t *block);


