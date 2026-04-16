/**
 * sha1.c — SHA-1 Hash Implementation (RFC 3174)
 *
 * A clean, educational implementation of SHA-1 from scratch.
 * Every step is commented to explain what's happening and why.
 *
 * See docs/03_sha1.md for an explanation of the algorithm.
 */

#include "core/sha1.h"
#include <string.h>

/* ── Left rotate macro ───────────────────────────────────────────────────── */

/*
 * Left rotation of a 32-bit value by n bits.
 * Used extensively in the SHA-1 compression function.
 *
 * Example: ROTL(0b10110000, 2) = 0b11000010
 *
 * We use a macro (not inline function) for guaranteed inlining.
 */
#define ROTL(x, n) (((x) << (n)) | ((x) >> (32 - (n))))

/* ── SHA-1 round constants ───────────────────────────────────────────────── */

/*
 * These constants are defined in RFC 3174.
 * They are the integer parts of the square roots of 2, 3, 5, and 10,
 * scaled to fit in 32 bits. "Nothing up my sleeve" numbers.
 *
 * K[0] = floor(2^30 * sqrt(2))  = 0x5A827999
 * K[1] = floor(2^30 * sqrt(3))  = 0x6ED9EBA1
 * K[2] = floor(2^30 * sqrt(5))  = 0x8F1BBCDC
 * K[3] = floor(2^30 * sqrt(10)) = 0xCA62C1D6
 */
static const uint32_t K[4] = {
    0x5A827999,  /* rounds  0-19 */
    0x6ED9EBA1,  /* rounds 20-39 */
    0x8F1BBCDC,  /* rounds 40-59 */
    0xCA62C1D6,  /* rounds 60-79 */
};

/* ── Round functions ─────────────────────────────────────────────────────── */

/*
 * SHA-1 uses 4 different boolean functions across 80 rounds.
 * Each operates on 32-bit words b, c, d (3 of the 5 state variables).
 *
 * Ch  (rounds  0-19): "choose" — select bit from c or d based on b
 * Par (rounds 20-39): "parity" — XOR mixing
 * Maj (rounds 40-59): "majority" — 2-of-3 majority vote
 * Par (rounds 60-79): same parity function again
 */
#define Ch(b,c,d)  (((b) & (c)) | (~(b) & (d)))
#define Par(b,c,d) ((b) ^ (c) ^ (d))
#define Maj(b,c,d) (((b) & (c)) | ((b) & (d)) | ((c) & (d)))

/* ── sha1_init ───────────────────────────────────────────────────────────── */

void sha1_init(SHA1_CTX *ctx) {
    /*
     * Initialize the 5 state words to their magic initial values.
     * These are specified in RFC 3174 Section 6.1.
     *
     * These are the fractional parts of the square roots of the first
     * five primes (2, 3, 5, 7, 11) — another "nothing up my sleeve" design.
     */
    ctx->h[0] = 0x67452301;
    ctx->h[1] = 0xEFCDAB89;
    ctx->h[2] = 0x98BADCFE;
    ctx->h[3] = 0x10325476;
    ctx->h[4] = 0xC3D2E1F0;

    ctx->count    = 0;
    ctx->buf_used = 0;
    memset(ctx->buffer, 0, sizeof(ctx->buffer));
}

/* ── sha1_process_block — the core compression function ─────────────────── */

void sha1_process_block(SHA1_CTX *ctx, const uint8_t *block) {
    /*
     * This is the heart of SHA-1: the compression function.
     * It takes one 512-bit (64-byte) block and mixes it into the state.
     *
     * Steps:
     *   1. Expand 16 input words W[0..15] to 80 words W[0..79]
     *   2. Initialize working variables a,b,c,d,e from current state
     *   3. Run 80 rounds of mixing
     *   4. Add the mixed values back to the state
     */

    uint32_t W[80];

    /* Step 1: Load the 16 input words from the block (big-endian!) */
    for (int i = 0; i < 16; i++) {
        /*
         * Each word is 4 bytes in big-endian order.
         * We must read byte-by-byte on x86 (little-endian architecture).
         */
        W[i] = ((uint32_t)block[i*4    ] << 24) |
               ((uint32_t)block[i*4 + 1] << 16) |
               ((uint32_t)block[i*4 + 2] <<  8) |
               ((uint32_t)block[i*4 + 3]);
    }

    /* Step 2: Expand W[16..79] using the message schedule.
     *
     * Each new word is a rotation of the XOR of 4 previous words.
     * This "spreads" changes throughout the schedule, making each block
     * affect all 80 rounds.
     */
    for (int i = 16; i < 80; i++) {
        W[i] = ROTL(W[i-3] ^ W[i-8] ^ W[i-14] ^ W[i-16], 1);
    }

    /* Step 3: Initialize working variables from the current hash state */
    uint32_t a = ctx->h[0];
    uint32_t b = ctx->h[1];
    uint32_t c = ctx->h[2];
    uint32_t d = ctx->h[3];
    uint32_t e = ctx->h[4];

    /* Step 4: 80 rounds of mixing in 4 groups of 20.
     *
     * Each round:
     *   temp = ROTL(a, 5) + f(b,c,d) + e + K + W[i]
     *   e = d
     *   d = c
     *   c = ROTL(b, 30)
     *   b = a
     *   a = temp
     *
     * The state "rotates" each round: a flows into b, b into c, etc.
     * The nonlinear function f and the message word W[i] inject data.
     */
    for (int i = 0; i < 80; i++) {
        uint32_t f, k;

        if      (i < 20) { f = Ch(b, c, d);  k = K[0]; }
        else if (i < 40) { f = Par(b, c, d); k = K[1]; }
        else if (i < 60) { f = Maj(b, c, d); k = K[2]; }
        else             { f = Par(b, c, d); k = K[3]; }

        uint32_t temp = ROTL(a, 5) + f + e + k + W[i];
        e = d;
        d = c;
        c = ROTL(b, 30);
        b = a;
        a = temp;
    }

    /* Step 5: Add the working variables back to the hash state.
     * This is the "Davies-Meyer" construction — each block's output
     * feeds into the next block's initial state.
     */
    ctx->h[0] += a;
    ctx->h[1] += b;
    ctx->h[2] += c;
    ctx->h[3] += d;
    ctx->h[4] += e;
}

/* ── sha1_update ─────────────────────────────────────────────────────────── */

void sha1_update(SHA1_CTX *ctx, const uint8_t *data, size_t len) {
    /*
     * Feed bytes into the SHA-1 computation.
     *
     * We buffer data until we have a full 64-byte block, then process it.
     * This handles arbitrary chunk sizes correctly.
     */

    /* Track total bits for padding */
    ctx->count += (uint64_t)len * 8;

    size_t i = 0;

    /* If there's leftover data in the buffer, fill it up first */
    if (ctx->buf_used > 0) {
        size_t space = SHA1_BLOCK_SIZE - ctx->buf_used;
        size_t take  = len < space ? len : space;
        memcpy(ctx->buffer + ctx->buf_used, data, take);
        ctx->buf_used += take;
        i += take;

        /* If buffer is now full, process it */
        if (ctx->buf_used == SHA1_BLOCK_SIZE) {
            sha1_process_block(ctx, ctx->buffer);
            ctx->buf_used = 0;
        }
    }

    /* Process full blocks directly from input (no copy needed) */
    while (i + SHA1_BLOCK_SIZE <= len) {
        sha1_process_block(ctx, data + i);
        i += SHA1_BLOCK_SIZE;
    }

    /* Buffer any remaining bytes (incomplete block) */
    if (i < len) {
        size_t remaining = len - i;
        memcpy(ctx->buffer, data + i, remaining);
        ctx->buf_used = remaining;
    }
}

/* ── sha1_final ──────────────────────────────────────────────────────────── */

void sha1_final(SHA1_CTX *ctx, uint8_t *digest) {
    /*
     * Finalize the hash by applying padding and processing the last block(s).
     *
     * SHA-1 Padding Rules (RFC 3174 Section 4):
     *
     *   1. Append a single '1' bit → byte 0x80
     *   2. Append '0' bits until message length ≡ 448 (mod 512 bits)
     *      i.e., the padded message is 8 bytes short of a 64-byte block
     *   3. Append the original message bit length as a 64-bit big-endian int
     *
     * This means padding always adds 1–64 bytes, never 0.
     */

    uint64_t bit_count = ctx->count;

    /* Append 0x80 byte (the '1' bit followed by 7 '0' bits) */
    uint8_t pad_byte = 0x80;
    sha1_update(ctx, &pad_byte, 1);

    /* Append 0x00 bytes until buf_used == 56 (8 bytes short of full block).
     * We need room for the 8-byte length field at the end. */
    uint8_t zero = 0x00;
    while (ctx->buf_used != 56) {
        sha1_update(ctx, &zero, 1);
    }

    /* Append the original message length as a 64-bit big-endian integer.
     * This "length extension" is critical — changing even one bit of the
     * original data changes this length, which changes the final hash. */
    uint8_t len_bytes[8];
    len_bytes[0] = (bit_count >> 56) & 0xFF;
    len_bytes[1] = (bit_count >> 48) & 0xFF;
    len_bytes[2] = (bit_count >> 40) & 0xFF;
    len_bytes[3] = (bit_count >> 32) & 0xFF;
    len_bytes[4] = (bit_count >> 24) & 0xFF;
    len_bytes[5] = (bit_count >> 16) & 0xFF;
    len_bytes[6] = (bit_count >>  8) & 0xFF;
    len_bytes[7] = (bit_count      ) & 0xFF;
    sha1_update(ctx, len_bytes, 8);

    /* The final block has now been processed. Write out the 5 state words
     * as 20 bytes in big-endian order. */
    for (int i = 0; i < 5; i++) {
        digest[i*4    ] = (ctx->h[i] >> 24) & 0xFF;
        digest[i*4 + 1] = (ctx->h[i] >> 16) & 0xFF;
        digest[i*4 + 2] = (ctx->h[i] >>  8) & 0xFF;
        digest[i*4 + 3] = (ctx->h[i]      ) & 0xFF;
    }
}

/* ── sha1 — convenience one-shot function ────────────────────────────────── */

void sha1(const uint8_t *data, size_t len, uint8_t *digest) {
    SHA1_CTX ctx;
    sha1_init(&ctx);
    sha1_update(&ctx, data, len);
    sha1_final(&ctx, digest);
}
