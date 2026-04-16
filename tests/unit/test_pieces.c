/**
 * test_pieces.c — Piece manager unit tests
 *
 * Build:
 *   gcc -Iinclude -std=c11 tests/unit/test_pieces.c \
 *       src/core/pieces.c src/core/sha1.c src/core/torrent.c \
 *       src/core/bencode.c src/proto/peer.c \
 *       src/utils.c src/log.c src/result.c \
 *       -o build/test_pieces && ./build/test_pieces
 */

#define _POSIX_C_SOURCE 200809L
#include "core/pieces.h"
#include "core/sha1.h"
#include "core/torrent.h"
#include "proto/peer.h"
#include "utils.h"
#include "log.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>

static int passed = 0, failed = 0;
#define ASSERT(cond, label) \
    do { if (cond) { printf("  PASS  %s\n", label); passed++; } \
         else { printf("  FAIL  %s  (line %d)\n", label, __LINE__); failed++; } } while(0)

/*
 * make_test_torrent — build a minimal single-file TorrentInfo.
 *
 * use_bogus_hashes=1 → pieces_hash filled with 0xFF so the resume scan
 *   never matches anything on disk. This lets tests start with a clean
 *   PIECE_EMPTY slate even if the output file already exists.
 *
 * use_bogus_hashes=0 → pieces_hash computed from real SHA-1 of zero-
 *   filled data, matching what ftruncate writes. Used for resume tests.
 */
static TorrentInfo *make_test_torrent(int piece_length, int num_pieces,
                                      const char *tmpfile, int use_bogus_hashes) {
    TorrentInfo *t = xcalloc(1, sizeof(TorrentInfo));
    strncpy(t->name, "test", sizeof(t->name) - 1);
    t->piece_length  = piece_length;
    t->num_pieces    = num_pieces;
    t->total_length  = (long)piece_length * num_pieces;
    t->is_multi_file = 0;
    t->num_files     = 1;
    strncpy(t->files[0].path, tmpfile, MAX_PATH_LEN - 1);
    t->files[0].length = t->total_length;
    t->files[0].offset = 0;

    t->pieces_hash = xmalloc((size_t)(num_pieces * 20));

    if (use_bogus_hashes) {
        /* 0xFF bytes will never match any real SHA-1 digest */
        memset(t->pieces_hash, 0xFF, (size_t)(num_pieces * 20));
    } else {
        /* Real SHA-1 of zero-filled pieces — used for resume test */
        uint8_t *zero_piece = xcalloc((size_t)piece_length, 1);
        for (int i = 0; i < num_pieces; i++)
            sha1(zero_piece, (size_t)piece_length, t->pieces_hash + i * 20);
        free(zero_piece);
    }
    return t;
}

/* ── Test: a complete zero-filled piece is accepted and written ──────────── */
static void test_complete_piece(void) {
    const char *tmp = "/tmp/bt_test_piece.bin";
    unlink(tmp);

    /* bogus hashes → resume finds nothing → all pieces start EMPTY */
    TorrentInfo *t = make_test_torrent(BLOCK_SIZE, 2, tmp, 1);
    PieceManager *pm = piece_manager_new(t, tmp);

    ASSERT(pm->completed == 0, "initial: no pieces completed");

    /* Compute the real hash for a zero block and store it so on_block accepts it */
    uint8_t *zero_block = xcalloc(BLOCK_SIZE, 1);
    sha1(zero_block, BLOCK_SIZE, t->pieces_hash); /* overwrite piece-0 hash */

    int result = piece_manager_on_block(pm, 0, 0, zero_block, BLOCK_SIZE);
    free(zero_block);

    ASSERT(result == 1,        "on_block: piece 0 verified and written");
    ASSERT(pm->completed == 1, "completed count == 1 after one piece");
    ASSERT( bitfield_has_piece(pm->our_bitfield, 0), "our bitfield bit 0 set");
    ASSERT(!bitfield_has_piece(pm->our_bitfield, 1), "our bitfield bit 1 unset");

    piece_manager_free(pm);
    free(t->pieces_hash); free(t);
    unlink(tmp);
}

/* ── Test: a piece with wrong data is rejected (SHA-1 mismatch) ─────────── */
static void test_corrupt_piece_rejected(void) {
    const char *tmp = "/tmp/bt_test_corrupt.bin";
    unlink(tmp);

    /* bogus hashes → no resume, no accidental match */
    TorrentInfo *t = make_test_torrent(BLOCK_SIZE, 1, tmp, 1);
    PieceManager *pm = piece_manager_new(t, tmp);

    uint8_t *block = xmalloc(BLOCK_SIZE);
    memset(block, 0xAB, BLOCK_SIZE);   /* non-zero, never matches 0xFF hash */
    int result = piece_manager_on_block(pm, 0, 0, block, BLOCK_SIZE);
    free(block);

    ASSERT(result == -1,                           "corrupt: returns -1");
    ASSERT(pm->completed == 0,                     "corrupt: completed stays 0");
    ASSERT(pm->pieces[0].state == PIECE_EMPTY,     "corrupt: piece reset to EMPTY");
    ASSERT(pm->pieces[0].blocks_done == 0,         "corrupt: blocks_done reset");

    piece_manager_free(pm);
    free(t->pieces_hash); free(t);
    unlink(tmp);
}

/* ── Test: multi-block piece assembled correctly from partial blocks ──────── */
static void test_two_block_piece(void) {
    const char *tmp = "/tmp/bt_test_twoblock.bin";
    unlink(tmp);

    int piece_len = BLOCK_SIZE * 2;
    TorrentInfo *t = make_test_torrent(piece_len, 1, tmp, 1);
    PieceManager *pm = piece_manager_new(t, tmp);

    /* Build a deterministic piece: first block = 0xAA, second = 0xBB */
    uint8_t *blk0 = xmalloc(BLOCK_SIZE);
    uint8_t *blk1 = xmalloc(BLOCK_SIZE);
    memset(blk0, 0xAA, BLOCK_SIZE);
    memset(blk1, 0xBB, BLOCK_SIZE);

    uint8_t full_piece[BLOCK_SIZE * 2];
    memcpy(full_piece,             blk0, BLOCK_SIZE);
    memcpy(full_piece + BLOCK_SIZE, blk1, BLOCK_SIZE);
    sha1(full_piece, (size_t)piece_len, t->pieces_hash); /* set real hash */

    int r0 = piece_manager_on_block(pm, 0, 0,          blk0, BLOCK_SIZE);
    int r1 = piece_manager_on_block(pm, 0, BLOCK_SIZE, blk1, BLOCK_SIZE);
    free(blk0); free(blk1);

    ASSERT(r0 == 0, "two-block: first block returns 0 (not done yet)");
    ASSERT(r1 == 1, "two-block: second block returns 1 (piece complete)");
    ASSERT(pm->completed == 1, "two-block: completed == 1");

    piece_manager_free(pm);
    free(t->pieces_hash); free(t);
    unlink(tmp);
}

/* ── Test: next_needed skips already-complete pieces ────────────────────── */
static void test_next_needed_skips_complete(void) {
    const char *tmp = "/tmp/bt_test_next.bin";
    unlink(tmp);

    TorrentInfo *t = make_test_torrent(BLOCK_SIZE, 3, tmp, 1);
    PieceManager *pm = piece_manager_new(t, tmp);

    /* Mark piece 0 complete manually */
    pm->pieces[0].state = PIECE_COMPLETE;
    pm->completed = 1;
    bitfield_set_piece(pm->our_bitfield, 0);

    int next = piece_manager_next_needed(pm, NULL, t->num_pieces);
    ASSERT(next == 1, "next_needed skips piece 0, returns 1");

    piece_manager_free(pm);
    free(t->pieces_hash); free(t);
    unlink(tmp);
}

/* ── Test: next_needed respects peer bitfield ────────────────────────────── */
static void test_next_needed_peer_filter(void) {
    const char *tmp = "/tmp/bt_test_filter.bin";
    unlink(tmp);

    TorrentInfo *t = make_test_torrent(BLOCK_SIZE, 4, tmp, 1);
    PieceManager *pm = piece_manager_new(t, tmp);

    /* Peer only has piece 2 */
    uint8_t peer_bf[1] = { 0 };
    bitfield_set_piece(peer_bf, 2);

    int next = piece_manager_next_needed(pm, peer_bf, t->num_pieces);
    ASSERT(next == 2, "next_needed: returns only piece peer has");

    piece_manager_free(pm);
    free(t->pieces_hash); free(t);
    unlink(tmp);
}

/* ── Test: resume — pieces matching on-disk SHA-1 are auto-completed ─────── */
static void test_resume(void) {
    const char *tmp = "/tmp/bt_test_resume.bin";
    unlink(tmp);

    /* Write a zero-filled file (3 pieces × BLOCK_SIZE bytes) explicitly.
     * Previously this relied on ftruncate() in piece_manager_new, but we
     * removed pre-allocation to avoid creating misleadingly large files.
     * The resume logic reads existing bytes from disk and SHA-1 verifies them;
     * we must put the expected bytes there first. */
    {
        int fd = open(tmp, O_RDWR | O_CREAT | O_TRUNC, 0644);
        if (fd >= 0) {
            uint8_t *zeros = xcalloc(BLOCK_SIZE * 3, 1);
            ssize_t written = write(fd, zeros, BLOCK_SIZE * 3);
            (void)written;
            free(zeros);
            close(fd);
        }
    }

    /* use_bogus_hashes=0 → real SHA-1 of zero-filled pieces */
    TorrentInfo *t = make_test_torrent(BLOCK_SIZE, 3, tmp, 0);

    /* First manager: should find all 3 pieces already matching */
    PieceManager *pm = piece_manager_new(t, tmp);
    int resumed = pm->completed;
    piece_manager_free(pm);

    /* Second manager on same file: must see same count */
    PieceManager *pm2 = piece_manager_new(t, tmp);
    ASSERT(pm2->completed == resumed, "resume: second open finds same completed count");
    ASSERT(resumed == 3, "resume: all 3 zero-filled pieces verified from disk");
    piece_manager_free(pm2);

    free(t->pieces_hash); free(t);
    unlink(tmp);
}

int main(void) {
    log_init(LOG_ERROR, NULL);   /* suppress noise in test output */
    printf("=== Piece Manager Tests ===\n");
    test_complete_piece();
    test_corrupt_piece_rejected();
    test_two_block_piece();
    test_next_needed_skips_complete();
    test_next_needed_peer_filter();
    test_resume();
    printf("\n%d passed, %d failed\n", passed, failed);
    return failed ? EXIT_FAILURE : EXIT_SUCCESS;
}
