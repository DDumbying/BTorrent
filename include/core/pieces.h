#pragma once
/**
 * pieces.h — Piece Download Manager
 */

#include "core/torrent.h"
#include "result.h"
#include <stdint.h>
#include <time.h>

#define BLOCK_SIZE 16384   /* 16 KiB */

typedef enum { PIECE_EMPTY, PIECE_ASSIGNED, PIECE_ACTIVE, PIECE_COMPLETE } PieceState;

typedef struct {
    PieceState  state;
    uint8_t    *data;
    int         piece_length;
    uint8_t    *block_received;
    int         num_blocks;
    int         blocks_done;
} PieceStatus;

typedef struct {
    const TorrentInfo *torrent;
    PieceStatus       *pieces;
    int                num_pieces;

    /* Output file descriptors (one per file, using pwrite/pread) */
    int    *file_fds;
    char    out_path[1024];

    int               completed;
    long long         bytes_downloaded;
    long long         bytes_at_start;
    struct timespec   start_time;

    /* Sliding-window speed: ring buffer of (time, bytes) samples */
#define SPEED_SAMPLES 8
    long long         spd_bytes[SPEED_SAMPLES];
    struct timespec   spd_time[SPEED_SAMPLES];
    int               spd_head;   /* next write index */
    int               spd_count;  /* samples filled so far */

    uint8_t *our_bitfield;
    int      bf_len;

    /* File lock — prevents two simultaneous instances corrupting the output */
    int      lock_fd;
} PieceManager;

/**
 * piece_manager_new — open (creating if needed) the output files and find
 * which pieces are already complete. With use_resume, a valid fast-resume
 * record (<out_path>.btresume, written by piece_manager_free) is trusted
 * instead of hashing every piece; without it — e.g. for `-c` — every piece
 * on disk is verified.
 */
PieceManager *piece_manager_new(const TorrentInfo *torrent, const char *out_path,
                                int use_resume);
void          piece_manager_free(PieceManager *pm);

int  piece_manager_on_block(PieceManager *pm, int piece_index,
                            int begin, const uint8_t *data, int len);

int  piece_manager_next_needed(PieceManager  *pm,
                               const uint8_t *peer_bitfield,
                               int            num_pieces);

int  piece_manager_is_complete(const PieceManager *pm);
void piece_manager_print_progress(const PieceManager *pm);
int  piece_manager_read_piece(PieceManager *pm, int piece_idx, uint8_t *buf);

/** Read `len` bytes at `begin` within a piece (bounds-checked). 1 on success. */
int  piece_manager_read_block(PieceManager *pm, int piece_idx,
                              int begin, int len, uint8_t *buf);
