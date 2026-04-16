#pragma once
/**
 * torrent.h — Torrent Metadata
 *
 * Defines the structures that hold everything parsed from a .torrent file,
 * and the function to parse a .torrent file into those structures.
 *
 * See docs/02_torrent_file.md for a full explanation of .torrent structure.
 *
 * Usage:
 *   TorrentInfo *t = torrent_parse("ubuntu.torrent");
 *   printf("Downloading: %s\n", t->name);
 *   printf("Total size:  %ld bytes\n", t->total_length);
 *   printf("Pieces:      %d\n", t->num_pieces);
 *   torrent_free(t);
 */


#include <stdint.h>
#include <stddef.h>

#define MAX_TRACKERS    32
#define MAX_FILES       512
#define MAX_PATH_LEN    1024

/* ── File entry (for multi-file torrents) ────────────────────────────────── */

typedef struct {
    char  path[MAX_PATH_LEN];  /* full relative path, e.g. "dir/file.txt" */
    long  length;               /* size in bytes */
    long  offset;               /* byte offset from start of torrent data */
} FileEntry;

/* ── Main torrent info structure ─────────────────────────────────────────── */

typedef struct {
    /* Basic metadata */
    char    name[256];          /* name from info dict (file or dir name) */
    char    comment[512];       /* optional comment */
    char    created_by[256];    /* software that created the torrent */

    /* Tracker URLs */
    char    announce[768];              /* primary tracker URL */
    char    announce_list[MAX_TRACKERS][768]; /* backup trackers */
    int     num_trackers;

    /* The info hash — SHA-1 of the bencoded info dictionary.
     * This is the torrent's global unique ID, used in:
     *   - tracker requests (info_hash query param)
     *   - peer handshake (to verify same torrent)
     */
    uint8_t info_hash[20];

    /* Piece information */
    int     piece_length;       /* bytes per piece (power of 2, e.g. 262144) */
    int     num_pieces;         /* total number of pieces */
    uint8_t *pieces_hash;       /* raw bytes: num_pieces × 20-byte SHA-1s */

    /* File layout */
    int     is_multi_file;      /* 0 = single file, 1 = multiple files */
    long    total_length;       /* total bytes across all files */

    /* Single-file: total_length is the file size. */

    /* Multi-file: array of FileEntry structs */
    FileEntry files[MAX_FILES];
    int       num_files;

} TorrentInfo;

/* ── Functions ───────────────────────────────────────────────────────────── */

/**
 * torrent_parse - parse a .torrent file from disk.
 *
 * @path   filesystem path to the .torrent file
 * @return heap-allocated TorrentInfo, or NULL on error
 *
 * What this does internally:
 *   1. Read the file into memory
 *   2. Bencode-parse the entire file
 *   3. Locate the "info" dict and compute its SHA-1 (the info_hash)
 *   4. Extract fields into TorrentInfo
 *   5. Free the bencode tree
 */
TorrentInfo *torrent_parse(const char *path);

/**
 * torrent_free - free a TorrentInfo and its allocated fields.
 */
void torrent_free(TorrentInfo *t);

/**
 * torrent_print - print torrent metadata to stdout (for debugging/learning).
 * Shows: name, info_hash, trackers, piece count, file list.
 */
void torrent_print(const TorrentInfo *t);

/**
 * torrent_get_piece_length - get the length of piece `piece_idx`.
 *
 * All pieces are `t->piece_length` bytes EXCEPT the last piece,
 * which may be smaller.
 *
 * @t           torrent info
 * @piece_idx   0-based piece index
 * @return      byte length of that piece
 */
int torrent_get_piece_length(const TorrentInfo *t, int piece_idx);

/**
 * torrent_get_piece_hash - get pointer to the 20-byte SHA-1 for piece i.
 *
 * Returns `t->pieces_hash + (piece_idx * 20)`.
 * The returned pointer is valid as long as `t` is alive.
 */
const uint8_t *torrent_get_piece_hash(const TorrentInfo *t, int piece_idx);


