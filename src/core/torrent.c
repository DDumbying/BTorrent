#define _POSIX_C_SOURCE 200809L
/**
 * torrent.c — .torrent File Parser
 *
 * Changes from v1:
 *   - compute_info_hash now uses bencode_parse_ex() to find the info dict
 *     end position directly, eliminating the redundant manual depth-walk.
 *   - All fprintf/printf diagnostic calls replaced with LOG_* macros.
 * Bugfixes:
 *   - read_file: use stat() for file size instead of fseek/ftell to avoid
 *     ambiguous -1 return on error being misreported as "file is empty".
 *     Also adds strerror(errno) to fopen/fread error messages.
 */

#include "core/torrent.h"
#include "core/bencode.h"
#include "core/sha1.h"
#include "utils.h"
#include "log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/stat.h>

static uint8_t *read_file(const char *path, size_t *out_len) {
    FILE *f = fopen(path, "rb");
    if (!f) { LOG_ERROR("cannot open %s: %s", path, strerror(errno)); return NULL; }

    struct stat st;
    if (fstat(fileno(f), &st) < 0) {
        LOG_ERROR("cannot stat %s: %s", path, strerror(errno));
        fclose(f); return NULL;
    }
    if (st.st_size == 0) {
        LOG_ERROR("torrent file is empty: %s", path);
        fclose(f); return NULL;
    }
    if (st.st_size < 0) {
        LOG_ERROR("invalid file size for %s", path);
        fclose(f); return NULL;
    }

    size_t size = (size_t)st.st_size;
    uint8_t *buf = xmalloc(size);
    if (fread(buf, 1, size, f) != size) {
        LOG_ERROR("failed to read %s: %s", path, strerror(errno));
        free(buf); fclose(f); return NULL;
    }
    fclose(f);
    *out_len = size;
    return buf;
}

/*
 * compute_info_hash — SHA-1 of the raw bencoded bytes of the info dict.
 *
 * FIX: Previous version called bencode_parse() to "find" the end, then
 * discarded the result and re-walked manually. Now we use bencode_parse_ex()
 * which returns the number of bytes consumed, giving us the exact end
 * position with a single parse pass and no dead code.
 */
static int compute_info_hash(const uint8_t *raw, size_t raw_len,
                              uint8_t *info_hash) {
    const uint8_t needle[] = "4:info";
    const size_t  needle_len = 6;

    for (size_t i = 0; i + needle_len <= raw_len; i++) {
        if (memcmp(raw + i, needle, needle_len) != 0) continue;

        size_t info_start = i + needle_len;

        BencodeNode *info_node = NULL;
        size_t consumed = bencode_parse_ex(raw + info_start,
                                           raw_len - info_start,
                                           &info_node);
        if (consumed == 0 || !info_node) {
            LOG_ERROR("%s", "failed to parse info dict");
            return -1;
        }

        sha1(raw + info_start, consumed, info_hash);
        bencode_free(info_node);
        return 0;
    }

    LOG_ERROR("%s", "'info' key not found in torrent file");
    return -1;
}

TorrentInfo *torrent_parse(const char *path) {
    size_t   raw_len;
    uint8_t *raw = read_file(path, &raw_len);
    if (!raw) return NULL;

    BencodeNode *root = bencode_parse(raw, raw_len);
    if (!root) { LOG_ERROR("bencode parse failed: %s", path); free(raw); return NULL; }
    if (root->type != BENCODE_DICT) {
        LOG_ERROR("%s", "top-level value is not a dict");
        bencode_free(root); free(raw); return NULL;
    }

    TorrentInfo *t = xcalloc(1, sizeof(TorrentInfo));

    BencodeNode *announce = bencode_dict_get(root, "announce");
    if (announce && announce->type == BENCODE_STR) {
        size_t len = announce->str.len < sizeof(t->announce)-1
                   ? announce->str.len : sizeof(t->announce)-1;
        memcpy(t->announce, announce->str.data, len);
    }

    BencodeNode *alist = bencode_dict_get(root, "announce-list");
    if (alist && alist->type == BENCODE_LIST) {
        for (size_t i = 0; i < alist->list.count && t->num_trackers < MAX_TRACKERS; i++) {
            BencodeNode *tier = alist->list.items[i];
            if (tier->type != BENCODE_LIST) continue;
            for (size_t j = 0; j < tier->list.count && t->num_trackers < MAX_TRACKERS; j++) {
                BencodeNode *url = tier->list.items[j];
                if (url->type != BENCODE_STR) continue;
                size_t len = url->str.len < 511 ? url->str.len : 511;
                memcpy(t->announce_list[t->num_trackers], url->str.data, len);
                t->num_trackers++;
            }
        }
    }

    BencodeNode *comment = bencode_dict_get(root, "comment");
    if (comment && comment->type == BENCODE_STR) {
        size_t len = comment->str.len < sizeof(t->comment)-1
                   ? comment->str.len : sizeof(t->comment)-1;
        memcpy(t->comment, comment->str.data, len);
    }
    BencodeNode *created_by = bencode_dict_get(root, "created by");
    if (created_by && created_by->type == BENCODE_STR) {
        size_t len = created_by->str.len < sizeof(t->created_by)-1
                   ? created_by->str.len : sizeof(t->created_by)-1;
        memcpy(t->created_by, created_by->str.data, len);
    }

    BencodeNode *info = bencode_dict_get(root, "info");
    if (!info || info->type != BENCODE_DICT) {
        LOG_ERROR("%s", "missing or invalid 'info' dict");
        bencode_free(root); free(raw); free(t); return NULL;
    }

    BencodeNode *name = bencode_dict_get(info, "name");
    if (name && name->type == BENCODE_STR) {
        size_t len = name->str.len < sizeof(t->name)-1
                   ? name->str.len : sizeof(t->name)-1;
        memcpy(t->name, name->str.data, len);
    }

    BencodeNode *pl = bencode_dict_get(info, "piece length");
    if (!pl || pl->type != BENCODE_INT) {
        LOG_ERROR("%s", "missing 'piece length'");
        bencode_free(root); free(raw); free(t); return NULL;
    }
    t->piece_length = (int)pl->integer;

    BencodeNode *pieces = bencode_dict_get(info, "pieces");
    if (!pieces || pieces->type != BENCODE_STR || pieces->str.len % 20 != 0) {
        LOG_ERROR("%s", "invalid 'pieces' field");
        bencode_free(root); free(raw); free(t); return NULL;
    }
    t->num_pieces  = (int)(pieces->str.len / 20);
    t->pieces_hash = xmalloc(pieces->str.len);
    memcpy(t->pieces_hash, pieces->str.data, pieces->str.len);

    BencodeNode *length_node = bencode_dict_get(info, "length");
    BencodeNode *files_node  = bencode_dict_get(info, "files");

    if (length_node && length_node->type == BENCODE_INT) {
        t->is_multi_file  = 0;
        t->total_length   = (long)length_node->integer;
        t->num_files      = 1;
        strncpy(t->files[0].path, t->name, MAX_PATH_LEN - 1);
        t->files[0].length = t->total_length;
        t->files[0].offset = 0;
    } else if (files_node && files_node->type == BENCODE_LIST) {
        t->is_multi_file = 1;
        long offset = 0;
        for (size_t i = 0; i < files_node->list.count && t->num_files < MAX_FILES; i++) {
            BencodeNode *file = files_node->list.items[i];
            if (file->type != BENCODE_DICT) continue;
            BencodeNode *flen  = bencode_dict_get(file, "length");
            BencodeNode *fpath = bencode_dict_get(file, "path");
            if (!flen || flen->type != BENCODE_INT) continue;
            if (!fpath || fpath->type != BENCODE_LIST) continue;
            FileEntry *fe = &t->files[t->num_files];
            fe->length = (long)flen->integer;
            fe->offset = offset;
            offset    += fe->length;
            /* Build "name/" prefix into path, avoiding -Wrestrict */
            char dir_prefix[MAX_PATH_LEN];
            int pfx_len = snprintf(dir_prefix, sizeof(dir_prefix), "%s/", t->name);
            if (pfx_len < 0 || pfx_len >= MAX_PATH_LEN) pfx_len = MAX_PATH_LEN - 1;
            memcpy(fe->path, dir_prefix, (size_t)pfx_len + 1);
            size_t path_pos = (size_t)pfx_len;
            for (size_t j = 0; j < fpath->list.count; j++) {
                BencodeNode *comp = fpath->list.items[j];
                if (comp->type != BENCODE_STR) continue;
                if (j > 0 && path_pos < (size_t)MAX_PATH_LEN - 1)
                    fe->path[path_pos++] = '/';
                size_t clen = comp->str.len < (size_t)(MAX_PATH_LEN - (int)path_pos - 1)
                            ? comp->str.len : (size_t)(MAX_PATH_LEN - (int)path_pos - 1);
                memcpy(fe->path + path_pos, comp->str.data, clen);
                path_pos += clen;
            }
            fe->path[path_pos] = '\0';
            t->num_files++;
            t->total_length += fe->length;
        }
    } else {
        LOG_ERROR("%s", "no 'length' or 'files' in info dict");
        bencode_free(root); free(raw); free(t->pieces_hash); free(t); return NULL;
    }

    if (compute_info_hash(raw, raw_len, t->info_hash) < 0) {
        bencode_free(root); free(raw); free(t->pieces_hash); free(t); return NULL;
    }

    bencode_free(root);
    free(raw);
    return t;
}

void torrent_free(TorrentInfo *t) {
    if (!t) return;
    free(t->pieces_hash);
    free(t);
}

void torrent_print(const TorrentInfo *t) {
    char hash_str[41];
    hex_to_str(t->info_hash, 20, hash_str);
    LOG_INFO("%s", "=== Torrent Info ===");
    LOG_INFO("Name:         %s", t->name);
    LOG_INFO("Comment:      %s", t->comment);
    LOG_INFO("Created by:   %s", t->created_by);
    LOG_INFO("Info hash:    %s", hash_str);
    LOG_INFO("Announce:     %s", t->announce);
    LOG_INFO("Total size:   %ld bytes (%.2f MB)",
             t->total_length, (double)t->total_length / (1024.0 * 1024.0));
    LOG_INFO("Piece length: %d bytes", t->piece_length);
    LOG_INFO("Pieces:       %d", t->num_pieces);
    LOG_INFO("Multi-file:   %s", t->is_multi_file ? "yes" : "no");
    for (int i = 0; i < t->num_trackers && i < 5; i++)
        LOG_DEBUG("Tracker[%d]: %s", i, t->announce_list[i]);
    if (t->is_multi_file) {
        LOG_INFO("Files (%d):", t->num_files);
        for (int i = 0; i < t->num_files && i < 10; i++)
            LOG_INFO("  [%d] %s (%ld bytes)", i, t->files[i].path, t->files[i].length);
        if (t->num_files > 10) LOG_INFO("  ... and %d more", t->num_files - 10);
    }
}

int torrent_get_piece_length(const TorrentInfo *t, int piece_idx) {
    if (piece_idx < t->num_pieces - 1) return t->piece_length;
    int remainder = (int)(t->total_length % t->piece_length);
    return remainder == 0 ? t->piece_length : remainder;
}

const uint8_t *torrent_get_piece_hash(const TorrentInfo *t, int piece_idx) {
    return t->pieces_hash + (piece_idx * 20);
}
