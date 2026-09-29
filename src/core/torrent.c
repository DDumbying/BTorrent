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
#include <limits.h>

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
 * compute_info_hash — SHA-1 of the raw bencoded bytes of the "info" value.
 *
 * Walks the top-level dict key by key, so the "info" value is located by
 * structure. (Searching the raw bytes for "4:info" matched inside earlier
 * values too — e.g. a comment "information 12" encodes as "14:information 12"
 * — which rejected valid torrents or could hash the wrong bytes.)
 */
static int compute_info_hash(const uint8_t *raw, size_t raw_len,
                              uint8_t *info_hash) {
    if (raw_len < 2 || raw[0] != 'd') return -1;
    size_t pos = 1;
    while (pos < raw_len && raw[pos] != 'e') {
        BencodeNode *key = NULL, *val = NULL;
        size_t klen = bencode_parse_ex(raw + pos, raw_len - pos, &key);
        if (klen == 0 || key->type != BENCODE_STR) { bencode_free(key); break; }
        pos += klen;
        size_t vlen = bencode_parse_ex(raw + pos, raw_len - pos, &val);
        bencode_free(val);
        if (vlen == 0) { bencode_free(key); break; }
        int is_info = key->str.len == 4 && memcmp(key->str.data, "info", 4) == 0;
        bencode_free(key);
        if (is_info) {
            sha1(raw + pos, vlen, info_hash);
            return 0;
        }
        pos += vlen;
    }
    LOG_ERROR("%s", "'info' key not found in torrent file");
    return -1;
}

/*
 * is_safe_component — a single path component that may be used as a file or
 * directory name under the output directory. Torrent metadata is untrusted
 * (for magnet links it comes from arbitrary peers), so anything that could
 * escape the output directory is rejected: empty names, "." / "..",
 * separators, and embedded NULs (which would silently truncate the name).
 */
static int is_safe_component(const uint8_t *s, size_t len) {
    if (len == 0) return 0;
    if (len == 1 && s[0] == '.') return 0;
    if (len == 2 && s[0] == '.' && s[1] == '.') return 0;
    for (size_t i = 0; i < len; i++)
        if (s[i] == '/' || s[i] == '\0') return 0;
    return 1;
}

TorrentInfo *torrent_parse(const char *path) {
    size_t   raw_len;
    uint8_t *raw = read_file(path, &raw_len);
    if (!raw) return NULL;
    TorrentInfo *t = torrent_parse_buffer(raw, raw_len);
    if (!t) LOG_ERROR("failed to parse torrent file: %s", path);
    free(raw);
    return t;
}

TorrentInfo *torrent_parse_buffer(const uint8_t *raw, size_t raw_len) {
    BencodeNode *root = bencode_parse(raw, raw_len);
    if (!root) { LOG_ERROR("%s", "bencode parse failed"); return NULL; }
    if (root->type != BENCODE_DICT) {
        LOG_ERROR("%s", "top-level value is not a dict");
        bencode_free(root); return NULL;
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
        goto fail;
    }

    /* The name becomes a file or directory under the output directory, so
     * it must be a single safe path component that fits without truncation. */
    BencodeNode *name = bencode_dict_get(info, "name");
    if (!name || name->type != BENCODE_STR ||
        name->str.len >= sizeof(t->name) ||
        !is_safe_component(name->str.data, name->str.len)) {
        LOG_ERROR("%s", "missing or unsafe 'name' in info dict");
        goto fail;
    }
    memcpy(t->name, name->str.data, name->str.len);

    BencodeNode *pl = bencode_dict_get(info, "piece length");
    if (!pl || pl->type != BENCODE_INT ||
        pl->integer <= 0 || pl->integer > MAX_PIECE_LENGTH) {
        LOG_ERROR("%s", "missing or invalid 'piece length'");
        goto fail;
    }
    t->piece_length = (int)pl->integer;

    BencodeNode *pieces = bencode_dict_get(info, "pieces");
    if (!pieces || pieces->type != BENCODE_STR || pieces->str.len == 0 ||
        pieces->str.len % 20 != 0 || pieces->str.len / 20 > INT_MAX) {
        LOG_ERROR("%s", "invalid 'pieces' field");
        goto fail;
    }
    t->num_pieces  = (int)(pieces->str.len / 20);
    t->pieces_hash = xmalloc(pieces->str.len);
    memcpy(t->pieces_hash, pieces->str.data, pieces->str.len);

    BencodeNode *length_node = bencode_dict_get(info, "length");
    BencodeNode *files_node  = bencode_dict_get(info, "files");

    if (length_node && length_node->type == BENCODE_INT) {
        if (length_node->integer <= 0 || length_node->integer > LONG_MAX) {
            LOG_ERROR("%s", "invalid 'length' in info dict");
            goto fail;
        }
        t->is_multi_file  = 0;
        t->total_length   = (long)length_node->integer;
        t->num_files      = 1;
        strncpy(t->files[0].path, t->name, MAX_PATH_LEN - 1);
        t->files[0].length = t->total_length;
        t->files[0].offset = 0;
    } else if (files_node && files_node->type == BENCODE_LIST) {
        if (files_node->list.count == 0 || files_node->list.count > MAX_FILES) {
            LOG_ERROR("torrent has %zu files (supported: 1..%d)",
                      files_node->list.count, MAX_FILES);
            goto fail;
        }
        t->is_multi_file = 1;
        for (size_t i = 0; i < files_node->list.count; i++) {
            BencodeNode *file  = files_node->list.items[i];
            BencodeNode *flen  = bencode_dict_get(file, "length");
            BencodeNode *fpath = bencode_dict_get(file, "path");
            if (file->type != BENCODE_DICT ||
                !flen  || flen->type  != BENCODE_INT ||
                !fpath || fpath->type != BENCODE_LIST || fpath->list.count == 0 ||
                flen->integer < 0 || flen->integer > LONG_MAX - t->total_length) {
                LOG_ERROR("invalid entry %zu in 'files' list", i);
                goto fail;
            }
            FileEntry *fe = &t->files[t->num_files];
            fe->length = (long)flen->integer;
            fe->offset = t->total_length;

            /* Build "name/comp1/comp2..." — every component must be safe and
             * the whole path must fit; truncation could create ".." or merge
             * two distinct files into one path. */
            size_t path_pos = strlen(t->name);
            memcpy(fe->path, t->name, path_pos);
            for (size_t j = 0; j < fpath->list.count; j++) {
                BencodeNode *comp = fpath->list.items[j];
                if (comp->type != BENCODE_STR ||
                    !is_safe_component(comp->str.data, comp->str.len) ||
                    path_pos + 1 + comp->str.len >= MAX_PATH_LEN) {
                    LOG_ERROR("unsafe or too-long path in 'files' entry %zu", i);
                    goto fail;
                }
                fe->path[path_pos++] = '/';
                memcpy(fe->path + path_pos, comp->str.data, comp->str.len);
                path_pos += comp->str.len;
            }
            fe->path[path_pos] = '\0';
            t->num_files++;
            t->total_length += fe->length;
        }
        if (t->total_length <= 0) {
            LOG_ERROR("%s", "multi-file torrent has zero total length");
            goto fail;
        }
    } else {
        LOG_ERROR("%s", "no 'length' or 'files' in info dict");
        goto fail;
    }

    /* The piece count must match the data size exactly; otherwise piece
     * offsets would point outside the files. */
    /* ceil(total / piece_length) without the overflow of total + pl - 1
     * (total_length may be close to LONG_MAX). */
    long long expected_pieces = t->total_length / t->piece_length +
                                (t->total_length % t->piece_length != 0);
    if (expected_pieces != t->num_pieces) {
        LOG_ERROR("piece count mismatch: %d hashes for %ld bytes "
                  "(expected %lld pieces)",
                  t->num_pieces, t->total_length, expected_pieces);
        goto fail;
    }

    if (compute_info_hash(raw, raw_len, t->info_hash) < 0) goto fail;

    bencode_free(root);
    return t;

fail:
    bencode_free(root);
    torrent_free(t);
    return NULL;
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
