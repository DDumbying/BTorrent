#define _POSIX_C_SOURCE 200809L
/**
 * pieces.c — Piece Download Manager
 *
 * Changes from v1:
 *   - All fprintf/printf replaced with LOG_* macros.
 *   - pwrite(2) replaces fseek+fwrite for atomic, positional file I/O.
 *   - piece_manager_new uses wall-clock start time correctly.
 */

#include "core/pieces.h"
#include "proto/peer.h"
#include "core/sha1.h"
#include "utils.h"
#include "log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>

static int compute_num_blocks(int piece_length) {
    return (piece_length + BLOCK_SIZE - 1) / BLOCK_SIZE;
}

/* ensure_parent_dir — create every directory component leading up to
 * the file described by `path`, but NOT the final component itself.
 *
 * Examples:
 *   "ubuntu"              → nothing (parent is cwd, always exists)
 *   "ubuntu/file.iso"     → mkdir "ubuntu/"
 *   "/tmp/dl/a/b.iso"     → mkdir "/tmp/", "/tmp/dl/", "/tmp/dl/a/"
 */
static void ensure_parent_dir(const char *path) {
    char tmp[2048];
    snprintf(tmp, sizeof(tmp), "%s", path);
    /* Walk up to the last '/' — everything before it is a directory */
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            mkdir(tmp, 0755);   /* ignore errors — dir may already exist */
            *p = '/';
        }
    }
    /* Intentionally stop before the final component (the file itself) */
}

/*
 * open_output_files — open (or create) all output files.
 * Uses file descriptors (int) instead of FILE* so we can use pwrite().
 */
/*
 * open_output_fds — open (or create) all output files.
 *
 * BUG FIX 1 — output path:
 *   For single-file torrents, t->files[0].path is always the torrent's
 *   internal name (e.g. "ubuntu-24.04.4-desktop-amd64.iso"). The user's
 *   -o flag is stored in pm->out_path. We must use out_path as the actual
 *   filesystem path for single-file torrents, and as the directory prefix
 *   for multi-file torrents.
 *
 * BUG FIX 2 — no pre-allocation:
 *   Removed ftruncate(). It created a full-size sparse file (6 GB) before
 *   a single byte was downloaded, which is misleading and wastes inodes on
 *   non-sparse filesystems. pwrite() already handles arbitrary offsets.
 */
static int *open_output_fds(PieceManager *pm) {
    const TorrentInfo *t = pm->torrent;
    int *fds = xcalloc((size_t)t->num_files, sizeof(int));

    for (int i = 0; i < t->num_files; i++) {
        /* Build the real filesystem path from out_path */
        char real_path[2048];
        if (!t->is_multi_file) {
            /* Single-file: out_path IS the file path */
            snprintf(real_path, sizeof(real_path), "%s", pm->out_path);
        } else {
            /* Multi-file: out_path is the base directory.
               t->files[i].path already has "name/sub/file" form;
               strip the leading torrent name component and replace
               it with out_path so -o is respected. */
            const char *rel = t->files[i].path;
            /* Skip the torrent-name prefix ("name/") */
            const char *slash = strchr(rel, '/');
            if (slash) rel = slash + 1;
            snprintf(real_path, sizeof(real_path), "%s/%s",
                     pm->out_path, rel);
        }

        ensure_parent_dir(real_path);
        /* Also ensure the immediate parent exists (for single-file in subdir) */
        {
            char dir[2048];
            snprintf(dir, sizeof(dir), "%s", real_path);
            char *slash = strrchr(dir, '/');
            if (slash && slash != dir) {
                *slash = '\0';
                mkdir(dir, 0755);
            }
        }

        fds[i] = open(real_path, O_RDWR | O_CREAT | O_CLOEXEC, 0644);
        if (fds[i] < 0) {
            LOG_ERROR("cannot open output file: %s", real_path);
            for (int j = 0; j < i; j++) close(fds[j]);
            free(fds);
            return NULL;
        }
        /* Store the resolved path back. Use memcpy after capping length. */
        {
            size_t _rlen = strlen(real_path);
            if (_rlen >= MAX_PATH_LEN) _rlen = MAX_PATH_LEN - 1;
            memcpy((char *)t->files[i].path, real_path, _rlen);
            ((char *)t->files[i].path)[_rlen] = '\0';
        }
    }
    return fds;
}

/* ── Fast resume ───────────────────────────────────────────────────────────
 *
 * On a clean exit we record which pieces are complete in <out>.btresume,
 * together with every output file's size and modification time. On the next
 * start, if the torrent and all file sizes/mtimes still match, the recorded
 * pieces are trusted instead of re-hashing everything on disk (which for a
 * multi-GB torrent takes seconds to minutes).
 *
 * Any write to the data after the record was taken changes a file's mtime,
 * so a stale record — e.g. after a crash mid-download, or a file edited by
 * hand — is rejected and we fall back to a full hash check. The data files
 * are fdatasync()ed before the record is written.
 *
 * Layout (big-endian): "BTRESUME" u32 version, info_hash[20], u32 num_pieces,
 * u32 num_files, then per file: u64 size, u64 mtime_sec, u32 mtime_nsec,
 * then the completed-pieces bitfield.
 */
#define RESUME_MAGIC   "BTRESUME"
#define RESUME_VERSION 1

static void put_be(uint8_t *p, uint64_t v, int n) {
    for (int i = n - 1; i >= 0; i--) { p[i] = (uint8_t)v; v >>= 8; }
}
static uint64_t get_be(const uint8_t *p, int n) {
    uint64_t v = 0;
    for (int i = 0; i < n; i++) v = (v << 8) | p[i];
    return v;
}

static void resume_path(const PieceManager *pm, char *out, size_t cap) {
    snprintf(out, cap, "%s.btresume", pm->out_path);
}

/* Serialise the header + file stamps + bitfield. Returns malloc'd buffer. */
static uint8_t *resume_encode(PieceManager *pm, size_t *out_len) {
    const TorrentInfo *t = pm->torrent;
    size_t len = 8 + 4 + 20 + 4 + 4 + (size_t)t->num_files * 20 + (size_t)pm->bf_len;
    uint8_t *buf = xcalloc(len, 1), *p = buf;
    memcpy(p, RESUME_MAGIC, 8);              p += 8;
    put_be(p, RESUME_VERSION, 4);            p += 4;
    memcpy(p, t->info_hash, 20);             p += 20;
    put_be(p, (uint64_t)t->num_pieces, 4);   p += 4;
    put_be(p, (uint64_t)t->num_files, 4);    p += 4;
    for (int i = 0; i < t->num_files; i++) {
        struct stat st;
        if (pm->file_fds[i] < 0 || fstat(pm->file_fds[i], &st) < 0) {
            free(buf); return NULL;
        }
        put_be(p, (uint64_t)st.st_size, 8);          p += 8;
        put_be(p, (uint64_t)st.st_mtim.tv_sec, 8);   p += 8;
        put_be(p, (uint64_t)st.st_mtim.tv_nsec, 4);  p += 4;
    }
    memcpy(p, pm->our_bitfield, (size_t)pm->bf_len);
    *out_len = len;
    return buf;
}

static void resume_save(PieceManager *pm) {
    for (int i = 0; i < pm->torrent->num_files; i++)
        if (pm->file_fds[i] >= 0 && fdatasync(pm->file_fds[i]) < 0) {
            LOG_WARN("resume: fdatasync failed: %s", strerror(errno));
            return;
        }
    size_t len;
    uint8_t *buf = resume_encode(pm, &len);
    if (!buf) return;

    /* Write a temp file and rename it, so a crash never leaves a torn record. */
    char path[1100], tmp[1110];
    resume_path(pm, path, sizeof(path));
    snprintf(tmp, sizeof(tmp), "%s.tmp", path);
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    int ok = fd >= 0 && write(fd, buf, len) == (ssize_t)len && fsync(fd) == 0;
    if (fd >= 0) close(fd);
    if (ok) ok = rename(tmp, path) == 0;
    if (!ok) { LOG_WARN("resume: cannot write %s: %s", path, strerror(errno)); unlink(tmp); }
    free(buf);
}

/* Returns the number of pieces restored, or -1 if there is no usable record. */
static int resume_load(PieceManager *pm) {
    char path[1100];
    resume_path(pm, path, sizeof(path));
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -1;

    size_t want;
    uint8_t *cur = resume_encode(pm, &want);   /* what the record should say */
    uint8_t *rec = cur ? xmalloc(want + 1) : NULL;
    ssize_t got  = rec ? read(fd, rec, want + 1) : -1;
    close(fd);

    /* Everything before the bitfield — torrent identity and every file's
     * size and mtime — must match byte for byte. */
    size_t hdr = want - (size_t)pm->bf_len;
    int restored = -1;
    if (got == (ssize_t)want && memcmp(rec, cur, hdr) == 0 &&
        get_be(rec + 8, 4) == RESUME_VERSION) {
        restored = 0;
        const uint8_t *bf = rec + hdr;
        for (int i = 0; i < pm->num_pieces; i++) {
            if (!bitfield_has_piece(bf, i)) continue;
            pm->pieces[i].state = PIECE_COMPLETE;
            bitfield_set_piece(pm->our_bitfield, i);
            pm->completed++;
            restored++;
        }
    }
    free(cur);
    free(rec);
    return restored;
}

PieceManager *piece_manager_new(const TorrentInfo *torrent,
                                const char        *out_path,
                                int                use_resume) {
    PieceManager *pm = xcalloc(1, sizeof(PieceManager));
    pm->torrent    = torrent;
    pm->num_pieces = torrent->num_pieces;
    pm->lock_fd    = -1;
    strncpy(pm->out_path, out_path, sizeof(pm->out_path) - 1);

    /* ── Lock file: prevent two simultaneous instances on the same output ── */
    char lock_path[1024 + 8];
    snprintf(lock_path, sizeof(lock_path), "%s.btlock", out_path);

    /* Ensure the parent directory of the lock file exists.
     * For single-file torrents, out_path = "dir/file" — the dir may not
     * exist yet since open_output_fds() hasn't run.  Create it now. */
    {
        char lock_dir[1024 + 8];
        snprintf(lock_dir, sizeof(lock_dir), "%s", lock_path);
        char *slash = strrchr(lock_dir, '/');
        if (slash && slash != lock_dir) {
            *slash = '\0';
            /* mkdir -p via ensure_parent_dir trick: just mkdir the dir */
            struct stat st;
            if (stat(lock_dir, &st) != 0)
                mkdir(lock_dir, 0755);
        }
    }

    pm->lock_fd = open(lock_path, O_RDWR | O_CREAT, 0600);
    if (pm->lock_fd >= 0) {
        struct flock fl = {
            .l_type   = F_WRLCK,
            .l_whence = SEEK_SET,
            .l_start  = 0,
            .l_len    = 0,
        };
        if (fcntl(pm->lock_fd, F_SETLK, &fl) < 0) {
            LOG_ERROR("Cannot lock %s — another btorrent instance is already "
                      "downloading this torrent. Stop it first or use -o to "
                      "choose a different output directory.", lock_path);
            close(pm->lock_fd);
            free(pm);
            return NULL;
        }
        /* Write our PID into the lock file so users can identify the owner */
        char pid_buf[24];
        int  pid_len = snprintf(pid_buf, sizeof(pid_buf), "%d\n", (int)getpid());
        if (ftruncate(pm->lock_fd, 0) == 0) {
            ssize_t _w = write(pm->lock_fd, pid_buf, (size_t)pid_len);
            (void)_w;  /* PID in lock file is informational; ignore write errors */
        }
    } else {
        LOG_WARN("Could not create lock file %s: %s — proceeding without lock",
                 lock_path, strerror(errno));
    }

    pm->pieces = xcalloc((size_t)pm->num_pieces, sizeof(PieceStatus));
    for (int i = 0; i < pm->num_pieces; i++) {
        pm->pieces[i].state        = PIECE_EMPTY;
        pm->pieces[i].piece_length = torrent_get_piece_length(torrent, i);
        pm->pieces[i].num_blocks   = compute_num_blocks(pm->pieces[i].piece_length);
        pm->pieces[i].block_received =
            xcalloc((size_t)pm->pieces[i].num_blocks, sizeof(uint8_t));
    }

    int bf_bytes = (pm->num_pieces + 7) / 8;
    pm->our_bitfield = xcalloc((size_t)bf_bytes, 1);
    pm->bf_len       = bf_bytes;

    pm->file_fds = open_output_fds(pm);
    if (!pm->file_fds) {
        for (int i = 0; i < pm->num_pieces; i++) free(pm->pieces[i].block_received);
        free(pm->pieces); free(pm->our_bitfield); free(pm);
        return NULL;
    }

    LOG_INFO("pieces: output %s (%d file%s, %.2f MB)",
             torrent->is_multi_file ? "directory" : "file",
             torrent->num_files, torrent->num_files == 1 ? "" : "s",
             (double)torrent->total_length / (1024.0 * 1024.0));

    /* Resume: trust a valid fast-resume record if allowed; otherwise verify
     * every piece already on disk by hash. (pread on sparse-file holes
     * returns zeros without I/O, so a fresh download scans quickly.) */
    int resumed = use_resume ? resume_load(pm) : -1;
    if (resumed >= 0)
        LOG_INFO("pieces: fast resume — %d pieces from %s.btresume",
                 resumed, out_path);
    uint8_t *buf = resumed >= 0 ? NULL : xmalloc((size_t)torrent->piece_length);
    if (resumed < 0) resumed = 0;
    for (int i = 0; buf && i < pm->num_pieces; i++) {
        int plen = pm->pieces[i].piece_length;
        if (!piece_manager_read_piece(pm, i, buf)) continue;
        uint8_t hash[20];
        sha1(buf, (size_t)plen, hash);
        if (memcmp(hash, torrent_get_piece_hash(torrent, i), 20) == 0) {
            pm->pieces[i].state = PIECE_COMPLETE;
            pm->completed++;
            resumed++;
            bitfield_set_piece(pm->our_bitfield, i);
        }
    }
    free(buf);

    if (resumed > 0)
        LOG_INFO("pieces: resumed %d/%d pieces", resumed, pm->num_pieces);

    /* Accurately count bytes already on disk (last piece may be shorter) */
    long long resumed_bytes = 0;
    for (int i = 0; i < pm->num_pieces; i++)
        if (pm->pieces[i].state == PIECE_COMPLETE)
            resumed_bytes += pm->pieces[i].piece_length;
    clock_gettime(CLOCK_MONOTONIC, &pm->start_time);
    pm->bytes_at_start = resumed_bytes;
    return pm;
}

void piece_manager_free(PieceManager *pm) {
    if (!pm) return;
    /* Record progress for a fast restart — only if we hold the output lock,
     * i.e. no other instance can be writing these files. */
    if (pm->file_fds && pm->lock_fd >= 0) resume_save(pm);
    if (pm->file_fds) {
        for (int i = 0; i < pm->torrent->num_files; i++)
            if (pm->file_fds[i] >= 0) close(pm->file_fds[i]);
        free(pm->file_fds);
    }
    /* Release and remove the lock file */
    if (pm->lock_fd >= 0) {
        struct flock fl = {
            .l_type   = F_UNLCK,
            .l_whence = SEEK_SET,
            .l_start  = 0,
            .l_len    = 0,
        };
        fcntl(pm->lock_fd, F_SETLK, &fl);
        close(pm->lock_fd);
        /* Remove it — harmless if it's already gone */
        char lock_path[1024 + 8];
        snprintf(lock_path, sizeof(lock_path), "%s.btlock", pm->out_path);
        unlink(lock_path);
    }
    for (int i = 0; i < pm->num_pieces; i++) {
        free(pm->pieces[i].data);
        free(pm->pieces[i].block_received);
    }
    free(pm->pieces);
    free(pm->our_bitfield);
    free(pm);
}

/*
 * rw_range — read or write `len` bytes starting `begin` bytes into a piece,
 * spanning file boundaries as needed. Uses pread/pwrite (positional I/O).
 * Returns 1 if every byte was transferred, 0 otherwise.
 */
static int rw_range(PieceManager *pm, int piece_idx, int begin, int len,
                    uint8_t *buf, int write_mode) {
    const TorrentInfo *t = pm->torrent;
    long long start = (long long)piece_idx * t->piece_length + begin;
    long long end   = start + len;
    int buf_pos = 0;

    for (int fi = 0; fi < t->num_files; fi++) {
        long long file_start = t->files[fi].offset;
        long long file_end   = file_start + t->files[fi].length;
        long long ov_start   = start > file_start ? start : file_start;
        long long ov_end     = end   < file_end   ? end   : file_end;
        if (ov_start >= ov_end) continue;

        off_t file_off  = (off_t)(ov_start - file_start);
        int   chunk_len = (int)(ov_end - ov_start);
        int   fd        = pm->file_fds[fi];
        if (fd < 0) continue;

        if (write_mode) {
            ssize_t written = pwrite(fd, buf + buf_pos, (size_t)chunk_len, file_off);
            if (written != chunk_len) { LOG_WARN("pwrite failed: %s", strerror(errno)); return 0; }
        } else {
            ssize_t got = pread(fd, buf + buf_pos, (size_t)chunk_len, file_off);
            if (got != chunk_len) return 0;
        }
        buf_pos += chunk_len;
    }
    return buf_pos == len;
}

static int rw_piece_multifile(PieceManager *pm, int piece_idx,
                              uint8_t *buf, int write_mode) {
    return rw_range(pm, piece_idx, 0, pm->pieces[piece_idx].piece_length,
                    buf, write_mode);
}

int piece_manager_read_block(PieceManager *pm, int piece_idx,
                             int begin, int len, uint8_t *buf) {
    if (piece_idx < 0 || piece_idx >= pm->num_pieces || begin < 0 || len <= 0 ||
        (long long)begin + len > pm->pieces[piece_idx].piece_length)
        return 0;
    return rw_range(pm, piece_idx, begin, len, buf, 0);
}

int piece_manager_read_piece(PieceManager *pm, int piece_idx, uint8_t *buf) {
    return rw_piece_multifile(pm, piece_idx, buf, 0);
}

static void write_piece(PieceManager *pm, int piece_idx) {
    PieceStatus *ps = &pm->pieces[piece_idx];
    if (!rw_piece_multifile(pm, piece_idx, ps->data, 1)) {
        LOG_ERROR("write failed for piece %d", piece_idx); return;
    }
    free(ps->data); ps->data = NULL;
    ps->state = PIECE_COMPLETE;
    pm->completed++;
    bitfield_set_piece(pm->our_bitfield, piece_idx);
    piece_manager_print_progress(pm);
}

int piece_manager_on_block(PieceManager  *pm,
                           int            piece_idx,
                           int            begin,
                           const uint8_t *data,
                           int            len) {
    if (piece_idx < 0 || piece_idx >= pm->num_pieces) return 0;
    PieceStatus *ps = &pm->pieces[piece_idx];
    if (ps->state == PIECE_COMPLETE) return 1;

    /* Validate before allocating or copying: begin and len come straight
     * off the wire. Blocks must be BLOCK_SIZE-aligned and exactly the size
     * we request (the last block of a piece may be shorter). Using 64-bit
     * arithmetic avoids the signed overflow in begin + len. */
    if (begin < 0 || len <= 0 || begin % BLOCK_SIZE != 0) return 0;
    if ((long long)begin + len > ps->piece_length) return 0;
    int expected = ps->piece_length - begin < BLOCK_SIZE
                 ? ps->piece_length - begin : BLOCK_SIZE;
    if (len != expected) return 0;

    if (ps->state == PIECE_EMPTY || ps->state == PIECE_ASSIGNED) {
        ps->data  = xmalloc((size_t)ps->piece_length);
        ps->state = PIECE_ACTIVE;
    }
    memcpy(ps->data + begin, data, (size_t)len);

    int block_idx = begin / BLOCK_SIZE;
    if (!ps->block_received[block_idx]) {
        ps->block_received[block_idx] = 1;
        ps->blocks_done++;
    }
    if (ps->blocks_done < ps->num_blocks) return 0;

    uint8_t computed[20];
    sha1(ps->data, (size_t)ps->piece_length, computed);
    if (memcmp(computed, torrent_get_piece_hash(pm->torrent, piece_idx), 20) != 0) {
        LOG_WARN("piece %d SHA-1 FAILED — will retry", piece_idx);
        free(ps->data); ps->data = NULL;
        ps->state = PIECE_EMPTY;
        memset(ps->block_received, 0, (size_t)ps->num_blocks);
        ps->blocks_done = 0;
        return -1;
    }
    write_piece(pm, piece_idx);

    /* Record verified bytes in sliding speed window */
    pm->bytes_downloaded += pm->pieces[piece_idx].piece_length;
    {
        int h = pm->spd_head;
        clock_gettime(CLOCK_MONOTONIC, &pm->spd_time[h]);
        pm->spd_bytes[h] = pm->bytes_downloaded;
        pm->spd_head  = (h + 1) % SPEED_SAMPLES;
        if (pm->spd_count < SPEED_SAMPLES) pm->spd_count++;
    }
    return 1;
}

int piece_manager_next_needed(PieceManager  *pm,
                              const uint8_t *peer_bitfield,
                              int            num_pieces) {
    for (int i = 0; i < pm->num_pieces; i++) {
        if (pm->pieces[i].state != PIECE_EMPTY && pm->pieces[i].state != PIECE_ASSIGNED) continue;
        if (peer_bitfield && i < num_pieces &&
            !bitfield_has_piece(peer_bitfield, i)) continue;
        return i;
    }
    return -1;
}

int piece_manager_is_complete(const PieceManager *pm) {
    return pm->completed == pm->num_pieces;
}

void piece_manager_print_progress(const PieceManager *pm) {
    int total  = pm->num_pieces;
    int done   = pm->completed;
    int pct    = total > 0 ? (done * 100) / total : 0;

    double speed_kbs = 0.0;
    if (pm->spd_count >= 2) {
        int newest = (pm->spd_head - 1 + SPEED_SAMPLES) % SPEED_SAMPLES;
        int oldest = (pm->spd_head - pm->spd_count + SPEED_SAMPLES) % SPEED_SAMPLES;
        double dt = (pm->spd_time[newest].tv_sec  - pm->spd_time[oldest].tv_sec) +
                    (pm->spd_time[newest].tv_nsec  - pm->spd_time[oldest].tv_nsec) / 1e9;
        long long db = pm->spd_bytes[newest] - pm->spd_bytes[oldest];
        if (dt > 0.01) speed_kbs = (double)db / dt / 1024.0;
    }

    long long total_bytes  = pm->torrent->total_length;
    long long done_bytes   = pm->bytes_at_start + pm->bytes_downloaded;
    long long remain_bytes = total_bytes - done_bytes;
    if (remain_bytes < 0) remain_bytes = 0;

    double eta_s = (speed_kbs > 1.0)
        ? (double)remain_bytes / (speed_kbs * 1024.0) : -1.0;

    char speed_str[32];
    if (speed_kbs >= 1024.0)
        snprintf(speed_str, sizeof(speed_str), "%.1f MB/s", speed_kbs / 1024.0);
    else
        snprintf(speed_str, sizeof(speed_str), "%.1f KB/s", speed_kbs);

    char eta_str[32];
    if (eta_s < 0)
        snprintf(eta_str, sizeof(eta_str), "--:--");
    else if (eta_s >= 3600)
        snprintf(eta_str, sizeof(eta_str), "%dh%02dm",
                 (int)(eta_s / 3600), ((int)(eta_s / 60)) % 60);
    else
        snprintf(eta_str, sizeof(eta_str), "%d:%02d",
                 (int)(eta_s / 60), (int)eta_s % 60);

    FILE *out = stdout;
    int   tty = isatty(fileno(stdout));

    if (tty) {
        int bar_w  = 40;
        int filled = total > 0 ? (done * bar_w) / total : 0;
        char bar[256];
        int  pos = 0;
        bar[pos++] = '\r';
        bar[pos++] = '[';
        for (int i = 0; i < bar_w; i++)
            bar[pos++] = (i < filled) ? '#' : '.';
        bar[pos++] = ']';
        pos += snprintf(bar + pos, sizeof(bar) - (size_t)pos,
                        " %3d%% %d/%d  %s  ETA %s   ",
                        pct, done, total, speed_str, eta_str);
        fwrite(bar, 1, (size_t)pos, out);
        fflush(out);
        if (done == total) { fputc('\n', out); fflush(out); }
    } else {
        static int last_pct_logged = -1;
        int milestone = (pct / 5) * 5;
        if (done == total || milestone > last_pct_logged) {
            last_pct_logged = milestone;
            fprintf(out, "[progress] %3d%%  %d/%d pieces  %s  ETA %s\n",
                    pct, done, total, speed_str, eta_str);
            fflush(out);
        }
    }
}
