/*
 * fuzz_wire.c — the peer wire protocol as handled by the scheduler.
 *
 * Includes scheduler.c to drive one real session. Each input:
 *   byte 0      selects the session's starting state:
 *                 bit 0    0 = we connected out, 1 = peer connected in
 *                 bit 1    0 = mid-handshake,    1 = handshake done
 *   bytes 1..   are what the peer sends; they arrive on a socketpair and go
 *               through handle_session() (framing, dispatch, piece/block
 *               handling, PEX, requests) and then the upload queue.
 *
 * The torrent has 6 pieces of 32 KiB (last one shorter), all zeros, with
 * info_hash = 20 x 'I'. Pieces 0-2 are complete (servable), piece 3 is
 * assigned to the session, 4-5 are wanted — so a fuzzed peer can finish
 * pieces, fail hashes, request blocks, and so on.
 */
#include "../../src/scheduler.c"   /* first: it sets feature macros */
#include "fuzz.h"
#include "core/sha1.h"
#include <sys/socket.h>

#define NPIECES   6
#define PLEN      (2 * BLOCK_SIZE)
#define LAST_PLEN 20000

static TorrentInfo  *g_torrent;
static PieceManager *g_pm;
static int           g_epfd;
static const uint8_t g_peer_id[21] = "-FZ0001-000000000000";  /* 20 + NUL */
static char          g_path[64];

static void reset_pieces(void) {
    memset(g_pm->our_bitfield, 0, (size_t)g_pm->bf_len);
    g_pm->completed = 0;
    for (int i = 0; i < NPIECES; i++) {
        PieceStatus *ps = &g_pm->pieces[i];
        free(ps->data);
        ps->data = NULL;
        memset(ps->block_received, 0, (size_t)ps->num_blocks);
        ps->blocks_done = 0;
        ps->state = i < 3 ? PIECE_COMPLETE : PIECE_EMPTY;
        if (i < 3) { bitfield_set_piece(g_pm->our_bitfield, i); g_pm->completed++; }
    }
    g_pm->bytes_downloaded = 0;
    g_pm->spd_count = g_pm->spd_head = 0;
}

static void remove_files(void) {
    char lock[80];
    snprintf(lock, sizeof(lock), "%s.btlock", g_path);
    unlink(g_path);
    unlink(lock);
}

int LLVMFuzzerInitialize(int *argc, char ***argv) {
    (void)argc; (void)argv;
    fuzz_quiet_logs();
    if (!freopen("/dev/null", "w", stdout)) return 0;   /* progress bar */

    g_torrent = xcalloc(1, sizeof(TorrentInfo));
    TorrentInfo *t = g_torrent;
    snprintf(t->name, sizeof(t->name), "fuzz");
    memset(t->info_hash, 'I', 20);
    t->piece_length = PLEN;
    t->num_pieces   = NPIECES;
    t->total_length = (long)PLEN * (NPIECES - 1) + LAST_PLEN;
    t->num_files    = 1;
    t->files[0].length = t->total_length;
    t->pieces_hash  = xmalloc(20 * NPIECES);
    uint8_t *zeros  = xcalloc(PLEN, 1);
    for (int i = 0; i < NPIECES; i++)
        sha1(zeros, (size_t)(i == NPIECES - 1 ? LAST_PLEN : PLEN),
             t->pieces_hash + 20 * i);
    free(zeros);

    /* Output file of zeros, so every piece verifies and can be served. */
    snprintf(g_path, sizeof(g_path), "/tmp/btorrent_fuzz_wire_%d.bin", (int)getpid());
    remove_files();
    int fd = open(g_path, O_RDWR | O_CREAT | O_TRUNC, 0600);
    if (fd < 0 || ftruncate(fd, t->total_length) < 0) abort();
    close(fd);
    g_pm = piece_manager_new(t, g_path, 0);
    if (!g_pm) abort();
    atexit(remove_files);

    g_epfd = epoll_create1(EPOLL_CLOEXEC);
    return 0;
}

static void drain(int fd) {
    uint8_t buf[65536];
    while (recv(fd, buf, sizeof(buf), MSG_DONTWAIT) > 0) {}
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size < 1) return 0;
    uint8_t mode = data[0];
    data++; size--;
    if (size > 65536) size = 65536;

    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) return 0;
    for (int i = 0; i < 2; i++)
        fcntl(sv[i], F_SETFL, fcntl(sv[i], F_GETFL) | O_NONBLOCK);

    reset_pieces();
    Config cfg = { .max_peers = 2, .pipeline_depth = 4, .peer_timeout_s = 5 };
    PeerList peers = { NULL, 0, 1800 };
    TokenBucket tb;
    tb_init(&tb, 0);
    long long uploaded = 0;

    Session sessions[2];
    memset(sessions, 0, sizeof(sessions));
    for (int i = 0; i < 2; i++) { sessions[i].sock = -1; sessions[i].phase = PS_DEAD; }
    Session *s = &sessions[0];
    int incoming  = mode & 1;
    int handshook = (mode >> 1) & 1;
    session_init(s, sv[0], "10.0.0.1", 6881, incoming);

    int bf_bytes = (NPIECES + 7) / 8;
    if (handshook) {
        s->peer_bitfield = xcalloc((size_t)bf_bytes, 1);
        s->bf_len = bf_bytes;
        if (incoming) {
            s->phase = PS_SEED_READY;
            s->peer_choked = 0;
        } else {
            s->phase      = PS_DOWNLOADING;
            s->am_choked  = 0;
            s->piece_idx  = 3;
            s->piece_len  = PLEN;
            s->num_blocks = 2;
            s->blocks_sent = 2;
            s->last_block = time(NULL);
            g_pm->pieces[3].state = PIECE_ASSIGNED;
        }
    } else if (!incoming) {
        s->phase = PS_HANDSHAKE;
        s->peer_bitfield = xcalloc((size_t)bf_bytes, 1);
        s->bf_len = bf_bytes;
    }

    if (size > 0 && write(sv[1], data, size) < 0) { /* buffer full: fine */ }

    handle_session(s, EPOLLIN, g_epfd, 0, g_torrent, g_pm,
                   g_torrent->info_hash, g_peer_id, sessions, 2, &cfg, &peers);
    drain(sv[1]);
    serve_pending(sessions, 2, g_pm, g_torrent, &tb, &uploaded);
    drain(sv[1]);

    if (s->sock >= 0) session_drop(s, g_pm, sessions, 2, g_epfd);
    else if (s->rbuf.data) rbuf_free(&s->rbuf);
    free(s->peer_bitfield);
    s->peer_bitfield = NULL;
    close(sv[1]);
    peer_list_free(&peers);
    return 0;
}
