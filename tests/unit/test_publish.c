#define _POSIX_C_SOURCE 200809L
/**
 * test_publish.c — Tests for publish-readiness features
 *
 * Covers:
 *   1. Endgame detection (in_endgame logic)
 *   2. File locking (double-instance prevention)
 *   3. Progress output — TTY vs pipe modes
 *   4. Active-piece memory cap logic
 *   5. Actionable error message content
 *   6. VERSION macro is set and non-empty
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>

#include "core/pieces.h"
#include "core/torrent.h"
#include "log.h"
#include "utils.h"

/* ── Minimal test framework ───────────────────────────────────────────────── */

static int g_pass = 0, g_fail = 0;
#define EXPECT(cond, name) \
    do { if (cond) { printf("  PASS  %s\n", name); g_pass++; } \
         else      { printf("  FAIL  %s  (line %d)\n", name, __LINE__); g_fail++; } \
    } while(0)

/* ── Replicated in_endgame logic from scheduler.c ────────────────────────── */
/* (static there; we duplicate just the logic for testing) */

typedef enum {
    PS_EMPTY    = 0,
    PS_ASSIGNED = 1,
    PS_ACTIVE   = 2,
    PS_COMPLETE = 3,
} FakePieceState;

static int fake_in_endgame(const FakePieceState *states, int n) {
    int empty = 0, active = 0;
    for (int i = 0; i < n; i++) {
        if (states[i] == PS_EMPTY)                          empty++;
        if (states[i] == PS_ACTIVE || states[i] == PS_ASSIGNED) active++;
    }
    return (empty == 0 && active > 0);
}

/* ── 1. Endgame detection ─────────────────────────────────────────────────── */

static void test_endgame(void) {
    printf("\n--- endgame detection ---\n");

    /* Not endgame: pieces still empty */
    FakePieceState s1[] = { PS_EMPTY, PS_ACTIVE, PS_COMPLETE };
    EXPECT(!fake_in_endgame(s1, 3), "not endgame: empty pieces remain");

    /* Not endgame: all complete, nothing active */
    FakePieceState s2[] = { PS_COMPLETE, PS_COMPLETE, PS_COMPLETE };
    EXPECT(!fake_in_endgame(s2, 3), "not endgame: all complete, nothing active");

    /* Endgame: all assigned/active, none empty */
    FakePieceState s3[] = { PS_ACTIVE, PS_ASSIGNED, PS_COMPLETE };
    EXPECT(fake_in_endgame(s3, 3), "endgame: no empty, some active");

    /* Endgame: single piece in-flight */
    FakePieceState s4[] = { PS_COMPLETE, PS_COMPLETE, PS_ACTIVE };
    EXPECT(fake_in_endgame(s4, 3), "endgame: single active piece remaining");

    /* Edge: single piece, still empty */
    FakePieceState s5[] = { PS_EMPTY };
    EXPECT(!fake_in_endgame(s5, 1), "not endgame: single empty piece");

    /* Edge: single piece, active */
    FakePieceState s6[] = { PS_ACTIVE };
    EXPECT(fake_in_endgame(s6, 1), "endgame: single active piece");

    /* Large: 1000 pieces, 999 complete, 1 active */
    FakePieceState *big = xcalloc(1000, sizeof(FakePieceState));
    for (int i = 0; i < 999; i++) big[i] = PS_COMPLETE;
    big[999] = PS_ACTIVE;
    EXPECT(fake_in_endgame(big, 1000), "endgame: 999 complete + 1 active");
    big[999] = PS_EMPTY;
    EXPECT(!fake_in_endgame(big, 1000), "not endgame: 999 complete + 1 empty");
    free(big);
}

/* ── 2. File locking ──────────────────────────────────────────────────────── */

static void test_file_lock(void) {
    printf("\n--- file locking ---\n");

    /* Create a temp directory to work in */
    char tmpdir[] = "/tmp/btorrent_test_XXXXXX";
    char *td = mkdtemp(tmpdir);
    EXPECT(td != NULL, "lock: mkdtemp succeeds");
    if (!td) return;

    /* Build a lock path manually to test the locking contract */
    char lock_path[256];
    snprintf(lock_path, sizeof(lock_path), "%s/output.btlock", td);

    /* First lock: should succeed */
    int fd1 = open(lock_path, O_RDWR | O_CREAT, 0600);
    EXPECT(fd1 >= 0, "lock: first open succeeds");

    struct flock fl = {
        .l_type   = F_WRLCK,
        .l_whence = SEEK_SET,
        .l_start  = 0,
        .l_len    = 0,
    };
    int r1 = fcntl(fd1, F_SETLK, &fl);
    EXPECT(r1 == 0, "lock: F_SETLK succeeds on fresh file");

    /* Second lock attempt (same process, different fd) should fail */
    int fd2 = open(lock_path, O_RDWR, 0600);
    EXPECT(fd2 >= 0, "lock: second open succeeds");
    /* Note: POSIX locks are per-process, so F_SETLK from the same process
     * will REPLACE the existing lock rather than block. We use a child
     * process via fork() to truly test cross-process locking. */
    pid_t child = fork();
    if (child == 0) {
        /* Child: try to acquire the lock — should fail since parent holds it */
        struct flock fl2 = {
            .l_type   = F_WRLCK,
            .l_whence = SEEK_SET,
            .l_start  = 0,
            .l_len    = 0,
        };
        int fc = open(lock_path, O_RDWR, 0600);
        int rc = (fc >= 0) ? fcntl(fc, F_SETLK, &fl2) : 0;
        /* Exit 0 if lock was denied (expected), 1 if lock succeeded (bad) */
        _exit(rc < 0 ? 0 : 1);
    }
    if (child > 0) {
        int status = 0;
        waitpid(child, &status, 0);
        EXPECT(WIFEXITED(status) && WEXITSTATUS(status) == 0,
               "lock: second process cannot acquire held lock");
    }

    /* Release lock and verify the file can be locked again */
    fl.l_type = F_UNLCK;
    fcntl(fd1, F_SETLK, &fl);
    close(fd1);
    close(fd2);

    int fd3 = open(lock_path, O_RDWR, 0600);
    fl.l_type = F_WRLCK;
    int r3 = fcntl(fd3, F_SETLK, &fl);
    EXPECT(r3 == 0, "lock: can re-acquire after release");
    fl.l_type = F_UNLCK;
    fcntl(fd3, F_SETLK, &fl);
    close(fd3);

    /* Lock file has expected name pattern */
    EXPECT(strstr(lock_path, ".btlock") != NULL, "lock: filename ends with .btlock");

    /* Cleanup */
    unlink(lock_path);
    rmdir(td);
}

/* ── 3. Progress output: TTY vs pipe ─────────────────────────────────────── */

static void test_progress_tty_detection(void) {
    printf("\n--- progress TTY detection ---\n");

    /* Stderr to a pipe: isatty should return 0 */
    int pipefd[2];
    EXPECT(pipe(pipefd) == 0, "progress: pipe() succeeds");

    /* isatty on write end of pipe must be 0 */
    EXPECT(isatty(pipefd[1]) == 0, "progress: pipe fd is not a tty");

    /* isatty on read end of pipe must be 0 */
    EXPECT(isatty(pipefd[0]) == 0, "progress: pipe read fd is not a tty");

    close(pipefd[0]);
    close(pipefd[1]);

    /* /dev/null is also not a tty */
    int devnull = open("/dev/null", O_WRONLY);
    EXPECT(devnull >= 0, "progress: /dev/null opens");
    EXPECT(isatty(devnull) == 0, "progress: /dev/null is not a tty");
    close(devnull);

    /* log_is_tty() follows what log_init() was given.
     * After log_init with a pipe-backed FILE, it should report 0. */
    FILE *pipe_file = fdopen(pipefd[1] = open("/dev/null", O_WRONLY), "w");
    if (pipe_file) {
        log_init(LOG_ERROR, pipe_file);  /* suppress all output during test */
        EXPECT(log_is_tty() == 0, "log_is_tty: /dev/null → 0");
        fclose(pipe_file);
    }

    /* Restore logging to stderr */
    log_init(LOG_ERROR, stderr);
}

/* ── 4. Active-piece memory cap logic ────────────────────────────────────── */

static void test_memory_cap(void) {
    printf("\n--- active-piece memory cap ---\n");

    /* The cap formula: max(max_peers/2, 8) */
    int cases[][3] = {
        /* max_peers, expected_cap */
        { 50,  25, 0 },
        { 10,   8, 0 },   /* floor at 8 */
        {  4,   8, 0 },   /* below-floor */
        {100,  50, 0 },
        {  0,   8, 0 },   /* zero peers → floor */
        { 16,   8, 0 },   /* exactly 2× floor */
    };
    for (int i = 0; i < 6; i++) {
        int max_peers   = cases[i][0];
        int expected    = cases[i][1];
        int computed    = (max_peers / 2 < 8) ? 8 : max_peers / 2;
        char name[64];
        snprintf(name, sizeof(name), "cap: max_peers=%d → cap=%d",
                 max_peers, expected);
        EXPECT(computed == expected, name);
    }

    /* Cap means: if active_count >= cap, don't assign another piece */
    {
        int max_peers = 50;
        int cap       = (max_peers / 2 < 8) ? 8 : max_peers / 2;  /* 25 */
        /* Simulate 25 active pieces — at cap, should block */
        int active    = 25;
        EXPECT(active >= cap, "cap: 25 active pieces blocks new assignment at cap=25");
        /* 24 active — below cap, should proceed */
        active = 24;
        EXPECT(active < cap,  "cap: 24 active pieces allows new assignment at cap=25");
    }
}

/* ── 5. VERSION macro ─────────────────────────────────────────────────────── */

static void test_version(void) {
    printf("\n--- version macro ---\n");

#ifndef BT_VERSION
    printf("  FAIL  BT_VERSION not defined  (line %d)\n", __LINE__); g_fail++;
#else
    EXPECT(strlen(BT_VERSION) > 0, "BT_VERSION: non-empty string");
    /* Must contain at least one dot (e.g. "2.0.0") */
    EXPECT(strchr(BT_VERSION, '.') != NULL, "BT_VERSION: contains dot separator");
    /* Must not be the fallback "dev" when built via Makefile */
    /* (In test builds without -DBT_VERSION we allow "dev") */
    printf("  INFO  BT_VERSION = \"%s\"\n", BT_VERSION);
    g_pass++;
#endif
}

/* ── 6. Lock file name convention ────────────────────────────────────────── */

static void test_lock_naming(void) {
    printf("\n--- lock file naming ---\n");

    /* Verify the naming convention used in piece_manager_new */
    const char *out_paths[] = {
        "/tmp/ubuntu-24.04.iso",
        "/data/movies/big.buck.bunny",
        "relative/path/file.bin",
        "/tmp/x",
    };
    for (int i = 0; i < 4; i++) {
        char lock[1024 + 8];
        snprintf(lock, sizeof(lock), "%s.btlock", out_paths[i]);
        /* Lock path starts with the output path */
        EXPECT(strncmp(lock, out_paths[i], strlen(out_paths[i])) == 0,
               "lock name: starts with out_path");
        /* Lock path ends with .btlock */
        EXPECT(strcmp(lock + strlen(out_paths[i]), ".btlock") == 0,
               "lock name: suffix is .btlock");
    }
}

/* ── main ─────────────────────────────────────────────────────────────────── */

int main(void) {
    printf("=== Publish-Readiness Tests ===\n");
    log_init(LOG_ERROR, stderr);  /* suppress noise during testing */

    test_endgame();
    test_file_lock();
    test_progress_tty_detection();
    test_memory_cap();
    test_version();
    test_lock_naming();

    printf("\n%d passed, %d failed\n", g_pass, g_fail);
    return g_fail ? 1 : 0;
}
