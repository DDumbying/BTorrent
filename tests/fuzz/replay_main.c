/**
 * replay_main.c — run a fuzz target over files, without libFuzzer.
 *
 *   build/fuzz/replay_<target> FILE_OR_DIR...
 *
 * Lets gcc (or any compiler, with or without sanitizers) replay the seed
 * corpus and saved crash inputs as ordinary regression tests.
 */
#define _POSIX_C_SOURCE 200809L
#include "fuzz.h"
#include <dirent.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static int run_file(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) { perror(path); return -1; }
    fseek(f, 0, SEEK_END);
    long n = ftell(f);
    fseek(f, 0, SEEK_SET);
    uint8_t *buf = malloc(n > 0 ? (size_t)n : 1);
    size_t got = n > 0 ? fread(buf, 1, (size_t)n, f) : 0;
    fclose(f);
    LLVMFuzzerTestOneInput(buf, got);
    free(buf);
    return 0;
}

int main(int argc, char **argv) {
    LLVMFuzzerInitialize(&argc, &argv);
    int files = 0;
    for (int i = 1; i < argc; i++) {
        struct stat st;
        if (stat(argv[i], &st) < 0) { perror(argv[i]); return 1; }
        if (!S_ISDIR(st.st_mode)) { if (run_file(argv[i]) == 0) files++; continue; }
        DIR *d = opendir(argv[i]);
        struct dirent *e;
        while (d && (e = readdir(d))) {
            if (e->d_name[0] == '.') continue;
            char path[4096];
            snprintf(path, sizeof(path), "%s/%s", argv[i], e->d_name);
            if (run_file(path) == 0) files++;
        }
        if (d) closedir(d);
    }
    fprintf(stderr, "replayed %d input%s\n", files, files == 1 ? "" : "s");
    return 0;
}
