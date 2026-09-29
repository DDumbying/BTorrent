#pragma once
/**
 * fuzz.h — shared setup for the fuzz targets in tests/fuzz/
 *
 * Each target defines LLVMFuzzerInitialize() and LLVMFuzzerTestOneInput().
 * Built with clang -fsanitize=fuzzer (make fuzz) they are libFuzzer
 * binaries; linked with replay_main.c (make fuzz-replay) any compiler can
 * replay the corpus as a regression test.
 */
#include "log.h"
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

int LLVMFuzzerInitialize(int *argc, char ***argv);
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

/* Keep logging on (so its format paths are exercised) but out of the way. */
static inline void fuzz_quiet_logs(void) {
    FILE *devnull = fopen("/dev/null", "w");
    log_init(LOG_DEBUG, devnull ? devnull : stderr);
}
