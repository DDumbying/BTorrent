#define _POSIX_C_SOURCE 200809L
/**
 * utils.c — Utility helpers
 */

#include "utils.h"
#include "log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>

void *xmalloc(size_t size) {
    void *ptr = malloc(size);
    if (!ptr) { LOG_ERROR("malloc(%zu) failed", size); exit(EXIT_FAILURE); }
    return ptr;
}

void *xcalloc(size_t nmemb, size_t size) {
    void *ptr = calloc(nmemb, size);
    if (!ptr) { LOG_ERROR("%s", "calloc failed"); exit(EXIT_FAILURE); }
    return ptr;
}

char *xstrdup(const char *s) {
    char *copy = strdup(s);
    if (!copy) { LOG_ERROR("%s", "strdup failed"); exit(EXIT_FAILURE); }
    return copy;
}

void random_bytes(uint8_t *out, size_t len) {
    int fd = open("/dev/urandom", O_RDONLY);
    if (fd >= 0) {
        size_t got = 0;
        while (got < len) {
            ssize_t n = read(fd, out + got, len - got);
            if (n <= 0) break;
            got += (size_t)n;
        }
        close(fd);
        if (got == len) return;
        LOG_WARN("%s", "random_bytes: /dev/urandom short read, using rand() fallback");
    }
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    srand((unsigned int)(ts.tv_nsec ^ (unsigned long)ts.tv_sec));
    for (size_t i = 0; i < len; i++)
        out[i] = (uint8_t)(rand() & 0xFF);
}

void print_hex(const uint8_t *data, size_t len) {
    for (size_t i = 0; i < len; i++) {
        printf("%02x", data[i]);
        if (i < len - 1) printf(" ");
    }
    printf("\n");
}

void hex_to_str(const uint8_t *data, size_t len, char *out) {
    for (size_t i = 0; i < len; i++)
        sprintf(out + (i * 2), "%02x", data[i]);
    out[len * 2] = '\0';
}

void url_encode_bytes(const uint8_t *bytes, size_t len, char *out) {
    size_t out_pos = 0;
    for (size_t i = 0; i < len; i++) {
        uint8_t b = bytes[i];
        if (isalnum(b) || b == '-' || b == '_' || b == '.' || b == '~')
            out[out_pos++] = (char)b;
        else
            out_pos += (size_t)sprintf(out + out_pos, "%%%02X", b);
    }
    out[out_pos] = '\0';
}

uint32_t read_uint32_be(const uint8_t *buf) {
    return ((uint32_t)buf[0] << 24) | ((uint32_t)buf[1] << 16) |
           ((uint32_t)buf[2] <<  8) |  (uint32_t)buf[3];
}

void write_uint32_be(uint8_t *buf, uint32_t value) {
    buf[0] = (value >> 24) & 0xFF;
    buf[1] = (value >> 16) & 0xFF;
    buf[2] = (value >>  8) & 0xFF;
    buf[3] = (value      ) & 0xFF;
}

uint16_t read_uint16_be(const uint8_t *buf) {
    return ((uint16_t)buf[0] << 8) | (uint16_t)buf[1];
}
