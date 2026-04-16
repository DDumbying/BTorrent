#pragma once
/**
 * utils.h — Utility helpers
 */

#include <stdint.h>
#include <stddef.h>

void    *xmalloc(size_t size);
void    *xcalloc(size_t nmemb, size_t size);
char    *xstrdup(const char *s);

void     print_hex(const uint8_t *data, size_t len);
void     hex_to_str(const uint8_t *data, size_t len, char *out);

void     url_encode_bytes(const uint8_t *bytes, size_t len, char *out);

uint32_t read_uint32_be(const uint8_t *buf);
void     write_uint32_be(uint8_t *buf, uint32_t value);
uint16_t read_uint16_be(const uint8_t *buf);

/** Safe random bytes — reads from /dev/urandom, falls back to rand(). */
void     random_bytes(uint8_t *out, size_t len);
