#pragma once
/**
 * bencode.h — Bencoding Parser
 *
 * Strings point into the input buffer (zero-copy). Keep the input buffer
 * alive for the entire lifetime of the parsed tree.
 */

#include <stddef.h>
#include <stdint.h>

typedef enum { BENCODE_INT, BENCODE_STR, BENCODE_LIST, BENCODE_DICT } BencodeType;

typedef struct BencodeNode BencodeNode;
struct BencodeNode {
    BencodeType type;
    union {
        long long integer;
        struct { uint8_t *data; size_t len; } str;
        struct { BencodeNode **items; size_t count; } list;
        struct { char **keys; BencodeNode **vals; size_t count; } dict;
    };
};

typedef struct { const uint8_t *data; size_t len; size_t pos; } BencodeParser;

/** Parse a bencoded buffer. Returns root node or NULL on error. */
BencodeNode *bencode_parse(const uint8_t *data, size_t len);

/**
 * bencode_parse_ex — parse and also return bytes consumed.
 * Used by torrent.c to find exact boundaries of the info dict for hashing.
 * Returns bytes consumed (> 0) on success, 0 on error.
 */
size_t bencode_parse_ex(const uint8_t *data, size_t len, BencodeNode **out);

void         bencode_free(BencodeNode *node);
BencodeNode *bencode_dict_get(const BencodeNode *node, const char *key);
void         bencode_print(const BencodeNode *node, int indent);
