#define _POSIX_C_SOURCE 200809L
/**
 * bencode.c — Bencoding Parser (recursive descent)
 */

#include "core/bencode.h"
#include "utils.h"
#include "log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

static BencodeNode *parse_value(BencodeParser *p);

static int peek(BencodeParser *p) {
    return (p->pos >= p->len) ? -1 : p->data[p->pos];
}

static int consume(BencodeParser *p) {
    return (p->pos >= p->len) ? -1 : p->data[p->pos++];
}

static int expect(BencodeParser *p, char expected) {
    int c = consume(p);
    if (c != expected) {
        LOG_WARN("bencode: expected '%c' at pos %zu, got '%c'",
                 expected, p->pos - 1, (char)c);
        return -1;
    }
    return 0;
}

static BencodeNode *new_node(BencodeType type) {
    BencodeNode *n = xcalloc(1, sizeof(BencodeNode));
    n->type = type;
    return n;
}

static BencodeNode *parse_integer(BencodeParser *p) {
    if (expect(p, 'i') < 0) return NULL;
    char buf[32]; int buf_len = 0;
    if (peek(p) == '-') buf[buf_len++] = (char)consume(p);
    while (isdigit(peek(p))) {
        if (buf_len >= (int)(sizeof(buf) - 1)) { LOG_WARN("%s", "bencode: integer too large"); return NULL; }
        buf[buf_len++] = (char)consume(p);
    }
    buf[buf_len] = '\0';
    if (buf_len == 0 || (buf_len == 1 && buf[0] == '-')) {
        LOG_WARN("%s", "bencode: empty integer"); return NULL;
    }
    if (expect(p, 'e') < 0) return NULL;
    BencodeNode *node = new_node(BENCODE_INT);
    node->integer = strtoll(buf, NULL, 10);
    return node;
}

static BencodeNode *parse_string(BencodeParser *p) {
    char len_buf[20]; int len_buf_pos = 0;
    while (isdigit(peek(p))) {
        if (len_buf_pos >= 18) { LOG_WARN("%s", "bencode: string length too large"); return NULL; }
        len_buf[len_buf_pos++] = (char)consume(p);
    }
    len_buf[len_buf_pos] = '\0';
    if (len_buf_pos == 0) {
        LOG_WARN("bencode: expected string length at pos %zu", p->pos); return NULL;
    }
    size_t str_len = (size_t)atol(len_buf);
    if (expect(p, ':') < 0) return NULL;
    if (p->pos + str_len > p->len) {
        LOG_WARN("bencode: string length %zu exceeds buffer", str_len); return NULL;
    }
    BencodeNode *node = new_node(BENCODE_STR);
    node->str.data = (uint8_t *)(p->data + p->pos);
    node->str.len  = str_len;
    p->pos += str_len;
    return node;
}

static BencodeNode *parse_list(BencodeParser *p) {
    if (expect(p, 'l') < 0) return NULL;
    BencodeNode *node = new_node(BENCODE_LIST);
    size_t capacity = 8;
    node->list.items = xmalloc(capacity * sizeof(BencodeNode *));
    node->list.count = 0;
    while (peek(p) != 'e' && peek(p) != -1) {
        BencodeNode *item = parse_value(p);
        if (!item) { bencode_free(node); return NULL; }
        if (node->list.count >= capacity) {
            capacity *= 2;
            void *tmp = realloc(node->list.items, capacity * sizeof(BencodeNode *));
            if (!tmp) { bencode_free(node); return NULL; }
            node->list.items = tmp;
        }
        node->list.items[node->list.count++] = item;
    }
    if (expect(p, 'e') < 0) { bencode_free(node); return NULL; }
    return node;
}

static BencodeNode *parse_dict(BencodeParser *p) {
    if (expect(p, 'd') < 0) return NULL;
    BencodeNode *node = new_node(BENCODE_DICT);
    size_t capacity = 8;
    node->dict.keys  = xmalloc(capacity * sizeof(char *));
    node->dict.vals  = xmalloc(capacity * sizeof(BencodeNode *));
    node->dict.count = 0;
    while (peek(p) != 'e' && peek(p) != -1) {
        if (!isdigit(peek(p))) {
            LOG_WARN("bencode: dict key must be a string at pos %zu", p->pos);
            bencode_free(node); return NULL;
        }
        BencodeNode *key_node = parse_string(p);
        if (!key_node) { bencode_free(node); return NULL; }
        char *key = xmalloc(key_node->str.len + 1);
        memcpy(key, key_node->str.data, key_node->str.len);
        key[key_node->str.len] = '\0';
        free(key_node);
        BencodeNode *val = parse_value(p);
        if (!val) { free(key); bencode_free(node); return NULL; }
        if (node->dict.count >= capacity) {
            capacity *= 2;
            void *tmp_keys = realloc(node->dict.keys, capacity * sizeof(char *));
            void *tmp_vals = realloc(node->dict.vals, capacity * sizeof(BencodeNode *));
            if (!tmp_keys || !tmp_vals) {
                free(tmp_keys);
                free(tmp_vals);
                free(key);
                bencode_free(node);
                return NULL;
            }
            node->dict.keys = tmp_keys;
            node->dict.vals = tmp_vals;
        }
        node->dict.keys[node->dict.count] = key;
        node->dict.vals[node->dict.count] = val;
        node->dict.count++;
    }
    if (expect(p, 'e') < 0) { bencode_free(node); return NULL; }
    return node;
}

static BencodeNode *parse_value(BencodeParser *p) {
    int c = peek(p);
    if (c == -1) { LOG_WARN("%s", "bencode: unexpected end of data"); return NULL; }
    if (c == 'i') return parse_integer(p);
    if (c == 'l') return parse_list(p);
    if (c == 'd') return parse_dict(p);
    if (isdigit(c)) return parse_string(p);
    LOG_WARN("bencode: unexpected character '%c' at pos %zu", (char)c, p->pos);
    return NULL;
}

BencodeNode *bencode_parse(const uint8_t *data, size_t len) {
    BencodeParser p = { .data = data, .len = len, .pos = 0 };
    return parse_value(&p);
}

/* Returns number of bytes consumed, 0 on error. */
size_t bencode_parse_ex(const uint8_t *data, size_t len, BencodeNode **out) {
    BencodeParser p = { .data = data, .len = len, .pos = 0 };
    *out = parse_value(&p);
    return (*out) ? p.pos : 0;
}

void bencode_free(BencodeNode *node) {
    if (!node) return;
    switch (node->type) {
        case BENCODE_INT: case BENCODE_STR: break;
        case BENCODE_LIST:
            for (size_t i = 0; i < node->list.count; i++) bencode_free(node->list.items[i]);
            free(node->list.items); break;
        case BENCODE_DICT:
            for (size_t i = 0; i < node->dict.count; i++) {
                free(node->dict.keys[i]);
                bencode_free(node->dict.vals[i]);
            }
            free(node->dict.keys); free(node->dict.vals); break;
    }
    free(node);
}

BencodeNode *bencode_dict_get(const BencodeNode *node, const char *key) {
    if (!node || node->type != BENCODE_DICT) return NULL;
    for (size_t i = 0; i < node->dict.count; i++)
        if (strcmp(node->dict.keys[i], key) == 0) return node->dict.vals[i];
    return NULL;
}

void bencode_print(const BencodeNode *node, int indent) {
    if (!node) { printf("(null)\n"); return; }
    for (int i = 0; i < indent; i++) printf("  ");
    switch (node->type) {
        case BENCODE_INT:
            printf("INT: %lld\n", node->integer); break;
        case BENCODE_STR: {
            int printable = 1;
            for (size_t i = 0; i < node->str.len && i < 64; i++)
                if (!isprint(node->str.data[i]) && !isspace(node->str.data[i])) { printable = 0; break; }
            printf("STR(%zu): ", node->str.len);
            if (printable && node->str.len < 200)
                printf("\"%.*s\"", (int)node->str.len, node->str.data);
            else {
                printf("<binary, first bytes: ");
                for (size_t i = 0; i < node->str.len && i < 8; i++) printf("%02x ", node->str.data[i]);
                printf("...>");
            }
            printf("\n"); break;
        }
        case BENCODE_LIST:
            printf("LIST (%zu):\n", node->list.count);
            for (size_t i = 0; i < node->list.count; i++) bencode_print(node->list.items[i], indent+1);
            break;
        case BENCODE_DICT:
            printf("DICT (%zu):\n", node->dict.count);
            for (size_t i = 0; i < node->dict.count; i++) {
                for (int j = 0; j < indent+1; j++) printf("  ");
                printf("[%s] =>\n", node->dict.keys[i]);
                bencode_print(node->dict.vals[i], indent+2);
            }
            break;
    }
}
