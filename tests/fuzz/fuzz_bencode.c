/* fuzz_bencode.c — the bencode parser, used on every untrusted input. */
#include "fuzz.h"
#include "core/bencode.h"

/* Touch every node so the fuzzer sees a well-formed tree. */
static size_t walk(const BencodeNode *n) {
    size_t total = 1;
    switch (n->type) {
    case BENCODE_INT:  break;
    case BENCODE_STR:  total += n->str.len ? n->str.data[n->str.len - 1] : 0; break;
    case BENCODE_LIST:
        for (size_t i = 0; i < n->list.count; i++) total += walk(n->list.items[i]);
        break;
    case BENCODE_DICT:
        for (size_t i = 0; i < n->dict.count; i++) {
            total += walk(n->dict.vals[i]);
            (void)bencode_dict_get(n, n->dict.keys[i]);
        }
        break;
    }
    return total;
}

int LLVMFuzzerInitialize(int *argc, char ***argv) {
    (void)argc; (void)argv;
    fuzz_quiet_logs();
    return 0;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    BencodeNode *root = bencode_parse(data, size);
    if (root) { (void)walk(root); bencode_free(root); }

    size_t used = bencode_parse_ex(data, size, &root);
    if (used > size) __builtin_trap();          /* consumed past the input */
    bencode_free(root);
    return 0;
}
