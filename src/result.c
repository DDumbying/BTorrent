/**
 * result.c — bt_strerror implementation
 */
#include "result.h"

const char *bt_strerror(BtErr err) {
    switch (err) {
        case BT_OK:            return "ok";
        case BT_ERR_IO:        return "I/O error";
        case BT_ERR_PARSE:     return "parse error";
        case BT_ERR_HASH:      return "SHA-1 mismatch";
        case BT_ERR_NETWORK:   return "network error";
        case BT_ERR_PROTOCOL:  return "protocol error";
        case BT_ERR_ALLOC:     return "out of memory";
        case BT_ERR_ARGS:      return "invalid arguments";
        case BT_ERR_CHOKED:    return "peer choked us";
        case BT_ERR_NO_PIECES: return "peer has no needed pieces";
        case BT_ERR_TRACKER:   return "tracker error";
        default:               return "unknown error";
    }
}
