#pragma once
/**
 * result.h — Unified error type
 *
 * Every function that can fail returns BtErr (BT_OK = 0 on success).
 * Use bt_strerror() to get a human-readable description.
 *
 * For functions that also return a value, use the BT_RESULT macro:
 *
 *   typedef BT_RESULT(PeerConn *) PeerConnResult;
 *   PeerConnResult r = peer_connect(...);
 *   if (r.err) { LOG_WARN(...); return r.err; }
 *   PeerConn *conn = r.value;
 */

typedef enum {
    BT_OK           = 0,
    BT_ERR_IO       = 1,   /* file / socket read-write failure      */
    BT_ERR_PARSE    = 2,   /* malformed bencode / torrent data       */
    BT_ERR_HASH     = 3,   /* SHA-1 mismatch on piece               */
    BT_ERR_NETWORK  = 4,   /* DNS, connect, timeout                  */
    BT_ERR_PROTOCOL = 5,   /* peer sent unexpected / invalid data    */
    BT_ERR_ALLOC    = 6,   /* out of memory                          */
    BT_ERR_ARGS     = 7,   /* bad function arguments                 */
    BT_ERR_CHOKED   = 8,   /* peer choked us before we could request */
    BT_ERR_NO_PIECES= 9,   /* peer has nothing we need               */
    BT_ERR_TRACKER  = 10,  /* tracker returned error or 0 peers      */
} BtErr;

/* Pair a value with an error code */
#define BT_RESULT(T) struct { T value; BtErr err; }

/* Convenience constructors — use inside a compound-literal cast */
#define BT_OK_VAL(T, v)  ((__typeof__(BT_RESULT(T))){ .value = (v), .err = BT_OK })
#define BT_ERR_VAL(T, e) ((__typeof__(BT_RESULT(T))){ .value = 0,   .err = (e)  })

const char *bt_strerror(BtErr err);
