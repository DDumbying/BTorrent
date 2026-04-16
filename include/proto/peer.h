#pragma once
/**
 * peer.h — Peer Wire Protocol
 */

#include "result.h"
#include <stdint.h>
#include <stddef.h>

typedef enum {
    MSG_KEEPALIVE    = -1,
    MSG_CHOKE        =  0,
    MSG_UNCHOKE      =  1,
    MSG_INTERESTED   =  2,
    MSG_UNINTERESTED =  3,
    MSG_HAVE         =  4,
    MSG_BITFIELD     =  5,
    MSG_REQUEST      =  6,
    MSG_PIECE        =  7,
    MSG_CANCEL       =  8,
} MsgType;

typedef struct {
    MsgType  type;
    uint32_t have_index;
    uint8_t *bitfield;
    uint32_t bitfield_len;
    uint32_t piece_index;
    uint32_t piece_begin;
    uint8_t *piece_data;
    uint32_t piece_data_len;
} PeerMsg;

typedef struct {
    int      sock;
    char     ip[16];
    uint16_t port;
    uint8_t  peer_id[20];
    int      am_choked;
    int      am_interested;
} PeerConn;

/** Result type for peer_connect */
typedef BT_RESULT(PeerConn *) PeerConnResult;

PeerConnResult peer_connect(const char    *ip,
                            uint16_t       port,
                            const uint8_t *info_hash,
                            const uint8_t *peer_id,
                            int            timeout_s);

void   peer_close(PeerConn *conn);

/** Returns BT_OK or BT_ERR_NETWORK / BT_ERR_PROTOCOL */
BtErr  peer_handshake(PeerConn *conn, const uint8_t *info_hash,
                      const uint8_t *our_peer_id);

int    peer_send_interested(PeerConn *conn);
int    peer_send_request(PeerConn *conn, uint32_t index,
                         uint32_t begin, uint32_t length);

int    peer_recv_msg(PeerConn *conn, PeerMsg *msg);
void   peer_msg_free(PeerMsg *msg);

int    bitfield_has_piece(const uint8_t *bitfield, int piece_index);
void   bitfield_set_piece(uint8_t *bitfield, int piece_index);
