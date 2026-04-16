#define _POSIX_C_SOURCE 200809L
/**
 * peer.c — Peer Wire Protocol
 *
 * Changes from v1:
 *   - All fprintf/printf replaced with LOG_* macros.
 *   - peer_connect / peer_handshake return BtErr via PeerConnResult.
 *   - bitfield_set_piece helper extracted (was duplicated in main.c).
 *   - TCP_NODELAY set on socket (reduces latency for small request msgs).
 */

#include "proto/peer.h"
#include "utils.h"
#include "log.h"
#include "result.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

#define HANDSHAKE_SIZE 68
#define PSTR           "BitTorrent protocol"
#define PSTRLEN        19

static int send_all(int sock, const uint8_t *buf, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(sock, buf + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0) { if (n < 0) LOG_DEBUG("send: %s", strerror(errno)); return -1; }
        sent += (size_t)n;
    }
    return 0;
}

static int recv_all(int sock, uint8_t *buf, size_t len) {
    size_t received = 0;
    while (received < len) {
        ssize_t n = recv(sock, buf + received, len - received, 0);
        if (n <= 0) { if (n < 0 && errno != EINTR) LOG_DEBUG("recv: %s", strerror(errno)); return -1; }
        received += (size_t)n;
    }
    return 0;
}

PeerConnResult peer_connect(const char    *ip,
                            uint16_t       port,
                            const uint8_t *info_hash,
                            const uint8_t *peer_id,
                            int            timeout_s) {
    (void)info_hash; (void)peer_id;

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0)
        return (PeerConnResult){ .value = NULL, .err = BT_ERR_NETWORK };

    /* Timeouts */
    struct timeval tv = { .tv_sec = (timeout_s > 0 ? timeout_s : 5), .tv_usec = 0 };
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    /* TCP_NODELAY: disable Nagle — request msgs are tiny (17 bytes) */
    int flag = 1;
    setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) <= 0) {
        LOG_DEBUG("peer: invalid IP: %s", ip);
        close(sock);
        return (PeerConnResult){ .value = NULL, .err = BT_ERR_ARGS };
    }

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(sock);
        return (PeerConnResult){ .value = NULL, .err = BT_ERR_NETWORK };
    }

    PeerConn *conn = xcalloc(1, sizeof(PeerConn));
    conn->sock          = sock;
    conn->am_choked     = 1;
    conn->am_interested = 0;
    strncpy(conn->ip, ip, sizeof(conn->ip) - 1);
    conn->port = port;
    return (PeerConnResult){ .value = conn, .err = BT_OK };
}

void peer_close(PeerConn *conn) {
    if (!conn) return;
    close(conn->sock);
    free(conn);
}

BtErr peer_handshake(PeerConn *conn, const uint8_t *info_hash,
                     const uint8_t *our_peer_id) {
    uint8_t our_hs[HANDSHAKE_SIZE];
    our_hs[0] = PSTRLEN;
    memcpy(our_hs + 1,  PSTR,      PSTRLEN);
    memset(our_hs + 20, 0,         8);
    memcpy(our_hs + 28, info_hash, 20);
    memcpy(our_hs + 48, our_peer_id, 20);

    if (send_all(conn->sock, our_hs, HANDSHAKE_SIZE) < 0) {
        LOG_DEBUG("peer %s:%d: handshake send failed", conn->ip, conn->port);
        return BT_ERR_NETWORK;
    }

    uint8_t their_hs[HANDSHAKE_SIZE];
    if (recv_all(conn->sock, their_hs, HANDSHAKE_SIZE) < 0) {
        LOG_DEBUG("peer %s:%d: handshake recv failed", conn->ip, conn->port);
        return BT_ERR_NETWORK;
    }

    if (their_hs[0] != PSTRLEN || memcmp(their_hs + 1, PSTR, PSTRLEN) != 0) {
        LOG_DEBUG("peer %s:%d: bad protocol string", conn->ip, conn->port);
        return BT_ERR_PROTOCOL;
    }

    if (memcmp(their_hs + 28, info_hash, 20) != 0) {
        LOG_WARN("peer %s:%d: info_hash mismatch", conn->ip, conn->port);
        return BT_ERR_PROTOCOL;
    }

    memcpy(conn->peer_id, their_hs + 48, 20);
    LOG_DEBUG("peer %s:%d: handshake OK", conn->ip, conn->port);
    return BT_OK;
}

static int send_simple_msg(PeerConn *conn, uint8_t msg_id) {
    uint8_t buf[5];
    write_uint32_be(buf, 1);
    buf[4] = msg_id;
    return send_all(conn->sock, buf, 5);
}

int peer_send_interested(PeerConn *conn) {
    LOG_DEBUG("peer %s:%d: sending interested", conn->ip, conn->port);
    conn->am_interested = 1;
    return send_simple_msg(conn, MSG_INTERESTED);
}

int peer_send_request(PeerConn *conn, uint32_t index,
                      uint32_t begin, uint32_t length) {
    uint8_t buf[17];
    write_uint32_be(buf,      13);
    buf[4] = MSG_REQUEST;
    write_uint32_be(buf + 5,  index);
    write_uint32_be(buf + 9,  begin);
    write_uint32_be(buf + 13, length);
    return send_all(conn->sock, buf, 17);
}

int peer_recv_msg(PeerConn *conn, PeerMsg *msg) {
    memset(msg, 0, sizeof(PeerMsg));

    uint8_t len_buf[4];
    if (recv_all(conn->sock, len_buf, 4) < 0) return -1;
    uint32_t msg_len = read_uint32_be(len_buf);

    if (msg_len == 0) { msg->type = MSG_KEEPALIVE; return 0; }

    if (msg_len > 16 * 1024 * 1024) {
        LOG_WARN("peer %s:%d: message too large: %u bytes",
                 conn->ip, conn->port, msg_len);
        return -1;
    }

    uint8_t id;
    if (recv_all(conn->sock, &id, 1) < 0) return -1;
    msg->type = (MsgType)id;
    uint32_t payload_len = msg_len - 1;

    switch (id) {
        case MSG_CHOKE:
        case MSG_UNCHOKE:
        case MSG_INTERESTED:
        case MSG_UNINTERESTED:
            if (id == MSG_CHOKE)   conn->am_choked = 1;
            if (id == MSG_UNCHOKE) conn->am_choked = 0;
            break;

        case MSG_HAVE:
            if (payload_len != 4) return -1;
            { uint8_t buf[4]; if (recv_all(conn->sock, buf, 4) < 0) return -1;
              msg->have_index = read_uint32_be(buf); }
            break;

        case MSG_BITFIELD:
            msg->bitfield     = xmalloc(payload_len);
            msg->bitfield_len = payload_len;
            if (recv_all(conn->sock, msg->bitfield, payload_len) < 0) {
                free(msg->bitfield); msg->bitfield = NULL; return -1;
            }
            break;

        case MSG_REQUEST:
        case MSG_CANCEL:
            { uint8_t buf[12]; if (recv_all(conn->sock, buf, 12) < 0) return -1; }
            break;

        case MSG_PIECE:
            if (payload_len < 8) return -1;
            { uint8_t header[8];
              if (recv_all(conn->sock, header, 8) < 0) return -1;
              msg->piece_index    = read_uint32_be(header);
              msg->piece_begin    = read_uint32_be(header + 4);
              msg->piece_data_len = payload_len - 8;
              msg->piece_data     = xmalloc(msg->piece_data_len);
              if (recv_all(conn->sock, msg->piece_data, msg->piece_data_len) < 0) {
                  free(msg->piece_data); msg->piece_data = NULL; return -1;
              }
            }
            break;

        default:
            LOG_DEBUG("peer %s:%d: unknown msg id %d, skipping %u bytes",
                      conn->ip, conn->port, id, payload_len);
            if (payload_len > 0) {
                uint8_t *discard = xmalloc(payload_len);
                recv_all(conn->sock, discard, payload_len);
                free(discard);
            }
            break;
    }
    return 0;
}

void peer_msg_free(PeerMsg *msg) {
    if (!msg) return;
    free(msg->bitfield);
    free(msg->piece_data);
    msg->bitfield   = NULL;
    msg->piece_data = NULL;
}

int bitfield_has_piece(const uint8_t *bitfield, int piece_index) {
    int byte_idx = piece_index / 8;
    int bit_idx  = 7 - (piece_index % 8);
    return (bitfield[byte_idx] >> bit_idx) & 1;
}

/* Extracted from main.c where it was duplicated */
void bitfield_set_piece(uint8_t *bitfield, int piece_index) {
    bitfield[piece_index / 8] |= (uint8_t)(0x80 >> (piece_index % 8));
}
