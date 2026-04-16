#pragma once
/**
 * net/tcp.h — Non-blocking TCP helpers
 *
 * Thin wrappers around POSIX sockets for use by the epoll scheduler.
 * Blocking peer_connect() in peer.c is kept for the test suite; this
 * module is used only by scheduler.c.
 */

#include <stdint.h>

/**
 * tcp_connect_nb — start a non-blocking TCP connect.
 *
 * Returns a socket fd with O_NONBLOCK set.
 * connect() will return -1 with errno == EINPROGRESS — that is expected.
 * The caller adds the fd to epoll(EPOLLOUT) and waits for writability,
 * then calls tcp_finish_connect() to confirm the connection succeeded.
 *
 * Returns -1 on immediate hard failure (bad IP, out of fds, etc.).
 */
int tcp_connect_nb(const char *ip, uint16_t port);

int tcp_connect_nb_ipv6(const char *ip, uint16_t port);

/**
 * tcp_finish_connect — called after epoll reports EPOLLOUT on a connecting fd.
 *
 * Checks SO_ERROR to confirm the connect() completed successfully.
 * Returns 0 on success, -1 on failure (peer refused / timed out).
 */
int tcp_finish_connect(int sock);

/**
 * tcp_set_timeouts — set SO_RCVTIMEO / SO_SNDTIMEO on a connected socket.
 */
void tcp_set_timeouts(int sock, int seconds);
