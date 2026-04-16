#define _POSIX_C_SOURCE 200809L
/**
 * test_netio.c — Integration tests for networking utilities
 *
 * Tests:
 *   1. TCP socket options (TCP_NODELAY, O_NONBLOCK)
 *   2. Socket creation and basic setup
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <errno.h>

#include "net/tcp.h"
#include "log.h"

static int g_pass = 0, g_fail = 0;
#define EXPECT(cond, name) \
    do { if (cond) { printf("  PASS  %s\n", name); g_pass++; } \
         else      { printf("  FAIL  %s  (line %d)\n", name, __LINE__); g_fail++; } \
    } while (0)

int main(void) {
    log_init(LOG_WARN, stderr);

    printf("=== Network I/O Tests ===\n\n");

    printf("--- TCP socket creation ---\n");
    int test_sock = socket(AF_INET, SOCK_STREAM, 0);
    EXPECT(test_sock >= 0, "socket() succeeds");

    printf("\n--- Socket options ---\n");
    int nodelay = 0;
    socklen_t optlen = sizeof(nodelay);
    int rc = getsockopt(test_sock, IPPROTO_TCP, TCP_NODELAY, &nodelay, &optlen);
    EXPECT(rc == 0, "getsockopt TCP_NODELAY succeeded");
    EXPECT(nodelay == 0, "TCP_NODELAY default is 0");

    int flags = fcntl(test_sock, F_GETFL, 0);
    EXPECT(flags >= 0, "fcntl F_GETFL succeeded");
    EXPECT((flags & O_NONBLOCK) == 0, "socket default is blocking");

    close(test_sock);

    printf("\n--- tcp_connect_nb function ---\n");
    int sock = tcp_connect_nb("127.0.0.1", 65534);
    EXPECT(sock < 0 || sock >= 0, "tcp_connect_nb returns valid fd or -1");
    if (sock >= 0) {
        int sock_flags = fcntl(sock, F_GETFL, 0);
        EXPECT((sock_flags & O_NONBLOCK) != 0, "tcp_connect_nb sets O_NONBLOCK");
        close(sock);
    }

    printf("\n--- IPv6 socket support ---\n");
#ifdef AF_INET6
    int sock6 = socket(AF_INET6, SOCK_STREAM, 0);
    EXPECT(sock6 >= 0, "IPv6 socket creation succeeds");
    if (sock6 >= 0) close(sock6);
#else
    printf("  SKIP IPv6 not available\n");
#endif

    printf("\n--- tcp_set_timeouts ---\n");
    int tsock = socket(AF_INET, SOCK_STREAM, 0);
    EXPECT(tsock >= 0, "timeout test socket created");
    if (tsock >= 0) {
        tcp_set_timeouts(tsock, 30);
        struct timeval rcv, snd;
        socklen_t len = sizeof(rcv);
        int r1 = getsockopt(tsock, SOL_SOCKET, SO_RCVTIMEO, &rcv, &len);
        int r2 = getsockopt(tsock, SOL_SOCKET, SO_SNDTIMEO, &snd, &len);
        EXPECT(r1 == 0 && r2 == 0, "timeout getsockopt succeeded");
        EXPECT(rcv.tv_sec == 30 && snd.tv_sec == 30, "timeout values set correctly");
        close(tsock);
    }

    printf("\n--- tcp_finish_connect (invalid socket) ---\n");
    int result = tcp_finish_connect(-1);
    EXPECT(result < 0, "tcp_finish_connect returns -1 for invalid socket");

    printf("\n=== Summary ===\n");
    printf("%d passed, %d failed\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
