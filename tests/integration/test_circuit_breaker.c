#define _POSIX_C_SOURCE 200809L
/**
 * test_circuit_breaker.c — Tests for circuit breaker pattern
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "log.h"

#define CIRCUIT_BREAKER_THRESHOLD 3
#define CIRCUIT_BREAKER_TIMEOUT_S 30

typedef struct {
    int        consecutive_failures;
    time_t     circuit_open_until;
    char       ip[16];
} Session;

static int is_circuit_open(Session *sessions, int max_s,
                           const char *ip, time_t now) {
    for (int i = 0; i < max_s; i++) {
        if (sessions[i].consecutive_failures >= CIRCUIT_BREAKER_THRESHOLD &&
            now < sessions[i].circuit_open_until) {
            if (strcmp(sessions[i].ip, ip) == 0) return 1;
        }
    }
    return 0;
}

static void record_failure(Session *s, time_t now) {
    s->consecutive_failures++;
    if (s->consecutive_failures >= CIRCUIT_BREAKER_THRESHOLD) {
        s->circuit_open_until = now + CIRCUIT_BREAKER_TIMEOUT_S;
    }
}

static void record_success(Session *s) {
    s->consecutive_failures = 0;
    s->circuit_open_until = 0;
}

static int g_pass = 0, g_fail = 0;
#define EXPECT(cond, name) \
    do { if (cond) { printf("  PASS  %s\n", name); g_pass++; } \
         else      { printf("  FAIL  %s  (line %d)\n", name, __LINE__); g_fail++; } \
    } while (0)

int main(void) {
    log_init(LOG_WARN, stderr);

    printf("=== Circuit Breaker Tests ===\n\n");

    printf("--- Circuit closed initially ---\n");
    Session sessions[5];
    memset(sessions, 0, sizeof(sessions));
    strcpy(sessions[0].ip, "192.168.1.1");
    strcpy(sessions[1].ip, "192.168.1.2");
    time_t now = time(NULL);
    EXPECT(!is_circuit_open(sessions, 5, "192.168.1.1", now),
           "circuit closed for new session");

    printf("\n--- Failure threshold opens circuit ---\n");
    record_failure(&sessions[0], now);
    EXPECT(!is_circuit_open(sessions, 1, "192.168.1.1", now),
           "circuit closed after 1 failure");
    record_failure(&sessions[0], now);
    EXPECT(!is_circuit_open(sessions, 1, "192.168.1.1", now),
           "circuit closed after 2 failures");
    record_failure(&sessions[0], now);
    EXPECT(is_circuit_open(sessions, 1, "192.168.1.1", now),
           "circuit OPEN after 3 failures");

    printf("\n--- Different IPs have separate circuits ---\n");
    record_failure(&sessions[1], now);
    EXPECT(!is_circuit_open(sessions, 2, "192.168.1.2", now),
           "different IP: circuit still closed");

    printf("\n--- Success resets circuit ---\n");
    record_success(&sessions[0]);
    EXPECT(!is_circuit_open(sessions, 1, "192.168.1.1", now),
           "circuit closed after success");

    printf("\n--- Circuit reopens on repeated failures ---\n");
    record_failure(&sessions[0], now);
    record_failure(&sessions[0], now);
    record_failure(&sessions[0], now);
    EXPECT(is_circuit_open(sessions, 1, "192.168.1.1", now),
           "circuit reopened after more failures");

    printf("\n--- Timeout closes circuit ---\n");
    time_t after_timeout = now + CIRCUIT_BREAKER_TIMEOUT_S + 1;
    EXPECT(!is_circuit_open(sessions, 1, "192.168.1.1", after_timeout),
           "circuit closed after timeout");

    printf("\n--- Multiple sessions tracked independently ---\n");
    Session multi[3];
    memset(multi, 0, sizeof(multi));
    strcpy(multi[0].ip, "10.0.0.1");
    strcpy(multi[1].ip, "10.0.0.2");
    record_failure(&multi[0], now);
    record_failure(&multi[0], now);
    record_failure(&multi[0], now);
    record_failure(&multi[1], now);
    record_failure(&multi[1], now);
    EXPECT(is_circuit_open(multi, 2, "10.0.0.1", now),
           "session[0] circuit open");
    EXPECT(!is_circuit_open(multi, 2, "10.0.0.2", now),
           "session[1] circuit closed");

    printf("\n=== Summary ===\n");
    printf("%d passed, %d failed\n", g_pass, g_fail);
    return g_fail > 0 ? 1 : 0;
}
