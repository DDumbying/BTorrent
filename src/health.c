#define _POSIX_C_SOURCE 200809L
/**
 * health.c — Health check implementation
 */

#include "health.h"
#include "core/pieces.h"
#include "log.h"
#include <stdio.h>
#include <string.h>
#include <time.h>

static HealthStatus g_health = {0};
static char         g_json_buf[4096];

void health_get_status(HealthStatus *status) {
    if (!status) return;
    *status = g_health;
    status->uptime_seconds = time(NULL) - g_health.uptime_seconds;
}

const char *health_json_report(void) {
    HealthStatus s;
    health_get_status(&s);
    snprintf(g_json_buf, sizeof(g_json_buf),
        "{"
            "\"peers_connected\": %d,"
            "\"peers_max\": %d,"
            "\"pieces_completed\": %d,"
            "\"pieces_total\": %d,"
            "\"download_speed_kbs\": %.2f,"
            "\"upload_speed_kbs\": %.2f,"
            "\"uptime_seconds\": %ld,"
            "\"dht_nodes\": %d,"
            "\"circuit_breakers_open\": %d"
        "}",
        s.peers_connected,
        s.peers_max,
        s.pieces_completed,
        s.pieces_total,
        s.download_speed_kbs,
        s.upload_speed_kbs,
        (long)s.uptime_seconds,
        s.dht_nodes,
        s.circuit_breakers_open
    );
    return g_json_buf;
}
