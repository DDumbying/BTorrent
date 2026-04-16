#define _POSIX_C_SOURCE 200809L
/**
 * health.h — Health check interface for monitoring
 */

#pragma once

#include <stdint.h>
#include <time.h>

typedef struct {
    int      peers_connected;
    int      peers_max;
    int      pieces_completed;
    int      pieces_total;
    double   download_speed_kbs;
    double   upload_speed_kbs;
    time_t   uptime_seconds;
    int      dht_nodes;
    int      circuit_breakers_open;
} HealthStatus;

void health_get_status(HealthStatus *status);
const char *health_json_report(void);
