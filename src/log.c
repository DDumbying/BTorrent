#define _POSIX_C_SOURCE 200809L
/**
 * log.c — Logging implementation
 */

#include "log.h"
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>

static LogLevel     g_min_level  = LOG_INFO;
static FILE        *g_dest       = NULL;
static int          g_is_tty     = 0;
static pthread_mutex_t g_mutex    = PTHREAD_MUTEX_INITIALIZER;

static const char *const level_tags[] = {
    "DEBUG", "INFO ", "WARN ", "ERROR"
};

void log_init(LogLevel min_level, FILE *dest) {
    pthread_mutex_lock(&g_mutex);
    g_min_level = min_level;
    g_dest      = dest ? dest : stderr;
    g_is_tty    = isatty(fileno(g_dest));
    pthread_mutex_unlock(&g_mutex);
}

int log_is_tty(void) { return g_is_tty; }

FILE *log_dest(void) { return g_dest ? g_dest : stderr; }

void _log_write(LogLevel level, const char *file, int line,
                const char *fmt, ...) {
    if (level < g_min_level) return;

    pthread_mutex_lock(&g_mutex);

    FILE *out = g_dest ? g_dest : stderr;

    time_t now = time(NULL);
    struct tm tm_buf;
    localtime_r(&now, &tm_buf);
    char ts[10];
    strftime(ts, sizeof(ts), "%H:%M:%S", &tm_buf);

    const char *base = strrchr(file, '/');
    base = base ? base + 1 : file;

    if (g_is_tty)
        fprintf(out, "\r\033[K[%s] %s  %s:%d  ", ts, level_tags[level], base, line);
    else
        fprintf(out, "[%s] %s  %s:%d  ", ts, level_tags[level], base, line);

    va_list ap;
    va_start(ap, fmt);
    vfprintf(out, fmt, ap);
    va_end(ap);

    fputc('\n', out);
    if (level >= LOG_WARN) fflush(out);

    pthread_mutex_unlock(&g_mutex);
}
