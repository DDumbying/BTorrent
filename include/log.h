#pragma once
/**
 * log.h — Structured Logging Module
 *
 * Replaces all raw printf/fprintf calls across the codebase.
 *
 * Levels (lowest → highest severity):
 *   LOG_DEBUG  verbose tracing, compiled out in release if LOG_MIN_LEVEL > 0
 *   LOG_INFO   normal operational messages
 *   LOG_WARN   recoverable problems
 *   LOG_ERROR  fatal or near-fatal conditions
 *
 * Usage:
 *   log_init(LOG_INFO, NULL);           // to stderr, INFO and above
 *   log_init(LOG_DEBUG, fopen(...));    // to file, all levels
 *
 *   LOG_INFO("tracker: %d peers", count);
 *   LOG_WARN("peer %s:%d: re-choked",  ip, port);
 *   LOG_ERROR("malloc failed");
 */

#include <stdio.h>

#ifndef LOG_MIN_LEVEL
#  define LOG_MIN_LEVEL 0   /* 0 = DEBUG, compile with -DLOG_MIN_LEVEL=2 for release */
#endif

typedef enum {
    LOG_DEBUG = 0,
    LOG_INFO  = 1,
    LOG_WARN  = 2,
    LOG_ERROR = 3,
} LogLevel;

/* Call once at program start. dest=NULL means stderr. */
void log_init(LogLevel min_level, FILE *dest);

/* Returns 1 if the log destination is an interactive terminal. */
int  log_is_tty(void);

/* Returns the active log FILE* (always non-NULL after log_init). */
FILE *log_dest(void);

/* Internal — do not call directly. */
void _log_write(LogLevel level, const char *file, int line,
                const char *fmt, ...)
    __attribute__((format(printf, 4, 5)));

#define LOG_DEBUG(fmt, ...) \
    do { if (LOG_DEBUG >= LOG_MIN_LEVEL) \
        _log_write(LOG_DEBUG, __FILE__, __LINE__, fmt, ##__VA_ARGS__); } while(0)

#define LOG_INFO(fmt, ...) \
    do { if (LOG_INFO >= LOG_MIN_LEVEL) \
        _log_write(LOG_INFO,  __FILE__, __LINE__, fmt, ##__VA_ARGS__); } while(0)

#define LOG_WARN(fmt, ...) \
    do { if (LOG_WARN >= LOG_MIN_LEVEL) \
        _log_write(LOG_WARN,  __FILE__, __LINE__, fmt, ##__VA_ARGS__); } while(0)

#define LOG_ERROR(fmt, ...) \
    do { if (LOG_ERROR >= LOG_MIN_LEVEL) \
        _log_write(LOG_ERROR, __FILE__, __LINE__, fmt, ##__VA_ARGS__); } while(0)
