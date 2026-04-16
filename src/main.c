/* _GNU_SOURCE enables GNU getopt argv permutation (flags can appear in
 * any order relative to positional arguments) while still providing
 * the POSIX types needed by sigaction and friends. */
#define _GNU_SOURCE
/**
 * main.c — Argument parsing + mode dispatch
 *
 * This file is intentionally thin. Its only jobs are:
 *   1. Parse command-line flags into a Config struct
 *   2. Initialise logging
 *   3. Install signal handlers
 *   4. Call the right cmd_* function
 *
 * Flag design  (short flags only — one key per action):
 *   -d  download        -i  inspect        -c  check
 *   -o  output path     -p  port           -n  max peers
 *   -t  timeout         -P  pipeline depth
 *   -v  verbose         -l  log file       -j  JSON (inspect)
 *   -V  version         -h  help
 *
 * Examples:
 *   btorrent -d ubuntu.torrent
 *   btorrent -d ubuntu.torrent -o /tmp/ubuntu -p 6881 -v
 *   btorrent -i ubuntu.torrent
 *   btorrent -i ubuntu.torrent -j | jq .name
 *   btorrent -c ubuntu.torrent -o /tmp/ubuntu
 */

#include "cmd/cmd.h"
#include "log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <getopt.h>

#ifndef BT_VERSION
#  define BT_VERSION "dev"
#endif

/* ── Signal flag — shared with cmd_download.c via extern ────────────────── */
volatile sig_atomic_t g_interrupted = 0;
static void sig_handler(int s) { (void)s; g_interrupted = 1; }

/* ── Usage ───────────────────────────────────────────────────────────────── */

static void print_usage(const char *prog) {
    printf(
        "btorrent %s — BitTorrent client\n"
        "\n"
        "Usage:\n"
        "  %s -d <file.torrent> [options]   Download a torrent\n"
        "  %s -i <file.torrent> [-j]        Inspect torrent metadata\n"
        "  %s -c <file.torrent> [-o path]   Check an existing download\n"
        "  %s -V                            Print version and exit\n"
        "\n"
        "Mode flags (pick one):\n"
        "  -d, --download    Download the torrent\n"
        "  -i, --inspect     Print metadata (no download)\n"
        "  -c, --check       Verify an existing download against piece hashes\n"
        "  -V, --version     Print version\n"
        "\n"
        "Options:\n"
        "  -o, --output <path>   Output file or directory  (default: torrent name)\n"
        "  -p, --port   <N>      Listen port               (default: 6881)\n"
        "  -n, --peers  <N>      Max concurrent peers      (default: 50)\n"
        "  -t, --timeout <N>     Per-peer connect timeout  (default: 5s)\n"
        "  -P, --pipeline <N>    Block pipeline depth      (default: 64)\n"
        "  -S, --seed            Keep seeding after download completes\n"
        "  -U, --ul <N>          Upload rate limit in KiB/s   (default: unlimited)\n"
        "  -D, --dl <N>          Download rate limit in KiB/s (default: unlimited)\n"
        "  -q, --quiet           Minimal output (progress bar only)\n"
        "  -v, --verbose         Enable verbose logging\n"
        "  -l, --log <file>      Write verbose log to file\n"
        "  -j, --json            JSON output (inspect mode)\n"
        "  -h, --help            Show this help\n"
        "\n"
        "Examples:\n"
        "  %s -d ubuntu.torrent\n"
        "  %s -d ubuntu.torrent -o /tmp/ubuntu -p 6881\n"
        "  %s -d ubuntu.torrent -v -l debug.log\n"
        "  %s -i ubuntu.torrent\n"
        "  %s -i ubuntu.torrent -j | jq .name\n"
        "\n"
        "Report bugs: https://github.com/cczcn/btorrent/issues\n",
        BT_VERSION,
        prog, prog, prog, prog, prog, prog, prog, prog, prog
    );
}

/* ── Argument parsing ────────────────────────────────────────────────────── */

static const struct option long_opts[] = {
    { "download",  no_argument,       NULL, 'd' },
    { "file",      required_argument, NULL, 'f' },
    { "inspect",   no_argument,       NULL, 'i' },
    { "check",     no_argument,       NULL, 'c' },
    { "version",   no_argument,       NULL, 'V' },
    { "output",    required_argument, NULL, 'o' },
    { "port",      required_argument, NULL, 'p' },
    { "peers",     required_argument, NULL, 'n' },
    { "timeout",   required_argument, NULL, 't' },
    { "pipeline",  required_argument, NULL, 'P' },
    { "verbose",   no_argument,       NULL, 'v' },
    { "quiet",     no_argument,       NULL, 'q' },
    { "log",       required_argument, NULL, 'l' },
    { "json",      no_argument,       NULL, 'j' },
    { "seed",      no_argument,       NULL, 'S' },
    { "ul",        required_argument, NULL, 'U' },
    { "dl",        required_argument, NULL, 'D' },
    { "help",      no_argument,       NULL, 'h' },
    { NULL, 0, NULL, 0 }
};

static int parse_args(int argc, char *argv[], Config *cfg) {
    int c;
    while ((c = getopt_long(argc, argv, "dicVo:p:n:t:P:vql:jf:hSU:D:",
                            long_opts, NULL)) != -1) {
        switch (c) {
            case 'd': cfg->mode           = MODE_DOWNLOAD;           break;
            case 'i': cfg->mode           = MODE_INSPECT;            break;
            case 'c': cfg->mode           = MODE_CHECK;              break;
            case 'V': cfg->mode           = MODE_VERSION;            break;
            case 'o': strncpy(cfg->output_path,  optarg, 1023);      break;
            case 'p': cfg->port           = (uint16_t)atoi(optarg);  break;
            case 'n': cfg->max_peers      = atoi(optarg);            break;
            case 't': cfg->peer_timeout_s = atoi(optarg);            break;
            case 'P': cfg->pipeline_depth = atoi(optarg);            break;
            case 'v': cfg->verbose        = 1;                        break;
            case 'q': cfg->verbose        = -1;                       break;
            case 'l': strncpy(cfg->log_path, optarg, 255);           break;
            case 'j': cfg->json_output    = 1;                        break;
            case 'f': strncpy(cfg->torrent_path, optarg, 1023);       break;
            case 'S': cfg->seed           = 1;                        break;
            case 'U': cfg->upload_limit_kbs   = atoi(optarg);        break;
            case 'D': cfg->download_limit_kbs = atoi(optarg);        break;
            case 'h': print_usage(argv[0]); exit(EXIT_SUCCESS);
            default:  print_usage(argv[0]); return -1;
        }
    }

    /* First non-option argument is the .torrent file */
    if (optind < argc)
        strncpy(cfg->torrent_path, argv[optind], 1023);

    return 0;
}

/* ── main ────────────────────────────────────────────────────────────────── */

int main(int argc, char *argv[]) {
    /* Defaults */
    Config cfg = {
        .mode           = MODE_NONE,
        .port           = 6881,
        .max_peers      = 50,
        .pipeline_depth = 64,   /* was 5 — deep pipeline needed for high throughput */
        .peer_timeout_s = 5,
    };

    if (argc < 2) { print_usage(argv[0]); return EXIT_FAILURE; }
    if (parse_args(argc, argv, &cfg) < 0) return EXIT_FAILURE;

    /* Version — print and exit before anything else */
    if (cfg.mode == MODE_VERSION) {
        printf("btorrent %s\n", BT_VERSION);
        return EXIT_SUCCESS;
    }

    /* Auto-detect magnet links: if the path starts with "magnet:" treat it as
     * a download without requiring -d flag. */
    if (cfg.torrent_path[0] && strncmp(cfg.torrent_path, "magnet:", 7) == 0) {
        if (cfg.mode == MODE_NONE) cfg.mode = MODE_DOWNLOAD;
        cfg.is_magnet = 1;
    }

    /* Validate: every mode except version needs a torrent file or magnet */
    if (cfg.mode == MODE_NONE) {
        fprintf(stderr, "error: specify a mode: -d (download), "
                        "-i (inspect), -c (check)\n\n");
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }
    if (!cfg.torrent_path[0]) {
        fprintf(stderr, "error: no .torrent file or magnet link specified\n\n");
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    /* Logging - default is WARN (quiet), -v enables INFO, -q suppresses all */
    FILE *log_file = NULL;
    if (cfg.log_path[0])
        log_file = fopen(cfg.log_path, "a");
    LogLevel level = LOG_WARN;
    if (cfg.verbose > 0) level = LOG_INFO;
    if (cfg.verbose > 1) level = LOG_DEBUG;
    log_init(level, log_file);

    /* Signals — only needed for download mode but harmless elsewhere */
    struct sigaction sa = { .sa_handler = sig_handler };
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* Ignore SIGPIPE — broken peer connections must not kill the process.
     * All sends use MSG_NOSIGNAL but belt-and-suspenders is safer. */
    signal(SIGPIPE, SIG_IGN);

    /* Dispatch */
    int rc;
    switch (cfg.mode) {
        case MODE_DOWNLOAD: rc = cmd_download(&cfg); break;
        case MODE_INSPECT:  rc = cmd_inspect(&cfg);  break;
        case MODE_CHECK:    rc = cmd_check(&cfg);    break;
        default:            rc = EXIT_FAILURE;        break;
    }

    if (log_file) fclose(log_file);
    return rc;
}
