#define _POSIX_C_SOURCE 200809L
/**
 * dht.c — BEP 5 DHT Implementation
 *
 * Finds peers for a given info_hash via an iterative Kademlia get_peers
 * lookup without requiring a tracker. All messages are bencoded KRPC
 * dicts sent over UDP.
 */

#include "dht/dht.h"
#include "core/bencode.h"
#include "utils.h"
#include "log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

/* ── Constants ───────────────────────────────────────────────────────────── */

#define DHT_ID_LEN    20
#define DHT_NODE_LEN  26   /* 20 ID + 4 IP + 2 port */
#define DHT_MAX_NODES 512  /* larger table for better convergence */
#define DHT_ALPHA     8    /* Kademlia parallelism — 8 gives faster convergence */

static const char *BOOTSTRAP_HOSTS[] = {
    "router.bittorrent.com",
    "router.utorrent.com",
    "dht.transmissionbt.com",
    "dht.aelitis.com",
    "bt2.tokaido.cc",
    "zero.pq hosting.org",
    NULL
};
static const int BOOTSTRAP_PORT = 6881;

/* ── Types ───────────────────────────────────────────────────────────────── */

typedef struct {
    uint8_t           id[DHT_ID_LEN];
    struct sockaddr_in addr;
    int               queried;
    int               responded;
} DhtNode;

struct DhtCtx {
    int      sock;
    uint8_t  our_id[DHT_ID_LEN];
    DhtNode  nodes[DHT_MAX_NODES];
    int      num_nodes;
};

/* ── XOR distance comparison ─────────────────────────────────────────────── */

/* Returns 1 if dist(a,target) < dist(b,target) */
static int closer(const uint8_t *a, const uint8_t *b, const uint8_t *target) {
    for (int i = 0; i < DHT_ID_LEN; i++) {
        uint8_t da = a[i] ^ target[i];
        uint8_t db = b[i] ^ target[i];
        if (da < db) return 1;
        if (da > db) return 0;
    }
    return 0;
}

/* ── KRPC message builders ───────────────────────────────────────────────── */

/* Build a bencoded get_peers query. Returns bytes written, -1 on error. */
static int build_get_peers(uint8_t *buf, size_t cap,
                            const uint8_t *our_id,
                            const uint8_t *info_hash,
                            const uint8_t *tid, size_t tid_len) {
    /* Format: d1:ad2:id20:<id>9:info_hash20:<hash>e1:q9:get_peers1:t<n>:<tid>1:y1:qe */
    int n = snprintf((char *)buf, cap, "d1:ad2:id20:");
    if (n < 0 || (size_t)n >= cap) return -1;
    memcpy(buf + n, our_id, DHT_ID_LEN);
    n += DHT_ID_LEN;

    int m = snprintf((char *)buf + n, cap - (size_t)n, "9:info_hash20:");
    if (m < 0 || (size_t)(n + m) >= cap) return -1;
    n += m;
    memcpy(buf + n, info_hash, DHT_ID_LEN);
    n += DHT_ID_LEN;

    m = snprintf((char *)buf + n, cap - (size_t)n,
                 "e1:q9:get_peers1:t%zu:", tid_len);
    if (m < 0 || (size_t)(n + m) >= cap) return -1;
    n += m;
    memcpy(buf + n, tid, tid_len);
    n += (int)tid_len;

    m = snprintf((char *)buf + n, cap - (size_t)n, "1:y1:qe");
    if (m < 0) return -1;
    n += m;
    return n;
}

/* ── Node list helpers ───────────────────────────────────────────────────── */

static void add_node(DhtCtx *ctx, const uint8_t *id,
                     const char *ip, uint16_t port) {
    /* Deduplicate */
    for (int i = 0; i < ctx->num_nodes; i++) {
        char nip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &ctx->nodes[i].addr.sin_addr, nip, sizeof(nip));
        if (ctx->nodes[i].addr.sin_port == htons(port) &&
            strcmp(nip, ip) == 0)
            return;
    }
    if (ctx->num_nodes >= DHT_MAX_NODES) return;

    DhtNode *node = &ctx->nodes[ctx->num_nodes++];
    if (id) {
        memcpy(node->id, id, DHT_ID_LEN);
    } else {
        memset(node->id, 0, DHT_ID_LEN);
    }
    memset(&node->addr, 0, sizeof(node->addr));
    node->addr.sin_family = AF_INET;
    node->addr.sin_port   = htons(port);
    inet_pton(AF_INET, ip, &node->addr.sin_addr);
    node->queried   = 0;
    node->responded = 0;
}

static void parse_nodes(DhtCtx *ctx, const uint8_t *data, size_t len) {
    for (size_t i = 0; i + DHT_NODE_LEN <= len; i += DHT_NODE_LEN) {
        const uint8_t *nid = data + i;
        struct in_addr a;
        memcpy(&a, data + i + DHT_ID_LEN, 4);
        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &a, ip, sizeof(ip));
        uint16_t port = (uint16_t)((data[i + DHT_ID_LEN + 4] << 8) |
                                    data[i + DHT_ID_LEN + 5]);
        if (port == 0) continue;
        add_node(ctx, nid, ip, port);
    }
}

static int parse_peers(const uint8_t *data, size_t len,
                        Peer *out, int max_out) {
    int count = 0;
    for (size_t i = 0; i + 6 <= len && count < max_out; i += 6) {
        struct in_addr a;
        memcpy(&a, data + i, 4);
        inet_ntop(AF_INET, &a, out[count].ip, sizeof(out[count].ip));
        out[count].port = (uint16_t)((data[i + 4] << 8) | data[i + 5]);
        if (out[count].port > 0) count++;
    }
    return count;
}

/* ── UDP helpers ─────────────────────────────────────────────────────────── */

static void send_to_node(DhtCtx *ctx, DhtNode *node,
                          const uint8_t *msg, int msg_len) {
    sendto(ctx->sock, msg, (size_t)msg_len, 0,
           (struct sockaddr *)&node->addr, sizeof(node->addr));
}

/* ── Public API ──────────────────────────────────────────────────────────── */

DhtCtx *dht_new(uint16_t port) {
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        LOG_WARN("dht: socket failed: %s", strerror(errno));
        return NULL;
    }

    struct sockaddr_in local = {0};
    local.sin_family      = AF_INET;
    local.sin_addr.s_addr = INADDR_ANY;
    local.sin_port        = htons(port);
    if (bind(sock, (struct sockaddr *)&local, sizeof(local)) < 0) {
        local.sin_port = 0;   /* try ephemeral port */
        bind(sock, (struct sockaddr *)&local, sizeof(local));
    }

    struct timeval tv = { .tv_sec = 2, .tv_usec = 0 };
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    DhtCtx *ctx = calloc(1, sizeof(DhtCtx));
    if (!ctx) { close(sock); return NULL; }
    ctx->sock = sock;
    random_bytes(ctx->our_id, DHT_ID_LEN);
    LOG_INFO("dht: initialized (port %d)", (int)port);
    return ctx;
}

void dht_bootstrap(DhtCtx *ctx) {
    LOG_INFO("%s", "dht: resolving bootstrap nodes...");
    for (int i = 0; BOOTSTRAP_HOSTS[i]; i++) {
        struct addrinfo hints = {0}, *res;
        hints.ai_family   = AF_INET;
        hints.ai_socktype = SOCK_DGRAM;
        char ps[8];
        snprintf(ps, sizeof(ps), "%d", BOOTSTRAP_PORT);
        if (getaddrinfo(BOOTSTRAP_HOSTS[i], ps, &hints, &res) != 0) continue;
        struct sockaddr_in *sa = (struct sockaddr_in *)res->ai_addr;
        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &sa->sin_addr, ip, sizeof(ip));
        add_node(ctx, NULL, ip, (uint16_t)BOOTSTRAP_PORT);
        freeaddrinfo(res);
        LOG_INFO("dht: bootstrap %s → %s", BOOTSTRAP_HOSTS[i], ip);
    }
}

PeerList dht_get_peers(DhtCtx *ctx, const uint8_t *info_hash, int timeout_s) {
    PeerList result = { NULL, 0, 1800 };
    if (ctx->num_nodes == 0) {
        LOG_WARN("%s", "dht: no bootstrap nodes");
        return result;
    }

    LOG_INFO("%s", "dht: iterative get_peers lookup...");

    Peer    found[512];
    int     num_found = 0;
    uint8_t msg[1024];
    uint8_t resp[4096];
    uint8_t tid[2];
    time_t  deadline = time(NULL) + timeout_s;
    int     round    = 0;

    while (time(NULL) < deadline && num_found < 200) {
        round++;
        int sent = 0;

        /* Round 1: query ALL bootstrap nodes immediately regardless of alpha.
         * Subsequent rounds: query up to DHT_ALPHA closest unqueried nodes. */
        int send_limit = (round == 1) ? ctx->num_nodes : DHT_ALPHA;

        for (int i = 0; i < ctx->num_nodes && sent < send_limit; i++) {
            DhtNode *nd = &ctx->nodes[i];
            if (nd->queried) continue;
            nd->queried = 1;
            random_bytes(tid, sizeof(tid));
            int mlen = build_get_peers(msg, sizeof(msg),
                                        ctx->our_id, info_hash,
                                        tid, sizeof(tid));
            if (mlen > 0) {
                send_to_node(ctx, nd, msg, mlen);
                sent++;
            }
        }
        if (sent == 0) break;  /* all nodes queried */

        /* Collect responses for up to 2 s */
        time_t round_end = time(NULL) + 2;
        while (time(NULL) < round_end) {
            struct sockaddr_in from = {0};
            socklen_t flen = sizeof(from);
            ssize_t rlen = recvfrom(ctx->sock, resp, sizeof(resp), 0,
                                     (struct sockaddr *)&from, &flen);
            if (rlen <= 0) break;

            BencodeNode *root = bencode_parse(resp, (size_t)rlen);
            if (!root) continue;

            BencodeNode *y = bencode_dict_get(root, "y");
            if (!y || y->type != BENCODE_STR ||
                y->str.len < 1 || y->str.data[0] != 'r') {
                bencode_free(root);
                continue;
            }

            BencodeNode *r = bencode_dict_get(root, "r");
            if (!r || r->type != BENCODE_DICT) {
                bencode_free(root);
                continue;
            }

            /* Mark responder */
            char fip[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &from.sin_addr, fip, sizeof(fip));
            uint16_t fport = ntohs(from.sin_port);
            for (int i = 0; i < ctx->num_nodes; i++) {
                char nip[INET_ADDRSTRLEN];
                inet_ntop(AF_INET, &ctx->nodes[i].addr.sin_addr,
                           nip, sizeof(nip));
                if (ctx->nodes[i].addr.sin_port == htons(fport) &&
                    strcmp(nip, fip) == 0) {
                    ctx->nodes[i].responded = 1;
                    BencodeNode *id_n = bencode_dict_get(r, "id");
                    if (id_n && id_n->type == BENCODE_STR &&
                        id_n->str.len >= DHT_ID_LEN)
                        memcpy(ctx->nodes[i].id, id_n->str.data, DHT_ID_LEN);
                    break;
                }
            }

            /* Peers in values list */
            BencodeNode *values = bencode_dict_get(r, "values");
            if (values && values->type == BENCODE_LIST) {
                int max_room = (int)(sizeof(found)/sizeof(found[0])) - num_found;
                int peers_this_response = 0;
                for (size_t vi = 0; vi < values->list.count && max_room > 0; vi++) {
                    BencodeNode *v = values->list.items[vi];
                    if (v->type != BENCODE_STR || v->str.len < 6) continue;
                    int np = parse_peers(v->str.data, v->str.len,
                                          found + num_found, max_room);
                    num_found          += np;
                    max_room           -= np;
                    peers_this_response += np;
                }
                if (peers_this_response > 0)
                    LOG_INFO("dht: %d peer%s from %s:%d",
                             peers_this_response,
                             peers_this_response == 1 ? "" : "s",
                             fip, (int)fport);
            }

            /* Closer nodes */
            BencodeNode *nodes_n = bencode_dict_get(r, "nodes");
            if (nodes_n && nodes_n->type == BENCODE_STR) {
                int before = ctx->num_nodes;
                parse_nodes(ctx, nodes_n->str.data, nodes_n->str.len);
                if (ctx->num_nodes > before)
                    LOG_INFO("dht: +%d nodes from %s",
                             ctx->num_nodes - before, fip);
            }

            bencode_free(root);
        }

        /* Sort by closeness to info_hash for next round */
        for (int i = 1; i < ctx->num_nodes; i++) {
            DhtNode tmp = ctx->nodes[i];
            int j = i - 1;
            while (j >= 0 && closer(tmp.id, ctx->nodes[j].id, info_hash)) {
                ctx->nodes[j + 1] = ctx->nodes[j];
                j--;
            }
            ctx->nodes[j + 1] = tmp;
        }

        LOG_INFO("dht: round %d — %d nodes, %d peers",
                 round, ctx->num_nodes, num_found);
    }

    if (num_found == 0) {
        LOG_WARN("%s", "dht: no peers found");
        return result;
    }

    LOG_INFO("dht: found %d peers", num_found);
    result.peers = calloc((size_t)num_found, sizeof(Peer));
    if (!result.peers) return result;
    memcpy(result.peers, found, (size_t)num_found * sizeof(Peer));
    result.count = num_found;
    return result;
}

void dht_free(DhtCtx *ctx) {
    if (!ctx) return;
    if (ctx->sock >= 0) close(ctx->sock);
    free(ctx);
}
