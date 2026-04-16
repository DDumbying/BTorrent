#define _POSIX_C_SOURCE 200809L
/**
 * tracker.c — HTTP + UDP Tracker Communication (BEP 3 + BEP 15)
 *
 * v1.1.0 changes:
 *   - UDP connection ID caching (60s TTL per BEP 15)
 *   - IPv6 peer parsing (compact6 18-byte format)
 *   - IPv6 peer support in dict responses
 */

#include "proto/tracker.h"
#include "core/bencode.h"
#include "utils.h"
#include "log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <curl/curl.h>

#define UDP_CONN_CACHE_TTL 60

typedef struct { uint8_t *data; size_t len; size_t cap; } CurlBuf;

static size_t curl_wcb(void *ptr, size_t sz, size_t n, void *ud) {
    CurlBuf *b = (CurlBuf *)ud;
    size_t bytes = sz * n;
    if (b->len + bytes > b->cap) {
        size_t new_cap = (b->len + bytes) * 2 + 512;
        void *tmp = realloc(b->data, new_cap);
        if (!tmp) return 0;
        b->data = tmp;
        b->cap = new_cap;
    }
    memcpy(b->data + b->len, ptr, bytes);
    b->len += bytes;
    return bytes;
}

static void  u32be(uint8_t *b, uint32_t v) {
    b[0]=(v>>24)&0xFF; b[1]=(v>>16)&0xFF; b[2]=(v>>8)&0xFF; b[3]=v&0xFF;
}
static void  u64be(uint8_t *b, uint64_t v) {
    u32be(b,(uint32_t)(v>>32)); u32be(b+4,(uint32_t)v);
}
static uint32_t r32be(const uint8_t *b) {
    return ((uint32_t)b[0]<<24)|((uint32_t)b[1]<<16)|((uint32_t)b[2]<<8)|(uint32_t)b[3];
}

/* ── UDP Connection ID Cache ────────────────────────────────────────────────── */

static UdpConnCache g_udp_cache;

void udp_cache_init(UdpConnCache *cache) {
    memset(cache, 0, sizeof(*cache));
}

uint64_t udp_cache_get(UdpConnCache *cache, const char *host, int64_t now) {
    if (!cache->host[0]) return 0;
    if (now >= cache->expires_at) {
        cache->host[0] = '\0';
        return 0;
    }
    if (strcmp(cache->host, host) == 0) {
        return cache->conn_id;
    }
    return 0;
}

void udp_cache_set(UdpConnCache *cache, const char *host, uint64_t conn_id, int64_t expires_at) {
    strncpy(cache->host, host, sizeof(cache->host) - 1);
    cache->host[sizeof(cache->host) - 1] = '\0';
    cache->conn_id = conn_id;
    cache->expires_at = expires_at;
}

/* ── Peer ID ───────────────────────────────────────────────────────────────── */

void generate_peer_id(uint8_t *out) {
    static const char prefix[] = "-BT0001-";
    static const char cs[] =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    memcpy(out, prefix, 8);
    uint8_t rnd[12];
    random_bytes(rnd, sizeof(rnd));
    for (int i = 0; i < 12; i++)
        out[8 + i] = cs[rnd[i] % (sizeof(cs) - 1)];
}

/* ── Peer Parsing ───────────────────────────────────────────────────────────── */

PeerList compact_peers(const uint8_t *d, size_t len) {
    PeerList pl = {NULL, 0, 1800};
    if (len % 6 != 0) return pl;
    int cnt = (int)(len / 6);
    pl.peers = xcalloc(cnt, sizeof(Peer));
    pl.count = cnt;
    for (int i = 0; i < cnt; i++) {
        const uint8_t *p = d + i*6;
        snprintf(pl.peers[i].ip, 16, "%d.%d.%d.%d", p[0],p[1],p[2],p[3]);
        pl.peers[i].port = read_uint16_be(p + 4);
        pl.peers[i].is_ipv6 = 0;
    }
    return pl;
}

PeerList compact6_peers(const uint8_t *d, size_t len) {
    PeerList pl = {NULL, 0, 1800};
    if (len % 18 != 0) return pl;
    int cnt = (int)(len / 18);
    pl.peers = xcalloc(cnt, sizeof(Peer));
    pl.count = cnt;
    for (int i = 0; i < cnt; i++) {
        const uint8_t *p = d + i*18;
        inet_ntop(AF_INET6, p, pl.peers[i].ip, 46);
        pl.peers[i].port = read_uint16_be(p + 16);
        pl.peers[i].is_ipv6 = 1;
    }
    return pl;
}

static PeerList dict_peers(BencodeNode *n) {
    PeerList pl = {NULL, 0, 1800};
    if (!n || n->type != BENCODE_LIST) return pl;
    int cnt = (int)n->list.count;
    pl.peers = xcalloc(cnt, sizeof(Peer));
    for (int i = 0; i < cnt; i++) {
        BencodeNode *e = n->list.items[i];
        if (e->type != BENCODE_DICT) continue;
        BencodeNode *ip   = bencode_dict_get(e, "ip");
        BencodeNode *port = bencode_dict_get(e, "port");
        if (!ip || ip->type != BENCODE_STR) continue;
        if (!port || port->type != BENCODE_INT) continue;

        int is_ipv6 = 0;
        if (ip->str.len < 46 && ip->str.len > 0) {
            char tmp[47] = {0};
            memcpy(tmp, ip->str.data, ip->str.len < 46 ? ip->str.len : 46);
            if (strchr(tmp, ':') != NULL) is_ipv6 = 1;
        }

        size_t l = ip->str.len < 46 ? ip->str.len : 45;
        memcpy(pl.peers[pl.count].ip, ip->str.data, l);
        pl.peers[pl.count].ip[l] = '\0';
        pl.peers[pl.count].port = (uint16_t)port->integer;
        pl.peers[pl.count].is_ipv6 = is_ipv6;
        pl.count++;
    }
    return pl;
}

PeerList parse_peers_binary(const uint8_t *data, size_t len) {
    if (len % 18 == 0) {
        return compact6_peers(data, len);
    }
    return compact_peers(data, len);
}

/* ── HTTP Tracker ───────────────────────────────────────────────────────────── */

static PeerList http_announce(const char *base,
                              const TorrentInfo *t, const uint8_t *pid,
                              uint16_t port, long dl, long ul, long left,
                              const char *event) {
    PeerList empty = {NULL, 0, 1800};
    char eh[61], ep[61];
    url_encode_bytes(t->info_hash, 20, eh);
    url_encode_bytes(pid, 20, ep);
    char url[2048];
    int n = snprintf(url, sizeof(url),
        "%s?info_hash=%s&peer_id=%s&port=%d&uploaded=%ld&downloaded=%ld&left=%ld&compact=1&numwant=200",
        base, eh, ep, port, ul, dl, left);
    if (event && event[0])
        snprintf(url+n, sizeof(url)-n, "&event=%s", event);

    CURL *c = curl_easy_init();
    if (!c) return empty;
    CurlBuf buf = {NULL, 0, 0};
    curl_easy_setopt(c, CURLOPT_URL,            url);
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION,  curl_wcb);
    curl_easy_setopt(c, CURLOPT_WRITEDATA,      &buf);
    curl_easy_setopt(c, CURLOPT_TIMEOUT,        30L);
    curl_easy_setopt(c, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(c, CURLOPT_USERAGENT,      "BTorrent/1.1");
    curl_easy_setopt(c, CURLOPT_SSL_VERIFYPEER, 1L);
    curl_easy_setopt(c, CURLOPT_SSL_VERIFYHOST, 2L);
    CURLcode rc = curl_easy_perform(c);
    curl_easy_cleanup(c);

    if (rc != CURLE_OK || !buf.data) { free(buf.data); return empty; }

    BencodeNode *root = bencode_parse(buf.data, buf.len);
    free(buf.data);
    if (!root) return empty;

    BencodeNode *fail = bencode_dict_get(root, "failure reason");
    if (fail && fail->type == BENCODE_STR) {
        LOG_WARN("tracker HTTP failure: %.*s", (int)fail->str.len, fail->str.data);
        bencode_free(root); return empty;
    }

    int interval = 1800;
    BencodeNode *iv = bencode_dict_get(root, "interval");
    if (iv && iv->type == BENCODE_INT) interval = (int)iv->integer;

    PeerList pl = {NULL, 0, 1800};
    BencodeNode *pn = bencode_dict_get(root, "peers");
    if (pn) {
        pl = (pn->type == BENCODE_STR)
             ? parse_peers_binary(pn->str.data, pn->str.len)
             : dict_peers(pn);
    }

    BencodeNode *p6 = bencode_dict_get(root, "peers6");
    if (p6 && p6->type == BENCODE_STR) {
        PeerList pl6 = compact6_peers(p6->str.data, p6->str.len);
        if (pl6.count > 0) {
            if (pl.count > 0) {
                pl.peers = realloc(pl.peers, (pl.count + pl6.count) * sizeof(Peer));
                memcpy(pl.peers + pl.count, pl6.peers, pl6.count * sizeof(Peer));
                pl.count += pl6.count;
                free(pl6.peers);
            } else {
                pl = pl6;
            }
        }
    }

    pl.interval = interval;
    bencode_free(root);
    return pl;
}

/* ── UDP Tracker ────────────────────────────────────────────────────────────── */

static int parse_udp_url(const char *url, char *host, size_t hlen, int *port) {
    const char *p = url;
    if (strncmp(p, "udp://", 6) == 0) p += 6; else return -1;
    const char *colon = strrchr(p, ':');
    if (!colon) return -1;
    size_t hl = (size_t)(colon - p);
    if (hl >= hlen) hl = hlen - 1;
    memcpy(host, p, hl); host[hl] = '\0';
    *port = atoi(colon + 1);
    return (*port > 0 && *port <= 65535) ? 0 : -1;
}

static uint64_t get_connection_id(const char *host, int tport,
                                  struct sockaddr *saddr, socklen_t *slen) {
    (void)tport;
    int64_t now = time(NULL);
    uint64_t cached = udp_cache_get(&g_udp_cache, host, now);
    if (cached != 0) {
        LOG_DEBUG("tracker UDP: using cached conn_id for %s", host);
        return cached;
    }

    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) return 0;
    struct timeval tv = {15, 0};
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    uint32_t txid;
    random_bytes((uint8_t *)&txid, sizeof(txid));

    uint8_t req[16];
    u64be(req,    0x41727101980ULL);
    u32be(req+8,  0);
    u32be(req+12, txid);
    if (sendto(sock, req, 16, 0, saddr, *slen) != 16) {
        close(sock); return 0;
    }

    uint8_t resp[16];
    ssize_t rlen = recv(sock, resp, sizeof(resp), 0);
    close(sock);
    if (rlen < 16) return 0;
    if (r32be(resp) != 0 || r32be(resp+4) != txid) return 0;

    uint64_t conn_id = ((uint64_t)r32be(resp+8)<<32) | r32be(resp+12);
    udp_cache_set(&g_udp_cache, host, conn_id, now + UDP_CONN_CACHE_TTL);
    LOG_DEBUG("tracker UDP: cached conn_id for %s (TTL=%ds)", host, UDP_CONN_CACHE_TTL);
    return conn_id;
}

static PeerList udp_announce(const char *url,
                             const TorrentInfo *t, const uint8_t *pid,
                             uint16_t port, long dl, long ul, long left,
                             const char *event) {
    PeerList empty = {NULL, 0, 1800};
    char host[256]; int tport;
    if (parse_udp_url(url, host, sizeof(host), &tport) < 0) return empty;

    struct addrinfo hints = {0}, *res;
    hints.ai_family   = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    char pstr[8]; snprintf(pstr, sizeof(pstr), "%d", tport);
    if (getaddrinfo(host, pstr, &hints, &res) != 0) {
        LOG_WARN("tracker UDP: DNS failed for %s", host); return empty;
    }

    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) { freeaddrinfo(res); return empty; }
    struct timeval tv = {15, 0};
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    struct sockaddr_in *saddr = (struct sockaddr_in *)res->ai_addr;
    socklen_t slen = sizeof(*saddr);

    uint64_t conn_id = get_connection_id(host, tport, (struct sockaddr *)saddr, &slen);
    if (conn_id == 0) { freeaddrinfo(res); close(sock); return empty; }

    uint32_t ev_code = 0;
    if (event) {
        if      (strcmp(event,"started")   == 0) ev_code = 2;
        else if (strcmp(event,"stopped")   == 0) ev_code = 3;
        else if (strcmp(event,"completed") == 0) ev_code = 1;
    }

    uint32_t txid;
    random_bytes((uint8_t *)&txid, sizeof(txid));
    uint8_t areq[98];
    u64be(areq,    conn_id);
    u32be(areq+8,  1);
    u32be(areq+12, txid);
    memcpy(areq+16, t->info_hash, 20);
    memcpy(areq+36, pid, 20);
    u64be(areq+56, (uint64_t)dl);
    u64be(areq+64, (uint64_t)left);
    u64be(areq+72, (uint64_t)ul);
    u32be(areq+80, ev_code);
    u32be(areq+84, 0);
    uint32_t rkey; random_bytes((uint8_t*)&rkey, 4);
    u32be(areq+88, rkey);
    u32be(areq+92, 200);
    areq[96] = (port >> 8) & 0xFF;
    areq[97] = port & 0xFF;

    if (sendto(sock, areq, 98, 0, (struct sockaddr*)saddr, slen) != 98) {
        close(sock); freeaddrinfo(res); return empty;
    }

    uint8_t aresp[4096];
    ssize_t rlen = recv(sock, aresp, sizeof(aresp), 0);
    close(sock); freeaddrinfo(res);
    if (rlen < 20) return empty;
    if (r32be(aresp) != 1 || r32be(aresp+4) != txid) return empty;

    int interval = (int)r32be(aresp+8);
    LOG_INFO("tracker UDP: seeders=%u leechers=%u interval=%d",
             r32be(aresp+16), r32be(aresp+12), interval);

    PeerList pl = parse_peers_binary(aresp+20, (size_t)(rlen-20));
    pl.interval = interval;
    return pl;
}

/* ── Tracker Dispatch ────────────────────────────────────────────────────────── */

static PeerList try_tracker(const char *url,
                            const TorrentInfo *t, const uint8_t *pid,
                            uint16_t port, long dl, long ul, long left,
                            const char *event) {
    PeerList empty = {NULL, 0, 1800};
    if (!url || !url[0]) return empty;
    LOG_INFO("tracker: trying %s", url);
    if (strncmp(url, "udp://", 6) == 0)
        return udp_announce(url, t, pid, port, dl, ul, left, event);
    if (strncmp(url, "http://", 7) == 0 || strncmp(url, "https://", 8) == 0)
        return http_announce(url, t, pid, port, dl, ul, left, event);
    LOG_WARN("tracker: unsupported scheme: %s", url);
    return empty;
}

PeerList tracker_announce_url(const char        *url,
                               const TorrentInfo *torrent,
                               const uint8_t     *peer_id,
                               uint16_t           port,
                               long               downloaded,
                               long               uploaded,
                               long               left,
                               const char        *event) {
    return try_tracker(url, torrent, peer_id, port, downloaded, uploaded, left, event);
}

PeerList tracker_announce(const TorrentInfo *torrent,
                          const uint8_t     *peer_id,
                          uint16_t           port,
                          long               downloaded,
                          long               uploaded,
                          long               left,
                          const char        *event) {
    udp_cache_init(&g_udp_cache);
    if (torrent->announce[0]) {
        PeerList pl = try_tracker(torrent->announce, torrent, peer_id,
                                  port, downloaded, uploaded, left, event);
        if (pl.count > 0) {
            LOG_INFO("tracker: %d peers from primary", pl.count);
            return pl;
        }
    }
    for (int i = 0; i < torrent->num_trackers; i++) {
        const char *url = torrent->announce_list[i];
        if (strcmp(url, torrent->announce) == 0) continue;
        PeerList pl = try_tracker(url, torrent, peer_id,
                                  port, downloaded, uploaded, left, event);
        if (pl.count > 0) {
            LOG_INFO("tracker: %d peers from backup: %s", pl.count, url);
            return pl;
        }
    }
    LOG_WARN("%s", "tracker: all trackers exhausted");
    return (PeerList){NULL, 0, 1800};
}

PeerList tracker_announce_with_retry(const TorrentInfo *torrent,
                                     const uint8_t     *peer_id,
                                     uint16_t           port,
                                     long               downloaded,
                                     long               uploaded,
                                     long               left,
                                     const char        *event) {
    int backoff = 1;
    for (int attempt = 0; attempt < 5; attempt++) {
        PeerList pl = tracker_announce(torrent, peer_id, port,
                                       downloaded, uploaded, left, event);
        if (pl.count > 0) return pl;
        LOG_WARN("tracker: attempt %d failed, retry in %ds", attempt + 1, backoff);
        struct timespec ts = { (time_t)backoff, 0 };
        nanosleep(&ts, NULL);
        backoff = backoff < 60 ? backoff * 2 : 60;
    }
    return (PeerList){NULL, 0, 1800};
}

void peer_list_free(PeerList *pl) {
    if (!pl) return;
    free(pl->peers);
    pl->peers = NULL;
    pl->count = 0;
}
