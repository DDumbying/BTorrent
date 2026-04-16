# 📘 Chapter 4: The Tracker Protocol

## What Is a Tracker?

A **tracker** is an HTTP server that keeps a list of peers for each torrent.
When you start downloading, you ask the tracker:

> "I'm downloading torrent X. Here's my ID. Who else is downloading it?"

The tracker replies with a list of IP addresses and ports you can connect to.

Importantly, the tracker does NOT have the actual files — it's just a
directory service. Once you have peer addresses, you talk to peers directly.

---

## The Announce Request

You make a GET request to the tracker's announce URL with these parameters:

| Parameter    | Type       | Description                                        |
|--------------|------------|----------------------------------------------------|
| `info_hash`  | 20 bytes   | URL-encoded SHA-1 of the info dict                 |
| `peer_id`    | 20 bytes   | Your randomly generated client ID                  |
| `port`       | int        | Port you're listening on for incoming connections  |
| `uploaded`   | int        | Bytes you've uploaded this session                 |
| `downloaded` | int        | Bytes you've downloaded this session               |
| `left`       | int        | Bytes remaining to download                        |
| `compact`    | 0 or 1     | 1 = ask for compact peer list (6 bytes/peer)       |
| `event`      | string     | `started`, `stopped`, or `completed` (optional)    |

### Example URL

```
http://tracker.example.com:6969/announce
  ?info_hash=%89%ab%cd%ef...   (20 URL-encoded bytes)
  &peer_id=-BT0001-xxxxxxxxxxxx
  &port=6881
  &uploaded=0
  &downloaded=0
  &left=1234567890
  &compact=1
  &event=started
```

---

## URL Encoding the Info Hash

The 20-byte info hash must be percent-encoded. Each byte becomes `%XX`
unless it's an "unreserved" character (letters, digits, `-`, `_`, `.`, `~`).

```c
void url_encode_bytes(const uint8_t *bytes, int len, char *out) {
    for (int i = 0; i < len; i++) {
        if (isalnum(bytes[i]) || bytes[i]=='-' || bytes[i]=='_'
                              || bytes[i]=='.' || bytes[i]=='~') {
            *out++ = bytes[i];
        } else {
            out += sprintf(out, "%%%02X", bytes[i]);
        }
    }
    *out = '\0';
}
```

---

## The Peer ID

Your peer_id is a 20-byte identifier for your client. Convention (BEP 20):

```
-BT0001-xxxxxxxxxxxx
 ^^^^^^              → client code + version
        ^^^^^^^^^^^^  → 12 random characters
```

We use `-BT0001-` as our prefix.

---

## The Tracker Response

The tracker responds with a bencoded dictionary:

### Success Response

```
d
  8:interval    → i1800e    (seconds between re-announces)
  5:peers       → <compact peer list> OR list of dicts
e
```

### Compact Peer Format (preferred)

When you send `compact=1`, peers is a byte string where each peer is
exactly **6 bytes**:

```
[IP byte 0][IP byte 1][IP byte 2][IP byte 3][Port high][Port low]
```

Example: `\xC0\xA8\x01\x01\x1A\xE1` = IP 192.168.1.1, Port 6881

```c
// Parsing compact peers
for (int i = 0; i < peers_len; i += 6) {
    uint8_t *p = peers_data + i;
    char ip[16];
    snprintf(ip, sizeof(ip), "%d.%d.%d.%d", p[0], p[1], p[2], p[3]);
    uint16_t port = (p[4] << 8) | p[5];   // big-endian!
    printf("Peer: %s:%d\n", ip, port);
}
```

### Dictionary Peer Format (fallback)

If compact=0 or tracker doesn't support compact:

```
5:peers l
  d
    2:ip   → "1.2.3.4"
    7:peer id → <20 bytes>
    4:port → i6881e
  e
  ...
e
```

### Error Response

```
d
  14:failure reason → "torrent not found"
e
```

---

## Re-Announcing

The tracker response includes an `interval` field (seconds).
You should re-announce after that many seconds to:
- Get fresh peer lists
- Tell the tracker you're still alive

Send `event=stopped` when you quit.

---

## Multiple Trackers (announce-list)

Some torrents have `announce-list` — a list of lists of tracker URLs:

```
announce-list: [
  ["http://tracker1.com/announce", "http://tracker2.com/announce"],
  ["udp://tracker3.com:6969/announce"]
]
```

Try each tier in order. Within a tier, shuffle and try each. Move to the
next tier only if all in the current tier fail.

---

## UDP Trackers (Advanced)

Some trackers use UDP instead of HTTP (more efficient). The protocol is
different — see BEP 15. Our client uses HTTP only for simplicity.
