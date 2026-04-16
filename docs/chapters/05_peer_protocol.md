# 📘 Chapter 5: The Peer Wire Protocol

## Overview

Once you have a list of peers from the tracker, you connect to them via
**TCP** and exchange messages to download pieces.

The protocol has two phases:
1. **Handshake** — Establish identity and verify you want the same torrent
2. **Message exchange** — Request and receive pieces

---

## Phase 1: The Handshake

The handshake is always the first thing sent on a new connection.
It is NOT a length-prefixed message — it has its own format:

```
[pstrlen][pstr][reserved][info_hash][peer_id]
```

| Field       | Size     | Value                                          |
|-------------|----------|------------------------------------------------|
| `pstrlen`   | 1 byte   | `19` (length of the protocol string)           |
| `pstr`      | 19 bytes | `"BitTorrent protocol"` (literal ASCII)        |
| `reserved`  | 8 bytes  | All zeros (extension bits — we don't use them) |
| `info_hash` | 20 bytes | SHA-1 of the info dict                         |
| `peer_id`   | 20 bytes | Your 20-byte peer ID                           |

**Total handshake size: 1 + 19 + 8 + 20 + 20 = 68 bytes**

```c
void build_handshake(uint8_t *buf, uint8_t *info_hash, uint8_t *peer_id) {
    buf[0] = 19;                              // pstrlen
    memcpy(buf + 1,  "BitTorrent protocol", 19); // pstr
    memset(buf + 20, 0, 8);                  // reserved
    memcpy(buf + 28, info_hash, 20);         // info_hash
    memcpy(buf + 48, peer_id, 20);           // peer_id
}
```

After sending your handshake, read back 68 bytes. Verify:
- `pstr` is `"BitTorrent protocol"`
- `info_hash` matches (you're downloading the same torrent!)
- `peer_id` matches if you expected a specific peer (optional)

---

## Phase 2: Messages

All subsequent messages follow this format:

```
[length prefix 4 bytes][message id 1 byte][payload variable]
```

The length prefix is a **big-endian 32-bit integer** giving the number of
bytes that follow (including the message ID byte, but NOT the length itself).

### Special: Keep-Alive

```
[00 00 00 00]
```

Length = 0, no message ID. Sent periodically to prevent timeout.

---

## Message Types

| ID | Name        | Payload              | Meaning                               |
|----|-------------|----------------------|---------------------------------------|
| 0  | choke       | none                 | I will not send you data              |
| 1  | unchoke     | none                 | I will send you data now              |
| 2  | interested  | none                 | I want data from you                  |
| 3  | uninterested| none                 | I don't want data from you            |
| 4  | have        | 4 bytes: piece index | I finished downloading this piece     |
| 5  | bitfield    | N bytes              | Which pieces I have (one bit per piece)|
| 6  | request     | 12 bytes             | Please send me this block             |
| 7  | piece       | 8 + N bytes          | Here is the block data you requested  |
| 8  | cancel      | 12 bytes             | Nevermind, cancel that request        |

---

## Choking and Unchoking

This is BitTorrent's flow control and tit-for-tat mechanism.

- You start **choked** by default (peer won't send you anything)
- You must send `interested` to signal you want data
- The peer may send `unchoke` to let you download
- If they don't unchoke you → they're choking you (try another peer)

The real BitTorrent "tit-for-tat" algorithm unchokes peers who upload to you.
For our simple client, we just wait to be unchoked.

```
You → interested
Peer → unchoke        (hopefully)
You → request(...)
Peer → piece(...)
```

---

## The Bitfield Message

The `bitfield` message is usually the first message after the handshake.
It tells you which pieces the peer has, as a compact bitmask:

```
Byte 0: [piece7][piece6][piece5][piece4][piece3][piece2][piece1][piece0]
Byte 1: [piece15]...
```

Most significant bit first. If the peer has piece 0, bit 7 of byte 0 is set.

```c
// Check if peer has piece i
int peer_has_piece(uint8_t *bitfield, int piece_index) {
    int byte_index = piece_index / 8;
    int bit_index  = 7 - (piece_index % 8);
    return (bitfield[byte_index] >> bit_index) & 1;
}
```

---

## Requesting Blocks

Pieces are downloaded in **blocks** (sub-pieces). The standard block size
is **16 KiB (16384 bytes)**. This is important:

- **Piece**: as defined in the torrent (e.g. 256 KiB)
- **Block**: what you actually request (always 16 KiB, or less for last block)

The `request` message has 3 fields (all big-endian 32-bit ints):

```
[index][begin][length]
```

- `index`  = piece number
- `begin`  = byte offset within the piece
- `length` = block size (16384, or smaller for last block)

To download piece 2 (256 KiB piece), you send 16 requests:
```
request(2, 0,     16384)  → gets bytes 0–16383 of piece 2
request(2, 16384, 16384)  → gets bytes 16384–32767
...
request(2, 245760, 10240) → last block (if piece is 256000 bytes)
```

---

## The Piece Message

When the peer responds, you receive:

```
[index 4B][begin 4B][block data ...]
```

Store the data at `piece_data[begin .. begin+len]`.

When all blocks of a piece arrive, SHA-1 verify the complete piece.
If it matches → write to disk. If not → discard and re-request.

---

## TCP and Sockets in C

```c
// Create a TCP socket
int sock = socket(AF_INET, SOCK_STREAM, 0);

// Set up server address
struct sockaddr_in addr;
addr.sin_family = AF_INET;
addr.sin_port   = htons(peer_port);    // htons = host-to-network byte order
inet_pton(AF_INET, peer_ip, &addr.sin_addr);

// Connect
connect(sock, (struct sockaddr*)&addr, sizeof(addr));

// Send data
send(sock, buffer, len, 0);

// Receive data
recv(sock, buffer, len, MSG_WAITALL);  // MSG_WAITALL waits for full buffer

// Close
close(sock);
```

---

## Connection State Machine

```
CONNECTING
    ↓
HANDSHAKING  → send handshake, read handshake
    ↓
WAITING      → send interested, wait for unchoke
    ↓
DOWNLOADING  → send requests, receive piece messages
    ↓
DONE         → close connection
```
