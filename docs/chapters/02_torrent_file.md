# 📘 Chapter 2: The .torrent File

## What Is a .torrent File?

A `.torrent` file is a bencoded dictionary that describes:
- **What** to download (file names, sizes, SHA-1 hashes of every piece)
- **Where** to start (the tracker URL)
- **How** it's split (piece length)

It does NOT contain the actual data — just metadata about it.

---

## Top-Level Structure

```
d
  8:announce      → tracker URL (string)
  13:announce-list→ list of tracker URL lists (optional fallback trackers)
  7:comment       → human description (optional)
  10:created by   → software that made this torrent (optional)
  13:creation date→ Unix timestamp integer (optional)
  4:info          → the "info dictionary" (dict) ← MOST IMPORTANT
e
```

---

## The Info Dictionary

This is the core of the torrent. Its SHA-1 hash is the **info hash** —
the unique identifier for this torrent across all peers and trackers.

### Single-File Torrent

```
d
  4:name         → suggested filename (string)
  6:length       → total file size in bytes (int)
  12:piece length → bytes per piece (int, usually 2^18 = 262144)
  6:pieces       → concatenated raw 20-byte SHA-1 hashes (string)
e
```

### Multi-File Torrent

```
d
  4:name         → suggested directory name (string)
  12:piece length → bytes per piece (int)
  6:pieces       → concatenated raw 20-byte SHA-1 hashes (string)
  5:files        → list of file dicts:
    d
      6:length   → this file's size in bytes (int)
      4:path     → list of strings representing the path, e.g. ["dir","file.txt"]
    e
e
```

---

## The Info Hash — The Torrent's Identity

The **info hash** is computed by:
1. Re-encoding the `info` dictionary as bencoded bytes (exactly as they
   appear in the file — no reformatting!)
2. Computing SHA-1 of those bytes

```c
// Pseudo-code
uint8_t info_hash[20];
sha1(raw_info_bytes, raw_info_length, info_hash);
```

This 20-byte hash is used:
- In tracker requests: `?info_hash=<url-encoded 20 bytes>`
- In the peer handshake to identify which torrent you want
- To deduplicate torrents globally (no central registry needed!)

---

## The Pieces Field

This is the most important field. It's a raw byte string of length `N * 20`,
where `N` is the number of pieces.

```
pieces[0..19]   = SHA-1 of piece 0
pieces[20..39]  = SHA-1 of piece 1
pieces[40..59]  = SHA-1 of piece 2
...
```

After downloading each piece, you SHA-1 hash it and compare against this.
If they match → data is authentic. If not → discard and re-download.

This is BitTorrent's integrity mechanism. No central authority needed!

---

## Piece Count Calculation

```c
// Number of pieces = ceil(total_bytes / piece_length)
int num_pieces = (total_length + piece_length - 1) / piece_length;

// The last piece is usually smaller:
int last_piece_size = total_length % piece_length;
if (last_piece_size == 0) last_piece_size = piece_length;
```

---

## Key Structures in `torrent.h`

```c
typedef struct {
    char     name[256];         // file or directory name
    uint8_t  info_hash[20];     // SHA-1 of bencoded info dict
    long     total_length;      // total bytes to download
    int      piece_length;      // bytes per piece
    int      num_pieces;        // total number of pieces
    uint8_t *pieces_hash;       // raw SHA-1 hashes (num_pieces * 20 bytes)
    char     announce[512];     // tracker URL
} TorrentInfo;
```

---

## Why Are Keys Sorted?

The spec requires dictionary keys to be in **lexicographic order**.
This means the bencoded bytes of the info dict are always canonical —
everyone who has the same torrent will compute the same info hash.

If keys could be in any order, two clients with the same logical data
might produce different bencoded bytes → different info hashes → chaos!

---

## Exercise

Open any `.torrent` file in a hex editor (or use `xxd`):

```bash
xxd some.torrent | head -50
```

You should see it starts with `64` (ASCII `d` = start of dict).
Try to trace the structure manually!
