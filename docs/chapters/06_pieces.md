# 📘 Chapter 6: Piece Management

## The Big Picture

Downloading a torrent involves coordinating:
1. Which pieces do we still need?
2. Which peers have which pieces?
3. Which blocks within a piece have we received?
4. When is a piece complete? Does it verify?
5. How do we write assembled pieces to disk?

This is the **piece manager's** job.

---

## Data Structures

### Piece State

Each piece can be in one of these states:

```
PIECE_EMPTY      → not started
PIECE_IN_FLIGHT  → requested from a peer, waiting for blocks
PIECE_COMPLETE   → all blocks received AND SHA-1 verified
PIECE_FAILED     → SHA-1 verification failed (will retry)
```

### Block Tracking

Within a piece, we track which 16 KiB blocks have arrived:

```c
typedef struct {
    uint8_t *data;          // piece data buffer
    uint8_t *block_received;// bool array: has block i arrived?
    int      num_blocks;    // number of 16KiB blocks in this piece
    int      blocks_done;   // how many blocks received so far
    int      state;         // EMPTY / IN_FLIGHT / COMPLETE / FAILED
} Piece;
```

---

## Piece Selection Strategy

**Rarest First**: prefer pieces that fewer peers have.
This improves swarm health — rare pieces get distributed faster.

For our simple client, we use **sequential** ordering (simplest to implement
and good enough for learning):

```c
int select_next_piece(PieceManager *pm) {
    for (int i = 0; i < pm->num_pieces; i++) {
        if (pm->pieces[i].state == PIECE_EMPTY) {
            return i;
        }
    }
    return -1;  // all done!
}
```

---

## Block Requests

For each piece, we compute how many 16 KiB blocks it contains:

```c
#define BLOCK_SIZE 16384   // 16 KiB — the standard block size

int blocks_in_piece(TorrentInfo *t, int piece_index) {
    int piece_len;
    if (piece_index == t->num_pieces - 1) {
        // Last piece may be smaller
        piece_len = t->total_length % t->piece_length;
        if (piece_len == 0) piece_len = t->piece_length;
    } else {
        piece_len = t->piece_length;
    }
    return (piece_len + BLOCK_SIZE - 1) / BLOCK_SIZE;
}
```

---

## Receiving a Block

When a `piece` message arrives:

```c
void on_block_received(PieceManager *pm, int piece_idx,
                       int begin, uint8_t *data, int length) {
    Piece *p = &pm->pieces[piece_idx];
    int block_idx = begin / BLOCK_SIZE;

    // Copy into piece buffer
    memcpy(p->data + begin, data, length);

    // Mark block as received
    if (!p->block_received[block_idx]) {
        p->block_received[block_idx] = 1;
        p->blocks_done++;
    }

    // Check if piece is complete
    if (p->blocks_done == p->num_blocks) {
        if (verify_piece(pm->torrent, piece_idx, p->data)) {
            p->state = PIECE_COMPLETE;
            write_piece_to_disk(pm, piece_idx);
        } else {
            p->state = PIECE_FAILED;
            // Reset and retry
            memset(p->block_received, 0, p->num_blocks);
            p->blocks_done = 0;
            p->state = PIECE_EMPTY;
        }
    }
}
```

---

## Verifying a Piece

```c
int verify_piece(TorrentInfo *t, int piece_idx, uint8_t *data) {
    int piece_len = get_piece_length(t, piece_idx);

    uint8_t computed_hash[20];
    sha1(data, piece_len, computed_hash);

    uint8_t *expected_hash = t->pieces_hash + (piece_idx * 20);
    return memcmp(computed_hash, expected_hash, 20) == 0;
}
```

---

## Writing to Disk

### Single File

```c
void write_piece_to_disk(PieceManager *pm, int piece_idx) {
    long offset = (long)piece_idx * pm->torrent->piece_length;
    int  len    = get_piece_length(pm->torrent, piece_idx);

    fseek(pm->file, offset, SEEK_SET);
    fwrite(pm->pieces[piece_idx].data, 1, len, pm->file);
    fflush(pm->file);

    printf("[%d/%d] Piece %d written ✓\n",
           pm->completed + 1, pm->num_pieces, piece_idx);
    pm->completed++;
}
```

### Multi-File

For multi-file torrents, pieces can span file boundaries. You need to
calculate which file(s) each piece overlaps and split writes accordingly.

This is left as an exercise — single-file mode is implemented here.

---

## Progress Display

```
Pieces:   [############################....] 87% (43/50)
Speed:    2.3 MB/s download
Left:     12.4 MB
```

Track:
- `completed` = pieces with state PIECE_COMPLETE
- Download speed = bytes received / time elapsed

---

## Memory Considerations

Don't keep all pieces in RAM at once! For a 10 GB torrent with 512 KiB pieces,
that's 20,000 pieces × 512 KiB = 10 GB of RAM.

Instead: allocate piece buffers on demand, free after writing to disk.

```c
// Allocate when we start downloading a piece
piece->data = malloc(piece_length);

// Free after writing to disk
free(piece->data);
piece->data = NULL;
```
