# 📘 Chapter 1: Bencoding

## What Is Bencoding?

Bencoding (pronounced "bee-encoding") is the data serialization format used
throughout the BitTorrent protocol. It's how `.torrent` files store their
metadata, and how trackers and peers exchange structured data.

Think of it like a much simpler version of JSON or MessagePack — but designed
to be unambiguous and easy to parse with minimal code.

---

## The 4 Data Types

Bencoding supports exactly 4 types:

### 1. Integer

```
i<number>e
```

Examples:
- `i42e`    → the integer 42
- `i-7e`    → the integer -7
- `i0e`     → the integer 0

⚠️ `i-0e` is invalid. Leading zeros (like `i07e`) are also invalid.

---

### 2. Byte String (or just "string")

```
<length>:<data>
```

The length is written in ASCII decimal, then a colon, then exactly that many bytes.

Examples:
- `4:spam`        → the string "spam"
- `11:hello world` → the string "hello world"
- `0:`            → the empty string

Strings are **byte strings** — not necessarily UTF-8 text. SHA-1 hashes are
stored as raw 20-byte strings, not hex.

---

### 3. List

```
l<item1><item2>...e
```

A list of any bencoded values, terminated with `e`.

Example:
- `l4:spami42ee` → ["spam", 42]
- `lli1ei2eeli3ei4eee` → [[1, 2], [3, 4]]

Lists can be nested and can contain mixed types.

---

### 4. Dictionary

```
d<key1><value1><key2><value2>...e
```

Keys MUST be byte strings. Keys MUST be in **lexicographic (sorted) order**.
This is a strict requirement of the spec — it ensures dictionaries are
canonical and hash consistently.

Example:
- `d3:cow3:moo4:spam4:eggse` → {"cow": "moo", "spam": "eggs"}
- `d4:listli1ei2eee`          → {"list": [1, 2]}

---

## Why No Floats?

Bencoding deliberately omits floating-point numbers. File sizes, piece counts,
and timestamps are all representable as integers. This simplicity avoids
platform-specific float encoding differences.

---

## Parsing Strategy

We use a **recursive descent parser** — a technique where each type has its
own parse function, and they call each other as needed.

```
parse_value()
  ├── if 'i' → parse_integer()
  ├── if 'l' → parse_list()  → calls parse_value() for each item
  ├── if 'd' → parse_dict()  → calls parse_value() for keys and values
  └── if digit → parse_string()
```

The parser advances a `cursor` pointer through the raw bytes and returns a
tree of `BencodeNode` structs.

---

## Memory Model

Each `BencodeNode` is heap-allocated. Lists and dicts store their children as
arrays of pointers. When you're done, call `bencode_free()` to walk the tree
and free everything.

```
BencodeNode (DICT)
├── key: "announce" → BencodeNode (STRING) "http://tracker.example.com"
├── key: "info"     → BencodeNode (DICT)
│   ├── key: "name"         → BencodeNode (STRING) "example.iso"
│   ├── key: "piece length" → BencodeNode (INT)    524288
│   └── key: "pieces"       → BencodeNode (STRING) <raw SHA-1 hashes>
└── key: "comment" → BencodeNode (STRING) "Created by mktorrent"
```

---

## Key Functions in `bencode.c`

| Function               | What it does                             |
|------------------------|------------------------------------------|
| `bencode_parse()`      | Entry point — parses a full bencoded buffer |
| `parse_integer()`      | Reads `i<num>e`                          |
| `parse_string()`       | Reads `<len>:<data>`                     |
| `parse_list()`         | Reads `l...e`, calls parse_value() for each item |
| `parse_dict()`         | Reads `d...e`, parses key-value pairs    |
| `bencode_dict_get()`   | Looks up a key in a dict node            |
| `bencode_print()`      | Debug: pretty-prints the node tree       |
| `bencode_free()`       | Recursively frees all nodes              |

---

## Exercise

Try parsing these by hand before running the code:

1. `d6:lengthi12345e4:name8:test.txte`
2. `l3:fooli1ei2ei3eee`
3. `i-99e`

Check your answers against `bencode_print()` output.
