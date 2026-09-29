#!/usr/bin/env python3
"""Generate the seed corpora in tests/fuzz/corpus/<target>/.

Seeds are small valid inputs that give each fuzzer a structured starting
point. Re-run after changing a target's input format:
    python3 tests/fuzz/make_seeds.py
"""
import hashlib, os, struct

HERE = os.path.dirname(os.path.abspath(__file__))

def be(x):
    if isinstance(x, int):   return b'i%de' % x
    if isinstance(x, str):   x = x.encode()
    if isinstance(x, bytes): return b'%d:' % len(x) + x
    if isinstance(x, list):  return b'l' + b''.join(be(i) for i in x) + b'e'
    if isinstance(x, dict):
        return b'd' + b''.join(be(k) + be(v) for k, v in sorted(x.items())) + b'e'
    raise TypeError(x)

def write(target, name, data):
    d = os.path.join(HERE, 'corpus', target)
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, name), 'wb') as f:
        f.write(data)

def msg(mid, payload=b''):
    return struct.pack('>IB', 1 + len(payload), mid) + payload

# ── torrents ──────────────────────────────────────────────────────────────
data = bytes(5)
single = be({'announce': 'http://tracker.example/announce',
             'announce-list': [['udp://a.example:1337/announce'], ['http://b.example/a']],
             'comment': 'information 12',
             'info': {'name': 'file.bin', 'piece length': 16384, 'length': len(data),
                      'pieces': hashlib.sha1(data).digest()}})
multi = be({'announce': 'udp://tracker.example:6969/announce',
            'info': {'name': 'dir', 'piece length': 32768,
                     'files': [{'length': 40000, 'path': ['a', 'b.txt']},
                               {'length': 0, 'path': ['empty']},
                               {'length': 1000, 'path': ['c.bin']}],
                     'pieces': b'\x11' * 40}})
for t in ('torrent', 'bencode'):
    write(t, 'single.torrent', single)
    write(t, 'multi.torrent', multi)
for name, v in {'int': b'i-42e', 'str': b'4:spam', 'list': b'l4:spami42ee',
                'dict': b'd3:bar4:spam3:fooi42ee', 'nested': b'lllleeee'}.items():
    write('bencode', name, v)

# ── magnet ────────────────────────────────────────────────────────────────
write('magnet', 'hex', b'magnet:?xt=urn:btih:c12fe1c06bba254a9dc9f519b335aa7c1367a88a'
                       b'&dn=Some+Name&tr=udp%3A%2F%2Ftracker.example%3A1337%2Fannounce')
write('magnet', 'base32', b'magnet:?xt=urn:btih:YEX6DQDLXISUVHOJ6UM3GNNKPQJWPKEK&tr=http://x/a')

# ── tracker (HTTP replies) ────────────────────────────────────────────────
compact = b''.join(struct.pack('>4sH', bytes([10, 0, 0, i]), 6881) for i in range(1, 4))
write('tracker', 'compact', be({'interval': 1800, 'peers': compact}))
write('tracker', 'dict', be({'interval': 60, 'peers': [{'ip': '10.0.0.1', 'port': 6881},
                                                        {'ip': '::1', 'port': 51413}]}))
write('tracker', 'peers6', be({'interval': 900, 'peers': compact,
                               'peers6': bytes(15) + b'\x01\x1a\xe1'}))
write('tracker', 'failure', be({'failure reason': 'unregistered torrent'}))

# ── dht (2-byte transaction id + KRPC reply) ──────────────────────────────
nodes = b''.join(bytes([i]) * 20 + bytes([10, 0, 1, i]) + b'\x1a\xe1' for i in range(1, 4))
write('dht', 'values', b'aa' + be({'t': b'aa', 'y': 'r',
                                  'r': {'id': b'N' * 20, 'token': 'tok',
                                        'values': [compact[:6], compact[6:12]]}}))
write('dht', 'nodes', b'bb' + be({'t': b'bb', 'y': 'r', 'r': {'id': b'M' * 20, 'nodes': nodes}}))

# ── ext (BEP 10 handshake and BEP 9 metadata messages) ───────────────────
write('ext', 'handshake', be({'m': {'ut_metadata': 3, 'ut_pex': 1},
                              'metadata_size': 31235, 'v': 'libtorrent/2.0'}))
write('ext', 'data', be({'msg_type': 1, 'piece': 0, 'total_size': 5}) + b'hello')
write('ext', 'reject', be({'msg_type': 2, 'piece': 1}))

# ── wire: byte 0 = mode (bit0 incoming, bit1 handshake done), then peer bytes
info_hash = b'I' * 20
hs = bytes([19]) + b'BitTorrent protocol' + bytes([0, 0, 0, 0, 0, 0x10, 0, 0]) \
     + info_hash + b'-XX0001-abcdefghijkl'
ext_hs = msg(20, b'\x00' + be({'m': {'ut_metadata': 1, 'ut_pex': 2}, 'reqq': 250}))
pex = msg(20, b'\x02' + be({'added': compact, 'added.f': b'\x00\x00\x00'}))
req = lambda i, b, n: msg(6, struct.pack('>III', i, b, n))
block = lambda i, b, n: msg(7, struct.pack('>II', i, b) + bytes(n))
write('wire', 'out_handshake', b'\x00' + hs + ext_hs + msg(5, b'\xfc') + msg(1))
write('wire', 'in_handshake', b'\x01' + hs + msg(2) + req(0, 0, 16384))
write('wire', 'download_piece', b'\x02' + msg(1) + msg(4, struct.pack('>I', 4))
      + block(3, 0, 16384) + block(3, 16384, 16384) + pex)
write('wire', 'bad_piece', b'\x02' + msg(1) + block(3, 0, 16384)
      + msg(7, struct.pack('>II', 3, 16384) + b'\xff' * 16384))
write('wire', 'serve', b'\x03' + msg(2) + req(0, 0, 16384) + req(2, 16384, 16384)
      + msg(8, struct.pack('>III', 2, 16384, 16384)) + b'\x00\x00\x00\x00')
print('seeds written to', os.path.join(HERE, 'corpus'))
