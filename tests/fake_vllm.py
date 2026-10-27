#!/usr/bin/env python3
"""Fake vLLM ZMQ KV-event publisher for smoke-testing kinference.

Wire format (matches vLLM's `ZmqEventPublisher`):
    multipart frame 0: empty (no topic filter)
    multipart frame 1: 8 bytes big-endian sequence number
    multipart frame 2: msgpack-encoded EventBatch

Usage:
    pip install msgspec pyzmq
    python tests/fake_vllm.py --endpoint tcp://*:5557 --hashes 100 200 300
"""

from __future__ import annotations

import argparse
import struct
import sys
import time
from typing import Optional

import msgspec
import zmq


# Mirror vLLM's event types, encoded as tagged arrays.

class BlockStored(msgspec.Struct, array_like=True, tag=True, omit_defaults=True):
    block_hashes: list[int]
    parent_block_hash: Optional[int]
    token_ids: list[int]
    block_size: int
    lora_id: Optional[int] = None


class BlockRemoved(msgspec.Struct, array_like=True, tag=True, omit_defaults=True):
    block_hashes: list[int]


class AllBlocksCleared(msgspec.Struct, array_like=True, tag=True, omit_defaults=True):
    pass


class EventBatch(msgspec.Struct, array_like=True):
    ts: float
    events: list
    data_parallel_rank: Optional[int] = None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint", default="tcp://*:5557",
                    help="ZMQ PUB bind endpoint (default: tcp://*:5557)")
    ap.add_argument("--hashes", type=int, nargs="+", required=True,
                    help="block hashes to publish as one Stored event")
    ap.add_argument("--block-size", type=int, default=16)
    ap.add_argument("--remove", type=int, nargs="*", default=[],
                    help="optional hashes to publish as a Removed event after a delay")
    ap.add_argument("--wait", type=float, default=1.0,
                    help="seconds to wait after bind before publishing (let subscribers join)")
    args = ap.parse_args()

    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.PUB)
    sock.bind(args.endpoint)

    print(f"bound {args.endpoint}, waiting {args.wait}s for subscribers...", flush=True)
    time.sleep(args.wait)

    encoder = msgspec.msgpack.Encoder()
    seq = 0

    # Distinct tokens give each block a distinct tokens_hash, which
    # find_matches keys on.
    block_size = args.block_size
    num_blocks = len(args.hashes)
    token_ids = list(range(1, num_blocks * block_size + 1))
    stored = BlockStored(
        block_hashes=list(args.hashes),
        parent_block_hash=None,
        token_ids=token_ids,
        block_size=block_size,
    )
    batch = EventBatch(ts=time.time(), events=[stored])
    payload = encoder.encode(batch)
    sock.send_multipart([b"", struct.pack(">Q", seq), payload])
    print(f"seq={seq} sent BlockStored {num_blocks} blocks "
          f"(block_size={block_size}, tokens=[1..{num_blocks * block_size}])",
          flush=True)
    seq += 1

    if args.remove:
        time.sleep(0.5)
        removed = BlockRemoved(block_hashes=list(args.remove))
        batch = EventBatch(ts=time.time(), events=[removed])
        payload = encoder.encode(batch)
        sock.send_multipart([b"", struct.pack(">Q", seq), payload])
        print(f"seq={seq} sent BlockRemoved hashes={args.remove}", flush=True)
        seq += 1

    time.sleep(0.5)
    sock.close(0)
    ctx.term()
    return 0


if __name__ == "__main__":
    sys.exit(main())
