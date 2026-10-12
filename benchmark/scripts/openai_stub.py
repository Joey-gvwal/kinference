#!/usr/bin/env python3
"""Minimal stdlib-only OpenAI-compatible chat-completions stub for smoke-testing
the benchmark harness.

Serves POST /v1/chat/completions with `stream: true`, emitting a configurable
number of dummy content chunks with realistic inter-chunk delays, plus a final
`usage` chunk and `[DONE]`. No external dependencies — just CPython.

    python scripts/openai_stub.py --port 8000 \
        --ttft-ms 80 --itl-ms 20 --output-tokens 64

Then in another shell:

    ./bench --base-url http://localhost:8000 --model stub \
        --workload synthetic --concurrency 4 --total-requests 16
"""

from __future__ import annotations

import argparse
import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    ttft_ms = 80
    itl_ms = 20
    output_tokens = 64

    def log_message(self, fmt, *args):
        return  # silence default access log

    def do_POST(self):  # noqa: N802
        if self.path != "/v1/chat/completions":
            self.send_error(404)
            return

        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            self.send_error(400, "bad json")
            return

        msgs = body.get("messages", []) or []
        prompt_tokens = sum(len((m.get("content") or "").split()) for m in msgs)
        n_out = body.get("max_tokens") or self.output_tokens
        n_out = min(int(n_out), self.output_tokens)

        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()

        time.sleep(self.ttft_ms / 1000.0)
        for i in range(n_out):
            chunk = {
                "object": "chat.completion.chunk",
                "choices": [{"index": 0, "delta": {"content": f"tok{i} "}}],
            }
            self.wfile.write(f"data: {json.dumps(chunk)}\n\n".encode())
            self.wfile.flush()
            if i < n_out - 1:
                time.sleep(self.itl_ms / 1000.0)

        usage = {
            "choices": [],
            "usage": {
                "prompt_tokens": prompt_tokens,
                "completion_tokens": n_out,
                "total_tokens": prompt_tokens + n_out,
            },
        }
        self.wfile.write(f"data: {json.dumps(usage)}\n\n".encode())
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--ttft-ms", type=int, default=80)
    ap.add_argument("--itl-ms", type=int, default=20)
    ap.add_argument("--output-tokens", type=int, default=64)
    args = ap.parse_args()

    Handler.ttft_ms = args.ttft_ms
    Handler.itl_ms = args.itl_ms
    Handler.output_tokens = args.output_tokens

    srv = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"openai-stub listening on http://{args.host}:{args.port}")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()


if __name__ == "__main__":
    main()
