#!/usr/bin/env python3
"""A local stand-in for the duckdb-geo MCP server's `query` tool, for precompute.

The explorer's all-basins queries scan whole global layers (4.8 billion carbon cells
per layer) and run longer than the shared MCP server's 300 s limit. This serves the
same JSON-RPC `tools/call` → `query` request from a local DuckDB, reading the same
public s3:// paths, so the stock glen-charts precompute runs unchanged:

    python3 scripts/local-mcp.py &                       # listens on 127.0.0.1:18765
    MCP_URL=http://127.0.0.1:18765/mcp MCP_TIMEOUT_MS=1800000 \
        npx github:SchmidtDSE/glen-charts#<tag> precompute --site explorer results --param level=all

It answers only `query`, holds no credentials (the buckets are public), and binds to
localhost. Size the machine first: the carbon query wants ~20 GB of memory and many
cores. DUCKDB_MEMORY (default 20GB) caps DuckDB's memory; past it, it spills to
DUCKDB_TMP (default /tmp/duckdb-spill).
"""
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import duckdb

PORT = int(os.environ.get("PORT", "18765"))

con = duckdb.connect()
con.sql("INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial; INSTALL h3 FROM community; LOAD h3;")
con.sql(
    "CREATE SECRET (TYPE s3, ENDPOINT 's3-west.nrp-nautilus.io', URL_STYLE 'path', "
    "USE_SSL true, KEY_ID '', SECRET '')"
)
con.sql("SET preserve_insertion_order = false")
# Leave headroom under the container's limit; joins and aggregates spill to disk past it.
con.sql(f"SET memory_limit = '{os.environ.get('DUCKDB_MEMORY', '20GB')}'")
con.sql(f"SET temp_directory = '{os.environ.get('DUCKDB_TMP', '/tmp/duckdb-spill')}'")


def run(sql, result):
    try:
        rows = con.sql(sql).fetchall()
        result["text"] = "" if not rows or rows[0][0] is None else rows[0][0]
    except Exception as err:
        result["error"] = str(err)


class Handler(BaseHTTPRequestHandler):
    # HTTP/1.0, so the body ends when the connection closes: headers go out at once and
    # a space every 20 s keeps the connection alive through long scans (Node's fetch
    # otherwise gives up after 300 s; the client trims the padding).
    protocol_version = "HTTP/1.0"

    def do_POST(self):
        msg = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
        params = msg.get("params", {})
        out = {"jsonrpc": "2.0", "id": msg.get("id")}
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        if msg.get("method") != "tools/call" or params.get("name") != "query":
            out["error"] = {"code": -32601, "message": "only tools/call → query is served"}
        else:
            sql = params.get("arguments", {}).get("sql", "")
            result = {}
            worker = threading.Thread(target=run, args=(sql, result))
            t0 = time.time()
            worker.start()
            while worker.is_alive():
                worker.join(20)
                if worker.is_alive():
                    self.wfile.write(b" ")
                    self.wfile.flush()
            if "error" in result:
                out["result"] = {"isError": True, "content": [{"type": "text", "text": f"SQL Error: {result['error']}"}]}
                print(f"error {time.time() - t0:.0f}s: {result['error'][:300]}", file=sys.stderr)
            else:
                out["result"] = {"content": [{"type": "text", "text": result["text"]}]}
                print(f"ok {time.time() - t0:.0f}s, {len(result['text']) / 1e6:.1f} MB", file=sys.stderr)
        self.wfile.write(json.dumps(out).encode())

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    print(f"local MCP query server on http://127.0.0.1:{PORT}/mcp", file=sys.stderr)
    HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
