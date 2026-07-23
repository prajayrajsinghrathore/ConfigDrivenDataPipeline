"""Tiny controllable mock API for Phase 3B.4 e2e validation.

Endpoints:
  GET /items?id=N        -> rows from /srv/data.json with id > N (watermark semantics)
  GET /data?event_date=D -> fixed rows tagged with the requested partition date
"""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

DATA_FILE = "/srv/data.json"


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        if parsed.path == "/items":
            watermark = int(qs.get("id", ["0"])[0])
            with open(DATA_FILE) as f:
                rows = json.load(f)
            body = [r for r in rows if r["id"] > watermark]
        elif parsed.path == "/data":
            date = qs.get("event_date", ["unknown"])[0]
            body = [
                {"id": 1, "event_date": date, "value": f"row-1-{date}"},
                {"id": 2, "event_date": date, "value": f"row-2-{date}"},
            ]
        else:
            self.send_response(404)
            self.end_headers()
            return
        payload = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, fmt, *args):
        print("%s - %s" % (self.address_string(), fmt % args), flush=True)


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 8000), Handler).serve_forever()

# Usage (from repo root, PowerShell to avoid Git-Bash path mangling):
#   docker run -d --name mock-api --network docker_default `
#     -v "<abs-path-to-a-dir-containing-this-file-and-data.json>:/srv" `
#     python:3.13-alpine python /srv/mock_api.py
# Seed /srv/data.json with e.g. [{"id":1,"v":"a"}, ...]; append rows between
# incremental runs to produce a delta.
