import os
from http.server import HTTPServer, BaseHTTPRequestHandler

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(b"<h1>THE DOGMA FX SYSTEM</h1><p>It works!</p>")

port = int(os.environ.get("PORT", 8080))
HTTPServer(("0.0.0.0", port), Handler).serve_forever()
