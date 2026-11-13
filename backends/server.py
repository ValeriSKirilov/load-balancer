"""Tiny test backend for the load balancer.

Usage: python3 backend.py [PORT]        (default 9001)

Routes
  /, /fast      ~2 ms response
  /heavy        ~200 ms response
  /slow?ms=N    N ms response (0-5000). The load balancer's socket timeout is
                2 s, so ms > 2000 is a handy way to test backend failure.
  /health       instant "OK"
  anything else 404
"""

import signal
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

DEFAULT_PORT = 9001
FAST_DELAY_S = 0.002
HEAVY_DELAY_S = 0.2
DEFAULT_SLOW_MS = 500
MAX_SLOW_MS = 5000


def parse_port(argv):
    if len(argv) < 2:
        return DEFAULT_PORT
    try:
        port = int(argv[1])
    except ValueError:
        sys.exit(f"[Backend] Invalid port '{argv[1]}': expected an integer")
    if not 1 <= port <= 65535:
        sys.exit(f"[Backend] Invalid port {port}: must be between 1 and 65535")
    return port


PORT = parse_port(sys.argv)


class BackendHandler(BaseHTTPRequestHandler):
    timeout = 5

    def _reply(self, status, text):
        body = text.encode()
        try:
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("X-Backend-Port", str(PORT))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Expose-Headers", "X-Backend-Port")
            self.send_header("Connection", "close")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        url = urlsplit(self.path)

        if url.path in ("/", "/fast"):
            time.sleep(FAST_DELAY_S)
            self._reply(200, f"Fast response from Backend on port {PORT}\n")
        elif url.path == "/heavy":
            time.sleep(HEAVY_DELAY_S)
            self._reply(200, f"Heavy response from Backend on port {PORT}\n")
        elif url.path == "/slow":
            raw = parse_qs(url.query).get("ms", [str(DEFAULT_SLOW_MS)])[0]
            try:
                ms = int(raw)
            except ValueError:
                self._reply(400, "ms must be an integer\n")
                return
            if not 0 <= ms <= MAX_SLOW_MS:
                self._reply(400, f"ms must be between 0 and {MAX_SLOW_MS}\n")
                return
            time.sleep(ms / 1000)
            self._reply(200, f"Slow ({ms} ms) response from Backend on port {PORT}\n")
        elif url.path == "/health":
            self._reply(200, "OK\n")
        else:
            self._reply(404, f"Not found on Backend port {PORT}\n")

    do_HEAD = do_GET

    def log_message(self, format, *args):
        return


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 128


def main():
    try:
        server = Server(("0.0.0.0", PORT), BackendHandler)
    except OSError as exc:
        sys.exit(f"[Backend] Cannot listen on port {PORT}: {exc}")

    def stop(signum, frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, stop)

    print(f"[Backend] Listening on port {PORT}...", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        print("Server stopped.")


if __name__ == "__main__":
    main()
