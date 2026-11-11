import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 9001


class BackendHanlder(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/heavy"):
            time.sleep(0.2)
            body = f"Heavy response from Backend on port {PORT}\n".encode()
        else:
            time.sleep(0.002)
            body = f"Fast response from Backend on port {PORT}\n".encode()

        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.headers["Connection"] = "close"
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        return


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", PORT), BackendHanlder)
    print(f"[Backend] Listening on port {PORT}...")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        print("Server stopped.")
