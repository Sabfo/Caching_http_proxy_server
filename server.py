from config import N_WORKERS, BLOCK_SIZE, MAX_CAPACITY

from typing import Dict, List
import time
import threading
from http.server import HTTPServer, ThreadingHTTPServer, BaseHTTPRequestHandler
import requests


class Repository:
    def __init__(self):
        self.capacity = MAX_CAPACITY


class InMemoryRepository(Repository):
    def __init__(self):
        super().__init__()
        self.blocks_by_url: Dict[str, List] = dict()


class HttpGetHandler(BaseHTTPRequestHandler):
    """Обработчик с реализованным методом do_GET."""

    def do_GET(self):
        url = "https://httpbin.org/get"
        with requests.get(url=url, stream=True) as response:
            self.send_response(200)
            transition_headers = ['content-length', 'content-type', 'accept', 'accept-encoding', 'user-agent']
            for header_name in transition_headers:
                header_ = response.headers.get(header_name, None)
                if header_:
                    self.send_header(header_name, header_)
                    print(header_name, header_)
            self.end_headers()

            for chunk in response.iter_content(chunk_size=BLOCK_SIZE):
                print(chunk, sep='')
                self.wfile.write(chunk)


def run(server_class=ThreadingHTTPServer, handler_class=HttpGetHandler):
    server_address = ('', 8000)
    httpd = server_class(server_address, handler_class)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.server_close()


if __name__ == '__main__':
    repository = InMemoryRepository()
    run()
