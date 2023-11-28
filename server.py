from config import N_WORKERS, BLOCK_SIZE, MAX_CAPACITY

from typing import Dict, Literal
from dataclasses import dataclass
from datetime import datetime
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from concurrent.futures import ThreadPoolExecutor

import requests


@dataclass
class Item:
    url: str
    http_headers: dict
    content: bytearray
    status: Literal['start', 'filled']
    last_call_date: datetime
    content_size: int = -1


class Repository:
    def __init__(self):
        self.capacity = MAX_CAPACITY


class InMemoryRepository(Repository):
    def __init__(self):
        super().__init__()
        self.blocks_by_url: Dict[str, Item] = dict()


class ThreadExecutorHTTPServer(ThreadingHTTPServer):
    daemon_threads = False

    def __init__(self, server_address, RequestHandlerClass):
        super().__init__(server_address, RequestHandlerClass)
        self._thread_executor = ThreadPoolExecutor(max_workers=N_WORKERS)

    def process_request(self, request, client_address):
        """Override ThreadingMixIn method. Submit a task in thread executor to process the request."""
        # if self.block_on_close:
        #     vars(self).setdefault('_threads', ThreadPoolExecutor(max_workers=N_WORKERS))
        # t = threading.Thread(target=self.process_request_thread,
        #                      args=(request, client_address))
        # t.daemon = self.daemon_threads
        # self._threads.append(t)
        # t.start()
        self._thread_executor.submit(self.process_request_thread, request, client_address)

    def server_close(self):
        super().server_close()
        # self._threads.join()
        self._thread_executor.shutdown(wait=True)


class BufferedHttpHandler(BaseHTTPRequestHandler):
    """Обработчик с реализованным методом do_POST."""

    def do_POST(self):
        url = "https://httpbin.org/get"

        with requests.get(url=url, stream=True) as response:
            self.send_response(200)
            transition_headers = ['content-length', 'content-type', 'accept', 'accept-encoding', 'user-agent']
            for header_name in transition_headers:
                header_ = response.headers.get(header_name, None)
                if header_:
                    self.send_header(header_name, header_)
            self.end_headers()

            for chunk in response.iter_content(chunk_size=BLOCK_SIZE):
                self.wfile.write(chunk)


def run(server_class=ThreadExecutorHTTPServer, handler_class=BufferedHttpHandler):
    server_address = ('', 8001)
    httpd = server_class(server_address, handler_class)
    print('Start server.')
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.server_close()


if __name__ == '__main__':
    repository = InMemoryRepository()
    run()
