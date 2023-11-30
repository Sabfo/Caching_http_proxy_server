from config import N_WORKERS, BLOCK_SIZE, MAX_CAPACITY, TRANSITION_HEADERS, SCALE_FACTOR

from typing import Dict, Literal
from dataclasses import dataclass
from datetime import datetime
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from concurrent.futures import ThreadPoolExecutor

import requests


@dataclass
class Item:
    http_headers: dict
    content: bytearray
    content_size: int
    last_call_date: datetime
    status: Literal['start', 'done'] = 'start'
    index_cursor: int = 0


class Repository:
    def __init__(self):
        self.capacity = MAX_CAPACITY


class InMemoryRepository(Repository):
    def __init__(self):
        super().__init__()
        self.cache: Dict[str, Item] = dict()

    def init_item(self, url: str, headers: Dict[str, str]) -> None:
        size = int(headers.get('content-length', -1))
        # Заранее по возможности выделяем память на весь контент, чтобы потом каждый раз не расширять bytearray
        content = bytearray(size) if size != -1 else bytearray(BLOCK_SIZE * 2)
        self.cache[url] = Item(http_headers=headers, content=content,
                               content_size=size, last_call_date=datetime.now())

    def append_content(self, url: str, content_chunk: bytes) -> None:
        try:
            index = self.cache[url].index_cursor
            if self.cache[url].content_size == -1:
                cache_len = len(self.cache[url].content)
                if index + len(content_chunk) >= cache_len:
                    new_buffer = bytearray(max(int(cache_len * SCALE_FACTOR), cache_len + BLOCK_SIZE))
                    new_buffer[:index] = self.cache[url].content
                    self.cache[url].content = new_buffer

            assert index + len(content_chunk) <= len(self.cache[url].content), 'Overflow cache buffer'
            self.cache[url].content[index:index + len(content_chunk)] = content_chunk
            self.cache[url].index_cursor += len(content_chunk)
            self.cache[url].last_call_date = datetime.now()
        except MemoryError as e:
            print(e)
            self.clean_old_items()

    def done_item(self, url: str) -> None:
        if self.cache[url].content_size == -1:
            # Меняем размер контента на реальный, чтоб не занимать лишней памяти
            self.cache[url].content = bytearray(self.cache[url].content[:self.cache[url].index_cursor])
            self.cache[url].content_size = self.cache[url].index_cursor
        if 'content-length' not in self.cache[url].http_headers:
            self.cache[url].http_headers['content-length'] = self.cache[url].index_cursor
        self.cache[url].last_call_date = datetime.now()
        self.cache[url].status = 'done'

    def has_url(self, url) -> bool:
        return url in self.cache

    def clean_old_items(self):
        pass


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
        url = self.rfile.readline().strip().decode()
        print(url)
        # if repository.has_url(url):
        #     pass
        # else:
        self.first_occurrence(url)

    def first_occurrence(self, url: str):

        with requests.get(url=url, stream=True) as response:
            self.send_response(200)

            cached_headers = dict()
            for header_name in TRANSITION_HEADERS:
                header_ = response.headers.get(header_name, None)
                if header_:
                    self.send_header(header_name, header_)
                    cached_headers[header_name] = header_
                    # print('Header ', header_name, header_)
            self.end_headers()

            repository.init_item(url=url, headers=cached_headers)

            for chunk in response.iter_content(chunk_size=BLOCK_SIZE):
                self.wfile.write(chunk)
                repository.append_content(url, chunk)
                # print(len(chunk))
        repository.done_item(url)


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
