from typing import Dict, Literal, Iterator, Optional
from dataclasses import dataclass
from datetime import datetime
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from concurrent.futures import ThreadPoolExecutor
from time import sleep

import requests

from config import N_WORKERS, BLOCK_SIZE, MAX_CAPACITY, TRANSITION_HEADERS, SCALE_FACTOR


@dataclass
class Item:
    http_headers: Dict[str, str]
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
        self.cache: Dict[str, Optional[Item]] = dict()

    def lock_url(self, url: str) -> None:
        self.cache[url] = None

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

            # Происходит AssertError, когда сайт возвращает заголовок content-length со значением меньшим,
            # чем возвращает контента в действительности. (Content-length: value < actual content size)
            assert index + len(content_chunk) <= len(self.cache[url].content), 'Overflow cache buffer'
            self.cache[url].content[index:index + len(content_chunk)] = content_chunk
            self.cache[url].index_cursor += len(content_chunk)
            self.cache[url].last_call_date = datetime.now()
        except MemoryError as e:
            print(e)
            self.clean_old_items()
            self.append_content(url, content_chunk)

    def done_item(self, url: str) -> None:
        if self.cache[url].content_size == -1:
            # Меняем размер контента на реальный, чтоб не занимать лишней памяти
            self.cache[url].content = bytearray(self.cache[url].content[:self.cache[url].index_cursor])
            self.cache[url].content_size = self.cache[url].index_cursor
        if 'content-length' not in self.cache[url].http_headers:
            self.cache[url].http_headers['content-length'] = str(self.cache[url].index_cursor)
        self.cache[url].last_call_date = datetime.now()
        self.cache[url].status = 'done'

    def has_url(self, url: str) -> bool:
        return url in self.cache

    def is_done(self, url: str) -> bool:
        return self.cache[url].status == 'done'

    def blocked_until_item_init(self, url: str) -> None:
        """Blocked until the item in the cache is initialized"""
        while self.cache[url] is None:
            # print('sleep 50ms until item is initialized')
            sleep(0.05)

    def get_headers_by_url(self, url: str) -> dict:
        return self.cache[url].http_headers

    def get_ready_content(self, url: str) -> Iterator[bytearray]:
        content = self.cache[url].content.copy()
        for _ in range(0, len(content), BLOCK_SIZE):
            yield content[:BLOCK_SIZE]
            del content[:BLOCK_SIZE]

    def get_processing_content(self, url: str, idx: int = 0) -> Iterator[bytearray]:
        total_size = self.cache[url].content_size
        if total_size != -1:
            # print('total size is known')
            is_multiply = total_size % BLOCK_SIZE != 0
            for _ in range(0, total_size, BLOCK_SIZE):
                is_tail = idx + BLOCK_SIZE > total_size
                while ((idx + BLOCK_SIZE > self.cache[url].index_cursor) and (is_multiply or not is_tail)) or \
                      (not is_multiply and is_tail and self.cache[url].index_cursor != total_size):
                    # print('sleep 50ms')
                    sleep(0.05)
                yield self.cache[url].content[idx:idx + BLOCK_SIZE].copy()
                idx += BLOCK_SIZE
        else:
            # print('total size is unknown')
            while self.cache[url].content_size == -1:
                # print('sleep 50ms')
                sleep(0.05)
                while idx + BLOCK_SIZE <= self.cache[url].index_cursor:
                    yield self.cache[url].content[idx:idx + BLOCK_SIZE].copy()
                    idx += BLOCK_SIZE
            total_size = self.cache[url].content_size
            if idx >= total_size:
                return
            self.get_processing_content(url, idx)

    def clean_old_items(self):
        oldest_url = min(self.cache, key=lambda k: self.cache[k].last_call_date)
        del self.cache[oldest_url]


class ThreadExecutorHTTPServer(ThreadingHTTPServer):
    daemon_threads = False

    def __init__(self, server_address, RequestHandlerClass):
        super().__init__(server_address, RequestHandlerClass)
        self._thread_executor = ThreadPoolExecutor(max_workers=N_WORKERS)

    def process_request(self, request, client_address):
        """Override ThreadingMixIn method. Submit a task in thread executor to process the request."""
        self._thread_executor.submit(self.process_request_thread, request, client_address)

    def server_close(self):
        super().server_close()
        self._thread_executor.shutdown(wait=True)


class BufferedHttpHandler(BaseHTTPRequestHandler):
    """A handler with an implemented do_POST method. Reads the url string in bytes
    from the first line of the request body. If such the request is in the cache,
    then it sends from there in streaming mode, or it connects itself via URL and
    simultaneously writes data to the cache and sends it to the client."""

    def do_POST(self):
        url = self.rfile.readline().strip().decode()
        print(url)
        if repository.has_url(url):
            self.return_content_from_cache(url)
        else:
            self.first_occurrence(url)

    def first_occurrence(self, url: str):
        repository.lock_url(url)
        with requests.get(url=url, stream=True) as response:
            # print('Connection to the server has been established.', url)
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
                print(len(chunk))
        repository.done_item(url)

    def return_content_from_cache(self, url: str):
        repository.blocked_until_item_init(url)

        self.send_response(200)
        for k, v in repository.get_headers_by_url(url).items():
            self.send_header(k, v)
            # print('Cached header ', k, v)
        self.end_headers()

        if repository.is_done(url):
            for chunk in repository.get_ready_content(url):
                self.wfile.write(chunk)
                # print(len(chunk), 'ready')
        else:
            for chunk in repository.get_processing_content(url):
                self.wfile.write(chunk)
                # print(len(chunk), 'processing')


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
