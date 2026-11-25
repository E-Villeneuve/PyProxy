"""Test fixtures: a scriptable fake origin server and a proxy, both on a background event loop.

Tests talk to the proxy with the standard library's blocking http.client, so the
proxy is exercised by a client that shares none of its parsing code.
"""
import asyncio
import http.client
import threading
from dataclasses import dataclass, field

import pytest

from pyproxy import Config, ProxyServer


class LoopThread:
    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()

    def run(self, coro, timeout=10):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout)

    def stop(self):
        self.run(self._cancel_leftovers())  # e.g. an origin still sleeping after a timeout test
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(5)
        self.loop.close()

    @staticmethod
    async def _cancel_leftovers():
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@dataclass
class Route:
    status: int = 200
    headers: list = field(default_factory=list)
    body: bytes = b'hello'
    delay: float = 0
    framing: str = 'length'  # 'length', 'chunked' or 'eof' (no length, body ends when the connection closes)


class FakeOrigin:
    """An HTTP server whose responses each test scripts through `routes`."""

    def __init__(self):
        self.routes = {}
        self.hits = {}
        self.requests = []
        self.server = None

    @property
    def port(self):
        return self.server.sockets[0].getsockname()[1]

    def url(self, path):
        return f'http://127.0.0.1:{self.port}{path}'

    async def start(self):
        self.server = await asyncio.start_server(self._handle, '127.0.0.1', 0)

    async def close(self):
        self.server.close()

    async def _handle(self, reader, writer):
        try:
            head = await reader.readuntil(b'\r\n\r\n')
            lines = head.decode('latin-1').split('\r\n')
            method, target, _ = lines[0].split(' ')
            headers = {}
            for line in lines[1:]:
                if ':' in line:
                    name, value = line.split(':', 1)
                    headers[name.strip().lower()] = value.strip()
            body = await reader.readexactly(int(headers['content-length'])) if 'content-length' in headers else b''
            path = target.split('?')[0]
            self.requests.append({'method': method, 'target': target, 'headers': headers, 'body': body})
            self.hits[path] = self.hits.get(path, 0) + 1

            route = self.routes.get(path, Route(404, body=b'not found'))
            if callable(route):
                route = route(method, headers, body)
            if route.delay:
                await asyncio.sleep(route.delay)

            response_headers = list(route.headers)
            etag = dict((k.lower(), v) for k, v in response_headers).get('etag')
            status, payload = route.status, route.body
            if etag and headers.get('if-none-match') == etag:
                status, payload = 304, b''

            out = [f'HTTP/1.1 {status} X'] + [f'{k}: {v}' for k, v in response_headers]
            if status == 304:
                pass
            elif route.framing == 'chunked':
                out.append('Transfer-Encoding: chunked')
            elif route.framing == 'length':
                out.append(f'Content-Length: {len(payload)}')
            out.append('Connection: close')
            writer.write(('\r\n'.join(out) + '\r\n\r\n').encode('latin-1'))
            if status != 304:
                if route.framing == 'chunked':
                    for i in range(0, len(payload), 3):
                        piece = payload[i:i + 3]
                        writer.write(f'{len(piece):x}\r\n'.encode() + piece + b'\r\n')
                    writer.write(b'0\r\n\r\n')
                else:
                    writer.write(payload)
            await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


@dataclass
class Result:
    status: int
    headers: object
    body: bytes


class ProxyHandle:
    def __init__(self, server, loop):
        self.server = server
        self.loop = loop

    @property
    def port(self):
        return self.server.port

    @property
    def stats(self):
        return self.server.stats

    def connection(self, timeout=5):
        return http.client.HTTPConnection('127.0.0.1', self.port, timeout=timeout)

    def request(self, method, url, body=None, headers=None, conn=None, timeout=5):
        own = conn is None
        conn = conn or self.connection(timeout)
        try:
            conn.request(method, url, body=body, headers=headers or {})
            resp = conn.getresponse()
            return Result(resp.status, resp.headers, resp.read())
        finally:
            if own:
                conn.close()

    def get(self, url, **kwargs):
        return self.request('GET', url, **kwargs)


@pytest.fixture(scope='session')
def loop():
    thread = LoopThread()
    yield thread
    thread.stop()


@pytest.fixture
def origin(loop):
    server = FakeOrigin()
    loop.run(server.start())
    yield server
    loop.run(server.close())


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def make_proxy(loop, origin, clock):
    started = []

    def make(**overrides):
        settings = {'port': 0, 'connect_ports': frozenset({origin.port}), 'ttl': 60}
        settings.update(overrides)
        server = loop.run(ProxyServer(Config(**settings), clock=clock).start())
        started.append(server)
        return ProxyHandle(server, loop)

    yield make
    for server in started:
        loop.run(server.close())


@pytest.fixture
def proxy(make_proxy):
    return make_proxy()
