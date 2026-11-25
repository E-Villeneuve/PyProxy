"""The proxy server: forwarding, caching and HTTPS tunnels, all on one asyncio event loop."""
import asyncio
import json
import logging
import time
from dataclasses import dataclass
from urllib.parse import urlsplit

from .cache import Cache, cache_control, storable
from .http import (
    CHUNK, MAX_HEAD, HTTPError, Response, parse_request, parse_response,
    read_head, relay_body, request_framing, response_framing,
)

log = logging.getLogger('pyproxy')

VIA = '1.1 pyproxy'
CONDITIONAL = ('if-none-match', 'if-modified-since', 'if-match', 'if-unmodified-since', 'if-range')


class ResponseAborted(ConnectionError):
    """The response head was already sent, so the only way to report a failure is to close the connection."""


@dataclass
class Config:
    host: str = '127.0.0.1'
    port: int = 8888
    ttl: float = 60                       # freshness when the origin sends no caching headers
    cache_bytes: int = 64 * 2**20         # 0 turns the cache off
    max_entry_bytes: int = 8 * 2**20
    connect_timeout: float = 10
    read_timeout: float = 30              # waiting on the origin for a response or the next piece of body
    idle_timeout: float = 60              # idle keep-alive connections and tunnels
    connect_ports: frozenset | None = frozenset({443})  # ports CONNECT may reach; None allows any


@dataclass
class Stats:
    requests: int = 0
    hits: int = 0
    revalidated: int = 0
    misses: int = 0
    bypassed: int = 0
    tunnels: int = 0
    errors: int = 0

    def as_dict(self):
        stats = dict(vars(self))
        lookups = self.hits + self.revalidated + self.misses
        stats['hit_ratio'] = round((self.hits + self.revalidated) / lookups, 4) if lookups else 0.0
        return stats


class ProxyServer:
    def __init__(self, config=None, clock=time.monotonic):
        self.config = config or Config()
        c = self.config
        self.cache = Cache(c.cache_bytes, c.max_entry_bytes, c.ttl, clock) if c.cache_bytes > 0 else None
        self.stats = Stats()
        self._server = None
        self._clients = set()

    async def start(self):
        self._server = await asyncio.start_server(
            self._serve_client, self.config.host, self.config.port, limit=MAX_HEAD)
        return self

    @property
    def port(self):
        return self._server.sockets[0].getsockname()[1]

    async def serve_forever(self):
        await self._server.serve_forever()

    async def close(self):
        self._server.close()
        for writer in list(self._clients):
            writer.close()
        await self._server.wait_closed()

    # One client connection may carry many requests (keep-alive).
    async def _serve_client(self, reader, writer):
        self._clients.add(writer)
        try:
            while await self._serve_request(reader, writer):
                pass
        except (ConnectionError, asyncio.IncompleteReadError, TimeoutError):
            pass
        except Exception:
            log.exception('unexpected error')
        finally:
            self._clients.discard(writer)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def _serve_request(self, reader, writer):
        """Handle one request. Returns whether the client connection stays open."""
        try:
            async with asyncio.timeout(self.config.idle_timeout):
                head = await read_head(reader)
            if head is None:
                return False
            request = parse_request(head)
        except TimeoutError:
            return False
        except HTTPError as error:
            await self._send_error(writer, error)
            return False

        self.stats.requests += 1
        started = time.perf_counter()
        try:
            if request.method == 'CONNECT':
                keep, status, outcome = await self._tunnel(request, reader, writer)
            elif request.target == '/stats':
                keep, status, outcome = await self._send_stats(request, writer)
            else:
                keep, status, outcome = await self._forward(request, reader, writer)
        except HTTPError as error:
            await self._send_error(writer, error)
            keep, status, outcome = False, error.status, 'ERROR'
        log.info('%s %s -> %s %s (%.0f ms)', request.method, request.target, status, outcome,
                 (time.perf_counter() - started) * 1000)
        return keep

    async def _forward(self, request, reader, writer):
        url = urlsplit(request.target)
        if url.scheme != 'http' or not url.hostname:
            detail = ('HTTPS goes through a CONNECT tunnel; most clients do this for you.'
                      if url.scheme == 'https' else
                      'Send absolute URLs, e.g. curl -x http://localhost:8888 http://example.com/')
            raise HTTPError(400, 'Bad Request', detail)
        try:
            host, port = url.hostname, url.port or 80
        except ValueError:
            raise HTTPError(400, 'Bad Request', 'invalid port') from None
        path = (url.path or '/') + (f'?{url.query}' if url.query else '')
        key = f'{host}:{port}{path}'

        framing = request_framing(request.headers)
        keep = request.keep_alive()
        directives = cache_control(request.headers)
        use_cache = (
            self.cache is not None
            and request.method == 'GET'
            and framing[0] == 'none'
            and 'no-store' not in directives
            and 'authorization' not in request.headers
            and not any(name in request.headers for name in CONDITIONAL)
        )

        entry = self.cache.get(key, request.headers) if use_cache else None
        must_revalidate = ('no-cache' in directives or directives.get('max-age') == '0'
                           or 'no-cache' in request.headers.tokens('pragma'))
        if entry and not must_revalidate and entry.is_fresh(self.cache.clock()):
            self.stats.hits += 1
            await self._send_cached(writer, entry, 'HIT', keep)
            return keep, entry.response.status, 'HIT'

        outgoing = request.headers.without_hop_by_hop()
        outgoing.set('Host', url.netloc.rpartition('@')[2])
        outgoing.set('Connection', 'close')
        outgoing.add('Via', VIA)
        validators = entry.validators() if entry else {}
        for name, value in validators.items():
            outgoing.set(name, value)

        upstream_reader, upstream_writer = await self._open(host, port)
        try:
            upstream_writer.write(f'{request.method} {path} HTTP/1.1\r\n'.encode('latin-1')
                                  + outgoing.encode() + b'\r\n')

            async def to_upstream(data):
                upstream_writer.write(data)
                await upstream_writer.drain()

            await relay_body(reader, framing, to_upstream, self.config.read_timeout)
            await upstream_writer.drain()
            response = await self._read_response_head(upstream_reader, host)

            if validators and response.status == 304:
                self.cache.refresh(key, entry, response.headers)
                self.stats.revalidated += 1
                await self._send_cached(writer, entry, 'REVALIDATED', keep)
                return keep, entry.response.status, 'REVALIDATED'

            return await self._relay_response(
                request, response, upstream_reader, writer, key, keep, use_cache)
        finally:
            upstream_writer.close()

    async def _read_response_head(self, upstream_reader, host):
        try:
            async with asyncio.timeout(self.config.read_timeout):
                head = await read_head(upstream_reader)
        except TimeoutError:
            raise HTTPError(504, 'Gateway Timeout', f'{host} did not respond in time') from None
        except HTTPError:
            raise HTTPError(502, 'Bad Gateway', f'{host} sent a malformed response') from None
        if head is None:
            raise HTTPError(502, 'Bad Gateway', f'{host} closed the connection without responding')
        return parse_response(head)

    async def _relay_response(self, request, response, upstream_reader, writer, key, keep, use_cache):
        framing = response_framing(request.method, response.status, response.headers)
        store = use_cache and storable(request.headers, response.status, response.headers, self.cache.default_ttl)
        stored_headers = response.headers.without_hop_by_hop()
        keep = keep and framing[0] != 'eof'  # a body that ends at close can't share the connection
        outcome = 'MISS' if use_cache else 'BYPASS'

        headers = stored_headers.copy()
        headers.add('Via', VIA)
        headers.set('X-Cache', outcome)
        headers.set('Connection', 'keep-alive' if keep else 'close')
        writer.write(response.encode_head(headers))

        body = bytearray() if store else None

        async def to_client(data):
            nonlocal body
            writer.write(data)
            await writer.drain()
            if body is not None:
                body += data
                if len(body) > self.cache.max_entry_bytes:
                    body = None  # too big to cache; keep streaming

        try:
            await relay_body(upstream_reader, framing, to_client, self.config.read_timeout)
            await writer.drain()
        except (HTTPError, TimeoutError, asyncio.IncompleteReadError) as error:
            self.stats.errors += 1
            raise ResponseAborted(f'origin failed mid-response: {error!r}') from error

        if body is not None:
            if framing[0] == 'eof':
                stored_headers.set('Content-Length', str(len(body)))  # so cached copies can use keep-alive
            stored = Response(response.version, response.status, response.reason, stored_headers)
            self.cache.store(key, stored, bytes(body), request.headers)
        if use_cache:
            self.stats.misses += 1
        else:
            self.stats.bypassed += 1
        return keep, response.status, outcome

    async def _send_cached(self, writer, entry, outcome, keep):
        headers = entry.response.headers.copy()
        headers.set('Age', str(int(entry.age(self.cache.clock()))))
        headers.add('Via', VIA)
        headers.set('X-Cache', outcome)
        headers.set('Connection', 'keep-alive' if keep else 'close')
        writer.write(entry.response.encode_head(headers) + entry.body)
        await writer.drain()

    async def _tunnel(self, request, reader, writer):
        """CONNECT host:port: open a raw TCP tunnel, used for HTTPS. The proxy never sees the decrypted traffic."""
        host, sep, port = request.target.rpartition(':')
        host = host.strip('[]')
        if not sep or not host or not port.isdigit():
            raise HTTPError(400, 'Bad Request', 'CONNECT needs host:port')
        port = int(port)
        allowed = self.config.connect_ports
        if allowed is not None and port not in allowed:
            raise HTTPError(403, 'Forbidden', f'CONNECT to port {port} is not allowed')

        upstream_reader, upstream_writer = await self._open(host, port)
        self.stats.tunnels += 1
        try:
            writer.write(b'HTTP/1.1 200 Connection Established\r\nVia: ' + VIA.encode() + b'\r\n\r\n')
            await writer.drain()
            await asyncio.gather(self._pipe(reader, upstream_writer), self._pipe(upstream_reader, writer))
        finally:
            upstream_writer.close()
        return False, 200, 'TUNNEL'

    async def _pipe(self, reader, writer):
        try:
            while True:
                async with asyncio.timeout(self.config.idle_timeout):
                    data = await reader.read(CHUNK)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
            if writer.can_write_eof():
                writer.write_eof()  # pass the half-close on; the other direction may still be sending
        except (ConnectionError, TimeoutError, OSError):
            writer.close()

    async def _open(self, host, port):
        try:
            async with asyncio.timeout(self.config.connect_timeout):
                return await asyncio.open_connection(host, port, limit=MAX_HEAD)
        except TimeoutError:
            raise HTTPError(504, 'Gateway Timeout', f'timed out connecting to {host}:{port}') from None
        except OSError as error:
            raise HTTPError(502, 'Bad Gateway', f'could not connect to {host}:{port} ({error.strerror or error})') from None

    async def _send_stats(self, request, writer):
        stats = self.stats.as_dict()
        stats['cache_entries'] = len(self.cache) if self.cache is not None else 0
        stats['cache_bytes'] = self.cache.bytes_used if self.cache is not None else 0
        body = json.dumps(stats, indent=2).encode() + b'\n'
        keep = request.keep_alive() and request_framing(request.headers)[0] == 'none'
        writer.write(
            b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nCache-Control: no-store\r\n'
            + f'Content-Length: {len(body)}\r\nConnection: {"keep-alive" if keep else "close"}\r\n\r\n'.encode()
            + body)
        await writer.drain()
        return keep, 200, 'STATS'

    async def _send_error(self, writer, error):
        self.stats.errors += 1
        body = f'{error.status} {error.reason}\n{error.detail}\n'.encode()
        writer.write(
            f'HTTP/1.1 {error.status} {error.reason}\r\nContent-Type: text/plain; charset=utf-8\r\n'
            f'Content-Length: {len(body)}\r\nConnection: close\r\nVia: {VIA}\r\n\r\n'.encode() + body)
        try:
            await writer.drain()
        except ConnectionError:
            pass
