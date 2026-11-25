"""HTTP/1.1 message parsing and body framing.

The proxy relays bodies byte for byte, keeping the origin's framing
(Content-Length, chunked or read-until-close), so nothing is re-encoded.
"""
import asyncio

MAX_HEAD = 64 * 1024
CHUNK = 64 * 1024

# Headers that describe one connection and must not be forwarded (RFC 9110, 7.6.1).
# Transfer-Encoding is left out on purpose: bodies are relayed with their framing intact.
HOP_BY_HOP = {
    'connection', 'keep-alive', 'proxy-connection', 'proxy-authenticate',
    'proxy-authorization', 'te', 'trailer', 'upgrade',
}


class HTTPError(Exception):
    """A problem that maps to an HTTP error response."""

    def __init__(self, status, reason, detail=''):
        super().__init__(f'{status} {reason}')
        self.status = status
        self.reason = reason
        self.detail = detail


class Headers:
    """An ordered, case-insensitive list of header fields."""

    def __init__(self, items=None):
        self._items = list(items or [])

    def get(self, name, default=None):
        name = name.lower()
        for key, value in self._items:
            if key.lower() == name:
                return value
        return default

    def get_all(self, name):
        name = name.lower()
        return [value for key, value in self._items if key.lower() == name]

    def tokens(self, name):
        """Comma-separated values of every `name` field, lowercased."""
        return [t.strip().lower() for value in self.get_all(name) for t in value.split(',') if t.strip()]

    def remove(self, name):
        name = name.lower()
        self._items = [(k, v) for k, v in self._items if k.lower() != name]

    def set(self, name, value):
        self.remove(name)
        self._items.append((name, value))

    def add(self, name, value):
        self._items.append((name, value))

    def copy(self):
        return Headers(self._items)

    def without_hop_by_hop(self):
        drop = HOP_BY_HOP | set(self.tokens('connection'))
        drop.discard('transfer-encoding')
        return Headers((k, v) for k, v in self._items if k.lower() not in drop)

    def encode(self):
        return b''.join(f'{k}: {v}\r\n'.encode('latin-1') for k, v in self._items)

    def __contains__(self, name):
        return self.get(name) is not None

    def __iter__(self):
        return iter(self._items)


class Request:
    def __init__(self, method, target, version, headers):
        self.method = method
        self.target = target
        self.version = version
        self.headers = headers

    def keep_alive(self):
        if 'close' in self.headers.tokens('connection'):
            return False
        return self.version == 'HTTP/1.1'


class Response:
    def __init__(self, version, status, reason, headers):
        self.version = version
        self.status = status
        self.reason = reason
        self.headers = headers

    def encode_head(self, headers=None):
        headers = self.headers if headers is None else headers
        return f'HTTP/1.1 {self.status} {self.reason}\r\n'.encode('latin-1') + headers.encode() + b'\r\n'


async def read_head(reader):
    """Read up to the blank line ending a message head. Returns None on a clean EOF."""
    try:
        data = await reader.readuntil(b'\r\n\r\n')
    except asyncio.IncompleteReadError as e:
        if not e.partial.strip():
            return None
        raise HTTPError(400, 'Bad Request') from None
    except asyncio.LimitOverrunError:
        raise HTTPError(431, 'Request Header Fields Too Large') from None
    return data.lstrip(b'\r\n')


def _parse_fields(lines):
    headers = Headers()
    for line in lines:
        if not line:
            continue
        name, sep, value = line.partition(':')
        if not sep or not name or name != name.strip():
            raise HTTPError(400, 'Bad Request')
        headers.add(name, value.strip())
    return headers


def parse_request(data):
    lines = data.decode('latin-1').split('\r\n')
    parts = lines[0].split(' ')
    if len(parts) != 3 or not parts[2].startswith('HTTP/1.'):
        raise HTTPError(400, 'Bad Request')
    method, target, version = parts
    return Request(method.upper(), target, version, _parse_fields(lines[1:]))


def parse_response(data):
    lines = data.decode('latin-1').split('\r\n')
    version, _, rest = lines[0].partition(' ')
    code, _, reason = rest.partition(' ')
    if not version.startswith('HTTP/1.') or not code.isdigit():
        raise HTTPError(502, 'Bad Gateway')
    return Response(version, int(code), reason, _parse_fields(lines[1:]))


def _content_length(headers, error_status):
    values = {v.strip() for v in headers.get_all('content-length')}
    if not values:
        return None
    if len(values) > 1 or not values.pop().isdigit():
        raise HTTPError(error_status, 'Bad Request' if error_status == 400 else 'Bad Gateway')
    return int(headers.get('content-length'))


def request_framing(headers):
    """('none' | 'length' | 'chunked', length) for a request body."""
    if 'chunked' in headers.tokens('transfer-encoding'):
        return 'chunked', None
    length = _content_length(headers, 400)
    return ('length', length) if length else ('none', 0)


def response_framing(method, status, headers):
    """('none' | 'length' | 'chunked' | 'eof', length) for a response body."""
    if method == 'HEAD' or 100 <= status < 200 or status in (204, 304):
        return 'none', 0
    if 'chunked' in headers.tokens('transfer-encoding'):
        return 'chunked', None
    length = _content_length(headers, 502)
    if length is not None:
        return 'length', length
    return 'eof', None


async def _read(reader, n, timeout):
    async with asyncio.timeout(timeout):
        return await reader.read(n)


async def _read_line(reader, timeout):
    async with asyncio.timeout(timeout):
        return await reader.readuntil(b'\r\n')


async def relay_body(reader, framing, write, timeout):
    """Copy one body from `reader` to the async callable `write`, framing included."""
    kind, length = framing
    if kind == 'none':
        return
    if kind == 'length':
        remaining = length
        while remaining:
            data = await _read(reader, min(CHUNK, remaining), timeout)
            if not data:
                raise asyncio.IncompleteReadError(b'', remaining)
            remaining -= len(data)
            await write(data)
    elif kind == 'eof':
        while data := await _read(reader, CHUNK, timeout):
            await write(data)
    else:
        while True:
            line = await _read_line(reader, timeout)
            await write(line)
            try:
                size = int(line.split(b';')[0].strip(), 16)
            except ValueError:
                raise HTTPError(502, 'Bad Gateway') from None
            if size == 0:
                while True:  # optional trailers, then a blank line
                    trailer = await _read_line(reader, timeout)
                    await write(trailer)
                    if trailer == b'\r\n':
                        return
            await relay_body(reader, ('length', size + 2), write, timeout)  # data plus its CRLF
