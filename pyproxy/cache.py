"""An in-memory, size-bounded LRU cache that follows the HTTP caching rules (RFC 9111) a shared proxy needs."""
import time
from collections import OrderedDict
from email.utils import parsedate_to_datetime

# Statuses this proxy will store. Others always go straight to the origin.
CACHEABLE_STATUS = {200, 203, 301, 308}

# Header fields a 304 Not Modified may update on the stored response (RFC 9111, 4.3.4).
NOT_UPDATED_BY_304 = {'content-length', 'content-encoding', 'transfer-encoding', 'content-range'}


def cache_control(headers):
    """Cache-Control directives as {name: value or None}."""
    directives = {}
    for value in headers.get_all('cache-control'):
        for part in value.split(','):
            name, _, arg = part.strip().partition('=')
            if name:
                directives[name.lower()] = arg.strip('"') or None
    return directives


def _seconds(value):
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _http_date(value):
    try:
        return parsedate_to_datetime(value).timestamp()
    except (TypeError, ValueError, IndexError):
        return None


def explicit_freshness(headers):
    """Whether the origin set the lifetime itself, rather than leaving it to the default TTL."""
    cc = cache_control(headers)
    return bool({'no-cache', 's-maxage', 'max-age'} & cc.keys()) or 'expires' in headers


def initial_age(headers):
    """Age the response already had when it arrived. The default TTL counts from when the proxy stores it."""
    return _seconds(headers.get('age')) if explicit_freshness(headers) else 0


def freshness_lifetime(headers, default_ttl):
    """How long a response stays fresh, in seconds, before it must be revalidated."""
    cc = cache_control(headers)
    if 'no-cache' in cc:
        return 0
    if 's-maxage' in cc:
        return _seconds(cc['s-maxage'])
    if 'max-age' in cc:
        return _seconds(cc['max-age'])
    if 'expires' in headers:
        expires = _http_date(headers.get('expires'))
        if expires is None:
            return 0  # an invalid Expires means "already expired"
        date = _http_date(headers.get('date')) or time.time()
        return max(0, expires - date)
    return default_ttl


def storable(request_headers, status, response_headers, default_ttl):
    """Whether a response to a plain GET may be stored by a shared cache."""
    if status not in CACHEABLE_STATUS:
        return False
    cc = cache_control(response_headers)
    if 'no-store' in cc or 'private' in cc:
        return False
    if '*' in response_headers.tokens('vary') or 'set-cookie' in response_headers:
        return False
    if 'authorization' in request_headers and not ({'public', 's-maxage'} & cc.keys()):
        return False
    has_validator = 'etag' in response_headers or 'last-modified' in response_headers
    return freshness_lifetime(response_headers, default_ttl) > 0 or has_validator


class CacheEntry:
    def __init__(self, response, body, request_headers, lifetime, stored_at):
        self.response = response
        self.body = body
        self.lifetime = lifetime
        self.stored_at = stored_at
        self.initial_age = initial_age(response.headers)
        # Request header values the response varies on; a later request must match them.
        self.vary = {name: request_headers.get(name) for name in response.headers.tokens('vary')}

    @property
    def size(self):
        return len(self.body) + len(self.response.encode_head())

    def age(self, now):
        return self.initial_age + max(0, now - self.stored_at)

    def is_fresh(self, now):
        return self.age(now) < self.lifetime

    def validators(self):
        """Conditional request headers that ask the origin whether this copy is still current."""
        conditions = {}
        if etag := self.response.headers.get('etag'):
            conditions['If-None-Match'] = etag
        if modified := self.response.headers.get('last-modified'):
            conditions['If-Modified-Since'] = modified
        return conditions

    def matches(self, request_headers):
        return all(request_headers.get(name) == value for name, value in self.vary.items())


class Cache:
    def __init__(self, max_bytes, max_entry_bytes, default_ttl, clock=time.monotonic):
        self.max_bytes = max_bytes
        self.max_entry_bytes = min(max_entry_bytes, max_bytes)
        self.default_ttl = default_ttl
        self.clock = clock
        self.bytes_used = 0
        self._entries = OrderedDict()

    def __len__(self):
        return len(self._entries)

    def __contains__(self, key):
        return key in self._entries

    def get(self, key, request_headers):
        entry = self._entries.get(key)
        if entry is None or not entry.matches(request_headers):
            return None
        self._entries.move_to_end(key)
        return entry

    def store(self, key, response, body, request_headers):
        """Store a response. Returns False when it is too large to keep."""
        lifetime = freshness_lifetime(response.headers, self.default_ttl)
        entry = CacheEntry(response, body, request_headers, lifetime, self.clock())
        if entry.size > self.max_entry_bytes:
            return False
        self.remove(key)
        self._entries[key] = entry
        self.bytes_used += entry.size
        while self.bytes_used > self.max_bytes:
            _, evicted = self._entries.popitem(last=False)
            self.bytes_used -= evicted.size
        return True

    def refresh(self, key, entry, not_modified_headers):
        """Apply a 304 Not Modified: update stored headers and restart the freshness clock."""
        tracked = self._entries.get(key) is entry  # it may have been evicted while revalidating
        if tracked:
            self.bytes_used -= entry.size
        entry.response.headers.remove('age')  # the old Age no longer applies
        for name, value in not_modified_headers.without_hop_by_hop():
            if name.lower() not in NOT_UPDATED_BY_304:
                entry.response.headers.set(name, value)
        entry.lifetime = freshness_lifetime(entry.response.headers, self.default_ttl)
        entry.stored_at = self.clock()
        entry.initial_age = initial_age(entry.response.headers)
        if tracked:
            self.bytes_used += entry.size

    def remove(self, key):
        entry = self._entries.pop(key, None)
        if entry is not None:
            self.bytes_used -= entry.size
