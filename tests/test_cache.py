from pyproxy.cache import Cache, freshness_lifetime, storable
from pyproxy.http import Headers, Response


def response(*headers, status=200):
    return Response('HTTP/1.1', status, 'OK', Headers(headers))


def test_lru_evicts_least_recently_used_when_full():
    one = len(response().encode_head()) + 100
    cache = Cache(max_bytes=one * 2, max_entry_bytes=one, default_ttl=60, clock=lambda: 0)
    for key in ('a', 'b'):
        cache.store(key, response(), b'x' * 100, Headers())
    cache.get('a', Headers())  # 'a' is now the most recently used
    cache.store('c', response(), b'x' * 100, Headers())
    assert 'a' in cache and 'c' in cache and 'b' not in cache
    assert cache.bytes_used <= cache.max_bytes


def test_entries_larger_than_the_limit_are_refused():
    cache = Cache(max_bytes=10_000, max_entry_bytes=500, default_ttl=60)
    assert not cache.store('big', response(), b'x' * 1000, Headers())
    assert len(cache) == 0 and cache.bytes_used == 0


def test_replacing_an_entry_keeps_the_byte_count_right():
    cache = Cache(max_bytes=10_000, max_entry_bytes=10_000, default_ttl=60)
    cache.store('k', response(), b'x' * 100, Headers())
    cache.store('k', response(), b'x' * 10, Headers())
    assert cache.bytes_used == len(response().encode_head()) + 10


def test_freshness_lifetime_precedence():
    assert freshness_lifetime(Headers([('Cache-Control', 's-maxage=5, max-age=50')]), 60) == 5
    assert freshness_lifetime(Headers([('Cache-Control', 'max-age=50')]), 60) == 50
    assert freshness_lifetime(Headers([('Cache-Control', 'no-cache, max-age=50')]), 60) == 0
    expires = Headers([('Date', 'Wed, 25 Nov 2026 12:00:00 GMT'), ('Expires', 'Wed, 25 Nov 2026 12:10:00 GMT')])
    assert freshness_lifetime(expires, 60) == 600
    assert freshness_lifetime(Headers([('Expires', 'not a date')]), 60) == 0
    assert freshness_lifetime(Headers(), 60) == 60


def test_storable_rules():
    plain = Headers()
    assert storable(plain, 200, Headers(), 60)
    assert not storable(plain, 500, Headers(), 60)
    assert not storable(plain, 200, Headers([('Vary', '*')]), 60)
    assert not storable(Headers([('Authorization', 'x')]), 200, Headers([('Cache-Control', 'max-age=60')]), 60)
    assert storable(Headers([('Authorization', 'x')]), 200, Headers([('Cache-Control', 'public, max-age=60')]), 60)
    # no-cache with a validator is stored, but must be revalidated on every use
    assert storable(plain, 200, Headers([('Cache-Control', 'no-cache'), ('ETag', '"e"')]), 60)
    assert not storable(plain, 200, Headers([('Cache-Control', 'no-cache')]), 60)
