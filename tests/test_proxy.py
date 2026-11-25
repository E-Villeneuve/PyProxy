import concurrent.futures
import http.client
import json
import socket
import time

from conftest import Route


def test_forwards_a_request(proxy, origin):
    origin.routes['/page'] = Route(body=b'<h1>hi</h1>', headers=[('Content-Type', 'text/html')])
    r = proxy.get(origin.url('/page?x=1'))
    assert (r.status, r.body) == (200, b'<h1>hi</h1>')
    assert r.headers['X-Cache'] == 'MISS'
    assert r.headers['Content-Type'] == 'text/html'
    sent = origin.requests[0]
    assert sent['target'] == '/page?x=1'  # origin-form, not the absolute URL
    assert sent['headers']['host'] == f'127.0.0.1:{origin.port}'
    assert sent['headers']['via'] == '1.1 pyproxy'


def test_strips_hop_by_hop_headers(proxy, origin):
    origin.routes['/'] = Route()
    proxy.get(origin.url('/'), headers={'Proxy-Authorization': 'secret', 'X-Drop': '1', 'Connection': 'X-Drop'})
    sent = origin.requests[0]['headers']
    assert 'proxy-authorization' not in sent and 'x-drop' not in sent
    assert sent['connection'] == 'close'


def test_second_request_is_served_from_cache(proxy, origin):
    origin.routes['/a'] = Route(body=b'cached', headers=[('Cache-Control', 'max-age=60')])
    assert proxy.get(origin.url('/a')).headers['X-Cache'] == 'MISS'
    r = proxy.get(origin.url('/a'))
    assert (r.headers['X-Cache'], r.body) == ('HIT', b'cached')
    assert origin.hits['/a'] == 1


def test_default_ttl_expires(make_proxy, origin, clock):
    proxy = make_proxy(ttl=30)
    origin.routes['/a'] = Route()
    proxy.get(origin.url('/a'))
    clock.advance(29)
    assert proxy.get(origin.url('/a')).headers['X-Cache'] == 'HIT'
    clock.advance(2)
    assert proxy.get(origin.url('/a')).headers['X-Cache'] == 'MISS'
    assert origin.hits['/a'] == 2


def test_max_age_beats_default_ttl(make_proxy, origin, clock):
    proxy = make_proxy(ttl=1000)
    origin.routes['/a'] = Route(headers=[('Cache-Control', 'max-age=5')])
    proxy.get(origin.url('/a'))
    clock.advance(6)
    assert proxy.get(origin.url('/a')).headers['X-Cache'] == 'MISS'


def test_upstream_age_counts_against_max_age(proxy, origin, clock):
    origin.routes['/a'] = Route(headers=[('Cache-Control', 'max-age=100'), ('Age', '95')])
    proxy.get(origin.url('/a'))
    clock.advance(6)
    assert proxy.get(origin.url('/a')).headers['X-Cache'] == 'MISS'


def test_stale_entry_is_revalidated_with_etag(proxy, origin, clock):
    origin.routes['/a'] = Route(body=b'v1', headers=[('Cache-Control', 'max-age=10'), ('ETag', '"v1"')])
    proxy.get(origin.url('/a'))
    clock.advance(11)
    r = proxy.get(origin.url('/a'))
    assert (r.status, r.body, r.headers['X-Cache']) == (200, b'v1', 'REVALIDATED')
    assert origin.requests[-1]['headers']['if-none-match'] == '"v1"'
    # The 304 restarted the freshness clock.
    assert proxy.get(origin.url('/a')).headers['X-Cache'] == 'HIT'
    assert origin.hits['/a'] == 2


def test_changed_resource_replaces_cached_copy(proxy, origin, clock):
    origin.routes['/a'] = Route(body=b'v1', headers=[('Cache-Control', 'max-age=10'), ('ETag', '"v1"')])
    proxy.get(origin.url('/a'))
    origin.routes['/a'] = Route(body=b'v2', headers=[('Cache-Control', 'max-age=10'), ('ETag', '"v2"')])
    clock.advance(11)
    assert proxy.get(origin.url('/a')).body == b'v2'
    assert proxy.get(origin.url('/a')).body == b'v2'
    assert origin.hits['/a'] == 2


def test_client_no_cache_forces_revalidation(proxy, origin):
    origin.routes['/a'] = Route(headers=[('Cache-Control', 'max-age=60'), ('ETag', '"x"')])
    proxy.get(origin.url('/a'))
    r = proxy.get(origin.url('/a'), headers={'Cache-Control': 'no-cache'})
    assert r.headers['X-Cache'] == 'REVALIDATED'


def test_no_store_private_and_set_cookie_are_not_cached(proxy, origin):
    origin.routes['/no-store'] = Route(headers=[('Cache-Control', 'no-store')])
    origin.routes['/private'] = Route(headers=[('Cache-Control', 'private, max-age=60')])
    origin.routes['/cookie'] = Route(headers=[('Set-Cookie', 'session=abc')])
    for path in ('/no-store', '/private', '/cookie'):
        proxy.get(origin.url(path))
        assert proxy.get(origin.url(path)).headers['X-Cache'] == 'MISS'
        assert origin.hits[path] == 2


def test_error_statuses_are_not_cached(proxy, origin):
    origin.routes['/err'] = Route(status=500, body=b'boom')
    proxy.get(origin.url('/err'))
    proxy.get(origin.url('/err'))
    assert origin.hits['/err'] == 2


def test_authorized_requests_bypass_the_cache(proxy, origin):
    origin.routes['/me'] = Route(headers=[('Cache-Control', 'max-age=60')])
    proxy.get(origin.url('/me'), headers={'Authorization': 'Bearer t'})
    proxy.get(origin.url('/me'), headers={'Authorization': 'Bearer t'})
    assert origin.hits['/me'] == 2


def test_vary_keeps_variants_apart(proxy, origin):
    origin.routes['/v'] = lambda method, headers, body: Route(
        body=headers.get('accept-language', '').encode(),
        headers=[('Cache-Control', 'max-age=60'), ('Vary', 'Accept-Language')])
    assert proxy.get(origin.url('/v'), headers={'Accept-Language': 'en'}).body == b'en'
    assert proxy.get(origin.url('/v'), headers={'Accept-Language': 'fr'}).body == b'fr'


def test_cache_can_be_turned_off(make_proxy, origin):
    proxy = make_proxy(cache_bytes=0)
    origin.routes['/a'] = Route(headers=[('Cache-Control', 'max-age=60')])
    assert proxy.get(origin.url('/a')).headers['X-Cache'] == 'BYPASS'
    proxy.get(origin.url('/a'))
    assert origin.hits['/a'] == 2


def test_responses_too_big_to_cache_still_stream(make_proxy, origin):
    proxy = make_proxy(max_entry_bytes=1000)
    big = b'x' * 200_000
    origin.routes['/big'] = Route(body=big, headers=[('Cache-Control', 'max-age=60')])
    assert proxy.get(origin.url('/big')).body == big
    assert proxy.get(origin.url('/big')).body == big
    assert origin.hits['/big'] == 2


def test_chunked_response_is_relayed_and_cached(proxy, origin):
    origin.routes['/c'] = Route(body=b'chunked body here', framing='chunked', headers=[('Cache-Control', 'max-age=60')])
    assert proxy.get(origin.url('/c')).body == b'chunked body here'
    r = proxy.get(origin.url('/c'))
    assert (r.body, r.headers['X-Cache']) == (b'chunked body here', 'HIT')


def test_close_delimited_response_is_cached_with_a_length(proxy, origin):
    origin.routes['/e'] = Route(body=b'until close', framing='eof', headers=[('Cache-Control', 'max-age=60')])
    first = proxy.get(origin.url('/e'))
    assert (first.body, first.headers['Connection']) == (b'until close', 'close')
    conn = proxy.connection()
    try:
        cached = proxy.get(origin.url('/e'), conn=conn)
        assert cached.headers['Content-Length'] == str(len(b'until close'))
        assert proxy.get(origin.url('/e'), conn=conn).body == b'until close'  # same connection reused
    finally:
        conn.close()


def test_post_passes_through_and_is_not_cached(proxy, origin):
    origin.routes['/echo'] = lambda method, headers, body: Route(body=method.encode() + b':' + body)
    r = proxy.request('POST', origin.url('/echo'), body=b'payload')
    assert r.body == b'POST:payload'
    assert r.headers['X-Cache'] == 'BYPASS'
    proxy.request('POST', origin.url('/echo'), body=b'payload')
    assert origin.hits['/echo'] == 2


def test_head_request_has_no_body(proxy, origin):
    origin.routes['/h'] = Route(body=b'')
    r = proxy.request('HEAD', origin.url('/h'))
    assert r.status == 200 and r.body == b''


def test_keep_alive_serves_several_requests_on_one_connection(proxy, origin):
    origin.routes['/a'] = Route(body=b'A')
    origin.routes['/b'] = Route(body=b'B')
    conn = proxy.connection()
    try:
        assert [proxy.get(origin.url(p), conn=conn).body for p in ('/a', '/b', '/a')] == [b'A', b'B', b'A']
    finally:
        conn.close()


def test_slow_origin_does_not_block_other_clients(proxy, origin):
    origin.routes['/slow'] = Route(body=b'slow', delay=1.5)
    origin.routes['/fast'] = Route(body=b'fast')
    with concurrent.futures.ThreadPoolExecutor() as pool:
        slow = pool.submit(proxy.get, origin.url('/slow'))
        time.sleep(0.2)  # the slow request is now waiting on the origin
        started = time.perf_counter()
        assert proxy.get(origin.url('/fast')).body == b'fast'
        assert time.perf_counter() - started < 0.5
        assert slow.result().body == b'slow'


def test_many_concurrent_clients(proxy, origin):
    origin.routes['/w'] = Route(body=b'ok', delay=0.3, headers=[('Cache-Control', 'no-store')])
    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(50) as pool:
        bodies = list(pool.map(lambda _: proxy.get(origin.url('/w'), timeout=10).body, range(50)))
    assert bodies == [b'ok'] * 50
    assert time.perf_counter() - started < 3  # 50 x 0.3 s would be 15 s if handled one at a time


def test_unknown_host_is_502(proxy):
    r = proxy.get('http://no-such-host.invalid/')
    assert r.status == 502


def test_refused_connection_is_502(proxy):
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        port = s.getsockname()[1]  # nothing listens here once the socket closes
    assert proxy.get(f'http://127.0.0.1:{port}/').status == 502


def test_slow_origin_times_out_with_504(make_proxy, origin):
    proxy = make_proxy(read_timeout=0.3)
    origin.routes['/hang'] = Route(delay=2)
    assert proxy.get(origin.url('/hang')).status == 504


def test_origin_form_request_gets_a_helpful_400(proxy):
    r = proxy.get('/not-a-proxy-request')
    assert r.status == 400 and b'absolute URL' in r.body


def test_malformed_request_is_400(proxy):
    with socket.create_connection(('127.0.0.1', proxy.port), timeout=5) as s:
        s.sendall(b'NONSENSE\r\n\r\n')
        assert s.recv(1024).startswith(b'HTTP/1.1 400')


def test_oversized_request_head_is_431(proxy):
    with socket.create_connection(('127.0.0.1', proxy.port), timeout=5) as s:
        s.sendall(b'GET http://x/ HTTP/1.1\r\nX-Big: ' + b'a' * 70_000 + b'\r\n\r\n')
        assert s.recv(1024).startswith(b'HTTP/1.1 431')


def test_connect_tunnel_relays_bytes(proxy, origin):
    origin.routes['/t'] = Route(body=b'through the tunnel')
    conn = http.client.HTTPConnection('127.0.0.1', proxy.port, timeout=5)
    conn.set_tunnel('127.0.0.1', origin.port)  # CONNECT, then plain HTTP inside the tunnel
    try:
        conn.request('GET', '/t')
        assert conn.getresponse().read() == b'through the tunnel'
    finally:
        conn.close()
    assert proxy.stats.tunnels == 1


def test_connect_to_disallowed_port_is_403(make_proxy, origin):
    proxy = make_proxy(connect_ports=frozenset({443}))
    with socket.create_connection(('127.0.0.1', proxy.port), timeout=5) as s:
        s.sendall(f'CONNECT 127.0.0.1:{origin.port} HTTP/1.1\r\n\r\n'.encode())
        assert s.recv(1024).startswith(b'HTTP/1.1 403')


def test_stats_endpoint(proxy, origin):
    origin.routes['/a'] = Route(headers=[('Cache-Control', 'max-age=60')])
    proxy.get(origin.url('/a'))
    proxy.get(origin.url('/a'))
    stats = json.loads(proxy.get('/stats').body)
    assert (stats['hits'], stats['misses'], stats['hit_ratio']) == (1, 1, 0.5)
