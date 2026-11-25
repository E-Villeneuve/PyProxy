# PyProxy

A caching HTTP proxy written in Python using asyncio. It also supports HTTPS through CONNECT tunnels.

I built this to learn more about networking, HTTP and async programming in Python. It doesn't use any third-party libraries.

## Features

- Handles many clients at once on a single asyncio event loop
- Forwards HTTP requests and tunnels HTTPS traffic
- Caches responses in memory, following the HTTP caching headers (Cache-Control, Expires, ETag, Vary)
- Revalidates stale cache entries with the server instead of downloading them again
- Least-recently-used (LRU) eviction with a configurable cache size
- Returns proper error responses (502, 504, etc.) instead of crashing
- `/stats` endpoint showing cache hits and misses

## Requirements

Python 3.11+

## Usage

```bash
python -m pyproxy
```

By default it listens on `127.0.0.1:8888`. Some options:

```bash
python -m pyproxy --port 9000 --ttl 120 --cache-size 256
python -m pyproxy --help
```

`--ttl` is how long (in seconds) to cache responses that don't have caching headers, and `--cache-size` is in MB.

To try it with curl:

```bash
curl -x http://127.0.0.1:8888 http://example.com/
curl -x http://127.0.0.1:8888 https://example.com/
curl http://127.0.0.1:8888/stats
```

Every response has an `X-Cache` header that says whether it came from the cache (`HIT`), from the server (`MISS`), or was checked with the server first (`REVALIDATED`).

## Running the tests

```bash
pip install -r requirements-dev.txt
pytest
```

The tests start a fake server and send requests through the proxy with Python's `http.client`. They cover caching, HTTPS tunnels, error handling, and making sure one slow server doesn't block other requests.

## Benchmark

```bash
python bench/bench.py
```

This starts a test server that waits 50 ms before each response (to act like a real server over the internet), then sends requests from 50 clients at once for 8 seconds. Results on my machine (Windows 11, Python 3.13):

| Setup | Requests/sec | Median latency | 99th percentile |
|---|---:|---:|---:|
| No proxy | 796 | 62.8 ms | 65.0 ms |
| Proxy, cache off | 794 | 62.7 ms | 73.7 ms |
| Proxy, cache on | 16,583 | 3.0 ms | 3.9 ms |

The proxy itself adds very little overhead. With the cache on, every request in this test is already cached, so this is a best case.

## How it works

When a request comes in, the proxy checks its cache:

1. If there's a fresh copy, it sends that back right away.
2. If there's an expired copy with an ETag or Last-Modified date, it asks the server if the page changed. If the server says no (304), it sends back the cached copy.
3. Otherwise it gets the page from the server and streams it to the client, saving a copy if the response is allowed to be cached.

HTTPS requests use CONNECT. The proxy opens a connection to the server and passes the encrypted data back and forth, so HTTPS responses aren't cached.

Code layout:

- `pyproxy/server.py`: handles connections, forwarding, tunnels and errors
- `pyproxy/cache.py`: cache rules and the LRU cache
- `pyproxy/http.py`: parsing HTTP requests and responses
- `tests/`: tests
- `bench/`: benchmark script

#