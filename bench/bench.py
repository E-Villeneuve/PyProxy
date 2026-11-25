"""Load test: an origin with simulated network latency, hit directly and through the proxy.

    python bench/bench.py                      # defaults below
    python bench/bench.py --latency 0.05 --concurrency 50 --duration 8

Runs the origin and the proxy as separate processes so the load generator does not
share their event loop, then prints a Markdown table.
"""
import argparse
import asyncio
import json
import platform
import socket
import statistics
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


# ---- origin process -------------------------------------------------------

async def origin_main(port, latency, size):
    body = b'x' * size

    async def handle(reader, writer):
        try:
            while True:
                head = await reader.readuntil(b'\r\n\r\n')
                await asyncio.sleep(latency)  # stands in for the round trip to a distant server
                close = b'connection: close' in head.lower()
                writer.write(b'HTTP/1.1 200 OK\r\nCache-Control: max-age=3600\r\nContent-Type: text/plain\r\n'
                             + f'Content-Length: {size}\r\n'.encode()
                             + (b'Connection: close\r\n' if close else b'') + b'\r\n' + body)
                await writer.drain()
                if close:
                    break
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, '127.0.0.1', port, backlog=1024)
    await server.serve_forever()


# ---- load generator -------------------------------------------------------

async def worker(port, targets, deadline, latencies, errors, offset):
    reader = writer = None
    i = offset
    while time.perf_counter() < deadline:
        target = targets[i % len(targets)]
        i += 1
        try:
            if writer is None:
                reader, writer = await asyncio.open_connection('127.0.0.1', port)
            started = time.perf_counter()
            writer.write(f'GET {target} HTTP/1.1\r\nHost: bench\r\n\r\n'.encode())
            head = (await reader.readuntil(b'\r\n\r\n')).lower()
            length = int(head.split(b'content-length:')[1].split(b'\r\n')[0])
            await reader.readexactly(length)
            latencies.append(time.perf_counter() - started)
            if b'connection: close' in head:
                writer.close()
                writer = None
        except Exception:
            errors.append(1)
            if writer is not None:
                writer.close()
            writer = None
    if writer is not None:
        writer.close()


async def load(port, targets, concurrency, duration):
    latencies, errors = [], []
    deadline = time.perf_counter() + duration
    started = time.perf_counter()
    await asyncio.gather(*(worker(port, targets, deadline, latencies, errors, n * 7) for n in range(concurrency)))
    elapsed = time.perf_counter() - started
    latencies.sort()
    pct = lambda p: latencies[min(len(latencies) - 1, int(len(latencies) * p))] * 1000
    return {
        'requests': len(latencies),
        'rps': len(latencies) / elapsed,
        'p50': pct(0.50),
        'p99': pct(0.99),
        'errors': len(errors),
    }


# ---- orchestration --------------------------------------------------------

def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def wait_for_port(port, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            socket.create_connection(('127.0.0.1', port), timeout=0.5).close()
            return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError(f'nothing listening on {port}')


def start(args):
    proc = subprocess.Popen([sys.executable, *args], cwd=ROOT,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return proc


def fetch_stats(port):
    with socket.create_connection(('127.0.0.1', port)) as s:
        s.sendall(b'GET /stats HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n')
        data = b''
        while chunk := s.recv(65536):
            data += chunk
    return json.loads(data.split(b'\r\n\r\n', 1)[1])


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--latency', type=float, default=0.05, help='origin delay per request, seconds')
    p.add_argument('--size', type=int, default=16 * 1024, help='response body size, bytes')
    p.add_argument('--concurrency', type=int, default=50)
    p.add_argument('--duration', type=float, default=8)
    p.add_argument('--urls', type=int, default=200, help='distinct URLs requested')
    args = p.parse_args()

    origin_port = free_port()
    procs = [start([__file__, 'origin', str(origin_port), str(args.latency), str(args.size)])]
    try:
        wait_for_port(origin_port)
        paths = [f'/item/{n}' for n in range(args.urls)]
        absolute = [f'http://127.0.0.1:{origin_port}{path}' for path in paths]
        rows = []

        rows.append(('Direct to origin', asyncio.run(load(origin_port, paths, args.concurrency, args.duration)), None))

        for label, extra in (('Proxy, cache off', ['--cache-size', '0']), ('Proxy, cache on (warm)', [])):
            port = free_port()
            procs.append(start(['-m', 'pyproxy', '--port', str(port), '-q', *extra]))
            wait_for_port(port)
            if not extra:
                asyncio.run(load(port, absolute, 10, 1.5))  # warm the cache
            rows.append((label, asyncio.run(load(port, absolute, args.concurrency, args.duration)), fetch_stats(port)))
            time.sleep(2)  # let closed sockets drain between runs

        print(f'\nOrigin latency {args.latency * 1000:.0f} ms, {args.size // 1024} KB responses, '
              f'{args.urls} URLs, {args.concurrency} concurrent clients, {args.duration:g} s per run')
        print(f'{platform.system()} {platform.release()}, {platform.processor() or platform.machine()}, '
              f'Python {platform.python_version()}\n')
        print('| Scenario | Requests/s | p50 (ms) | p99 (ms) | Errors | Cache hit ratio |')
        print('|---|---:|---:|---:|---:|---:|')
        for label, r, stats in rows:
            ratio = f"{stats['hit_ratio']:.0%}" if stats and stats['hits'] + stats['misses'] else '-'
            print(f"| {label} | {r['rps']:,.0f} | {r['p50']:.1f} | {r['p99']:.1f} | {r['errors']} | {ratio} |")
    finally:
        for proc in procs:
            proc.terminate()
            proc.wait(5)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'origin':
        asyncio.run(origin_main(int(sys.argv[2]), float(sys.argv[3]), int(sys.argv[4])))
    else:
        main()
