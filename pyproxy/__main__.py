"""Command line: python -m pyproxy --help"""
import argparse
import asyncio
import logging

from .server import Config, ProxyServer

MB = 2**20


def parse_args(argv=None):
    p = argparse.ArgumentParser(prog='pyproxy', description='An HTTP/HTTPS forward proxy with a response cache.')
    p.add_argument('--host', default='127.0.0.1', help='address to listen on (default: %(default)s)')
    p.add_argument('--port', type=int, default=8888, help='port to listen on (default: %(default)s)')
    p.add_argument('--ttl', type=float, default=60,
                   help='seconds a response stays fresh when the origin sends no caching headers (default: %(default)s)')
    p.add_argument('--cache-size', type=float, default=64, help='cache size in MB; 0 turns caching off (default: %(default)s)')
    p.add_argument('--max-entry', type=float, default=8, help='largest response to cache, in MB (default: %(default)s)')
    p.add_argument('--connect-timeout', type=float, default=10, help='seconds to connect to an origin (default: %(default)s)')
    p.add_argument('--read-timeout', type=float, default=30, help='seconds to wait on an origin (default: %(default)s)')
    p.add_argument('--allow-connect-port', type=int, action='append', metavar='PORT',
                   help='port HTTPS tunnels may reach; repeat for more (default: 443)')
    p.add_argument('--quiet', '-q', action='store_true', help='log errors only')
    return p.parse_args(argv)


def config_from(args):
    return Config(
        host=args.host,
        port=args.port,
        ttl=args.ttl,
        cache_bytes=int(args.cache_size * MB),
        max_entry_bytes=int(args.max_entry * MB),
        connect_timeout=args.connect_timeout,
        read_timeout=args.read_timeout,
        connect_ports=frozenset(args.allow_connect_port or [443]),
    )


async def run(config):
    proxy = await ProxyServer(config).start()
    cache = f'{config.cache_bytes / MB:g} MB cache' if proxy.cache is not None else 'cache off'
    logging.getLogger('pyproxy').warning('listening on %s:%d (%s)', config.host, proxy.port, cache)
    try:
        await proxy.serve_forever()
    finally:
        await proxy.close()


def main(argv=None):
    args = parse_args(argv)
    logging.basicConfig(level=logging.WARNING if args.quiet else logging.INFO,
                        format='%(asctime)s %(message)s', datefmt='%H:%M:%S')
    try:
        asyncio.run(run(config_from(args)))
    except KeyboardInterrupt:
        print('\nClosing server...')


if __name__ == '__main__':
    main()
