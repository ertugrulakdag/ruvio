"""Sustained, rate-limited RESP expiry load; local, single shard, memory durability.

Usage: python3 expiry_sustained.py /absolute/path/to/ruvio [--future 20000]
"""
import argparse
import json
import socket
import subprocess
import threading
import time


def resp(*args):
    return b'*' + str(len(args)).encode() + b'\r\n' + b''.join(
        b'$' + str(len(arg)).encode() + b'\r\n' + arg + b'\r\n' for arg in args
    )


def connect(port):
    s = socket.create_connection(('127.0.0.1', port), timeout=5)
    s.settimeout(10)
    return s, s.makefile('rb')


def rss(pid):
    return int(subprocess.check_output(['ps', '-o', 'rss=', '-p', str(pid)]).strip()) * 1024


def metrics(s, reader, pid):
    s.sendall(resp(b'INFO', b'prometheus'))
    header = reader.readline()
    if not header.startswith(b'$'):
        raise RuntimeError(f'INFO: {header!r}')
    text = reader.read(int(header[1:])).decode()
    assert reader.read(2) == b'\r\n'
    def value(name, label=''):
        return sum(int(line.rsplit(' ', 1)[1]) for line in text.splitlines()
                   if line.startswith(name + '{') and label in line.split('}')[0])
    return {'keys': value('ruvio_keys'), 'active': value('ruvio_expired_keys_total', 'reason="active"'),
            'passive': value('ruvio_expired_keys_total', 'reason="passive"'), 'rss': rss(pid)}


def pct(values, p):
    return round(sorted(values)[min(len(values)-1, int(len(values)*p))], 1) if values else None


def run(binary, future, phases, workers, ttl_ms, refresh=False):
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    server = subprocess.Popen([binary, '--address', f'127.0.0.1:{port}', '--durability', 'memory',
                               '--shards', '1', '--active-expire-keys', '16'],
                              stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    try:
        for _ in range(100):
            try:
                control, reader = connect(port)
                break
            except OSError:
                if server.poll() is not None:
                    raise RuntimeError(f'Server failed: {server.stderr.read().decode()[-1000:]}')
                time.sleep(.05)
        else:
            raise RuntimeError('Server startup timed out')
        with control, reader:
            baseline_start = time.monotonic()
            for start in range(0, future, 500):
                end = min(future, start + 500)
                control.sendall(b''.join(resp(b'SET', f'f:{i:09d}'.encode(), b'x' * 64,
                                             b'PX', b'600000') for i in range(start, end)))
                for _ in range(start, end):
                    assert reader.readline() == b'+OK\r\n'
            print(json.dumps({'stage': 'loaded', 'future': future, 'load_s': round(time.monotonic()-baseline_start, 2),
                              **metrics(control, reader, server.pid)}), flush=True)
            sequence = 0
            for rate, seconds in phases:
                begin = time.monotonic()
                stop = begin + seconds
                lock = threading.Lock()
                completed = [0] * workers
                errors = []
                latencies = []
                # 20 ms chunks keep load fairly uniform without one syscall per request.
                def writer(worker):
                    nonlocal sequence
                    try:
                        s, r = connect(port)
                        with s, r:
                            slot = 0
                            while True:
                                target = begin + slot * .020
                                if target >= stop:
                                    break
                                if time.monotonic() < target:
                                    time.sleep(target - time.monotonic())
                                with lock:
                                    size = rate // (50 * workers)
                                    extra = ((slot % 50) * workers + worker) < (rate % (50 * workers))
                                    size += int(extra)
                                    first = sequence
                                    sequence += size
                                if size:
                                    if refresh:
                                        payload = b''.join(resp(b'PEXPIRE', f'f:{i % future:09d}'.encode(),
                                                                b'1800000' if (i // future) % 2 else b'3600000')
                                                           for i in range(first, first + size))
                                        expected = b':1\r\n'
                                    else:
                                        payload = b''.join(resp(b'SET', f'd:{i:012d}'.encode(), b'x' * 64,
                                                                *([b'PX', str(ttl_ms).encode()] if ttl_ms else []))
                                                           for i in range(first, first + size))
                                        expected = b'+OK\r\n'
                                    s.sendall(payload)
                                    for _ in range(size):
                                        if r.readline() != expected:
                                            raise RuntimeError('write/PEXPIRE failed')
                                    completed[worker] += size
                                slot += 1
                    except Exception as exc:
                        errors.append(repr(exc))
                threads = [threading.Thread(target=writer, args=(i,)) for i in range(workers)]
                for t in threads: t.start()
                samples = []
                while time.monotonic() < stop:
                    t0 = time.perf_counter_ns()
                    control.sendall(resp(b'PING'))
                    if reader.readline() != b'+PONG\r\n':
                        raise RuntimeError('PING failed')
                    latencies.append((time.perf_counter_ns() - t0) / 1000)
                    if time.monotonic() - begin >= (len(samples) + 1):
                        samples.append({'elapsed_s': round(time.monotonic() - begin, 2),
                                        'writes': sum(completed), **metrics(control, reader, server.pid)})
                    time.sleep(.009)
                for t in threads: t.join(timeout=12)
                if any(t.is_alive() for t in threads) or errors:
                    raise RuntimeError(f'Writer stalled/failed: {errors}')
                after = metrics(control, reader, server.pid)
                print(json.dumps({'stage': 'load', 'rate_target': rate, 'seconds': seconds,
                                  'writes': sum(completed), 'rate_actual': round(sum(completed)/(time.monotonic()-begin)),
                                  'ping_count': len(latencies), 'ping_p50_us': pct(latencies, .5),
                                  'ping_p99_us': pct(latencies, .99), 'ping_max_us': pct(latencies, 1),
                                  'samples': samples, **after}), flush=True)
            if ttl_ms == 0 or refresh:
                return
            start = time.monotonic()
            while time.monotonic() - start < 15:
                time.sleep(1)
                current = metrics(control, reader, server.pid)
                print(json.dumps({'stage': 'drain', 'elapsed_s': round(time.monotonic() - start, 1), **current}), flush=True)
                if current['keys'] <= future:
                    break
    finally:
        server.terminate()
        try: server.wait(timeout=3)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('binary')
    p.add_argument('--future', type=int, default=20000)
    p.add_argument('--workers', type=int, default=4)
    p.add_argument('--ttl-ms', type=int, default=1200)
    p.add_argument('--phases', default='2000:8,10000:8,20000:8')
    p.add_argument('--refresh', action='store_true')
    a = p.parse_args()
    if a.workers < 1 or a.future < (1 if a.refresh else 0) or a.ttl_ms < 0:
        p.error('workers must be positive; refresh needs future keys; ttl-ms cannot be negative')
    run(a.binary, a.future, [(int(r), int(d)) for r, d in (part.split(':') for part in a.phases.split(','))],
        a.workers, a.ttl_ms, a.refresh)
