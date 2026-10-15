#!/usr/bin/env python3
import argparse
import socket
import time


def command(*arguments: bytes) -> bytes:
    frame = bytearray(f"*{len(arguments)}\r\n", "ascii")
    for argument in arguments:
        frame.extend(f"${len(argument)}\r\n".encode())
        frame.extend(argument)
        frame.extend(b"\r\n")
    return bytes(frame)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--operations", type=int, default=200_000)
    parser.add_argument("--keys", type=int, default=1_024)
    parser.add_argument("--value-size", type=int, default=4_096)
    parser.add_argument("--pipeline", type=int, default=64)
    args = parser.parse_args()

    value = b"x" * args.value_size
    started = time.perf_counter()
    received = 0
    pending = bytearray()
    with socket.create_connection((args.host, args.port), timeout=5) as client:
        client.settimeout(5)
        for offset in range(0, args.operations, args.pipeline):
            batch_size = min(args.pipeline, args.operations - offset)
            batch = bytearray()
            for operation in range(offset, offset + batch_size):
                key = f"churn:{(operation // 2) % args.keys}".encode()
                if operation % 2 == 0:
                    batch.extend(command(b"SET", key, value))
                else:
                    batch.extend(command(b"DEL", key))
            client.sendall(batch)

            expected = offset + batch_size
            while received < expected:
                pending.extend(client.recv(1 << 20))
                while True:
                    end = pending.find(b"\r\n")
                    if end < 0:
                        break
                    del pending[: end + 2]
                    received += 1
    elapsed = time.perf_counter() - started

    print(f"operations={args.operations}")
    print(f"elapsed_seconds={elapsed:.6f}")
    print(f"operations_per_second={args.operations / elapsed:.2f}")


if __name__ == "__main__":
    main()