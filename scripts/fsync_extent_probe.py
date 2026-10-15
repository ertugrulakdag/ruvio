import os, time, statistics as s, sys
D = sys.argv[1]; N = 2000; REC = 6000
def run(name, mode):
    p = f"{D}/{name}"; fd = os.open(p, os.O_RDWR | os.O_CREAT | os.O_TRUNC)
    size = 64 << 20
    if mode == "unwritten": os.posix_fallocate(fd, 0, size)
    elif mode == "zeroed":
        os.posix_fallocate(fd, 0, size); os.pwrite(fd, b"\0" * size, 0); os.fsync(fd)
    elif mode == "append": pass
    ts = []; off = 0; buf = b"x" * REC
    for i in range(N):
        t = time.perf_counter()
        if mode == "append": os.write(fd, buf)
        else: os.pwrite(fd, buf, off)
        os.fdatasync(fd); ts.append(time.perf_counter() - t); off += REC
    ts.sort(); print(f"{name:10} {mode:10} sequential {REC} B + fdatasync: median {s.median(ts)*1e3:6.3f} ms p99 {ts[int(N*.99)]*1e3:6.3f} ms rate {1/s.mean(ts):6.0f}/s", flush=True)
for mode in ("append", "unwritten", "zeroed"): run("e", mode)
