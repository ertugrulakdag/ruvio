import os, time, mmap, statistics as s, sys
D = sys.argv[1]
N = 3000
def report(name, ts):
    ts.sort(); print(f"{name:42} median {s.median(ts)*1e3:6.3f} ms  p99 {ts[int(len(ts)*.99)]*1e3:6.3f} ms  rate {1/s.mean(ts):7.0f}/s", flush=True)
def prep(flags, name):
    p = f"{D}/{name}"; fd = os.open(p, os.O_RDWR | os.O_CREAT | os.O_TRUNC | flags)
    os.posix_fallocate(fd, 0, 64 << 20)
    # make blocks "written" (not unwritten extents) so the first writes do not convert extents
    os.pwrite(fd, b"\0" * (64 << 20), 0); os.fsync(fd)
    return fd
rec = b"x" * 120
# 1 baseline: pwrite + fdatasync
fd = prep(0, "a"); ts = []
for i in range(N):
    t = time.perf_counter(); os.pwrite(fd, rec, i * 120); os.fdatasync(fd); ts.append(time.perf_counter() - t)
report("pwrite + fdatasync (today)", ts)
# 2 O_DSYNC
fd = prep(os.O_DSYNC, "b"); ts = []
for i in range(N):
    t = time.perf_counter(); os.pwrite(fd, rec, i * 120); ts.append(time.perf_counter() - t)
report("pwrite on O_DSYNC fd", ts)
# 3 O_DIRECT|O_DSYNC, 4 KiB aligned block rewritten each time (log block with growing content)
try:
    fd = prep(os.O_DIRECT | os.O_DSYNC, "c"); buf = mmap.mmap(-1, 4096); buf.write(b"x" * 4096); ts = []
    for i in range(N):
        t = time.perf_counter(); os.pwritev(fd, [buf], (i % 1000) * 4096); ts.append(time.perf_counter() - t)
    report("O_DIRECT|O_DSYNC 4 KiB block", ts)
except OSError as e: print("O_DIRECT failed", e)
# 4 O_DIRECT only + fdatasync
try:
    fd = prep(os.O_DIRECT, "d"); buf = mmap.mmap(-1, 4096); buf.write(b"x" * 4096); ts = []
    for i in range(N):
        t = time.perf_counter(); os.pwritev(fd, [buf], (i % 1000) * 4096); os.fdatasync(fd); ts.append(time.perf_counter() - t)
    report("O_DIRECT 4 KiB + fdatasync", ts)
except OSError as e: print("O_DIRECT failed", e)
