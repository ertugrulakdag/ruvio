import os, time, threading, sys
D = sys.argv[1]
def run(nthreads, same_file, secs=3):
    paths = [f"{D}/p0"] * nthreads if same_file else [f"{D}/p{i}" for i in range(nthreads)]
    fds = {}
    for p in set(paths):
        fd = os.open(p, os.O_RDWR | os.O_CREAT | os.O_TRUNC); os.posix_fallocate(fd, 0, 64 << 20); os.pwrite(fd, b"\0" * (64 << 20), 0); os.fsync(fd); fds[p] = fd
    counts = [0] * nthreads; end = time.time() + secs
    def w(i):
        fd = fds[paths[i]]; n = 0
        while time.time() < end:
            os.pwrite(fd, b"x" * 120, (i * 1000000 + n % 100000) * 120 % (60 << 20)); os.fdatasync(fd); n += 1
        counts[i] = n
    ts = [threading.Thread(target=w, args=(i,)) for i in range(nthreads)]
    [t.start() for t in ts]; [t.join() for t in ts]
    return sum(counts) / secs
for same in (True, False):
    print("same file" if same else "separate files", " ".join(f"T={t}: {run(t, same):6.0f}/s" for t in (1, 2, 4, 8, 16)), flush=True)
