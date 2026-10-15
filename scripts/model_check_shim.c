// LD_PRELOAD fault shim for scripts/model_check.py.
//
// While the file named by RUVIO_FAULT_CONTROL exists, calls on descriptors whose path contains
// one of the configured substrings fail with the configured errno. The control file holds one
// line:
//
//     <class> <errno> <substring>[,<substring>...] [partial]
//
// <class> is `sync` (fsync, fdatasync, sync_file_range), `write` (write, pwrite, writev,
// pwritev, fallocate) or `all`. <errno> is `EIO` or `ENOSPC`. `partial` lets a write first
// complete half of its buffer and fail the call that follows, so a record is torn on disk.
// Every injected failure is appended to RUVIO_FAULT_LOG as `<unix ms> <call> <errno> <path> <size>`,
// where <size> is the file's length at the time of the call.
//
// Two more classes inject no error (their <errno> field is ignored):
//
// - `track` appends every write to a matching file as `W <path> <start> <end>` and every
//   successful fsync/fdatasync of one as `S <path>` to RUVIO_TRACK_LOG. The checker uses it to
//   simulate a power loss: after kill -9 it zeroes the ranges written since a file's last sync.
// - `tear` makes the next write to a matching file write a random prefix of its buffer and then
//   SIGKILL the process, so a crash lands inside a batch, between or inside its records.
//
// Build: cc -O2 -shared -fPIC -o model_check_shim.so model_check_shim.c -ldl -lpthread
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

enum { CLASS_NONE = 0, CLASS_SYNC = 1, CLASS_WRITE = 2, CLASS_TRACK = 4, CLASS_TEAR = 8 };

static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static long long last_check_ns = -1;
static int active_classes = CLASS_NONE;
static int active_errno = 0;
static int active_partial = 0;
static int tear_skip = 0;   // matching writes a tear lets through before it cuts one
static int tear_passed = 0;
static char active_paths[512];

static long long now_ns(void) {
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
    return (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

// Reads the control file at most once per millisecond.
static void refresh(void) {
    long long now = now_ns();

    pthread_mutex_lock(&lock);
    if (last_check_ns >= 0 && now - last_check_ns < 1000000LL) {
        pthread_mutex_unlock(&lock);
        return;
    }
    last_check_ns = now;
    int was_tearing = active_classes & CLASS_TEAR;
    active_classes = CLASS_NONE;

    const char *control = getenv("RUVIO_FAULT_CONTROL");

    if (control != NULL) {
        int fd = (int)syscall(SYS_openat, AT_FDCWD, control, O_RDONLY | O_CLOEXEC);

        if (fd >= 0) {
            char buffer[640];
            long n = syscall(SYS_read, fd, buffer, sizeof(buffer) - 1);

            syscall(SYS_close, fd);
            if (n > 0) {
                char class_name[16] = {0}, errno_name[16] = {0}, partial[16] = {0};

                buffer[n] = 0;
                active_paths[0] = 0;
                if (sscanf(buffer, "%15s %15s %511s %15s", class_name, errno_name, active_paths,
                           partial) >= 3) {
                    if (strcmp(class_name, "sync") == 0) {
                        active_classes = CLASS_SYNC;
                    } else if (strcmp(class_name, "write") == 0) {
                        active_classes = CLASS_WRITE;
                    } else if (strcmp(class_name, "all") == 0) {
                        active_classes = CLASS_SYNC | CLASS_WRITE;
                    } else if (strcmp(class_name, "track") == 0) {
                        active_classes = CLASS_TRACK;
                    } else if (strcmp(class_name, "tear") == 0) {
                        active_classes = CLASS_TEAR;
                        // "tear <n> <paths>": the cut lands on the (n+1)th matching write.
                        tear_skip = errno_name[0] >= '0' && errno_name[0] <= '9' ? atoi(errno_name) : 0;
                        if (!was_tearing) {
                            tear_passed = 0;
                        }
                    }
                    active_errno = strcmp(errno_name, "ENOSPC") == 0 ? ENOSPC : EIO;
                    active_partial = strcmp(partial, "partial") == 0;
                }
            }
        }
    }
    pthread_mutex_unlock(&lock);
}

static void log_fault(int target, const char *call, int error, const char *path) {
    const char *log = getenv("RUVIO_FAULT_LOG");

    if (log == NULL) {
        return;
    }

    int fd = (int)syscall(SYS_openat, AT_FDCWD, log, O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0644);

    if (fd < 0) {
        return;
    }

    struct timespec ts;
    struct stat st;
    char line[800];
    long long size = fstat(target, &st) == 0 ? (long long)st.st_size : -1;

    clock_gettime(CLOCK_REALTIME, &ts);
    int n = snprintf(line, sizeof(line), "%lld %s %s %s %lld\n", (long long)ts.tv_sec * 1000LL + ts.tv_nsec / 1000000,
                     call, error == ENOSPC ? "ENOSPC" : error == 0 ? "TEAR" : "EIO", path, size);

    if (n > 0) {
        syscall(SYS_write, fd, line, (size_t)(n < (int)sizeof(line) ? n : (int)sizeof(line) - 1));
    }
    syscall(SYS_close, fd);
}

// True when `class` is active and `fd`'s path matches; the path is left in `path`.
static int matches(int fd, int class, char *path, size_t size) {
    refresh();
    if ((active_classes & class) == 0 || fd < 0) {
        return 0;
    }

    char link[64];

    snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    long n = syscall(SYS_readlinkat, AT_FDCWD, link, path, size - 1);

    if (n <= 0) {
        return 0;
    }
    path[n] = 0;

    char patterns[512];

    pthread_mutex_lock(&lock);
    strncpy(patterns, active_paths, sizeof(patterns) - 1);
    patterns[sizeof(patterns) - 1] = 0;
    pthread_mutex_unlock(&lock);

    for (char *save = NULL, *token = strtok_r(patterns, ",", &save); token != NULL;
         token = strtok_r(NULL, ",", &save)) {
        if (strstr(path, token) != NULL) {
            return 1;
        }
    }
    return 0;
}

// Returns the errno to inject for `fd` and `class`, or 0.
static int should_fail(int fd, int class, const char *call) {
    char path[512];

    if (!matches(fd, class, path, sizeof(path))) {
        return 0;
    }

    int error;

    pthread_mutex_lock(&lock);
    error = active_errno;
    pthread_mutex_unlock(&lock);
    log_fault(fd, call, error, path);
    return error;
}

static void track_line(const char *line, int length) {
    const char *log = getenv("RUVIO_TRACK_LOG");

    if (log == NULL || length <= 0) {
        return;
    }

    int fd = (int)syscall(SYS_openat, AT_FDCWD, log, O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0644);

    if (fd >= 0) {
        syscall(SYS_write, fd, line, (size_t)length);
        syscall(SYS_close, fd);
    }
}

// `track`: a write of `written` bytes ending at `end` (-1: at the descriptor's offset now).
static void track_write(int fd, ssize_t written, long long end) {
    char path[512], line[700];

    if (written <= 0 || !matches(fd, CLASS_TRACK, path, sizeof(path))) {
        return;
    }
    if (end < 0) {
        end = (long long)lseek(fd, 0, SEEK_CUR);
        if (end < 0) {
            return;
        }
    }
    track_line(line, snprintf(line, sizeof(line), "W %s %lld %lld\n", path, end - written, end));
}

static void track_sync(int fd, int result) {
    char path[512], line[600];

    if (result == 0 && matches(fd, CLASS_TRACK, path, sizeof(path))) {
        track_line(line, snprintf(line, sizeof(line), "S %s\n", path));
    }
}

// `tear`: how many of `count` bytes to write before the process dies, or 0 for no tear.
static size_t tear_at(int fd, size_t count, const char *call) {
    char path[512];

    if (count < 2 || !matches(fd, CLASS_TEAR, path, sizeof(path))) {
        return 0;
    }
    pthread_mutex_lock(&lock);
    int pass = tear_passed < tear_skip;
    tear_passed += pass;
    pthread_mutex_unlock(&lock);
    if (pass) {
        return 0;
    }
    log_fault(fd, call, 0, path);
    return 1 + (size_t)(now_ns() / 1000 % (long long)(count - 1));
}

static void die(void) {
    syscall(SYS_kill, getpid(), SIGKILL);
}

#define REAL(name)                                                                                 \
    static __typeof__(&name) real_##name = NULL;                                                   \
    if (real_##name == NULL) real_##name = (__typeof__(&name))dlsym(RTLD_NEXT, #name)

int fsync(int fd) {
    REAL(fsync);
    int error = should_fail(fd, CLASS_SYNC, "fsync");

    if (error) {
        errno = error;
        return -1;
    }
    int result = real_fsync(fd);

    track_sync(fd, result);
    return result;
}

int fdatasync(int fd) {
    REAL(fdatasync);
    int error = should_fail(fd, CLASS_SYNC, "fdatasync");

    if (error) {
        errno = error;
        return -1;
    }
    int result = real_fdatasync(fd);

    track_sync(fd, result);
    return result;
}

int sync_file_range(int fd, off64_t offset, off64_t nbytes, unsigned int flags) {
    REAL(sync_file_range);
    int error = should_fail(fd, CLASS_SYNC, "sync_file_range");

    if (error) {
        errno = error;
        return -1;
    }
    return real_sync_file_range(fd, offset, nbytes, flags);
}

// A partial fault writes half of the buffer once per descriptor run, then fails.
static __thread int partial_done_fd = -1;

ssize_t write(int fd, const void *buffer, size_t count) {
    REAL(write);
    int error = should_fail(fd, CLASS_WRITE, "write");

    if (error) {
        if (active_partial && partial_done_fd != fd && count > 1) {
            partial_done_fd = fd;
            return real_write(fd, buffer, count / 2);
        }
        errno = error;
        return -1;
    }
    partial_done_fd = -1;

    size_t torn = tear_at(fd, count, "write");

    if (torn) {
        (void)!real_write(fd, buffer, torn);
        die();
    }

    ssize_t written = real_write(fd, buffer, count);

    track_write(fd, written, -1);
    return written;
}

static ssize_t pwrite_common(const char *call, int fd, const void *buffer, size_t count, off64_t offset,
                             ssize_t (*real)(int, const void *, size_t, off64_t)) {
    int error = should_fail(fd, CLASS_WRITE, call);

    if (error) {
        if (active_partial && partial_done_fd != fd && count > 1) {
            partial_done_fd = fd;
            return real(fd, buffer, count / 2, offset);
        }
        errno = error;
        return -1;
    }
    partial_done_fd = -1;

    size_t torn = tear_at(fd, count, call);

    if (torn) {
        (void)!real(fd, buffer, torn, offset);
        die();
    }

    ssize_t written = real(fd, buffer, count, offset);

    track_write(fd, written, written > 0 ? (long long)offset + written : 0);
    return written;
}

ssize_t pwrite(int fd, const void *buffer, size_t count, off_t offset) {
    REAL(pwrite64);
    return pwrite_common("pwrite", fd, buffer, count, offset, real_pwrite64);
}

ssize_t pwrite64(int fd, const void *buffer, size_t count, off64_t offset) {
    REAL(pwrite64);
    return pwrite_common("pwrite64", fd, buffer, count, offset, real_pwrite64);
}

// `tear` for a vectored write: fills `cut` with the prefix to write and returns its length in
// pieces, or 0 for no tear.
static int torn_iov(int fd, const struct iovec *iov, int iovcnt, struct iovec *cut, const char *call) {
    size_t total = 0;

    if (iovcnt <= 0 || iovcnt > 64) {
        return 0;
    }
    for (int i = 0; i < iovcnt; i++) {
        total += iov[i].iov_len;
    }

    size_t keep = tear_at(fd, total, call);
    int pieces = 0;

    while (keep > 0 && pieces < iovcnt) {
        cut[pieces] = iov[pieces];
        if (cut[pieces].iov_len > keep) {
            cut[pieces].iov_len = keep;
        }
        keep -= cut[pieces].iov_len;
        pieces++;
    }
    return pieces;
}

ssize_t writev(int fd, const struct iovec *iov, int iovcnt) {
    REAL(writev);
    int error = should_fail(fd, CLASS_WRITE, "writev");

    if (error) {
        errno = error;
        return -1;
    }

    struct iovec cut[64];
    int pieces = torn_iov(fd, iov, iovcnt, cut, "writev");

    if (pieces) {
        (void)!real_writev(fd, cut, pieces);
        die();
    }

    ssize_t written = real_writev(fd, iov, iovcnt);

    track_write(fd, written, -1);
    return written;
}

ssize_t pwritev(int fd, const struct iovec *iov, int iovcnt, off_t offset) {
    REAL(pwritev);
    int error = should_fail(fd, CLASS_WRITE, "pwritev");

    if (error) {
        errno = error;
        return -1;
    }

    struct iovec cut[64];
    int pieces = torn_iov(fd, iov, iovcnt, cut, "pwritev");

    if (pieces) {
        (void)!real_pwritev(fd, cut, pieces, offset);
        die();
    }

    ssize_t written = real_pwritev(fd, iov, iovcnt, offset);

    track_write(fd, written, written > 0 ? (long long)offset + written : 0);
    return written;
}

int fallocate(int fd, int mode, off_t offset, off_t length) {
    REAL(fallocate);
    int error = should_fail(fd, CLASS_WRITE, "fallocate");

    if (error) {
        errno = error;
        return -1;
    }
    return real_fallocate(fd, mode, offset, length);
}

int fallocate64(int fd, int mode, off64_t offset, off64_t length) {
    REAL(fallocate64);
    int error = should_fail(fd, CLASS_WRITE, "fallocate64");

    if (error) {
        errno = error;
        return -1;
    }
    return real_fallocate64(fd, mode, offset, length);
}
