#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "forge/platform.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if !defined(FORGE_OS_WINDOWS)
#include <errno.h>
#include <fcntl.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <time.h>
#include <unistd.h>
#if defined(FORGE_OS_LINUX)
#include <sched.h>
#endif
#endif

static int g_net_inited = 0;

void fr_platform_init(void) {
#if defined(FORGE_OS_WINDOWS)
    if (!g_net_inited) {
        WSADATA wsa;
        if (WSAStartup(MAKEWORD(2, 2), &wsa) == 0) g_net_inited = 1;
    }
#else
    (void)g_net_inited;
#endif
}

void fr_platform_shutdown(void) {
#if defined(FORGE_OS_WINDOWS)
    if (g_net_inited) {
        WSACleanup();
        g_net_inited = 0;
    }
#endif
}

void fr_platform_tune_for_server(void) {
#if !defined(FORGE_OS_WINDOWS)
    struct rlimit rl;
    if (getrlimit(RLIMIT_NOFILE, &rl) == 0) {
        rlim_t target = 65536;
        const char *configured = getenv("FORGE_MAX_FDS");
        if (configured && configured[0]) {
            char *end = NULL;
            unsigned long long value = strtoull(configured, &end, 10);
            if (end && *end == '\0' && value >= 1024 && value <= 1048576) {
                target = (rlim_t)value;
            }
        }
        if (rl.rlim_max != RLIM_INFINITY && target > rl.rlim_max) target = rl.rlim_max;
        if (target > rl.rlim_cur) {
            rl.rlim_cur = target;
            (void)setrlimit(RLIMIT_NOFILE, &rl);
        }
    }
    if (getrlimit(RLIMIT_MEMLOCK, &rl) == 0) {
        rlim_t target = rl.rlim_max;
        if (target == RLIM_INFINITY || target > (64 * 1024 * 1024)) target = 64 * 1024 * 1024;
        if (target > rl.rlim_cur) {
            rl.rlim_cur = target;
            (void)setrlimit(RLIMIT_MEMLOCK, &rl);
        }
    }
#endif
}

int fr_sock_would_block(int err) {
#if defined(FORGE_OS_WINDOWS)
    return err == WSAEWOULDBLOCK;
#else
    return err == EAGAIN || err == EWOULDBLOCK;
#endif
}

void fr_platform_sleep_us(int microseconds) {
    if (microseconds <= 0) return;
#if defined(FORGE_OS_WINDOWS)
    Sleep((DWORD)((microseconds + 999) / 1000));
#else
    struct timespec ts;
    ts.tv_sec = (time_t)(microseconds / 1000000);
    ts.tv_nsec = (long)(microseconds % 1000000) * 1000L;
    nanosleep(&ts, NULL);
#endif
}

int fr_platform_available_cpu_count(void) {
#if defined(FORGE_OS_LINUX)
    cpu_set_t local;
    if (sched_getaffinity(0, sizeof(local), &local) == 0) {
        int count = CPU_COUNT(&local);
        if (count > 0) return count;
    } else if (errno == EINVAL) {
        /* Large kernel masks can exceed cpu_set_t even when few CPUs are
         * allowed. Bound probing; other failures retain the online fallback. */
        for (size_t bytes = sizeof(local) * 2; bytes <= 1024 * 1024; bytes *= 2) {
            cpu_set_t *mask = (cpu_set_t *)malloc(bytes);
            if (!mask) break;
            int result = sched_getaffinity(0, bytes, mask);
            int error = errno;
            int count = result == 0 ? CPU_COUNT_S(bytes, mask) : 0;
            free(mask);
            if (count > 0) return count;
            if (result == 0 || error != EINVAL) break;
        }
    }
#endif
    return fr_platform_cpu_count();
}

int fr_platform_cpu_count(void) {
#if defined(FORGE_OS_WINDOWS)
    SYSTEM_INFO info;
    GetSystemInfo(&info);
    int n = (int)info.dwNumberOfProcessors;
    return n > 0 ? n : 4;
#else
    long n = sysconf(_SC_NPROCESSORS_ONLN);
    return n > 0 ? (int)n : 4;
#endif
}

void fr_platform_sleep_forever(void) {
#if defined(FORGE_OS_WINDOWS)
    for (;;) Sleep(INFINITE);
#else
    for (;;) pause();
#endif
}

ssize_t fr_sock_send(int fd, const void *buf, size_t len) {
#if defined(FORGE_OS_WINDOWS)
    return send((SOCKET)fd, (const char *)buf, (int)len, 0);
#else
    ssize_t sent = 0;
    const char *p = (const char *)buf;
    while ((size_t)sent < len) {
        ssize_t n = send(fd, p + sent, len - (size_t)sent, MSG_NOSIGNAL);
        if (n <= 0) return n;
        sent += n;
    }
    return sent;
#endif
}

ssize_t fr_sock_recv(int fd, void *buf, size_t len) {
#if defined(FORGE_OS_WINDOWS)
    return recv((SOCKET)fd, (char *)buf, (int)len, 0);
#else
    for (;;) {
        ssize_t n = recv(fd, buf, len, 0);
        if (n < 0 && errno == EINTR) continue;
        return n;
    }
#endif
}

int fr_sock_set_tcp_nodelay(int fd) {
    int yes = 1;
#if defined(FORGE_OS_WINDOWS)
    return setsockopt((SOCKET)fd, IPPROTO_TCP, TCP_NODELAY, (const char *)&yes, sizeof(yes));
#else
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
#endif
}

int fr_sock_set_recv_timeout(int fd, int timeout_ms) {
#if defined(FORGE_OS_WINDOWS)
    DWORD ms = (DWORD)timeout_ms;
    return setsockopt((SOCKET)fd, SOL_SOCKET, SO_RCVTIMEO, (const char *)&ms, sizeof(ms));
#else
    struct timeval tv;
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    return setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
#endif
}

int fr_sock_set_nonblocking(int fd) {
#if defined(FORGE_OS_WINDOWS)
    u_long mode = 1;
    return ioctlsocket((SOCKET)fd, FIONBIO, &mode);
#else
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
#endif
}

int fr_sock_set_blocking(int fd) {
#if defined(FORGE_OS_WINDOWS)
    u_long mode = 0;
    return ioctlsocket((SOCKET)fd, FIONBIO, &mode);
#else
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
#endif
}

int fr_sock_set_timeout(int fd, int timeout_ms) {
    if (timeout_ms <= 0) return -1;
#if defined(FORGE_OS_WINDOWS)
    DWORD timeout = (DWORD)timeout_ms;
    if (setsockopt((SOCKET)fd, SOL_SOCKET, SO_RCVTIMEO,
                   (const char *)&timeout, sizeof(timeout)) != 0) return -1;
    return setsockopt((SOCKET)fd, SOL_SOCKET, SO_SNDTIMEO,
                      (const char *)&timeout, sizeof(timeout));
#else
    struct timeval timeout;
    timeout.tv_sec = timeout_ms / 1000;
    timeout.tv_usec = (timeout_ms % 1000) * 1000;
    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) != 0) return -1;
    return setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
#endif
}

int fr_sock_accept_nb(int listen_fd) {
#if defined(FORGE_OS_LINUX)
    return (int)accept4(listen_fd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
#else
    fr_socket_t client = accept((fr_socket_t)listen_fd, NULL, NULL);
    if (client == FR_SOCK_INVALID) return -1;
    fr_sock_set_nonblocking((int)client);
    return (int)client;
#endif
}

void fr_sock_close(int fd) {
    if (fd < 0) return;
#if defined(FORGE_OS_WINDOWS)
    closesocket((SOCKET)fd);
#else
    close(fd);
#endif
}

int fr_path_exists(const char *path) {
    if (!path) return 0;
#if defined(FORGE_OS_WINDOWS)
    DWORD attr = GetFileAttributesA(path);
    return attr != INVALID_FILE_ATTRIBUTES;
#else
    return access(path, R_OK) == 0;
#endif
}

void fr_path_normalize(char *path) {
    if (!path) return;
#if defined(FORGE_OS_WINDOWS)
    for (char *p = path; *p; p++) {
        if (*p == '\\') *p = '/';
    }
#endif
}

int fr_make_temp_path(char *buf, size_t cap, const char *prefix, const char *suffix) {
    if (!buf || cap == 0) return -1;
#if defined(FORGE_OS_WINDOWS)
    char temp_dir[MAX_PATH];
    DWORD temp_len = GetTempPathA(sizeof(temp_dir), temp_dir);
    if (temp_len == 0 || temp_len >= sizeof(temp_dir)) return -1;
    char base[MAX_PATH];
    const char *ext = suffix ? suffix : "";
    for (unsigned attempt = 0; attempt < 128; ++attempt) {
        if (GetTempFileNameA(temp_dir, prefix ? prefix : "frg", 0, base) == 0)
            return -1;
        if (strlen(base) + strlen(ext) + 1 > cap) {
            DeleteFileA(base);
            return -1;
        }
        if (!ext[0]) {
            memcpy(buf, base, strlen(base) + 1);
            return 0;
        }

        char candidate[MAX_PATH];
        if (strlen(base) + strlen(ext) + 1 > sizeof(candidate)) {
            DeleteFileA(base);
            return -1;
        }
        strcpy(candidate, base);
        strcat(candidate, ext);
        /* MoveFileEx has no FAIL_IF_EXISTS flag. Omitting REPLACE_EXISTING
         * preserves an existing destination atomically, without a racy
         * existence check followed by a replacing move. */
        if (MoveFileExA(base, candidate, MOVEFILE_WRITE_THROUGH)) {
            memcpy(buf, candidate, strlen(candidate) + 1);
            return 0;
        }
        DWORD move_error = GetLastError();
        DeleteFileA(base);
        if (move_error != ERROR_ALREADY_EXISTS && move_error != ERROR_FILE_EXISTS)
            return -1;
    }
    return -1;
#else
    const char *name = prefix ? prefix : "forge";
    const char *ext = suffix ? suffix : "";
    int needed = snprintf(buf, cap, "/tmp/%s-XXXXXX%s", name, ext);
    if (needed < 0 || (size_t)needed >= cap) return -1;
#if defined(__GLIBC__) || defined(__APPLE__) || defined(__FreeBSD__) || defined(__NetBSD__) || defined(__OpenBSD__)
    int fd = mkstemps(buf, (int)strlen(ext));
#else
    if (ext[0]) return -1;
    int fd = mkstemp(buf);
#endif
    if (fd < 0) return -1;
    close(fd);
    return 0;
#endif
}
