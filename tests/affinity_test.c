#define _GNU_SOURCE
#include "forge_runtime.h"
#include "forge/platform.h"
#include "forge/thread.h"
#include <errno.h>
#include <limits.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x); exit(1); } } while (0)

enum { REAL, SMALL, LARGE, EMPTY, DENIED, TOO_LARGE };
static int probe_mode, probes, allocations, releases, fallback_calls, fail_allocation;
static size_t largest_probe;
static void *live_allocation;
void *__real_malloc(size_t);
void __real_free(void *);
long __real_sysconf(int);
int __real_sched_getaffinity(pid_t, size_t, cpu_set_t *);
int __real_pthread_setaffinity_np(pthread_t, size_t, const cpu_set_t *);

void *__wrap_malloc(size_t bytes) {
    if (probe_mode != REAL) {
        allocations++;
        if (allocations == fail_allocation) return NULL;
        CHECK(live_allocation == NULL);
        live_allocation = __real_malloc(bytes);
        CHECK(live_allocation != NULL);
        return live_allocation;
    }
    return __real_malloc(bytes);
}
void __wrap_free(void *p) {
    if (probe_mode != REAL && p) {
        CHECK(p == live_allocation);
        live_allocation = NULL;
        releases++;
    }
    __real_free(p);
    /* Prove the affinity error was saved before cleanup. */
    if (probe_mode != REAL) errno = EIO;
}
long __wrap_sysconf(int name) {
    if (probe_mode != REAL && name == _SC_NPROCESSORS_ONLN) {
        fallback_calls++;
        return 7;
    }
    return __real_sysconf(name);
}
int __wrap_sched_getaffinity(pid_t pid, size_t bytes, cpu_set_t *mask) {
    if (probe_mode == REAL) return __real_sched_getaffinity(pid, bytes, mask);
    CHECK(pid == 0);
    probes++;
    if (bytes > largest_probe) largest_probe = bytes;
    if (probe_mode == DENIED) { errno = EPERM; return -1; }
    if (probe_mode == TOO_LARGE || (probe_mode == LARGE && bytes < 512)) {
        errno = EINVAL;
        return -1;
    }
    CPU_ZERO_S(bytes, mask);
    if (probe_mode == SMALL) {
        CPU_SET_S(1, bytes, mask); CPU_SET_S(5, bytes, mask); CPU_SET_S(7, bytes, mask);
    } else if (probe_mode == LARGE) {
        CPU_SET_S(0, bytes, mask); CPU_SET_S(67, bytes, mask);
        CPU_SET_S(2048, bytes, mask); CPU_SET_S(4095, bytes, mask);
    }
    return 0;
}
static int reject_pin, pin_calls;
int __wrap_pthread_setaffinity_np(pthread_t thread, size_t bytes, const cpu_set_t *mask) {
    pin_calls++;
    if (reject_pin) return EINVAL;
    return __real_pthread_setaffinity_np(thread, bytes, mask);
}

static void simulate(int mode, int fail, int expected, int fallback) {
    probe_mode = mode;
    probes = allocations = releases = fallback_calls = 0;
    fail_allocation = fail;
    largest_probe = 0;
    CHECK(fr_platform_available_cpu_count() == expected);
    CHECK(fallback_calls == fallback && live_allocation == NULL);
    CHECK(releases == allocations - (fail > 0 && allocations >= fail));
    if (mode == SMALL || mode == EMPTY || mode == DENIED) CHECK(allocations == 0 && probes == 1);
    if (mode == TOO_LARGE) CHECK(probes < 20 && largest_probe == 1024 * 1024);
    probe_mode = REAL;
}

typedef struct {
    cpu_set_t expected;
    fr_mutex_t *lock;
    fr_cond_t *cond;
    int entered, release;
} gate_t;
static void inspect_worker(void *arg) {
    gate_t *g = arg;
    cpu_set_t actual;
    CHECK(sched_getaffinity(0, sizeof(actual), &actual) == 0);
    CHECK(CPU_EQUAL(&actual, &g->expected));
    CHECK(fr_platform_available_cpu_count() == CPU_COUNT(&g->expected));
    fr_mutex_lock(g->lock);
    g->entered++;
    fr_cond_broadcast(g->cond);
    while (!g->release) fr_cond_wait(g->cond, g->lock);
    fr_mutex_unlock(g->lock);
}
static void pool_mask(cpu_set_t mask, int requested, int expected_count) {
    CHECK(sched_setaffinity(0, sizeof(mask), &mask) == 0);
    CHECK(fr_platform_available_cpu_count() == CPU_COUNT(&mask));
    CHECK(fr_platform_cpu_count() == __real_sysconf(_SC_NPROCESSORS_ONLN));
    fr_scheduler_t *s = fr_scheduler_create(requested);
    CHECK(s && fr_scheduler_worker_count(s) == expected_count);
    gate_t g = {.expected = mask, .lock = fr_mutex_create(), .cond = fr_cond_create()};
    CHECK(g.lock && g.cond);
    fr_scheduler_start(s);
    for (int i = 0; i < expected_count; i++) CHECK(fr_sched_pool_submit(s, inspect_worker, &g) == 0);
    fr_mutex_lock(g.lock);
    while (g.entered != expected_count) fr_cond_wait(g.cond, g.lock);
    g.release = 1;
    fr_cond_broadcast(g.cond);
    fr_mutex_unlock(g.lock);
    fr_scheduler_run(s);
    fr_scheduler_destroy(s);
    fr_cond_destroy(g.cond);
    fr_mutex_destroy(g.lock);
}
static cpu_set_t original;
static int first_cpu, last_cpu;
static void *explicit_pin(void *arg) {
    (void)arg;
    cpu_set_t mask;
    fr_thread_pin_cpu(last_cpu);
    CHECK(sched_getaffinity(0, sizeof(mask), &mask) == 0);
    CHECK(CPU_COUNT(&mask) == 1 && CPU_ISSET(last_cpu, &mask));
    CHECK(fr_platform_available_cpu_count() == 1);
    int before = pin_calls;
    fr_thread_pin_cpu(-1); fr_thread_pin_cpu(CPU_SETSIZE); fr_thread_pin_cpu(INT_MAX);
    CHECK(pin_calls == before);
    reject_pin = 1;
    fr_thread_pin_cpu(first_cpu);
    fr_thread_pin_cpu(CPU_SETSIZE - 1);
    reject_pin = 0;
    CHECK(sched_getaffinity(0, sizeof(mask), &mask) == 0);
    CHECK(CPU_COUNT(&mask) == 1 && CPU_ISSET(last_cpu, &mask));
    fr_thread_pin_cpu(first_cpu); /* Explicit request may replace an inherited mask. */
    CHECK(sched_getaffinity(0, sizeof(mask), &mask) == 0);
    CHECK(CPU_COUNT(&mask) == 1 && CPU_ISSET(first_cpu, &mask));
    return NULL;
}
int main(void) {
    simulate(SMALL, 0, 3, 0);
    simulate(LARGE, 0, 4, 0);
    CHECK(probes == 3 && allocations == 2);
    simulate(LARGE, 1, 7, 1);
    simulate(LARGE, 2, 7, 1);
    simulate(EMPTY, 0, 7, 1);
    simulate(DENIED, 0, 7, 1);
    simulate(TOO_LARGE, 0, 7, 1);
    CHECK(sched_getaffinity(0, sizeof(original), &original) == 0);
    first_cpu = -1;
    for (int i = 0; i < CPU_SETSIZE; i++) if (CPU_ISSET(i, &original)) {
        if (first_cpu < 0) first_cpu = i;
        last_cpu = i;
    }
    CHECK(first_cpu >= 0);
    cpu_set_t one, sparse;
    CPU_ZERO(&one); CPU_SET(last_cpu, &one);
    sparse = one; CPU_SET(first_cpu, &sparse);
    pool_mask(one, 0, 1);
    pool_mask(sparse, 0, CPU_COUNT(&sparse));
    pool_mask(one, 3, 3); /* Explicit oversubscription stays supported. */
    CHECK(sched_setaffinity(0, sizeof(original), &original) == 0);
    fr_thread_t *thread = NULL;
    CHECK(fr_thread_start(&thread, explicit_pin, NULL) == 0);
    CHECK(fr_thread_join(thread) == 0);
    cpu_set_t actual;
    CHECK(sched_getaffinity(0, sizeof(actual), &actual) == 0 && CPU_EQUAL(&actual, &original));
    CHECK(fr_platform_available_cpu_count() == CPU_COUNT(&original));
    return 0;
}
