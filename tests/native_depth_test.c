#define _POSIX_C_SOURCE 200809L
#include "forge_runtime.h"
#include "forge/thread.h"
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>

#define CHECK(c) do { if (!(c)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #c); exit(1); } } while (0)

static int fail_malloc, malloc_failures;
static int worker_limit = -1, worker_calls, worker_failures;
void *__real_malloc(size_t);
void *__wrap_malloc(size_t size) {
    if (fail_malloc) { fail_malloc = 0; ++malloc_failures; return NULL; }
    return __real_malloc(size);
}
int __real_fr_thread_start(fr_thread_t **, fr_thread_fn, void *);
int __wrap_fr_thread_start(fr_thread_t **out, fr_thread_fn fn, void *arg) {
    if (worker_limit >= 0 && worker_calls++ >= worker_limit) {
        ++worker_failures;
        return -1;
    }
    return __real_fr_thread_start(out, fn, arg);
}

static void noop(void *arg) { (void)arg; }
static void test_rejected_publication(void) {
    CHECK(fr_sched_pool_queued(NULL) == 0);
    fr_scheduler_t *s = fr_scheduler_create(4);
    CHECK(s && fr_sched_pool_queued(s) == 0);
    CHECK(fr_sched_pool_submit(NULL, noop, NULL) == -1);
    CHECK(fr_sched_pool_submit(s, NULL, NULL) == -1);
    fail_malloc = 1;
    CHECK(fr_sched_pool_submit(s, noop, NULL) == -1);
    CHECK(malloc_failures == 1 && !fail_malloc);
    CHECK(fr_sched_pool_queued(s) == 0);
    CHECK(fr_sched_pool_submit(s, noop, NULL) == 0);
    CHECK(fr_sched_pool_queued(s) == 1);
    fr_scheduler_run(s);
    CHECK(fr_sched_pool_queued(s) == 0);
    fr_scheduler_destroy(s);
}

typedef struct {
    fr_mutex_t *lock;
    fr_cond_t *changed;
    int entered, released, completed;
} gate_t;
static void gated(void *arg) {
    gate_t *gate = arg;
    fr_mutex_lock(gate->lock);
    ++gate->entered;
    fr_cond_broadcast(gate->changed);
    while (!gate->released) fr_cond_wait(gate->changed, gate->lock);
    ++gate->completed;
    fr_mutex_unlock(gate->lock);
}

typedef struct {
    fr_scheduler_t *s;
    atomic_int seen;
    atomic_int *completed;
    size_t upper_bound;
} item_t;
static void counted(void *arg) {
    item_t *item = arg;
    /* Native callbacks run outside the scheduler lock and may query it. */
    CHECK(fr_sched_pool_queued(item->s) <= item->upper_bound);
    CHECK(atomic_fetch_add(&item->seen, 1) == 0);
    atomic_fetch_add(item->completed, 1);
}
static void *stopper(void *arg) { fr_scheduler_stop(arg); return NULL; }

static void test_gated_stop_restart(int workers, int active, int activate_late) {
    enum { BACKLOG = 31, EXTRA = 7, TOTAL = BACKLOG + EXTRA };
    fr_scheduler_t *s = fr_scheduler_create(workers);
    CHECK(s);
    gate_t gate = {fr_mutex_create(), fr_cond_create(), 0, 0, 0};
    CHECK(gate.lock && gate.changed);
    atomic_int completed = 0;
    item_t items[TOTAL];
    for (int i = 0; i < TOTAL; ++i) {
        items[i].s = s;
        atomic_init(&items[i].seen, 0);
        items[i].completed = &completed;
        items[i].upper_bound = TOTAL + (size_t)active;
    }
    /* First jobs occupy each surviving worker. With one surviving worker zero,
     * the backlog on the failed workers' queues must later be stolen. */
    for (int i = 0; i < active; ++i) CHECK(fr_sched_pool_submit(s, gated, &gate) == 0);
    for (int i = 0; i < BACKLOG; ++i) CHECK(fr_sched_pool_submit(s, counted, &items[i]) == 0);
    if (!activate_late) CHECK(fr_sched_pool_queued(s) == (size_t)(active + BACKLOG));
    worker_limit = active;
    worker_calls = worker_failures = 0;
    fr_scheduler_start(s);
    CHECK(worker_failures == workers - active);
    worker_limit = -1;
    fr_mutex_lock(gate.lock);
    while (gate.entered < active) fr_cond_wait(gate.changed, gate.lock);
    fr_mutex_unlock(gate.lock);
    /* Claimed/running jobs are excluded, even though none has completed. */
    if (activate_late < 2) CHECK(fr_sched_pool_queued(s) == BACKLOG);
    for (int i = BACKLOG; i < TOTAL; ++i) CHECK(fr_sched_pool_submit(s, counted, &items[i]) == 0);
    if (activate_late < 2) CHECK(fr_sched_pool_queued(s) == TOTAL);

    fr_thread_t *stop = NULL;
    CHECK(fr_thread_start(&stop, stopper, s) == 0);
    /* Stop clears running before joining the gated workers. Observing this
     * state prevents them from draining backlog when their gate is released. */
    while (fr_sched_pool_available()) fr_thread_yield();
    CHECK(fr_sched_pool_queued(s) == TOTAL);
    fr_mutex_lock(gate.lock);
    gate.released = 1;
    fr_cond_broadcast(gate.changed);
    fr_mutex_unlock(gate.lock);
    CHECK(fr_thread_join(stop) == 0);
    CHECK(gate.completed == active);
    CHECK(atomic_load(&completed) == 0);
    CHECK(fr_sched_pool_queued(s) == TOTAL);
    fr_scheduler_run(s);
    CHECK(fr_sched_pool_queued(s) == 0);
    CHECK(atomic_load(&completed) == TOTAL);
    for (int i = 0; i < TOTAL; ++i) CHECK(atomic_load(&items[i].seen) == 1);
    fr_scheduler_destroy(s);
    fr_cond_destroy(gate.changed);
    fr_mutex_destroy(gate.lock);
}

static void test_first_query_inside_callback(void) {
    fr_scheduler_t *s = fr_scheduler_create(2);
    CHECK(s);
    atomic_int completed = 0;
    item_t item = {.s = s, .completed = &completed, .upper_bound = 1};
    atomic_init(&item.seen, 0);
    CHECK(fr_sched_pool_submit(s, counted, &item) == 0);
    fr_scheduler_run(s); /* No caller query before the callback is claimed. */
    CHECK(atomic_load(&completed) == 1 && atomic_load(&item.seen) == 1);
    CHECK(fr_sched_pool_queued(s) == 0);
    fr_scheduler_destroy(s);
}

enum { PRODUCERS = 4, PER_PRODUCER = 1000, JOBS = PRODUCERS * PER_PRODUCER };
typedef struct { fr_scheduler_t *s; item_t *items; } producer_t;
static void *producer(void *arg) {
    producer_t *p = arg;
    for (int i = 0; i < PER_PRODUCER; ++i)
        CHECK(fr_sched_pool_submit(p->s, counted, &p->items[i]) == 0);
    return NULL;
}
typedef struct {
    fr_scheduler_t *s;
    atomic_int ready, stop;
    atomic_size_t observations;
} observer_t;
static void *observer(void *arg) {
    observer_t *o = arg;
    atomic_fetch_add(&o->ready, 1);
    while (!atomic_load(&o->stop)) {
        CHECK(fr_sched_pool_queued(o->s) <= JOBS);
        atomic_fetch_add(&o->observations, 1);
    }
    return NULL;
}
static void test_concurrent_observation(void) {
    fr_scheduler_t *s = fr_scheduler_create(4);
    CHECK(s);
    item_t *items = calloc(JOBS, sizeof(*items));
    CHECK(items);
    atomic_int completed = 0;
    for (int i = 0; i < JOBS; ++i) {
        items[i].s = s;
        atomic_init(&items[i].seen, 0);
        items[i].completed = &completed;
        items[i].upper_bound = JOBS;
    }
    observer_t observation = {.s = s};
    atomic_init(&observation.ready, 0);
    atomic_init(&observation.stop, 0);
    atomic_init(&observation.observations, 0);
    fr_thread_t *observers[2], *producers[PRODUCERS];
    producer_t arguments[PRODUCERS];
    fr_scheduler_start(s);
    for (int i = 0; i < 2; ++i) CHECK(fr_thread_start(&observers[i], observer, &observation) == 0);
    while (atomic_load(&observation.ready) != 2) fr_thread_yield();
    for (int i = 0; i < PRODUCERS; ++i) {
        arguments[i] = (producer_t){s, items + i * PER_PRODUCER};
        CHECK(fr_thread_start(&producers[i], producer, &arguments[i]) == 0);
    }
    for (int i = 0; i < PRODUCERS; ++i) CHECK(fr_thread_join(producers[i]) == 0);
    fr_scheduler_run(s);
    atomic_store(&observation.stop, 1);
    for (int i = 0; i < 2; ++i) CHECK(fr_thread_join(observers[i]) == 0);
    CHECK(atomic_load(&observation.observations) > 0);
    CHECK(atomic_load(&completed) == JOBS && fr_sched_pool_queued(s) == 0);
    for (int i = 0; i < JOBS; ++i) CHECK(atomic_load(&items[i].seen) == 1);
    fr_scheduler_destroy(s);
    free(items);
}

int main(void) {
    test_rejected_publication();
    test_first_query_inside_callback();
    for (int late = 0; late < 3; ++late) {
        test_gated_stop_restart(1, 1, late);
        test_gated_stop_restart(4, 4, late);
        test_gated_stop_restart(4, 1, late);
    }
    for (int repeat = 0; repeat < 3; ++repeat) test_concurrent_observation();
    puts("native depth tests passed");
    return 0;
}
