#define _POSIX_C_SOURCE 200809L
#include "forge_runtime.h"
#include "forge/thread.h"
#include "work_queue.h"
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(x) do { if (!(x)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #x); exit(1); } } while (0)

static atomic_int fail_push, failed_pushes, fail_start, failed_starts;
static atomic_int observe_waits, worker_idle, failed_wake_seen, controller_returned;
static _Atomic(fr_cond_t *) controller_cond;
static _Thread_local int is_controller;
void __real_fr_cond_wait(fr_cond_t *, fr_mutex_t *);
void __wrap_fr_cond_wait(fr_cond_t *c, fr_mutex_t *m) {
    if (atomic_load(&observe_waits)) {
        if (is_controller) atomic_store(&controller_cond, c);
        else atomic_store(&worker_idle, 1);
    }
    __real_fr_cond_wait(c, m);
}
void __real_fr_cond_signal(fr_cond_t *);
void __wrap_fr_cond_signal(fr_cond_t *c) {
    if (atomic_load(&observe_waits) && c == atomic_load(&controller_cond))
        atomic_store(&failed_wake_seen, 1);
    __real_fr_cond_signal(c);
}
int __real_fr_run_queue_try_push_owned(fr_run_queue_t *, fr_coro_t *);
int __wrap_fr_run_queue_try_push_owned(fr_run_queue_t *q, fr_coro_t *c) {
    if (atomic_exchange(&fail_push, 0)) {
        atomic_fetch_add(&failed_pushes, 1);
        return 0;
    }
    return __real_fr_run_queue_try_push_owned(q, c);
}
int __real_fr_thread_start(fr_thread_t **, fr_thread_fn, void *);
int __wrap_fr_thread_start(fr_thread_t **out, fr_thread_fn fn, void *arg) {
    if (atomic_exchange(&fail_start, 0)) {
        atomic_fetch_add(&failed_starts, 1);
        return -1;
    }
    return __real_fr_thread_start(out, fn, arg);
}

typedef struct { atomic_int *completed; } count_state_t;
static fr_coro_status_t count_coro(fr_coro_t *coro, void *arg) {
    (void)coro;
    count_state_t *s = arg;
    atomic_fetch_add(s->completed, 1);
    return FR_CORO_DONE;
}
static fr_coro_t *spawn_count(fr_process_t *p, atomic_int *completed) {
    count_state_t *s = malloc(sizeof(*s));
    CHECK(s);
    *s = (count_state_t){completed};
    fr_coro_t *c = fr_coro_spawn(p, count_coro, s, sizeof(*s));
    CHECK(c);
    return c;
}
typedef struct {
    fr_mutex_t *lock;
    fr_cond_t *cond;
    int entered, release;
} gate_t;
static gate_t make_gate(void) {
    gate_t g = {.lock = fr_mutex_create(), .cond = fr_cond_create()};
    CHECK(g.lock && g.cond);
    return g;
}
static void gate(void *arg) {
    gate_t *g = arg;
    fr_mutex_lock(g->lock);
    g->entered = 1;
    fr_cond_broadcast(g->cond);
    while (!g->release) fr_cond_wait(g->cond, g->lock);
    fr_mutex_unlock(g->lock);
}
static void wait_gate(gate_t *g) {
    fr_mutex_lock(g->lock);
    while (!g->entered) fr_cond_wait(g->cond, g->lock);
    fr_mutex_unlock(g->lock);
}
static void release_gate(gate_t *g) {
    fr_mutex_lock(g->lock);
    g->release = 1;
    fr_cond_broadcast(g->cond);
    fr_mutex_unlock(g->lock);
}
static void destroy_gate(gate_t *g) {
    fr_cond_destroy(g->cond);
    fr_mutex_destroy(g->lock);
}

/* Attach already-created work from a native callback while queue inventory
 * starts at zero, then add more work to that same nonempty queue. */
typedef struct { fr_scheduler_t *sched; atomic_int completed, published; } publish_t;
static void publish(void *arg) {
    publish_t *s = arg;
    fr_process_t *p = fr_process_create("late-attach");
    CHECK(p);
    spawn_count(p, &s->completed);
    fr_scheduler_add_process(s->sched, p);
    spawn_count(p, &s->completed);
    atomic_store(&s->published, 1);
}
static void late_publish(void) {
    publish_t s = {.sched = fr_scheduler_create(1)};
    CHECK(s.sched);
    CHECK(fr_sched_pool_submit(s.sched, publish, &s) == 0);
    fr_scheduler_run(s.sched);
    CHECK(atomic_load(&s.published) == 1 && atomic_load(&s.completed) == 2);
    fr_scheduler_destroy(s.sched);
}

static void *stop_pool(void *arg) { fr_scheduler_stop(arg); return NULL; }
static void retained_queue(void) {
    fr_scheduler_t *s = fr_scheduler_create(1);
    CHECK(s);
    gate_t g = make_gate();
    CHECK(fr_sched_pool_submit(s, gate, &g) == 0);
    fr_scheduler_start(s);
    wait_gate(&g);
    atomic_int completed = 0;
    fr_process_t *p = fr_process_create("retained-coroutines");
    CHECK(p);
    fr_scheduler_add_process(s, p);
    for (int i = 0; i < 9; i++) spawn_count(p, &completed);
    fr_thread_t *stopper = NULL;
    CHECK(fr_thread_start(&stopper, stop_pool, s) == 0);
    while (fr_sched_pool_available()) fr_thread_yield();
    release_gate(&g);
    CHECK(fr_thread_join(stopper) == 0);
    CHECK(atomic_load(&completed) == 0);
    fr_scheduler_run(s);
    CHECK(atomic_load(&completed) == 9);
    fr_scheduler_destroy(s);
    destroy_gate(&g);
}

static void partial_workers(void) {
    fr_scheduler_t *s = fr_scheduler_create(2);
    fr_process_t *p = fr_process_create("missing-owner-worker");
    CHECK(s && p);
    fr_scheduler_add_process(s, p); /* First process belongs to queue zero. */
    atomic_int completed = 0;
    spawn_count(p, &completed);
    int before = atomic_load(&failed_starts);
    atomic_store(&fail_start, 1); /* Worker zero cannot start; worker one steals. */
    fr_scheduler_run(s);
    CHECK(atomic_load(&failed_starts) == before + 1 && atomic_load(&completed) == 1);
    fr_scheduler_destroy(s);
}

typedef struct { atomic_int calls; } retry_state_t;
static fr_coro_status_t repeat_until_failed_requeue(fr_coro_t *coro, void *arg) {
    retry_state_t *s = arg;
    if (atomic_fetch_add(&s->calls, 1) == 0) atomic_store(&fail_push, 1);
    return fr_yield(coro);
}
static void push_failures(void) {
    fr_scheduler_t *s = fr_scheduler_create(1);
    fr_process_t *p = fr_process_create("queue-failure");
    CHECK(s && p);
    fr_scheduler_add_process(s, p);
    atomic_int completed = 0;
    int before = atomic_load(&failed_pushes);
    atomic_store(&fail_push, 1);
    spawn_count(p, &completed); /* Failed initial publication becomes ERROR. */
    spawn_count(p, &completed);
    fr_scheduler_run(s);
    CHECK(atomic_load(&completed) == 1 && atomic_load(&failed_pushes) == before + 1);
    retry_state_t *state = calloc(1, sizeof(*state));
    CHECK(state);
    atomic_init(&state->calls, 0);
    CHECK(fr_coro_spawn(p, repeat_until_failed_requeue, state, sizeof(*state)));
    fr_scheduler_run(s);
    CHECK(atomic_load(&state->calls) > 1 && atomic_load(&failed_pushes) == before + 2);
    spawn_count(p, &completed);
    fr_scheduler_run(s);
    CHECK(atomic_load(&completed) == 2);
    fr_scheduler_destroy(s);
}

typedef struct { atomic_int waiting, completed; } receiver_t;
static fr_coro_status_t receive(fr_coro_t *coro, void *arg) {
    receiver_t *s = arg;
    fr_msg_t message;
    if (!fr_try_recv(fr_coro_process(coro), &message)) {
        atomic_store(&s->waiting, 1);
        return FR_CORO_WAITING_RECV;
    }
    CHECK(message.tag == 17 && message.value == 43);
    fr_msg_free_payload(&message);
    atomic_fetch_add(&s->completed, 1);
    return FR_CORO_DONE;
}
static void wake_during_native(void) {
    fr_scheduler_t *s = fr_scheduler_create(1);
    fr_process_t *p = fr_process_create("native-to-coroutine-wake");
    CHECK(s && p);
    fr_scheduler_add_process(s, p);
    receiver_t *state = calloc(1, sizeof(*state));
    CHECK(state);
    atomic_init(&state->waiting, 0); atomic_init(&state->completed, 0);
    CHECK(fr_coro_spawn(p, receive, state, sizeof(*state)));
    gate_t g = make_gate();
    CHECK(fr_sched_pool_submit(s, gate, &g) == 0);
    fr_scheduler_start(s);
    wait_gate(&g); /* Receiver has committed WAITING_RECV before native claim. */
    CHECK(atomic_load(&state->waiting) == 1);
    CHECK(fr_send(p, 17, 43, NULL, 0));
    CHECK(atomic_load(&state->completed) == 0);
    release_gate(&g);
    fr_scheduler_run(s);
    CHECK(atomic_load(&state->completed) == 1);
    fr_scheduler_destroy(s);
    destroy_gate(&g);
}
static void *run_pool(void *arg) {
    is_controller = 1;
    fr_scheduler_run(arg);
    atomic_store(&controller_returned, 1);
    return NULL;
}
static void failed_external_wake(void) {
    fr_scheduler_t *s = fr_scheduler_create(1);
    fr_process_t *p = fr_process_create("last-receiver-enqueue-failure");
    CHECK(s && p);
    fr_scheduler_add_process(s, p);
    receiver_t *state = calloc(1, sizeof(*state));
    CHECK(state);
    atomic_init(&state->waiting, 0); atomic_init(&state->completed, 0);
    CHECK(fr_coro_spawn(p, receive, state, sizeof(*state)));
    atomic_store(&worker_idle, 0); atomic_store(&failed_wake_seen, 0);
    atomic_store(&controller_returned, 0); atomic_store(&controller_cond, NULL);
    atomic_store(&observe_waits, 1);
    fr_thread_t *controller = NULL;
    CHECK(fr_thread_start(&controller, run_pool, s) == 0);
    while (!atomic_load(&worker_idle) || !atomic_load(&controller_cond)) fr_thread_yield();
    CHECK(atomic_load(&state->waiting) == 1);
    /* The sole worker has committed WAITING_RECV and the controller has entered
     * its condition wait. Sending takes the same scheduler mutex after release. */
    atomic_store(&failed_wake_seen, 0);
    atomic_store(&fail_push, 1);
    CHECK(fr_send(p, 17, 43, NULL, 0));
    CHECK(atomic_load(&fail_push) == 0);
    /* A missing predicate wake fails immediately instead of hanging the test. */
    CHECK(atomic_load(&failed_wake_seen) == 1);
    CHECK(fr_thread_join(controller) == 0 && atomic_load(&controller_returned) == 1);
    CHECK(atomic_load(&state->completed) == 0);
    atomic_store(&observe_waits, 0); atomic_store(&controller_cond, NULL);
    fr_scheduler_destroy(s);
}
int main(int argc, char **argv) {
    if (argc == 2 && !strcmp(argv[1], "wake-failure")) {
        failed_external_wake();
        return 0;
    }
    CHECK(argc == 1);
    late_publish();
    retained_queue();
    partial_workers();
    push_failures();
    wake_during_native();
    failed_external_wake();
    CHECK(atomic_load(&fail_push) == 0 && atomic_load(&fail_start) == 0);
    return 0;
}
