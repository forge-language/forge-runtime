#define _POSIX_C_SOURCE 200809L
#include "forge_runtime.h"
#include "forge/event.h"
#include "forge/thread.h"
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#define CHECK(c) do { if (!(c)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #c); exit(1); } } while (0)

static void pause_ms(int ms) {
    struct timespec delay = {ms / 1000, (ms % 1000) * 1000000L};
    nanosleep(&delay, NULL);
}

typedef struct {
    int fd;
    int count;
    int target;
    int expose_race;
    atomic_int *executing;
    atomic_int *completed;
} reader_state_t;

static fr_coro_status_t reader(fr_coro_t *coro, void *state) {
    reader_state_t *s = state;
    CHECK(atomic_fetch_add(s->executing, 1) == 0);
    int64_t ready = fr_await_fd(coro, s->fd, FR_EVENT_READ);
    CHECK(ready >= 0);
    if (!ready) {
        /* Give the event callback time to observe readiness before the current
         * invocation returns; it must not enqueue a second concurrent reader. */
        if (s->expose_race) pause_ms(2);
        atomic_fetch_sub(s->executing, 1);
        return FR_CORO_WAITING_IO;
    }
    char byte;
    CHECK(read(s->fd, &byte, 1) == 1);
    s->count++;
    atomic_fetch_add(s->completed, 1);
    atomic_fetch_sub(s->executing, 1);
    return s->count == s->target ? FR_CORO_DONE : fr_yield(coro);
}

typedef struct { int fd; int count; } writer_state_t;
static void *writer(void *arg) {
    writer_state_t *s = arg;
    for (int i = 0; i < s->count; i++) {
        pause_ms(2);
        CHECK(write(s->fd, "x", 1) == 1);
    }
    return NULL;
}

static void test_await(int immediate) {
    int fds[2];
    CHECK(pipe(fds) == 0);
    atomic_int executing = 0, completed = 0;
    fr_scheduler_t *sched = fr_scheduler_create(4);
    fr_process_t *proc = fr_process_create("reader");
    CHECK(sched && proc);
    fr_scheduler_add_process(sched, proc);
    reader_state_t *state = malloc(sizeof(*state));
    CHECK(state);
    *state = (reader_state_t){fds[0], 0, 20, immediate, &executing, &completed};
    CHECK(fr_coro_spawn(proc, reader, state, sizeof(*state)));
    fr_thread_t *thread = NULL;
    writer_state_t write_state = {fds[1], 20};
    if (immediate) {
        for (int i = 0; i < 20; i++) CHECK(write(fds[1], "x", 1) == 1);
    } else CHECK(fr_thread_start(&thread, writer, &write_state) == 0);
    fr_scheduler_run(sched);
    if (thread) CHECK(fr_thread_join(thread) == 0);
    CHECK(atomic_load(&completed) == 20);
    CHECK(atomic_load(&executing) == 0);
    fr_scheduler_destroy(sched);
    close(fds[0]); close(fds[1]);
}

static atomic_int native_completed;
static void native_job(void *arg) {
    (void)arg;
    pause_ms(5);
    atomic_fetch_add(&native_completed, 1);
}

static void test_native_completion(void) {
    atomic_store(&native_completed, 0);
    fr_scheduler_t *sched = fr_scheduler_create(4);
    CHECK(sched);
    for (int i = 0; i < 32; i++) fr_sched_pool_submit(sched, native_job, NULL);
    fr_scheduler_run(sched);
    CHECK(atomic_load(&native_completed) == 32);
    fr_scheduler_destroy(sched);
}

static void *submit_jobs(void *arg) {
    fr_scheduler_t *sched = arg;
    for (int i = 0; i < 50; i++) fr_sched_pool_submit(sched, native_job, NULL);
    return NULL;
}

static void test_idle_wakeup(void) {
    atomic_store(&native_completed, 0);
    fr_scheduler_t *sched = fr_scheduler_create(4);
    CHECK(sched);
    fr_scheduler_start(sched);
    pause_ms(10);
    fr_thread_t *submitters[4];
    for (int i = 0; i < 4; i++) CHECK(fr_thread_start(&submitters[i], submit_jobs, sched) == 0);
    for (int i = 0; i < 4; i++) CHECK(fr_thread_join(submitters[i]) == 0);
    fr_scheduler_run(sched);
    CHECK(atomic_load(&native_completed) == 200);
    fr_scheduler_destroy(sched);
}

static void test_stop_waiting(void) {
    int fds[2];
    CHECK(pipe(fds) == 0);
    atomic_int executing = 0, completed = 0;
    fr_scheduler_t *sched = fr_scheduler_create(2);
    fr_process_t *proc = fr_process_create("stop-reader");
    CHECK(sched && proc);
    fr_scheduler_add_process(sched, proc);
    reader_state_t *state = malloc(sizeof(*state));
    CHECK(state);
    *state = (reader_state_t){fds[0], 0, 1, 0, &executing, &completed};
    CHECK(fr_coro_spawn(proc, reader, state, sizeof(*state)));
    fr_scheduler_start(sched);
    pause_ms(10);
    fr_scheduler_stop(sched);
    CHECK(atomic_load(&completed) == 0);
    CHECK(write(fds[1], "x", 1) == 1);
    fr_scheduler_run(sched);
    CHECK(atomic_load(&completed) == 1);
    fr_scheduler_destroy(sched);
    close(fds[0]); close(fds[1]);
    for (int i = 0; i < 20; i++) {
        sched = fr_scheduler_create(2);
        CHECK(sched);
        fr_scheduler_start(sched);
        fr_scheduler_stop(sched);
        fr_scheduler_destroy(sched);
    }
}

typedef struct { fr_process_t *proc; atomic_int *completed; } recv_state_t;
static fr_coro_status_t receiver(fr_coro_t *coro, void *state) {
    (void)coro;
    recv_state_t *s = state;
    fr_msg_t message;
    if (!fr_try_recv(s->proc, &message)) return FR_CORO_WAITING_RECV;
    CHECK(message.value == 42);
    atomic_fetch_add(s->completed, 1);
    return FR_CORO_DONE;
}
static void *sender(void *arg) {
    pause_ms(5);
    fr_send(arg, 1, 42, NULL, 0);
    return NULL;
}
static void test_receive_wakeup(void) {
    atomic_int completed = 0;
    fr_scheduler_t *sched = fr_scheduler_create(2);
    fr_process_t *proc = fr_process_create("receiver");
    CHECK(sched && proc);
    fr_scheduler_add_process(sched, proc);
    recv_state_t *state = malloc(sizeof(*state));
    CHECK(state);
    *state = (recv_state_t){proc, &completed};
    CHECK(fr_coro_spawn(proc, receiver, state, sizeof(*state)));
    fr_thread_t *thread;
    CHECK(fr_thread_start(&thread, sender, proc) == 0);
    fr_scheduler_run(sched);
    CHECK(fr_thread_join(thread) == 0);
    CHECK(atomic_load(&completed) == 1);
    fr_scheduler_destroy(sched);
}

typedef struct { int fd; atomic_int *registered; } stop_state_t;
static fr_coro_status_t await_stop(fr_coro_t *coro, void *arg) {
    stop_state_t *s = arg;
    CHECK(fr_await_fd(coro, s->fd, FR_EVENT_READ) == 0);
    atomic_store(s->registered, 1);
    return FR_CORO_WAITING_IO;
}
static void *run_scheduler(void *arg) {
    fr_scheduler_run(arg);
    return NULL;
}
static void test_external_stop(void) {
    int fds[2];
    CHECK(pipe(fds) == 0);
    atomic_int registered = 0;
    fr_scheduler_t *sched = fr_scheduler_create(2);
    fr_process_t *proc = fr_process_create("external-stop");
    CHECK(sched && proc);
    fr_scheduler_add_process(sched, proc);
    stop_state_t *state = malloc(sizeof(*state));
    CHECK(state);
    *state = (stop_state_t){fds[0], &registered};
    CHECK(fr_coro_spawn(proc, await_stop, state, sizeof(*state)));
    fr_thread_t *runner;
    CHECK(fr_thread_start(&runner, run_scheduler, sched) == 0);
    while (!atomic_load(&registered)) fr_thread_yield();
    fr_scheduler_stop(sched);
    CHECK(fr_thread_join(runner) == 0);
    fr_scheduler_destroy(sched);
    close(fds[0]); close(fds[1]);
}

typedef struct {
    fr_process_t *proc;
    atomic_int *completed;
    int spawn_child;
    int fail;
} task_state_t;

static fr_coro_status_t counted_task(fr_coro_t *coro, void *arg) {
    task_state_t *s = arg;
    if (s->spawn_child) {
        task_state_t *child = malloc(sizeof(*child));
        CHECK(child);
        *child = (task_state_t){s->proc, s->completed, 0, 0};
        CHECK(fr_coro_spawn(s->proc, counted_task, child, sizeof(*child)));
        s->spawn_child = 0;
        return fr_yield(coro);
    }
    atomic_fetch_add(s->completed, 1);
    return s->fail ? FR_CORO_ERROR : FR_CORO_DONE;
}

static void spawn_counted(fr_process_t *proc, atomic_int *completed, int child, int fail) {
    task_state_t *state = malloc(sizeof(*state));
    CHECK(state);
    *state = (task_state_t){proc, completed, child, fail};
    CHECK(fr_coro_spawn(proc, counted_task, state, sizeof(*state)));
}

static void test_attach_spawn_and_restart(void) {
    atomic_int completed = 0;
    fr_scheduler_t *sched = fr_scheduler_create(4);
    fr_process_t *proc = fr_process_create("counted");
    CHECK(sched && proc);
    for (int i = 0; i < 4000; i++) spawn_counted(proc, &completed, i % 2, i % 3 == 0);
    fr_scheduler_add_process(sched, proc);
    fr_scheduler_run(sched);
    CHECK(atomic_load(&completed) == 6000);
    fr_scheduler_run(sched);
    CHECK(atomic_load(&completed) == 6000);
    spawn_counted(proc, &completed, 1, 0);
    fr_scheduler_run(sched);
    CHECK(atomic_load(&completed) == 6002);
    fr_scheduler_destroy(sched);
}

static fr_coro_status_t failed_await(fr_coro_t *coro, void *arg) {
    int fd = *(int *)arg;
    CHECK(fr_await_fd(coro, fd, FR_EVENT_READ) == -1);
    pause_ms(2);
    return FR_CORO_ERROR;
}

static void test_await_registration_failure(void) {
    int fds[2];
    CHECK(pipe(fds) == 0);
    fr_scheduler_t *sched = fr_scheduler_create(2);
    fr_process_t *proc = fr_process_create("failed-await");
    CHECK(sched && proc);
    fr_scheduler_add_process(sched, proc);
    close(fds[0]);
    int *state = malloc(sizeof(*state));
    CHECK(state);
    *state = fds[0];
    CHECK(fr_coro_spawn(proc, failed_await, state, sizeof(*state)));
    fr_scheduler_run(sched);
    fr_scheduler_run(sched);
    fr_scheduler_destroy(sched);
    close(fds[1]);
}

int main(void) {
    test_await(0);
    test_await(1);
    test_native_completion();
    test_idle_wakeup();
    test_stop_waiting();
    test_receive_wakeup();
    test_external_stop();
    test_attach_spawn_and_restart();
#if defined(__linux__) || defined(__APPLE__)
    test_await_registration_failure();
#endif
    puts("scheduler tests passed");
    return 0;
}
