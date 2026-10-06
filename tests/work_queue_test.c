#include "work_queue.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>

#define CHECK(c) do { if (!(c)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #c); exit(1); } } while (0)

enum { ITEM_COUNT = 20000, THREAD_COUNT = 4 };

typedef struct {
    size_t id;
} item_t;

typedef struct {
    fr_run_queue_t *queue;
    fr_run_queue_t *thief;
    atomic_uint *seen;
    atomic_size_t *consumed;
    unsigned worker_id;
} run_worker_t;

typedef struct {
    fr_native_queue_t *queue;
    atomic_uint *seen;
    atomic_size_t *consumed;
    unsigned worker_id;
} native_worker_t;

static void native_task(void *arg) {
    (void)arg;
}

static void check_run_links(const fr_run_queue_t *q) {
    CHECK((q->head == NULL) == (q->tail == NULL));
    if (q->head) CHECK(q->head->prev == NULL);
    if (q->tail) CHECK(q->tail->next == NULL);
}

static void check_native_links(const fr_native_queue_t *q) {
    CHECK((q->head == NULL) == (q->tail == NULL));
    if (q->head) CHECK(q->head->prev == NULL);
    if (q->tail) CHECK(q->tail->next == NULL);
}

static void test_run_operations(void) {
    fr_run_queue_t q;
    fr_run_queue_t thief;
    item_t items[4] = {{0}, {1}, {2}, {3}};
    fr_run_queue_init(&q);
    fr_run_queue_init(&thief);

    CHECK(fr_run_queue_pop(&q) == NULL);
    CHECK(fr_run_queue_steal(&q, &thief) == NULL);
    fr_run_queue_push(&q, (fr_coro_t *)&items[0]);
    CHECK(q.count == 1);
    CHECK(fr_run_queue_steal(&q, &thief) == (fr_coro_t *)&items[0]);
    CHECK(q.count == 0);
    check_run_links(&q);

    for (size_t i = 0; i < 4; i++) fr_run_queue_push(&q, (fr_coro_t *)&items[i]);
    CHECK(q.head->next->prev == q.head);
    CHECK(fr_run_queue_pop(&q) == (fr_coro_t *)&items[0]);
    CHECK(fr_run_queue_steal(&q, &thief) == (fr_coro_t *)&items[3]);
    CHECK(fr_run_queue_pop(&q) == (fr_coro_t *)&items[1]);
    CHECK(fr_run_queue_steal(&q, &thief) == (fr_coro_t *)&items[2]);
    CHECK(q.count == 0);
    check_run_links(&q);

    fr_run_queue_destroy(&thief);
    fr_run_queue_destroy(&q);
}

static void test_native_operations(void) {
    fr_native_queue_t q;
    item_t items[4] = {{0}, {1}, {2}, {3}};
    void *arg = NULL;
    fr_native_queue_init(&q);

    CHECK(fr_native_queue_pop(&q, &arg) == NULL);
    CHECK(fr_native_queue_steal(&q, &arg) == NULL);
    fr_native_queue_push(&q, native_task, &items[0]);
    CHECK(fr_native_queue_steal(&q, &arg) == native_task);
    CHECK(arg == &items[0]);
    CHECK(q.count == 0);
    check_native_links(&q);

    for (size_t i = 0; i < 4; i++) fr_native_queue_push(&q, native_task, &items[i]);
    CHECK(q.head->next->prev == q.head);
    CHECK(fr_native_queue_pop(&q, &arg) == native_task && arg == &items[0]);
    CHECK(fr_native_queue_steal(&q, &arg) == native_task && arg == &items[3]);
    CHECK(fr_native_queue_pop(&q, &arg) == native_task && arg == &items[1]);
    CHECK(fr_native_queue_steal(&q, &arg) == native_task && arg == &items[2]);
    CHECK(q.count == 0);
    check_native_links(&q);

    fr_native_queue_destroy(&q);
}

static void *consume_run(void *arg) {
    run_worker_t *worker = (run_worker_t *)arg;
    for (;;) {
        fr_coro_t *coro = (worker->worker_id & 1)
            ? fr_run_queue_steal(worker->queue, worker->thief)
            : fr_run_queue_pop(worker->queue);
        if (coro) {
            item_t *item = (item_t *)coro;
            atomic_fetch_add_explicit(&worker->seen[item->id], 1, memory_order_relaxed);
            atomic_fetch_add_explicit(worker->consumed, 1, memory_order_release);
        } else {
            return NULL;
        }
    }
}

static void *consume_native(void *arg) {
    native_worker_t *worker = (native_worker_t *)arg;
    for (;;) {
        void *task_arg = NULL;
        fr_native_fn fn = (worker->worker_id & 1)
            ? fr_native_queue_steal(worker->queue, &task_arg)
            : fr_native_queue_pop(worker->queue, &task_arg);
        if (fn) {
            item_t *item = (item_t *)task_arg;
            CHECK(fn == native_task);
            atomic_fetch_add_explicit(&worker->seen[item->id], 1, memory_order_relaxed);
            atomic_fetch_add_explicit(worker->consumed, 1, memory_order_release);
        } else {
            return NULL;
        }
    }
}

static void test_concurrent_run(void) {
    fr_run_queue_t q;
    fr_run_queue_t thief;
    item_t *items = (item_t *)calloc(ITEM_COUNT, sizeof(*items));
    atomic_uint *seen = (atomic_uint *)calloc(ITEM_COUNT, sizeof(*seen));
    atomic_size_t consumed = 0;
    fr_thread_t *threads[THREAD_COUNT];
    run_worker_t workers[THREAD_COUNT];
    CHECK(items && seen);
    fr_run_queue_init(&q);
    fr_run_queue_init(&thief);
    for (size_t i = 0; i < ITEM_COUNT; i++) {
        items[i].id = i;
        atomic_init(&seen[i], 0);
        fr_run_queue_push(&q, (fr_coro_t *)&items[i]);
    }
    for (unsigned i = 0; i < THREAD_COUNT; i++) {
        workers[i] = (run_worker_t){&q, &thief, seen, &consumed, i};
        CHECK(fr_thread_start(&threads[i], consume_run, &workers[i]) == 0);
    }
    for (size_t i = 0; i < THREAD_COUNT; i++) CHECK(fr_thread_join(threads[i]) == 0);
    CHECK(atomic_load(&consumed) == ITEM_COUNT);
    for (size_t i = 0; i < ITEM_COUNT; i++) CHECK(atomic_load(&seen[i]) == 1);
    CHECK(q.count == 0);
    check_run_links(&q);
    fr_run_queue_destroy(&thief);
    fr_run_queue_destroy(&q);
    free(seen);
    free(items);
}

static void test_concurrent_native(void) {
    fr_native_queue_t q;
    item_t *items = (item_t *)calloc(ITEM_COUNT, sizeof(*items));
    atomic_uint *seen = (atomic_uint *)calloc(ITEM_COUNT, sizeof(*seen));
    atomic_size_t consumed = 0;
    fr_thread_t *threads[THREAD_COUNT];
    native_worker_t workers[THREAD_COUNT];
    CHECK(items && seen);
    fr_native_queue_init(&q);
    for (size_t i = 0; i < ITEM_COUNT; i++) {
        items[i].id = i;
        atomic_init(&seen[i], 0);
        fr_native_queue_push(&q, native_task, &items[i]);
    }
    for (unsigned i = 0; i < THREAD_COUNT; i++) {
        workers[i] = (native_worker_t){&q, seen, &consumed, i};
        CHECK(fr_thread_start(&threads[i], consume_native, &workers[i]) == 0);
    }
    for (size_t i = 0; i < THREAD_COUNT; i++) CHECK(fr_thread_join(threads[i]) == 0);
    CHECK(atomic_load(&consumed) == ITEM_COUNT);
    for (size_t i = 0; i < ITEM_COUNT; i++) CHECK(atomic_load(&seen[i]) == 1);
    CHECK(q.count == 0);
    check_native_links(&q);
    fr_native_queue_destroy(&q);
    free(seen);
    free(items);
}

/* Drain a burst larger than the retained cache, then reuse nodes repeatedly.
 * Validate both the retention bound and FIFO/LIFO behavior after recycling. */
static void test_recycling(void) {
    fr_run_queue_t run;
    fr_native_queue_t native;
    item_t items[1000];
    fr_run_queue_init(&run);
    fr_native_queue_init(&native);
    for (size_t i = 0; i < 1000; i++) {
        items[i].id = i;
        CHECK(fr_run_queue_try_push(&run, (fr_coro_t *)&items[i]));
        CHECK(fr_native_queue_try_push(&native, native_task, &items[i]));
    }
    for (size_t i = 0; i < 1000; i++) {
        void *arg;
        CHECK(fr_run_queue_pop(&run) == (fr_coro_t *)&items[i]);
        CHECK(fr_native_queue_pop(&native, &arg) == native_task);
        CHECK(arg == &items[i]);
    }
    CHECK(run.free_count == 256 && native.free_count == 256);
    fr_run_node_t *run_cached = run.free_nodes;
    fr_native_node_t *native_cached = native.free_nodes;
    for (size_t i = 0; i < 20000; i++) {
        void *arg;
        fr_run_queue_push(&run, (fr_coro_t *)&items[0]);
        fr_native_queue_push(&native, native_task, &items[0]);
        CHECK(run.head == run_cached && native.head == native_cached);
        CHECK(fr_run_queue_steal(&run, NULL) == (fr_coro_t *)&items[0]);
        CHECK(fr_native_queue_steal(&native, &arg) == native_task && arg == &items[0]);
        CHECK(run.free_count == 256 && native.free_count == 256);
        check_run_links(&run);
        check_native_links(&native);
    }
    fr_run_queue_destroy(&run);
    fr_native_queue_destroy(&native);
}

typedef struct {
    fr_run_queue_t *queue;
    item_t *items;
    atomic_uint *seen;
    atomic_int *producers;
    unsigned id;
} mixed_worker_t;
static void *produce_mixed(void *arg) {
    mixed_worker_t *w = arg;
    for (size_t i = w->id; i < ITEM_COUNT; i += THREAD_COUNT)
        CHECK(fr_run_queue_try_push(w->queue, (fr_coro_t *)&w->items[i]));
    atomic_fetch_sub(w->producers, 1);
    return NULL;
}
static void *consume_mixed(void *arg) {
    mixed_worker_t *w = arg;
    for (;;) {
        fr_coro_t *c = w->id & 1 ? fr_run_queue_steal(w->queue, NULL) : fr_run_queue_pop(w->queue);
        if (c) {
            item_t *item = (item_t *)c;
            atomic_fetch_add(&w->seen[item->id], 1);
        } else if (atomic_load(w->producers) == 0) {
            /* A producer may have published between the first empty pop and
             * its completion. Recheck after observing all producers done. */
            c = fr_run_queue_pop(w->queue);
            if (!c) break;
            item_t *item = (item_t *)c;
            atomic_fetch_add(&w->seen[item->id], 1);
        } else fr_thread_yield();
    }
    return NULL;
}
static void test_concurrent_recycling(void) {
    fr_run_queue_t q;
    item_t *items = calloc(ITEM_COUNT, sizeof(*items));
    atomic_uint *seen = calloc(ITEM_COUNT, sizeof(*seen));
    atomic_int producers = THREAD_COUNT;
    fr_thread_t *producer_threads[THREAD_COUNT], *consumer_threads[THREAD_COUNT];
    mixed_worker_t workers[THREAD_COUNT];
    CHECK(items && seen);
    fr_run_queue_init(&q);
    for (size_t i = 0; i < ITEM_COUNT; i++) { items[i].id = i; atomic_init(&seen[i], 0); }
    for (unsigned i = 0; i < THREAD_COUNT; i++) {
        workers[i] = (mixed_worker_t){&q, items, seen, &producers, i};
        CHECK(fr_thread_start(&consumer_threads[i], consume_mixed, &workers[i]) == 0);
        CHECK(fr_thread_start(&producer_threads[i], produce_mixed, &workers[i]) == 0);
    }
    for (int i = 0; i < THREAD_COUNT; i++) CHECK(fr_thread_join(producer_threads[i]) == 0);
    for (int i = 0; i < THREAD_COUNT; i++) CHECK(fr_thread_join(consumer_threads[i]) == 0);
    for (size_t i = 0; i < ITEM_COUNT; i++) CHECK(atomic_load(&seen[i]) == 1);
    CHECK(q.count == 0 && q.free_count <= 256);
    fr_run_queue_destroy(&q);
    free(items); free(seen);
}

int main(void) {
    test_run_operations();
    test_native_operations();
    test_concurrent_run();
    test_concurrent_native();
    test_recycling();
    test_concurrent_recycling();
    puts("work queue tests passed");
    return 0;
}
