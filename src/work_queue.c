#include "work_queue_core.h"

void fr_run_queue_init(fr_run_queue_t *q) {
    q->lock = fr_mutex_create();
    q->head = q->tail = q->free_nodes = NULL;
    q->count = q->free_count = 0;
}

void fr_run_queue_destroy(fr_run_queue_t *q) {
    if (!q) return;
    fr_run_node_t *n = q->head;
    while (n) { fr_run_node_t *next = n->next; free(n); n = next; }
    n = q->free_nodes;
    while (n) { fr_run_node_t *next = n->next; free(n); n = next; }
    q->head = q->tail = q->free_nodes = NULL;
    q->count = q->free_count = 0;
    fr_mutex_destroy(q->lock);
    q->lock = NULL;
}

int fr_run_queue_try_push(fr_run_queue_t *q, fr_coro_t *coro) {
    return fr_queue_core_push_run(q, coro, 1);
}

void fr_run_queue_push(fr_run_queue_t *q, fr_coro_t *coro) {
    (void)fr_run_queue_try_push(q, coro);
}

static fr_coro_t *take_run(fr_run_queue_t *q, int steal) {
    return fr_queue_core_take_run(q, steal, 1);
}

fr_coro_t *fr_run_queue_pop(fr_run_queue_t *q) { return take_run(q, 0); }
fr_coro_t *fr_run_queue_steal(fr_run_queue_t *victim, fr_run_queue_t *thief) {
    (void)thief;
    return take_run(victim, 1);
}

void fr_native_queue_init(fr_native_queue_t *q) {
    q->lock = fr_mutex_create();
    q->head = q->tail = q->free_nodes = NULL;
    q->count = q->free_count = 0;
}

void fr_native_queue_destroy_with_cleanup(fr_native_queue_t *q,
                                         void (*discard)(fr_native_fn, void *)) {
    if (!q) return;
    fr_native_node_t *n = q->head;
    while (n) {
        fr_native_node_t *next = n->next;
        if (discard) discard(n->fn, n->arg);
        free(n);
        n = next;
    }
    n = q->free_nodes;
    while (n) { fr_native_node_t *next = n->next; free(n); n = next; }
    q->head = q->tail = q->free_nodes = NULL;
    q->count = q->free_count = 0;
    fr_mutex_destroy(q->lock);
    q->lock = NULL;
}

void fr_native_queue_destroy(fr_native_queue_t *q) {
    fr_native_queue_destroy_with_cleanup(q, NULL);
}

int fr_native_queue_try_push(fr_native_queue_t *q, fr_native_fn fn, void *arg) {
    return fr_queue_core_push_native(q, fn, arg, 1);
}

int fr_native_queue_push(fr_native_queue_t *q, fr_native_fn fn, void *arg) {
    return fr_native_queue_try_push(q, fn, arg) ? 0 : -1;
}

size_t fr_native_queue_count(fr_native_queue_t *q) {
    if (!q || !q->lock) return 0;
    fr_mutex_lock(q->lock);
    size_t count = q->count;
    fr_mutex_unlock(q->lock);
    return count;
}

static fr_native_fn take_native(fr_native_queue_t *q, void **arg_out, int steal) {
    return fr_queue_core_take_native(q, arg_out, steal, 1);
}

fr_native_fn fr_native_queue_pop(fr_native_queue_t *q, void **arg_out) {
    return take_native(q, arg_out, 0);
}
fr_native_fn fr_native_queue_steal(fr_native_queue_t *q, void **arg_out) {
    return take_native(q, arg_out, 1);
}
