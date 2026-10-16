#ifndef FORGE_WORK_QUEUE_CORE_H
#define FORGE_WORK_QUEUE_CORE_H

#include "work_queue.h"
#include <stdlib.h>

/* Shared structural core. Callers pass a literal lock policy: standalone 1,
 * externally serialized 0. Separate objects keep unused owner entry points
 * out of standalone consumers. Overflow nodes are freed after private unlock. */
#define FR_QUEUE_NODE_CACHE_LIMIT 256

static inline int fr_queue_core_push_run(fr_run_queue_t *q, fr_coro_t *coro, int locked) {
    if (locked) fr_mutex_lock(q->lock);
    fr_run_node_t *node = q->free_nodes;
    if (node) { q->free_nodes = node->next; q->free_count--; }
    else node = (fr_run_node_t *)malloc(sizeof(*node));
    if (!node) { if (locked) fr_mutex_unlock(q->lock); return 0; }
    node->coro = coro;
    node->prev = q->tail;
    node->next = NULL;
    if (q->tail) q->tail->next = node;
    else q->head = node;
    q->tail = node;
    q->count++;
    if (locked) fr_mutex_unlock(q->lock);
    return 1;
}

static inline fr_coro_t *fr_queue_core_take_run(fr_run_queue_t *q, int steal, int locked) {
    if (locked) fr_mutex_lock(q->lock);
    fr_run_node_t *node = steal ? q->tail : q->head;
    if (!node) { if (locked) fr_mutex_unlock(q->lock); return NULL; }
    if (node->prev) node->prev->next = node->next;
    else q->head = node->next;
    if (node->next) node->next->prev = node->prev;
    else q->tail = node->prev;
    q->count--;
    fr_coro_t *coro = node->coro;
    if (q->free_count < FR_QUEUE_NODE_CACHE_LIMIT) {
        node->next = q->free_nodes;
        q->free_nodes = node;
        q->free_count++;
        node = NULL;
    }
    if (locked) fr_mutex_unlock(q->lock);
    if (node) free(node);
    return coro;
}

static inline int fr_queue_core_push_native(fr_native_queue_t *q, fr_native_fn fn, void *arg, int locked) {
    if (!fn) return 0;
    if (locked) fr_mutex_lock(q->lock);
    fr_native_node_t *node = q->free_nodes;
    if (node) { q->free_nodes = node->next; q->free_count--; }
    else node = (fr_native_node_t *)malloc(sizeof(*node));
    if (!node) { if (locked) fr_mutex_unlock(q->lock); return 0; }
    node->fn = fn;
    node->arg = arg;
    node->prev = q->tail;
    node->next = NULL;
    if (q->tail) q->tail->next = node;
    else q->head = node;
    q->tail = node;
    q->count++;
    if (locked) fr_mutex_unlock(q->lock);
    return 1;
}

static inline fr_native_fn fr_queue_core_take_native(fr_native_queue_t *q, void **arg_out, int steal, int locked) {
    if (locked) fr_mutex_lock(q->lock);
    fr_native_node_t *node = steal ? q->tail : q->head;
    if (!node) { if (locked) fr_mutex_unlock(q->lock); return NULL; }
    if (node->prev) node->prev->next = node->next;
    else q->head = node->next;
    if (node->next) node->next->prev = node->prev;
    else q->tail = node->prev;
    q->count--;
    if (arg_out) *arg_out = node->arg;
    fr_native_fn fn = node->fn;
    if (q->free_count < FR_QUEUE_NODE_CACHE_LIMIT) {
        node->next = q->free_nodes;
        q->free_nodes = node;
        q->free_count++;
        node = NULL;
    }
    if (locked) fr_mutex_unlock(q->lock);
    if (node) free(node);
    return fn;
}

#endif
