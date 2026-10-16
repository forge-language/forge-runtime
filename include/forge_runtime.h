#ifndef FORGE_RUNTIME_H
#define FORGE_RUNTIME_H

#include "forge/arena.h"
#include <stddef.h>
#include <stdint.h>

typedef struct fr_msg fr_msg_t;
typedef struct fr_process fr_process_t;
typedef struct fr_coro fr_coro_t;
typedef struct fr_scheduler fr_scheduler_t;
typedef struct fr_event_loop fr_event_loop_t;

typedef enum {
    FR_CORO_RUNNING = 0,
    FR_CORO_YIELDED,
    FR_CORO_WAITING_RECV,
    FR_CORO_WAITING_IO,
    FR_CORO_DONE,
    FR_CORO_ERROR
} fr_coro_status_t;

typedef enum {
    FR_RESTART_CORO = 0,
    FR_RESTART_PROCESS,
    FR_RESTART_ALL
} fr_restart_policy_t;

typedef fr_coro_status_t (*fr_coro_fn)(fr_coro_t *coro, void *state);

struct fr_msg {
    int tag;
    int64_t value;
    void *payload;
    size_t payload_size;
    int owns_payload;
    fr_process_t *from;
};

/* worker_count: 0 = auto (Linux caller-affinity CPU count, online fallback),
 * 1 = single-threaded cooperative. Positive counts remain explicit; workers
 * inherit the starting thread's affinity rather than imposing CPU placement. */
/* Construction returns NULL without publishing a partially initialized owner
 * if any required allocation, native primitive or event loop fails. */
fr_scheduler_t *fr_scheduler_create(int worker_count);
/* External-owner lifecycle operations. Keep the scheduler alive through all
 * start/stop/run calls; callbacks must not wait on their own pool's lifecycle.
 * Before destroy, quiesce publication and other lifecycle callers. Destroy
 * discards unclaimed internal spawn wrappers without running user functions;
 * already-published indexed waiters are released when those jobs are discarded.
 * Direct-submit arguments remain caller-owned if their callback never ran. */
void fr_scheduler_destroy(fr_scheduler_t *sched);
void fr_scheduler_add_process(fr_scheduler_t *sched, fr_process_t *proc);
void fr_scheduler_start(fr_scheduler_t *sched);
void fr_scheduler_stop(fr_scheduler_t *sched);
void fr_scheduler_run(fr_scheduler_t *sched);
fr_event_loop_t *fr_scheduler_event_loop(fr_scheduler_t *sched);
int fr_scheduler_worker_count(fr_scheduler_t *sched);
fr_scheduler_t *fr_scheduler_global(void);
void fr_scheduler_set_global(fr_scheduler_t *sched);

/* Native OS-thread work on the scheduler worker pool (hybrid mode). */
typedef int64_t (*fr_sched_native_fn1_t)(int64_t);
typedef int64_t (*fr_sched_native_fn2_t)(int64_t, int64_t);
int64_t fr_sched_pool_spawn(fr_sched_native_fn1_t fn, int64_t arg);
void fr_sched_pool_spawn_indexed(fr_sched_native_fn2_t fn, int64_t count);
int fr_sched_pool_available(void);
int fr_sched_pool_submit(fr_scheduler_t *sched, void (*fn)(void *), void *arg);
/* Exact queued-only snapshot at the scheduler lock; excludes running callbacks.
 * Multi-worker accounting initializes on the first query with one queue scan.
 * A depth check followed by submit does not reserve capacity across callers. */
size_t fr_sched_pool_queued(fr_scheduler_t *sched);

/* NULL on incomplete construction; a successful process owns its name, mutex
 * and condition variable. Callers must check before attaching or spawning. */
fr_process_t *fr_process_create(const char *name);
void fr_process_destroy(fr_process_t *proc);
fr_scheduler_t *fr_process_scheduler(fr_process_t *proc);
const char *fr_process_name(fr_process_t *proc);
void fr_process_set_receive_handler(fr_process_t *proc, fr_coro_fn handler, size_t state_size);

/* A non-NULL return consumes heap-allocated init_state: positive-sized state
 * is retained until process destruction, and zero-sized input is freed
 * immediately (NULL is allowed). A NULL return leaves ownership with the caller.
 * A non-NULL coroutine may already have a terminal queue-publication error;
 * ownership transfer does not guarantee execution. */
fr_coro_t *fr_coro_spawn(fr_process_t *proc, fr_coro_fn fn, void *init_state, size_t state_size);
fr_coro_status_t fr_yield(fr_coro_t *coro);
/* Generated coroutine code may continue through a logical yield in the same
 * worker invocation while its reduction budget remains. Returns nonzero when
 * the coroutine must return to the scheduler. Outside an active worker call,
 * it always requests a return so direct callers retain ordinary yield behavior. */
int fr_coro_inline_yield(fr_coro_t *coro, int next_step);
int fr_coro_id(fr_coro_t *coro);
fr_process_t *fr_coro_process(fr_coro_t *coro);
int fr_coro_step(fr_coro_t *coro);
void fr_coro_set_step(fr_coro_t *coro, int step);
fr_coro_t *fr_coro_current(void);

/* Per-coroutine scratch arena, lazily created on first use and freed when
 * the owning process is destroyed. Unlike fr_arena_tls(), this stays bound
 * to the coroutine across work-stealing moves between worker threads, so a
 * pointer allocated before a yield remains valid (and un-aliased by an
 * unrelated coroutine's fr_str_arena_reset()) after the coroutine resumes
 * on a different OS thread. */
fr_arena_t *fr_coro_get_arena(fr_coro_t *coro);

/* Returns 1 if enqueued, 0 if the mailbox is full or the destination is NULL.
 * A heap payload is consumed in either case: the receiver owns an accepted
 * payload; a rejected payload is freed here. The caller must not reuse the
 * payload after sending, including when delivery fails. */
int fr_send(fr_process_t *dst, int tag, int64_t value, void *payload, size_t payload_size);
int fr_try_recv(fr_process_t *self, fr_msg_t *out);
int fr_recv(fr_process_t *self, fr_msg_t *out);
void fr_msg_free_payload(fr_msg_t *msg);

/* await I/O in coroutine — returns 0 until ready, then 1 */
int64_t fr_await_fd(fr_coro_t *coro, int64_t fd, uint32_t events);

fr_process_t *fr_supervisor_create(const char *name, fr_restart_policy_t policy);
/* Best-effort borrowed child registration. Growth failure leaves the previous
 * list unchanged; child ownership is not transferred. Configuration requires
 * caller-exclusive access. Not automatic recovery. */
void fr_supervisor_add_child(fr_process_t *supervisor, fr_process_t *child);

int fr_event_poll(fr_scheduler_t *sched, int timeout_ms);
int64_t fr_event_add_read(fr_scheduler_t *sched, int64_t fd);

#endif
