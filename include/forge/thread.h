#ifndef FORGE_THREAD_H
#define FORGE_THREAD_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct fr_mutex fr_mutex_t;
typedef struct fr_cond fr_cond_t;
typedef struct fr_thread fr_thread_t;

/* Create functions return NULL if allocation or native initialization fails. */
fr_mutex_t *fr_mutex_create(void);
void fr_mutex_destroy(fr_mutex_t *m);
void fr_mutex_lock(fr_mutex_t *m);
void fr_mutex_unlock(fr_mutex_t *m);

fr_cond_t *fr_cond_create(void);
void fr_cond_destroy(fr_cond_t *c);
void fr_cond_signal(fr_cond_t *c);
void fr_cond_broadcast(fr_cond_t *c);
void fr_cond_wait(fr_cond_t *c, fr_mutex_t *m);

typedef void *(*fr_thread_fn)(void *arg);
int fr_thread_start(fr_thread_t **out, fr_thread_fn fn, void *arg);
void fr_thread_detach(fr_thread_t *t);
int fr_thread_join(fr_thread_t *t);
void fr_thread_yield(void);
/* Best-effort explicit pin of the calling thread to an absolute Linux CPU ID.
 * Invalid IDs (<0 or >=CPU_SETSIZE), OS rejection and unsupported targets leave
 * affinity unchanged. This can change an inherited taskset mask; scheduler
 * workers themselves keep the creator's mask. Query the OS to confirm success. */
void fr_thread_pin_cpu(int cpu);

#ifdef __cplusplus
}
#endif

#endif
