#ifndef FORGE_ARENA_H
#define FORGE_ARENA_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct fr_arena fr_arena_t;

fr_arena_t *fr_arena_create(size_t initial_cap);
void fr_arena_destroy(fr_arena_t *a);
/* Allocations remain valid until reset/destroy, including across growth.
 * align must be a power of two; zero selects pointer alignment. */
void *fr_arena_alloc(fr_arena_t *a, size_t size, size_t align);
char *fr_arena_strdup(fr_arena_t *a, const char *s);
void fr_arena_reset(fr_arena_t *a);

fr_arena_t *fr_arena_tls(void);
void fr_arena_tls_reset(void);

/* Lets a higher layer (the coroutine scheduler) supply a per-coroutine
 * arena to back fr_arena_tls()/fr_arena_tls_reset() instead of the plain
 * OS-thread-local one, without arena.c linking against the scheduler.
 * Registered once by the runtime; NULL (the default) means "no coroutine
 * context available", falling back to the thread-local arena. */
typedef fr_arena_t *(*fr_arena_coro_provider_fn)(void);
void fr_arena_set_coro_provider(fr_arena_coro_provider_fn fn);

#ifdef __cplusplus
}
#endif

#endif
