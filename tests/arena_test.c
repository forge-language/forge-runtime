#include "forge/arena.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(c) do { if (!(c)) { fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #c); exit(1); } } while (0)

int main(void) {
    fr_arena_t *a = fr_arena_create(4096);
    CHECK(a != NULL);
    char *first = fr_arena_strdup(a, "stable across growth");
    CHECK(first != NULL);
    unsigned char *blocks[12];
    for (size_t i = 0; i < 12; i++) {
        blocks[i] = fr_arena_alloc(a, 8192 + i * 17, 64);
        CHECK(blocks[i] != NULL);
        CHECK((uintptr_t)blocks[i] % 64 == 0);
        memset(blocks[i], (int)i + 1, 8192 + i * 17);
        CHECK(strcmp(first, "stable across growth") == 0);
    }
    for (size_t i = 0; i < 12; i++)
        for (size_t j = 0; j < 8192 + i * 17; j++) CHECK(blocks[i][j] == i + 1);
    CHECK(fr_arena_alloc(a, SIZE_MAX, 8) == NULL);
    CHECK(fr_arena_alloc(a, 1, 3) == NULL);
    CHECK(fr_arena_alloc(a, 0, 8) == NULL);
    CHECK(fr_arena_alloc(NULL, 1, 8) == NULL);
    CHECK(fr_arena_create(SIZE_MAX) == NULL);
    fr_arena_reset(a);
    CHECK(fr_arena_strdup(a, "stable across growth") == first);
    for (size_t i = 0; i < 12; i++) CHECK(fr_arena_alloc(a, 8192 + i * 17, 64) == blocks[i]);
    for (size_t align = 1; align <= 4096; align *= 2) {
        void *p = fr_arena_alloc(a, 13, align);
        CHECK(p != NULL);
        CHECK((uintptr_t)p % align == 0);
    }
    fr_arena_destroy(a);
    puts("arena regression tests passed");
    return 0;
}
