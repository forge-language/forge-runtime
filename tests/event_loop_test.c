#include "forge/event.h"

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/eventfd.h>
#include <unistd.h>

static void *seen_userdata;
static uint32_t seen_events;
static int callback_count;
static uint8_t *burst_seen;
static uint32_t burst_count;
static uint32_t burst_observed;

static void record_event(fr_event_loop_t *loop, int fd, uint32_t events, void *userdata) {
    (void)loop;
    (void)fd;
    seen_userdata = userdata;
    seen_events = events;
    callback_count++;
}

static void record_burst_event(fr_event_loop_t *loop, int fd, uint32_t events, void *userdata) {
    (void)loop;
    (void)fd;
    (void)events;
    uintptr_t index = (uintptr_t)userdata;
    assert(index < burst_count);
    if (!burst_seen[index]) {
        burst_seen[index] = 1;
        burst_observed++;
    }
}

static void signal_eventfd(int fd) {
    uint64_t value = 1;
    assert(write(fd, &value, sizeof(value)) == (ssize_t)sizeof(value));
}

static void drain_eventfd(int fd) {
    uint64_t value;
    assert(read(fd, &value, sizeof(value)) == (ssize_t)sizeof(value));
}

int main(void) {
    fr_event_loop_t *loop = fr_event_loop_create();
    assert(loop);
    int fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    assert(fd >= 0);
    int initial = 1, updated = 2, readded = 3;
    fr_event_loop_set_cb(loop, record_event);

    assert(fr_event_loop_add(loop, fd, FR_EVENT_READ, &initial) == 0);
    signal_eventfd(fd);
    assert(fr_event_loop_mod(loop, fd, FR_EVENT_READ, &updated) == 0);
    assert(fr_event_loop_poll(loop, 1000) == 1);
    assert(callback_count == 1 && seen_userdata == &updated);
    assert(seen_events & FR_EVENT_READ);
    drain_eventfd(fd);

    /* Preserve the API's add-if-missing behavior after explicit deletion. */
    assert(fr_event_loop_del(loop, fd) == 0);
    assert(fr_event_loop_mod(loop, fd, FR_EVENT_READ, &readded) == 0);
    signal_eventfd(fd);
    assert(fr_event_loop_poll(loop, 1000) == 1);
    assert(callback_count == 2 && seen_userdata == &readded);
    assert(seen_events & FR_EVENT_READ);
    drain_eventfd(fd);

    assert(fr_event_loop_mod(loop, -1, FR_EVENT_READ, NULL) == -1);
    fr_event_loop_destroy(loop);
    close(fd);

    enum { READY_FDS = 300 };
    fr_event_loop_t *burst_loop = fr_event_loop_create();
    assert(burst_loop);
    int burst_fds[READY_FDS];
    burst_seen = calloc(READY_FDS, sizeof(*burst_seen));
    assert(burst_seen);
    burst_count = READY_FDS;
    fr_event_loop_set_cb(burst_loop, record_burst_event);
    for (uintptr_t i = 0; i < READY_FDS; i++) {
        burst_fds[i] = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        assert(burst_fds[i] >= 0);
        assert(fr_event_loop_add(burst_loop, burst_fds[i], FR_EVENT_READ,
                                 (void *)i) == 0);
    }
    for (int i = 0; i < READY_FDS; i++) signal_eventfd(burst_fds[i]);
    int polls = 0;
    while (burst_observed < READY_FDS) {
        assert(fr_event_loop_poll(burst_loop, 0) > 0);
        assert(++polls <= 8);
    }
    assert(burst_observed == READY_FDS);
    fr_event_loop_destroy(burst_loop);
    for (int i = 0; i < READY_FDS; i++) close(burst_fds[i]);
    free(burst_seen);
    return 0;
}
