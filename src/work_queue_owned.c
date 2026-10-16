#include "work_queue_core.h"

int fr_run_queue_try_push_owned(fr_run_queue_t *q, fr_coro_t *coro) {
    return fr_queue_core_push_run(q, coro, 0);
}
fr_coro_t *fr_run_queue_pop_owned(fr_run_queue_t *q) {
    return fr_queue_core_take_run(q, 0, 0);
}
fr_coro_t *fr_run_queue_steal_owned(fr_run_queue_t *q) {
    return fr_queue_core_take_run(q, 1, 0);
}
int fr_native_queue_try_push_owned(fr_native_queue_t *q, fr_native_fn fn, void *arg) {
    return fr_queue_core_push_native(q, fn, arg, 0);
}
fr_native_fn fr_native_queue_pop_owned(fr_native_queue_t *q, void **arg_out) {
    return fr_queue_core_take_native(q, arg_out, 0, 0);
}
fr_native_fn fr_native_queue_steal_owned(fr_native_queue_t *q, void **arg_out) {
    return fr_queue_core_take_native(q, arg_out, 1, 0);
}
