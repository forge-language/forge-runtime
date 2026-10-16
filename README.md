# Forge runtime

Independent native C library extracted from [Forge](https://github.com/forge-language/forge)
commit `725aefb`. Apache-2.0; bundled third-party files keep their original notices.

```sh
cmake -S . -B build
cmake --build build -j2
ctest --test-dir build --output-on-failure
cmake --install build --prefix "$PWD/build/install"
```

Consume an installed package with `find_package(ForgeRuntime CONFIG REQUIRED)` and
link `ForgeRuntime::runtime`. Headers install under `include/` and archives under `lib/`.

The runtime owns arenas, ownership, scheduler, work queues, event loops and platform
primitives. It has no dependency on the standard library. `src/work_queue.h` is
an internal header and is deliberately excluded from the public SDK.

On Linux, an automatic scheduler (`worker_count == 0`) sizes its pool from the
creating thread's current CPU affinity. Workers inherit the starting thread's
mask, including sparse CPU IDs; the runtime imposes no implicit per-worker pin.
Explicit positive worker counts still permit oversubscription. Creation and
start may occur under different masks, and later mask/hotplug changes do not
resize an existing pool. CPU affinity is not a bandwidth quota or an exclusive
CPU reservation.

`fr_platform_cpu_count()` retains its online-CPU meaning.
`fr_platform_available_cpu_count()` is the separate sizing snapshot. Linux
probes larger masks on EINVAL, bounded at 1 MiB; allocation, probe, empty-mask or
size-limit failure falls back to the online count and may overprovision. Other
platforms retain their existing online-count fallback. The explicit
`fr_thread_pin_cpu()` helper now works on Linux for absolute IDs below
`CPU_SETSIZE`; invalid inputs, OS rejection and unsupported targets are no-ops.
It may replace an inherited taskset mask and returns no success status. Query
the OS when placement must be verified.
