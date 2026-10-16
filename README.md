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

Native pool depth is an exact queued-only observation under the scheduler lock;
claimed callbacks remain in the separate pending-work completion count. A first
multi-worker depth query initializes an execution ledger from the protected queue
inventory, including already claimed work. Later queries use that ledger. Pools
that never query depth keep tracking disabled, and single-worker pools count
their one queue directly. Stop/restart retains queued work and the ledger.
A depth check followed by submission does not reserve capacity across callers.

Scheduler/process factories return NULL if a required allocation, native mutex
or condition initialization, or event-loop creation fails. Partial construction
is cleaned before return; failed scheduler creation preserves the existing arena
provider and global scheduler. A successful scheduler has valid private queue
locks. Callers must check factory results before use. Supervisor child registration
borrows children and requires exclusive configuration; failed list growth leaves
the old pointer and count intact. Its void API cannot report that failure and does
not provide automatic recovery.

The scheduler inspects its private queue heads/counts while holding the owner
mutex and skips empty pop/steal operations. All publication and claim paths share
that mutex; owned queue operations omit the redundant private queue lock.
Standalone queues retain their own locking API. A shared inline core takes a
literal lock policy, with owned entry points in a separate archive object.
Queue mutexes are still constructed, checked and destroyed. Selection order,
callback boundaries and reduction budgets are unchanged. A live
parked coroutine does not imply queued coroutine work. Debug builds independently
check queue inventory against runnable/live counts; Release adds no readiness
counter, scheduler field or per-job allocation.

Owned queue APIs are private, non-installed interfaces. Every access, including
count/cache reads, requires the same external owner guard or exclusive quiescent
ownership; mixing in standalone access outside that guard is invalid. Measured
dispatch gains are workload-specific, with adverse p99 latency in some blocked
multi-worker samples; see the sibling forge-benchmarks report
`docs/compiler/queue-owner-2026-10-16.md` for both paired batches and limits.

If coroutine queue publication fails and retires the last active task, completion
waiters are signaled under the same mutex even if no worker received the task.
Mailbox acceptance still denotes transfer to the mailbox, not successful callback
execution; pending mailbox payloads retain their existing owner and cleanup rules.

External start/stop calls serialize on the scheduler lock. A start waits for an
ongoing stop, and stop clears the old global registration before allowing a new
start to publish it again. These blocking lifecycle operations belong to an
external owner; callbacks must not wait for their own workers to stop.

Before terminal destruction, quiesce publication and other lifecycle callers.
Destruction joins workers, then discards pending runtime-owned spawn wrappers
without running user functions. Indexed batches whose publication has finished
are released after their remaining jobs complete or are discarded. Direct-submit
arguments and resources referenced by opaque user handles remain caller-owned.
Cached queue nodes have already transferred their arguments and are never
discarded a second time. This is not automatic lifetime checking for raw pointers
or a general cancellation-result API.
