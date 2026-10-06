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
