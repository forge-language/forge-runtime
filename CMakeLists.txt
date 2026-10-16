cmake_minimum_required(VERSION 3.16)
project(ForgeRuntime VERSION 0.3.0 LANGUAGES C)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
find_package(Threads REQUIRED)

add_library(forge_runtime STATIC
    src/arena.c src/scheduler.c src/work_queue.c src/event_loop.c
    src/ownership.c src/platform.c src/thread.c src/work_queue_owned.c)
add_library(ForgeRuntime::runtime ALIAS forge_runtime)
set_target_properties(forge_runtime PROPERTIES EXPORT_NAME runtime)
target_include_directories(forge_runtime PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src")
target_link_libraries(forge_runtime PUBLIC Threads::Threads)
if(WIN32)
    target_link_libraries(forge_runtime PUBLIC ws2_32)
else()
    target_compile_definitions(forge_runtime PRIVATE _GNU_SOURCE)
endif()
if(MSVC)
    target_compile_options(forge_runtime PRIVATE /W4)
else()
    target_compile_options(forge_runtime PRIVATE -Wall -Wextra)
endif()
install(TARGETS forge_runtime EXPORT ForgeRuntimeTargets ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT ForgeRuntimeTargets NAMESPACE ForgeRuntime:: DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ForgeRuntime)
configure_package_config_file(cmake/ForgeRuntimeConfig.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/ForgeRuntimeConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ForgeRuntime)
write_basic_package_version_file("${CMAKE_CURRENT_BINARY_DIR}/ForgeRuntimeConfigVersion.cmake"
    VERSION ${PROJECT_VERSION} COMPATIBILITY SameMajorVersion)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/ForgeRuntimeConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/ForgeRuntimeConfigVersion.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/ForgeRuntime)
include(CTest)
if(BUILD_TESTING)
    set(runtime_tests arena work_queue)
    if(WIN32)
        list(APPEND runtime_tests platform_temp)
    endif()
    if(UNIX)
        list(APPEND runtime_tests scheduler)
    endif()
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        list(APPEND runtime_tests event_loop dispatch factory queue_owner)
    endif()
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        add_executable(affinity_test tests/affinity_test.c)
        target_link_libraries(affinity_test PRIVATE ForgeRuntime::runtime)
        target_compile_options(affinity_test PRIVATE -UNDEBUG)
        target_link_options(affinity_test PRIVATE "-Wl,--wrap=malloc" "-Wl,--wrap=free"
            "-Wl,--wrap=sysconf" "-Wl,--wrap=sched_getaffinity"
            "-Wl,--wrap=pthread_setaffinity_np")
        add_test(NAME affinity_regressions COMMAND affinity_test)
        set_tests_properties(affinity_regressions PROPERTIES TIMEOUT 30)
        add_executable(lifecycle_test tests/lifecycle_test.c)
        target_link_libraries(lifecycle_test PRIVATE ForgeRuntime::runtime)
        target_include_directories(lifecycle_test PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src")
        target_compile_options(lifecycle_test PRIVATE -UNDEBUG)
        target_link_options(lifecycle_test PRIVATE "-Wl,--wrap=malloc" "-Wl,--wrap=free"
            "-Wl,--wrap=fr_cond_wait" "-Wl,--wrap=fr_mutex_unlock"
            "-Wl,--wrap=fr_native_queue_try_push_owned")
        add_test(NAME lifecycle_regressions COMMAND lifecycle_test)
        set_tests_properties(lifecycle_regressions PROPERTIES TIMEOUT 30)
        add_executable(native_depth_test tests/native_depth_test.c)
        target_link_libraries(native_depth_test PRIVATE ForgeRuntime::runtime)
        target_compile_options(native_depth_test PRIVATE -UNDEBUG)
        target_link_options(native_depth_test PRIVATE "-Wl,--wrap=malloc" "-Wl,--wrap=fr_thread_start")
        add_test(NAME native_depth_regressions COMMAND native_depth_test)
        set_tests_properties(native_depth_regressions PROPERTIES TIMEOUT 30)
    endif()
    foreach(name IN LISTS runtime_tests)
        add_executable(${name}_test "tests/${name}_test.c")
        target_link_libraries(${name}_test PRIVATE ForgeRuntime::runtime)
        target_include_directories(${name}_test PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src")
        if(name STREQUAL "dispatch")
            target_link_options(${name}_test PRIVATE "-Wl,--wrap=fr_run_queue_try_push_owned"
                "-Wl,--wrap=fr_thread_start" "-Wl,--wrap=fr_cond_wait" "-Wl,--wrap=fr_cond_signal")
        endif()
        if(name STREQUAL "factory")
            target_link_options(${name}_test PRIVATE "-Wl,--wrap=malloc" "-Wl,--wrap=calloc"
                "-Wl,--wrap=realloc" "-Wl,--wrap=free" "-Wl,--wrap=pthread_mutex_init"
                "-Wl,--wrap=pthread_mutex_destroy" "-Wl,--wrap=pthread_cond_init"
                "-Wl,--wrap=pthread_cond_destroy" "-Wl,--wrap=epoll_create1" "-Wl,--wrap=close")
        endif()
        if(name STREQUAL "queue_owner")
            foreach(symbol IN ITEMS fr_mutex_create fr_mutex_lock fr_mutex_unlock fr_cond_wait
                    malloc fr_thread_start fr_run_queue_try_push_owned fr_run_queue_pop_owned
                    fr_run_queue_steal_owned fr_native_queue_try_push_owned
                    fr_native_queue_pop_owned fr_native_queue_steal_owned)
                target_link_options(${name}_test PRIVATE "-Wl,--wrap=${symbol}")
            endforeach()
        endif()
        if(MSVC)
            target_compile_options(${name}_test PRIVATE /UNDEBUG)
        else()
            target_compile_options(${name}_test PRIVATE -UNDEBUG)
        endif()
        add_test(NAME ${name}_regressions COMMAND ${name}_test)
        set_tests_properties(${name}_regressions PROPERTIES TIMEOUT 30)
    endforeach()
endif()

install(FILES LICENSE NOTICE DESTINATION ${CMAKE_INSTALL_DATADIR}/forge-runtime)
