#include "forge/platform.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>

#if defined(FORGE_OS_WINDOWS)
/* Compile the real Windows implementation with one move-operation seam.
 * The collision case uses real files and the real no-replace Win32 move.
 * No runtime archive object replaces the helper being tested here. */
static int move_mode, move_calls;
static char collision_path[MAX_PATH], last_source[MAX_PATH];

static BOOL test_move(LPCSTR source, LPCSTR destination, DWORD flags) {
    ++move_calls;
    strcpy(last_source, source);
    assert(!(flags & MOVEFILE_REPLACE_EXISTING));
    if (move_mode == 2) {
        SetLastError(ERROR_ACCESS_DENIED);
        return FALSE;
    }
    if (move_mode == 3) {
        SetLastError(ERROR_ALREADY_EXISTS);
        return FALSE;
    }
    if (move_mode == 1 && move_calls == 1) {
        HANDLE file = CreateFileA(destination, GENERIC_WRITE, 0, NULL,
                                  CREATE_NEW, FILE_ATTRIBUTE_NORMAL, NULL);
        assert(file != INVALID_HANDLE_VALUE);
        const char marker[] = "collision must survive";
        DWORD written;
        assert(WriteFile(file, marker, sizeof(marker), &written, NULL));
        assert(written == sizeof(marker));
        assert(CloseHandle(file));
        strcpy(collision_path, destination);
    }
    return MoveFileExA(source, destination, flags);
}

#define MoveFileExA test_move
#include "../src/platform.c"
#undef MoveFileExA

int main(void) {
    char path[MAX_PATH];
    assert(fr_make_temp_path(path, sizeof(path), "fgt", "") == 0);
    assert(GetFileAttributesA(path) != INVALID_FILE_ATTRIBUTES);
    assert(DeleteFileA(path));

    move_mode = 1;
    assert(fr_make_temp_path(path, sizeof(path), "fgt", ".fg") == 0);
    assert(move_calls >= 2 && strcmp(path, collision_path) != 0);
    assert(GetFileAttributesA(path) != INVALID_FILE_ATTRIBUTES);
    assert(DeleteFileA(path));
    HANDLE file = CreateFileA(collision_path, GENERIC_READ, FILE_SHARE_READ, NULL,
                              OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    assert(file != INVALID_HANDLE_VALUE);
    char marker[64] = {0};
    DWORD count;
    assert(ReadFile(file, marker, sizeof(marker), &count, NULL));
    assert(strcmp(marker, "collision must survive") == 0);
    assert(CloseHandle(file));
    assert(DeleteFileA(collision_path));

    move_mode = 2;
    move_calls = 0;
    assert(fr_make_temp_path(path, sizeof(path), "fgt", ".fg") == -1);
    assert(move_calls == 1);
    assert(GetFileAttributesA(last_source) == INVALID_FILE_ATTRIBUTES);
    move_mode = 3;
    move_calls = 0;
    assert(fr_make_temp_path(path, sizeof(path), "fgt", ".fg") == -1);
    assert(move_calls == 128);
    assert(GetFileAttributesA(last_source) == INVALID_FILE_ATTRIBUTES);
    assert(fr_make_temp_path(path, 1, "fgt", ".fg") == -1);
    puts("Windows temporary-path collision and cleanup checks passed");
    return 0;
}
#else
int main(void) { return 0; }
#endif
