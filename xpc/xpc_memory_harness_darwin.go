//go:build gattxpc_test

package xpc

/*
#include <malloc/malloc.h>
static size_t XpcTestHeapBytes(void) {
    malloc_statistics_t stats;
    malloc_zone_statistics(NULL, &stats);
    return stats.size_in_use;
}
*/
import "C"

func cHeapBytesForTest() uint64 { return uint64(C.XpcTestHeapBytes()) }
