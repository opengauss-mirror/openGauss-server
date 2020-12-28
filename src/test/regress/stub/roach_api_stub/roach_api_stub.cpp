
#include "postgres.h"
#include "knl/knl_variable.h"

#include "bulkload/roach_api.h"
#include "fmgr.h"

PG_MODULE_MAGIC;

/*
 * SQL functions
 */

PG_FUNCTION_INFO_V1(roach_handler);
extern "C" Datum roach_handler(PG_FUNCTION_ARGS);

static void* openRoach(char* url, char* mode)
{
    // success, return a non-zero value
    return (void*)1;
}

static int closeRoach(void* roach_context)
{
    // success, return 0
    return 0;
}

static size_t readRoach(void* buf, size_t size, size_t len, void* roach_context)
{
    // success, the number of items (size_t len)
    return 0;
}

static size_t writeRoach(void* buf, size_t size, size_t len, void* roach_context, bool complete_line)
{
    // success, the number of items (size_t len)
    return len;
}

extern int errorRoach(void* roach_context)
{
    // success, return 0
    return 0;
}

Datum roach_handler(PG_FUNCTION_ARGS)
{
    RoachRoutine* roachroutine = makeNode(RoachRoutine);

    roachroutine->Open = openRoach;
    roachroutine->Close = closeRoach;
    roachroutine->Read = readRoach;
    roachroutine->Write = writeRoach;
    roachroutine->Error = errorRoach;

    PG_RETURN_POINTER(roachroutine);
}
