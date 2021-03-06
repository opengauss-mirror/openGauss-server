/*
 * redistribute.cpp
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <limits.h>

#include "fmgr.h"
#include "funcapi.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/redistrib.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"

#define DatumGetItemPointer(X) ((ItemPointer)DatumGetPointer(X))
#define ItemPointerGetDatum(X) PointerGetDatum(X)
#define PG_GETARG_ITEMPOINTER(n) DatumGetItemPointer(PG_GETARG_DATUM(n))
#define PG_RETURN_ITEMPOINTER(x) return ItemPointerGetDatum(x)
#define PG_RETURN_BOOL(x) return BoolGetDatum(x)

PG_MODULE_MAGIC;

extern "C" Datum pg_get_redis_rel_end_ctid(PG_FUNCTION_ARGS);
extern "C" Datum pg_get_redis_rel_start_ctid(PG_FUNCTION_ARGS);
extern "C" Datum pg_enable_redis_proc_cancelable(PG_FUNCTION_ARGS);
extern "C" Datum pg_disable_redis_proc_cancelable(PG_FUNCTION_ARGS);
extern "C" Datum pg_tupleid_get_blocknum(PG_FUNCTION_ARGS);
extern "C" Datum pg_tupleid_get_offset(PG_FUNCTION_ARGS);
extern "C" Datum pg_tupleid_get_ctid_to_bigint(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_get_redis_rel_end_ctid);
PG_FUNCTION_INFO_V1(pg_get_redis_rel_start_ctid);
PG_FUNCTION_INFO_V1(pg_enable_redis_proc_cancelable);
PG_FUNCTION_INFO_V1(pg_disable_redis_proc_cancelable);
PG_FUNCTION_INFO_V1(pg_tupleid_get_blocknum);
PG_FUNCTION_INFO_V1(pg_tupleid_get_offset);
PG_FUNCTION_INFO_V1(pg_tupleid_get_ctid_to_bigint);

/*
 * - Brief:  SQL interface for finding the end_ctid of redis relaion
 * - Parameter:
 *      @name: relation name in Name
 * - Return:
 *      @Datum: end_ctid in pointer.
 */
Datum pg_get_redis_rel_end_ctid(PG_FUNCTION_ARGS)
{
    char* rel_name = text_to_cstring(PG_GETARG_TEXT_P(0));
    Name part_name = PG_GETARG_NAME(1);
    ItemPointer result = NULL;

    result = (ItemPointer)palloc0(sizeof(ItemPointerData));
    ItemPointerSetInvalid(result);
    get_redis_rel_ctid(rel_name, NameStr(*part_name), REDIS_END_CTID, result);

    PG_RETURN_ITEMPOINTER(result);
}

/*
 * - Brief:  SQL interface for finding the start_ctid of redis relaion
 * - Parameter:
 *      @name: relation name in Name
 * - Return:
 *      @Datum: start_ctid in pointer.
 */
Datum pg_get_redis_rel_start_ctid(PG_FUNCTION_ARGS)
{
    char* rel_name = text_to_cstring(PG_GETARG_TEXT_P(0));
    Name part_name = PG_GETARG_NAME(1);
    ItemPointer result = NULL;

    result = (ItemPointer)palloc0(sizeof(ItemPointerData));
    ItemPointerSetInvalid(result);
    get_redis_rel_ctid(rel_name, NameStr(*part_name), REDIS_START_CTID, result);

    PG_RETURN_ITEMPOINTER(result);
}

/*
 * - Brief:  SQL interface to set PGPROC as redistribution
 * - Parameter:
 *      @input: NULL
 * - Return:
 *      @Datum: true if succeed or false if failed.
 */
Datum pg_enable_redis_proc_cancelable(PG_FUNCTION_ARGS)
{
    bool result = false;

    result = set_proc_redis();

    PG_RETURN_BOOL(result);
}

/*
 * - Brief:  SQL interface to reset PGPROC as redistribution
 * - Parameter:
 *      @input: NULL
 * - Return:
 *      @Datum: true if succeed or false if failed.
 */
Datum pg_disable_redis_proc_cancelable(PG_FUNCTION_ARGS)
{
    bool result = false;

    result = reset_proc_redis();

    PG_RETURN_BOOL(result);
}

/*
 * - Brief:  SQL interface for getting the blockid from tid
 * - Parameter:
 *      @Datum: tid in pointer
 * - Return:
 *      @Datum: blockid in pointer.
 */
Datum pg_tupleid_get_blocknum(PG_FUNCTION_ARGS)
{
    ItemPointer tupleid = NULL;
    tupleid = PG_GETARG_ITEMPOINTER(0);
    if (!ItemPointerIsValid(tupleid)) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("pg_tupleid_get_blocknum para tupleid is null!")));
    }
    PG_RETURN_UINT32(ItemPointerGetBlockNumber(tupleid));
}

/*
 * - Brief:  SQL interface for getting the offsetnumber from tid
 * - Parameter:
 *      @Datum: tid in pointer
 * - Return:
 *      @Datum: offsetnumber in pointer.
 */
Datum pg_tupleid_get_offset(PG_FUNCTION_ARGS)
{
    ItemPointer tupleid = NULL;
    tupleid = PG_GETARG_ITEMPOINTER(0);
    if (!ItemPointerIsValid(tupleid)) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("pg_tupleid_get_offset para tupleid is null!")));
    }
    PG_RETURN_UINT16(ItemPointerGetOffsetNumber(tupleid));
}

Datum pg_tupleid_get_ctid_to_bigint(PG_FUNCTION_ARGS)
{
    ItemPointer tupleid = NULL;
    tupleid = PG_GETARG_ITEMPOINTER(0);
    if (!ItemPointerIsValid(tupleid)) {
        ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("pg_tupleid_get_ctid_to_bigint para tupleid is null!")));
    }
    PG_RETURN_INT64(((uint64)ItemPointerGetBlockNumber(tupleid) << 16) | ItemPointerGetOffsetNumber(tupleid));
}
