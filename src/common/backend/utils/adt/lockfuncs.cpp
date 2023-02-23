/* -------------------------------------------------------------------------
 *
 * lockfuncs.c
 *		Functions for SQL access to various lock-manager capabilities.
 *
 * Copyright (c) 2002-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/backend/utils/adt/lockfuncs.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/transam.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "catalog/pg_type.h"
#include "catalog/pg_database.h"
#include "catalog/indexing.h"
#include "commands/dbcommands.h"
#include "funcapi.h"
#include "miscadmin.h"
#ifdef PGXC
#include "pgxc/groupmgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/nodemgr.h"
#include "pgxc/poolutils.h"
#include "executor/spi.h"
#include "tcop/utility.h"
#endif
#include "storage/predicate_internals.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "access/heapam.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "pgstat.h"

#define NUM_LOCKTAG_ID 17
/* This must match enum LockTagType! */
const char* const LockTagTypeNames[] = {"relation",
    "extend",
    "partition",
    "partition_seq",
    "page",
    "tuple",
    "transactionid",
    "virtualxid",
    "object",
    "cstore_freespace",
    "userlock",
    "advisory",
    "filenode",
    "subtransactionid",
    "tuple_uid"};

/* This must match enum PredicateLockTargetType (predicate_internals.h) */
static const char* const PredicateLockTagTypeNames[] = {"relation", "page", "tuple"};

/* Working status for pg_lock_status */
typedef struct {
    LockData* lockData;              /* state data from lmgr */
    int currIdx;                     /* current PROCLOCK index */
    PredicateLockData* predLockData; /* state data for pred locks */
    int predLockIdx;                 /* current index for pred lock */
} PG_Lock_Status;

#ifdef PGXC
/*
 * These enums are defined to make calls to pgxc_advisory_lock more readable.
 */
typedef enum { SESSION_LOCK, TRANSACTION_LOCK } LockLevel;

typedef enum { WAIT, DONT_WAIT } TryType;

static bool pgxc_advisory_lock(int64 key64, int32 key1, int32 key2, bool iskeybig, LOCKMODE lockmode,
    LockLevel locklevel, TryType locktry, Name databaseName = NULL);
#endif
/* Number of columns in pg_locks output */
#define NUM_LOCK_STATUS_COLUMNS 19

/*
 * VXIDGetDatum - Construct a text representation of a VXID
 *
 * This is currently only used in pg_lock_status, so we put it here.
 */
static Datum VXIDGetDatum(BackendId bid, LocalTransactionId lxid)
{
    /*
     * The representation is "<bid>/<lxid>", decimal and unsigned decimal
     * respectively.  Note that elog.c also knows how to format a vxid.
     */
    char vxidstr[64];

    errno_t ss_rc = snprintf_s(vxidstr, sizeof(vxidstr), sizeof(vxidstr) - 1, "%d/" XID_FMT, bid, lxid);
    securec_check_ss(ss_rc, "\0", "\0");

    return CStringGetTextDatum(vxidstr);
}

char* LocktagToString(const LOCKTAG locktag)
{

    StringInfoData tag;
    initStringInfo(&tag);
    
    appendStringInfo(&tag, "%x:%x:%x:%x:%x:%x", locktag.locktag_field1, locktag.locktag_field2,
        locktag.locktag_field3, locktag.locktag_field4, locktag.locktag_field5, locktag.locktag_type);

    return tag.data;
}

static void GetLocktagInfo(const LockInstanceData* instance, Datum values[])
{
    char* blocklocktag = LocktagToString(instance->locktag);
    /* colume No.17 */
    values[NUM_LOCKTAG_ID] = CStringGetTextDatum(blocklocktag);
    pfree_ext(blocklocktag);
}

static char* LockPredTagToString(const PREDICATELOCKTARGETTAG* predTag) 
{
    LOCKTAG tag;
    Oid dbId = GET_PREDICATELOCKTARGETTAG_DB(*predTag);
    Oid relId = GET_PREDICATELOCKTARGETTAG_RELATION(*predTag);

    switch (GET_PREDICATELOCKTARGETTAG_TYPE(*predTag)) {
        case PREDLOCKTAG_PAGE:
            {
                BlockNumber pageId = GET_PREDICATELOCKTARGETTAG_PAGE(*predTag);
                SET_LOCKTAG_PAGE(tag, dbId, relId, 0, pageId);
            }
            break;
        case PREDLOCKTAG_TUPLE:
            {
                BlockNumber pageId = GET_PREDICATELOCKTARGETTAG_PAGE(*predTag);
                OffsetNumber itemId = GET_PREDICATELOCKTARGETTAG_OFFSET(*predTag);
                SET_LOCKTAG_TUPLE(tag, dbId, relId, 0, pageId, itemId);
            }
            break;
        case PREDLOCKTAG_RELATION:
            SET_LOCKTAG_RELATION(tag, dbId, relId);
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR), errmsg("invalid predlock locktype")));
            break;
    }

    return LocktagToString(tag);
}

/*
 * pg_lock_status - produce a view with one row per held or awaited lock mode
 */
Datum pg_lock_status(PG_FUNCTION_ARGS)
{
    FuncCallContext* funcctx = NULL;
    PG_Lock_Status* mystatus = NULL;
    LockData* lockData = NULL;
    PredicateLockData* predLockData = NULL;

    if (SRF_IS_FIRSTCALL()) {
        TupleDesc tupdesc;
        MemoryContext oldcontext;

        /* create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /*
         * switch to memory context appropriate for multiple function calls
         */
        oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

        /* build tupdesc for result tuples */
        /* this had better match pg_locks view in system_views.sql */
        tupdesc = CreateTemplateTupleDesc(NUM_LOCK_STATUS_COLUMNS, false);
        TupleDescInitEntry(tupdesc, (AttrNumber)1, "locktype", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)2, "database", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)3, "relation", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)4, "page", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)5, "tuple", INT2OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)6, "bucket", INT4OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)7, "virtualxid", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)8, "transactionid", XIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)9, "classid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)10, "objid", OIDOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)11, "objsubid", INT2OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)12, "virtualtransaction", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)13, "pid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)14, "sessionid", INT8OID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)15, "mode", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)16, "granted", BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)17, "fastpath", BOOLOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)18, "locktag", TEXTOID, -1, 0);
        TupleDescInitEntry(tupdesc, (AttrNumber)19, "global_sessionid", TEXTOID, -1, 0);

        funcctx->tuple_desc = BlessTupleDesc(tupdesc);

        /*
         * Collect all the locking information that we will format and send
         * out as a result set.
         */
        mystatus = (PG_Lock_Status*)palloc(sizeof(PG_Lock_Status));
        funcctx->user_fctx = (void*)mystatus;

        mystatus->lockData = GetLockStatusData();
        mystatus->currIdx = 0;
        mystatus->predLockData = GetPredicateLockStatusData();
        mystatus->predLockIdx = 0;

        MemoryContextSwitchTo(oldcontext);
    }

    funcctx = SRF_PERCALL_SETUP();
    mystatus = (PG_Lock_Status*)funcctx->user_fctx;
    lockData = mystatus->lockData;

    while (mystatus->currIdx < lockData->nelements) {
        bool granted = false;
        LOCKMODE mode = 0;
        const char* locktypename = NULL;
        char tnbuf[32];
        Datum values[NUM_LOCK_STATUS_COLUMNS];
        bool nulls[NUM_LOCK_STATUS_COLUMNS];
        HeapTuple tuple;
        Datum result;
        LockInstanceData* instance = NULL;

        instance = &(lockData->locks[mystatus->currIdx]);

        /*
         * Look to see if there are any held lock modes in this PROCLOCK. If
         * so, report, and destructively modify lockData so we don't report
         * again.
         */
        granted = false;
        if (instance->holdMask) {
            for (mode = 0; mode < MAX_LOCKMODES; mode++) {
                if (instance->holdMask & LOCKBIT_ON(mode)) {
                    granted = true;
                    instance->holdMask &= LOCKBIT_OFF(mode);
                    break;
                }
            }
        }

        /*
         * If no (more) held modes to report, see if PROC is waiting for a
         * lock on this lock.
         */
        if (!granted) {
            if (instance->waitLockMode != NoLock) {
                /* Yes, so report it with proper mode */
                mode = instance->waitLockMode;

                /*
                 * We are now done with this PROCLOCK, so advance pointer to
                 * continue with next one on next call.
                 */
                mystatus->currIdx++;
            } else {
                /*
                 * Okay, we've displayed all the locks associated with this
                 * PROCLOCK, proceed to the next one.
                 */
                mystatus->currIdx++;
                continue;
            }
        }

        /*
         * Form tuple with appropriate data.
         */
        errno_t rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        if (instance->locktag.locktag_type <= LOCKTAG_LAST_TYPE)
            locktypename = LockTagTypeNames[instance->locktag.locktag_type];
        else {
            errno_t ss_rc =
                snprintf_s(tnbuf, sizeof(tnbuf), sizeof(tnbuf) - 1, "unknown %d", (int)instance->locktag.locktag_type);
            securec_check_ss(ss_rc, "\0", "\0");
            locktypename = tnbuf;
        }
        values[0] = CStringGetTextDatum(locktypename);

        switch ((LockTagType)instance->locktag.locktag_type) {
            case LOCKTAG_RELATION:
            case LOCKTAG_CSTORE_FREESPACE:
                values[1] = ObjectIdGetDatum(instance->locktag.locktag_field1);
                values[2] = ObjectIdGetDatum(instance->locktag.locktag_field2);
                nulls[3] = true;
                nulls[4] = true;
                nulls[5] = true;
                nulls[6] = true;
                nulls[7] = true;
                nulls[8] = true;
                nulls[9] = true;
                nulls[10] = true;
                break;
            case LOCKTAG_RELFILENODE:
                values[1] = ObjectIdGetDatum(instance->locktag.locktag_field1);
                values[2] = ObjectIdGetDatum(instance->locktag.locktag_field2);
                values[3] = ObjectIdGetDatum(instance->locktag.locktag_field3);
                nulls[4] = true;
                nulls[5] = true;
                nulls[6] = true;
                nulls[7] = true;
                nulls[8] = true;
                nulls[9] = true;
                nulls[10] = true;
                break;
            case LOCKTAG_RELATION_EXTEND:
                values[1] = ObjectIdGetDatum(instance->locktag.locktag_field1);
                values[2] = ObjectIdGetDatum(instance->locktag.locktag_field2);
                values[5] = UInt16GetDatum(instance->locktag.locktag_field5);
                nulls[3] = true;
                nulls[4] = true;
                nulls[6] = true;
                nulls[7] = true;
                nulls[8] = true;
                nulls[9] = true;
                nulls[10] = true;
                break;
            case LOCKTAG_PAGE:
                values[1] = ObjectIdGetDatum(instance->locktag.locktag_field1);
                values[2] = ObjectIdGetDatum(instance->locktag.locktag_field2);
                values[3] = UInt32GetDatum(instance->locktag.locktag_field3);
                values[5] = UInt16GetDatum(instance->locktag.locktag_field5);
                nulls[4] = true;
                nulls[6] = true;
                nulls[7] = true;
                nulls[8] = true;
                nulls[9] = true;
                nulls[10] = true;
                break;
            case LOCKTAG_TUPLE:
                values[1] = ObjectIdGetDatum(instance->locktag.locktag_field1);
                values[2] = ObjectIdGetDatum(instance->locktag.locktag_field2);
                values[3] = UInt32GetDatum(instance->locktag.locktag_field3);
                values[4] = UInt32GetDatum(instance->locktag.locktag_field4);
                values[5] = UInt16GetDatum(instance->locktag.locktag_field5);
                nulls[6] = true;
                nulls[7] = true;
                nulls[8] = true;
                nulls[9] = true;
                nulls[10] = true;
                break;
            case LOCKTAG_UID:
                values[1] = ObjectIdGetDatum(instance->locktag.locktag_field1);
                values[2] = ObjectIdGetDatum(instance->locktag.locktag_field2);
                nulls[3] = true;
                nulls[4] = true;
                nulls[5] = true;
                nulls[6] = true;
                values[7] = TransactionIdGetDatum((uint64)instance->locktag.locktag_field3 << 32 |
                                                  ((uint64)instance->locktag.locktag_field4));

                nulls[8] = true;
                nulls[9] = true;
                nulls[10] = true;
                break;
            case LOCKTAG_TRANSACTION:
                values[7] = TransactionIdGetDatum((TransactionId)instance->locktag.locktag_field1 |
                                                  ((TransactionId)instance->locktag.locktag_field2 << 32));
                nulls[1] = true;
                nulls[2] = true;
                nulls[3] = true;
                nulls[4] = true;
                nulls[5] = true;
                nulls[6] = true;
                nulls[8] = true;
                nulls[9] = true;
                nulls[10] = true;
                break;
            case LOCKTAG_VIRTUALTRANSACTION:
                values[6] = VXIDGetDatum(instance->locktag.locktag_field1,
                    (TransactionId)instance->locktag.locktag_field2 |
                        ((TransactionId)instance->locktag.locktag_field3 << 32));
                nulls[1] = true;
                nulls[2] = true;
                nulls[3] = true;
                nulls[4] = true;
                nulls[5] = true;
                nulls[7] = true;
                nulls[8] = true;
                nulls[9] = true;
                nulls[10] = true;
                break;
            case LOCKTAG_OBJECT:
            case LOCKTAG_USERLOCK:
            case LOCKTAG_ADVISORY:
            default: /* treat unknown locktags like OBJECT */
                values[1] = ObjectIdGetDatum(instance->locktag.locktag_field1);
                values[8] = ObjectIdGetDatum(instance->locktag.locktag_field2);
                values[9] = ObjectIdGetDatum(instance->locktag.locktag_field3);
                values[10] = Int32GetDatum(instance->locktag.locktag_field4);
                nulls[2] = true;
                nulls[3] = true;
                nulls[4] = true;
                nulls[5] = true;
                nulls[6] = true;
                nulls[7] = true;
                break;
        }

        values[11] = VXIDGetDatum(instance->backend, instance->lxid);
        if (instance->pid != 0)
            values[12] = Int64GetDatum(instance->pid);
        else
            nulls[12] = true;
        if (instance->sessionid != 0)
            values[13] = Int64GetDatum(instance->sessionid);
        else
            nulls[13] = true;
        values[14] = CStringGetTextDatum(GetLockmodeName(instance->locktag.locktag_lockmethodid, mode));
        values[15] = BoolGetDatum(granted);
        values[16] = BoolGetDatum(instance->fastpath);
        GetLocktagInfo(instance, values);
        char* gId = GetGlobalSessionStr(instance->globalSessionId);
        values[18] = CStringGetTextDatum(gId);
        pfree(gId);
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    /*
     * Have returned all regular locks. Now start on the SIREAD predicate
     * locks.
     */
    predLockData = mystatus->predLockData;
    if (mystatus->predLockIdx < predLockData->nelements) {
        PredicateLockTargetType lockType;

        PREDICATELOCKTARGETTAG* predTag = &(predLockData->locktags[mystatus->predLockIdx]);
        SERIALIZABLEXACT* xact = &(predLockData->xacts[mystatus->predLockIdx]);
        Datum values[NUM_LOCK_STATUS_COLUMNS];
        bool nulls[NUM_LOCK_STATUS_COLUMNS];
        HeapTuple tuple;
        Datum result;
        char* blocklocktag = NULL;
        
        mystatus->predLockIdx++;

        /*
         * Form tuple with appropriate data.
         */
        errno_t rc = EOK;
        rc = memset_s(values, sizeof(values), 0, sizeof(values));
        securec_check(rc, "\0", "\0");
        rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
        securec_check(rc, "\0", "\0");

        /* lock type */
        lockType = GET_PREDICATELOCKTARGETTAG_TYPE(*predTag);

        values[0] = CStringGetTextDatum(PredicateLockTagTypeNames[lockType]);

        /* lock target */
        values[1] = GET_PREDICATELOCKTARGETTAG_DB(*predTag);
        values[2] = GET_PREDICATELOCKTARGETTAG_RELATION(*predTag);
        if (lockType == PREDLOCKTAG_TUPLE)
            values[4] = GET_PREDICATELOCKTARGETTAG_OFFSET(*predTag);
        else
            nulls[4] = true;
        if ((lockType == PREDLOCKTAG_TUPLE) || (lockType == PREDLOCKTAG_PAGE))
            values[3] = GET_PREDICATELOCKTARGETTAG_PAGE(*predTag);
        else
            nulls[3] = true;

        /* these fields are targets for other types of locks */
        nulls[5] = true; /* bucketid */
        nulls[6] = true; /* virtualxid */
        nulls[7] = true; /* transactionid */
        nulls[8] = true; /* classid */
        nulls[9] = true; /* objid */
        nulls[10] = true; /* objsubid */

        /* lock holder */
        values[11] = VXIDGetDatum(xact->vxid.backendId, xact->vxid.localTransactionId);
        if (xact->pid != 0)
            values[12] = Int64GetDatum(xact->pid);
        else
            nulls[12] = true;
        nulls[13] = true;
        /*
         * Lock mode. Currently all predicate locks are SIReadLocks, which are
         * always held (never waiting) and have no fast path
         */
        values[14] = CStringGetTextDatum("SIReadLock");
        values[15] = BoolGetDatum(true);
        values[16] = BoolGetDatum(false);
        blocklocktag = LockPredTagToString(predTag);
        values[NUM_LOCKTAG_ID] = CStringGetTextDatum(blocklocktag);
        pfree_ext(blocklocktag);
        nulls[18] = true;
        tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
        result = HeapTupleGetDatum(tuple);
        SRF_RETURN_NEXT(funcctx, result);
    }

    SRF_RETURN_DONE(funcctx);
}

/*
 * Functions for manipulating advisory locks
 *
 * We make use of the locktag fields as follows:
 *
 *	field1: u_sess->proc_cxt.MyDatabaseId ... ensures locks are local to each database
 *	field2: first of 2 int4 keys, or high-order half of an int8 key
 *	field3: second of 2 int4 keys, or low-order half of an int8 key
 *	field4: 1 if using an int8 key, 2 if using 2 int4 keys
 */
#define SET_LOCKTAG_INT64(tag, key64) \
    SET_LOCKTAG_ADVISORY(tag, u_sess->proc_cxt.MyDatabaseId, (uint32)((key64) >> 32), (uint32)(key64), 1)
#define SET_LOCKTAG_INT32(tag, key1, key2) SET_LOCKTAG_ADVISORY(tag, u_sess->proc_cxt.MyDatabaseId, key1, key2, 2)

#define SET_LOCKTAG_INT32_DB(tag, databaseOid, key1, key2) SET_LOCKTAG_ADVISORY(tag, databaseOid, key1, key2, 2)

static void CheckIfAnySchemaInRedistribution()
{
    Relation rel = heap_open(NamespaceRelationId, AccessShareLock);
    TableScanDesc scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);
    bool isNull = false;
    HeapTuple tuple;
    Datum datum;
    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        datum = heap_getattr(tuple, Anum_pg_namespace_in_redistribution, RelationGetDescr(rel), &isNull);
        if (isNull) {
            continue;
        }
        if (DatumGetChar(datum) == 'y') {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Please check if another schema is in redistribution in the same database.")));
        }
    }
    tableam_scan_end(scan);
    heap_close(rel, NoLock);
}

static void UpdateSchemaInRedistribution(Name schemaName, bool isLock)
{
    Relation rel = heap_open(NamespaceRelationId, RowExclusiveLock);
    HeapTuple tuple = SearchSysCache1(NAMESPACENAME, CStringGetDatum(schemaName->data));
    if (!HeapTupleIsValid(tuple)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_SCHEMA),
            errmsg("schema \"%s\" does not exist", schemaName->data)));
    }
    Oid schemaOid = HeapTupleGetOid(tuple);
    if (schemaOid < FirstNormalObjectId && schemaOid != PG_PUBLIC_NAMESPACE) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("system schema \"%s\" does not support transfer", schemaName->data)));
    }

    /* Build an updated tuple */
    Datum newRecord[Natts_pg_namespace];
    bool newRecordNulls[Natts_pg_namespace];
    bool newRecordRepl[Natts_pg_namespace];
    errno_t rc = memset_s(newRecord, sizeof(newRecord), 0, sizeof(newRecord));
    securec_check(rc, "\0", "\0");
    rc = memset_s(newRecordNulls, sizeof(newRecordNulls), false, sizeof(newRecordNulls));
    securec_check(rc, "\0", "\0");
    rc = memset_s(newRecordRepl, sizeof(newRecordRepl), false, sizeof(newRecordRepl));
    securec_check(rc, "\0", "\0");
    if (isLock) {
        newRecord[Anum_pg_namespace_in_redistribution - 1] = CharGetDatum('n');
    } else {
        newRecord[Anum_pg_namespace_in_redistribution - 1] = CharGetDatum('y');
    }
    newRecordRepl[Anum_pg_namespace_in_redistribution - 1] = true;

    /* Update */
    HeapTuple newtuple = heap_modify_tuple(tuple, RelationGetDescr(rel), newRecord, newRecordNulls, newRecordRepl);
    simple_heap_update(rel, &tuple->t_self, newtuple);
    CatalogUpdateIndexes(rel, newtuple);
    heap_freetuple_ext(newtuple);

    ReleaseSysCache(tuple);
    heap_close(rel, NoLock);
}

#ifdef PGXC

/* Send updata pg_namespace statement to all other cns */
static void PGXCSendTransfer(Name schemaName, bool isLock)
{
    char updateSql[CHAR_BUF_SIZE] = {0};
    int rc;
    if (isLock) {
        rc = snprintf_s(updateSql, CHAR_BUF_SIZE, CHAR_BUF_SIZE - 1,
            "select pg_catalog.pgxc_unlock_for_transfer('%s'::name)", schemaName->data);
    } else {
        rc = snprintf_s(updateSql, CHAR_BUF_SIZE, CHAR_BUF_SIZE - 1,
            "select pg_catalog.pgxc_lock_for_transfer('%s'::name)", schemaName->data);
    }
    securec_check_ss(rc, "\0", "\0");
    ExecUtilityStmtOnNodes(updateSql, NULL, false, false, EXEC_ON_COORDS, false);
}

#define MAXINT8LEN 25

/*
 * pgxc_advisory_lock - Core function that implements the algorithm needed to
 * propogate the advisory lock function calls to all Coordinators.
 * The idea is to make the advisory locks cluster-aware, so that a user having
 * a lock from Coordinator 1 will make the user from Coordinator 2 to wait for
 * the same lock.
 *
 * Return true if all locks are returned successfully. False otherwise.
 * Effectively this function returns false only if dontWait is true. Otherwise
 * it either returns true, or waits on a resource, or throws an exception
 * returned by the lock function calls in case of unexpected or fatal errors.
 *
 * Currently used only for session level locks; not used for transaction level
 * locks.
 */
static bool pgxc_advisory_lock(int64 key64, int32 key1, int32 key2, bool iskeybig, LOCKMODE lockmode,
    LockLevel locklevel, TryType locktry, Name databaseName)
{
    LOCKTAG locktag;
    Oid *coOids = NULL, *dnOids = NULL;
    int numdnodes, numcoords;
    StringInfoData lock_cmd, unlock_cmd, lock_funcname, unlock_funcname, args;
    char str_key[MAXINT8LEN + 1];
    int i, prev;
    bool abort_locking = false;
    Datum lock_status;
    bool sessionLock = (locklevel == SESSION_LOCK);
    bool dontWait = (locktry == DONT_WAIT);
    bool sp_database = (databaseName != NULL);
    Oid databaseOid = u_sess->proc_cxt.MyDatabaseId;

    if (sp_database)
        databaseOid = get_database_oid(databaseName->data, false);

    if (iskeybig)
        SET_LOCKTAG_INT64(locktag, key64);
    else
        SET_LOCKTAG_INT32_DB(locktag, databaseOid, key1, key2);

    /*
     * Before get cn/dn oids, we should refresh NumCoords/NumDataNodes and co_handles/dn_handles in u_sess in case of
     * can not process SIGUSR1 of "pgxc_pool_reload" command immediately.
     */
#ifdef ENABLE_MULTIPLE_NODES
    ReloadPoolerWithoutTransaction();
#endif
    PgxcNodeGetOids(&coOids, &dnOids, &numcoords, &numdnodes, false);

    /* Skip everything XC specific if there's only one Coordinator running */
    if (numcoords <= 1) {
        LockAcquireResult res;

        res = LockAcquire(&locktag, lockmode, sessionLock, dontWait);
        return (res == LOCKACQUIRE_OK || res == LOCKACQUIRE_ALREADY_HELD);
    }

    /*
     * If there is already a lock held by us, just increment and return; we
     * already did all necessary steps when we locked for the first time.
     */
    if (LockIncrementIfExists(&locktag, lockmode, sessionLock) == true)
        return true;

    initStringInfo(&lock_funcname);
    appendStringInfo(&lock_funcname,
        "pg_%sadvisory_%slock%s",
        (dontWait ? "try_" : ""),
        (sessionLock ? "" : "xact_"),
        ((lockmode == ShareLock) ? "_shared" : ""));

    initStringInfo(&unlock_funcname);
    appendStringInfo(&unlock_funcname, "pg_advisory_unlock%s", ((lockmode == ShareLock) ? "_shared" : ""));

    initStringInfo(&args);

    if (iskeybig) {
        pg_lltoa(key64, str_key);
        appendStringInfo(&args, "%s", str_key);
    } else {
        pg_ltoa(key1, str_key);
        appendStringInfo(&args, "%s, ", str_key);
        pg_ltoa(key2, str_key);
        appendStringInfo(&args,
            "%s%s%s%s",
            str_key,
            (sp_database ? ", \'" : ""),
            (sp_database ? databaseName->data : ""),
            (sp_database ? "\'" : ""));
    }

    initStringInfo(&lock_cmd);
    appendStringInfo(&lock_cmd, "SELECT pg_catalog.%s(%s)", lock_funcname.data, args.data);
    initStringInfo(&unlock_cmd);
    appendStringInfo(&unlock_cmd, "SELECT pg_catalog.%s(%s)", unlock_funcname.data, args.data);

    /*
     * Go on locking on each Coordinator. Keep on unlocking the previous one
     * after a lock is held on next Coordinator. Don't unlock the local
     * Coordinator. After finishing all Coordinators, ultimately only the local
     * Coordinator would be locked, but still we will have scanned all
     * Coordinators to make sure no one else has already grabbed the lock. The
     * reason for unlocking all remote locks is because the session level locks
     * don't get unlocked until explicitly unlocked or the session quits. After
     * the user session quits without explicitly unlocking, the coord-to-coord
     * pooler connection stays and so does the remote Coordinator lock.
     */
    prev = -1;
    for (i = 0; i <= numcoords && !abort_locking; i++, prev++) {
        if (i < numcoords) {
            /* If this Coordinator is myself, execute native lock calls */
            if (i == u_sess->pgxc_cxt.PGXCNodeId - 1)
                lock_status = LockAcquire(&locktag, lockmode, sessionLock, dontWait);
            else
                lock_status = pgxc_execute_on_nodes(1, &coOids[i], lock_cmd.data);

            if (dontWait == true && DatumGetBool(lock_status) == false) {
                abort_locking = true;
                /*
                 * If we have gone past the local Coordinator node, it implies
                 * that we have obtained a local lock. But now that we are
                 * aborting, we need to release the local lock first.
                 */
                if (i > u_sess->pgxc_cxt.PGXCNodeId - 1)
                    (void)LockRelease(&locktag, lockmode, sessionLock);
            }
        }

        /*
         * If we are dealing with session locks, unlock the previous lock, but
         * only if it is a remote Coordinator. If it is a local one, we want to
         * keep that lock. Remember, the final status should be that there is
         * only *one* lock held, and that is the local lock.
         */
        if (sessionLock && prev >= 0 && prev != u_sess->pgxc_cxt.PGXCNodeId - 1)
            pgxc_execute_on_nodes(1, &coOids[prev], unlock_cmd.data);
    }

    return (!abort_locking);
}

#endif /* PGXC */

/*
 * pg_advisory_lock(int8) - acquire exclusive lock on an int8 key
 */
Datum pg_advisory_lock_int8(PG_FUNCTION_ARGS)
{
    int64 key = PG_GETARG_INT64(0);
    LOCKTAG tag;

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        (void)pgxc_advisory_lock(key, 0, 0, true, ExclusiveLock, SESSION_LOCK, WAIT);
        PG_RETURN_VOID();
    }
#endif

    SET_LOCKTAG_INT64(tag, key);

    (void)LockAcquire(&tag, ExclusiveLock, true, false);

    PG_RETURN_VOID();
}

/*
 * pg_advisory_xact_lock(int8) - acquire xact scoped
 * exclusive lock on an int8 key
 */
Datum pg_advisory_xact_lock_int8(PG_FUNCTION_ARGS)
{
    int64 key = PG_GETARG_INT64(0);
    LOCKTAG tag;

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        (void)pgxc_advisory_lock(key, 0, 0, true, ExclusiveLock, TRANSACTION_LOCK, WAIT);
        PG_RETURN_VOID();
    }
#endif

    SET_LOCKTAG_INT64(tag, key);

    (void)LockAcquire(&tag, ExclusiveLock, false, false);

    PG_RETURN_VOID();
}

/*
 * pg_advisory_lock_shared(int8) - acquire share lock on an int8 key
 */
Datum pg_advisory_lock_shared_int8(PG_FUNCTION_ARGS)
{
    int64 key = PG_GETARG_INT64(0);
    LOCKTAG tag;

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        (void)pgxc_advisory_lock(key, 0, 0, true, ShareLock, SESSION_LOCK, WAIT);
        PG_RETURN_VOID();
    }
#endif

    SET_LOCKTAG_INT64(tag, key);

    (void)LockAcquire(&tag, ShareLock, true, false);

    PG_RETURN_VOID();
}

/*
 * pg_advisory_xact_lock_shared(int8) - acquire xact scoped
 * share lock on an int8 key
 */
Datum pg_advisory_xact_lock_shared_int8(PG_FUNCTION_ARGS)
{
    int64 key = PG_GETARG_INT64(0);
    LOCKTAG tag;

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        (void)pgxc_advisory_lock(key, 0, 0, true, ShareLock, TRANSACTION_LOCK, WAIT);
        PG_RETURN_VOID();
    }
#endif

    SET_LOCKTAG_INT64(tag, key);

    (void)LockAcquire(&tag, ShareLock, false, false);

    PG_RETURN_VOID();
}

/*
 * pg_try_advisory_lock(int8) - acquire exclusive lock on an int8 key, no wait
 *
 * Returns true if successful, false if lock not available
 */
Datum pg_try_advisory_lock_int8(PG_FUNCTION_ARGS)
{
    int64 key = PG_GETARG_INT64(0);
    LOCKTAG tag;
    LockAcquireResult res;

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        PG_RETURN_BOOL(pgxc_advisory_lock(key, 0, 0, true, ExclusiveLock, SESSION_LOCK, DONT_WAIT));
#endif

    SET_LOCKTAG_INT64(tag, key);

    res = LockAcquire(&tag, ExclusiveLock, true, true);

    PG_RETURN_BOOL(res != LOCKACQUIRE_NOT_AVAIL);
}

/*
 * pg_try_advisory_xact_lock(int8) - acquire xact scoped
 * exclusive lock on an int8 key, no wait
 *
 * Returns true if successful, false if lock not available
 */
Datum pg_try_advisory_xact_lock_int8(PG_FUNCTION_ARGS)
{
    int64 key = PG_GETARG_INT64(0);
    LOCKTAG tag;
    LockAcquireResult res;

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        PG_RETURN_BOOL(pgxc_advisory_lock(key, 0, 0, true, ExclusiveLock, TRANSACTION_LOCK, DONT_WAIT));
#endif

    SET_LOCKTAG_INT64(tag, key);

    res = LockAcquire(&tag, ExclusiveLock, false, true);

    PG_RETURN_BOOL(res != LOCKACQUIRE_NOT_AVAIL);
}

/*
 * pg_try_advisory_lock_shared(int8) - acquire share lock on an int8 key, no wait
 *
 * Returns true if successful, false if lock not available
 */
Datum pg_try_advisory_lock_shared_int8(PG_FUNCTION_ARGS)
{
    int64 key = PG_GETARG_INT64(0);
    LOCKTAG tag;
    LockAcquireResult res;

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        PG_RETURN_BOOL(pgxc_advisory_lock(key, 0, 0, true, ShareLock, SESSION_LOCK, DONT_WAIT));
#endif

    SET_LOCKTAG_INT64(tag, key);

    res = LockAcquire(&tag, ShareLock, true, true);

    PG_RETURN_BOOL(res != LOCKACQUIRE_NOT_AVAIL);
}

/*
 * pg_try_advisory_xact_lock_shared(int8) - acquire xact scoped
 * share lock on an int8 key, no wait
 *
 * Returns true if successful, false if lock not available
 */
Datum pg_try_advisory_xact_lock_shared_int8(PG_FUNCTION_ARGS)
{
    int64 key = PG_GETARG_INT64(0);
    LOCKTAG tag;
    LockAcquireResult res;

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        PG_RETURN_BOOL(pgxc_advisory_lock(key, 0, 0, true, ShareLock, TRANSACTION_LOCK, DONT_WAIT));
#endif

    SET_LOCKTAG_INT64(tag, key);

    res = LockAcquire(&tag, ShareLock, false, true);

    PG_RETURN_BOOL(res != LOCKACQUIRE_NOT_AVAIL);
}

/*
 * pg_advisory_unlock(int8) - release exclusive lock on an int8 key
 *
 * Returns true if successful, false if lock was not held
 */
Datum pg_advisory_unlock_int8(PG_FUNCTION_ARGS)
{
    int64 key = PG_GETARG_INT64(0);
    LOCKTAG tag;
    bool res = false;

    SET_LOCKTAG_INT64(tag, key);

    res = LockRelease(&tag, ExclusiveLock, true);

    PG_RETURN_BOOL(res);
}

/*
 * pg_advisory_unlock_shared(int8) - release share lock on an int8 key
 *
 * Returns true if successful, false if lock was not held
 */
Datum pg_advisory_unlock_shared_int8(PG_FUNCTION_ARGS)
{
    int64 key = PG_GETARG_INT64(0);
    LOCKTAG tag;
    bool res = false;

    SET_LOCKTAG_INT64(tag, key);

    res = LockRelease(&tag, ShareLock, true);

    PG_RETURN_BOOL(res);
}

/*
 * pg_advisory_lock(int4, int4) - acquire exclusive lock on 2 int4 keys
 */
Datum pg_advisory_lock_int4(PG_FUNCTION_ARGS)
{
    int32 key1 = PG_GETARG_INT32(0);
    int32 key2 = PG_GETARG_INT32(1);
    LOCKTAG tag;

    if (key1 == XC_LOCK_FOR_BACKUP_KEY_1 && key2 == XC_LOCK_FOR_BACKUP_KEY_2 && !superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Only system admin can lock the cluster.")));

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        (void)pgxc_advisory_lock(0, key1, key2, false, ExclusiveLock, SESSION_LOCK, WAIT);
        elog(INFO, "please do not close this session until you are done adding the new node");
        PG_RETURN_VOID();
    }
#endif

    SET_LOCKTAG_INT32(tag, key1, key2);

    (void)LockAcquire(&tag, ExclusiveLock, true, false);

    PG_RETURN_VOID();
}

/*
 * pg_advisory_lock(int4, int4, Name) - acquire exclusive lock on 2 int4 keys for specific database
 */
Datum pg_advisory_lock_sp_db_int4(PG_FUNCTION_ARGS)
{
    int32 key1 = PG_GETARG_INT32(0);
    int32 key2 = PG_GETARG_INT32(1);
    Name databaseName = PG_GETARG_NAME(2);
    LOCKTAG tag;
    Oid databaseOid = u_sess->proc_cxt.MyDatabaseId;

    if (key1 == XC_LOCK_FOR_BACKUP_KEY_1 && key2 == XC_LOCK_FOR_BACKUP_KEY_2 && !superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Only system admin can lock the cluster.")));

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        (void)pgxc_advisory_lock(0, key1, key2, false, ExclusiveLock, SESSION_LOCK, WAIT, databaseName);
        elog(INFO, "please do not close this session until you are done adding the new node");
        PG_RETURN_VOID();
    }
#endif

    if (databaseName != NULL)
        databaseOid = get_database_oid(databaseName->data, false);

    SET_LOCKTAG_INT32_DB(tag, databaseOid, key1, key2);

    (void)LockAcquire(&tag, ExclusiveLock, true, false);

    PG_RETURN_VOID();
}

/*
 * pg_advisory_xact_lock(int4, int4) - acquire xact scoped
 * exclusive lock on 2 int4 keys
 */
Datum pg_advisory_xact_lock_int4(PG_FUNCTION_ARGS)
{
    int32 key1 = PG_GETARG_INT32(0);
    int32 key2 = PG_GETARG_INT32(1);
    LOCKTAG tag;

    if (key1 == XC_LOCK_FOR_BACKUP_KEY_1 && key2 == XC_LOCK_FOR_BACKUP_KEY_2 && !superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Only system admin can lock the cluster.")));

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        (void)pgxc_advisory_lock(0, key1, key2, false, ExclusiveLock, TRANSACTION_LOCK, WAIT);
        PG_RETURN_VOID();
    }
#endif

    SET_LOCKTAG_INT32(tag, key1, key2);

    (void)LockAcquire(&tag, ExclusiveLock, false, false);

    PG_RETURN_VOID();
}

/*
 * pg_advisory_lock_shared(int4, int4) - acquire share lock on 2 int4 keys
 */
Datum pg_advisory_lock_shared_int4(PG_FUNCTION_ARGS)
{
    int32 key1 = PG_GETARG_INT32(0);
    int32 key2 = PG_GETARG_INT32(1);
    LOCKTAG tag;

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        (void)pgxc_advisory_lock(0, key1, key2, false, ShareLock, SESSION_LOCK, WAIT);
        PG_RETURN_VOID();
    }
#endif

    SET_LOCKTAG_INT32(tag, key1, key2);

    (void)LockAcquire(&tag, ShareLock, true, false);

    PG_RETURN_VOID();
}

/*
 * pg_advisory_xact_lock_shared(int4, int4) - acquire xact scoped
 * share lock on 2 int4 keys
 */
Datum pg_advisory_xact_lock_shared_int4(PG_FUNCTION_ARGS)
{
    int32 key1 = PG_GETARG_INT32(0);
    int32 key2 = PG_GETARG_INT32(1);
    LOCKTAG tag;

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        (void)pgxc_advisory_lock(0, key1, key2, false, ShareLock, TRANSACTION_LOCK, WAIT);
        PG_RETURN_VOID();
    }
#endif

    SET_LOCKTAG_INT32(tag, key1, key2);

    (void)LockAcquire(&tag, ShareLock, false, false);

    PG_RETURN_VOID();
}

/*
 * pg_try_advisory_lock(int4, int4) - acquire exclusive lock on 2 int4 keys, no wait
 *
 * Returns true if successful, false if lock not available
 */
Datum pg_try_advisory_lock_int4(PG_FUNCTION_ARGS)
{
    int32 key1 = PG_GETARG_INT32(0);
    int32 key2 = PG_GETARG_INT32(1);
    LOCKTAG tag;
    LockAcquireResult res;

    if (key1 == XC_LOCK_FOR_BACKUP_KEY_1 && key2 == XC_LOCK_FOR_BACKUP_KEY_2 && !superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Only system admin can lock the cluster.")));

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        PG_RETURN_BOOL(pgxc_advisory_lock(0, key1, key2, false, ExclusiveLock, SESSION_LOCK, DONT_WAIT));
#endif

    SET_LOCKTAG_INT32(tag, key1, key2);

    res = LockAcquire(&tag, ExclusiveLock, true, true);

    PG_RETURN_BOOL(res != LOCKACQUIRE_NOT_AVAIL);
}

/*
 * pg_try_advisory_xact_lock(int4, int4) - acquire xact scoped
 * exclusive lock on 2 int4 keys, no wait
 *
 * Returns true if successful, false if lock not available
 */
Datum pg_try_advisory_xact_lock_int4(PG_FUNCTION_ARGS)
{
    int32 key1 = PG_GETARG_INT32(0);
    int32 key2 = PG_GETARG_INT32(1);
    LOCKTAG tag;
    LockAcquireResult res;

    if (key1 == XC_LOCK_FOR_BACKUP_KEY_1 && key2 == XC_LOCK_FOR_BACKUP_KEY_2 && !superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("Only system admin can lock the cluster.")));

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        PG_RETURN_BOOL(pgxc_advisory_lock(0, key1, key2, false, ExclusiveLock, TRANSACTION_LOCK, DONT_WAIT));
#endif

    SET_LOCKTAG_INT32(tag, key1, key2);

    res = LockAcquire(&tag, ExclusiveLock, false, true);

    PG_RETURN_BOOL(res != LOCKACQUIRE_NOT_AVAIL);
}

/*
 * pg_try_advisory_lock_shared(int4, int4) - acquire share lock on 2 int4 keys, no wait
 *
 * Returns true if successful, false if lock not available
 */
Datum pg_try_advisory_lock_shared_int4(PG_FUNCTION_ARGS)
{
    int32 key1 = PG_GETARG_INT32(0);
    int32 key2 = PG_GETARG_INT32(1);
    LOCKTAG tag;
    LockAcquireResult res;

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        PG_RETURN_BOOL(pgxc_advisory_lock(0, key1, key2, false, ShareLock, SESSION_LOCK, DONT_WAIT));
#endif

    SET_LOCKTAG_INT32(tag, key1, key2);

    res = LockAcquire(&tag, ShareLock, true, true);

    PG_RETURN_BOOL(res != LOCKACQUIRE_NOT_AVAIL);
}

/*
 * pg_try_advisory_xact_lock_shared(int4, int4) - acquire xact scoped
 * share lock on 2 int4 keys, no wait
 *
 * Returns true if successful, false if lock not available
 */
Datum pg_try_advisory_xact_lock_shared_int4(PG_FUNCTION_ARGS)
{
    int32 key1 = PG_GETARG_INT32(0);
    int32 key2 = PG_GETARG_INT32(1);
    LOCKTAG tag;
    LockAcquireResult res;

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord())
        PG_RETURN_BOOL(pgxc_advisory_lock(0, key1, key2, false, ShareLock, TRANSACTION_LOCK, DONT_WAIT));
#endif

    SET_LOCKTAG_INT32(tag, key1, key2);

    res = LockAcquire(&tag, ShareLock, false, true);

    PG_RETURN_BOOL(res != LOCKACQUIRE_NOT_AVAIL);
}

/*
 * pg_advisory_unlock(int4, int4) - release exclusive lock on 2 int4 keys
 *
 * Returns true if successful, false if lock was not held
 */
Datum pg_advisory_unlock_int4(PG_FUNCTION_ARGS)
{
    int32 key1 = PG_GETARG_INT32(0);
    int32 key2 = PG_GETARG_INT32(1);
    LOCKTAG tag;
    bool res = false;

    SET_LOCKTAG_INT32(tag, key1, key2);

    res = LockRelease(&tag, ExclusiveLock, true);

    PG_RETURN_BOOL(res);
}

/*
 * pg_advisory_unlock(int4, int4, Name) - release exclusive lock on 2 int4 keys for specific database
 *
 * Returns true if successful, false if lock was not held
 */
Datum pg_advisory_unlock_sp_db_int4(PG_FUNCTION_ARGS)
{
    int32 key1 = PG_GETARG_INT32(0);
    int32 key2 = PG_GETARG_INT32(1);
    Name databaseName = PG_GETARG_NAME(2);
    LOCKTAG tag;
    bool res = false;
    Oid databaseOid = u_sess->proc_cxt.MyDatabaseId;

    if (databaseName != NULL)
        databaseOid = get_database_oid(databaseName->data, false);

    SET_LOCKTAG_INT32_DB(tag, databaseOid, key1, key2);

    res = LockRelease(&tag, ExclusiveLock, true);

    PG_RETURN_BOOL(res);
}

/*
 * pg_advisory_unlock_shared(int4, int4) - release share lock on 2 int4 keys
 *
 * Returns true if successful, false if lock was not held
 */
Datum pg_advisory_unlock_shared_int4(PG_FUNCTION_ARGS)
{
    int32 key1 = PG_GETARG_INT32(0);
    int32 key2 = PG_GETARG_INT32(1);
    LOCKTAG tag;
    bool res = false;

    SET_LOCKTAG_INT32(tag, key1, key2);

    res = LockRelease(&tag, ShareLock, true);

    PG_RETURN_BOOL(res);
}

/*
 * pg_advisory_unlock_all() - release all advisory locks
 */
Datum pg_advisory_unlock_all(PG_FUNCTION_ARGS)
{
    LockReleaseSession(USER_LOCKMETHOD);

    PG_RETURN_VOID();
}

#ifdef PGXC
/*
 * pgxc_lock_for_backup
 *
 * Lock the cluster for taking backup
 * To lock the cluster, try to acquire a session level advisory lock exclusivly
 * By lock we mean to disallow any statements that change
 * the portions of the catalog which are backed up by pg_dump/pg_dumpall
 * Returns true or fails with an error message.
 */
Datum pgxc_lock_for_backup(PG_FUNCTION_ARGS)
{
    bool lockAcquired = false;
    int prepared_xact_count = 0;

    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("only system admin can lock the cluster for backup")));

    /*
     * The system cannot be locked for backup if there is an uncommitted
     * prepared transaction, the reason is as follows:
     * Utility statements are divided into two groups, one is allowed group
     * and the other is disallowed group. A statement is put in allowed group
     * if it does not make changes to the catalog or makes such changes which
     * are not backed up by pg_dump or pg_dumpall, otherwise it is put in
     * disallowed group. Every time a disallowed statement is issued we try to
     * hold an advisory lock in shared mode and if the lock can be acquired
     * only then the statement is allowed.
     * In case of prepared transactions suppose the lock is not released at
     * prepare transaction 'txn_id'
     * Consider the following scenario:
     *
     *	begin;
     *	create table abc_def(a int, b int);
     *	insert into abc_def values(1,2),(3,4);
     *	prepare transaction 'abc';
     *
     * Now assume that the server is restarted for any reason.
     * When prepared transactions are saved on disk, session level locks are
     * ignored and hence when the prepared transactions are reterieved and all
     * the other locks are reclaimed, but session level advisory locks are
     * not reclaimed.
     * Hence we made the following decisions
     * a) Transaction level advisory locks should be used for DDLs which are
     *    automatically released at prepare transaction 'txn_id'
     * b) If there is any uncomitted prepared transaction, it is assumed
     *    that it must be issuing a statement that belongs to disallowed
     *    group and hence the request to hold the advisory lock exclusively
     *    is denied.
     */
    /* Connect to SPI manager to check any prepared transactions */
    SPI_STACK_LOG("connect", NULL, NULL);
    if (SPI_connect() < 0) {
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("internal error while locking the cluster for backup")));
    }

    /* Are there any prepared transactions that have not yet been committed? */
    SPI_execute("select gid from pg_catalog.pg_prepared_xacts limit 1", true, 0);
    prepared_xact_count = SPI_processed;
    SPI_STACK_LOG("finish", NULL, NULL);
    SPI_finish();

    if (prepared_xact_count > 0) {
        ereport(ERROR,
            (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                errmsg("cannot lock cluster for backup in presence of %d uncommitted prepared transactions",
                    prepared_xact_count)));
    }

    /* try to acquire the advisory lock in exclusive mode */
    lockAcquired = DatumGetBool(DirectFunctionCall2(pg_try_advisory_lock_int4,
        t_thrd.postmaster_cxt.xc_lockForBackupKey1,
        t_thrd.postmaster_cxt.xc_lockForBackupKey2));

    if (!lockAcquired)
        ereport(ERROR,
            (errcode(ERRCODE_LOCK_NOT_AVAILABLE), errmsg("cannot lock cluster for backup, lock is already held")));

    /*
     * sessin level advisory locks stay for only as long as the session
     * that issues them does
     */
    elog(INFO, "please do not close this session until you are done adding the new node");

    /* will be true always */
    PG_RETURN_BOOL(lockAcquired);
}

/*
 * pgxc_unlock_for_sp_database (Name)
 * Unlock the specific database in Exclusive mode for specific database.
 */
Datum pgxc_unlock_for_sp_database(PG_FUNCTION_ARGS)
{
    Name databaseName = PG_GETARG_NAME(0);
    bool result = false;

    /* try to acquire the advisory lock in exclusive mode */
    result = DatumGetBool(DirectFunctionCall3(pg_advisory_unlock_sp_db_int4,
        t_thrd.postmaster_cxt.xc_lockForBackupKey1,
        t_thrd.postmaster_cxt.xc_lockForBackupKey2,
        NameGetDatum(databaseName)));

    PG_RETURN_BOOL(result);
}

/*
 * pgxc_lock_for_sp_database (Name)
 *
 * Lock the specific database in Exclusive mode.
 * To lock the cluster, try to acquire a session level advisory lock exclusivly
 * By lock we mean to disallow any DDL operation.
 * Returns true or fails with an error message.
 * If all objects in DDL is temp, then we only need to get lock of current coordinator, because
 * temp objects' defination will not be sent to remote coordinators.
 * if could not get the lock after the first try, pgxc_lock_for_sp_database will go into WaitOnLock until
 * try to get the lock again or lock wait timeout.
 */
Datum pgxc_lock_for_sp_database(PG_FUNCTION_ARGS)
{
    int prepared_xact_count;
    Name databaseName = PG_GETARG_NAME(0);

    if (!superuser())
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("only system admin can lock the cluster for backup")));

    /*
     * The system cannot be locked for backup if there is an uncommitted
     * prepared transaction, the reason is as follows:
     * Utility statements are divided into two groups, one is allowed group
     * and the other is disallowed group. A statement is put in allowed group
     * if it does not make changes to the catalog or makes such changes which
     * are not backed up by pg_dump or pg_dumpall, otherwise it is put in
     * disallowed group. Every time a disallowed statement is issued we try to
     * hold an advisory lock in shared mode and if the lock can be acquired
     * only then the statement is allowed.
     * In case of prepared transactions suppose the lock is not released at
     * prepare transaction 'txn_id'
     * Consider the following scenario:
     *
     *	begin;
     *	create table abc_def(a int, b int);
     *	insert into abc_def values(1,2),(3,4);
     *	prepare transaction 'abc';
     *
     * Now assume that the server is restarted for any reason.
     * When prepared transactions are saved on disk, session level locks are
     * ignored and hence when the prepared transactions are reterieved and all
     * the other locks are reclaimed, but session level advisory locks are
     * not reclaimed.
     * Hence we made the following decisions
     * a) Transaction level advisory locks should be used for DDLs which are
     *    automatically released at prepare transaction 'txn_id'
     * b) If there is any uncomitted prepared transaction, it is assumed
     *    that it must be issuing a statement that belongs to disallowed
     *    group and hence the request to hold the advisory lock exclusively
     *    is denied.
     */

    /* Connect to SPI manager to check any prepared transactions */
    SPI_STACK_LOG("connect", NULL, NULL);
    if (SPI_connect() < 0) {
        ereport(ERROR,
            (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("internal error while locking the cluster for backup")));
    }

    /* Are there any prepared transactions that have not yet been committed? */
    SPI_execute("select gid from pg_catalog.pg_prepared_xacts limit 1", true, 0);
    prepared_xact_count = SPI_processed;
    SPI_STACK_LOG("finish", NULL, NULL);
    SPI_finish();

    if (prepared_xact_count > 0) {
        ereport(ERROR,
            (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                errmsg("cannot lock cluster for backup in presence of %d uncommitted prepared transactions",
                    prepared_xact_count)));
    }

    /* try to acquire the advisory lock in exclusive mode */
    DirectFunctionCall3(pg_advisory_lock_sp_db_int4,
        t_thrd.postmaster_cxt.xc_lockForBackupKey1,
        t_thrd.postmaster_cxt.xc_lockForBackupKey2,
        NameGetDatum(databaseName));

    PG_RETURN_BOOL(true);
}

bool pg_try_advisory_lock_for_redis(Relation rel)
{
    LOCKMODE lockmode = u_sess->attr.attr_sql.enable_cluster_resize ? ExclusiveLock : ShareLock;
    LockLevel locklevel = u_sess->attr.attr_sql.enable_cluster_resize ? SESSION_LOCK : TRANSACTION_LOCK;
    TryType locktry = u_sess->attr.attr_sql.enable_cluster_resize ? WAIT : DONT_WAIT;
    bool result = pgxc_advisory_lock(0, 65534, RelationGetRelCnOid(rel), false, lockmode, locklevel, locktry, NULL);
    if (u_sess->attr.attr_sql.enable_cluster_resize && result) {
        return true;
    } else if (result) {
        LOCKTAG tag;
        SET_LOCKTAG_INT32_DB(tag, u_sess->proc_cxt.MyDatabaseId, 65534, RelationGetRelCnOid(rel));
        (void)LockRelease(&tag, ShareLock, false);
        return true;
    }
    return false;
}

/*
 * pgxc_lock_for_backup
 *
 * Lock the cluster for taking backup
 * To lock the cluster, try to acquire a session level advisory lock exclusivly
 * By lock we mean to disallow any statements that change
 * the portions of the catalog which are backed up by pg_dump/pg_dumpall
 * Returns true or fails with an error message.
 * If all objects in DDL is temp, then we only need to get lock of current coordinator, because
 * temp objects' defination will not be sent to remote coordinators.
 */
void pgxc_lock_for_utility_stmt(Node* parsetree, bool is_temp)
{
    bool lockAcquired = false;
    LOCKTAG tag;
    LockAcquireResult res;

    /*
     * Reload configuration if we got SIGHUP from the postmaster, since we want to fetch
     * latest enable_online_ddl_waitlock values.
     */
    reload_configfile();

    /*
     * Change donwait lock to wait lock for online expansion condition to prevent
     * disruption of business DDL.
     */
    if (!u_sess->attr.attr_sql.enable_online_ddl_waitlock) {

        /*
         * Temp table. For temp table, no need to lock other coordinator, because
         * it's defination is only on this coordinator.
         */
        if (!is_temp) {
            lockAcquired = DatumGetBool(DirectFunctionCall2(pg_try_advisory_xact_lock_shared_int4,
                t_thrd.postmaster_cxt.xc_lockForBackupKey1,
                t_thrd.postmaster_cxt.xc_lockForBackupKey2));
        } else {
            SET_LOCKTAG_INT32(
                tag, t_thrd.postmaster_cxt.xc_lockForBackupKey1, t_thrd.postmaster_cxt.xc_lockForBackupKey2);

            res = LockAcquire(&tag, ShareLock, false, true);

            lockAcquired = (res != LOCKACQUIRE_NOT_AVAIL);
        }

        if (!lockAcquired)
            ereport(ERROR,
                (errcode(ERRCODE_READ_ONLY_SQL_TRANSACTION),
                    errmsg(
                        "cannot execute %s in a locked cluster", parsetree ? CreateCommandTag(parsetree) : "VACUUM")));
    } else {
        /*
         * We use pg_advisory_lock to ensure metadata synchronization in online expansion, so
         * we need used wait lock for DDL so that DDL can wait for pg_advisory_lock but not
         * exit directly when cluster was locked. and there is no need to check the return
         * value in wait lock condition.
         */
        if (!is_temp) {
            DirectFunctionCall2(pg_advisory_xact_lock_shared_int4,
                t_thrd.postmaster_cxt.xc_lockForBackupKey1,
                t_thrd.postmaster_cxt.xc_lockForBackupKey2);
        } else {
            /*
             * When in online node replace we don't need be blocked by cluster lock,
             * and we don't need call LockAcquire here.
             */
            if (OM_ONLINE_EXPANSION == get_om_online_state()) {
                /*
                 * Don't support temp table in online scenarios for reasons:
                 * 1.temp table only created in one coordinator, and can't be blocked if cluster
                 *  lock is on other coordinators.
                 * 2.temp table's defination is on only one coordinator which cause metadata of
                 *  temp table on difference coordinators will loss when use random one cn build
                 *  dn to restore metadata.
                 * 3.we won't do data redistribute of temp table through gs_redis which will cause
                 *  data loss of temp table after online expansion.
                 */
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("temp table is not supported in online expansion")));
            }
        }

        /*
         * Set current_installation_nodegroup to null here for
         * PgxcGroupGetInstallationGroup to get the newest one.
         */
        CleanNodeGroupStatus();

        /*
         * We should manually reload pooler message here for online expansion.
         * Notice : Reload pooler in transaction block is not supported, but we
         * we need do it for single-query scene.
         */
        reload_online_pooler();
    }
}

Datum pgxc_lock_for_transfer(PG_FUNCTION_ARGS)
{
    Name schemaName = PG_GETARG_NAME(0);
    bool isLock = false;

    /* 1. superuser is allowed */
    if (!superuser() || IS_PGXC_DATANODE) {
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("Only system admin can use the function on coordinator")));
    }
    if (IsInitdb || u_sess->attr.attr_common.IsInplaceUpgrade) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Can not run the function during initdb or upgrade")));
    }

    /* 2. lock database */
    char *databaseName = get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId, true);
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        /* pgxc_lock_for_sp_database will affect gs_redis in process, so check first */
        CheckIfAnySchemaInRedistribution();

        DirectFunctionCall1(pgxc_lock_for_sp_database, CStringGetDatum(databaseName));
    }

    /* 3. updata in_redistribution in pg_namespace system table */
    UpdateSchemaInRedistribution(schemaName, isLock);

    /* 4. send transfer to all other coordinator and unlock database */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        PGXCSendTransfer(schemaName, isLock);

        DirectFunctionCall1(pgxc_unlock_for_sp_database, CStringGetDatum(databaseName));
    }

    PG_RETURN_BOOL(true);
}

Datum pgxc_unlock_for_transfer(PG_FUNCTION_ARGS)
{
    Name schemaName = PG_GETARG_NAME(0);
    bool isLock = true;

    /* 1. superuser is allowed */
    if (!superuser() || IS_PGXC_DATANODE) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("Only system admin can use the function on coordinator")));
    }
    if (IsInitdb || u_sess->attr.attr_common.IsInplaceUpgrade) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Can not run the function during initdb or upgrade")));
    }

    /* 2. not lock database in case we cannot, just set the value */
    UpdateSchemaInRedistribution(schemaName, isLock);

    /* 3. send transfer statement to all other coordinator. */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord()) {
        PGXCSendTransfer(schemaName, isLock);
    }

    PG_RETURN_BOOL(true);
}
#endif
