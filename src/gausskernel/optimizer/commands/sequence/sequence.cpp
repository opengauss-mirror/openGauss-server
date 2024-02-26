/* -------------------------------------------------------------------------
 *
 * sequence.cpp
 *	  openGauss sequences support code.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/sequence.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/gtm.h"
#include "access/multixact.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/sysattr.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "access/xlogutils.h"
#include "access/xlogproc.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "catalog/pg_object.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/sequence.h"
#include "commands/tablecmds.h"
#include "funcapi.h"
#include "gtm/gtm_client.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/smgr/smgr.h"
#include "storage/tcap.h"
#include "utils/int8.h"
#include "utils/int16.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/resowner.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "commands/dbcommands.h"
#include "replication/slot.h"

#ifdef PGXC
#include "pgxc/groupmgr.h"
#include "pgxc/pgxc.h"
/* PGXC_COORD */
#include "access/gtm.h"
#include "utils/memutils.h"
#include "pgxc/remoteCombiner.h"
#include "pgxc/execRemote.h"
#include "pgxc/pgxcXact.h"
#include "libpq/pqformat.h"
#include "libpq/libpq.h"

#endif

template<typename T_Form>
static T_Form read_seq_tuple(SeqTable elm, Relation rel, Buffer* buf, HeapTuple seqtuple, GTM_UUID* uuid);
template<typename T_FormData, typename T_Int, bool large>
static ObjectAddress DefineSequence(CreateSeqStmt* seq);
template<typename T_Form, typename T_Int, bool large>
static ObjectAddress AlterSequence(const AlterSeqStmt* stmt);
#ifdef PGXC
template<typename T_Form, typename T_Int, bool large>
static void init_params(List* options, bool isInit, bool isUseLocalSeq, void* newm_p, List** owned_by,
    bool* is_restart, bool* need_seq_rewrite);
#else
static void init_params(List* options, bool isInit, bool isUseLocalSeq, void* newm_p, List** owned_by,
    bool* need_seq_rewrite);
#endif
template<typename T_Form, typename T_Int, bool large>
static void do_setval(Oid relid, int128 next, bool iscalled);
static void process_owned_by(const Relation seqrel, List* owned_by);
template<typename T_Form>
static GTM_UUID get_uuid_from_tuple(const void* seq_p, const Relation rel, const HeapTuple seqtuple);
extern Oid searchSeqidFromExpr(Node* cooked_default);
extern bool check_contains_tbllike_in_multi_nodegroup(CreateStmt* stmt);
static int64 get_uuid_from_uuids(List** uuids);
static SeqTable InitGlobalSeqElm(Oid relid);
static int64 GetNextvalGlobal(SeqTable sess_elm, Relation seqrel);
template<typename T_Int, typename T_Form, bool large>
static int128 GetNextvalLocal(SeqTable elm, Relation seqrel);
template<typename T_Int, bool large>
static T_Int FetchLogLocal(T_Int* next, T_Int* result, T_Int* last, T_Int maxv, T_Int minv, T_Int fetch,
    T_Int log, T_Int incby, T_Int rescnt, bool is_cycled, T_Int cache, Relation seqrel);
#ifdef ENABLE_MULTIPLE_NODES
static void sendUpdateSequenceMsgToDn(List* dbname, List* schemaname, List* seqname, List* last_value);
static void CheckUpdateSequenceMsgStatus(PGXCNodeHandle* exec_handle, const char* seqname, const int64 last_value);
#endif
static void updateNextValForSequence(Buffer buf, Form_pg_sequence seq, HeapTupleData seqtuple, Relation seqrel,
                                    int64 result);
static int64 GetNextvalResult(SeqTable sess_elm, Relation seqrel, Form_pg_sequence seq, HeapTupleData seqtuple,
    Buffer buf, int64* rangemax, SeqTable elm, GTM_UUID uuid);
template<typename T_Int, bool large>
static char* Int8or16Out(T_Int num);
/*
 * Sequence concurrent improvements
 *
 * - SEQUENCE_GTM_RETRY_SLEEPING_TIME
 *		Intro: Interval of sleeping times if use a shot-GTM connection mechanism to get SeqNo
 *
 * - gtm_conn_lock
 *		Intro: GaussDB proc share variable that indicates that only current DN is allowed
 *		to connect to GTM.
 */
const long SEQUENCE_GTM_RETRY_SLEEPING_TIME = 10000L;
static pthread_mutex_t gtm_conn_lock;

/*
 * get_uuid_from_uuids
 * 		take out one uuid from uuid list, and delete the cell we have got.
 */
static int64 get_uuid_from_uuids(List** uuids)
{
    Const* n = (Const*)linitial(*uuids);
    int64 id = DatumGetInt64(n->constvalue);

    *uuids = list_delete(*uuids, n);

    return id;
}

/*
 * gen_uuid
 * 		get uuid from GTM or from uuid list
 */
int64 gen_uuid(List* uuids)
{
    if (IS_MAIN_COORDINATOR && uuids == NIL) {
        return (int64)GetSeqUUIDGTM();
    }

    if (isRestoreMode && uuids == NIL) {
        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_NOT_SUPPORTED),
                errmsg("uuids can not be NIL when generating uuid in restore mode.")));
    }

    Assert(uuids != NIL);
    return get_uuid_from_uuids(&uuids);
}

/*
 * gen_hybirdmsg_for_CreateSchemaStmt
 * 		traverse the schemaElts, gen uuid, and package the uuid into hybird message.
 *
 * 		we shoud keep the uuid in order. Keep uuids used by CreateSeqStmt in the front of the
 * List, and uuids used by CreateStmt after them.
 */
char* gen_hybirdmsg_for_CreateSchemaStmt(CreateSchemaStmt* stmt, const char* queryString)
{
    ListCell* lc = NULL;
    List* uuids = NIL;
    List* seqstmt_uuids = NIL;
    List* createstmt_uuids = NIL;
    char* queryStringwithinfo = (char*)queryString;

    foreach (lc, stmt->schemaElts) {
        switch (nodeTag(lfirst(lc))) {
            case T_CreateSeqStmt: {
                CreateSeqStmt* elp = (CreateSeqStmt*)lfirst(lc);

                elp->uuid = gen_uuid(NIL);

                Const* n = makeConst(INT8OID, -1, InvalidOid, sizeof(int64), Int64GetDatum(elp->uuid), false, true);

                seqstmt_uuids = lappend(seqstmt_uuids, n);
            } break;
            case T_CreateStmt: {
                CreateStmt* elp = (CreateStmt*)lfirst(lc);

                /* Unsupport create table like in multi-nodegroup */
                if (check_contains_tbllike_in_multi_nodegroup(elp)) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("Create table Like in multi-nodegroup is not supported")));
                }

                gen_uuid_for_CreateStmt(elp, NIL);

                List* cur_uuids = (List*)copyObject(elp->uuids);
                createstmt_uuids = list_concat(createstmt_uuids, cur_uuids);
            } break;
            default:
                break;
        }
    }

    uuids = list_concat(seqstmt_uuids, createstmt_uuids);

    if (uuids != NULL) {
        char* uuidinfo = nodeToString(uuids);

        /* The 'create table' statement will be sent as Hybird Message with uuids for sequence. */
        AssembleHybridMessage(&queryStringwithinfo, queryString, uuidinfo);
    }
    list_free_deep(uuids);
    return queryStringwithinfo;
}

/*
 * gen_hybirdmsg_for_CreateSeqStmt
 * 		gen uuid and set uuid to stmt, package it into hybird message
 * 		the routine work for standard_ProcessUtility when stmt is CreateSeqStmt.
 */
char* gen_hybirdmsg_for_CreateSeqStmt(CreateSeqStmt* stmt, const char* queryString)
{
    char* queryStringWithUUID = NULL;

    /*
     * Only the CN that receiving "create sequence" statement need get uuid,
     * the uuid will be send to other node.
     */
    if (IS_MAIN_COORDINATOR && !stmt->is_serial && stmt->uuid == INVALIDSEQUUID) {
        stmt->uuid = (int64)GetSeqUUIDGTM();

        Const* n = makeConst(INT8OID, -1, InvalidOid, sizeof(int64), Int64GetDatum(stmt->uuid), false, true);
        char* uuidinfo = nodeToString(n);

        AssembleHybridMessage(&queryStringWithUUID, queryString, uuidinfo);
    }

    return queryStringWithUUID;
}

static bool TypenameIsSerial(const char* typname)
{
    if (strcmp(typname, "smallserial") == 0 || strcmp(typname, "serial2") == 0 || strcmp(typname, "serial") == 0 ||
        strcmp(typname, "serial4") == 0 || strcmp(typname, "bigserial") == 0 || strcmp(typname, "serial8") == 0 ||
        strcmp(typname, "largeserial") == 0 || strcmp(typname, "serial16") == 0)
        return true;

    return false;
}

/*
 * gen_uuid_for_CreateStmt
 * 		Traverse the tableElts, if the node is serial type, need gen uuid.
 * 		If the node is TableLikeClause, gen uuid when the parent table contains
 * 		serial type.
 *
 * @in stmt: the CreateStmt we set.
 * @in uuids: If it is NULL, get uuid from GTM, else get uuid from the uuids.
 */
void gen_uuid_for_CreateStmt(CreateStmt* stmt, List* uuids)
{
    ListCell* elements = NULL;

    foreach (elements, stmt->tableElts) {
        Node* element = (Node*)lfirst(elements);

        switch (nodeTag(element)) {
            case T_ColumnDef: {
                ColumnDef* column = (ColumnDef*)element;

                if (column->typname && list_length(column->typname->names) == 1 && !column->typname->pct_type) {
                    char* typname = strVal(linitial(column->typname->names));
                    if (TypenameIsSerial(typname)) {
                        int64 uuid = gen_uuid(uuids);

                        Const* n = makeConst(INT8OID, -1, InvalidOid, sizeof(int64), Int64GetDatum(uuid), false, true);

                        stmt->uuids = lappend(stmt->uuids, n);
                    }
                }
                break;
            }
            case T_TableLikeClause: {
                TupleConstr* constr = NULL;
                Relation relation = NULL;
                TupleDesc tupleDesc;
                TableLikeClause* table_like_clause = (TableLikeClause*)element;

                relation = relation_openrv(table_like_clause->relation, AccessShareLock);
                tupleDesc = RelationGetDescr(relation);
                constr = tupleDesc->constr;

                for (int parent_attno = 1; parent_attno <= tupleDesc->natts; parent_attno++) {
                    Form_pg_attribute attribute = &tupleDesc->attrs[parent_attno - 1];

                    if (attribute->attisdropped && !u_sess->attr.attr_sql.enable_cluster_resize)
                        continue;

                    if (attribute->atthasdef) {
                        Oid seqId = InvalidOid;
                        Node* this_default = NULL;
                        AttrDefault* attrdef = NULL;
                        attrdef = constr->defval;

                        for (int i = 0; i < constr->num_defval; i++) {
                            if (attrdef[i].adnum == parent_attno) {
                                this_default = (Node*)stringToNode_skip_extern_fields(attrdef[i].adbin);
                                break;
                            }
                        }

                        seqId = searchSeqidFromExpr(this_default);
                        if (OidIsValid(seqId)) {
                            List* seqs = getOwnedSequences(relation->rd_id);
                            if (seqs != NULL && list_member_oid(seqs, DatumGetObjectId(seqId))) {
                                int64 uuid = gen_uuid(uuids);

                                Const* n =
                                    makeConst(INT8OID, -1, InvalidOid, sizeof(int64), Int64GetDatum(uuid), false, true);

                                stmt->uuids = lappend(stmt->uuids, n);
                            }
                        }
                    }
                }

                heap_close(relation, AccessShareLock);

                break;
            }
            default:
                break;
        }
    }

    return;
}

/*
 * gen_uuid_for_CreateSchemaStmt
 * 		Traverse the stmts, if the node is CreateSeqStmt, need gen uuid.
 * 		if the node is CreateStmt, gen uuid for CreateStmt(see above).
 *
 * 		the routine work for CreateSchemaCommand when it is not MAIN CN, we
 * 		shoud assgin uuid from the uuids.
 * 		Because we send the uuids from MAIN CN in order, so now, we assgin
 * 		the uuid in the same order, CreateSeqStmt first, and then CreateStmt.
 *
 * @in stmts: the Stmt we traverse, only care about CreateSeqStmt and CreateStmt
 * @in uuids: the list of uuid where we take out uuid.
 */
void gen_uuid_for_CreateSchemaStmt(List* stmts, List* uuids)
{
    ListCell* elements = NULL;
    foreach (elements, stmts) {
        Node* element = (Node*)lfirst(elements);

        if (IsA(element, CreateSeqStmt)) {
            CreateSeqStmt* elp = (CreateSeqStmt*)element;
            elp->uuid = gen_uuid(uuids);
        }
    }

    foreach (elements, stmts) {
        Node* element = (Node*)lfirst(elements);

        if (IsA(element, CreateStmt)) {
            CreateStmt* elp = (CreateStmt*)element;
            gen_uuid_for_CreateStmt(elp, uuids);
        }
    }

    return;
}

void InitGlobalSeq()
{
    for (int i = 0; i < NUM_GS_PARTITIONS; i++) {
        g_instance.global_seq[i].shb_list = NULL;
        g_instance.global_seq[i].lock_id = FirstGlobalSeqLock + i;
    }
}

static SeqTable InitGlobalSeqElm(Oid relid)
{
    SeqTable elm = NULL;
    elm = (SeqTable)MemoryContextAlloc(
        INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER), sizeof(SeqTableData));
    errno_t rc = EOK;
    rc = memset_s(elm, sizeof(SeqTableData), 0, sizeof(SeqTableData));
    securec_check(rc, "\0", "\0");
    elm->relid = relid;
    return elm;
}

template<typename T_Int, bool large>
static void AssignInt(T_Int* elem, int128 value)
{
    if (large) {
        int128 holder = (int128)value;
        errno_t rc = memcpy_s(elem, sizeof(int128), &holder, sizeof(int128));
        securec_check(rc, "\0", "\0");
    } else {
        /* known that it is safe to truncate */
        *elem = (int64)value;
    }
}

static int64 GetNextvalGlobal(SeqTable sess_elm, Relation seqrel)
{
    SeqTable elm = NULL;
    GlobalSeqInfoHashBucket* bucket = NULL;
    Buffer buf;
    Page page;
    HeapTupleData seqtuple;
    Form_pg_sequence seq;
    GTM_UUID uuid;
    int64 cache;
    int64 log;
    int64 fetch;
    int64 last = 0;
    int64 result;
    int64 sesscache;
    int64 range;
    int64 rangemax;
    uint32 hash = 0;
    hash = RelidGetHash(sess_elm->relid);
    /* guc */
    sesscache = u_sess->attr.attr_common.session_sequence_cache;
    bucket = &g_instance.global_seq[hash];

    (void)LWLockAcquire(GetMainLWLockByIndex(bucket->lock_id), LW_EXCLUSIVE);

    elm = GetGlobalSeqElm(sess_elm->relid, bucket);

    if (elm == NULL) {
        elm = InitGlobalSeqElm(sess_elm->relid);
        /* add new seqence in bucket */
        MemoryContext oldcontext = MemoryContextSwitchTo(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));
        bucket->shb_list = dlappend(bucket->shb_list, elm);
        (void)MemoryContextSwitchTo(oldcontext);
    }

    /* If sequence relation is changed */
    if (seqrel->rd_rel->relfilenode != elm->filenode) {
        elm->filenode = seqrel->rd_rel->relfilenode;
        AssignInt<int128, true>(&(elm->cached), elm->last);
    }
    /* If sess_elm first apply from global sequence,
     * should update sess_elm infomation
     */
    if (!sess_elm->last_valid) {
        AssignInt<int128, true>(&(sess_elm->increment), (int128)elm->increment);
        AssignInt<int128, true>(&(sess_elm->minval), (int128)elm->minval);
        AssignInt<int128, true>(&(sess_elm->maxval), (int128)elm->maxval);
        AssignInt<int128, true>(&(sess_elm->startval), (int128)elm->startval);
        sess_elm->is_cycled = elm->is_cycled;
        sess_elm->uuid = elm->uuid;
    }

    if (elm->last != elm->cached) {
        result = elm->last + elm->increment;
        AssignInt<int128, true>(&(sess_elm->last), (int128)result);
        /* if session cache value <= current cached value */
        if (elm->last + sesscache * elm->increment <= elm->cached) {
            AssignInt<int128, true>(&(elm->last), (int128)(elm->last + sesscache * elm->increment));
            AssignInt<int128, true>(&(sess_elm->cached), elm->last);
        } else {
            AssignInt<int128, true>(&(elm->last), (int128)elm->cached);
            AssignInt<int128, true>(&(sess_elm->cached), (int128)elm->last);
        }
        sess_elm->last_valid = true;
        LWLockRelease(GetMainLWLockByIndex(bucket->lock_id)); 
        return result;
    }

    /* lock page' buffer and read tuple */
    seq = read_seq_tuple<Form_pg_sequence>(elm, seqrel, &buf, &seqtuple, &uuid);
    /* update session sequcence info */
    AssignInt<int128, true>(&(sess_elm->increment), (int128)elm->increment);
    AssignInt<int128, true>(&(sess_elm->minval), (int128)elm->minval);
    AssignInt<int128, true>(&(sess_elm->maxval), (int128)elm->maxval);
    AssignInt<int128, true>(&(sess_elm->startval), (int128)elm->startval);
    sess_elm->is_cycled = elm->is_cycled;
    sess_elm->uuid = elm->uuid;

    page = BufferGetPage(buf);

    /*
     * Above, we still use the page as a locking mechanism to handle
     * concurrency
     */
    Assert(!IS_SINGLE_NODE);

    range = seq->cache_value; /* how many values to ask from GTM? */
    rangemax = 0;             /* the max value returned from the GTM for our request */
    result = GetNextvalResult(sess_elm, seqrel, seq, seqtuple, buf, &rangemax, elm, uuid);

    /* Update the on-disk data */
    seq->last_value = result; /* disable the unreliable last_value */
    seq->is_called = true;

    if (sesscache < range) {
        int64 tmpcached;
        /* save info in session cache */
        AssignInt<int128, true>(&(sess_elm->last), (int128)result);
        tmpcached = result + (sesscache - 1) * sess_elm->increment;
        tmpcached = (tmpcached <= rangemax) ? tmpcached : rangemax;
        AssignInt<int128, true>(&(sess_elm->cached), (int128)tmpcached);
        sess_elm->last_valid = true; 
        /* save info in global cache */
        AssignInt<int128, true>(&(elm->last), (int128)sess_elm->cached);
        AssignInt<int128, true>(&(elm->cached), (int128)rangemax); /* last fetched number */
    } else {
        /* save info in session cache */
        AssignInt<int128, true>(&(sess_elm->last), (int128)result);      /* last returned number */
        AssignInt<int128, true>(&(sess_elm->cached), (int128)rangemax);  /* last fetched number */
        sess_elm->last_valid = true;
        /* save info in global cache */
        AssignInt<int128, true>(&(elm->cached), (int128)sess_elm->cached);
        AssignInt<int128, true>(&(elm->last), (int128)elm->cached);
    }
    elm->last_valid = true;

    LWLockRelease(GetMainLWLockByIndex(bucket->lock_id));

    /* keep invalid for last value in distributed mode */
    last = seq->last_value;

    fetch = cache = seq->cache_value;
    log = seq->log_cnt;

    /*
     * Decide whether we should emit a WAL log record.	If so, force up the
     * fetch count to grab SEQ_LOG_VALS more values than we actually need to
     * cache.  (These will then be usable without logging.)
     *
     * If this is the first nextval after a checkpoint, we must force a new
     * WAL record to be written anyway, else replay starting from the
     * checkpoint would fail to advance the sequence past the logged values.
     * In this case we may as well fetch extra values.
     */
    if (log < fetch || !seq->is_called) {
        /* forced log to satisfy local demand for values */
        log = fetch + SEQ_LOG_VALS;
    } else {
        XLogRecPtr redoptr = GetRedoRecPtr();

        if (XLByteLE(PageGetLSN(page), redoptr)) {
            /* last update of seq was before checkpoint */
            log = fetch + SEQ_LOG_VALS;
        }
    }

    log  -= cache;
    Assert(log >= 0);

    updateNextValForSequence(buf, seq, seqtuple, seqrel, result);
    return result;
}

template<typename T_Int, typename T_Form, bool large>
static int128 GetNextvalLocal(SeqTable elm, Relation seqrel)
{
    Buffer buf;
    Page page;
    HeapTupleData seqtuple;
    T_Form seq;
    GTM_UUID uuid;
    T_Int incby = 0;
    T_Int maxv = 0;
    T_Int minv = 0;
    T_Int cache = 0;
    T_Int log = 0;
    T_Int fetch = 0;
    T_Int last = 0;
    T_Int result;
    T_Int next = 0;
    T_Int rescnt = 0;
    bool logit = false;

    /* lock page' buffer and read tuple */
    seq = read_seq_tuple<T_Form>(elm, seqrel, &buf, &seqtuple, &uuid);
    page = BufferGetPage(buf);

    AssignInt<T_Int, large>(&last, (int128)seq->last_value);
    AssignInt<T_Int, large>(&next, (int128)seq->last_value);
    AssignInt<T_Int, large>(&result, (int128)seq->last_value);
    AssignInt<T_Int, large>(&incby, (int128)seq->increment_by);
    AssignInt<T_Int, large>(&maxv, (int128)seq->max_value);
    AssignInt<T_Int, large>(&minv, (int128)seq->min_value);
    AssignInt<T_Int, large>(&fetch, (int128)seq->cache_value);
    AssignInt<T_Int, large>(&cache, (int128)seq->cache_value);
    AssignInt<T_Int, large>(&log, (int128)seq->log_cnt);

    if (!seq->is_called) {
        rescnt++; /* return last_value if not is_called */
        fetch--;
    }

    if (log < fetch || !seq->is_called) {
        /* forced log to satisfy local demand for values */
        fetch = log = fetch + SEQ_LOG_VALS;
        logit = true;
    } else {
        XLogRecPtr redoptr = GetRedoRecPtr();

        if (XLByteLE(PageGetLSN(page), redoptr)) {
            /* last update of seq was before checkpoint */
            fetch = log = fetch + SEQ_LOG_VALS;
            logit = true;
        }
    }

    log = FetchLogLocal<T_Int, large>(&next, &result, &last, maxv, minv, fetch, log, incby,
        rescnt, seq->is_cycled, cache, seqrel);
    /* Save info in local cache for temporary sequences */
    AssignInt<int128, true>(&(elm->last), (int128)result); /* last returned number */
    AssignInt<int128, true>(&(elm->cached), (int128)last); /* last fetched number */
    elm->last_valid = true;

    /* ready to change the on-disk (or really, in-buffer) tuple */
    START_CRIT_SECTION();

    MarkBufferDirty(buf);

    /* XLOG stuff, only single-node need WAL */
    if (logit && RelationNeedsWAL(seqrel)) {
        xl_seq_rec xlrec;
        XLogRecPtr recptr;

        /*
         * We don't log the current state of the tuple, but rather the state
         * as it would appear after "log" more fetches.  This lets us skip
         * that many future WAL records, at the cost that we lose those
         * sequence values if we crash.
         */
        /* set values that will be saved in xlog */
        AssignInt<T_Int, large>(&(seq->last_value), (int128)next);
        seq->is_called = true;
        seq->log_cnt = 0;

        RelFileNodeRelCopy(xlrec.node, seqrel->rd_node);

        XLogBeginInsert();
        XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);
        XLogRegisterData((char*)&xlrec, sizeof(xl_seq_rec));
        XLogRegisterData((char*)seqtuple.t_data, seqtuple.t_len);

        recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG, seqrel->rd_node.bucketNode);

        PageSetLSN(page, recptr);
    }

    /* Now update sequence tuple to the intended final state */
    AssignInt<T_Int, large>(&(seq->last_value), (int128)last); /* last fetched number */
    seq->is_called = true;
    seq->log_cnt = log; /* how much is logged */

    END_CRIT_SECTION();

    UnlockReleaseBuffer(buf);

    return result;
}

template<typename T_Int>
static bool FetchNOverMaxBound(T_Int maxv, T_Int next, T_Int incby)
{
    return (maxv >= 0 && next > maxv - incby) || (maxv < 0 && next + incby > maxv);
}

template<typename T_Int>
static bool FetchNOverMinBound(T_Int minv, T_Int next, T_Int incby)
{
    return (minv < 0 && next < minv - incby) || (minv >= 0 && next + incby < minv);
}

/*
 * Try to fetch cache [+ log ] numbers
 */
template<typename T_Int, bool large>
static T_Int FetchLogLocal(T_Int* next, T_Int* result, T_Int* last, T_Int maxv, T_Int minv, T_Int fetch,
    T_Int log, T_Int incby, T_Int rescnt, bool is_cycled, T_Int cache, Relation seqrel)
{
    while (fetch) {
        /* Result has been checked and received from GTM */
        if (incby > 0) {
            /* ascending sequence */
            if (FetchNOverMaxBound(maxv, *next, incby)) {
                if (rescnt > 0) {
                    break; /* stop fetching */
                }
                if (!is_cycled) {
                    char* tmp_buf = Int8or16Out<T_Int, large>(maxv);
                    ereport(ERROR,
                        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                            errmsg("nextval: reached maximum value of sequence \"%s\" (%s)",
                                RelationGetRelationName(seqrel),
                                tmp_buf)));
                }
                *next = minv;
            } else {
                *next += incby;
            }
        } else {
            /* descending sequence */
            if (FetchNOverMinBound(minv, *next, incby)) {
                if (rescnt > 0) {
                    break; /* stop fetching */
                }
                if (!is_cycled) {
                    char* tmp_buf = large ? DatumGetCString(DirectFunctionCall1(int16out, Int128GetDatum(minv))) :
                        DatumGetCString(DirectFunctionCall1(int8out, minv));
                    ereport(ERROR,
                        (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                            errmsg("nextval: reached minimum value of sequence \"%s\" (%s)",
                                RelationGetRelationName(seqrel),
                                tmp_buf)));
                }
                *next = maxv;
            } else {
                *next += incby;
            }
        }

        fetch--;
        if (rescnt < cache) {
            log--;
            rescnt++;
            /*
             * This part is not taken into account,
             * result has been received from GTM
             */
            *last = *next;
            if (rescnt == 1) {  /* if it's first result - */
                *result = *next; /* it's what to return */
            }
        }
        /*
         * If cache is set to a large value, it will cycle here for a long time.
         * So provide a way to break this loop.
         */
        if (t_thrd.int_cxt.ProcDiePending || t_thrd.int_cxt.QueryCancelPending) {
            break;
        }
    }
    log -= fetch; /* adjust for any unfetched numbers */
    Assert(log >= 0);

    return log;
}

template<typename T, bool large>
static Datum GetIntDefVal(TypeName* name, T value)
{
    if (large) {
        *name = makeTypeNameFromOid(INT16OID, -1);
        return Int128GetDatum(value);
    } else {
        *name = makeTypeNameFromOid(INT8OID, -1);
        return Int64GetDatumFast(value);
    }
}

ObjectAddress DefineSequenceWrapper(CreateSeqStmt* seq)
{
    if (seq->is_large) {
        if (t_thrd.proc->workingVersionNum < LARGE_SEQUENCE_VERSION_NUM) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("It is not supported to create large sequence during upgrade.")));
        }
        return DefineSequence<FormData_pg_large_sequence, int128, true>(seq);
    } else {
        return DefineSequence<FormData_pg_sequence, int64, false>(seq);
    }
}

template<typename T_Int, bool large>
static Datum Int8or16GetDatum(T_Int value)
{
    if (large) {
        return Int128GetDatum(value);
    } else {
        return Int64GetDatumFast(value);
    }
}

template<typename T_FormData>
static int CreateSequenceWithUUIDGTMWrapper(T_FormData newm, int64 uuid);

template<>
int CreateSequenceWithUUIDGTMWrapper(FormData_pg_sequence newm, int64 uuid)
{
    return CreateSequenceWithUUIDGTM(newm, uuid);
}

template<>
int CreateSequenceWithUUIDGTMWrapper(FormData_pg_large_sequence newm, int64 uuid)
{
    return 0; /* large sequence is ignored by distribution logic */
}

/*
 * DefineSequence
 *				Creates a new sequence relation
 */
template<typename T_FormData, typename T_Int, bool large>
static ObjectAddress DefineSequence(CreateSeqStmt* seq)
{
    T_FormData newm;
    List* owned_by = NIL;
    CreateStmt* stmt = makeNode(CreateStmt);
    Oid seqoid;
    Relation rel;
    HeapTuple tuple;
    TupleDesc tupDesc;
    Datum value[SEQ_COL_LASTCOL];
    bool null[SEQ_COL_LASTCOL];
    int i;
    NameData name;
    bool need_seq_rewrite = false;
    bool isUseLocalSeq = false;
    Oid namespaceOid = InvalidOid;
    ObjectAddress address;
    Oid existing_relid = InvalidOid;
    Oid namespaceId = InvalidOid;
    char rel_kind = large ? RELKIND_LARGE_SEQUENCE : RELKIND_SEQUENCE;

    if (seq->missing_ok) {
        namespaceId = RangeVarGetAndCheckCreationNamespace(seq->sequence, NoLock, &existing_relid, rel_kind);
        if (existing_relid != InvalidOid) {
            char* namespace_of_existing_rel = get_namespace_name(namespaceId);
            ereport(NOTICE, (errmodule(MOD_COMMAND), errmsg("relation \"%s\" already exists in schema \"%s\", skipping", seq->sequence->relname, namespace_of_existing_rel)));
            return InvalidObjectAddress;
        }
    }

#ifdef PGXC /* PGXC_COORD */
    GTM_Sequence start_value = 1;
    GTM_Sequence min_value = 1;
    GTM_Sequence max_value = MaxSequenceValue;
    GTM_Sequence increment = 1;
    bool cycle = false;
    bool is_restart = false;
#endif
    if (seq->sequence->schemaname != NULL) {
        namespaceOid = get_namespace_oid(seq->sequence->schemaname, true);
    }
    /* temp sequence and single_node do not need gtm, they use information on local node */
#ifdef ENABLE_MUTIPLE_NODES
    isUseLocalSeq = IS_SINGLE_NODE || isTempNamespace(namespaceOid);
#else
    isUseLocalSeq = true;
#endif

    bool notSupportTmpSeq = false;
    if (seq->sequence->relpersistence == RELPERSISTENCE_TEMP ||
        seq->sequence->relpersistence == RELPERSISTENCE_GLOBAL_TEMP) {
        notSupportTmpSeq = true;
    } else if (IS_MAIN_COORDINATOR || IS_SINGLE_NODE) {
        if (seq->canCreateTempSeq) {
            /*
             * If canCreateTempSeq is true, we allow to create temp sequence,
             * and we only need test this on main cn because canCreateTempSeq
             * is valid only on main cn. See createSeqOwnedByTable for more details.
             */
            notSupportTmpSeq = false;
        } else if (seq->sequence->schemaname != NULL) {
            notSupportTmpSeq =
                strcmp(seq->sequence->schemaname, "pg_temp") == 0 || isTempOrToastNamespace(namespaceOid);
        }
    }

    /* Unlogged sequences are not implemented -- not clear if useful. */
    if (seq->sequence->relpersistence == RELPERSISTENCE_UNLOGGED)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("unlogged sequences are not supported")));

    if (notSupportTmpSeq)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Temporary sequences are not supported")));
#ifdef ENABLE_MULTIPLE_NODES
    if (!IS_SINGLE_NODE && seq->uuid == INVALIDSEQUUID)
        ereport(ERROR,
            (errcode(ERRCODE_DATA_CORRUPTED), errmsg("Invaild UUID for CREATE SEQUENCE %s.", seq->sequence->relname)));
#endif

    /* Check and set all option values */
#ifdef PGXC
    if (large) {
        init_params<Form_pg_large_sequence, int128, true>(seq->options, true, isUseLocalSeq, &newm, &owned_by,
            &is_restart, &need_seq_rewrite);
    } else {
        init_params<Form_pg_sequence, int64, false>(seq->options, true, isUseLocalSeq, &newm, &owned_by,
            &is_restart, &need_seq_rewrite);
    }
#else
    if (large) {
        init_params<Form_pg_large_sequence, int128, true>(seq->options, true, isUseLocalSeq, &newm, &owned_by,
            &need_seq_rewrite);
    } else {
        init_params<Form_pg_sequence, int64, false>(seq->options, true, isUseLocalSeq, &newm, &owned_by,
            &need_seq_rewrite);
    }
#endif
    /*
     * Create relation (and fill value[] and null[] for the tuple)
     */
    stmt->tableElts = NIL;
    Oid intTypeOid = large ? INT16OID : INT8OID;
    for (i = SEQ_COL_FIRSTCOL; i <= SEQ_COL_LASTCOL; i++) {
        ColumnDef* coldef = makeNode(ColumnDef);

        coldef->inhcount = 0;
        coldef->is_local = true;
        coldef->is_not_null = true;
        coldef->is_from_type = false;
        coldef->storage = 0;
        /* each tuple within SEQ relation is independent,so don't set the compress method. */
        coldef->cmprs_mode = ATT_CMPR_UNDEFINED;
        coldef->raw_default = NULL;
        coldef->cooked_default = NULL;
        coldef->collClause = NULL;
        coldef->collOid = InvalidOid;
        coldef->constraints = NIL;

        null[i - 1] = false;

        switch (i) {
            case SEQ_COL_NAME:
                coldef->typname = makeTypeNameFromOid(NAMEOID, -1);
                coldef->colname = "sequence_name";
                (void)namestrcpy(&name, seq->sequence->relname);
                value[i - 1] = NameGetDatum(&name);
                break;
            case SEQ_COL_LASTVAL:
                coldef->typname = makeTypeNameFromOid(intTypeOid, -1);
                coldef->colname = "last_value";
                value[i - 1] = Int8or16GetDatum<T_Int, large>(newm.last_value);
                break;
            case SEQ_COL_STARTVAL:
                coldef->typname = makeTypeNameFromOid(intTypeOid, -1);
                coldef->colname = "start_value";
                value[i - 1] = Int8or16GetDatum<T_Int, large>(newm.start_value);
#ifdef PGXC /* PGXC_COORD */
                start_value = newm.start_value;
#endif
                break;
            case SEQ_COL_INCBY:
                coldef->typname = makeTypeNameFromOid(intTypeOid, -1);
                coldef->colname = "increment_by";
                value[i - 1] = Int8or16GetDatum<T_Int, large>(newm.increment_by);
#ifdef PGXC /* PGXC_COORD */
                increment = newm.increment_by;
#endif
                break;
            case SEQ_COL_MAXVALUE:
                coldef->typname = makeTypeNameFromOid(intTypeOid, -1);
                coldef->colname = "max_value";
                value[i - 1] = Int8or16GetDatum<T_Int, large>(newm.max_value);
#ifdef PGXC /* PGXC_COORD */
                max_value = newm.max_value;
#endif
                break;
            case SEQ_COL_MINVALUE:
                coldef->typname = makeTypeNameFromOid(intTypeOid, -1);
                coldef->colname = "min_value";
                value[i - 1] = Int8or16GetDatum<T_Int, large>(newm.min_value);
#ifdef PGXC /* PGXC_COORD */
                min_value = newm.min_value;
#endif
                break;
            case SEQ_COL_CACHE:
                coldef->typname = makeTypeNameFromOid(intTypeOid, -1);
                coldef->colname = "cache_value";
                value[i - 1] = Int8or16GetDatum<T_Int, large>(newm.cache_value);
                break;
            case SEQ_COL_LOG:
                coldef->typname = makeTypeNameFromOid(INT8OID, -1);
                coldef->colname = "log_cnt";
                value[i - 1] = Int64GetDatum((int64)0);
                break;
            case SEQ_COL_CYCLE:
                coldef->typname = makeTypeNameFromOid(BOOLOID, -1);
                coldef->colname = "is_cycled";
                value[i - 1] = BoolGetDatum(newm.is_cycled);
#ifdef PGXC /* PGXC_COORD */
                cycle = newm.is_cycled;
#endif
                break;
            case SEQ_COL_CALLED:
                coldef->typname = makeTypeNameFromOid(BOOLOID, -1);
                coldef->colname = "is_called";
                value[i - 1] = BoolGetDatum(false);
                break;
            case SEQ_COL_UUID:
                coldef->typname = makeTypeNameFromOid(INT8OID, -1);
                coldef->colname = "uuid";
                value[i - 1] = Int64GetDatum(seq->uuid);

                break;
            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized sequence columns: %d", i)));
        }
        stmt->tableElts = lappend(stmt->tableElts, coldef);
    }

    stmt->relation = seq->sequence;
    stmt->inhRelations = NIL;
    stmt->constraints = NIL;
    stmt->options = list_make1(defWithOids(false));
    stmt->oncommit = ONCOMMIT_NOOP;
    stmt->tablespacename = NULL;
    stmt->if_not_exists = false;
    stmt->charset = PG_INVALID_ENCODING;
    address = DefineRelation(stmt, rel_kind, seq->ownerId, NULL);
    seqoid = address.objectId;
    Assert(seqoid != InvalidOid);

    rel = heap_open(seqoid, AccessExclusiveLock);
    tupDesc = RelationGetDescr(rel);

    /* now initialize the sequence's data */
    tuple = (HeapTuple)heap_form_tuple(tupDesc, value, null);
    fill_seq_with_data(rel, tuple);

    /* process OWNED BY if given */
    if (owned_by != NIL)
        process_owned_by(rel, owned_by);

    heap_close(rel, NoLock);

#ifdef PGXC /* PGXC_COORD */
    /*
     * Remote Coordinator is in charge of creating sequence in GTM.
     * If sequence is temporary, it is not necessary to create it on GTM.
     */
    if (IS_PGXC_COORDINATOR && !IsConnFromCoord() &&
        (seq->sequence->relpersistence == RELPERSISTENCE_PERMANENT ||
            seq->sequence->relpersistence == RELPERSISTENCE_UNLOGGED)) {
        int status;
        int seqerrcode = ERRCODE_SQL_STATEMENT_NOT_YET_COMPLETE;

        /* We also need to create it on the GTM */
        if ((status = CreateSequenceWithUUIDGTMWrapper<T_FormData>(newm, seq->uuid)) < 0) {
            if (status == GTM_RESULT_COMM_ERROR)
                seqerrcode = ERRCODE_CONNECTION_FAILURE;
            ereport(ERROR, (errcode(seqerrcode), errmsg("GTM error, could not create sequence")));
        }
        /* Define a callback to drop sequence on GTM in case transaction fails  */
        register_sequence_cb(seq->uuid, GTM_CREATE_SEQ);
    }
#endif
    return address;
}

template<typename T_Form>
static HeapTuple ResetSequenceTuple(Relation seq_rel, SeqTable elm, bool restart)
{
    T_Form seq = NULL;
    Buffer buf;
    HeapTupleData seqtuple;
    HeapTuple result;
    GTM_UUID uuid;
    errno_t rc = memset_s(&seqtuple, sizeof(seqtuple), 0, sizeof(seqtuple));
    securec_check_c(rc, "\0", "\0");
    seqtuple.tupTableType = HEAP_TUPLE;

    (void)read_seq_tuple<T_Form>(elm, seq_rel, &buf, &seqtuple, &uuid);

    /*
     * Copy the existing sequence tuple.
     */
    result = (HeapTuple)tableam_tops_copy_tuple(&seqtuple);

    /* Now we're done with the old page */
    UnlockReleaseBuffer(buf);

    /*
     * Modify the copied tuple to execute the restart (compare the RESTART
     * action in AlterSequence)
     */
    seq = (T_Form)GETSTRUCT(result);
    seq->last_value = restart ? seq->min_value : -1; /* if restart, set a valid last_value */
    seq->is_called = false;
    seq->log_cnt = 0;

    return result;
}

/*
 * Reset a sequence to its initial value.
 *
 * The change is made transactionally, so that on failure of the current
 * transaction, the sequence will be restored to its previous state.
 * We do that by creating a whole new relfilenode for the sequence; so this
 * works much like the rewriting forms of ALTER TABLE.
 *
 * Caller is assumed to have acquired AccessExclusiveLock on the sequence,
 * which must not be released until end of transaction.  Caller is also
 * responsible for permissions checking.
 */
void ResetSequence(Oid seq_relid, bool restart)
{
    Relation seq_rel;
    SeqTable elm = NULL;
    HeapTuple tuple;

    /*
     * Read the old sequence.  This does a bit more work than really
     * necessary, but it's simple, and we do want to double-check that it's
     * indeed a sequence.
     */
    init_sequence(seq_relid, &elm, &seq_rel);
    if (RelationGetRelkind(seq_rel) == RELKIND_SEQUENCE) {
        tuple = ResetSequenceTuple<Form_pg_sequence>(seq_rel, elm, restart);
    } else {
        tuple = ResetSequenceTuple<Form_pg_large_sequence>(seq_rel, elm, restart);
    }

    /*
     * Create a new storage file for the sequence.	We want to keep the
     * sequence's relfrozenxid at 0, since it won't contain any unfrozen XIDs.
     */
    RelationSetNewRelfilenode(seq_rel, InvalidTransactionId, InvalidMultiXactId);

    /*
     * Insert the modified tuple into the new storage file.
     */
    fill_seq_with_data(seq_rel, tuple);

    /* Clear local cache so that we don't think we have cached numbers */
    /* Note that we do not change the currval() state */
    AssignInt<int128, true>(&(elm->cached), elm->last);
    if (restart) {
        elm->last_valid = false;
    }

    relation_close(seq_rel, NoLock);
}

ObjectAddress AlterSequenceWrapper(AlterSeqStmt* stmt)
{
    if (stmt->is_large) {
        return AlterSequence<Form_pg_large_sequence, int128, true>(stmt);
    } else {
        return AlterSequence<Form_pg_sequence, int64, false>(stmt);
    }
}

bool CheckSeqOwnedByAutoInc(Oid seqoid)
{
    Oid relid;
    int32 attrnum;
    Relation rel;
    if (!DB_IS_CMPT(B_FORMAT)) {
        return false;
    }
    if (sequenceIsOwned(seqoid, &relid, &attrnum)) {
        rel = relation_open(relid, NoLock);
        if (seqoid == RelAutoIncSeqOid(rel)) {
            relation_close(rel, NoLock);
            return true;
        }
        relation_close(rel, NoLock);
    }
    return false;
}

/*
 * AlterSequence
 *
 * Modify the definition of a sequence relation
 * For now we only support alter sequence owned_by, owner and maxvalue.
 * Alter sequence maxvalue needs update info in GTM.
 */
template<typename T_Form, typename T_Int, bool large>
static ObjectAddress AlterSequence(const AlterSeqStmt* stmt)
{
    Oid relid;
    SeqTable elm = NULL;
    Relation seqrel;
    Buffer buf;
    HeapTupleData seqtuple;
    HeapTuple tuple = NULL;
    T_Form newm = NULL;
    List* owned_by = NIL;
    bool isUseLocalSeq = false;
#ifdef PGXC
    bool is_restart = false;
#endif
    bool need_seq_rewrite = false;
    ObjectAddress address;

    /* Open and lock sequence. */
    relid = RangeVarGetRelid(stmt->sequence, ShareRowExclusiveLock, stmt->missing_ok);
    if (relid == InvalidOid) {
        ereport(NOTICE, (errmsg("relation \"%s\" does not exist, skipping", stmt->sequence->relname)));
        return InvalidObjectAddress;
    }

    TrForbidAccessRbObject(RelationRelationId, relid, stmt->sequence->relname);

    init_sequence(relid, &elm, &seqrel);
    char relkind = RelationGetRelkind(seqrel);
    if (large && relkind == RELKIND_SEQUENCE) {
        ereport(ERROR, (
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("%s is not a large sequence, please use ALTER SEQUENCE instead.", stmt->sequence->relname)));
    } else if (!large && relkind == RELKIND_LARGE_SEQUENCE) {
        ereport(ERROR, (
            errcode(ERRCODE_WRONG_OBJECT_TYPE),
            errmsg("%s is not a sequence, please use ALTER LARGE SEQUENCE instead.", stmt->sequence->relname)));
    }
    if (CheckSeqOwnedByAutoInc(relid)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("cannot alter sequence owned by auto_increment column")));
    }
    /* Must be owner or have alter privilege of the sequence. */
    AclResult aclresult = pg_class_aclcheck(relid, GetUserId(), ACL_ALTER);
    if (aclresult != ACLCHECK_OK && !pg_class_ownercheck(relid, GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_CLASS, stmt->sequence->relname);
    }

    /* temp sequence and single_node do not need gtm, they only use info on local node */
    isUseLocalSeq = RelationIsLocalTemp(seqrel) || IS_SINGLE_NODE;

    /* lock page' buffer and read tuple into new sequence structure */
    GTM_UUID uuid;
    (void)read_seq_tuple<T_Form>(elm, seqrel, &buf, &seqtuple, &uuid);

    /* Copy the existing sequence tuple. */
    tuple = (HeapTuple)tableam_tops_copy_tuple(&seqtuple);
    UnlockReleaseBuffer(buf);

    newm = (T_Form)GETSTRUCT(tuple);

    /* Check and set new values */
#ifdef PGXC
    init_params<T_Form, T_Int, large>(stmt->options, false, isUseLocalSeq, newm, &owned_by,
        &is_restart, &need_seq_rewrite);
#else
    init_params<T_Form, T_Int, large>(stmt->options, false, isUseLocalSeq, newm, &owned_by,
        &need_seq_rewrite);
#endif
#ifdef PGXC /* PGXC_COORD */
    /*
     * Remote Coordinator is in charge of alter sequence in GTM.
     * If sequence is temporary, it is not necessary to alter it on GTM.
     * ALTER OWNED BY is not necessary to alter on GTM..
     */
    if (!isUseLocalSeq && need_seq_rewrite && IS_MAIN_COORDINATOR &&
        (stmt->sequence->relpersistence == RELPERSISTENCE_PERMANENT ||
            stmt->sequence->relpersistence == RELPERSISTENCE_UNLOGGED)) {
        int status;
        int seqerrcode = ERRCODE_SQL_STATEMENT_NOT_YET_COMPLETE;

        /* We also need to alter sequence on the GTM */
        if ((status = AlterSequenceGTM((int64)newm->uuid,
                                       (int64)newm->increment_by,
                                       (int64)newm->min_value,
                                       (int64)newm->max_value,
                                       (int64)newm->start_value,
                                       (int64)newm->last_value,
                                       newm->is_cycled,
                                       is_restart)) < 0) {
            if (status == GTM_RESULT_COMM_ERROR) {
                seqerrcode = ERRCODE_CONNECTION_FAILURE;
            }
            ereport(ERROR, (errcode(seqerrcode), errmsg("GTM error, could not alter sequence")));
        }
    }
#endif

    /* Clear local cache so that we don't think we have cached numbers */
    /* Note that we do not change the currval() state */
    AssignInt<int128, true>(&(elm->cached), (int128)elm->last);

    if (!isUseLocalSeq) {
        ResetvalGlobal(relid);
    }

    /* If needed, rewrite the sequence relation itself */
    if (need_seq_rewrite) {
        /*
         * Create a new storage file for the sequence, making the state
         * changes transactional.  We want to keep the sequence's relfrozenxid
         * at 0, since it won't contain any unfrozen XIDs.
         */
        RelationSetNewRelfilenode(seqrel, InvalidTransactionId, InvalidMultiXactId);
        /*
         * Insert the modified tuple into the new storage file.
         */
        fill_seq_with_data(seqrel, tuple);
    }
    heap_freetuple(tuple);

    /* process OWNED BY if given */
    if (owned_by != NIL)
        process_owned_by(seqrel, owned_by);

    /* Recode the sequence alter time. */
    PgObjectType objectType = GetPgObjectTypePgClass(seqrel->rd_rel->relkind);
    if (objectType != OBJECT_TYPE_INVALID) {
        UpdatePgObjectMtime(seqrel->rd_id, objectType);
    }

    ObjectAddressSet(address, RelationRelationId, relid);
    relation_close(seqrel, NoLock);
    return address;
}

/*
 * Note: nextval cannot rollback, but it can be used a in transaction block.
 * Thus when we alter sequence and select nextval() in the same transaction, we may
 * get unexpected result. So we prevent alter sequence maxvalue in a transaction.
 */
void PreventAlterSeqInTransaction(bool isTopLevel, AlterSeqStmt* stmt)
{
    ListCell* option = NULL;
    foreach (option, stmt->options) {
        DefElem* defel = (DefElem*)lfirst(option);
        if (strcmp(defel->defname, "maxvalue") == 0) {
            PreventTransactionChain(isTopLevel, "ALTER SEQUENCE MAXVALUE");
        }
    }
}

/*
 * Note: nextval with a text argument is no longer exported as a pg_proc
 * entry, but we keep it around to ease porting of C code that may have
 * called the function directly.
 */
Datum nextval(PG_FUNCTION_ARGS)
{
    text* seqin = PG_GETARG_TEXT_P(0);
    RangeVar* sequence = NULL;
    Oid relid;

    List* nameList = textToQualifiedNameList(seqin);
    sequence = makeRangeVarFromNameList(nameList);

    /*
     * XXX: This is not safe in the presence of concurrent DDL, but acquiring
     * a lock here is more expensive than letting nextval_internal do it,
     * since the latter maintains a cache that keeps us from hitting the lock
     * manager more than once per transaction.	It's not clear whether the
     * performance penalty is material in practice, but for now, we do it this
     * way.
     */
    relid = RangeVarGetRelid(sequence, NoLock, false);
    list_free_deep(nameList);
    if (CheckSeqOwnedByAutoInc(relid)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("cannot change sequence owned by auto_increment column")));
    }
    PG_RETURN_INT64(nextval_internal(relid));
}

Oid get_nextval_rettype()
{
    /*
     * deliberately scan systable instead of search in syscache so that the
     * influence of hard-coded pg_proc is eliminated.
     */
    HeapTuple tup = NULL;
    ScanKeyData entry;
    SysScanDesc scanDesc = NULL;
    Relation rel = heap_open(ProcedureRelationId, NoLock);
    ScanKeyInit(&entry, ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(NEXTVALFUNCOID));
    scanDesc = systable_beginscan(rel, ProcedureOidIndexId, true, SnapshotNow, 1, &entry);
    tup = systable_getnext(scanDesc);
    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR, (errmsg("catalog lookup failed for proc %u", NEXTVALFUNCOID)));
    }
    Form_pg_proc form = (Form_pg_proc)GETSTRUCT(tup);
    Oid ret = form->prorettype;
    systable_endscan(scanDesc);
    heap_close(rel, NoLock);

    return ret;
}

bool shouldReturnNumeric()
{
    /*
     * The return type is controled because the binary may mismatch that of system catalog.
     * Sequence functions should always return the desired type that is determined during
     * optimizer stage.
     * During inplace upgrade, if the nextval function is called by default value, I.E.
     * u_sess->opt_cxt.nextval_default_expr_type != NDE_UNKNOWN, we return the required type
     * in build_column_default.
     * Otherwise, we scan the systable for current return type.
     */
    if (t_thrd.proc->workingVersionNum >= LARGE_SEQUENCE_VERSION_NUM) {
        return true;
    }

    switch (u_sess->opt_cxt.nextval_default_expr_type) {
        case NDE_NUMERIC:
            return true;
        case NDE_BIGINT:
            return false;
        default:
            break;
    }

    HeapTuple ftup = SearchSysCache1(PROCOID, ObjectIdGetDatum(NEXTVALFUNCOID));
    if (!HeapTupleIsValid(ftup)) {
        ereport(ERROR, (errmsg("cache lookup failed for function %u", NEXTVALFUNCOID)));
    }
    Form_pg_proc pform = (Form_pg_proc)GETSTRUCT(ftup);
    bool ret = pform->prorettype == NUMERICOID;
    ReleaseSysCache(ftup);

    return ret;
}

Datum nextval_oid(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    if (CheckSeqOwnedByAutoInc(relid)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("cannot change sequence owned by auto_increment column")));
    }
    int128 result = nextval_internal(relid);

    if (shouldReturnNumeric()) {
        PG_RETURN_NUMERIC(convert_int128_to_numeric(result, 0));
    } else {
        PG_RETURN_INT64(int64(result));
    }
}

int128 nextval_internal(Oid relid)
{
    SeqTable elm = NULL;
    Relation seqrel;
    int128 result;
    bool is_use_local_seq = false;

    if (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE) {
        ereport(ERROR, (errmsg("Standby do not support nextval, please do it in primary!")));
    }
    if (SS_STANDBY_MODE) {
        ereport(ERROR, (errmsg("Shared storage standby do not support nextval, please do it in shared storage primary!")));
    }

    /* open and lock sequence */
    init_sequence(relid, &elm, &seqrel);
    char relkind = RelationGetRelkind(seqrel);

    TrForbidAccessRbObject(RelationRelationId, relid, RelationGetRelationName(seqrel));

    if (pg_class_aclcheck(elm->relid, GetUserId(), ACL_USAGE) != ACLCHECK_OK &&
        pg_class_aclcheck(elm->relid, GetUserId(), ACL_UPDATE) != ACLCHECK_OK)
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied for sequence %s", RelationGetRelationName(seqrel))));

    is_use_local_seq = RelationIsLocalTemp(seqrel) || IS_SINGLE_NODE;

    /* read-only transactions may only modify temp sequences */
    if (!is_use_local_seq)
        PreventCommandIfReadOnly("nextval()");
    if (elm->last != elm->cached) {
        /* some numbers were cached */
        Assert(elm->last_valid);
        Assert(elm->increment != 0);
        elm->last += elm->increment;
        char* buf_last = DatumGetCString(DirectFunctionCall1(int16out, Int128GetDatum(elm->last)));
        char* buf_cached = DatumGetCString(DirectFunctionCall1(int16out, Int128GetDatum(elm->cached)));

        ereport(DEBUG2,
            (errmodule(MOD_SEQ),
                (errmsg("Sequence %s retrun ID %s from cache, the cached is %s, ",
                    RelationGetRelationName(seqrel),
                    buf_last,
                    buf_cached))));

        pfree_ext(buf_last);
        pfree_ext(buf_cached);

        relation_close(seqrel, NoLock);
        u_sess->cmd_cxt.last_used_seq = elm;
        return elm->last;
    }

    /* If don't have cached value, we should fetch some. */
    if (!is_use_local_seq) {
        result = GetNextvalGlobal(elm, seqrel);
    } else {
        if (relkind == RELKIND_SEQUENCE) {
            result = GetNextvalLocal<int64, Form_pg_sequence, false>(elm, seqrel);
        } else { /* can only be large sequence. init_sequence rules out other cases */
            result = GetNextvalLocal<int128, Form_pg_large_sequence, true>(elm, seqrel);
        }
    }

    u_sess->cmd_cxt.last_used_seq = elm;
    char* buf = DatumGetCString(DirectFunctionCall1(int16out, Int128GetDatum(result)));
    ereport(DEBUG2,
        (errmodule(MOD_SEQ),
            (errmsg("Sequence %s retrun ID from nextval %s, ", RelationGetRelationName(seqrel), buf))));
    pfree_ext(buf);

    relation_close(seqrel, NoLock);

    return result;
}

Datum currval_oid(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    int128 result;
    SeqTable elm = NULL;
    Relation seqrel;

    if (!IS_SINGLE_NODE && !u_sess->attr.attr_common.enable_beta_features) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("currval function is not supported")));
    }

    /* open and lock sequence */
    init_sequence(relid, &elm, &seqrel);

    TrForbidAccessRbObject(RelationRelationId, relid, RelationGetRelationName(seqrel));

    if (pg_class_aclcheck(elm->relid, GetUserId(), ACL_SELECT) != ACLCHECK_OK &&
        pg_class_aclcheck(elm->relid, GetUserId(), ACL_USAGE) != ACLCHECK_OK)
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied for sequence %s", RelationGetRelationName(seqrel))));

    if (!elm->last_valid)
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg(
                    "currval of sequence \"%s\" is not yet defined in this session", RelationGetRelationName(seqrel))));

    result = elm->last;
    relation_close(seqrel, NoLock);

    if (shouldReturnNumeric()) {
        PG_RETURN_NUMERIC(convert_int128_to_numeric(result, 0));
    } else {
        PG_RETURN_INT64(int64(result));
    }
}

Datum lastval(PG_FUNCTION_ARGS)
{
    Relation seqrel;
    int128 result;

    if (!IS_SINGLE_NODE && !g_instance.attr.attr_common.lastval_supported &&
        !u_sess->attr.attr_common.enable_beta_features) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("lastval function is not supported")));
    }

    if (u_sess->cmd_cxt.last_used_seq == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("lastval is not yet defined in this session")));

    /* Someone may have dropped the sequence since the last nextval() */
    if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(u_sess->cmd_cxt.last_used_seq->relid)))
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE), errmsg("lastval is not yet defined in this session")));

    seqrel = lock_and_open_seq(u_sess->cmd_cxt.last_used_seq);

    /* nextval() must have already been called for this sequence */
    Assert(u_sess->cmd_cxt.last_used_seq->last_valid);

    if (pg_class_aclcheck(u_sess->cmd_cxt.last_used_seq->relid, GetUserId(), ACL_SELECT) != ACLCHECK_OK &&
        pg_class_aclcheck(u_sess->cmd_cxt.last_used_seq->relid, GetUserId(), ACL_USAGE) != ACLCHECK_OK)
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied for sequence %s", RelationGetRelationName(seqrel))));

    result = u_sess->cmd_cxt.last_used_seq->last;
    relation_close(seqrel, NoLock);

    if (shouldReturnNumeric()) {
        PG_RETURN_NUMERIC(convert_int128_to_numeric(result, 0));
    } else {
        PG_RETURN_INT64(int64(result));
    }
}

Datum last_insert_id_no_args(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("last_insert_id is not supported for distributed system")));
#endif
    if (!DB_IS_CMPT(B_FORMAT)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("last_insert_id is supported only in B-format database")));
    }
    PG_RETURN_INT128(u_sess->cmd_cxt.last_insert_id);
}

Datum last_insert_id(PG_FUNCTION_ARGS)
{
#ifdef ENABLE_MULTIPLE_NODES
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("last_insert_id is not supported for distributed system")));
#endif
    if (!DB_IS_CMPT(B_FORMAT)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("last_insert_id is supported only in B-format database")));
    }
    if (PG_ARGISNULL(0)) {
        u_sess->cmd_cxt.last_insert_id = (int128)0;
        PG_RETURN_NULL();
    }
    u_sess->cmd_cxt.last_insert_id = PG_GETARG_INT128(0);
    PG_RETURN_INT128(u_sess->cmd_cxt.last_insert_id);
}
/*
 * Set sequence last value for auto_increment column.
 */
void autoinc_setval(Oid relid, int128 next, bool iscalled)
{
    SeqTable elm = NULL;
    Relation seqrel;
    Buffer buf;
    HeapTupleData seqtuple;

    init_sequence(relid, &elm, &seqrel);
    /* no need to set a small value */
    if (elm->last_valid && next < elm->last) {
        relation_close(seqrel, NoLock);
        return;
    }

    GTM_UUID uuid;
    Form_pg_large_sequence seq = read_seq_tuple<Form_pg_large_sequence>(elm, seqrel, &buf, &seqtuple, &uuid);

    next = (next > seq->max_value) ? seq->max_value : next;
    /* no need to set a small value */
    if (seq->last_value > next || (seq->last_value == next && seq->is_called)) {
        if (seq->is_called) {
            AssignInt<int128, true>(&(elm->last), (int128)seq->last_value);
            elm->last_valid = true;
        }
        AssignInt<int128, true>(&(elm->cached), elm->last);
        UnlockReleaseBuffer(buf);
        relation_close(seqrel, NoLock);
        return;
    }

    if (iscalled) {
        AssignInt<int128, true>(&(elm->last), (int128)next);
        elm->last_valid = true;
    }
    AssignInt<int128, true>(&(elm->cached), elm->last);

    START_CRIT_SECTION();
    AssignInt<int128, true>(&(seq->last_value), next);
    seq->is_called = iscalled;
    seq->log_cnt = 0;

    MarkBufferDirty(buf);

    if (RelationNeedsWAL(seqrel)) {
        xl_seq_rec xlrec;
        XLogRecPtr recptr;
        Page page = BufferGetPage(buf);

        RelFileNodeRelCopy(xlrec.node, seqrel->rd_node);

        XLogBeginInsert();
        XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);
        XLogRegisterData((char *)&xlrec, sizeof(xl_seq_rec));
        XLogRegisterData((char *)seqtuple.t_data, (int)seqtuple.t_len);

        recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG, seqrel->rd_node.bucketNode);

        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();
    UnlockReleaseBuffer(buf);
    relation_close(seqrel, NoLock);
}

int128 autoinc_get_nextval(Oid relid)
{
    SeqTable elm = NULL;
    Relation seqrel;
    Buffer buf;
    HeapTupleData seqtuple;
    GTM_UUID uuid;
    Form_pg_large_sequence seq;
    int128 result;

    init_sequence(relid, &elm, &seqrel);
    seq = read_seq_tuple<Form_pg_large_sequence>(elm, seqrel, &buf, &seqtuple, &uuid);
    if (seq->is_called) {
        result = (seq->last_value < seq->max_value) ? seq->last_value + 1 : seq->max_value;
    } else {
        result = seq->last_value;
    }
    UnlockReleaseBuffer(buf);
    relation_close(seqrel, NoLock);
    return result;
}

/*
 * Main internal procedure that handles 2 & 3 arg forms of SETVAL.
 *
 * Note that the 3 arg version (which sets the is_called flag) is
 * only for use in pg_dump, and setting the is_called flag may not
 * work if multiple users are attached to the database and referencing
 * the sequence (unlikely if pg_dump is restoring it).
 *
 * It is necessary to have the 3 arg version so that pg_dump can
 * restore the state of a sequence exactly during data-only restores -
 * it is the only way to clear the is_called flag in an existing
 * sequence.
 */
template<typename T_Form, typename T_Int, bool large>
static void do_setval(Oid relid, int128 next, bool iscalled)
{
    SeqTable elm = NULL;
    Relation seqrel;
    Buffer buf;
    HeapTupleData seqtuple;
    T_Form seq;
    bool isUseLocalSeq = false;
#ifdef PGXC
    bool is_temp = false;
#endif
    if (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE) {
        ereport(ERROR, (errmsg("Standby do not support setval, please do it in primary!")));
    }

    /* open and lock sequence */
    init_sequence(relid, &elm, &seqrel);

    TrForbidAccessRbObject(RelationRelationId, relid, RelationGetRelationName(seqrel));

    if (pg_class_aclcheck(elm->relid, GetUserId(), ACL_UPDATE) != ACLCHECK_OK)
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied for sequence %s", RelationGetRelationName(seqrel))));

#ifdef PGXC
    is_temp = RelationIsLocalTemp(seqrel);
    /* temp sequence and single_node do not need gtm, they only use info on local node */
    isUseLocalSeq = is_temp || IS_SINGLE_NODE;

    /* read-only transactions may only modify temp sequences */
    if (!is_temp)
        PreventCommandIfReadOnly("setval()");
#endif

    /* lock page' buffer and read tuple */
    GTM_UUID uuid;
    seq = read_seq_tuple<T_Form>(elm, seqrel, &buf, &seqtuple, &uuid);

    if ((next < seq->min_value) || (next > seq->max_value)) {
        char* bufv = DatumGetCString(DirectFunctionCall1(int16out, Int128GetDatum(next)));
        char* bufm = DatumGetCString(DirectFunctionCall1(int16out, Int128GetDatum(seq->min_value)));
        char* bufx = DatumGetCString(DirectFunctionCall1(int16out, Int128GetDatum(seq->max_value)));
        ereport(ERROR,
            (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                errmsg("setval: value %s is out of bounds for sequence \"%s\" (%s..%s)",
                    bufv,
                    RelationGetRelationName(seqrel),
                    bufm,
                    bufx)));
        pfree_ext(bufv);
        pfree_ext(bufm);
        pfree_ext(bufx);
    }

#ifdef PGXC
    if (IS_PGXC_COORDINATOR && !is_temp) {
        int status;
        int seqerrcode = ERRCODE_SQL_STATEMENT_NOT_YET_COMPLETE;
        if ((status = SetValGTM(uuid, next, iscalled)) < 0) {
            if (status == GTM_RESULT_COMM_ERROR)
                seqerrcode = ERRCODE_CONNECTION_FAILURE;
            ereport(ERROR, (errcode(seqerrcode), errmsg("GTM error, could not obtain sequence value")));
        }

        UnlockReleaseBuffer(buf);

        /* Update the on-disk data */
        seq->last_value = -1; /* disable the unreliable last_value */
        seq->is_called = iscalled;
        seq->log_cnt = (iscalled) ? 0 : 1;

        /* Update the global sequence info */
        ResetvalGlobal(relid);

        if (iscalled) {
            elm->last = next; /* last returned number */
            elm->last_valid = true;
        }

        AssignInt<int128, true>(&(elm->cached), elm->last);
    } else {
#endif

        /* Set the currval() state only if iscalled = true */
        if (iscalled) {
            AssignInt<int128, true>(&(elm->last), (int128)next); /* last returned number */
            elm->last_valid = true;
        }

        /* In any case, forget any future cached numbers */
        AssignInt<int128, true>(&(elm->cached), elm->last);

        /* ready to change the on-disk (or really, in-buffer) tuple */
        START_CRIT_SECTION();

        /* keep the last value if isUseLocalSeq */
        if (isUseLocalSeq) {
            AssignInt<T_Int, large>(&(seq->last_value), (T_Int)next); /* last fetched number */
        } else {
            AssignInt<T_Int, large>(&(seq->last_value), (T_Int)-1); /* disable the unreliable last_value */
        }
        seq->is_called = iscalled;
        seq->log_cnt = 0;

        MarkBufferDirty(buf);

        /* XLOG stuff */
        if (RelationNeedsWAL(seqrel)) {
            xl_seq_rec xlrec;
            XLogRecPtr recptr;
            Page page = BufferGetPage(buf);

            RelFileNodeRelCopy(xlrec.node, seqrel->rd_node);

            XLogBeginInsert();
            XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);
            XLogRegisterData((char*)&xlrec, sizeof(xl_seq_rec));
            XLogRegisterData((char*)seqtuple.t_data, seqtuple.t_len);

            recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG, seqrel->rd_node.bucketNode);

            PageSetLSN(page, recptr);
        }

        END_CRIT_SECTION();
        UnlockReleaseBuffer(buf);

#ifdef PGXC
    }
#endif

    relation_close(seqrel, NoLock);
}

/*
 * Implement the 2 arg setval procedure.
 * See do_setval for discussion.
 */
Datum setval_oid(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    Numeric nextArg = PG_GETARG_NUMERIC(1);
    int128 next = numeric_int16_internal(nextArg);

    Relation rel = relation_open(relid, NoLock);
    char relkind = RelationGetRelkind(rel);
    relation_close(rel, NoLock);
    if (CheckSeqOwnedByAutoInc(relid)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("cannot change sequence owned by auto_increment column")));
    }
    if (relkind == RELKIND_SEQUENCE) {
        do_setval<Form_pg_sequence, int64, false>(relid, next, true);
    } else if (relkind == RELKIND_LARGE_SEQUENCE) {
        do_setval<Form_pg_large_sequence, int128, true>(relid, next, true);
    }

    PG_RETURN_NUMERIC(nextArg);
}

/*
 * Implement the 3 arg setval procedure.
 * See do_setval for discussion.
 */
Datum setval3_oid(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    Numeric nextArg = PG_GETARG_NUMERIC(1);
    int128 next = numeric_int16_internal(nextArg);
    bool iscalled = PG_GETARG_BOOL(2);

    Relation rel = relation_open(relid, NoLock);
    char relkind = RelationGetRelkind(rel);
    relation_close(rel, NoLock);
    if (CheckSeqOwnedByAutoInc(relid)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("cannot change sequence owned by auto_increment column")));
    }
    if (relkind == RELKIND_SEQUENCE) {
        do_setval<Form_pg_sequence, int64, false>(relid, next, iscalled);
    } else if (relkind == RELKIND_LARGE_SEQUENCE) {
        do_setval<Form_pg_large_sequence, int128, true>(relid, next, iscalled);
    }

    PG_RETURN_NUMERIC(nextArg);
}

/* lock sequence on cn when "select nextval" */
void lockNextvalOnCn(Oid relid)
{
    /*
     * When execute alter sequence and nextval at the same time, if nextval is shipped to DNs,
     * for example: nextval locked DN1, and acquire lock on DN2, but at the same time, alter
     * sequence locked DN2, and acquire lock on DN1, which caused deadlock.
     * Thus, we lock sequence on CN so that before locking relation on DN, nextval or alter
     * sequence will wait the lock on CN.
     */
    if (IS_PGXC_COORDINATOR) {
        SeqTable elm = NULL;
        Relation seqrel = NULL;
        init_sequence(relid, &elm, &seqrel);
        relation_close(seqrel, NoLock);
    }
}

GTM_UUID get_uuid_from_rel(Relation rel)
{
    Buffer buf;
    SeqTableData elm;
    HeapTupleData seqtuple;
    GTM_UUID uuid;
    (void)read_seq_tuple<Form_pg_sequence>(&elm, rel, &buf, &seqtuple, &uuid);

    /* Now we're done with the old page */
    UnlockReleaseBuffer(buf);

    return uuid;
}

template<typename T_Form>
static GTM_UUID get_uuid_from_tuple(const void* seq_p, const Relation rel, const HeapTuple seqtuple)
{
    T_Form seq = (T_Form)seq_p;
    bool isnull = true;
    GTM_UUID uuid;
    unsigned int natts = rel->rd_att->natts;
    unsigned int tdesc_natts = HeapTupleHeaderGetNatts(seqtuple->t_data, rel->rd_att);

    /*
     * If natts != tdesc_natts, The uuid is added by 'alter sequence add column default',
     * mainly added through the upgrade process, so get the uuid from attinitdefval of
     * pg_attribute.
     */
    if (natts != tdesc_natts) {
        Assert(natts == SEQ_COL_LASTCOL);
        Assert(tdesc_natts == SEQ_COL_LASTCOL - 1);
        uuid = DatumGetInt64(heapGetInitDefVal(SEQ_COL_LASTCOL, rel->rd_att, &isnull));
        Assert(isnull == false);
        Assert(seq->uuid != 0);
    } else {
        /* the uuid is added when create sequence. */
        uuid = seq->uuid;
    }

    return uuid;
}
/*
 * Given an opened sequence relation, lock the page buffer and find the tuple
 *
 * *buf receives the reference to the pinned-and-ex-locked buffer
 * *seqtuple receives the reference to the sequence tuple proper
 *		(this arg should point to a local variable of type HeapTupleData)
 *
 * Function's return value points to the data payload of the tuple
 */
template<typename T_Form>
static T_Form read_seq_tuple(SeqTable elm, Relation rel, Buffer* buf, HeapTuple seqtuple, GTM_UUID* uuid)
{
    Page page;
    ItemId lp;
    T_Form seq;
    sequence_magic* sm = NULL;

    *buf = ReadBuffer(rel, 0);
    LockBuffer(*buf, BUFFER_LOCK_EXCLUSIVE);

    page = BufferGetPage(*buf);

    sm = (sequence_magic*)PageGetSpecialPointer(page);
    if (sm->magic != SEQ_MAGIC)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("bad magic number in sequence \"%s\": %08X", RelationGetRelationName(rel), sm->magic)));

    lp = PageGetItemId(page, FirstOffsetNumber);
    Assert(ItemIdIsNormal(lp));

    /* Note we currently only bother to set these two fields of *seqtuple */
    seqtuple->t_data = (HeapTupleHeader)PageGetItem(page, lp);
    seqtuple->t_self = seqtuple->t_data->t_ctid;
    seqtuple->t_len = ItemIdGetLength(lp);
    HeapTupleCopyBaseFromPage(seqtuple, page);

    /*
     * Previous releases of openGauss neglected to prevent SELECT FOR UPDATE on
     * a sequence, which would leave a non-frozen XID in the sequence tuple's
     * xmax, which eventually leads to clog access failures or worse. If we
     * see this has happened, clean up after it.  We treat this like a hint
     * bit update, ie, don't bother to WAL-log it, since we can certainly do
     * this again if the update gets lost.
     */
    if (HeapTupleGetRawXmax(seqtuple) != InvalidTransactionId) {
        HeapTupleSetXmax(seqtuple, InvalidTransactionId);
        seqtuple->t_data->t_infomask &= ~HEAP_XMAX_COMMITTED;
        seqtuple->t_data->t_infomask |= HEAP_XMAX_INVALID;
        MarkBufferDirtyHint(*buf, true);
    }

    seq = (T_Form)GETSTRUCT(seqtuple);

    /* this is a handy place to update our copy of the increment */
    AssignInt<int128, true>(&(elm->increment), (int128)seq->increment_by);
    AssignInt<int128, true>(&(elm->minval), (int128)seq->min_value);
    AssignInt<int128, true>(&(elm->maxval), (int128)seq->max_value);
    AssignInt<int128, true>(&(elm->startval), (int128)seq->start_value);
    elm->is_cycled = seq->is_cycled;
    elm->uuid = *uuid = get_uuid_from_tuple<T_Form>((void*)seq, rel, seqtuple);

    return seq;
}

template<typename T_Int, bool large>
static void CheckValueMinMax(T_Int value, T_Int minValue, T_Int maxValue, bool isStart)
{
    char* bufs = NULL;
    char* bufm = NULL;
    /* crosscheck RESTART (or current value, if changing MIN/MAX) */
    if (value < minValue) {
        bufs = large ? DatumGetCString(DirectFunctionCall1(int16out, Int128GetDatum(value))) :
            DatumGetCString(DirectFunctionCall1(int8out, value));
        bufm = large ? DatumGetCString(DirectFunctionCall1(int16out, Int128GetDatum(minValue))) :
            DatumGetCString(DirectFunctionCall1(int8out, minValue));
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("%s value (%s) cannot be less than MINVALUE (%s)", isStart? "START":"RESTART", bufs, bufm)));
    }
    if (value > maxValue) {
        bufs = large ? DatumGetCString(DirectFunctionCall1(int16out, Int128GetDatum(value))) :
            DatumGetCString(DirectFunctionCall1(int8out, value));
        bufm = large ? DatumGetCString(DirectFunctionCall1(int16out, Int128GetDatum(maxValue))) :
            DatumGetCString(DirectFunctionCall1(int8out, maxValue));
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("%s value (%s) cannot be greater than MAXVALUE (%s)", isStart? "START":"RESTART", bufs, bufm)));
    }
}

template<typename T_Int, bool large>
static T_Int defGetInt(DefElem* def)
{
    if (def->arg == NULL)
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("%s requires a numeric value", def->defname)));
    switch (nodeTag(def->arg)) {
        case T_Integer:
            return (T_Int)intVal(def->arg);
        case T_Float:
            /*
             * Values too large for int4 will be represented as Float
             * constants by the lexer.	Accept these if they are valid int8 or int16
             * strings.
             */
            if (large) {
                return DatumGetInt128(DirectFunctionCall1(int16in, CStringGetDatum(strVal(def->arg))));
            } else {
                return DatumGetInt64(DirectFunctionCall1(int8in, CStringGetDatum(strVal(def->arg))));
            }
        default:
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("%s requires a numeric value", def->defname)));
    }
    return 0; /* keep compiler quiet */
}

enum {
    DEF_IDX_START_VALUE,
    DEF_IDX_RESTART_VALUE,
    DEF_IDX_INCREMENT_BY,
    DEF_IDX_MAX_VALUE,
    DEF_IDX_MIN_VALUE,
    DEF_IDX_CACHE_VALUE,
    DEF_IDX_IS_CYCLED,
    DEF_IDX_NUM
};

static void CheckDuplicateDef(const void* elm)
{
    if (elm != NULL) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
    }
}

static void PreProcessSequenceOptions(
    List* options, DefElem* elms[DEF_IDX_NUM], List** owned_by, bool* need_seq_rewrite, bool isInit)
{
    ListCell* option = NULL;

    foreach (option, options) {
        DefElem* defel = (DefElem*)lfirst(option);

        if (strcmp(defel->defname, "increment") == 0) {
            CheckDuplicateDef(elms[DEF_IDX_INCREMENT_BY]);
            elms[DEF_IDX_INCREMENT_BY] = defel;
            *need_seq_rewrite = true;
        } else if (strcmp(defel->defname, "start") == 0) {
            CheckDuplicateDef(elms[DEF_IDX_START_VALUE]);
            elms[DEF_IDX_START_VALUE] = defel;
            *need_seq_rewrite = true;
        } else if (strcmp(defel->defname, "restart") == 0) {
            CheckDuplicateDef(elms[DEF_IDX_RESTART_VALUE]);
            elms[DEF_IDX_RESTART_VALUE] = defel;
            *need_seq_rewrite = true;
        } else if (strcmp(defel->defname, "maxvalue") == 0) {
            CheckDuplicateDef(elms[DEF_IDX_MAX_VALUE]);
            elms[DEF_IDX_MAX_VALUE] = defel;
            *need_seq_rewrite = true;
        } else if (strcmp(defel->defname, "minvalue") == 0) {
            CheckDuplicateDef(elms[DEF_IDX_MIN_VALUE]);
            elms[DEF_IDX_MIN_VALUE] = defel;
            *need_seq_rewrite = true;
        } else if (strcmp(defel->defname, "cache") == 0) {
            CheckDuplicateDef(elms[DEF_IDX_CACHE_VALUE]);
            elms[DEF_IDX_CACHE_VALUE] = defel;
            *need_seq_rewrite = true;
        } else if (strcmp(defel->defname, "cycle") == 0) {
            CheckDuplicateDef(elms[DEF_IDX_IS_CYCLED]);
            elms[DEF_IDX_IS_CYCLED] = defel;
            *need_seq_rewrite = true;
        } else if (strcmp(defel->defname, "owned_by") == 0) {
            CheckDuplicateDef(*owned_by);
            *owned_by = defGetQualifiedName(defel);
        } else {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("option \"%s\" not recognized", defel->defname)));
        }
    }
}

template<typename T_Form, typename T_Int, bool large>
static void ProcessSequenceOptIncrementBy(DefElem* elm, T_Form newm, bool isInit)
{
    if (elm != NULL) {
        AssignInt<T_Int, large>(&(newm->increment_by), defGetInt<T_Int, large>(elm));
        if (newm->increment_by == 0) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("INCREMENT must not be zero")));
        }
        newm->log_cnt = 0;
    } else if (isInit) {
        AssignInt<T_Int, large>(&(newm->increment_by), (int128)1);
    }
}

template<typename T_Form>
static void ProcessSequenceOptCycle(DefElem* elm, T_Form newm, bool isInit)
{
    if (elm != NULL) {
        newm->is_cycled = intVal(elm->arg);
        Assert(BoolIsValid(newm->is_cycled));
        newm->log_cnt = 0;
    } else if (isInit) {
        newm->is_cycled = false;
    }
}

template<typename T_Form, typename T_Int, bool large>
static void ProcessSequenceOptMax(DefElem* elm, T_Form newm, bool isInit)
{
    if (elm != NULL && elm->arg) {
        AssignInt<T_Int, large>(&(newm->max_value), defGetInt<T_Int, large>(elm));
        newm->log_cnt = 0;
    } else if (isInit || elm != NULL) {
        if (newm->increment_by > 0) {
            /* ascending seq */
            AssignInt<T_Int, large>(&(newm->max_value), large ? LARGE_SEQ_MAXVALUE : SEQ_MAXVALUE);
        } else {
            /* descending seq */
            AssignInt<T_Int, large>(&(newm->max_value), (int128)-1);
        }
        newm->log_cnt = 0;
    }
}

template<typename T_Form, typename T_Int, bool large>
static void ProcessSequenceOptMin(DefElem* elm, T_Form newm, bool isInit)
{
    if (elm != NULL && elm->arg) {
        AssignInt<T_Int, large>(&(newm->min_value), defGetInt<T_Int, large>(elm));
        newm->log_cnt = 0;
    } else if (isInit || elm != NULL) {
        if (newm->increment_by > 0) {
            /* ascending seq */
            AssignInt<T_Int, large>(&(newm->min_value), (int128)1);
        } else {
            /* descending seq */
            AssignInt<T_Int, large>(&(newm->min_value), large ? LARGE_SEQ_MINVALUE : SEQ_MINVALUE);
        }
        newm->log_cnt = 0;
    }
}

template<typename T_Form, typename T_Int, bool large>
static void ProcessSequenceOptStartWith(DefElem* elm, T_Form newm, bool isInit)
{
    if (elm != NULL)
        AssignInt<T_Int, large>(&(newm->start_value), defGetInt<T_Int, large>(elm));
    else if (isInit) {
        AssignInt<T_Int, large>(&(newm->start_value), (newm->increment_by > 0) ? newm->min_value : newm->max_value);
    }
}

template<typename T_Form, typename T_Int, bool large>
static void ProcessSequenceOptReStartWith(DefElem* elm, T_Form newm, bool isInit, bool* is_restart, bool isUseLocalSeq)
{
    if (elm != NULL) {
        AssignInt<T_Int, large>(&(newm->last_value),
                                (elm->arg != NULL) ? defGetInt<T_Int, large>(elm) : newm->start_value);
#ifdef PGXC
        *is_restart = true;
#endif
        newm->is_called = false;
        newm->log_cnt = 0;
    } else if (isInit) {
        AssignInt<T_Int, large>(&(newm->last_value), (isUseLocalSeq) ? newm->start_value : (int128)-1);
        newm->is_called = false;
    }
}

template<typename T_Form, typename T_Int, bool large>
static void ProcessSequenceOptCache(DefElem* elmCache, DefElem* elmMax, DefElem* elmMin, T_Form newm, bool isInit)
{
    if (elmCache != NULL) {
        AssignInt<T_Int, large>(&(newm->cache_value), defGetInt<T_Int, large>(elmCache));
        if (newm->cache_value <= 0) {
            char* buf = large ? DatumGetCString(DirectFunctionCall1(int16out, Int128GetDatum(newm->cache_value))) :
                DatumGetCString(DirectFunctionCall1(int8out, newm->cache_value));
            ereport(
                ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("CACHE (%s) must be greater than zero", buf)));
        } else if (newm->cache_value > 1) {
            if ((newm->increment_by > 0 && elmMax != NULL && elmMax->arg != NULL) ||
                (newm->increment_by < 0 && elmMin != NULL && elmMin->arg != NULL))
                ereport(NOTICE,
                    (errmsg("Not advised to use MAXVALUE or MINVALUE together with CACHE."),
                        errdetail("If CACHE is defined, some sequence values may be wasted, causing available sequence "
                                  "numbers to be less than expected.")));
        }
        newm->log_cnt = 0;
    } else if (isInit) {
        AssignInt<T_Int, large>(&(newm->cache_value), (int128)1);
    }
}

template<typename T_Int, bool large>
static void CrossCheckMinMax(T_Int min, T_Int max)
{
    if (min >= max) {
        char* bufm = large ? DatumGetCString(DirectFunctionCall1(int16out, Int128GetDatum(min))) :
            DatumGetCString(DirectFunctionCall1(int8out, min));
        char* bufx = large ? DatumGetCString(DirectFunctionCall1(int16out, Int128GetDatum(max))) :
            DatumGetCString(DirectFunctionCall1(int8out, max));
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("MINVALUE (%s) must be less than MAXVALUE (%s)", bufm, bufx)));
    }
}

/*
 * init_params: process the options list of CREATE or ALTER SEQUENCE,
 * and store the values into appropriate fields of *new.  Also set
 * *owned_by to any OWNED BY option, or to NIL if there is none.
 *
 * If isInit is true, fill any unspecified options with default values;
 * otherwise, do not change existing options that aren't explicitly overridden.
 *
 * Note: we force a sequence rewrite whenever we change parameters that affect
 * generation of future sequence values, even if the seqdataform per se is not
 * changed.  This allows ALTER SEQUENCE to behave transactionally.  Currently,
 * the only option that doesn't cause that is OWNED BY.  It's *necessary* for
 * ALTER SEQUENCE OWNED BY to not rewrite the sequence, because that would
 * break pg_upgrade by causing unwanted changes in the sequence's relfilenode.
 */

#ifdef PGXC
template<typename T_Form, typename T_Int, bool large>
static void init_params(List* options, bool isInit, bool isUseLocalSeq, void* newm_p, List** owned_by,
    bool* is_restart, bool* need_seq_rewrite)
#else
template<typename T_Form, typename T_Int, bool large>
static void init_params(List* options, bool isInit, bool isUseLocalSeq, void* newm_p, List** owned_by,
    bool* need_seq_rewrite)
#endif
{
    T_Form newm = (T_Form)newm_p;
    DefElem* elms[DEF_IDX_NUM] = {0};

#ifdef PGXC
    *is_restart = false;
#endif

    *owned_by = NIL;

    PreProcessSequenceOptions(options, elms, owned_by, need_seq_rewrite, isInit);

    /*
     * We must reset log_cnt when isInit or when changing any parameters
     * that would affect future nextval allocations.
     */
    if (isInit) {
        newm->log_cnt = 0;
    }

    ProcessSequenceOptIncrementBy<T_Form, T_Int, large>(elms[DEF_IDX_INCREMENT_BY], newm, isInit);
    ProcessSequenceOptCycle<T_Form>(elms[DEF_IDX_IS_CYCLED], newm, isInit);
    ProcessSequenceOptMax<T_Form, T_Int, large>(elms[DEF_IDX_MAX_VALUE], newm, isInit);
    ProcessSequenceOptMin<T_Form, T_Int, large>(elms[DEF_IDX_MIN_VALUE], newm, isInit);

    /* crosscheck min/max */
    CrossCheckMinMax<T_Int, large>(newm->min_value, newm->max_value);

    ProcessSequenceOptStartWith<T_Form, T_Int, large>(elms[DEF_IDX_START_VALUE], newm, isInit);

    /* crosscheck START */
    CheckValueMinMax<T_Int, large>(newm->start_value, newm->min_value, newm->max_value, true);

    ProcessSequenceOptReStartWith<T_Form, T_Int, large>(
        elms[DEF_IDX_RESTART_VALUE], newm, isInit, is_restart, isUseLocalSeq);

    if (isUseLocalSeq) {
        /* crosscheck RESTART (or current value, if changing MIN/MAX) */
        CheckValueMinMax<T_Int, large>(newm->last_value, newm->min_value, newm->max_value, false);
    }

    ProcessSequenceOptCache<T_Form, T_Int, large>(
        elms[DEF_IDX_CACHE_VALUE], elms[DEF_IDX_MAX_VALUE], elms[DEF_IDX_MIN_VALUE], newm, isInit);
}

/*
 * Process an OWNED BY option for CREATE/ALTER SEQUENCE
 *
 * Ownership permissions on the sequence are already checked,
 * but if we are establishing a new owned-by dependency, we must
 * enforce that the referenced table has the same owner and namespace
 * as the sequence.
 */
static void process_owned_by(const Relation seqrel, List* owned_by)
{
    int nnames;
    Relation tablerel;
    AttrNumber attnum;

    nnames = list_length(owned_by);
    Assert(nnames > 0);
    if (nnames == 1) {
        /* Must be OWNED BY NONE */
        if (strcmp(strVal(linitial(owned_by)), "none") != 0)
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("invalid OWNED BY option"),
                    errhint("Specify OWNED BY table.column or OWNED BY NONE.")));
        tablerel = NULL;
        attnum = 0;
    } else {
        List* relname = NIL;
        char* attrname = NULL;
        RangeVar* rel = NULL;

        /* Separate relname and attr name */
        relname = list_truncate(list_copy(owned_by), nnames - 1);
        attrname = strVal(lfirst(list_tail(owned_by)));

        /* Open and lock rel to ensure it won't go away meanwhile */
        rel = makeRangeVarFromNameList(relname);
        tablerel = relation_openrv(rel, AccessShareLock);

        /* Must be a regular table or MOT or postgres_fdw table */
        if (tablerel->rd_rel->relkind != RELKIND_RELATION &&
            !(RelationIsForeignTable(tablerel) && (
#ifdef ENABLE_MOT
                isMOTFromTblOid(RelationGetRelid(tablerel)) ||
#endif
                isPostgresFDWFromTblOid(RelationGetRelid(tablerel))))) {
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("referenced relation \"%s\" is not a table", RelationGetRelationName(tablerel))));
        }

        /* We insist on same owner and schema */
        if (seqrel->rd_rel->relowner != tablerel->rd_rel->relowner)
            ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("sequence must have same owner as table it is linked to")));
        if (RelationGetNamespace(seqrel) != RelationGetNamespace(tablerel))
            ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("sequence must be in same schema as table it is linked to")));

        /* Now, fetch the attribute number from the system cache */
        attnum = get_attnum(RelationGetRelid(tablerel), attrname);
        if (attnum == InvalidAttrNumber)
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_COLUMN),
                    errmsg("column \"%s\" of relation \"%s\" does not exist",
                        attrname,
                        RelationGetRelationName(tablerel))));
    }

    /*
     * OK, we are ready to update pg_depend.  First remove any existing AUTO
     * dependencies for the sequence, then optionally add a new one.
     */
    markSequenceUnowned(RelationGetRelid(seqrel));

    if (tablerel) {
        ObjectAddress refobject, depobject;

        refobject.classId = RelationRelationId;
        refobject.objectId = RelationGetRelid(tablerel);
        refobject.objectSubId = attnum;
        depobject.classId = RelationRelationId;
        depobject.objectId = RelationGetRelid(seqrel);
        depobject.objectSubId = 0;
        recordDependencyOn(&depobject, &refobject, DEPENDENCY_AUTO);
    }

    /* Done, but hold lock until commit */
    if (tablerel)
        relation_close(tablerel, NoLock);
}

/*
 * Return sequence parameters, detailed
 */
sequence_values *get_sequence_values(Oid sequenceId)
{
    Relation seq_rel;
    SeqTable elm = NULL;
    HeapTupleData seqtuple;
    int64 uuid;
    Buffer buf;

    sequence_values *seqvalues = NULL;

    /*
     * Read the old sequence. This does a bit more work than really
     * necessary, but it's simple, and we do want to double-check that it's
     * indeed a sequence.
     */
    init_sequence(sequenceId, &elm, &seq_rel);

    seqvalues = (sequence_values *)palloc(sizeof(sequence_values));
    seqvalues->large = (RelationGetRelkind(seq_rel) == RELKIND_LARGE_SEQUENCE);

    if (seqvalues->large) {
        Form_pg_large_sequence seq = read_seq_tuple<Form_pg_large_sequence>(elm, seq_rel, &buf, &seqtuple, &uuid);
        seqvalues->sequence_name = pstrdup(seq->sequence_name.data);
        seqvalues->is_cycled = seq->is_cycled;
        seqvalues->last_value = Int8or16Out<int128, true>(seq->last_value);
        seqvalues->start_value = Int8or16Out<int128, true>(seq->start_value);
        seqvalues->increment_by = Int8or16Out<int128, true>(seq->increment_by);
        seqvalues->max_value = Int8or16Out<int128, true>(seq->max_value);
        seqvalues->min_value = Int8or16Out<int128, true>(seq->min_value);
        seqvalues->cache_value = Int8or16Out<int128, true>(seq->cache_value);
        UnlockReleaseBuffer(buf);
    } else {
        Form_pg_sequence seq = read_seq_tuple<Form_pg_sequence>(elm, seq_rel, &buf, &seqtuple, &uuid);
        seqvalues->sequence_name = pstrdup(seq->sequence_name.data);
        seqvalues->is_cycled = seq->is_cycled;
        seqvalues->last_value = Int8or16Out<int64, false>(seq->last_value);
        seqvalues->start_value = Int8or16Out<int64, false>(seq->start_value);
        seqvalues->increment_by = Int8or16Out<int64, false>(seq->increment_by);
        seqvalues->max_value = Int8or16Out<int64, false>(seq->max_value);
        seqvalues->min_value = Int8or16Out<int64, false>(seq->min_value);
        seqvalues->cache_value = Int8or16Out<int64, false>(seq->cache_value);
        UnlockReleaseBuffer(buf);
    }

    relation_close(seq_rel, NoLock);
    
    return seqvalues;
}

/*
 * Return sequence parameters
 */
void get_sequence_params(Relation rel, int64* uuid, int64* start, int64* increment, int64* maxvalue, int64* minvalue,
    int64* cache, bool* cycle)
{
    Buffer buf;
    SeqTableData elm;
    HeapTupleData seqtuple;
    Form_pg_sequence seq;

    seq = read_seq_tuple<Form_pg_sequence>(&elm, rel, &buf, &seqtuple, uuid);

    *start = seq->start_value;
    *increment = seq->increment_by;
    *maxvalue = seq->max_value;
    *minvalue = seq->min_value;
    *cache = seq->cache_value;
    *cycle = seq->is_cycled;

    /* Now we're done with the old page */
    UnlockReleaseBuffer(buf);
}

/*
 * Return sequence parameters, for use by information schema
 */
Datum pg_sequence_parameters(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    Relation rel = relation_open(relid, NoLock);
    char relkind = RelationGetRelkind(rel);
    bool large = relkind == 'L';
    relation_close(rel, NoLock);
    TupleDesc tupdesc;
    Datum values[5];
    bool isnull[5];
    SeqTable elm = NULL;
    Relation seqrel;
    Buffer buf;
    HeapTupleData seqtuple;

    /* open and lock sequence */
    init_sequence(relid, &elm, &seqrel);

    if (pg_class_aclcheck(relid, GetUserId(), ACL_SELECT | ACL_UPDATE | ACL_USAGE) != ACLCHECK_OK)
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied for sequence %s", RelationGetRelationName(seqrel))));

    tupdesc = CreateTemplateTupleDesc(5, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "start_value", INT16OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "minimum_value", INT16OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "maximum_value", INT16OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)4, "increment", INT16OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)5, "cycle_option", BOOLOID, -1, 0);

    BlessTupleDesc(tupdesc);

    errno_t rc = memset_s(isnull, sizeof(isnull), 0, sizeof(isnull));
    securec_check(rc, "\0", "\0");

    GTM_UUID uuid;
    int i = 0;
    if (large) {
        Form_pg_large_sequence seq = read_seq_tuple<Form_pg_large_sequence>(elm, seqrel, &buf, &seqtuple, &uuid);
        values[i++] = Int128GetDatum(seq->start_value);
        values[i++] = Int128GetDatum(seq->min_value);
        values[i++] = Int128GetDatum(seq->max_value);
        values[i++] = Int128GetDatum(seq->increment_by);
        values[i++] = BoolGetDatum(seq->is_cycled);
    } else {
        Form_pg_sequence seq = read_seq_tuple<Form_pg_sequence>(elm, seqrel, &buf, &seqtuple, &uuid);
        values[i++] = Int128GetDatum((int128)seq->start_value);
        values[i++] = Int128GetDatum((int128)seq->min_value);
        values[i++] = Int128GetDatum((int128)seq->max_value);
        values[i++] = Int128GetDatum((int128)seq->increment_by);
        values[i++] = BoolGetDatum(seq->is_cycled);
    }

    UnlockReleaseBuffer(buf);
    relation_close(seqrel, NoLock);

    return HeapTupleGetDatum(heap_form_tuple(tupdesc, values, isnull));
}

Datum pg_sequence_last_value(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    Relation rel = relation_open(relid, NoLock);
    char relkind = RelationGetRelkind(rel);
    bool large = relkind == 'L';
    relation_close(rel, NoLock);
    TupleDesc tupdesc;
    Datum values[2];
    bool isnull[2];
    SeqTable elm = NULL;
    Relation seqrel;
    Buffer buf;
    HeapTupleData seqtuple;

    /* open and lock sequence */
    init_sequence(relid, &elm, &seqrel);

    if (pg_class_aclcheck(relid, GetUserId(), ACL_SELECT | ACL_UPDATE | ACL_USAGE) != ACLCHECK_OK)
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied for sequence %s", RelationGetRelationName(seqrel))));

    tupdesc = CreateTemplateTupleDesc(2, false);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "cache_value", INT16OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "last_value", INT16OID, -1, 0);

    BlessTupleDesc(tupdesc);

    errno_t rc = memset_s(isnull, sizeof(isnull), 0, sizeof(isnull));
    securec_check(rc, "\0", "\0");

    GTM_UUID uuid;
    int i = 0;
    if (large) {
        Form_pg_large_sequence seq = read_seq_tuple<Form_pg_large_sequence>(elm, seqrel, &buf, &seqtuple, &uuid);
        values[i++] = Int128GetDatum(seq->cache_value);
        values[i++] = Int128GetDatum(seq->last_value);
    } else {
        Form_pg_sequence seq = read_seq_tuple<Form_pg_sequence>(elm, seqrel, &buf, &seqtuple, &uuid);
        values[i++] = Int128GetDatum((int128)seq->cache_value);
        values[i++] = Int128GetDatum((int128)seq->last_value);
    }

    UnlockReleaseBuffer(buf);
    relation_close(seqrel, NoLock);

    return HeapTupleGetDatum(heap_form_tuple(tupdesc, values, isnull));
}

#ifdef PGXC

/*
 * Delete sequence from global hash bucket
 */
void delete_global_seq(Oid relid, Relation seqrel)
{
    SeqTable currseq = NULL;
    DListCell* elem = NULL;
    GlobalSeqInfoHashBucket* bucket = NULL;
    uint32 hash = RelidGetHash(relid);

    bucket = &g_instance.global_seq[hash];

    (void)LWLockAcquire(GetMainLWLockByIndex(bucket->lock_id), LW_EXCLUSIVE);

    dlist_foreach_cell(elem, bucket->shb_list)
    {
        currseq = (SeqTable)lfirst(elem);
        if (currseq->relid == relid) {
            bucket->shb_list = dlist_delete_cell(bucket->shb_list, elem, true);
            break;
        }
    }

    LWLockRelease(GetMainLWLockByIndex(bucket->lock_id));

    ereport(DEBUG2,
        (errmodule(MOD_SEQ),
            (errmsg("Delete sequence %s .",
                    RelationGetRelationName(seqrel)))));
}

/*
 * Register a callback for a sequence drop on GTM
 */
void register_sequence_cb(GTM_UUID seq_uuid, GTM_SequenceDropType type)
{
    drop_sequence_callback_arg* args;

    /* We change the memory from u_sess->top_transaction_mem_cxt to t_thrd.top_mem_cxt,
     * because we postpone the CallSequenceCallback after CN/DN commit.
     * not same as CallGTMCallback which is called in PreparedTranscaton phase.
     * The u_sess->top_transaction_mem_cxt is released, after Prepared finished.
     * If CallSequenceCallback is called in Prepared Phase, it will be
     * difficult to rollback if the transaction is abort after prepared.
     */
    args = (drop_sequence_callback_arg*)MemoryContextAlloc(
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER), sizeof(drop_sequence_callback_arg));

    args->seq_uuid = seq_uuid;
    args->type = type;

    RegisterSequenceCallback(drop_sequence_cb, (void*)args);
}

#endif

#ifdef ENABLE_MULTIPLE_NODES
static void sendUpdateSequenceMsgToDn(List* dbname, List* schemaname, List* seqname, List* last_value)
{
    List* dataNodeList = NULL;
    PGXCNodeAllHandles* conn_handles = NULL;
    int msglen;
    int low = 0;
    int high = 0;
    errno_t rc = 0;
    GlobalTransactionId gxid =  GetCurrentTransactionId();
    PGXCNodeHandle *exec_handle = NULL;
    exec_handle = GetRegisteredTransactionNodes(true);
    if (exec_handle == NULL) {
        /* not transaction already, choose first node */
        dataNodeList = lappend_int(dataNodeList, 0);
        conn_handles = get_handles(dataNodeList, NULL, false);
        list_free_ext(dataNodeList);
        if (conn_handles->dn_conn_count != 1) {
            ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
                            errmsg("Could not get handle  on datanode for sequence")));
        }
        exec_handle = conn_handles->datanode_handles[0];
    }
    uint64 u_last_value;
    ereport(DEBUG1,
        (errmsg("sendUpdateSequenceMsgToDn %s %ld", (char*)lfirst(list_head(seqname)),
        *(int64*)lfirst(list_head(last_value)))));
    if (pgxc_node_begin(1, &exec_handle, gxid, true, false, PGXC_NODE_DATANODE)) {
        ereport(ERROR, (errcode(ERRCODE_CONNECTION_EXCEPTION),
                errmsg("Could not begin transaction on datanode for sequence")));
    }

    PGXCNodeHandle* handle = NULL;

    handle = exec_handle;

    /* Invalid connection state, return error */
    if (handle->state != DN_CONNECTION_STATE_IDLE)
        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_FAILED),
                errmsg("Failed to send sendUpdateSequenceMsgToDn request "
                       "to the node")));
    
    ListCell* v = NULL;
    int dbname_len = 0;
    int schemaname_len = 0;
    int seqname_len = 0;

    foreach(v, dbname) {
        dbname_len += strlen((char*)lfirst(v)) + 1;
    }
    v = NULL;
    foreach(v, schemaname) {
        schemaname_len += strlen((char*)lfirst(v)) + 1;
    }
    v = NULL;
    foreach(v, seqname) {
        seqname_len += strlen((char*)lfirst(v)) + 1;
    }

    int number1 = dbname->length;
    msglen = 4; /* for the length itself */
    msglen += sizeof(int); /* for the number of sequences */
    msglen +=  1 * dbname->length; /* for signed */
    msglen += sizeof(int64) * dbname->length;
    msglen += dbname_len + schemaname_len + seqname_len;

    /* msgType + msgLen */
    ensure_out_buffer_capacity(1 + msglen, handle);

    Assert(handle->outBuffer != NULL);
    handle->outBuffer[handle->outEnd++] = 'y';
    msglen = htonl(msglen);
    rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &msglen, sizeof(int));
    securec_check(rc, "\0", "\0");
    handle->outEnd += 4;
    
    number1 = htonl(number1);
    rc = memcpy_s(handle->outBuffer + handle->outEnd, sizeof(int), &number1, sizeof(int));
    securec_check(rc, "\0", "\0");
    handle->outEnd += sizeof(int);
    
    ListCell* cell1 = NULL;
    ListCell* cell2 = NULL;
    ListCell* cell3 = NULL;
    ListCell* cell4 = NULL;
    forfour(cell1, last_value, cell2, dbname, cell3, schemaname, cell4, seqname) {
        if (*(int64*)lfirst(cell1) < 0) {
            handle->outBuffer[handle->outEnd++] = '-';
            u_last_value = (uint64)(*(int64*)lfirst(cell1) * -1);
        } else {
            handle->outBuffer[handle->outEnd++] = '+';
            u_last_value = (uint64)(*(int64*)lfirst(cell1));
        }
        low = u_last_value & 0xFFFFFFFF;
        high   = (u_last_value >> 32) & 0xFFFFFFFF;
        low = htonl(low);
        high = htonl(high);

        rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &high, sizeof(high));
        securec_check(rc, "\0", "\0");
        handle->outEnd += sizeof(high);

        rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &low, sizeof(low));
        securec_check(rc, "\0", "\0");
        handle->outEnd += sizeof(low);

        rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, 
            (char*)lfirst(cell2), strlen((char*)lfirst(cell2)) + 1);
        securec_check(rc, "\0", "\0");
        handle->outEnd += strlen((char*)lfirst(cell2)) + 1;
        rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, 
            (char*)lfirst(cell3), strlen((char*)lfirst(cell3)) + 1);
        securec_check(rc, "\0", "\0");
        handle->outEnd += strlen((char*)lfirst(cell3)) + 1;

        rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, 
            (char*)lfirst(cell4), strlen((char*)lfirst(cell4)) + 1);
        securec_check(rc, "\0", "\0");
        handle->outEnd += strlen((char*)lfirst(cell4)) + 1;
    }

    handle->state = DN_CONNECTION_STATE_QUERY;
    pgxc_node_flush(handle);
    CheckUpdateSequenceMsgStatus(exec_handle, (char*)lfirst(list_head(seqname)),
        *(int64*)lfirst(list_head(last_value)));
    pfree_pgxc_all_handles(conn_handles);
}
#endif // ENABLE_MULTIPLE_NODES

void flushSequenceMsg()
{
    StringInfoData retbuf;
    pq_beginmessage(&retbuf, 'y');
    pq_endmessage(&retbuf);
    pq_flush();
}

void  processUpdateSequenceMsg(List* nameList, int64 lastvalue)
{
    RangeVar* sequence = NULL;
    Oid relid;
    Form_pg_sequence seq;
    HeapTupleData seqtuple;
    sequence = makeRangeVarFromNameList(nameList);
    GTM_UUID uuid;
    Buffer buf;
    SeqTable elm = NULL;
    Relation seqrel;
    /*
     * XXX: This is not safe in the presence of concurrent DDL, but acquiring
     * a lock here is more expensive than letting nextval_internal do it,
     * since the latter maintains a cache that keeps us from hitting the lock
     * manager more than once per transaction.	It's not clear whether the
     * performance penalty is material in practice, but for now, we do it this
     * way.
     */
    relid = RangeVarGetRelid(sequence, NoLock, true);
    list_free_deep(nameList);
    if (!OidIsValid(relid)) {
        ereport(
            WARNING, (errcode(ERRCODE_OPERATE_FAILED), errmsg("Failed to find relation %s for sequence update", sequence->relname)));
        return;
    }
    /* open and lock sequence */
    init_sequence(relid, &elm, &seqrel);
    /* lock page' buffer and read tuple */
    seq = read_seq_tuple<Form_pg_sequence>(elm, seqrel, &buf, &seqtuple, &uuid);
    updateNextValForSequence(buf, seq, seqtuple, seqrel, lastvalue);
    relation_close(seqrel, NoLock);
}

#ifdef ENABLE_MULTIPLE_NODES
static void CheckUpdateSequenceMsgStatus(PGXCNodeHandle* exec_handle, const char* seqname, const int64 last_value)
{
    RemoteQueryState* combiner = NULL;
    ereport(DEBUG1,
        (errmodule(MOD_SEQ),
             (errmsg("Check update sequence <%s> %ld command status", seqname, last_value))));

    combiner = CreateResponseCombiner(1, COMBINE_TYPE_NONE);

    if (pgxc_node_receive(1, &exec_handle, NULL))
        ereport(
            ERROR, (errcode(ERRCODE_OPERATE_FAILED), errmsg("Failed to receive response from the remote side")));
    if (handle_response(exec_handle, combiner) != RESPONSE_SEQUENCE_OK)
        ereport(ERROR,
            (errcode(ERRCODE_OPERATE_FAILED),
                errmsg("update sequence %s command failed with error %s", seqname, exec_handle->error)));
    CloseCombiner(combiner);
    ereport(DEBUG1,
        (errmsg("Successfully completed update sequence <%s> to %ld command on all nodes", seqname, last_value)));
}
#endif // ENABLE_MULTIPLE_NODES

static void SaveNextValForSequence(char* dbname, char* schemaname, char *seqname, int64* res)
{
    bool issame = false;
    if (u_sess->xact_cxt.sendSeqName != NULL) {
        ListCell *db_name_cell = NULL;
        ListCell *schema_name_cell = NULL;
        ListCell *seq_name_cell = NULL;
        ListCell *res_cell = NULL;
        forfour(db_name_cell, u_sess->xact_cxt.sendSeqDbName, schema_name_cell, u_sess->xact_cxt.sendSeqSchmaName,
                seq_name_cell, u_sess->xact_cxt.sendSeqName, res_cell, u_sess->xact_cxt.send_result) {
            if (strcmp(seqname, (char*)lfirst(seq_name_cell)) == 0 && strcmp(dbname, (char*)lfirst(db_name_cell)) == 0
                && strcmp(schemaname, (char*)lfirst(schema_name_cell)) == 0) {
                int64* nu = (int64*)lfirst(res_cell);
                pfree_ext(nu);
                lfirst(res_cell) = (void*)res;
                issame = true;
                break;
            }
        }
    }
    if (!issame) {
        u_sess->xact_cxt.sendSeqDbName = lappend(u_sess->xact_cxt.sendSeqDbName, pstrdup(dbname));
        u_sess->xact_cxt.sendSeqSchmaName = lappend(u_sess->xact_cxt.sendSeqSchmaName, pstrdup(schemaname));
        u_sess->xact_cxt.sendSeqName = lappend(u_sess->xact_cxt.sendSeqName, pstrdup(seqname));
        u_sess->xact_cxt.send_result = lappend(u_sess->xact_cxt.send_result, res);
    }
}

static void updateNextValForSequence(Buffer buf, Form_pg_sequence seq, HeapTupleData seqtuple, Relation seqrel,
            int64 result)
{
    Page page;
    bool need_wal = false;
    char *seqname = NULL;
    page = BufferGetPage(buf);
    /* ready to change the on-disk (or really, in-buffer) tuple */

    need_wal = RelationNeedsWAL(seqrel);
    if (IS_PGXC_DATANODE) {
        START_CRIT_SECTION();
        /* when obs archive is on, we need xlog to keep hadr standby get sequence */
        if (need_wal) {
            xl_seq_rec xlrec;
            XLogRecPtr recptr;
            /*
             * We don't log the current state of the tuple, but rather the state
             * as it would appear after "log" more fetches.  This lets us skip
             * that many future WAL records, at the cost that we lose those
             * sequence values if we crash.
             */
            /* set values that will be saved in xlog */
            seq->last_value = result;
            seq->is_called = true;
            seq->log_cnt = 0;
            RelFileNodeRelCopy(xlrec.node, seqrel->rd_node);
            XLogBeginInsert();
            XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);
            XLogRegisterData((char*)&xlrec, sizeof(xl_seq_rec));
            XLogRegisterData((char*)seqtuple.t_data, seqtuple.t_len);
            recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG, seqrel->rd_node.bucketNode);
            PageSetLSN(page, recptr);
        }
        /*
         * We must mark the buffer dirty before doing XLogInsert(); see notes in
         * SyncOneBuffer().  However, we don't apply the desired changes just yet.
         * This looks like a violation of the buffer update protocol, but it is
         * in fact safe because we hold exclusive lock on the buffer.  Any other
         * process, including a checkpoint, that tries to examine the buffer
         * contents will block until we release the lock, and then will see the
         * final state that we install below.
         */
        MarkBufferDirty(buf);
        /* Now update sequence tuple to the intended final state */
        seq->last_value = result; /* last fetched number */
        seq->is_called = true;
        seq->log_cnt = 0;
        END_CRIT_SECTION();
    }
    UnlockReleaseBuffer(buf);
    /* 1 nextval execute on dn, will record in xlog above
     * 2 nextval execute on cn, will notify to dn for record
     * 3 nextval execute direct on another cn, will ignore it for execute direct on is against with write transaction
    */
    if (need_wal && IS_PGXC_COORDINATOR) {
        if (!IsConnFromCoord()) {
            char* dbname = NULL;
            char* schemaname = NULL;
            MemoryContext curr;
            seqname = GetGlobalSeqNameForUpdate(seqrel, &dbname, &schemaname);
            curr = MemoryContextSwitchTo(u_sess->top_transaction_mem_cxt);
            int64* res = (int64*)palloc(sizeof(int64));
            *res = result;
            SaveNextValForSequence(dbname, schemaname, seqname, res);
            MemoryContextSwitchTo(curr);
        } else {
            /* nexval execute direct on cn will not notify dn */
            ereport(WARNING,
                (errmodule(MOD_SEQ),
                    (errmsg("Sequence %s execute nextval direct on cn, will not notify dn ",
                        RelationGetRelationName(seqrel)))));
        }
    }
    return;
}

static int64 GetNextvalResult(SeqTable sess_elm, Relation seqrel, Form_pg_sequence seq, HeapTupleData seqtuple,
                              Buffer buf, int64* rangemax, SeqTable elm, GTM_UUID uuid)
{
    GlobalSeqInfoHashBucket* bucket = NULL;
    int64 range;
    char* seqname = NULL;
    uint32 hash;
    bool get_next_for_datanode = false;
    hash = RelidGetHash(sess_elm->relid);
    bucket = &g_instance.global_seq[hash];
    int64 result = 0;

    range = seq->cache_value; /* how many values to ask from GTM? */
    seqname = GetGlobalSeqName(seqrel, NULL, NULL);
    /*
    * Before connect GTM to get seqno we need check if current thread is able
    * to get connection.
    */
    get_next_for_datanode = range > 1 && IS_PGXC_DATANODE;
    if (get_next_for_datanode) {
        int retry_times = 0;
        const int retry_warning_threshold = 100;
        const int retry_error_threshold = 30000; /* retry exceeds 5: minutes report error */

        AutoMutexLock gtmConnLock(&gtm_conn_lock);

        while (!gtmConnLock.TryLock()) {
            if (retry_times == retry_warning_threshold) {
                elog(WARNING,
                    "Sequence \"%s\" retry %d times to connect GTM on datanode %s",
                    seqname,
                    retry_warning_threshold,
                    g_instance.attr.attr_common.PGXCNodeName);
            }

            if (retry_times > retry_error_threshold) {
                LWLockRelease(GetMainLWLockByIndex(bucket->lock_id));
                UnlockReleaseBuffer(buf);

                ereport(ERROR,
                    (errcode(ERRCODE_CONNECTION_TIMED_OUT),
                        errmsg("Sequence \"%s\" retry %d times to connect GTM on datanode %s",
                            seqname,
                            retry_error_threshold,
                            g_instance.attr.attr_common.PGXCNodeName),
                        errhint("Need reduce the num of concurrent Sequence request")));
            }
            retry_times++;
            pg_usleep(SEQUENCE_GTM_RETRY_SLEEPING_TIME);
        }
        result = (int64)GetNextValGTM(seq, range, rangemax, uuid);
        ereport(DEBUG2,
            (errmodule(MOD_SEQ),
                (errmsg("Sequence %s get ID %ld from GTM, the rangemax is %ld, ",
                    RelationGetRelationName(seqrel),
                    result,
                    *rangemax))));
        CloseGTM();
        gtmConnLock.unLock();
    } else {
        result = (int64)GetNextValGTM(seq, range, rangemax, uuid);

        ereport(DEBUG2,
            (errmodule(MOD_SEQ),
                (errmsg("Sequence %s get ID %ld from GTM, the rangemax is %ld, ",
                    RelationGetRelationName(seqrel),
                    result,
                    *rangemax))));
    }
    pfree_ext(seqname);
    return result;
}

void checkAndDoUpdateSequence()
{
#ifdef ENABLE_MULTIPLE_NODES
    if (u_sess->xact_cxt.sendSeqName != NULL) {
        sendUpdateSequenceMsgToDn(u_sess->xact_cxt.sendSeqDbName, u_sess->xact_cxt.sendSeqSchmaName,
            u_sess->xact_cxt.sendSeqName, u_sess->xact_cxt.send_result);
        list_free_deep(u_sess->xact_cxt.sendSeqDbName);
        list_free_deep(u_sess->xact_cxt.sendSeqSchmaName);
        list_free_deep(u_sess->xact_cxt.sendSeqName);
        list_free_deep(u_sess->xact_cxt.send_result);
        u_sess->xact_cxt.sendSeqDbName = NULL;
        u_sess->xact_cxt.sendSeqSchmaName = NULL;
        u_sess->xact_cxt.sendSeqName = NULL;
        u_sess->xact_cxt.send_result = NULL;
    }
#endif
}

template<typename T_Int, bool large>
static char* Int8or16Out(T_Int num)
{
    char* ret = NULL;
    if (large) {
        ret = DatumGetCString(DirectFunctionCall1(int16out, Int128GetDatum(num)));
    } else {
        ret = DatumGetCString(DirectFunctionCall1(int8out, num));
    }
    return ret;
}
