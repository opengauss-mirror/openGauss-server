/* -------------------------------------------------------------------------
 *
 * sequence.cpp
 *	  PostgreSQL sequences support code.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
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
#include "storage/smgr.h"
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

/*
 * We don't want to log each fetching of a value from a sequence,
 * so we pre-log a few fetches in advance. In the event of
 * crash we can lose (skip over) as many values as we pre-logged.
 */
#define SEQ_LOG_VALS 32
#define GS_NUM_OF_BUCKETS 1024

/*
 * The "special area" of a old version sequence's buffer page looks like this.
 */

/*
 * We store a SeqTable item for every sequence we have touched in the current
 * session.  This is needed to hold onto nextval/currval state.  (We can't
 * rely on the relcache, since it's only, well, a cache, and may decide to
 * discard entries.)
 *
 * XXX We use linear search to find pre-existing SeqTable entries.	This is
 * good when only a small number of sequences are touched in a session, but
 * would suck with many different sequences.  Perhaps use a hashtable someday.
 */
typedef struct SeqTableData {
    struct SeqTableData* next; /* link to next SeqTable object */
    Oid relid;                 /* pg_class OID of this sequence */
    Oid filenode;              /* last seen relfilenode of this sequence */
    LocalTransactionId lxid;   /* xact in which we last did a seq op */
    bool last_valid;           /* do we have a valid "last" value? */
    bool is_cycled;
    int64 last;   /* value last returned by nextval */
    int64 cached; /* last value already cached for nextval */
    /* if last != cached, we have not used up all the cached values */
    int64 increment; /* copy of sequence's increment field */
    /* note that increment is zero until we first do read_seq_tuple() */
    int64 minval;
    int64 maxval;
    int64 startval;
    int64 uuid;
} SeqTableData;

typedef SeqTableData* SeqTable;

static void fill_seq_with_data(Relation rel, HeapTuple tuple);
static int64 nextval_internal(Oid relid);
static Relation lock_and_open_seq(SeqTable seq);
static void init_sequence(Oid relid, SeqTable* p_elm, Relation* p_rel);
static Form_pg_sequence read_seq_tuple(SeqTable elm, Relation rel, Buffer* buf, HeapTuple seqtuple, GTM_UUID* uuid);
#ifdef PGXC
static void init_params(List* options, bool isInit, bool isUseLocalSeq, Form_pg_sequence newm, List** owned_by,
    bool* is_restart, bool* need_seq_rewrite);
#else
static void init_params(
    List* options, bool isInit, bool isUseLocalSeq, Form_pg_sequence newm, List** owned_by, bool* need_seq_rewrite);
#endif
static void do_setval(Oid relid, int64 next, bool iscalled);
static void process_owned_by(const Relation seqrel, List* owned_by);
static GTM_UUID get_uuid_from_tuple(const Form_pg_sequence seq, const Relation rel, const HeapTuple seqtuple);
extern Oid searchSeqidFromExpr(Node* cooked_default);
extern bool check_contains_tbllike_in_multi_nodegroup(CreateStmt* stmt);
static int64 get_uuid_from_uuids(List** uuids);
static uint32 RelidGetHash(Oid seq_relid);
static SeqTable GetGlobalSeqElm(Oid relid, GlobalSeqInfoHashBucket* bucket);
static SeqTable InitGlobalSeqElm(Oid relid);
static SeqTable GetSessSeqElm(Oid relid);
static int64 GetNextvalGlobal(SeqTable sess_elm, Relation seqrel);
static int64 GetNextvalLocal(SeqTable elm, Relation seqrel);
static void ResetvalGlobal(Oid relid);
static int64 FetchLogLocal(int64* next, int64* result, int64* last, int64 maxv, int64 minv, int64 fetch,
    int64 log, int64 incby, int64 rescnt, bool is_cycled, int64 cache, Relation seqrel);
#ifdef ENABLE_MULTIPLE_NODES
static void sendUpdateSequenceMsgToDn(char* seqname, int64 last_value);
static void CheckUpdateSequenceMsgStatus(PGXCNodeHandle* exec_handle, const char* seqname, const int64 last_value);
#endif
static void updateNextValForSequence(Buffer buf, Form_pg_sequence seq, HeapTupleData seqtuple, Relation seqrel, 
                                    int64 result);
static int64 GetNextvalResult(SeqTable sess_elm, Relation seqrel, Form_pg_sequence seq, HeapTupleData seqtuple, 
    Buffer buf, int64* rangemax, SeqTable elm, GTM_UUID uuid);

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
#define SEQUENCE_GTM_RETRY_SLEEPING_TIME 10000L
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
        strcmp(typname, "serial4") == 0 || strcmp(typname, "bigserial") == 0 || strcmp(typname, "serial8") == 0)
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
                    Form_pg_attribute attribute = tupleDesc->attrs[parent_attno - 1];

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
    for (int i = 0; i < GS_NUM_OF_BUCKETS; i++) {
        g_instance.global_seq[i].shb_list = NULL;
        g_instance.global_seq[i].lock_id = FirstGlobalSeqLock + i;
    }
}

static uint32 RelidGetHash(Oid seq_relid)
{
    return ((uint32)seq_relid % GS_NUM_OF_BUCKETS);
}

/*
 * Get sequence elem from bucket
 */
static SeqTable GetGlobalSeqElm(Oid relid, GlobalSeqInfoHashBucket* bucket)
{
    DListCell* elem = NULL;
    SeqTable currseq = NULL;
    
    /* Search sequence from bucket . */
    dlist_foreach_cell(elem, bucket->shb_list)
    {
        currseq = (SeqTable)lfirst(elem);
        if (currseq->relid == relid) {
            break;
        }
    }

    if (elem == NULL) {
        return NULL;
    } 

    return currseq;
}

static SeqTable InitGlobalSeqElm(Oid relid)
{
    SeqTable elm = NULL;
    elm = (SeqTable)MemoryContextAlloc(
        INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER), sizeof(SeqTableData));
    elm->relid = relid;
    elm->filenode = InvalidOid;
    elm->lxid = InvalidLocalTransactionId;
    elm->last_valid = false;
    elm->last = elm->cached = elm->increment = 0;
    elm->minval = 0;
    elm->maxval = 0;
    elm->startval = 0;
    elm->uuid = INVALIDSEQUUID;

    return elm;
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
        elm->cached = elm->last;
    }

    /* If sess_elm first apply from global sequence,
     * should update sess_elm infomation
     */
    if (!sess_elm->last_valid) {
        sess_elm->increment = elm->increment;
        sess_elm->minval = elm->minval;
        sess_elm->maxval = elm->maxval;
        sess_elm->startval = elm->startval;
        sess_elm->is_cycled = elm->is_cycled;
        sess_elm->uuid = elm->uuid;
    }

    if (elm->last != elm->cached) {
        result = elm->last + elm->increment;
        sess_elm->last = result;
        /* if session cache value <= current cached value */
        if (elm->last + sesscache * elm->increment <= elm->cached) {
            elm->last += sesscache * elm->increment;
            sess_elm->cached = elm->last;
        } else {
            sess_elm->cached = elm->last = elm->cached;
        }
        sess_elm->last_valid = true;
        LWLockRelease(GetMainLWLockByIndex(bucket->lock_id)); 
        return result;
    }

    /* lock page' buffer and read tuple */
    seq = read_seq_tuple(elm, seqrel, &buf, &seqtuple, &uuid);
    /* update session sequcence info */
    sess_elm->increment = elm->increment;
    sess_elm->minval = elm->minval;
    sess_elm->maxval = elm->maxval;
    sess_elm->startval = elm->startval;
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
        sess_elm->last = result;
        tmpcached = result + (sesscache - 1) * sess_elm->increment;
        sess_elm->cached = (tmpcached <= rangemax) ? tmpcached : rangemax;
        sess_elm->last_valid = true; 
        /* save info in global cache */
        elm->last = sess_elm->cached; 
        elm->cached = rangemax; /* last fetched number */
    } else {
        /* save info in session cache */ 
        sess_elm->last = result;     /* last returned number */
        sess_elm->cached = rangemax; /* last fetched number */
        sess_elm->last_valid = true;
        /* save info in global cache */
        elm->last = elm->cached = sess_elm->cached;
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


static int64 GetNextvalLocal(SeqTable elm, Relation seqrel)
{
    Buffer buf;
    Page page;
    HeapTupleData seqtuple;
    Form_pg_sequence seq;
    GTM_UUID uuid;
    int64 incby = 0;
    int64 maxv = 0;
    int64 minv = 0;
    int64 cache;
    int64 log;
    int64 fetch;
    int64 last = 0;
    int64 result;
    int64 next = 0;
    int64 rescnt = 0;
    bool logit = false;

    /* lock page' buffer and read tuple */
    seq = read_seq_tuple(elm, seqrel, &buf, &seqtuple, &uuid);
    page = BufferGetPage(buf);

    last = next = result = seq->last_value;
    incby = seq->increment_by;
    maxv = seq->max_value;
    minv = seq->min_value;

    fetch = cache = seq->cache_value;
    log = seq->log_cnt;

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

    log = FetchLogLocal(&next, &result, &last, maxv, minv, fetch, log, incby,
        rescnt, seq->is_cycled, cache, seqrel);
    /* Save info in local cache for temporary sequences */
    elm->last = result; /* last returned number */
    elm->cached = last; /* last fetched number */
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
        seq->last_value = next;
        seq->is_called = true;
        seq->log_cnt = 0;

        RelFileNodeRelCopy(xlrec.node, seqrel->rd_node);

        XLogBeginInsert();
        XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);
        XLogRegisterData((char*)&xlrec, sizeof(xl_seq_rec));
        XLogRegisterData((char*)seqtuple.t_data, seqtuple.t_len);

        recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG, false, seqrel->rd_node.bucketNode);

        PageSetLSN(page, recptr);
    }

    /* Now update sequence tuple to the intended final state */
    seq->last_value = last; /* last fetched number */
    seq->is_called = true;
    seq->log_cnt = log; /* how much is logged */

    END_CRIT_SECTION();

    UnlockReleaseBuffer(buf);

    return result;
}

/*
 * Try to fetch cache [+ log ] numbers
 */
static int64 FetchLogLocal(int64* next, int64* result, int64* last, int64 maxv, int64 minv, int64 fetch,
    int64 log, int64 incby, int64 rescnt, bool is_cycled, int64 cache, Relation seqrel)
{
    while (fetch) {
        /* Result has been checked and received from GTM */
        if (incby > 0) {
            /* ascending sequence */
            if ((maxv >= 0 && *next > maxv - incby) || (maxv < 0 && *next + incby > maxv)) {
                if (rescnt > 0)
                    break; /* stop fetching */
                if (!is_cycled) {
                    char tmp_buf[100];

                    errno_t rc = snprintf_s(tmp_buf, sizeof(tmp_buf), sizeof(tmp_buf) - 1, INT64_FORMAT, maxv);
                    securec_check_ss(rc, "", "");
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
            if ((minv < 0 && *next < minv - incby) || (minv >= 0 && *next + incby < minv)) {
                if (rescnt > 0)
                    break; /* stop fetching */
                if (!is_cycled) {
                    char tmp_buf[100];

                    int iRet = snprintf_s(tmp_buf, sizeof(tmp_buf), sizeof(tmp_buf) - 1, INT64_FORMAT, minv);
                    securec_check_ss(iRet, "\0", "\0");
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
    }
    log -= fetch; /* adjust for any unfetched numbers */
    Assert(log >= 0);

    return log;
}

/*
 * DefineSequence
 *				Creates a new sequence relation
 */
void DefineSequence(CreateSeqStmt* seq)
{
    FormData_pg_sequence newm;
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
    isUseLocalSeq = IS_SINGLE_NODE || isTempNamespace(namespaceOid);

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
    if (!IS_SINGLE_NODE && seq->uuid == INVALIDSEQUUID)
        ereport(ERROR,
            (errcode(ERRCODE_DATA_CORRUPTED), errmsg("Invaild UUID for CREATE SEQUENCE %s.", seq->sequence->relname)));

        /* Check and set all option values */
#ifdef PGXC
    init_params(seq->options, true, isUseLocalSeq, &newm, &owned_by, &is_restart, &need_seq_rewrite);
#else
    init_params(seq->options, true, isUseLocalSeq, &newm, &owned_by, &need_seq_rewrite);
#endif

    /*
     * Create relation (and fill value[] and null[] for the tuple)
     */
    stmt->tableElts = NIL;
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
                coldef->typname = makeTypeNameFromOid(INT8OID, -1);
                coldef->colname = "last_value";
                value[i - 1] = Int64GetDatumFast(newm.last_value);
                break;
            case SEQ_COL_STARTVAL:
                coldef->typname = makeTypeNameFromOid(INT8OID, -1);
                coldef->colname = "start_value";
                value[i - 1] = Int64GetDatumFast(newm.start_value);
#ifdef PGXC /* PGXC_COORD */
                start_value = newm.start_value;
#endif
                break;
            case SEQ_COL_INCBY:
                coldef->typname = makeTypeNameFromOid(INT8OID, -1);
                coldef->colname = "increment_by";
                value[i - 1] = Int64GetDatumFast(newm.increment_by);
#ifdef PGXC /* PGXC_COORD */
                increment = newm.increment_by;
#endif
                break;
            case SEQ_COL_MAXVALUE:
                coldef->typname = makeTypeNameFromOid(INT8OID, -1);
                coldef->colname = "max_value";
                value[i - 1] = Int64GetDatumFast(newm.max_value);
#ifdef PGXC /* PGXC_COORD */
                max_value = newm.max_value;
#endif
                break;
            case SEQ_COL_MINVALUE:
                coldef->typname = makeTypeNameFromOid(INT8OID, -1);
                coldef->colname = "min_value";
                value[i - 1] = Int64GetDatumFast(newm.min_value);
#ifdef PGXC /* PGXC_COORD */
                min_value = newm.min_value;
#endif
                break;
            case SEQ_COL_CACHE:
                coldef->typname = makeTypeNameFromOid(INT8OID, -1);
                coldef->colname = "cache_value";
                value[i - 1] = Int64GetDatumFast(newm.cache_value);
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

    seqoid = DefineRelation(stmt, RELKIND_SEQUENCE, seq->ownerId);
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
        if ((status = CreateSequenceWithUUIDGTM(newm, seq->uuid)) < 0) {
            if (status == GTM_RESULT_COMM_ERROR)
                seqerrcode = ERRCODE_CONNECTION_FAILURE;
            ereport(ERROR, (errcode(seqerrcode), errmsg("GTM error, could not create sequence")));
        }

        /* Define a callback to drop sequence on GTM in case transaction fails  */
        register_sequence_cb(seq->uuid, GTM_CREATE_SEQ);
    }
#endif
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
void ResetSequence(Oid seq_relid)
{
    Relation seq_rel;
    SeqTable elm = NULL;
    Form_pg_sequence seq;
    Buffer buf;
    HeapTupleData seqtuple;
    HeapTuple tuple;
    errno_t rc;
    rc = memset_s(&seqtuple, sizeof(seqtuple), 0, sizeof(seqtuple));
    securec_check_c(rc, "\0", "\0");

    /*
     * Read the old sequence.  This does a bit more work than really
     * necessary, but it's simple, and we do want to double-check that it's
     * indeed a sequence.
     */
    init_sequence(seq_relid, &elm, &seq_rel);
    GTM_UUID uuid;
    (void)read_seq_tuple(elm, seq_rel, &buf, &seqtuple, &uuid);

    /*
     * Copy the existing sequence tuple.
     */
    tuple = (HeapTuple)tableam_tops_copy_tuple(&seqtuple);

    /* Now we're done with the old page */
    UnlockReleaseBuffer(buf);

    /*
     * Modify the copied tuple to execute the restart (compare the RESTART
     * action in AlterSequence)
     */
    seq = (Form_pg_sequence)GETSTRUCT(tuple);
    seq->last_value = -1; /* disable the unreliable last_value */
    seq->is_called = false;
    seq->log_cnt = 0;

    /*
     * Create a new storage file for the sequence.	We want to keep the
     * sequence's relfrozenxid at 0, since it won't contain any unfrozen XIDs.
     */
    RelationSetNewRelfilenode(seq_rel, InvalidTransactionId);

    /*
     * Insert the modified tuple into the new storage file.
     */
    fill_seq_with_data(seq_rel, tuple);

    /* Clear local cache so that we don't think we have cached numbers */
    /* Note that we do not change the currval() state */
    elm->cached = elm->last;

    relation_close(seq_rel, NoLock);
}

/*
 * Initialize a sequence's relation with the specified tuple as content
 */
static void fill_seq_with_data(Relation rel, HeapTuple tuple)
{
    Buffer buf;
    Page page;
    sequence_magic* sm = NULL;
    HeapPageHeader phdr;

    /* Initialize first page of relation with special magic number */
    buf = ReadBuffer(rel, P_NEW);
    Assert(BufferGetBlockNumber(buf) == 0);

    page = BufferGetPage(buf);

    PageInit(page, BufferGetPageSize(buf), sizeof(sequence_magic), true);
    sm = (sequence_magic*)PageGetSpecialPointer(page);
    sm->magic = SEQ_MAGIC;

    phdr = (HeapPageHeader)page;
    phdr->pd_xid_base = u_sess->utils_cxt.RecentXmin - FirstNormalTransactionId;
    phdr->pd_multi_base = 0;

    /* hack: ensure heap_insert will insert on the just-created page */
    RelationSetTargetBlock(rel, 0);

    /* Now insert sequence tuple */
    (void)simple_heap_insert(rel, tuple);

    Assert(ItemPointerGetOffsetNumber(&(tuple->t_self)) == FirstOffsetNumber);

    /*
     * Two special hacks here:
     *
     * 1. Since VACUUM does not process sequences, we have to force the tuple
     * to have xmin = FrozenTransactionId now.	Otherwise it would become
     * invisible to SELECTs after 2G transactions.	It is okay to do this
     * because if the current transaction aborts, no other xact will ever
     * examine the sequence tuple anyway.
     *
     * 2. Even though heap_insert emitted a WAL log record, we have to emit an
     * XLOG_SEQ_LOG record too, since (a) the heap_insert record will not have
     * the right xmin, and (b) REDO of the heap_insert record would re-init
     * page and sequence magic number would be lost.  This means two log
     * records instead of one :-(
     */
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

    START_CRIT_SECTION();

    {
        /*
         * Note that the "tuple" structure is still just a local tuple record
         * created by heap_form_tuple; its t_data pointer doesn't point at the
         * disk buffer.  To scribble on the disk buffer we need to fetch the
         * item pointer.  But do the same to the local tuple, since that will
         * be the source for the WAL log record, below.
         */
        ItemId itemId;
        Item item;

        itemId = PageGetItemId((Page)page, FirstOffsetNumber);
        item = PageGetItem((Page)page, itemId);

        HeapTupleHeaderSetXmin(page, (HeapTupleHeader)item, FrozenTransactionId);
        HeapTupleHeaderSetXminFrozen((HeapTupleHeader)item);

        HeapTupleHeaderSetXmin(page, tuple->t_data, FrozenTransactionId);
        HeapTupleHeaderSetXminFrozen(tuple->t_data);
    }

    MarkBufferDirty(buf);

    /* XLOG stuff */
    if (RelationNeedsWAL(rel)) {
        xl_seq_rec xlrec;
        XLogRecPtr recptr;
        tuple->t_data->t_ctid = tuple->t_self;
        RelFileNodeRelCopy(xlrec.node, rel->rd_node);

        XLogBeginInsert();
        XLogRegisterBuffer(0, buf, REGBUF_WILL_INIT);

        XLogRegisterData((char*)&xlrec, sizeof(xl_seq_rec));
        XLogRegisterData((char*)tuple->t_data, tuple->t_len);

        recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG, false, rel->rd_node.bucketNode);

        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();

    UnlockReleaseBuffer(buf);
}

/*
 * AlterSequence
 *
 * Modify the definition of a sequence relation
 * For now we only support alter sequence owned_by, owner and maxvalue.
 * Alter sequence maxvalue needs update info in GTM.
 */
void AlterSequence(AlterSeqStmt* stmt)
{
    Oid relid;
    SeqTable elm = NULL;
    Relation seqrel;
    Buffer buf;
    HeapTupleData seqtuple;
    HeapTuple tuple = NULL;
    Form_pg_sequence newm = NULL;
    List* owned_by = NIL;
    bool isUseLocalSeq = false;
#ifdef PGXC
    bool is_restart = false;
#endif
    bool need_seq_rewrite = false;
    
    /* Open and lock sequence. */
    relid = RangeVarGetRelid(stmt->sequence, ShareRowExclusiveLock, stmt->missing_ok);
    if (relid == InvalidOid) {
        ereport(NOTICE, (errmsg("relation \"%s\" does not exist, skipping", stmt->sequence->relname)));
        return;
    }

    init_sequence(relid, &elm, &seqrel);

    /* Must be owner or have alter privilege of the sequence. */
    AclResult aclresult = pg_class_aclcheck(relid, GetUserId(), ACL_ALTER);
    if (aclresult != ACLCHECK_OK && !pg_class_ownercheck(relid, GetUserId())) {
        aclcheck_error(ACLCHECK_NO_PRIV, ACL_KIND_CLASS, stmt->sequence->relname);
    }

    /* temp sequence and single_node do not need gtm, they only use info on local node */
    isUseLocalSeq = RelationIsLocalTemp(seqrel) || IS_SINGLE_NODE;

    /* lock page' buffer and read tuple into new sequence structure */
    GTM_UUID uuid;
    (void)read_seq_tuple(elm, seqrel, &buf, &seqtuple, &uuid);

    /* Copy the existing sequence tuple. */
    tuple = (HeapTuple)tableam_tops_copy_tuple(&seqtuple);
    UnlockReleaseBuffer(buf);

    newm = (Form_pg_sequence)GETSTRUCT(tuple);

    /* Check and set new values */
#ifdef PGXC
    init_params(stmt->options, false, isUseLocalSeq, newm, &owned_by, &is_restart, &need_seq_rewrite);
#else
    init_params(stmt->options, false, isUseLocalSeq, newm, &owned_by, &need_seq_rewrite);
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
        if ((status = AlterSequenceGTM(newm->uuid,
                 newm->increment_by,
                 newm->min_value,
                 newm->max_value,
                 newm->start_value,
                 newm->last_value,
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
    elm->cached = elm->last;

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
        RelationSetNewRelfilenode(seqrel, InvalidTransactionId);
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

    relation_close(seqrel, NoLock);
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
    PG_RETURN_INT64(nextval_internal(relid));
}

Datum nextval_oid(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);

    PG_RETURN_INT64(nextval_internal(relid));
}

static int64 nextval_internal(Oid relid)
{
    SeqTable elm = NULL;
    Relation seqrel;   
    int64 result;
    bool is_use_local_seq = false;
	
    if (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE) {
        ereport(ERROR, (errmsg("Standby do not support nextval, please do it in primary!")));
    }

    /* open and lock sequence */
    init_sequence(relid, &elm, &seqrel);

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

        ereport(DEBUG2,
            (errmodule(MOD_SEQ),
                (errmsg("Sequence %s retrun ID %ld from cache, the cached is %ld, ",
                    RelationGetRelationName(seqrel),
                    elm->last,
                    elm->cached))));
        
        relation_close(seqrel, NoLock);
        u_sess->cmd_cxt.last_used_seq = elm;
        return elm->last;
    }

    /* If don't have cached value, we should fetch some. */
    if (!is_use_local_seq) {
        result = GetNextvalGlobal(elm, seqrel);
    } else {
        result = GetNextvalLocal(elm, seqrel);
    }

    u_sess->cmd_cxt.last_used_seq = elm;
    ereport(DEBUG2,
        (errmodule(MOD_SEQ),
            (errmsg("Sequence %s retrun ID from nextval %ld, ", RelationGetRelationName(seqrel), result))));
    
    relation_close(seqrel, NoLock);

    return result;
}

Datum currval_oid(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    int64 result;
    SeqTable elm = NULL;
    Relation seqrel;

    if (!IS_SINGLE_NODE && !u_sess->attr.attr_common.enable_beta_features) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("currval function is not supported")));
    }

    /* open and lock sequence */
    init_sequence(relid, &elm, &seqrel);

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

    PG_RETURN_INT64(result);
}

Datum lastval(PG_FUNCTION_ARGS)
{
    Relation seqrel;
    int64 result;

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

    PG_RETURN_INT64(result);
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
static void do_setval(Oid relid, int64 next, bool iscalled)
{
    SeqTable elm = NULL;
    Relation seqrel;
    Buffer buf;
    HeapTupleData seqtuple;
    Form_pg_sequence seq;
    int rc = 0;
    bool isUseLocalSeq = false;
#ifdef PGXC
    bool is_temp = false;
#endif
    if (t_thrd.postmaster_cxt.HaShmData->current_mode == STANDBY_MODE) {
        ereport(ERROR, (errmsg("Standby do not support setval, please do it in primary!")));
    }

    /* open and lock sequence */
    init_sequence(relid, &elm, &seqrel);

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
    seq = read_seq_tuple(elm, seqrel, &buf, &seqtuple, &uuid);

    if ((next < seq->min_value) || (next > seq->max_value)) {
        char bufv[100], bufm[100], bufx[100];

        rc = snprintf_s(bufv, sizeof(bufv), sizeof(bufv) - 1, INT64_FORMAT, next);
        securec_check_ss(rc, "\0", "\0");
        rc = snprintf_s(bufm, sizeof(bufm), sizeof(bufm) - 1, INT64_FORMAT, seq->min_value);
        securec_check_ss(rc, "\0", "\0");
        rc = snprintf_s(bufx, sizeof(bufx), sizeof(bufx) - 1, INT64_FORMAT, seq->max_value);
        securec_check_ss(rc, "\0", "\0");
        ereport(ERROR,
            (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                errmsg("setval: value %s is out of bounds for sequence \"%s\" (%s..%s)",
                    bufv,
                    RelationGetRelationName(seqrel),
                    bufm,
                    bufx)));
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

        elm->cached = elm->last;
    } else {
#endif

        /* Set the currval() state only if iscalled = true */
        if (iscalled) {
            elm->last = next; /* last returned number */
            elm->last_valid = true;
        }

        /* In any case, forget any future cached numbers */
        elm->cached = elm->last;

        /* ready to change the on-disk (or really, in-buffer) tuple */
        START_CRIT_SECTION();

        /* keep the last value if isUseLocalSeq */
        if (isUseLocalSeq)
            seq->last_value = next; /* last fetched number */
        else
            seq->last_value = -1; /* disable the unreliable last_value */
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

            recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG, false, seqrel->rd_node.bucketNode);

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
 * If call setval, in any case ,we shoule forget all catched values.
 */ 
static void ResetvalGlobal(Oid relid)
{
    SeqTable elm = NULL;
    GlobalSeqInfoHashBucket* bucket = NULL;
    uint32 hash = RelidGetHash(relid);

    bucket = &g_instance.global_seq[hash];

    (void)LWLockAcquire(GetMainLWLockByIndex(bucket->lock_id), LW_EXCLUSIVE);

    elm = GetGlobalSeqElm(relid, bucket);
    if (elm != NULL) {
        elm->last = elm->cached;
    }

    LWLockRelease(GetMainLWLockByIndex(bucket->lock_id));
}

/*
 * Implement the 2 arg setval procedure.
 * See do_setval for discussion.
 */
Datum setval_oid(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    int64 next = PG_GETARG_INT64(1);

    do_setval(relid, next, true);

    PG_RETURN_INT64(next);
}

/*
 * Implement the 3 arg setval procedure.
 * See do_setval for discussion.
 */
Datum setval3_oid(PG_FUNCTION_ARGS)
{
    Oid relid = PG_GETARG_OID(0);
    int64 next = PG_GETARG_INT64(1);
    bool iscalled = PG_GETARG_BOOL(2);

    do_setval(relid, next, iscalled);

    PG_RETURN_INT64(next);
}

/*
 * Open the sequence and acquire RowExclusiveLock if needed
 *
 * If we haven't touched the sequence already in this transaction,
 * we need to acquire a lock.	We arrange for the lock to
 * be owned by the top transaction, so that we don't need to do it
 * more than once per xact.
 */
static Relation lock_and_open_seq(SeqTable seq)
{
    LocalTransactionId thislxid = t_thrd.proc->lxid;

    /* Get the lock if not already held in this xact */
    if (seq->lxid != thislxid) {
        ResourceOwner currentOwner;

        currentOwner = t_thrd.utils_cxt.CurrentResourceOwner;
        PG_TRY();
        {
            t_thrd.utils_cxt.CurrentResourceOwner = t_thrd.utils_cxt.TopTransactionResourceOwner;
            LockRelationOid(seq->relid, RowExclusiveLock);
        }
        PG_CATCH();
        {
            /* Ensure CurrentResourceOwner is restored on error */
            t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;
            PG_RE_THROW();
        }
        PG_END_TRY();
        t_thrd.utils_cxt.CurrentResourceOwner = currentOwner;

        /* Flag that we have a lock in the current xact */
        seq->lxid = thislxid;
    }

    /* We now know we have the lock, and can safely open the rel */
    return relation_open(seq->relid, NoLock);
}

/*
 * Given a relation OID, open and lock the sequence.  p_elm and p_rel are
 * output parameters.
 */
static void init_sequence(Oid relid, SeqTable* p_elm, Relation* p_rel)
{
    SeqTable elm = NULL;
    Relation seqrel;

    elm = GetSessSeqElm(relid);

    /*
     * Open the sequence relation.
     */
    seqrel = lock_and_open_seq(elm);

    if (seqrel->rd_rel->relkind != RELKIND_SEQUENCE) {
        Oid nspoid = RelationGetNamespace(seqrel);

        if (seqrel->rd_rel->relpersistence == RELPERSISTENCE_TEMP) {
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("\"%s.%s\" is not a sequence", get_namespace_name(nspoid), RelationGetRelationName(seqrel)),
                    errdetail("Please make sure using the correct schema")));
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg(
                        "\"%s.%s\" is not a sequence", get_namespace_name(nspoid), RelationGetRelationName(seqrel))));
        }
    }
    /*
     * If the sequence has been transactionally replaced since we last saw it,
     * discard any cached-but-unissued values.	We do not touch the currval()
     * state, however.
     */
    if (seqrel->rd_rel->relfilenode != elm->filenode) {
        elm->filenode = seqrel->rd_rel->relfilenode;
        elm->cached = elm->last;
    }

    /* Return results */
    *p_elm = elm;
    *p_rel = seqrel;
}

static SeqTable GetSessSeqElm(Oid relid)
{
    SeqTable elm = NULL;
    /* Look to see if we already have a seqtable entry for relation */
    for (elm = u_sess->cmd_cxt.seqtab; elm != NULL; elm = elm->next) {
        if (elm->relid == relid)
            break;
    }

    /*
    * Allocate new seqtable entry if we didn't find one.
    *
    * NOTE: seqtable entries remain in the list for the life of a backend. If
    * the sequence itself is deleted then the entry becomes wasted memory,
    * but it's small enough that this should not matter.
    */
    if (elm == NULL) {
        /*
        * Time to make a new seqtable entry.  These entries live as long as
        * the backend does, so we use plain malloc for them.
        */
        elm = (SeqTable)MemoryContextAlloc(
            SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER), sizeof(SeqTableData));
        elm->relid = relid;
        elm->filenode = InvalidOid;
        elm->lxid = InvalidLocalTransactionId;
        elm->last_valid = false;
        elm->last = elm->cached = elm->increment = 0;
        elm->minval = 0;
        elm->maxval = 0;
        elm->startval = 0;
        elm->uuid = INVALIDSEQUUID;
        
        elm->next = u_sess->cmd_cxt.seqtab;
        u_sess->cmd_cxt.seqtab = elm;
    }

    return elm;
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
    (void)read_seq_tuple(&elm, rel, &buf, &seqtuple, &uuid);

    /* Now we're done with the old page */
    UnlockReleaseBuffer(buf);

    return uuid;
}

static GTM_UUID get_uuid_from_tuple(const Form_pg_sequence seq, const Relation rel, const HeapTuple seqtuple)
{
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
static Form_pg_sequence read_seq_tuple(SeqTable elm, Relation rel, Buffer* buf, HeapTuple seqtuple, GTM_UUID* uuid)
{
    Page page;
    ItemId lp;
    Form_pg_sequence seq;
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
     * Previous releases of Postgres neglected to prevent SELECT FOR UPDATE on
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

    seq = (Form_pg_sequence)GETSTRUCT(seqtuple);

    /* this is a handy place to update our copy of the increment */
    elm->increment = seq->increment_by;
    elm->minval = seq->min_value;
    elm->maxval = seq->max_value;
    elm->startval = seq->start_value;
    elm->is_cycled = seq->is_cycled;

    elm->uuid = *uuid = get_uuid_from_tuple(seq, rel, seqtuple);

    return seq;
}

static void check_value_min_max(int64 value, int64 min_value, int64 max_value)
{
    int err_rc = 0;
    /* crosscheck RESTART (or current value, if changing MIN/MAX) */
    if (value < min_value) {
        char bufs[100], bufm[100];

        err_rc = snprintf_s(bufs, sizeof(bufs), sizeof(bufs) - 1, INT64_FORMAT, value);
        securec_check_ss(err_rc, "\0", "\0");
        err_rc = snprintf_s(bufm, sizeof(bufm), sizeof(bufm) - 1, INT64_FORMAT, min_value);
        securec_check_ss(err_rc, "\0", "\0");
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("RESTART value (%s) cannot be less than MINVALUE (%s)", bufs, bufm)));
    }
    if (value > max_value) {
        char bufs[100], bufm[100];

        err_rc = snprintf_s(bufs, sizeof(bufs), sizeof(bufs) - 1, INT64_FORMAT, value);
        securec_check_ss(err_rc, "\0", "\0");
        err_rc = snprintf_s(bufm, sizeof(bufm), sizeof(bufm) - 1, INT64_FORMAT, max_value);
        securec_check_ss(err_rc, "\0", "\0");
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("RESTART value (%s) cannot be greater than MAXVALUE (%s)", bufs, bufm)));
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
    static void init_params(List* options, bool isInit, bool isUseLocalSeq, Form_pg_sequence newm, List** owned_by,
        bool* is_restart, bool* need_seq_rewrite)
#else
    static void init_params(
        List* options, bool isInit, bool isUseLocalSeq, Form_pg_sequence newm, List** owned_by, bool* need_seq_rewrite)
#endif
{
    DefElem* start_value = NULL;
    DefElem* restart_value = NULL;
    DefElem* increment_by = NULL;
    DefElem* max_value = NULL;
    DefElem* min_value = NULL;
    DefElem* cache_value = NULL;
    DefElem* is_cycled = NULL;
    ListCell* option = NULL;
    int err_rc = 0;

#ifdef PGXC
    *is_restart = false;
#endif

    *owned_by = NIL;

    foreach (option, options) {
        DefElem* defel = (DefElem*)lfirst(option);

        /* Now we only support ALTER SEQUENCE OWNED BY and MAXVALUE to support upgrade. */
        if (!isInit) {
            /* isInit is true for DefineSequence, false for AlterSequence, we use it
             * to differentiate them
             */
            if (strcmp(defel->defname, "owned_by") != 0 && strcmp(defel->defname, "maxvalue") != 0) {
                ereport(
                    ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("ALTER SEQUENCE is not yet supported.")));
            }
        }

        if (strcmp(defel->defname, "increment") == 0) {
            if (increment_by != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            increment_by = defel;
            *need_seq_rewrite = true;
        } else if (strcmp(defel->defname, "start") == 0) {
            if (start_value != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            start_value = defel;
            *need_seq_rewrite = true;
        } else if (strcmp(defel->defname, "restart") == 0) {
            if (restart_value != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            restart_value = defel;
            *need_seq_rewrite = true;
        } else if (strcmp(defel->defname, "maxvalue") == 0) {
            if (max_value != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            max_value = defel;
            *need_seq_rewrite = true;
        } else if (strcmp(defel->defname, "minvalue") == 0) {
            if (min_value != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            min_value = defel;
            *need_seq_rewrite = true;
        } else if (strcmp(defel->defname, "cache") == 0) {
            if (cache_value != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            cache_value = defel;
            *need_seq_rewrite = true;
        } else if (strcmp(defel->defname, "cycle") == 0) {
            if (is_cycled != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            is_cycled = defel;
            *need_seq_rewrite = true;
        } else if (strcmp(defel->defname, "owned_by") == 0) {
            if (*owned_by != NIL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            *owned_by = defGetQualifiedName(defel);
        } else {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("option \"%s\" not recognized", defel->defname)));
        }
    }

    /*
     * We must reset log_cnt when isInit or when changing any parameters
     * that would affect future nextval allocations.
     */
    if (isInit)
        newm->log_cnt = 0;

    /* INCREMENT BY */
    if (increment_by != NULL) {
        newm->increment_by = defGetInt64(increment_by);
        if (newm->increment_by == 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("INCREMENT must not be zero")));
        newm->log_cnt = 0;
    } else if (isInit)
        newm->increment_by = 1;

    /* CYCLE */
    if (is_cycled != NULL) {
        newm->is_cycled = intVal(is_cycled->arg);
        Assert(BoolIsValid(newm->is_cycled));
        newm->log_cnt = 0;
    } else if (isInit)
        newm->is_cycled = false;

    /* MAXVALUE (null arg means NO MAXVALUE) */
    if (max_value != NULL && max_value->arg) {
        newm->max_value = defGetInt64(max_value);
        newm->log_cnt = 0;
    } else if (isInit || max_value != NULL) {
        if (newm->increment_by > 0)
            newm->max_value = SEQ_MAXVALUE; /* ascending seq */
        else
            newm->max_value = -1; /* descending seq */
        newm->log_cnt = 0;
    }

    /* MINVALUE (null arg means NO MINVALUE) */
    if (min_value != NULL && min_value->arg) {
        newm->min_value = defGetInt64(min_value);
        newm->log_cnt = 0;
    } else if (isInit || min_value != NULL) {
        if (newm->increment_by > 0)
            newm->min_value = 1; /* ascending seq */
        else
            newm->min_value = SEQ_MINVALUE; /* descending seq */
        newm->log_cnt = 0;
    }

    /* crosscheck min/max */
    if (newm->min_value >= newm->max_value) {
        char bufm[100], bufx[100];
        err_rc = snprintf_s(bufm, sizeof(bufm), sizeof(bufm) - 1, INT64_FORMAT, newm->min_value);
        securec_check_ss(err_rc, "\0", "\0");
        err_rc = snprintf_s(bufx, sizeof(bufx), sizeof(bufx) - 1, INT64_FORMAT, newm->max_value);
        securec_check_ss(err_rc, "\0", "\0");
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("MINVALUE (%s) must be less than MAXVALUE (%s)", bufm, bufx)));
    }

    /* START WITH */
    if (start_value != NULL)
        newm->start_value = defGetInt64(start_value);
    else if (isInit) {
        newm->start_value = (newm->increment_by > 0) ? newm->min_value : newm->max_value;
    }

    /* crosscheck START */
    check_value_min_max(newm->start_value, newm->min_value, newm->max_value);

    /* RESTART [WITH] */
    if (restart_value != NULL) {
        newm->last_value = (restart_value->arg != NULL) ? defGetInt64(restart_value)
                                                        : newm->start_value;
#ifdef PGXC
        *is_restart = true;
#endif
        newm->is_called = false;
        newm->log_cnt = 0;
    } else if (isInit) {
        newm->last_value = (isUseLocalSeq) ? newm->start_value : -1;
        newm->is_called = false;
    }

    if (isUseLocalSeq) {
        /* crosscheck RESTART (or current value, if changing MIN/MAX) */
        check_value_min_max(newm->last_value, newm->min_value, newm->max_value);
    }

    /* CACHE */
    if (cache_value != NULL) {
        newm->cache_value = defGetInt64(cache_value);
        if (newm->cache_value <= 0) {
            char buf[100];

            err_rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, INT64_FORMAT, newm->cache_value);
            securec_check_ss(err_rc, "\0", "\0");
            ereport(
                ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("CACHE (%s) must be greater than zero", buf)));
        } else if (newm->cache_value > 1) {
            if ((newm->increment_by > 0 && max_value != NULL && max_value->arg != NULL) ||
                (newm->increment_by < 0 && min_value != NULL && min_value->arg != NULL))
                ereport(NOTICE,
                    (errmsg("Not advised to use MAXVALUE or MINVALUE together with CACHE."),
                        errdetail("If CACHE is defined, some sequence values may be wasted, causing available sequence "
                                  "numbers to be less than expected.")));
        }

        newm->log_cnt = 0;
    } else if (isInit)
        newm->cache_value = 1;
}

#ifdef PGXC
/*
 * GetGlobalSeqName
 *
 * Returns a global sequence name adapted to GTM
 * Name format is dbname.schemaname.seqname
 * so as to identify in a unique way in the whole cluster each sequence
 */
char* GetGlobalSeqName(Relation seqrel, const char* new_seqname, const char* new_schemaname)
{
    char* seqname = NULL;
    char* dbname = NULL;
    char* schemaname = NULL;
    char* relname = NULL;
    int charlen;

    /* Get all the necessary relation names */
    dbname = get_database_name(seqrel->rd_node.dbNode);
    
    if (dbname == NULL) {
        ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for database %u", seqrel->rd_node.dbNode)));
    }

    if (new_seqname != NULL)
        relname = (char*)new_seqname;
    else
        relname = RelationGetRelationName(seqrel);

    if (new_schemaname != NULL)
        schemaname = (char*)new_schemaname;
    else
        schemaname = get_namespace_name(RelationGetNamespace(seqrel));
    
    if (schemaname == NULL) {
        ereport(ERROR,
                    (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                        errmsg("cache lookup failed for schema %u", RelationGetNamespace(seqrel))));
    }

    /* Calculate the global name size including the dots and \0 */
    charlen = strlen(dbname) + strlen(schemaname) + strlen(relname) + 3;
    seqname = (char*)palloc(charlen);

    /* Form a unique sequence name with schema and database name for GTM */
    int ret = snprintf_s(seqname, charlen, charlen - 1, "%s.%s.%s", dbname, schemaname, relname);
    securec_check_ss(ret, "\0", "\0");
    pfree_ext(dbname);
    pfree_ext(schemaname);

    return seqname;
}

/*
 * IsTempSequence
 *
 * Determine if given sequence is temporary or not.
 */
bool IsTempSequence(Oid relid)
{
    Relation seqrel;
    bool res = false;
    SeqTable elm = NULL;

    /* open and lock sequence */
    init_sequence(relid, &elm, &seqrel);

    res = RelationIsLocalTemp(seqrel);
    relation_close(seqrel, NoLock);
    return res;
}
#endif

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
 * Return sequence parameters
 */
void get_sequence_params(Relation rel, int64* uuid, int64* start, int64* increment, int64* maxvalue, int64* minvalue,
    int64* cache, bool* cycle)
{
    Buffer buf;
    SeqTableData elm;
    HeapTupleData seqtuple;
    Form_pg_sequence seq;

    seq = read_seq_tuple(&elm, rel, &buf, &seqtuple, uuid);

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
    TupleDesc tupdesc;
    Datum values[5];
    bool isnull[5];
    SeqTable elm = NULL;
    Relation seqrel;
    Buffer buf;
    HeapTupleData seqtuple;
    Form_pg_sequence seq;

    /* open and lock sequence */
    init_sequence(relid, &elm, &seqrel);

    if (pg_class_aclcheck(relid, GetUserId(), ACL_SELECT | ACL_UPDATE | ACL_USAGE) != ACLCHECK_OK)
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied for sequence %s", RelationGetRelationName(seqrel))));

    tupdesc = CreateTemplateTupleDesc(5, false, TAM_HEAP);
    TupleDescInitEntry(tupdesc, (AttrNumber)1, "start_value", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)2, "minimum_value", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)3, "maximum_value", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)4, "increment", INT8OID, -1, 0);
    TupleDescInitEntry(tupdesc, (AttrNumber)5, "cycle_option", BOOLOID, -1, 0);

    BlessTupleDesc(tupdesc);

    errno_t rc = memset_s(isnull, sizeof(isnull), 0, sizeof(isnull));
    securec_check(rc, "\0", "\0");

    GTM_UUID uuid;
    seq = read_seq_tuple(elm, seqrel, &buf, &seqtuple, &uuid);

    values[0] = Int64GetDatum(seq->start_value);
    values[1] = Int64GetDatum(seq->min_value);
    values[2] = Int64GetDatum(seq->max_value);
    values[3] = Int64GetDatum(seq->increment_by);
    values[4] = BoolGetDatum(seq->is_cycled);

    UnlockReleaseBuffer(buf);
    relation_close(seqrel, NoLock);

    return HeapTupleGetDatum(heap_form_tuple(tupdesc, values, isnull));
}

void seq_redo(XLogReaderState* record)
{
    uint8 info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
    RedoBufferInfo buffer;
    char* item = NULL;
    Size itemsz;
    xl_seq_rec* xlrec = (xl_seq_rec*)XLogRecGetData(record);

    if (info != XLOG_SEQ_LOG) {
        elog(PANIC, "seq_redo: unknown op code %u", (uint)info);
    }

    XLogInitBufferForRedo(record, 0, &buffer);

    /*
     * We must always reinit the page and reinstall the magic number (see
     * comments in fill_seq_with_data).  However, since this WAL record type
     * is also used for updating sequences, it's possible that a hot-standby
     * backend is examining the page concurrently; so we mustn't transiently
     * trash the buffer.  The solution is to build the correct new page
     * contents in local workspace and then memcpy into the buffer.  Then only
     * bytes that are supposed to change will change, even transiently. We
     * must palloc the local page for alignment reasons.
     */
    item = (char*)xlrec + sizeof(xl_seq_rec);
    itemsz = XLogRecGetDataLen(record) - sizeof(xl_seq_rec);

    seqRedoOperatorPage(&buffer, item, itemsz);

    MarkBufferDirty(buffer.buf);
    UnlockReleaseBuffer(buffer.buf);

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
 * Register a callback for a sequence rename drop on GTM
 * If need to implement rename sequence, this also need to
 * change to hte t_thrd.top_mem_cxt
 */
void register_sequence_rename_cb(const char* oldseqname, const char* newseqname)
{
    rename_sequence_callback_arg* args = NULL;
    char* oldseqnamearg = NULL;
    char* newseqnamearg = NULL;

    /* We change the memory from u_sess->top_transaction_mem_cxt to t_thrd.top_mem_cxt,
     * because we postpone the CallSequenceCallback after CN/DN commit.
     * not same as CallGTMCallback which is called in PreparedTranscaton phase.
     * The u_sess->top_transaction_mem_cxt is released, after Prepared finished.
     * If CallSequenceCallback is called in Prepared Phase, it will be
     * difficult to rollback if the transaction is abort after prepared.
     */
    args = (rename_sequence_callback_arg*)MemoryContextAlloc(
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER), sizeof(rename_sequence_callback_arg));

    oldseqnamearg = (char*)MemoryContextAlloc(
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER), strlen(oldseqname) + 1);
    newseqnamearg = (char*)MemoryContextAlloc(
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER), strlen(newseqname) + 1);

    errno_t errorno = sprintf_s(oldseqnamearg, strlen(oldseqname) + 1, "%s", oldseqname);
    securec_check_ss(errorno, "\0", "\0");
    errorno = sprintf_s(newseqnamearg, strlen(newseqname) + 1, "%s", newseqname);
    securec_check_ss(errorno, "\0", "\0");

    args->oldseqname = oldseqnamearg;
    args->newseqname = newseqnamearg;

    RegisterSequenceCallback(rename_sequence_cb, (void*)args);
}

/*
 * Callback a sequence rename
 */
#ifndef ENABLE_MULTIPLE_NODES
void rename_sequence_cb(GTMEvent event, void* args)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}
#else
void rename_sequence_cb(GTMEvent event, void* args)
{
    rename_sequence_callback_arg* cbargs = (rename_sequence_callback_arg*)args;
    char* newseqname = cbargs->newseqname;
    char* oldseqname = cbargs->oldseqname;
    int err = 0;
    int seqerrcode = ERRCODE_SQL_STATEMENT_NOT_YET_COMPLETE;

    /*
     * A sequence is here renamed to its former name only when a transaction
     * that involved a sequence rename was dropped.
     */
    switch (event) {
        case GTM_EVENT_ABORT:
            /*
             * Here sequence is renamed to its former name
             * so what was new becomes old.
             */
            err = RenameSequenceGTM(newseqname, oldseqname);
            break;
        case GTM_EVENT_COMMIT:
        case GTM_EVENT_PREPARE:
            /* Nothing to do */
            break;
        default:
            Assert(0);
    }

    if (err == GTM_RESULT_COMM_ERROR)
        seqerrcode = ERRCODE_CONNECTION_FAILURE;

    /* Report error if necessary */
    if (err < 0 && event != GTM_EVENT_ABORT)
        ereport(ERROR, (errcode(seqerrcode), errmsg("GTM error, could not rename sequence")));
}

#endif

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

/*
 * Callback of sequence drop
 */
#ifndef ENABLE_MULTIPLE_NODES
void drop_sequence_cb(GTMEvent event, void* args)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}
#else
void drop_sequence_cb(GTMEvent event, void* args)
{
    drop_sequence_callback_arg* cbargs = (drop_sequence_callback_arg*)args;
    GTM_UUID seq_uuid = cbargs->seq_uuid;
    GTM_SequenceDropType type = cbargs->type;
    int err = 0;
    int seqerrcode = ERRCODE_SQL_STATEMENT_NOT_YET_COMPLETE;

    /*
     * A sequence is dropped on GTM if the transaction that created sequence
     * aborts or if the transaction that dropped the sequence commits. This mechanism
     * insures that sequence information is consistent on all the cluster nodes including
     * GTM. This callback is done before transaction really commits so it can still fail
     * if an error occurs.
     */
    switch (event) {
        case GTM_EVENT_COMMIT:
        case GTM_EVENT_PREPARE:
            if (type == GTM_DROP_SEQ)
                err = DropSequenceGTM(seq_uuid);
            break;
        case GTM_EVENT_ABORT:
            if (type == GTM_CREATE_SEQ)
                err = DropSequenceGTM(seq_uuid);
            break;
        default:
            /* Should not come here */
            Assert(0);
            break;
    }

    ereport(DEBUG2,
        (errmodule(MOD_SEQ),
            (errmsg("Call drop_sequence_cb: in state %d for sequence uuid %ld with type %d", event, seq_uuid, type))));

    if (err == GTM_RESULT_COMM_ERROR)
        seqerrcode = ERRCODE_CONNECTION_FAILURE;

    if (err < 0 && event != GTM_EVENT_ABORT) {
        ereport(WARNING,
            (errcode(seqerrcode),
                errmsg(
                    "Deletion of sequences uuid %ld from gtm may be not completed, please check gtm log", seq_uuid)));
    }
}

#endif

#endif

#ifdef ENABLE_MULTIPLE_NODES
static void sendUpdateSequenceMsgToDn(char* seqname, int64 last_value)
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
        (errmsg("sendUpdateSequenceMsgToDn %s %ld", seqname, last_value)));
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

    msglen = 4; /* for the length itself */
    msglen +=  1; /* for signed */
    msglen += sizeof(int64);
    msglen += strlen(seqname) + 1;

    /* msgType + msgLen */
    ensure_out_buffer_capacity(1 + msglen, handle);

    Assert(handle->outBuffer != NULL);
    handle->outBuffer[handle->outEnd++] = 'y';
    msglen = htonl(msglen);
    rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &msglen, sizeof(int));
    securec_check(rc, "\0", "\0");
    handle->outEnd += 4;
    if (last_value < 0) {
        handle->outBuffer[handle->outEnd++] = '-';
        u_last_value = (uint64)(last_value * -1);
    } else {
        handle->outBuffer[handle->outEnd++] = '+';
        u_last_value = (uint64)(last_value);
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
        seqname, strlen(seqname) + 1);
    securec_check(rc, "\0", "\0");
    handle->outEnd += strlen(seqname) + 1;
    handle->state = DN_CONNECTION_STATE_QUERY;
    pgxc_node_flush(handle);
    CheckUpdateSequenceMsgStatus(exec_handle, seqname, last_value);
    pfree_pgxc_all_handles(conn_handles);
}
#endif // ENABLE_MULTIPLE_NODES

void  processUpdateSequenceMsg(const char* seqname, int64 lastvalue)
{
    StringInfoData retbuf;
    RangeVar* sequence = NULL;
    Oid relid;
    Form_pg_sequence seq;
    HeapTupleData seqtuple;
    text *seqname_text = cstring_to_text(seqname);
    List* nameList = textToQualifiedNameList(seqname_text);
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
            WARNING, (errcode(ERRCODE_OPERATE_FAILED), errmsg("Failed to find relation %s for sequence update", seqname)));
        goto SEND_SUCCESS_MSG;
    }
    /* open and lock sequence */
    init_sequence(relid, &elm, &seqrel);
    /* lock page' buffer and read tuple */
    seq = read_seq_tuple(elm, seqrel, &buf, &seqtuple, &uuid);
    updateNextValForSequence(buf, seq, seqtuple, seqrel, lastvalue);
    relation_close(seqrel, NoLock);
SEND_SUCCESS_MSG:
    pq_beginmessage(&retbuf, 'y');
    pq_endmessage(&retbuf);
    pq_flush();
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
            recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG, false, seqrel->rd_node.bucketNode);
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
            MemoryContext curr;
            seqname = GetGlobalSeqName(seqrel, NULL, NULL);
            curr = MemoryContextSwitchTo(u_sess->top_transaction_mem_cxt);
            u_sess->xact_cxt.send_seqname = pstrdup(seqname);
            u_sess->xact_cxt.send_result = result;
            MemoryContextSwitchTo(curr);
            pfree_ext(seqname);
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
    if(u_sess->xact_cxt.send_seqname != NULL) {
        sendUpdateSequenceMsgToDn(u_sess->xact_cxt.send_seqname, u_sess->xact_cxt.send_result);
        pfree_ext(u_sess->xact_cxt.send_seqname);
    }
#endif 
}
