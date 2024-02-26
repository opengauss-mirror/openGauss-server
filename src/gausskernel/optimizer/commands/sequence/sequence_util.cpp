/* -------------------------------------------------------------------------
 *
 * sequence.cpp
 *	  openGauss sequences utility code.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/sequence_util.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/gtm.h"
#include "access/multixact.h"
#include "access/xlogproc.h"
#include "catalog/pg_proc.h"
#include "commands/dbcommands.h"
#include "commands/sequence.h"
#include "gtm/gtm_client.h"
#include "parser/parse_coerce.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

/*
 * Initialize a sequence's relation with the specified tuple as content
 */
void fill_seq_with_data(Relation rel, HeapTuple tuple)
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

        recptr = XLogInsert(RM_SEQ_ID, XLOG_SEQ_LOG, rel->rd_node.bucketNode);

        PageSetLSN(page, recptr);
    }

    END_CRIT_SECTION();

    UnlockReleaseBuffer(buf);
}

/*
 * If call setval, in any case ,we shoule forget all catched values.
 */
void ResetvalGlobal(Oid relid)
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
 * Open the sequence and acquire RowExclusiveLock if needed
 *
 * If we haven't touched the sequence already in this transaction,
 * we need to acquire a lock.	We arrange for the lock to
 * be owned by the top transaction, so that we don't need to do it
 * more than once per xact.
 */
Relation lock_and_open_seq(SeqTable seq)
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
void init_sequence(Oid relid, SeqTable* p_elm, Relation* p_rel)
{
    SeqTable elm = NULL;
    Relation seqrel;

    elm = GetSessSeqElm(relid);

    /*
     * Open the sequence relation.
     */
    seqrel = lock_and_open_seq(elm);
    if (!RELKIND_IS_SEQUENCE(seqrel->rd_rel->relkind)) {
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
        errno_t rc = memcpy_s(&(elm->cached), sizeof(int128), &(elm->last), sizeof(int128));
        securec_check(rc, "\0", "\0");
    }

    /* Return results */
    *p_elm = elm;
    *p_rel = seqrel;
}

SeqTable GetSessSeqElm(Oid relid)
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
        errno_t rc = EOK;
        rc = memset_s(elm, sizeof(SeqTableData), 0, sizeof(SeqTableData));
        securec_check(rc, "\0", "\0");
        elm->relid = relid;

        elm->next = u_sess->cmd_cxt.seqtab;
        u_sess->cmd_cxt.seqtab = elm;
    }

    return elm;
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
    const int extra = 3; /* length of two dots and one EOL */
    charlen = strlen(dbname) + strlen(schemaname) + strlen(relname) + extra;
    seqname = (char*)palloc(charlen);

    /* Form a unique sequence name with schema and database name for GTM */
    int ret = snprintf_s(seqname, charlen, charlen - 1, "%s.%s.%s", dbname, schemaname, relname);
    securec_check_ss(ret, "\0", "\0");
    pfree_ext(dbname);
    pfree_ext(schemaname);

    return seqname;
}

char* GetGlobalSeqNameForUpdate(Relation seqrel, char** dbname, char** schemaname)
{
    char* relname = NULL;

    *dbname = get_database_name(seqrel->rd_node.dbNode);
    if (*dbname == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache lookup failed for database %u", seqrel->rd_node.dbNode)));
    }

    *schemaname = get_namespace_name(RelationGetNamespace(seqrel));

    if (*schemaname == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                    errmsg("cache lookup failed for schema %u", RelationGetNamespace(seqrel))));
    }

    relname = RelationGetRelationName(seqrel);

    return relname;
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

    if (SSCheckInitPageXLogSimple(record, 0, &buffer) == BLK_DONE) {
        return;
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

uint32 RelidGetHash(Oid seq_relid)
{
    return ((uint32)seq_relid % GS_NUM_OF_BUCKETS);
}

/*
 * Get sequence elem from bucket
 */
SeqTable GetGlobalSeqElm(Oid relid, GlobalSeqInfoHashBucket* bucket)
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

/*
 * Add coercion for (numeric)func() to get (int8)func().
 * There's no need to concern numeric overflow since large sequence is not supported before upgrade.
 */
static Node* update_seq_expr(FuncExpr* func)
{
    func->funcresulttype = NUMERICOID;
    Node* newnode = coerce_to_target_type(
        NULL, (Node*)func, NUMERICOID, INT8OID, -1, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, -1);
    return newnode;
}

/*
 * Add coercion for (int8)func() to get (numeric)func().
 */
static Node* rollback_seq_expr(FuncExpr* func)
{
    func->funcresulttype = INT8OID;
    Node* newnode = coerce_to_target_type(
        NULL, (Node*)func, INT8OID, NUMERICOID, -1, COERCION_ASSIGNMENT, COERCE_IMPLICIT_CAST, -1);
    return newnode;
}

typedef struct MutateSeqExprCxt {
    Node* (*worker)(FuncExpr* func);
    Oid expextedResType;
} MutateSeqExprCxt;

static bool RevertUpgradedFunc(FuncExpr* func, Node** ret)
{
    Assert(list_length(func->args) == 1);
    Node* arg = (Node*)linitial(func->args);
    if (!IsA(arg, FuncExpr)) {
        return false;
    }
    FuncExpr* innerFunc = (FuncExpr*)arg;
    if (innerFunc->funcid == NEXTVALFUNCOID || innerFunc->funcid == CURRVALFUNCOID ||
        innerFunc->funcid == LASTVALFUNCOID) {
        innerFunc->funcresulttype = INT8OID;
        *ret = (Node*)innerFunc;
        return true;
    }
    return false;
}

static Node* large_sequence_modify_node_tree_mutator(Node* node, void* cxt)
{
    /* Traverse through the expression tree and convert nextval() calls with proper coercion */
    if (node == NULL) {
        return NULL;
    }
    if (IsA(node, Query)) {
        return (Node*)query_tree_mutator(
            (Query*) node, (Node* (*)(Node*, void*))large_sequence_modify_node_tree_mutator, cxt, 0);
    }
    if (IsA(node, FuncExpr)) {
        FuncExpr* func = (FuncExpr*) node;
        MutateSeqExprCxt* context = (MutateSeqExprCxt*)cxt;
        if (func->funcid == NEXTVALFUNCOID || func->funcid == CURRVALFUNCOID || func->funcid == LASTVALFUNCOID) {
            if (func->funcresulttype == context->expextedResType) {
                /* Check to allow reentrancy of rollback/upgrade procedure */
                return (Node*)func;
            }
            return ((MutateSeqExprCxt*)cxt)->worker(func);
        } else if (context->expextedResType == INT8OID && func->funcid == 1779) { /* Only for rollback func()::int8 */
            Node* ret = NULL;
            if (RevertUpgradedFunc(func, &ret)) {
                return ret;
            }
        }
    }
    return expression_tree_mutator(
        node, (Node* (*)(Node*, void*))large_sequence_modify_node_tree_mutator, cxt);
}

Datum large_sequence_upgrade_node_tree(PG_FUNCTION_ARGS)
{
    char* res = NULL;
    char* orig = text_to_cstring(PG_GETARG_TEXT_P(0));
    Node* expr = (Node*)stringToNode_skip_extern_fields(orig);
    MutateSeqExprCxt cxt = {update_seq_expr, NUMERICOID};
    expr = query_or_expression_tree_mutator(
        expr, (Node* (*)(Node*, void*))large_sequence_modify_node_tree_mutator, &cxt, 0);
    res = nodeToString(expr);

    PG_RETURN_TEXT_P(cstring_to_text(res));
}

Datum large_sequence_rollback_node_tree(PG_FUNCTION_ARGS)
{
    char* res = NULL;
    char* orig = text_to_cstring(PG_GETARG_TEXT_P(0));
    Node* expr = (Node*)stringToNode_skip_extern_fields(orig);
    MutateSeqExprCxt cxt = {rollback_seq_expr, INT8OID};
    expr = query_or_expression_tree_mutator(
        expr, (Node* (*)(Node*, void*))large_sequence_modify_node_tree_mutator, &cxt, 0);
    res = nodeToString(expr);

    PG_RETURN_TEXT_P(cstring_to_text(res));
}
