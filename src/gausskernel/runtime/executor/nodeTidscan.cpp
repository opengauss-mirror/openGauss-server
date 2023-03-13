/* -------------------------------------------------------------------------
 *
 * nodeTidscan.cpp
 *	  Routines to support direct tid scans of relations
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeTidscan.cpp
 *
 * -------------------------------------------------------------------------
 *
 * INTERFACE ROUTINES
 *
 *		ExecTidScan			scans a relation using tids
 *		ExecInitTidScan		creates and initializes state info.
 *		ExecReScanTidScan	rescans the tid relation.
 *		ExecEndTidScan		releases all storage.
 *		ExecTidMarkPos		marks scan position.
 *		ExecTidRestrPos		restores scan position.
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/sysattr.h"
#include "access/tableam.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_type.h"
#include "executor/exec/execdebug.h"
#include "executor/node/nodeTidscan.h"
#include "optimizer/clauses.h"
#include "storage/buf/bufmgr.h"
#include "storage/tcap.h"
#include "utils/array.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "access/ustore/knl_uam.h"

#define IsCTIDVar(node)                                                                                  \
    ((node) != NULL && IsA((node), Var) && ((Var*)(node))->varattno == SelfItemPointerAttributeNumber && \
        ((Var*)(node))->varlevelsup == 0)

static TupleTableSlot* ExecTidScan(PlanState* state);
static void TidListCreateByRecursion(TidScanState* tidstate, bool isBucket);
/* one element in tss_tidexprs */
typedef struct TidExpr
{
	ExprState  *exprstate;		/* ExprState for a TID-yielding subexpr */
	bool		isarray;		/* if true, it yields tid[] not just tid */
	CurrentOfExpr *cexpr;		/* alternatively, we can have CURRENT OF */
} TidExpr;

static void TidExprListInit(TidScanState *tidstate);
static void TidListCreateByFlatten(TidScanState* tidstate, bool isBucket);
static int ItemptrComparator(const void* a, const void* b);
static TupleTableSlot* TidNext(TidScanState* node);
static void ExecInitNextPartitionForTidScan(TidScanState* node);
static void ExecInitPartitionForTidScan(TidScanState* tidstate, EState* estate);

/*
 * Extract the qual subexpressions that yield TIDs to search for,
 * and compile them into ExprStates if they're ordinary expressions.
 *
 * CURRENT OF is a special case that we can't compile usefully;
 * just drop it into the TidExpr list as-is.
 */
static void
TidExprListInit(TidScanState *tidstate)
{
	TidScan    *node = (TidScan *) tidstate->ss.ps.plan;
	ListCell   *l;

	tidstate->tss_tidexprs = NIL;
	tidstate->tss_isCurrentOf = false;

	foreach(l, node->tidquals)
	{
		Expr	   *expr = (Expr *) lfirst(l);
		TidExpr    *tidexpr = (TidExpr *) palloc0(sizeof(TidExpr));

		if (is_opclause(expr))
		{
			Node	   *arg1;
			Node	   *arg2;

			arg1 = get_leftop(expr);
			arg2 = get_rightop(expr);
			if (IsCTIDVar(arg1))
				tidexpr->exprstate = ExecInitExpr((Expr *) arg2,
												  &tidstate->ss.ps);
			else if (IsCTIDVar(arg2))
				tidexpr->exprstate = ExecInitExpr((Expr *) arg1,
												  &tidstate->ss.ps);
			else
				elog(ERROR, "could not identify CTID variable");
			tidexpr->isarray = false;
		}
		else if (expr && IsA(expr, ScalarArrayOpExpr))
		{
			ScalarArrayOpExpr *saex = (ScalarArrayOpExpr *) expr;

			Assert(IsCTIDVar(linitial(saex->args)));
			tidexpr->exprstate = ExecInitExpr((Expr*)lsecond(saex->args),
											  &tidstate->ss.ps);
			tidexpr->isarray = true;
		}
		else if (expr && IsA(expr, CurrentOfExpr))
		{
			CurrentOfExpr *cexpr = (CurrentOfExpr *) expr;

			tidexpr->cexpr = cexpr;
			tidstate->tss_isCurrentOf = true;
		}
		else
			elog(ERROR, "could not identify CTID expression");

		tidstate->tss_tidexprs = lappend(tidstate->tss_tidexprs, tidexpr);
	}

	/* CurrentOfExpr could never appear OR'd with something else */
	Assert(list_length(tidstate->tss_tidexprs) == 1 ||
		   !tidstate->tss_isCurrentOf);
}

/*
 * Compute the list of TIDs to be visited, by evaluating the expressions
 * for them.
 *
 * (The result is actually an array, not a list.)
 */
static void TidListCreateByFlatten(TidScanState* tidstate, bool isBucket)
{
    ExprContext* econtext = tidstate->ss.ps.ps_ExprContext;
    BlockNumber nblocks;
    ItemPointerData* tid_list = NULL;
    int num_alloc_tids;
    int num_tids;
    ListCell* l = NULL;

    /*
     * We silently discard any TIDs that are out of range at the time of scan
     * start.  (Since we hold at least AccessShareLock on the table, it won't
     * be possible for someone to truncate away the blocks we intend to
     * visit.)
     */
    if (isBucket) {
        nblocks = MaxBlockNumber;
    } else {
        if (tidstate->ss.isPartTbl) {
            nblocks = RelationGetNumberOfBlocks(tidstate->ss.ss_currentPartition);
        } else {
            nblocks = RelationGetNumberOfBlocks(tidstate->ss.ss_currentRelation);
        }
    }

    /*
     * We initialize the array with enough slots for the case that all quals
     * are simple OpExprs or CurrentOfExprs.  If there are any
     * ScalarArrayOpExprs, we may have to enlarge the array.
     */
    num_alloc_tids = list_length(tidstate->tss_tidexprs);
    tid_list = (ItemPointerData*)palloc(num_alloc_tids * sizeof(ItemPointerData));
    num_tids = 0;

    foreach(l, tidstate->tss_tidexprs) {
        TidExpr    *tidexpr = (TidExpr *) lfirst(l);
        ItemPointer itemptr;
        bool is_null = false;

        if (tidexpr->exprstate && !tidexpr->isarray) {
            itemptr = (ItemPointer)DatumGetPointer(ExecEvalExprSwitchContext(tidexpr->exprstate, econtext, &is_null));
            if (!is_null && ItemPointerIsValid(itemptr) && ItemPointerGetBlockNumber(itemptr) < nblocks) {
                if (num_tids >= num_alloc_tids) {
                    num_alloc_tids *= 2;
                    tid_list = (ItemPointerData*)repalloc(tid_list, num_alloc_tids * sizeof(ItemPointerData));
                }
                tid_list[num_tids++] = *itemptr;
            }
        } else if (tidexpr->exprstate && tidexpr->isarray) {
            Datum arraydatum;
            ArrayType* itemarray = NULL;
            Datum* ipdatums = NULL;
            bool* ipnulls = NULL;
            int ndatums;
            int i;

            arraydatum = ExecEvalExprSwitchContext(tidexpr->exprstate, econtext, &is_null);
            if (is_null)
                continue;
            itemarray = DatumGetArrayTypeP(arraydatum);
            deconstruct_array(itemarray, TIDOID, SizeOfIptrData, false, 's', &ipdatums, &ipnulls, &ndatums);
            if (num_tids + ndatums > num_alloc_tids) {
                num_alloc_tids = num_tids + ndatums;
                tid_list = (ItemPointerData*)repalloc(tid_list, num_alloc_tids * sizeof(ItemPointerData));
            }
            for (i = 0; i < ndatums; i++) {
                if (!ipnulls[i]) {
                    itemptr = (ItemPointer)DatumGetPointer(ipdatums[i]);
                    if (ItemPointerIsValid(itemptr) && ItemPointerGetBlockNumber(itemptr) < nblocks)
                        tid_list[num_tids++] = *itemptr;
                }
            }
            pfree_ext(ipdatums);
            pfree_ext(ipnulls);
        } else {
            ItemPointerData cursor_tid;

            Assert(tidexpr->cexpr);
            if (execCurrentOf(tidexpr->cexpr, econtext, tidstate->ss.ss_currentRelation, &cursor_tid,
                &(tidstate->tss_CurrentOf_CurrentPartition))) {
                if (num_tids >= num_alloc_tids) {
                    num_alloc_tids *= 2;
                    tid_list = (ItemPointerData*)repalloc(tid_list, num_alloc_tids * sizeof(ItemPointerData));
                }
                tid_list[num_tids++] = cursor_tid;
            }
        }
    }

    /*
     * Sort the array of TIDs into order, and eliminate duplicates.
     * Eliminating duplicates is necessary since we want OR semantics across
     * the list.  Sorting makes it easier to detect duplicates, and as a bonus
     * ensures that we will visit the heap in the most efficient way.
     */
    if (num_tids > 1) {
        int last_tid;
        int i;

        /* CurrentOfExpr could never appear OR'd with something else */
        Assert(!tidstate->tss_isCurrentOf);

        qsort((void*)tid_list, num_tids, sizeof(ItemPointerData), ItemptrComparator);
        last_tid = 0;
        for (i = 1; i < num_tids; i++) {
            if (!ItemPointerEquals(&tid_list[last_tid], &tid_list[i]))
                tid_list[++last_tid] = tid_list[i];
        }
        num_tids = last_tid + 1;
    }

    tidstate->tss_TidList = tid_list;
    tidstate->tss_NumTids = num_tids;
    tidstate->tss_TidPtr = -1;
}

/*
 * Compute the list of TIDs to be visited, by evaluating the expressions
 * for them.
 *
 * (The result is actually an array, not a list.)
 */
static void TidListCreateByRecursion(TidScanState* tidstate, bool isBucket)
{
    List* eval_list = tidstate->tss_tidexprs;
    ExprContext* econtext = tidstate->ss.ps.ps_ExprContext;
    BlockNumber nblocks;
    ItemPointerData* tid_list = NULL;
    int num_alloc_tids;
    int num_tids;
    ListCell* l = NULL;

    /*
     * We silently discard any TIDs that are out of range at the time of scan
     * start.  (Since we hold at least AccessShareLock on the table, it won't
     * be possible for someone to truncate away the blocks we intend to
     * visit.)
     */
    if (isBucket) {
        nblocks = MaxBlockNumber;
    } else {
        if (tidstate->ss.isPartTbl) {
            nblocks = RelationGetNumberOfBlocks(tidstate->ss.ss_currentPartition);
        } else {
            nblocks = RelationGetNumberOfBlocks(tidstate->ss.ss_currentRelation);
        }
    }

    /*
     * We initialize the array with enough slots for the case that all quals
     * are simple OpExprs or CurrentOfExprs.  If there are any
     * ScalarArrayOpExprs, we may have to enlarge the array.
     */
    num_alloc_tids = list_length(eval_list);
    tid_list = (ItemPointerData*)palloc(num_alloc_tids * sizeof(ItemPointerData));
    num_tids = 0;
    tidstate->tss_isCurrentOf = false;

    foreach (l, eval_list) {
        ExprState* exstate = (ExprState*)lfirst(l);
        Expr* expr = exstate->expr;
        ItemPointer itemptr;
        bool is_null = false;

        if (is_opclause(expr)) {
            FuncExprState* fex_state = (FuncExprState*)exstate;
            Node* arg1 = NULL;
            Node* arg2 = NULL;

            arg1 = get_leftop(expr);
            arg2 = get_rightop(expr);
            if (IsCTIDVar(arg1))
                exstate = (ExprState*)lsecond(fex_state->args);
            else if (IsCTIDVar(arg2))
                exstate = (ExprState*)linitial(fex_state->args);
            else
                ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("could not identify CTID variable")));

            itemptr = (ItemPointer)DatumGetPointer(ExecEvalExprSwitchContext(exstate, econtext, &is_null, NULL));
            if (!is_null && ItemPointerIsValid(itemptr) && ItemPointerGetBlockNumber(itemptr) < nblocks) {
                if (num_tids >= num_alloc_tids) {
                    num_alloc_tids *= 2;
                    tid_list = (ItemPointerData*)repalloc(tid_list, num_alloc_tids * sizeof(ItemPointerData));
                }
                tid_list[num_tids++] = *itemptr;
            }
        } else if (expr && IsA(expr, ScalarArrayOpExpr)) {
            ScalarArrayOpExprState* saexstate = (ScalarArrayOpExprState*)exstate;
            Datum arraydatum;
            ArrayType* itemarray = NULL;
            Datum* ipdatums = NULL;
            bool* ipnulls = NULL;
            int ndatums;
            int i;

            exstate = (ExprState*)lsecond(saexstate->fxprstate.args);
            arraydatum = ExecEvalExprSwitchContext(exstate, econtext, &is_null, NULL);
            if (is_null)
                continue;
            itemarray = DatumGetArrayTypeP(arraydatum);
            deconstruct_array(itemarray, TIDOID, SizeOfIptrData, false, 's', &ipdatums, &ipnulls, &ndatums);
            if (num_tids + ndatums > num_alloc_tids) {
                num_alloc_tids = num_tids + ndatums;
                tid_list = (ItemPointerData*)repalloc(tid_list, num_alloc_tids * sizeof(ItemPointerData));
            }
            for (i = 0; i < ndatums; i++) {
                if (!ipnulls[i]) {
                    itemptr = (ItemPointer)DatumGetPointer(ipdatums[i]);
                    if (ItemPointerIsValid(itemptr) && ItemPointerGetBlockNumber(itemptr) < nblocks)
                        tid_list[num_tids++] = *itemptr;
                }
            }
            pfree_ext(ipdatums);
            pfree_ext(ipnulls);
        } else if (expr && IsA(expr, CurrentOfExpr)) {
            CurrentOfExpr* cexpr = (CurrentOfExpr*)expr;
            ItemPointerData cursor_tid;

            if (execCurrentOf(cexpr, econtext, tidstate->ss.ss_currentRelation, &cursor_tid,
                              &(tidstate->tss_CurrentOf_CurrentPartition))) {
                if (num_tids >= num_alloc_tids) {
                    num_alloc_tids *= 2;
                    tid_list = (ItemPointerData*)repalloc(tid_list, num_alloc_tids * sizeof(ItemPointerData));
                }
                tid_list[num_tids++] = cursor_tid;
                tidstate->tss_isCurrentOf = true;
            }
        } else
            ereport(ERROR,
                    (errcode(ERRCODE_DATA_EXCEPTION),
                     errmsg("could not identify CTID expression, %s", (expr == NULL) ? "expr is NULL" : "")));
    }

    /*
     * Sort the array of TIDs into order, and eliminate duplicates.
     * Eliminating duplicates is necessary since we want OR semantics across
     * the list.  Sorting makes it easier to detect duplicates, and as a bonus
     * ensures that we will visit the heap in the most efficient way.
     */
    if (num_tids > 1) {
        int last_tid;
        int i;

        /* CurrentOfExpr could never appear OR'd with something else */
        Assert(!tidstate->tss_isCurrentOf);

        qsort((void*)tid_list, num_tids, sizeof(ItemPointerData), ItemptrComparator);
        last_tid = 0;
        for (i = 1; i < num_tids; i++) {
            if (!ItemPointerEquals(&tid_list[last_tid], &tid_list[i]))
                tid_list[++last_tid] = tid_list[i];
        }
        num_tids = last_tid + 1;
    }

    tidstate->tss_TidList = tid_list;
    tidstate->tss_NumTids = num_tids;
    tidstate->tss_TidPtr = -1;
}

/*
 * qsort comparator for ItemPointerData items
 */
static int ItemptrComparator(const void* a, const void* b)
{
    const ItemPointerData* ipa = (const ItemPointerData*)a;
    const ItemPointerData* ipb = (const ItemPointerData*)b;
    BlockNumber ba = ItemPointerGetBlockNumber(ipa);
    BlockNumber bb = ItemPointerGetBlockNumber(ipb);
    OffsetNumber oa = ItemPointerGetOffsetNumber(ipa);
    OffsetNumber ob = ItemPointerGetOffsetNumber(ipb);

    if (ba < bb)
        return -1;
    if (ba > bb)
        return 1;
    if (oa < ob)
        return -1;
    if (oa > ob)
        return 1;
    return 0;
}

void InitTidPtr(TidScanState* node, bool bBackward)
{
    Assert(node != NULL);

    if (bBackward) {
        if (node->tss_TidPtr < 0) {
            /* initialize for backward scan */
            node->tss_TidPtr = node->tss_NumTids - 1;
        } else
            node->tss_TidPtr--;
    } else {
        if (node->tss_TidPtr < 0) {
            /* initialize for forward scan */
            node->tss_TidPtr = 0;
        } else
            node->tss_TidPtr++;
    }
}

static void MoveToNextTid(TidScanState* node, bool is_backward)
{
    if (is_backward) {
        node->tss_TidPtr--;
    } else {
        node->tss_TidPtr++;
    }
}

static TupleTableSlot* HbktTidFetchTuple(TidScanState* node, bool bBackward)
{
    Assert(node != NULL);

    Buffer buffer = InvalidBuffer;
    int num_tids = node->tss_NumTids;
    HeapTuple tuple = &(node->tss_htup);
    ItemPointerData* tid_list = node->tss_TidList;
    TupleTableSlot* slot = node->ss.ss_ScanTupleSlot;
    Snapshot snapshot = node->ss.ps.state->es_snapshot;
    HBktTblScanDesc hp_scan = (HBktTblScanDesc)node->ss.ss_currentScanDesc;

    Assert(hp_scan != NULL);

    while (hp_scan->currBktRel != NULL) {
        InitTidPtr(node, bBackward);

        while (node->tss_TidPtr >= 0 && node->tss_TidPtr < num_tids) {
            tuple->t_self = tid_list[node->tss_TidPtr];
            Relation bkt_rel = hp_scan->currBktRel;
            tuple->t_tableOid = RelationGetRelid(bkt_rel);
            tuple->t_bucketId = RelationGetBktid(bkt_rel);
            /* if fetch failed in heap_fetch, the tuple->t_data is cleaned, so
             * when we change to another hash bucket, it should be assigned again
             */
            tuple->t_data = &(node->tss_ctbuf_hdr);
            Assert(tuple->t_data != NULL);

            if (node->tss_isCurrentOf) {
                tableam_tuple_get_latest_tid(bkt_rel, snapshot, &tuple->t_self);
            }

            /* before fetch tuple, ensure that the bucket relation to be fetched has tuples */
            if (RelationGetNumberOfBlocks(bkt_rel) > ItemPointerGetBlockNumber(&(tuple->t_self)) && tableam_tuple_fetch(bkt_rel, snapshot, tuple, &buffer, false, NULL)) {
                (void)ExecStoreTuple(tuple, slot, buffer, false);
                ReleaseBuffer(buffer);
                return slot;
            }
            MoveToNextTid(node, bBackward);
        }
        /* switch to the next bucket relation and reset the tidptr, because we should
         * find the tids from the first one in the new hash bucket relation */
        if (!hbkt_tbl_tid_nextbucket(hp_scan)) {
            break;
        }

        /* Reset the tidPtr */
        node->tss_TidPtr = -1;

        CHECK_FOR_INTERRUPTS();
    }

    return ExecClearTuple(slot);
}

bool HeapFetchRowVersion(TidScanState* node, Relation relation,
                          ItemPointer tid,
                          Snapshot snapshot,
                          TupleTableSlot *slot)
{
    Buffer buffer = InvalidBuffer;
    HeapTuple tuple = &(node->tss_htup);
    Snapshot scanSnap;

    /*
     * Choose user-specified snapshot if TimeCapsule clause exists, otherwise 
     * estate->es_snapshot instead.
     */
    scanSnap = TvChooseScanSnap(relation, (Scan *)node->ss.ps.plan, &node->ss);
    /* Must set a private data buffer for TidScan */
    tuple->t_data = &(node->tss_ctbuf_hdr);
    Assert(tid != NULL);
    tuple->t_self = *tid;
    if (heap_fetch(relation, scanSnap, tuple, &buffer, false, NULL, &node->ss.ps.state->have_current_xact_date)) {
        /*
         * store the scanned tuple in the scan tuple slot of the scan
         * state.  Eventually we will only do this and not return a tuple.
         * Note: we pass 'false' because tuples returned by amgetnext are
         * pointers onto disk pages and were not created with palloc() and
         * so should not be pfree_ext()'d.
         */
        (void)ExecStoreTuple(tuple, /* tuple to store */
            slot,             /* slot to store in */
            buffer,           /* buffer associated with tuple  */
            false);           /* don't pfree */

        /*
         * At this point we have an extra pin on the buffer, because
         * ExecStoreTuple incremented the pin count. Drop our local pin.
         */
        ReleaseBuffer(buffer);
        return true;
    }
    return false;
}


static TupleTableSlot* TidFetchTuple(TidScanState* node, bool b_backward, Relation heap_relation)
{
    int num_tids = node->tss_NumTids;
    ItemPointerData* tid_list = node->tss_TidList;
    Snapshot scanSnap;
    TupleTableSlot* slot = node->ss.ss_ScanTupleSlot;

    /*
     * Choose user-specified snapshot if TimeCapsule clause exists, otherwise 
     * estate->es_snapshot instead.
     */
    scanSnap = TvChooseScanSnap(heap_relation, (Scan *)node->ss.ps.plan, &node->ss);

    InitTidPtr(node, b_backward);
    while (node->tss_TidPtr >= 0 && node->tss_TidPtr < num_tids) {
        ItemPointerData tid = tid_list[node->tss_TidPtr];

        /* WHERE CURRENT OF is disabled at transformCurrentOfExpr, so no need to support it here. */
        Assert (node->tss_isCurrentOf == false);
        if (node->tss_isCurrentOf) {
            ereport(ERROR, (errcode(ERRCODE_STATEMENT_TOO_COMPLEX),
                (errmsg("WHERE CURRENT OF clause not yet supported"))));
        }

        if (tableam_tops_tuple_fetch_row_version(node, heap_relation, &tid, scanSnap, slot)) {
            return slot;
        }

        /* Bad TID or failed snapshot qual; try next */
        MoveToNextTid(node, b_backward);
    }

    /*
     * if we get here it means the tid scan failed so we are at the end of the
     * scan..
     */
    return ExecClearTuple(slot);
}

/* ----------------------------------------------------------------
 *		TidNext
 *
 *		Retrieve a tuple from the TidScan node's currentRelation
 *		using the tids in the TidScanState information.
 *
 * ----------------------------------------------------------------
 */
static TupleTableSlot* TidNext(TidScanState* node)
{
    bool b_backward = false;
    Relation heap_relation = node->ss.ss_currentRelation;

    /*
     * extract necessary information from tid scan node
     */
    EState* estate = node->ss.ps.state;
    ScanDirection direction = estate->es_direction;

    if (node->ss.isPartTbl) {
        if (!PointerIsValid(node->ss.partitions)) {
            return NULL;
        }
        heap_relation = node->ss.ss_currentPartition;
    }

    /*
     * First time through, compute the list of TIDs to be visited
     */
    if (node->tss_TidList == NULL) {
        if (estate->es_is_flt_frame) {
            TidListCreateByFlatten(node, RELATION_CREATE_BUCKET(heap_relation));
        } else {
            TidListCreateByRecursion(node, RELATION_CREATE_BUCKET(heap_relation));
        }
    }

    if (node->tss_isCurrentOf && node->ss.isPartTbl) {
        if (PointerIsValid(node->tss_CurrentOf_CurrentPartition)) {
            if (heap_relation->rd_id != node->tss_CurrentOf_CurrentPartition->rd_id) {
                heap_relation = node->tss_CurrentOf_CurrentPartition;
            }
        }
    }

    /*
     * Initialize or advance scan position, depending on direction.
     */
    b_backward = ScanDirectionIsBackward(direction);

    if (RELATION_CREATE_BUCKET(heap_relation)) {
        return HbktTidFetchTuple(node, b_backward);
    } else {
        return TidFetchTuple(node, b_backward, heap_relation);
    }
}

/*
 * TidRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool TidRecheck(TidScanState* node, TupleTableSlot* slot)
{
    /*
     * We should check here to make sure tuple matches TID list.
     */
    ExprContext* econtext = node->ss.ps.ps_ExprContext;
    econtext->ecxt_scantuple = slot;
    ResetExprContext(econtext);
    if (node->tss_tidexprs != NIL) {
        return ExecQual(node->tss_tidexprs, econtext);
    } else
        return true;
}

/* ----------------------------------------------------------------
 *		ExecTidScan(node)
 *
 *		Scans the relation using tids and returns
 *		   the next qualifying tuple in the direction specified.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 *
 *		Conditions:
 *		  -- the "cursor" maintained by the AMI is positioned at the tuple
 *			 returned previously.
 *
 *		Initial States:
 *		  -- the relation indicated is opened for scanning so that the
 *			 "cursor" is positioned before the first qualifying tuple.
 *		  -- tidPtr is -1.
 * ----------------------------------------------------------------
 */
static TupleTableSlot* ExecTidScan(PlanState* state)
{
    TidScanState* node = castNode(TidScanState, state);
    return ExecScan(&node->ss, (ExecScanAccessMtd)TidNext, (ExecScanRecheckMtd)TidRecheck);
}

/* ----------------------------------------------------------------
 *		ExecReScanTidScan(node)
 * ----------------------------------------------------------------
 */
void ExecReScanTidScan(TidScanState* node)
{
    if (node->tss_TidList)
        pfree_ext(node->tss_TidList);
    node->tss_TidList = NULL;
    node->tss_NumTids = 0;
    node->tss_TidPtr = -1;

    if (node->ss.isPartTbl && !(((Scan *)node->ss.ps.plan)->partition_iterator_elimination)) {
        scan_handler_tbl_end_tidscan((TableScanDesc)(node->ss.ss_currentScanDesc));

        if (PointerIsValid(node->ss.partitions)) {
            /* switch to next partition for scan */
            ExecInitNextPartitionForTidScan(node);
        }
    }

    ExecScanReScan(&node->ss);
}

/* ----------------------------------------------------------------
 *		ExecEndTidScan
 *
 *		Releases any storage allocated through C routines.
 *		Returns nothing.
 * ----------------------------------------------------------------
 */
void ExecEndTidScan(TidScanState* node)
{
    /*
     * Free the exprcontext
     */
    ExecFreeExprContext(&node->ss.ps);

    /*
     * clear out tuple table slots
     */
    (void)ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);

    scan_handler_tbl_end_tidscan((TableScanDesc)(node->ss.ss_currentScanDesc));
    if (node->ss.isPartTbl) {
        if (PointerIsValid(node->ss.partitions)) {
            /* switch node->ss.ss_currentRelation to partitioned relation */
            Assert(node->ss.ss_currentPartition);
            releaseDummyRelation(&(node->ss.ss_currentPartition));
            releasePartitionList(node->ss.ss_currentRelation, &(node->ss.partitions), NoLock);
            releaseSubPartitionList(node->ss.ss_currentRelation, &(node->ss.subpartitions), NoLock);
        }
    }

    /*
     * close the heap relation.
     */
    ExecCloseScanRelation(node->ss.ss_currentRelation);
}

/* ----------------------------------------------------------------
 *		ExecTidMarkPos
 *
 *		Marks scan position by marking the current tid.
 *		Returns nothing.
 * ----------------------------------------------------------------
 */
void ExecTidMarkPos(TidScanState* node)
{
    node->tss_MarkTidPtr = node->tss_TidPtr;
}

/* ----------------------------------------------------------------
 *		ExecTidRestrPos
 *
 *		Restores scan position by restoring the current tid.
 *		Returns nothing.
 *
 *		XXX Assumes previously marked scan position belongs to current tid
 * ----------------------------------------------------------------
 */
void ExecTidRestrPos(TidScanState* node)
{
    node->tss_TidPtr = node->tss_MarkTidPtr;
}

/* ----------------------------------------------------------------
 *		ExecInitTidScan
 *
 *		Initializes the tid scan's state information, creates
 *		scan keys, and opens the base and tid relations.
 *
 *		Parameters:
 *		  node: TidNode node produced by the planner.
 *		  estate: the execution state initialized in InitPlan.
 * ----------------------------------------------------------------
 */
TidScanState* ExecInitTidScan(TidScan* node, EState* estate, int eflags)
{
    TidScanState* tidstate = NULL;
    Relation current_relation;

    /*
     * create state structure ,MaxHeapTupleSize to get the memory for t_bits
     */
    tidstate = makeNodeWithSize(TidScanState, SizeofTidScanState + MaxHeapTupleSize);
    tidstate->ss.ps.plan = (Plan*)node;
    tidstate->ss.ps.state = estate;
    tidstate->ss.isPartTbl = node->scan.isPartTbl;
    tidstate->ss.currentSlot = 0;
    tidstate->ss.partScanDirection = node->scan.partScanDirection;
    tidstate->ss.ps.ExecProcNode = ExecTidScan;

    tidstate->tss_htup.tupTableType = HEAP_TUPLE;
    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &tidstate->ss.ps);

    /*
     * initialize child expressions
     */
    if (estate->es_is_flt_frame) {
        tidstate->ss.ps.qual = (List*)ExecInitQualByFlatten(node->scan.plan.qual, (PlanState*)tidstate);
        TidExprListInit(tidstate);
    } else {
        tidstate->ss.ps.targetlist = (List*)ExecInitExprByRecursion((Expr*)node->scan.plan.targetlist, (PlanState*)tidstate);
        tidstate->ss.ps.qual = (List*)ExecInitExprByRecursion((Expr*)node->scan.plan.qual, (PlanState*)tidstate);
        tidstate->tss_tidexprs = (List*)ExecInitExprByRecursion((Expr*)node->tidquals, (PlanState*)tidstate);
    }

    /*
     * mark tid list as not computed yet
     */
    tidstate->tss_TidList = NULL;
    tidstate->tss_NumTids = 0;
    tidstate->tss_TidPtr = -1;

    /*
     * open the base relation and acquire appropriate lock on it.
     */
    current_relation = ExecOpenScanRelation(estate, node->scan.scanrelid);

    tidstate->ss.ss_currentRelation = current_relation;
    tidstate->ss.ss_currentScanDesc = NULL; /* no heap scan here */

    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &tidstate->ss.ps, current_relation->rd_tam_ops);
    ExecInitScanTupleSlot(estate, &tidstate->ss, current_relation->rd_tam_ops);

    /* deal with partitioned table branch */
    if (node->scan.isPartTbl) {
        tidstate->ss.ss_currentPartition = NULL;

        if (node->scan.itrs > 0) {
            Partition partition = NULL;
            Relation partitiontrel = NULL;

            /* get table partition list for partitioned table */
            ExecInitPartitionForTidScan(tidstate, estate);

            /* make dummy table realtion with the first partition for scan */
            if (tidstate->ss.partitions != NIL) {
                partition = (Partition)list_nth(tidstate->ss.partitions, 0);
                partitiontrel = partitionGetRelation(current_relation, partition);

                if (RelationIsSubPartitioned(current_relation)) {
                    List *currentSubpartList = (List *)list_nth(tidstate->ss.subpartitions, 0);
                    Partition currentSubPart = (Partition)list_nth(currentSubpartList, 0);
                    Relation currentSubPartitionRel = partitionGetRelation(partitiontrel, currentSubPart);

                    releaseDummyRelation(&partitiontrel);
                    tidstate->ss.ss_currentPartition = currentSubPartitionRel;
                    tidstate->ss.ss_currentScanDesc =
                        scan_handler_tbl_begin_tidscan(currentSubPartitionRel, (ScanState *)tidstate);
                } else {
                    tidstate->ss.ss_currentPartition = partitiontrel;
                    tidstate->ss.ss_currentScanDesc =
                        scan_handler_tbl_begin_tidscan(partitiontrel, (ScanState *)tidstate);
                }
            }
        }
    } else {
        tidstate->ss.ss_currentScanDesc = scan_handler_tbl_begin_tidscan(current_relation, (ScanState*)tidstate);
    }

    /*
     * get the scan type from the relation descriptor.
     */
    ExecAssignScanType(&tidstate->ss, RelationGetDescr(current_relation));

    /*
     * Initialize result tuple type and projection info.
     */
    ExecAssignResultTypeFromTL(
            &tidstate->ss.ps,
            tidstate->ss.ss_ScanTupleSlot->tts_tupleDescriptor->td_tam_ops);

    ExecAssignScanProjectionInfo(&tidstate->ss);

    Assert(tidstate->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor->td_tam_ops);

    /*
     * all done.
     */
    return tidstate;
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: construt a dummy relation with the next partition and the partitiobed
 *			: table for the following TidScan, and swith the scaning relation to the
 *			: dummy relation
 * Description	:
 * Input		:
 * Output	:
 * Notes		:
 */
static void ExecInitNextPartitionForTidScan(TidScanState* node)
{
    Partition current_partition = NULL;
    Relation current_partitionrel = NULL;
    TidScan* plan = NULL;
    int paramno = -1;
    ParamExecData* param = NULL;
    int subPartParamno = -1;
    ParamExecData* SubPrtParam = NULL;

    plan = (TidScan*)(node->ss.ps.plan);

    /* get partition sequnce */
    paramno = plan->scan.plan.paramno;
    param = &(node->ss.ps.state->es_param_exec_vals[paramno]);
    node->ss.currentSlot = (int)param->value;

    subPartParamno = plan->scan.plan.subparamno;
    SubPrtParam = &(node->ss.ps.state->es_param_exec_vals[subPartParamno]);

    /* construct a dummy table relation with the next table partition */
    current_partition = (Partition)list_nth(node->ss.partitions, node->ss.currentSlot);
    current_partitionrel = partitionGetRelation(node->ss.ss_currentRelation, current_partition);

    /*
     * update scan-related table partition
     * ss_currentRelation is the dummy relation we make it
     */
    Assert(PointerIsValid(node->ss.ss_currentPartition));
    releaseDummyRelation(&(node->ss.ss_currentPartition));
    if (current_partitionrel->partMap != NULL) {
        Partition currentSubPartition = NULL;
        List* currentSubPartitionList = NULL;
        Relation currentSubPartitionRel = NULL;
        Assert(SubPrtParam != NULL);
        currentSubPartitionList = (List *)list_nth(node->ss.subpartitions, node->ss.currentSlot);
        currentSubPartition = (Partition)list_nth(currentSubPartitionList, (int)SubPrtParam->value);
        currentSubPartitionRel = partitionGetRelation(current_partitionrel, currentSubPartition);

        releaseDummyRelation(&(current_partitionrel));
        node->ss.ss_currentPartition = currentSubPartitionRel;
        node->ss.ss_currentScanDesc = scan_handler_tbl_begin_tidscan(currentSubPartitionRel, (ScanState*)node);
    } else {
        node->ss.ss_currentPartition = current_partitionrel;
        node->ss.ss_currentScanDesc = scan_handler_tbl_begin_tidscan(current_partitionrel, (ScanState*)node);
    }
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		:  This does the initialization for scan partition
 * Description	:
 * Input		:
 * Output	:
 * Notes		:
 */
static void ExecInitPartitionForTidScan(TidScanState* tidstate, EState* estate)
{
    TidScan* plan = NULL;
    Relation current_relation = NULL;

    plan = (TidScan*)tidstate->ss.ps.plan;
    current_relation = tidstate->ss.ss_currentRelation;

    tidstate->ss.ss_currentPartition = NULL;
    tidstate->ss.partitions = NIL;

    if (plan->scan.itrs > 0) {
        LOCKMODE lock = NoLock;
        Partition table_partition = NULL;
        bool relistarget = false;

        relistarget = ExecRelationIsTargetRelation(estate, plan->scan.scanrelid);
        lock = (relistarget ? RowExclusiveLock : AccessShareLock);
        tidstate->ss.lockMode = lock;

        PruningResult* pruningResult = NULL;
        if (plan->scan.pruningInfo->expr != NULL) {
            pruningResult = GetPartitionInfo(plan->scan.pruningInfo, estate, current_relation);
        } else {
            pruningResult = plan->scan.pruningInfo;
        }
        if (pruningResult->ls_rangeSelectedPartitions != NULL) {
            tidstate->ss.part_id = pruningResult->ls_rangeSelectedPartitions->length;
        } else {
            tidstate->ss.part_id = 0;
        }

        ListCell* cell1 = NULL;
        ListCell* cell2 = NULL;
        List* part_seqs = pruningResult->ls_rangeSelectedPartitions;
        List* partitionnos = pruningResult->ls_selectedPartitionnos;
        Assert(list_length(part_seqs) == list_length(partitionnos));

        forboth (cell1, part_seqs, cell2, partitionnos) {
            Oid table_partitionid = InvalidOid;
            int part_seq = lfirst_int(cell1);
            int partitionno = lfirst_int(cell2);
            /* add table partition to list */
            table_partitionid =
                getPartitionOidFromSequence(current_relation, part_seq, partitionno);
            table_partition = PartitionOpenWithPartitionno(current_relation, table_partitionid, partitionno, lock);
            tidstate->ss.partitions = lappend(tidstate->ss.partitions, table_partition);
            if (pruningResult->ls_selectedSubPartitions != NIL) {
                Relation partRelation = partitionGetRelation(current_relation, table_partition);
                SubPartitionPruningResult* subPartPruningResult =
                    GetSubPartitionPruningResult(pruningResult->ls_selectedSubPartitions, part_seq, partitionno);
                if (subPartPruningResult == NULL) {
                    continue;
                }
                List *subpartSeqs = subPartPruningResult->ls_selectedSubPartitions;
                List *subpartitionnos = subPartPruningResult->ls_selectedSubPartitionnos;
                Assert(list_length(subpartSeqs) == list_length(subpartitionnos));
                List *subpartition = NIL;
                ListCell *lc1 = NULL;
                ListCell *lc2 = NULL;
                forboth (lc1, subpartSeqs, lc2, subpartitionnos) {
                    Oid subpartitionid = InvalidOid;
                    int subpartSeq = lfirst_int(lc1);
                    int subpartitionno = lfirst_int(lc2);

                    subpartitionid = getPartitionOidFromSequence(partRelation, subpartSeq, subpartitionno);
                    Partition subpart =
                        PartitionOpenWithPartitionno(partRelation, subpartitionid, subpartitionno, lock);
                    subpartition = lappend(subpartition, subpart);
                }
                releaseDummyRelation(&(partRelation));
                tidstate->ss.subPartLengthList =
                    lappend_int(tidstate->ss.subPartLengthList, list_length(subpartition));
                tidstate->ss.subpartitions = lappend(tidstate->ss.subpartitions, subpartition);
            }
        }
    }
}
