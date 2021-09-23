/* -------------------------------------------------------------------------
 *
 * nodeSetOp.cpp
 *	  Routines to handle INTERSECT and EXCEPT selection
 *
 * The input of a SetOp node consists of tuples from two relations,
 * which have been combined into one dataset, with a junk attribute added
 * that shows which relation each tuple came from.	In SETOP_SORTED mode,
 * the input has furthermore been sorted according to all the grouping
 * columns (ie, all the non-junk attributes).  The SetOp node scans each
 * group of identical tuples to determine how many came from each input
 * relation.  Then it is a simple matter to emit the output demanded by the
 * SQL spec for INTERSECT, INTERSECT ALL, EXCEPT, or EXCEPT ALL.
 *
 * In SETOP_HASHED mode, the input is delivered in no particular order,
 * except that we know all the tuples from one input relation will come before
 * all the tuples of the other.  The planner guarantees that the first input
 * relation is the left-hand one for EXCEPT, and tries to make the smaller
 * input relation come first for INTERSECT.  We build a hash table in memory
 * with one entry for each group of identical tuples, and count the number of
 * tuples in the group from each relation.	After seeing all the input, we
 * scan the hashtable and generate the correct output using those counts.
 * We can avoid making hashtable entries for any tuples appearing only in the
 * second input relation, since they cannot result in any output.
 *
 * This node type is not used for UNION or UNION ALL, since those can be
 * implemented more cheaply (there's no need for the junk attribute to
 * identify the source relation).
 *
 * Note that SetOp does no qual checking nor projection.  The delivered
 * output tuples are just copies of the first-to-arrive tuple in each
 * input group.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeSetOp.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/tableam.h"
#include "executor/executor.h"
#include "executor/node/nodeSetOp.h"
#include "executor/node/nodeAgg.h"
#include "optimizer/pgxcplan.h"
#include "pgxc/pgxc.h"
#include "utils/memutils.h"
#include "utils/memprot.h"

/*
 * SetOpStatePerGroupData - per-group working state
 *
 * These values are working state that is initialized at the start of
 * an input tuple group and updated for each input tuple.
 *
 * In SETOP_SORTED mode, we need only one of these structs, and it's kept in
 * the plan state node.  In SETOP_HASHED mode, the hash table contains one
 * of these for each tuple group.
 */
typedef struct SetOpStatePerGroupData {
    long numLeft;  /* number of left-input dups in group */
    long numRight; /* number of right-input dups in group */
} SetOpStatePerGroupData;

/*
 * To implement hashed mode, we need a hashtable that stores a
 * representative tuple and the duplicate counts for each distinct set
 * of grouping columns.  We compute the hash key from the grouping columns.
 */
typedef struct SetOpHashEntryData* SetOpHashEntry;

typedef struct SetOpHashEntryData {
    TupleHashEntryData shared; /* common header for hash table entries */
    SetOpStatePerGroupData pergroup;
} SetOpHashEntryData;

static TupleTableSlot* setop_retrieve_direct(SetOpState* setopstate);
static void setop_fill_hash_table(SetOpState* setopstate);
static TupleTableSlot* setop_retrieve_hash_table(SetOpState* setopstate);
static TupleTableSlot* setop_retrieve(SetOpState* setopstate);
void ClearSetOp(SetOpState* node, SetopWriteFileControl* TempFileControl);
void ReSetFile(SetOpState* node, SetopWriteFileControl* TempFilePara);

/*
 * Initialize state for a new group of input values.
 */
static inline void initialize_counts(SetOpStatePerGroup pergroup)
{
    pergroup->numLeft = pergroup->numRight = 0;
}

/*
 * Advance the appropriate counter for one input tuple.
 */
static inline void advance_counts(SetOpStatePerGroup pergroup, int flag)
{
    if (flag)
        pergroup->numRight++;
    else
        pergroup->numLeft++;
}

/*
 * Fetch the "flag" column from an input tuple.
 * This is an integer column with value 0 for left side, 1 for right side.
 */
static int fetch_tuple_flag(SetOpState* setopstate, TupleTableSlot* inputslot)
{
    SetOp* node = (SetOp*)setopstate->ps.plan;
    int flag;
    bool is_null = false;

    Assert(inputslot != NULL && inputslot->tts_tupleDescriptor != NULL);

    flag = DatumGetInt32(tableam_tslot_getattr(inputslot, node->flagColIdx, &is_null));
    Assert(!is_null);
    Assert(flag == 0 || flag == 1);
    return flag;
}

/*
 * Initialize the hash table to empty.
 */
static void build_hash_table(SetOpState* setopstate)
{
    SetOp* node = (SetOp*)setopstate->ps.plan;
    int64 work_mem = SET_NODEMEM(node->plan.operatorMemKB[0], node->plan.dop);

    Assert(node->strategy == SETOP_HASHED);
    Assert(node->numGroups > 0);

    setopstate->hashtable = BuildTupleHashTable(node->numCols,
        node->dupColIdx,
        setopstate->eqfunctions,
        setopstate->hashfunctions,
        node->numGroups,
        sizeof(SetOpHashEntryData),
        setopstate->tableContext,
        setopstate->tempContext,
        work_mem);
}

/*
 * We've completed processing a tuple group.  Decide how many copies (if any)
 * of its representative row to emit, and store the count into numOutput.
 * This logic is straight from the SQL92 specification.
 */
static void set_output_count(SetOpState* setopstate, SetOpStatePerGroup pergroup)
{
    SetOp* plan_node = (SetOp*)setopstate->ps.plan;

    switch (plan_node->cmd) {
        case SETOPCMD_INTERSECT:
            if (pergroup->numLeft > 0 && pergroup->numRight > 0)
                setopstate->numOutput = 1;
            else
                setopstate->numOutput = 0;
            break;
        case SETOPCMD_INTERSECT_ALL:
            setopstate->numOutput = (pergroup->numLeft < pergroup->numRight) ? pergroup->numLeft : pergroup->numRight;
            break;
        case SETOPCMD_EXCEPT:
            if (pergroup->numLeft > 0 && pergroup->numRight == 0)
                setopstate->numOutput = 1;
            else
                setopstate->numOutput = 0;
            break;
        case SETOPCMD_EXCEPT_ALL:
            setopstate->numOutput =
                (pergroup->numLeft < pergroup->numRight) ? 0 : (pergroup->numLeft - pergroup->numRight);
            break;
        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized set op: %d when seting the count of output.", (int)plan_node->cmd)));
            break;
    }
}

/* ----------------------------------------------------------------
 *		ExecSetOp
 * ----------------------------------------------------------------
 */
/* return: a tuple or NULL */
TupleTableSlot* ExecSetOp(SetOpState* node)
{
    SetOp* plan_node = (SetOp*)node->ps.plan;
    TupleTableSlot* result_tuple_slot = node->ps.ps_ResultTupleSlot;

    /*
     * If the previously-returned tuple needs to be returned more than once,
     * keep returning it.
     */
    if (node->numOutput > 0) {
        node->numOutput--;
        return result_tuple_slot;
    }

    /* Otherwise, we're done if we are out of groups */
    if (node->setop_done)
        return NULL;

    /* Fetch the next tuple group according to the correct strategy */
    if (plan_node->strategy == SETOP_HASHED) {
        return setop_retrieve(node);
    } else
        return setop_retrieve_direct(node);
}

/*
 * ExecSetOp for non-hashed case
 */
static TupleTableSlot* setop_retrieve_direct(SetOpState* setopstate)
{
    SetOp* node = (SetOp*)setopstate->ps.plan;
    TupleTableSlot* outer_slot = NULL;

    /*
     * get state info from node
     */
    PlanState* outer_plan = outerPlanState(setopstate);
    SetOpStatePerGroup pergroup = setopstate->pergroup;
    TupleTableSlot* result_tuple_slot = setopstate->ps.ps_ResultTupleSlot;

    /*
     * We loop retrieving groups until we find one we should return
     */
    while (!setopstate->setop_done) {
        /*
         * If we don't already have the first tuple of the new group, fetch it
         * from the outer plan.
         */
        if (setopstate->grp_firstTuple == NULL) {
            outer_slot = ExecProcNode(outer_plan);
            if (!TupIsNull(outer_slot)) {
                /* Make a copy of the first input tuple */
                setopstate->grp_firstTuple = ExecCopySlotTuple(outer_slot);
            } else {
                /* outer plan produced no tuples at all */
                setopstate->setop_done = true;
                return NULL;
            }
        }

        /*
         * Store the copied first input tuple in the tuple table slot reserved
         * for it.	The tuple will be deleted when it is cleared from the
         * slot.
         */
        (void)ExecStoreTuple(setopstate->grp_firstTuple, result_tuple_slot, InvalidBuffer, true);
        setopstate->grp_firstTuple = NULL; /* don't keep two pointers */

        /* Initialize working state for a new input tuple group */
        initialize_counts(pergroup);

        /* Count the first input tuple */
        advance_counts(pergroup, fetch_tuple_flag(setopstate, result_tuple_slot));

        /*
         * Scan the outer plan until we exhaust it or cross a group boundary.
         */
        for (;;) {
            outer_slot = ExecProcNode(outer_plan);
            if (TupIsNull(outer_slot)) {
                /* no more outer-plan tuples available */
                setopstate->setop_done = true;
                break;
            }

            /*
             * Check whether we've crossed a group boundary.
             */
            if (!execTuplesMatch(result_tuple_slot, outer_slot, node->numCols, node->dupColIdx, setopstate->eqfunctions,
                                 setopstate->tempContext)) {
                /*
                 * Save the first input tuple of the next group.
                 */
                setopstate->grp_firstTuple = ExecCopySlotTuple(outer_slot);
                break;
            }

            /* Still in same group, so count this tuple */
            advance_counts(pergroup, fetch_tuple_flag(setopstate, outer_slot));
        }

        /*
         * Done scanning input tuple group.  See if we should emit any copies
         * of result tuple, and if so return the first copy.
         */
        set_output_count(setopstate, pergroup);

        if (setopstate->numOutput > 0) {
            setopstate->numOutput--;
            return result_tuple_slot;
        }
    }

    /* No more groups */
    (void)ExecClearTuple(result_tuple_slot);
    return NULL;
}

/*
 * ExecSetOp for hashed case: phase 1, read input and build hash table
 */
static void setop_fill_hash_table(SetOpState* setopstate)
{
    SetOp* node = (SetOp*)setopstate->ps.plan;
    int first_flag;
    bool PG_USED_FOR_ASSERTS_ONLY in_first_rel = false;
    SetopWriteFileControl* temp_file_control = (SetopWriteFileControl*)setopstate->TempFileControl;
    MemoryContext old_context = NULL;

    /*
     * get state info from node
     */
    first_flag = node->firstFlag;
    /* verify planner didn't mess up */
    Assert(first_flag == 0 || (first_flag == 1 && (node->cmd == SETOPCMD_INTERSECT ||
           node->cmd == SETOPCMD_INTERSECT_ALL)));

    /*
     * Process each outer-plan tuple, and then fetch the next one, until we
     * exhaust the outer plan.
     */
    in_first_rel = true;
    WaitState old_status = pgstat_report_waitstatus(STATE_EXEC_HASHSETOP_BUILD_HASH);
    for (;;) {
        int flag;
        SetOpHashEntry entry = NULL;
        bool is_new = false;

        TupleTableSlot* outer_slot = temp_file_control->m_hashAggSource->getTup();
        if (TupIsNull(outer_slot)) {
            (void)pgstat_report_waitstatus(old_status);
            break;
        }

        /* Identify whether it's left or right input */
        flag = fetch_tuple_flag(setopstate, outer_slot);
        if (flag == first_flag) {
            /* (still) in first input relation */
            Assert(in_first_rel);

            if (temp_file_control->spillToDisk == false || temp_file_control->finishwrite == true)
                /* Find or build hashtable entry for this tuple's group */
                entry = (SetOpHashEntry)LookupTupleHashEntry(setopstate->hashtable, outer_slot, &is_new, true);
            /* this slot need to be inserted into temp file instead of hash table if it is not existed in hash table */
            else
                entry = (SetOpHashEntry)LookupTupleHashEntry(setopstate->hashtable, outer_slot, &is_new, false);

            if (is_new) {
                /* this slot is new and has been inserted to hash table */
                if (entry != NULL) {
                    /* If new tuple group, initialize counts */
                    initialize_counts(&entry->pergroup);
                    agg_spill_to_disk(temp_file_control,
                        setopstate->hashtable,
                        outer_slot,
                        ((SetOp*)setopstate->ps.plan)->numGroups,
                        false,
                        setopstate->ps.plan->plan_node_id,
                        SET_DOP(setopstate->ps.plan->dop),
                        setopstate->ps.instrument);

                    if (temp_file_control->filesource) {
                        if (setopstate->ps.instrument) {
                            temp_file_control->filesource->m_spill_size =
                                &setopstate->ps.instrument->sorthashinfo.spill_size;
                        } else {
                            temp_file_control->filesource->m_spill_size = &setopstate->spill_size;
                        }
                    }
                    /* this slot is new, it need be inserted to temp file */
                } else {
                    Assert(temp_file_control->spillToDisk == true && temp_file_control->finishwrite == false);

                    uint32 hash_value;
                    MinimalTuple tuple = ExecFetchSlotMinimalTuple(outer_slot);
                    old_context = MemoryContextSwitchTo(setopstate->tempContext);
                    hash_value = ComputeHashValue(setopstate->hashtable);
                    MemoryContextSwitchTo(old_context);
                    temp_file_control->filesource->writeTup(tuple, hash_value & (temp_file_control->filenum - 1));
                }
            }

            if (entry != NULL) {
                /* Advance the counts */
                advance_counts(&entry->pergroup, flag);
            }
        } else {
            /* reached second relation */
            in_first_rel = false;

            /* For tuples not seen previously, do not make hashtable entry */
            entry = (SetOpHashEntry)LookupTupleHashEntry(setopstate->hashtable, outer_slot, NULL);
            /* Advance the counts if entry is already present */
            if (entry != NULL) {
                advance_counts(&entry->pergroup, flag);
            } else {
                if (temp_file_control->spillToDisk == true && temp_file_control->finishwrite == false) {
                    uint32 hash_value;
                    MinimalTuple tuple = ExecFetchSlotMinimalTuple(outer_slot);
                    old_context = MemoryContextSwitchTo(setopstate->tempContext);
                    hash_value = ComputeHashValue(setopstate->hashtable);
                    MemoryContextSwitchTo(old_context);
                    temp_file_control->filesource->writeTup(tuple, hash_value & (temp_file_control->filenum - 1));
                }
            }
        }

        /* Must reset temp context after each hashtable lookup */
        MemoryContextReset(setopstate->tempContext);
    }

    setopstate->table_filled = true;
    if (HAS_INSTR(setopstate, true)) {
        if (temp_file_control->spillToDisk == false && temp_file_control->inmemoryRownum > 0) {
            setopstate->hashtable->width /= temp_file_control->inmemoryRownum;
        }
        if (temp_file_control->strategy == MEMORY_HASHAGG) {
            setopstate->ps.instrument->width = (int)setopstate->hashtable->width;
        }
        setopstate->ps.instrument->sysBusy = setopstate->hashtable->causedBySysRes;
        setopstate->ps.instrument->spreadNum = temp_file_control->spreadNum;
    }
    if (temp_file_control->spillToDisk && temp_file_control->finishwrite == false) {
        temp_file_control->finishwrite = true;
        if (HAS_INSTR(setopstate, true)) {
            PlanState* planstate = &setopstate->ps;
            planstate->instrument->sorthashinfo.hash_FileNum = (temp_file_control->filenum);
            planstate->instrument->sorthashinfo.hash_writefile = true;
        }
    }
    /* Initialize to walk the hash table */
    ResetTupleHashIterator(setopstate->hashtable, &setopstate->hashiter);
}

static bool prepare_data_source(SetOpState* node)
{
    SetopWriteFileControl* tempfile_control = (SetopWriteFileControl*)node->TempFileControl;
    /* get data from lefttree node */
    if (tempfile_control->strategy == MEMORY_HASHSETOP) {
        /*
         * To avoid unnesseray memory allocate during initialization, we move building
         * process here and hash table should only be initialized once.
         */
        if (unlikely(node->hashtable == NULL)) {
            build_hash_table(node);
        }
        tempfile_control->m_hashAggSource = New(CurrentMemoryContext) hashOpSource(outerPlanState(node));
        /* get data from temp file */
    } else if (tempfile_control->strategy == DIST_HASHSETOP) {
        tempfile_control->m_hashAggSource = tempfile_control->filesource;
        if (tempfile_control->curfile >= 0) {
            Assert(tempfile_control->curfile < tempfile_control->filenum);
            tempfile_control->filesource->close(tempfile_control->curfile);
        }
        tempfile_control->curfile++;
        while (tempfile_control->curfile < tempfile_control->filenum) {
            int currfileidx = tempfile_control->curfile;
            if (tempfile_control->filesource->m_rownum[currfileidx] != 0) {
                tempfile_control->filesource->setCurrentIdx(currfileidx);
                MemoryContextResetAndDeleteChildren(node->tableContext);
                build_hash_table(node);

                tempfile_control->filesource->rewind(currfileidx);
                node->table_filled = false;
                node->setop_done = false;
                break;
                /* no data in this temp file */
            } else {
                tempfile_control->filesource->close(currfileidx);
                tempfile_control->curfile++;
            }
        }
        if (tempfile_control->curfile == tempfile_control->filenum) {
            return false;
        }
    } else {
        Assert(false);
    }
    tempfile_control->runState = HASHSETOP_FETCH;
    return true;
}

static TupleTableSlot* setop_retrieve(SetOpState* setopstate)
{
    SetopWriteFileControl* tempfile_control = (SetopWriteFileControl*)setopstate->TempFileControl;
    TupleTableSlot* tmptup = NULL;
    for (;;) {
        switch (tempfile_control->runState) {
            case HASHSETOP_PREPARE: {
                if (!prepare_data_source(setopstate)) {
                    return NULL;
                }
                break;
            }
            case HASHSETOP_FETCH: {
                if (!setopstate->table_filled)
                    setop_fill_hash_table(setopstate);
                tmptup = setop_retrieve_hash_table(setopstate);
                if (tmptup != NULL) {
                    return tmptup;
                } else if (tmptup == NULL && tempfile_control->spillToDisk == true) {
                    tempfile_control->runState = HASHSETOP_PREPARE;
                    tempfile_control->strategy = DIST_HASHSETOP;
                } else {
                    return NULL;
                }
                break;
            }
            default:
                break;
        }
    }
}
/*
 * ExecSetOp for hashed case: phase 2, retrieving groups from hash table
 */
static TupleTableSlot* setop_retrieve_hash_table(SetOpState* setopstate)
{
    SetOpHashEntry entry = NULL;
    TupleTableSlot* result_tuple_slot = NULL;

    /*
     * get state info from node
     */
    result_tuple_slot = setopstate->ps.ps_ResultTupleSlot;

    /*
     * We loop retrieving groups until we find one we should return
     */
    while (!setopstate->setop_done) {
        /*
         * Find the next entry in the hash table
         */
        entry = (SetOpHashEntry)ScanTupleHashTable(&setopstate->hashiter);
        if (entry == NULL) {
            /* No more entries in hashtable, so done */
            setopstate->setop_done = true;
            return NULL;
        }

        /*
         * See if we should emit any copies of this tuple, and if so return
         * the first copy.
         */
        set_output_count(setopstate, &entry->pergroup);

        if (setopstate->numOutput > 0) {
            setopstate->numOutput--;
            return ExecStoreMinimalTuple(entry->shared.firstTuple, result_tuple_slot, false);
        }
    }

    /* No more groups */
    (void)ExecClearTuple(result_tuple_slot);
    return NULL;
}

/* ----------------------------------------------------------------
 *		ExecInitSetOp
 *
 *		This initializes the setop node state structures and
 *		the node's subplan.
 * ----------------------------------------------------------------
 */
SetOpState* ExecInitSetOp(SetOp* node, EState* estate, int eflags)
{
    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    /*
     * create state structure
     */
    SetOpState* setopstate = makeNode(SetOpState);
    setopstate->ps.plan = (Plan*)node;
    setopstate->ps.state = estate;

    setopstate->eqfunctions = NULL;
    setopstate->hashfunctions = NULL;
    setopstate->setop_done = false;
    setopstate->numOutput = 0;
    setopstate->pergroup = NULL;
    setopstate->grp_firstTuple = NULL;
    setopstate->hashtable = NULL;
    setopstate->tableContext = NULL;
    SetopWriteFileControl* tempfile_para = NULL;

    /*
     * Miscellaneous initialization
     *
     * SetOp nodes have no ExprContext initialization because they never call
     * ExecQual or ExecProject.  But they do need a per-tuple memory context
     * anyway for calling execTuplesMatch.
     */
    setopstate->tempContext = AllocSetContextCreate(
        CurrentMemoryContext, "SetOp", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE);

    /*
     * If hashing, we also need a longer-lived context to store the hash
     * table.  The table can't just be kept in the per-query context because
     * we want to be able to throw it away in ExecReScanSetOp.
     */
    if (node->strategy == SETOP_HASHED) {
        int64 operator_mem = SET_NODEMEM(node->plan.operatorMemKB[0], node->plan.dop);
        setopstate->tableContext = AllocSetContextCreate(CurrentMemoryContext,
            "SetOp hash table",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE,
            STANDARD_CONTEXT,
            operator_mem * 1024L);
    }

    /*
     * Tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &setopstate->ps);

    /*
     * initialize child nodes
     *
     * If we are hashing then the child plan does not need to handle REWIND
     * efficiently; see ExecReScanSetOp.
     */
    if (node->strategy == SETOP_HASHED) {
        eflags &= ~EXEC_FLAG_REWIND;
    }  
    outerPlanState(setopstate) = ExecInitNode(outerPlan(node), estate, eflags);

    /*
     * setop nodes do no projections, so initialize projection info for this
     * node appropriately
     */
    ExecAssignResultTypeFromTL(
            &setopstate->ps,
            ExecGetResultType(outerPlanState(setopstate))->tdTableAmType);

    setopstate->ps.ps_ProjInfo = NULL;

    /*
     * Precompute fmgr lookup data for inner loop. We need both equality and
     * hashing functions to do it by hashing, but only equality if not
     * hashing.
     */
    if (node->strategy == SETOP_HASHED) {
        execTuplesHashPrepare(node->numCols, node->dupOperators, &setopstate->eqfunctions, &setopstate->hashfunctions);
    } else {
        setopstate->eqfunctions = execTuplesMatchPrepare(node->numCols, node->dupOperators);
    }

    if (node->strategy == SETOP_HASHED) {
        setopstate->table_filled = false;
    } else {
        setopstate->pergroup = (SetOpStatePerGroup)palloc0(sizeof(SetOpStatePerGroupData));
    }

    if (node->strategy == SETOP_HASHED) {
        int64 operator_mem = SET_NODEMEM(node->plan.operatorMemKB[0], node->plan.dop);
        int64 max_mem = (node->plan.operatorMaxMem > 0) ? SET_NODEMEM(node->plan.operatorMaxMem, node->plan.dop) : 0;
        tempfile_para = (SetopWriteFileControl*)palloc(sizeof(SetopWriteFileControl));
        tempfile_para->strategy = MEMORY_HASHSETOP;
        tempfile_para->spillToDisk = false;
        tempfile_para->totalMem = operator_mem * 1024L;
        tempfile_para->useMem = 0;
        tempfile_para->inmemoryRownum = 0;
        tempfile_para->finishwrite = false;
        tempfile_para->runState = HASHSETOP_PREPARE;
        tempfile_para->curfile = -1;
        tempfile_para->filenum = 0;
        tempfile_para->filesource = NULL;
        tempfile_para->m_hashAggSource = NULL;
        tempfile_para->maxMem = max_mem * 1024L;
        tempfile_para->spreadNum = 0;
    }
    setopstate->TempFileControl = tempfile_para;

    return setopstate;
}
void ClearSetOp(SetOpState* node, SetopWriteFileControl* TempFileControl)
{
    if (TempFileControl != NULL) {
        int file_num = TempFileControl->filenum;
        hashFileSource* file = TempFileControl->filesource;

        if (file != NULL) {
            for (int i = 0; i < file_num; i++) {
                file->close(i);
            }
            file->freeFileSource();
        }
    }
    /* clean up tuple table */
    (void)ExecClearTuple(node->ps.ps_ResultTupleSlot);

    /* free subsidiary stuff including hashtable */
    if (node->tempContext) {
        MemoryContextDelete(node->tempContext);
        node->tempContext = NULL;
    }

    if (node->tableContext) {
        MemoryContextDelete(node->tableContext);
        node->tableContext = NULL;
    }
}
/* ----------------------------------------------------------------
 *		ExecEndSetOp
 *
 *		This shuts down the subplan and frees resources allocated
 *		to this node.
 * ----------------------------------------------------------------
 */
void ExecEndSetOp(SetOpState* node)
{
    SetopWriteFileControl* tempfile_control = (SetopWriteFileControl*)node->TempFileControl;

    ClearSetOp(node, tempfile_control);

    ExecEndNode(outerPlanState(node));
}

void ReSetFile(SetOpState* node, SetopWriteFileControl* TempFilePara)
{
    SetopWriteFileControl* tempfile_control = (SetopWriteFileControl*)node->TempFileControl;

    int file_num = tempfile_control->filenum;

    hashFileSource* file = tempfile_control->filesource;

    Plan* plan = node->ps.plan;
    int64 operator_mem = SET_NODEMEM(plan->operatorMemKB[0], plan->dop);
    int64 max_mem = (plan->operatorMaxMem > 0) ? SET_NODEMEM(plan->operatorMaxMem, plan->dop) : 0;

    if (NULL != file) {
        for (int i = 0; i < file_num; i++) {
            file->close(i);
        }
        file->freeFileSource();
    }
    build_hash_table(node);
    node->table_filled = false;
    /* reset hashagg temp file para */
    TempFilePara->strategy = MEMORY_HASHSETOP;
    TempFilePara->spillToDisk = false;
    TempFilePara->finishwrite = false;
    TempFilePara->totalMem = operator_mem * 1024L;
    TempFilePara->useMem = 0;
    TempFilePara->inmemoryRownum = 0;
    TempFilePara->runState = HASHSETOP_PREPARE;
    TempFilePara->curfile = -1;
    TempFilePara->filenum = 0;
    TempFilePara->maxMem = max_mem * 1024L;
    TempFilePara->spreadNum = 0;
}

void ExecReScanSetOp(SetOpState* node)
{
    SetopWriteFileControl* tempfile_para = (SetopWriteFileControl*)node->TempFileControl;

    /* Already reset, just rescan lefttree */
    if (node->ps.recursive_reset && node->ps.state->es_recursive_next_iteration) {
        if (node->ps.lefttree->chgParam == NULL)
            ExecReScan(node->ps.lefttree);

        node->ps.recursive_reset = false;
        return;
    }

    (void)ExecClearTuple(node->ps.ps_ResultTupleSlot);
    node->setop_done = false;
    node->numOutput = 0;

    if (((SetOp*)node->ps.plan)->strategy == SETOP_HASHED) {
        /*
         * In the hashed case, if we haven't yet built the hash table then we
         * can just return; nothing done yet, so nothing to undo. If subnode's
         * chgParam is not NULL then it will be re-scanned by ExecProcNode,
         * else no reason to re-scan it at all.
         */
        if (!node->table_filled)
            return;

        /*
         * If we do have the hash table and the subplan does not have any
         * parameter changes, then we can just rescan the existing hash table;
         * no need to build it again.
         */
        if (node->ps.lefttree->chgParam == NULL && tempfile_para->spillToDisk == false) {
            ResetTupleHashIterator(node->hashtable, &node->hashiter);
            return;
        }
    }

    /* Release first tuple of group, if we have made a copy */
    if (node->grp_firstTuple != NULL) {
        tableam_tops_free_tuple(node->grp_firstTuple);
    }

    /* Release any hashtable storage */
    if (node->tableContext)
        MemoryContextResetAndDeleteChildren(node->tableContext);

    /* And rebuild empty hashtable if needed */
    if (((SetOp*)node->ps.plan)->strategy == SETOP_HASHED) {
        ReSetFile(node, tempfile_para);
    }

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (node->ps.lefttree->chgParam == NULL)
        ExecReScan(node->ps.lefttree);
}

/*
 * @Description: Early free the memory for HashedSetop.
 *
 * @param[IN] node:  executor state for HashedSetop
 * @return: void
 */
void ExecEarlyFreeHashedSetop(SetOpState* node)
{
    SetopWriteFileControl* tempfile_control = (SetopWriteFileControl*)node->TempFileControl;
    PlanState* plan_state = &node->ps;

    if (plan_state->earlyFreed)
        return;

    ClearSetOp(node, tempfile_control);

    EARLY_FREE_LOG(elog(LOG,
        "Early Free: After early freeing Setop "
        "at node %d, memory used %d MB.",
        plan_state->plan->plan_node_id,
        getSessionMemoryUsageMB()));

    plan_state->earlyFreed = true;
    ExecEarlyFree(outerPlanState(node));
}

/*
 * @Function: ExecReSetSetOp()
 *
 * @Brief: Reset the setop state structure in rescan case
 *	under recursive-stream new iteration condition.
 *
 * @Input node: node setop planstate
 *
 * @Return: no return value
 */
void ExecReSetSetOp(SetOpState* node)
{
    Assert(IS_PGXC_DATANODE && node != NULL && IsA(node, SetOpState));
    Assert(EXEC_IN_RECURSIVE_MODE(node->ps.plan));

    SetopWriteFileControl* tempfile_para = (SetopWriteFileControl*)node->TempFileControl;
    (void)ExecClearTuple(node->ps.ps_ResultTupleSlot);
    node->setop_done = false;
    node->numOutput = 0;

    /* Release first tuple of group, if we have made a copy */
    if (node->grp_firstTuple != NULL) {
        tableam_tops_free_tuple(node->grp_firstTuple);
        node->grp_firstTuple = NULL;
    }

    /* Release any hashtable storage */
    if (node->tableContext)
        MemoryContextResetAndDeleteChildren(node->tableContext);

    /* And rebuild empty hashtable if needed */
    if (((SetOp*)node->ps.plan)->strategy == SETOP_HASHED) {
        ReSetFile(node, tempfile_para);
    }

    node->ps.recursive_reset = true;

    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (node->ps.lefttree->chgParam == NULL)
        ExecReSetRecursivePlanTree(node->ps.lefttree);
}
