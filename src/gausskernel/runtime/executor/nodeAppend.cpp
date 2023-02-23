/* -------------------------------------------------------------------------
 *
 * nodeAppend.cpp
 *	  routines to handle append nodes.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeAppend.cpp
 *
 * -------------------------------------------------------------------------
 * INTERFACE ROUTINES
 *		ExecInitAppend	- initialize the append node
 *		ExecAppend		- retrieve the next tuple from the node
 *		ExecEndAppend	- shut down the append node
 *		ExecReScanAppend - rescan the append node
 *
 *	 NOTES
 *		Each append node contains a list of one or more subplans which
 *		must be iteratively processed (forwards or backwards).
 *		Tuples are retrieved by executing the 'whichplan'th subplan
 *		until the subplan stops returning tuples, at which point that
 *		plan is shut down and the next started up.
 *
 *		Append nodes don't make use of their left and right
 *		subtrees, rather they maintain a list of subplans so
 *		a typical append node looks like this in the plan tree:
 *
 *				   ...
 *				   /
 *				Append -------+------+------+--- nil
 *				/	\		  |		 |		|
 *			  nil	nil		 ...	...    ...
 *								 subplans
 *
 *		Append nodes are currently used for unions, and to support
 *		inheritance queries, where several relations need to be scanned.
 *		For example, in our standard person/student/employee/student-emp
 *		example, where student and employee inherit from person
 *		and student-emp inherits from student and employee, the
 *		query:
 *
 *				select name from person
 *
 *		generates the plan:
 *
 *				  |
 *				Append -------+-------+--------+--------+
 *				/	\		  |		  |		   |		|
 *			  nil	nil		 Scan	 Scan	  Scan	   Scan
 *							  |		  |		   |		|
 *							person employee student student-emp
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/exec/execdebug.h"
#include "executor/node/nodeAppend.h"

static TupleTableSlot* ExecAppend(PlanState* state);

/* ----------------------------------------------------------------
 *		exec_append_initialize_next
 *
 *		Sets up the append state node for the "next" scan.
 *
 *		Returns t iff there is a "next" scan to process.
 * ----------------------------------------------------------------
 */
bool exec_append_initialize_next(AppendState* appendstate)
{
    int whichplan;

    /*
     * get information from the append node
     */
    whichplan = appendstate->as_whichplan;

    if (whichplan < 0) {
        /*
         * if scanning in reverse, we start at the last scan in the list and
         * then proceed back to the first.. in any case we inform ExecAppend
         * that we are at the end of the line by returning FALSE
         */
        appendstate->as_whichplan = 0;
        return FALSE;
    } else if (whichplan >= appendstate->as_nplans) {
        /*
         * as above, end the scan if we go beyond the last scan in our list..
         */
        appendstate->as_whichplan = appendstate->as_nplans - 1;
        return FALSE;
    } else {
        return TRUE;
    }
}

/* ----------------------------------------------------------------
 *		ExecInitAppend
 *
 *		Begin all of the subscans of the append node.
 *
 *	   (This is potentially wasteful, since the entire result of the
 *		append node may not be scanned, but this way all of the
 *		structures get allocated in the executor's top level memory
 *		block instead of that of the call to ExecAppend.)
 * ----------------------------------------------------------------
 */
AppendState* ExecInitAppend(Append* node, EState* estate, int eflags)
{
    AppendState* appendstate = makeNode(AppendState);
    PlanState** appendplanstates;
    int nplans;
    int i;
    ListCell* lc = NULL;

    /* check for unsupported flags */
    Assert(!(eflags & EXEC_FLAG_MARK));

    /*
     * Set up empty vector of subplan states
     */
    nplans = list_length(node->appendplans);

    appendplanstates = (PlanState**)palloc0(nplans * sizeof(PlanState*));

    /*
     * create new AppendState for our append node
     */
    appendstate->ps.plan = (Plan*)node;
    appendstate->ps.state = estate;
    appendstate->appendplans = appendplanstates;
    appendstate->as_nplans = nplans;
    appendstate->ps.ExecProcNode = ExecAppend;

    /*
     * Miscellaneous initialization
     *
     * Append plans don't have expression contexts because they never call
     * ExecQual or ExecProject.
     */
    /*
     * append nodes still have Result slots, which hold pointers to tuples, so
     * we have to initialize them.
     */
    ExecInitResultTupleSlot(estate, &appendstate->ps);

    /*
     * call ExecInitNode on each of the plans to be executed and save the
     * results into the array "appendplans".
     */
    i = 0;
    foreach (lc, node->appendplans) {
        Plan* initNode = (Plan*)lfirst(lc);

        appendplanstates[i] = ExecInitNode(initNode, estate, eflags);
        i++;
    }

    /*
     * initialize output tuple type
     * Result tuple slot of Append always contains a virtual tuple,
     * Default tableAMtype for this slot is Heap.
     */
    ExecAssignResultTypeFromTL(&appendstate->ps);
    appendstate->ps.ps_ProjInfo = NULL;

    /*
     * initialize to scan first subplan
     */
    appendstate->as_whichplan = 0;
    (void)exec_append_initialize_next(appendstate);
    return appendstate;
}

/* ----------------------------------------------------------------
 *	   ExecAppend
 *
 *		Handles iteration over multiple subplans.
 * ----------------------------------------------------------------
 */
static TupleTableSlot* ExecAppend(PlanState* state)
{
    AppendState* node = castNode(AppendState, state);
    for (;;) {
        PlanState* subnode = NULL;
        TupleTableSlot* result = NULL;

        CHECK_FOR_INTERRUPTS();

        /*
         * figure out which subplan we are currently processing
         */
        subnode = node->appendplans[node->as_whichplan];

        /*
         * get a tuple from the subplan
         */
        result = ExecProcNode(subnode);
        if (!TupIsNull(result)) {
            /*
             * If the subplan gave us something then return it as-is. We do
             * NOT make use of the result slot that was set up in
             * ExecInitAppend; there's no need for it.
             */
            return result;
        }

        /* Early free each of subplans after finishing execution */
        ExecEarlyFree(subnode);

        /*
         * Go on to the "next" subplan in the appropriate direction. If no
         * more subplans, return the empty slot set up for us by
         * ExecInitAppend.
         */
        if (ScanDirectionIsForward(node->ps.state->es_direction))
            node->as_whichplan++;
        else
            node->as_whichplan--;
        if (!exec_append_initialize_next(node))
            return ExecClearTuple(node->ps.ps_ResultTupleSlot);

        /* Else loop back and try to get a tuple from the new subplan */
    }
}

/* ----------------------------------------------------------------
 *		ExecEndAppend
 *
 *		Shuts down the subscans of the append node.
 *
 *		Returns nothing of interest.
 * ----------------------------------------------------------------
 */
void ExecEndAppend(AppendState* node)
{
    PlanState** appendplans;
    int nplans;
    int i;

    /*
     * get information from the node
     */
    appendplans = node->appendplans;
    nplans = node->as_nplans;

    /*
     * shut down each of the subscans
     */
    for (i = 0; i < nplans; i++)
        ExecEndNode(appendplans[i]);
}

void ExecReScanAppend(AppendState* node)
{
    int i;

    for (i = 0; i < node->as_nplans; i++) {
        PlanState* subnode = node->appendplans[i];

        /*
         * ExecReScan doesn't know about my subplans, so I have to do
         * changed-parameter signaling myself.
         */
        if (node->ps.chgParam != NULL)
            UpdateChangedParamSet(subnode, node->ps.chgParam);

        /*
         * If chgParam of subnode is not null then plan will be re-scanned by
         * first ExecProcNode.
         */
        if (subnode->chgParam == NULL)
            ExecReScan(subnode);
    }
    node->as_whichplan = 0;
    (void)exec_append_initialize_next(node);
}
