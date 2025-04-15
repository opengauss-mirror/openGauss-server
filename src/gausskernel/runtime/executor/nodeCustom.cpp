/* ------------------------------------------------------------------------
 *
 * nodeCustom.c
 *		Routines to handle execution of custom scan node
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * ------------------------------------------------------------------------
 */
#include "postgres.h"

#include "executor/executor.h"
#include "executor/node/nodeCustom.h"
#include "miscadmin.h"
#include "nodes/execnodes.h"
#include "nodes/extensible.h"
#include "nodes/plannodes.h"
#include "utils/rel.h"

static TupleTableSlot *ExecCustomScan(PlanState *pstate);


CustomScanState *
ExecInitCustomScan(CustomScan *cscan, EState *estate, int eflags)
{
    CustomScanState *css;
    const TableAmRoutine *table_am;
    Relation              scan_rel = NULL;
    Index                 scanrelid = cscan->scan.scanrelid;
    int                   tlistvarno;
    TupleDesc             scan_tupdesc;
    /*
     * Allocate the CustomScanState object.  We let the custom scan provider
     * do the palloc, in case it wants to make a larger object that embeds
     * CustomScanState as the first field.  It must set the node tag and the
     * methods field correctly at this time.  Other standard fields should be
     * set to zero.
     */
    css = castNode(CustomScanState, cscan->methods->CreateCustomScanState(cscan));

    /* ensure flags is filled correctly */
    css->flags = cscan->flags;

    /* fill up fields of ScanState */
    css->ss.ps.plan = &cscan->scan.plan;
    css->ss.ps.state = estate;
    css->ss.ps.ExecProcNode = ExecCustomScan;

    /* create expression context for node */
    ExecAssignExprContext(estate, &css->ss.ps);

    /*
     * open the scan relation, if any
     */
    if (scanrelid > 0) {
        scan_rel = ExecOpenScanRelation(estate, scanrelid);
        css->ss.ss_currentRelation = scan_rel;
    }

    /*
     * Use a custom slot if specified in CustomScanState or use virtual slot
     * otherwise.
     */
    table_am = css->tb_am;
    if (table_am == nullptr) {
        table_am = TableAmHeap;
    }

    /*
     * Determine the scan tuple type.  If the custom scan provider provided a
     * targetlist describing the scan tuples, use that; else use base
     * relation's rowtype.
     */
    if (cscan->custom_scan_tlist != NIL || scan_rel == NULL) {
        scan_tupdesc = ExecTypeFromTL(cscan->custom_scan_tlist, false);
        ExecInitScanTupleSlot(estate, &css->ss, table_am);
        ExecAssignScanType(&css->ss, scan_tupdesc);
        /* Node's targetlist will contain Vars with varno = INDEX_VAR */
        tlistvarno = INDEX_VAR;
    } else {
        scan_tupdesc = RelationGetDescr(scan_rel);
        ExecInitScanTupleSlot(estate, &css->ss, table_am);
        ExecAssignScanType(&css->ss, scan_tupdesc);
        /* Node's targetlist will contain Vars with varno = scanrelid */
        tlistvarno = scanrelid;
    }


    /*
     * Initialize result slot, type and projection.
     */
    ExecInitResultTupleSlot(css->ss.ps.state, &css->ss.ps, table_am);
    ExecAssignResultTypeFromTL(&css->ss.ps, table_am);
    ExecAssignScanProjectionInfoWithVarno(&css->ss, tlistvarno);

    /* initialize child expressions */
    if (css->ss.ps.state->es_is_flt_frame) {
        css->ss.ps.qual = (List *)ExecInitQualByFlatten(cscan->scan.plan.qual, (PlanState *)css);
    } else {
        css->ss.ps.targetlist =
            (List *)ExecInitExprByRecursion((Expr *)cscan->scan.plan.targetlist, (PlanState *)css);
        css->ss.ps.qual =
            (List *)ExecInitExprByRecursion((Expr *)cscan->scan.plan.qual, (PlanState *)css);
    }

    /*
     * The callback of custom-scan provider applies the final initialization
     * of the custom-scan-state node according to its logic.
     */
    css->methods->BeginCustomScan(css, estate, eflags);

    return css;
}

static TupleTableSlot *
ExecCustomScan(PlanState *pstate)
{
    CustomScanState *node = castNode(CustomScanState, pstate);

    CHECK_FOR_INTERRUPTS();

    Assert(node->methods->ExecCustomScan != NULL);
    return node->methods->ExecCustomScan(node);
}

void
ExecEndCustomScan(CustomScanState *node)
{
    Assert(node->methods->EndCustomScan != NULL);
    node->methods->EndCustomScan(node);
}

void
ExecReScanCustomScan(CustomScanState *node)
{
    Assert(node->methods->ReScanCustomScan != NULL);
    node->methods->ReScanCustomScan(node);
}

void
ExecCustomMarkPos(CustomScanState *node)
{
    if (!node->methods->MarkPosCustomScan) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("custom scan \"%s\" does not support MarkPos",
                        node->methods->CustomName)));
    }
    node->methods->MarkPosCustomScan(node);
}

void
ExecCustomRestrPos(CustomScanState *node)
{
    if (!node->methods->RestrPosCustomScan) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("custom scan \"%s\" does not support MarkPos",
                        node->methods->CustomName)));
    }
    node->methods->RestrPosCustomScan(node);
}
