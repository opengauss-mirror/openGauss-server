/* -------------------------------------------------------------------------
 *
 * planswcb.h
 *	  prototypes for plan_startwith.cpp
 *
 *
 * Portions Copyright (c) 2021, openGauss Contributors
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/optimizer/planswcb.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PLANSWCB_H
#define PLANSWCB_H

#include "executor/exec/execdesc.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "nodes/parsenodes.h"
#include "utils/selfuncs.h"

typedef enum StartWithOpColumnType
{
    /* Pseudo return columns, generated in CteScan's output */
    SWCOL_LEVEL = 0,
    SWCOL_ISLEAF,
    SWCOL_ISCYCLE,
    SWCOL_ROWNUM,

    /* Pseudo internal columns, first generated in RuScan */
    SWCOL_RUITR,
    SWCOL_ARRAY_KEY,
    SWCOL_ARRAY_COL,

    /* Pseudo internal columns for siblings */
    SWCOL_ARRAY_SIBLINGS,

    /* Invalid */
    SWCOL_REGULAR,
    SWCOL_UNKNOWN
} StartWithOpColumnType;

#define CONNECT_BY_LEVEL_FAKEVALUE        100001
#define CONNECT_BY_ROWNUM_FAKEVALUE       100002

/*
 * control flags to support run-time optimization of SWCB
 */
#define StartWithSkipColumnMask         (0x000F)
#define StartWithSkipLevelMask          (0x01 << 0)
#define StartWithSkipIsLeafMask         (0x01 << 1)
#define StartWithSkipIsCycleMask        (0x01 << 2)
#define StartWithSkipRownumMask         (0x01 << 3)

#define ClearColumnSkipOptions(x) \
    (x &= ~StartWithSkipColumnMask)
#define SetColumnSkipOptions(x) \
    (x |= StartWithSkipColumnMask)

#define IsSkipLevel(x) \
    (x &  StartWithSkipLevelMask)
#define SetSkipLevel(x) \
    (x |= StartWithSkipLevelMask)
#define ClearSkipLevel(x) \
    (x &= ~StartWithSkipLevelMask)

#define IsSkipIsLeaf(x) \
    (x &  StartWithSkipIsLeafMask)
#define SetSkipIsLeaf(x)\
    (x |= StartWithSkipIsLeafMask)
#define ClearSkipIsLeaf(x)\
    (x &= ~StartWithSkipIsLeafMask)

#define IsSkipIsCycle(x) \
    (x &  StartWithSkipIsCycleMask)
#define SetSkipIsCycle(x) \
    (x |= StartWithSkipIsCycleMask)
#define ClearSkipIsCycle(x) \
    (x &= ~StartWithSkipIsCycleMask)

#define IsSkipRownum(x)   \
    (x &  StartWithSkipRownumMask)
#define SetSkipRownum(x) \
    (x |= StartWithSkipRownumMask)
#define ClearSkipRownum(x) \
    (x &= ~StartWithSkipRownumMask)

extern int32 GetStartWithFakeConstValue(A_Const *val);
extern Plan *AddStartWithOpProcNode(PlannerInfo *root, CteScan *cteplan, RecursiveUnion *ruplan);
extern StartWithOpColumnType GetPseudoColumnType(const char *resname);
extern StartWithOpColumnType GetPseudoColumnType(const TargetEntry *entry);
extern bool IsConnectByLevelStartWithPlan(const StartWithOp *plan);
extern bool IsPseudoReturnColumn(const char *resname);
extern bool IsPseudoInternalTargetEntry(const TargetEntry *tle);
extern bool IsPseudoReturnTargetEntry(const TargetEntry *tle);
extern bool IsCteScanProcessForStartWith(CteScan *ctescan);
extern void ProcessStartWithOpMixWork(PlannerInfo *root, Plan *topplan,
                                      PlannerInfo *subroot, StartWithOp *swplan);
extern List *FixSwTargetlistResname(PlannerInfo *root, RangeTblEntry *curRte, List *tlist);
typedef struct StartWithCTEPseudoReturnColumns {
    char    *colname;
    Oid      coltype;
    int32    coltypmod;
    Oid      colcollation;
} StartWithCTEPseudoReturnColumns;

extern StartWithCTEPseudoReturnColumns g_StartWithCTEPseudoReturnColumns[];

#define STARTWITH_PSEUDO_RETURN_ATTNUMS 4

#endif /* PLANSWCB_H */