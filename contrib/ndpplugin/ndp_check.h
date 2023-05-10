/* -------------------------------------------------------------------------
 * ndp_check.h
 *	  prototypes for functions in contrib/ndpplugin/ndp_check.cpp
 *
 * Portions Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * IDENTIFICATION
 *	  contrib/ndpplugin/ndp_check.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef NDP_CHECK_H
#define NDP_CHECK_H

#include "pgstat.h"
#include "catalog/pg_proc.h"

Plan* CheckAndGetNdpPlan(PlannedStmt* stmt, SeqScan* scan, Plan* parent);
bool CheckNdpSupport(Query* querytree, PlannedStmt *stmt);

#endif // NDP_CHECK_H
