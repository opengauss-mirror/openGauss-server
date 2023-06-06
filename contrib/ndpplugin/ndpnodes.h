/* -------------------------------------------------------------------------
 * ndpnodes.h
 *	  prototypes for functions in contrib/ndpplugin/ndpoutfuncs.cpp
 *
 * Portions Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * IDENTIFICATION
 *	  contrib/ndpplugin/ndpnodes.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef NDPNODES_H
#define NDPNODES_H

#include "postgres.h"
#include "commands/extension.h"
#include "ndp/ndp_nodes.h"

void stateToString(NdpPlanState* node, StringInfo str);
void queryToString(NdpQuery* node, StringInfo str);

#endif // NDPNODES_H
