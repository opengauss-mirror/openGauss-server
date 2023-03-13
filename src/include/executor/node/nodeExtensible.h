/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * ---------------------------------------------------------------------------------------
 * 
 * nodeExtensible.h
 *        the structure of extensible plan node
 * 
 * 
 * IDENTIFICATION
 *        src/include/executor/nodeExtensible.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef NODEEXTENSIBLE_H
#define NODEEXTENSIBLE_H

#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
/*
 * General executor code
 */
extern ExtensiblePlanState* ExecInitExtensiblePlan(ExtensiblePlan* eplan, EState* estate, int eflags);
extern void ExecEndExtensiblePlan(ExtensiblePlanState* node);

extern void ExecReScanExtensiblePlan(ExtensiblePlanState* node);
extern ExtensiblePlanMethods* GetExtensiblePlanMethods(const char* ExtensibleName, bool missing_ok);

#ifdef ENABLE_MULTIPLE_NODES
extern void InitExtensiblePlanMethodsHashTable();
#endif

/*
 * Flags for extensible paths, indicating what capabilities the resulting scan
 * will have.  The flags fields of ExtensiblePath and ExtensiblePlan nodes are
 * bitmasks of these flags.
 */
#define EXTENSIBLEPATH_SUPPORT_BACKWARD_SCAN 0x0001
#define EXTENSIBLEPATH_SUPPORT_MARK_RESTORE 0x0002
#define EXTENSIBLEPATH_SUPPORT_PROJECTION 0x0004

#endif /* NODEEXTENSIBLE_H */
