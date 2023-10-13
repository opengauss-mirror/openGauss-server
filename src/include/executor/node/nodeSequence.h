/*-------------------------------------------------------------------------
 *
 * nodeSequence.h
 *    header file for nodeSequence.cpp.
 *
 * Portions Copyright (c) 2023 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2012 - 2022, EMC/Greenplum
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/executor/node/nodeSequence.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODESEQUENCE_H
#define NODESEQUENCE_H
 
#ifdef USE_SPQ
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
 
extern SequenceState *ExecInitSequence(Sequence *node, EState *estate, int eflags);
extern TupleTableSlot *ExecSequence(PlanState *pstate);
extern void ExecReScanSequence(SequenceState *node);
extern void ExecEndSequence(SequenceState *node);
#endif /* USE_SPQ */
 
#endif
