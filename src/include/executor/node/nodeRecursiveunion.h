/* -------------------------------------------------------------------------
 *
 * nodeRecursiveunion.h
 *
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeRecursiveunion.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef NODERECURSIVEUNION_H
#define NODERECURSIVEUNION_H

#include "executor/exec/execStream.h"
#include "nodes/execnodes.h"
#include "storage/smgr/fd.h"
#include "storage/vfd.h"
/*
 * --------------------------------------------------------------------------------------
 * MPPDB Distributed RecursiveUnion Support (with-recursive)
 * --------------------------------------------------------------------------------------
 */

extern THR_LOCAL char* producer_top_plannode_str;
extern THR_LOCAL bool is_syncup_producer;

extern char* nodeTagToString(NodeTag type);

/* Cluster-wide sync up each datanodes that none-recursive part has finished */
#define WITH_RECURSIVE_SYNC_NONERQ 1

/* Cluster-wide sync up each datanodes that one iteration is done */
#define WITH_RECURSIVE_SYNC_RQSTEP 2

/* Cluster-wide sync up each datanodes that all iteration steps are done */
#define WITH_RECURSIVE_SYNC_DONE 3

/*
 * Recursive Union message types
 *    - 'R' indicates datanode STEP finsih the iteration (W->C)
 *    - 'F' indicates cluster  STEP finsih the iteration (C->W)
 */
#define RUSYNC_MSGTYPE_NODE_FINISH 'R'

/*
 * Base class for syncup controller
 */
typedef struct SyncController {
    /* type of controller*/
    NodeTag controller_type;

    /* controller plan node id */
    int controller_plannodeid;

    /* control-node xc_node_id */
    int controlnode_xcnodeid;

    /* controler plan state desc */
    PlanState* controller_planstate;

    /* indicate the controller side is showdown */
    bool executor_stop;
} SyncController;

/*
 * SubClass inheriented from SyncController for Stream Operator
 */
typedef struct StreamController {
    SyncController controller;
    int iteration;
    bool iteration_finished;
    int total_tuples;
    bool stream_finish;
} StreamController;

typedef struct RecursiveVfd {
    /* Reference to Consumer thread's VfdCache' address */
    Vfd** recursive_VfdCache;

    /* Reference to Consumer thread's SizeVfdCache */
    Size* recursive_SizeVfdCache;
} RecursiveVfd;

/*
 * SubClass inheriented from SyncController for RecursiveUnion Operator
 */
typedef struct RecursiveUnionController {
    /* base controller information */
    SyncController controller;

    /* will move to base */
    int total_execution_datanodes;
    List* streamlist;
    StreamState* syncup_streamnode;
    int stepno;
    int ru_coordnodeid;

    /* RecursiveUnion specific control information */
    int recursiveunion_stepno;

    /* none-recursive desc */
    int* none_recursive_tuples;
    bool none_recursive_finished;
    int total_step1_rows;

    /* recursive desc */
    int* recursive_tuples;
    bool recursive_finished;
    int iteration;
    int total_step2_substep_rows;
    int total_step2_rows;

    /* overall desc */
    int total_step_rows;
    bool recursive_union_finish;

    RecursiveVfd recursive_vfd;
} RecursiveUnionController;

/*
 * ***********************************************************************************
 *  Synchronization functions for each controller
 * ***********************************************************************************
 */

/* For RecursoveUnion's Producer/Consumer functions */
extern void ExecSyncRecursiveUnionConsumer(RecursiveUnionController* controller, int stepno);
extern void ExecSyncRecursiveUnionProducer(RecursiveUnionController* controller, int producer_plannodeid, int step,
    int tuple_count, bool* need_rescan, int target_iteration);

/*
 * For Stream's Producer/Consumer functions
 */
extern void ExecSyncStreamProducer(StreamController* controller, bool* need_rescan, int target_iteration);
extern void ExecSyncStreamConsumer(StreamController* controller);

extern bool ExecutePlanSyncProducer(
    PlanState* planstate, int step, bool* recursive_early_stop, long* current_tuple_count);

/*
 * SyncController-Creator for each supported operator
 * SyncController-Destructor for each supported oeprator
 */
extern void ExecSyncControllerCreate(Plan* node);
extern void ExecSyncControllerDelete(SyncController* controller);

/*
 * Producer thread reset function for each operator
 */
extern void ExecSetStreamFinish(PlanState* state);

/*
 * working-horse marocs for sync-up relevant operations
 */
#define GET_PLAN_NODEID(x) ((Plan*)x)->plan_node_id
#define GET_RECURSIVE_UNION_PLAN_NODEID(x) ((Plan*)x)->recursive_union_plan_nodeid
#define GET_CONTROL_PLAN_NODEID(x) ((Plan*)x)->control_plan_nodeid

/*
 * Plan Step sync-up check functions
 * [1]. NeedSyncUpRecursiveUnionStep() invoked in ExecRecursiveUnion to set up sync-up point
 * [2]. NeedSetupSyncUpController() Invoked in init-node to set up controller
 * [3]. NeedSyncUpProducerStep() invoked in producer side to do sync-up point
 */
extern bool NeedSyncUpRecursiveUnionStep(Plan* plan);
extern bool NeedSetupSyncUpController(Plan* plan);
extern bool NeedSyncUpProducerStep(Plan* top_plan);

typedef struct TryStreamRecursivePlanContext {
    bool stream_plan;
} TryStreamRecursivePlanContext;

extern void mark_stream_recursiveunion_plan(
    RecursiveUnion* runode, Plan* node, bool recursive_branch, List* subplans, List** initplans);

/*
 * Helper functions to identify the thread or consumer node is the sync-up thread
 */
extern bool IsSyncUpProducerThread();
extern bool IsSyncUpConsumerStreamNode(const Stream* node);

/*
 * Iteration context structure to support distributed RecursiveUnion plan generation
 */
typedef struct RecursiveRefContext {
    /* field to store recursive union plan node */
    const RecursiveUnion* ru_plan;

    /*
     * store the current control plan node if current runing-plan need controlled when
     * iterating the plan tree.
     */
    Plan* control_plan;

    /* depth of current stream operation depths */
    int nested_stream_depth;

    /* indicate if we need set control_plan_id for current iterating plan node */
    bool set_control_plan_nodeid;

    /* flag to indicate whether the sync up producer is specified (for multi-stream case) */
    bool is_syncup_producer_specified;

    /* join type of most close upper layer join type */
    NodeTag join_type;

    List* initplans; /* Plans for initplan nodes */
    List* subplans;  /* Plans for SubPlan nodes */
} RecursiveRefContext;

/* Function to generate control plannode for recursvie-union's underlying plantree */
extern void set_recursive_cteplan_ref(Plan* node, RecursiveRefContext* context);

extern RecursiveUnionState* ExecInitRecursiveUnion(RecursiveUnion* node, EState* estate, int eflags);
extern TupleTableSlot* ExecRecursiveUnion(RecursiveUnionState* node);
extern void ExecEndRecursiveUnion(RecursiveUnionState* node);
extern void ExecReScanRecursiveUnion(RecursiveUnionState* node);
extern bool IsFirstLevelStreamStateNode(StreamState* node);
extern void ExecReScanRecursivePlanTree(PlanState* ps);

/*
 * @Function: recursive_union_sleep() **INLINE**
 *
 * @Brief: wrapper sleep() function for recursive union use, compare with regular
 *         pg_usleep(), we check interrupt as well
 *
 * @Input usec: time-unit(micro) that going to sleep
 *
 * @Return: void
 */
static inline void recursive_union_sleep(long usec)
{
    /* setup interrupt check */
    CHECK_FOR_INTERRUPTS();

    /* wait for given length */
    pg_usleep(usec);

    return;
}

/* default check interval for iteration finish */
#define CHECK_INTERVAL 500L

/* check interval macro */
#define WITH_RECURSIVE_SYNCPOINT_WAIT_INTERVAL                             \
    {                                                                      \
        recursive_union_sleep(CHECK_INTERVAL);                             \
                                                                           \
        /* Check if current thread is required to exit from top consumer*/ \
        if (u_sess->exec_cxt.executorStopFlag) {                         \
            break;                                                         \
        }                                                                  \
    }

#endif /* NODERECURSIVEUNION_H */
