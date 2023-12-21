/* ---------------------------------------------------------------------------------------
 * 
 * stream_cost.h
 *        prototypes used to calculate stream plan costs.
 * 
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd. 
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *        src/include/optimizer/stream_cost.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef STREAM_COST_H
#define STREAM_COST_H

#include "catalog/pgxc_class.h"
#include "nodes/parsenodes.h"
#include "optimizer/pgxcplan.h"
#include "optimizer/pgxcship.h"
#include "optimizer/paths.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"

typedef enum {
    STREAM_GATHER = 0,
    STREAM_BROADCAST,
    STREAM_REDISTRIBUTE,
    STREAM_ROUNDROBIN,
    STREAM_HYBRID,
    STREAM_LOCAL,
    STREAM_NONE
} StreamType;

typedef enum {
    PARALLEL_NONE = 0,       /* Default value, means no parallelization. */
    REMOTE_DISTRIBUTE,       /* Distribute data to all nodes. */
    REMOTE_DIRECT_DISTRIBUTE,/* Distribute data to one specific nodes. */
    REMOTE_SPLIT_DISTRIBUTE, /* Distribute data to all parallel threads at all nodes. */
    REMOTE_BROADCAST,        /* Broadcast data to all nodes. */
    REMOTE_SPLIT_BROADCAST,  /* Broadcast data to all parallel threads all nodes. */
    REMOTE_HYBRID,           /* Hybrid send data. */
#ifdef USE_SPQ
    REMOTE_ROUNDROBIN,
    REMOTE_DML_WRITE_NODE,   /* DML only send datas to write node. */
#endif
    LOCAL_DISTRIBUTE,        /* Distribute data to all threads at local node. */
    LOCAL_BROADCAST,         /* Broadcast data to all threads at local node. */
    LOCAL_ROUNDROBIN         /* Roundrobin data to all threads at local node. */
} SmpStreamType;

typedef struct ParallelDesc {
    int consumerDop;          /* DOP of consumer side. */
    int producerDop;          /* DOP of producer side. */
    SmpStreamType distriType; /* Tag the data distribution type. */
} ParallelDesc;

/*
 * All stream-type paths share these fields.
 */
typedef struct StreamPath {
    Path path;
    StreamType type;
    Path* subpath;
    ParallelDesc* smpDesc; /* the parallel description of the stream. */
    Distribution consumer_distribution;
    List* skew_list;
} StreamPath;

typedef struct Stream {
    Scan scan;
    StreamType type;           /* indicate it's Redistribute/Broadcast. */
    char* plan_statement;
    ExecNodes* consumer_nodes; /* indicate nodes group that send to. */
    List* distribute_keys;
    bool is_sorted;            /* true if the underlying node is sorted, default false. */
    SimpleSort* sort;          /* do the merge sort if producer is sorted */
    bool is_dummy;             /* is dummy means the stream thread init by this stream node
                                * will end asap and init a child stream thread. */

    ParallelDesc smpDesc;      /* the parallel description of the stream. */
    char* jitted_serialize;    /* jitted serialize function for vecstream */
    List* skew_list;
    int stream_level;          /* number of stream level (starts from 1), normally
                                * used for recursive sql execution that under recursive-union operator. */
    ExecNodes* origin_consumer_nodes;
    bool is_recursive_local;   /* LOCAL GATHER for recursive */
#ifdef USE_SPQ
    int streamID;
#endif
} Stream;

extern void compute_stream_cost(StreamType type, char locator_type, double subrows, double subgblrows,
    double skew_ratio, int width, bool isJoin, List* distribute_keys, Cost* total_cost, double* gblrows,
    unsigned int producer_num_dn, unsigned int consumer_num_dn, ParallelDesc* smpDesc = NULL, List* ssinfo = NIL);
extern void cost_stream(StreamPath* stream, int width, bool isJoin = false);
extern List* get_max_cost_distkey_for_hasdistkey(PlannerInfo* root, List* subPlans, int subPlanNum,
    List** subPlanKeyArray, Cost* subPlanCostArray, Bitmapset** redistributePlanSetCopy);
extern List* get_max_cost_distkey_for_nulldistkey(
    PlannerInfo* root, List* subPlans, int subPlanNum, Cost* subPlanCostArray);
extern void parallel_stream_info_print(ParallelDesc* smpDesc, StreamType type);
extern List* make_distkey_for_append(PlannerInfo* root, Plan* subPlan);

#endif /* STREAM_COST_H */
