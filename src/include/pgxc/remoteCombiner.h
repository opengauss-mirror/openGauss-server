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
 * -------------------------------------------------------------------------
 *
 * remoteCombiner.h
 *
 * IDENTIFICATION
 *	 src/include/pgxc/remoteCombiner.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef REMOTE_COMBINER_H
#define REMOTE_COMBINER_H

#include "locator.h"
#include "nodes/nodes.h"
#include "pgxcnode.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "optimizer/pgxcplan.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "tcop/utility.h"
#include "utils/lsyscache.h"
#include "utils/snapshot.h"
#include "utils/rowstore.h"

typedef enum {
    REQUEST_TYPE_NOT_DEFINED, /* not determined yet */
    REQUEST_TYPE_COMMAND,     /* OK or row count response */
    REQUEST_TYPE_QUERY,       /* Row description response */
    REQUEST_TYPE_COPY_IN,     /* Copy In response */
    REQUEST_TYPE_COPY_OUT,    /* Copy Out response */
    REQUEST_TYPE_COMMITING    /* Commiting response */
} RequestType;

/*
 * Type of requests associated to a remote COPY OUT
 */
typedef enum {
    REMOTE_COPY_NONE,      /* Not defined yet */
    REMOTE_COPY_STDOUT,    /* Send back to client */
    REMOTE_COPY_FILE,      /* Write in file */
    REMOTE_COPY_TUPLESTORE /* Store data in tuplestore */
} RemoteCopyType;

typedef struct NodeIdxInfo {
    Oid nodeoid;
    int nodeidx;
} NodeIdxInfo;

typedef enum {
    PBE_NONE = 0,      /* no pbe or for initialization */
    PBE_ON_ONE_NODE,   /* pbe running on one datanode for now */
    PBE_ON_MULTI_NODES /* pbe running on more than one datanode */
} PBERunStatus;

typedef struct ParallelFunctionState {
    char* sql_statement;
    ExecNodes* exec_nodes;
    TupleDesc tupdesc;
    Tuplestorestate* tupstore;
    int64 result;
    void* resultset;
    MemoryContext slotmxct;
    bool read_only;
    bool need_tran_block;
    /* if anyarray data received from remote nodes and need store in tuple, this flag shoud be true.
     * because remote nodes use anyarry_out transform anyarray to string, so transform string to
     * anyarray in local node is required */
    bool need_transform_anyarray;
    /* this flag is for execute direct on multi nodes, if set this to true, executer will only get active
     * nodes connections. */
    bool active_nodes_only;
} ParallelFunctionState;

typedef bool (*fetchTupleFun)(
    RemoteQueryState* combiner, TupleTableSlot* slot, ParallelFunctionState* parallelfunctionstate);

typedef struct RemoteQueryState {
    ScanState ss;                 /* its first field is NodeTag */
    int node_count;               /* total count of participating nodes */
    PGXCNodeHandle** connections; /* Datanode connections being combined */
    int conn_count;               /* count of active connections */
    int current_conn;             /* used to balance load when reading from connections */
    CombineType combine_type;     /* see CombineType enum */
    int command_complete_count;   /* count of received CommandComplete messages */
    RequestType request_type;     /* see RequestType enum */
    TupleDesc tuple_desc;         /* tuple descriptor to be referenced by emitted tuples */
    int description_count;        /* count of received RowDescription messages */
    int copy_in_count;            /* count of received CopyIn messages */
    int copy_out_count;           /* count of received CopyOut messages */
    int errorCode;                /* error code to send back to client */
    char* errorMessage;           /* error message to send back to client */
    char* errorDetail;            /* error detail to send back to client */
    char* errorContext;           /* error context to send back to client */
    char* hint;                   /* hint message to send back to client */
    char* query;                  /* query message to send back to client */
    int cursorpos;                /* cursor index into query string */
    bool is_fatal_error;          /* mark if it is a FATAL error */
    bool query_Done;              /* query has been sent down to Datanodes */
    RemoteDataRowData currentRow; /* next data ro to be wrapped into a tuple */
    RowStoreManager row_store;    /* buffer where rows are stored when connection
                             should be cleaned for reuse by other RemoteQuery*/
    CommitSeqNo maxCSN;
    bool hadrMainStandby;
    /*
     * To handle special case - if this RemoteQuery is feeding sorted data to
     * Sort plan and if the connection fetching data from the Datanode
     * is buffered. If EOF is reached on a connection it should be removed from
     * the array, but we need to know node number of the connection to find
     * messages in the buffer. So we store nodenum to that array if reach EOF
     * when buffering
     */
    int* tapenodes;
    RemoteCopyType remoteCopyType; /* Type of remote COPY operation */
    FILE* copy_file;               /* used if remoteCopyType == REMOTE_COPY_FILE */
    uint64 processed;              /* count of data rows when running CopyOut */
    /* cursor support */
    char* cursor;                        /* cursor name */
    char* update_cursor;                 /* throw this cursor current tuple can be updated */
    int cursor_count;                    /* total count of participating nodes */
    PGXCNodeHandle** cursor_connections; /* Datanode connections being combined */
    /* Support for parameters */
    char* paramval_data;  /* parameter data, format is like in BIND */
    int paramval_len;     /* length of parameter values data */
    Oid* rqs_param_types; /* Types of the remote params */
    int rqs_num_params;

    int eflags;          /* capability flags to pass to tuplestore */
    bool eof_underlying; /* reached end of underlying plan? */
    Tuplestorestate* tuplestorestate;
    CommandId rqs_cmd_id;     /* Cmd id to use in some special cases */
    uint64 rqs_processed;     /* Number of rows processed (only for DMLs) */
    uint64 rqs_cur_processed; /* Number of rows processed for currently processed DN */

    void* tuplesortstate; /* for merge tuplesort */
    void* batchsortstate; /* for merge batchsort */
    fetchTupleFun fetchTuple;
    bool need_more_data;
    RemoteErrorData remoteErrData;           /* error data from remote */
    bool need_error_check;                   /* if need check other errors? */
    int64 analyze_totalrowcnt[ANALYZEDELTA]; /* save total row count received from dn under analyzing. */
    int64 analyze_memsize[ANALYZEDELTA];     /* save processed row count from dn under analyzing. */
    int valid_command_complete_count;        /* count of valid complete count */

    RemoteQueryType position; /* the position where the RemoteQuery node will run. */
    bool* switch_connection;  /* mark if connection reused by other RemoteQuery */
    bool refresh_handles;
    NodeIdxInfo* nodeidxinfo;
    PBERunStatus pbe_run_status; /* record running status of a pbe remote query */

    bool resource_error;    /* true if error from the compute pool */
    char* previousNodeName; /* previous DataNode that rowcount is different from current DataNode */
    char* serializedPlan;   /* the serialized plan tree */
    ParallelFunctionState* parallel_function_state;
    bool has_stream_for_loop; /* has stream node in for loop sql which may cause hang. */
#ifdef USE_SPQ
    uint64 queryId;
    PGXCNodeHandle** spq_connections_info;
    pg_conn **nodeCons;
    spq_qc_ctx* qc_ctx;
#endif
} RemoteQueryState;

extern RemoteQueryState* CreateResponseCombiner(int node_count, CombineType combine_type);
extern RemoteQueryState* CreateResponseCombinerForBarrier(int nodeCount, CombineType combineType);
extern bool validate_combiner(RemoteQueryState* combiner);
extern void CloseCombiner(RemoteQueryState* combiner);
extern void CloseCombinerForBarrier(RemoteQueryState* combiner);
extern bool ValidateAndCloseCombiner(RemoteQueryState* combiner);

#endif
