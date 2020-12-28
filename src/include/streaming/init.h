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
 * init.h
 *        Head file for streaming engine init.
 *
 *
 * IDENTIFICATION
 *        src/include/streaming/init.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_STREAMING_INIT_H_
#define SRC_INCLUDE_STREAMING_INIT_H_

#include "gs_thread.h"
#include "streaming/launcher.h"
#include "access/tupdesc.h"

typedef int StreamingThreadSeqNum;

typedef struct StreamingBatchStats {
    union {
        struct { // coordinator stats
            volatile uint64 worker_in_rows; /* worker input rows */
            volatile uint64 worker_in_bytes; /* worker input bytes */
            volatile uint64 worker_out_rows; /* worker output rows */
            volatile uint64 worker_out_bytes; /* worker output bytes */
            volatile uint64 worker_pending_times; /* worker pending times */
            volatile uint64 worker_error_times; /* worker error times */
        };
        struct { // datanode stats
            volatile uint64 router_in_rows; /* router input rows */
            volatile uint64 router_in_bytes; /* router input bytes */
            volatile uint64 router_out_rows; /* router output rows */
            volatile uint64 router_out_bytes; /* router output bytes */
            volatile uint64 router_error_times; /* router error times */
            volatile uint64 collector_in_rows; /* collector input rows */
            volatile uint64 collector_in_bytes; /* collector input bytes */
            volatile uint64 collector_out_rows; /* collector output rows */
            volatile uint64 collector_out_bytes; /* collector output bytes */
            volatile uint64 collector_pending_times; /* collector pending times */
            volatile uint64 collector_error_times; /* collector error times */
        };
    };
} StreamingBatchStats;

typedef struct StreamingSharedMetaData {
    volatile uint32 client_push_conn_atomic; /* round robin client push connection atomic */
    void *conn_hash_tbl; /* connection hash table for streaming threads */
    StreamingBatchStats *batch_stats; /* streaming engine microbatch statistics */
}StreamingSharedMetaData;

typedef struct StreamingThreadMetaData {
    ThreadId tid;
    knl_thread_role subrole;
    StreamingThreadSeqNum tseq;
}StreamingThreadMetaData;

typedef struct DictDesc
{
    char *nspname;
    char *relname;
    char *indname;
    Oid relid;
    Oid indrelid;
    TupleDesc desc;
    int nkeys;
    int key;
} DictDesc;

#define DICT_CACHE_SIZE 1024

typedef struct knl_t_streaming_context {
    volatile bool is_streaming_engine;
    volatile bool loaded;    /* streaming engine loaded flag */
    void *save_utility_hook;
    void *save_post_parse_analyze_hook;
    StreamingBackendServerLoopFunc streaming_backend_serverloop_hook;
    StreamingBackendShutdownFunc streaming_backend_shutdown_hook;
    void *streaming_planner_hook;
    volatile bool got_SIGHUP;
    volatile bool got_SIGTERM;
    int client_push_conn_id;
    StreamingThreadMetaData *thread_meta; /* streaming current thread meta */
    unsigned int streaming_context_flags;
    TransactionId cont_query_cache_xid;
    MemoryContext cont_query_cache_cxt;
    void *cont_query_cache;
    int current_cont_query_id;
    Oid streaming_exec_lock_oid;
    MemoryContext ContQueryTransactionContext;
    MemoryContext ContQueryBatchContext;

    HTAB *dict_htable[DICT_CACHE_SIZE];
    MemoryContext dict_context;
    bool dict_inited;
    DictDesc dictdesc[DICT_CACHE_SIZE];
} knl_t_streaming_context;

typedef struct knl_g_streaming_context {
    MemoryContext meta_cxt;    /* streaming engine meta context */
    MemoryContext conn_cxt;    /* streaming engine conn context */
    StreamingSharedMetaData *shared_meta; /* streaming shared meta */
    StreamingThreadMetaData *thread_metas; /* streaming thread metas */
    char *krb_server_keyfile; /* kerberos server keyfile */
    volatile bool got_SIGHUP; /* SIGHUP comm with nanomsg auth */
    bool enable;   /* streaming engine enable flag */
    int router_port; /* the port router thread listens on */
    int routers;   /* number of router threads */
    int workers;   /* number of worker threads */
    int combiners;   /* number of combiner threads */
    int queues;   /* number of queue threads */
    int reapers;   /* number of reaper threads */
    int batch_size; /* max number of tuples for streaming microbatch */
    int batch_mem; /* max size (KB) for streaming microbatch */
    int batch_wait; /* receive timeout (ms) for streaming microbatch */
    int flush_mem; /* max size (KB) for streaming disk flush */
    int flush_wait; /* receive timeout (ms) for streaming disk flush */
    volatile bool exec_lock_flag; /* get exec lock flag */
    int gather_window_interval; /* interval (min) of gather window */
}knl_g_streaming_context;

bool is_streaming_engine_available();
void validate_streaming_engine_status(Node *stmt);

#endif /* SRC_INCLUDE_STREAMING_INIT_H_ */
