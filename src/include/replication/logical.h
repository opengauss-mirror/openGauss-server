/* ---------------------------------------------------------------------------------------
 * 
 * logical.h
 *        openGauss logical decoding coordination
 *
 * Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/logical.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef LOGICAL_H
#define LOGICAL_H

#include "replication/slot.h"

#include "access/xlog.h"
#include "access/xlogreader.h"
#include "replication/output_plugin.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "nodes/pg_list.h"
#include "storage/proc.h"
#include "access/parallel_recovery/redo_item.h"

#include "nodes/parsenodes_common.h"
#include "nodes/replnodes.h"
#include "access/ustore/knl_utuple.h"
#include "replication/logical_queue.h"
#include "replication/parallel_reorderbuffer.h"

/* The number of extra digits displayed for floating-point values in logical decoding */
#define LOGICAL_DECODE_EXTRA_FLOAT_DIGITS 3

/* Maximum number of max parallel decode threads */
#define MAX_PARALLEL_DECODE_NUM 20

/* Maximum number of max replication slots */
#define MAX_REPLICATION_SLOT_NUM 100

/* FLag and mask for TOAST in parallel decoding */
#define TOAST_FLAG ((uint32)1 << 31)
#define TOAST_MASK (((uint32)1 << 31) - 1)

typedef void (*LogicalOutputPluginWriterWrite)(
    struct LogicalDecodingContext* lr, XLogRecPtr Ptr, TransactionId xid, bool last_write);
typedef LogicalOutputPluginWriterWrite LogicalOutputPluginWriterPrepareWrite;
typedef struct logicalLog logicalLog;

typedef struct LogicalDecodingContext {
    /* memory context this is all allocated in */
    MemoryContext context;

    /* infrastructure pieces */
    XLogReaderState* reader;
    ReplicationSlot* slot;
    struct ReorderBuffer* reorder;
    struct SnapBuild* snapshot_builder;
    /*
     * Marks the logical decoding context as fast forward decoding one.
     * Such a context does not have plugin loaded so most of the the following
     * properties are unused.
     */
    bool fast_forward;

    OutputPluginCallbacks callbacks;
    OutputPluginOptions options;

    /*
     * User specified options
     */
    List* output_plugin_options;

    /*
     * User-Provided callback for writing/streaming out data.
     */
    LogicalOutputPluginWriterPrepareWrite prepare_write;
    LogicalOutputPluginWriterWrite do_write;

    /*
     * Output buffer.
     */
    StringInfo out;

    /*
     * Private data pointer of the output plugin.
     */
    void* output_plugin_private;

    /*
     * Private data pointer for the data writer.
     */
    void* output_writer_private;

    /*
     * State for writing output.
     */
    bool accept_writes;
    bool prepared_write;
    XLogRecPtr write_location;
    TransactionId write_xid;

    bool random_mode;
} LogicalDecodingContext;

typedef struct chosenTable {
    char *schema; /* NULL means any schema */
    char *table; /* NULL means any table */
} chosenTable;

/* parallel decode callback signature */
typedef void(*ParallelDecodeChangeCB)(Relation relation, ParallelReorderBufferChange* change, logicalLog *logChange,
    ParallelLogicalDecodingContext* ctx, int slotId);

typedef struct {
    bool include_xids;
    bool include_timestamp;
    bool skip_empty_xacts;
    bool xact_wrote_changes;
    bool only_local;
    int max_txn_in_memory;
    int max_reorderbuffer_in_memory;
    char decode_style; /* 'j' stands for json while 't' stands for text */
    int parallel_decode_num;
    int sending_batch;
    ParallelDecodeChangeCB decode_change;
    List *tableWhiteList;
    int parallel_queue_size;
    bool include_originid;
} ParallelDecodeOption;

typedef struct {
    MemoryContext context;
    ParallelDecodeOption pOptions;
} ParallelDecodingData;

typedef struct {
    MemoryContext context;
    bool include_xids;
    bool include_timestamp;
    bool skip_empty_xacts;
    bool xact_wrote_changes;
    bool only_local;
    int max_txn_in_memory;
    int max_reorderbuffer_in_memory;
    List *tableWhiteList;
    bool include_originid;
} PluginTestDecodingData;

typedef struct ParallelLogicalDecodingContext {
    /* memory context this is all allocated in */
    MemoryContext context;

    /* infrastructure pieces */
    XLogReaderState* reader;
    ReplicationSlot* slot;
    ParallelReorderBuffer* reorder;
    /*
     * Marks the logical decoding context as fast forward decoding one.
     * Such a context does not have plugin loaded so most of the the following
     * properties are unused.
     */
    bool fast_forward;

    ParallelOutputPluginCallbacks callbacks;
    OutputPluginOptions options;

    /*
     * User specified options
     */
    List* output_plugin_options;

    /*
     * Output buffer.
     */
    StringInfo out;

    /*
     * Private data pointer of the output plugin.
     */
    void* output_plugin_private;

    /*
     * Private data pointer for the data writer.
     */
    void* output_writer_private;

    /*
     * State for writing output.
     */
    bool accept_writes;
    bool prepared_write;
    XLogRecPtr write_location;
    TransactionId write_xid;

    bool random_mode;
    bool isParallel;
    /*
     * Buffer for updating write_location
     */
    StringInfo writeLocationBuffer;
} ParallelLogicalDecodingContext;

typedef struct ParallelDecodeWorker {
    /* Worker id. */
    int id;
    /* Thread id */
    gs_thread_t tid;
    int slotId;
    /* To-be-replayed log-record-list queue. */
    LogicalQueue* changeQueue;
    LogicalQueue* LogicalLogQueue;
    logicalLog* freeGetLogicalLogHead;
    MemoryContext oldCtx;
    char dbUser[NAMEDATALEN];
    char dbName[NAMEDATALEN];
    char slotname[NAMEDATALEN];
    char decodeStyle;
} ParallelDecodeWorker;

typedef struct ParallelDecodeReaderWorker {
    /* Worker id. */
    uint32 id;
    /* Thread id */
    ThreadId tid;
    int slotId;
    char dbUser[NAMEDATALEN];
    char dbName[NAMEDATALEN];
    char slotname[NAMEDATALEN];
    StartReplicationCmd *cmd;
    
    XLogRecPtr current_lsn;
    XLogRecPtr restart_lsn;
    XLogRecPtr candidate_oldest_xmin_lsn;
    XLogRecPtr candidate_oldest_xmin;
    XLogRecPtr flushLSN;
    /* To-be-replayed log-record-list queue. */
    LogicalQueue* queue;
    MemoryContext oldCtx;
    ParallelDecodingData data;
    slock_t rwlock;
} ParallelDecodeReaderWorker;

typedef struct LogicalDispatcher {
    MemoryContext oldCtx;
    int decodeWorkerId;
    ParallelDecodeWorker** decodeWorkers; /* Array of parallel decode workers. */
    ParallelDecodeOption pOptions;

    int totalWorkerCount;      /* Number of parallel decode workers started. */
    ParallelDecodeReaderWorker* readWorker;     /* Txn reader worker. */
    ParallelReorderBufferChange* freeChangeHead;           /* Head of freed-item list. */
    ParallelReorderBufferChange* freeGetChangeHead;

    ReorderBufferTupleBuf* freeTupleHead;           /* Head of freed-item list. */
    ReorderBufferTupleBuf* freeGetTupleHead;

    logicalLog* freeLogicalLogHead;           /* Head of freed-item list. */
    char slotName[NAMEDATALEN];
    int32 pendingCount; /* Number of records pending. */
    int32 pendingMax;   /* The max. pending count per batch. */
    int exitCode;       /* Thread exit code. */
    uint64 totalCostTime;
    uint64 txnCostTime; /* txn cost time */
    uint64 pprCostTime;
    uint64 sentPtr;
    uint32 curChangeNum;
    uint32 curTupleNum;
    uint32 curLogNum;
    uint64 num;
    uint32* chosedWorkerIds;
    uint32 chosedWorkerCount;
    uint32 readyWorkerCnt;
    int id;
    TimestampTz decodeTime;
    bool remainPatch;
    bool checkpointNeedFullSync;
    bool active;
    bool firstLoop;
    bool abnormal;
    XLogRecPtr startpoint;
    int64 workingTxnCnt;
    int64 workingTxnMemory;
    struct ReplicationSlot* MyReplicationSlot;
} LogicalDispatcher;

#define QUEUE_RESULT_LEN 512
typedef struct ParallelStatusData {
    char slotName[NAMEDATALEN];
    int parallelDecodeNum;
    char readQueueLen[QUEUE_RESULT_LEN];
    char decodeQueueLen[QUEUE_RESULT_LEN];
    char readerLsn[MAXFNAMELEN];
    int64 workingTxnCnt;
    int64 workingTxnMemory;
} ParallelStatusData;

typedef struct DecodeOptionsDefault {
    int parallel_decode_num;
    int parallel_queue_size;
    int max_txn_in_memory;
    int max_reorderbuffer_in_memory;
} DecodeOptionsDefault;

extern LogicalDispatcher g_Logicaldispatcher[];
extern bool firstCreateDispatcher;
extern bool QuoteCheckOut(char* newval);
extern void CheckLogicalDecodingRequirements(Oid databaseId);
extern void ParallelReorderBufferQueueChange(ParallelReorderBuffer *rb, logicalLog *change, int slotId);
extern void ParallelReorderBufferForget(ParallelReorderBuffer *rb, int slotId, ParallelReorderBufferTXN *txn);
extern void ParallelReorderBufferCommit(ParallelReorderBuffer *rb, logicalLog *change, int slotId,
    ParallelReorderBufferTXN *txn);
extern LogicalDecodingContext* CreateInitDecodingContext(const char* plugin, List* output_plugin_options,
    bool need_full_snapshot, XLogPageReadCB read_page, LogicalOutputPluginWriterPrepareWrite prepare_write,
    LogicalOutputPluginWriterWrite do_write);
extern LogicalDecodingContext* CreateInitDecodingContextInternal(char* plugin, List* output_plugin_options,
    XLogPageReadCB read_page, LogicalOutputPluginWriterPrepareWrite prepare_write,
    LogicalOutputPluginWriterWrite do_write, bool set_xmin_horizon, ReplicationSlot* slot);
extern LogicalDecodingContext* CreateDecodingContext(XLogRecPtr start_lsn, List* output_plugin_options,
    bool fast_forward, XLogPageReadCB read_page, LogicalOutputPluginWriterPrepareWrite prepare_write,
    LogicalOutputPluginWriterWrite do_write);
extern LogicalDecodingContext* CreateDecodingContextForArea(XLogRecPtr start_lsn, const char* plugin,List* output_plugin_options,
    bool fast_forward, XLogPageReadCB read_page, LogicalOutputPluginWriterPrepareWrite prepare_write,
    LogicalOutputPluginWriterWrite do_write);
extern ParallelLogicalDecodingContext *ParallelCreateDecodingContext(XLogRecPtr start_lsn, List *output_plugin_options,
    bool fast_forward, XLogPageReadCB read_page, int slotId);

extern void DecodingContextFindStartpoint(LogicalDecodingContext* ctx);
extern bool DecodingContextReady(LogicalDecodingContext* ctx);
extern void FreeDecodingContext(LogicalDecodingContext* ctx);

extern void LogicalIncreaseXminForSlot(XLogRecPtr lsn, TransactionId xmin);
extern void LogicalIncreaseRestartDecodingForSlot(XLogRecPtr current_lsn, XLogRecPtr restart_lsn);
extern void LogicalConfirmReceivedLocation(XLogRecPtr lsn);
extern bool filter_by_origin_cb_wrapper(LogicalDecodingContext* ctx, RepOriginId origin_id);
extern void CloseLogicalAdvanceConnect();
extern void NotifyPrimaryAdvance(XLogRecPtr restart, XLogRecPtr flush);
extern void NotifyPrimaryCatalogXmin(TransactionId catalogXmin);
extern void ParallelDecodeWorkerMain(void* point);
extern void LogicalReadWorkerMain(void* point);
extern void ParseProcessRecord(ParallelLogicalDecodingContext *ctx, XLogReaderState *record, ParallelDecodeReaderWorker
    *worker);
extern void StartLogicalLogWorkers(char* dbUser, char* dbName, char* slotname, List *options, int parallelDecodeNum);
extern void CheckBooleanOption(DefElem *elem, bool *booleanOption, bool defaultValue);
extern void CheckIntOption(DefElem *elem, int *intOption, int defaultValue, int minVal, int maxVal);
extern int ParseParallelDecodeNumOnly(List *options);
extern bool CheckWhiteList(const List *whiteList, const char *schema, const char *table);
extern bool ParseStringToWhiteList(char *tableString, List **tableWhiteList);
extern void ParseWhiteList(List **whiteList, DefElem* elem);
extern void ParseDecodingOptionPlugin(ListCell* option, PluginTestDecodingData* data, OutputPluginOptions* opt);
extern ParallelStatusData *GetParallelDecodeStatus(uint32 *num);
extern void PrintLiteral(StringInfo s, Oid typid, char* outputstr);
extern void FreeLogicalLog(ParallelReorderBuffer *rb, logicalLog *logChange, int slotId, bool nocache);

extern bool LogicalDecodeParseOptionsDefault(const char* defaultStr, void **options);
extern DecodeOptionsDefault* LogicalDecodeGetOptionsDefault();
template <typename T> void LogicalDecodeReportLostChanges(const T *iterstate);
extern void tuple_to_stringinfo(Relation relation, StringInfo s, TupleDesc tupdesc, HeapTuple tuple, bool isOld,
    bool printOid = false);

#endif
