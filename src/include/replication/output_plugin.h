/* ---------------------------------------------------------------------------------------
 * 
 * output_plugin.h
 *        openGauss Logical Decode Plugin Interface
 *
 * Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/output_plugin.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef OUTPUT_PLUGIN_H
#define OUTPUT_PLUGIN_H

#include "replication/reorderbuffer.h"
#include "replication/parallel_reorderbuffer.h"

struct LogicalDecodingContext;
struct OutputPluginCallbacks;

typedef enum OutputPluginOutputType {
    OUTPUT_PLUGIN_BINARY_OUTPUT,
    OUTPUT_PLUGIN_TEXTUAL_OUTPUT
} OutputPluginOutputType;

/*
 * Options set by the output plugin, in the startup callback.
 */
typedef struct OutputPluginOptions {
    OutputPluginOutputType output_type;
} OutputPluginOptions;

/*
 * Type of the shared library symbol _PG_output_plugin_init that is looked up
 * when loading an output plugin shared library.
 */
typedef void (*LogicalOutputPluginInit)(struct OutputPluginCallbacks* cb);
typedef void (*ParallelLogicalOutputPluginInit)(struct ParallelOutputPluginCallbacks* cb);

/*
 * Callback that gets called in a user-defined plugin. ctx->private_data can
 * be set to some private data.
 *
 * "is_init" will be set to "true" if the decoding slot just got defined. When
 * the same slot is used from there one, it will be "false".
 */
typedef void (*LogicalDecodeStartupCB)(struct LogicalDecodingContext* ctx, OutputPluginOptions* options, bool is_init);

/*
 * Callback called for every (explicit or implicit) BEGIN of a successful
 * transaction.
 */
typedef void (*LogicalDecodeBeginCB)(struct LogicalDecodingContext* ctx, ReorderBufferTXN* txn);

/*
 * Called for the logical decoding DDL message.
 */
typedef void (*LogicalDecodeDDLMessageCB) (struct LogicalDecodingContext *ctx,
                                            ReorderBufferTXN *txn,
                                            XLogRecPtr message_lsn,
                                            const char *prefix,
                                            Oid relid,
                                            DeparsedCommandType cmdtype,
                                            Size message_size,
                                            const char *message);

/*
 * Callback for every individual change in a successful transaction.
 */
typedef void (*LogicalDecodeChangeCB)(
    struct LogicalDecodingContext* ctx, ReorderBufferTXN* txn, Relation relation, ReorderBufferChange* change);

typedef void (*ParallelLogicalDecodeChangeCB)(
    struct ParallelLogicalDecodingContext* ctx, ReorderBufferTXN* txn, Relation relation, ParallelReorderBufferChange* change);

/*
 * Called for every (explicit or implicit) COMMIT of a successful transaction.
 */
typedef void (*LogicalDecodeCommitCB)(struct LogicalDecodingContext* ctx, ReorderBufferTXN* txn, XLogRecPtr commit_lsn);

/*
 * Called for every (explicit or implicit) COMMIT of a successful transaction.
 */
typedef void (*LogicalDecodeAbortCB)(struct LogicalDecodingContext* ctx, ReorderBufferTXN* txn);

/*
 * Called for every (explicit or implicit) COMMIT of a successful transaction.
 */
typedef void (*LogicalDecodePrepareCB)(struct LogicalDecodingContext* ctx, ReorderBufferTXN* txn);

/*
 * Called to shutdown an output plugin.
 */
typedef void (*LogicalDecodeShutdownCB)(struct LogicalDecodingContext* ctx);

/*
 * Filter changes by origin.
 */
typedef bool (*LogicalDecodeFilterByOriginCB)(struct LogicalDecodingContext* ctx, RepOriginId origin_id);

/*
 * Output plugin callbacks
 */
typedef struct OutputPluginCallbacks {
    LogicalDecodeStartupCB startup_cb;
    LogicalDecodeBeginCB begin_cb;
    LogicalDecodeChangeCB change_cb;
    LogicalDecodeCommitCB commit_cb;
    LogicalDecodeAbortCB abort_cb;
    LogicalDecodePrepareCB prepare_cb;
    LogicalDecodeShutdownCB shutdown_cb;
    LogicalDecodeFilterByOriginCB filter_by_origin_cb;
    LogicalDecodeDDLMessageCB ddl_cb;
} OutputPluginCallbacks;

typedef struct ParallelOutputPluginCallbacks {
    LogicalDecodeStartupCB startup_cb;
    LogicalDecodeBeginCB begin_cb;
    ParallelLogicalDecodeChangeCB change_cb;
    LogicalDecodeCommitCB commit_cb;
    LogicalDecodeShutdownCB shutdown_cb;
    LogicalDecodeFilterByOriginCB filter_by_origin_cb;
} ParallelOutputPluginCallbacks;

extern void OutputPluginPrepareWrite(struct LogicalDecodingContext* ctx, bool last_write);
extern void OutputPluginWrite(struct LogicalDecodingContext* ctx, bool last_write);

#endif /* OUTPUT_PLUGIN_H */
