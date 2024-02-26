/*
 *
 * pgoutput.cpp
 * 		Logical Replication output plugin
 *
 * Portions Copyright (c) 2021 Huawei Technologies Co.,Ltd.
 * Copyright (c) 2012-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 * 		  src/gausskernel/storage/replication/pgoutput/pgoutput.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_publication.h"

#include "commands/defrem.h"

#include "replication/logical.h"
#include "replication/logicalproto.h"
#include "replication/origin.h"
#include "replication/pgoutput.h"
#include "commands/publicationcmds.h"

#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

static const int DEFAULT_REL_SYNC_CACHE_HASH_ELEM  = 128;

PG_MODULE_MAGIC;

extern "C" void _PG_output_plugin_init(OutputPluginCallbacks *cb);

static void pgoutput_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init);
static void pgoutput_shutdown(LogicalDecodingContext *ctx);
static void pgoutput_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn);
static void pgoutput_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void pgoutput_abort_txn(LogicalDecodingContext* ctx, ReorderBufferTXN* txn);
static void pgoutput_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation rel,
    ReorderBufferChange *change);
static void pgoutput_ddl(LogicalDecodingContext *ctx,
                         ReorderBufferTXN *txn, XLogRecPtr message_lsn,
                         const char *prefix, Oid relid,
                         DeparsedCommandType cmdtype,
                         Size sz, const char *message);

static bool pgoutput_origin_filter(LogicalDecodingContext *ctx, RepOriginId origin_id);

static List *LoadPublications(List *pubnames);
static void publication_invalidation_cb(Datum arg, int cacheid, uint32 hashvalue);
static bool ReplconninfoChanged();
static bool GetConninfo(StringInfoData* standbysInfo);

typedef struct PGOutputTxnData
{
    bool sent_begin_txn;    /* flag indicating where BEGIN has been set */
    List *deleted_relids;   /* maintain list of deleted table oids */
} PGOutputTxnData;

/* Entry in the map used to remember which relation schemas we sent. */
typedef struct RelationSyncEntry {
    Oid relid;        /* relation oid */
    bool schema_sent; /* did we send the schema? */
    bool replicate_valid;
    PublicationActions pubactions;
} RelationSyncEntry;

static void init_rel_sync_cache(MemoryContext decoding_context);
static bool NeedPublicationDDL(PGOutputData *data, bool needall);
static RelationSyncEntry *get_rel_sync_entry(PGOutputData *data, Oid relid);
static void rel_sync_cache_relation_cb(Datum arg, Oid relid);
static void rel_sync_cache_publication_cb(Datum arg, int cacheid, uint32 hashvalue);

/*
 * Specify output plugin callbacks
 */
void _PG_output_plugin_init(OutputPluginCallbacks *cb)
{
    AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

    cb->startup_cb = pgoutput_startup;
    cb->begin_cb = pgoutput_begin_txn;
    cb->change_cb = pgoutput_change;
    cb->commit_cb = pgoutput_commit_txn;
    cb->abort_cb = pgoutput_abort_txn;
    cb->filter_by_origin_cb = pgoutput_origin_filter;
    cb->shutdown_cb = pgoutput_shutdown;
    cb->ddl_cb = pgoutput_ddl;
}

static void parse_output_parameters(List *options, PGOutputData *data)
{
    ListCell *lc;
    bool protocol_version_given = false;
    bool publication_names_given = false;
    bool binary_option_given = false;
    bool use_snapshot_given = false;

    data->binary = false;

    foreach (lc, options) {
        DefElem *defel = (DefElem *)lfirst(lc);

        Assert(defel->arg == NULL || IsA(defel->arg, String));

        /* Check each param, whether or not we recognise it */
        if (strcmp(defel->defname, "proto_version") == 0) {
            int64 parsed;

            if (protocol_version_given)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            protocol_version_given = true;

            if (!scanint8(strVal(defel->arg), true, &parsed))
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid proto_version")));

            if (parsed > PG_UINT32_MAX || parsed < 0)
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("proto_version \"%s\" out of range", strVal(defel->arg))));

            data->protocol_version = (uint32)parsed;
        } else if (strcmp(defel->defname, "publication_names") == 0) {
            if (publication_names_given)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            publication_names_given = true;

            if (!SplitIdentifierString(strVal(defel->arg), ',', &(data->publication_names)))
                ereport(ERROR, (errcode(ERRCODE_INVALID_NAME), errmsg("invalid publication_names syntax")));
        } else if (strcmp(defel->defname, "binary") == 0) {
            if (binary_option_given)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            binary_option_given = true;

            data->binary = defGetBoolean(defel);
        } else if (strcmp(defel->defname, "usesnapshot") == 0) {
            if (use_snapshot_given)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            use_snapshot_given = true;

            t_thrd.walsender_cxt.isUseSnapshot = true;
        } else
            elog(ERROR, "unrecognized pgoutput option: %s", defel->defname);
    }
}

/*
 * Initialize this plugin
 */
static void pgoutput_startup(LogicalDecodingContext *ctx, OutputPluginOptions *opt, bool is_init)
{
    PGOutputData *data = (PGOutputData *)palloc0(sizeof(PGOutputData));

    /* Create our memory context for private allocations. */
    data->common.context = AllocSetContextCreate(ctx->context,
        "logical replication output context", ALLOCSET_DEFAULT_SIZES);

    ctx->output_plugin_private = data;

    /* This plugin uses binary protocol. */
    opt->output_type = OUTPUT_PLUGIN_BINARY_OUTPUT;

    /*
     * This is replication start and not slot initialization.
     *
     * Parse and validate options passed by the client.
     */
    if (!is_init) {
        /* Parse the params and ERROR if we see any we don't recognise */
        parse_output_parameters(ctx->output_plugin_options, data);

        /* Check if we support requested protol */
        if (data->protocol_version >= LOGICALREP_PROTO_MAX_VERSION_NUM)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("client sent proto_version=%d but we only support protocol %d or lower", data->protocol_version,
                LOGICALREP_PROTO_MAX_VERSION_NUM - 1)));

        if (data->protocol_version <= LOGICALREP_PROTO_MIN_VERSION_NUM)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("client sent proto_version=%d but we only support protocol %d or higher", data->protocol_version,
                LOGICALREP_PROTO_MIN_VERSION_NUM + 1)));

        if (list_length(data->publication_names) < 1)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("publication_names parameter missing")));

        /* Init publication state. */
        data->publications = NIL;
        t_thrd.publication_cxt.publications_valid = false;
        if (data->protocol_version >= LOGICALREP_CONNINFO_PROTO_VERSION_NUM) {
            t_thrd.publication_cxt.updateConninfoNeeded = true;
        }
        t_thrd.publication_cxt.firstTimeSendConninfo = true;
        CacheRegisterThreadSyscacheCallback(PUBLICATIONOID, publication_invalidation_cb, (Datum)0);

        /* Initialize relation schema cache. */
        init_rel_sync_cache(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));
    }
}

/* Initialize the per-transaction private data for the given transaction. */
static void
init_txn_data(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
    PGOutputTxnData *txndata;

    if (txn->output_plugin_private != NULL)
        return;

    txndata = (PGOutputTxnData*)MemoryContextAllocZero(ctx->context, sizeof(PGOutputTxnData));

    txn->output_plugin_private = txndata;
}

/* Clean up the per-transaction private data for the given transaction. */
static void
clean_txn_data(ReorderBufferTXN *txn)
{
    PGOutputTxnData *txndata = (PGOutputTxnData *) txn->output_plugin_private;

    if (txndata == NULL)
        return;

    list_free(txndata->deleted_relids);
    pfree(txndata);
    txn->output_plugin_private = NULL;
}

/*
 * BEGIN callback
 */
static void pgoutput_begin_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
    bool send_replication_origin = txn->origin_id != InvalidRepOriginId;
    init_txn_data(ctx, txn);

    OutputPluginPrepareWrite(ctx, !send_replication_origin);
    logicalrep_write_begin(ctx->out, txn);

    if (send_replication_origin) {
        char *origin = NULL;

        /*
         * XXX: which behaviour we want here?
         *
         * Alternatives:
         * - don't send origin message if origin name not found
         * (that's what we do now)
         * - throw error - that will break replication, not good
         * - send some special "unknown" origin
         */
        if (replorigin_by_oid(txn->origin_id, true, &origin)) {
            /*
             * Message boundary, we should do this when we sure we will send 'O' message.
             * Otherwise the apply worker will receive a message with header only, which
             * will lead to 'no data left in message' error.
             */
            OutputPluginWrite(ctx, false);
            OutputPluginPrepareWrite(ctx, true);
            logicalrep_write_origin(ctx->out, origin, txn->origin_lsn);
        }
    }

    OutputPluginWrite(ctx, true);
}

/*
 * COMMIT callback
 */
static void pgoutput_commit_txn(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, XLogRecPtr commit_lsn)
{
    clean_txn_data(txn);
    OutputPluginPrepareWrite(ctx, true);
    logicalrep_write_commit(ctx->out, txn, commit_lsn);
    OutputPluginWrite(ctx, true);

    /*
     * Send the newest connection information to the subscriber,
     * when the connection information about the standby changes.
     */
    if ((t_thrd.publication_cxt.updateConninfoNeeded && ReplconninfoChanged()) ||
        t_thrd.publication_cxt.firstTimeSendConninfo) {
        StringInfoData standbysInfo;
        initStringInfo(&standbysInfo);

        /* If there is no standby, don't need to send connection info to subscriber */
        if (GetConninfo(&standbysInfo)) {
            OutputPluginPrepareWrite(ctx, true);
            logicalrep_write_conninfo(ctx->out, standbysInfo.data);
            OutputPluginWrite(ctx, true);
        }

        FreeStringInfo(&standbysInfo);
        t_thrd.publication_cxt.firstTimeSendConninfo = false;
    }
}

/*
 * ABORT callback
 */
static void pgoutput_abort_txn(LogicalDecodingContext* ctx, ReorderBufferTXN* txn)
{
    /* Should not happen */
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("abort transaction not currently supported")));
}

/* Check whether the buffer change type is supported, return true if supported */
static inline bool CheckAction(ReorderBufferChangeType type, PublicationActions pubAction)
{
    switch (type) {
        case REORDER_BUFFER_CHANGE_INSERT:
        case REORDER_BUFFER_CHANGE_UINSERT:
            return pubAction.pubinsert;
        case REORDER_BUFFER_CHANGE_UPDATE:
        case REORDER_BUFFER_CHANGE_UUPDATE:
            return pubAction.pubupdate;
        case REORDER_BUFFER_CHANGE_DELETE:
        case REORDER_BUFFER_CHANGE_UDELETE:
            return pubAction.pubdelete;
        default:
            Assert(false);
    }
    return true;
}

static void MaybeSendSchema(LogicalDecodingContext *ctx, Relation relation, RelationSyncEntry *relentry)
{
    if (relentry->schema_sent) {
        return;
    }

    int i;
    TupleDesc desc = RelationGetDescr(relation);

    /*
     * Write out type info if needed. We do that only for user created
     * types.
     */
    for (i = 0; i < desc->natts; i++) {
        Form_pg_attribute att = &desc->attrs[i];

        if (att->attisdropped || GetGeneratedCol(desc, i))
            continue;

        if (att->atttypid < FirstNormalObjectId)
            continue;

        OutputPluginPrepareWrite(ctx, false);
        logicalrep_write_typ(ctx->out, att->atttypid);
        OutputPluginWrite(ctx, false);
    }

    OutputPluginPrepareWrite(ctx, false);
    logicalrep_write_rel(ctx->out, relation);
    OutputPluginWrite(ctx, false);
    relentry->schema_sent = true;
}

/*
 * Sends the decoded DML over wire.
 */
static void pgoutput_change(LogicalDecodingContext *ctx, ReorderBufferTXN *txn, Relation relation,
    ReorderBufferChange *change)
{
    PGOutputData *data = (PGOutputData *)ctx->output_plugin_private;
    MemoryContext old;
    RelationSyncEntry *relentry;

    if (!is_publishable_relation(relation))
        return;

    relentry = get_rel_sync_entry(data, RelationGetRelid(relation));
    /* First check the table filter */
    if (!CheckAction(change->action, relentry->pubactions)) {
        return;
    }

    /* Avoid leaking memory by using and resetting our own context */
    old = MemoryContextSwitchTo(data->common.context);

    /*
     * Write the relation schema if the current schema haven't been sent yet.
     */
    MaybeSendSchema(ctx, relation, relentry);

    /* Send the data */
    switch (change->action) {
        case REORDER_BUFFER_CHANGE_INSERT:
            if (change->data.tp.newtuple != NULL) {
                OutputPluginPrepareWrite(ctx, true);
                logicalrep_write_insert(ctx->out, relation, &change->data.tp.newtuple->tuple, data->binary);
                OutputPluginWrite(ctx, true);
            }
            break;
        case REORDER_BUFFER_CHANGE_UINSERT:
            if (change->data.utp.newtuple != NULL) {
                OutputPluginPrepareWrite(ctx, true);
                logicalrep_write_insert(ctx->out, relation,
                    (HeapTuple)(&change->data.utp.newtuple->tuple), data->binary);
                OutputPluginWrite(ctx, true);
            }
            break;
        case REORDER_BUFFER_CHANGE_UPDATE: {
            if (change->data.tp.newtuple != NULL) {
                HeapTuple oldtuple = change->data.tp.oldtuple ? &change->data.tp.oldtuple->tuple : NULL;

                OutputPluginPrepareWrite(ctx, true);
                logicalrep_write_update(ctx->out, relation, oldtuple, &change->data.tp.newtuple->tuple, data->binary);
                OutputPluginWrite(ctx, true);
            }
            break;
        }
        case REORDER_BUFFER_CHANGE_UUPDATE: {
            if (change->data.utp.newtuple != NULL) {
                HeapTuple oldtuple = change->data.utp.oldtuple ?
                    ((HeapTuple)(&change->data.utp.oldtuple->tuple)) : NULL;

                OutputPluginPrepareWrite(ctx, true);
                logicalrep_write_update(ctx->out, relation, oldtuple, (HeapTuple)(&change->data.utp.newtuple->tuple),
                    data->binary);
                OutputPluginWrite(ctx, true);
            }
            break;
        }
        case REORDER_BUFFER_CHANGE_DELETE:
            if (change->data.tp.oldtuple) {
                OutputPluginPrepareWrite(ctx, true);
                logicalrep_write_delete(ctx->out, relation, &change->data.tp.oldtuple->tuple, data->binary);
                OutputPluginWrite(ctx, true);
            } else
                elog(DEBUG1, "didn't send DELETE change because of missing oldtuple");
            break;
        case REORDER_BUFFER_CHANGE_UDELETE:
            if (change->data.utp.oldtuple) {
                OutputPluginPrepareWrite(ctx, true);
                logicalrep_write_delete(ctx->out, relation, (HeapTuple)(&change->data.utp.oldtuple->tuple), data->binary);
                OutputPluginWrite(ctx, true);
            } else
                elog(DEBUG1, "didn't send DELETE change because of missing oldtuple");
            break;
        default:
            Assert(false);
    }

    /* Cleanup */
    MemoryContextSwitchTo(old);
    MemoryContextReset(data->common.context);
}


/* Check if the given object is published. */
static bool
is_object_published_ddl(LogicalDecodingContext *ctx, Oid objid)
{
    RelationSyncEntry *relentry;
    PGOutputData *data = (PGOutputData *) ctx->output_plugin_private;

    /* First check the DDL command filter. */
    switch (get_rel_relkind(objid)) {
        case RELKIND_RELATION:
            relentry = get_rel_sync_entry(data, objid);

            /* Only send this ddl if we publish ddl message. */
            if (ENABLE_PUBDDL_TYPE(relentry->pubactions.pubddl, PUBDDL_TABLE))
                return true;

            break;

        case RELKIND_SEQUENCE:
            /* Send sequences */
            return NeedPublicationDDL(data, false);
        default:
            /* unsupported objects */
            return NeedPublicationDDL(data, true);
    }

    return false;
}

/*
 * Send the decoded DDL message.
 */
static void
pgoutput_ddl(LogicalDecodingContext *ctx, ReorderBufferTXN *txn,
             XLogRecPtr message_lsn,
             const char *prefix, Oid relid, DeparsedCommandType cmdtype,
             Size sz, const char *message)
{
    PGOutputTxnData *txndata = (PGOutputTxnData *) txn->output_plugin_private;

    /*
     * Check if the given object is published. Note that for dropped objects,
     * we cannot get the required information from the catalog, so we skip the
     * check for them.
     */
    if (cmdtype != DCT_TableDropEnd && !is_object_published_ddl(ctx, relid)) {
        return;
    }

    switch (cmdtype) {
        case DCT_TableDropStart: {
            MemoryContext oldctx;

            init_txn_data(ctx, txn);

            txndata = (PGOutputTxnData *) txn->output_plugin_private;

            /*
             * On DROP start, add the relid to a deleted_relid list if the
             * relid is part of a publication that supports ddl
             * publication. We need this because on DROP end, the relid
             * will no longer be valid. Later on Drop end, verify that the
             * drop is for a relid that is on the deleted_rid list, and
             * only then send the ddl message.
             */
            oldctx = MemoryContextSwitchTo(ctx->context);
            txndata->deleted_relids = lappend_oid(txndata->deleted_relids,
                                                    relid);
            MemoryContextSwitchTo(oldctx);     
        }
            return;
        case DCT_TableDropEnd:
            if (!list_member_oid(txndata->deleted_relids, relid))
                return;

            txndata->deleted_relids = list_delete_oid(txndata->deleted_relids,
                                                      relid);
            break;

        case DCT_SimpleCmd:
        case DCT_ObjectDrop:
        case DCT_ObjectCreate:
            /* do nothing */
            break;
        
        default:
            elog(ERROR, "unsupported type %d", cmdtype);
            break;
    }

    /* Send BEGIN if we haven't yet */
    pgoutput_begin_txn(ctx, txn);

    OutputPluginPrepareWrite(ctx, true);
    logicalrep_write_ddl(ctx->out, message_lsn, prefix, sz, message);
    OutputPluginWrite(ctx, true);
}

/*
 * Currently we always forward.
 */
static bool pgoutput_origin_filter(LogicalDecodingContext *ctx, RepOriginId origin_id)
{
    if (origin_id != InvalidRepOriginId) {
        return true;
    }
    return false;
}

/*
 * Shutdown the output plugin.
 *
 * Note, we don't need to clean the data->context as it's child context
 * of the ctx->context so it will be cleaned up by logical decoding machinery.
 */
static void pgoutput_shutdown(LogicalDecodingContext *ctx)
{
    if (t_thrd.publication_cxt.RelationSyncCache) {
        hash_destroy(t_thrd.publication_cxt.RelationSyncCache);
        t_thrd.publication_cxt.RelationSyncCache = NULL;
    }
}

/*
 * Load publications from the list of publication names.
 */
static List *LoadPublications(List *pubnames)
{
    List *result = NIL;
    ListCell *lc;

    foreach (lc, pubnames) {
        char *pubname = (char *)lfirst(lc);
        Publication *pub = GetPublicationByName(pubname, false);

        result = lappend(result, pub);
    }

    return result;
}

/*
 * Publication cache invalidation callback.
 */
static void publication_invalidation_cb(Datum arg, int cacheid, uint32 hashvalue)
{
    t_thrd.publication_cxt.publications_valid = false;

    /*
     * Also invalidate per-relation cache so that next time the filtering
     * info is checked it will be updated with the new publication
     * settings.
     */
    rel_sync_cache_publication_cb(arg, cacheid, hashvalue);
}

/*
 * Initialize the relation schema sync cache for a decoding session.
 *
 * The hash table is destroyed at the end of a decoding session. While
 * relcache invalidations still exist and will still be invoked, they
 * will just see the null hash table global and take no action.
 */
static void init_rel_sync_cache(MemoryContext cachectx)
{
    HASHCTL ctl;
    MemoryContext old_ctxt;
    int rc;

    if (t_thrd.publication_cxt.RelationSyncCache != NULL)
        return;

    /* Make a new hash table for the cache */
    rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
    securec_check(rc, "", "");
    ctl.keysize = sizeof(Oid);
    ctl.entrysize = sizeof(RelationSyncEntry);
    ctl.hcxt = cachectx;

    old_ctxt = MemoryContextSwitchTo(cachectx);
    t_thrd.publication_cxt.RelationSyncCache = hash_create("logical replication output relation cache",
        DEFAULT_REL_SYNC_CACHE_HASH_ELEM, &ctl, HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
    (void)MemoryContextSwitchTo(old_ctxt);

    Assert(t_thrd.publication_cxt.RelationSyncCache != NULL);

    CacheRegisterThreadRelcacheCallback(rel_sync_cache_relation_cb, (Datum)0);
    CacheRegisterThreadSyscacheCallback(PUBLICATIONRELMAP, rel_sync_cache_publication_cb, (Datum)0);
}

static bool NeedPublicationDDL(PGOutputData *data, bool needall)
{
    ListCell *lc;

    /* Reload publications if needed before use. */
    if (!t_thrd.publication_cxt.publications_valid) {
        MemoryContext oldctx = MemoryContextSwitchTo(u_sess->cache_mem_cxt);
        if (data->publications)
            list_free_deep(data->publications);

        data->publications = LoadPublications(data->publication_names);
        MemoryContextSwitchTo(oldctx);
        t_thrd.publication_cxt.publications_valid = true;
    }

    foreach (lc, data->publications) {
        Publication *pub = (Publication*)lfirst(lc);
        if (pub->pubactions.pubddl == PUBDDL_ALL) {
            return true;
        } else if (!needall && ENABLE_PUBDDL_TYPE(pub->pubactions.pubddl, PUBDDL_TABLE)) {
            return true;
        }
    }
    return false;
}

static void RefreshRelationEntry(RelationSyncEntry *entry, PGOutputData *data, Oid relid)
{
    List *pubids = GetRelationPublications(relid);
    ListCell *lc;

    /* Reload publications if needed before use. */
    if (!t_thrd.publication_cxt.publications_valid) {
        MemoryContext oldctx = MemoryContextSwitchTo(u_sess->cache_mem_cxt);
        if (data->publications)
            list_free_deep(data->publications);

        data->publications = LoadPublications(data->publication_names);
        MemoryContextSwitchTo(oldctx);
        t_thrd.publication_cxt.publications_valid = true;
    }

    /*
     * Build publication cache. We can't use one provided by relcache
     * as relcache considers all publications given relation is in, but
     * here we only need to consider ones that the subscriber requested.
     */
    entry->pubactions.pubinsert = false;
    entry->pubactions.pubupdate = false;
    entry->pubactions.pubdelete = false;
    entry->pubactions.pubddl = 0;

    foreach (lc, data->publications) {
        Publication *pub = (Publication *)lfirst(lc);

        /*
         * Skip tables that look like they are from a heap rewrite (see
         * make_new_heap()).  We need to skip them because the subscriber
         * won't have a table by that name to receive the data.  That
         * means we won't ship the new data in, say, an added column with
         * a DEFAULT, but if the user applies the same DDL manually on the
         * subscriber, then this will work out for them.
         *
         * We only need to consider the alltables case, because such a
         * transient heap won't be an explicit member of a publication.
         */
        if (pub->alltables) {
            char *relname = get_rel_name(relid);
            unsigned int u;
            int n;

            if (relname != NULL && sscanf_s(relname, "pg_temp_%u%n", &u, &n) == 1 && relname[n] == '\0' &&
                get_rel_relkind(u) == RELKIND_RELATION) {
                break;
            }
        }

        if (pub->alltables || list_member_oid(pubids, pub->oid)) {
            entry->pubactions.pubinsert |= pub->pubactions.pubinsert;
            entry->pubactions.pubupdate |= pub->pubactions.pubupdate;
            entry->pubactions.pubdelete |= pub->pubactions.pubdelete;
            if (entry->pubactions.pubddl != PUBDDL_ALL) {
                entry->pubactions.pubddl |= pub->pubactions.pubddl;
            }
        }

        if (entry->pubactions.pubinsert && entry->pubactions.pubupdate && entry->pubactions.pubdelete &&
            pub->pubactions.pubddl == PUBDDL_ALL)
            break;
    }

    list_free_ext(pubids);

    entry->replicate_valid = true;
}

/*
 * Find or create entry in the relation schema cache.
 */
static RelationSyncEntry *get_rel_sync_entry(PGOutputData *data, Oid relid)
{
    RelationSyncEntry *entry;
    bool found;
    MemoryContext oldctx;

    Assert(t_thrd.publication_cxt.RelationSyncCache != NULL);

    /* Find cached function info, creating if not found */
    oldctx = MemoryContextSwitchTo(u_sess->cache_mem_cxt);
    entry =
        (RelationSyncEntry *)hash_search(t_thrd.publication_cxt.RelationSyncCache, (void *)&relid, HASH_ENTER, &found);
    MemoryContextSwitchTo(oldctx);
    Assert(entry != NULL);

    /* Not found means schema wasn't sent */
    if (!found || !entry->replicate_valid) {
        RefreshRelationEntry(entry, data, relid);
    }

    if (!found) {
        entry->schema_sent = false;
    }

    return entry;
}

/*
 * Relcache invalidation callback
 */
static void rel_sync_cache_relation_cb(Datum arg, Oid relid)
{
    RelationSyncEntry *entry;

    /*
     * We can get here if the plugin was used in SQL interface as the
     * RelSchemaSyncCache is detroyed when the decoding finishes, but there
     * is no way to unregister the relcache invalidation callback.
     */
    if (t_thrd.publication_cxt.RelationSyncCache == NULL)
        return;

    /*
     * Nobody keeps pointers to entries in this hash table around outside
     * logical decoding callback calls - but invalidation events can come in
     * *during* a callback if we access the relcache in the callback. Because
     * of that we must mark the cache entry as invalid but not remove it from
     * the hash while it could still be referenced, then prune it at a later
     * safe point.
     *
     * Getting invalidations for relations that aren't in the table is
     * entirely normal, since there's no way to unregister for an
     * invalidation event. So we don't care if it's found or not.
     */
    entry = (RelationSyncEntry *)hash_search(t_thrd.publication_cxt.RelationSyncCache, &relid, HASH_FIND, NULL);
    /*
     * Reset schema sent status as the relation definition may have
     * changed.
     */
    if (entry != NULL) {
        entry->schema_sent = false;
    }
}

/*
 * Publication relation map syscache invalidation callback
 */
static void rel_sync_cache_publication_cb(Datum arg, int cacheid, uint32 hashvalue)
{
    HASH_SEQ_STATUS status;
    RelationSyncEntry *entry;

    /*
     * We can get here if the plugin was used in SQL interface as the
     * RelSchemaSyncCache is detroyed when the decoding finishes, but there
     * is no way to unregister the relcache invalidation callback.
     */
    if (t_thrd.publication_cxt.RelationSyncCache == NULL)
        return;

    /*
     * There is no way to find which entry in our cache the hash belongs to
     * so mark the whole cache as invalid.
     */
    hash_seq_init(&status, t_thrd.publication_cxt.RelationSyncCache);
    while ((entry = (RelationSyncEntry *)hash_seq_search(&status)) != NULL) {
        entry->replicate_valid = false;
        /*
         * There might be some relations dropped from the publication so we
         * don't need to publish the changes for them.
         */
        entry->pubactions.pubinsert = false;
        entry->pubactions.pubupdate = false;
        entry->pubactions.pubdelete = false;
    }
}

/* Get all primary and standby connection info. If there is no standby, return false, otherwise return true. */
static bool GetConninfo(StringInfoData* standbysInfo)
{
    bool primaryJoined = false;
    StringInfoData hosts;
    StringInfoData ports;
    initStringInfo(&hosts);
    initStringInfo(&ports);
    for (int i = 1; i < MAX_REPLNODE_NUM + 1; ++i) {
        t_thrd.postmaster_cxt.ReplConnChangeType[i] = 0;
        if (t_thrd.postmaster_cxt.ReplConnArray[i] == NULL) {
            continue;
        }
        if (!primaryJoined) {
            appendStringInfo(&hosts, "%s,%s",
                t_thrd.postmaster_cxt.ReplConnArray[i]->localhost,
                t_thrd.postmaster_cxt.ReplConnArray[i]->remotehost);
            appendStringInfo(&ports, "%d,%d",
                t_thrd.postmaster_cxt.ReplConnArray[i]->localport,
                t_thrd.postmaster_cxt.ReplConnArray[i]->remoteport);
            primaryJoined = true;
        } else {
            appendStringInfo(&hosts, ",%s",
                t_thrd.postmaster_cxt.ReplConnArray[i]->remotehost);
            appendStringInfo(&ports, ",%d",
                t_thrd.postmaster_cxt.ReplConnArray[i]->remoteport);
        }
    }
    appendStringInfo(standbysInfo, "host=%s port=%s", hosts.data, ports.data);
    FreeStringInfo(&hosts);
    FreeStringInfo(&ports);
    return primaryJoined;
}

static inline bool ReplconninfoChanged()
{
    for (int i = 1; i < MAX_REPLNODE_NUM; ++i) {
        if (t_thrd.postmaster_cxt.ReplConnChangeType[i]) {
            return true;
        }
    }
    return false;
}
