/* -------------------------------------------------------------------------
 *
 * subscriptioncmds.c
 * 		subscription catalog manipulation functions
 *
 * Copyright (c) 2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 * 		subscriptioncmds.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/heapam.h"
#include "access/htup.h"
#include "access/xact.h"

#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_type.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_subscription_rel.h"
#include "catalog/dependency.h"

#include "commands/defrem.h"
#include "commands/subscriptioncmds.h"
#include "commands/event_trigger.h"
#include "nodes/makefuncs.h"

#include "replication/logicallauncher.h"
#include "replication/origin.h"
#include "replication/walreceiver.h"
#include "replication/worker_internal.h"

#include "storage/lmgr.h"

#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/array.h"
#include "utils/acl.h"
#include "access/tableam.h"
#include "libpq/libpq-fe.h"
#include "replication/slot.h"

static bool ConnectPublisher(char* conninfo, char* slotname);
static void CreateSlotInPublisherAndInsertSubRel(char *slotname, Oid subid, List *publications,
                                                 bool *copy_data, bool create_slot);
static void ValidateReplicationSlot(char *slotname, List *publications);
static List *fetch_table_list(List *publications);
static void ReportSlotConnectionError(List *rstates, Oid subid, char *slotname);
static bool CheckPublicationsExistOnPublisher(List *publications);

/*
 * Common option parsing function for CREATE and ALTER SUBSCRIPTION commands.
 *
 * Since not all options can be specified in both commands, this function
 * will report an error on options if the target output pointer is NULL to
 * accommodate that.
 */
static void parse_subscription_options(const List *options, char **conninfo, List **publications, bool *enabled_given,
    bool *enabled, bool *slot_name_given, char **slot_name, char **synchronous_commit, bool *binary_given, bool *binary,
    bool *copy_data_given, bool *copy_data, bool *connect_given, bool *connect)
{
    ListCell *lc;

    if (conninfo) {
        *conninfo = NULL;
    }
    if (publications) {
        *publications = NIL;
    }
    if (enabled) {
        *enabled_given = false;
    }
    if (slot_name) {
        *slot_name_given = false;
        *slot_name = NULL;
    }
    if (synchronous_commit) {
        *synchronous_commit = NULL;
    }
    if (binary) {
        *binary_given = false;
        *binary = false;
    }

    if (copy_data) {
        *copy_data_given = false;
        *copy_data = true;
    }

    if (connect) {
        *connect_given = false;
        *connect = true;
    }

    /* Parse options */
    foreach (lc, options) {
        DefElem *defel = (DefElem *)lfirst(lc);

        if (strcmp(defel->defname, "conninfo") == 0 && conninfo) {
            if (*conninfo) {
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }

            *conninfo = defGetString(defel);
        } else if (strcmp(defel->defname, "publication") == 0 && publications) {
            if (*publications) {
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }

            *publications = defGetStringList(defel);
        } else if (strcmp(defel->defname, "enabled") == 0 && enabled) {
            if (*enabled_given) {
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }

            *enabled_given = true;
            *enabled = defGetBoolean(defel);
        } else if (strcmp(defel->defname, "slot_name") == 0 && slot_name) {
            if (*slot_name_given) {
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }

            *slot_name_given = true;
            *slot_name = defGetString(defel);

            /* Setting slot_name = NONE is treated as no slot name. */
            if (strcmp(*slot_name, "none") == 0) {
                *slot_name = NULL;
            } else {
                ReplicationSlotValidateName(*slot_name, ERROR);
            }
        } else if (strcmp(defel->defname, "synchronous_commit") == 0 && synchronous_commit) {
            if (*synchronous_commit) {
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            }

            *synchronous_commit = defGetString(defel);

            /* Test if the given value is valid for synchronous_commit GUC. */
            (void)set_config_option("synchronous_commit", *synchronous_commit, PGC_BACKEND, PGC_S_TEST, GUC_ACTION_SET,
                false, 0, false);
        } else if (strcmp(defel->defname, "binary") == 0 && binary) {
            if (*binary_given) {
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options")));
            }

            *binary_given = true;
            *binary = defGetBoolean(defel);
        } else if (strcmp(defel->defname, "copy_data") == 0 && copy_data) {
            if (*copy_data_given) {
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options")));
            }

            *copy_data_given = true;
            *copy_data = defGetBoolean(defel);
        } else if (strcmp(defel->defname, "connect") == 0 && connect) {
            if (*connect_given) {
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                         errmsg("conflicting or redundant options")));
            }

            *connect_given = true;
            *connect = defGetBoolean(defel);
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR), errmsg("unrecognized subscription parameter: %s", defel->defname)));
        }
    }

    /*
     * We've been explicitly asked to not connect, that requires some
     * additional processing.
     */
    if (connect && !*connect) {
        /* Check for incompatible options from the user. */
        if (*enabled_given && *enabled)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("%s and %s are mutually exclusive options",
                "connect = false", "enabled = true")));

        if (*copy_data_given && *copy_data)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("%s and %s are mutually exclusive options",
                "connect = false", "copy_data = true")));

        /* Change the defaults of other options. */
        *enabled = false;
        *copy_data = false;
    }

    /*
     * Do additional checking for disallowed combination when
     * slot_name = NONE was used.
     */
    if (slot_name && *slot_name_given && !*slot_name) {
        if (enabled && *enabled_given && *enabled) {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("slot_name = NONE and enabled = true are mutually exclusive options")));
        }

        if (enabled && !*enabled_given && *enabled) {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("subscription with slot_name = NONE must also set enabled = false")));
        }
    }

    if (copy_data && *copy_data && u_sess->attr.attr_storage.max_sync_workers_per_subscription == 0) {
        ereport(WARNING, (errmsg("you need to set max_sync_workers_per_subscription because it is zero but "
                                 "copy_data is true")));
    }
}

/*
 * Auxiliary function to build a text array out of a list of String nodes.
 */
static Datum publicationListToArray(List *publist)
{
    ArrayType *arr;
    Datum *datums;
    int j = 0;
    ListCell *cell;
    MemoryContext memcxt;
    MemoryContext oldcxt;

    if (list_length(publist) <= 0) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
            errmsg("LogicDecode[Publication]: null publication name list")));
    }

    /* Create memory context for temporary allocations. */
    memcxt = AllocSetContextCreate(CurrentMemoryContext, "publicationListToArray to array", ALLOCSET_DEFAULT_SIZES);
    oldcxt = MemoryContextSwitchTo(memcxt);

    datums = (Datum *)palloc(sizeof(Datum) * list_length(publist));
    foreach (cell, publist) {
        char *name = strVal(lfirst(cell));
        ListCell *pcell;

        /* Check for duplicates. */
        foreach (pcell, publist) {
            char *pname = strVal(lfirst(pcell));

            if (pcell == cell)
                break;

            if (strcmp(name, pname) == 0)
                ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR), errmsg("publication name \"%s\" used more than once", pname)));
        }

        datums[j++] = CStringGetTextDatum(name);
    }

    MemoryContextSwitchTo(oldcxt);

    arr = construct_array(datums, list_length(publist), TEXTOID, -1, false, 'i');
    MemoryContextDelete(memcxt);

    return PointerGetDatum(arr);
}

/*
 * Parse the original connection string which is encrypted, poll all hosts and ports,
 * and try to connect to the publisher.
 * When checkRemoteMode is true, the remotemode must be normal or primary.
 * Return true to indicate successful connection.
 */
bool AttemptConnectPublisher(const char *conninfoOriginal, char* slotname, bool checkRemoteMode)
{
    size_t conninfoLen = strlen(conninfoOriginal) + 1;

    char* conninfo = NULL;
    StringInfoData conninfoWithoutHostport;
    initStringInfo(&conninfoWithoutHostport);
    HostPort* hostPortList[MAX_REPLNODE_NUM] = {NULL};
    ParseConninfo(conninfoOriginal, &conninfoWithoutHostport, hostPortList);
    if (hostPortList[0] == NULL) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg(
            "invalid connection string syntax, missing host and port")));
    }
    bool connectSuccess = false;
    conninfo = (char*)palloc(conninfoLen * sizeof(char));
    for (int i = 0; i < MAX_REPLNODE_NUM; ++i) {
        if (hostPortList[i] == NULL) {
            break;
        }
        int ret = snprintf_s(conninfo, conninfoLen, conninfoLen - 1,
            "%s host=%s port=%s", conninfoWithoutHostport.data,
            hostPortList[i]->host, hostPortList[i]->port);
        securec_check_ss(ret, "\0", "\0");

        connectSuccess = ConnectPublisher(conninfo, slotname);
        if (!connectSuccess) {
            /* try next host */
            continue;
        }
        if (!checkRemoteMode) {
            break;
        }
        ServerMode publisherServerMde = IdentifyRemoteMode();
        if (publisherServerMde == NORMAL_MODE || publisherServerMde == PRIMARY_MODE) {
            break;
        }
        /* it's a standby, try next host */
        (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_disconnect();
        connectSuccess = false;
    }
    pfree_ext(conninfo);

    /* clean up */
    FreeStringInfo(&conninfoWithoutHostport);
    for (int i = 0; i < MAX_REPLNODE_NUM; ++i) {
        if (hostPortList[i] == NULL) {
            break;
        }
        pfree_ext(hostPortList[i]->host);
        pfree_ext(hostPortList[i]->port);
        pfree_ext(hostPortList[i]);
    }
    return connectSuccess;
}

/*
 * connect to publisher with conninfo
 */
static bool ConnectPublisher(char* conninfo, char* slotname)
{
    /* Try to connect to the publisher. */
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    SpinLockAcquire(&walrcv->mutex);
    walrcv->conn_target = REPCONNTARGET_PUBLICATION;
    SpinLockRelease(&walrcv->mutex);
    char* decryptConninfo = EncryptOrDecryptConninfo(conninfo, 'D');
    bool connectSuccess = (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_connect(decryptConninfo, NULL, slotname, -1);
    int rc = memset_s(decryptConninfo, strlen(decryptConninfo), 0, strlen(decryptConninfo));
    securec_check(rc, "", "");
    pfree_ext(decryptConninfo);
    return connectSuccess;
}

/*
 * Create replication slot in publisher side and Insert tables into pg_subscription_rel.
 * Please make sure you have already connect to publisher before calling this func.
 */
static void CreateSlotInPublisherAndInsertSubRel(char *slotname, Oid subid, List *publications, bool *copy_data,
    bool create_slot)
{
    LibpqrcvConnectParam options;
    char table_state;
    List *tables = NIL;
    ListCell *lc = NULL;
    int rc = memset_s(&options, sizeof(LibpqrcvConnectParam), 0, sizeof(LibpqrcvConnectParam));
    securec_check(rc, "", "");
    options.logical = true;
    options.slotname = slotname;
    PG_TRY();
    {
        if (copy_data) {
            /*
             * Set sync state based on if we were asked to do data copy or
             * not.
             */
            table_state = *copy_data ? SUBREL_STATE_INIT : SUBREL_STATE_READY;

            /*
             * Get the table list from publisher and build local table status
             * info.
             */
            tables = fetch_table_list(publications);
            foreach (lc, tables) {
                RangeVar *rv = (RangeVar *)lfirst(lc);
                Oid relid = RangeVarGetRelid(rv, AccessShareLock, false);

                AddSubscriptionRelState(subid, relid, table_state);
            }
        }

        /*
         * Create slot after synchronizing the table list to avoid
         * leaving an unused slot on the publisher.
         */
        if (create_slot) {
            (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_create_slot(&options, NULL, NULL);
            ereport(NOTICE, (errmsg("created replication slot \"%s\" on publisher", slotname)));
        }
    }
    PG_CATCH();
    {
        /* Close the connection in case of failure. */
        (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_disconnect();
        PG_RE_THROW();
    }
    PG_END_TRY();
}

/* Validate the replication slot by start streaming */
static void ValidateReplicationSlot(char *slotname, List *publications)
{
    /*
     * We just want to validate the replication slot, so the start point is not important.
     * so we use InvalidXLogRecPtr as the start point, then the replication slot won't advance.
     * and we won't decode any data here.
     */
    LibpqrcvConnectParam options;
    int rc = memset_s(&options, sizeof(LibpqrcvConnectParam), 0, sizeof(LibpqrcvConnectParam));
    securec_check(rc, "", "");
    options.logical = true;
    options.startpoint = InvalidXLogRecPtr;
    options.slotname = slotname;
    options.protoVersion = 1;
    options.publicationNames = publications;
    PG_TRY();
    {
        (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_startstreaming(&options);
    }
    PG_CATCH();
    {
        /* Close the connection in case of failure. */
        (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_disconnect();
        PG_RE_THROW();
    }
    PG_END_TRY();
}

/*
 * Create new subscription.
 */
ObjectAddress CreateSubscription(CreateSubscriptionStmt *stmt, bool isTopLevel)
{
    if (t_thrd.proc->workingVersionNum < PUBLICATION_VERSION_NUM) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Before GRAND VERSION NUM %u, we do not support subscription.", PUBLICATION_VERSION_NUM)));
    }

    Relation rel;
    ObjectAddress myself;
    Oid subid;
    bool nulls[Natts_pg_subscription];
    Datum values[Natts_pg_subscription];
    Oid owner = GetUserId();
    HeapTuple tup;
    bool enabled_given = false;
    bool enabled = true;
    char *synchronous_commit;
    char *slotname;
    bool slotname_given;
    bool binary;
    bool binary_given;
    bool copy_data;
    bool copy_data_given;
    bool connect;
    bool connect_given;
    char originname[NAMEDATALEN];
    List *publications;
    int rc;

    /*
     * Parse and check options.
     * Connection and publication should not be specified here.
     */
    parse_subscription_options(stmt->options, NULL, NULL, &enabled_given, &enabled, &slotname_given, &slotname,
        &synchronous_commit, &binary_given, &binary, &copy_data_given, &copy_data, &connect_given, &connect);

    /*
     * Since creating a replication slot is not transactional, rolling back
     * the transaction leaves the created replication slot.  So we cannot run
     * CREATE SUBSCRIPTION inside a transaction block if creating a
     * replication slot.
     */
    if (enabled)
        PreventTransactionChain(isTopLevel, "CREATE SUBSCRIPTION ... WITH (enabled = true)");

    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to create subscriptions")));

    rel = heap_open(SubscriptionRelationId, RowExclusiveLock);

    /* Check if name is used */
    subid = GetSysCacheOid2(SUBSCRIPTIONNAME, u_sess->proc_cxt.MyDatabaseId, CStringGetDatum(stmt->subname));
    if (OidIsValid(subid)) {
        ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("subscription \"%s\" already exists", stmt->subname)));
    }

    /* The default for synchronous_commit of subscriptions is off. */
    if (synchronous_commit == NULL) {
        synchronous_commit = "off";
    }

    publications = stmt->publication;

    /* Check the connection info string. */
    libpqrcv_check_conninfo(stmt->conninfo);

    /* Everything ok, form a new tuple. */
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "", "");
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "", "");

    values[Anum_pg_subscription_subdbid - 1] = ObjectIdGetDatum(u_sess->proc_cxt.MyDatabaseId);
    values[Anum_pg_subscription_subname - 1] = DirectFunctionCall1(namein, CStringGetDatum(stmt->subname));
    values[Anum_pg_subscription_subowner - 1] = ObjectIdGetDatum(owner);
    values[Anum_pg_subscription_subenabled - 1] = BoolGetDatum(enabled);
    values[Anum_pg_subscription_subbinary - 1] = BoolGetDatum(binary);

    /* encrypt conninfo */
    char *encryptConninfo = EncryptOrDecryptConninfo(stmt->conninfo, 'E');
    values[Anum_pg_subscription_subconninfo - 1] = CStringGetTextDatum(encryptConninfo);

    if (enabled) {
        if (!slotname_given) {
            slotname = stmt->subname;
        }
        values[Anum_pg_subscription_subslotname - 1] = DirectFunctionCall1(namein, CStringGetDatum(slotname));
    } else {
        if (slotname_given && slotname) {
            ereport(WARNING, (errmsg("When enabled=false, it is dangerous to set slot_name. "
                "This will cause wal log accumulation on the publisher, "
                "so slot_name will be forcibly set to NULL.")));
        }
        nulls[Anum_pg_subscription_subslotname - 1] = true;
    }
    values[Anum_pg_subscription_subsynccommit - 1] = CStringGetTextDatum(synchronous_commit);
    values[Anum_pg_subscription_subpublications - 1] = publicationListToArray(publications);

    tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

    /* Insert tuple into catalog. */
    subid = simple_heap_insert(rel, tup);
    CatalogUpdateIndexes(rel, tup);
    heap_freetuple(tup);

    recordDependencyOnOwner(SubscriptionRelationId, subid, owner);

    rc = sprintf_s(originname, sizeof(originname), "pg_%u", subid);
    securec_check_ss(rc, "", "");
    replorigin_create(originname);

    /*
     * Connect to remote side to execute requested commands and fetch table
     * info.
     */
    if (connect) {
        if (!AttemptConnectPublisher(encryptConninfo, slotname, true)) {
            ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("Failed to connect to publisher.")));
        }

        if (!CheckPublicationsExistOnPublisher(publications)) {
            (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_disconnect();
            ereport(ERROR, (errmsg("There are some publications not exist on the publisher.")));
        }

        /*
         * If requested, create the replication slot on remote side for our
         * newly created subscription.
         */
        Assert(!enabled || slotname);
        CreateSlotInPublisherAndInsertSubRel(slotname, subid, publications, &copy_data, enabled);

        (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_disconnect();
    } else {
        ereport(WARNING, (errmsg("tables were not subscribed, you will have to run %s to subscribe the tables",
                "ALTER SUBSCRIPTION ... REFRESH PUBLICATION")));
    }

    pfree_ext(encryptConninfo);
    heap_close(rel, RowExclusiveLock);
    rc = memset_s(stmt->conninfo, strlen(stmt->conninfo), 0, strlen(stmt->conninfo));
    securec_check(rc, "", "");

    /* Don't wake up logical replication launcher unnecessarily */
    if (enabled) {
        ApplyLauncherWakeupAtCommit();
    }

    myself.classId = SubscriptionRelationId;
    myself.objectId = subid;
    myself.objectSubId = 0;

    InvokeObjectAccessHook(OAT_POST_CREATE, SubscriptionRelationId, subid, 0, NULL);

    return myself;
}

static void AlterSubscription_refresh(Subscription *sub, bool copy_data)
{
    List *pubrel_names = NIL;
    List *subrel_states = NIL;
    Oid *subrel_local_oids = NULL;
    Oid *pubrel_local_oids = NULL;
    ListCell *lc = NULL;
    int off;
    int remove_rel_len;
    Relation rel = NULL;
    typedef struct SubRemoveRels {
        Oid relid;
        char state;
    } SubRemoveRels;
    SubRemoveRels *sub_remove_rels = NULL;

    PG_TRY();
    {
        /* Try to connect to the publisher. */
        if (!AttemptConnectPublisher(sub->conninfo, sub->slotname, true)) {
            ereport(ERROR, (errmsg("could not connect to the publisher: %s",
                                PQerrorMessage(t_thrd.libwalreceiver_cxt.streamConn))));
        }

        /* Get the table list from publisher. */
        pubrel_names = fetch_table_list(sub->publications);

        /* Get local table list. */
        subrel_states = GetSubscriptionRelations(sub->oid, false);

        /*
         * Build qsorted array of local table oids for faster lookup.
         * This can potentially contain all tables in the database so
         * speed of lookup is important.
         */
        subrel_local_oids = (Oid*)palloc(list_length(subrel_states) * sizeof(Oid));
        off = 0;
        foreach (lc, subrel_states) {
            SubscriptionRelState *relstate = (SubscriptionRelState *)lfirst(lc);
            subrel_local_oids[off++] = relstate->relid;
        }
        qsort(subrel_local_oids, list_length(subrel_states), sizeof(Oid), oid_cmp);

        /*
         * Rels that we want to remove from subscription and drop any slots
         * and origins corresponding to them.
         */
        sub_remove_rels = (SubRemoveRels*)palloc(list_length(subrel_states) * sizeof(SubRemoveRels));
        /*
        * Walk over the remote tables and try to match them to locally
        * known tables. If the table is not known locally create a new state
        * for it.
        *
        * Also builds array of local oids of remote tables for the next step.
        */
        off = 0;
        pubrel_local_oids = (Oid *)palloc(list_length(pubrel_names) * sizeof(Oid));

        foreach (lc, pubrel_names) {
            RangeVar *rv = (RangeVar *)lfirst(lc);
            Oid relid;

            relid = RangeVarGetRelid(rv, AccessShareLock, false);
            pubrel_local_oids[off++] = relid;

            if (!bsearch(&relid, subrel_local_oids, list_length(subrel_states), sizeof(Oid), oid_cmp)) {
                AddSubscriptionRelState(sub->oid, relid, copy_data ? SUBREL_STATE_INIT : SUBREL_STATE_READY);
                ereport(DEBUG1, (errmsg("table \"%s.%s\" added to subscription \"%s\"",
                                        rv->schemaname, rv->relname, sub->name)));
            }
        }

        /*
         * Next remove state for tables we should not care about anymore using
         * the data we collected above
         */
        qsort(pubrel_local_oids, list_length(pubrel_names), sizeof(Oid), oid_cmp);

        remove_rel_len = 0;
        for (off = 0; off < list_length(subrel_states); off++) {
            Oid relid = subrel_local_oids[off];

            if (!bsearch(&relid, pubrel_local_oids, list_length(pubrel_names), sizeof(Oid), oid_cmp)) {
                char state;
                XLogRecPtr statelsn;

                /*
                 * Lock pg_subscription_rel with AccessExclusiveLock to
                 * prevent any race conditions with the apply worker
                 * re-launching workers at the same time this code is trying
                 * to remove those tables.
                 *
                 * Even if new worker for this particular rel is restarted it
                 * won't be able to make any progress as we hold exclusive
                 * lock on subscription_rel till the transaction end. It will
                 * simply exit as there is no corresponding rel entry.
                 *
                 * This locking also ensures that the state of rels won't
                 * change till we are done with this refresh operation.
                 */
                if (!rel)
                    rel = heap_open(SubscriptionRelRelationId, AccessExclusiveLock);

                /* Last known rel state. */
                state = GetSubscriptionRelState(sub->oid, relid, &statelsn);

                sub_remove_rels[remove_rel_len].relid = relid;
                sub_remove_rels[remove_rel_len++].state = state;

                RemoveSubscriptionRel(sub->oid, relid);

                logicalrep_worker_stop(sub->oid, relid);

                /*
                 * For READY state, we would have already dropped the
                 * tablesync origin.
                 */
                if (state != SUBREL_STATE_READY) {
                    char originname[NAMEDATALEN];

                    /*
                     * Drop the tablesync's origin tracking if exists.
                     *
                     * It is possible that the origin is not yet created for
                     * tablesync worker, this can happen for the states before
                     * SUBREL_STATE_FINISHEDCOPY. The apply worker can also
                     * concurrently try to drop the origin and by this time
                     * the origin might be already removed. For these reasons,
                     * passing missing_ok = true.
                     */
                    ReplicationOriginNameForTablesync(sub->oid, relid, originname, sizeof(originname));
                    replorigin_drop_by_name(originname, true, false);
                }

                ereport(DEBUG1, (errmsg("table \"%s.%s\" removed from subscription \"%s\"",
                    get_namespace_name(get_rel_namespace(relid)), get_rel_name(relid), sub->name)));
            }
        }

        /*
         * Drop the tablesync slots associated with removed tables. This has
         * to be at the end because otherwise if there is an error while doing
         * the database operations we won't be able to rollback dropped slots.
         */
        for (off = 0; off < remove_rel_len; off++) {
            if (sub_remove_rels[off].state != SUBREL_STATE_READY &&
                sub_remove_rels[off].state != SUBREL_STATE_SYNCDONE) {
                char  syncslotname[NAMEDATALEN] = {0};

                /*
                 * For READY/SYNCDONE states we know the tablesync slot has
                 * already been dropped by the tablesync worker.
                 *
                 * For other states, there is no certainty, maybe the slot
                 * does not exist yet. Also, if we fail after removing some of
                 * the slots, next time, it will again try to drop already
                 * dropped slots and fail. For these reasons, we allow
                 * missing_ok = true for the drop.
                 */
                ReplicationSlotNameForTablesync(sub->oid, sub_remove_rels[off].relid, syncslotname,
                                                sizeof(syncslotname));
                ReplicationSlotDropAtPubNode(syncslotname, true);
            }
        }
    }
    PG_CATCH();
    {
        (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_disconnect();

        if (rel)
            heap_close(rel, NoLock);
        PG_RE_THROW();
    }
    PG_END_TRY();

    (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_disconnect();

    if (rel)
        heap_close(rel, NoLock);
}

/*
 * Alter the existing subscription.
 */
ObjectAddress AlterSubscription(AlterSubscriptionStmt *stmt, bool isTopLevel)
{
    if (t_thrd.proc->workingVersionNum < PUBLICATION_VERSION_NUM) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Before GRAND VERSION NUM %u, we do not support subscription.", PUBLICATION_VERSION_NUM)));
    }

    Relation rel;
    ObjectAddress myself;
    bool nulls[Natts_pg_subscription];
    bool replaces[Natts_pg_subscription];
    Datum values[Natts_pg_subscription];
    HeapTuple tup;
    Oid subid;
    bool enabled_given = false;
    bool enabled = false;
    bool binary_given = false;
    bool binary = false;
    char *synchronous_commit = NULL;
    char *conninfo = NULL;
    char *slot_name = NULL;
    bool slotname_given = false;
    bool copy_data = false;
    bool copy_data_given = false;
    List *publications = NIL;
    Subscription *sub = NULL;
    int rc;
    bool checkConn = false;
    bool validateSlot = false;
    bool createSlot = false;
    bool needFreeConninfo = false;
    char *finalSlotName = NULL;
    char *encryptConninfo = NULL;

    rel = heap_open(SubscriptionRelationId, RowExclusiveLock);

    /* Fetch the existing tuple. */
    tup = SearchSysCacheCopy2(SUBSCRIPTIONNAME, u_sess->proc_cxt.MyDatabaseId, CStringGetDatum(stmt->subname));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("subscription \"%s\" does not exist", stmt->subname)));

    /* must be owner */
    if (!pg_subscription_ownercheck(HeapTupleGetOid(tup), GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_SUBSCRIPTION, stmt->subname);

    subid = HeapTupleGetOid(tup);
    sub = GetSubscription(subid, false);

    /* Lock the subscription so nobody else can do anything with it. */
    LockSharedObject(SubscriptionRelationId, subid, 0, AccessExclusiveLock);

    enabled = sub->enabled;
    finalSlotName = sub->name;
    encryptConninfo = sub->conninfo;

    /* Parse options. */
    if (!stmt->refresh) {
        parse_subscription_options(stmt->options, &conninfo, &publications, &enabled_given, &enabled, &slotname_given,
            &slot_name, &synchronous_commit, &binary_given, &binary, NULL, NULL, NULL, NULL);
    } else {
        parse_subscription_options(stmt->options, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            &copy_data_given, &copy_data, NULL, NULL);

        PreventTransactionChain(isTopLevel, "ALTER SUBSCRIPTION ... REFRESH");
    }

    /* Form a new tuple. */
    rc = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(rc, "", "");
    rc = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(rc, "", "");
    rc = memset_s(replaces, sizeof(replaces), false, sizeof(replaces));
    securec_check(rc, "", "");

    if (enabled_given && enabled != sub->enabled) {
        values[Anum_pg_subscription_subenabled - 1] = BoolGetDatum(enabled);
        replaces[Anum_pg_subscription_subenabled - 1] = true;
    }
    if (conninfo) {
        /* Check the connection info string. */
        libpqrcv_check_conninfo(conninfo);
        encryptConninfo = EncryptOrDecryptConninfo(conninfo, 'E');
        rc = memset_s(conninfo, strlen(conninfo), 0, strlen(conninfo));
        securec_check(rc, "\0", "\0");
        values[Anum_pg_subscription_subconninfo - 1] = CStringGetTextDatum(encryptConninfo);
        replaces[Anum_pg_subscription_subconninfo - 1] = true;
        needFreeConninfo = true;

        /* need to check whether new conninfo can be used to connect to new publisher */
        if (sub->enabled || (enabled_given && enabled)) {
            checkConn = true;
        }
    }
    if (slotname_given) {
        if (sub->enabled && !slot_name) {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR), errmsg("cannot set slot_name = NONE for enabled subscription")));
        }

        /* change to non-null value */
        if (slot_name) {
            if (sub->enabled || (enabled_given && enabled)) {
                values[Anum_pg_subscription_subslotname - 1] = DirectFunctionCall1(namein, CStringGetDatum(slot_name));
                /* if old slotname is null or same as new slot name, then we need to validate the new slot name */
                validateSlot = sub->slotname == NULL || strcmp(slot_name, sub->slotname) != 0;
                finalSlotName = slot_name;
            } else {
                ereport(ERROR, (errmsg("Currently enabled=false, cannot change slot_name to a non-null value.")));
            }
        } else {
            /* change to NULL */

            /*
             * when enable this subscription but slot_name is not specified,
             * it will be set to default value subname.
             */
            if (!sub->enabled && enabled_given && enabled) {
                values[Anum_pg_subscription_subslotname - 1] = DirectFunctionCall1(namein, CStringGetDatum(sub->name));
            } else {
                nulls[Anum_pg_subscription_subslotname - 1] = true;
            }
        }
        replaces[Anum_pg_subscription_subslotname - 1] = true;
    } else if (!sub->enabled && enabled_given && enabled) {
        values[Anum_pg_subscription_subslotname - 1] = DirectFunctionCall1(namein, CStringGetDatum(sub->name));
        replaces[Anum_pg_subscription_subslotname - 1] = true;
    }
    if (synchronous_commit) {
        values[Anum_pg_subscription_subsynccommit - 1] = CStringGetTextDatum(synchronous_commit);
        replaces[Anum_pg_subscription_subsynccommit - 1] = true;
    }
    if (binary_given) {
        values[Anum_pg_subscription_subbinary - 1] = BoolGetDatum(binary);
        replaces[Anum_pg_subscription_subbinary - 1] = true;
    }
    if (publications != NIL) {
        values[Anum_pg_subscription_subpublications - 1] = publicationListToArray(publications);
        replaces[Anum_pg_subscription_subpublications - 1] = true;
    } else {
        publications = sub->publications;
    }

    tup = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);

    /* Update the catalog. */
    simple_heap_update(rel, &tup->t_self, tup);
    CatalogUpdateIndexes(rel, tup);

    myself.classId = SubscriptionRelationId;
    myself.objectId = subid;
    myself.objectSubId = 0;

    /* Cleanup. */
    heap_freetuple(tup);
    heap_close(rel, RowExclusiveLock);

    /* case 0: keep the subscription enabled, it's over here */
    /* case 1: deactivating subscription */
    if (sub->enabled && !enabled) {
        ereport(ERROR, (errmsg("If you want to deactivate this subscription, use DROP SUBSCRIPTION.")));
    }

    /* case 2: keep the subscription active */
    if (sub->enabled) {
        if (!AttemptConnectPublisher(encryptConninfo, finalSlotName, true)) {
            ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg( "Failed to connect to publisher.")));
        }
        if (!CheckPublicationsExistOnPublisher(publications)) {
            (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_disconnect();
            ereport(ERROR, (errmsg("There are some publications not exist on the publisher.")));
        }
        (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_disconnect();
    }

    /* case 3:
     * enabling subscription, but slot hasn't been created,
     * then mark createSlot to true.
     */
    if (!sub->enabled && enabled && (!sub->slotname || !*(sub->slotname))) {
            createSlot = true;
    }

    if (checkConn || createSlot || validateSlot) {
        if (!AttemptConnectPublisher(encryptConninfo, finalSlotName, true)) {
            ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg(
                checkConn ? "The new conninfo cannot connect to new publisher." : "Failed to connect to publisher.")));
        }

        if (!CheckPublicationsExistOnPublisher(publications)) {
            (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_disconnect();
            ereport(ERROR, (errmsg("There are some publications not exist on the publisher.")));
        }

        if (createSlot) {
            CreateSlotInPublisherAndInsertSubRel(finalSlotName, subid, publications, NULL, true);
        }

        /* no need to validate replication slot if the slot is created just by ourself */
        if (!createSlot && validateSlot) {
            ValidateReplicationSlot(finalSlotName, sub->publications);
        }

        (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_disconnect();
        ApplyLauncherWakeupAtCommit();
    }

    if (stmt->refresh) {
        if (!sub->enabled) {
            ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    errmsg("ALTER SUBSCRIPTION ... REFRESH is not allowed for disabled subscriptions")));
        }
        AlterSubscription_refresh(sub, copy_data);
    }

    if (needFreeConninfo) {
        pfree_ext(encryptConninfo);
    }
    return myself;
}

/*
 * Drop a subscription
 */
void DropSubscription(DropSubscriptionStmt *stmt, bool isTopLevel)
{
    if (t_thrd.proc->workingVersionNum < PUBLICATION_VERSION_NUM) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Before GRAND VERSION NUM %u, we do not support subscription.", PUBLICATION_VERSION_NUM)));
    }

    Relation rel;
    ObjectAddress myself;
    HeapTuple tup;
    Oid subid;
    Datum datum;
    bool isnull;
    char *subname;
    char *conninfo;
    char *slotname;
    List *subWorkers;
    ListCell *lc;
    char originname[NAMEDATALEN];
    int rc;
    List *rstates = NIL;

    /*
     * Lock pg_subscription with AccessExclusiveLock to ensure that the
     * launcher doesn't restart new worker during dropping the subscription
     */
    rel = heap_open(SubscriptionRelationId, AccessExclusiveLock);

    tup = SearchSysCache2(SUBSCRIPTIONNAME, u_sess->proc_cxt.MyDatabaseId, CStringGetDatum(stmt->subname));
    if (!HeapTupleIsValid(tup)) {
        heap_close(rel, NoLock);

        if (!stmt->missing_ok)
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("subscription \"%s\" does not exist", stmt->subname)));
        else
            ereport(NOTICE, (errmsg("subscription \"%s\" does not exist, skipping", stmt->subname)));

        return;
    }

    subid = HeapTupleGetOid(tup);
    /* must be owner */
    if (!pg_subscription_ownercheck(subid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_SUBSCRIPTION, stmt->subname);

    /* DROP hook for the subscription being removed */
    InvokeObjectAccessHook(OAT_DROP, SubscriptionRelationId, subid, 0, NULL);

    /*
     * Lock the subscription so nobody else can do anything with it
     * (including the replication workers).
     */
    LockSharedObject(SubscriptionRelationId, subid, 0, AccessExclusiveLock);

    /* Get subname */
    datum = SysCacheGetAttr(SUBSCRIPTIONOID, tup, Anum_pg_subscription_subname, &isnull);
    Assert(!isnull);
    subname = pstrdup(NameStr(*DatumGetName(datum)));

    /* Get conninfo */
    datum = SysCacheGetAttr(SUBSCRIPTIONOID, tup, Anum_pg_subscription_subconninfo, &isnull);
    Assert(!isnull);
    conninfo = TextDatumGetCString(datum);

    /* Get slotname */
    datum = SysCacheGetAttr(SUBSCRIPTIONOID, tup, Anum_pg_subscription_subslotname, &isnull);
    if (!isnull) {
        slotname = pstrdup(NameStr(*DatumGetName(datum)));
    } else {
        slotname = NULL;
    }

    /*
     * Since dropping a replication slot is not transactional, the replication
     * slot stays dropped even if the transaction rolls back.  So we cannot
     * run DROP SUBSCRIPTION inside a transaction block if dropping the
     * replication slot.
     *
     * XXX The command name should really be something like "DROP SUBSCRIPTION
     * of a subscription that is associated with a replication slot", but we
     * don't have the proper facilities for that.
     */
    if (slotname) {
        PreventTransactionChain(isTopLevel, "DROP SUBSCRIPTION");
    }

    myself.classId = SubscriptionRelationId;
    myself.objectId = subid;
    myself.objectSubId = 0;
    EventTriggerSQLDropAddObject(&myself, true, true);

    /* Remove the tuple from catalog. */
    simple_heap_delete(rel, &tup->t_self);

    ReleaseSysCache(tup);

    /*
     * Stop all the subscription workers immediately.
     *
     * This is necessary if we are dropping the replication slot, so that the
     * slot becomes accessible.
     *
     * It is also necessary if the subscription is disabled and was disabled
     * in the same transaction.  Then the workers haven't seen the disabling
     * yet and will still be running, leading to hangs later when we want to
     * drop the replication origin.  If the subscription was disabled before
     * this transaction, then there shouldn't be any workers left, so this
     * won't make a difference.
     *
     * New workers won't be started because we hold an exclusive lock on the
     * subscription till the end of the transaction.
     */
    LWLockAcquire(LogicalRepWorkerLock, LW_SHARED);
    subWorkers = logicalrep_workers_find(subid, false);
    LWLockRelease(LogicalRepWorkerLock);

    foreach (lc, subWorkers) {
        LogicalRepWorker *w = (LogicalRepWorker *)lfirst(lc);
        logicalrep_worker_stop(w->subid, w->relid);
    }
    list_free(subWorkers);

    /*
     * Cleanup of tablesync replication origins.
     *
     * Any READY-state relations would already have dealt with clean-ups.
     *
     * Note that the state can't change because we have already stopped both
     * the apply and tablesync workers and they can't restart because of
     * exclusive lock on the subscription.
     */
    rstates = GetSubscriptionRelations(subid, true);
    foreach (lc, rstates) {
        SubscriptionRelState *rstate = (SubscriptionRelState *)lfirst(lc);
        Oid relid = rstate->relid;

        /* Only cleanup resources of tablesync workers */
        if (!OidIsValid(relid))
            continue;

        /*
         * Drop the tablesync's origin tracking if exists.
         *
         * It is possible that the origin is not yet created for tablesync
         * worker so passing missing_ok = true. This can happen for the states
         * before SUBREL_STATE_FINISHEDCOPY.
         */
        ReplicationOriginNameForTablesync(subid, relid, originname, sizeof(originname));
        replorigin_drop_by_name(originname, true, false);
    }

    /* Clean up dependencies */
    deleteSharedDependencyRecordsFor(SubscriptionRelationId, subid, 0);

    /* Remove any associated relation synchronization states. */
    RemoveSubscriptionRel(subid, InvalidOid);

    /* Remove the origin tracking if exists. */
    rc = sprintf_s(originname, sizeof(originname), "pg_%u", subid);
    securec_check_ss(rc, "", "");
    replorigin_drop_by_name(originname, true, false);

    /* If there is no slot associated with the subscription, we can finish here. */
    if (!slotname && rstates == NIL) {
        pfree_ext(conninfo);
        heap_close(rel, NoLock);
        return;
    }

    if (!AttemptConnectPublisher(conninfo, slotname, true)) {
        if (!slotname) {
            /* be tidy */
            list_free(rstates);
            pfree_ext(conninfo);
            heap_close(rel, NoLock);
            return;
        } else {
            ReportSlotConnectionError(rstates, subid, slotname);
        }
    }

    PG_TRY();
    {
        foreach (lc, rstates) {
            SubscriptionRelState *rstate = (SubscriptionRelState *)lfirst(lc);
            Oid relid = rstate->relid;

            /* Only cleanup resources of tablesync workers */
            if (!OidIsValid(relid))
                continue;

            /*
             * Drop the tablesync slots associated with removed tables.
             *
             * For SYNCDONE/READY states, the tablesync slot is known to have
             * already been dropped by the tablesync worker.
             *
             * For other states, there is no certainty, maybe the slot does
             * not exist yet. Also, if we fail after removing some of the
             * slots, next time, it will again try to drop already dropped
             * slots and fail. For these reasons, we allow missing_ok = true
             * for the drop.
             */
            if (rstate->state != SUBREL_STATE_SYNCDONE) {
                char  syncslotname[NAMEDATALEN] = {0};

                ReplicationSlotNameForTablesync(subid, relid, syncslotname, sizeof(syncslotname));
                ReplicationSlotDropAtPubNode(syncslotname, true);
            }
        }

        list_free(rstates);

        /*
         * If there is a slot associated with the subscription, then drop the
         * replication slot at the publisher.
         */
        if (slotname)
            ReplicationSlotDropAtPubNode(slotname, true);
    }
    PG_CATCH();
    {
        /* Close the connection in case of failure */
        (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_disconnect();
        PG_RE_THROW();
    }
    PG_END_TRY();

    (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_disconnect();

    pfree_ext(conninfo);
    heap_close(rel, NoLock);
}

/*
 * Drop the replication slot at the publisher node using the replication
 * connection.
 *
 * missing_ok - if true then only issue a LOG message if the slot doesn't
 * exist.
 */
void ReplicationSlotDropAtPubNode(char *slotname, bool missing_ok)
{
    WalRcvExecResult *res = NULL;
    StringInfoData cmd;

    Assert(t_thrd.libwalreceiver_cxt.streamConn);

    initStringInfo(&cmd);

    /* Check if the replication slot exists on publisher. */
    if (missing_ok) {
        Oid row[1] = {INT4OID};

        appendStringInfo(&cmd, "SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s'", slotname);
        res = (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_exec(cmd.data, 1, row);
        resetStringInfo(&cmd);

        if (res->status == WALRCV_OK_TUPLES && tuplestore_get_memtupcount(res->tuplestore) == 0) {
            walrcv_clear_result(res);
            FreeStringInfo(&cmd);

            ereport(WARNING, (errmsg("replication slot \"%s\" does not exist on publisher", slotname)));
            return;
        }
    }

    appendStringInfo(&cmd, "DROP_REPLICATION_SLOT %s WAIT", quote_identifier(slotname));

    res = WalReceiverFuncTable[GET_FUNC_IDX].walrcv_exec(cmd.data, 0, NULL);
    if (res->status != WALRCV_OK_COMMAND) {
        walrcv_clear_result(res);
        FreeStringInfo(&cmd);

        ereport(ERROR, (errmsg("could not drop the replication slot \"%s\" on publisher", slotname),
            errdetail("The error was: %s", res->err)));
    } else {
        ereport(NOTICE, (errmsg("dropped replication slot \"%s\" on publisher", slotname)));
    }

    walrcv_clear_result(res);
    FreeStringInfo(&cmd);
}

/*
 * Internal workhorse for changing a subscription owner
 */
static void AlterSubscriptionOwner_internal(Relation rel, HeapTuple tup, Oid newOwnerId)
{
    Form_pg_subscription form;

    form = (Form_pg_subscription)GETSTRUCT(tup);
    if (form->subowner == newOwnerId) {
        return;
    }

    if (!pg_subscription_ownercheck(HeapTupleGetOid(tup), GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_SUBSCRIPTION, NameStr(form->subname));

    /* New owner must be a superuser */
    if (!superuser_arg(newOwnerId))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
            errmsg("permission denied to change owner of subscription \"%s\"", NameStr(form->subname)),
            errhint("The owner of a subscription must be a superuser.")));

    form->subowner = newOwnerId;
    simple_heap_update(rel, &tup->t_self, tup);
    CatalogUpdateIndexes(rel, tup);

    /* Update owner dependency reference */
    changeDependencyOnOwner(SubscriptionRelationId, HeapTupleGetOid(tup), newOwnerId);
}

/*
 * Change subscription owner -- by name
 */
ObjectAddress AlterSubscriptionOwner(const char *name, Oid newOwnerId)
{
    Oid subid;
    HeapTuple tup;
    Relation rel;
    ObjectAddress address;

    rel = heap_open(SubscriptionRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy2(SUBSCRIPTIONNAME, u_sess->proc_cxt.MyDatabaseId, CStringGetDatum(name));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("subscription \"%s\" does not exist", name)));

    subid = HeapTupleGetOid(tup);

    AlterSubscriptionOwner_internal(rel, tup, newOwnerId);

    address.classId = SubscriptionRelationId;
    address.objectId = subid;
    address.objectSubId = 0;

    heap_freetuple(tup);

    heap_close(rel, RowExclusiveLock);

    return address;
}

/*
 * Change subscription owner -- by OID
 */
void AlterSubscriptionOwner_oid(Oid subid, Oid newOwnerId)
{
    HeapTuple tup;
    Relation rel;

    rel = heap_open(SubscriptionRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(SUBSCRIPTIONOID, ObjectIdGetDatum(subid));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("subscription with OID %u does not exist", subid)));

    AlterSubscriptionOwner_internal(rel, tup, newOwnerId);

    heap_freetuple(tup);

    heap_close(rel, RowExclusiveLock);
}

/*
 * Parse the host or port string into a string array,
 * where host and port are separated by ",".
 * input: conn --- host or port string separated by ","
 * output: connArray --- host or port string array
 * return: the length of connArray
 * for example:
 * (1):
 * conn = 1.1.1.1,2.2.2.2,...,9.9.9.9
 * connArray = {
 *              1,.1.1.1,
 *              2.2.2.2,
 *              ...,
 *              9.9.9.9
 *              }
 * return 9
 * (2):
 * conn = 1,2,...,9
 * connArray = {1,2,...,9}
 * return 9
 */
static int HostsPortsToArray(const char* conn, char** connArray)
{
    if (conn == NULL) {
        return 0;
    }
    char* cp = NULL;
    char* cur = NULL;
    char *buf = pstrdup(conn);

    cp = buf;
    int i = 0;
    while (*cp) {
        cur = cp;
        while (*cp && *cp != ',') {
            ++cp;
        }
        if (*cp == ',') {
            *cp = '\0';
            ++cp;
        }
        if (i >= MAX_REPLNODE_NUM) {
            ereport(ERROR, (errmsg("Currently, a maximum of %d servers are "
                "supported.", MAX_REPLNODE_NUM)));
        }
        connArray[i++] = pstrdup(cur);

        if (*cp == 0) {
            break;
        }
    }
    pfree(buf);
    return i;
}

/*
 * parse host and port
 */
static void ParseHostPort(char* hoststr, char* portstr, HostPort** hostPortList)
{
    char* hosts[MAX_REPLNODE_NUM] = {NULL};
    char* ports[MAX_REPLNODE_NUM] = {NULL};
    int hostNum = HostsPortsToArray(hoststr, hosts);
    int portNum = HostsPortsToArray(portstr, ports);
    if (hostNum != portNum) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("The number of host and port are inconsistent.")));
    }

    for (int i = 0; i < hostNum; ++i) {
        hostPortList[i] = (HostPort*)palloc(sizeof(HostPort));
        hostPortList[i]->host = hosts[i];
        hostPortList[i]->port = ports[i];
    }
}

/*
 * Parse conninfo
 * conninfo format:
 *      'dbname=abc user=username password=xxxx host=ip1,ip2,...,ip9 port=p1,p2,...,p9'
 * after parsing:
 * conninfoWithoutHostPort:
 *      'dbname=abc user=username password=xxxx'
 * hostPortList:
 *      {
 *          {host=ip1, port=p1},
 *          {host=ip2, port=p2},
 *          ...
 *          {host=ip9, port=p9}
 *      }
 */
void ParseConninfo(const char* conninfo, StringInfoData* conninfoWithoutHostPort, HostPort** hostPortList)
{
    List* conninfoList = ConninfoToDefList(conninfo);
    ListCell* l = NULL;

    char* hostStr = NULL;
    char* portStr = NULL;
    foreach (l, conninfoList) {
        DefElem* defel = (DefElem*)lfirst(l);
        if (pg_strcasecmp(defel->defname, "host") == 0) {
            hostStr = defGetString(defel);
        } else if (pg_strcasecmp(defel->defname, "port") == 0) {
            portStr = defGetString(defel);
        } else {
            appendStringInfo(conninfoWithoutHostPort, "%s=%s ", defel->defname, defGetString(defel));
        }
    }
    if (hostPortList != NULL) {
        ParseHostPort(hostStr, portStr, hostPortList);
    }
}

/*
 * encrypt conninfo when action = 'E'
 * decrypt conninfo when action = 'D'
 * conninfoNew: encrypted or decrypted conninfo
 */
char* EncryptOrDecryptConninfo(const char* conninfo, const char action)
{
    /* parse conninfo to list */
    List *conninfoList = ConninfoToDefList(conninfo);
    /* Sensitive options for subscription */
    const char* sensitiveOptionsArray[] = {"password"};
    const int sensitiveArrayLength = lengthof(sensitiveOptionsArray);
    switch (action) {
        /* Encrypt */
        case 'E':
            EncryptGenericOptions(conninfoList, sensitiveOptionsArray, sensitiveArrayLength, SUBSCRIPTION_MODE);
            break;

        /* Decrypt */
        case 'D':
            DecryptOptions(conninfoList, sensitiveOptionsArray, sensitiveArrayLength, SUBSCRIPTION_MODE);
            break;

        default:
            break;
    }

    char* conninfoNew = DefListToString(conninfoList);
    ClearListContent(conninfoList);
    list_free_ext(conninfoList);

    return conninfoNew;
}

/*
 * Get the list of tables which belong to specified publications on the
 * publisher connection.
 */
static List* fetch_table_list(List *publications)
{
    WalRcvExecResult *res;
    StringInfoData cmd;
    TupleTableSlot *slot;
    Oid tableRow[2] = {TEXTOID, TEXTOID};
    ListCell *lc;
    bool first;
    List *tablelist = NIL;

    Assert(list_length(publications) > 0);

    initStringInfo(&cmd);
    appendStringInfo(&cmd, "SELECT DISTINCT t.schemaname, t.tablename\n"
                        "  FROM pg_catalog.pg_publication_tables t\n"
                        " WHERE t.pubname IN (");
    first = true;
    foreach (lc, publications) {
        char *pubname = strVal(lfirst(lc));

        if (first)
            first = false;
        else
            appendStringInfoString(&cmd, ", ");

        appendStringInfo(&cmd, "%s", quote_literal_cstr(pubname));
    }
    appendStringInfoString(&cmd, ")");

    res = (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_exec(cmd.data, 2, tableRow);
    pfree(cmd.data);

    if (res->status != WALRCV_OK_TUPLES)
        ereport(ERROR,
                (errmsg("could not receive list of replicated tables from the publisher: %s",
                        res->err)));

    /* Process tables. */
    slot = MakeSingleTupleTableSlot(res->tupledesc);
    while (tuplestore_gettupleslot(res->tuplestore, true, false, slot)) {
        char *nspname;
        char *relname;
        bool isnull;
        RangeVar *rv;

        nspname = TextDatumGetCString(tableam_tslot_getattr(slot, 1, &isnull));
        Assert(!isnull);
        relname = TextDatumGetCString(tableam_tslot_getattr(slot, 2, &isnull));
        Assert(!isnull);

        rv = makeRangeVar(pstrdup(nspname), pstrdup(relname), -1);
        tablelist = lappend(tablelist, rv);

        ExecClearTuple(slot);
    }
    ExecDropSingleTupleTableSlot(slot);

    walrcv_clear_result(res);

    return tablelist;
}

/*
 * This is to report the connection failure while dropping replication slots.
 * Here, we report the WARNING for all tablesync slots so that user can drop
 * them manually, if required.
 */
static void ReportSlotConnectionError(List *rstates, Oid subid, char *slotname)
{
    ListCell *lc;

    foreach (lc, rstates) {
        SubscriptionRelState *rstate = (SubscriptionRelState *)lfirst(lc);
        Oid relid = rstate->relid;

        /* Only cleanup resources of tablesync workers */
        if (!OidIsValid(relid))
            continue;

        /*
         * Caller needs to ensure that relstate doesn't change underneath us.
         * See DropSubscription where we get the relstates.
         */
        if (rstate->state != SUBREL_STATE_SYNCDONE) {
            char  syncslotname[NAMEDATALEN] = {0};

            ReplicationSlotNameForTablesync(subid, relid, syncslotname, sizeof(syncslotname));
            ereport(WARNING, (errmsg("could not drop tablesync replication slot \"%s\"", syncslotname)));
        }
    }

    ereport(ERROR, (errmsg("could not connect to publisher when attempting to "
                           "drop the replication slot \"%s\"", slotname),
                    errdetail("The error was: %s", PQerrorMessage(t_thrd.libwalreceiver_cxt.streamConn))));
}

/*
 * Check whether publications exist on publisher
 */
static bool CheckPublicationsExistOnPublisher(List *publications)
{
    Assert(list_length(publications) > 0);

    StringInfoData cmd;
    initStringInfo(&cmd);
    appendStringInfo(&cmd, "SELECT 1 FROM pg_catalog.pg_publication t"
                        " WHERE t.pubname IN (");
    ListCell *lc;
    bool first  = true;
    foreach (lc, publications) {
        char *pubname = strVal(lfirst(lc));
        if (first) {
            first = false;
        } else {
            appendStringInfoString(&cmd, ", ");
        }
        appendStringInfo(&cmd, "%s", quote_literal_cstr(pubname));
    }
    appendStringInfoString(&cmd, ")");

    WalRcvExecResult *res;
    Oid tableRow[1] = {INT4OID};
    res = (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_exec(cmd.data, 1, tableRow);
    FreeStringInfo(&cmd);

    if (res->status != WALRCV_OK_TUPLES) {
        ereport(ERROR, (errmsg("Failed to get publication list from the publisher.")));
    }
    bool exists = false;
    if (tuplestore_get_memtupcount(res->tuplestore) == list_length(publications)) {
        exists = true;
    }

    walrcv_clear_result(res);

    return exists;
}
