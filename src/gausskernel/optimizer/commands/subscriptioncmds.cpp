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
#include "catalog/objectaccess.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_type.h"
#include "catalog/pg_subscription.h"
#include "catalog/dependency.h"

#include "commands/defrem.h"
#include "commands/subscriptioncmds.h"

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

static bool ConnectPublisher(char* conninfo, char* slotname);
static void CreateSlotInPublisher(char *slotname);
static void ValidateReplicationSlot(char *slotname, List *publications);

/*
 * Common option parsing function for CREATE and ALTER SUBSCRIPTION commands.
 *
 * Since not all options can be specified in both commands, this function
 * will report an error on options if the target output pointer is NULL to
 * accommodate that.
 */
static void parse_subscription_options(const List *options, char **conninfo, List **publications, bool *enabled_given,
    bool *enabled, bool *slot_name_given, char **slot_name, char **synchronous_commit, bool *binary_given, bool *binary)
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
        } else {
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR), errmsg("unrecognized subscription parameter: %s", defel->defname)));
        }
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
 * Create replication slot in publisher side.
 * Please make sure you have already connect to publisher before calling this func.
 */
static void CreateSlotInPublisher(char *slotname)
{
    LibpqrcvConnectParam options;
    int rc = memset_s(&options, sizeof(LibpqrcvConnectParam), 0, sizeof(LibpqrcvConnectParam));
    securec_check(rc, "", "");
    options.logical = true;
    options.slotname = slotname;
    PG_TRY();
    {
        (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_create_slot(&options);
        ereport(NOTICE, (errmsg("created replication slot \"%s\" on publisher", slotname)));
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
    char originname[NAMEDATALEN];
    List *publications;
    int rc;

    /*
     * Parse and check options.
     * Connection and publication should not be specified here.
     */
    parse_subscription_options(stmt->options, NULL, NULL, &enabled_given, &enabled, &slotname_given, &slotname,
        &synchronous_commit, &binary_given, &binary);

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
     * If requested, create the replication slot on remote side for our
     * newly created subscription.
     */
    if (enabled) {
        Assert(slotname);

        if (!AttemptConnectPublisher(encryptConninfo, slotname, true)) {
            ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg("Failed to connect to publisher.")));
        }

        CreateSlotInPublisher(slotname);
        (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_disconnect();
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

/*
 * Alter the existing subscription.
 */
ObjectAddress AlterSubscription(AlterSubscriptionStmt *stmt)
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
    bool enabled;
    bool binary_given;
    bool binary;
    char *synchronous_commit;
    char *conninfo;
    char *slot_name;
    bool slotname_given;
    List *publications;
    Subscription *sub;
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
    enabled = sub->enabled;
    finalSlotName = sub->name;
    encryptConninfo = sub->conninfo;

    /* Parse options. */
    parse_subscription_options(stmt->options, &conninfo, &publications, &enabled_given, &enabled, &slotname_given,
        &slot_name, &synchronous_commit, &binary_given, &binary);

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

    if (sub->enabled && !enabled) {
        ereport(ERROR, (errmsg("If you want to deactivate this subscription, use DROP SUBSCRIPTION.")));
    }
    /* enabling subscription, but slot hasn't been created,
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

        if (createSlot) {
            CreateSlotInPublisher(finalSlotName);
        }

        /* no need to validate replication slot if the slot is created just by ourself */
        if (!createSlot && validateSlot) {
            ValidateReplicationSlot(finalSlotName, sub->publications);
        }

        (WalReceiverFuncTable[GET_FUNC_IDX]).walrcv_disconnect();
        ApplyLauncherWakeupAtCommit();
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
    char *err = NULL;
    StringInfoData cmd;
    int rc;

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

    /* Remove the tuple from catalog. */
    simple_heap_delete(rel, &tup->t_self);

    ReleaseSysCache(tup);

    /* Clean up dependencies */
    deleteSharedDependencyRecordsFor(SubscriptionRelationId, subid, 0);

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
        logicalrep_worker_stop(w->subid);
    }
    list_free(subWorkers);

    /* Remove the origin tracking if exists. */
    rc = sprintf_s(originname, sizeof(originname), "pg_%u", subid);
    securec_check_ss(rc, "", "");
    replorigin_drop_by_name(originname, true);

    /* If there is no slot associated with the subscription, we can finish here. */
    if (!slotname) {
        heap_close(rel, NoLock);
        return;
    }

    /*
     * Otherwise drop the replication slot at the publisher node using
     * the replication connection.
     */
    initStringInfo(&cmd);
    appendStringInfo(&cmd, "DROP_REPLICATION_SLOT %s", quote_identifier(slotname));

    if (!AttemptConnectPublisher(conninfo, slotname, true)) {
        ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE), errmsg(
            "could not connect to publisher.")));
    }

    PG_TRY();
    {
        int sqlstate = 0;
        bool res = WalReceiverFuncTable[GET_FUNC_IDX].walrcv_command(cmd.data, &err, &sqlstate);
        if (!res && sqlstate == ERRCODE_UNDEFINED_OBJECT) {
            /* drop replication slot failed cause it doesn't exist on publisher, give a warning and continue */
            ereport(WARNING, (errmsg("could not drop the replication slot \"%s\" on publisher", slotname),
                errdetail("The error was: %s", err)));
        } else if (!res) {
            ereport(ERROR, (errmsg("could not drop the replication slot \"%s\" on publisher", slotname),
                errdetail("The error was: %s", err)));
        } else {
            ereport(NOTICE, (errmsg("dropped replication slot \"%s\" on publisher", slotname)));
        }
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
    pfree(cmd.data);
    heap_close(rel, NoLock);
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
 * Internal workhorse for rename a Subscription
 */
static void RenameSubscriptionInternal(Relation rel, HeapTuple tup, const char *newname)
{
    Form_pg_subscription form = (Form_pg_subscription)GETSTRUCT(tup);

    if (!pg_subscription_ownercheck(HeapTupleGetOid(tup), GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_SUBSCRIPTION, NameStr(form->subname));

    namestrcpy(&(form->subname), newname);
    simple_heap_update(rel, &tup->t_self, tup);
    CatalogUpdateIndexes(rel, tup);
}

/*
 * Rename Subscription
 */
void RenameSubscription(List *oldname, const char *newname)
{
    HeapTuple tup;
    HeapTuple newtup;
    Relation rel;
    const char *subname = strVal(linitial(oldname));

    rel = heap_open(SubscriptionRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy2(SUBSCRIPTIONNAME, u_sess->proc_cxt.MyDatabaseId, CStringGetDatum(subname));
    if (!HeapTupleIsValid(tup)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("Subscription \"%s\" does not exist", subname)));
    }

    newtup = SearchSysCacheCopy2(SUBSCRIPTIONNAME, u_sess->proc_cxt.MyDatabaseId, CStringGetDatum(newname));
    if (HeapTupleIsValid(newtup)) {
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("Subscription \"%s\" has already exists", newname)));
    }

    RenameSubscriptionInternal(rel, tup, newname);

    heap_freetuple(tup);
    heap_close(rel, RowExclusiveLock);

    return;
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
