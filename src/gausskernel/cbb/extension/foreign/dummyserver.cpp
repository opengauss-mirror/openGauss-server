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
 * dummyserver.cpp
 *    support for computing pool. The dummy server is used to store the
 *    connection information of computing pool.
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/extension/foreign/dummyserver.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/tableam.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "foreign/dummyserver.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"

/* The dummy server cache pointer. It store the struct DummyServerOptions
 * value. we search the sepcified server options info by server oid from the
 * DummyServerCache. The interface funstion is getDummyServerOption.
 */
HTAB* DummyServerCache = NULL;

/**
 * @Description: get the dummy server from entire database.
 * @return return the DummyServerOptions if exist one dummy server,
 * otherwise return NULL.
 */
DummyServerOptions* getDummyServerOptionsFromCache(Oid serverOid);

/**
 * @Description: get dummy server oid from pg_foreign_server catalog.
 * @return return server oid if find one dummy server oid, otherwise return InvalidOid.
 */
Oid getDummyServerOid()
{
    Relation rel;
    TableScanDesc scan;
    HeapTuple tuple = NULL;
    Form_pg_foreign_server serverForm;
    Oid serverOid = InvalidOid;

    rel = heap_open(ForeignServerRelationId, AccessShareLock);
    scan = tableam_scan_begin(rel, SnapshotNow, 0, NULL);
    while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
        serverForm = (Form_pg_foreign_server)GETSTRUCT(tuple);
        if (isSpecifiedSrvTypeFromSrvName(NameStr(serverForm->srvname), DUMMY_SERVER)) {
            serverOid = HeapTupleGetOid(tuple);
            break;
        }
    }

    tableam_scan_end(scan);
    heap_close(rel, NoLock);

    return serverOid;
}

/**
 * @Description: get the dummy server from entire database.
 * @return return the DummyServerOptions if exist one dummy server,
 * otherwise return NULL.
 */
DummyServerOptions* getDummyServerOption()
{
    DummyServerOptions* serverOptions = NULL;

    Oid serverOid = getDummyServerOid();

    if (OidIsValid(serverOid)) {
        serverOptions = getDummyServerOptionsFromCache(serverOid);
    }

    if (NULL == serverOptions) {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmodule(MOD_OBS), errmsg("Failed to found one dummy server.")));
    }

    return serverOptions;
}

char* getOptionValueFromOptionList(List* optionList, const char* optionName)
{
    char* optionValue = NULL;
    ListCell* optionCell = NULL;
    foreach (optionCell, optionList) {
        DefElem* optionDef = (DefElem*)lfirst(optionCell);
        char* optionDefName = optionDef->defname;

        if (0 == pg_strcasecmp(optionDefName, optionName)) {
            optionValue = defGetString(optionDef);
            break;
        }
    }

    return optionValue;
}

/**
 * @Description: if the optionList belong to dummy server, we need check the entire
 * database. if exists one dummy server, throw error, otherwise do nothing.
 * @in optionList, the option list to be checked.
 * @return if exists one dummy server in current database,
 * throw error, otherwise do nothing.
 */
void checkExistDummyServer(List* optionList)
{
    char* typeStr = getOptionValueFromOptionList(optionList, "type");
    if (NULL != typeStr && 0 == pg_strcasecmp(DUMMY_SERVER, typeStr)) {
        /*
         * Currently, one dummy server is allowed to exist in one database.
         */
        Oid serverOid = getDummyServerOid();
        if (OidIsValid(serverOid)) {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmodule(MOD_OBS),
                    errmsg("Only one dummy server is allowed to exist in one database."),
                    errdetail("There is a \"%s\" server in the database.", GetForeignServer(serverOid)->servername)));
        }
    }
}

/**
 * @Description: jugde a dummy server type by checking server option list.
 * @in optionList, the option list to be checked.
 * @return
 */
bool isDummyServerByOptions(List* optionList)
{
    char* srvType = getOptionValueFromOptionList(optionList, "type");

    if (NULL != srvType && 0 == pg_strcasecmp(DUMMY_SERVER, srvType)) {
        return true;
    }

    return false;
}

/**
 * @Description: This function is used to suit DummyServerCache in order to
 * find the cache context by server oid.
 * @in key1, use this key1(server oid) to find cache.
 * @in key2, the stored server oid in cache.
 * @return return 0 if the two keys are equal, otherwise return !0
 */
static int matchOid(const void* key1, const void* key2, Size keySize)
{
    return (*(Oid*)key1 - *(Oid*)key2);
}

/**
 * @Description: get the dummy server option from cache.
 * @in serverOid, the dummy server oid.
 * @return return the DummyServerOptions
 */
DummyServerOptions* getDummyServerOptionsFromCache(Oid serverOid)
{
    bool found = false;
    DummyServerOptions* entry = NULL;

    /* First seach from the hash table cache. */
    (void)LWLockAcquire(dummyServerInfoCacheLock, LW_SHARED);
    entry = (DummyServerOptions*)hash_search(DummyServerCache, (void*)&serverOid, HASH_FIND, &found);
    LWLockRelease(dummyServerInfoCacheLock);

    /* Return if target is found. */
    if (found && (entry && NULL != entry->userName && NULL != entry->passWord)) {
        return entry;
    }

    MemoryContext oldCxt = MemoryContextSwitchTo(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    char* userName = pstrdup(getServerOptionValue(serverOid, OPTION_NAME_USER_NAME));
    /* pstrdup called in getServerOptionValue when password */
    char* passWord = getServerOptionValue(serverOid, OPTION_NAME_PASSWD);
    char* address = pstrdup(getServerOptionValue(serverOid, OPTION_NAME_ADDRESS));

    char* dbname = getServerOptionValue(serverOid, OPTION_NAME_DBNAME);
    if (NULL != dbname)
        dbname = pstrdup(dbname);

    char* remoteservername = getServerOptionValue(serverOid, OPTION_NAME_REMOTESERVERNAME);
    if (NULL != remoteservername)
        remoteservername = pstrdup(remoteservername);

    (void)MemoryContextSwitchTo(oldCxt);

    /* Put the new server info into the hash table. */
    (void)LWLockAcquire(dummyServerInfoCacheLock, LW_EXCLUSIVE);
    entry = (DummyServerOptions*)hash_search(DummyServerCache, (void*)&serverOid, HASH_ENTER, &found);
    if (entry == NULL)
        ereport(PANIC,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmodule(MOD_OBS),
                errmsg("build global Dummy servder cache hash table failed")));
    if (!found) {
        entry->userName = userName;
        entry->passWord = passWord;
        entry->address = address;
        entry->dbname = dbname;
        entry->remoteservername = remoteservername;

        entry->serverOid = serverOid;
    }
    LWLockRelease(dummyServerInfoCacheLock);
    if (found) {
        int passWordLength = strlen(passWord);
        errno_t rc = memset_s(passWord, passWordLength, 0, passWordLength);
        securec_check(rc, "\0", "\0");
        pfree_ext(passWord);
        pfree_ext(userName);
        pfree_ext(address);
        pfree_ext(dbname);
        pfree_ext(remoteservername);
    }

    return entry;
}

/**
 * @Description: Init dummy server cache.
 * @return none.
 */
void InitDummyServrCache()
{
    HASHCTL ctl;
    errno_t rc = 0;

    if (NULL == DummyServerCache) {
        rc = memset_s(&ctl, sizeof(ctl), 0, sizeof(ctl));
        securec_check(rc, "\0", "\0");
        ctl.hcxt = g_instance.instance_context;
        ctl.keysize = sizeof(Oid);
        ctl.entrysize = sizeof(DummyServerOptions);
        ctl.match = (HashCompareFunc)matchOid;
        ctl.hash = (HashValueFunc)oid_hash;
        DummyServerCache =
            hash_create("Dummy server cache", 50, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_SHRCTX);
        if (NULL == DummyServerCache)
            ereport(PANIC, (errmodule(MOD_OBS), errmsg("could not initialize Dummny server hash table")));
    }
}

/**
 * @Description: Drop cache when we operator alter server
 * or drop server.
 * @in serverOid, the server oid to be deleted.
 * @return none.
 */
void InvalidDummyServerCache(Oid serverOid)
{
    (void)LWLockAcquire(dummyServerInfoCacheLock, LW_EXCLUSIVE);
    DummyServerOptions* entry =
        (DummyServerOptions*)hash_search(DummyServerCache, (void*)&serverOid, HASH_REMOVE, NULL);

    if (NULL != entry) {
        if (NULL != entry->userName) {
            pfree(entry->userName);
            entry->userName = NULL;
        }
        if (NULL != entry->passWord) {
            int passWordLength = strlen(entry->passWord);
            errno_t rc = memset_s(entry->passWord, passWordLength, 0, passWordLength);
            securec_check(rc, "\0", "\0");
            pfree(entry->passWord);
            entry->passWord = NULL;
        }
        if (NULL != entry->address) {
            pfree(entry->address);
            entry->address = NULL;
        }
    }
    LWLockRelease(dummyServerInfoCacheLock);
}
