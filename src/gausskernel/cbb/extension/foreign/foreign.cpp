/*
 * foreign.cpp
 *    support for foreign-data wrappers, servers and user mappings.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/extension/foreign/foreign.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "foreign/dummyserver.h"
#include "foreign/regioninfo.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "cipher.h"
#include "utils/knl_relcache.h"

extern Datum pg_options_to_table(PG_FUNCTION_ARGS);
extern Datum postgresql_fdw_validator(PG_FUNCTION_ARGS);

extern void CheckGetServerIpAndPort(const char* Address, List** AddrList, bool IsCheck, int real_addr_max);
extern void decryptKeyString(
    const char* keyStr, char destplainStr[], uint32 destplainLength, const char* obskey = NULL);

char* pg_strrstr(char* string, const char* subStr);

char* rebuildLocationOption(char* region, char* location);

/*
 * GetForeignDataWrapper -	look up the foreign-data wrapper by OID.
 */
ForeignDataWrapper* GetForeignDataWrapper(Oid fdwid)
{
    Form_pg_foreign_data_wrapper fdwform;
    ForeignDataWrapper* fdw = NULL;
    Datum datum;
    HeapTuple tp;
    bool isnull = false;

    tp = SearchSysCache1(FOREIGNDATAWRAPPEROID, ObjectIdGetDatum(fdwid));

    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cache lookup failed for foreign-data wrapper %u", fdwid)));
    }

    fdwform = (Form_pg_foreign_data_wrapper)GETSTRUCT(tp);

    fdw = (ForeignDataWrapper*)palloc(sizeof(ForeignDataWrapper));
    fdw->fdwid = fdwid;
    fdw->owner = fdwform->fdwowner;
    fdw->fdwname = pstrdup(NameStr(fdwform->fdwname));
    fdw->fdwhandler = fdwform->fdwhandler;
    fdw->fdwvalidator = fdwform->fdwvalidator;

    /* Extract the fdwoptions */
    datum = SysCacheGetAttr(FOREIGNDATAWRAPPEROID, tp, Anum_pg_foreign_data_wrapper_fdwoptions, &isnull);
    if (isnull) {
        fdw->options = NIL;
    } else {
        fdw->options = untransformRelOptions(datum);
    }

    ReleaseSysCache(tp);

    return fdw;
}

/*
 * GetForeignDataWrapperByName - look up the foreign-data wrapper
 * definition by name.
 */
ForeignDataWrapper* GetForeignDataWrapperByName(const char* fdwname, bool missing_ok)
{
    Oid fdwId = get_foreign_data_wrapper_oid(fdwname, missing_ok);

    if (!OidIsValid(fdwId)) {
        return NULL;
    }

    return GetForeignDataWrapper(fdwId);
}

/*
 * GetForeignServer - look up the foreign server name by serverid.
 */
char* GetForeignServerName(Oid serverid)
{
    Form_pg_foreign_server serverform;
    char* server_name = NULL;
    HeapTuple tp;

    tp = SearchSysCache1(FOREIGNSERVEROID, ObjectIdGetDatum(serverid));

    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR,
            (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for foreign server %u", serverid)));
    }

    serverform = (Form_pg_foreign_server)GETSTRUCT(tp);
    server_name = pstrdup(NameStr(serverform->srvname));

    ReleaseSysCache(tp);

    return server_name;
}

/*
 * GetForeignServer - look up the foreign server definition.
 */
ForeignServer* GetForeignServer(Oid serverid)
{
    Form_pg_foreign_server serverform;
    ForeignServer* server = NULL;
    HeapTuple tp;
    Datum datum;
    bool isnull = false;

    tp = SearchSysCache1(FOREIGNSERVEROID, ObjectIdGetDatum(serverid));

    if (!HeapTupleIsValid(tp)) {
        ereport(
            ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cache lookup failed for foreign server %u", serverid)));
    }

    serverform = (Form_pg_foreign_server)GETSTRUCT(tp);

    server = (ForeignServer*)palloc(sizeof(ForeignServer));
    server->serverid = serverid;
    server->servername = pstrdup(NameStr(serverform->srvname));
    server->owner = serverform->srvowner;
    server->fdwid = serverform->srvfdw;

    /* Extract server type */
    datum = SysCacheGetAttr(FOREIGNSERVEROID, tp, Anum_pg_foreign_server_srvtype, &isnull);
    server->servertype = isnull ? NULL : pstrdup(TextDatumGetCString(datum));

    /* Extract server version */
    datum = SysCacheGetAttr(FOREIGNSERVEROID, tp, Anum_pg_foreign_server_srvversion, &isnull);
    server->serverversion = isnull ? NULL : pstrdup(TextDatumGetCString(datum));

    /* Extract the srvoptions */
    datum = SysCacheGetAttr(FOREIGNSERVEROID, tp, Anum_pg_foreign_server_srvoptions, &isnull);
    if (isnull) {
        server->options = NIL;
    } else {
        server->options = untransformRelOptions(datum);
    }

    ReleaseSysCache(tp);

    return server;
}

/*
 * GetForeignServerByName - look up the foreign server definition by name.
 */
ForeignServer* GetForeignServerByName(const char* srvname, bool missing_ok)
{
    Oid serverid = get_foreign_server_oid(srvname, missing_ok);

    if (!OidIsValid(serverid)) {
        return NULL;
    }

    return GetForeignServer(serverid);
}

/* Jude whether type of the foreign table equal to the specified type or not.
 * ServerName: the server name of foreign table.
 * SepcifiedType: the given type of foreign table.
 */
bool IsSpecifiedFDW(const char* ServerName, const char* SepcifiedType)
{
    ForeignServer* Server = GetForeignServerByName(ServerName, false);

    /* GetForeignServerByname may return NULL. */
    if (NULL == Server) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("foreign table server is invalid.")));
    }

    ForeignDataWrapper* Fdw = GetForeignDataWrapper(Server->fdwid);

    if (0 == pg_strcasecmp(Fdw->fdwname, SepcifiedType)) {
        return true;
    } else {
        return false;
    }
}

/**
 * @Description: Jude whether type of the foreign table equal to the specified type or not.
 * @in relId: The foreign table Oid.
 * @in SepcifiedType: The given type of foreign table.
 * @return If the relation type equal to the given type of foreign table, return true,
 *         otherwise return false.
 */
bool IsSpecifiedFDWFromRelid(Oid relId, const char* SepcifiedType)
{
    ForeignTable* ftbl = NULL;
    ForeignServer* fsvr = NULL;
    bool IsSpecifiedTable = false;

    if (u_sess->opt_cxt.ft_context == NULL) {
        u_sess->opt_cxt.ft_context = AllocSetContextCreate(u_sess->top_mem_cxt,
            "ForeignTableTemp1",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
    } else {
        MemoryContextReset(u_sess->opt_cxt.ft_context);
    }
    MemoryContext old_context = MemoryContextSwitchTo(u_sess->opt_cxt.ft_context);

    ftbl = GetForeignTable(relId);
    Assert(NULL != ftbl);

    fsvr = GetForeignServer(ftbl->serverid);
    Assert(NULL != fsvr);

    if (IsSpecifiedFDW(fsvr->servername, SepcifiedType)) {
        IsSpecifiedTable = true;
    }

    MemoryContextSwitchTo(old_context);
    MemoryContextReset(u_sess->opt_cxt.ft_context);

    return IsSpecifiedTable;
}

/**
 * @Description: Jude whether type of the foreign table support SELECT/INSERT/UPDATE/DELETE/COPY
 * @in relId: The foreign table Oid.
 * @return Rreturn true if the foreign table support those DML.
 */
bool CheckSupportedFDWType(Oid relId)
{
    static const char* supportFDWType[] = {MOT_FDW, MYSQL_FDW, ORACLE_FDW, POSTGRES_FDW};
    int size = sizeof(supportFDWType) / sizeof(supportFDWType[0]);
    bool support = false;

    if (u_sess->opt_cxt.ft_context == NULL) {
        u_sess->opt_cxt.ft_context = AllocSetContextCreate(u_sess->top_mem_cxt,
            "ForeignTableTemp1",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
    } else {
        MemoryContextReset(u_sess->opt_cxt.ft_context);
    }
    MemoryContext oldContext = MemoryContextSwitchTo(u_sess->opt_cxt.ft_context);

    ForeignTable* ftbl = GetForeignTable(relId);
    ForeignServer* fsvr = GetForeignServer(ftbl->serverid);
    ForeignDataWrapper* fdw = GetForeignDataWrapper(fsvr->fdwid);

    for (int i = 0; i < size; i++) {
        if (pg_strcasecmp(fdw->fdwname, supportFDWType[i]) == 0) {
            support = true;
            break;
        }
    }

    MemoryContextSwitchTo(oldContext);
    MemoryContextReset(u_sess->opt_cxt.ft_context);
    return support;
}

bool isSpecifiedSrvTypeFromRelId(Oid relId, const char* SepcifiedType)
{
    ForeignTable* ftbl = NULL;
    ForeignServer* fsrv = NULL;
    bool ret = false;

    if (u_sess->opt_cxt.ft_context == NULL) {
        u_sess->opt_cxt.ft_context = AllocSetContextCreate(u_sess->top_mem_cxt,
            "ForeignTableTemp2",
            ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE);
    } else {
        MemoryContextReset(u_sess->opt_cxt.ft_context);
    }
    MemoryContext old_context = MemoryContextSwitchTo(u_sess->opt_cxt.ft_context);

    ftbl = GetForeignTable(relId);
    Assert(NULL != ftbl);

    fsrv = GetForeignServer(ftbl->serverid);
    Assert(NULL != fsrv);

    ret = isSpecifiedSrvTypeFromSrvName(fsrv->servername, SepcifiedType);

    MemoryContextSwitchTo(old_context);
    MemoryContextReset(u_sess->opt_cxt.ft_context);

    return ret;
}

bool isSpecifiedSrvTypeFromSrvName(const char* srvName, const char* SepcifiedType)
{
    ForeignServer* Server = GetForeignServerByName(srvName, false);
    /* GetForeignServerByname may return NULL. */
    if (NULL == Server) {
        ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                errmsg("cannot find foreign server with given name %s .", srvName)));
    }

    bool isSpecifiedSrvType = false;
    ListCell* optionCell = NULL;

    foreach (optionCell, Server->options) {
        DefElem* optionDef = (DefElem*)lfirst(optionCell);
        char* optionDefName = optionDef->defname;

        if (0 == pg_strcasecmp(optionDefName, "type")) {
            char* optionValue = defGetString(optionDef);
            if (0 == pg_strcasecmp(optionValue, SepcifiedType)) {
                isSpecifiedSrvType = true;
                break;
            }
        }
    }

    return isSpecifiedSrvType;
}

/*
 * GetUserMapping - look up the user mapping.
 *
 * If no mapping is found for the supplied user, we also look for
 * PUBLIC mappings (userid == InvalidOid).
 */
UserMapping* GetUserMapping(Oid userid, Oid serverid)
{
    Datum datum;
    HeapTuple tp;
    bool isnull = false;
    UserMapping* um = NULL;

    tp = SearchSysCache2(USERMAPPINGUSERSERVER, ObjectIdGetDatum(userid), ObjectIdGetDatum(serverid));

    if (!HeapTupleIsValid(tp)) {
        /* Not found for the specific user -- try PUBLIC */
        tp = SearchSysCache2(USERMAPPINGUSERSERVER, ObjectIdGetDatum(InvalidOid), ObjectIdGetDatum(serverid));
    }

    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("user mapping not found for \"%s\"", MappingUserName(userid))));
    }

    um = (UserMapping*)palloc(sizeof(UserMapping));
    um->userid = userid;
    um->serverid = serverid;

    /* Extract the umoptions */
    datum = SysCacheGetAttr(USERMAPPINGUSERSERVER, tp, Anum_pg_user_mapping_umoptions, &isnull);
    if (isnull) {
        um->options = NIL;
    } else {
        um->options = untransformRelOptions(datum);
    }

    DecryptOptions(um->options, g_sensitiveOptionsArray, g_sensitiveArrayLength, USER_MAPPING_MODE);

    ReleaseSysCache(tp);

    return um;
}

/*
 * GetForeignTable - look up the foreign table definition by relation oid.
 */
ForeignTable* GetForeignTable(Oid relid)
{
    Form_pg_foreign_table tableform;
    ForeignTable* ft = NULL;
    HeapTuple tp;
    Datum datum;
    bool isnull = false;

    tp = SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cache lookup failed for foreign table %u", relid)));
    }
    tableform = (Form_pg_foreign_table)GETSTRUCT(tp);

    ft = (ForeignTable*)palloc(sizeof(ForeignTable));
    ft->relid = relid;
    ft->serverid = tableform->ftserver;
    ft->write_only = tableform->ftwriteonly;

    /* Extract the ftoptions */
    datum = SysCacheGetAttr(FOREIGNTABLEREL, tp, Anum_pg_foreign_table_ftoptions, &isnull);
    if (isnull) {
        ft->options = NIL;
    } else {
        ft->options = untransformRelOptions(datum);
    }

    ReleaseSysCache(tp);

    return ft;
}

/**
 * @Description: Juge the foreign table is write only table?
 * @in relid, the foreign table oid.
 * @return return true if the foreign talbe is write-only mode, otherwise return false.
 */
bool isWriteOnlyFt(Oid relid)
{
    Form_pg_foreign_table tableform;
    HeapTuple tp;

    tp = SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cache lookup failed for foreign table %u", relid)));
    }
    tableform = (Form_pg_foreign_table)GETSTRUCT(tp);

    ReleaseSysCache(tp);

    return tableform->ftwriteonly;
}

/*
 * GetForeignColumnOptions - Get attfdwoptions of given relation/attnum
 * as list of DefElem.
 */
List* GetForeignColumnOptions(Oid relid, AttrNumber attnum)
{
    List* options = NIL;
    HeapTuple tp;
    Datum datum;
    bool isnull = false;

    tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attnum));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("cache lookup failed for attribute %d of relation %u", attnum, relid)));
    }
    datum = SysCacheGetAttr(ATTNUM, tp, Anum_pg_attribute_attfdwoptions, &isnull);
    if (isnull) {
        options = NIL;
    } else {
        options = untransformRelOptions(datum);
    }

    ReleaseSysCache(tp);

    return options;
}

/*
 * GetFdwRoutine - call the specified foreign-data wrapper handler routine
 * to get its FdwRoutine struct.
 */
FdwRoutine* GetFdwRoutine(Oid fdwhandler)
{
    Datum datum;
    FdwRoutine* routine = NULL;

    datum = OidFunctionCall0(fdwhandler);
    routine = (FdwRoutine*)DatumGetPointer(datum);

    if (routine == NULL || !IsA(routine, FdwRoutine)) {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_INVALID_HANDLE),
                errmsg("foreign-data wrapper handler function %u did not return an FdwRoutine struct", fdwhandler)));
    }

    return routine;
}

/*
 * GetFdwRoutineByRelId - look up the handler of the foreign-data wrapper
 * for the given foreign table, and retrieve its FdwRoutine struct.
 */
FdwRoutine* GetFdwRoutineByRelId(Oid relid)
{
    HeapTuple tp;
    Form_pg_foreign_data_wrapper fdwform;
    Form_pg_foreign_server serverform;
    Form_pg_foreign_table tableform;
    Oid serverid;
    Oid fdwid;
    Oid fdwhandler;

    /* Get server OID for the foreign table. */
    tp = SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cache lookup failed for foreign table %u", relid)));
    }
    tableform = (Form_pg_foreign_table)GETSTRUCT(tp);
    serverid = tableform->ftserver;
    ReleaseSysCache(tp);

    /* Get foreign-data wrapper OID for the server. */
    tp = SearchSysCache1(FOREIGNSERVEROID, ObjectIdGetDatum(serverid));
    if (!HeapTupleIsValid(tp)) {
        ereport(
            ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cache lookup failed for foreign table %u", serverid)));
    }
    serverform = (Form_pg_foreign_server)GETSTRUCT(tp);
    fdwid = serverform->srvfdw;
    ReleaseSysCache(tp);

    /* Get handler function OID for the FDW. */
    tp = SearchSysCache1(FOREIGNDATAWRAPPEROID, ObjectIdGetDatum(fdwid));
    if (!HeapTupleIsValid(tp)) {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("cache lookup failed for foreign-data wrapper %u", fdwid)));
    }
    fdwform = (Form_pg_foreign_data_wrapper)GETSTRUCT(tp);
    fdwhandler = fdwform->fdwhandler;

    /* Complain if FDW has been set to NO HANDLER. */
    if (!OidIsValid(fdwhandler)) {
        ereport(ERROR,
            (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("foreign-data wrapper \"%s\" has no handler", NameStr(fdwform->fdwname))));
    }

    ReleaseSysCache(tp);

    /* And finally, call the handler function. */
    return GetFdwRoutine(fdwhandler);
}

/*
 * GetFdwRoutineByServerId - look up the handler of the foreign-data wrapper
 * for the given foreign table, and retrieve its FdwRoutine struct.
 */
FdwRoutine* GetFdwRoutineByServerId(Oid serverid)
{
    HeapTuple tp;
    Form_pg_foreign_data_wrapper fdwform;
    Form_pg_foreign_server serverform;
    Oid fdwid;
    Oid fdwhandler;

    /* Get foreign-data wrapper OID for the server. */
    tp = SearchSysCache1(FOREIGNSERVEROID, ObjectIdGetDatum(serverid));
    if (!HeapTupleIsValid(tp))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("cache lookup failed for foreign server %u", serverid)));
    serverform = (Form_pg_foreign_server) GETSTRUCT(tp);
    fdwid = serverform->srvfdw;
    ReleaseSysCache(tp);

    /* Get handler function OID for the FDW. */
    tp = SearchSysCache1(FOREIGNDATAWRAPPEROID, ObjectIdGetDatum(fdwid));
    if (!HeapTupleIsValid(tp))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("cache lookup failed for foreign-data wrapper %u", fdwid)));
    fdwform = (Form_pg_foreign_data_wrapper) GETSTRUCT(tp);
    fdwhandler = fdwform->fdwhandler;

    /* Complain if FDW has been set to NO HANDLER. */
    if (!OidIsValid(fdwhandler))
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg("foreign-data wrapper \"%s\" has no handler", NameStr(fdwform->fdwname))));

    ReleaseSysCache(tp);

    /* And finally, call the handler function. */
    return GetFdwRoutine(fdwhandler);
}

/*
 * GetFdwRoutineForRelation - look up the handler of the foreign-data wrapper
 * for the given foreign table, and retrieve its FdwRoutine struct.
 *
 * This function is preferred over GetFdwRoutineByRelId because it caches
 * the data in the relcache entry, saving a number of catalog lookups.
 *
 * If makecopy is true then the returned data is freshly palloc'd in the
 * caller's memory context.  Otherwise, it's a pointer to the relcache data,
 * which will be lost in any relcache reset --- so don't rely on it long.
 */
FdwRoutine* GetFdwRoutineForRelation(Relation relation, bool makecopy)
{
    FdwRoutine* fdwroutine = NULL;
    FdwRoutine* cfdwroutine = NULL;
    errno_t rc = EOK;

    if (relation->rd_fdwroutine == NULL) {
        /* Get the info by consulting the catalogs and the FDW code */
        fdwroutine = GetFdwRoutineByRelId(RelationGetRelid(relation));

        /* Save the data for later reuse in LocalMyDBCacheMemCxt() */
        cfdwroutine = (FdwRoutine*)MemoryContextAlloc(LocalMyDBCacheMemCxt(), sizeof(FdwRoutine));
        rc = memcpy_s(cfdwroutine, sizeof(FdwRoutine), fdwroutine, sizeof(FdwRoutine));
        securec_check(rc, "", "");

        relation->rd_fdwroutine = cfdwroutine;

        /* Give back the locally palloc'd copy regardless of makecopy */
        return fdwroutine;
    }

    /* We have valid cached data --- does the caller want a copy? */
    if (makecopy) {
        fdwroutine = (FdwRoutine*)palloc(sizeof(FdwRoutine));
        rc = memcpy_s(fdwroutine, sizeof(FdwRoutine), relation->rd_fdwroutine, sizeof(FdwRoutine));
        securec_check(rc, "", "");
        return fdwroutine;
    }

    /* Only a short-lived reference is needed, so just hand back cached copy */
    return relation->rd_fdwroutine;
}

/*
 * deflist_to_tuplestore - Helper function to convert DefElem list to
 * tuplestore usable in SRF.
 */
static void deflist_to_tuplestore(ReturnSetInfo* rsinfo, List* options)
{
    ListCell* cell = NULL;
    TupleDesc tupdesc;
    Tuplestorestate* tupstore = NULL;
    Datum values[2];
    bool nulls[2];
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;

    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("set-valued function called in context that cannot accept a set")));
    }
    if (!(rsinfo->allowedModes & SFRM_Materialize) || rsinfo->expectedDesc == NULL) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("materialize mode required, but it is not allowed in this context")));
    }

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    /*
     * Now prepare the result set.
     */
    tupdesc = CreateTupleDescCopy(rsinfo->expectedDesc);
    tupstore = tuplestore_begin_heap(true, false, u_sess->attr.attr_memory.work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;

    foreach (cell, options) {
        DefElem* def = (DefElem*)lfirst(cell);

        values[0] = CStringGetTextDatum(def->defname);
        nulls[0] = false;
        if (def->arg) {
            values[1] = CStringGetTextDatum(((Value*)(def->arg))->val.str);
            nulls[1] = false;
        } else {
            values[1] = (Datum)0;
            nulls[1] = true;
        }
        tuplestore_putvalues(tupstore, tupdesc, values, nulls);
    }

    /* clean up and return the tuplestore */
    tuplestore_donestoring(tupstore);

    MemoryContextSwitchTo(oldcontext);
}

/*
 * Convert options array to name/value table.  Useful for information
 * schema and pg_dump.
 */
Datum pg_options_to_table(PG_FUNCTION_ARGS)
{
    Datum array = PG_GETARG_DATUM(0);

    deflist_to_tuplestore((ReturnSetInfo*)fcinfo->resultinfo, untransformRelOptions(array));

    return (Datum)0;
}

/*
 * Describes the valid options for openGauss FDW, server, and user mapping.
 */
struct ConnectionOption {
    const char* optname;
    Oid optcontext; /* Oid of catalog in which option may appear */
};

/*
 * Copied from fe-connect.c PQconninfoOptions.
 *
 * The list is small - don't bother with bsearch if it stays so.
 */
static const struct ConnectionOption libpq_conninfo_options[] = {{"authtype", ForeignServerRelationId},
    {"service", ForeignServerRelationId},
    {"user", UserMappingRelationId},
    {"password", UserMappingRelationId},
    {"connect_timeout", ForeignServerRelationId},
    {"dbname", ForeignServerRelationId},
    {"host", ForeignServerRelationId},
    {"hostaddr", ForeignServerRelationId},
    {"port", ForeignServerRelationId},
    {"tty", ForeignServerRelationId},
    {"options", ForeignServerRelationId},
    {"requiressl", ForeignServerRelationId},
    {"sslmode", ForeignServerRelationId},
    {"gsslib", ForeignServerRelationId},
    {NULL, InvalidOid}};

/*
 * Check if the provided option is one of libpq conninfo options.
 * context is the Oid of the catalog the option came from, or 0 if we
 * don't care.
 */
static bool is_conninfo_option(const char* option, Oid context)
{
    const struct ConnectionOption* opt = NULL;

    for (opt = libpq_conninfo_options; opt->optname; opt++) {
        if (context == opt->optcontext && strcmp(opt->optname, option) == 0) {
            return true;
        }
    }
    return false;
}

/*
 * Validate the generic option given to SERVER or USER MAPPING.
 * Raise an ERROR if the option or its value is considered invalid.
 *
 * Valid server options are all libpq conninfo options except
 * user and password -- these may only appear in USER MAPPING options.
 *
 * Caution: this function is deprecated, and is now meant only for testing
 * purposes, because the list of options it knows about doesn't necessarily
 * square with those known to whichever libpq instance you might be using.
 * Inquire of libpq itself, instead.
 */
Datum postgresql_fdw_validator(PG_FUNCTION_ARGS)
{
    List* options_list = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid catalog = PG_GETARG_OID(1);

    ListCell* cell = NULL;

    foreach (cell, options_list) {
        DefElem* def = (DefElem*)lfirst(cell);

        if (!is_conninfo_option(def->defname, catalog)) {
            const struct ConnectionOption* opt = NULL;
            StringInfoData buf;

            /*
             * Unknown option specified, complain about it. Provide a hint
             * with list of valid options for the object.
             */
            initStringInfo(&buf);
            for (opt = libpq_conninfo_options; opt->optname; opt++) {
                if (catalog == opt->optcontext) {
                    appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "", opt->optname);
                }
            }

            ereport(ERROR,
                (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                    errmsg("invalid option \"%s\"", def->defname),
                    errhint("Valid options in this context are: %s", buf.data)));

            PG_RETURN_BOOL(false);
        }
    }

    PG_RETURN_BOOL(true);
}

/*
 * get_foreign_data_wrapper_oid - given a FDW name, look up the OID
 *
 * If missing_ok is false, throw an error if name not found.  If true, just
 * return InvalidOid.
 */
Oid get_foreign_data_wrapper_oid(const char* fdwname, bool missing_ok)
{
    Oid oid;

    oid = GetSysCacheOid1(FOREIGNDATAWRAPPERNAME, CStringGetDatum(fdwname));
    if (!OidIsValid(oid) && !missing_ok) {
        ereport(
            ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("foreign-data wrapper \"%s\" does not exist", fdwname)));
    }
    return oid;
}

/*
 * get_foreign_server_oid - given a FDW name, look up the OID
 *
 * If missing_ok is false, throw an error if name not found.  If true, just
 * return InvalidOid.
 */
Oid get_foreign_server_oid(const char* servername, bool missing_ok)
{
    Oid oid;

    oid = GetSysCacheOid1(FOREIGNSERVERNAME, CStringGetDatum(servername));
    if (!OidIsValid(oid) && !missing_ok) {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("server \"%s\" does not exist", servername)));
    }
    return oid;
}

DefElem* GetForeignTableOptionByName(Oid reloid, const char* optname)
{
    ForeignTable* table = NULL;
    ForeignServer* server = NULL;
    ForeignDataWrapper* wrapper = NULL;
    List* options = NIL;
    ListCell* lc = NULL;

    /*
     * Extract options from FDW objects.  We ignore user mappings because
     * file_fdw doesn't have any options that can be specified there.
     *
     * (XXX Actually, given the current contents of valid_options[], there's
     * no point in examining anything except the foreign table's own options.
     * Simplify?)
     */
    table = GetForeignTable(reloid);
    server = GetForeignServer(table->serverid);
    wrapper = GetForeignDataWrapper(server->fdwid);

    options = NIL;
    options = list_concat(options, wrapper->options);
    options = list_concat(options, server->options);
    options = list_concat(options, table->options);

    foreach (lc, options) {
        DefElem* def = (DefElem*)lfirst(lc);

        if (strcmp(def->defname, optname) == 0) {
            return def;
        }
    }

    return NULL;
}

HdfsFdwOptions* HdfsGetOptions(Oid foreignTableId)
{
    HdfsFdwOptions* hdfsFdwOptions = NULL;
    char* address = NULL;
    List* AddrList = NULL;

    hdfsFdwOptions = (HdfsFdwOptions*)palloc0(sizeof(HdfsFdwOptions));
    hdfsFdwOptions->filename = HdfsGetOptionValue(foreignTableId, OPTION_NAME_FILENAMES);
    hdfsFdwOptions->foldername = HdfsGetOptionValue(foreignTableId, OPTION_NAME_FOLDERNAME);
    hdfsFdwOptions->location = HdfsGetOptionValue(foreignTableId, OPTION_NAME_LOCATION);

    address = HdfsGetOptionValue(foreignTableId, OPTION_NAME_ADDRESS);

    if (NULL != address) {
        if (T_HDFS_SERVER == getServerType(foreignTableId)) {
            CheckGetServerIpAndPort(address, &AddrList, false, -1);
            hdfsFdwOptions->address = ((HdfsServerAddress*)linitial(AddrList))->HdfsIp;
            hdfsFdwOptions->port = atoi(((HdfsServerAddress*)linitial(AddrList))->HdfsPort);
        } else {
            /* As for OBS table, only the address is needed. */
            hdfsFdwOptions->address = address;
        }
    }

    return hdfsFdwOptions;
}

/*
 * @Description: get option DefElem
 * @IN foreignTableId: foreign table oid
 * @IN optionName: option name
 * @Return: DefElem ptr of option
 * @See also:
 */
DefElem* HdfsGetOptionDefElem(Oid foreignTableId, const char* optionName)
{
    List* optionList = NIL;
    ListCell* optionCell = NULL;
    DefElem* returnOptDef = NULL;

    optionList = getFdwOptions(foreignTableId);

    foreach (optionCell, optionList) {
        DefElem* optionDef = (DefElem*)lfirst(optionCell);
        char* optionDefName = optionDef->defname;

        if (strncmp(optionDefName, optionName, NAMEDATALEN) == 0) {
            returnOptDef = (DefElem*)copyObject(optionDef);
            break;
        }
    }

    if (optionList != NIL) {
        pfree(optionList);
    }

    return returnOptDef;
}

/* GetOption,GetOptionValue,GetOptionName and related structs need to be merged. */
char* HdfsGetOptionValue(Oid foreignTableId, const char* optionName)
{
#define DEST_CIPHER_LENGTH 1024
    char* optionValue = NULL;
    char decrypStr[DEST_CIPHER_LENGTH] = {'\0'};
    errno_t rc = EOK;

    DefElem* optionDef = HdfsGetOptionDefElem(foreignTableId, optionName);

    if (optionDef != NULL) {
        optionValue = defGetString(optionDef);
        if (0 == pg_strcasecmp(optionName, OPTION_NAME_SERVER_SAK)) {
            decryptKeyString(optionValue, decrypStr, DEST_CIPHER_LENGTH);
            optionValue = pstrdup(decrypStr);
            rc = memset_s(decrypStr, DEST_CIPHER_LENGTH, 0, DEST_CIPHER_LENGTH);
            securec_check(rc, "\0", "\0");
        }
    }

    return optionValue;
}

ObsOptions* getObsOptions(Oid foreignTableId)
{
#define LOCAL_STRING_BUFFER_SIZE 512

    ObsOptions* obsOptions = NULL;
    AutoContextSwitch newContext(INSTANCE_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE));
    errno_t rc = EOK;

    obsOptions = (ObsOptions*)palloc0(sizeof(ObsOptions));

    if (IS_OBS_CSV_TXT_FOREIGN_TABLE(foreignTableId)) {
        char buffer[LOCAL_STRING_BUFFER_SIZE];
        int ibegin = 0;
        int iend = 0;
        int copylen = 0;

        char* location = HdfsGetOptionValue(foreignTableId, optLocation);
        ibegin = find_Nth(location, 2, "/");
        iend = find_Nth(location, 3, "/");
        copylen = iend - ibegin - 1;
        rc = strncpy_s(buffer, LOCAL_STRING_BUFFER_SIZE, location + (ibegin + 1), copylen);
        securec_check(rc, "", "");
        obsOptions->address = pstrdup(buffer);
    } else {
        obsOptions->address = HdfsGetOptionValue(foreignTableId, OPTION_NAME_ADDRESS);
        if (NULL == obsOptions->address) {
            char* region = HdfsGetOptionValue(foreignTableId, OPTION_NAME_REGION);
            obsOptions->address = readDataFromJsonFile(region);
        }
    }

    DefElem* optionDef = HdfsGetOptionDefElem(foreignTableId, OPTION_NAME_SERVER_ENCRYPT);
    if (optionDef == NULL) {
        obsOptions->encrypt = false;
    } else {
        obsOptions->encrypt = defGetBoolean(optionDef);
    }

    obsOptions->access_key = HdfsGetOptionValue(foreignTableId, OPTION_NAME_SERVER_AK);

    char* tempStr = HdfsGetOptionValue(foreignTableId, OPTION_NAME_SERVER_SAK);
    if (tempStr != NULL) {
        obsOptions->secret_access_key = (char*)SEC_encodeBase64(tempStr, strlen(tempStr));
        rc = memset_s(tempStr, strlen(tempStr), 0, strlen(tempStr));
        securec_check(rc, "\0", "\0");
        pfree(tempStr);
    }

    obsOptions->bucket = NULL;

    obsOptions->prefix = NULL;

    return obsOptions;
}

/**
 * @Description: get the foreign server type of the special foreign talbe..
 * @in foreignTableId, the given foreign table oid.
 * @return
 * notes: currently, only the hdfs foreign table and obs foreign table have
 * server type. so when the given foreign table is not either of obs foreign
 * foreign table and obs foreign table, return T_INVALID.
 */
ServerTypeOption getServerType(Oid foreignTableId)
{
    char* optionValue = HdfsGetOptionValue(foreignTableId, "type");
    ServerTypeOption srvType = T_INVALID;

    if (optionValue != NULL) {
        if (0 == pg_strcasecmp(optionValue, OBS_SERVER)) {
            srvType = T_OBS_SERVER;
        } else if (0 == pg_strcasecmp(optionValue, HDFS_SERVER)) {
            srvType = T_HDFS_SERVER;
        } else if (0 == pg_strcasecmp(optionValue, DUMMY_SERVER)) {
            srvType = T_DUMMY_SERVER;
        }
    } else if (IsSpecifiedFDWFromRelid(foreignTableId, DIST_FDW) &&
               (is_obs_protocol(HdfsGetOptionValue(foreignTableId, optLocation)))) {
        srvType = T_TXT_CSV_OBS_SERVER;
    } else if (IsSpecifiedFDWFromRelid(foreignTableId, GC_FDW)) {
        srvType = T_PGFDW_SERVER;
    }

    /* for in-place upgrade */
    if (T_INVALID == srvType && IsSpecifiedFDWFromRelid(foreignTableId, HDFS_FDW)) {
        srvType = T_HDFS_SERVER;
    }

    return srvType;
}

/**
 * @Description: get the all option by the given foreign table oid,
 * which include foreign talbe option
 * and foreign server option.
 * @in foreignTableId, the foreign table oid.
 * @return return all options.
 */
List* getFdwOptions(Oid foreignTableId)
{
    List* optionList = NIL;
    ForeignTable* foreignTbl = NULL;
    ForeignServer* foreignSrv = NULL;

    foreignTbl = GetForeignTable(foreignTableId);
    foreignSrv = GetForeignServer(foreignTbl->serverid);

    optionList = list_concat(optionList, foreignTbl->options);
    optionList = list_concat(optionList, foreignSrv->options);

    return optionList;
}

/**
 * @Description: get one server option value by special option name.
 * server oid.
 * @in srvOid, the given foreign server option.
 * @in optionName, the server option name.
 * @out
 * @out
 * @return
 */
char* getServerOptionValue(Oid srvOid, const char* optionName)
{
#define DEST_CIPHER_LENGTH 1024

    ForeignServer* foreignSrv = GetForeignServer(srvOid);
    List* optionList = foreignSrv->options;
    char* optionValue = NULL;
    ListCell* optionCell = NULL;
    char decrypStr[DEST_CIPHER_LENGTH] = {'\0'};

    foreach (optionCell, optionList) {
        DefElem* optionDef = (DefElem*)lfirst(optionCell);
        char* optionDefName = optionDef->defname;

        if (strncmp(optionDefName, optionName, NAMEDATALEN) == 0) {
            optionValue = defGetString(optionDef);

            if (0 == pg_strcasecmp(optionName, OPTION_NAME_PASSWD)) {
                decryptKeyString(optionValue, decrypStr, DEST_CIPHER_LENGTH);
                optionValue = pstrdup(decrypStr);
            }

            break;
        }
    }

    return optionValue;
}

/**
 * @Description: count the number of the given specialChar
 * in string "path".
 * @in path, find special character in this string.
 * @in specialChar, the given character.
 * @return return the statistic.
 */
int getSpecialCharCnt(const char* path, const char specialChar)
{
    int specialCharNum = 0;

    Assert(NULL != path);

    size_t pathLen = strlen(path);

    for (size_t i = 0; i < pathLen; i++) {
        if (path[i] == specialChar) {
            specialCharNum++;
        }
    }

    return specialCharNum;
}

/**
 * @Description: get the specified option from the option list.
 * @in optionList, the given optionList.
 * @in optionName, the specified option name.
 * @return retue the option value using the DefElem struct.
 */
DefElem* getFTOptionDefElemFromList(List* optionList, const char* optionName)
{
    ListCell* optionCell = NULL;
    DefElem* def = NULL;
    foreach (optionCell, optionList) {
        DefElem* optionDef = (DefElem*)lfirst(optionCell);
        char* optionDefName = optionDef->defname;

        if (strncmp(optionDefName, optionName, NAMEDATALEN) == 0) {
            def = (DefElem*)copyObject(optionDef);
            break;
        }
    }

    return def;
}

/**
 * @Description: get value assocaited with option
 * @List* optionList: input option list
 * @char* optionName: input option name
 * @return char* optionValue: output option value
 */
char* getFTOptionValue(List* optionList, const char* optionName)
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
 * @Description: get alter action assocaited with option
 * @List* optionList: input option list
 * @char* optionName: input option name
 * @return DefElemAction alterAction: output alter action
 */
DefElemAction getFTAlterAction(List* optionList, const char* optionName)
{
    DefElemAction alterAction = DEFELEM_UNSPEC;
    ListCell* optionCell = NULL;
    foreach (optionCell, optionList) {
        DefElem* optionDef = (DefElem*)lfirst(optionCell);
        char* optionDefName = optionDef->defname;

        if (pg_strcasecmp(optionDefName, optionName) == 0) {
            alterAction = optionDef->defaction;
            break;
        }
    }

    return alterAction;
}

/**
 * @Description: validate sting and convert it to double
 * @char* s_value: input string
 * @return double if input string is valid
 */
double convertFTOptionValue(const char* s_value)
{
    int64 l_value = 0;
    bool cvtd = scanint8(s_value, true, &l_value);

    if ((!cvtd) || (l_value < MIN_TOTALROWS)) {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("totalrows is invalid")));
    }

    return (double)l_value;
}

ObsOptions* setObsSrvOptions(ForeignOptions* fOptions)
{
#define DEST_CIPHER_LENGTH 1024

    ObsOptions* options = (ObsOptions*)palloc0(sizeof(ObsOptions));
    char* address = getFTOptionValue(fOptions->fOptions, OPTION_NAME_ADDRESS);
    errno_t rc = EOK;

    if (NULL == address) {
        char* region = getFTOptionValue(fOptions->fOptions, OPTION_NAME_REGION);
        options->address = readDataFromJsonFile(region);
    } else {
        options->address = address;
    }

    options->access_key = getFTOptionValue(fOptions->fOptions, OPTION_NAME_SERVER_AK);

    DefElem* def_elem = getFTOptionDefElemFromList(fOptions->fOptions, OPTION_NAME_SERVER_ENCRYPT);
    options->encrypt = def_elem ? defGetBoolean(def_elem) : false;
    char* sak = getFTOptionValue(fOptions->fOptions, OPTION_NAME_SERVER_SAK);

    char* obskey = getFTOptionValue(fOptions->fOptions, OPTION_NAME_OBSKEY);
    if (NULL == obskey) {
        ereport(ERROR,
            (errmodule(MOD_ACCELERATE),
                errcode(ERRCODE_FDW_ERROR),
                errmsg("Failed to get obskey from options of DWS.")));
    }

    char decrypStr[DEST_CIPHER_LENGTH] = {'\0'};
    decryptKeyString(sak, decrypStr, DEST_CIPHER_LENGTH, obskey);
    options->secret_access_key = pstrdup(decrypStr);

    rc = memset_s(decrypStr, DEST_CIPHER_LENGTH, 0, DEST_CIPHER_LENGTH);
    securec_check(rc, "\0", "\0");

    return options;
}

HdfsOptions* setHdfsSrvOptions(ForeignOptions* fOptions)
{
    HdfsOptions* options = (HdfsOptions*)palloc0(sizeof(HdfsOptions));

    options->servername = getFTOptionValue(fOptions->fOptions, OPTION_NAME_REMOTESERVERNAME);

    options->format = getFTOptionValue(fOptions->fOptions, OPTION_NAME_FORMAT);
    if (NULL == options->format) {
        ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("No \"format\" option provided.")));
    }

    return options;
}

extern char* TrimStr(const char* str);

/**
 * @Description: Currently, two OBS location format support in database.
 * one format is "gsobs:://obsdomain/bucket/prefix", another is "gsobs:://bucket.obsdomain/prefix".
 * we adjust second format to the first format, we only deal with the first format in parse phase.
 * @in optionsList, if the list include location option, we will adjust format.
 * @return return new optionsList.
 */
List* regularizeObsLocationInfo(List* optionsList)
{
    char* sourceStr = NULL;
    ListCell* optionCell = NULL;
    DefElem* optionDef = NULL;
    char* returnLocation = NULL;
    char* token = NULL;
    char* saved = NULL;
    char* cpyLocation = NULL;
    char* str = NULL;
    size_t startPos = 0;
    errno_t rc;
    DefElem* locationDefElem = NULL;

    foreach (optionCell, optionsList) {
        optionDef = (DefElem*)lfirst(optionCell);
        char* optionDefName = optionDef->defname;

        if (0 == pg_strcasecmp(optionDefName, optLocation)) {

            if (DEFELEM_DROP == optionDef->defaction) {
                /*
                 * if the user commond is that "alter foreign table ft options(drop location)"
                 * the optionDef has no value, so we retrun it.
                 */
            } else {
                /* found it. */
                sourceStr = defGetString(optionDef);

                locationDefElem = (DefElem*)copyObject(optionDef);
                break;
            }
        }
    }
    /* if we do not found this option, return it. */
    if (NULL == sourceStr || 0 == strlen(sourceStr) || !is_obs_protocol(sourceStr)) {
        return optionsList;
    }

    optionsList = list_delete(optionsList, optionDef);

    returnLocation = (char*)palloc0(sizeof(char) * (strlen(sourceStr) + 1));

    cpyLocation = pstrdup(sourceStr);
    token = strtok_r(cpyLocation, "|", &saved);

    while (NULL != token) {
        char* tempStr = NULL;
        str = TrimStr(token);
        if (NULL == str) {
            ereport(ERROR, (errcode(ERRCODE_FDW_ERROR), errmsg("Invalid URL \"%s\" in LOCATION", token)));
        }
        tempStr = adjustObsLocationInfoOrder(str);

        rc = memcpy_s(returnLocation + startPos, strlen(tempStr), tempStr, strlen(tempStr));
        securec_check(rc, "\0", "\0");
        startPos += strlen(tempStr);
        pfree(tempStr);

        token = strtok_r(NULL, "|", &saved);

        if (NULL != token) {
            rc = memcpy_s(returnLocation + startPos, strlen("|"), "|", strlen("|"));
            securec_check(rc, "\0", "\0");
            startPos += strlen("|");
        }
    }

    pfree(cpyLocation);

    locationDefElem->arg = (Node*)makeString(returnLocation);
    optionsList = lappend(optionsList, locationDefElem);

    return optionsList;
}

/**
 * @Description: Currently, two OBS location format support in database.
 * one format is "gsobs:://obsdomain/bucket/prefix", another is "gsobs:://bucket.obsdomain/prefix".
 * we adjust second format to the first format, we only deal with the first format in parse phase.
 * @in sourceStr, if the given string includes GSOBS_PREFIX , we will adjust format.
 * @return return new location string.
 */
char* adjustObsLocationInfoOrder(char* sourceStr)
{
    size_t strLen = strlen(sourceStr);
    char* newStr = NULL;
    errno_t rc;
    if (pg_strncasecmp(sourceStr, GSOBS_PREFIX, strlen(GSOBS_PREFIX))) {
        /* it means that the option is not obs option. */
        return sourceStr;
    }

    /*
     * if we do not found the slash '/', it means that the option value is
     * invalid, but we do not deal with it here, return it.
     */
    char* prefixPos = strchr(sourceStr + strlen(GSOBS_PREFIX), '/');
    if (NULL == prefixPos) {
        return sourceStr;
    }

    /* it store the string "bucket.obsdomain", for example, bucket.obsdomain/prefix. */
    size_t tempStrLen = (prefixPos - sourceStr) - strlen(GSOBS_PREFIX) + 1;
    char* tempStr = (char*)palloc0(sizeof(char) * tempStrLen);
    rc = memcpy_s(tempStr, tempStrLen, sourceStr + strlen(GSOBS_PREFIX), tempStrLen - 1);
    securec_check(rc, "\0", "\0");

    if (NULL != strstr(tempStr, OBS_BUCKET_URL_FORMAT_FLAG)) {
        /*
         * if we found the OBS_BUCKET_URL_FORMAT_FLAG in tempStr.
         * we assume that location string format is the "bucket.obsdomain/prefix" form.
         */
        /* found the last subStr OBS_BUCKET_URL_FORMAT_FLAG. */
        char* urlStart = pg_strrstr(tempStr, OBS_BUCKET_URL_FORMAT_FLAG);

        newStr = (char*)palloc0(sizeof(char) * strLen + 1);
        /* copy the GSOBS_PREFIX string. */
        rc = memcpy_s(newStr, strLen, GSOBS_PREFIX, strlen(GSOBS_PREFIX));
        securec_check(rc, "\0", "\0");
        strLen -= strlen(GSOBS_PREFIX);

        /* append url string. */
        rc = memcpy_s(newStr + strlen(GSOBS_PREFIX), strLen, urlStart + 1, strlen(urlStart) - 1);
        securec_check(rc, "\0", "\0");
        strLen -= strlen(urlStart) - 1;

        /* append one char '/'. */
        rc = memcpy_s(newStr + strlen(GSOBS_PREFIX) + strlen(urlStart) - 1, strLen, "/", strlen("/"));
        securec_check(rc, "\0", "\0");
        strLen -= strlen("/");

        /* append bucket string. */
        rc = memcpy_s(newStr + strlen(GSOBS_PREFIX) + strlen(urlStart) - 1 + 1, strLen, tempStr, urlStart - tempStr);
        securec_check(rc, "\0", "\0");
        strLen -= urlStart - tempStr;

        /* append prefix string. */
        rc = memcpy_s(newStr + strlen(GSOBS_PREFIX) + strlen(urlStart) + (urlStart - tempStr),
            strLen,
            prefixPos,
            strlen(prefixPos));
        securec_check(rc, "\0", "\0");
    }

    pfree(tempStr);

    return newStr ? newStr : sourceStr;
}

/**
 * @Description: find the given sub string in source string, mark the postion
 * on which appear the sub string in last time.
 * @in string, the source string.
 * @in subStr, the subStr to be found.
 * @return if found it return the possition, otherwise return NULL.
 */
char* pg_strrstr(char* string, const char* subStr)
{
    char* index = NULL;
    char* ret = NULL;
    int i = 0;

    do {
        index = strstr(string + i++, subStr);
        if (NULL != index) {
            ret = index;
        }

    } while (NULL != index);

    return ret;
}

List* adaptOBSURL(List* optionList)
{
    ListCell* optionCell = NULL;
    char* regionCode = NULL;
    Value* locationValue = NULL;
    foreach (optionCell, optionList) {
        DefElem* optionDef = (DefElem*)lfirst(optionCell);
        char* optionDefName = optionDef->defname;

        if (pg_strcasecmp(optionDefName, OPTION_NAME_REGION) == 0) {
            regionCode = defGetString(optionDef);
        }

        if (pg_strcasecmp(optionDefName, OPTION_NAME_LOCATION) == 0) {
            locationValue = (Value*)(optionDef->arg);
        }
    }

    /*
     * Fill the location option. As for the gsobs:// prefix, we do not fill
     * the location.
     */
    if (NULL != locationValue && 0 == pg_strncasecmp(strVal(locationValue), OBS_PREFIX, OBS_PREfIX_LEN)) {
        /* the regionCode may be NULL, we will get the default region. */
        char* newLocation = rebuildAllLocationOptions(regionCode, strVal(locationValue));
        strVal(locationValue) = newLocation;
    }

    return optionList;
}

/**
 * @Description: rebuild the each location option for the obs import/export foregin table.
 * we will "hostname/region" fill the fornt end of each location.
 * @in regionCode, it may be an empty pointor.
 * @return return the filled location.
 */
char* rebuildAllLocationOptions(char* regionCode, char* location)
{
    size_t len = location ? strlen(location) : 0;
    char* str = NULL;
    char* token = NULL;
    char* saved = NULL;
    StringInfo retOptions = makeStringInfo();

    if (0 == len) {
        ereport(ERROR,
            (errcode(ERRCODE_FDW_ERROR), errmodule(MOD_DFS), errmsg("Invalid location value for the foreign table.")));
    }

    token = strtok_r(location, "|", &saved);
    while (NULL != token) {
        str = TrimStr(token);
        if (NULL != str) {
            appendStringInfo(retOptions, "%s", rebuildLocationOption(regionCode, str));
        }
        token = strtok_r(NULL, "|", &saved);
        if (NULL != token) {
            appendStringInfo(retOptions, "|");
        }
    }
    ereport(DEBUG1,
        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmodule(MOD_DFS),
            errmsg("The rebuild all region string: %s.", retOptions->data)));

    return retOptions->data;
}

/**
 * @Description: fill the region string to the front end of the given location.
 * get the region string from the readDataFromJsonFile function.
 * @in regionCode, the given regionCode.
 * @in location, the given location.
 * @return return the filled location.
 */
char* rebuildLocationOption(char* regionCode, char* location)
{
    char* region = readDataFromJsonFile(regionCode);
    char* rebuildLocation = NULL;
    size_t rebuildLen = strlen(region) + strlen(location) + 2;

    rebuildLocation = (char*)palloc0(rebuildLen * (sizeof(char)));

    ereport(DEBUG1,
        (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmodule(MOD_DFS), errmsg("The location string: %s.", location)));

    ereport(DEBUG1,
        (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmodule(MOD_DFS), errmsg("The region string: %s.", region)));

    /* find the position of starting bucket. */
    int pos = find_Nth(location, 2, "/") + 1;

    int errorno = 0;

    errorno = strncpy_s(rebuildLocation, rebuildLen, OBS_PREFIX, OBS_PREfIX_LEN + 1);
    securec_check(errorno, "\0", "\0");
    rebuildLocation[OBS_PREfIX_LEN] = '\0';

    errorno = strncpy_s(rebuildLocation + OBS_PREfIX_LEN, rebuildLen, region, strlen(region) + 1);
    securec_check(errorno, "\0", "\0");
    rebuildLocation[OBS_PREfIX_LEN + strlen(region)] = '\0';

    errorno = strncpy_s(rebuildLocation + OBS_PREfIX_LEN + strlen(region), rebuildLen, "/", 1 + 1);
    securec_check(errorno, "\0", "\0");
    rebuildLocation[OBS_PREfIX_LEN + strlen(region) + 1] = '\0';

    errorno = strncpy_s(
        rebuildLocation + OBS_PREfIX_LEN + strlen(region) + 1, rebuildLen, location + pos, strlen(location + pos) + 1);
    securec_check(errorno, "\0", "\0");
    rebuildLocation[OBS_PREfIX_LEN + strlen(region) + strlen(location + pos) + 1] = '\0';

    ereport(DEBUG1,
        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmodule(MOD_DFS),
            errmsg("The rebuild region string: %s.", rebuildLocation)));

    return rebuildLocation;
}
