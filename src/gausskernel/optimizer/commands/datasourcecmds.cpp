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
 * datasourcecmds.cpp
 *	  data source creation/manipulation commands
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/datasourcecmds.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/reloptions.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pg_extension_data_source.h"
#include "cipher.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "datasource/datasource.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_func.h"
#include "parser/parser.h"
#include "pgxc/pgxc.h"
#include "storage/smgr/fd.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "catalog/toasting.h"
#include "nodes/parsenodes.h"


/* Valid options of a data source */
static const char* DataSourceOptionsArray[] = {"dsn", "username", "password", "encoding"};

/* Sensitive options */
static const char* SensitiveOptionsArray[] = {"username", "password"};

/*
 * check_source_generic_options:
 * 	Check validity and uniqueness of options in CREATE/ALTER Data Source
 *
 * @IN src_options: input generic options
 * @RETURN: void
 */
static void check_source_generic_options(const List* src_options)
{
    int i;
    int NumValidOptions = sizeof(DataSourceOptionsArray) / sizeof(DataSourceOptionsArray[0]);
    bool* isgiven = NULL;
    ListCell* cell = NULL;
    errno_t ret;

    isgiven = (bool*)palloc(NumValidOptions);
    ret = memset_s(isgiven, NumValidOptions, false, NumValidOptions);
    securec_check(ret, "\0", "\0");

    foreach (cell, src_options) {
        DefElem* def = (DefElem*)lfirst(cell);

        /* Check validity and uniqueness */
        for (i = 0; i < NumValidOptions; i++) {
            if (0 == strcmp(def->defname, DataSourceOptionsArray[i])) {
                if (isgiven[i] == false)
                    isgiven[i] = true;
                else
                    ereport(ERROR,
                        (errmodule(MOD_EC),
                            errcode(ERRCODE_DUPLICATE_OBJECT),
                            errmsg("option \"%s\" provided more than once", def->defname)));
                break;
            }
        }

        /* unrecognized option */
        if (i >= NumValidOptions)
            ereport(ERROR,
                (errmodule(MOD_EC),
                    errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("option \"%s\" not recognized.", def->defname),
                    errhint("valid options are: dsn, username, password, encoding")));
    }
}

/*
 * CreateDataSource:
 * 	Create a Data Source, only allowed by superuser!
 *
 * @IN stmt: Create Data Souce Stmt structure
 * @RETURN: void
 */
void CreateDataSource(CreateDataSourceStmt* stmt)
{
    Relation rel;
    Datum srcoptions;
    Datum values[Natts_pg_extension_data_source];
    bool nulls[Natts_pg_extension_data_source];
    HeapTuple tuple;
    Oid srcId;
    Oid ownerId;
    ObjectAddress myself;
    errno_t ret;

    rel = heap_open(DataSourceRelationId, RowExclusiveLock);

    /* Must be super user */
    if (!superuser())
        ereport(ERROR,
            (errmodule(MOD_EC),
                errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to create data source \"%s\"", stmt->srcname),
                errhint("Must be system admin to create a data source.")));

    /* For now the owner cannot be specified on create. Use effective user ID. */
    ownerId = GetUserId();

    /*
     * Check that there is no other data source by this name.
     */
    if (GetDataSourceByName(stmt->srcname, true) != NULL)
        ereport(ERROR,
            (errmodule(MOD_EC),
                errcode(ERRCODE_DUPLICATE_OBJECT),
                errmsg("data source \"%s\" already exists", stmt->srcname)));

    /*
     * Insert tuple into pg_extension_data_source
     */
    ret = memset_s(values, sizeof(values), 0, sizeof(values));
    securec_check(ret, "\0", "\0");
    ret = memset_s(nulls, sizeof(nulls), false, sizeof(nulls));
    securec_check(ret, "\0", "\0");

    /* Add source name and owner */
    values[Anum_pg_extension_data_source_srcname - 1] = DirectFunctionCall1(namein, CStringGetDatum(stmt->srcname));
    values[Anum_pg_extension_data_source_srcowner - 1] = ObjectIdGetDatum(ownerId);

    /* Add source type if supplied */
    if (stmt->srctype)
        values[Anum_pg_extension_data_source_srctype - 1] = CStringGetTextDatum(stmt->srctype);
    else
        nulls[Anum_pg_extension_data_source_srctype - 1] = true;

    /* Add source version if supplied */
    if (stmt->version)
        values[Anum_pg_extension_data_source_srcversion - 1] = CStringGetTextDatum(stmt->version);
    else
        nulls[Anum_pg_extension_data_source_srcversion - 1] = true;

    /* Start with a blank acl */
    nulls[Anum_pg_extension_data_source_srcacl - 1] = true;

    /* Check the source options */
    check_source_generic_options(stmt->options);

    /* Encrypt sensitive options before any operations */
    EncryptGenericOptions(stmt->options, SensitiveOptionsArray, lengthof(SensitiveOptionsArray), SOURCE_MODE);

    /* Add source options */
    srcoptions = transformGenericOptions(DataSourceRelationId, PointerGetDatum(NULL), stmt->options, InvalidOid);

    if (PointerIsValid(DatumGetPointer(srcoptions)))
        values[Anum_pg_extension_data_source_srcoptions - 1] = srcoptions;
    else
        nulls[Anum_pg_extension_data_source_srcoptions - 1] = true;

    tuple = heap_form_tuple(rel->rd_att, values, nulls);

    srcId = simple_heap_insert(rel, tuple);

    CatalogUpdateIndexes(rel, tuple);

    tableam_tops_free_tuple(tuple);

    /* record dependencies */
    myself.classId = DataSourceRelationId;
    myself.objectId = srcId;
    myself.objectSubId = 0;

    recordDependencyOnOwner(DataSourceRelationId, srcId, ownerId);

    /* dependency on extension */
    recordDependencyOnCurrentExtension(&myself, false);

    /* Post creation hook for new data source */
    InvokeObjectAccessHook(OAT_POST_CREATE, DataSourceRelationId, srcId, 0, NULL);

    heap_close(rel, RowExclusiveLock);
}

/*
 * AlterDataSource:
 * 	Alter a Data Source, only allowed by its owner or a superuser!
 *
 * @IN stmt: Alter Data Source Stmt structure
 * @RETURN: void
 */
void AlterDataSource(AlterDataSourceStmt* stmt)
{
    Relation rel;
    HeapTuple tp;
    Datum repl_val[Natts_pg_extension_data_source];
    bool repl_null[Natts_pg_extension_data_source];
    bool repl_repl[Natts_pg_extension_data_source];
    Oid srcId;
    Form_pg_extension_data_source srcForm;
    errno_t ret;

    rel = heap_open(DataSourceRelationId, RowExclusiveLock);
    tp = SearchSysCacheCopy1(DATASOURCENAME, CStringGetDatum(stmt->srcname));
    if (!HeapTupleIsValid(tp))
        ereport(ERROR,
            (errmodule(MOD_EC),
                errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("data source \"%s\" does not exist", stmt->srcname)));

    srcId = HeapTupleGetOid(tp);
    srcForm = (Form_pg_extension_data_source)GETSTRUCT(tp);

    /*
     * Check Privileges: only owner or a superuser can ALTER a DATA SOURCE.
     */
    if (!pg_extension_data_source_ownercheck(srcId, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_DATA_SOURCE, stmt->srcname);

    ret = memset_s(repl_val, sizeof(repl_val), 0, sizeof(repl_val));
    securec_check(ret, "\0", "\0");
    ret = memset_s(repl_null, sizeof(repl_null), false, sizeof(repl_null));
    securec_check(ret, "\0", "\0");
    ret = memset_s(repl_repl, sizeof(repl_repl), false, sizeof(repl_repl));
    securec_check(ret, "\0", "\0");

    /*
     * Change the source TYPE string
     */
    if (stmt->srctype) {
        repl_val[Anum_pg_extension_data_source_srctype - 1] = CStringGetTextDatum(stmt->srctype);
        repl_repl[Anum_pg_extension_data_source_srctype - 1] = true;
    }

    /*
     * Change the source VERSION string
     */
    if (stmt->has_version) {
        if (stmt->version)
            repl_val[Anum_pg_extension_data_source_srcversion - 1] = CStringGetTextDatum(stmt->version);
        else
            repl_null[Anum_pg_extension_data_source_srcversion - 1] = true;

        repl_repl[Anum_pg_extension_data_source_srcversion - 1] = true;
    }

    /*
     * Change the options if supplied
     */
    if (stmt->options) {
        Datum datum;
        bool isnull = false;

        /* Extract the current srvoptions */
        datum = SysCacheGetAttr(DATASOURCEOID, tp, Anum_pg_extension_data_source_srcoptions, &isnull);
        if (isnull)
            datum = PointerGetDatum(NULL);

        /* Check options */
        check_source_generic_options((const List*)stmt->options);

        /* Encrypt sensitive options before any operations */
        EncryptGenericOptions(stmt->options, SensitiveOptionsArray, lengthof(SensitiveOptionsArray), SOURCE_MODE);

        /* Prepare the options array */
        datum = transformGenericOptions(DataSourceRelationId, datum, stmt->options, InvalidOid);
        if (PointerIsValid(DatumGetPointer(datum)))
            repl_val[Anum_pg_extension_data_source_srcoptions - 1] = datum;
        else
            repl_null[Anum_pg_extension_data_source_srcoptions - 1] = true;

        repl_repl[Anum_pg_extension_data_source_srcoptions - 1] = true;
    }

    /*
     * Everything looks good - update the tuple
     */
    tp = (HeapTuple) tableam_tops_modify_tuple(tp, RelationGetDescr(rel), repl_val, repl_null, repl_repl);

    simple_heap_update(rel, &tp->t_self, tp);
    CatalogUpdateIndexes(rel, tp);

    tableam_tops_free_tuple(tp);
    heap_close(rel, RowExclusiveLock);
}

/*
 * RemoveDataSourceById:
 * 	Remove a data source by Oid
 *
 * @IN src_Id: Oid of the data source to be removed
 * @RETURN: void
 */
void RemoveDataSourceById(Oid src_Id)
{
    HeapTuple tp;
    Relation rel;

    rel = heap_open(DataSourceRelationId, RowExclusiveLock);
    tp = SearchSysCache1(DATASOURCEOID, ObjectIdGetDatum(src_Id));
    if (!HeapTupleIsValid(tp))
        ereport(ERROR,
            (errmodule(MOD_EC),
                errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("cache lookup failed for data source %u", src_Id)));

    simple_heap_delete(rel, &tp->t_self);

    ReleaseSysCache(tp);

    heap_close(rel, RowExclusiveLock);
}

/*
 * RenameDataSource:
 * 	Rename a Data Source: only allowed by owner or a superuser!
 *
 * @IN oldname: old name
 * @IN newname: new name
 * @RETURN : void
 */
void RenameDataSource(const char* oldname, const char* newname)
{
    HeapTuple tup;
    Relation rel;

    rel = heap_open(DataSourceRelationId, RowExclusiveLock);

    /* make sure the old name does exist */
    tup = SearchSysCacheCopy1(DATASOURCENAME, CStringGetDatum(oldname));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR,
            (errmodule(MOD_EC),
                errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("data source \"%s\" does not exist", oldname)));

    /* make sure the new name doesn't exist */
    if (SearchSysCacheExists1(DATASOURCENAME, CStringGetDatum(newname)))
        ereport(ERROR,
            (errmodule(MOD_EC),
                errcode(ERRCODE_DUPLICATE_OBJECT),
                errmsg("data source \"%s\" already exists", newname)));

    /* must be owner of server */
    if (!pg_extension_data_source_ownercheck(HeapTupleGetOid(tup), GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_DATA_SOURCE, oldname);

    /* rename */
    (void)namestrcpy(&(((Form_pg_extension_data_source)GETSTRUCT(tup))->srcname), newname);
    simple_heap_update(rel, &tup->t_self, tup);
    CatalogUpdateIndexes(rel, tup);

    tableam_tops_free_tuple(tup);
    heap_close(rel, RowExclusiveLock);
}

/*
 * AlterDataSourceOwner_internal:
 * 	Internal workhorse for changing a data source's owner
 */
static void AlterDataSourceOwner_Internal(Relation rel, HeapTuple tup, Oid newOwnerId)
{
    Form_pg_extension_data_source form;

    form = (Form_pg_extension_data_source)GETSTRUCT(tup);

    /*
     * Must be a superuser to change a data source owner
     */
    if (!superuser())
        ereport(ERROR,
            (errmodule(MOD_EC),
                errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to change owner of data source \"%s\"", NameStr(form->srcname)),
                errhint("Must be system admin to change owner of a data source.")));

    /*
     * New owner must also be a superuser
     * Database Security:  Support separation of privilege.
     */
    if (!(superuser_arg(newOwnerId) || systemDBA_arg(newOwnerId)))
        ereport(ERROR,
            (errmodule(MOD_EC),
                errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("permission denied to change owner of data source \"%s\"", NameStr(form->srcname)),
                errhint("The owner of a data source must be a system admin.")));

    /* Change owner if not the current owner */
    if (form->srcowner != newOwnerId) {
        /* Change its owner */
        form->srcowner = newOwnerId;

        simple_heap_update(rel, &tup->t_self, tup);
        CatalogUpdateIndexes(rel, tup);

        /* Update owner dependency reference */
        changeDependencyOnOwner(DataSourceRelationId, HeapTupleGetOid(tup), newOwnerId);
    }
}

/*
 * AlterDataSourceOwner:
 * 	Change data source owner -- by name
 *
 * @IN name: data source name
 * @IN newOwnerId: new owner's Oid
 * @RETURN: void
 */
void AlterDataSourceOwner(const char* name, Oid newOwnerId)
{
    HeapTuple tup;
    Relation rel;

    rel = heap_open(DataSourceRelationId, RowExclusiveLock);
    tup = SearchSysCacheCopy1(DATASOURCENAME, CStringGetDatum(name));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR,
            (errmodule(MOD_EC), errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("data source \"%s\" does not exist", name)));

    AlterDataSourceOwner_Internal(rel, tup, newOwnerId);
    tableam_tops_free_tuple(tup);
    heap_close(rel, RowExclusiveLock);
}
