/*
* Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
* gsobject_gsdependencies.cpp
*
*
* IDENTIFICATION
*        src/common/backend/utils/gsplsql/gsobject_gsdependencies.cpp
*
* ---------------------------------------------------------------------------------------
 */

#include "utils/plpgsql.h"
#include "catalog/gs_dependencies_fn.h"
#include "utils/lsyscache.h"
#include "utils/pl_package.h"
#include "catalog/gs_package.h"
#include "catalog/pg_synonym.h"
#include "catalog/pg_proc.h"
#include "utils/fmgroids.h"
#include "catalog/gs_dependencies.h"
#include "catalog/gs_dependencies_obj.h"
#include "catalog/indexing.h"
#include "utils/builtins.h"
#include "catalog/pg_proc_fn.h"
#include "catalog/pg_object.h"
#include "access/sysattr.h"
#include "catalog/pg_type_fn.h"
#include "libpq/md5.h"
#include "catalog/dependency.h"
#include "utils/lsyscache.h"
#include "parser/parse_type.h"

static void gsplsql_update_ref_dep_obj_base_on_ast(const GsDependObjDesc *ref_obj_desc, HeapTuple obj_tuple,
                                            char *ref_obj_ast_str, bool ast_changed);
static Oid gsplsql_get_curr_compile_oid();
static void gsplsql_do_ref_obj_affect_oper(const GsDependObjDesc *ref_obj_desc, Relation gs_depend_rel, List *list,
                                    bool pre_undefined, bool ast_changed);
static void gsplsql_cascade_delete_type_by_type_name(GsDependObjDesc *obj_desc);
static void gsplsql_refresh_proc_header(Oid ref_obj_oid);
static void gsplsql_delete_dependencies_object_by_oid(Oid oid);
static bool gsplsql_update_proc_header(CreateFunctionStmt *stmt, Oid funcid);
static void gsplsql_rename_func_name(const char *nsp_name, const char *pkg_name, const char *old_func_name,
                              const char *new_func_name);
static void gsplsql_obj_func_name(const char *nsp_name, const char *pkg_name, const char *old_func_name,
                                  const char *new_func_name);

static bool is_proc_info_changed(Oid funcid, oidvector *new_parameter_types, ArrayType *new_all_parameter_types,
                                 char *new_parameter_defaults, Oid new_prorettype, bool new_returnsset);
static inline void gsplsql_delete_ref_obj_from_pkg(Oid ref_obj_oid);

bool gsplsql_is_undefined_func(Oid func_oid)
{
    GsDependObjDesc obj;
    Form_pg_proc procForm = NULL;
    gsplsql_init_gs_depend_obj_desc(&obj);
    HeapTuple tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_oid));
    if (!HeapTupleIsValid(tup)) {
       return true;
    }
    procForm = (Form_pg_proc)GETSTRUCT(tup);
    bool is_null;
    Datum proPackageIdDatum = SysCacheGetAttr(PROCOID, tup, Anum_pg_proc_packageid, &is_null);
    Oid proPackageId = DatumGetObjectId(proPackageIdDatum);
    obj = gsplsql_construct_func_head_obj(func_oid, procForm->pronamespace, proPackageId);
    obj.type = GSDEPEND_OBJECT_TYPE_PROCHEAD;
    ReleaseSysCache(tup);
    HeapTuple obj_tuple = gsplsql_search_object(&obj, false);
    if (!HeapTupleIsValid(obj_tuple)) {
       return false;
    }
    Datum ast_datum = gsplsql_get_depend_object_attr(obj_tuple, Anum_gs_dependencies_obj_objnode, &is_null);
    if (is_null) {
       return false;
    }
    char* ast_str = TextDatumGetCString(ast_datum);
    DependenciesProchead* ast_node = (DependenciesProchead*)stringToNode(ast_str);
    if (ast_node == NULL) {
       pfree_ext(ast_str);
       return true;
    }
    pfree_ext(ast_str);
    heap_freetuple(obj_tuple);
    pfree_ext(obj.name);
    pfree_ext(obj.schemaName);
    return ast_node->undefined;
}

void gsplsql_delete_unrefer_depend_obj_oid(Oid ref_obj_oid, bool skip_in_use)
{
    if (!OidIsValid(ref_obj_oid) ||
        (skip_in_use && list_member_oid(u_sess->plsql_cxt.usindDependObjOids, ref_obj_oid))) {
        return;
    }
    Relation rel;
    ScanKeyData key;
    rel = heap_open(DependenciesRelationId, AccessShareLock);
    ScanKeyInit(&key, Anum_gs_dependencies_refobjoid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(ref_obj_oid));
    SysScanDesc scan = systable_beginscan(rel, DependenciesRefOidIndexId, true, SnapshotSelf, 1, &key);
    HeapTuple tuple = systable_getnext(scan);
    if (!HeapTupleIsValid(tuple)) {
        gsplsql_delete_dependencies_object_by_oid(ref_obj_oid);
    }
    systable_endscan(scan);
    heap_close(rel, AccessShareLock);
}

bool gsplsql_search_depend_obj_by_oid(Oid oid, GsDependObjDesc* obj_desc)
{
    bool is_null = false;
    HeapTuple tuple = NULL;
    Relation relation;
    const int keyNum = 1;
    ScanKeyData key[keyNum];
    SysScanDesc scan = NULL;
    ScanKeyInit(&key[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ,
                ObjectIdGetDatum(oid));
    relation = heap_open(DependenciesObjRelationId, AccessShareLock);
    scan = systable_beginscan(relation, DependenciesObjOidIndexId, true, SnapshotSelf, keyNum, key);
    tuple = systable_getnext(scan);
    if (HeapTupleIsValid(tuple)) {
        Datum schemaNameDatum = heap_getattr(tuple, Anum_gs_dependencies_obj_schemaname,
                                        RelationGetDescr(relation), &is_null);
        Datum pkgNameDatum = heap_getattr(tuple, Anum_gs_dependencies_obj_packagename,
                                        RelationGetDescr(relation), &is_null);
        Datum nameDatum = heap_getattr(tuple, Anum_gs_dependencies_obj_name,
                                        RelationGetDescr(relation), &is_null);
        obj_desc->schemaName = pstrdup(DatumGetName(schemaNameDatum)->data);
        obj_desc->packageName = DatumGetName(pkgNameDatum)->data;
        if (0 == strcmp(obj_desc->packageName, "null")) {
            obj_desc->packageName = NULL;
        } else {
            obj_desc->packageName = pstrdup(obj_desc->packageName);
        }
        char* nameStr= TextDatumGetCString(nameDatum);
        obj_desc->name = pstrdup(nameStr);
        pfree_ext(nameStr);
        systable_endscan(scan);
        heap_close(relation, AccessShareLock);
        return true;
    }
    systable_endscan(scan);
    heap_close(relation, AccessShareLock);
    return false;
}

HeapTuple gsplsql_search_object_by_name(const char* schema_name, const char* package_name,
                                   const char*  object_name, GsDependObjectType type)
{
    GsDependObjDesc object;
    object.schemaName = (char*)schema_name;
    object.packageName = (char*)package_name;
    object.name = (char*)object_name;
    object.type = type;
    object.refPosType = GSDEPEND_REFOBJ_POS_INVALID;
    return gsplsql_search_object(&object, false);
}

HeapTuple gsplsql_search_object(const GsDependObjDesc* obj_desc, bool supp_undef_type)
{
    NameData schema_name_data;
    NameData pkg_name_data;
    StringInfoData object_name_data;
    gsplsql_construct_non_empty_obj(obj_desc, &schema_name_data, &pkg_name_data, &object_name_data);
    int keyNum = 0;
    ScanKeyData key[3];
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_obj_schemaname, BTEqualStrategyNumber, F_NAMEEQ, NameGetDatum(&schema_name_data));
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_obj_packagename, BTEqualStrategyNumber, F_NAMEEQ, NameGetDatum(&pkg_name_data));
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_obj_type, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(obj_desc->type));
    Relation relation = heap_open(DependenciesObjRelationId, AccessShareLock);
    SysScanDesc scan = systable_beginscan(relation, DependenciesObjNameIndexId, true, SnapshotSelf, keyNum, key);
    HeapTuple tuple;
    HeapTuple retTuple = NULL;
    /* compare the first partition'delta with main table */
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        bool is_null;
        Datum datum = heap_getattr(tuple, Anum_gs_dependencies_obj_name,
                                               RelationGetDescr(relation), &is_null);
        if (is_null) {
            continue;
        }
        char* obj_name = TextDatumGetCString(datum);
        if (0 == strcmp(object_name_data.data, obj_name)) {
            retTuple = heap_copytuple(tuple);
            pfree_ext(obj_name);
            break;
        }
        pfree_ext(obj_name);
    }
    systable_endscan(scan);
    heap_close(relation, AccessShareLock);
    FreeStringInfo(&object_name_data);
    if (!HeapTupleIsValid(retTuple) && supp_undef_type) {
        retTuple = gsplsql_search_object_by_name(obj_desc->schemaName, obj_desc->packageName,
                                                 obj_desc->name, GSDEPEND_OBJECT_TYPE_UNDEFIND);
    }
    return retTuple;
}

Datum gsplsql_get_depend_object_attr(HeapTuple tup, int attr_number, bool *is_null)
{
    Relation relation = heap_open(DependenciesObjRelationId, AccessShareLock);
    Datum datum = heap_getattr(tup, attr_number, RelationGetDescr(relation), is_null);
    heap_close(relation, AccessShareLock);
    return datum;
}

List* gsplsql_prepare_recompile_func(Oid func_oid, Oid schema_oid, Oid pkg_oid, bool is_recompile)
{
    if (!is_recompile || OidIsValid(pkg_oid)) {
        return NULL;
    }
    if (!enable_plpgsql_gsdependency()) {
        return NULL;
    }
    GsDependObjDesc obj_desc = gsplsql_construct_func_head_obj(func_oid, schema_oid, pkg_oid);
    obj_desc.type = GSDEPEND_OBJECT_TYPE_FUNCTION;
    obj_desc.refPosType = GSDEPEND_REFOBJ_POS_IN_PROCBODY;
    Relation rel = heap_open(DependenciesRelationId, RowExclusiveLock);
    List* list = gsplsql_delete_objs(rel, &obj_desc);
    heap_close(rel, RowExclusiveLock);
    return list;
}

void gsplsql_complete_recompile_func(List* list)
{
    if (list == NULL) {
        return;
    }
    if (!enable_plpgsql_gsdependency()) {
        return;
    }
    gsplsql_delete_unrefer_depend_obj_in_list(list, false);
    list_free(list);
}

Oid gsplsql_get_pkg_oid_by_func_oid(Oid func_oid)
{
    HeapTuple tuple = SearchSysCache((int)PROCOID, ObjectIdGetDatum(func_oid), 0, 0, 0);
    if (!HeapTupleIsValid(tuple)) {
        return InvalidOid;
    }
    bool is_null;
    Datum packageOidDatum = SysCacheGetAttr((int)PROCOID, tuple, Anum_pg_proc_packageid, &is_null);
    Oid pkg_oid = DatumGetObjectId(packageOidDatum);
    ReleaseSysCache(tuple);
    return pkg_oid;
}

Oid gsplsql_get_proc_oid(const char* schemaName, const char* packageName, const char* name)
{
    Oid expected_nsp_oid = GetSysCacheOid1(NAMESPACENAME, CStringGetDatum(schemaName));
    Oid expected_pkg_oid = InvalidOid;
    if (packageName != NULL) {
        expected_pkg_oid = GetSysCacheOid2(PKGNAMENSP, CStringGetDatum(packageName), ObjectIdGetDatum(expected_nsp_oid));
    }
    HeapTuple proc_tup = NULL;
    bool is_null = false;
    Form_pg_proc proc_form = NULL;
    const char* leftBracketPos = strchr(name, '(');
    if (leftBracketPos == NULL) {
        return InvalidOid;
    }
    StringInfoData procNameStr;
    initStringInfo(&procNameStr);
    appendBinaryStringInfo(&procNameStr, name, leftBracketPos - name);
    CatCList* catlist;
    catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(procNameStr.data));
    for (int i = 0; i < catlist->n_members; i++) {
        proc_tup = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
        if (HeapTupleIsValid(proc_tup)) {
            proc_form = (Form_pg_proc)GETSTRUCT(proc_tup);
            if (proc_form->pronamespace != expected_nsp_oid) {
                continue;
            }
            Datum proPackageIdDatum = SysCacheGetAttr(PROCOID, proc_tup, Anum_pg_proc_packageid, &is_null);
            Oid proPackageId = DatumGetObjectId(proPackageIdDatum);
            if (proPackageId != expected_pkg_oid) {
                continue;
            }
            Oid proc_oid = HeapTupleGetOid(proc_tup);
            char* dbFuncHeadName = format_procedure_no_visible(proc_oid);
            if (strcmp(name, dbFuncHeadName) == 0) {
                ReleaseSysCacheList(catlist);
                return proc_oid;
            }
        }
    }
    ReleaseSysCacheList(catlist);
    FreeStringInfo(&procNameStr);
    ereport(WARNING, (errmodule(MOD_PLSQL), errmsg("function %s does not exist", name)));
    return InvalidOid;
}

Oid gsplsql_get_proc_oid(const char* schemaName, const char* packageName, const char* name, const char* proc_arg_src)
{
    Oid expected_nsp_oid = GetSysCacheOid1(NAMESPACENAME, CStringGetDatum(schemaName));
    Oid expected_pkg_oid = InvalidOid;
    if (packageName != NULL) {
        expected_pkg_oid = GetSysCacheOid2(PKGNAMENSP, CStringGetDatum(packageName), ObjectIdGetDatum(expected_nsp_oid));
    }
    HeapTuple proc_tup = NULL;
    bool is_null = false;
    Form_pg_proc proc_form = NULL;
    CatCList* catlist;
    catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(name));
    for (int i = 0; i < catlist->n_members; i++) {
        proc_tup = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
        if (HeapTupleIsValid(proc_tup)) {
            proc_form = (Form_pg_proc)GETSTRUCT(proc_tup);
            if (proc_form->pronamespace != expected_nsp_oid) {
                continue;
            }
            Datum proPackageIdDatum = SysCacheGetAttr(PROCOID, proc_tup, Anum_pg_proc_packageid, &is_null);
            Oid proPackageId = DatumGetObjectId(proPackageIdDatum);
            if (proPackageId != expected_pkg_oid) {
                continue;
            }
            Oid proc_oid = HeapTupleGetOid(proc_tup);
            Datum proArgSrcdatum = SysCacheGetAttr(PROCOID, proc_tup, Anum_pg_proc_proargsrc, &is_null);
            if (is_null && proc_arg_src == NULL) {
                ReleaseSysCacheList(catlist);
                return proc_oid;
            }
            if (is_null || proc_arg_src == NULL) {
                continue;
            }
            char* pro_arg_src_in_db = TextDatumGetCString(proArgSrcdatum);
            if (strcmp(proc_arg_src, pro_arg_src_in_db) == 0) {
                pfree_ext(pro_arg_src_in_db);
                ReleaseSysCacheList(catlist);
                return proc_oid;
            }
            pfree_ext(pro_arg_src_in_db);
        }
    }
    ReleaseSysCacheList(catlist);
    ereport(WARNING, (errmodule(MOD_PLSQL), errmsg("function %s does not exist", name)));
    return InvalidOid;
}

char* gsplsql_do_refresh_proc_header(const GsDependObjDesc* obj_desc, bool* is_undefined)
{
    Oid funcid = InvalidOid;
    char* schema_name = obj_desc->schemaName;
    bool is_null = false;
    char* package_name = obj_desc->packageName;
    HeapTuple obj_tuple = gsplsql_search_object(obj_desc, false);
    if (!HeapTupleIsValid(obj_tuple)) {
        return NULL;
    }
    Datum ast_datum = gsplsql_get_depend_object_attr(obj_tuple, Anum_gs_dependencies_obj_objnode, &is_null);
    char* ast_str = TextDatumGetCString(ast_datum);
    DependenciesProchead* ast_node = (DependenciesProchead*)stringToNode(ast_str);
    pfree_ext(ast_str);
    char* func_name = ast_node->proName;
    if (package_name == NULL || strcmp(package_name, "null") == 0) {
        funcid = gsplsql_get_proc_oid(schema_name, NULL, func_name, ast_node->proArgSrc);
    } else {
        funcid = gsplsql_get_proc_oid(schema_name, package_name, func_name, ast_node->proArgSrc);
    }
    if (!OidIsValid(funcid)) {
        heap_freetuple(obj_tuple);
        return NULL;
    }
    StringInfoData  proc_src;
    initStringInfo(&proc_src);
    if (ast_node->funcHeadSrc != NULL) {
        appendStringInfoString(&proc_src, ast_node->funcHeadSrc);
    } else {
        ereport(ERROR,  (errcode(ERRCODE_UNDEFINED_OBJECT),
                          errmsg("The header information of procedure %s is null.", func_name)));
    }
    appendStringInfoString(&proc_src, "\n");
    appendStringInfoString(&proc_src, "is begin null; end;");
    volatile bool has_undefined = false;
    CreatePlsqlType oldCreatePlsqlType = u_sess->plsql_cxt.createPlsqlType;
    FunctionStyleType oldFunctionStyleType = u_sess->plsql_cxt.functionStyleType;
    bool oldNeedCreateDepend = u_sess->plsql_cxt.need_create_depend;
    Oid oldCurrRefreshPkgOid = u_sess->plsql_cxt.currRefreshPkgOid;
    Oid oldCurrObjectNspoid = u_sess->plsql_cxt.curr_object_nspoid;
    PG_TRY();
    {
        u_sess->plsql_cxt.createPlsqlType = CREATE_PLSQL_TYPE_START;
        set_create_plsql_type_not_check_nsp_oid();
        u_sess->plsql_cxt.functionStyleType = FUNCTION_STYLE_TYPE_REFRESH_HEAD;
        u_sess->plsql_cxt.need_create_depend = true;
        u_sess->plsql_cxt.currRefreshPkgOid = gsplsql_get_pkg_oid_by_func_oid(funcid);
        HeapTuple procTup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
        if (!HeapTupleIsValid(procTup)) {
            ereport(ERROR,  (errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                            errmsg("cache lookup failed for function %u", funcid)));
        }
        Form_pg_proc procForm = (Form_pg_proc)GETSTRUCT(procTup);
        u_sess->plsql_cxt.curr_object_nspoid = procForm->pronamespace;
        ReleaseSysCache(procTup);
        OverrideSearchPath* overrideSearchPath = GetOverrideSearchPath(CurrentMemoryContext);
        overrideSearchPath->schemas = lcons_oid(u_sess->plsql_cxt.curr_object_nspoid, overrideSearchPath->schemas);
        PushOverrideSearchPath(overrideSearchPath);
        list_free(overrideSearchPath->schemas);
        pfree_ext(overrideSearchPath);
        List* stmtList = raw_parser(proc_src.data, NULL);
        CreateFunctionStmt* stmt = (CreateFunctionStmt*)linitial(stmtList);
        u_sess->plsql_cxt.createPlsqlType = CREATE_PLSQL_TYPE_START;
        set_create_plsql_type_not_check_nsp_oid();
        has_undefined = gsplsql_update_proc_header(stmt, funcid);
        if (enable_plpgsql_gsdependency_guc()) {
            u_sess->plsql_cxt.createPlsqlType = oldCreatePlsqlType;
        }
        u_sess->plsql_cxt.functionStyleType = oldFunctionStyleType;
        u_sess->plsql_cxt.need_create_depend = oldNeedCreateDepend;
        u_sess->plsql_cxt.currRefreshPkgOid = oldCurrRefreshPkgOid;
        u_sess->plsql_cxt.curr_object_nspoid = oldCurrObjectNspoid;
        PopOverrideSearchPath();
    }
    PG_CATCH();
    {
        u_sess->plsql_cxt.currRefreshPkgOid = oldCurrRefreshPkgOid;
        if (enable_plpgsql_gsdependency_guc()) {
            u_sess->plsql_cxt.createPlsqlType = oldCreatePlsqlType;
        }
        u_sess->plsql_cxt.functionStyleType = oldFunctionStyleType;
        u_sess->plsql_cxt.need_create_depend = oldNeedCreateDepend;
        u_sess->plsql_cxt.curr_object_nspoid = oldCurrObjectNspoid;
        PopOverrideSearchPath();
        PG_RE_THROW();
    }
    PG_END_TRY();
    // updates the AST status, which is used for automatic recompillation
    // the data queried last time cannot be used. There is a bug in nested compilation
    HeapTuple objTupleNew = gsplsql_search_object(obj_desc, false);
    if (!HeapTupleIsValid(objTupleNew)) {
        return NULL;
    }
    Datum astDatumNew = gsplsql_get_depend_object_attr(objTupleNew, Anum_gs_dependencies_obj_objnode, &is_null);
    char* astStrNew = TextDatumGetCString(astDatumNew);
    DependenciesProchead* astNodeNew = (DependenciesProchead*)stringToNode(astStrNew);
    pfree_ext(astStrNew);
    bool undefined_in_db = astNodeNew->undefined;
    if (is_undefined != NULL) {
        *is_undefined = has_undefined;
    }
    if (has_undefined != undefined_in_db) {
        ast_node->undefined = has_undefined;
        if (ast_node->funcHeadSrc == NULL) {
            ereport(ERROR,  (errcode(ERRCODE_UNDEFINED_OBJECT),
                            errmsg("The header info of procedure %s is null.", func_name)));
        }
        Datum values[Natts_gs_dependencies_obj];
        bool nulls[Natts_gs_dependencies_obj] = {false};
        bool replaces[Natts_gs_dependencies_obj] = {false};
        values[Anum_gs_dependencies_obj_objnode -1] = CStringGetTextDatum(nodeToString(ast_node));
        replaces[Anum_gs_dependencies_obj_objnode -1] = true;
        Relation relation = heap_open(DependenciesObjRelationId, RowExclusiveLock);
        HeapTuple newTuple = heap_modify_tuple(objTupleNew, RelationGetDescr(relation), values, nulls, replaces);
        (void)simple_heap_update(relation, &objTupleNew->t_self, newTuple);
        CatalogUpdateIndexes(relation, newTuple);
        heap_freetuple(newTuple);
        heap_close(relation, RowExclusiveLock);
        CommandCounterIncrement();
    }
    heap_freetuple(objTupleNew);
    char* new_func_head_name = format_procedure_no_visible(funcid);
    //The function header info may change. The obj name is updated accordingly.
    gsplsql_rename_func_name(schema_name, package_name, obj_desc->name, new_func_head_name);
    gsplsql_obj_func_name(schema_name, package_name, obj_desc->name, new_func_head_name);
    return new_func_head_name;
}

void gsplsql_delete_unrefer_depend_obj_in_list(List* ref_obj_oid_list, bool skip_in_use)
{
    ListCell* lc = NULL;
    foreach(lc, ref_obj_oid_list) {
        Oid ref_obj_oid = lfirst_oid(lc);
        gsplsql_delete_unrefer_depend_obj_oid(ref_obj_oid, skip_in_use);
    }
}

Oid gsplsql_insert_dependencies_object(const GsDependObjDesc *object, char *obj_ast_str)
{
    Datum values[Natts_gs_dependencies_obj];
    bool nulls[Natts_gs_dependencies_obj] = {false};
    NameData  schemaNameData;
    NameData  packageNameData;
    StringInfoData objectNameData;
    HeapTuple tup;
    Oid tupOid;
    Relation rel;
    TupleDesc tupDesc = NULL;
    gsplsql_construct_non_empty_obj(object, &schemaNameData, &packageNameData, &objectNameData);
    values[Anum_gs_dependencies_obj_schemaname - 1] = NameGetDatum(&schemaNameData);
    values[Anum_gs_dependencies_obj_packagename - 1] = NameGetDatum(&packageNameData);
    values[Anum_gs_dependencies_obj_name - 1] = CStringGetTextDatum(objectNameData.data);
    values[Anum_gs_dependencies_obj_type - 1] = Int32GetDatum(object->type);
    values[Anum_gs_dependencies_obj_objnode - 1] = CStringGetTextDatum(obj_ast_str);
    rel = heap_open(DependenciesObjRelationId, RowExclusiveLock);
    tupDesc = RelationGetDescr(rel);
    tup = heap_form_tuple(tupDesc, values, nulls);
    (void)simple_heap_insert(rel, tup);
    CatalogUpdateIndexes(rel, tup);
    tupOid = HeapTupleGetOid(tup);
    heap_close(rel, RowExclusiveLock);
    FreeStringInfo(&objectNameData);
    return tupOid;
}

void gsplsql_remove_dependencies_object(const GsDependObjDesc* obj_desc)
{
    HeapTuple tuple = NULL;
    Relation relation = NULL;
    SysScanDesc scan = NULL;
    bool is_null = false;
    NameData schema_name_data;
    NameData pkg_name_data;
    StringInfoData object_name_data;
    gsplsql_construct_non_empty_obj(obj_desc, &schema_name_data, &pkg_name_data, &object_name_data);
    int keyNum = 0;
    ScanKeyData key[3];
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_obj_schemaname, BTEqualStrategyNumber, F_NAMEEQ, NameGetDatum(&schema_name_data));
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_obj_packagename, BTEqualStrategyNumber, F_NAMEEQ, NameGetDatum(&pkg_name_data));
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_obj_type, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(obj_desc->type));
    relation = heap_open(DependenciesObjRelationId, RowExclusiveLock);
    scan = systable_beginscan(relation, DependenciesObjNameIndexId, true, SnapshotSelf, keyNum, key);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        Datum datum = heap_getattr(tuple, Anum_gs_dependencies_obj_name,
                                               RelationGetDescr(relation), &is_null);
        if (is_null) {
            continue;
        }
        char* obj_name = TextDatumGetCString(datum);
        if (0 == strcmp(object_name_data.data, obj_name)) {
            pfree_ext(obj_name);
            break;
        }
        pfree_ext(obj_name);
    }
    if (HeapTupleIsValid(tuple)) {
        simple_heap_delete(relation, &tuple->t_self, 0, true);
        CommandCounterIncrement();
    }
    systable_endscan(scan);
    heap_close(relation, RowExclusiveLock);
    FreeStringInfo(&object_name_data);
}

bool gsplsql_is_object_depend(Oid oid, GsDependObjectType type)
{
    if (!OidIsValid(oid)) {
        return false;
    }
    GsDependObjDesc obj;
    obj.schemaName = NULL;
    bool result = false;
    switch (type)
    {
        case GSDEPEND_OBJECT_TYPE_TYPE:
            gsplsql_get_depend_obj_by_typ_id(&obj, oid, InvalidOid);
            obj.refPosType = GSDEPEND_REFOBJ_POS_IN_TYPE;
            break;
        default:
            return result;
    }
    if (NULL == obj.schemaName) {
        return false;
    }
    HeapTuple obj_tup = gsplsql_search_object(&obj, false);
    if (HeapTupleIsValid(obj_tup)) {
        heap_freetuple(obj_tup);
        result = true;
    }
    if (!result) {
        result = gsplsql_exist_dependency(&obj);
    }
    pfree_ext(obj.schemaName);
    pfree_ext(obj.packageName);
    pfree_ext(obj.name);
    return result;
}

List* gsplsql_get_depend_obj_list_by_specified_pkg(const char* schema_name, const char* package_name,
                                                   GsDependObjectType type)
{
    int keyNum = 0;
    ScanKeyData key[2];
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_obj_schemaname, BTEqualStrategyNumber, F_NAMEEQ,
                NULL != schema_name ? NameGetDatum(schema_name) : NameGetDatum("null"));
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_obj_packagename, BTEqualStrategyNumber, F_NAMEEQ,
                NULL != package_name ? NameGetDatum(package_name) : NameGetDatum("null"));
    Relation relation = heap_open(DependenciesObjRelationId, AccessShareLock);
    SysScanDesc scan = systable_beginscan(relation, DependenciesObjNameIndexId, true, SnapshotSelf, keyNum, key);
    HeapTuple tuple;
    Datum obj_oid_datum;
    bool is_null = true;
    List* list = NIL;
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {

        Datum obj_type_datum = heap_getattr(tuple, Anum_gs_dependencies_obj_type,
                                   RelationGetDescr(relation), &is_null);
        if (DatumGetInt32(obj_type_datum) == GSDEPEND_OBJECT_TYPE_PROCHEAD) {
            continue;
        }
        obj_oid_datum = heap_getattr(tuple, ObjectIdAttributeNumber,
                                     RelationGetDescr(relation), &is_null);

        if (type == GSDEPEND_OBJECT_TYPE_PKG || type == GSDEPEND_OBJECT_TYPE_PKG_RECOMPILE) {
            if (DatumGetInt32(obj_type_datum) != GSDEPEND_OBJECT_TYPE_FUNCTION) {
                list = lappend_oid(list, DatumGetObjectId(obj_oid_datum));
            }
        }  else {
            if (DatumGetInt32(obj_type_datum) == type) {
                list = lappend_oid(list, DatumGetObjectId(obj_oid_datum));
            }
        }
    }
    systable_endscan(scan);
    heap_close(relation, AccessShareLock);
    return list;
}

bool gsplsql_remove_ref_dependency(const GsDependObjDesc* ref_obj_desc)
{
    Oid oid = InvalidOid;
    if (ref_obj_desc->type == GSDEPEND_OBJECT_TYPE_PKG ||
        ref_obj_desc->type == GSDEPEND_OBJECT_TYPE_PKG_BODY) {
        List* list = gsplsql_get_depend_obj_list_by_specified_pkg(ref_obj_desc->schemaName,
            ref_obj_desc->packageName, ref_obj_desc->type);
        ListCell* cell = NULL;
        foreach(cell, list) {
            Oid ref_obj_oid = lfirst_oid(cell);
            gsplsql_remove_depend_obj_by_specified_oid(ref_obj_oid, gsplsql_get_curr_compile_oid());
        }
        list_free(list);
        return true;
    }
    HeapTuple obj_tuple = gsplsql_search_object(ref_obj_desc, false);
    if (!HeapTupleIsValid(obj_tuple)) {
        return false;
    }
    oid = HeapTupleGetOid(obj_tuple);
    heap_freetuple(obj_tuple);
    gsplsql_remove_depend_obj_by_specified_oid(oid, gsplsql_get_curr_compile_oid());
    return true;
}

void gsplsql_remove_depend_obj_by_specified_oid(Oid ref_obj_oid, Oid curr_compile_oid, bool is_same_pkg)
{
    const int keyNum = 1;
    ScanKeyData key[keyNum];
    ScanKeyInit(&key[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(ref_obj_oid));
    Relation relation = heap_open(DependenciesObjRelationId, RowExclusiveLock);
    SysScanDesc scan = systable_beginscan(relation, DependenciesObjOidIndexId, true, SnapshotSelf, keyNum, key);
    HeapTuple tuple = systable_getnext(scan);
    if (!HeapTupleIsValid(tuple)) {
        systable_endscan(scan);
        heap_close(relation, RowExclusiveLock);
    }
    Datum values[Natts_gs_dependencies_obj];
    bool nulls[Natts_gs_dependencies_obj] = {false};
    bool replaces[Natts_gs_dependencies_obj] = {false};
    DependenciesUndefined* undefinedNode = makeNode(DependenciesUndefined);
    char* ast_str = nodeToString(undefinedNode);
    pfree_ext(undefinedNode);
    values[Anum_gs_dependencies_obj_objnode -1] = CStringGetTextDatum(ast_str);
    replaces[Anum_gs_dependencies_obj_objnode -1] = true;
    HeapTuple new_tuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);
    (void)simple_heap_update(relation, &new_tuple->t_self, new_tuple, true);
    CatalogUpdateIndexes(relation, new_tuple);
    heap_freetuple(new_tuple);
    systable_endscan(scan);
    heap_close(relation, RowExclusiveLock);
    pfree_ext(ast_str);
    CommandCounterIncrement();
    gsplsql_refresh_proc_header(ref_obj_oid);
    if (!is_same_pkg) {
        gsplsql_invalidate_objs(ref_obj_oid, true,
            OidIsValid(curr_compile_oid) ? curr_compile_oid : gsplsql_get_curr_compile_oid());
    }
}

void gsplsql_get_depend_obj_by_typ_id(GsDependObjDesc* ref_obj, Oid typ_oid,
    Oid pkg_oid, bool drop_typ)
{
    gsplsql_init_gs_depend_obj_desc(ref_obj);
    if (!OidIsValid(typ_oid)) {
        return;
    }
    Oid rel_oid = get_typ_typrelid(typ_oid);
    if (OidIsValid(rel_oid)) {
        char rel_kind = get_rel_relkind(rel_oid);
        switch (rel_kind)
        {
            case RELKIND_RELATION:
            case RELKIND_FOREIGN_TABLE:
            case RELKIND_COMPOSITE_TYPE:
            case RELPERSISTENCE_PERMANENT:
            case RELPERSISTENCE_UNLOGGED:
            case RELPERSISTENCE_GLOBAL_TEMP:
                break;
            default:
                if (!drop_typ || rel_kind != '\0') {
                    return;
                }
        }
    }
    ref_obj->schemaName = get_typenamespace(typ_oid);
    ref_obj->name = get_typename(typ_oid);
    Oid cur_pkg_oid = GetTypePackageOid(typ_oid);
    if (!OidIsValid(cur_pkg_oid)) {
        cur_pkg_oid = pkg_oid;
    }
    if (OidIsValid(cur_pkg_oid)) {
        ref_obj->packageName = GetPackageName(cur_pkg_oid);
        char* real_name = ParseTypeName(ref_obj->name, cur_pkg_oid);
        pfree_ext(ref_obj->name);
        ref_obj->name = real_name;
    } else {
        ref_obj->packageName = pstrdup("null");
    }
    ref_obj->refPosType = GSDEPEND_REFOBJ_POS_INVALID;
    ref_obj->type = GSDEPEND_OBJECT_TYPE_TYPE;
}

bool gsplsql_build_ref_type_dependency(Oid typ_oid)
{
    if (!enable_plpgsql_gsdependency_guc()) {
        return false;
    }
    Oid curr_pkg_oid = InvalidOid;
    int cw = CompileWhich();
    if (cw == PLPGSQL_COMPILE_PACKAGE_PROC || cw == PLPGSQL_COMPILE_PACKAGE) {
        curr_pkg_oid = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid;
    }
    GsDependObjDesc ref_obj;
    gsplsql_get_depend_obj_by_typ_id(&ref_obj, typ_oid, curr_pkg_oid);
    if (ref_obj.type == GSDEPEND_OBJECT_TYPE_INVALID) {
        return false;
    }
    bool has_undefined = false;
    DependenciesDatum* datum = gsplsql_make_depend_datum_by_type_oid(typ_oid, &has_undefined);
    bool ret = gsplsql_build_ref_dependency(&ref_obj, datum);
    pfree_ext(datum);
    free_gs_depend_obj_desc(&ref_obj);
    return ret;
}

bool gsplsql_build_ref_dependency(Oid nsp_oid, const PLpgSQL_datum* ref_datum, const char* pkg_name,
                                  List** ref_obj_oids, Oid curr_compile_oid)
{
    if (ref_datum == NULL) {
        return false;
    }
    GsDependObjDesc ref_obj_desc = {"", "", ""};
    bool succeed = gsplsql_parse_type_and_name_from_gsplsql_datum(ref_datum, &ref_obj_desc.name, &ref_obj_desc.type);
    if (!succeed) {
        return false;
    }
    ref_obj_desc.schemaName = get_namespace_name(nsp_oid);
    if (pkg_name != NULL) {
        ref_obj_desc.packageName = (char*)pkg_name;
    }
    bool depend_undefined = false;
    DependenciesDatum* ref_obj_ast = gsplsql_make_depend_datum_from_plsql_datum(ref_datum, NULL, &depend_undefined);
    bool ret = gsplsql_build_ref_dependency(&ref_obj_desc, ref_obj_ast, ref_obj_oids, curr_compile_oid);
    pfree_ext(ref_obj_desc.schemaName);
    if (depend_undefined) {
        if (GetCurrCompilePgObjStatus()) {
            ereport(WARNING,  (errcode(ERRCODE_UNDEFINED_OBJECT),
                            errmsg("Object %s depends on an undefined type.", ref_obj_desc.name)));
        }
        InvalidateCurrCompilePgObj();
    }
    return ret;
}

bool gsplsql_build_ref_dependency(const GsDependObjDesc* ref_obj_desc, const DependenciesDatum* ref_obj_ast,
                                  List** ref_obj_oids, Oid curr_compile_oid)
{
    HeapTuple obj_tuple = gsplsql_search_object(ref_obj_desc, true);
    if (!HeapTupleIsValid(obj_tuple)) {
        return false;
    }
    Oid ref_obj_oid = HeapTupleGetOid(obj_tuple);
    if (ref_obj_oids != NULL) {
        *ref_obj_oids = list_delete_oid(*ref_obj_oids, ref_obj_oid);
    }
    gsplsql_delete_ref_obj_from_pkg(ref_obj_oid);
    Relation gs_depend_rel;
    List* list = NIL;
    gs_depend_rel = heap_open(DependenciesRelationId, AccessShareLock);
    list = gsplsql_search_gs_depend_rel_by_oid(gs_depend_rel, ref_obj_oid);
    if (list == NULL) {
        heap_close(gs_depend_rel, AccessShareLock);
        return false;
    }
    bool pre_undefined = false;
    bool ast_changed = false;
    char* ref_obj_ast_str = NULL;
    if (ref_obj_ast != NULL) {
        ref_obj_ast_str = nodeToString(ref_obj_ast);
        bool is_null;
        Datum ast_datum = gsplsql_get_depend_object_attr(obj_tuple, Anum_gs_dependencies_obj_objnode, &is_null);
        if (is_null) {
            ast_changed = true;
        } else {
            char* pre_ast_str = TextDatumGetCString(ast_datum);
            DependenciesUndefined* undefined_ast = makeNode(DependenciesUndefined);
            char* undefined_ast_str = nodeToString(undefined_ast);
            pre_undefined = (0 == strcmp(undefined_ast_str, pre_ast_str));
            if (strcmp(ref_obj_ast_str, pre_ast_str) != 0) {
                ast_changed = true;
            }
            if (pre_undefined && strcmp(ref_obj_ast_str, undefined_ast_str) == 0) {
                ast_changed = true;
            }
            pfree_ext(pre_ast_str);
            pfree_ext(undefined_ast);
            pfree_ext(undefined_ast_str);
        }
    } else {
        ast_changed = true;
    }
    gsplsql_update_ref_dep_obj_base_on_ast(ref_obj_desc, obj_tuple, ref_obj_ast_str, ast_changed);
    heap_freetuple(obj_tuple);
    pfree_ext(ref_obj_ast_str);
    gsplsql_do_ref_obj_affect_oper(ref_obj_desc, gs_depend_rel, list, pre_undefined, ast_changed);
    heap_close(gs_depend_rel, AccessShareLock);
    if (ast_changed) {
        gsplsql_invalidate_objs(ref_obj_oid, false,
                                OidIsValid(curr_compile_oid) ? curr_compile_oid : gsplsql_get_curr_compile_oid());
    }
    return true;
}

Oid  gsplsql_update_object_ast(const GsDependObjDesc* object, const DependenciesDatum* ref_obj_ast)
{
    Datum values[Natts_gs_dependencies_obj];
    bool nulls[Natts_gs_dependencies_obj] = {false};
    bool replaces[Natts_gs_dependencies_obj] = {false};
    bool is_null = false;
    Oid oid;
    HeapTuple newTuple = NULL;
    char* ref_obj_ast_str = nodeToString(ref_obj_ast);
    HeapTuple tup = gsplsql_search_object(object, NULL == ref_obj_ast || T_DependenciesUndefined == ref_obj_ast->type);
    if (!HeapTupleIsValid(tup)) {
        oid = gsplsql_insert_dependencies_object(object, ref_obj_ast_str);
        pfree_ext(ref_obj_ast_str);
        return oid;
    }
    oid = HeapTupleGetOid(tup);
    Datum ast_datum = gsplsql_get_depend_object_attr(tup, Anum_gs_dependencies_obj_objnode, &is_null);
    char* ast_str = TextDatumGetCString(ast_datum);
    if (strcmp(ast_str, ref_obj_ast_str) == 0) {
        pfree_ext(ref_obj_ast_str);
        pfree_ext(ast_str);
        heap_freetuple(tup);
        return oid;
    }
    replaces[Anum_gs_dependencies_obj_objnode -1] = true;
    values[Anum_gs_dependencies_obj_objnode -1] = CStringGetTextDatum(ref_obj_ast_str);
    Relation relation = heap_open(DependenciesObjRelationId, RowExclusiveLock);
    newTuple = heap_modify_tuple(tup, RelationGetDescr(relation), values, nulls, replaces);
    simple_heap_update(relation, &tup->t_self, newTuple, true);
    CatalogUpdateIndexes(relation, newTuple);
    heap_freetuple(tup);
    heap_freetuple(newTuple);
    heap_close(relation, RowExclusiveLock);
    CommandCounterIncrement();
    return oid;
}

void free_gs_depend_obj_desc(GsDependObjDesc* obj_desc)
{
    if (obj_desc != NULL) {
        pfree_ext(obj_desc->schemaName);
        pfree_ext(obj_desc->packageName);
        pfree_ext(obj_desc->name);
    }
}

Oid gsplsql_flush_undef_ref_type_dependency(Oid type_oid)
{
    if (!enable_plpgsql_gsdependency_guc()) {
        return false;
    }
    Oid cur_pkg_oid = InvalidOid;
    int cw = CompileWhich();
    if (cw == PLPGSQL_COMPILE_PACKAGE || cw == PLPGSQL_COMPILE_PACKAGE_PROC) {
        cur_pkg_oid = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid;
    }
    GsDependObjDesc ref_obj_desc;
    gsplsql_get_depend_obj_by_typ_id(&ref_obj_desc, type_oid, cur_pkg_oid);
    if (ref_obj_desc.type == GSDEPEND_OBJECT_TYPE_INVALID) {
        free_gs_depend_obj_desc(&ref_obj_desc);
        return false;
    }
    HeapTuple obj_tuple = gsplsql_search_object(&ref_obj_desc, true);
    if (!HeapTupleIsValid(obj_tuple)) {
        free_gs_depend_obj_desc(&ref_obj_desc);
        return false;
    }
    Oid ref_obj_oid = HeapTupleGetOid(obj_tuple);
    heap_freetuple(obj_tuple);
    gsplsql_delete_ref_obj_from_pkg(ref_obj_oid);
    free_gs_depend_obj_desc(&ref_obj_desc);
    return false;
}

Oid get_compiled_object_nspoid()
{
    int rc = CompileWhich();
    Oid nspOid = InvalidOid;
    if (rc == PLPGSQL_COMPILE_PACKAGE || rc == PLPGSQL_COMPILE_PACKAGE_PROC) {
        nspOid = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->namespaceOid;
    } else if (rc == PLPGSQL_COMPILE_PROC) {
        nspOid = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile->namespaceOid;
    } else if (OidIsValid(u_sess->plsql_cxt.curr_object_nspoid)) {
        nspOid = u_sess->plsql_cxt.curr_object_nspoid;
    } else {
        nspOid = GetOidBySchemaName();
    }
    return nspOid;
}

bool gsplsql_check_type_depend_ast_equal(Relation obj_rel, Oid obj_oid, const char* equal_ast)
{
    ScanKeyData key;
    ScanKeyInit(&key, ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(obj_oid));
    SysScanDesc scan = systable_beginscan(obj_rel, DependenciesObjOidIndexId, true, SnapshotSelf, 1, &key);
    HeapTuple tuple = systable_getnext(scan);
    if (!HeapTupleIsValid(tuple)) {
        systable_endscan(scan);
        ereport(WARNING,  (errcode(ERRCODE_UNDEFINED_OBJECT),
                      errmsg("dependencies object %u does not exist.", obj_oid)));
        return false;
    }
    bool is_null = false;
    Datum ast_datum = heap_getattr(tuple, Anum_gs_dependencies_obj_objnode,
                                   RelationGetDescr(obj_rel), &is_null);
    if (is_null) {
        systable_endscan(scan);
        return false;
    }
    char* pre_ast_str = TextDatumGetCString(ast_datum);
    bool ret = strcmp(equal_ast, pre_ast_str) == 0;
    pfree_ext(pre_ast_str);
    systable_endscan(scan);
    return ret;
}

/*
 * static func
 */
static void gsplsql_cascade_delete_type_by_type_name(GsDependObjDesc *obj_desc)
{
    ObjectAddress address;
    Oid typeOid = InvalidOid;
    if (obj_desc->refPosType == GSDEPEND_REFOBJ_POS_IN_TYPE) {
        typeOid = TypeNameGetOid(obj_desc->schemaName, obj_desc->packageName, obj_desc->name);
        if (!OidIsValid(typeOid)) {
            return;
        }
        address.classId = TypeRelationId;
        address.objectId = typeOid;
        address.objectSubId = 0;
        performDeletion(&address, DROP_CASCADE, PERFORM_DELETION_INTERNAL);
    }
}

static inline void gsplsql_delete_ref_obj_from_pkg(Oid ref_obj_oid)
{
    int rc = CompileWhich();
    if (rc == PLPGSQL_COMPILE_PACKAGE || rc == PLPGSQL_COMPILE_PACKAGE_PROC) {
       PLpgSQL_package* pkg = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package;
       pkg->preSelfObjectList = list_delete_oid(pkg->preSelfObjectList, ref_obj_oid);
    }
}

static Oid gsplsql_get_curr_compile_oid()
{
    Oid oid = InvalidOid;
    int rc = CompileWhich();
    switch (rc) {
        case PLPGSQL_COMPILE_PACKAGE:
        case PLPGSQL_COMPILE_PACKAGE_PROC: {
            PLpgSQL_package *pkg = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package;
            oid = pkg->pkg_oid;
        } break;
        case PLPGSQL_COMPILE_PROC: {
            PLpgSQL_function *func = u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile;
            oid = func->fn_oid;
        } break;
        default:
            break;
    }
    return oid;
}

/*
 * update proc header and the values of some fields in pg_proc
 * this methods copies part of the code of ProcedureCreate and CreateFunction
 */
static bool gsplsql_update_proc_header(CreateFunctionStmt *stmt, Oid funcid)
{
    Oid language_oid = InvalidOid;
    int all_param_count;
    char* alquery_string = stmt->queryStr;
    oidvector* parameter_types = NULL;
    TypeDependExtend* param_type_depend_ext = NULL;
    TypeDependExtend* ret_type_depend_ext = NULL;
    ArrayType* all_parameter_types = NULL;
    ArrayType* parameter_modes = NULL;
    ArrayType* parameter_names = NULL;
    Datum values[Natts_pg_proc];
    bool nulls[Natts_pg_proc] = {false};
    bool replaces[Natts_pg_proc] = {false};
    List* parameter_defaults = NIL;
    Oid required_result_type;
    List* defargpos = NIL;
    int parameter_count;
#ifdef ENABLE_MULTI_NODES
    bool fenced = true;
#else
    bool fenced = false;
#endif
    Oid prorettype = InvalidOid;
    bool returnsSet = false;
    List* parameters = stmt->parameters;
    bool has_undefined = false;

    examine_parameter_list(parameters, language_oid, alquery_string, &parameter_types, &param_type_depend_ext,
                           &all_parameter_types, &parameter_modes, &parameter_names, &parameter_defaults,
                           &required_result_type, &defargpos, fenced, &has_undefined);
    pfree_ext(param_type_depend_ext);
    if (stmt->returnType) {
        InstanceTypeNameDependExtend(&ret_type_depend_ext);
        /* explicit RETURNS clause */
        compute_return_type(stmt->returnType, language_oid, &prorettype, &returnsSet, fenced, stmt->startLineNumber,
                            ret_type_depend_ext, true);
        if (ret_type_depend_ext->dependUndefined) {
            has_undefined = true;
        }
        pfree_ext(ret_type_depend_ext);
    } else if (OidIsValid(required_result_type)) {
        /* default RETURNS clause from OUT parameters */
        prorettype = required_result_type;
        returnsSet = false;
    } else {
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_FUNCTION_DEFINITION), errmsg("function result type must be specified")));
        /* Alternative possibility: default to RETURNS VOID */
        prorettype = VOIDOID;
        returnsSet = false;
    }
    char* parameter_defaults_str = NULL;
    if (parameter_defaults != NIL) {
        parameter_defaults_str = nodeToString(parameter_defaults);
    }
    parameter_count = parameter_types->dim1;
    if (parameter_count < 0 || parameter_count > FUNC_MAX_ARGS) {
        ereport(ERROR,
                (errcode(ERRCODE_TOO_MANY_ARGUMENTS),
                 errmsg_plural("functions cannot have more than %d argument",
                               "functions cannot have more than %d arguments",
                               FUNC_MAX_ARGS,
                               FUNC_MAX_ARGS)));
    }
    if (all_parameter_types != PointerGetDatum(NULL)) {
        ArrayType* all_param_array = (ArrayType*)DatumGetPointer(all_parameter_types);
        all_param_count = ARR_DIMS(all_param_array)[0];
        if (ARR_NDIM(all_param_array) != 1 || all_param_count <= 0 || ARR_HASNULL(all_param_array) ||
            ARR_ELEMTYPE(all_param_array) != OIDOID) {
            ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("all_parameter_types is not a 1-D Oid array")));
        }
    } else {
        all_param_count = parameter_count;
    }

    if (parameter_count < 0 || parameter_count > FUNC_MAX_ARGS) {
        ereport(ERROR,
                (errcode(ERRCODE_TOO_MANY_ARGUMENTS),
                 errmsg_plural("functions cannot have more than %d argument",
                               "functions cannot have more than %d arguments",
                               FUNC_MAX_ARGS,
                               FUNC_MAX_ARGS)));
    }
    if (parameter_count > FUNC_MAX_ARGS) {
        char hex[MD5_HASH_LEN + 1];
        if (!pg_md5_hash((void*)parameter_types->values, parameter_types->dim1 * sizeof(Oid), hex)) {
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));
        }
        /* Build a dummy oidvector using the hash value and use it as proargtypes field value. */
        oidvector* dummy = buildoidvector((Oid*)hex, MD5_HASH_LEN / sizeof(Oid));
        values[Anum_pg_proc_proargtypes - 1] = PointerGetDatum(dummy);
        values[Anum_pg_proc_proargtypesext - 1] = PointerGetDatum(parameter_types);
    } else {
        values[Anum_pg_proc_proargtypes - 1] = PointerGetDatum(parameter_types);
        nulls[Anum_pg_proc_proargtypesext - 1] = true;
    }
    CacheInvalidateFunction(funcid, InvalidOid);
    if (!is_proc_info_changed(funcid, parameter_types, all_parameter_types, parameter_defaults_str,
                              prorettype, returnsSet)) {
        pfree_ext(parameter_defaults_str);
        return has_undefined;
    }

    replaces[Anum_pg_proc_allargtypes - 1] = true;
    replaces[Anum_pg_proc_allargtypesext - 1] = true;
    replaces[Anum_pg_proc_proargtypes - 1] = true;
    replaces[Anum_pg_proc_proallargtypes - 1] = true;
    replaces[Anum_pg_proc_proargtypesext - 1] = true;
    if (all_parameter_types != PointerGetDatum(NULL)) {
        values[Anum_pg_proc_proallargtypes - 1] = PointerGetDatum(all_parameter_types);
    } else {
        nulls[Anum_pg_proc_proallargtypes - 1] = true;
    }
    if (all_parameter_types != PointerGetDatum(NULL)) {
        if (all_param_count <= FUNC_MAX_ARGS_INROW) {
            nulls[Anum_pg_proc_allargtypesext - 1] = true;
            values[Anum_pg_proc_allargtypes - 1] = PointerGetDatum(all_parameter_types);
        } else {
            oidvector* dummy = MakeMd5HashOids((oidvector*)all_parameter_types);
            values[Anum_pg_proc_allargtypes - 1] = PointerGetDatum(dummy);
            values[Anum_pg_proc_allargtypesext - 1] = PointerGetDatum(all_parameter_types);
        }
    } else if (parameter_types != PointerGetDatum(NULL)){
        values[Anum_pg_proc_allargtypes - 1] = values[Anum_pg_proc_proargtypes - 1];
        values[Anum_pg_proc_allargtypesext - 1] = values[Anum_pg_proc_proargtypesext - 1];
        nulls[Anum_pg_proc_allargtypesext - 1] = nulls[Anum_pg_proc_proargtypesext - 1];
    } else {
        nulls[Anum_pg_proc_allargtypes - 1] = true;
        nulls[Anum_pg_proc_allargtypesext - 1] = true;
    }
    replaces[Anum_pg_proc_proargdefaults - 1] = true;
    if (parameter_defaults != NIL) {
        values[Anum_pg_proc_proargdefaults - 1] = CStringGetTextDatum(parameter_defaults_str);
    } else {
        nulls[Anum_pg_proc_proargdefaults - 1] = true;
    }
    pfree_ext(parameter_defaults_str);
    replaces[Anum_pg_proc_prorettype - 1] = true;
    replaces[Anum_pg_proc_proretset - 1] = true;
    values[Anum_pg_proc_prorettype - 1] = ObjectIdGetDatum(prorettype);
    values[Anum_pg_proc_proretset - 1] = BoolGetDatum(returnsSet);

    Relation rel = heap_open(ProcedureRelationId, RowExclusiveLock);
    HeapTuple tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (HeapTupleIsValid(tup)) {
        HeapTuple newTuple = heap_modify_tuple(tup, RelationGetDescr(rel), values, nulls, replaces);
        simple_heap_update(rel, &newTuple->t_self, newTuple);
        CatalogUpdateIndexes(rel, newTuple);
        heap_freetuple(newTuple);
        ReleaseSysCache(tup);
        CommandCounterIncrement();
    }
    heap_close(rel, RowExclusiveLock);
    return has_undefined;
}

static bool is_proc_info_changed(Oid funcid, oidvector *new_parameter_types, ArrayType *new_all_parameter_types,
                                 char *new_parameter_defaults, Oid new_prorettype, bool new_returnsset)
{
    HeapTuple proc_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    bool is_null = false;
    if (!HeapTupleIsValid((proc_tuple))) {
        return false;
    }

    Form_pg_proc proc_form = (Form_pg_proc)GETSTRUCT(proc_tuple);
    oidvector* proargtypes = NULL;
    if (proc_form->pronargs <= FUNC_MAX_ARGS_INROW) {
        proargtypes = &proc_form->proargtypes;
    } else {
        Datum proargtypesDatum = SysCacheGetAttr(PROCOID, proc_tuple, Anum_pg_proc_proargtypesext, &is_null);
        if (is_null) {
            proargtypes = (oidvector*)PG_DETOAST_DATUM(proargtypesDatum);
        }
    }
    //check proargtypes
    if ((new_parameter_types != NULL && proargtypes == NULL) ||
        (new_parameter_types == NULL && proargtypes != NULL)) {
        ReleaseSysCache(proc_tuple);
        return true;
    } else if (new_parameter_types != NULL && proargtypes != NULL &&
               !DatumGetBool(DirectFunctionCall2(oidvectoreq,
                                                 PointerGetDatum(new_parameter_types),
                                                 PointerGetDatum(proargtypes)))){
        ReleaseSysCache(proc_tuple);
        return true;
    }

    Datum pro_all_arg_types = SysCacheGetAttr(PROCOID, proc_tuple, Anum_pg_proc_proallargtypes, &is_null);
    if (pro_all_arg_types != PointerGetDatum(NULL)) {
        if (new_all_parameter_types == NULL) {
            ReleaseSysCache(proc_tuple);
            return true;
        } else {
            // get new_allpara_type
            int new_allpara_count = ARR_DIMS(new_all_parameter_types)[0];
            size_t size_tmp = new_allpara_count * sizeof(Oid);
            Oid *new_p_argtypes = (Oid *)palloc(size_tmp);
            errno_t rc = memcpy_s(new_p_argtypes, size_tmp, ARR_DATA_PTR(new_all_parameter_types), size_tmp);
            securec_check(rc, "\0", "\0");
            oidvector *new_allpara_type = buildoidvector(new_p_argtypes, new_allpara_count);
            // get allpara_type
            ArrayType *arr = DatumGetArrayTypeP(pro_all_arg_types);
            int allpara_count = ARR_DIMS(arr)[0];
            if (ARR_NDIM(arr) != 1 || allpara_count < 0 || ARR_HASNULL(arr) || ARR_ELEMTYPE(arr) != OIDOID) {
                ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("pro_all_arg_types is not a 1-D Oid array")));
            }
            size_tmp = allpara_count * sizeof(Oid);
            Oid *p_argtypes = (Oid *)palloc(size_tmp);
            rc = memcpy_s(p_argtypes, size_tmp, ARR_DATA_PTR(arr), size_tmp);
            securec_check(rc, "\0", "\0");
            oidvector *allpara_type = buildoidvector(p_argtypes, allpara_count);
            if (!DatumGetBool(DirectFunctionCall2(oidvectoreq, PointerGetDatum(allpara_type),
                                                  PointerGetDatum(new_allpara_type)))) {
                ReleaseSysCache(proc_tuple);
                return true;
            }
            pfree_ext(allpara_type);
            pfree_ext(p_argtypes);
            pfree_ext(new_allpara_type);
            pfree_ext(new_p_argtypes);
        }
    } else {
        if (new_all_parameter_types != NULL) {
            ReleaseSysCache(proc_tuple);
            return true;
        }
    }

    Datum proargdefaultsDatum = SysCacheGetAttr(PROCOID, proc_tuple, Anum_pg_proc_proargdefaults, &is_null);
    if (!is_null) {
        if (new_parameter_defaults == NULL) {
            ReleaseSysCache(proc_tuple);
            return true;
        } else {
            char* proargdefaults = TextDatumGetCString(proargdefaultsDatum);
            if (strcmp(new_parameter_defaults, proargdefaults) != 0) {
                ReleaseSysCache(proc_tuple);
                return true;
            }
            pfree_ext(proargdefaults);
        }
    } else {
        if (new_parameter_defaults != NULL) {
            ReleaseSysCache(proc_tuple);
            return true;
        }
    }

    Datum retTypeDatum = SysCacheGetAttr(PROCOID, proc_tuple, Anum_pg_proc_prorettype, &is_null);
    if (!is_null) {
        Oid rettype = DatumGetObjectId(retTypeDatum);
        if (new_prorettype != rettype) {
            ReleaseSysCache(proc_tuple);
            return true;
        }
    } else {
        ReleaseSysCache(proc_tuple);
        return true;
    }

    Datum retSetDatum = SysCacheGetAttr(PROCOID, proc_tuple, Anum_pg_proc_proretset, &is_null);
    if (!is_null) {
        bool retset = DatumGetBool(retSetDatum);
        if (new_returnsset != retset) {
            ReleaseSysCache(proc_tuple);
            return true;
        }
    } else {
        ReleaseSysCache(proc_tuple);
        return true;
    }

    ReleaseSysCache(proc_tuple);
    return false;
}

static void gsplsql_refresh_proc_header(Oid ref_obj_oid)
{
    Relation gs_depend_rel;
    List* list = NIL;
    gs_depend_rel = heap_open(DependenciesRelationId, AccessShareLock);
    list = gsplsql_search_gs_depend_rel_by_oid(gs_depend_rel, ObjectIdGetDatum(ref_obj_oid));
    if (list == NIL) {
        heap_close(gs_depend_rel, AccessShareLock);
        return;
    }
    ListCell* cell = NULL;
    foreach(cell, list) {
        HeapTuple tuple = (HeapTuple)lfirst(cell);
        GsDependObjDesc obj_desc;
        gsplsql_make_desc_from_gs_dependencies_tuple(tuple, RelationGetDescr(gs_depend_rel), &obj_desc);
        if (obj_desc.refPosType == GSDEPEND_REFOBJ_POS_IN_PROCHEAD) {
            obj_desc.type = GSDEPEND_OBJECT_TYPE_PROCHEAD;
            (void)gsplsql_do_refresh_proc_header(&obj_desc);
        }
        pfree_ext(obj_desc.name);
        heap_freetuple(tuple);
    }
    heap_close(gs_depend_rel, AccessShareLock);
}

static void gsplsql_do_ref_obj_affect_oper(const GsDependObjDesc *ref_obj_desc, Relation gs_depend_rel, List *list,
                                    bool pre_undefined, bool ast_changed)
{
    ListCell* lc = NULL;
    foreach (lc, list) {
        HeapTuple tuple = (HeapTuple)lfirst(lc);
        GsDependObjDesc obj_desc;
        gsplsql_make_desc_from_gs_dependencies_tuple(tuple, RelationGetDescr(gs_depend_rel), &obj_desc);
        if (obj_desc.refPosType == GSDEPEND_REFOBJ_POS_IN_PROCHEAD &&
            (ref_obj_desc->type != GSDEPEND_OBJECT_TYPE_VARIABLE || ast_changed)) {
            obj_desc.type = GSDEPEND_OBJECT_TYPE_PROCHEAD;
            char* new_func_head_name = gsplsql_do_refresh_proc_header(&obj_desc);
            if (new_func_head_name != NULL) {
                obj_desc.name = new_func_head_name;
            }
        }
        if (ast_changed) {
            if (pre_undefined && ref_obj_desc->type == GSDEPEND_OBJECT_TYPE_TYPE &&
                (obj_desc.refPosType & (GSDEPEND_REFOBJ_POS_IN_TYPE | GSDEPEND_REFOBJ_POS_IN_PROCBODY))) {
                gsplsql_cascade_delete_type_by_type_name(&obj_desc);
            }
        }
        pfree_ext(obj_desc.name);
        heap_freetuple(tuple);
    }
}

static inline bool gsplsql_is_need_update_ref_dep_obj(char* ref_obj_ast_str, bool ast_changed, int32 obj_type)
{
    return (NULL != ref_obj_ast_str && ast_changed) || obj_type == GSDEPEND_OBJECT_TYPE_UNDEFIND;
}

static void gsplsql_update_ref_dep_obj_base_on_ast(const GsDependObjDesc *ref_obj_desc, HeapTuple obj_tuple,
                                            char *ref_obj_ast_str, bool ast_changed)
{
    bool is_null;
    Datum type_datum = gsplsql_get_depend_object_attr(obj_tuple, Anum_gs_dependencies_obj_type, &is_null);
    AssertEreport(!is_null, MOD_PLSQL, "Object Type must not be null.");
    if (!gsplsql_is_need_update_ref_dep_obj(ref_obj_ast_str, ast_changed, DatumGetInt32(type_datum))) {
        return;
    }
    HeapTuple new_obj_tuple;
    Relation gs_dep_obj_rel;
    Datum values[Natts_gs_dependencies_obj];
    bool nulls[Natts_gs_dependencies_obj] = {false};
    bool replaces[Natts_gs_dependencies_obj] = {false};
    if (DatumGetInt32(type_datum) == GSDEPEND_OBJECT_TYPE_UNDEFIND) {
        values[Anum_gs_dependencies_obj_type -1] = DatumGetInt32(ref_obj_desc->type);
        replaces[Anum_gs_dependencies_obj_type -1] = true;
    }
    if (NULL != ref_obj_ast_str && ast_changed) {
        values[Anum_gs_dependencies_obj_objnode -1] = CStringGetTextDatum(ref_obj_ast_str);
        replaces[Anum_gs_dependencies_obj_objnode -1] = true;
    }

    gs_dep_obj_rel = heap_open(DependenciesObjRelationId, RowExclusiveLock);
    new_obj_tuple = heap_modify_tuple(obj_tuple, RelationGetDescr(gs_dep_obj_rel), values, nulls, replaces);
    (void)simple_heap_update(gs_dep_obj_rel, &obj_tuple->t_self, new_obj_tuple, true);
    CatalogUpdateIndexes(gs_dep_obj_rel, new_obj_tuple);
    heap_freetuple(new_obj_tuple);
    heap_close(gs_dep_obj_rel, RowExclusiveLock);
    CommandCounterIncrement();
}

static void gsplsql_rename_func_name(const char *nsp_name, const char *pkg_name, const char *old_func_name,
                              const char *new_func_name)
{
    HeapTuple tuple = NULL;
    HeapTuple new_tuple = NULL;
    int keyNum = 0;
    ScanKeyData key[3];
    SysScanDesc scan = NULL;
    Datum values[Natts_gs_dependencies];
    bool nulls[Natts_gs_dependencies] = {false};
    bool replaces[Natts_gs_dependencies] = {false};
    Assert(nsp_name != NULL && pkg_name != NULL && old_func_name != NULL && new_func_name != NULL);
    values[Anum_gs_dependencies_objectname -1] = CStringGetTextDatum(new_func_name);
    replaces[Anum_gs_dependencies_objectname -1] = true;
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_schemaname, BTEqualStrategyNumber, F_NAMEEQ,
                NameGetDatum(nsp_name));
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_packagename, BTEqualStrategyNumber, F_NAMEEQ,
                NameGetDatum(pkg_name));
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_objectname, BTEqualStrategyNumber, F_TEXTEQ,
                CStringGetTextDatum(old_func_name));
    Relation relation = heap_open(DependenciesRelationId, RowExclusiveLock);
    scan = systable_beginscan(relation, InvalidOid, false, SnapshotSelf, keyNum, key);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        new_tuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);
        simple_heap_update(relation, &new_tuple->t_self, new_tuple, true);
        CatalogUpdateIndexes(relation, new_tuple);
        heap_freetuple(new_tuple);
    }
    systable_endscan(scan);
    heap_close(relation, RowExclusiveLock);
    CommandCounterIncrement();
}

static void gsplsql_obj_func_name(const char *nsp_name, const char *pkg_name, const char *old_func_name,
                                  const char *new_func_name)
{
    HeapTuple tuple = NULL;
    HeapTuple new_tuple = NULL;
    int keyNum = 0;
    ScanKeyData key[3];
    SysScanDesc scan = NULL;
    Datum values[Natts_gs_dependencies_obj];
    bool nulls[Natts_gs_dependencies_obj] = {false};
    bool replaces[Natts_gs_dependencies_obj] = {false};
    Assert(nsp_name != NULL && pkg_name != NULL && old_func_name != NULL && new_func_name != NULL);
    values[Anum_gs_dependencies_obj_name -1] = CStringGetTextDatum(new_func_name);
    replaces[Anum_gs_dependencies_obj_name -1] = true;
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_schemaname, BTEqualStrategyNumber, F_NAMEEQ,
                NameGetDatum(nsp_name));
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_packagename, BTEqualStrategyNumber, F_NAMEEQ,
                NameGetDatum(pkg_name));
    ScanKeyInit(&key[keyNum++], Anum_gs_dependencies_obj_name, BTEqualStrategyNumber, F_TEXTEQ,
                CStringGetTextDatum(old_func_name));
    Relation relation = heap_open(DependenciesObjRelationId, RowExclusiveLock);
    scan = systable_beginscan(relation, InvalidOid, false, SnapshotSelf, keyNum, key);
    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        new_tuple = heap_modify_tuple(tuple, RelationGetDescr(relation), values, nulls, replaces);
        simple_heap_update(relation, &new_tuple->t_self, new_tuple, true);
        CatalogUpdateIndexes(relation, new_tuple);
        heap_freetuple(new_tuple);
    }
    systable_endscan(scan);
    heap_close(relation, RowExclusiveLock);
    CommandCounterIncrement();
}

static void gsplsql_delete_dependencies_object_by_oid(Oid oid)
{
    Relation dep_rel = heap_open(DependenciesObjRelationId, RowExclusiveLock);
    ScanKeyData key;
    ScanKeyInit(&key, ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oid));
    SysScanDesc scan = systable_beginscan(dep_rel, DependenciesObjOidIndexId, true, SnapshotSelf, 1, &key);
    HeapTuple tuple = systable_getnext(scan);
    if (!HeapTupleIsValid(tuple)) {
        systable_endscan(scan);
        heap_close(dep_rel, RowExclusiveLock);
        return;
    }
    simple_heap_delete(dep_rel, &tuple->t_self, 0, true);
    systable_endscan(scan);
    heap_close(dep_rel, RowExclusiveLock);
    CommandCounterIncrement();
}

void gsplsql_remove_type_gs_dependency(const GsDependObjDesc* obj_desc)
{
    Relation relation = heap_open(DependenciesRelationId, RowExclusiveLock);
    List* list = gsplsql_delete_objs(relation, obj_desc);
    if (obj_desc->packageName != NULL) {
        Oid nsp_oid = get_namespace_oid(obj_desc->schemaName, true);
        Oid pkg_oid = PackageNameGetOid(obj_desc->packageName, nsp_oid);
        ObjectAddress object;
        object.classId = PackageRelationId;
        object.objectId = pkg_oid;
        ObjectAddress ref_object;
        ListCell* cell = NULL;
        foreach(cell, list) {
            Oid ref_obj_oid = lfirst_oid(cell);
            GsDependObjDesc ref_obj_desc;
            bool has_object = gsplsql_search_depend_obj_by_oid(ref_obj_oid, &ref_obj_desc);
            if (has_object) {
                if (ref_obj_desc.packageName != NULL) {
                    Oid ref_nsp_oid = get_namespace_oid(ref_obj_desc.schemaName, true);
                    Oid ref_pkg_oid = PackageNameGetOid(ref_obj_desc.packageName, ref_nsp_oid);
                    ref_object.classId = PackageRelationId;
                    ref_object.objectId = ref_pkg_oid;
                    DeletePgDependObject(&object, &ref_object);
                }
                free_gs_depend_obj_desc(&ref_obj_desc);
            }
        }
    }
    gsplsql_delete_unrefer_depend_obj_in_list(list, true);
    list_free(list);
    heap_close(relation, RowExclusiveLock);
}