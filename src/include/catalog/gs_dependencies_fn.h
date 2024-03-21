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
* gs_dependencies_fn.h
*     Definition about catalog of dependencies function object.
*
*
* IDENTIFICATION
*        src/include/catalog/gs_dependencies_fn.h
*
* ---------------------------------------------------------------------------------------
*/

#ifndef GS_DEPENDENCIES_FN_H
#define GS_DEPENDENCIES_FN_H
#include "utils/plpgsql.h"
#include "nodes/pg_list.h"

struct ObjectAddress;

typedef struct GsDependObjDesc {
   char* schemaName;
   char* packageName;
   char* name;
   GsDependObjectType type;
   int refPosType;
} GsDependObjDesc;

typedef struct GsDependParamBody {
   Oid dependNamespaceOid;
   Oid dependPkgOid;
   char* dependPkgName;
   char* dependName;
   TypeDependExtend* dependExtend;
   GsDependObjectType type;
   int refPosType;
   bool hasDependency;
} GsDependParamBody;

typedef struct PkgVarInfo {
   char* nsp_name;
   char* pkg_name;
   char* var_name;
   Param* param;
} PkgVarInfo;

/*
 * business interface
 */
extern bool gsplsql_is_undefined_func(Oid func_oid);
extern void gsplsql_build_gs_variable_dependency(List* var_name);
extern DependenciesDatum* gsplsql_make_depend_datum_by_type_oid(const Oid typOid, bool* depend_undefined);
extern void free_gs_depend_obj_desc(GsDependObjDesc* obj_desc);
extern Oid gsplsql_get_pkg_oid_by_func_oid(Oid func_oid);
extern Oid get_compiled_object_nspoid();
extern bool gsplsql_build_gs_type_dependency(GsDependParamBody* gsdependParamBody, const ObjectAddress* referenced);
extern bool gsplsql_undefine_in_same_pkg(GsDependObjDesc* obj_desc, Oid undefObjOid);
extern Oid gsplsql_try_build_exist_pkg_undef_var(List* dtnames);
extern Oid gsplsql_try_build_exist_schema_undef_table(RangeVar* rel_var);
extern Oid gsplsql_flush_undef_ref_depend_obj(const GsDependObjDesc* object);
extern Oid gsplsql_flush_undef_ref_type_dependency(Oid type_oid);
extern bool gsplsql_need_build_gs_dependency(GsDependParamBody* gsdependParamBody, const ObjectAddress* referenced, bool is_pinned);
extern void gsplsql_init_gs_depend_param_body(GsDependParamBody* param_body);
extern void gsplsql_get_depend_obj_by_typ_id(GsDependObjDesc* ref_obj, Oid typ_oid,
   Oid pkg_oid, bool drop_typ = false);
extern bool gsplsql_exist_dependency(GsDependObjDesc* obj_desc);
extern bool gsplsql_is_object_depend(Oid oid, GsDependObjectType type);
extern bool gsplsql_build_gs_type_in_body_dependency(PLpgSQL_type* type);
/*
 * basic interface
 */
extern DependenciesDatum* gsplsql_make_depend_datum_from_plsql_datum(const PLpgSQL_datum *datum, char** schema_name,
                                                                     bool* depend_undefined);
extern bool gsplsql_check_type_depend_undefined(const char* schemaName, const char* pkg_name,
                                                const char* typname, bool isDeleteType = false);
extern TupleDesc build_gs_depend_expr(PLpgSQL_expr* expr, PLpgSQL_function* func, bool need_tupdesc = false);
extern bool gsplsql_build_gs_dependency(const GsDependObjDesc* obj_desc, const GsDependObjDesc* ref_obj_desc,
                                          const DependenciesDatum* ref_obj_ast);
extern bool gsplsql_build_gs_dependency(const GsDependObjDesc* obj_desc, const Oid ref_obj_oid);
extern Oid  gsplsql_update_object_ast(const GsDependObjDesc* object, const DependenciesDatum* ref_obj_ast);
extern bool gsplsql_build_ref_dependency(Oid nsp_oid, const PLpgSQL_datum* ref_datum, const char* pkg_name = NULL,
                                         List** ref_obj_oids = NULL, Oid curr_compile_oid = InvalidOid);
extern bool gsplsql_build_ref_dependency(const GsDependObjDesc* ref_obj_desc, const DependenciesDatum* ref_obj_ast,
                                         List** ref_obj_oids = NULL, Oid curr_compile_oid = InvalidOid);
extern Oid gsplsql_insert_dependencies_object(const GsDependObjDesc *object, char *obj_ast_str);
extern bool gsplsql_set_pkg_func_status(Oid schema_oid, Oid pkg_oid, bool status);
extern void gsplsql_remove_dependencies_object(const GsDependObjDesc* ref_obj_desc);
extern bool gsplsql_remove_gs_dependency(const GsDependObjDesc* ref_obj_desc);
extern bool gsplsql_remove_ref_dependency(const GsDependObjDesc* ref_obj_desc);
extern bool gsplsql_build_ref_type_dependency(Oid typ_oid);
extern List* gsplsql_prepare_recompile_func(Oid func_oid, Oid schema_oid, Oid pkg_oid, bool is_recompile);
extern void gsplsql_complete_recompile_func(List* list);
extern Oid gsplsql_parse_pkg_var_obj4(GsDependObjDesc* obj, List* var_name);
extern void gsplsql_do_autonomous_compile(Oid objoid, bool is_pkg);
/*
 * internal interface
 */
extern void gsplsql_remove_depend_obj_by_specified_oid(Oid ref_obj_oid, Oid curr_compile_oid = InvalidOid, bool is_same_pkg = false);
extern GsDependObjDesc gsplsql_construct_func_head_obj(Oid procOid, Oid procNamespace, Oid proPackageId);
extern char* gsplsql_do_refresh_proc_header(const GsDependObjDesc* obj, bool* is_undefined = NULL);
extern List* gsplsql_delete_objs(Relation relation, const GsDependObjDesc* obj_desc);
extern List* gsplsql_get_depend_obj_list_by_specified_pkg(const char* schema_name, const char* package_name,
                                                   GsDependObjectType type);
extern void gsplsql_make_desc_from_gs_dependencies_tuple(HeapTuple gs_depend_tuple, TupleDesc tup_desc,
                                                                GsDependObjDesc *obj_desc);
extern void gsplsql_remove_depend_obj_by_specified_oid(Oid ref_obj_oid, Oid curr_compile_oid, bool is_same_pkg);
extern void gsplsql_delete_unrefer_depend_obj_in_list(List* ref_obj_oid_list, bool skip_in_use);
extern List *gsplsql_search_gs_depend_rel_by_oid(Relation relation, Oid ref_obj_oid);
extern void gsplsql_invalidate_objs(Oid oid, bool is_ref_remove, Oid curr_compile_oid);
extern void gsplsql_build_gs_var_dependency(Param* param);
extern void gsplsql_build_gs_synonym_dependency(Oid refSynOid);
extern void gsplsql_build_gs_table_dependency(Oid relid);
extern Oid gsplsql_get_proc_oid(const char* schemaName, const char* packageName, const char* name);
extern Oid gsplsql_get_proc_oid(const char* schemaName, const char* packageName, const char* name,
                                const char* proc_arg_src);
extern Datum gsplsql_get_depend_object_attr(HeapTuple tup, int attr_number, bool *is_null);
extern void gsplsql_construct_non_empty_obj(const GsDependObjDesc* object, Name schema_name_data,
                                     Name pkg_name_data, StringInfo object_name_data);
extern void gsplsql_build_gs_func_dependency(Oid func_oid);
extern void gsplsql_build_gs_func_dependency(List* func_name_list, FuncCandidateList func_candidate_list);
extern GsDependObjDesc gsplsql_parse_func_name_to_obj_desc(List *funcNameList, Oid namespaceOid);
extern void gsplsql_complete_gs_depend_for_pkg_compile(PLpgSQL_package* pkg, bool isCreate, bool isRecompile);
extern void gsplsql_prepare_gs_depend_for_pkg_compile(PLpgSQL_package* pkg, bool isCreate);
extern void gsplsql_init_gs_depend_obj_desc(GsDependObjDesc* object);
extern HeapTuple gsplsql_search_object(const GsDependObjDesc* object, bool supp_undef_type);
extern bool gsplsql_parse_type_and_name_from_gsplsql_datum(const PLpgSQL_datum* datum, char** datum_name,
                                                            GsDependObjectType* datum_type);
extern HeapTuple gsplsql_search_object_by_name(const char* schema_name, const char* package_name,
                                               const char*  object_name, GsDependObjectType type);
extern GsDependObjDesc get_func_gs_depend_obj_desc(Oid func_oid);
extern void gsplsql_delete_unrefer_depend_obj_oid(Oid ref_obj_oid, bool skip_in_use);
extern bool gsplsql_check_type_depend_ast_equal(Relation obj_rel, Oid obj_oid, const char* equal_ast);
extern bool gsplsql_search_depend_obj_by_oid(Oid oid, GsDependObjDesc* obj_desc);
extern void gsplsql_remove_type_gs_dependency(const GsDependObjDesc* obj_desc);
extern bool gsplsql_exists_func_obj(Oid nsp_oid, Oid pkg_oid, const char* old_func_head_name,
                                    const char* old_func_name);
extern bool gsplsql_exists_schema_name(const char* schema_name);
#endif   /* GS_DEPENDENCIES_FN_H */
