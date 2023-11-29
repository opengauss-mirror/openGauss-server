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
 * ---------------------------------------------------------------------------------------
 * 
 * pl_package.h
 *     Definition about package.
 * 
 * 
 * IDENTIFICATION
 *        src/common/pl/plpgsql/src/pl_package.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef PL_PACKAGE_H
#define PL_PACKAGE_H
#include "utils/plpgsql.h"



extern bool check_search_path_interface(List *schemas, HeapTuple proc_tup);


extern PLpgSQL_package* plpgsql_pkg_HashTableLookup(PLpgSQL_pkg_hashkey* pkg_key);

extern void delete_package(PLpgSQL_package* pkg);

extern bool plpgsql_check_opexpr_colocate(
    Query* query, List* qry_part_attr_num, List* trig_part_attr_num, PLpgSQL_function* func, List* opexpr_list);

extern bool plpgsql_check_updel_colocate(
    Query* query, List* qry_part_attr_num, List* trig_part_attr_num, PLpgSQL_function* func);

extern bool check_search_path_interface(List *schemas, HeapTuple proc_tup);

extern void plpgsql_compile_error_callback(void* arg);

extern Oid findPackageParameter(const char* objname);

extern int plpgsql_getCustomErrorCode(void);

extern PLpgSQL_row* build_row_from_class(Oid class_oid);

extern int GetLineNumber(const char* procedureStr, int loc);

extern int GetProcedureLineNumberInPackage(const char* procedureStr, int loc);

extern void InsertError(Oid objId);

extern int CompileWhich();

extern void InsertErrorMessage(const char* message, int yyloc, bool isQueryString = false, int lines = 0);

extern void DropErrorByOid(int objtype, Oid objoid);

extern bool IsOnlyCompilePackage();

extern HeapTuple getCursorTypeTup(const char* word);

extern List* GetPackageListName(const char* pkgName, const Oid nspOid);

extern HeapTuple getPLpgsqlVarTypeTup(char* word);

extern HeapTuple FindRowVarColType(List* nameList, int* collectionType = NULL, Oid* tableofIndexType = NULL,
                                    int32* typMod = NULL, TypeDependExtend* dependExtend = NULL);
#endif
