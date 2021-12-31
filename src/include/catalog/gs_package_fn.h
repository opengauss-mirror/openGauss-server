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
 * gs_matview.h
 *     Definition about catalog of matviews.
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/gs_package_fn.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GS_PACKAGE_FN_H
#define GS_PACKAGE_FN_H

#include "nodes/pg_list.h"

extern Oid PackageSpecCreate(Oid pkgNamespace, const char *pkgName, const Oid ownerId, 
                             const char* pkgSpecSrc, bool replace, bool isSecDef);
extern Oid PackageBodyCreate(Oid pkgNamespace, const char* pkgName, const Oid ownerId, 
                             const char* pkgBodySrc, const char* pkgInitSrc, bool replace);
extern bool IsFunctionInPackage(List* wholename); 
extern PLpgSQL_package* PackageInstantiation(Oid packageOid);
extern void PackageInit(PLpgSQL_package* pkg, bool isCreate);
extern NameData* GetPackageName(Oid packageOid);
extern Oid PackageNameListGetOid(List* pkgnameList, bool missing_ok);
extern Oid PackageNameGetOid(const char* pkgname, Oid namespaceId);
extern bool IsExistPackageName(const char* pkgname);
#endif   /* PG_PROC_FN_H */

