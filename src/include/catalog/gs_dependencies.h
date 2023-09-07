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
* gs_dependencies.h
*     Definition about package dependencies.
*
*
* IDENTIFICATION
*        src/include/catalog/gs_dependencies.h
*
* ---------------------------------------------------------------------------------------
*/
#ifndef GS_DEPENDENCIES_H
#define GS_DEPENDENCIES_H

#include "catalog/genbki.h"

#define DependenciesRelationId 7111
#define DependenciesRelationId_Rowtype_Id 7112

CATALOG(gs_dependencies,7111) BKI_SCHEMA_MACRO BKI_WITHOUT_OIDS
{
   NameData schemaname;
   NameData packagename;
   int4 refobjpos;
   Oid refobjoid;
#ifdef CATALOG_VARLEN
   text objectname;
#endif
} FormData_gs_dependencies;

typedef FormData_gs_dependencies *Form_gs_dependencies;

#define Natts_gs_dependencies                             5
#define Anum_gs_dependencies_schemaname                   1
#define Anum_gs_dependencies_packagename                  2
#define Anum_gs_dependencies_refobjpos                    3
#define Anum_gs_dependencies_refobjoid                    4
#define Anum_gs_dependencies_objectname                   5
#endif