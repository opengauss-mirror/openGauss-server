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
*     Definition about dependencies object.
*
*
* IDENTIFICATION
*        src/include/catalog/gs_dependencies_obj.h
*
* ---------------------------------------------------------------------------------------
*/
#ifndef GS_DEPENDENCIES_OBJ_H
#define GS_DEPENDENCIES_OBJ_H

#include "catalog/genbki.h"

#define DependenciesObjRelationId 7169
#define DependenciesObjRelationId_Rowtype_Id 7170

CATALOG(gs_dependencies_obj,7169) BKI_SCHEMA_MACRO
{
   NameData schemaname;
   NameData packagename;
   int4 type;
#ifdef CATALOG_VARLEN
   text name;
   pg_node_tree objnode;
#endif
} FormData_gs_dependencies_obj;

typedef FormData_gs_dependencies_obj *Form_gs_dependencies_obj;

#define Natts_gs_dependencies_obj                     5
#define Anum_gs_dependencies_obj_schemaname           1
#define Anum_gs_dependencies_obj_packagename          2
#define Anum_gs_dependencies_obj_type                 3
#define Anum_gs_dependencies_obj_name                 4
#define Anum_gs_dependencies_obj_objnode              5
#endif