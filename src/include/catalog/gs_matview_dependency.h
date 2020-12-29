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
 * gs_matview_dependency.h
 *     Definition about catalog of matviews dependency
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/gs_matview_dependency.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef GS_MATVIEW_DEPENDENCY_H
#define GS_MATVIEW_DEPENDENCY_H

#include "catalog/genbki.h"
#include "c.h"

#define MatviewDependencyId   9985
#define MatviewDependencyId_Rowtype_Id 9987

CATALOG(gs_matview_dependency,9985) BKI_SCHEMA_MACRO
{
    Oid             matviewid;
    Oid             relid;
    Oid             mlogid;
    int4            mxmin;
} FormData_gs_matview_dependency;

typedef FormData_gs_matview_dependency *Form_gs_matview_dependency;

#define Natts_gs_matview_dependency         4

#define Anum_gs_matview_dep_matviewid       1
#define Anum_gs_matview_dep_relid           2
#define Anum_gs_matview_dep_mlogid          3
#define Anum_gs_matview_dep_mxmin           4


#endif   /* GS_MATVIEW_H */
