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
 * gs_policy_label.h
 *
 * IDENTIFICATION
 *    src/include/catalog/gs_policy_label.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef GS_POLICY_LABEL_H
#define GS_POLICY_LABEL_H

#include "postgres.h"
#include "catalog/genbki.h"
#include "utils/timestamp.h"
#include "utils/date.h"

#ifdef HAVE_INT64_TIMESTAMP
#define timestamp int64
#else
#define timestamp double
#endif

#define GsPolicyLabelRelationId  9500
#define GsPolicyLabelRelationId_Rowtype_Id 9503

CATALOG(gs_policy_label,9500) BKI_SCHEMA_MACRO
{
    NameData    labelname;
    NameData    labeltype;      /* resource label */
    Oid         fqdnnamespace;  /* namespace oid */
    Oid         fqdnid;         /* relation oid */
    NameData    relcolumn;      /* column name */
    NameData    fqdntype;       /* schema, table, column, view etc. */
} FormData_gs_policy_label;

typedef FormData_gs_policy_label *Form_gs_policy_label;

#define Natts_gs_policy_label                      6

#define Anum_gs_policy_label_labelname             1
#define Anum_gs_policy_label_labeltype             2
#define Anum_gs_policy_label_fqdnnamespace         3
#define Anum_gs_policy_label_fqdnid                4
#define Anum_gs_policy_label_relcolumn             5
#define Anum_gs_policy_label_fqdntype              6

#endif   /* GS_POLICY_LABEL_H */