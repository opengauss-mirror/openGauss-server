/* -------------------------------------------------------------------------
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
 * -------------------------------------------------------------------------
 *
 * gs_encrypted_proc.h
 *
 * IDENTIFICATION
 *	  src/include/catalog/gs_encrypted_proc.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef GS_ENCRYPTED_PROC_H
#define GS_ENCRYPTED_PROC_H

#include "catalog/genbki.h"

#ifdef HAVE_INT64_TIMESTAMP
#define timestamp int64
#else
#define timestamp double
#endif

#define ClientLogicProcId  9750
#define ClientLogicProcId_Rowtype_Id  9753
CATALOG(gs_encrypted_proc,9750) BKI_SCHEMA_MACRO
{
    Oid func_id;                            /* function oid */
    int4         prorettype_orig;           /* OID of result type */
    timestamp    last_change;               /* last change of this procedure */
    oidvector    proargcachedcol;           /* colums settings oid (excludes OUT params) */

#ifdef CATALOG_VARLEN
    Oid         proallargtypes_orig[1];     /* all param types (NULL if IN only) */
#endif
} FormData_gs_encrypted_proc;

typedef FormData_gs_encrypted_proc *Form_gs_encrypted_proc;

#define Natts_gs_encrypted_proc                        5

#define Anum_gs_encrypted_proc_func_id                 1
#define Anum_gs_encrypted_proc_prorettype_orig         2
#define Anum_gs_encrypted_proc_last_change             3
#define Anum_gs_encrypted_proc_proargcachedcol         4
#define Anum_gs_encrypted_proc_proallargtypes_orig     5

#endif   /* GS_ENCRYPTED_PROC_H */
