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
 * gs_global_chain.h
 *
 * IDENTIFICATION
 *    src/include/catalog/gs_global_chain.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef GS_GLOBAL_CHAIN_H
#define GS_GLOBAL_CHAIN_H

#include "catalog/genbki.h"
#include "utils/timestamp.h"
#include "utils/date.h"
#include "utils/uuid.h"


#define hash16 uint64
#define hash32 hash32_t

#define GsGlobalChainRelationId 5818
#define GsGlobalChainRelationId_Rowtype_Id 5819

#ifndef timestamptz
#ifdef HAVE_INT64_TIMESTAMP
#define timestamptz int64
#else
#define timestamptz double
#endif
#define new_timestamptz
#endif

CATALOG(gs_global_chain,5818) BKI_WITHOUT_OIDS BKI_SCHEMA_MACRO
{
    int8        blocknum;
    NameData    dbname;
    NameData    username;
    timestamptz   starttime;
    Oid         relid;
    NameData    relnsp;
    NameData    relname;
    hash16      relhash;
    hash32      globalhash;
#ifdef CATALOG_VARLEN
    text        txcommand;
#endif
} FormData_gs_global_chain;

typedef FormData_gs_global_chain *Form_gs_global_chain;

#define Natts_gs_global_chain                   10

#define Anum_gs_global_chain_blocknum           1
#define Anum_gs_global_chain_dbname             2
#define Anum_gs_global_chain_username           3
#define Anum_gs_global_chain_starttime          4
#define Anum_gs_global_chain_relid              5
#define Anum_gs_global_chain_relnsp             6
#define Anum_gs_global_chain_relname            7
#define Anum_gs_global_chain_relhash            8
#define Anum_gs_global_chain_globalhash         9
#define Anum_gs_global_chain_txcommand          10

#ifdef new_timestamptz
#undef new_timestamptz
#undef timestamptz
#endif

#endif   /* GS_GLOBAL_CHAIN_H */
