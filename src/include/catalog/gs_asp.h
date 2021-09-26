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
 * gs_asp.h
 *
 * IDENTIFICATION
 *	  src/include/catalog/gs_asp.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef PG_ASP_H
#define PG_ASP_H

#include "postgres.h"
#include "utils/inet.h"
#include "knl/knl_variable.h"
#include "catalog/genbki.h"
#include "fmgr.h"
#include "utils/date.h"

#define timestamptz Datum
#define int8 int64

/*-------------------------------------------------------------------------
 *		gs_asp definition.  cpp turns this into
 *		typedef struct GsAspRelation_Rowtype_Id
 *-------------------------------------------------------------------------
 *     */
#define GsAspRelationId 9534
#define GsAspRelation_Rowtype_Id 3465

CATALOG(gs_asp,9534) BKI_WITHOUT_OIDS BKI_ROWTYPE_OID(3465) BKI_SCHEMA_MACRO
{
    int8 sampleid;
    timestamptz sample_time;
    bool need_flush_sample;
    Oid databaseid;
    int8 thread_id;
    int8 sessionid;
    timestamptz start_time;
#ifdef CATALOG_VARLEN                                   /* Null value constrain */
    text event;
    int4 lwtid;
    int8 psessionid;
    int4 tlevel;
    int4 smpid;
    Oid userid;
    text application_name;
    inet client_addr;
    text client_hostname;
    int4 client_port;
    int8 query_id;
    int8 unique_query_id;
    Oid user_id;
    int4 cn_id;
    text unique_query;
    text locktag;
    text lockmode;
    int8 block_sessionid;
    text wait_status;
    text global_sessionid;
#endif
} FormData_gs_asp;

#undef timestamp
#undef int8

/*-------------------------------------------------------------------------
 *		FormData_gs_asp corresponds to a pointer to a tuple with
 *		the format of gs_asp relation.
 *-------------------------------------------------------------------------
 *     */
typedef FormData_gs_asp* Form_gs_asp;

/*-------------------------------------------------------------------------
 *		compiler constants for gs_asp
 *-------------------------------------------------------------------------
 */
#define Natts_gs_asp                      27
#define Anum_gs_asp_sample_id             1
#define Anum_gs_asp_sample_time           2
#define Anum_gs_asp_need_flush_sample     3
#define Anum_gs_asp_databaseid            4
#define Anum_gs_asp_tid                   5
#define Anum_gs_asp_sessionid             6
#define Anum_gs_asp_start_time            7
#define Anum_gs_asp_event                 8
#define Anum_gs_asp_lwtid                 9
#define Anum_gs_asp_psessionid            10
#define Anum_gs_asp_tlevel                11
#define Anum_gs_asp_smpid                 12
#define Anum_gs_asp_useid                 13
#define Anum_gs_asp_application_name      14
#define Anum_gs_asp_client_addr           15
#define Anum_gs_asp_client_hostname       16
#define Anum_gs_asp_client_port           17
#define Anum_gs_asp_query_id              18
#define Anum_gs_asp_unique_query_id       19
#define Anum_gs_asp_user_id               20
#define Anum_gs_asp_cn_id                 21
#define Anum_gs_asp_unique_query          22
#define Anum_gs_asp_locktag               23
#define Anum_gs_asp_lockmode              24
#define Anum_gs_asp_block_sessionid       25
#define Anum_gs_asp_wait_status           26
#define Anum_gs_asp_global_sessionid      27

#endif /* GS_ASP */

