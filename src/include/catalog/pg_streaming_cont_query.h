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
 *---------------------------------------------------------------------------------------
 *
 * pg_streaming_cont_query.h
 *      streaming continuous query catalog schema
 *
 *
 * IDENTIFICATION
 *        src/include/catalog/pg_streaming_cont_query.h
 *
 *---------------------------------------------------------------------------------------
 */
#ifndef STREAMING_CONT_QUERY_H
#define STREAMING_CONT_QUERY_H

#include "catalog/genbki.h"

#define StreamingContQueryRelationId  9029
#define StreamingContQueryRelation_Rowtype_Id  7201

CATALOG(streaming_cont_query,9029) BKI_SCHEMA_MACRO
{
    int4        id;
    char        type;
    Oid         relid;
    Oid         defrelid;
    bool        active;
    Oid         streamrelid;
    Oid         matrelid;
    Oid         lookupidxid;
    int2        step_factor;
    int4        ttl;
    int2        ttl_attno;
    Oid         dictrelid;
    int2        grpnum;
    int2vector  grpidx;
} FormData_streaming_cont_query;

typedef FormData_streaming_cont_query *Form_streaming_cont_query;


#define Natts_streaming_cont_query			            14

#define Anum_streaming_cont_query_id			        1
#define Anum_streaming_cont_query_type                  2
#define Anum_streaming_cont_query_relid                 3
#define Anum_streaming_cont_query_defrelid              4
#define Anum_streaming_cont_query_active                5
#define Anum_streaming_cont_query_streamrelid           6
#define Anum_streaming_cont_query_matrelid              7
#define Anum_streaming_cont_query_lookupidxid           8
#define Anum_streaming_cont_query_step_factor           9
#define Anum_streaming_cont_query_ttl                   10
#define Anum_streaming_cont_query_ttl_attno             11
#define Anum_streaming_cont_query_dictrelid             12
#define Anum_streaming_cont_query_grpnum                13
#define Anum_streaming_cont_query_grpidx                14

#endif
