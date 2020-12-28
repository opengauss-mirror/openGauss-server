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
 * pg_streaming_stream.h
 *      streaming stream schema
 *
 * IDENTIFICATION
 *        src/include/catalog/pg_streaming_stream.h
 *
 *---------------------------------------------------------------------------------------
 */
#ifndef STREAMING_STREAM_H
#define STREAMING_STREAM_H

#include "catalog/genbki.h"

#define StreamingStreamRelationId  9028
#define StreamingStreamRelation_Rowtype_Id  7200

CATALOG(streaming_stream,9028) BKI_SCHEMA_MACRO
{
    Oid     relid;
    bytea   queries;
} FormData_streaming_stream;

typedef FormData_streaming_stream *Form_streaming_stream;

#define Natts_streaming_stream			    2

#define Anum_streaming_stream_relid			1
#define Anum_streaming_stream_queries       2

#endif   /* PG_STREAMING_STREAM_H */
