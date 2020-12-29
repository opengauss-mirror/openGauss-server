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
 * pg_streaming_reaper_status.h
 *      streaming continuous query catalog schema
 *
 * IDENTIFICATION
 *        src/include/catalog/pg_streaming_reaper_status.h
 *
 *---------------------------------------------------------------------------------------
 */
#ifndef STREAMING_REAPER_STATUS_H
#define STREAMING_REAPER_STATUS_H

#include "catalog/genbki.h"

#define StreamingReaperStatusRelationId  9030
#define StreamingReaperStatusRelation_Rowtype_Id  7202

CATALOG(streaming_reaper_status,9030) BKI_SCHEMA_MACRO
{
    int4        id;
    NameData    contquery_name;
#ifdef CATALOG_VARLEN
    text        gather_interval;
    text        gather_completion_time;
#endif
} FormData_streaming_reaper_status;

typedef FormData_streaming_reaper_status *Form_streaming_reaper_status;


#define Natts_streaming_reaper_status			                     4

#define Anum_streaming_reaper_status_id			                     1
#define Anum_streaming_reaper_status_contquery_name                  2
#define Anum_streaming_reaper_status_gather_interval                 3
#define Anum_streaming_reaper_status_gather_completion_time          4

#endif
