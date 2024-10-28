/*
* Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * imcustorage.cpp
 *      routines to support IMColStore
 *
 * IDENTIFICATION
 *        src/include/catalog/query_imcstore_views.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef QUERY_IMCSTORE_VIEWS_H
#define QUERY_IMCSTORE_VIEWS_H
#include "utils/date.h"

#define Natts_imcstore_views 11

#define Anum_imcstore_views_reloid 1
#define Anum_imcstore_views_relname 2
#define Anum_imcstore_views_imcs_attrs 3
#define Anum_imcstore_views_imcs_nattrs 4
#define Anum_imcstore_views_imcs_status 5
#define Anum_imcstore_views_is_partition 6
#define Anum_imcstore_views_parent_oid 7
#define Anum_imcstore_views_cu_size_in_mem 8
#define Anum_imcstore_views_cu_num_in_mem 9
#define Anum_imcstore_views_cu_size_in_disk 10
#define Anum_imcstore_views_cu_num_in_disk 11

struct IMCStoreView {
    Oid relOid;
    const char* relname;
    int2vector* imcsAttsNum;
    int imcsNatts;
    const char* imcsStatus;
    bool isPartition;
    Oid parentOid;
    uint64 cuSizeInMem;
    uint64 cuNumsInMem;
    uint64 cuSizeInDisk;
    uint64 cuNumsInDisk;
};

extern Datum query_imcstore_views(PG_FUNCTION_ARGS);

extern IMCStoreView* SearchAllIMCStoreViews(uint32 *num);
#endif // QUERY_IMCSTORE_VIEWS_H
