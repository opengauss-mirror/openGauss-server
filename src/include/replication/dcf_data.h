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
 * rto_statistic.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/replication/dcf_data.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef _DCF_DATA_H
#define _DCF_DATA_H

#include "port/pg_crc32c.h"
#define DCF_DATA_VERSION 1
typedef struct DCFData {
    uint32 dcfDataVersion; /* track change */
    uint64 appliedIndex; /* corresponding to the lsn replayed */
    /*
     * Save DCF real min apply index in case that the min apply index got from dcf is 0
     * when one node has exception and can't report its apply index
     */
    uint64 realMinAppliedIdx;
    pg_crc32c crc;
} DCFData;
#endif

