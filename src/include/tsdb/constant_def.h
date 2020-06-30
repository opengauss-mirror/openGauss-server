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
 * constant_def.h
 *      the definition of tsdb constant
 *
 * IDENTIFICATION
 *        src/include/tsdb/constant_def.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef TSDB_CONSTANT_DEF_H
#define TSDB_CONSTANT_DEF_H

#include "c.h"

namespace Tsdb {
    extern const char *TAG_TABLE_NAME_PREFIX;
    extern const char *CUDESC_TABLE_NAME_PREFIX;
    extern const char *METRIC_TABLE_NAME_PREFIX;
    extern const char *INDEX_TABLE_NAME_SUFFIX;
    extern const char *TSDATAINSERTMEMORYCONTEXT;
    extern const char *TSDATAINSERTDATAROWMEMORYCONTEXT;
    extern const int INERT_CACHE_CAPACITY;
    extern const int PARTITION_CACHE_CAPACITY;
    enum {MAX_CMMON_PREFIX_LEN = 256};
    extern const int   FIRST_TAG_ID;
    extern const uint64 MAX_TAG_ID;
    extern const uint64 TAG_ID_WARING_THRESHOLE;
    extern const int TS_RANGE_PARTKEYNUM;
}

namespace TsCudesc {
    extern const int16 COL_ID;
    extern const int16 CU_ID;
    extern const int16 MIN;
    extern const int16 MAX;
    extern const int16 ROW_COUNT;
    extern const int16 CU_MODE;
    extern const int16 SIZE;
    extern const int16 CU_POINTER;
    extern const int16 MAGIC;
    extern const int16 EXTRA;
    extern const int16 TAG_ID;
    extern const int16 MAX_ATT_NUM;
}

#endif /* TSDB_CONSTANT_DEF_H */

