/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
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
 * knl_localsyscache_common.h
 *
 *
 *
 * IDENTIFICATION
 *        src/include/utils/knl_localsyscache_common.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef KNL_LOCALSYSCACHE_COMMON_H
#define KNL_LOCALSYSCACHE_COMMON_H
#include "utils/knl_globalsyscache_common.h"

struct LocalBaseEntry {
    Oid oid;
    Dlelem cache_elem;
    bool obj_is_nailed;
};
struct LocalPartitionEntry : LocalBaseEntry {
    Partition part;
};
struct LocalRelationEntry : LocalBaseEntry {
    Relation rel;
};

struct InvalidBaseEntry {
    int count;
    int size;
    uint32 *invalid_values;
    bool is_reset;
    InvalidBaseEntry()
    {
        count = 0;
        size = 0;
        invalid_values = NULL;
        is_reset = false;
    }
    /* base interface */
    void Init()
    {
        count = 0;
        size = 32;
        invalid_values = (uint32 *)palloc0(size * sizeof(uint32));
    }
    void ResetInitFlag()
    {
        /* we dont clean invalid_values and size variable if rebuild lsc
         * for catcache, they will be reinited when rebuild catbucket
         * for rel/part, they will never be reinited */
        count = 0;
        is_reset = false;
    }

    void InsertInvalidValue(uint32 value)
    {
        if (ExistDefValue(value)) {
            return;
        }
        if (count == size) {
            size = size * 2;
            invalid_values = (uint32 *)repalloc(invalid_values, size * sizeof(uint32));
        }
        invalid_values[count] = value;
        count++;
    }

    bool ExistValue(uint32 value)
    {
        for (int i = 0; i < count; i++) {
            if (invalid_values[i] == value) {
                return true;
            }
        }
        return false;
    }

    /* rel/partcache */

    bool ExistDefValue(uint32 value)
    {
        return ExistValue(value);
    }

    void InsertInvalidDefValue(uint32 value)
    {
        InsertInvalidValue(value);
    }

    /* catcache */
    void InsertInvalidTupleValue(uint32 value)
    {
        if (is_reset) {
            return;
        }
        InsertInvalidValue(value);
    }

    void ResetCatalog()
    {
        is_reset = true;
    }

    bool ExistTuple(uint32 value)
    {
        if (is_reset) {
            return true;
        }
        return ExistDefValue(value);
    }

    bool ExistList()
    {
        if (is_reset || count > 0) {
            return true;
        }
        return false;
    }
};

void StreamTxnContextSaveInvalidMsg(void *stc);
void StreamTxnContextRestoreInvalidMsg(void *stc);
#define EnableLocalSysCache() t_thrd.lsc_cxt.enable_lsc

/*
 * if MAX_LSC_SWAPOUT_RATIO is 0.9, MAX_LSC_FREESIZE_RATIO is 0.8
 * used_space <= threshold * 0.9 < threshold ==> max_used_space = threshold * 0.8
 * total_space > threshold >= total_space * 0.9 ==> max_total_space = threshold / 0.8
 * so max_used_space = max_total_space * 0.72
 * this means 72% memory can be used at the worst sence */
const double MAX_LSC_FREESIZE_RATIO = 0.2;
const double MAX_LSC_SWAPOUT_RATIO = 0.9;
const double MIN_LSC_SWAPOUT_RATIO = 0.7;
#endif