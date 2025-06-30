/*
* Copyright (c) 2025 Huawei Technologies Co.,Ltd.
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
 */

#include "postgres.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "catalog/pg_type.h"
#include "utils/elog.h"
#include "postmaster/rack_mem_cleaner.h"
#include "rack_mem_tuple_desc.h"
#include "rack_mem_cleaner_dev.h"

PG_FUNCTION_INFO_V1(rack_mem_cleaner_details);

Datum rack_mem_cleaner_details(PG_FUNCTION_ARGS)
{
    const int returnSize = 4;
    TupleDescEntry entries[returnSize] = {
        {"total_mem_count", INT8OID},
        {"queue_mem_count", INT8OID},
        {"free_mem_count", INT8OID},
        {"process_mem_count", INT8OID}
    };
    TupleDesc desc = construct_tuple_desc(entries, returnSize);
    Datum values[desc->natts];
    uint64_t results[] = {g_instance.rackMemCleanerCxt.total, g_instance.rackMemCleanerCxt.queueSize,
                          g_instance.rackMemCleanerCxt.freeCount, g_instance.rackMemCleanerCxt.countToProcess};
    bool nulls[] = {false, false, false, false};

    for (int i = 0; i < desc->natts; i++) {
        values[i] = UInt64GetDatum(results[i]);
    }
    return HeapTupleGetDatum(heap_form_tuple(desc, values, nulls));
}