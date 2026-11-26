/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "rack_mem_tuple_desc.h"
#include "funcapi.h"

TupleDesc construct_tuple_desc(TupleDescEntry* entries, int size)
{
    TupleDesc desc = CreateTemplateTupleDesc(size, false);
    for (int i = 0; i < size; i++) {
        TupleDescEntry entry = entries[i];
        AttrNumber attr_num = (AttrNumber)(i + 1);
        TupleDescInitEntry(desc, attr_num, entry.attributeName, entry.oidTypeId, entry.typmod, entry.attdim);
    }
    return BlessTupleDesc(desc);
}