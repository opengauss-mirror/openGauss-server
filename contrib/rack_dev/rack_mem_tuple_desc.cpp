/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
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