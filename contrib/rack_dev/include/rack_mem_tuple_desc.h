/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef OPENGAUSS_RACK_MEM_TUPLE_DESC_H
#define OPENGAUSS_RACK_MEM_TUPLE_DESC_H

#include "postgres.h"
#include "access/tupdesc.h"

struct TupleDescEntry {
    const char* attributeName;
    Oid oidTypeId;
    int32 typmod = -1;
    int attdim = 0;
};

/**
 * common function for construct tuple desc of function
 * @param tupleDescEntries columns details
 * @param size columns size
 * @return tuple desc
 */
TupleDesc construct_tuple_desc(TupleDescEntry* entries, int size);

#endif