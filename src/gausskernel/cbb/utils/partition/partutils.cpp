/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * -------------------------------------------------------------------------
 * File Name	: partutils.cpp
 * Target		: utils functions for data partition
 * Brief		:
 * Description	:
 * History	:
 *
 * IDENTIFICATION
 *	  src/gausskernel/cbb/utils/partition/partutils.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/tableam.h"
#include "catalog/pg_partition.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_type.h"
#include "datatype/timestamp.h"
#include "executor/executor.h"
#include "nodes/value.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_node.h"
#include "parser/parse_coerce.h"
#include "parser/parse_expr.h"
#include "utils/catcache.h"
#include "utils/syscache.h"
#include "utils/array.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "utils/datetime.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "utils/partcache.h"
#include "utils/partitionmap.h"
#include "utils/partitionmap_gs.h"
#include "utils/partitionkey.h"
#include "utils/date.h"
#include "utils/resowner.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "fmgr.h"
#include "utils/memutils.h"
#include "utils/datum.h"
#include "utils/knl_relcache.h"

char *PartKeyGetCstring(PartitionKey* partkeys)
{
    StringInfoData buf;
    initStringInfo(&buf);
    for (int i = 0; i < partkeys->count; i++) {
        appendStringInfoString(&buf, PartKeyGetCstring(partkeys->values[i]));
    }
    return buf.data;
}


/*
 * @@GaussDB@@
 * Brief
 * Description  : helper function to print the part key in c-string format, normally for debug/dfx
 *                output
 * input        : partKey in Const
 * return value : partKey's c-string format
 * Note         : return value is palloc-ed(), try to free the content if invoke the function frequently
 */
char *PartKeyGetCstring(Const* partKey)
{
    Datum d = partKey->constvalue;
    switch (partKey->consttype) {
        /* numeric type */
        case INT1OID: {
            return DatumGetCString(DirectFunctionCall1(int1out, d));
        }
        case INT2OID: {
            return DatumGetCString(DirectFunctionCall1(int2out, d));
        }
        case INT4OID: {
            return DatumGetCString(DirectFunctionCall1(int4out, d));
        }
        case INT8OID: {
            return DatumGetCString(DirectFunctionCall1(int8out, d));
        }
        case NUMERICOID: {
            return DatumGetCString(DirectFunctionCall1(numeric_out, d));
        }

        /* characterize type */
        case CHAROID: {
            return DatumGetCString(DirectFunctionCall1(charout, d));
        }
        case VARCHAROID: {
            return DatumGetCString(DirectFunctionCall1(varcharout, d));
        }
        case BPCHAROID: {
            return DatumGetCString(DirectFunctionCall1(bpcharout, d));
        }

        /* time and date type */
        case DATEOID: {
            return DatumGetCString(DirectFunctionCall1(date_out, d));
        }
        case TIMESTAMPOID: {
            return DatumGetCString(DirectFunctionCall1(timestamp_out, d));
        }
        case TIMESTAMPTZOID: {
            return DatumGetCString(DirectFunctionCall1(timestamptz_out, d));
        }

        default: {
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("Unhandled datatype debug info partition key %u type\n", partKey->consttype)));
        }
    }

    return pstrdup("UNKNOWN");
}
/* function to check whether a partElem is default partition key value */
bool IsDefaultValueListPartition(ListPartitionMap *listMap, ListPartElement *partElem)
{
    Assert (listMap != NULL && partElem != NULL);

    if (partElem->len == 1 && constIsMaxValue(partElem->boundary->values[0])) {
        return true;
    }

    return false;
}

/* function to check whether two partKey are identical */
bool ConstEqual(Const *c1, Const *c2)
{
    int compare = -1;
    constCompare(c1, c2, c2->constcollid, compare);
 
    return (bool)(compare == 0);
}