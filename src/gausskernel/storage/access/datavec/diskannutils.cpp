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
 * -------------------------------------------------------------------------
 *
 * diskannutils.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/datavec/diskannutils.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/datavec/diskann.h"

/*
 * Get type info
 */
const DiskAnnTypeInfo* DiskAnnGetTypeInfo(Relation index)
{
    FmgrInfo* procinfo = DiskAnnOptionalProcInfo(index, DISKANN_TYPE_INFO_PROC);

    if (procinfo == NULL) {
        static const DiskAnnTypeInfo typeInfo = {.maxDimensions = DISKANN_MAX_DIM,
                                                 .supportPQ = true,
                                                 .itemSize = VectorItemSize,
                                                 .normalize = l2_normalize,
                                                 .checkValue = NULL};
        return (&typeInfo);
    } else {
        return (const DiskAnnTypeInfo*)DatumGetPointer(OidFunctionCall0Coll(procinfo->fn_oid, InvalidOid));
    }
}

FmgrInfo* DiskAnnOptionalProcInfo(Relation index, uint16 procnum)
{
    if (!OidIsValid(index_getprocid(index, 1, procnum)))
        return NULL;

    return index_getprocinfo(index, 1, procnum);
}
