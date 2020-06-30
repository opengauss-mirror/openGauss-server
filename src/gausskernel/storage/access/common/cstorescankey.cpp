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
 * -------------------------------------------------------------------------
 *
 * cstorescankey.c
 *	  scan key support code
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/common/cstorescankey.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/cstoreskey.h"
#include "catalog/pg_collation.h"

/* initialize the scan key's fields appropriately */
void CStoreScanKeyInit(CStoreScanKey entry, uint16 flags, AttrNumber attributeNumber, CStoreStrategyNumber strategy,
                       Oid collation, RegProcedure procedure, Datum argument, Oid left_type)
{
    entry->cs_flags = flags;
    entry->cs_attno = attributeNumber;
    entry->cs_strategy = strategy;
    entry->cs_collation = collation;
    entry->cs_argument = argument;
    fmgr_info(procedure, &entry->cs_func);
    entry->cs_left_type = left_type;
}
