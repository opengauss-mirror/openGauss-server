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
 * cstoreskey.h
 *        openGauss column store scan key definitions.
 *
 *
 * IDENTIFICATION
 *        src/include/access/cstoreskey.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef CSTORESKEY_H
#define CSTORESKEY_H

#include "access/attnum.h"
#include "fmgr.h"

/*
 * Strategy numbers identify the semantics that particular operators have.
 */
typedef uint16 CStoreStrategyNumber;

#define InvalidCStoreStrategy ((CStoreStrategyNumber)0)

/*
 * define the strategy numbers for cstore scan.
 */
const CStoreStrategyNumber CStoreLessStrategyNumber = 1;
const CStoreStrategyNumber CStoreLessEqualStrategyNumber = 2;
const CStoreStrategyNumber CStoreEqualStrategyNumber = 3;
const CStoreStrategyNumber CStoreGreaterEqualStrategyNumber = 4;
const CStoreStrategyNumber CStoreGreaterStrategyNumber = 5;

const CStoreStrategyNumber CStoreMaxStrategyNumber = 5;

typedef struct CStoreScanKeyData {
    uint16 cs_flags;                   // no use.
    AttrNumber cs_attno;               // a sequence column numbers, begin with 0
    CStoreStrategyNumber cs_strategy;  // operator strategy number
    Oid cs_collation;                  // collation to use, if needed
    FmgrInfo cs_func;                  // op func
    Datum cs_argument;                 // op args.
    Oid cs_left_type;                  // op left type
} CStoreScanKeyData;

typedef CStoreScanKeyData *CStoreScanKey;

void CStoreScanKeyInit(CStoreScanKey entry, uint16 flags, AttrNumber attributeNumber, CStoreStrategyNumber strategy,
                       Oid collation, RegProcedure procedure, Datum argument, Oid left_type);

#endif /* CSTORESKEY_H */
