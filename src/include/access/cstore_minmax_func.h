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
 * cstore_minmax_func.h
 *        routines to support ColStore
 *
 *
 * IDENTIFICATION
 *        src/include/access/cstore_minmax_func.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CSTORE_MINMAX_FUNC_H
#define CSTORE_MINMAX_FUNC_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "storage/cu.h"

typedef void (*FuncSetMinMax)(Datum v, CUDesc *cuDescPtr, bool *);
typedef void (*CompareDatum)(char *minval, char *maxval, Datum v, bool *first, int *varstr_maxlen);
typedef void (*FinishCompareDatum)(const char *minval, const char *maxval, CUDesc *cuDescPtr);

extern FuncSetMinMax GetMinMaxFunc(Oid typeOid);

extern CompareDatum GetCompareDatumFunc(Oid typeOid);
extern FinishCompareDatum GetFinishCompareDatum(Oid typeOid);
extern bool IsCompareDatumDummyFunc(CompareDatum f);

#endif
