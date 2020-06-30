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
 * cstore_roughcheck_func.h
 *         routines to support ColStore
 *
 *
 * IDENTIFICATION
 *        src/include/access/cstore_roughcheck_func.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CSTORE_ROUGHCHECK_FUNC_H
#define CSTORE_ROUGHCHECK_FUNC_H

#include "postgres.h"
#include "knl/knl_variable.h"
#include "access/cstoreskey.h"
#include "storage/cu.h"

typedef bool (*RoughCheckFunc)(CUDesc *cudesc, Datum arg);

RoughCheckFunc GetRoughCheckFunc(Oid typeOid, int strategy, Oid collation);

#endif /* CSTORE_ROUGHCHECK_FUNC_H */
