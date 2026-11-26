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

#include "postgres.h"
#include "utils/expandeddatum.h"


/*
 * If the Datum represents a R/W expanded object, change it to R/O.
 * Otherwise return the original Datum.
 *
 * Caller must ensure that the datum is a non-null varlena value.  Typically
 * this is invoked via MakeExpandedObjectReadOnly(), which checks that.
 */
Datum
MakeExpandedObjectReadOnlyInternal(Datum d)
{
    return d;
}