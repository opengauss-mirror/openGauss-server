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

/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include "scan_iterator.h"

void
ts_scan_iterator_close(ScanIterator *iterator)
{
	ts_scanner_end_scan(&iterator->ctx, &iterator->ictx);
}

TSDLLEXPORT void
ts_scan_iterator_scan_key_init(ScanIterator *iterator, AttrNumber attributeNumber,
							   StrategyNumber strategy, RegProcedure procedure, Datum argument)
{
	Assert(iterator->ctx.scankey == NULL || iterator->ctx.scankey == iterator->scankey);
	iterator->ctx.scankey = iterator->scankey;

	if (iterator->ctx.nkeys >= EMBEDDED_SCAN_KEY_SIZE)
		elog(ERROR, "cannot scan more than %d keys", EMBEDDED_SCAN_KEY_SIZE);

	ScanKeyInit(&iterator->scankey[iterator->ctx.nkeys++],
				attributeNumber,
				strategy,
				procedure,
				argument);
}
