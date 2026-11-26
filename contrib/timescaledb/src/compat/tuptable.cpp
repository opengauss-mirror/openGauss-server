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
 * Copyright (c) 2017-2022 Timescale, Inc. All Rights Reserved.
 * Copyright (c) 2016-2017 440 Labs Inc. dba Timescale. All Rights Reserved
 */

/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 */
#include <postgres.h>
#include <access/htup.h>

#include "tuptable.h"

HeapTuple
ts_exec_fetch_slot_heap_tuple(TupleTableSlot *slot, bool materialize, bool *should_free)
{
	if (should_free)
		*should_free = false;

	return ExecFetchSlotTuple(slot);
}

void
ts_tuptableslot_set_table_oid(TupleTableSlot *slot, Oid table_oid)
{
	if (table_oid != InvalidOid)
	{
		HeapTuple tuple = ExecFetchSlotTuple(slot);
		tuple->t_tableOid = table_oid;
	}
}
