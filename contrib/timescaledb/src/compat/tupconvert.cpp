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
#include <access/tupconvert.h>

#include "tupconvert.h"
#include "compat.h"
/*
 * Perform conversion of a tuple slot according to the map.
 */
TupleTableSlot *
ts_execute_attr_map_slot(AttrNumber *attrmap, TupleTableSlot *in_slot, TupleTableSlot *out_slot)
{
	Datum *invalues;
	bool *inisnull;
	Datum *outvalues;
	bool *outisnull;
	int outnatts;
	int i;

	/* Sanity checks */
	Assert(in_slot->tts_tupleDescriptor != NULL && out_slot->tts_tupleDescriptor != NULL);
	Assert(in_slot->tts_values != NULL && out_slot->tts_values != NULL);

	outnatts = out_slot->tts_tupleDescriptor->natts;

	/* Extract all the values of the in slot. */
	slot_getallattrs(in_slot);

	/* Before doing the mapping, clear any old contents from the out slot */
	ExecClearTuple(out_slot);

	invalues = in_slot->tts_values;
	inisnull = in_slot->tts_isnull;
	outvalues = out_slot->tts_values;
	outisnull = out_slot->tts_isnull;

	/* Transpose into proper fields of the out slot. */
	for (i = 0; i < outnatts; i++)
	{
		int j = attrmap[i] - 1;

		/* attrMap[i] == 0 means it's a NULL datum. */
		if (j == -1)
		{
			outvalues[i] = (Datum) 0;
			outisnull[i] = true;
		}
		else
		{
			outvalues[i] = invalues[j];
			outisnull[i] = inisnull[j];
		}
	}

	ExecStoreVirtualTuple(out_slot);

	return out_slot;
}
