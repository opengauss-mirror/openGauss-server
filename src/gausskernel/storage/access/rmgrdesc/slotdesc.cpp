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
 * slotdesc.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/rmgrdesc/slotdesc.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "replication/slot.h"
#include "replication/logical.h"
#include "replication/logicalfuncs.h"
#include "replication/decode.h"
#include "access/transam.h"
#include "utils/builtins.h"
#include "access/xlog_internal.h"
#include "utils/inval.h"
#include "utils/resowner.h"
#include "utils/pg_lsn.h"
#include "access/xlog.h"
#include "replication/replicainternal.h"
#include "replication/walsender.h"
#include "replication/syncrep.h"

void slot_desc(StringInfo buf, XLogReaderState *record)
{
    appendStringInfo(buf, "slot info");
}
