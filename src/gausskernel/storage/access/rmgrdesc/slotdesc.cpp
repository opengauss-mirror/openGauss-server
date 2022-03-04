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

const char* slot_type_name(uint8 subtype)
{
    uint8 info = subtype & ~XLR_INFO_MASK;
    if (info == XLOG_SLOT_CREATE) {
        return "slot_create";
    } else if (info == XLOG_SLOT_ADVANCE) {
        return "slot_advance";
    } else if (info == XLOG_SLOT_DROP) {
        return "slot_drop";
    } else if (info == XLOG_SLOT_CHECK) {
        return "slot_check";
    } else if (info == XLOG_TERM_LOG) {
        return "slot_term_log";
    } else {
        return "unknown_type";
    }
}

void slot_desc(StringInfo buf, XLogReaderState *record)
{
    appendStringInfo(buf, "slot info");
}
