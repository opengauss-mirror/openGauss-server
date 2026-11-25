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
 * rmgrdesc.cpp
 *
 * pg_xlogdump resource managers definition
 *
 * contrib/pg_xlogdump/rmgrdesc.cpp
 */
#define FRONTEND 1
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/clog.h"
#include "access/generic_xlog.h"
#include "access/gin.h"
#include "access/gist_private.h"
#include "access/hash.h"
#include "access/hash_xlog.h"
#include "access/heapam.h"
#include "access/multixact.h"
#include "access/nbtree.h"
#include "access/ubtree.h"
#include "access/ubtreepcr.h"
#include "access/rmgr.h"
#include "access/spgist.h"
#include "access/ubtree.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/ustore/undo/knl_uundoxlog.h"
#include "catalog/storage_xlog.h"
#include "commands/dbcommands.h"
#include "commands/sequence.h"
#include "commands/tablespace.h"
#include "replication/slot.h"
#include "replication/origin.h"
#include "replication/ddlmessage.h"
#ifdef PGXC
#include "pgxc/barrier.h"
#endif
#include "rmgrdesc.h"
#include "storage/standby.h"
#include "utils/relmapper.h"
#include "replication/slot.h"
#ifdef ENABLE_MOT
#include "storage/mot/mot_xlog.h"
#endif

#include "access/ustore/knl_uredo.h"

#define PG_RMGR(symname, name, redo, desc, startup, cleanup, safe_restartpoint, undo, undo_desc, type_name) \
    {name, desc},

const RmgrDescData RmgrDescTable[RM_MAX_ID + 1] = {
#include "access/rmgrlist.h"
};
