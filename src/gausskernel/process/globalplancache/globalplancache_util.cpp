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
* -------------------------------------------------------------------------
*
* globalplancache_util.cpp
*    global plan cache
*
* IDENTIFICATION
*     src/gausskernel/process/globalplancache/globalplancache_util.cpp
*
* -------------------------------------------------------------------------
*/

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/hash.h"
#include "access/xact.h"
#include "catalog/pgxc_node.h"
#include "commands/prepare.h"
#include "optimizer/nodegroups.h"
#include "pgxc/groupmgr.h"
#include "pgxc/pgxcnode.h"
#include "utils/dynahash.h"
#include "utils/globalplancache.h"
#include "utils/memutils.h"
#include "utils/plancache.h"
#include "utils/syscache.h"

GlobalPlanCache *GPC;

/*
 * @Description: generate the global session id with CN nodeid and the session id
 * @in num: session id
 * @return - uint64 global session id
*/
uint64 generate_global_sessid(uint64 local_id)
{
    uint64 nodeid = (uint64)u_sess->pgxc_cxt.PGXCNodeId;
    uint64 id = ((nodeid << 48) | (local_id & 0xffffffffffff));

    return id;
}

void GlobalPlanCache::RefcountAdd(CachedPlanSource *plansource) const
{
    (void)gs_atomic_add_32(&plansource->gpc.refcount, 1);
}

void GlobalPlanCache::RefcountSub(CachedPlanSource *plansource) const
{
    (void)gs_atomic_add_32(&plansource->gpc.refcount, -1);
}
