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
 * route.h
 *
 * IDENTIFICATION
 *	 src/include/pgxc/route.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef SENDROUTER_HEADER
#define SENDROUTER_HEADER

#include "catalog/pg_type.h"
#include "postgres.h"
#include "utils/relcache.h"
#include "utils/guc.h"
#include "nodes/pg_list.h"
#include "pgxcnode.h"

#define HAS_ROUTER (u_sess->attr.attr_common.enable_router && u_sess->exec_cxt.CurrentRouter != NULL &&  \
                    IS_PGXC_COORDINATOR && IsConnFromApp())
#define ENABLE_ROUTER(cmdtype) (HAS_ROUTER && ((cmdtype) == CMD_SELECT || (cmdtype) == CMD_UPDATE ||   \
                   (cmdtype) == CMD_INSERT || (cmdtype) == CMD_DELETE || (cmdtype) == CMD_MERGE))
#define ENABLE_ROUTER_DN  (IS_PGXC_DATANODE && u_sess->attr.attr_common.enable_router \
                           && u_sess->exec_cxt.is_dn_enable_router)

extern bool check_router_attr(char** newval, void** extra, GucSource source);
extern void assign_router_attr(const char* newval, void* extra);

typedef struct RouteMsg {
    char* table_name;
    List* key_vals;
    int   key_num;
} RouteMsg;

class SendRouter : public BaseObject {
public:
    SendRouter()
    {
        m_table_oid = InvalidOid;
        m_node_id = -1;
    };
    SendRouter(Oid table_oid, int node_id);
    ~SendRouter() {};
public:
    static bool ParseRouterAttr(const char* newval, RouteMsg* attr);
    static Oid GetRelationOid(const char* table_name);
    static int CountNodeId(struct RouteMsg* attr);
    int GetRouterNodeId();
    void Destory();

private:
    Oid m_table_oid;
    int m_node_id;
};


#endif /* SENDROUTER_HEADER */
