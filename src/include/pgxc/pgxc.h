/* -------------------------------------------------------------------------
 *
 * pgxc.h
 *		openGauss flags and connection control information
 *
 *
 * Portions Copyright (c) 1996-2011  PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * src/include/pgxc/pgxc.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PGXC_H
#define PGXC_H

#include "storage/lock/lwlock.h"
#include "postgres.h"
#include "knl/knl_variable.h"
extern bool isRestoreMode;

// @Temp Table. Normally we only execute "drop schema pg_temp_XXX" on coordinator,
// but initdb use some temp tables too, whose we must do dropping in this case,
extern bool isSingleMode;

extern bool isSecurityMode;

typedef enum {
    REMOTE_CONN_APP,
    REMOTE_CONN_COORD,
    REMOTE_CONN_DATANODE,
    REMOTE_CONN_GTM,
    REMOTE_CONN_GTM_PROXY,
    REMOTE_CONN_INTERNAL_TOOL,
    REMOTE_CONN_GTM_TOOL
} RemoteConnTypes;

#ifdef ENABLE_MULTIPLE_NODES
#define IS_PGXC_COORDINATOR (g_instance.role == VCOORDINATOR && !is_streaming_engine())
#define IS_PGXC_DATANODE (g_instance.role == VDATANODE || g_instance.role == VSINGLENODE || is_streaming_engine())
#else
#define IS_PGXC_COORDINATOR false
#define IS_PGXC_DATANODE true
#endif
#define IS_SINGLE_NODE (g_instance.role == VSINGLENODE)
#define REMOTE_CONN_TYPE u_sess->attr.attr_common.remoteConnType
#define COORDINATOR_NOT_SINGLE (g_instance.role == VDATANODE && g_instance.role != VSINGLENODE)
#define IS_SERVICE_NODE (g_instance.role == VCOORDINATOR || g_instance.role == VSINGLENODE)

#define IsConnFromApp() (u_sess->attr.attr_common.remoteConnType == REMOTE_CONN_APP)
#define IsConnFromCoord() (u_sess->attr.attr_common.remoteConnType == REMOTE_CONN_COORD)
#define IsConnFromDatanode() (u_sess->attr.attr_common.remoteConnType == REMOTE_CONN_DATANODE)
#define IsConnFromGtm() (u_sess->attr.attr_common.remoteConnType == REMOTE_CONN_GTM)
#define IsConnFromGtmProxy() (u_sess->attr.attr_common.remoteConnType == REMOTE_CONN_GTM_PROXY)
#define IsConnFromInternalTool() (u_sess->attr.attr_common.remoteConnType == REMOTE_CONN_INTERNAL_TOOL)
#define IsConnFromGTMTool() (u_sess->attr.attr_common.remoteConnType == REMOTE_CONN_GTM_TOOL)

/* Is the CN receive SQL statement ? */
#define IS_MAIN_COORDINATOR (IS_PGXC_COORDINATOR && !IsConnFromCoord())

#define ENABLE_THREAD_POOL_DN_LOGICCONN ((g_instance.attr.attr_common.enable_thread_pool && \
    g_instance.attr.attr_storage.comm_cn_dn_logic_conn &&                                   \
    IS_PGXC_DATANODE))

/* key pair to be used as object id while using advisory lock for backup */
#define XC_LOCK_FOR_BACKUP_KEY_1 0xFFFF
#define XC_LOCK_FOR_BACKUP_KEY_2 0xFFFF

#endif /* PGXC_H */
