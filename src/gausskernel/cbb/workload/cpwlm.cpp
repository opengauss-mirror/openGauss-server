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
 * cpwlm.cpp
 *    mainly about two things:
 *    1. Create a backend thread to collect the number of the resource package(RP) from all DN
 *        in the compute pool(CP).
 *    2. update cluster status.
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/workload/cpwlm.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xact.h"
#include "catalog/pg_authid.h"
#include "gssignal/gs_signal.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pgxc/execRemote.h"
#include "pgxc/nodemgr.h"
#include "storage/ipc.h"
#include "storage/pmsignal.h"
#include "tcop/tcopprot.h"
#include "utils/atomic.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/ps_status.h"
#include "workload/commgr.h"
#include "workload/cpwlm.h"
#include "workload/dywlm_client.h"
#include "workload/workload.h"
#include <mntent.h>
#ifdef PGXC
#include "pgxc/pgxc.h"
#endif

#define MAX_DATANODE_NUMBER 4096
#define MAX_SEGMENT_CONF 10

static void CPmonitor_MainLoop(int dn_num);
static void NormalBackendInit();
static void CPmonitorSigTermHandler(SIGNAL_ARGS);

static void collect_rpnumber_from_DNs();
static int send_request(PGXCNodeHandle* handle, char subCmdId);
static ComputePoolState* copy_cluster_state();
static PGXCNodeHandle* get_ccn_conn();
static List* get_dnlist_from_cluster_state(int needed);

static int request_for_rpnumber(PGXCNodeHandle* handle);
static void send_rpnumber();
static bool update_cluster_state(PGXCNodeHandle** handles, List* connlist);
static void update_rp(int dn, const char* msg, int len, char* nodename);

static int request_for_clusterstate(PGXCNodeHandle* handle);
static void send_cluster_state();
static ComputePoolState* receive_cluster_state(PGXCNodeHandle* handle);

static int request_for_dnlist(PGXCNodeHandle* handle, int neededDNnum);
static void send_dnlist(int neededDNnum);
static List* receive_dnlist(PGXCNodeHandle* handle);
static List* assemble_dnlist(const char* msg, int len);

static void assemble_cpinfo(const char* msg, int len);
static void receive_cp_runtime_info(PGXCNodeHandle* handle);

static void send_cpinfo();
static int request_for_cpinfo(PGXCNodeHandle* handle);
static ComputePoolState* get_cluster_state_from_ccn();
static PGXCNodeHandle* get_dn_conn(int dnInx);
static void set_rpnumber(int dn, int rpnumber, bool state);

char* trim(char* src)
{
    char* s = 0;
    char* e = 0;
    char* c = 0;

    for (c = src; c && *c; ++c) {
        if (isspace(*c)) {
            if (NULL == e) {
                e = c;
            }
        } else {
            if (NULL == s) {
                s = c;
            }
            e = 0;
        }
    }

    if (NULL == s) {
        s = src;
    }

    if (e != NULL) {
        *e = 0;
    }
    return s;
}

/*
 * @Description: entry function to process all compute pool requests, 'A' is the top
 *                       command ID.
 * @IN : input message
 * @RETURN: void
 * @See also: None
 */
void process_request(StringInfo input_message)
{
    int subtype;
    int neededDNnum;

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    subtype = pq_getmsgbyte(input_message);

    switch (subtype) {
        case 'R':  // cp DN => cp CCN
            send_rpnumber();
            break;

        case 'D':  // cp CCN => cp CN
            neededDNnum = pq_getmsgint(input_message, 4);
            send_dnlist(neededDNnum);
            break;

        case 'C':  // cp CCN => cp CN
            send_cluster_state();
            break;

        case 'I':  // cp CN => DWS CN
            t_thrd.utils_cxt.CurrentResourceOwner = ResourceOwnerCreate(NULL, "cpwlm",
                THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_CBB));
            exec_init_poolhandles();
            send_cpinfo();
            break;

        case 'P':  // DWS DN => cp CN
            u_sess->wlm_cxt->wlm_userpl = pq_getmsgint64(input_message);
            break;

        default:
            ereport(ERROR,
                (errmodule(MOD_WLM_CP),
                    errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("invalid compute pool message subtype %d", subtype)));
            break;
    }
}

/*
 * @Description: send the runtime info(free rp number, dnnum, version) from
 *                       CN of the compute pool to DWS CN.
 * @IN : void
 * @RETURN: void
 * @See also: request_for_rpnumber()
 */
static void send_cpinfo()
{
    int freerp = 0;
    int dnnum = 0;

    int dn_num_in_cp = 0;

    if (!IS_PGXC_COORDINATOR) {
        ereport(ERROR, (errmodule(MOD_WLM_CP),
                    errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("only coordinator could receive compute pool message type 'I'")));
    }

    if (t_thrd.pgxc_cxt.shmemNumDataNodes != NULL && (*t_thrd.pgxc_cxt.shmemNumDataNodes)) {
        dn_num_in_cp = *t_thrd.pgxc_cxt.shmemNumDataNodes;
    }

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    /*
     * max_resource_package == 0, means it is the compute pool for HDFS foreignscan.
     * max_resource_package != 0, means it is the compute pool for OBS foreignscan.
     */
    if (g_instance.attr.attr_sql.max_resource_package) {
        ComputePoolState* cps = get_cluster_state();

        if (NULL == cps) {
            freerp = 0;
            dnnum = dn_num_in_cp;
        } else {
            int inuse = 0;

            DNState* dns = cps->dn_state;

            for (int dn = 0; dn < dn_num_in_cp; dn++) {
                inuse += dns[dn].num_rp;

                if (dns[dn].is_normal) {
                    dnnum++;
                }
            }

            freerp = g_instance.attr.attr_sql.max_resource_package * dn_num_in_cp - inuse;
        }
    } else {
        freerp = u_sess->attr.attr_resource.max_active_statements;
        dnnum = dn_num_in_cp;
    }

    /* special case: we cann't get the number of the datanode from t_thrd.pgxc_cxt.shmemNumDataNodes. */
    if (dnnum == 0) {
        dnnum = MAX_DATANODE_NUMBER;
    }

    /* send cp runtime info back. */
    StringInfoData retbuf;

    pq_beginmessage(&retbuf, 'I');

    pq_sendint(&retbuf, dnnum, sizeof(int));

    pq_sendint(&retbuf, freerp, sizeof(int));

    char* version = get_version();
    pq_sendbytes(&retbuf, version, strlen(version) + 1);

    pq_endmessage(&retbuf);

    pq_flush();

    pfree(version);
}

/*
 * @Description: send rpnumber(resource package number in use) to CCN
 * @IN : void
 * @RETURN: void
 * @See also: request_for_rpnumber()
 */
static void send_rpnumber()
{
    StringInfoData retbuf;

    if (!IS_PGXC_DATANODE) {
        ereport(ERROR, (errmodule(MOD_WLM_CP),
                    errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("only datanode could receive compute pool message type 'R'")));
    }

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    pq_beginmessage(&retbuf, 'R');

    pq_sendint(&retbuf, g_instance.wlm_cxt->rp_number_in_dn, sizeof(g_instance.wlm_cxt->rp_number_in_dn));

    pq_endmessage(&retbuf);

    pq_flush();
}

/*
 * @Description: send cluster state from CCN to other CN for pv_compute_pool_workload().
 * @IN : void
 * @RETURN: void
 * @See also:
 */
static void send_cluster_state()
{
    StringInfoData retbuf;

    if (!IS_PGXC_COORDINATOR) {
        ereport(ERROR, (errmodule(MOD_WLM_CP),
                    errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("only coordinator could receive compute pool message type 'C'")));
    }

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    pq_beginmessage(&retbuf, 'C');

    pq_sendint(&retbuf, g_instance.wlm_cxt->dnnum_in_cluster_state, sizeof(int));

    LWLockAcquire(ClusterRPLock, LW_EXCLUSIVE);

    if (g_instance.wlm_cxt->cluster_state != NULL) {
        pq_sendbytes(&retbuf,
            (const char*)g_instance.wlm_cxt->cluster_state,
            sizeof(DNState) * g_instance.wlm_cxt->dnnum_in_cluster_state);
    }

    LWLockRelease(ClusterRPLock);

    pq_endmessage(&retbuf);

    pq_flush();
}

/*
 * @Description: send dn list from CCN to other CN for a request.
 * @IN : dn number needed for a request.
 * @RETURN: void
 * @See also: receive_dnlist()
 */
static void send_dnlist(int neededDNnum)
{
    if (!IS_PGXC_COORDINATOR) {
        ereport(ERROR, (errmodule(MOD_WLM_CP),
                    errcode(ERRCODE_SYNTAX_ERROR),
                    errmsg("only coordinator could receive compute pool message type 'D'")));
    }

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    ListCell* lc = NULL;
    StringInfoData retbuf;

    List* dnList = get_dnlist_from_cluster_state(neededDNnum);

    pq_beginmessage(&retbuf, 'D');

    foreach (lc, dnList) {
        int dn = lfirst_int(lc);

        pq_sendint(&retbuf, dn, sizeof(int));
    }

    pq_endmessage(&retbuf);

    pq_flush();
}

/*
 * @Description: get version string
 * @IN : void
 * @RETURN: version string
 * @See also:
 */
char* get_version()
{
    Assert(PG_VERSION_STR);

    char* version = (char*)palloc0(strlen(PG_VERSION_STR) + 1);

    errno_t ss_rc = memcpy_s(version, strlen(PG_VERSION_STR) + 1, PG_VERSION_STR, strlen(PG_VERSION_STR));

    securec_check(ss_rc, "\0", "\0");

    char* p = strchr(version, '(');
    if (NULL == p) {
        return "unknown version";
    }

    char* v = ++p;

    char* t = strchr(v, ')');
    if (NULL == t) {
        return "unknown version";
    }

    *t = '\0';

    char* result = (char*)palloc0(strlen(v) + 1);

    ss_rc = memcpy_s(result, strlen(v) + 1, v, strlen(v));

    securec_check(ss_rc, "\0", "\0");

    pfree(version);

    return result;
}

/*
 * @Description: send request for the info of the comptue pool(including load, version...).
 * @IN : conn of CCN, dn number needed for a request.
 * @RETURN: 0: success; EOF: failed.
 * @See also:
 */
int request_for_cpinfo(PGXCNodeHandle* handle)
{
    Assert(IS_PGXC_COORDINATOR);

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    return send_request(handle, 'I');
}

/*
 * @Description: get runtime info of the compute pool from  input message.
 * @IN : msg, len
 * @RETURN: void
 * @See also:
 */
static void assemble_cpinfo(const char* msg, int len)
{
    Assert(IS_PGXC_COORDINATOR);

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    u_sess->wlm_cxt->cp_runtime_info = (CPRuntimeInfo*)palloc0(sizeof(CPRuntimeInfo));

    StringInfoData input_msg;

    initStringInfo(&input_msg);

    appendBinaryStringInfo(&input_msg, msg, len);

    u_sess->wlm_cxt->cp_runtime_info->dnnum = pq_getmsgint(&input_msg, sizeof(int));
    u_sess->wlm_cxt->cp_runtime_info->freerp = pq_getmsgint(&input_msg, sizeof(int));

    if ((uint)(len + 1) <= sizeof(int) * 2) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("len is invalid[%d]", len)));
    }
    char* buf = (char*)palloc0(len - sizeof(int) * 2 + 1);
    pq_copymsgbytes(&input_msg, buf, len - sizeof(int) * 2);

    u_sess->wlm_cxt->cp_runtime_info->version = buf;

    pq_getmsgend(&input_msg);

    pfree(input_msg.data);
}

/*
 * @Description: receive input message which contains dn list from CCN.
 * @IN : conn to CCN
 * @RETURN: dn list
 * @See also:
 */
static void receive_cp_runtime_info(PGXCNodeHandle* handle)
{
    Assert(IS_PGXC_COORDINATOR);

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    char* err = NULL;
    bool hasError = false;

    struct timeval timeout = {30, 0};

    for (;;) {
        if (pgxc_node_receive(1, &handle, &timeout)) {
            ereport(LOG,
                (errmsg("%s:%d recv fail", __FUNCTION__, __LINE__)));
            break;
        }

        int len;
        char* msg = NULL;

        handle->state = DN_CONNECTION_STATE_IDLE;

        char msg_type = get_message(handle, &len, &msg);

        /* check message type */
        switch (msg_type) {
            case '\0': /* message is not completed */
                err = "incomplete message from the compute pool";
                hasError = true;
                break;

            case 'E': /* cn is down or crashed */
                err = "CN of the compute pool is down or crashed";
                hasError = true;
                break;

            case 'I': /* get cluster state */
                assemble_cpinfo(msg, len);
                return;

            default: /* never get here */
                err = "unknown error from the compute pool";
                hasError = true;
                break;
        }
    }

    /* "cp_runtime_info == NULL" means timeout */
    if (hasError || NULL == u_sess->wlm_cxt->cp_runtime_info) {
        ereport(LOG,
            (errmodule(MOD_WLM_CP),
                errcode(ERRCODE_CONNECTION_FAILURE),
                errmsg("Failed to get runtime info from the compute pool, cause: %s", err ? err : "unknown error")));

        ereport(ERROR,
            (errmodule(MOD_WLM_CP),
                errcode(ERRCODE_CONNECTION_FAILURE),
                errmsg("Failed to get runtime info from the compute pool.")));
    }
}

/*
 * @Description: send request to get the runtime info from DWS CN to the compute pool.
 * @IN : conn of the compute pool.
 * @RETURN: void
 * @See also:
 */
void get_cp_runtime_info(PGXCNodeHandle* handle)
{
    Assert(IS_PGXC_COORDINATOR);

    u_sess->wlm_cxt->cp_runtime_info = NULL;

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    if (EOF == request_for_cpinfo(handle)) {
        ereport(ERROR,
            (errmodule(MOD_WLM_CP),
                errcode(ERRCODE_CANNOT_CONNECT_NOW),
                errmsg("Failed to send request to ccn: %s for cp runtime info! cause: %s",
                    handle->connInfo.host.data,
                    handle->error ? handle->error : "unknown")));
    }
    return receive_cp_runtime_info(handle);
}

/*
 * @Description: send request to CCN for dn list.
 * @IN : conn of CCN, dn number needed for a request.
 * @RETURN: 0: success; EOF: failed.
 * @See also:
 */
static int request_for_dnlist(PGXCNodeHandle* handle, int neededDNnum)
{
    Assert(IS_PGXC_COORDINATOR);

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    errno_t ss_rc = 0;
    int msgLen = 0;

    if (handle->state != DN_CONNECTION_STATE_IDLE) {
        ereport(DEBUG1, (errmodule(MOD_WLM_CP), errmsg("invalid connection state: %d", handle->state)));

        return EOF;
    }

    /* msgType + msgLen */
    /* 1: msgtype, 4: msglen, 1: msg subtype, 4: dn number needed */
    ensure_out_buffer_capacity(1 + 4 + 1 + 4, handle);

    Assert(handle->outBuffer != NULL);

    handle->outBuffer[handle->outEnd++] = 'A';

    msgLen = 4 + 1 + 4;
    msgLen = htonl(msgLen);
    ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &msgLen, 4);
    securec_check(ss_rc, "\0", "\0");
    handle->outEnd += 4;

    /* assemble sub command in 'A' */
    handle->outBuffer[handle->outEnd++] = 'D';

    neededDNnum = htonl(neededDNnum);
    ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &neededDNnum, 4);
    securec_check(ss_rc, "\0", "\0");
    handle->outEnd += 4;

    handle->state = DN_CONNECTION_STATE_QUERY;

    return pgxc_node_flush(handle);
}

/*
 * @Description: get dn list from cluster state in CCN.
 * @IN : dn number needed for a request.
 * @RETURN: dn list
 * @See also:
 */
static List* get_dnlist_from_cluster_state(int needed)
{
    static int dn_start = 0;

    List* dnlist = NIL;

    Assert(IS_PGXC_COORDINATOR);

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    if (NULL == g_instance.wlm_cxt->cluster_state) {
        return NIL;
    }

    if (needed > g_instance.wlm_cxt->dnnum_in_cluster_state) {
        needed = g_instance.wlm_cxt->dnnum_in_cluster_state;
    }

    LWLockAcquire(ClusterRPLock, LW_EXCLUSIVE);

    if (dn_start >= g_instance.wlm_cxt->dnnum_in_cluster_state) {
        dn_start %= g_instance.wlm_cxt->dnnum_in_cluster_state;
    }

    int index = 0;
    for (int dn = 0; dn < g_instance.wlm_cxt->dnnum_in_cluster_state; dn++) {
        index = dn + dn_start;
        if (index >= g_instance.wlm_cxt->dnnum_in_cluster_state) {
            index -= g_instance.wlm_cxt->dnnum_in_cluster_state;
        }

        if (g_instance.wlm_cxt->cluster_state[index].is_normal &&
            g_instance.wlm_cxt->cluster_state[index].num_rp < g_instance.attr.attr_sql.max_resource_package) {
            dnlist = lappend_int(dnlist, index);
        }

        if (list_length(dnlist) >= needed) {
            break;
        }
    }

    dn_start = index;

    /* no enough resource package for this request */
    if (list_length(dnlist) < needed) {
        LWLockRelease(ClusterRPLock);

        return NIL;
    }

    ListCell* lc = NULL;
    foreach (lc, dnlist) {
        int dn = lfirst_int(lc);
        ++g_instance.wlm_cxt->cluster_state[dn].num_rp;
    }

    LWLockRelease(ClusterRPLock);

    return dnlist;
}

/*
 * @Description: entry function in CN to get dn list to run a request pushdowned from DWS.
 * @IN : dn number needed for a request.
 * @RETURN: dn list
 * @See also:
 */
List* get_dnlist(int neededDNnum)
{
    Assert(IS_PGXC_COORDINATOR);

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    if (g_instance.wlm_cxt->is_ccn) {
        return get_dnlist_from_cluster_state(neededDNnum);
    }

    /*
     * this is a CN not CCN, so send request to CCN to get cluster state.
     */
    PGXCNodeHandle* handle = get_ccn_conn();

    if (NULL == handle) {
        ereport(ERROR, (errmodule(MOD_WLM_CP), errcode(ERRCODE_UNEXPECTED_NULL_VALUE), errmsg("No CCN in cluster!")));
    }

    if (EOF == request_for_dnlist(handle, neededDNnum)) {
        ereport(ERROR,
            (errmodule(MOD_WLM_CP),
                errcode(ERRCODE_CANNOT_CONNECT_NOW),
                errmsg("Failed to send request to ccn: %s for dn list! dnnum: %d, cause: %s",
                    handle->connInfo.host.data,
                    neededDNnum,
                    handle->error ? handle->error : "unknown")));
    }

    return receive_dnlist(handle);
}

/*
 * @Description: make dn list from input message.
 * @IN : input message
 * @RETURN: dn list
 * @See also:
 */
static List* assemble_dnlist(const char* msg, int len)
{
    List* dnlist = NIL;

    Assert(IS_PGXC_COORDINATOR);

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    StringInfoData input_msg;

    initStringInfo(&input_msg);

    appendBinaryStringInfo(&input_msg, msg, len);

    for (int i = 0; i < len / 4; i++) {
        int dn = pq_getmsgint(&input_msg, sizeof(int));

        dnlist = lappend_int(dnlist, dn);
    }

    pq_getmsgend(&input_msg);

    pfree(input_msg.data);

    return dnlist;
}

/*
 * @Description: receive input message which contains dn list from CCN.
 * @IN : conn to CCN
 * @RETURN: dn list
 * @See also:
 */
static List* receive_dnlist(PGXCNodeHandle* handle)
{
    Assert(IS_PGXC_COORDINATOR);

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    char* err = NULL;
    bool hasError = false;

    struct timeval timeout = {30, 0};

    for (;;) {
        if (pgxc_node_receive(1, &handle, &timeout)) {
            ereport(LOG,
                (errmsg("%s:%d recv fail", __FUNCTION__, __LINE__)));
            break;
        }

        int len;
        char* msg = NULL;

        handle->state = DN_CONNECTION_STATE_IDLE;

        char msg_type = get_message(handle, &len, &msg);

        /* check message type */
        switch (msg_type) {
            case '\0': /* message is not completed */
                err = "incomplete message";
                hasError = true;
                break;

            case 'E': /* cn is down or crashed */
                err = "cn is down or crashed";
                hasError = true;
                break;

            case 'D': /* get cluster state */
                if (0 == len) {
                    return NIL;
                } else {
                    return assemble_dnlist(msg, len);
                }
                break;

            default: /* never get here */
                err = "invalid msg type";
                hasError = true;
                break;
        }
    }

    if (hasError) {
        ereport(ERROR,
            (errmodule(MOD_WLM_CP),
                errcode(ERRCODE_CANNOT_CONNECT_NOW),
                errmsg("Failed to get dn list from CCN: %s, cause: %s", handle->connInfo.host.data, err)));
    }

    /* keep compiler silence */
    return NIL;
}

/*
 * @Description: run in CCN, return a copy of cluster state for pv_compute_pool_workload().
 * @IN : None
 * @RETURN: a copy of cluster state
 * @See also: get_cluster_state()
 */
static ComputePoolState* copy_cluster_state()
{
    size_t size;
    errno_t ss_rc;

    Assert(IS_PGXC_COORDINATOR);

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    if (!g_instance.wlm_cxt->cluster_state) {
        return NULL;
    }

    size = sizeof(DNState) * g_instance.wlm_cxt->dnnum_in_cluster_state;

    DNState* ptr = (DNState*)palloc0(size);

    LWLockAcquire(ClusterRPLock, LW_EXCLUSIVE);

    ss_rc = memcpy_s(ptr, size, g_instance.wlm_cxt->cluster_state, size);

    LWLockRelease(ClusterRPLock);

    securec_check(ss_rc, "\0", "\0");

    ComputePoolState* cps = (ComputePoolState*)palloc0(sizeof(ComputePoolState));
    cps->dn_num = g_instance.wlm_cxt->dnnum_in_cluster_state;
    cps->dn_state = ptr;
    return cps;
}

/*
 * @Description: called by pv_compute_pool_workload() to show cluster state.
 * @IN : None.
 * @RETURN:  a copy of cluster state
 * @See also:
 */
ComputePoolState* get_cluster_state()
{
    Assert(IS_PGXC_COORDINATOR);

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    if (g_instance.wlm_cxt->is_ccn) {
        return copy_cluster_state();
    }

    return get_cluster_state_from_ccn();
}

/*
 * @Description: get cluster state from ccn.
 * @IN : None.
 * @RETURN:  a copy of cluster state
 * @See also:
 */
static ComputePoolState* get_cluster_state_from_ccn()
{
    Assert(IS_PGXC_COORDINATOR);

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    ComputePoolState* result = NULL;

    /*
     * this is a CN not CCN, so send request to CCN to get cluster state.
     */
    MemoryContext current_ctx;

    current_ctx = CurrentMemoryContext;

    PG_TRY();
    {
        PGXCNodeHandle* handle = get_ccn_conn();

        if (EOF == request_for_clusterstate(handle)) {
            ereport(ERROR,
                (errmodule(MOD_WLM_CP), errcode(ERRCODE_CANNOT_CONNECT_NOW), errmsg("Failed to send request to CCN.")));
        }

        result = receive_cluster_state(handle);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(current_ctx);

        /* Save error info */
        ErrorData* edata = CopyErrorData();

        ereport(DEBUG1,
            (errmodule(MOD_WLM_CP),
                errmsg("Failed to get the workload of the compute pool from CCN.\nreason: %s", edata->message)));

        FlushErrorState();

        FreeErrorData(edata);
    }
    PG_END_TRY();

    if (NULL == result) {
        ereport(DEBUG1, (errmodule(MOD_WLM_CP), errmsg("The workload of the compute pool is not ready current.")));
    }

    return result;
}

/*
 * @Description: get connection to ccn.
 * @IN : none
 * @RETURN:  a conn to ccn
 * @See also:
 */
static PGXCNodeHandle* get_ccn_conn()
{
    Assert(IS_PGXC_COORDINATOR);

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    PGXCNodeAllHandles* pgxc_handles = NULL;

    if (-1 == g_instance.wlm_cxt->ccn_idx) {
        ereport(ERROR,
            (errmodule(MOD_WLM_CP),
                errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("invalid ccn index %d", g_instance.wlm_cxt->ccn_idx)));
    }

    List* coordlist = NIL;
    coordlist = lappend_int(coordlist, g_instance.wlm_cxt->ccn_idx);

    /* get connection of CCN */
    pgxc_handles = get_handles(NULL, coordlist, true);

    return pgxc_handles->coord_handles[0];
}

/*
 * @Description: parse message from the CCN to the state of the compute pool.
 * @IN : msg: data from CCN
 *          len:  the length of the msg
 * @RETURN:  cluster state of the compute pool.
 * @See also:
 */
static ComputePoolState* get_computepool_state(const char* msg, int len)
{
    if (len <= (int)sizeof(int)) {
        ereport(DEBUG1, (errmodule(MOD_WLM_CP), errmsg("incomplete message")));
        return NULL;
    }

    if ((len - sizeof(int)) % sizeof(DNState) != 0) {
        ereport(DEBUG1, (errmodule(MOD_WLM_CP), errmsg("incomplete message")));
        return NULL;
    }

    ComputePoolState* cps = (ComputePoolState*)palloc0(sizeof(ComputePoolState));

    StringInfoData input_msg;
    initStringInfo(&input_msg);

    appendBinaryStringInfo(&input_msg, msg, len);

    /* parse message to get record */
    int dn_num = pq_getmsgint(&input_msg, sizeof(int));

    size_t size = sizeof(DNState) * dn_num;
    if (size > MaxAllocSize) {
        ereport(ERROR, (errmodule(MOD_WLM_CP), errmsg("invalid size [%lu]", size)));
    }

    DNState* dnstate = (DNState*)palloc0(size);
    errno_t ss_rc = memcpy_s(dnstate, size, msg + sizeof(int), size);
    securec_check(ss_rc, "\0", "\0");

    cps->dn_num = dn_num;
    cps->dn_state = dnstate;

    pfree(input_msg.data);

    return cps;
}

/*
 * @Description: receive cluster state from CCN.
 * @IN : conn to CCN
 * @RETURN:  a copy of cluster state
 * @See also:
 */
static ComputePoolState* receive_cluster_state(PGXCNodeHandle* handle)
{
    Assert(IS_PGXC_COORDINATOR);

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    char* err = NULL;
    bool hasError = false;

    struct timeval timeout = {30, 0};

    for (;;) {
        if (pgxc_node_receive(1, &handle, &timeout)) {
            ereport(LOG,
                (errmsg("%s:%d recv fail", __FUNCTION__, __LINE__)));
            break;
        }

        int len;
        char* msg = NULL;

        handle->state = DN_CONNECTION_STATE_IDLE;

        char msg_type = get_message(handle, &len, &msg);

        /* check message type */
        switch (msg_type) {
            case '\0': /* message is not completed */
                err = "incomplete message";
                hasError = true;
                break;

            case 'E': /* cn is down or crashed */
                err = "cn is down or crashed";
                hasError = true;
                break;

            case 'C': /* get cluster state */
                return get_computepool_state(msg, len);

            default: /* never get here */
                err = "invalid msg type";
                hasError = true;
                break;
        }
    }

    if (hasError) {
        ereport(ERROR, (errmodule(MOD_WLM_CP), errcode(ERRCODE_CANNOT_CONNECT_NOW), errmsg("%s", err)));
    }

    /* keep compiler silence */
    return NULL;
}

/*
 * @Description: send request to CCN for cluster state.
 * @IN : conn of the CCN.
 * @RETURN: 0: success; EOF: failed
 * @See also:
 */
static int request_for_clusterstate(PGXCNodeHandle* handle)
{
    Assert(IS_PGXC_COORDINATOR);

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    return send_request(handle, 'C');
}

/*
 * @Description: send request just with sub command ID.
 * @IN : conn
 * @RETURN: 0: success; EOF: failed
 * @See also:
 */
static int send_request(PGXCNodeHandle* handle, char subCmdId)
{
    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    errno_t ss_rc = 0;
    int msgLen = 0;

    if (handle->state != DN_CONNECTION_STATE_IDLE) {
        ereport(DEBUG1, (errmodule(MOD_WLM_CP), errmsg("invalid connection state: %d", handle->state)));

        return EOF;
    }

    /*
     * msgType(1 byte) + msgLen(4 bytes) + sub command ID(1 byte)
     */
    ensure_out_buffer_capacity(1 + 4 + 1, handle);

    Assert(handle->outBuffer != NULL);

    /* 'A' means acceleration command ID. */
    handle->outBuffer[handle->outEnd++] = 'A';

    /* not include msgType(1 byte), just for msgLen(4 bytes) + command ID(1 byte) */
    msgLen = 4 + 1;
    msgLen = htonl(msgLen);
    ss_rc = memcpy_s(handle->outBuffer + handle->outEnd, handle->outSize - handle->outEnd, &msgLen, 4);
    securec_check(ss_rc, "\0", "\0");
    handle->outEnd += 4;

    /* assemble sub command in 'A' */
    handle->outBuffer[handle->outEnd++] = subCmdId;

    handle->state = DN_CONNECTION_STATE_QUERY;

    return pgxc_node_flush(handle);
}

/*
 * @Description: send request to DN for rp(resource package) number in use.
 * @IN : conn of the DN.
 * @RETURN: 0: success; EOF: failed
 * @See also:
 */
static int request_for_rpnumber(PGXCNodeHandle* handle)
{
    Assert(IS_PGXC_COORDINATOR);

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    return send_request(handle, 'R');
}

/*
 * @Description: update cluster according to rp number from DN.
 * @IN : message from DN.
 * @RETURN: void
 * @See also:
 */
static void update_rp(int dn, const char* msg, int len, char* nodename)
{
    Assert(IS_PGXC_COORDINATOR);

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    int num_rp = 0;
    bool is_normal = true;

    StringInfoData input_msg;
    initStringInfo(&input_msg);

    appendBinaryStringInfo(&input_msg, msg, len);

    /* parse message to get record */
    num_rp = pq_getmsgint(&input_msg, 4);

    pq_getmsgend(&input_msg);

    pfree(input_msg.data);

    /* check rp number from DN. */
    if (num_rp < 0 || num_rp > g_instance.attr.attr_sql.max_resource_package) {
        ereport(DEBUG1,
            (errmodule(MOD_WLM_CP),
                errmsg("invalid resource package number from DN: %s, "
                       "rp number: %d, max_resource_package: %d",
                    nodename,
                    num_rp,
                    g_instance.attr.attr_sql.max_resource_package)));

        num_rp = g_instance.attr.attr_sql.max_resource_package;

        is_normal = false;
    }

    set_rpnumber(dn, num_rp, is_normal);
}

/*
 * @Description: get the conn to dn that dn index is dnInx.
 * @IN : dn index.
 * @RETURN: the conn to dn.
 * @See also:
 */
static PGXCNodeHandle* get_dn_conn(int dnInx)
{
    List* dnlist = NIL;

    dnlist = lappend_int(dnlist, dnInx);

    bool available = false;
    MemoryContext current_ctx;
    PGXCNodeAllHandles* handles = NULL;

    available = true;
    current_ctx = CurrentMemoryContext;

    PG_TRY();
    {
        handles = get_handles(dnlist, NULL, false);
    }
    PG_CATCH();
    {
        MemoryContextSwitchTo(current_ctx);

        /* Save error info */
        ErrorData* edata = CopyErrorData();

        ereport(LOG, (errmodule(MOD_WLM_CP), errmsg("Failed to get connection to DN index: %d", dnInx)));

        FlushErrorState();

        FreeErrorData(edata);

        available = false;
    }
    PG_END_TRY();

    if (!available) {
        return NULL;
    }

    PGXCNodeHandle* handle = handles->datanode_handles[0];

    pfree(handles);

    return handle;
}

/*
 * @Description: update dn state.
 * @IN : ...
 * @RETURN: void
 * @See also:
 */
static void set_rpnumber(int dn, int rpnumber, bool state)
{
    LWLockAcquire(ClusterRPLock, LW_EXCLUSIVE);

    g_instance.wlm_cxt->cluster_state[dn].num_rp = rpnumber;

    g_instance.wlm_cxt->cluster_state[dn].is_normal = state;

    LWLockRelease(ClusterRPLock);
}

/*
 * @Description: get the number of resource package in use from DN.
 * @IN : conn of all DNs.
 * @RETURN: void
 * @See also:
 */
static void collect_rpnumber_from_DNs()
{
    Assert(IS_PGXC_COORDINATOR);

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    int dn = 0;

    List* connlist = NULL;

    bool hasError = false;

    PGXCNodeHandle** handles =
        (PGXCNodeHandle**)palloc0(sizeof(PGXCNodeHandle*) * g_instance.wlm_cxt->dnnum_in_cluster_state);

    /*
     * send request of "get the number of rp in use" to all DNs.
     */
    for (dn = 0; dn < g_instance.wlm_cxt->dnnum_in_cluster_state; dn++) {
        PGXCNodeHandle* handle = get_dn_conn(dn);

        if (NULL == handle) {
            set_rpnumber(dn, g_instance.attr.attr_sql.max_resource_package, false);

            ereport(DEBUG1, (errmodule(MOD_WLM_CP), errmsg("No conn to dn index: %d", dn)));

            hasError = true;

            continue;
        }

        if (EOF == request_for_rpnumber(handle)) {
            set_rpnumber(dn, g_instance.attr.attr_sql.max_resource_package, false);

            ereport(LOG,
                (errmodule(MOD_WLM_CP),
                    errmsg("Failed to send request to dn: %s to collect RP number! cause: %s",
                        handle->connInfo.host.data,
                        handle->error ? handle->error : "unknown")));

            hasError = true;

            continue;
        }

        connlist = lappend_int(connlist, dn);

        handles[dn] = handle;
    }

    if (false == update_cluster_state(handles, connlist)) {
        hasError = true;
    }

    pfree(handles);

    if (hasError) {
        ereport(ERROR,
            (errmodule(MOD_WLM_CP),
                errcode(ERRCODE_CANNOT_CONNECT_NOW),
                errmsg("something wrong in CPmonitor thread, so reboot CPmonitor thread.")));
    }
}

/*
 * @Description: get the number of resource package from all DNs.
 * @IN : conn of all DNs.
 * @RETURN: void
 * @See also:
 */
static bool update_cluster_state(PGXCNodeHandle** handles, List* connlist)
{
    Assert(IS_PGXC_COORDINATOR && handles);

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    /*
     * receive response from all DNs, and save the number of rp in use to
     * cluster status.
     */
    char* msg = NULL;
    int len;

    char* err = NULL;
    bool hasError = false;

    while (list_length(connlist) > 0) {
        int dn = linitial_int(connlist);

        connlist = list_delete_first(connlist);

        PGXCNodeHandle* handle = handles[dn];

        Assert(handle);

        struct timeval timeout = {30, 0};

        /* receive workload records from each node */
        for (;;) {
            if (pgxc_node_receive(1, &handle, &timeout)) {
                ereport(LOG, (errmsg("%s:%d recv fail", __FUNCTION__, __LINE__)));
                break;
            }

            handle->state = DN_CONNECTION_STATE_IDLE;

            char msg_type = get_message(handle, &len, &msg);

            /* check message type */
            switch (msg_type) {
                case '\0': /* message is not completed */
                    err = "incomplete message";
                    hasError = true;
                    break;

                case 'E': /* cn is down or crashed */
                    err = "dn is down or crashed";
                    hasError = true;
                    break;

                case 'R': /* get workload record */
                    update_rp(dn, msg, len, handle->connInfo.host.data);
                    break;

                default: /* never get here */
                    err = "invalid msg type";
                    hasError = true;
                    break;
            }

            if (hasError) {
                set_rpnumber(dn, g_instance.attr.attr_sql.max_resource_package, false);

                ereport(LOG, (errmodule(MOD_WLM_CP),
                    errmsg("Failed to get rp number from DN: %s, cause: %s", handle->connInfo.host.data, err)));
            }
        }
    }

    if (hasError) {
        return false;
    }

    return true;
}

static char* get_conn_data()
{
    char abs_path[MAXPGPATH];
    errno_t rc = EOK;
    char* gausshome = NULL;
    char real_gausshome[PATH_MAX + 1] = {'\0'};

    /*
     * Important: function getenv() is not thread safe.
     */
    LWLockAcquire(OBSGetPathLock, LW_SHARED);
    gausshome = gs_getenv_r("GAUSSHOME");
    LWLockRelease(OBSGetPathLock);
    if (gausshome == NULL || realpath(gausshome, real_gausshome) == NULL) {
        ereport(ERROR, (errmodule(MOD_ACCELERATE), errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("Failed to get the values of $GAUSSHOME")));
    }

    if (backend_env_valid(real_gausshome, "GAUSSHOME") == false) {
        ereport(ERROR, (errmodule(MOD_ACCELERATE), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg("Incorrect backend environment variable $GAUSSHOME"),
            errdetail("Please refer to the backend instance log for the detail")));
    }
    rc = sprintf_s(abs_path, MAXPGPATH, "%s/bin/%s", real_gausshome, "cp_client.conf");
    securec_check_ss(rc, "", "");

    gausshome = NULL;
    rc = memset_s(real_gausshome, sizeof(real_gausshome), 0, sizeof(real_gausshome));
    securec_check(rc, "\0", "\0");

    int fd = open(abs_path, O_RDONLY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(ERROR, (errmodule(MOD_ACCELERATE),
                errcode(ERRCODE_FILE_READ_FAILED),
                errmsg("Failed to open config file to connect compute pool. file path: %s", abs_path)));
    }

    off_t size = lseek(fd, 0, SEEK_END);
    /* cp_clent.conf is only conf file, it should not be so large, the size INT_MAX/2 is 1GB */
    if (size == -1 || size > (INT_MAX / 2)) {
        close(fd);
        ereport(ERROR, (errmodule(MOD_ACCELERATE),
                errcode(ERRCODE_FILE_READ_FAILED),
                errmsg("Failed to get the size of the config file to connect compute pool. file path: %s", abs_path)));
    }
    (void)lseek(fd, 0, SEEK_SET);

    char* data = (char*)palloc0(size + 2);

    int rs = read(fd, data, size);
    if (rs != size) {
        close(fd);
        ereport(ERROR, (errmodule(MOD_ACCELERATE),
                errcode(ERRCODE_FILE_READ_FAILED),
                errmsg("Failed to get the data of the config file to connect compute pool. file path: %s", abs_path)));
    }

    close(fd);

    data[size] = '\n';

    return data;
}

static void check_compute_pool_config(ComputePoolConfig* config)
{
    if (!config->cpip || !config->cpport || !config->username || !config->password || !config->version ||
        0 == config->dnnum || 0 == config->pl) {
        ereport(ERROR,
            (errmodule(MOD_ACCELERATE),
                errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("\"cpip\", \"cpport\", \"username\", \"password\", \"version\", \"dnnum\", \"pl\""
                       " are needed to connect to the compute pool.")));
    }
}

static ComputePoolConfig* parse_segment_info(char* conn)
{
    ComputePoolConfig* config = NULL;

    config = (ComputePoolConfig*)palloc0(sizeof(ComputePoolConfig));

    StringInfo si = makeStringInfo();
    appendStringInfo(si, "%s\n", conn);

    char* line = si->data;

    while (true) {
        char* tail = strchr(line, '\n');
        if (NULL == tail) { /* end of file */
            break;
        }

        *tail = '\0';

        char* v = strchr(line, '=');
        if (NULL == v) { /* ignore line without '=' */
            line = tail + 1;
            continue;
        }

        *v = '\0';
        v = trim(v + 1);
        char* p = trim(line);

        if (!strncasecmp(p, "cpip", sizeof("cpip"))) {
            size_t len = strlen(v);
            if (';' == v[len - 1]) {
                v[len - 1] = '\0';
            }

            v = trim(v);
            config->cpip = pstrdup(v);
        } else if (!strncasecmp(p, "cpport", sizeof("cpport"))) {
            config->cpport = pstrdup(v);
        } else if (!strncasecmp(p, "username", sizeof("username"))) {
            config->username = pstrdup(v);
        } else if (!strncasecmp(p, "password", sizeof("password"))) {
            config->password = pstrdup(v);
        } else if (!strncasecmp(p, "dnnum", sizeof("dnnum"))) {
            config->dnnum = atoi(v);
        } else if (!strncasecmp(p, "version", sizeof("version"))) {
            config->version = pstrdup(v);
        } else if (!strncasecmp(p, "pl", sizeof("pl"))) {
            config->pl = atoi(v);
            if (config->pl <= 0) {
                ereport(ERROR,
                    (errmodule(MOD_ACCELERATE),
                        errcode(ERRCODE_DATA_CORRUPTED),
                        errmsg("pl should be greater than 0, pl is %d current.", config->pl)));
            }
        } else if (!strncasecmp(p, "rpthreshold", sizeof("rpthreshold"))) {
            config->rpthreshold = atoi(v);
        }

        line = tail + 1;
    }

    check_compute_pool_config(config);

    return config;
}

ComputePoolConfig** parse_conn_info(char* data, int* num)
{
    int segnum;
    char* segment[MAX_SEGMENT_CONF];
    char* tail = NULL;
    ComputePoolConfig** configs;

    configs = (ComputePoolConfig**)palloc0(sizeof(ComputePoolConfig*) * MAX_SEGMENT_CONF);

    tail = data;
    for (segnum = 0; segnum < MAX_SEGMENT_CONF; segnum++) {
        tail = strstr(tail, "cpip");
        if (NULL == tail) {
            break;
        }

        if (segnum != 0) {
            *(tail - 1) = '\0';
        }
        segment[segnum] = tail;
        tail++;
    }

    if (0 == segnum) {
        ereport(ERROR,
            (errmodule(MOD_ACCELERATE),
                errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("no valid config info in cp_client.conf.")));
    }

    for (int i = 0; i < segnum; i++) {
        configs[i] = parse_segment_info(segment[i]);
    }

    if (num != NULL) {
        *num = segnum;
    }

    return configs;
}

/*
 * @Description: in CN, get conn info of the compute pool from cp_client.conf in
 *                       data directory of the DWS DN.
 *                       in DN, the conn info is from DWS CN, do_query() will push the
 *                       conn info th all DNs.
 *
 * @IN :void
 * @RETURN: void
 * @See also:
 */
ComputePoolConfig** get_cp_conninfo(int* cnum)
{
    char* data = NULL;
    ComputePoolConfig** confs;
    int num = 0;

    data = get_conn_data();
    confs = parse_conn_info(data, &num);

    for (int i = 0; i < num; i++) {
        ComputePoolConfig* conf = confs[i];

        ereport(DEBUG1, (errmodule(MOD_ACCELERATE), errmsg("conf->cpip: %s", conf->cpip)));

        ereport(DEBUG1, (errmodule(MOD_ACCELERATE), errmsg("conf->cpport: %s", conf->cpport)));

        ereport(DEBUG1, (errmodule(MOD_ACCELERATE), errmsg("conf->username: %s", conf->username)));

        ereport(DEBUG1, (errmodule(MOD_ACCELERATE), errmsg("conf->pl: %d", conf->pl)));
    }

    if (cnum != NULL) {
        *cnum = num;
    }
    return confs;
}

/*
 * @Description: compute pool monitor main loop
 * @IN :void
 * @RETURN: void
 * @See also:
 */
static void CPmonitor_MainLoop(int dn_num)
{
    bool node_is_ccn, pre_node_is_ccn;
    ItemPointerData tuple_pos, pre_tuple_pos;

    node_is_ccn = false;
    pre_node_is_ccn = false;

    tuple_pos.ip_blkid.bi_hi = 0;
    tuple_pos.ip_blkid.bi_lo = 0;
    tuple_pos.ip_posid = 0;

    pre_tuple_pos.ip_blkid.bi_hi = 0;
    pre_tuple_pos.ip_blkid.bi_lo = 0;
    pre_tuple_pos.ip_posid = 0;

    Assert(IS_PGXC_COORDINATOR);

    Assert(g_instance.attr.attr_sql.max_resource_package);

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    /* special case: we cann't get the number of the datanode from t_thrd.pgxc_cxt.shmemNumDataNodes. */
    if (dn_num == 0) {
        return;
    }

    /* alloc cluster_state on g_instance.instance_context. */
    MemoryContext old_context;

    LWLockAcquire(ClusterRPLock, LW_EXCLUSIVE);

    if (g_instance.wlm_cxt->cluster_state != NULL) {
        pfree_ext(g_instance.wlm_cxt->cluster_state);
    }

    g_instance.wlm_cxt->dnnum_in_cluster_state = dn_num;

    if (NULL == g_instance.wlm_cxt->cluster_state) {
        old_context = MemoryContextSwitchTo(g_instance.wlm_cxt->workload_manager_mcxt);

        g_instance.wlm_cxt->cluster_state =
            (DNState*)palloc0(sizeof(DNState) * g_instance.wlm_cxt->dnnum_in_cluster_state);

        MemoryContextSwitchTo(old_context);
    }

    for (int i = 0; i < g_instance.wlm_cxt->dnnum_in_cluster_state; i++) {
        g_instance.wlm_cxt->cluster_state[i].is_normal = false;

        g_instance.wlm_cxt->cluster_state[i].num_rp = g_instance.attr.attr_sql.max_resource_package;
    }

    LWLockRelease(ClusterRPLock);

    get_node_info(g_instance.attr.attr_common.PGXCNodeName, &pre_node_is_ccn, &pre_tuple_pos);

    /* collect the number of resource package in use from DN. */
    while (PostmasterIsAlive()) {
        CHECK_FOR_INTERRUPTS();

        if (u_sess->sig_cxt.cp_PoolReload) {
            pfree_ext(g_instance.wlm_cxt->cluster_state);

            ereport(ERROR,
                (errmodule(MOD_WLM_CP),
                    errcode(ERRCODE_INVALID_OPERATION),
                    errmsg("get reload signal in %s", __FUNCTION__)));
        }

        if (t_thrd.wlm_cxt.wlm_got_sigterm) {
            t_thrd.wlm_cxt.wlm_got_sigterm = (int)false;

            pfree_ext(g_instance.wlm_cxt->cluster_state);

            break;
        }

        ResetLatch(&t_thrd.wlm_cxt.wlm_mainloop_latch);

        get_node_info(g_instance.attr.attr_common.PGXCNodeName, &node_is_ccn, &tuple_pos);

        if (pre_node_is_ccn != node_is_ccn ||
            (pre_node_is_ccn == node_is_ccn && false == ItemPointerEquals(&tuple_pos, &pre_tuple_pos))) {
            ereport(DEBUG1,
                (errmodule(MOD_WLM_CP),
                    errmsg("the config of node is changed, details: pre_node_is_ccn: %d, node_is_ccn: %d, "
                           "pre_tuple_pos: (%d, %d), tuple_pos: (%d, %d)",
                        (int)pre_node_is_ccn,
                        (int)node_is_ccn,
                        (int)pre_tuple_pos.ip_blkid.bi_lo,
                        (int)pre_tuple_pos.ip_posid,
                        (int)tuple_pos.ip_blkid.bi_lo,
                        (int)tuple_pos.ip_posid)));

            pfree_ext(g_instance.wlm_cxt->cluster_state);

            ereport(ERROR,
                (errmodule(MOD_WLM_CP), errcode(ERRCODE_CONFIG_FILE_ERROR), errmsg("the config of node is changed.")));
        }

        ereport(DEBUG1,
            (errmodule(MOD_WLM_CP),
                errmsg("pre_node_is_ccn: %d, node_is_ccn: %d, "
                       "pre_tuple_pos: (%d, %d), tuple_pos: (%d, %d)",
                    (int)pre_node_is_ccn,
                    (int)node_is_ccn,
                    (int)pre_tuple_pos.ip_blkid.bi_lo,
                    (int)pre_tuple_pos.ip_posid,
                    (int)tuple_pos.ip_blkid.bi_lo,
                    (int)tuple_pos.ip_posid)));

        if (node_is_ccn) {
            g_instance.wlm_cxt->is_ccn = true;
            g_instance.wlm_cxt->ccn_idx = PgxcGetCentralNodeIndex();

            ereport(DEBUG1, (errmodule(MOD_WLM_CP), errmsg("%s is a CCN.", g_instance.attr.attr_common.PGXCNodeName)));

            collect_rpnumber_from_DNs();
        } else {
            g_instance.wlm_cxt->is_ccn = false;
            g_instance.wlm_cxt->ccn_idx = PgxcGetCentralNodeIndex();

            ereport(
                DEBUG1, (errmodule(MOD_WLM_CP), errmsg("%s is NOT a CCN.", g_instance.attr.attr_common.PGXCNodeName)));
        }

        pg_usleep(10 * USECS_PER_SEC);
    }
}

/*
 * @Description: increase rp number in DN of the compute pool.
 * @IN : void
 * @RETURN: void
 * @See also:
 */
void increase_rp_number()
{
    if (IS_PGXC_COORDINATOR) {
        return;
    }

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    LWLockAcquire(RPNumberLock, LW_EXCLUSIVE);

    g_instance.wlm_cxt->rp_number_in_dn++;

    LWLockRelease(RPNumberLock);

    u_sess->wlm_cxt->cp_task_running = true;
}

/*
 * @Description: decrease rp number in DN whatever commit or abort.
 * @IN : void
 * @RETURN: void
 * @See also:
 */
void decrease_rp_number()
{
    if (IS_PGXC_COORDINATOR || false == u_sess->wlm_cxt->cp_task_running) {
        return;
    }

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));
    LWLockAcquire(RPNumberLock, LW_EXCLUSIVE);
    g_instance.wlm_cxt->rp_number_in_dn--;

    LWLockRelease(RPNumberLock);
    u_sess->wlm_cxt->cp_task_running = false;
}

/*****************************************************************************
 *
 * CPmonitorMain is called in sub thread.
 *
 * MainStarterThreadFunc -> SubPostmasterMain -> *CPmonitorMain* -> *CPmonitor_MainLoop*
 *
 *****************************************************************************/
/*
 * Description: Receive SIGTERM and time to die.
 *
 * Parameters:
 *  @in SIGNAL_ARGS: the args of signal.
 * Returns: void
 */
static void CPmonitorSigTermHandler(SIGNAL_ARGS)
{
    Assert(IS_PGXC_COORDINATOR);

    int save_errno = errno;

    t_thrd.wlm_cxt.wlm_got_sigterm = (int)true;

    SetLatch(&t_thrd.wlm_cxt.wlm_mainloop_latch);

    errno = save_errno;
}

/*
 * @Description: some necessary steps.
 * @IN :void
 * @RETURN: void
 * @See also:
 */
static void NormalBackendInit()
{
    Assert(IS_PGXC_COORDINATOR);

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    /* we are a postmaster subprocess now */
    IsUnderPostmaster = true;

    /* reset t_thrd.proc_cxt.MyProcPid */
    t_thrd.proc_cxt.MyProcPid = gs_thread_self();

    /* record Start Time for logging */
    t_thrd.proc_cxt.MyStartTime = time(NULL);

    t_thrd.proc_cxt.MyProgName = "CPmonitor";

    if (u_sess->proc_cxt.MyProcPort->remote_host) {
        pfree(u_sess->proc_cxt.MyProcPort->remote_host);
    }

    u_sess->proc_cxt.MyProcPort->remote_host = pstrdup("localhost");

    /* Identify myself via ps */
    init_ps_display("compute pool monitor process", "", "", "");

    /* release all connections on exit. */
    if (IS_PGXC_COORDINATOR && IsPostmasterEnvironment) {
        /*
         * If we exit, first try and clean connections and send to
         * pooler thread does NOT exist any more, PoolerLock of LWlock is used instead.
         *
         * PoolManagerDisconnect() which is called by PGXCNodeCleanAndRelease()
         * is the last call to pooler in the openGauss thread, and PoolerLock is
         * used in PoolManagerDisconnect(), but it is called after ProcKill()
         * when openGauss thread exits.
         * ProcKill() releases any of its held LW locks. So Assert(!(proc == NULL ...))
         * will fail in LWLockAcquire() which is called by PoolManagerDisconnect().
         *
         * All exit functions in "on_shmem_exit_list" will be called before those functions
         * in "on_proc_exit_list", so move PGXCNodeCleanAndRelease() to "on_shmem_exit_list"
         * and registers it after ProcKill(), and PGXCNodeCleanAndRelease() will
         * be called before ProcKill().
         */
        on_shmem_exit(PGXCNodeCleanAndRelease, 0);
    }

    u_sess->attr.attr_common.application_name = pstrdup("ComputePoolMonitor");

    t_thrd.wlm_cxt.collect_info->sdetail.statement = "CP monitor fetch IO collect info from data nodes";
}

/*
 * @Description: main entry of the compute pool(CP) monitor thread.
 * @IN :void
 * @RETURN: void
 * @See also:
 */
NON_EXEC_STATIC void CPmonitorMain(void)
{
    Assert(IS_PGXC_COORDINATOR);

    ereport(DEBUG5, (errmodule(MOD_WLM_CP), errmsg("in %s", __FUNCTION__)));

    sigjmp_buf local_sigjmp_buf;

    t_thrd.bootstrap_cxt.MyAuxProcType = CPMonitorProcess;

    NormalBackendInit();

    /* must be after NormalBackendInit()!!! */
    t_thrd.wlm_cxt.wlm_init_done = false;

    SetProcessingMode(InitProcessing);

    /* set handler function for signal. */
    (void)gspqsignal(SIGINT, SIG_IGN);
    (void)gspqsignal(SIGTERM, CPmonitorSigTermHandler);
    (void)gspqsignal(SIGQUIT, quickdie);
    (void)gspqsignal(SIGPIPE, SIG_IGN);
    (void)gspqsignal(SIGUSR1, procsignal_sigusr1_handler);
    (void)gspqsignal(SIGUSR2, SIG_IGN);
    (void)gspqsignal(SIGFPE, FloatExceptionHandler);
    (void)gspqsignal(SIGCHLD, SIG_DFL);
    (void)gspqsignal(SIGHUP, SIG_IGN);
    (void)gspqsignal(SIGURG, print_stack);
    (void)gs_signal_unblock_sigusr2();

    if (IsUnderPostmaster) {
        /* We allow SIGQUIT (quickdie) at all times */
        (void)sigdelset(&t_thrd.libpq_cxt.BlockSig, SIGQUIT);
    }

    gs_signal_setmask(&t_thrd.libpq_cxt.UnBlockSig, NULL);

    /* Early initialization */
    BaseInit();

    WLMInitPostgres();

    SetProcessingMode(NormalProcessing);

    /* initialize latch used in main loop */
    InitLatch(&t_thrd.wlm_cxt.wlm_mainloop_latch);

    if (t_thrd.wlm_cxt.wlm_got_sigterm) {
        t_thrd.wlm_cxt.wlm_got_sigterm = (int)false;

        proc_exit(0);
    }

    /* set long jump point on error. */
    int curTryCounter;
    int *oldTryCounter = NULL;
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        gstrace_tryblock_exit(true, oldTryCounter);

        /* Since not using PG_TRY, we must reset error stack by hand */
        t_thrd.log_cxt.error_context_stack = NULL;

        t_thrd.log_cxt.call_stack = NULL;

        /* Prevents interrupts while cleaning up */
        HOLD_INTERRUPTS();

        /*
         * Forget any pending QueryCancel request, since we're returning to
         * the idle loop anyway, and cancel the statement timer if running.
         */
        t_thrd.int_cxt.QueryCancelPending = false;
        disable_sig_alarm(true);
        t_thrd.int_cxt.QueryCancelPending = false; /* again in case timeout occurred */

        if (IS_PGXC_COORDINATOR && hash_get_seq_num() > 0) {
            release_all_seq_scan();
        }

        ereport(DEBUG1, (errmodule(MOD_WLM_CP), errmsg("CPmonitor thread exit because of some error.")));

        /* Report the error to the client and/or server log */
        EmitErrorReport();

        if (t_thrd.wlm_cxt.wlm_init_done) {
            t_thrd.wlm_cxt.wlm_init_done = false;
            AbortCurrentTransaction();
        }
        /* release resource held by lsc */
        AtEOXact_SysDBCache(false);

        /*
         *   Notice: at the most time it isn't necessary to call because
         *   all the LWLocks are released in AbortCurrentTransaction().
         *   but in some rare exception not in one transaction (for
         *   example the following InitMultinodeExecutor() calling )
         *   maybe hold LWLocks unused.
         */
        LWLockReleaseAll();

        /*
         * Now return to normal top-level context and clear ErrorContext for
         * next time.
         */
        MemoryContextSwitchTo(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_DEFAULT));
        FlushErrorState();

        /* Now we can allow interrupts again */
        RESUME_INTERRUPTS();

        return;
    } else {
        oldTryCounter = gstrace_tryblock_entry(&curTryCounter);

        t_thrd.log_cxt.PG_exception_stack = &local_sigjmp_buf;
    }

    int dn_num = 0;

    if (IS_PGXC_COORDINATOR) {
        /* init transaction to execute query */
        WLMInitTransaction(&t_thrd.wlm_cxt.wlm_init_done);

        pg_usleep(10 * USECS_PER_SEC);

        /* initialize current pool handles, it's also only once */
        exec_init_poolhandles();

        /*
         * If the PGXC_NODE system table is not prepared, the number of CN / DN
         * can not be obtained, if we can not get the number of DN or CN, that
         * will make the collection module can not complete the task, so the
         * thread need restart
         */
        if (0 == u_sess->pgxc_cxt.NumDataNodes || 0 == u_sess->pgxc_cxt.NumCoords) {
            ereport(ERROR,
                (errmodule(MOD_WLM_CP),
                    errcode(ERRCODE_INITIALIZE_FAILED),
                    errmsg("init transaction error, data nodes or coordinators num init failed")));
        }

        if (t_thrd.pgxc_cxt.shmemNumDataNodes == NULL) {
            ereport(ERROR,
                (errmodule(MOD_WLM_CP),
                    errcode(ERRCODE_INITIALIZE_FAILED),
                    errmsg("init transaction error, data nodes or coordinators num init failed")));
        }

        if (0 == *t_thrd.pgxc_cxt.shmemNumDataNodes) {
            ereport(ERROR,
                (errmodule(MOD_WLM_CP),
                    errcode(ERRCODE_INITIALIZE_FAILED),
                    errmsg("init transaction error, data nodes or coordinators num init failed")));
        }

        dn_num = *t_thrd.pgxc_cxt.shmemNumDataNodes;
    }

    /*
     * Identify myself via ps
     */
    ereport(LOG, (errmodule(MOD_WLM_CP), errmsg("CPmonitor thread is starting up.")));

    /* do real job */
    CPmonitor_MainLoop(dn_num);

    /* If transaction has started, we must commit it here. */
    if (t_thrd.wlm_cxt.wlm_init_done) {
        CommitTransactionCommand();
        t_thrd.wlm_cxt.wlm_init_done = false;
    }

    proc_exit(0);
}

/*****************************************************************************
 *
 * StartCPmonitor() and CPmonitorLauncher() ard called in parent thread.
 *
 * ServerLoop -> *StartCPmonitor* -> *CPmonitorLauncher* -> postmaster_forkexec ->
 * internal_forkexec -> gs_thread_create
 *
 *****************************************************************************/
bool check_version_compatibility(const char* remote_version)
{
    char* tail = NULL;

    if (NULL == remote_version) {
        ereport(ERROR,
            (errmodule(MOD_ACCELERATE),
                errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("no the version of the compute pool is provided.")));
    }

    char* localver = get_version();
    // Gauss200 -> GaussDB Kernel, so here to avoid to modify the comparation algorigthm,
    // modify the version string to keep compatibility.
    localver = localver + strlen("GaussDB ");
    remote_version = remote_version + strlen("GaussDB ");
    if (!pg_strcasecmp(localver, remote_version)) {
        /* main version and minor version are both same. */
        return true;
    }

    char* rver = (char*)palloc0(strlen(remote_version) + 1);
    errno_t rs = memcpy_s(rver, strlen(remote_version), remote_version, strlen(remote_version));
    securec_check_c(rs, "\0", "\0");

    char* rminorver = strrchr(rver, ' ');
    char* rmainver = strchr(rver, ' ');
    if (NULL == rminorver || NULL == rmainver) {
        return false;
    }

    rminorver++;
    rmainver++;

    tail = strchr(rmainver + 1, ' ');
    if (NULL == tail) {
        return false;
    }
    *tail = '\0';

    ereport(DEBUG1,
        (errmodule(MOD_ACCELERATE), errmsg("remote main version: %s, remote minor version: %s", rmainver, rminorver)));

    /* get main and minor version from local version string. */
    char* lminorver = strrchr(localver, ' ');
    char* lmainver = strchr(localver, ' ');
    if (NULL == lminorver || NULL == lmainver) {
        return false;
    }

    lminorver++;
    lmainver++;

    tail = strchr(lmainver + 1, ' ');
    if (NULL == tail) {
        return false;
    }
    *tail = '\0';

    ereport(DEBUG1,
        (errmodule(MOD_ACCELERATE), errmsg("local main version: %s, local minor version: %s", lmainver, lminorver)));

    /* compare main version and minor version */
    if (pg_strcasecmp(rmainver, lmainver)) {
        /* main version is different. */
        return false;
    }

    /* here, main version is same. */
    if (!pg_strcasecmp(rminorver, lminorver)) {
        /* minor version is same. */
        return true;
    }

    /* here, main version is same, minor version is different. get compatible minor versions from cp_client.conf */
    int cnum = 0;
    ComputePoolConfig** confs = get_cp_conninfo(&cnum);
    char* versions = confs[0]->version;

    if (strcasestr(versions, rminorver)) {
        /* remote minor version is included in versions from cp_client.conf */
        return true;
    } else {
        return false;
    }
}
