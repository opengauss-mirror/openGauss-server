/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2010-2012, PostgreSQL Global Development Group
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
 * cluster_upgrade.cpp
 *
 *
 *
 *
 * IDENTIFICATION
 *        contrib/pg_upgrade/cluster_upgrade.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "pg_upgrade.h"
#include "cm/cm_c.h"
#include "common/config/cm_config.h"

extern char* dump_option;

#define GTM_CONTROL_FILE "gtm.control"
#define BUILD_RETRY 30
#define CM_NODE_NAME_LEN 64
/* BEGIN: PDK support for node type */
typedef enum {
    INSTANCE_ANY,
    INSTANCE_DATANODE,    /* postgresql.conf */
    INSTANCE_COORDINATOR, /* postgresql.conf */
    INSTANCE_GTM,         /* gtm.conf        */
    INSTANCE_GTM_PROXY,   /* gtm_proxy.conf  */
} NodeType;
/* END: PDK support for node type */

#define CLUSTER_CONFIG_SUCCESS (0)
#define CLUSTER_CONFIG_ERROR (1)

#define STATIC_CONFIG_FILE "cluster_static_config"

staticConfigHeader g_nodeOldHeader;
staticNodeConfig* g_oldnode = NULL;
uint32 g_oldnode_num = 0;
uint32 g_oldnode_id;
staticNodeConfig* g_currentoldNode;
char* NodeName = NULL;
char* cluster_map_file = NULL;

char* form_local_instance_command(char* olddatadir, char* newdatadir, char* HA_name, char* HA_path);
int upgrade_local_instance(void);
int execute_command_in_remote_node(char* nodename, char* command, bool bparallel);

void dump_new_cluster_node_information(char* olddatapath);
void execute_popen_commands_parallel(char* cmd, char* nodename, char* instance_name);
void read_popen_output_parallel();
int execute_popen_command(char* cmd, char* nodename, char* instance_name);
static void check_input_env_value(char* input_env_value);

extern "C" {
extern uint32 g_nodeId;

extern int init_gauss_cluster_config(char* gaussbinpath);
extern int get_local_instancename_by_dbpath(char* dbpath, char* instancename);
extern int get_local_dbpath_by_instancename(char* instancename, int* type, char* dbpath, uint32* pinst_slot_id);
extern char* getnodename(uint32 nodeidx);
extern uint32 get_num_nodes();
extern bool is_local_node(char* nodename);
extern bool is_local_nodeid(uint32 nodeid);
}

void listen_ip_merge(uint32 ip_count, char ip_listen[][CM_IP_LENGTH], char* ret_ip_merge)
{
    int nRet = 0;
    if (1 == ip_count) {
        nRet = snprintf_s(ret_ip_merge, CM_IP_ALL_NUM_LENGTH, CM_IP_ALL_NUM_LENGTH - 1, "%s", ip_listen[0]);
        securec_check_ss_c(nRet, "\0", "\0");
    } else if (2 == ip_count) {
        nRet = snprintf_s(
            ret_ip_merge, CM_IP_ALL_NUM_LENGTH, CM_IP_ALL_NUM_LENGTH - 1, "%s,%s", ip_listen[0], ip_listen[1]);
        securec_check_ss_c(nRet, "\0", "\0");
    } else if (3 == ip_count) {
        nRet = snprintf_s(ret_ip_merge,
            CM_IP_ALL_NUM_LENGTH,
            CM_IP_ALL_NUM_LENGTH - 1,
            "%s,%s,%s",
            ip_listen[0],
            ip_listen[1],
            ip_listen[2]);
        securec_check_ss_c(nRet, "\0", "\0");
    }

    return;
}

int search_PN_node(uint32 Peer_port, uint32 PeerHAListenCount, char PeerHAIP[][CM_IP_LENGTH], uint32 loal_port,
    uint32 LocalHAListenCount, char LocalHAIP[][CM_IP_LENGTH], int* node_index, int* instance_index)
{
    uint32 i;
    uint32 j;
    char input_local_listen_ip[CM_IP_ALL_NUM_LENGTH];
    char input_peer_listen_ip[CM_IP_ALL_NUM_LENGTH];
    char local_listen_ip[CM_IP_ALL_NUM_LENGTH];
    char peer_listen_ip[CM_IP_ALL_NUM_LENGTH];
    errno_t rc = 0;

    *node_index = 0;
    *instance_index = 0;

    rc = memset_s(input_local_listen_ip, CM_IP_ALL_NUM_LENGTH, 0, CM_IP_ALL_NUM_LENGTH);
    securec_check_c(rc, "\0", "\0");
    rc = memset_s(input_peer_listen_ip, CM_IP_ALL_NUM_LENGTH, 0, CM_IP_ALL_NUM_LENGTH);
    securec_check_c(rc, "\0", "\0")

        listen_ip_merge(LocalHAListenCount, LocalHAIP, input_local_listen_ip);
    listen_ip_merge(PeerHAListenCount, PeerHAIP, input_peer_listen_ip);

    for (i = 0; i < g_node_num; i++) {
        for (j = 0; j < g_node[i].datanodeCount; j++) {
            if (PRIMARY_DN != g_node[i].datanode[j].datanodeRole) {
                continue;
            }

            uint32 standby_dn_idx = 0;
            if (g_multi_az_cluster) {
                bool be_continue = true;
                for (uint32 dnId = 0; dnId < g_dn_replication_num - 1; ++dnId) {
                    be_continue = true;
                    if (STANDBY_DN == g_node[i].datanode[j].peerDatanodes[dnId].datanodePeerRole) {
                        be_continue = false;
                        standby_dn_idx = dnId;
                        if ((g_node[i].datanode[j].datanodeLocalHAPort != Peer_port) ||
                            (g_node[i].datanode[j].peerDatanodes[dnId].datanodePeerHAPort != loal_port)) {
                            be_continue = true;
                        }
                        break;
                    }
                }
                if (be_continue) {
                    continue;
                }
            } else {
                if (STANDBY_DN == g_node[i].datanode[j].datanodePeerRole) {
                    if ((g_node[i].datanode[j].datanodeLocalHAPort != Peer_port) ||
                        (g_node[i].datanode[j].datanodePeerHAPort != loal_port)) {
                        continue;
                    }
                } else if (STANDBY_DN == g_node[i].datanode[j].datanodePeer2Role) {
                    if ((g_node[i].datanode[j].datanodeLocalHAPort != Peer_port) ||
                        (g_node[i].datanode[j].datanodePeer2HAPort != loal_port)) {
                        continue;
                    }
                } else {
                    continue;
                }
            }

            rc = memset_s(local_listen_ip, CM_IP_ALL_NUM_LENGTH, 0, CM_IP_ALL_NUM_LENGTH);
            securec_check_c(rc, "\0", "\0");
            rc = memset_s(peer_listen_ip, CM_IP_ALL_NUM_LENGTH, 0, CM_IP_ALL_NUM_LENGTH);
            securec_check_c(rc, "\0", "\0");

            listen_ip_merge(g_node[i].datanode[j].datanodeLocalHAListenCount,
                g_node[i].datanode[j].datanodeLocalHAIP,
                local_listen_ip);
            if (g_multi_az_cluster) {
                if (STANDBY_DN == g_node[i].datanode[j].peerDatanodes[standby_dn_idx].datanodePeerRole) {
                    listen_ip_merge(g_node[i].datanode[j].peerDatanodes[standby_dn_idx].datanodePeerHAListenCount,
                        g_node[i].datanode[j].peerDatanodes[standby_dn_idx].datanodePeerHAIP,
                        peer_listen_ip);
                }
            } else {
                if (STANDBY_DN == g_node[i].datanode[j].datanodePeerRole) {
                    listen_ip_merge(g_node[i].datanode[j].datanodePeerHAListenCount,
                        g_node[i].datanode[j].datanodePeerHAIP,
                        peer_listen_ip);
                } else if (STANDBY_DN == g_node[i].datanode[j].datanodePeer2Role) {
                    listen_ip_merge(g_node[i].datanode[j].datanodePeer2HAListenCount,
                        g_node[i].datanode[j].datanodePeer2HAIP,
                        peer_listen_ip);
                }
            }

            if ((strncmp(local_listen_ip, input_peer_listen_ip, CM_IP_ALL_NUM_LENGTH) == 0) &&
                (strncmp(peer_listen_ip, input_local_listen_ip, CM_IP_ALL_NUM_LENGTH) == 0)) {
                *node_index = i;
                *instance_index = j;
                return 0;
            }
        }
    }

    return -1;
}

int search_HA_node(uint32 loal_port, uint32 LocalHAListenCount, char LocalHAIP[][CM_IP_LENGTH], uint32 Peer_port,
    uint32 PeerHAListenCount, char PeerHAIP[][CM_IP_LENGTH], int* node_index, int* instance_index)
{
    uint32 i;
    uint32 max_node_count;
    char input_local_listen_ip[CM_IP_ALL_NUM_LENGTH];
    char input_peer_listen_ip[CM_IP_ALL_NUM_LENGTH];
    char local_listen_ip[CM_IP_ALL_NUM_LENGTH];
    char peer_listen_ip[CM_IP_ALL_NUM_LENGTH];
    uint32 j;
    errno_t rc = 0;

    *node_index = 0;
    *instance_index = 0;

    max_node_count = g_node_num;
    rc = memset_s(input_local_listen_ip, CM_IP_ALL_NUM_LENGTH, 0, CM_IP_ALL_NUM_LENGTH);
    securec_check_c(rc, "\0", "\0");
    rc = memset_s(input_peer_listen_ip, CM_IP_ALL_NUM_LENGTH, 0, CM_IP_ALL_NUM_LENGTH);
    securec_check_c(rc, "\0", "\0");

    listen_ip_merge(LocalHAListenCount, LocalHAIP, input_local_listen_ip);
    listen_ip_merge(PeerHAListenCount, PeerHAIP, input_peer_listen_ip);

    if (!g_multi_az_cluster) {
        for (i = 0; i < max_node_count; i++) {
            for (j = 0; j < g_node[i].datanodeCount; j++) {
                if (PRIMARY_DN == g_node[i].datanode[j].datanodePeerRole) {
                    if ((g_node[i].datanode[j].datanodeLocalHAPort != Peer_port) ||
                        (g_node[i].datanode[j].datanodePeerHAPort != loal_port)) {
                        continue;
                    }
                } else if (PRIMARY_DN == g_node[i].datanode[j].datanodePeer2Role) {
                    if ((g_node[i].datanode[j].datanodeLocalHAPort != Peer_port) ||
                        (g_node[i].datanode[j].datanodePeer2HAPort != loal_port)) {
                        continue;
                    }
                } else {
                    continue;
                }

                rc = memset_s(local_listen_ip, CM_IP_ALL_NUM_LENGTH, 0, CM_IP_ALL_NUM_LENGTH);
                securec_check_c(rc, "\0", "\0");
                rc = memset_s(peer_listen_ip, CM_IP_ALL_NUM_LENGTH, 0, CM_IP_ALL_NUM_LENGTH);
                securec_check_c(rc, "\0", "\0");
                listen_ip_merge(g_node[i].datanode[j].datanodeLocalHAListenCount,
                    g_node[i].datanode[j].datanodeLocalHAIP,
                    local_listen_ip);
                if (PRIMARY_DN == g_node[i].datanode[j].datanodePeerRole) {
                    listen_ip_merge(g_node[i].datanode[j].datanodePeerHAListenCount,
                        g_node[i].datanode[j].datanodePeerHAIP,
                        peer_listen_ip);
                } else if (PRIMARY_DN == g_node[i].datanode[j].datanodePeer2Role) {
                    listen_ip_merge(g_node[i].datanode[j].datanodePeer2HAListenCount,
                        g_node[i].datanode[j].datanodePeer2HAIP,
                        peer_listen_ip);
                }

                if ((strncmp(local_listen_ip, input_peer_listen_ip, CM_IP_ALL_NUM_LENGTH) == 0) &&
                    (strncmp(peer_listen_ip, input_local_listen_ip, CM_IP_ALL_NUM_LENGTH) == 0)) {
                    *node_index = i;
                    *instance_index = j;
                    return 0;
                }
            }
        }
    }

    return -1;
}

void upgrade_gtm(void)
{
    uint32 nodeidx;
    char system_cmd[(CM_PATH_LENGTH * 2) + (CM_NODE_NAME_LEN * 4)] = {0};
    int32 ret;
    char* mpprvFile = NULL;
    int nRet = 0;

    mpprvFile = getenv("MPPDB_ENV_SEPARATE_PATH");

    for (nodeidx = 0; nodeidx < g_node_num; nodeidx++) {
        if (g_node[nodeidx].gtm) {
            nRet = snprintf_s(system_cmd,
                ((CM_PATH_LENGTH * 2) + (CM_NODE_NAME_LEN * 4)),
                sizeof(system_cmd) - 1,
                "scp %s:%s/" GTM_CONTROL_FILE " %s:%s/" GTM_CONTROL_FILE " 2>&1 >/dev/null",
                g_node[nodeidx].nodeName,
                g_oldnode[nodeidx].gtmLocalDataPath,
                g_node[nodeidx].nodeName,
                g_node[nodeidx].gtmLocalDataPath);
            securec_check_ss_c(nRet, "\0", "\0");
            ret = system(system_cmd);
            if (ret != 0) {
                pg_log(PG_FATAL, "ERROR: failed to upgrade the GTM.\n");
            }

            if (mpprvFile == NULL) {
                nRet = snprintf_s(system_cmd,
                    ((CM_PATH_LENGTH * 2) + (CM_NODE_NAME_LEN * 4)),
                    sizeof(system_cmd) - 1,
                    "ssh %s \"su - %s -c 'gtm_ctl -o -r -l %s/gtmstartuplog -D %s -Z gtm start'\"",
                    g_node[nodeidx].nodeName,
                    new_cluster.user,
                    g_log_path,
                    g_node[nodeidx].gtmLocalDataPath);
            } else {
                check_input_env_value(mpprvFile);
                nRet = snprintf_s(system_cmd,
                    ((CM_PATH_LENGTH * 2) + (CM_NODE_NAME_LEN * 4)),
                    sizeof(system_cmd) - 1,
                    "ssh %s \"su - %s -c 'source %s;gtm_ctl -o -r -l %s/gtmstartuplog -D %s -Z gtm start'\"",
                    g_node[nodeidx].nodeName,
                    new_cluster.user,
                    mpprvFile,
                    g_log_path,
                    g_node[nodeidx].gtmLocalDataPath);
            }
            securec_check_ss_c(nRet, "\0", "\0");
            ret = system(system_cmd);
            if (ret != 0) {
                pg_log(PG_FATAL, "ERROR: failed to start the GTM.\n");
            }
            break;
        }
    }
}

void stop_new_gtm(void)
{
    uint32 nodeidx;
    char system_cmd[(CM_PATH_LENGTH * 3)] = {0};
    int32 ret;
    char* mpprvFile = NULL;
    int nRet = 0;

    mpprvFile = getenv("MPPDB_ENV_SEPARATE_PATH");

    for (nodeidx = 0; nodeidx < g_node_num; nodeidx++) {
        if (g_node[nodeidx].gtm) {
            if (mpprvFile == NULL) {
                nRet = snprintf_s(system_cmd,
                    (CM_PATH_LENGTH * 3),
                    sizeof(system_cmd) - 1,
                    "ssh %s \"su - %s sh -c '%s/gtm_ctl -D %s -Z gtm stop'\"",
                    g_node[nodeidx].nodeName,
                    new_cluster.user,
                    new_cluster.bindir,
                    g_node[nodeidx].gtmLocalDataPath);
            } else {
                check_input_env_value(mpprvFile);
                nRet = snprintf_s(system_cmd,
                    (CM_PATH_LENGTH * 3),
                    sizeof(system_cmd) - 1,
                    "ssh %s \"su - %s sh -c 'source %s;%s/gtm_ctl -D %s -Z gtm stop'\"",
                    g_node[nodeidx].nodeName,
                    new_cluster.user,
                    mpprvFile,
                    new_cluster.bindir,
                    g_node[nodeidx].gtmLocalDataPath);
            }
            securec_check_ss_c(nRet, "\0", "\0");
            ret = system(system_cmd);
            if (ret != 0) {
                pg_log(PG_FATAL, "ERROR: failed to stop the GTM.\n");
            }
            break;
        }
    }
}

int read_old_cluster_config_file_version_three(char* gaussbinpath)
{
    int ret;
    errno_t rc = 0;

    /* read latest cluster configuration file */
    ret = init_gauss_cluster_config(gaussbinpath);
    if (0 != ret) {
        return ret;
    }
    /* copy old cluster configuration values */
    rc = memcpy_s(&g_nodeOldHeader, sizeof(staticConfigHeader), &g_nodeHeader, sizeof(staticConfigHeader));
    securec_check_c(rc, "\0", "\0");

    g_oldnode = g_node;
    g_node = NULL;
    g_oldnode_num = g_node_num;
    g_node_num = 0;
    g_oldnode_id = g_node[g_nodeId].node;
    g_nodeId = 0;
    g_currentoldNode = g_currentNode;
    g_currentNode = NULL;
    max_node_name_len = 0;
    max_datapath_len = 0;

    return 0;
}

#define FREAD(ptr, size, nitems, stream)                \
    do {                                                \
        if (fread(ptr, size, nitems, stream) != nitems) \
            goto read_failed;                           \
    } while (0)

int read_old_cluster_config_file_version_one(char* gaussbinpath)
{
    char cmpath[MAXPGPATH];

    FILE* fd = NULL;
    uint32 ii = 0;
    uint32 header_size = 0;
    uint32 header_aglinment_size = 0;
    long current_pos = 0;
    int nRet = 0;

    header_size = sizeof(staticConfigHeader);
    header_aglinment_size =
        (header_size / AGLINMENT_SIZE + (header_size % AGLINMENT_SIZE == 0) ? 0 : 1) * AGLINMENT_SIZE;

    nRet = snprintf_s(cmpath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", gaussbinpath, STATIC_CONFIG_FILE);
    securec_check_ss_c(nRet, "\0", "\0");

    fd = fopen(cmpath, "r");
    if (fd == NULL) {
        return -1;
    }

    // read head info
    FREAD(&g_nodeOldHeader.crc, 1, sizeof(uint32), fd);
    FREAD(&g_nodeOldHeader.len, 1, sizeof(uint32), fd);
    FREAD(&g_nodeOldHeader.version, 1, sizeof(uint32), fd);
    FREAD(&g_nodeOldHeader.time, 1, sizeof(int64), fd);
    FREAD(&g_nodeOldHeader.nodeCount, 1, sizeof(uint32), fd);
    FREAD(&g_nodeOldHeader.node, 1, sizeof(uint32), fd);

    if (fseek(fd, (off_t)(header_aglinment_size), SEEK_SET) != 0)
        goto read_failed;

    g_oldnode_num = g_nodeOldHeader.nodeCount;

    if (NULL != g_oldnode) {
        free(g_oldnode);
        g_oldnode = NULL;
    }

    if (NULL == g_oldnode) {
        g_oldnode = (staticNodeConfig*)malloc(sizeof(staticNodeConfig) * g_oldnode_num);
        if (NULL == g_oldnode) {
            fclose(fd);
            return -1;
        }
    }

    for (ii = 0; ii < g_oldnode_num; ii++) {
        int jj = 0;
        uint32 kk = 0;
        uint32 body_aglinment_size = 0;

        // read node info
        FREAD(&g_oldnode[ii].crc, 1, sizeof(uint32), fd);
        FREAD(&g_oldnode[ii].node, 1, sizeof(uint32), fd);
        FREAD(g_oldnode[ii].nodeName, 1, CM_NODE_NAME_LEN, fd);

        FREAD(&g_oldnode[ii].sshCount, 1, sizeof(uint32), fd);
        for (jj = 0; jj < CM_IP_NUM; jj++) {
            FREAD(g_oldnode[ii].sshChannel[jj], 1, CM_IP_LENGTH, fd);
        }
        FREAD(&g_oldnode[ii].backIpCount, 1, sizeof(uint32), fd);
        for (jj = 0; jj < CM_IP_NUM; jj++) {
            FREAD(g_oldnode[ii].backIps[jj], 1, CM_IP_LENGTH, fd);
        }

        // read CMServer info
        FREAD(&g_oldnode[ii].cmServerId, 1, sizeof(uint32), fd);
        FREAD(&g_oldnode[ii].cmServerMirrorId, 1, sizeof(uint32), fd);
        FREAD(g_oldnode[ii].cmDataPath, 1, CM_PATH_LENGTH, fd);
        FREAD(&g_oldnode[ii].cmServerLevel, 1, sizeof(uint32), fd);
        FREAD(g_oldnode[ii].cmServerFloatIP, 1, CM_IP_LENGTH, fd);
        FREAD(&g_oldnode[ii].cmServerListenCount, 1, sizeof(uint32), fd);
        for (jj = 0; jj < CM_IP_NUM; jj++) {
            FREAD(g_oldnode[ii].cmServer[jj], 1, CM_IP_LENGTH, fd);
        }
        FREAD(&g_oldnode[ii].port, 1, sizeof(uint32), fd);
        FREAD(&g_oldnode[ii].cmServerLocalHAListenCount, 1, sizeof(uint32), fd);
        for (jj = 0; jj < CM_IP_NUM; jj++) {
            FREAD(g_oldnode[ii].cmServerLocalHAIP[jj], 1, CM_IP_LENGTH, fd);
        }
        FREAD(&g_oldnode[ii].cmServerLocalHAPort, 1, sizeof(uint32), fd);
        FREAD(&g_oldnode[ii].cmServerRole, 1, sizeof(uint32), fd);
        FREAD(&g_oldnode[ii].cmServerPeerHAListenCount, 1, sizeof(uint32), fd);
        for (jj = 0; jj < CM_IP_NUM; jj++) {
            FREAD(g_oldnode[ii].cmServerPeerHAIP[jj], 1, CM_IP_LENGTH, fd);
        }
        FREAD(&g_oldnode[ii].cmServerPeerHAPort, 1, sizeof(uint32), fd);

        // read GTM info
        FREAD(&g_oldnode[ii].gtmAgentId, 1, sizeof(uint32), fd);
        FREAD(&g_oldnode[ii].cmAgentMirrorId, 1, sizeof(uint32), fd);
        FREAD(&g_oldnode[ii].cmAgentListenCount, 1, sizeof(uint32), fd);
        for (jj = 0; jj < CM_IP_NUM; jj++) {
            FREAD(g_oldnode[ii].cmAgentIP[jj], 1, CM_IP_LENGTH, fd);
        }
        FREAD(&g_oldnode[ii].gtmId, 1, sizeof(uint32), fd);
        FREAD(&g_oldnode[ii].gtmMirrorId, 1, sizeof(uint32), fd);
        FREAD(&g_oldnode[ii].gtm, 1, sizeof(uint32), fd);
        FREAD(g_oldnode[ii].gtmLocalDataPath, 1, CM_PATH_LENGTH, fd);

        FREAD(&g_oldnode[ii].gtmLocalListenCount, 1, sizeof(uint32), fd);
        for (jj = 0; jj < CM_IP_NUM; jj++) {
            FREAD(g_oldnode[ii].gtmLocalListenIP[jj], 1, CM_IP_LENGTH, fd);
        }
        FREAD(&g_oldnode[ii].gtmLocalport, 1, sizeof(uint32), fd);
        FREAD(&g_oldnode[ii].gtmRole, 1, sizeof(uint32), fd);
        FREAD(&g_oldnode[ii].gtmLocalHAListenCount, 1, sizeof(uint32), fd);
        for (jj = 0; jj < CM_IP_NUM; jj++) {
            FREAD(g_oldnode[ii].gtmLocalHAIP[jj], 1, CM_IP_LENGTH, fd);
        }
        FREAD(&g_oldnode[ii].gtmLocalHAPort, 1, sizeof(uint32), fd);
        FREAD(g_oldnode[ii].gtmPeerDataPath, 1, CM_PATH_LENGTH, fd);
        FREAD(&g_oldnode[ii].gtmPeerHAListenCount, 1, sizeof(uint32), fd);
        for (jj = 0; jj < CM_IP_NUM; jj++) {
            FREAD(g_oldnode[ii].gtmPeerHAIP[jj], 1, CM_IP_LENGTH, fd);
        }
        FREAD(&g_oldnode[ii].gtmPeerHAPort, 1, sizeof(uint32), fd);
        FREAD(&g_oldnode[ii].gtmProxyId, 1, sizeof(uint32), fd);
        FREAD(&g_oldnode[ii].gtmProxyMirrorId, 1, sizeof(uint32), fd);
        FREAD(&g_oldnode[ii].gtmProxy, 1, sizeof(uint32), fd);
        FREAD(&g_oldnode[ii].gtmProxyListenCount, 1, sizeof(uint32), fd);
        for (jj = 0; jj < CM_IP_NUM; jj++) {
            FREAD(g_oldnode[ii].gtmProxyListenIP[jj], 1, CM_IP_LENGTH, fd);
        }
        FREAD(&g_oldnode[ii].gtmProxyPort, 1, sizeof(uint32), fd);

        // read CN info
        FREAD(&g_oldnode[ii].coordinateId, 1, sizeof(uint32), fd);
        FREAD(&g_oldnode[ii].coordinateMirrorId, 1, sizeof(uint32), fd);
        FREAD(&g_oldnode[ii].coordinate, 1, sizeof(uint32), fd);
        FREAD(g_oldnode[ii].DataPath, 1, CM_PATH_LENGTH, fd);

        FREAD(&g_oldnode[ii].coordinateListenCount, 1, sizeof(uint32), fd);
        for (jj = 0; jj < CM_IP_NUM; jj++) {
            FREAD(g_oldnode[ii].coordinateListenIP[jj], 1, CM_IP_LENGTH, fd);
        }
        FREAD(&g_oldnode[ii].coordinatePort, 1, sizeof(uint32), fd);
        FREAD(&g_oldnode[ii].coordinateHAPort, 1, sizeof(uint32), fd);

        // read DNs info
        FREAD(&g_oldnode[ii].datanodeCount, 1, sizeof(uint32), fd);
        for (kk = 0; kk < g_oldnode[ii].datanodeCount; kk++) {
            int nn = 0;

            FREAD(&g_oldnode[ii].datanode[kk].datanodeId, 1, sizeof(uint32), fd);
            FREAD(&g_oldnode[ii].datanode[kk].datanodeMirrorId, 1, sizeof(uint32), fd);
            FREAD(g_oldnode[ii].datanode[kk].datanodeLocalDataPath, 1, CM_PATH_LENGTH, fd);

            FREAD(&g_oldnode[ii].datanode[kk].datanodeListenCount, 1, sizeof(uint32), fd);
            for (nn = 0; nn < CM_IP_NUM; nn++) {
                FREAD(g_oldnode[ii].datanode[kk].datanodeListenIP[nn], 1, CM_IP_LENGTH, fd);
            }
            FREAD(&g_oldnode[ii].datanode[kk].datanodePort, 1, sizeof(uint32), fd);
            FREAD(&g_oldnode[ii].datanode[kk].datanodeRole, 1, sizeof(uint32), fd);
            FREAD(&g_oldnode[ii].datanode[kk].datanodeLocalHAListenCount, 1, sizeof(uint32), fd);
            for (nn = 0; nn < CM_IP_NUM; nn++) {
                FREAD(g_oldnode[ii].datanode[kk].datanodeLocalHAIP[nn], 1, CM_IP_LENGTH, fd);
            }
            FREAD(&g_oldnode[ii].datanode[kk].datanodeLocalHAPort, 1, sizeof(uint32), fd);
            FREAD(g_oldnode[ii].datanode[kk].datanodePeerDataPath, 1, CM_PATH_LENGTH, fd);
            FREAD(&g_oldnode[ii].datanode[kk].datanodePeerHAListenCount, 1, sizeof(uint32), fd);
            for (nn = 0; nn < CM_IP_NUM; nn++) {
                FREAD(g_oldnode[ii].datanode[kk].datanodePeerHAIP[nn], 1, CM_IP_LENGTH, fd);
            }
            FREAD(&g_oldnode[ii].datanode[kk].datanodePeerHAPort, 1, sizeof(uint32), fd);
            FREAD(&g_oldnode[ii].datanode[kk].datanodePeerRole, 1, sizeof(uint32), fd);
            FREAD(g_oldnode[ii].datanode[kk].datanodePeer2DataPath, 1, CM_PATH_LENGTH, fd);
            FREAD(&g_oldnode[ii].datanode[kk].datanodePeer2HAListenCount, 1, sizeof(uint32), fd);
            for (nn = 0; nn < CM_IP_NUM; nn++) {
                FREAD(g_oldnode[ii].datanode[kk].datanodePeer2HAIP[nn], 1, CM_IP_LENGTH, fd);
            }
            FREAD(&g_oldnode[ii].datanode[kk].datanodePeer2HAPort, 1, sizeof(uint32), fd);
            FREAD(&g_oldnode[ii].datanode[kk].datanodePeer2Role, 1, sizeof(uint32), fd);
        }
        FREAD(&g_oldnode[ii].sctpBeginPort, 1, sizeof(uint32), fd);
        FREAD(&g_oldnode[ii].sctpEndPort, 1, sizeof(uint32), fd);

        current_pos = ftell(fd);
        body_aglinment_size =
            (current_pos / AGLINMENT_SIZE + ((current_pos % AGLINMENT_SIZE == 0) ? 0 : 1)) * AGLINMENT_SIZE;

        if (fseek(fd, (off_t)body_aglinment_size, SEEK_SET))
            goto read_failed;
    }

    g_oldnode_id = g_nodeOldHeader.node - 1;
    if (g_oldnode_id < 0) {
        fprintf(stderr, "current node self is invalid  node =%d\n", g_nodeOldHeader.node);
        return -1;
    }
    g_currentoldNode = &g_oldnode[g_oldnode_id];

    fclose(fd);
    return 0;
read_failed:
    if (NULL != g_oldnode) {
        free(g_oldnode);
        g_oldnode = NULL;
    }
    fclose(fd);
    return -1;
}

int read_old_cluster_config_file_version_zero(char* gaussbinpath)
{
    char cmpath[MAXPGPATH];
    FILE* fd = NULL;
    uint32 ii = 0;

    uint32 header_size = 0;
    uint32 header_aglinment_size = 0;
    long current_pos;
    int nRet = 0;

    nRet = snprintf_s(cmpath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", gaussbinpath, STATIC_CONFIG_FILE);
    securec_check_ss_c(nRet, "\0", "\0");

    header_size = sizeof(staticConfigHeader);
    header_aglinment_size =
        (header_size / AGLINMENT_SIZE + (header_size % AGLINMENT_SIZE == 0) ? 0 : 1) * AGLINMENT_SIZE;

    fd = fopen(cmpath, "r");
    if (fd == NULL) {
        fprintf(stderr, "STATIC_CONFIG_FILE(%s) is not found errno =%d\n", cmpath, errno);
        return -1;
    }

    // uint32 crc;
    if (fread(&g_nodeOldHeader.crc, 1, sizeof(uint32), fd) != sizeof(uint32))
        goto read_failed;
    // uint32 len;
    if (fread(&g_nodeOldHeader.len, 1, sizeof(uint32), fd) != sizeof(uint32))
        goto read_failed;
    // uint32  version;
    if (fread(&g_nodeOldHeader.version, 1, sizeof(uint32), fd) != sizeof(uint32))
        goto read_failed;
    // int64   time;
    if (fread(&g_nodeOldHeader.time, 1, sizeof(int64), fd) != sizeof(int64))
        goto read_failed;
    // uint32 nodeCount;
    if (fread(&g_nodeOldHeader.nodeCount, 1, sizeof(uint32), fd) != sizeof(uint32))
        goto read_failed;
    // uint32  node;
    if (fread(&g_nodeOldHeader.node, 1, sizeof(uint32), fd) != sizeof(uint32))
        goto read_failed;
    fseek(fd, (off_t)(header_aglinment_size), SEEK_SET);

    if (g_nodeOldHeader.nodeCount > CM_NODE_MAXNUM) {
        fprintf(stderr, "nodeCount(%d) exceed the max node num(%d)\n", g_nodeOldHeader.nodeCount, CM_NODE_MAXNUM);
        return -1;
    }

    if (NULL == g_oldnode) {
        g_oldnode_num = g_nodeOldHeader.nodeCount;
        g_oldnode = (staticNodeConfig*)malloc(sizeof(staticNodeConfig) * g_oldnode_num);
        if (NULL == g_oldnode) {
            fprintf(stderr, "malloc staticNodeConfig failed! size = %ld\n", sizeof(staticNodeConfig) * g_oldnode_num);
            return -1;
        }
    } else {
        if (g_oldnode_num != g_nodeOldHeader.nodeCount) {
            free(g_oldnode);
            g_oldnode = NULL;
            g_oldnode_num = g_nodeOldHeader.nodeCount;
            g_oldnode = (staticNodeConfig*)malloc(sizeof(staticNodeConfig) * g_oldnode_num);
            if (NULL == g_oldnode) {
                fprintf(
                    stderr, "malloc staticNodeConfig failed! size = %ld\n", sizeof(staticNodeConfig) * g_oldnode_num);
                return -1;
            }
        }
    }

    /**/
    for (ii = 0; ii < g_oldnode_num; ii++) {
        int jj = 0;
        uint32 kk = 0;
        uint32 body_aglinment_size = 0;

        // uint32 crc;
        if (fread(&g_oldnode[ii].crc, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // uint32  node;
        if (fread(&g_oldnode[ii].node, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // char   nodeName[CM_NODE_NAME_LEN];
        if (fread(&g_oldnode[ii].nodeName, 1, CM_NODE_NAME_LEN, fd) != CM_NODE_NAME_LEN)
            goto read_failed;
        // uint32 sshCount;
        if (fread(&g_oldnode[ii].sshCount, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // char    sshChannel[CM_IP_NUM][CM_IP_LENGTH];
        for (jj = 0; jj < CM_IP_NUM; jj++) {
            if (fread(&g_oldnode[ii].sshChannel[jj], 1, CM_IP_LENGTH, fd) != CM_IP_LENGTH)
                goto read_failed;
        }
        // uint32 cmServerId;
        if (fread(&g_oldnode[ii].cmServerId, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // char   cmDataPath[CM_PATH_LENGTH];
        if (fread(&g_oldnode[ii].cmDataPath, 1, CM_PATH_LENGTH, fd) != CM_PATH_LENGTH)
            goto read_failed;
        //	uint32 cmServerLevel;
        if (fread(&g_oldnode[ii].cmServerLevel, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // char cmServerFloatIP [CM_IP_LENGTH];
        if (fread(&g_oldnode[ii].cmServerFloatIP, 1, CM_IP_LENGTH, fd) != CM_IP_LENGTH)
            goto read_failed;

        // uint32 cmServerListenCount;
        if (fread(&g_oldnode[ii].cmServerListenCount, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // char cmServer[CM_IP_NUM][CM_IP_LENGTH];
        for (jj = 0; jj < CM_IP_NUM; jj++) {
            if (fread(&g_oldnode[ii].cmServer[jj], 1, CM_IP_LENGTH, fd) != CM_IP_LENGTH)
                goto read_failed;
        }
        // uint32  port ;
        if (fread(&g_oldnode[ii].port, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;

        // uint32 cmServerLocalHAListenCount;
        if (fread(&g_oldnode[ii].cmServerLocalHAListenCount, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // char cmServerLocalHAIP[CM_IP_NUM][CM_IP_LENGTH];
        for (jj = 0; jj < CM_IP_NUM; jj++) {
            if (fread(&g_oldnode[ii].cmServerLocalHAIP[jj], 1, CM_IP_LENGTH, fd) != CM_IP_LENGTH)
                goto read_failed;
        }
        // uint32  cmServerLocalHAPort;
        if (fread(&g_oldnode[ii].cmServerLocalHAPort, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // int32  cmServerRole;
        if (fread(&g_oldnode[ii].cmServerRole, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // uint32 cmServerPeerHAListenCount;
        if (fread(&g_oldnode[ii].cmServerPeerHAListenCount, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // char cmServerPeerHAIP[CM_IP_NUM][CM_IP_LENGTH];
        for (jj = 0; jj < CM_IP_NUM; jj++) {
            if (fread(&g_oldnode[ii].cmServerPeerHAIP[jj], 1, CM_IP_LENGTH, fd) != CM_IP_LENGTH)
                goto read_failed;
        }
        // uint32  cmServerPeerHAPort;
        if (fread(&g_oldnode[ii].cmServerPeerHAPort, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // uint32 gtmAgentId;
        if (fread(&g_oldnode[ii].gtmAgentId, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // uint32 cmAgentListenCount;
        if (fread(&g_oldnode[ii].cmAgentListenCount, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // char    cmAgentIP[CM_IP_NUM][CM_IP_LENGTH];
        for (jj = 0; jj < CM_IP_NUM; jj++) {
            if (fread(&g_oldnode[ii].cmAgentIP[jj], 1, CM_IP_LENGTH, fd) != CM_IP_LENGTH)
                goto read_failed;
        }
        // uint32 gtmId;
        if (fread(&g_oldnode[ii].gtmId, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // uint32  gtm;
        if (fread(&g_oldnode[ii].gtm, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // char   gtmLocalDataPath[CM_PATH_LENGTH];
        if (fread(&g_oldnode[ii].gtmLocalDataPath, 1, CM_PATH_LENGTH, fd) != CM_PATH_LENGTH)
            goto read_failed;
        // uint32 gtmLocalListenCount;
        if (fread(&g_oldnode[ii].gtmLocalListenCount, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // char  gtmLocalListenIP[CM_IP_NUM][CM_IP_LENGTH];
        for (jj = 0; jj < CM_IP_NUM; jj++) {
            if (fread(&g_oldnode[ii].gtmLocalListenIP[jj], 1, CM_IP_LENGTH, fd) != CM_IP_LENGTH)
                goto read_failed;
        }
        // uint32  gtmLocalport ;
        if (fread(&g_oldnode[ii].gtmLocalport, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // uint32  gtmRole;
        if (fread(&g_oldnode[ii].gtmRole, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // uint32 gtmLocalHAListenCount;
        if (fread(&g_oldnode[ii].gtmLocalHAListenCount, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // char gtmLocalHAIP[CM_IP_NUM][CM_IP_LENGTH];
        for (jj = 0; jj < CM_IP_NUM; jj++) {
            if (fread(&g_oldnode[ii].gtmLocalHAIP[jj], 1, CM_IP_LENGTH, fd) != CM_IP_LENGTH)
                goto read_failed;
        }
        // uint32  gtmLocalHAPort;
        if (fread(&g_oldnode[ii].gtmLocalHAPort, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // char   gtmPeerDataPath[CM_PATH_LENGTH];
        if (fread(&g_oldnode[ii].gtmPeerDataPath, 1, CM_PATH_LENGTH, fd) != CM_PATH_LENGTH)
            goto read_failed;
        // uint32 gtmPeerHAListenCount;
        if (fread(&g_oldnode[ii].gtmPeerHAListenCount, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // char gtmPeerHAIP[CM_IP_NUM] [CM_IP_LENGTH];
        for (jj = 0; jj < CM_IP_NUM; jj++) {
            if (fread(&g_oldnode[ii].gtmPeerHAIP[jj], 1, CM_IP_LENGTH, fd) != CM_IP_LENGTH)
                goto read_failed;
        }
        // uint32  gtmPeerHAPort ;
        if (fread(&g_oldnode[ii].gtmPeerHAPort, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // uint32 gtmProxyId;
        if (fread(&g_oldnode[ii].gtmProxyId, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // uint32  gtmProxy;
        if (fread(&g_oldnode[ii].gtmProxy, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // uint32 gtmProxyListenCount;
        if (fread(&g_oldnode[ii].gtmProxyListenCount, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // char  gtmProxyListenIP[CM_IP_NUM][CM_IP_LENGTH];
        for (jj = 0; jj < CM_IP_NUM; jj++) {
            if (fread(&g_oldnode[ii].gtmProxyListenIP[jj], 1, CM_IP_LENGTH, fd) != CM_IP_LENGTH)
                goto read_failed;
        }
        // uint32  gtmProxyPort;
        if (fread(&g_oldnode[ii].gtmProxyPort, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // uint32 coordinateId;
        if (fread(&g_oldnode[ii].coordinateId, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // uint32  coordinate;
        if (fread(&g_oldnode[ii].coordinate, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // char    DataPath[CM_PATH_LENGTH];
        if (fread(&g_oldnode[ii].DataPath, 1, CM_PATH_LENGTH, fd) != CM_PATH_LENGTH)
            goto read_failed;
        // uint32 coordinateListenCount;
        if (fread(&g_oldnode[ii].coordinateListenCount, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        // char  coordinateListenIP[CM_IP_NUM][CM_IP_LENGTH];
        for (jj = 0; jj < CM_IP_NUM; jj++) {
            if (fread(&g_oldnode[ii].coordinateListenIP[jj], 1, CM_IP_LENGTH, fd) != CM_IP_LENGTH)
                goto read_failed;
        }
        // uint32  coordinatePort;
        if (fread(&g_oldnode[ii].coordinatePort, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        //	uint32  datanodeCount;
        if (fread(&g_oldnode[ii].datanodeCount, 1, sizeof(uint32), fd) != sizeof(uint32))
            goto read_failed;
        for (kk = 0; kk < g_oldnode[ii].datanodeCount; kk++) {
            int nn = 0;
            // uint32 datanodeId;
            if (fread(&g_oldnode[ii].datanode[kk].datanodeId, 1, sizeof(uint32), fd) != sizeof(uint32))
                goto read_failed;
            // char   datanodeLocalDataPath[CM_PATH_LENGTH];
            if (fread(&g_oldnode[ii].datanode[kk].datanodeLocalDataPath, 1, CM_PATH_LENGTH, fd) != CM_PATH_LENGTH)
                goto read_failed;
            // uint32 datanodeListenCount;
            if (fread(&g_oldnode[ii].datanode[kk].datanodeListenCount, 1, sizeof(uint32), fd) != sizeof(uint32))
                goto read_failed;
            // char  datanodeListenIP[CM_IP_NUM][CM_IP_LENGTH];
            for (nn = 0; nn < CM_IP_NUM; nn++) {
                if (fread(&g_oldnode[ii].datanode[kk].datanodeListenIP[nn], 1, CM_IP_LENGTH, fd) != CM_IP_LENGTH)
                    goto read_failed;
            }
            // uint32  datanodePort;
            if (fread(&g_oldnode[ii].datanode[kk].datanodePort, 1, sizeof(uint32), fd) != sizeof(uint32))
                goto read_failed;
            // uint32  datanodeRole;
            if (fread(&g_oldnode[ii].datanode[kk].datanodeRole, 1, sizeof(uint32), fd) != sizeof(uint32))
                goto read_failed;
            // uint32 datanodeLocalHAListenCount;
            if (fread(&g_oldnode[ii].datanode[kk].datanodeLocalHAListenCount, 1, sizeof(uint32), fd) != sizeof(uint32))
                goto read_failed;
            // char datanodeLocalHAIP[CM_IP_NUM][CM_IP_LENGTH];
            for (nn = 0; nn < CM_IP_NUM; nn++) {
                if (fread(&g_oldnode[ii].datanode[kk].datanodeLocalHAIP[nn], 1, CM_IP_LENGTH, fd) != CM_IP_LENGTH)
                    goto read_failed;
            }
            // uint32  datanodeLocalHAPort;
            if (fread(&g_oldnode[ii].datanode[kk].datanodeLocalHAPort, 1, sizeof(uint32), fd) != sizeof(uint32))
                goto read_failed;
            // char   datanodePeerDataPath[CM_PATH_LENGTH];
            if (fread(&g_oldnode[ii].datanode[kk].datanodePeerDataPath, 1, CM_PATH_LENGTH, fd) != CM_PATH_LENGTH)
                goto read_failed;
            // uint32 datanodePeerHAListenCount;
            if (fread(&g_oldnode[ii].datanode[kk].datanodePeerHAListenCount, 1, sizeof(uint32), fd) != sizeof(uint32))
                goto read_failed;
            // char datanodePeerHAIP[CM_IP_NUM][CM_IP_LENGTH];
            for (nn = 0; nn < CM_IP_NUM; nn++) {
                if (fread(&g_oldnode[ii].datanode[kk].datanodePeerHAIP[nn], 1, CM_IP_LENGTH, fd) != CM_IP_LENGTH)
                    goto read_failed;
            }
            // uint32  datanodePeerHAPort;
            if (fread(&g_oldnode[ii].datanode[kk].datanodePeerHAPort, 1, sizeof(uint32), fd) != sizeof(uint32))
                goto read_failed;
        }

        current_pos = ftell(fd);
        body_aglinment_size =
            (current_pos / AGLINMENT_SIZE + ((current_pos % AGLINMENT_SIZE == 0) ? 0 : 1)) * AGLINMENT_SIZE;
        if (fseek(fd, (off_t)body_aglinment_size, SEEK_SET) != 0)
            goto read_failed;
    }

    /*  */
    g_oldnode_id = g_nodeOldHeader.node - 1;
    if (g_oldnode_id < 0) {
        fprintf(stderr, "current node self is invalid  node =%d\n", g_nodeOldHeader.node);
        return -1;
    }
    g_currentoldNode = &g_oldnode[g_oldnode_id];

    fclose(fd);
    return 0;
read_failed:
    free(g_oldnode);
    g_oldnode = NULL;
    fclose(fd);
    fprintf(stderr, "read staticNodeConfig failed!\n");

    return -1;
}

bool cmp_old_and_new_cluster()
{
    uint32 old_dn_idx;
    char instname[CM_NODE_NAME_LEN];
    char dbpath[CM_PATH_LENGTH];
    if (g_oldnode_num != g_node_num) {
        pg_log(PG_FATAL, "old and new cluster has different number of nodes\n");
        return false;
    }

    /* validate only current node information */
    if (g_currentoldNode->coordinate != g_currentNode->coordinate) {
        pg_log(PG_FATAL,
            "coordinator are mismatch in node %s [OLD:%s, NEW:%s]\n",
            g_currentoldNode->nodeName,
            g_currentoldNode->coordinate ? "YES" : "NO",
            g_currentNode->coordinate ? "YES" : "NO");
        return false;
    }

    if (g_currentoldNode->gtm != g_currentNode->gtm) {
        pg_log(PG_FATAL, "gtm are mismaptch in node %s\n", g_currentoldNode->nodeName);
        return false;
    }

    for (old_dn_idx = 0; old_dn_idx < g_currentoldNode->datanodeCount; old_dn_idx++) {
        int insttype = INSTANCE_DATANODE;

        instname[0] = '\0';
        dbpath[0] = '\0';

        if ((user_opts.cluster_version >= 1) &&
            (DUMMY_STANDBY_DN == g_currentoldNode->datanode[old_dn_idx].datanodeRole)) {
            continue;
        }

        /* get dn instance name */
        if (CLUSTER_CONFIG_SUCCESS !=
            get_local_instancename_by_dbpath(g_currentoldNode->datanode[old_dn_idx].datanodeLocalDataPath, instname)) {
            pg_log(PG_FATAL,
                "unable to get the instance name in dbpath %s in node %s\n",
                g_currentoldNode->datanode[old_dn_idx].datanodeLocalDataPath,
                g_currentoldNode->nodeName);
            return false;
        }

        if (CLUSTER_CONFIG_SUCCESS != get_local_dbpath_by_instancename(instname, &insttype, dbpath, NULL)) {
            pg_log(PG_FATAL,
                "unable to map the instance name %s in new instance in node %s\n",
                instname,
                g_currentoldNode->nodeName);
            return false;
        }
    }

    return true;
}

bool upgrade_current_node(char* nodename)
{
    uint32 old_dn_idx;
    char instname[CM_NODE_NAME_LEN];
    char dbpath[CM_PATH_LENGTH];
    char* cmd = NULL;
    int32 ret;
    if (g_oldnode_num != g_node_num) {
        return false;
    }

    /* validate only current node information */
    if (g_currentNode->coordinate) {
        instname[0] = '\0';

        get_local_instancename_by_dbpath(g_currentoldNode->DataPath, instname);
        /* form gs_upgrade command and execute */
        cmd = form_local_instance_command(g_currentoldNode->DataPath, g_currentNode->DataPath, NULL, NULL);
        if ((dump_option == NULL || dump_option[0] == '\0') && (user_opts.command == COMMAND_INIT_UPGRADE)) {
            ret = execute_popen_command(cmd, nodename, instname);
            if (ret != 0) {
                pg_log(PG_FATAL, "Please check the last error\n");
            }

            dump_new_cluster_node_information(g_currentoldNode->DataPath);
        } else {
            execute_popen_commands_parallel(cmd, nodename, instname);
        }
        pg_free(cmd);
    }

    for (old_dn_idx = 0; old_dn_idx < g_currentoldNode->datanodeCount; old_dn_idx++) {
        int insttype = INSTANCE_DATANODE;
        uint32 instance_slot_id = 0;
        int node_index;
        int instance_index;
        instname[0] = '\0';
        dbpath[0] = '\0';

        if ((user_opts.cluster_version >= 1) &&
            (DUMMY_STANDBY_DN == g_currentoldNode->datanode[old_dn_idx].datanodeRole)) {
            continue;
        }

        /* get old dn instance name */
        if (CLUSTER_CONFIG_SUCCESS !=
            get_local_instancename_by_dbpath(g_currentoldNode->datanode[old_dn_idx].datanodeLocalDataPath, instname)) {
            return false;
        }

        /* search instance name in new cluster */
        if (CLUSTER_CONFIG_SUCCESS !=
            get_local_dbpath_by_instancename(instname, &insttype, dbpath, &instance_slot_id)) {
            return false;
        }

        if (PRIMARY_DN == g_currentNode->datanode[instance_slot_id].datanodeRole) {
            if (STANDBY_DN == g_currentNode->datanode[instance_slot_id].datanodePeerRole) {
                ret = search_HA_node(g_currentNode->datanode[instance_slot_id].datanodeLocalHAPort,
                    g_currentNode->datanode[instance_slot_id].datanodeLocalHAListenCount,
                    g_currentNode->datanode[instance_slot_id].datanodeLocalHAIP,
                    g_currentNode->datanode[instance_slot_id].datanodePeerHAPort,
                    g_currentNode->datanode[instance_slot_id].datanodePeerHAListenCount,
                    g_currentNode->datanode[instance_slot_id].datanodePeerHAIP,
                    &node_index,
                    &instance_index);
            } else if (STANDBY_DN == g_currentNode->datanode[instance_slot_id].datanodePeer2Role) {
                ret = search_HA_node(g_currentNode->datanode[instance_slot_id].datanodeLocalHAPort,
                    g_currentNode->datanode[instance_slot_id].datanodeLocalHAListenCount,
                    g_currentNode->datanode[instance_slot_id].datanodeLocalHAIP,
                    g_currentNode->datanode[instance_slot_id].datanodePeer2HAPort,
                    g_currentNode->datanode[instance_slot_id].datanodePeer2HAListenCount,
                    g_currentNode->datanode[instance_slot_id].datanodePeer2HAIP,
                    &node_index,
                    &instance_index);
            }
        }
        if (STANDBY_DN == g_currentNode->datanode[instance_slot_id].datanodeRole) {
            if (PRIMARY_DN == g_currentNode->datanode[instance_slot_id].datanodePeerRole) {
                ret = search_PN_node(g_currentNode->datanode[instance_slot_id].datanodePeerHAPort,
                    g_currentNode->datanode[instance_slot_id].datanodePeerHAListenCount,
                    g_currentNode->datanode[instance_slot_id].datanodePeerHAIP,
                    g_currentNode->datanode[instance_slot_id].datanodeLocalHAPort,
                    g_currentNode->datanode[instance_slot_id].datanodeLocalHAListenCount,
                    g_currentNode->datanode[instance_slot_id].datanodeLocalHAIP,
                    &node_index,
                    &instance_index);
            } else if (PRIMARY_DN == g_currentNode->datanode[instance_slot_id].datanodePeer2Role) {
                ret = search_PN_node(g_currentNode->datanode[instance_slot_id].datanodePeer2HAPort,
                    g_currentNode->datanode[instance_slot_id].datanodePeer2HAListenCount,
                    g_currentNode->datanode[instance_slot_id].datanodePeer2HAIP,
                    g_currentNode->datanode[instance_slot_id].datanodeLocalHAPort,
                    g_currentNode->datanode[instance_slot_id].datanodeLocalHAListenCount,
                    g_currentNode->datanode[instance_slot_id].datanodeLocalHAIP,
                    &node_index,
                    &instance_index);
            }
        }

        /* form gs_upgrade command and execute */
        cmd = form_local_instance_command(g_currentoldNode->datanode[old_dn_idx].datanodeLocalDataPath,
            dbpath,
            g_node[node_index].nodeName,
            g_node[node_index].datanode[instance_index].datanodeLocalDataPath);
        execute_popen_commands_parallel(cmd, nodename, instname);
        free(cmd);
    }

    return true;
}

bool validate_is_subcriber()
{
    return false;
}

void dump_new_cluster_node_information(char* olddatapath)
{
    char listen_ip[CM_IP_ALL_NUM_LENGTH] = {0};
    char peer_listen_ip[CM_IP_ALL_NUM_LENGTH] = {0};
    char sql_command[CM_MAX_COMMAND_LEN] = {0};
    char node_group_members1[MAX_NODE_GROUP_MEMBERS_LEN] = {0};
    char dn_nodename[CM_NODE_NAME_LEN + 1] = {0};
    FILE* globals_dump = NULL;

    int node_index = 0;
    int instance_index = 0;
    uint32 i;
    uint32 j;
    int ret;
    int nRet = 0;
    errno_t rc = 0;

    nRet = snprintf_s(
        sql_command, CM_MAX_COMMAND_LEN, CM_MAX_COMMAND_LEN - 1, "%s/upgrade_info/%s", olddatapath, GLOBALS_DUMP_FILE);
    securec_check_ss_c(nRet, "\0", "\0");
    if ((globals_dump = fopen(sql_command, PG_BINARY_A)) == NULL)
        pg_log(PG_FATAL, "Could not open dump file \"%s\": %s\n", sql_command, getErrorText(errno));

    fputs("\n\n--\n-- Nodes\n--\n", globals_dump);

    for (i = 0; i < g_node_num; i++) {
        if (1 == g_node[i].coordinate) {
            if (g_currentNode->node != g_node[i].node) {
                rc = memset_s(sql_command, CM_MAX_COMMAND_LEN, 0, CM_MAX_COMMAND_LEN);
                securec_check_c(rc, "\0", "\0");
                rc = memset_s(listen_ip, CM_IP_ALL_NUM_LENGTH, 0, CM_IP_ALL_NUM_LENGTH);
                securec_check_c(rc, "\0", "\0");

                listen_ip_merge(g_node[i].coordinateListenCount, g_node[i].coordinateListenIP, listen_ip);

                nRet = snprintf_s(sql_command,
                    CM_MAX_COMMAND_LEN,
                    CM_MAX_COMMAND_LEN - 1,
                    "CREATE NODE cn_%u WITH (type = 'coordinator', host='%s',port=%u);\n",
                    g_node[i].coordinateId,
                    listen_ip,
                    g_node[i].coordinatePort);
                securec_check_ss_c(nRet, "\0", "\0");
                fputs(sql_command, globals_dump);
            }
        }

        for (j = 0; j < g_node[i].datanodeCount; j++) {
            if (0 != g_node[i].datanode[j].datanodeRole) {
                continue;
            }

            rc = memset_s(sql_command, CM_MAX_COMMAND_LEN, 0, CM_MAX_COMMAND_LEN);
            securec_check_c(rc, "\0", "\0");
            rc = memset_s(listen_ip, CM_IP_ALL_NUM_LENGTH, 0, CM_IP_ALL_NUM_LENGTH);
            securec_check_c(rc, "\0", "\0");

            listen_ip_merge(
                g_node[i].datanode[j].datanodeListenCount, g_node[i].datanode[j].datanodeListenIP, listen_ip);

            node_index = 0;
            instance_index = 0;
            if (STANDBY_DN == g_node[i].datanode[j].datanodePeerRole) {
                ret = search_HA_node(g_node[i].datanode[j].datanodeLocalHAPort,
                    g_node[i].datanode[j].datanodeLocalHAListenCount,
                    g_node[i].datanode[j].datanodeLocalHAIP,
                    g_node[i].datanode[j].datanodePeerHAPort,
                    g_node[i].datanode[j].datanodePeerHAListenCount,
                    g_node[i].datanode[j].datanodePeerHAIP,
                    &node_index,
                    &instance_index);
            } else if (STANDBY_DN == g_node[i].datanode[j].datanodePeer2Role) {
                ret = search_HA_node(g_node[i].datanode[j].datanodeLocalHAPort,
                    g_node[i].datanode[j].datanodeLocalHAListenCount,
                    g_node[i].datanode[j].datanodeLocalHAIP,
                    g_node[i].datanode[j].datanodePeer2HAPort,
                    g_node[i].datanode[j].datanodePeer2HAListenCount,
                    g_node[i].datanode[j].datanodePeer2HAIP,
                    &node_index,
                    &instance_index);
            } else {
                ret = -1;
            }

            if (0 == ret) {
                rc = memset_s(peer_listen_ip, CM_IP_ALL_NUM_LENGTH, 0, CM_IP_ALL_NUM_LENGTH);
                securec_check_c(rc, "\0", "\0");

                listen_ip_merge(g_node[node_index].datanode[instance_index].datanodeListenCount,
                    g_node[node_index].datanode[instance_index].datanodeListenIP,
                    peer_listen_ip);

                nRet = snprintf_s(sql_command,
                    CM_MAX_COMMAND_LEN,
                    CM_MAX_COMMAND_LEN - 1,
                    "CREATE NODE dn_%u_%u WITH (type = 'datanode', host='%s',port= %d,  host1 = '%s',port1=%d);\n",
                    g_node[i].datanode[j].datanodeId,
                    g_node[node_index].datanode[instance_index].datanodeId,
                    listen_ip,
                    g_node[i].datanode[j].datanodePort,
                    peer_listen_ip,
                    g_node[node_index].datanode[instance_index].datanodePort);
                securec_check_ss_c(nRet, "\0", "\0");

                nRet = snprintf_s(dn_nodename,
                    (CM_NODE_NAME_LEN + 1),
                    sizeof(dn_nodename) - 1,
                    "dn_%u_%u,",
                    g_node[i].datanode[j].datanodeId,
                    g_node[node_index].datanode[instance_index].datanodeId);
                securec_check_ss_c(nRet, "\0", "\0");
            } else {
                nRet = snprintf_s(sql_command,
                    CM_MAX_COMMAND_LEN,
                    CM_MAX_COMMAND_LEN - 1,
                    "CREATE NODE dn_%u WITH (type = 'datanode', host='%s',port= %d,  host1 = '%s',port1=%d);\n",
                    g_node[i].datanode[j].datanodeId,
                    listen_ip,
                    g_node[i].datanode[j].datanodePort,
                    " ",
                    5432);
                securec_check_ss_c(nRet, "\0", "\0");

                nRet = snprintf_s(dn_nodename,
                    (CM_NODE_NAME_LEN + 1),
                    sizeof(dn_nodename) - 1,
                    "dn_%u,",
                    g_node[i].datanode[j].datanodeId);
                securec_check_ss_c(nRet, "\0", "\0");
            }

            fputs(sql_command, globals_dump);

            rc = strncat_s(node_group_members1, MAX_NODE_GROUP_MEMBERS_LEN, dn_nodename, sizeof(node_group_members1));
            securec_check_c(rc, "\0", "\0");
        }
    }

    node_group_members1[strlen(node_group_members1) - 1] = '\0';

    fputs("\n\n--\n-- Node groups\n--\n", globals_dump);

    nRet = snprintf_s(
        sql_command, CM_MAX_COMMAND_LEN, sizeof(sql_command) - 1, "CREATE NODE GROUP %s WITH ( ", GROUP_NAME_VERSION1);
    securec_check_ss_c(nRet, "\0", "\0");

    fputs(sql_command, globals_dump);
    /*node_group_members1*/
    fputs(node_group_members1, globals_dump);
    fputs(" );\n\n", globals_dump);
}

void create_standby_instance_setup(void)
{
    char repcommand[MAXPGPATH * 2];
    int i;
    int nRet = 0;

    if (os_info.is_root_user) {
        for (i = 0; i < os_info.num_tablespaces; i++) {
            nRet = snprintf_s(repcommand,
                (MAXPGPATH * 2),
                sizeof(repcommand) - 1,
                "mkdir --mode=700 -p %supgrd",
                os_info.tablespaces[i]);
            securec_check_ss_c(nRet, "\0", "\0");
            execute_command_in_remote_node(user_opts.pgHAipaddr, repcommand, false);

            nRet = snprintf_s(repcommand,
                (MAXPGPATH * 2),
                sizeof(repcommand) - 1,
                "chown --reference=\"%s/global/pg_control\" %supgrd",
                user_opts.pgHAdata,
                os_info.tablespaces[i]);
            securec_check_ss_c(nRet, "\0", "\0");
            execute_command_in_remote_node(user_opts.pgHAipaddr, repcommand, false);
        }

        read_popen_output_parallel();
    }

    /* start the master */
    start_postmaster_in_replication_mode(&new_cluster);

    pg_log(PG_REPORT, "Preparing Standby Instance for [%s]%s\n", user_opts.pgHAipaddr, user_opts.pgHAdata);

    if (os_info.is_root_user) {
        /* Form replication build command */
        nRet = snprintf_s(repcommand,
            (MAXPGPATH * 2),
            sizeof(repcommand) - 1,
            "su - %s -c 'gs_ctl build -r 3600 -Z restoremode -D %s -o \\\"-b\\\" '",
            new_cluster.user,
            user_opts.pgHAdata);
    } else {
        nRet = snprintf_s(repcommand,
            (MAXPGPATH * 2),
            sizeof(repcommand) - 1,
            "gs_ctl build -r 3600 -Z restoremode -D %s -o \\\"-b\\\" ",
            user_opts.pgHAdata);
    }

    securec_check_ss_c(nRet, "\0", "\0");
    for (i = 0; i < BUILD_RETRY; i++) {
        nRet = execute_command_in_remote_node(user_opts.pgHAipaddr, repcommand, false);
        read_popen_output_parallel();
        if (0 == nRet) {
            break;
        }
        sleep(3);
    }

    if (i == BUILD_RETRY) {
        pg_log(PG_FATAL, "Tried to build standby node %d times, failed\n", i);
    }

    if (os_info.is_root_user) {
        /* stop remote server */
        nRet = snprintf_s(repcommand,
            (MAXPGPATH * 2),
            sizeof(repcommand) - 1,
            "su - %s -c 'gs_ctl -Z restoremode -D %s stop'",
            new_cluster.user,
            user_opts.pgHAdata);
    } else {
        /* stop remote server */
        nRet = snprintf_s(repcommand,
            (MAXPGPATH * 2),
            sizeof(repcommand) - 1,
            "gs_ctl -Z restoremode -D %s stop",
            user_opts.pgHAdata);
    }
    securec_check_ss_c(nRet, "\0", "\0");
    execute_command_in_remote_node(user_opts.pgHAipaddr, repcommand, false);
    read_popen_output_parallel();

    /* stop the master */
    stop_postmaster(false);
}

void transfer_files_in_standby_instance()
{
    char new_instace_name[CM_NODE_NAME_LEN];
    char old_instace_name[CM_NODE_NAME_LEN];
    char* old_dbpath = NULL;
    char* new_dbpath = NULL;
    DbInfo* newDbInfo = NULL;
    DbInfo* oldDbInfo = NULL;
    int32 retval;

    uint32 i;
    int j;
    int nRet = 0;
    int tblnum = 0;

    if (0 != get_local_instancename_by_dbpath(new_cluster.pgdata, new_instace_name)) {
        pg_log(PG_FATAL, "Not able to find instance name in dbpath %s\n", new_cluster.pgdata);
        /*PG_FATAL*/
    }
    new_dbpath = new_cluster.pgdata;

    for (i = 0; i < g_currentoldNode->datanodeCount; i++) {
        retval =
            get_local_instancename_by_dbpath(g_currentoldNode->datanode[i].datanodeLocalDataPath, old_instace_name);
        if ((retval == CLUSTER_CONFIG_SUCCESS) &&
            (0 == strncmp(new_instace_name, old_instace_name, CM_NODE_NAME_LEN))) {
            old_dbpath = (char*)pg_malloc(MAXPGPATH);

            nRet = snprintf_s(old_dbpath,
                MAXPGPATH,
                MAXPGPATH - 1,
                "%s%s",
                g_currentoldNode->datanode[i].datanodeLocalDataPath,
                (true == user_opts.inplace_upgrade) ? "/full_upgrade_bak" : "");
            securec_check_ss_c(nRet, "\0", "\0");
            break;
        }
    }

    if (i >= g_currentoldNode->datanodeCount) {
        pg_log(PG_FATAL,
            "Can not map old and new database paths for %s with instance name %s\n",
            new_cluster.pgdata,
            new_instace_name);
        /*PG_FATAL*/
    }

    pg_log(PG_REPORT,
        "Transfer files from local standby for %s\n"
        "Old standby database path: %s\n"
        "New standby database path: %s\n",
        new_instace_name,
        old_dbpath,
        new_cluster.pgdata);
    pg_log(PG_REPORT, "--------------------------------------------------\n");

    readClusterInfo(&old_cluster, &os_info, new_dbpath, "oldclusterstatus");
    readClusterInfo(&new_cluster, &os_info, new_dbpath, "newclusterstatus");

    /* old_cluster.pgdata & new_cluster.pgdata is allocated again inside readClusterInfo */
    /* these paths can be used as paths are different in master and standby */
    pg_free(old_cluster.pgdata);
    pg_free(new_cluster.pgdata);

    /* restore preserved values */
    old_cluster.pgdata = old_dbpath;
    new_cluster.pgdata = new_dbpath;
    oldDbInfo = old_cluster.dbarr.dbs;
    for (j = 0; j < old_cluster.dbarr.ndbs; j++) {

        if (strlen(oldDbInfo->db_relative_tblspace) > 0) {
            nRet = memset_s(oldDbInfo->db_tblspace, MAXPGPATH, 0, MAXPGPATH);
            securec_check_c(nRet, "\0", "\0");
            nRet = snprintf_s(oldDbInfo->db_tblspace,
                MAXPGPATH,
                MAXPGPATH - 1,
                "%s/pg_location/%s",
                new_dbpath,
                oldDbInfo->db_relative_tblspace);
            securec_check_ss_c(nRet, "\0", "\0");
        }
        oldDbInfo++;
    }
    newDbInfo = new_cluster.dbarr.dbs;
    for (j = 0; j < new_cluster.dbarr.ndbs; j++) {
        if (strlen(newDbInfo->db_relative_tblspace) > 0) {
            nRet = memset_s(newDbInfo->db_tblspace, MAXPGPATH, 0, MAXPGPATH);
            securec_check_c(nRet, "\0", "\0");
            nRet = snprintf_s(newDbInfo->db_tblspace,
                MAXPGPATH,
                MAXPGPATH - 1,
                "%s/pg_location/%s",
                new_dbpath,
                newDbInfo->db_relative_tblspace);
            securec_check_ss_c(nRet, "\0", "\0");
        }
        newDbInfo++;
    }

    for (tblnum = 0; tblnum < os_info.num_tablespaces; tblnum++) {
        os_info.tablespaces[tblnum] = os_info.slavetablespaces[tblnum];
    }

    transfer_all_new_dbs(&old_cluster.dbarr, &new_cluster.dbarr, old_cluster.pgdata, new_cluster.pgdata);

    if (os_info.is_root_user) {
        exec_prog(UTILITY_LOG_FILE,
            NULL,
            true,
            "chown -R --reference=\"%s/global/pg_control\" \"%s\"",
            new_cluster.pgdata,
            new_cluster.pgdata);
        update_permissions_for_tablspace_dir();
    }
}

bool pg_isblank(const char c)
{
    return c == ' ' || c == '\t' || c == '\r';
}

void process_cluster_mapfile_option(void)
{
    char line[QUERY_ALLOC];
    FILE* all_dump = NULL;
    int32 i;
    int32 c;
    int32 line_len;
    char* cmd = NULL;

    if ((all_dump = fopen(cluster_map_file, PG_BINARY_R)) == NULL)
        pg_log(PG_FATAL, "Could not open dump file \"%s\": %s\n", cluster_map_file, getErrorText(errno));

    while (fgets(line, sizeof(line), all_dump) != NULL) {
        char* nodename = NULL;
        char* olddatadir = NULL;
        char* newdatadir = NULL;

        line_len = strlen(line);

        if (line_len > 0 && line[line_len - 1] != '\n') {
            pg_log(PG_FATAL, "Line is too long in mapfile can't process.\n");
        }

        i = 0;
        /*skip blanks*/
        while ((c = line[i]) != '\0' && pg_isblank(c))
            i++;
        if (c == '\0' || c == '\n') {
            continue;
        }

        nodename = line + i;
        while ((c = line[i]) != '\0' && (c != '\n') && (c != ','))
            i++;
        if (c == '\0' || c == '\n') {
            continue;
        }
        line[i] = '\0';

        i++;
        while ((c = line[i]) != '\0' && pg_isblank(c))
            i++;
        if (c == '\0' || c == '\n') {
            continue;
        }

        olddatadir = line + i;
        while ((c = line[i]) != '\0' && (c != '\n') && (c != ','))
            i++;
        if (c == '\0' || c == '\n') {
            continue;
        }
        line[i] = '\0';

        i++;
        while ((c = line[i]) != '\0' && pg_isblank(c))
            i++;
        if (c == '\0' || c == '\n') {
            continue;
        }
        newdatadir = line + i;
        while ((c = line[i]) != '\0' && (c != '\n') && (c != ','))
            i++;
        line[i] = '\0';

        if (nodename[0] == '\0' || olddatadir[0] == '\0' || newdatadir[0] == '\0') {
            continue;
        }

        while (pg_isblank(nodename[strlen(nodename) - 1]))
            nodename[strlen(nodename) - 1] = '\0';
        while (pg_isblank(olddatadir[strlen(olddatadir) - 1]))
            olddatadir[strlen(olddatadir) - 1] = '\0';
        while (pg_isblank(newdatadir[strlen(newdatadir) - 1]))
            newdatadir[strlen(newdatadir) - 1] = '\0';

        cmd = form_local_instance_command(olddatadir, newdatadir, NULL, NULL);
        execute_command_in_remote_node(nodename, cmd, true);
        pg_free(cmd);
    }

    fclose(all_dump);
}

char* form_local_instance_command(char* olddatadir, char* newdatadir, char* HA_name, char* HA_path)
{
    char* cmd = NULL;
    char* ha_options = NULL;
    int nRet = 0;

    cmd = (char*)pg_malloc(CM_PATH_LENGTH * 10);
    ha_options = (char*)pg_malloc(CM_PATH_LENGTH + CM_NODE_NAME_LEN + CM_NODE_NAME_LEN);
    if ((HA_name != NULL) && (HA_path != NULL)) {
        nRet = snprintf_s(ha_options,
            (CM_PATH_LENGTH + CM_NODE_NAME_LEN + CM_NODE_NAME_LEN),
            (CM_PATH_LENGTH + CM_NODE_NAME_LEN + CM_NODE_NAME_LEN) - 1,
            "--ha-nodename=%s --ha-path=%s",
            HA_name,
            HA_path);
        securec_check_ss_c(nRet, "\0", "\0");
    } else {
        ha_options[0] = '\0';
    }

    nRet = snprintf_s(cmd,
        (CM_PATH_LENGTH * 10),
        (CM_PATH_LENGTH * 10) - 1,
        "cd %s;export LD_LIBRARY_PATH"
        "=%s/../lib:\\$LD_LIBRARY_PATH;"
        "./gs_upgrade -d '%s%s' -D '%s' %s %s",
        new_cluster.bindir,
        new_cluster.bindir,
        olddatadir,
        ((user_opts.command == COMMAND_UPGRADE) && (true == user_opts.inplace_upgrade)) ? "/full_upgrade_bak" : "",
        newdatadir,
        g_gs_upgrade_opts->data,
        ha_options);
    securec_check_ss_c(nRet, "\0", "\0");

    return cmd;
}

char* form_standby_transfer_command(char* newdatadir)
{
    char* cmd = NULL;
    int nRet = 0;

    cmd = (char*)pg_malloc(CM_PATH_LENGTH * 10);

    nRet = snprintf_s(cmd,
        (CM_PATH_LENGTH * 10),
        (CM_PATH_LENGTH * 10) - 1,
        "cd %s;export LD_LIBRARY_PATH"
        "=%s/../lib:\\$LD_LIBRARY_PATH;"
        "./gs_upgrade -D '%s' %s -c transfer_files",
        new_cluster.bindir,
        new_cluster.bindir,
        newdatadir,
        g_gs_upgrade_opts->data);
    securec_check_ss_c(nRet, "\0", "\0");

    return cmd;
}

int32 find_cluster_config_version_number(char* gaussbinpath)
{
    char cmpath[MAXPGPATH];
    FILE* fd = NULL;
    int nRet = 0;

    nRet = snprintf_s(cmpath, MAXPGPATH, MAXPGPATH - 1, "%s/%s", gaussbinpath, STATIC_CONFIG_FILE);
    securec_check_ss_c(nRet, "\0", "\0");

    fd = fopen(cmpath, "r");
    if (fd == NULL) {
        fprintf(stderr, "STATIC_CONFIG_FILE(%s) is not found errno =%d\n", cmpath, errno);
        return -1;
    }

    // uint32 crc;
    if (fread(&g_nodeOldHeader.crc, 1, sizeof(uint32), fd) != sizeof(uint32))
        goto read_failed;
    // uint32 len;
    if (fread(&g_nodeOldHeader.len, 1, sizeof(uint32), fd) != sizeof(uint32))
        goto read_failed;
    // uint32  version;
    if (fread(&g_nodeOldHeader.version, 1, sizeof(uint32), fd) != sizeof(uint32))
        goto read_failed;

    fclose(fd);
    return g_nodeOldHeader.version;
read_failed:
    fclose(fd);
    fprintf(stderr, "failed to read (%s) file errno =%d\n", cmpath, errno);
    return -1;
}

void validate_cluster_upgade_options()
{
    int32 ret = -1;

    switch (user_opts.cluster_version) {
        case 0: /* POC_530 */
            ret = read_old_cluster_config_file_version_zero(old_cluster.bindir);
            break;
        case 1: /* PERF_POC */
        case 2: /* V1R5C00 & V1R5C10 */
            ret = read_old_cluster_config_file_version_one(old_cluster.bindir);
            break;
        case 3: /* LATEST VERSION */ /* V1R6C00, V1R6C10 && V1R7C00 */
            ret = read_old_cluster_config_file_version_three(old_cluster.bindir);
            break;
        default:
            pg_log(PG_FATAL, "Unknown cluster config file version number in old cluster.\n");
            break;
    }

    if (0 != ret) {
        pg_log(PG_FATAL, "Failed to parse the cluster config file in old cluster.\n");
    }

    /* currently hack for V1R5 as new cluster static_config_file is not created */
    /* TODO: Need to fix in read_config_file based on version number for reading latest file */
    if ((user_opts.inplace_upgrade == true) && (user_opts.command == COMMAND_INIT_UPGRADE) &&
        (user_opts.cluster_version < 3)) {
        uint32 nodeidx = 0;

        g_node = g_oldnode;
        g_node_num = g_oldnode_num;
        g_currentNode = g_currentoldNode;
        g_nodeHeader = g_nodeOldHeader;
        for (nodeidx = 0; nodeidx < g_node_num; nodeidx++) {
            if (g_node[nodeidx].node == g_oldnode_id) {
                g_nodeId = nodeidx;
            }
        }

        return;
    }

    if (0 != init_gauss_cluster_config(new_cluster.bindir)) {
        exit(1);
    }

    if (true != cmp_old_and_new_cluster()) {
        exit(1);
    }
}

int execute_command_in_remote_node(char* nodename, char* command, bool bparallel)
{
    char* fcmd = NULL;
    int32 len_fcmd = strlen(command) + strlen(nodename) + 64;
    char* mpprvFile = NULL;
    int nRet = 0;

    mpprvFile = getenv("MPPDB_ENV_SEPARATE_PATH");
    if (mpprvFile == NULL) {
        fcmd = (char*)pg_malloc(len_fcmd);
        nRet = snprintf_s(fcmd, len_fcmd, len_fcmd - 1, "ssh %s \"%s\"", nodename, command);
    } else {
        check_input_env_value(mpprvFile);
        len_fcmd = len_fcmd + strlen(mpprvFile);
        fcmd = (char*)pg_malloc(len_fcmd);
        nRet = snprintf_s(fcmd, len_fcmd, len_fcmd - 1, "ssh %s \"source %s;%s\"", nodename, mpprvFile, command);
    }
    securec_check_ss_c(nRet, "\0", "\0");

    if (bparallel) {
        execute_popen_commands_parallel(fcmd, nodename, NULL);
        nRet = 0;
    } else {
        nRet = execute_popen_command(fcmd, nodename, NULL);
    }

    free(fcmd);
    return nRet;
}

static char* form_upgrade_remote_cmd(char* nodename)
{
    char* buffer = NULL;
    int buflen;
    int nRet = 0;

    /* ./gs_upgrade -U newuser -u olduser -N nodename -b oldpath -B newpath */
    buflen = 256;
    /*LD_LIBRARY_PATH*/
    buflen += CM_PATH_LENGTH;
    /*newpath/gs_upgrade*/
    buflen += CM_PATH_LENGTH;
    /*newuser*/
    buflen += CM_NODE_NAME_LEN;
    /*olduser*/
    buflen += CM_NODE_NAME_LEN;
    /*nodename*/
    buflen += CM_NODE_NAME_LEN;
    /*oldpath*/
    buflen += CM_PATH_LENGTH;
    /*newpath*/
    buflen += CM_PATH_LENGTH;

    buffer = (char*)pg_malloc(buflen);

    nRet = snprintf_s(buffer,
        buflen,
        buflen - 1,
        "cd %s; export LD_LIBRARY_PATH=%s/../lib:\\$LD_LIBRARY_PATH; ./gs_upgrade -N '%s' %s",
        new_cluster.bindir,
        new_cluster.bindir,
        nodename,
        g_gs_upgrade_opts->data);
    securec_check_ss_c(nRet, "\0", "\0");

    return buffer;
}

void process_cluster_upgrade_options(void)
{
    if (COMMAND_GTM_UPGRADE == user_opts.command) {
        validate_cluster_upgade_options();
        upgrade_gtm();
        return;
    } else if (COMMAND_GTM_STOP == user_opts.command) {
        validate_cluster_upgade_options();
        stop_new_gtm();
        return;
    } else if (COMMAND_UPGRADE_LOG == user_opts.command) {
        upgrade_clog_dir();
        upgrade_log("pg_multixact/members");
        upgrade_log("pg_multixact/offsets");
        upgrade_replslot();
        unlink_pg_notify();
        return;
    }
    if ((NULL != cluster_map_file) || (NodeName != NULL)) {
        validate_cluster_upgade_options();
        /* process cluster mapfile option */
        if (NULL != cluster_map_file) {
            process_cluster_mapfile_option();
            return;
        }
        /* process all node options */
        else if (0 == strncmp(NodeName, "all", sizeof("all"))) {
            char* command = NULL;
            uint32 idx;

            for (idx = 0; idx < get_num_nodes(); idx++) {
                if (is_local_nodeid(g_node[idx].node)) {
                    upgrade_current_node(getnodename(idx));
                    continue;
                }

                command = form_upgrade_remote_cmd(getnodename(idx));
                execute_command_in_remote_node(getnodename(idx), command, true);
                free(command);
            }
            read_popen_output_parallel();
        }
        /* process single node option */
        else if (is_local_node(NodeName)) {
            upgrade_current_node(NULL);
            read_popen_output_parallel();
        }
    } else if (COMMAND_TRANSFER_FILES == user_opts.command) {
        validate_cluster_upgade_options();
        transfer_files_in_standby_instance();
    }
    /* process single instance option */
    else {
        validate_cluster_upgade_options();
        upgrade_local_instance();
    }
}

static void check_input_env_value(char* input_env_value)
{
    char* danger_token[] = {"|",
        ";",
        "&",
        "$",
        "<",
        ">",
        "`",
        "\\",
        "'",
        "\"",
        "{",
        "}",
        "(",
        ")",
        "[",
        "]",
        "~",
        "*",
        "?",
        "!",
        "\n",
        NULL};
    int i = 0;
    for (i = 0; danger_token[i] != NULL; i++) {
        if (strstr(input_env_value, danger_token[i])) {
            fprintf(stderr, "Error: invalid token \"%s\".\n", danger_token[i]);
            exit(1);
        }
    }
}
