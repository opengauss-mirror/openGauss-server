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
 * cm_config.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/common/config/cm_config.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CM_CONFIG_H
#define CM_CONFIG_H

#include "c.h"

#define CM_NODE_NAME 65
#define CM_AZ_NAME 65
#define CM_QUORUM_NAME 65
#define CM_LOGIC_CLUSTER_NAME_LEN 64
#define LOGIC_CLUSTER_NUMBER (32 + 1)  // max 32 logic + 1 elastic group
#define MAX_PATH_LEN 1024
#define CM_IP_NUM 3
#define GTM_IP_PORT 64
#define CM_IP_LENGTH 128
#define CM_IP_ALL_NUM_LENGTH (CM_IP_NUM * CM_IP_LENGTH)
#define CM_PATH_LENGTH 1024
#define CM_NODE_MAXNUM 1024
#define CM_MAX_DATANODE_PER_NODE 160
#define CM_MAX_INSTANCE_PER_NODE 192
#define INVALID_NODE_NUM (CM_NODE_MAXNUM + 1)
#define INVALID_INSTACNE_NUM (0xFFFFFFFF)
#define CM_MAX_INSTANCE_GROUP_NUM (CM_NODE_MAXNUM * CM_MAX_INSTANCE_PER_NODE)
#define AGLINMENT_SIZE (1024 * 8)
#define CM_MAX_COMMAND_LEN 1024
#define CM_MAX_COMMAND_LONG_LEN 2048
#define CONNSTR_LEN 256
#define DN_SYNC_LEN (256)

// A logic datanode number in a node, The logic datanode can be include the primary, standby and dummystandby DN.
#define LOGIC_DN_PER_NODE 32
#define MAX_LOGIC_DATANODE (CM_NODE_MAXNUM * LOGIC_DN_PER_NODE)

#define CM_PRIMARY_STANDBY_NUM (8)  // supprot one primary and multi standby
#define CM_MAX_CMSERVER_STANDBY_NUM (7)

#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS)))
#define CM_MAX_DATANODE_STANDBY_NUM 7
#else
#define CM_MAX_DATANODE_STANDBY_NUM 8
#endif

#define CM_MAX_GTM_STANDBY_NUM (7)
#define MAX_CN_NUM 256
#define CM_MAX_SQL_COMMAND_LEN 2048

#define HAVE_GTM 1
#define NONE_GTM 0
#define PRIMARY_GTM 0
#define STANDBY_GTM 1

// consist with OM
#define PRIMARY_DN 0
#define STANDBY_DN 1
#define DUMMY_STANDBY_DN 2

#define CM_DATANODE 1
#define CM_COORDINATENODE 2
#define CM_GTM 3

#define CM_AGENT 7
#define CM_CTL 8
#define CM_SERVER 9

#define GROUP_NAME_VERSION1 "group_version1"
#define GROUP_NAME_VERSION2 "group_version2"
#define GROUP_NAME_VERSION1_BUCKET "group_version1_bucket_"
#define CHILD_NODE_GROUP_NUM 4
#define MIN_BUCKETCOUNT 32

#define OPEN_FILE_ERROR -1
#define OUT_OF_MEMORY -2
#define READ_FILE_ERROR -3

#define CLUSTER_STATUS_QUERY 0
#define CLUSTER_DETAIL_STATUS_QUERY 1
#define CLUSTER_COUPLE_STATUS_QUERY 2
#define CLUSTER_COUPLE_DETAIL_STATUS_QUERY 3
#define CLUSTER_BALANCE_COUPLE_DETAIL_STATUS_QUERY 4
#define CLUSTER_LOGIC_COUPLE_DETAIL_STATUS_QUERY 5
#define CLUSTER_PARALLEL_REDO_REPLAY_STATUS_QUERY 6
#define CLUSTER_PARALLEL_REDO_REPLAY_DETAIL_STATUS_QUERY 7
#define CLUSTER_ABNORMAL_COUPLE_DETAIL_STATUS_QUERY 8
#define CLUSTER_ABNORMAL_BALANCE_COUPLE_DETAIL_STATUS_QUERY 9
#define CLUSTER_START_STATUS_QUERY 10

#define DYNAMC_CONFIG_FILE "cluster_dynamic_config"

#define PROCESS_UNKNOWN -1
#define PROCESS_NOT_EXIST 0
#define PROCESS_CORPSE 1
#define PROCESS_RUNNING 2
#define PROCESS_PHONY_DEAD_T 3
#define PROCESS_PHONY_DEAD_D 4
#define PROCESS_PHONY_DEAD_Z 5
/***/
#define PROCESS_WAIT_STOP 3
#define PROCESS_WAIT_START 4

#define PROCESS_ETCD 0
#define PROCESS_CMSERVER 1

#define QUERY_STATUS_CMSERVER_STEP 0
#define QUERY_STATUS_CMSERVER_SETP_ACK 1
#define QUERY_STATUS_CMAGENT_STEP 2
#define QUERY_STATUS_CMAGENT_STEP_ACK 3

#define AZ_START_STOP_INTERVEL 2
#define AZ_STOP_DELAY 2

#define ETCD_KEY_LENGTH 1024
#define ETCD_VLAUE_LENGTH 1024

#define MALLOC_BY_NODE_NUM 0
#define MALLOC_BY_NODE_MAXNUM 1

#define CASCADE_STANDBY_TYPE 3
#define STATIC_CONFIG_FILE "cluster_static_config"
#define DYNAMIC_DNROLE_FILE "cluster_dnrole_config"
/* the max real path length in linux is 4096, adapt this for realpath func */
#define MAX_REALPATH_LEN 4096

/* az_Priorities, only init the values when load config file */
const uint32 g_az_invalid = 0;
extern uint32 g_az_master;
extern uint32 g_az_slave;
extern uint32 g_az_arbiter;

typedef enum AZRole { AZMaster, AZSlave, AZArbiter } AZRole;

typedef struct az_role_string {
    const char* role_string;
    uint32 role_val;
} az_role_string;

typedef enum cmServerLevel {
    CM_SERVER_NONE = 0,  // no cm_server
    CM_SERVER_LEVEL_1 = 1
} cmServerLevel;

typedef enum ctlToCmNotifyDetail {
    CLUSTER_STARTING = 0
} ctlToCmNotifyDetail;

typedef struct staticConfigHeader {
    uint32 crc;
    uint32 len;
    uint32 version;
    int64 time;
    uint32 nodeCount;
    uint32 node;
} staticConfigHeader;

typedef struct peerDatanodeInfo {
    char datanodePeerDataPath[CM_PATH_LENGTH];
    uint32 datanodePeerHAListenCount;
    char datanodePeerHAIP[CM_IP_NUM][CM_IP_LENGTH];
    uint32 datanodePeerHAPort;
    uint32 datanodePeerRole;
} peerDatanodeInfo;

typedef struct dataNodeInfo {
    uint32 datanodeId;
    uint32 datanodeMirrorId;
    char datanodeLocalDataPath[CM_PATH_LENGTH];
    char datanodeXlogPath[CM_PATH_LENGTH];
    char datanodeSSDDataPath[CM_PATH_LENGTH];
    uint32 datanodeListenCount;
    char datanodeListenIP[CM_IP_NUM][CM_IP_LENGTH];
    uint32 datanodePort;

    uint32 datanodeRole;

    uint32 datanodeLocalHAListenCount;
    char datanodeLocalHAIP[CM_IP_NUM][CM_IP_LENGTH];
    uint32 datanodeLocalHAPort;

    char datanodePeerDataPath[CM_PATH_LENGTH];
    uint32 datanodePeerHAListenCount;
    char datanodePeerHAIP[CM_IP_NUM][CM_IP_LENGTH];
    uint32 datanodePeerHAPort;
    uint32 datanodePeerRole;

    char datanodePeer2DataPath[CM_PATH_LENGTH];
    uint32 datanodePeer2HAListenCount;
    char datanodePeer2HAIP[CM_IP_NUM][CM_IP_LENGTH];
    uint32 datanodePeer2HAPort;
    uint32 datanodePeer2Role;

    char LogicClusterName[CM_LOGIC_CLUSTER_NAME_LEN];

    // multi available zone: single primary and multi standby datanodes
    peerDatanodeInfo peerDatanodes[CM_MAX_DATANODE_STANDBY_NUM];
} dataNodeInfo;

typedef struct peerCmServerInfo {
    uint32 cmServerPeerHAListenCount;
    char cmServerPeerHAIP[CM_IP_NUM][CM_IP_LENGTH];
    uint32 cmServerPeerHAPort;
} peerCmServerInfo;

typedef struct peerGtmInfo {
    uint32 gtmPeerHAListenCount;
    char gtmPeerHAIP[CM_IP_NUM][CM_IP_LENGTH];
    uint32 gtmPeerHAPort;
} peerGtmInfo;

typedef struct staticNodeConfig {
    uint32 crc;

    char azName[CM_AZ_NAME];
    uint32 azPriority;

    uint32 node;
    char nodeName[CM_NODE_NAME];

    uint32 sshCount;
    char sshChannel[CM_IP_NUM][CM_IP_LENGTH];

    uint32 backIpCount;
    char backIps[CM_IP_NUM][CM_IP_LENGTH];

    /**/
    uint32 cmServerId;
    uint32 cmServerMirrorId;
    char cmDataPath[CM_PATH_LENGTH];
    uint32 cmServerLevel;
    char cmServerFloatIP[CM_IP_LENGTH];

    uint32 cmServerListenCount;
    char cmServer[CM_IP_NUM][CM_IP_LENGTH];
    uint32 port;
    /**/
    uint32 cmServerLocalHAListenCount;
    char cmServerLocalHAIP[CM_IP_NUM][CM_IP_LENGTH];
    uint32 cmServerLocalHAPort;

    int32 cmServerRole;
    /**/
    uint32 cmServerPeerHAListenCount;
    char cmServerPeerHAIP[CM_IP_NUM][CM_IP_LENGTH];
    uint32 cmServerPeerHAPort;

    peerCmServerInfo peerCmServers[CM_MAX_CMSERVER_STANDBY_NUM];

    /**/
    uint32 gtmAgentId;
    uint32 cmAgentMirrorId;
    uint32 cmAgentListenCount;
    char cmAgentIP[CM_IP_NUM][CM_IP_LENGTH];

    /**/
    uint32 gtmId;
    uint32 gtmMirrorId;
    uint32 gtm;
    char gtmLocalDataPath[CM_PATH_LENGTH];
    uint32 gtmLocalListenCount;
    char gtmLocalListenIP[CM_IP_NUM][CM_IP_LENGTH];
    uint32 gtmLocalport;

    uint32 gtmRole;

    uint32 gtmLocalHAListenCount;
    char gtmLocalHAIP[CM_IP_NUM][CM_IP_LENGTH];
    uint32 gtmLocalHAPort;

    char gtmPeerDataPath[CM_PATH_LENGTH];
    uint32 gtmPeerHAListenCount;
    char gtmPeerHAIP[CM_IP_NUM][CM_IP_LENGTH];
    uint32 gtmPeerHAPort;

    peerGtmInfo peerGtms[CM_MAX_CMSERVER_STANDBY_NUM];

    /*****/
    uint32 gtmProxyId;
    uint32 gtmProxyMirrorId;
    uint32 gtmProxy;
    uint32 gtmProxyListenCount;
    char gtmProxyListenIP[CM_IP_NUM][CM_IP_LENGTH];
    uint32 gtmProxyPort;

    /*****/
    uint32 coordinateId;
    uint32 coordinateMirrorId;
    uint32 coordinate;
    char DataPath[CM_PATH_LENGTH];
    char SSDDataPath[CM_PATH_LENGTH];
    uint32 coordinateListenCount;
    char coordinateListenIP[CM_IP_NUM][CM_IP_LENGTH];
    uint32 coordinatePort;
    uint32 coordinateHAPort;

    /*****/
    uint32 datanodeCount;
    dataNodeInfo datanode[CM_MAX_DATANODE_PER_NODE];

    uint32 etcd;
    uint32 etcdId;
    uint32 etcdMirrorId;
    char etcdName[CM_NODE_NAME];
    char etcdDataPath[CM_PATH_LENGTH];
    uint32 etcdClientListenIPCount;
    char etcdClientListenIPs[CM_IP_NUM][CM_IP_LENGTH];
    uint32 etcdClientListenPort;
    uint32 etcdHAListenIPCount;
    char etcdHAListenIPs[CM_IP_NUM][CM_IP_LENGTH];
    uint32 etcdHAListenPort;

    uint32 sctpBeginPort;
    uint32 sctpEndPort;

} staticNodeConfig;

typedef struct dynamicConfigHeader {
    uint32 crc;
    uint32 len;
    uint32 version;
    int64 time;
    uint32 nodeCount;  /* physicalNodeNum,same as staic config file head. */
    uint32 relationCount;
    uint32 term;
} dynamicConfigHeader;

typedef struct dynamic_cms_timeline {
    long int timeline;  // Record current time at the CMS instance is upgraded to the primary instance.
} dynamic_cms_timeline;

typedef struct dynamicRelationConfig {
    uint32 crc;
    uint32 currentPrimary;
    uint32 node[CM_PRIMARY_STANDBY_NUM];
    char dataPath[CM_PRIMARY_STANDBY_NUM][CM_PATH_LENGTH];
} dynamicRelationConfig;

typedef struct staticLogicNodeConfig {

    uint32 crc;
    uint32 node;
    char nodeName[CM_NODE_NAME];

    uint32 sshCount;
    char sshChannel[CM_IP_NUM][CM_IP_LENGTH];

    uint32 backIpCount;
    char backIps[CM_IP_NUM][CM_IP_LENGTH];

    uint32 sctpBeginPort;
    uint32 sctpEndPort;

    uint32 datanodeCount;
    uint32 datanodeId[CM_MAX_DATANODE_PER_NODE];
} staticLogicNodeConfig;

typedef struct logicClusterStaticConfig {
    char LogicClusterName[CM_LOGIC_CLUSTER_NAME_LEN];
    uint32 LogicClusterStatus;
    uint32 isLogicClusterBalanced;
    uint32 isRedistribution;
    staticConfigHeader logicClusterNodeHeader;
    staticLogicNodeConfig* logicClusterNode;
} logicClusterStaticConfig;

typedef struct logicClusterInfo {
    uint32 logicClusterId;
    char logicClusterName[CM_LOGIC_CLUSTER_NAME_LEN];
} logicClusterInfo;

typedef struct logicClusterList {
    uint32 logicClusterCount;
    logicClusterInfo lcInfoArray[LOGIC_CLUSTER_NUMBER];
} logicClusterList;

typedef struct AZ_Info {
    char* nodeName;
    uint32 azPriority;
} AZList;

extern staticConfigHeader g_nodeHeader;
extern staticNodeConfig* g_node;
extern staticNodeConfig* g_currentNode;
extern bool g_single_node_cluster;
extern bool g_multi_az_cluster;
extern bool g_one_master_multi_slave;
extern bool g_only_dn_cluster;
extern uint32 g_node_num;
extern uint32 g_cluster_total_instance_group_num;
extern uint32 g_etcd_num;
extern uint32 g_cm_server_num;
extern uint32 g_dn_replication_num;
extern uint32 g_gtm_num;
extern uint32 g_coordinator_num;
extern uint32 max_node_name_len;
extern uint32 max_az_name_len;
extern uint32 max_datapath_len;
extern uint32 max_cnpath_len;
extern uint32 max_gtmpath_len;
extern uint32 max_etcdpath_len;
extern uint32 max_cmpath_len;
extern logicClusterStaticConfig g_logicClusterStaticConfig[LOGIC_CLUSTER_NUMBER];
extern uint32 g_logic_cluster_count;
extern uint32 max_logic_cluster_state_len;
extern uint32 max_logic_cluster_name_len;
extern bool logic_cluster_query;
extern bool logic_cluster_restart;
extern uint32 g_datanodeid;
extern char* g_logicClusterName;
extern uint32 g_local_node_idx;
extern char* g_local_node_name;
extern char* g_lcname;

extern int read_single_file(const char *file_path, int *err_no, uint32 nodeId, const char *dataPath);
extern int read_config_file(const char* file_path, int* err_no,
    bool inReload = false, int mallocByNodeNum = MALLOC_BY_NODE_NUM);
extern int read_logic_cluster_name(const char* file_path, logicClusterList& lcList, int* err_no);
extern int read_logic_cluster_config_files(const char* file_path, int* err_no);
extern int read_lc_config_file(const char* file_path, int* err_no);
extern int find_node_index_by_nodeid(uint32 nodeId, uint32* node_index);
extern int find_current_node_by_nodeid(uint32 nodeId);
extern int node_index_Comparator(const void* arg1, const void* arg2);
extern void set_cm_read_flag(bool falg);
extern char* getAZNamebyPriority(uint32 azPriority);
extern int cmconfig_getenv(const char* env_var, char* output_env_value, uint32 env_value_len);
extern int get_dynamic_dn_role(void);
extern int get_nodename_list_by_AZ(const char* AZName, const char* data_dir, char** nodeNameList);
extern int checkPath(const char* fileName);
extern bool has_static_config();
extern bool CheckDataNameValue(const char *datanodeName, const char *dataDir);

#endif
