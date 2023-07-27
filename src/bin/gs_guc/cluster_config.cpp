/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
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
 *---------------------------------------------------------------------------------------
 *
 *  cluster_config.cpp
 *        Interfaces for analysis manager of PDK tool.
 *
 *        Function List:  find_gucoption_available
 *                        freefile
 *                        getnodename
 *                        get_local_cordinator_dbpath
 *                        get_local_datanode_dbpath
 *                        get_local_dbpath_by_instancename
 *                        get_local_gtmproxy_dbpath
 *                        get_local_gtm_dbpath
 *                        get_local_gtm_name
 *                        get_local_gtm_proxy_name
 *                        get_local_instancename_by_dbpath
 *                        get_local_num_datanode
 *                        get_nodeidx_by_name
 *                        get_node_nodename
 *                        get_num_nodes
 *                        get_value_in_config_file
 *                        init_gauss_cluster_config
 *                        is_local_node
 *                        is_local_nodeid
 *                        readfile
 *
 * IDENTIFICATION
 *        src/bin/gs_guc/cluster_config.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <getopt.h>
#include <stdio.h>
#include <limits.h>

#include "common/config/cm_config.h"
#include "bin/elog.h"
#include "securec.h"
#include "securec_check.h"
#include "port.h"

#define MAX_VALUE_LEN 1024
#define MAX_PARAM_LEN 1024
#define CLUSTER_CONFIG_SUCCESS 0
#define CLUSTER_CONFIG_ERROR 1
#define CM_NODE_NAME_LEN 64

#define STADARD_SSH_PORT 22

#define STD_FORMAT_ARG_POSITION 2

extern char** cndn_param;
extern char** cmserver_param;
extern char** cmagent_param;
extern char** gtm_param;
extern char** lc_param;
extern char** cndn_guc_info;
extern char** cmserver_guc_info;
extern char** cmagent_guc_info;
extern char** gtm_guc_info;
extern char** lc_guc_info;
extern int cndn_param_number;
extern int cmserver_param_number;
extern int cmagent_param_number;
extern int gtm_param_number;
extern int lc_param_number;
extern uint32 g_local_dn_idx;
extern char* g_current_data_dir;

const int g_min_ip_len = 7;  // IPV4 and IPV6, choose the minimum length

#ifndef GS_COLLECTOR_BUILD
extern void write_stderr(const char* fmt, ...)
    /* This extension allows gcc to check the format string for consistency with
       the supplied arguments. */
    __attribute__((format(PG_PRINTF_ATTRIBUTE, 1, STD_FORMAT_ARG_POSITION)));
#else
#define write_stderr printf
#endif

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

typedef enum {
    INSTANCE_ANY,
    INSTANCE_DATANODE,     /* postgresql.conf */
    INSTANCE_COORDINATOR,  /* postgresql.conf */
    INSTANCE_GTM,          /* gtm.conf        */
    INSTANCE_GTM_PROXY,    /* gtm_proxy.conf  */
    INSTANCE_CMAGENT,      /* cm_agent.conf */
    INSTANCE_CMSERVER,     /* cm_server.conf */
    INSTANCE_DATAINSTANCE, /* postgresql.conf */
} NodeType;

/* Define all the node types */
typedef enum {
    GUC_NONE = 0,
    GUC_CNDN,
    GUC_GTM,
    GUC_CMSERVER,
    GUC_CMAGENT,
    GUC_LCNAME
} GUC_Node_Type;

const int INVALID_LINES_IDX = -1;
#define GUC_OPT_CONF_FILE "cluster_guc.conf"
#define SUCCESS 0
#define FAILURE 1
#define GS_FREE(ptr)            \
    do {                        \
        if (NULL != (ptr)) {    \
            free((char*)(ptr)); \
            ptr = NULL;         \
        }                       \
    } while (0)

int32 get_local_instancename_by_dbpath(const char* dbpath, char* instancename);
int32 get_local_gtm_name(char* instancename);
int get_value_in_config_file(const char* pg_config_file, const char* parameter_in_config, char* para_value);
int get_all_datanode_num();
int get_all_coordinator_num();
int get_all_cmserver_num();
int get_all_cmagent_num();
int get_all_cndn_num();
int get_all_gtm_num();
char* get_AZname_by_nodename(const char* nodename);
int find_gucoption_available(
    const char** optlines, const char* opt_name, int* name_offset, int* name_len, int* value_offset, int* value_len);
char** readfile(const char* path, int reserve_num_lines);

void freefile(char** lines);

extern bool get_env_value(const char* env_var, char* output_env_value, size_t env_var_value_len);
void* pg_malloc_memory(size_t size);
extern char* xstrdup(const char* s);
extern char* g_local_instance_path;
extern void check_env_value(const char* input_env_value);

/* 
 ******************************************************************************
 Function    : get_local_num_datanode
 Description :
 Input       :
 Output      : None
 Return      : None
 ****************************************************************************** 
*/
uint32 get_local_num_datanode()
{
    return g_currentNode->datanodeCount;
}

/*
 ******************************************************************************
 Function    : get_num_nodes
 Description :
 Input       :
 Output      : None
 Return      : None
 ******************************************************************************
*/
uint32 get_num_nodes()
{
    return g_node_num;
}

/*
 ******************************************************************************
 Function    : is_local_nodeid
 Description : check the input node id is current node id
 Input       : nodeid - node id
 Output      : None
 Return      : None
 ******************************************************************************
*/
bool is_local_nodeid(uint32 nodeid)
{
    return (g_currentNode->node == nodeid);
}

#ifdef GS_COLLECTOR_BUILD
staticNodeConfig* get_node_nodename(char* name);

/*
 ******************************************************************************
 Function    : get_node_nodename
 Description :
 Input       : nodename -
 Output      : None
 Return      : None
 ******************************************************************************
*/
staticNodeConfig* get_node_nodename(char* nodename)
{
    uint32 nodeidx = 0;
    for (nodeidx = 0; nodeidx < g_node_num; nodeidx++) {
        if (0 == strncmp(g_node[nodeidx].nodeName, nodename, CM_NODE_NAME_LEN)) {
            return &g_node[nodeidx];
        }
    }

    return NULL;
}

#endif

/*
 ******************************************************************************
 Function    : get_nodeidx_by_name
 Description :
 Input       : nodename - node name
 Output      : None
 Return      : uint32 - node id index
 ******************************************************************************
*/
int32 get_nodeidx_by_name(const char* nodename)
{
    uint32 nodeidx = 0;
    for (nodeidx = 0; nodeidx < get_num_nodes(); nodeidx++) {
        if (0 == strncmp(g_node[nodeidx].nodeName, nodename, CM_NODE_NAME_LEN)) {
            return (int32)nodeidx;
        }
    }

    return -1;
}
/*
 ******************************************************************************
 Function    : get_all_datanode_num
 Description : get all datanode instance number
 ******************************************************************************
*/
int get_all_datanode_num()
{
    uint32 nodeidx = 0;
    int count = 0;
    for (nodeidx = 0; nodeidx < get_num_nodes(); nodeidx++) {
        count += (int)g_node[nodeidx].datanodeCount;
    }
    return count;
}
/*
 ******************************************************************************
 Function    : get_all_coordinator_num
 Description : get all coordinator instance number
 ******************************************************************************
*/
int get_all_coordinator_num()
{
    uint32 nodeidx = 0;
    int count = 0;
    for (nodeidx = 0; nodeidx < get_num_nodes(); nodeidx++) {
        if (1 == g_node[nodeidx].coordinate) {
            count += 1;
        }
    }
    return count;
}
/*
 ******************************************************************************
 Function    : get_all_cmserver_num
 Description : get all cm_server instance number
 ******************************************************************************
*/
int get_all_cmserver_num()
{
    uint32 nodeidx;
    int count = 0;
    for (nodeidx = 0; nodeidx < get_num_nodes(); nodeidx++) {
        if (1 == g_node[nodeidx].cmServerLevel && g_node[nodeidx].cmDataPath[0] != '\0') {
            count += 1;
        }
    }

    return count;
}
/*
 ******************************************************************************
 Function    : get_all_cmagent_num
 Description : get all cm_agent instance number
 ******************************************************************************
*/
int get_all_cmagent_num()
{
    return get_num_nodes();
}

/*
 ******************************************************************************
 Function    : get_all_cndn_num
 Description : get all CN and DN instance number
 ******************************************************************************
*/
int get_all_cndn_num()
{
    int count = 0;
    count = get_all_datanode_num() + get_all_coordinator_num();

    return count;
}

int get_all_gtm_num()
{
    uint32 nodeidx = 0;
    int count = 0;
    for (nodeidx = 0; nodeidx < get_num_nodes(); nodeidx++) {
        if (1 == g_node[nodeidx].gtm && g_node[nodeidx].gtmLocalDataPath[0] != '\0') {
            count += 1;
        }
    }
    return count;
}

/*
 ******************************************************************************
 Function    : is_local_node
 Description :
 Input       : nodename -
 Output      : None
 Return      : None
 ******************************************************************************
*/
bool is_local_node(const char* nodename)
{
    return (0 == strncmp(g_currentNode->nodeName, nodename, CM_NODE_NAME_LEN));
}

/*
 ******************************************************************************
 Function    : getnodename
 Description : get node name by node id index
 Input       : nodeidx - node id index
 Output      : None
 Return      : char * - node name
 ******************************************************************************
*/
char* getnodename(uint32 nodeidx)
{
    return g_node[nodeidx].nodeName;
}

/*
 ******************************************************************************
 Function    : get_hostname_or_ip
 Description : if In agent mode, there is an environment variable HOST_IP writen in /etc/profile,
               then get host ip from environment variables.
               Else, get hostname for adaptation to the previous version.
 Input       : name_len - the length of ip or hostname
 Output      : out_name - the host ip get from environment variables or the hostname
 Return      : bool
 ******************************************************************************
*/
bool get_hostname_or_ip(char* out_name, size_t name_len)
{
    int rc = 0;
    char* env_value = NULL;

    if (out_name == NULL) {
        (void)write_stderr("ERROR: Get NULL point from upper function when get hostip or hostname.\n");
        return false;
    }

    env_value = gs_getenv_r("HOST_IP");
    if (env_value != NULL) {
        check_env_value(env_value);
    }

    if ((env_value == NULL) || (env_value[0] == '\0')) {
        (void)gethostname(out_name, name_len);
        if (out_name[0] == '\0') {
            return false;
        }
    } else {
        if (strlen(env_value) >= name_len) {
            (void)write_stderr("ERROR: The value of environment variable HOST_IP is too long.\n");
            return false;
        }

        if (strlen(env_value) < g_min_ip_len) {
            (void)write_stderr("ERROR: The value of environment variable HOST_IP is too short.\n");
            return false;
        }

        rc = strcpy_s(out_name, name_len, env_value);
        securec_check_c(rc, "\0", "\0");
    }
    return true;
}

/*
 ******************************************************************************
 Function    : get_backIps_by_nodename
 Description : get back ip  by node name,
 Input       : nodename -
 Output      : ipAddress
 Return      : ipAddress
 ******************************************************************************
*/
char* get_backIps_by_nodename(const char* nodename)
{
    uint32 nodeidx = 0;
    char* ipAddress = NULL;
    for (nodeidx = 0; nodeidx < g_node_num; nodeidx++) {
        if (strcmp(g_node[nodeidx].nodeName, nodename) == 0) {
            // Assignment by cluster_static_config value
            ipAddress = xstrdup(g_node[nodeidx].backIps[0]);
        }
    }
    return ipAddress;
}

/*
 ******************************************************************************
 Function    : is_instance_in_nodename
 Description : check is the instance in node
 Input       : nodename -
 Output      : bool
 ******************************************************************************
*/
bool is_instance_in_nodename(const char* nodename)
{
    uint32 i;
    char* backIp = NULL;

    backIp = get_backIps_by_nodename(nodename);
    if (NULL == backIp) {
        return false;
    }

    for (i = 0; i < g_currentNode->datanodeCount; i++) {
        if (strcmp(g_currentNode->datanode[i].datanodeLocalDataPath, g_local_instance_path) == 0) {
            for (uint32 dnId = 0; dnId < CM_MAX_DATANODE_STANDBY_NUM; dnId++) {
                if (strcmp(backIp, g_currentNode->datanode[i].peerDatanodes[dnId].datanodePeerHAIP[0]) == 0) {
                    GS_FREE(backIp);
                    return true;
                }
            }
        }
    }
    GS_FREE(backIp);
    return false;
}
/*
 ******************************************************************************
 Function    : get_AZname_by_nodename
 Description : get az name list by node name,
 Input       : nodename -
 Output      : azname
 Return      : azname
 ******************************************************************************
*/
char* get_AZname_by_nodename(const char* nodename)
{
    uint32 nodeidx;
    char* azName = NULL;
    for (nodeidx = 0; nodeidx < g_node_num; nodeidx++) {
        if (strcmp(g_node[nodeidx].nodeName, nodename) == 0) {
            // Assignment by cluster_static_config value
            azName = xstrdup(g_node[nodeidx].azName);
        }
    }
    return azName;
}

/*
 ******************************************************************************
 Function    : get_local_dbpath_by_instancename
 Description : get the instance directory where the instance is located by instance name
 Input       : instancename - the instance name
                 type - The value of the -Z parameter
                 dbpath - Instance of the path
 Output      : None
 Return      : None
 ******************************************************************************
*/
int32 get_local_dbpath_by_instancename(const char* instancename, const int* type, char* dbpath)
{
    uint32 i;
    char local_inst_name[CM_NODE_NAME_LEN] = {0};
    int32 retval;
    errno_t rc = 0;

    if ((*type == INSTANCE_ANY) || (*type == INSTANCE_COORDINATOR)) {
        if ('\0' != g_currentNode->DataPath[0]) {
            retval = get_local_instancename_by_dbpath(g_currentNode->DataPath, local_inst_name);
            if ((retval == CLUSTER_CONFIG_SUCCESS) && (0 == strncmp(local_inst_name, instancename, CM_NODE_NAME_LEN))) {
                rc = memcpy_s(dbpath, CM_PATH_LENGTH, g_currentNode->DataPath, CM_PATH_LENGTH);
                securec_check_c(rc, "\0", "\0");
                return CLUSTER_CONFIG_SUCCESS;
            }
        }
    }

    if ((*type == INSTANCE_ANY) || (*type == INSTANCE_DATANODE)) {
        for (i = 0; i < g_currentNode->datanodeCount; i++) {
            retval =
                get_local_instancename_by_dbpath(g_currentNode->datanode[i].datanodeLocalDataPath, local_inst_name);
            if ((retval == CLUSTER_CONFIG_SUCCESS) && (0 == strncmp(local_inst_name, instancename, CM_NODE_NAME_LEN))) {
                rc = memcpy_s(dbpath, CM_PATH_LENGTH, g_currentNode->datanode[i].datanodeLocalDataPath, CM_PATH_LENGTH);
                securec_check_c(rc, "\0", "\0");
                return CLUSTER_CONFIG_SUCCESS;
            }
        }
    }

    if ((*type == INSTANCE_ANY) || (*type == INSTANCE_GTM)) {
        retval = get_local_gtm_name(local_inst_name);
        if ((retval == CLUSTER_CONFIG_SUCCESS) && (0 == strncmp(local_inst_name, instancename, CM_NODE_NAME_LEN))) {
            rc = memcpy_s(dbpath, CM_PATH_LENGTH, g_currentNode->gtmLocalDataPath, CM_PATH_LENGTH);
            securec_check_c(rc, "\0", "\0");
            return CLUSTER_CONFIG_SUCCESS;
        }
    }

    return CLUSTER_CONFIG_ERROR;
}

/*
 ******************************************************************************
 Function    : get_local_instancename_by_dbpath
 Description :
 Input       : dbpath - the data path of instance
                 instancename - the instance name, such as: dn_6002_6003
 Output      : None
 Return      : None
 ******************************************************************************
*/
int32 get_local_instancename_by_dbpath(const char* dbpath, char* instancename)
{
    char name[MAX_VALUE_LEN] = "";
    int retval;
    char pg_config_file[MAXPGPATH] = {0};
    int nRet;
    errno_t rc;

    nRet = snprintf_s(pg_config_file, MAXPGPATH, MAXPGPATH - 1, "%s/postgresql.conf", dbpath);
    securec_check_ss_c(nRet, "\0", "\0");

    retval = get_value_in_config_file(pg_config_file, "pgxc_node_name", name);
    if (0 == retval) {
        rc = strncpy_s(instancename, CM_NODE_NAME_LEN, name, CM_NODE_NAME_LEN - 1);
        securec_check_c(rc, "\0", "\0");
        instancename[CM_NODE_NAME_LEN - 1] = '\0';
        return CLUSTER_CONFIG_SUCCESS;
    }

    instancename[0] = '\0';

    return CLUSTER_CONFIG_ERROR;
}

/*
 ******************************************************************************
 Function    : get_local_gtm_name
 Description : get the name of gtm from the configuration file---gtm.conf.
                    Find the parameter "nodename" in the file "gtm.conf" to get its corresponding parameter value
                    name---"one"
 Input       : instancename -the instance name
 Output      : None
 Return      : None
 ******************************************************************************
*/
int32 get_local_gtm_name(char* instancename)
{
    char name[MAX_VALUE_LEN] = "";
    int retval;
    char pg_config_file[MAXPGPATH] = {0};
    int nRet;
    errno_t rc;

    if (g_currentNode->gtmId == 0) {
        return CLUSTER_CONFIG_ERROR;
    }

    nRet = snprintf_s(pg_config_file, MAXPGPATH, MAXPGPATH - 1, "%s/gtm.conf", g_currentNode->gtmLocalDataPath);
    securec_check_ss_c(nRet, "\0", "\0");

    /* Retrieves the parameter values for the specified parameters from the configuration file */
    retval = get_value_in_config_file(pg_config_file, "nodename", name);
    if (0 == retval) {
        rc = strncpy_s(instancename, CM_NODE_NAME_LEN, name, CM_NODE_NAME_LEN - 1);
        securec_check_c(rc, "\0", "\0");
        return CLUSTER_CONFIG_SUCCESS;
    }

    return CLUSTER_CONFIG_ERROR;
}

/*
 ******************************************************************************
 Function    : init_gauss_cluster_config
 Description : Obtain cluster information from cluster_static_config
 Input       :  None
 Output      : void
 Return      : None
 ******************************************************************************
*/
int init_gauss_cluster_config(void)
{
    char path[MAXPGPATH] = {0};
    char gausshome[MAXPGPATH] = {0};
    int err_no = 0;
    int nRet = 0;
    int status = 0;
    uint32 nodeidx = 0;
    struct stat statbuf {};

    static bool is_init = false;
    if (is_init) {
        return 0;
    }
    is_init = true;
    g_dn_replication_num = 0;

    if (!get_env_value("GAUSSHOME", gausshome, sizeof(gausshome) / sizeof(char)))
        return 1;

    check_env_value(gausshome);
    if (NULL != g_lcname) {
        nRet = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/bin/%s.%s", gausshome, g_lcname, STATIC_CONFIG_FILE);
    } else {
        nRet = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/bin/%s", gausshome, STATIC_CONFIG_FILE);
    }
    securec_check_ss_c(nRet, "\0", "\0");

    if (checkPath(path) != 0) {
        write_stderr(_("realpath(%s) failed : %s!\n"), path, strerror(errno));
        return 1;
    }

    if (lstat(path, &statbuf) != 0) {
        write_stderr("ERROR: could not stat file \"%s\": %s\n", path, strerror(errno));
        return 1;
    }

    if (NULL != g_lcname) {
        status = read_lc_config_file(path, &err_no);
    } else {
        status = read_config_file(path, &err_no);
    }
    if (0 != status) {
        switch (status) {
            case OPEN_FILE_ERROR: {
                write_stderr("ERROR: The cluster_staic_config file is not generated or is manually deleted.\n");
                return 1;
            }
            case READ_FILE_ERROR: {
                write_stderr("ERROR: The cluster_staic_config file permission is insufficient.\n");
                return 1;
            }
            case OUT_OF_MEMORY: {
                write_stderr("ERROR: The cluster_staic_config open failed cause out of memeory.\n");
                return 1;
            }
            default:
                break;
        }
        write_stderr("ERROR: Invalid return value from read_config_file\n");
        return 1;
    }

    if (g_nodeHeader.node == 0) {
        write_stderr("ERROR: Invalid cluster_staic_config file,"
                     " curerent node id is:%d .\n",
            (int32)g_nodeHeader.node);
        GS_FREE(g_node);
        return 1;
    }

    for (nodeidx = 0; nodeidx < g_node_num; nodeidx++) {
        if (g_node[nodeidx].node == g_nodeHeader.node) {
            g_currentNode = &g_node[nodeidx];
        }
    }

    if (NULL == g_currentNode) {
        write_stderr("ERROR: failed to find current node by nodeid, curerent node id is:%d .\n", (int32)g_nodeHeader.node);
        GS_FREE(g_node);
        return 1;
    }

    if (get_dynamic_dn_role() != 0) {
        write_stderr("ERROR: failed to get dynamic dn role.\n");
        GS_FREE(g_node);
        return 1;
    }

    return 0;
}

/*
 * @@GaussDB@@
 * Brief            : save_guc_para_info()
 * Description   : get parameter of CN/DN/CMSERVER/CMAGENT from cluster_guc.conf file
 * Notes          : if it cann't open file, return NULL
 * Input        : the path of cluster_guc.conf file
 * Output        : the config parameter list of CN/DN/CMSERVER/CMAGENT
 */
int save_guc_para_info()
{
    int rc = 0;
    errno_t ret;
    FILE* fp = NULL;
    char line_info[MAXPGPATH] = {0};
    char temp_line_info[MAXPGPATH] = {0};
    char* get_result = NULL;
    char* outer_ptr = NULL;
    GUC_Node_Type type = GUC_NONE;
    char gausshome[MAXPGPATH] = {0};
    char guc_file[MAXPGPATH] = {0};

    rc = memset_s(line_info, MAXPGPATH, 0, MAXPGPATH);
    securec_check_c(rc, "\0", "\0");
    rc = memset_s(temp_line_info, MAXPGPATH, 0, MAXPGPATH);
    securec_check_c(rc, "\0", "\0");
    if (!get_env_value("GAUSSHOME", gausshome, sizeof(gausshome) / sizeof(char)))
        return FAILURE;

    check_env_value(gausshome);
    rc = snprintf_s(guc_file, MAXPGPATH, MAXPGPATH - 1, "%s/bin/%s", gausshome, GUC_OPT_CONF_FILE);
    securec_check_ss_c(rc, "\0", "\0");

    if (checkPath(guc_file) != 0) {
        write_stderr(_("realpath(%s) failed : %s!\n"), guc_file, strerror(errno));
        return FAILURE;
    }
    /* maybe fail because of privilege */
    fp = fopen(guc_file, "r");
    if (fp == NULL) {
        write_stderr("ERROR: Failed to open file\"%s\"\n", guc_file);
        return FAILURE;
    }
    if (NULL == fgets(line_info, MAXPGPATH - 1, fp)) {
        write_stderr("ERROR: Failed to read file\"%s\"\n", guc_file);
        fclose(fp);
        return FAILURE;
    }

    while ((fgets(line_info, MAXPGPATH - 1, fp)) != NULL) {
        if ((int)strlen(line_info) > 0)
            line_info[(int)strlen(line_info) - 1] = '\0';
        else
            continue;

        if (line_info[0] == '#') {
            continue;
        } else if (strncmp(line_info, "[coordinator/datanode]", sizeof("[coordinator/datanode]")) == 0) {
            type = GUC_CNDN;
            continue;
        } else if (strncmp(line_info, "[gtm]", sizeof("[gtm]")) == 0) {
            type = GUC_GTM;
            continue;
        } else if (strncmp(line_info, "[cmserver]", sizeof("[cmserver]")) == 0) {
            type = GUC_CMSERVER;
            continue;
        } else if (strncmp(line_info, "[cmagent]", sizeof("[cmagent]")) == 0) {
            type = GUC_CMAGENT;
            continue;
        } else if (strncmp(line_info, "[lcname]", sizeof("[lcname]")) == 0) {
            type = GUC_LCNAME;
            continue;
        }  else if (strncmp(line_info, "[end]", sizeof("[end]")) == 0) {
            break;
        }

        ret = strcpy_s(temp_line_info, sizeof(temp_line_info), line_info);
        securec_check_c(ret, "\0", "\0");
        get_result = strtok_r(line_info, "|", &outer_ptr);
        if (NULL == get_result) {
            write_stderr("ERROR: Line information is incorrect\n");
            fclose(fp);
            return FAILURE;
        }

        switch (type) {
            case GUC_CNDN:
                cndn_param[cndn_param_number] = xstrdup(get_result);
                cndn_guc_info[cndn_param_number] = xstrdup(temp_line_info);
                cndn_param_number++;
                break;
            case GUC_GTM:
                gtm_param[gtm_param_number] = xstrdup(get_result);
                gtm_guc_info[gtm_param_number] = xstrdup(temp_line_info);
                gtm_param_number++;
                break;
            case GUC_CMSERVER:
                cmserver_param[cmserver_param_number] = xstrdup(get_result);
                cmserver_guc_info[cmserver_param_number] = xstrdup(temp_line_info);
                cmserver_param_number++;
                break;
            case GUC_CMAGENT:
                cmagent_param[cmagent_param_number] = xstrdup(get_result);
                cmagent_guc_info[cmagent_param_number] = xstrdup(temp_line_info);
                cmagent_param_number++;
                break;
            case GUC_LCNAME:
                lc_param[lc_param_number] = xstrdup(get_result);
                lc_guc_info[lc_param_number] = xstrdup(temp_line_info);
                lc_param_number++;
                break;
            default:
                fclose(fp);
                return FAILURE;
        }
    }

    fclose(fp);
    return SUCCESS;
}

/*
 * @@GaussDB@@
 * Brief            : readfile(const char* path, int reserve_num_lines)
 * Description      : get value from directory
 * Notes            : if it cann't open file, return NULL
 */
char** readfile(const char* path, int reserve_num_lines)
{
    int fd;
    int nlines = 0;
    char** result = NULL;
    char* buffer = NULL;
    char* linebegin = NULL;
    int i = 0;
    int n = 0;
    int len = 0;
    struct stat statbuf {};
    errno_t rc = 0;

    /*
     * Slurp the file into memory.
     *
     * The file can change concurrently,
     * so we read the whole file into memory
     * with a single read() call. That's not
     * guaranteed to get an atomic
     * snapshot, but in practice, for a
     * small file, it's close enough for the
     * current use.
     */
    fd = open(path, O_RDONLY | PG_BINARY, 0);
    if (fd < 0) {
        return NULL;
    }
    if (fstat(fd, &statbuf) < 0) {
        close(fd);
        return NULL;
    }
    if (statbuf.st_size == 0) {
        /* empty file */
        close(fd);
        result = (char**)malloc((1 + reserve_num_lines) * sizeof(char*));
        if (NULL == result) {
            write_stderr("ERROR: Memory allocation failed.\n");
            return NULL;
        }

        for (i = 0; i < reserve_num_lines + 1; i++) {
            result[i] = NULL;
        }

        *result = NULL;
        return result;
    }

    if (statbuf.st_size > LONG_MAX - 1) {
        write_stderr("malloc size too big, size (%ld).\n", statbuf.st_size);
        close(fd);
        return NULL;
    }

    buffer = (char*)malloc((size_t)(statbuf.st_size + 1));
    if (NULL == buffer) {
        close(fd);
        write_stderr("ERROR: Memory allocation failed.\n");
        return NULL;
    }

    len = read(fd, buffer, statbuf.st_size + 1);
    close(fd);
    if (len != statbuf.st_size) {
        /* oops, the file size changed between fstat and read */
        write_stderr("ERROR: File is buzy read failed.\n");
        GS_FREE(buffer);
        return NULL;
    }

    /*
     * Count newlines. We expect there to be a newline after each full line,
     * including one at the end of file. If there isn't a newline at the end,
     * any characters after the last newline will be ignored.
     */
    nlines = 0;
    for (i = 0; i < len; i++) {
        if (buffer[i] == '\n') {
            nlines++;
        }
    }

    /* set up the result buffer */
    result = (char**)malloc((nlines + 1 + reserve_num_lines) * sizeof(char*));
    if (NULL == result) {
        GS_FREE(buffer);
        write_stderr("ERROR: Memory allocation failed.\n");
        return NULL;
    }

    /* now split the buffer into lines */
    linebegin = buffer;
    n = 0;
    for (i = 0; i < len; i++) {
        if (buffer[i] == '\n') {
            int slen = &buffer[i] - linebegin + 1;
            char* linebuf = (char*)malloc(slen + 1);
            if (NULL == linebuf) {
                write_stderr("ERROR: Memory allocation failed.\n");
                for (i = 0; i < n; i++) {
                    GS_FREE(result[i]);
                }
                GS_FREE(result);
                GS_FREE(buffer);
                return NULL;
            }
            rc = memcpy_s(linebuf, slen, linebegin, slen);
            securec_check_c(rc, "\0", "\0");
            linebuf[slen] = '\0';
            result[n++] = linebuf;
            linebegin = &buffer[i + 1];
        }
    }
    result[n] = NULL;

    for (i = 0; i < reserve_num_lines; i++) {
        result[n + i] = NULL;
    }

    GS_FREE(buffer);

    return result;
}

/*******************************************************************************
 Function    : get_value_in_config_file
 Description :
 Input       : pg_config_file -  configuration file, such as: postgresql.conf/gtm.conf
                 parameter_in_config - The name of the parameter in the configuration file, such as: dn_6002_6003
                 para_value -  the value of the parameter in the configuration file
 Output      : None
 Return      : None
*******************************************************************************/
int get_value_in_config_file(const char* pg_config_file, const char* parameter_in_config, char* para_value)
{
    int values_offset = 0;
    int values_len = 0;
    int values_line = 0;
    char** all_lines = NULL;
    int rc = 0;

    all_lines = readfile(pg_config_file, 0);
    if (NULL == all_lines) {
        return 1;
    }
    values_line =
        find_gucoption_available((const char**)all_lines, parameter_in_config, NULL, NULL, &values_offset, &values_len);

    if (values_line != INVALID_LINES_IDX) {
        rc = strncpy_s(para_value,
            MAX_VALUE_LEN,
            all_lines[values_line] + values_offset + 1,
            (size_t)Min(values_len - 2, MAX_VALUE_LEN - 1));
        securec_check_c(rc, "\0", "\0");
    }

    freefile(all_lines);

    return (values_line == INVALID_LINES_IDX);
}

/*******************************************************************************
 Function    : find_gucoption_available
 Description :
 Input       : optlines -
               opt_name -
               name_offset -
               name_len -
               value_offset -
               value_len -
 Output      : None
 Return      : None
*******************************************************************************/
int find_gucoption_available(
    const char** optlines, const char* opt_name, int* name_offset, int* name_len, int* value_offset, int* value_len)
{
    char* p = NULL;
    char* q = NULL;
    char* tmp = NULL;
    int i = 0;
    size_t paramlen = 0;

    if (NULL == optlines || NULL == opt_name) {
        return INVALID_LINES_IDX;
    }
    paramlen = (size_t)strnlen(opt_name, MAX_PARAM_LEN);
    if (name_len != NULL) {
        *name_len = (int)paramlen;
    }
    for (i = 0; optlines[i] != NULL; i++) {
        p = (char*)optlines[i];
        while (isspace((unsigned char)*p)) {
            p++;
        }
        if (strncmp(p, opt_name, paramlen) != 0) {
            continue;
        }
        if (name_offset != NULL) {
            *name_offset = p - optlines[i];
        }
        p += paramlen;
        while (isspace((unsigned char)*p)) {
            p++;
        }
        if (*p != '=') {
            continue;
        }
        p++;
        while (isspace((unsigned char)*p)) {
            p++;
        }
        q = p;
        while (*q && !(*q == '\n' || *q == '#')) {
            if (!isspace((unsigned char)*q)) {
                tmp = ++q;
            } else {
                q++;
            }
        }
        if (value_offset != NULL) {
            *value_offset = p - optlines[i];
        }
        if (value_len != NULL) {
            *value_len = (NULL == tmp) ? 0 : (tmp - p);
        }
        return i;
    }

    return INVALID_LINES_IDX;
}

/*******************************************************************************
 Function    : freefile
 Description :
 Input       : lines -
 Output      : None
 Return      : None
*******************************************************************************/
void freefile(char** lines)
{
    char** line = NULL;
    if (NULL == lines) {
        return;
    }
    line = lines;
    while (*line != NULL) {
        free(*line);
        *line = NULL;
        line++;
    }
    free(lines);
    lines = NULL;
}

#ifdef __cplusplus
}
#endif /* __cplusplus */
