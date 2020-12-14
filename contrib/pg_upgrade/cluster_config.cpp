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
 * cluster_config.cpp
 *
 *
 *
 *
 * IDENTIFICATION
 *        contrib/pg_upgrade/cluster_config.cpp
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
#include "postgres.h"
#include "knl/knl_variable.h"
#include "common/config/cm_config.h"
#include "bin/elog.h"

#define STATIC_CONFIG_FILE "cluster_static_config"

#define MAX_VALUE_LEN (1024)
#define MAX_PARAM_LEN (1024)
#define CLUSTER_CONFIG_SUCCESS (0)
#define CLUSTER_CONFIG_ERROR (1)
#define CM_NODE_NAME_LEN 64

#ifndef GS_COLLECTOR_BUILD
extern void write_stderr(const char* fmt, ...)
    /* This extension allows gcc to check the format string for consistency with
       the supplied arguments. */
    __attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));
#else
#define write_stderr printf
#endif

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

int node_num = 0;

/* current node id index */
uint32 g_nodeId;

/* BEGIN: PDK support for node type */
typedef enum {
    INSTANCE_ANY,
    INSTANCE_DATANODE,    /* postgresql.conf */
    INSTANCE_COORDINATOR, /* postgresql.conf */
    INSTANCE_GTM,         /* gtm.conf        */
    INSTANCE_GTM_PROXY,   /* gtm_proxy.conf  */
} NodeType;
/* END: PDK support for node type */

#define INVALID_LINES_IDX -1

int32 get_local_instancename_by_dbpath(char* dbpath, char* instancename);
int32 get_local_gtm_name(char* instancename);
int32 get_local_gtm_proxy_name(char* instancename);
int get_value_in_config_file(char* pg_config_file, char* parameter_in_config, char* para_value);
int find_gucoption_available(
    char** optlines, const char* opt_name, int* name_offset, int* name_len, int* value_offset, int* value_len);
char** readfile(const char* path, int reserve_num_lines);

void freefile(char** lines);

uint32 get_local_num_datanode()
{
    return g_currentNode->datanodeCount;
}

uint32 get_num_nodes()
{
    return g_node_num;
}
bool is_local_nodeid(uint32 nodeid)
{
    return (g_currentNode->node == nodeid);
}

#ifdef GS_COLLECTOR_BUILD
staticNodeConfig* get_node_nodename(char* name);

staticNodeConfig* get_node_nodename(char* nodename)
{
    uint32 nodeidx;
    for (nodeidx = 0; nodeidx < g_node_num; nodeidx++) {
        if (0 == strncmp(g_node[nodeidx].nodeName, nodename, CM_NODE_NAME_LEN)) {
            return &g_node[nodeidx];
        }
    }

    return NULL;
}

uint32 get_cluster_datanode_num()
{
    uint32 nodeidx;
    uint32 datanodeidx;
    uint32 datanode_num = 0;
    for (nodeidx = 0; nodeidx < g_node_num; nodeidx++) {
        for (datanodeidx = 0; datanodeidx < g_node[nodeidx].datanodeCount; datanodeidx++) {
            if (PRIMARY_DN == g_node[nodeidx].datanode[datanodeidx].datanodeRole) {
                datanode_num++;
            }
        }
    }
    return datanode_num;
}

#endif

bool is_local_node(char* nodename)
{
    return (0 == strncmp(g_currentNode->nodeName, nodename, CM_NODE_NAME_LEN));
}

char* getnodename(uint32 nodeidx)
{
    return g_node[nodeidx].nodeName;
}

int32 get_local_dbpath_by_instancename(char* instancename, int* type, char* dbpath, uint32* pinst_slot_id)
{
    uint32 i;
    char local_inst_name[CM_NODE_NAME_LEN] = {0};
    int32 retval;
    errno_t ret = 0;

    if (pinst_slot_id) {
        *pinst_slot_id = 0;
    }

    if ((*type == INSTANCE_ANY) || (*type == INSTANCE_COORDINATOR)) {

        if ('\0' != g_currentNode->DataPath[0]) {
            retval = get_local_instancename_by_dbpath(g_currentNode->DataPath, local_inst_name);
            if ((retval == CLUSTER_CONFIG_SUCCESS) && (0 == strncmp(local_inst_name, instancename, CM_NODE_NAME_LEN))) {
                ret = memcpy_s(dbpath, CM_PATH_LENGTH, g_currentNode->DataPath, CM_PATH_LENGTH);
                securec_check_c(ret, "\0", "\0");
                return CLUSTER_CONFIG_SUCCESS;
            }
        }
    }

    if ((*type == INSTANCE_ANY) || (*type == INSTANCE_DATANODE)) {
        for (i = 0; i < g_currentNode->datanodeCount; i++) {
            retval =
                get_local_instancename_by_dbpath(g_currentNode->datanode[i].datanodeLocalDataPath, local_inst_name);
            if ((retval == CLUSTER_CONFIG_SUCCESS) && (0 == strncmp(local_inst_name, instancename, CM_NODE_NAME_LEN))) {
                if (pinst_slot_id) {
                    *pinst_slot_id = i;
                }
                ret =
                    memcpy_s(dbpath, CM_PATH_LENGTH, g_currentNode->datanode[i].datanodeLocalDataPath, CM_PATH_LENGTH);
                securec_check_c(ret, "\0", "\0");
                return CLUSTER_CONFIG_SUCCESS;
            }
        }
    }

    if ((*type == INSTANCE_ANY) || (*type == INSTANCE_GTM)) {
        retval = get_local_gtm_name(local_inst_name);
        if ((retval == CLUSTER_CONFIG_SUCCESS) && (0 == strncmp(local_inst_name, instancename, CM_NODE_NAME_LEN))) {
            ret = memcpy_s(dbpath, CM_PATH_LENGTH, g_currentNode->gtmLocalDataPath, CM_PATH_LENGTH);
            securec_check_c(ret, "\0", "\0");
            return CLUSTER_CONFIG_SUCCESS;
        }
    }

    if ((*type == INSTANCE_ANY) || (*type == INSTANCE_GTM_PROXY)) {
        retval = get_local_gtm_proxy_name(local_inst_name);
        if ((retval == CLUSTER_CONFIG_SUCCESS) && (0 == strncmp(local_inst_name, instancename, CM_NODE_NAME_LEN))) {
            ret = memcpy_s(dbpath, CM_PATH_LENGTH, g_currentNode->gtmLocalDataPath, CM_PATH_LENGTH);
            securec_check_c(ret, "\0", "\0");
            return CLUSTER_CONFIG_SUCCESS;
        }
    }

    return CLUSTER_CONFIG_ERROR;
}

int32 get_local_instancename_by_dbpath(char* dbpath, char* instancename)
{
    char name[MAX_VALUE_LEN] = "";
    int retval;
    char pg_config_file[MAXPGPATH];
    int nRet = 0;
    errno_t rc = 0;

    nRet = snprintf_s(pg_config_file, MAXPGPATH, MAXPGPATH - 1, "%s/postgresql.conf", dbpath);
    securec_check_ss_c(nRet, "\0", "\0");

    retval = get_value_in_config_file(pg_config_file, "pgxc_node_name", name);
    if (0 == retval) {
        rc = strncpy_s(instancename, CM_NODE_NAME_LEN, name, CM_NODE_NAME_LEN);
        securec_check_c(rc, "\0", "\0");
        instancename[CM_NODE_NAME_LEN - 1] = '\0';
        return CLUSTER_CONFIG_SUCCESS;
    }

    instancename[0] = '\0';

    return CLUSTER_CONFIG_ERROR;
}

int32 get_local_gtm_name(char* instancename)
{
    char name[MAX_VALUE_LEN] = "";
    int retval;
    char pg_config_file[MAXPGPATH];
    int nRet = 0;
    errno_t rc = 0;

    if (g_currentNode->gtmId == 0) {
        return CLUSTER_CONFIG_ERROR;
    }

    nRet = snprintf_s(pg_config_file, MAXPGPATH, MAXPGPATH - 1, "%s/gtm.conf", g_currentNode->gtmLocalDataPath);
    securec_check_ss_c(nRet, "\0", "\0");

    retval = get_value_in_config_file(pg_config_file, "nodename", name);
    if (0 == retval) {
        rc = strncpy_s(instancename, CM_NODE_NAME_LEN, name, CM_NODE_NAME_LEN);
        securec_check_c(rc, "\0", "\0");
        return CLUSTER_CONFIG_SUCCESS;
    }

    return CLUSTER_CONFIG_ERROR;
}

void get_local_gtm_dbpath(char* dbpath)
{
    errno_t ret = 0;
    ret = memcpy_s(dbpath, CM_PATH_LENGTH, g_currentNode->gtmLocalDataPath, CM_PATH_LENGTH);
    securec_check_c(ret, "\0", "\0");
}

void get_local_gtmproxy_dbpath(char* dbpath)
{
    errno_t ret = 0;
    ret = memcpy_s(dbpath, CM_PATH_LENGTH, g_currentNode->gtmLocalDataPath, CM_PATH_LENGTH);
    securec_check_c(ret, "\0", "\0");
}

int32 get_local_gtm_proxy_name(char* instancename)
{
    char name[MAX_VALUE_LEN] = "";
    int retval;
    char pg_config_file[MAXPGPATH];
    int nRet = 0;
    errno_t rc = 0;

    if (g_currentNode->gtmProxyId == 0) {
        return CLUSTER_CONFIG_ERROR;
    }

    nRet = snprintf_s(pg_config_file, MAXPGPATH, MAXPGPATH - 1, "%s/gtm_proxy.conf", g_currentNode->gtmLocalDataPath);
    securec_check_ss_c(nRet, "\0", "\0");

    retval = get_value_in_config_file(pg_config_file, "nodename", name);
    if (0 == retval) {
        rc = strncpy_s(instancename, CM_NODE_NAME_LEN, name, CM_NODE_NAME_LEN);
        securec_check_c(rc, "\0", "\0");
        return CLUSTER_CONFIG_SUCCESS;
    }

    return CLUSTER_CONFIG_ERROR;
}

void get_local_cordinator_dbpath(char* dbpath)
{
    errno_t ret = 0;
    ret = memcpy_s(dbpath, CM_PATH_LENGTH, g_currentNode->DataPath, CM_PATH_LENGTH);
    securec_check_c(ret, "\0", "\0");
}

int32 get_local_datanode_dbpath(uint32 slot, char* dbpath)
{
    errno_t ret = 0;

    if ((slot < 0) && (slot >= g_currentNode->datanodeCount)) {
        return CLUSTER_CONFIG_ERROR;
    }

    ret = memcpy_s(dbpath, CM_PATH_LENGTH, g_currentNode->datanode[slot].datanodeLocalDataPath, CM_PATH_LENGTH);
    securec_check_c(ret, "\0", "\0");

    return CLUSTER_CONFIG_SUCCESS;
}

int init_gauss_cluster_config(char* gaussbinpath)
{
    char path[MAXPGPATH];
    char* gausshome = NULL;
    int err_no = 0;
    int nRet = 0;
    uint32 nodeidx = 0;

    if (NULL == gaussbinpath) {
        gausshome = getenv("GAUSSHOME");
        if ((NULL == gausshome) || ('\0' == gausshome[0])) {
            write_stderr("ERROR: Get GAUSSHOME environment variable failed.\n");
            return 1;
        }
        nRet = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/bin/%s", gausshome, STATIC_CONFIG_FILE);
    } else {
        nRet = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, "%s/%s", gaussbinpath, STATIC_CONFIG_FILE);
    }
    securec_check_ss_c(nRet, "\0", "\0");

    if (0 != read_config_file(path, &err_no)) {
        write_stderr("Invalid config file\n");
        return 1;
    }

    if (g_nodeHeader.node <= 0) {
        write_stderr("invalid config file ,node:%d .\n", g_nodeHeader.node);
        free(g_node);
        return 1;
    }

    for (nodeidx = 0; nodeidx < g_node_num; nodeidx++) {
        if (g_node[nodeidx].node == g_nodeHeader.node) {
            g_currentNode = &g_node[nodeidx];
            g_nodeId = nodeidx;
        }
    }

    if (NULL == g_currentNode) {
        write_stderr("ERROR: failed to find current node by nodeid, curerent node id is:%d .\n", g_nodeHeader.node);
        free(g_node);
        return 1;
    }

    return 0;
}

/*
 * @@GaussDB@@
 * Brief            : static char ** readfile(const char* path)
 * Description      : 从指定的路径中获得配置文件的数据
 * Notes            : 如果文件不能打开，则返回NULL
 */
char** readfile(const char* path, int reserve_num_lines)
{
    int fd;
    int nlines;
    char** result;
    char* buffer = NULL;
    char* linebegin = NULL;
    int i;
    int n;
    int len;
    struct stat statbuf;
    errno_t ret = 0;

    /*
     * Slurp the file into memory.
     *
     * The file can change concurrently, so we read the whole file into memory
     * with a single read() call. That's not guaranteed to get an atomic
     * snapshot, but in practice, for a small file, it's close enough for the
     * current use.
     */
    fd = open(path, O_RDONLY | PG_BINARY, 0);
    if (fd < 0)
        return NULL;
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
    buffer = (char*)malloc(statbuf.st_size + 1);
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
        free(buffer);
        return NULL;
    }

    /*
     * Count newlines. We expect there to be a newline after each full line,
     * including one at the end of file. If there isn't a newline at the end,
     * any characters after the last newline will be ignored.
     */
    nlines = 0;
    for (i = 0; i < len; i++) {
        if (buffer[i] == '\n')
            nlines++;
    }

    /* set up the result buffer */
    result = (char**)malloc((nlines + 1 + reserve_num_lines) * sizeof(char*));
    if (NULL == result) {
        free(buffer);
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
                    free(result[i]);
                }
                free(result);
                free(buffer);
                return NULL;
            }
            ret = memcpy_s(linebuf, slen, linebegin, slen);
            securec_check_c(ret, "\0", "\0");
            linebuf[slen] = '\0';
            result[n++] = linebuf;
            linebegin = &buffer[i + 1];
        }
    }
    result[n] = NULL;

    for (i = 0; i < reserve_num_lines; i++) {
        result[n + i] = NULL;
    }

    free(buffer);

    return result;
}

int get_value_in_config_file(char* pg_config_file, char* parameter_in_config, char* para_value)
{
    int values_offset = 0;
    int values_len = 0;
    int values_line = 0;
    char** all_lines = NULL;
    errno_t retcode = EOK;
    all_lines = readfile(pg_config_file, 0);
    if (NULL == all_lines) {
        return 1;
    }
    values_line = find_gucoption_available(all_lines, parameter_in_config, NULL, NULL, &values_offset, &values_len);

    if (INVALID_LINES_IDX != values_line) {
        retcode = strncpy_s(para_value,
            MAX_VALUE_LEN,
            all_lines[values_line] + values_offset + 1,
            (size_t)Min(values_len - 2, MAX_VALUE_LEN - 1));
        securec_check_c(retcode, "\0", "\0");
    }

    freefile(all_lines);

    return (INVALID_LINES_IDX == values_line);
}

int find_gucoption_available(
    char** optlines, const char* opt_name, int* name_offset, int* name_len, int* value_offset, int* value_len)
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
    if (name_len) {
        *name_len = (int)paramlen;
    }
    for (i = 0; optlines[i] != NULL; i++) {
        p = optlines[i];
        while (isspace((unsigned char)*p)) {
            p++;
        }
        if (strncmp(p, opt_name, paramlen) != 0) {
            continue;
        }
        if (name_offset)
            *name_offset = p - optlines[i];
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
        if (value_offset)
            *value_offset = p - optlines[i];
        if (value_len)
            *value_len = (NULL == tmp) ? 0 : (tmp - p);
        return i;
    }

    return INVALID_LINES_IDX;
}

void freefile(char** lines)
{
    char** line = NULL;
    if (NULL == lines)
        return;
    line = lines;
    while (*line) {
        free(*line);
        line++;
    }
    free(lines);
}

#ifdef __cplusplus
}
#endif /* __cplusplus */
