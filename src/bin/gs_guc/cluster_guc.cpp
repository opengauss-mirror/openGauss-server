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
 * ---------------------------------------------------------------------------------------
 *
 *  cluster_guc.cpp
 *        Interfaces for analysis manager of PDK tool.
 *
 *        Function List:  execute_guc_command_in_remote_node
 *                        form_commandline_options
 *                        get_instance_type
 *                        process_cluster_guc_option
 *                        validate_cluster_guc_options
 *
 * IDENTIFICATION
 *        src/bin/gs_guc/cluster_guc.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "postgres_fe.h"
#include "libpq/libpq-fe.h"
#include "bin/elog.h"
#include "pg_config.h"
#include "common/config/cm_config.h"
#include <limits.h>
#include <fcntl.h>

const int CLUSTER_CONFIG_SUCCESS = 0;
const int CLUSTER_CONFIG_ERROR = 1;
#define LOOP_COUNT 3
#define DOUBLE_PRECISE 0.000000001
#define MAX_HOST_NAME_LENGTH 255
#define LARGE_INSTANCE_NUM 2
#define CM_NODE_NAME_LEN 64
#define STATIC_CONFIG_FILE "cluster_static_config"
#define SSH_OPTIONS                                                                                                   \
    "-o BatchMode=yes -o TCPKeepAlive=yes -o ServerAliveInterval=15 -o ServerAliveCountMax=4 -o ConnectTimeout=5 -o " \
    "ConnectionAttempts=6"
#define GS_FREE(ptr)            \
    do {                        \
        if (NULL != (ptr)) {    \
            free((char*)(ptr)); \
            ptr = NULL;         \
        }                       \
    } while (0)

#define PROCESS_STATUS(status)                                                  \
    do {                                                                        \
        if (status == OUT_OF_MEMORY) {                                    \
            write_stderr("Failed: out of memory\n");                            \
            exit(1);                                                            \
        }                                                                       \
        if (status == OPEN_FILE_ERROR) {                                  \
            write_stderr("Failed: cannot find the expected data dir\n");        \
            exit(1);                                                            \
        }                                                                       \
    } while (0)

const int GTM_INSTANCE_LEN = 3;  // eg: one
const int CN_INSTANCE_LEN = 7;   // eg: cn_5001
const int DN_INSTANCE_LEN = 12;  // eg: dn_6001_6002

extern char** config_param;
extern char** config_value;
extern int config_param_number;
extern bool is_hba_conf;
extern int node_type_number;

extern bool g_need_changed;
extern char* g_local_instance_path;

typedef struct {
    char** nodename_array;
    char** gucinfo_array;
    char** paramname_array;
    char** paramvalue_array;
    uint32 nodename_num;
    uint32 gucinfo_num;
    uint32 paramname_num;
    uint32 paramvalue_num;
} gucInfo;

typedef struct {
    char** nodename_array;
    uint32 num;
} nodeInfo;

#define MAX_P_READ_BUF 1024
typedef struct tag_pcommand {
    FILE* pfp;
    char readbuf[MAX_P_READ_BUF];
    int cur_buf_loc;
    char* nodename;
    int retvalue;
} PARALLEL_COMMAND_S;

PARALLEL_COMMAND_S* g_parallel_command_cxt = NULL;
static int g_max_commands_parallel = 0;
static int g_cur_commands_parallel = 0;

/* real result */
extern gucInfo* g_real_gucInfo;
/* expect result */
extern gucInfo* g_expect_gucInfo;

extern char gucconf_file[MAXPGPATH];
extern int config_param_number;
extern char** config_param;
extern char** config_value;

extern int config_param_number;
extern int cndn_param_number;
extern int cmserver_param_number;
extern int cmagent_param_number;
extern int gtm_param_number;
extern int lc_param_number;
extern int config_value_number;
extern int node_type_number;
extern int arraysize;
extern char** cndn_param;
extern char** gtm_param;
extern char** cmserver_param;
extern char** cmagent_param;
extern char** lc_param;
extern char** cndn_guc_info;
extern char** cmserver_guc_info;
extern char** cmagent_guc_info;
extern char** gtm_guc_info;
extern char** lc_guc_info;
extern const char* progname;

/* status which perform remote connection */
extern bool g_remote_connection_signal;
/* result which perform remote command */
extern unsigned int g_remote_command_result;

/* storage the name which perform remote connection failed */
extern nodeInfo* g_incorrect_nodeInfo;
/* storage the name which need to ignore */
extern nodeInfo* g_ignore_nodeInfo;

typedef enum {
    NO_COMMAND = 0,
    SET_CONF_COMMAND,
    RELOAD_CONF_COMMAND,
    ENCRYPT_KEY_COMMAND,
    CHECK_CONF_COMMAND
} CtlCommand;
extern CtlCommand ctl_command;

typedef enum {
    INSTANCE_ANY,
    INSTANCE_DATANODE,    /* postgresql.conf */
    INSTANCE_COORDINATOR, /* postgresql.conf */
    INSTANCE_GTM,         /* gtm.conf        */
    INSTANCE_GTM_PROXY,   /* gtm_proxy.conf  */
    INSTANCE_CMAGENT,     /* cm_agent.conf */
    INSTANCE_CMSERVER,    /* cm_server.conf */
} NodeType;

/* transform unit */
const int KB_PER_MB = 1024;
#define KB_PER_GB (1024 * 1024)
const int MB_PER_GB = 1024;
#define MS_PER_S 1000
#define MS_PER_MIN (1000 * 60)
#define MS_PER_H (1000 * 60 * 60)
#define MS_PER_D (1000 * 60 * 60 * 24)
#define S_PER_MIN 60
#define S_PER_H (60 * 60)
#define S_PER_D (60 * 60 * 24)
#define MIN_PER_H 60
#define MIN_PER_D (60 * 24)
#define H_PER_D 24

/* execute result */
#define SUCCESS 0
#define FAILURE 1

#define MAX_LINE_LEN 8192
#define MAX_MESG_LEN 4096
#define MAX_PARAM_LEN 1024
#define MAX_VALUE_LEN 1024
#define MAX_UNIT_LEN 8
#define MAX_INSTANCENAME_LEN 128
#define GUC_OPT_CONF_FILE "cluster_guc.conf"

bool is_disable_log_directory = false;

/*
 type about all guc options
 */
typedef enum { GUC_ERROR = -1, GUC_NAME, GUC_TYPE, GUC_VALUE, GUC_MESG } OptType;

/* type about all guc unit */
/*
 *********************************************
 parameters value support units
 *********************************************
 *    type_name       units_type      numbers
 *********************************************
 *    real            units_d          3
 *    integer         units_kB         26
 *    integer         units_MB         3
 *    integer         units_ms         9
 *    integer         units_s          19
 *    integer         units_min        4
 *    integer         units_d          1
 *********************************************
*/
typedef enum { UNIT_ERROR = -1, UNIT_KB, UNIT_MB, UNIT_GB, UNIT_MS, UNIT_S, UNIT_MIN, UNIT_H, UNIT_D } UnitType;

/* type about all guc parameters */
typedef enum {
    GUC_PARA_ERROR = -1,
    GUC_PARA_BOOL,  /* bool    */
    GUC_PARA_ENUM,  /* enum    */
    GUC_PARA_INT,   /* int     */
    GUC_PARA_INT64, /* int64   */
    GUC_PARA_REAL,  /* real    */
    GUC_PARA_STRING /* string  */
} GucParaType;

struct guc_config_enum_entry {
    char guc_name[MAX_PARAM_LEN];
    GucParaType type;
    char guc_value[MAX_VALUE_LEN];
    char guc_unit[MAX_UNIT_LEN];
    char message[MAX_MESG_LEN];
};

struct guc_minmax_value {
    char min_val_str[MAX_VALUE_LEN];
    char max_val_str[MAX_VALUE_LEN];
};

/* value about bool type */
const char* guc_bool_valuelist[] = {
    "true",
    "false",
    "on",
    "off",
    "yes",
    "no",
    "0",
    "1",
};

/* value type list */
const char *value_type_list[] = {
    "boolean",
    "enum",
    "integer",
    "real",
    "string",
};

/* value about the parameters which unit is 8kB */
const char* unit_eight_kB_parameter_list[] = {
    "backwrite_quantity",
    "effective_cache_size",
    "pca_shared_buffers",
    "prefetch_quantity",
    "segment_size",
    "shared_buffers",
    "temp_buffers",
    "wal_buffers",
    "wal_segment_size",
    "huge_page_size",
    "heap_bulk_read_size",
    "vacuum_bulk_read_size"
};
/* the size of page, unit is kB */
#define PAGE_SIZE 8

int process_guc_command(const char* datadir);
void do_checkvalidate(int type);
void get_instance_configfile(const char* datadir);
char* get_ctl_command_type();
void* pg_malloc(size_t size);
void* pg_malloc_zero(size_t size);
#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

int execute_guc_command_in_remote_node(int idx, char* command);
static char* form_commandline_options(const char* instance_name, const char* indatadir, bool local_mode);
uint32 get_num_nodes();
uint32 get_local_num_datanode();
bool is_local_nodeid(uint32 nodeid);
bool is_local_node(char* nodename);
int32 get_nodeidx_by_name(char* nodename);
int get_local_dbpath_by_instancename(const char* instancename, int* type, char* dbpath);
int init_gauss_cluster_config(void);

extern NodeType nodetype;
char* getnodename(uint32 nodeidx);
bool get_hostname_or_ip(char* out_name, size_t name_len);
int32 get_local_instancename_by_dbpath(char* dbpath, char* instancename);
char* xstrdup(const char* s);
char** readfile(const char* path, int reserve_num_lines);
void freefile(char** lines);
bool get_env_value(const char* env_var, char* output_env_value, size_t env_var_value_len);
int get_all_datanode_num();
int get_all_coordinator_num();
int get_all_cmserver_num();
int get_all_cmagent_num();
int get_all_cndn_num();
int get_all_gtm_num();
char* get_AZ_value(const char* value, const char* data_dir);
char* get_AZname_by_nodename(char* nodename);

void make_string_tolower(const char* source, char* dest, const int destlen);

void save_expect_instance_info(const char* datadir);
void save_remote_instance_info(
    const char* result_file, const char* nodename, char* command, gucInfo* guc_info, bool isRealGucInfo);

void do_local_instance(int type, char* instance_name, char* indatadir);
void do_remote_instance(char* nodename, const char* instance_name, const char* indatadir);
void do_all_nodes_instance(const char* instance_name, const char* indatadir);

void check_env_value(const char* input_env_value);

/* *********************************************************************************** */
GucParaType get_guc_type(const char* type);
UnitType get_guc_unit(const char* unit);
int do_local_para_value_change(int type, char* datadir);
int do_local_guc_command(int type, char* temp_datadir);
char** get_guc_option();
int do_gucopt_parse(const char* guc_opt, struct guc_config_enum_entry& guc_variable_list);
int check_parameter(int type);
int check_parameter_value(
    const char* paraname, GucParaType type, char* guc_list_value, const char* guc_list_unit, const char* value);
int check_parameter_name(char** guc_opt, int type);
bool check_parameter_is_valid(int type);
int parse_value(const char* paraname, const char* value, const char* guc_list_unit, int64* result_int,
    double* result_double, bool isInt);
int get_guc_minmax_value(const char* guc_list_val, struct guc_minmax_value& value_list);
int check_int_real_type_value(
    const char* paraname, const char* guc_list_value, const char* guc_list_unit, const char* value, bool isInt);
int check_enum_type_value(const char* paraname, char* guc_list_value, const char* value);
int check_bool_type_value(const char* value);
int check_string_type_value(const char* paraname, const char* value);

void do_command_for_cn_gtm(int type, char* indatadir, bool isCoordinator);
void do_command_for_dn(int type, char* indatadir);
void do_command_for_cm(int type, char* indatadir);
void do_command_for_cndn(int type, char* indatadir);
char *get_cm_real_path(int type);
void create_tmp_dir(const char* pathdir);
void remove_tmp_dir(const char* pathdir);
bool is_record(int type, char* flag_str);
bool compare_str(char* src_str, char* start_str, char* end_str);
void do_command_with_instance_name_option_local(int type, char* instance_name);
void do_remote_instance_local(char* nodename, const char* instance_name, const char* indatadir);
void do_all_nodes_instance_local(const char* instance_name, const char* indatadir);
void do_all_nodes_instance_local_in_serial(const char* instance_name, const char* indatadir);
void do_all_nodes_instance_local_in_parallel(const char* instance_name, const char* indatadir);
char** get_guc_line_info(const char** line);
static char* GetEnvStr(const char* env);
static void executePopenCommandsParallel(const char* cmd, int idx, bool is_local_node);
static void readPopenOutputParallel(const char* cmd, bool if_for_all_instance);
static void SleepInMilliSec(uint32_t sleepMs);
static void init_global_command();
static void reset_global_command();
static void do_all_nodes_instance_local_in_parallel_loop(const char* instance_name, const char* indatadir);
/*******************************************************************************
 Function    : xstrdup
 Description :
 Input       :
 Output      : src string
 Return      : dest string
 *****************************************************************************
*/
char* xstrdup(const char* s)
{
    char* result = NULL;

    result = strdup(s);
    if (NULL == result) {
        (void)write_stderr(_("%s: out of memory\n"), "gs_guc");
        exit(1);
    }
    return result;
}
/*
 ******************************************************************************
 Function    : make_string_tolower
 Description : copy source to dest, and make all alpha about dest to lower.
 Input       : source  -- source string
               dest    -- dest string
 Output      : void
 Return      : void
 *****************************************************************************
*/
void make_string_tolower(const char* source, char* dest, const int destlen)
{
    int i = 0;
    int len = (int)strlen(source);
    if (len > destlen) {
        len = destlen;
    }
    for (i = 0; i < len; i++)
        dest[i] = tolower(source[i]);
    dest[i] = '\0';
}
/*
 ******************************************************************************
 Function    : get_instance_type
 Description :
 Input       :
 Output      : None
 Return      : None
 ******************************************************************************
*/
const char* get_instance_type()
{
    char* type = NULL;
    switch (nodetype) {
        case INSTANCE_COORDINATOR: {
            type = "-Z coordinator";
            break;
        }
        case INSTANCE_DATANODE: {
#ifdef ENABLE_MULTIPLE_NODES
            type = "-Z datanode";
#else
            type = "";
#endif
            break;
        }
        case INSTANCE_CMSERVER: {
            type = "-Z cmserver";
            break;
        }
        case INSTANCE_CMAGENT: {
            type = "-Z cmagent";
            break;
        }
        case INSTANCE_GTM: {
            type = "-Z gtm";
            break;
        }
        default: {
            type = "";
            break;
        }
    }
    return (const char*)type;
}

/*
 ******************************************************************************
 Function    : modify_parameter_value
 Description : If parameter value have the special character '$', when do remote setting
               we should changed the parameter value first.
 Input       : value         parameter value
             : localMode     do it on local node
 Return      : char *
 Warning     : this function will malloc a buffer for returned value, and won't free in this function.
               so the caller should free this buffer after use this function's returned value.
 ******************************************************************************
*/
char* modify_parameter_value(const char* value, bool localMode)
{
    int i = 0;
    int j = 0;
    int k = 0;
    int backslash_num = 0;
    const int local_backslash_num = 1;
    const int remote_backslash_num = 3;

    char* buffer = (char*)pg_malloc_zero(MAX_VALUE_LEN * sizeof(char));

    for (i = 0, j = 0; i < (int)strlen(value) && j < MAX_VALUE_LEN; i++, j++) {
        if (value[i] == '$') {
            /*
             *    If value have the special character '$', adding backslash before '$' is different between local
             * command and remote command. when do remote setting, the commands like this: remote command: ssh -n
             * nodename "gs_guc set -Z datanode -I all -c \"dynamic_library_path='\\\$libdir/xxx'\"" local  command:
             * gs_guc set -Z datanode -I all -c \"dynamic_library_path='\$libdir/xxx'\""
             */
            backslash_num = localMode ? local_backslash_num : remote_backslash_num;
            for (k = 0; k < backslash_num && j < MAX_VALUE_LEN; k++) {
                buffer[j] = '\\';
                j++;
            }
            if (j >= MAX_VALUE_LEN) {
                write_stderr(_("%s: out of memory\n"), progname);
                exit(1);
            }
            buffer[j] = value[i];
        } else {
            buffer[j] = value[i];
        }
    }
    return buffer;
}

/*
 ******************************************************************************
 Function    : form_commandline_options
 Description : Generate the complete guc command
 Input       : instance_name - the instance name
                 indatadir - the path of instance
                 local_mode -  local mode or not
 Output      : None
 Return      : None
 ******************************************************************************
*/
static char* form_commandline_options(const char* instance_name, const char* indatadir, bool local_mode)
{
    char* buffer = NULL;
    int buflen = 0;
    int curlen = 0;
    int i = 0;
    int nRet = 0;
    /* a variable that storage new parameter value*/
    char* new_value = NULL;

    /* other standard options */
#define MIN_COMMAND_LEN 256

    /* adding -c + '=' + two ' ' +  two '\' + two '"' */
#define ALLIG_POSTGRES_CONF_LEN 20

    /* adding -h + two '\' + two '"'  */
#define ALLIG_HBA_CONF_LEN 10

    /* -N options is not required */
    buflen = MIN_COMMAND_LEN;
    if (instance_name != NULL) {
        buflen += strlen(instance_name);
    } else {
        buflen += strlen(indatadir);
    }

    /* find length required for options */
    for (i = 0; i < config_param_number; i++) {
        if (!is_hba_conf) {
            buflen += (int)(ALLIG_POSTGRES_CONF_LEN + strlen(config_param[i]));
            if (config_value[i] != NULL) {
                buflen += strlen(config_value[i]);
            }
        } else {
            buflen += ALLIG_HBA_CONF_LEN;
            if (config_value[i] != NULL) {
                buflen += strlen(config_value[i]);
            }
        }
    }

    buffer = (char*)pg_malloc_zero(buflen);

    /* SET / RESET [--cordinator --datanode --gtm  ] */
    curlen = snprintf_s(
        buffer, buflen, buflen - 1, "gs_guc %s %s ", get_ctl_command_type(), get_instance_type());

    securec_check_ss_c(curlen, buffer, "\0");
    if (nodetype == INSTANCE_CMAGENT ||
        nodetype == INSTANCE_CMSERVER) {
        nRet = snprintf_s(buffer + curlen, (buflen - curlen), (buflen - curlen - 1), "-D \"cm_instance_data_path\"");
    } else {
        /* -I or -D */
        if (NULL != instance_name) {
            nRet = snprintf_s(buffer + curlen , (buflen - curlen), (buflen - curlen - 1), "-I %s",
                              instance_name);
        } else {
            nRet = snprintf_s(buffer + curlen, (buflen - curlen), (buflen - curlen - 1), "-D \"%s\"",
                              indatadir);
        }
    }
    securec_check_ss_c(nRet, buffer, "\0");
    curlen = curlen + nRet;
    /* -c options */
    for (i = 0; i < config_param_number; i++) {
        if (!is_hba_conf) {
            /* The parameter name does not has special character '$'.
             * So We need to give attention to the parameter value.
             */
            if (config_value[i] != NULL) {
                new_value = modify_parameter_value(config_value[i], local_mode);
                if (local_mode) {
                    nRet = snprintf_s(buffer + curlen,
                        (buflen - curlen),
                        (buflen - curlen - 1),
                        " -c %c%s=%s%c",
                        '"',
                        config_param[i],
                        new_value,
                        '"');
                } else {
                    nRet = snprintf_s(buffer + curlen,
                        (buflen - curlen),
                        (buflen - curlen - 1),
                        " -c \\\"%s=%s\\\"",
                        config_param[i],
                        new_value);
                }
                GS_FREE(new_value);
            } else {
                nRet = snprintf_s(buffer + curlen, (buflen - curlen), (buflen - curlen - 1), " -c %s", config_param[i]);
            }
            securec_check_ss_c(nRet, buffer, "\0");
            curlen = curlen + nRet;
        } else {
            if (local_mode) {
                nRet = snprintf_s(
                    buffer + curlen, (buflen - curlen), (buflen - curlen - 1), " -h %c%s%c", '"', config_value[i], '"');
            } else {
                nRet = snprintf_s(
                    buffer + curlen, (buflen - curlen), (buflen - curlen - 1), " -h \\\"%s\\\"", config_value[i]);
            }
            securec_check_ss_c(nRet, buffer, "\0");
            curlen = curlen + nRet;
        }
    }

    return buffer;
}

/*
 ******************************************************************************
 Function    : get_nodeidx_by_HA
 Description : get the node index by HA ip/port
 Input       : HAIp - HA ipaddr
             : HAPort  HA port
 Output      : None
 Return      : int  -  node id index
 *******************************************************************************
*/
uint32 get_nodeidx_by_HA(const char* HAIp, uint32 HAPort)
{
    uint32 i = 0;
    uint32 j = 0;

    for (i = 0; i < g_node_num; i++) {
        for (j = 0; j < g_node[i].datanodeCount; j++) {
            if ((0 == strncmp(g_node[i].datanode[j].datanodeLocalHAIP[0], HAIp, strlen(HAIp))) &&
                (0 == g_node[i].datanode[j].datanodeLocalHAPort - HAPort))
                return i;
        }
    }
    return 0;
}
/*
 ******************************************************************************
 Function    : get_instance_id
 Description : get_instance_id by data path and HA ip/port. First, get node index by HA ip/port;
                  then get instance id by data path
 Input       : dataPath - datanode instance path
             : HAIp - HA ipaddr
             : HAPort - HA port
 Output      : None
 Return      : int  -  datanode instance id
 ******************************************************************************
*/
uint32 get_instance_id(const char* dataPath, const char* HAIp, uint32 HAPort)
{
    uint32 i = 0;
    uint32 nodeidx = 0;

    nodeidx = get_nodeidx_by_HA(HAIp, HAPort);
    for (i = 0; i < g_node[nodeidx].datanodeCount; i++) {
        if (0 == strncmp(g_node[nodeidx].datanode[i].datanodeLocalDataPath, dataPath, strlen(dataPath)))
            return g_node[nodeidx].datanode[i].datanodeId;
    }
    return 0;
}

/*
 ******************************************************************************
 Function    : is_instance_level_correct
 Description : check whether the instance is in same safety ring by data path , HA ip/port and the level. First, get
node index by HA ip/port; then check the level Input       : dataPath - datanode instance path : HAIp - HA ipaddr :
HAPort - HA port : level - the instance level Output      : None Return      : True/False
 ******************************************************************************
*/
bool is_instance_level_correct(const char* dataPath, const char* HAIp, uint32 HAPort, uint32 level)
{
    uint32 i = 0;
    uint32 nodeidx = 0;
    /*get node idx by HA information */
    nodeidx = get_nodeidx_by_HA(HAIp, HAPort);
    for (i = 0; i < g_node[nodeidx].datanodeCount; i++) {
        if ((0 == strncmp(g_node[nodeidx].datanode[i].datanodeLocalDataPath, dataPath, strlen(dataPath))) &&
            (level == g_node[nodeidx].datanode[i].datanodeRole))
            return true;
    }
    return false;
}

/*
 ******************************************************************************
GetPgxcNodeNameForMasterDnInstance
    get the pgxc_node_name on single primary mutile standby cluster, by node index and datanode instance index
Input: nodeidx -> node index(it comes from static_config_file)
    instanceidx -> node index(it comes from static_config_file)
 ******************************************************************************
*/
char* GetPgxcNodeNameForMasterDnInstance(int32 nodeidx, int32 instanceidx)
{
    char* pgxcNodeName = (char*)pg_malloc_zero(sizeof(char) * MAXPGPATH);
    int ret = 0;

    /*
     * get all standby dn instance id arry.
     * name dn_6001_6002_6003 -> 6001 must be primary DN instance
     */
    uint32 instance_id_arry[CM_NODE_MAXNUM] = {0};

    ret = snprintf_s(pgxcNodeName, MAXPGPATH, MAXPGPATH - 1, "dn_%u", g_node[nodeidx].datanode[instanceidx].datanodeId);
    securec_check_ss_c(ret, "\0", "\0");

    if (g_dn_replication_num == 0) {
        write_stderr("ERROR: Failed to get dn instance in the partition.\n");
        exit(1);
    }

    for (uint32 dnId = 0; dnId < g_dn_replication_num - 1; dnId++) {
        char tmp_command[MAXPGPATH] = {0};
        /*
         * The instance must be standby. So use datanodePeerHAIP replace datanodePeer2HAIP
         */
        instance_id_arry[dnId] =
            get_instance_id(g_node[nodeidx].datanode[instanceidx].peerDatanodes[dnId].datanodePeerDataPath,
                g_node[nodeidx].datanode[instanceidx].peerDatanodes[dnId].datanodePeerHAIP[0],
                g_node[nodeidx].datanode[instanceidx].peerDatanodes[dnId].datanodePeerHAPort);
        ret = memset_s(tmp_command, MAXPGPATH, '\0', MAXPGPATH);
        securec_check_c(ret, "\0", "\0");
        ret = snprintf_s(tmp_command, MAXPGPATH, MAXPGPATH - 1, "_%u", instance_id_arry[dnId]);
        securec_check_ss_c(ret, "\0", "\0");
        ret = strncat_s(pgxcNodeName, MAXPGPATH, tmp_command, strlen(tmp_command));
        securec_check_ss_c(ret, "\0", "\0");
    }

    return pgxcNodeName;
}

/*
 ******************************************************************************
 CheckInstanceNameForSinglePrimaryMutilStandby
    check the instance_name, whether it is in single primary mutile standby cluster or not
 instance Type info: consist with OM
    PRIMARY_DN         0
    STANDBY_DN         1
    DUMMY_STANDBY_DN   2
 ******************************************************************************
*/
bool CheckInstanceNameForSinglePrimaryMutilStandby(int32 nodeidx, const char* instance_name)
{
    uint32 i = 0;
    uint32 j = 0;
    int ret = 0;
    uint32 nameLen = 0;
    char* pgxcNodeName = NULL;

    for (i = 0; i < g_node[nodeidx].datanodeCount; i++) {
        /* deal with the primary dn instance branch */
        if (g_node[nodeidx].datanode[i].datanodeRole == 0) {
            pgxcNodeName = GetPgxcNodeNameForMasterDnInstance(nodeidx, i);
            nameLen = strlen(instance_name) > strlen(pgxcNodeName) ? strlen(instance_name) : strlen(pgxcNodeName);
            ret = strncmp(pgxcNodeName, instance_name, nameLen);
            GS_FREE(pgxcNodeName);
            if (ret == 0) {
                return true;
            }
        } else {
            /* deal with the standby dn instance branch
             * The pgxc_node_name of the primary and standby instances are the same, So get it by primary DN instance
             * get the master instance first
             *     nodeIndex -> primary DN node index
             *     instanceidx -> primary DN instance index
             *     dataPath -> primary DN data path
             */
            uint32 nodeIndex = 0;
            uint32 instanceidx = 0;
            char dataPath[MAXPGPATH] = {0};
            size_t dataPathLen = 0;

            ret = memset_s(dataPath, MAXPGPATH, '\0', MAXPGPATH);
            securec_check_c(ret, "\0", "\0");

            /*
             * Each data ring must have a primary instance. So get the node index and data path.
             */
            for (uint32 dnId = 0; dnId < g_dn_replication_num - 1; dnId++) {
                if (0 == g_node[nodeidx].datanode[i].peerDatanodes[dnId].datanodePeerRole) {
                    nodeIndex = get_nodeidx_by_HA(g_node[nodeidx].datanode[i].peerDatanodes[dnId].datanodePeerHAIP[0],
                        g_node[nodeidx].datanode[i].peerDatanodes[dnId].datanodePeerHAPort);
                    ret = snprintf_s(dataPath,
                        MAXPGPATH,
                        MAXPGPATH - 1,
                        "%s",
                        g_node[nodeidx].datanode[i].peerDatanodes[dnId].datanodePeerDataPath);
                    securec_check_ss_c(ret, "\0", "\0");
                    break;
                }
            }

            /*
             * Check the result to ensure that the nodeIndex and instance data path of primary instance is found.
             */
            if (dataPath[0] == '\0') {
                fprintf(stderr, _("ERROR: Failed to get primary DN instance information.\n"));
                exit(1);
            }

            /*
             * get the primary DN instance index by node index and data path
             */
            for (j = 0; j < g_node[nodeIndex].datanodeCount; j++) {
                if (g_node[nodeIndex].datanode[i].datanodeRole == 0) {
                    dataPathLen = strlen(g_node[nodeIndex].datanode[j].datanodeLocalDataPath);
                    if (0 == strncmp(g_node[nodeIndex].datanode[j].datanodeLocalDataPath,
                                 dataPath,
                                 dataPathLen > strlen(dataPath) ? dataPathLen : strlen(dataPath))) {
                        instanceidx = j;
                        break;
                    }
                }
            }

            pgxcNodeName = GetPgxcNodeNameForMasterDnInstance(nodeIndex, instanceidx);
            nameLen = strlen(instance_name) > strlen(pgxcNodeName) ? strlen(instance_name) : strlen(pgxcNodeName);
            ret = strncmp(pgxcNodeName, instance_name, nameLen);
            GS_FREE(pgxcNodeName);
            if (0 == ret) {
                return true;
            }
        }
    }
    return false;
}

/*
 ******************************************************************************
 Function    : validate_instance_name_for_DN
 Description : validate the DN instance name.
 Input       : nodeidx - node id index
               instance_name - instance name
 Return      : bool
 ******************************************************************************
*/
bool validate_instance_name_for_DN(int32 nodeidx, const char* instance_name)
{
    bool isCorrect = false;
    uint32 i = 0;
    uint32 instance_id = 0;
    char temp_instance_name[MAXPGPATH];
    int rc = 0;

    rc = memset_s(temp_instance_name, MAXPGPATH, '\0', MAXPGPATH);
    securec_check_c(rc, "\0", "\0");

    if ((int)strlen(instance_name) < DN_INSTANCE_LEN) {
        return false;
    }

    /*single primary multil standby */
    if (g_multi_az_cluster) {
        isCorrect = CheckInstanceNameForSinglePrimaryMutilStandby(nodeidx, instance_name);
    } else {
        /* master_standby */
        if ((int)strlen(instance_name) != DN_INSTANCE_LEN)
            return false;

        for (i = 0; i < g_node[nodeidx].datanodeCount; i++) {
            if (g_node[nodeidx].datanode[i].datanodeRole == 0) {
                if (is_instance_level_correct(g_node[nodeidx].datanode[i].datanodePeerDataPath,
                        g_node[nodeidx].datanode[i].datanodePeerHAIP[0],
                        g_node[nodeidx].datanode[i].datanodePeerHAPort,
                        1))
                    instance_id = get_instance_id(g_node[nodeidx].datanode[i].datanodePeerDataPath,
                        g_node[nodeidx].datanode[i].datanodePeerHAIP[0],
                        g_node[nodeidx].datanode[i].datanodePeerHAPort);
                else
                    instance_id = get_instance_id(g_node[nodeidx].datanode[i].datanodePeer2DataPath,
                        g_node[nodeidx].datanode[i].datanodePeer2HAIP[0],
                        g_node[nodeidx].datanode[i].datanodePeer2HAPort);
                rc = snprintf_s(temp_instance_name,
                    MAXPGPATH,
                    MAXPGPATH - 1,
                    "dn_%d_%d",
                    (int)g_node[nodeidx].datanode[i].datanodeId,
                    (int)instance_id);
            } else {
                if (is_instance_level_correct(g_node[nodeidx].datanode[i].datanodePeerDataPath,
                        g_node[nodeidx].datanode[i].datanodePeerHAIP[0],
                        g_node[nodeidx].datanode[i].datanodePeerHAPort,
                        0))
                    instance_id = get_instance_id(g_node[nodeidx].datanode[i].datanodePeerDataPath,
                        g_node[nodeidx].datanode[i].datanodePeerHAIP[0],
                        g_node[nodeidx].datanode[i].datanodePeerHAPort);
                else
                    instance_id = get_instance_id(g_node[nodeidx].datanode[i].datanodePeer2DataPath,
                        g_node[nodeidx].datanode[i].datanodePeer2HAIP[0],
                        g_node[nodeidx].datanode[i].datanodePeer2HAPort);
                rc = snprintf_s(temp_instance_name,
                    MAXPGPATH,
                    MAXPGPATH - 1,
                    "dn_%d_%d",
                    (int)instance_id,
                    (int)g_node[nodeidx].datanode[i].datanodeId);
            }
            securec_check_ss_c(rc, "\0", "\0");

            if (strncmp(temp_instance_name, instance_name, strlen(instance_name)) == 0) {
                isCorrect = true;
                break;
            }

            rc = memset_s(temp_instance_name, MAXPGPATH, '\0', MAXPGPATH);
            securec_check_c(rc, "\0", "\0");
        }
    }

    return isCorrect;
}

/*
 ******************************************************************************
 Function    : validate_remote_instance_name
 Description : validate remote instance name.
               The gs_guc commands like this: "-I instance_name -N nodename".
               The instance name type like this:
                     INSTANCE_COORDINATOR   -> cn_instanceId
                     INSTANCE_GTM           -> one
                     INSTANCE_DATANODE      -> dn_masterId_slaveId, dn_masterId_dummyslaveId
 Input       : nodename - node name
               type  - instance type
               instance_name - instance name
 Return      : int
 ******************************************************************************
*/
int validate_remote_instance_name(char* nodename, int type, char* instance_name)
{
    int32 nodeidx = 0;
    char temp_instance_name[MAXPGPATH];
    int rc = 0;
    bool isCorrect = false;

    rc = memset_s(temp_instance_name, MAXPGPATH, '\0', MAXPGPATH);
    securec_check_c(rc, "\0", "\0");

    nodeidx = get_nodeidx_by_name(nodename);
    /* check the node name, makesure it is in cluster_static_config */
    if (nodeidx < 0) {
        write_stderr("ERROR: Node %s is not found in static config file.\n", nodename);
        return 1;
    }

    /*
     *  INSTANCE_COORDINATOR   -> cn_instanceId
     *  INSTANCE_GTM           -> one
     *  INSTANCE_DATANODE      -> dn_masterId_slaveId, dn_masterId_dummyslaveId
     */
    if (type == INSTANCE_COORDINATOR) {
        rc = snprintf_s(temp_instance_name, MAXPGPATH, MAXPGPATH - 1, "cn_%d", (int)g_node[nodeidx].coordinateId);
        securec_check_ss_c(rc, "\0", "\0");

        if ((CN_INSTANCE_LEN == (int)strlen(instance_name)) &&
            (0 == strncmp(temp_instance_name, instance_name, strlen(instance_name))))
            isCorrect = true;
    } else if (type == INSTANCE_GTM) {
        if ((0 != g_node[nodeidx].gtmId) && (GTM_INSTANCE_LEN == (int)strlen(instance_name)) &&
            (0 == strncmp(instance_name, "one", strlen("one"))))
            isCorrect = true;
    } else {
        isCorrect = validate_instance_name_for_DN(nodeidx, instance_name);
    }

    if (isCorrect) {
        return 0;
    } else {
        write_stderr("ERROR: Instance name %s is incorrect.\n", instance_name);
        return 1;
    }
}
/*
 ******************************************************************************
 Function    : validate_nodename
 Description : validate the node name. If the node name is not all, makesure it is
               in the cluster static config file.
 Input       : nodename - node name
 Return      : int
 ******************************************************************************
*/
int validate_nodename(char* nodename)
{
    int32 nodeidx = 0;
    /* makesure the node name is correct */
    if ((NULL != nodename) && (0 != strncmp(nodename, "all", sizeof("all")))) {
        nodeidx = get_nodeidx_by_name(nodename);
        /* check the node name, makesure it is in cluster_static_config */
        if (nodeidx < 0) {
            write_stderr("ERROR: Node %s is not found in static config file.\n", nodename);
            return 1;
        }
    }
    return 0;
}
/*
 ******************************************************************************
 Function    : check_instance_name
 Description : check instance name. We known that the node name and instance name are both not 'NULL' and not 'all'.
 Input       : nodename - node name
               type  - instance type
               instance_name - instance name
 Return      : int
 ******************************************************************************
*/
int check_instance_name(char* nodename, int type, char* instance_name)
{
    char temp_datadir[MAXPGPATH];
    int rc = 0;

    rc = memset_s(temp_datadir, MAXPGPATH, '\0', MAXPGPATH);
    securec_check_c(rc, "\0", "\0");

    /* -N nodename -I instance_name*/
    if ((0 != strncmp(nodename, "all", sizeof("all"))) && (0 != strncmp(instance_name, "all", sizeof("all")))) {
        if (is_local_node(nodename)) {
            if (get_local_dbpath_by_instancename(instance_name, &type, temp_datadir) == CLUSTER_CONFIG_ERROR) {
                write_stderr("ERROR: Instance name %s is incorrect.\n", instance_name);
                return 1;
            }
        } else {
            if (0 != validate_remote_instance_name(nodename, type, instance_name))
                return 1;
        }
    }

    return 0;
}
/*
 ******************************************************************************
 Function    : validate_node_instance_name
 Description : validate the node name and instance name
 Input       : nodename - node name
               type  - instance type
               instance_name - instance name
 Output      : None
 Return      : int
 ******************************************************************************
*/
int validate_node_instance_name(char* nodename, int type, char* instance_name)
{
    char temp_datadir[MAXPGPATH];
    int rc = 0;

    rc = memset_s(temp_datadir, MAXPGPATH, '\0', MAXPGPATH);
    securec_check_c(rc, "\0", "\0");

    /* Verify that the node name is correct */
    if (0 != validate_nodename(nodename))
        return 1;

    if ((NULL == nodename) && (NULL != instance_name)) {
        /* -I instance_name*/
        if ((0 != strncmp(instance_name, "all", sizeof("all"))) &&
            (get_local_dbpath_by_instancename(instance_name, &type, temp_datadir) == CLUSTER_CONFIG_ERROR)) {
            write_stderr("ERROR: Instance name %s is incorrect.\n", instance_name);
            return 1;
        }
    }

    if ((NULL != nodename) && (NULL != instance_name)) {
        /* skip  check '-N all -I all', '-N nodename -I all'*/
        /* command ' -N all -I instance_name' is incorrect expect for DN*/
        if (type != INSTANCE_DATANODE) {
            if ((strncmp(nodename, "all", sizeof("all")) == 0) && (strncmp(instance_name, "all", sizeof("all")) != 0)) {
                write_stderr(
                    "ERROR: Instance name %s is incorrect. When -N is 'all', -I must be the same.\n", instance_name);
                return 1;
            }
        }

        /* -N nodename -I instance_name*/
        if (0 != check_instance_name(nodename, type, instance_name))
            return 1;
    }

    return 0;
}

#ifndef ENABLE_MULTIPLE_NODES
/*
 ******************************************************************************
 Function    : gsguc_precheck_forbid_parameters
 Description : Forbid reload/set/check GUC parameters in list for ALL node type
 Input       : nodename(indicates node name)
 Output      : none
 Return      : bool
 ******************************************************************************
 */

/* Forbid parameters for method "reload" with "all" nodes */
char *gsguc_forbid_list_reload[] = {
    "listen_addresses",
    NULL
};

bool gsguc_precheck_forbid_parameters(char *nodename)
{
    /* If set pg_hba, just pass */
    if (is_hba_conf) {
        return true;
    }

    switch (ctl_command) {
        case SET_CONF_COMMAND:
            /* process checking parameters for SET mode */
            break;
        case RELOAD_CONF_COMMAND:
            /* process checking parameters for RELOAD mode */
            if (strncmp(nodename, "all", strlen("all")) != 0) {
                break;
            }
            /* parameters not support '-N all' */
            for (int i = 0; i < config_param_number && config_param[i] != NULL; i++) {
                for (int j = 0; gsguc_forbid_list_reload[j] != NULL; j++) {
                    if (strcmp(config_param[i], gsguc_forbid_list_reload[j]) == 0) {
                        write_stderr(_("ERROR: \"%s\" can not \"%s\" with \"%s\" method.\n"),
                            gsguc_forbid_list_reload[j],
                            get_ctl_command_type(),
                            nodename);
                        return false;
                    }
                }
            }
            break;
        case CHECK_CONF_COMMAND:
            /* process checking parameters for CHECK mode */
            break;
        default:
            break;
    }
    return true;
}
#endif

/*
 ******************************************************************************
 Function    : validate_cluster_guc_options
 Description : check the -N, -I and -D parameter
 Input       : nodename - node name
               type  - node type
               instance_name - instance name
               indatadir - instance data directory
 Output      : None
 Return      : int
 ******************************************************************************
*/
int validate_cluster_guc_options(char* nodename, int type, char* instance_name, char* indatadir)
{
    if ((NULL != nodename) || (NULL != instance_name)) {
        if (0 != init_gauss_cluster_config()) {
            (void)write_stderr("ERROR: Failed to get cluster information from static configuration file.\n");
            return 1;
        }
    }

    if ((NULL == instance_name) && (NULL == indatadir)) {
        if (type == INSTANCE_CMAGENT || type == INSTANCE_CMSERVER) {
            write_stderr("ERROR: -I all are mandatory for executing gs_guc.\n");
        } else {
            write_stderr("ERROR: -D or -I are mandatory for executing gs_guc.\n");
        }
        return 1;
    } else if ((NULL != instance_name) && (NULL != indatadir)) {
        write_stderr("ERROR: -D or -I only need one for executing gs_guc.\n");
        return 1;
    }

    if (node_type_number == LARGE_INSTANCE_NUM && (NULL != instance_name) &&
        (0 != strncmp(instance_name, "all", sizeof("all")))) {
        write_stderr("ERROR: when -Z is coordinator and datanode, the -I must be 'all'.\n");
        return 1;
    }

    /* The user guarantees the correctness of the -D parameter value*/
    if (0 != validate_node_instance_name(nodename, type, instance_name))
        return 1;

    do_checkvalidate(type);

    return 0;
}

/*
 ******************************************************************************
 Function    : save_expect_instance_info
 Description : save expect instance information into global parameter.
               node name and instance guc configure file
 Input       : datadir (the directory about instance)
 Output      : "expected instance path: %s\n", gucconf_file
 Return      : void
 ******************************************************************************
*/
void save_expect_instance_info(const char* datadir)
{
    int i = 0;
    if (NULL == datadir || '\0' == datadir[0]) {
        (void)write_stderr("instance data directory is NULL.\n");
        return;
    }

    // Get the configuration file, such as pg_hba.conf/postgresql.conf/cmagent.conf
    get_instance_configfile(datadir);
    if (CHECK_CONF_COMMAND == ctl_command) {
        for (i = 0; i < config_param_number; i++) {
            g_expect_gucInfo->nodename_array[g_expect_gucInfo->nodename_num++] = xstrdup(g_local_node_name);
            g_expect_gucInfo->gucinfo_array[g_expect_gucInfo->gucinfo_num++] = xstrdup(gucconf_file);
            g_expect_gucInfo->paramname_array[g_expect_gucInfo->paramname_num++] = xstrdup(config_param[i]);
            g_expect_gucInfo->paramvalue_array[g_expect_gucInfo->paramvalue_num++] = xstrdup("NULL");
            (void)write_stderr(
                "expected guc information: %s: %s=NULL: [%s]\n", g_local_node_name, config_param[i], gucconf_file);
        }
    } else {
        g_expect_gucInfo->nodename_array[g_expect_gucInfo->nodename_num++] = xstrdup(g_local_node_name);
        g_expect_gucInfo->gucinfo_array[g_expect_gucInfo->gucinfo_num++] = xstrdup(gucconf_file);
        (void)write_stderr("expected instance path: [%s]\n", gucconf_file);
    }
}

void check_env_value(const char* input_env_value)
{
    const char* danger_character_list[] = {"|",
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

    for (i = 0; danger_character_list[i] != NULL; i++) {
        if (strstr(input_env_value, danger_character_list[i]) != NULL) {
            fprintf(stderr,
                _("ERROR: Failed to check environment value: invalid token \"%s\".\n"),
                danger_character_list[i]);
            exit(1);
        }
    }
}

/*
 ******************************************************************************
 Function    : get_env_value
 Description : get environment variable value.
 Input       : env_var      (environment variable name) ,output_env_value
 Output      :
 Return      : bool
 ******************************************************************************
*/
bool get_env_value(const char* env_var, char* output_env_value, size_t env_var_value_len)
{
    char* env_value = NULL;
    errno_t rc = 0;

    if (NULL == env_var)
        return false;

    env_value = getenv(env_var);
    if ((NULL == env_value) || ('\0' == env_value[0])) {
        write_stderr(
            "ERROR: Failed to obtain environment variable \"%s\". Please check and makesure it is set.\n", env_var);
        return false;
    }

    if (env_var_value_len <= strlen(env_value)) {
        write_stderr("ERROR: The value of environment variable \"%s\" is too long.\n", env_var);
        return false;
    }

    rc = strcpy_s(output_env_value, env_var_value_len, env_value);
    securec_check_c(rc, "\0", "\0");
    return true;
}

/*
 ******************************************************************************
 Function    : process_cluster_guc_option
 Description :
 Input       : nodename -
               type -
               instance_name -
               indatadir -
 Output      : None
 Return      : void
 ******************************************************************************
*/
void process_cluster_guc_option(char* nodename, int type, char* instance_name, char* indatadir)
{
    uint32 idx = 0;
    int instance_nums = 0;
    int nRet = 0;
    char local_name[MAX_HOST_NAME_LENGTH];
    int malloc_num = 1;
    char *cmpath = NULL;
    /* init g_remote_connection_signal */
    g_remote_connection_signal = true;
    /* init g_remote_command_result */
    g_remote_command_result = 0;

    g_real_gucInfo = (gucInfo*)pg_malloc(sizeof(gucInfo));
    g_expect_gucInfo = (gucInfo*)pg_malloc(sizeof(gucInfo));
    /* only execute gs_guc in one node and one instance. Only specify the -D parameter */
    if (NULL != indatadir && NULL == nodename) {
        nRet = memset_s(local_name, MAX_HOST_NAME_LENGTH, '\0', MAX_HOST_NAME_LENGTH);
        securec_check_c(nRet, "\0", "\0");
        if (get_hostname_or_ip(local_name, MAX_HOST_NAME_LENGTH) == false) {
            exit(1);
        }

        g_local_node_name = xstrdup(local_name);

        /* get current cluster information from cluster_staic_config */
        if (has_static_config() && 0 == init_gauss_cluster_config()) {
            for (idx = 0; idx < get_num_nodes(); idx++) {
                if (is_local_nodeid(g_node[idx].node)) {
                    g_local_node_idx = idx;
                }
            }
        }

        malloc_num = 1;
    } else {
        /* get current cluster information from cluster_staic_config */
        if (0 != init_gauss_cluster_config())
            return;

        /* get local node idx and node name */
        for (idx = 0; idx < get_num_nodes(); idx++) {
            if (is_local_nodeid(g_node[idx].node)) {
                g_local_node_idx = idx;
            }
        }
        g_local_node_name = getnodename(g_local_node_idx);

        /* On node, coordinator/gtm number <= 1, datanode number >= 0. */
        if (node_type_number == LARGE_INSTANCE_NUM) {
            instance_nums = get_all_cndn_num();
        } else if (type == INSTANCE_DATANODE) {
            instance_nums = get_all_datanode_num();
        } else if (type == INSTANCE_COORDINATOR) {
            instance_nums = get_all_coordinator_num();
        } else if (type == INSTANCE_CMSERVER) {
            instance_nums = get_all_cmserver_num();
        } else if (type == INSTANCE_CMAGENT) {
            instance_nums = get_all_cmagent_num();
        } else {
            instance_nums = get_all_gtm_num();
        }

        g_incorrect_nodeInfo = (nodeInfo*)pg_malloc(sizeof(nodeInfo));
        g_incorrect_nodeInfo->nodename_array = (char**)pg_malloc_zero(get_num_nodes() * sizeof(char*));
        g_incorrect_nodeInfo->num = 0;

        malloc_num = instance_nums + 1;
    }

    if (NULL == g_local_node_name || '\0' == g_local_node_name[0]) {
        (void)write_stderr("ERROR: Failed to obtain local host name.\n");
        exit(1);
    }

    /* init global parameter */
    if (CHECK_CONF_COMMAND == ctl_command || type == INSTANCE_CMSERVER || type == INSTANCE_CMAGENT) {
        malloc_num = malloc_num * config_param_number;
    }
    g_real_gucInfo->nodename_array = (char**)pg_malloc_zero(sizeof(char*) * malloc_num);
    g_real_gucInfo->gucinfo_array = (char**)pg_malloc_zero(sizeof(char*) * malloc_num);
    g_expect_gucInfo->nodename_array = (char**)pg_malloc_zero(sizeof(char*) * malloc_num);
    g_expect_gucInfo->gucinfo_array = (char**)pg_malloc_zero(sizeof(char*) * malloc_num);
    g_real_gucInfo->nodename_num = 0;
    g_real_gucInfo->gucinfo_num = 0;
    g_expect_gucInfo->nodename_num = 0;
    g_expect_gucInfo->gucinfo_num = 0;

    if (CHECK_CONF_COMMAND == ctl_command) {
        g_real_gucInfo->paramname_array = (char**)pg_malloc_zero(sizeof(char*) * malloc_num);
        g_real_gucInfo->paramvalue_array = (char**)pg_malloc_zero(sizeof(char*) * malloc_num);
        g_expect_gucInfo->paramname_array = (char**)pg_malloc_zero(sizeof(char*) * malloc_num);
        g_expect_gucInfo->paramvalue_array = (char**)pg_malloc_zero(sizeof(char*) * malloc_num);
        g_real_gucInfo->paramname_num = 0;
        g_real_gucInfo->paramvalue_num = 0;
        g_expect_gucInfo->paramname_num = 0;
        g_expect_gucInfo->paramvalue_num = 0;
    }
    /* CN & DN & GTM && CMA && CMS */
    /* when nodename=NULL, it means only do setting for local node */
    if (NULL == nodename)
    {
        if ((INSTANCE_CMSERVER == type) || (INSTANCE_CMAGENT == type)) {
            cmpath = get_cm_real_path(type);
            do_local_instance(type, instance_name, cmpath);
            GS_FREE(cmpath);
        } else {
            do_local_instance(type, instance_name, indatadir);
        }
    }
    else
    {
        if (0 == strncmp(nodename, "all", strlen("all"))) {
#ifndef ENABLE_MULTIPLE_NODES
            if (!gsguc_precheck_forbid_parameters(nodename)) {
                exit(1);
            }
#endif
            do_all_nodes_instance(instance_name, indatadir);
        } else {
            do_remote_instance(nodename, instance_name, indatadir);
        }
    }
}

/*
 * the ssh return value:
 *     0     : The connection is successful, the command was successful
 *     1     : The connection is successful, the command fails
 *     127     : The connection is successful, the command fails
 *     255     : Connection failed
 */
void
printExecErrorMesg(const char* fcmd, const char *nodename)
{
    if (g_remote_command_result == 127) {
        write_stderr(_("ERROR: Failed to execute gs_guc command: %s on node \"%s\", error code is %u, "
                "please ensure that gs_guc exists.\n"), fcmd, nodename, g_remote_command_result);
    } else if (g_remote_command_result == 255) {
        write_stderr(_("ERROR: Failed to execute gs_guc command: %s on node \"%s\", error code is %u, "
                "Failed to connect node \"%s\".\n"), fcmd, nodename, g_remote_command_result, nodename);
    } else if (g_remote_command_result != 0) {
        write_stderr(_("ERROR: Failed to execute gs_guc command: %s on node \"%s\", error code is %u, "
                "please get more details from current node path \"$GAUSSLOG/bin/gs_guc\".\n"),
                fcmd, nodename, g_remote_command_result);
    }
}

/*
 ******************************************************************************
 Function    : is_changed_default_value_failed
 Description : Modify the default value for parameter "log_directory" and "audit_directory".
  Input       :type                instance type
               datadir             instance data directory
               param_name_str      parameter name
               index               parameter name index
               gausslog            defaul value
  return      :true                failed to change the default values
               false               Successfully set default values
 ******************************************************************************
*/
bool is_changed_default_value_failed(int type, char* datadir, char* param_name_str, int index, const char* gausslog)
{
    char local_inst_name[MAX_INSTANCENAME_LEN] = {0};
    char log_dir[MAX_VALUE_LEN] = {0};
    int32 retval;
    int nRet = 0;

    /* get local instance name by data path */
    retval = get_local_instancename_by_dbpath(datadir, local_inst_name);
    if (retval == CLUSTER_CONFIG_ERROR) {
        (void)write_stderr("ERROR: Failed to obtain instance name by data directory \"%s\".\n", datadir);
        return true;
    }

    if (type == INSTANCE_COORDINATOR || type == INSTANCE_DATANODE) {
        if (0 == strncmp(param_name_str, "log_directory", strlen("log_directory")))
            nRet = snprintf_s(log_dir, MAX_VALUE_LEN, MAX_VALUE_LEN - 1, "'%s/pg_log/%s'", gausslog, local_inst_name);
        else
            nRet = snprintf_s(log_dir, MAX_VALUE_LEN, MAX_VALUE_LEN - 1, "'%s/pg_audit/%s'", gausslog, local_inst_name);
        securec_check_ss_c(nRet, "\0", "\0");
    } else {
        if (0 == strncmp(param_name_str, "log_directory", strlen("log_directory"))) {
            nRet = snprintf_s(log_dir, MAX_VALUE_LEN, MAX_VALUE_LEN - 1, "'%s/pg_log/gtm'", gausslog);
            securec_check_ss_c(nRet, "\0", "\0");
        } else {
            (void)write_stderr("ERROR: The parameter \"%s\" don't support gtm instance.\n", param_name_str);
            return true;
        }
    }

    config_value[index] = xstrdup(log_dir);
    is_disable_log_directory = true;

    return false;
}

/*
 ******************************************************************************
 Function    : check_AZ_value
 Description : check the input azName.
  Input       :AZValue             az name
  return      :true                input az name is correct
               false               input az name is incorrect
 ******************************************************************************
*/
bool check_AZ_value(const char* AZValue)
{
    // Cause AZName can define by user, the check standard should ensure by om module. Here is a simple check
    if (strlen(AZValue) > (CM_AZ_NAME - 1)) {
        return false;
    }

    return true;
}

/*
 ******************************************************************************
 Function    : parse_AZ_result
 Description : parse AZ string into the node name list .
  Input       :AZValue             az name
  return      :NULL                input az name is incorrect
               other               the real result
 ******************************************************************************
*/
char* parse_AZ_result(char* AZStr, const char* data_dir)
{
    int nRet = 0;
    char* vptr = NULL;
    char* vouter_ptr = NULL;
    char* p = NULL;
    char delims[] = ",";
    char tmp[MAX_VALUE_LEN] = {0};
    int i = 0;
    char azList[3][MAX_INSTANCENAME_LEN] = {0};
    char tmpAzName[MAX_INSTANCENAME_LEN] = {0};
    char** array = NULL;
    char* buffer = NULL;
    int curlen = 0;
    char* azName = NULL;
    size_t len = 0;
    int ind = -1;
    const int az1_index = 0;
    const int az2_index = 1;
    const int az3_index = 2;
    int resultStatus = 0;

    // init tmp az string, array which storage az string
    nRet = memset_s(tmp, MAX_VALUE_LEN, '\0', MAX_VALUE_LEN);
    securec_check_c(nRet, "\0", "\0");
    nRet = snprintf_s(tmp, MAX_VALUE_LEN, MAX_VALUE_LEN - 1, "%s", AZStr);
    securec_check_ss_c(nRet, "\0", "\0");
    for (i = 0; i < 3; i++) {
        nRet = memset_s(azList[i], MAX_INSTANCENAME_LEN, '\0', MAX_INSTANCENAME_LEN);
        securec_check_c(nRet, "\0", "\0");
    }

    // split the az name by ','
    vptr = strtok_r(tmp, delims, &vouter_ptr);
    while (NULL != vptr) {
        p = vptr;

        // p like this:   AZ1,    AZ2...
        while (isspace((unsigned char)*p))
            p++;

        // Skip if the object already exists, otherwise store it.
        size_t azNameLength = strlen(p);
        if (check_AZ_value(p)) {
            // Skip if the object already exists, otherwise store it
            if (azList[az1_index][0] == '\0') {
                nRet = strncpy_s(azList[az1_index], MAX_INSTANCENAME_LEN, p, azNameLength);
                securec_check_c(nRet, "\0", "\0");
            } else if (azList[az2_index][0] == '\0') {
                if (azNameLength != strlen(azList[az1_index]) ||
                    strncmp(p, azList[az1_index], strlen(azList[az1_index])) != 0) {
                    nRet = strncpy_s(azList[az2_index], MAX_INSTANCENAME_LEN, p, azNameLength);
                    securec_check_c(nRet, "\0", "\0");
                }
            } else if (azList[az3_index][0] == '\0') {
                if ((azNameLength != strlen(azList[az1_index]) ||
                        strncmp(p, azList[az1_index], strlen(azList[az1_index])) != 0) &&
                    (azNameLength != strlen(azList[az2_index]) ||
                        strncmp(p, azList[az2_index], strlen(azList[az2_index])) != 0)) {
                    nRet = strncpy_s(azList[az3_index], MAX_INSTANCENAME_LEN, p, azNameLength);
                    securec_check_c(nRet, "\0", "\0");
                }
            } else {
                // nothing to do
            }

            // do spilt again
            vptr = strtok_r(NULL, delims, &vouter_ptr);
        } else {
            // input az name is incorrect
            (void)write_stderr("Notice: azName value check failed.\n");
            return NULL;
        }
    }

    // there is no AZ name, this branch can not be reached
    if ('\0' == azList[0][0]) {
        // input az name is incorrect
        return NULL;
    }

    // sort AZ list
    azName = get_AZname_by_nodename(g_local_node_name);
    if (NULL == azName) {
        (void)write_stderr("ERROR: Failed to obtain AZ name by local node.\n");
        return NULL;
    }

    for (i = 0; i < 3; i++) {
        if (0 == strncmp(azList[i], azName, strlen(azList[i]) > strlen(azName) ? strlen(azList[i]) : strlen(azName))) {
            ind = i;
        }
    }

    if (ind > 0) {
        // swap azlist[0] and azlist[ind]
        // save azlist[0]
        nRet = memset_s(tmpAzName, MAX_INSTANCENAME_LEN, '\0', MAX_INSTANCENAME_LEN);
        securec_check_c(nRet, "\0", "\0");
        nRet = strncpy_s(tmpAzName, MAX_INSTANCENAME_LEN, azList[0], strlen(azList[0]));
        securec_check_c(nRet, "\0", "\0");
        // set azlist[0] to azlist[ind]
        nRet = memset_s(azList[0], MAX_INSTANCENAME_LEN, '\0', MAX_INSTANCENAME_LEN);
        securec_check_c(nRet, "\0", "\0");
        nRet = strncpy_s(azList[0], MAX_INSTANCENAME_LEN, azList[ind], strlen(azList[ind]));
        securec_check_c(nRet, "\0", "\0");
        // set azlist[ind] to tmpAzName
        nRet = memset_s(azList[ind], MAX_INSTANCENAME_LEN, '\0', MAX_INSTANCENAME_LEN);
        securec_check_c(nRet, "\0", "\0");
        nRet = strncpy_s(azList[ind], MAX_INSTANCENAME_LEN, tmpAzName, strlen(tmpAzName));
        securec_check_c(nRet, "\0", "\0");
    }
    GS_FREE(azName);

    // init array
    array = (char**)pg_malloc(3 * sizeof(char*));
    array[0] = NULL;
    array[1] = NULL;
    array[2] = NULL;

    resultStatus = get_nodename_list_by_AZ(azList[0], data_dir, &array[0]);
    PROCESS_STATUS(resultStatus);
    // input az name is incorrect
    if (NULL == array[0]) {
        (void)write_log("ERROR: The AZ name \"%s\" does not be found on cluster. please makesure the AZ string "
                           "\"%s\" is correct.\n",
            azList[0],
            AZStr);
        goto failed;
    }
    len += strlen(array[0]) + 1;

    if ('\0' != azList[1][0]) {
        resultStatus = get_nodename_list_by_AZ(azList[1], data_dir, &array[1]);
        PROCESS_STATUS(resultStatus);
        // input az name is incorrect
        if (NULL == array[1]) {
            (void)write_log("ERROR: The AZ name \"%s\" does not be found on cluster. please makesure the AZ string "
                               "\"%s\" is correct.\n",
                azList[1],
                AZStr);
            goto failed;
        }
        len += strlen(array[1]) + 1;
    }

    if ('\0' != azList[2][0]) {
        resultStatus = get_nodename_list_by_AZ(azList[2], data_dir, &array[2]);
        PROCESS_STATUS(resultStatus);
        // input az name is incorrect
        if (NULL == array[2]) {
            (void)write_log("ERROR: The AZ name \"%s\" does not be found on cluster. please makesure the AZ string "
                               "\"%s\" is correct.\n",
                azList[2],
                AZStr);
            goto failed;
        }
        len += strlen(array[2]) + 1;
    }

    // get the string information
    buffer = (char*)pg_malloc_zero((len + 1) * sizeof(char));
    for (i = 0; i < 3; i++) {
        if (NULL != array[i] && strlen(array[i]) > 0) {
            nRet = snprintf_s(buffer + curlen, (len + 1 - curlen), (len - curlen), "%s,", array[i]);
            securec_check_ss_c(nRet, buffer, "\0");
            curlen = curlen + nRet;
        }
    }
    if (strlen(buffer) >= 2) {
        // skip the last character ','
        buffer[strlen(buffer) - 1] = '\0';
    } else {
        (void)write_stderr(
            "ERROR: There is no standby node, please makesure the AZ string \"%s\" is correct.\n", AZStr);
        goto failed;
    }

    GS_FREE(array[0]);
    GS_FREE(array[1]);
    GS_FREE(array[2]);
    GS_FREE(array);
    return buffer;

failed:
    GS_FREE(array[0]);
    GS_FREE(array[1]);
    GS_FREE(array[2]);
    GS_FREE(array);
    GS_FREE(buffer);
    return NULL;
}

/*
 ******************************************************************************
 Function    : get_nodename_number_from_nodelist
 Description : get the number of nodenames in the nodename string. String is split with ','
  Input       :AZValue             namelist
  return      :int                 the nodename number
 ******************************************************************************
*/
int get_nodename_number_from_nodelist(const char* namelist)
{
    char* ptr = NULL;
    char* outer_ptr = NULL;
    char delims[] = ",";
    size_t len = 0;
    int count = 0;
    char* buffer = NULL;
    int nRet = 0;

    len = strlen(namelist) + 1;
    buffer = (char*)pg_malloc_zero(len * sizeof(char));
    nRet = snprintf_s(buffer, len, len - 1, "%s", namelist);
    securec_check_ss_c(nRet, buffer, "\0");

    ptr = strtok_r(buffer, delims, &outer_ptr);
    while (NULL != ptr) {
        count++;
        ptr = strtok_r(NULL, delims, &outer_ptr);
    }

    GS_FREE(buffer);
    return count;
}

/*
 ******************************************************************************
 Function    : parse_datanodename_result
 Description : check data node name.
  Input       :datanodenamelist    data node name
  return      :NULL                input data node name is incorrect
               other               the real result
 ******************************************************************************
*/
char *ParseDatanameResult(const char *datanodeNameList, const char *dataDir)
{
    int nRet;
    char *vptr = NULL;
    char *vouterPtr = NULL;
    char *p = NULL;
    char delims[] = ",";
    char tmp[MAX_VALUE_LEN] = {0};
    char *buffer = NULL;
    size_t len;

    // init tmp nodeName string, array which storage nodeName string
    nRet = memset_s(tmp, MAX_VALUE_LEN, '\0', MAX_VALUE_LEN);
    securec_check_c(nRet, "\0", "\0");
    nRet = snprintf_s(tmp, MAX_VALUE_LEN, MAX_VALUE_LEN - 1, "%s", datanodeNameList);
    securec_check_ss_c(nRet, "\0", "\0");

    // split the node name by ','
    vptr = strtok_r(tmp, delims, &vouterPtr);
    while (vptr != NULL) {
        p = vptr;

        // p like this: dn_6001, dn_6002
        while (isspace((unsigned char)*p)) {
            p++;
        }

        if (CheckDataNameValue(p, dataDir)) {
            // do split again
            vptr = strtok_r(NULL, delims, &vouterPtr);
        } else {
            // input node name is incorrect
            write_stderr("Notice: datanodename value check failed.(datanodename=%s)\n", p);
            return NULL;
        }
    }

    len = strlen(datanodeNameList) + 1;
    // get the string information
    buffer = (char *)pg_malloc_zero(len * sizeof(char));
    nRet = snprintf_s(buffer, len, (len - 1), "%s", datanodeNameList);
    securec_check_ss_c(nRet, buffer, "\0");
    return buffer;
}

/*
 ******************************************************************************
 Function    : get_AZ_value
 Description : parse AZ string into the node name list .
  Input       :value               the parameter value from input
 ******************************************************************************
*/
char* get_AZ_value(const char* value, const char* data_dir)
{
    size_t minLen = 0;
    int nRet = 0;
    char tmp[MAX_VALUE_LEN] = {0};
    char* p = NULL;
    char* q = NULL;
    char* s = NULL;
    char preStr[16] = {0};
    char level[4] = {0};
    int i = 0;
    int j = 0;
    int count = 0;
    char* nodenameList = NULL;
    char* result = NULL;
    size_t len = 0;
    char* az1 = getAZNamebyPriority(g_az_master);
    char* vouter_ptr = NULL;
    char delims[] = ",";
    char* vptr = NULL;
    char emptyvalue[] = "''";
    bool isNodeName = false;

    if (az1 != NULL) {
        minLen = strlen("ANY X()") + strlen(az1);
    } else {
        (void)write_stderr("ERROR: can not find AZ_MASTER Name, current az_master priority=%u.\n", g_az_master);
        return NULL;
    }

    nRet = memset_s(preStr, sizeof(preStr) / sizeof(char), '\0', sizeof(preStr) / sizeof(char));
    securec_check_c(nRet, "\0", "\0");
    nRet = memset_s(level, sizeof(level) / sizeof(char), '\0', sizeof(level) / sizeof(char));
    securec_check_c(nRet, "\0", "\0");

    /* the value including ''' or space, so skip it */
    nRet = memset_s(tmp, MAX_VALUE_LEN, '\0', MAX_VALUE_LEN);
    securec_check_c(nRet, "\0", "\0");
    i = 0;
    j = 1;
    while (j < (int)strlen(value) - 1) {
        if (!isspace(value[j])) {
            tmp[i] = value[j];
            i++;
            j++;
        } else {
            j++;
        }
    }

    /* check value length */
    if (strlen(value) > MAX_VALUE_LEN) {
        (void)write_stderr("ERROR: The value of pamameter synchronous_standby_names is incorrect.\n");
        return NULL;
    }

    p = tmp;
    if (strlen(p) == 0 || *p == '*') {
        len = strlen(emptyvalue) + strlen(p) + 1;
        result = (char*)pg_malloc_zero(len * sizeof(char));
        nRet = snprintf_s(result, len, len - 1, "'%s'", p);
        securec_check_ss_c(nRet, "\0", "\0");
        return result;
    }

    // Assign values to preStr
    /* FIRST branch */
    if (0 == strncmp(p, "FIRST", strlen("FIRST"))) {
        nRet = strncpy_s(preStr, sizeof(preStr) / sizeof(char), "FIRST ", strlen("FIRST "));
        securec_check_c(nRet, "\0", "\0");
        p = p + strlen("FIRST");
    }
    /* ANY branch */
    if (0 == strncmp(p, "ANY", strlen("ANY"))) {
        nRet = strncpy_s(preStr, sizeof(preStr) / sizeof(char), "ANY ", strlen("ANY "));
        securec_check_c(nRet, "\0", "\0");
        p = p + strlen("ANY");
    }

    if (strncmp(p, "NODE", strlen("NODE")) == 0) {
        isNodeName = true;
        p = p + strlen("NODE");
    }

    /* make sure it is digit and between 1 and 7, including 1 and 7 */
    if (isdigit((unsigned char)*p)) {
        nRet = snprintf_s(level, sizeof(level) / sizeof(char),
            sizeof(level) / sizeof(char) - 1, "%c", (unsigned char)*p);
        securec_check_ss_c(nRet, "\0", "\0");
        if (atoi(level) < 1 || atoi(level) > 7) {
            goto failed;
        }

        if (strchr(p, '(') && strrchr(p, ')')) {
            q = strchr(p, '(');
            q++;
            s = strrchr(p, ')');
            s[0] = '\0';
        } else {
            goto failed;
        }
    } else {
        q = p;
    }

    /* skip this branch ANY 1() or ANY 1(*) */
    if (*q == '\0' || *q == '*') {
        goto failed;
    }

    if (isNodeName) {
        // parse and check nodeName string
        nodenameList = ParseDatanameResult(q, data_dir);
    } else {
        // parse and check the AZName string
        nodenameList = parse_AZ_result(q, data_dir);
    }

    if (NULL == nodenameList) {
        // try dn

        len = strlen(q) + 1;
        s = (char *)pg_malloc_zero(len * sizeof(char));
        nRet = snprintf_s(s, len, len - 1, "%s", q);
        securec_check_ss_c(nRet, s, "\0");

        vptr = strtok_r(s, delims, &vouter_ptr);
        while (vptr != NULL) {
            p = vptr;

            if (CheckDataNameValue(p, data_dir) == false) {
                GS_FREE(s);
                goto failed;
            }
            vptr = strtok_r(NULL, delims, &vouter_ptr);
        }

        GS_FREE(s);
        nodenameList = (char *)pg_malloc_zero(len * sizeof(char));
        nRet = snprintf_s(nodenameList, len, len - 1, "%s", q);
        securec_check_ss_c(nRet, nodenameList, "\0");
    } else if ('\0' == nodenameList[0]) {
        (void)write_stderr("ERROR: There is no standby node name. Please make sure the value of "
                           "synchronous_standby_names is correct.\n");
        GS_FREE(nodenameList);
        return NULL;
    }
    // X must less than node name numbers
    count = get_nodename_number_from_nodelist(nodenameList);
    if (atoi(level) > count) {
        (void)write_stderr("ERROR: The sync number(%d) must less or equals to the number of standby node names(%d). "
                           "Please make sure the value of synchronous_standby_names is correct.\n",
                           atoi(level), count);
        GS_FREE(nodenameList);
        return NULL;
    }

    // ANY/FIRST X + nodenameList + () + '' + \0
    if (atoi(level) >= 1) {
        len = strlen(preStr) + 6 + strlen(nodenameList);
        result = (char*)pg_malloc_zero(len * sizeof(char));
        nRet = snprintf_s(result, len, len - 1, "'%s%s(%s)'", preStr, level, nodenameList);
        securec_check_ss_c(nRet, "\0", "\0");
    } else {
        len = 3 + strlen(nodenameList);
        result = (char*)pg_malloc_zero(len * sizeof(char));
        nRet = snprintf_s(result, len, len - 1, "'%s'", nodenameList);
        securec_check_ss_c(nRet, "\0", "\0");
    }

    GS_FREE(nodenameList);
    return result;

failed:
    GS_FREE(nodenameList);
    GS_FREE(result);
    (void)write_stderr("ERROR: The value of pamameter synchronous_standby_names is incorrect.\n");
    return NULL;
}

/*
 ******************************************************************************
 Function    : do_local_para_value_change
 Description : only support parameter "log_directory" and "audit_directory".
               If we want to disable "log_directory", using the default value "$GAUSSLOG/pg_log/instance_name"
               If we want to disable "audit_directory", using the default value "$GAUSSLOG/pg_audit/instance_name"
 ******************************************************************************
*/
int do_local_para_value_change(int type, char* datadir)
{
    bool is_failed = false;
    int i = 0;
    char gausslog[MAXPGPATH] = {0};
    char staticfile[MAXPGPATH] = {0};
    char gausshome[MAXPGPATH] = {0};
    int nRet = 0;
    struct stat statbuf;

    if (type != INSTANCE_COORDINATOR && type != INSTANCE_DATANODE && type != INSTANCE_CMSERVER &&
        type != INSTANCE_CMAGENT && type != INSTANCE_GTM) {
        (void)write_stderr("ERROR: The instance type is incorrect.\n");
        return FAILURE;
    }

    for (i = 0; i < config_param_number; i++) {
        if (0 == strncmp(config_param[i],
                     "synchronous_standby_names",
                     strlen(config_param[i]) > strlen("synchronous_standby_names")
                         ? strlen(config_param[i])
                         : strlen("synchronous_standby_names"))) {
            if (type != INSTANCE_DATANODE) {
                (void)write_stderr(
                    "ERROR: The pamameter synchronous_standby_names only can be used for datanode type.\n");
                return FAILURE;
            }

            if (!get_env_value("GAUSSHOME", gausshome, sizeof(gausshome) / sizeof(char))) {
                g_need_changed = false;
            } else {
                check_env_value(gausshome);
                nRet = snprintf_s(staticfile, MAXPGPATH, MAXPGPATH - 1, "%s/bin/%s", gausshome, STATIC_CONFIG_FILE);
                securec_check_ss_c(nRet, "\0", "\0");
                if (lstat(staticfile, &statbuf) != 0) {
                    g_need_changed = false;
                } else {
                    if (0 != init_gauss_cluster_config()) {
                        (void)write_stderr(
                            "ERROR: Failed to get cluster information from static configuration file.\n");
                        return FAILURE;
                    }
                }
            }
            /* init g_local_instance_path */
            if (NULL == g_local_instance_path) {
                g_local_instance_path = xstrdup(datadir);
            }
        }
        if (NULL == config_value[i] || is_disable_log_directory) {
            if (0 == strncmp(config_param[i], "log_directory", strlen("log_directory")) ||
                0 == strncmp(config_param[i], "audit_directory", strlen("audit_directory"))) {
                if (!get_env_value("GAUSSLOG", gausslog, sizeof(gausslog) / sizeof(char)))
                    return FAILURE;

                check_env_value(gausslog);
                is_failed = is_changed_default_value_failed(type, datadir, config_param[i], i, gausslog);
            }
        }
    }

    if (is_failed)
        return FAILURE;
    return SUCCESS;
}

int do_local_guc_command(int type, char* temp_datadir)
{
    if ('\0' != temp_datadir[0]) {
        /*
         * When do check, do_local_para_value_change is not be used.
         */
        if ((type != INSTANCE_CMAGENT) && (type != INSTANCE_CMSERVER)) {
            if ((CHECK_CONF_COMMAND != ctl_command) && (FAILURE == do_local_para_value_change(type, temp_datadir)))
                return FAILURE;
        }

        if (0 != process_guc_command(temp_datadir))
            return FAILURE;
    }
    return SUCCESS;
}

/*
 ******************************************************************************
 Function    : do_command_in_local_node
 Description : set/reload guc parameter in local node
 Input       : type                (instance type)
               indatadir           (the instance data path)
 Output      : None
 Return      : void
 ******************************************************************************
*/
void do_command_in_local_node(int type, char* indatadir)
{
    char datadir[MAXPGPATH] = {0};

    /* process only in datadir */
    if (NULL == indatadir) {
        char* envvar = NULL;
        // datadir = /* get the it from PGDATA */
        if ((INSTANCE_COORDINATOR == type) || (INSTANCE_DATANODE == type))
            envvar = "PGDATA";
        else if (INSTANCE_GTM == type)
            envvar = "GTMDATA";
        else
            return;

        if (!get_env_value(envvar, datadir, sizeof(datadir) / sizeof(char)))
            return;
        if (NULL != datadir) {
            check_env_value(datadir);
        }
        /* process the PGDATA / GTMDATA */
        if (checkPath(datadir) != 0) {
            write_stderr(_("realpath(%s) failed : %s!\n"), datadir, strerror(errno));
        }
        save_expect_instance_info(datadir);
        if (FAILURE == do_local_guc_command(type, datadir))
            return;
    } else {
        /* process the -D option */
        if (checkPath(indatadir) != 0) {
            write_stderr(_("realpath(%s) failed : %s!\n"), indatadir, strerror(errno));
        }
        save_expect_instance_info(indatadir);
        if (FAILURE == do_local_guc_command(type, indatadir))
            return;
    }
}

/*
 ******************************************************************************
 Function    : do_command_with_all_option
 Description : set/reload guc parameter using "-I all" option
 Input       : type                (instance type)
               indatadir           (the instance data path)
 Output      : None
 Return      : void
 ******************************************************************************
*/
void do_command_with_all_option(int type, char* indatadir)
{
    if (node_type_number == LARGE_INSTANCE_NUM)
        do_command_for_cndn(type, indatadir);
    else if (type == INSTANCE_COORDINATOR)
        do_command_for_cn_gtm(type, indatadir, true);
    else if (type == INSTANCE_GTM)
        do_command_for_cn_gtm(type, indatadir, false);
    else if (type == INSTANCE_DATANODE)
        do_command_for_dn(type, indatadir);
    else if ((type == INSTANCE_CMAGENT) || (type == INSTANCE_CMSERVER))
        do_command_for_cm(type, indatadir);
    else
        return;
}

/*
 ******************************************************************************
 Function    : do_command_for_cn
 Description :
 Input       : type                (instance type)
               indatadir           (the instance data path)
               isCoordinator       if true is Coordinator, else is gtm
 Output      : None
 Return      : void
 ******************************************************************************
*/
void do_command_for_cn_gtm(int type, char* indatadir, bool isCoordinator)
{
    char temp_datadir[MAXPGPATH] = {0};
    errno_t rc = 0;

    if (isCoordinator)
        rc = memcpy_s(temp_datadir, sizeof(temp_datadir) / sizeof(char), g_currentNode->DataPath, sizeof(temp_datadir) / sizeof(char));
    else
        rc = memcpy_s(temp_datadir, sizeof(temp_datadir) / sizeof(char), g_currentNode->gtmLocalDataPath, sizeof(temp_datadir) / sizeof(char));
    securec_check_c(rc, "\0", "\0");

    save_expect_instance_info(temp_datadir);
    if (FAILURE == do_local_guc_command(type, temp_datadir))
        return;
}

/*
 ******************************************************************************
 Function    : do_command_for_dn
 Description :
 Input       : type                (instance type)
               indatadir           (the instance data path)
 Output      : None
 Return      : void
 ******************************************************************************
*/
void do_command_for_dn(int type, char* indatadir)
{
    char temp_datadir[MAXPGPATH] = {0};
    uint32 i = 0;
    errno_t rc = 0;

    for (i = 0; i < get_local_num_datanode(); i++) {
        rc = memcpy_s(temp_datadir, sizeof(temp_datadir) / sizeof(char), g_currentNode->datanode[i].datanodeLocalDataPath, sizeof(temp_datadir) / sizeof(char));
        securec_check_c(rc, "\0", "\0");
        save_expect_instance_info(temp_datadir);
    }

    for (i = 0; i < get_local_num_datanode(); i++) {
        rc = memcpy_s(temp_datadir, sizeof(temp_datadir) / sizeof(char), g_currentNode->datanode[i].datanodeLocalDataPath, sizeof(temp_datadir) / sizeof(char));
        securec_check_c(rc, "\0", "\0");
        if (FAILURE == do_local_guc_command(type, temp_datadir)) {
            return;
        }
    }
}

/*
 ******************************************************************************
 Function    : do_command_for_cm
 Description :
 Input       : type                (instance type)
               indatadir           (the instance data path)
               isCmserver          (if true is cmserver and pathname is "cm_server", else is cmagent and pathname is
"cm_agent") Output      : None Return      : void
 ******************************************************************************
*/
void
do_command_for_cm(int type, char* indatadir)
{
    char temp_datadir[MAXPGPATH] = {0};
    char cm_dir[MAXPGPATH] = {0};
    int  nRet = 0;
    errno_t rc = 0;

    rc = memcpy_s(cm_dir, sizeof(cm_dir)/sizeof(char), g_currentNode->cmDataPath, sizeof(cm_dir)/sizeof(char));
    securec_check_c(rc, "\0", "\0");

    if (cm_dir[0] == '\0') {
        write_stderr("Failed to get cm base datapath from static config file.");
        return;
    }

    if (type == INSTANCE_CMAGENT) {
        nRet = snprintf_s(temp_datadir, sizeof(temp_datadir)/sizeof(char),
                sizeof(temp_datadir)/sizeof(char) -1, "%s/cm_agent", cm_dir);
        securec_check_ss_c(nRet, "\0", "\0");
    } else {
        if (g_currentNode->cmServerLevel == 1) {
            nRet = snprintf_s(temp_datadir, sizeof(temp_datadir)/sizeof(char),
                    sizeof(temp_datadir)/sizeof(char) -1, "%s/cm_server", cm_dir);
            securec_check_ss_c(nRet, "\0", "\0");
        } else {
            /* There is no cmserver instance on the node */
            return;
        }
    }

    save_expect_instance_info(temp_datadir);
    if (FAILURE == do_local_guc_command(type, temp_datadir))
        return;
}

/*
 ******************************************************************************
 Function    : do_command_for_datainstance
 Description :
 Input       : type                (instance type)
               indatadir           (the instance data path)
 Output      : None
 Return      : void
 ******************************************************************************
*/
void do_command_for_cndn(int type, char* indatadir)
{
    do_command_for_cn_gtm(INSTANCE_COORDINATOR, indatadir, true);
    do_command_for_dn(INSTANCE_DATANODE, indatadir);
}

/*
 ******************************************************************************
 Function    : do_command_with_instance_name_option
 Description : set/reload guc parameter using "-I instance_name" option
 Input       : type                (instance type)
               indatadir           (the instance data path)
 Output      : None
 Return      : void
 ******************************************************************************
*/
void do_command_with_instance_name_option(int type, char* instance_name)
{
    if (node_type_number == LARGE_INSTANCE_NUM) {
        do_command_with_instance_name_option_local(INSTANCE_COORDINATOR, instance_name);

        do_command_with_instance_name_option_local(INSTANCE_DATANODE, instance_name);
    } else {
        do_command_with_instance_name_option_local(type, instance_name);
    }
}

char *
get_cm_real_path(int type)
{
    char *cmpath = NULL;
    if (INSTANCE_CMSERVER == type) {
        if (1 == g_node[g_local_node_idx].cmServerLevel && g_node[g_local_node_idx].cmDataPath[0] != '\0') {
            cmpath = xstrdup(g_node[g_local_node_idx].cmDataPath);
        } else {
            write_stderr("ERROR: Failed to get cmserver instance path.\n");
            exit(1);
        }
    } else if (INSTANCE_CMAGENT == type) {
        if (g_node[g_local_node_idx].cmDataPath[0] != '\0') {
            cmpath = xstrdup(g_node[g_local_node_idx].cmDataPath);
        } else {
            write_stderr("ERROR: Failed to get cmagent instance path.\n");
            exit(1);
        }
    } else {
        write_stderr("ERROR: the instance type is incorrect.\n");
        exit(1);
    }
    return cmpath;
}

void do_command_with_instance_name_option_local(int type, char* instance_name)
{
    char temp_datadir[MAXPGPATH];
    int rc = 0;

    rc = memset_s(temp_datadir, MAXPGPATH, '\0', MAXPGPATH);
    securec_check_c(rc, "\0", "\0");

    if (get_local_dbpath_by_instancename(instance_name, &type, temp_datadir) == CLUSTER_CONFIG_SUCCESS) {
        save_expect_instance_info(temp_datadir);
        if (FAILURE == do_local_guc_command(type, temp_datadir))
            return;
    } else {
        write_stderr("ERROR: Instance name %s is incorrect.\n", instance_name);
        exit(1);
    }
}
/*
 ******************************************************************************
 Function    : do_local_instance
 Description : set/reload guc parameter for local node.
                   1. -N and -I are NULL, Only specify -D parameter
                   2. -N is NULL, -I is "all", specify -D parameter
                   3. -N is NULL, specify -D or -I parameter
 Input       : type                (instance type)
               instance_name       (instance name)
               indatadir           (the instance data path)
 Output      : None
 Return      : void
 ******************************************************************************
*/
void
do_local_instance(int type, char* instance_name, char* indatadir)
{
    /* process the command in local node---Only specify -D parameter, -N and -I are NULL */
    if (NULL == instance_name) {
        do_command_in_local_node(type, indatadir);
    } else if (0 == strncmp(instance_name, "all", sizeof("all"))) {
        /* process the -I all option ---specify -D parameter, -I is "all",  -N is NULL */
        do_command_with_all_option(type, indatadir);
    } else {
        /* process the -I instance_name option. This branch CMA && CMS can not be reached */
        do_command_with_instance_name_option(type, instance_name);
    }
}
/*
 ******************************************************************************
 Function    : do_remote_instance
 Description : set/reload guc parameter for remote node
 Input       : nodename                (node name)
               instance_name           (instance name)
               indatadir               (the instance data path)
 Output      : None
 Return      : void
 ******************************************************************************
*/
void do_remote_instance(char* nodename, const char* instance_name, const char* indatadir)
{
    if (node_type_number == LARGE_INSTANCE_NUM) {
        nodetype = INSTANCE_COORDINATOR;
        do_remote_instance_local(nodename, instance_name, indatadir);

        nodetype = INSTANCE_DATANODE;
        do_remote_instance_local(nodename, instance_name, indatadir);
    } else {
        do_remote_instance_local(nodename, instance_name, indatadir);
    }
}

void do_remote_instance_local(char* nodename, const char* instance_name, const char* indatadir)
{
    char* command = NULL;
    int32 nodeidx;
    bool local_mode = !strncmp(g_local_node_name,
        nodename,
        strlen(g_local_node_name) > strlen(nodename) ? strlen(g_local_node_name) : strlen(nodename));
    command = form_commandline_options(instance_name, indatadir, local_mode);
    nodeidx = get_nodeidx_by_name(nodename);

    /* check the node name, makesure it is in cluster_staic_config */
    if (nodeidx < 0) {
        write_stderr("ERROR: Node %s not found in static config file\n", nodename);
        GS_FREE(command);
        exit(1);
    }

    (void)execute_guc_command_in_remote_node(nodeidx, command);

    GS_FREE(command);
}

/*
 ******************************************************************************
 Function    : do_all_nodes_instance
 Description : set/reload guc parameter for all cluster node
 Input       : instance_name           (instance name)
               indatadir               (the instance data path)
 Output      : None
 Return      : void
 ******************************************************************************
*/
void do_all_nodes_instance(const char* instance_name, const char* indatadir)
{
    if (node_type_number == LARGE_INSTANCE_NUM) {
        nodetype = INSTANCE_COORDINATOR;
        do_all_nodes_instance_local(instance_name, indatadir);

        nodetype = INSTANCE_DATANODE;
        do_all_nodes_instance_local(instance_name, indatadir);
    } else {
        do_all_nodes_instance_local(instance_name, indatadir);
    }
}
/*
 ******************************************************************************
 Function    : do_all_nodes_instance_local
 Description : do_all_nodes_instance_local. When do check in serial, do set/reload in parallel
 Input       : instance_name, indatadir
 Output      : void
 Return      : void
 ******************************************************************************
*/
void do_all_nodes_instance_local(const char* instance_name, const char* indatadir)
{
    if (CHECK_CONF_COMMAND == ctl_command) {
        do_all_nodes_instance_local_in_serial(instance_name, indatadir);
    } else {
        do_all_nodes_instance_local_in_parallel_loop(instance_name, indatadir);
    }
}

void do_all_nodes_instance_local_in_serial(const char* instance_name, const char* indatadir)
{
    uint32 idx = 0;
    for (idx = 0; idx < get_num_nodes(); idx++) {
        char* nodename = getnodename(idx);
        bool local_mode = !strncmp(g_local_node_name,
            nodename,
            strlen(g_local_node_name) > strlen(nodename) ? strlen(g_local_node_name) : strlen(nodename));
        char* command = form_commandline_options(instance_name, indatadir, local_mode);
        (void)execute_guc_command_in_remote_node(idx, command);
        GS_FREE(command);
    }
}

static void init_global_command()
{
    int i;
    int rc = 0;
    PARALLEL_COMMAND_S* curr_cxt = NULL;

    g_max_commands_parallel = get_num_nodes();
    g_parallel_command_cxt = (PARALLEL_COMMAND_S*)pg_malloc(g_max_commands_parallel * sizeof(PARALLEL_COMMAND_S));

    rc = memset_s(g_parallel_command_cxt,
        g_max_commands_parallel * sizeof(PARALLEL_COMMAND_S),
        '\0',
        g_max_commands_parallel * sizeof(PARALLEL_COMMAND_S));
    securec_check_c(rc, "\0", "\0");

    g_cur_commands_parallel = 0;
    for (i = 0; i < g_max_commands_parallel; i++) {
        curr_cxt = &g_parallel_command_cxt[i];
        curr_cxt->cur_buf_loc = 0;
        rc = memset_s(curr_cxt->readbuf, sizeof(curr_cxt->readbuf), '\0', sizeof(curr_cxt->readbuf));
        securec_check_c(rc, "\0", "\0");

        curr_cxt->pfp = NULL;
        curr_cxt->nodename = xstrdup(getnodename((uint32)i));
    }
}

static void reset_global_command()
{
    int i;
    for (i = 0; i < (int)g_incorrect_nodeInfo->num; i++) {
        GS_FREE(g_incorrect_nodeInfo->nodename_array[i]);
    }
    g_incorrect_nodeInfo->num = 0;
    g_cur_commands_parallel = 0;
}
static void do_all_nodes_instance_local_in_parallel_loop(const char* instance_name, const char* indatadir)
{
    int i;
    init_global_command();
    write_stderr("Begin to perform the total nodes: %d.\n", g_max_commands_parallel);
    for (i = 0; i < LOOP_COUNT; i++) {
        do_all_nodes_instance_local_in_parallel(instance_name, indatadir);
        if (g_incorrect_nodeInfo->num == 0 || i == LOOP_COUNT - 1) {
            break;
        }

        write_stderr("Retry to perform the failed nodes: %d.\n", (int32)g_incorrect_nodeInfo->num);
        reset_global_command();
        (void)SleepInMilliSec(100);
    }

    for (i = 0; i < g_max_commands_parallel; i++) {
        GS_FREE(g_parallel_command_cxt[i].nodename);
    }
    GS_FREE(g_parallel_command_cxt);

    if (g_incorrect_nodeInfo->num == 0) {
        (void)write_stderr("ALL: Success to perform gs_guc!\n\n");
    }
    else {
        (void)write_stderr("ALL: Failure to perform gs_guc!\n\n");
        exit(1);
    }
}

static bool needPassNode(const char* nodename)
{
    int cmpLen = 0;
    char* ignoreNode = NULL;

    for (uint32 i = 0; i < g_ignore_nodeInfo->num; i++) {
        ignoreNode = g_ignore_nodeInfo->nodename_array[i];
        cmpLen = (strlen(ignoreNode) > strlen(nodename)) ? strlen(ignoreNode) : strlen(nodename);
        if (strncmp(ignoreNode, nodename, cmpLen) == 0) {
            return true;
        }
    }
    return false;
}

void do_all_nodes_instance_local_in_parallel(const char* instance_name, const char* indatadir)
{
    int idx = 0;
    char* nodename = NULL;
    int buf_len = 0;
    bool if_for_all_instance = true;
    int open_count = 0;
    bool is_local_node = false;
    char* command_local = form_commandline_options(instance_name, indatadir, true);
    char* command_remote = form_commandline_options(instance_name, indatadir, false);

    if ((instance_name != NULL) && (strncmp(instance_name, "all", sizeof("all")) != 0)) {
        if_for_all_instance = false;
    }

    for (idx = 0; idx < g_max_commands_parallel; idx++) {
        if (if_for_all_instance == false && validate_instance_name_for_DN(idx, instance_name) == false) {
            continue;
        }
        /*
         * When instance type is INSTANCE_CMSERVER, only the nodes that contain the cm_server instance are setting.
         * When instance type is INSTANCE_GTM/INSTANCE_COORDINATOR/INSTANCE_DATANODE, only the nodes that contain the gtm instance are setting.
         */
        if ((nodetype == INSTANCE_CMSERVER && 1 != g_node[idx].cmServerLevel) ||
            (nodetype == INSTANCE_GTM && 1 != g_node[idx].gtm) ||
            (nodetype == INSTANCE_COORDINATOR && 1 != g_node[idx].coordinate) ||
            (nodetype == INSTANCE_DATANODE && 0 == g_node[idx].datanodeCount)) {
            continue;
        }
        if (NULL == g_parallel_command_cxt[idx].nodename) {
            continue;
        }

        nodename = g_parallel_command_cxt[idx].nodename;
        if ((g_ignore_nodeInfo != NULL) && needPassNode(nodename)) {
            continue;
        }
        open_count++;

        buf_len = (strlen(g_local_node_name) > strlen(nodename)) ? strlen(g_local_node_name) : strlen(nodename);
        is_local_node = (0 == strncmp(g_local_node_name, nodename, buf_len)) ? true : false;
        is_local_node ? executePopenCommandsParallel(command_local, idx, is_local_node) :
            executePopenCommandsParallel(command_remote, idx, is_local_node);
    }
    write_stderr("Popen count is %d, Popen success count is %d, Popen failure count is %d.\n",
        open_count, g_cur_commands_parallel, (int)g_incorrect_nodeInfo->num);

    readPopenOutputParallel(command_local, if_for_all_instance);

    GS_FREE(command_local);
    GS_FREE(command_remote);
}

/*
 * Execute commands in parallel
 */
static void executePopenCommandsParallel(const char* cmd, int idx, bool is_local_node)
{
    int rc = 0;
    PARALLEL_COMMAND_S* curr_cxt = NULL;
    char* fcmd = NULL;
    char* mpprvFile = NULL;
    int nRet = 0;
    size_t len_fcmd = 0;
    char* nodename = NULL;
    /* the temp directory that stores gs_guc result information */
    char gausshome[MAXPGPATH] = {0};

    curr_cxt = &g_parallel_command_cxt[idx];
    nodename = g_parallel_command_cxt[idx].nodename;

    curr_cxt->cur_buf_loc = 0;
    rc = memset_s(curr_cxt->readbuf, sizeof(curr_cxt->readbuf), '\0', sizeof(curr_cxt->readbuf));
    securec_check_c(rc, "\0", "\0");
    len_fcmd = strlen(cmd) + strlen(nodename) + NAMEDATALEN + MAXPGPATH;

    if (!get_env_value("GAUSSHOME", gausshome, sizeof(gausshome) / sizeof(char))) {
        return;
    }
    check_env_value(gausshome);

    mpprvFile = GetEnvStr("MPPDB_ENV_SEPARATE_PATH");
    /* execute gs_guc commands by 'ssh' */
    if (mpprvFile == NULL) {
        fcmd = (char*)pg_malloc_zero(len_fcmd);
        nRet = is_local_node ? snprintf_s(fcmd, len_fcmd, len_fcmd - 1, "%s 2>&1", cmd) :
            snprintf_s(fcmd, len_fcmd, len_fcmd - 1, "pssh -s -H %s \"%s\" 2>&1", nodename, cmd);
    } else {
        if (MAXPGPATH <= strlen(mpprvFile)) {
            write_stderr("ERROR: The value of environment variable \"MPPDB_ENV_SEPARATE_PATH\" is too long.");
            GS_FREE(mpprvFile);
            return;
        }
        check_env_value(mpprvFile);
        len_fcmd = len_fcmd + (int)strlen(mpprvFile);
        fcmd = (char*)pg_malloc_zero(len_fcmd);
        nRet = is_local_node ? snprintf_s(fcmd, len_fcmd, len_fcmd - 1, "source %s; %s 2>&1", mpprvFile, cmd) :
            snprintf_s(fcmd, len_fcmd, len_fcmd - 1, "pssh -s -H %s \"source %s; %s\" 2>&1", nodename, mpprvFile, cmd);
    }
    securec_check_ss_c(nRet, fcmd, "\0");

    curr_cxt->pfp = popen(fcmd, "r");
    GS_FREE(fcmd);
    GS_FREE(mpprvFile);

    if (NULL != curr_cxt->pfp) {
        g_cur_commands_parallel++;
        uint32 flags;
        int fd = fileno(curr_cxt->pfp);
        flags = fcntl(fd, F_GETFL, 0);
        flags |= O_NONBLOCK;
        (void)fcntl(fd, F_SETFL, flags);
    }
    else {
        g_incorrect_nodeInfo->nodename_array[g_incorrect_nodeInfo->num++] = xstrdup(curr_cxt->nodename);
    }
    return;
}

/*
 * read popen output parallel
 */
static void readPopenOutputParallel(const char* cmd, bool if_for_all_instance)
{
    int rc = 0;
    int idx = 0;
    bool read_pending = true;
    char* result = NULL;
    PARALLEL_COMMAND_S* curr_cxt = g_parallel_command_cxt;
    int i = 0;
    char* endsp = NULL;
    int successNumber = 0;
    int failedNumber = 0;
    int instance_nums = 0;
    uint32 ret = 0;

    if (nodetype == INSTANCE_COORDINATOR) {
        instance_nums = get_all_coordinator_num();
        write_stderr("Begin to perform gs_guc for coordinators.\n");
    } else if (nodetype == INSTANCE_DATANODE) {
        instance_nums = get_all_datanode_num();
        write_stderr("Begin to perform gs_guc for datanodes.\n");
    } else if (nodetype == INSTANCE_CMSERVER) {
        instance_nums = get_all_cmserver_num();
        write_stderr("Begin to perform gs_guc for cm_servers.\n");
    } else if (nodetype == INSTANCE_CMAGENT) {
        instance_nums = get_all_cmagent_num();
        write_stderr("Begin to perform gs_guc for cm_agents.\n");
    } else {
        instance_nums = get_all_gtm_num();
        write_stderr("Begin to perform gs_guc for gtms.\n");
    }

    result = (char*)pg_malloc_zero(MAX_P_READ_BUF + 1);
    while (true == read_pending) {
        read_pending = false;
        for (idx = 0; idx < g_max_commands_parallel; idx++) {
            curr_cxt = g_parallel_command_cxt + idx;
            /* pipe closed, stop to read pipe */
            if (NULL == curr_cxt->pfp) {
                continue;
            }
            if (NULL == curr_cxt->nodename) {
                continue;
            }

            errno = 0;
            /* successful get some results from pipe, read again */
            if (fgets(result, MAX_P_READ_BUF - 1, curr_cxt->pfp) != NULL) {
                int len = strlen(result);
                int hasnewline = false;

                read_pending = true;
                if (len > 1 && result[len - 1] == '\n') {
                    hasnewline = true;
                } else if ((curr_cxt->cur_buf_loc + len + 1) < (int)sizeof(curr_cxt->readbuf)) {
                    rc = strncpy_s(curr_cxt->readbuf + curr_cxt->cur_buf_loc,
                        sizeof(curr_cxt->readbuf) - curr_cxt->cur_buf_loc,
                        result,
                        len + 1);
                    securec_check_c(rc, "\0", "\0");
                    curr_cxt->cur_buf_loc += len;
                    continue;
                }
                curr_cxt->readbuf[0] = '\0';
                curr_cxt->cur_buf_loc = 0;
                endsp = strstr(result, "WARNING");
                if (NULL != endsp) {
                    (void)write_stderr("%s", result);
                }
                endsp = strstr(result, "Success to perform gs_guc");
                if (NULL != endsp) {
                    successNumber++;
                    if (NULL != curr_cxt->pfp) {
                        curr_cxt->retvalue = pclose(curr_cxt->pfp);
                        curr_cxt->pfp = NULL;
                        GS_FREE(curr_cxt->nodename);
                        curr_cxt->nodename = NULL;
                    }
                }
                endsp = strstr(result, "Failure to perform gs_guc");
                if (NULL != endsp) {
                    failedNumber++;
                    g_incorrect_nodeInfo->nodename_array[g_incorrect_nodeInfo->num++] = xstrdup(curr_cxt->nodename);
                    if (NULL != curr_cxt->pfp) {
                        curr_cxt->retvalue = pclose(curr_cxt->pfp);
                        curr_cxt->pfp = NULL;
                        ret = (uint32)curr_cxt->retvalue;
                        g_remote_command_result = WEXITSTATUS(ret);
                        printExecErrorMesg(cmd, curr_cxt->nodename);
                    }
                }
            }
            /* no results currently, read again */
            else if (errno == EAGAIN) {
                read_pending = true;
                (void)SleepInMilliSec(100);
                continue;
            }
            /* failed to get results from pipe, exit */
            else {
                curr_cxt->retvalue = pclose(curr_cxt->pfp);
                curr_cxt->pfp = NULL;
                failedNumber++;
                g_incorrect_nodeInfo->nodename_array[g_incorrect_nodeInfo->num++] = xstrdup(curr_cxt->nodename);
                if (curr_cxt->retvalue != 0) {
                    ret = (uint32)curr_cxt->retvalue;
                    g_remote_command_result = WEXITSTATUS(ret);
                    printExecErrorMesg(cmd, curr_cxt->nodename);
                }
                else {
                    (void)write_stderr("Exception: Failed to get the result from the node %s.\n", curr_cxt->nodename);
                }
            }
        }

        if ((successNumber + failedNumber - g_cur_commands_parallel) >= 0) {
            break;
        }
        if (!read_pending) {
            (void)write_stderr("Exception: There are some nodes not executed. %d %d %d\n",
                successNumber, failedNumber, g_cur_commands_parallel);
        }
        (void)SleepInMilliSec(100);
    }
    (void)write_stderr("Command count is %d, Command success count is %d, Command failure count is %d.\n",
        g_cur_commands_parallel, successNumber, failedNumber);

    /*an error happend, close all commands and exit */
    if (0 != failedNumber) {
        for (idx = 0; idx < g_max_commands_parallel; idx++) {
            curr_cxt = g_parallel_command_cxt + idx;
            if (NULL == curr_cxt->pfp) {
                continue;
            }
            /*
             * Wait for other nodes to complete, otherwise there will be residual processes.
             */
            curr_cxt->retvalue = pclose(curr_cxt->pfp);
        }
        GS_FREE(result);

        if (if_for_all_instance == false) {
            (void)write_stderr("\nTotal nodes: %d. Effective nodes: %d. Failed nodes: %u.\n",
                g_cur_commands_parallel,
                successNumber + failedNumber,
                g_incorrect_nodeInfo->num);
        } else {
            (void)write_stderr(
                "\nTotal nodes: %d. Failed nodes: %u.\n", g_max_commands_parallel, g_incorrect_nodeInfo->num);
        }
        (void)write_stderr("Failed node names:\n");
        for (i = 0; i < (int32)g_incorrect_nodeInfo->num; i++) {
            (void)write_stderr("    [%s]\n", g_incorrect_nodeInfo->nodename_array[i]);
        }
    }
    GS_FREE(result);
    /*set DN command '-N all -I instance_name' return total nodes, effective nodes and failied node*/
    if (g_incorrect_nodeInfo->num == 0) {
        if (if_for_all_instance == false) {
            (void)write_stderr("\nTotal nodes: %d. Effective nodes: %d. Failed nodes: %u.\n",
                g_max_commands_parallel,
                successNumber + failedNumber,
                g_incorrect_nodeInfo->num);
        } else {
            (void)write_stderr("\nTotal instances: %d. Failed instances: 0.\n", instance_nums);
        }
    }
}

static void SleepInMilliSec(uint32_t sleepMs)
{
    struct timespec ts;
    ts.tv_sec = (sleepMs - (sleepMs % 1000)) / 1000;
    ts.tv_nsec = (sleepMs % 1000) * 1000;

    (void)nanosleep(&ts, NULL);
}
/*
 ******************************************************************************
 Function    : create_tmp_dir
 Description : create a temp directory
 Input       : pathdir           (dirctory name)
 Output      : None
 Return      : void
 ******************************************************************************
*/
void create_tmp_dir(const char* pathdir)
{
    if (NULL == pathdir) {
        (void)write_stderr(_("ERROR: failed to create a temp directory: invalid path <NULL>. \n"));
        exit(1);
    }
    /* check whether directory is exits or not */
    if (-1 == access(pathdir, F_OK)) {
        if (mkdir(pathdir, 0700) < 0) {
            (void)write_stderr(_("ERROR: could not create directory \"%s\": %s.\n"), pathdir, strerror(errno));
            exit(1);
        }
    }

    if (-1 == access(pathdir, R_OK | W_OK)) {
        (void)write_stderr(_("ERROR: Could not access the specified log path: %s\n"), pathdir);
        exit(1);
    }
}

/*
 ******************************************************************************
 Function    : remove_tmp_dir
 Description : remove the temp directory
 Input       : pathdir           (dirctory name)
 Output      : None
 Return      : void
 ******************************************************************************
*/
void remove_tmp_dir(const char* pathdir)
{
    char cmd[MAXPGPATH] = {0};
    int nRet = 0;
    if (-1 != access(pathdir, R_OK | W_OK)) {
        nRet = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1, "rm -rf %s", pathdir);
        securec_check_ss_c(nRet, "", "");
        nRet = gs_system(cmd);
        if (nRet != 0) {
            (void)write_stderr(_("ERROR: Could not delete directory \"%s\": %s\n"), pathdir, strerror(errno));
            exit(1);
        }
    }
}
/*
 ******************************************************************************
 Function    : execute_guc_command_in_remote_node
 Description :
 Input       : idx - node id index
               command - gs_guc execute command
 Output      : None
 Return      : None
 ******************************************************************************
*/
int execute_guc_command_in_remote_node(int idx, char* command)
{
    char* nodename = getnodename(idx);
    char* fcmd = NULL;
    char* mpprvFile = NULL;
    size_t len_fcmd = strlen(command) + strlen(nodename) + NAMEDATALEN + MAXPGPATH;
    int nRet = 0;
    uint32 ret = 0;
    /* the temp directory that stores gs_guc result information */
    char gausshome[MAXPGPATH] = {0};
    char sshlogpathdir[MAXPGPATH] = {0};
    char result_file[MAXPGPATH] = {0};
    int pid = 0;
    time_t tick;

    if (!get_env_value("GAUSSHOME", gausshome, sizeof(gausshome) / sizeof(char)))
        return 1;
    check_env_value(gausshome);
    /* get the ssh log directory */
    pid = getpid();
    tick = time(NULL);
    nRet = snprintf_s(sshlogpathdir, MAXPGPATH, MAXPGPATH - 1, "%s/gs_guc_psshlog_%d_%d", gausshome, (int)tick, pid);
    securec_check_ss_c(nRet, "", "");

    /* create ssh log directory */
    create_tmp_dir((const char*)sshlogpathdir);
    /* get the ssh result file name */
    nRet = snprintf_s(result_file, MAXPGPATH, MAXPGPATH - 1, "%s/%s", sshlogpathdir, nodename);
    securec_check_ss_c(nRet, "", "");

    mpprvFile = GetEnvStr("MPPDB_ENV_SEPARATE_PATH");
    /* execute gs_guc commands by 'ssh' */
    if (mpprvFile == NULL) {
        fcmd = (char*)pg_malloc_zero(len_fcmd);
        if (0 != strncmp(g_local_node_name,
                     nodename,
                     strlen(g_local_node_name) > strlen(nodename) ? strlen(g_local_node_name) : strlen(nodename))) {
            nRet = snprintf_s(
                fcmd, len_fcmd, len_fcmd - 1, "pssh -s -H %s \"%s\" >%s 2>&1", nodename, command, result_file);
        } else {
            nRet = snprintf_s(fcmd, len_fcmd, len_fcmd - 1, "%s >%s 2>&1", command, result_file);
        }
    } else {
        if (MAXPGPATH <= strlen(mpprvFile)) {
            write_stderr("ERROR: The value of environment variable \"MPPDB_ENV_SEPARATE_PATH\" is too long.");
            GS_FREE(mpprvFile);
            return 1;
        }
        check_env_value(mpprvFile);
        len_fcmd = len_fcmd + (int)strlen(mpprvFile);
        fcmd = (char*)pg_malloc_zero(len_fcmd);
        if (0 != strncmp(g_local_node_name,
                     nodename,
                     strlen(g_local_node_name) > strlen(nodename) ? strlen(g_local_node_name) : strlen(nodename))) {
            nRet = snprintf_s(fcmd,
                len_fcmd,
                len_fcmd - 1,
                "pssh -s -H %s \"source %s; %s\" >%s 2>&1",
                nodename,
                mpprvFile,
                command,
                result_file);
        } else {
            nRet = snprintf_s(fcmd, len_fcmd, len_fcmd - 1, "source %s; %s >%s 2>&1", mpprvFile, command, result_file);
        }
    }
    securec_check_ss_c(nRet, fcmd, "\0");
    ret = (uint32)gs_system(fcmd);
    g_remote_command_result = WEXITSTATUS(ret);
    printExecErrorMesg(fcmd, nodename);
    GS_FREE(fcmd);
    GS_FREE(mpprvFile);

    if (g_remote_command_result == 255 || g_remote_command_result == 127) {
        g_remote_connection_signal = false;
        g_incorrect_nodeInfo->nodename_array[g_incorrect_nodeInfo->num++] = xstrdup(nodename);
        remove_tmp_dir(sshlogpathdir);
        return 1;
    }

    if (g_remote_command_result == 1) {
        /* save expect instance information into global parameter */
        save_remote_instance_info(result_file, nodename, command, g_expect_gucInfo, false);
    } else {
        /* save expect and real instance information into global parameter */
        save_remote_instance_info(result_file, nodename, command, g_expect_gucInfo, false);
        save_remote_instance_info(result_file, nodename, command, g_real_gucInfo, true);
    }
    remove_tmp_dir(sshlogpathdir);
    return 0;
}

/*
 ******************************************************************************
 Function    : is_information_exists
 Description : check the instance information whether have been storaged or not
 Input       : nodename - node name
               gucfile -  the instance guc config file
 Output      : None
 Return      : true            the information has been in global parameter
               false           the information doesn't not in global parameter
 ******************************************************************************
*/
bool is_information_exists(const char* nodename, const char* gucfile)
{
    uint32 i = 0;

    for (i = 0; i < g_real_gucInfo->nodename_num; i++) {
        /* We must makesure that the parameter is exactly equal to array value. So strncmp cann't be used. */
        if (NULL != g_real_gucInfo->nodename_array[i] && NULL != g_real_gucInfo->gucinfo_array[i]) {
            if ((0 == strcmp(g_real_gucInfo->nodename_array[i], nodename)) &&
                (0 == strcmp(g_real_gucInfo->gucinfo_array[i], gucfile)))
                return true;
        }
    }
    return false;
}

/*
 ******************************************************************************
 Function    : get_keywords
 Description : get keywords from the command, the information is used for analysis gs_guc
 Input       : command
 Output      : None
 Return      : keywords
 ******************************************************************************
*/
char* get_keywords(char* command)
{
    char* keywords = NULL;

    /* get keywords by action type */
    if (!is_hba_conf) {
        if (strstr(command, " set ") != NULL)
            keywords = xstrdup("gs_guc set:");
        else if (strstr(command, " reload ") != NULL)
            keywords = xstrdup("gs_guc reload:");
        else
            keywords = xstrdup("gs_guc check:");
    } else {
        if (strstr(command, " set ") != NULL)
            keywords = xstrdup("gs_guc sethba:");
        else
            keywords = xstrdup("gs_guc reloadhba:");
    }

    return keywords;
}

void save_parameter_info(char* buffer, gucInfo* guc_info)
{
    char* p1 = NULL;
    char* p = NULL;

    char* tmp_str = NULL;
    char* ptr = NULL;
    char* outer_ptr = NULL;
    /*
     * The result type of check
     *     expected guc information: NodeName: max_connections=NULL: [$PATH]
     *     gs_guc check: NodeName: pamameter=value: [$PATH]
     */
    /* get the second ':' position */
    p1 = strstr(buffer, ":");
    if (NULL == p1)
        return;
    p1++;
    p = strstr(p1, ":");
    if (NULL == p)
        return;
    p++;

    /**skip the space and goto the begining of parameter position*/
    while (isspace((unsigned char)*p))
        p++;
    tmp_str = xstrdup(p);

    /*
     * split with ':', get the result information "parameter=value"
     * split with '=', get parameter and value
     * both this two, we can makesure the point ptr is not NULL.
     */
    ptr = strrchr(tmp_str, ':');
    if (NULL == ptr) {
        GS_FREE(tmp_str);
        return;
    }
    *ptr = '\0';
    ptr = strtok_r(tmp_str, "=", &outer_ptr);
    if (NULL == ptr) {
        GS_FREE(tmp_str);
        return;
    }

    guc_info->paramname_array[guc_info->paramname_num++] = xstrdup(ptr);
    guc_info->paramvalue_array[guc_info->paramvalue_num++] = xstrdup(outer_ptr);

    GS_FREE(tmp_str);
}
/*
 ******************************************************************************
 Function    : save_remote_instance_info
 Description : save the instance information which parse from the result file that
               do remote gs_guc set/reload  into global parameter
 Input       : nodename    - node name
               result_file -  the instance guc config file
               command     -  the execute commands
               gucInfo     - struct of guc information
               isRealGucInfo     - the struct kind
 Output      : None
 Return      : void
 ******************************************************************************
*/
void save_remote_instance_info(
    const char* result_file, const char* nodename, char* command, gucInfo* guc_info, bool isRealGucInfo)
{
    char** all_lines = NULL;
    char** line = NULL;
    char gucfile[MAXPGPATH] = {0};
    char* keywords = NULL;
    char* p = NULL;
    char* tmp_str = NULL;  // tmp string information
    bool is_found = false;
    int nRet = 0;

    /* read all informations into buffer */
    all_lines = readfile(result_file, 0);
    if (NULL == all_lines) {
        write_stderr(_("Failed to read file %s. ERROR: %s\n"), result_file, strerror(errno));
        exit(1);
    }

    if (isRealGucInfo)
        keywords = get_keywords(command);
    else
        keywords = xstrdup("expected ");
    if (NULL == keywords) {
        write_stderr(_("Failed to get key words.\n"));
        exit(1);
    }

    line = all_lines;
    while (*line != NULL) {
        if (strstr(*line, keywords) != NULL) {
            p = *line;
            is_found = false;
            if ((int)strlen(p) > MAX_VALUE_LEN) {
                (void)write_stderr(_("ERROR: The content of line is too long. Please check and make sure it is "
                                     "correct.\nThe content is \"%s\".\n"), p);
                exit(1);
            }

            /*
             * If we want to parse the result, we must find the position of '['
             * The result type of set/reload
             *     expected instance path: [$PATH]
             *     gs_guc set: pamameter=value: [$PATH]
             * The result type of check
             *     expected guc information: NodeName: max_connections=NULL: [$PATH]
             *     gs_guc check: NodeName: pamameter=value: [$PATH]
             */
            while (*p && !(*p == '\n' || *p == '[')) {
                if (*p == '\'') {
                    while (*(++p) && !(*p == '\n' || *p == '\''));
                }
                p++;
            }

            if (*p == '[') {
                is_found = true;
                p++;
            }
            /*skill space*/
            while (isspace((unsigned char)*p))
                p++;

            /* the gucconfig path is startwith '/' */
            if (is_found && *p == '/') {
                /* If the gucconfig value in the following format, then skip it*/
                if (0 == strncmp(p, "/postgresql.conf", strlen("/postgresql.conf")) ||
                    0 == strncmp(p, "/pg_hba.conf", strlen("/pg_hba.conf")) ||
                    0 == strncmp(p, "/cm_server.conf", strlen("/cm_server.conf")) ||
                    0 == strncmp(p, "/cm_agent.conf", strlen("/cm_agent.conf")) ||
                    0 == strncmp(p, "/gtm.conf", strlen("/gtm.conf")))
                    continue;

                /*the gs_guc result information is "[gucconfig]\n", so remove ']\n' first.*/
                nRet = strncpy_s(gucfile, sizeof(gucfile) / sizeof(char), p, ((int)strlen(p) - 2));
                securec_check_c(nRet, "\0", "\0");
                if (CHECK_CONF_COMMAND == ctl_command) {
                    guc_info->nodename_array[guc_info->nodename_num++] = xstrdup(nodename);
                    guc_info->gucinfo_array[guc_info->gucinfo_num++] = xstrdup(gucfile);

                    tmp_str = xstrdup(*line);
                    (void)save_parameter_info(tmp_str, guc_info);
                    GS_FREE(tmp_str);
                } else {
                    if (!is_information_exists(nodename, gucfile)) {
                        guc_info->nodename_array[guc_info->nodename_num++] = xstrdup(nodename);
                        guc_info->gucinfo_array[guc_info->gucinfo_num++] = xstrdup(gucfile);
                    }
                }
            }
        }
        line++;
    }

    GS_FREE(keywords);
    freefile(all_lines);
}

/*
 ******************************************************************************
 Function    : get_guc_option
 Description : write guc option informations into guc_opt
 ******************************************************************************
*/
char** get_guc_option()
{
    char** guc_line_info = NULL;

    if (nodetype == INSTANCE_COORDINATOR) {
        guc_line_info = get_guc_line_info((const char**)cndn_guc_info);
    } else if (nodetype == INSTANCE_DATANODE) {
        if (NULL != g_lcname) {
            guc_line_info = get_guc_line_info((const char**)lc_guc_info);
        } else {
            guc_line_info = get_guc_line_info((const char**)cndn_guc_info);
        }
    } else if (nodetype == INSTANCE_CMSERVER) {
        guc_line_info = get_guc_line_info((const char**)cmserver_guc_info);
    } else if (nodetype == INSTANCE_CMAGENT) {
        guc_line_info = get_guc_line_info((const char**)cmagent_guc_info);
    } else if (nodetype == INSTANCE_GTM) {
        guc_line_info = get_guc_line_info((const char**)gtm_guc_info);
    } else {
        write_stderr(_("%s: unrecognized -Z parameter.\n"), progname);
        exit(1);
    }

    return guc_line_info;
}

/*
 ************************************************************************************
 Function: get_guc_line_info
 Desc    : get guc parameter infomation
 ************************************************************************************
*/
char** get_guc_line_info(const char** optlines)
{
    int nRet = 0;
    int i = 0;
    int j = 0;
    char* p = NULL;
    char* q = NULL;
    char tmp_paraname[MAX_PARAM_LEN] = {0};
    char new_paraname[MAX_PARAM_LEN] = {0};
    int paramlen = 0;
    char** guc_opt = NULL;

    // allocate memory
    guc_opt = (char**)pg_malloc_zero(config_param_number * sizeof(char*));
    for (i = 0; i < config_param_number; i++) {
        guc_opt[i] = (char*)pg_malloc_zero(MAX_LINE_LEN * sizeof(char));
    }

    // Check the parameters
    if (NULL == optlines) {
        (void)write_stderr("ERROR: Faile to read file \"%s\".\n", "cluster_guc.conf");

        for (i = 0; i < config_param_number; i++) {
            GS_FREE(guc_opt[i]);
        }
        GS_FREE(guc_opt);

        return NULL;
    }

    for (i = 0; optlines[i] != NULL; i++) {
        p = (char*)optlines[i];
        // remove the spaces in the string
        while (isspace((unsigned char)*p))
            p++;

        q = p;

        if (*p == '#' || *p == '[')
            continue;

        for (j = 0; j < config_param_number; j++) {
            nRet = memset_s(tmp_paraname, MAX_PARAM_LEN, '\0', MAX_PARAM_LEN);
            securec_check_c(nRet, "\0", "\0");
            make_string_tolower(config_param[j], tmp_paraname, sizeof(tmp_paraname) / sizeof(char));
            nRet = snprintf_s(new_paraname, MAX_PARAM_LEN, MAX_PARAM_LEN - 1, "%s|", tmp_paraname);
            securec_check_ss_c(nRet, "\0", "\0");

            paramlen = strnlen(new_paraname, MAX_PARAM_LEN);
            if (0 != strncmp(p, new_paraname, paramlen))
                continue;

            nRet = snprintf_s(guc_opt[j], MAX_LINE_LEN, MAX_LINE_LEN - 1, "%s", q);
            securec_check_ss_c(nRet, "\0", "\0");
        }
    }

    return guc_opt;
}

/*
 ************************************************************************************
 Function: get_guc_type
 Desc    : get guc parameter type
 Return  : UnitType
 ************************************************************************************
*/
GucParaType get_guc_type(const char* type)
{
    if (0 == strncmp(type, "bool", strlen("bool")))
        return GUC_PARA_BOOL;
    else if (0 == strncmp(type, "real", strlen("real")))
        return GUC_PARA_REAL;
    else if (0 == strncmp(type, "int", strlen("int")))
        return GUC_PARA_INT;
    else if (0 == strncmp(type, "enum", strlen("enum")))
        return GUC_PARA_ENUM;
    else if (0 == strncmp(type, "string", strlen("string")))
        return GUC_PARA_STRING;
    else if (0 == strncmp(type, "int64", strlen("int64")))
        return GUC_PARA_INT64;
    else
        return GUC_PARA_ERROR;
}

/*
 ************************************************************************************
 Function: get_guc_unit
 Desc    : get guc parameter unit
 Return  : UnitType
 ************************************************************************************
*/
UnitType get_guc_unit(const char* unit)
{
    if (0 == strncmp(unit, "kB", strlen("kB")))
        return UNIT_KB;
    else if (0 == strncmp(unit, "MB", strlen("MB")))
        return UNIT_MB;
    else if (0 == strncmp(unit, "GB", strlen("GB")))
        return UNIT_GB;
    else if (0 == strncmp(unit, "ms", strlen("ms")))
        return UNIT_MS;
    else if (0 == strncmp(unit, "s", strlen("s")))
        return UNIT_S;
    else if (0 == strncmp(unit, "min", strlen("min")))
        return UNIT_MIN;
    else if (0 == strncmp(unit, "h", strlen("h")))
        return UNIT_H;
    else if (0 == strncmp(unit, "d", strlen("d")))
        return UNIT_D;
    else
        return UNIT_ERROR;
}

/*
 ************************************************************************************
 Function: do_gucopt_parse
 Desc    : according to guc option line information, parse them into struct
           guc_config_enum_entry
 Return  : SUCCESS
           FAILURE
 ************************************************************************************
*/
int do_gucopt_parse(const char* guc_opt, struct guc_config_enum_entry& guc_variable_list)
{
    char opts[MAX_LINE_LEN];
    int nRet = 0;
    char* ptr = NULL;
    char* outer_ptr = NULL;
    char delims[] = "|";
    GucParaType type_val = GUC_PARA_ERROR;

    nRet = memset_s(opts, MAX_LINE_LEN, '\0', MAX_LINE_LEN);
    securec_check_c(nRet, "\0", "\0");
    nRet = snprintf_s(opts, MAX_LINE_LEN, MAX_LINE_LEN - 1, "%s", guc_opt);
    securec_check_ss_c(nRet, "\0", "\0");

    /* guc_name */
    ptr = strtok_r(opts, delims, &outer_ptr);
    if (NULL != ptr) {
        nRet = snprintf_s(guc_variable_list.guc_name, MAX_PARAM_LEN, MAX_PARAM_LEN - 1, "%s", ptr);
        securec_check_ss_c(nRet, "\0", "\0");
    }

    /* guc_type */
    ptr = strtok_r(NULL, delims, &outer_ptr);
    if (NULL != ptr) {
        type_val = get_guc_type(ptr);
        if (GUC_PARA_ERROR == type_val) {
            (void)write_stderr("ERROR: Failed to parse the guc \"%s\" option. The type \"%s\" is incorrect.\n",
                guc_variable_list.guc_name,
                ptr);
            return FAILURE;
        }
        guc_variable_list.type = type_val;
    }

    /* guc_value */
    ptr = strtok_r(NULL, delims, &outer_ptr);
    if (NULL != ptr) {
        nRet = snprintf_s(guc_variable_list.guc_value, MAX_VALUE_LEN, MAX_VALUE_LEN - 1, "%s", ptr);
        securec_check_ss_c(nRet, "\0", "\0");
    } else {
        (void)write_stderr(
            "ERROR: Failed to parse the guc \"%s\" option. The value range information \"%s\" is incorrect.\n",
            guc_variable_list.guc_name,
            ptr);
        return FAILURE;
    }

    /* guc_unit */
    ptr = strtok_r(NULL, delims, &outer_ptr);
    if (NULL != ptr) {
        if (0 == strncmp(ptr, "NULL", strlen("NULL"))) {
            nRet = memset_s(guc_variable_list.guc_unit, MAX_UNIT_LEN, '\0', MAX_UNIT_LEN);
            securec_check_c(nRet, "\0", "\0");
        } else {
            nRet = snprintf_s(guc_variable_list.guc_unit, MAX_UNIT_LEN, MAX_UNIT_LEN - 1, "%s", ptr);
            securec_check_ss_c(nRet, "\0", "\0");
        }
    } else {
        (void)write_stderr("ERROR: Failed to parse the guc \"%s\" option. The parameter unit is incorrect.\n",
            guc_variable_list.guc_name);
        return FAILURE;
    }

    /* guc_message */
    ptr = strtok_r(NULL, delims, &outer_ptr);
    if (NULL != ptr) {
        if (0 == strncmp(ptr, "NULL", strlen("NULL"))) {
            nRet = memset_s(guc_variable_list.message, MAX_MESG_LEN, '\0', MAX_MESG_LEN);
            securec_check_c(nRet, "\0", "\0");
        } else {
            nRet = snprintf_s(guc_variable_list.message, MAX_MESG_LEN, MAX_MESG_LEN - 1, "%s", ptr);
            securec_check_ss_c(nRet, "\0", "\0");
        }
    } else {
        (void)write_stderr(
            "ERROR: Failed to parse the guc \"%s\" option. The parameter relation message is incorrect.\n",
            guc_variable_list.guc_name);
        return FAILURE;
    }

    ptr = strtok_r(NULL, delims, &outer_ptr);
    if (NULL != ptr && '\n' != ptr[0]) {
        (void)write_stderr("ERROR: The guc \"%s\" options is incorrect.\n", guc_variable_list.guc_name);
        return FAILURE;
    }

    return SUCCESS;
}
/*
 ************************************************************************************
 Function: check_parameter_name
 Desc    : according to guc option line information, check the parameter name
           guc_opt      guc information list
 Return  : SUCCESS
           FAILURE
 ************************************************************************************
*/
int check_parameter_name(char** guc_opt, int type)
{
    int i = 0;
    bool is_failed = false;

    if (false == check_parameter_is_valid(type))
        return FAILURE;
    else
        return SUCCESS;

    for (i = 0; i < config_param_number; i++) {
        if (NULL == guc_opt[i] || '\0' == guc_opt[i][0]) {
            is_failed = true;
            (void)write_stderr("ERROR: The name of parameter \"%s\" is incorrect. Please check if the parameters are "
                               "within the required range.\n",
                config_param[i]);
        }
    }

    if (is_failed)
        return FAILURE;
    else
        return SUCCESS;
}
/*
 ************************************************************************************
 Function: check_parameter_is_valid
 Desc    : according to guc option line information, check the parameter name
           guc_opt      guc information list
 Return  : SUCCESS
           FAILURE
 ************************************************************************************
*/
bool check_cn_dn_parameter_is_valid()
{
    int para_num = 0;
    bool is_valid = false;
    bool all_valid = true;
    int len = 0;

    char tmp[MAX_PARAM_LEN] = {0};
    int nRet = 0;

    for (para_num = 0; para_num < config_param_number; para_num++) {
        nRet = memset_s(tmp, MAX_PARAM_LEN, '\0', MAX_PARAM_LEN);
        securec_check_c(nRet, "\0", "\0");
        make_string_tolower(config_param[para_num], tmp, sizeof(tmp) / sizeof(char));

        is_valid = false;
        if (NULL != g_lcname) {
            for (int i = 0; i < lc_param_number; i++) {
                len = strlen(tmp) > strlen(lc_param[i]) ? strlen(tmp) : strlen(lc_param[i]);
                if (0 == strncmp(tmp, lc_param[i], len)) {
                    is_valid = true;
                    break;
                }
            }
            if (is_valid == false) {
                (void)write_stderr("ERROR: The name of parameter \"%s\" is incorrect. It is not within the logical "
                                   "cluster support parameters.\n",
                    config_param[para_num]);
                all_valid = false;
            }
        } else {
            for (int i = 0; i < cndn_param_number; i++) {
                len = strlen(tmp) > strlen(cndn_param[i]) ? strlen(tmp) : strlen(cndn_param[i]);
                if (0 == strncmp(tmp, cndn_param[i], len)) {
                    is_valid = true;
                    break;
                }
            }
            if (is_valid == false) {
                (void)write_stderr("ERROR: The name of parameter \"%s\" is incorrect. It is not within the CN/DN "
                                   "support parameters or it is a read only parameter.\n",
                    config_param[para_num]);
                all_valid = false;
            } else if (strncmp(tmp, "enableseparationofduty", strlen("enableseparationofduty")) == 0) {
                /* for enableSeparationOfDuty, we give warning */
                (void)write_stderr("WARNING: please take care of the actual privileges of the users "
                                   "while changing enableSeparationOfDuty.\n");
            }
#ifndef USE_ASSERT_CHECKING
            /* distribute_test_param only work on debug mode */
            char* distribute_test_param = "distribute_test_param";
            len = strlen(tmp) > strlen(distribute_test_param) ? strlen(tmp) : strlen(distribute_test_param);
            if (0 == strncmp(tmp, distribute_test_param, len)) {
                all_valid = false;
                (void)write_stderr("ERROR: The name of parameter \"%s\" is incorrect."
                                   "not work on this mode.\n",
                    config_param[para_num]);
            }
            /* segment_test_param only work on debug mode */
            char* segmentTestParam = "segment_test_param";
            len = (strlen(tmp) > strlen(segmentTestParam)) ? strlen(tmp) : strlen(segmentTestParam);
            if (strncmp(tmp, segmentTestParam, len) == 0) {
                all_valid = false;
                (void)write_stderr("ERROR: The name of parameter \"%s\" is incorrect."
                                "not work on this mode.\n",
                    config_param[para_num]);
            }
            /* enable_memory_context_check_debug  only work on debug mode */
            char* memCtxCheckParam = "enable_memory_context_check_debug";
            len = (strlen(tmp) > strlen(memCtxCheckParam)) ? strlen(tmp) : strlen(memCtxCheckParam);
            if (strncmp(tmp, memCtxCheckParam, len) == 0) {
                all_valid = false;
                (void)write_stderr("ERROR: The name of parameter \"%s\" is incorrect."
                                "not work on this mode.\n",
                    config_param[para_num]);
            }
#endif
#ifdef ENABLE_FINANCE_MODE
            /* enable_ustore does not working on finance mode */
            char* enableUstore = "enable_ustore";
            len = strlen(tmp) > strlen(enableUstore) ? strlen(tmp) : strlen(enableUstore);
            if (0 == strncmp(tmp, enableUstore, len)) {
                all_valid = false;
                (void)write_stderr("ERROR: The name of parameter \"%s\" is incorrect. "
                                   "not work on finance mode.\n",
                    config_param[para_num]);
            }
            /* enable_tde does not working on finance mode */
            char* enableTde = "enable_tde";
            len = (strlen(tmp) > strlen(enableTde)) ? strlen(tmp) : strlen(enableTde);
            if (strncmp(tmp, enableTde, len) == 0) {
                all_valid = false;
                (void)write_stderr("ERROR: The name of parameter \"%s\" is incorrect. "
                                "not work on finance mode.\n",
                    config_param[para_num]);
            }
            /* query_dop does not working on finance mode */
            char* queryDop = "query_dop";
            len = (strlen(tmp) > strlen(queryDop)) ? strlen(tmp) : strlen(queryDop);
            if (strncmp(tmp, queryDop, len) == 0) {
                all_valid = false;
                (void)write_stderr("ERROR: The name of parameter \"%s\" is incorrect. "
                                "not work on finance mode.\n",
                    config_param[para_num]);
            }
            /* enable_dcf does not working on finance mode */
            char* enableDcf = "enable_dcf";
            len = (strlen(tmp) > strlen(enableDcf)) ? strlen(tmp) : strlen(enableDcf);
            if (strncmp(tmp, enableDcf, len) == 0) {
                all_valid = false;
                (void)write_stderr("ERROR: The name of parameter \"%s\" is incorrect. "
                                "not work on finance mode.\n",
                    config_param[para_num]);
            }
            /* try_vector_engine_strategy does not working on finance mode */
            char* vectorEngineStrategy = "try_vector_engine_strategy";
            len = (strlen(tmp) > strlen(vectorEngineStrategy)) ? strlen(tmp) : strlen(vectorEngineStrategy);
            if (strncmp(tmp, vectorEngineStrategy, len) == 0) {
                all_valid = false;
                (void)write_stderr("ERROR: The name of parameter \"%s\" is incorrect. "
                                "not work on finance mode.\n",
                    config_param[para_num]);
            }
#endif
        }
    }

    return all_valid;
}

bool check_gtm_parameter_is_valid()
{
    int para_num = 0;
    bool is_valid = false;
    bool all_valid = true;
    int len = 0;

    char tmp[MAX_PARAM_LEN] = {0};
    int nRet = 0;

    for (para_num = 0; para_num < config_param_number; para_num++) {
        nRet = memset_s(tmp, MAX_PARAM_LEN, '\0', MAX_PARAM_LEN);
        securec_check_c(nRet, "\0", "\0");
        make_string_tolower(config_param[para_num], tmp, sizeof(tmp) / sizeof(char));

        is_valid = false;
        for (int i = 0; i < gtm_param_number; i++) {
            len = strlen(tmp) > strlen(gtm_param[i]) ? strlen(tmp) : strlen(gtm_param[i]);
            if (0 == strncmp(tmp, gtm_param[i], len)) {
                is_valid = true;
                break;
            }
        }
        if (is_valid == false) {
            (void)write_stderr(
                "ERROR: The name of parameter \"%s\" is incorrect. It is not in the GTM parameter range\n",
                config_param[para_num]);
            all_valid = false;
        }
#ifndef USE_ASSERT_CHECKING
        /* distribute_test_param only work on debug mode */
        char* gtm_distribute_test_param = "distribute_test_param";
        len = strlen(tmp) > strlen(gtm_distribute_test_param) ? strlen(tmp) : strlen(gtm_distribute_test_param);
        if (0 == strncmp(tmp, gtm_distribute_test_param, len)) {
            all_valid = false;
            (void)write_stderr("ERROR: The name of parameter \"%s\" is incorrect."
                               "not work on this mode.\n",
                config_param[para_num]);
        }
#endif
    }

    return all_valid;
}

bool check_cm_server_parameter_is_valid()
{
    int para_num = 0;
    bool is_valid = false;
    bool all_valid = true;
    int len = 0;

    char tmp[MAX_PARAM_LEN] = {0};
    int nRet = 0;

    for (para_num = 0; para_num < config_param_number; para_num++) {
        nRet = memset_s(tmp, MAX_PARAM_LEN, '\0', MAX_PARAM_LEN);
        securec_check_c(nRet, "\0", "\0");
        make_string_tolower(config_param[para_num], tmp, sizeof(tmp) / sizeof(char));

        is_valid = false;
        for (int i = 0; i < cmserver_param_number; i++) {
            len = strlen(tmp) > strlen(cmserver_param[i]) ? strlen(tmp) : strlen(cmserver_param[i]);
            if (0 == strncmp(tmp, cmserver_param[i], len)) {
                is_valid = true;
                break;
            }
        }
        if (is_valid == false) {
            (void)write_stderr(
                "ERROR: The name of parameter \"%s\" is incorrect. It is not in the CMSERVER parameter range\n",
                config_param[para_num]);
            all_valid = false;
        }
    }

    return all_valid;
}

bool check_cm_agent_parameter_is_valid()
{
    int para_num = 0;
    bool is_valid = false;
    bool all_valid = true;
    int len = 0;

    char tmp[MAX_PARAM_LEN] = {0};
    int nRet = 0;

    for (para_num = 0; para_num < config_param_number; para_num++) {
        nRet = memset_s(tmp, MAX_PARAM_LEN, '\0', MAX_PARAM_LEN);
        securec_check_c(nRet, "\0", "\0");
        make_string_tolower(config_param[para_num], tmp, sizeof(tmp) / sizeof(char));

        is_valid = false;
        for (int i = 0; i < cmagent_param_number; i++) {
            len = strlen(tmp) > strlen(cmagent_param[i]) ? strlen(tmp) : strlen(cmagent_param[i]);
            if (0 == strncmp(tmp, cmagent_param[i], len)) {
                is_valid = true;
                break;
            }
        }
        if (is_valid == false) {
            (void)write_stderr(
                "ERROR: The name of parameter \"%s\" is incorrect. It is not in the CMAGENT parameter range\n",
                config_param[para_num]);
            all_valid = false;
        }
    }

    return all_valid;
}

bool check_parameter_is_valid(int type)
{
    bool is_valid = true;

    if (type == INSTANCE_COORDINATOR || type == INSTANCE_DATANODE) {
        is_valid = check_cn_dn_parameter_is_valid();
    } else if (type == INSTANCE_GTM) {
        is_valid = check_gtm_parameter_is_valid();
    } else if (type == INSTANCE_CMSERVER) {
        is_valid = check_cm_server_parameter_is_valid();
    } else if (type == INSTANCE_CMAGENT) {
        is_valid = check_cm_agent_parameter_is_valid();
    } else {
        is_valid = false;
        (void)write_stderr("ERROR: Node type is not correct.\n");
    }
    return is_valid;
}

/*
 ************************************************************************************
 Function: is_parameter_value_error
 Desc    : check the parameter value.
           guc_opt_str           guc information string
           config_value_str      parameter value string
           config_param_str      parameter name string
 Return  : false                the parameter name is incorrect
           true                 the parameter name is correct
 ************************************************************************************
*/
bool is_parameter_value_error(const char* guc_opt_str, char* config_value_str, char* config_param_str)
{
    struct guc_config_enum_entry guc_variable_list;
    int nRet = 0;
    int rc = 0;
    int j = 0;
    int k = 0;
    int len = 0;
    bool is_failed = false;
    char newvalue[MAX_VALUE_LEN];
    char* ch_position = NULL;

    /* init a struct guc_config_enum_entry that storage guc information */
    guc_variable_list.type = GUC_PARA_ERROR;
    nRet = memset_s(guc_variable_list.guc_name, MAX_PARAM_LEN, '\0', MAX_PARAM_LEN);
    securec_check_c(nRet, "\0", "\0");
    nRet = memset_s(guc_variable_list.guc_value, MAX_VALUE_LEN, '\0', MAX_VALUE_LEN);
    securec_check_c(nRet, "\0", "\0");
    nRet = memset_s(guc_variable_list.guc_unit, MAX_UNIT_LEN, '\0', MAX_UNIT_LEN);
    securec_check_c(nRet, "\0", "\0");
    nRet = memset_s(guc_variable_list.message, MAX_MESG_LEN, '\0', MAX_MESG_LEN);
    securec_check_c(nRet, "\0", "\0");

    /* parse the guc value string. If FAILURE, return */
    if (FAILURE == do_gucopt_parse(guc_opt_str, guc_variable_list)) {
        is_failed = true;
    } else {
        /* if message is not NULL, print it */
        if ('\0' != guc_variable_list.message[0])
            (void)write_stderr("NOTICE: %s\n", guc_variable_list.message);

        if (0 == strncmp(guc_variable_list.guc_name, "comm_tcp_mode", strlen("comm_tcp_mode")))
            (void)write_stderr(
                "WARNING: If the cluster was not restarted, it can not communicate properly after dilatation.\n");

        nRet = memset_s(newvalue, MAX_VALUE_LEN, '\0', MAX_VALUE_LEN);
        securec_check_c(nRet, "\0", "\0");
        len = (int)strlen(config_value_str);

	if (guc_variable_list.type == GUC_PARA_ENUM) {
            ch_position = strchr(config_value_str,',');
            if (ch_position != NULL) {
                if (!((config_value_str[0] == '\'' || config_value_str[0] == '"') &&
                    config_value_str[0] == config_value_str[len - 1])) {
                    (void)write_stderr("ERROR: The value \"%s\" for parameter \"%s\" is incorrect. Please do it like this "
                                       "\"parameter = \'value\'\".\n",
                    config_value_str,
                    config_param_str);
                    exit(1);
                }
	    }
        }

        if (guc_variable_list.type == GUC_PARA_INT || guc_variable_list.type == GUC_PARA_REAL ||
            guc_variable_list.type == GUC_PARA_ENUM || guc_variable_list.type == GUC_PARA_BOOL) {
            /* the value like this "XXX" or 'XXXX' */
            if ((config_value_str[0] == '\'' || config_value_str[0] == '"') &&
                config_value_str[0] == config_value_str[len - 1]) {
                for (j = 1, k = 0; j < len - 1 && k < MAX_VALUE_LEN; j++, k++)
                    newvalue[k] = config_value_str[j];
            } else {
                rc = snprintf_s(newvalue, MAX_VALUE_LEN, MAX_VALUE_LEN - 1, "%s", config_value_str);
                securec_check_ss_c(rc, "\0", "\0");
            }
        } else {
            if ((config_value_str[0] == '\'' || config_value_str[0] == '"') &&
                config_value_str[0] == config_value_str[len - 1]) {
                rc = snprintf_s(newvalue, MAX_VALUE_LEN, MAX_VALUE_LEN - 1, "%s", config_value_str);
                securec_check_ss_c(rc, "\0", "\0");
            } else {
                (void)write_stderr("ERROR: The value \"%s\" for parameter \"%s\" is incorrect. Please do it like this "
                                   "\"parameter = \'value\'\".\n",
                    config_value_str,
                    config_param_str);
                exit(1);
            }
        }

        if (FAILURE == check_parameter_value(config_param_str,
                           guc_variable_list.type,
                           guc_variable_list.guc_value,
                           guc_variable_list.guc_unit,
                           newvalue)) {
            is_failed = true;

            if (guc_variable_list.type >= 0 &&
                guc_variable_list.type < (GucParaType)(sizeof(value_type_list) / sizeof(value_type_list[0]))) {
                (void) write_stderr("ERROR: The value \"%s\" for parameter \"%s\" is incorrect, requires a %s value\n",
                                    config_value_str, config_param_str, value_type_list[guc_variable_list.type]);
            } else {
                (void) write_stderr("ERROR: The value \"%s\" for parameter \"%s\" is incorrect.\n", config_value_str,
                                    config_param_str);
            }
        }
    }

    return is_failed;
}
/*
 ************************************************************************************
 Function: check_parameter
 Desc    : a interface that do patameter name and value checking.
           if value is NULL, it means that we will disable the parameter
 Return  : SUCCESS
           FAILURE
 ************************************************************************************
*/
int check_parameter(int type)
{
    char** guc_opt = NULL;
    bool is_failed = false;
    int i = 0;

    guc_opt = get_guc_option();

    /* First we must makesure that the guc information list is correct */
    if (NULL == guc_opt)
        return FAILURE;

    if (FAILURE == check_parameter_name(guc_opt, type)) {
        /* free guc_opt */
        for (i = 0; i < config_param_number; i++) {
            GS_FREE(guc_opt[i]);
        }
        GS_FREE(guc_opt);

        return FAILURE;
    }

    for (i = 0; i < config_param_number; i++) {
        /* When config value is not NULL and the value is error, set 'is_failed=true' */
        if (NULL != config_value[i] && is_parameter_value_error(guc_opt[i], config_value[i], config_param[i]))
            is_failed = true;
    }

    /* free guc_opt */
    for (i = 0; i < config_param_number; i++) {
        GS_FREE(guc_opt[i]);
    }
    GS_FREE(guc_opt);

    if (is_failed)
        return FAILURE;
    else
        return SUCCESS;
}

/*
 ************************************************************************************
 Function: check_parameter_value
 Desc    : do parameter value checking
 Input   : paraname         paraname
           type             paratype
           guc_list_value
           guc_list_unit
           value
 Return  : SUCCESS
           FAILURE
 ************************************************************************************
*/
int check_parameter_value(
    const char* paraname, GucParaType type, char* guc_list_value, const char* guc_list_unit, const char* value)
{
    if (type == GUC_PARA_INT)
        return check_int_real_type_value(paraname, guc_list_value, guc_list_unit, value, true);
    else if (type == GUC_PARA_REAL)
        return check_int_real_type_value(paraname, guc_list_value, guc_list_unit, value, false);
    else if (type == GUC_PARA_ENUM)
        return check_enum_type_value(paraname, guc_list_value, value);
    else if (type == GUC_PARA_BOOL)
        return check_bool_type_value(value);
    else if (type == GUC_PARA_STRING) {
        return check_string_type_value(paraname, value);
    }
    else
        return FAILURE;
}

/*
 ************************************************************************************
 Function: get_guc_minmax_value
 Desc    : get min and max value from guc config
 Input   : guc_list_value         value from guc config
           guc_minmax_value       a struct that storage min and max value string
 Return  : SUCCESS
           FAILURE
 ************************************************************************************
*/
int get_guc_minmax_value(const char* guc_list_val, struct guc_minmax_value& value_list)
{
    char guc_val[MAX_VALUE_LEN];
    int nRet = 0;
    char* ptr = NULL;
    char* outer_ptr = NULL;
    char delims[] = ",";

    nRet = memset_s(guc_val, MAX_VALUE_LEN, '\0', MAX_VALUE_LEN);
    securec_check_c(nRet, "\0", "\0");
    nRet = snprintf_s(guc_val, MAX_VALUE_LEN, MAX_VALUE_LEN - 1, "%s", guc_list_val);
    securec_check_ss_c(nRet, "\0", "\0");

    /* min value string */
    ptr = strtok_r(guc_val, delims, &outer_ptr);
    if (NULL != ptr) {
        nRet = snprintf_s(value_list.min_val_str, MAX_VALUE_LEN, MAX_VALUE_LEN - 1, "%s", ptr);
        securec_check_ss_c(nRet, "\0", "\0");
    } else {
        (void)write_stderr("ERROR: The minimum value information is incorrect.\n");
        return FAILURE;
    }

    /* max value string */
    ptr = strtok_r(NULL, delims, &outer_ptr);
    if (NULL != ptr) {
        nRet = snprintf_s(value_list.max_val_str, MAX_VALUE_LEN, MAX_VALUE_LEN - 1, "%s", ptr);
        securec_check_ss_c(nRet, "\0", "\0");
    } else {
        (void)write_stderr("ERROR: The maximum value information is incorrect.\n");
        return FAILURE;
    }

    ptr = strtok_r(NULL, delims, &outer_ptr);
    if (NULL != ptr) {
        (void)write_stderr("ERROR: The minmax information for parameter is incorrect.\n");
        return FAILURE;
    }

    return SUCCESS;
}

/*
 ************************************************************************************
 Function: is_alpha_in_string
 Desc    : judge the string contains alpha or not
 Input   : str
 Return  : true          the string contains alpha
           false         the string does not contain alpha
 ************************************************************************************
*/
bool is_alpha_in_string(const char* str)
{
    const char* p = str;

    while ('\0' != *p) {
        if (isalpha(*p))
            return true;

        p++;
    }
    return false;
}
/*
 ************************************************************************************
 Function: is_string_in_list
 Desc    : judge the string in list or not
 Input   : str                      string name
           str_list                 string name list
           list_nums                the length of str list
 Return  : true          the string is in value_list
           false         the string is not in value_list
 ************************************************************************************
*/
bool is_string_in_list(const char* str, const char** str_list, int list_nums)
{
    int i = 0;
    char tmp[MAX_PARAM_LEN];
    int nRet = 0;

    nRet = memset_s(tmp, MAX_PARAM_LEN, '\0', MAX_PARAM_LEN);
    securec_check_c(nRet, "\0", "\0");
    make_string_tolower(str, tmp, sizeof(tmp) / sizeof(char));

    for (i = 0; i < list_nums; i++) {
        if (0 == strcmp(str_list[i], tmp))
            return true;
    }

    return false;
}
/*
 ************************************************************************************
 Function: check_int_value
 Desc    : check the int parameter value
 Input   : paraname               parameter name
           guc_list_value         value from guc config
           value                  parameter value string, including unit
           int_newval             the parameter value, only including number
           int_min_val            the min parameter value
           int_max_val            the max parameter value
 Return  : SUCCESS
           FAILURE
 ************************************************************************************
*/
int check_int_value(const char* paraname, const struct guc_minmax_value& value_list, const char* value,
    int64 int_newval, int64 int_min_val, int64 int_max_val)
{
    /* a signal that storage whether the paraname in unit_eight_kB_parameter_list or not */
    bool is_in_list = false;
    bool is_exists_alpha = false;
    /* makesure the min/max value from guc config list file is correct */
    if ((FAILURE == parse_value(paraname, value_list.min_val_str, NULL, &int_min_val, NULL, true)) ||
        (FAILURE == parse_value(paraname, value_list.max_val_str, NULL, &int_max_val, NULL, true))) {
        (void)write_stderr("ERROR: The minmax value of parameter \"%s\" requires an integer value.\n", paraname);
        return FAILURE;
    }
    /*modify the min/max value , if the parameter unit is 8kB and the value contains unit string.
      if the unit is incorrect, when do parse_value by config_value, it will print error messages and exit.
      So, we can makesure the unit is correct, if it is exists
     */
    is_in_list = is_string_in_list(paraname, unit_eight_kB_parameter_list, lengthof(unit_eight_kB_parameter_list));
    is_exists_alpha = is_alpha_in_string(value);
    if (is_in_list && is_exists_alpha) {
        int_newval = int_newval / PAGE_SIZE;
    }
    /* if int_newval < int_min_val or int_newval > int_max_val, print error message */
    if (int_newval < int_min_val || int_newval > int_max_val) {
        if (is_in_list && is_exists_alpha) {
            (void)write_stderr(
                "Notice: The default unit for parameter \"%s\" is disk page and each page is usually %dkB.\n",
                paraname,
                PAGE_SIZE);
            (void)write_stderr("ERROR: The value \"%s\" is outside the valid range for parameter \"%s\" (" INT64_FORMAT
                               " .. " INT64_FORMAT ").\n",
                value,
                paraname,
                int_min_val,
                int_max_val);
        } else {
            (void)write_stderr("ERROR: The value " INT64_FORMAT
                               " is outside the valid range for parameter \"%s\" (" INT64_FORMAT " .. " INT64_FORMAT
                               ").\n",
                int_newval,
                paraname,
                int_min_val,
                int_max_val);
        }
        return FAILURE;
    }
    return SUCCESS;
}

/*
 ************************************************************************************
 Function: check_real_value
 Desc    : check the real parameter value
 Input   : paraname               parameter name
           guc_list_value         value from guc config
           double_newval             the parameter value
           double_min_val            the min parameter value
           double_max_val            the max parameter value
 Return  : SUCCESS
           FAILURE
 ************************************************************************************
*/
int check_real_value(const char* paraname, const struct guc_minmax_value& value_list, double double_newval,
    double double_min_val, double double_max_val)
{
    /* makesure the min/max value from guc config list file is correct */
    if ((FAILURE == parse_value(paraname, value_list.min_val_str, NULL, NULL, &double_min_val, false)) ||
        (FAILURE == parse_value(paraname, value_list.max_val_str, NULL, NULL, &double_max_val, false))) {
        (void)write_stderr("ERROR: The minmax value of parameter \"%s\" requires a numeric value.\n", paraname);
        return FAILURE;
    }
    /* if double_newval < double_min_val - DOUBLE_PRECISE or double_newval > double_max_val + DOUBLE_PRECISE, print
     * error message */
    if (double_newval < double_min_val - DOUBLE_PRECISE || double_newval > double_max_val + DOUBLE_PRECISE) {
        (void)write_stderr("ERROR: The value %g is outside the valid range for parameter \"%s\" (%g .. %g).\n",
            double_newval,
            paraname,
            double_min_val,
            double_max_val);
        return FAILURE;
    }
    return SUCCESS;
}

/*
 ************************************************************************************
 Function: check_int_real_type_value
 Desc    : check the int/real parameter value
 Input   : paraname               parameter name
           guc_list_value         value from guc config
           value                  parameter value
           isInt                  true is int, false is real
 Return  : SUCCESS
           FAILURE
 ************************************************************************************
*/
int check_int_real_type_value(
    const char* paraname, const char* guc_list_value, const char* guc_list_unit, const char* value, bool isInt)
{
    int64 int_newval = INT_MIN;
    int64 int_min_val = LLONG_MIN;
    int64 int_max_val = LLONG_MAX;
    double double_newval = LLONG_MIN;
    double double_min_val = LLONG_MIN;
    double double_max_val = LLONG_MIN;
    struct guc_minmax_value value_list;
    int nRet = 0;

    /* init a struct guc_minmax_value that storage guc min/max value information */
    nRet = memset_s(value_list.min_val_str, MAX_VALUE_LEN, '\0', MAX_VALUE_LEN);
    securec_check_c(nRet, "\0", "\0");
    nRet = memset_s(value_list.max_val_str, MAX_VALUE_LEN, '\0', MAX_VALUE_LEN);
    securec_check_c(nRet, "\0", "\0");

    /* parse int_newval/double_newval value*/
    if (FAILURE == parse_value(paraname, value, guc_list_unit, &int_newval, &double_newval, isInt)) {
        if (isInt)
            (void)write_stderr("ERROR: The parameter \"%s\" requires an integer value.\n", paraname);
        else
            (void)write_stderr("ERROR: The parameter \"%s\" requires a numeric value.\n", paraname);
        return FAILURE;
    }

    /* get min/max value from guc config file */
    if (FAILURE == get_guc_minmax_value(guc_list_value, value_list))
        return FAILURE;

    if ('\0' == value_list.min_val_str[0] || '\0' == value_list.max_val_str[0]) {
        (void)write_stderr("ERROR: The minmax information for parameter \"%s\" is incorrect.\n", paraname);
        return FAILURE;
    }

    if (isInt)
        return check_int_value(paraname, value_list, value, int_newval, int_min_val, int_max_val);
    else
        return check_real_value(paraname, value_list, double_newval, double_min_val, double_max_val);
}

/*
 ************************************************************************************
 Function: parse_value
 Desc    : parese value from guc config file.
           paraname          parameter name
           value             parameter value
           guc_list_unit     the unit of parameter from guc config file
           result_int        the parse result about int
           result_double        the parse result about double
           isInt                  true is int, false is real
 Return  : SUCCESS
           FAILURE
 ************************************************************************************
*/
int parse_value(const char* paraname, const char* value, const char* guc_list_unit, int64* result_int,
    double* result_double, bool isInt)
{
    int64 int_val = INT_MIN;
    double double_val;
    long double tmp_double_val;
    char* endptr = NULL;
    UnitType unitval = UNIT_ERROR;
    bool contain_space = false;

    if (NULL != result_int)
        *result_int = 0;
    if (NULL != result_double)
        *result_double = 0;

    errno = 0;
    if (isInt) {
        /* transform value into long int */
        int_val = strtoll(value, &endptr, 0);
        if (endptr == value || errno == ERANGE)
            return FAILURE;
        tmp_double_val = (long double)int_val;
    } else {
        /* transform value into double */
        double_val = strtod(value, &endptr);
        if (endptr == value || errno == ERANGE)
            return FAILURE;
        tmp_double_val = (long double)double_val;
    }

    /* skill the blank */
    while (isspace((unsigned char)*endptr)) {
        endptr++;
        contain_space = true;
    }

    if ('\0' != *endptr) {
        /* if unit is NULL, it means the value is incorrect */
        if (NULL == guc_list_unit || '\0' == guc_list_unit[0])
            return FAILURE;

        if (contain_space) {
            (void)write_stderr("ERROR: There should not hava space between value and unit.\n");
            return FAILURE;
        }

        unitval = get_guc_unit(guc_list_unit);
        if (UNIT_ERROR == unitval) {
            (void)write_stderr("ERROR: Invalid units for this parameter \"%s\".\n", paraname);
            return FAILURE;
        } else if (UNIT_KB == unitval) {
            if (strncmp(endptr, "kB", 2) == 0) {
                endptr += 2;
            } else if (strncmp(endptr, "MB", 2) == 0) {
                endptr += 2;
                tmp_double_val *= KB_PER_MB;
            } else if (strncmp(endptr, "GB", 2) == 0) {
                endptr += 2;
                tmp_double_val *= KB_PER_GB;
            } else {
                (void)write_stderr(
                    "ERROR: Valid units for this parameter \"%s\" are \"kB\", \"MB\" and \"GB\".\n", paraname);
                return FAILURE;
            }
        } else if (UNIT_MB == unitval) {
            if (strncmp(endptr, "MB", 2) == 0) {
                endptr += 2;
            } else if (strncmp(endptr, "GB", 2) == 0) {
                endptr += 2;
                tmp_double_val *= MB_PER_GB;
            } else {
                (void)write_stderr("ERROR: Valid units for this parameter \"%s\" are \"MB\" and \"GB\".\n", paraname);
                return FAILURE;
            }
        } else if (UNIT_GB == unitval) {
            if (strncmp(endptr, "GB", 2) == 0) {
                endptr += 2;
            } else {
                (void)write_stderr("ERROR: Valid units for this parameter \"%s\" is \"GB\".\n", paraname);
                return FAILURE;
            }
        } else if (UNIT_MS == unitval) {
            if (strncmp(endptr, "ms", 2) == 0) {
                endptr += 2;
            } else if (strncmp(endptr, "s", 1) == 0) {
                endptr += 1;
                tmp_double_val *= MS_PER_S;
            } else if (strncmp(endptr, "min", 3) == 0) {
                endptr += 3;
                tmp_double_val *= MS_PER_MIN;
            } else if (strncmp(endptr, "h", 1) == 0) {
                endptr += 1;
                tmp_double_val *= MS_PER_H;
            } else if (strncmp(endptr, "d", 1) == 0) {
                endptr += 1;
                tmp_double_val *= MS_PER_D;
            } else {
                (void)write_stderr(
                    "ERROR: Valid units for this parameter \"%s\" are \"ms\", \"s\", \"min\", \"h\", and \"d\".\n",
                    paraname);
                return FAILURE;
            }
        } else if (UNIT_S == unitval) {
            if (strncmp(endptr, "s", 1) == 0) {
                endptr += 1;
            } else if (strncmp(endptr, "min", 3) == 0) {
                endptr += 3;
                tmp_double_val *= S_PER_MIN;
            } else if (strncmp(endptr, "h", 1) == 0) {
                endptr += 1;
                tmp_double_val *= S_PER_H;
            } else if (strncmp(endptr, "d", 1) == 0) {
                endptr += 1;
                tmp_double_val *= S_PER_D;
            } else {
                (void)write_stderr(
                    "ERROR: Valid units for this parameter \"%s\" are \"s\", \"min\", \"h\", and \"d\".\n", paraname);
                return FAILURE;
            }
        } else if (UNIT_MIN == unitval) {
            if (strncmp(endptr, "min", 3) == 0) {
                endptr += 3;
            } else if (strncmp(endptr, "h", 1) == 0) {
                endptr += 1;
                tmp_double_val *= MIN_PER_H;
            } else if (strncmp(endptr, "d", 1) == 0) {
                endptr += 1;
                tmp_double_val *= MIN_PER_D;
            } else {
                (void)write_stderr(
                    "ERROR: Valid units for this parameter \"%s\" are \"min\", \"h\", and \"d\".\n", paraname);
                return FAILURE;
            }
        } else if (UNIT_H == unitval) {
            if (strncmp(endptr, "h", 1) == 0) {
                endptr += 1;
            } else if (strncmp(endptr, "d", 1) == 0) {
                endptr += 1;
                tmp_double_val *= H_PER_D;
            } else {
                (void)write_stderr(
                    "ERROR: Valid units for this parameter \"%s\" are \"min\", \"h\", and \"d\".\n", paraname);
                return FAILURE;
            }
        } else if (UNIT_D == unitval) {
            if (strncmp(endptr, "d", 1) == 0) {
                endptr += 1;
            } else {
                (void)write_stderr("ERROR: Valid units for this parameter \"%s\" is \"d\".\n", paraname);
                return FAILURE;
            }
        } else {
            return FAILURE;
        }
    }

    while (isspace((unsigned char)*endptr))
        endptr++;

    if (*endptr != '\0')
        return FAILURE;

    if (isInt) {
        if (tmp_double_val > LLONG_MAX || tmp_double_val < LLONG_MIN)
            return FAILURE;
        if (NULL != result_int)
            *result_int = (int64)tmp_double_val;
    } else {
        if (NULL != result_double)
            *result_double = (double)tmp_double_val;
    }

    return SUCCESS;
}

int is_value_in_range(const char* guc_list_value, const char* value)
{
    char* ptr = NULL;
    char* outer_ptr = NULL;
    char delims[] = ",";
    char guc_val[MAX_VALUE_LEN] = {0};
    int nRet = 0;

    nRet = memset_s(guc_val, MAX_VALUE_LEN, '\0', MAX_VALUE_LEN);
    securec_check_c(nRet, "\0", "\0");
    nRet = snprintf_s(guc_val, MAX_VALUE_LEN, MAX_VALUE_LEN - 1, "%s", guc_list_value);
    securec_check_ss_c(nRet, "\0", "\0");

    ptr = strtok_r(guc_val, delims, &outer_ptr);
    while (NULL != ptr) {
        if (0 == strcmp(ptr, value))
            return SUCCESS;
        else
            ptr = strtok_r(NULL, delims, &outer_ptr);
    }
    return FAILURE;
}

/*************************************************************************************
 Function: check_enum_type_value
 Desc    : check the parameter value of enum type.
 Input   : paraname               parameter name
           guc_list_value         the string from config file
           value                  parameter value
 Return  : SUCCESS
           FAILURE
 *************************************************************************************/
int check_enum_type_value(const char* paraname, char* guc_list_value, const char* value)
{
    char guc_val[MAX_VALUE_LEN] = {0};
    int nRet = 0;
    const char* vptr = NULL;
    char* vouter_ptr = NULL;
    const char* p = NULL;
    char delims[] = ",";
    char tmp_paraname[MAX_PARAM_LEN];

    nRet = memset_s(guc_val, MAX_VALUE_LEN, '\0', MAX_VALUE_LEN);
    securec_check_c(nRet, "\0", "\0");
    nRet = snprintf_s(guc_val, MAX_VALUE_LEN, MAX_VALUE_LEN - 1, "%s", guc_list_value);
    securec_check_ss_c(nRet, "\0", "\0");
    nRet = memset_s(tmp_paraname, MAX_PARAM_LEN, '\0', MAX_PARAM_LEN);
    securec_check_c(nRet, "\0", "\0");

    if (NULL == guc_list_value || '\0' == guc_list_value[0]) {
        (void)write_stderr("ERROR: Failed to obtain the range information of parameter \"%s\".\n", paraname);
        return FAILURE;
    }

    make_string_tolower(value, tmp_paraname, sizeof(tmp_paraname) / sizeof(char));
    if (tmp_paraname != NULL && strlen(tmp_paraname) > 0) {
        vptr = strtok_r(tmp_paraname, delims, &vouter_ptr);
    } else {
        vptr = "";
    }
    while (NULL != vptr) {
        p = vptr;
        while (isspace((unsigned char)*p))
            p++;
        if (SUCCESS == is_value_in_range(guc_val, p)) {
            vptr = strtok_r(NULL, delims, &vouter_ptr);
        } else {
            (void)write_stderr("ERROR: The value \"%s\" is outside the valid range(%s) for parameter \"%s\".\n",
                value,
                guc_list_value,
                paraname);
            return FAILURE;
        }
    }
    return SUCCESS;
}

/*
 ************************************************************************************
 Function: check_bool_type_value
 Desc    : check the parameter value of bool type.
           GUC_PARA_BOOL    -     in bool value list
 Return  : SUCCESS
           FAILURE
 ************************************************************************************
*/
int check_bool_type_value(const char* value)
{
    /* the length of value list */
    int list_nums = lengthof(guc_bool_valuelist);

    if (is_string_in_list(value, guc_bool_valuelist, list_nums))
        return SUCCESS;
    else
        return FAILURE;
}

static bool check_datestyle_gs_guc(const char* paraname, const char* value)
{
    // datestyleList should be consistent with check_datestyle() in variable.cpp
    const char* dateStyleList = "iso,sql,postgres,german";
    const char* dateOrderList = "ymd,dmy,euro,european,mdy,us,noneuro,noneuropean,default";

    char* rawstring = NULL;
    const char delims[] = ",";
    char* vptr = NULL;
    char* vouter_ptr = NULL;
    char* p = NULL;
    bool  hasDateStyle = false;
    bool  hasDateOrder = false;
    bool  hasConflict = false;
    char* pname = xstrdup(paraname);

    make_string_tolower(paraname, pname, (int)strlen(pname));
    if (strcmp(pname, "datestyle") != 0) {
        // not datestyle, do not change result
        free(pname);
        return true;
    }

    /* Need a modifiable copy of string */
    rawstring = xstrdup(value);
    make_string_tolower(value, rawstring, (int)strlen(rawstring));
    // remove last '\'' or space
    p = rawstring + strlen(rawstring) - 1;
    while (isspace((unsigned char)*p) || *p == '\'') {
        *p = '\0';
        p--;
    }

    vptr = strtok_r(rawstring, delims, &vouter_ptr);
    while (vptr != NULL) {
        p = vptr;
        while (isspace((unsigned char)*p) || *p == '\'')
            p++;
        if (is_value_in_range(dateStyleList, p) == SUCCESS) {
            if (!hasDateStyle) {
                hasDateStyle = true;
            } else {
                hasConflict = true;
                break;
            }
            vptr = strtok_r(NULL, delims, &vouter_ptr);
        } else if (is_value_in_range(dateOrderList, p) == SUCCESS) {
            if (!hasDateOrder) {
                hasDateOrder = true;
            } else {
                hasConflict = true;
                break;
            }
            vptr = strtok_r(NULL, delims, &vouter_ptr);
        } else {
            write_stderr("ERROR: The value \"%s\" is invalid for parameter datestyle.\n", value);
            free(rawstring);
            free(pname);
            return false;
        }
    }

    free(rawstring);
    free(pname);
    if (hasConflict) {
        write_stderr("ERROR: The value \"%s\" have conflict options for parameter datestyle.\n", value);
    }
    return hasConflict ? false : true;
}

/*************************************************************************************
 Function: check_string_type_value
 Desc    : check the paraname value of string type.
           GUC_PARA_STRING  -     length(str) > 0
 Return  : SUCCESS
           FAILURE
 *************************************************************************************/
int check_string_type_value(const char* paraname, const char* value)
{
    bool result = ((int)strlen(value) > 0) ? true : false;
    /*
     * For now, we only check value for datestyle.
     * If we want to check more value, it is better to use hooks.
     */
    if (result && paraname != NULL) {
        result = check_datestyle_gs_guc(paraname, value);
    }
    return result ? SUCCESS : FAILURE;
}

/*
 * GetEnvStr
 *
 * Note: malloc space for get the return of getenv() function, then return the malloc space.
 *         so, this space need be free.
 */
static char* GetEnvStr(const char* env)
{
    char* tmpvar = NULL;
    const char* temp = getenv(env);
    errno_t rc = 0;
    if (temp != NULL) {
        size_t len = strlen(temp);
        if (0 == len)
            return NULL;
        tmpvar = (char*)malloc(len + 1);
        if (tmpvar != NULL) {
            rc = strcpy_s(tmpvar, len + 1, temp);
            securec_check_c(rc, "\0", "\0");
            return tmpvar;
        }
    }
    return NULL;
}

#ifdef __cplusplus
}
#endif /* __cplusplus */
