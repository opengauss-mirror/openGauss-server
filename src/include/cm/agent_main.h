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
 * agent_main.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/cm/agent_main.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef AGENT_MAIN_H
#define AGENT_MAIN_H

#include "common/config/cm_config.h"
#include "cm/cm_msg.h"
#include "alarm/alarm.h"
#include "cm/agent_alarm.h"

#define MAX_PATH_LEN 1024
/*time style length*/
#define MAX_TIME_LEN 20
const int cn_repair_retry_times = 3;
/* interval set to 10s to reduce invalid log */
const uint32 g_check_dn_sql5_interval = 10;

typedef long pgpid_t;

// These global variables are used to compressed traces
typedef struct LogFile {
    char fileName[MAX_PATH_LEN];
    char basePath[MAX_PATH_LEN];
    char pattern[MAX_TIME_LEN];
    char timestamp[MAX_TIME_LEN];
    int64 fileSize;
} LogFile;

// Log pattern for compress
typedef struct LogPattern {
    char patternName[MAX_PATH_LEN];
} LogPattern;

// get local max lsn
typedef struct LocalMaxLsnMng {
    bool checked;
    XLogRecPtr max_lsn;
} LocalMaxLsnMng;

/*
 *		Cut time from trace name.
 *		This time will be used to sort traces.
 */
void cutTimeFromFileLog(const char* fileName, char* pattern, char* time);
/*
 *		Sort of trace file by time asc.
 *		This time is part of trace name.
 */
void sortLogFileByTimeAsc(LogFile* logFile, int low, int high);

/*
 *		get the mode of the cluster.
 */
void get_start_mode(char* config_file);

/*
 *		used for connection mode or option between cm_agent and cn/dn.
 */
void get_connection_mode(char* config_file);

/*
 *		Read parameter from cm_agent.conf by accurate parameter name.
 */
int get_config_param(const char* config_file, const char* srcParam, char* destParam, int destLen);

/*
 *		Compressed trace to gz by zlib.
 *		The gzread() function shall read data from the compressed file referenced by file,
 *		which shall have been opened in a read mode (see gzopen() and gzdopen()). The gzread()
 *		function shall read data from file, and   *		uncompress it into buf. At most, len
 *		bytes of uncompressed data shall be copied to buf. If the file is not compressed,
 *		gzread() shall simply copy data from file to buf without alteration.
 *		The gzwrite() function shall write data to the compressed file referenced by file, which shall
 *		have been opened in a write mode (see gzopen() and gzdopen()). On entry, buf shall point to a
 *		buffer containing lenbytes of uncompressed data. The gzwrite() function shall compress this
 *		data and write it to file. The gzwrite() function shall return the number of uncompressed
 *		bytes actually written.
 */

int GZCompress(char* inpath, char* outpath);

/*
 *		Get trace pattern from cm_agent.conf.
 *		All trace pattern to be compressed are defined in cm_agent.conf.
 */
int get_log_pattern(char* config_file, const char* srcParam);

/*
 *		Create compress and remove thread for trace.
 *		Use Thread for this task avoid taking too much starting time of cm server.
 */
void CreateLogFileCompressAndRemoveThread(const char* patternName);

/*
 *		Read all traces by log pattern,including zip file and non zip file.
 *		Trace information are file time,file size,file path.These traces are
 *		saved in the global variable.
 */
int readFileList(char* basePath, LogFile* logFile, uint32* count, int64* totalSize);
/*
 *		Remove a file.
 *		It's always used to remove a trace compressed.
 */
void delLogFile(const char* fileName);
extern int check_datanode_status_phony_dead(char pid_path[MAXPGPATH]);

typedef enum { STARTING, ALL_NORMAL, INSTANCE_NEED_REPAIR, ALL_PENDING } ClusterStatus;

typedef enum { INSTANCE_CN, INSTANCE_DN, INSTANCE_GTM, INSTANCE_CM, INSTANCE_FENCED } InstanceTypes;

typedef enum { DOWN, PRIMARY, STANDBY, PENDING } InstanceStatus;

typedef enum { UNKNOWN_HA_STATE, NORMAL_HA_STATE, NEED_REPAIR, BUILD } HAStatus;

typedef enum { NA, SYSTEM_MARK_INCONSISTENT, TIMELINE_INCONSISTENT, LOG_NOT_EXIST } BuildReason;

typedef enum { SMART_MODE, FAST_MODE, IMMEDIATE_MODE, RESUME_MODE } ShutdownMode;

typedef struct InstanceStatusReport {
    uint32 node;

    char DataPath[CM_PATH_LENGTH];
    uint32 type;
    uint32 InstanceStatus;

    char LogPath[CM_PATH_LENGTH];
    uint32 HAStatus;
    uint32 reason;
} InstanceStatusReport;

typedef enum { NORMAL, UNKNOWN } CoordinateStatus;

typedef struct NodeStatusReport {
    uint32 node;
    char nodeName[CM_NODE_NAME];
    uint32 isCn;
    uint32 CoordinateStatus;
    uint32 isGtm;
    InstanceStatusReport gtm;
    uint32 datanodesNum;
    InstanceStatusReport datanodes[CM_MAX_DATANODE_PER_NODE];
} NodeStatusReport;

typedef struct datanode_failover {
    bool datanodes[CM_MAX_DATANODE_PER_NODE];
    bool coordinator;
} datanode_failover;

typedef struct coordinator_status {
    int cn_status;
    bool delayed_repair;
} coordinator_status;

extern volatile bool g_repair_cn;
extern coordinator_status* g_cn_status;
extern bool* g_coordinators_drop;
extern pthread_rwlock_t g_coordinators_drop_lock;
extern uint32* g_dropped_coordinatorId;
extern bool g_coordinators_cancel;
extern pthread_rwlock_t g_coordinators_cancel_lock;
extern cm_instance_central_node_msg g_ccn_notify;

/* Control whether agent request cluster state */
extern bool pooler_ping_end_request;
/* Control whether agent close the pooler ping switch */
extern bool pooler_ping_end;

extern datanode_failover* g_datanodes_failover;
extern pthread_rwlock_t g_datanodes_failover_lock;
extern int g_gtmMode;

extern int datanode_status_check_and_report_wrapper(agent_to_cm_datanode_status_report* report_msg, uint32 ii,
    char* data_path, bool do_build, uint32 check_dn_sql5_timer);
extern int node_match_find(char* node_type, const char* node_port, const char* node_host, char* node_port1,
    char* node_host1, int* node_index, int* instance_index, int* inode_type);
extern int check_one_instance_status(char* process_name, char* cmd_line);
extern void report_conn_fail_alarm(AlarmType alarmType, InstanceTypes instance_type, uint32 instanceId);
extern int get_connection_to_coordinator();
extern int cmagent_getenv(const char* env_var, char* output_env_value, uint32 env_value_len);
typedef bool (*IsResultExpectedFunPtr)(const void*);
int cmagent_to_coordinator_execute_query(char* run_command, IsResultExpectedFunPtr IsResultExpected = NULL);
bool IsResultExpectedPoolReload(const void* result);

#endif
