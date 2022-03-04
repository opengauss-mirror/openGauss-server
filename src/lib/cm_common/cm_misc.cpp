/**
 * @file cm_misc.cpp
 * @brief 
 * @author xxx
 * @version 1.0
 * @date 2020-08-06
 * 
 * @copyright Copyright (c) Huawei Technologies Co., Ltd. 2011-2020. All rights reserved.
 * 
 */
#include <sys/types.h>
#include <time.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
#include <signal.h>
#include <dirent.h>
#include <limits.h>
#include <syslog.h>
#include <sys/procfs.h>

#include "cm/elog.h"
#include "cm/cm_c.h"
#include "cm/stringinfo.h"
#include "cm/cm_msg.h"
#include "common/config/cm_config.h"
#include "cm/etcdapi.h"
#include "cm/cm_misc.h"

/*
 * ssh connect does not exit automatically when the network is fault,
 * this will cause cm_ctl hang for several hours,
 * so we should add the following timeout options for ssh. 
 */
#define SSH_CONNECT_TIMEOUT "5"
#define SSH_CONNECT_ATTEMPTS "3"
#define SSH_SERVER_ALIVE_INTERVAL "15"
#define SSH_SERVER_ALIVE_COUNT_MAX "3"
#define PSSH_TIMEOUT_OPTION                                                                        \
    " -t 60 -O ConnectTimeout=" SSH_CONNECT_TIMEOUT " -O ConnectionAttempts=" SSH_CONNECT_ATTEMPTS \
    " -O ServerAliveInterval=" SSH_SERVER_ALIVE_INTERVAL " -O ServerAliveCountMax=" SSH_SERVER_ALIVE_COUNT_MAX " "

const int MAXLISTEN = 64;

uint32 g_health_etcd_index[CM_NODE_MAXNUM] = {0};
bool g_health_etcd_flag = false;
uint32 g_health_etcd_count = 0;
pthread_rwlock_t g_health_etcd_rwlock = PTHREAD_RWLOCK_INITIALIZER;

/*
 * @@GaussDB@@
 * Brief            : void *pg_malloc(size_t size)
 * Description      : malloc space
 * Notes            :
 */
static void* pg_malloc(size_t size)
{
    void* result = NULL;

    /* Avoid unportable behavior of malloc(0) */
    if (size == 0) {
        write_runlog(ERROR, "malloc 0.\n");
        exit(1);
    }

    result = (void*)malloc(size);
    if (result == NULL) {
        write_runlog(ERROR, "malloc failed, out of memory.\n");
        exit(1);
    }
    return result;
}

char** readfile(const char* path)
{
    int fd = 0;
    int nlines;
    char** result;
    char* buffer = NULL;
    char* linebegin = NULL;
    int i;
    int n;
    int len;
    struct stat statbuf = {0};

    /*
     * Slurp the file into memory.
     *
     * The file can change concurrently, so we read the whole file into memory
     * with a single read() call. That's not guaranteed to get an atomic
     * snapshot, but in practice, for a small file, it's close enough for the
     * current use.
     */
    fd = open(path, O_RDONLY | PG_BINARY | O_CLOEXEC, 0);
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
        result = (char**)pg_malloc(sizeof(char*));
        *result = NULL;
        return result;
    }
    buffer = (char*)pg_malloc(statbuf.st_size + 1);

    len = read(fd, buffer, statbuf.st_size + 1);
    close(fd);
    if (len != statbuf.st_size) {
        /* oops, the file size changed between fstat and read */
        FREE_AND_RESET(buffer);
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
    result = (char**)pg_malloc((nlines + 1) * sizeof(char*));

    /* now split the buffer into lines */
    linebegin = buffer;
    n = 0;
    for (i = 0; i < len; i++) {
        if (buffer[i] == '\n') {
            int slen = &buffer[i] - linebegin + 1;
            char* linebuf = (char*)pg_malloc(slen + 1);
            errno_t rc;

            rc = memcpy_s(linebuf, slen + 1, linebegin, slen);
            securec_check_c(rc, linebuf, "\0");
            linebuf[slen] = '\0';
            result[n++] = linebuf;
            linebegin = &buffer[i + 1];
        }
    }
    result[n] = NULL;

    FREE_AND_RESET(buffer);

    return result;
}

void freefile(char** lines)
{
    char** line = NULL;
    if (lines == nullptr) {
        return;
    }
    line = lines;
    while (*line != NULL) {
        FREE_AND_RESET(*line);
        line++;
    }
    free(lines);
}

log_level_string log_level_map_string[] = {

    {"DEBUG5", DEBUG5},
    {"DEBUG1", DEBUG1},
    {"WARNING", WARNING},
    {"LOG", LOG},
    {"ERROR", ERROR},
    {"FATAL", FATAL},
    {NULL, UNKNOWN_LEVEL}

};

int log_level_string_to_int(const char* log_level)
{
    int i;
    for (i = 0; log_level_map_string[i].level_string != NULL; i++) {
        if (strcasecmp(log_level_map_string[i].level_string, log_level) == 0) {
            return log_level_map_string[i].level_val;
        }
    }
    return UNKNOWN_LEVEL;
}

const char* log_level_int_to_string(int log_level)
{
    int i;
    for (i = 0; log_level_map_string[i].level_string != NULL; i++) {
        if (log_level_map_string[i].level_val == log_level) {
            return log_level_map_string[i].level_string;
        }
    }
    return "Unknown";
}

const char* DcfRoleToString(int role)
{
    switch (role) {
        case DCF_ROLE_LEADER:
            return "LEADER";
        case DCF_ROLE_FOLLOWER:
            return "FOLLOWER";
        case DCF_ROLE_LOGGER:
            return "LOGGER";
        case DCF_ROLE_PASSIVE:
            return "PASSIVE";
        case DCF_ROLE_PRE_CANDIDATE:
            return "PRE_CANDIDATE";
        case DCF_ROLE_CANDIDATE:
            return "CANDIDATE";
        default:
            return "UNKNOWN";
    }

    return "UNKNOWN";
}

instance_datanode_build_reason_string datanode_build_reason_map_string[] = {

    {"Normal", INSTANCE_HA_DATANODE_BUILD_REASON_NORMAL},
    {"WAL segment removed", INSTANCE_HA_DATANODE_BUILD_REASON_WALSEGMENT_REMOVED},
    {"Disconnected", INSTANCE_HA_DATANODE_BUILD_REASON_DISCONNECT},
    {"Version not matched", INSTANCE_HA_DATANODE_BUILD_REASON_VERSION_NOT_MATCHED},
    {"Mode not matched", INSTANCE_HA_DATANODE_BUILD_REASON_MODE_NOT_MATCHED},
    {"System id not matched", INSTANCE_HA_DATANODE_BUILD_REASON_SYSTEMID_NOT_MATCHED},
    {"Timeline not matched", INSTANCE_HA_DATANODE_BUILD_REASON_TIMELINE_NOT_MATCHED},
    {"DCF log loss", INSTANCE_HA_DATANODE_BUILD_REASON_DCF_LOG_LOSS},
    {"Unknown", INSTANCE_HA_DATANODE_BUILD_REASON_UNKNOWN},
    {"User/Password invalid", INSTANCE_HA_DATANODE_BUILD_REASON_USER_PASSWD_INVALID},
    {"Connecting", INSTANCE_HA_DATANODE_BUILD_REASON_CONNECTING},
    {NULL, INSTANCE_HA_DATANODE_BUILD_REASON_UNKNOWN}

};

int datanode_rebuild_reason_string_to_int(const char* reason)
{
    int i;

    for (i = 0; datanode_build_reason_map_string[i].reason_string != NULL; i++) {
        if (strstr(reason, datanode_build_reason_map_string[i].reason_string) != NULL) {
            return datanode_build_reason_map_string[i].reason_val;
        }
    }

    return INSTANCE_HA_DATANODE_BUILD_REASON_UNKNOWN;
}

const char* datanode_rebuild_reason_int_to_string(int reason)
{
    int i;

    for (i = 0; datanode_build_reason_map_string[i].reason_string != NULL; i++) {
        if (datanode_build_reason_map_string[i].reason_val == reason) {
            return datanode_build_reason_map_string[i].reason_string;
        }
    }
    return "Unknown";
}

instacne_type_string type_map_string[] = {

    {"GTM", INSTANCE_TYPE_GTM},
    {"Datanode", INSTANCE_TYPE_DATANODE},
    {"Coordinator", INSTANCE_TYPE_COORDINATE},
    {"Fenced UDF", INSTANCE_TYPE_FENCED_UDF},
    {NULL, INSTANCE_TYPE_UNKNOWN}};

const char* type_int_to_string(int type)
{
    int i;
    for (i = 0; type_map_string[i].type_string != NULL; i++) {
        if (type_map_string[i].type_val == type) {
            return type_map_string[i].type_string;
        }
    }
    return "Unknown";
}

gtm_con_string gtm_con_map_string[] = {{"Connection ok", CON_OK},
    {"Connection bad", CON_BAD},
    {"Connection started", CON_STARTED},
    {"Connection made", CON_MADE},
    {"Connection awaiting response", CON_AWAITING_RESPONSE},
    {"Connection authentication ok", CON_AUTH_OK},
    {"Connection prepare environment", CON_SETEN},
    {"Connection prepare SSL", CON_SSL_STARTUP},
    {"Connection needed", CON_NEEDED},
    {"Unknown", CON_UNKNOWN},
    {"Manually stopped", CON_MANUAL_STOPPED},
    {"Disk damaged", CON_DISK_DEMAGED},
    {"Port conflicting", CON_PORT_USED},
    {"Nic down", CON_NIC_DOWN},
    {"Starting", CON_GTM_STARTING},
    {NULL, CON_UNKNOWN}};

const char* gtm_con_int_to_string(int con)
{
    int i;
    for (i = 0; gtm_con_map_string[i].con_string != NULL; i++) {
        if (gtm_con_map_string[i].con_val == con) {
            return gtm_con_map_string[i].con_string;
        }
    }
    return "Unknown";
}

server_role_string server_role_string_map[] = {{CM_SERVER_UNKNOWN, "UNKNOWN"},
    {CM_SERVER_PRIMARY, "Primary"},
    {CM_SERVER_STANDBY, "Standby"},
    {CM_SERVER_INIT, "Init"},
    {CM_SERVER_DOWN, "Down"}};


server_role_string etcd_role_string_map[] = {{CM_ETCD_UNKNOWN, "UNKNOWN"},
    {CM_ETCD_FOLLOWER, "StateFollower"},
    {CM_ETCD_LEADER, "StateLeader"},
    {CM_ETCD_DOWN, "Down"}};

server_role_string kerberos_role_string_map[] = {{KERBEROS_STATUS_UNKNOWN, "UNKNOWN"},
    {KERBEROS_STATUS_NORMAL, "Normal"},
    {KERBEROS_STATUS_ABNORMAL, "Abnormal"},
    {KERBEROS_STATUS_DOWN, "Down"}};

const char* etcd_role_to_string(int role)
{
    if (role <= CM_ETCD_UNKNOWN || role > CM_ETCD_DOWN) {
        return etcd_role_string_map[CM_ETCD_UNKNOWN].role_string;
    } else {
        return etcd_role_string_map[role].role_string;
    }
}

const char* server_role_to_string(int role, bool is_pending)
{
    if (role <= CM_SERVER_UNKNOWN || role >= CM_SERVER_INIT) {
        return "Unknown";
    } else {
        if (CM_SERVER_PRIMARY == role && is_pending) {
            return "Pending";
        } else {
            return server_role_string_map[role].role_string;
        }
    }
}

instance_datanode_lockmode_string g_datanode_lockmode_map_string[] = {{"polling_connection", POLLING_CONNECTION},
    {"specify_connection", SPECIFY_CONNECTION},
    {"prohibit_connection", PROHIBIT_CONNECTION},
    {NULL, UNDEFINED_LOCKMODE}};

int datanode_lockmode_string_to_int(const char* lockmode)
{
    int i;
    if (lockmode == NULL || strlen(lockmode) == 0) {
        write_runlog(ERROR, "datanode_lockmode_string_to_int failed, input string role is: NULL\n");
        return UNDEFINED_LOCKMODE;
    } else {
        for (i = 0; g_datanode_lockmode_map_string[i].lockmode_string != NULL; i++) {
            if (strncmp(g_datanode_lockmode_map_string[i].lockmode_string, lockmode, strlen(lockmode)) == 0) {
                return g_datanode_lockmode_map_string[i].lockmode_val;
            }
        }
    }
    write_runlog(ERROR, "datanode_lockmode_string_to_int failed, input lockmode is: (%s)\n", lockmode);
    return UNDEFINED_LOCKMODE;
}

instacne_datanode_role_string datanode_role_map_string[] = {

    {"Primary", INSTANCE_ROLE_PRIMARY},
    {"Standby", INSTANCE_ROLE_STANDBY},
    {"Pending", INSTANCE_ROLE_PENDING},
    {"Normal", INSTANCE_ROLE_NORMAL},
    {"Down", INSTANCE_ROLE_UNKNOWN},
    {"Secondary", INSTANCE_ROLE_DUMMY_STANDBY},
    {"Deleted", INSTANCE_ROLE_DELETED},
    {"ReadOnly", INSTANCE_ROLE_READONLY},
    {"Offline", INSTANCE_ROLE_OFFLINE},
    {"Main Standby", INSTANCE_ROLE_MAIN_STANDBY},
    {"Cascade Standby", INSTANCE_ROLE_CASCADE_STANDBY},
    {NULL, INSTANCE_ROLE_UNKNOWN}};

int datanode_role_string_to_int(const char* role)
{
    int i;
    if (NULL == role) {
        write_runlog(ERROR, "datanode_role_string_to_int failed, input string role is: NULL\n");
        return INSTANCE_ROLE_UNKNOWN;
    }
    for (i = 0; datanode_role_map_string[i].role_string != NULL; i++) {
        if (strcmp(datanode_role_map_string[i].role_string, role) == 0) {
            return datanode_role_map_string[i].role_val;
        }
    }
    write_runlog(ERROR, "datanode_role_string_to_int failed, input string role is: (%s)\n", role);
    return INSTANCE_ROLE_UNKNOWN;
}

const char* datanode_role_int_to_string(int role)
{
    int i;
    for (i = 0; datanode_role_map_string[i].role_string != NULL; i++) {
        if ((int)datanode_role_map_string[i].role_val == role) {
            return datanode_role_map_string[i].role_string;
        }
    }
    return "Unknown";
}


instacne_datanode_role_string datanode_static_role_map_string[] = {
    {"P", PRIMARY_DN}, {"S", STANDBY_DN}, {"R", DUMMY_STANDBY_DN}, {NULL, INSTANCE_ROLE_NORMAL}};

const char* datanode_static_role_int_to_string(uint32 role)
{
    int i;
    for (i = 0; datanode_static_role_map_string[i].role_string != NULL; i++) {
        if (datanode_static_role_map_string[i].role_val == role) {
            return datanode_static_role_map_string[i].role_string;
        }
    }
    return "Unknown";
}

instacne_datanode_dbstate_string datanode_dbstate_map_string[] = {{"Unknown", INSTANCE_HA_STATE_UNKONWN},
    {"Normal", INSTANCE_HA_STATE_NORMAL},
    {"Need repair", INSTANCE_HA_STATE_NEED_REPAIR},
    {"Starting", INSTANCE_HA_STATE_STARTING},
    {"Wait promoting", INSTANCE_HA_STATE_WAITING},
    {"Demoting", INSTANCE_HA_STATE_DEMOTING},
    {"Promoting", INSTANCE_HA_STATE_PROMOTING},
    {"Building", INSTANCE_HA_STATE_BUILDING},
    {"Manually stopped", INSTANCE_HA_STATE_MANUAL_STOPPED},
    {"Disk damaged", INSTANCE_HA_STATE_DISK_DAMAGED},
    {"Port conflicting", INSTANCE_HA_STATE_PORT_USED},
    {"Build failed", INSTANCE_HA_STATE_BUILD_FAILED},
    {"Catchup", INSTANCE_HA_STATE_CATCH_UP},
    {"CoreDump", INSTANCE_HA_STATE_COREDUMP},
    {"ReadOnly", INSTANCE_HA_STATE_READ_ONLY},
    {NULL, INSTANCE_ROLE_NORMAL}};

int datanode_dbstate_string_to_int(const char* dbstate)
{
    int i;
    if (NULL == dbstate) {
        write_runlog(ERROR, "datanode_dbstate_string_to_int failed, input string dbstate is: NULL\n");
        return INSTANCE_HA_STATE_UNKONWN;
    }
    for (i = 0; datanode_dbstate_map_string[i].dbstate_string != NULL; i++) {
        if (strcmp(datanode_dbstate_map_string[i].dbstate_string, dbstate) == 0) {
            return datanode_dbstate_map_string[i].dbstate_val;
        }
    }
    write_runlog(ERROR, "datanode_dbstate_string_to_int failed, input string dbstate is: (%s)\n", dbstate);
    return INSTANCE_HA_STATE_UNKONWN;
}

const char* datanode_dbstate_int_to_string(int dbstate)
{
    int i;
    for (i = 0; datanode_dbstate_map_string[i].dbstate_string != NULL; i++) {
        if (datanode_dbstate_map_string[i].dbstate_val == dbstate) {
            return datanode_dbstate_map_string[i].dbstate_string;
        }
    }
    return "Unknown";
}

instacne_datanode_wal_send_state_string datanode_wal_send_state_map_string[] = {
    {"Startup", INSTANCE_WALSNDSTATE_STARTUP},
    {"Backup", INSTANCE_WALSNDSTATE_BACKUP},
    {"Catchup", INSTANCE_WALSNDSTATE_CATCHUP},
    {"Streaming", INSTANCE_WALSNDSTATE_STREAMING},
    {"Dump syslog", INSTANCE_WALSNDSTATE_DUMPLOG},
    {"Normal", INSTANCE_WALSNDSTATE_NORMAL},
    {"Unknown", INSTANCE_WALSNDSTATE_UNKNOWN},
    {NULL, INSTANCE_WALSNDSTATE_UNKNOWN}};

int datanode_wal_send_state_string_to_int(const char* dbstate)
{
    int i;
    if (NULL == dbstate) {
        write_runlog(ERROR, "datanode_wal_send_state_string_to_int failed, input string dbstate is: NULL\n");
        return INSTANCE_WALSNDSTATE_UNKNOWN;
    }
    for (i = 0; datanode_wal_send_state_map_string[i].wal_send_state_string != NULL; i++) {
        if (strcmp(datanode_wal_send_state_map_string[i].wal_send_state_string, dbstate) == 0) {
            return datanode_wal_send_state_map_string[i].wal_send_state_val;
        }
    }
    write_runlog(ERROR, "datanode_wal_send_state_string_to_int failed, input string dbstate is: (%s)\n", dbstate);
    return INSTANCE_WALSNDSTATE_UNKNOWN;
}

const char* datanode_wal_send_state_int_to_string(int dbstate)
{
    int i;
    for (i = 0; datanode_wal_send_state_map_string[i].wal_send_state_string != NULL; i++) {
        if (datanode_wal_send_state_map_string[i].wal_send_state_val == dbstate) {
            return      datanode_wal_send_state_map_string[i].wal_send_state_string;
        }
    }
    return "Unknown";
}

instacne_datanode_sync_state_string datanode_wal_sync_state_map_string[] = {{"Async", INSTANCE_DATA_REPLICATION_ASYNC},
    {"Sync", INSTANCE_DATA_REPLICATION_SYNC},
    {"Most available", INSTANCE_DATA_REPLICATION_MOST_AVAILABLE},
    {"Potential", INSTANCE_DATA_REPLICATION_POTENTIAL_SYNC},
    {"Quorum", INSTANCE_DATA_REPLICATION_QUORUM},
    {NULL, INSTANCE_DATA_REPLICATION_UNKONWN}};

int datanode_wal_sync_state_string_to_int(const char* dbstate)
{
    int i;
    if (NULL == dbstate) {
        write_runlog(ERROR, "datanode_wal_sync_state_string_to_int failed, input string dbstate is: NULL\n");
        return INSTANCE_DATA_REPLICATION_UNKONWN;
    }
    for (i = 0; datanode_wal_sync_state_map_string[i].wal_sync_state_string != NULL; i++) {
        if (strcmp(datanode_wal_sync_state_map_string[i].wal_sync_state_string, dbstate) == 0) {
            return datanode_wal_sync_state_map_string[i].wal_sync_state_val;
        }
    }
    write_runlog(ERROR, "datanode_wal_sync_state_string_to_int failed, input string dbstate is: (%s)\n", dbstate);
    return INSTANCE_DATA_REPLICATION_UNKONWN;
}

const char* datanode_wal_sync_state_int_to_string(int dbstate)
{
    int i;
    for (i = 0; datanode_wal_sync_state_map_string[i].wal_sync_state_string != NULL; i++) {
        if (datanode_wal_sync_state_map_string[i].wal_sync_state_val == dbstate) {
            return datanode_wal_sync_state_map_string[i].wal_sync_state_string;
        }
    }
    return "Unknown";
}

cluster_state_string cluster_state_map_string[] = {
    {"Starting", CM_STATUS_STARTING},
    {"Redistributing", CM_STATUS_PENDING},
    {"Normal", CM_STATUS_NORMAL},
    {"Unavailable", CM_STATUS_NEED_REPAIR},
    {"Degraded", CM_STATUS_DEGRADE},
    {"Unknown", CM_STATUS_UNKNOWN},
    {"NormalCNDeleted", CM_STATUS_NORMAL_WITH_CN_DELETED},
    {NULL, CM_STATUS_UNKNOWN},
};

const char* cluster_state_int_to_string(int cluster_state)
{
    int i;
    for (i = 0; cluster_state_map_string[i].cluster_state_string != NULL; i++) {
        if (cluster_state_map_string[i].cluster_state_val == cluster_state) {
            return cluster_state_map_string[i].cluster_state_string;
        }
    }
    return "Unknown";
}

/* this map should be sync with CM_MessageType in cm_msg.h file. */
cluster_msg_string cluster_msg_map_string[] = {

    {"MSG_CTL_CM_SWITCHOVER", MSG_CTL_CM_SWITCHOVER},
    {"MSG_CTL_CM_BUILD", MSG_CTL_CM_BUILD},
    {"MSG_CTL_CM_SYNC", MSG_CTL_CM_SYNC},
    {"MSG_CTL_CM_QUERY", MSG_CTL_CM_QUERY},
    {"MSG_CTL_CM_NOTIFY", MSG_CTL_CM_NOTIFY},
    {"MSG_CTL_CM_BUTT", MSG_CTL_CM_BUTT},
    {"MSG_CM_CTL_DATA_BEGIN", MSG_CM_CTL_DATA_BEGIN},
    {"MSG_CM_CTL_DATA", MSG_CM_CTL_DATA},
    {"MSG_CM_CTL_NODE_END", MSG_CM_CTL_NODE_END},
    {"MSG_CM_CTL_DATA_END", MSG_CM_CTL_DATA_END},
    {"MSG_CM_CTL_COMMAND_ACK", MSG_CM_CTL_COMMAND_ACK},

    {"MSG_CM_AGENT_SWITCHOVER", MSG_CM_AGENT_SWITCHOVER},
    {"MSG_CM_AGENT_FAILOVER", MSG_CM_AGENT_FAILOVER},
    {"MSG_CM_AGENT_BUILD", MSG_CM_AGENT_BUILD},
    {"MSG_CM_AGENT_SYNC", MSG_CM_AGENT_SYNC},
    {"MSG_CM_AGENT_NOTIFY", MSG_CM_AGENT_NOTIFY},
    {"MSG_CM_AGENT_NOTIFY_CN", MSG_CM_AGENT_NOTIFY_CN},
    {"MSG_CM_AGENT_NOTIFY_CN_CENTRAL_NODE", MSG_CM_AGENT_NOTIFY_CN_CENTRAL_NODE},
    {"MSG_AGENT_CM_NOTIFY_CN_FEEDBACK", MSG_AGENT_CM_NOTIFY_CN_FEEDBACK},
    {"MSG_CM_AGENT_DROP_CN", MSG_CM_AGENT_DROP_CN},
    {"MSG_CM_AGENT_CANCEL_SESSION", MSG_CM_AGENT_CANCEL_SESSION},
    {"MSG_CM_AGENT_DROPPED_CN", MSG_CM_AGENT_DROPPED_CN},
    {"MSG_CM_AGENT_RESTART", MSG_CM_AGENT_RESTART},
    {"MSG_CM_AGENT_RESTART_BY_MODE", MSG_CM_AGENT_RESTART_BY_MODE},
    {"MSG_CM_AGENT_REP_SYNC", MSG_CM_AGENT_REP_SYNC},
    {"MSG_CM_AGENT_REP_ASYNC", MSG_CM_AGENT_REP_ASYNC},
    {"MSG_CM_AGENT_REP_MOST_AVAILABLE", MSG_CM_AGENT_REP_MOST_AVAILABLE},
    {"MSG_CM_AGENT_BUTT", MSG_CM_AGENT_BUTT},

    {"MSG_AGENT_CM_DATA_INSTANCE_REPORT_STATUS", MSG_AGENT_CM_DATA_INSTANCE_REPORT_STATUS},
    {"MSG_AGENT_CM_COORDINATE_INSTANCE_STATUS", MSG_AGENT_CM_COORDINATE_INSTANCE_STATUS},
    {"MSG_AGENT_CM_GTM_INSTANCE_STATUS", MSG_AGENT_CM_GTM_INSTANCE_STATUS},
    {"MSG_AGENT_CM_FENCED_UDF_INSTANCE_STATUS", MSG_AGENT_CM_FENCED_UDF_INSTANCE_STATUS},
    {"MSG_AGENT_CM_BUTT", MSG_AGENT_CM_BUTT},

    {"MSG_CM_CM_VOTE", MSG_CM_CM_VOTE},
    {"MSG_CM_CM_BROADCAST", MSG_CM_CM_BROADCAST},
    {"MSG_CM_CM_NOTIFY", MSG_CM_CM_NOTIFY},
    {"MSG_CM_CM_SWITCHOVER", MSG_CM_CM_SWITCHOVER},
    {"MSG_CM_CM_FAILOVER", MSG_CM_CM_FAILOVER},
    {"MSG_CM_CM_SYNC", MSG_CM_CM_SYNC},
    {"MSG_CM_CM_SWITCHOVER_ACK", MSG_CM_CM_SWITCHOVER_ACK},
    {"MSG_CM_CM_FAILOVER_ACK", MSG_CM_CM_FAILOVER_ACK},
    {"MSG_CM_CM_ROLE_CHANGE_NOTIFY", MSG_CM_CM_ROLE_CHANGE_NOTIFY},
    {"MSG_CM_CM_REPORT_SYNC", MSG_CM_CM_REPORT_SYNC},

    {"MSG_AGENT_CM_HEARTBEAT", MSG_AGENT_CM_HEARTBEAT},
    {"MSG_CM_AGENT_HEARTBEAT", MSG_CM_AGENT_HEARTBEAT},
    {"MSG_CTL_CM_SET", MSG_CTL_CM_SET},
    {"MSG_CTL_CM_SWITCHOVER_ALL", MSG_CTL_CM_SWITCHOVER_ALL},
    {"MSG_CM_CTL_SWITCHOVER_ALL_ACK", MSG_CM_CTL_SWITCHOVER_ALL_ACK},
    {"MSG_CTL_CM_BALANCE_CHECK", MSG_CTL_CM_BALANCE_CHECK},
    {"MSG_CM_CTL_BALANCE_CHECK_ACK", MSG_CM_CTL_BALANCE_CHECK_ACK},
    {"MSG_CTL_CM_BALANCE_RESULT", MSG_CTL_CM_BALANCE_RESULT},
    {"MSG_CM_CTL_BALANCE_RESULT_ACK", MSG_CM_CTL_BALANCE_RESULT_ACK},
    {"MSG_CTL_CM_QUERY_CMSERVER", MSG_CTL_CM_QUERY_CMSERVER},
    {"MSG_CM_CTL_CMSERVER", MSG_CM_CTL_CMSERVER},
    {"MSG_TYPE_BUTT", MSG_TYPE_BUTT},
    {"MSG_CTL_CM_SWITCHOVER_FULL", MSG_CTL_CM_SWITCHOVER_FULL},
    {"MSG_CM_CTL_SWITCHOVER_FULL_ACK", MSG_CM_CTL_SWITCHOVER_FULL_ACK},
    {"MSG_CM_CTL_SWITCHOVER_FULL_DENIED", MSG_CM_CTL_SWITCHOVER_FULL_DENIED},
    {"MSG_CTL_CM_SWITCHOVER_FULL_CHECK", MSG_CTL_CM_SWITCHOVER_FULL_CHECK},
    {"MSG_CM_CTL_SWITCHOVER_FULL_CHECK_ACK", MSG_CM_CTL_SWITCHOVER_FULL_CHECK_ACK},
    {"MSG_CTL_CM_SWITCHOVER_FULL_TIMEOUT", MSG_CTL_CM_SWITCHOVER_FULL_TIMEOUT},
    {"MSG_CM_CTL_SWITCHOVER_FULL_TIMEOUT_ACK", MSG_CM_CTL_SWITCHOVER_FULL_TIMEOUT_ACK},
    {"MSG_CTL_CM_SETMODE", MSG_CTL_CM_SETMODE},
    {"MSG_CM_CTL_SETMODE_ACK", MSG_CM_CTL_SETMODE_ACK},

    {"MSG_CTL_CM_SWITCHOVER_AZ", MSG_CTL_CM_SWITCHOVER_AZ},
    {"MSG_CM_CTL_SWITCHOVER_AZ_ACK", MSG_CM_CTL_SWITCHOVER_AZ_ACK},
    {"MSG_CM_CTL_SWITCHOVER_AZ_DENIED", MSG_CM_CTL_SWITCHOVER_AZ_DENIED},
    {"MSG_CTL_CM_SWITCHOVER_AZ_CHECK", MSG_CTL_CM_SWITCHOVER_AZ_CHECK},
    {"MSG_CM_CTL_SWITCHOVER_AZ_CHECK_ACK", MSG_CM_CTL_SWITCHOVER_AZ_CHECK_ACK},
    {"MSG_CTL_CM_SWITCHOVER_AZ_TIMEOUT", MSG_CTL_CM_SWITCHOVER_AZ_TIMEOUT},
    {"MSG_CM_CTL_SWITCHOVER_AZ_TIMEOUT_ACK", MSG_CM_CTL_SWITCHOVER_AZ_TIMEOUT_ACK},

    {"MSG_CM_CTL_SET_ACK", MSG_CM_CTL_SET_ACK},
    {"MSG_CTL_CM_GET", MSG_CTL_CM_GET},
    {"MSG_CM_CTL_GET_ACK", MSG_CM_CTL_GET_ACK},

    {"MSG_CM_AGENT_GS_GUC", MSG_CM_AGENT_GS_GUC},
    {"MSG_AGENT_CM_GS_GUC_ACK", MSG_AGENT_CM_GS_GUC_ACK},
    {"MSG_CM_CTL_SWITCHOVER_INCOMPLETE_ACK", MSG_CM_CTL_SWITCHOVER_INCOMPLETE_ACK},
    {"MSG_CM_CM_TIMELINE", MSG_CM_CM_TIMELINE},
    {"MSG_CM_BUILD_DOING", MSG_CM_BUILD_DOING},
    {"MSG_AGENT_CM_ETCD_CURRENT_TIME", MSG_AGENT_CM_ETCD_CURRENT_TIME},
    {"MSG_CM_QUERY_INSTANCE_STATUS", MSG_CM_QUERY_INSTANCE_STATUS},
    {"MSG_CM_SERVER_TO_AGENT_CONN_CHECK", MSG_CM_SERVER_TO_AGENT_CONN_CHECK},
    {"MSG_CTL_CM_GET_DATANODE_RELATION", MSG_CTL_CM_GET_DATANODE_RELATION},
    {"MSG_CM_BUILD_DOWN", MSG_CM_BUILD_DOWN},
    {"MSG_CM_SERVER_REPAIR_CN_ACK", MSG_CM_SERVER_REPAIR_CN_ACK},
    {"MSG_CTL_CM_SETMODE", MSG_CTL_CM_DISABLE_CN},
    {"MSG_CM_CTL_SETMODE_ACK", MSG_CTL_CM_DISABLE_CN_ACK},
    {"MSG_CM_AGENT_LOCK_NO_PRIMARY", MSG_CM_AGENT_LOCK_NO_PRIMARY},
    {"MSG_CM_AGENT_LOCK_CHOSEN_PRIMARY", MSG_CM_AGENT_LOCK_CHOSEN_PRIMARY},
    {"MSG_CM_AGENT_UNLOCK", MSG_CM_AGENT_UNLOCK},
    {"MSG_CTL_CM_STOP_ARBITRATION", MSG_CTL_CM_STOP_ARBITRATION},
    {"MSG_CTL_CM_FINISH_REDO", MSG_CTL_CM_FINISH_REDO},
    {"MSG_CM_CTL_FINISH_REDO_ACK", MSG_CM_CTL_FINISH_REDO_ACK},
    {"MSG_CM_AGENT_FINISH_REDO", MSG_CM_AGENT_FINISH_REDO},
    {"MSG_CTL_CM_FINISH_REDO_CHECK", MSG_CTL_CM_FINISH_REDO_CHECK},
    {"MSG_CM_CTL_FINISH_REDO_CHECK_ACK", MSG_CM_CTL_FINISH_REDO_CHECK_ACK},
    {"MSG_AGENT_CM_KERBEROS_STATUS", MSG_AGENT_CM_KERBEROS_STATUS},
    {"MSG_CTL_CM_QUERY_KERBEROS", MSG_CTL_CM_QUERY_KERBEROS},
    {"MSG_CTL_CM_QUERY_KERBEROS_ACK", MSG_CTL_CM_QUERY_KERBEROS_ACK},
    {"MSG_AGENT_CM_DISKUSAGE_STATUS", MSG_AGENT_CM_DISKUSAGE_STATUS},
    {"MSG_CM_AGENT_OBS_DELETE_XLOG", MSG_CM_AGENT_OBS_DELETE_XLOG},
    {"MSG_CM_AGENT_DROP_CN_OBS_XLOG", MSG_CM_AGENT_DROP_CN_OBS_XLOG},
    {"MSG_AGENT_CM_DATANODE_INSTANCE_BARRIER", MSG_AGENT_CM_DATANODE_INSTANCE_BARRIER},
    {"MSG_CTL_CM_GLOBAL_BARRIER_QUERY", MSG_CTL_CM_GLOBAL_BARRIER_QUERY},
    {"MSG_AGENT_CM_COORDINATE_INSTANCE_BARRIER", MSG_AGENT_CM_COORDINATE_INSTANCE_BARRIER},
    {"MSG_CM_CTL_GLOBAL_BARRIER_DATA_BEGIN", MSG_CM_CTL_GLOBAL_BARRIER_DATA_BEGIN},
    {"MSG_CM_CTL_GLOBAL_BARRIER_DATA", MSG_CM_CTL_GLOBAL_BARRIER_DATA},
    {"MSG_CM_CTL_BARRIER_DATA_END", MSG_CM_CTL_BARRIER_DATA_END},
    {"MSG_CM_CTL_BACKUP_OPEN", MSG_CM_CTL_BACKUP_OPEN},
    {"MSG_CM_AGENT_DN_SYNC_LIST", MSG_CM_AGENT_DN_SYNC_LIST},
    {"MSG_AGENT_CM_DN_SYNC_LIST", MSG_AGENT_CM_DN_SYNC_LIST},
    {"MSG_CTL_CM_SWITCHOVER_FAST", MSG_CTL_CM_SWITCHOVER_FAST},
    {"MSG_CM_AGENT_SWITCHOVER_FAST", MSG_CM_AGENT_SWITCHOVER_FAST},
    {"MSG_CTL_CM_RELOAD", MSG_CTL_CM_RELOAD},
    {"MSG_CM_CTL_RELOAD_ACK", MSG_CM_CTL_RELOAD_ACK},
    {"MSG_CM_CTL_INVALID_COMMAND_ACK", MSG_CM_CTL_INVALID_COMMAND_ACK},
    {"MSG_AGENT_CM_CN_OBS_STATUS", MSG_AGENT_CM_CN_OBS_STATUS},
    {"MSG_CM_AGENT_NOTIFY_CN_RECOVER", MSG_CM_AGENT_NOTIFY_CN_RECOVER},
    {"MSG_CM_AGENT_FULL_BACKUP_CN_OBS", MSG_CM_AGENT_FULL_BACKUP_CN_OBS},
    {"MSG_AGENT_CM_BACKUP_STATUS_ACK", MSG_AGENT_CM_BACKUP_STATUS_ACK},
    {"MSG_CM_AGENT_REFRESH_OBS_DEL_TEXT", MSG_CM_AGENT_REFRESH_OBS_DEL_TEXT},
    {"MSG_AGENT_CM_INSTANCE_BARRIER_NEW", MSG_AGENT_CM_INSTANCE_BARRIER_NEW},
    {"MSG_CTL_CM_GLOBAL_BARRIER_QUERY_NEW", MSG_CTL_CM_GLOBAL_BARRIER_QUERY_NEW},
    {"MSG_CM_CTL_GLOBAL_BARRIER_DATA_BEGIN_NEW", MSG_CM_CTL_GLOBAL_BARRIER_DATA_BEGIN_NEW},
    {"MSG_CM_AGENT_DATANODE_INSTANCE_BARRIER", MSG_CM_AGENT_DATANODE_INSTANCE_BARRIER},
    {"MSG_CM_AGENT_COORDINATE_INSTANCE_BARRIER", MSG_CM_AGENT_COORDINATE_INSTANCE_BARRIER},
    {NULL, MSG_TYPE_BUTT},
};

static ObsBackupStatusMapString g_obsBackupMapping[] = {
    {"build start", OBS_BACKUP_PROCESSING},
    {"build failed", OBS_BACKUP_FAILED},
    {"build done", OBS_BACKUP_COMPLETED},
    {NULL, OBS_BACKUP_UNKNOWN},
};

int32 ObsStatusStr2Int(const char *statusStr)
{
    for (uint32 i = 0; g_obsBackupMapping[i].obsStatusStr != NULL; i++) {
        if (strcmp(g_obsBackupMapping[i].obsStatusStr, statusStr) == 0) {
            return g_obsBackupMapping[i].backupStatus;
        }
    }

    write_runlog(ERROR, "ObsStatusStr2Int failed, input status is: (%s)\n", statusStr);
    return OBS_BACKUP_UNKNOWN;
}

const char* cluster_msg_int_to_string(int cluster_msg)
{
    int i = 0;
    for (i = 0; cluster_msg_map_string[i].cluster_msg_str != NULL; i++) {
        if (cluster_msg_map_string[i].cluster_msg_val == cluster_msg) {
            return cluster_msg_map_string[i].cluster_msg_str;
        }
    }
    write_runlog(ERROR, "cluster_msg_int_to_string failed, input int cluster_msg is: (%d)\n", cluster_msg);
    return "Unknown message type";
}

instance_not_exist_reason_string instance_not_exist_reason[] = {
    {"unknown", UNKNOWN_BAD_REASON},
    {"check port fail", PORT_BAD_REASON},
    {"nic not up", NIC_BAD_REASON},
    {"data path disc writable test failed", DISC_BAD_REASON},
    {"stopped by users", STOPPED_REASON},
    {"cn deleted, please repair quickly", CN_DELETED_REASON},
    {NULL, MSG_TYPE_BUTT},
};

const char* instance_not_exist_reason_to_string(int reason)
{
    int i = 0;
    for (i = 0; instance_not_exist_reason[i].level_string != NULL; i++) {
        if (instance_not_exist_reason[i].level_val == reason) {
            return instance_not_exist_reason[i].level_string;
        }
    }
    return "unknown";
}

static void cm_init_block_sig(sigset_t* sleep_block_sig)
{
#ifdef SIGTRAP
    (void)sigdelset(sleep_block_sig, SIGTRAP);
#endif
#ifdef SIGABRT
    (void)sigdelset(sleep_block_sig, SIGABRT);
#endif
#ifdef SIGILL
    (void)sigdelset(sleep_block_sig, SIGILL);
#endif
#ifdef SIGFPE
    (void)sigdelset(sleep_block_sig, SIGFPE);
#endif
#ifdef SIGSEGV
    (void)sigdelset(sleep_block_sig, SIGSEGV);
#endif
#ifdef SIGBUS
    (void)sigdelset(sleep_block_sig, SIGBUS);
#endif
#ifdef SIGSYS
    (void)sigdelset(sleep_block_sig, SIGSYS);
#endif
}

void cm_sleep(unsigned int sec)
{
    sigset_t sleep_block_sig;
    sigset_t old_sig;
    (void)sigfillset(&sleep_block_sig);

    cm_init_block_sig(&sleep_block_sig);

    (void)sigprocmask(SIG_SETMASK, &sleep_block_sig, &old_sig);

    (void)sleep(sec);

    (void)sigprocmask(SIG_SETMASK, &old_sig, NULL);
}

void cm_usleep(unsigned int usec)
{
    sigset_t sleep_block_sig;
    sigset_t old_sig;
    (void)sigfillset(&sleep_block_sig);

    cm_init_block_sig(&sleep_block_sig);

    (void)sigprocmask(SIG_SETMASK, &sleep_block_sig, &old_sig);

    (void)usleep(usec);

    (void)sigprocmask(SIG_SETMASK, &old_sig, NULL);
}

uint32 get_healthy_etcd_node_count(EtcdTlsAuthPath* tlsPath, int programType)
{
    uint32 i;
    uint32 health_count = 0;
    uint32 unhealth_count = 0;
    bool findUnhealth = true;
    int logLevel = (programType == CM_CTL) ? DEBUG1 : ERROR;

    char* health = (char*)malloc(ETCD_STATE_LEN * sizeof(char));
    if (health == NULL) {
        write_runlog(logLevel, "malloc memory failed! size = %d\n", ETCD_STATE_LEN);
        exit(1);
    }
    errno_t rc = memset_s(health, ETCD_STATE_LEN, 0, ETCD_STATE_LEN);
    securec_check_errno(rc, );

    if (g_health_etcd_flag) {
        for (i = 0; i < g_health_etcd_count; i++) {
            uint32 etcd_index = g_health_etcd_index[i];
            if (etcd_index >= g_node_num || !g_node[etcd_index].etcd) {
                break;
            }

            int serverLen = 2;
            EtcdServerSocket server[serverLen];
            server[0].host = g_node[etcd_index].etcdClientListenIPs[0];
            server[0].port = g_node[etcd_index].etcdClientListenPort;
            server[1].host = NULL;
            EtcdSession sess = 0;
            int etcd_cluster_result = ETCD_OK;
            if (etcd_open(&sess, server, tlsPath, ETCD_DEFAULT_TIMEOUT) != 0) {
                const char* err_now = get_last_error();
                write_runlog(logLevel, "open etcd server %s failed: %s.\n", server[0].host, err_now);
                break;
            }

            etcd_cluster_result = etcd_cluster_health(sess, g_node[etcd_index].etcdName, health, ETCD_STATE_LEN);
            if (etcd_close(sess) != 0) {
                /* Only print error info */
                const char* err = get_last_error();
                write_runlog(WARNING, "etcd_close failed,%s\n", err);
            }

            if (etcd_cluster_result == 0) {
                if (0 == strcmp(health, "healthy")) {
                    health_count++;
                } else {
                    break;
                }
            } else {
                const char* err_now = get_last_error();
                write_runlog(logLevel, "etcd get all node health failed: %s.\n", err_now);
                break;
            }

            if (health_count > g_etcd_num / 2) {
                FREE_AND_RESET(health);
                return health_count;
            }
        }
    }
    health_count = 0;
    (void)pthread_rwlock_wrlock(&g_health_etcd_rwlock);
    g_health_etcd_count = 0;
    for (i = 0; i < g_node_num; i++) {
        if (g_node[i].etcd) {
            EtcdServerSocket server[2];
            server[0].host = g_node[i].etcdClientListenIPs[0];
            server[0].port = g_node[i].etcdClientListenPort;
            server[1].host = NULL;
            EtcdSession sess = 0;
            int etcd_cluster_result = ETCD_OK;
            if (etcd_open(&sess, server, tlsPath, ETCD_DEFAULT_TIMEOUT) != 0) {
                const char* err_now = get_last_error();
                write_runlog(logLevel, "open etcd server %s failed: %s.\n", server[0].host, err_now);
                continue;
            }
            etcd_cluster_result = etcd_cluster_health(sess, g_node[i].etcdName, health, ETCD_STATE_LEN);
            if (etcd_cluster_result == 0) {
                if (0 == strcmp(health, "healthy")) {
                    g_health_etcd_index[health_count] = i;
                    health_count++;
                } else {
                    unhealth_count++;
                }
            } else {
                unhealth_count++;
                const char* err_now = get_last_error();
                write_runlog(logLevel, "etcd get node %s health state failed: %s.\n", g_node[i].etcdName, err_now);
            }

            if (etcd_close(sess) != 0) {
                /* Only print error info */
                const char* err = get_last_error();
                write_runlog(WARNING, "etcd_close failed,%s\n", err);
            }

            if (health_count > g_etcd_num / 2) {
                findUnhealth = false;
                break;
            }

            if (unhealth_count > g_etcd_num / 2) {
                break;
            }
        }
    }
    if (findUnhealth) {
        g_health_etcd_flag = false;
    } else {
        g_health_etcd_flag = true;
        g_health_etcd_count = health_count;
    }
    (void)pthread_rwlock_unlock(&g_health_etcd_rwlock);

    FREE_AND_RESET(health);
    return health_count;
}

void check_input_for_security(const char* input)
{
    char* danger_token[] = {"|", ";", "&", "$", "<", ">", "`", "\\", "!", "\n", NULL};
    int i = 0;
    for (i = 0; danger_token[i] != NULL; i++) {
        if (strstr(input, danger_token[i]) != NULL) {
            printf("invalid token \"%s\" in string: %s.", danger_token[i], input);
            exit(1);
        }
    }
}

/* CAUTION: the env value MPPDB_ENV_SEPARATE_PATH does not exist in some system */
int cm_getenv(const char* env_var, char* output_env_value, uint32 env_value_len, syscalllock cmLock, int elevel)
{
    char* env_value = NULL;
    elevel = (elevel == -1) ? ERROR : elevel;

    if (env_var == NULL) {
        write_runlog(elevel, "cm_getenv: invalid env_var !\n");
        return -1;
    }

    (void)syscalllockAcquire(&cmLock);
    env_value = getenv(env_var);

    if (env_value == NULL || env_value[0] == '\0') {

        if (strcmp(env_var, "MPPDB_KRB5_FILE_PATH") == 0 ||
            strcmp(env_var, "KRB_HOME") == 0 ||
            strcmp(env_var, "MPPDB_ENV_SEPARATE_PATH") == 0) {

            /* MPPDB_KRB5_FILE_PATH, KRB_HOME, MPPDB_ENV_SEPARATE_PATH is not necessary,
                and do not print failed to get environment log */
            (void)syscalllockRelease(&cmLock);
            return -1;
        } else {
            write_runlog(elevel,
                "cm_getenv: failed to get environment variable:%s. Please check and make sure it is configured!\n",
                env_var);
        }
        (void)syscalllockRelease(&cmLock);
        return -1;
    }
    check_env_value(env_value);

    int rc = strcpy_s(output_env_value, env_value_len, env_value);
    if (rc != EOK) {
        write_runlog(elevel,
            "cm_getenv: failed to get environment variable %s , variable length:%lu.\n",
            env_var,
            strlen(env_value));
        (void)syscalllockRelease(&cmLock);
        return -1;
    }

    (void)syscalllockRelease(&cmLock);
    return EOK;
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
            fprintf(
                stderr, "invalid token \"%s\" in input_env_value: (%s)\n", danger_character_list[i], input_env_value);
            exit(1);
        }
    }
}

void print_environ(void)
{
    int i;

    write_runlog(LOG, "begin printing environment variables.\n");
    for (i = 0; environ[i] != NULL; i++) {
        write_runlog(LOG, "%s\n", environ[i]);
    }
    write_runlog(LOG, "end printing environment variables\n");
}

void cm_pthread_rw_lock(pthread_rwlock_t* rwlock)
{
    int ret = pthread_rwlock_wrlock(rwlock);
    if (ret != 0) {
        write_runlog(ERROR, "pthread_rwlock_wrlock failed.\n");
        exit(1);
    }
}

void cm_pthread_rw_unlock(pthread_rwlock_t* rwlock)
{
    int ret = pthread_rwlock_unlock(rwlock);
    if (ret != 0) {
        write_runlog(ERROR, "pthread_rwlock_unlock failed.\n");
        exit(1);
    }
}

/**
 * @brief Creates a lock file for a process with a specified PID.
 * 
 * @note When the parameter "pid" is set to -1, the specified process is the current process.
 * @param  filename         The name of the lockfile to create.
 * @param  data_path        The data path of the instance.
 * @param  pid              The pid of the process.
 * @return 0 Create successfully, -1 Create failure.
 */
int create_lock_file(
        const char* filename,
        const char* data_path,
        const pid_t pid)
{
    int         fd;
    char        buffer[MAXPGPATH + 100] = { 0 };
    const pid_t my_pid = (pid >= 0) ? pid : getpid();
    int         try_times = 0;

    do
    {
        /* The maximum number of attempts is 3. */
        if (try_times++ > 3) {
            write_runlog(ERROR, "could not create lock file: filename=\"%s\", error_no=%d.\n", filename, errno);
            return -1;
        }

        /* Attempt to create a specified PID file. */
        fd = open(filename, O_RDWR | O_CREAT | O_EXCL, 0600);
        if (fd >= 0) {
            break;
        }

        /* If the creation fails, try to open the existing pid file. */
        fd = open(filename, O_RDONLY | O_CLOEXEC, 0600);
        if (fd < 0) {
            write_runlog(ERROR, "could not open lock file: filename=\"%s\", error_no=%d.\n", filename, errno);
            return EEXIST;
        }

        /* If the file is opened successfully, the system attempts to read the file content. */
        int len = read(fd, buffer, sizeof(buffer) - 1);
        (void)close(fd);
        if (len < 0 || len >= (MAXPGPATH + 100)) {
            write_runlog(ERROR, "could not read lock file: filename=\"%s\", error_no=%d.\n", filename, errno);
            return EEXIST;
        }

        /* Obtains the PID information in a PID file. */
        const pid_t other_pid = static_cast<pid_t>(atoi(buffer));
        if (other_pid <= 0) {
            write_runlog(ERROR,
                "bogus data in lock file: filename=\"%s\", buffer=\"%s\", error_no=%d.\n",
                filename, buffer, errno);
            return EEXIST;
        }

        /* If the obtained PID is not the specified process ID or parent process ID. */
        if (other_pid != my_pid
#ifndef WIN32
            && other_pid != getppid()
#endif
                ) {
            /* Sends signals to the specified PID. */
            if (kill(other_pid, 0) == 0 || (errno != ESRCH && errno != EPERM)) {
                write_runlog(WARNING,
                    "lock file \"%s\"  exists, Is another instance (PID %d) running in data directory \"%s\"?\n",
                    filename, (int)(other_pid), data_path);
            }
        }

        /* Attempt to delete the specified PID file. */
        if (unlink(filename) < 0) {
            write_runlog(ERROR,
                "could not remove old lock file \"%s\", The file seems accidentally"
                " left over, but it could not be removed. Please remove the file by hand and try again: errno=%d.\n",
                filename, errno);
            return -1;
        }
    } while (true);

    int rc = snprintf_s(buffer, sizeof(buffer), sizeof(buffer) - 1, "%d\n%s\n%d\n", (int)(my_pid), data_path, 0);
    securec_check_intval(rc, );

    /* Writes PID information. */
    errno = 0;
    if (write(fd, buffer, strlen(buffer)) != (int)(strlen(buffer))) {
        write_runlog(ERROR, "could not write lock file: filename=\"%s\", error_no=%d.\n", filename, errno);

        close(fd);
        unlink(filename);
        return EEXIST;
    }

    /* Close the pid file. */
    if (close(fd)) {
        write_runlog(FATAL, "could not write lock file: filename=\"%s\", error_no=%d.\n", filename, errno);

        unlink(filename);
        return -1;
    }

    return 0;
}

/**
 * @brief Delete pid file.
 * 
 * @param  filename         The pid file to be deleted.
 */
void delete_lock_file(const char* filename)
{
    struct stat stat_buf = {0};

    /* Check whether the pid file exists. */
    if (stat(filename, &stat_buf) != 0) {
        return;
    }

    /* Delete the PID file. */
    if (unlink(filename) < 0) {
        write_runlog(FATAL, "could not remove old lock file \"%s\"", filename);
    }
}

/* kerberos status to string */
const char* kerberos_status_to_string(int role)
{
    if (role <= KERBEROS_STATUS_UNKNOWN || role > KERBEROS_STATUS_DOWN) {
        return kerberos_role_string_map[KERBEROS_STATUS_UNKNOWN].role_string;
    } else {
        return kerberos_role_string_map[role].role_string;
    }
}

static void SigAlarmHandler(int arg)
{
    ;
}

int CmExecuteCmd(const char* command, struct timeval timeout)
{
#ifndef WIN32
    pid_t pid;
    pid_t child = 0;
    struct sigaction intact = {};
    struct sigaction quitact = {};
    sigset_t newsigblock, oldsigblock;
    struct itimerval write_timeout;
    errno_t rc;

    if (command == NULL) {
        write_runlog(ERROR, "ExecuteCmd invalid command.\n");
        return 1;
    }
    /*
     * Ignore SIGINT and SIGQUIT, block SIGCHLD. Remember to save existing
     * signal dispositions.
     */
    struct sigaction ign = {};
    rc = memset_s(&ign, sizeof(struct sigaction), 0, sizeof(struct sigaction));
    securec_check_errno(rc, (void)rc);
    ign.sa_handler = SIG_IGN;
    (void)sigemptyset(&ign.sa_mask);
    ign.sa_flags = 0;
    (void)sigaction(SIGINT, &ign, &intact);
    (void)sigaction(SIGQUIT, &ign, &quitact);
    (void)sigemptyset(&newsigblock);
    (void)sigaddset(&newsigblock, SIGCHLD);
    (void)sigprocmask(SIG_BLOCK, &newsigblock, &oldsigblock);

    switch (pid = fork()) {
        case -1: /* error */
            break;
        case 0: /* child */

            /*
             * Restore original signal dispositions and exec the command.
             */
            (void)sigaction(SIGINT, &intact, NULL);
            (void)sigaction(SIGQUIT, &quitact, NULL);
            (void)sigprocmask(SIG_SETMASK, &oldsigblock, NULL);
            (void)execl("/bin/sh", "sh", "-c", command, (char*)0);
            _exit(127);
            break;
        default:
            /* wait the child process end ,if timeout then kill the child process force */
            write_runlog(LOG, "ExecuteCmd: %s, pid:%d. start!\n", command, pid);
            write_timeout.it_value.tv_sec = timeout.tv_sec;
            write_timeout.it_value.tv_usec = timeout.tv_usec;
            write_timeout.it_interval.tv_sec = 0;
            write_timeout.it_interval.tv_usec = 0;
            child = pid;
            (void)setitimer(ITIMER_REAL, &write_timeout, NULL);
            (void)signal(SIGALRM, SigAlarmHandler);
            if (pid != waitpid(pid, NULL, 0)) {
                /* kill child process */
                (void)kill(child, SIGKILL);
                pid = -1;
                /* avoid the zombie process */
                (void)wait(NULL);
            }
            write_runlog(LOG, "ExecuteCmd: %s, pid:%d. end!\n", command, pid);
            (void)signal(SIGALRM, SIG_IGN);
            break;
    }
    (void)sigaction(SIGINT, &intact, NULL);
    (void)sigaction(SIGQUIT, &quitact, NULL);
    (void)sigprocmask(SIG_SETMASK, &oldsigblock, NULL);
    if (pid == -1) {
        write_runlog(ERROR, "ExecuteCmd: %s, failed errno:%d.\n", command, errno);
    }
    return ((pid == -1) ? -1 : 0);
#else
    return -1;
#endif
}

int CmInitMasks(const int* ListenSocket, fd_set* rmask)
{
    int maxsock = -1;
    int i;

    FD_ZERO(rmask);

    for (i = 0; i < MAXLISTEN; i++) {
        int fd = ListenSocket[i];

        if (fd == -1) {
            break;
        }
        FD_SET(fd, rmask);
        if (fd > maxsock) {
            maxsock = fd;
        }
    }

    return maxsock + 1;
}
