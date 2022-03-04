/* ---------------------------------------------------------------------------------------
 * 
 * cm_misc.h
 *	  Declarations/definitions for "StringInfo" functions.
 *
 * StringInfo provides an indefinitely-extensible string data type.
 * It can be used to buffer either ordinary C strings (null-terminated text)
 * or arbitrary binary data.  All storage is allocated with palloc().
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * 
 * 
 * IDENTIFICATION
 *        src/include/cm/cm_misc.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef CM_MISC_H
#define CM_MISC_H

#include "utils/syscall_lock.h"
#include "cm/etcdapi.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct instance_not_exist_reason_string {
    const char* level_string;
    int level_val;
} instance_not_exist_reason_string;

typedef struct log_level_string {
    const char* level_string;
    int level_val;
} log_level_string;

typedef struct instance_datanode_build_reason_string {
    const char* reason_string;
    int reason_val;
} instance_datanode_build_reason_string;

typedef struct instacne_type_string {
    const char* type_string;
    int type_val;
} instacne_type_string;

typedef struct gtm_con_string {
    const char* con_string;
    int con_val;
} gtm_con_string;

typedef struct instance_coordinator_active_status_string {
    const char* active_status_string;
    int active_status_val;
} instance_coordinator_active_status_string;

typedef struct instance_datanode_lockmode_string {
    const char* lockmode_string;
    uint32 lockmode_val;
} instance_datanode_lockmode_string;

typedef struct instacne_datanode_role_string {
    const char* role_string;
    uint32 role_val;
} instacne_datanode_role_string;

typedef struct instacne_datanode_dbstate_string {
    const char* dbstate_string;
    int dbstate_val;
} instacne_datanode_dbstate_string;

typedef struct instacne_datanode_wal_send_state_string {
    const char* wal_send_state_string;
    int wal_send_state_val;
} instacne_datanode_wal_send_state_string;

typedef struct instacne_datanode_sync_state_string {
    const char* wal_sync_state_string;
    int wal_sync_state_val;
} instacne_datanode_sync_state_string;

typedef struct cluster_state_string {
    const char* cluster_state_string;
    int cluster_state_val;
} cluster_state_string;

typedef struct cluster_msg_string {
    const char* cluster_msg_str;
    int cluster_msg_val;
} cluster_msg_string;

typedef struct ObsBackupStatusMapString_t {
    const char *obsStatusStr;
    int backupStatus;
} ObsBackupStatusMapString;

typedef struct server_role_string {
    int role_val;
    const char* role_string;
} server_role_string;

#define LOCK_FILE_LINE_PID 1
#define LOCK_FILE_LINE_DATA_DIR 2
#define LOCK_FILE_LINE_START_TIME 3
#define LOCK_FILE_LINE_PORT 4
#define LOCK_FILE_LINE_SOCKET_DIR 5
#define LOCK_FILE_LINE_LISTEN_ADDR 6
#define LOCK_FILE_LINE_SHMEM_KEY 7

#ifndef ERROR_LIMIT_LEN
#define ERROR_LIMIT_LEN 256
#endif

/**
 * @def SHELL_RETURN_CODE
 * @brief Get the shell command return code.
 * @return Return the shell command return code.
 */
#define SHELL_RETURN_CODE(systemReturn) \
    (systemReturn > 0 ? static_cast<int>(static_cast<uint32>(systemReturn) >> 8) : systemReturn)

extern char** readfile(const char* path);
extern void freefile(char** lines);
extern int log_level_string_to_int(const char* log_level);
extern int datanode_rebuild_reason_string_to_int(const char* reason);
extern const char* DcfRoleToString(int role);
extern const char* instance_not_exist_reason_to_string(int reason);
extern int datanode_lockmode_string_to_int(const char* lockmode);
extern int datanode_role_string_to_int(const char* role);
extern int datanode_dbstate_string_to_int(const char* dbstate);
extern int datanode_wal_send_state_string_to_int(const char* dbstate);
extern int datanode_wal_sync_state_string_to_int(const char* dbstate);
extern const char* log_level_int_to_string(int log_level);
extern const char* cluster_state_int_to_string(int cluster_state);
extern const char* cluster_msg_int_to_string(int cluster_msg);
extern int32 ObsStatusStr2Int(const char *statusStr);
extern const char* datanode_wal_sync_state_int_to_string(int dbstate);
extern const char* datanode_wal_send_state_int_to_string(int dbstate);

extern const char* datanode_dbstate_int_to_string(int dbstate);
extern const char* type_int_to_string(int type);
const char* gtm_con_int_to_string(int con);
extern const char* datanode_role_int_to_string(int role);
extern const char* datanode_static_role_int_to_string(uint32 role);
extern const char* datanode_rebuild_reason_int_to_string(int reason);
extern const char* server_role_to_string(int role, bool is_pending);
extern const char* etcd_role_to_string(int role);
extern const char* kerberos_status_to_string(int role);

extern void cm_sleep(unsigned int sec);
extern void cm_usleep(unsigned int usec);

extern uint32 get_healthy_etcd_node_count(EtcdTlsAuthPath* tlsPath, int programType);

extern void check_input_for_security(const char* input);
extern void check_env_value(const char* input_env_value);

extern void print_environ(void);

/**
 * @brief
 *  Creates a lock file for a process with a specified PID.
 *
 * @note
 *  When pid is set to -1, the specified process is the current process.
 *
 * @param [in] filename
 *  The name of the lockfile to create.
 * @param [in] data_path
 *  The data path of the instance.
 * @param [in] pid
 *  The pid of the process.
 *
 * @return
 *  - 0     Create successfully.
 *  - -1    Create failure.
 */
extern int create_lock_file(const char* filename, const char* data_path, const pid_t pid = -1);


/**
 * @brief
 *  Delete pid file.
 *
 * @param [in] filename
 *  The pid file to be deleted.
 *
 * @return
 *  void.
 */
extern void delete_lock_file(const char *filename);

extern void cm_pthread_rw_lock(pthread_rwlock_t* rwlock);
extern void cm_pthread_rw_unlock(pthread_rwlock_t* rwlock);

extern int cm_getenv(
    const char* env_var, char* output_env_value, uint32 env_value_len, syscalllock cmLock, int elevel = -1);

extern int CmExecuteCmd(const char* command, struct timeval timeout);
extern int CmInitMasks(const int* ListenSocket, fd_set* rmask);

#ifdef __cplusplus
}
#endif

#endif
