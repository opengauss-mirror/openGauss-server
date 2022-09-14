/* -------------------------------------------------------------------------
 *
 * gtm_client.h
 *
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL$
 *
 * -------------------------------------------------------------------------
 */
#ifndef GTM_CLIENT_H
#define GTM_CLIENT_H

#include "gtm/gtm_c.h"
#include "gtm/gtm_seq.h"
#include "gtm/gtm_txn.h"
#include "gtm/gtm_msg.h"
#include "gtm/utils/register.h"
#include "gtm/utils/libpq-fe.h"

/* DFX: client error code for connecting to GTM */
typedef enum GTMClientErrCode {
    GTMC_OK = 0,
    GTMC_SEND_ERROR,
    GTMC_RECV_ERROR
} GTMClientErrCode;

typedef enum GTM_CONN_ERR {
    GTM_OK,
    GTM_UNCONNECTED = 0xE,
    GTM_START_ERROR,
    GTM_SEND_ERROR,
    GTM_SEND_END_ERROR,
    GTM_FLUSH_ERROR,
    GTM_RECEIVE_ERROR,
} GTM_CONN_ERR_T;

typedef union GTM_ResultData {
    GTM_TransactionHandle grd_txnhandle; /* TXN_BEGIN */

    struct {
        GlobalTransactionId gxid;
        GTM_Timestamp timestamp;
    } grd_gxid_tp; /* TXN_BEGIN_GETGXID */

    GlobalTransactionId grd_gxid; /* TXN_PREPARE
                                   * TXN_START_PREPARED
                                   * TXN_GET_GLOBAL_XMIN_RESULT
                                   */
    struct {
        GlobalTransactionId gxid;
        int status;
    } grd_txn_rc; /*
                   * TXN_COMMIT
                   * TXN_COMMIT_PREPARED
                   * TXN_ROLLBACK
                   */

    GlobalTransactionId grd_next_gxid;

    struct {
        GTM_TransactionHandle txnhandle;
        GlobalTransactionId gxid;
    } grd_txn; /* TXN_GET_GXID */

    GTM_SequenceKeyData grd_seqkey; /* SEQUENCE_INIT
                                     * SEQUENCE_RESET
                                     * SEQUENCE_CLOSE */

    GTM_DBNameData grd_seqdb; /* SEQUENCE_RENAME */

    struct {
        GTM_UUID seq_uuid;
    } grd_uuid; /* SEQUENCE_GET_UUID */

    struct {
        GTM_UUID seq_uuid;
        GTM_Sequence seqval;
        GTM_Sequence rangemax;
    } grd_seq; /* SEQUENCE_GET_NEXT */

    struct {
        int seq_count;
        GTM_SeqInfo* seq;
    } grd_seq_list; /* SEQUENCE_GET_LIST */

    struct {
        int txn_count; /* TXN_BEGIN_GETGXID_MULTI */
        GlobalTransactionId start_gxid;
        GTM_Timestamp timestamp;
    } grd_txn_get_multi;

    struct {
        int txn_count; /* TXN_COMMIT_MULTI */
        int status[GTM_MAX_GLOBAL_TRANSACTIONS];
    } grd_txn_rc_multi;

    struct {
        GTM_TransactionHandle txnhandle; /* SNAPSHOT_GXID_GET */
        GlobalTransactionId gxid;        /* SNAPSHOT_GET */
        int txn_count;                   /* SNAPSHOT_GET_MULTI */
        int status[GTM_MAX_GLOBAL_TRANSACTIONS];
    } grd_txn_snap_multi;

    struct {
        GlobalTransactionId gxid;
        GlobalTransactionId prepared_gxid;
        int nodelen;
        char* nodestring;
    } grd_txn_get_gid_data; /* TXN_GET_GID_DATA_RESULT */

    struct {
        char* ptr;
        size_t len;
    } grd_txn_gid_list; /* TXN_GXID_LIST_RESULT */

    struct {
        GTM_PGXCNodeType type; /* NODE_REGISTER */
        size_t len;
        char* node_name; /* NODE_UNREGISTER */
    } grd_node;

    struct {
        int num_node;
        GTM_PGXCNodeInfo* nodeinfo[MAX_NODES];
    } grd_node_list;

    struct {
        int server_mode;
        int connection_status;
        GlobalTransactionId xid;
        long send_count;
        long receive_count;
        int sync_mode;
    } grd_query_gtm_status; /*MSG_QUREY_GTM_STATUS*/

    int grd_gtm_sync_mode_result;

    int32 grd_check_standby_role_result; /*MSG_CHECK_STANDBY_ROLE*/

    int32 grd_sync_file_result; /*MSG_SYNC_FILE*/

    int32 grd_check_next_gxid_result; /*MSG_SYNC_FILE*/

    struct {
        int max_server_memory;
        double query_memory_limit;
    } grd_workload_manager_init_result; /*MSG_TXN_WORKLOAD_INIT*/

    int32 grd_create_resource_pool_result; /*MSG_WLM_RESOURCEPOOL_CREATE*/
    int32 grd_update_resource_pool_result; /*MSG_WLM_RESOURCEPOOL_UPDATE*/
    int32 grd_delete_resource_pool_result; /*MSG_WLM_RESOURCEPOOL_DELETE*/
    int32 grd_init_resource_pool_result;   /*MSG_WLM_RESOURCEPOOL_INIT*/

    int32 grd_set_disaster_cluster_result;
    int32 grd_del_disaster_cluster_result;
    struct {
        int32 length;
        char* result;
    } grd_get_disaster_cluster_result;

    struct {
        int32 length;
        char* result;
    } gtm_hotpatch_result;
    /*
     * 	TXN_GET_STATUS
     * 	TXN_GET_ALL_PREPARED
     */
} GTM_ResultData;

#define GTM_RESULT_COMM_ERROR (-2) /* Communication error */
#define GTM_RESULT_ERROR (-1)
#define GTM_RESULT_OK (0)
/*
 * This error is used ion the case where allocated buffer is not large
 * enough to store the errors. It may happen of an allocation failed
 * so it's status is considered as unknown.
 */
#define GTM_RESULT_UNKNOWN (1)

typedef struct GTM_Result {
    GTM_ResultType gr_type;
    int gr_msglen;
    int gr_status;
    GTM_ProxyMsgHeader gr_proxyhdr;
    GTM_ResultData gr_resdata;
    GTM_Timeline gr_timeline; /*GTM start status, now mean restart times*/
    uint64 csn;               /* snapshot csn or commit csn */
    /*
     * We keep these two items outside the union to avoid repeated malloc/free
     * of the xip array. If these items are pushed inside the union, they may
     * get overwritten by other members in the union
     */
    int gr_xip_size;
    GTM_SnapshotData gr_snapshot;
    GTM_SnapshotStatusData gr_snapshot_status;
    GTMLite_StatusData gr_gtm_lite_status;

    /*
     * Similarly, keep the buffer for proxying data outside the union
     */
    char* gr_proxy_data;
    int gr_proxy_datalen;
} GTM_Result;

/*
 * Connection Management API
 */
GTM_Conn* connect_gtm(const char* connect_string);

int get_node_list(GTM_Conn*, GTM_PGXCNodeInfo*, size_t);
GlobalTransactionId get_next_gxid(GTM_Conn*);
CommitSeqNo get_next_csn(GTM_Conn* conn, bool is_gtm_lite);
void get_next_csn_time(GTM_Conn* conn, bool is_gtm_lite, CommitSeqNo* csn, GTM_Timestamp* timestamp);
TransactionId get_global_xmin(GTM_Conn* conn);
GTM_Timeline get_gtm_timeline(GTM_Conn* conn);
int32 get_txn_gxid_list(GTM_Conn*, GTM_Transactions*);
int get_sequence_list(GTM_Conn*, GTM_SeqInfo**);

/*
 * Transaction Management API
 */
int reset_gtm_handle_xmin(GTM_Conn* conn, GTM_IsolationLevel isolevel, GTM_TransactionKey txnKey);
int set_gtm_vaccum_flag(GTM_Conn* conn, bool is_vaccum, GTM_TransactionKey txnKey);
GTM_TransactionKey begin_transaction(GTM_Conn* conn, GTM_IsolationLevel isolevel, GTM_Timestamp* timestamp);
GlobalTransactionId begin_get_gxid(GTM_Conn* conn, GTM_TransactionKey txn, bool is_sub_xact, GTMClientErrCode* err);

bool begin_set_disaster_cluster(GTM_Conn* conn, char* disasterCluster, GTMClientErrCode* err);
bool begin_get_disaster_cluster(GTM_Conn* conn, char** disasterCluster, GTMClientErrCode* err);
bool begin_del_disaster_cluster(GTM_Conn* conn, GTMClientErrCode* err);

GlobalTransactionId begin_transaction_gxid(GTM_Conn* conn, GTM_IsolationLevel isolevel, GTM_Timestamp* timestamp);

int bkup_gtm_control_file_gxid(GTM_Conn* conn, GlobalTransactionId gxid);
int bkup_gtm_control_file_timeline(GTM_Conn* conn, GTM_Timeline timeline);

GlobalTransactionId begin_transaction_autovacuum(GTM_Conn* conn, GTM_IsolationLevel isolevel);

int commit_transaction(GTM_Conn *conn, GlobalTransactionId gxid, GlobalTransactionId *childXids, int nChildXids,
                       uint64 *commit_csn = NULL);
int commit_transaction_handle(GTM_Conn *conn, GTM_TransactionKey txnKey, GlobalTransactionId *gxid, bool is_backup,
                              uint64 *commit_csn);
int bkup_commit_transaction(GTM_Conn *conn, GlobalTransactionId gxid);

int commit_prepared_transaction(GTM_Conn* conn, GlobalTransactionId gxid, GlobalTransactionId prepared_gxid);
int bkup_commit_prepared_transaction(GTM_Conn* conn, GlobalTransactionId gxid, GlobalTransactionId prepared_gxid);
int abort_transaction(GTM_Conn* conn, GlobalTransactionId gxid, GlobalTransactionId* childXids, int nChildXids);
int abort_transaction_handle(GTM_Conn* conn, GTM_TransactionKey txnKey, GlobalTransactionId* gxid, bool is_backup);
int bkup_abort_transaction(GTM_Conn* conn, GlobalTransactionId gxid);

int start_prepared_transaction(GTM_Conn* conn, GlobalTransactionId gxid, const char* gid, const char* nodestring);
int prepare_transaction(GTM_Conn* conn, GlobalTransactionId gxid);
int bkup_prepare_transaction(GTM_Conn* conn, GlobalTransactionId gxid);
int get_gid_data(GTM_Conn* conn, GTM_IsolationLevel isolevel, const char* gid, GlobalTransactionId* gxid,
    GlobalTransactionId* prepared_gxid, char** nodestring);

bool set_consistency_point_csn(GTM_Conn* conn, CommitSeqNo consistencyPointCSN, GTMClientErrCode* err);
bool set_consistency_point(GTM_Conn* conn, CommitSeqNo consistencyPointCSN, const char* cnName, GTMClientErrCode* err);

/*
 * Snapshot Management API
 */
GTM_SnapshotData *get_snapshot(GTM_Conn *conn, GTM_TransactionKey txnKey, GlobalTransactionId gxid, bool canbe_grouped,
                               bool is_vacuum);
GTM_SnapshotData *get_snapshot_gtm_lite(GTM_Conn *conn);
GTM_SnapshotData *get_snapshot_gtm_dr(GTM_Conn *conn);
GTM_SnapshotData *get_snapshot_gtm_disaster(GTM_Conn *conn, const char* cnName);
GTMLite_StatusData *get_status_gtm_lite(GTM_Conn *conn);


GTM_SnapshotStatusData* get_snapshot_status(GTM_Conn* conn, GTM_TransactionKey key);

/*
 * Node Registering management API
 */
int node_register(GTM_Conn *conn, GTM_PGXCNodeType type, GTM_PGXCNodePort port, char *node_name, char *datafolder,
                  uint32 timeline);

int node_register_internal(GTM_Conn* conn, GTM_PGXCNodeType type, const char* host, GTM_PGXCNodePort port,
    const char* node_name, const char* datafolder, GTM_PGXCNodeStatus status);
int bkup_node_register_internal(GTM_Conn* conn, GTM_PGXCNodeType type, const char* host, GTM_PGXCNodePort port,
    const char* node_name, const char* datafolder, GTM_PGXCNodeStatus status);
int node_unregister(GTM_Conn* conn, GTM_PGXCNodeType type, const char* node_name);
int bkup_node_unregister(GTM_Conn* conn, GTM_PGXCNodeType type, const char* node_name);
int backend_disconnect(GTM_Conn* conn, bool is_postmaster, GTM_PGXCNodeType type, const char* node_name);

/*
 * Sequence Management API
 */
GTM_UUID get_next_sequuid(GTM_Conn* conn);
int get_uuid(GTM_Conn* conn, GTM_UUID* result, GTMClientErrCode* err);
int bkup_seq_uuid(GTM_Conn* conn, GTM_UUID seq_uuid);

int open_sequence(GTM_Conn *conn, GTM_UUID seq_uuid, GTM_Sequence increment, GTM_Sequence minval, GTM_Sequence maxval,
                  GTM_Sequence startval, bool cycle, GTM_DBName seq_dbName, GTMClientErrCode *err = NULL);
int bkup_open_sequence(GTM_Conn *conn, GTM_UUID seq_uuid, GTM_Sequence increment, GTM_Sequence minval,
                       GTM_Sequence maxval, GTM_Sequence startval, bool cycle, GTM_DBName seq_dbName,
                       GTMClientErrCode *err);
int alter_sequence(GTM_Conn *conn, GTM_UUID seq_uuid, GTM_Sequence increment, GTM_Sequence minval, GTM_Sequence maxval,
                   GTM_Sequence startval, GTM_Sequence lastval, bool cycle, bool is_restart);
int bkup_alter_sequence(GTM_Conn *conn, GTM_UUID seq_uuid, GTM_Sequence increment, GTM_Sequence minval,
                        GTM_Sequence maxval, GTM_Sequence startval, GTM_Sequence lastval, bool cycle, bool is_restart);
int close_sequence(GTM_Conn *conn, GTM_UUID seq_uuid, GTM_DBName seq_dbName);
int bkup_close_sequence(GTM_Conn *conn, GTM_UUID seq_uuid, GTM_DBName seq_dbName);
int rename_sequence(GTM_Conn *conn, GTM_DBName dbName, GTM_DBName newdbName);
int bkup_rename_sequence(GTM_Conn *conn, GTM_DBName dbName, GTM_DBName newdbName);

int get_next(GTM_Conn* conn, GTM_UUID seq_uuid, GTM_Sequence* result, GTM_Sequence increment, GTM_Sequence minval,
    GTM_Sequence maxval, GTM_Sequence range, GTM_Sequence* rangemax, bool cycle);
int bkup_get_next(GTM_Conn* conn, GTM_UUID seq_uuid, GTM_Sequence* result, GTM_Sequence increment, GTM_Sequence minval,
    GTM_Sequence maxval, GTM_Sequence range, GTM_Sequence* rangemax, bool cycle);
int set_val(GTM_Conn* conn, GTM_UUID seq_uuid, GTM_Sequence nextval, bool is_called);
int bkup_set_val(GTM_Conn* conn, GTM_UUID seq_uuid, GTM_Sequence nextval, GTM_Sequence range, bool is_called);
int reset_sequence(GTM_Conn* conn, GTM_UUID seq_uuid);
int bkup_reset_sequence(GTM_Conn* conn, GTM_UUID seq_uuid);

/*
 * Barrier
 */
int report_barrier(GTM_Conn* conn, const char* barier_id);
int bkup_report_barrier(GTM_Conn* conn, const char* barrier_id);

/*
 * GTM-Standby
 */
int set_begin_end_backup(GTM_Conn* conn, bool begin);
int gtm_sync_standby(GTM_Conn* conn, bool autosync);
int set_begin_end_switchover(GTM_Conn* conn, bool begin);
int gtm_check_remote_role(GTM_Conn* conn, int32 port, int32* result);
int sync_files_to_standby(GTM_Conn* conn, int filetype, const char* files, int len);

/*
 *  * workload management API
 *   */
int bkup_initialize_workload_manager(GTM_Conn* conn, int initServerMem);
int initialize_workload_manager(GTM_Conn* conn, int* max_server_memory, double* query_memory_limit);
int bkup_reserve_memory_wlm(GTM_Conn* conn, int memoryCost, const char*, int);
int reserve_memory_wlm(GTM_Conn* conn, int memoryCost, const char*, int);
int bkup_release_memory_wlm(GTM_Conn* conn, int memoryReserve, const char*, int);
int release_memory_wlm(GTM_Conn* conn, int memoryReserve, const char*, int);

int CreateUpdateResourcePool(GTM_Conn* conn, bool create, char* rpName, int rpId, int memPercentage);
int DeleteResourcePool(GTM_Conn* conn, char* rpName, int rpId);
int InitResourcePool(GTM_Conn* conn, int rp_count, int buf_len, char* buf);

/* Set parameter API */
void set_gtm_client_rw_timeout(int timeout);
extern void process_for_gtm_connection_failed(GTM_Conn* conn);
#endif
