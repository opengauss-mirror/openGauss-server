/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * DMS is licensed under Mulan PSL v2.
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
 * dms_api.h
 *
 *
 * IDENTIFICATION
 *    src/interface/dms_api.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __DMS_API_H__
#define __DMS_API_H__

#include <stdlib.h>
#ifdef __cplusplus
extern "C" {
#endif

#define DMS_LOCAL_MAJOR_VER_WEIGHT  1000000
#define DMS_LOCAL_MINOR_VER_WEIGHT  1000
#define DMS_LOCAL_MAJOR_VERSION     0
#define DMS_LOCAL_MINOR_VERSION     0
#define DMS_LOCAL_VERSION           135

#define DMS_SUCCESS 0
#define DMS_ERROR (-1)
#ifdef OPENGAUSS
#define DMS_PAGEID_SIZE         24  // openGauss bufferTag size
#else
#define DMS_PAGEID_SIZE         16
#endif

#define DMS_XID_SIZE            12
#define DMS_INSTANCES_SIZE      4
#define DMS_ROWID_SIZE          16
#define DMS_INDEX_PROFILE_SIZE  96
#define DMS_MAX_IP_LEN          64
#define DMS_MAX_INSTANCES       64
#define DMS_MAX_NAME_LEN        64

#define DMS_VERSION_MAX_LEN     256
#define DMS_OCK_LOG_PATH_LEN    256
#define DMS_LOG_PATH_LEN        (256)

// The values of the following two macros must be same with (GS_MAX_XA_BASE16_GTRID_LEN GS_MAX_XA_BASE16_BQUAL_LEN)
#define DMS_MAX_XA_BASE16_GTRID_LEN    (128)
#define DMS_MAX_XA_BASE16_BQUAL_LEN    (128)

typedef enum en_dms_online_status {
    DMS_ONLINE_STATUS_OUT = 0,
    DMS_ONLINE_STATUS_JOIN = 1,
    DMS_ONLINE_STATUS_REFORM = 2,
    DMS_ONLINE_STATUS_IN = 3,
} dms_online_status_t;

typedef enum en_dms_dr_type {
    DMS_DR_TYPE_INVALID = 0,
    DMS_DR_TYPE_DATABASE = 1,
    DMS_DR_TYPE_SPACE = 2,
    DMS_DR_TYPE_TABLE = 3,
    DMS_DR_TYPE_DDL = 4,
    DMS_DR_TYPE_SEQENCE = 5,
    DMS_DR_TYPE_SERIAL = 6,
    DMS_DR_TYPE_ROLE = 7,
    DMS_DR_TYPE_USER = 8,
    DMS_DR_TYPE_DC = 9,
    DMS_DR_TYPE_INDEX = 10,
    DMS_DR_TYPE_TRIGGER = 11,
    DMS_DR_TYPE_HEAP = 12,
    DMS_DR_TYPE_HEAP_PART = 13,
    DMS_DR_TYPE_HEAP_LATCH = 14,
    DMS_DR_TYPE_HEAP_PART_LATCH = 15,
    DMS_DR_TYPE_BTREE_LATCH = 16,
    DMS_DR_TYPE_BRTEE_PART_LATCH = 17,
    DMS_DR_TYPE_INTERVAL_PART_LATCH = 18,
    DMS_DR_TYPE_LOB_LATCH = 19,
    DMS_DR_TYPE_LOB_PART_LATCH = 20,
    DMS_DR_TYPE_PROFILE = 21,
    DMS_DR_TYPE_UNDO = 22,
    DMS_DR_TYPE_PROC = 23,
    DMS_DR_TYPE_GDV = 24,
    DMS_DR_TYPE_SEQVAL = 25,
    DMS_DR_TYPE_SHARED_INNODE = 26,
    DMS_DR_TYPE_PROC_ENTRY = 27,
    DMS_DR_TYPE_PART_TABLE,
    DMS_DR_TYPE_MAX,
} dms_dr_type_t;

#define DMS_DR_IS_TABLE_TYPE(type) ((type) == DMS_DR_TYPE_TABLE || (type) == DMS_DR_TYPE_PART_TABLE)

// persistent distributed resource id
typedef enum en_dms_persistent_id {
    DMS_ID_DATABASE_CTRL = 0,
    DMS_ID_DATABASE_SWITCH_CTRL = 1,
    DMS_ID_DATABASE_BAKUP = 2,
    DMS_ID_DATABASE_LINK = 3,
    DMS_ID_SPACE_OP = 10,
    DMS_ID_SPACE_BLOCK = 11,
    DMS_ID_DDL_OP = 20,
    DMS_ID_DC_CTX = 30,
    DMS_ID_INDEX_RECYLE = 40,
    DMS_ID_UNDO_SET = 50,
}dms_pst_id_t;

// for smon deadlock check
#define DMS_SMON_DLOCK_MSG_MAX_LEN  24
#define DMS_SMON_TLOCK_MSG_MAX_LEN  24
#define DMS_SMON_ILOCK_MSG_MAX_LEN  60
#define DMS_SMON_MAX_SQL_LEN    10240  // The maximum size of a message to be transferred in the MES is 32 KB.
#define MAX_TABLE_LOCK_NUM 2048

typedef enum en_dms_smon_req_type {
    DMS_SMON_REQ_SID_BY_RMID = 0,
    DMS_SMON_REQ_DLOCK_BY_RMID = 1,
    DMS_SMON_REQ_ROWID_BY_RMID = 2,
}dms_smon_req_type_t;

typedef enum en_dms_smon_req_tlock_type {
    DMS_SMON_REQ_TABLE_LOCK_SHARED_MSG = 0,
    DMS_SMON_REQ_TABLE_LOCK_EXCLU_MSG = 1,
    DMS_SMON_REQ_TABLE_LOCK_ALL_MSG = 2,
}dms_smon_req_tlock_type_t;

typedef enum en_dms_smon_req_rm_type {
    DMS_SMON_REQ_TABLE_LOCK_RM = 0,
    DMS_SMON_REQ_TABLE_LOCK_WAIT_RM = 1,
}dms_smon_req_rm_type_t;

typedef enum en_dms_smon_check_tlock_type {
    DMS_SMON_CHECK_WAIT_EVENT_STATUS_BY_SID = 0,
    DMS_SMON_CHECK_WAIT_TABLE_STATUS_BY_TID = 1,
}dms_smon_check_tlock_type_t;

/* distributed resource id definition */
#pragma pack(4)
typedef struct st_dms_drid {
    union {
        struct {
            unsigned long long key1;
            unsigned long long key2;
            unsigned int key3;
        };
        struct {
            unsigned short  type;  // lock type
            unsigned short  uid;   // user id, for table lock resource
            union {
                struct {
                    unsigned int    oid;   // lock id
                    unsigned int    index; // index id
                    unsigned int    parent_part;  // parent partition id
                    unsigned int    part;  // partition id
                };
                struct {
                    unsigned long long oid_64;
                    unsigned long long unused;
                };
            };
        };
    };
} dms_drid_t;
#pragma pack()

typedef enum en_drc_res_type {
    DRC_RES_INVALID_TYPE,
    DRC_RES_PAGE_TYPE,
    DRC_RES_LOCK_TYPE,
    DRC_RES_LOCAL_LOCK_TYPE,
    DRC_RES_TXN_TYPE,
    DRC_RES_LOCAL_TXN_TYPE,
    DRC_RES_LOCK_ITEM_TYPE,
    DRC_RES_GLOBAL_XA_TYPE,
} drc_res_type_e;

typedef enum en_dms_session {
    DMS_SESSION_NORMAL = 0,     // can not access DRC when DRC is inaccessible
    DMS_SESSION_REFORM = 1,     // can access DRC when DRC is inaccessible
    DMS_SESSION_RECOVER = 2,    // can access DRC when DRC is inaccessible, buf if no owner, should set in recovery
    DMS_SESSION_RECOVER_HOT_STANDBY = 3, // can access DRC when pmstate = PM_HOT_STANDBY
} dms_session_e;

/*
 * before construct CR page, we need to obtain the basic page,
 * and rollback those unvisible transaction on the basic page's content,
 * it represents where the basic page comes from.
 */
typedef enum st_dms_cr_version_t {
    DMS_CR_VERSION_NONE = 0,
    DMS_CR_VERSION_CURR_PAGE,
    DMS_CR_VERSION_EDP_PAGE,
    DMS_CR_VERSION_CR_PAGE,
} dms_cr_version_t;

/*
 * CR construct state machine:
 * first try to read page from local node's data buffer (DMS_CR_PHASE_TRY_READ_PAGE),
 * if the current page or edp page is usable, we use it to construct CR;
 * otherwise, check status from the master node (DMS_CR_PHASE_CHECK_MASTER),
 * if master is remote node, the request CR to the master (DMS_CR_PHASE_REQ_MASTER),
 * otherwise obtain the owner's position, the either request CR to the remote owner (DMS_CR_PHASE_REQ_OWNER),
 * or trigger local node to read current page and do the CR construct (DMS_CR_PHASE_READ_PAGE);
 * the master receives CR request, it will route the request to the owner,
 * and the owner will do CR construct operations,
 * meanwhile it will decide whether the other node needs to continue CR construct (DMS_CR_PHASE_CONSTRUCT).
 * if all CR construct operations have done, the phase will be DMS_CR_PHASE_DONE.
 */
typedef enum st_dms_cr_phase_t {
    DMS_CR_PHASE_TRY_READ_PAGE = 0,
    DMS_CR_PHASE_CHECK_MASTER,
    DMS_CR_PHASE_REQ_MASTER,
    DMS_CR_PHASE_REQ_OWNER,
    DMS_CR_PHASE_READ_PAGE,
    DMS_CR_PHASE_CONSTRUCT,
    DMS_CR_PHASE_DONE,
} dms_cr_phase_t;

typedef enum st_dms_cr_status_t {
    DMS_CR_STATUS_ABORT = 0,
    DMS_CR_STATUS_INVISIBLE_TXN,            /* local node invisible transaction */
    DMS_CR_STATUS_OTHER_NODE_INVISIBLE_TXN, /* other node invisible transaction */
    DMS_CR_STATUS_PENDING_TXN,              /* prepared transaction */
    DMS_CR_STATUS_ALL_VISIBLE,
    DMS_CR_STATUS_DB_NOT_READY,             /* db is not ready for tx */
} dms_cr_status_t;

typedef struct st_dms_cr_assist_t {
    void *handle;                           /* IN parameter */
    unsigned long long query_scn;           /* IN parameter */
    unsigned int ssn;                       /* IN parameter */
    unsigned int relay_inst;                /* OUT parameter */
    char *page;                             /* IN & OUT parameter */
    char *fb_mark;                          /* IN & OUT parameter */
    char page_id[DMS_PAGEID_SIZE];          /* IN parameter */
    char curr_xid[DMS_XID_SIZE];            /* IN parameter */
    char wxid[DMS_XID_SIZE];                /* OUT parameter */
    char entry[DMS_PAGEID_SIZE];            /* IN parameter */
    char profile[DMS_INDEX_PROFILE_SIZE];   /* IN parameter */
    char rowid[DMS_ROWID_SIZE];             /* IN parameter */
    unsigned int check_restart;             /* IN parameter */
    unsigned int *check_found;              /* IN & OUT parameter */
    dms_cr_phase_t phase;                   /* OUT parameter */
    dms_cr_status_t status;                 /* OUT parameter */
} dms_cr_assist_t;

#define DMS_RESID_SIZE  32
#define DMS_DRID_SIZE   sizeof(dms_drid_t)

typedef struct st_dms_drlock {
    dms_drid_t      drid;
} dms_drlock_t;

typedef struct st_dms_drlatch {
    dms_drid_t   drid;
} dms_drlatch_t;

typedef struct st_dms_xid_ctx {
    unsigned long long xid;
    unsigned char is_scan;
    unsigned char inst_id;
    unsigned char unused[2];
    unsigned long long scn;
} dms_xid_ctx_t;

typedef struct dms_opengauss_relfilenode {
    unsigned int spcNode;
    unsigned int dbNode;
    unsigned int relNode;
    signed short bucketNode;
    unsigned short opt;
} dms_opengauss_relfilenode_t;

typedef struct st_dms_rfn {
    dms_opengauss_relfilenode_t rnode;
    unsigned char inst_id;
} dms_rfn_t;

typedef struct st_dms_xmap_ctx {
    unsigned int xmap;
    unsigned int dest_id;
} dms_xmap_ctx_t;

typedef struct st_dms_process_context {
    void *db_handle;
    unsigned int sess_id; // current session id
    unsigned int inst_id;  // current instance id
} dms_process_context_t;

typedef struct st_drc_global_xid {
    unsigned long long fmt_id;
    char   gtrid[DMS_MAX_XA_BASE16_GTRID_LEN];
    char   bqual[DMS_MAX_XA_BASE16_BQUAL_LEN];
    unsigned char gtrid_len;
    unsigned char bqual_len;
} drc_global_xid_t;

typedef struct st_dms_context {
    union {
        struct {
            void *db_handle;
            unsigned int sess_id; // current session id
            unsigned int inst_id;  // current instance id
        };
        dms_process_context_t proc_ctx;
    };
    dms_session_e sess_type;  // request page: recovery session flag
    unsigned char is_try;
    unsigned char type;
    unsigned short len;
    unsigned long long ctx_ruid; /* this ruid indicates one message ack is pending recv */
    union {
        char resid[DMS_RESID_SIZE];
        dms_drid_t lock_id;
        dms_xid_ctx_t xid_ctx;
        dms_xmap_ctx_t xmap_ctx;
        dms_rfn_t rfn;
        unsigned char edp_inst;
        drc_global_xid_t global_xid;
    };
    unsigned char intercept_type;
    unsigned char curr_mode;    // used for table lock
} dms_context_t;

typedef struct st_dms_cr {
    void *cr_cursor;
    unsigned long long query_scn;
    unsigned int ssn;
    char *page;
    unsigned char *fb_mark;
    dms_cr_status_t status;
    dms_cr_phase_t phase;
} dms_cr_t;

typedef struct st_dms_opengauss_xid_csn {
    unsigned long long xid;
    unsigned long long snapshotcsn;
    unsigned long long snapshotxmin;
    unsigned char is_committed;
    unsigned char is_mvcc;
    unsigned char is_nest;
} dms_opengauss_xid_csn_t;

typedef struct st_dms_opengauss_csn_result {
    unsigned long long csn;
    unsigned char sync;
    unsigned int clogstatus;
    unsigned long long lsn;
} dms_opengauss_csn_result_t;

typedef struct dms_opengauss_txn_snapshot {
    unsigned long long xmin;
    unsigned long long xmax;
    unsigned long long snapshotcsn;
    unsigned long long localxmin;
} dms_opengauss_txn_snapshot_t;

typedef struct dms_opengauss_txn_sw_info {
    unsigned long long sxid;        // transaction id of master, used for standby write feature
    unsigned int scid;              // command id of master, used for standby write feature
    unsigned int server_proc_slot;  // backend slot of master, used for standby write feature
} dms_opengauss_txn_sw_info_t;

typedef struct st_dms_opengauss_page_status_result {
    int bit_count;
    unsigned long int page_map[8];
} dms_opengauss_page_status_result_t;

typedef enum dms_opengauss_lock_req_type {
    SHARED_INVAL_MSG,
    DROP_BUF_MSG,
    LOCK_NORMAL_MODE,
    LOCK_RELEASE_SELF,
    LOCK_REACQUIRE,
} dms_opengauss_lock_req_type_t;

typedef struct st_dms_txn_info {
    unsigned long long scn;
    unsigned char is_owscn;
    unsigned char status;
    unsigned char unused[2];
} dms_txn_info_t;

typedef struct st_dms_txn_snapshot {
    unsigned long long scn;
    unsigned int xnum;
    unsigned short rmid;
    unsigned char status;
    unsigned char in_process;
} dms_txn_snapshot_t;

typedef struct st_dms_edp_info {
    char page[DMS_PAGEID_SIZE];
    unsigned long long lsn;
    union {
        unsigned char id;
        unsigned long long edp_map;
    };
} dms_edp_info_t;

typedef struct st_dms_broadcast_context {
    char *data;
    unsigned int len;
    char *output_msg;
    unsigned int *output_msg_len;
} dms_broadcast_context_t;

typedef struct st_dms_buf_ctrl {
    volatile unsigned char is_remote_dirty;
    volatile unsigned char lock_mode;       // used only in DMS, 0: Null, 1: Shared lock, 2: Exclusive lock
    // used only in DMS, 0: no, 1: yes, this page is old version,
    // can be discard only after latest version in other instance is cleaned
    volatile unsigned char is_edp;
    volatile unsigned char force_request;   // force to request page from remote
    volatile unsigned char need_flush;      // for recovery, owner is abort, copy instance should flush before release
    volatile unsigned char been_loaded;     // first alloc ctrl:FALSE, after successfully loaded: TRUE
    volatile unsigned char in_rcy;          // if drc lost, we can rebuild in_recovery flag according buf_ctrl
    volatile unsigned char unused;
    unsigned long long edp_scn;          // set when become edp, lastest scn when page becomes edp
    unsigned long long edp_map;             // records edp instance
    long long last_ckpt_time; // last time when local edp page is added to group.
#ifdef OPENGAUSS
    int buf_id;
    unsigned int state;
    unsigned int pblk_relno;
    unsigned int pblk_blkno;
    unsigned long long  pblk_lsn;
    unsigned char seg_fileno;
    unsigned int seg_blockno;
    void* ctrl_lock;
#endif
} dms_buf_ctrl_t;

typedef struct st_dms_ctrl_info {
    dms_buf_ctrl_t      ctrl;
    unsigned long long  lsn;
    unsigned char       is_dirty;
} dms_ctrl_info_t;

typedef enum en_dms_page_latch_mode {
    DMS_PAGE_LATCH_MODE_S = 1,
    DMS_PAGE_LATCH_MODE_X = 2,
    DMS_PAGE_LATCH_MODE_FORCE_S = 3,
} dms_page_latch_mode_t;

#define DMS_ENTER_PAGE_NORMAL         (unsigned char)0    // normal access for single page
#define DMS_ENTER_PAGE_RESIDENT       (unsigned char)1    // resident in memory, not in LRU
#define DMS_ENTER_PAGE_PINNED         (unsigned char)2    // temp pinned for undo rollback
#define DMS_ENTER_PAGE_NO_READ        (unsigned char)4    // don't read from disk,caller will initialize
#define DMS_ENTER_PAGE_TRY            (unsigned char)8    // try to read from buffer, don't read from disk
#define DMS_ENTER_PAGE_LRU_STATS_SCAN (unsigned char)0x10 // add to stats LRU list
#define DMS_ENTER_PAGE_LRU_HIGH_AGE   (unsigned char)0x20 // decrease possibility to be recycled of page
#define DMS_ENTER_PAGE_LOCAL          (unsigned char)0x40 // check local page without redo log, use carefully

// pack read page parameters together
typedef struct st_dms_read_page_assist {
    char                  *pageid;
    unsigned long long     query_scn;  // if not invalid, try edp, check edp scn with query_scn
    dms_page_latch_mode_t  mode;
    unsigned char          options;    // DMS_ENTER_PAGE_XXX
    unsigned char          try_edp;    // check edp if local page not usable
    unsigned short         read_num;   // == 1 no prefetch, > 1 prefetch multiple pages
} dms_read_page_assist_t;

typedef enum en_dms_buf_load_status {
    DMS_BUF_NEED_LOAD = 0x00,
    DMS_BUF_IS_LOADED = 0x01,
    DMS_BUF_LOAD_FAILED = 0x02,
    DMS_BUF_NEED_TRANSFER = 0x04,          // used only in DTC, means need ask master/coordinator for latest version
} dms_buf_load_status_t;

typedef enum en_dms_log_level {
    DMS_LOG_LEVEL_ERROR = 0,  // error conditions
    DMS_LOG_LEVEL_WARN,       // warning conditions
    DMS_LOG_LEVEL_INFO,       // informational messages
    DMS_LOG_LEVEL_COUNT,
} dms_log_level_t;

typedef enum en_dms_log_id {
    DMS_LOG_ID_RUN = 0,
    DMS_LOG_ID_DEBUG,
    DMS_LOG_ID_COUNT,
} dms_log_id_t;

/*
* lock mode in DMS, we use it to coordinate concurrent access among different instances.
* !!!!Attention Do not modify its order
*/
typedef enum en_dms_lock_mode {
    DMS_LOCK_NULL = 0,
    DMS_LOCK_SHARE = 1,
    DMS_LOCK_EXCLUSIVE = 2,
    DMS_LOCK_MODE_MAX = 3,
} dms_lock_mode_t;

typedef enum en_dms_conn_mode {
    DMS_CONN_MODE_TCP = 0,
    DMS_CONN_MODE_RDMA = 1,
} dms_conn_mode_t;

typedef enum en_dms_txn_wait_status {
    DMS_REMOTE_TXN_WAIT = 0,
    DMS_REMOTE_TXN_END = 1
} dms_txn_wait_status_t;

typedef enum en_dms_xact_status {
    DMS_XACT_END = 0,
    DMS_XACT_BEGIN = 1,
    DMS_XACT_PHASE1 = 2,
    DMS_XACT_PHASE2 = 3
} dms_xact_status_t;

typedef enum en_dms_cm_stat {
    DMS_CM_RES_UNKNOWN = 0,
    DMS_CM_RES_ONLINE = 1,
    DMS_CM_RES_OFFLINE = 2,
    /********************/
    DMS_CM_RES_STATE_COUNT = 3,
} dms_cm_stat_t;

typedef struct st_dw_recovery_info {
    unsigned long long bitmap_old_join;     // the old-join-inst bitmap in dw_recovery phase
    unsigned long long bitmap_old_remove;   // the old-remove-inst bitmap in dw_recovery phase
    unsigned long long bitmap_new_join;     // the new-join-inst bitmap in dw_recovery phase
} dw_recovery_info_t;

typedef struct st_inst_list {
    unsigned char inst_id_list[DMS_MAX_INSTANCES];
    unsigned char inst_id_count;
    unsigned char reserve[3];
} instance_list_t;

typedef enum en_dms_wait_event {
    DMS_EVT_IDLE_WAIT = 0,

    DMS_EVT_GC_BUFFER_BUSY,
    DMS_EVT_DCS_REQ_MASTER4PAGE_1WAY,
    DMS_EVT_DCS_REQ_MASTER4PAGE_2WAY,
    DMS_EVT_DCS_REQ_MASTER4PAGE_3WAY,
    DMS_EVT_DCS_REQ_MASTER4PAGE_TRY,
    DMS_EVT_DCS_REQ_OWNER4PAGE,
    DMS_EVT_DCS_CLAIM_OWNER,
    DMS_EVT_DCS_RELEASE_OWNER,
    DMS_EVT_DCS_INVLDT_SHARE_COPY_REQ,
    DMS_EVT_DCS_INVLDT_SHARE_COPY_PROCESS,
    DMS_EVT_DCS_TRANSFER_PAGE_LATCH,
    DMS_EVT_DCS_TRANSFER_PAGE_READONLY2X,
    DMS_EVT_DCS_TRANSFER_PAGE_FLUSHLOG,
    DMS_EVT_DCS_TRANSFER_PAGE,
    DMS_EVT_PCR_REQ_BTREE_PAGE,
    DMS_EVT_PCR_REQ_HEAP_PAGE,
    DMS_EVT_PCR_REQ_MASTER,
    DMS_EVT_PCR_REQ_OWNER,
    DMS_EVT_PCR_CHECK_CURR_VISIBLE,
    DMS_EVT_TXN_REQ_INFO,
    DMS_EVT_TXN_REQ_SNAPSHOT,
    DMS_EVT_DLS_REQ_LOCK,
    DMS_EVT_DLS_REQ_TABLE,
    DMS_EVT_DLS_REQ_PART_X,
    DMS_EVT_DLS_REQ_PART_S,
    DMS_EVT_DLS_WAIT_TXN,
    DMS_EVT_DEAD_LOCK_TXN,
    DMS_EVT_DEAD_LOCK_TABLE,
    DMS_EVT_DEAD_LOCK_ITL,
    DMS_EVT_BROADCAST_BTREE_SPLIT,
    DMS_EVT_BROADCAST_ROOT_PAGE,
    DMS_EVT_QUERY_OWNER_ID,
    DMS_EVT_LATCH_X,
    DMS_EVT_LATCH_S,
    DMS_EVT_LATCH_X_REMOTE,
    DMS_EVT_LATCH_S_REMOTE,
    DMS_EVT_ONDEMAND_REDO,
    DMS_EVT_PAGE_STATUS_INFO,
    DMS_EVT_OPENGAUSS_SEND_XMIN,
    DMS_EVT_DCS_REQ_CREATE_XA_RES,
    DMS_EVT_DCS_REQ_DELETE_XA_RES,
    DMS_EVT_DCS_REQ_XA_OWNER_ID,
    DMS_EVT_DCS_REQ_XA_IN_USE,
    DMS_EVT_DCS_REQ_END_XA,
    DMS_EVT_REQ_CKPT,

// add new enum at tail, or make adaptations to openGauss
    DMS_EVT_COUNT,
} dms_wait_event_t;

typedef enum en_dms_sysstat {
    DMS_STAT_BUFFER_GETS = 0,
    DMS_STAT_BUFFER_SENDS,
    DMS_STAT_CR_READS,
    DMS_STAT_CR_GETS,
    DMS_STAT_CR_SENDS,
    DMS_STAT_NET_TIME,

    DMS_STAT_COUNT,
} dms_sysstat_t;

typedef enum en_dms_role {
    DMS_ROLE_UNKNOW = 0,
    DMS_ROLE_REFORMER = 1,
    DMS_ROLE_PARTNER = 2
} dms_role_t;

typedef enum en_reform_phase {
    DMS_PHASE_START = 0,
    DMS_PHASE_AFTER_DRC_ACCESS = 1,
    DMS_PHASE_AFTER_RECOVERY = 2,
    DMS_PHASE_AFTER_TXN_DEPOSIT = 3,
    DMS_PHASE_BEFORE_ROLLBACK = 4,
    DMS_PHASE_END = 5,
} reform_phase_t;

typedef enum en_dms_reform_type {
    // for multi_write
    DMS_REFORM_TYPE_FOR_NORMAL = 0,

    // for Gauss100
    DMS_REFORM_TYPE_FOR_BUILD,
    DMS_REFORM_TYPE_FOR_FAILOVER,
    DMS_REFORM_TYPE_FOR_SWITCHOVER,

    // for openGauss
    DMS_REFORM_TYPE_FOR_NORMAL_OPENGAUSS,
    DMS_REFORM_TYPE_FOR_FAILOVER_OPENGAUSS,
    DMS_REFORM_TYPE_FOR_SWITCHOVER_OPENGAUSS,

    // common
    DMS_REFORM_TYPE_FOR_FULL_CLEAN, // for all instances are online and stable, and all instances status is IN
    DMS_REFORM_TYPE_FOR_MAINTAIN,   // for start database without CM, every instance is supported
    // New type need to be added start from here
    DMS_REFORM_TYPE_FOR_RST_RECOVER,
    DMS_REFORM_TYPE_COUNT
} dms_reform_type_t;

typedef enum en_dms_status {
    DMS_STATUS_OUT = 0,
    DMS_STATUS_JOIN = 1,
    DMS_STATUS_REFORM = 2,
    DMS_STATUS_IN = 3
} dms_status_t;             // used in database startup

typedef enum st_dms_session_type {
    DMS_SESSION_TYPE_NONE = 0,
    DMS_SESSION_TYPE_WORKER = 1,
    DMS_SESSION_TYPE_FULL_RCY = 2,
    DMS_SESSION_TYPE_FULL_RCY_PARAL = 3,
}dms_session_type_e;

#define DMS_RES_INTERCEPT_TYPE_NONE 0
#define DMS_RES_INTERCEPT_TYPE_BIZ_SESSION 1

#define DCS_BATCH_BUF_SIZE (1024 * 30)
#define DCS_RLS_OWNER_BATCH_SIZE (DCS_BATCH_BUF_SIZE / DMS_PAGEID_SIZE)
typedef struct st_dcs_batch_buf {
    char buffers[DMS_MAX_INSTANCES][DCS_BATCH_BUF_SIZE];
    unsigned int count[DMS_MAX_INSTANCES];
    unsigned int max_count;
} dcs_batch_buf_t;

typedef enum en_dms_inst_behavior {
    DMS_INST_BEHAVIOR_IN_IDLE = 0,
    DMS_INST_BEHAVIOR_IN_REFORM,
    DMS_INST_BEHAVIOR_IN_BACKUP,
} dms_inst_behavior_t;

typedef struct st_stat_buf_info {
    int                 instance_id;
    unsigned char       lock_mode;              /* which lock held by instance of this buffer */
    unsigned long int   mem_lsn;                /* page lsn in memory */
    unsigned long int   rec_lsn;                /* recovery LSN */
    unsigned long int   lsn_on_disk;            /* page lsn in dick */
    unsigned long int   dirty_queue_loc;        /* actual location of dirty page queue */
    char                aio_in_progress;        /* indicate aio is in progress */
    char                data[DMS_RESID_SIZE];   /* user defined resource(page) identifier */
} stat_buf_info_t;

typedef enum en_broadcast_scope {
    DMS_BROADCAST_OLDIN_LIST = 0,    // default value
    DMS_BROADCAST_ONLINE_LIST = 1,
    DMS_BROADCAST_TYPE_COUNT,
} dms_broadcast_scope_e;

/*
* used by openGauss server to get DRC information
*/
typedef struct st_dv_drc_buf_info {
    stat_buf_info_t         buf_info[DMS_MAX_INSTANCES];           /* save buffer related information */
    dms_context_t           dms_ctx;
    char                    data[DMS_MAX_NAME_LEN];            /* user defined resource(page) identifier */
    unsigned char           master_id;
    unsigned long long      copy_insts;         /* bitmap for owners, for S mode, more than one owner may exist */
    unsigned char           claimed_owner;      /* owner */
    unsigned char           lock_mode;          /* current DRC lock mode */
    unsigned char           last_edp;           /* the newest edp instance id */
    unsigned char           type;               /* page or lock */
    unsigned char           in_recovery;        /* in recovery or not */
    unsigned char           copy_promote;       /* copy promote to owner, can not release, may need flush */
    unsigned short          part_id;            /* which partition id that current page belongs to */
    unsigned long long      edp_map;            /* indicate which instance has current page's EDP(Earlier Dirty Page) */
    unsigned long long      lsn;                /* the newest edp LSN of current page in the cluster */
    unsigned short          len;                /* the length of data below */
    unsigned char           recovery_skip;      /* DRC is accessed in recovery and skip because drc has owner */
    unsigned char           recycling;
    unsigned char           converting_req_info_inst_id;
    unsigned char           converting_req_info_curr_mode;
    unsigned char           converting_req_info_req_mode;
    unsigned char           is_valid;
} dv_drc_buf_info;

typedef struct st_dms_reform_start_context {
    dms_role_t role;
    dms_reform_type_t reform_type;
    unsigned long long bitmap_participated;
    unsigned long long bitmap_reconnect;
} dms_reform_start_context_t;

typedef int(*dms_get_list_stable)(void *db_handle, unsigned long long *list_stable, unsigned char *reformer_id);
typedef int(*dms_save_list_stable)(void *db_handle, unsigned long long list_stable, unsigned char reformer_id,
    unsigned long long list_in, unsigned int save_ctrl);
typedef int(*dms_get_dms_status)(void *db_handle);
typedef void(*dms_set_dms_status)(void *db_handle, int status);
typedef int(*dms_confirm_converting)(void *db_handle, char *pageid, unsigned char smon_chk,
    unsigned char *lock_mode, unsigned long long *edp_map, unsigned long long *lsn);
typedef int(*dms_confirm_owner)(void *db_handle, char *pageid, unsigned char *lock_mode, unsigned char *is_edp,
    unsigned long long *lsn);
typedef int(*dms_flush_copy)(void *db_handle, char *pageid);
typedef int(*dms_flush_copy_check_lsn)(void *db_handle, char *pageid, unsigned long long lsn);
typedef int(*dms_need_flush)(void *db_handle, char *pageid, unsigned char *is_edp);
typedef int(*dms_edp_to_owner)(void *db_handle, char *pageid, unsigned char *is_edp);
typedef int(*dms_edp_lsn)(void *db_handle, char *pageid, unsigned long long *lsn);
typedef int(*dms_disk_lsn)(void *db_handle, char *pageid, unsigned long long *lsn);
typedef int(*dms_recovery)(void *db_handle, void *recovery_list, int reform_type, int is_reformer);
typedef int(*dms_recovery_analyse)(void *db_handle, void *recovery_list, int is_reformer);
typedef int(*dms_dw_recovery)(void *db_handle, void *recovery_list, unsigned long long list_in, int is_reformer);
typedef int(*dms_df_recovery)(void *db_handle, unsigned long long list_in, void *recovery_list);
typedef int(*dms_space_reload)(void *db_handle, unsigned long long list_in);
typedef int(*dms_opengauss_startup)(void *db_handle);
typedef int(*dms_opengauss_recovery_standby)(void *db_handle, int inst_id);
typedef int(*dms_opengauss_recovery_primary)(void *db_handle, int inst_id);
typedef void(*dms_reform_start_notify)(void *db_handle, dms_reform_start_context_t *rs_ctx);
typedef int(*dms_undo_init)(void *db_handle, unsigned char inst_id);
typedef int(*dms_tx_area_init)(void *db_handle, unsigned char inst_id);
typedef int(*dms_tx_area_load)(void *db_handle, unsigned char inst_id);
typedef int(*dms_tx_rollback_start)(void *db_handle, unsigned char inst_id);
typedef int(*dms_convert_to_readwrite)(void *db_handle);
typedef int(*dms_tx_rollback_finish)(void *db_handle, unsigned char inst_id);
typedef unsigned char(*dms_recovery_in_progress)(void *db_handle);
typedef unsigned int(*dms_get_page_hash_val)(const char pageid[DMS_PAGEID_SIZE]);
typedef unsigned int(*dms_inc_and_get_srsn)(unsigned int sess_id);
typedef unsigned long long(*dms_get_page_lsn)(const dms_buf_ctrl_t *buf_ctrl);
typedef int(*dms_set_buf_load_status)(dms_buf_ctrl_t *buf_ctrl, dms_buf_load_status_t dms_buf_load_status);
typedef int(*dms_remove_buf_load_status)(dms_buf_ctrl_t *buf_ctrl, dms_buf_load_status_t dms_buf_load_status);
typedef void(*dms_update_global_lsn)(void *db_handle, unsigned long long lamport_lsn);
typedef void(*dms_update_global_scn)(void *db_handle, unsigned long long lamport_scn);
typedef void(*dms_update_page_lfn)(dms_buf_ctrl_t *buf_ctrl, unsigned long long lastest_lfn);
typedef unsigned long long (*dms_get_page_lfn)(dms_buf_ctrl_t *buf_ctrl);
typedef unsigned long long(*dms_get_global_lfn)(void *db_handle);
typedef unsigned long long(*dms_get_global_scn)(void *db_handle);
typedef unsigned long long(*dms_get_global_lsn)(void *db_handle);
typedef unsigned long long(*dms_get_global_flushed_lfn)(void *db_handle);
typedef int(*dms_read_local_page4transfer)(void *db_handle, char pageid[DMS_PAGEID_SIZE],
    dms_lock_mode_t mode, dms_buf_ctrl_t **buf_ctrl);
typedef int(*dms_try_read_local_page)(void *db_handle, char pageid[DMS_PAGEID_SIZE],
    dms_lock_mode_t mode, dms_buf_ctrl_t **buf_ctrl);
typedef unsigned char(*dms_page_is_dirty)(dms_buf_ctrl_t *buf_ctrl);
typedef void(*dms_leave_local_page)(void *db_handle, dms_buf_ctrl_t *buf_ctrl);
typedef void(*dms_get_pageid)(dms_buf_ctrl_t *buf_ctrl, char **pageid, unsigned int *size);
typedef char *(*dms_get_page)(dms_buf_ctrl_t *buf_ctrl);
typedef int (*dms_invalidate_page)(void *db_handle, char pageid[DMS_PAGEID_SIZE], unsigned char invld_owner);
typedef void *(*dms_get_db_handle)(unsigned int *db_handle_index, dms_session_type_e session_type);
typedef void (*dms_release_db_handle)(void *db_handle);
typedef char *(*dms_get_wxid_from_cr_cursor)(void *cr_cursor);
typedef void(*dms_set_page_force_request)(void *db_handle, char pageid[DMS_PAGEID_SIZE]);
typedef void(*dms_get_entry_pageid_from_cr_cursor)(void *cr_cursor, char index_entry_pageid[DMS_PAGEID_SIZE]);
typedef void(*dms_get_index_profile_from_cr_cursor)(void *cr_cursor, char index_profile[DMS_INDEX_PROFILE_SIZE]);
typedef void(*dms_get_xid_from_cr_cursor)(void *cr_cursor, char xid[DMS_XID_SIZE]);
typedef void(*dms_get_rowid_from_cr_cursor)(void *cr_cursor, char rowid[DMS_ROWID_SIZE]);
typedef int (*dms_heap_construct_cr_page)(dms_cr_assist_t *pcr);
typedef int (*dms_btree_construct_cr_page)(dms_cr_assist_t *pcr);
typedef int (*dms_check_heap_page_visible)(dms_cr_assist_t *pcr);
typedef int(*dms_read_page)(void *db_handle, dms_read_page_assist_t *assist, char **page_addr, unsigned int *status);
typedef void(*dms_leave_page)(void *db_handle, unsigned char changed, unsigned int status);
typedef char *(*dms_mem_alloc)(void *context, unsigned int size);
typedef void(*dms_mem_free)(void *context, void *ptr);
typedef void(*dms_mem_reset)(void *context);
// The maximum length of output_msg is 128 bytes.
typedef int (*dms_process_broadcast)(void *db_handle, dms_broadcast_context_t *broad_ctx);
typedef int (*dms_process_broadcast_ack)(void *db_handle, dms_broadcast_context_t *broad_ctx);
typedef int(*dms_get_txn_info)(void *db_handle, unsigned long long xid,
    unsigned char is_scan, dms_txn_info_t *txn_info);
typedef int(*dms_get_opengauss_xid_csn)(void *db_handle, dms_opengauss_xid_csn_t *csn_req,
    dms_opengauss_csn_result_t *csn_ack);
typedef int(*dms_get_opengauss_update_xid)(void *db_handle, unsigned long long xid,
    unsigned int t_infomask, unsigned int t_infomask2, unsigned long long *uxid);
typedef int(*dms_get_opengauss_txn_status)(void *db_handle, unsigned long long xid, unsigned char type,
    unsigned char* status);
typedef int(*dms_opengauss_lock_buffer)(void *db_handle, int buffer, unsigned char lock_mode,
    unsigned char* curr_mode);
typedef int(*dms_get_txn_snapshot)(void *db_handle, unsigned int xmap, dms_txn_snapshot_t *txn_snapshot);
typedef int(*dms_get_opengauss_txn_snapshot)(void *db_handle, dms_opengauss_txn_snapshot_t *txn_snapshot,
    unsigned char inst_id);
typedef int(*dms_get_opengauss_txn_of_master)(void *db_handle, dms_opengauss_txn_sw_info_t *txn_swinfo);
typedef int(*dms_get_opengauss_page_status)(void *db_handle, dms_opengauss_relfilenode_t *rnode, unsigned int page,
    int page_num, dms_opengauss_page_status_result_t *page_result);
typedef void (*dms_log_output)(dms_log_id_t log_type, dms_log_level_t log_level, const char *code_file_name,
    unsigned int code_line_num, const char *module_name, const char *format, ...);
typedef int (*dms_log_flush)(void *db_handle, unsigned long long *lsn);
typedef int (*dms_log_conditional_flush)(void *db_handle, unsigned long long lfn, unsigned long long *lsn);
typedef int(*dms_process_edp)(void *db_handle, dms_edp_info_t *pages, unsigned int count);
typedef void (*dms_clean_ctrl_edp)(void *db_handle, dms_buf_ctrl_t *dms_ctrl);
typedef char *(*dms_display_pageid)(char *display_buf, unsigned int count, char *pageid);
typedef char *(*dms_display_xid)(char *display_buf, unsigned int count, char *xid);
typedef char *(*dms_display_rowid)(char *display_buf, unsigned int count, char *rowid);
typedef int (*dms_check_session_invalid)(unsigned int sid);
typedef int (*dms_drc_buf_res_rebuild)(void *db_handle);
typedef int (*dms_drc_buf_res_rebuild_parallel)(void *db_handle, unsigned char thread_index, unsigned char thread_num);
typedef int (*dms_drc_tlock_rebuild_parallel)(void *db_handle, unsigned char thread_index, unsigned char thread_num);
typedef int (*dms_drc_validate_page)(void *db_handle, unsigned char thread_index, unsigned char thread_num);
typedef int (*dms_drc_validate_tlock)(void *db_handle, unsigned char thread_index, unsigned char thread_num);
typedef int(*dms_ctl_rcy_clean_parallel_t)(void *db_handle, unsigned char thread_index, unsigned char thread_num);
typedef unsigned char(*dms_ckpt_session)(void *db_handle);
typedef void (*dms_check_if_build_complete)(void *db_handle, unsigned int *build_complete);
typedef void (*dms_check_if_restore_recover)(void *db_handle, unsigned int *rst_recover);
typedef void (*dms_set_switchover_result)(void *db_handle, int result);
typedef int (*dms_mount_to_recovery)(void *db_handle, unsigned int *has_offline);
typedef int(*dms_get_open_status)(void *db_handle);
typedef void (*dms_reform_set_dms_role)(void *db_handle, unsigned int reformer_id);
typedef void (*dms_reset_user)(void *db_handle, unsigned long long list_in);
typedef int (*dms_drc_xa_res_rebuild)(void *db_handle, unsigned char thread_index, unsigned char parall_num);
typedef void (*dms_reform_shrink_xa_rms)(void *db_handle, unsigned char undo_seg_id);
typedef void (*dms_ckpt_unblock_rcy_local)(void *db_handle, unsigned long long list_in);

// for openGauss
typedef void (*dms_thread_init_t)(unsigned char need_startup, char **reg_data);
typedef void (*dms_thread_deinit_t)(void);
typedef int (*dms_get_db_primary_id)(void *db_handle, unsigned int *primary_id);
typedef int (*dms_opengauss_ondemand_redo_buffer)(void *block_key, int *redo_status);
typedef int (*dms_opengauss_do_ckpt_immediate)(unsigned long long *ckpt_loc);

// for ssl
typedef int(*dms_decrypt_pwd_t)(const char *cipher, unsigned int len, char *plain, unsigned int size);

// for smon check deadlock
typedef unsigned short (*dms_get_sid_by_rmid)(void *db_handle, unsigned short rmid);
typedef void (*dms_get_txn_dlock_by_rmid)(void *db_handle, unsigned short rmid, char *dlock, unsigned int dlock_len);
typedef void (*dms_get_rowid_by_rmid)(void *db_handle, unsigned short rmid, char rowid[DMS_ROWID_SIZE]);
typedef void (*dms_get_sql_from_session)(void *db_handle, unsigned short sid, char *sql_str, unsigned int sql_str_len);
typedef int (*dms_get_itl_lock_by_xid)(void *db_handle, char xid[DMS_XID_SIZE], char *ilock, unsigned int ilock_len);
typedef void (*dms_check_tlock_status)(void *db_handle, unsigned int type, unsigned short sid,
    unsigned long long tableid, unsigned int *in_use);
typedef void (*dms_get_tlock_msg_by_tid)(void *db_handle, unsigned long long table_id, unsigned int type, char *rsp,
    unsigned int rsp_len, unsigned int *tlock_cnt);
typedef void (*dms_get_tlock_msg_by_rm)(void *db_handle, unsigned short sid, unsigned short rmid, int type, char *tlock,
    unsigned int tlock_len);

typedef int (*dms_switchover_demote)(void *db_handle);
typedef int (*dms_switchover_promote)(void *db_handle);
typedef int (*dms_switchover_promote_opengauss)(void *db_handle, unsigned char origPrimaryId);
typedef int (*dms_failover_promote_opengauss)(void *db_handle);
typedef int (*dms_reform_done_notify)(void *db_handle);
typedef int (*dms_log_wait_flush)(void *db_handle, unsigned long long lsn);
typedef int (*dms_wait_ckpt)(void *db_handle);
typedef void (*dms_verify_page)(dms_buf_ctrl_t *buf_ctrl, char *new_page);
typedef int (*dms_drc_validate)(void *db_handle);
typedef int (*dms_db_check_lock)(void *db_handle);
typedef int (*dms_cache_msg)(void *db_handle, char* msg);
typedef void (*dms_ckpt_enque_one_page)(void *db_handle, dms_buf_ctrl_t *ctrl);
typedef int (*dms_set_remove_point)(void *db_handle, unsigned int node_id, void *curr_point);
typedef int (*dms_get_enable_checksum)(void *db_handle);
typedef unsigned int (*dms_calc_page_checksum)(void *db_handle, dms_buf_ctrl_t *ctrl, unsigned int page_size);
typedef int (*dms_verify_page_checksum)(void *db_handle, dms_buf_ctrl_t *ctrl, unsigned int page_size, int cks);
typedef int (*dms_update_node_oldest_xmin)(void *db_handle, unsigned char inst_id, unsigned long long oldest_xmin);
typedef void (*dms_set_inst_behavior)(void *db_handle, dms_inst_behavior_t inst_behavior);
typedef int (*dms_db_prepare)(void *db_handle);
typedef void (*dms_get_buf_info)(char* resid, stat_buf_info_t *buf_info);
typedef int (*dms_end_xa)(void *db_handle, void *knl_xa_xid, unsigned long long flags, unsigned long long scn,
    unsigned char is_commit);
typedef unsigned char (*dms_xa_inuse)(void *db_handle, void *knl_xa_xid);
typedef int (*dms_ddl_2phase_rcy)(void *db_handle, unsigned long long inst_remove_bitmap);
typedef int (*dms_reform_is_need_ddl_2phase_rcy)(void *db_handle);
typedef void (*dms_buf_ctrl_recycle)(void *db_handle);
typedef void *(*dms_malloc_prot_proc)(size_t size);
typedef void (*dms_free_prot_proc)(void *ptr);
typedef int (*dms_get_kernel_error_code)();
typedef int (*dms_lsn_validate)(void *db_handle, char *pageid, unsigned long long lsn, unsigned char in_recovery);
typedef int (*dms_invld_tlock_ownership)(void *db_handle, char *resid, unsigned char req_mode, unsigned char is_try);
typedef int (*dms_get_tlock_mode)(void *db_handle, char *resid);

typedef struct st_dms_callback {
    // used in reform
    dms_get_list_stable get_list_stable;
    dms_save_list_stable save_list_stable;
    dms_get_dms_status get_dms_status;
    dms_set_dms_status set_dms_status;
    dms_confirm_owner confirm_owner;
    dms_confirm_converting confirm_converting;
    dms_flush_copy flush_copy;
    dms_flush_copy_check_lsn flush_copy_check_lsn;
    dms_need_flush need_flush;
    dms_edp_to_owner edp_to_owner;
    dms_edp_lsn edp_lsn;
    dms_disk_lsn disk_lsn;
    dms_recovery recovery;
    dms_recovery_analyse recovery_analyse;
    dms_dw_recovery dw_recovery;
    dms_df_recovery df_recovery;
    dms_space_reload space_reload;
    dms_get_open_status get_open_status;
    dms_undo_init undo_init;
    dms_tx_area_init tx_area_init;
    dms_tx_area_load tx_area_load;
    dms_tx_rollback_start tx_rollback_start;
    dms_convert_to_readwrite convert_to_readwrite;
    dms_tx_rollback_finish tx_rollback_finish;
    dms_recovery_in_progress recovery_in_progress;
    dms_drc_buf_res_rebuild dms_reform_rebuild_buf_res;
    dms_drc_buf_res_rebuild_parallel dms_reform_rebuild_parallel;
    dms_drc_tlock_rebuild_parallel dms_reform_rebuild_tlock_parallel;
    dms_drc_validate_page validate_page;
    dms_drc_validate_tlock validate_table_lock;
    dms_ctl_rcy_clean_parallel_t dms_ctl_rcy_clean_parallel;
    dms_check_if_build_complete check_if_build_complete;
    dms_check_if_restore_recover check_if_restore_recover;
    dms_reset_user reset_user;
    dms_drc_xa_res_rebuild dms_reform_rebuild_xa_res;
    dms_reform_shrink_xa_rms dms_shrink_xa_rms;
    dms_ckpt_unblock_rcy_local ckpt_unblock_rcy_local;

    // used in reform for opengauss
    dms_thread_init_t dms_thread_init;
    dms_thread_deinit_t dms_thread_deinit;
    dms_get_db_primary_id get_db_primary_id;
    dms_opengauss_startup opengauss_startup;
    dms_opengauss_recovery_standby opengauss_recovery_standby;
    dms_opengauss_recovery_primary opengauss_recovery_primary;
    dms_opengauss_ondemand_redo_buffer opengauss_ondemand_redo_buffer;
    dms_reform_start_notify reform_start_notify;
    dms_reform_set_dms_role reform_set_dms_role;

    dms_inc_and_get_srsn inc_and_get_srsn;
    dms_get_page_hash_val get_page_hash_val;
    dms_get_page_lsn get_page_lsn;
    dms_set_buf_load_status set_buf_load_status;
    dms_remove_buf_load_status remove_buf_load_status;
    dms_update_global_scn update_global_scn;
    dms_update_global_lsn update_global_lsn;
    dms_update_page_lfn update_page_lfn;
    dms_get_global_scn get_global_scn;
    dms_get_global_lsn get_global_lsn;
    dms_get_global_lfn get_global_lfn;
    dms_get_page_lfn get_page_lfn;
    dms_get_global_flushed_lfn get_global_flushed_lfn;
    dms_read_local_page4transfer read_local_page4transfer;
    dms_page_is_dirty page_is_dirty;
    dms_leave_local_page leave_local_page;
    dms_get_pageid get_pageid;
    dms_get_page get_page;
    dms_invalidate_page invalidate_page;
    dms_get_db_handle get_db_handle;
    dms_release_db_handle release_db_handle;
    dms_get_wxid_from_cr_cursor get_wxid_from_cr_cursor;
    dms_set_page_force_request set_page_force_request;
    dms_get_entry_pageid_from_cr_cursor get_entry_pageid_from_cr_cursor;
    dms_get_index_profile_from_cr_cursor get_index_profile_from_cr_cursor;
    dms_get_xid_from_cr_cursor get_xid_from_cr_cursor;
    dms_get_rowid_from_cr_cursor get_rowid_from_cr_cursor;
    dms_heap_construct_cr_page heap_construct_cr_page;
    dms_btree_construct_cr_page btree_construct_cr_page;
    dms_check_heap_page_visible check_heap_page_visible;
    dms_read_page read_page;
    dms_leave_page leave_page;
    dms_verify_page verify_page;

    /* memory manager callback functions provided by DB */
    dms_mem_alloc mem_alloc;
    dms_mem_free mem_free;
    dms_mem_reset mem_reset;

    dms_process_broadcast process_broadcast;
    dms_process_broadcast_ack process_broadcast_ack;
    dms_get_txn_info get_txn_info;
    dms_get_opengauss_xid_csn get_opengauss_xid_csn;
    dms_get_opengauss_update_xid get_opengauss_update_xid;
    dms_get_opengauss_txn_status get_opengauss_txn_status;
    dms_opengauss_lock_buffer opengauss_lock_buffer;
    dms_opengauss_do_ckpt_immediate opengauss_do_ckpt_immediate;
    dms_get_txn_snapshot get_txn_snapshot;
    dms_get_opengauss_txn_snapshot get_opengauss_txn_snapshot;
    dms_get_opengauss_txn_of_master get_opengauss_txn_of_master;
    dms_get_opengauss_page_status get_opengauss_page_status;
    dms_log_output log_output;
    dms_log_flush log_flush;
    dms_log_conditional_flush log_conditional_flush;
    dms_process_edp ckpt_edp;
    dms_process_edp clean_edp;
    dms_ckpt_session ckpt_session;
    dms_clean_ctrl_edp clean_ctrl_edp;
    dms_display_pageid display_pageid;
    dms_display_xid display_xid;
    dms_display_rowid display_rowid;
    dms_check_session_invalid check_session_invalid;

    // for smon deadlock check
    dms_get_sid_by_rmid get_sid_by_rmid;
    dms_get_txn_dlock_by_rmid get_txn_dlock_by_rmid;
    dms_get_rowid_by_rmid get_rowid_by_rmid;
    dms_get_sql_from_session get_sql_from_session;
    dms_get_itl_lock_by_xid get_itl_lock_by_xid;
    dms_check_tlock_status check_tlock_status;
    dms_get_tlock_msg_by_tid get_tlock_by_tid;
    dms_get_tlock_msg_by_rm get_tlock_by_rm;

    // for switchover
    dms_switchover_demote switchover_demote;
    dms_switchover_promote switchover_promote;
    dms_switchover_promote_opengauss switchover_promote_opengauss;
    dms_failover_promote_opengauss failover_promote_opengauss;
    dms_set_switchover_result set_switchover_result;
    dms_mount_to_recovery mount_to_recovery;

    dms_reform_done_notify reform_done_notify;
    dms_log_wait_flush log_wait_flush;
    dms_wait_ckpt wait_ckpt;

    dms_drc_validate drc_validate;
    dms_db_check_lock db_check_lock;
    dms_cache_msg cache_msg;
    dms_ckpt_enque_one_page ckpt_enque_one_page;
    dms_set_remove_point set_remove_point;
    dms_get_enable_checksum get_enable_checksum;
    dms_calc_page_checksum calc_page_checksum;
    dms_verify_page_checksum verify_page_checksum;
    dms_update_node_oldest_xmin update_node_oldest_xmin;

    //for shared storage backup
    dms_set_inst_behavior set_inst_behavior;
    dms_db_prepare db_prepare;

    dms_get_buf_info get_buf_info;
    dms_end_xa end_xa;
    dms_xa_inuse xa_inuse;

    dms_ddl_2phase_rcy ddl_2phase_rcy;
    dms_reform_is_need_ddl_2phase_rcy reform_is_need_ddl_2phase_rcy;
    dms_buf_ctrl_recycle buf_ctrl_recycle;
    dms_malloc_prot_proc dms_malloc_prot;
    dms_free_prot_proc dms_free_prot;
    dms_get_kernel_error_code db_get_kernel_error_code;
    dms_lsn_validate lsn_validate;
    dms_invld_tlock_ownership invld_tlock_ownership;
    dms_get_tlock_mode get_tlock_mode;
} dms_callback_t;

typedef struct st_dms_instance_net_addr {
    unsigned int inst_id;
    char ip[DMS_MAX_IP_LEN];
    char secondary_ip[DMS_MAX_IP_LEN];
    unsigned short port;
    unsigned char need_connect;
    unsigned char reserved[1];
} dms_instance_net_addr_t;

typedef struct st_dms_profile {
    unsigned int inst_id;
    unsigned long long inst_map;
    dms_callback_t callback;
    unsigned long long data_buffer_size;
    unsigned int channel_cnt;     // Number of connections between instances
    unsigned int work_thread_cnt; // Number of MES working threads
    unsigned int max_session_cnt; // Number of client sessions to be supported
    unsigned short mfc_tickets; // message flow control, max requests from A instance to B instance
    unsigned short mfc_max_wait_ticket_time; // max time to wait for ticket while sending a message
    unsigned int page_size;
    unsigned long long recv_msg_buf_size;
    unsigned int log_level;

    dms_conn_mode_t pipe_type;    // Inter-instance communication mode. Currently, only TCP and RDMA are supported.
    unsigned int inst_cnt;        // Number of cluster instances
    dms_instance_net_addr_t inst_net_addr[DMS_MAX_INSTANCES]; // Cluster instance ip and port
    // Indicates whether to connected to other instances during DMS initialization.
    unsigned int conn_created_during_init : 1;
    unsigned int resource_catalog_centralized : 1; // 1: centralized, 0: distributed
    unsigned int time_stat_enabled : 1;
    unsigned int reserved : 29;
    unsigned int elapsed_switch;
    unsigned char rdma_rpc_use_busypoll;    // busy poll need to occupy the cpu core
    unsigned char rdma_rpc_is_bind_core;
    unsigned char rdma_rpc_bind_core_start;
    unsigned char rdma_rpc_bind_core_end;
    char ock_log_path[DMS_OCK_LOG_PATH_LEN];
    unsigned char enable_reform;
    // ock scrlock configs
    unsigned char enable_scrlock;
    unsigned int primary_inst_id;
    unsigned char enable_ssl;
    unsigned int scrlock_log_level;
    unsigned char enable_scrlock_worker_bind_core;
    unsigned int scrlock_worker_cnt;
    unsigned char scrlock_worker_bind_core_start;
    unsigned char scrlock_worker_bind_core_end;
    unsigned int scrlock_server_port;
    unsigned char enable_scrlock_server_sleep_mode;
    unsigned char scrlock_server_bind_core_start;
    unsigned char scrlock_server_bind_core_end;
    unsigned char parallel_thread_num;
    unsigned int max_wait_time;
    char gsdb_home[DMS_LOG_PATH_LEN];
    unsigned char enable_mes_task_threadpool;
    unsigned int mes_task_worker_max_cnt;
    unsigned int max_alive_time_for_abnormal_status;
} dms_profile_t;

typedef struct st_logger_param {
    unsigned int log_level;
    unsigned long long log_max_file_size;
    unsigned int log_backup_file_count;
    char log_home[DMS_LOG_PATH_LEN];
} logger_param_t;

#define DMS_BUF_CTRL_IS_OWNER(ctrl) ((ctrl)->lock_mode == DMS_LOCK_EXCLUSIVE || \
    ((ctrl)->lock_mode == DMS_LOCK_SHARE))
#define DMS_BUF_CTRL_NOT_LOCK(ctrl)  ((ctrl)->lock_mode == DMS_LOCK_NULL)

typedef enum en_dms_info_id {
    DMS_INFO_REFORM_CURRENT = 0,
    DMS_INFO_REFORM_LAST = 1,
} dms_info_id_e;

typedef struct st_wait_cmd_stat_result {
    char name[DMS_MAX_NAME_LEN];
    char p1[DMS_MAX_NAME_LEN];
    char wait_class[DMS_MAX_NAME_LEN];
    unsigned long long wait_count;
    unsigned long long wait_time;
    unsigned char is_valid;
} wait_cmd_stat_result_t;

typedef struct st_drc_local_lock_res_result {
    char               lock_id[DMS_MAX_NAME_LEN];
    unsigned char      is_owner;
    unsigned char      is_locked;
    unsigned char      releasing;
    unsigned char      unused;
    unsigned short     shared_count;
    unsigned short     stat;
    unsigned short     sid;
    unsigned char      lock_mode;
    unsigned char      is_valid;
} drc_local_lock_res_result_t;

typedef enum en_reform_callback_stat {
    REFORM_CALLBACK_STAT_CKPT_LATCH = 0,
    REFORM_CALLBACK_STAT_BUCKET_LOCK,
    REFORM_CALLBACK_STAT_SS_READ_LOCK,
    REFORM_CALLBACK_STAT_ENTRY_TLOCK,
    REFORM_CALLBACK_STAT_PART_ENTRY_TLOCK,
    REFORM_CALLBACK_STAT_REBUILD_TLOCK_REMOTE,
    REFORM_CALLBACK_STAT_GET_DISK_LSN,
    REFORM_CALLBACK_STAT_DRC_EXIST,
    REFORM_CALLBACK_STAT_CLEAN_EDP,
    REFORM_CALLBACK_STAT_NEED_NOT_REBUILD,
    REFORM_CALLBACK_STAT_EXPIRE,
    REFORM_MES_TASK_STAT_CONFIRM_OWNER_BUCKET_LOCK,
    REFORM_MES_TASK_STAT_CONFIRM_OWNER_GET_DISK_LSN,
    REFORM_MES_TASK_STAT_CONFIRM_CVT_BUCKET_LOCK,
    REFORM_MES_TASK_STAT_CONFIRM_CVT_SS_READ_LOCK,
    REFORM_MES_TASK_STAT_NEED_FLUSH_ALLOC_CTRL,
    REFORM_MES_TASK_STAT_NEED_FLUSH_SS_READ_LOCK,
    REFORM_MES_TASK_STAT_EDP_TO_OWNER_GET_DISK_LSN,
    REFORM_MES_TASK_STAT_EDP_TO_OWNER_ALLOC_CTRL,
    REFORM_CALLBACK_STAT_VALIDATE_DRC_PAGE_BUCKET_LOCK,
    REFORM_CALLBACK_STAT_VALIDATE_DRC_PAGE_SS_READ_LOCK,
    REFORM_CALLBACK_STAT_VALIDATE_DRC_PAGE_REMOTE,
    REFORM_CALLBACK_STAT_VALIDATE_DRC_ENTRY_TLOCK,
    REFORM_CALLBACK_STAT_VALIDATE_DRC_PART_ENTRY_TLOCK,
    REFORM_CALLBACK_STAT_VALIDATE_DRC_TLOCK_REMOTE,
    REFORM_MES_TASK_STAT_VALIDATE_LSN_GET_CTRL,
    REFORM_MES_TASK_STAT_VALIDATE_LSN_GET_CTRL_TIMEOUT,
    REFORM_MES_TASK_STAT_VALIDATE_LSN_GET_DISK_LSN,
    REFORM_MES_TASK_STAT_VALIDATE_LSN_BUF_UNLATCH,

    REFORM_CALLBACK_STAT_COUNT
} reform_callback_stat_e;

typedef struct st_dms_tlock_info {
    dms_drid_t resid;
    unsigned char lock_mode;
    unsigned char unused[3];
} dms_tlock_info_t;

#ifdef __cplusplus
}
#endif

#endif /* __DMS_H__ */

