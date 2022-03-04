/* -------------------------------------------------------------------------
 *
 * gtm_msg.h
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL$
 *
 * -------------------------------------------------------------------------
 */
#ifndef GTM_MSG_H
#define GTM_MSG_H

/*
 * The following enum symbols are also used in message_name_tab structure
 * in gtm_utils.c.   Modification of the following enum should reflect
 * changes to message_name_tab structure as well.
 */
typedef enum GTM_MessageType {
    MSG_TYPE_INVALID,
    MSG_SYNC_STANDBY,                /* Message to sync woth GTM-Standby */
    MSG_NODE_REGISTER,               /* Register a PGXC Node with GTM */
    MSG_BKUP_NODE_REGISTER,          /* Backup of MSG_NODE_REGISTER */
    MSG_NODE_UNREGISTER,             /* Unregister a PGXC Node with GTM */
    MSG_BKUP_NODE_UNREGISTER,        /* Backup of MSG_NODE_UNREGISTER */
    MSG_NODE_LIST,                   /* Get node list */
    MSG_NODE_BEGIN_REPLICATION_INIT, /* deprecated message for upgrade compatibility */
    MSG_NODE_END_REPLICATION_INIT,   /* deprecated message for upgrade compatibility */
    MSG_BEGIN_BACKUP,                /* Start backup by Standby */

    MSG_END_BACKUP,                     /* End backup preparation by Standby */
    MSG_TXN_XMINRESET,                  /* reset handle xmin when isolation is RC */
    MSG_TXN_BEGIN,                      /* Start a new transaction */
    MSG_BKUP_TXN_BEGIN,                 /* Backup of MSG_TXN_BEGIN */
    MSG_TXN_BEGIN_GETGXID,              /* Start a new transaction and get GXID */
    MSG_BKUP_TXN_BEGIN_GETGXID,         /* Backup of MSG_TXN_BEGIN_GETGXID */
    MSG_BKUP_GTM_CONTROL_FILE_GXID,     /*Backup of gtm control file gxid */
    MSG_BKUP_GTM_CONTROL_FILE_TIMELINE, /*Backup of gtm control file timeline */
    MSG_TXN_BEGIN_GETGXID_MULTI,        /* Start multiple new transactions and get GXIDs */
    MSG_BKUP_TXN_BEGIN_GETGXID_MULTI,   /* Backup of MSG_TXN_BEGIN_GETGXID_MULTI */
    MSG_TXN_START_PREPARED,             /* Begins to prepare a transation for commit */
    MSG_BKUP_TXN_START_PREPARED,        /* Backup of MSG_TXN_START_PREPARED */

    MSG_TXN_COMMIT,               /* Commit a running or prepared transaction */
    MSG_BKUP_TXN_COMMIT,          /* Backup of MSG_TXN_COMMIT */
    MSG_TXN_COMMIT_MULTI,         /* Commit multiple running or prepared transactions */
    MSG_BKUP_TXN_COMMIT_MULTI,    /* Bacukp of MSG_TXN_COMMIT_MULTI */
    MSG_TXN_COMMIT_PREPARED,      /* Commit a prepared transaction */
    MSG_BKUP_TXN_COMMIT_PREPARED, /* Backup of MSG_TXN_COMMIT_PREPARED */
    MSG_TXN_PREPARE,              /* Finish preparing a transaction */
    MSG_BKUP_TXN_PREPARE,         /* Backup of MSG_TXN_PREPARE */
    MSG_TXN_ROLLBACK,             /* Rollback a transaction */
    MSG_BKUP_TXN_ROLLBACK,        /* Backup of MSG_TXN_ROLLBACK */

    MSG_TXN_ROLLBACK_MULTI,      /* Rollback multiple transactions */
    MSG_BKUP_TXN_ROLLBACK_MULTI, /* Backup of MSG_TXN_ROLLBACK_MULTI */
    MSG_TXN_GET_GID_DATA,        /* Get info associated with a GID, and get a GXID */
    MSG_TXN_GET_GXID,            /* Get a GXID for a transaction */
    MSG_SUB_TXN_GET_GXID,        /* Get a GXID for a sub transaction */
    MSG_BKUP_TXN_GET_GXID,
    MSG_TXN_GET_NEXT_GXID,   /* Get next GXID */
    MSG_TXN_GET_NEXT_CSN,    /* Get next CSN without commit */
    MSG_TXN_GET_TIMELINE,    /* Get gtm timeline*/
    MSG_TXN_GET_GLOBAL_XMIN, /* Get global xmin */
    MSG_TXN_GXID_LIST,
    MSG_SET_VACUUM_FLAG,
    MSG_SNAPSHOT_GET,        /* Get a global snapshot */
    MSG_SNAPSHOT_GET_MULTI,  /* Get multiple global snapshots */
    MSG_SNAPSHOT_STATUS_GET, /* Get snapshot status */

    MSG_SNAPSHOT_GXID_GET,      /* Get GXID and snapshot together */
    MSG_SEQUENCE_GET_NEXT_UUID, /* Get sequuid for issuring sequence */
    MSG_SEQUENCE_UUID,          /* Create UUID matched with seqKey */
    MSG_BKUP_SEQUENCE_UUID,     /* Backup of MSG_SEQUENCE_UUID */
    MSG_BKUP_GTM_UUID,          /* Backup of gtm control file seq_uuid*/
    MSG_SEQUENCE_INIT,          /* Initialize a new global sequence =41*/
    MSG_BKUP_SEQUENCE_INIT,     /* Backup of MSG_SEQUENCE_INIT */
    MSG_SEQUENCE_GET_NEXT,      /* Get the next sequence value of sequence */
    MSG_BKUP_SEQUENCE_GET_NEXT, /* Backup of MSG_SEQUENCE_GET_NEXT */
    MSG_SEQUENCE_GET_LAST,      /* Get the last sequence value of sequence */
    MSG_SEQUENCE_SET_VAL,       /* Set values for sequence */
    MSG_BKUP_SEQUENCE_SET_VAL,  /* Backup of MSG_SEQUENCE_SET_VAL */
    MSG_SEQUENCE_RESET,         /* Reset the sequence */
    MSG_BKUP_SEQUENCE_RESET,    /* Backup of MSG_SEQUENCE_RESET */

    MSG_SEQUENCE_CLOSE,                    /* Close a previously inited sequence 50*/
    MSG_BKUP_SEQUENCE_CLOSE,               /* Backup of MSG_SEQUENCE_CLOSE */
    MSG_SEQUENCE_RENAME,                   /* Rename a sequence */
    MSG_BKUP_SEQUENCE_RENAME,              /* Backup of MSG_SEQUENCE_RENAME */
    MSG_SEQUENCE_ALTER,                    /* Alter a sequence */
    MSG_BKUP_SEQUENCE_ALTER,               /* Backup of MSG_SEQUENCE_ALTER */
    MSG_SEQUENCE_LIST,                     /* Get a list of sequences */
    MSG_TXN_GET_STATUS,                    /* Get status of a given transaction */
    MSG_TXN_GET_ALL_PREPARED,              /* Get information about all outstanding
                                            * prepared transactions */
    MSG_TXN_BEGIN_GETGXID_AUTOVACUUM,      /* Start a new transaction and get GXID for autovacuum */
    MSG_BKUP_TXN_BEGIN_GETGXID_AUTOVACUUM, /* Backup of MSG_TXN_BEGIN_GETGXID_AUTOVACUUM */
    MSG_DATA_FLUSH,                        /* flush pending data */
    MSG_BACKEND_DISCONNECT,                /* tell GTM that the backend diconnected from the proxy */
    MSG_BARRIER,                           /* Tell the barrier was issued */
    MSG_BKUP_BARRIER,                      /* Backup barrier to standby */
    MSG_BEGIN_SWITCHOVER,                  /*Tell primary GTM to do switchover*/
    MSG_END_SWITCHOVER,                    /*Tell primary GTM to do switchover*/
    MSG_QUERY_GTM_STATUS,
    MSG_SET_GTM_SYNCMODE,
    MSG_CHECK_REMOTE_ROLE,
    MSG_SYNC_FILE,
    MSG_TXN_WORKLOAD_INIT,             /* Initialize the workload manager bookkeeper */
    MSG_BKUP_TXN_WORKLOAD_INIT,        /* Backup of MSG_TXN_WORKLOAD_INIT */
    MSG_TXN_WORKLOAD_RESERVE_MEM,      /* Workload management: reserve memory if there is enough */
    MSG_BKUP_TXN_WORKLOAD_RESERVE_MEM, /*  Backup of MSG_TXN_WORKLOAD_RESERVE_MEM */
    MSG_TXN_WORKLOAD_RELEASE_MEM,      /* return to WLM the memory reserved after execution is done */
    MSG_BKUP_TXN_WORKLOAD_RELEASE_MEM, /* Backup of MSG_TXN_WORKLOAD_RELEASE_MEM */
    MSG_WLM_RESOURCEPOOL_CREATE,       /*create resource pool on gtm*/
    MSG_WLM_RESOURCEPOOL_UPDATE,       /*update resource pool on gtm*/
    MSG_WLM_RESOURCEPOOL_DELETE,       /*delete resource pool on gtm*/
    MSG_WLM_RESOURCEPOOL_INIT,         /*initialize resource pool list on gtm*/
    MSG_CHECK_NEXT_GXID,
    MSG_BKUP_GTM_CONTROL_FILE_SEQUENCE, /*Backup of gtm control file sequence*/

    MSG_HOTPATCH,

    MSG_TXN_GET_NEXT_CSN_LITE,          /* gtm-lite commit csn */
    MSG_SNAPSHOT_GET_LITE,              /* Get a GTMLite global snapshot */
    MSG_GET_GTM_LITE_STATUS,            /* Get the status of gtm lite */

    MSG_SET_DISASTER_CLUSTER,
    MSG_GET_DISASTER_CLUSTER,
    MSG_DEL_DISASTER_CLUSTER,
    MSG_SNAPSHOT_GET_DR,
    MSG_SET_CONSISTENCY_POINT,
    /*
     * Must be at the end
     */
    MSG_TYPE_COUNT /* A dummmy entry just to count the message types */
} GTM_MessageType;

/*
 * Symbols in the following enum are usd in result_name_tab defined in gtm_utils.c.
 * Modifictaion to the following enum should be reflected to result_name_tab as well.
 */
typedef enum GTM_ResultType {
    SYNC_STANDBY_RESULT,
    NODE_REGISTER_RESULT,
    NODE_UNREGISTER_RESULT,
    NODE_LIST_RESULT,
    NODE_BEGIN_REPLICATION_INIT_RESULT, /* deprecated return type for upgrade compatibility */
    NODE_END_REPLICATION_INIT_RESULT,   /* deprecated return type for upgrade compatibility */
    BEGIN_BACKUP_RESULT,
    END_BACKUP_RESULT,
    TXN_XMINRESET_RESULT,
    TXN_SETVACUUM_RESULT,
    TXN_BEGIN_RESULT,
    TXN_BEGIN_GETGXID_RESULT,
    TXN_BEGIN_GETGXID_MULTI_RESULT,
    TXN_PREPARE_RESULT,
    TXN_START_PREPARED_RESULT,
    TXN_COMMIT_PREPARED_RESULT,
    TXN_COMMIT_RESULT,
    TXN_COMMIT_MULTI_RESULT,
    TXN_ROLLBACK_RESULT,
    TXN_ROLLBACK_MULTI_RESULT,
    TXN_GET_GID_DATA_RESULT,
    TXN_GET_GXID_RESULT,
    TXN_GET_NEXT_GXID_RESULT,
    TXN_GET_NEXT_CSN_RESULT,
    TXN_GET_TIMELINE_RESULT,
    TXN_GET_GLOBAL_XMIN_RESULT,
    TXN_WORKLOAD_INIT_RESULT,
    TXN_WORKLOAD_RESERVE_MEM_RESULT,
    TXN_WORKLOAD_RELEASE_MEM_RESULT,
    WLM_RESOURCEPOOL_CREATE_RESULT, /*create resource pool on gtm*/
    WLM_RESOURCEPOOL_UPDATE_RESULT, /*update resource pool on gtm*/
    WLM_RESOURCEPOOL_DELETE_RESULT, /*delete resource pool on gtm*/
    WLM_RESOURCEPOOL_INIT_RESULT,   /*initialize resource pool list on gtm*/

    TXN_GXID_LIST_RESULT,
    SNAPSHOT_GET_RESULT,
    SNAPSHOT_GET_MULTI_RESULT,
    SNAPSHOT_GXID_GET_RESULT,
    SNAPSHOT_STATUS_GET_RESULT,
    SEQUENCE_GET_NEXT_UUID_RESULT,
    SEQUENCE_GET_UUID_RESULT,
    SEQUENCE_INIT_RESULT,
    SEQUENCE_GET_NEXT_RESULT,
    SEQUENCE_GET_LAST_RESULT,
    SEQUENCE_SET_VAL_RESULT,
    SEQUENCE_RESET_RESULT,
    SEQUENCE_CLOSE_RESULT,
    SEQUENCE_RENAME_RESULT,
    SEQUENCE_ALTER_RESULT,
    SEQUENCE_LIST_RESULT,
    TXN_GET_STATUS_RESULT,
    TXN_GET_ALL_PREPARED_RESULT,
    TXN_BEGIN_GETGXID_AUTOVACUUM_RESULT,
    BARRIER_RESULT,
    BEGIN_SWITCHOVER_RESULT,
    END_SWITCHOVER_RESULT,
    QUERY_GTM_STATUS_RESULT,
    CHECK_STANDBY_ROLE_RESULT,
    SYNC_FILE_RESUTL,
    GTM_SETSYNCMODE_RESULT,
    CHECK_SYNC_NEXT_GXID_RESULT,
    GTM_HOTPATCH_RESULT,
    SNAPSHOT_GET_RESULT_LITE,
    GTM_LITE_STATUS_RESULT,
    DISASTER_CLUSTER_SET_RESULT,
    DISASTER_CLUSTER_GET_RESULT,
    DISASTER_CLUSTER_DEL_RESULT,
    SNAPSHOT_GET_RESULT_DR,
    TXN_SET_CONSISTENCY_POINT_RESULT,
    RESULT_TYPE_COUNT
} GTM_ResultType;

/*
 * Special message header for the messgaes exchanged between the GTM server and
 * the proxy.
 *
 * ph_conid: connection identifier which is used to route
 * the messages to the right backend.
 */
typedef struct GTM_ProxyMsgHeader {
    GTMProxy_ConnID ph_conid;
} GTM_ProxyMsgHeader;

#endif
