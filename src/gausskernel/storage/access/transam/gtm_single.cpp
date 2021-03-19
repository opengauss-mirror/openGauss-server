/* -------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2000-2016, PostgreSQL Global Development Group
 *
 *
 * gtm_single.cpp
 *
 *	  Module interfacing with GTM
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/access/transam/gtm_single.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <sys/types.h>
#include <unistd.h>

#include "gtm/utils/libpq-fe.h"
#include "gtm/utils/libpq-int.h"
#include "gtm/gtm_client.h"
#include "access/gtm.h"
#include "access/transam.h"
#include "storage/procarray.h"
#include "utils/elog.h"
#include "miscadmin.h"
#include "job/job_scheduler.h"
#include "job/job_worker.h"
#include "pgxc/pgxc.h"
#include "pgxc/execRemote.h"
#include "postmaster/autovacuum.h"
#include "workload/workload.h"
#include "postmaster/postmaster.h"
#include "catalog/pg_control.h"
#include "alarm/alarm.h"
#include "pgstat.h"

#define MAX_HOSTNAME_LEN 128

#define MAXADRLEN 1024

#include "securec.h"

#ifdef ENABLE_UT
#define static
#endif

volatile char LastInuseHost[MAX_HOSTNAME_LEN] = {0};
volatile int LastInusePort = 0;
volatile GtmHostIndex LastInuseHostIndex = GTM_HOST_INVAILD;

THR_LOCAL bool need_reset_xmin = true;
extern THR_LOCAL volatile sig_atomic_t catchupInterruptPending;

extern char *get_database_name(Oid dbid);

bool DataInstConnToGTMSucceed = true;

AlarmCheckResult DataInstConnToGTMChecker(Alarm *alarm, AlarmAdditionalParam *additionalParam)
{
    if (true == DataInstConnToGTMSucceed) {
        /* fill the resume message */
        WriteAlarmAdditionalInfo(additionalParam, g_instance.attr.attr_common.PGXCNodeName, "", "", alarm,
                                 ALM_AT_Resume);
        return ALM_ACR_Normal;
    } else {
        /* fill the alarm message */
        WriteAlarmAdditionalInfo(additionalParam, g_instance.attr.attr_common.PGXCNodeName, "", "", alarm, ALM_AT_Fault,
                                 g_instance.attr.attr_common.PGXCNodeName);
        return ALM_ACR_Abnormal;
    }
}

#ifndef ENABLE_MULTIPLE_NODES
bool IsGTMConnected()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

GtmHostIndex InitGTM(void)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return GTM_HOST_INVAILD;
}

void CloseGTM(void)
{
    volatile PgBackendStatus *beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
    /* Clear GTM connection information in MyBEEntry */
    if (NULL != beentry) {
        SpinLockAcquire(&beentry->use_mutex);
        pgstat_report_connected_gtm_host(GTM_HOST_INVAILD);
        pgstat_report_connected_gtm_timeline(InvalidTransactionTimeline);
        SpinLockRelease(&beentry->use_mutex);
    }
}

void ResetGtmHandleXmin(GTM_TransactionKey txnKey)
{
    /* in Single_Node mode, txnKey.txnHandle must be inValid, and t_thrd.xact_cxt.conn must be NULL, */
    /* so, just return here. */
    return;
}

int SetGTMVacuumFlag(GTM_TransactionKey txnKey, bool is_vacuum)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return GTM_RESULT_ERROR;
}

GTM_TransactionKey BeginTranGTM(GTM_Timestamp *timestamp)
{
    GTM_TransactionKey txn;
    txn.txnHandle = InvalidTransactionHandle;
    txn.txnTimeline = InvalidTransactionTimeline;

    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return txn;
}

/* begin transaction on GTM and get gxid from GTM */
GlobalTransactionId GetGxidGTM(GTM_TransactionKey txn, bool is_sub_xact)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return InvalidGlobalTransactionId;
}

/**
 * @Description: get the next csn on success without really commit in gtm.
 * @return -  return the next csn on gtm without commit
 */
CommitSeqNo GetCSNGTM()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return InvalidCommitSeqNo;
}

CommitSeqNo CommitCSNGTM(bool need_clean)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return InvalidCommitSeqNo;
}

/**
 * @Description: get the global xmin in gtm.
 * @return -  return the global xmin in gtm
 */
TransactionId GetGTMGlobalXmin()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return InvalidTransactionId;
}

GlobalTransactionId BeginTranAutovacuumGTM(void)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return InvalidGlobalTransactionId;
}

int CommitTranGTM(GlobalTransactionId gxid, TransactionId *childXids, int nChildXids)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return GTM_RESULT_ERROR;
}

/* commit transaction with handle */
int CommitTranHandleGTM(GTM_TransactionKey txnKey, GlobalTransactionId transactionId, GlobalTransactionId &outgxid)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return GTM_RESULT_ERROR;
}

/*
 * For a prepared transaction, commit the gxid used for PREPARE TRANSACTION
 * and for COMMIT PREPARED.
 */
int CommitPreparedTranGTM(GlobalTransactionId gxid, GlobalTransactionId prepared_gxid)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return GTM_RESULT_ERROR;
}

int RollbackTranGTM(GlobalTransactionId gxid, TransactionId *childXids, int nChildXids)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return GTM_RESULT_ERROR;
}

int RollbackTranHandleGTM(GTM_TransactionKey txnKey, GlobalTransactionId &outgxid)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return GTM_RESULT_ERROR;
}

int StartPreparedTranGTM(GlobalTransactionId gxid, const char *gid, const char *nodestring)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return GTM_RESULT_ERROR;
}

int PrepareTranGTM(GlobalTransactionId gxid)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return GTM_RESULT_ERROR;
}

int GetGIDDataGTM(const char *gid, GlobalTransactionId *gxid, GlobalTransactionId *prepared_gxid, char **nodestring)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return GTM_RESULT_ERROR;
}

GTM_Snapshot GetSnapshotGTM(GTM_TransactionKey txnKey, GlobalTransactionId gxid, bool canbe_grouped, bool is_vacuum)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

GTM_SnapshotStatus GetGTMSnapshotStatus(GTM_TransactionKey txnKey)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

/*
 * Create a sequence on the GTM with UUID.
 */
int CreateSequenceWithUUIDGTM(FormData_pg_sequence seq, GTM_UUID uuid)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return GTM_RESULT_ERROR;
}

/*
 * Alter a sequence on the GTM
 */
int AlterSequenceGTM(GTM_UUID seq_uuid, GTM_Sequence increment, GTM_Sequence minval, GTM_Sequence maxval,
                     GTM_Sequence startval, GTM_Sequence lastval, bool cycle, bool is_restart)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return GTM_RESULT_ERROR;
}

/*
 * Get the sequence UUID value
 */
GTM_UUID GetSeqUUIDGTM()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return GTM_RESULT_ERROR;
}

/*
 * Get the next sequence value
 */
GTM_Sequence GetNextValGTM(Form_pg_sequence seq, GTM_Sequence range, GTM_Sequence *rangemax, GTM_UUID uuid)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return GTM_RESULT_ERROR;
}

/*
 * Set values for sequence
 */
int SetValGTM(GTM_UUID seq_uuid, GTM_Sequence nextval, bool iscalled)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return GTM_RESULT_ERROR;
}

/*
 * Drop the sequence depending the key type
 *
 * Type of Sequence name use in key;
 *		GTM_SEQ_FULL_NAME, full name of sequence
 *		GTM_SEQ_DB_NAME, DB name part of sequence key
 */
int DropSequenceGTM(GTM_UUID seq_uuid, const char *dbname)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return GTM_RESULT_ERROR;
}

/*
 * Rename the sequence
 */
int RenameSequenceGTM(char *oldname, char *newname, GTM_SequenceKeyType keytype)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return GTM_RESULT_ERROR;
}

/*
 * Report BARRIER
 */
int ReportBarrierGTM(const char *barrier_id)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return GTM_RESULT_ERROR;
}

/*
 * First, initiate GTM connection and report connected GTM host index to MyBEEntry.
 * Second, get and report connected GTM host timeline to MyBEEntry.
 */
void InitGTM_Reporttimeline(void)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: set gtm response wait timeout
 * @in timeout: input parameter timeout for gtm reponse wait
 * @return: void
 */
void SetGTMrwTimeout(int timeout)
{
    return;
}

#else
void InitGTM(void)
{
    /* 256 bytes should be enough */
    char conn_str[256];
    int rc = 0;

    /* If this thread is postmaster itself, it contacts gtm identifying itself */
    if (!IsUnderPostmaster) {
        GTM_PGXCNodeType remote_type = GTM_NODE_DEFAULT;

        if (IS_PGXC_COORDINATOR)
            remote_type = GTM_NODE_COORDINATOR;
        else if (IS_PGXC_DATANODE)
            remote_type = GTM_NODE_DATANODE;

        rc = sprintf_s(conn_str, sizeof(conn_str), "host=%s port=%d node_name=%s remote_type=%d postmaster=1", GtmHost,
                       GtmPort, PGXCNodeName, remote_type);
        securec_check_ss(rc, "", "");

        /* Log activity of GTM connections */
        elog(DEBUG1, "Postmaster: connection established to GTM with string %s", conn_str);
    } else {
        rc = sprintf_s(conn_str, sizeof(conn_str), "host=%s port=%d node_name=%s", GtmHost, GtmPort, PGXCNodeName);
        securec_check_ss(rc, "", "");

        /* Log activity of GTM connections */
        if (IsAutoVacuumWorkerProcess())
            elog(DEBUG1, "Autovacuum worker: connection established to GTM with string %s", conn_str);
        else if (IsAutoVacuumLauncherProcess())
            elog(DEBUG1, "Autovacuum launcher: connection established to GTM with string %s", conn_str);
        else
            elog(DEBUG1, "Postmaster child: connection established to GTM with string %s", conn_str);
    }

    conn = PQconnectGTM(conn_str);
    if (GTMPQstatus(conn) != CONNECTION_OK) {
        int save_errno = errno;

        ereport(WARNING, (errcode(ERRCODE_CONNECTION_EXCEPTION), errmsg("can not connect to GTM: %m")));

        errno = save_errno;

        CloseGTM();
    }
}

void CloseGTM(void)
{
    GTMPQfinish(conn);
    conn = NULL;

    /* Log activity of GTM connections */
    if (!IsUnderPostmaster)
        elog(DEBUG1, "Postmaster: connection to GTM closed");
    else if (IsAutoVacuumWorkerProcess())
        elog(DEBUG1, "Autovacuum worker: connection to GTM closed");
    else if (IsAutoVacuumLauncherProcess())
        elog(DEBUG1, "Autovacuum launcher: connection to GTM closed");
    else
        elog(DEBUG1, "Postmaster child: connection to GTM closed");
}

GlobalTransactionId BeginTranGTM(GTM_Timestamp *timestamp)
{
    GlobalTransactionId xid = InvalidGlobalTransactionId;

    CheckConnection();
    if (conn)
        xid = begin_transaction(conn, GTM_ISOLATION_RC, timestamp);

    /* If something went wrong (timeout), try and reset GTM connection
     * and retry. This is safe at the beginning of a transaction.
     */
    if (!TransactionIdIsValid(xid)) {
        CloseGTM();
        InitGTM();
        if (conn)
            xid = begin_transaction(conn, GTM_ISOLATION_RC, timestamp);
    }
    currentGxid = xid;
    return xid;
}

GlobalTransactionId BeginTranAutovacuumGTM(void)
{
    GlobalTransactionId xid = InvalidGlobalTransactionId;

    CheckConnection();
    if (conn)
        xid = begin_transaction_autovacuum(conn, GTM_ISOLATION_RC);

    /*
     * If something went wrong (timeout), try and reset GTM connection and retry.
     * This is safe at the beginning of a transaction.
     */
    if (!TransactionIdIsValid(xid)) {
        CloseGTM();
        InitGTM();
        if (conn)
            xid = begin_transaction_autovacuum(conn, GTM_ISOLATION_RC);
    }
    currentGxid = xid;
    return xid;
}

int CommitTranGTM(GlobalTransactionId gxid)
{
    int ret;

    if (!GlobalTransactionIdIsValid(gxid))
        return 0;
    CheckConnection();
    ret = commit_transaction(conn, gxid);
    /*
     * If something went wrong (timeout), try and reset GTM connection.
     * We will close the transaction locally anyway, and closing GTM will force
     * it to be closed on GTM.
     */
    if (ret < 0) {
        CloseGTM();
        InitGTM();
    }

    /* Close connection in case commit is done by autovacuum worker or launcher */
    if (IsAutoVacuumWorkerProcess() || IsAutoVacuumLauncherProcess())
        CloseGTM();

    currentGxid = InvalidGlobalTransactionId;
    return ret;
}

/*
 * For a prepared transaction, commit the gxid used for PREPARE TRANSACTION
 * and for COMMIT PREPARED.
 */
int CommitPreparedTranGTM(GlobalTransactionId gxid, GlobalTransactionId prepared_gxid)
{
    int ret = 0;

    if (!GlobalTransactionIdIsValid(gxid) || !GlobalTransactionIdIsValid(prepared_gxid))
        return ret;
    CheckConnection();
    ret = commit_prepared_transaction(conn, gxid, prepared_gxid);
    /*
     * If something went wrong (timeout), try and reset GTM connection.
     * We will close the transaction locally anyway, and closing GTM will force
     * it to be closed on GTM.
     */
    if (ret < 0) {
        CloseGTM();
        InitGTM();
    }
    currentGxid = InvalidGlobalTransactionId;
    return ret;
}

int RollbackTranGTM(GlobalTransactionId gxid)
{
    int ret = -1;

    if (!GlobalTransactionIdIsValid(gxid))
        return 0;
    CheckConnection();

    if (conn)
        ret = abort_transaction(conn, gxid);

    /*
     * If something went wrong (timeout), try and reset GTM connection.
     * We will abort the transaction locally anyway, and closing GTM will force
     * it to end on GTM.
     */
    if (ret < 0) {
        CloseGTM();
        InitGTM();
    }

    currentGxid = InvalidGlobalTransactionId;
    return ret;
}

int StartPreparedTranGTM(GlobalTransactionId gxid, char *gid, char *nodestring)
{
    int ret = 0;

    if (!GlobalTransactionIdIsValid(gxid))
        return 0;
    CheckConnection();

    ret = start_prepared_transaction(conn, gxid, gid, nodestring);
    /*
     * If something went wrong (timeout), try and reset GTM connection.
     * We will abort the transaction locally anyway, and closing GTM will force
     * it to end on GTM.
     */
    if (ret < 0) {
        CloseGTM();
        InitGTM();
    }

    return ret;
}

int PrepareTranGTM(GlobalTransactionId gxid)
{
    int ret;

    if (!GlobalTransactionIdIsValid(gxid))
        return 0;
    CheckConnection();
    ret = prepare_transaction(conn, gxid);
    /*
     * If something went wrong (timeout), try and reset GTM connection.
     * We will close the transaction locally anyway, and closing GTM will force
     * it to be closed on GTM.
     */
    if (ret < 0) {
        CloseGTM();
        InitGTM();
    }
    currentGxid = InvalidGlobalTransactionId;
    return ret;
}

int GetGIDDataGTM(char *gid, GlobalTransactionId *gxid, GlobalTransactionId *prepared_gxid, char **nodestring)
{
    int ret = 0;

    CheckConnection();
    ret = get_gid_data(conn, GTM_ISOLATION_RC, gid, gxid, prepared_gxid, nodestring);
    /*
     * If something went wrong (timeout), try and reset GTM connection.
     * We will abort the transaction locally anyway, and closing GTM will force
     * it to end on GTM.
     */
    if (ret < 0) {
        CloseGTM();
        InitGTM();
    }

    return ret;
}

GTM_Snapshot GetSnapshotGTM(GlobalTransactionId gxid, bool canbe_grouped)
{
    GTM_Snapshot ret_snapshot = NULL;
    CheckConnection();
    if (conn)
        ret_snapshot = get_snapshot(conn, gxid, canbe_grouped);
    if (ret_snapshot == NULL) {
        CloseGTM();
        InitGTM();
    }
    return ret_snapshot;
}

/*
 * Create a sequence on the GTM.
 */
int CreateSequenceGTM(char *seqname, GTM_Sequence increment, GTM_Sequence minval, GTM_Sequence maxval,
                      GTM_Sequence startval, bool cycle)
{
    GTM_SequenceKeyData seqkey;
    CheckConnection();
    seqkey.gsk_keylen = strlen(seqname) + 1;
    seqkey.gsk_key = seqname;

    return conn ? open_sequence(conn, &seqkey, increment, minval, maxval, startval, cycle) : 0;
}

/*
 * Alter a sequence on the GTM
 */
int AlterSequenceGTM(char *seqname, GTM_Sequence increment, GTM_Sequence minval, GTM_Sequence maxval,
                     GTM_Sequence startval, GTM_Sequence lastval, bool cycle, bool is_restart)
{
    GTM_SequenceKeyData seqkey;
    CheckConnection();
    seqkey.gsk_keylen = strlen(seqname) + 1;
    seqkey.gsk_key = seqname;

    return conn ? alter_sequence(conn, &seqkey, increment, minval, maxval, startval, lastval, cycle, is_restart) : 0;
}

/*
 * Get the next sequence value
 */
GTM_Sequence GetNextValGTM(char *seqname)
{
    GTM_Sequence ret = -1;
    GTM_SequenceKeyData seqkey;
    int status = GTM_RESULT_OK;

    CheckConnection();
    seqkey.gsk_keylen = strlen(seqname) + 1;
    seqkey.gsk_key = seqname;

    if (conn)
        status = get_next(conn, &seqkey, &ret);
    if (status != GTM_RESULT_OK) {
        CloseGTM();
        InitGTM();
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("%s", GTMPQerrorMessage(conn))));
    }
    return ret;
}

/*
 * Set values for sequence
 */
int SetValGTM(char *seqname, GTM_Sequence nextval, bool iscalled)
{
    GTM_SequenceKeyData seqkey;
    CheckConnection();
    seqkey.gsk_keylen = strlen(seqname) + 1;
    seqkey.gsk_key = seqname;

    return conn ? set_val(conn, &seqkey, nextval, iscalled) : -1;
}

/*
 * Drop the sequence depending the key type
 *
 * Type of Sequence name use in key;
 *		GTM_SEQ_FULL_NAME, full name of sequence
 *		GTM_SEQ_DB_NAME, DB name part of sequence key
 */
int DropSequenceGTM(char *name, GTM_SequenceKeyType type)
{
    GTM_SequenceKeyData seqkey;
    CheckConnection();
    seqkey.gsk_keylen = strlen(name) + 1;
    seqkey.gsk_key = name;
    seqkey.gsk_type = type;

    return conn ? close_sequence(conn, &seqkey) : -1;
}

/*
 * Rename the sequence
 */
int RenameSequenceGTM(char *seqname, const char *newseqname)
{
    GTM_SequenceKeyData seqkey, newseqkey;
    CheckConnection();
    seqkey.gsk_keylen = strlen(seqname) + 1;
    seqkey.gsk_key = seqname;
    newseqkey.gsk_keylen = strlen(newseqname) + 1;
    newseqkey.gsk_key = (char *)newseqname;

    return conn ? rename_sequence(conn, &seqkey, &newseqkey) : -1;
}

/*
 * Report BARRIER
 */
int ReportBarrierGTM(char *barrier_id)
{
    if (!gtm_backup_barrier)
        return GTM_RESULT_OK;

    CheckConnection();

    if (!conn)
        return EOF;

    return (report_barrier(conn, barrier_id));
}
#endif
