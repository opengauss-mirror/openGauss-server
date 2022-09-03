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

GtmHostIndex InitGTM(bool useCache)
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
CommitSeqNo GetCSNGTM(GTM_Timestamp *timestamp)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return InvalidCommitSeqNo;
}

CommitSeqNo CommitCSNGTM(bool need_clean, GTM_Timestamp *timestamp)
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
#endif
