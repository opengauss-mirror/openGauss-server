/* -------------------------------------------------------------------------
 *
 * gtm_serialize.c
 *  Serialization management of GTM data
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *        src/gtm/common/gtm_serialize.c
 *
 * -------------------------------------------------------------------------
 */

#include "gtm/gtm_c.h"
#include "gtm/elog.h"
#include "gtm/gtm.h"
#include "gtm/gtm_txn.h"
#include "gtm/gtm_seq.h"
#include "gtm/assert.h"
#include "gtm/register.h"
#include "gtm/stringinfo.h"
#include "gtm/libpq.h"
#include "gtm/pqformat.h"
#include "gtm/gtm_msg.h"

#include "gen_alloc.h"

#include "gtm/gtm_serialize.h"

/*
 * gtm_get_snapshotdata_size
 * Get a serialized size of GTM_SnapshotData structure
 * Corrected snapshort serialize data calculation.
 * May 3rd, 2011, K.Suzuki
 *
 * Serialize of snapshot_data
 *
 * sn_xmin ---> sn_xmax ---> sn_recent_global_xmin
 * ---> sn_xcnt ---> GXID * sn_xcnt
 *  |<--- sn_xip -->|
 */
size_t gtm_get_snapshotdata_size(GTM_SnapshotData* data)
{
    size_t len = 0;
    uint32 snapshot_elements;

    snapshot_elements = data->sn_xcnt;
    len += sizeof(GlobalTransactionId);
    len += sizeof(GlobalTransactionId);
    len += sizeof(GlobalTransactionId);
    len += sizeof(uint32);
    len += sizeof(GlobalTransactionId) * snapshot_elements;

    return len;
}

/*
 * gtm_serialize_snapshotdata
 * Serialize a GTM_SnapshotData structure
 */
size_t gtm_serialize_snapshotdata(GTM_SnapshotData* data, char* buf, size_t buflen)
{
    int len = 0;
    errno_t rc = 0;

    rc = memset_s(buf, buflen, 0, buflen);
    securec_check(rc, "\0", "\0");

    /* size check */
    if (gtm_get_snapshotdata_size(data) > buflen) {
        return 0;
    }

    /* GTM_SnapshotData.sn_xmin */
    rc = memcpy_s(buf + len, sizeof(GlobalTransactionId), &(data->sn_xmin), sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_SnapshotData.sn_xmax */
    rc = memcpy_s(buf + len, sizeof(GlobalTransactionId), &(data->sn_xmax), sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_SnapshotData.sn_recent_global_xmin */
    rc = memcpy_s(buf + len, sizeof(GlobalTransactionId), &(data->sn_recent_global_xmin), sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_SnapshotData.sn_xcnt */
    rc = memcpy_s(buf + len, sizeof(uint32), &(data->sn_xcnt), sizeof(uint32));
    securec_check(rc, "\0", "\0");
    len += sizeof(uint32);

    if (data->sn_xcnt > 0) {
        rc = memcpy_s(buf + len, sizeof(GlobalTransactionId) * data->sn_xcnt, data->sn_xip,
                      sizeof(GlobalTransactionId) * data->sn_xcnt);
        securec_check(rc, "\0", "\0");
        len += sizeof(GlobalTransactionId) * data->sn_xcnt;
    }

    return len;
}

/* -----------------------------------------------------
 * Deserialize a GTM_SnapshotData structure
 * -----------------------------------------------------
 */
size_t gtm_deserialize_snapshotdata(GTM_SnapshotData* data, const char* buf, size_t buflen)
{
    size_t len = 0;
    errno_t rc = 0;

    /* GTM_SnapshotData.sn_xmin */
    rc = memcpy_s(&(data->sn_xmin), sizeof(GlobalTransactionId), buf + len, sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_SnapshotData.sn_xmax */
    rc = memcpy_s(&(data->sn_xmax), sizeof(GlobalTransactionId), buf + len, sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_SnapshotData.sn_recent_global_xmin */
    rc = memcpy_s(&(data->sn_recent_global_xmin), sizeof(GlobalTransactionId), buf + len, sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_SnapshotData.sn_xcnt */
    rc = memcpy_s(&(data->sn_xcnt), sizeof(uint32), buf + len, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    len += sizeof(uint32);

    /* GTM_SnapshotData.sn_xip */
    if (data->sn_xcnt > 0) {
        /*
         * Please note that this function runs with TopMemoryContext.  So we must
         * free this area manually later.
         */
        data->sn_xip = genAlloc(sizeof(GlobalTransactionId) * data->sn_xcnt);
        rc = memcpy_s(data->sn_xip, sizeof(GlobalTransactionId) * data->sn_xcnt, buf + len,
                      sizeof(GlobalTransactionId) * data->sn_xcnt);
        securec_check(rc, "\0", "\0");
        len += sizeof(GlobalTransactionId) * data->sn_xcnt;
    } else {
        data->sn_xip = NULL;
    }

    return len;
}

/*
 * gtm_get_transactioninfo_size
 * Get a serialized size of GTM_TransactionInfo structure
 *
 * Original gti_gid serialization was just "null-terminated string".
 * This should be prefixed with the length of the string.
 */
size_t gtm_get_transactioninfo_size(GTM_TransactionInfo* data)
{
    size_t len = 0;

    if (data == NULL) {
        return len;
    }

    len += sizeof(GTM_TransactionHandle); /* gti_handle */
    len += sizeof(GTM_ThreadID);          /* gti_thread_id */
    len += sizeof(bool);                  /* gti_in_use */
    len += sizeof(GlobalTransactionId);   /* gti_gxid */
    len += sizeof(GTM_TransactionStates); /* gti_state */
    len += sizeof(uint32);                /* used to store length of gti_coordname*/
    if (data->gti_coordname != NULL) {
        len += strlen(data->gti_coordname); /* gti_coordname */
    }
    len += sizeof(GlobalTransactionId);     /* gti_xmin */
    len += sizeof(GTM_IsolationLevel);      /* gti_isolevel */
    len += sizeof(bool);                    /* gti_readonly */
    len += sizeof(GTMProxy_ConnID);         /* gti_backend_id */
    len += sizeof(uint32);                  /* gti_nodestring length */
    if (data->nodestring != NULL) {
        len += strlen(data->nodestring);
    }

    len += sizeof(uint32);
    if (data->gti_gid != NULL) {
        len += strlen(data->gti_gid); /* gti_gid */
    }

    len += gtm_get_snapshotdata_size(&(data->gti_current_snapshot));
    /* gti_current_snapshot */
    len += sizeof(bool); /* gti_snapshot_set */
    /* NOTE: nothing to be done for gti_lock */
    len += sizeof(bool); /* gti_vacuum */

    return len;
}

/* -----------------------------------------------------
 * Serialize a GTM_TransactionInfo structure
 * -----------------------------------------------------
 */
size_t gtm_serialize_transactioninfo(GTM_TransactionInfo* data, char* buf, size_t buflen)
{
    int len = 0;
    char* buf2;
    int i;
    int namelen;
    errno_t rc = 0;

    /* size check */
    if (gtm_get_transactioninfo_size(data) > buflen) {
        return 0;
    }

    rc = memset_s(buf, buflen, 0, buflen);
    securec_check(rc, "\0", "\0");

    /* GTM_TransactionInfo.gti_handle */
    rc = memcpy_s(buf + len, sizeof(GTM_TransactionHandle), &(data->gti_handle), sizeof(GTM_TransactionHandle));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_TransactionHandle);

    /* GTM_TransactionInfo.gti_thread_id */
    rc = memcpy_s(buf + len, sizeof(GTM_ThreadID), &(data->gti_thread_id), sizeof(GTM_ThreadID));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_ThreadID);

    /* GTM_TransactionInfo.gti_in_use */
    rc = memcpy_s(buf + len, sizeof(bool), &(data->gti_in_use), sizeof(bool));
    securec_check(rc, "\0", "\0");
    len += sizeof(bool);

    /* GTM_TransactionInfo.gti_gxid */
    rc = memcpy_s(buf + len, sizeof(GlobalTransactionId), &(data->gti_gxid), sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_TransactionInfo.gti_state */
    rc = memcpy_s(buf + len, sizeof(GTM_TransactionStates), &(data->gti_state), sizeof(GTM_TransactionStates));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_TransactionStates);

    /* GTM_TransactionInfo.gti_coordname */
    if (data->gti_coordname != NULL) {
        namelen = (uint32)strlen(data->gti_coordname);
        rc = memcpy_s(buf + len, sizeof(uint32), &namelen, sizeof(uint32));
        securec_check(rc, "\0", "\0");
        len += sizeof(uint32);
        rc = memcpy_s(buf + len, namelen, data->gti_coordname, namelen);
        securec_check(rc, "\0", "\0");
        len += namelen;
    } else {
        namelen = 0;
        rc = memcpy_s(buf + len, sizeof(uint32), &namelen, sizeof(uint32));
        securec_check(rc, "\0", "\0");
        len += sizeof(uint32);
    }

    /* GTM_TransactionInfo.gti_xmin */
    rc = memcpy_s(buf + len, sizeof(GlobalTransactionId), &(data->gti_xmin), sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_TransactionInfo.gti_isolevel */
    rc = memcpy_s(buf + len, sizeof(GTM_IsolationLevel), &(data->gti_isolevel), sizeof(GTM_IsolationLevel));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_IsolationLevel);

    /* GTM_TransactionInfo.gti_readonly */
    rc = memcpy_s(buf + len, sizeof(bool), &(data->gti_readonly), sizeof(bool));
    securec_check(rc, "\0", "\0");
    len += sizeof(bool);

    /* GTM_TransactionInfo.gti_backend_id */
    rc = memcpy_s(buf + len, sizeof(GTMProxy_ConnID), &(data->gti_backend_id), sizeof(GTMProxy_ConnID));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTMProxy_ConnID);

    /* GTM_TransactionInfo.nodestring */
    if (data->nodestring != NULL) {
        uint32 gidlen;

        gidlen = (uint32)strlen(data->nodestring);
        rc = memcpy_s(buf + len, sizeof(uint32), &gidlen, sizeof(uint32));
        securec_check(rc, "\0", "\0");
        len += sizeof(uint32);
        rc = memcpy_s(buf + len, gidlen, data->nodestring, gidlen);
        securec_check(rc, "\0", "\0");
        len += gidlen;
    } else {
        uint32 gidlen = 0;

        rc = memcpy_s(buf + len, sizeof(uint32), &gidlen, sizeof(uint32));
        securec_check(rc, "\0", "\0");
        len += sizeof(uint32);
    }

    /* GTM_TransactionInfo.gti_gid */
    if (data->gti_gid != NULL) {
        uint32 gidlen;

        gidlen = (uint32)strlen(data->gti_gid);
        rc = memcpy_s(buf + len, sizeof(uint32), &gidlen, sizeof(uint32));
        securec_check(rc, "\0", "\0");
        len += sizeof(uint32);
        rc = memcpy_s(buf + len, gidlen, data->gti_gid, gidlen);
        securec_check(rc, "\0", "\0");
        len += gidlen;
    } else {
        uint32 gidlen = 0;

        rc = memcpy_s(buf + len, sizeof(uint32), &gidlen, sizeof(uint32));
        securec_check(rc, "\0", "\0");
        len += sizeof(uint32);
    }

    /* GTM_TransactionInfo.gti_current_snapshot */
    buf2 = malloc(gtm_get_snapshotdata_size(&(data->gti_current_snapshot)));
    i = gtm_serialize_snapshotdata(
        &(data->gti_current_snapshot), buf2, gtm_get_snapshotdata_size(&(data->gti_current_snapshot)));
    rc = memcpy_s(buf + len, i, buf2, i);
    securec_check(rc, "\0", "\0");
    free(buf2);
    len += i;

    /* GTM_TransactionInfo.gti_snapshot_set */
    rc = memcpy_s(buf + len, sizeof(bool), &(data->gti_snapshot_set), sizeof(bool));
    securec_check(rc, "\0", "\0");
    len += sizeof(bool);

    /* GTM_TransactionInfo.gti_lock would not be serialized. */

    /* GTM_TransactionInfo.gti_vacuum */
    rc = memcpy_s(buf + len, sizeof(bool), &(data->gti_vacuum), sizeof(bool));
    securec_check(rc, "\0", "\0");
    len += sizeof(bool);

    return len;
}

/* -----------------------------------------------------
 * Deserialize a GTM_TransactionInfo structure
 * -----------------------------------------------------
 */
size_t gtm_deserialize_transactioninfo(GTM_TransactionInfo* data, const char* buf, size_t maxlen)
{
    int len = 0;
    int i;
    uint32 string_len;
    errno_t rc = 0;

    rc = memset_s(data, sizeof(GTM_TransactionInfo), 0, sizeof(GTM_TransactionInfo));
    securec_check(rc, "\0", "\0");

    /* GTM_TransactionInfo.gti_handle */
    rc = memcpy_s(&(data->gti_handle), sizeof(GTM_TransactionHandle), buf + len, sizeof(GTM_TransactionHandle));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_TransactionHandle);

    /* GTM_TransactionInfo.gti_thread_id */
    rc = memcpy_s(&(data->gti_thread_id), sizeof(GTM_ThreadID), buf + len, sizeof(GTM_ThreadID));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_ThreadID);

    /* GTM_TransactionInfo.gti_in_use */
    rc = memcpy_s(&(data->gti_in_use), sizeof(bool), buf + len, sizeof(bool));
    securec_check(rc, "\0", "\0");
    len += sizeof(bool);

    /* GTM_TransactionInfo.gti_gxid */
    rc = memcpy_s(&(data->gti_gxid), sizeof(GlobalTransactionId), buf + len, sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_TransactionInfo.gti_state */
    rc = memcpy_s(&(data->gti_state), sizeof(GTM_TransactionStates), buf + len, sizeof(GTM_TransactionStates));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_TransactionStates);

    /* GTM_TransactionInfo.gti_coordname */
    {
        uint32 ll;

        rc = memcpy_s(&ll, sizeof(uint32), buf + len, sizeof(uint32));
        securec_check(rc, "\0", "\0");
        len += sizeof(uint32);
        if (ll > 0) {
            data->gti_coordname = genAllocTop(sizeof(ll + 1)); /* Should be allocated at TopMostContext */
            rc = memcpy_s(data->gti_coordname, ll, buf + len, ll);
            securec_check(rc, "\0", "\0");
            data->gti_coordname[ll] = 0;
        } else
            data->gti_coordname = NULL;
    }

    /* GTM_TransactionInfo.gti_xmin */
    rc = memcpy_s(&(data->gti_xmin), sizeof(GlobalTransactionId), buf + len, sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_TransactionInfo.gti_isolevel */
    rc = memcpy_s(&(data->gti_isolevel), sizeof(GTM_IsolationLevel), buf + len, sizeof(GTM_IsolationLevel));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_IsolationLevel);

    /* GTM_TransactionInfo.gti_readonly */
    rc = memcpy_s(&(data->gti_readonly), sizeof(bool), buf + len, sizeof(bool));
    securec_check(rc, "\0", "\0");
    len += sizeof(bool);

    /* GTM_TransactionInfo.gti_backend_id */
    rc = memcpy_s(&(data->gti_backend_id), sizeof(GTMProxy_ConnID), buf + len, sizeof(GTMProxy_ConnID));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTMProxy_ConnID);

    /* GTM_TransactionInfo.gti_nodestring */
    rc = memcpy_s(&string_len, sizeof(uint32), buf + len, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    len += sizeof(uint32);
    if (string_len > 0) {
        data->nodestring = (char*)genAllocTop(string_len + 1); /* Should allocate at TopMostMemoryContext */
        rc = memcpy_s(data->nodestring, string_len, buf + len, string_len);
        securec_check(rc, "\0", "\0");
        data->nodestring[string_len] = 0; /* null-terminated */
        len += string_len;
    } else {
        data->nodestring = NULL;
    }

    /* GTM_TransactionInfo.gti_gid */
    rc = memcpy_s(&string_len, sizeof(uint32), buf + len, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    len += sizeof(uint32);
    if (string_len > 0) {
        data->gti_gid = (char*)genAllocTop(string_len + 1); /* Should allocate at TopMostMemoryContext */
        rc = memcpy_s(data->gti_gid, string_len, buf + len, string_len);
        securec_check(rc, "\0", "\0");
        data->gti_gid[string_len] = 0; /* null-terminated */
        len += string_len;
    } else {
        data->gti_gid = NULL;
    }

    /* GTM_TransactionInfo.gti_current_snapshot */
    i = gtm_deserialize_snapshotdata(&(data->gti_current_snapshot), buf + len, sizeof(GTM_SnapshotData));
    len += i;

    /* GTM_TransactionInfo.gti_snapshot_set */
    rc = memcpy_s(&(data->gti_snapshot_set), sizeof(bool), buf + len, sizeof(bool));
    securec_check(rc, "\0", "\0");
    len += sizeof(bool);

    /* GTM_TransactionInfo.gti_lock would not be serialized. */

    /* GTM_TransactionInfo.gti_vacuum */
    rc = memcpy_s(&(data->gti_vacuum), sizeof(bool), buf + len, sizeof(bool));
    securec_check(rc, "\0", "\0");
    len += sizeof(bool);

    return len;
}

size_t gtm_get_transactions_size(GTM_Transactions* data)
{
    size_t len = 0;
    int i;

    len += sizeof(uint32);     /* gt_txn_count */
    len += sizeof(GTM_States); /* gt_gtm_state */

    /* NOTE: nothing to be done for gt_XidGenLock */

    len += sizeof(GlobalTransactionId); /* gt_nextXid */
    len += sizeof(GlobalTransactionId); /* gt_oldestXid */
    len += sizeof(GlobalTransactionId); /* gt_xidVacLimit */
    len += sizeof(GlobalTransactionId); /* gt_xidWarnLimit */
    len += sizeof(GlobalTransactionId); /* gt_xidStopLimit */
    len += sizeof(GlobalTransactionId); /* gt_xidWrapLimit */

    len += sizeof(GlobalTransactionId); /* gt_latestCompletedXid */
    len += sizeof(GlobalTransactionId); /* gt_recent_global_xmin */

    len += sizeof(int32); /* gt_lastslot */

    len += sizeof(int32); /* txn_count */

    for (i = 0; i < GTM_MAX_GLOBAL_TRANSACTIONS; i++) {
        len += sizeof(size_t); /* length */
        len += gtm_get_transactioninfo_size(&data->gt_transactions_array[i]);
    }

    /* NOTE: nothing to be done for gt_open_transactions */
    /* NOTE: nothing to be done for gt_TransArrayLock */

    return len;
}

/*
 * Return a number of serialized transactions.
 */
size_t gtm_serialize_transactions(GTM_Transactions* data, char* buf, size_t buflen)
{
    int len = 0;
    int i;
    uint32 txn_count;
    errno_t rc = 0;

    /* size check */
    if (gtm_get_transactions_size(data) > buflen) {
        return 0;
    }

    rc = memset_s(buf, buflen, 0, buflen);
    securec_check(rc, "\0", "\0");

    /* GTM_Transactions.gt_txn_count */
    rc = memcpy_s(buf + len, sizeof(uint32), &(data->gt_txn_count), sizeof(uint32));
    securec_check(rc, "\0", "\0");
    len += sizeof(uint32);

    /* GTM_Transactions.gt_gtm_state */
    rc = memcpy_s(buf + len, sizeof(GTM_States), &(data->gt_gtm_state), sizeof(GTM_States));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_States);

    /* NOTE: nothing to be done for gt_XidGenLock */

    /* GTM_Transactions.gt_nextXid */
    rc = memcpy_s(buf + len, sizeof(GlobalTransactionId), &(data->gt_nextXid), sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_Transactions.gt_oldestXid */
    rc = memcpy_s(buf + len, sizeof(GlobalTransactionId), &(data->gt_oldestXid), sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_Transactions.gt_xidVacLimit */
    rc = memcpy_s(buf + len, sizeof(GlobalTransactionId), &(data->gt_xidVacLimit), sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_Transactions.gt_xidWarnLimit */
    rc = memcpy_s(buf + len, sizeof(GlobalTransactionId), &(data->gt_xidWarnLimit), sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_Transactions.gt_xidStopLimit */
    rc = memcpy_s(buf + len, sizeof(GlobalTransactionId), &(data->gt_xidStopLimit), sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_Transactions.gt_xidWrapLimit */
    rc = memcpy_s(buf + len, sizeof(GlobalTransactionId), &(data->gt_xidWrapLimit), sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_Transactions.gt_latestCompletedXid */
    rc = memcpy_s(buf + len, sizeof(GlobalTransactionId), &(data->gt_latestCompletedXid), sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_Transactions.gt_recent_global_xmin */
    rc = memcpy_s(buf + len, sizeof(GlobalTransactionId), &(data->gt_recent_global_xmin), sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_Transactions.gt_lastslot */
    rc = memcpy_s(buf + len, sizeof(int32), &(data->gt_lastslot), sizeof(int32));
    securec_check(rc, "\0", "\0");
    len += sizeof(int32);

    /* Count up for valid transactions. */
    txn_count = 0;

    for (i = 0; i < GTM_MAX_GLOBAL_TRANSACTIONS; i++) {
        /* Select a used slot with the transaction array. */
        if (data->gt_transactions_array[i].gti_in_use == TRUE) {
            txn_count++;
        }
    }

    rc = memcpy_s(buf + len, sizeof(int32), &txn_count, sizeof(int32));
    securec_check(rc, "\0", "\0");
    len += sizeof(int32);

    /*
     * GTM_Transactions.gt_transactions_array
     */
    for (i = 0; i < GTM_MAX_GLOBAL_TRANSACTIONS; i++) {
        char* buf2;
        size_t buflen2, len2;

        /*
         * Not to include invalid global transactions.
         */
        if (data->gt_transactions_array[i].gti_in_use != TRUE) {
            continue;
        }

        buflen2 = gtm_get_transactioninfo_size(&data->gt_transactions_array[i]);

        /* store a length of following data. */
        rc = memcpy_s(buf + len, sizeof(size_t), &buflen2, sizeof(size_t));
        securec_check(rc, "\0", "\0");
        len += sizeof(size_t);

        buf2 = (char*)malloc(buflen2);

        len2 = gtm_serialize_transactioninfo(&data->gt_transactions_array[i], buf2, buflen2);

        /* store a serialized GTM_TransactionInfo structure. */
        rc = memcpy_s(buf + len, len2, buf2, len2);
        securec_check(rc, "\0", "\0");
        len += len2;

        free(buf2);
    }

    /* NOTE: nothing to be done for gt_TransArrayLock */
    return len;
}

/*
 * Return a number of deserialized transactions.
 */
size_t gtm_deserialize_transactions(GTM_Transactions* data, const char* buf, size_t maxlen)
{
    int len = 0;
    int i;
    uint32 txn_count;
    errno_t rc = 0;

    /* GTM_Transactions.gt_txn_count */
    rc = memcpy_s(&(data->gt_txn_count), sizeof(uint32), buf + len, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    len += sizeof(uint32);

    /* GTM_Transactions.gt_gtm_state */
    rc = memcpy_s(&(data->gt_gtm_state), sizeof(GTM_States), buf + len, sizeof(GTM_States));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_States);

    /* NOTE: nothing to be done for gt_XidGenLock */

    /* GTM_Transactions.gt_nextXid */
    rc = memcpy_s(&(data->gt_nextXid), sizeof(GlobalTransactionId), buf + len, sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_Transactions.gt_oldestXid */
    rc = memcpy_s(&(data->gt_oldestXid), sizeof(GlobalTransactionId), buf + len, sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_Transactions.gt_xidVacLimit */
    rc = memcpy_s(&(data->gt_xidVacLimit), sizeof(GlobalTransactionId), buf + len, sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_Transactions.gt_xidWarnLimit */
    rc = memcpy_s(&(data->gt_xidWarnLimit), sizeof(GlobalTransactionId), buf + len, sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_Transactions.gt_xidStopLimit */
    rc = memcpy_s(&(data->gt_xidStopLimit), sizeof(GlobalTransactionId), buf + len, sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_Transactions.gt_xidWrapLimit */
    rc = memcpy_s(&(data->gt_xidWrapLimit), sizeof(GlobalTransactionId), buf + len, sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_Transactions.gt_latestCompletedXid */
    rc = memcpy_s(&(data->gt_latestCompletedXid), sizeof(GlobalTransactionId), buf + len, sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_Transactions.gt_recent_global_xmin */
    rc = memcpy_s(&(data->gt_recent_global_xmin), sizeof(GlobalTransactionId), buf + len, sizeof(GlobalTransactionId));
    securec_check(rc, "\0", "\0");
    len += sizeof(GlobalTransactionId);

    /* GTM_Transactions.gt_lastslot */
    rc = memcpy_s(&(data->gt_lastslot), sizeof(int32), buf + len, sizeof(int32));
    securec_check(rc, "\0", "\0");
    len += sizeof(int32);

    /* A number of valid transactions */
    rc = memcpy_s(&txn_count, sizeof(int32), buf + len, sizeof(int32));
    securec_check(rc, "\0", "\0");
    len += sizeof(int32);

    /* GTM_Transactions.gt_transactions_array */
    for (i = 0; i < txn_count; i++) {
        size_t buflen2, len2;

        /* read a length of following data. */
        rc = memcpy_s(&buflen2, sizeof(size_t), buf + len, sizeof(size_t));
        securec_check(rc, "\0", "\0");
        len += sizeof(size_t);

        /* reada serialized GTM_TransactionInfo structure. */
        len2 = gtm_deserialize_transactioninfo(&(data->gt_transactions_array[i]), buf + len, buflen2);

        len += len2;
    }

    /* NOTE: nothing to be done for gt_TransArrayLock */

    return txn_count;
}

/*
 * Return size of PGXC node information
 */
size_t gtm_get_pgxcnodeinfo_size(GTM_PGXCNodeInfo* data)
{
    size_t len = 0;

    len += sizeof(GTM_PGXCNodeType); /* type */

    len += sizeof(uint32);       /* proxy name length */
    if (data->proxyname != NULL) { /* proxy name */
        len += strlen(data->proxyname);
    }

    len += sizeof(GTM_PGXCNodePort); /* port */

    len += sizeof(uint32);      /* node name length */
    if (data->nodename != NULL) { /* node name */
        len += strlen(data->nodename);
    }

    len += sizeof(uint32);       /* ipaddress length */
    if (data->ipaddress != NULL) { /* ipaddress */
        len += strlen(data->ipaddress);
    }

    len += sizeof(uint32);        /* datafolder length */
    if (data->datafolder != NULL) { /* datafolder */
        len += strlen(data->datafolder);
    }

    len += sizeof(GTM_PGXCNodeStatus); /* status */

    return len;
}

/*
 * Return a serialize number of PGXC node information
 */
size_t gtm_serialize_pgxcnodeinfo(GTM_PGXCNodeInfo* data, char* buf, size_t buflen)
{
    size_t len = 0;
    uint32 len_wk;
    errno_t rc = 0;

    /* size check */
    if (gtm_get_pgxcnodeinfo_size(data) > buflen) {
        return 0;
    }

    rc = memset_s(buf, buflen, 0, buflen);
    securec_check(rc, "\0", "\0");

    /* GTM_PGXCNodeInfo.type */
    rc = memcpy_s(buf + len, sizeof(GTM_PGXCNodeType), &(data->type), sizeof(GTM_PGXCNodeType));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_PGXCNodeType);

    /* GTM_PGXCNodeInfo.nodename */
    if (data->nodename == NULL) {
        len_wk = 0;
    } else {
        len_wk = (uint32)strlen(data->nodename);
    }

    rc = memcpy_s(buf + len, sizeof(uint32), &len_wk, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    len += sizeof(uint32);
    if (len_wk > 0) {
        rc = memcpy_s(buf + len, len_wk, data->nodename, len_wk);
        securec_check(rc, "\0", "\0");
        len += len_wk;
    }

    /* GTM_PGXCNodeInfo.proxyname */
    if (data->proxyname == NULL) {
        len_wk = 0;
    } else {
        len_wk = (uint32)strlen(data->proxyname);
    }

    rc = memcpy_s(buf + len, sizeof(uint32), &len_wk, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    len += sizeof(uint32);
    if (len_wk > 0) {
        rc = memcpy_s(buf + len, len_wk, data->proxyname, len_wk);
        securec_check(rc, "\0", "\0");
        len += len_wk;
    }

    /* GTM_PGXCNodeInfo.port */
    rc = memcpy_s(buf + len, sizeof(GTM_PGXCNodePort), &(data->port), sizeof(GTM_PGXCNodePort));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_PGXCNodePort);

    /* GTM_PGXCNodeInfo.ipaddress */
    if (data->ipaddress == NULL) {
        len_wk = 0;
    } else {
        len_wk = (uint32)strlen(data->ipaddress);
    }

    rc = memcpy_s(buf + len, sizeof(uint32), &len_wk, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    len += sizeof(uint32);
    if (len_wk > 0) {
        rc = memcpy_s(buf + len, len_wk, data->ipaddress, len_wk);
        securec_check(rc, "\0", "\0");
        len += len_wk;
    }

    /* GTM_PGXCNodeInfo.datafolder */
    if (data->datafolder == NULL) {
        len_wk = 0;
    } else {
        len_wk = (uint32)strlen(data->datafolder);
    }

    rc = memcpy_s(buf + len, sizeof(uint32), &len_wk, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    len += sizeof(uint32);
    if (len_wk > 0) {
        rc = memcpy_s(buf + len, len_wk, data->datafolder, len_wk);
        securec_check(rc, "\0", "\0");
        len += len_wk;
    }

    /* GTM_PGXCNodeInfo.status */
    rc = memcpy_s(buf + len, sizeof(GTM_PGXCNodeStatus), &(data->status), sizeof(GTM_PGXCNodeStatus));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_PGXCNodeStatus);

    /* NOTE: nothing to be done for node_lock */
    return len;
}

/*
 * Return a deserialize number of PGXC node information
 */
size_t gtm_deserialize_pgxcnodeinfo(GTM_PGXCNodeInfo* data, const char* buf, size_t buflen)
{
    size_t len = 0;
    uint32 len_wk;
    errno_t rc = 0;

    /* GTM_PGXCNodeInfo.type */
    rc = memcpy_s(&(data->type), sizeof(GTM_PGXCNodeType), buf + len, sizeof(GTM_PGXCNodeType));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_PGXCNodeType);

    /* GTM_PGXCNodeInfo.nodename*/
    rc = memcpy_s(&len_wk, sizeof(uint32), buf + len, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    len += sizeof(uint32);
    if (len_wk == 0) {
        data->nodename = NULL;
    } else {
        /* PGXCTODO: free memory */
        data->nodename = (char*)genAlloc(len_wk + 1);
        rc = memcpy_s(data->nodename, (size_t)len_wk, buf + len, (size_t)len_wk);
        securec_check(rc, "\0", "\0");
        data->nodename[len_wk] = 0; /* null_terminate */
        len += len_wk;
    }

    /* GTM_PGXCNodeInfo.proxyname*/
    rc = memcpy_s(&len_wk, sizeof(uint32), buf + len, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    len += sizeof(uint32);
    if (len_wk == 0) {
        data->proxyname = NULL;
    } else {
        /* PGXCTODO: free memory */
        data->proxyname = (char*)genAlloc(len_wk + 1);
        rc = memcpy_s(data->proxyname, (size_t)len_wk, buf + len, (size_t)len_wk);
        securec_check(rc, "\0", "\0");
        data->proxyname[len_wk] = 0; /* null_terminate */
        len += len_wk;
    }

    /* GTM_PGXCNodeInfo.port */
    rc = memcpy_s(&(data->port), sizeof(GTM_PGXCNodePort), buf + len, sizeof(GTM_PGXCNodePort));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_PGXCNodePort);

    /* GTM_PGXCNodeInfo.ipaddress */
    rc = memcpy_s(&len_wk, sizeof(uint32), buf + len, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    len += sizeof(uint32);
    if (len_wk == 0) {
        data->ipaddress = NULL;
    } else {
        data->ipaddress = (char*)genAlloc(len_wk + 1);
        rc = memcpy_s(data->ipaddress, (size_t)len_wk, buf + len, (size_t)len_wk);
        securec_check(rc, "\0", "\0");
        data->ipaddress[len_wk] = 0; /* null_terminate */
        len += len_wk;
    }

    /* GTM_PGXCNodeInfo.datafolder */
    rc = memcpy_s(&len_wk, sizeof(uint32), buf + len, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    len += sizeof(uint32);
    if (len_wk == 0) {
        data->datafolder = NULL;
    } else {
        data->datafolder = (char*)genAlloc(len_wk + 1);
        rc = memcpy_s(data->datafolder, (size_t)len_wk, buf + len, (size_t)len_wk);
        securec_check(rc, "\0", "\0");
        data->datafolder[len_wk] = 0; /* null_terminate */
        len += len_wk;
    }

    /* GTM_PGXCNodeInfo.status */
    rc = memcpy_s(&(data->status), sizeof(GTM_PGXCNodeStatus), buf + len, sizeof(GTM_PGXCNodeStatus));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_PGXCNodeStatus);

    /* NOTE: nothing to be done for node_lock */

    return len;
}

/*
 * Return size of sequence information
 */
size_t gtm_get_sequence_size(GTM_SeqInfo* seq)
{
    size_t len = 0;

    len += sizeof(uint32);              /* gs_key.gsk_keylen */
    len += seq->gs_key->gsk_keylen;     /* gs_key.gsk_key */
    len += sizeof(GTM_SequenceKeyType); /* gs_key.gsk_type */
    len += sizeof(GTM_Sequence);        /* gs_value */
    len += sizeof(GTM_Sequence);        /* gs_init_value */
    len += sizeof(GTM_Sequence);        /* gs_last_value */
    len += sizeof(GTM_Sequence);        /* gs_increment_by */
    len += sizeof(GTM_Sequence);        /* gs_min_value */
    len += sizeof(GTM_Sequence);        /* gs_max_value */
    len += sizeof(bool);                /* gs_cycle */
    len += sizeof(bool);                /* gs_called */
    len += sizeof(uint32);              /* gs_ref_count */
    len += sizeof(uint32);              /* ge_state */

    return len;
}

/*
 * Return number of serialized sequence information
 */
size_t gtm_serialize_sequence(GTM_SeqInfo* s, char* buf, size_t buflen)
{
    size_t len = 0;
    errno_t rc = 0;

    /* size check */
    if (gtm_get_sequence_size(s) > buflen) {
        return 0;
    }

    rc = memset_s(buf, buflen, 0, buflen);
    securec_check(rc, "\0", "\0");

    rc = memcpy_s(buf + len, sizeof(uint32), &s->gs_key->gsk_keylen, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    len += sizeof(uint32); /* gs_key.gsk_keylen */

    rc = memcpy_s(buf + len, s->gs_key->gsk_keylen, s->gs_key->gsk_key, s->gs_key->gsk_keylen);
    securec_check(rc, "\0", "\0");
    len += s->gs_key->gsk_keylen; /* gs_key.gsk_key */

    rc = memcpy_s(buf + len, sizeof(GTM_SequenceKeyType), &s->gs_key->gsk_type, sizeof(GTM_SequenceKeyType));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_SequenceKeyType); /* gs_key.gsk_type */

    rc = memcpy_s(buf + len, sizeof(GTM_Sequence), &s->gs_value, sizeof(GTM_Sequence));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_Sequence); /* gs_value */

    rc = memcpy_s(buf + len, sizeof(GTM_Sequence), &s->gs_init_value, sizeof(GTM_Sequence));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_Sequence); /* gs_init_value */

    rc = memcpy_s(buf + len, sizeof(GTM_Sequence), &s->gs_last_value, sizeof(GTM_Sequence));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_Sequence); /* gs_last_value */

    rc = memcpy_s(buf + len, sizeof(GTM_Sequence), &s->gs_increment_by, sizeof(GTM_Sequence));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_Sequence); /* gs_increment_by */

    rc = memcpy_s(buf + len, sizeof(GTM_Sequence), &s->gs_min_value, sizeof(GTM_Sequence));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_Sequence); /* gs_min_value */

    rc = memcpy_s(buf + len, sizeof(GTM_Sequence), &s->gs_max_value, sizeof(GTM_Sequence));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_Sequence); /* gs_max_value */

    rc = memcpy_s(buf + len, sizeof(bool), &s->gs_cycle, sizeof(bool));
    securec_check(rc, "\0", "\0");
    len += sizeof(bool); /* gs_cycle */

    rc = memcpy_s(buf + len, sizeof(bool), &s->gs_called, sizeof(bool));
    securec_check(rc, "\0", "\0");
    len += sizeof(bool); /* gs_called */

    rc = memcpy_s(buf + len, sizeof(uint32), &s->gs_ref_count, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    len += sizeof(uint32); /* gs_ref_count */

    rc = memcpy_s(buf + len, sizeof(uint32), &s->gs_state, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    len += sizeof(uint32); /* gs_state */

    return len;
}

/*
 * Return number of deserialized sequence information
 */
size_t gtm_deserialize_sequence(GTM_SeqInfo* seq, const char* buf, size_t buflen)
{
    size_t len = 0;
    errno_t rc = 0;

    seq->gs_key = (GTM_SequenceKeyData*)genAlloc0(sizeof(GTM_SequenceKeyData));

    rc = memcpy_s(&seq->gs_key->gsk_keylen, sizeof(uint32), buf + len, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    len += sizeof(uint32); /* gs_key.gsk_keylen */

    seq->gs_key->gsk_key = (char*)genAlloc0(seq->gs_key->gsk_keylen + 1);
    rc = memcpy_s(seq->gs_key->gsk_key, seq->gs_key->gsk_keylen, buf + len, seq->gs_key->gsk_keylen);
    securec_check(rc, "\0", "\0");
    len += seq->gs_key->gsk_keylen; /* gs_key.gsk_key */

    rc = memcpy_s(&seq->gs_key->gsk_type, sizeof(GTM_SequenceKeyType), buf + len, sizeof(GTM_SequenceKeyType));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_SequenceKeyType); /* gs_key.gsk_type */

    rc = memcpy_s(&seq->gs_value, sizeof(GTM_Sequence), buf + len, sizeof(GTM_Sequence));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_Sequence); /* gs_value */

    rc = memcpy_s(&seq->gs_init_value, sizeof(GTM_Sequence), buf + len, sizeof(GTM_Sequence));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_Sequence); /* gs_init_value */

    rc = memcpy_s(&seq->gs_last_value, sizeof(GTM_Sequence), buf + len, sizeof(GTM_Sequence));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_Sequence); /* gs_last_value */

    rc = memcpy_s(&seq->gs_increment_by, sizeof(GTM_Sequence), buf + len, sizeof(GTM_Sequence));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_Sequence); /* gs_increment_by */

    rc = memcpy_s(&seq->gs_min_value, sizeof(GTM_Sequence), buf + len, sizeof(GTM_Sequence));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_Sequence); /* gs_min_value */

    rc = memcpy_s(&seq->gs_max_value, sizeof(GTM_Sequence), buf + len, sizeof(GTM_Sequence));
    securec_check(rc, "\0", "\0");
    len += sizeof(GTM_Sequence); /* gs_max_value */

    rc = memcpy_s(&seq->gs_cycle, sizeof(bool), sizeof(bool), buf + len, sizeof(bool));
    securec_check(rc, "\0", "\0");
    len += sizeof(bool); /* gs_cycle */

    rc = memcpy_s(&seq->gs_called, sizeof(bool), buf + len, sizeof(bool));
    securec_check(rc, "\0", "\0");
    len += sizeof(bool); /* gs_called */

    rc = memcpy_s(&seq->gs_ref_count, sizeof(uint32), buf + len, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    len += sizeof(uint32);

    rc = memcpy_s(&seq->gs_state, sizeof(uint32), buf + len, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    len += sizeof(uint32);

    return len;
}
