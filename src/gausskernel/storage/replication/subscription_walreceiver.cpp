/*
 * Portions Copyright (c) Huawei Technologies Co., Ltd. 2021.
 * Portions Copyright (c) 2010-2012, PostgreSQL Global Development Group
 *
 * subscription_walreceiver.cpp
 *
 * Description: This file contains the subscription-specific parts of apply worker.
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/replication/subscription_walreceiver.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "libpq/libpq-int.h"
#include "mb/pg_wchar.h"
#include "replication/walreceiver.h"
#include "replication/subscription_walreceiver.h"

bool sub_connect(char *conninfo, XLogRecPtr *startpoint, char *appname, int channel_identifier)
{
    const char *keys[5];
    const char *vals[5];
    int i = 0;

    /*
     * We use the expand_dbname parameter to process the connection string (or
     * URI), and pass some extra options. The deliberately undocumented
     * parameter "replication=database" makes it a replication connection. The
     * database name is ignored by the server in replication mode.
     */
    keys[i] = "dbname";
    vals[i] = conninfo;
    keys[++i] = "replication";
    vals[i] = "database";
    keys[++i] = "fallback_application_name";
    vals[i] = "subscription";
    keys[++i] = "client_encoding";
    vals[i] = GetDatabaseEncodingName();
    keys[++i] = NULL;
    vals[i] = NULL;

    t_thrd.libwalreceiver_cxt.streamConn = PQconnectdbParams(keys, vals, true);
    if ((t_thrd.libwalreceiver_cxt.streamConn != NULL) && (t_thrd.libwalreceiver_cxt.streamConn->pgpass != NULL)) {
        /* clear password related memory to avoid leaks */
        int rc = memset_s(t_thrd.libwalreceiver_cxt.streamConn->pgpass,
            strlen(t_thrd.libwalreceiver_cxt.streamConn->pgpass),
            0, strlen(t_thrd.libwalreceiver_cxt.streamConn->pgpass));
        securec_check_c(rc, "\0", "\0");
    }
    if (PQstatus(t_thrd.libwalreceiver_cxt.streamConn) != CONNECTION_OK) {
        ereport(WARNING, (errcode(ERRCODE_CONNECTION_TIMED_OUT),
            errmsg("apply worker could not connect to the remote server")));
        PQfinish(t_thrd.libwalreceiver_cxt.streamConn);
        t_thrd.libwalreceiver_cxt.streamConn = NULL;
        return false;
    }

    ereport(LOG, (errmsg("Connected to remote server success.")));
    return true;
}

void sub_identify_system()
{
    return IdentifyRemoteSystem(false);
}
void sub_startstreaming(const LibpqrcvConnectParam *options)
{
    return StartRemoteStreaming(options);
}
void sub_create_slot(const LibpqrcvConnectParam *options)
{
    return CreateRemoteReplicationSlot(options->startpoint, options->slotname, options->logical);
}
