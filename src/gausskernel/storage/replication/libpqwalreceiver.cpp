/* -------------------------------------------------------------------------
 *
 * libpqwalreceiver.cpp
 *
 * This file contains the libpq-specific parts of walreceiver. It's
 * loaded as a dynamic module to avoid linking the main server binary with
 * libpq.
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2010-2012, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/replication/libpqwalreceiver.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/time.h>

#include "libpq/libpq-int.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "miscadmin.h"
#include "replication/walreceiver.h"
#include "replication/walsender_private.h"
#include "replication/libpqwalreceiver.h"
#include "storage/pmsignal.h"
#include "storage/proc.h"
#include "utils/guc.h"
#ifdef HAVE_NETINET_TCP_H
#include <netinet/tcp.h>
#endif
#include <arpa/inet.h>

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif
#ifdef HAVE_SYS_SELECT_H
#include <sys/select.h>
#endif

/* Prototypes for private functions */
static bool libpq_select(int timeout_ms);
static PGresult *libpqrcv_PQexec(const char *query);
static void ha_set_conn_channel(void);
static void ha_add_disconnect_count(void);

static void ha_set_port_to_remote(PGconn *dummy_conn, int ha_port);
static char *stringlist_to_identifierstr(PGconn *conn, List *strings);

extern void SetDataRcvDummyStandbySyncPercent(int percent);

#define AmWalReceiverForDummyStandby() \
    (t_thrd.walreceiver_cxt.AmWalReceiverForFailover && !t_thrd.walreceiver_cxt.AmWalReceiverForStandby)

#ifndef ENABLE_MULTIPLE_NODES
/*
 * Identify remote az should be same with local for a cascade standby.
 */
static void IdentifyRemoteAvailableZone(void)
{
    if (!t_thrd.xlog_cxt.is_cascade_standby) {
        return;
    }

    volatile WalRcvData* walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    int nRet = 0;
    PGresult* res = NULL;

    /* Send query and get available zone of the remote server. */
    res = libpqrcv_PQexec("IDENTIFY_AZ");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_STATUS),
                errmsg("could not receive the ongoing az infomation from "
                       "the remote server: %s",
                    PQerrorMessage(t_thrd.libwalreceiver_cxt.streamConn))));
    }

    /* check remote az */
    char remoteAZname[NAMEDATALEN];
    nRet = snprintf_s(remoteAZname, NAMEDATALEN, NAMEDATALEN -1, "%s", PQgetvalue(res, 0, 0));
    securec_check_ss(nRet, "", "");

    if (strcmp(remoteAZname, g_instance.attr.attr_storage.available_zone) != 0) {
        PQclear(res);

        SpinLockAcquire(&walrcv->mutex);
        walrcv->conn_errno = REPL_INFO_ERROR;
        SpinLockRelease(&walrcv->mutex);

        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("the remote available zone should be same with local, remote is %s, local is %s",
                        remoteAZname, g_instance.attr.attr_storage.available_zone)));
    }
    PQclear(res);
}
#endif

/*
 * Establish the connection to the primary server for XLOG streaming
 */
bool libpqrcv_connect_for_TLI(TimeLineID *timeLineID, char *conninfo)
{
    char conninfoRepl[MAXCONNINFO + 75] = {0};
    TimeLineID remoteTli;
    PGresult *res = NULL;
    int nRet = 0;

    /*
     * Connect using deliberately undocumented parameter: replication. The
     * database name is ignored by the server in replication mode, but specify
     * "replication" for .pgpass lookup.
     */
    if (!dummyStandbyMode) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("dummyStandbyMode should be true")));
    }
    nRet = snprintf_s(conninfoRepl, sizeof(conninfoRepl), sizeof(conninfoRepl) - 1,
                      "%s dbname=replication replication=true "
                      "fallback_application_name=dummystandby connect_timeout=%d enable_ce=1",
                      conninfo, u_sess->attr.attr_storage.wal_receiver_connect_timeout);
    securec_check_ss(nRet, "", "");

    ereport(LOG, (errmsg("Connecting to primary :%s", conninfo)));

    t_thrd.libwalreceiver_cxt.streamConn = PQconnectdb(conninfoRepl);
    if (PQstatus(t_thrd.libwalreceiver_cxt.streamConn) != CONNECTION_OK) {
        char *errMsg = PQerrorMessage(t_thrd.libwalreceiver_cxt.streamConn);
        char *subErrMsg = "can not accept connection in standby mode";

        ereport(LOG, (errmsg("wal receiver could not connect to the primary server,the connection info :%s : %s",
                             conninfo, PQerrorMessage(t_thrd.libwalreceiver_cxt.streamConn))));

        if (errMsg != NULL && strstr(errMsg, subErrMsg) != NULL) {
            clean_failover_host_conninfo_for_dummy();
        }

        PQfinish(t_thrd.libwalreceiver_cxt.streamConn);
        t_thrd.libwalreceiver_cxt.streamConn = NULL;

        return false;
    }

    ereport(LOG, (errmsg("Connected to primary :%s success.", conninfo)));

    /*
     * Get the system identifier and timeline ID as a DataRow message from the
     * primary server.
     */
    res = libpqrcv_PQexec("IDENTIFY_SYSTEM");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        ereport(LOG, (errmsg("could not receive database system identifier and timeline ID from "
                             "the primary server: %s",
                             PQerrorMessage(t_thrd.libwalreceiver_cxt.streamConn))));
        PQfinish(t_thrd.libwalreceiver_cxt.streamConn);
        t_thrd.libwalreceiver_cxt.streamConn = NULL;

        return false;
    }
    if (PQnfields(res) < 3 || PQntuples(res) != 1) {
        int ntuples = PQntuples(res);
        int nfields = PQnfields(res);

        PQclear(res);
        ereport(
            LOG,
            (errmsg("invalid response from primary server"),
             errdetail("Could not identify system: Got %d rows and %d fields, expected %d rows and %d or more fields.",
                       ntuples, nfields, 1, 3)));
        PQfinish(t_thrd.libwalreceiver_cxt.streamConn);
        t_thrd.libwalreceiver_cxt.streamConn = NULL;

        return false;
    }
    remoteTli = pg_strtoint32(PQgetvalue(res, 0, 1));

    /*
     * Confirm that the current timeline of the primary is the same as the
     * recovery target timeline.
     */
    PQclear(res);

    *timeLineID = t_thrd.xlog_cxt.ThisTimeLineID = remoteTli;

    libpqrcv_disconnect();

    return true;
}

#ifndef ENABLE_MULTIPLE_NODES
#define IS_PRIMARY_NORMAL(servermode) (servermode == PRIMARY_MODE)
#else
#define IS_PRIMARY_NORMAL(servermode) ((servermode == PRIMARY_MODE) || (servermode == NORMAL_MODE))
#endif

static bool CheckRemoteServerSharedStorage(ServerMode remoteMode, PGresult* res)
{
    if (IS_SHARED_STORAGE_PRIMARY_CLUSTER_STANDBY_MODE && !IS_SHARED_STORAGE_CASCADE_STANDBY_MODE) {
        if (!IS_PRIMARY_NORMAL(remoteMode)) {
            PQclear(res);
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                            errmsg("for standby the mode of the remote server must be primary, current is %s",
                            wal_get_role_string(remoteMode, true))));
            return false;
        }
    } else if (IS_SHARED_STORAGE_CASCADE_STANDBY_MODE) {
        if (remoteMode != MAIN_STANDBY_MODE) {
            PQclear(res);
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                            errmsg("for main-standby cluster standby the mode of the remote server must be "
                            "main standby, current is %s", wal_get_role_string(remoteMode, true))));
            return false;
        }
    } else if (IS_SHARED_STORAGE_STANDBY_CLUSTER_STANDBY_MODE) {
        if (!IS_PRIMARY_NORMAL(remoteMode) && remoteMode != MAIN_STANDBY_MODE) {
            PQclear(res);
            ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                            errmsg("for standby cluster standby the mode of the remote server must be "
                            "primary or main standby, current is %s", wal_get_role_string(remoteMode, true))));
            return false;
        }
    }
    return true;
}

void StartRemoteStreaming(const LibpqrcvConnectParam *options)
{
    Assert(t_thrd.libwalreceiver_cxt.streamConn != NULL);
    StringInfoData cmd;
    initStringInfo(&cmd);

    appendStringInfoString(&cmd, "START_REPLICATION");
    if (!t_thrd.walreceiver_cxt.AmWalReceiverForFailover && options->slotname != NULL) {
        appendStringInfo(&cmd, " SLOT \"%s\"", options->slotname);
    }

    if (options->logical) {
        appendStringInfo(&cmd, " LOGICAL");
    }

    appendStringInfo(&cmd, " %X/%X", (uint32)(options->startpoint >> 32), (uint32)(options->startpoint));
    if (options->logical) {
        appendStringInfoString(&cmd, " (");
        appendStringInfo(&cmd, "proto_version '%u'", options->protoVersion);
        char *pubnames_str =
            stringlist_to_identifierstr(t_thrd.libwalreceiver_cxt.streamConn, options->publicationNames);
        appendStringInfo(&cmd, ", publication_names %s",
            PQescapeLiteral(t_thrd.libwalreceiver_cxt.streamConn, pubnames_str, strlen(pubnames_str)));
        pfree(pubnames_str);

        if (options->binary && PQserverVersion(t_thrd.libwalreceiver_cxt.streamConn) >= 90204) {
            appendStringInfoString(&cmd, ", binary 'true'");
            ereport(DEBUG5, (errmsg("append binary true")));
        }

        appendStringInfoChar(&cmd, ')');
    }

    PGresult *res = libpqrcv_PQexec(cmd.data);
    pfree(cmd.data);
    if (PQresultStatus(res) != PGRES_COPY_BOTH) {
        PQclear(res);
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS), errmsg("could not start WAL streaming: %s",
                                                                PQerrorMessage(t_thrd.libwalreceiver_cxt.streamConn))));
    }
    PQclear(res);
}

void CreateRemoteReplicationSlot(XLogRecPtr startpoint, const char* slotname, bool isLogical)
{
    Assert(t_thrd.libwalreceiver_cxt.streamConn != NULL);
    char cmd[1024];
    int nRet = 0;

    if (isLogical) {
        nRet = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1, "CREATE_REPLICATION_SLOT \"%s\" LOGICAL pgoutput",
                          slotname);
    } else {
        nRet = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1, "CREATE_REPLICATION_SLOT \"%s\" PHYSICAL %X/%X", slotname,
                          (uint32)(startpoint >> 32), (uint32)(startpoint));
    }
    securec_check_ss(nRet, "", "");

    PGresult *res = libpqrcv_PQexec(cmd);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_STATUS), errmsg("could not create replication slot %s : %s", slotname,
                                                         PQerrorMessage(t_thrd.libwalreceiver_cxt.streamConn))));
    }
    PQclear(res);
}

/* checkRemote: don't check the result is checkRemote is false, just let remote do some init */
void IdentifyRemoteSystem(bool checkRemote)
{
    Assert(t_thrd.libwalreceiver_cxt.streamConn != NULL);
    char *remoteSysid = NULL;
    char localSysid[32] = {0};
    TimeLineID remoteTli;
    TimeLineID localTli;
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    PGresult *res = libpqrcv_PQexec("IDENTIFY_SYSTEM");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
            errmsg("could not receive database system identifier and timeline ID from "
            "the remote server: %s",
            PQerrorMessage(t_thrd.libwalreceiver_cxt.streamConn))));
    }
    if (PQnfields(res) != 4 || PQntuples(res) != 1) {
        int num_tuples = PQntuples(res);
        int num_fields = PQnfields(res);

        PQclear(res);
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS), errmsg("invalid response from remote server"),
            errdetail("Could not identify system: Got %d rows and %d fields, expected %d rows and %d or more fields.",
            num_tuples, num_fields, 1, 4)));
    }

    if (!checkRemote) {
        PQclear(res);
        return;
    }

    remoteSysid = PQgetvalue(res, 0, 0);
    remoteTli = pg_strtoint32(PQgetvalue(res, 0, 1));

    /*
     * Confirm that the system identifier of the primary is the same as ours.
     */
    int nRet = snprintf_s(localSysid, sizeof(localSysid), sizeof(localSysid) - 1, UINT64_FORMAT, GetSystemIdentifier());
    securec_check_ss(nRet, "", "");

    if (strcmp(remoteSysid, localSysid) != 0) {
        if (dummyStandbyMode) {
            /* delete local xlog. */
            ProcessWSRmXLog();
            if (g_instance.attr.attr_storage.enable_mix_replication) {
                while (true) {
                    if (!ws_dummy_data_writer_use_file) {
                        CloseWSDataFileOnDummyStandby();
                        break;
                    } else {
                        pg_usleep(100000); /* sleep 0.1 s */
                    }
                }
                ProcessWSRmData();
            }

            // only walreceiver set standby_sysid.datareceiver do not set again.
            int rc = memcpy_s(localSysid, sizeof(localSysid), remoteSysid, sizeof(localSysid));
            securec_check(rc, "", "");

            sync_system_identifier = strtoul(remoteSysid, 0, 10);

            ereport(LOG, (errmsg("DummyStandby system identifier differs between the primary"),
                errdetail("The primary's identifier is %s, the standby's identifier is %s.sync_system_identifier=%lu",
                remoteSysid, localSysid, sync_system_identifier)));
        } else {
            remoteSysid = pstrdup(remoteSysid);
            PQclear(res);
            /*
             * If the system id is different,
             * then set error message in WalRcv and rebuild reason in HaShmData.
             */
            SpinLockAcquire(&walrcv->mutex);
            if (AmWalReceiverForDummyStandby()) {
                walrcv->dummyStandbyConnectFailed = true;
            }
            SpinLockRelease(&walrcv->mutex);
            ha_set_rebuild_connerror(SYSTEMID_REBUILD, REPL_INFO_ERROR);
            if (!t_thrd.xlog_cxt.is_cascade_standby) {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("database system identifier differs between the primary and standby"),
                        errdetail("The primary's identifier is %s, the standby's identifier is %s.",
                            remoteSysid,
                            localSysid)));
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("database system identifier differs between the standby and cascade standby"),
                        errdetail("The standby's identifier is %s, the cascade standby's identifier is %s.",
                            remoteSysid,
                            localSysid)));
            }
        }
    }

    /*
     * Confirm that the current timeline of the primary is the same as the
     * recovery target timeline.
     */
    if (dummyStandbyMode) {
        localTli = remoteTli;
    } else {
        localTli = GetRecoveryTargetTLI();
    }
    PQclear(res);

    if (t_thrd.walreceiver_cxt.AmWalReceiverForFailover) {
        t_thrd.xlog_cxt.ThisTimeLineID = localTli;
    } else {
        if (remoteTli != localTli) {
            /*
             * If the timeline id different,
             * then set error message in WalRcv and rebuild reason in HaShmData.
             */
            ha_set_rebuild_connerror(TIMELINE_REBUILD, REPL_INFO_ERROR);
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                            errmsg("timeline %u of the primary does not match recovery target timeline %u", remoteTli,
                                   localTli)));
        }
        t_thrd.xlog_cxt.ThisTimeLineID = remoteTli;
    }
}

/* identify remote mode, should do this after connect success. */
ServerMode IdentifyRemoteMode()
{
    Assert(t_thrd.libwalreceiver_cxt.streamConn != NULL);
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    PGresult *res = libpqrcv_PQexec("IDENTIFY_MODE");
    ServerMode remoteMode = UNKNOWN_MODE;
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_STATUS), errmsg("could not receive the ongoing mode infomation from "
                                                         "the remote server: %s",
                                                         PQerrorMessage(t_thrd.libwalreceiver_cxt.streamConn))));
    }
    if (PQnfields(res) != 1 || PQntuples(res) != 1) {
        int num_tuples = PQntuples(res);
        int num_fields = PQnfields(res);

        PQclear(res);
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS), errmsg("invalid response from remote server"),
                        errdetail("Expected 1 tuple with 1 fields, got %d tuples with %d fields.", num_tuples,
                                  num_fields)));
    }
    remoteMode = (ServerMode)pg_strtoint32(PQgetvalue(res, 0, 0));
    if (walrcv->conn_target != REPCONNTARGET_PUBLICATION &&
        !t_thrd.walreceiver_cxt.AmWalReceiverForFailover &&
        (!IS_PRIMARY_NORMAL(remoteMode)) &&
        /* remoteMode of cascade standby is a standby */
        !t_thrd.xlog_cxt.is_cascade_standby && !IS_SHARED_STORAGE_MODE) {
        PQclear(res);

        if (dummyStandbyMode) {
            clean_failover_host_conninfo_for_dummy();
        }

        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("the mode of the remote server must be primary, current is %s",
                               wal_get_role_string(remoteMode, true))));
    }

    if (t_thrd.postmaster_cxt.HaShmData->is_cascade_standby && remoteMode != STANDBY_MODE &&
        !IS_SHARED_STORAGE_MODE) {
        PQclear(res);

        SpinLockAcquire(&walrcv->mutex);
        walrcv->conn_errno = REPL_INFO_ERROR;
        SpinLockRelease(&walrcv->mutex);

        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("for cascade standby the mode of the remote server must be standby, current is %s",
                        wal_get_role_string(remoteMode, true))));
    }

    if (IS_SHARED_STORAGE_MODE) {
        if (!CheckRemoteServerSharedStorage(remoteMode, res)) {
            return UNKNOWN_MODE;
        }
    }

    PQclear(res);
    return remoteMode;
}

/* identify remote version, should do this after connect success. */
static int32 IdentifyRemoteVersion()
{
    Assert(t_thrd.libwalreceiver_cxt.streamConn != NULL);
    const int versionFields = 3;
    uint32 remoteSversion;
    uint32 localSversion;
    char *remotePversion = NULL;
    char *localPversion = NULL;
    uint32 remoteTerm;
    uint32 localTerm;
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;

    PGresult *res = libpqrcv_PQexec("IDENTIFY_VERSION");
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("could not receive database system version and protocol version from "
                               "the remote server: %s",
                               PQerrorMessage(t_thrd.libwalreceiver_cxt.streamConn))));
    }
    if (PQnfields(res) != versionFields || PQntuples(res) != 1) {
        int ntuples = PQntuples(res);
        int nfields = PQnfields(res);

        PQclear(res);
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS), errmsg("invalid response from remote server"),
                        errdetail("Expected 1 tuple with 3 fields, got %d tuples with %d fields.", ntuples, nfields)));
    }
    remoteSversion = pg_strtoint32(PQgetvalue(res, 0, 0));
    localSversion = PG_VERSION_NUM;
    remotePversion = PQgetvalue(res, 0, 1);
    localPversion = pstrdup(PG_PROTOCOL_VERSION);
    remoteTerm = pg_strtoint32(PQgetvalue(res, 0, 2));
    localTerm = Max(g_instance.comm_cxt.localinfo_cxt.term_from_file, g_instance.comm_cxt.localinfo_cxt.term_from_xlog);
    ereport(LOG, (errmsg("remote term[%u], local term[%u]", remoteTerm, localTerm)));
    if (localPversion == NULL) {
        PQclear(res);
        if (t_thrd.role != APPLY_WORKER) {
            ha_set_rebuild_connerror(VERSION_REBUILD, REPL_INFO_ERROR);
        }
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_STATUS),
                 errmsg("could not get the local protocal version, make sure the PG_PROTOCOL_VERSION is defined")));
    }
    if (!IS_SHARED_STORAGE_STANDBY_CLUSTER_STANDBY_MODE) {
        if (walrcv->conn_target != REPCONNTARGET_DUMMYSTANDBY && (localTerm == 0 || localTerm > remoteTerm) &&
            !AM_HADR_WAL_RECEIVER) {
            PQclear(res);
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                errmsg("invalid local term or remote term smaller than local. remote term[%u], local term[%u]",
                    remoteTerm, localTerm)));
        }
    }

    /*
     * If the version of the remote server is not the same as the local's, then set error
     * message in WalRcv and rebuild reason in HaShmData
     */
    if (remoteSversion != localSversion || strncmp(remotePversion, localPversion, strlen(PG_PROTOCOL_VERSION)) != 0) {
        PQclear(res);
        if (t_thrd.role != APPLY_WORKER) {
            ha_set_rebuild_connerror(VERSION_REBUILD, REPL_INFO_ERROR);
        }

        if (remoteSversion != localSversion) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                            errmsg("database system version is different between the remote and local"),
                            errdetail("The remote's system version is %u, the local's system version is %u.",
                                      remoteSversion, localSversion)));
        } else {
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                            errmsg("the remote protocal version %s is not the same as the local protocal version %s.",
                                   remotePversion, localPversion)));
        }
    }

    PQclear(res);
    pfree_ext(localPversion);
    return remoteTerm;
}

static void SetWalSendTermChanged(void)
{
    for (int i = 0; i < g_instance.attr.attr_storage.max_wal_senders; i++) {
        volatile WalSnd *walsnd = &t_thrd.walsender_cxt.WalSndCtl->walsnds[i];

        if (walsnd->pid == 0)
            continue;

        SpinLockAcquire(&walsnd->mutex);
        walsnd->isTermChanged = true;
        SpinLockRelease(&walsnd->mutex);
    }
}

/*
 * Establish the connection to the primary server for XLOG streaming
 */
bool libpqrcv_connect(char *conninfo, XLogRecPtr *startpoint, char *slotname, int channel_identifier)
{
    char conninfoRepl[MAXCONNINFO + 75];
    PGresult *res = NULL;
    char cmd[1024];
    char *remoteRecCrc = NULL;
    pg_crc32 recCrc = 0;
    XLogRecPtr localRec;
    pg_crc32 localRecCrc = 0;
    uint32 remoteTerm;
    uint32 localTerm;
    ServerMode remoteMode = UNKNOWN_MODE;
    int haveXlog = 0;
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    int count = 0;
    int nRet = 0;
    errno_t rc = EOK;
    XLogRecPtr remoteMaxLsn = InvalidXLogRecPtr;
    pg_crc32 remoteMaxLsnCrc = 0;
    char *remoteMaxLsnStr = NULL;
    char *remoteMaxLsnCrcStr = NULL;
    uint32 hi, lo;

    /*
     * Connect using deliberately undocumented parameter: replication. The
     * database name is ignored by the server in replication mode, but specify
     * "replication" for .pgpass lookup.
     */
    if (dummyStandbyMode) {
        nRet = snprintf_s(conninfoRepl, sizeof(conninfoRepl), sizeof(conninfoRepl) - 1,
                          "%s dbname=replication replication=true "
                          "fallback_application_name=dummystandby "
                          "connect_timeout=%d",
                          conninfo, u_sess->attr.attr_storage.wal_receiver_connect_timeout);
#ifndef ENABLE_MULTIPLE_NODES
    } else if (AM_HADR_WAL_RECEIVER) {
#else
    } else if (AM_HADR_WAL_RECEIVER || AM_HADR_CN_WAL_RECEIVER) {
#endif
        char passwd[MAXPGPATH] = {'\0'};
        char username[MAXPGPATH] = {'\0'};
        GetPasswordForHadrStreamingReplication(username, passwd);
        nRet = snprintf_s(conninfoRepl, sizeof(conninfoRepl), sizeof(conninfoRepl) - 1,
                          "%s dbname=postgres replication=%s "
                          "fallback_application_name=hadr_%s "
                          "connect_timeout=%d user=%s password=%s",
                          conninfo, AM_HADR_WAL_RECEIVER ? "hadr_main_standby" : "hadr_standby_cn",
                          (u_sess->attr.attr_common.application_name &&
                           strlen(u_sess->attr.attr_common.application_name) > 0)
                                ? u_sess->attr.attr_common.application_name
                                : "walreceiver",
                          u_sess->attr.attr_storage.wal_receiver_connect_timeout, username, passwd);
        rc = memset_s(passwd, MAXPGPATH, 0, MAXPGPATH);
        securec_check(rc, "\0", "\0");
    } else if (IS_SHARED_STORAGE_STANDBY_CLUSTER_STANDBY_MODE) {
        nRet = snprintf_s(conninfoRepl, sizeof(conninfoRepl), sizeof(conninfoRepl) - 1,
                          "%s dbname=postgres replication=standby_cluster "
                          "fallback_application_name=%s_hass "
                          "connect_timeout=%d",
                          conninfo,
                          (u_sess->attr.attr_common.application_name &&
                           strlen(u_sess->attr.attr_common.application_name) > 0)
                                ? u_sess->attr.attr_common.application_name
                                : "walreceiver",
                          u_sess->attr.attr_storage.wal_receiver_connect_timeout);
    } else {
        nRet = snprintf_s(conninfoRepl, sizeof(conninfoRepl), sizeof(conninfoRepl) - 1,
                          "%s dbname=replication replication=true "
                          "fallback_application_name=%s "
                          "connect_timeout=%d",
                          conninfo,
                          (u_sess->attr.attr_common.application_name &&
                           strlen(u_sess->attr.attr_common.application_name) > 0)
                                ? u_sess->attr.attr_common.application_name
                                : "walreceiver",
                          u_sess->attr.attr_storage.wal_receiver_connect_timeout);
    }

    securec_check_ss(nRet, "", "");
#ifndef ENABLE_MULTIPLE_NODES
    if (AM_HADR_WAL_RECEIVER) {
#else
    if (AM_HADR_WAL_RECEIVER || AM_HADR_CN_WAL_RECEIVER) {
#endif
        char *tmp = NULL;
        char *tok = NULL;
        char printConnInfo[MAXCONNINFO] = {0};
        char* copyConnInfo = NULL;
        copyConnInfo = pstrdup(conninfoRepl);
        if (copyConnInfo == NULL) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_STATUS), errmsg("could not get walreceiver conncetion info.")));
        }
        tok = strtok_s(copyConnInfo, " ", &tmp);
        if (tok == NULL) {
            if (copyConnInfo != NULL) {
                rc = memset_s(copyConnInfo, sizeof(copyConnInfo), 0, sizeof(copyConnInfo));
                securec_check(rc, "\0", "\0");
                pfree_ext(copyConnInfo);
            }
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_STATUS), errmsg("could not parse walreceiver conncetion info string.")));
        }
        while (strncmp(tok, "password=", strlen("password=")) != 0) {
            nRet = strcat_s(printConnInfo, MAXCONNINFO - 1, tok);
            securec_check_ss(nRet, "", "");
            nRet = strcat_s(printConnInfo, MAXCONNINFO - 1, " ");
            securec_check_ss(nRet, "", "");
            tok = strtok_s(NULL, " ", &tmp);
        }
        ereport(LOG, (errmsg("Connecting to remote server :%s", printConnInfo)));
        if (copyConnInfo != NULL) {
            rc = memset_s(copyConnInfo, sizeof(copyConnInfo), 0, sizeof(copyConnInfo));
            securec_check(rc, "\0", "\0");
            pfree_ext(copyConnInfo);
        }
    } else {
        ereport(LOG, (errmsg("Connecting to remote server :%s", conninfoRepl)));
    }
retry:
    /* 1. try to connect to primary */
    t_thrd.libwalreceiver_cxt.streamConn = PQconnectdb(conninfoRepl);
    if (PQstatus(t_thrd.libwalreceiver_cxt.streamConn) != CONNECTION_OK) {
        /* If startupxlog shut down walreceiver, we need not to retry. */
        if (++count < u_sess->attr.attr_storage.wal_receiver_connect_retries && !WalRcvIsShutdown()) {
            ereport(
                LOG,
                (errmsg("retry: %d, walreceiver could not connect to the remote server,the connection info :%s : %s",
                        count, conninfo, PQerrorMessage(t_thrd.libwalreceiver_cxt.streamConn))));
            libpqrcv_disconnect();
            goto retry;
        }

        ha_set_rebuild_connerror(CONNECT_REBUILD, CHANNEL_ERROR);
        if (AmWalReceiverForDummyStandby()) {
            SpinLockAcquire(&walrcv->mutex);
            walrcv->dummyStandbyConnectFailed = true;
            SpinLockRelease(&walrcv->mutex);
        }
        ha_add_disconnect_count();
        ereport(WARNING, (errcode(ERRCODE_CONNECTION_TIMED_OUT),
            errmsg("walreceiver could not connect to the remote server,the connection info :%s : %s",
            conninfo, PQerrorMessage(t_thrd.libwalreceiver_cxt.streamConn))));
        libpqrcv_disconnect();
        ereport(ERROR, (errcode(ERRCODE_CONNECTION_TIMED_OUT),
                        errmsg("walreceiver could not connect and shutting down")));
    }

    ereport(LOG, (errmsg("Connected to remote server :%s success.", conninfo)));
#ifndef ENABLE_MULTIPLE_NODES
    if (AM_HADR_WAL_RECEIVER) {
#else
    if (AM_HADR_WAL_RECEIVER || AM_HADR_CN_WAL_RECEIVER) {
#endif
        rc = memset_s(conninfoRepl, MAXCONNINFO + 75, 0, MAXCONNINFO + 75);
        securec_check(rc, "\0", "\0");
    }

    /* 2. identify version */
    remoteTerm = IdentifyRemoteVersion();
    localTerm = Max(g_instance.comm_cxt.localinfo_cxt.term_from_file, g_instance.comm_cxt.localinfo_cxt.term_from_xlog);

    /* If connect to primary or standby (for failover), check remote role */
    if (!t_thrd.walreceiver_cxt.AmWalReceiverForFailover || t_thrd.walreceiver_cxt.AmWalReceiverForStandby) {
        /* Send query and get server mode of the remote server. */
        remoteMode = IdentifyRemoteMode();
        if (remoteMode == UNKNOWN_MODE) {
            return false;
        }
    }

#ifndef ENABLE_MULTIPLE_NODES
    if (t_thrd.xlog_cxt.is_cascade_standby && !t_thrd.postmaster_cxt.HaShmData->is_cross_region) {
        IdentifyRemoteAvailableZone();
    }
#endif

    if (IS_SHARED_STORAGE_STANDBY_CLUSTER_STANDBY_MODE) {
        if (walrcv->conn_target != REPCONNTARGET_DUMMYSTANDBY && (localTerm == 0 || localTerm > remoteTerm)) {
            PQclear(res);
            ha_set_rebuild_connerror(WALSEGMENT_REBUILD, REPL_INFO_ERROR);
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                errmsg("shared storage: invalid local term or remote term smaller than local."
                       "remote term[%u], local term[%u]", remoteTerm, localTerm)));
            return false;
        }
    }

    /*
     * Get the system identifier and timeline ID as a DataRow message from the
     * primary server.
     */
    IdentifyRemoteSystem(true);

    if (dummyStandbyMode) {
        char msgBuf[XLOG_READER_MAX_MSGLENTH] = {0};
        /* find the max lsn by xlog file, if i'm dummystandby or standby for failover */
        localRec = FindMaxLSN(t_thrd.proc_cxt.DataDir, msgBuf, XLOG_READER_MAX_MSGLENTH, &localRecCrc);
    } else {
        SpinLockAcquire(&walrcv->mutex);
        localRecCrc = walrcv->latestRecordCrc;
        localRec = walrcv->latestValidRecord;
        SpinLockRelease(&walrcv->mutex);

        ereport(LOG,
                (errmsg("local request lsn/crc [%X/%X, %u]", (uint32)(localRec >> 32), (uint32)localRec, localRecCrc)));

        if (!XRecOffIsValid(localRec)) {
            ereport(PANIC,
                    (errmsg("Invalid xlog offset at %X/%X. Please check xlog files or rebuild the primary/standby "
                            "relationship.",
                            (uint32)(localRec >> 32), (uint32)localRec)));
        }
    }

    /* if dummystandby has no xlog, dont check crc */
    if (!(dummyStandbyMode && XLogRecPtrIsInvalid(localRec))) {
        nRet = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1, "IDENTIFY_CONSISTENCE %X/%X", (uint32)(localRec >> 32),
                          (uint32)localRec);
        securec_check_ss(nRet, "", "");

        res = libpqrcv_PQexec(cmd);
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            PQclear(res);
            ha_set_rebuild_connerror(WALSEGMENT_REBUILD, REPL_INFO_ERROR);
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                            errmsg("failed to identify consistence at %X/%X: %s", (uint32)(localRec >> 32),
                                   (uint32)localRec, PQerrorMessage(t_thrd.libwalreceiver_cxt.streamConn))));
            return false;
        }

        /*
         * Indentify consistence is motified when importing cm-ha enhancement code.
         * To support greyupgrade, msg with 1 row of 2 and 3 cols is handled to
         * go through two different logics. Will remove later.
         */
        if ((PQnfields(res) != 3 && PQnfields(res) != 2) || PQntuples(res) != 1) {
            int ntuples = PQntuples(res);
            int nfields = PQnfields(res);

            PQclear(res);
            ha_set_rebuild_connerror(WALSEGMENT_REBUILD, REPL_INFO_ERROR);
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_STATUS), errmsg("invalid response from primary server"),
                     errdetail("Could not identify system: Got %d rows and %d fields, expected 1 row and 2 or 3 fields",
                               ntuples, nfields)));
            return false;
        }

        if (PQnfields(res) == 3) {
            /* col1: crc of standby rec. */
            remoteRecCrc = PQgetvalue(res, 0, 0);
            if (remoteRecCrc && sscanf_s(remoteRecCrc, "%8X", &recCrc) != 1) {
                PQclear(res);
                ha_set_rebuild_connerror(WALSEGMENT_REBUILD, REPL_INFO_ERROR);
                ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                                errmsg("could not parse remote record's crc, remoteRecCrc=%s recCrc=%u",
                                       (remoteRecCrc[0] != 0) ? remoteRecCrc : "NULL", (uint32)recCrc)));
                return false;
            }

            /* col2: max lsn of dummy standby. */
            remoteMaxLsnStr = PQgetvalue(res, 0, 1);
            if (sscanf_s(remoteMaxLsnStr, "%X/%X", &hi, &lo) != 2) {
                PQclear(res);
                ha_set_rebuild_connerror(WALSEGMENT_REBUILD, REPL_INFO_ERROR);
                ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS), errmsg("could not parse remoteMaxLsn")));
                return false;
            }
            remoteMaxLsn = ((uint64)hi << 32) | lo;

            /* col3: crc of max lsn of dummy standby. */
            remoteMaxLsnCrcStr = PQgetvalue(res, 0, 2);
            if (remoteMaxLsnCrcStr && sscanf_s(remoteMaxLsnCrcStr, "%8X", &remoteMaxLsnCrc) != 1) {
                PQclear(res);
                ha_set_rebuild_connerror(WALSEGMENT_REBUILD, REPL_INFO_ERROR);
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_STATUS),
                         errmsg("could not parse remote max record's crc, remoteMaxLsnCrc=%s maxLsnCrc=%u",
                                (remoteMaxLsnCrcStr[0] != 0) ? remoteMaxLsnCrcStr : "NULL", (uint32)remoteMaxLsnCrc)));
                return false;
            }

            PQclear(res);

            if (!t_thrd.walreceiver_cxt.AmWalReceiverForFailover) {
                /* including recCrc == 0, which means local has more xlog */
                if (recCrc != localRecCrc) {
                    /* dummy standby connect to primary */
                    if (dummyStandbyMode) {
                        if (recCrc == IGNORE_REC_CRC) {
                            ereport(LOG, (errmsg("receive ignore reccrc, rm xlog.")));
                            ProcessWSRmXLog();
                        } else {
                            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                                            errmsg("dummystandby's local request lsn[%X/%X] 's crc "
                                                   "mismatched with remote server"
                                                   "crc(local, remote):[%u,%u].",
                                                   (uint32)(localRec >> 32), (uint32)localRec, localRecCrc, recCrc)));
                        }
                    } else {
                        /*
                         * standby connect to primary
                         * Direct check Primary and Standby, trigger build.
                         */
                        ha_set_rebuild_connerror(WALSEGMENT_REBUILD, REPL_INFO_ERROR);
                        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                                        errmsg("standby's local request lsn[%X/%X] 's crc mismatched with remote server"
                                               "crc(local, remote):[%u,%u].",
                                               (uint32)(localRec >> 32), (uint32)localRec, localRecCrc, recCrc)));
                    }
                }
            } else if (t_thrd.walreceiver_cxt.AmWalReceiverForStandby) {
                /* standby connect to standby */
                bool crcvalid = false;
                /* local xlog must be more than(or equal to) remote, and last crc must be matched */
                if (XLByteLE(remoteMaxLsn, localRec) && 
                    remoteMaxLsnCrc == GetXlogRecordCrc(remoteMaxLsn, crcvalid, XLogPageRead, 0)) {
                    ereport(LOG, (errmsg("crc check on remote standby success, local standby "
                                         "will promote to primary")));
                    SetWalRcvDummyStandbySyncPercent(SYNC_DUMMY_STANDBY_END);
                    /* also set datareceiver to continue failover */
                    SetDataRcvDummyStandbySyncPercent(SYNC_DUMMY_STANDBY_END);
                    haveXlog = false;
                } else {
                    SpinLockAcquire(&walrcv->mutex);
                    walrcv->conn_errno = REPL_INFO_ERROR;
                    SpinLockRelease(&walrcv->mutex);
                    ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                                    errmsg("crc of %X/%X is different across remote and local standby, "
                                           "standby promote failed",
                                           (uint32)(remoteMaxLsn >> 32), (uint32)remoteMaxLsn)));
                }
            } else {
                /* standby connect to dummystandby */
                if (recCrc != 0 && recCrc != localRecCrc) {
                    /* FUTURE CASE::
                     * When standby failover, its xlog is not the same with secondary
                     * standby, walreceiver will ereport ERROR, or the standby
                     * promoting will hang, and if the primary is pending, cm server
                     * will not arbitrate the primary.
                     */
                    SpinLockAcquire(&walrcv->mutex);
                    if (AmWalReceiverForDummyStandby()) {
                        walrcv->dummyStandbyConnectFailed = true;
                    }
                    SpinLockRelease(&walrcv->mutex);
                    ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                                    errmsg("invalid crc on secondary standby, has xlog, standby promote failed, "
                                           "(local, remote) = (%u, %u) on %X/%X",
                                           localRecCrc, recCrc, (uint32)(localRec >> 32), (uint32)localRec)));
                } else if (recCrc == 0) {
                    bool crcvalid = false;

                    /*
                     *  Standby      ------>(lsn is 10)
                     *  DummyStandby --->(lsn is 5)
                     *  recCrc of lsn == 10 of dummy is 0
                     *  We should check crc of lsn is 5 of standby
                     */
                    if (XLByteEQ(remoteMaxLsn, InvalidXLogRecPtr) ||
                        (XLByteLT(remoteMaxLsn, localRec) &&
                         remoteMaxLsnCrc == GetXlogRecordCrc(remoteMaxLsn, crcvalid, XLogPageRead, 0))) {
                        ereport(LOG, (errmsg("invalid crc on secondary standby, no xlog, standby "
                                             "will promote primary")));
                        SetWalRcvDummyStandbySyncPercent(SYNC_DUMMY_STANDBY_END);
                        haveXlog = false;
                    } else {
                        SpinLockAcquire(&walrcv->mutex);
                        if (AmWalReceiverForDummyStandby()) {
                            walrcv->dummyStandbyConnectFailed = true;
                        }
                        SpinLockRelease(&walrcv->mutex);
                        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                                        errmsg("crc of %X/%X is different across dummy and standby, "
                                               "standby promote failed",
                                               (uint32)(remoteMaxLsn >> 32), (uint32)remoteMaxLsn)));
                    }
                }
            }
        } else {
            char *primary_reccrc = PQgetvalue(res, 0, 0);
            bool havexlog = pg_strtoint32(PQgetvalue(res, 0, 1));

            if (primary_reccrc && sscanf_s(primary_reccrc, "%8X", &recCrc) != 1) {
                PQclear(res);
                ha_set_rebuild_connerror(WALSEGMENT_REBUILD, REPL_INFO_ERROR);
                ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                                errmsg("could not parse primary record's crc,primary_reccrc=%s reccrc=%u",
                                       (primary_reccrc[0] != 0) ? primary_reccrc : "NULL", (uint32)recCrc)));
                return false;
            }

            PQclear(res);

            if (0 == recCrc && t_thrd.walreceiver_cxt.AmWalReceiverForFailover) {
                if (0 == havexlog) {
                    ereport(LOG, (errmsg("invalid crc on secondary standby, no xlog, standby "
                                         "will promoting primary")));
                    SetWalRcvDummyStandbySyncPercent(SYNC_DUMMY_STANDBY_END);
                    return true;
                } else {
                    /* FUTURE CASE::
                     * When standby failover, its xlog is not the same with secondary
                     * standby, walreceiver will ereport ERROR, or the standby
                     * promoting will hang, and if the primary is pending, cm server
                     * will not arbitrate the primary.
                     */
                    ereport(ERROR,
                            (errcode(ERRCODE_INVALID_STATUS), errmsg("invalid crc on secondary standby, has xlog, "
                                                                     "standby promote failed")));
                }
            } else if (recCrc != walrcv->latestRecordCrc) {
                ha_set_rebuild_connerror(WALSEGMENT_REBUILD, REPL_INFO_ERROR);
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_STATUS),
                         errmsg("standby_rec=%x/%x "
                                "standby latest record's crc %u and primary corresponding record's crc %u not matched",
                                (uint32)(walrcv->latestValidRecord >> 32), (uint32)walrcv->latestValidRecord,
                                walrcv->latestRecordCrc, recCrc)));
            }
        }
    }

    if (t_thrd.walreceiver_cxt.AmWalReceiverForFailover) {
        if (t_thrd.walreceiver_cxt.AmWalReceiverForStandby) {
            /* failover to standby */
            if (!haveXlog) {
                return true;
            }
        } else {
            /* failover to dummy */
            ha_set_port_to_remote(t_thrd.libwalreceiver_cxt.streamConn, channel_identifier);
            if (recCrc == 0 && !haveXlog) {
                return true;
            }
        }
    } else {
        ha_set_port_to_remote(t_thrd.libwalreceiver_cxt.streamConn, channel_identifier);
    }

    /* Create replication slot if need */
    if (AM_HADR_WAL_RECEIVER && slotname != NULL) {
        rc = strcat_s(slotname, NAMEDATALEN - 1, "_hadr");
        securec_check(rc, "", "");
    } else if (IS_SHARED_STORAGE_STANDBY_CLUSTER_STANDBY_MODE && slotname != NULL) {
        rc = strcat_s(slotname, NAMEDATALEN - 1, "_hass");
        securec_check(rc, "", "");
    }

    if (!t_thrd.walreceiver_cxt.AmWalReceiverForFailover && slotname != NULL) {
        CreateRemoteReplicationSlot(*startpoint, slotname, false);
    }

    /* Start streaming from the point requested by startup process */
    LibpqrcvConnectParam options;
    rc = memset_s(&options, sizeof(LibpqrcvConnectParam), 0, sizeof(LibpqrcvConnectParam));
    securec_check(rc, "", "");
    options.slotname = slotname;
    options.startpoint = *startpoint;
    options.logical = false;
    StartRemoteStreaming(&options);

    ereport(LOG,
            (errmsg("streaming replication successfully connected to primary, the connection is %s, start from %X/%X ",
                    conninfo, (uint32)(*startpoint >> 32), (uint32)(*startpoint))));

    if (IS_DISASTER_RECOVER_MODE && t_thrd.postmaster_cxt.HaShmData->is_hadr_main_standby && localTerm != remoteTerm) {
        SetWalSendTermChanged();
    }

    if (!t_thrd.walreceiver_cxt.AmWalReceiverForFailover) {
        SpinLockAcquire(&walrcv->mutex);
        walrcv->peer_role = remoteMode;
        SpinLockRelease(&walrcv->mutex);
    }

    volatile HaShmemData *hashmdata = t_thrd.postmaster_cxt.HaShmData;
    SpinLockAcquire(&hashmdata->mutex);
    hashmdata->disconnect_count[hashmdata->current_repl] = 0;
    hashmdata->prev_repl = hashmdata->current_repl;
    SpinLockRelease(&hashmdata->mutex);
    /*
     * If the streaming replication successfully connected to primary,
     * then clean the rebuild reason in HaShmData.
     */
    ha_set_rebuild_connerror(NONE_REBUILD, NONE_ERROR);

    /* Save the current connect channel info in WalRcv */
    ha_set_conn_channel();

    return true;
}

/*
 * Wait until we can read WAL stream, or timeout.
 *
 * Returns true if data has become available for reading, false if timed out
 * or interrupted by signal.
 *
 * This is based on pqSocketCheck.
 */
static bool libpq_select(int timeout_ms)
{
    int ret;

    Assert(t_thrd.libwalreceiver_cxt.streamConn != NULL);
    if (PQsocket(t_thrd.libwalreceiver_cxt.streamConn) < 0) {
        ereport(ERROR, (errcode_for_socket_access(), errmsg("socket not open")));
    }

    /* We use poll(2) if available, otherwise select(2) */
    {
#ifdef HAVE_POLL
        struct pollfd input_fd;

        input_fd.fd = PQsocket(t_thrd.libwalreceiver_cxt.streamConn);
        input_fd.events = POLLIN | POLLERR;
        input_fd.revents = 0;

        ret = poll(&input_fd, 1, timeout_ms);
#else  /* !HAVE_POLL */

        fd_set input_mask;
        struct timeval timeout = { 0, 0 };
        struct timeval *ptr_timeout = NULL;

        FD_ZERO(&input_mask);
        FD_SET(PQsocket(t_thrd.libwalreceiver_cxt.streamConn), &input_mask);

        if (timeout_ms < 0) {
            ptr_timeout = NULL;
        } else {
            timeout.tv_sec = timeout_ms / 1000;
            timeout.tv_usec = (timeout_ms % 1000) * 1000;
            ptr_timeout = &timeout;
        }

        ret = select(PQsocket(t_thrd.libwalreceiver_cxt.streamConn) + 1, &input_mask, NULL, NULL, ptr_timeout);
#endif /* HAVE_POLL */
    }

    if (ret == 0 || (ret < 0 && errno == EINTR)) {
        return false;
    }
    if (ret < 0) {
        ereport(ERROR, (errcode_for_socket_access(), errmsg("select() failed: %m")));
    }
    return true;
}

/*
 * Send a query and wait for the results by using the asynchronous libpq
 * functions and the backend version of select().
 *
 * We must not use the regular blocking libpq functions like PQexec()
 * since they are uninterruptible by signals on some platforms, such as
 * Windows.
 *
 * We must also not use vanilla select() here since it cannot handle the
 * signal emulation layer on Windows.
 *
 * The function is modeled on PQexec() in libpq, but only implements
 * those parts that are in use in the walreceiver.
 *
 * Queries are always executed on the connection in streamConn.
 */
static PGresult *libpqrcv_PQexec(const char *query)
{
    PGresult *result = NULL;
    PGresult *lastResult = NULL;

    /*
     * PQexec() silently discards any prior query results on the connection.
     * This is not required for walreceiver since it's expected that walsender
     * won't generate any such junk results.
     */
    /*
     * Submit a query. Since we don't use non-blocking mode, this also can
     * block. But its risk is relatively small, so we ignore that for now.
     */
    if (!PQsendQuery(t_thrd.libwalreceiver_cxt.streamConn, query)) {
        return NULL;
    }

    for (;;) {
        /*
         * Receive data until PQgetResult is ready to get the result without
         * blocking.
         */
        while (PQisBusy(t_thrd.libwalreceiver_cxt.streamConn)) {
            /*
             * We don't need to break down the sleep into smaller increments,
             * and check for interrupts after each nap, since we can just
             * elog(FATAL) within SIGTERM signal handler if the signal arrives
             * in the middle of establishment of replication connection.
             */
            if (!libpq_select(-1)) {
                pqClearAsyncResult(t_thrd.libwalreceiver_cxt.streamConn);
                continue; /* interrupted */
            }
            if (PQconsumeInput(t_thrd.libwalreceiver_cxt.streamConn) == 0) {
                pqClearAsyncResult(t_thrd.libwalreceiver_cxt.streamConn);

                PQclear(lastResult);
                return NULL; /* trouble */
            }
        }

        /*
         * Emulate the PQexec()'s behavior of returning the last result when
         * there are many. Since walsender will never generate multiple
         * results, we skip the concatenation of error messages.
         */
        result = PQgetResult(t_thrd.libwalreceiver_cxt.streamConn);
        if (result == NULL) {
            break; /* query is complete */
        }

        PQclear(lastResult);
        lastResult = result;

        if (PQresultStatus(lastResult) == PGRES_COPY_IN || PQresultStatus(lastResult) == PGRES_COPY_OUT ||
            PQresultStatus(lastResult) == PGRES_COPY_BOTH ||
            PQstatus(t_thrd.libwalreceiver_cxt.streamConn) == CONNECTION_BAD) {
            break;
        }
    }

    return lastResult;
}

void libpqrcv_check_conninfo(const char *conninfo)
{
    PQconninfoOption *opts = PQconninfoParse(conninfo, NULL);
    if (opts == NULL) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("invalid connection string syntax")));
    }

    PQconninfoFree(opts);
}

/*
 * Disconnect connection to primary, if any.
 */
void libpqrcv_disconnect(void)
{
    PQfinish(t_thrd.libwalreceiver_cxt.streamConn);
    t_thrd.libwalreceiver_cxt.streamConn = NULL;
}

/*
 * Receive a message available from XLOG stream, blocking for
 * maximum of 'timeout' ms.
 *
 * Returns:
 *
 *	 True if data was received. *type, *buffer and *len are set to
 *	 the type of the received data, buffer holding it, and length,
 *	 respectively.
 *
 *	 False if no data was available within timeout, or wait was interrupted
 *	 by signal.
 *
 * The buffer returned is only valid until the next call of this function or
 * libpq_connect/disconnect.
 *
 * ereports on error.
 */
bool libpqrcv_receive(int timeout, unsigned char *type, char **buffer, int *len)
{
    int rawlen;
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;

    if (t_thrd.libwalreceiver_cxt.recvBuf != NULL) {
        PQfreemem(t_thrd.libwalreceiver_cxt.recvBuf);
    }
    t_thrd.libwalreceiver_cxt.recvBuf = NULL;

    /* Try to receive a CopyData message */
    rawlen = PQgetCopyData(t_thrd.libwalreceiver_cxt.streamConn, &t_thrd.libwalreceiver_cxt.recvBuf, 1);
    if (rawlen == 0) {
        /*
         * No data available yet. If the caller requested to block, wait for
         * more data to arrive.
         */
        if (timeout > 0) {
            if (!libpq_select(timeout)) {
                return false;
            }
        }

        if (PQconsumeInput(t_thrd.libwalreceiver_cxt.streamConn) == 0) {
            ereport(ERROR,
                    (errcode(ERRCODE_INVALID_STATUS), errmsg("could not receive data from WAL streaming: %s",
                                                             PQerrorMessage(t_thrd.libwalreceiver_cxt.streamConn))));
        }

        /* Now that we've consumed some input, try again */
        rawlen = PQgetCopyData(t_thrd.libwalreceiver_cxt.streamConn, &t_thrd.libwalreceiver_cxt.recvBuf, 1);
        if (rawlen == 0) {
            return false;
        }
    }
    if (rawlen == -1) { /* end-of-streaming or error */
        PGresult *res = NULL;
        const char *sqlstate = NULL;
        int retcode = 0;

        res = PQgetResult(t_thrd.libwalreceiver_cxt.streamConn);
        if (PQresultStatus(res) == PGRES_COMMAND_OK) {
            PQclear(res);
            ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                            errmsg("replication terminated by primary server at %X/%X",
                                   (uint32)(walrcv->receivedUpto >> 32), (uint32)walrcv->receivedUpto)));
            return false;
        }

        sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
        if (sqlstate && strlen(sqlstate) == 5) {
            retcode = MAKE_SQLSTATE(sqlstate[0], sqlstate[1], sqlstate[2], sqlstate[3], sqlstate[4]);
        }
        if (retcode == ERRCODE_UNDEFINED_FILE) {
            if (t_thrd.role != APPLY_WORKER) {
                ha_set_rebuild_connerror(WALSEGMENT_REBUILD, REPL_INFO_ERROR);
            }
            SpinLockAcquire(&walrcv->mutex);
            walrcv->ntries++;
            SpinLockRelease(&walrcv->mutex);
        }

        PQclear(res);
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS), errmsg("could not receive data from WAL stream: %s",
                                                                PQerrorMessage(t_thrd.libwalreceiver_cxt.streamConn))));
    }
    if (rawlen < -1) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS), errmsg("could not receive data from WAL stream: %s",
                                                                PQerrorMessage(t_thrd.libwalreceiver_cxt.streamConn))));
    }

    /* Return received messages to caller */
    *type = *((unsigned char *)t_thrd.libwalreceiver_cxt.recvBuf);
    if (IS_SHARED_STORAGE_MODE && !AM_HADR_WAL_RECEIVER && *type == 'w') {
        *len = 0;
        return false;
    }
    *buffer = t_thrd.libwalreceiver_cxt.recvBuf + sizeof(*type);
    *len = rawlen - sizeof(*type);

    return true;
}

/*
 * Send a message to XLOG stream.
 *
 * ereports on error.
 */
void libpqrcv_send(const char *buffer, int nbytes)
{
    if (PQputCopyData(t_thrd.libwalreceiver_cxt.streamConn, buffer, nbytes) <= 0 ||
        PQflush(t_thrd.libwalreceiver_cxt.streamConn))
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS), errmsg("could not send data to WAL stream: %s",
                                                                PQerrorMessage(t_thrd.libwalreceiver_cxt.streamConn))));
}

bool libpqrcv_command(const char *cmd, char **err, int *sqlstate)
{
    PGresult *res = libpqrcv_PQexec(cmd);

    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
        PQclear(res);
        *err = pstrdup(PQerrorMessage(t_thrd.libwalreceiver_cxt.streamConn));
        if (sqlstate != NULL && t_thrd.libwalreceiver_cxt.streamConn != NULL) {
            *sqlstate = MAKE_SQLSTATE(t_thrd.libwalreceiver_cxt.streamConn->last_sqlstate[0],
                                      t_thrd.libwalreceiver_cxt.streamConn->last_sqlstate[1],
                                      t_thrd.libwalreceiver_cxt.streamConn->last_sqlstate[2],
                                      t_thrd.libwalreceiver_cxt.streamConn->last_sqlstate[3],
                                      t_thrd.libwalreceiver_cxt.streamConn->last_sqlstate[4]);
        }
        return false;
    }

    PQclear(res);
    return true;
}

void HaSetRebuildRepInfoError(HaRebuildReason reason)
{
    if (NONE_REBUILD == reason) {
        ha_set_rebuild_connerror(NONE_REBUILD, NONE_ERROR);
    } else if (DCF_LOG_LOSS_REBUILD == reason) {
        ha_set_rebuild_connerror(DCF_LOG_LOSS_REBUILD, DCF_LOG_ERROR);
    } else {
        /* Assert the left is REPL_INFO_ERROR */
        ha_set_rebuild_connerror(reason, REPL_INFO_ERROR);
    }
}

void SetObsRebuildReason(HaRebuildReason reason)
{
    ha_set_rebuild_connerror(reason, REPL_INFO_ERROR);
}

/*
 * Get the current channel info from streamConn, then save it in WalRcv.
 */
static void ha_set_conn_channel()
{
    struct sockaddr *laddr = (struct sockaddr *)PQLocalSockaddr(t_thrd.libwalreceiver_cxt.streamConn);
    struct sockaddr *raddr = (struct sockaddr *)PQRemoteSockaddr(t_thrd.libwalreceiver_cxt.streamConn);
    char local_ip[IP_LEN] = {0};
    char remote_ip[IP_LEN] = {0};
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    errno_t rc = 0;

    char *result = NULL;

    if (laddr == NULL || raddr == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS),
                        errmsg("sockaddr is NULL, because there is no connection to primary")));
        return;
    }
    if (laddr->sa_family == AF_INET6) {
        result = inet_net_ntop(AF_INET6, &((struct sockaddr_in6 *)laddr)->sin6_addr, 128, local_ip, IP_LEN);
        if (result == NULL) {
            ereport(WARNING, (errmsg("inet_net_ntop failed, error: %d", EAFNOSUPPORT)));
        }
    } else if (laddr->sa_family == AF_INET) {
        result = inet_net_ntop(AF_INET, &((struct sockaddr_in *)laddr)->sin_addr, 32, local_ip, IP_LEN);
        if (result == NULL) {
            ereport(WARNING, (errmsg("inet_net_ntop failed, error: %d", EAFNOSUPPORT)));
        }
    }

    if (raddr->sa_family == AF_INET6) {
        result = inet_net_ntop(AF_INET6, &((struct sockaddr_in6 *)raddr)->sin6_addr, 128, remote_ip, IP_LEN);
        if (result == NULL) {
            ereport(WARNING, (errmsg("inet_net_ntop failed, error: %d", EAFNOSUPPORT)));
        }
    } else if (raddr->sa_family == AF_INET) {
        result = inet_net_ntop(AF_INET, &((struct sockaddr_in *)raddr)->sin_addr, 32, remote_ip, IP_LEN);
        if (result == NULL) {
            ereport(WARNING, (errmsg("inet_net_ntop failed, error: %d", EAFNOSUPPORT)));
        }
    }

    SpinLockAcquire(&walrcv->mutex);
    rc = strncpy_s((char *)walrcv->conn_channel.localhost, sizeof(walrcv->conn_channel.localhost), local_ip,
                   IP_LEN - 1);
    securec_check(rc, "", "");
    walrcv->conn_channel.localhost[IP_LEN - 1] = '\0';
    if (laddr->sa_family == AF_INET6) {
        walrcv->conn_channel.localport = ntohs(((struct sockaddr_in6 *)laddr)->sin6_port);
    } else if (laddr->sa_family == AF_INET) {
        walrcv->conn_channel.localport = ntohs(((struct sockaddr_in *)laddr)->sin_port);
    }

    rc = strncpy_s((char *)walrcv->conn_channel.remotehost, sizeof(walrcv->conn_channel.remotehost), remote_ip,
                   IP_LEN - 1);
    securec_check(rc, "", "");
    walrcv->conn_channel.remotehost[IP_LEN - 1] = '\0';
    if (raddr->sa_family == AF_INET6) {
        walrcv->conn_channel.remoteport = ntohs(((struct sockaddr_in6 *)raddr)->sin6_port);
    } else if (raddr->sa_family == AF_INET) {
        walrcv->conn_channel.remoteport = ntohs(((struct sockaddr_in *)raddr)->sin_port);
    }

    SpinLockRelease(&walrcv->mutex);
}

/* Add disconnect_count of the current repl */
static void ha_add_disconnect_count()
{
    volatile HaShmemData *hashmdata = t_thrd.postmaster_cxt.HaShmData;

    SpinLockAcquire(&hashmdata->mutex);
    hashmdata->disconnect_count[hashmdata->current_repl] += 1;
    SpinLockRelease(&hashmdata->mutex);
}

static void ha_set_port_to_remote(PGconn *dummy_conn, int ha_port)
{
    int nRet = 0;
    char cmd[64];
    PGresult *res = NULL;

    if (ha_port == 0 || dummy_conn == NULL) {
        ereport(WARNING, (errcode(ERRCODE_INVALID_STATUS), errmsg("could not set channel identifier, "
                                                                  "local port or connection is invalid")));
        return;
    }

    /*
     * using localport for channel identifier,
     * make primary can using ip and port to find out the channel
     */
    nRet = snprintf_s(cmd, sizeof(cmd), sizeof(cmd) - 1, "IDENTIFY_CHANNEL %d", ha_port);
    securec_check_ss(nRet, "", "");

    res = libpqrcv_PQexec(cmd);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        PQclear(res);
        ereport(ERROR, (errcode(ERRCODE_INVALID_STATUS), errmsg("could not set channel identifier, localport %d : %s",
                                                                ha_port, PQerrorMessage(dummy_conn))));
    }
    PQclear(res);
    return;
}

/*
 * Given a List of strings, return it as single comma separated
 * string, quoting identifiers as needed.
 *
 * This is essentially the reverse of SplitIdentifierString.
 *
 * The caller should free the result.
 */
static char *stringlist_to_identifierstr(PGconn *conn, List *strings)
{
    ListCell *lc;
    StringInfoData res;
    bool first = true;

    initStringInfo(&res);

    foreach (lc, strings) {
        char *val = strVal(lfirst(lc));

        if (first) {
            first = false;
        } else {
            appendStringInfoChar(&res, ',');
        }
        appendStringInfoString(&res, PQescapeIdentifier(conn, val, strlen(val)));
    }

    return res.data;
}
