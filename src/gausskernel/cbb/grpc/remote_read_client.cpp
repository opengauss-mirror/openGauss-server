/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
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
 * remote_read_clinet.cpp
 *     c++ code
 *     Don't include any of PG header file.
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/bbox/bbox_lib.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "securec.h"
#include "service/remote_read_client.h"
#include "catalog/catalog.h"
#include "knl/knl_variable.h"
#include "utils/elog.h"
#include "libpq/libpq-fe.h"
#include "libpq/libpq-int.h"
#include "replication/walreceiver.h"

#define atou64(x) ((uint64)strtoul((x), NULL, 0))
const int DEFAULT_WAIT_TIMES = 60;

/* same as max CU size */
const static int RPC_MAX_MESSAGE_SIZE = 1024 * 1024 * 1024;

const int standby_recv_timeout = 120;    /* 120 sec = default */
static int  MAX_IP_LEN = 64;  /* default ip len */

#define disconnect_and_return_null(tempconn) \
    do {                                     \
        if ((tempconn) != NULL) {            \
            PQfinish(tempconn);              \
            tempconn = NULL;                 \
        }                                    \
        tempconn = NULL;                     \
        return tempconn;                     \
    } while (0)

static PGconn* RemoteReadGetConn(const char* remoteReadConnInfo)
{
    PGconn* remoteReadConn = NULL;

    if (remoteReadConnInfo == NULL) {
        return NULL;
    }
    /* Connect server */
    remoteReadConn = PQconnectdb(remoteReadConnInfo);
    if ((remoteReadConn == NULL) || PQstatus(remoteReadConn) != CONNECTION_OK) {
        disconnect_and_return_null(remoteReadConn);
    }
    return remoteReadConn;
}

/*
 * @Description: remote read cu
 * @IN remote_address: remote address
 * @IN spcnode: tablespace id
 * @IN dbnode: database id
 * @IN relnode: relfilenode
 * @IN colid: column id
 * @IN offset: cu offset
 * @IN size: cu size
 * @IN lsn: current lsn
 * @OUT cu_data: pointer of cu data
 * @Return: remote read error code
 */
int RemoteGetCU(char* remoteAddress, uint32 spcnode, uint32 dbnode, uint32 relnode, int32 colid, uint64 offset,
    int32 size, uint64 lsn, char* cuData)
{
    PGconn* conGet = NULL;
    PGresult* res = NULL;
    int errCode = REMOTE_READ_OK;
    int len = 0;
    char remoteReadConnInfo[MAXPGPATH];
    char sqlCommands[MAX_PATH_LEN] = {0};
    int tnRet = 0;
    char* tempToken = NULL;
    struct replconninfo *conninfo = NULL;
    char* remoteHost;
    int remotePort = 0;
    char* ipNoZone = NULL;
    char ipNoZoneData[MAX_IP_LEN] = {0};

    if (size < 0 || size > RPC_MAX_MESSAGE_SIZE) {
        errCode = REMOTE_READ_SIZE_ERROR;
        return errCode;
    }

    tnRet = memset_s(remoteReadConnInfo, MAXPGPATH, 0, MAXPGPATH);
    securec_check(tnRet, "\0", "\0");

    remoteHost = strtok_s(remoteAddress, "@", &tempToken);
    for (int i = 0; i < MAX_REPLNODE_NUM; i++) {
        conninfo = t_thrd.postmaster_cxt.ReplConnArray[i];
        if (conninfo == NULL) {
            continue;
        }

        /* remove any '%zone' part from an IPv6 address string */
        ipNoZone = remove_ipv6_zone(conninfo->remotehost, ipNoZoneData, MAX_IP_LEN);

        if (strcmp(remoteHost, ipNoZone) == 0) {
            remotePort = conninfo->remoteport;
            break;
        }
    }

    tnRet = snprintf_s(remoteReadConnInfo,
            sizeof(remoteReadConnInfo),
            sizeof(remoteReadConnInfo) - 1,
            "localhost=%s localport=%d host=%s port=%d "
            "application_name=remote_read "
            "dbname=postgres "
            "connect_timeout=5 rw_timeout=%d",
            t_thrd.postmaster_cxt.ReplConnArray[1]->localhost,
            t_thrd.postmaster_cxt.ReplConnArray[1]->localport,
            (remotePort == 0) ? remoteHost : conninfo->remotehost,
            remotePort,
            standby_recv_timeout);
    securec_check_ss(tnRet, "", "");
    conGet = RemoteReadGetConn(remoteReadConnInfo);
    if (conGet == NULL) {
        errCode = REMOTE_READ_RPC_ERROR;
        return errCode;
    }

    tnRet = snprintf_s(sqlCommands, MAX_PATH_LEN, MAX_PATH_LEN - 1,
        "SELECT gs_read_block_from_remote(%u, %u, %u, %d, %d, %d, '%lu', %d, '%lu', true, %d);",
        spcnode, dbnode, relnode, 0, 0, colid, offset, size, lsn, DEFAULT_WAIT_TIMES);
    securec_check_ss(tnRet, "", "");

    res = PQexecParams(conGet, (const char*)sqlCommands, 0, NULL, NULL, NULL, NULL, 1);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        ereport(WARNING, (errmodule(MOD_REMOTE),
                            errmsg("could not get remote buffer: %s", PQresultErrorMessage(res))));
        errCode = REMOTE_READ_RPC_ERROR;
        PQclear(res);
        res = NULL;
        PQfinish(conGet);
        conGet = NULL;
        return errCode;
    }

    if (PQntuples(res) != 1 || PQgetisnull(res, 0, 0)) {
        ereport(WARNING, (errmodule(MOD_REMOTE),
                            errmsg("remote file get null: %s", PQresultErrorMessage(res))));
        errCode = REMOTE_READ_RPC_ERROR;
        PQclear(res);
        res = NULL;
        PQfinish(conGet);
        conGet = NULL;
        return errCode;
    }
    len = PQgetlength(res, 0, 0);
    if (len < 0 || (int32)len > size) {
        ereport(WARNING, (errmodule(MOD_REMOTE),
                            errmsg("remote request get incorrect length: %s", PQresultErrorMessage(res))));
        errCode = REMOTE_READ_SIZE_ERROR;
        PQclear(res);
        res = NULL;
        PQfinish(conGet);
        conGet = NULL;
        return errCode;
    }

    tnRet = memcpy_s(cuData, len, PQgetvalue(res, 0, 0), len);
    securec_check(tnRet, "\0", "\0");
    PQclear(res);
    res = NULL;
    PQfinish(conGet);
    conGet = NULL;

    return errCode;
}

int GetRemoteConnInfo(char* remoteAddress, char* remoteReadConnInfo, int len)
{
    int errCode = REMOTE_READ_OK;
    int tnRet = 0;
    char* tempToken = NULL;
    struct replconninfo *conninfo = NULL;
    char* remoteHost = NULL;
    int remotePort = 0;
    char* ipNoZone = NULL;
    char ipNoZoneData[MAX_IP_LEN] = {0};
    int timeout = standby_recv_timeout;

    tnRet = memset_s(remoteReadConnInfo, len, 0, len);
    securec_check(tnRet, "\0", "\0");

    remoteHost = strtok_s(remoteAddress, "@", &tempToken);
    if (remoteHost == NULL) {
        errCode = REMOTE_READ_RPC_ERROR;
        return errCode;
    }
    for (int i = 0; i < MAX_REPLNODE_NUM; i++) {
        conninfo = t_thrd.postmaster_cxt.ReplConnArray[i];
        if (conninfo == NULL) {
            continue;
        }

        /* remove any '%zone' part from an IPv6 address string */
        ipNoZone = remove_ipv6_zone(conninfo->remotehost, ipNoZoneData, MAX_IP_LEN);
        if (strcmp(remoteHost, ipNoZone) == 0) {
            remotePort = conninfo->remoteport;
            break;
        }
    }

    if (t_thrd.storage_cxt.timeoutRemoteOpera != 0) {
        timeout += t_thrd.storage_cxt.timeoutRemoteOpera;
    }

    tnRet = snprintf_s(remoteReadConnInfo, len, len - 1,
            "localhost=%s localport=%d host=%s port=%d "
            "application_name=remote_read dbname=postgres "
            "connect_timeout=5 rw_timeout=%d",
            t_thrd.postmaster_cxt.ReplConnArray[1]->localhost,
            t_thrd.postmaster_cxt.ReplConnArray[1]->localport,
            (remotePort == 0) ? remoteHost : conninfo->remotehost,
            remotePort,
            timeout);
    securec_check_ss(tnRet, "", "");
    return errCode;
}

/**
 * get connection
 * @param remoteAddress ip@port 
 * @return connection if success or else nullptr
 */
static PGconn* RemoteGetConnection(char* remoteAddress)
{
    char remoteReadConnInfo[MAXPGPATH];
    int errCode = GetRemoteConnInfo(remoteAddress, remoteReadConnInfo, MAXPGPATH);
    if (errCode != REMOTE_READ_OK) {
        return nullptr;
    }
    PGconn* conGet = RemoteReadGetConn(remoteReadConnInfo);
    if (conGet == nullptr) {
        errCode = REMOTE_READ_RPC_ERROR;
        return nullptr;
    }
    /* need to close by caller */
    return conGet;
}

uint64 RemoteGetXlogReplayPtr(char* remoteAddress)
{
    PGconn* conGet = RemoteGetConnection(remoteAddress);
    if (conGet == nullptr) {
        return InvalidXLogRecPtr;
    }
    PGresult* res = PQexec(conGet, "SELECT lsn::varchar from pg_last_xlog_replay_location()");
    if (PQresultStatus(res) != PGRES_TUPLES_OK || PQgetisnull(res, 0, 0)) {
        PQclear(res);
        res = nullptr;
        PQfinish(conGet);
        conGet = nullptr;
        return InvalidXLogRecPtr;
    }
    uint32 hi = 0;
    uint32 lo = 0;
    /* get remote lsn location */
    if (sscanf_s(PQgetvalue(res, 0, 0), "%X/%X", &hi, &lo) != 2) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("could not parse log location \"%s\"", PQgetvalue(res, 0, 0))));
    }
    PQclear(res);
    PQfinish(conGet);
    return (((uint64)hi) << 32) | lo;
}


/*
 * @Description: remote read page
 * @IN/OUT remote_address:remote address
 * @IN spcnode: tablespace id
 * @IN dbnode: database id
 * @IN relnode: relfilenode
 * @IN bucketnode: bucketnode
 * @IN opt: compressed table options
 * @IN/OUT forknum: forknum
 * @IN/OUT blocknum: block number
 * @IN/OUT blocksize: block size
 * @IN/OUT lsn: current lsn
 * @IN/OUT page_data: pointer of page data
 * @Return: remote read error code
 */
extern int RemoteGetPage(char* remoteAddress, RepairBlockKey *key, uint32 blocksize, uint64 lsn, char* pageData,
    const XLogPhyBlock *pblk, int timeout)
{
    PGconn* conGet = NULL;
    PGresult* res = NULL;
    int errCode = REMOTE_READ_OK;
    int len = 0;
    char remoteReadConnInfo[MAXPGPATH];
    char sqlCommands[MAX_PATH_LEN] = {0};
    int tnRet = 0;

    errCode = GetRemoteConnInfo(remoteAddress, remoteReadConnInfo, MAXPGPATH);
    if (errCode != REMOTE_READ_OK) {
        return errCode;
    }
    conGet = RemoteReadGetConn(remoteReadConnInfo);
    if (conGet == NULL) {
        errCode = REMOTE_READ_RPC_ERROR;
        return errCode;
    }

    if (pblk != NULL) {
        tnRet = snprintf_s(sqlCommands, MAX_PATH_LEN, MAX_PATH_LEN - 1,
            "SELECT pg_catalog.gs_read_segment_block_from_remote(%u, %u, %u, %d, %d, '%lu', %u, '%lu', %u, %u, %d);",
            key->relfilenode.spcNode, key->relfilenode.dbNode, key->relfilenode.relNode,
            key->relfilenode.bucketNode, key->forknum, key->blocknum, blocksize, lsn, pblk->relNode,
            pblk->block, timeout);
    } else {
        tnRet = snprintf_s(sqlCommands, MAX_PATH_LEN, MAX_PATH_LEN - 1,
            "SELECT gs_read_block_from_remote(%u, %u, %u, %d, %d, %d, '%lu', %u, '%lu', false, %d);",
            key->relfilenode.spcNode, key->relfilenode.dbNode, key->relfilenode.relNode,
            key->relfilenode.bucketNode, (int2)key->relfilenode.opt, key->forknum, key->blocknum,
            blocksize, lsn, timeout);
    }

    securec_check_ss(tnRet, "", "");

    res = PQexecParams(conGet, (const char*)sqlCommands, 0, NULL, NULL, NULL, NULL, 1);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        ereport(WARNING, (errmodule(MOD_REMOTE),
            errmsg("could not get remote buffer: %s", PQresultErrorMessage(res))));
        errCode = REMOTE_READ_RPC_ERROR;
        PQclear(res);
        res = NULL;
        PQfinish(conGet);
        conGet = NULL;
        return errCode;
    }

    if (PQgetisnull(res, 0, 0)) {
        ereport(WARNING, (errmodule(MOD_REMOTE),
            errmsg("remote get page, the executed res is null: %s", PQresultErrorMessage(res))));
        errCode = REMOTE_READ_RPC_ERROR;
        PQclear(res);
        res = NULL;
        PQfinish(conGet);
        conGet = NULL;
        return errCode;
    }

    len = PQgetlength(res, 0, 0);
    if (len < 0 || (uint32)len != blocksize) {
        ereport(WARNING, (errmodule(MOD_REMOTE),
                            errmsg("remote request get incorrect length: %s", PQresultErrorMessage(res))));
        errCode = REMOTE_READ_SIZE_ERROR;
        PQclear(res);
        res = NULL;
        PQfinish(conGet);
        conGet = NULL;
        return errCode;
    }

    tnRet = memcpy_s(pageData, len + 1, PQgetvalue(res, 0, 0), len);
    securec_check(tnRet, "\0", "\0");
    PQclear(res);
    res = NULL;
    PQfinish(conGet);
    conGet = NULL;

    return errCode;
}

int RemoteGetFileSize(char* remoteAddress, RemoteReadFileKey *key, uint64 lsn, int64 *size, int timeout)
{
    PGconn* conGet = NULL;
    PGresult* res = NULL;
    int errCode = REMOTE_READ_OK;
    char remoteReadConnInfo[MAXPGPATH];
    char sqlCommands[MAX_PATH_LEN] = {0};
    int tnRet = 0;

    errCode = GetRemoteConnInfo(remoteAddress, remoteReadConnInfo, MAXPGPATH);
    if (errCode != REMOTE_READ_OK) {
        return errCode;
    }
    conGet = RemoteReadGetConn(remoteReadConnInfo);
    if (conGet == NULL) {
        errCode = REMOTE_READ_CONN_ERROR;
        return errCode;
    }

    tnRet = snprintf_s(sqlCommands, MAX_PATH_LEN, MAX_PATH_LEN - 1,
        "SELECT gs_read_file_size_from_remote(%u, %u, %u, %d, %d, %d, '%lu', %d);",
        key->relfilenode.spcNode, key->relfilenode.dbNode, key->relfilenode.relNode, key->relfilenode.bucketNode,
        (int2)key->relfilenode.opt, key->forknum, lsn, timeout);
    securec_check_ss(tnRet, "", "");

    res = PQexecParams(conGet, (const char*)sqlCommands, 0, NULL, NULL, NULL, NULL, 0);
    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        ereport(WARNING, (errmodule(MOD_REMOTE),
                            errmsg("could not get remote file size: %s", PQresultErrorMessage(res))));
        errCode = REMOTE_READ_RPC_ERROR;
        PQclear(res);
        res = NULL;
        PQfinish(conGet);
        conGet = NULL;
        return errCode;
    }

    if (PQgetisnull(res, 0, 0)) {
        ereport(WARNING, (errmodule(MOD_REMOTE),
            errmsg("remote get file size, remote request return null: %s", PQresultErrorMessage(res))));
        errCode = REMOTE_READ_RPC_ERROR;
        PQclear(res);
        res = NULL;
        PQfinish(conGet);
        conGet = NULL;
        return errCode;
    }

    *size = atou64(PQgetvalue(res, 0, 0));
    if (*size >= 0 && (*size % BLCKSZ != 0)) {
        ereport(WARNING, (errmodule(MOD_REMOTE),
            errmsg("remote get file size, size is %lu, remote request get incorrect length: %s",
                *size, PQresultErrorMessage(res))));
        errCode = REMOTE_READ_SIZE_ERROR;
        PQclear(res);
        res = NULL;
        PQfinish(conGet);
        conGet = NULL;
        return errCode;
    }

    PQclear(res);
    res = NULL;
    PQfinish(conGet);
    conGet = NULL;

    return errCode;
}

const int CLOG_NODE = 1;
const int CSN_NODE = 2;
int RemoteGetFile(char* remoteAddress, RemoteReadFileKey* key, uint64 lsn, uint32 size, char* fileData,
    XLogRecPtr *remote_lsn, uint32 *remote_size, int timeout)
{
    PGconn* conGet = NULL;
    PGresult* res = NULL;
    int errCode = REMOTE_READ_OK;
    int32 len = 0;
    char remoteReadConnInfo[MAXPGPATH];
    char sqlCommands[MAX_PATH_LEN] = {0};
    int tnRet = 0;

    errCode = GetRemoteConnInfo(remoteAddress, remoteReadConnInfo, MAXPGPATH);
    if (errCode != REMOTE_READ_OK) {
        return errCode;
    }
    conGet = RemoteReadGetConn(remoteReadConnInfo);
    if (conGet == NULL) {
        errCode = REMOTE_READ_RPC_ERROR;
        return errCode;
    }

    tnRet = snprintf_s(sqlCommands, MAX_PATH_LEN, MAX_PATH_LEN - 1,
        "SELECT * from gs_read_file_from_remote(%u, %u, %u, %d, %d, %d, %d, %d, '%lu', %d);",
        key->relfilenode.spcNode, key->relfilenode.dbNode, key->relfilenode.relNode, key->relfilenode.bucketNode,
        (int2)key->relfilenode.opt, (int4)size, key->forknum, key->blockstart, lsn, timeout);
    securec_check_ss(tnRet, "", "");

    res = PQexecParams(conGet, (const char*)sqlCommands, 0, NULL, NULL, NULL, NULL, 1);

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
        ereport(WARNING, (errmodule(MOD_REMOTE),
                            errmsg("could not get remote file: %s", PQresultErrorMessage(res))));
        errCode = REMOTE_READ_RPC_ERROR;
        PQclear(res);
        res = NULL;
        PQfinish(conGet);
        conGet = NULL;
        return errCode;
    }

    if (PQgetisnull(res, 0, 0)) {
        ereport(WARNING, (errmodule(MOD_REMOTE),
                            errmsg("remote get file, remote request return null: %s", PQresultErrorMessage(res))));
        errCode = REMOTE_READ_RPC_ERROR;
        PQclear(res);
        res = NULL;
        PQfinish(conGet);
        conGet = NULL;
        return errCode;
    }

    len = PQgetlength(res, 0, 0);
    *remote_size = len;

    /* primary get the file from standby, the len need same with size obtained from the standby DN */
    if (!RecoveryInProgress() && len != (int)size && (key->relfilenode.spcNode != CLOG_NODE &&
            key->relfilenode.spcNode != CSN_NODE)) {
        ereport(WARNING, (errmodule(MOD_REMOTE),
            errmsg("remote request get incorrect length %u request size is %u : %s", len, size,
                PQresultErrorMessage(res))));
        errCode = REMOTE_READ_SIZE_ERROR;
        PQclear(res);
        res = NULL;
        PQfinish(conGet);
        conGet = NULL;
        return errCode;
    }

    if (len < 0 || (uint32)len > MAX_BATCH_READ_BLOCKNUM * BLCKSZ) {
        ereport(WARNING, (errmodule(MOD_REMOTE),
                            errmsg("remote request get incorrect length %u : %s", len, PQresultErrorMessage(res))));
        errCode = REMOTE_READ_SIZE_ERROR;
        PQclear(res);
        res = NULL;
        PQfinish(conGet);
        conGet = NULL;
        return errCode;
    }

    if (len != 0) {
        int copylen = len <= (int)size ? len : size;
        tnRet = memcpy_s(fileData, size, PQgetvalue(res, 0, 0), copylen);
        securec_check(tnRet, "\0", "\0");
    }

    if (RecoveryInProgress()) {
        tnRet = snprintf_s(sqlCommands, MAX_PATH_LEN, MAX_PATH_LEN - 1,
            "SELECT * from gs_current_xlog_insert_end_location();");
        securec_check_ss(tnRet, "", "");

        res = PQexecParams(conGet, (const char*)sqlCommands, 0, NULL, NULL, NULL, NULL, 0);
        if (PQresultStatus(res) != PGRES_TUPLES_OK || PQgetisnull(res, 0, 0)) {
            ereport(WARNING, (errmodule(MOD_REMOTE),
                                errmsg("could not get remote lsn or retrun null: %s", PQresultErrorMessage(res))));
            errCode = REMOTE_READ_RPC_ERROR;
            PQclear(res);
            res = NULL;
            PQfinish(conGet);
            conGet = NULL;
            return errCode;
        }

        uint32 hi = 0;
        uint32 lo = 0;
        /* get remote lsn location */
        if (sscanf_s(PQgetvalue(res, 0, 0), "%X/%X", &hi, &lo) != 2)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("could not parse log location \"%s\"", PQgetvalue(res, 0, 0))));

        *remote_lsn = (((uint64)hi) << 32) | lo;
    }
    PQclear(res);
    res = NULL;
    PQfinish(conGet);
    conGet = NULL;

    return errCode;
}
