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

/* same as max CU size */
const static int RPC_MAX_MESSAGE_SIZE = 1024 * 1024 * 1024;

int standby_recv_timeout = 120;    /* 120 sec = default */
#define IP_LEN 64

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

    if (size < 0 || size > RPC_MAX_MESSAGE_SIZE) {
        errCode = REMOTE_READ_SIZE_ERROR;
        return errCode;
    }

    tnRet = memset_s(remoteReadConnInfo, MAXPGPATH, 0, MAXPGPATH);
    securec_check(tnRet, "\0", "\0");

    remoteHost = strtok_s(remoteAddress, ":", &tempToken);
    for (int i = 0; i < MAX_REPLNODE_NUM; i++) {
        conninfo = t_thrd.postmaster_cxt.ReplConnArray[i];
        if (conninfo == NULL) {
            continue;
        }
        if (strcmp(remoteHost, conninfo->remotehost) == 0) {
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
            remoteHost,
            remotePort,
            standby_recv_timeout);
    securec_check_ss(tnRet, "", "");
    conGet = RemoteReadGetConn(remoteReadConnInfo);
    if (conGet == NULL) {
        errCode = REMOTE_READ_RPC_ERROR;
        return errCode;
    }

    tnRet = snprintf_s(sqlCommands, MAX_PATH_LEN, MAX_PATH_LEN - 1,
        "SELECT gs_read_block_from_remote(%u, %u, %u, %d, %d, '%lu', %d, '%lu', true);",
        spcnode, dbnode, relnode, 0, colid, offset, size, lsn);
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

    tnRet = memcpy_s(cuData, len + 1, PQgetvalue(res, 0, 0), len);
    securec_check(tnRet, "\0", "\0");
    PQclear(res);
    res = NULL;
    PQfinish(conGet);
    conGet = NULL;

    return errCode;
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
int RemoteGetPage(char* remoteAddress, uint32 spcnode, uint32 dbnode, uint32 relnode, int2 bucketnode, uint2 opt,
    int32 forknum, uint32 blocknum, uint32 blocksize, uint64 lsn, char* pageData)
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
    char* remoteHost = NULL;
    int remotePort = 0;

    tnRet = memset_s(remoteReadConnInfo, MAXPGPATH, 0, MAXPGPATH);
    securec_check(tnRet, "\0", "\0");

    remoteHost = strtok_s(remoteAddress, ":", &tempToken);
    if (remoteHost == NULL) {
        errCode = REMOTE_READ_RPC_ERROR;
        return errCode;
    }
    for (int i = 0; i < MAX_REPLNODE_NUM; i++) {
        conninfo = t_thrd.postmaster_cxt.ReplConnArray[i];
        if (conninfo == NULL) {
            continue;
        }
        if (strcmp(remoteHost, conninfo->remotehost) == 0) {
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
            remoteHost,
            remotePort,
            standby_recv_timeout);
    securec_check_ss(tnRet, "", "");
    conGet = RemoteReadGetConn(remoteReadConnInfo);
    if (conGet == NULL) {
        errCode = REMOTE_READ_RPC_ERROR;
        return errCode;
    }

    tnRet = snprintf_s(sqlCommands, MAX_PATH_LEN, MAX_PATH_LEN - 1,
                       "SELECT gs_read_block_from_remote(%u, %u, %u, %d, %d, %d, '%lu', %u, '%lu', false);", spcnode,
                       dbnode, relnode, bucketnode, (int2)opt, forknum, blocknum, blocksize, lsn);

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
    if (len < 0 || (uint32)len > blocksize) {
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
