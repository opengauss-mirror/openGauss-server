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
 * ---------------------------------------------------------------------------------------
 *
 * remote_read.cpp
 *         using simple C API interface
 *         Don't include any of RPC header file.
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/remote/remote_read.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "pgxc/pgxc.h"
#include "postmaster/postmaster.h"
#include "replication/walreceiver.h"
#include "replication/walsender.h"
#include "storage/remote_read.h"
#include "service/remote_read_client.h"
#include <arpa/inet.h>

/*
 * @Description: get remote error message
 * @IN error_code: remote read error code
 * @Return: remote read error message
 */
const char* RemoteReadErrMsg(int error_code)
{
    const char* error_msg = "";
    switch (error_code) {
        case REMOTE_READ_OK:
            error_msg = "normal";
            break;
        case REMOTE_READ_NEED_WAIT:
            error_msg = "local LSN larger than remote LSN and wait remote redo timeout";
            break;
        case REMOTE_READ_CRC_ERROR:
            error_msg = "remote data checksum error, maybe remote data corrupted";
            break;
        case REMOTE_READ_RPC_ERROR:
            error_msg = "rpc error";
            break;
        case REMOTE_READ_SIZE_ERROR:
            error_msg = "size error";
            break;
        case REMOTE_READ_IO_ERROR:
            error_msg = "io error";
            break;
        case REMOTE_READ_RPC_TIMEOUT:
            error_msg = "timeout";
            break;
        case REMOTE_READ_BLCKSZ_NOT_SAME:
            error_msg = "BLCKSZ different between remote and local";
            break;
        case REMOTE_READ_MEMCPY_ERROR:
            error_msg = "memcpy_s error";
            break;
        case REMOTE_READ_CONN_ERROR:
            error_msg = "remote connect status not ok";
            break;
        default:
            error_msg = "error code unknown";
            break;
    }

    return error_msg;
}

void GetPrimaryServiceAddress(char *address, size_t address_len)
{
    if (address == NULL || address_len == 0 || t_thrd.walreceiverfuncs_cxt.WalRcv == NULL)
        return;

    bool is_running = false;
    volatile WalRcvData *walrcv = t_thrd.walreceiverfuncs_cxt.WalRcv;
    int rc = 0;

    SpinLockAcquire(&walrcv->mutex);
    is_running = walrcv->isRuning;
    SpinLockRelease(&walrcv->mutex);

    if (walrcv->pid == 0 || !is_running)
        return;

    SpinLockAcquire(&walrcv->mutex);
    rc = snprintf_s(address, address_len, (address_len - 1), "%s@%d", walrcv->conn_channel.remotehost,
                    walrcv->conn_channel.remoteport);
    securec_check_ss(rc, "\0", "\0");
    SpinLockRelease(&walrcv->mutex);
}


static void FormatAddressByReplConn(replconninfo* replconninfo, char* remoteAddress, int addressLen)
{
    int rc = snprintf_s(remoteAddress, addressLen, (addressLen - 1), "%s@%d",
                        replconninfo->remotehost,
                        replconninfo->remoteport);
    securec_check_ss(rc, "", "");
}

/**
 * in startup, walsnder is not ready. we need to get remote address from replConnArray
 * @param firstAddress first address
 * @param secondAddress second address
 * @param addressLen address length
 */
static void GetRemoteReadAddressFromReplconn(char* firstAddress, char* secondAddress, size_t addressLen)
{
    XLogRecPtr fastest_replay = InvalidXLogRecPtr;
    XLogRecPtr second_fastest_replay = InvalidXLogRecPtr;
    int fastest = 0;
    int second_fastest = 0;
    for (int i = 0; i < MAX_REPLNODE_NUM; i++) {
        if (t_thrd.postmaster_cxt.ReplConnArray[i]) {
            char remoteAddress[MAXPGPATH];
            FormatAddressByReplConn(t_thrd.postmaster_cxt.ReplConnArray[i], remoteAddress, MAXPGPATH);
            XLogRecPtr insertXLogRecPtr = RemoteGetXlogReplayPtr(remoteAddress);
            if (XLByteLT(second_fastest_replay, insertXLogRecPtr)) {
                if (XLByteLT(fastest_replay, insertXLogRecPtr)) {
                    /* walsnd_replay is larger than fastest_replay */
                    second_fastest = fastest;
                    second_fastest_replay = fastest_replay;

                    fastest = i;
                    fastest_replay = insertXLogRecPtr;
                } else {
                    /* walsnd_replay is in the range (second_fastest_replay, fastest_replay] */
                    second_fastest = i;
                    second_fastest_replay = insertXLogRecPtr;
                }
            }
        }
    }

    /* find fastest replay standby */
    if (!XLogRecPtrIsInvalid(fastest_replay)) {
        FormatAddressByReplConn(t_thrd.postmaster_cxt.ReplConnArray[fastest], firstAddress, MAXPGPATH);

    }

    /* find second fastest replay standby */
    if (!XLogRecPtrIsInvalid(second_fastest_replay)) {
        FormatAddressByReplConn(t_thrd.postmaster_cxt.ReplConnArray[second_fastest], secondAddress, MAXPGPATH);
    }

}

/*
 * @Description: get remote address
 * @IN/OUT first_address: first address
 * @IN/OUT second_address: second_address
 * @IN address_len: address len
 */
void GetRemoteReadAddress(char* firstAddress, char* secondAddress, size_t addressLen)
{
    char ip[MAX_IPADDR_LEN] = {0};
    char port[MAX_IPADDR_LEN] = {0};
    errno_t rc = EOK;

    /* make sure first_address is correct */
    if (firstAddress == NULL || addressLen == 0)
        return;

    volatile HaShmemData* hashmdata = t_thrd.postmaster_cxt.HaShmData;
    ServerMode serverMode = hashmdata->current_mode;

    if (IS_DN_DUMMY_STANDYS_MODE()) {
        if (serverMode == PRIMARY_MODE && !IsPrimaryStandByReadyToRemoteRead()) {
            return;
        }

        if (t_thrd.postmaster_cxt.ReplConnArray[1]) {
            rc = snprintf_s(firstAddress, addressLen, (addressLen - 1),
                            "%s@%d", t_thrd.postmaster_cxt.ReplConnArray[1]->remotehost,
                            t_thrd.postmaster_cxt.ReplConnArray[1]->remoteport);
            securec_check_ss(rc, "", "");
        }
    } else if (IS_DN_MULTI_STANDYS_MODE()) {
        if (serverMode == PRIMARY_MODE) {
            /* address format: ip@port */
            if (RecoveryInProgress()) {
                /* during recovery, walsnder is not valid so we cant get standby info from walsnder */
                GetRemoteReadAddressFromReplconn(firstAddress, secondAddress, addressLen);
            } else {
                GetFastestReplayStandByServiceAddress(firstAddress, secondAddress, addressLen);
            }  
        } else if (serverMode == STANDBY_MODE) {
            GetPrimaryServiceAddress(firstAddress, addressLen);
            if (firstAddress[0] != '\0') {
                GetIPAndPort(firstAddress, ip, port, MAX_IPADDR_LEN);
                rc = snprintf_s(firstAddress, addressLen, (addressLen - 1), "%s@%s", ip, port);
                securec_check_ss(rc, "", "");
            }
        }
    } else {
        return;
    }
}

void GetIPAndPort(char* address, char* ip, char* port, size_t len)
{
    char* outerPtr = NULL;
    errno_t rc = EOK;
    char* tmpIp;
    char* tempPort;

    tmpIp = strtok_r(address, "@", &outerPtr);
    tempPort = strtok_r(NULL, "@", &outerPtr);
    if (tmpIp != NULL && tmpIp[0] != '\0' && tempPort != NULL && tempPort[0] != '\0' &&
        strlen(tmpIp) + strlen(tempPort) + 1 < len) {
        rc = strcpy_s(ip, MAX_IPADDR_LEN, tmpIp);
        securec_check(rc, "", "");
        rc = strcpy_s(port, MAX_IPADDR_LEN, tempPort);
        securec_check(rc, "", "");
    }
    return;
}

/*
 * @Description: have remote node to read
 * @Return: true if have remote node
 */
bool CanRemoteRead()
{
    volatile HaShmemData* hashmdata = t_thrd.postmaster_cxt.HaShmData;
    ServerMode serveMode = hashmdata->current_mode;

    if (IsRemoteReadModeOn() && !IS_DN_WITHOUT_STANDBYS_MODE() && IS_PGXC_DATANODE && serveMode != NORMAL_MODE &&
        serveMode != PENDING_MODE && serveMode != STANDBY_MODE) {
        return true;
    }
    return false;
}

/*
 * @Description: remote mode is on
 * @Return: true if emote read is on
 */
bool IsRemoteReadModeOn()
{
    return g_instance.attr.attr_storage.remote_read_mode != REMOTE_READ_OFF;
}

/*
 * @Description: set remote mode off and get old remote_read_mode
 * @Return: old remote_read_mode
 */
int SetRemoteReadModeOffAndGetOldMode()
{
    int oldRemoteRead = g_instance.attr.attr_storage.remote_read_mode;

    g_instance.attr.attr_storage.remote_read_mode = REMOTE_READ_OFF;
    return oldRemoteRead;
}

void SetRemoteReadMode(int mode)
{
    g_instance.attr.attr_storage.remote_read_mode = mode;

    return;
}
