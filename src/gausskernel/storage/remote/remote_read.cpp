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
        default:
            error_msg = "error code unknown";
            break;
    }

    return error_msg;
}

/*
 * @Description: get remote address
 * @IN/OUT first_address: first address
 * @IN/OUT second_address: second_address
 * @IN address_len: address len
 */
void GetRemoteReadAddress(char* firstAddress, char* secondAddress, size_t addressLen)
{
    char remoteHostname[MAX_IPADDR_LEN] = {0};
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
            GetHostnamebyIP(t_thrd.postmaster_cxt.ReplConnArray[1]->remotehost, remoteHostname, MAX_IPADDR_LEN);
            rc = snprintf_s(firstAddress, addressLen, (addressLen - 1),
                            "%s:%d", remoteHostname,
                            t_thrd.postmaster_cxt.ReplConnArray[1]->remoteservice);
            securec_check_ss(rc, "", "");
        }
    } else if (IS_DN_MULTI_STANDYS_MODE()) {
        if (serverMode == PRIMARY_MODE) {
            GetFastestReplayStandByServiceAddress(firstAddress, secondAddress, addressLen);
            if (firstAddress[0] != '\0') {
                GetIPAndPort(firstAddress, ip, port, MAX_IPADDR_LEN);
                GetHostnamebyIP(ip, remoteHostname, MAX_IPADDR_LEN);
                rc = snprintf_s(firstAddress, addressLen, (addressLen - 1), "%s:%s", remoteHostname, port);
                securec_check_ss(rc, "", "");
            }

            if (secondAddress[0] != '\0') {
                GetIPAndPort(secondAddress, ip, port, MAX_IPADDR_LEN);
                GetHostnamebyIP(ip, remoteHostname, MAX_IPADDR_LEN);
                rc = snprintf_s(secondAddress, addressLen, (addressLen - 1), "%s:%s", remoteHostname, port);
                securec_check_ss(rc, "", "");
            }
        } else if (serverMode == STANDBY_MODE) {
            GetPrimaryServiceAddress(firstAddress, addressLen);
            if (firstAddress[0] != '\0') {
                GetIPAndPort(firstAddress, ip, port, MAX_IPADDR_LEN);
                GetHostnamebyIP(ip, remoteHostname, MAX_IPADDR_LEN);
                rc = snprintf_s(firstAddress, addressLen, (addressLen - 1), "%s:%s", remoteHostname, port);
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

    tmpIp = strtok_r(address, ":", &outerPtr);
    tempPort = strtok_r(NULL, ":", &outerPtr);
    if (tmpIp != NULL && tmpIp[0] != '\0' && tempPort != NULL && tempPort[0] != '\0' &&
        strlen(tmpIp) + strlen(tempPort) + 1 < len) {
        rc = strcpy_s(ip, MAX_IPADDR_LEN, tmpIp);
        securec_check(rc, "", "");
        rc = strcpy_s(port, MAX_IPADDR_LEN, tempPort);
        securec_check(rc, "", "");
    }
    return;
}

void GetHostnamebyIP(const char* ip, char* hostname, size_t hostnameLen)
{
    struct in_addr ipv4addr;
    struct hostent hstEnt;
    struct hostent* hp = NULL;
    char buff[MAX_PATH_LEN];
    int error = 0;
    errno_t rc = EOK;

    if (ip != NULL && ip[0] != '\0') {
        inet_pton(AF_INET, ip, &ipv4addr);
        int ret = gethostbyaddr_r((char*)&ipv4addr, sizeof(ipv4addr), AF_INET, &hstEnt, buff, MAX_PATH_LEN, &hp,
                                  &error);
        if (0 != ret || 0 != error) {
            ereport(WARNING, (errmodule(MOD_REMOTE), errmsg("Fail to find the remote host.")));
        } else {
            if (strlen(hstEnt.h_name) < hostnameLen) {
                rc = strcpy_s(hostname, MAX_IPADDR_LEN, hstEnt.h_name);
                securec_check(rc, "", "");
            }
        }
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
        serveMode != PENDING_MODE) {
        return true;
    }
    return false;
}

/*
 * @Description: check remote mode is auth
 * @Return: true if remote read is auth
 */
bool IsRemoteReadModeAuth()
{
    return (g_instance.attr.attr_storage.remote_read_mode == REMOTE_READ_AUTH);
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
