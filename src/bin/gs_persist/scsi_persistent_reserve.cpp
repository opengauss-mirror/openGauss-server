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
 * scsi_persistent_reserve.cpp
 *      init/reserve key
 *
 * IDENTIFICATION
 *    src/bin/gs_persist/scsi_persistent_reserve.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "scsi_protocol.h"
#include <stdint.h>
#include <endian.h>
#include <scsi/sg.h>
#include <sys/ioctl.h>
#include <string.h>
#include <stdlib.h>
#include <arpa/inet.h>

const uint8 RESERVATION_CONFLICT_STATUS_CODE = 0x18;
const uint32 SCSI_PR_CDB_LEN = 10;
const uint32 SCSI_MAX_KEY_NUM = 128;

static const uint32 MAX_SCSI_BUF_LEN = 8192;

static uint8 g_prKey[PR_KEY_LEN] = {0};

static const char *ReserveTypeName[] = {
    "unkown [0]",
    "Write Exclusive",
    "unkown [2]",
    "Exclusive Access",
    "unkown [4]",
    "Write Exclusive, registrants only",
    "Exclusive Access, registrants only",
    "Write Exclusive, all registrants",
    "Exclusive Access, all registrants",
    "unkown [9]",
    "unkown [10]",
    "unkown [11]",
    "unkown [12]",
    "unkown [13]",
    "unkown [14]",
    "unkown [15]",
};


int ScsiInitPrKey(const uint8 *key, uint32 len)
{
    if (len != PR_KEY_LEN) {
        printf(_("Invalid PR key len %u.\n"), len);
        return SISC_E_ERROR;
    }
    *((uint64_t *)g_prKey) = *((uint64_t *)key);
    printf(_("Set PR reserve key {%02x%02x%02x%02x%02x%02x%02x%02x}.\n"), key[0], key[1], key[2], key[3], key[4],
           key[5], key[6], key[7]);

    return SISC_E_SUCCESS;
}

int ScsiSetPRInCdb(uint8 *cdb, uint32 cdbLen, uint8 sAction, uint32 dataLen)
{
    ScsiPRInCdb *prInCdb = (ScsiPRInCdb *)cdb;

    if (cdbLen != sizeof(ScsiPRInCdb)) {
        printf(_("Invalid cdbLen %u.\n"), cdbLen);
        return SISC_E_ERROR;
    }

    errno_t rc = memset_s(cdb, sizeof(ScsiPRInCdb), 0, sizeof(ScsiPRInCdb));
    securec_check_c(rc, "\0", "\0");
    prInCdb->opCode = SC_PERSISTENT_RESERVE_IN;
    prInCdb->sAction = sAction;
    prInCdb->allocationLen = htons((uint16)dataLen);

    return SISC_E_SUCCESS;
}

int ScsiSetPROutCdb(uint8 *cdb, uint32 cdbLen, uint8 sAction, uint8 type, uint32 dataLen)
{
    ScsiPROutCdb *prOutCdb = (ScsiPROutCdb *)cdb;

    if (cdbLen != sizeof(ScsiPROutCdb)) {
        printf(_("Invalid cdbLen %u.\n"), cdbLen);
        return SISC_E_ERROR;
    }

    errno_t rc = memset_s(cdb, sizeof(ScsiPROutCdb), 0, sizeof(ScsiPROutCdb));
    securec_check_c(rc, "\0", "\0");
    prOutCdb->opCode = SC_PERSISTENT_RESERVE_OUT;
    prOutCdb->sAction = sAction;
    prOutCdb->type = type;
    prOutCdb->scope = 0;
    prOutCdb->parameterLen = htonl(dataLen);

    return SISC_E_SUCCESS;
}

int ScsiWrite(int fd, uint8 *buf, uint32 len, uint8 *cdb, uint32 cdbLen)
{
    struct sg_io_hdr sgIo;
    errno_t rc = EOK;
    int ret;

    rc = memset_s(&sgIo, sizeof(sgIo), 0, sizeof(sgIo));
    securec_check_c(rc, "\0", "\0");
    sgIo.cmdp = cdb;
    sgIo.cmd_len = cdbLen;
    sgIo.dxfer_direction = SG_DXFER_TO_DEV;

    ret = DbsScsiDoIoctl(fd, &sgIo, (char *)buf, len);
    if (ret != SISC_E_SUCCESS) {
        if (sgIo.status == RESERVATION_CONFLICT_STATUS_CODE) {
            printf(_("Write PR OUT cmd RESERVATION_CONFLICT.\n"));
            return SISC_E_RESERVATION_CONFLICT;
        }
        printf(_("Write PR OUT cmd failed.\n"));
        return ret;
    }

    return SISC_E_SUCCESS;
}

int ScsiRead(int fd, uint8 *buf, uint32 len, uint8 *cdb, uint32 cdbLen)
{
    struct sg_io_hdr sgIo;
    errno_t rc = EOK;
    int ret;

    rc = memset_s(&sgIo, sizeof(sgIo), 0, sizeof(sgIo));
    securec_check_c(rc, "\0", "\0");
    sgIo.cmdp = cdb;
    sgIo.cmd_len = cdbLen;
    sgIo.dxfer_direction = SG_DXFER_FROM_DEV;

    ret = DbsScsiDoIoctl(fd, &sgIo, (char *)buf, len);
    if (ret != SISC_E_SUCCESS) {
        printf(_("Read PR IN cmd failed.\n"));
        return ret;
    }

    return SISC_E_SUCCESS;
}

bool IsKeyExist(uint8 key[MAX_PR_KEY_NUM][PR_KEY_LEN], uint8 singleKey[PR_KEY_LEN], uint32 keyNum)
{
    uint32 i = 0;
    uint32 j = 0;

    for (i = 0; i < keyNum; ++i) {
        for (j = 0; j < PR_KEY_LEN; j++) {
            if (key[i][j] != singleKey[j]) {
                break;
            }
        }
        if (j == PR_KEY_LEN)
            break;
    }

    if (i == keyNum) {
        return false;
    } else {
        return true;
    }
}

int ScsiGetReserveInfo(int fd, bool *IsReserved)
{
    uint8 cdb[SCSI_PR_CDB_LEN];
    int ret;

    uint32 len = MAX_SCSI_BUF_LEN;
    uint8 *buf = (uint8 *)malloc(len);
    if (buf == NULL) {
        printf(_("Malloc buffer failed.\n"));
        return SISC_E_ERROR;
    }

    ret = ScsiSetPRInCdb(cdb, SCSI_PR_CDB_LEN, SCSI_PRIN_READ_RESERVATION, len);
    if (ret != SISC_E_SUCCESS) {
        free(buf);
        printf(_("Set PR IN cdb failed.\n"));
        return ret;
    }

    ret = ScsiRead(fd, buf, len, cdb, SCSI_PR_CDB_LEN);
    if (ret != SISC_E_SUCCESS) {
        free(buf);
        printf(_("Get reserved information failed.\n"));
        return ret;
    }

    ScsiPRInCtrl *ctrl = (ScsiPRInCtrl *)buf;
    uint32 addLen = ntohl(ctrl->additionalLen);
    printf(_("get reserved print gener:%u, addLen %u\n"), ntohl(ctrl->prGeneration), ntohl(ctrl->additionalLen));
    if (addLen / PR_INFO_LEN == 0) {
        printf(_("No PR reserve information.\n"));
        free(buf);
        if (IsReserved != NULL) {
            *IsReserved = false;
        }
        return SISC_E_SUCCESS;
    }
    
    printf(_("PR reserve information:\n"));
    uint8 *key = buf + sizeof(ScsiPRInCtrl);
    printf(_("\tkeys:{%02x%02x%02x%02x%02x%02x%02x%02x}.\n"), key[0], key[1], key[2], key[3], key[4], key[5], key[6],
        key[7]);
    const int typePos = 13;
    uint8 type = key[typePos];
    printf("\tscope: %d ", (type >> 4) & 0xf);
    printf(" type: %s\n", ReserveTypeName[(type & 0xf)]);
    free(buf);
    if (IsReserved != NULL) {
        *IsReserved = true;
    }
    return SISC_E_SUCCESS;
}

void CopyReadKey(uint8 dstKeys[MAX_PR_KEY_NUM][PR_KEY_LEN], uint32 *dstKeyNum, char *srcKeys, uint32 srcKeyNum)
{
    *dstKeyNum = 0;
    for (uint32 i = 0; i < srcKeyNum; ++i) {
        uint8 tmpKey[PR_KEY_LEN];
        errno_t rc = memcpy_s(tmpKey, sizeof(tmpKey), srcKeys + i * PR_KEY_LEN, PR_KEY_LEN);
        securec_check_c(rc, "\0", "\0");
        if (!IsKeyExist(dstKeys, tmpKey, *dstKeyNum)) {
            if (*dstKeyNum == MAX_PR_KEY_NUM) {
                printf(_("The number of reserve keys is greater than %u.\n"), MAX_PR_KEY_NUM);
                break;
            }
            errno_t rc = memcpy_s(dstKeys[*dstKeyNum], PR_KEY_LEN, tmpKey, sizeof(tmpKey));
            securec_check_c(rc, "\0", "\0");
            *dstKeyNum += 1;
        }
    }
}

int ScsiGetReserveKey(int fd, uint8 key[MAX_PR_KEY_NUM][PR_KEY_LEN], uint32 *keyNum)
{
    uint8 cdb[SCSI_PR_CDB_LEN];
    errno_t rc = EOK;

    uint8 *buf = (uint8 *)malloc(MAX_SCSI_BUF_LEN);
    if (buf == NULL) {
        printf(_("Malloc buffer failed.\n"));
        return SISC_E_ERROR;
    }

    int ret = ScsiSetPRInCdb(cdb, SCSI_PR_CDB_LEN, SCSI_PRIN_READ_KEYS, MAX_SCSI_BUF_LEN);
    if (ret != SISC_E_SUCCESS) {
        free(buf);
        printf(_("Set PR IN cdb failed.\n"));
        return ret;
    }

    ret = ScsiRead(fd, buf, MAX_SCSI_BUF_LEN, cdb, SCSI_PR_CDB_LEN);
    if (ret != SISC_E_SUCCESS) {
        free(buf);
        printf(_("Get reserve key failed.\n"));
        return ret;
    }

    // parse the key
    ScsiPRInCtrl *ctrl = (ScsiPRInCtrl *)buf;
    uint32 addLen = ntohl(ctrl->additionalLen);
    printf(_("get key print gener:%u, addLen %u"), ntohl(ctrl->prGeneration), ntohl(ctrl->additionalLen));
    if (addLen == 0) {
        printf(_("No PR reserve key.\n"));
        free(buf);
        rc = memset_s(key, PR_KEY_LEN, 0, PR_KEY_LEN);
        securec_check_c(rc, "\0", "\0");
        return SISC_E_SUCCESS;
    }
    if (addLen < PR_KEY_LEN) {
        printf(_("Invalid additionalLen %u.\n"), addLen);
        free(buf);
        return SISC_E_ERROR;
    }

    CopyReadKey(key, keyNum, (char *)(ctrl + 1), addLen / PR_KEY_LEN);

    free(buf);
    printf(_("Get PR reserve key :\n"));
    for (uint32 i = 0; i < *keyNum; ++i) {
        printf(_("{%02x%02x%02x%02x%02x%02x%02x%02x}.\n"), key[i][0], key[i][1], key[i][2], key[i][3], key[i][4],
               key[i][5], key[i][6], key[i][7]);
    }

    return SISC_E_SUCCESS;
}

int ScsiPrOutRegister(int fd, uint8 key[PR_KEY_LEN])
{
    uint8 cdb[SCSI_PR_CDB_LEN];
    int ret;
    uint32 len;
    ScsiPROutData data;
    errno_t rc = EOK;

    len = sizeof(ScsiPROutData);
    ret = ScsiSetPROutCdb(cdb, SCSI_PR_CDB_LEN, SCSI_PROUT_REGISTER_AND_IGNORE_EXISTING_KEY, 0, len);
    if (ret != SISC_E_SUCCESS) {
        printf(_("Set PR OUT cdb failed.\n"));
        return ret;
    }
    rc = memset_s(&data, sizeof(ScsiPROutData), 0, sizeof(ScsiPROutData));
    securec_check_c(rc, "\0", "\0");
    rc = memcpy_s(data.prActKey, sizeof(data.prActKey), g_prKey, PR_KEY_LEN);
    securec_check_c(rc, "\0", "\0");
    if (ret != 0) {
        printf(_("Set register PR key failed.\n"));
        return SISC_E_ERROR;
    }

    ret = ScsiWrite(fd, (uint8 *)&data, sizeof(ScsiPROutData), cdb, SCSI_PR_CDB_LEN);
    if (ret != SISC_E_SUCCESS) {
        printf(_("Register failed.\n"));
        return ret;
    }
    printf(_("Register suceess.\n"));

    return SISC_E_SUCCESS;
}

int ScsiPrOutClear(int fd)
{
    uint8 cdb[SCSI_PR_CDB_LEN];
    int ret;
    uint32 len;
    ScsiPROutData data;
    errno_t rc = EOK;

    bool isReserved = false;
    ret = ScsiGetReserveInfo(fd, &isReserved);
    if (ret != SISC_E_SUCCESS) {
        printf(_("Get PR reserve info failed.\n"));
        return ret;
    }

    if (!isReserved) {
        return SISC_E_SUCCESS;
    }

    ret = ScsiReserve(fd);
    if (ret != SISC_E_SUCCESS) {
        printf(_("reserve in clear failed.\n"));
        return ret;
    }
    
    len = sizeof(ScsiPROutData);
    ret = ScsiSetPROutCdb(cdb, SCSI_PR_CDB_LEN, SCSI_PROUT_CLEAR, 0, len);
    if (ret != SISC_E_SUCCESS) {
        printf(_("Set PR OUT cdb failed.\n"));
        return ret;
    }

    rc = memset_s(&data, sizeof(ScsiPROutData), 0, sizeof(ScsiPROutData));
    securec_check_c(rc, "\0", "\0");
    rc = memcpy_s(data.prKey, sizeof(data.prKey), g_prKey, PR_KEY_LEN);
    securec_check_c(rc, "\0", "\0");
    if (ret != 0) {
        printf(_("clear all PR key failed.\n"));
        return SISC_E_ERROR;
    }

    ret = ScsiWrite(fd, (uint8 *)&data, sizeof(ScsiPROutData), cdb, SCSI_PR_CDB_LEN);
    if (ret != SISC_E_SUCCESS) {
        printf(_("clear failed.\n"));
        return ret;
    }
    printf(_("clear success.\n"));

    return SISC_E_SUCCESS;
}

int ScsiPrOutReserve(int fd, uint8 key[PR_KEY_LEN])
{
    uint8 cdb[SCSI_PR_CDB_LEN];
    int ret;
    uint32 len;
    ScsiPROutData data;
    errno_t rc = EOK;

    len = sizeof(ScsiPROutData);
    ret = ScsiSetPROutCdb(cdb, SCSI_PR_CDB_LEN, SCSI_PROUT_RESERVE, SCSI_PR_WRITE_EXCL_RO, len);
    if (ret != SISC_E_SUCCESS) {
        printf(_("Set PR OUT cdb failed.\n"));
        return ret;
    }
    rc = memset_s(&data, sizeof(ScsiPROutData), 0, sizeof(ScsiPROutData));
    securec_check_c(rc, "\0", "\0");
    rc = memcpy_s(data.prKey, sizeof(data.prKey), g_prKey, PR_KEY_LEN);
    securec_check_c(rc, "\0", "\0");
    if (ret != 0) {
        printf(_("Set reserve PR key failed.\n"));
        return SISC_E_ERROR;
    }

    ret = ScsiWrite(fd, (uint8 *)&data, sizeof(ScsiPROutData), cdb, SCSI_PR_CDB_LEN);
    if (ret != SISC_E_SUCCESS) {
        printf(_("Reserve failed.\n"));
        return ret;
    }
    printf(_("Reserve success.\n"));

    return SISC_E_SUCCESS;
}

int ScsiPrOutPreempt(int fd, uint8 oldKey[PR_KEY_LEN], uint8 key[PR_KEY_LEN])
{
    uint8 cdb[SCSI_PR_CDB_LEN];
    int ret;
    uint32 len;
    ScsiPROutData data;
    errno_t rc = EOK;

    len = sizeof(ScsiPROutData);
    ret = ScsiSetPROutCdb(cdb, SCSI_PR_CDB_LEN, SCSI_PROUT_PREEMPT, SCSI_PR_WRITE_EXCL_RO, len);
    if (ret != SISC_E_SUCCESS) {
        printf(_("Set PR OUT cdb failed.\n"));
        return ret;
    }
    rc = memset_s(&data, sizeof(ScsiPROutData), 0, sizeof(ScsiPROutData));
    securec_check_c(rc, "\0", "\0");
    rc = memcpy_s(data.prKey, sizeof(data.prKey), key, PR_KEY_LEN);
    securec_check_c(rc, "\0", "\0");
    if (ret != 0) {
        printf(_("Set preempt PR key failed.\n"));
        return SISC_E_ERROR;
    }
    rc = memcpy_s(data.prActKey, sizeof(data.prActKey), oldKey, PR_KEY_LEN);
    securec_check_c(rc, "\0", "\0");
    if (ret != 0) {
        printf(_("Set preempt PR action key failed.\n"));
        return SISC_E_ERROR;
    }

    ret = ScsiWrite(fd, (uint8 *)&data, sizeof(ScsiPROutData), cdb, SCSI_PR_CDB_LEN);
    if (ret != SISC_E_SUCCESS) {
        printf(_("Preempt failed.\n"));
        return ret;
    }

    return SISC_E_SUCCESS;
}

int ScsciDoPreempt(int fd, uint8 oldKeys[MAX_PR_KEY_NUM][PR_KEY_LEN], uint32 keyNum, uint8 key[PR_KEY_LEN])
{
    for (uint32 i = 0; i < keyNum; ++i) {
        if (*((uint64_t *)oldKeys[i]) != *((uint64_t *)key)) {
            int ret = ScsiPrOutPreempt(fd, oldKeys[i], key);
            if (ret != SISC_E_RESERVATION_CONFLICT && ret != SISC_E_SUCCESS) {
                printf(_("PR OUT preempt failed -- Pr Out Preempt key failed.\n"));
                return ret;
            }
        }
    }
    uint8 newKeys[MAX_PR_KEY_NUM][PR_KEY_LEN];
    uint32 newKeyNum = 0;
    // get pre-reserved key
    int ret = ScsiGetReserveKey(fd, newKeys, &newKeyNum);
    if (ret != SISC_E_SUCCESS) {
        printf(_("PR OUT preempt failed -- get PR reserve key failed.\n"));
        return ret;
    }
    if (newKeyNum == 1 && *((uint64_t *)newKeys[0]) == *((uint64_t *)key)) {
        printf(_("PR OUT preempt success.\n"));
        return SISC_E_SUCCESS;
    }

    return SISC_E_ERROR;
}

int ScsiReserve(int fd)
{
    uint8 key[MAX_PR_KEY_NUM][PR_KEY_LEN];
    uint32 keyNum = 0;
    int ret;

    // get pre-reserved key
    ret = ScsiGetReserveKey(fd, key, &keyNum);
    if (ret != SISC_E_SUCCESS) {
        printf(_("Get PR reserve key failed.\n"));
        return ret;
    }

    // if there is no pre-reserved key, register and reserve the new key
    if (keyNum == 0) {
        ret = ScsiPrOutRegister(fd, g_prKey);
        if (ret != SISC_E_SUCCESS) {
            printf(_("PR OUT register failed.\n"));
            return ret;
        }
        ret = ScsiPrOutReserve(fd, g_prKey);
        if (ret != SISC_E_SUCCESS) {
            printf(_("PR OUT reserve failed.\n"));
            return ret;
        }
    } else if (keyNum == 1 && *((uint64_t *)key[0]) == *((uint64_t *)g_prKey)) {
        printf(_("Reserved by current host.\n"));
        ret = ScsiPrOutReserve(fd, g_prKey);
        if (ret != SISC_E_SUCCESS) {
            printf(_("PR OUT reserve failed.\n"));
            return ret;
        }
    } else {  // if there is pre-reserved key, register and preempt by new key
        ret = ScsiPrOutRegister(fd, g_prKey);
        if (ret != SISC_E_SUCCESS) {
            printf(_("PR OUT register failed.\n"));
            return ret;
        }
        ret = ScsciDoPreempt(fd, key, keyNum, g_prKey);
        if (ret != SISC_E_SUCCESS) {
            printf(_("PR OUT preempt failed.\n"));
            return ret;
        }
        ret = ScsiPrOutReserve(fd, g_prKey);
        if (ret != SISC_E_SUCCESS) {
            printf(_("PR OUT reserve failed.\n"));
            return ret;
        }
    }

    return SISC_E_SUCCESS;
}
