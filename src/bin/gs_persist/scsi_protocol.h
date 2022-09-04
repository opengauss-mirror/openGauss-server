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
 * scsi_protocol.h
 *
 *
 * IDENTIFICATION
 *    src/bin/gs_persist/scsi_protocol.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef SCSI_PROTOCOL_H
#define SCSI_PROTOCOL_H

#include <stddef.h>
#include <scsi/sg.h>
#include <stdint.h>
#include <stdio.h>
#include "securec.h"
#include "securec_check.h"

#define _(x) x

typedef unsigned char uint8;   /* == 8 bits */
typedef unsigned short uint16; /* == 16 bits */
typedef unsigned int uint32;   /* == 32 bits */

const uint32 PR_KEY_LEN = 8;
const uint32 MAX_PR_KEY_NUM = 10;
const uint32 PR_INFO_LEN = 16;


#pragma pack(push)
#pragma pack(1)
typedef struct {
    uint8 opcode;
    uint8 subOpcode;
    uint32 cmdSn;
    uint32 objId;
    uint32 transLen;
    uint8 ctrlCode;
    uint8 reserved;
} ScsiPrivateCdb;

typedef struct {
    uint8 opCode;
    uint8 sAction : 5;
    uint8 reserved1 : 3;
    uint8 reserved2[5];
    uint16 allocationLen;
    uint8 control;
} ScsiPRInCdb;

typedef struct {
    uint8 opCode;
    uint8 sAction : 5;
    uint8 reserved1 : 3;
    uint8 type : 4;
    uint8 scope : 4;
    uint8 reserved2[2];
    uint32 parameterLen;
    uint8 control;
} ScsiPROutCdb;

typedef struct {
    uint8 prKey[PR_KEY_LEN];
    uint8 prActKey[PR_KEY_LEN];
    uint32 obsolete1;

    uint8 aptpl : 1;
    uint8 reserved1 : 1;
    uint8 allTgPt : 1;
    uint8 specIPt : 1;
    uint8 reserved2 : 4;

    uint8 reserved3;

    uint8 obsolete2[2];
} ScsiPROutData;

#pragma pack(pop)

#define SC_PERSISTENT_RESERVE_IN 0x5e
#define SC_PERSISTENT_RESERVE_OUT 0x5f
/* PRIN service action codes */
#define SCSI_PRIN_READ_KEYS 0x00
#define SCSI_PRIN_READ_RESERVATION 0x01
#define SCSI_PRIN_REPORT_CAPABILITIES 0x02
/* PROUT service action codes */
#define SCSI_PROUT_REGISTER 0x00
#define SCSI_PROUT_RESERVE 0x01
#define SCSI_PROUT_RELEASE 0x02
#define SCSI_PROUT_CLEAR 0x03
#define SCSI_PROUT_PREEMPT 0x04
#define SCSI_PROUT_PREEMPT_AND_ABORT 0x05
#define SCSI_PROUT_REGISTER_AND_IGNORE_EXISTING_KEY 0x06
#define SCSI_PROUT_REGISTER_AND_MOVE 0x07
/* Persistent reservation type codes ref spc3r23 6.11.3.4 */
#define SCSI_PR_WRITE_EXCL 0x01
#define SCSI_PR_WRITE_EXCL_RO 0x05
#define SCSI_PR_EXCL_ACCESS_RO 0x06
#define SCSI_PR_WRITE_EXCL_AR 0x07
#define SCSI_PR_EXCL_ACCESS_AR 0x08

typedef struct {
    uint32 prGeneration;
    uint32 additionalLen;
} ScsiPRInCtrl;

#define CDB16_LEN 16
#define SENSE_BUFF_LEN 96

typedef struct {
    struct sg_io_hdr sgIo;
    int fd;
    uint8 cdb[CDB16_LEN];
    uint8 sense[SENSE_BUFF_LEN];
} ScsiFsdWrCtx;

#define FSD_W 0
#define FSD_R 1
#define FSD_COUNT 2

typedef enum SISC_PROC_STATUS_ {
    SISC_E_SUCCESS,
    SISC_E_ERROR,
    SISC_E_RESERVATION_CONFLICT,
} SISC_PROC_STATUS;

#define DBS_TRACE(module, ...) printf(...)

int ScsiReserve(int fd);
int ScsiInitPrKey(const uint8 *key, uint32 len);
int DbsScsiDoIoctl(int fd, struct sg_io_hdr *sgIo, char *buf, uint32 len);
int ScsiPrOutClear(int fd);
int ScsiGetReserveInfo(int fd, bool *IsReserved = NULL);


#endif
