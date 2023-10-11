/*
 * Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
 * fio_device_com.h
 *  Storage Adapter Header File.
 *
 *
 * IDENTIFICATION
 *        src/include/storage/file/fio_device_com.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef FIO_DEVICE_COM_H
#define FIO_DEVICE_COM_H

#include "c.h"
#include <dirent.h>
#include <sys/stat.h>
#include <stdio.h>

typedef enum en_device_type {
    DEV_TYPE_FILE = 0,
    DEV_TYPE_DSS,
    DEV_TYPE_NUM,
    DEV_TYPE_INVALID
} device_type_t;

extern bool g_enable_dss;
extern uint64 XLogSegmentSize;

#define INVALID_DEVICE_SIZE 0x7FFFFFFFFFFFFFFF
#define FILE_EXTEND_STEP_SIZE 2097152 // 2MB
#define DSS_XLOG_SEG_SIZE 1073741824 // 1GB xlog seg file size for DSS only
#define DSS_BATCH_SIZE 2048
#define DSS_SLRU_SEGMENT_SZIE 16777216

#define DSS_MAGIC_NUMBER 0xFEDCBA9876543210
#define MAX_FILE_NAME_LEN 64
#define DSS_MAX_MXACTOFFSET 1024
#define DSS_MAX_MXACTMEMBER 2048

#define GS_SUCCESS 0
#define GS_ERROR (-1)
#define GS_TIMEOUT 1

#endif /* FIO_DEVICE_COM_H */
