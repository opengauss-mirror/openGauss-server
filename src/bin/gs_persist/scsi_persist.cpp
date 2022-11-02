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
 * scsi_persist.cpp
 *      do ioctl
 *
 * IDENTIFICATION
 *    src/bin/gs_persist/scsi_persist.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "scsi_protocol.h"
#include <stdint.h>
#include <endian.h>
#include <scsi/sg.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>

int DbsScsiDoIoctl(int fd, struct sg_io_hdr *sgIo, char *buf, uint32 len)
{
    uint8 sense[SENSE_BUFF_LEN] = {0};
    uint8 *cdb = sgIo->cmdp;
    const uint32 timeout = 5000;
    int ret;

    sgIo->interface_id = 'S';
    sgIo->sbp = sense;
    sgIo->mx_sb_len = sizeof(sense);
    sgIo->dxferp = buf;
    sgIo->dxfer_len = len;
    sgIo->timeout = timeout;

    ret = ioctl(fd, SG_IO, sgIo);
    if (ret < 0) {
        printf("Ioctl(SG_IO %d %02x%02x) failed: %s (errno=%d).\n", sgIo->dxfer_direction, cdb[0], cdb[1],
               strerror(errno), errno);
        return SISC_E_ERROR;
    }
    if (sgIo->status != 0 || sgIo->host_status != 0 || sgIo->driver_status != 0) {
        printf("Ioctl failed (len %u). status:%d, host_status:%d, driver_status:%d.\n", len, sgIo->status,
               sgIo->host_status, sgIo->driver_status);
        return SISC_E_ERROR;
    }
    return SISC_E_SUCCESS;
}
