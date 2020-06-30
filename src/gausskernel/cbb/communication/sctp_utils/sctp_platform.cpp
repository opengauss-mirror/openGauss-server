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
 * sctp_platform.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/communication/sctp_utils/sctp_platform.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <sys/utsname.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <linux/version.h>
#include "sctp_platform.h"
#include "libcomm.h"
#include "securec.h"

typedef enum LinuxVersionPart {
    LINUX_MAJORO_VERSION = 0,
    LINUX_MINOR_VERSION = 1,
    LINUX_PATCH_VERSION = 2,
    LINUX_BUILD_VERSION = 3,
    LINUX_VERSOION_COUNT = 4
} LinuxVersionPart;

static int has_string(const char* line, const char* str)
{
    int len = strlen(str);
    char* ptr = (char*)line;
    while (*ptr != '\0') {
        if ((*ptr == *str) && (strncmp(str, ptr, len) == 0)) {
            return 1;
        }
        ptr++;
    }
    return 0;
}

/*
 * function name: mc_check_SLESSP2_version
 * description  : check the current OS kernel version
 * return value :
 *             1: suse kernel version >=2.6.32.22 or other OS kernel version >=2.6.32
 *             0: suse kernel version < 2.6.32.22 or other OS kernel version < 2.6.32
 *            -1: abnormal
 *
 * Note: sctp libcomm need SUSE 11 SP2(kernel version is 3.0.13)
 *       or Redhat6.4(kernel version is 2.6.32).
 */
int mc_check_SLESSP2_version(void)
{
    FILE* procinfo = NULL;
    char buffer[8192];
    int is_suse = 0;
    uint idx = 0;
    uint part_value[LINUX_VERSOION_COUNT] = {0};
    struct utsname un;
    unsigned long ver = 0;
    char* q = NULL;
    const uint EXPECT_MAJORO_VERSION = 2;
    const uint EXPECT_MINOR_VERSION = 6;
    const uint EXPECT_PATCH_VERSION = 32;
    const uint EXPECT_BUILD_VERSION = 22;

    procinfo = fopen("/proc/version", "r");
    if (procinfo == NULL) {
        if (errno != ENOENT) {
            perror("/proc/version");
        }
    } else {
        do {
            if (fgets(buffer, sizeof(buffer), procinfo)) {
                if (has_string(buffer, "SUSE")) {
                    is_suse = 1;
                    break;
                }
            }
        } while (!feof(procinfo));
        fclose(procinfo);
    }

    if (uname(&un) != -1) {
        q = un.release;
        for (idx = 0; ((q != NULL) && (idx < LINUX_VERSOION_COUNT)); idx++) {
            part_value[idx] = (uint)atoi(q);
            q = strchr(q, '.');
            if (q == NULL) {
                break;
            }
            ++q;
        }

        /* if idx less than 3, means we get linux version not complete,  */
        if (idx < LINUX_BUILD_VERSION) {
            return -1;
        }

        ver = KERNEL_VERSION(
            part_value[LINUX_MAJORO_VERSION], part_value[LINUX_MINOR_VERSION], part_value[LINUX_PATCH_VERSION]);
        if (ver > KERNEL_VERSION(EXPECT_MAJORO_VERSION, EXPECT_MINOR_VERSION, EXPECT_PATCH_VERSION)) {
            return 1;
        } else if (ver == KERNEL_VERSION(EXPECT_MAJORO_VERSION, EXPECT_MINOR_VERSION, EXPECT_PATCH_VERSION)) {
            /* check if the current SUSE kernel version is great than 2.6.32.22 */
            if (is_suse) {
                return (part_value[LINUX_BUILD_VERSION] >= EXPECT_BUILD_VERSION) ? 1 : 0;
            } else {
                return 1;
            }
        } else {
            return 0;
        }
    }

    return -1;
}

int mc_check_sctp_support(void)
{
    int fd = -1;

    fd = socket(PF_INET, SOCK_STREAM, IPPROTO_SCTP);
    if (fd < 0) {
        return -1;
    } else {
        close(fd);
        return 1;
    }
}
