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
 * gs_persist.cpp
 *      reserve key on device
 *
 * IDENTIFICATION
 *    src/bin/gs_persist/gs_persist.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <stdio.h>
#include <scsi_protocol.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>


static const int ARGS_NUM = 4;
static const int ACTION_NO = 3;
static const int KEY_NO = 2;
static const int DEVICE_NO = 1;

static void usage()
{
    printf(_("gs_persist: reserve key and preempt for the shared storage.\n\n"));
    printf(_("Usage:\n"));
    printf(_("  gs_persist [DEVICEPATH] [KEY] [ACTION]\n"));
    printf(_("[DEVICEPATH]: the path of the shared storage\n"));
    printf(_("[KEY]: the key to be reserved on the shared storage(the valid length of key is %u)\n"), PR_KEY_LEN);
    printf(_("[ACTION]: \n"
        "\t\t-I query storage reserved infomation\n"
        "\t\t-R reserved on storage\n"
        "\t\t-C clear reserved infomation on storage\n"));
    printf(_("-?, --help            show this help, then exit\n"));
}


int GsPersistMain(int argc, char **argv)
{
    if (argc > 1) {
        if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0) {
            usage();
            return 0;
        }
    }

    if (argc != ARGS_NUM) {
        printf(_("the num(%d) of parameters input is invalid.\n\n"), argc);
        usage();
        return 1;
    }

    uint8 key[PR_KEY_LEN];
    uint32 len = strlen(argv[KEY_NO]);

    errno_t rc = memset_s(key, sizeof(key), '0', sizeof(key));
    securec_check_c(rc, "\0", "\0");
    for (uint32 i = 0; i < len && i < PR_KEY_LEN; i++) {
        key[i] = argv[KEY_NO][len - 1 - i];
    }

    int ret = ScsiInitPrKey(key, PR_KEY_LEN);
    if (ret != 0) {
        return 1;
    }

    int fd = open(argv[DEVICE_NO], O_NONBLOCK | O_RDWR);
    if (fd < 0) {
        printf(_("input file name(%s) error, %m\n"), argv[DEVICE_NO]);
        return 1;
    }

    if (argv[ACTION_NO][0] != '-') {
        printf(_("input action error:%s\n"), argv[ACTION_NO]);
        close(fd);
        return 1;
    }

    switch (argv[ACTION_NO][1]) {
        case 'R':
            ret = ScsiReserve(fd);
            break;
        case 'C':
            ret = ScsiPrOutClear(fd);
            break;
        case 'I':
            ret = ScsiGetReserveInfo(fd);
            break;
        default:
            break;
    }
    close(fd);
    return ret;
}

int main(int argc, char **argv)
{
    return GsPersistMain(argc, argv);
}

