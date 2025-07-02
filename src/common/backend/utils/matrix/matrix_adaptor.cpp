/* -------------------------------------------------------------------------
 *
 * matrix_adaptor.cpp
 *	  matrix adaptor function.
 *
 * adaptor function
 *
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * Mulan Permissive Software License，Version 2
 * Mulan Permissive Software License，Version 2 (Mulan PSL v2)
 *
 * IDENTIFICATION
 *	  src/common/backend/utils/matrix/matrix_adaptor.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "knl/knl_instance.h"
#include "utils/matrix_adaptor.h"

#ifdef __USE_NUMA
#include <numa.h>
#endif

#ifdef __USE_NUMA
/*
 * remote node like:
 * numa 0:0-79
 * numa 1:80-159
 * numa 2:160-139
 * numa 3:240-319
 * numa 5:
 * numa 63:
 */
int MatrixMaxNumaNode(void)
{
    int numaNodeNum = numa_max_node() + 1;

    static const char* basePath = "/sys/devices/system/node/node%d/remote";
    int localNodeNum = 0;
    for (int i = 0; i < numaNodeNum; i++) {
        char path[MAXPGPATH] = {};
        errno_t ret = snprintf_s(path, MAXPGPATH, MAXPGPATH - 1, basePath, i);
        securec_check_ss(ret, "\0", "\0");
        FILE* file = fopen(path, "r");
        int remote = 0;
        /* do not contains remote node */
        if (!file && i == 0) {
            return numaNodeNum;
        }

        /* file exits and remote tag = 1 */
        if (file && fscanf_s(file, "%d", &remote) == 1) {
            if (remote == 0) {
                localNodeNum++;
            }
            fclose(file);
        } else {
            /* if node6 file no exists, quick stop */
            break;
        }
    }
    return localNodeNum;
}

#endif