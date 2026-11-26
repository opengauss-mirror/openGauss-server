/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-server is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#include "utils/dynloader.h"

#include "dlfcn.h"
#ifndef NDP_CLIENT
#include "utils/log.h"
#else
#include "utils/elog.h"
#endif

Status LoadSymbol(void *libHandle, char *symbol, void **symbolHandle)
{
    const char *dlsymErr = NULL;

    *symbolHandle = dlsym(libHandle, symbol);
    dlsymErr = dlerror();
    if (dlsymErr != NULL) {
#ifndef NDP_CLIENT
        LOG_ERROR << "load symbol error: " << symbol;
#else
        ereport(WARNING, (errmsg("load symbol error: %s", symbol)));
#endif
        return STATUS_ERROR;
    }
    return STATUS_OK;
}

Status OpenDl(void **libHandle, char *symbol)
{
    *libHandle = dlopen(symbol, RTLD_LAZY);
    if (*libHandle == NULL) {
#ifndef NDP_CLIENT
        LOG_ERROR << "load dynamic lib (" << symbol << ") error: " << dlerror();
#else
        ereport(WARNING, (errmsg("load dynamic lib error: %s", symbol)));
#endif
        return STATUS_ERROR;
    }
    return STATUS_OK;
}

void CloseDl(void *libHandle)
{
    (void)dlclose(libHandle);
}
