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
