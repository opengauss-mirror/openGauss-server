/* -------------------------------------------------------------------------
 * ndpplugin.h
 *	  prototypes for functions in contrib/ndpplugin/ndpplugin.cpp
 *
 * Portions Copyright (c) 2022 Huawei Technologies Co.,Ltd.
 *
 * IDENTIFICATION
 *	  contrib/ndpplugin/ndpplugin.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef NDPPLUGIN_NDPPLUGIN_H
#define NDPPLUGIN_NDPPLUGIN_H

#include <pthread.h>
#include "utils/palloc.h"

extern "C" void _PG_init(void);
extern "C" void _PG_fini(void);
extern "C" void ndpplugin_invoke(void);
extern "C" Datum pushdown_statistics(PG_FUNCTION_ARGS);

#define NDP_ASYNC_RPC
#define NDP_RPC_IP_LEN 16
#define NDP_RPC_WAIT_USEC 10

typedef enum NdpInstanceContextStatus {
    UNINITIALIZED,
    INITIALIZED
} NdpInstanceContextStatus;



typedef struct NdpInstanceContext {
    pthread_mutex_t mutex;
    volatile NdpInstanceContextStatus status;
    MpmcBoundedQueue<void *>* pageContext;
    void* pageContextPtr;
} NdpInstanceContext;

extern NdpInstanceContext g_ndp_instance;

#endif //NDPPLUGIN_NDPPLUGIN_H
