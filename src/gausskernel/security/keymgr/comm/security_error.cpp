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
 * -------------------------------------------------------------------------
 *
 * security_error.cpp
 *
 * IDENTIFICATION
 *	  src/gausskernel/security/keymgr/src/comm/security_error.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "keymgr/comm/security_error.h"
#ifndef FRONTEND
#include "knl/knl_instance.h"
#include "utils/aset.h"
#endif
#include <stdio.h>
KmErr *km_err_new(size_t sz)
{
    KmErr *err;

    if (sz == 0) {
        return NULL;
    }

#ifndef FRONTEND
    MemoryContext cxt;
    MemoryContext old;

    cxt = AllocSetContextCreate(
        g_instance.instance_context,
        "key manager module",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_MINSIZE,
        SHARED_CONTEXT);

    old = MemoryContextSwitchTo(cxt);
#endif

    err = (KmErr *)km_alloc(sizeof(KmErr));
    if (err == NULL) {
        return NULL;
    }

    err->buf = (char *)km_alloc(sz);
    if (err->buf == NULL) {
        km_free(err);
        return NULL;
    }

#ifndef FRONTEND
    MemoryContextSwitchTo(old);
    err->cxt = cxt;
#endif

    err->sz = sz;
    err->code = KM_ERR_INIT;

    km_err_reset(err);

    return err;
}

void km_err_free(KmErr *err)
{
    if (err == NULL) {
        return;
    }

    km_free(err->buf);
    km_free(err);
}

#ifndef FRONTEND
void *km_cxt_alloc_zero(MemoryContext cxt, size_t size)
{
    void *buf;
    MemoryContext old;
    
    old = MemoryContextSwitchTo(cxt);
    buf = km_alloc_zero(size);
    MemoryContextSwitchTo(old);

    return buf;
}
#endif