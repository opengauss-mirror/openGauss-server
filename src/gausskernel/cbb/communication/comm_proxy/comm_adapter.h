/*
 * Copyright (c) 2021 Huawei Technologies Co.,Ltd.
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
 * comm_adapter.h
 *     Include all param data structure for Socket API invokation in proxy mode
 *     example:
 *        @api: socket()
 *        @data structure: CommSocketParam
 *
 * IDENTIFICATION
 *        src/gausskernel/cbb/communication/comm_proxy/comm_adapter.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef COMM_ADAPTER_H
#define COMM_ADAPTER_H

#include "postgres.h"

#include "utils/atomic_lse.h"

/* for gaussdb kernel */
#include "utils/atomic.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "gssignal/gs_signal.h"
#include "utils/memprot.h"
#include "gs_threadlocal.h"
#include "postgres.h"
#include "knl/knl_variable.h"
#include "libpq/ip.h"
#include "libpq/libpq.h"
#include "postmaster/postmaster.h"
#include "utils/ps_status.h"
#include "threadpool/threadpool.h"
#include "port.h"
#include "pgtime.h"
#include "stdarg.h"

extern char* TrimStr(const char* str);

#ifdef CM_INSTANCE
/* for cm */
#include "cm/cm_c.h"
#endif

#ifdef GTM_INSTANCE
/* for gtm */

#endif

#ifdef INTERFACE_LIBPQ
/* for client interface */

#endif

#endif /* COMM_ADAPTER_H */
