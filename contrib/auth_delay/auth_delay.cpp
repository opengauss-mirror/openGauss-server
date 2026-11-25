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

/* -------------------------------------------------------------------------
 *
 * auth_delay.c
 *
 * Copyright (C) 2010-2011, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		contrib/auth_delay/auth_delay.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "libpq/auth.h"
#include "port.h"
#include "utils/guc.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

extern "C" void _PG_init(void);

/* GUC Variables */
static int auth_delay_milliseconds;

/* Original Hook */
static ClientAuthentication_hook_type original_client_auth_hook = NULL;

/*
 * Check authentication
 */
static void auth_delay_checks(Port* port, int status)
{
    /*
     * Any other plugins which use ClientAuthentication_hook.
     */
    if (original_client_auth_hook)
        original_client_auth_hook(port, status);

    /*
     * Inject a short delay if authentication failed.
     */
    if (status != STATUS_OK) {
        pg_usleep(1000L * auth_delay_milliseconds);
    }
}

/*
 * Module Load Callback
 */
void _PG_init(void)
{
    /* Define custom GUC variables */
    DefineCustomIntVariable("auth_delay.milliseconds",
        "Milliseconds to delay before reporting authentication failure",
        NULL,
        &auth_delay_milliseconds,
        0,
        0,
        INT_MAX / 1000,
        PGC_SIGHUP,
        GUC_UNIT_MS,
        NULL,
        NULL,
        NULL);
    /* Install Hooks */
    original_client_auth_hook = ClientAuthentication_hook;
    ClientAuthentication_hook = auth_delay_checks;
}
