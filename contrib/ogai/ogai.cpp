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
 *
 * -------------------------------------------------------------------------
 *
 * ogai.cpp
 *    OGAI extension implementation
 *
 * IDENTIFICATION
 *    contrib/ogai/ogai.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "commands/extension.h"
#include "executor/spi.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "tcop/utility.h"
#include "utils/builtins.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

extern "C" void create_ogai_extension(void)
{
    int ret;

    start_xact_command();
    ret = SPI_connect();
    if (ret != SPI_OK_CONNECT) {
        ereport(ERROR,
                (errcode(ERRCODE_INTERNAL_ERROR),
                 errmsg("OGAI SPI_connect failed: %s", SPI_result_code_string(ret))));
    }

    if (SPI_execute("CREATE EXTENSION IF NOT EXISTS ogai", false, 0) != SPI_OK_UTILITY) {
        ereport(WARNING,
                (errcode(ERRCODE_DATA_EXCEPTION),
                 errmsg("invalid query: CREATE EXTENSION IF NOT EXISTS ogai")));
    }

    SPI_finish();
    finish_xact_command();
}

/*
 * init_ogai_object
 *     Initialize OGAI hooks and objects.
 *     This function is called after the extension is loaded.
 */
extern "C" void init_ogai_object(void)
{
    /* Currently no hooks to initialize */
    /* Future: Initialize any function hooks or callbacks here */
    ereport(DEBUG1,
            (errmsg("OGAI extension objects initialized")));
}

/*
 * _PG_init
 *     Library initialization function (called when shared library is loaded).
 */
extern "C" void _PG_init(void)
{
    /* Register background workers, GUC variables, etc. if needed */
    ereport(DEBUG1,
            (errmsg("OGAI extension library loaded")));
}

/*
 * _PG_fini
 *     Library cleanup function (called when shared library is unloaded).
 */
extern "C" void _PG_fini(void)
{
    /* Cleanup any resources allocated in _PG_init */
    ereport(DEBUG1,
            (errmsg("OGAI extension library unloaded")));
}
