/* -------------------------------------------------------------------------
 *
 * shutdown.cpp
 *     do shutdown command
 *
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * IDENTIFICATION
 *        src/gausskernel/optimizer/commands/shutdown.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"
#include "commands/shutdown.h"

void DoShutdown(ShutdownStmt* stmt)
{
    if (!superuser()) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), (errmsg("Only system admin can shutdown database."))));
    }

    int signal;
    char* shutdown_mode = stmt->mode;

    if (shutdown_mode == NULL || strcmp(shutdown_mode, "fast") == 0) {
        signal = SIGINT;
    } else if (strcmp(shutdown_mode, "smart") == 0) {
        signal = SIGTERM;
    } else if (strcmp(shutdown_mode, "immediate") == 0) {
        signal = SIGQUIT;
    } else {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
             errmsg("unknow parameter: %s\nshutdown only support fast, smart and immediate mode.\n", shutdown_mode)));
    }

    if (gs_signal_send(PostmasterPid, signal)) {
        ereport(WARNING,
            (errmsg("Failed to send %s shutdown signal to postmaster.", shutdown_mode)));
    }
}
