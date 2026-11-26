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
 * assert.c
 * Assert code.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 * src/backend/utils/error/assert.c
 *
 * NOTE
 * This should eventually work with elog()
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <unistd.h>

/*
 * ExceptionalCondition - Handles the failure of an Assert()
 */
void ExceptionalCondition(const char* conditionName, const char* errorType, const char* fileName, int lineNumber)
{
    if (!PointerIsValid(conditionName) || !PointerIsValid(fileName) || !PointerIsValid(errorType)) {
        write_stderr("TRAP: ExceptionalCondition: bad arguments\n");
    } else {
        write_stderr("TRAP: %s(\"%s\", File: \"%s\", Line: %d)\n", errorType, conditionName, fileName, lineNumber);
    }

    /* Usually this shouldn't be needed, but make sure the msg went out */
    fflush(stderr);

#ifdef SLEEP_ON_ASSERT

    /*
     * It would be nice to use pg_usleep() here, but only does 2000 sec or 33
     * minutes, which seems too short.
     */
    sleep(1000000);
#endif

    abort();
}
