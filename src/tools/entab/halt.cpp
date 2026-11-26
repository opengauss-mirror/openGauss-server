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
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 */

/*
**
**		halt.c
**
** src/tools/entab/halt.c
**
**		This is used to print out error messages and exit
*/

#include <stdarg.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

/* -------------------------------------------------------------------------
**
**		halt - print error message, and call clean up routine or exit
**
** ------------------------------------------------------------------------ */

/*VARARGS*/
void halt(const char* format, ...)
{
    va_list arg_ptr;
    const char* pstr = NULL;
    void (*sig_func)();

    va_start(arg_ptr, format);
    if (strncmp(format, "PERROR", 6) != 0)
        vfprintf(stderr, format, arg_ptr);
    else {
        for (pstr = format + 6; *pstr == ' ' || *pstr == ':'; pstr++)
            ;
        vfprintf(stderr, pstr, arg_ptr);
        perror("");
    }
    va_end(arg_ptr);
    fflush(stderr);

    /* call one clean up function if defined */
    if ((sig_func = signal(SIGTERM, SIG_DFL)) != SIG_DFL && sig_func != SIG_IGN)
        (*sig_func)(0);
    else if ((sig_func = signal(SIGHUP, SIG_DFL)) != SIG_DFL && sig_func != SIG_IGN)
        (*sig_func)(0);
    else if ((sig_func = signal(SIGINT, SIG_DFL)) != SIG_DFL && sig_func != SIG_IGN)
        (*sig_func)(0);
    else if ((sig_func = signal(SIGQUIT, SIG_DFL)) != SIG_DFL && sig_func != SIG_IGN)
        (*sig_func)(0);
    exit(1);
}
