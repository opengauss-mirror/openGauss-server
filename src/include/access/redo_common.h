/*
 * redo_common.h
 *
 * Utilities for replaying WAL records.
 *
 * openGauss transaction log manager utility routines
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/redo_common.h
 */
#ifndef REDO_COMMON_H
#define REDO_COMMON_H

#ifdef BUILD_ALONE

#define pfree(pointer) free(pointer)
#define palloc(sz) malloc(sz)

#define module_logging_is_on(a) false

#define securec_check(errno, charList, ...)                                    \
    {                                                                          \
        if (unlikely(EOK != errno)) {                                          \
            elog(ERROR, "%s : %d : securec check error.", __FILE__, __LINE__); \
        }                                                                      \
    }

#define START_CRIT_SECTION()
#define END_CRIT_SECTION()

#define elog(...)
#define elog_finish(a, b, ...)
#define elog_start(a, b, c)
#define errstart(...)
#define ereport(...)
#define errfinish(a, ...)
#define errmodule(a)
#define errmsg(c, ...)
#define errcode(a)

extern THR_LOCAL bool assert_enabled;

#endif

#endif