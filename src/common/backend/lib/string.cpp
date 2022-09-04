/*-------------------------------------------------------------------------
 *
 * string.cpp
 *		string handling helpers
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * IDENTIFICATION
 *	  src/common/backend/lib/string.cpp
 *
 * -------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "lib/string.h"

/*
 * Returns whether the string `str' has the postfix `end'.
 */
bool pg_str_endswith(const char* str, const char* end)
{
    size_t slen = strlen(str);
    size_t elen = strlen(end);
    /* can't be a postfix if longer */
    if (elen > slen) {
        return false;
    }
    /* compare the end of the strings */
    str += slen - elen;
    return strcmp(str, end) == 0;
}
