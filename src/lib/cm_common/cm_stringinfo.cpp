/**
 * @file cm_stringinfo.cpp
 * @brief StringInfo provides an indefinitely-extensible string data type.
 *  It can be used to buffer either ordinary C strings (null-terminated text)
 *  or arbitrary binary data.  All storage is allocated with palloc().
 * @author xxx
 * @version 1.0
 * @date 2020-08-06
 * 
 * @copyright Copyright (c) Huawei Technologies Co., Ltd. 2011-2020. All rights reserved.
 * 
 */
#include "cm/cm_c.h"
#include "cm/stringinfo.h"
#include "cm/elog.h"

/*
 * makeStringInfo
 *
 * Create an empty 'StringInfoData' & return a pointer to it.
 */
CM_StringInfo CM_makeStringInfo(void)
{
    CM_StringInfo res;

    res = (CM_StringInfo)malloc(sizeof(CM_StringInfoData));
    if (res == NULL) {
        write_runlog(ERROR, "malloc CM_StringInfo failed, out of memory.\n");
        exit(1);
    }

    CM_initStringInfo(res);

    return res;
}

/*
 * makeStringInfo
 *
 * Create an empty 'StringInfoData' & return a pointer to it.
 */
void CM_destroyStringInfo(CM_StringInfo str)
{
    if (str != NULL) {
        if (str->maxlen > 0) {
            FREE_AND_RESET(str->data);
        }
        free(str);
    }
    return;
}

/*
 * makeStringInfo
 *
 * Create an empty 'StringInfoData' & return a pointer to it.
 */
void CM_freeStringInfo(CM_StringInfo str)
{
    if (str->maxlen > 0) {
        FREE_AND_RESET(str->data);
    }
    return;
}

/*
 * dupStringInfo
 *
 * Get new StringInfo and copy the original to it.
 */
CM_StringInfo CM_dupStringInfo(CM_StringInfo orig)
{
    CM_StringInfo newvar;

    newvar = CM_makeStringInfo();
    if (newvar == NULL) {
        return (newvar);
    }

    if (orig->len > 0) {
        CM_appendBinaryStringInfo(newvar, orig->data, orig->len);
        newvar->cursor = orig->cursor;
    }
    return (newvar);
}

/*
 * copyStringInfo
 * Deep copy: Data part is copied too.   Cursor of the destination is
 * initialized to zero.
 */
void CM_copyStringInfo(CM_StringInfo to, CM_StringInfo from)
{
    CM_resetStringInfo(to);
    CM_appendBinaryStringInfo(to, from->data, from->len);
    return;
}

/*
 * initStringInfo
 *
 * Initialize a StringInfoData struct (with previously undefined contents)
 * to describe an empty string.
 */
void CM_initStringInfo(CM_StringInfo str)
{
    int size = 1024; /* initial default buffer size */

    str->data = (char*)malloc(size);
    if (str->data == NULL) {
        write_runlog(ERROR, "malloc CM_StringInfo->data failed, out of memory.\n");
        exit(1);
    }
    str->maxlen = size;
    CM_resetStringInfo(str);
}

/*
 * resetStringInfo
 *
 * Reset the StringInfo: the data buffer remains valid, but its
 * previous content, if any, is cleared.
 */
void CM_resetStringInfo(CM_StringInfo str)
{
    if (str == NULL) {
        return;
    }

    str->data[0] = '\0';
    str->len = 0;
    str->cursor = 0;
    str->qtype = 0;
    str->msglen = 0;
}

/*
 * appendStringInfo
 *
 * Format text data under the control of fmt (an sprintf-style format string)
 * and append it to whatever is already in str.  More space is allocated
 * to str if necessary.  This is sort of like a combination of sprintf and
 * strcat.
 */
void CM_appendStringInfo(CM_StringInfo str, const char* fmt, ...)
{
    for (;;) {
        va_list args;
        bool success = false;

        /* Try to format the data. */
        va_start(args, fmt);
        success = CM_appendStringInfoVA(str, fmt, args);
        va_end(args);

        if (success) {
            break;
        }

        /* Double the buffer size and try again. */
        (void)CM_enlargeStringInfo(str, str->maxlen);
    }
}

/*
 * appendStringInfoVA
 *
 * Attempt to format text data under the control of fmt (an sprintf-style
 * format string) and append it to whatever is already in str.	If successful
 * return true; if not (because there's not enough space), return false
 * without modifying str.  Typically the caller would enlarge str and retry
 * on false return --- see appendStringInfo for standard usage pattern.
 *
 * XXX This API is ugly, but there seems no alternative given the C spec's
 * restrictions on what can portably be done with va_list arguments: you have
 * to redo va_start before you can rescan the argument list, and we can't do
 * that from here.
 */
bool CM_appendStringInfoVA(CM_StringInfo str, const char* fmt, va_list args)
{
    int avail, nprinted;

    /*
     * If there's hardly any space, don't bother trying, just fail to make the
     * caller enlarge the buffer first.
     */
    avail = str->maxlen - str->len - 1;
    if (avail < 16) {
        return false;
    }

        /*
         * Assert check here is to catch buggy vsnprintf that overruns the
         * specified buffer length.  Solaris 7 in 64-bit mode is an example of a
         * platform with such a bug.
         */
#ifdef USE_ASSERT_CHECKING
    str->data[str->maxlen - 1] = '\0';
#endif

    nprinted = vsnprintf_s(str->data + str->len, str->maxlen - str->len, avail, fmt, args);

    /*
     * Note: some versions of vsnprintf return the number of chars actually
     * stored, but at least one returns -1 on failure. Be conservative about
     * believing whether the print worked.
     */
    if (nprinted >= 0 && nprinted < avail - 1) {
        /* Success.  Note nprinted does not include trailing null. */
        str->len += nprinted;
        return true;
    }

    /* Restore the trailing null so that str is unmodified. */
    str->data[str->len] = '\0';
    return false;
}

/*
 * appendStringInfoString
 *
 * Append a null-terminated string to str.
 * Like appendStringInfo(str, "%s", s) but faster.
 */
void CM_appendStringInfoString(CM_StringInfo str, const char* s)
{
    CM_appendBinaryStringInfo(str, s, strlen(s));
}

/*
 * appendStringInfoChar
 *
 * Append a single byte to str.
 * Like appendStringInfo(str, "%c", ch) but much faster.
 */
void CM_appendStringInfoChar(CM_StringInfo str, char ch)
{
    /* Make more room if needed */
    if (str->len + 1 >= str->maxlen) {
        (void)CM_enlargeStringInfo(str, 1);
    }

    /* OK, append the character */
    str->data[str->len] = ch;
    str->len++;
    str->data[str->len] = '\0';
}

/*
 * appendBinaryStringInfo
 *
 * Append arbitrary binary data to a StringInfo, allocating more space
 * if necessary.
 */
void CM_appendBinaryStringInfo(CM_StringInfo str, const char* data, int datalen)
{
    errno_t rc;

    /* Make more room if needed */
    (void)CM_enlargeStringInfo(str, datalen);

    /* OK, append the data */
    rc = memcpy_s(str->data + str->len, str->maxlen - str->len, data, datalen);
    securec_check_c(rc, "\0", "\0");
    str->len += datalen;

    /*
     * Keep a trailing null in place, even though it's probably useless for
     * binary data...
     */
    str->data[str->len] = '\0';
}

/*
 * enlargeStringInfo
 *
 * Make sure there is enough space for 'needed' more bytes
 * ('needed' does not include the terminating null).
 *
 * External callers usually need not concern themselves with this, since
 * all stringinfo.c routines do it automatically.  However, if a caller
 * knows that a StringInfo will eventually become X bytes large, it
 * can save some palloc overhead by enlarging the buffer before starting
 * to store data in it.
 *
 * NB: because we use repalloc() to enlarge the buffer, the string buffer
 * will remain allocated in the same memory context that was current when
 * initStringInfo was called, even if another context is now current.
 * This is the desired and indeed critical behavior!
 */
int CM_enlargeStringInfo(CM_StringInfo str, int needed)
{
    int newlen;
    char* newdata = NULL;

    /*
     * Guard against out-of-range "needed" values.	Without this, we can get
     * an overflow or infinite loop in the following.
     */
    if (needed < 0) /* should not happen */
    {
        write_runlog(ERROR, "invalid string enlargement request size: %d\n", needed);
        return -1;
    }

    if (((Size)needed) >= (CM_MaxAllocSize - (Size)str->len)) {
        write_runlog(ERROR,
            "out of memory !Cannot enlarge string buffer containing %d bytes by %d more bytes.\n",
            str->len,
            needed);
        return -1;
    }

    needed += str->len + 1; /* total space required now */

    /* Because of the above test, we now have needed <= MaxAllocSize */

    if (needed <= str->maxlen) {
        return 0; /* got enough space already */
    }

    /*
     * We don't want to allocate just a little more space with each append;
     * for efficiency, double the buffer size each time it overflows.
     * Actually, we might need to more than double it if 'needed' is big...
     */
    newlen = 2 * str->maxlen;
    while (needed > newlen) {
        newlen = 2 * newlen;
    }

    /*
     * Clamp to MaxAllocSize in case we went past it.  Note we are assuming
     * here that MaxAllocSize <= INT_MAX/2, else the above loop could
     * overflow.  We will still have newlen >= needed.
     */
    if (newlen > (int)CM_MaxAllocSize) {
        newlen = (int)CM_MaxAllocSize;
    }

    newdata = (char*)malloc(newlen);
    if (newdata != NULL) {
        if (str->data != NULL) {
            errno_t rc;
            rc = memcpy_s(newdata, newlen, str->data, str->maxlen);
            securec_check_c(rc, "\0", "\0");
            FREE_AND_RESET(str->data);
        }
        str->data = newdata;
        str->maxlen = newlen;
    } else {
        if (str->data != NULL) {
            FREE_AND_RESET(str->data);
            str->maxlen = 0;
        }
    }
    return 0;
}

int CM_is_str_all_digit(const char* name)
{
    int size = 0;
    int i = 0;

    if (name == NULL) {
        write_runlog(ERROR, "CM_is_str_all_digit input null\n");
        return -1;
    }

    size = strlen(name);
    for (i = 0; i < size; i++) {
        if (name[i] < '0' || name[i] > '9') {
            return -1;
        }
    }
    return 0;
}