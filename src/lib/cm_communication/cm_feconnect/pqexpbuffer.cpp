/* -------------------------------------------------------------------------
 *
 * pqexpbuffer.c
 *
 * PQExpBuffer provides an indefinitely-extensible string data type.
 * It can be used to buffer either ordinary C strings (null-terminated text)
 * or arbitrary binary data.  All storage is allocated with malloc().
 *
 * This module is essentially the same as the backend's StringInfo data type,
 * but it is intended for use in frontend libpq and client applications.
 * Thus, it does not rely on palloc() nor elog().
 *
 * It does rely on vsnprintf(); if configure finds that libc doesn't provide
 * a usable vsnprintf(), then a copy of our own implementation of it will
 * be linked into libpq.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 * $PostgreSQL: pgsql/src/interfaces/libpq/pqexpbuffer.c,v 1.25 2008/11/26 00:26:23 tgl Exp $
 *
 * -------------------------------------------------------------------------
 */

#include "cm/cm_c.h"

#include <limits.h>

#include "cm/pqexpbuffer.h"

/* All "broken" PQExpBuffers point to this string. */
static const char oom_buffer[1] = "";

/*
 * markPQExpBufferBroken
 *
 * Put a PQExpBuffer in "broken" state if it isn't already.
 */
static void markPQExpBufferBroken(PQExpBuffer str)
{
    if (str->data != oom_buffer) {
        FREE_AND_RESET(str->data);
    }
    /*
     * Casting away const here is a bit ugly, but it seems preferable to
     * not marking oom_buffer const.  We want to do that to encourage the
     * compiler to put oom_buffer in read-only storage, so that anyone who
     * tries to scribble on a broken PQExpBuffer will get a failure.
     */
    str->data = (char*)oom_buffer;
    str->len = 0;
    str->maxlen = 0;
}

/*
 * initCMPQExpBuffer
 *
 * Initialize a PQExpBufferData struct (with previously undefined contents)
 * to describe an empty string.
 */
void initCMPQExpBuffer(PQExpBuffer str)
{
    str->data = (char*)malloc(INITIAL_EXPBUFFER_SIZE);
    if (str->data == NULL) {
        str->data = (char*)oom_buffer; /* see comment above */
        str->maxlen = 0;
        str->len = 0;
    } else {
        str->maxlen = INITIAL_EXPBUFFER_SIZE;
        str->len = 0;
        str->data[0] = '\0';
    }
}

/*
 * termCMPQExpBuffer(str)
 *		free()s the data buffer but not the PQExpBufferData itself.
 *		This is the inverse of initCMPQExpBuffer().
 */
void termCMPQExpBuffer(PQExpBuffer str)
{
    if (str->data != oom_buffer) {
        FREE_AND_RESET(str->data);
    }

    /* just for luck, make the buffer validly empty. */
    str->data = (char*)oom_buffer; /* see comment above */
    str->maxlen = 0;
    str->len = 0;
}

/*
 * resetCMPQExpBuffer
 *		Reset a PQExpBuffer to empty
 *
 * Note: if possible, a "broken" PQExpBuffer is returned to normal.
 */
void resetCMPQExpBuffer(PQExpBuffer str)
{
    if (str != NULL) {
        if ((str->data != NULL) && str->data != oom_buffer) {
            str->len = 0;
            str->data[0] = '\0';
        } else {
            /* try to reinitialize to valid state */
            initCMPQExpBuffer(str);
        }
    }
}

/*
 * enlargeCMPQExpBuffer
 * Make sure there is enough space for 'needed' more bytes in the buffer
 * ('needed' does not include the terminating null).
 *
 * Returns 1 if OK, 0 if failed to enlarge buffer.  (In the latter case
 * the buffer is left in "broken" state.)
 */
int enlargeCMPQExpBuffer(PQExpBuffer str, size_t needed)
{
    size_t newlen;
    char* newdata = NULL;

    if (PQExpBufferBroken(str))
        return 0; /* already failed */

    /*
     * Guard against ridiculous "needed" values, which can occur if we're fed
     * bogus data.	Without this, we can get an overflow or infinite loop in
     * the following.
     */
    if (needed >= ((size_t)INT_MAX - str->len)) {
        markPQExpBufferBroken(str);
        return 0;
    }

    needed += str->len + 1; /* total space required now */

    /* Because of the above test, we now have needed <= INT_MAX */
    if (needed <= str->maxlen) {
        return 1; /* got enough space already */
    }

    /*
     * We don't want to allocate just a little more space with each append;
     * for efficiency, double the buffer size each time it overflows.
     * Actually, we might need to more than double it if 'needed' is big...
     */
    newlen = (str->maxlen > 0) ? (2 * str->maxlen) : 64;
    while (needed > newlen) {
        newlen = 2 * newlen;
    }

    /*
     * Clamp to INT_MAX in case we went past it.  Note we are assuming here
     * that INT_MAX <= UINT_MAX/2, else the above loop could overflow.	We
     * will still have newlen >= needed.
     */
    if (newlen > (size_t)INT_MAX) {
        newlen = (size_t)INT_MAX;
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
        return 1;
    }

    markPQExpBufferBroken(str);
    return 0;
}

/*
 * printfCMPQExpBuffer
 * Format text data under the control of fmt (an sprintf-like format string)
 * and insert it into str.	More space is allocated to str if necessary.
 * This is a convenience routine that does the same thing as
 * resetCMPQExpBuffer() followed by appendCMPQExpBuffer().
 */
void printfCMPQExpBuffer(PQExpBuffer str, const char* fmt, ...)
{
    va_list args;
    size_t avail;
    int nprinted;

    resetCMPQExpBuffer(str);

    if (PQExpBufferBroken(str)) {
        return; /* already failed */
    }

    for (;;) {
        /*
         * Try to format the given string into the available space; but if
         * there's hardly any space, don't bother trying, just fall through to
         * enlarge the buffer first.
         */
        if (str->maxlen > str->len + 16) {
            avail = str->maxlen - str->len - 1;
            va_start(args, fmt);
            nprinted = vsnprintf_s(str->data + str->len, str->maxlen - str->len, avail, fmt, args);
            va_end(args);

            /*
             * Note: some versions of vsnprintf return the number of chars
             * actually stored, but at least one returns -1 on failure. Be
             * conservative about believing whether the print worked.
             */
            if (nprinted >= 0 && nprinted < (int)avail - 1) {
                /* Success.  Note nprinted does not include trailing null. */
                str->len += nprinted;
                break;
            }
        }
        /* Double the buffer size and try again. */
        if (!enlargeCMPQExpBuffer(str, str->maxlen))
            return; /* oops, out of memory */
    }
}

/*
 * appendCMPQExpBuffer
 *
 * Format text data under the control of fmt (an sprintf-like format string)
 * and append it to whatever is already in str.  More space is allocated
 * to str if necessary.  This is sort of like a combination of sprintf and
 * strcat.
 */
void appendCMPQExpBuffer(PQExpBuffer str, const char* fmt, ...)
{
    va_list args;
    size_t avail;
    int nprinted;

    if (PQExpBufferBroken(str)) {
        return; /* already failed */
    }

    for (;;) {
        /*
         * Try to format the given string into the available space; but if
         * there's hardly any space, don't bother trying, just fall through to
         * enlarge the buffer first.
         */
        if (str->maxlen > str->len + 16) {
            avail = str->maxlen - str->len - 1;
            va_start(args, fmt);
            nprinted = vsnprintf_s(str->data + str->len, str->maxlen - str->len, avail, fmt, args);
            va_end(args);

            /*
             * Note: some versions of vsnprintf return the number of chars
             * actually stored, but at least one returns -1 on failure. Be
             * conservative about believing whether the print worked.
             */
            if (nprinted >= 0 && nprinted < (int)avail - 1) {
                /* Success.  Note nprinted does not include trailing null. */
                str->len += nprinted;
                break;
            }
        }
        /* Double the buffer size and try again. */
        if (!enlargeCMPQExpBuffer(str, str->maxlen)) {
            return; /* oops, out of memory */
        }
    }
}

/*
 * appendBinaryCMPQExpBuffer
 *
 * Append arbitrary binary data to a PQExpBuffer, allocating more space
 * if necessary.
 */
void appendBinaryCMPQExpBuffer(PQExpBuffer str, const char* data, size_t datalen)
{
    errno_t rc;
    /* Make more room if needed */
    if (!enlargeCMPQExpBuffer(str, datalen)) {
        return;
    }

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
