/*
 * Code mostly borrowed from PostgreSQL's stringinfo.c
 * palloc replaced to malloc, etc.
 */

#include "postgres_fe.h"
#include "postgres.h"
#include <lib/stringinfo.h>

constexpr size_t MAX_ALLOC_SIZE = 0x3fffffff; /* 1 gigabyte - 1 */
/* buffer growth factor */
constexpr int BUFFER_GROWTH_FACTOR = 2;

/*-------------------------
 * StringInfoData holds information about an extensible string.
 *	  data	  is the current buffer for the string.
 *	  len	  is the current string length.  There is guaranteed to be
 *			  a terminating '\0' at data[len], although this is not very
 *			  useful when the string holds binary data rather than text.
 *	  maxlen  is the allocated size in bytes of 'data', i.e. the maximum
 *			  string size (including the terminating '\0' char) that we can
 *			  currently store in 'data' without having to reallocate
 *			  more space.  We must always have maxlen > len.
 *	  cursor  is initialized to zero by makeStringInfo or initStringInfo,
 *			  but is not otherwise touched by the stringinfo.c routines.
 *			  Some routines use it to scan through a StringInfo.
 *-------------------------
 */

/*
 * initStringInfo
 *
 * Initialize a StringInfoData struct (with previously undefined contents)
 * to describe an empty string.
 */
void initStringInfo(StringInfo str)
{
    int size = 1024; /* initial default buffer size */

    str->data = (char *)malloc(size);
    str->maxlen = size;
    resetStringInfo(str);
}

/*
 * appendStringInfoString
 *
 * Append a null-terminated string to str.
 */
void appendStringInfoString(StringInfo str, const char *s)
{
    appendBinaryStringInfo(str, s, strlen(s));
}

/*
 * appendBinaryStringInfo
 *
 * Append arbitrary binary data to a StringInfo, allocating more space
 * if necessary.
 */
void appendBinaryStringInfo(StringInfo str, const char *data, int datalen)
{
    Assert(str != NULL);

    /* Make more room if needed */
    enlargeStringInfo(str, datalen);

    /* OK, append the data */
    errno_t rc = memcpy_s(str->data + str->len, (size_t)(str->maxlen - str->len), data, (size_t)datalen);
    securec_check(rc, "\0", "\0");
    str->len += datalen;

    /*
     * Keep a trailing null in place, even though it's probably useless for
     * binary data.  (Some callers are dealing with text but call this because
     * their input isn't null-terminated.)
     */
    str->data[str->len] = '\0';
}

/*
 * enlargeBuffer
 *
 * Make sure there is enough space for 'needed' more bytes
 * ('needed' does not include the terminating null).
 *
 * NB: because we use repalloc() to enlarge the buffer, the string buffer
 * will remain allocated in the same memory context that was current when
 * initStringInfo was called, even if another context is now current.
 * This is the desired and indeed critical behavior!
 */
void enlargeBuffer(int needed,   // needed more bytes
                   int len,      // current used buffer length in bytes
                   int *maxlen,  // original/new allocated buffer length
                   char **data)  // pointer to original/new buffer
{
    int newlen;

    /*
     * Guard against out-of-range "needed" values.	Without this, we can get
     * an overflow or infinite loop in the following.
     */
    /* should not happen */
    if (unlikely(needed < 0)) {
        printf("Error: invalid string enlargement request size: %d\n", needed);
        exit(1);
    }

    needed += len + 1; /* total space required now */

    /* Because of the above test, we now have needed <= MAX_ALLOC_SIZE */
    if (likely(needed <= static_cast<int>(*maxlen))) {
        return; /* got enough space already */
    }

    if (unlikely(((Size)len > MAX_ALLOC_SIZE) || ((Size)(needed - 1)) >= MAX_ALLOC_SIZE)) {
        printf("out of memory\n");
        printf("Cannot enlarge buffer containing %d bytes by %d more bytes.\n", len, needed);
        exit(1);
    }
    /*
     * We don't want to allocate just a little more space with each append;
     * for efficiency, double the buffer size each time it overflows.
     * Actually, we might need to more than double it if 'needed' is big...
     */
    newlen = BUFFER_GROWTH_FACTOR * *maxlen;
    while (needed > newlen) {
        newlen = BUFFER_GROWTH_FACTOR * newlen;
    }

    /*
     * Clamp to MAX_ALLOC_SIZE in case we went past it.  Note we are assuming
     * here that MAX_ALLOC_SIZE <= INT_MAX/2, else the above loop could
     * overflow.  We will still have newlen >= needed.
     */
    if (newlen > (int)MAX_ALLOC_SIZE) {
        newlen = (int)MAX_ALLOC_SIZE;
    }

    void* tmp = realloc(*data, newlen);
    if (tmp == nullptr) {
        return;
    }
    *data = static_cast<char*>(tmp);
    *maxlen = newlen;
}