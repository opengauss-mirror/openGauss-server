/* ---------------------------------------------------------------------------------------
 * 
 * stringinfo.h
 *        Declarations/definitions for "StringInfo" functions.
 *
 * StringInfo provides an indefinitely-extensible string data type.
 * It can be used to buffer either ordinary C strings (null-terminated text)
 * or arbitrary binary data.  All storage is allocated with palloc().
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 * 
 * IDENTIFICATION
 *        src/include/cm/stringinfo.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef CM_STRINGINFO_H
#define CM_STRINGINFO_H

#include <stdarg.h>

/*-------------------------
 * StringInfoData holds information about an extensible string.
 *		data	is the current buffer for the string (allocated with palloc).
 *		len		is the current string length.  There is guaranteed to be
 *				a terminating '\0' at data[len], although this is not very
 *				useful when the string holds binary data rather than text.
 *		maxlen	is the allocated size in bytes of 'data', i.e. the maximum
 *				string size (including the terminating '\0' char) that we can
 *				currently store in 'data' without having to reallocate
 *				more space.  We must always have maxlen > len.
 *		cursor	is initialized to zero by makeStringInfo or initStringInfo,
 *				but is not otherwise touched by the stringinfo.c routines.
 *				Some routines use it to scan through a StringInfo.
 *-------------------------
 */
typedef struct CM_StringInfoData {
    char* data;
    int len;
    int maxlen;
    int cursor;
    int qtype;
    int msglen;
} CM_StringInfoData;

typedef CM_StringInfoData* CM_StringInfo;

#define CM_MaxAllocSize ((Size)0x3fffffff) /* 1 gigabyte - 1 */

#define CM_AllocSizeIsValid(size) ((Size)(size) <= CM_MaxAllocSize)

/*------------------------
 * There are two ways to create a StringInfo object initially:
 *
 * StringInfo stringptr = makeStringInfo();
 *		Both the StringInfoData and the data buffer are palloc'd.
 *
 * StringInfoData string;
 * initStringInfo(&string);
 *		The data buffer is palloc'd but the StringInfoData is just local.
 *		This is the easiest approach for a StringInfo object that will
 *		only live as long as the current routine.
 *
 * To destroy a StringInfo, pfree() the data buffer, and then pfree() the
 * StringInfoData if it was palloc'd.  There's no special support for this.
 *
 * NOTE: some routines build up a string using StringInfo, and then
 * release the StringInfoData but return the data string itself to their
 * caller.	At that point the data string looks like a plain palloc'd
 * string.
 *-------------------------
 */

/*------------------------
 * makeStringInfo
 * Create an empty 'StringInfoData' & return a pointer to it.
 */
extern CM_StringInfo CM_makeStringInfo(void);

/*------------------------
 * initStringInfo
 * Initialize a StringInfoData struct (with previously undefined contents)
 * to describe an empty string.
 */
extern void CM_initStringInfo(CM_StringInfo str);

/*------------------------
 * resetStringInfo
 * Clears the current content of the StringInfo, if any. The
 * StringInfo remains valid.
 */
extern void CM_resetStringInfo(CM_StringInfo str);

/*------------------------
 * appendStringInfo
 * Format text data under the control of fmt (an sprintf-style format string)
 * and append it to whatever is already in str.  More space is allocated
 * to str if necessary.  This is sort of like a combination of sprintf and
 * strcat.
 */
extern void CM_appendStringInfo(CM_StringInfo str, const char* fmt, ...)
    /* This extension allows gcc to check the format string */
    __attribute__((format(printf, 2, 3)));

/*------------------------
 * appendStringInfoVA
 * Attempt to format text data under the control of fmt (an sprintf-style
 * format string) and append it to whatever is already in str.	If successful
 * return true; if not (because there's not enough space), return false
 * without modifying str.  Typically the caller would enlarge str and retry
 * on false return --- see appendStringInfo for standard usage pattern.
 */
extern bool CM_appendStringInfoVA(CM_StringInfo str, const char* fmt, va_list args)
    __attribute__((format(printf, 2, 0)));

/*------------------------
 * appendStringInfoString
 * Append a null-terminated string to str.
 * Like appendStringInfo(str, "%s", s) but faster.
 */
extern void CM_appendStringInfoString(CM_StringInfo str, const char* s);

/*------------------------
 * appendStringInfoChar
 * Append a single byte to str.
 * Like appendStringInfo(str, "%c", ch) but much faster.
 */
extern void CM_appendStringInfoChar(CM_StringInfo str, char ch);

/*------------------------
 * appendStringInfoCharMacro
 * As above, but a macro for even more speed where it matters.
 * Caution: str argument will be evaluated multiple times.
 */
#define CM_appendStringInfoCharMacro(str, ch)                             \
    (((str)->len + 1 >= (str)->maxlen) ? CM_appendStringInfoChar(str, ch) \
                                       : (void)((str)->data[(str)->len] = (ch), (str)->data[++(str)->len] = '\0'))

/*------------------------
 * appendBinaryStringInfo
 * Append arbitrary binary data to a StringInfo, allocating more space
 * if necessary.
 */
extern void CM_appendBinaryStringInfo(CM_StringInfo str, const char* data, int datalen);

/*------------------------
 * enlargeStringInfo
 * Make sure a StringInfo's buffer can hold at least 'needed' more bytes.
 */
extern int CM_enlargeStringInfo(CM_StringInfo str, int needed);

/*-----------------------
 * dupStringInfo
 * Get new StringInfo and copy the original to it.
 */
extern CM_StringInfo CM_dupStringInfo(CM_StringInfo orig);

/*------------------------
 * copyStringInfo
 * Copy StringInfo. Deep copy: Data will be copied too.
 * cursor of "to" will be initialized to zero.
 */
extern void CM_copyStringInfo(CM_StringInfo to, CM_StringInfo from);

extern int CM_is_str_all_digit(const char* name);
extern void CM_destroyStringInfo(CM_StringInfo str);
extern void CM_freeStringInfo(CM_StringInfo str);

#endif /* STRINGINFO_H */
