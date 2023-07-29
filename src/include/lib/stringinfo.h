/* -------------------------------------------------------------------------
 *
 * stringinfo.h
 *	  Declarations/definitions for "StringInfo" functions.
 *
 * StringInfo provides an indefinitely-extensible string data type.
 * It can be used to buffer either ordinary C strings (null-terminated text)
 * or arbitrary binary data.  All storage is allocated with palloc().
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/lib/stringinfo.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef STRINGINFO_H
#define STRINGINFO_H

/* -------------------------
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
 * -------------------------
 */
typedef struct StringInfoData {
    char* data;
    int len;
    int maxlen;
    int cursor;
} StringInfoData;

typedef StringInfoData* StringInfo;

/* ------------------------
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
 * -------------------------
 */

/* ------------------------
 * makeStringInfo
 * Create an empty 'StringInfoData' & return a pointer to it.
 */
extern StringInfo makeStringInfo(void);

/* destroy a 'StringInfoData', use this function when call makeStringInfo */
extern void DestroyStringInfo(StringInfo str);

/* ------------------------
 * initStringInfo
 * Initialize a StringInfoData struct (with previously undefined contents)
 * to describe an empty string.
 */
extern void initStringInfo(StringInfo str);

/* free a 'StringInfoData', use this function when call initStringInfo */
extern void FreeStringInfo(StringInfo str);

/* ------------------------
 * resetStringInfo
 * Clears the current content of the StringInfo, if any. The
 * StringInfo remains valid.
 */
inline void resetStringInfo(StringInfo str)
{
    str->data[0] = '\0';
    str->len = 0;
    str->cursor = 0;
}

/* ------------------------
 * appendStringInfo
 * Format text data under the control of fmt (an sprintf-style format string)
 * and append it to whatever is already in str.  More space is allocated
 * to str if necessary.  This is sort of like a combination of sprintf and
 * strcat.
 */
extern void appendStringInfo(StringInfo str, const char* fmt, ...)
    /* This extension allows gcc to check the format string */
    __attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

/* ------------------------
 * appendStringInfoVA
 * Attempt to format text data under the control of fmt (an sprintf-style
 * format string) and append it to whatever is already in str.	If successful
 * return true; if not (because there's not enough space), return false
 * without modifying str.  Typically the caller would enlarge str and retry
 * on false return --- see appendStringInfo for standard usage pattern.
 */
extern bool appendStringInfoVA(StringInfo str, const char* fmt, va_list args)
    __attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 0)));

/* ------------------------
 * appendStringInfoString
 * Append a null-terminated string to str.
 * Like appendStringInfo(str, "%s", s) but faster.
 */
extern void appendStringInfoString(StringInfo str, const char* s);

/* ------------------------
 * appendStringInfoChar
 * Append a single byte to str.
 * Like appendStringInfo(str, "%c", ch) but much faster.
 */
extern void appendStringInfoChar(StringInfo str, char ch);

/* ------------------------
 * appendStringInfoCharMacro
 * As above, but a macro for even more speed where it matters.
 * Caution: str argument will be evaluated multiple times.
 */
#define appendStringInfoCharMacro(str, ch)                             \
    (((str)->len + 1 >= (str)->maxlen) ? appendStringInfoChar(str, ch) \
                                       : (void)((str)->data[(str)->len] = (ch), (str)->data[++(str)->len] = '\0'))

/* ------------------------
 * appendStringInfoSpaces
 * Append a given number of spaces to str.
 */
extern void appendStringInfoSpaces(StringInfo str, int count);

/* ------------------------
 * appendBinaryStringInfo
 * Append arbitrary binary data to a StringInfo, allocating more space
 * if necessary.
 */
extern void appendBinaryStringInfo(StringInfo str, const char* data, int datalen);

/*------------------------
 * appendBinaryStringInfoNT
 * Append arbitrary binary data to a StringInfo, allocating more space
 * if necessary. Does not ensure a trailing null-byte exists.
 */
extern void appendBinaryStringInfoNT(StringInfo str, const char *data, int datalen);

extern void copyStringInfo(StringInfo to, StringInfo from);

/* ------------------------
 * enlargeBuffer
 * Make sure a buffer can hold at least 'needed' more bytes.
 */
void enlargeBuffer(int needed, int len, int* maxlen, char** data);

void enlargeBufferSize(int needed, int len, size_t* maxlen, char** data);

/* ------------------------
 * enlargeStringInfo
 *
 * Make sure there is enough space for StringInfo
 *
 * External callers usually need not concern themselves with this, since
 * all stringinfo.c routines do it automatically.  However, if a caller
 * knows that a StringInfo will eventually become X bytes large, it
 * can save some palloc overhead by enlarging the buffer before starting
 * to store data in it.
 */
inline void enlargeStringInfo(StringInfo str, int needed)
{
    enlargeBuffer(needed, str->len, &str->maxlen, &str->data);
}

/*
 * The following function is used to request the use of more than 1GB of memory
 */

/* -------------------------
 * StringInfoDataHuge holds information about an extensible string.
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
 * -------------------------
 */
typedef struct StringInfoDataHuge {
    char* data;
    int64 len;
    int64 maxlen;
    int64 cursor;
} StringInfoDataHuge;

typedef StringInfoDataHuge* StringInfoHuge;


/* ------------------------
 * initStringInfoHuge
 * Initialize a StringInfoDataHuge struct (with previously undefined contents)
 * to describe an empty string.
 */
extern void initStringInfoHuge(StringInfoHuge str);

/* free a 'StringInfoDataHuge', use this function when call initStringInfo */
extern void FreeStringInfoHuge(StringInfoHuge str);

/* ------------------------
 * resetStringInfoHuge
 * Clears the current content of the StringInfoHuge, if any. The
 * StringInfo remains valid.
 */
extern void resetStringInfoHuge(StringInfoHuge str);

/* ------------------------
 * enlargeBufferHuge
 * Make sure a buffer can hold at least 'needed' more bytes.
 */
void enlargeBufferHuge(int64 needed, int64 len, int64* maxlen, char** data);

/* ------------------------
 * enlargeStringInfoHuge
 * Make sure a StringInfoHuge's buffer can hold at least 'needed' more bytes.
 */
extern void enlargeStringInfoHuge(StringInfoHuge str, int64 needed);

/* ------------------------
 * appendStringInfoHugeVA
 * Attempt to format text data under the control of fmt (an sprintf-style
 * format string) and append it to whatever is already in str.	If successful
 * return true; if not (because there's not enough space), return false
 * without modifying str.  Typically the caller would enlarge str and retry
 * on false return --- see appendStringInfo for standard usage pattern.
 */
extern bool appendStringInfoHugeVA(StringInfoHuge str, const char* fmt, va_list args)
    __attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 0)));

/* ------------------------
 * appendStringInfoHuge
 * Format text data under the control of fmt (an sprintf-style format string)
 * and append it to whatever is already in str.  More space is allocated
 * to str if necessary.  This is sort of like a combination of sprintf and
 * strcat.
 */
extern void appendStringInfoHuge(StringInfoHuge str, const char* fmt, ...)
    /* This extension allows gcc to check the format string */
    __attribute__((format(PG_PRINTF_ATTRIBUTE, 2, 3)));

extern char *psprintf(const char *fmt,...) __attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2))); 
#endif /* STRINGINFO_H */
