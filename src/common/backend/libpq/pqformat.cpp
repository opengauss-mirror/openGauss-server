/* -------------------------------------------------------------------------
 *
 * pqformat.cpp
 *		Routines for formatting and parsing frontend/backend messages
 *
 * Outgoing messages are built up in a StringInfo buffer (which is expansible)
 * and then sent in a single call to pq_putmessage.  This module provides data
 * formatting/conversion routines that are needed to produce valid messages.
 * Note in particular the distinction between "raw data" and "text"; raw data
 * is message protocol characters and binary values that are not subject to
 * character set conversion, while text is converted by character encoding
 * rules.
 *
 * Incoming messages are similarly read into a StringInfo buffer, via
 * pq_getmessage, and then parsed and converted from that using the routines
 * in this module.
 *
 * These same routines support reading and writing of external binary formats
 * (typsend/typreceive routines).  The conversion routines for individual
 * data types are exactly the same, only initialization and completion
 * are different.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *	src/common/backend/libpq/pqformat.cpp
 *
 * -------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 * Message assembly and output:
 *		pq_beginmessage - initialize StringInfo buffer
 *		pq_sendbyte		- append a raw byte to a StringInfo buffer
 *		pq_sendint		- append a binary integer to a StringInfo buffer
 *		pq_sendint64	- append a binary 8-byte int to a StringInfo buffer
 *		pq_sendfloat4	- append a float4 to a StringInfo buffer
 *		pq_sendfloat8	- append a float8 to a StringInfo buffer
 *		pq_sendbytes	- append raw data to a StringInfo buffer
 *		pq_sendcountedtext - append a counted text string (with character set conversion)
 *		pq_sendtext		- append a text string (with conversion)
 *		pq_sendstring	- append a null-terminated text string (with conversion)
 *		pq_send_ascii_string - append a null-terminated text string (without conversion)
 *		pq_endmessage	- send the completed message to the frontend
 * Note: it is also possible to append data to the StringInfo buffer using
 * the regular StringInfo routines, but this is discouraged since required
 * character set conversion may not occur.
 *
 * typsend support (construct a bytea value containing external binary data):
 *		pq_begintypsend - initialize StringInfo buffer
 *		pq_endtypsend	- return the completed string as a "bytea*"
 *
 * Special-case message output:
 *		pq_puttextmessage - generate a character set-converted message in one step
 *		pq_putemptymessage - convenience routine for message with empty body
 *
 * Message parsing after input:
 *		pq_getmsgbyte	- get a raw byte from a message buffer
 *		pq_getmsgint	- get a binary integer from a message buffer
 *		pq_getmsgint64	- get a binary 8-byte int from a message buffer
 *		pq_getmsgfloat4 - get a float4 from a message buffer
 *		pq_getmsgfloat8 - get a float8 from a message buffer
 *		pq_getmsgbytes	- get raw data from a message buffer
 *		pq_copymsgbytes - copy raw data from a message buffer
 *		pq_getmsgtext	- get a counted text string (with conversion)
 *		pq_getmsgstring - get a null-terminated text string (with conversion)
 *		pq_getmsgend	- verify message fully consumed
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include <sys/param.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "libpq/libpq.h"
#include "libpq/pqformat.h"

/* --------------------------------
 *		pq_beginmessage		- initialize for sending a message
 * --------------------------------
 */
void pq_beginmessage(StringInfo buf, char msgtype)
{
    initStringInfo(buf);

    /*
     * We stash the message type into the buffer's cursor field, expecting
     * that the pq_sendXXX routines won't touch it.  We could alternatively
     * make it the first byte of the buffer contents, but this seems easier.
     */
    buf->cursor = msgtype;
}

/* --------------------------------
 *		pq_beginmessage_reuse - initialize for sending a message, reuse buffer
 *
 * This requires the buffer to be allocated in an sufficiently long-lived
 * memory context.
 * --------------------------------
 */
void pq_beginmessage_reuse(StringInfo buf, char msgtype)
{
    resetStringInfo(buf);

    /*
     * We stash the message type into the buffer's cursor field, expecting
     * that the pq_sendXXX routines won't touch it.  We could alternatively
     * make it the first byte of the buffer contents, but this seems easier.
     */
    buf->cursor = msgtype;
}

/* --------------------------------
 *		pq_sendbytes	- append raw data to a StringInfo buffer
 * --------------------------------
 */
void pq_sendbytes(StringInfo buf, const char* data, int datalen)
{
    /* use variant that maintains a trailing null-byte, out of caution */
    appendBinaryStringInfo(buf, data, datalen);
}

/* --------------------------------
 *		pq_sendcountedtext - append a counted text string (with character set conversion)
 *
 * The data sent to the frontend by this routine is a 4-byte count field
 * followed by the string.	The count includes itself or not, as per the
 * countincludesself flag (pre-3.0 protocol requires it to include itself).
 * The passed text string need not be null-terminated, and the data sent
 * to the frontend isn't either.
 * --------------------------------
 */
void pq_sendcountedtext(StringInfo buf, const char* str, int slen, bool countincludesself)
{
    int extra = countincludesself ? 4 : 0;
    char* p = NULL;

    p = pg_server_to_client(str, slen);
    if (p != str) { /* actual conversion has been done? */
        slen = strlen(p);
        pq_sendint32(buf, slen + extra);
        appendBinaryStringInfo(buf, p, slen);
        pfree(p);
        p = NULL;
    } else {
        pq_sendint32(buf, slen + extra);
        appendBinaryStringInfo(buf, str, slen);
    }
}

void pq_sendcountedtext_printtup(StringInfo buf, const char* str, int slen, int src_encoding, void* convert_finfo)
{
    char* p = (char*)str;

    if (unlikely(src_encoding != u_sess->mb_cxt.ClientEncoding->encoding)) {
        p = pg_any_to_client(str, slen, src_encoding, convert_finfo);
    }
    if (unlikely(p != str)) { /* actual conversion has been done? */
        slen = strlen(p);
        enlargeBuffer(slen + sizeof(uint32), buf->len, &buf->maxlen, &buf->data);
        pq_writeint32(buf, (uint32)slen);
        errno_t rc = memcpy_sp(buf->data + buf->len, (size_t)(buf->maxlen - buf->len), p, (size_t)slen);
        securec_check(rc, "\0", "\0");
        buf->len += slen;
        buf->data[buf->len] = '\0';
        pfree(p);
        p = NULL;
    } else {
        enlargeBuffer(slen + sizeof(uint32), buf->len, &buf->maxlen, &buf->data);
        pq_writeint32(buf, (uint32)slen);
        errno_t rc = memcpy_sp(buf->data + buf->len, (size_t)(buf->maxlen - buf->len), str, (size_t)slen);
        securec_check(rc, "\0", "\0");
        buf->len += slen;
        buf->data[buf->len] = '\0';
    }
}


/* --------------------------------
 *		pq_sendtext		- append a text string (with conversion)
 *
 * The passed text string need not be null-terminated, and the data sent
 * to the frontend isn't either.  Note that this is not actually useful
 * for direct frontend transmissions, since there'd be no way for the
 * frontend to determine the string length.  But it is useful for binary
 * format conversions.
 * --------------------------------
 */
void pq_sendtext(StringInfo buf, const char* str, int slen)
{
    char* p = NULL;

    p = pg_server_to_client(str, slen);
    if (p != str) /* actual conversion has been done? */
    {
        slen = strlen(p);
        appendBinaryStringInfo(buf, p, slen);
        pfree(p);
        p = NULL;
    } else {
        appendBinaryStringInfo(buf, str, slen);
    }
}

/* --------------------------------
 *		pq_sendstring	- append a null-terminated text string (with conversion)
 *
 * NB: passed text string must be null-terminated, and so is the data
 * sent to the frontend.
 * --------------------------------
 */
void pq_sendstring(StringInfo buf, const char* str)
{
    int slen = strlen(str);
    char* p = NULL;

    p = pg_server_to_client(str, slen);
    if (p != str) { /* actual conversion has been done? */
        slen = strlen(p);
        appendBinaryStringInfo(buf, p, slen + 1);
        pfree(p);
        p = NULL;
    } else {
        appendBinaryStringInfo(buf, str, slen + 1);
    }
}

/* --------------------------------
 *		pq_send_ascii_string	- append a null-terminated text string (without conversion)
 *
 * This function intentionally bypasses encoding conversion, instead just
 * silently replacing any non-7-bit-ASCII characters with question marks.
 * It is used only when we are having trouble sending an error message to
 * the client with normal localization and encoding conversion.  The caller
 * should already have taken measures to ensure the string is just ASCII;
 * the extra work here is just to make certain we don't send a badly encoded
 * string to the client (which might or might not be robust about that).
 *
 * NB: passed text string must be null-terminated, and so is the data
 * sent to the frontend.
 * --------------------------------
 */
void pq_send_ascii_string(StringInfo buf, const char* str)
{
    while (*str) {
        char ch = *str++;

        if (IS_HIGHBIT_SET(ch)) {
            ch = '?';
        }
        appendStringInfoCharMacro(buf, ch);
    }
    appendStringInfoChar(buf, '\0');
}

/* --------------------------------
 *		pq_sendfloat4	- append a float4 to a StringInfo buffer
 *
 * The point of this routine is to localize knowledge of the external binary
 * representation of float4, which is a component of several datatypes.
 *
 * We currently assume that float4 should be byte-swapped in the same way
 * as int4.  This rule is not perfect but it gives us portability across
 * most IEEE-float-using architectures.
 * --------------------------------
 */
void pq_sendfloat4(StringInfo buf, float4 f)
{
    union {
        float4 f;
        uint32 i;
    } swap = { .i = 0 };

    swap.f = f;
    pq_sendint32(buf, swap.i);
}

/* --------------------------------
 *		pq_sendfloat8	- append a float8 to a StringInfo buffer
 *
 * The point of this routine is to localize knowledge of the external binary
 * representation of float8, which is a component of several datatypes.
 *
 * We currently assume that float8 should be byte-swapped in the same way
 * as int8.  This rule is not perfect but it gives us portability across
 * most IEEE-float-using architectures.
 * --------------------------------
 */
void pq_sendfloat8(StringInfo buf, float8 f)
{
    union {
        float8 f;
        int64 i;
    } swap = { .i = 0 };

    swap.f = f;
    pq_sendint64(buf, swap.i);
}

/* --------------------------------
 *		pq_endmessage	- send the completed message to the frontend
 *
 * The data buffer is pfree()d, but if the StringInfo was allocated with
 * makeStringInfo then the caller must still pfree it.
 * --------------------------------
 */
void pq_endmessage(StringInfo buf)
{
    /* msgtype was saved in cursor field */
    (void)pq_putmessage(buf->cursor, buf->data, buf->len);
    /* no need to complain about any failure, since pqcomm.c already did */
    pfree(buf->data);
    buf->data = NULL;
}

/* --------------------------------
 *		pq_endmessage_noblock - Same as pq_endmessage but non-blocking
 * --------------------------------
 */
void pq_endmessage_noblock(StringInfo buf)
{
    /* msgtype was saved in cursor field */
    (void)pq_putmessage_noblock(buf->cursor, buf->data, buf->len);
    /* no need to complain about any failure, since pqcomm.c already did */
    pfree(buf->data);
    buf->data = NULL;
}

/* --------------------------------
 *		pq_begintypsend		- initialize for constructing a bytea result
 * --------------------------------
 */
void pq_begintypsend(StringInfo buf)
{
    initStringInfo(buf);
    /* Reserve four bytes for the bytea length word */
    appendStringInfoCharMacro(buf, '\0');
    appendStringInfoCharMacro(buf, '\0');
    appendStringInfoCharMacro(buf, '\0');
    appendStringInfoCharMacro(buf, '\0');
}

/* --------------------------------
 *		pq_endtypsend	- finish constructing a bytea result
 *
 * The data buffer is returned as the palloc'd bytea value.  (We expect
 * that it will be suitably aligned for this because it has been palloc'd.)
 * We assume the StringInfoData is just a local variable in the caller and
 * need not be pfree'd.
 * --------------------------------
 */
bytea* pq_endtypsend(StringInfo buf)
{
    bytea* result = (bytea*)buf->data;

    /* Insert correct length into bytea length word */
    Assert(buf->len >= VARHDRSZ);
    SET_VARSIZE(result, buf->len);

    return result;
}

/* --------------------------------
 *		pq_puttextmessage - generate a character set-converted message in one step
 *
 *		This is the same as the pqcomm.c routine pq_putmessage, except that
 *		the message body is a null-terminated string to which encoding
 *		conversion applies.
 * --------------------------------
 */
void pq_puttextmessage(char msgtype, const char* str)
{
    int slen = strlen(str);
    char* p = NULL;

    p = pg_server_to_client(str, slen);
    if (p != str) { /* actual conversion has been done? */
        (void)pq_putmessage(msgtype, p, strlen(p) + 1);
        pfree(p);
        p = NULL;
        return;
    }
    (void)pq_putmessage(msgtype, str, slen + 1);
}

/* --------------------------------
 *		pq_puttextmessage_noblock - Same as pq_puttextmessage but non-blocking
 * --------------------------------
 */
void pq_puttextmessage_noblock(char msgtype, const char* str)
{
    int slen = strlen(str);
    char* p = NULL;

    p = pg_server_to_client(str, slen);
    if (p != str) { /* actual conversion has been done? */
        (void)pq_putmessage_noblock(msgtype, p, strlen(p) + 1);
        pfree(p);
        p = NULL;
        return;
    }
    (void)pq_putmessage_noblock(msgtype, str, slen + 1);
}

/* --------------------------------
 *		pq_putemptymessage - convenience routine for message with empty body
 * --------------------------------
 */
void pq_putemptymessage(char msgtype)
{
    (void)pq_putmessage(msgtype, NULL, 0);
}

/* --------------------------------
 *		pq_putemptymessage_noblock - Same as pq_putemptymessage but non-block
 * --------------------------------
 */
void pq_putemptymessage_noblock(char msgtype)
{
    (void)pq_putmessage_noblock(msgtype, NULL, 0);
}

/* --------------------------------
 *		pq_getmsgbyte	- get a raw byte from a message buffer
 * --------------------------------
 */
int pq_getmsgbyte(StringInfo msg)
{
    if (unlikely(msg->cursor >= msg->len)) {
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("no data left in message")));
    }
    return (unsigned char)msg->data[msg->cursor++];
}

/* --------------------------------
 *		pq_getmsgint	- get a binary integer from a message buffer
 *
 *		Values are treated as unsigned.
 * --------------------------------
 */
unsigned int pq_getmsgint(StringInfo msg, int b)
{
    unsigned int result;
    unsigned char n8;
    uint16 n16;
    uint32 n32;

    switch (b) {
        case 1:
            pq_copymsgbytes(msg, (char*)&n8, 1);
            result = n8;
            break;
        case 2:
            pq_copymsgbytes(msg, (char*)&n16, 2);
            result = ntohs(n16);
            break;
        case 4:
            pq_copymsgbytes(msg, (char*)&n32, 4);
            result = ntohl(n32);
            break;
        default:
            ereport(ERROR, (errmsg("unsupported integer size %d", b)));
            result = 0; /* keep compiler quiet */
            break;
    }
    return result;
}

/* --------------------------------
 *		pq_getmsgint64	- get a binary 8-byte int from a message buffer
 *
 * It is tempting to merge this with pq_getmsgint, but we'd have to make the
 * result int64 for all data widths --- that could be a big performance
 * hit on machines where int64 isn't efficient.
 * --------------------------------
 */
int64 pq_getmsgint64(StringInfo msg)
{
    uint64 result;
    uint32 h32;
    uint32 l32;

    pq_copymsgbytes(msg, (char*)&h32, 4);
    pq_copymsgbytes(msg, (char*)&l32, 4);
    h32 = ntohl(h32);
    l32 = ntohl(l32);

    result = h32;
    result <<= 32;
    result |= l32;

    return (int64)result;
}

/* --------------------------------
 *		pq_getmsgint128	- get a binary 16-byte int from a message buffer
 *
 * It is tempting to merge this with pq_getmsgint, but we'd have to make the
 * result int128 for all data widths --- that could be a big performance
 * hit on machines where int64 isn't efficient.
 * --------------------------------
 */
int128 pq_getmsgint128(StringInfo msg)
{
    uint128 result;
    const int int128bytes = 4;

    /* [hh h l ll] -> int128 */
    uint32 hh32;
    uint32 h32;
    uint32 l32;
    uint32 ll32;
    
    pq_copymsgbytes(msg, (char*)&hh32, int128bytes);
    pq_copymsgbytes(msg, (char*)&h32, int128bytes);
    pq_copymsgbytes(msg, (char*)&l32, int128bytes);
    pq_copymsgbytes(msg, (char*)&ll32, int128bytes);
    hh32 = ntohl(hh32);
    h32 = ntohl(h32);
    l32 = ntohl(l32);
    ll32 = ntohl(ll32);

    result = hh32;
    result <<= INT128_HALF_HIGH_NBYTES;
    result |= h32;
    result <<= INT128_HALF_HIGH_NBYTES;
    result |= l32;
    result <<= INT128_HALF_HIGH_NBYTES;
    result |= ll32;

    return (int128)result;
}

/* --------------------------------
 *		pq_getmsgfloat4 - get a float4 from a message buffer
 *
 * See notes for pq_sendfloat4.
 * --------------------------------
 */
float4 pq_getmsgfloat4(StringInfo msg)
{
    union {
        float4 f;
        uint32 i;
    } swap = { .i = 0 };

    swap.i = pq_getmsgint(msg, 4);
    return swap.f;
}

/* --------------------------------
 *		pq_getmsgfloat8 - get a float8 from a message buffer
 *
 * See notes for pq_sendfloat8.
 * --------------------------------
 */
float8 pq_getmsgfloat8(StringInfo msg)
{
    union {
        float8 f;
        int64 i;
    } swap = { .i = 0 };

    swap.i = pq_getmsgint64(msg);
    return swap.f;
}

/* --------------------------------
 *		pq_getmsgbytes	- get raw data from a message buffer
 *
 *		Returns a pointer directly into the message buffer; note this
 *		may not have any particular alignment.
 * --------------------------------
 */
const char* pq_getmsgbytes(StringInfo msg, int datalen)
{
    if (datalen < 0 || datalen > (msg->len - msg->cursor)) {
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("insufficient data left in message")));
    }
    const char* result = &msg->data[msg->cursor];
    msg->cursor += datalen;
    return result;
}

/* --------------------------------
 *		pq_copymsgbytes - copy raw data from a message buffer
 *
 *		Same as above, except data is copied to caller's buffer.
 * --------------------------------
 */
void pq_copymsgbytes(StringInfo msg, char* buf, int datalen)
{
    if (datalen < 0 || datalen > (msg->len - msg->cursor)) {
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("insufficient data left in message")));
    }
    if (datalen > 0) {
        int rcs = memcpy_s(buf, datalen, &msg->data[msg->cursor], datalen);
        securec_check(rcs, "\0", "\0");
        msg->cursor += datalen;
    }
}

/* --------------------------------
 *		pq_getmsgtext	- get a counted text string (with conversion)
 *
 *		Always returns a pointer to a freshly palloc'd result.
 *		The result has a trailing null, *and* we return its strlen in *nbytes.
 * --------------------------------
 */
char* pq_getmsgtext(StringInfo msg, int rawbytes, int* nbytes)
{
    char* str = NULL;
    char* p = NULL;

    if (rawbytes < 0 || rawbytes > (msg->len - msg->cursor)) {
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("insufficient data left in message")));
    }
    str = &msg->data[msg->cursor];
    msg->cursor += rawbytes;

    p = pg_client_to_server(str, rawbytes);
    if (p != str) { /* actual conversion has been done? */
        *nbytes = strlen(p);
    }
    else {
        p = (char*)palloc(rawbytes + 1);
        if (rawbytes > 0) {
            int rcs = memcpy_s(p, rawbytes + 1, str, rawbytes);
            securec_check(rcs, "\0", "\0");
        }
        p[rawbytes] = '\0';
        *nbytes = rawbytes;
    }
    return p;
}

/* --------------------------------
 *		pq_getmsgstring - get a null-terminated text string (with conversion)
 *
 *		May return a pointer directly into the message buffer, or a pointer
 *		to a palloc'd conversion result.
 * --------------------------------
 */
const char* pq_getmsgstring(StringInfo msg)
{
    char* str = NULL;
    int slen;

    str = &msg->data[msg->cursor];

    /*
     * It's safe to use strlen() here because a StringInfo is guaranteed to
     * have a trailing null byte.  But check we found a null inside the
     * message.
     */
    slen = strlen(str);
    if (msg->cursor + slen >= msg->len) {
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("invalid string in message")));
    }
    msg->cursor += slen + 1;

    return pg_client_to_server(str, slen);
}

/* --------------------------------
 *		pq_getmsgend	- verify message fully consumed
 * --------------------------------
 */
void pq_getmsgend(StringInfo msg)
{
    if (msg->cursor != msg->len) {
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION), errmsg("invalid message format")));
    }
}
