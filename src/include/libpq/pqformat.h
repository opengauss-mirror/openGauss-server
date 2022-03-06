/* -------------------------------------------------------------------------
 *
 * pqformat.h
 *		Definitions for formatting and parsing frontend/backend messages
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * Portions Copyright (c) 2021, openGauss Contributors
 * src/include/libpq/pqformat.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PQFORMAT_H
#define PQFORMAT_H

#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"

extern void pq_beginmessage(StringInfo buf, char msgtype);
extern void pq_beginmessage_reuse(StringInfo buf, char msgtype);
extern void pq_sendbytes(StringInfo buf, const char* data, int datalen);
extern void pq_sendcountedtext(StringInfo buf, const char* str, int slen, bool countincludesself);
extern void pq_sendtext(StringInfo buf, const char* str, int slen);
extern void pq_sendstring(StringInfo buf, const char* str);
extern void pq_send_ascii_string(StringInfo buf, const char* str);
extern void pq_sendfloat4(StringInfo buf, float4 f);
extern void pq_sendfloat8(StringInfo buf, float8 f);
extern void pq_endmessage(StringInfo buf);
extern void pq_endmessage_reuse(StringInfo buf);
extern void pq_endmessage_noblock(StringInfo buf);

extern void pq_begintypsend(StringInfo buf);
extern bytea* pq_endtypsend(StringInfo buf);

extern void pq_puttextmessage(char msgtype, const char* str);
extern void pq_puttextmessage_noblock(char msgtype, const char* str);
extern void pq_putemptymessage(char msgtype);
extern void pq_putemptymessage_noblock(char msgtype);

extern int pq_getmsgbyte(StringInfo msg);
extern unsigned int pq_getmsgint(StringInfo msg, int b);
extern int64 pq_getmsgint64(StringInfo msg);
extern int128 pq_getmsgint128(StringInfo msg);
extern float4 pq_getmsgfloat4(StringInfo msg);
extern float8 pq_getmsgfloat8(StringInfo msg);
extern const char* pq_getmsgbytes(StringInfo msg, int datalen);
extern void pq_copymsgbytes(StringInfo msg, char* buf, int datalen);
extern char* pq_getmsgtext(StringInfo msg, int rawbytes, int* nbytes);
extern const char* pq_getmsgstring(StringInfo msg);
extern void pq_getmsgend(StringInfo msg);

static inline uint128 pg_bswap128(uint128 x)
{
    return
    ((x << 120) & ((uint128)0xff) << 120) |
    ((x << 104) & ((uint128)0xff) << 112) |
    ((x <<  88) & ((uint128)0xff) << 104) |
    ((x <<  72) & ((uint128)0xff) << 96) |
    ((x <<  56) & ((uint128)0xff) << 88) |
    ((x <<  40) & ((uint128)0xff) << 80) |
    ((x <<  24) & ((uint128)0xff) << 72) |
    ((x <<   8) & ((uint128)0xff) << 64) |
    ((x >>   8) & ((uint128)0xff) << 56) |
    ((x >>  24) & ((uint128)0xff) << 48) |
    ((x >>  40) & ((uint128)0xff) << 40) |
    ((x <<  56) & ((uint128)0xff) << 32) |
    ((x <<  72) & ((uint128)0xff) << 24) |
    ((x <<  88) & ((uint128)0xff) << 16) |
    ((x << 104) & ((uint128)0xff) << 8) |
    ((x << 120) & 0xff);
}

#ifdef WORDS_BIGENDIAN
#define pg_hton128(x) (x)
#define pg_ntoh128(x) (x)
#else
#define pg_hton128(x) pg_bswap128(x)
#define pg_ntoh128(x) pg_bswap128(x)
#endif /* WORDS_BIGENDIAN */

/*
 * Append a [u]int8 to a StringInfo buffer, which already has enough space
 * preallocated.
 *
 * The use of restrict allows the compiler to optimize the code based on the
 * assumption that buf, buf->len, buf->data and *buf->data don't
 * overlap. Without the annotation buf->len etc cannot be kept in a register
 * over subsequent pq_writeintN calls.
 *
 * The use of StringInfoData * rather than StringInfo is due to MSVC being
 * overly picky and demanding a * before a restrict.
 */
static inline void pq_writeint8(StringInfoData* pg_restrict buf, uint8 i)
{
    uint8 ni = i;
    errno_t rc = EOK;

    rc = memcpy_s((char* pg_restrict)(buf->data + buf->len), sizeof(uint8), &ni, sizeof(uint8));
    securec_check(rc, "\0", "\0");
    buf->len += sizeof(uint8);
}

/*
 * Append a [u]int16 to a StringInfo buffer, which already has enough space
 * preallocated.
 */
static inline void pq_writeint16(StringInfoData* pg_restrict buf, uint16 i)
{
    uint16 ni = htons(i);
    errno_t rc = EOK;

    rc = memcpy_s((char* pg_restrict)(buf->data + buf->len), sizeof(uint16), &ni, sizeof(uint16));
    securec_check(rc, "\0", "\0");
    buf->len += sizeof(uint16);
}

/*
 * Append a [u]int32 to a StringInfo buffer, which already has enough space
 * preallocated.
 */
static inline void pq_writeint32(StringInfoData* pg_restrict buf, uint32 i)
{
    uint32 ni = htonl(i);
    errno_t rc = EOK;

    rc = memcpy_s((char* pg_restrict)(buf->data + buf->len), sizeof(uint32), &ni, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    buf->len += sizeof(uint32);
}

/*
 * Append a [u]int64 to a StringInfo buffer, which already has enough space
 * preallocated.
 */
static inline void pq_writeint64(StringInfoData* pg_restrict buf, uint64 i)
{
    uint32 n32;
    errno_t rc = EOK;

    /* High order half first, since we're doing MSB-first */
    n32 = (uint32)(i >> 32);
    n32 = htonl(n32);
    rc = memcpy_s((char* pg_restrict)(buf->data + buf->len), sizeof(uint32), &n32, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    buf->len += sizeof(uint32);

    /* Now the low order half */
    n32 = (uint32)i;
    n32 = htonl(n32);
    rc = memcpy_s((char* pg_restrict)(buf->data + buf->len), sizeof(uint32), &n32, sizeof(uint32));
    securec_check(rc, "\0", "\0");
    buf->len += sizeof(uint32);
}

/*
 * Append a [u]int128 to a StringInfo buffer, which already has enough space
 * preallocated.
 */
static inline void pq_writeint128(StringInfoData *pg_restrict buf, uint64 i)
{
    uint128 ni = pg_hton128(i);
    errno_t rc = EOK;
    Assert(buf->len + (int) sizeof(uint128) <= buf->maxlen);
    rc = memcpy_s((char *pg_restrict)(buf->data + buf->len), sizeof(uint128), &ni, sizeof(uint128));
    securec_check(rc, "\0", "\0");
    buf->len += sizeof(uint128);
}


/*
 * Append a null-terminated text string (with conversion) to a buffer with
 * preallocated space.
 *
 * NB: The pre-allocated space needs to be sufficient for the string after
 * converting to client encoding.
 *
 * NB: passed text string must be null-terminated, and so is the data
 * sent to the frontend.
 */
static inline void pq_writestring(StringInfoData* pg_restrict buf, const char* pg_restrict str)
{
    int slen = strlen(str);
    char* p = NULL;
    errno_t rc = EOK;

    p = pg_server_to_client(str, slen);
    if (p != str) /* actual conversion has been done? */
        slen = strlen(p);

    rc = memcpy_s(((char* pg_restrict)buf->data + buf->len), slen + 1, p, slen + 1);
    securec_check(rc, "\0", "\0");
    buf->len += slen + 1;

    if (p != str)
        pfree(p);
}

/* append a binary [u]int8 to a StringInfo buffer */
static inline void pq_sendint8(StringInfo buf, uint8 i)
{
    enlargeStringInfo(buf, sizeof(uint8));
    pq_writeint8(buf, i);
}

/* append a binary [u]int16 to a StringInfo buffer */
static inline void pq_sendint16(StringInfo buf, uint16 i)
{
    enlargeStringInfo(buf, sizeof(uint16));
    pq_writeint16(buf, i);
}

/* append a binary [u]int32 to a StringInfo buffer */
#ifndef ENABLE_UT
static
#endif
 inline void pq_sendint32(StringInfo buf, uint32 i)
{
    enlargeStringInfo(buf, sizeof(uint32));
    pq_writeint32(buf, i);
}

/* append a binary [u]int64 to a StringInfo buffer */
static inline void pq_sendint64(StringInfo buf, uint64 i)
{
    enlargeStringInfo(buf, sizeof(uint64));
    pq_writeint64(buf, i);
}

/* append a binary [u]int128 to a StringInfo buffer */
static inline void pq_sendint128(StringInfo buf, uint64 i)
{
    enlargeStringInfo(buf, sizeof(uint128));
    pq_writeint128(buf, i);
}

/* append a binary byte to a StringInfo buffer */
static inline void pq_sendbyte(StringInfo buf, uint8 byt)
{
    pq_sendint8(buf, byt);
}

/*
 * Append a binary integer to a StringInfo buffer
 *
 * This function is deprecated; prefer use of the functions above.
 */
static inline void pq_sendint(StringInfo buf, uint32 i, int b)
{
    switch (b) {
        case 1:
            pq_sendint8(buf, (uint8)i);
            break;
        case 2:
            pq_sendint16(buf, (uint16)i);
            break;
        case 4:
            pq_sendint32(buf, (uint32)i);
            break;
        default:
            elog(ERROR, "unsupported integer size %d", b);
            break;
    }
}

#endif /* PQFORMAT_H */
