/*------------------------------------------------------------------------------
 * gms_lob.cpp
 *
 * gms_lob内置包的实现
 *
 * Copyright (c) 2002-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * IDENTIFICATION
 *      contrib/gms_stats/gms_lob.cpp
 *
 *------------------------------------------------------------------------------
 */
#include "postgres.h"

#include "knl/knl_session.h"
#include "utils/memutils.h"
#include "catalog/pg_proc.h"
#include "c.h"
#include "miscadmin.h"
#include "access/xact.h"
#include "access/hash.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "access/genam.h"
#include "utils/lsyscache.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "executor/executor.h"
#include "catalog/storage_gtt.h"
#include "utils/numeric.h"
#include "access/tuptoaster.h"

#include "funcapi.h"
#include "fmgr.h"
#include "catalog/pg_directory.h"
#include "mb/pg_wchar.h"
#include "libpq/pqformat.h"
#include "storage/ipc.h"
#include "utils/acl.h"
#include "utils/bytea.h"
#include "libpq/be-fsstubs.h"
#include "libpq/libpq-fs.h"
#include "commands/extension.h"
#include "gms_lob.h"
#include "utils/palloc.h"

PG_MODULE_MAGIC;

#define NUMLOB 64
static uint32 gmslob_index;

PG_FUNCTION_INFO_V1(gms_lob_og_createtemporary);
PG_FUNCTION_INFO_V1(gms_lob_og_freetemporary);
PG_FUNCTION_INFO_V1(gms_lob_og_read_blob);
PG_FUNCTION_INFO_V1(gms_lob_og_read_clob);
PG_FUNCTION_INFO_V1(gms_lob_og_write_blob);
PG_FUNCTION_INFO_V1(gms_lob_og_write_clob);
PG_FUNCTION_INFO_V1(gms_lob_og_isopen);
PG_FUNCTION_INFO_V1(gms_lob_og_open);
PG_FUNCTION_INFO_V1(gms_lob_og_append_blob);
PG_FUNCTION_INFO_V1(gms_lob_og_append_clob);
PG_FUNCTION_INFO_V1(gms_lob_og_close);
PG_FUNCTION_INFO_V1(gms_lob_og_cloblength);
PG_FUNCTION_INFO_V1(gms_lob_og_bloblength);
PG_FUNCTION_INFO_V1(gms_lob_og_null);

void set_extension_index(uint32 index)
{
    gmslob_index = index;
}

void init_session_vars(void) {
    RepallocSessionVarsArrayIfNecessary();
    GmsLobContext* psc = 
        (GmsLobContext*)MemoryContextAllocZero(u_sess->self_mem_cxt, sizeof(GmsLobContext));
    u_sess->attr.attr_common.extension_session_vars_array[gmslob_index] = psc;
    psc->gmsLobNameHash = NULL;
}

GmsLobContext* get_session_context() {
    if (u_sess->attr.attr_common.extension_session_vars_array[gmslob_index] == NULL) {
        init_session_vars();
    }
    return (GmsLobContext*)u_sess->attr.attr_common.extension_session_vars_array[gmslob_index];
}

static int32 getVarSize(varlena* var)
{
    if (VARATT_IS_HUGE_TOAST_POINTER(var)) {
        struct varatt_lob_external large_toast_pointer;

        VARATT_EXTERNAL_GET_HUGE_POINTER(large_toast_pointer, var);
        return large_toast_pointer.va_rawsize;
    } else {
        return VARSIZE_ANY_EXHDR(var);
    }
}

/*
 * charlen_to_bytelen()
 *	Compute the number of bytes occupied by n characters starting at *p
 *
 * It is caller's responsibility that there actually are n characters;
 * the string need not be null-terminated.
 */
static int charlen_to_bytelen(const char* p, int n)
{
    if (pg_database_encoding_max_length() == 1) {
        /* Optimization for single-byte encodings */
        return n;
    } else {
        const char* s = NULL;

        for (s = p; n > 0; n--)
            s += pg_mblen(s);

        return s - p;
    }
}

/* numeric 向下取整转成int4 */
int32 numericFloorToInt4(Numeric num)
{
    Datum datumnum = DirectFunctionCall1(numeric_floor, NumericGetDatum(num));
    return DatumGetInt32(DirectFunctionCall1(numeric_int4, datumnum));
}
/*****************************************************************************
 * LOB哈希表相关
 *****************************************************************************/
struct GmsLobHashKey {
    char* keyvalue;
    int length;
};

typedef struct LobHashEnt {
    GmsLobHashKey key;       /* hash表key值 */
    int open_mode;   /* 打开方式 */
} LobHashEnt;
/*
 * lob数据的hash值生成函数
 */
uint32 lob_hash(const void* key, Size kwysize)
{
    const GmsLobHashKey* hash_key = (const GmsLobHashKey*)key;
    return DatumGetUInt32(hash_any((const unsigned char*)hash_key->keyvalue, hash_key->length));
}
/*
 * lob数据的hash比较函数
 */
int lob_match(const void* key1, const void* key2, Size kwysize)
{
    const GmsLobHashKey* k1 = (const GmsLobHashKey*)key1;
    const GmsLobHashKey* k2 = (const GmsLobHashKey*)key2;

    if (k1->length > k2->length) {
        return 1;
    } else if (k1->length < k2->length) {
        return -1;
    }

    return strncmp(k1->keyvalue, k2->keyvalue, k1->length);
}
/*
 * 创建hash表
 */
static HTAB* createlobHash()
{
    HASHCTL hash_ctl;

    hash_ctl.keysize = sizeof(GmsLobHashKey);
    hash_ctl.entrysize = sizeof(LobHashEnt);
    hash_ctl.hash = lob_hash;
    hash_ctl.match = lob_match;

    return hash_create("Lob hash", NUMLOB, &hash_ctl, HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);
}

/* 
 * 程序退出时的清理函数
 * 作为回调函数添加到proc_exit()调用的函数列表中
 */
static void gms_lob_og_lob_exit(int code, Datum arg)
{
    HASH_SEQ_STATUS scan;
    LobHashEnt *hentry = NULL;

    if (u_sess->attr.attr_common.extension_session_vars_array == NULL) {
        return;
    }

    if (get_session_context()->gmsLobNameHash == NULL) {
        return;
    }

    hash_seq_init(&scan, get_session_context()->gmsLobNameHash);
    while ((hentry = (LobHashEnt *)hash_seq_search(&scan))) {
        if (hentry->key.keyvalue) {
            pfree(hentry->key.keyvalue);
        }
    }
    /* 清理hash表 */
    hash_destroy(get_session_context()->gmsLobNameHash);
    get_session_context()->gmsLobNameHash = NULL;
}

char* generateLobKey(char* argname)
{
    char* key = NULL;
    int keylen = 0;
    if (strcmp(argname, ":") == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("output parameter not a bind variable")));
    }

    TransactionId currentTransactionId = GetCurrentTransactionId();
    // TransactionId采用16进制方式打印，逗号末尾结束符各占一个字节
    keylen = strlen(argname) + sizeof(TransactionId) * 2 + 1 + 1;
    key = (char*)palloc0(keylen);
    errno_t errorno = snprintf_s(key, keylen, keylen - 1, "%16x,%s", currentTransactionId, argname);
    securec_check_ss(errorno, "\0", "\0");
    return key;
}

int searchLob(char* argname, bool* found)
{
    LobHashEnt* hentry = NULL;
    GmsLobHashKey hash_key;
    if (get_session_context()->gmsLobNameHash) {
        hash_key.keyvalue = generateLobKey(argname);
        hash_key.length = strlen(hash_key.keyvalue) + 1;
        hentry = (LobHashEnt*)hash_search(get_session_context()->gmsLobNameHash, &hash_key, HASH_FIND, NULL);
        if (hentry) {
            *found = true;
            return hentry->open_mode;
        }

        if (hash_key.keyvalue) {
            pfree(hash_key.keyvalue);
        }
    } else {
        get_session_context()->gmsLobNameHash = createlobHash();
        on_proc_exit(&gms_lob_og_lob_exit, PointerGetDatum(NULL));
    }
    *found = false;
    return -1;
}

static void closeLob(char* argname)
{
    bool found;
    LobHashEnt* hentry = NULL;
    GmsLobHashKey hash_key;

    searchLob(argname, &found);
    if (found) {
        hash_key.keyvalue = generateLobKey(argname);
        hash_key.length = strlen(hash_key.keyvalue) + 1;
        hentry = (LobHashEnt*)hash_search(get_session_context()->gmsLobNameHash, &hash_key, HASH_REMOVE, NULL);
        if (!hentry) {
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("undefined lob.")));
        }

        if (hash_key.keyvalue) {
            pfree(hash_key.keyvalue);
        }

        if (hentry->key.keyvalue) {
            pfree(hentry->key.keyvalue);
        }
    }
}

static void openLob(char* argname, int openmode)
{
    bool found;
    LobHashEnt* hentry = NULL;
    GmsLobHashKey hash_key;

    hash_key.keyvalue = generateLobKey(argname);
    hash_key.length = strlen(hash_key.keyvalue) + 1;
    hentry = (LobHashEnt*)hash_search(get_session_context()->gmsLobNameHash, &hash_key, HASH_ENTER, &found);
    if (found) {
        closeLob(argname);
        ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT), 
                 errmsg("duplicate lob(%s) openned", argname)));
    }

    hentry->key.keyvalue = (char*)MemoryContextAlloc(THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR), hash_key.length);
    int rc = memcpy_s(hentry->key.keyvalue, hash_key.length, hash_key.keyvalue, hash_key.length);
    securec_check(rc, "\0", "\0");
    hentry->key.length = hash_key.length;
    hentry->open_mode = openmode;

    if (hash_key.keyvalue) {
        pfree(hash_key.keyvalue);
    }
}

/*
 * GMS_LOB.CREATETEMPORARY (
 *  lob_loc IN OUT    BLOB/CLOB,   --blob/clob对象
 *  cache   IN        BOOLEAN,     --是否将LOB读取到缓冲区（不生效）
 *  dur     IN        PLS_INTEGER := GMS_LOB.SESSION);  --指定何时清除临时LOB（10/ SESSION：会话结束时；12/ CALL：调用结束时）(不生效
)
 */
Datum gms_lob_og_createtemporary(PG_FUNCTION_ARGS)
{
    char* argname = text_to_cstring(PG_GETARG_TEXT_P(3));
    if (strcmp(argname, ":") == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("output parameter not a bind variable")));
    }

    varlena* res = NULL;
    if (PG_ARGISNULL(0)) {
        res = (varlena*)palloc(VARHDRSZ);
        SET_VARSIZE(res, VARHDRSZ);
        PG_RETURN_POINTER(res);
    }

    PG_RETURN_DATUM(PG_GETARG_DATUM(0));
}

/*
 * GMS_LOB.FREETEMPORARY (
 *  lob_loc IN OUT    BLOB/CLOB);   --blob/clob对象
 */
Datum gms_lob_og_freetemporary(PG_FUNCTION_ARGS)
{
    char* argname = text_to_cstring(PG_GETARG_TEXT_P(1));
    if (strcmp(argname, ":") == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("output parameter not a bind variable")));
    }
    /* lob如果没关闭，将其关闭 */
    closeLob(argname);

    /* 返回一个空值的varlena */
    varlena* res = (varlena*)palloc0(VARHDRSZ);
    SET_VARSIZE(res, VARHDRSZ);

    PG_RETURN_DATUM(PointerGetDatum(res));
}

/*
 *  lob_loc   IN              BLOB,
 *  amount    IN OUT   NOCOPY BINARY_INTEGER,
 *  offset    IN              INTEGER,
 *  buffer    INOUT             RAW
 */
Datum gms_lob_og_read_blob(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0))
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid LOB object specified")));

    if (PG_ARGISNULL(1) || PG_ARGISNULL(2)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("numeric or value error")));
    }

    bytea *src_lob = PG_GETARG_BYTEA_P(0);
    int32 amount = PG_GETARG_INT32(1);
    int32 offset = PG_GETARG_INT32(2);

    if (amount < 1 || amount > AMOUNT_MAX_SIZE) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("amount is invalid or out of range")));
    }
    if (offset < 1 || offset > LOBMAXSIZE) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("offset is invalid or out of range")));
    }

    int32 srclen = VARSIZE(src_lob) - VARHDRSZ;

    if (offset > srclen) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("no data found")));
    }

    int32 srcamount = amount > (srclen - offset + 1) ? (srclen - offset + 1) : amount; //src实际复制的字节数
    bytea* res = NULL;
    int rc = 0;
    res = (bytea*)palloc0(srcamount + VARHDRSZ);
    SET_VARSIZE(res, srcamount + VARHDRSZ);
    rc = memcpy_s(VARDATA(res), srcamount, VARDATA(src_lob) + offset - 1, srcamount);
    securec_check(rc, "\0", "\0");

    TupleDesc   tupdesc;
    Datum       result;
    HeapTuple   tuple;
    Datum       values[2];
    bool        nulls[2] = { false, false };
    /* 构造返回结果集 */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
        elog(ERROR, "return type must be a row type");
    }

    /*拼接结果*/
    values[0] = Int32GetDatum(srcamount);
    values[1] = PointerGetDatum(res);
    tuple = heap_form_tuple(tupdesc, values, nulls);
    result = HeapTupleGetDatum(tuple);

    PG_RETURN_DATUM(result);
}

/*
 *  lob_loc   IN              CLOB,
 *  amount    IN OUT   NOCOPY BINARY_INTEGER,
 *  offset    IN              INTEGER,
 *  buffer    INOUT             VARCHAR2
 */
Datum gms_lob_og_read_clob(PG_FUNCTION_ARGS)
{
    if (PG_ARGISNULL(0))
        ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),errmsg("invalid LOB object specified")));

    if (PG_ARGISNULL(1) || PG_ARGISNULL(2)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("numeric or value error")));
    }

    text* src_lob = PG_GETARG_TEXT_P(0);
    int32 amount = PG_GETARG_INT32(1);
    int32 offset = PG_GETARG_INT32(2);

    if (amount < 1 || amount > AMOUNT_MAX_SIZE) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("amount is invalid or out of range")));
    }

    if (offset < 1 || offset > LOBMAXSIZE) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("offset is invalid or out of range")));
    }

    int32 srclen = DirectFunctionCall1(textlen, PointerGetDatum(src_lob));

    if (offset > srclen) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("no data found")));
    }

    int32 srcamount = amount > (srclen - offset + 1) ? (srclen - offset + 1) : amount; //src实际复制的字符数
    int32 srcbytestart = charlen_to_bytelen(VARDATA(src_lob), offset - 1); //src复制起点
    int32 copybyte = charlen_to_bytelen(VARDATA(src_lob) + srcbytestart, srcamount); //src实际复制的字节数
    text* res = NULL;
    int rc = 0;

    res = (text*)palloc0(copybyte + VARHDRSZ);
    SET_VARSIZE(res, copybyte + VARHDRSZ);
    rc = memcpy_s(VARDATA(res), copybyte, VARDATA(src_lob) + srcbytestart, copybyte);
    securec_check(rc, "\0", "\0");

    TupleDesc   tupdesc;
    Datum       result;
    HeapTuple   tuple;
    Datum       values[2];
    bool        nulls[2] = { false, false };
    /* 构造返回结果集 */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
        elog(ERROR, "return type must be a row type");
    }

    /*拼接结果*/
    values[0] = Int32GetDatum(srcamount);
    values[1] = PointerGetDatum(res);

    tuple = heap_form_tuple(tupdesc, values, nulls);
    result = HeapTupleGetDatum(tuple);

    PG_RETURN_DATUM(result);
}

/*
* GMS_LOB.WRITE (
*  lob_loc  IN OUT NOCOPY  BLOB, --目标blob对象
*  amount   IN             INTEGER, --指定的传入字节数
*  offset   IN             INTEGER, --开始读取blob的字节数的偏移量（原点在1）
*  buffer   IN             RAW); --用于写的输入缓冲区
*/
Datum gms_lob_og_write_blob(PG_FUNCTION_ARGS)
{
    char* argname = text_to_cstring(PG_GETARG_TEXT_P(4));
    /* check input args */
    if (strcmp(argname, ":") == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("output parameter not a bind variable")));
    }
    if (PG_ARGISNULL(0)) {
        closeLob(argname);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid LOB object specified")));
    }
    if (PG_ARGISNULL(1) || PG_ARGISNULL(2)) {
        closeLob(argname);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("Any of the input parameters are NULL")));
    }

    bytea* destlob = PG_GETARG_BYTEA_P(0);
    int32 amount = numericFloorToInt4(PG_GETARG_NUMERIC(1));
    int32 offset = numericFloorToInt4(PG_GETARG_NUMERIC(2));
    bytea* buffer = PG_GETARG_BYTEA_P(3);
    int32 bufsize = getVarSize(buffer);

    if (amount < 1 || amount > 32767 || amount > bufsize) {
        closeLob(argname);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("amount is invalid or out of range")));
    }
    if (offset < 1 || offset > LOBMAXSIZE) {
        closeLob(argname);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("Invalid argument value for dest_offset.")));
    }

    /* check lob's write and read permissions */
    bool found;
    int32 openmode = searchLob(argname, &found);
    if (found && openmode == 0) {
        closeLob(argname);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_OPERATION),
                 errmsg("cannot update a LOB opened in read-only mode.")));
    }

    int32 destlen = getVarSize(destlob);
    int destbytestart = (offset - 1) > destlen ? destlen : (offset - 1);
    int32 spacecnt = (offset - 1) > destlen ? (offset - destlen - 1) : 0;
    int32 appendbyte = amount;
    int32 destbyteend = (offset - 1 + appendbyte) > destlen ? destlen : (offset - 1 + appendbyte);
    int64 reslen = destbytestart + spacecnt + appendbyte + (destlen - destbyteend);
    bytea* res = NULL;
    char* resdata = NULL;
    int rc = 0;

    if (reslen != destlen || buffer == destlob) {
        /* dest is lack of space, or displaced copy when
         * buffer is the same as destlob, to avoid errors 
         * in memcpy_s checking, reapply 
         */
        if (reslen > MAX_TOAST_CHUNK_SIZE - VARHDRSZ) {
#ifdef ENABLE_MULTIPLE_NODES
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-support clob/blob type more than 1GB for distributed system")));
#endif
        }
        res = (bytea*)palloc(reslen + VARHDRSZ);
        SET_VARSIZE(res, reslen + VARHDRSZ);
    } else {
        res = destlob;
    }
    resdata = VARDATA(res);
    if (destbytestart > 0 && res != destlob) {
        rc = memcpy_s(resdata, destbytestart, VARDATA(destlob), destbytestart);
        securec_check(rc, "\0", "\0");
    }
    resdata += destbytestart;
    if (spacecnt > 0) {
        rc = memset_s(resdata, spacecnt, ' ', spacecnt);
        securec_check(rc, "\0", "\0");
    }
    resdata += spacecnt;
    if (appendbyte > 0) {
        rc = memcpy_s(resdata, appendbyte, VARDATA(buffer), appendbyte);
        securec_check(rc, "\0", "\0");
    }
    resdata += appendbyte;
    if (destbyteend < destlen && res != destlob) {
        rc = memcpy_s(resdata, destlen - destbyteend, VARDATA(destlob) + destbyteend, destlen - destbyteend);
        securec_check(rc, "\0", "\0");
    }
    PG_RETURN_BYTEA_P(res);
}

/*
* GMS_LOB.WRITE (
*  lob_loc  IN OUT  NOCOPY CLOB   CHARACTER SET ANY_CS, --目标clob对象
*  amount   IN             INTEGER, --指定传入的字符数
*  offset   IN             INTEGER, --开始读取blob的字符数的偏移量（原点在1）
*  buffer   IN             VARCHAR2 CHARACTER SET lob_loc%CHARSET); --用于写入的缓冲区
*/
Datum gms_lob_og_write_clob(PG_FUNCTION_ARGS)
{
    char* argname = text_to_cstring(PG_GETARG_TEXT_P(4));
    /* check input args */
    if (strcmp(argname, ":") == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("output parameter not a bind variable")));
    }
    if (PG_ARGISNULL(0)) {
        closeLob(argname);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid LOB object specified")));
    }
    if (PG_ARGISNULL(1) || PG_ARGISNULL(2)) {
        closeLob(argname);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("Any of the input parameters are NULL")));
    }

    text* destlob = PG_GETARG_TEXT_P(0);
    int32 amount = numericFloorToInt4(PG_GETARG_NUMERIC(1));
    int32 offset = numericFloorToInt4(PG_GETARG_NUMERIC(2));
    text* buffer = PG_GETARG_TEXT_P(3);
    int32 bufsize = DirectFunctionCall1(textlen, (Datum)buffer);

    if (amount < 1 || amount > 32767 || amount > bufsize) {
        closeLob(argname);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("amount is invalid or out of range")));
    }
    if (offset < 1 || offset > LOBMAXSIZE) {
        closeLob(argname);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("Invalid argument value for dest_offset.")));
    }

    /* check lob's write and read permissions */
    bool found;
    int32 openmode = searchLob(argname, &found);
    if (found && openmode == 0) {
        closeLob(argname);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_OPERATION),
                 errmsg("cannot update a LOB opened in read-only mode.")));
    }

    int32 destlen = DirectFunctionCall1(textlen, (Datum)destlob);
    int32 srcamount = amount;
    int32 destbytestart = 0;
    int32 spacecnt = 0;
    int32 appendbyte = charlen_to_bytelen(VARDATA(buffer), srcamount);
    int32 destamount = 0;
    int32 destbyteend = 0;
    int32 destbytecnt = VARSIZE(destlob) - VARHDRSZ;
    int64 reslen = 0;
    text* res = NULL;
    char* resdata = NULL;
    int rc = 0;

    if (destlen < offset - 1) {
        destamount = destlen;
        spacecnt = offset - 1  - destamount;
        destbytestart = destbytecnt;
        destbyteend = destbytecnt;
    } else {
        destamount = srcamount > (destlen - offset + 1) ? (destlen - offset + 1) : srcamount;
        destbytestart = charlen_to_bytelen(VARDATA(destlob), offset - 1);
        destbyteend = charlen_to_bytelen(VARDATA(destlob) + destbytestart, destamount) + destbytestart;
    }
    reslen = destbytestart + spacecnt + appendbyte + (destbytecnt - destbyteend);

    if (reslen != destbytecnt || buffer == destlob) {
        /* dest is lack of space, or displaced copy when
         * buffer is the same as destlob, to avoid errors 
         * in memcpy_s checking, reapply 
         */
        if (reslen > MAX_TOAST_CHUNK_SIZE - VARHDRSZ) {
#ifdef ENABLE_MULTIPLE_NODES
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-support clob/blob type more than 1GB for distributed system")));
#endif
        }
        res = (text*)palloc(reslen + VARHDRSZ);
        SET_VARSIZE(res, reslen + VARHDRSZ);
    } else {
        res = destlob;
    }
    resdata = VARDATA(res);
    if (destbytestart > 0 && res != destlob) {
        rc = memcpy_s(resdata, destbytestart, VARDATA(destlob), destbytestart);
        securec_check(rc, "\0", "\0");
    }
    resdata += destbytestart;
    if (spacecnt > 0) {
        rc = memset_s(resdata, spacecnt, ' ', spacecnt);
        securec_check(rc, "\0", "\0");
    }
    resdata += spacecnt;
    if (appendbyte > 0) {
        rc = memcpy_s(resdata, appendbyte, VARDATA(buffer), appendbyte);
        securec_check(rc, "\0", "\0");
    }
    resdata += appendbyte;
    if (destbyteend < destbytecnt && res != destlob) {
        rc = memcpy_s(resdata, destbytecnt - destbyteend, VARDATA(destlob) + destbyteend, destbytecnt - destbyteend);
        securec_check(rc, "\0", "\0");
    }
    PG_RETURN_TEXT_P(res);
}

/*
 * GMS_LOB.ISOPEN (
 *  lob_loc IN OUT    BLOB/CLOB);   --blob/clob对象
 */
Datum gms_lob_og_isopen(PG_FUNCTION_ARGS)
{
    bool found;
    char* argname = text_to_cstring(PG_GETARG_TEXT_P(1));
    if (strcmp(argname, ":") == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("output parameter not a bind variable")));
    }
    searchLob(argname, &found);
    if (found) {
        PG_RETURN_UINT8(1);
    } else {
        PG_RETURN_UINT8(0);
    }
}
/*
 * GMS_LOB.OPEN (
 *  lob_loc   IN OUT  BLOB/CLOB,
 *  open_mode IN      BINARY_INTEGER);
 */
Datum gms_lob_og_open(PG_FUNCTION_ARGS)
{
    if PG_ARGISNULL(0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid LOB object specified")));
    }
    bool found;
    int openmode = PG_GETARG_INT32(1);
    if (openmode != 0 && openmode != 1) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid open_mode")));
    }
    char* argname = text_to_cstring(PG_GETARG_TEXT_P(2));
    if (strcmp(argname, ":") == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("output parameter not a bind variable")));
    }

    searchLob(argname, &found);
    if (found) {
        ereport(NOTICE, (errmsg("Lob(%s) already opened in the same transaction", argname)));
        PG_RETURN_DATUM(PG_GETARG_DATUM(0));
    }
    openLob(argname, openmode);

    PG_RETURN_DATUM(PG_GETARG_DATUM(0));
}

/*
GMS_LOB.APPEND (
   dest_lob IN OUT  NOCOPY BLOB, --目标blob对象
   src_lob  IN             BLOB); --源blob对象
*/
Datum gms_lob_og_append_blob(PG_FUNCTION_ARGS)
{
    char* argname = text_to_cstring(PG_GETARG_TEXT_P(2));
    /* check input args */
    if (strcmp(argname, ":") == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("output parameter not a bind variable")));
    }
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1)) {
        closeLob(argname);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid LOB object specified")));
    }

    /* check lob's write and read permissions */
    bool found;
    int32 openmode = searchLob(argname, &found);
    if (found && openmode == 0) {
        closeLob(argname);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_OPERATION),
                 errmsg("cannot update a LOB opened in read-only mode.")));
    }
    
    Datum destlob = PG_GETARG_DATUM(0);
    Datum srclob = PG_GETARG_DATUM(1);
    Datum result = DirectFunctionCall2(byteacat, destlob, srclob);
    
    PG_RETURN_BYTEA_P(DatumGetPointer(result));
}

/*
GMS_LOB.APPEND (
   dest_lob IN OUT  NOCOPY CLOB  CHARACTER SET ANY_CS, --目标clob对象
   src_lob  IN             CLOB  CHARACTER SET dest_lob%CHARSET); --源clob对象
*/
Datum gms_lob_og_append_clob(PG_FUNCTION_ARGS)
{
    char* argname = text_to_cstring(PG_GETARG_TEXT_P(2));
    /* check input args */
    if (strcmp(argname, ":") == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("output parameter not a bind variable")));
    }
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1)) {
        closeLob(argname);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid LOB object specified")));
    }

    /* check lob's write and read permissions */
    bool found;
    int32 openmode = searchLob(argname, &found);
    if (found && openmode == 0) {
        closeLob(argname);
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_OPERATION),
                 errmsg("cannot update a LOB opened in read-only mode.")));
    }
    
    Datum destlob = PG_GETARG_DATUM(0);
    Datum srclob = PG_GETARG_DATUM(1);
    Datum result = DirectFunctionCall2(textcat, destlob, srclob);
    
    PG_RETURN_TEXT_P(DatumGetPointer(result));
}

/*
 * GMS_LOB.CLOSE (
 *  lob_loc IN OUT    BLOB/CLOB);   --blob/clob对象
 */
Datum gms_lob_og_close(PG_FUNCTION_ARGS)
{
    char* argname = text_to_cstring(PG_GETARG_TEXT_P(1));
    if (strcmp(argname, ":") == 0) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("output parameter not a bind variable")));
    }
    bool found;
    searchLob(argname, &found);
    if (!found) {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                 errmsg("cannot perform operation on an unopened file or LOB")));
    }
    closeLob(argname);

    PG_RETURN_DATUM(PG_GETARG_DATUM(0));
}

/*
 * GMS_LOB.getlength (
 *  lob_loc IN OUT    CLOB);   --clob对象
 */
Datum gms_lob_og_cloblength(PG_FUNCTION_ARGS)
{
    Datum data = PG_GETARG_DATUM(0);
    int datalen = 0;

    datalen = DirectFunctionCall1(textlen, data);
    PG_RETURN_INT32(datalen);
}
/*
 * GMS_LOB.getlength (
 *  lob_loc IN OUT    BLOB);   --blob对象
 */        
Datum gms_lob_og_bloblength(PG_FUNCTION_ARGS)
{
    bytea* data = PG_GETARG_BYTEA_P(0);
    int datalen = 0;

    datalen = VARSIZE(data) - VARHDRSZ;
    PG_RETURN_INT32(datalen);
}

/*
 * GMS_LOB.getlength ();   --NULL对象
 */
Datum gms_lob_og_null(PG_FUNCTION_ARGS)
{
    PG_RETURN_VOID();
}