/*
 * This file contains public functions for conversion between
 * client encoding and server (database) encoding.
 *
 * Tatsuo Ishii
 *
 * src/backend/utils/mb/mbutils.c
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "mb/pg_wchar.h"
#include "pgxc/execRemote.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "storage/ipc.h"
#include "executor/executor.h"

/*
 * We maintain a simple linked list caching the fmgr lookup info for the
 * currently selected conversion functions, as well as any that have been
 * selected previously in the current session.	(We remember previous
 * settings because we must be able to restore a previous setting during
 * transaction rollback, without doing any fresh catalog accesses.)
 *
 * Since we'll never release this data, we just keep it in t_thrd.top_mem_cxt.
 */
typedef struct ConvProcInfo {
    int s_encoding; /* server and client encoding IDs */
    int c_encoding;
    FmgrInfo to_server_info; /* lookup info for conversion procs */
    FmgrInfo to_client_info;
} ConvProcInfo;

/* Internal functions */
static char* perform_default_encoding_conversion(const char* src, int len, bool is_client_to_server);
static int cliplen(const char* str, int len, int limit);

// Determine whether the current case needs to be converted
bool NoNeedToConvert(int srcEncoding, int destEncoding)
{
    if (srcEncoding == destEncoding) {
        return true;
    }
    if (srcEncoding == PG_SQL_ASCII || destEncoding == PG_SQL_ASCII) {
        return true;
    }
    if (srcEncoding == PG_GB18030_2022 && destEncoding == PG_GB18030) {
        return true;
    }
    if (srcEncoding == PG_GB18030 && destEncoding == PG_GB18030_2022) {
        return true;
    }
    return false;
}

/*
 * Prepare for a future call to SetClientEncoding.	Success should mean
 * that SetClientEncoding is guaranteed to succeed for this encoding request.
 *
 * (But note that success before u_sess->mb_cxt.backend_startup_complete does not guarantee
 * success after ...)
 *
 * Returns 0 if okay, -1 if not (bad encoding or can't support conversion)
 */
int PrepareClientEncoding(int encoding)
{
    int current_server_encoding;
    ListCell* lc = NULL;

    if (!PG_VALID_FE_ENCODING(encoding)) {
        return -1;
    }

    /* Can't do anything during startup, per notes above */
    if (!u_sess->mb_cxt.backend_startup_complete) {
        return 0;
    }

    /*
     * Check for cases that require no conversion function.
     */
    current_server_encoding = GetDatabaseEncoding();
    if (NoNeedToConvert(current_server_encoding, encoding)) {
        return 0;
    }

    if (IsTransactionState()) {
        /*
         * If we're in a live transaction, it's safe to access the catalogs,
         * so look up the functions.  We repeat the lookup even if the info is
         * already cached, so that we can react to changes in the contents of
         * pg_conversion.
         */
        Oid to_server_proc;
        Oid to_client_proc;
        ConvProcInfo* conv_info = NULL;
        MemoryContext old_context;

        to_server_proc = FindDefaultConversionProc(encoding, current_server_encoding);
        if (!OidIsValid(to_server_proc)) {
            return -1;
        }
        to_client_proc = FindDefaultConversionProc(current_server_encoding, encoding);
        if (!OidIsValid(to_client_proc)) {
            return -1;
        }

        /*
         * Load the fmgr info into t_thrd.top_mem_cxt (could still fail here)
         */
        MemoryContext executorCxt = SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR);
        conv_info = (ConvProcInfo*)MemoryContextAlloc(executorCxt, sizeof(ConvProcInfo));
        conv_info->s_encoding = current_server_encoding;
        conv_info->c_encoding = encoding;
        fmgr_info_cxt(to_server_proc, &conv_info->to_server_info, executorCxt);
        fmgr_info_cxt(to_client_proc, &conv_info->to_client_info, executorCxt);

        /* Attach new info to head of list */
        old_context = MemoryContextSwitchTo(executorCxt);
        u_sess->mb_cxt.ConvProcList = lcons(conv_info, u_sess->mb_cxt.ConvProcList);
        (void)MemoryContextSwitchTo(old_context);

        /*
         * We cannot yet remove any older entry for the same encoding pair,
         * since it could still be in use.	SetClientEncoding will clean up.
         */
        return 0; /* success */
    } else {
        /*
         * If we're not in a live transaction, the only thing we can do is
         * restore a previous setting using the cache.	This covers all
         * transaction-rollback cases.	The only case it might not work for is
         * trying to change client_encoding on the fly by editing
         * postgresql.conf and SIGHUP'ing.  Which would probably be a stupid
         * thing to do anyway.
         */
        foreach (lc, u_sess->mb_cxt.ConvProcList) {
            ConvProcInfo* oldinfo = (ConvProcInfo*)lfirst(lc);
            if (oldinfo->s_encoding == current_server_encoding && oldinfo->c_encoding == encoding) {
                return 0;
            }
        }

        return -1; /* it's not cached, so fail */
    }
}

/*
 * Set the active client encoding and set up the conversion-function pointers.
 * PrepareClientEncoding should have been called previously for this encoding.
 *
 * Returns 0 if okay, -1 if not (bad encoding or can't support conversion)
 */
int SetClientEncoding(int encoding)
{
    int current_server_encoding;
    bool found = false;
    ListCell* lc = NULL;
    ListCell* prev = NULL;
    ListCell* next = NULL;

    if (!PG_VALID_FE_ENCODING(encoding)) {
        return -1;
    }

    /* Can't do anything during startup, per notes above */
    if (!u_sess->mb_cxt.backend_startup_complete) {
        u_sess->mb_cxt.pending_client_encoding = encoding;
        return 0;
    }

    /*
     * Check for cases that require no conversion function.
     */
    current_server_encoding = GetDatabaseEncoding();
    if (NoNeedToConvert(current_server_encoding, encoding)) {
        u_sess->mb_cxt.ClientEncoding = &pg_enc2name_tbl[encoding];
        u_sess->mb_cxt.ToServerConvProc = NULL;
        u_sess->mb_cxt.ToClientConvProc = NULL;
        return 0;
    }

    /*
     * Search the cache for the entry previously prepared by
     * PrepareClientEncoding; if there isn't one, we lose.  While at it,
     * release any duplicate entries so that repeated Prepare/Set cycles don't
     * leak memory.
     */
    found = false;
    prev = NULL;
    for (lc = list_head(u_sess->mb_cxt.ConvProcList); lc; lc = next) {
        ConvProcInfo* conv_info = (ConvProcInfo*)lfirst(lc);
        next = lnext(lc);
        if (conv_info->s_encoding == current_server_encoding && conv_info->c_encoding == encoding) {
            if (!found) {
                /* Found newest entry, so set up */
                u_sess->mb_cxt.ClientEncoding = &pg_enc2name_tbl[encoding];
                u_sess->mb_cxt.ToServerConvProc = &conv_info->to_server_info;
                u_sess->mb_cxt.ToClientConvProc = &conv_info->to_client_info;
                found = true;
            } else {
                /* Duplicate entry, release it */
                u_sess->mb_cxt.ConvProcList = list_delete_cell(u_sess->mb_cxt.ConvProcList, lc, prev);
                pfree(conv_info);
                continue; /* prev mustn't advance */
            }
        }

        prev = lc;
    }

    if (found) {
        return 0; /* success */
    } else {
        return -1; /* it's not cached, so fail */
    }
}

/*
 * Initialize client encoding conversions.
 *		Called from InitPostgres() once during backend startup.
 */
void InitializeClientEncoding(void)
{
    Assert(!u_sess->mb_cxt.backend_startup_complete);
    u_sess->mb_cxt.backend_startup_complete = true;

    if (PrepareClientEncoding(u_sess->mb_cxt.pending_client_encoding) < 0 ||
        SetClientEncoding(u_sess->mb_cxt.pending_client_encoding) < 0) {
        /*
         * Oops, the requested conversion is not available. We couldn't fail
         * before, but we can now.
         */
        ereport(FATAL,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("conversion between %s and %s is not supported",
                    pg_enc2name_tbl[u_sess->mb_cxt.pending_client_encoding].name,
                    GetDatabaseEncodingName())));
    }
    u_sess->mb_cxt.character_set_connection = &pg_enc2name_tbl[GetDatabaseEncoding()];
    if (ENABLE_MULTI_CHARSET) {
        u_sess->mb_cxt.collation_connection = get_default_collation_by_charset(GetDatabaseEncoding(), false);
    } else {
        u_sess->mb_cxt.collation_connection = InvalidOid;
    }
}

/*
 * returns the current client encoding
 */
int pg_get_client_encoding(void)
{
    Assert(u_sess->mb_cxt.ClientEncoding);
    return u_sess->mb_cxt.ClientEncoding->encoding;
}

/*
 * returns the current client encoding name
 */
const char* pg_get_client_encoding_name(void)
{
    Assert(u_sess->mb_cxt.ClientEncoding);
    return u_sess->mb_cxt.ClientEncoding->name;
}

/*
 * Apply encoding conversion on src and return it. The encoding
 * conversion function is chosen from the pg_conversion system catalog
 * marked as "default". If it is not found in the schema search path,
 * it's taken from pg_catalog schema. If it even is not in the schema,
 * warn and return src.
 *
 * If conversion occurs, a palloc'd null-terminated string is returned.
 * In the case of no conversion, src is returned.
 *
 * CAUTION: although the presence of a length argument means that callers
 * can pass non-null-terminated strings, care is required because the same
 * string will be passed back if no conversion occurs.	Such callers *must*
 * check whether result == src and handle that case differently.
 *
 * Note: we try to avoid raising error, since that could get us into
 * infinite recursion when this function is invoked during error message
 * sending.  It should be OK to raise error for overlength strings though,
 * since the recursion will come with a shorter message.
 */
unsigned char* pg_do_encoding_conversion(unsigned char* src, int len, int src_encoding, int dest_encoding)
{
    unsigned char* result = NULL;
    Oid proc;

    if (!IsTransactionState()) {
        return src;
    }
    if (NoNeedToConvert(src_encoding, dest_encoding)) {
        return src;
    }
    if (len <= 0) {
        return src;
    }
    proc = FindDefaultConversionProc(src_encoding, dest_encoding);
    if (!OidIsValid(proc)) {
        ereport(DEBUG2,
            (errcode(ERRCODE_UNDEFINED_FUNCTION),
                errmsg("default conversion function for encoding \"%s\" to \"%s\" does not exist",
                    pg_encoding_to_char(src_encoding),
                    pg_encoding_to_char(dest_encoding))));
        return src;
    }

    /*
     * XXX we should avoid throwing errors in OidFunctionCall. Otherwise we
     * are going into infinite loop!  So we have to make sure that the
     * function exists before calling OidFunctionCall.
     */
    if (!SearchSysCacheExists1(PROCOID, ObjectIdGetDatum(proc))) {
        ereport(LOG, (errmsg("cache lookup failed for function %u", proc)));
        return src;
    }

    /*
     * Allocate space for conversion result, being wary of integer overflow
     */
    if ((Size)len >= (MaxAllocSize / (Size)MAX_CONVERSION_GROWTH)) {
        ereport(ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                errmsg("out of memory"),
                errdetail("String of %d bytes is too long for encoding conversion.", len)));
    }
    result = (unsigned char*)palloc(len * MAX_CONVERSION_GROWTH + 1);

    OidFunctionCall5(proc,
        Int32GetDatum(src_encoding),
        Int32GetDatum(dest_encoding),
        CStringGetDatum(src),
        CStringGetDatum(result),
        Int32GetDatum(len));
    return result;
}

void construct_conversion_fmgr_info(int src_encoding, int dst_encoding, void* finfo)
{
    Assert(finfo != NULL);

    FmgrInfo* convert_finfo = (FmgrInfo*)finfo;
    if (src_encoding == dst_encoding) {
        convert_finfo->fn_oid = InvalidOid;
        return;
    }

    if (src_encoding == PG_SQL_ASCII || dst_encoding == PG_SQL_ASCII) {
        convert_finfo->fn_oid = InvalidOid;
        return;
    }

    Oid convert_func = FindDefaultConversionProc(src_encoding, dst_encoding);
    if (OidIsValid(convert_func)) {
        fmgr_info(convert_func, convert_finfo);
    } else {
        convert_finfo->fn_oid = InvalidOid;
    }
}


static char* fast_encoding_conversion(char* src, int len, int src_encoding, int dest_encoding, FmgrInfo* convert_finfo)
{
    if (len <= 0) {
        return src;
    }

    char* result = NULL;
    Assert(convert_finfo != NULL);
    Assert(OidIsValid(convert_finfo->fn_oid));

    /*
     * Allocate space for conversion result, being wary of integer overflow
     */
    if ((Size)len >= (MaxAllocSize / (Size)MAX_CONVERSION_GROWTH)) {
        ereport(ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                errmsg("out of memory"),
                errdetail("String of %d bytes is too long for encoding conversion.", len)));
    }
    result = (char*)palloc(len * MAX_CONVERSION_GROWTH + 1);

    FunctionCall5(convert_finfo,
        Int32GetDatum(src_encoding),
        Int32GetDatum(dest_encoding),
        CStringGetDatum(src),
        CStringGetDatum(result),
        Int32GetDatum(len));
    return result;
}

char* try_fast_encoding_conversion(char* src, int len, int src_encoding, int dest_encoding, void* convert_finfo)
{
    if (unlikely(!OidIsValid(((FmgrInfo*)convert_finfo)->fn_oid))) {
        return (char*)pg_do_encoding_conversion((unsigned char*)src, len, src_encoding, dest_encoding);
    }

    return fast_encoding_conversion(src, len, src_encoding, dest_encoding, (FmgrInfo*)convert_finfo);
}

/*
 * Convert string using encoding_name. The source
 * encoding is the DB encoding.
 *
 * BYTEA convert_to(TEXT string, NAME encoding_name) */
Datum pg_convert_to(PG_FUNCTION_ARGS)
{
    Datum string = PG_GETARG_DATUM(0);
    Datum dest_encoding_name = PG_GETARG_DATUM(1);
    Datum src_encoding_name = DirectFunctionCall1(namein, CStringGetDatum(u_sess->mb_cxt.DatabaseEncoding->name));
    Datum result;

    /*
     * pg_convert expects a bytea as its first argument. We're passing it a
     * text argument here, relying on the fact that they are both in fact
     * varlena types, and thus structurally identical.
     */
    result = DirectFunctionCall3(pg_convert, string, src_encoding_name, dest_encoding_name);

    PG_RETURN_DATUM(result);
}

/* for GBK order */
Datum pg_convert_to_nocase(PG_FUNCTION_ARGS)
{
    Datum string = PG_GETARG_DATUM(0);
    Datum dest_encoding_name = PG_GETARG_DATUM(1);
    Datum src_encoding_name = DirectFunctionCall1(namein, CStringGetDatum(u_sess->mb_cxt.DatabaseEncoding->name));
    Datum result;
    FUNC_CHECK_HUGE_POINTER(PG_ARGISNULL(0), DatumGetPointer(string), "pg_convert()");

    /*
     * pg_convert expects a bytea as its first argument. We're passing it a
     * text argument here, relying on the fact that they are both in fact
     * varlena types, and thus structurally identical.
     */
    result = DirectFunctionCall3(pg_convert_nocase, string, src_encoding_name, dest_encoding_name);

    PG_RETURN_DATUM(result);
}

/*
 * Convert string using encoding_name. The destination
 * encoding is the DB encoding.
 *
 * TEXT convert_from(BYTEA string, NAME encoding_name) */
Datum pg_convert_from(PG_FUNCTION_ARGS)
{
    Datum string = PG_GETARG_DATUM(0);
    Datum src_encoding_name = PG_GETARG_DATUM(1);
    Datum dest_encoding_name = DirectFunctionCall1(namein, CStringGetDatum(u_sess->mb_cxt.DatabaseEncoding->name));
    Datum result;

    result = DirectFunctionCall3(pg_convert, string, src_encoding_name, dest_encoding_name);

    /*
     * pg_convert returns a bytea, which we in turn return as text, relying on
     * the fact that they are both in fact varlena types, and thus
     * structurally identical. Although not all bytea values are valid text,
     * in this case it will be because we've told pg_convert to return one
     * that is valid as text in the current database encoding.
     */
    PG_RETURN_DATUM(result);
}

/*
 * Convert string using encoding_names.
 *
 * BYTEA convert(BYTEA string, NAME src_encoding_name, NAME dest_encoding_name)
 */
Datum pg_convert(PG_FUNCTION_ARGS)
{
    bytea* string = PG_GETARG_BYTEA_PP(0);
    char* src_encoding_name = NameStr(*PG_GETARG_NAME(1));
    int src_encoding = pg_char_to_encoding(src_encoding_name);
    char* dest_encoding_name = NameStr(*PG_GETARG_NAME(2));
    int dest_encoding = pg_char_to_encoding(dest_encoding_name);
    const char* src_str = NULL;
    char* dest_str = NULL;
    bytea* retval = NULL;
    int len;

    if (src_encoding < 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid source encoding name \"%s\"", src_encoding_name)));
    }
    if (dest_encoding < 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid destination encoding name \"%s\"", dest_encoding_name)));
    }

    /* make sure that source string is valid */
    len = VARSIZE_ANY_EXHDR(string);
    src_str = VARDATA_ANY(string);
    (void)pg_verify_mbstr_len(src_encoding, src_str, len, false);

    dest_str = (char*)pg_do_encoding_conversion((unsigned char*)src_str, len, src_encoding, dest_encoding);
    if (dest_str != src_str) {
        len = strlen(dest_str);
    }
    /*
     * build bytea data type structure.
     */
    retval = (bytea*)palloc(len + VARHDRSZ);
    SET_VARSIZE(retval, len + VARHDRSZ);
    if (len > 0) {
        errno_t rc = memcpy_s(VARDATA(retval), len, dest_str, len);
        securec_check(rc, "", "");
    }

    if (dest_str != src_str) {
        pfree(dest_str);
    }
    /* free memory if allocated by the toaster */
    PG_FREE_IF_COPY(string, 0);

    PG_RETURN_BYTEA_P(retval);
}

Datum pg_convert_nocase(PG_FUNCTION_ARGS)
{
    bytea* string = PG_GETARG_BYTEA_PP(0);
    char* src_encoding_name = NameStr(*PG_GETARG_NAME(1));
    int src_encoding = pg_char_to_encoding(src_encoding_name);
    char* dest_encoding_name = NameStr(*PG_GETARG_NAME(2));
    int dest_encoding = pg_char_to_encoding(dest_encoding_name);
    const char* src_str = NULL;
    char* dest_str = NULL;
    char* dest_str_tmp = NULL;

    bytea* retval = NULL;
    int len;
    int char_index = 0;
    char achar = '\0';
    int chardiff = 'a' - 'A';

    if (src_encoding < 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid source encoding name \"%s\"", src_encoding_name)));
    }
    if (dest_encoding < 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid destination encoding name \"%s\"", dest_encoding_name)));
    }

    /* make sure that source string is valid */
    len = VARSIZE_ANY_EXHDR(string);
    src_str = VARDATA_ANY(string);
    (void)pg_verify_mbstr_len(src_encoding, src_str, len, false);

    dest_str = (char*)pg_do_encoding_conversion((unsigned char*)src_str, len, src_encoding, dest_encoding);
    if (dest_str != src_str) {
        len = strlen(dest_str);
    }

    /*
     * build bytea data type structure.
     */
    retval = (bytea*)palloc(len + VARHDRSZ);
    SET_VARSIZE(retval, len + VARHDRSZ);

    int ss_rc = memcpy_s(VARDATA(retval), len, dest_str, len);
    securec_check(ss_rc, "", "");

    dest_str_tmp = VARDATA(retval);
    achar = *dest_str_tmp;
    while (achar != '\0' && char_index < len) {
        achar = *dest_str_tmp;
        if (achar >= 'A' && achar <= 'Z') {
            *dest_str_tmp += chardiff;
        }
        dest_str_tmp++;
        char_index++;
    }
    if (dest_str != src_str) {
        pfree(dest_str);
    }
    /* free memory if allocated by the toaster */
    PG_FREE_IF_COPY(string, 0);

    PG_RETURN_BYTEA_P(retval);
}

/*
 * get the length of the string considered as text in the specified
 * encoding. Raises an error if the data is not valid in that
 * encoding.
 *
 * INT4 length (BYTEA string, NAME src_encoding_name)
 */
Datum length_in_encoding(PG_FUNCTION_ARGS)
{
    bytea* string = PG_GETARG_BYTEA_P(0);
    char* src_encoding_name = NameStr(*PG_GETARG_NAME(1));
    int src_encoding = pg_char_to_encoding(src_encoding_name);
    int len = VARSIZE(string) - VARHDRSZ;
    int ret_val;

    if (src_encoding < 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid encoding name \"%s\"", src_encoding_name)));
    }
    ret_val = pg_verify_mbstr_len(src_encoding, VARDATA(string), len, false);
    PG_RETURN_INT32(ret_val);
}

Datum pg_encoding_max_length_sql(PG_FUNCTION_ARGS)
{
    int encoding = PG_GETARG_INT32(0);
    if (PG_VALID_ENCODING(encoding)) {
        PG_RETURN_INT32(pg_wchar_table[encoding].maxmblen);
    } else {
        PG_RETURN_NULL();
    }
}

/*
 * convert client encoding to server encoding.
 */
char* pg_client_to_server(const char* s, int len)
{
    Assert(u_sess->mb_cxt.ClientEncoding);

    return pg_any_to_server(s, len, u_sess->mb_cxt.ClientEncoding->encoding);
}

char* verify_string_for_ascii(const char* s, int len, int encoding, bool bulkload_illegal_chars_conversion)
{
    /*
    * No conversion is possible, but we must still validate the data,
    * because the client-side code might have done string escaping using
    * the selected client_encoding.  If the client encoding is ASCII-safe
    * then we just do a straight validation under that encoding.  For an
    * ASCII-unsafe encoding we have a problem: we dare not pass such data
    * to the parser but we have no way to convert it.	We compromise by
    * rejecting the data if it contains any non-ASCII characters.
    */
    if (PG_VALID_BE_ENCODING(encoding)) {
        (void)pg_verify_mbstr(encoding, s, len, false);
        return (char*)s;
    }

    int i;
    for (i = 0; i < len; i++) {
        if (s[i] == '\0' || IS_HIGHBIT_SET(s[i])) {
            if (!bulkload_illegal_chars_conversion) {
                ereport(ERROR,
                    (errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
                        errmsg("invalid byte value for encoding \"%s\": 0x%02x",
                            pg_enc2name_tbl[PG_SQL_ASCII].name,
                            (unsigned char)s[i])));
            }

            if (s[i] == '\0') {
                *((char*)&s[i]) = ' ';
            } else {
                *((char*)&s[i]) = '?';
            }
        }
    }
    return (char*)s;
}

/*
 * convert any encoding to server encoding.
 */
char* pg_any_to_server(const char* s, int len, int encoding)
{
    bool bulkload_illegal_chars_conversion = false;

    Assert(u_sess->mb_cxt.DatabaseEncoding);
    Assert(u_sess->mb_cxt.ClientEncoding);

    if (len <= 0) {
        return (char*)s;
    }
    if (u_sess->cmd_cxt.bulkload_compatible_illegal_chars) {
        bulkload_illegal_chars_conversion = true;
    }

    if (encoding == u_sess->mb_cxt.DatabaseEncoding->encoding || encoding == PG_SQL_ASCII ||
        (encoding == PG_GB18030 && u_sess->mb_cxt.DatabaseEncoding->encoding == PG_GB18030_2022)) {
        /*
         * No conversion is needed, but we must still validate the data.
         */
        (void)pg_verify_mbstr(u_sess->mb_cxt.DatabaseEncoding->encoding, s, len, false);
        return (char*)s;
    }

    if (u_sess->mb_cxt.DatabaseEncoding->encoding == PG_SQL_ASCII) {
        return verify_string_for_ascii(s, len, encoding, bulkload_illegal_chars_conversion);
    }

    if (u_sess->mb_cxt.ClientEncoding->encoding == encoding) {
        return perform_default_encoding_conversion(s, len, true);
    } else {
        return (char*)pg_do_encoding_conversion(
            (unsigned char*)s, len, encoding, u_sess->mb_cxt.DatabaseEncoding->encoding);
    }
}

/*
 * convert any encoding to client encoding.
 */
char* pg_any_to_client(const char* s, int len, int encoding, void* convert_finfo)
{
    Assert(u_sess->mb_cxt.ClientEncoding);

    if (len <= 0) {
        return (char*)s;
    }
    int client_encoding = u_sess->mb_cxt.ClientEncoding->encoding;

    if (encoding == client_encoding || client_encoding == PG_SQL_ASCII) {
        /*
         * No conversion is needed, but we must still validate the data.
         */
        return (char*)s;
    }

    if (encoding == PG_SQL_ASCII) {
        /* No conversion is possible, but we must validate the result */
        (void) pg_verify_mbstr(client_encoding, s, len, false);
        return (char*)s;
    }

    if (u_sess->mb_cxt.DatabaseEncoding->encoding == encoding) {
        return perform_default_encoding_conversion(s, len, false);
    } else if (convert_finfo != NULL) {
        return try_fast_encoding_conversion(
            (char*)s, len, encoding, client_encoding, convert_finfo);
    } else {
        return (char*)pg_do_encoding_conversion(
            (unsigned char*)s, len, encoding, client_encoding);
    }
}

/*
 * convert client encoding to encoding.
 */
char* pg_client_to_any(const char* s, int len, int dst_encoding, void* convert_finfo)
{
    bool bulkload_illegal_chars_conversion = false;

    Assert(u_sess->mb_cxt.ClientEncoding);

    if (len <= 0) {
        return (char*)s;
    }
    if (u_sess->cmd_cxt.bulkload_compatible_illegal_chars) {
        bulkload_illegal_chars_conversion = true;
    }

    int client_encoding = u_sess->mb_cxt.ClientEncoding->encoding;
    if (client_encoding == dst_encoding || client_encoding == PG_SQL_ASCII) {
        /*
         * No conversion is needed, but we must still validate the data.
         */
        (void)pg_verify_mbstr(dst_encoding, s, len, false);
        return (char*)s;
    }

    if (dst_encoding == PG_SQL_ASCII) {
        return verify_string_for_ascii(s, len, client_encoding, bulkload_illegal_chars_conversion);
    }

    if (u_sess->mb_cxt.DatabaseEncoding->encoding == dst_encoding) {
        return perform_default_encoding_conversion(s, len, true);
    } else if (convert_finfo != NULL) {
        return try_fast_encoding_conversion( (char*)s, len, client_encoding, dst_encoding, convert_finfo);
    } else {
        return (char*)pg_do_encoding_conversion((unsigned char*)s, len, client_encoding, dst_encoding);
    }
}

/*
 * convert server encoding to client encoding.
 */
char* pg_server_to_client(const char* s, int len)
{
    char* str = NULL;

    Assert(u_sess->mb_cxt.ClientEncoding);

    str = pg_server_to_any(s, len, u_sess->mb_cxt.ClientEncoding->encoding);
    if (str == NULL) {
        ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("pg_server_to_any returns null.")));
    }
    return str;
}

/*
 * Preheck if pg_server_to_any is really gonna do a conversion. That makes a difference
 * in COPY TO FILE, which is weird and not logical. Yet HandleCopyDataRow are not to
 * be changed and this function is added instead.
 */
bool WillTranscodingBePerformed(int encoding)
{
    return (!(encoding == u_sess->mb_cxt.DatabaseEncoding->encoding || encoding == PG_SQL_ASCII ||
              u_sess->mb_cxt.DatabaseEncoding->encoding == PG_SQL_ASCII));
}

/*
 * convert server encoding to any encoding.
 */
char* pg_server_to_any(const char* s, int len, int encoding, void *convert_finfo)
{
    Assert(u_sess->mb_cxt.DatabaseEncoding);
    Assert(u_sess->mb_cxt.ClientEncoding);

    if (len <= 0) {
        return (char*)s;
    }
    if (encoding == u_sess->mb_cxt.DatabaseEncoding->encoding || encoding == PG_SQL_ASCII) {
        return (char*)s; /* assume data is valid */
    }
    if (u_sess->mb_cxt.DatabaseEncoding->encoding == PG_SQL_ASCII) {
        /* No conversion is possible, but we must validate the result */
        (void) pg_verify_mbstr(encoding, s, len, false);
        return (char*)s;
    }
    if (u_sess->mb_cxt.ClientEncoding->encoding == encoding) {
        return perform_default_encoding_conversion(s, len, false);
    } else if (convert_finfo != NULL) {
        return try_fast_encoding_conversion(
            (char*)s, len, u_sess->mb_cxt.DatabaseEncoding->encoding, encoding, convert_finfo);
    } else {
        return (char*)pg_do_encoding_conversion(
            (unsigned char*)s, len, u_sess->mb_cxt.DatabaseEncoding->encoding, encoding);
    }
}

/*
 *	Perform default encoding conversion using cached FmgrInfo. Since
 *	this function does not access database at all, it is safe to call
 *	outside transactions.  If the conversion has not been set up by
 *	SetClientEncoding(), no conversion is performed.
 */
static char* perform_default_encoding_conversion(const char* src, int len, bool is_client_to_server)
{
    char* result = NULL;
    int src_encoding, dest_encoding;
    FmgrInfo* flinfo = NULL;

    if (is_client_to_server) {
        src_encoding = u_sess->mb_cxt.ClientEncoding->encoding;
        dest_encoding = u_sess->mb_cxt.DatabaseEncoding->encoding;
        flinfo = u_sess->mb_cxt.ToServerConvProc;
    } else {
        src_encoding = u_sess->mb_cxt.DatabaseEncoding->encoding;
        dest_encoding = u_sess->mb_cxt.ClientEncoding->encoding;
        flinfo = u_sess->mb_cxt.ToClientConvProc;
    }

    if (flinfo == NULL) {
        return (char*)src;
    }
    /*
     * Allocate space for conversion result, being wary of integer overflow
     */
    if ((Size)len >= (MaxAllocSize / (Size)MAX_CONVERSION_GROWTH)) {
        ereport(ERROR,
            (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                errmsg("out of memory"),
                errdetail("String of %d bytes is too long for encoding conversion.", len)));
    }
    result = (char*)palloc(len * MAX_CONVERSION_GROWTH + 1);

    FunctionCall5(flinfo,
        Int32GetDatum(src_encoding),
        Int32GetDatum(dest_encoding),
        CStringGetDatum(src),
        CStringGetDatum(result),
        Int32GetDatum(len));
    return result;
}

/* convert a multibyte string to a wchar */
int pg_mb2wchar(const char* from, pg_wchar* to)
{
    return (*pg_wchar_table[u_sess->mb_cxt.DatabaseEncoding->encoding].mb2wchar_with_len)(
        (const unsigned char*)from, to, strlen(from));
}

/* convert a multibyte string to a wchar with a limited length */
int pg_mb2wchar_with_len(const char* from, pg_wchar* to, int len)
{
    return (*pg_wchar_table[u_sess->mb_cxt.DatabaseEncoding->encoding].mb2wchar_with_len)(
        (const unsigned char*)from, to, len);
}

/* same, with any encoding */
int pg_encoding_mb2wchar_with_len(int encoding, const char* from, pg_wchar* to, int len)
{
    return (*pg_wchar_table[encoding].mb2wchar_with_len)((const unsigned char*)from, to, len);
}

/* convert a wchar string to a multibyte */
int pg_wchar2mb(const pg_wchar* from, char* to)
{
    return (*pg_wchar_table[u_sess->mb_cxt.DatabaseEncoding->encoding].wchar2mb_with_len)(
        from, (unsigned char*)to, pg_wchar_strlen(from));
}

/* convert a wchar string to a multibyte with a limited length */
int pg_wchar2mb_with_len(const pg_wchar* from, char* to, int len)
{
    return (*pg_wchar_table[u_sess->mb_cxt.DatabaseEncoding->encoding].wchar2mb_with_len)(
        from, (unsigned char*)to, len);
}

/* same, with any encoding */
int pg_encoding_wchar2mb_with_len(int encoding, const pg_wchar* from, char* to, int len)
{
    return (*pg_wchar_table[encoding].wchar2mb_with_len)(from, (unsigned char*)to, len);
}

/* returns the byte length of a multibyte character */
int pg_mblen(const char* mbstr)
{
    return ((*pg_wchar_table[u_sess->mb_cxt.DatabaseEncoding->encoding].mblen)((const unsigned char*)mbstr));
}

/* returns the display length of a multibyte character */
int pg_dsplen(const char* mbstr)
{
    return ((*pg_wchar_table[u_sess->mb_cxt.DatabaseEncoding->encoding].dsplen)((const unsigned char*)mbstr));
}

/* returns the length (counted in wchars) of a multibyte string */
int pg_mbstrlen(const char* mbstr)
{
    int len = 0;

    /* optimization for single byte encoding */
    if (pg_database_encoding_max_length() == 1) {
        return strlen(mbstr);
    }
    while (*mbstr) {
        mbstr += pg_mblen(mbstr);
        len++;
    }
    return len;
}

/* returns the length (counted in wchars) of a multibyte string
 * (not necessarily NULL terminated)
 */
int pg_mbstrlen_with_len(const char* mbstr, int limit)
{
    int len = 0;

    /* optimization for single byte encoding */
    if (pg_database_encoding_max_length() == 1) {
        return limit;
    }
    while (limit > 0 && *mbstr) {
        int l = pg_mblen(mbstr);

        limit -= l;
        mbstr += l;
        len++;
    }
    return len;
}

/* returns the length (counted in wchars) of a multibyte string
 * (not necessarily NULL terminated)
 */
int pg_encoding_mbstrlen_with_len(const char* mbstr, int limit, int encoding)
{
    int len = 0;

    /* optimization for single byte encoding */
    if (pg_encoding_max_length(encoding) == 1) {
        return limit;
    }
    while (limit > 0 && *mbstr) {
        int l = pg_encoding_mblen(encoding, mbstr);

        limit -= l;
        mbstr += l;
        len++;
    }
    return len;
}

/* returns the length (counted in wchars) of a multibyte string
 * with fixed encoding.
 */
int pg_mbstrlen_with_len_eml(const char* mbstr, int limit, int eml)
{
    int len = 0;

    /* optimization for single byte encoding */
    if (eml == 1) {
        return limit;
    }
    while (limit > 0 && *mbstr) {
        int l = pg_mblen(mbstr);
        limit -= l;
        mbstr += l;
        len++;
    }
    return len;
}

int pg_mbstrlen_with_len_toast(const char* mbstr, int* limit)
{
    int len = 0;

    while (*limit > 0 && *mbstr) {
        int l = pg_mblen(mbstr);

        *limit -= l;
        mbstr += l;
        len++;
    }
    return len;
}

/*
 * returns the byte length of a multibyte string
 * (not necessarily NULL terminated)
 * that is no longer than limit.
 * this function does not break multibyte character boundary.
 */
int pg_mbcliplen(const char* mbstr, int len, int limit)
{
    return pg_encoding_mbcliplen(u_sess->mb_cxt.DatabaseEncoding->encoding, mbstr, len, limit);
}

/*
 * pg_mbcliplen with specified encoding
 */
int pg_encoding_mbcliplen(int encoding, const char* mbstr, int len, int limit)
{
    mblen_converter mblen_fn;
    int clen = 0;
    int l;

    /* optimization for single byte encoding */
    if (pg_encoding_max_length(encoding) == 1) {
        return cliplen(mbstr, len, limit);
    }
    mblen_fn = pg_wchar_table[encoding].mblen;

    while (len > 0 && *mbstr) {
        l = (*mblen_fn)((const unsigned char*)mbstr);
        if ((clen + l) > limit) {
            break;
        }
        clen += l;
        if (clen == limit) {
            break;
        }
        len -= l;
        mbstr += l;
    }
    return clen;
}

/**
 * calculate the length of mbstr
 * @tparam calCharLength true for the character length, false for the byte length
 * @param mbstr mbstr
 * @param len length of mbstr
 * @param limit limit of mbstr
 * @return the length of mbstr  
 */
template<bool calCharLength> int MbCharClipLen(const char* mbstr, int len, int limit)
{
    int clen = 0;
    int nch = 0;
    int l;

    /* optimization for single byte encoding */
    if (pg_database_encoding_max_length() == 1) {
        return cliplen(mbstr, len, limit);
    }
    while (len > 0 && *mbstr) {
        l = pg_mblen(mbstr);
        if (calCharLength) {
            nch++;
        } else {
            nch += l;
        }
        if (nch > limit) {
            break;
        }
        clen += l;
        len -= l;
        mbstr += l;
    }
    return clen;
}

/*
 * Similar to pg_mbcliplen except the limit parameter specifies the
 * byte length, not the character length.
 */
int pg_mbcharcliplen(const char* mbstr, int len, int limit)
{
    bool calCharLength = DB_IS_CMPT(PG_FORMAT | B_FORMAT);
    if (calCharLength) {
        return MbCharClipLen<true>(mbstr, len, limit);
    } else {
        return MbCharClipLen<false>(mbstr, len, limit);
    }
}
/*
 * Description	: Similar to pg_mbcliplen except the limit parameter specifies
 * 				  the character length, not the byte length.
 * Notes		:
 */
int pg_mbcharcliplen_orig(const char* mbstr, int len, int limit)
{
    return MbCharClipLen<true>(mbstr, len, limit);
}

/* mbcliplen for any single-byte encoding */
static int cliplen(const char* str, int len, int limit)
{
    int l = 0;
    len = Min(len, limit);
    while (l < len && str[l]) {
        l++;
    }
    return l;
}

void SetDatabaseEncoding(int encoding)
{
    if (!PG_VALID_BE_ENCODING(encoding)) {
        ereport(ERROR, (errcode(ERRCODE_SYSTEM_ERROR), errmsg("invalid database encoding: %d", encoding)));
    }
    u_sess->mb_cxt.DatabaseEncoding = &pg_enc2name_tbl[encoding];
    Assert(u_sess->mb_cxt.DatabaseEncoding->encoding == encoding);
}

/*
 * Bind gettext to the codeset equivalent with the database encoding.
 */
void pg_bind_textdomain_codeset(const char* domain_name)
{
#if defined(ENABLE_NLS)
    int encoding = GetDatabaseEncoding();
    int i;

    /*
     * gettext() uses the codeset specified by LC_CTYPE by default, so if that
     * matches the database encoding we don't need to do anything. In CREATE
     * DATABASE, we enforce or trust that the locale's codeset matches
     * database encoding, except for the C locale. In C locale, we bind
     * gettext() explicitly to the right codeset.
     *
     * On Windows, though, gettext() tends to get confused so we always bind
     * it.
     */
#ifndef WIN32
    /* setlocale is thread-unsafe */
    AutoMutexLock localeLock(&gLocaleMutex);
    localeLock.lock();
    const char* ctype = gs_setlocale_r(LC_CTYPE, NULL);

    if (pg_strcasecmp(ctype, "C") != 0 && pg_strcasecmp(ctype, "POSIX") != 0) {
        localeLock.unLock();
        return;
    }

    localeLock.unLock();
#endif

    for (i = 0; pg_enc2gettext_tbl[i].name != NULL; i++) {
        if (pg_enc2gettext_tbl[i].encoding == encoding) {
            if (bind_textdomain_codeset(domain_name, pg_enc2gettext_tbl[i].name) == NULL) {
                ereport(LOG, (errmsg("bind_textdomain_codeset failed")));
            }
            break;
        }
    }
#endif
}

int GetDatabaseEncoding(void)
{
    Assert(u_sess->mb_cxt.DatabaseEncoding);
    return u_sess->mb_cxt.DatabaseEncoding->encoding;
}

const char* GetDatabaseEncodingName(void)
{
    Assert(u_sess->mb_cxt.DatabaseEncoding);
    return u_sess->mb_cxt.DatabaseEncoding->name;
}

int GetCharsetConnection(void)
{
    Assert(u_sess->mb_cxt.character_set_connection);
    return u_sess->mb_cxt.character_set_connection->encoding;
}

const char* GetCharsetConnectionName(void)
{
    Assert(u_sess->mb_cxt.character_set_connection);
    return u_sess->mb_cxt.character_set_connection->name;
}

Oid GetCollationConnection(void)
{
    if (!ENABLE_MULTI_CHARSET || !DB_IS_CMPT(B_FORMAT)) {
        return InvalidOid;
    }
    return u_sess->mb_cxt.collation_connection;
}

Datum getdatabaseencoding(PG_FUNCTION_ARGS)
{
    Assert(u_sess->mb_cxt.DatabaseEncoding);
    return DirectFunctionCall1(namein, CStringGetDatum(u_sess->mb_cxt.DatabaseEncoding->name));
}

Datum pg_client_encoding(PG_FUNCTION_ARGS)
{
    Assert(u_sess->mb_cxt.ClientEncoding);
    return DirectFunctionCall1(namein, CStringGetDatum(u_sess->mb_cxt.ClientEncoding->name));
}

int GetPlatformEncoding(void)
{
    if (u_sess->mb_cxt.PlatformEncoding == NULL) {
        int encoding;

        AutoMutexLock localeLock(&gLocaleMutex);
        localeLock.lock();
        /* try to determine encoding of server's environment locale */
        encoding = pg_get_encoding_from_locale("", true);
        localeLock.unLock();

        if (encoding < 0) {
            encoding = PG_SQL_ASCII;
        }
        u_sess->mb_cxt.PlatformEncoding = &pg_enc2name_tbl[encoding];
    }
    return u_sess->mb_cxt.PlatformEncoding->encoding;
}

#ifdef WIN32

/*
 * Result is palloc'ed null-terminated utf16 string. The character length
 * is also passed to utf16len if not null. Returns NULL iff failed.
 */
WCHAR* pgwin32_toUTF16(const char* str, int len, int* utf16_len)
{
    WCHAR* utf16 = NULL;
    int dst_len;

    /*
     * Use MultiByteToWideChar directly if there is a corresponding codepage,
     * or double conversion through UTF8 if not.
     */
    UINT codepage = pg_enc2name_tbl[GetDatabaseEncoding()].codepage;
    if (codepage != 0) {
        utf16 = (WCHAR*)palloc(sizeof(WCHAR) * (len + 1));
        dst_len = MultiByteToWideChar(codepage, 0, str, len, utf16, len);
        utf16[dst_len] = (WCHAR)0;
    } else {
        char* utf8 = NULL;
        utf8 = (char*)pg_do_encoding_conversion((unsigned char*)str, len, GetDatabaseEncoding(), PG_UTF8);
        if (utf8 != str) {
            len = strlen(utf8);
        }
        utf16 = (WCHAR*)palloc(sizeof(WCHAR) * (len + 1));
        dst_len = MultiByteToWideChar(CP_UTF8, 0, utf8, len, utf16, len);
        utf16[dst_len] = (WCHAR)0;

        if (utf8 != str) {
            pfree(utf8);
        }
    }

    if (dst_len == 0 && len > 0) {
        pfree(utf16);
        return NULL; /* error */
    }

    if (utf16_len != NULL) {
        *utf16_len = dst_len;
    }
    return utf16;
}

#endif
