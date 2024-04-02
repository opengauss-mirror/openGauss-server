#include "postgres.h"
#include "funcapi.h"
#include "commands/extension.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include "gms_output.h"

PG_MODULE_MAGIC;

/*
 * TODO: BUFSIZE_UNLIMITED to be truely unlimited (or INT_MAX),
 * and allocate buffers on-demand.
 */
#define BUFSIZE_DEFAULT        20000
#define BUFSIZE_MIN            2000
#define BUFSIZE_MAX            1000000
#define BUFSIZE_UNLIMITED    BUFSIZE_MAX

PG_FUNCTION_INFO_V1(gms_output_enable_default);
PG_FUNCTION_INFO_V1(gms_output_enable);
PG_FUNCTION_INFO_V1(gms_output_disable);
PG_FUNCTION_INFO_V1(gms_output_serveroutput);
PG_FUNCTION_INFO_V1(gms_output_put);
PG_FUNCTION_INFO_V1(gms_output_put_line);
PG_FUNCTION_INFO_V1(gms_output_new_line);
PG_FUNCTION_INFO_V1(gms_output_get_line);
PG_FUNCTION_INFO_V1(gms_output_get_lines);

static uint32 output_index;

static void add_str(const char *str, int len);
static void add_text(text *str);
static void add_newline(void);
static void send_buffer(void);

void set_extension_index(uint32 index)
{
    output_index = index;
}

void init_session_vars(void) {
    RepallocSessionVarsArrayIfNecessary();
    OutputContext* psc = 
        (OutputContext*)MemoryContextAllocZero(u_sess->self_mem_cxt, sizeof(OutputContext));
    u_sess->attr.attr_common.extension_session_vars_array[output_index] = psc;

    psc->is_server_output = false;
    psc->buffer = NULL;
    psc->buffer_size = 0;
    psc->buffer_len = 0;
    psc->buffer_get = 0;
}

OutputContext* get_session_context() {
    if (u_sess->attr.attr_common.extension_session_vars_array[output_index] == NULL) {
        init_session_vars();
    }
    return (OutputContext*)u_sess->attr.attr_common.extension_session_vars_array[output_index];
}

/*
 * Aux. buffer functionality
 */
static void
add_str(const char *str, int len)
{
    errno_t sret = EOK;
    bool is_change = false;
    if (strlen(str) != 0 && GetDatabaseEncoding() != pg_get_client_encoding()) {
        char *p = pg_server_to_client(str, len);
        if (p != str) {
            unsigned int mb_len = 0;
            const char *mb_str = p;
            str = p;
            is_change = true;
            while (*mb_str++)
                mb_len++;
            len = mb_len;
        }
    }

    /* Discard all buffers if get_line was called. */
    if (get_session_context()->buffer_get > 0)
    {
        get_session_context()->buffer_get = 0;
        get_session_context()->buffer_len = 0;
        get_session_context()->gms_getline = false;
        get_session_context()->gms_valid_num = 0;
    }

    if (get_session_context()->buffer_len + len > get_session_context()->buffer_size) {
        get_session_context()->buffer_len = 0;
        get_session_context()->buffer_get = 0;
        get_session_context()->gms_getline = false;
        get_session_context()->gms_valid_num = 0;
        get_session_context()->gms_serveroutput = true;
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
             errmsg("buffer overflow"),
             errdetail("Buffer overflow, limit of %d bytes", get_session_context()->buffer_size),
             errhint("Increase buffer size in gms_output.enable() next time")));
    }
        
    if ( len != 0 ) {
        sret = memcpy_s(get_session_context()->buffer + get_session_context()->buffer_len, 
                get_session_context()->buffer_size - get_session_context()->buffer_len, str, len);
        securec_check(sret, "\0", "\0");

        int tmplen = (len < (OUTPUTBUF_LEN-1)) ? len : (OUTPUTBUF_LEN-1);
        sret = memcpy_s(get_session_context()->output_buf, OUTPUTBUF_LEN, get_session_context()->buffer + get_session_context()->buffer_len, tmplen);
        securec_check(sret, "\0", "\0");
        get_session_context()->output_buf[tmplen] = '\0';
        if (pg_strcasecmp(str, "") != 0 && get_session_context()->gms_serveroutput) {
            ereport(NOTICE,
                    (errmsg("%s", get_session_context()->output_buf), onlyfrontmsg(true)));
        }
    }
    if (is_change) {
        pfree_ext(str);
    }
    get_session_context()->buffer_len += len;
    get_session_context()->buffer[get_session_context()->buffer_len] = '\0';
}

static void
add_text(text *str)
{
    add_str(VARDATA_ANY(str), VARSIZE_ANY_EXHDR(str));
}

static void
add_newline(void)
{
    add_str("", 1);	   /* add \0 */
    if (get_session_context()->is_server_output)
        send_buffer();
}


static void
send_buffer()
{
    if (get_session_context()->buffer_len > 0)
    {
        StringInfoData msgbuf;
        char *cursor = get_session_context()->buffer;

        while (--get_session_context()->buffer_len > 0)
        {
            if (*cursor == '\0')
                *cursor = '\n';
            cursor++;
        }

        if (*cursor != '\0')
            ereport(ERROR,
                    (errcode(ERRCODE_INTERNAL_ERROR),
                     errmsg("internal error"),
                     errdetail("Wrong message format detected")));

        pq_beginmessage(&msgbuf, 'N');
        /*
         * FrontendProtocol is not avalilable in MSVC because it is not
         * PGDLLEXPORT'ed. So, we assume always the protocol >= 3.
         */
#ifndef _MSC_VER
        if (PG_PROTOCOL_MAJOR(FrontendProtocol) >= 3)
        {
#endif
            pq_sendbyte(&msgbuf, PG_DIAG_MESSAGE_PRIMARY);
            pq_sendstring(&msgbuf, get_session_context()->buffer);
            pq_sendbyte(&msgbuf, '\0');
#ifndef _MSC_VER
        }
        else
        {
            *cursor++ = '\n';
            *cursor = '\0';
            pq_sendstring(&msgbuf, get_session_context()->buffer);
        }
#endif
        pq_endmessage(&msgbuf);
        pq_flush();
    }
}

/*
 * Aux db functions
 *
 */

static void
gms_output_enable_internal(int32 n_buf_size)
{
    /* We allocate +2 bytes for an end-of-line and a string terminator. */
    if (get_session_context()->buffer == NULL)
    {
        get_session_context()->buffer = (char*)MemoryContextAlloc(u_sess->self_mem_cxt, n_buf_size + 2);
        get_session_context()->buffer_size = n_buf_size;
        get_session_context()->buffer_len = 0;
        get_session_context()->buffer_get = 0;
        get_session_context()->gms_getline = false;
        get_session_context()->gms_valid_num = 0;
        get_session_context()->gms_serveroutput = true;
    }
    else if (n_buf_size > get_session_context()->buffer_len)
    {
        /* We cannot shrink buffer less than current length. */
        get_session_context()->buffer = (char*)repalloc(get_session_context()->buffer, n_buf_size + 2);
        get_session_context()->buffer_size = n_buf_size;
    } 
    else 
    {
        ereport(NOTICE, (errmsg("The buf size(%d) cannot be smaller than the used buf size(%d).", n_buf_size, get_session_context()->buffer_len)));
    }
}

Datum
gms_output_enable_default(PG_FUNCTION_ARGS)
{
    gms_output_enable_internal(BUFSIZE_DEFAULT);
    PG_RETURN_VOID();
}

Datum
gms_output_enable(PG_FUNCTION_ARGS)
{
    int32 n_buf_size;

    if (PG_ARGISNULL(0))
        n_buf_size = BUFSIZE_UNLIMITED;
    else
    {
        n_buf_size = PG_GETARG_INT32(0);

        if (n_buf_size > BUFSIZE_MAX)
        {
            n_buf_size = BUFSIZE_MAX;
            elog(WARNING, "Limit decreased to %d bytes.", BUFSIZE_MAX);
        }
        else if (n_buf_size < BUFSIZE_MIN)
        {
            n_buf_size = BUFSIZE_MIN;
            elog(WARNING, "Limit increased to %d bytes.", BUFSIZE_MIN);
        }
    }

    gms_output_enable_internal(n_buf_size);
    PG_RETURN_VOID();
}

Datum
gms_output_disable(PG_FUNCTION_ARGS)
{
    if (get_session_context()->buffer)
        pfree(get_session_context()->buffer);

    get_session_context()->buffer = NULL;
    get_session_context()->buffer_size = 0;
    get_session_context()->buffer_len = 0;
    get_session_context()->buffer_get = 0;
    get_session_context()->gms_getline = false;
    get_session_context()->gms_valid_num = 0;
    get_session_context()->gms_serveroutput = true;
    PG_RETURN_VOID();
}

Datum
gms_output_serveroutput(PG_FUNCTION_ARGS)
{
    get_session_context()->is_server_output = PG_GETARG_BOOL(0);
    if (get_session_context()->is_server_output && !get_session_context()->buffer)
        gms_output_enable_internal(BUFSIZE_DEFAULT);
    PG_RETURN_VOID();
}


/*
 * main functions
 */

Datum
gms_output_put(PG_FUNCTION_ARGS)
{
    if (get_session_context()->buffer)
        add_text(PG_GETARG_TEXT_PP(0));
    PG_RETURN_VOID();
}

Datum
gms_output_put_line(PG_FUNCTION_ARGS)
{
    if (get_session_context()->buffer)
    {
        add_text(PG_GETARG_TEXT_PP(0));
        add_newline();
    }
    PG_RETURN_VOID();
}

Datum
gms_output_new_line(PG_FUNCTION_ARGS)
{
    if (get_session_context()->buffer)
        add_newline();
    PG_RETURN_VOID();
}

static text *
gms_output_next(void)
{
    if (get_session_context()->buffer_get < get_session_context()->buffer_len)
    {
        text *line = cstring_to_text(get_session_context()->buffer + get_session_context()->buffer_get);
        get_session_context()->buffer_get += VARSIZE_ANY_EXHDR(line) + 1;
        return line;
    }
    else
        return NULL;
}

Datum
gms_output_get_line(PG_FUNCTION_ARGS)
{
    TupleDesc    tupdesc;
    Datum        result;
    HeapTuple    tuple;
    Datum        values[2];
    bool        nulls[2] = { false, false };
    text       *line;

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("function returning record called in context "
                        "that cannot accept type record"),
                 errhint("Try calling the function in the FROM clause "
                         "using a column definition list.")));

    if ((line = gms_output_next()) != NULL)
    {
        values[0] = PointerGetDatum(line);
        values[1] = Int32GetDatum(0);    /* 0: succeeded */
    }
    else
    {
        nulls[0] = true;
        values[1] = Int32GetDatum(1);    /* 1: failed */
    }

    tuple = heap_form_tuple(tupdesc, values, nulls);
    result = HeapTupleGetDatum(tuple);

    get_session_context()->gms_getline = true;
    PG_RETURN_DATUM(result);
}

Datum
gms_output_get_lines(PG_FUNCTION_ARGS)
{
    TupleDesc    tupdesc;
    Datum        result;
    HeapTuple    tuple;
    Datum        values[2];
    bool         nulls[2] = { false, false };
    text         *line;
    int32        max_lines = PG_GETARG_INT32(1);
    int32        n;
    ArrayBuildState *astate = NULL;

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("function returning record called in context "
                        "that cannot accept type record"),
                 errhint("Try calling the function in the FROM clause "
                         "using a column definition list.")));

    for (n = 0; n < max_lines && (line = gms_output_next()) != NULL; n++)
    {
        astate = accumArrayResult(astate, PointerGetDatum(line), false,
                    TEXTOID, CurrentMemoryContext);
    }

    /* 0: lines as text array */
    if (n > 0)
        values[0] = makeArrayResult(astate, CurrentMemoryContext);
    else
    {
        int16        typlen;
        bool        typbyval;
        char        typalign;
        ArrayType  *arr;

        get_typlenbyvalalign(TEXTOID, &typlen, &typbyval, &typalign);
        arr = construct_md_array(
            values,
            NULL,
            0, NULL, NULL, TEXTOID, typlen, typbyval, typalign);
        values[0] = PointerGetDatum(arr);
    }

    get_session_context()->gms_getline = true;
    /* 1: # of lines as integer */
    values[1] = Int32GetDatum(n);

    tuple = heap_form_tuple(tupdesc, values, nulls);
    result = HeapTupleGetDatum(tuple);

    PG_RETURN_DATUM(result);
}
