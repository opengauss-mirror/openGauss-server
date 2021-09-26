/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * odbc_connetcor.cpp
 * 	The implementation of the process of exec_on_extension() based on ODBC
 *
 * IDENTIFICATION
 *      Code/src/gausskernel/cbb/extension/connector/odbc_connector.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <dlfcn.h>

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>

#include "access/tupdesc.h"
#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/value.h"
#include "storage/lock/lwlock.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/dynamic_loader.h"
#include "utils/int8.h"
#include "utils/timestamp.h"
#include "utils/bytea.h"
#include "mb/pg_wchar.h"
#include "executor/node/nodeFunctionscan.h"

#include "odbc_bridge.h"
#include "odbc_connector.h"
#include "connector.h"

/* the function pointer of the ODBC API */
THR_LOCAL SQLGetDiagRecAPI pSQLGetDiagRec = NULL;
THR_LOCAL SQLAllocHandleAPI pSQLAllocHandle = NULL;
THR_LOCAL SQLFreeHandleAPI pSQLFreeHandle = NULL;
THR_LOCAL SQLGetInfoAPI pSQLGetInfo = NULL;
THR_LOCAL SQLGetConnectAttrAPI pSQLGetConnectAttr = NULL;
THR_LOCAL SQLSetConnectAttrAPI pSQLSetConnectAttr = NULL;
THR_LOCAL SQLSetEnvAttrAPI pSQLSetEnvAttr = NULL;
THR_LOCAL SQLSetStmtAttrAPI pSQLSetStmtAttr = NULL;
THR_LOCAL SQLConnectAPI pSQLConnect = NULL;
THR_LOCAL SQLDisconnectAPI pSQLDisconnect = NULL;
THR_LOCAL SQLCancelAPI pSQLCancel = NULL;
THR_LOCAL SQLExecDirectAPI pSQLExecDirect = NULL;
THR_LOCAL SQLFetchAPI pSQLFetch = NULL;
THR_LOCAL SQLGetDataAPI pSQLGetData = NULL;
THR_LOCAL SQLNumResultColsAPI pSQLNumResultCols = NULL;
THR_LOCAL SQLDescribeColAPI pSQLDescribeCol = NULL;
THR_LOCAL SQLBindColAPI pSQLBindCol = NULL;

static StringInfo get_odbc_errmsg(StringInfo all_msg);
static void unload_libodbc();
static void load_libodbc(FunctionCallInfo fcinfo);
static void check_typeoid(Form_pg_attribute* attrs, int natts);
static Datum odbc_type_2_Datum(Form_pg_attribute attr, void* buf, const char* encoding);
extern void clean_ec_conn();
extern void delete_ec_ctrl();

/* MACROS which help to catch and print the exception. */
#define ODBC_TRY()                                         \
    bool saveStatus = t_thrd.int_cxt.ImmediateInterruptOK; \
    t_thrd.int_cxt.ImmediateInterruptOK = false;           \
    bool errOccur = false;                                 \
    StringInfo errMsg = makeStringInfo();                  \
    try

#define ODBC_CATCH(exception)                                                              \
    catch (abi::__forced_unwind&)                                                          \
    {                                                                                      \
        throw;                                                                             \
    }                                                                                      \
    catch (exception & ex)                                                                 \
    {                                                                                      \
        errOccur = true;                                                                   \
        try {                                                                              \
            appendStringInfo(errMsg, "%s", ex.what());                                     \
        } catch (...) {                                                                    \
        }                                                                                  \
    }                                                                                      \
    catch (...)                                                                            \
    {                                                                                      \
        errOccur = true;                                                                   \
    }                                                                                      \
    t_thrd.int_cxt.ImmediateInterruptOK = saveStatus;                                      \
    if (errOccur && errMsg->len > 0) {                                                     \
        ereport(LOG, (errmodule(MOD_EC), errmsg("Caught ODBC error: %s.", errMsg->data))); \
    }

#define ODBC_ERRREPORT(msg, dsn)                                                        \
    if (errOccur) {                                                                     \
        /* end_odbc() must be called before ereport(ERROR, ...) to avoid memory leak */ \
        end_odbc(dsn);                                                                  \
        StringInfo head_msg = makeStringInfo();                                         \
        if (NULL != (dsn)) {                                                            \
            appendStringInfoString(head_msg, "DSN:");                                   \
            appendStringInfoString(head_msg, dsn);                                      \
            appendStringInfoChar(head_msg, ',');                                        \
        }                                                                               \
        appendStringInfoString(head_msg, msg);                                          \
        appendStringInfoString(head_msg, " Detail can be found in node log of '%s'.");  \
        StringInfo hive_err = get_odbc_errmsg(errMsg);                                  \
        if (NULL != hive_err)                                                           \
            ereport(ERROR,                                                              \
                (errmodule(MOD_EC),                                                     \
                    errcode(ERRCODE_CONNECTION_EXCEPTION),                              \
                    errmsg(head_msg->data, g_instance.attr.attr_common.PGXCNodeName),   \
                    errdetail("%s", hive_err->data)));                                  \
        char* p = strstr(errMsg->data, "00000: ");                                      \
        if (NULL == p)                                                                  \
            ereport(ERROR,                                                              \
                (errmodule(MOD_EC),                                                     \
                    errcode(ERRCODE_CONNECTION_EXCEPTION),                              \
                    errmsg(head_msg->data, g_instance.attr.attr_common.PGXCNodeName))); \
        p = p + 7;                                                                      \
        if (strlen(p) <= 0)                                                             \
            ereport(ERROR,                                                              \
                (errmodule(MOD_EC),                                                     \
                    errcode(ERRCODE_CONNECTION_EXCEPTION),                              \
                    errmsg(head_msg->data, g_instance.attr.attr_common.PGXCNodeName))); \
        else                                                                            \
            ereport(ERROR,                                                              \
                (errmodule(MOD_EC),                                                     \
                    errcode(ERRCODE_CONNECTION_EXCEPTION),                              \
                    errmsg(head_msg->data, g_instance.attr.attr_common.PGXCNodeName),   \
                    errdetail("%s", p)));                                               \
    }

/*
 * @Description: load the dynamic library of the unixODBC(libodbc.so.2)
 *
 * Note: use unixODBC-2.3.1 or later
 */
static void load_libodbc(FunctionCallInfo fcinfo)
{
    unload_libodbc();

    bool is_libodbc_type_one = false;

    /* load libodbc.so.2 */
    (void)LWLockAcquire(ExtensionConnectorLibLock, LW_EXCLUSIVE);
    t_thrd.conn_cxt.dl_handle = dlopen("libodbc.so.2", RTLD_NOW | RTLD_GLOBAL);
    LWLockRelease(ExtensionConnectorLibLock);

    if (t_thrd.conn_cxt.dl_handle == NULL) {
        /* load libodbc.so.1 */
        (void)LWLockAcquire(ExtensionConnectorLibLock, LW_EXCLUSIVE);
        t_thrd.conn_cxt.dl_handle = dlopen("libodbc.so.1", RTLD_NOW | RTLD_GLOBAL);
        LWLockRelease(ExtensionConnectorLibLock);

        is_libodbc_type_one = true;
    }

    if (t_thrd.conn_cxt.dl_handle == NULL) {
        ereport(ERROR,
            (errmodule(MOD_EC),
                errcode(ERRCODE_UNDEFINED_FILE),
                errmsg("libodbc.so.1 or libodbc.so.2 not found, which is needed to run the Extension Connector."),
                errdetail("libodbc.so.1 or libodbc.so.2 is the part of unixODBC, make sure that unixODBC has been "
                          "installed.")));
    }

    FunctionScanState* node = (FunctionScanState*)fcinfo->context;
    if (node != NULL && node->ss.ps.instrument != NULL) {
        if (is_libodbc_type_one) {
            node->ss.ps.instrument->ec_libodbc_type = EC_LIBODBC_TYPE_ONE;
        } else {
            node->ss.ps.instrument->ec_libodbc_type = EC_LIBODBC_TYPE_TWO;
        }
    }

    /* clear any existing error */
    (void)dlerror();

    /* assemble the function pointers with ODBC API name */
    const char* func_name = NULL;

    pSQLGetDiagRec = (SQLGetDiagRecAPI)dlsym(t_thrd.conn_cxt.dl_handle, "SQLGetDiagRec");
    if (pSQLGetDiagRec == NULL) {
        func_name = "SQLGetDiagRec";
        goto loaderr;
    }

    pSQLAllocHandle = (SQLAllocHandleAPI)dlsym(t_thrd.conn_cxt.dl_handle, "SQLAllocHandle");
    if (pSQLAllocHandle == NULL) {
        func_name = "SQLAllocHandle";
        goto loaderr;
    }

    pSQLFreeHandle = (SQLFreeHandleAPI)dlsym(t_thrd.conn_cxt.dl_handle, "SQLFreeHandle");
    if (pSQLFreeHandle == NULL) {
        func_name = "SQLFreeHandle";
        goto loaderr;
    }

    pSQLGetInfo = (SQLGetInfoAPI)dlsym(t_thrd.conn_cxt.dl_handle, "SQLGetInfo");
    if (pSQLGetInfo == NULL) {
        func_name = "SQLGetInfo";
        goto loaderr;
    }

    pSQLGetConnectAttr = (SQLGetConnectAttrAPI)dlsym(t_thrd.conn_cxt.dl_handle, "SQLGetConnectAttr");
    if (pSQLGetConnectAttr == NULL) {
        func_name = "SQLGetConnectAttr";
        goto loaderr;
    }

    pSQLSetConnectAttr = (SQLSetConnectAttrAPI)dlsym(t_thrd.conn_cxt.dl_handle, "SQLSetConnectAttr");
    if (pSQLSetConnectAttr == NULL) {
        func_name = "SQLSetConnectAttr";
        goto loaderr;
    }

    pSQLSetEnvAttr = (SQLSetEnvAttrAPI)dlsym(t_thrd.conn_cxt.dl_handle, "SQLSetEnvAttr");
    if (pSQLSetEnvAttr == NULL) {
        func_name = "SQLSetEnvAttr";
        goto loaderr;
    }

    pSQLSetStmtAttr = (SQLSetStmtAttrAPI)dlsym(t_thrd.conn_cxt.dl_handle, "SQLSetStmtAttr");
    if (pSQLSetStmtAttr == NULL) {
        func_name = "SQLSetStmtAttr";
        goto loaderr;
    }

    pSQLConnect = (SQLConnectAPI)dlsym(t_thrd.conn_cxt.dl_handle, "SQLConnect");
    if (pSQLConnect == NULL) {
        func_name = "SQLConnect";
        goto loaderr;
    }

    pSQLDisconnect = (SQLDisconnectAPI)dlsym(t_thrd.conn_cxt.dl_handle, "SQLDisconnect");
    if (pSQLDisconnect == NULL) {
        func_name = "SQLDisconnect";
        goto loaderr;
    }

    pSQLCancel = (SQLCancelAPI)dlsym(t_thrd.conn_cxt.dl_handle, "SQLCancel");
    if (pSQLCancel == NULL) {
        func_name = "SQLCancel";
        goto loaderr;
    }

    pSQLExecDirect = (SQLExecDirectAPI)dlsym(t_thrd.conn_cxt.dl_handle, "SQLExecDirect");
    if (pSQLExecDirect == NULL) {
        func_name = "SQLExecDirect";
        goto loaderr;
    }

    pSQLFetch = (SQLFetchAPI)dlsym(t_thrd.conn_cxt.dl_handle, "SQLFetch");
    if (pSQLFetch == NULL) {
        func_name = "SQLFetch";
        goto loaderr;
    }

    pSQLGetData = (SQLGetDataAPI)dlsym(t_thrd.conn_cxt.dl_handle, "SQLGetData");
    if (pSQLGetData == NULL) {
        func_name = "SQLGetData";
        goto loaderr;
    }

    pSQLNumResultCols = (SQLNumResultColsAPI)dlsym(t_thrd.conn_cxt.dl_handle, "SQLNumResultCols");
    if (pSQLNumResultCols == NULL) {
        func_name = "SQLNumResultCols";
        goto loaderr;
    }

    pSQLDescribeCol = (SQLDescribeColAPI)dlsym(t_thrd.conn_cxt.dl_handle, "SQLDescribeCol");
    if (pSQLDescribeCol == NULL) {
        func_name = "SQLDescribeCol";
        goto loaderr;
    }

    pSQLBindCol = (SQLBindColAPI)dlsym(t_thrd.conn_cxt.dl_handle, "SQLBindCol");
    if (pSQLBindCol == NULL) {
        func_name = "SQLBindCol";
        goto loaderr;
    }

    return;

loaderr:
    StringInfo err_msg = makeStringInfo();
    appendStringInfo(err_msg, "%s", dlerror());
    unload_libodbc();

    ereport(ERROR,
        (errmodule(MOD_EC),
            errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Can NOT find function \"%s()\" in libodbc.so.1 or libodbc.so.2.", func_name),
            errdetail("%s", err_msg->data)));
    return;
}

/*
 * @Description: close the dynamic library of the unixODBC.
 */
static void unload_libodbc()
{
    (void)LWLockAcquire(ExtensionConnectorLibLock, LW_EXCLUSIVE);
    if (t_thrd.conn_cxt.dl_handle) {
        (void)dlclose(t_thrd.conn_cxt.dl_handle);
        t_thrd.conn_cxt.dl_handle = NULL;
    }
    LWLockRelease(ExtensionConnectorLibLock);
}

/*
 * @Description: find error message from hive in ODBC error message.
 * @IN  value: all error message from hive and ODBC
 * @return: error message from hive
 */
static StringInfo get_odbc_errmsg(StringInfo all_msg)
{
    char* p = strstr(all_msg->data, "SQL_ERROR");
    if (NULL == p) {
        return NULL;
    }

    StringInfo tmp = makeStringInfo();
    appendStringInfoString(tmp, p);

    StringInfo hive_err = makeStringInfo();
    copyStringInfo(hive_err, tmp);

    char* src = tmp->data;
    char* dst = hive_err->data;
    while (*src != '\0') {
        if (*src == '\n' || *src == '\t') {
            src++;
            continue;
        }
        *dst++ = *src++;
    }
    *src = '\0';

    return hive_err;
}

/*
 * @Description: check column type of returned record.
 *
 * @IN  value: attrs, the attribute array.
 * @IN  value: natts, the number of the items in attrs.
 */
static void check_typeoid(Form_pg_attribute* attrs, int natts)
{
    for (int i = 0; i < natts; i++) {
        switch (attrs[i]->atttypid) {
            case BOOLOID:
            case INT1OID:
            case INT2OID:
            case INT4OID:
            case INT8OID:
            case FLOAT4OID:
            case FLOAT8OID:
            case NUMERICOID:
            case TIMESTAMPOID:
            case TIMESTAMPTZOID:
            case INTERVALOID:
            case DATEOID:
            case TEXTOID:
            case CHAROID:
            case VARCHAROID:
            case BPCHAROID:
            case NVARCHAR2OID:
                break;

            /* unsupported data type */
            default:
                ereport(ERROR,
                    (errmodule(MOD_EC),
                        errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("unsupport data type: [%s] found in record definition.",
                            format_type_with_typemod(attrs[i]->atttypid, attrs[i]->atttypmod))));
        }
    }
}

static Datum odbc_type_2_Datum_sub(Form_pg_attribute attr, void* buf, const char* encoding)
{
    Datum result;

    switch (attr->atttypid) {
        case VARCHAROID: {
            /* for varchar(n) and varchar2(n) type input: */
            char* varchar_str = (char*)pg_do_encoding_conversion((unsigned char*)buf,
                strlen((char*)buf),
                pg_char_to_encoding((const char*)encoding),
                pg_char_to_encoding((const char*)t_thrd.conn_cxt._DatabaseEncoding->name));
            result = DirectFunctionCall3(varcharin,
                CStringGetDatum((const char*)varchar_str),
                ObjectIdGetDatum(InvalidOid),
                Int32GetDatum(attr->atttypmod));
            break;
        }
        case NVARCHAR2OID: {
            /* for nvarchar2(n) type input: */
            char* nvarchar2_str = (char*)pg_do_encoding_conversion((unsigned char*)buf,
                strlen((char*)buf),
                pg_char_to_encoding((const char*)encoding),
                pg_char_to_encoding((const char*)t_thrd.conn_cxt._DatabaseEncoding->name));
            result = DirectFunctionCall3(nvarchar2in,
                CStringGetDatum((const char*)nvarchar2_str),
                ObjectIdGetDatum(InvalidOid),
                Int32GetDatum(attr->atttypmod));
            break;
        }
        case INTERVALOID: {
            /* for interval[fields] type input: */
            result = DirectFunctionCall3(
                interval_in, CStringGetDatum(buf), ObjectIdGetDatum(InvalidOid), Int32GetDatum(attr->atttypmod));
            break;
        }

        default: /* should never run here, check_typeoid() has checked. */
        {
            ereport(ERROR,
                (errmodule(MOD_EC),
                    errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("unsupport data type found, type oid: %d", attr->atttypid)));
            return (Datum)NULL; /* just keep compiler silent */
        }
    }

    return result;
}
/*
 * @Description: convert SQL_C data type to Datum in PG.
 *
 * @IN  value: type, destination Oid in PG
 * @IN  value: buf, buffer of data from ODBC server.
 *
 * @return: the Datum from buf
 */
static Datum odbc_type_2_Datum(Form_pg_attribute attr, void* buf, const char* encoding)
{
    Datum result;

    switch (attr->atttypid) {
        case BOOLOID: {
            result = BoolGetDatum(pg_atoi((char*)buf, sizeof(int8), '\0'));
            break;
        }
        case INT1OID: {
            result = Int8GetDatum(pg_atoi((char*)buf, sizeof(int8), '\0'));
            break;
        }
        case INT2OID: {
            result = DirectFunctionCall3(int2in, CStringGetDatum(buf), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
            break;
        }
        case INT4OID: {
            result = DirectFunctionCall3(int4in, CStringGetDatum(buf), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
            break;
        }
        case INT8OID: {
            result = DirectFunctionCall3(int8in, CStringGetDatum(buf), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
            break;
        }
        case FLOAT4OID: {
            /*
             * For A db infinity type: ODBC returns '#'
             * For A db NaN type: ODBC returns 0
             */
            if (*(char*)buf == '#') {
                result = DirectFunctionCall3(float4in,
                    CStringGetDatum(t_thrd.conn_cxt._float_inf),
                    ObjectIdGetDatum(InvalidOid),
                    Int32GetDatum(-1));
            } else {
                result = DirectFunctionCall3(
                    float4in, CStringGetDatum(buf), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
            }
            break;
        }
        case FLOAT8OID: {
            /*
             * For A db infinity type: ODBC returns '#'
             * For A db NaN type: ODBC returns 0
             */
            if (*(char*)buf == '#') {
                result = DirectFunctionCall3(float8in,
                    CStringGetDatum(t_thrd.conn_cxt._float_inf),
                    ObjectIdGetDatum(InvalidOid),
                    Int32GetDatum(-1));
            } else {
                result = DirectFunctionCall3(
                    float8in, CStringGetDatum(buf), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
            }
            break;
        }
        case NUMERICOID: {
            result = DirectFunctionCall3(
                numeric_in, CStringGetDatum(buf), ObjectIdGetDatum(InvalidOid), Int32GetDatum(attr->atttypmod));
            break;
        }
        case TIMESTAMPOID: {
            result = DirectFunctionCall3(
                timestamp_in, CStringGetDatum(buf), ObjectIdGetDatum(InvalidOid), Int32GetDatum(attr->atttypmod));
            break;
        }
        case TIMESTAMPTZOID: {
            result = DirectFunctionCall3(
                timestamptz_in, CStringGetDatum(buf), ObjectIdGetDatum(InvalidOid), Int32GetDatum(attr->atttypmod));
            break;
        }
        case DATEOID: {
            result =
                DirectFunctionCall3(date_in, CStringGetDatum(buf), ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
            break;
        }
        case TEXTOID: {
            /* for text type input: */
            result =
                DirectFunctionCall2(pg_convert_from, CStringGetTextDatum((const char*)buf), CStringGetDatum(encoding));
            break;
        }
        case CHAROID: {
            /* it seems that, in MPPDB, we treat char(n) as BPCHAR internally other than CHAR */
            char* char_str = (char*)pg_do_encoding_conversion((unsigned char*)buf,
                strlen((char*)buf),
                pg_char_to_encoding((const char*)encoding),
                pg_char_to_encoding((const char*)t_thrd.conn_cxt._DatabaseEncoding->name));
            result = DirectFunctionCall3(charin,
                CStringGetDatum((const char*)char_str),
                ObjectIdGetDatum(InvalidOid),
                Int32GetDatum(attr->atttypmod));
            break;
        }
        case BPCHAROID: {
            /* for char(n) and nchar(n) type input: */
            char* bpchar_str = (char*)pg_do_encoding_conversion((unsigned char*)buf,
                strlen((char*)buf),
                pg_char_to_encoding((const char*)encoding),
                pg_char_to_encoding((const char*)t_thrd.conn_cxt._DatabaseEncoding->name));
            result = DirectFunctionCall3(bpcharin,
                CStringGetDatum((const char*)bpchar_str),
                ObjectIdGetDatum(InvalidOid),
                Int32GetDatum(attr->atttypmod));
            break;
        }
        default: /* should never run here, check_typeoid() has checked. */
        {
            result = odbc_type_2_Datum_sub(attr, buf, encoding);
        }
    }

    return result;
}

/*
 * @Description: initialize the enviroment of the odbc connection.
 *
 * @IN  value: funcctx, context during running c-function
 * @IN  value: fcinfo, c-function information
 */
void connect_odbc(
    const char* dsn, const char* user, const char* pass, FuncCallContext* funcctx, FunctionCallInfo fcinfo)
{
    MemoryContext oldcontext;
    TupleDesc tupdesc;
    AttInMetadata* attinmeta = NULL;

    /* switch to memory context appropriate for multiple function calls */
    oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    /* create a new memory context for making a tuple */
    funcctx->user_fctx = AllocSetContextCreate(funcctx->multi_call_memory_ctx,
        "odbc tuple context",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE);

    /* Build a tuple descriptor for our result type */
    if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE) {
        ereport(ERROR,
            (errmodule(MOD_EC),
                errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("function returning record called in context "
                       "that cannot accept type record")));
    }

    /* check column type */
    funcctx->tuple_desc = tupdesc;
    check_typeoid(tupdesc->attrs, tupdesc->natts);

    /*
     * generate attribute metadata needed later to produce tuples from raw
     * C strings
     */
    attinmeta = TupleDescGetAttInMetadata(tupdesc);
    funcctx->attinmeta = attinmeta;

    (void)MemoryContextSwitchTo(oldcontext);

    if (t_thrd.conn_cxt._conn) {
        clean_ec_conn();
        delete_ec_ctrl();
        ereport(
            ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Only support one ec function at the same time")));
    }

    /* load libodbc.so.1 or libodbc.so.2 */
    load_libodbc(fcinfo);

    t_thrd.conn_cxt._conn = NULL;
    t_thrd.conn_cxt._result = NULL;

    ODBC_TRY() {
        t_thrd.conn_cxt._conn = odbc_lib_connect(dsn, user, pass);
    }
    ODBC_CATCH(std::exception);

    ODBC_ERRREPORT("Fail to initialize the ODBC connection!", dsn);
}

/*
 * @Description: exec sql with odbc connection.
 *
 * @IN  value: funcctx, context during running c-function
 * @IN  value: fcinfo, c-function information
 */
void exec_odbc(const char* dsn, const char* sql, const char* encoding)
{
    Datum query;

    /* get query */
    query = DirectFunctionCall2(pg_convert_to, CStringGetTextDatum(sql), CStringGetDatum(encoding));

    ODBC_TRY() {
        t_thrd.conn_cxt._result = odbc_lib_query(t_thrd.conn_cxt._conn, text_to_cstring(DatumGetTextP(query)));
    }
    ODBC_CATCH(std::exception);

    ODBC_ERRREPORT("Fail to exec SQL with the ODBC connection!", dsn);
}

/*
 * @Description: get data from odbc server.
 *
 * @IN  value: funcctx, context during running c-function
 * @IN  value: isEnd, true if no more data returned.
 *
 * @return: the datum of the tuple
 */
Datum fetch_odbc(const char* dsn, FuncCallContext* funcctx, bool& isEnd, const char* encoding)
{
    MemoryContext tupleContext = (MemoryContext)funcctx->user_fctx;
    MemoryContextReset(tupleContext);
    MemoryContext oldcontext = MemoryContextSwitchTo(tupleContext);

    Size cols = (Size)funcctx->tuple_desc->natts;
    char** buffer = (char**)palloc0(cols * sizeof(char*));
    bool* nulls = (bool*)palloc0(cols * sizeof(bool));
    Oid* oid_types = (Oid*)palloc0(cols * sizeof(Oid));
    Datum* values = (Datum*)palloc0(cols * sizeof(Datum));

    for (Size i = 0; i < cols; i++) {
        oid_types[i] = funcctx->tuple_desc->attrs[i]->atttypid;
    }
    /* get one row from odbc */
    ODBC_TRY() {
        odbc_lib_get_result(t_thrd.conn_cxt._result, oid_types, (void**)buffer, nulls, cols, isEnd);
        if (isEnd) {
            (void)MemoryContextSwitchTo(oldcontext);
            return (Datum)NULL;
        }
    }
    ODBC_CATCH(std::exception);

    ODBC_ERRREPORT("Fail to get data from ODBC connection!", dsn);

    /* build a tuple */
    for (Size i = 0; i < cols; i++) {
        if (!nulls[i]) {
            values[i] = odbc_type_2_Datum(funcctx->tuple_desc->attrs[i], buffer[i], encoding);
        }
    }
    HeapTuple tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

    /* make the tuple into a datum */
    Datum result = HeapTupleGetDatum(tuple);

    (void)MemoryContextSwitchTo(oldcontext);

    return result;
}

/*
 * clean odbc enviroment and resource.
 */
void end_odbc(const char* dsn)
{
    if (t_thrd.conn_cxt._conn == NULL && t_thrd.conn_cxt._result == NULL) {
        return;
    }
    CONN_HANDLE tmp_conn = t_thrd.conn_cxt._conn;
    RESULT_HANDLE tmp_rs = t_thrd.conn_cxt._result;
    t_thrd.conn_cxt._conn = NULL;
    t_thrd.conn_cxt._result = NULL;

    ODBC_TRY() {
        odbc_lib_cleanup(tmp_conn, tmp_rs);
        unload_libodbc();
    }
    ODBC_CATCH(std::exception);

    ODBC_ERRREPORT("Fail to close ODBC connection!", dsn);
}
