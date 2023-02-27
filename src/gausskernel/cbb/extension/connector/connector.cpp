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
 * connector.cpp
 *	The entrance function of extension connector.
 *
 * 	There are two connect functions:
 *
 * 	* exec_on_extension(text, text)         -- by DN with privileges control
 * 	* exec_hadoop_sql(text, text, text)    -- by CN
 *
 * IDENTIFICATION
 *      Code/src/gausskernel/cbb/extension/connector/connector.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/builtins.h"
#include "datasource/datasource.h"
#include "miscadmin.h"
#include "parser/scansup.h"
#include "utils/acl.h"

#include "connector.h"
#include "gaussdb_version.h"
#include "odbc_connector.h"

PG_MODULE_MAGIC;

#define REMOTE_DEFAULT_ENCODING "UTF8"
#define DEFAULT_DRIVERTYPE "odbc"

/*
 * Key set in Extension Connector
 *
 * Note: length of the keyname must be less than NAMEDATALEN
 */
#define NAME_DSN "dsn"
#define NAME_SQL "sql"
#define NAME_USERNAME "username"
#define NAME_PASSWORD "password"
#define NAME_ENCODING "encoding"

/* Local functions */
static void init_key_value(void);
static void set_default_if_not_given(void);
static void read_one_key_value(const char* key, const char* value);
static void get_conn_info(FuncCallContext* funcctx, FunctionCallInfo fcinfo);
static void init_ec(FuncCallContext* funcctx, FunctionCallInfo fcinfo);
static void check_args3(FuncCallContext* funcctx, FunctionCallInfo fcinfo);
static void copy_ec_info(char* field, const char* info, int fieldMaxLen);

/*
 * Structures for Extension Connector Controler: a base class
 */
class ECControlBasic {
public:
    /* connect to the server */
    virtual void connect() = 0;

    /* execute the given statement */
    virtual void execute() = 0;

    /* fetch data */
    virtual Datum fetch(bool& isEnd) = 0;

    /* disconnect */
    virtual void end() = 0;

    virtual ~ECControlBasic(){};
};

/*
 * A Specific Extension Connector Controler based on ODBC
 */
class ECControlODBC : public ECControlBasic {
public:
    ECControlODBC(FuncCallContext* fctx, FunctionCallInfo finfo);
    ECControlODBC() : funcctx(NULL), fcinfo(NULL)
    {}
    virtual ~ECControlODBC() override {};

    /* main functions */
    void connect() override;
    void execute() override;
    Datum fetch(bool& isEnd);
    void end() override;

private:
    /* private function structure */
    FuncCallContext* funcctx;
    FunctionCallInfo fcinfo;
};

ECControlODBC::ECControlODBC(FuncCallContext* fctx, FunctionCallInfo finfo) : funcctx(fctx), fcinfo(finfo)
{}

/* specific connect function in ODBC */
void ECControlODBC::connect()
{
    /* call internal connect function */
    connect_odbc((const char*)t_thrd.conn_cxt.value_dsn,
        (const char*)t_thrd.conn_cxt.value_username,
        (const char*)t_thrd.conn_cxt.value_password,
        funcctx,
        fcinfo);
}

/* specific execute function in ODBC */
void ECControlODBC::execute()
{
    /* call internal execute function */
    exec_odbc((const char*)t_thrd.conn_cxt.value_dsn,
        (const char*)t_thrd.conn_cxt.value_sql,
        (const char*)t_thrd.conn_cxt.value_encoding);
}

/* specific fetch function in ODBC */
Datum ECControlODBC::fetch(bool& isEnd)
{
    /* call internal fetch function */
    return fetch_odbc(
        (const char*)t_thrd.conn_cxt.value_dsn, funcctx, isEnd, (const char*)t_thrd.conn_cxt.value_encoding);
}

/* specific end function in ODBC */
void ECControlODBC::end()
{
    /* Clear sensitive infos if any */
    if (t_thrd.conn_cxt.value_username != NULL) {
        errno_t ret;
        ret = memset_s(t_thrd.conn_cxt.value_username,
            strlen(t_thrd.conn_cxt.value_username),
            0,
            strlen(t_thrd.conn_cxt.value_username));
        securec_check(ret, "\0", "\0");
    }
    if (t_thrd.conn_cxt.value_password != NULL) {
        errno_t ret;
        ret = memset_s(t_thrd.conn_cxt.value_password,
            strlen(t_thrd.conn_cxt.value_password),
            0,
            strlen(t_thrd.conn_cxt.value_password));
        securec_check(ret, "\0", "\0");
    }

    /* call internal end function */
    end_odbc((const char*)t_thrd.conn_cxt.value_dsn);
}

/*
 * Get value of a key by a given string and its len
 */
#define GetKeyValueByString(name, value, p, len)                         \
    do {                                                                 \
        errno_t ret;                                                     \
        if ((value) != NULL)                                             \
            ereport(ERROR,                                               \
                (errmodule(MOD_EC),                                      \
                    errcode(ERRCODE_INVALID_PARAMETER_VALUE),            \
                    errmsg("key '%s' is given more than once!", name))); \
        value = (char*)palloc0((len) + 1);                               \
        ret = memcpy_s(value, (len) + 1, p, (len));                      \
        securec_check(ret, "\0", "\0");                                  \
    } while (0)

/*
 * init_key_value
 * 	initialize key value
 *
 * Note: This function must be called before reading any key-value
 */
static void init_key_value(void)
{
    t_thrd.conn_cxt.value_drivertype = NULL;
    t_thrd.conn_cxt.value_dsn = NULL;
    t_thrd.conn_cxt.value_username = NULL;
    t_thrd.conn_cxt.value_password = NULL;
    t_thrd.conn_cxt.value_sql = NULL;
    t_thrd.conn_cxt.value_encoding = NULL;
}

/*
 * set_default_if_not_given
 * 	Set default value of keys if not given
 *
 * Note: This function should be called after read key-value
 */
static void set_default_if_not_given(void)
{
    errno_t ret;
    int len;

    if (t_thrd.conn_cxt.value_encoding == NULL) {
        len = strlen(REMOTE_DEFAULT_ENCODING);
        t_thrd.conn_cxt.value_encoding = (char*)palloc0(len + 1);
        ret = memcpy_s(t_thrd.conn_cxt.value_encoding, len + 1, REMOTE_DEFAULT_ENCODING, len);
        securec_check(ret, "\0", "\0");
    }

    if (t_thrd.conn_cxt.value_drivertype == NULL) {
        len = strlen(DEFAULT_DRIVERTYPE);
        t_thrd.conn_cxt.value_drivertype = (char*)palloc0(len + 1);
        ret = memcpy_s(t_thrd.conn_cxt.value_drivertype, len + 1, DEFAULT_DRIVERTYPE, len);
        securec_check(ret, "\0", "\0");
    }
}

/*
 * read_one_key_value:
 * 	read one key-value from a given key and value
 *
 * @IN key:  key name
 * @IN value: value of the key
 * @RETURN void
 *
 * Note: We don't report error if the key doesn't match any keys in EC system,
 * 	because those keys may be used in some objects elsewhere.
 */
static void read_one_key_value(const char* key, const char* value)
{
    size_t len;

    if (key == NULL || value == NULL)
        return;
    len = strlen(value);

    /* Read the key-value */
    if (0 == strcasecmp(key, NAME_DSN)) {
        GetKeyValueByString(NAME_DSN, t_thrd.conn_cxt.value_dsn, value, len);
    } else if (0 == strcasecmp(key, NAME_ENCODING)) {
        GetKeyValueByString(NAME_ENCODING, t_thrd.conn_cxt.value_encoding, value, len);
    } else if (0 == strcasecmp(key, NAME_USERNAME)) {
        errno_t errCode;
        char *plainuid = NULL;

        /* If an empty username is given, just return */
        if (len == 0)
            return;

        /* Decrypt username */
        decryptECString(value, &plainuid, SOURCE_MODE);
        GetKeyValueByString(NAME_USERNAME, t_thrd.conn_cxt.value_username, plainuid, len);

        /* Clear buffer */
        errCode = memset_s(plainuid, strlen(plainuid), 0, strlen(plainuid));
        securec_check(errCode, "\0", "\0");
        pfree(plainuid);
    } else if (0 == strcasecmp(key, NAME_PASSWORD)) {
        errno_t errCode;
        char *plainpwd = NULL;

        /* If an empty password is given, just return */
        if (len == 0)
            return;

        /* Decrypt password */
        decryptECString(value, &plainpwd, SOURCE_MODE);
        GetKeyValueByString(NAME_PASSWORD, t_thrd.conn_cxt.value_password, plainpwd, len);

        /* Clear buffer */
        errCode = memset_s(plainpwd, strlen(plainpwd), 0, strlen(plainpwd));
        securec_check(errCode, "\0", "\0");
        pfree(plainpwd);
    } else
        return; /* just skip unknown keys */
}

/*
 * get_conn_info:
 * 	get connection information, which is defined by the format of KV
 *
 * @IN funcctx: function call context
 * @IN fcinfo: function call info
 * @RETURN void
 */
static void get_conn_info(FuncCallContext* funcctx, FunctionCallInfo fcinfo)
{
    DataSource* data_source = NULL;
    MemoryContext oldcontext;
    ListCell* cell = NULL;
    AclResult aclresult;
    Oid sourceid;
    Oid userid;
    char* srcname = NULL;
    char* str = NULL;
    int len;

    /* Switch to the context hold by funcctx */
    oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    /* Init keys */
    init_key_value();

    /*
     * Parameter-1:  Data Source Object
     */
    if (PG_ARGISNULL(0)) {
        ereport(ERROR,
            (errmodule(MOD_EC),
                errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("missing data source to be connected for first parameter.")));
    }
    str = text_to_cstring(PG_GETARG_TEXT_PP(0));
    srcname = pstrdup(str);
    truncate_identifier(srcname, strlen(srcname), true);

    /* Get data source structure */
    data_source = GetDataSourceByName(srcname, false);
    if (data_source == NULL)
        ereport(ERROR,
            (errmodule(MOD_EC),
                errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("data source \"%s\" does not exist", srcname)));

    /* Check privileges, user must have USAGE on the data source */
    userid = GetUserId();
    sourceid = data_source->sourceid;
    aclresult = pg_extension_data_source_aclcheck(sourceid, userid, ACL_USAGE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, ACL_KIND_DATA_SOURCE, data_source->srcname);

    /* Read options from data source */
    foreach (cell, data_source->options) {
        DefElem* def = (DefElem*)lfirst(cell);
        read_one_key_value(def->defname, strVal(def->arg));
    }

    /*
     * Parameter-2:  Statement to be executed
     */
    if (PG_ARGISNULL(1)) {
        ereport(ERROR,
            (errmodule(MOD_EC),
                errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("missing statement to be executed for second parameter.")));
    }
    str = text_to_cstring(PG_GETARG_TEXT_PP(1));
    len = strlen(str);
    GetKeyValueByString(NAME_SQL, t_thrd.conn_cxt.value_sql, str, len);

    /* Set default values of keys if not given */
    set_default_if_not_given();

    MemoryContextSwitchTo(oldcontext);
}

void clean_ec_conn(void)
{
    if (t_thrd.conn_cxt.ecCtrl)
        t_thrd.conn_cxt.ecCtrl->end();
}

void delete_ec_ctrl(void)
{
    if (t_thrd.conn_cxt.ecCtrl) {
        delete t_thrd.conn_cxt.ecCtrl;
        t_thrd.conn_cxt.ecCtrl = NULL;
    }
}

/*
 * init_ec:
 * 	Check keys and create a ECControler
 *
 * @IN funcctx: Function Call Context
 * @IN fcinfo: FunctionCallInfo
 * @RETURN: void
 */
static void init_ec(FuncCallContext* funcctx, FunctionCallInfo fcinfo)
{
    MemoryContext oldcontext;

    /* Set: Fllowing actions are done in the func context */
    oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    /* Driver type can be given by user; if not given, it will use default value. */
    AssertEreport((t_thrd.conn_cxt.value_drivertype != NULL), MOD_EC, "driver is NULL");

    /* Classify the Job: according to given keys */
    if (0 == strcasecmp(t_thrd.conn_cxt.value_drivertype, "odbc")) {
        /*
         * Job based on ODBC.
         *
         * We connect to other server by ODBC-driver,
         * execute sql or other statements in the remote server,
         * and get what we want.
         */
        if (t_thrd.conn_cxt.value_dsn == NULL) {
            /* DSN must be given by user */
            ereport(ERROR,
                (errmodule(MOD_EC),
                    errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("for ODBC driver, we need a DSN!")));
        }

        /* Job-1: SQL job on ODBC */
        AssertEreport((t_thrd.conn_cxt.value_sql != NULL), MOD_EC, "input sql is NULL");

        delete_ec_ctrl();
        t_thrd.conn_cxt.ecCtrl = new ECControlODBC(funcctx, fcinfo);
        if (unlikely(t_thrd.conn_cxt.ecCtrl == NULL)) {
            ereport(ERROR, (errmodule(MOD_EC), errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                errmsg("Initialize ODBC Connector failed due to insufficient memory.")));
        }
    }

    MemoryContextSwitchTo(oldcontext);
}

/*
 * check_args3
 * 	check the arguments (total 3 parameters) for old function exec_hadoop_sql
 *
 * @IN funcctx: FuncCallContext*, for Set Returning Functions
 * @IN fcinfo: FunctionCallInfo
 * @return void
 */
static void check_args3(FuncCallContext* funcctx, FunctionCallInfo fcinfo)
{
    char* str = NULL;
    Size len;
    MemoryContext oldcontext;

    /* Buffer for Keys are set in the func context */
    oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

    init_key_value();

    /*
     * argu_1: DSN
     */
    if (PG_ARGISNULL(0)) {
        ereport(
            ERROR, (errmodule(MOD_EC), errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("DSN should not be NULL!")));
    } else {
        str = text_to_cstring(PG_GETARG_TEXT_PP(0));
        len = strlen(str);
        if (unlikely(len == 0)) {
            ereport(ERROR, (errmodule(MOD_EC), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Invalid string length")));
        }
        GetKeyValueByString(NAME_DSN, t_thrd.conn_cxt.value_dsn, str, len);
    }

    /*
     * argu_2: SQL
     */
    if (PG_ARGISNULL(1)) {
        ereport(ERROR, (errmodule(MOD_EC), errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("SQL can not be null!")));
    } else {
        str = text_to_cstring(PG_GETARG_TEXT_PP(1));
        len = strlen(str);
        if (unlikely(len == 0)) {
            ereport(ERROR, (errmodule(MOD_EC), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Invalid string length")));
        }
        GetKeyValueByString(NAME_SQL, t_thrd.conn_cxt.value_sql, str, len);
    }

    /*
     * argu_3: Encoding name
     */
    if (PG_ARGISNULL(2)) {
        /* use default encoding */
        errno_t ret;
        len = strlen(REMOTE_DEFAULT_ENCODING);
        t_thrd.conn_cxt.value_encoding = (char*)palloc0(len + 1);
        ret = memcpy_s(t_thrd.conn_cxt.value_encoding, len + 1, REMOTE_DEFAULT_ENCODING, len);
        securec_check(ret, "\0", "\0");
    } else {
        str = text_to_cstring(PG_GETARG_TEXT_PP(2));
        len = strlen(str);
        if (unlikely(len == 0)) {
            ereport(ERROR, (errmodule(MOD_EC), errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("Invalid string length")));
        }
        GetKeyValueByString(NAME_ENCODING, t_thrd.conn_cxt.value_encoding, str, len);
    }

    MemoryContextSwitchTo(oldcontext);
}

/*
 * copy_ec_info
 * 	copy ec info to instrumentation field for ec view
 *
 * @IN field: char, instrumentation field
 * @IN ecInfo: char *, ec info
 * @IN fieldMaxLen: int, instrumentation field max len
 * @return void
 */
static void copy_ec_info(char* field, const char* ecInfo, int fieldMaxLen)
{
    errno_t rc = EOK;
    if (ecInfo != NULL) {
        rc = strncpy_s(field, fieldMaxLen, ecInfo, fieldMaxLen - 1);
        securec_check(rc, "\0", "\0");
        field[fieldMaxLen - 1] = '\0';
    } else {
        field[0] = '\0';
    }
}

PG_FUNCTION_INFO_V1(exec_on_extension);
PG_FUNCTION_INFO_V1(exec_hadoop_sql);

void check_ec_supported()
{
    if (is_feature_disabled(EXTENSION_CONNECTOR) == true) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Extension Connector is not supported in current version.")));
    }
}

void record_ec_info(FunctionScanState* node)
{
    /* record ec info for EC view in active sql */
    if (node != NULL && node->ss.ps.instrument) {
        /* fetching */
        node->ss.ps.instrument->ec_status = EC_STATUS_FETCHING;
        (node->ss.ps.instrument->ec_fetch_count)++;
    }
}

/*
 * exec_hadoop_sql:
 * 	Entrance function (old version) of the connector by odbc
 *
 * @IN  text: "DSN"
 * @IN  text: "SQL" (query string)
 * @IN  text: "Encoding_Name" (to/from server, include sql and results,
 * 	default value is "UTF8")
 *
 * @RETURN: set of record, and vary with query string
 *
 * Note: here we just keep the old fashion of the connector
 */
Datum exec_hadoop_sql(PG_FUNCTION_ARGS)
{
    if (!superuser()) {
        ereport(ERROR,
            (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                errmsg("must be system admin to use this function"),
                errhint("This function may have security risk, please use exec_on_extension instead.")));
    }

    if (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-support feature in B compatibility")));
    }

    FuncCallContext* funcctx = NULL;
    Datum result;
    bool isend = false;
    FunctionScanState* node = (FunctionScanState*)fcinfo->context;

    check_ec_supported();

    /* Stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        /* Create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /* Get the DSN */
        check_args3(funcctx, fcinfo);

        /* record ec info for EC view in active sql */
        if (node != NULL && node->ss.ps.instrument != NULL) {
            /* init ec info */
            node->ss.ps.instrument->ec_operator = IS_EC_OPERATOR; /* 0 false 1 true */
            node->ss.ps.instrument->ec_status = EC_STATUS_INIT;
            node->ss.ps.instrument->ec_fetch_count = 0;
        }

        /* Connect to the server, and execute the statement */
        connect_odbc((const char*)t_thrd.conn_cxt.value_dsn,
            (const char*)t_thrd.conn_cxt.value_username,
            (const char*)t_thrd.conn_cxt.value_password,
            funcctx,
            fcinfo);

        /* record ec info for EC view in active sql */
        if (node != NULL && node->ss.ps.instrument) {
            /* connected */
            node->ss.ps.instrument->ec_status = EC_STATUS_CONNECTED;

            copy_ec_info(
                node->ss.ps.instrument->ec_execute_datanode, g_instance.attr.attr_common.PGXCNodeName, NAMEDATALEN + 1);
            copy_ec_info(node->ss.ps.instrument->ec_dsn, t_thrd.conn_cxt.value_dsn, NAMEDATALEN + 1);
            copy_ec_info(node->ss.ps.instrument->ec_username, t_thrd.conn_cxt.value_username, NAMEDATALEN + 1);
            copy_ec_info(node->ss.ps.instrument->ec_query, t_thrd.conn_cxt.value_sql, ECQUERYDATALEN + 1);
        }

        exec_odbc((const char*)t_thrd.conn_cxt.value_dsn,
            (const char*)t_thrd.conn_cxt.value_sql,
            (const char*)t_thrd.conn_cxt.value_encoding);

        /* record ec info for EC view in active sql */
        if (node != NULL && node->ss.ps.instrument) {
            /* executed */
            node->ss.ps.instrument->ec_status = EC_STATUS_EXECUTED;
        }
    }

    /* Stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    /* Get result */
    result =
        fetch_odbc((const char*)t_thrd.conn_cxt.value_dsn, funcctx, isend, (const char*)t_thrd.conn_cxt.value_encoding);
    if (!isend) {
        record_ec_info(node);

        SRF_RETURN_NEXT(funcctx, result);
    } else {
        end_odbc((const char*)t_thrd.conn_cxt.value_dsn);

        /* record ec info for EC view in active sql */
        if (node != NULL && node->ss.ps.instrument) {
            /* end */
            node->ss.ps.instrument->ec_status = EC_STATUS_END;
        }

        SRF_RETURN_DONE(funcctx);
    }
}

/*
 * exec_on_extension:
 * 	Entrance Function of the Extension Connector
 *
 * @IN text: server name
 * @IN text: a statement to be executed
 * @RETURN: set of record
 *
 * Note:
 * 	Here we manage the authority by using Data Source Object
 */
Datum exec_on_extension(PG_FUNCTION_ARGS)
{
    if (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("Un-support feature in B compatibility")));
    }
    FuncCallContext* funcctx = NULL;
    Datum result;
    bool isEnd = false;
    FunctionScanState* node = (FunctionScanState*)fcinfo->context;

    check_ec_supported();

    /* Stuff done only on the first call of the function */
    if (SRF_IS_FIRSTCALL()) {
        /* Create a function context for cross-call persistence */
        funcctx = SRF_FIRSTCALL_INIT();

        /* Init and create a EC Controler */
        get_conn_info(funcctx, fcinfo);
        init_ec(funcctx, fcinfo);

        /* init ec info */
        if (node != NULL && node->ss.ps.instrument != NULL) {
            node->ss.ps.instrument->ec_operator = IS_EC_OPERATOR; /* 0 false 1 true */
            node->ss.ps.instrument->ec_status = EC_STATUS_INIT;
            node->ss.ps.instrument->ec_fetch_count = 0;
        }

        /* Connect to the server, and create a connect handler */
        t_thrd.conn_cxt.ecCtrl->connect();

        /* record ec info for EC view in active sql */
        if (node != NULL && node->ss.ps.instrument) {
            /* connected */
            node->ss.ps.instrument->ec_status = EC_STATUS_CONNECTED;

            copy_ec_info(
                node->ss.ps.instrument->ec_execute_datanode, g_instance.attr.attr_common.PGXCNodeName, NAMEDATALEN + 1);
            copy_ec_info(node->ss.ps.instrument->ec_dsn, t_thrd.conn_cxt.value_dsn, NAMEDATALEN + 1);
            copy_ec_info(node->ss.ps.instrument->ec_username, t_thrd.conn_cxt.value_username, NAMEDATALEN + 1);
            copy_ec_info(node->ss.ps.instrument->ec_query, t_thrd.conn_cxt.value_sql, ECQUERYDATALEN + 1);
        }

        /* Send the statement to the server and execute */
        t_thrd.conn_cxt.ecCtrl->execute();

        /* record ec info for EC view in active sql */
        if (node != NULL && node->ss.ps.instrument) {
            /* executed */
            node->ss.ps.instrument->ec_status = EC_STATUS_EXECUTED;
        }
    }

    /* Stuff done on every call of the function */
    funcctx = SRF_PERCALL_SETUP();

    /* Get result */
    result = t_thrd.conn_cxt.ecCtrl->fetch(isEnd);

    if (!isEnd) {
        record_ec_info(node);

        /* May be more result */
        SRF_RETURN_NEXT(funcctx, result);
    } else {
        /* No more: disconnect and clean up */
        t_thrd.conn_cxt.ecCtrl->end();
        delete_ec_ctrl();

        /* record ec info for EC view in active sql */
        if (node != NULL && node->ss.ps.instrument) {
            /* end */
            node->ss.ps.instrument->ec_status = EC_STATUS_END;
        }

        SRF_RETURN_DONE(funcctx);
    }
}
