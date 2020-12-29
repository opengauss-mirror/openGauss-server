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
 * odbc.cpp
 *       The implementations of all classes which hide details of the ODBC APIs.
 *
 * IDENTIFICATION
 *       Code/src/gausskernel/cbb/extension/connector/odbc.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <algorithm>
#include <cassert>
#include <clocale>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <map>

#include "postgres.h"
#include "knl/knl_variable.h"
#include <sqlext.h>
#include "odbc.h"
#include "odbc_bridge.h"
#include "catalog/pg_type.h"

#define MYODBC_ASSERT(expr) assert(expr)

/* Default to ODBC version defined by MYODBC_ODBC_VERSION if provided. */
#ifndef MYODBC_ODBC_VERSION
#ifdef SQL_OV_ODBC3_80
/* Otherwise, use ODBC v3.8 if it's available... */
#define MYODBC_ODBC_VERSION SQL_OV_ODBC3_80
#else
/* or fallback to ODBC v3.x. */
#define MYODBC_ODBC_VERSION SQL_OV_ODBC3
#endif
#endif

#define MYODBC_TEXT(s) s
#define MYODBC_FUNC(f) f
#define MYODBC_SQLCHAR SQLCHAR

typedef std::u16string wide_string_type;
#define MYODBC_CODECVT_TYPE std::codecvt_utf8_utf16
typedef wide_string_type::value_type wide_char_t;

#define MYODBC_STRINGIZE_I(text) #text
#define MYODBC_STRINGIZE(text) MYODBC_STRINGIZE_I(text)

/*
 * By making all calls to ODBC functions through this macro, we can easily get
 * runtime debugging information of which ODBC functions are being called,
 * in what order, and with what parameters by defining MYODBC_ODBC_API_DEBUG.
 */
#ifdef MYODBC_ODBC_API_DEBUG
#include <iostream>
#define MYODBC_CALL_RC(FUNC, RC, ...)                                                                          \
    do {                                                                                                       \
        std::cerr << __FILE__ ":" MYODBC_STRINGIZE(__LINE__) " " MYODBC_STRINGIZE((FUNC)) "(" #__VA_ARGS__ ")" \
                  << std::endl;                                                                                \
        RC = (FUNC)(__VA_ARGS__);                                                                              \
    } while (false)

#define MYODBC_CALL(FUNC, ...)                                                                                 \
    do {                                                                                                       \
        std::cerr << __FILE__ ":" MYODBC_STRINGIZE(__LINE__) " " MYODBC_STRINGIZE((FUNC)) "(" #__VA_ARGS__ ")" \
                  << std::endl;                                                                                \
        (FUNC)(__VA_ARGS__);                                                                                   \
    } while (false)

#else
#define MYODBC_CALL_RC(FUNC, RC, ...)                                                                                 \
    do {                                                                                                              \
        ereport(DEBUG2, (errmodule(MOD_EC), errmsg("CALL_RC ODBC: " MYODBC_STRINGIZE((FUNC)) "(" #__VA_ARGS__ ")"))); \
        RC = (FUNC)(__VA_ARGS__);                                                                                     \
    } while (false)
#define MYODBC_CALL(FUNC, ...)                                                                                     \
    do {                                                                                                           \
        ereport(DEBUG2, (errmodule(MOD_EC), errmsg("CALL ODBC: " MYODBC_STRINGIZE((FUNC)) "(" #__VA_ARGS__ ")"))); \
        (FUNC)(__VA_ARGS__);                                                                                       \
    } while (false)
#endif

const int SQL_TINYINT_LEN = 8;
const int SQL_SMALLINT_LEN = 32;
const int SQL_BIGINT_LEN = 64;
const int INTERVAL_SECOND_LEN = 4;

namespace {
/* Returns the array size. */
template <typename T, std::size_t N>
inline std::size_t arrlen(T (&)[N])
{
    return N;
}

/* Easy way to check if a  return  code signifies success. */
inline bool success(RETCODE rc)
{
#ifdef MYODBC_ODBC_API_DEBUG
    std::cerr << "<-- rc: " << return _code(rc) << " | ";
#endif
    return rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO;
}

#ifdef MYODBC_ODBC_API_DEBUG
inline myodbc::string_type return _code(RETCODE rc)
{
    switch (rc) {
        case SQL_SUCCESS:
            return MYODBC_TEXT("SQL_SUCCESS");
        case SQL_SUCCESS_WITH_INFO:
            return MYODBC_TEXT("SQL_SUCCESS_WITH_INFO");
        case SQL_ERROR:
            return MYODBC_TEXT("SQL_ERROR");
        case SQL_INVALID_HANDLE:
            return MYODBC_TEXT("SQL_INVALID_HANDLE");
        case SQL_NO_DATA:
            return MYODBC_TEXT("SQL_NO_DATA");
        case SQL_NEED_DATA:
            return MYODBC_TEXT("SQL_NEED_DATA");
        case SQL_STILL_EXECUTING:
            return MYODBC_TEXT("SQL_STILL_EXECUTING");
        default:
            break;
    }
    MYODBC_ASSERT(0);
    return "unknown";
}
#endif

/* Operates like strlen() on a character array. */
template <typename T, std::size_t N>
inline std::size_t strarrlen(T (&a)[N])
{
    const T* s = &a[0];
    std::size_t i = 0;
    while (*s++ && i < N) {
        i++;
    }
    return i;
}

/* Convert result by given string */
inline void convert(const std::string& in, std::string& out)
{
    out = in;
}

/*
 * Attempts to get the most recent ODBC error as a string.
 * Always  return s std::string
 */
inline std::string recent_error(SQLHANDLE handle, SQLSMALLINT handle_type, long& native, std::string& state)
{
    myodbc::string_type result;
    std::string rvalue;
    std::vector<MYODBC_SQLCHAR> sql_message(SQL_MAX_MESSAGE_LENGTH);
    sql_message[0] = '\0';

    SQLINTEGER i = 1;
    SQLINTEGER native_error;
    SQLSMALLINT total_bytes;
    MYODBC_SQLCHAR sql_state[6];
    RETCODE rc;

    do {
        MYODBC_CALL_RC(MYODBC_FUNC(*pSQLGetDiagRec),
            rc,
            handle_type,
            handle,
            (SQLSMALLINT)i,
            sql_state,
            &native_error,
            0,
            0,
            &total_bytes);

        if (success(rc) && total_bytes > 0) {
            sql_message.resize(total_bytes + 1);
        }

        if (rc == SQL_NO_DATA) {
            break;
        }

        MYODBC_CALL_RC(MYODBC_FUNC(*pSQLGetDiagRec),
            rc,
            handle_type,
            handle,
            (SQLSMALLINT)i,
            sql_state,
            &native_error,
            sql_message.data(),
            (SQLSMALLINT)sql_message.size(),
            &total_bytes);
        if (!success(rc)) {
            convert(result, rvalue);
            return rvalue;
        }

        if (!result.empty()) {
            result += '\n';
        }

        result += myodbc::string_type(sql_message.begin(), sql_message.end());
        i++;
    } while (rc != SQL_NO_DATA);

    convert(result, rvalue);

    SQLCHAR* errmsg = &sql_state[0];
    SQLCHAR* errmsg_endpos = &sql_state[arrlen(sql_state) - 1];
    state = std::string(errmsg, errmsg_endpos);

    std::string status = state;
    status = status + ": ";
    status = status + rvalue;

    std::replace(status.begin(), status.end(), '\0', ' ');

    native = native_error;

    return status;
}
}  // namespace

/*
 * These exceptions below will be catch in ODBC_CATCH() in odbc_connector.cpp
 * so no any exception in this file can be lost.
 */
namespace myodbc {
void type_incompatible_error(int ctype, int column)
{
    ereport(ERROR,
        (errmodule(MOD_EC),
            errcode(ERRCODE_DATATYPE_MISMATCH),
            errmsg("Column %d receive unexpected C_TYPE %d", column, ctype)));
}

void null_access_error(int column)
{
    ereport(ERROR,
        (errmodule(MOD_EC), errcode(ERRCODE_DATA_EXCEPTION), errmsg("unexpected null string in column %d", column)));
}

void index_range_error()
{
    ereport(ERROR,
        (errmodule(MOD_EC),
            errcode(ERRCODE_TOO_MANY_COLUMNS),
            errmsg("query in function exec_on_extension returns too many columns")));
}

database_error::database_error(void* handle, short handle_type, const std::string& info)
    : std::runtime_error(info), native_error(0), sql_state("00000")
{
    message = std::string(std::runtime_error::what()) + recent_error(handle, handle_type, native_error, sql_state);
}

database_error::~database_error(void)
{}

const char* database_error::what() const MYODBC_NOEXCEPT
{
    return message.c_str();
}

const long database_error::native() const MYODBC_NOEXCEPT
{
    return native_error;
}

const std::string database_error::state() const MYODBC_NOEXCEPT
{
    return sql_state;
}
}  // namespace myodbc

/* Throwing exceptions using MYODBC_THROW_DATABASE_ERROR enables file name
 * and line numbers to be inserted into the error message. Useful for debugging.
 */
#define MYODBC_THROW_DATABASE_ERROR(handle, handle_type) \
    throw myodbc::database_error(handle, handle_type, __FILE__ ":" MYODBC_STRINGIZE(__LINE__) ": ")

namespace {
using namespace std;

/*
 * Encapsulates resources needed for column binding.
 * This class will be used to describe the attribute of column
 */
class bound_column {
public:
    bound_column(const bound_column&) = delete;
    bound_column& operator=(bound_column) = delete;

    bound_column()
        : name_(),
          column_(0),
          ctype_(0),
          clen_(0),
          sqltype_(0),
          scale_(0),
          sqlsize_(0),
          blob_(false),
          pdata_(0),
          cbdata_(0)
    {}

    ~bound_column()
    {
        delete[] pdata_;
        delete[] cbdata_;
    }

public:
    myodbc::string_type name_;
    short column_;
    SQLSMALLINT ctype_;
    SQLULEN clen_;
    SQLSMALLINT sqltype_;
    SQLSMALLINT scale_;
    SQLULEN sqlsize_;
    bool blob_;
    char* pdata_;
    myodbc::null_type* cbdata_;
};

/* Allocates the native ODBC handles. */
inline void allocate_handle(SQLHENV& env, SQLHDBC& conn)
{
    RETCODE rc;
    MYODBC_CALL_RC(*pSQLAllocHandle, rc, SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    if (!success(rc)) {
        MYODBC_THROW_DATABASE_ERROR(env, SQL_HANDLE_ENV);
    }

    try {
        MYODBC_CALL_RC(
            *pSQLSetEnvAttr, rc, env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)MYODBC_ODBC_VERSION, SQL_IS_UINTEGER);
        if (!success(rc)) {
            MYODBC_THROW_DATABASE_ERROR(env, SQL_HANDLE_ENV);
        }

        MYODBC_CALL_RC(*pSQLAllocHandle, rc, SQL_HANDLE_DBC, env, &conn);
        if (!success(rc)) {
            MYODBC_THROW_DATABASE_ERROR(env, SQL_HANDLE_ENV);
        }
    } catch (...) {
        MYODBC_CALL(*pSQLFreeHandle, SQL_HANDLE_ENV, env);
        throw;
    }
}
}  // namespace

namespace myodbc {
/*
 * Implementation of connection class: connection_impl
 */
class connection::connection_impl {
public:
    connection_impl(const connection_impl&) = delete;
    connection_impl& operator=(const connection_impl&) = delete;

    connection_impl() : env_(0), conn_(0), connected_(false)
    {
        allocate_handle(env_, conn_);
    }

    /* Entrance of connection implementation */
    connection_impl(const string_type& dsn, const string_type& user, const string_type& pass, long timeout)
        : env_(0), conn_(0), connected_(false)
    {
        allocate_handle(env_, conn_);
        try {
            connect(dsn, user, pass, timeout);
        } catch (...) {
            MYODBC_CALL(*pSQLFreeHandle, SQL_HANDLE_DBC, conn_);
            MYODBC_CALL(*pSQLFreeHandle, SQL_HANDLE_ENV, env_);
            throw;
        }
    }

    /* destroy the class */
    ~connection_impl() MYODBC_NOEXCEPT
    {
        try {
            disconnect();
        } catch (...) {
        }
        MYODBC_CALL(*pSQLFreeHandle, SQL_HANDLE_DBC, conn_);
        MYODBC_CALL(*pSQLFreeHandle, SQL_HANDLE_ENV, env_);
    }

    /*
     * Connection Implementation:
     * 	connect to server with dsn, username, password and timeout
     */
    void connect(const string_type& dsn, const string_type& user, const string_type& pass, long timeout,
        void* event_handle = NULL)
    {
        /* first disconnect if necessary */
        disconnect();

        RETCODE rc;
        MYODBC_CALL_RC(*pSQLFreeHandle, rc, SQL_HANDLE_DBC, conn_);
        if (!success(rc)) {
            MYODBC_THROW_DATABASE_ERROR(conn_, SQL_HANDLE_DBC);
        }

        MYODBC_CALL_RC(*pSQLAllocHandle, rc, SQL_HANDLE_DBC, env_, &conn_);
        if (!success(rc)) {
            MYODBC_THROW_DATABASE_ERROR(env_, SQL_HANDLE_ENV);
        }

        MYODBC_CALL_RC(*pSQLSetConnectAttr, rc, conn_, SQL_LOGIN_TIMEOUT, (SQLPOINTER)(std::intptr_t)timeout, 0);
        if (!success(rc)) {
            MYODBC_THROW_DATABASE_ERROR(conn_, SQL_HANDLE_DBC);
        }

        MYODBC_CALL_RC(MYODBC_FUNC(*pSQLConnect),
            rc,
            conn_,
            (MYODBC_SQLCHAR*)dsn.c_str(),
            SQL_NTS,
            !user.empty() ? (MYODBC_SQLCHAR*)user.c_str() : 0,
            SQL_NTS,
            !pass.empty() ? (MYODBC_SQLCHAR*)pass.c_str() : 0,
            SQL_NTS);
        if (!success(rc) && (event_handle == NULL || rc != SQL_STILL_EXECUTING)) {
            MYODBC_THROW_DATABASE_ERROR(conn_, SQL_HANDLE_DBC);
        }

        /* save the connection status */
        connected_ = success(rc);
    }

    /* check the status of connection: success or failure */
    bool connected() const
    {
        return connected_;
    }

    /* disconnect */
    void disconnect()
    {
        if (connected()) {
            RETCODE rc;
            MYODBC_CALL_RC(*pSQLDisconnect, rc, conn_);
            if (!success(rc)) {
                MYODBC_THROW_DATABASE_ERROR(conn_, SQL_HANDLE_DBC);
            }
        }
        connected_ = false;
    }

    /* get connection handle */
    void* native_dbc_handle() const
    {
        return conn_;
    }

    /* get environment handle */
    void* native_env_handle() const
    {
        return env_;
    }

public:
    HENV env_;
    HDBC conn_;

private:
    bool connected_;
};

}  // namespace myodbc

namespace myodbc {
/*
 * Implementation of statement class: statement_impl
 */
class statement::statement_impl {
public:
    statement_impl(const statement_impl&) = delete;
    statement_impl& operator=(const statement_impl&) = delete;

    /* initialize */
    statement_impl() : stmt_(0), remote_ncols_(-1), open_(false), conn_()
    {}

    /* destroy the class */
    ~statement_impl() MYODBC_NOEXCEPT
    {
        if (open() && connected()) {
            MYODBC_CALL(*pSQLCancel, stmt_);
            MYODBC_CALL(*pSQLFreeHandle, SQL_HANDLE_STMT, stmt_);
        }
    }

    /*
     * Close old connection and reopen one new connection
     */
    void open(class connection& conn)
    {
        close();
        RETCODE rc;
        MYODBC_CALL_RC(*pSQLAllocHandle, rc, SQL_HANDLE_STMT, conn.native_dbc_handle(), &stmt_);
        open_ = success(rc);
        if (!open_) {
            MYODBC_THROW_DATABASE_ERROR(stmt_, SQL_HANDLE_STMT);
        }
        conn_ = conn;
    }

    /* check if the connection is opened (prepared) */
    bool open() const
    {
        return open_;
    }

    /* check if the connection is success */
    bool connected() const
    {
        return conn_.connected();
    }

    /* close current connection and free resources */
    void close()
    {
        if (open() && connected()) {
            RETCODE rc;
            MYODBC_CALL_RC(*pSQLCancel, rc, stmt_);
            if (!success(rc)) {
                MYODBC_THROW_DATABASE_ERROR(stmt_, SQL_HANDLE_STMT);
            }

            MYODBC_CALL_RC(*pSQLFreeHandle, rc, SQL_HANDLE_STMT, stmt_);
            if (!success(rc)) {
                MYODBC_THROW_DATABASE_ERROR(stmt_, SQL_HANDLE_STMT);
            }
        }

        remote_ncols_ = -1;
        open_ = false;
        stmt_ = 0;
    }

    /*
     * Entrance of the execution
     */
    result execute_direct(
        class connection& conn, const string_type& query, long batch_operations, long timeout, statement& statement)
    {
        just_execute_direct(conn, query, batch_operations, timeout, statement);
        return result(statement, batch_operations);
    }

    /*
     * Actual execution function
     */
    RETCODE just_execute_direct(class connection& conn, const string_type& query, long batch_operations, long timeout,
        statement&, /* statement */ 
        void* event_handle = NULL)
    {
        /* open: allocate handler */
        open(conn);
        RETCODE rc;

        /* execute: send command to server */
        MYODBC_CALL_RC(MYODBC_FUNC(*pSQLExecDirect), rc, stmt_, (MYODBC_SQLCHAR*)query.c_str(), SQL_NTS);
        if (!success(rc) && rc != SQL_NO_DATA && rc != SQL_STILL_EXECUTING) {
            MYODBC_THROW_DATABASE_ERROR(stmt_, SQL_HANDLE_STMT);
        }

        /* return the status */
        return rc;
    }

    /* get the column number of the result set */
    short columns()
    {
        if (remote_ncols_ == -1) {
            RETCODE rc;
            MYODBC_CALL_RC(*pSQLNumResultCols, rc, stmt_, &remote_ncols_);
            if (!success(rc)) {
                MYODBC_THROW_DATABASE_ERROR(stmt_, SQL_HANDLE_STMT);
            }
        }

        return remote_ncols_;
    }

    /* get statement handle */
    void* native_statement_handle() const
    {
        return stmt_;
    }

public:
    HSTMT stmt_;

private:
    SQLSMALLINT remote_ncols_;
    bool open_;
    class connection conn_;
};

}  // namespace myodbc

namespace myodbc {

/*
 * Implementation of result class: result_impl
 */
class result::result_impl {

public:
    result_impl(const result_impl& impl) = delete;
    result_impl& operator=(const result_impl& impl) = delete;

    result_impl(statement stmtobj, long rowset_len)
        : stmt_(stmtobj),
          rowset_length(rowset_len),
          row_number(1),
          bound_columns_(0),
          bound_columns_length(0),
          rowset_pos(0),
          bound_columns_by_name(),
          at_end(false)
    {
        /*
         * Bind the column attribute with results from the server
         */
        SQLLEN bufferSize = sizeof(blob_buffer);
        errno_t errorno = memset_s(blob_buffer, bufferSize, 0, bufferSize);
        securec_check(errorno, "\0", "\0");
        auto_bind();
    }

    /* destroy the class and free the buffer */
    ~result_impl() MYODBC_NOEXCEPT
    {
        cleanup_bound_columns();
    }

    /* get the statment handle */
    void* native_statement_handle() const
    {
        return stmt_.native_statement_handle();
    }

    /* get the size of rowset (diffs from rows()) */
    long rowset_size() const
    {
        return rowset_length;
    }

    /* return the remain rows in the buffer */
    long rows() const MYODBC_NOEXCEPT
    {
        return static_cast<long>(row_number);
    }

    /* get column numbers */
    short columns()
    {
        return stmt_.columns();
    }

    /* forward to next item, return false means no more data */
    bool next()
    {
        if (rows() && ++rowset_pos < rowset_length) {
            return rowset_pos < rows();
        }
        rowset_pos = 0;
        return fetch(0, SQL_FETCH_NEXT);
    }

    /* check if the column is NULL */
    bool is_null(short column) const
    {
        if (column >= bound_columns_length) {
            index_range_error();
        }

        bound_column& col = bound_columns_[column];
        if (rowset_pos >= rows()) {
            index_range_error();
        }

        return col.cbdata_[rowset_pos] == SQL_NULL_DATA;
    }

    /* get column's C_TYPE */
    int column_c_datatype(short column) const
    {
        if (column >= bound_columns_length) {
            index_range_error();
        }
        bound_column& col = bound_columns_[column];
        return col.ctype_;
    }

    /* implementation function of the getting data */
    template <class T>
    void get_ref(short column, T& result)
    {
        if (column >= bound_columns_length) {
            index_range_error();
        }

        if (is_null(column)) {
            null_access_error(column);
        }

        get_ref_impl<T>(column, result);
    }

    /* interface function to get data */
    template <class T>
    T get(short column)
    {
        T result;
        get_ref(column, result);
        return result;
    }

private:
    /* get blob data as a string type */
    void get_blob_data(short column, string_type& result);

    /* get data according given type */
    template <class T>
    void get_ref_impl(short column, T& result);

    /* release buffer everytime */
    void before_move() MYODBC_NOEXCEPT
    {
        for (short i = 0; i < bound_columns_length; i++) {
            bound_column& colobj = bound_columns_[i];
            for (long j = 0; j < rowset_length; j++)
                colobj.cbdata_[j] = 0;
            if (colobj.blob_ && colobj.pdata_) {
                release_bound_resources(i);
            }
        }
    }

    /* release the resources: pdata_ (store data) */
    void release_bound_resources(short column) MYODBC_NOEXCEPT
    {
        MYODBC_ASSERT(column < bound_columns_length);
        bound_column& col = bound_columns_[column];
        delete[] col.pdata_;
        col.pdata_ = 0;
        col.clen_ = 0;
    }

    /* clean up the column buffer before next binding */
    void cleanup_bound_columns() MYODBC_NOEXCEPT
    {
        before_move();
        delete[] bound_columns_;
        bound_columns_ = NULL;
        bound_columns_length = 0;
        bound_columns_by_name.clear();
    }

    /* fetch a batch of the data which the row number is provided by param "rows" */
    bool fetch(long rows, SQLUSMALLINT orientation)
    {
        before_move();
        RETCODE rc;
        MYODBC_CALL_RC(*pSQLFetch, rc, stmt_.native_statement_handle());
        if (rc == SQL_NO_DATA) {
            at_end = true;
            return false;
        }
        if (!success(rc)) {
            MYODBC_THROW_DATABASE_ERROR(stmt_.native_statement_handle(), SQL_HANDLE_STMT);
        }
        return true;
    }

    /*
     * find the SQL data type of the column in result set, and provide the according
     * SQL C data type.
     * the others column attrs are alse set here, such as null flag, column name,
     * the number of the column sequence ...
     */
    void auto_bind()
    {
        cleanup_bound_columns();

        const short n_columns = columns();
        if (n_columns < 1) {
            return;
        }

        MYODBC_ASSERT(!bound_columns_);
        MYODBC_ASSERT(!bound_columns_length);
        bound_columns_ = new bound_column[n_columns];
        bound_columns_length = n_columns;

        RETCODE rc;
        MYODBC_SQLCHAR column_name[1024];
        SQLSMALLINT sqltype, scale, nullable, len;
        SQLULEN sqlsize;

        for (SQLSMALLINT i = 0; i < n_columns; i++) {
            /* get base attr from SQLDescribeCol */
            MYODBC_CALL_RC(MYODBC_FUNC(*pSQLDescribeCol),
                rc,
                stmt_.native_statement_handle(),
                i + 1,
                (MYODBC_SQLCHAR*)column_name,
                sizeof(column_name) / sizeof(MYODBC_SQLCHAR),
                &len,
                &sqltype,
                &sqlsize,
                &scale,
                &nullable);
            if (!success(rc)) {
                MYODBC_THROW_DATABASE_ERROR(stmt_.native_statement_handle(), SQL_HANDLE_STMT);
            }

            bool is_blob = false;

            /* special process with SQL_WVARCHAR */
            if (sqlsize == 0) {
                switch (sqltype) {
                    case SQL_WVARCHAR: {
                        /*
                         * Divide in half, due to sqlsize being 32-bit in Win32 (and 64-bit in x64)
                         * sqlsize = std::numeric_limits<int32_t>::max() / 2 - 1;
                         */
                        is_blob = true;
                    }
                    default:
                        break;
                }
            }

            /* make column object and set the column attr from SQLDescribeCol */
            bound_column& colobj = bound_columns_[i];
            colobj.name_ = reinterpret_cast<string_type::value_type *>(column_name);
            colobj.column_ = i;
            colobj.sqltype_ = sqltype;
            colobj.sqlsize_ = sqlsize;
            colobj.scale_ = scale;
            bound_columns_by_name[colobj.name_] = &colobj;
            colobj.blob_ = false; /* default */

            /* set SQL C data type with SQL data type got by SQLDescribeCol */
            using namespace std;
            switch (colobj.sqltype_) {
                case SQL_TINYINT: {
                    colobj.ctype_ = SQL_C_CHAR;
                    colobj.clen_ = SQL_TINYINT_LEN;
                    break;
                }
                case SQL_SMALLINT: {
                    colobj.ctype_ = SQL_C_CHAR;
                    colobj.clen_ = SQL_SMALLINT_LEN;
                    break;
                }
                case SQL_INTEGER:
                case SQL_BIGINT:
                case SQL_DOUBLE:
                case SQL_FLOAT:
                case SQL_REAL: {
                    colobj.ctype_ = SQL_C_CHAR;
                    colobj.clen_ = SQL_BIGINT_LEN;
                    break;
                }
                case SQL_DECIMAL:
                case SQL_NUMERIC: {
                    colobj.ctype_ = SQL_C_CHAR;
                    colobj.clen_ = colobj.sqlsize_ + 1;
                    break;
                }
                case SQL_DATE:
                case SQL_TYPE_DATE: {
                    colobj.ctype_ = SQL_C_DATE;
                    colobj.clen_ = sizeof(date);
                    break;
                }
                case SQL_TIMESTAMP:
                case SQL_TYPE_TIMESTAMP: {
                    colobj.ctype_ = SQL_C_TIMESTAMP;
                    colobj.clen_ = sizeof(timestamp);
                    break;
                }
                case SQL_CHAR:
                case SQL_VARCHAR: {
                    colobj.ctype_ = SQL_C_CHAR;
                    /*
                     * For type: interval second(p): saved as 00:00:12.999999 in MPPDB.
                     * unixODBC give shorter estimate sqlsize=12, so we can only get 00:00:12.999 (sth be lost)
                     * For this reasion, we enlarge the sqlsize here(will be used to bind column below) as:
                     * col.sqlsize_ + 1 (for '\0') + 3  (enlarged)
                     */
                    colobj.clen_ = (colobj.sqlsize_ + INTERVAL_SECOND_LEN) * sizeof(SQLCHAR);
                    break;
                }
                case SQL_WCHAR:
                case SQL_WVARCHAR: {
                    colobj.ctype_ = SQL_C_WCHAR;
                    colobj.clen_ = (colobj.sqlsize_ + 1) * sizeof(SQLWCHAR);
                    if (is_blob) {
                        colobj.clen_ = 0;
                        colobj.blob_ = true;
                    }
                    break;
                }
                case SQL_LONGVARCHAR: {
                    colobj.ctype_ = SQL_C_CHAR;
                    colobj.blob_ = true;
                    colobj.clen_ = 0;
                    break;
                }

                default: {
                    throw std::runtime_error("SQL_ERROR: unsupport data type return by ODBC");
                }
            }
        }

        /* bind the SQL C data type to the according column */
        for (SQLSMALLINT i = 0; i < n_columns; ++i) {
            bound_column& colobj = bound_columns_[i];
            colobj.cbdata_ = new null_type[rowset_length];
            if (colobj.blob_) {
                /* bind blob data */
                MYODBC_CALL_RC(
                    *pSQLBindCol, rc, stmt_.native_statement_handle(), i + 1, colobj.ctype_, 0, 0, colobj.cbdata_);
                if (!success(rc)) {
                    MYODBC_THROW_DATABASE_ERROR(stmt_.native_statement_handle(), SQL_HANDLE_STMT);
                }
            } else {
                /* bind normal data */
                colobj.pdata_ = new char[rowset_length * colobj.clen_];
                MYODBC_CALL_RC(*pSQLBindCol,
                    rc,
                    stmt_.native_statement_handle(),
                    i + 1,
                    colobj.ctype_,
                    colobj.pdata_,
                    colobj.clen_,
                    colobj.cbdata_);
                if (!success(rc)) {
                    MYODBC_THROW_DATABASE_ERROR(stmt_.native_statement_handle(), SQL_HANDLE_STMT);
                }
            }
        }
    }

private:
    statement stmt_;
    const long rowset_length;
    SQLULEN row_number;
    bound_column* bound_columns_;
    short bound_columns_length;
    long rowset_pos;
    std::map<string_type, bound_column *> bound_columns_by_name;
    bool at_end;
    char blob_buffer[1024];
};

/*
 * @Description:
 * 	get blob data from server
 *
 * @IN column: column number
 * @IN result: where blob data stored
 * @RETURN: void
 */
void result::result_impl::get_blob_data(short column, string_type& result)
{
    bound_column& col = bound_columns_[column];
    std::string out;
    SQLLEN strlen_or_ind;
    SQLRETURN rc;
    void* handle = native_statement_handle();
    SQLLEN bufferSize = sizeof(blob_buffer);
    SQLLEN ntotal = 0;
    SQLUSMALLINT icol = column + 1;

    /* Call SQLGetData to determine the amount of data that's waiting. */
    MYODBC_CALL_RC(*pSQLGetData, rc, handle, icol, col.ctype_, blob_buffer, 0, &ntotal);
    if (rc == SQL_SUCCESS_WITH_INFO) {
        /* Read data one by one batch */
        while (ntotal > 0) {
            errno_t errorno = memset_s(blob_buffer, bufferSize, 0, bufferSize);
            securec_check(errorno, "\0", "\0");
            MYODBC_CALL_RC(*pSQLGetData, rc, handle, icol, col.ctype_, blob_buffer, bufferSize, &strlen_or_ind);
            /*
             * SQLGetData returns
             *
             * 		SQL_SUCCESS, SQL_SUCCESS_WITH_INFO, SQL_NO_DATA: --success
             * 		SQL_STILL_EXECUTING,
             * 		SQL_ERROR, SQL_INVALID_HANDLE: --fail
             */
            if (rc == SQL_SUCCESS_WITH_INFO || rc == SQL_SUCCESS) {
                /* append data into result */
                if (ntotal > (bufferSize - 1)) {
                    /* the last character of the array is invalid */
                    out.append(blob_buffer, bufferSize - 1);
                } else {
                    out.append(blob_buffer, ntotal);
                }
                ntotal = ntotal - (bufferSize - 1);
            } else if (rc == SQL_NO_DATA) {
                /* at end of the result */
                break;
            } else {
                /* unexpected error */
                throw std::runtime_error("SQL_ERROR: unexpected error in getting blob data from server!");
            }
        }
    }

    /* Convert result */
    convert(out, result);
}

/*
 * @Description:
 * 	get data as a string from the result buffer
 *
 * @IN column: column number
 * @IN result: where data stored
 * @RETURN: void
 */
template <>
inline void result::result_impl::get_ref_impl<string_type>(short column, string_type& result)
{
    bound_column& col = bound_columns_[column];

    /*
     * Get string data by C_TYPE
     */
    switch (col.ctype_) {
        /* numeric, string, default type, etc. */
        case SQL_C_CHAR: {
            if (col.blob_) {
                /* it is blob data */
                get_blob_data(column, result);
            } else {
                /* just assign the result by the buffer data */
                const char* s = col.pdata_ + rowset_pos * col.clen_;
                const std::string::size_type str_size = std::strlen(s);
                result.assign(s, s + str_size);
            }
            break;
        }

        /* date */
        case SQL_C_DATE: {
            char date_str[64] = {0};
            errno_t ret;
            date d = *reinterpret_cast<date *>(col.pdata_ + rowset_pos * col.clen_);
            ret = sprintf_s(date_str, sizeof(date_str), "%d-%d-%d", d.year, d.month, d.day);
            securec_check_ss(ret, "\0", "\0");
            convert(date_str, result);
            break;
        }

        /* timestamp */
        case SQL_C_TIMESTAMP: {
            char date_str[64] = {0};
            errno_t ret;
            timestamp stamp = *reinterpret_cast<timestamp *>(col.pdata_ + rowset_pos * col.clen_);
            /* %09d adapt to A db timestamp */
            ret = sprintf_s(date_str,
                sizeof(date_str),
                "%d-%d-%d %d:%d:%d.%09d",
                stamp.year,
                stamp.month,
                stamp.day,
                stamp.hour,
                stamp.min,
                stamp.sec,
                stamp.fract);
            securec_check_ss(ret, "\0", "\0");
            convert(date_str, result);
            break;
        }

        /* unexpected type */
        default:
            type_incompatible_error(col.ctype_, col.column_);
    }
}

/*
 * @Description:
 * 	get data as a TEMPLATE_TYPE from the result buffer
 *
 * @IN column: column number
 * @IN result: where data stored
 * @RETURN: void
 */
template <>
inline void result::result_impl::get_ref_impl<bool>(short column, bool& result)
{
    bound_column& col = bound_columns_[column];
    using namespace std;
    const char* s = NULL;

    /*
     * Check if convert blob type(e.g. text) into bool, if so just error
     */
    if (col.blob_ || col.pdata_ == NULL) {
        type_incompatible_error(col.ctype_, col.column_);
    } else {
        s = col.pdata_ + rowset_pos * col.clen_;
    }

    if (s == NULL) {
        ereport(ERROR,
            (errmodule(MOD_EC),
                errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("SQL_ERROR: unexpected data while converting it into bool.")));
    }

    /*
     * Convert buffer data according to C_TYPE
     */
    switch (col.ctype_) {
        case SQL_C_CHAR:
            /* here, we apply standard bool value in MPPDB */
            if (*s == '0' || *s == 'f' || *s == 'F' || *s == 'n' || *s == 'N' || strcasecmp(s, "no") == 0 ||
                strcasecmp(s, "false") == 0) {
                result = false;
            } else if (*s == '1' || *s == 't' || *s == 'T' || *s == 'y' || *s == 'Y' || strcasecmp(s, "yes") == 0 ||
                     strcasecmp(s, "true") == 0) {
                result = true;
            } else {
                /* otherwise, we don't know the value */
                throw std::runtime_error("SQL_ERROR: unexpected data while converting it into bool.");
            }
            break;

        /* unsupported C_TYPE */
        default:
            type_incompatible_error(col.ctype_, col.column_);
    }
}

}  // namespace myodbc

namespace myodbc {
/*
 * Entrance of Execution
 */
result execute(connection& conn, const string_type& query, long batch_operations, long timeout)
{
    class statement statement;
    return statement.execute_direct(conn, query, batch_operations, timeout);
}
}  // namespace myodbc

namespace myodbc {
/*
 * Interface function of Connection Class: connection
 */
connection::connection() : impl_(new connection_impl())
{}

connection& connection::operator=(connection rhs)
{
    swap(rhs);
    return *this;
}

void connection::swap(connection& rhs_) MYODBC_NOEXCEPT
{
    using std::swap;
    swap(impl_, rhs_.impl_);
}

connection::connection(const string_type& dsn, const string_type& user, const string_type& pass, long timeout)
    : impl_(new connection_impl(dsn, user, pass, timeout))
{}

connection::~connection() MYODBC_NOEXCEPT
{}

void connection::connect(const string_type& dsn, const string_type& user, const string_type& pass, long timeout)
{
    impl_->connect(dsn, user, pass, timeout);
}

bool connection::connected() const
{
    return impl_->connected();
}

void connection::disconnect()
{
    impl_->disconnect();
}

void* connection::native_dbc_handle() const
{
    return impl_->native_dbc_handle();
}

void* connection::native_env_handle() const
{
    return impl_->native_env_handle();
}

}  // namespace myodbc

namespace myodbc {
/*
 * Interface function of Statement Class: statement
 */
statement::statement() : impl_(new statement_impl())
{}

statement::statement(statement&& rhs) MYODBC_NOEXCEPT : impl_(std::move(rhs.impl_))
{}

statement::statement(const statement& rhs) : impl_(rhs.impl_)
{}

statement& statement::operator=(statement rhs)
{
    swap(rhs);
    return *this;
}

void statement::swap(statement& rhs) MYODBC_NOEXCEPT
{
    using std::swap;
    swap(impl_, rhs.impl_);
}

statement::~statement() MYODBC_NOEXCEPT
{}

void statement::open(class connection& conn)
{
    impl_->open(conn);
}

bool statement::open() const
{
    return impl_->open();
}

bool statement::connected() const
{
    return impl_->connected();
}

void statement::close()
{
    impl_->close();
}

void* statement::native_statement_handle() const
{
    return impl_->native_statement_handle();
}

result statement::execute_direct(class connection& conn, const string_type& query, long batch_operations, long timeout)
{
    return impl_->execute_direct(conn, query, batch_operations, timeout, *this);
}

short statement::columns()
{
    return impl_->columns();
}

}  // namespace myodbc

namespace myodbc {
/*
 * Interface function of Result Class: result
 */
result::result() : impl_()
{}

result::~result() MYODBC_NOEXCEPT
{}

result::result(statement stmt, long rowset_size) : impl_(new result_impl(stmt, rowset_size))
{}

result::result(result&& rhs) MYODBC_NOEXCEPT : impl_(std::move(rhs.impl_))
{}

result::result(const result& rhs) : impl_(rhs.impl_)
{}

result& result::operator=(result rhs)
{
    swap(rhs);
    return *this;
}

void result::swap(result& rhs) MYODBC_NOEXCEPT
{
    using std::swap;
    swap(impl_, rhs.impl_);
}

short result::columns()
{
    return impl_->columns();
}

bool result::next()
{
    return impl_->next();
}

bool result::is_null(short column) const
{
    return impl_->is_null(column);
}

int result::column_c_datatype(short column) const
{
    return impl_->column_c_datatype(column);
}

template <class T>
T result::get(short column)
{
    return impl_->get<T>(column);
}
}  // namespace myodbc

#undef MYODBC_THROW_DATABASE_ERROR
#undef MYODBC_STRINGIZE
#undef MYODBC_STRINGIZE_I
#undef MYODBC_CALL_RC
#undef MYODBC_CALL

/*
 * @Description: connect to the server by DSN
 *
 * @IN  value: conn_str, the DSN sent to the unixODBC.
 * @IN  return: connection handle
 */
CONN_HANDLE odbc_lib_connect(const char* dsn, const char* user, const char* pass)
{
    myodbc::string_type dsn_str = (dsn == NULL ? "" : dsn);
    myodbc::string_type user_str = (user == NULL ? "" : user);
    myodbc::string_type pass_str = (pass == NULL ? "" : pass);

    return new myodbc::connection(dsn_str, user_str, pass_str);
}

/*
 * @Description: clean up the resources such as connection and result objects.
 *
 * @IN  value: conn, the handle of the connection object.
 * @IN  value: result, the handle of the result object.
 */
void odbc_lib_cleanup(CONN_HANDLE conn, RESULT_HANDLE result)
{
    myodbc::result* _result = (myodbc::result*)result;
    if (NULL != _result) {
        delete _result;
    }

    myodbc::connection* _conn = (myodbc::connection*)conn;
    if (_conn != NULL) {
        if (_conn->connected()) {
            _conn->disconnect();
        }

        delete _conn;
    }
}

/*
 * @Description: send SQL to the remote service.
 *
 * @IN  value: conn, the handle of the connection object.
 * @IN  value: query, the SQL sent to the server
 */
RESULT_HANDLE odbc_lib_query(CONN_HANDLE conn, const char* query)
{
    myodbc::connection* _conn = (myodbc::connection*)conn;
    myodbc::string_type query_str = (query == NULL ? "" : query);
    myodbc::result* _result = new myodbc::result();

    *_result = execute(*_conn, query_str);

    return _result;
}

/*
 * @Description: get one row of the result set from odbc driver.
 *
 * @IN  value: result, the handle of the result object.
 * @IN  value: types,  the type oid array of the column
 * @IN  value: _buf,   the buffer array of the result
 * @IN  value: nulls,  the flags array of the null
 * @IN  value: cols,   the number of the target column
 * @IN  value: isEnd, true if no more data returned.
 */
void odbc_lib_get_result(RESULT_HANDLE result, unsigned int* types, void** _buf, bool* nulls, int cols, bool& isend)
{
    myodbc::result* _result = (myodbc::result*)result;
    int remote_ncols = (int)_result->columns();
    /* columns == 0 means that query maybe a DDL, such as create, drop, ... */
    if (0 == remote_ncols) {
        isend = true;
        return;
    }

    /* check the number of column in table definition */
    if (cols > remote_ncols) {
        throw std::runtime_error("SQL_ERROR: The number of the column in relation"
                                 " definition is greater than the number of the column in result!");
    }

    /* check whether there is more data */
    isend = !_result->next();
    if (isend) {
        return;
    }

#define GetStringToBuffer                                                             \
    do {                                                                              \
        errno_t ret;                                                                  \
        std::string tmp = _result->get<string>(col);                                  \
        buf[col] = (int64_t)palloc0(tmp.size() + 1);                                  \
        ret = memcpy_s((void*)buf[col], tmp.size() + 1, tmp.c_str(), tmp.size() + 1); \
        securec_check(ret, "\0", "\0");                                               \
    } while (0)

    /* get data from result object. */
    int64_t* buf = (int64_t*)_buf;
    for (int col = 0; col < cols; ++col) {
        if (_result->is_null(col)) {
            nulls[col] = true;
            continue;
        }

        int sql_c_type = _result->column_c_datatype(col);

        switch (types[col]) {
            /*
             * Boolean: read bool and then store as one byte.
             */
            case BOOLOID: {
                if (sql_c_type == SQL_C_CHAR) {
                    bool x = false;

                    x = _result->get<bool>(col);
                    buf[col] = (int64_t)palloc0(2);
                    *(char*)buf[col] = (x == true) ? '1' : '0';
                    *((char*)buf[col] + 1) = '\0';
                } else
                    throw std::runtime_error("SQL_ERROR: cast unexpected type to bool.");
                break;
            }

            /*
             * Numeric: up to now,  number is stored as a string
             */
            case INT1OID:
                if (sql_c_type == SQL_C_CHAR) {
                    GetStringToBuffer;
                } else {
                    throw std::runtime_error("SQL_ERROR: cast unexpected type to tinyint.");
                }
                break;

            case INT2OID:
                if (sql_c_type == SQL_C_CHAR) {
                    GetStringToBuffer;
                } else {
                    throw std::runtime_error("SQL_ERROR: cast unexpected type to smallint.");
                }
                break;

            case INT4OID:
                if (sql_c_type == SQL_C_CHAR) {
                    GetStringToBuffer;
                } else {
                    throw std::runtime_error("SQL_ERROR: cast unexpected type to int.");
                }
                break;

            case INT8OID:
                if (sql_c_type == SQL_C_CHAR) {
                    GetStringToBuffer;
                } else {
                    throw std::runtime_error("SQL_ERROR: cast unexpected type to bigint.");
                }
                break;

            case FLOAT4OID:
                if (sql_c_type == SQL_C_CHAR) {
                    GetStringToBuffer;
                } else {
                    throw std::runtime_error("SQL_ERROR: cast unexpected type to float.");
                }
                break;

            case FLOAT8OID:
                if (sql_c_type == SQL_C_CHAR) {
                    GetStringToBuffer;
                } else {
                    throw std::runtime_error("SQL_ERROR: cast unexpected type to double.");
                }
                break;

            case NUMERICOID:
                if (sql_c_type == SQL_C_CHAR) {
                    GetStringToBuffer;
                } else {
                    throw std::runtime_error("SQL_ERROR: cast unexpected type to numeric.");
                }
                break;

            /*
             * Characters/String
             */
            case CHAROID:
            case BPCHAROID:
                if (sql_c_type == SQL_C_CHAR) {
                    GetStringToBuffer;
                } else {
                    throw std::runtime_error("SQL_ERROR: cast unexpected type to char.");
                }
                break;

            case VARCHAROID:
                if (sql_c_type == SQL_C_CHAR) {
                    GetStringToBuffer;
                } else {
                    throw std::runtime_error("SQL_ERROR: cast unexpected type to varchar.");
                }
                break;

            case NVARCHAR2OID:
                if (sql_c_type == SQL_C_CHAR) {
                    GetStringToBuffer;
                } else {
                    throw std::runtime_error("SQL_ERROR: cast unexpected type to nvarchar2.");
                }
                break;

            /*
             * Date/Timestamp
             */
            case TIMESTAMPOID:
            case DATEOID:
            case TIMESTAMPTZOID: {
                if (sql_c_type == SQL_C_TIMESTAMP || sql_c_type == SQL_C_DATE) {
                    /*
                     * Store the date or timestamp as  a string
                     */
                    GetStringToBuffer;
                } else {
                    throw std::runtime_error("SQL_ERROR: cast unexpected type to date/timestamp.");
                }
                break;
            }

            /*
             * Interval
             */
            case INTERVALOID: {
                if (sql_c_type == SQL_C_CHAR) {
                    /*
                     * Store the interval as a string
                     */
                    GetStringToBuffer;
                } else {
                    throw std::runtime_error("SQL_ERROR: cast unexpected type to interval.");
                }
                break;
            }

            /*
             * Text
             */
            case TEXTOID: {
                /*
                 * Store the result as a string no matter what sql_c_type is
                 */
                GetStringToBuffer;
                break;
            }

            /*
             * Default: should never run here, check_typeoid() has checked.
             */
            default:
                throw std::runtime_error("SQL_ERROR: unsupport data type found in table definition.");
        }
    }
}
