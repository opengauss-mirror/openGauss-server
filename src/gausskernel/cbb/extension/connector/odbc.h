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
 * odbc.h
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/extension/connector/odbc.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef ODBC_H
#define ODBC_H

#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include <cstdint>

namespace myodbc {
typedef long null_type;
typedef std::string string_type;
#define MYODBC_NOEXCEPT noexcept

class database_error : public std::runtime_error {
public:
    database_error(void* handle, short handle_type, const std::string& info = "");
    ~database_error(void);
    const char* what() const MYODBC_NOEXCEPT;
    const long native() const MYODBC_NOEXCEPT;
    const std::string state() const MYODBC_NOEXCEPT;

private:
    long native_error;
    std::string sql_state;
    std::string message;
};

/*
 * Data type used to get time & date type.
 */
struct timestamp {
    std::int16_t year;
    std::int16_t month;
    std::int16_t day;
    std::int16_t hour;
    std::int16_t min;
    std::int16_t sec;
    std::int32_t fract;
};

struct date {
    std::int16_t year;
    std::int16_t month;
    std::int16_t day;
};

/*
 * Class statement:
 * 	represents a SQL sent to ODBC server.
 */
class statement {
public:
    statement(const statement& rhs);
    statement(class connection& conn, const string_type& query, long timeout = 0);
    explicit statement(class connection& conn);
    statement();

#ifndef MYODBC_NO_MOVE_CTOR
    statement(statement&& rhs) MYODBC_NOEXCEPT;
#endif

    ~statement() MYODBC_NOEXCEPT;
    void swap(statement& rhs) MYODBC_NOEXCEPT;
    statement& operator=(statement rhs);

    bool connected() const;
    bool open() const;
    void open(class connection& conn);
    void close();
    short columns();
    void* native_statement_handle() const;

    class result execute_direct(
        class connection& conn, const string_type& query, long batch_operations = 1, long timeout = 0);

private:
    class statement_impl;
    friend class myodbc::result;
    std::shared_ptr<statement_impl> impl_;
};

/*
 * Class connection:
 * 	represents a live connection to ODBC service, and can be reused later.
 */
class connection {
public:
    connection();
    connection(const string_type& dsn, const string_type& user, const string_type& pass, long timeout = 0);
    connection& operator=(connection rhs);
    void swap(connection&) MYODBC_NOEXCEPT;
    ~connection() MYODBC_NOEXCEPT;

    void connect(const string_type& dsn, const string_type& user, const string_type& pass, long timeout = 0);
    void disconnect();
    bool connected() const;

    void* native_dbc_handle() const;
    void* native_env_handle() const;

private:
    class connection_impl;
    std::shared_ptr<connection_impl> impl_;
};

/*
 * Class result:
 * 	result set description, including the attributes and storage
 */
class result {
public:
    result();
    result(const result& rhs);
    result(result&& rhs) MYODBC_NOEXCEPT;

    ~result() MYODBC_NOEXCEPT;

    void* native_statement_handle() const;
    void swap(result& rhs) MYODBC_NOEXCEPT;
    result& operator=(result rhs);
    short columns();
    bool next();
    bool is_null(short column) const;
    int column_c_datatype(short column) const;

    template <class T>
    T get(short column);

private:
    result(statement statement, long rowset_size);

    class result_impl;
    friend class myodbc::statement::statement_impl;
    std::shared_ptr<result_impl> impl_;
};

/* Execute function */
result execute(connection& conn, const string_type& query, long batch_operations = 1, long timeout = 0);

} /* end of namespace myodbc */

#endif /* ODBC_H */
