/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * driver_error.h
 *
 * IDENTIFICATION
 * src/common/interfaces/libpq/jdbc/client_logic_jni/driver_error.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef DRIVER_ERROR_H_
#define DRIVER_ERROR_H_

#include <string.h>

static constexpr const int MAX_ERROR_MESSAGE_LENGTH = 4096;
static constexpr const int MAX_INTEGER_DIGITS = 11;

enum JNI_SYSTEM_ERROR_CODES {
    CANNOT_LINK,
    NO_ACTIVE_DB_CONNECTION,
    DB_CONNECTION_MISS_MATCH,
    INVALID_HANDLE,
    INSTANCE_CREATION_FAILED,
    JAVA_STRING_CREATION_FAILED,
    PRE_QUERY_FAILED,
    STMNT_PRE_QUERY_FAILED,
    STRING_CREATION_FAILED,
    CLIENT_LOGIC_FAILED,
    CLIENT_LOGIC_RETURNED_NULL,
    POST_QUERY_NOT_REQUIRED,
    PRE_QUERY_EXEC_FAILED,
    STATEMENT_DATA_EMPTY,
    UNEXPECTED_ERROR,
    CHANGE_SEARCH_PATH_FAILED,
    INVALID_INPUT_PARAMETER
};

static constexpr const char JNI_SYSTEM_ERROR_TEXT[][MAX_ERROR_MESSAGE_LENGTH] = {
    "Failed linking client encryption",
    "Database Connection is not active",
    "Database connection parameter mismatch",
    "Invalid handle, cannot create instance",
    "Instance creation failed",
    "String creation failed from Java",
    "Pre query failed"
    "Statement Pre query failed"
    "String creation failed",
    "client encryption failed",
    "client encryption returned null",
    "Post Query not required at this point.",
    "Pre query exec failed",
    "Statement data is empty",
    "Unexpected Error",
    "Failed changing back end search path",
    "Invalid input parameter"
};

static constexpr const int JNI_SYSTEM_ERROR_SIZE = (int)sizeof(JNI_SYSTEM_ERROR_TEXT) / MAX_ERROR_MESSAGE_LENGTH;

/*
 * @class DriverError
 * @brief capture errors text & numbers between the different methods and layer
 */
class DriverError {
public:
    explicit DriverError(const int error_code, const char *error_message);
    ~DriverError() {}
    int get_error_code() const;
    const char *get_error_message() const;
    void set_no_error();
    void set_error(const int error_code, const char *error_message);
    void set_error(const int error_code);
    const char *to_string();

private:
    void copy_error_text(const char *error_message);
    int m_error_code = 0;
    char m_error_message[MAX_ERROR_MESSAGE_LENGTH] = "";
    /* m_error_message_and_code length is MAX_ERROR_MESSAGE_LENGTH + MAX_INTEGER_DIGITS
     * for adding the error code that is an integer */
    char m_error_message_and_code[MAX_ERROR_MESSAGE_LENGTH + MAX_INTEGER_DIGITS] = "";
};

#endif /* DRIVER_ERROR_H_ */
