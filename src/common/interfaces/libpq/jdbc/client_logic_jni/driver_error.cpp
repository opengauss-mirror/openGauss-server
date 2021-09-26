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
 * driver_error.cpp
 *
 * IDENTIFICATION
 * src/common/interfaces/libpq/jdbc/client_logic_jni/driver_error.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "driver_error.h"
#include <securec.h>
#include "jni_logger.h"

/*
 * @return the error code number
 */
int DriverError::get_error_code() const
{
    return m_error_code;
}

/*
 *
 * @return pointer to the error text
 */
const char *DriverError::get_error_message() const
{
    return m_error_message;
}
/*
 * Constructor
 * @param[in] error_code
 * @param[out] error_message
 */
DriverError::DriverError(const int error_code, const char *error_message) : m_error_code(error_code)
{
    if (error_message == NULL) {
        return;
    }
    copy_error_text(error_message);
}
/*
 * clears any error text and code
 */
void DriverError::set_no_error()
{
    m_error_code = 0;
    copy_error_text("");
}
/*
 * sets the error code & text
 * @param error_code[in]
 * @param error_message[in]
 */
void DriverError::set_error(const int error_code, const char *error_message)
{
    if (error_message == NULL) {
        return;
    }
    m_error_code = error_code;
    copy_error_text(error_message);
}
/*
 * sets the error using code only and list of predefined error codes
 * @param[in] error_code
 */
void DriverError::set_error(const int error_code)
{
    const char *error_message = "Unknown error.";
    if (error_code > 0 && error_code < JNI_SYSTEM_ERROR_SIZE) {
        error_message = JNI_SYSTEM_ERROR_TEXT[error_code - 1];
    }
    set_error(error_code, error_message);
}

/*
 * Copy the error text to m_error_message_and_code
 * @param[in] error_message
 */
void DriverError::copy_error_text(const char *error_message)
{
    if (error_message == NULL) {
        return;
    }
    size_t source_len = strlen(error_message);
    if (source_len >= sizeof(m_error_message)) {
        source_len = sizeof(m_error_message) - 1;
    }
    errno_t rc = strncpy_s(m_error_message, sizeof(m_error_message), error_message, source_len);
    if (rc != EOK) {
        JNI_LOG_ERROR("Unable to copy error text : %s", error_message);
        return;
    }
    int len_used = snprintf_s(m_error_message_and_code, sizeof(m_error_message_and_code),
        sizeof(m_error_message_and_code) - 1, "%d:%s", m_error_code, m_error_message);
    if (len_used < 0) {
        JNI_LOG_ERROR("Unable to copy error code and text : %s", error_message);
        return;
    }
}
