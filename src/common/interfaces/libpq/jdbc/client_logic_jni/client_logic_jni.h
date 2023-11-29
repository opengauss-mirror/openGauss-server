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
 * client_logic_jni.h
 *
 * IDENTIFICATION
 * src/common/interfaces/libpq/jdbc/client_logic_jni/client_logic_jni.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef CLIENT_LOGIC_JNI_H_
#define CLIENT_LOGIC_JNI_H_

#include "client_logic_common/statement_data.h"
#include "driver_error.h"
#include "jni_string_convertor.h"

typedef struct pg_conn PGconn; /* forward declaration */

constexpr size_t MAXIMUM_SQL_SE = 63 * 5 + 1; /* up to 5 names and 1 extra for null termination */
/*
 * The ClientLogicJNI class captures the functionality of client logic for the JDBC driver
 * It holds a PGconn instance that is equivalent to the JDBC connection and perform client logic operations on it
 */
class ClientLogicJNI {
public:
    /* Delete constructors that are not supported */
    ClientLogicJNI(const ClientLogicJNI &) = delete;
    ClientLogicJNI(ClientLogicJNI &&) = delete;
    ClientLogicJNI &operator = (const ClientLogicJNI &) = delete;
    ClientLogicJNI &operator = (ClientLogicJNI &&) = delete;

    ClientLogicJNI();
    virtual ~ClientLogicJNI();
    void clean_stmnt();
    void clean_con();
    bool link_client_logic(JNIEnv *env, jobject jdbc_cl_impl, const char *database_name, DriverError *status);
    bool set_kms_info(const char *key, const char *value);
    static bool from_handle(long handle, ClientLogicJNI **handle_ptr, DriverError *status);
    bool run_pre_query(const char *original_query, DriverError *status);
    bool run_post_query(const char *statement_name, DriverError *status);
    bool preare_statement(const char *query, const char *statement_name, size_t parameter_count, DriverError *status);
    bool replace_statement_params(const char *statement_name, const char * const param_values[], size_t parameter_count,
        DriverError *status);
    const char *get_pre_query_result() const;
    const StatementData *get_statement_data() const;
    bool deprocess_value(const char *data_2_process, int data_type, unsigned char **proccessed_data,
        size_t &length_output, DriverError *status) const;
    size_t get_record_data_oid_length(int oid, const char* column_name);
    const int *get_record_data_oids(int oid, const char* column_name);
    bool process_record_data(const char *data_2_process, const int *original_oids,  size_t original_oids_length,
        unsigned char **proccessed_data, bool *is_encreypted, size_t &length_output, DriverError *status);
    bool replace_message(const char *original_message, char **new_message, DriverError *status) const;
    const char *get_new_query(const char *query);
    void set_jni_env_and_cl_impl(JNIEnv *env, jobject jni_cl_impl);
    void reload_cache_if_needed() const;
    void reload_cache() const;

private:
    PGconn *m_stub_conn = NULL;
    StatementData *m_stmnt = NULL;
    bool m_post_query_needed = false; /* Flag that check if RunPreQuery ran and */
    /* Place holder for the current query, needed for replace_message_impl
     * Since client logic keep pointers to the query to get the user input values oin error messages.
     */
    char *current_query = NULL;
    void clear_current_query();
    static bool replace_message_impl(PGconn *conn, const char *original_message, char *new_message, size_t buffer_size,
        DriverError *status);
};

#endif /* CLIENT_LOGIC_JNI_H_ */
