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
 * client_logic_jni.cpp
 *
 * IDENTIFICATION
 * src/common/interfaces/libpq/jdbc/client_logic_jni/client_logic_jni.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "client_logic_jni.h"
#include "libpq/libpq-fe.h"
#include "postgres_fe.h"
#include "client_logic_common/client_logic_utils.h"
#include "client_logic_common/statement_data.h"
#include "client_logic_processor/create_stmt_processor.h"
#include "client_logic_processor/stmt_processor.h"
#include "client_logic_processor/values_processor.h"
#include "client_logic_processor/record_processor.h"
#include "client_logic_processor/raw_values_cont.h"
#include "client_logic_data_fetcher/data_fetcher_manager.h"
#include "jni_util.h"
#include "jni_logger.h"

void clean_empty_conn_4cl(PGconn *conn);

/* *
 * creates a dummy PgConn object just to hold the client logic class instance with the minimum dependencies
 * @param env
 * @param jdbc_cl_impl
 * @return pointer to the connection created
 */
PGconn *make_empty_conn_4cl(JNIEnv *env, jobject jdbc_cl_impl)
{
    PGconn *conn = static_cast<PGconn *>(malloc(sizeof(PGconn)));
    if (conn == NULL) {
        return conn;
    }
    check_memset_s(memset_s(conn, sizeof(PGconn), 0, sizeof(PGconn)));
    initPQExpBuffer(&conn->errorMessage);
    if (PQExpBufferBroken(&conn->errorMessage)) {
        /* out of memory already :-( */
        clean_empty_conn_4cl(conn);
        conn = NULL;
    }
    conn->client_logic = new (std::nothrow) PGClientLogic(conn, env, jdbc_cl_impl);
    if (conn->client_logic == NULL) {
        clean_empty_conn_4cl(conn);
        conn = NULL;
    }
    return conn;
}

/* *
 * Cleans an empty connection created with make_empty_conn_4cl
 * @param conn connection to clean
 */
void clean_empty_conn_4cl(PGconn *conn)
{
    if (conn == NULL) {
        return;
    }
    libpq_free(conn->dbName);
    if (conn->client_logic != NULL) {
#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS) && (!defined(ENABLE_LITE_MODE))))
        free_kms_cache(conn->client_logic->client_cache_id);
#endif
        delete conn->client_logic;
        conn->client_logic = NULL;
    }
    termPQExpBuffer(&conn->errorMessage);
    libpq_free(conn);
}

ClientLogicJNI::ClientLogicJNI() {}

ClientLogicJNI::~ClientLogicJNI()
{
    clear_current_query();
    clean_stmnt();
    clean_con();
}
/* *
 * Cleans the connection object
 */
void ClientLogicJNI::clean_con()
{
    if (m_stub_conn != NULL) {
        clean_empty_conn_4cl(m_stub_conn);
        m_stub_conn = NULL;
    }
}
/* *
 * Cleans open statement data if any
 */
void ClientLogicJNI::clean_stmnt()
{
    if (m_stmnt != NULL) {
        delete m_stmnt;
        m_stmnt = NULL;
    }
}
/* *
 * Create a dummy PGconn object and initialize its client logic
 * @param jdbc_cl_impl java object to use to call the database with
 * @param env Java environment
 * @param status error information of any
 * @return true on success and false on failure
 */
bool ClientLogicJNI::link_client_logic(JNIEnv *env, jobject jdbc_cl_impl, const char *database_name,
    DriverError *status)
{
    if (status == NULL) {
        return false;
    }
    m_stub_conn = make_empty_conn_4cl(env, jdbc_cl_impl);
    if (m_stub_conn == NULL) {
        status->set_error(JNI_SYSTEM_ERROR_CODES::CANNOT_LINK);
        JNI_LOG_ERROR("Failed creating empty connection");
        return false;
    }

    m_stub_conn->dbName = (database_name != NULL) ? strdup(database_name) : NULL;
    m_stub_conn->client_logic->enable_client_encryption = true;
    m_stub_conn->std_strings = true; // To make unescape bytea work
    // The minimum server version we have this feature in, could not be less than it anyway
    m_stub_conn->sversion = 90204; 
    m_stub_conn->client_logic->m_cached_column_manager->load_cache(m_stub_conn); // Load the cache
    return true;
}

bool ClientLogicJNI::set_kms_info(const char *key, const char *value)
{
    if (m_stub_conn == NULL) {
        return false; /* should never happen */
    }

#if ((defined(ENABLE_MULTIPLE_NODES)) || (defined(ENABLE_PRIVATEGAUSS) && (!defined(ENABLE_LITE_MODE))))
    CmkemErrCode ret = CMKEM_SUCCEED;
    ret = set_kms_cache_auth_info(m_stub_conn->client_logic->client_cache_id, key, value);
    if (ret != CMKEM_SUCCEED) {
        JNI_LOG_ERROR(get_cmkem_errmsg(ret));
        return false;
    }
#endif
    
    return true;
}

/*
 * Retrieves instance of ClientLogicJNI using its pointer
 * @param[in] handle the pointer value
 * @param[out] handle_ptr the reffrence to the instance
 * @param[out] status error information if any
 * @return true on success and false on failures
 */
bool ClientLogicJNI::from_handle(long handle, ClientLogicJNI **handle_ptr, DriverError *status)
{
    if (status == NULL || handle_ptr == NULL) {
        return false;
    }
    if (handle > 0) {
        *handle_ptr = (ClientLogicJNI *)handle;
        if (handle_ptr == NULL) {
            JNI_LOG_ERROR("Cannot create instance from handle %ld", handle);
            status->set_error(JNI_SYSTEM_ERROR_CODES::INSTANCE_CREATION_FAILED);
            return false;
        }
    } else {
        JNI_LOG_ERROR("Invalid handle %ld", handle);
        status->set_error(JNI_SYSTEM_ERROR_CODES::INVALID_HANDLE);
        return false;
    }

    return true;
}
/*
 * Runs the pre-query client logic function
 * @param[in] original_query the original query with potentially client logic values that need to be replaced
 * @param[out] status error information if any
 * @return true on success or false on failure
 */
bool ClientLogicJNI::run_pre_query(const char *original_query, DriverError *status)
{
    if (status == NULL || original_query == NULL) {
        return false;
    }
    clean_stmnt();
    m_stmnt = new (std::nothrow) StatementData(m_stub_conn, original_query);
    if (m_stmnt == NULL) {
        return false;
    }
    bool success = false;

    // Zero the client logic must do it here otherwise it may fail to handle create table statament after create
    // statements
    Processor::run_post_query(m_stub_conn);
    m_post_query_needed = false;

    success = Processor::run_pre_query(m_stmnt);
    // Must set the flag right after the RunPreQuery,
    // ...so even in case of error, post query will happen before next statament
    m_post_query_needed = true;
    if (!success) {
        const char *error_message = PQerrorMessage(m_stub_conn);
        status->set_error(JNI_SYSTEM_ERROR_CODES::PRE_QUERY_FAILED, error_message);
        return false;
    }
    return true;
}

/*
 * Retries a reference to last statement data action, to be called after pre-query
 * @return a reference to last statement data action
 */
const char *ClientLogicJNI::get_pre_query_result() const
{
    if (m_stmnt == NULL) {
        JNI_LOG_ERROR("STATEMENT IS NULL");
        return NULL;
    }
    return m_stmnt->params.adjusted_query;
}
/*
 * unpack client logic values back to user input
 * @param[in] data_2_process the data in client logic binary format (hexa)
 * @param[in] data_type the OID of the original column (modid)
 * @param[out] proccessed_data the unpacked data back in user format
 * @param[out] length_output the length of the unpacked data
 * @param[out] status error information if any
 * @return true on success or false on failure
 */
bool ClientLogicJNI::deprocess_value(const char *data_2_process, int data_type, unsigned char **proccessed_data,
    size_t &length_output, DriverError *status) const
{
    if (status == NULL || data_2_process == NULL || proccessed_data == NULL) {
        return false;
    }

    size_t converted_len = strlen(data_2_process);
    ProcessStatus process_status = ONLY_VALUE;
    DecryptDataRes success = ValuesProcessor::deprocess_value(m_stub_conn, (unsigned char *)data_2_process,
        converted_len, data_type, 0, proccessed_data, length_output, process_status);
    if (success != DEC_DATA_SUCCEED) {
        status->set_error(JNI_SYSTEM_ERROR_CODES::CLIENT_LOGIC_FAILED);
        return false;
    }
    if (*proccessed_data == NULL) {
        status->set_error(JNI_SYSTEM_ERROR_CODES::CLIENT_LOGIC_RETURNED_NULL);
        return false;
    }

    return true;
}

/**
 * get the number of oids in a record , zero means no
 * @param oid
 * @param column_name
 * @return
 */
size_t ClientLogicJNI::get_record_data_oid_length(int oid, const char *column_name)
{
    return m_stub_conn->client_logic->get_rec_origial_ids_length(oid, column_name);
}

/**
 * gets a reference to an array on integers with the ids a record type might have
 * The length of the array is returned by get_record_data_oid_length
 * @param oid
 * @param column_name
 * @return
 */
const int* ClientLogicJNI::get_record_data_oids(int oid, const char *column_name)
{
    return m_stub_conn->client_logic->get_rec_origial_ids(oid, column_name);
}

bool ClientLogicJNI::process_record_data(const char *data_2_process, const int *original_oids,
                                         size_t original_oids_length, unsigned char **proccessed_data,
                                         bool *is_client_logic, size_t &length_output, DriverError *status)
{
    if (status == NULL || data_2_process == NULL || proccessed_data == NULL) {
        return false;
    }
    size_t converted_len = strlen(data_2_process);
    if (!RecordProcessor::DeProcessRecord(m_stub_conn, data_2_process, converted_len, original_oids,
                                          original_oids_length, 0, proccessed_data, length_output, is_client_logic)) {
        status->set_error(JNI_SYSTEM_ERROR_CODES::CLIENT_LOGIC_FAILED);
        return false;
    }
    if (*proccessed_data == NULL) {
        *proccessed_data = (unsigned char *)strdup(data_2_process);
    }
    return true;
}

/*
 * runs the post query client logic function to clean its state machine
 * @param status
 * @param statement_name when issued for prepared statement contains the statement name, otherwise an empty string
 * @param[out] status error information if any
 * @return true on success or false on failure
 */
bool ClientLogicJNI::run_post_query(const char *statement_name,  DriverError *status)
{
    if (status == NULL) {
        return false;
    }
    if (!m_post_query_needed) {
        status->set_error(JNI_SYSTEM_ERROR_CODES::POST_QUERY_NOT_REQUIRED);
        JNI_LOG_ERROR("%s", status->get_error_message() ? status->get_error_message() : "");
        return false;
    }
    m_stub_conn->client_logic->m_lastResultStatus = PGRES_COMMAND_OK;
    if (strlen(statement_name) > 0) {
        check_strncpy_s(strncpy_s(m_stub_conn->client_logic->lastStmtName, NAMEDATALEN, statement_name,
            strlen(statement_name)));
    }

    Processor::run_post_query(m_stub_conn);
    m_post_query_needed = false;
    return true;
}

/*
 * Run / Register the prepare statement to be able to replace any hard coded value in the query itself & to be able
 * later in
 * @param[in] query The statement SQL
 * @param[in] parameter_count Number of parameters found
 * @param[out] status placeholder to return error if any
 * @return true on success and false on failures
 */
bool ClientLogicJNI::preare_statement(const char *query, const char *statement_name, size_t parameter_count,
    DriverError *status)
{
    if (query == NULL || statement_name == NULL || status == NULL) {
        return false;
    }
    if (m_post_query_needed) {
        JNI_LOG_WARN("Post query was not ran after last run_pre_query, running it now and moving on.");
        Processor::run_post_query(m_stub_conn);
        m_post_query_needed = false;
    }
    clean_stmnt();
    m_stmnt = new (std::nothrow) StatementData(m_stub_conn, statement_name, query, 0, 0, 0, 0, 0);
    if (m_stmnt == NULL) {
        return false;
    }
    bool clientLogicRet = Processor::run_pre_query(m_stmnt);
    m_post_query_needed = true; // must be right after the run pre query call
    if (!clientLogicRet) {
        status->set_error(JNI_SYSTEM_ERROR_CODES::STMNT_PRE_QUERY_FAILED);
        JNI_LOG_ERROR("%s", status->get_error_message() ? status->get_error_message() : "");
        return false;
    }
    // run_post_query is required here to make replace parameters work
    m_stub_conn->client_logic->m_lastResultStatus = PGRES_COMMAND_OK;
    m_stub_conn->queryclass = PGQUERY_PREPARE;
    Processor::run_post_query(m_stub_conn);
    m_stub_conn->queryclass = PGQUERY_SIMPLE; // Setting it back to PGQUERY_SIMPLE as the default
    return true;
}
/*
 * replace values of statement parameters from user input to client logic format
 * @param statement_name the name of the statement
 * @param param_values the current parameters
 * @param parameter_count the number of parameters
 * @param status placeholder to hold error information if any
 * @return true on success and false on failures
 */
bool ClientLogicJNI::replace_statement_params(const char *statement_name, const char * const param_values[],
    size_t parameter_count, DriverError *status)
{
    if (statement_name == NULL || param_values == NULL || status == NULL) {
        return false;
    }

    int *param_lengths = new (std::nothrow) int[parameter_count];
    if (param_lengths == NULL) {
        return false;
    }
    for (size_t i = 0; i < parameter_count; ++i) {
        param_lengths[i] = (int)strlen(param_values[i]);
    }

    Oid *param_types = NULL;
    if (parameter_count > 0) {
        param_types = (Oid *)malloc(parameter_count * sizeof(Oid));
        if (param_types == NULL) {
            delete[] param_lengths;
            return false;
        }
        check_memset_s(memset_s(param_types, parameter_count * sizeof(Oid), 0, parameter_count * sizeof(Oid)));
    }

    clean_stmnt();
    m_stmnt = new (std::nothrow)
        StatementData(m_stub_conn, statement_name, parameter_count, param_types, param_values, param_lengths, 0);
    if (m_stmnt == NULL) {
        delete[] param_lengths;
        libpq_free(param_types);
        return false;
    }
    bool success = Processor::run_pre_exec(m_stmnt);
    m_post_query_needed = true;
    delete[] param_lengths;
    libpq_free(param_types);
    if (!success) {
        status->set_error(JNI_SYSTEM_ERROR_CODES::PRE_QUERY_EXEC_FAILED);
    }
    return success;
}

/*
 * @return handle to the statement data, to be used after calling run_pre_query, preare_statement and
 * replace_statement_params
 */
const StatementData *ClientLogicJNI::get_statement_data() const
{
    return m_stmnt;
}

/*
 * replace client logic values in error message coming from the server
 * For example, when inserting a duplicate unique value,
 * it will change the client logic value of \x... back to the user input
 * The buffer is created using new here and has to be cleared by the caller
 * @param[in] original_message the message received from the server
 * @param[out] new_message the message to present to the user - created here and has to be cleared by the caller !!!
 * @param[in] buffer_size the new message buffer created
 * @param status error information in case of failure
 * @return true if the message changed and false if it was not changed
 */
bool ClientLogicJNI::replace_message(const char *original_message, char **new_message, DriverError *status) const
{
    // Nothing need to be done
    if (original_message == NULL || strlen(original_message) == 0 || new_message == NULL || status == NULL) {
        return true; 
    }

    // The converted message shall be shorter than the original message
    *new_message = (char *)malloc(strlen(original_message) * sizeof(char)); 
    if (*new_message == NULL) {
        JNI_LOG_ERROR("Could not create new message string in replace_message");
        status->set_error(JNI_SYSTEM_ERROR_CODES::UNEXPECTED_ERROR);
        return false;
    }
    bool retval = replace_message_impl(m_stub_conn, original_message, *new_message, strlen(original_message), status);
    if (status->get_error_code() != 0) {
        retval = false;
        JNI_LOG_ERROR("replace message failed");
    }
    if (!retval) {
        return false;
    }
    return true;
}

/*
 * replace client logic values in error message coming from the server
 * For example, when inserting a duplicate unique value,
 * it will change the client logic value of \x... back to the user input
 * @param[in] conn database connection
 * @param[in] original_message the message received from the server
 * @param[out] new_message the message to present to the user
 * @param[in] buffer_size the max buffer size to write to
 * @param status error information in case of failure
 * @return true if the message changed and false if it was not changed
 */
bool ClientLogicJNI::replace_message_impl(PGconn *conn, const char *original_message, char *new_message,
    size_t buffer_size, DriverError *status)
{
    if (conn == NULL || original_message == NULL || new_message == NULL || status == NULL) {
        return false; /* Should never happen */
    }
    bool isProcessedMessage(false);
    unsigned char *processedValueStart = (unsigned char *)strstr(original_message, ")=(\\x");
    if (processedValueStart == NULL) {
        return false;
    }
    processedValueStart += 3; /* 3 is move forward to the \\x */
    char stop_chars[] = ",)";
    while (processedValueStart != NULL) {
        /* locating the closing parentheses */
        unsigned char *processedValueEnd =
            (unsigned char *)strpbrk((const char *)(processedValueStart + 1), stop_chars);
        if (processedValueEnd != NULL) {
            int processedValueSize = processedValueEnd - processedValueStart;
            char valueEofChar = processedValueStart[processedValueSize];
            /* string must be null terminated, processedValueSize is ignored for textual values */
            processedValueStart[processedValueSize] = '\0';
            /* deprocess data */
            unsigned char *plaintext = (unsigned char *)malloc(processedValueSize);
            if (plaintext == NULL) {
                return false;
            }
            size_t plaintextSize(0);
            Assert(processedValueSize >= 0);
            bool ret = RawValues::get_unprocessed_data(conn->client_logic->rawValuesForReplace, processedValueStart,
                plaintext, plaintextSize);

            /* place back the removed char (it was removed for the null terminated \0) */
            processedValueStart[processedValueSize] = valueEofChar;
            /* build error string if deprocessing succeeded */
            if (ret) {
                /* copy error string chunk by chunk */
                if (!isProcessedMessage) {
                    errno_t rc = strncpy_s(new_message, buffer_size, original_message,
                        (char *)processedValueStart - original_message); /* until processed text */
                    if (rc != EOK) {
                        JNI_LOG_ERROR("str copy failed");
                        status->set_error(JNI_SYSTEM_ERROR_CODES::UNEXPECTED_ERROR);
                        libpq_free(plaintext);
                        return false;
                    }
                    rc = strncat_s(new_message, buffer_size, (char *)plaintext, plaintextSize); /* deprocessed text */
                    if (rc != EOK) {
                        JNI_LOG_ERROR("strncat failed");
                        status->set_error(JNI_SYSTEM_ERROR_CODES::UNEXPECTED_ERROR);
                        libpq_free(plaintext);
                        return false;
                    }
                    rc = strncat_s(new_message, buffer_size, (char *)processedValueEnd,
                        strlen((char *)processedValueEnd)); /* end of string */
                    if (rc != EOK) {
                        JNI_LOG_ERROR("strncat failed");
                        status->set_error(JNI_SYSTEM_ERROR_CODES::UNEXPECTED_ERROR);
                        libpq_free(plaintext);
                        return false;
                    }
                } else {
                    /* location where to start writing the modified message at */
                    char *loc = strstr(new_message, "\\x");
                    if (loc != NULL) {
                        errno_t rc = memset_s(loc, buffer_size - ((char *)processedValueStart - original_message), '\0',
                            strlen(loc)); /* until processed text */
                        if (rc != EOK) {
                            JNI_LOG_ERROR("memeset copy failed");
                            status->set_error(JNI_SYSTEM_ERROR_CODES::UNEXPECTED_ERROR);
                            libpq_free(plaintext);
                            return false;
                        }
                        rc = strncpy_s(loc, buffer_size - ((char *)processedValueStart - original_message),
                            (char *)plaintext, plaintextSize); /* deprocessed text */
                        if (rc != EOK) {
                            JNI_LOG_ERROR("str copy failed");
                            status->set_error(JNI_SYSTEM_ERROR_CODES::UNEXPECTED_ERROR);
                            libpq_free(plaintext);
                            return false;
                        }
                        rc = strncat_s(loc, buffer_size - ((char *)processedValueStart - original_message),
                            (char *)processedValueEnd, strlen((char *)processedValueEnd)); /* end of string */
                        if (rc != EOK) {
                            JNI_LOG_ERROR("str cat failed");
                            status->set_error(JNI_SYSTEM_ERROR_CODES::UNEXPECTED_ERROR);
                            libpq_free(plaintext);
                            return false;
                        }
                    }
                }
                isProcessedMessage = true;
            }
            libpq_free(plaintext);
            processedValueStart = (unsigned char *)strstr((char *)processedValueEnd, "\\x");
        }
    }
    return isProcessedMessage;
}

/* *
 * maintains one active query per connection.
 * it is required to keep the original query until a new query arrives for replace error message to function.
 * @param query the query to use
 * @return duplicate instance of a query
 */
const char *ClientLogicJNI::get_new_query(const char *query)
{
    clear_current_query();
    current_query = strdup(query);
    if (current_query == NULL) {
        JNI_LOG_ERROR("String duplication failed");
    }
    return current_query;
}

/**
 * Reloads the client logic cache only if the local timestamp
 * is earlier than the maximum timestmp on the server
 */
void ClientLogicJNI::reload_cache_if_needed() const
{
    JNI_LOG_DEBUG("in reload_cache_if_needed");
    m_stub_conn->client_logic->m_cached_column_manager->reload_cache_if_needed(m_stub_conn);
}

/* *
 * Cleans up the current query pointer
 */
void ClientLogicJNI::clear_current_query()
{
    if (current_query != NULL) {
        free(current_query);
        current_query = NULL;
    }
}

/* *
 * set pointers needed for JNI operations
 */
void ClientLogicJNI::set_jni_env_and_cl_impl(JNIEnv *env, jobject jni_cl_impl)
{
    m_stub_conn->client_logic->m_data_fetcher_manager->set_jni_env_and_cl_impl(env, jni_cl_impl);
}

/**
 * Reloads the client logic cache, called when there is an error related to missing client logic cache
 */
void ClientLogicJNI::reload_cache() const
{
    JNI_LOG_DEBUG("in reload_cache");
    m_stub_conn->client_logic->cacheRefreshType = CacheRefreshType::CACHE_ALL;
    m_stub_conn->client_logic->m_cached_column_manager->load_cache(m_stub_conn); // Load the cache
}
