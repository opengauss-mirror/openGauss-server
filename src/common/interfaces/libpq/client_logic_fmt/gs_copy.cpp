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
 * gs_copy.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_fmt\gs_copy.cpp
 *
 * -------------------------------------------------------------------------
 */
 
#ifdef HAVE_CE

#include "postgres_fe.h"
#include "nodes/parsenodes_common.h"
#include <ctype.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "mb/pg_wchar.h"
#include "gs_copy.h"
#include "libpq/libpq-int.h"
#include "client_logic_processor/stmt_processor.h"
#include "client_logic_processor/values_processor.h"
#include "client_logic_processor/prepared_statement.h"
#include "client_logic_processor/prepared_statements_list.h"
#include "client_logic_hooks/hooks_manager.h"
#include "client_logic_cache/icached_column.h"
#include "client_logic_common/statement_data.h"
#include "client_logic_fmt/copy_state_data.h"

#define ISOCTAL(c) (((c) >= '0') && ((c) <= '7'))
#define OCTVALUE(c) ((c) - '0')
#define INTEGER_SIZE 64

#ifdef ENABLE_UT
#define static
#endif

/* non-export function prototypes */
static CopyStateData *begin_copy(bool is_from, bool is_rel, Node *raw_query, const char *query_string,
    List *attnamelist, List *options);
static CopyStateData *begin_copy_to(bool is_rel, Node *query, const char *query_string, const char *filename,
    List *attnamelist, List *options);
static CopyStateData *begin_copy_from(bool is_rel, const char *filename, List *attnamelist, List *options);

static int deprocess_csv_line(PGconn *conn, PreparedStatement *entry, const char *in_buffer, int msg_length,
    char **buffer);
static int deprocess_txt_line(PGconn *conn, PreparedStatement *entry, const char *in_buffer, int msg_length,
    char **buffer);

static int process_csv_chunk(PGconn *conn, PreparedStatement *entry, const char *in_buffer, int msg_length,
    char **buffer);
static int process_txt_chunk(PGconn *conn, PreparedStatement *entry, const char *in_buffer, int msg_length,
    char **buffer);

static int check_line_end(CopyStateData *cstate, const char *in_buffer, int msg_length);
static int remove_line_end(CopyStateData *cstate, const char *in_buffer, int msg_length);
static bool deprocess_and_replace(PGconn *conn, PreparedStatement *entry, const char *processed_bytea,
    size_t processed_bytea_size, bool is_quoted, char **buffer, int &written, int &res_length);
static bool process_and_replace(PGconn *conn, PreparedStatement *entry, const char *processed_bytea,
    size_t processed_bytea_size, bool is_quoted, char **buffer, int &written, int &res_length);
static bool append_buffer(char **buffer, int &res_length, int &written, const char *data, int length);
static int get_decimal_from_hex(char hex);

CopyStateData *pre_copy(const CopyStmt * const stmt, const char *query_string)
{
    if (stmt->is_from) {
        return begin_copy_from(stmt->relation != NULL, stmt->filename, stmt->attlist, stmt->options);
    } else {
        return begin_copy_to(stmt->relation != NULL, stmt->query, query_string, stmt->filename, stmt->attlist,
            stmt->options);
    }
}
/* copy from csv or txt into buffer */
int deprocess_copy_line(PGconn *conn, const char *in_buffer, int msg_length, char **buffer)
{
    PreparedStatement *entry = conn->client_logic->preparedStatements->get_or_create(conn->client_logic->lastStmtName);
    if (!entry) {
        Assert(false);
        return -1;
    }
    CopyStateData *cstate = entry->copy_state;
    if (!cstate || entry->original_data_types_oids_size == 0) {
        return -1;
    }

    if (cstate->binary) {
        /* DO NOT SUPPORT BINARY COPY FOR NOW */
        return -1;
    } else {
        cstate->cur_lineno++;
        /* on input just throw the header line away */
        if (cstate->cur_lineno == 1 && cstate->header_line) {
            return -1;
        }
        /*
         * lines with processed data must be longer than 3, 
         * so it is easier to detect special lines that 
         * we do not touch like EOF. 
         */
        if (msg_length < 3) {
            return -1;
        }

        msg_length = remove_line_end(cstate, in_buffer, msg_length);

        int decrypted_msg_length = 0;
        if (cstate->csv_mode) {
            decrypted_msg_length = deprocess_csv_line(conn, entry, in_buffer, msg_length, buffer);
        } else {
            decrypted_msg_length = deprocess_txt_line(conn, entry, in_buffer, msg_length, buffer);
        }
        if (decrypted_msg_length < 0) {
            libpq_free(*buffer);
        }
        return decrypted_msg_length;
    }
}

int process_copy_chunk(PGconn *conn, const char *in_buffer, int msg_length, char **buffer)
{
    PreparedStatement *entry = conn->client_logic->preparedStatements->get_or_create(conn->client_logic->lastStmtName);
    if (!entry) {
        Assert(false);
        return -1;
    }
    CopyStateData *cstate = entry->copy_state;
    if (!cstate || !entry->cached_copy_columns || !entry->cached_copy_columns->is_to_process()) {
        return 0; /* no need to process data */
    }
    if (cstate->binary) {
        /* DO NOT SUPPORT BINARY COPY FOR NOW */
        fprintf(stderr, "cstate->binary is NULL\n");
        return -1;
    } else {
        char *full_chunk(NULL);
        if (entry->partial_csv_column_size > 0) {
            /* we need to concatenate the partialCsv we have saved with the incoming buffer */
            if ((size_t)msg_length > (size_t)PG_INT32_MAX - entry->partial_csv_column_size) {
                Assert(false);
                return -1;
            }
            size_t new_size = entry->partial_csv_column_size + msg_length;
            full_chunk = (char *)malloc(new_size);
            if (full_chunk == NULL) {
                Assert(false);
                return -1;
            } else {
                check_memset_s(memset_s(full_chunk, new_size, 0, new_size));
            }

            /*
             * 1. copy the partialCsv to the beginning of the chunk
             * 2. copy the incoming buffer to the end of the chunk
             */
            check_memcpy_s(memcpy_s(full_chunk, new_size, entry->partial_csv_column, entry->partial_csv_column_size));
            check_memcpy_s(memcpy_s(full_chunk + entry->partial_csv_column_size,
                new_size - entry->partial_csv_column_size, in_buffer, msg_length));

            /*
             * 1. reset the partialCsv
             * 2. prepare buffer/chunk for processing
             */
            entry->partial_csv_column_size = 0;
            in_buffer = full_chunk;
            msg_length = new_size;
        }

        int ret = 0;
        if (cstate->csv_mode) {
            ret = process_csv_chunk(conn, entry, in_buffer, msg_length, buffer);
        } else {
            ret = process_txt_chunk(conn, entry, in_buffer, msg_length, buffer);
        }
        if (ret < 0) {
            libpq_free(*buffer);
        }
        if (full_chunk != NULL) {
            free(full_chunk);
        }
        return ret;
    }
}

/* Return decimal value for a hexadecimal digit */
static int get_decimal_from_hex(char hex)
{
    if (isdigit((unsigned char)hex)) {
        return hex - '0';
    } else {
        const int decimal_base = 10;
        return tolower((unsigned char)hex) - 'a' + decimal_base;
    }
}
/* append data to the end of buffer */
static bool append_buffer(char **buffer, int &res_length, int &written, const char *data, int length)
{
    Assert(*buffer != NULL);
    int new_size = written + length;
    if (res_length < new_size) {
        char *tmp_buffer = (char *)malloc(new_size);
        if (tmp_buffer == NULL) {
            written = res_length = 0;
            return false;
        }
        check_memcpy_s(memcpy_s(tmp_buffer, new_size, *buffer, res_length));
        free(*buffer);
        *buffer = tmp_buffer;
        res_length = new_size;
    }

    if (length == 0) {
        return true;
    }
    check_memcpy_s(memcpy_s((*buffer) + written, res_length - written, data, length));
    written += length;
    return true;
}

/* Eliminate \r \n in the end of line */
static int remove_line_end(CopyStateData *cstate, const char *in_buffer, int msg_length)
{
    cstate->cur_eol = "";
    const int end_char_count = 2;
    switch (cstate->eol_type) {
        case EOL_NL:
            if (msg_length >= 1 && in_buffer[msg_length - 1] == '\n') {
                --msg_length;
                cstate->cur_eol = "\n";
            }
            break;
        case EOL_CR:
            if (msg_length >= 1 && in_buffer[msg_length - 1] == '\r') {
                --msg_length;
                cstate->cur_eol = "\r";
            }
            break;
        case EOL_CRNL:
            if (msg_length >= end_char_count && in_buffer[msg_length - end_char_count] == '\r' &&
                in_buffer[msg_length - 1] == '\n') {
                msg_length -= end_char_count;
                cstate->cur_eol = "\r\n";
            }
            break;
        case EOL_UNKNOWN:
            if (msg_length >= end_char_count && in_buffer[msg_length - end_char_count] == '\r' &&
                in_buffer[msg_length - 1] == '\n') {
                cstate->eol_type = EOL_CRNL;
                msg_length -= end_char_count;
                cstate->cur_eol = "\r\n";
            } else if (msg_length >= 1 && in_buffer[msg_length - 1] == '\n') {
                cstate->eol_type = EOL_NL;
                msg_length -= 1;
                cstate->cur_eol = "\n";
            } else if (msg_length >= 1 && in_buffer[msg_length - 1] == '\r') {
                cstate->eol_type = EOL_CR;
                msg_length -= 1;
                cstate->cur_eol = "\r";
            }
            break;
        default:
            break;
    }

    return msg_length;
}

/* check if exists \r \n in the end of line */
static int check_line_end(CopyStateData *cstate, const char *in_buffer, int msg_length)
{
    int res = 0;
    cstate->cur_eol = "";
    const int end_char_count = 2;
    switch (cstate->eol_type) {
        case EOL_NL:
            if (msg_length >= 1 && in_buffer[0] == '\n') {
                res = 1;
            }
            break;
        case EOL_CR:
            if (msg_length >= 1 && in_buffer[0] == '\r') {
                res = 1;
            }
            break;
        case EOL_CRNL:
            if (msg_length >= end_char_count && in_buffer[0] == '\r' && in_buffer[1] == '\n') {
                res = end_char_count;
            }
            break;
        case EOL_UNKNOWN:
            if (msg_length >= end_char_count && in_buffer[0] == '\r' && in_buffer[1] == '\n') {
                cstate->eol_type = EOL_CRNL;
                res = end_char_count;
            } else if (msg_length >= 1 && in_buffer[0] == '\n') {
                cstate->eol_type = EOL_NL;
                res = 1;
            } else if (msg_length >= 1 && in_buffer[0] == '\r') {
                cstate->eol_type = EOL_CR;
                res = 1;
            }
            break;
        default:
            break;
    }

    return res;
}

/*
 * check the field need to be encrypted or not
 */
static bool field_needs_processing(PGconn *conn, PreparedStatement *entry)
{
    if (!entry || !entry->cached_copy_columns) {
        Assert(false);
        return false;
    }

    /*
     * if the attlist is not null, copy the specifying columns
     * if the attlist is null, copy the whole table
     */
    if (!entry->copy_state->is_attlist_null) {
        const ICachedColumn *cached_column = entry->cached_copy_columns->at(entry->copy_state->fieldno);
        if (cached_column) {
            return true;
        }
    } else {
        size_t cached_columns_size = entry->cached_copy_columns->size();
        for (size_t i = 0; i < cached_columns_size; ++i) {
            const ICachedColumn *cached_column = entry->cached_copy_columns->at(i);
            if (!cached_column) {
                continue;
            }
            if (entry->copy_state->fieldno == (int)cached_column->get_col_idx() - 1) {
                return true;
            }
        }
    }
    return false;
}

static int deprocess_txt_line(PGconn *conn, PreparedStatement *entry, const char *in_buffer, int msg_length,
    char **buffer)
{
    if (!entry) {
        return -1;
    }
    *buffer = NULL;
    CopyStateData *cstate = entry->copy_state;

    static const int DATA_BUFFER_SIZE = 64;
    size_t data_allocated = DATA_BUFFER_SIZE;
    char *data = (char *)malloc(data_allocated);
    if (data == NULL) {
        return -1;
    } else {
        check_memset_s(memset_s(data, data_allocated, 0, data_allocated));
    }
    size_t data_size = 0;

    char delimc = cstate->delim;
    const char *cur_ptr;
    const char *line_end_ptr;

    /* set pointer variables for loop */
    cur_ptr = in_buffer;
    line_end_ptr = in_buffer + msg_length;

    /* Outer loop iterates over fields */
    cstate->fieldno = 0;
    size_t buffer_size = msg_length + (cstate->cur_eol ? strlen(cstate->cur_eol) : 0);
    *buffer = (char *)malloc(buffer_size);
    if (*buffer == NULL) {
        free(data);
        data = NULL;
        return -1;
    } else {
        check_memset_s(memset_s(*buffer, buffer_size, 0, buffer_size));
    }
    int res_length = buffer_size;
    int written = 0;
    for (; ;) {
        bool found_delim = false;
        const char *start_ptr = NULL;
        const char *end_ptr = NULL;
        int input_len;
        bool saw_non_ascii = false;

        /* Make sure there is enough space for the next value */
        if (cstate->fieldno >= (int)entry->original_data_types_oids_size) {
            free(data);
            data = NULL;
            return -1;
        }

        /* Remember start of field on both input and output sides */
        start_ptr = cur_ptr;

        /*
         * Scan data for field.
         *
         * Note that in this loop, we are scanning to locate the end of field
         * and also speculatively performing de-escaping.  Once we find the
         * end-of-field, we can match the raw field contents against the null
         * marker string.  Only after that comparison fails do we know that
         * de-escaping is actually the right thing to do; therefore we *must
         * not* throw any syntax errors before we've done the null-marker
         * check.
         */
        data_size = 0;
        for (; ;) {
            char c;

            end_ptr = cur_ptr;
            if (cur_ptr >= line_end_ptr) {
                break;
            }
            c = *cur_ptr++;
            if (c == delimc) {
                found_delim = true;
                break;
            }
            if (c == '\\') {
                if (cur_ptr >= line_end_ptr) {
                    break;
                }
                c = *cur_ptr++;
                switch (c) {
                    case '0':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7': {
                        /* handle \013 */
                        int val;

                        val = OCTVALUE(c);
                        if (cur_ptr < line_end_ptr) {
                            c = *cur_ptr;
                            if (ISOCTAL(c)) {
                                cur_ptr++;
                                val = (val << 3) + OCTVALUE(c);
                                if (cur_ptr < line_end_ptr) {
                                    c = *cur_ptr;
                                    if (ISOCTAL(c)) {
                                        cur_ptr++;
                                        val = (val << 3) + OCTVALUE(c);
                                    }
                                }
                            }
                        }
                        c = val & 0377;
                        if (c == '\0' || IS_HIGHBIT_SET(c)) {
                            saw_non_ascii = true;
                        }
                    } break;
                    case 'x':
                        /* Handle \x3F */
                        if (cur_ptr < line_end_ptr) {
                            char hexchar = *cur_ptr;

                            if (isxdigit((unsigned char)hexchar)) {
                                int val = get_decimal_from_hex(hexchar);

                                cur_ptr++;
                                if (cur_ptr < line_end_ptr) {
                                    hexchar = *cur_ptr;
                                    if (isxdigit((unsigned char)hexchar)) {
                                        cur_ptr++;
                                        val = (val << 4) + get_decimal_from_hex(hexchar);
                                    }
                                }
                                c = val & 0xff;
                                if (c == '\0' || IS_HIGHBIT_SET(c)) {
                                    saw_non_ascii = true;
                                }
                            }
                        }
                        break;
                    case 'b':
                        c = '\b';
                        break;
                    case 'f':
                        c = '\f';
                        break;
                    case 'n':
                        c = '\n';
                        break;
                    case 'r':
                        c = '\r';
                        break;
                    case 't':
                        c = '\t';
                        break;
                    case 'v':
                        c = '\v';
                        break;

                        /*
                         * in all other cases, take the char after '\'
                         * literally
                         */
                    default:
                        break;
                }
            }

            /* Add c to output string */
            if (entry->original_data_types_oids[cstate->fieldno] != 0) {
                if (data_size >= data_allocated) {
                    data = (char *)libpq_realloc(data, data_allocated, data_allocated + DATA_BUFFER_SIZE);
                    if (data == NULL) {
                        return -1;
                    }
                    check_memset_s(memset_s(data + data_allocated, DATA_BUFFER_SIZE, 0, DATA_BUFFER_SIZE));
                    data_allocated += DATA_BUFFER_SIZE;
                }
                data[data_size] = c;
                ++data_size;
            }
        }

        /* Check whether raw input matched null marker */
        input_len = end_ptr - start_ptr;
        if (entry->original_data_types_oids[cstate->fieldno] == 0 ||
            (input_len == cstate->null_print_len && strncmp(start_ptr, cstate->null_print, input_len) == 0)) {
            if (!append_buffer(buffer, res_length, written, start_ptr, cur_ptr - start_ptr)) {
                free(data);
                data = NULL;
                return -1;
            }
        } else {
            if (!deprocess_and_replace(conn, entry, data, data_size, false, buffer, written, res_length)) {
                free(data);
                data = NULL;
                return -1;
            }
            if (!append_buffer(buffer, res_length, written, end_ptr, cur_ptr - end_ptr)) {
                free(data);
                data = NULL;
                return -1;
            }
        }

        cstate->fieldno++;
        /* Done if we hit EOL instead of a delim */
        if (!found_delim) {
            break;
        }
    }
    free(data);
    data = NULL;

    if (written) {
        if (cstate->cur_eol) {
            if (!append_buffer(buffer, res_length, written, cstate->cur_eol, strlen(cstate->cur_eol))) {
                return -1;
            }
        }
        if (!append_buffer(buffer, res_length, written, "\0", 1)) {
            return -1;
        }
        --written;
    }

    return written;
}


static int process_txt_chunk(PGconn *conn, PreparedStatement *entry, const char *in_buffer, int msg_length,
    char **buffer)
{
    *buffer = NULL;
    if (!entry) {
        return -1;
    }
    static const int DATA_BUFFER_SIZE = 64;
    size_t data_allocated = DATA_BUFFER_SIZE;
    char *data = (char *)calloc(sizeof(char), data_allocated);
    if (data == NULL) {
        printfPQExpBuffer(&conn->errorMessage, "ERROR(CLIENT): failed to allocate memory for data\n");
        return -1;
    }
    size_t data_size = 0;

    CopyStateData *cstate = entry->copy_state;

    char delimc = cstate->delim;
    const char *cur_ptr;
    const char *line_end_ptr;

    bool is_field_needs_processing = field_needs_processing(conn, entry);

    /* set pointer variables for loop */
    cur_ptr = in_buffer;
    line_end_ptr = in_buffer + msg_length;

    /* Outer loop iterates over fields */
    size_t buffer_size = msg_length + (cstate->cur_eol ? strlen(cstate->cur_eol) : 0);
    *buffer = (char *)malloc(buffer_size);
    if (*buffer == NULL) {
        free(data);
        data = NULL;
        printfPQExpBuffer(&conn->errorMessage, "ERROR(CLIENT): failed to allocate memory for buffer\n");
        return -1;
    } else {
        check_memset_s(memset_s(*buffer, buffer_size, 0, buffer_size));
    }
    int res_length = buffer_size;
    int written = 0;
    for (; ;) {
        bool found_delim = false;
        bool found_eol = false;
        const char *start_ptr = NULL;
        const char *end_ptr = NULL;
        int input_len;
        bool saw_non_ascii = false;

        /* Remember start of field on both input and output sides */
        start_ptr = cur_ptr;

        /*
         * Scan data for field.
         *
         * Note that in this loop, we are scanning to locate the end of field
         * and also speculatively performing de-escaping.  Once we find the
         * end-of-field, we can match the raw field contents against the null
         * marker string.  Only after that comparison fails do we know that
         * de-escaping is actually the right thing to do; therefore we *must
         * not* throw any syntax errors before we've done the null-marker
         * check.
         */
        data_size = 0;
        for (; ;) {
            char c;

            end_ptr = cur_ptr;
            if (cur_ptr >= line_end_ptr) {
                break;
            }

            int offset = check_line_end(cstate, cur_ptr, line_end_ptr - cur_ptr);
            if (offset) {
                cur_ptr += offset;
                found_eol = true;
                break;
            }

            c = *cur_ptr++;
            if (c == delimc) {
                found_delim = true;
                break;
            }
            if (c == '\\') {
                if (cur_ptr >= line_end_ptr) {
                    break;
                }
                c = *cur_ptr++;
                bool for_exit = false;
                switch (c) {
                    case '0':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7': {
                        /* handle \013 */
                        int val;

                        val = OCTVALUE(c);
                        if (cur_ptr < line_end_ptr) {
                            c = *cur_ptr;
                            if (ISOCTAL(c)) {
                                cur_ptr++;
                                val = (val << 3) + OCTVALUE(c);
                                if (cur_ptr < line_end_ptr) {
                                    c = *cur_ptr;
                                    if (ISOCTAL(c)) {
                                        cur_ptr++;
                                        val = (val << 3) + OCTVALUE(c);
                                    }
                                }
                            }
                        }
                        c = val & 0377;
                        if (c == '\0' || IS_HIGHBIT_SET(c)) {
                            saw_non_ascii = true;
                        }
                    } break;
                    case 'x':
                        /* Handle \x3F */
                        if (cur_ptr < line_end_ptr) {
                            char hexchar = *cur_ptr;

                            if (isxdigit((unsigned char)hexchar)) {
                                int val = get_decimal_from_hex(hexchar);

                                cur_ptr++;
                                if (cur_ptr < line_end_ptr) {
                                    hexchar = *cur_ptr;
                                    if (isxdigit((unsigned char)hexchar)) {
                                        cur_ptr++;
                                        val = (val << 4) + get_decimal_from_hex(hexchar);
                                    }
                                }
                                c = val & 0xff;
                                if (c == '\0' || IS_HIGHBIT_SET(c)) {
                                    saw_non_ascii = true;
                                }
                            }
                        }
                        break;
                    case 'b':
                        c = '\b';
                        break;
                    case 'f':
                        c = '\f';
                        break;
                    case 'n':
                        c = '\n';
                        break;
                    case 'r':
                        c = '\r';
                        break;
                    case 't':
                        c = '\t';
                        break;
                    case 'v':
                        c = '\v';
                        break;
                    case '\n':
                        found_eol = true;
                        for_exit = true;
                        cur_ptr--;
                        c = '\\';
                        break;
                        /*
                         * in all other cases, take the char after '\'
                         * literally
                         */
                    default:
                        break;
                }
                if (for_exit) {
                    break;
                }
            }

            /* Add c to output string */
            if (is_field_needs_processing) {
                if (data_size >= data_allocated) {
                    data = (char *)libpq_realloc(data, data_allocated, data_allocated + DATA_BUFFER_SIZE);
                    if (data == NULL) {
                        printfPQExpBuffer(&conn->errorMessage, "ERROR(CLIENT): failed to realloc memory for data\n");
                        return -1;
                    }
                    data_allocated += DATA_BUFFER_SIZE;
                }
                data[data_size] = c;
                ++data_size;
            }
        }

        /* Check whether raw input matched null marker */
        input_len = end_ptr - start_ptr;
        if ((cstate->header_line && cstate->cur_lineno == 0) || !is_field_needs_processing ||
            (input_len == cstate->null_print_len && strncmp(start_ptr, cstate->null_print, input_len) == 0)) {
            /* copy as-is */
            if (!append_buffer(buffer, res_length, written, start_ptr, cur_ptr - start_ptr)) {
                free(data);
                data = NULL;
                printfPQExpBuffer(&conn->errorMessage, "ERROR(CLIENT): failed to append buffer for data\n");
                return -1;
            }
        } else if (found_delim || found_eol || (end_ptr == line_end_ptr)) {
            /* process */
            if (!process_and_replace(conn, entry, data, data_size, false, buffer, written, res_length)) {
                free(data);
                data = NULL;
                return -1;
            }
            if (!append_buffer(buffer, res_length, written, end_ptr, cur_ptr - end_ptr)) {
                free(data);
                data = NULL;
                printfPQExpBuffer(&conn->errorMessage, "ERROR(CLIENT): failed to append buffer for process data\n");
                return -1;
            }
        } else {
            /* aggregate partial column for processing until the next chunk is available */
            size_t new_size = cur_ptr - start_ptr;
            if (entry->partial_csv_column_allocated < new_size) {
                entry->partial_csv_column =
                    (char *)libpq_realloc(entry->partial_csv_column, entry->partial_csv_column_size, new_size);
                if (entry->partial_csv_column == NULL) {
                    free(data);
                    data = NULL;
                    printfPQExpBuffer(&conn->errorMessage,
                        "ERROR(CLIENT): failed to realloc memory for partial_csv_column");
                    return -1;
                }
                entry->partial_csv_column_allocated = new_size;
            }
            check_memcpy_s(
                memcpy_s(entry->partial_csv_column, entry->partial_csv_column_allocated, start_ptr, new_size));
            entry->partial_csv_column_size = new_size;
        }

        if (found_delim) {
            cstate->fieldno++;
            is_field_needs_processing = field_needs_processing(conn, entry);
        } else if (found_eol) {
            cstate->cur_lineno++;
            cstate->fieldno = 0;
            is_field_needs_processing = field_needs_processing(conn, entry);
        } else { /* if (!found_delim && !found_eol) */
            break;
        }
    }
    free(data);
    data = NULL;

    if (written) {
        if (!append_buffer(buffer, res_length, written, "\0", 1)) {
            printfPQExpBuffer(&conn->errorMessage, "ERROR(CLIENT): failed to append buffer for written data");
            return -1;
        }
        --written;
    }

    return written;
}

static int deprocess_csv_line(PGconn *conn, PreparedStatement *entry, const char *in_buffer, int msg_length,
    char **buffer)
{
    if (!entry) {
        Assert(false);
        return -1;
    }
    *buffer = NULL;
    CopyStateData *cstate = entry->copy_state;

    static const int DATA_BUFFER_SIZE = 64;
    size_t data_allocated = DATA_BUFFER_SIZE;
    char *data = (char *)malloc(data_allocated);
    if (data == NULL) {
        return -1;
    } else {
        check_memset_s(memset_s(data, data_allocated, 0, data_allocated));
    }
    size_t data_size = 0;

    char delimc = cstate->delim;
    char quotec = cstate->quote;
    char escapec = cstate->escape;
    const char *cur_ptr;
    const char *line_end_ptr;

    /* set pointer variables for loop */
    cur_ptr = in_buffer;
    line_end_ptr = in_buffer + msg_length;

    /* Outer loop iterates over fields */
    cstate->fieldno = 0;
    size_t buffer_size = msg_length + (cstate->cur_eol ? strlen(cstate->cur_eol) : 0);
    *buffer = (char *)malloc(buffer_size);
    if (*buffer == NULL) {
        free(data);
        data = NULL;
        return -1;
    } else {
        check_memset_s(memset_s(*buffer, buffer_size, 0, buffer_size));
    }
    int res_length = buffer_size;
    int written = 0;
    for (; ;) {
        bool found_delim = false;
        bool saw_quote = false;
        const char *start_ptr = NULL;
        const char *end_ptr = NULL;
        int input_len;

        /* Make sure there is enough space for the next value */
        if (cstate->fieldno >= (int)entry->original_data_types_oids_size) {
            free(data);
            data = NULL;
            return -1;
        }

        /* Remember start of field on both input and output sides */
        start_ptr = cur_ptr;

        /*
         * Scan data for field,
         *
         * The loop starts in "not quote" mode and then toggles between that
         * and "in quote" mode. The loop exits normally if it is in "not
         * quote" mode and a delimiter or line end is seen.
         */
        data_size = 0;
        for (; ;) {
            char c;

            /* Not in quote */
            for (; ;) {
                end_ptr = cur_ptr;
                if (cur_ptr >= line_end_ptr) {
                    goto endfield;
                }
                c = *cur_ptr++;
                /* unquoted field delimiter */
                if (c == delimc) {
                    found_delim = true;
                    goto endfield;
                }
                /* start of quoted field (or part of field) */
                if (c == quotec) {
                    saw_quote = true;
                    break;
                }
                /* Add c to output string */
                if (entry->original_data_types_oids[cstate->fieldno] != 0) {
                    if (data_size >= data_allocated) {
                        data = (char *)libpq_realloc(data, data_allocated, data_allocated + DATA_BUFFER_SIZE);
                        if (data == NULL) {
                            return -1;
                        }
                        check_memset_s(memset_s(data + data_allocated, DATA_BUFFER_SIZE, 0, DATA_BUFFER_SIZE));
                        data_allocated += DATA_BUFFER_SIZE;
                    }
                    data[data_size] = c;
                    ++data_size;
                }
            }

            /* In quote */
            for (; ;) {
                end_ptr = cur_ptr;
                if (cur_ptr >= line_end_ptr) {
                    free(data);
                    data = NULL;
                    return -1;
                }

                c = *cur_ptr++;

                /* escape within a quoted field */
                if (c == escapec) {
                    /*
                     * peek at the next char if available, and escape it if it
                     * is an escape char or a quote char
                     */
                    if (cur_ptr < line_end_ptr) {
                        char nextc = *cur_ptr;

                        if (nextc == escapec || nextc == quotec) {
                            if (entry->original_data_types_oids[cstate->fieldno] != 0) {
                                if (data_size >= data_allocated) {
                                    data =
                                        (char *)libpq_realloc(data, data_allocated, data_allocated + DATA_BUFFER_SIZE);
                                    if (data == NULL) {
                                        return -1;
                                    }
                                    data_allocated += DATA_BUFFER_SIZE;
                                }
                                data[data_size] = c;
                                ++data_size;
                            }
                            cur_ptr++;
                            continue;
                        }
                    }
                }

                /*
                 * end of quoted field. Must do this test after testing for
                 * escape in case quote char and escape char are the same
                 * (which is the common case).
                 */
                if (c == quotec) {
                    break;
                }

                /* Add c to output string */
                if (entry->original_data_types_oids[cstate->fieldno] != 0) {
                    if (data_size >= data_allocated) {
                        data = (char *)libpq_realloc(data, data_allocated, data_allocated + DATA_BUFFER_SIZE);
                        if (data == NULL) {
                            return -1;
                        }
                        data_allocated += DATA_BUFFER_SIZE;
                    }
                    data[data_size] = c;
                    ++data_size;
                }
            }
        }
    endfield:

        /* Check whether raw input matched null marker */
        input_len = end_ptr - start_ptr;
        if (entry->original_data_types_oids[cstate->fieldno] == 0 || (!saw_quote &&
            input_len == cstate->null_print_len && strncmp(start_ptr, cstate->null_print, input_len) == 0)) {
            if (!append_buffer(buffer, res_length, written, start_ptr, cur_ptr - start_ptr)) {
                free(data);
                data = NULL;
                return -1;
            }
        } else {
            if (!deprocess_and_replace(conn, entry, data, data_size, saw_quote, buffer, written, res_length)) {
                free(data);
                data = NULL;
                return -1;
            }
            if (!append_buffer(buffer, res_length, written, end_ptr, cur_ptr - end_ptr)) {
                return -1;
            }
        }

        cstate->fieldno++;
        /* Done if we hit EOL instead of a delim */
        if (!found_delim) {
            break;
        }
    }
    free(data);
    data = NULL;

    if (written) {
        if (cstate->cur_eol) {
            if (!append_buffer(buffer, res_length, written, cstate->cur_eol, strlen(cstate->cur_eol))) {
                return -1;
            }
        }
        if (!append_buffer(buffer, res_length, written, "\0", 1)) {
            return -1;
        }
        --written;
    }

    return written;
}

int process_csv_chunk(PGconn *conn, PreparedStatement *entry, const char *in_buffer, int msg_length, char **buffer)
{
    if (!entry) {
        Assert(false);
        return -1;
    }
    *buffer = NULL;
    CopyStateData *cstate = entry->copy_state;

    char delimc = cstate->delim;
    char quotec = cstate->quote;
    char escapec = cstate->escape;
    const char *cur_ptr;
    const char *line_end_ptr;
    bool is_field_needs_processing = field_needs_processing(conn, entry);

    static const int DATA_BUFFER_SIZE = 64;
    size_t data_allocated = DATA_BUFFER_SIZE;
    char *data = (char *)calloc(sizeof(char), data_allocated);
    if (data == NULL) {
        return -1;
    }
    size_t data_size = 0;

    /* set pointer variables for loop */
    cur_ptr = in_buffer;
    line_end_ptr = in_buffer + msg_length;

    /* Outer loop iterates over fields */
    size_t buffer_size = msg_length + (cstate->cur_eol ? strlen(cstate->cur_eol) : 0);
    *buffer = (char *)malloc(buffer_size);
    if (*buffer == NULL) {
        free(data);
        data = NULL;
        return -1;
    } else {
        check_memset_s(memset_s(*buffer, buffer_size, 0, buffer_size));
    }
    int res_length = buffer_size;
    int written = 0;
    for (; ;) {
        bool found_delim = false;
        bool found_eol = false;
        bool saw_quote = false;
        const char *start_ptr = NULL;
        const char *end_ptr = NULL;
        int input_len;

        /* Remember start of field on both input and output sides */
        start_ptr = cur_ptr;

        /*
         * Scan data for field,
         *
         * The loop starts in "not quote" mode and then toggles between that
         * and "in quote" mode. The loop exits normally if it is in "not
         * quote" mode and a delimiter or line end is seen.
         */
        data_size = 0;
        for (; ;) {
            char c;

            /* Not in quote */
            for (; ;) {
                end_ptr = cur_ptr;
                if (cur_ptr >= line_end_ptr) {
                    goto endfield;
                }

                int offset = check_line_end(cstate, cur_ptr, line_end_ptr - cur_ptr);
                if (offset) {
                    cur_ptr += offset;
                    found_eol = true;
                    goto endfield;
                }

                c = *cur_ptr++;
                /* unquoted field delimiter */
                if (c == delimc) {
                    found_delim = true;
                    goto endfield;
                }
                /* start of quoted field (or part of field) */
                if (c == quotec) {
                    saw_quote = true;
                    break;
                }
                /* Add c to output string */
                if (is_field_needs_processing) {
                    if (data_size >= data_allocated) {
                        data = (char *)libpq_realloc(data, data_allocated, data_allocated + DATA_BUFFER_SIZE);
                        if (data == NULL) {
                            return -1;
                        }
                        data_allocated += DATA_BUFFER_SIZE;
                    }
                    data[data_size] = c;
                    ++data_size;
                }
            }

            /* In quote */
            for (; ;) {
                end_ptr = cur_ptr;
                if (cur_ptr >= line_end_ptr) {
                    free(data);
                    data = NULL;
                    return -1;
                }

                c = *cur_ptr++;

                /* escape within a quoted field */
                if (c == escapec) {
                    /*
                     * peek at the next char if available, and escape it if it
                     * is an escape char or a quote char
                     */
                    if (cur_ptr < line_end_ptr) {
                        char nextc = *cur_ptr;

                        if (nextc == escapec || nextc == quotec) {
                            if (is_field_needs_processing) {
                                if (data_size >= data_allocated) {
                                    data =
                                        (char *)libpq_realloc(data, data_allocated, data_allocated + DATA_BUFFER_SIZE);
                                    if (data == NULL) {
                                        return -1;
                                    }
                                    data_allocated += DATA_BUFFER_SIZE;
                                }
                                data[data_size] = c;
                                ++data_size;
                            }
                            cur_ptr++;
                            continue;
                        }
                    }
                }

                /*
                 * end of quoted field. Must do this test after testing for
                 * escape in case quote char and escape char are the same
                 * (which is the common case).
                 */
                if (c == quotec) {
                    break;
                }

                /* Add c to output string */
                if (is_field_needs_processing) {
                    if (data_size >= data_allocated) {
                        data = (char *)libpq_realloc(data, data_allocated, data_allocated + DATA_BUFFER_SIZE);
                        if (data == NULL) {
                            return -1;
                        }
                        data_allocated += DATA_BUFFER_SIZE;
                    }
                    data[data_size] = c;
                    ++data_size;
                }
            }
        }
    endfield:

        /* Check whether raw input matched null marker */
        input_len = end_ptr - start_ptr;
        if ((cstate->header_line && cstate->cur_lineno == 0) || !is_field_needs_processing ||
            (!saw_quote && input_len == cstate->null_print_len &&
            strncmp(start_ptr, cstate->null_print, input_len) == 0)) {
            if (!append_buffer(buffer, res_length, written, start_ptr, cur_ptr - start_ptr)) {
                free(data);
                data = NULL;
                return -1;
            }
        } else if (found_delim || found_eol || (end_ptr == line_end_ptr)) {
            /* process */
            if (!process_and_replace(conn, entry, data, data_size, false, buffer, written, res_length)) {
                free(data);
                data = NULL;
                return -1;
            }
            if (!append_buffer(buffer, res_length, written, end_ptr, cur_ptr - end_ptr)) {
                free(data);
                data = NULL;
                return -1;
            }
        } else {
            /* aggregate partial column for processing until the next chunk is available */
            size_t new_size = cur_ptr - start_ptr;
            if (entry->partial_csv_column_allocated < new_size) {
                entry->partial_csv_column =
                    (char *)libpq_realloc(entry->partial_csv_column, entry->partial_csv_column_size, new_size);
                if (entry->partial_csv_column == NULL) {
                    free(data);
                    data = NULL;
                    return -1;
                }
                entry->partial_csv_column_allocated = new_size;
            }
            check_memcpy_s(
                memcpy_s(entry->partial_csv_column, entry->partial_csv_column_allocated, start_ptr, new_size));
            entry->partial_csv_column_size = new_size;
        }

        if (found_delim) {
            cstate->fieldno++;
            is_field_needs_processing = field_needs_processing(conn, entry);
        } else if (found_eol) {
            cstate->cur_lineno++;
            cstate->fieldno = 0;
            is_field_needs_processing = field_needs_processing(conn, entry);
        } else { /* if (!found_delim && !found_eol) */
            break;
        }
    }
    free(data);
    data = NULL;

    if (written) {
        if (!append_buffer(buffer, res_length, written, "\0", 1)) {
            return -1;
        }
        --written;
    }

    return written;
}


/* Send text representation of one attribute, with conversion and escaping */
static void dumpsofar(const char *start, char *ptr, char *&quoted_text, size_t *quoted_text_size,
    size_t *quoted_text_allocated)
{
    if (ptr <= start) {
        return;
    }

    static const int QUOTED_TEXT_ALLOC_BUFFER = 64;
    size_t size = ptr - start;
    if (*quoted_text_size + size >= *quoted_text_allocated) {
        quoted_text = (char *)libpq_realloc(quoted_text, *quoted_text_allocated,
            *quoted_text_allocated + QUOTED_TEXT_ALLOC_BUFFER);
        if (quoted_text == NULL) {
            return;
        }
        check_memset_s(
            memset_s(quoted_text + *quoted_text_allocated, QUOTED_TEXT_ALLOC_BUFFER, 0, QUOTED_TEXT_ALLOC_BUFFER));
        *quoted_text_allocated += QUOTED_TEXT_ALLOC_BUFFER;
    }
    check_memcpy_s(memcpy_s(quoted_text + *quoted_text_size, size, start, size));
    *quoted_text_size += size;
}

static bool csv_need_quote(PreparedStatement *entry, char *string, bool use_quote) 
{
    CopyStateData *cstate = entry->copy_state;
    bool single_attr = entry->original_data_types_oids_size == 1;

    char c;
    char delimc = cstate->delim;
    char quotec = cstate->quote;

    /* force quoting if it matches null_print (before conversion!) */
    if (!use_quote && strcmp(string, cstate->null_print) == 0) {
        use_quote = true;
    }

    /* Make a preliminary pass to discover if it needs quoting */
    if (!use_quote) {
        /*
         * Because '\.' can be a data value, quote it if it appears alone on a
         * line so it is not interpreted as the end-of-data marker.
         */
        if (single_attr && strcmp(string, "\\.") == 0) {
            use_quote = true;
        } else {
            char *tptr = string;

            while ((c = *tptr) != '\0') {
                if (c == delimc || c == quotec || c == '\n' || c == '\r') {
                    use_quote = true;
                    break;
                }
                if (IS_HIGHBIT_SET(c) && cstate->encoding_embeds_ascii) {
                    tptr += pg_encoding_mblen(cstate->file_encoding, tptr);
                } else {
                    tptr++;
                }
            }
        }
    }
    return use_quote;
}

static char *escape_and_quote_csv(PGconn *conn, PreparedStatement *entry, char *string, bool use_quote,
    size_t *quoted_text_size)
{
    if (!conn || !entry || !string || !quoted_text_size) {
        Assert(false);
        return NULL;
    }
    static const int QUOTED_TEXT_ALLOC_BUFFER = 64;
    *quoted_text_size = 0;
    size_t quoted_text_allocated = strlen(string) + QUOTED_TEXT_ALLOC_BUFFER;
    char *quoted_text = (char *)malloc(quoted_text_allocated);
    if (quoted_text == NULL) {
        return NULL;
    } else {
        check_memset_s(memset_s(quoted_text, quoted_text_allocated, 0, quoted_text_allocated));
    }

    CopyStateData *cstate = entry->copy_state;
    use_quote = csv_need_quote(entry, string, use_quote);
    
    char *ptr = NULL;
    char *start = NULL;
    char c;
    char quotec = cstate->quote;
    char escapec = cstate->escape;

    ptr = string;

    if (use_quote) {
        if (*quoted_text_size + 1 >= quoted_text_allocated) {
            quoted_text = (char *)libpq_realloc(quoted_text, quoted_text_allocated,
                quoted_text_allocated + QUOTED_TEXT_ALLOC_BUFFER);
            if (quoted_text == NULL) {
                return NULL;
            }
            check_memset_s(
                memset_s(quoted_text + quoted_text_allocated, QUOTED_TEXT_ALLOC_BUFFER, 0, QUOTED_TEXT_ALLOC_BUFFER));
            quoted_text_allocated += QUOTED_TEXT_ALLOC_BUFFER;
        }
        quoted_text[*quoted_text_size] = quotec;
        *quoted_text_size += 1;

        /* We adopt the same optimization strategy as in CopyAttributeOutText */
        start = ptr;
        while ((c = *ptr) != '\0') {
            if (c == quotec || c == escapec) {
                dumpsofar(start, ptr, quoted_text, quoted_text_size, &quoted_text_allocated);
                if (*quoted_text_size + 1 >= quoted_text_allocated) {
                    quoted_text = (char *)libpq_realloc(quoted_text, quoted_text_allocated,
                        quoted_text_allocated + QUOTED_TEXT_ALLOC_BUFFER);
                    if (quoted_text == NULL) {
                        return NULL;
                    }
                    check_memset_s(memset_s(quoted_text + quoted_text_allocated, QUOTED_TEXT_ALLOC_BUFFER, 0,
                        QUOTED_TEXT_ALLOC_BUFFER));
                    quoted_text_allocated += QUOTED_TEXT_ALLOC_BUFFER;
                }
                quoted_text[*quoted_text_size] = escapec;
                *quoted_text_size += 1;
                start = ptr; /* we include char in next run */
            }
            if (IS_HIGHBIT_SET(c) && cstate->encoding_embeds_ascii) {
                ptr += pg_encoding_mblen(cstate->file_encoding, ptr);
            } else {
                ptr++;
            }
        }
        dumpsofar(start, ptr, quoted_text, quoted_text_size, &quoted_text_allocated);

        if (*quoted_text_size + 1 >= quoted_text_allocated) {
            quoted_text = (char *)libpq_realloc(quoted_text, quoted_text_allocated,
                quoted_text_allocated + QUOTED_TEXT_ALLOC_BUFFER);
            if (quoted_text == NULL) {
                return NULL;
            }
            check_memset_s(
                memset_s(quoted_text + quoted_text_allocated, QUOTED_TEXT_ALLOC_BUFFER, 0, QUOTED_TEXT_ALLOC_BUFFER));
            quoted_text_allocated += QUOTED_TEXT_ALLOC_BUFFER;
        }
        quoted_text[*quoted_text_size] = quotec;
        *quoted_text_size += 1;
    } else {
        /* If it doesn't need quoting, we can just dump it as-is */
        dumpsofar(ptr, ptr + strlen(ptr), quoted_text, quoted_text_size, &quoted_text_allocated);
    }

    return quoted_text;
}

static bool deal_ctl_char(char *c, char delimc, char **ptr)
{
    bool is_continue = false;
    switch (c[0]) {
        case '\b':
            c[0] = 'b';
            break;
        case '\f':
            c[0] = 'f';
            break;
        case '\n':
            c[0] = 'n';
            break;
        case '\r':
            c[0] = 'r';
            break;
        case '\t':
            c[0] = 't';
            break;
        case '\v':
            c[0] = 'v';
            break;
        default:
            /* If it's the delimiter, must backslash it */
            if (c[0] == delimc) {
                break;
            }
            /* All ASCII control chars are length 1 */
            ptr++;
            is_continue = true; /* fall to end of loop */
    }
    return is_continue;
}

static char *escape_and_quote_txt(PGconn *conn, PreparedStatement *entry, char *string, size_t *quoted_text_size)
{
    if (!conn || !entry || !string || !quoted_text_size) {
        return NULL;
    }

    static const int QUOTED_TEXT_ALLOC_BUFFER = 64;
    *quoted_text_size = 0;
    size_t quoted_text_allocated = strlen(string) + QUOTED_TEXT_ALLOC_BUFFER;
    char *quoted_text = (char *)malloc(quoted_text_allocated);
    if (quoted_text == NULL) {
        return NULL;
    }
    check_memset_s(memset_s(quoted_text, quoted_text_allocated, 0, quoted_text_allocated));

    CopyStateData *cstate = entry->copy_state;

    char *ptr = string;
    char *start;
    char c;
    char delimc = cstate->delim;

    /*
     * We have to grovel through the string searching for control characters
     * and instances of the delimiter character.  In most cases, though, these
     * are infrequent.  To avoid overhead from calling CopySendData once per
     * character, we dump out all characters between escaped characters in a
     * single call.  The loop invariant is that the data from "start" to "ptr"
     * can be sent literally, but hasn't yet been.
     *
     * We can skip pg_encoding_mblen() overhead when encoding is safe, because
     * in valid backend encodings, extra bytes of a multibyte character never
     * look like ASCII.  This loop is sufficiently performance-critical that
     * it's worth making two copies of it to get the IS_HIGHBIT_SET() test out
     * of the normal safe-encoding path.
     */
    start = ptr;
    while ((c = *ptr) != '\0') {
        if ((unsigned char)c < (unsigned char)0x20) {
            /*
             * \r and \n must be escaped, the others are traditional. We
             * prefer to dump these using the C-like notation, rather than
             * a backslash and the literal character, because it makes the
             * dump file a bit more proof against Microsoftish data
             * mangling.
             */
            if (deal_ctl_char(&c, delimc, &ptr)) {
                continue;
            } 
            /* if we get here, we need to convert the control char */
            dumpsofar(start, ptr, quoted_text, quoted_text_size, &quoted_text_allocated);
            if (*quoted_text_size + 2 >= quoted_text_allocated) {
                quoted_text = (char *)libpq_realloc(quoted_text, quoted_text_allocated,
                    quoted_text_allocated + QUOTED_TEXT_ALLOC_BUFFER);
                if (quoted_text == NULL) {
                    return NULL;
                }
                check_memset_s(memset_s(quoted_text + quoted_text_allocated, QUOTED_TEXT_ALLOC_BUFFER, 0,
                    QUOTED_TEXT_ALLOC_BUFFER));
                quoted_text_allocated += QUOTED_TEXT_ALLOC_BUFFER;
            }
            quoted_text[*quoted_text_size] = '\\';
            quoted_text[*quoted_text_size + 1] = c;
            *quoted_text_size += 2;
            start = ++ptr; /* do not include char in next run */
        } else if (c == '\\' || c == delimc) {
            dumpsofar(start, ptr, quoted_text, quoted_text_size, &quoted_text_allocated);
            if (*quoted_text_size + 1 >= quoted_text_allocated) {
                quoted_text = (char *)libpq_realloc(quoted_text, quoted_text_allocated,
                    quoted_text_allocated + QUOTED_TEXT_ALLOC_BUFFER);
                if (quoted_text == NULL) {
                    return NULL;
                }
                check_memset_s(memset_s(quoted_text + quoted_text_allocated, QUOTED_TEXT_ALLOC_BUFFER, 0,
                    QUOTED_TEXT_ALLOC_BUFFER));
                quoted_text_allocated += QUOTED_TEXT_ALLOC_BUFFER;
            }
            quoted_text[*quoted_text_size] = '\\';
            *quoted_text_size += 1;
            start = ptr++; /* we include char in next run */
        } else if (cstate->encoding_embeds_ascii && IS_HIGHBIT_SET(c)) {
            ptr += pg_encoding_mblen(cstate->file_encoding, ptr);
        } else {
            ptr++;
        }
    }

    dumpsofar(start, ptr, quoted_text, quoted_text_size, &quoted_text_allocated);
    return quoted_text;
}

static char *escape_and_quote(PGconn *conn, PreparedStatement *entry, unsigned char *plaintext, bool is_quoted,
    size_t *quoted_text_size)
{
    CopyStateData *cstate = NULL;
    Assert(entry != NULL);
    if (entry) {
        cstate = entry->copy_state;
    }
    if (cstate && cstate->csv_mode) {
        return escape_and_quote_csv(conn, entry, (char *)plaintext, is_quoted, quoted_text_size);
    } else {
        return escape_and_quote_txt(conn, entry, (char *)plaintext, quoted_text_size);
    }
}

static bool process_and_replace(PGconn *conn, PreparedStatement *entry, const char *deprocessed_data,
    size_t deprocessed_data_size, bool is_quoted, char **buffer, int &written, int &res_length)
{
    bool ret = false;
    const ICachedColumn *cached_column;
    if (!entry || !entry->cached_copy_columns) {
        Assert(false);
        return false;
    }

    bool is_found = false;
    if (!entry->copy_state->is_attlist_null) {
        cached_column = entry->cached_copy_columns->at(entry->copy_state->fieldno);
        if (cached_column) {
            is_found = true;
        }
    } else {
        size_t cached_columns_size = entry->cached_copy_columns->size();
        for (size_t i = 0; i < cached_columns_size; ++i) {
            cached_column = entry->cached_copy_columns->at(i);
            if (!cached_column) {
                continue;
            }
            if (entry->copy_state->fieldno == (int)cached_column->get_col_idx() - 1) {
                is_found = true;
                break;
            }
        }
    }

    if (!is_found) {
        Assert(false);
        return false;
    }

    RawValue rawValue(conn);
    rawValue.set_data((const unsigned char *)deprocessed_data, deprocessed_data_size);
    rawValue.m_is_param = true;
    /* process data */
    char err_msg[1024];
    err_msg[0] = '\0';
    if (!rawValue.process(cached_column, err_msg)) {
        if (strlen(err_msg) == 0) {
            printfPQExpBuffer(&conn->errorMessage,
                libpq_gettext("ERROR(CLIENT): failed to process data of processed column"));
        } else {
            printfPQExpBuffer(&conn->errorMessage, libpq_gettext("ERROR(CLIENT): %s\n"), err_msg);
        }
        return false;
    }

    size_t escaped_size = 0;
    char *escaped = escape_and_quote(conn, entry, rawValue.m_processed_data, is_quoted, &escaped_size);
    ret = append_buffer(buffer, res_length, written, escaped, escaped_size);
    libpq_free(escaped);
    return ret;
}

static bool deprocess_and_replace(PGconn *conn, PreparedStatement *entry, const char *processed_bytea,
    size_t processed_bytea_size, bool is_quoted, char **buffer, int &written, int &res_length)
{
    bool ret = false;
    if (!entry) {
        Assert(false);
        return false;
    }

    unsigned char *plaintext = NULL;
    size_t plaintext_len;
    ProcessStatus process_status = ONLY_VALUE;
    if (ValuesProcessor::deprocess_value(conn, (const unsigned char *)processed_bytea, processed_bytea_size,
        entry->original_data_types_oids[entry->copy_state->fieldno], 0, &plaintext, plaintext_len, process_status) == 
        DEC_DATA_SUCCEED) {
        size_t escaped_size = 0;
        char *escaped = escape_and_quote(conn, entry, plaintext, is_quoted, &escaped_size);
        ret = append_buffer(buffer, res_length, written, escaped, escaped_size);
        libpq_free(escaped);
        free(plaintext);
    } else {
        printfPQExpBuffer(&conn->errorMessage,
            libpq_gettext("ERROR(CLIENT): failed to deprocess data of processed column"));
        conn->client_logic->isInvalidOperationOnColumn = true; /* FAILED TO READ */
        checkRefreshCacheOnError(conn);
    }

    return ret;
}

/* Extract a string value (otherwise uninterpreted) from a DefElem. */
char *fe_def_get_string(DefElem *def)
{
    if (def->arg == NULL)
        return "";
    switch (nodeTag(def->arg)) {
        case T_Integer: {
            char *str = (char *)malloc(INTEGER_SIZE);
            if (str == NULL) {
                return NULL;
            } else {
                check_memset_s(memset_s(str, INTEGER_SIZE, 0, INTEGER_SIZE));
            }
            errno_t ss_rc = EOK;
            ss_rc = snprintf_s(str, INTEGER_SIZE, INTEGER_SIZE - 1, "%ld", (long)intVal(def->arg));
            securec_check_ss_c(ss_rc, "\0", "\0");
            return str;
        }
        case T_Float:

            /*
             * T_Float values are kept in string form, so this type cheat
             * works (and doesn't risk losing precision)
             */
            return strVal(def->arg);
        case T_String:
            return strVal(def->arg);
        case T_TypeName:
            printf("Oh Fuck TypeName\n");
            return "";
        case T_List:
            printf("Oh Fuck List\n");
            return "";
        case T_A_Star: {
            char *str = (char *)malloc(2);
            if (str == NULL) {
                return NULL;
            }
            str[0] = '*';
            str[1] = 0;
            return str;
        }
        default:
            return "";
    }
    return NULL; /* keep compiler quiet */
}

/* Extract a boolean value from a DefElem. */
bool fe_def_get_boolean(DefElem *def)
{
    /*
     * If no parameter given, assume "true" is meant.
     */
    if (def->arg == NULL) {
        return true;
    }

    /* Allow 0, 1, "true", "false", "on", "off" */
    switch (nodeTag(def->arg)) {
        case T_Integer:
            switch (intVal(def->arg)) {
                case 0:
                    return false;
                case 1:
                    return true;
                default:
                    /* otherwise, error out below */
                    break;
            }
            break;
        default: {
            char *sval = fe_def_get_string(def);
            if (sval == NULL) {
                return false;
            }
            /*
             * The set of strings accepted here should match up with the
             * grammar's opt_boolean production.
             */
            if (pg_strcasecmp(sval, "true") == 0) {
                return true;
            }
            if (pg_strcasecmp(sval, "false") == 0) {
                return false;
            }
            if (pg_strcasecmp(sval, "on") == 0) {
                return true;
            }
            if (pg_strcasecmp(sval, "off") == 0) {
                return false;
            }
        } break;
    }
    return false; /* keep compiler quiet */
}


/*
 * Process the statement option list for COPY.
 *
 * Scan the options list (a list of DefElem) and transpose the information
 * into cstate, applying appropriate error checking.
 *
 * cstate is assumed to be filled with zeroes initially.
 *
 * This is exported so that external users of the COPY API can sanity-check
 * a list of options.  In that usage, cstate should be passed as NULL
 * (since external users don't know sizeof(CopyStateData)) and the collected
 * data is just leaked until CurrentMemoryContext is reset.
 *
 * Note that additional checking, such as whether column names listed in FORCE
 * QUOTE actually exist, has to be applied later.  This just checks for
 * self-consistency of the options list.
 */
bool fe_process_copy_options(CopyStateData *cstate, bool is_from, List *options)
{
    if (!cstate) {
        Assert(false);
        return false;
    }
    bool format_specified = false;
    ListCell *option = NULL;
    cstate->file_encoding = -1;

    /* Extract options from the statement node tree */
    foreach (option, options) {
        DefElem *defel = (DefElem *)lfirst(option);

        if (strcmp(defel->defname, "format") == 0) {
            char *fmt = fe_def_get_string(defel);

            if (format_specified) {
                return false;
            }
            format_specified = true;
            if (strcmp(fmt, "text") == 0) {
                ; /* default format */
            } else if (strcmp(fmt, "csv") == 0) {
                cstate->csv_mode = true;
            } else if (strcmp(fmt, "binary") == 0) {
                cstate->binary = true;
            } else {
                return false;
            }
        } else if (strcmp(defel->defname, "oids") == 0) {
            if (cstate->oids) {
                return false;
            }
            cstate->oids = fe_def_get_boolean(defel);
        } else if (strcmp(defel->defname, "delimiter") == 0) {
            if (cstate->delim) {
                return false;
            }
            char *delim = fe_def_get_string(defel);
            if (delim == NULL) {
                return false;
            }
            cstate->delim = delim[0];
        } else if (strcmp(defel->defname, "null") == 0) {
            if (cstate->null_print) {
                return false;
            }
            cstate->null_print = fe_def_get_string(defel);
        } else if (strcmp(defel->defname, "header") == 0) {
            if (cstate->header_line) {
                return false;
            }
            cstate->header_line = fe_def_get_boolean(defel);
        } else if (strcmp(defel->defname, "quote") == 0) {
            if (cstate->quote) {
                return false;
            }
            char *quote = fe_def_get_string(defel);
            if (quote == NULL) {
                return false;
            }
            cstate->quote = quote[0];
        } else if (strcmp(defel->defname, "escape") == 0) {
            if (cstate->escape) {
                return false;
            }
            char *escape = fe_def_get_string(defel);
            if (escape == NULL) {
                return false;
            }
            cstate->escape = escape[0];
        } else if (strcmp(defel->defname, "force_quote") == 0) {
            if (cstate->force_quote || cstate->force_quote_all) {
                return false;
            }
            if (defel->arg && IsA(defel->arg, A_Star)) {
                cstate->force_quote_all = true;
            } else if (defel->arg && IsA(defel->arg, List)) {
                cstate->force_quote = (List *)defel->arg;
            } else {
                return false;
            }
        } else if (strcmp(defel->defname, "force_not_null") == 0) {
            if (cstate->force_notnull) {
                return false;
            }
            if (defel->arg && IsA(defel->arg, List)) {
                cstate->force_notnull = (List *)defel->arg;
            } else {
                return false;
            }
        } else if (strcmp(defel->defname, "encoding") == 0) {
            if (cstate->file_encoding >= 0) {
                return false;
            }
            cstate->file_encoding = pg_char_to_encoding(fe_def_get_string(defel));
            if (cstate->file_encoding < 0) {
                return false;
            }
        } else {
            return false;
        }
    }

    /*
     * Check for incompatible options (must do these two before inserting
     * defaults)
     */
    if (cstate->binary && cstate->delim) {
        return false;
    }

    if (cstate->binary && cstate->null_print) {
        return false;
    }

    /* Set defaults for omitted options */
    if (!cstate->delim) {
        cstate->delim = cstate->csv_mode ? ',' : '\t';
    }

    if (!cstate->null_print) {
        cstate->null_print = cstate->csv_mode ? "" : "\\N";
    }
    cstate->null_print_len = strlen(cstate->null_print);

    if (cstate->csv_mode) {
        if (!cstate->quote) {
            cstate->quote = '\"';
        }
        if (!cstate->escape) {
            cstate->escape = cstate->quote;
        }
    }

    /* Disallow end-of-line characters */
    if (cstate->delim == '\r' || cstate->delim == '\n') {
        return false;
    }

    if (strchr(cstate->null_print, '\r') != NULL || strchr(cstate->null_print, '\n') != NULL) {
        return false;
    }

    /*
     * Disallow unsafe delimiter characters in non-CSV mode.  We can't allow
     * backslash because it would be ambiguous.  We can't allow the other
     * cases because data characters matching the delimiter must be
     * backslashed, and certain backslash combinations are interpreted
     * non-literally by COPY IN.  Disallowing all lower case ASCII letters is
     * more than strictly necessary, but seems best for consistency and
     * future-proofing.  Likewise we disallow all digits though only octal
     * digits are actually dangerous.
     */
    if (!cstate->csv_mode && strchr("\\.abcdefghijklmnopqrstuvwxyz0123456789", cstate->delim) != NULL) {
        return false;
    }

    /* Check header */
    if (!cstate->csv_mode && cstate->header_line) {
        return false;
    }

    /* Check quote */
    if (!cstate->csv_mode && cstate->quote != '\0') {
        return false;
    }

    if (cstate->csv_mode && cstate->delim == cstate->quote) {
        return false;
    }

    /* Check escape */
    if (!cstate->csv_mode && cstate->escape != '\0') {
        return false;
    }

    /* Check force_quote */
    if (!cstate->csv_mode && (cstate->force_quote || cstate->force_quote_all)) {
        return false;
    }
    if ((cstate->force_quote || cstate->force_quote_all) && is_from) {
        return false;
    }

    /* Check force_notnull */
    if (!cstate->csv_mode && cstate->force_notnull != NIL) {
        return false;
    }
    if (cstate->force_notnull != NIL && !is_from) {
        return false;
    }

    /* Don't allow the delimiter to appear in the null string. */
    if (strchr(cstate->null_print, cstate->delim) != NULL) {
        return false;
    }

    /* Don't allow the CSV quote char to appear in the null string. */
    if (cstate->csv_mode && strchr(cstate->null_print, cstate->quote) != NULL) {
        return false;
    }
    return true;
}

/*
 * Common setup routines used by begin_copy_from and begin_copy_to.
 *
 * Iff <binary>, unload or reload in the binary format, as opposed to the
 * more wasteful but more robust and portable text format.
 *
 * Iff <oids>, unload or reload the format that includes OID information.
 * On input, we accept OIDs whether or not the table has an OID column,
 * but silently drop them if it does not.  On output, we report an error
 * if the user asks for OIDs in a table that has none (not providing an
 * OID column might seem friendlier, but could seriously confuse programs).
 *
 * If in the text format, delimit columns with delimiter <delim> and print
 * NULL values as <null_print>.
 */
static CopyStateData *begin_copy(bool is_from, bool is_rel, Node *raw_query, const char *query_string,
    List *attnamelist, List *options)
{
    /* Allocate workspace and zero all fields */
    CopyStateData *cstate = new (std::nothrow) CopyStateData();
    if (cstate == NULL) {
        fprintf(stderr, "failed to new CopyStateData object\n");
        exit(EXIT_FAILURE);
    }
    copy_state_data_init(cstate);
    /* Extract options from the statement node tree */
    fe_process_copy_options(cstate, is_from, options);
    cstate->is_attlist_null = (attnamelist == NIL);

    /* Process the source/target relation or query */
    cstate->is_rel = is_rel;
    bool is_return_empty = false;
    if (is_rel) {
        if (raw_query) {
            is_return_empty = true;
        }
    } else {
        if (is_from) {
            is_return_empty = true;
        }

        /* Don't allow COPY w/ OIDs from a select */
        if (cstate->oids) {
            is_return_empty = true;
        }
    }

    if (is_return_empty) {
        delete cstate;
        cstate = new (std::nothrow) CopyStateData();
        if (cstate == NULL) {
            fprintf(stderr, "failed to new CopyStateData object\n");
            exit(EXIT_FAILURE);
        }
        return cstate;
    }

    /* Use client encoding when ENCODING option is not specified. */
    if (cstate->file_encoding < 0) {
        cstate->file_encoding = pg_get_client_encoding();
    }

    /*
     * Set up encoding conversion info.  Even if the file and server encodings
     * are the same, we must apply pg_any_to_server() to validate data in
     * multibyte encodings.
     */
    cstate->need_transcoding =
        (cstate->file_encoding != GetDatabaseEncoding() || pg_database_encoding_max_length() > 1);
    /* See Multibyte encoding comment above */
    cstate->encoding_embeds_ascii = PG_ENCODING_IS_CLIENT_ONLY(cstate->file_encoding);

    cstate->copy_dest = COPY_FILE; /* default */

    return cstate;
}

/* Setup CopyState to read tuples from a table or a query for COPY TO. */
CopyStateData *begin_copy_to(bool is_rel, Node *query, const char *query_string, const char *filename,
    List *attnamelist, List *options)
{
    return begin_copy(false, is_rel, query, query_string, attnamelist, options);
}


/*
 * Setup to read tuples from a file for COPY FROM.
 *
 * 'rel': Used as a template for the tuples
 * 'filename': Name of server-local file to read
 * 'attnamelist': List of char *, columns to include. NIL selects all cols.
 * 'options': List of DefElem. See copy_opt_item in gram.y for selections.
 *
 * Returns a CopyState, to be passed to NextCopyFrom and related functions.
 */
CopyStateData *begin_copy_from(bool is_rel, const char *filename, List *attnamelist, List *options)
{
    CopyStateData *cstate = begin_copy(true, is_rel, NULL, NULL, attnamelist, options);

    /* Initialize state variables */
    cstate->fe_eof = false;
    cstate->eol_type = EOL_UNKNOWN;
    cstate->cur_lineno = 0;

    if (!cstate->binary) {
        /* must rely on user to tell us... */
        cstate->file_has_oids = cstate->oids;
    }

    return cstate;
}

void delete_copy_state(CopyStateData *cstate)
{
    if (cstate != NULL) {
        delete cstate;
        cstate = NULL;
    }
}

#endif /* HAVE_CE */
