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
 * client_logic_utils.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_common\client_logic_utils.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <cstdint>
#include <cstdarg>
#include "client_logic_utils.h"
#include "securec.h"
typedef uintptr_t Datum;
#include "nodes/primnodes.h"
#include "libpq-int.h"
#define HTONLL(x) ((1 == htonl(1)) ? (x) : ((((uint64_t)htonl((x)&0xFFFFFFFFUL)) << 32) | htonl((uint32_t)((x) >> 32))))
#define NTOHLL(x) ((1 == ntohl(1)) ? (x) : ((((uint64_t)ntohl((x)&0xFFFFFFFFUL)) << 32) | ntohl((uint32_t)((x) >> 32))))

static const size_t MIN_SIZE_WITH_ELEMENTS = 3;

bool is_clientlogic_datatype(const Oid o)
{
    return (o == BYTEAWITHOUTORDERWITHEQUALCOLOID || o == BYTEAWITHOUTORDERCOLOID);
}
/*
 * Get the string without space
 */
char *del_blanks(char *str, const int str_len)
{
    char *source = NULL;
    char *dest = NULL;
    char *cpy_str = NULL;
    errno_t rc = 0;

    if (str == NULL || str_len <= 0) {
        return NULL;
    }

    cpy_str = (char *)malloc(str_len + 1);
    if (cpy_str == NULL) {
        return NULL;
    }
    rc = memset_s(cpy_str, str_len + 1, 0, str_len + 1);
    securec_check_c(rc, "\0", "\0");
    source = str;
    dest = cpy_str;
    while (source != NULL && isspace((int)*source)) {
        source++;
    }
    if (*source == '\0') {
        free(cpy_str);
        cpy_str = NULL;
        return NULL;
    }

    for (; *source != '\0'; source++) {
        if (!isspace((int)*source)) {
            *dest = *source;
            dest++;
        }
    }

    *dest = '\0';
    return cpy_str;
}


bool concat_col_fqdn(const char *catalogname, const char *schemaname, const char *relname, const char *colname,
    char *fqdn)
{
    if (!fqdn || !colname) {
        return false;
    }
    bool ret = concat_table_fqdn(catalogname, schemaname, relname, fqdn);
    if (ret) {
        check_strncat_s(strncat_s(fqdn, NAMEDATALEN, ".", 1));
    } else {
        fqdn[0] = '\0';
    }
    check_strncat_s(strncat_s(fqdn, NAMEDATALEN, colname, strlen(colname)));
    return true;
}

bool concat_table_fqdn(const char *catalogname, const char *schemaname, const char *relname, char *fqdn)
{
    if (!fqdn) {
        return false;
    }

    fqdn[0] = '\0';
    if (catalogname && catalogname[0] != '\0') {
        check_strncat_s(strncat_s(fqdn, NAMEDATALEN, catalogname, strlen(catalogname)));
        check_strncat_s(strncat_s(fqdn, NAMEDATALEN, ".", 1));
    } else {
        fqdn[0] = '\0';
        return false;
    }

    if (schemaname && schemaname[0] != '\0') {
        check_strncat_s(strncat_s(fqdn, NAMEDATALEN, schemaname, strlen(schemaname)));
        check_strncat_s(strncat_s(fqdn, NAMEDATALEN, ".", 1));
    } else {
        fqdn[0] = '\0';
        return false;
    }

    if (relname && relname[0] != '\0') {
        check_strncat_s(strncat_s(fqdn, NAMEDATALEN, relname, strlen(relname)));
    } else {
        fqdn[0] = '\0';
        return false;
    }

    return true;
}

void free_obj_list(ObjName *obj_list)
{
    ObjName *cur_obj = obj_list;
    ObjName *to_free = NULL;

    while (cur_obj != NULL) {
        to_free = cur_obj;
        cur_obj = cur_obj->next;
        free(to_free);
        to_free = NULL;
    }

    obj_list = NULL;
}

ObjName *obj_list_append(ObjName *obj_list, const char *new_obj_name)
{
    ObjName *new_obj = NULL;
    ObjName *last_obj = obj_list;
    errno_t rc = 0;

    if (new_obj_name == NULL || strlen(new_obj_name) >= OBJ_NAME_BUF_LEN) {
        free_obj_list(obj_list);
        return NULL;
    }
    
    new_obj = (ObjName *)malloc(sizeof(ObjName));
    if (new_obj == NULL) {
        free_obj_list(obj_list);
        return NULL;
    }

    rc = strcpy_s(new_obj->obj_name, sizeof(new_obj->obj_name), new_obj_name);
    securec_check_c(rc, "", "");
    new_obj->next = NULL;

    if (obj_list == NULL) {
        return new_obj;
    }

    while (last_obj->next != NULL) {
        last_obj = last_obj->next;
    }

    last_obj->next = new_obj;
    return obj_list;
}
/**
 * helper function to parse a string array of Oids and types.
 * It searches for possible separator with are ',' or ' ' space and count its occurrences
 * @param input input array
 * @return the numbers of seperators in the input string
 */
size_t count_sep_in_str(const char *input)
{
    size_t result = 0;
    if (input == NULL || strlen(input) == 0) {
        return result;
    }
    for (size_t index = 0; index < strlen(input); ++index) {
        if (input[index] == ',' || input[index] == ' ') {
            ++result;
        }
    }
    return result;
}

/**
 * Parses a char array coming from the database server when loading the cache
 * It is in the form of {elem,elem,elem ...} or "elem elem elem elem"
 * Note that this method allocates the memory for the items_out parameters and it is up to the caller to free it
 * @param[in] input the input array string
 * @param[out] items_out vector of items allocated by this method
 * @return the numbers of items in items_out
 */
size_t parse_string_array(PGconn* const conn, const char *input, char ***items_out)
{
    *items_out = NULL;
    char **items = NULL;
    size_t output_length = 0;

    if (input == NULL || strlen(input) == 0) {
        /* there are 2 characters for opening and closing brakets {item1,item2....itemn} */
        return output_length;
    }
    size_t start_offset = 0;
    if (input[0] == '{') {
        start_offset = 1;
        if (strlen(input) < MIN_SIZE_WITH_ELEMENTS) {
            return output_length;
        }
    }
    size_t count_of_column = count_sep_in_str(input);
    output_length = count_of_column + 1;
    size_t elem_index = 0;
    items = (char **)malloc(output_length * sizeof(char*));
    *items_out = items;
    if (items == NULL) {
        printfPQExpBuffer(&conn->errorMessage, libpq_gettext("Error: out of memory?\n"));
        output_length = 0;
        return output_length;
    }
    check_memset_s(memset_s(items, output_length * sizeof(char *), 0, output_length * sizeof(char*)));
    size_t begin_index = start_offset;
    size_t last_index = start_offset;
    for (size_t index = start_offset; index < strlen(input); ++index) {
        // Ignoring the bracket if exists {item1,item2....itemn}
        last_index = index;
        if (input[index] == ',' || input[index] == ' ' || input[index] == '}') {
            if (elem_index < output_length) {
                size_t item_len = index - begin_index;
                items[elem_index] = (char *)malloc((item_len + 1) * sizeof(char));
                check_strncpy_s(strncpy_s(items[elem_index], item_len + 1, input + begin_index, item_len));
                items[elem_index][index - begin_index] = 0;
            } else {
                printfPQExpBuffer(&conn->errorMessage,
                    libpq_gettext("Error: index out of bound on parse_string_array %s\n"), input);
            }
            begin_index = index + 1;
            ++elem_index;
        }
    }
    if (start_offset == 0) { // Handle the last item
        if (elem_index < output_length) {
            size_t item_len = last_index + 1 - begin_index;
            items[elem_index] = (char *)malloc((item_len + 1) * sizeof(char));
            check_strncpy_s(strncpy_s(items[elem_index], item_len + 1, input + begin_index, item_len));
            items[elem_index][item_len] = 0;
        }
    }
    return output_length;
}

/**
 * Parses a char array coming from the database server when loading the cache
 * It is in the form of {'c','i',..'o'} or "c i ... o"
 * Note that this method allocates the memory for the items_out parameters and it is up to the caller to free it
 * @param[in] input the input array string
 * @param[out] items_out vector of items allocated by this method
 * @return the numbers of items in items_out*
 */
size_t parse_char_array(PGconn* const conn, const char *input, char **items_out)
{
    *items_out = NULL;
    char **items_char = NULL;
    char *items = NULL;
    size_t output_length = parse_string_array(conn, input, &items_char);
    if (output_length > 0) {
        items = (char*)malloc(output_length * sizeof(char));
        *items_out = items;
        for (size_t index = 0; index < output_length; ++index) {
            items[index] = *(items_char[index]);
            free(items_char[index]);
        }
    }
    free(items_char);
    return output_length;
}
/**
 * Parses a oid array coming from the database server when loading the cache
 * It is in the form of {oid1,oid2,...oidn) or "oid1 oid2 ... oidn"
 * Note that this method allocates the memory for the items_out parameters and it is up to the caller to free it
 * @param[in] input the input array string
 * @param[out] items_out vector of items allocated by this method
 * @return the numbers of items in items_out*
 */
size_t parse_oid_array(PGconn* const conn, const char *input, Oid **items_out)
{
    *items_out = NULL;
    char **items_char = NULL;
    Oid *items = NULL;
    size_t output_length = parse_string_array(conn, input, &items_char);
    if (output_length > 0) {
        items = (Oid *)malloc(output_length * sizeof(Oid));
        if (items == NULL) {
            printfPQExpBuffer(&conn->errorMessage, libpq_gettext("Error: out of memory\n"));
            for (size_t index = 0; index < output_length; ++index) {
                free(items_char[index]);
            }
            free(items_char);
            output_length = 0;
            return output_length;
        }
        *items_out = items;
        for (size_t index = 0; index < output_length; ++index) {
            items[index] = (Oid)atoi(items_char[index]);
            free(items_char[index]);
        }
    }
    free(items_char);
    return output_length;
}
