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

static bool is_type_simple(Oid elem_type)
{
    switch (elem_type) {
        case OIDOID:
        case INT4OID:
        case INT8OID:
        case INT2OID:
        case INT1OID:
            return true;
        default:
            return false;
    }
    return false; /* passinate compiler */
}

/*
 * Converts bynary array from qiuery into internal array
 * @IN - input_array
 * @OUT - output_array in type of int[] for fixed len types or char*[] for varailbe length
 * For our needs the onlt Oid[] and char*[] are handled
 */
int parseBinaryArray(char* input_array, void** output_array)
{
    /* claculate binary size - one dimensinal array */
    char* pos = input_array;
    int ndim = 0;
    int n_elements = 0;
    ArrayHead* ah = (ArrayHead*)pos;
    bool is_simple_type = false;
    /* fill outpu */
    ndim = ntohl(ah->ndim);
    if (!ndim) { /* array is empty */
        return 0;
    }
    int dataoffset = ntohl(ah->dataoffset);
    Oid elem_type = ntohl(ah->elemtype); //
    uint32 elem_size = 0;
    char* new_array = NULL;

    pos += sizeof(ArrayHead);
    for (int i = 0; i < ndim; i++) {
        n_elements += ntohl(((DimHeader*)pos)->numElements);
        pos += sizeof(DimHeader);
    }
    if (!n_elements) {
        return 0;
    }
    pos += dataoffset;
    is_simple_type = is_type_simple(elem_type);
    if (is_simple_type) {
        elem_size = ntohl(*(uint32*)pos);
        new_array = (char*)malloc(elem_size * n_elements);
    } else {
        new_array = (char*)calloc(n_elements, sizeof(char*));
    }
    if (new_array == NULL) {
        return 0;
    }
    /*
     * earch array element consists of 2 parts element_size : element_val
     * element_size is 4 bytes long
     */
    for (int i = 0; i < n_elements; i++) {
        if (is_simple_type) {
            pos += sizeof(elem_size);
            switch (elem_size) {
                case (sizeof(uint64)):
                    *((uint64*)new_array + i) = NTOHLL(*(uint64*)pos);
                    break;
                case (sizeof(uint32)):
                    *((uint32*)new_array + i) = ntohl(*(uint32*)pos);
                    break;
                case (sizeof(uint16)):
                    *((uint16*)new_array + i) = ntohs(*(uint16*)pos);
                    break;
                case 1:
                    *((char*)new_array + i) = *(char*)pos;
                default:
                    fprintf(stderr, "array of simple type %u is not handled\n", elem_type);
                    Assert(false);
                    break;
            }
        } else {
            elem_size = ntohl(*(uint32*)pos);
            *((char**)new_array + i) = (char*)calloc(elem_size + 1, sizeof(char));
            if (!*((char**)new_array + i)) {
                if (new_array != NULL) {
                    for (int j = 0; j < i; j++) {
                        libpq_free(*((char**)new_array + j));
                    }
                    libpq_free(new_array);
                }
                fprintf(stderr, "error allocating memory for array\n");
                Assert(false);
                return 0;
            }
            pos += sizeof(elem_size);
            check_memcpy_s(memcpy_s(*((char**)new_array + i), elem_size + 1, pos, elem_size));
        }
        pos += elem_size;
    }
    *output_array = new_array;
    return n_elements;
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