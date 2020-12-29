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
