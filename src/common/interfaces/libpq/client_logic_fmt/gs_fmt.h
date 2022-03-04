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
 * gs_fmt.h
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_fmt\gs_fmt.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef GS_FMT_H
#define GS_FMT_H

#include <string>
#include "postgres_fe.h"

typedef struct pg_conn PGconn;

class Format {
public:
    static unsigned char *text_to_binary(const PGconn* conn, const char *text, Oid type, int atttypmod,
        size_t *binary_size, char *err_msg);
    static char *binary_to_text(const unsigned char *binary, size_t size, Oid type, size_t *result_size);
    static unsigned char *verify_and_adjust_binary(unsigned char *binary, size_t *binary_size, Oid type,
        int atttypmod, char *err_msg);
    static unsigned char *restore_binary(const unsigned char *binary, size_t size, Oid type, size_t *binary_size,
        const char *err_msg);

private:
    static unsigned char *type_char_bin(const char *text, Oid type, int atttypmod, size_t *binary_size,
        char *err_msg);
};

#endif