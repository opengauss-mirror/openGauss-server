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
 * numeric.h
 *
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_fmt/numeric.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef NUMERIC_H
#define NUMERIC_H

#include "postgres_fe.h"
#include <string>

typedef int16 NumericDigit;

#define NUMERIC_SIGN_MASK 0xC000
#define NUMERIC_POS 0x0000
#define NUMERIC_NEG 0x4000
#define NUMERIC_SHORT 0x8000
#define NUMERIC_NAN 0xC000
#define NBASE 10000
#define HALF_NBASE 5000
#define DEC_DIGITS 4 /* decimal digits per NBASE digit */
#define NUMERIC_MAX_PRECISION 1000
#define NUMERIC_MAX_RESULT_SCALE (NUMERIC_MAX_PRECISION * 2)

typedef struct NumericVar {
    int ndigits;          /* # of digits in digits[] - can be 0! */
    int weight;           /* weight of first digit */
    int sign;             /* NUMERIC_POS, NUMERIC_NEG, or NUMERIC_NAN */
    int dscale;           /* display scale */
    NumericDigit *buf;    /* start of palloc'd space for digits[] */
    NumericDigit *digits; /* base-NBASE digits */
} NumericVar;
struct NumericShort {
    uint16 n_header;        /* Sign + display scale + weight */
    NumericDigit n_data[1]; /* Digits */
};

struct NumericLong {
    uint16 n_sign_dscale;   /* Sign + display scale */
    int16 n_weight;         /* Weight of 1st digit  */
    NumericDigit n_data[1]; /* Digits */
};

/*
 * NumericBi is used for big integer(bi64 or bi128)
 * n_header stores mark bits and scale of big integer, first 4 bits to
 * distinguish bi64 and bi128, next 4 bits are not used, the last 8 bits
 * store the scale of bit integer, scale value is between 0 and 38.
 * n_data store big integer value(int64 or int128)
 *
 */
struct NumericBi {
    uint16 n_header;
    uint8 n_data[1];
};

/*
 * Add NumericBi struct to NumericChoice
 */
union NumericChoice {
    uint16 n_header;             /* Header word */
    struct NumericLong n_long;   /* Long form (4-byte header) */
    struct NumericShort n_short; /* Short form (2-byte header) */
    struct NumericBi n_bi;       /* Short form (2-byte header) */
};

struct NumericData {
    int32 vl_len_;              /* varlena header (do not touch directly!) */
    union NumericChoice choice; /* choice of format */
};

/* The actual contents of Numeric are private to numeric.c */
struct NumericData;
typedef struct NumericData *Numeric;
typedef struct pg_conn PGconn;

unsigned char *scan_numeric(const PGconn* conn, const char *num, int atttypmod, size_t *binary_size, char *err_msg);
bool numerictoa(NumericData* num, char *ascii, size_t max_size);
bool apply_typmod(NumericVar *var, int32 typmod, char *err_msg);
unsigned char *make_result(NumericVar *var, size_t *binary_size, char *err_msg);

#endif
