/*
 * Portions Copyright (c) 2024 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
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
 * float.h
 *    Declarations for operations on float types.
 *
 * IDENTIFICATION
 *    src/include/utils/float.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef FLOAT_H
#define FLOAT_H

#include "postgres.h"
#include "knl/knl_variable.h"

#include <cmath>
#include <cstring>
#include <limits>

#include "miscadmin.h"
#include "shortest_dec.h"

/* not sure what the following should be, but better to make it over-sufficient */
#define MAXFLOATWIDTH 64
#define MAXDOUBLEWIDTH 128

template <std::size_t ascii_size>
void pg_ftoa(float num, char* ascii);

template <std::size_t ascii_size>
void pg_dtoa(float num, char* ascii);

inline bool is_req_from_jdbc()
{
    return std::strcmp(u_sess->attr.attr_common.application_name, "PostgreSQL JDBC Driver") == 0;
}

namespace detail {

/*
 * Delete 0 before decimal.
 * For Example: convert `0.123` to `.123`, or `-0.123` to `-.123`.
 */
inline void delete_leading_zero(char* ascii, const std::size_t ascii_size)
{
    const bool negative = (*ascii == '-');

    /* Skip the sign `-` if exist. */
    char* ascii_copy = negative ? ascii + 1 : ascii;

    /* Delete 0 before decimal. */
    if (*ascii_copy == '0') {
        const std::size_t remain_size = ascii_size - (negative ? 1 : 0);
        errno_t rc = memmove_s(ascii_copy, remain_size, ascii_copy + 1, remain_size - 1);
        securec_check(rc, "\0", "\0");
    }
}

}  // namespace detail

/*
 * Converts a float4 number to a string
 * using a standard output format.
 *
 * @param ascii The buffer to output conversion result.
 * @param ascii_size The size of `ascii`.
 */
template <std::size_t ascii_size>
void pg_ftoa(float num, char* ascii)
{
    errno_t rc = memset_sp(ascii, ascii_size, 0, ascii_size);
    securec_check(rc, "\0", "\0");

    if (std::isnan(num)) {
        rc = strcpy_sp(ascii, ascii_size, "NaN");
        securec_check_ss(rc, "\0", "\0");

        return;
    } else if (std::isinf(num)) {
        if (num > 0) {
            rc = strcpy_sp(ascii, ascii_size, "Infinity");
        } else {
            rc = strcpy_sp(ascii, ascii_size, "-Infinity");
        }
        securec_check_ss(rc, "\0", "\0");

        return;
    }

    if (u_sess->attr.attr_common.extra_float_digits > 0) {
        (void)float_to_shortest_decimal_buf(num, ascii);
    } else {
        int ndig = std::numeric_limits<float>::digits10 + u_sess->attr.attr_common.extra_float_digits;
        if (ndig < 1) {
            ndig = 1;
        }

        rc = snprintf_s(ascii, ascii_size, ascii_size - 1, "%.*g", ndig, num);
        securec_check_ss(rc, "\0", "\0");
    }

    if (DISPLAY_LEADING_ZERO) {
        return;
    }

    if ((num > 0 && num < 1) || (num > -1 && num < 0)) {
        detail::delete_leading_zero(ascii, ascii_size);
    }
}

/*
 * Converts float8 number to a string
 * using a standard output format
 *
 * @param ascii The buffer to output conversion result.
 * @param ascii_size The size of `ascii`.
 */
template <std::size_t ascii_size>
void pg_dtoa(double num, char* ascii)
{
    errno_t rc = memset_sp(ascii, ascii_size, 0, ascii_size);
    securec_check(rc, "\0", "\0");

    if (std::isnan(num)) {
        rc = strcpy_sp(ascii, ascii_size, "NaN");
        securec_check(rc, "\0", "\0");
        return;
    } else if (std::isinf(num)) {
        if (num > 0) {
            rc = strcpy_sp(ascii, ascii_size, "Infinity");
        } else {
            rc = strcpy_sp(ascii, ascii_size, "-Infinity");
        }
        securec_check(rc, "\0", "\0");
        return;
    }

    if (u_sess->attr.attr_common.extra_float_digits > 0) {
        (void)double_to_shortest_decimal_buf(num, ascii);
    } else {
        int ndig = std::numeric_limits<double>::digits10 + u_sess->attr.attr_common.extra_float_digits;
        if (ndig < 1) {
            ndig = 1;
        }

        rc = snprintf_s(ascii, ascii_size, ascii_size - 1, "%.*g", ndig, num);
        securec_check_ss(rc, "\0", "\0");
    }

    if (DISPLAY_LEADING_ZERO) {
        return;
    }

    if ((num > 0 && num < 1) || (num > -1 && num < 0)) {
        detail::delete_leading_zero(ascii, ascii_size);
    }
}

#endif
