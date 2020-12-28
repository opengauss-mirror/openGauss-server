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
 * biginteger.cpp
 *
 * IDENTIFICATION
 *	  src\common\interfaces\libpq\client_logic_fmt\biginteger.cpp
 *
 * -------------------------------------------------------------------------
 */
 
#include "biginteger.h"
#define MAXBI64LEN 25
#define MAXBI128LEN 45

static const char *int64_min_str = "-9223372036854775808";
static const char *int128_min_str = "-170141183460469231731687303715884105728";
bool bi64_out(int64 data, int scale, char *buf)
{
    Assert(scale >= 0 && scale <= MAXINT64DIGIT);
    uint64 val_u64 = 0;

    errno_t rc = EOK;
    /* data == INT64_MIN */
    if (unlikely(data == (-INT64CONST(0x7FFFFFFFFFFFFFFF) - 1))) {
        /*
         * Avoid problems with the most negative integer not being representable
         * as a positive integer.
         */
        if (scale > 0) {
            int len = strlen(int64_min_str) - scale;
            rc = memcpy_s(buf, MAXBI64LEN, int64_min_str, len);
            /* check the return value of security function */
            securec_check_c(rc, "\0", "\0");
            buf[len] = '.';
            rc = memcpy_s(buf + len + 1, MAXBI64LEN - len - 1, int64_min_str + len, scale + 1);
            /* check the return value of security function */
            securec_check_c(rc, "\0", "\0");
        } else {
            rc = memcpy_s(buf, MAXBI64LEN, int64_min_str, strlen(int64_min_str) + 1);
            /* check the return value of security function */
            securec_check_c(rc, "\0", "\0");
        }
        return true;
    }

    int64 pre_val = 0;
    int64 post_val = 0;

    /* data is positive */
    if (data >= 0) {
        val_u64 = data;
        pre_val = val_u64 / (uint64)get_scale_multiplier(scale);
        post_val = val_u64 % (uint64)get_scale_multiplier(scale);
        if (likely(scale > 0)) {
            /* ignore preceding 0, eg: value is 0.1, output is '.1' */
            if (pre_val == 0 && post_val != 0) {
                rc = sprintf_s(buf, MAXBI64LEN, ".%0*ld", scale, post_val);
            } else {
                rc = sprintf_s(buf, MAXBI64LEN, "%ld.%0*ld", pre_val, scale, post_val);
            }
        } else {
            rc = sprintf_s(buf, MAXBI64LEN, "%ld", pre_val);
        }
        /* check the return value of security function */
        securec_check_ss_c(rc, "\0", "\0");
    } else {
        /* data is negative */
        val_u64 = -data;
        pre_val = val_u64 / (uint64)get_scale_multiplier(scale);
        post_val = val_u64 % (uint64)get_scale_multiplier(scale);
        if (likely(scale > 0)) {
            /* ignore preceding 0, eg: value is -0.1, output is '-.1' */
            if (pre_val == 0 && post_val != 0) {
                rc = sprintf_s(buf, MAXBI64LEN, "-.%0*ld", scale, post_val);
            } else {
                rc = sprintf_s(buf, MAXBI64LEN, "-%ld.%0*ld", pre_val, scale, post_val);
            }
        } else {
            rc = sprintf_s(buf, MAXBI64LEN, "-%ld", pre_val);
        }
        /* check the return value of security function */
        securec_check_ss_c(rc, "\0", "\0");
    }

    return true;
}
/*
 * @Description: print int128 data to string, because of current GCC doesn't provide
 * solution to print int128 data, we provide this function.
 *
 * @IN  preceding_zero: mark whether print preceding zero or not
 * @IN  data: int128 data
 * @OUT str:  the output string buffer
 * @IN  len:  the length of string buffer
 * @IN  scale:when preceding_zero is true, scale is the standard output width size
 * @Return: print succeed or not
 */
template<bool preceding_zero> static int int128_to_string(int128 data, char *str, int len, int scale)
{
    Assert(data >= 0);
    Assert(scale >= 0 && scale <= MAXINT128DIGIT);

    errno_t rc = EOK;
    /* turn to int64 */
    if (INT128_INT64_EQ(data)) {
        if (preceding_zero) {
            rc = sprintf_s(str, len, "%0*ld", scale, (int64)data);
        } else {
            rc = sprintf_s(str, len, "%ld", (int64)data);
        }
        securec_check_ss_c(rc, "\0", "\0");
        return rc;
    }

    /* get the absolute value of data, it's useful for sprintf */
    int128 num = data;
    int64 leading = 0;
    int64 trailing = 0;
    trailing = num % P10_INT64;
    num = num / P10_INT64;
    /* two int64 num can represent the int128 data */
    if (INT128_INT64_EQ(num)) {
        leading = (int64)num;
        if (preceding_zero) {
            const int trailing_digits = 18;
            Assert(scale > trailing_digits);
            rc = sprintf_s(str, len, "%0*ld%018ld", scale - trailing_digits, leading, trailing);
        } else {
            rc = sprintf_s(str, len, "%ld%018ld", leading, trailing);
        }
        securec_check_ss_c(rc, "\0", "\0");
        return rc;
    }

    /* two int64 num can't represent int128data, use 3 int64 numbers */
    int64 middle = num % P10_INT64;
    num = num / P10_INT64;
    leading = (int64)num;
    /* both the middle and trailing digits have 18 digits */
    if (preceding_zero) {
        const int middle_trailing_digits = 36;
        Assert(scale > middle_trailing_digits);
        rc = sprintf_s(str, len, "%0*ld%018ld%018ld", scale - middle_trailing_digits, leading, middle, trailing);
    } else {
        rc = sprintf_s(str, len, "%ld%018ld%018ld", leading, middle, trailing);
    }
    securec_check_ss_c(rc, "\0", "\0");
    return rc;
}

/*
 * @Description: print bi128 data to string like numeric_out.
 *
 * @IN  data: int128 data
 * @IN  scale:  the scale of bi128 data
 * @Return: Output string for numeric data type
 */
bool bi128_out(int128 data, int scale, char *buf)
{
    Assert(scale >= 0 && scale <= MAXINT128DIGIT);

    /* data == INT128_MIN */
    if (unlikely(data == INT128_MIN)) {
        errno_t rc = EOK;
        /*
         * Avoid problems with the most negative integer not being representable
         * as a positive integer.
         */
        if (scale > 0) {
            int len = strlen(int128_min_str) - scale;
            rc = memcpy_s(buf, MAXBI128LEN, int128_min_str, len);
            /* check the return value of security function */
            securec_check_c(rc, "\0", "\0");
            buf[len] = '.';
            rc = memcpy_s(buf + len + 1, MAXBI128LEN - len - 1, int128_min_str + len, scale + 1);
            /* check the return value of security function */
            securec_check_c(rc, "\0", "\0");
        } else {
            rc = memcpy_s(buf, MAXBI128LEN, int128_min_str, strlen(int128_min_str) + 1);
            /* check the return value of security function */
            securec_check_c(rc, "\0", "\0");
        }
        return true;
    }

    int128 pre_val = 0;
    int128 post_val = 0;

    /* data is positive */
    if (data >= 0) {
        pre_val = data / get_scale_multiplier(scale);
        post_val = data % get_scale_multiplier(scale);
        if (likely(scale > 0)) {
            if (pre_val == 0 && post_val != 0) {
                buf[0] = '.';
                int128_to_string<true>(post_val, buf + 1, MAXBI128LEN - 1, scale);
            } else {
                int128_to_string<false>(pre_val, buf, MAXBI128LEN, 0);
                int len = strlen(buf);
                buf[len] = '.';
                int128_to_string<true>(post_val, buf + len + 1, MAXBI128LEN - len - 1, scale);
            }
        } else {
            int128_to_string<false>(pre_val, buf, MAXBI128LEN, 0);
        }
    } else { /* data is negative */
        data = -data;
        pre_val = data / get_scale_multiplier(scale);
        post_val = data % get_scale_multiplier(scale);
        buf[0] = '-';
        if (likely(scale > 0)) {
            if (pre_val == 0 && post_val != 0) {
                buf[1] = '.';
                int128_to_string<true>(post_val, buf + 2, MAXBI128LEN - 2, scale); /* 2 is buf[0] and buf[1] */
            } else {
                int128_to_string<false>(pre_val, buf + 1, MAXBI128LEN - 1, 0);
                int len = strlen(buf);
                buf[len] = '.';
                int128_to_string<true>(post_val, buf + len + 1, MAXBI128LEN - len - 1, scale);
            }
        } else {
            int128_to_string<false>(pre_val, buf + 1, MAXBI128LEN - 1, 0);
        }
    }
    return true;
}
