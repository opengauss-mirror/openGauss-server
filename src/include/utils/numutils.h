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
 * numutils.h
 *
 * IDENTIFICATION
 *    src/include/utils/numutils.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef NUMUTILS_H
#define NUMUTILS_H

#include "postgres.h"

extern int32 pg_atoi(char* s, int size, int c, bool can_ignore);
extern int16 pg_strtoint16(const char* s, bool can_ignore = false);
extern int32 pg_strtoint32(const char* s, bool can_ignore);
extern void pg_itoa(int16 i, char* a);
extern void pg_ltoa(int32 l, char* a);
extern char* pg_ltoa2(int32 l, int* len);
extern void pg_ctoa(uint8 i, char* a);
extern void pg_lltoa(int64 ll, char* a);
extern char* pg_lltoa2(int64 ll, int* len);
extern void pg_i128toa(int128 value, char* a, int length);

extern char *pg_ultostr(char *str, uint32 value);
extern char *pg_ultostr_zeropad(char *str, uint32 value, int32 minwidth);
inline char *pg_ultostr_zeropad_width_2(char *str, uint32 value);
inline char *pg_ultostr_zeropad_min_width_4(char *str, uint32 value);

/*
 * A table of all two-digit numbers. This is used to speed up decimal digit
 * generation by copying pairs of digits into the final output.
 */
static constexpr char DIGIT_TABLE[] =
    "00" "01" "02" "03" "04" "05" "06" "07" "08" "09"
    "10" "11" "12" "13" "14" "15" "16" "17" "18" "19"
    "20" "21" "22" "23" "24" "25" "26" "27" "28" "29"
    "30" "31" "32" "33" "34" "35" "36" "37" "38" "39"
    "40" "41" "42" "43" "44" "45" "46" "47" "48" "49"
    "50" "51" "52" "53" "54" "55" "56" "57" "58" "59"
    "60" "61" "62" "63" "64" "65" "66" "67" "68" "69"
    "70" "71" "72" "73" "74" "75" "76" "77" "78" "79"
    "80" "81" "82" "83" "84" "85" "86" "87" "88" "89"
    "90" "91" "92" "93" "94" "95" "96" "97" "98" "99";

inline char *pg_ultostr_zeropad_width_2(char *str, uint32 value)
{
    Assert(value < 100);

    str[0] = DIGIT_TABLE[value * 2];
    str[1] = DIGIT_TABLE[value * 2 + 1];
    return str + 2;
}

inline char *pg_ultostr_zeropad_min_width_4(char *str, uint32 value)
{
    if(likely(value < 10000)) {
        str = pg_ultostr_zeropad_width_2(str, value / 100);
        str = pg_ultostr_zeropad_width_2(str, value % 100);
        return str;
    }

    return pg_ultostr_zeropad(str, value, 4);
}

#endif
