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
 * catalog_column_types.h
 *    Various catalog column types.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/catalog_column_types.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef _CATALOG_COLUMN_TYPES_H_
#define _CATALOG_COLUMN_TYPES_H_

#include <cstdint>

namespace MOT {
#define GetBytes1(x) (((uintptr_t)(x)) & 0x000000ff)
#define GetBytes2(x) (((uintptr_t)(x)) & 0x0000ffff)
#define GetBytes3(x) (((uintptr_t)(x)) & 0x00ffffff)
#define GetBytes4(x) (((uintptr_t)(x)) & 0xffffffff)
#define GetBytes8(x) ((uintptr_t)(x))

typedef union {
    float m_v;
    uint32_t m_r;
    uint8_t m_c[4];
} FloatConvT;

typedef union {
    double m_v;
    uint64_t m_r;
    uint16_t m_c[4];
} DoubleConvT;

#define ColumnClass(Name) Column##Name
#define ColumnType(Type) MOT_TYPE_##Type

#define TYPENAMES                 \
    X(CHAR, "Char")               \
    X(TINY, "Tiny")               \
    X(SHORT, "Short")             \
    X(INT, "Int")                 \
    X(LONG, "Long")               \
    X(FLOAT, "Float")             \
    X(DOUBLE, "Double")           \
    X(DATE, "Date")               \
    X(TIME, "Time")               \
    X(TIMESTAMP, "Timestamp")     \
    X(TIMESTAMPTZ, "TimestampTZ") \
    X(INTERVAL, "Interval")       \
    X(TINTERVAL, "TInterval")     \
    X(TIMETZ, "TimeTZ")           \
    X(DECIMAL, "Decimal")         \
    X(VARCHAR, "Varchar")         \
    X(BLOB, "Blob")               \
    X(NULLBYTES, "NullBytes")     \
    X(UNKNOWN, "Unknown")

typedef enum _mot_catalog_field_types : uint8_t {
#define X(Enum, String) ColumnType(Enum),
    TYPENAMES
#undef X
} MOT_CATALOG_FIELD_TYPES;

// Decimal representation
#define DECIMAL_POSITIVE 0x01
#define DECIMAL_NEGATIVE 0x02
#define DECIMAL_NAN 0x04
#define DECIMAL_DIGITS_PTR 0x08
#define DECIMAL_MAX_DIGITS 256  // supports precision of 1000

typedef struct __attribute__((packed)) _decimal_hdr {
    uint8_t m_flags;
    uint16_t m_weight;
    uint16_t m_scale;
    uint16_t m_ndigits;
} DecimalHdrSt;

typedef struct __attribute__((packed)) _decimal {
    DecimalHdrSt m_hdr;
    uint16_t m_round;
    uint16_t m_digits[0];
} DecimalSt;

#define DECIMAL_MAX_SIZE (sizeof(MOT::DecimalSt) + DECIMAL_MAX_DIGITS * sizeof(uint16_t))
#define DECIMAL_SIZE(d) (sizeof(MOT::DecimalSt) + d->m_hdr.m_ndigits * sizeof(int16_t))

typedef struct __attribute__((packed)) _interval {
    uint64_t m_time;
    int32_t m_day;
    int32_t m_month;
} IntervalSt;

typedef struct __attribute__((packed)) _timetz {
    uint64_t m_time;
    int32_t m_zone;
} TimetzSt;

typedef struct __attribute__((packed)) _tinterval {
    int32_t m_status;
    int32_t m_data[2];
} TintervalSt;
}  // namespace MOT
#endif /* _CATALOG_COLUMN_TYPES_H_ */
