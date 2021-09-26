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
 * column.cpp
 *    The Column class describes a single field in a row in a table.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/storage/column.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include "global.h"
#include "column.h"
#include "utilities.h"
#include "mot_error.h"

extern uint16_t MOTTimestampToStr(uintptr_t src, char* destBuf, size_t len);
extern uint16_t MOTTimestampTzToStr(uintptr_t src, char* destBuf, size_t len);
extern uint16_t MOTDateToStr(uintptr_t src, char* destBuf, size_t len);

namespace MOT {
DECLARE_LOGGER(Column, Storage)

// Class column
const char* Column::ColumnTypeToStr(MOT_CATALOG_FIELD_TYPES type)
{
    const char* res = nullptr;
    switch (type) {
#define X(Enum, String)    \
    case ColumnType(Enum): \
        res = String;      \
        break;
        TYPENAMES
#undef X
        default:
            res = "Unknown";
            break;
    }

    return res;
}

MOT_CATALOG_FIELD_TYPES Column::ColumnStrTypeToEnum(const char* type)
{
    if (type == nullptr) {
        return MOT_CATALOG_FIELD_TYPES::MOT_TYPE_UNKNOWN;
    }
#define X(Enum, String) else if (strcasecmp(type, String) == 0) return ColumnType(Enum);
    TYPENAMES
#undef X
    // some extentions to support testing schema
    else if (strcasecmp(type, "uint64_t") == 0 || strcasecmp(type, "int64_t") == 0 || strcasecmp(type, "uint64") == 0 ||
             strcasecmp(type, "int64") == 0) return MOT_CATALOG_FIELD_TYPES::MOT_TYPE_LONG;
    else if (strcasecmp(type, "string") == 0) return MOT_CATALOG_FIELD_TYPES::MOT_TYPE_VARCHAR;
    else return MOT_CATALOG_FIELD_TYPES::MOT_TYPE_UNKNOWN;
}

Column* Column::AllocColumn(MOT_CATALOG_FIELD_TYPES type)
{
    Column* res = nullptr;
    switch (type) {
#define X(Enum, String)                                                                                   \
    case ColumnType(Enum):                                                                                \
        res = (Column*)new (std::nothrow) ColumnClass(Enum);                                              \
        if (res == nullptr) {                                                                             \
            MOT_REPORT_ERROR(MOT_ERROR_OOM, "Table Initiailization", "Failed to allocate column object"); \
        }                                                                                                 \
        break;
        TYPENAMES
#undef X

        default:
            MOT_REPORT_ERROR(MOT_ERROR_INVALID_ARG, "Table Initialization", "Invalid column type: %d", (int)type);
            res = nullptr;
    }

    return res;
}

const char* Column::ColumnErrorMsg(RC err)
{
    switch (err) {
        case RC_UNSUPPORTED_COL_TYPE:
            return "Column type is not supported";

        case RC_EXCEEDS_MAX_ROW_SIZE:
            return "Column size causes row to exceed max row size allowed";

        case RC_COL_NAME_EXCEEDS_MAX_SIZE:
            return "Column name is longer that allowed";

        case RC_COL_SIZE_INVALID:
            return "Column size is invalid";

        case RC_TABLE_EXCEEDS_MAX_DECLARED_COLS:
            return "Table declared column count exceeded";

        default:
            return "Foreign table does not support this column definition";
    }
}

Column::Column()
{
    this->m_id = 0;
    this->m_size = 0;
    this->m_offset = 0;
    this->m_isNotNull = true;
    this->m_nameLen = 0;
    errno_t erc = memset_s(&(this->m_name), sizeof(this->m_name), 0, sizeof(this->m_name));
    securec_check(erc, "\0", "\0");
    erc = memset_s(&(this->m_type), sizeof(this->m_type), 0, sizeof(this->m_type));
    securec_check(erc, "\0", "\0");
    this->m_envelopeType = 0;
}

Column::~Column()
{}

bool ColumnCHAR::Pack(uint8_t* dest, uintptr_t src, size_t len)
{
    *(dest + m_offset) = (uint8_t)GetBytes1(src);

    return true;
}

bool ColumnCHAR::PackKey(uint8_t* dest, uintptr_t src, size_t len, uint8_t fill)
{
    *(dest) = (uint8_t)GetBytes1(src);

    return true;
}

void ColumnCHAR::Unpack(uint8_t* data, uintptr_t* dest, size_t& len)
{
    *dest = GetBytes1(*(uint8_t*)(data + m_offset));
}

void ColumnCHAR::SetKeySize()
{
    m_keySize = m_size;
}

uint16_t ColumnCHAR::PrintValue(uint8_t* data, char* destBuf, size_t len)
{
    if (len >= 1) {
        *destBuf = GetBytes1(*(uint8_t*)(data + m_offset));
        return 1;
    } else
        return 0;
}

bool ColumnTINY::Pack(uint8_t* dest, uintptr_t src, size_t len)
{
    *(dest + m_offset) = (uint8_t)GetBytes1(src);

    return true;
}

bool ColumnTINY::PackKey(uint8_t* dest, uintptr_t src, size_t len, uint8_t fill)
{
    *(dest + 1) = (uint8_t)GetBytes1(src);
    if (*(dest + 1) & 0x80)
        *dest = 0x00;
    else
        *dest = 0x01;

    return true;
}

void ColumnTINY::Unpack(uint8_t* data, uintptr_t* dest, size_t& len)
{
    *dest = GetBytes1(*(uint8_t*)(data + m_offset));
}

void ColumnTINY::SetKeySize()
{
    m_keySize = m_size + 1;
}

uint16_t ColumnTINY::PrintValue(uint8_t* data, char* destBuf, size_t len)
{
    if (len >= 3) {
        uint8_t val = GetBytes1(*(uint8_t*)(data + m_offset));
        errno_t erc = snprintf_s(destBuf, len, len - 1, "%d", val);
        securec_check_ss(erc, "\0", "\0");
        return erc;
    }
    return 0;
}

bool ColumnSHORT::Pack(uint8_t* dest, uintptr_t src, size_t len)
{
    *(uint16_t*)(dest + m_offset) = (uint16_t)GetBytes2(src);

    return true;
}

bool ColumnSHORT::PackKey(uint8_t* dest, uintptr_t src, size_t len, uint8_t fill)
{
    uint16_t tmp = (uint16_t)GetBytes2(src);
    *(uint16_t*)(dest + 1) = ntohs(tmp);
    if (tmp & 0x8000)
        *dest = 0x00;
    else
        *dest = 0x01;

    return true;
}

void ColumnSHORT::Unpack(uint8_t* data, uintptr_t* dest, size_t& len)
{
    *dest = GetBytes2(*(uint16_t*)(data + m_offset));
}

void ColumnSHORT::SetKeySize()
{
    m_keySize = m_size + 1;
}

uint16_t ColumnSHORT::PrintValue(uint8_t* data, char* destBuf, size_t len)
{
    if (len >= 5) {
        uint16_t val = GetBytes2(*(uint16_t*)(data + m_offset));
        errno_t erc = snprintf_s(destBuf, len, len - 1, "%d", val);
        securec_check_ss(erc, "\0", "\0");
        return erc;
    }

    return 0;
}

bool ColumnINT::Pack(uint8_t* dest, uintptr_t src, size_t len)
{
    *(uint32_t*)(dest + m_offset) = (uint32_t)GetBytes4(src);

    return true;
}

bool ColumnINT::PackKey(uint8_t* dest, uintptr_t src, size_t len, uint8_t fill)
{
    uint32_t tmp = (uint32_t)GetBytes4(src);
    *(uint32_t*)(dest + 1) = ntohl(tmp);
    if (tmp & 0x80000000)
        *dest = 0x00;
    else
        *dest = 0x01;

    return true;
}

void ColumnINT::Unpack(uint8_t* data, uintptr_t* dest, size_t& len)
{
    *dest = GetBytes4(*(uint32_t*)(data + m_offset));
}

void ColumnINT::SetKeySize()
{
    m_keySize = m_size + 1;
}

uint16_t ColumnINT::PrintValue(uint8_t* data, char* destBuf, size_t len)
{
    if (len >= 10) {
        uint32_t val = GetBytes4(*(uint32_t*)(data + m_offset));
        errno_t erc = snprintf_s(destBuf, len, len - 1, "%d", val);
        securec_check_ss(erc, "\0", "\0");
        return erc;
    }

    return 0;
}

bool ColumnLONG::Pack(uint8_t* dest, uintptr_t src, size_t len)
{
    *(uint64_t*)(dest + m_offset) = (uint64_t)GetBytes8(src);

    return true;
}

bool ColumnLONG::PackKey(uint8_t* dest, uintptr_t src, size_t len, uint8_t fill)
{
    uint64_t tmp = (uint64_t)GetBytes8(src);
    *(uint64_t*)(dest + 1) = be64toh(tmp);
    if (tmp & 0x8000000000000000) {
        *dest = 0x00;
    } else {
        *dest = 0x01;
    }
    return true;
}

void ColumnLONG::Unpack(uint8_t* data, uintptr_t* dest, size_t& len)
{
    *dest = GetBytes8(*(uint64_t*)(data + m_offset));
}

void ColumnLONG::SetKeySize()
{
    m_keySize = m_size + 1;
}

uint16_t ColumnLONG::PrintValue(uint8_t* data, char* destBuf, size_t len)
{
    if (len >= 20) {
        uint64_t val = GetBytes8(*(uint64_t*)(data + m_offset));
        errno_t erc = snprintf_s(destBuf, len, len - 1, "%ld", val);
        securec_check_ss(erc, "\0", "\0");
        return erc;
    }
    return 0;
}

bool ColumnFLOAT::Pack(uint8_t* dest, uintptr_t src, size_t len)
{
    FloatConvT t;
    t.m_r = (uint32_t)GetBytes4(src);

    *(float*)(dest + m_offset) = t.m_v;

    return true;
}

#define FLOAT_SIGN_MASK 0x80000000

bool ColumnFLOAT::PackKey(uint8_t* dest, uintptr_t src, size_t len, uint8_t fill)
{
    FloatConvT t;
    t.m_r = (uint32_t)GetBytes4(src);

    // keep sign
    if (t.m_r & FLOAT_SIGN_MASK) {
        *dest = 0x00;
    } else {
        *dest = 0x01;
    }
    // remove sign and move exp to a most significant byte
    t.m_r <<= 1;
    // extract exp
    if (*dest == 0) {
        *(dest + 1) = 255 - t.m_c[3];
        t.m_r <<= 8;
        t.m_r = (~t.m_r + 1);
    } else {
        *(dest + 1) = t.m_c[3];
        t.m_r <<= 8;
    }
    *(uint32_t*)(dest + 2) = htonl(t.m_r);

    return true;
}

void ColumnFLOAT::Unpack(uint8_t* data, uintptr_t* dest, size_t& len)
{
    FloatConvT t;
    t.m_v = *(float*)(data + m_offset);
    *dest = GetBytes4(t.m_r);
}

void ColumnFLOAT::SetKeySize()
{
    m_keySize = m_size + 2;
}

uint16_t ColumnFLOAT::PrintValue(uint8_t* data, char* destBuf, size_t len)
{
    if (len >= 40) {
        FloatConvT t;
        t.m_v = *(float*)(data + m_offset);
        errno_t erc = snprintf_s(destBuf, len, len - 1, "%.9g", t.m_v);
        securec_check_ss(erc, "\0", "\0");
        return erc;
    }
    return 0;
}

bool ColumnDOUBLE::Pack(uint8_t* dest, uintptr_t src, size_t len)
{
    DoubleConvT t;
    t.m_r = (uint64_t)GetBytes8(src);

    *(double*)(dest + m_offset) = t.m_v;

    return true;
}

#define DOUBLE_SIGN_MASK 0x8000000000000000
#define DOUBLE_EXP_MASK 0xFFE0

bool ColumnDOUBLE::PackKey(uint8_t* dest, uintptr_t src, size_t len, uint8_t fill)
{
    DoubleConvT t;
    t.m_r = (uint64_t)GetBytes8(src);

    uint16_t* exp_ptr = (uint16_t*)(dest + 1);
    uint16_t exp;

    // keep sign
    if (t.m_r & DOUBLE_SIGN_MASK) {
        *dest = 0x00;
    } else {
        *dest = 0x01;
    }

    // remove sign and move exp to a most significant byte
    t.m_r <<= 1;
    // extract exp
    if (*dest == 0) {
        exp = 2048 - (t.m_c[3] & DOUBLE_EXP_MASK);
    } else {
        exp = (t.m_c[3] & DOUBLE_EXP_MASK);
    }

    *exp_ptr = htons(exp);
    // remove exp
    t.m_r <<= 11;
    if (*dest == 0) {
        t.m_r = (~t.m_r + 1);
    }

    *(uint64_t*)(dest + 3) = be64toh(t.m_r);
    return true;
}

void ColumnDOUBLE::Unpack(uint8_t* data, uintptr_t* dest, size_t& len)
{
    DoubleConvT t;
    t.m_v = *(double*)(data + m_offset);
    *dest = GetBytes8(t.m_r);
}

void ColumnDOUBLE::SetKeySize()
{
    m_keySize = m_size + 3;
}

uint16_t ColumnDOUBLE::PrintValue(uint8_t* data, char* destBuf, size_t len)
{
    if (len >= 40) {
        DoubleConvT t;
        t.m_v = *(double*)(data + m_offset);
        errno_t erc = snprintf_s(destBuf, len, len - 1, "%.17g", t.m_v);
        securec_check_ss(erc, "\0", "\0");
        return erc;
    }
    return 0;
}

bool ColumnDATE::Pack(uint8_t* dest, uintptr_t src, size_t len)
{
    *(uint32_t*)(dest + m_offset) = (uint32_t)GetBytes4(src);

    return true;
}

bool ColumnDATE::PackKey(uint8_t* dest, uintptr_t src, size_t len, uint8_t fill)
{
    *(uint32_t*)(dest) = ntohl((uint32_t)GetBytes4(src));

    return true;
}

void ColumnDATE::Unpack(uint8_t* data, uintptr_t* dest, size_t& len)
{
    *dest = GetBytes4(*(uint32_t*)(data + m_offset));
}

void ColumnDATE::SetKeySize()
{
    m_keySize = m_size + 1;
}

uint16_t ColumnDATE::PrintValue(uint8_t* data, char* destBuf, size_t len)
{
    if (len >= MOT_MAXDATELEN) {
        return MOTDateToStr(GetBytes4(*(uint32_t*)(data + m_offset)), destBuf, len);
    }
    return 0;
}

bool ColumnTIME::Pack(uint8_t* dest, uintptr_t src, size_t len)
{
    *(uint64_t*)(dest + m_offset) = (uint64_t)GetBytes8(src);

    return true;
}

bool ColumnTIME::PackKey(uint8_t* dest, uintptr_t src, size_t len, uint8_t fill)
{
    *(uint64_t*)(dest) = be64toh((uint64_t)GetBytes8(src));

    return true;
}

void ColumnTIME::Unpack(uint8_t* data, uintptr_t* dest, size_t& len)
{
    *dest = GetBytes8(*(uint64_t*)(data + m_offset));
}

void ColumnTIME::SetKeySize()
{
    m_keySize = m_size;
}

uint16_t ColumnTIME::PrintValue(uint8_t* data, char* destBuf, size_t len)
{
    if (len >= 3) {
        errno_t erc = snprintf_s(destBuf, len, len - 1, "NaN");
        securec_check_ss(erc, "\0", "\0");
        return erc;
    }
    return 0;
}

bool ColumnINTERVAL::Pack(uint8_t* dest, uintptr_t src, size_t len)
{
    *(IntervalSt*)(dest + m_offset) = *(IntervalSt*)GetBytes8(src);

    return true;
}

bool ColumnINTERVAL::PackKey(uint8_t* dest, uintptr_t src, size_t len, uint8_t fill)
{
    *(IntervalSt*)(dest) = *(IntervalSt*)GetBytes8(src);

    return true;
}

void ColumnINTERVAL::Unpack(uint8_t* data, uintptr_t* dest, size_t& len)
{
    *dest = GetBytes8((data + m_offset));
}

void ColumnINTERVAL::SetKeySize()
{
    m_keySize = m_size;
}

uint16_t ColumnINTERVAL::PrintValue(uint8_t* data, char* destBuf, size_t len)
{
    if (len >= 3) {
        errno_t erc = snprintf_s(destBuf, len, len - 1, "NaN");
        securec_check_ss(erc, "\0", "\0");
        return erc;
    }
    return 0;
}

bool ColumnTINTERVAL::Pack(uint8_t* dest, uintptr_t src, size_t len)
{
    *(TintervalSt*)(dest + m_offset) = *(TintervalSt*)GetBytes8(src);

    return true;
}

bool ColumnTINTERVAL::PackKey(uint8_t* dest, uintptr_t src, size_t len, uint8_t fill)
{
    *(TintervalSt*)(dest) = *(TintervalSt*)GetBytes8(src);

    return true;
}

void ColumnTINTERVAL::Unpack(uint8_t* data, uintptr_t* dest, size_t& len)
{
    *dest = GetBytes8((data + m_offset));
}

void ColumnTINTERVAL::SetKeySize()
{
    m_keySize = m_size;
}

uint16_t ColumnTINTERVAL::PrintValue(uint8_t* data, char* destBuf, size_t len)
{
    if (len >= 3) {
        errno_t erc = snprintf_s(destBuf, len, len - 1, "NaN");
        securec_check_ss(erc, "\0", "\0");
        return erc;
    }
    return 0;
}

bool ColumnTIMETZ::Pack(uint8_t* dest, uintptr_t src, size_t len)
{
    *(TimetzSt*)(dest + m_offset) = *(TimetzSt*)GetBytes8(src);

    return true;
}

bool ColumnTIMETZ::PackKey(uint8_t* dest, uintptr_t src, size_t len, uint8_t fill)
{
    *(TimetzSt*)(dest) = *(TimetzSt*)GetBytes8(src);

    return true;
}

void ColumnTIMETZ::Unpack(uint8_t* data, uintptr_t* dest, size_t& len)
{
    *dest = GetBytes8((data + m_offset));
}

void ColumnTIMETZ::SetKeySize()
{
    m_keySize = m_size;
}

uint16_t ColumnTIMETZ::PrintValue(uint8_t* data, char* destBuf, size_t len)
{
    if (len >= 3) {
        errno_t erc = snprintf_s(destBuf, len, len - 1, "NaN");
        securec_check_ss(erc, "\0", "\0");
        return erc;
    }
    return 0;
}

bool ColumnTIMESTAMP::Pack(uint8_t* dest, uintptr_t src, size_t len)
{
    *(uint64_t*)(dest + m_offset) = (uint64_t)GetBytes8(src);

    return true;
}

bool ColumnTIMESTAMP::PackKey(uint8_t* dest, uintptr_t src, size_t len, uint8_t fill)
{
    uint64_t tmp = (uint64_t)GetBytes8(src);
    *(uint64_t*)(dest + 1) = be64toh(tmp);
    if (tmp & 0x8000000000000000) {
        *dest = 0x00;
    } else {
        *dest = 0x01;
    }
    return true;
}

void ColumnTIMESTAMP::Unpack(uint8_t* data, uintptr_t* dest, size_t& len)
{
    *dest = GetBytes8(*(uint64_t*)(data + m_offset));
}

void ColumnTIMESTAMP::SetKeySize()
{
    m_keySize = m_size + 1;
}

uint16_t ColumnTIMESTAMP::PrintValue(uint8_t* data, char* destBuf, size_t len)
{
    if (len >= MOT_MAXDATELEN) {
        return MOTTimestampToStr(GetBytes8(*(uint64_t*)(data + m_offset)), destBuf, len);
    }
    return 0;
}

bool ColumnTIMESTAMPTZ::Pack(uint8_t* dest, uintptr_t src, size_t len)
{
    *(uint64_t*)(dest + m_offset) = (uint64_t)GetBytes8(src);

    return true;
}

bool ColumnTIMESTAMPTZ::PackKey(uint8_t* dest, uintptr_t src, size_t len, uint8_t fill)
{
    uint64_t tmp = (uint64_t)GetBytes8(src);
    *(uint64_t*)(dest + 1) = be64toh(tmp);
    if (tmp & 0x8000000000000000) {
        *dest = 0x00;
    } else {
        *dest = 0x01;
    }
    return true;
}

void ColumnTIMESTAMPTZ::Unpack(uint8_t* data, uintptr_t* dest, size_t& len)
{
    *dest = GetBytes8(*(uint64_t*)(data + m_offset));
}

void ColumnTIMESTAMPTZ::SetKeySize()
{
    m_keySize = m_size + 1;
}

uint16_t ColumnTIMESTAMPTZ::PrintValue(uint8_t* data, char* destBuf, size_t len)
{
    if (len >= MOT_MAXDATELEN) {
        return MOTTimestampTzToStr(GetBytes8(*(uint64_t*)(data + m_offset)), destBuf, len);
    }
    return 0;
}

bool ColumnDECIMAL::Pack(uint8_t* dest, uintptr_t src, size_t len)
{
    DecimalSt* d = (DecimalSt*)src;

    if (len > m_size)
        len = m_size;
    errno_t erc = memcpy_s(dest + m_offset, m_size, (void*)d, len);
    securec_check(erc, "\0", "\0");
    return true;
}

bool ColumnDECIMAL::PackKey(uint8_t* dest, uintptr_t src, size_t len, uint8_t fill)
{
    DecimalSt* d = (DecimalSt*)src;

    if (len > m_size)
        len = m_size;
    errno_t erc = memcpy_s(dest, m_size, (void*)d, len);
    securec_check(erc, "\0", "\0");

    return true;
}

void ColumnDECIMAL::Unpack(uint8_t* data, uintptr_t* dest, size_t& len)
{
    *dest = GetBytes8(data + m_offset);
}

void ColumnDECIMAL::SetKeySize()
{
    m_keySize = m_size;
}

uint16_t ColumnDECIMAL::PrintValue(uint8_t* data, char* destBuf, size_t len)
{
    if (len >= 3) {
        errno_t erc = snprintf_s(destBuf, len, len - 1, "NaN");
        securec_check_ss(erc, "\0", "\0");
        return erc;
    }
    return 0;
}

bool ColumnVARCHAR::Pack(uint8_t* dest, uintptr_t src, size_t len)
{
    if (len > m_size)
        return false;

    *((uint32_t*)(dest + m_offset)) = len;
    errno_t erc = memcpy_s(dest + m_offset + 4, m_size - 4, (void*)src, len);
    securec_check(erc, "\0", "\0");
    return true;
}

bool ColumnVARCHAR::PackKey(uint8_t* dest, uintptr_t src, size_t len, uint8_t fill)
{
    *((uint32_t*)dest) = 0;
    errno_t erc = memcpy_s(dest + 4, m_keySize - 4, (void*)src, len);
    securec_check(erc, "\0", "\0");
    if (fill != 0x00) {
        erc = memset_s(dest + 4 + len, m_keySize - len - 4, fill, m_keySize - len - 4);
        securec_check(erc, "\0", "\0");
    }

    return true;
}

void ColumnVARCHAR::Unpack(uint8_t* data, uintptr_t* dest, size_t& len)
{
    len = *(uint32_t*)(data + m_offset);
    *dest = GetBytes8(data + m_offset + 4);
}

void ColumnVARCHAR::SetKeySize()
{
    m_keySize = m_size;
}

uint16_t ColumnVARCHAR::PrintValue(uint8_t* data, char* destBuf, size_t len)
{
    uint32_t val_len = *(uint32_t*)(data + m_offset);
    if (len >= val_len) {
        errno_t erc =
            snprintf_s(destBuf, len, len - 1, "%*.*s", val_len, val_len, (char*)GetBytes8(data + m_offset + 4));
        securec_check_ss(erc, "\0", "\0");
        return erc;
    }
    return 0;
}

bool ColumnBLOB::Pack(uint8_t* dest, uintptr_t src, size_t len)
{
    if (len > m_size)
        return false;

    *((uint32_t*)(dest + m_offset)) = len;
    errno_t erc = memcpy_s(dest + m_offset + 4, m_size - 4, (void*)src, len);
    securec_check(erc, "\0", "\0");
    return true;
}

bool ColumnBLOB::PackKey(uint8_t* dest, uintptr_t src, size_t len, uint8_t fill)
{
    *((uint32_t*)dest) = len;
    errno_t erc = memcpy_s(dest + 4, m_keySize - 4, (void*)src, len);
    securec_check(erc, "\0", "\0");
    return true;
}

void ColumnBLOB::Unpack(uint8_t* data, uintptr_t* dest, size_t& len)
{
    len = *(uint32_t*)(data + m_offset);
    *dest = GetBytes8(data + m_offset + 4);
}

void ColumnBLOB::SetKeySize()
{
    m_keySize = m_size;
}

uint16_t ColumnBLOB::PrintValue(uint8_t* data, char* destBuf, size_t len)
{
    if (len >= 3) {
        errno_t erc = snprintf_s(destBuf, len, len - 1, "NaN");
        securec_check_ss(erc, "\0", "\0");
        return erc;
    }
    return 0;
}

bool ColumnNULLBYTES::Pack(uint8_t* dest, uintptr_t src, size_t len)
{
    if (len > m_size)
        return false;

    errno_t erc = memcpy_s(dest + m_offset, m_size, (void*)src, len);
    securec_check(erc, "\0", "\0");
    return true;
}

bool ColumnNULLBYTES::PackKey(uint8_t* dest, uintptr_t src, size_t len, uint8_t fill)
{
    return false;
}

void ColumnNULLBYTES::Unpack(uint8_t* data, uintptr_t* dest, size_t& len)
{
    *dest = GetBytes8(data + m_offset);
}

void ColumnNULLBYTES::SetKeySize()
{
    m_keySize = m_size;
}

uint16_t ColumnNULLBYTES::PrintValue(uint8_t* data, char* destBuf, size_t len)
{
    if (len >= 3) {
        errno_t erc = snprintf_s(destBuf, len, len - 1, "NaN");
        securec_check_ss(erc, "\0", "\0");
        return erc;
    }
    return 0;
}

bool ColumnUNKNOWN::Pack(uint8_t* dest, uintptr_t src, size_t len)
{
    MOT_ASSERT(false);
    return false;
}

bool ColumnUNKNOWN::PackKey(uint8_t* dest, uintptr_t src, size_t len, uint8_t fill)
{
    MOT_ASSERT(false);
    return false;
}

void ColumnUNKNOWN::Unpack(uint8_t* data, uintptr_t* dest, size_t& len)
{
    MOT_ASSERT(false);
}

void ColumnUNKNOWN::SetKeySize()
{
    MOT_ASSERT(false);
}

uint16_t ColumnUNKNOWN::PrintValue(uint8_t* data, char* destBuf, size_t len)
{
    if (len >= 3) {
        errno_t erc = snprintf_s(destBuf, len, len - 1, "NaN");
        securec_check_ss(erc, "\0", "\0");
        return erc;
    }
    return 0;
}
}  // namespace MOT
