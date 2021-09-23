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
 * bitmapset.cpp
 *    BitmapSet implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/containers/bitmapset.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "bitmapset.h"
#include "debug_utils.h"
#include "global.h"
#include <stdlib.h>
#include <string.h>

namespace MOT {
const int SIZE_OF_BYTE = 8;

BitmapSet::BitmapSet()
{
    Reset();
}

BitmapSet::BitmapSet(uint8_t* data, uint16_t size) : m_data(data), m_size(size), m_init(true)
{}

BitmapSet::~BitmapSet()
{}

void BitmapSet::Init(uint8_t* data, uint16_t size)
{
    m_size = size;
    m_data = data;
    m_init = true;
}

void BitmapSet::Reset()
{
    m_data = NULL;
    m_size = 0;
    m_init = false;
}

void BitmapSet::Reset(uint16_t size)
{
    MOT_ASSERT(m_init);
    m_size = size;
    errno_t erc = memset_s(m_data, GetLength(), 0, GetLength());
    securec_check(erc, "\0", "\0");
}

void BitmapSet::Clear()
{
    errno_t erc = memset_s(m_data, GetLength(), 0, GetLength());
    securec_check(erc, "\0", "\0");
}

bool BitmapSet::IsClear()
{
    uint16_t numBytes = GetLength();
    for (uint16_t i = 0; i < numBytes; i++)
        if (m_data[i] != 0) {
            return false;
        }
    return true;
}

void BitmapSet::SetBit(uint16_t bit)
{
    MOT_ASSERT(bit < m_size);
    m_data[GetByteIndex(bit)] |= (1 << (bit & 0x07));
}

void BitmapSet::UnsetBit(uint16_t bit)
{
    MOT_ASSERT(bit < m_size);
    m_data[GetByteIndex(bit)] &= ~(1 << (bit & 0x07));
}

uint8_t BitmapSet::GetBit(uint16_t bit)
{
    MOT_ASSERT(bit < m_size);
    return (m_data[GetByteIndex(bit)] & (1 << (bit & 0x07))) != 0;
}

uint16_t BitmapSet::GetByteIndex(uint16_t bit)
{
    return (bit >> 3);
}

void BitmapSet::operator|=(BitmapSet bitmapSet)
{
    uint16_t length = GetLength();
    for (uint16_t i = 0; i < length; i++) {
        m_data[i] |= bitmapSet.m_data[i];
    }
}

void BitmapSet::operator&=(BitmapSet bitmapSet)
{
    uint16_t length = GetLength();
    for (uint16_t i = 0; i < length; i++) {
        m_data[i] &= bitmapSet.m_data[i];
    }
}

BitmapSet::BitmapSetIterator::BitmapSetIterator(const BitmapSet& bitmapSet)
    : m_bms(&bitmapSet), m_data(m_bms->m_data), m_bitIndex(-1), m_byteCache(0), m_isSetCache(false)
{
    Next();
}

BitmapSet::BitmapSetIterator::~BitmapSetIterator()
{}

bool BitmapSet::BitmapSetIterator::Start()
{
    m_bitIndex = -1;
    return Next();
}

bool BitmapSet::BitmapSetIterator::End() const
{
    return m_bitIndex >= m_bms->m_size;
}

bool BitmapSet::BitmapSetIterator::Next()
{
    m_bitIndex++;
    if (m_bitIndex >= m_bms->m_size) {
        return false;
    }

    if (m_bitIndex % SIZE_OF_BYTE == 0) {
        m_byteCache = m_data[GetByteIndex(m_bitIndex)];
    } else {
        m_byteCache = m_byteCache >> 1;
    }

    m_isSetCache = (m_byteCache & 1) != 0;
    return true;
}
}  // namespace MOT
