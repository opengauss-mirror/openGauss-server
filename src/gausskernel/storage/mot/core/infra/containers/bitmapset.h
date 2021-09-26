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
 * bitmapset.h
 *    BitmapSet implementation.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/infra/containers/bitmapset.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MOT_BITMAPSET_H
#define MOT_BITMAPSET_H

#include <stdint.h>

namespace MOT {
class BitmapSet {
public:
    class BitmapSetIterator {
    public:
        explicit BitmapSetIterator(const BitmapSet& bitmapSet);
        ~BitmapSetIterator();
        inline bool IsSet() const
        {
            return m_isSetCache;
        }
        inline uint16_t GetPosition() const
        {
            return (uint16_t)m_bitIndex;
        }
        bool Start();
        bool End() const;
        bool Next();

    private:
        const BitmapSet* m_bms;
        uint8_t* m_data;
        int16_t m_bitIndex;
        uint8_t m_byteCache;
        bool m_isSetCache;
    };

    BitmapSet();
    BitmapSet(uint8_t* data, uint16_t size);
    ~BitmapSet();

    void Init(uint8_t* data, uint16_t size);
    void Reset();
    void Clear();
    void Reset(uint16_t size);
    void SetBit(uint16_t bit);
    void UnsetBit(uint16_t bit);
    uint8_t GetBit(uint16_t bit);
    bool IsClear();

    inline uint8_t* GetData()
    {
        return m_data;
    }
    inline uint16_t GetSize() const
    {
        return m_size;
    }
    inline bool IsInitialized() const
    {
        return m_init;
    }
    inline uint16_t GetLength()
    {
        return GetLength(m_size);
    }
    static uint16_t GetByteIndex(uint16_t index);
    static uint16_t GetLength(uint16_t numBits)
    {
        return GetByteIndex(numBits) + 1;
    }

    void operator|=(BitmapSet bitmapSet);
    void operator&=(BitmapSet bitmapSet);

private:
    uint8_t* m_data;
    uint16_t m_size;
    bool m_init;

    friend class BitmapSetIterator;
};
}  // namespace MOT

#endif /* MOT_BITMAPSET_H */
