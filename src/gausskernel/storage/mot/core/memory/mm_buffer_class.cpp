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
 * mm_buffer_class.cpp
 *    Various buffer classes.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_buffer_class.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "mm_buffer_class.h"
#include "mm_def.h"

namespace MOT {
const uint32_t g_memBufferClassSizeKb[]{
    1,    // index 0: MEM_BUFFER_CLASS_KB_1
    2,    // index 1: MEM_BUFFER_CLASS_KB_2
    4,    // index 2: MEM_BUFFER_CLASS_KB_4
    8,    // index 3: MEM_BUFFER_CLASS_KB_8
    16,   // index 4: MEM_BUFFER_CLASS_KB_16
    32,   // index 5: MEM_BUFFER_CLASS_KB_32
    64,   // index 6: MEM_BUFFER_CLASS_KB_64
    127,  // index 7: MEM_BUFFER_CLASS_KB_127
    255,  // index 8: MEM_BUFFER_CLASS_KB_255
    511,  // index 9: MEM_BUFFER_CLASS_KB_511
    1022  // index 10: MEM_BUFFER_CLASS_KB_1022
};
extern const char* MemBufferClassToString(MemBufferClass bufferClass)
{
    switch (bufferClass) {
        case MEM_BUFFER_CLASS_KB_1:
            return "1 KB";
        case MEM_BUFFER_CLASS_KB_2:
            return "2 KB";
        case MEM_BUFFER_CLASS_KB_4:
            return "4 KB";
        case MEM_BUFFER_CLASS_KB_8:
            return "8 KB";
        case MEM_BUFFER_CLASS_KB_16:
            return "16 KB";
        case MEM_BUFFER_CLASS_KB_32:
            return "32 KB";
        case MEM_BUFFER_CLASS_KB_64:
            return "64 KB";
        case MEM_BUFFER_CLASS_KB_127:
            return "127 KB";
        case MEM_BUFFER_CLASS_KB_255:
            return "255 KB";
        case MEM_BUFFER_CLASS_KB_511:
            return "511 KB";
        case MEM_BUFFER_CLASS_KB_1022:
            return "1022 KB";
        default:
            return "N/A";
    }
}

}  // namespace MOT
