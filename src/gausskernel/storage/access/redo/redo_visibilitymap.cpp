/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * visibilitymap.cpp
 *    common function for page visible
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/access/redo/visibilitymap.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "storage/buf/bufpage.h"
#include "access/visibilitymap.h"
#include "access/xlogproc.h"
#include "access/redo_common.h"

bool visibilitymap_clear_page(Page mapPage, BlockNumber heapBlk)
{
    uint32 mapByte = (uint32)HEAPBLK_TO_MAPBYTE(heapBlk);
    uint32 mapBit = (uint32)HEAPBLK_TO_MAPBIT(heapBlk);
    uint8 mask = (uint8)(uint32(1) << mapBit);
    unsigned char *map = NULL;

    map = (unsigned char *)PageGetContents(mapPage);
    if (map[mapByte] & mask) {
        map[mapByte] &= ~mask;
        return true;
    }

    return false;
}

void visibilitymap_clear_buffer(RedoBufferInfo *bufferInfo, BlockNumber heapBlk)
{
    if (visibilitymap_clear_page(bufferInfo->pageinfo.page, heapBlk)) {
        MakeRedoBufferDirty(bufferInfo);
    }
}

bool visibilitymap_set_page(Page page, BlockNumber heapBlk)
{
    uint32 mapByte = HEAPBLK_TO_MAPBYTE(heapBlk);
    uint8 mapBit = HEAPBLK_TO_MAPBIT(heapBlk);
    unsigned char *map = NULL;

    map = (unsigned char *)PageGetContents(page);
    if (!(map[mapByte] & (uint32(1) << mapBit))) {
        map[mapByte] |= (uint32(1) << mapBit);
        return true;
    }
    return false;
}
