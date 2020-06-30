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
 * ---------------------------------------------------------------------------------------
 * 
 * aiomem.h
 *        memory function for adio
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/aiomem.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef _AIOMEM_H
#define _AIOMEM_H

extern MemoryContext AdioSharedContext;
#define adio_share_alloc(buf_size) MemoryContextAlloc(AdioSharedContext, (buf_size))
#define adio_share_free(ptr) pfree((ptr))

// /sys/block/sda/queue/logical_block_size
#define SYS_LOGICAL_BLOCK_SIZE (512)
#define adio_align_alloc(size) mem_align_alloc(SYS_LOGICAL_BLOCK_SIZE, size)
#define adio_align_free(ptr) mem_align_free(ptr)

#endif /* _AIOMEM_H */
