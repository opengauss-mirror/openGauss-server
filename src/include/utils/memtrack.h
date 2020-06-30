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
 * memtrack.h
 *        This file contains declarations for memory tracking utility functions.
 *        The relative GUC to trigger the memory tracking feature is listed:
 *        enable_memory_tracking: the GUC must be set as ON;
 *        enable_memory_logging: to generate the file with memory context information
 *        memory_detail_logging: to generate the file with debug allocation information
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/memtrack.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef MEMTRACK_H
#define MEMTRACK_H

#include "c.h"
#include "utils/aset.h"
#include "nodes/memnodes.h"

extern THR_LOCAL int guc_memory_tracking_mode;

extern THR_LOCAL char mctx_track_name[256];

extern THR_LOCAL long mctx_track_value;

extern void MemoryTrackingInit(void);
extern void MemoryTrackingCreate(MemoryContext set, MemoryContext parent);
extern void MemoryTrackingAllocInfo(MemoryContext context, Size size);
extern void MemoryTrackingFreeInfo(MemoryContext context, Size size);
extern void MemoryTrackingLoggingToFile(MemoryTrack track, int key);
extern void MemoryTrackingNodeFree(MemoryTrack track);
extern void MemoryTrackingOutputFile(void);

#ifdef MEMORY_CONTEXT_CHECKING

extern void MemoryTrackingParseGUC(const char* val);

extern void MemoryTrackingDetailInfo(MemoryContext context, Size reqSize, Size chunkSize, const char* file, int line);

extern void MemoryTrackingDetailInfoToFile(void);

#endif

#endif
