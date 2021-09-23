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
 * mm_raw_chunk_dir.h
 *    A raw size chunk directory.
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/mot/core/memory/mm_raw_chunk_dir.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef MM_RAW_CHUNK_DIR_H
#define MM_RAW_CHUNK_DIR_H

#include "string_buffer.h"
#include "utilities.h"

namespace MOT {

/**
 * @brief Initializes the global chunk directory.
 * @param[opt] lazy Specifies whether to use lazy load directory (default is yes).
 * @return Zero if succeeded, otherwise an error code.
 * @note In production, this value should be set to zero, otherwise set it to non-zero.
 */
extern int MemRawChunkDirInit(int lazy = 1);

/**
 * @brief Destroys the global chunk directory.
 */
extern void MemRawChunkDirDestroy();

/**
 * @brief Inserts a chunk into the chunk directory.
 * @param chunk The chunk to insert.
 */
extern void MemRawChunkDirInsert(void* chunk);

/**
 * @brief Inserts a virtual chunk into the chunk directory.
 * @param chunk The virtual chunk to insert (real start address).
 * @param chunkCount The number of chunk slots that the virtual chunk spans.
 * @param chunkData The actual data to store in each chunk slot.
 */
extern void MemRawChunkDirInsertEx(void* chunk, uint32_t chunkCount, void* chunkData);

/**
 * @brief Removes a chunk from the chunk directory.
 * @param chunk The chunk to remove.
 */
extern void MemRawChunkDirRemove(void* chunk);

/**
 * @brief Removes a virtual chunk from the chunk directory.
 * @param chunk The chunk to remove.
 * @param chunkCount The number of chunk slots that the virtual chunk spans.
 */
extern void MemRawChunkDirRemoveEx(void* chunk, uint32_t chunkCount);

/**
 * @brief Looks up the source chunk of some address in the chunk directory.
 * @param address The address to lookup.
 * @return The source chunk or NULL of not found.
 */
extern void* MemRawChunkDirLookup(void* address);

/**
 * @brief Prints the chunk directory into log.
 * @param name The name to prepend to the log message.
 * @param logLevel The log level to use in printing.
 */
extern void MemRawChunkDirPrint(const char* name, LogLevel logLevel);

/**
 * @brief Dumps the chunk directory into string buffer.
 * @param indent The indentation level.
 * @param name The name to prepend to the log message.
 * @param stringBuffer The string buffer.
 */
extern void MemRawChunkDirToString(int indent, const char* name, StringBuffer* stringBuffer);

}  // namespace MOT

/**
 * @brief Dumps the chunk directory into standard error stream.
 */
extern "C" void MemRawChunkDirDump();

#endif /* MM_RAW_CHUNK_DIR_H */
