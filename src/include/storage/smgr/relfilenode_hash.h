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
 * relfilenode_hash.h
 *        hash/match function of relfilenode
 * 
 * 
 * IDENTIFICATION
 *        src/include/storage/smgr/relfilenode_hash.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef RELFILENODE_HASH_H
#define RELFILENODE_HASH_H

#include "access/hash.h"
#include "storage/smgr/relfilenode.h"

/* ForkRelFileNode hash/match function */
uint32 fork_file_node_ignore_opt_hash(const void *key, Size keysize);
int fork_file_node_ignore_opt_match(const void *left, const void *right, Size keysize);

/* relfilenode hash/match function */
extern int file_node_ignore_opt_match(const void* left, const void* right, Size keysize);
extern uint32 file_node_ignore_opt_hash(const void* key, Size keysize);

/* fileTag hash/match function */
extern uint32 FileTagHashWithoutOpt(const void *key, Size keysize);
extern int FileTagMatchWithoutOpt(const void *left, const void *right, Size keysize);

/* BufferTag hash/match function */
extern uint32 BufTagHashWithoutOpt(const void *key, Size keysize);
extern int BufTagMatchWithoutOpt(const void *left, const void *right, Size keysize);

/* ReorderBufferTupleCidKey hash/match function */
extern uint32 ReorderBufferTupleCidKeyHash(const void *key, Size keysize);
extern int ReorderBufferTupleCidKeyMatch(const void *left, const void *right, Size keysize);

/* CBMPageTag hash/match function */
extern uint32 CBMPageTagHash(const void *key, Size keysize);
extern int CBMPageTagMatch(const void *left, const void *right, Size keysize);

/* RedoItemTag hash/match function */
extern uint32 RedoItemTagHash(const void *key, Size keysize);
extern int RedoItemTagMatch(const void *left, const void *right, Size keysize);

/* data_writer_rel_key hash/match function */
extern uint32 DataWriterRelKeyHash(const void *key, Size keysize);
extern int DataWriterRelKeyMatch(const void *left, const void *right, Size keysize);

/* CheckpointerRequest hash/match function */
extern uint32 CheckpointerRequestHash(const void *key, Size keysize);
extern int CheckpointerRequestMatch(const void *left, const void *right, Size keysize);

/* BadBlockKey hash/match function */
extern uint32 BadBlockKeyHash(const void *key, Size keysize);
extern int BadBlockKeyMatch(const void *left, const void *right, Size keysize);

/* BadBlockHashKey hash/match function */
extern uint32 BadBlockHashKeyHash(const void *key, Size keysize);
extern int BadBlockHashKeyMatch(const void *left, const void *right, Size keysize);

/* RepairBlockKey hash/match function */
extern uint32 RepairBlockKeyHash(const void *key, Size keysize);
extern int RepairBlockKeyMatch(const void *left, const void *right, Size keysize);

/* RepairFileKey hash/match function */
extern uint32 RepairFileKeyHash(const void *key, Size keysize);
extern int RepairFileKeyMatch(const void *left, const void *right, Size keysize);
#endif
