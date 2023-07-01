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
 * relfilenode_hash.cpp
 *        hash/match function of relfilenode
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/access/common/relfilenode_hash.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "storage/buf/buf_internals.h"
#include "access/cbmparsexlog.h"
#include "replication/reorderbuffer.h"
#include "replication/datareceiver.h"
#include "pgstat.h"
#include "storage/smgr/relfilenode_hash.h"

uint32 fork_file_node_ignore_opt_hash(const void *key, Size keysize)
{
    ForkRelFileNode rnode = *(const ForkRelFileNode *)key;
    rnode.rnode.opt = DefaultFileNodeOpt;
    return DatumGetUInt32(hash_any((const unsigned char *)&rnode, (int)keysize));
}

int fork_file_node_ignore_opt_match(const void *left, const void *right, Size keysize)
{
    const ForkRelFileNode *lnode = (const ForkRelFileNode *)left;
    const ForkRelFileNode *rnode = (const ForkRelFileNode *)right;
    Assert(lnode != NULL && rnode != NULL);
    Assert(keysize == sizeof(ForkRelFileNode));

    /* we just care whether the result is 0 or not */
    if (RelFileNodeEquals(lnode->rnode, rnode->rnode) && lnode->forkNum == rnode->forkNum) {
        return 0;
    }
    return 1;
}

int file_node_ignore_opt_match(const void *left, const void *right, Size keysize)
{
    const RelFileNode *lnode = (const RelFileNode *)left;
    const RelFileNode *rnode = (const RelFileNode *)right;
    Assert(lnode != NULL && rnode != NULL);
    Assert(keysize == sizeof(RelFileNode));

    /* we just care whether the result is 0 or not */
    if (RelFileNodeEquals(*lnode, *rnode)) {
        return 0;
    }

    return 1;
}

uint32 file_node_ignore_opt_hash(const void *key, Size keysize)
{
    RelFileNode rnode = *(const RelFileNode *)key;
    rnode.opt = DefaultFileNodeOpt;
    return DatumGetUInt32(hash_any((const unsigned char *)&rnode, (int)keysize));
}

uint32 FileTagHashWithoutOpt(const void *key, Size keysize)
{
    FileTag fileTag = *(const FileTag *)key;
    fileTag.rnode.opt = 0;
    return tag_hash((const void *)&fileTag, keysize);
}

int FileTagMatchWithoutOpt(const void *left, const void *right, Size keysize)
{
    const FileTag *lnode = (const FileTag *)left;
    const FileTag *rnode = (const FileTag *)right;
    Assert(keysize == sizeof(FileTag));

    /* we just care whether the result is 0 or not */
    if (RelFileNodeEquals(lnode->rnode, rnode->rnode) && lnode->handler == rnode->handler &&
        rnode->segno == lnode->segno && rnode->forknum == lnode->forknum) {
        return 0;
    }

    return 1;
}

uint32 BufTagHashWithoutOpt(const void *key, Size keysize)
{
    return BufTableHashCode((BufferTag *)key);
}

int BufTagMatchWithoutOpt(const void *left, const void *right, Size keysize)
{
    const BufferTag *leftBufferTag = (const BufferTag *)left;
    const BufferTag *rightBufferTag = (const BufferTag *)right;
    Assert(keysize == sizeof(BufferTag));

    /* we just care whether the result is 0 or not */
    if (RelFileNodeEquals(leftBufferTag->rnode, rightBufferTag->rnode) &&
        leftBufferTag->forkNum == rightBufferTag->forkNum && leftBufferTag->blockNum == rightBufferTag->blockNum) {
        return 0;
    }

    return 1;
}

uint32 ReorderBufferTupleCidKeyHash(const void *key, Size keysize)
{
    ReorderBufferTupleCidKey reorderBufferTupleCidKey = *(const ReorderBufferTupleCidKey *)key;
    reorderBufferTupleCidKey.relnode.opt = DefaultFileNodeOpt;
    return DatumGetUInt32(hash_any((const unsigned char *)&reorderBufferTupleCidKey, (int)keysize));
}

int ReorderBufferTupleCidKeyMatch(const void *left, const void *right, Size keysize)
{
    const ReorderBufferTupleCidKey *leftKey = (const ReorderBufferTupleCidKey *)left;
    const ReorderBufferTupleCidKey *rightKey = (const ReorderBufferTupleCidKey *)right;
    Assert(keysize == sizeof(ReorderBufferTupleCidKey));

    /* we just care whether the result is 0 or not */
    if (RelFileNodeEquals(leftKey->relnode, rightKey->relnode) &&
        BlockIdEquals(&(leftKey->tid.ip_blkid), &(rightKey->tid.ip_blkid)) &&
        leftKey->tid.ip_posid == rightKey->tid.ip_posid) {
        return 0;
    }

    return 1;
}

uint32 CBMPageTagHash(const void *key, Size keysize)
{
    CBMPageTag cbmPage = *(const CBMPageTag *)key;
    cbmPage.rNode.opt = DefaultFileNodeOpt;
    return DatumGetUInt32(hash_any((const unsigned char *)&cbmPage, (int)keysize));
}

int CBMPageTagMatch(const void *left, const void *right, Size keysize)
{
    const CBMPageTag *leftKey = (const CBMPageTag *)left;
    const CBMPageTag *rightKey = (const CBMPageTag *)right;
    Assert(keysize == sizeof(CBMPageTag));

    /* we just care whether the result is 0 or not */
    if (RelFileNodeEquals(leftKey->rNode, rightKey->rNode) && leftKey->forkNum == rightKey->forkNum) {
        return 0;
    }

    return 1;
}

uint32 DataWriterRelKeyHash(const void *key, Size keysize)
{
    data_writer_rel_key dataWriterRelKey = *(const data_writer_rel_key *)key;
    dataWriterRelKey.node.opt = DefaultFileNodeOpt;
    return DatumGetUInt32(hash_any((const unsigned char *)&dataWriterRelKey, (int)keysize));
}

int DataWriterRelKeyMatch(const void *left, const void *right, Size keysize)
{
    const data_writer_rel_key *leftKey = (const data_writer_rel_key *)left;
    const data_writer_rel_key *rightKey = (const data_writer_rel_key *)right;
    Assert(keysize == sizeof(data_writer_rel_key));

    /* we just care whether the result is 0 or not */
    if (RelFileNodeEquals(leftKey->node, rightKey->node) && leftKey->forkno == rightKey->forkno &&
        leftKey->attid == rightKey->attid && leftKey->type == rightKey->type) {
        return 0;
    }

    return 1;
}

uint32 CheckpointerRequestHash(const void *key, Size keysize)
{
    CheckpointerRequest checkpointerRequest = *(const CheckpointerRequest *)key;
    checkpointerRequest.ftag.rnode.opt = DefaultFileNodeOpt;
    return DatumGetUInt32(hash_any((const unsigned char *)&checkpointerRequest, (int)keysize));
}

int CheckpointerRequestMatch(const void *left, const void *right, Size keysize)
{
    const CheckpointerRequest *leftKey = (const CheckpointerRequest *)left;
    const CheckpointerRequest *rightKey = (const CheckpointerRequest *)right;
    Assert(keysize == sizeof(CheckpointerRequest));

    /* we just care whether the result is 0 or not */
    if (FileTagMatchWithoutOpt((const void *)(&leftKey->ftag), (const void *)(&rightKey->ftag), sizeof(FileTag)) == 0) {
        if (leftKey->type == rightKey->type) {
            return 0;
        }
    }
    return 1;
}

uint32 BadBlockKeyHash(const void *key, Size keysize)
{
    BadBlockKey badBlockKey = *(const BadBlockKey *)key;
    badBlockKey.relfilenode.opt = DefaultFileNodeOpt;
    return DatumGetUInt32(hash_any((const unsigned char *)&badBlockKey, (int)keysize));
}

int BadBlockKeyMatch(const void *left, const void *right, Size keysize)
{
    const BadBlockKey *leftKey = (const BadBlockKey *)left;
    const BadBlockKey *rightKey = (const BadBlockKey *)right;
    Assert(keysize == sizeof(BadBlockKey));

    /* we just care whether the result is 0 or not */
    if (RelFileNodeEquals(leftKey->relfilenode, rightKey->relfilenode) && leftKey->forknum == rightKey->forknum &&
        leftKey->blocknum == rightKey->blocknum) {
        return 0;
    }

    return 1;
}

uint32 BadBlockHashKeyHash(const void *key, Size keysize)
{
    BadBlockHashKey badBlockHashKey = *(const BadBlockHashKey *)key;
    badBlockHashKey.relfilenode.opt = DefaultFileNodeOpt;
    return DatumGetUInt32(hash_any((const unsigned char *)&badBlockHashKey, (int)keysize));
}

int BadBlockHashKeyMatch(const void *left, const void *right, Size keysize)
{
    const BadBlockHashKey *leftKey = (const BadBlockHashKey *)left;
    const BadBlockHashKey *rightKey = (const BadBlockHashKey *)right;
    Assert(keysize == sizeof(BadBlockHashKey));

    /* we just care whether the result is 0 or not */
    if (RelFileNodeEquals(leftKey->relfilenode, rightKey->relfilenode) && leftKey->forknum == rightKey->forknum) {
        return 0;
    }

    return 1;
}

uint32 RepairBlockKeyHash(const void *key, Size keysize)
{
    RepairBlockKey repairBlockKey = *(const RepairBlockKey *)key;
    repairBlockKey.relfilenode.opt = DefaultFileNodeOpt;
    return DatumGetUInt32(hash_any((const unsigned char *)&repairBlockKey, (int)keysize));
}

int RepairBlockKeyMatch(const void *left, const void *right, Size keysize)
{
    const RepairBlockKey *leftKey = (const RepairBlockKey *)left;
    const RepairBlockKey *rightKey = (const RepairBlockKey *)right;
    Assert(keysize == sizeof(RepairBlockKey));

    /* we just care whether the result is 0 or not */
    if (RelFileNodeEquals(leftKey->relfilenode, rightKey->relfilenode) && leftKey->forknum == rightKey->forknum &&
        leftKey->blocknum == rightKey->blocknum) {
        return 0;
    }
    return 1;
}

uint32 RepairFileKeyHash(const void *key, Size keysize)
{
    RepairFileKey repairFileKey = *(const RepairFileKey *)key;
    repairFileKey.relfilenode.opt = DefaultFileNodeOpt;
    return DatumGetUInt32(hash_any((const unsigned char *)&repairFileKey, (int)keysize));
}

int RepairFileKeyMatch(const void *left, const void *right, Size keysize)
{
    const RepairFileKey *leftKey = (const RepairFileKey *)left;
    const RepairFileKey *rightKey = (const RepairFileKey *)right;
    Assert(keysize == sizeof(RepairFileKey));

    /* we just care whether the result is 0 or not */
    if (RelFileNodeEquals(leftKey->relfilenode, rightKey->relfilenode) && leftKey->forknum == rightKey->forknum &&
        leftKey->segno == rightKey->segno) {
        return 0;
    }
    return 1;
}
