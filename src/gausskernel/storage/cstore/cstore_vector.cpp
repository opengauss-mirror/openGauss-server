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
 * cstore_vector.cpp
 * @brief: batch buffer implement for bulk-loading, including
 *     1.  bulkload_block
 *     2.  bulkload_block_list
 *     3.  bulkload_minmax
 *     4.  bulkload_vector
 *     5.  bulkload_vector_iter
 *     6.  bulkload_rows
 *     7.  bulkload_rows_iter
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/cstore/cstore_vector.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "pgstat.h"
#include "executor/executor.h"
#include "access/cstore_vector.h"
#include "storage/item/itemptr.h"
#include "utils/gs_bitmap.h"
#include "utils/memutils.h"
#include "utils/memprot.h"
#include "workload/workload.h"

/*
 * Configure different block size for string and integer.
 * Block Size of int8   -> BULKLOAD_FIXED_BASE_BLOCKSZIE
 * Block Size of int16  -> BULKLOAD_FIXED_BASE_BLOCKSZIE * 2
 * Block Size of int32  -> BULKLOAD_FIXED_BASE_BLOCKSZIE * 4
 * Block Size of int64  -> BULKLOAD_FIXED_BASE_BLOCKSZIE * 8
 * Block Size of fixed length -> BULKLOAD_FIXED_BASE_BLOCKSZIE * length
 * Block Size of string -> BULKLOAD_VAR_BASE_BLOCKSZIE
 */
#define BULKLOAD_FIXED_BASE_BLOCKSZIE 1
#define BULKLOAD_VAR_BASE_BLOCKSZIE 8

/*
 * @Description: remember the current used size of given memory context
 * @IN memcxt: given memory context
 * @OUT old_size: used memory size
 * @See also: bulkload_memsize_increase()
 */
void bulkload_memsize_prepare(MemoryContext memcxt, Size* size)
{
    /* remember the current used size of given memory context */
    *size = AllocSetContextUsedSpace((AllocSetContext*)memcxt);
}

/*
 * @Description: record used memory size by this memory context
 * @IN memcxt: given memory context
 * @IN old_size: set by bulkload_memsize_prepare()
 * @See also: bulkload_memsize_decrease() bulkload_memsize_prepare()
 */
void bulkload_memsize_increase(MemoryContext memcxt, Size old_size)
{
    Size new_size = AllocSetContextUsedSpace((AllocSetContext*)memcxt);
    /* record incrementally used memory size by this memory context */
    t_thrd.cstore_cxt.bulkload_memsize_used += (new_size - old_size);
}

#define MACRO_MEMSIZE_DECREASE(_used_)                             \
    do {                                                           \
        if (t_thrd.cstore_cxt.bulkload_memsize_used >= (_used_)) { \
            /* forget memory size of the whole memory context */   \
            t_thrd.cstore_cxt.bulkload_memsize_used -= (_used_);   \
        } else {                                                   \
            t_thrd.cstore_cxt.bulkload_memsize_used = 0;           \
        }                                                          \
    } while (0)

/*
 * @Description: forget used memory size by this memory context, and then
 *    this memory context must be destroyed.
 * @IN memcxt: given memory context
 * @See also: bulkload_memsize_increase().
 */
void bulkload_memsize_decrease(MemoryContext memcxt)
{
    Size used_size = AllocSetContextUsedSpace((AllocSetContext*)memcxt);
    MACRO_MEMSIZE_DECREASE(used_size);
}

/*
 * @Description: forget used memory size by this memory chunk, and then
 *    this memory chunk must be freed.
 * @IN memptr: memory point to free
 * @See also: bulkload_memsize_decrease(memcxt)
 */
void bulkload_memsize_decrease(char* memptr)
{
    Size used_size = GetMemoryChunkSpace(memptr);
    MACRO_MEMSIZE_DECREASE(used_size);
}

/*
 * @Description: check whether reach the limitation of session memory size.
 * @Return: true if reach the limitation otherwise false.
 * @See also:
 */
bool bulkload_memsize_reach_limitation(void)
{
    return (t_thrd.cstore_cxt.bulkload_memsize_used >=
            (unsigned long)(unsigned int)u_sess->attr.attr_storage.partition_max_cache_size * 1024ULL);
}

/*
 * @Description: get data unit for different type attribute.
 *    in other words, we should treat a block as a array whose element size is data unit.
 * @IN attr: meta info about this attribute
 * @Return: (1) attribute length, if it's fixed length;
 *          (2) one byte otherwise;
 * @See also:
 */
static int data_unit(Form_pg_attribute attr)
{
    return (attr->attlen > 0) ? attr->attlen : (int)sizeof(char);
}

/*
 * @Description: create a new block
 * @IN u_sess->attr.attr_storage.block_size: block size
 * @IN data_unit: data unit
 * @Return: new block
 * @See also: data_unit()
 */
bulkload_block* bulkload_block::create(int block_size, int data_unit)
{
    /* first remember the current used memory size */
    Size cur_size = 0;
    bulkload_memsize_prepare(CurrentMemoryContext, &cur_size);

    /* new a block */
    bulkload_block* memblk = (bulkload_block*)palloc(sizeof(bulkload_block));

    memblk->buff = (char*)palloc(block_size);
    memblk->next = NULL;
    memblk->used = 0;
    /*  total is computed as: N * data_unit  */
    memblk->total = (block_size / data_unit) * data_unit;

    /* book-keep block size used */
    bulkload_memsize_increase(CurrentMemoryContext, cur_size);

    return memblk;
}

/*
 * @Description: configure block size
 * @IN attlen: attribute length info
 * @See also:
 */
void bulkload_block_list::configure(int attlen)
{
    if (attlen > 0) {
        /* = bulkload_fixed_base_blocksize * N */
        m_block_size = BULKLOAD_FIXED_BASE_BLOCKSZIE * 1024 * attlen;
        /*
         * Handle int type specially for MemoryContext usage rate.
         * If block_size is 4K, MemoryContext will allocate 8K in fact due to chunk header,
         * we don't use the extra 3K+. So it is better that block_size is 8K for integer.
         */
        if (attlen == 4)
            m_block_size = 8192;
    } else {
        /* = bulkload_var_base_blocksize * 1, default 8K */
        m_block_size = BULKLOAD_VAR_BASE_BLOCKSZIE * 1024;
    }
}

/*
 * @Description: initialize block list
 * @IN attr: attribute info
 * @See also:
 */
void bulkload_block_list::init(Form_pg_attribute attr)
{
    /* first configure block size */
    configure(attr->attlen);

    /* create the first block buffer */
    m_head = bulkload_block::create(m_block_size, data_unit(attr));
    m_block_num = 1;

    /* reset the other info */
    reset(true);
}

/*
 * @Description: destroy this block list.
 * @See also:
 */
void bulkload_block_list::destroy(void)
{
    bulkload_block* block = m_head;
    bulkload_block* last_block = NULL;

    while (block != NULL) {
        last_block = block;
        /* advance to the next block */
        block = block->next;

        /* free block's buffer and itself */
        pfree_ext(last_block->buff);
        pfree_ext(last_block);
    }

    m_head = NULL;
    m_current = NULL;
}

/*
 * @Description: reset block list.
 *    if memory size has reached the up limitation, all blocks (exclude head block)
 *    will be freed. otherwise just make all blocks empty.
 * @See also:
 */
template <bool to_free>
void bulkload_block_list::reset_inner(void)
{
    /* always keep head block alive, just make it usable */
    this->m_head->used = 0;

    /* handle the other blocks after head block */
    bulkload_block* nxt_block = this->m_head->next;
    bulkload_block* cur_block = NULL;

    while (nxt_block != NULL) {
        /* remember current block to handle */
        cur_block = nxt_block;
        /* get next block to handle */
        nxt_block = nxt_block->next;

        if (to_free) {
            /* forget their memory size before destroying */
            bulkload_memsize_decrease((char*)cur_block->buff);
            bulkload_memsize_decrease((char*)cur_block);

            /* release this block memory */
            pfree_ext(cur_block->buff);
            pfree_ext(cur_block);
        } else {
            /* make it usable/empty */
            cur_block->used = 0;
            /* keep buff + next + total unchanged */
        }
    }

    if (to_free) {
        this->m_current = this->m_head;
        /* there isn't any available block, so set next be NULL. */
        this->m_head->next = NULL;
        this->m_block_num = 1;
        /* keep m_block_size unchanged */
    } else {
        this->m_current = this->m_head;
        /* keep m_block_num + m_block_size unchanged */
    }
}

/*
 * @Description: reset block list.
 *    if memory size has reached the up limitation, all blocks (exclude head block)
 *    will be freed. otherwise just make all blocks empty.
 * @See also: reset_inner()
 */
void bulkload_block_list::reset(bool reuse_blocks)
{
    if (reuse_blocks) {
        /* just make all blocks empty/unused including head block */
        reset_inner<false>();
    } else {
        /* free all blocks excluding head block */
        reset_inner<true>();
    }
}

/*
 * @Description: initialize min/max info
 * @IN atttypid: attribute type OID info
 * @See also:
 */
void bulkload_minmax::init(Oid atttypid)
{
    configure(atttypid);
    reset();
}

/*
 * @Description: reset min/max info
 * @See also:
 */
void bulkload_minmax::reset(void)
{
    errno_t rc = EOK;

    /* prepare for next reusing */
    m_first_compare = true;
    m_varstr_maxlen = 0;
    rc = memset_s(m_min_buf, MIN_MAX_LEN, 0, MIN_MAX_LEN);
    securec_check(rc, "\0", "\0");
    rc = memset_s(m_max_buf, MIN_MAX_LEN, 0, MIN_MAX_LEN);
    securec_check(rc, "\0", "\0");
    /* don't change function pointer */
}

/*
 * @Description: configure compare functions
 * @IN atttypid: attribute type OID info
 * @See also:
 */
void bulkload_minmax::configure(Oid atttypid)
{
    m_compare = GetCompareDatumFunc(atttypid);
    m_finish_compare = GetFinishCompareDatum(atttypid);
}

/*
 * @Description: initialize Values[]/Nulls[] for one attribute.
 * @IN attr: attribute info about this column
 * @IN max_num: max number of values
 * @See also:
 */
void bulkload_datums::init(Form_pg_attribute attr, int max_num)
{
    /* first remember the current used memory size */
    Size cur_size = 0;
    bulkload_memsize_prepare(CurrentMemoryContext, &cur_size);

    /* null bitmap always be created because its biggest size is
     * only 8KB. So reduce IF judge in set_null() method.
     */
    m_null_bitmap = (char*)palloc(bitmap_size(max_num));

    if (attr->attlen < 0 || attr->attlen > (int)sizeof(Datum)) {
        /* need extra Datum array to storage pointers */
        m_vals_points = (Datum*)palloc(sizeof(Datum) * max_num);
    } else {
        m_vals_points = NULL;
    }

    reset(max_num);

    /* book-keep block size used */
    bulkload_memsize_increase(CurrentMemoryContext, cur_size);
}

/*
 * @Description: destroy Values[]/Nulls[] for one attribute.
 * @See also:
 */
void bulkload_datums::destroy(void)
{
    if (m_null_bitmap) {
        pfree(m_null_bitmap);
        m_null_bitmap = NULL;
    }

    if (m_vals_points) {
        pfree(m_vals_points);
        m_vals_points = NULL;
    }
}

/*
 * @Description: reset Values[]/Nulls[] for one attribute.
 * @IN max_num: max values' number in null bitmap
 * @See also:
 */
void bulkload_datums::reset(int max_num)
{
    /* needn't to reset m_vals_points */
    m_vals_num = 0;

    /* reset null bitmap */
    Size bpsize = bitmap_size(max_num);
    errno_t rc = memset_s(m_null_bitmap, bpsize, 0, bpsize);
    securec_check(rc, "\0", "\0");
    m_has_null = false;
    m_all_null = true;
}

/*
 * @Description: query one value is null or not
 * @IN which: which value
 * @Return: null or not
 * @See also: set_null()
 */
bool bulkload_datums::is_null(int which)
{
    return bitmap_been_set(m_null_bitmap, which);
}

/*
 * @Description: set null for one value
 * @IN which: which value
 * @See also: is_null()
 */
void bulkload_datums::set_null(int which)
{
    Assert(m_null_bitmap);
    bitmap_set(m_null_bitmap, which);
    /* Must plus 1 because get_datum() require this action */
    ++m_vals_num;
    m_has_null = true;
}

/*
 * @Description: get memory point to this value.
 *    this function is only for attlen > 8 or attlen < 0
 * @IN which: which value
 * @Return: memory point to this value
 * @See also:
 */
Datum bulkload_datums::get_datum(int which) const
{
    return PointerGetDatum(m_vals_points[which]);
}

/*
 * @Description: configure vector info
 * @IN attr: attribute info about this column
 * @See also:
 */
void bulkload_vector::configure(Form_pg_attribute attr)
{
    m_attlen = attr->attlen;
    if ((int)sizeof(int8) == attr->attlen) {
        /* set functions for int8 */
        m_append = &bulkload_vector::append_int8;
        m_decode = &bulkload_vector::decode_integer;
    } else if ((int)sizeof(int16) == attr->attlen) {
        /* set functions for int16 */
        m_append = &bulkload_vector::append_int16;
        m_decode = &bulkload_vector::decode_integer;
    } else if ((int)sizeof(int32) == attr->attlen) {
        /* set functions for int32 */
        m_append = &bulkload_vector::append_int32;
        m_decode = &bulkload_vector::decode_integer;
    } else if ((int)sizeof(int64) == attr->attlen) {
        /* set functions for int64 */
        m_append = &bulkload_vector::append_int64;
        m_decode = &bulkload_vector::decode_integer;
    } else if (attr->attlen > (int)sizeof(Datum)) {
        /* set functions for the other fixed-length data-type */
        m_append = &bulkload_vector::append_fixed_length;
        m_decode = &bulkload_vector::decode_fixed_length;
    } else if (-1 == attr->attlen) {
        /* set functions for varlen data-type */
        m_append = &bulkload_vector::append_var_length<-1>;
        m_decode = &bulkload_vector::decode_var_length;
    } else if (-2 == attr->attlen) {
        /* set functions for varlen data-type */
        m_append = &bulkload_vector::append_var_length<-2>;
        m_decode = &bulkload_vector::decode_var_length;
    } else {
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), (errmsg("Unsupported data type in column storage"))));
    }
}

/*
 * @Description:
 * @IN attr: attribute info about this column
 * @IN max_values: max number of values to hold
 * @See also:
 */
void bulkload_vector::init(Form_pg_attribute attr, int max_num_of_values)
{
    /* initialize block list */
    m_blocks.init(attr);
    /* initialize min/max info */
    m_minmax.init(attr->atttypid);
    /* initialize values/nulls info */
    m_values_nulls.init(attr, max_num_of_values);
    /* initialize the others */
    configure(attr);
}

/*
 * @Description: get all the data size in this vector.
 * @Return: all data size.
 * @See also:
 */
Size bulkload_vector::data_size(void)
{
    Size total = 0;
    bulkload_block* block = m_blocks.m_head;

    /* scan all the blocks and get the total size. */
    while (block != NULL) {
        Assert(block->used <= block->total);
        total += block->used;
        block = block->next;
    }

    return total;
}

/*
 * @Description: copy all the data in this vector
 * @OUT outbuf: buffer to copy in whose size is >= data_size()
 * @See also: bulkload_vector::data_size()
 */
void bulkload_vector::data_copy(char* outbuf)
{
    bulkload_block* block = m_blocks.m_head;
    errno_t rc = EOK;

    /* scan all the blocks and copy all the data. */
    while (block != NULL) {
        Assert(block->used <= block->total);
        if (block->used > 0) {
            /* data_size() ensure that outbuf is large enough to hold all values */
            rc = memcpy_s(outbuf, block->used, block->buff, block->used);
            securec_check(rc, "", "");
        }
        outbuf += block->used;
        block = block->next;
    }
}

/*
 * @Description: reset this vector
 * @IN max_num: max number of values to hold
 * @IN reuse_blocks: flag to resue memory blocks.
 *       if false, blocks excluding head block will be freed.
 * @See also:
 */
void bulkload_vector::reset(int max_num, bool reuse_blocks)
{
    /* reset block list */
    m_blocks.reset(reuse_blocks);
    /* reset values/nulls info */
    m_values_nulls.reset(max_num);
    /* reset min/max info */
    m_minmax.reset();
}

/*
 * @Description: destroy this vector
 * @See also:
 */
void bulkload_vector::destroy(void)
{
    /* destroy block list */
    m_blocks.destroy();
    /* destroy values/nulls info */
    m_values_nulls.destroy();
}

/*
 * @Description: choose a block to hold one fixed-length value
 * @IN data_unit: see data_unit()
 * @IN memcxt: given memory context to allocate new block
 * @See also:
 */
void bulkload_vector::choose_fixedsize_block(bulkload_rows* batch_rows, int data_unit)
{
    if (m_blocks.m_current->used < m_blocks.m_current->total) {
        /* use current block */
        Assert(data_unit <= m_blocks.m_current->total - m_blocks.m_current->used);
        return;
    }

    if (m_blocks.m_current->next) {
        /* use existing block */
        m_blocks.m_current = m_blocks.m_current->next;
    } else {
        /* use a new block */
        MemoryContext oldMemCnxt = MemoryContextSwitchTo(batch_rows->m_context);
        new_fixedsize_block(data_unit);
        (void)MemoryContextSwitchTo(oldMemCnxt);
    }

    /* update using memory size */
    batch_rows->m_using_blocks_total_rawsize += m_blocks.m_block_size;
}

/*
 * @Description: new a block to hold new fixed-length value
 * @IN data_unit: see data_unit()
 * @See also:
 */
void bulkload_vector::new_fixedsize_block(int data_unit)
{
    Assert(m_blocks.m_current->next == NULL);
    Assert(m_blocks.m_block_size > 0);

    m_blocks.m_current->next = bulkload_block::create(m_blocks.m_block_size, data_unit);
    /* advance to the tail */
    m_blocks.m_current = m_blocks.m_current->next;
    m_blocks.m_block_num++;
}

/*
 * @Description: choose a block to hold one var-length value
 * @IN data_len: data real length
 * @IN memcxt: given memory context to allocate new block
 * @See also:
 */
void bulkload_vector::choose_varsize_block(bulkload_rows* batch_rows, int data_len)
{
    Assert(data_len > 0);

    bulkload_block* currblk = m_blocks.m_current;
    if (currblk->total - currblk->used >= data_len) {
        /* current block can hold this value */
        return;
    }

    bulkload_block* nextblk = m_blocks.m_current->next;
    bulkload_block* prevblk = m_blocks.m_current;
    while (nextblk != NULL) {
        Assert(nextblk->used == 0);
        if (nextblk->total >= data_len) {
            /* an existing block found can hold this value
             *
             * detach this block from list
             */
            prevblk->next = nextblk->next;
            /* insert this new block after current block */
            nextblk->next = currblk->next;
            currblk->next = nextblk;

            /* update current point */
            m_blocks.m_current = nextblk;

            /* update using memory size */
            batch_rows->m_using_blocks_total_rawsize += nextblk->total;

            return;
        }

        prevblk = nextblk;
        nextblk = nextblk->next;
    }

    /* new a block that can hold this value */
    MemoryContext oldMemCnxt = MemoryContextSwitchTo(batch_rows->m_context);
    if (data_len <= m_blocks.m_block_size) {
        new_varsize_block(m_blocks.m_block_size);

        /* update using memory size */
        batch_rows->m_using_blocks_total_rawsize += m_blocks.m_block_size;
    } else {
        /* Here maybe a defect exists:
         * We believe that most values of the same attribute have the similar
         * length, so that all the blocks can be reused again.
         * But if this assumption don't work, maybe some blocks are always
         * free and not reused again.
         */
        new_varsize_block(data_len);

        /* update using memory size */
        batch_rows->m_using_blocks_total_rawsize += data_len;
    }
    (void)MemoryContextSwitchTo(oldMemCnxt);
}

/*
 * @Description: new a block to hold one var-length value
 * @IN data_len: data real length
 * @See also:
 */
void bulkload_vector::new_varsize_block(int data_len)
{
    bulkload_block* newblk = bulkload_block::create(data_len, (int)sizeof(char));

    /* insert this new block after current block */
    newblk->next = m_blocks.m_current->next;
    m_blocks.m_current->next = newblk;

    /* point to the new block */
    m_blocks.m_current = newblk;
    m_blocks.m_block_num++;
}

/*
 * @Description: append function for int8 data-type
 * @IN batch_rows: batchrows object
 * @IN len: length of this value
 * @IN v: this value
 * @Return: Datum represent about this value
 * @See also:
 */
Datum bulkload_vector::append_int8(bulkload_rows* batch_rows, Datum v, int len)
{
    /* choose a block which can hold this value */
    choose_fixedsize_block(batch_rows, (int)sizeof(int8));

    /* append int8 value */
    bulkload_block* blk = m_blocks.m_current;
    *(int8*)(blk->buff + blk->used) = (int8)DatumGetChar(v);
    blk->used += (int)sizeof(int8);

    /* update all-nulls flag */
    m_values_nulls.m_all_null = false;

    Assert((int)sizeof(int8) == len);
    return (v);
}

/*
 * @Description: append function for int16 data-type
 * @IN batch_rows: batchrows object
 * @IN len: length of this value
 * @IN v: this value
 * @Return: Datum represent about this value
 * @See also:
 */
Datum bulkload_vector::append_int16(bulkload_rows* batch_rows, Datum v, int len)
{
    /* choose a block which can hold this value */
    choose_fixedsize_block(batch_rows, (int)sizeof(int16));

    /* append int16 value */
    bulkload_block* blk = m_blocks.m_current;
    *(int16*)(blk->buff + blk->used) = (int16)DatumGetInt16(v);
    blk->used += (int)sizeof(int16);

    /* update all-nulls flag */
    m_values_nulls.m_all_null = false;

    Assert((int)sizeof(int16) == len);
    return v;
}

/*
 * @Description: append function for int32 data-type
 * @IN batch_rows: batchrows object
 * @IN len: length of this value
 * @IN v: this value
 * @Return: Datum represent about this value
 * @See also:
 */
Datum bulkload_vector::append_int32(bulkload_rows* batch_rows, Datum v, int len)
{
    /* choose a block which can hold this value */
    choose_fixedsize_block(batch_rows, (int)sizeof(int32));

    /* append int32 value */
    bulkload_block* blk = m_blocks.m_current;
    *(int32*)(blk->buff + blk->used) = (int32)DatumGetInt32(v);
    blk->used += (int)sizeof(int32);

    /* update all-nulls flag */
    m_values_nulls.m_all_null = false;

    Assert((int)sizeof(int32) == len);
    return v;
}

/*
 * @Description: append function for int64 data-type
 * @IN batch_rows: batchrows object
 * @IN len: length of this value
 * @IN v: this value
 * @Return: Datum represent about this value
 * @See also:
 */
Datum bulkload_vector::append_int64(bulkload_rows* batch_rows, Datum v, int len)
{
    /* choose a block which can hold this value */
    choose_fixedsize_block(batch_rows, (int)sizeof(int64));

    /* append int64 value */
    bulkload_block* blk = m_blocks.m_current;
    *(int64*)(blk->buff + blk->used) = (int64)DatumGetInt64(v);
    blk->used += (int)sizeof(int64);

    /* update all-nulls flag */
    m_values_nulls.m_all_null = false;

    Assert((int)sizeof(int64) == len);
    return v;
}

/*
 * @Description: append function for fixed length data-type
 * @IN batch_rows: batchrows object
 * @IN len: length of this value
 * @IN v: this value
 * @Return: Datum represent about this value
 * @See also:
 */
Datum bulkload_vector::append_fixed_length(bulkload_rows* batch_rows, Datum v, int len)
{
    /* choose a block which can hold this value */
    Assert(len < m_blocks.m_block_size);
    Assert(len > (int)sizeof(Datum));
    choose_fixedsize_block(batch_rows, len);

    /* append fixed length value */
    bulkload_block* blk = m_blocks.m_current;
    char* ptr = blk->buff + blk->used;
    errno_t rc = memcpy_s(ptr, (blk->total - blk->used), DatumGetPointer(v), len);
    securec_check(rc, "", "");
    blk->used += len;

    /* update all-nulls flag */
    m_values_nulls.m_all_null = false;
    m_values_nulls.m_vals_points[m_values_nulls.m_vals_num++] = PointerGetDatum(ptr);

    return PointerGetDatum(ptr);
}

/*
 * @Description: append function for var-length data-type
 * @IN batch_rows: batchrows object
 * @IN len: length of this value
 * @IN v: this value
 * @Return: Datum represent about this value
 * @See also:
 */
template <int varlen>
Datum bulkload_vector::append_var_length(bulkload_rows* batch_rows, Datum v, int len)
{
    char* data = DatumGetPointer(v);
    char* ptr = NULL;
    int data_len = (varlen == -1) ? VARSIZE_ANY(data) : (1 + strlen(data));

    if (varlen == -1 && VARATT_IS_HUGE_TOAST_POINTER(data)) {
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmsg("Un-support clob/blob type more than 1GB for cstore")));
    }
    /* choose a block which can hold this string */
    this->choose_varsize_block(batch_rows, data_len);

    /* append var length value */
    bulkload_block* blk = this->m_blocks.m_current;
    Assert(blk->total - blk->used >= data_len);
    ptr = blk->buff + blk->used;
    errno_t rc = memcpy_s(ptr, (blk->total - blk->used), DatumGetPointer(v), data_len);
    securec_check(rc, "", "");
    blk->used += data_len;

    /* update all-nulls flag */
    this->m_values_nulls.m_all_null = false;
    this->m_values_nulls.m_vals_points[this->m_values_nulls.m_vals_num++] = PointerGetDatum(ptr);

    return PointerGetDatum(ptr);
}

/*
 * @Description: decode function for integer data-type,
 * @IN pvector: scalar vector
 * @IN row_idx: which value
 * @Return: datum value
 * @See also:
 */
Datum bulkload_vector::decode_integer(ScalarVector* pvector, int row_idx) const
{
    return pvector->m_vals[row_idx];
}

/*
 * @Description: decode function for fixed length data-type,
 *    this datum has head, and skip it.
 * @IN pvector: scalar vector
 * @IN row_idx: which value
 * @Return: datum value
 * @See also:
 */
Datum bulkload_vector::decode_fixed_length(ScalarVector* pvector, int row_idx) const
{
    Datum v = ScalarVector::Decode(pvector->m_vals[row_idx]);
    return PointerGetDatum((char*)v + VARHDRSZ_SHORT);
}

/*
 * @Description: decode function for var-length data-type
 * @IN pvector: scalar vector
 * @IN row_idx: which value
 * @Return: datum value
 * @See also:
 */
Datum bulkload_vector::decode_var_length(ScalarVector* pvector, int row_idx) const
{
    return ScalarVector::Decode(pvector->m_vals[row_idx]);
}

/*
 * @Description: init attribute info of TID data-type
 * @OUT tid_attr: TID attribute info
 * @See also: GetPsortTupleDesc(); ItemPointerData
 */
void init_tid_attinfo(FormData_pg_attribute* tid_attr)
{
    /* set all required meta-data for TID  */
    errno_t rc = memset_s(tid_attr, sizeof(FormData_pg_attribute), 0, sizeof(FormData_pg_attribute));
    securec_check(rc, "\0", "\0");

    /* use a Datum to store one CTID value.
     * becuase only 48 bits are used, so we treat it as a int64 value.
     * see 1. GetPsortTupleDesc() codes; 2. ItemPointerData
     */
    tid_attr->atttypid = INT8OID;
    tid_attr->attlen = (int)sizeof(Datum);
    tid_attr->attbyval = true;
}

/*
 * @Description: initialize TID vector
 * @IN max_values: how many TIDs to hold
 * @IN/OUT vect: TID vector
 * @See also: init_tid_attinfo()
 */
void init_tid_vector(bulkload_vector* vect, int max_num_of_values)
{
    FormData_pg_attribute tid_attr;

    /* initialize attribute info for TID type */
    init_tid_attinfo(&tid_attr);

    /* initialize TID vector */
    vect->init(&tid_attr, max_num_of_values);
}

/*
 * @Description: get the special TID vector within index batch buffer
 * @IN index_batch: batch buffer for index
 * @Return: the special TID vector
 * @See also: bulkload_indexbatch_init()
 */
static inline bulkload_vector* bulkload_indexbatch_get_tid_vector(bulkload_rows* index_batch)
{
    return (index_batch->m_vectors - 1);
}

/*
 * @Description: create batch buffer for index
 * @IN max_key_num: the max number of index keys
 * @IN max_values: how many values to hold within one batch
 * @Return: batch buffer for index
 * @See also: bulkload_indexbatch_deinit()
 */
bulkload_rows* bulkload_indexbatch_init(int max_key_num, int max_num_of_values)
{
    /* plus TID system attribute */
    int max_num_plus_tid = max_key_num + 1;

    /*
     * memory struct
     * 0. bulkload_rows struct
     * 1. (bulkload_vector struct) special for TID vector
     * 2. (bulkload_vector struct) * ( max_num_plus_tid )
     */
    char* buf = (char*)palloc0(sizeof(bulkload_rows) + sizeof(bulkload_vector) * (max_num_plus_tid + 1));

    bulkload_rows* index_batch = (bulkload_rows*)buf;
    index_batch->m_context = CurrentMemoryContext;
    /* set the vector's max number, and it will be updated in running time */
    index_batch->m_attr_num = max_num_plus_tid;
    index_batch->m_rows_curnum = 0;
    index_batch->m_rows_maxnum = max_num_of_values;

    /* special TID vector for reusing */
    bulkload_vector* tid_vector = (bulkload_vector*)(buf + sizeof(bulkload_rows));
    init_tid_vector(tid_vector, max_num_of_values);

    /* these are all reused vectors including TID vector.
     * because TID vector is always the last, and we keep a copy in front,
     * so we can copy directly and use it.
     */
    index_batch->m_vectors = tid_vector + 1;
    index_batch->m_inited = true;

    return index_batch;
}

/*
 * @Description: destroy batch buffer for index
 * @IN index_batch: batch buffer for index
 * @See also: bulkload_indexbatch_init()
 */
void bulkload_indexbatch_deinit(bulkload_rows* index_batch)
{
    /* TID vector has its private resource, so destroy it */
    bulkload_indexbatch_get_tid_vector(index_batch)->destroy();
    pfree(index_batch);
}

/*
 * @Description: shallow copy and set column data for indexing key
 * @IN dest_idx: the position of destination vector
 * @OUT index_batch: batch buffer for index
 * @IN src: source batch buffer for indexing key
 * @IN src_idx: the position of source vector
 * @See also: bulkload_indexbatch_copy_tids()
 */
void bulkload_indexbatch_copy(bulkload_rows* index_batch, int dest_idx, bulkload_rows* src, int src_idx)
{
    /* copy simple struct */
    errno_t rc = memcpy_s((char*)(index_batch->m_vectors + dest_idx),
                          sizeof(bulkload_vector),
                          (char*)(src->m_vectors + src_idx),
                          sizeof(bulkload_vector));
    securec_check(rc, "", "");
}

/*
 * @Description: set all TIDs for this index batch.
 * @IN cuid: this CU id
 * @IN/OUT index_batch: batch buffer for index
 * @IN num: how many TIDs to append
 * @See also: bulkload_indexbatch_copy_tids()
 */
void bulkload_indexbatch_set_tids(bulkload_rows* index_batch, uint32 cuid, int num)
{
    Assert(num <= index_batch->m_rows_maxnum);

    /* first reset TID vector */
    bulkload_vector* tid_vector = bulkload_indexbatch_get_tid_vector(index_batch);
    tid_vector->reset(num, true);

    /* set TIDs number */
    index_batch->m_rows_curnum = num;

    /* form the CTID data according to CU ID.
     * Note that itemPtr->offset start from 1.
     */
    for (int offset = 1; offset <= num; ++offset) {
        Datum tid = 0;
        ItemPointer itemPtr = (ItemPointer) & tid;
        ItemPointerSet(itemPtr, cuid, offset);

        /* append this TID into its vector */
        (void)(tid_vector->*(tid_vector->m_append))(index_batch, tid, sizeof(Datum));
    }
}

/*
 * @Description: set TIDs data for each index, and TID vector
 *     always is the last vector/attribute/column of index batch.
 * @IN dest_idx: the position of TID vector.
 * @OUT index_batch: batch buffer for index
 * @See also: bulkload_indexbatch_copy()
 */
void bulkload_indexbatch_copy_tids(bulkload_rows* index_batch, int dest_idx)
{
    /* copy simple struct */
    errno_t rc = memcpy_s((char*)(index_batch->m_vectors + dest_idx),
                          sizeof(bulkload_vector),
                          (char*)bulkload_indexbatch_get_tid_vector(index_batch),
                          sizeof(bulkload_vector));
    securec_check(rc, "", "");
}

/*
 * @Description: iteration next function for int8 data-type
 * @OUT null:  null flag
 * @OUT value: datum value
 * @See also:
 */
void bulkload_vector_iter::next_int8(Datum* value, bool* null)
{
    if (unlikely(m_vector->m_values_nulls.is_null(m_cur_num))) {
        /* set NULL flag */
        *null = true;
    } else if (m_block_pos < m_block->total) {
        *null = false;
        *value = Int8GetDatum(*(int8*)(m_block->buff + m_block_pos));
        /* prepare for next value */
        m_block_pos += sizeof(int8);
    } else {
        *null = false;
        /* move block point at advance */
        m_block = m_block->next;
        *value = Int8GetDatum(*(int8*)(m_block->buff));
        /* prepare for next value */
        m_block_pos = sizeof(int8);
    }
    ++m_cur_num;
}

/*
 * @Description: iteration next function for int16 data-type
 * @OUT null: null flag
 * @OUT value: datum value
 * @See also:
 */
void bulkload_vector_iter::next_int16(Datum* value, bool* null)
{
    if (unlikely(m_vector->m_values_nulls.is_null(m_cur_num))) {
        /* set NULL flag */
        *null = true;
    } else if (m_block_pos < m_block->total) {
        *null = false;
        *value = Int16GetDatum(*(int16*)(m_block->buff + m_block_pos));
        /* prepare for next value */
        m_block_pos += sizeof(int16);
    } else {
        *null = false;
        /* move block point at advance */
        m_block = m_block->next;
        *value = Int16GetDatum(*(int16*)(m_block->buff));
        /* prepare for next value */
        m_block_pos = sizeof(int16);
    }
    ++m_cur_num;
}

/*
 * @Description: iteration next function for int32 data-type
 * @OUT null: null flag
 * @OUT value: datum value
 * @See also:
 */
void bulkload_vector_iter::next_int32(Datum* value, bool* null)
{
    if (unlikely(m_vector->m_values_nulls.is_null(m_cur_num))) {
        /* set NULL flag */
        *null = true;
    } else if (m_block_pos < m_block->total) {
        *null = false;
        *value = Int32GetDatum(*(int32*)(m_block->buff + m_block_pos));
        /* prepare for next value */
        m_block_pos += sizeof(int32);
    } else {
        *null = false;
        /* move block point at advance */
        m_block = m_block->next;
        *value = Int32GetDatum(*(int32*)(m_block->buff));
        /* prepare for next value */
        m_block_pos = sizeof(int32);
    }
    ++m_cur_num;
}

/*
 * @Description: iteration next function for int64 data-type
 * @OUT null: null flag
 * @OUT value: datum value
 * @See also:
 */
void bulkload_vector_iter::next_int64(Datum* value, bool* null)
{
    if (unlikely(m_vector->m_values_nulls.is_null(m_cur_num))) {
        /* set NULL flag */
        *null = true;
    } else if (m_block_pos < m_block->total) {
        *null = false;
        *value = Int64GetDatum(*(int64*)(m_block->buff + m_block_pos));
        /* prepare for next value */
        m_block_pos += sizeof(int64);
    } else {
        *null = false;
        /* move block point at advance */
        m_block = m_block->next;
        *value = Int64GetDatum(*(int64*)(m_block->buff));
        /* prepare for next value */
        m_block_pos = sizeof(int64);
    }
    ++m_cur_num;
}

/*
 * @Description: iteration next function for data-type whose len < 0 or len not in (1,2,4,8)
 * @OUT null: null flag
 * @OUT value: datum value
 * @See also:
 */
void bulkload_vector_iter::next_var_length(Datum* value, bool* null)
{
    if (unlikely(m_vector->m_values_nulls.is_null(m_cur_num))) {
        /* set NULL flag */
        *null = true;
    } else {
        *null = false;
        /* directly return the formed memory address */
        *value = m_vector->m_values_nulls.get_datum(m_cur_num);
    }
    /* prepare for next value */
    ++m_cur_num;
}

/*
 * @Description: begin to scan a bulkload_vector object
 * @IN max_values: max number of values to hold
 * @IN vect: bulkload_vector object to scan
 * @See also:
 */
void bulkload_vector_iter::begin(bulkload_vector* vect, int max_num_of_values)
{
    /* set current/max number of accessed values */
    m_max_num = max_num_of_values;
    m_cur_num = 0;

    /* set accessing vector + block + block position */
    m_vector = vect;
    m_block = vect->m_blocks.m_head;
    m_block_pos = 0;

    /* set next function for different data-type */
    if ((int)sizeof(int8) == vect->m_attlen) {
        m_next = &bulkload_vector_iter::next_int8;
    } else if ((int)sizeof(int16) == vect->m_attlen) {
        m_next = &bulkload_vector_iter::next_int16;
    } else if ((int)sizeof(int32) == vect->m_attlen) {
        m_next = &bulkload_vector_iter::next_int32;
    } else if ((int)sizeof(int64) == vect->m_attlen) {
        m_next = &bulkload_vector_iter::next_int64;
    } else if (vect->m_attlen > (int)sizeof(Datum) || vect->m_attlen < 0) {
        /* for the other attribute (m_attlen > 8),
         * the following next function will be used.
         */
        m_next = &bulkload_vector_iter::next_var_length;
    } else {
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), (errmsg("Unsupported data type in column storage"))));
    }
}

/*
 * @Description: get the next attribute from bulkload_vector object
 * @OUT nulls:  next attribute' null flag
 * @OUT values: next attribute' value
 * @See also:
 */
void bulkload_vector_iter::next(Datum* value, bool* null)
{
    (this->*m_next)(value, null);
}

/*
 * @Description: check to reach the end of scanning
 * @Return: true if reaching the end of scanning
 * @See also:
 */
bool bulkload_vector_iter::not_end(void) const
{
    return (m_cur_num < m_max_num);
}

/*
 * @Description: bulkload_rows constructor
 * @IN rows_maxnum: max number of values to hold
 * @IN to_init: initialize flag because of lazy-initialize
 * @IN tuple_desc: tuple description
 * @See also:
 */
bulkload_rows::bulkload_rows(TupleDesc tuple_desc, int rows_maxnum, bool to_init)
{
    /* the two are initialized by init() method */
    m_context = NULL;
    m_vectors = NULL;
    m_using_blocks_init_rawsize = 0;
    m_using_blocks_total_rawsize = 0;

    m_attr_num = tuple_desc->natts;
    m_rows_maxnum = rows_maxnum;
    m_rows_curnum = 0;
    m_inited = false;
    m_form_append_column_func = NULL;
    m_form_sample_tuple_size_func = NULL;
    m_has_dropped_column = false;

    if (to_init) {
        init(tuple_desc, rows_maxnum);
        Assert(m_inited);
    }
}

/*
 * @Description: initialize batchrows object
 * @IN rows_maxnum: max number of values to hold
 * @IN tup_desc: tuple description
 * @See also:
 */
void bulkload_rows::init(TupleDesc tup_desc, int rows_maxnum)
{
    if (!m_inited) {
        int attr_num = tup_desc->natts;
        MemoryContext old_context = NULL;

        /* caller must switch to the expected memory context,
         * and then initialize these batch buffers.
         */
        m_context = AllocSetContextCreate(CurrentMemoryContext,
                                          "CSTORE BULKLOAD_ROWS",
                                          ALLOCSET_DEFAULT_MINSIZE,
                                          ALLOCSET_DEFAULT_INITSIZE,
                                          ALLOCSET_DEFAULT_MAXSIZE);

        old_context = MemoryContextSwitchTo(m_context);

        m_rows_maxnum = rows_maxnum;
        m_attr_num = attr_num;
        m_rows_curnum = 0;

        Size cur_size = 0;
        /* first remember the current used memory size */
        bulkload_memsize_prepare(m_context, &cur_size);
        m_vectors = (bulkload_vector*)palloc(sizeof(bulkload_vector) * attr_num);
        /* book-keep block size used */
        bulkload_memsize_increase(m_context, cur_size);

        bulkload_vector* vector = m_vectors;
        FormData_pg_attribute* attrs = tup_desc->attrs;
        m_has_dropped_column = false;
        for (int i = 0; i < attr_num; ++i) {
            if ((*attrs).attisdropped)
                m_has_dropped_column = true;
            /* initialize each vector */
            vector->init(attrs, rows_maxnum);
            /* compute the total memory size of all head blocks */
            m_using_blocks_init_rawsize += vector->m_blocks.m_block_size;
            ++vector;
            ++attrs;
        }

        if (m_has_dropped_column) {
            m_form_append_column_func = &bulkload_rows::append_in_column_orientation<true>;
            m_form_sample_tuple_size_func = &bulkload_rows::sample_tuple_size<true>;
        } else {
            m_form_append_column_func = &bulkload_rows::append_in_column_orientation<false>;
            m_form_sample_tuple_size_func = &bulkload_rows::sample_tuple_size<false>;
        }

        /* update initialized flag */
        m_inited = true;
        m_using_blocks_total_rawsize = m_using_blocks_init_rawsize;

        (void)MemoryContextSwitchTo(old_context);
    }
}

/*
 * @Description: reset bulkload_rows object.
 * @IN reuse_blocks: decide to free all holding memory blocks.
 *       a temp object should set true during its current period of fetching values,
 *       and then set false after finishing.
 *       in the other common conditions, just set true.
 * @See also:
 */
void bulkload_rows::reset(bool reuse_blocks)
{
    /* reset using blocks' total size */
    m_using_blocks_total_rawsize = m_using_blocks_init_rawsize;

    for (int i = 0; i < m_attr_num; ++i) {
        /* reset each vector */
        m_vectors[i].reset(m_rows_maxnum, reuse_blocks);
    }

    /* reset rows' number */
    m_rows_curnum = 0;
}

/*
 * @Description: destroy this object
 * @See also:
 */
void bulkload_rows::destroy(void)
{
    /* destroy my private memory context */
    bulkload_memsize_decrease(m_context);
    MemoryContextDelete(m_context);

    /* reset necessary values */
    m_context = NULL;
    m_vectors = NULL;
    m_using_blocks_init_rawsize = 0;
    m_using_blocks_total_rawsize = 0;
    m_inited = false;
}

/*
 * @Description: get the total memory size including all
 *   data size and struct itself.
 * @Return: total memory size
 * @See also:
 */
Size bulkload_rows::total_memory_size(void) const
{
    return sizeof(bulkload_rows) + static_cast<int>(m_inited) ? (((AllocSetContext*)m_context)->totalSpace) : 0;
}

/*
 * @Description: append one tuple into batchrows
 * @IN isnull: attributes' null flags
 * @IN tup_desc: tuple description
 * @IN values: attributes' values
 * @Return: true if row number or total memory reaches its limitation.
 * @See also:
 */
bool bulkload_rows::append_one_tuple(Datum* values, const bool* isnull, TupleDesc tup_desc)
{
    Assert(m_attr_num == tup_desc->natts);
    Assert(m_rows_curnum < m_rows_maxnum);

    bulkload_vector* vector = m_vectors;
    FormData_pg_attribute* attrs = tup_desc->attrs;

    /* append one tuple */
    for (int attrIdx = 0; attrIdx < m_attr_num; ++attrIdx) {
        if (unlikely(isnull[attrIdx])) {
            /* append a NULL */
            vector->m_values_nulls.set_null(m_rows_curnum);
        } else {
            /* append this value into vector */
            Datum v = (vector->*(vector->m_append))(this, values[attrIdx], (*attrs).attlen);

            /* compare for min/max values */
            vector->m_minmax.m_compare(vector->m_minmax.m_min_buf,
                                       vector->m_minmax.m_max_buf,
                                       v,
                                       &(vector->m_minmax.m_first_compare),
                                       &(vector->m_minmax.m_varstr_maxlen));
        }

        /* advance to the next attribute */
        ++vector;
        ++attrs;
    }

    /* update rows' number */
    ++m_rows_curnum;

    return full_rownum() || full_rowsize();
}

/*
 * @Description: append one vector into batchrows
 * @IN p_batch: vector batch
 * @IN/OUT start_idx: starting index, it will be updated.
 * @IN tup_desc: tuple description
 * @Return: true if row number or total memory reaches its limitation.
 * @See also:
 */
bool bulkload_rows::append_one_vector(TupleDesc tup_desc, VectorBatch* p_batch, int* start_idx, MemInfoArg* m_memInfo)
{
    Assert(p_batch->m_rows > 0);

    if (*start_idx < p_batch->m_rows) {
        /* we use the first tuple, the last tuple, and a random tuple as the sampling one to determine the average size.
         */
        Size first_tuple_size = (this->*m_form_sample_tuple_size_func)(tup_desc, p_batch, *start_idx);
        Size last_tuple_size = (this->*m_form_sample_tuple_size_func)(tup_desc, p_batch, (p_batch->m_rows - 1));
        int random_pos = (((unsigned int)random()) % (p_batch->m_rows - *start_idx)) + *start_idx;
        Size random_tuple_size = (this->*m_form_sample_tuple_size_func)(tup_desc, p_batch, random_pos);
        Size tuple_size = MaxTriple(first_tuple_size, last_tuple_size, random_tuple_size);
        if (m_memInfo != NULL && m_memInfo->MemInsert > 0 &&
            ((unsigned long)(unsigned int)m_memInfo->MemInsert < m_using_blocks_total_rawsize / 1024ULL)) {
            int64 spreadMem = Min(dywlm_client_get_memory() * 1024L, m_memInfo->MemInsert * 1024L) / 1024;
            if (spreadMem > m_memInfo->MemInsert * 0.1) {
                m_memInfo->MemInsert += spreadMem;
                m_memInfo->spreadNum++;
                AllocSet context = (AllocSet)m_context->parent;
                context->maxSpaceSize += spreadMem * 1024L;

                MEMCTL_LOG(DEBUG2,
                           "CStoreInsert(Batch) auto mem spread %ldKB succeed, and work mem is %dKB, spreadNum is %d.",
                           spreadMem,
                           m_memInfo->MemInsert,
                           m_memInfo->spreadNum);
            } else {
                MEMCTL_LOG(LOG,
                           "CStoreInsert(Batch) auto mem spread %ldKB failed, and work mem is %dKB.",
                           spreadMem,
                           m_memInfo->MemInsert);
                return true;
            }
        }

        if ((m_using_blocks_total_rawsize < BULKLOAD_MAX_MEMSIZE) &&
            ((BULKLOAD_MAX_MEMSIZE - m_using_blocks_total_rawsize) / (p_batch->m_rows - *start_idx) > tuple_size)) {
            return (this->*m_form_append_column_func)(tup_desc, p_batch, start_idx);
        } else {
            return append_in_row_orientation(tup_desc, p_batch, start_idx);
        }
    } else {
        /* fast-path to judge whether p_batch is handled over */
        return full_rownum() || full_rowsize();
    }
}

/*
 * @Description: append one column in datums. Since the outer logic has
 *     ensured that rows will not exceed the limitation of bulkload_rows,
 *     so we need to return nothing here.
 * @IN hasNull: if has the null values
 * @IN values: array of datums to append
 * @IN isnull: array of null flag
 * @IN rows: number of datums to append
 * @IN tup_desc: tuple description
 * @IN dest_idx: the target column to append
 * @See also:
 */
void bulkload_rows::append_one_column(Datum* values, const bool* isnull, int rows, TupleDesc tup_desc, int dest_idx)
{
    /* for normal columns */
    if (dest_idx >= 0) {
        Assert(tup_desc);
        bulkload_vector* vector = m_vectors + dest_idx;
        int attlen = tup_desc->attrs[dest_idx].attlen;

        /* handle a batch of values for one attribute */
        for (int rowCnt = 0; rowCnt < rows; ++rowCnt) {
            if (likely(isnull && isnull[rowCnt])) {
                /* append a NULL */
                vector->m_values_nulls.set_null(m_rows_curnum + rowCnt);
            } else {
                /* append this value into vector */
                Datum v = (vector->*(vector->m_append))(this, values[rowCnt], attlen);

                /* compare for min/max values */
                vector->m_minmax.m_compare(vector->m_minmax.m_min_buf,
                                           vector->m_minmax.m_max_buf,
                                           v,
                                           &(vector->m_minmax.m_first_compare),
                                           &(vector->m_minmax.m_varstr_maxlen));
            }
        }
    } else {
        /* for tid column */
        bulkload_vector* tid_vector = bulkload_indexbatch_get_tid_vector(this);
        int attlen = sizeof(Datum);
        tid_vector->reset(rows, true);

        for (int rowCnt = 0; rowCnt < rows; ++rowCnt) {
            (void)(tid_vector->*(tid_vector->m_append))(this, values[rowCnt], attlen);
        }

        m_rows_curnum += rows;
    }
}

/*
 * @Description: sample one tuple and guess the average tuple size
 * @IN p_batch: batch tuples
 * @IN idx_sample: which tuple to sample
 * @IN tup_desc: tuple description
 * @Return: the average tuple size
 * @See also:
 */
template <bool hasDroppedColumn>
Size bulkload_rows::sample_tuple_size(TupleDesc tup_desc, VectorBatch* p_batch, int idx_sample)
{
    ScalarVector* scalar_vector = p_batch->m_arr;
    bulkload_vector* vector = m_vectors;
    FormData_pg_attribute* attrs = tup_desc->attrs;
    Size tup_size = 0;

    /* compute the sampling tuple' size */
    for (int i = 0; i < tup_desc->natts; ++i) {
        if (hasDroppedColumn && (*attrs).attisdropped) {
            /* advance to the next attribute */
            ++scalar_vector;
            ++vector;
            ++attrs;
            continue;
        }

        if ((*attrs).attlen > 0) {
            /* datum struct: len-B value */
            tup_size += (*attrs).attlen;
        } else if (!scalar_vector->IsNull(idx_sample) && (*attrs).attlen == -1) {
            /* datum struct: 4B var-header + varlen-B value */
            Assert((*attrs).attlen == -1);
            Datum v = (vector->*(vector->m_decode))(scalar_vector, idx_sample);
            tup_size += VARSIZE_ANY(DatumGetPointer(v));
        } else if (!scalar_vector->IsNull(idx_sample) && (*attrs).attlen == -2) {
            Assert((*attrs).attlen == -2);
            Datum v = (vector->*(vector->m_decode))(scalar_vector, idx_sample);
            tup_size += strlen(DatumGetPointer(v)) + 1;
        }
        /* advance to the next attribute */
        ++scalar_vector;
        ++vector;
        ++attrs;
    }
    return tup_size;
}

/*
 * @Description: calculate one tuple size
 * @IN tup_values: value array
 * @IN tup_nulls: null array
 * @IN tup_desc: tuple description
 * @Return: the tuple size
 * @See also:
 */
Size bulkload_rows::calculate_tuple_size(TupleDesc tup_desc, Datum* tup_values, const bool* tup_nulls) const
{
    FormData_pg_attribute* attrs = tup_desc->attrs;
    Size tup_size = 0;

    /* compute the sampling tuple' size */
    for (int attrIdx = 0; attrIdx < tup_desc->natts; attrIdx++) {
        if ((*attrs).attisdropped) {
            /* advance to the next attribute */
            ++attrs;
            continue;
        }

        if ((*attrs).attlen > 0) {
            /* datum struct: len-B value */
            tup_size += (*attrs).attlen;
        } else if (!tup_nulls[attrIdx] && (*attrs).attlen == -1) {
            /* datum struct: 4B var-header + varlen-B value */
            Assert((*attrs).attlen == -1);
            tup_size += VARSIZE_ANY(DatumGetPointer(tup_values[attrIdx]));
        } else if (!tup_nulls[attrIdx] && (*attrs).attlen == -2) {
            Assert((*attrs).attlen == -2);
            tup_size += strlen(DatumGetPointer(tup_values[attrIdx])) + 1;
        }
        /* advance to the next attribute */
        ++attrs;
    }
    return tup_size;
}

/*
 * @Description: append one vector in row orientation.
 * @IN p_batch: one vectorbatch values
 * @IN/OUT start_idx: starting position
 * @IN tup_desc: tuple description
 * @Return: whether this batchrow is full
 * @See also:
 */
bool bulkload_rows::append_in_row_orientation(TupleDesc tup_desc, VectorBatch* p_batch, int* start_idx)
{
    Assert(p_batch);
    Assert(m_attr_num == tup_desc->natts);

    /* nothing to do if they are equal to. */
    Assert(m_rows_curnum <= m_rows_maxnum);
    Assert(*start_idx <= p_batch->m_rows);

    int const nattrs = tup_desc->natts;
    int srcRowIdx = *start_idx;
    int destRowIdx = m_rows_curnum;
    int const maxRows = Min(m_rows_maxnum - m_rows_curnum, p_batch->m_rows - srcRowIdx);
    int row_count = 0;

    /* append one tuple each loop. */
    while (row_count < maxRows) {
        ScalarVector* scalar_vector = p_batch->m_arr;
        bulkload_vector* vector = m_vectors;
        FormData_pg_attribute* attrs = tup_desc->attrs;

        Size tuple_size = (this->*m_form_sample_tuple_size_func)(tup_desc, p_batch, srcRowIdx);
        if ((BULKLOAD_MAX_MEMSIZE - m_using_blocks_total_rawsize) < tuple_size) {
            m_rows_curnum += row_count;
            *start_idx += row_count;
            return true;
        }

        for (int attrIdx = 0; attrIdx < nattrs; ++attrIdx) {
            if (!scalar_vector->IsNull(srcRowIdx)) {
                /* decode from scalar vector */
                Datum vector_value = (vector->*(vector->m_decode))(scalar_vector, srcRowIdx);

                /* append this value into vector */
                vector_value = (vector->*(vector->m_append))(this, vector_value, (*attrs).attlen);

                /* compare for min/max values */
                vector->m_minmax.m_compare(vector->m_minmax.m_min_buf,
                                           vector->m_minmax.m_max_buf,
                                           vector_value,
                                           &(vector->m_minmax.m_first_compare),
                                           &(vector->m_minmax.m_varstr_maxlen));
            } else {
                /* append a NULL */
                vector->m_values_nulls.set_null(destRowIdx);
            }

            /* advance to the next attribute */
            ++scalar_vector;
            ++vector;
            ++attrs;
        }
        ++row_count;

        if (!full_rowsize()) {
            /* move to the next tuple/record */
            ++srcRowIdx;
            ++destRowIdx;
        } else {
            break;
        }
    }

    /* update rows number and handling cursor */
    m_rows_curnum += row_count;
    *start_idx += row_count;

    return full_rownum() || full_rowsize();
}

/*
 * @Description: append one vector in column orientation.
 * @IN p_batch: one vectorbatch values
 * @IN/OUT start_idx: starting position
 * @IN tup_desc: tuple description
 * @Return: whether this batchrow is full
 * @See also:
 */
template <bool hasDroppedColumn>
bool bulkload_rows::append_in_column_orientation(TupleDesc tup_desc, VectorBatch* p_batch, int* start_idx)
{
    Assert(p_batch);
    Assert(m_attr_num == tup_desc->natts);
    Assert(m_rows_curnum < m_rows_maxnum);

    int const nattrs = tup_desc->natts;
    int const maxRows = Min(m_rows_maxnum - m_rows_curnum, p_batch->m_rows - *start_idx);

    ScalarVector* scalar_vector = p_batch->m_arr;
    bulkload_vector* vector = m_vectors;
    FormData_pg_attribute* attrs = tup_desc->attrs;

    for (int attrIdx = 0; attrIdx < nattrs; ++attrIdx) {
        if (hasDroppedColumn && (*attrs).attisdropped) {
            /* advance to the next attribute */
            /* for droped columns, set all null */
            int destRowIdx = m_rows_curnum;
            for (int rowCnt = 0; rowCnt < maxRows; ++rowCnt) {
                vector->m_values_nulls.set_null(destRowIdx++);
            }
            ++scalar_vector;
            ++vector;
            ++attrs;
            continue;
        }
        int srcRowIdx = *start_idx;
        int destRowIdx = m_rows_curnum;

        /* handle a batch of values for one attribute */
        for (int rowCnt = 0; rowCnt < maxRows; ++rowCnt) {
            if (unlikely(scalar_vector->IsNull(srcRowIdx))) {
                /* append a NULL */
                vector->m_values_nulls.set_null(destRowIdx);
            } else {
                /* decode from scalar vector */
                Datum value = (vector->*(vector->m_decode))(scalar_vector, srcRowIdx);

                /* append this value into vector */
                value = (vector->*(vector->m_append))(this, value, (*attrs).attlen);

                /* compare for min/max values */
                vector->m_minmax.m_compare(vector->m_minmax.m_min_buf,
                                           vector->m_minmax.m_max_buf,
                                           value,
                                           &(vector->m_minmax.m_first_compare),
                                           &(vector->m_minmax.m_varstr_maxlen));
            }

            /* move to the next value for this attribute */
            ++srcRowIdx;
            ++destRowIdx;
        }
        /* advance to the next attribute */
        ++scalar_vector;
        ++vector;
        ++attrs;
    }

    /* update rows number and handling cursor */
    m_rows_curnum += maxRows;
    *start_idx += maxRows;

    return full_rownum() || full_rowsize();
}

/*
 * @Description: begin to scan a bulkload_rows object
 * @IN batch_rows: bulkload_rows object to scan
 * @See also:
 */
void bulkload_rows_iter::begin(bulkload_rows* batch_rows)
{
    /* m_attr_num is the actual attributes' number to scan */
    int attr_num = batch_rows->m_attr_num;
    m_batch = batch_rows;

    /* each itor for each vector */
    m_vec_iters = (bulkload_vector_iter*)palloc(attr_num * sizeof(bulkload_vector_iter));

    bulkload_vector_iter* iter = m_vec_iters;
    bulkload_vector* vector = batch_rows->m_vectors;
    for (int i = 0; i < attr_num; ++i) {
        /* each vector iteration prepares to scan */
        (iter++)->begin(vector++, batch_rows->m_rows_curnum);
    }
}

/*
 * @Description: get the next tuple from bulkload_rows object
 * @OUT nulls:  next tuple's null flag
 * @OUT values: next tuple's value
 * @See also:
 */
void bulkload_rows_iter::next(Datum* values, bool* nulls)
{
    bulkload_vector_iter* iter = m_vec_iters;

    for (int i = 0; i < m_batch->m_attr_num; ++i) {
        /* get the next tuple from each vector iteration */
        (iter++)->next(values++, nulls++);
    }
}

/*
 * @Description: check to reach the end of scanning
 * @Return: true if reaching the end of scanning
 * @See also:
 */
bool bulkload_rows_iter::not_end(void) const
{
    return m_vec_iters->not_end();
}

/*
 * @Description: finish this scan
 * @See also:
 */
void bulkload_rows_iter::end(void)
{
    pfree_ext(m_vec_iters);
}
