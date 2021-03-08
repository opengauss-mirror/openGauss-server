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
 * cstore_vector.h
 *        batch buffer for bulk-loading, including
 *        1.  bulkload_block
 *        2.  bulkload_block_list
 *        3.  bulkload_minmax
 *        4.  bulkload_vector
 *        5.  bulkload_vector_iter
 *        6.  bulkload_rows
 *        7.  bulkload_rows_iter
 *
 *
 * IDENTIFICATION
 *        src/include/access/cstore_vector.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef GAUSS_CODE_SRC_INCLUDE_ACCESS_CSTORE_VECTOR_H
#define GAUSS_CODE_SRC_INCLUDE_ACCESS_CSTORE_VECTOR_H

#include "access/cstore_minmax_func.h"
#include "catalog/pg_attribute.h"
#include "vecexecutor/vectorbatch.h"

extern bool bulkload_memsize_reach_limitation(void);

/* Limitation about the total memory size of using blocks of one bulkload_rows object */
#define BULKLOAD_MAX_MEMSIZE (1024 * 1024 * 1024LL)

/* type declaration */
struct bulkload_block;
struct bulkload_block_list;
struct bulkload_minmax;
struct bulkload_datums;
struct bulkload_vector;
struct bulkload_vector_iter;
struct bulkload_rows;

struct MemInfoArg {
    /* record memory auto spread info. bulkload memory */
    int canSpreadmaxMem;
    int MemInsert;
    int MemSort;
    int spreadNum;
    int partitionNum;

    MemInfoArg()
    {
        canSpreadmaxMem = 0;
        MemInsert = 0;
        MemSort = 0;
        spreadNum = 0;
        partitionNum = 0;
    }
};

/* bulkload_block
 *   memory block for bulk loading.
 */
struct bulkload_block {
    struct bulkload_block *next; /* next bulkload_block */
    char *buff;                  /* memory buffer */
    int used;                    /* used  space by bytes */
    int total;                   /* total space by bytes */

    /* *data_unit* see data_unit() function */
    static bulkload_block *create(int block_size, int data_unit);
};

/* bulkload_block_list
 *  memory block list for bulk loading.
 */
struct bulkload_block_list {
    bulkload_block *m_head;    /* the first block */
    bulkload_block *m_current; /* the m_current block */
    int m_block_num;           /* block number */

    /*
     * for fixed length data type, all the blocks have the same block size;
     * for varied length data type, not all the block sizes are the same,
     * but we remember the minimum block size in m_block_size. if data size if
     * larger than m_block_size, the block to hold the data will adopt the block
     * size which is equal to data size.
     */
    int m_block_size;

    void init(Form_pg_attribute attr);
    void destroy();
    void expand(void);
    void expand(int needed);
    void reset(bool reuse_blocks);
    void configure(int attlen);

private:
    template <bool to_free>
    void reset_inner(void);
};

/* bulkload_minmax
 *  min/max info during bulk loading.
 */
struct bulkload_minmax {
    CompareDatum m_compare;              /* datum compare function */
    FinishCompareDatum m_finish_compare; /* datum compare finishing function */

    /* make min/max buffer address be 8 aligned,
     * because the first 8 bytes will be accessed as Datum pointer.
     * so place the two after function pointers, and MIN_MAX_LEN = 8*N .
     */
    char m_min_buf[MIN_MAX_LEN]; /* buffer for min value */
    char m_max_buf[MIN_MAX_LEN]; /* buffer for max value */

    int m_varstr_maxlen;  /* max length of variable length column */
    bool m_first_compare; /* indicate the first comparing action */

    void init(Oid atttypid);
    void reset(void);
    void configure(Oid atttypid);
};

/* bulkload_datums
 *  values/nulls info during bulk loading.
 */
struct bulkload_datums {
    Datum *m_vals_points; /* only for attlen > 8 or attlen < 0 */
    char *m_null_bitmap;  /* NULLs bitmap */
    int m_vals_num;       /* current number of values in m_vals_points including NULLs */
    bool m_has_null;      /* indicate at least one NULL exists */
    bool m_all_null;      /* indicate all are NULL */

    void init(Form_pg_attribute attr, int max_num);
    void destroy();
    void reset(int max_num);
    bool is_null(int which);
    void set_null(int which);
    Datum get_datum(int which) const;
};

/* bulkload_vector
 *   a batch of values for one column.
 */
struct bulkload_vector : public BaseObject {
    /* memory block info && m_append api */
    bulkload_block_list m_blocks;
    int m_attlen;

    /* function pointer */
    Datum (bulkload_vector::*m_decode)(ScalarVector *, int) const;
    Datum (bulkload_vector::*m_append)(bulkload_rows *, Datum, int);

    /* Values/Nulls info */
    bulkload_datums m_values_nulls;

    /* Min/Max info */
    bulkload_minmax m_minmax;

    /* ====== bulkload_vector API ====== */
    void init(Form_pg_attribute attr, int max_values);
    void configure(Form_pg_attribute attr);
    void reset(int max_values, bool reuse_blocks);
    void destroy(void);

    Size data_size(void);
    void data_copy(char *outbuf);

    void new_fixedsize_block(int data_unit);
    void choose_fixedsize_block(bulkload_rows *batch_rows, int data_unit);

    void new_varsize_block(int data_len);
    void choose_varsize_block(bulkload_rows *batch_rows, int data_len);

private:
    /* append functions for all datatype */
    Datum append_int8(bulkload_rows *batch_rows, Datum v, int len);
    Datum append_int16(bulkload_rows *batch_rows, Datum v, int len);
    Datum append_int32(bulkload_rows *batch_rows, Datum v, int len);
    Datum append_int64(bulkload_rows *batch_rows, Datum v, int len);
    Datum append_fixed_length(bulkload_rows *batch_rows, Datum v, int len);
    template <int varlen>
    Datum append_var_length(bulkload_rows *batch_rows, Datum v, int len);

    /* decode functions for all datatype */
    Datum decode_integer(ScalarVector *pvector, int rowIdx) const;
    Datum decode_fixed_length(ScalarVector *pvector, int rowIdx) const;
    Datum decode_var_length(ScalarVector *pvector, int rowIdx) const;
};

/* Vector Itor */
struct bulkload_vector_iter {
    void begin(bulkload_vector *vect, int max_values);
    void next(Datum *value, bool *null);
    bool not_end(void) const;

private:
    bulkload_vector *m_vector; /* current accessing vector */
    bulkload_block *m_block;   /* current accessing block */
    int m_block_pos;           /* position of current block */
    int m_cur_num;             /* current number of accessed values */
    int m_max_num;             /* max number of accessed values */

    /* next function point */
    void (bulkload_vector_iter::*m_next)(Datum *, bool *);

    /* next function */
    void next_int8(Datum *value, bool *null);
    void next_int16(Datum *value, bool *null);
    void next_int32(Datum *value, bool *null);
    void next_int64(Datum *value, bool *null);
    void next_var_length(Datum *value, bool *null);
};

/* batch rows for bulk loading */
struct bulkload_rows : public BaseObject {
    /* make all the used memory within m_context */
    MemoryContext m_context;

    /* we must control total memory size used by this object,
     * so m_using_blocks_total_rawsize will remember using memory size.
     */
    Size m_using_blocks_total_rawsize;
    Size m_using_blocks_init_rawsize;

    /* values vectors for each attribute/field */
    bulkload_vector *m_vectors;
    int m_attr_num;

    /* the current and max number of values to hold within each vector */
    int m_rows_maxnum;
    int m_rows_curnum;

    /* becuase buffer is lazy-created, so we can
     * 1. create all buffers if needed,
     * 2. re-create all buffers and reuse after the previous destroy.
     */
    bool m_inited;

    typedef bool (bulkload_rows::*FormAppendColumnFuncType)(TupleDesc tup_desc, VectorBatch *p_batch, int *start_idx);
    FormAppendColumnFuncType m_form_append_column_func;

    typedef Size (bulkload_rows::*FormSampleTupleSizeFuncType)(TupleDesc tup_desc, VectorBatch *p_batch,
                                                               int idx_sample);
    FormSampleTupleSizeFuncType m_form_sample_tuple_size_func;

    bool m_has_dropped_column;

    /* constructor and deconstructor */
    bulkload_rows(TupleDesc tuple_desc, int rows_maxnum, bool to_init = true);
    ~bulkload_rows()
    {
    }

    /* important:
     * Destroy is the common funtion to release all resources,
     * so don't change this function name.
     */
    void Destroy(void)
    {
        destroy();
    }

    void init(TupleDesc tup_desc, int rows_maxnum);
    void reset(bool reuse_blocks);
    bool append_one_vector(TupleDesc tup_desc, VectorBatch *p_batch, int *start_idx, MemInfoArg *m_memInfo = NULL);
    bool append_one_tuple(Datum *values, const bool *isnull, TupleDesc tup_desc);
    void append_one_column(Datum *values, const bool *isnull, int rows, TupleDesc tup_desc, int dest_idx);

    Size total_memory_size(void) const;

    /*
     * @Description: check whether row number reaches limitation.
     * @Return: true if row number reaches limitation; otherwise false.
     * @See also:
     */
    inline bool full_rownum(void)
    {
        return (m_rows_curnum == m_rows_maxnum);
    }

    /*
     * @Description: check whether rows size reaches limitation.
     * @Return: true if rows size reaches limitation; otherwise false.
     * @See also:
     */
    inline bool full_rowsize(void)
    {
        return (m_using_blocks_total_rawsize >= BULKLOAD_MAX_MEMSIZE);
    }

    Size calculate_tuple_size(TupleDesc tup_desc, Datum *tup_values, const bool *tup_nulls) const;

private:
    template <bool hasDroppedColumn>
    bool append_in_column_orientation(TupleDesc tup_desc, VectorBatch *p_batch, int *start_idx);
    bool append_in_row_orientation(TupleDesc tup_desc, VectorBatch *p_batch, int *start_idx);
    template <bool hasDroppedColumn>
    Size sample_tuple_size(TupleDesc tup_desc, VectorBatch *p_batch, int start_idx);
    void destroy(void);
};

/* itor for bulkload_rows */
struct bulkload_rows_iter {
    void begin(bulkload_rows *batch_rows);
    void next(Datum *values, bool *nulls);
    bool not_end(void) const;
    void end(void);

private:
    bulkload_rows *m_batch;
    bulkload_vector_iter *m_vec_iters;
};

extern void init_tid_attinfo(FormData_pg_attribute *tid_attr);
extern void init_tid_vector(bulkload_vector *vect, int max_values);
extern bulkload_rows *bulkload_indexbatch_init(int max_key_num, int max_values);
extern void bulkload_indexbatch_set_tids(bulkload_rows *index_batch, uint32 cuid, int copy_num);
extern void bulkload_indexbatch_copy(bulkload_rows *index_batch, int dest_idx, bulkload_rows *src, int src_idx);
extern void bulkload_indexbatch_copy_tids(bulkload_rows *index_batch, int dest_idx);
extern void bulkload_indexbatch_deinit(bulkload_rows *index_batch);

#endif /* GAUSS_CODE_SRC_INCLUDE_ACCESS_CSTORE_VECTOR_H */
