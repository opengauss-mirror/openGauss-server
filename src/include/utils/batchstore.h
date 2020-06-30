/* ---------------------------------------------------------------------------------------
 * 
 * batchstore.h
 *        Generalized routines for temporary tuple storage.
 * 
 * This module handles temporary storage of tuples for purposes such
 * as Materialize nodes, hashjoin batch files, etc.  It is essentially
 * a dumbed-down version of tuplesort.c; it does no sorting of tuples
 * but can only store and regurgitate a sequence of tuples.  However,
 * because no sort is required, it is allowed to start reading the sequence
 * before it has all been written.	This is particularly useful for cursors,
 * because it allows random access within the already-scanned portion of
 * a query without having to process the underlying scan to completion.
 * Also, it is possible to support multiple independent read pointers.
 *
 * A temporary file is used to handle the data if it exceeds the
 * space limit specified by the caller.
 *
 * Beginning in Postgres 8.2, what is stored is just MinimalTuples;
 * callers cannot expect valid system columns in regurgitated tuples.
 * Also, we have changed the API to return tuples in TupleTableSlots,
 * so that there is a check to prevent attempted access to system columns.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * 
 * IDENTIFICATION
 *        src/include/utils/batchstore.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef BATCHSTORE_H
#define BATCHSTORE_H

#include "executor/tuptable.h"
#include "vecexecutor/vecstore.h"

/* BatchStore is an opaque type whose details are not known outside
 * batchstore.cpp.
 */

/*
 * Possible states of a Tuplestore object.	These denote the states that
 * persist between calls of Tuplestore routines.
 */
typedef enum {
    BSS_INMEM,     /* batch still fit in memory */
    BSS_WRITEFILE, /* Writing to temp file */
    BSS_READFILE   /* Reading from temp file */
} BatchStoreStatus;

/*
 * State for a single read pointer.  If we are in state INMEM then all the
 * read pointers' "current" fields denote the read positions.  In state
 * WRITEFILE, the file/offset fields denote the read positions.  In state
 * READFILE, inactive read pointers have valid file/offset, but the active
 * read pointer implicitly has position equal to the temp file's seek position.
 *
 * Special case: if eof_reached is true, then the pointer's read position is
 * implicitly equal to the write position, and current/file/offset aren't
 * maintained.	This way we need not update all the read pointers each time
 * we write.
 */
typedef struct BSReadPointer {
    int eflags;       /* capability flags */
    bool eof_reached; /* read has reached EOF */
    int file;         /* temp file# */
    off_t offset;     /* byte offset in file */
} BSReadPointer;

class BatchStore : public VecStore {

public:
    // Memory context for main allocation
    MemoryContext m_storecontext;

    /*
     * enumerated value as shown above
     */
    BatchStoreStatus m_status;
    bool m_backward; /* store extra length words in file? */
    int m_eflags;    /* capability flags (OR of pointers' flags) */

    /*
     * remaining memory allowed, in bytes
     */
    int m_memtupdeleted;

    BufFile* m_myfile; /* underlying file, or NULL if none */

    ResourceOwner m_resowner; /* resowner for holding temp files */
    bool m_interXact;         /* keep open through transactions? */

    bool m_eofReached; /* reached EOF (needed for cursors) */

    /*
     * These variables are used to keep track of the current positions.
     *
     * In state WRITEFILE, the current file seek position is the write point;
     * in state READFILE, the write position is remembered in writepos_xxx.
     * (The write position is the same as EOF, but since BufFileSeek doesn't
     * currently implement SEEK_END, we have to remember it explicitly.)
     */
    BSReadPointer* m_readptrs; /* array of read pointers */
    int m_activeptr;           /* index of the active read pointer */
    int m_readptrcount;        /* number of pointers currently valid */
    int m_readptrsize;         /* allocated length of readptrs array */

    int writepos_file;     /* file# (valid if READFILE state) */
    off_t writepos_offset; /* offset (valid if READFILE state) */
    int m_markposOffset;   /* saved "current", or offset in tape block */
    int m_lastbatchnum;
    int m_lastfile_num;

    int m_lastwritebatchnum;
    off_t m_lastwritefile_offset;

    off_t m_lastfile_offset;
    bool m_windowagg_use; /* a flag used in windowagg */

    /*
     * Initialize variables.
     */

    void DumpMultiColumn();

    void GetBatch(bool forward, VectorBatch* batch, int batch_rows = BatchMaxSize);
    void PutBatch(VectorBatch* batch);

    void GetBatchInMemory(bool forward, VectorBatch* batch, int batch_rows = BatchMaxSize);

    void GetBatchReadFile(bool forward, VectorBatch* batch, int batch_rows = BatchMaxSize);
    void GetBatchWriteFile(bool forward, VectorBatch* batch);

    void PutBatchToMemory(MultiColumns* multiColumn);

    void PutBatchReadFile(MultiColumns* multiColumn);
    void PutBatchWriteFile(MultiColumns* multiColumn);

    void WriteMultiColumn(MultiColumns* stup);

    /*
     * Function to read a stored tuple from tape back into memory. 'len' is
     * the already-read length of the stored tuple.  Create a palloc'd copy,
     * initialize tuple/datum1/isnull1 in the target SortTuple struct, and
     * decrease state->availMem by the amount of memory space consumed.
     */
    void (*readMultiColumn)(BatchStore* state, MultiColumns& stup, unsigned int len);

#ifdef PGXC
    /*
     * Function to read length of next stored tuple.
     * Used as 'len' parameter for readtup function.
     */
    unsigned int (*getlen)(BatchStore* state, bool eofOK);
#endif
};

/*
 * Currently we only need to store MinimalTuples, but it would be easy
 * to support the same behavior for IndexTuples and/or bare Datums.
 */

extern BatchStore* batchstore_begin_heap(
    TupleDesc tupDesc, bool randomAccess, bool interXact, int64 maxKBytes, int maxMem = 0, int planId = 0, int dop = 1);

extern void batchstore_set_eflags(BatchStore* state, int eflags);

extern void batchstore_putbatch(BatchStore* state, VectorBatch* batch);

extern int batchstore_alloc_read_pointer(BatchStore* state, int eflags);

extern void batchstore_trim(BatchStore* state, bool from_memory);
extern void batchstore_restrpos(BatchStore* state);
extern void batchstore_markpos(BatchStore* state, bool from_mem);
extern bool batchstore_getbatch(BatchStore* state, bool forward, VectorBatch* batch);
extern bool batchstore_ateof(BatchStore* state);
extern void batchstore_rescan(BatchStore* state);
extern void batchstore_end(BatchStore* state);
extern void ReadMultiColumn(BatchStore* state, MultiColumns& multiColumn, unsigned int len);

unsigned int GetLen(BatchStore* state, bool eofOK);

#endif /* TUPLESTORE_H */
