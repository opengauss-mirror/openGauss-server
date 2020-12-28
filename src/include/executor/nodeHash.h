/* -------------------------------------------------------------------------
 *
 * nodeHash.h
 *	  prototypes for nodeHash.c
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeHash.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef NODEHASH_H
#define NODEHASH_H

#include "nodes/execnodes.h"
#include "nodes/relation.h"

#define MIN_HASH_BUCKET_SIZE 32768 /* min bucketsize for hash join */
#define BUCKET_OVERHEAD 8

extern HashState* ExecInitHash(Hash* node, EState* estate, int eflags);
extern TupleTableSlot* ExecHash(void);
extern Node* MultiExecHash(HashState* node);
extern void ExecEndHash(HashState* node);
extern void ExecReScanHash(HashState* node);

extern HashJoinTable ExecHashTableCreate(Hash* node, List* hashOperators, bool keepNulls);
extern void ExecHashTableDestroy(HashJoinTable hashtable);
extern void ExecHashTableInsert(HashJoinTable hashtable, TupleTableSlot* slot, uint32 hashvalue, int planid, int dop,
    Instrumentation* instrument = NULL);
extern bool ExecHashGetHashValue(HashJoinTable hashtable, ExprContext* econtext, List* hashkeys, bool outer_tuple,
    bool keep_nulls, uint32* hashvalue);
extern void ExecHashGetBucketAndBatch(HashJoinTable hashtable, uint32 hashvalue, int* bucketno, int* batchno);
extern bool ExecScanHashBucket(HashJoinState* hjstate, ExprContext* econtext);
extern void ExecPrepHashTableForUnmatched(HashJoinState* hjstate);
extern bool ExecScanHashTableForUnmatched(HashJoinState* hjstate, ExprContext* econtext);
extern void ExecHashTableReset(HashJoinTable hashtable);
extern void ExecHashTableResetMatchFlags(HashJoinTable hashtable);
extern void ExecChooseHashTableSize(double ntuples, int tupwidth, bool useskew, int* numbuckets, int* numbatches,
    int* num_skew_mcvs, int4 localWorkMem, bool vectorized = false, OpMemInfo* memInfo = NULL);
extern double ExecChooseHashTableMaxTuples(int tupwidth, bool useskew, bool vectorized, double hash_table_bytes);
extern int ExecHashGetSkewBucket(HashJoinTable hashtable, uint32 hashvalue);
extern void ExecHashTableStats(HashJoinTable hashtable, int planid);
extern int ExecSonicHashGetAtomTypeSize(Oid typeOid, int typeMod, bool isHashKey);
extern int64 ExecSonicHashGetAtomArrayBytes(
    double ntuples, int m_arrSize, int m_atomSize, int64 atomTypeSize, bool hasNullFlag);
extern void ExecChooseSonicHashTableSize(Path* inner_path, List* hashclauses, int* inner_width,
    bool isComplicateHashKey, int* numbuckets, int* numbatches, int4 localWorkMem, OpMemInfo* memInfo, int dop);
extern uint8 EstimateBucketTypeSize(int nbuckets);

#endif /* NODEHASH_H */
