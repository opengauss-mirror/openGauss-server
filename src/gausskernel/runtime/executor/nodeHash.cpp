/* -------------------------------------------------------------------------
 *
 * nodeHash.cpp
 *	  Routines to hash relations for hashjoin
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeHash.cpp
 *
 * -------------------------------------------------------------------------
 *
 * INTERFACE ROUTINES
 *		MultiExecHash	- generate an in-memory hash table of the relation
 *		ExecInitHash	- initialize node and subnodes
 *		ExecEndHash		- shutdown node and subnodes
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <math.h>
#include <limits.h>
#include "access/hash.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_statistic.h"
#include "commands/tablespace.h"
#include "executor/exec/execdebug.h"
#include "executor/hashjoin.h"
#include "executor/node/nodeHash.h"
#include "executor/node/nodeHashjoin.h"
#include "miscadmin.h"
#include "optimizer/clauses.h"
#include "optimizer/streamplan.h"
#include "pgstat.h"
#include "pgxc/pgxc.h"
#include "instruments/instr_unique_sql.h"
#include "utils/anls_opt.h"
#include "utils/dynahash.h"
#include "utils/lsyscache.h"
#include "utils/memprot.h"
#include "utils/memutils.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"
#include "vecexecutor/vechashtable.h"
#include "vectorsonic/vsonicarray.h"
#include "vectorsonic/vsonichash.h"
#include "workload/workload.h"

static void ExecHashIncreaseNumBatches(HashJoinTable hashtable);
static void ExecHashBuildSkewHash(HashJoinTable hashtable, Hash* node, int mcvsToUse);
static void ExecHashSkewTableInsert(HashJoinTable hashtable, TupleTableSlot* slot, uint32 hashvalue, int bucketNumber);
static void ExecHashRemoveNextSkewBucket(HashJoinTable hashtable);
static void ExecHashIncreaseBuckets(HashJoinTable hashtable);

static void* dense_alloc(HashJoinTable hashtable, Size size);
/* ----------------------------------------------------------------
 *		ExecHash
 *
 *		stub for pro forma compliance
 * ----------------------------------------------------------------
 */
TupleTableSlot* ExecHash(void)
{
    ereport(ERROR,
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
            errmodule(MOD_EXECUTOR),
            errmsg("Hash node does not support ExecProcNode call convention")));
    return NULL;
}

/* ----------------------------------------------------------------
 *		MultiExecHash
 *
 *		build hash table for hashjoin, doing partitioning if more
 *		than one batch is required.
 * ----------------------------------------------------------------
 */
Node* MultiExecHash(HashState* node)
{
    PlanState* outerNode = NULL;
    List* hashkeys = NIL;
    HashJoinTable hashtable;
    TupleTableSlot* slot = NULL;
    ExprContext* econtext = NULL;
    uint32 hashvalue;
    TimestampTz start_time = 0;

    /* must provide our own instrumentation support */
    if (node->ps.instrument) {
        InstrStartNode(node->ps.instrument);
        node->hashtable->spill_size = &node->ps.instrument->sorthashinfo.spill_size;
    } else {
        node->hashtable->spill_size = &node->spill_size;
    }

    /* init unique sql hash state if needed*/
    UpdateUniqueSQLHashStats(NULL, &start_time);

    /*
     * get state info from node
     */
    outerNode = outerPlanState(node);
    hashtable = node->hashtable;

    /*
     * set expression context
     */
    hashkeys = node->hashkeys;
    econtext = node->ps.ps_ExprContext;

    /*
     * get all inner tuples and insert into the hash table (or temp files)
     */
    WaitState oldStatus = pgstat_report_waitstatus(STATE_EXEC_HASHJOIN_BUILD_HASH);
    for (;;) {
        slot = ExecProcNode(outerNode);
        if (TupIsNull(slot))
            break;
        /* We have to compute the hash value */
        econtext->ecxt_innertuple = slot;
        if (ExecHashGetHashValue(hashtable, econtext, hashkeys, false, hashtable->keepNulls, &hashvalue)) {
            int bucketNumber;

            bucketNumber = ExecHashGetSkewBucket(hashtable, hashvalue);
            if (bucketNumber != INVALID_SKEW_BUCKET_NO) {
                /* It's a skew tuple, so put it into that hash table */
                ExecHashSkewTableInsert(hashtable, slot, hashvalue, bucketNumber);
            } else {
                /* Not subject to skew optimization, so insert normally */
                ExecHashTableInsert(hashtable,
                    slot,
                    hashvalue,
                    node->ps.plan->plan_node_id,
                    SET_DOP(node->ps.plan->dop),
                    node->ps.instrument);
            }
            hashtable->totalTuples += 1;
        }
    }
    (void)pgstat_report_waitstatus(oldStatus);

    /* analysis hash table information created in memory */
    if (anls_opt_is_on(ANLS_HASH_CONFLICT))
        ExecHashTableStats(hashtable, node->ps.plan->plan_node_id);

    /* analyze hash table information for unique sql hash state */
    UpdateUniqueSQLHashStats(hashtable, &start_time);


    /* must provide our own instrumentation support */
    if (node->ps.instrument) {
        InstrStopNode(node->ps.instrument, hashtable->totalTuples);
        node->ps.instrument->sorthashinfo.nbatch = hashtable->nbatch;
        node->ps.instrument->sorthashinfo.nbuckets = hashtable->nbuckets;
        node->ps.instrument->sorthashinfo.nbatch_original = hashtable->nbatch_original;
        node->ps.instrument->sorthashinfo.spacePeak = hashtable->spacePeak;
        if (hashtable->width[0] > 0) {
            hashtable->width[1] = hashtable->width[1] / hashtable->width[0];
            hashtable->width[0] = -1;
        }
        node->ps.instrument->width = (int)hashtable->width[1];
        node->ps.instrument->sysBusy = hashtable->causedBySysRes;
        node->ps.instrument->spreadNum = hashtable->spreadNum;
    }

    /*
     * We do not return the hash table directly because it's not a subtype of
     * Node, and so would violate the MultiExecProcNode API.  Instead, our
     * parent Hashjoin node is expected to know how to fish it out of our node
     * state.  Ugly but not really worth cleaning up, since Hashjoin knows
     * quite a bit more about Hash besides that.
     */
    return NULL;
}

/* ----------------------------------------------------------------
 *		ExecInitHash
 *
 *		Init routine for Hash node
 * ----------------------------------------------------------------
 */
HashState* ExecInitHash(Hash* node, EState* estate, int eflags)
{
    HashState* hashstate = NULL;

    /* check for unsupported flags */
    Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

    /*
     * create state structure
     */
    hashstate = makeNode(HashState);
    hashstate->ps.plan = (Plan*)node;
    hashstate->ps.state = estate;
    hashstate->hashtable = NULL;
    hashstate->hashkeys = NIL; /* will be set by parent HashJoin */

    /*
     * Miscellaneous initialization
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &hashstate->ps);

    /*
     * initialize our result slot
     */
    ExecInitResultTupleSlot(estate, &hashstate->ps);

    /*
     * initialize child expressions
     */
    hashstate->ps.targetlist = (List*)ExecInitExpr((Expr*)node->plan.targetlist, (PlanState*)hashstate);
    hashstate->ps.qual = (List*)ExecInitExpr((Expr*)node->plan.qual, (PlanState*)hashstate);

    /*
     * initialize child nodes
     */
    outerPlanState(hashstate) = ExecInitNode(outerPlan(node), estate, eflags);

    /*
     * initialize tuple type. no need to initialize projection info because
     * this node doesn't do projections
     */
    TupleDesc resultDesc = ExecGetResultType(outerPlanState(hashstate));
    ExecAssignResultTypeFromTL(&hashstate->ps, resultDesc->tdTableAmType);

    hashstate->ps.ps_ProjInfo = NULL;

    Assert(hashstate->ps.ps_ResultTupleSlot->tts_tupleDescriptor->tdTableAmType != TAM_INVALID);

    return hashstate;
}

/* ---------------------------------------------------------------
 *		ExecEndHash
 *
 *		clean up routine for Hash node
 * ----------------------------------------------------------------
 */
void ExecEndHash(HashState* node)
{
    PlanState* outerPlan = NULL;

    /*
     * free exprcontext
     */
    ExecFreeExprContext(&node->ps);

    /*
     * shut down the subplan
     */
    outerPlan = outerPlanState(node);
    ExecEndNode(outerPlan);
}

/* ----------------------------------------------------------------
 *		ExecHashTableCreate
 *
 *		create an empty hashtable data structure for hashjoin.
 * ----------------------------------------------------------------
 */
HashJoinTable ExecHashTableCreate(Hash* node, List* hashOperators, bool keepNulls)
{
    HashJoinTable hashtable;
    Plan* outerNode = NULL;
    int nbuckets;
    int nbatch;
    int num_skew_mcvs;
    int log2_nbuckets;
    int nkeys;
    int i;
    int64 local_work_mem = SET_NODEMEM(node->plan.operatorMemKB[0], node->plan.dop);
    int64 max_mem = (node->plan.operatorMaxMem > 0) ? SET_NODEMEM(node->plan.operatorMaxMem, node->plan.dop) : 0;
    ListCell* ho = NULL;
    MemoryContext oldcxt;

    /*
     * Get information about the size of the relation to be hashed (it's the
     * "outer" subtree of this node, but the inner relation of the hashjoin).
     * Compute the appropriate size of the hash table.
     */
    outerNode = outerPlan(node);

    ExecChooseHashTableSize(PLAN_LOCAL_ROWS(outerNode) / SET_DOP(node->plan.dop),
        outerNode->plan_width,
        OidIsValid(node->skewTable),
        &nbuckets,
        &nbatch,
        &num_skew_mcvs,
        local_work_mem);

    /*
     * If we allows mem auto spread, we should set nbatch to 1 to avoid disk
     * spill if estimation from optimizer differs from that from executor
     */
    if (node->plan.operatorMaxMem > 0 && nbatch > 1 && nbuckets < INT_MAX / nbatch) {
        if (nbuckets * nbatch < (int)(MaxAllocSize / sizeof(HashJoinTuple))) {
            nbuckets *= nbatch;
            nbatch = 1;
        }
    }

#ifdef HJDEBUG
    printf("nbatch = %d, nbuckets = %d\n", nbatch, nbuckets);
#endif

    /* nbuckets must be a power of 2 */
    log2_nbuckets = my_log2(nbuckets);
    Assert(nbuckets == (1 << log2_nbuckets));

    /*
     * Initialize the hash table control block.
     *
     * The hashtable control block is just palloc'd from the executor's
     * per-query memory context.
     */
    hashtable = (HashJoinTable)palloc(sizeof(HashJoinTableData));
    hashtable->nbuckets = nbuckets;
    hashtable->log2_nbuckets = log2_nbuckets;
    hashtable->buckets = NULL;
    hashtable->keepNulls = keepNulls;
    hashtable->skewEnabled = false;
    hashtable->skewBucket = NULL;
    hashtable->skewBucketLen = 0;
    hashtable->nSkewBuckets = 0;
    hashtable->skewBucketNums = NULL;
    hashtable->nbatch = nbatch;
    hashtable->curbatch = 0;
    hashtable->nbatch_original = nbatch;
    hashtable->nbatch_outstart = nbatch;
    hashtable->growEnabled = true;
    hashtable->totalTuples = 0;
    hashtable->innerBatchFile = NULL;
    hashtable->outerBatchFile = NULL;
    hashtable->spaceUsed = 0;
    hashtable->spacePeak = 0;
    hashtable->spill_count = 0;
    hashtable->spaceAllowed = local_work_mem * 1024L;

    hashtable->spaceUsedSkew = 0;
    hashtable->spaceAllowedSkew = hashtable->spaceAllowed * SKEW_WORK_MEM_PERCENT / 100;
    hashtable->chunks = NULL;
    hashtable->width[0] = hashtable->width[1] = 0;
    hashtable->causedBySysRes = false;

    /* should we allow auto mem spread in query mem mode? */
    hashtable->maxMem = max_mem * 1024L;
    hashtable->spreadNum = 0;

    /*
     * Get info about the hash functions to be used for each hash key. Also
     * remember whether the join operators are strict.
     */
    nkeys = list_length(hashOperators);
    hashtable->outer_hashfunctions = (FmgrInfo*)palloc(nkeys * sizeof(FmgrInfo));
    hashtable->inner_hashfunctions = (FmgrInfo*)palloc(nkeys * sizeof(FmgrInfo));
    hashtable->hashStrict = (bool*)palloc(nkeys * sizeof(bool));
    i = 0;
    foreach (ho, hashOperators) {
        Oid hashop = lfirst_oid(ho);
        Oid left_hashfn;
        Oid right_hashfn;

        if (!get_op_hash_functions(hashop, &left_hashfn, &right_hashfn))
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                    errmodule(MOD_EXECUTOR),
                    errmsg("could not find hash function for hash operator %u", hashop)));
        fmgr_info(left_hashfn, &hashtable->outer_hashfunctions[i]);
        fmgr_info(right_hashfn, &hashtable->inner_hashfunctions[i]);
        hashtable->hashStrict[i] = op_strict(hashop);
        i++;
    }

    /*
     * Create temporary memory contexts in which to keep the hashtable working
     * storage.  See notes in executor/hashjoin.h.
     */
    hashtable->hashCxt = AllocSetContextCreate(CurrentMemoryContext,
        "HashTableContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        STANDARD_CONTEXT,
        local_work_mem * 1024L);

    hashtable->batchCxt = AllocSetContextCreate(hashtable->hashCxt,
        "HashBatchContext",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        STANDARD_CONTEXT,
        local_work_mem * 1024L);

    /* Allocate data that will live for the life of the hashjoin */
    oldcxt = MemoryContextSwitchTo(hashtable->hashCxt);

    if (nbatch > 1) {
        /*
         * allocate and initialize the file arrays in hashCxt
         */
        hashtable->innerBatchFile = (BufFile**)palloc0(nbatch * sizeof(BufFile*));
        hashtable->outerBatchFile = (BufFile**)palloc0(nbatch * sizeof(BufFile*));
        /* The files will not be opened until needed... */
        /* ... but make sure we have temp tablespaces established for them */
        PrepareTempTablespaces();
    }

    /*
     * Prepare context for the first-scan space allocations; allocate the
     * hashbucket array therein, and set each bucket "empty".
     */
    MemoryContextSwitchTo(hashtable->batchCxt);

    hashtable->buckets = (HashJoinTuple*)palloc0(nbuckets * sizeof(HashJoinTuple));

    /*
     * Set up for skew optimization, if possible and there's a need for more
     * than one batch.	(In a one-batch join, there's no point in it.)
     */
    if (nbatch > 1)
        ExecHashBuildSkewHash(hashtable, node, num_skew_mcvs);

    MemoryContextSwitchTo(oldcxt);

    return hashtable;
}

/*
 * Compute max tuples which fit into a given mem
 *
 *       HashTable (total of nbuckets)
 *  [ ] <--hash_header
 *  [ ]
 *  [ ]->[ ]->[ ]->....
 *  [ ]
 *  [ ]
 *  [ ]
 *  ^^^ bucket_bytes
 *      ^^^^^^^^^^^^^^^inner_rel_bytes
 *
 * bucket_bytes     = hash_header_size * nbuckets
 * nbuckets         = ntuples/ntuple_per_bucket
 * inner_rel_bytes  = ntuples * tupsize
 * hash_table_bytes = bucket_bytes + inner_rel_bytes;
 *
 * for a given hash_table_bytes of memory we can most fit in
 *
 *                   hash_table_bytes - skew_table_bytes
 *  ntuples=  ------------------------------------------------
 *             tuple_size + hash_header_size/NTUP_PER_BUCKET
 *
 * current we don't take max_pointers into account, that will
 * give a lower bound of hash_table_max_tuples which is safe
 * for us to use.
 *
 * This is exported so that the planner's costsize.c can use it.
 * also refer to ExecChooseHashTableSize
 */
/* Target bucket loading (tuples per bucket) */
#define NTUP_PER_BUCKET 1

double ExecChooseHashTableMaxTuples(int tupwidth, bool useskew, bool vectorized, double hash_table_bytes)
{
    int tupsize;
    int hash_header_size;
    double hash_table_max_tuples;

    hash_header_size = vectorized ? sizeof(void*) : sizeof(HashJoinTuple);

    if (vectorized) {
        tupsize = sizeof(void*) + MAXALIGN(tupwidth);
    } else {
        tupsize = HJTUPLE_OVERHEAD + MAXALIGN(sizeof(MinimalTupleData)) + MAXALIGN(tupwidth);
    }

    if (useskew) {
        double skew_table_bytes = hash_table_bytes * SKEW_WORK_MEM_PERCENT / 100;

        /* ----------
         * Divisor is:
         * size of a hash tuple +
         * worst-case size of skewBucket[] per MCV +
         * size of skewBucketNums[] entry +
         * size of skew bucket struct itself
         * ----------
         */
        int num_skew_mcvs =
            (int)(skew_table_bytes / (tupsize + (8 * sizeof(HashSkewBucket*)) + sizeof(int) + SKEW_BUCKET_OVERHEAD));
        if (num_skew_mcvs > 0) {
            hash_table_bytes -= skew_table_bytes;
        }
    }

    hash_table_max_tuples = hash_table_bytes / ((double)tupsize + (double)hash_header_size / NTUP_PER_BUCKET);

    return hash_table_max_tuples;
}

/*
 * Compute appropriate size for hashtable given the estimated size of the
 * relation to be hashed (number of rows and average row width).
 *
 * This is exported so that the planner's costsize.c can use it.
 */
void ExecChooseHashTableSize(double ntuples, int tupwidth, bool useskew, int* numbuckets, int* numbatches,
    int* num_skew_mcvs, int4 localWorkMem, bool vectorized, OpMemInfo* memInfo)
{
    int tupsize;
    double inner_rel_bytes;
    int64 bucket_bytes;
    int64 hash_table_bytes;
    int64 skew_table_bytes;
    int64 max_pointers;
    int64 mppow2;
    int nbatch = 1;
    int nbuckets;
    double dbuckets;
    int hash_header_size = vectorized ? sizeof(void*) : sizeof(HashJoinTuple);

    MEMCTL_LOG(DEBUG2,
        "[ExecChooseHashTableSize] ntuples %lf, width %d, useskew: %s, workmem %d, vectorized %s",
        ntuples,
        tupwidth,
        useskew ? "true" : "false",
        localWorkMem,
        vectorized ? "true" : "false");

    /* Force a plausible relation size if no info */
    if (ntuples <= 0.0) {
        ntuples = 1000.0;
    }

    /*
     * Estimate tupsize based on footprint of tuple in hashtable... note this
     * does not allow for any palloc overhead.	The manipulations of spaceUsed
     * don't count palloc overhead either.
     */
    if (vectorized) {
        tupsize = sizeof(void*) + MAXALIGN(tupwidth);
    } else {
        tupsize = HJTUPLE_OVERHEAD + MAXALIGN(sizeof(MinimalTupleData)) + MAXALIGN(tupwidth);
    }
    inner_rel_bytes = ntuples * tupsize;

    /*
     * Target in-memory hashtable size is work_mem kilobytes.
     */
    hash_table_bytes = localWorkMem * 1024L;

    /*
     * If skew optimization is possible, estimate the number of skew buckets
     * that will fit in the memory allowed, and decrement the assumed space
     * available for the main hash table accordingly.
     *
     * We make the optimistic assumption that each skew bucket will contain
     * one inner-relation tuple.  If that turns out to be low, we will recover
     * at runtime by reducing the number of skew buckets.
     *
     * hashtable->skewBucket will have up to 8 times as many HashSkewBucket
     * pointers as the number of MCVs we allow, since ExecHashBuildSkewHash
     * will round up to the next power of 2 and then multiply by 4 to reduce
     * collisions.
     */
    if (useskew) {
        skew_table_bytes = hash_table_bytes * SKEW_WORK_MEM_PERCENT / 100;

        /* ----------
         * Divisor is:
         * size of a hash tuple +
         * worst-case size of skewBucket[] per MCV +
         * size of skewBucketNums[] entry +
         * size of skew bucket struct itself
         * ----------
         */
        *num_skew_mcvs =
            skew_table_bytes / (tupsize + (8 * sizeof(HashSkewBucket*)) + sizeof(int) + SKEW_BUCKET_OVERHEAD);
        if (*num_skew_mcvs > 0) {
            hash_table_bytes -= skew_table_bytes;
        }
    } else {
        *num_skew_mcvs = 0;
    }

    /*
     * Set nbuckets to achieve an average bucket load of NTUP_PER_BUCKET when
     * memory is filled, assuming a single batch; but limit the value so that
     * the pointer arrays we'll try to allocate do not exceed work_mem nor
     * MaxAllocSize.
     *
     * Note that both nbuckets and nbatch must be powers of 2 to make
     * ExecHashGetBucketAndBatch fast.
     */
    max_pointers = (localWorkMem * 1024L) / hash_header_size;
    max_pointers = Min(max_pointers, (long)(MaxAllocSize / hash_header_size));
    /*  If max_pointers isn't a power of 2, must round it down to one */
    mppow2 = 1UL << my_log2(max_pointers);
    if (max_pointers != mppow2) {
        max_pointers = mppow2 / 2;
    }

    /* Also ensure we avoid integer overflow in nbatch and nbuckets */
    /* (this step is redundant given the current value of MaxAllocSize) */
    max_pointers = Min(max_pointers, INT_MAX / 2);

    dbuckets = ceil(ntuples / NTUP_PER_BUCKET);
    dbuckets = Min(dbuckets, max_pointers);
    nbuckets = (int)dbuckets;
    /* don't let nbuckets be really small, though ... */
    nbuckets = Max(nbuckets, MIN_HASH_BUCKET_SIZE);
    /* ... and force it to be a power of 2. */
    nbuckets = 1 << my_log2(nbuckets);

    /*
     * If there's not enough space to store the projected number of tuples and
     * the required bucket headers, we will need multiple batches.
     */
    bucket_bytes = ((int64)hash_header_size) * nbuckets;

    if (memInfo != NULL) {
        memInfo->maxMem = (inner_rel_bytes + bucket_bytes) / 1024L;
    }

    if (inner_rel_bytes + bucket_bytes > hash_table_bytes) {
        /* We'll need multiple batches */
        int64 lbuckets;
        double dbatch;
        int minbatch;
        double max_batch;
        int64 bucket_size;

        /*
         * Estimate the number of buckets we'll want to have when work_mem is
         * entirely full.  Each bucket will contain a bucket pointer plus
         * NTUP_PER_BUCKET tuples, whose projected size already includes
         * overhead for the hash code, pointer to the next tuple, etc.
         */
        bucket_size = ((int64)tupsize * NTUP_PER_BUCKET + hash_header_size);
        lbuckets = 1UL << my_log2(hash_table_bytes / bucket_size);
        lbuckets = Min(lbuckets, max_pointers);
        nbuckets = (int)lbuckets;
        nbuckets = 1 << my_log2(nbuckets);
        bucket_bytes = (int64)nbuckets * hash_header_size;

        /*
         * Buckets are simple pointers to hashjoin tuples, while tupsize
         * includes the pointer, hash code, and MinimalTupleData.  So buckets
         * should never really exceed 25% of work_mem (even for
         * NTUP_PER_BUCKET=1); except maybe for work_mem values that are not
         * 2^N bytes, where we might get more because of doubling. So let's
         * look for 50% here.
         */
        Assert(bucket_bytes <= hash_table_bytes / 2);

        /* Calculate required number of batches. */
        dbatch = ceil(inner_rel_bytes / (hash_table_bytes - bucket_bytes));
        dbatch = Min(dbatch, max_pointers);
        minbatch = (int)dbatch;
        nbatch = 2;
        while (nbatch < minbatch) {
            nbatch <<= 1;
        }

        /*
         * This Min() steps limit the nbatch so that the pointer arrays
         * we'll try to allocate do not exceed MaxAllocSize.
         */
        max_batch = (MaxAllocSize + 1) / sizeof(BufFile*) / 2;
        nbatch = (int)Min(nbatch, max_batch);
    }

    Assert(nbuckets > 0);
    Assert(nbatch > 0);

    *numbuckets = nbuckets;
    *numbatches = nbatch;
    MEMCTL_LOG(DEBUG2, "[ExecChooseHashTableSize] nbuckets: %d, nbatch: %d", nbuckets, nbatch);
}

/*
 * Get typeSize of the input typeOid and typeMod.
 * Some special adjustment for hashkey col.
 */
int ExecSonicHashGetAtomTypeSize(Oid typeOid, int typeMod, bool isHashKey)
{
    int minlen;
    int atomTypeSize;

    minlen = getDataMinLen(typeOid, typeMod);
    if (!COL_IS_ENCODE(typeOid)) {
        atomTypeSize = minlen;
        if (typeOid == TIDOID)
            atomTypeSize = 8;
    } else if (minlen == -1) {
        atomTypeSize = sizeof(Datum);
    } else {
        atomTypeSize = minlen;
    }

    /*
     * These hashKey cols are a little special, which is one of these typeOid:
     * SONIC_CHAR_DIC_TYPE, SONIC_FIXLEN_TYPE, SONIC_NUMERIC_COMPRESS_TYPE.
     * Because they store them as pointer. Thus, do some adjustment.
     */
    if (isHashKey && ((minlen > 8 && minlen <= 16) || typeOid == BPCHAROID || typeOid == CHAROID)) {
        atomTypeSize = sizeof(Datum);
    }

    return atomTypeSize;
}

/*
 * Compute m_arr size according to atomTypeSize
 */
int64 ExecSonicHashGetAtomArrayBytes(
    double ntuples, int m_arrSize, int m_atomSize, int64 atomTypeSize, bool hasNullFlag)
{
    int64 atomItemNum;
    int64 atomFlagSize;
    int64 atom_array_bytes;

    atomItemNum = m_atomSize * ((int64)ntuples / m_atomSize + 1);
    atomFlagSize = hasNullFlag ? ((atomItemNum + 7) / 8) : 0;
    atom_array_bytes = m_arrSize * sizeof(void*) + atomItemNum * atomTypeSize + atomFlagSize;
    return atom_array_bytes;
}

/*
 * Estimate hash bucket typeSize related to nbuckets
 */
uint8 EstimateBucketTypeSize(int nbuckets)
{
    uint8 bucketTypeSize;
    if ((((uint64)nbuckets) & 0xffff) == ((uint64)nbuckets)) {
        bucketTypeSize = sizeof(uint16);  // 2 bytes
    } else if ((((uint64)nbuckets) & 0xffffffff) == ((uint64)nbuckets)) {
        bucketTypeSize = sizeof(uint32);  // 4 byttes
    } else {
        bucketTypeSize = sizeof(uint64);  // 8 bytes
    }
    return bucketTypeSize;
}

/*
 * Compute appropriate size for hashtable given the estimated size of the
 * relation to be hashed (number of rows).
 */
void ExecChooseSonicHashTableSize(Path* inner_path, List* hashclauses, int* inner_width, bool isComplicateHashKey,
    int* numbuckets, int* numbatches, int4 localWorkMem, OpMemInfo* memInfo, int dop)
{
    ListCell* lc = NULL;
    Var* var = NULL;

    Node* innerkey = NULL;
    int64 atomTypeSize;
    int m_arrSize;
    int m_atomSize;
    int64 hash_data_bytes = 0;

    int nbatch = 1;
    int64 nbuckets;
    int64 mppow2;
    int m_bucketTypeSize;

    int64 hash_bucket_bytes;
    int64 hash_table_bytes;

    int tuple_width = 0;
    double ntuples = PATH_LOCAL_ROWS(inner_path) / dop;
    List* tleList = inner_path->parent->reltargetlist;
    Relids inner_relids = inner_path->parent->relids;

    MEMCTL_LOG(
        DEBUG2, "[ExecChooseHashTableSize] ntuples %lf, workmem %d, vectorized %s", ntuples, localWorkMem, "true");

    /* Force a plausible relation size if no info */
    if (ntuples <= 0.0) {
        ntuples = 1000.0;
    }

    hash_table_bytes = localWorkMem * 1024L;

    /* Estimate atomTypeSize for each col and caculate in-memory data size in hashtable*/
    m_atomSize = INIT_DATUM_ARRAY_SIZE;
    m_arrSize = ((int64)ntuples / (m_atomSize * INIT_ARR_CONTAINER_SIZE) + 1) * INIT_ARR_CONTAINER_SIZE;

    if (!isComplicateHashKey && list_length(hashclauses) == 1) {
        RestrictInfo* restrictinfo = (RestrictInfo*)lfirst(list_head(hashclauses));

        if (bms_is_subset(restrictinfo->right_relids, inner_relids)) {
            innerkey = get_rightop(restrictinfo->clause);
        } else {
            Assert(bms_is_subset(restrictinfo->left_relids, inner_relids));
            innerkey = get_leftop(restrictinfo->clause);
        }
    }

    foreach (lc, tleList) {
        var = (Var*)lfirst(lc);
        /* Check whether the current target entry is hashKey or not. */
        if (innerkey != NULL && IsA(innerkey, Var) && equal((Var*)innerkey, var)) {
            atomTypeSize = ExecSonicHashGetAtomTypeSize(var->vartype, var->vartypmod, true);
        } else {
            atomTypeSize = ExecSonicHashGetAtomTypeSize(var->vartype, var->vartypmod, false);
        }

        tuple_width += atomTypeSize;
        hash_data_bytes += ExecSonicHashGetAtomArrayBytes(ntuples, m_arrSize, m_atomSize, atomTypeSize, true);
    }

    /* Complicate hashkey needs m_hash defined as SonicDatumArray type */
    if (isComplicateHashKey) {
        tuple_width += sizeof(uint32);
        hash_data_bytes += ExecSonicHashGetAtomArrayBytes(ntuples, m_arrSize, m_atomSize, sizeof(uint32), false);
    }

    *inner_width = tuple_width;

    /* Estimate bucket number.*/
#ifdef USE_PRIME
    nbuckets = (int64)hashfindprime((uint64)ntuples);
#else
    nbuckets = (int64)Min(ntuples, MAX_BUCKET_NUM);

    /*
     * Ensure nbuckets is not too small.
     * Note: e must round nbuckets down to one if it is not a power of 2.
     * Similar to getPower2LessNum().
     */
    mppow2 = 1L << my_log2(nbuckets);
    if (nbuckets != mppow2)
        nbuckets = mppow2 / 2;
    nbuckets = Max(nbuckets, MIN_HASH_TABLE_SIZE);
#endif

    /* Estimate the size of m_bucket type*/
    m_bucketTypeSize = EstimateBucketTypeSize(nbuckets);

    /* Caculate bucket size*/
    hash_bucket_bytes = m_bucketTypeSize * nbuckets;

    /*
     * Record ideal memory usage without disk.
     * That is, the size of hash table including the data size and bucket size.
     */
    if (memInfo != NULL) {
        memInfo->maxMem = (double)((hash_data_bytes + hash_bucket_bytes) / 1024L);
    }

    /* Target in-memory hashtable size is work_mem kilobytes. */
    if (hash_data_bytes + hash_bucket_bytes > hash_table_bytes) {
        /* We'll need multiple batches */
        double dbatch;
        int64 maxbucket;
        int minbatch;
        double maxbatch;

        maxbucket =
            Min(hash_table_bytes / ((long)m_bucketTypeSize + BUCKET_OVERHEAD), (long)(MaxAllocSize / m_bucketTypeSize));
#ifdef USE_PRIME
        nbuckets = (int64)Min(maxbucket, (int64)hashfindprime((uint64)ntuples));
#else
        nbuckets = Min(maxbucket, max_pointers);
#endif
        mppow2 = 1UL << my_log2(nbuckets);
        if (nbuckets != mppow2)
            nbuckets = mppow2 / 2;

        hash_bucket_bytes = nbuckets * m_bucketTypeSize;

        Assert(hash_bucket_bytes <= hash_table_bytes / 2);

        /* Calculate required number of batches. */
        dbatch = ceil(hash_data_bytes / (hash_table_bytes - hash_bucket_bytes));
        dbatch = Min(dbatch, maxbucket);
        minbatch = (int)dbatch;
        nbatch = 2;

        while (nbatch < minbatch) {
            nbatch <<= 1;
        }

        /*
         * This Min() steps limit the nbatch so that the pointer arrays
         * we'll try to allocate do not exceed MaxAllocSize.
         */
        maxbatch = (MaxAllocSize + 1) / sizeof(BufFile*) / 2;
        nbatch = (int)Min(nbatch, maxbatch);
    }

    Assert(nbuckets > 0);
    Assert(nbatch > 0);

    *numbuckets = nbuckets;
    *numbatches = nbatch;
    MEMCTL_LOG(DEBUG2, "[ExecChooseSonicHashTableSize] nbuckets: %ld, nbatch: %d", nbuckets, nbatch);
}

/* ----------------------------------------------------------------
 *		ExecHashTableDestroy
 *
 *		destroy a hash table
 * ----------------------------------------------------------------
 */
void ExecHashTableDestroy(HashJoinTable hashtable)
{
    int i;

    /*
     * Make sure all the temp files are closed.  We skip batch 0, since it
     * can't have any temp files (and the arrays might not even exist if
     * nbatch is only 1).
     */
    for (i = 1; i < hashtable->nbatch; i++) {
        if (hashtable->innerBatchFile[i])
            BufFileClose(hashtable->innerBatchFile[i]);
        if (hashtable->outerBatchFile[i])
            BufFileClose(hashtable->outerBatchFile[i]);
    }

    /* Free the unused buffers */
    pfree_ext(hashtable->outer_hashfunctions);
    pfree_ext(hashtable->inner_hashfunctions);
    pfree_ext(hashtable->hashStrict);

    /* Release working memory (batchCxt is a child, so it goes away too) */
    MemoryContextDelete(hashtable->hashCxt);

    /* And drop the control block */
    pfree_ext(hashtable);
}

/*
 * ExecHashIncreaseNumBatches
 *		increase the original number of batches in order to reduce
 *		current memory consumption
 */
static void ExecHashIncreaseNumBatches(HashJoinTable hashtable)
{
    int oldnbatch = hashtable->nbatch;
    int curbatch = hashtable->curbatch;
    int nbatch;
    MemoryContext oldcxt;
    long ninmemory;
    long nfreed;
    HashMemoryChunk oldchunks;
    errno_t rc;

    /* do nothing if we've decided to shut off growth */
    if (!hashtable->growEnabled)
        return;

    /* safety check to avoid overflow */
    if ((uint32)oldnbatch > Min(INT_MAX / 2, MaxAllocSize / (sizeof(void*) * 2)))
        return;

    nbatch = oldnbatch * 2;
    Assert(nbatch > 1);

#ifdef HJDEBUG
    printf("Increasing nbatch to %d because space = %lu\n", nbatch, (unsigned long)hashtable->spaceUsed);
#endif

    oldcxt = MemoryContextSwitchTo(hashtable->hashCxt);

    if (hashtable->innerBatchFile == NULL) {
        /* we had no file arrays before */
        hashtable->innerBatchFile = (BufFile**)palloc0(nbatch * sizeof(BufFile*));
        hashtable->outerBatchFile = (BufFile**)palloc0(nbatch * sizeof(BufFile*));
        /* time to establish the temp tablespaces, too */
        PrepareTempTablespaces();
    } else {
        /* enlarge arrays and zero out added entries */
        hashtable->innerBatchFile = (BufFile**)repalloc(hashtable->innerBatchFile, nbatch * sizeof(BufFile*));
        hashtable->outerBatchFile = (BufFile**)repalloc(hashtable->outerBatchFile, nbatch * sizeof(BufFile*));
        rc = memset_s(hashtable->innerBatchFile + oldnbatch,
            (nbatch - oldnbatch) * sizeof(BufFile*),
            0,
            (nbatch - oldnbatch) * sizeof(BufFile*));
        securec_check(rc, "\0", "\0");
        rc = memset_s(hashtable->outerBatchFile + oldnbatch,
            (nbatch - oldnbatch) * sizeof(BufFile*),
            0,
            (nbatch - oldnbatch) * sizeof(BufFile*));
        securec_check(rc, "\0", "\0");
    }

    MemoryContextSwitchTo(oldcxt);

    hashtable->nbatch = nbatch;

    /*
     * Scan through the existing hash table entries and dump out any that are
     * no longer of the current batch.
     */
    ninmemory = nfreed = 0;

    /*
     * We will scan through the chunks directly, so that we can reset the
     * buckets now and not have to keep track which tuples in the buckets have
     * already been processed. We will free the old chunks as we go.
     */
    rc = memset_s(hashtable->buckets,
        sizeof(HashJoinTuple) * hashtable->nbuckets,
        0,
        sizeof(HashJoinTuple) * hashtable->nbuckets);
    securec_check(rc, "\0", "\0");
    oldchunks = hashtable->chunks;
    hashtable->chunks = NULL;

    /* so, let's scan through the old chunks, and all tuples in each chunk */
    while (oldchunks != NULL) {
        HashMemoryChunk nextchunk = oldchunks->next;
        /* position within the buffer (up to oldchunks->used) */
        size_t idx = 0;

        /* process all tuples stored in this chunk (and then free it) */
        while (idx < oldchunks->used) {
            HashJoinTuple hashTuple = (HashJoinTuple)(oldchunks->data + idx);
            MinimalTuple tuple = HJTUPLE_MINTUPLE(hashTuple);
            int hashTupleSize = (HJTUPLE_OVERHEAD + tuple->t_len);
            int bucketno;
            int batchno;

            ninmemory++;
            ExecHashGetBucketAndBatch(hashtable, hashTuple->hashvalue, &bucketno, &batchno);

            if (batchno == curbatch) {
                /* keep tuple in memory - copy it into the new chunk */
                HashJoinTuple copyTuple = (HashJoinTuple)dense_alloc(hashtable, hashTupleSize);
                rc = memcpy_s(copyTuple, hashTupleSize, hashTuple, hashTupleSize);
                securec_check(rc, "\0", "\0");

                /* and add it back to the appropriate bucket */
                copyTuple->next = hashtable->buckets[bucketno];
                hashtable->buckets[bucketno] = copyTuple;
            } else {
                /* dump it out */
                Assert(batchno > curbatch);
                ExecHashJoinSaveTuple(
                    HJTUPLE_MINTUPLE(hashTuple), hashTuple->hashvalue, &hashtable->innerBatchFile[batchno]);

                hashtable->spaceUsed -= hashTupleSize;
                nfreed++;
            }

            /* next tuple in this chunk */
            idx += MAXALIGN(hashTupleSize);

            /* allow this loop to be cancellable */
            CHECK_FOR_INTERRUPTS();
        }

        /* we're done with this chunk - free it and proceed to the next one */
        pfree_ext(oldchunks);
        oldchunks = nextchunk;
    }

#ifdef HJDEBUG
    printf("Freed %ld of %ld tuples, space now %lu\n", nfreed, ninmemory, (unsigned long)hashtable->spaceUsed);
#endif

    /*
     * If we dumped out either all or none of the tuples in the table, disable
     * further expansion of nbatch.  This situation implies that we have
     * enough tuples of identical hashvalues to overflow spaceAllowed.
     * Increasing nbatch will not fix it since there's no way to subdivide the
     * group any more finely. We have to just gut it out and hope the server
     * has enough RAM.
     */
    if (nfreed == 0 || nfreed == ninmemory) {
        hashtable->growEnabled = false;
#ifdef HJDEBUG
        printf("Disabling further increase of nbatch\n");
#endif
    }
}

/*
 * ExecHashTableInsert
 *		insert a tuple into the hash table depending on the hash value
 *		it may just go to a temp file for later batches
 *
 * Note: the passed TupleTableSlot may contain a regular, minimal, or virtual
 * tuple; the minimal case in particular is certain to happen while reloading
 * tuples from batch files.  We could save some cycles in the regular-tuple
 * case by not forcing the slot contents into minimal form; not clear if it's
 * worth the messiness required.
 */
void ExecHashTableInsert(
    HashJoinTable hashtable, TupleTableSlot* slot, uint32 hashvalue, int planid, int dop, Instrumentation* instrument)
{
    MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot);
    int bucketno;
    int batchno;
    errno_t errorno = EOK;

    ExecHashGetBucketAndBatch(hashtable, hashvalue, &bucketno, &batchno);

    /*
     * decide whether to put the tuple in the hash table or a temp file
     */
    if (batchno == hashtable->curbatch) {
        /*
         * put the tuple in hash table
         */
        HashJoinTuple hashTuple;
        int hashTupleSize;

        /* Create the HashJoinTuple */
        hashTupleSize = HJTUPLE_OVERHEAD + tuple->t_len;
        hashTuple = (HashJoinTuple)dense_alloc(hashtable, hashTupleSize);
        hashTuple->hashvalue = hashvalue;
        errorno = memcpy_s(HJTUPLE_MINTUPLE(hashTuple), tuple->t_len, tuple, tuple->t_len);
        securec_check(errorno, "\0", "\0");

        /*
         * We always reset the tuple-matched flag on insertion.  This is okay
         * even when reloading a tuple from a batch file, since the tuple
         * could not possibly have been matched to an outer tuple before it
         * went into the batch file.
         */
        HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(hashTuple));

        /* Push it onto the front of the bucket's list */
        hashTuple->next = hashtable->buckets[bucketno];
        hashtable->buckets[bucketno] = hashTuple;

        /* Record the total width and total tuples for first batch until spill */
        if (hashtable->width[0] >= 0) {
            hashtable->width[0]++;
            hashtable->width[1] += tuple->t_len;
        }

        /* Account for space used, and back off if we've used too much */
        hashtable->spaceUsed += hashTupleSize;
        if (hashtable->spaceUsed > hashtable->spacePeak) {
            hashtable->spacePeak = hashtable->spaceUsed;
        }
        bool sysBusy = gs_sysmemory_busy(hashtable->spaceUsed * dop, false);
        if (hashtable->spaceUsed > hashtable->spaceAllowed || sysBusy) {
            AllocSetContext* set = (AllocSetContext*)(hashtable->hashCxt);
            if (sysBusy) {
                hashtable->causedBySysRes = true;
                hashtable->spaceAllowed = hashtable->spaceUsed;
                set->maxSpaceSize = hashtable->spaceUsed;
                /* if hashtable failed to grow, this branch can be kicked many times */
                if (hashtable->growEnabled) {
                    MEMCTL_LOG(LOG,
                        "HashJoin(%d) early spilled, workmem: %ldKB, usedmem: %ldKB",
                        planid,
                        hashtable->spaceAllowed / 1024L,
                        hashtable->spaceUsed / 1024L);
                    pgstat_add_warning_early_spill();
                }
            /* try to auto spread memory if possible */
            } else if (hashtable->curbatch == 0 && hashtable->maxMem > hashtable->spaceAllowed) {
                hashtable->spaceAllowed = hashtable->spaceUsed;
                int64 spreadMem = Min(Min(dywlm_client_get_memory() * 1024L, hashtable->spaceAllowed),
                    hashtable->maxMem - hashtable->spaceAllowed);
                if (spreadMem > hashtable->spaceAllowed * MEM_AUTO_SPREAD_MIN_RATIO) {
                    hashtable->spaceAllowed += spreadMem;
                    hashtable->spreadNum++;
                    ExecHashIncreaseBuckets(hashtable);
                    set->maxSpaceSize += spreadMem;
                    MEMCTL_LOG(DEBUG2,
                        "HashJoin(%d) auto mem spread %ldKB succeed, and work mem is %ldKB.",
                        planid,
                        spreadMem / 1024L,
                        hashtable->spaceAllowed / 1024L);
                    return;
                }
                /* if hashtable failed to grow, this branch can be kicked many times */
                if (hashtable->growEnabled) {
                    MEMCTL_LOG(LOG,
                        "HashJoin(%d) auto mem spread %ldKB failed, and work mem is %ldKB.",
                        planid,
                        spreadMem / 1024L,
                        hashtable->spaceAllowed / 1024L);
                    if (hashtable->spreadNum) {
                        pgstat_add_warning_spill_on_memory_spread();
                    }
                }
            }

            /* cache the memory size into instrument for explain performance */
            if (instrument != NULL) {
                instrument->memoryinfo.peakOpMemory = hashtable->spaceUsed;
            }

            if (hashtable->width[0] > 0)
                hashtable->width[1] = hashtable->width[1] / hashtable->width[0];
            hashtable->width[0] = -1;
            ExecHashIncreaseNumBatches(hashtable);
        }
    } else {
        /*
         * put the tuple into a temp file for later batches
         */
        Assert(batchno > hashtable->curbatch);
        ExecHashJoinSaveTuple(tuple, hashvalue, &hashtable->innerBatchFile[batchno]);

        hashtable->spill_count += 1;
        *hashtable->spill_size += sizeof(uint32) + tuple->t_len;
        pgstat_increase_session_spill_size(sizeof(uint32) + tuple->t_len);
    }
}

/*
 * ExecHashGetHashValue
 *		Compute the hash value for a tuple
 *
 * The tuple to be tested must be in either econtext->ecxt_outertuple or
 * econtext->ecxt_innertuple.  Vars in the hashkeys expressions should have
 * varno either OUTER_VAR or INNER_VAR.
 *
 * A TRUE result means the tuple's hash value has been successfully computed
 * and stored at *hashvalue.  A FALSE result means the tuple cannot match
 * because it contains a null attribute, and hence it should be discarded
 * immediately.  (If keep_nulls is true then FALSE is never returned.)
 */
bool ExecHashGetHashValue(HashJoinTable hashtable, ExprContext* econtext, List* hashkeys, bool outer_tuple,
    bool keep_nulls, uint32* hashvalue)
{
    uint32 hashkey = 0;
    FmgrInfo* hashfunctions = NULL;
    ListCell* hk = NULL;
    int i = 0;
    MemoryContext oldContext;

    /*
     * We reset the eval context each time to reclaim any memory leaked in the
     * hashkey expressions.
     */
    ResetExprContext(econtext);

    oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

    if (outer_tuple)
        hashfunctions = hashtable->outer_hashfunctions;
    else
        hashfunctions = hashtable->inner_hashfunctions;

    foreach (hk, hashkeys) {
        ExprState* keyexpr = (ExprState*)lfirst(hk);
        Datum keyval;
        bool isNull = false;

        /* rotate hashkey left 1 bit at each step */
        hashkey = (hashkey << 1) | ((hashkey & 0x80000000) ? 1 : 0);

        /*
         * Get the join attribute value of the tuple
         */
        keyval = ExecEvalExpr(keyexpr, econtext, &isNull, NULL);

        /*
         * If the attribute is NULL, and the join operator is strict, then
         * this tuple cannot pass the join qual so we can reject it
         * immediately (unless we're scanning the outside of an outer join, in
         * which case we must not reject it).  Otherwise we act like the
         * hashcode of NULL is zero (this will support operators that act like
         * IS NOT DISTINCT, though not any more-random behavior).  We treat
         * the hash support function as strict even if the operator is not.
         *
         * Note: currently, all hashjoinable operators must be strict since
         * the hash index AM assumes that.	However, it takes so little extra
         * code here to allow non-strict that we may as well do it.
         */
        if (isNull) {
            if (hashtable->hashStrict[i] && !keep_nulls) {
                MemoryContextSwitchTo(oldContext);
                return false; /* cannot match */
            }
            /* else, leave hashkey unmodified, equivalent to hashcode 0 */
        } else {
            /* Compute the hash function */
            uint32 hkey;

            hkey = DatumGetUInt32(FunctionCall1(&hashfunctions[i], keyval));
            hashkey ^= hkey;
        }

        i++;
    }

    MemoryContextSwitchTo(oldContext);
    hashkey = DatumGetUInt32(hash_uint32(hashkey));
    *hashvalue = hashkey;
    return true;
}

/*
 * ExecHashGetBucketAndBatch
 *		Determine the bucket number and batch number for a hash value
 *
 * Note: on-the-fly increases of nbatch must not change the bucket number
 * for a given hash code (since we don't move tuples to different hash
 * chains), and must only cause the batch number to remain the same or
 * increase.  Our algorithm is
 *		bucketno = hashvalue MOD nbuckets
 *		batchno = (hashvalue DIV nbuckets) MOD nbatch
 * where nbuckets and nbatch are both expected to be powers of 2, so we can
 * do the computations by shifting and masking.  (This assumes that all hash
 * functions are good about randomizing all their output bits, else we are
 * likely to have very skewed bucket or batch occupancy.)
 *
 * nbuckets doesn't change over the course of the join.
 *
 * nbatch is always a power of 2; we increase it only by doubling it.  This
 * effectively adds one more bit to the top of the batchno.
 */
void ExecHashGetBucketAndBatch(HashJoinTable hashtable, uint32 hashvalue, int* bucketno, int* batchno)
{
    uint32 nbuckets = (uint32)hashtable->nbuckets;
    uint32 nbatch = (uint32)hashtable->nbatch;

    if (nbatch > 1) {
        /* we can do MOD by masking, DIV by shifting */
        *bucketno = hashvalue & (nbuckets - 1);
        *batchno = (hashvalue >> hashtable->log2_nbuckets) & (nbatch - 1);
    } else {
        *bucketno = hashvalue & (nbuckets - 1);
        *batchno = 0;
    }
}

/*
 * ExecScanHashBucket
 *		scan a hash bucket for matches to the current outer tuple
 *
 * The current outer tuple must be stored in econtext->ecxt_outertuple.
 *
 * On success, the inner tuple is stored into hjstate->hj_CurTuple and
 * econtext->ecxt_innertuple, using hjstate->hj_HashTupleSlot as the slot
 * for the latter.
 */
bool ExecScanHashBucket(HashJoinState* hjstate, ExprContext* econtext)
{
    List* hjclauses = hjstate->hashclauses;
    HashJoinTable hashtable = hjstate->hj_HashTable;
    HashJoinTuple hashTuple = hjstate->hj_CurTuple;
    uint32 hashvalue = hjstate->hj_CurHashValue;

    /*
     * hj_CurTuple is the address of the tuple last returned from the current
     * bucket, or NULL if it's time to start scanning a new bucket.
     *
     * If the tuple hashed to a skew bucket then scan the skew bucket
     * otherwise scan the standard hashtable bucket.
     */
    if (hashTuple != NULL)
        hashTuple = hashTuple->next;
    else if (hjstate->hj_CurSkewBucketNo != INVALID_SKEW_BUCKET_NO)
        hashTuple = hashtable->skewBucket[hjstate->hj_CurSkewBucketNo]->tuples;
    else
        hashTuple = hashtable->buckets[hjstate->hj_CurBucketNo];

    while (hashTuple != NULL) {
        if (hashTuple->hashvalue == hashvalue) {
            TupleTableSlot* inntuple = NULL;

            /* insert hashtable's tuple into exec slot so ExecQual sees it */
            inntuple =
                ExecStoreMinimalTuple(HJTUPLE_MINTUPLE(hashTuple), hjstate->hj_HashTupleSlot, false); /* do not pfree */
            econtext->ecxt_innertuple = inntuple;

            /* reset temp memory each time to avoid leaks from qual expr */
            ResetExprContext(econtext);

            /* we allow null = null in special case, so an additional judgement is needed*/
            if (ExecQual(hjclauses, econtext, false) ||
                (hjstate->js.nulleqqual != NIL && ExecQual(hjstate->js.nulleqqual, econtext, false))) {
                hjstate->hj_CurTuple = hashTuple;
                return true;
            }
        }

        /*
         * For right Semi/Anti join, we delete mathced tuples in HashTable to make next matching faster,
         * so pointer hj_PreTuple is designed to follow the hj_CurTuple and to help us to clear the HashTable.
         */
        if (hjstate->js.jointype == JOIN_RIGHT_SEMI || hjstate->js.jointype == JOIN_RIGHT_ANTI)
            hjstate->hj_PreTuple = hashTuple;

        hashTuple = hashTuple->next;
    }

    /*
     * no match
     */
    return false;
}

/*
 * ExecPrepHashTableForUnmatched
 *		set up for a series of ExecScanHashTableForUnmatched calls
 */
void ExecPrepHashTableForUnmatched(HashJoinState* hjstate)
{
    /*
     * ---------- During this scan we use the HashJoinState fields as follows:
     *
     * hj_CurBucketNo: next regular bucket to scan hj_CurSkewBucketNo: next
     * skew bucket (an index into skewBucketNums) hj_CurTuple: last tuple
     * returned, or NULL to start next bucket ----------
     */
    hjstate->hj_CurBucketNo = 0;
    hjstate->hj_CurSkewBucketNo = 0;
    hjstate->hj_CurTuple = NULL;
}

/*
 * ExecScanHashTableForUnmatched
 *		scan the hash table for unmatched inner tuples
 *
 * On success, the inner tuple is stored into hjstate->hj_CurTuple and
 * econtext->ecxt_innertuple, using hjstate->hj_HashTupleSlot as the slot
 * for the latter.
 */
bool ExecScanHashTableForUnmatched(HashJoinState* hjstate, ExprContext* econtext)
{
    HashJoinTable hashtable = hjstate->hj_HashTable;
    HashJoinTuple hashTuple = hjstate->hj_CurTuple;

    for (;;) {
        /*
         * hj_CurTuple is the address of the tuple last returned from the
         * current bucket, or NULL if it's time to start scanning a new
         * bucket.
         */
        if (hashTuple != NULL)
            hashTuple = hashTuple->next;
        else if (hjstate->hj_CurBucketNo < hashtable->nbuckets) {
            hashTuple = hashtable->buckets[hjstate->hj_CurBucketNo];
            hjstate->hj_CurBucketNo++;
        } else if (hjstate->hj_CurSkewBucketNo < hashtable->nSkewBuckets) {
            int j = hashtable->skewBucketNums[hjstate->hj_CurSkewBucketNo];

            hashTuple = hashtable->skewBucket[j]->tuples;
            hjstate->hj_CurSkewBucketNo++;
        } else
            break; /* finished all buckets */

        while (hashTuple != NULL) {
            if (!HeapTupleHeaderHasMatch(HJTUPLE_MINTUPLE(hashTuple))) {
                TupleTableSlot* inntuple = NULL;

                /* insert hashtable's tuple into exec slot */
                inntuple = ExecStoreMinimalTuple(
                    HJTUPLE_MINTUPLE(hashTuple), hjstate->hj_HashTupleSlot, false); /* do not pfree */
                econtext->ecxt_innertuple = inntuple;

                /*
                 * Reset temp memory each time; although this function doesn't
                 * do any qual eval, the caller will, so let's keep it
                 * parallel to ExecScanHashBucket.
                 */
                ResetExprContext(econtext);

                hjstate->hj_CurTuple = hashTuple;
                return true;
            }

            hashTuple = hashTuple->next;
        }
    }

    /*
     * no more unmatched tuples
     */
    return false;
}

/*
 * ExecHashTableReset
 *
 *		reset hash table header for new batch
 */
void ExecHashTableReset(HashJoinTable hashtable)
{
    MemoryContext oldcxt;
    int nbuckets = hashtable->nbuckets;

    /*
     * Release all the hash buckets and tuples acquired in the prior pass, and
     * reinitialize the context for a new pass.
     */
    MemoryContextReset(hashtable->batchCxt);
    oldcxt = MemoryContextSwitchTo(hashtable->batchCxt);

    /* Reallocate and reinitialize the hash bucket headers. */
    hashtable->buckets = (HashJoinTuple*)palloc0(nbuckets * sizeof(HashJoinTuple));

    hashtable->spaceUsed = 0;

    MemoryContextSwitchTo(oldcxt);

    /* Forget the chunks (the memory was freed by the context reset above). */
    hashtable->chunks = NULL;
}

/*
 * ExecHashTableResetMatchFlags
 *		Clear all the HeapTupleHeaderHasMatch flags in the table
 */
void ExecHashTableResetMatchFlags(HashJoinTable hashtable)
{
    HashJoinTuple tuple;
    int i;

    /* Reset all flags in the main table ... */
    for (i = 0; i < hashtable->nbuckets; i++) {
        for (tuple = hashtable->buckets[i]; tuple != NULL; tuple = tuple->next)
            HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(tuple));
    }

    /* ... and the same for the skew buckets, if any */
    for (i = 0; i < hashtable->nSkewBuckets; i++) {
        int j = hashtable->skewBucketNums[i];
        HashSkewBucket* skewBucket = hashtable->skewBucket[j];

        for (tuple = skewBucket->tuples; tuple != NULL; tuple = tuple->next)
            HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(tuple));
    }
}

void ExecReScanHash(HashState* node)
{
    /*
     * if chgParam of subnode is not null then plan will be re-scanned by
     * first ExecProcNode.
     */
    if (node->ps.lefttree->chgParam == NULL)
        ExecReScan(node->ps.lefttree);
}

/*
 * ExecHashBuildSkewHash
 *
 *		Set up for skew optimization if we can identify the most common values
 *		(MCVs) of the outer relation's join key.  We make a skew hash bucket
 *		for the hash value of each MCV, up to the number of slots allowed
 *		based on available memory.
 */
static void ExecHashBuildSkewHash(HashJoinTable hashtable, Hash* node, int mcvsToUse)
{
    HeapTupleData* statsTuple = NULL;
    Datum* values = NULL;
    int nvalues;
    float4* numbers = NULL;
    int nnumbers;
    char stakind = STARELKIND_CLASS;

    /* do nothing if we don't have room for at least one skew bucket */
    /* Also, Do nothing if planner didn't identify the outer relation's join key */
    if (mcvsToUse <= 0 || !OidIsValid(node->skewTable)) {
        return;
    }

    if (isPartitionObject(node->skewTable, PART_OBJ_TYPE_TABLE_PARTITION, true)) {
        stakind = STARELKIND_PARTITION;
    }

    /*
     * Try to find the MCV statistics for the outer relation's join key.
     *
     * Note: We don't consider multi-column skew-optimization values here(improve later)
     */
    statsTuple = SearchSysCache4(STATRELKINDATTINH,
                                ObjectIdGetDatum(node->skewTable),
                                CharGetDatum(stakind),
                                Int16GetDatum(node->skewColumn),
                                BoolGetDatum(node->skewInherit));
    if (!HeapTupleIsValid(statsTuple)) {
        return;
    }

    if (get_attstatsslot(statsTuple,
                        node->skewColType,
                        node->skewColTypmod,
                        STATISTIC_KIND_MCV,
                        InvalidOid,
                        NULL,
                        &values,
                        &nvalues,
                        &numbers,
                        &nnumbers)) {
        double frac;
        int nbuckets;
        FmgrInfo* hashfunctions = NULL;
        int i;

        if (mcvsToUse > nvalues) {
            mcvsToUse = nvalues;
        }

        /*
         * Calculate the expected fraction of outer relation that will
         * participate in the skew optimization.  If this isn't at least
         * SKEW_MIN_OUTER_FRACTION, don't use skew optimization.
         */
        frac = 0;
        for (i = 0; i < mcvsToUse; i++) {
            frac += numbers[i];
        }
        if (frac < SKEW_MIN_OUTER_FRACTION) {
            free_attstatsslot(node->skewColType, values, nvalues, numbers, nnumbers);
            ReleaseSysCache(statsTuple);
            return;
        }

        /*
         * Okay, set up the skew hashtable.
         *
         * skewBucket[] is an open addressing hashtable with a power of 2 size
         * that is greater than the number of MCV values.  (This ensures there
         * will be at least one null entry, so searches will always
         * terminate.)
         *
         * Note: this code could fail if mcvsToUse exceeds INT_MAX/8 or
         * MaxAllocSize/sizeof(void *)/8, but that is not currently possible
         * since we limit pg_statistic entries to much less than that.
         */
        nbuckets = 2;
        while (nbuckets <= mcvsToUse) {
            nbuckets <<= 1;
        }
        /* use two more bits just to help avoid collisions */
        nbuckets <<= 2;

        hashtable->skewEnabled = true;
        hashtable->skewBucketLen = nbuckets;

        /*
         * We allocate the bucket memory in the hashtable's batch context. It
         * is only needed during the first batch, and this ensures it will be
         * automatically removed once the first batch is done.
         */
        hashtable->skewBucket =
            (HashSkewBucket**)MemoryContextAllocZero(hashtable->batchCxt, nbuckets * sizeof(HashSkewBucket*));
        hashtable->skewBucketNums = (int*)MemoryContextAllocZero(hashtable->batchCxt, mcvsToUse * sizeof(int));

        hashtable->spaceUsed += nbuckets * sizeof(HashSkewBucket*) + mcvsToUse * sizeof(int);
        hashtable->spaceUsedSkew += nbuckets * sizeof(HashSkewBucket*) + mcvsToUse * sizeof(int);
        if (hashtable->spaceUsed > hashtable->spacePeak) {
            hashtable->spacePeak = hashtable->spaceUsed;
        }

        /*
         * Create a skew bucket for each MCV hash value.
         *
         * Note: it is very important that we create the buckets in order of
         * decreasing MCV frequency.  If we have to remove some buckets, they
         * must be removed in reverse order of creation (see notes in
         * ExecHashRemoveNextSkewBucket) and we want the least common MCVs to
         * be removed first.
         */
        hashfunctions = hashtable->outer_hashfunctions;

        for (i = 0; i < mcvsToUse; i++) {
            uint32 hashvalue;
            int bucket;

            hashvalue = DatumGetUInt32(FunctionCall1(&hashfunctions[0], values[i]));

            /*
             * While we have not hit a hole in the hashtable and have not hit
             * the desired bucket, we have collided with some previous hash
             * value, so try the next bucket location.	NB: this code must
             * match ExecHashGetSkewBucket.
             */
            bucket = hashvalue & (nbuckets - 1);
            while (hashtable->skewBucket[bucket] != NULL && hashtable->skewBucket[bucket]->hashvalue != hashvalue) {
                bucket = (bucket + 1) & (nbuckets - 1);
            }

            /*
             * If we found an existing bucket with the same hashvalue, leave
             * it alone.  It's okay for two MCVs to share a hashvalue.
             */
            if (hashtable->skewBucket[bucket] != NULL) {
                continue;
            }

            /* Okay, create a new skew bucket for this hashvalue. */
            hashtable->skewBucket[bucket] =
                (HashSkewBucket*)MemoryContextAlloc(hashtable->batchCxt, sizeof(HashSkewBucket));
            hashtable->skewBucket[bucket]->hashvalue = hashvalue;
            hashtable->skewBucket[bucket]->tuples = NULL;
            hashtable->skewBucketNums[hashtable->nSkewBuckets] = bucket;
            hashtable->nSkewBuckets++;
            hashtable->spaceUsed += SKEW_BUCKET_OVERHEAD;
            hashtable->spaceUsedSkew += SKEW_BUCKET_OVERHEAD;
            if (hashtable->spaceUsed > hashtable->spacePeak) {
                hashtable->spacePeak = hashtable->spaceUsed;
            }
        }

        free_attstatsslot(node->skewColType, values, nvalues, numbers, nnumbers);
    }

    ReleaseSysCache(statsTuple);
}

/*
 * ExecHashGetSkewBucket
 *
 *		Returns the index of the skew bucket for this hashvalue,
 *		or INVALID_SKEW_BUCKET_NO if the hashvalue is not
 *		associated with any active skew bucket.
 */
int ExecHashGetSkewBucket(HashJoinTable hashtable, uint32 hashvalue)
{
    int bucket;

    /*
     * Always return INVALID_SKEW_BUCKET_NO if not doing skew optimization (in
     * particular, this happens after the initial batch is done).
     */
    if (!hashtable->skewEnabled)
        return INVALID_SKEW_BUCKET_NO;

    /*
     * Since skewBucketLen is a power of 2, we can do a modulo by ANDing.
     */
    bucket = hashvalue & (hashtable->skewBucketLen - 1);

    /*
     * While we have not hit a hole in the hashtable and have not hit the
     * desired bucket, we have collided with some other hash value, so try the
     * next bucket location.
     */
    while (hashtable->skewBucket[bucket] != NULL && hashtable->skewBucket[bucket]->hashvalue != hashvalue)
        bucket = (bucket + 1) & (hashtable->skewBucketLen - 1);

    /*
     * Found the desired bucket?
     */
    if (hashtable->skewBucket[bucket] != NULL)
        return bucket;

    /*
     * There must not be any hashtable entry for this hash value.
     */
    return INVALID_SKEW_BUCKET_NO;
}

/*
 * ExecHashSkewTableInsert
 *
 *		Insert a tuple into the skew hashtable.
 *
 * This should generally match up with the current-batch case in
 * ExecHashTableInsert.
 */
static void ExecHashSkewTableInsert(HashJoinTable hashtable, TupleTableSlot* slot, uint32 hashvalue, int bucketNumber)
{
    MinimalTuple tuple = ExecFetchSlotMinimalTuple(slot);
    HashJoinTuple hashTuple;
    int hashTupleSize;
    errno_t rc = EOK;

    /* Create the HashJoinTuple */
    hashTupleSize = HJTUPLE_OVERHEAD + tuple->t_len;
    hashTuple = (HashJoinTuple)MemoryContextAlloc(hashtable->batchCxt, hashTupleSize);
    hashTuple->hashvalue = hashvalue;
    rc = memcpy_s(HJTUPLE_MINTUPLE(hashTuple), tuple->t_len, tuple, tuple->t_len);
    securec_check(rc, "\0", "\0");
    HeapTupleHeaderClearMatch(HJTUPLE_MINTUPLE(hashTuple));

    /* Push it onto the front of the skew bucket's list */
    hashTuple->next = hashtable->skewBucket[bucketNumber]->tuples;
    hashtable->skewBucket[bucketNumber]->tuples = hashTuple;

    /* Account for space used, and back off if we've used too much */
    hashtable->spaceUsed += hashTupleSize;
    hashtable->spaceUsedSkew += hashTupleSize;
    if (hashtable->spaceUsed > hashtable->spacePeak)
        hashtable->spacePeak = hashtable->spaceUsed;
    while (hashtable->spaceUsedSkew > hashtable->spaceAllowedSkew)
        ExecHashRemoveNextSkewBucket(hashtable);

    /* Check we are not over the total spaceAllowed, either */
    if (hashtable->spaceUsed > hashtable->spaceAllowed)
        ExecHashIncreaseNumBatches(hashtable);
}

/*
 *		ExecHashRemoveNextSkewBucket
 *
 *		Remove the least valuable skew bucket by pushing its tuples into
 *		the main hash table.
 */
static void ExecHashRemoveNextSkewBucket(HashJoinTable hashtable)
{
    int bucketToRemove;
    HashSkewBucket* bucket = NULL;
    uint32 hashvalue;
    int bucketno;
    int batchno;
    HashJoinTuple hashTuple;

    /* Locate the bucket to remove */
    bucketToRemove = hashtable->skewBucketNums[hashtable->nSkewBuckets - 1];
    bucket = hashtable->skewBucket[bucketToRemove];

    /*
     * Calculate which bucket and batch the tuples belong to in the main
     * hashtable.  They all have the same hash value, so it's the same for all
     * of them.  Also note that it's not possible for nbatch to increase while
     * we are processing the tuples.
     */
    hashvalue = bucket->hashvalue;
    ExecHashGetBucketAndBatch(hashtable, hashvalue, &bucketno, &batchno);

    /* Process all tuples in the bucket */
    hashTuple = bucket->tuples;
    while (hashTuple != NULL) {
        HashJoinTuple nextHashTuple = hashTuple->next;
        MinimalTuple tuple;
        Size tupleSize;

        /*
         * This code must agree with ExecHashTableInsert.  We do not use
         * ExecHashTableInsert directly as ExecHashTableInsert expects a
         * TupleTableSlot while we already have HashJoinTuples.
         */
        tuple = HJTUPLE_MINTUPLE(hashTuple);
        tupleSize = HJTUPLE_OVERHEAD + tuple->t_len;

        /* Decide whether to put the tuple in the hash table or a temp file */
        if (batchno == hashtable->curbatch) {
            /* Move the tuple to the main hash table */
            hashTuple->next = hashtable->buckets[bucketno];
            hashtable->buckets[bucketno] = hashTuple;
            /* We have reduced skew space, but overall space doesn't change */
            hashtable->spaceUsedSkew -= tupleSize;
        } else {
            /* Put the tuple into a temp file for later batches */
            Assert(batchno > hashtable->curbatch);
            ExecHashJoinSaveTuple(tuple, hashvalue, &hashtable->innerBatchFile[batchno]);
            pfree_ext(hashTuple);
            hashtable->spaceUsed -= tupleSize;
            hashtable->spaceUsedSkew -= tupleSize;
        }

        hashTuple = nextHashTuple;

        /* allow this loop to be cancellable */
        CHECK_FOR_INTERRUPTS();
    }

    /*
     * Free the bucket struct itself and reset the hashtable entry to NULL.
     *
     * NOTE: this is not nearly as simple as it looks on the surface, because
     * of the possibility of collisions in the hashtable.  Suppose that hash
     * values A and B collide at a particular hashtable entry, and that A was
     * entered first so B gets shifted to a different table entry.	If we were
     * to remove A first then ExecHashGetSkewBucket would mistakenly start
     * reporting that B is not in the hashtable, because it would hit the NULL
     * before finding B.  However, we always remove entries in the reverse
     * order of creation, so this failure cannot happen.
     */
    hashtable->skewBucket[bucketToRemove] = NULL;
    hashtable->nSkewBuckets--;
    pfree_ext(bucket);
    hashtable->spaceUsed -= SKEW_BUCKET_OVERHEAD;
    hashtable->spaceUsedSkew -= SKEW_BUCKET_OVERHEAD;

    /*
     * If we have removed all skew buckets then give up on skew optimization.
     * Release the arrays since they aren't useful any more.
     */
    if (hashtable->nSkewBuckets == 0) {
        hashtable->skewEnabled = false;
        pfree_ext(hashtable->skewBucket);
        pfree_ext(hashtable->skewBucketNums);
        hashtable->skewBucket = NULL;
        hashtable->skewBucketNums = NULL;
        hashtable->spaceUsed -= hashtable->spaceUsedSkew;
        hashtable->spaceUsedSkew = 0;
    }
}

/*
 *		ExecHashIncreaseBuckets
 *
 *		Increase the original number of buckets during memory
 *		auto spread to avoid hash bucket conflict
 */
static void ExecHashIncreaseBuckets(HashJoinTable hashtable)
{
    int64 ntotal = 0;
    int64 nmove = 0;
    errno_t rc;

    /* do nothing if we've decided to shut off growth */
    if (!hashtable->growEnabled)
        return;

    /* do nothing if disk spill is already happened */
    if (hashtable->nbatch > 1)
        return;

    /* do nothing if there's still enough space */
    if (hashtable->totalTuples * 2 <= hashtable->nbuckets)
        return;

    /* safety check to avoid overflow */
    if ((uint32)hashtable->nbuckets > Min(INT_MAX, MaxAllocSize / (sizeof(HashJoinTuple))) / 2)
        return;

    hashtable->buckets = (HashJoinTuple*)repalloc(hashtable->buckets, hashtable->nbuckets * 2 * sizeof(HashJoinTuple));
    rc = memset_s(hashtable->buckets + hashtable->nbuckets,
        hashtable->nbuckets * sizeof(HashJoinTuple),
        0,
        hashtable->nbuckets * sizeof(HashJoinTuple));
    securec_check(rc, "\0", "\0");

    /*
     * Scan through the existing hash table entries and dump out any that are
     * no longer of the current batch.
     */
    for (int i = 0; i < hashtable->nbuckets; i++) {
        HashJoinTuple htuple = hashtable->buckets[i];
        HashJoinTuple prev = NULL;
        HashJoinTuple next = NULL;
        while (htuple != NULL) {
            int offset = (htuple->hashvalue >> hashtable->log2_nbuckets) & 1;

            ntotal++;
            next = htuple->next;
            if (offset == 1) {
                if (prev == NULL)
                    hashtable->buckets[i] = htuple->next;
                else
                    prev->next = htuple->next;
                htuple->next = hashtable->buckets[i + hashtable->nbuckets];
                hashtable->buckets[i + hashtable->nbuckets] = htuple;
                Assert((int32)(htuple->hashvalue % (hashtable->nbuckets * 2)) == i + hashtable->nbuckets);
                nmove++;
            } else {
                prev = htuple;
                Assert((int32)(htuple->hashvalue % (hashtable->nbuckets * 2)) == i);
            }
            htuple = next;
        }

        /* allow this loop to be cancellable */
        CHECK_FOR_INTERRUPTS();
    }

    /*
     * If we dumped out either all or none of the tuples in the table, disable
     * further expansion of nbatch.  This situation implies that we have
     * enough tuples of identical hashvalues to overflow spaceAllowed.
     * Increasing nbatch will not fix it since there's no way to subdivide the
     * group any more finely. We have to just gut it out and hope the server
     * has enough RAM.
     */
    if (nmove == 0 || nmove == ntotal) {
        hashtable->growEnabled = false;
#ifdef HJDEBUG
        printf("Disabling further increase of nbatch or nbucket\n");
#endif
    }

    hashtable->nbuckets = hashtable->nbuckets * 2;
    hashtable->log2_nbuckets++;
}

void ExecHashTableStats(HashJoinTable hashtable, int planid)
{
    int fillRows = 0;
    int singleNum = 0;
    int doubleNum = 0;
    int conflictNum = 0;
    int chainLen = 0;
    int maxChainLen = 0;

    for (int i = 0; i < hashtable->nbuckets; i++) {
        HashJoinTuple htuple = hashtable->buckets[i];

        /* record each hash chain's length and accumulate hash element */
        chainLen = 0;

        while (htuple != NULL) {
            fillRows++;
            chainLen++;
            htuple = htuple->next;
        }

        /* record the number of hash chains with length equal to 1 */
        if (chainLen == 1)
            singleNum++;

        /* record the number of hash chains with length equal to 2 */
        if (chainLen == 2)
            doubleNum++;

        /* mark if the length of hash chain is greater than 3, we meet hash confilct */
        if (chainLen >= 3)
            conflictNum++;

        /* record the length of the max hash chain  */
        if (chainLen > maxChainLen)
            maxChainLen = chainLen;
    }

    /* print the information */
    ereport(LOG,
        (errmodule(MOD_VEC_EXECUTOR),
            errmsg("[HashJoin(%d) batch %d] Hash Table Profiling: table size: %d,"
                   " hash elements: %d, table fill ratio %.2f, max hash chain len: %d,"
                   " %d chains have length 1, %d chains have length 2, %d chains have conficts "
                   "with length >= 3.",
                planid,
                hashtable->curbatch,
                hashtable->nbuckets,
                fillRows,
                (double)fillRows / hashtable->nbuckets,
                maxChainLen,
                singleNum,
                doubleNum,
                conflictNum)));
}

/*
 * Allocate 'size' bytes from the currently active HashMemoryChunk
 */
static void* dense_alloc(HashJoinTable hashtable, Size size)
{
    HashMemoryChunk newChunk;
    char* ptr = NULL;

    /* just in case the size is not already aligned properly */
    size = MAXALIGN(size);
    /*
     * If tuple size is larger than of 1/4 of chunk size, allocate a separate
     * chunk.
     */
    if (size > HASH_CHUNK_THRESHOLD) {
        /* allocate new chunk and put it at the beginning of the list */
        newChunk = (HashMemoryChunk)MemoryContextAlloc(hashtable->batchCxt, offsetof(HashMemoryChunkData, data) + size);
        newChunk->maxlen = size;
        newChunk->used = 0;
        newChunk->ntuples = 0;

        /*
         * Add this chunk to the list after the first existing chunk, so that
         * we don't lose the remaining space in the "current" chunk.
         */
        if (hashtable->chunks != NULL) {
            newChunk->next = hashtable->chunks->next;
            hashtable->chunks->next = newChunk;
        } else {
            newChunk->next = hashtable->chunks;
            hashtable->chunks = newChunk;
        }

        newChunk->used += size;
        newChunk->ntuples += 1;

        return newChunk->data;
    }

    /*
     * See if we have enough space for it in the current chunk (if any).
     * If not, allocate a fresh chunk.
     */
    if ((hashtable->chunks == NULL) || (hashtable->chunks->maxlen - hashtable->chunks->used) < size) {
        /* allocate new chunk and put it at the beginning of the list */
        newChunk = (HashMemoryChunk)MemoryContextAlloc(
            hashtable->batchCxt, offsetof(HashMemoryChunkData, data) + HASH_CHUNK_SIZE);

        newChunk->maxlen = HASH_CHUNK_SIZE;
        newChunk->used = size;
        newChunk->ntuples = 1;

        newChunk->next = hashtable->chunks;
        hashtable->chunks = newChunk;

        return newChunk->data;
    }

    /* There is enough space in the current chunk, let's add the tuple */
    ptr = hashtable->chunks->data + hashtable->chunks->used;
    hashtable->chunks->used += size;
    hashtable->chunks->ntuples += 1;

    /* return pointer to the start of the tuple memory */
    return ptr;
}
