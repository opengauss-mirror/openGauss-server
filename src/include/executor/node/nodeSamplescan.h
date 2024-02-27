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
 * nodeSamplescan.h
 *        Table Sample Scan header file, include class and class member declaration.
 * 
 * 
 * IDENTIFICATION
 *        src/include/executor/nodeSamplescan.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef NODESAMPLESCAN_H
#define NODESAMPLESCAN_H

#include "access/relscan.h"
#include "nodes/execnodes.h"
#include "vecexecutor/vecnodes.h"

#define NEWBLOCK 0   /* Identify we have got a new block. */
#define NONEWBLOCK 1 /* Identify we are under scanning tuple in old block. */

#define MIN_PERCENT_ARG 0.000001   /* Minimum value of percent. */
#define MAX_PERCENT_ARG 100 /* Maxmum value of percent. */

#define MIN_SEED_ARG 0   /* Minimum value of seed. */
#define MAX_SEED_ARG 4294967295 /* Maxmum value of seed. */


/* The flag identify the tuple or block is valid or not for sample scan. */
typedef enum { VALIDDATA, NEXTDATA, INVALIDBLOCKNO, INVALIDOFFSET } ScanValid;

/* The flag identify the sample scan state. */
typedef enum { GETMAXBLOCK, GETBLOCKNO, GETMAXOFFSET, GETOFFSET, GETDATA } SampleScanFlag;

/* Table sample scan base class.*/
class BaseTableSample : public BaseObject {
public:
    BaseTableSample(void* scanstate);
    virtual ~BaseTableSample();
    void resetSampleScan();

public:
    ScanState* sampleScanState;
    CStoreScanState* vecsampleScanState;
    int runState;
    int scanTupState;
    uint32 seed;
    double* percent;
    BlockNumber totalBlockNum;
    /* current block to consider sampling. To system table, next blosk need be chosen.*/
    BlockNumber currentBlock;
    /* last tuple returned from current block.*/
    OffsetNumber currentOffset;
    /* how many tuples in current page. */
    OffsetNumber curBlockMaxoffset;
    bool finished;
    unsigned short rand48Seed[3];
    void (BaseTableSample::*nextSampleBlock_function)();
    void (BaseTableSample::*nextSampleTuple_function)();

private:
    void getSeed();
    void getPercent();
    void system_nextsampleblock();
    void system_nextsampletuple();
    void bernoulli_nextsampleblock();
    void bernoulli_nextsampletuple();
};

/* Row table sample scan.*/
class RowTableSample : public BaseTableSample {
public:
    RowTableSample(ScanState* scanstate);
    virtual ~RowTableSample();
    ScanValid scanTup();
    HeapTuple scanSample();
    void getMaxOffset();
};

/* Ustore table sample scan. */
class UstoreTableSample : public BaseTableSample {
public:
    UstoreTableSample(ScanState* scanstate);
    virtual ~UstoreTableSample();
    ScanValid scanTup();
    UHeapTuple scanSample();
    void getMaxOffset();
};

/* Column table sample scan.*/
class ColumnTableSample : public BaseTableSample {
private:
    uint16* offsetIds;
    uint32 currentCuId;
    int batchRowCount;
    VectorBatch* tids;

public:
    ColumnTableSample(CStoreScanState* scanstate);
    virtual ~ColumnTableSample();
    void resetVecSampleScan();
    void getBatchBySamples(VectorBatch* vbout);
    ScanValid scanBatch(VectorBatch* batch);
    void scanVecSample(VectorBatch* batch);
    void getMaxOffset();
};

extern TableScanDesc InitSampleScanDesc(ScanState *scanstate, Relation currentRelation);
extern TupleTableSlot* SeqSampleNext(SeqScanState *node);
extern TupleTableSlot* HbktSeqSampleNext(SeqScanState *node);

#endif /* NODESAMPLESCAN_H */

