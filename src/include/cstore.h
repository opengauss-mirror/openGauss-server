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
 * cstore.h
 *        Common head file to support ColStore
 * 
 * 
 * IDENTIFICATION
 *        src/include/cstore.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CSTORE_H
#define CSTORE_H

#include "storage/smgr/relfilenode.h"
#include "vecexecutor/vectorbatch.h"

/* 0~FirstCUID is reserved for page id of delta table
 * We want to keep delta table small
 */
const int FirstCUID = 1000;
/*
 * The size of full CU by default
 */
#define DefaultFullCUSize ((int)BatchMaxSize * 60)

/*
 * Invalid CU ID
 */
const uint32 InValidCUID = 0xffffffff;

/*
 * Dictionary virtual CU Id
 */
const uint32 DicVirtualCUID = 0xfffffffe;

/* Warning threshold of CUID */
const uint32 CUIDWarningThreshold = 0xfff00000;

/* Max CUID */
const uint32 MaxCUID = 0xfffffff0;

/* The ID of system-defined attributes have used from -1 to -9.
 * look up system-define attributes. you can refer to file sysattr.h
 */
const int VitrualDelColID = -10;

const int VirtualSpaceCacheColID = -11;

/* The attribute number of CUDesc table */
const int CUDescColIDAttr = 1;
const int CUDescCUIDAttr = 2;
const int CUDescMinAttr = 3;
const int CUDescMaxAttr = 4;
const int CUDescRowCountAttr = 5;
const int CUDescCUModeAttr = 6;
const int CUDescSizeAttr = 7;
const int CUDescCUPointerAttr = 8;
const int CUDescCUMagicAttr = 9;
const int CUDescCUExtraAttr = 10;

// The column number of CUDesc table.
// If that table schema change, we should change this const variable
//
#define CUDescMaxAttrNum 10

typedef uint64 CUPointer;

/*
 * Judge whether valid CUID
 */
#define IsValidCUID(CUId) ((CUId) != InValidCUID)

/*
 * Judge whether dictionary CUID
 */
#define IsDicVCU(CUId) ((CUId) == DicVirtualCUID)

/*
 * File node of GsCStore. It includes file type.
 */
typedef struct CFileNode : public BaseObject {
    RelFileNode m_rnode;
    ForkNumber m_forkNum;
    int m_attid;

    CFileNode(RelFileNode rnode, ForkNumber forkNum = MAIN_FORKNUM)
    {
        m_rnode = rnode;
        m_rnode.bucketNode = -1;
        m_forkNum = forkNum;
        m_attid = -1;
    }

    CFileNode(RelFileNode rnode, int attid, ForkNumber forkNum = MAIN_FORKNUM)
    {
        m_rnode = rnode;
        m_rnode.bucketNode = -1;
        m_forkNum = forkNum;
        m_attid = attid;
    }

    CFileNode(const CFileNode& cFilenode)
    {
        m_rnode = cFilenode.m_rnode;
        m_rnode.bucketNode = -1;
        m_forkNum = cFilenode.m_forkNum;
        m_attid = cFilenode.m_attid;
    }
} CFileNode;

/*
 * CStorage allocate strategy.
 */
typedef enum { APPEND_ONLY, USING_FREE_SPACE } CStoreAllocateStrategy;

#endif

