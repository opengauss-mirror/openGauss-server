/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * rowstore.h
 *       Generalized routines for buffer row storage rotine
 * 
 * Description: Generalized data row store routines.
 *
 * What is row store?
 * -------------------
 * Quite like tuple store, row store is for storing the row data(serialized
 * tuple recieved from dn) and providing the disk swap ability. If the tuple
 * size stored in RowStore exceeds the max memory allowed (work_mem), the
 * following coming data will be written into disk.
 *
 * You might ask why don't we use tuple/batch store instead?
 * (1) for performance. The tuple/batch store will de-serialize data when store
 * the row data. That cause un-needed CPU overhead. That's why we have row store
 * which have the ability to put the row data in its original format.
 * (2) the tuple/batch store cannot store the nodeid of a row.
 *
 *
 * The design of row store
 * ------------------------
 * row store coming with the features such as
 * (1) FIFO: which perserved data order.
 * (2) DiskOnDemand: the comming data will be written to disk only when the
 *     memory is full. and unlike tuple/batch store all data(including the data
 *     in the memory) is written to disk, the rowstore only write those data
 *     who cannot fit into memory.
 * (3) O(1) for read and write. The data row is directly read/write from/to the
 *     node's bank. Because we allocate a bank for each node.
 *
 *
 * The internal of row store
 * ------------------------
 * The data structure for the row store is as follows:
 *
 * [row store]
 *   bank[1]---->[m][m][m].....[m][d][d][d]......[d]
 *   bank[2]---->[m][m][m].....[m][d][d][d]......[d]
 *   bank[3]---->[m][m][m].....[m][d][d][d]......[d]
 *   bank[N]---->[m][m][m].....[m][d][d][d]......[d]
 *               *----in mem----*------in disk-----*
 *
 * The row store has many banks(NumDataNodes), each datanode has its own bank.
 * Each bank contains many cells. The input data row is directed to its own
 * bank based on the msgid, and append to the bank's cell list. If the bank
 * has enough memory to hold the cell, the cell will be appeded to the in mem
 * list, otherwise will be append to the in disk list. The cell will be compressed
 * to save both memory and diskspace.
 *
 * Relation of structs
 * -------------------------
 * RowStoreManager: holds all meta-data for a row store and have one bank for each datanode
 * Bank: holds all cells of a given datanode
 * RowStoreCell: each datarow convert to a cell and stored in bank
 *
 * introduce to the APIs
 * -------------------------
 * First of all, have to invoke RowStoreAlloc() to initialize a RowStoreManager
 * And then you can use RowStoreInsert() to insert DataRow into the RowStore.
 * If the row store contains any data, you can use RowStoreFetch() to get the data
 * in the row store.
 * Finally, you have to invoke RowStoreDestory() to do the clean up work.
 *
 * IDENTIFICATION
 *        src/include/utils/rowstore.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef ROWSTORE_H
#define ROWSTORE_H

#include "storage/buf/buffile.h"
#include "utils/memprot.h"
#include "utils/memutils.h"
#include "pgxc/pgxcnode.h"

#ifdef USE_ASSERT_CHECKING
#define CHECK_MEM(rs) MemoryContextCheck(rs->context, rs->context->session_id > 0)
#else
#define CHECK_MEM(rs)
#endif

#define ROW_STORE_MAX_MEM \
    (u_sess->attr.attr_memory.work_mem * 1024L) /* use u_sess->attr.attr_memory.work_mem, default 64MB */

/*
 * bank holds all cell of a given datanode
 */
typedef struct Bank {
    List* mem;           /* in memory list */
    BufFile* file;       /* in disk list */
    size_t max_size;     /* max memory allowed by this bank */
    size_t current_size; /* current memory allocated for this bank */
    int write_fileno;    /* disk file no for write */
    off_t write_offset;  /* file no offset for write */
    int read_fileno;     /* disk file no for read */
    off_t read_offset;   /* file no offset for read */
    int length;          /* how many tuples stored in this bank */
} Bank;

/*
 * each row convert to a cell and stored in the bank
 */
typedef struct RowStoreCellData {
    char* msg;   /* data row msg, might be compressed */
    int msglen;  /* length of the data row message */
    int msgnode; /* node number of the data row message */
    int clen;    /* compressed length of the msg */
} RowStoreCellData;

typedef RowStoreCellData* RowStoreCell;

/*
 * row store manager holds all meta-data for a row store.
 */
typedef struct RowStoreManagerData {

    MemoryContext context; /* memory context for the RowStore */
    ResourceOwner resowner;     /* resource owner for the RowStore */

    Bank** cn_banks;          /* each coordinator has its own bank */
    int cn_bank_num;          /* total number of cn_banks, should equal to NumCoordinates */

    Bank** dn_banks;          /* each datanode has its own bank */
    int dn_bank_num;          /* total number of dn_banks, should equal to NumDataNodes */
} RowStoreManagerData;


typedef RowStoreManagerData* RowStoreManager;

/*===============API================*/

/* initialize a row store */
extern RowStoreManager RowStoreAlloc(MemoryContext context, size_t max_size, ResourceOwner resowner = NULL);

/* insert a value into row store */
extern void RowStoreInsert(RowStoreManager rs, RemoteDataRow data);

/* fetch data row based on node id */
extern bool RowStoreFetch(RowStoreManager rs, Oid nodeid, RemoteDataRow datarow);

/* fetch data row */
extern bool RowStoreFetch(RowStoreManager rs, RemoteDataRow datarow);

/* get the length of row store */
extern int RowStoreLen(RowStoreManager rs);

/* reset a row store */
extern void RowStoreReset(RowStoreManager rs);

/* distory a row store */
extern void RowStoreDestory(RowStoreManager rs);

#endif
