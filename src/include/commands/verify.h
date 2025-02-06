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
 * verify.h
 *        header file for anlayse verify commands to check the datafile.
 * 
 * 
 * IDENTIFICATION
 *        src/include/commands/verify.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef VERIFY_H
#define VERIFY_H

#include "pgxc/pgxcnode.h"
#include "tcop/tcopprot.h"
#include "access/htup.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_auth_members.h"
#include "catalog/namespace.h"
#include "catalog/pg_database.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_partition_fn.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_user_status.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_type.h"
#include "nodes/parsenodes.h"
#include "storage/buf/buf.h"
#include "storage/buf/bufmgr.h"
#include "storage/cu.h"
#include "storage/lock/lock.h"
#include "storage/remote_read.h"
#include "utils/relcache.h"
#include "postmaster/pagerepair.h"
#include "storage/copydir.h"
#include "utils/inval.h"
#include "utils/timestamp.h"
#include "storage/lock/lwlock.h"
#include "utils/hsearch.h"
#include "c.h"
#include "storage/buf/bufmgr.h"
#include "storage/buf/block.h"
#include "storage/smgr/smgr.h"
#include "storage/checksum.h"
#include "storage/smgr/segment.h"
#include "fmgr.h"
#include "funcapi.h"
#include "storage/remote_adapter.h"
#include "utils/relmapper.h"
#include "utils/relcache.h"
#include "access/sysattr.h"
#include "pgstat.h"
#include "access/double_write.h"
#include "miscadmin.h"
#include "utils/relfilenodemap.h"

#define FAIL_RETRY_MAX_NUM 5
#define REGR_MCR_SIZE_1MB 1048576
#define REGR_MCR_SIZE_1GB 1073741824

extern void DoGlobalVerifyMppTable(VacuumStmt* stmt, const char* queryString, bool sentToRemote);
extern void DoGlobalVerifyDatabase(VacuumStmt* stmt, const char* queryString, bool sentToRemote);
extern void DoVerifyTableOtherNode(VacuumStmt* stmt, bool sentToRemote);
extern void VerifyAbortBufferIO(void);
extern bool isCLogOrCsnLogPath(char* path);
extern bool gsRepairCsnOrCLog(char* path, int timeout);
extern bool isSegmentPath(char* path, uint32* relfileNode);
extern void getRelfiNodeAndSegno(char* str, const char *delim, char **relfileNodeAndSegno);
extern bool gsRepairFile(Oid tableOid, char* path, int timeout);

#define BAD_BLOCK_NAME_LEN = 128;

const int ERR_MSG_LEN = 512;

typedef struct BadFileItem {
    Oid reloid;
    NameData relname;
    char relfilepath[MAX_PATH];
} BadFileItem;

extern void addGlobalRepairBadBlockStat(const RelFileNodeBackend &rnode, ForkNumber forknum,
    BlockNumber blocknum);
extern void UpdateRepairTime(const RelFileNode &rnode, ForkNumber forknum, BlockNumber blocknum);
extern void initRepairBadBlockStat();
extern void verifyAndTryRepairPage(char* path, int blockNum, bool verify_mem, bool is_segment);
extern void PrepForRead(char* path, uint blocknum, bool is_segment, RelFileNode *relnode);
extern Buffer PageIsInMemory(SMgrRelation smgr, ForkNumber forkNum, BlockNumber blockNum);
extern void resetRepairBadBlockStat();
extern bool repairPage(char* path, uint blocknum, bool is_segment, int timeout);
extern bool tryRepairPage(BlockNumber blocknum, bool is_segment, RelFileNode *relnode, int timeout);
extern char* relSegmentDir(RelFileNode rnode, ForkNumber forknum);
extern List* getSegmentMainFilesPath(char* segmentDir, char split, int num, bool isCompressed);
extern List* appendIfNot(List* targetList, Oid datum);
extern uint32 getSegmentFileHighWater(char* path);
extern int getIntLength(uint32 intValue);
extern List* getPartitionBadFiles(Relation tableRel, List* badFileItems, Oid relOid);
extern int getMaxSegno(RelFileNode* prnode);
extern List* getTableBadFiles(List* badFileItems, Oid relOid, Form_pg_class classForm, Relation tableRel);
extern List* getSegmentBadFiles(List* spcList, List* badFileItems);
extern List* getSegnoBadFiles(char* path, int maxSegno, Oid relOid, char* tabName, List* badFileItems, bool isCompressed);
extern List* appendBadFileItems(List* badFileItems, Oid relOid, char* tabName, char* path);
extern List* getNonSegmentBadFiles(List* badFileItems, Oid relOid, Form_pg_class classForm, Relation tableRel);
extern void gs_verify_page_by_disk(SMgrRelation smgr, ForkNumber forkNum, int blockNum, char* disk_page_res);
extern void splicMemPageMsg(bool isPageValid, bool isDirty, char* mem_page_res);
extern bool isNeedRepairPageByMem(char* disk_page_res, BlockNumber blockNum, char* mem_page_res,
                                  bool isSegment, RelFileNode relnode);
extern void gs_tryrepair_compress_extent(SMgrRelation reln, BlockNumber logicBlockNumber);
extern int CheckAndRenameFile(char* path);
extern void BatchClearBadBlock(const RelFileNode rnode, ForkNumber forknum, BlockNumber startblkno);
extern void df_close_all_file(RepairFileKey key, int32 max_sliceno);

Datum local_bad_block_info(PG_FUNCTION_ARGS);
Datum local_clear_bad_block_info(PG_FUNCTION_ARGS);
Datum gs_repair_page(PG_FUNCTION_ARGS);
Datum gs_verify_and_tryrepair_page(PG_FUNCTION_ARGS);
Datum gs_verify_data_file(PG_FUNCTION_ARGS);
Datum gs_repair_file(PG_FUNCTION_ARGS);
Datum remote_bad_block_info(PG_FUNCTION_ARGS);
Datum gs_read_segment_block_from_remote(PG_FUNCTION_ARGS);
#endif /* VERIFY_H */
