/*
 * Copyright (c) Huawei Technologies Co.,Ltd. 2020-2020. All rights reserved.
 */

#include <iostream>
#include <sstream>
#include <cstdio>
#include <getopt.h>

#include "postgres.h"
#include "access/smb.h"
#include "storage/smgr/relfilenode.h"
#include "storage/buf/block.h"
#include "storage/matrix_mem.h"
#include "../pg_basebackup/streamutil.h"

static constexpr auto SIZE_KB = (uint64) 1024;
static constexpr auto SIZE_MB = (uint64) 1024 * 1024;
static constexpr auto SIZE_GB = (uint64) 1024 * 1024 * 1024;
static constexpr auto MAX_TOTAL_SIZE = 50 * SIZE_GB;
static constexpr auto BASE_NID = "";
static constexpr auto MAX_LINE_LENGTH = 1024;
static constexpr auto VALID_PARA_NUM = 2;
static constexpr auto PARAM_NAME = "max_smb_memory";
static constexpr auto ENV_PGDATA = "PGDATA";

static char confPath[MAXPGPATH];
static char* g_matrixMemLibPath;
static uint64 ParseSize(const char *str);
static const char* PROGNAME;

#define SMB_MAX_ITEM_SIZE (bufNums * sizeof(smb_recovery::SMBBufItem))
#define SMB_ITEM_SIZE_PERMETA (SMB_MAX_ITEM_SIZE / smb_recovery::SMB_BUF_MGR_NUM)
#define SMB_BUFMETA_SIZE \
    (SMB_ITEM_SIZE_PERMETA + SMB_WRITER_BUCKET_SIZE_PERMETA + 2 * sizeof(int) + 2 * sizeof(XLogRecPtr))
#define SMB_MAX_BUCKET_SIZE (10 * SIZE_KB * 200 * sizeof(int))

static void CheckInputForSecurity(char* inputEnvValue)
{
    if (inputEnvValue == nullptr) {
        return;
    }

    const char* dangerCharacterList[] = {"|", ";", "&", "$", "<", ">", "`", "\\", "!", "\n", nullptr};

    for (int i = 0; dangerCharacterList[i] != nullptr; i++) {
        if (strstr(static_cast<const char*>(inputEnvValue), dangerCharacterList[i]) != nullptr) {
            pg_log(PG_WARNING,
                "Error: variable \"%s\" token invalid symbol \"%s\".\n",
                inputEnvValue,
                dangerCharacterList[i]);
            exit(1);
        }
    }
}

static char* Xstrdup(const char* s)
{
    char* result = nullptr;

    result = strdup(s);
    if (result == nullptr) {
        pg_log(PG_PRINT, _("%s: out of memory\n"), PROGNAME);
        exit(1);
    }
    return result;
}

static int GetSMBItemEachMgr(int bufNums)
{
    return ((bufNums) / smb_recovery::SMB_BUF_MGR_NUM);
}

static smb_recovery::SMBBufItem *SMBIdGetItem(int id, int bufNums, char *mgr)
{
    smb_recovery::SMBBufItem *item;
    int mgrId = id / GetSMBItemEachMgr(bufNums);
    if (id > bufNums) {
        pg_log(PG_ERROR, _("FATAL: SMB buffer id %d out of range [0, %d].\n"), id, bufNums);
        exit(EXIT_FAILURE);
    }
    item = (smb_recovery::SMBBufItem *)(mgr + mgrId * SMB_BUFMETA_SIZE);
    return (item + id % GetSMBItemEachMgr(bufNums));
}

static void GetUsedBufNums(int bufNums, int &usedBufs, int curId, char *mgr)
{
    smb_recovery::SMBBufItem *cur;
    while (curId != SMB_INVALID_ID) {
        cur = SMBIdGetItem(curId, bufNums, mgr);
        if (curId == cur->nextLRUId) {
            break;
        }
        usedBufs++;
        curId = cur->nextLRUId;
    }
}

static bool GetShmChunkNum(int &bufNums, int &shmChunkNum, const char *paramName)
{
    uint64 cacheSize;
    char line[MAX_LINE_LENGTH];

    FILE *file = fopen(confPath, "r");
    if (file == nullptr) {
        pg_log(PG_ERROR, _("Failed to open %s.\n"), confPath);
        return false;
    }

    while (fgets(line, sizeof(line), file) != nullptr) {
        if (line[0] == '#' || line[0] == '\n' || strstr(line, PARAM_NAME) == nullptr) {
            continue;
        }

        cacheSize = ParseSize(strchr(line, '=') + 1);
        bufNums = cacheSize / BLCKSZ;
        shmChunkNum = TYPEALIGN(MAX_RACK_ALLOC_SIZE, cacheSize) / MAX_RACK_ALLOC_SIZE;
    }

    fclose(file);
    if (shmChunkNum == 0) {
        pg_log(PG_ERROR, _("SMB: invalid shm chunk num.\n"));
        return false;
    }
    return true;
}

static bool DeleteAllShmChunks(int shmChunkNum, char* shmChunkName)
{
    bool isDeleted = true;
    int ret = RackMemShmDelete(SHARED_MEM_NAME);
    if (ret != 0) {
        isDeleted = false;
        pg_log(PG_ERROR, _("SMB: failed to delete share memory %s, code is [%d].\n"), SHARED_MEM_NAME, ret);
    }
    for (int i = 0; i < shmChunkNum; i++) {
        smb_recovery::GetSmbShmChunkName(shmChunkName, i);
        ret = RackMemShmDelete(shmChunkName);
        if (ret != 0) {
            isDeleted = false;
            pg_log(PG_ERROR, _("SMB: failed to delete share memory %s, code is [%d].\n"), shmChunkName, ret);
        }
    }
    return isDeleted;
}

static bool CreateShmChunk(int deleteChunkNum, char* name, SHMRegions* regions)
{
    int ret = 0;
    void* data = RackMemShmMmap(nullptr, MAX_RACK_ALLOC_SIZE, PROT_READ | PROT_WRITE,
                                MAP_SHARED, name, 0);
    if (data == nullptr) {
        ret = RackMemShmCreate(name, MAX_RACK_ALLOC_SIZE, BASE_NID, &regions->region[0]);
        if (ret != 0) {
            pg_log(PG_ERROR, _("SMB: create share memory %s failed, code is [%d].\n"), name, ret);
            DeleteAllShmChunks(deleteChunkNum, name);
            return false;
        }
    } else {
        pg_log(PG_PRINT, _("SMB: %s already exists, it will not be create again.\n"), name);
        RackMemShmUnmmap(data, MAX_RACK_ALLOC_SIZE);
    }
    return true;
}

bool SMBWriterMemCreate()
{
    int ret = 0;
    int shmChunkNum;
    int bufNums;
    char shmChunkName[MAX_SHM_CHUNK_NAME_LENGTH] = "";

    if (!GetShmChunkNum(bufNums, shmChunkNum, PARAM_NAME)) {
        return false;
    }

    SHMRegions regions = SHMRegions();
    ret = RackMemShmLookupShareRegions(BASE_NID, ShmRegionType::INCLUDE_ALL_TYPE, &regions);
    if (ret != 0 || regions.region[0].num <= 0) {
        pg_log(PG_ERROR,
            _("SMB: lookup share memory regions failed, code is [%d], node num: [%d].\n"), ret, regions.region[0].num);
        return false;
    }

    /* meta data */
    if (!CreateShmChunk(0, SHARED_MEM_NAME, &regions)) {
        return false;
    }

    /* buf data */
    for (int i = 0; i < shmChunkNum; i++) {
        smb_recovery::GetSmbShmChunkName(shmChunkName, i);
        if (!CreateShmChunk(i + 1, shmChunkName, &regions)) {
            return false;
        }
    }

    return true;
}

void SMBWriterMemDelete()
{
    int shmChunkNum;
    int bufNums;
    char shmChunkName[MAX_SHM_CHUNK_NAME_LENGTH] = "";

    if (!GetShmChunkNum(bufNums, shmChunkNum, PARAM_NAME)) {
        return;
    }

    if (DeleteAllShmChunks(shmChunkNum, shmChunkName)) {
        pg_log(PG_PRINT, _("SMB: succeed to delete all share memory.\n"));
    }
}

static void PrintSMBUsedInfos(char *mgr, int bufNums)
{
    int usedBufs = 0;
    float bufUseRate = 0.0f;
    int curId;
    int *current;
    int *tail;
    uint32 shiftSize = 32;
    smb_recovery::SMBBufItem *curBufItem;
    smb_recovery::SMBBufItem *tailBufItem;
    XLogRecPtr headLsn;
    XLogRecPtr tailLsn;
    XLogRecPtr startLsn = InvalidXLogRecPtr;
    XLogRecPtr endLsn = MAX_XLOG_REC_PTR;

    for (int i = 0; i < smb_recovery::SMB_BUF_MGR_NUM; i++) {
        current = reinterpret_cast<int*>(mgr + i * SMB_BUFMETA_SIZE +
                                        SMB_ITEM_SIZE_PERMETA + SMB_WRITER_BUCKET_SIZE_PERMETA);
        curId = *current;
        tail = current + 1;
        while (curId != *tail && XLogRecPtrIsInvalid(SMBIdGetItem(curId, bufNums, mgr)->lsn)) {
            curId = SMBIdGetItem(curId, bufNums, mgr)->nextLRUId;
        }
        curBufItem = SMBIdGetItem(curId, bufNums, mgr);
        headLsn = curBufItem->lsn;
        tailBufItem = SMBIdGetItem(*tail, bufNums, mgr);
        tailLsn = tailBufItem->lsn;
        startLsn = Max(startLsn, headLsn);
        endLsn = Min(endLsn, tailLsn);
        pg_log(PG_PRINT, _("SMB Buffer Manager %d, head lsn: %X/%X, tail lsn: %X/%X.\n"),
            i, static_cast<uint32>(headLsn >> shiftSize), static_cast<uint32>(headLsn),
            static_cast<uint32>(tailLsn >> shiftSize), static_cast<uint32>(tailLsn));
        GetUsedBufNums(bufNums, usedBufs, curId, mgr);
    }
    pg_log(PG_PRINT, _("SMB start lsn: %X/%X, end lsn: %X/%X.\n"),
           static_cast<uint32>(startLsn >> shiftSize), static_cast<uint32>(startLsn),
           static_cast<uint32>(endLsn >> shiftSize), static_cast<uint32>(endLsn));

    if (bufNums != 0) {
        bufUseRate = static_cast<float>(usedBufs) / bufNums * 100.0f;
    }
    pg_log(PG_PRINT, _("SMB: buffer usage rate: %.2f%%.\n"), bufUseRate);
}

void QuerySMBShmState()
{
    int ret = 0;
    int shmChunkNum;
    int bufNums;

    char* mgr = static_cast<char*>(RackMemShmMmap(nullptr, MAX_RACK_ALLOC_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED,
        SHARED_MEM_NAME, 0));

    if (mgr == nullptr) {
        pg_log(PG_ERROR,  _("SMB: failed to mmap share memory %s.\n"), SHARED_MEM_NAME);
        RackMemShmUnmmap(mgr, MAX_RACK_ALLOC_SIZE);
        return;
    }

    if (!GetShmChunkNum(bufNums, shmChunkNum, PARAM_NAME)) {
        ret = RackMemShmUnmmap(mgr, MAX_RACK_ALLOC_SIZE);
        if (ret != 0) {
            pg_log(PG_ERROR, _("SMB: failed to unmap share memory %s, code is [%d].\n"), SHARED_MEM_NAME, ret);
        }
        return;
    }

    PrintSMBUsedInfos(mgr, bufNums);
    ret = RackMemShmUnmmap(mgr, MAX_RACK_ALLOC_SIZE);
    if (ret != 0) {
        pg_log(PG_ERROR, _("SMB: failed to unmap share memory %s, code is [%d].\n"), SHARED_MEM_NAME, ret);
    }
}

static uint64 ParseSize(const char *str)
{
    char *endPtr;
    uint64 result = strtoll(str, &endPtr, 10);
    if (*endPtr == 'G' || *endPtr == 'g') {
        result *= SIZE_GB;
    } else if (*endPtr == 'M' || *endPtr == 'm') {
        result *= SIZE_MB;
    } else if (*endPtr == 'K' || *endPtr == 'k') {
        result *= SIZE_KB;
    } else {
        return 0;
    }

    if (result > MAX_TOTAL_SIZE) {
        result = MAX_TOTAL_SIZE;
        pg_log(PG_PRINT, _("SMB: applied memory is too large, max allocsize is %dGB.\n"),
            MAX_TOTAL_SIZE / SIZE_GB);
    }
    return result;
}

static void Usage(void)
{
    printf(_("Usage: smb_mgr [options]\n"));
    printf(_("  start [-D DATABASE_DIR] [-L MATRIX_MEM_PATH]    SMB shared memory allocate.\n"));
    printf(_("  stop  [-D DATABASE_DIR] [-L MATRIX_MEM_PATH]    SMB shared memory release.\n"));
    printf(_("  state [-D DATABASE_DIR] [-L MATRIX_MEM_PATH]    SMB shared memory usage status.\n"));
    printf(_("  Common options:\n"));
    printf(_("  -D                     Path of database cluster.\n"));
    printf(_("  -L                     Path of matrix memory dynamic library.\n"));
    printf(_("  -h|--help              show this help, then exit.\n"));
}

static int GetOpts(int argc, char *argv[])
{
    int optionValue;
    while ((optionValue = getopt(argc, argv, "D:L:")) != -1) {
        switch (optionValue) {
            case 'D': {
                char *pgdenv = nullptr;
                CheckInputForSecurity(optarg);
                pgdenv = Xstrdup(optarg);
                canonicalize_path(pgdenv);
                setenv(ENV_PGDATA, pgdenv, 1);
                break;
            }
            case 'L':
                CheckInputForSecurity(optarg);
                g_matrixMemLibPath = Xstrdup(optarg);
                break;
            default:
                pg_log(PG_ERROR, _("Try \"%s --help\" for more information.\n"), PROGNAME);
                return -1;
        }
    }
    return 0;
}

int main (int argc, char* argv[])
{
    int nRet = 0;
    char *pgData = nullptr;
    g_matrixMemLibPath = "libmemfabric_client.so";

    PROGNAME = get_progname(argv[0]);
    if (argc < VALID_PARA_NUM || strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-h") == 0) {
        Usage();
        exit(0);
    }

    if (GetOpts(argc, argv) == -1) {
        exit(1);
    }

    pgData = Xstrdup(getenv(ENV_PGDATA));
    canonicalize_path(pgData);
    nRet = snprintf_s(confPath, MAXPGPATH, MAXPGPATH - 1, "%s/postgresql.conf", pgData);
    securec_check_ss_c(nRet, "\0", "\0");

    MatrixMemFuncInit(g_matrixMemLibPath);
    if (strcmp(argv[optind], "start") == 0) {
        if (!SMBWriterMemCreate()) {
            pg_log(PG_ERROR, _("SMB create share memory failed.\n"));
            exit(1);
        }
        pg_log(PG_PRINT, _("SMB create share memory success.\n"));
    } else if (strcmp(argv[optind], "stop") == 0) {
        SMBWriterMemDelete();
    } else if (strcmp(argv[optind], "state") == 0) {
        QuerySMBShmState();
    } else {
        pg_log(PG_ERROR, _("error command, use start, stop or state.\n"));
        exit(1);
    }

    return 0;
}