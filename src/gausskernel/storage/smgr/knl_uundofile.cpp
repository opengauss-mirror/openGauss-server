/* -------------------------------------------------------------------------
 *
 * knl_uundofile.cpp
 *    c++ code
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/smgr/knl_uundofile.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "access/xlog.h"
#include "access/ustore/knl_whitebox_test.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgwriter.h"
#include "storage/smgr/fd.h"
#include "storage/vfd.h"
#include "storage/smgr/smgr.h"
#include "utils/memutils.h"

#define UNDODEBUGINFO , __FUNCTION__, __LINE__
#define UNDODEBUGSTR "[%s:%d]"
#define UNDOFORMAT(f) UNDODEBUGSTR f UNDODEBUGINFO

/* Populate a file tag describing an undofile.cpp segment file. */
#define INIT_UNDO_FILE_TAG(tag, rNode, segNo)                               \
    do                                                                      \
    {                                                                       \
        errno_t errorno = EOK;                                              \
        errorno = memset_s(&(tag), sizeof(FileTag), (0), sizeof(FileTag));  \
        securec_check(errorno, "\0", "\0");                                 \
        (tag).handler = SYNC_HANDLER_UNDO;                                  \
        (tag).forknum = MAIN_FORKNUM;                                       \
        (tag).rnode = (rNode);                                              \
        (tag).segno = (segNo);                                              \
    } while (false);

/*
 * While md.c expects random access and has a small number of huge
 * segments, undofile.c manages a potentially very large number of smaller
 * segments and has a less random access pattern.  Therefore, instead of
 * keeping a potentially huge array of vfds we'll just keep the most
 * recently accessed.
 */
typedef struct _UndoFileState {
    int segno;
    File file;
} UndoFileState;

const int UNDO_FILE_MAXSIZE = 1024 * 1024;
/* Number of blocks of BLCKSZ in an undo log segment file.  128 = 1MB. */
const int UNDO_FILE_BLOCKS = UNDO_FILE_MAXSIZE / BLCKSZ;
const int UNDO_META_MAXSIZE = 1024 * 32;
const int UNDO_META_BLOCKS = UNDO_META_MAXSIZE / BLCKSZ;
const int ZONEID_CHARS = 5;
const int FILENO_CHARS = 7;

/* path = undo/zid.fileno. */
const int UNDO_DIR_LEN = 5;
const int UNDO_FILE_DIR_LEN = 15;
const int UNDO_FILE_PATH_LEN = UNDO_FILE_DIR_LEN + 1 + ZONEID_CHARS + 1 + FILENO_CHARS + 6;

const char *UNDO_FILE_DIR_PREFIX = "undo";
const char *UNDO_PERMANENT_DIR = "undo/permanent";
const char *UNDO_UNLOGGED_DIR = "undo/unlogged";
const char *UNDO_TEMP_DIR = "undo/temp";

static const int UNDO_FILE_EXTEND_PAGES = 8;

/* local routines */
static UndoFileState *AllocUndoFileState(void);
static void GetUndoFilePath(int zoneId, uint32 dbId, int segno, char *path, int len);
static UndoFileState *OpenUndoFile(SMgrRelation reln, ForkNumber forknum, BlockNumber blockNum, ExtensionBehavior behavior);
static void RegisterDirtyUndoSegment(SMgrRelation reln, const UndoFileState *state);
static void RegisterForgetUndoRequests(RelFileNodeBackend rnode, uint32 segno);
static void RegisterUnlinkUndoRequests(RelFileNodeBackend rnode, uint32 segno);
static BlockNumber GetUndoFileBlocks(SMgrRelation reln, ForkNumber forknum, const UndoFileState *state);
void CheckUndoFileDirectory(UndoPersistence upersistence);

static inline uint32 UNDO_FILE_SIZE(uint32 dbId)
{
    if (dbId == UNDO_DB_OID) {
        return UNDO_FILE_MAXSIZE;
    }
    return UNDO_META_MAXSIZE;
}

static inline uint32 UNDO_FILE_BLOCK(uint32 dbId)
{
    if (dbId == UNDO_DB_OID) {
        return UNDO_FILE_BLOCKS;
    }
    return UNDO_META_BLOCKS;
}

/* allocate UndoFileState memory. */
static UndoFileState *AllocUndoFileState(void)
{
    MemoryContext current;
    if (EnableLocalSysCache()) {
        current = t_thrd.lsc_cxt.lsc->lsc_mydb_memcxt;
    } else {
        current = u_sess->storage_cxt.UndoFileCxt;
    }
    return (UndoFileState *)MemoryContextAlloc(current, sizeof(UndoFileState));
}

static inline void SetUndoFileState(UndoFileState *state, int segno, File file)
{
    state->segno = segno;
    state->file = file;
}

void InitUndoFile(void)
{
    if (EnableLocalSysCache()) {
        return;
    }
    Assert(u_sess->storage_cxt.UndoFileCxt == NULL);
    u_sess->storage_cxt.UndoFileCxt = AllocSetContextCreate(u_sess->top_mem_cxt, "UndoFileSmgr", ALLOCSET_DEFAULT_SIZES);
}

void GetUndoFileDirectory(char *path, int len, UndoPersistence upersistence)
{
    Assert(len >= UNDO_FILE_DIR_LEN);
    errno_t rc = EOK;
    if (upersistence == UNDO_PERMANENT) {
        rc = snprintf_s(path, len, len - 1, UNDO_PERMANENT_DIR);
    } else if (upersistence == UNDO_UNLOGGED) {
        rc = snprintf_s(path, len, len - 1, UNDO_UNLOGGED_DIR);
    } else {
        rc = snprintf_s(path, len, len - 1, UNDO_TEMP_DIR);
    }
    securec_check_ss(rc, "\0", "\0");
    return;
}

void CheckUndoDirectory(void)
{
    WHITEBOX_TEST_STUB(UNDO_CHECK_DIRECTORY_FAILED, WhiteboxDefaultErrorEmit);

    if (mkdir(UNDO_FILE_DIR_PREFIX, S_IRWXU) < 0 && errno != EEXIST) {
        ereport(ERROR, (errcode_for_file_access(),
            errmsg("could not create directory \"%s\": %m", UNDO_FILE_DIR_PREFIX)));
    }
    return;
}

void CheckUndoFileDirectory(UndoPersistence upersistence)
{
    char dir[UNDO_FILE_DIR_LEN];
    GetUndoFileDirectory(dir, UNDO_FILE_DIR_LEN, upersistence);

    WHITEBOX_TEST_STUB(UNDO_CHECK_FILE_DIRECTORY_FAILED, WhiteboxDefaultErrorEmit);

    if (mkdir(dir, S_IRWXU) != 0 && errno != EEXIST) {
        if (errno != ENOENT || !t_thrd.xlog_cxt.InRecovery) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not create directory \"%s\": %m", dir)));
        }
        if (mkdir(UNDO_FILE_DIR_PREFIX, S_IRWXU) < 0 && errno != EEXIST) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not create directory \"%s\": %m", UNDO_FILE_DIR_PREFIX)));
        }
        if (mkdir(dir, S_IRWXU) != 0 && errno != EEXIST) {
            ereport(ERROR, (errcode_for_file_access(), errmsg("could not create directory \"%s\": %m", dir)));
        }
    }
    return;
}

void CleanUndoFileDirectory(UndoPersistence upersistence)
{
    char dir[UNDO_FILE_DIR_LEN];
    GetUndoFileDirectory(dir, UNDO_FILE_DIR_LEN, upersistence);
    if (!rmtree(dir, false)) {
        ereport(WARNING, (errmsg(UNDOFORMAT("could not remove file \"%s\": %m."), dir)));
    }
    return;
}

void GetUndoFilePath(int zoneId, uint32 dbId, int segno, char *path, int len)
{
    Assert(len >= UNDO_FILE_PATH_LEN);
    char dir[UNDO_FILE_DIR_LEN];
    errno_t rc = EOK;
    DECLARE_NODE_COUNT();
    GET_UPERSISTENCE_BY_ZONEID(zoneId, nodeCount);
    GetUndoFileDirectory(dir, UNDO_FILE_DIR_LEN, upersistence);
    if (dbId == UNDO_DB_OID) {
        rc = snprintf_s(path, len, len - 1, "%s/%05X.%07zX", dir, zoneId, segno);
    } else {
        rc = snprintf_s(path, len, len - 1, "%s/%05X.meta.%07zX", dir, zoneId, segno);
    }
    securec_check_ss(rc, "\0", "\0");
    return;
}

BlockNumber GetUndoFileNblocks(SMgrRelation reln, ForkNumber forknum)
{
    return MaxBlockNumber;
}

/* Get number of blocks present in a single disk undofile. */
static BlockNumber GetUndoFileBlocks(SMgrRelation reln, ForkNumber forknum, const UndoFileState *state)
{
    Assert(state != NULL);

    char *fileName = FilePathName(state->file);
    off_t len = FileSeek(state->file, 0L, SEEK_END);
    uint32 undoFileSize = UNDO_FILE_SIZE(reln->smgr_rnode.node.dbNode);

    WHITEBOX_TEST_STUB(UNDO_GET_FILE_BLOCKS_FAILED, WhiteboxDefaultErrorEmit);

    if (len < 0) {
        CloseUndoFile(reln, forknum, InvalidBlockNumber);
        ereport(ERROR, (errmsg(UNDOFORMAT("could not seek to end of file \"%s\": %m."), fileName)));
    }

    if (len % BLCKSZ != 0) {
        CloseUndoFile(reln, forknum, InvalidBlockNumber);
        ereport(WARNING, (errmsg(UNDOFORMAT("The expected size of file \"%s\" is %d, but the actual size is %ld."),
            fileName, undoFileSize, len)));
    }

    /* note that this calculation will ignore any partial block at EOF */
    return (BlockNumber)(len / BLCKSZ);
}

void CreateUndoFile(SMgrRelation reln, ForkNumber forknum, bool isRedo)
{
    /* Undo file creation is managed by ExtendUndoFile. */
    return;
}

/* Create an undo file, expand the file by 8 pages until the file size reaches 1 MB. */
void ExtendUndoFile(SMgrRelation reln, ForkNumber forknum, BlockNumber blockno, char *buffer, bool skipFsync)
{
    Assert(reln != NULL);

    UndoFileState *state = (UndoFileState *)reln->fileState;
    uint32 undoFileBlocks = UNDO_FILE_BLOCK(reln->smgr_rnode.node.dbNode);
    uint32 undoFileSize = UNDO_FILE_SIZE(reln->smgr_rnode.node.dbNode);
    int segno = -1;
    char path[UNDO_FILE_PATH_LEN];
    File fd;
    volatile uint32 flags = O_RDWR | O_CREAT | PG_BINARY;
    int nbytes;
    off_t seekpos;
    struct stat statBuffer;
    BlockNumber blockNum;
    RelFileNodeForkNum filenode;

    if (blockno == InvalidBlockNumber) {
        ereport(ERROR, (errmsg(UNDOFORMAT("cannot extend undo file beyond %u blocks."), InvalidBlockNumber)));
    }

    if (state == NULL) {
        state = AllocUndoFileState();
        reln->fileState = state;
    } else if (state->file > 0) {
        FileClose(state->file);
    }
    SetUndoFileState(state, -1, -1);

    WHITEBOX_TEST_STUB(UNDO_EXTEND_FILE_FAILED, WhiteboxDefaultErrorEmit);

    ADIO_RUN() {
        flags |= O_DIRECT;
    }
    ADIO_END();

    segno = (int)(blockno / undoFileBlocks);
    GetUndoFilePath(reln->smgr_rnode.node.relNode, reln->smgr_rnode.node.dbNode,
        segno, path, UNDO_FILE_PATH_LEN);
    filenode = RelFileNodeForkNumFill(reln->smgr_rnode, forknum, segno);
    fd = DataFileIdOpenFile(path, filenode, (int)flags, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        int saveErrno = errno;
        DECLARE_NODE_COUNT();
        GET_UPERSISTENCE_BY_ZONEID((int)reln->smgr_rnode.node.relNode, nodeCount);
        CheckUndoFileDirectory(upersistence);
        fd = DataFileIdOpenFile(path, filenode, (int)flags, S_IRUSR | S_IWUSR);
        if (fd < 0) {
            /* be sure to report the error reported by create, not open */
            errno = saveErrno;
            CloseUndoFile(reln, forknum, InvalidBlockNumber);
            ereport(ERROR, (errmsg(UNDOFORMAT("could not create file \"%s\": %m."), path)));
        }
    }

    if (fstat(GetVfdCache()[fd].fd, &statBuffer) < 0) {
        CloseUndoFile(reln, forknum, InvalidBlockNumber);
        ereport(ERROR, (errmsg(UNDOFORMAT("could not stat file \"%s\": %m."), path)));
    }

    SetUndoFileState(state, segno, fd);
    seekpos = statBuffer.st_size;
    char undoBuffer[BLCKSZ] = {'\0'};

    /* Extend file to undoFileSize. */
    while (seekpos < (off_t)undoFileSize) {
        off_t diffSize = (off_t)undoFileSize - seekpos;
        if (diffSize < BLCKSZ) {
            nbytes = FilePWrite(fd, (char *)undoBuffer, (int)diffSize, seekpos, (uint32)WAIT_EVENT_UNDO_FILE_EXTEND);
        } else {
            nbytes = FilePWrite(fd, (char *)undoBuffer, BLCKSZ, seekpos, (uint32)WAIT_EVENT_UNDO_FILE_EXTEND);
        }
        if (nbytes < 0) {
            CloseUndoFile(reln, forknum, InvalidBlockNumber);
            if (unlink(path) != 0) {
                ereport(ERROR, (errmsg(UNDOFORMAT("could not delete undo file during initialization \"%s\": %m."), path)));
            }
            ereport(ERROR, (errmsg(UNDOFORMAT("could not initialize undo log segment file \"%s\": %m."), path)));
        }
        seekpos += (off_t)nbytes;
    }

    if (!skipFsync && !SmgrIsTemp(reln)) {
        RegisterDirtyUndoSegment(reln, state);
    }

    blockNum = GetUndoFileBlocks(reln, forknum, state);
    if (blockNum != undoFileBlocks) {
        ereport(PANIC, (errmsg(UNDOFORMAT("The undo file \"%s\" size is incorrect, blockNum=%u."), path, blockNum)));
    }
    return;
}

/* Open the undo file and return the UndoFileState. */
static UndoFileState *OpenUndoFile(SMgrRelation reln, ForkNumber forknum, BlockNumber blockno, ExtensionBehavior behavior)
{
    Assert(reln != NULL);

    UndoFileState *state = (UndoFileState *)reln->fileState;
    uint32 undoFileBlocks = UNDO_FILE_BLOCK(reln->smgr_rnode.node.dbNode);
    char path[UNDO_FILE_PATH_LEN];
    File fd;
    uint32 flags = O_RDWR | PG_BINARY;
    int segno = (int)(blockno / undoFileBlocks);
    BlockNumber blockNum;
    RelFileNodeForkNum filenode;

    WHITEBOX_TEST_STUB(UNDO_OPEN_FILE_FAILED, WhiteboxDefaultErrorEmit);

    if (blockno == InvalidBlockNumber) {
        ereport(ERROR, (errmsg(UNDOFORMAT("cannot open undo file beyond %u blocks."), InvalidBlockNumber)));
    }

    /* No work if already open */
    if (state != NULL) {
        if (state->file > 0) {
            if (state->segno == segno) {
                return state;
            }
            /* This is not the file we're looking for. */
            FileClose(state->file);
        }
    } else {
        state = AllocUndoFileState();
        reln->fileState = state;
    }
    SetUndoFileState(state, -1, -1);

    ADIO_RUN() {
        flags |= O_DIRECT;
    }
    ADIO_END();

    GetUndoFilePath(reln->smgr_rnode.node.relNode, reln->smgr_rnode.node.dbNode,
        segno, path, UNDO_FILE_PATH_LEN);
    filenode = RelFileNodeForkNumFill(reln->smgr_rnode, forknum, segno);
    fd = DataFileIdOpenFile(path, filenode, (int)flags, S_IRUSR | S_IWUSR);

    if (fd < 0) {
        if ((behavior == EXTENSION_RETURN_NULL) && FILE_POSSIBLY_DELETED(errno)) {
            return NULL;
        }
        if (IsBootstrapProcessingMode() || t_thrd.xlog_cxt.InRecovery) {
            DECLARE_NODE_COUNT();
            GET_UPERSISTENCE_BY_ZONEID((int)reln->smgr_rnode.node.relNode, nodeCount);
            CheckUndoFileDirectory(upersistence);
            ExtendUndoFile(reln, forknum, blockno, NULL, false);
            fd = DataFileIdOpenFile(path, filenode, (int)flags, S_IRUSR | S_IWUSR);
        }
        if (fd < 0) {
            CloseUndoFile(reln, forknum, InvalidBlockNumber);
            if (t_thrd.undo_cxt.fetchRecord == true) {
                t_thrd.undo_cxt.fetchRecord = false;
                if (t_thrd.storage_cxt.InProgressBuf != NULL) {
                    Buffer buffer = BufferDescriptorGetBuffer(t_thrd.storage_cxt.InProgressBuf);
                    TerminateBufferIO(t_thrd.storage_cxt.InProgressBuf, false, BM_VALID);
                    ReleaseBuffer(buffer);
                }
                ereport(ERROR, (errmsg(UNDOFORMAT("could not open undo file \"%s\": %m."), path)));
            } else {
                ereport(ERROR, (errmsg(UNDOFORMAT("could not open undo file \"%s\": %m."), path)));
            }
        }
    }

    SetUndoFileState(state, segno, fd);
    blockNum = GetUndoFileBlocks(reln, forknum, state);
    if (blockNum < undoFileBlocks) {
        ereport(WARNING, (errmsg(UNDOFORMAT("The undo file \"%s\" size is small than undoFileBlocks, "
            "file blockNum=%u, we will extend undo file."), path, blockNum)));
        ExtendUndoFile(reln, forknum, blockno, NULL, false);
    } else if (blockNum > undoFileBlocks) {
        ereport(WARNING, (errmsg(UNDOFORMAT("The undo file \"%s\" size is big than undoFileBlocks, "
            "file blockNum=%u, we will extend undo file."), path, blockNum)));
        /* close undofile before unlink undo file */
        CloseUndoFile(reln, forknum, InvalidBlockNumber);
        UnlinkUndoFile(reln->smgr_rnode, forknum, true, blockno);
        ExtendUndoFile(reln, forknum, blockno, NULL, false);
    }
    return state;
}

/* Read the specified block from a undo file. */
SMGR_READ_STATUS ReadUndoFile(SMgrRelation reln, ForkNumber forknum, BlockNumber blockno, char *buffer)
{
    Assert(buffer != NULL);

    UndoFileState *state = NULL;
    char *fileName = NULL;
    off_t seekpos;
    int nbytes;
    uint32 undoFileBlocks = UNDO_FILE_BLOCK(reln->smgr_rnode.node.dbNode);

    state = OpenUndoFile(reln, forknum, blockno, EXTENSION_FAIL);
    seekpos = (off_t)BLCKSZ * (blockno % undoFileBlocks);
    fileName = FilePathName(state->file);

    Assert(seekpos < (off_t)(undoFileBlocks * BLCKSZ));

    WHITEBOX_TEST_STUB(UNDO_READ_FILE_FAILED, WhiteboxDefaultErrorEmit);

    nbytes = FilePRead(state->file, buffer, BLCKSZ, seekpos, WAIT_EVENT_UNDO_FILE_READ);
    if (nbytes != BLCKSZ) {
        CloseUndoFile(reln, forknum, InvalidBlockNumber);
        if (nbytes < 0) {
            ereport(ERROR, (errmsg(UNDOFORMAT("could not read block %u in file \"%s\": %m."), blockno, fileName)));
        }
        ereport(ERROR, (errmsg(UNDOFORMAT("could not read block %u in file \"%s\": read only %d of %d bytes."),
            blockno, fileName, nbytes, BLCKSZ)));
    }

    if (PageIsVerified((Page)buffer, blockno)) {
        return SMGR_RD_OK;
    } else {
        return SMGR_RD_CRC_ERROR;
    }
}

void WriteUndoFile(SMgrRelation reln, ForkNumber forknum, BlockNumber blockNum, const char *buffer, bool skipFsync)
{
    Assert(buffer != NULL);

    UndoFileState *state = NULL;
    char *fileName = NULL;
    int nbytes;
    off_t seekpos;
    uint32 undoFileBlocks = UNDO_FILE_BLOCK(reln->smgr_rnode.node.dbNode);

    state = OpenUndoFile(reln, forknum, blockNum, EXTENSION_FAIL);
    seekpos = (off_t)BLCKSZ * (blockNum % undoFileBlocks);
    fileName = FilePathName(state->file);

    Assert(seekpos < (off_t)(undoFileBlocks * BLCKSZ));

    WHITEBOX_TEST_STUB(UNDO_WRITE_FILE_FAILED, WhiteboxDefaultErrorEmit);

    nbytes = FilePWrite(state->file, buffer, BLCKSZ, seekpos, (uint32)WAIT_EVENT_UNDO_FILE_WRITE);
    if (nbytes != BLCKSZ) {
        CloseUndoFile(reln, forknum, InvalidBlockNumber);
        if (nbytes < 0) {
            ereport(ERROR, (errmsg(UNDOFORMAT("could not write block %u in file \"%s\": %m."), blockNum, fileName)));
        }
        ereport(ERROR, (errmsg(UNDOFORMAT("could not write block %u in file \"%s\": wrote only %d of %d bytes."),
            blockNum, fileName, nbytes, BLCKSZ)));
    }

    /* Tell checkpointer this file is dirty. */
    if (!skipFsync && !SmgrIsTemp(reln)) {
        RegisterDirtyUndoSegment(reln, state);
    }
    return;
}

void WritebackUndoFile(SMgrRelation reln, ForkNumber forknum, BlockNumber blockno, BlockNumber nblocks)
{
    uint32 undoFileBlocks = UNDO_FILE_BLOCK(reln->smgr_rnode.node.dbNode);

    while (nblocks > 0) {
        UndoFileState *state = NULL;
        int segStart;
        int segEnd;
        BlockNumber nflush = nblocks;
        off_t seekpos;

        state = OpenUndoFile(reln, forknum, blockno, EXTENSION_RETURN_NULL);
        segStart = blockno / undoFileBlocks;
        segEnd = (blockno + nblocks - 1) / undoFileBlocks;
        /*
         * We might be flushing buffers of already removed relations, that's
         * ok, just ignore that case.
         */
        if (state == NULL) {
            return;
        }

        if (segStart != segEnd) {
            nflush = undoFileBlocks - (blockno % undoFileBlocks);
        }

        Assert(nflush >= 1);
        Assert(nflush <= nblocks);

        seekpos = (off_t)BLCKSZ * (blockno % undoFileBlocks);
        FileWriteback(state->file, seekpos, (off_t)BLCKSZ * nflush);

        nblocks -= nflush;
        blockno += nflush;
    }
}

void PrefetchUndoFile(SMgrRelation reln, ForkNumber forknum, BlockNumber blockNum)
{
#ifdef USE_PREFETCH
    UndoFileState *state = NULL;
    off_t seekpos;
    uint32 undoFileBlocks = UNDO_FILE_BLOCK(reln->smgr_rnode.node.dbNode);

    state = OpenUndoFile(reln, forknum, blockNum, EXTENSION_FAIL);
    seekpos = (off_t)BLCKSZ * (blockNum % undoFileBlocks);

    Assert(seekpos < (off_t)(undoFileBlocks * BLCKSZ));

    (void)FilePrefetch(state->file, seekpos, BLCKSZ, WAIT_EVENT_UNDO_FILE_PREFETCH);
#endif /* USE_PREFETCH */
}

void UnlinkUndoFile(const RelFileNodeBackend& rnode, ForkNumber forkNum, bool isRedo, BlockNumber blockNum)
{
    Assert(blockNum != InvalidBlockNumber);

    char path[UNDO_FILE_PATH_LEN];
    uint32 undoFileBlocks = UNDO_FILE_BLOCK(rnode.node.dbNode);
    int ret;
    int zid = rnode.node.relNode;
    int segno = blockNum / undoFileBlocks;
    GetUndoFilePath(zid, rnode.node.dbNode, segno, path, UNDO_FILE_PATH_LEN);

    if (isRedo || u_sess->attr.attr_common.IsInplaceUpgrade || forkNum != MAIN_FORKNUM ||
        RelFileNodeBackendIsTemp(rnode) || rnode.node.bucketNode != InvalidBktId) {
        if (!RelFileNodeBackendIsTemp(rnode)) {
            RegisterForgetUndoRequests(rnode, segno);
        }
        pgstat_report_waitevent(WAIT_EVENT_UNDO_FILE_UNLINK);
        if (unlink(path) < 0 && errno != ENOENT) {
            /* try again */
            if ((unlink(path) < 0) && (errno != ENOENT) && !isRedo) {
                ereport(WARNING, (errmsg(UNDOFORMAT("could not remove file \"%s\": %m."), path)));
            }
        }
        pgstat_report_waitevent(WAIT_EVENT_END);
    } else {
        /* truncate(2) would be easier here, but Windows hasn't got it */
        int fd;
        fd = BasicOpenFile(path, O_RDWR | PG_BINARY, 0);
        if (fd >= 0) {
            int save_errno;
            ret = ftruncate(fd, 0);
            save_errno = errno;
            (void)close(fd);
            errno = save_errno;
        } else {
            ret = -1;
        }
        if (ret < 0 && errno != ENOENT) {
            ereport(WARNING, (errcode_for_file_access(), errmsg("could not truncate file \"%s\": %m", path)));
        }

        /* Register request to unlink first segment later */
        RegisterUnlinkUndoRequests(rnode, segno);
    }
    return;
}

void CloseUndoFile(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum)
{
    Assert(reln != NULL);

    UndoFileState *state = (UndoFileState *)reln->fileState;

    /* No work if already closed */
    if (state == NULL) {
        return;
    }
    reln->fileState = NULL;    /* prevent dangling pointer after error */

    /* if not closed already */
    if (state->file >= 0) {
        FileClose(state->file);
    }
    pfree(state);
}

/* Sync undo file. */
int SyncUndoFile(const FileTag *tag, char *path)
{
    SMgrRelation reln = smgropen(tag->rnode, InvalidBackendId);
    uint32 undoFileBlocks = UNDO_FILE_BLOCK(tag->rnode.dbNode);

    GetUndoFilePath(tag->rnode.relNode, tag->rnode.dbNode, tag->segno,
        path, UNDO_FILE_PATH_LEN);
    UndoFileState *state = OpenUndoFile(reln, tag->forknum,
        tag->segno * undoFileBlocks, EXTENSION_RETURN_NULL);
    if (state == NULL) {
        return 0;
    }

    return FileSync(state->file, WAIT_EVENT_UNDO_FILE_SYNC);
}

int SyncUnlinkUndoFile(const FileTag *tag, char *path)
{
    GetUndoFilePath(tag->rnode.relNode, tag->rnode.dbNode, tag->segno,
        path, UNDO_FILE_PATH_LEN);

    /* Try to unlink the file. */
    return unlink(path);
}

static void RegisterUnlinkUndoRequests(RelFileNodeBackend rnode, uint32 segno)
{
    FileTag tag;
    INIT_UNDO_FILE_TAG(tag, rnode.node, segno);
    (void) RegisterSyncRequest(&tag, SYNC_UNLINK_REQUEST, true /* retryOnError */ );
}

static void RegisterForgetUndoRequests(RelFileNodeBackend rnode, uint32 segno)
{
    FileTag tag;
    INIT_UNDO_FILE_TAG(tag, rnode.node, segno);
    (void) RegisterSyncRequest(&tag, SYNC_FORGET_REQUEST, true /* retryOnError */ );
}

static void RegisterDirtyUndoSegment(SMgrRelation reln, const UndoFileState *state)
{
    /* Temp relations should never be fsync'd */
    Assert(!SmgrIsTemp(reln));

    FileTag tag;
    INIT_UNDO_FILE_TAG(tag, reln->smgr_rnode.node, state->segno);

    if (!RegisterSyncRequest(&tag, SYNC_REQUEST, false /* retryOnError */ )) {
        ereport(DEBUG5, (errmsg(UNDOFORMAT("could not forward fsync request because request queue is full."))));

        if (FileSync(state->file, WAIT_EVENT_DATA_FILE_SYNC) < 0) {
            ereport(ERROR, (errmsg(UNDOFORMAT("could not fsync file \"%s\": %m."), FilePathName(state->file))));
        }
    }
}

bool CheckUndoFileExists(SMgrRelation reln, ForkNumber forkNum, BlockNumber blockNum)
{
    /*
     * Close it first, to ensure that we notice if the fork has been unlinked
     * since we opened it.
     */
    CloseUndoFile(reln, forkNum, blockNum);

    bool isExist = false;
    if (OpenUndoFile(reln, forkNum, blockNum, EXTENSION_RETURN_NULL) != NULL) {
        isExist = true;
    }
    CloseUndoFile(reln, forkNum, blockNum);
    return isExist;
}
