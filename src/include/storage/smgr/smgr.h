/* -------------------------------------------------------------------------
 *
 * smgr.h
 *	  storage manager switch public interface declarations.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/smgr.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef SMGR_H
#define SMGR_H

#include "fmgr.h"
#include "lib/ilist.h"
#include "storage/smgr/knl_usync.h" 
#include "storage/smgr/relfilenode.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "vecexecutor/vectorbatch.h"
#include "nodes/bitmapset.h"
#include "storage/file/fio_device_com.h"
#include "storage/dss/dss_api_def.h"

typedef int File;

/*
 * smgr.c maintains a table of SMgrRelation objects, which are essentially
 * cached file handles.  An SMgrRelation is created (if not already present)
 * by smgropen(), and destroyed by smgrclose().  Note that neither of these
 * operations imply I/O, they just create or destroy a hashtable entry.
 * (But smgrclose() may release associated resources, such as OS-level file
 * descriptors.)
 *
 * An SMgrRelation may have an "owner", which is just a pointer to it from
 * somewhere else; smgr.c will clear this pointer if the SMgrRelation is
 * closed.	We use this to avoid dangling pointers from relcache to smgr
 * without having to make the smgr explicitly aware of relcache.  There
 * can't be more than one "owner" pointer per SMgrRelation, but that's
 * all we need.
 *
 * SMgrRelations that do not have an "owner" are considered to be transient,
 * and are deleted at end of transaction.
 */
typedef struct SMgrRelationData {
    /* rnode is the hashtable lookup key, so it must be first! */
    RelFileNodeBackend smgr_rnode; /* relation physical identifier */

    /* pointer to owning pointer, or NULL if none */
    struct SMgrRelationData** smgr_owner;
    uint64 xact_seqno;

    /*
     * These next three fields are not actually used or manipulated by smgr,
     * except that they are reset to InvalidBlockNumber upon a cache flush
     * event (in particular, upon truncation of the relation).	Higher levels
     * store cached state here so that it will be reset when truncation
     * happens.  In all three cases, InvalidBlockNumber means "unknown".
     */
    BlockNumber smgr_targblock;   /* current insertion target block */
    BlockNumber smgr_prevtargblock;   /* previous insertion target block */
    BlockNumber smgr_fsm_nblocks; /* last known size of fsm fork */
    BlockNumber smgr_vm_nblocks;  /* last known size of vm fork */
    BlockNumber smgr_cached_nblocks; /* last known size of main fork */

    int smgr_bcmarry_size;
    BlockNumber* smgr_bcm_nblocks; /* last known size of bcm fork */

    /* additional public fields may someday exist here */

    /*
     * Fields below here are intended to be private to smgr.c and its
     * submodules. Do not touch them from elsewhere.
     */
    int smgr_which; /* storage manager selector */

    /* for md.c; NULL for forks that are not open */
    int md_fdarray_size;
    struct _MdfdVec** md_fd;

    struct SegmentDesc** seg_desc;
    struct SegSpace* seg_space;
    
    /* This variable is used for expansion. */
    void *fileState;

    /* if unowned, list link in list of all unowned SMgrRelations */
    dlist_node node;

    /* bucket smgr nodes of the same relation are linked together */
    dlist_head bucket_smgr_head;
    dlist_node bucket_smgr_node;

    bool encrypt; /* enable table's level data encryption */
} SMgrRelationData;

typedef enum {             /* behavior for open file */
    EXTENSION_FAIL,        /* ereport if segment not present */
    EXTENSION_RETURN_NULL, /* return NULL if not present */
    EXTENSION_CREATE       /* create new segments as needed */
} ExtensionBehavior;

typedef SMgrRelationData* SMgrRelation;

typedef struct _MdfdVec {
    File mdfd_vfd;               /* fd number in fd.c's pool */
    BlockNumber mdfd_segno;      /* segment number, from 0 */
    struct _MdfdVec *mdfd_chain; /* next segment, or NULL */
} MdfdVec;

#define IsSegmentSmgrRelation(smgr) (IsSegmentFileNode((smgr)->smgr_rnode.node))

#define SmgrIsTemp(smgr) RelFileNodeBackendIsTemp((smgr)->smgr_rnode)

enum SMGR_READ_STATUS {
    SMGR_RD_OK = 0,
    SMGR_RD_NO_BLOCK = 1,
    SMGR_RD_CRC_ERROR = 2
};

#define INVALID_DB_OID (0)
#define UNDO_DB_OID (9)
#define UNDO_SLOT_DB_OID (10)

#define EXRTO_BASE_PAGE_SPACE_OID (6)
#define EXRTO_LSN_INFO_SPACE_OID (7)
#define EXRTO_BLOCK_INFO_SPACE_OID (8)
#define EXRTO_FORK_NUM MAX_FORKNUM + 1

#define MD_MANAGER (0)
#define UNDO_MANAGER (1)
#define SEGMENT_MANAGER (2)
#define EXRTO_MANAGER (3)

#define IS_UNDO_RELFILENODE(rnode) ((rnode).dbNode == UNDO_DB_OID || (rnode).dbNode == UNDO_SLOT_DB_OID)
#define IS_EXRTO_RELFILENODE(rnode) ((rnode).spcNode == EXRTO_BASE_PAGE_SPACE_OID || \
                                     (rnode).spcNode == EXRTO_LSN_INFO_SPACE_OID || \
                                     (rnode).spcNode == EXRTO_BLOCK_INFO_SPACE_OID)
/*
 * On Windows, we have to interpret EACCES as possibly meaning the same as
 * ENOENT, because if a file is unlinked-but-not-yet-gone on that platform,
 * that's what you get.  Ugh.  This code is designed so that we don't
 * actually believe these cases are okay without further evidence (namely,
 * a pending fsync request getting canceled ... see mdsync).
 */
#ifndef WIN32
#define FILE_POSSIBLY_DELETED(err) ((err) == ENOENT || (err) == ERR_DSS_FILE_NOT_EXIST)
#else
#define FILE_POSSIBLY_DELETED(err) ((err) == ENOENT || (err) == EACCES)
#endif

#define FILE_ALREADY_EXIST(err) ((err) == EEXIST || (err) == ERR_DSS_DIR_CREATE_DUPLICATED)

extern void smgrinit(void);
extern SMgrRelation smgropen(const RelFileNode& rnode, BackendId backend, int col = 0);
extern void smgrshutdown(int code, Datum arg);
extern bool smgrexists(SMgrRelation reln, ForkNumber forknum, BlockNumber blockNum = InvalidBlockNumber);
extern void smgrsetowner(SMgrRelation* owner, SMgrRelation reln);
extern void smgrclearowner(SMgrRelation* owner, SMgrRelation reln);
extern void smgrclose(SMgrRelation reln, BlockNumber blockNum = InvalidBlockNumber);
extern void smgrcloseall(void);
extern void smgrclosenode(const RelFileNodeBackend& rnode);
extern void smgrcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern void smgrdounlink(SMgrRelation reln, bool isRedo, BlockNumber blockNum = InvalidBlockNumber);
extern void smgrdounlinkfork(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern void smgrextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum,
                       char* buffer, bool skipFsync);
extern void smgrprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum);
extern SMGR_READ_STATUS smgrread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char* buffer);
extern void smgrbulkread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, int blockCount,char *buffer);
extern void smgrwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const char* buffer, bool skipFsync);
extern void smgrwriteback(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber smgrnblocks(SMgrRelation reln, ForkNumber forknum);
extern BlockNumber smgrnblocks_cached(SMgrRelation reln, ForkNumber forknum);
extern BlockNumber smgrtotalblocks(SMgrRelation reln, ForkNumber forknum);
extern void smgrtruncatefunc(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks);
extern void smgrtruncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks);
extern void smgrimmedsync(SMgrRelation reln, ForkNumber forknum);
extern void smgrmovebuckets(SMgrRelation reln1, SMgrRelation reln2, List *bList);
extern void SmgrRecoveryPca(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, bool isPcaCheckNeed, bool skipFsync);
extern void SmgrAssistFileProcess(const char *assistInfo, int assistFd);
extern void SmgrChunkFragmentsRestore(const RelFileNode& rnode, ForkNumber forknum, char parttype, bool nowait);
extern void SmgrChunkFragmentsRestoreRecord(const RelFileNode &rnode, ForkNumber forknum);
extern void CfsShrinkerShmemListPush(const RelFileNode &rnode, ForkNumber forknum, char parttype);

extern void AtEOXact_SMgr(void);

/* internals: move me elsewhere -- ay 7/94 */

/* in md.cpp */
extern void mdinit(void);
extern void mdclose(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum);
extern void mdcreate(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern bool mdexists(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum);
extern void mdunlink(const RelFileNodeBackend& rnode, ForkNumber forknum, bool isRedo, BlockNumber blocknum);
extern void mdextend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char* buffer, bool skipFsync);
extern void mdprefetch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum);
extern SMGR_READ_STATUS mdread(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char* buffer);
extern void mdreadbatch(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, int blockCount,char *buffer);
extern void mdwrite(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const char* buffer, bool skipFsync);
extern void mdwriteback(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, BlockNumber nblocks);
extern BlockNumber mdnblocks(SMgrRelation reln, ForkNumber forknum);
extern void mdtruncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks);
extern void mdimmedsync(SMgrRelation reln, ForkNumber forknum);
extern char* mdsegpath(const RelFileNode& rnode, ForkNumber forknum, BlockNumber blkno);
extern void md_register_forget_request(RelFileNode rnode, ForkNumber forknum, BlockNumber segno);
extern MdfdVec *_mdfd_getseg(SMgrRelation reln, ForkNumber forknum, BlockNumber blkno, bool skipFsync, ExtensionBehavior behavior);

/* md sync callbacks */
extern void mdForgetDatabaseFsyncRequests(Oid dbid);

/* chunk compression api */
extern void MdRecoveryPcaPage(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, bool skipFsync);
extern void MdAssistFileProcess(SMgrRelation relation, const char *assistInfo, int assistFd);
extern void CfsRecycleChunk(SMgrRelation reln, ForkNumber forknum);
extern void CfsShrinkRecord(const RelFileNode &node, ForkNumber forknum);
extern void CfsShrinkImpl(void);

/* md sync requests */
extern void ForgetDatabaseSyncRequests(Oid dbid);
extern void CheckPointSyncWithAbsorption(void);
/* md sync callbacks */
extern int SyncMdFile(const FileTag *ftag, char *path);
extern int UnlinkMdFile(const FileTag *ftag, char *path);
extern bool MatchMdFileTag(const FileTag *ftag, const FileTag *candidate);

/* in knl_uundofile.cpp */
extern void InitUndoFile(void);
extern void CloseUndoFile(SMgrRelation reln, ForkNumber forknum, BlockNumber blockNum);
extern void CreateUndoFile(SMgrRelation reln, ForkNumber forknum, bool isRedo);
extern bool CheckUndoFileExists(SMgrRelation reln, ForkNumber forknum, BlockNumber blockNum);
extern void UnlinkUndoFile(const RelFileNodeBackend& rnode, ForkNumber forknum, bool isRedo, BlockNumber blockNum);
extern void ExtendUndoFile(SMgrRelation reln, ForkNumber forknum, BlockNumber blockNum, char *buffer,
    bool isRedo);
extern void PrefetchUndoFile(SMgrRelation reln, ForkNumber forknum, BlockNumber blockNum);
extern SMGR_READ_STATUS ReadUndoFile(SMgrRelation reln, ForkNumber forknum, BlockNumber blockNum, char *buffer);
extern void WriteUndoFile(SMgrRelation reln, ForkNumber forknum, BlockNumber blockNum, const char *buffer,
    bool skipFsync);
extern void WritebackUndoFile(SMgrRelation reln, ForkNumber forknum, BlockNumber blockNum, BlockNumber nblocks);
extern BlockNumber GetUndoFileNblocks(SMgrRelation reln, ForkNumber forknum);
extern void CheckUndoFileDirectory(UndoPersistence upersistence);
extern void CleanUndoFileDirectory(UndoPersistence upersistence);
extern void CheckUndoDirectory(void);

/* undo sync callbacks */
extern int SyncUndoFile(const FileTag *tag, char *path);
extern int SyncUnlinkUndoFile(const FileTag *tag, char *path);

/* smgrtype.c */
extern Datum smgrout(PG_FUNCTION_ARGS);
extern Datum smgrin(PG_FUNCTION_ARGS);
extern Datum smgreq(PG_FUNCTION_ARGS);
extern Datum smgrne(PG_FUNCTION_ARGS);

extern void partition_create_new_storage(Relation rel, Partition part, const RelFileNodeBackend& filenode,
    bool keep_old_relfilenode = false);
extern ScalarToDatum GetTransferFuncByTypeOid(Oid attTypeOid);
extern bool check_unlink_rel_hashtbl(RelFileNode rnode, ForkNumber forknum);

/* storage_exrto_file.cpp */
void exrto_init(void);
void exrto_close(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum);
bool exrto_exists(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum);
void exrto_unlink(const RelFileNodeBackend& rnode, ForkNumber forknum, bool is_redo, BlockNumber blocknum);
void exrto_extend(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer, bool skip_fsync);
SMGR_READ_STATUS exrto_read(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, char *buffer);
void exrto_write(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, const char *buffer, bool skip_fsync);
BlockNumber exrto_nblocks(SMgrRelation reln, ForkNumber forknum);
void exrto_truncate(SMgrRelation reln, ForkNumber forknum, BlockNumber nblocks);
void exrto_writeback(SMgrRelation reln, ForkNumber forknum, BlockNumber blocknum, BlockNumber nblocks);

#endif /* SMGR_H */
