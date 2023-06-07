/* -------------------------------------------------------------------------
 *
 * tablespace.h
 *		Tablespace management commands (create/drop tablespace).
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/tablespace.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef TABLESPACE_H
#define TABLESPACE_H

#include "access/xloginsert.h"
#include "catalog/objectaddress.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "workload/workload.h"
#include "catalog/indexing.h"

/* XLOG stuff */
#define XLOG_TBLSPC_CREATE 0x00
#define XLOG_TBLSPC_DROP 0x10
#define XLOG_TBLSPC_RELATIVE_CREATE 0x20

#define TABLESPACE_USAGE_SLOT_NUM 65536U
#define TABLESPACE_BUCKET_CONFLICT_LISTLEN 2
#define CRITICA_POINT_VALUE 104857600 /* 100 MB */
#define TABLESPACE_THRESHOLD_RATE 0.9 /* threshold rate */
#define TABLESPACE_UNLIMITED_STRING "unlimited"
#define PG_LOCATION_DIR (g_instance.datadir_cxt.locationDir)

typedef struct TableSpaceUsageSlot {
    uint64 maxSize;
    uint64 currentSize;
    uint64 thresholdSize;
    Oid tableSpaceOid;
} TableSpaceUsageSlot;

typedef struct TableSpaceUsageBucket {
    int count;
    slock_t mutex;
    TableSpaceUsageSlot spcUsage[TABLESPACE_BUCKET_CONFLICT_LISTLEN];
} TableSpaceUsageBucket;

typedef struct TableSpaceUsageStruct {
    TableSpaceUsageBucket m_tab[TABLESPACE_USAGE_SLOT_NUM];
} TableSpaceUsageStruct;

class TableSpaceUsageManager {
public:
    static int ShmemSize(void);
    static void Init(void);
    static void IsExceedMaxsize(Oid tableSpaceOid, uint64 requestSize, bool segment);
    static bool IsLimited(Oid tableSpaceOid, uint64* maxSize);
    
private:
    static inline int GetBucketIndex(Oid tableSpaceOid);
    static inline void ResetUsageSlot(TableSpaceUsageSlot* info);
    static inline void ResetBucket(TableSpaceUsageBucket* bucket);
    static inline bool WithinLimit(TableSpaceUsageSlot* slot, uint64 currentSize, uint64 requestSize);
    static inline bool IsFull(uint64 maxSize, uint64 currentSize, uint64 requestSize);
    static inline uint64 GetThresholdSize(uint64 maxSize, uint64 currentSize);
};

typedef struct xl_tblspc_create_rec {
    Oid ts_id;
    char ts_path[FLEXIBLE_ARRAY_MEMBER]; /* VARIABLE LENGTH STRING */
} xl_tblspc_create_rec;

typedef struct xl_tblspc_drop_rec {
    Oid ts_id;
} xl_tblspc_drop_rec;

typedef struct TableSpaceOpts {
    int32 vl_len_; /* varlena header (do not touch directly!) */
    float8 random_page_cost;
    float8 seq_page_cost;
    char* filesystem;
    char* address;
    char* cfgpath;
    char* storepath;
} TableSpaceOpts;

/* This definition is used when storage space is increasing, it includes two main functionalities:
 * 1. Check tablespace is exceed the specified size. Skip segment-page storage because it does not
 *    actually extend physical space.
 * 2. Increase the permanent space on users' record
 */
#define STORAGE_SPACE_OPERATION(relation, requestSize)                                                         \
    {                                                                                                          \
        if (RelationIsSegmentTable(relation)) {                                                            \
            TableSpaceUsageManager::IsExceedMaxsize(relation->rd_node.spcNode, 0, true);                       \
        } else if (relation->rd_id != ClassOidIndexId) {                                                                                               \
            TableSpaceUsageManager::IsExceedMaxsize(relation->rd_node.spcNode, requestSize, false);            \
        }                                                                                                      \
        perm_space_increase(                                                                                   \
            relation->rd_rel->relowner, requestSize, RelationUsesSpaceType(relation->rd_rel->relpersistence)); \
    }

extern Oid CreateTableSpace(CreateTableSpaceStmt* stmt);
extern void DropTableSpace(DropTableSpaceStmt* stmt);
extern ObjectAddress RenameTableSpace(const char* oldname, const char* newname);
extern ObjectAddress AlterTableSpaceOwner(const char* name, Oid newOwnerId);
extern Oid AlterTableSpaceOptions(AlterTableSpaceOptionsStmt* stmt);
extern bool IsSpecifiedTblspc(Oid spcOid, const char* specifedTblspc);

extern void TablespaceCreateDbspace(Oid spcNode, Oid dbNode, bool isRedo);

extern Oid GetDefaultTablespace(char relpersistence);
extern DataSpaceType RelationUsesSpaceType(char relpersistence);

/*
 * Get the Specified optioin value.
 */
extern char* GetTablespaceOptionValue(Oid spcNode, const char* optionName);
/*
 * Get the all optioin values.
 */
extern List* GetTablespaceOptionValues(Oid spcNode);

extern void PrepareTempTablespaces(void);

extern Oid get_tablespace_oid(const char* tablespacename, bool missing_ok);
extern char* get_tablespace_name(Oid spc_oid);

extern bool directory_is_empty(const char* path);
extern void remove_tablespace_symlink(const char *linkloc);
extern void check_create_dir(char* location);

extern void tblspc_redo(XLogReaderState* rptr);
extern void tblspc_desc(StringInfo buf, XLogReaderState* record);
extern const char* tblspc_type_name(uint8 subtype);
extern uint64 pg_cal_tablespace_size_oid(Oid tblspcOid);
extern Oid ConvertToPgclassRelTablespaceOid(Oid tblspc);
extern Oid ConvertToRelfilenodeTblspcOid(Oid tblspc);
extern void xlog_create_tblspc(Oid tsId, char* tsPath, bool isRelativePath);
extern void xlog_drop_tblspc(Oid tsId);

#endif /* TABLESPACE_H */
