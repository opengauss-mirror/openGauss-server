/* -------------------------------------------------------------------------
 *
 * utility.h
 *	  prototypes for utility.c.
 *
 *
 * Portions Copyright (c) 2021, openGauss Contributors
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/tcop/utility.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef UTILITY_H
#define UTILITY_H

#include "pgxc/pgxcnode.h"
#include "tcop/tcopprot.h"

#define CHOOSE_EXEC_NODES(is_temp) ((is_temp) ? EXEC_ON_DATANODES : EXEC_ON_ALL_NODES)

#define TRANSFER_DISABLE_DDL(namespaceOid) \
    do { \
        if (!IsInitdb && \
            IS_PGXC_COORDINATOR && !IsConnFromCoord() && \
            IsSchemaInDistribution(namespaceOid) && \
            (u_sess->attr.attr_common.application_name == NULL || \
                strncmp(u_sess->attr.attr_common.application_name, "gs_redis", strlen("gs_redis")) != 0)) { \
                ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), \
                    errmsg("Disallow DDL while schema transfering."))); \
        } \
    } while(0) \

/* Hook for plugins to get control in ProcessUtility() */
typedef void (*ProcessUtility_hook_type)(Node* parsetree, const char* queryString, ParamListInfo params,
    bool isTopLevel, DestReceiver* dest,
#ifdef PGXC
    bool sentToRemote,
#endif /* PGXC */
    char* completionTag,
    bool isCTAS);
extern THR_LOCAL PGDLLIMPORT ProcessUtility_hook_type ProcessUtility_hook;

extern void ProcessUtility(Node* parsetree, const char* queryString, ParamListInfo params, bool isTopLevel,
    DestReceiver* dest,
#ifdef PGXC
    bool sentToRemote,
#endif /* PGXC */
    char* completionTag,
    bool isCTAS = false);
extern void standard_ProcessUtility(Node* parsetree, const char* queryString, ParamListInfo params, bool isTopLevel,
    DestReceiver* dest,
#ifdef PGXC
    bool sentToRemote,
#endif /* PGXC */
    char* completionTag,
    bool isCTAS = false);

extern char* find_first_exec_cn();
extern bool find_hashbucket_options(List* stmts);

#ifdef PGXC
extern void CreateCommand(CreateStmt *parsetree, const char *queryString, ParamListInfo params, bool isTopLevel,
    bool sentToRemote, bool isCTAS = false);
#else
extern void CreateCommand(CreateStmt *parsetree, const char *queryString, ParamListInfo params, bool isTopLevel,
    bool isCTAS = false);
#endif

extern void ReindexCommand(ReindexStmt* stmt, bool isTopLevel);

extern bool UtilityReturnsTuples(Node* parsetree);

extern TupleDesc UtilityTupleDescriptor(Node* parsetree);

extern Query* UtilityContainsQuery(Node* parsetree);

extern const char* CreateCommandTag(Node* parsetree);

extern LogStmtLevel GetCommandLogLevel(Node* parsetree);

extern bool CommandIsReadOnly(Node* parsetree);

#ifdef PGXC

typedef enum {
    ARQ_TYPE_TOTALROWCNTS,
    ARQ_TYPE_SAMPLE, /* include sample rows or sample table. */
} ANALYZE_RQTYPE;

extern bool pg_try_advisory_lock_for_redis(Relation rel);
extern void pgxc_lock_for_utility_stmt(Node* parsetree, bool is_temp);
extern void ExecUtilityStmtOnNodes(const char* queryString, ExecNodes* nodes, bool sentToRemote, bool force_autocommit,
    RemoteQueryExecType exec_type, bool is_temp, Node* parsetree = NULL);
extern HeapTuple* ExecRemoteVacuumStmt(VacuumStmt* stmt, const char* queryString, bool sentToRemote,
    ANALYZE_RQTYPE arq_type, AnalyzeMode eAnalyzeMode = ANALYZENORMAL, Oid relid = InvalidOid);

extern void ExecUtilityStmtOnNodes_ParallelDDLMode(const char* queryString, ExecNodes* nodes, bool sentToRemote,
    bool force_autocommit, RemoteQueryExecType exec_type, bool is_temp, const char* FirstExecNode = NULL,
    Node* parsetree = NULL);

extern HeapTuple* ExecUtilityStmtOnNodesWithResults(VacuumStmt* stmt, const char* queryString, bool sentToRemote,
    ANALYZE_RQTYPE arq_type, AnalyzeMode eAnalyzeMode = ANALYZENORMAL);
extern void CheckObjectInBlackList(ObjectType objtype, const char* queryString);
extern bool CheckExtensionInWhiteList(const char* extensionName, uint32 hashvalue, bool hashCheck);
/* @hdfs Judge if a foreign table support analyze operation. */
bool IsHDFSTableAnalyze(Oid foreignTableId);
/* @pgfdw judge if is a gc_fdw foreign table support analysis operation. */
bool IsPGFDWTableAnalyze(Oid foreignTableId);
#ifdef ENABLE_MOT
/* @MOT IsMOTForeignTbale */
bool IsMOTForeignTable(Oid foreignTableId);
#endif
/* @hdfs IsForeignTableAnalyze function is called by standard_ProcessUtility */
bool IsHDFSForeignTableAnalyzable(VacuumStmt* stmt);
void global_stats_set_samplerate(AnalyzeMode eAnalyzeMode, VacuumStmt* stmt, const double* NewSampleRate);
bool check_analyze_permission(Oid rel);

extern ExecNodes* RelidGetExecNodes(Oid relid, bool isutility = true);
extern bool ObjectsInSameNodeGroup(List* objects, NodeTag stmttype);
extern void EstIdxMemInfo(
    Relation rel, RangeVar* relation, UtilityDesc* desc, IndexInfo* info, const char* accessMethod);
extern void AdjustIdxMemInfo(AdaptMem* operatorMem, UtilityDesc* desc);
extern void AlterGlobalConfig(AlterGlobalConfigStmt *stmt);
extern void DropGlobalConfig(DropGlobalConfigStmt *stmt);

/*
 * @hdfs The struct HDFSTableAnalyze is used to store datanode work infomation
 */
typedef struct HDFSTableAnalyze {
    NodeTag type;
    List* DnWorkFlow;
    int DnCnt;
    bool isHdfsStore; /* the flag identify whether or not the relation for analyze is Dfs store */
    double sampleRate[ANALYZE_MODE_MAX_NUM - 1]; /* There are three sampleRate values for HDFS table, include
                                                    main/delta/complex. */
    unsigned int orgCnNodeNo; /* the nodeId identify which CN receive analyze command from client, other CN need to get
                                 stats from it. */
    bool isHdfsForeignTbl;    /* the current analyze table is hdfs foreign table or not. */
    bool sampleTableRequired; /* require sample table for get statistic. */
    List* tmpSampleTblNameList; /* identify sample table name if under debugging. */
    DistributionType disttype;  /* Distribution type for analyze's table. */
    AdaptMem memUsage;          /* meminfo that should pass down to dn */
} HDFSTableAnalyze;

typedef struct PGFDWTableAnalyze {
    Oid relid;
    PGXCNodeAllHandles* pgxc_handles;

    int natts_pg_statistic;
    int natts_pg_statistic_ext;

    int natts;
    char** attname;
    int* attnum;

    bool has_analyze;
} PGFDWTableAnalyze;

/*
 * @hdfs The struct ForeignTableDesc is used to represent foreign table
 */
typedef struct ForeignTableDesc {
    NodeTag type;
    char* tableName;
    char* schemaName;
    Oid tableOid;
} ForeignTableDesc;

/* The struct for verify information*/
typedef struct VerifyDesc {
    bool isCudesc;        /* judge cudesc relation and its toast\index relation */
    bool isCudescDamaged; /*judge the relation data corrupt*/
} VerifyDesc;

enum FOREIGNTABLEFILETYPE_ {
    HDFS_ORC = 1,
    MOT_ORC = 2,
    MYSQL_ORC = 3,
    ORACLE_ORC = 4,
    POSTGRES_ORC = 5
};

typedef enum FOREIGNTABLEFILETYPE_ FOREIGNTABLEFILETYPE;

#endif

extern void BlockUnsupportedDDL(const Node* parsetree);
extern const char* CreateAlterTableCommandTag(const AlterTableType subtype);

extern void SetSortMemInfo(UtilityDesc* desc, int rowCnt, int width, bool vectorized, bool indexSort, Size defaultSize,
    Relation copyRel = NULL);
extern char* ConstructMesageWithMemInfo(const char* queryString, AdaptMem memInfo);

extern void AssembleHybridMessage(char** queryStringWithInfo, const char* queryString, const char* schedulingMessage);

extern void ClearCreateSeqStmtUUID(CreateSeqStmt* stmt);
extern void ClearCreateStmtUUIDS(CreateStmt* stmt);
extern bool IsSchemaInDistribution(const Oid namespaceOid);
extern Oid GetNamespaceIdbyRelId(const Oid relid);

#endif /* UTILITY_H */
