/* -------------------------------------------------------------------------
 *
 * explain.h
 *	  prototypes for explain.c
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994-5, Regents of the University of California
 *
 * src/include/commands/explain.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef EXPLAIN_H
#define EXPLAIN_H

#include "executor/executor.h"
#include "utils/portal.h"

extern uint32 RunGetSlotFromExplain(Portal portal, TupleTableSlot* slot, DestReceiver* dest, int count);

typedef enum ExplainFormat {
    EXPLAIN_FORMAT_TEXT,
    EXPLAIN_FORMAT_XML,
    EXPLAIN_FORMAT_JSON,
    EXPLAIN_FORMAT_YAML
} ExplainFormat;

struct ExplainState;

#define ASCE 1
#define DESC 5

typedef enum PlanInfoType {
    PLANID = 0,
    PLAN,
    ACTUAL_TIME,
    PREDICT_TIME,
    ACTUAL_ROWS,
    PREDICT_ROWS,
    ESTIMATE_ROWS,
    ESTIMATE_DISTINCT,
    ACTUAL_MEMORY,
    PREDICT_MEMORY,
    ESTIMATE_MEMORY,
    ACTUAL_WIDTH,
    ESTIMATE_WIDTH,
    ESTIMATE_COSTS,

    SLOT_NUMBER  // this is the length of the slot number,  this is the last number
} PlanInfoType;

typedef enum Plantype {
    PLANINFO = 0,
    DETAILINFO,
    STATICINFO,
    VERBOSEINFO,
    DATANODEINFO,
    IOINFO,
    RUNTIMEINFO,
    PROFILEINFO,
    RECURSIVEINFO,
    QUERYSUMMARY
} Plantype;

typedef enum ExplainType {
    QUERY_ID = 0,
    PLAN_PARENT_ID,
    PLAN_NODE_ID,
    PLAN_TYPE,
    FROM_DATANODE,
    NODE_NAME,
    PLAN_NAME,
    START_TIME,
    TOTAL_TIME,
    OPER_TIME,
    PLAN_ROWS,
    PLAN_LOOPS,
    EX_CYC,
    INC_CYC,
    EX_CYC_PER_ROWS,
    PEAK_OP_MEMORY,
    PEAK_NODE_MEMORY,

    SHARED_BLK_HIT,
    SHARED_BLK_READ,
    SHARED_BLK_DIRTIED,
    SHARED_BLK_WRITTEN,
    LOCAL_BLK_HIT,
    LOCAL_BLK_READ,
    LOCAL_BLK_DIRTIED,
    LOCAL_BLK_WRITTEN,
    TEMP_BLK_READ,
    TEMP_BLK_WRITTEN,
    BLK_READ_TIME,
    BLK_WRITE_TIME,

    CU_NONE,
    CU_SOME,

    SORT_METHOD,
    SORT_TYPE,
    SORT_SPACE,

    HASH_BATCH,
    HASH_BATCH_ORIGNAL,
    HASH_BUCKET,
    HASH_SPACE,

    HASH_FILENUM,

    QUERY_NETWORK,
    NETWORK_POLL_TIME,

    LLVM_OPTIMIZATION,
    BLOOM_FILTER_INFO,
    DFS_BLOCK_INFO,

    MERGE_INSERTED,
    MERGE_UPDATED,

    EXPLAIN_TOTAL_ATTNUM  // this is the attnum of the table
} ExplainType;

typedef struct MultiInfo {
    Datum* m_Datum;
    bool* m_Nulls;
} MultiInfo;

typedef struct CompareInfo {
    SortSupport ssup;
    int attnum;
    int* att;
} CompareInfo;

/* plan table entry of each column in the table */
typedef enum PlanColOpt {
    ANAL_OPT = 0, COST_OPT, VERBOSE_OPT, PRED_OPT_TIME, PRED_OPT_ROW, PRED_OPT_MEM
} PlanColOpt;
typedef enum ExecutorTime { DN_START_TIME = 0, DN_RUN_TIME, DN_END_TIME} ExecutorTime;
typedef struct PlanTableEntry {
    const char* name;     /* column name */
    int val;              /* column enum index in PlanInfoType */
    Oid typid;            /* column type */
    PlanColOpt disoption; /* explain display option */
} PlanTableEntry;

/* --------------------------for explain plan && plan_table--------------------------- */
/* for table "plan_table_data". */
#define T_PLAN_TABLE_DATA "plan_table_data"
/* for view "plan_table". */
#define V_PLAN_TABLE "plan_table"

/* plan_table column length for explain plan. */
#define PLANTABLECOLNUM 13
#define SESSIONIDLEN 32
#define STMTIDLEN 31 /* the max statement_id length is 30 byte. */
#define OPERATIONLEN 31
#define OPTIONSLEN 256
#define OBJECTLEN 31
#define PROJECTIONLEN 4001

/* plan_table_data column defination. */
typedef struct PlanTableData {
    char session_id[SESSIONIDLEN];  /* start time + thread id */
    Oid user_id;                    /* user id of this record, uint32 */
    char statement_id[STMTIDLEN];   /* statement_id that user input. */
    uint64 query_id;                /* A db use plan_id instead of query_id as column name*/
    int node_id;                    /* plan node id. */
    char operation[OPERATIONLEN];   /* plan node operation. */
    char options[OPTIONSLEN];       /* plan node options. */
    char object_name[NAMEDATALEN];  /* object_name of this node operate. */
    char object_type[OBJECTLEN];    /* object type. */
    char object_owner[NAMEDATALEN]; /* object schema */
    StringInfo projection;          /* output targetlist of the node */
    double cost;                    /* cost of operation */
    double cardinality;                    /* cost of operation */
} PlanTableData;

typedef enum PlanTableCol {
    PT_SESSION_ID = 0,
    PT_USER_ID,
    PT_STATEMENT_ID,
    PT_QUERY_ID,

    PT_NODE_ID,

    PT_OPERATION,
    PT_OPTIONS,

    PT_OBJECT_NAME,
    PT_OBJECT_TYPE,
    PT_OBJECT_OWNER,

    PT_PROJECTION,
    PT_COST,
    PT_CARDINALITY
} PlanTableCol;

/* Store all node tupls of one plan. */
typedef struct PlanTableMultiData {
    PlanTableData* m_datum;
    bool* m_isnull;
} PlanTableMultiData;

extern THR_LOCAL bool OnlySelectFromPlanTable;
extern THR_LOCAL bool OnlyDeleteFromPlanTable;
extern THR_LOCAL bool PTFastQueryShippingStore;
extern THR_LOCAL bool IsExplainPlanStmt;

#define AFTER_EXPLAIN_APPLY_SET_HINT() \
    /* Set PTFastQueryShippingStore in case of changed enable_fast_query_shipping */                \
    bool savedPTFastQueryShippingStore = PTFastQueryShippingStore;                                  \
    PTFastQueryShippingStore = u_sess->attr.attr_sql.enable_fast_query_shipping;                    

#define AFTER_EXPLAIN_RECOVER_SET_HINT() \
    PTFastQueryShippingStore = savedPTFastQueryShippingStore;
/* --------------------------end--------------------------- */

typedef int (*SortCompareFunc)(const MultiInfo* arg1, const MultiInfo* arg2, CompareInfo* ssup);

class PlanTable : public BaseObject {
    friend class PlanInformation;

public:
    Datum get(int type);
    void put(int infotype, Datum datum);
    void put(int nodeId, int smpId, int type, Datum value);
    void flush(DestReceiver* dest, int plantype);
    int print_plan(Portal portal, DestReceiver* dest);
    void init(int plantype);
    void set(int planNodeId)
    {
        m_plan_node_id = planNodeId;
        m_has_write_planname = false;
    }

    template <bool set_name, bool set_space>
    void set_plan_name();
    void flush_data_to_file();
    void set_datanode_name(char* nodename, int smp_idx, int dop);

    /* -----------Functions for explain plan stmt. ---------------- */
    void make_session_id(char* sessid);

    /* Set session_id, user_id, statement_id, query_id and plan node id for the query. */
    void set_plan_table_ids(uint64 query_id, ExplainState* es);

    /* Set object_name, object_type, object_owner for one paln node. */
    void set_plan_table_objs(
        int plan_node_id, const char* object_name, const char* object_type, const char* object_owner);

    /* Set object_type according to relkind in pg_class. */
    void set_object_type(RangeTblEntry* rte, char** object_type);

    /* Set operation and options for remote query and stream node.*/
    void set_plan_table_streaming_ops(char* pname, char** operation, char** options);

    /* Set join option 'CARTESIAN' to fit with A db. */
    void set_plan_table_join_options(Plan* plan, char** options);

    /* Set operation and options for node except stream node. */
    void set_plan_table_ops(int plan_node_id, char* operation, char* options);

    /* Set projection for one paln node. */
    void set_plan_table_projection(int plan_node_id, List* tlist);

    /* Set plan cost and cardinality. */
    void set_plan_table_cost_card(int plan_node_id, double plan_cost, double plan_cardinality);

    /* Call heap_insert to insert all nodes tuples of the plan into table. */
    void insert_plan_table_tuple();

public:
    StringInfo info_str;
    int64 m_size;
    int m_plan_node_id;
    TupleDesc m_desc;
    bool m_costs;
    bool m_verbose;
    bool m_cpu;
    bool m_analyze;
    bool m_timing;
    StringInfoData m_pname;
    bool m_has_write_planname;
    double m_total_time;
    int m_plan_size;
    int m_data_size;
    int* m_node_num;
    int m_consumer_data_size; /* the number of datanode that has consumer info */
    bool m_query_mem_mode;
    PlanTableMultiData** m_plan_table;
    int m_plan_node_num;
    bool m_pred_time;
    bool m_pred_row;
    bool m_pred_mem;

private:
    Datum** m_data;
    bool** m_isnull;
    MultiInfo*** m_multi_info;
    int2 m_col_loc[SLOT_NUMBER];

    void init_planinfo(int plansize);
    void init_multi_info(ExplainState* es, int plansize, int num_nodes);
    /* init the data of MultiInfo. */
    void init_multi_info_data(MultiInfo* multi_info, int dop);

    /* For explain plan: init the data of plan_table. */
    void init_plan_table_data(int num_plan_nodes);

    TupleDesc getTupleDesc();
    TupleDesc getTupleDesc(const char* attname);
    TupleDesc getTupleDesc_detail();

    void flush_plan(TupOutputState* tstate);
    void flush_other(TupOutputState* tstate);
    void set_pname(char* data);
};

class PlanInformation : public BaseObject {
public:
    void init(ExplainState* es, int size, int num_nodes, bool quey_mem_mode);

    void dump(DestReceiver* dest)
    {
        if (m_runtimeinfo && t_thrd.explain_cxt.explain_perf_mode == EXPLAIN_RUN)
            dump_runtimeinfo_file();
        if (m_planInfo)
            m_planInfo->flush(dest, PLANINFO);
        if (m_detailInfo)
            m_detailInfo->flush(dest, DETAILINFO);
        if (m_staticInfo)
            m_staticInfo->flush(dest, STATICINFO);
        if (m_verboseInfo)
            m_verboseInfo->flush(dest, VERBOSEINFO);
        if (m_datanodeInfo && m_runtimeinfo)
            m_datanodeInfo->flush(dest, DATANODEINFO);
        if (m_IOInfo)
            m_IOInfo->flush(dest, IOINFO);
        if (m_recursiveInfo)
            m_recursiveInfo->flush(dest, RECURSIVEINFO);
        if (m_profileInfo)
            m_profileInfo->flush(dest, PROFILEINFO);
        if (m_query_summary)
            m_query_summary->flush(dest, QUERYSUMMARY);
    }

    int print_plan(Portal portal, DestReceiver* dest);

    void set_id(int plan_id)
    {
        if (m_planInfo)
            m_planInfo->set(plan_id);
        if (m_detailInfo)
            m_detailInfo->set(plan_id);
        if (m_staticInfo)
            m_staticInfo->set(plan_id);
        if (m_verboseInfo)
            m_verboseInfo->set(plan_id);
        if (m_datanodeInfo)
            m_datanodeInfo->set(plan_id);
        if (m_IOInfo)
            m_IOInfo->set(plan_id);
        if (m_recursiveInfo)
            m_recursiveInfo->set(plan_id);
        if (m_query_summary)
            m_query_summary->set(plan_id);
    }

    void set_pname(char* data)
    {
        if (m_planInfo)
            m_planInfo->set_pname(data);
        if (m_detailInfo)
            m_detailInfo->set_pname(data);
        if (m_staticInfo)
            m_staticInfo->set_pname(data);
        if (m_verboseInfo)
            m_verboseInfo->set_pname(data);
        if (m_datanodeInfo)
            m_datanodeInfo->set_pname(data);
        if (m_IOInfo)
            m_IOInfo->set_pname(data);
        if (m_recursiveInfo)
            m_recursiveInfo->set_pname(data);
        if (m_query_summary)
            m_query_summary->set_pname(data);
    }

    void append_str_info(const char* data, int id, const char* value);

    PlanTable* m_planInfo;
    PlanTable* m_detailInfo;
    PlanTable* m_staticInfo;
    PlanTable* m_verboseInfo;
    PlanTable* m_datanodeInfo;
    PlanTable* m_IOInfo;
    PlanTable* m_profileInfo;
    PlanTable* m_query_summary;
    PlanTable* m_recursiveInfo;

    /* For explain plan */
    PlanTable* m_planTableData;

    int m_count;
    PlanTable* m_runtimeinfo;
    bool m_detail;
    int m_query_id;

private:
    void dump_runtimeinfo_file();
    void flush_runtime_info(DestReceiver* dest);
    void flush_summary_info(DestReceiver* dest);
    void flush_memory_info(DestReceiver* dest);

    void write_datanode();
    void append_time_info(int node_idx, int plan_idx, bool from_datanode);
    void append_cpu_info(int node_idx, int plan_idx, bool from_datanode);
    void append_buffer_info(int node_idx, int plan_idx, bool from_datanode);
    void append_roughcheck_info(int node_idx, int plan_idx, bool from_datanode);
    void append_llvm_info(int node_idx, int plan_idx, bool from_datanode);
    void append_bloomfilter_info(int node_idx, int plan_idx, bool from_datanode);
    void append_dfs_block_info(int node_idx, int plan_idx, bool from_datanode);

    void write_memory();
    void append_peak_memory_info(int node_idx, int plan_idx, bool from_datanode);
    void append_memory_info(int node_idx, int plan_idx, bool from_datanode);
    void append_network_info(int node_idx, int plan_idx);
    void append_sort_info(int node_idx, int plan_idx, bool from_datanode);
    void append_filenum_info(int node_idx, int plan_idx, bool from_datanode);
    void append_hash_info(int node_idx, int plan_idx, bool from_datanode);
    void append_vechash_info(int node_idx, int plan_idx, bool from_datanode);

    void free_memory();
};

typedef struct DN_RunInfo {
    bool all_datanodes;
    int len_nodelist;
    int* node_index;
} DN_RunInfo;

typedef struct ExplainState {
    StringInfo str; /* output buffer */
    StringInfo post_str; /* output buffer after plan tree */
                    /* options */
    bool plan;      /* do not print plan */
    bool verbose;   /* be verbose */
    bool analyze;   /* print actual times */
    bool costs;     /* print costs */
    bool buffers;   /* print buffer usage */
#ifdef PGXC
    bool nodes;     /* print nodes in RemoteQuery node */
    bool num_nodes; /* print number of nodes in RemoteQuery node */
#endif              /* PGXC */
    bool timing;    /* print timing */
    bool cpu;
    bool detail;
    bool performance;
    bool from_dn;
    bool sql_execute;
    bool isexplain_execute; /* is explain execute statement */
    ExplainFormat format;   /* output format */
    /* other states */
    PlannedStmt* pstmt; /* top of plan */
    List* rtable;       /* range table */
    int indent;         /* current indentation level */
    int pindent;
    List* grouping_stack; /* format-specific grouping state */
    PlanInformation* planinfo;
    DN_RunInfo datanodeinfo;
    int* wlm_statistics_plan_max_digit; /* print plan for wlm statistics */
    char* statement_id;                 /* statement_id for EXPLAIN PLAN */
    bool is_explain_gplan;
    char* opt_model_name;
} ExplainState;

/* Hook for plugins to get control in explain_get_index_name() */
typedef const char *(*explain_get_index_name_hook_type) (Oid indexId);
extern THR_LOCAL PGDLLIMPORT explain_get_index_name_hook_type explain_get_index_name_hook;

extern void ExplainQuery(
    ExplainStmt* stmt, const char* queryString, ParamListInfo params, DestReceiver* dest, char* completionTag);

extern void ExplainInitState(ExplainState* es);

extern TupleDesc ExplainResultDesc(ExplainStmt* stmt);

extern void ExplainOneUtility(
    Node* utilityStmt, IntoClause* into, ExplainState* es, const char* queryString, ParamListInfo params);

extern void ExplainOnePlan(
    PlannedStmt* plannedstmt, IntoClause* into, ExplainState* es, const char* queryString,
    DestReceiver *dest, ParamListInfo params);

extern void ExplainPrintPlan(ExplainState* es, QueryDesc* queryDesc);

extern void ExplainQueryText(ExplainState* es, QueryDesc* queryDesc);

extern void ExplainOneQueryForStatistics(QueryDesc* queryDesc);

extern void ExplainBeginOutput(ExplainState* es);
extern void ExplainEndOutput(ExplainState* es);
extern void ExplainSeparatePlans(ExplainState* es);

extern void ExplainPropertyList(const char* qlabel, List* data, ExplainState* es);
extern void ExplainPropertyListPostPlanTree(const char* qlabel, List* data, ExplainState* es);
extern void ExplainPropertyText(const char* qlabel, const char* value, ExplainState* es);
extern void ExplainPropertyInteger(const char* qlabel, int value, ExplainState* es);
extern void ExplainPropertyLong(const char* qlabel, long value, ExplainState* es);
extern void ExplainPropertyFloat(const char* qlabel, double value, int ndigits, ExplainState* es);

extern int get_track_time(ExplainState* es, PlanState* planstate, bool show_track, bool show_buffer,
    bool show_dummygroup, bool show_indexinfo, bool show_storage_info = false);

extern void ExplainPropertyListNested(const char* qlabel, List* data, ExplainState* es);
extern List* set_deparse_context_planstate(List* dpcontext, Node* planstate, List* ancestors);
extern double elapsed_time(instr_time* starttime);
extern void print_explain_info(StreamInstrumentation* instrumentdata, QueryDesc* querydesc, ExplainState* es);
extern bool checkSelectStmtForPlanTable(List* rangeTable);
extern int checkPermsForPlanTable(RangeTblEntry* rte);

#endif /* EXPLAIN_H */
