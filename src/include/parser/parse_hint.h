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
 * parse_hint.h
 *        Table parse hint header file, include struct member declaration.
 * 
 * 
 * IDENTIFICATION
 *        src/include/parser/parse_hint.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef PARSE_HINT_h
#define PARSE_HINT_h

#include "optimizer/streamplan.h"
#include "parser/parse_node.h"
#include "tcop/dest.h"
#include "utils/guc.h"

/* hint keywords */
#define HINT_NESTLOOP "NestLoop"
#define HINT_MERGEJOIN "MergeJoin"
#define HINT_HASHJOIN "HashJoin"
#define HINT_NO "No"
#define HINT_LEADING "Leading"
#define HINT_ROWS "Rows"
#define HINT_BROADCAST "Broadcast"
#define HINT_REDISTRIBUTE "Redistribute"
#define HINT_BLOCKNAME "BlockName"
#define HINT_TABLESCAN "TableScan"
#define HINT_INDEXSCAN "IndexScan"
#define HINT_INDEXONLYSCAN "IndexOnlyScan"
#define HINT_SKEW "Skew"
#define HINT_MULTI_NODE "MultiNode"
#define HINT_NULL "Null"
#define HINT_TRUE "True"
#define HINT_FALSE "False"
#define HINT_PRED_PUSH "Predpush"
#define HINT_PRED_PUSH_SAME_LEVEL "Predpush_Same_Level"
#define HINT_REWRITE "Rewrite_rule"
#define HINT_GATHER "Gather"
#define HINT_NO_EXPAND "No_expand"
#define HINT_SET "Set"
#define HINT_CPLAN "Use_cplan"
#define HINT_GPLAN "Use_gplan"
#define HINT_NO_GPC "No_gpc"

#define BLOCK_COMMENT_START "/*"
#define BLOCK_COMMENT_END "*/"
#define HINT_COMMENT_KEYWORD "+"
#define HINT_START BLOCK_COMMENT_START HINT_COMMENT_KEYWORD
#define HINT_END BLOCK_COMMENT_END

typedef struct pull_hint_warning_context {
    List* warning;
} pull_qual_vars_context;

#define append_warning_to_list(root, hint, format, ...)                                     \
    do {                                                                                    \
        StringInfoData buf;                                                                 \
        char* hint_string = descHint(hint);                                                 \
        initStringInfo(&buf);                                                               \
        appendStringInfo(&buf, format, ##__VA_ARGS__);                                      \
        root->glob->hint_warning = lappend(root->glob->hint_warning, makeString(buf.data)); \
        pfree(hint_string);                                                                 \
    } while (0)

extern THR_LOCAL List* hint_list;
extern THR_LOCAL List* hint_warning;

/* hint keyword of enum type. DO NOT change the order! */
typedef enum HintKeyword {
    HINT_KEYWORD_NESTLOOP = 0,
    HINT_KEYWORD_MERGEJOIN,
    HINT_KEYWORD_HASHJOIN,
    HINT_KEYWORD_LEADING,
    HINT_KEYWORD_ROWS,
    HINT_KEYWORD_BROADCAST,
    HINT_KEYWORD_REDISTRIBUTE,
    HINT_KEYWORD_BLOCKNAME,
    HINT_KEYWORD_TABLESCAN,
    HINT_KEYWORD_INDEXSCAN,
    HINT_KEYWORD_INDEXONLYSCAN,
    HINT_KEYWORD_SKEW,
    HINT_KEYWORD_PREDPUSH,
    HINT_KEYWORD_PREDPUSH_SAME_LEVEL,
    HINT_KEYWORD_REWRITE,
    HINT_KEYWORD_GATHER,
    HINT_KEYWORD_NO_EXPAND,
    HINT_KEYWORD_SET,
    HINT_KEYWORD_CPLAN,
    HINT_KEYWORD_GPLAN,
    HINT_KEYWORD_NO_GPC,
} HintKeyword;

/* hint status */
typedef enum HintStatus {
    HINT_STATE_NOTUSED = 0, /* specified relation not used in query */
    HINT_STATE_USED,        /* hint is used */
    HINT_STATE_DUPLICATION, /* specified hint duplication */
    HINT_STATE_ERROR        /* hint contains error */
} HintStatus;

/* gather source */
typedef enum GatherSource {
    HINT_GATHER_GUC = 0,    /* exist as guc */
    HINT_GATHER_REL,        /* Gather baserel */
    HINT_GATHER_JOIN,       /* Gather joinrel */
    HINT_GATHER_ALL,        /* Gather baserel and joinrel, and find the best */
    HINT_GATHER_UNKNOWN     /* unknown, should report error */
} GatherSource;

/* common data for all hints. */
struct Hint {
    NodeTag type;             /* Hint type */
    List* relnames;           /* relation name list */
    HintKeyword hint_keyword; /* Hint Keyword */
    HintStatus state;         /* hint used state */
};

typedef struct LeadingHint {
    Hint base;            /* Base hint */
    bool join_order_hint; /* If join order is hinted */
} LeadingHint;

/* join method hints */
typedef struct JoinMethodHint {
    Hint base;               /* Base hint */
    bool negative;           /* Positive or negative? */
    Relids joinrelids;       /* Join relids */
    Relids inner_joinrelids; /* Inner relids */
} JoinMethodHint;

/* rows hints kind */
typedef enum RowsValueType {
    RVT_ABSOLUTE, /* Rows(... #1000) absolute rows */
    RVT_ADD,      /* Rows(... +1000) add rows */
    RVT_SUB,      /* Rows(... -1000) subtract rows */
    RVT_MULTI     /* Rows(... *1.2) multiply rows */
} RowsValueType;

/* rows hint */
typedef struct RowsHint {
    Hint base;                /* base hint */
    Relids joinrelids;        /* Join relids */
    char* rows_str;           /* Row string */
    RowsValueType value_type; /* hints kind */
    double rows;              /* Rows */
} RowsHint;

typedef struct StreamHint {
    Hint base;              /* base hint */
    bool negative;          /* Positive or negative? */
    Relids joinrelids;      /* Join relids */
    StreamType stream_type; /* Stream type, redistribute or broadcast */
} StreamHint;

typedef struct BlockNameHint {
    Hint base; /* base hint */
               /* Block name will be stored in base.relname. */
} BlockNameHint;

/* scan method hints */
typedef struct ScanMethodHint {
    Hint base;
    bool negative;
    Relids relid;
    List* indexlist;
} ScanMethodHint;

/* skew hints */
typedef struct SkewHint {
    Hint base;         /* base hint */
    Relids relid;      /* skew relation relids */
    List* column_list; /* skew column list */
    List* value_list;  /* skew value list */
} SkewHint;

/* multinode hints */
typedef struct MultiNodeHint {
    Hint base;         /* base hint */
    bool multi_node_hint;
} MultiNodeHint;

/* relation information from RangeTblEntry and pg_class */
typedef struct SkewRelInfo {
    NodeTag type;
    char* relation_name;       /* relation name */
    Oid relation_oid;          /* relation`s oid */
    RangeTblEntry* rte;        /* relation`s rte */
    RangeTblEntry* parent_rte; /* if relation comes from subquery, then it`s subquery`s rte */
} SkewRelInfo;

/* column information from PG_ATTRIBUTE or targetEntry */
typedef struct SkewColumnInfo {
    NodeTag type;
    Oid relation_Oid;  /* table`s oid */
    char* column_name; /* column`s name */
    AttrNumber attnum; /* column's location in relation */
    Oid column_typid;  /* column`s type oid */
    Expr* expr;        /* expr that column comes from */
} SkewColumnInfo;

/* make skew value to Const */
typedef struct SkewValueInfo {
    NodeTag type;
    bool support_redis; /* column type whether support redistribution, if not the const_value will be null */
    Const* const_value; /* skew value */
} SkewValueInfo;

/* SkewHint structure after transform */
typedef struct SkewHintTransf {
    NodeTag type;
    SkewHint* before;       /* skew hint struct before transform */
    List* rel_info_list;    /* relation info list after transform */
    List* column_info_list; /* column info list after transform */
    List* value_info_list;  /* value info list after transform */
} SkewHintTransf;

/* prompts which predicates can be pushdown */
typedef struct PredpushHint {
    Hint base; /* base hint */
    bool negative;
    char *dest_name;
    int dest_id;
    Relids candidates;      /* which one will be push down */
} PredpushHint;

typedef struct PredpushSameLevelHint {
    Hint base; /* base hint */
    bool negative;
    char *dest_name;
    int dest_id;
    Relids candidates;      /* which one will be push down */
} PredpushSameLevelHint;

/* Enable/disable rewrites with hint */
typedef struct RewriteHint {
    Hint base; /* base hint */
    List* param_names;   /* rewrite parameters */
    unsigned int param_bits;
} RewriteHint;

/* Enable/disable CN gather with hint */
typedef struct GatherHint {
    Hint base; /* base hint */
    GatherSource source;   /* rewrite parameters */
} GatherHint;

/* Enforce custom/generic plan with hint */
typedef struct NoExpandHint {
    Hint base; /* base hint */
} NoExpandHint;

/* Hint session level optimizer guc parameter */
typedef struct SetHint {
    Hint base; /* base hint */
    char* name;
    char* value;
} SetHint;

typedef struct PlanCacheHint {
    Hint base; /* base hint */
    bool chooseCustomPlan;
} PlanCacheHint;

/* Avoid saving global plan with hint */
typedef struct NoGPCHint {
    Hint base; /* base hint */
} NoGPCHint;

typedef struct hintKeyword {
    const char* name;
    int value;
} hintKeyword;

extern HintState* HintStateCreate();
extern HintState* create_hintstate(const char* hints);
extern List* find_specific_join_hint(
    HintState* hstate, Relids joinrelids, Relids innerrelids, HintKeyword keyWord, bool leading = true);
extern List* find_specific_scan_hint(HintState* hstate, Relids relids, HintKeyword keyWord);
extern ScanMethodHint* find_scan_hint(HintState* hstate, Relids relid, HintKeyword keyWord);
extern char* descHint(Hint* hint);
extern void hintDelete(Hint* hint);
extern void desc_hint_in_state(PlannerInfo* root, HintState* hstate);

extern void transform_hints(PlannerInfo* root, Query* parse, HintState* hstate);

extern void check_scan_hint_validity(PlannerInfo* root);
extern void adjust_scanhint_relid(HintState* hstate, Index oldIdx, Index newIdx);
extern bool pull_hint_warning_walker(Node* node, pull_qual_vars_context* context);
extern List* retrieve_query_hint_warning(Node* parse);
extern void output_utility_hint_warning(Node* query, int lev);
extern void output_hint_warning(List* warning, int lev);
extern void HintStateDelete(HintState* hintState);
extern bool permit_predpush(PlannerInfo *root);
extern bool permit_from_rewrite_hint(PlannerInfo *root, unsigned int params);
extern Relids predpush_candidates_same_level(PlannerInfo *root);
extern bool is_predpush_same_level_matched(PredpushSameLevelHint* hint, Relids relids, ParamPathInfo* ppi);
extern bool permit_gather(PlannerInfo *root, GatherSource src = HINT_GATHER_GUC);
extern GatherSource get_gather_hint_source(PlannerInfo *root);
extern bool check_set_hint_in_white_list(const char* name);
extern bool has_no_expand_hint(Query* subquery);
extern bool has_no_gpc_hint(HintState* hintState);

extern void RemoveQueryHintByType(Query* query, HintKeyword hint);
extern bool CheckNodeNameHint(HintState* hintstate);

#define skip_space(str)   \
    while (isspace(*str)) \
        str++;
#endif
