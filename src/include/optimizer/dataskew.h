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
 * dataskew.h
 *        functions for dataskew solution in GaussDB
 * 
 * 
 * IDENTIFICATION
 *        src/include/optimizer/dataskew.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DATASKEW_H
#define DATASKEW_H
#include "nodes/execnodes.h"
#include "nodes/relation.h"
#include "optimizer/streamplan.h"
#include "parser/parse_hint.h"

#define SKEW_RES_NONE 0x00 /* No skew optimization path if create. */
#define SKEW_RES_STAT 0x01 /* Create skew optimization by statistic. */
#define SKEW_RES_RULE 0x02 /* Create skew optimization by rule. */
#define SKEW_RES_HINT 0x04 /* Create skew optimization by hint. */

typedef enum {
    SKEW_NONE, /* Initial value. */
    SKEW_AGG,  /* Skew info for agg. */
    SKEW_JOIN  /* Skew info for join. */
} SkewType;

typedef enum {
    PART_NONE = 0,                     /* Noe hybrid stream. */
    PART_REDISTRIBUTE_PART_BROADCAST,  /* Do broadcast for some special data, and redistribute the rest. */
    PART_REDISTRIBUTE_PART_ROUNDROBIN, /* Do roundrobin for some special data, and redistribute the rest. */
    PART_REDISTRIBUTE_PART_LOCAL,      /* Do local send for some special data, and redistribute the rest. */
    PART_LOCAL_PART_BROADCAST          /* Do broadcast for some special data, and send the rest to local DN. */
} SkewStreamType;

typedef struct ColSkewInfo {
    Node* var;           /* Skew var of distribute keys. */
    Const* value;        /* Skew value of this column. */
    double mcv_ratio;    /* The proportion of data equal to the skew value. */
    double mcv_op_ratio; /* The proportion of data equal to skew value at the none skew side. */
    bool is_null;        /* If this is null skew value. */
} ColSkewInfo;

typedef struct MultiColSkewInfo {
    List* vars;          /* Var list for distribute keys. */
    List* values;        /* Skew value lists. */
    double mcv_ratio;    /* The proportion of data equal to the skew value. */
    double mcv_op_ratio; /* The proportion of data equal to skew value at the none skew side. */
    bool is_null;        /* If this is null skew value. */
} MultiColSkewInfo;

typedef struct QualSkewInfo {
    NodeTag type;                    /* Node tag of qual skew info. */
    SkewStreamType skew_stream_type; /* Stream type for this skew qual. */
    List* skew_quals;                /* Qual list for skew value. */
    QualCost qual_cost;              /* Qual Cost for skew value. */
    double broadcast_ratio;          /* Record ratio for broadcast data. */
} QualSkewInfo;

typedef struct QualSkewState {
    SkewStreamType skew_stream_type; /* Stream type for this skew qual. */
    List* skew_quals_state;          /* Qual State for skew value. */
} QualSkewState;

class SkewInfo : public BaseObject {
public:
    SkewInfo(PlannerInfo* root);

    ~SkewInfo();

    /* Get skew info. */
    uint32 getSkewInfo() const;

protected:
    /* Find skew value for single column distribute key. */
    List* findSingleColSkewValues();

    /* Find skew value for single column distribute key from statistic info. */
    List* findSingleColSkewValuesFromStatistic();

    /* Find skew value for single column distribute key from hint info. */
    List* findSingleColSkewValuesFromHint();

    /* Find skew value for multi columns distribute key. */
    List* findMultiColSkewValues();

    /* Find skew value for multi columns distribute key from statistic info. */
    List* findMultiColSkewValuesFromStatistic();

    /* Find skew value for multi columns distribute key from single column statistic info. */
    List* findMultiColSkewValuesFromSingleStatistic();

    /* Find skew value for multi columns distribute key from multi column statistic info. */
    List* findMultiColSkewValuesFromMultiStatistic();

    /* Find skew value for multi columns distribute key from hint. */
    List* findMultiColSkewValuesFromHint();

    /* Try to get single column statistic info. */
    List* getSingleColumnStatistics(Node* node);

    /* Try to get multi column statistic info. */
    List* getMultiColumnStatistics();

    /* Find skew value from MCV. */
    List* findMcvValue(Node* var, int mcv_num, Datum* mcv_values, float4* mcv_ratios) const;

    /* Make const for skew value. */
    Const* makeConstForSkew(Var* var, Datum value) const;

    /* Create equal expression to identify skew values. */
    OpExpr* createEqualExprForSkew(Node* expr, Const* con) const;

    /* Get join clause from join path. */
    List* getJoinClause(Path* jpath) const;

    /* Find the column at may add null value in outer join. */
    List* findNullCols(List* target_list, List* subtarget_list, List* join_clauses) const;

    /* Check if the skew value can pass the restriction. */
    List* checkRestrict(List* baseSkewList);

    /* Check if the skew value of single column can pass the restriction. */
    List* checkRestrictForSingleCol(List* baseSkewList);

    /* Check if the skew value of multi column can pass the restriction. */
    List* checkRestrictForMultiCol(List* baseSkewList);

    /* Check if the current relation equal relation in skew hint. */
    bool checkSkewRelEqualHintRel(List* skewRelHint);

    /* Check if the distribute key is compitable to hint key. */
    bool checkEqualKey(Node* node, SkewColumnInfo* colHint) const;

    /* Check if the current column equal column in skew hint. */
    bool checkSkewColEqualHintCol(Node* node, SkewColumnInfo* colHint);

    /* Get skew value for hint. */
    bool getSkewKeys(List* skewHints, int* colLoc, int colLen);

    /* Get skew value for statistic. */
    bool getSkewKeys();

    /* Get real skew value after type tranfer. */
    Const* getRealSkewValue(Node* diskey, Node* skewkey, Const* value);

    /* Process skew value for type tranfer. */
    void processSkewValue(List* skewlist);

    /* Check if the current distribute keys are simple var. */
    bool checkDistributekeys(List* distributekeys);

    /* Check if the restriction include this distribute keys. */
    bool isColInRestrict(List* distributekeys, Node* clause) const;

    /* Execute the equal expression actually. */
    bool ExecSkewvalExpr(Expr* expr, TupleTableSlot* slot);

    /* Check if the skew value can pass restriction. */
    bool canValuePassQual(List* varList, List* valueList, Expr* expr);

    /* Check if skew values can equal for single col. */
    bool isSingleColSkewValueEqual(ColSkewInfo* cs_inner, ColSkewInfo* cs_outer);

    /* Check if skew values can equal for multi col. */
    bool isMultiColSkewValueEqual(MultiColSkewInfo* mcs_inner, MultiColSkewInfo* mcs_outer);

    /* Updata the skewness for multi column distirbute keys. */
    void updateMultiColSkewness(StreamInfo* sinfo, List* skew_list) const;

    /* Generate a bitmap for rel in skew hint. */
    Relids generateRelidsFromRelList(List* skewRelHint);

    /* Compare two rels are the same one. */
    bool compareRelByRTE(RangeTblEntry* rte1, RangeTblEntry* rte2) const;

    /* Check if we need broadcast the non skew side. */
    bool needPartBroadcast(ColSkewInfo* csinfo) const;

    /* Check if we need broadcast the non skew side. */
    bool needPartBroadcast(MultiColSkewInfo* mcsinfo) const;

    /* Print detail info of skew oprimization info. */
    void printSkewOptimizeDetail(const char* msg);

protected:
    /* Planner info of current query level. */
    PlannerInfo* m_root;

    /* Reloptinfo of current path. */
    RelOptInfo* m_rel;

    /* Reloptinfo of subpath. */
    RelOptInfo* m_subrel;

    /* Distribute keys for this stream. */
    List* m_distributeKeys;

    /* Base skew keys. */
    List* m_skewKeys;

    /* Consumer parallel degree. */
    int m_dop;

    /* Skew type */
    SkewType m_skewType;

    /* Flag to identify if the distribute key include multi-columns. */
    bool m_isMultiCol;

    /* Original memory context. */
    MemoryContext m_oldContext;

    /* Memory context for skew optimization process. */
    MemoryContext m_context;

    /* Find skew info from statistic. */
    bool m_hasStatSkew;

    /* Find skew info from hint. */
    bool m_hasHintSkew;

    /* Find skew info from rule. */
    bool m_hasRuleSkew;
};

class JoinSkewInfo : public SkewInfo {
public:
    JoinSkewInfo(PlannerInfo* root, RelOptInfo* rel, List* join_clause, JoinType join_type, JoinType save_join_type);

    ~JoinSkewInfo();

    /* Find skew info for join. */
    uint32 findStreamSkewInfo();

    /* Set stream info to find skew info for join. */
    void setStreamInfo(StreamInfo* inner_stream_info, StreamInfo* outer_stream_info, Distribution* distribution);

    /* Reset skew info for next use. */
    void resetSkewInfo();

private:
    /* Find skew info from base relation. */
    void findBaseSkewInfo();

    /* Find skew info caused by outer join. */
    void findNullSkewInfo();

    /* Add qual cost. */
    void addQualSkewInfo();

    /* Add the skew info to stream info. */
    void addToStreamInfo();

    /* Find skew values for base rel. */
    void findBaseSkewValues(bool stream_outer);

    /* Find null skew from sub outer join. */
    void findSubNullSkew(bool is_outer);

    /* Add skew info to both stream side when we detect skew values. */
    void addSkewInfoBothSides();

    /* Create quals for skew values. */
    List* createSkewQuals(bool is_stream_outer);

    /* Create quals of single distribute key. */
    List* createSingleColSkewQuals(bool is_stream_outer);

    /* Create quals of multi distribute keys. */
    List* createMultiColSkewQuals(bool is_stream_outer);

    /* Add skew info to the opposite side of skew side. */
    List* addSkewInfoToOtherSide(bool is_outer_skew, List* other_keys);

    /* Add skew info for single column to the opposite side of skew side. */
    List* addSingleColSkewInfoToOtherSide(bool is_outer_skew, List* other_keys);

    /* Add skew info for multi column to the opposite side of skew side. */
    List* addMultiColSkewInfoToOtherSide(bool is_outer_skew, List* other_keys);

    /* Choose suitable skew stream type for none skew side of join. */
    SkewStreamType chooseStreamForNoSkewSide(StreamInfo* sinfo) const;

    /* Delete duplicate skew values at both sides of join. */
    void deleteDuplicateSkewValue();

    /* Delete duplicate skew values for single column at both sides of join. */
    void deleteDuplicateSingleColSkewValue();

    /* Delete duplicate skew values for multi column at both sides of join. */
    void deleteDuplicateMultiColSkewValue();

    /* Find out which side has more skew data or which is set by hint. */
    bool findMoreSkewSideForSingleCol(ColSkewInfo* cs1, ColSkewInfo* cs2) const;

    /* Find out which side has more skew data or which is set by hint. */
    bool findMoreSkewSideForMultiCol(MultiColSkewInfo* mcs1, MultiColSkewInfo* mcs2) const;

    /* Delete if we can not handle this skew situation. */
    void deleteUnoptimzeSkew(bool is_outer);

    /* Traverse the sub path to find null skew. */
    void traverseSubPath(Path* path);

    /* Check if current distribute keys are included in outer join's null column. */
    bool checkOuterJoinNulls(Path* jpath);

    /* Get all target list of join's input. */
    List* getSubTargetList(JoinPath* jpath) const;

    /* Add skew qual cost. */
    void addQualCost(bool is_outer);

    /* Check if this join may have skew problem. */
    bool checkSkewPossibility(bool is_outer);

    /* Check if we can optimize this skew problem. */
    bool checkSkewOptimization(bool is_outer);

    /* Check if we have create redundant skew optimization stream. */
    bool checkRedundant(bool is_outer);

    /* Travers sub path to find redundant skew stream. */
    bool checkPathRedundant(List* streamKeys, Path* path);

    /* Find key for the none skew side. */
    List* findOtherSidekeys(bool is_outer_skew);

    /* Find equal var list. */
    List* findEqualVarList(List* skewList, RelOptInfo* rel);

    /* Find equal vae. */
    Node* findEqualVar(Node* var, RelOptInfo* rel);

private:
    /* Join Type. */
    JoinType m_joinType;

    /* Save Join Type. */
    JoinType m_saveJoinType;

    /* Join clause. */
    List* m_joinClause;

    /* Target distribution. */
    Distribution* m_distribution;

    /* Stream info of join's inner side. */
    StreamInfo* m_innerStreamInfo;

    /* Stream info of join's outer side. */
    StreamInfo* m_outerStreamInfo;

    /* Skew info for join inner side. */
    List* m_innerSkewInfo;

    /* Skew Info for join outer side. */
    List* m_outerSkewInfo;

    /* Skew info. */
    List** m_skewInfo;

    /* If this is a outer stream. */
    bool m_isOuterStream;
};

class AggSkewInfo : public SkewInfo {
public:
    AggSkewInfo(PlannerInfo* root, Plan* subplan, RelOptInfo* rel_info);

    ~AggSkewInfo();

    /* Find stream skew info for agg. */
    void findStreamSkewInfo();

    /* Set the distribute keys for agg. */
    void setDistributeKeys(List* distribute_keys);

private:
    /* Reset skew info for later use. */
    void resetSkewInfo();

    /* Find skew info from hint. */
    void findHintSkewInfo();

    /* Find skew info from statistic. */
    void findStatSkewInfo();

    /* Find skew info from outer join' null side. */
    void findNullSkewInfo();

    /* traverse subplan to find outer join's null skew. */
    void traverseSubPlan(Plan* subplan);

    /* Get the target list that may contain null column. */
    List* getSubTargetListByPlan(Plan* plan) const;

    /* Check if the distribute keys of agg are included in the null columns. */
    bool checkOuterJoinNullsForAgg(Plan* jplan) const;

private:
    /* Subplan of agg. */
    Plan* m_subplan;
};

class StreamSkew : public BaseObject {
public:
    StreamSkew(List* ssinfo, bool isVec);

    ~StreamSkew();

    /* Do initial work for skew stream. */
    void init(bool isVec);

    /* Choose suitable stream type for the skew stream. */
    int chooseStreamType(TupleTableSlot* tuple);

    /* Choose suitable stream type for the skew vecstream. */
    void chooseVecStreamType(VectorBatch* batch, int* skewStream);

public:
    /* The node id of local DN */
    int m_localNodeId;

private:
    /* Skew info list. */
    List* m_ssinfo;

    /* Skew qual state list. */
    List* m_skewQual;

    /* EState for skew quals. */
    EState* m_estate;

    /* Expr context for skew quals. */
    ExprContext* m_econtext;
};

extern List* find_skew_join_distribute_keys(Plan* plan);
#endif
