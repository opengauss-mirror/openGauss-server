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
 * streampath.h
 *        functions for distribute join path generation in GaussDB
 * 
 * 
 * IDENTIFICATION
 *        src/include/optimizer/streampath.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef STREAMPATH_H
#define STREAMPATH_H

#include "nodes/relation.h"
#include "optimizer/dataskew.h"
#include "optimizer/streamplan.h"

class PathGen : public BaseObject {
public:
    PathGen(PlannerInfo* root, RelOptInfo* rel);

    virtual ~PathGen();

    /* Add a path to path list. */
    void addPath(Path* new_path);

protected:
    /* PlannerInfo for this path. */
    PlannerInfo* m_root;

    /* Rel info for this path. */
    RelOptInfo* m_rel;
};

class JoinPathGenBase : public PathGen {
public:
    JoinPathGenBase(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype, JoinType save_jointype,
        JoinPathExtraData* extra, List* joinclauses, List* restrictinfo,
        Path* outer_path, Path* inner_path, Relids required_outer);

    virtual ~JoinPathGenBase();

protected:
    /* Init member variable. */
    void init();

    /* special process for list/range distributed table */
    void initRangeListDistribution();

    /* Reset member variable for later use. */
    void reset();

    /* Check if we can create parallel join path. */
    const bool isParallelEnable();

    /* check if is a nestloop parameter path */
    const bool is_param_path();

    /* Add join path to path list. */
    virtual void addJoinPath(Path* path, List* desired_key, bool exact_match);

    /* Create stream path for join. */
    void addJoinStreamPath();

    /* Add stream info for MPP structure. */
    void addStreamMppInfo();

    /* Add stream info for parallel execution. */
    void addStreamParallelInfo();

    /* Add parallel info for join. */
    virtual bool addJoinParallelInfo();

    /* Set base stream info. */
    void setStreamBaseInfo(StreamType inner_type, StreamType outer_type, List* inner_keys, List* outer_keys);

    /* Check if we need local redistribute, */
    void parallelLocalRedistribute(bool* inner_can_local_distribute, bool* outer_can_local_distribute,
        bool* inner_need_local_distribute, bool* outer_need_local_distribute);

    /* Find distribute keys for one join side. */
    List* getOthersideKey(bool stream_outer);

    /* Get distribute keys for join. */
    void getDistributeKeys(
        List** distribute_keys_outer, List** distribute_keys_inner, List* desired_keys, bool exact_match);

    /* Check if we can alteer join method. */
    bool checkJoinMethodAlternative(bool* try_eq_related_indirectly);

    /* Check if the replicate path can do redistribute. */
    bool isReplicateJoinCanRedistribute();

    /* Create unique join path. */
    Path* makeJoinUniquePath(bool stream_outer, List* pathkeys);

    /* create a unique path for unique join with skewness at the no-unique side */
    virtual Path* makeJoinSkewUniquePath(bool stream_outer, List* pathkeys);

    /* set distribute keys for join path. */
    virtual const bool setJoinDistributeKeys(JoinPath* joinpath, List* desired_key = NIL, bool exact_match = false);

    /* create a new stream info pair from the old one. */
    void newStreamInfoPair(StreamInfoPair* streamInfoPair);

    /* Create stream path for join. */
    virtual Path* streamSidePath(bool stream_outer);

    /* Set stream parallel execution info. */
    virtual void setStreamParallelInfo(bool stream_outer, SmpStreamType sstype = PARALLEL_NONE);

protected:
    /* Join method. */
    NodeTag m_joinmethod;

    /* Join type. */
    JoinType m_jointype;

    /* Save join type. */
    JoinType m_saveJointype;

    /* Cost work space for join. */
    JoinCostWorkspace* m_workspace;

    /*extra data for join*/
    JoinPathExtraData* m_extra;

    /* Join clauses list. */
    List* m_joinClauses;

    /* Join restrict list. */
    List* m_joinRestrictinfo;

    /* path keys of the join. */
    List* m_pathkeys;

    /* Outer path of join. */
    Path* m_outerPath;

    /* Inner path of join. */
    Path* m_innerPath;

    /* Outer stream path created for join. */
    Path* m_outerStreamPath;

    /* Inner Stream path created for join. */
    Path* m_innerStreamPath;

    /* Outer rel info. */
    RelOptInfo* m_outerRel;

    /* Inner rel info. */
    RelOptInfo* m_innerRel;

    /* The set of required outer rels. */
    Relids m_requiredOuter;

    /* Resource owner for join generation. */
    ResourceOwner m_resourceOwner;

    /* The target distibution of node group. */
    Distribution* m_targetDistribution;

    /* The restrictinfo on which we can join directly. */
    List* m_rrinfoInner;

    /* The restrictinfo on which we can join directly. */
    List* m_rrinfoOuter;

    /* Distribute keys of inner stream. */
    List* m_distributeKeysInner;

    /* Distribute keys of outer stream. */
    List* m_distributeKeysOuter;

    /* Stream info pair. */
    StreamInfoPair* m_streamInfoPair;

    /* Stream info list for the join. */
    List* m_streamInfoList;

    /* Skew mutiple for inner redistribute. */
    double m_multipleInner;

    /* Skew multiple for outer redistribute */
    double m_multipleOuter;

    /* Parallel degree for this join. */
    int m_dop;

    /* If the inner path is replicate. */
    bool m_replicateInner;

    /* If the outer path is replicate. */
    bool m_replicateOuter;

    /* If the inner path is rangelist. */
    bool m_rangelistInner;

    /* If the outer path is rangelist. */
    bool m_rangelistOuter;

    /* true if outer and inner path's rangelist boundary same. */
    bool m_sameBoundary;

    /* If we should redistribute the inner side. */
    bool m_redistributeInner;

    /* If we should redistribute the outer side. */
    bool m_redistributeOuter;

    /* If  we can broadcast the inner side. */
    bool m_canBroadcastInner;

    /* If we can broadcast the outer side. */
    bool m_canBroadcastOuter;
};

class JoinPathGen : public JoinPathGenBase {
public:
    JoinPathGen(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype, JoinType save_jointype,
        JoinPathExtraData* extra, List* joinclauses, List* restrictinfo,
        Path* outer_path, Path* inner_path, Relids required_outer);

    virtual ~JoinPathGen();

protected:
    /* Init if we need stream. */
    void initDistribution();

    /* Add stream info for join. */
    void addJoinStreamInfo();

    /* Set stream parallel execution info. */
    void setStreamParallelInfo(bool stream_outer, SmpStreamType sstype = PARALLEL_NONE);

    /* Add parallel info for join. */
    bool addJoinParallelInfo();

    /* set distribute keys for join path. */
    const bool setJoinDistributeKeys(JoinPath* joinpath, List* desired_key = NIL, bool exact_match = false);
#ifdef ENABLE_MULTIPLE_NODES
    /* Init member variable. */
    void init();

    /* Check if we can create parallel join path. */
    const bool isParallelEnable();

    /* Add stream info for multi node group execution. */
    void addStreamNodeGroupInfo();

    /* Add stream info for MPP structure. */
    void addStreamMppInfo();

    /* Add stream info for skew optimization. */
    void addStreamSkewInfo();

    /* Set stream info for multi nodegroup. */
    bool setStreamNodeGroupInfo(bool stream_outer);

    /* Set stream skew optimization info. */
    uint32 setStreamSkewInfo();

    /* Confirm the distribute keys in the target list of subpath. */
    void confirmDistributeKeys(StreamInfo* sinfo);

    /* create a unique path for unique join with skewness at the no-unique side */
    Path* makeJoinSkewUniquePath(bool stream_outer, List* pathkeys);

    /* Create stream path for join. */
    virtual Path* streamSidePath(bool stream_outer);
protected:
    /* Skew optimization info for join. */
    JoinSkewInfo* m_skewInfo;

    /* If we need shuffle the inner side. */
    bool m_needShuffleInner;

    /* If we need shuffle the outer side. */
    bool m_needShuffleOuter;
#endif
};

class HashJoinPathGen : public JoinPathGen {
public:
    HashJoinPathGen(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype, JoinType save_jointype,
        JoinPathExtraData* extra, Path* outer_path, Path* inner_path,
        List* restrictlist, Relids required_outer, List* hashclauses);

    virtual ~HashJoinPathGen();

    /* Create Hash join path and add it to path list. */
    void addHashJoinPath(JoinCostWorkspace* workspace, Distribution* targetDistribution, int dop);

private:
    /* Add hash join path to path list. */
    void addHashjoinPathToList();

    /* Initial the cost of hash join. */
    void initialCostHashjoin();

    /* Finalize the cost of hash join. */
    void finalCostHashjoin(HashPath* path, bool hasalternative);

    /* Create hash join path. */
    Path* createHashJoinPath();

private:
    /* Hash clauses for hash join. */
    List* m_hashClauses;
};

class NestLoopPathGen : public JoinPathGen {
public:
    NestLoopPathGen(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype, JoinType save_jointype,
        JoinPathExtraData* extra, Path* outer_path, Path* inner_path,
        List* restrictlist, List* pathkeys, Relids required_outer);

    virtual ~NestLoopPathGen();

    /* Create nest loop path and add it to path list. */
    void addNestLoopPath(JoinCostWorkspace* workspace, Distribution* targetDistribution, int dop);

private:
    /* Add nest loop path to path list. */
    void addNestloopPathToList();

    /* Initial the cost of nest loop join. */
    void initialCostNestloop();

    /* Finalize the cost of nest loop join. */
    void finalCostNestloop(NestPath* path, bool hasalternative);

    /* Create nest loop path. */
    Path* createNestloopPath();
};

class MergeJoinPathGen : public JoinPathGen {
public:
    MergeJoinPathGen(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype, JoinType save_jointype,
        JoinPathExtraData* extra, Path* outer_path, Path* inner_path,
        List* restrict_clauses, List* pathkeys, Relids required_outer, List* mergeclauses, List* outersortkeys,
        List* innersortkeys);

    virtual ~MergeJoinPathGen();

    /* Create merge join path and add to path list. */
    void addMergeJoinPath(JoinCostWorkspace* workspace, Distribution* targetDistribution, int dop);

private:
    /* Add merge join path to path list. */
    void addMergejoinPathToList();

    /* Initial the cost of merge join. */
    void initialCostMergejoin();

    /* Finalize the cost of merge join. */
    void finalCostMergejoin(MergePath* path, bool hasalternative);

    /* Create mergejoin path. */
    Path* createMergejoinPath();

private:
    /* Merge clauses for merge join. */
    List* m_mergeClauses;

    /* Sort key of inner path. */
    List* m_innerSortKeys;

    /* Sort key of outer path. */
    List* m_outerSortKeys;
};

#endif /* STREAMPATH_H */
