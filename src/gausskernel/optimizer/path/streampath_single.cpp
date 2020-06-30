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
 * -------------------------------------------------------------------------
 *
 * streampath_single.cpp
 *	  functions for path generation in MPP
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/path/streampath_single.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <math.h>

#include "bulkload/foreignroutine.h"
#include "catalog/pg_statistic.h"
#include "commands/copy.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/nodegroups.h"
#include "optimizer/optimizerdebug.h"
#include "optimizer/planmain.h"
#include "optimizer/planner.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planner.h"
#include "optimizer/pruning.h"
#include "optimizer/randomplan.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/streampath.h"
#include "optimizer/tlist.h"
#include "parser/parse_hint.h"
#include "parser/parsetree.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/selfuncs.h"
#ifdef PGXC
#include "commands/tablecmds.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/streamplan.h"
#include "pgxc/pgxc.h"
#endif /* PGXC */

#define IS_DUMMY_UNIQUE(path) (T_Unique == (path)->pathtype && UNIQUE_PATH_NOOP == (((UniquePath*)(path))->umethod))

#define IS_ROUNDROBIN_PATH(path) (LOCATOR_TYPE_RROBIN == (path)->locator_type)

/*
 * @Description: copy stream info pair to a new one.
 *
 * @param[IN] dst: the destination stream info pair.
 * @param[IN] src: the source stream info pair.
 * @return void
 */
void copy_stream_info_pair(StreamInfoPair* dst, StreamInfoPair* src)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: construnctor for PathGen.
 *
 * @param[IN] root: planner base info.
 * @param[IN] rel: relation option info.
 */
PathGen::PathGen(PlannerInfo* root, RelOptInfo* rel) : m_root(root), m_rel(rel)
{}

/*
 * @Description: destructor function for path gen.
 */
PathGen::~PathGen()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: add path to the path list.
 *
 * @param[IN] new_path: new path to be added.
 * @return void
 */
void PathGen::addPath(Path* new_path)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: constructor for JoinPathGen.
 *
 * @param[IN] root: the plannerInfo for this join.
 * @param[IN] joinrel: the join relation.
 * @param[IN] jointype: join type.
 * @param[IN] save_jointype: save join type.
 * @param[IN] sjinfo: extra info about the join for selectivity estimation.
 * @param[IN] semifactors: contains valid data if jointype is SEMI or ANTI.
 * @param[IN] joinclauses: the clauses nodes to do join match.
 * @param[IN] restrictlist: all RestrictInfo nodes to apply at the join.
 * @param[IN] inner_path: the inner subpath for join.
 * @param[IN] outer_path: the outer subpath for join.
 * @param[IN] required_outer: the set of required outer rels.
 */
JoinPathGen::JoinPathGen(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype, JoinType save_jointype,
    SpecialJoinInfo* sjinfo, SemiAntiJoinFactors* semifactors, List* joinclauses, List* restrictinfo, Path* outer_path,
    Path* inner_path, Relids required_outer)
    : PathGen(root, joinrel),
      m_jointype(jointype),
      m_saveJointype(save_jointype),
      m_workspace(NULL),
      m_sjinfo(sjinfo),
      m_semifactors(semifactors),
      m_joinClauses(joinclauses),
      m_joinRestrictinfo(restrictinfo),
      m_pathkeys(NIL),
      m_outerPath(outer_path),
      m_innerPath(inner_path),
      m_outerStreamPath(NULL),
      m_innerStreamPath(NULL),
      m_outerRel(NULL),
      m_innerRel(NULL),
      m_requiredOuter(required_outer),
      m_resourceOwner(NULL),
      m_targetDistribution(NULL),
      m_rrinfoInner(NIL),
      m_rrinfoOuter(NIL),
      m_distributeKeysInner(NIL),
      m_distributeKeysOuter(NIL),
      m_streamInfoPair(NULL),
      m_streamInfoList(NIL),
      m_skewInfo(NULL),
      m_multipleInner(0),
      m_multipleOuter(0),
      m_dop(0),
      m_replicateInner(false),
      m_replicateOuter(false),
      m_redistributeInner(false),
      m_redistributeOuter(false),
      m_canBroadcastInner(false),
      m_canBroadcastOuter(false),
      m_needShuffleInner(false),
      m_needShuffleOuter(false)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: decontructor function for join path generation.
 */
JoinPathGen::~JoinPathGen()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: init member variable.
 *
 * @return void
 */
void JoinPathGen::init()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: decide whether we need redistribute inner or outer side
 *               base on subpath distribute keys, join clauses and target
 *               node group.
 *
 * @return void
 */
void JoinPathGen::initDistribution()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: reset info and free memory for next join generation.
 *
 * @return void
 */
void JoinPathGen::reset()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: check whether we can use smp.
 *
 * @return bool: true -- smp is usable for this situation.
 */
const bool JoinPathGen::isParallelEnable()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

/*
 * @Description: find distribute keys for one join side.
 *
 * @param[IN] stream_outer: true -- the outer side of join.
 * @return List*: list of distribute keys.
 */
List* JoinPathGen::getOthersideKey(bool stream_outer)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

/*
 * @Description: get inner and outer distribute keys for join.
 *
 * @param[IN] desired_keys: desired key that try to meet.
 * @param[IN] exact_match: if there's a desired key, whether we should do exact match.
 * @param[OUT] distribute_keys_outer: distribute keys for outer side.
 * @param[OUT] distribute_keys_inner: distribute keys for outer side.
 * @return void.
 */
void JoinPathGen::getDistributeKeys(
    List** distribute_keys_outer, List** distribute_keys_inner, List* desired_keys, bool exact_match)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: check if there's any alternatives when we disable one or more
 *               join methods, and if not, we should add large cost for the
 *               sole path, which will influence judgement of other joins.
 *
 * @param[OUT] try_eq_related_indirectly: true --  there exists indirect
 *             equivalence relationship between inner_relids and outer_relids.
 * @return bool: there's alternatives when we disable one or more methods
 */
bool JoinPathGen::checkJoinMethodAlternative(bool* try_eq_related_indirectly)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

/*
 * @Description: create stream path for join.
 *
 * @param[IN] stream_outer: the stream is at the outer side of join.
 * @return Path*
 */
Path* JoinPathGen::streamSidePath(bool stream_outer)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

/*
 * @Description: check to see if this path is a nestloop index params path
 *
 * The nestloop param path is fast since the index params path will filter out
 * many rows based on the passed in index qual. This making the nestloop path
 * compatitive with conventional hashjoin path. In hashjoin path we have to
 * build a hash table for the inner rel. However, the nestloop path does not
 * have to do so.
 *
 * a plan segment looks like:
 *
 * id |							operation						   | E-rows
 * ----+-------------------------------------------------------------+-------
 *  1 | ->  Streaming (type: GATHER) 							   | 100000
 *  2 |	  ->  Nested Loop (3,4) 								   | 100000
 *  3 |		 ->  Seq Scan on public.t_hash						   |	 20
 *  4 |		 ->  Index Only Scan using idx_t_rep_a on public.t_rep |	  2
 * (4 rows)
 *
 *   Predicate Information (identified by plan id)
 * ---------------------------------------------------------
 * 4 --Index Only Scan using idx_t_rep_a on public.t_rep
 *  Index Cond: (t_rep.a = t_hash.a)  -- **the Index Condition will filter most tuples**
 *
 * To make the result right, we only allow LHS join for this kind of path.
 * that's to say: only the left side rel can probe into the inner index param replicate rel.
 * this some what act as a *hash filter* for the hash table to disallow duplicate
 * rows return in distributed database.
 *
 * in order to get such a path we have to check the following quals
 *   1. the join path is a nestloop path
 *   2. the inner path is an index scan
 *   3. the inner index scan have a valid params
 *   4. have to be a LHS join
 *
 * @return bool: true if a nestloop param path
 */
const bool JoinPathGen::is_param_path()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

/*
 * @Description: when subpath is replicate, we need to check if we can use redistribute.
 *
 * @return bool: true -- redistribution can be used.
 */
bool JoinPathGen::isReplicateJoinCanRedistribute()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

/*
 * @Description: add join path to path list.
 *
 * @param[IN] path: join path.
 * @param[IN] desired_key: desired key that try to meet.
 * @param[IN] exact_match: if there's a desired key, whether we should do exact match.
 * @return void
 */
void JoinPathGen::addJoinPath(Path* path, List* desired_key = NIL, bool exact_match = false)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: create stream path for both join side.
 *
 * @return void
 */
void JoinPathGen::addJoinStreamPath()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: the most important part of join path generation is create
 *               stream path for join base on join clauses, subpath distribution,
 *               target nodegroup info, data skew info and smp info. We should
 *               consider all of these situations one by one to create a join path.
 *               Only the stream info is set at this stage, the stream path will
 *               be created later.
 *
 * @return void
 */
void JoinPathGen::addJoinStreamInfo()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: Add stream info base on join clauses and subpath distributions.
 *               This part is the basic part of join generation for MPP structure .
 *
 * @return void
 */
void JoinPathGen::addStreamMppInfo()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: Add stream info based on target nodegroup and subpath nodegroup.
 *
 * @return void
 */
void JoinPathGen::addStreamNodeGroupInfo()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: Add stream info based on parallel degree.
 *
 * @return void
 */
void JoinPathGen::addStreamParallelInfo()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: Add stream info based on data skew info when we use redistribute.
 *
 * @return void
 */
void JoinPathGen::addStreamSkewInfo()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: choose suitable parallel stream(like local stream)
 *               for parallel plan.
 *
 * @return void
 */
bool JoinPathGen::addJoinParallelInfo()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

/*
 * @Description: create stream info pair and set base stream info.
 *
 * @param[IN] inner_type: inner stream type.
 * @param[IN] outer_type: outer stream type.
 * @param[IN] inner_keys: inner distribute keys.
 * @param[IN] outer_keys: outer distribute keys.
 * @return void
 */
void JoinPathGen::setStreamBaseInfo(StreamType inner_type, StreamType outer_type, List* inner_keys, List* outer_keys)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: set stream info for nodegroup.
 *
 * @param[IN] stream_outer: true -- outer side of join.
 * @return bool: not suitable for this target nodegroup.
 */
bool JoinPathGen::setStreamNodeGroupInfo(bool stream_outer)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

/*
 * @Description: check data skew info and try to generate a optimize path.
 *               And we need know where the skew info comes from to decide
 *               futher option.
 *
 * @return int: SKEW_RES_NONE -- no skew optimize path is generate.
 *              SKEW_RES_STATISTIC -- skew info comes from statistic info.
 *              SKEW_RES_HINT -- skew info comes from plan hint.
 *              SKEW_RES_RULE -- skew info comes from rules.
 */
uint32 JoinPathGen::setStreamSkewInfo()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return 0;
}

/*
 * @Description: set parallel info include consumer/producer dop and
 *               parallel stream type.
 *
 * @param[IN] stream_outer: is outer side of join.
 * @param[IN] sstype: smp stream type.
 * @return void
 */
void JoinPathGen::setStreamParallelInfo(bool stream_outer, SmpStreamType sstype)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: set distribute keys for join path.
 *
 * @param[IN] joinpath: join path.
 * @param[IN] desired_key: desired key that try to meet.
 * @param[IN] exact_match: if there's a desired key, whether we should do exact match.
 * @return bool: if the path is valid.
 */
const bool JoinPathGen::setJoinDistributeKeys(JoinPath* joinpath, List* desired_key, bool exact_match)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return false;
}

/*
 * @Description: check if we can and need local redistribtue.
 *
 * @param[OUT] inner_can_local_distribute: we can use local redistribute in inner side.
 * @param[OUT] outer_can_local_distribute: we can use local redistribute in outer side.
 * @param[OUT] inner_need_local_distribute: local redistribute is needed in inner side.
 * @param[OUT] outer_need_local_distribute: local redistribute is needed in outer side.
 * @return void
 */
void JoinPathGen::parallelLocalRedistribute(bool* inner_can_local_distribute, bool* outer_can_local_distribute,
    bool* inner_need_local_distribute, bool* outer_need_local_distribute)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: create a unique path for unique join.
 *
 * @param[IN] stream_outer: if it is the outer side of join.
 * @param[OUT] pathkeys: path sort keys.
 * @return Path*: unique path.
 */
Path* JoinPathGen::makeJoinUniquePath(bool stream_outer, List* pathkeys)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

/*
 * @Description: create a unique path for unique join with
 *               skewness at the no-unique side.
 *
 * @param[IN] stream_outer: if it is the outer side of join.
 * @param[OUT] pathkeys: path sort keys.
 * @return Path*: unique path.
 */
Path* JoinPathGen::makeJoinSkewUniquePath(bool stream_outer, List* pathkeys)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

/*
 * @Description: create a new stream info pair from an old one.
 *
 * @param[IN] streamInfoPair: old stream info pair.
 * @return void
 */
void JoinPathGen::newStreamInfoPair(StreamInfoPair* streamInfoPair)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: confirm if we can find distribute keys at subpath's targetlist.
 *
 * @return void
 */
void JoinPathGen::confirmDistributeKeys(StreamInfo* sinfo)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: constructor for HashJoinPathGen.
 *
 * @param[IN] root: the plannerInfo for this join.
 * @param[IN] joinrel: the join relation.
 * @param[IN] jointype: join type.
 * @param[IN] save_jointype: save join type.
 * @param[IN] sjinfo: extra info about the join for selectivity estimation.
 * @param[IN] semifactors: contains valid data if jointype is SEMI or ANTI.
 * @param[IN] hashclauses: the RestrictInfo nodes to use as hash clauses
 *		                   (this should be a subset of the restrict_clauses list).
 * @param[IN] restrictlist: all RestrictInfo nodes to apply at the join.
 * @param[IN] inner_path: the inner subpath for join.
 * @param[IN] outer_path: the outer subpath for join.
 * @param[IN] required_outer: the set of required outer rels.
 */
HashJoinPathGen::HashJoinPathGen(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype, JoinType save_jointype,
    SpecialJoinInfo* sjinfo, SemiAntiJoinFactors* semifactors, Path* outer_path, Path* inner_path, List* restrictlist,
    Relids required_outer, List* hashclauses)
    : JoinPathGen(root, joinrel, jointype, save_jointype, sjinfo, semifactors, hashclauses, restrictlist, outer_path,
          inner_path, required_outer),
      m_hashClauses(hashclauses)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: release memory at deconstruct stage.
 */
HashJoinPathGen::~HashJoinPathGen()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: add hash join path to path list base on the
 *               target nodegroup and parallel degree.
 *
 * @param[IN] workspace: work space for join cost.
 * @param[IN] targetDistribution: target nodegroup distribution.
 * @param[IN] dop: target parallel degree.
 * return void
 */
void HashJoinPathGen::addHashJoinPath(JoinCostWorkspace* workspace, Distribution* targetDistribution, int dop)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: create stream path for join, then create hash join path
 *               and add it to path list.
 *
 * return void
 */
void HashJoinPathGen::addHashjoinPathToList()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: create hash join path.
 *
 * return Path*:
 */
Path* HashJoinPathGen::createHashJoinPath()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

/*
 * @Description: Preliminary estimate of the cost of a hashjoin path.
 *
 * This must quickly produce lower-bound estimates of the path's startup and
 * total costs.  If we are unable to eliminate the proposed path from
 * consideration using the lower bounds, final_cost_hashjoin will be called
 * to obtain the final estimates.
 *
 * The exact division of labor between this function and final_cost_hashjoin
 * is private to them, and represents a tradeoff between speed of the initial
 * estimate and getting a tight lower bound.  We choose to not examine the
 * join quals here (other than by counting the number of hash clauses),
 * so we can't do much with CPU costs.  We do assume that
 * ExecChooseHashTableSize is cheap enough to use here.
 *
 * return void
 */
void HashJoinPathGen::initialCostHashjoin()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: Final estimate of the cost and result size of a hashjoin path.
 *
 * Notice: the numbatches estimate is also saved into 'path' for use later
 *
 * @param[IN] path: hash join path
 * @param[IN] hasalternative: has alternative join.
 * return void
 */
void HashJoinPathGen::finalCostHashjoin(HashPath* path, bool hasalternative)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: constructor for NestLoopPathGen.
 *
 * @param[IN] root: the plannerInfo for this join.
 * @param[IN] joinrel: the join relation.
 * @param[IN] jointype: join type.
 * @param[IN] save_jointype: save join type.
 * @param[IN] sjinfo: extra info about the join for selectivity estimation.
 * @param[IN] semifactors: contains valid data if jointype is SEMI or ANTI.
 * @param[IN] restrictlist: all RestrictInfo nodes to apply at the join.
 * @param[IN] pathkeys: the path keys of the new join path.
 * @param[IN] inner_path: the inner subpath for join.
 * @param[IN] outer_path: the outer subpath for join.
 * @param[IN] required_outer: the set of required outer rels.
 */
NestLoopPathGen::NestLoopPathGen(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype, JoinType save_jointype,
    SpecialJoinInfo* sjinfo, SemiAntiJoinFactors* semifactors, Path* outer_path, Path* inner_path, List* restrictlist,
    List* pathkeys, Relids required_outer)
    : JoinPathGen(root, joinrel, jointype, save_jointype, sjinfo, semifactors, restrictlist, restrictlist, outer_path,
          inner_path, required_outer)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: release memory at deconstruct stage.
 */
NestLoopPathGen::~NestLoopPathGen()
{}

/*
 * @Description: add nestloop join path to path list base on the
 *               target nodegroup and parallel degree.
 *
 * @param[IN] workspace: work space for join cost.
 * @param[IN] targetDistribution: target nodegroup distribution.
 * @param[IN] dop: target parallel degree.
 * return void
 */
void NestLoopPathGen::addNestLoopPath(JoinCostWorkspace* workspace, Distribution* targetDistribution, int dop)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: create stream path for join, then create nestloop join path
 *               and add it to path list.
 *
 * return void
 */
void NestLoopPathGen::addNestloopPathToList()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: Preliminary estimate of the cost of a nestloop join path.
 *
 * This must quickly produce lower-bound estimates of the path's startup and
 * total costs.  If we are unable to eliminate the proposed path from
 * consideration using the lower bounds, final_cost_nestloop will be called
 * to obtain the final estimates.
 *
 * The exact division of labor between this function and final_cost_nestloop
 * is private to them, and represents a tradeoff between speed of the initial
 * estimate and getting a tight lower bound.  We choose to not examine the
 * join quals here, since that's by far the most expensive part of the
 * calculations.  The end result is that CPU-cost considerations must be
 * left for the second phase.
 *
 * return void
 */
void NestLoopPathGen::initialCostNestloop()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: Final estimate of the cost and result size of a hashjoin path.
 *
 * return void
 */
void NestLoopPathGen::finalCostNestloop(NestPath* path, bool hasalternative)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: Creates a pathnode corresponding to a nestloop join between two
 *	             relations.
 *
 * return void
 */
Path* NestLoopPathGen::createNestloopPath()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}

/*
 * @Description: constructor for NestLoopPathGen.
 *
 * @param[IN] root: the plannerInfo for this join.
 * @param[IN] joinrel: the join relation.
 * @param[IN] jointype: join type.
 * @param[IN] save_jointype: save join type.
 * @param[IN] sjinfo: extra info about the join for selectivity estimation.
 * @param[IN] semifactors: contains valid data if jointype is SEMI or ANTI.
 * @param[IN] restrictlist: all RestrictInfo nodes to apply at the join.
 * @param[IN] pathkeys: the path keys of the new join path.
 * @param[IN] mergeclauses: the RestrictInfo nodes to use as merge clauses
 *		      (this should be a subset of the restrict_clauses list)
 * @param[IN] outersortkeys: sort varkeys for the outer relation.
 * @param[IN] innersortkeys: sort varkeys for the inner relation.
 * @param[IN] inner_path: the inner subpath for join.
 * @param[IN] outer_path: the outer subpath for join.
 * @param[IN] required_outer: the set of required outer rels.
 */
MergeJoinPathGen::MergeJoinPathGen(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype, JoinType save_jointype,
    SpecialJoinInfo* sjinfo, SemiAntiJoinFactors* semifactors, Path* outer_path, Path* inner_path,
    List* restrict_clauses, List* pathkeys, Relids required_outer, List* mergeclauses, List* outersortkeys,
    List* innersortkeys)
    : JoinPathGen(root, joinrel, jointype, save_jointype, sjinfo, semifactors, restrict_clauses, restrict_clauses,
          outer_path, inner_path, required_outer)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: release memory at deconstruct stage.
 */
MergeJoinPathGen::~MergeJoinPathGen()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: add merge join path to path list base on the
 *               target nodegroup and parallel degree.
 *
 * @param[IN] workspace: work space for join cost.
 * @param[IN] targetDistribution: target nodegroup distribution.
 * @param[IN] dop: target parallel degree.
 * return void
 */
void MergeJoinPathGen::addMergeJoinPath(JoinCostWorkspace* workspace, Distribution* targetDistribution, int dop)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: create stream path for join, then create merge join path
 *               and add it to path list.
 *
 * return void
 */
void MergeJoinPathGen::addMergejoinPathToList()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: Preliminary estimate of the cost of a mergejoin path.
 *
 * This must quickly produce lower-bound estimates of the path's startup and
 * total costs.  If we are unable to eliminate the proposed path from
 * consideration using the lower bounds, final_cost_mergejoin will be called
 * to obtain the final estimates.
 *
 * The exact division of labor between this function and final_cost_mergejoin
 * is private to them, and represents a tradeoff between speed of the initial
 * estimate and getting a tight lower bound.  We choose to not examine the
 * join quals here, except for obtaining the scan selectivity estimate which
 * is really essential (but fortunately, use of caching keeps the cost of
 * getting that down to something reasonable).
 * We also assume that cost_sort is cheap enough to use here.
 *
 * return void
 */
void MergeJoinPathGen::initialCostMergejoin()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: Preliminary estimate of the cost of a mergejoin path.
 *
 * Unlike other costsize functions, this routine makes one actual decision:
 * whether we should materialize the inner path.  We do that either because
 * the inner path can't support mark/restore, or because it's cheaper to
 * use an interposed Material node to handle mark/restore.	When the decision
 * is cost-based it would be logically cleaner to build and cost two separate
 * paths with and without that flag set; but that would require repeating most
 * of the cost calculations, which are not all that cheap.	Since the choice
 * will not affect output pathkeys or startup cost, only total cost, there is
 * no possibility of wanting to keep both paths.  So it seems best to make
 * the decision here and record it in the path's materialize_inner field.
 *
 * return void
 */
void MergeJoinPathGen::finalCostMergejoin(MergePath* path, bool hasalternative)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

/*
 * @Description: Creates a pathnode corresponding to a mergejoin join between
 *	             two relations.
 *
 * return Path*: mergejoin path.
 */
Path* MergeJoinPathGen::createMergejoinPath()
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
}
