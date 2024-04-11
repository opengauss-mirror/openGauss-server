/* -------------------------------------------------------------------------
 *
 * streampath_base.cpp
 *	  functions for stream path generation
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/gausskernel/optimizer/path/streampath_base.cpp
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

/*
 * @Description: copy stream info pair to a new one.
 *
 * @param[IN] dst: the destination stream info pair.
 * @param[IN] src: the source stream info pair.
 * @return void
 */
void copy_stream_info_pair(StreamInfoPair* dst, StreamInfoPair* src)
{
    if (dst == NULL || src == NULL)
        return;

    errno_t rc = EOK;

    rc = memcpy_s(&dst->inner_info, sizeof(StreamInfo), &src->inner_info, sizeof(StreamInfo));
    securec_check(rc, "\0", "\0");

    rc = memcpy_s(&dst->outer_info, sizeof(StreamInfo), &src->outer_info, sizeof(StreamInfo));
    securec_check(rc, "\0", "\0");

    dst->skew_optimize = SKEW_RES_NONE;
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
    m_rel = NULL;
    m_root = NULL;
}

/*
 * @Description: add path to the path list.
 *
 * @param[IN] new_path: new path to be added.
 * @return void
 */
void PathGen::addPath(Path* new_path)
{
    add_path(m_root, m_rel, new_path);
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
JoinPathGenBase::JoinPathGenBase(PlannerInfo* root, RelOptInfo* joinrel, JoinType jointype, JoinType save_jointype,
    JoinPathExtraData* extra, List* joinclauses, List* restrictinfo,
    Path* outer_path, Path* inner_path, Relids required_outer)
    : PathGen(root, joinrel),
      m_jointype(jointype),
      m_saveJointype(save_jointype),
      m_workspace(NULL),
      m_extra(extra),
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
      m_multipleInner(0),
      m_multipleOuter(0),
      m_dop(0),
      m_replicateInner(false),
      m_replicateOuter(false),
      m_rangelistInner(false),
      m_rangelistOuter(false),
      m_sameBoundary(false),
      m_redistributeInner(false),
      m_redistributeOuter(false),
      m_canBroadcastInner(false),
      m_canBroadcastOuter(false)
{
    init();
}

/*
 * @Description: decontructor function for join path generation.
 */
JoinPathGenBase::~JoinPathGenBase()
{
    if (m_resourceOwner != NULL) {
        ResourceOwnerRelease(m_resourceOwner, RESOURCE_RELEASE_BEFORE_LOCKS, false, false);
        ResourceOwnerRelease(m_resourceOwner, RESOURCE_RELEASE_LOCKS, false, false);
        ResourceOwnerRelease(m_resourceOwner, RESOURCE_RELEASE_AFTER_LOCKS, false, false);
        ResourceOwnerDelete(m_resourceOwner);
        m_resourceOwner = NULL;
    }

    m_distributeKeysInner = NIL;
    m_distributeKeysOuter = NIL;
    m_innerPath = NULL;
    m_innerRel = NULL;
    m_innerStreamPath = NULL;
    m_joinClauses = NIL;
    m_joinRestrictinfo = NIL;
    m_outerPath = NULL;
    m_outerRel = NULL;
    m_outerStreamPath = NULL;
    m_pathkeys = NIL;
    m_requiredOuter = NULL;
    m_resourceOwner = NULL;
    m_rrinfoInner = NIL;
    m_rrinfoOuter = NIL;
    m_streamInfoList = NIL;
    m_streamInfoPair = NULL;
    m_targetDistribution = NULL;
    m_workspace = NULL;
}

/*
 * @Description: init member variable.
 *
 * @return void
 */
void JoinPathGenBase::init()
{
    m_joinmethod = T_HashJoin;
    m_workspace = NULL;
    m_pathkeys = NIL;
    m_targetDistribution = NULL;

    m_outerStreamPath = NULL;
    m_innerStreamPath = NULL;

    m_innerRel = m_innerPath->parent;
    m_outerRel = m_outerPath->parent;

    m_rrinfoInner = NIL;
    m_rrinfoOuter = NIL;

    m_distributeKeysInner = m_innerPath->distribute_keys;
    m_distributeKeysOuter = m_outerPath->distribute_keys;

    /* Init replicate flag. */
    m_replicateInner = is_replicated_path(m_innerPath);
    m_replicateOuter = is_replicated_path(m_outerPath);

    /* Init broadcast flag base on join type etc. */
    m_canBroadcastInner =
        can_broadcast_inner(m_jointype, m_saveJointype, m_replicateOuter, m_distributeKeysOuter, m_outerPath);
    m_canBroadcastOuter =
        can_broadcast_outer(m_jointype, m_saveJointype, m_replicateInner, m_distributeKeysInner, m_innerPath);

    m_streamInfoList = NIL;
    m_streamInfoPair = NULL;

    /*
     * Create a resource owner to keep track of resources
     * in order to release resources when catch the exception.
     */
    m_resourceOwner = ResourceOwnerCreate(t_thrd.utils_cxt.CurrentResourceOwner, "join_path_gen",
        THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_OPTIMIZER));

    m_dop = 0;
    m_multipleInner = 1.0;
    m_multipleOuter = 1.0;

    m_redistributeInner = false;
    m_redistributeOuter = false;
}

void JoinPathGenBase::initRangeListDistribution()
{
    m_rangelistOuter = IsLocatorDistributedBySlice(m_outerPath->locator_type);
    m_rangelistInner = IsLocatorDistributedBySlice(m_innerPath->locator_type);
    m_sameBoundary = false;
    
    if (m_rangelistOuter || m_rangelistInner) {
        m_sameBoundary = IsSliceInfoEqualByOid(m_outerPath->rangelistOid, m_innerPath->rangelistOid);
        if (m_sameBoundary) {
            if (m_redistributeOuter || m_redistributeInner) {
                /* if one side needs to redistribtue, the other side should redistribute too */
                m_redistributeOuter = true;
                m_redistributeInner = true;
            } else if (m_rrinfoInner && m_rrinfoOuter && !equal(m_rrinfoInner, m_rrinfoOuter)) {
                /* if distribute on different join keys, need to redistribute range/list table */
                if (m_rangelistOuter) {
                    m_redistributeOuter = true;
                }
                if (m_rangelistInner) {
                    m_redistributeInner = true;
                }
            }
        } else {
            if (m_rangelistOuter && !m_redistributeOuter) {
                /* treat range/list as roundrobin */
                m_redistributeOuter = true;
            }
            if (m_rangelistInner && !m_redistributeInner) {
                /* treat range/list as roundrobin */
                m_redistributeInner = true;
            }
        }
        if (m_rangelistOuter && m_redistributeOuter) {
            m_distributeKeysOuter = NIL;
        }
        if (m_rangelistInner && m_redistributeInner) {
            m_distributeKeysInner = NIL;
        }
    }
}

/*
 * @Description: reset info and free memory for next join generation.
 *
 * @return void
 */
void JoinPathGenBase::reset()
{
    m_multipleInner = 1.0;
    m_multipleOuter = 1.0;

    m_streamInfoPair = NULL;

    list_free(m_streamInfoList);
    m_streamInfoList = NIL;

    list_free(m_rrinfoInner);
    list_free(m_rrinfoOuter);
    m_rrinfoInner = NIL;
    m_rrinfoOuter = NIL;
}

/*
 * @Description: check whether we can use smp.
 *
 * @return bool: true -- smp is usable for this situation.
 */
const bool JoinPathGenBase::isParallelEnable()
{
    /* SMP do not support merge join. */
    if (m_joinmethod == T_MergeJoin)
        return false;

    /*
     * If both sides are not parallelized,
     * then there is no need to parallel the join.
     */
    if (m_innerPath->dop <= 1 && m_outerPath->dop <= 1)
        return false;

    /* Avoid parameterized path to be parallelized. */
    if (m_innerPath->param_info != NULL || m_outerPath->param_info != NULL)
        return false;

    if (u_sess->opt_cxt.query_dop > 1 && IS_STREAM_PLAN)
        return true;
    else
        return false;
}

/*
 * @Description: find distribute keys for one join side.
 *
 * @param[IN] stream_outer: true -- the outer side of join.
 * @return List*: list of distribute keys.
 */
List* JoinPathGenBase::getOthersideKey(bool stream_outer)
{
    List* rinfo = stream_outer ? m_rrinfoInner : m_rrinfoOuter;
    RelOptInfo* otherside_rel = stream_outer ? m_outerRel : m_innerRel;
    double* multiple = stream_outer ? &m_multipleOuter : &m_multipleInner;

    ListCell *lc1 = NULL, *lc2 = NULL, *lc3 = NULL;
    List* key_list = NULL;
    List* targetlist = otherside_rel->reltarget->exprs;
    Node* match_var = NULL;
    ListCell* cell = NULL;

    foreach (cell, rinfo) {
        EquivalenceClass* oeclass = NULL;
        RestrictInfo* restrictinfo = (RestrictInfo*)lfirst(cell);
        match_var = NULL;

        if (bms_is_subset(restrictinfo->left_relids, otherside_rel->relids)) {
            oeclass = restrictinfo->left_ec;
        } else {
            Assert(bms_is_subset(restrictinfo->right_relids, otherside_rel->relids));
            oeclass = restrictinfo->right_ec;
        }

        Assert(restrictinfo->orclause == NULL);

        foreach (lc1, oeclass->ec_members) {
            EquivalenceMember* em = (EquivalenceMember*)lfirst(lc1);
            Node* nem = (Node*)em->em_expr;
            Oid datatype = exprType(nem);
            List* vars = NIL;
            Relids relIds;

            if (!OidIsValid(datatype) || !IsTypeDistributable(datatype))
                continue;

            relIds = pull_varnos(nem);
            if (bms_is_empty(relIds) || !bms_is_subset(relIds, otherside_rel->relids)) {
                bms_free(relIds);
                continue;
            }
            bms_free(relIds);

            /*
             * Check if all vars in sub targetlist
             *
             * For coalesce column in target list, it will presented as a Place Holder,
             * so we will leave it as it is without expand it.
             */
            vars = pull_var_clause(nem, PVC_REJECT_AGGREGATES, PVC_INCLUDE_PLACEHOLDERS);
            foreach (lc2, vars) {
                Node* node = (Node*)lfirst(lc2);
                foreach (lc3, targetlist) {
                    Node* te = (Node*)lfirst(lc3);
                    if ((IsA(te, Var) && _equalSimpleVar((Var*)te, node)) || (!IsA(te, Var) && equal(te, node))) {
                        break;
                    }
                }
                if (lc3 == NULL) /* doesn't find the same in sub target list */
                    break;
            }
            list_free(vars);
            if (lc2 != NULL) /* not all vars in sub targetlist */
                continue;

            match_var = nem;
            break;
        }

        if (match_var != NULL) {
            key_list = lappend(key_list, (void*)copyObject(match_var));
        } else {
            list_free(key_list);
            return NIL;
        }
    }

    /* Calculate skew multiple of the distribute keys. */
    *multiple = get_multiple_by_distkey(m_root, key_list, otherside_rel->rows);

    if (!ng_is_distribute_key_valid(m_root, key_list, targetlist)) {
        list_free(key_list);
        key_list = NIL;
    }

    return key_list;
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
void JoinPathGenBase::getDistributeKeys(
    List** distribute_keys_outer, List** distribute_keys_inner, List* desired_keys, bool exact_match)
{
    get_distribute_keys(m_root,
        m_joinClauses,
        m_outerPath,
        m_innerPath,
        &m_multipleOuter,
        &m_multipleInner,
        distribute_keys_outer,
        distribute_keys_inner,
        desired_keys,
        exact_match);
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
bool JoinPathGenBase::checkJoinMethodAlternative(bool* try_eq_related_indirectly)
{
    bool hasalternative = false;
    ListCell* l = NULL;

    foreach (l, m_joinRestrictinfo) {
        RestrictInfo* restrictinfo = (RestrictInfo*)lfirst(l);

        /* Check if clause is a hashable or mergeable operator clause */
        if (restrictinfo->can_join && clause_sides_match_join(restrictinfo, m_outerRel, m_innerRel)) {
            if (u_sess->attr.attr_sql.enable_hashjoin && restrictinfo->hashjoinoperator != InvalidOid)
                hasalternative = true;
            if (u_sess->attr.attr_sql.enable_mergejoin && restrictinfo->mergeopfamilies != NIL)
                hasalternative = true;
            if (u_sess->attr.attr_sql.enable_hashjoin || u_sess->attr.attr_sql.enable_mergejoin)
                *try_eq_related_indirectly = true;
        }
        if (hasalternative)
            break;
    }

    if (u_sess->attr.attr_sql.enable_nestloop && m_jointype != JOIN_FULL)
        hasalternative = true;

    return hasalternative;
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
const bool JoinPathGenBase::is_param_path()
{
    /* oops, to ensure the path correct,Currently limited to  nestloop path */
    if (m_joinmethod != T_NestLoop)
        return false;

    /* oops, empty inner path */
    if (m_innerPath == NULL)
        return false;

    /* oops, not a param path */
    if (m_innerPath->param_info == NULL)
        return false;

    /* oops, the param path seems invalid */
    if (!bms_overlap(m_innerPath->param_info->ppi_req_outer, m_outerPath->parent->relids))
        return false;

    /* pass all check, we get a nestloop index with param path */
    return true;
}

/*
 * @Description: when subpath is replicate, we need to check if we can use redistribute.
 *
 * @return bool: true -- redistribution can be used.
 */
bool JoinPathGenBase::isReplicateJoinCanRedistribute()
{
    bool can_redistribute = true;

    /*
     * Followed cases can choose local plan:
     *   1.Outer is replicate and inner is hash: RHS join or probing side execute on CN, and build side need
     * redistribute; 2.Outer is hash and inner is replicate: LHS join or probing side execute on CN, and build side need
     * redistribute or is param path;
     */
    if (m_replicateOuter && !m_replicateInner) {
        if (RHS_join(m_saveJointype) && m_redistributeInner)
            can_redistribute = false;
    } else if (!m_replicateOuter && m_replicateInner) {
        if (LHS_join(m_saveJointype) && (m_redistributeOuter || is_param_path()))
            can_redistribute = false;
    } else {
        can_redistribute = false;
    }

    /* Need hash filter for replicate table, so delete this path. */
    if (can_redistribute) {
        m_streamInfoList = list_delete(m_streamInfoList, m_streamInfoPair);
        pfree_ext(m_streamInfoPair);
        m_streamInfoPair = NULL;
    }

    return can_redistribute;
}

/*
 * @Description: add join path to path list.
 *
 * @param[IN] path: join path.
 * @param[IN] desired_key: desired key that try to meet.
 * @param[IN] exact_match: if there's a desired key, whether we should do exact match.
 * @return void
 */
void JoinPathGenBase::addJoinPath(Path* path, List* desired_key = NIL, bool exact_match = false)
{
    JoinPath* joinpath = (JoinPath*)path;

    if (setJoinDistributeKeys(joinpath, desired_key, exact_match))
        addPath(path);
}

/*
 * @Description: create stream path for both join side.
 *
 * @return void
 */
void JoinPathGenBase::addJoinStreamPath()
{
    m_innerStreamPath = streamSidePath(false);
    m_outerStreamPath = streamSidePath(true);
}

/*
 * @Description: Add stream info base on join clauses and subpath distributions.
 *               This part is the basic part of join generation for MPP structure .
 *
 * @return void
 */
void JoinPathGenBase::addStreamMppInfo()
{
    List *stream_keys_inner = NIL, *stream_keys_outer = NIL;

    if (!m_redistributeInner && !m_redistributeOuter) {
        /*
         * if redistribute on different join key, still need to redistribute either one.
         */
        if (m_rrinfoInner && m_rrinfoOuter && !equal(m_rrinfoInner, m_rrinfoOuter)) {
            /* try redistribut inner. */
            stream_keys_inner = getOthersideKey(false);
            if (stream_keys_inner != NIL) {
                setStreamBaseInfo(STREAM_REDISTRIBUTE, STREAM_NONE, stream_keys_inner, NIL);
            }

            /* try redistribute outer. */
            stream_keys_outer = getOthersideKey(true);
            if (stream_keys_outer != NIL) {
                setStreamBaseInfo(STREAM_NONE, STREAM_REDISTRIBUTE, NIL, stream_keys_outer);
            }

            /* try broadcast */
            if (m_canBroadcastInner && stream_keys_inner == NIL)
                setStreamBaseInfo(STREAM_BROADCAST, STREAM_NONE, NIL, NIL);
            if (m_canBroadcastOuter && stream_keys_outer == NIL)
                setStreamBaseInfo(STREAM_NONE, STREAM_BROADCAST, NIL, NIL);
        } else {
            setStreamBaseInfo(STREAM_NONE, STREAM_NONE, NIL, NIL);
        }
    } else if (m_redistributeInner && !m_redistributeOuter) {
        stream_keys_inner = getOthersideKey(false);
        if (stream_keys_inner != NIL) {
            setStreamBaseInfo(STREAM_REDISTRIBUTE, STREAM_NONE, stream_keys_inner, NIL);
        }

        /* try broadcast */
        if (m_canBroadcastInner && stream_keys_inner == NIL)
            setStreamBaseInfo(STREAM_BROADCAST, STREAM_NONE, NIL, NIL);
        if (m_canBroadcastOuter)
            setStreamBaseInfo(STREAM_NONE, STREAM_BROADCAST, NIL, NIL);
    } else if (!m_redistributeInner && m_redistributeOuter) {
        stream_keys_outer = getOthersideKey(true);
        if (stream_keys_outer != NIL) {
            setStreamBaseInfo(STREAM_NONE, STREAM_REDISTRIBUTE, NIL, stream_keys_outer);
        }

        /* try broadcast */
        if (m_canBroadcastInner)
            setStreamBaseInfo(STREAM_BROADCAST, STREAM_NONE, NIL, NIL);
        if (m_canBroadcastOuter && stream_keys_outer == NIL)
            setStreamBaseInfo(STREAM_NONE, STREAM_BROADCAST, NIL, NIL);
    } else {
        int i = ((m_rel->rel_dis_keys.matching_keys != NIL) ? -1 : 0); /* loop start */
        int key_num = list_length(m_rel->rel_dis_keys.superset_keys);
        List* old_distribute_keys = NIL;
        List* desired_keys = NIL;
        bool choose_optimal = false;

        /*
         * For redistribute path, we check all the matching key and superset keys
         * to be distribute keys if possible. We check with the following sequence:
         * (1) matching key; (2) superset key; (3) optimal key. We use variable i
         * to track all process, with (1) i = -1; (2) i = 0 to key_num -1;
         * (3) i = key_num. During whole process, we skip if distribute key is already
         * used before. Also, if (3) is found in (1) and (2), we just skip (3).
         */
        for (; i <= key_num; i++) {
            stream_keys_inner = NIL, stream_keys_outer = NIL;
            m_multipleInner = 0.0, m_multipleOuter = 0.0;
            desired_keys = NIL;

            if (i == -1)
                desired_keys = m_rel->rel_dis_keys.matching_keys;
            else if (i < key_num)
                desired_keys = (List*)list_nth(m_rel->rel_dis_keys.superset_keys, i);

            if (i == key_num && choose_optimal)
                continue;

            /* Determine which clause both sides redistribute on */
            getDistributeKeys(&stream_keys_outer, &stream_keys_inner, desired_keys, (i == -1));

            if (stream_keys_inner != NIL && stream_keys_outer != NIL) {
                if (m_multipleOuter <= 1.0 && m_multipleInner <= 1.0)
                    choose_optimal = true;

                if (list_member(old_distribute_keys, stream_keys_outer))
                    continue;
                else
                    old_distribute_keys = lappend(old_distribute_keys, (void*)stream_keys_outer);

                setStreamBaseInfo(STREAM_REDISTRIBUTE, STREAM_REDISTRIBUTE, stream_keys_inner, stream_keys_outer);
            }
        }

        list_free(old_distribute_keys);
        /* try broadcast */
        if (m_canBroadcastInner)
            setStreamBaseInfo(STREAM_BROADCAST, STREAM_NONE, NIL, NIL);
        if (m_canBroadcastOuter)
            setStreamBaseInfo(STREAM_NONE, STREAM_BROADCAST, NIL, NIL);
    }
}

/*
 * @Description: Add stream info based on parallel degree.
 *
 * @return void
 */
void JoinPathGenBase::addStreamParallelInfo()
{
    List* tmp_list = m_streamInfoList;
    ListCell* lc = NULL;

    /* only try smp path when u_sess->opt_cxt.query_dop > 1 */
    if (u_sess->opt_cxt.query_dop <= 1)
        return;

    /* Try to add parallel info to spare stream info, only keep suitable stream info. */
    m_streamInfoList = NIL;
    foreach (lc, tmp_list) {
        m_streamInfoPair = (StreamInfoPair*)lfirst(lc);
        if (addJoinParallelInfo())
            m_streamInfoList = lappend(m_streamInfoList, (void*)m_streamInfoPair);
    }

    list_free(tmp_list);
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
void JoinPathGenBase::setStreamBaseInfo(
    StreamType inner_type, StreamType outer_type, List* inner_keys, List* outer_keys)
{
    StreamInfoPair* sinfopair = NULL;
    sinfopair = (StreamInfoPair*)palloc0(sizeof(StreamInfoPair));
    sinfopair->inner_info.type = inner_type;
    sinfopair->inner_info.subpath = m_innerPath;
    sinfopair->inner_info.stream_keys = inner_keys;
    sinfopair->inner_info.ssinfo = NULL;
    sinfopair->inner_info.multiple = (inner_type == STREAM_NONE) ? m_innerPath->multiple : m_multipleInner;

    sinfopair->outer_info.type = outer_type;
    sinfopair->outer_info.subpath = m_outerPath;
    sinfopair->outer_info.stream_keys = outer_keys;
    sinfopair->outer_info.ssinfo = NULL;
    sinfopair->outer_info.multiple = (outer_type == STREAM_NONE) ? m_innerPath->multiple : m_multipleOuter;

    sinfopair->skew_optimize = SKEW_RES_NONE;

    m_streamInfoPair = sinfopair;
    m_streamInfoList = lappend(m_streamInfoList, (void*)sinfopair);

    m_multipleInner = 1.0;
    m_multipleOuter = 1.0;
}

/*
 * @Description: set distribute keys for join path.
 *
 * @param[IN] joinpath: join path.
 * @param[IN] desired_key: desired key that try to meet.
 * @param[IN] exact_match: if there's a desired key, whether we should do exact match.
 * @return bool: if the path is valid.
 */
const bool JoinPathGenBase::setJoinDistributeKeys(JoinPath* joinpath, List* desired_key, bool exact_match)
{
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
void JoinPathGenBase::parallelLocalRedistribute(bool* inner_can_local_distribute, bool* outer_can_local_distribute,
    bool* inner_need_local_distribute, bool* outer_need_local_distribute)
{
    Path* inner_tmp = m_innerPath;
    Path* outer_tmp = m_outerPath;

    *inner_can_local_distribute =
        check_dsitribute_key_in_targetlist(m_root, m_distributeKeysInner, m_innerRel->reltarget->exprs);
    *outer_can_local_distribute =
        check_dsitribute_key_in_targetlist(m_root, m_distributeKeysOuter, m_outerRel->reltarget->exprs);

    if (IS_DUMMY_UNIQUE(inner_tmp))
        inner_tmp = ((UniquePath*)inner_tmp)->subpath;
    if (IS_DUMMY_UNIQUE(outer_tmp))
        outer_tmp = ((UniquePath*)outer_tmp)->subpath;

    /*
     * If we already have redistribute or local redistribute
     * in the subquery path, then there is no need to add
     * new local redistribute for parallelism.
     */
    if (inner_tmp->pathtype == T_SubqueryScan) {
        Plan* subplan = inner_tmp->parent->subplan;
        *inner_need_local_distribute = is_local_redistribute_needed(subplan);
    }

    if (outer_tmp->pathtype == T_SubqueryScan) {
        Plan* subplan = outer_tmp->parent->subplan;
        *outer_need_local_distribute = is_local_redistribute_needed(subplan);
    }
}

/*
 * @Description: create a unique path for unique join.
 *
 * @param[IN] stream_outer: if it is the outer side of join.
 * @param[OUT] pathkeys: path sort keys.
 * @return Path*: unique path.
 */
Path* JoinPathGenBase::makeJoinUniquePath(bool stream_outer, List* pathkeys)
{
    StreamInfo* sinfo = stream_outer ? &m_streamInfoPair->outer_info : &m_streamInfoPair->inner_info;
    double skew = sinfo->multiple;
    StreamType stream_type = sinfo->type;
    List* distribute_key = sinfo->stream_keys;
    ParallelDesc* smpDesc = &sinfo->smpDesc;
    Path* path = sinfo->subpath;

    /* Get the optimal method to make this join unuque path */
    SJoinUniqueMethod option = get_optimal_join_unique_path(
        m_root, path, stream_type, distribute_key, pathkeys, skew, m_targetDistribution, smpDesc);

    ereport(DEBUG1, (errmodule(MOD_OPT_JOIN), errmsg("[Join Unique] Best path method is No. %d.", option + 1)));

    /* Make this join unique path */
    Path* best_path = NULL;
    switch (option) {
        case REDISTRIBUTE_UNIQUE:
            best_path = get_redist_unique(
                m_root, path, stream_type, distribute_key, pathkeys, skew, m_targetDistribution, smpDesc);
            break;
        case UNIQUE_REDISTRIBUTE:
            best_path = get_unique_redist(
                m_root, path, stream_type, distribute_key, pathkeys, skew, m_targetDistribution, smpDesc);
            break;
        case UNIQUE_REDISTRIBUTE_UNIQUE:
            best_path = get_unique_redist_unique(
                m_root, path, stream_type, distribute_key, pathkeys, skew, m_targetDistribution, smpDesc);
            break;
        case REDISTRIBUTE_UNIQUE_REDISTRIBUTE_UNIQUE:
            best_path = get_redist_unique_redist_unique(
                m_root, path, stream_type, distribute_key, pathkeys, skew, m_targetDistribution, smpDesc);
            break;
        default:
            break;
    }

    if (best_path == NULL) {
        ereport(ERROR, (errmodule(MOD_OPT_JOIN), errmsg("[Join Unique] best_path should not be NULL")));
    }

    ereport(DEBUG1,
        (errmodule(MOD_OPT_JOIN),
            errmsg("[Join Unique] Finish building path, final startup cost : %lf, final total cost : %lf.",
                best_path->startup_cost,
                best_path->total_cost)));

    return best_path;
}

/*
 * @Description: create a new stream info pair from an old one.
 *
 * @param[IN] streamInfoPair: old stream info pair.
 * @return void
 */
void JoinPathGenBase::newStreamInfoPair(StreamInfoPair* streamInfoPair)
{
    StreamInfoPair* tmpStreamInfo = (StreamInfoPair*)palloc0(sizeof(StreamInfoPair));
    copy_stream_info_pair(tmpStreamInfo, streamInfoPair);
    m_streamInfoPair = tmpStreamInfo;
}

/*
 * @Description: create stream path for join.
 *
 * @param[IN] stream_outer: the stream is at the outer side of join.
 * @return Path*
 */
Path* JoinPathGenBase::streamSidePath(bool stream_outer)
{
    StreamInfo* sinfo = stream_outer ? &m_streamInfoPair->outer_info : &m_streamInfoPair->inner_info;
    StreamType stream_type = sinfo->type;
    List* distribute_key = sinfo->stream_keys;
    ParallelDesc* smpDesc = &sinfo->smpDesc;
    List* ssinfo = sinfo->ssinfo;

    Path* path = sinfo->subpath;
    double skew = sinfo->multiple;
    List* pathkeys = NIL;

    if (sinfo->type == STREAM_NONE)
        return path;

    /* choose pathkeys for stream */
    if (m_joinmethod == T_MergeJoin) {
        pathkeys = stream_outer ? m_outerPath->pathkeys : m_innerPath->pathkeys;
    } else if (m_joinmethod == T_NestLoop && m_pathkeys != NIL) {
        if (m_dop > 1)
            pathkeys = NIL;
        else
            pathkeys = stream_outer ? m_outerPath->pathkeys : m_innerPath->pathkeys;
    }

    if ((m_saveJointype == JOIN_UNIQUE_INNER && stream_outer == false) ||
        (m_saveJointype == JOIN_UNIQUE_OUTER && stream_outer)) {
#ifdef ENABLE_MULTIPLE_NODES
        if (u_sess->opt_cxt.skew_strategy_opt != SKEW_OPT_OFF) {
            return makeJoinSkewUniquePath(stream_outer, pathkeys);
        } else {
            return makeJoinUniquePath(stream_outer, pathkeys);
        }
#else
        return makeJoinUniquePath(stream_outer, pathkeys);
#endif
    } else {
        return create_stream_path(m_root,
            path->parent,
            stream_type,
            distribute_key,
            pathkeys,
            path,
            skew,
            m_targetDistribution,
            smpDesc,
            ssinfo);
    }
}

/*
 * @Description: choose suitable parallel stream(like local stream)
 *               for parallel plan.
 *
 * @return void
 */
bool JoinPathGenBase::addJoinParallelInfo()
{
    return false;
}

/*
 * @Description: set parallel info include consumer/producer dop and
 *               parallel stream type.
 *
 * @param[IN] stream_outer: is outer side of join.
 * @param[IN] sstype: smp stream type.
 * @return void
 */
void JoinPathGenBase::setStreamParallelInfo(bool stream_outer, SmpStreamType sstype)
{
}

/*
 * @Description: create a unique path for unique join with
 *               skewness at the no-unique side.
 *
 * @param[IN] stream_outer: if it is the outer side of join.
 * @param[OUT] pathkeys: path sort keys.
 * @return Path*: unique path.
 */
Path* JoinPathGenBase::makeJoinSkewUniquePath(bool stream_outer, List* pathkeys)
{
    return NULL;
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
    JoinPathExtraData* extra, Path* outer_path, Path* inner_path, List* restrictlist,
    Relids required_outer, List* hashclauses)
    : JoinPathGen(root, joinrel, jointype, save_jointype, extra, hashclauses, restrictlist, outer_path,
          inner_path, required_outer),
      m_hashClauses(hashclauses)
{
    m_joinmethod = T_HashJoin;
}

/*
 * @Description: release memory at deconstruct stage.
 */
HashJoinPathGen::~HashJoinPathGen()
{
    m_hashClauses = NULL;
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
    m_dop = dop;
    m_workspace = workspace;
    m_targetDistribution = targetDistribution;

    /* Choose suitable stream for join. */
    addJoinStreamInfo();

    /* Generate hash join paths and add them to path list. */
    addHashjoinPathToList();

    /* Reset for next time usage. */
    reset();
}

/*
 * @Description: create stream path for join, then create hash join path
 *               and add it to path list.
 *
 * return void
 */
void HashJoinPathGen::addHashjoinPathToList()
{
    ListCell* lc = NULL;
    Path* joinpath = NULL;

    if (m_streamInfoList == NIL)
        return;

    foreach (lc, m_streamInfoList) {
        m_streamInfoPair = (StreamInfoPair*)lfirst(lc);
        addJoinStreamPath();
        joinpath = createHashJoinPath();
        addJoinPath(joinpath);
    }
}

/*
 * @Description: create hash join path.
 *
 * return Path*:
 */
Path* HashJoinPathGen::createHashJoinPath()
{
    HashPath* pathnode = makeNode(HashPath);
    bool try_eq_related_indirectly = false;

    initialCostHashjoin();

    pathnode->jpath.path.pathtype = T_HashJoin;
    pathnode->jpath.path.parent = m_rel;
    pathnode->jpath.path.pathtarget = m_rel->reltarget;
    pathnode->jpath.path.param_info = get_joinrel_parampathinfo(
        m_root, m_rel, m_outerStreamPath, m_innerStreamPath, m_extra->sjinfo, m_requiredOuter, &m_joinRestrictinfo);

    /*
     * A hashjoin never has pathkeys, since its output ordering is
     * unpredictable due to possible batching.      XXX If the inner relation is
     * small enough, we could instruct the executor that it must not batch,
     * and then we could assume that the output inherits the outer relation's
     * ordering, which might save a sort step.      However there is considerable
     * downside if our estimate of the inner relation size is badly off. For
     * the moment we don't risk it.  (Note also that if we wanted to take this
     * seriously, joinpath.c would have to consider many more paths for the
     * outer rel than it does now.)
     */
    pathnode->jpath.path.pathkeys = NIL;
    pathnode->jpath.path.dop = m_dop;
    pathnode->jpath.jointype = m_jointype;
    pathnode->jpath.inner_unique = m_extra->inner_unique;
    pathnode->jpath.outerjoinpath = m_outerStreamPath;
    pathnode->jpath.innerjoinpath = m_innerStreamPath;
    pathnode->jpath.joinrestrictinfo = m_joinRestrictinfo;
    pathnode->jpath.skewoptimize = m_streamInfoPair->skew_optimize;
    pathnode->path_hashclauses = m_hashClauses;

    pathnode->jpath.path.exec_type = SetExectypeForJoinPath(m_innerStreamPath, m_outerStreamPath);

#ifdef STREAMPLAN
    pathnode->jpath.path.locator_type =
        locator_type_join(m_innerStreamPath->locator_type, m_outerStreamPath->locator_type);
    ProcessRangeListJoinType(&pathnode->jpath.path, m_outerStreamPath, m_innerStreamPath);
#ifdef ENABLE_MULTIPLE_NODES
    if (IS_STREAM_PLAN) {
        /* add location information for hash join path */
        Distribution* distribution = ng_get_join_distribution(m_outerStreamPath, m_innerStreamPath);
        ng_copy_distribution(&pathnode->jpath.path.distribution, distribution);
    }
#endif
#endif

    /* final_cost_hashjoin will fill in pathnode->num_batches */
    finalCostHashjoin(pathnode, checkJoinMethodAlternative(&try_eq_related_indirectly));

    return (Path*)pathnode;
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
    initial_cost_hashjoin(m_root,
        m_workspace,
        m_jointype,
        m_hashClauses,
        m_outerStreamPath,
        m_innerStreamPath,
        m_extra,
        m_dop);
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
    final_cost_hashjoin(m_root, path, m_workspace, m_extra, hasalternative, path->jpath.path.dop);
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
    JoinPathExtraData* extra, Path* outer_path, Path* inner_path, List* restrictlist,
    List* pathkeys, Relids required_outer)
    : JoinPathGen(root, joinrel, jointype, save_jointype, extra, restrictlist, restrictlist, outer_path,
          inner_path, required_outer)
{
    m_joinmethod = T_NestLoop;
    m_pathkeys = pathkeys;
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
    m_dop = dop;
    m_workspace = workspace;
    m_targetDistribution = targetDistribution;

    /* Choose suitable stream for join. */
    addJoinStreamInfo();

    /* Generate nestloop join paths and add them to path list. */
    addNestloopPathToList();

    /* Reset for next time usage. */
    reset();
}

/*
 * @Description: create stream path for join, then create nestloop join path
 *               and add it to path list.
 *
 * return void
 */
void NestLoopPathGen::addNestloopPathToList()
{
    ListCell* lc = NULL;
    Path* joinpath = NULL;

    if (m_streamInfoList == NIL)
        return;

    foreach (lc, m_streamInfoList) {
        m_streamInfoPair = (StreamInfoPair*)lfirst(lc);
        addJoinStreamPath();
        joinpath = createNestloopPath();
        addJoinPath(joinpath);
    }
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
    initial_cost_nestloop(
        m_root, m_workspace, m_jointype, m_outerStreamPath, m_innerStreamPath, m_extra, m_dop);
}

/*
 * @Description: Final estimate of the cost and result size of a hashjoin path.
 *
 * return void
 */
void NestLoopPathGen::finalCostNestloop(NestPath* path, bool hasalternative)
{
    final_cost_nestloop(m_root, path, m_workspace, m_extra, hasalternative, m_dop);
}

/*
 * @Description: Creates a pathnode corresponding to a nestloop join between two
 *	             relations.
 *
 * return void
 */
Path* NestLoopPathGen::createNestloopPath()
{
    NestPath* pathnode = makeNode(NestPath);
    Relids inner_req_outer = PATH_REQ_OUTER(m_innerStreamPath);
    bool try_eq_related_indirectly = false;
    bool hasalternative = checkJoinMethodAlternative(&try_eq_related_indirectly);

    initialCostNestloop();

    if (m_outerRel != NULL && m_innerRel != NULL && m_root != NULL && !hasalternative && try_eq_related_indirectly &&
        !u_sess->attr.attr_sql.enable_nestloop)
        hasalternative = equivalence_class_overlap(m_root, m_outerRel->relids, m_innerRel->relids);
    if (m_outerRel != NULL && m_innerRel != NULL && !hasalternative && log_min_messages <= DEBUG3) {
        StringInfoData buf;
        debug3_print_two_relids(m_outerRel->relids, m_innerRel->relids, m_root, &buf);
        ereport(
            DEBUG3, (errmodule(MOD_OPT_JOIN), "[OPTHashjoin]Print Outer relids and Inner relids:\n\n%s\n", buf.data));
        pfree_ext(buf.data);
        ListCell* l = NULL;
        foreach (l, m_joinClauses) {
            RestrictInfo* restrictinfo = (RestrictInfo*)lfirst(l);
            StringInfoData buf2;
            debug3_print_two_relids(restrictinfo->left_relids, restrictinfo->right_relids, m_root, &buf2);
            ereport(
                DEBUG3, (errmodule(MOD_OPT_JOIN), "[OPTHashjoin]Print clause left and right side:\n\n%s\n", buf2.data));
            pfree_ext(buf2.data);
        }
    }
    /*
     * If the inner path is parameterized by the outer, we must drop any
     * restrict_clauses that are due to be moved into the inner path.  We have
     * to do this now, rather than postpone the work till createplan time,
     * because the restrict_clauses list can affect the size and cost
     * estimates for this path.
     */
    if (m_outerRel != NULL && m_innerRel != NULL && bms_overlap(inner_req_outer, m_outerRel->relids)) {
        Relids inner_and_outer = bms_union(m_innerRel->relids, inner_req_outer);
        List* jclauses = NIL;
        ListCell* lc = NULL;

        foreach (lc, m_joinClauses) {
            RestrictInfo* rinfo = (RestrictInfo*)lfirst(lc);

            if (!join_clause_is_movable_into(rinfo, m_innerRel->relids, inner_and_outer))
                jclauses = lappend(jclauses, (void*)rinfo);
        }
        m_joinClauses = jclauses;
    }

    pathnode->path.pathtype = T_NestLoop;
    pathnode->path.parent = m_rel;
    pathnode->path.pathtarget = m_rel->reltarget;
    if (m_root != NULL) {
        pathnode->path.param_info = get_joinrel_parampathinfo(
            m_root, m_rel, m_outerStreamPath, m_innerStreamPath, m_extra->sjinfo, m_requiredOuter, &m_joinClauses);
    }
    pathnode->path.pathkeys = m_pathkeys;
    if (IsA(m_outerStreamPath, StreamPath) && NIL == m_outerStreamPath->pathkeys) {
        pathnode->path.pathkeys = NIL;
    }
    pathnode->path.dop = m_dop;
    pathnode->jointype = m_jointype;
    pathnode->inner_unique = m_extra->inner_unique;
    pathnode->outerjoinpath = m_outerStreamPath;
    pathnode->innerjoinpath = m_innerStreamPath;
    pathnode->joinrestrictinfo = m_joinClauses;
    pathnode->skewoptimize = m_streamInfoPair->skew_optimize;

    pathnode->path.exec_type = SetExectypeForJoinPath(m_innerStreamPath, m_outerStreamPath);

#ifdef STREAMPLAN
    pathnode->path.locator_type = locator_type_join(m_outerStreamPath->locator_type, m_innerStreamPath->locator_type);
    ProcessRangeListJoinType(&pathnode->path, m_outerStreamPath, m_innerStreamPath);
#ifdef ENABLE_MULTIPLE_NODES
    if (IS_STREAM_PLAN) {
        /* add location information for nest loop join path */
        Distribution* distribution = ng_get_join_distribution(m_outerStreamPath, m_innerStreamPath);
        ng_copy_distribution(&pathnode->path.distribution, distribution);
    }
#endif
#endif

    finalCostNestloop(pathnode, hasalternative);

    return (Path*)pathnode;
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
    JoinPathExtraData* extra, Path* outer_path, Path* inner_path,
    List* restrict_clauses, List* pathkeys, Relids required_outer, List* mergeclauses, List* outersortkeys,
    List* innersortkeys)
    : JoinPathGen(root, joinrel, jointype, save_jointype, extra, restrict_clauses, restrict_clauses,
          outer_path, inner_path, required_outer)
{
    m_joinmethod = T_MergeJoin;
    m_pathkeys = pathkeys;
    m_mergeClauses = mergeclauses;
    m_innerSortKeys = innersortkeys;
    m_outerSortKeys = outersortkeys;
}

/*
 * @Description: release memory at deconstruct stage.
 */
MergeJoinPathGen::~MergeJoinPathGen()
{
    m_innerSortKeys = NIL;
    m_mergeClauses = NIL;
    m_outerSortKeys = NIL;
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
    m_dop = dop;
    m_workspace = workspace;
    m_targetDistribution = targetDistribution;

    /* Choose suitable stream for join. */
    addJoinStreamInfo();

    /* Generate merge join paths and add them to path list. */
    addMergejoinPathToList();

    /* Reset for next time usage. */
    reset();
}

/*
 * @Description: create stream path for join, then create merge join path
 *               and add it to path list.
 *
 * return void
 */
void MergeJoinPathGen::addMergejoinPathToList()
{
    ListCell* lc = NULL;
    Path* joinpath = NULL;

    if (m_streamInfoList == NIL)
        return;

    foreach (lc, m_streamInfoList) {
        m_streamInfoPair = (StreamInfoPair*)lfirst(lc);
        addJoinStreamPath();
        joinpath = createMergejoinPath();
        addJoinPath(joinpath);
    }
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
    initial_cost_mergejoin(m_root,
        m_workspace,
        m_jointype,
        m_mergeClauses,
        m_outerStreamPath,
        m_innerStreamPath,
        m_outerSortKeys,
        m_innerSortKeys,
        m_extra);
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
    final_cost_mergejoin(m_root, path, m_workspace, m_extra, hasalternative);
}

/*
 * @Description: Creates a pathnode corresponding to a mergejoin join between
 *	             two relations.
 *
 * return Path*: mergejoin path.
 */
Path* MergeJoinPathGen::createMergejoinPath()
{
    MergePath* pathnode = makeNode(MergePath);
    bool try_eq_related_indirectly = false;
    bool hasalternative = checkJoinMethodAlternative(&try_eq_related_indirectly);

    initialCostMergejoin();

    pathnode->jpath.path.pathtype = T_MergeJoin;
    pathnode->jpath.path.parent = m_rel;
    pathnode->jpath.path.pathtarget = m_rel->reltarget;
    pathnode->jpath.path.param_info = get_joinrel_parampathinfo(
        m_root, m_rel, m_outerStreamPath, m_innerStreamPath, m_extra->sjinfo, m_requiredOuter, &m_joinRestrictinfo);
    pathnode->jpath.path.pathkeys = m_pathkeys;
    pathnode->jpath.jointype = m_jointype;
    pathnode->jpath.inner_unique = m_extra->inner_unique;
    pathnode->jpath.outerjoinpath = m_outerStreamPath;
    pathnode->jpath.innerjoinpath = m_innerStreamPath;
    pathnode->jpath.joinrestrictinfo = m_joinRestrictinfo;
    pathnode->jpath.skewoptimize = m_streamInfoPair->skew_optimize;
    pathnode->path_mergeclauses = m_mergeClauses;
    pathnode->outersortkeys = m_outerSortKeys;
    pathnode->innersortkeys = m_innerSortKeys;
    /* pathnode->materialize_inner will be set by final_cost_mergejoin */

    pathnode->jpath.path.exec_type = SetExectypeForJoinPath(m_innerStreamPath, m_outerStreamPath);

#ifdef STREAMPLAN
    pathnode->jpath.path.locator_type =
        locator_type_join(m_outerStreamPath->locator_type, m_innerStreamPath->locator_type);
    ProcessRangeListJoinType(&pathnode->jpath.path, m_outerStreamPath, m_innerStreamPath);
#ifdef ENABLE_MULTIPLE_NODES
    if (IS_STREAM_PLAN) {
        /* add location information for merge join path */
        Distribution* distribution = ng_get_join_distribution(m_outerStreamPath, m_innerStreamPath);
        ng_copy_distribution(&pathnode->jpath.path.distribution, distribution);
    }
#endif
#endif

    finalCostMergejoin(pathnode, hasalternative);

    return (Path*)pathnode;
}

