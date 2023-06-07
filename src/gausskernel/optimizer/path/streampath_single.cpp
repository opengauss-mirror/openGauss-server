/* -------------------------------------------------------------------------
 *
 * streampath_single.cpp
 *	  functions for stream path generation in local node.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/gausskernel/optimizer/path/streampath_single.cpp
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
    JoinPathExtraData* extra, List* joinclauses, List* restrictinfo,
    Path* outer_path, Path* inner_path, Relids required_outer)
    : JoinPathGenBase(root, joinrel, jointype, save_jointype, extra, joinclauses,
                      restrictinfo, outer_path, inner_path, required_outer)
{}

/*
 * @Description: decontructor function for join path generation.
 */
JoinPathGen::~JoinPathGen()
{}

/*
 * @Description: decide whether we need redistribute inner or outer side
 *               base on subpath distribute keys, join clauses and target
 *               node group.
 *
 * @return void
 */
void JoinPathGen::initDistribution()
{
    /*
     * Wheather inner or outer need to be redistributed base on their distribute key and join clauses
     *     TRUE means need to be redistributed,
     *     FALSE means do not need to be redistributed
     */
    m_redistributeInner = is_distribute_need_on_joinclauses(
        m_root, m_innerPath->distribute_keys, m_joinClauses, m_innerRel, m_outerRel, &m_rrinfoInner);

    /*
     * Wheather inner or outer need to be redistributed base on their distribute key and join clauses
     *     TRUE means need to be redistributed,
     *     FALSE means do not need to be redistributed
     */
    m_redistributeOuter = is_distribute_need_on_joinclauses(
        m_root, m_outerPath->distribute_keys, m_joinClauses, m_outerRel, m_innerRel, &m_rrinfoOuter);

    initRangeListDistribution();
}

/*
 * @Description: choose suitable parallel stream(like local stream)
 *               for parallel plan.
 *
 * @return void
 */
bool JoinPathGen::addJoinParallelInfo()
{
    StreamType inner_stream = m_streamInfoPair->inner_info.type;
    StreamType outer_stream = m_streamInfoPair->outer_info.type;

    bool is_subplan_parallel = (m_innerPath->dop > 1) || (m_outerPath->dop > 1);
    bool inner_can_local_distribute = true;
    bool outer_can_local_distribute = true;
    bool inner_need_local_distribute = true;
    bool outer_need_local_distribute = true;

    /* No need to add parallel info, keep origin path. */
    if (!is_subplan_parallel && m_dop <= 1) {
        return true;
    }

    /* check if we can use local redistribute and if we need local redistribute. */
    parallelLocalRedistribute(&inner_can_local_distribute,
        &outer_can_local_distribute,
        &inner_need_local_distribute,
        &outer_need_local_distribute);

    /* Set unparallel stream info. */
    if (is_subplan_parallel && m_dop <= 1) {
        setStreamParallelInfo(false);
        setStreamParallelInfo(true);
    } else {
        if (inner_stream == STREAM_BROADCAST || outer_stream == STREAM_BROADCAST) {
                setStreamParallelInfo(false);
                setStreamParallelInfo(true);
        } else if (inner_stream == STREAM_REDISTRIBUTE && outer_stream == STREAM_NONE) {
            if (outer_can_local_distribute) {
                if (outer_need_local_distribute)
                    setStreamParallelInfo(true, LOCAL_DISTRIBUTE);
                else
                    setStreamParallelInfo(true);
                setStreamParallelInfo(false);
            } else {
                return false;
            }
        } else if (inner_stream == STREAM_NONE && outer_stream == STREAM_REDISTRIBUTE) {
            if (inner_can_local_distribute) {
                if (inner_need_local_distribute)
                    setStreamParallelInfo(false, LOCAL_DISTRIBUTE);
                else
                    setStreamParallelInfo(false);
                setStreamParallelInfo(true);
            } else {
                return false;
            }
        } else if (inner_stream == STREAM_REDISTRIBUTE && outer_stream == STREAM_REDISTRIBUTE) {
            setStreamParallelInfo(false);
            setStreamParallelInfo(true);
        } else if (inner_stream == STREAM_NONE && outer_stream == STREAM_NONE) {
            /* New stream info is create for this situation. */
            StreamInfoPair* streamInfoPair = m_streamInfoPair;

            /* case 1: local broadcast inner */
            if (can_broadcast_inner(m_jointype, m_saveJointype, false, NIL, NIL)) {
                newStreamInfoPair(streamInfoPair);
                setStreamParallelInfo(false, LOCAL_BROADCAST);
                setStreamParallelInfo(true);
                m_streamInfoList = lappend(m_streamInfoList, (void*)m_streamInfoPair);
            }

            /* case 2: local broadcast outer */
            if (can_broadcast_outer(m_jointype, m_saveJointype, false, NIL, NIL)) {
                newStreamInfoPair(streamInfoPair);
                setStreamParallelInfo(false);
                setStreamParallelInfo(true, LOCAL_BROADCAST);
                m_streamInfoList = lappend(m_streamInfoList, m_streamInfoPair);
            }

            /* case 3: local redistribute inner and outer */
            if (inner_can_local_distribute && outer_can_local_distribute) {
                newStreamInfoPair(streamInfoPair);
                if (inner_need_local_distribute)
                    setStreamParallelInfo(false, LOCAL_DISTRIBUTE);
                else
                    setStreamParallelInfo(false);

                if (outer_need_local_distribute)
                    setStreamParallelInfo(true, LOCAL_DISTRIBUTE);
                else
                    setStreamParallelInfo(true);
                m_streamInfoList = lappend(m_streamInfoList, (void*)m_streamInfoPair);
            }

            pfree_ext(streamInfoPair);
            return false;
        }
    }

    return true;
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
    if (!isParallelEnable() && m_dop > 1)
        return;

    initDistribution();

    if (m_dop > 1) {
        /* Add stream info base on join clauses and subpath distributions. */
        addStreamMppInfo();
    } else {
        setStreamBaseInfo(STREAM_NONE, STREAM_NONE, NIL, NIL);
    }

    /* Add stream info base on parallel degree. */
    addStreamParallelInfo();
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
    StreamInfo* sinfo = stream_outer ? &m_streamInfoPair->outer_info : &m_streamInfoPair->inner_info;
    Path* subpath = stream_outer ? m_outerPath : m_innerPath;
    StreamType stype = sinfo->type;

    ParallelDesc* pdesc = &sinfo->smpDesc;
    pdesc->distriType = PARALLEL_NONE;
    pdesc->consumerDop = m_dop;
    pdesc->producerDop = SET_DOP(subpath->dop);

    switch (stype) {
        case STREAM_BROADCAST: {
            pdesc->distriType = LOCAL_BROADCAST;
            break;
        }
        case STREAM_REDISTRIBUTE: {
            pdesc->distriType = LOCAL_DISTRIBUTE;
            break;
        }
        case STREAM_NONE: {
            if (sstype == PARALLEL_NONE) {
                if (pdesc->consumerDop != pdesc->producerDop) {
                    pdesc->distriType = LOCAL_ROUNDROBIN;
                    sinfo->type = STREAM_REDISTRIBUTE;
                }
            } else {
                pdesc->distriType = sstype;
                sinfo->type = STREAM_REDISTRIBUTE;
            }

            sinfo->stream_keys = stream_outer ? m_distributeKeysOuter : m_distributeKeysInner;
            break;
        }
        default:
            break;
    }
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
    Path* inner_path = joinpath->innerjoinpath;
    Path* outer_path = joinpath->outerjoinpath;

    bool is_roundrobin_inner = (inner_path->locator_type == LOCATOR_TYPE_RROBIN);
    bool is_roundrobin_outer = (outer_path->locator_type == LOCATOR_TYPE_RROBIN);

    bool is_replicate_inner = is_replicated_path(inner_path);
    bool is_replicate_outer = is_replicated_path(outer_path);

    bool is_path_valid = true;

    if (is_roundrobin_inner || is_roundrobin_outer) {
        joinpath->path.distribute_keys = NIL;
    } else if (is_replicate_outer && is_replicate_inner) {
        joinpath->path.distribute_keys = NIL;
    } else if (is_replicate_outer || is_replicate_inner) {
        if (is_replicate_outer) {
            joinpath->path.distribute_keys = inner_path->distribute_keys;
            joinpath->path.rangelistOid = inner_path->rangelistOid;
        } else {
            joinpath->path.distribute_keys = outer_path->distribute_keys;
            joinpath->path.rangelistOid = outer_path->rangelistOid;
        }
    } else if (joinpath->path.dop > 1) {
        if (m_jointype != JOIN_FULL) {
            joinpath->path.distribute_keys = locate_distribute_key(
                m_jointype, outer_path->distribute_keys, inner_path->distribute_keys, desired_key, exact_match);
            if (!joinpath->path.distribute_keys)
                is_path_valid = false;
        }
    }

    return is_path_valid;
}

