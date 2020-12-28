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
 * @Description: create stream path for join.
 *
 * @param[IN] stream_outer: the stream is at the outer side of join.
 * @return Path*
 */
Path* JoinPathGen::streamSidePath(bool stream_outer)
{
    StreamInfo* sinfo = stream_outer ? &m_streamInfoPair->outer_info : &m_streamInfoPair->inner_info;
    StreamType stream_type = sinfo->type;
    List* distribute_key = sinfo->stream_keys;
    ParallelDesc* smpDesc = &sinfo->smpDesc;
    List* ssinfo = sinfo->ssinfo;

    Path* path = sinfo->subpath;
    List* pathkeys = NIL;

    if (STREAM_NONE == sinfo->type)
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

    if ((JOIN_UNIQUE_INNER == m_saveJointype && stream_outer == false) ||
        (JOIN_UNIQUE_OUTER == m_saveJointype && stream_outer)) {
        return makeJoinUniquePath(stream_outer, pathkeys);
    } else {
        return create_stream_path(m_root,
            path->parent,
            stream_type,
            distribute_key,
            pathkeys,
            path,
            sinfo->multiple,
            m_targetDistribution,
            smpDesc,
            ssinfo);
    }

    return NULL;
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

    /* Add stream info base on join clauses and subpath distributions. */
    addStreamMppInfo();

    /* Add stream info base on parallel degree. */
    addStreamParallelInfo();
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
 * @Description: Add stream info based on data skew info when we use redistribute.
 *
 * @return void
 */
void JoinPathGen::addStreamSkewInfo()
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
 * @Description: confirm if we can find distribute keys at subpath's targetlist.
 *
 * @return void
 */
void JoinPathGen::confirmDistributeKeys(StreamInfo* sinfo)
{
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
}

