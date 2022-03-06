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
 * parctl.cpp
 *   functions for parallel control
 *
 * IDENTIFICATION
 *    src/gausskernel/cbb/workload/parctl.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/xact.h"
#include "catalog/pg_authid.h"
#include "tcop/tcopprot.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "pgxc/pgxc.h"
#include "utils/atomic.h"
#include "utils/lsyscache.h"
#include "utils/memprot.h"
#include "utils/syscache.h"
#include "utils/elog.h"
#include "workload/workload.h"

#define RESOURCE_POOL_HASH_SIZE 32

extern THR_LOCAL int reserved_in_central_waiting;

/*
 * **************** STATIC FUNCTIONS ************************
 */
/*
 * function name: IsQueuedSubquery
 * description  : Whether control the query or not.
 * return value : bool
 *
 */
bool IsQueuedSubquery(void)
{
    return (u_sess->attr.attr_resource.enable_transaction_parctl ||
            !(IsTransactionBlock() || t_thrd.wlm_cxt.parctl_state.subquery));
}

/*
 * function name: WLMSearchAndCheckMaxNode
 * description  : Search the node with the max priority from workload list.
 * return value :
 *            NULL:         not find
 *            ListCell*:    the node to use
 *
 */
ListCell* WLMSearchAndCheckMaxNode(const List* list)
{
    return list_head(list);
}

/*
 * function name: WLMSearchQNode
 * description  : Search the qnode with the thread id of query.
 * return value :
 *            NULL:         not find
 *            ListCell*:    the qnode to use
 *
 */
WLMQNodeInfo* WLMSearchQNode(ParctlManager* parctl, uint64 sess_id)
{
    ListCell* cell = NULL;

    /* search priority list */
    foreach (cell, parctl->statements_waiting_list) {
        ListCell* qcell = NULL;

        WLMListNode* node = (WLMListNode*)lfirst(cell);

        /* search request list */
        foreach (qcell, node->request_list) {
            WLMQNodeInfo* qnode = (WLMQNodeInfo*)lfirst(qcell);

            if (qnode->sessid == sess_id) {
                return qnode;
            }
        }
    }

    return NULL;
}

/*
 * function name: WLMDeleteRequestListNode
 * description  : remove a node from statements waiting list.
 * return value : bool
 */
bool WLMDeleteRequestListNode(ListCell* lcnode, WLMQNodeInfo* datum)
{
    ParctlManager* parctl = &(t_thrd.wlm_cxt.thread_node_group->parctl);
    WLMListNode* node = (WLMListNode*)lfirst(lcnode);

    if (datum == NULL) {
        datum = (WLMQNodeInfo*)linitial(node->request_list);
    }

    node->request_list = list_delete_ptr(node->request_list, datum);

    /* update the removed flag */
    if (datum != NULL) {
        datum->removed = true;
    }

    /* delete node from the priority list */
    if (list_length(node->request_list) == 0) {
        parctl->statements_waiting_list = list_delete_ptr(parctl->statements_waiting_list, node);
        pfree(node);
        return true;
    }

    return false;
}


static int CheckStatEnqueueConditionGlobal(ParctlManager* parctl)
{
    if (parctl->max_active_statements <= 0) {
        return 0;
    }

    /*
     * If the statement will do parallel control, we must check
     * current total memory that active statements used whether is out of
     * 80% of max process memory, or current running statements is
     * out of max active statements.
     */
    if (t_thrd.wlm_cxt.parctl_state.enqueue && t_thrd.wlm_cxt.parctl_state.global_reserve == 0) {
        if (parctl->max_statements > 0 && parctl->statements_waiting_count >= parctl->max_statements) {
            return -1;
        }

        if (u_sess->wlm_cxt->query_count_record == false && parctl->max_support_statements > 0 &&
            parctl->current_support_statements >= parctl->max_support_statements) {
            return 1;
        }

        if (parctl->statements_runtime_count >= parctl->max_active_statements) {
            /*
             * If we have active statements waiting in the resource pool,
             * we will check whether we can make a new statement run.
             */
            if (parctl->statements_runtime_count >= parctl->respool_waiting_count &&
                (parctl->statements_runtime_count - parctl->respool_waiting_count) <
                    parctl->max_active_statements) {
                return 0;
            } else {
                return 1;
            }
        }
    }

    return 0;
}


static int CheckStatEnqueueConditionRespool()
{
    if (t_thrd.wlm_cxt.parctl_state.rp_reserve == 0) {
        if (t_thrd.wlm_cxt.parctl_state.simple) {
            if (t_thrd.wlm_cxt.qnode.rp->running_count_simple >=
                u_sess->wlm_cxt->wlm_params.rpdata.max_stmt_simple) {
                return 1;
            }
        } else {
            if ((t_thrd.wlm_cxt.qnode.rp->active_points + u_sess->wlm_cxt->wlm_params.rpdata.act_pts) >
                u_sess->wlm_cxt->wlm_params.rpdata.max_pts) {
                return 1;
            }
        }
    }

    return 0;    
}



/*
 * function name: CheckStatementsEnqueueCondition
 * description  : check the statements whether need enqueue.
 * return value : 1: yes
 *                0: no
 *               -1: error
 */
int CheckStatementsEnqueueCondition(ParctlManager* parctl, ParctlType qtype)
{
    int ret = 0;
    switch (qtype) {
        case PARCTL_GLOBAL: {
            ret = CheckStatEnqueueConditionGlobal(parctl);

            break;
        }
        case PARCTL_RESPOOL: {
            ret = CheckStatEnqueueConditionRespool();
            break;
        }
        default:
            break;
    }

    return ret;
}

/*
 * function name: WLMEnqueueCleanHandler
 * description  : a clean handler for global waiting list.
 * return value : void
 */
void WLMEnqueueCleanHandler(void* ptr)
{
    if (ptr == NULL) {
        return;
    }

    WLMQNodeInfo* qnode = (WLMQNodeInfo*)ptr;

    ParctlManager* parctl = &(t_thrd.wlm_cxt.thread_node_group->parctl);

    Assert(qnode != NULL);

    if (qnode->rp) {
        WLMContextLock rp_lock(&qnode->rp->mutex);

        /*
         * If the query is waiting in the resource pool list,
         * remove it from the list while exception.
         */
        if (!rp_lock.IsOwner()) {
            rp_lock.Lock();
        }

        if (!qnode->removed) {
            if (t_thrd.wlm_cxt.parctl_state.simple) {
                qnode->rp->waiters_simple = list_delete_ptr(qnode->rp->waiters_simple, qnode);
                qnode->removed = true;
            } else {
                qnode->rp->waiters = list_delete_ptr(qnode->rp->waiters, qnode);
                qnode->removed = true;

                /*
                 * only release ref_count because
                 * we have not became active statement yet.
                 */
                if (qnode->rp->ref_count > 0) {
                    (void)gs_atomic_add_32(&qnode->rp->ref_count, -1);
                }
            }
            if (t_thrd.wlm_cxt.parctl_state.global_reserve) {
                USE_CONTEXT_LOCK(&parctl->statements_list_mutex);

                if (parctl->respool_waiting_count > 0) {
                    parctl->respool_waiting_count--;
                    if (u_sess != NULL) {
                        u_sess->wlm_cxt->reserved_in_respool_waiting--;
                    }
                }
            }
        }
        rp_lock.UnLock();

        t_thrd.wlm_cxt.parctl_state.respool_waiting = 0;
        if (!t_thrd.proc_cxt.proc_exit_inprogress) {
            pgstat_report_waiting_on_resource(STATE_NO_ENQUEUE);
        }
    } else if (qnode->lcnode) {
        /*
         * the query is waiting in the global waiting list, remove it from
         * the global waiting list while exception.
         */
        if (!qnode->removed) {
            (void)WLMDeleteRequestListNode(qnode->lcnode, qnode);
            qnode->removed = true;
        }

        parctl->statements_waiting_count--;

        t_thrd.wlm_cxt.parctl_state.global_waiting = 0;
        if (!t_thrd.proc_cxt.proc_exit_inprogress) {
            pgstat_report_waiting_on_resource(STATE_NO_ENQUEUE);
        }
    }

    if (!t_thrd.proc_cxt.proc_exit_inprogress) {
        WLMResetStatInfo4Exception();
    }

    t_thrd.wlm_cxt.parctl_state.except = 0;
}

/*
 * function name: WLMGlobalRequestListThreadWakeUp
 * description  : Weak a statement up which is waiting in the list.
 * return value : void
 */
void WLMGlobalRequestListThreadWakeUp(ParctlManager* parctl)
{
    ListCell* lcnode = WLMSearchAndCheckMaxNode(parctl->statements_waiting_list);
    WLMListNode* node = NULL;

    if (lcnode == NULL) {
        return;
    }

    node = (WLMListNode*)lfirst(lcnode);

    /* check request list is valid */
    if (list_length(node->request_list)) {
        WLMQNodeInfo* qnode = (WLMQNodeInfo*)linitial(node->request_list);

        /* wake up the qnode to execute statement */
        if (qnode != NULL) {
            (void)pthread_cond_signal(&qnode->condition);

            ereport(DEBUG3,
                (errmsg("WLMRequestListThreadWakeUp node->data: %d, "
                        "request_list length: %d, runtime count: %d, "
                        "max_statements: %d.",
                    node->data,
                    list_length(node->request_list),
                    parctl->statements_runtime_count,
                    parctl->max_active_statements)));
        }
    }
}

/*
 * function name: WLMGroupSimpleThreadWakeUp
 * description  : Weak a simple statement up which is waiting
 *                in workload group simple waiters.
 * return value : void
 */
void WLMGroupSimpleThreadWakeUp(ResourcePool* rp)
{
    if (rp == NULL) {
        ereport(WARNING, (errmsg("Failed to waik up simple stmt, current stmt related resource pool is null.")));
        return;
    }

    if (list_length(rp->waiters_simple)) {
        WLMQNodeInfo* qnode = (WLMQNodeInfo*)linitial(rp->waiters_simple);

        if (qnode != NULL) {
            ereport(DEBUG3,
                (errmsg("wake up simple statements_cond, act_statements: %d, "
                        "max_statements: %d.",
                    rp->active_points,
                    u_sess->wlm_cxt->wlm_params.rpdata.max_pts)));

            (void)pthread_cond_signal(&qnode->condition);
        }
    }
}

/*
 * function name: WLMGroupComplicateThreadWakeUp
 * description  : Weak a complicate statement up which is waiting
 *                in workload group complicate waiters.
 * return value : void
 */
void WLMGroupComplicateThreadWakeUp(ResourcePool* rp)
{
    if (rp == NULL) {
        ereport(WARNING, (errmsg("Failed to waik up complicated stmt, current stmt related resource pool is null.")));
        return;
    }

    if (list_length(rp->waiters)) {
        WLMQNodeInfo* qnode = (WLMQNodeInfo*)linitial(rp->waiters);

        if (qnode != NULL) {
            ereport(DEBUG3,
                (errmsg("wake up complicated acstatements_cond, act_statements: %d, "
                        "max_statements: %d.",
                    rp->active_points,
                    u_sess->wlm_cxt->wlm_params.rpdata.max_pts)));

            (void)pthread_cond_signal(&qnode->condition);
        }
    }
}

/*
 * function name: WLMGroupRequestListThreadWakeUp
 * description  : Weak a statement up which is waiting
 *                in workload group waiter.
 * return value : void
 */
void WLMGroupRequestListThreadWakeUp(ResourcePool* rp)
{
    if (t_thrd.wlm_cxt.parctl_state.simple) {
        WLMGroupSimpleThreadWakeUp(rp);
    } else {
        WLMGroupComplicateThreadWakeUp(rp);
    }
}

/* clean active statements counts when thread is exiting */
void WLMProcReleaseActiveStatement(void)
{
    ParctlManager* parctl = &(t_thrd.wlm_cxt.thread_node_group->parctl);

    WLMContextLock list_lock(&parctl->statements_list_mutex);

    list_lock.Lock();

    if (u_sess->wlm_cxt->query_count_record) {
        parctl->current_support_statements--;
        u_sess->wlm_cxt->query_count_record = false;

        if (parctl->max_support_statements > 0 &&
            (parctl->max_support_statements - parctl->current_support_statements == 1)) {
            WLMGlobalRequestListThreadWakeUp(parctl);
        }
    }

    if (u_sess->wlm_cxt->reserved_in_active_statements > 0) {
        ereport(LOG,
            (errmsg("When thread is exited, thread is reserved %d global statement and "
                    "the reserved debug query is %s.",
                u_sess->wlm_cxt->reserved_in_active_statements,
                u_sess->wlm_cxt->reserved_debug_query)));

        parctl->statements_runtime_count =
            (parctl->statements_runtime_count > u_sess->wlm_cxt->reserved_in_active_statements)
                ? (parctl->statements_runtime_count - u_sess->wlm_cxt->reserved_in_active_statements)
                : 0;

        u_sess->wlm_cxt->reserved_in_active_statements = 0;
        WLMGlobalRequestListThreadWakeUp(parctl);
    }

    if (u_sess->wlm_cxt->reserved_in_respool_waiting > 0) {
        ereport(LOG,
            (errmsg("When thread is exited and waiting in resource pool, thread is reserved %d statement and "
                    "the reserved debug query is %s.",
                u_sess->wlm_cxt->reserved_in_respool_waiting,
                u_sess->wlm_cxt->reserved_debug_query)));

        parctl->respool_waiting_count =
            (parctl->respool_waiting_count > u_sess->wlm_cxt->reserved_in_respool_waiting)
                ? (parctl->respool_waiting_count - u_sess->wlm_cxt->reserved_in_respool_waiting)
                : 0;

        u_sess->wlm_cxt->reserved_in_respool_waiting = 0;
    }

    list_lock.UnLock();

    if (u_sess->wlm_cxt->reserved_in_group_statements > 0) {
        ereport(LOG,
            (errmsg("When thread is exited, thread is reserved %d group statement and "
                    "the reserved debug query is %s.",
                u_sess->wlm_cxt->reserved_in_group_statements,
                u_sess->wlm_cxt->reserved_debug_query)));

        ResourcePool* respool = t_thrd.wlm_cxt.qnode.rp;

        if (respool == NULL) {
            return;
        }

        USE_CONTEXT_LOCK(&respool->mutex);

        respool->active_points = (respool->active_points > u_sess->wlm_cxt->reserved_in_group_statements)
                                     ? (respool->active_points - u_sess->wlm_cxt->reserved_in_group_statements)
                                     : 0;

        u_sess->wlm_cxt->reserved_in_group_statements = 0;

        WLMGroupComplicateThreadWakeUp(t_thrd.wlm_cxt.qnode.rp);
    }

    if (u_sess->wlm_cxt->reserved_in_group_statements_simple > 0) {
        ereport(LOG,
            (errmsg("When thread is exited, thread is reserved %d simple group statement and "
                    "the reserved debug query is %s.",
                u_sess->wlm_cxt->reserved_in_group_statements_simple,
                u_sess->wlm_cxt->reserved_debug_query)));

        ResourcePool* respool = t_thrd.wlm_cxt.qnode.rp;

        if (respool == NULL) {
            return;
        }

        USE_CONTEXT_LOCK(&respool->mutex);

        respool->running_count_simple =
            (respool->running_count_simple > u_sess->wlm_cxt->reserved_in_group_statements_simple)
                ? (respool->running_count_simple - u_sess->wlm_cxt->reserved_in_group_statements_simple)
                : 0;

        u_sess->wlm_cxt->reserved_in_group_statements_simple = 0;

        WLMGroupSimpleThreadWakeUp(t_thrd.wlm_cxt.qnode.rp);
    }
}

/*
 * function name: WLMReleaseGlobalActiveStatement
 * description  : Global Workload Manager.
 *                Release resource after execution complete,
 *                and weak up the statements with max priority in
 *                the statements waiting list.
 * return value : void
 */
void WLMReleaseGlobalActiveStatement(int toWakeUp)
{
    ParctlManager* parctl = &(t_thrd.wlm_cxt.thread_node_group->parctl);

    if (t_thrd.wlm_cxt.parctl_state.global_reserve == 0 || t_thrd.wlm_cxt.parctl_state.global_release ||
        u_sess->wlm_cxt->reserved_in_active_statements < 1) {
        return;
    }

    USE_CONTEXT_LOCK(&parctl->statements_list_mutex);

    if (parctl->statements_runtime_count > 0) {
        parctl->statements_runtime_count--;
        u_sess->wlm_cxt->reserved_in_active_statements--;
    }

    /*
     * If a statement has been waken up from the resource pool,
     * we must not wake up the statement in the global list.
     */
    if (parctl->statements_waiting_count > 0 && toWakeUp == 0 &&
        (parctl->max_support_statements == 0 || parctl->current_support_statements < parctl->max_support_statements)) {
        WLMGlobalRequestListThreadWakeUp(parctl);
    }

    ereport(DEBUG3,
        (errmsg("release active statement, statements_runtime_count: %d, "
                "statements_waiting_count: %d, max_active_statements: %d. ",
            parctl->statements_runtime_count,
            parctl->statements_waiting_count,
            parctl->max_active_statements)));

    t_thrd.wlm_cxt.parctl_state.global_release = 1;
}

void WLMCheckReserveGlobalActiveStatement(ParctlManager* parctl)
{
    if (u_sess->wlm_cxt->reserved_in_active_statements > 0) {
        ereport(LOG,
            (errmsg("When new query is arriving, thread is reserved %d statement and "
                    "the reserved debug query is %s.",
                u_sess->wlm_cxt->reserved_in_active_statements,
                u_sess->wlm_cxt->reserved_debug_query)));

        parctl->statements_runtime_count =
            (parctl->statements_runtime_count > u_sess->wlm_cxt->reserved_in_active_statements)
                ? (parctl->statements_runtime_count - u_sess->wlm_cxt->reserved_in_active_statements)
                : 0;
        u_sess->wlm_cxt->reserved_in_active_statements = 0;
    }

    if (u_sess->wlm_cxt->reserved_in_respool_waiting > 0) {
        ereport(LOG,
            (errmsg("When new query is waiting in resource pool, thread is reserved %d statement and "
                    "the reserved debug query is %s.",
                u_sess->wlm_cxt->reserved_in_respool_waiting,
                u_sess->wlm_cxt->reserved_debug_query)));

        parctl->respool_waiting_count =
            (parctl->respool_waiting_count > u_sess->wlm_cxt->reserved_in_respool_waiting)
                ? (parctl->respool_waiting_count - u_sess->wlm_cxt->reserved_in_respool_waiting)
                : 0;

        u_sess->wlm_cxt->reserved_in_respool_waiting = 0;
    }

    if (u_sess->wlm_cxt->reserved_in_group_statements != 0) {
        ereport(LOG,
            (errmsg("When new query is arriving, resource pool is reserved %d statement and "
                    "the reserved respool debug query is %s.",
                u_sess->wlm_cxt->reserved_in_group_statements,
                u_sess->wlm_cxt->reserved_debug_query)));
    }
    if (u_sess->wlm_cxt->reserved_in_group_statements_simple != 0) {
        ereport(LOG,
            (errmsg("When new query is arriving, resource pool is reserved %d simple statement and "
                    "the reserved respool debug query is %s.",
                u_sess->wlm_cxt->reserved_in_group_statements_simple,
                u_sess->wlm_cxt->reserved_debug_query)));
    }

}

/*
 * function name: WLMReserveGlobalActiveStatement
 * description  : Global Workload Manager.
 *                Reserve resource to execute statement.
 * return value : void
 */
void WLMReserveGlobalActiveStatement(void)
{
    ParctlManager* parctl = &(t_thrd.wlm_cxt.thread_node_group->parctl);

    WLMContextLock list_lock(&parctl->statements_list_mutex);
    int ret;

    list_lock.Lock();

    WLMCheckReserveGlobalActiveStatement(parctl);

    if ((ret = CheckStatementsEnqueueCondition(parctl, PARCTL_GLOBAL)) == -1) {
        list_lock.UnLock();
        ereport(
            ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("sorry, too many statements are active now.")));
    }

    /* previous query exits abnormally, the resource pool count not return
     * current stmt enqueue on global will cause hang
     */
    if (ret != 0 && u_sess->wlm_cxt->reserved_in_group_statements == 0 &&
        u_sess->wlm_cxt->reserved_in_group_statements_simple == 0) {
        MemoryContext oldContext;
        bool hasWakeThreadUp = false;

        ListCell* lcnode = NULL;
        WLMListNode* node = NULL;
        WLMQNodeInfo* qnode = &t_thrd.wlm_cxt.qnode;

        pgstat_report_waiting_on_resource(STATE_ACTIVE_STATEMENTS);

        oldContext = MemoryContextSwitchTo(g_instance.wlm_cxt->workload_manager_mcxt);

        // search the node with the priority, if do not find it, it will be created
        lcnode = append_to_list<int, WLMListNode, true>(&parctl->statements_waiting_list, &qnode->priority);

        if (lcnode == NULL) {
            list_lock.UnLock();
            ereport(
                ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("reserve global statements failed, out of memory.")));
        }

        node = (WLMListNode*)lfirst(lcnode);
        node->data = qnode->priority;
        node->request_list = lappend(node->request_list, qnode);

        (void)MemoryContextSwitchTo(oldContext);

        qnode->lcnode = lcnode;

        parctl->statements_waiting_count++;

        // set clean handler while lock is released
        list_lock.set(WLMEnqueueCleanHandler, qnode);

        /* waiting in global queue */
        t_thrd.wlm_cxt.parctl_state.global_waiting = 1;

        pgstat_report_statement_wlm_status();

        /* loop to check if we can run again. */
        do {
            bool ImmediateInterruptOK_Old = t_thrd.int_cxt.ImmediateInterruptOK;

            PG_TRY();
            {
                /* check for pending interrupts before waiting. */
                CHECK_FOR_INTERRUPTS();

                t_thrd.int_cxt.ImmediateInterruptOK = true;

                list_lock.ConditionWait(&qnode->condition);

                t_thrd.int_cxt.ImmediateInterruptOK = ImmediateInterruptOK_Old;
            }
            PG_CATCH();
            {
                t_thrd.int_cxt.ImmediateInterruptOK = ImmediateInterruptOK_Old;

                // We must make sure the lock is mine, otherwise we must get the lock firstly.
                if (!list_lock.IsOwner()) {
                    list_lock.Lock(true);
                }

                /* UnLock global waiting list mutex. */
                list_lock.UnLock();

                PG_RE_THROW();
            }
            PG_END_TRY();

            /*
             * We will wake up threads until current active statements
             * is not less than max active statements.
             */
            if ((parctl->max_active_statements <= 0 && parctl->statements_waiting_count > 0) ||
                (((parctl->statements_runtime_count + 1) < parctl->max_active_statements) &&
                    ((parctl->current_support_statements + 1) < parctl->max_support_statements))) {
                hasWakeThreadUp = true;

                (void)WLMDeleteRequestListNode(qnode->lcnode, qnode);

                parctl->statements_waiting_count--;

                WLMGlobalRequestListThreadWakeUp(parctl);

                list_lock.reset();

                break;
            }
        } while (CheckStatementsEnqueueCondition(parctl, PARCTL_GLOBAL));

        /*
         * if the query has not wake any other thread up,
         * it must be removed from the waiting list and update
         * the total waiting count.
         */
        if (!hasWakeThreadUp) {
            (void)WLMDeleteRequestListNode(qnode->lcnode, qnode);

            parctl->statements_waiting_count--;

            list_lock.reset();
        }

        t_thrd.wlm_cxt.parctl_state.global_waiting = 0;
    }

    pgstat_report_waiting_on_resource(STATE_NO_ENQUEUE);

    if (t_thrd.wlm_cxt.parctl_state.enqueue && t_thrd.wlm_cxt.parctl_state.global_reserve == 0) {
        parctl->statements_runtime_count++;
        parctl->statements_runtime_plus = Min(parctl->statements_runtime_plus + 1, parctl->max_active_statements);
        t_thrd.wlm_cxt.parctl_state.global_reserve = 1;
        u_sess->wlm_cxt->reserved_in_active_statements++;

        if (u_sess->wlm_cxt->query_count_record == false) {
            u_sess->wlm_cxt->query_count_record = true;
            parctl->current_support_statements++;
        }

        /* reserved the string for later checking */
        if (t_thrd.postgres_cxt.debug_query_string) {
            int rcs = snprintf_truncated_s(u_sess->wlm_cxt->reserved_debug_query,
                sizeof(u_sess->wlm_cxt->reserved_debug_query),
                "%s",
                t_thrd.postgres_cxt.debug_query_string);
            securec_check_ss(rcs, "\0", "\0");
        }
    }

    pgstat_report_statement_wlm_status();

    list_lock.UnLock();
}

/*
 * function name: WLMReleaseGroupActiveStatement
 * description  : Workload Group Manager.
 *                Release resource after execution complete,
 *                and weak up the statements in
 *                the workload group waiter.
 * return value : int
 *                0: no statement is waken up from the resource pool list
 *                1: a statement is waken up
 */
int WLMReleaseGroupActiveStatement()
{
    if (t_thrd.wlm_cxt.parctl_state.rp_reserve == 0 || t_thrd.wlm_cxt.parctl_state.rp_release) {
        return 0;
    }
    if (t_thrd.wlm_cxt.parctl_state.simple && u_sess->wlm_cxt->reserved_in_group_statements_simple <= 0) {
        return 0;
    }
    if (!t_thrd.wlm_cxt.parctl_state.simple && u_sess->wlm_cxt->reserved_in_group_statements <= 0) {
        return 0;
    }

    /*
     * we can get resource pool from collect info quickly.
     */
    ResourcePool* respool = t_thrd.wlm_cxt.qnode.rp;

    if (respool == NULL) {
        return 0;
    }

    USE_CONTEXT_LOCK(&respool->mutex);

    ereport(DEBUG3,
        (errmsg("release active statement, "
                "rp_entry->active_points: %d, "
                "g_wlm_params.rpdata.max_pts: %d. "
                "waiters: %d",
            respool->active_points,
            u_sess->wlm_cxt->wlm_params.rpdata.max_pts,
            list_length(respool->waiters))));

    if (t_thrd.wlm_cxt.parctl_state.simple) {
        if (respool->running_count_simple > 0) {
            respool->running_count_simple--;
            u_sess->wlm_cxt->reserved_in_group_statements_simple--;
        }

        t_thrd.wlm_cxt.parctl_state.rp_release = 1;

        if (list_length(respool->waiters_simple) == 0) {
            return 0;
        }
    } else {
        if (respool->active_points > 0) {
            respool->active_points -= u_sess->wlm_cxt->wlm_params.rpdata.act_pts;

            u_sess->wlm_cxt->reserved_in_group_statements -= u_sess->wlm_cxt->wlm_params.rpdata.act_pts;

            if (respool->active_points < 0) {
                respool->active_points = 0;
            }
        }

        if (respool->running_count > 0) {
            respool->running_count--;
        }

        /* we can use ref_count without lock */
        if (respool->ref_count > 0) {
            (void)gs_atomic_add_32(&respool->ref_count, -1);
        }

        t_thrd.wlm_cxt.parctl_state.rp_release = 1;

        if (list_length(respool->waiters) == 0) {
            return 0;
        }
    }
    /*
     * we always try to wake up the head waiter.
     * and whether it will run or not depends on
     * it's own check.
     */
    WLMGroupRequestListThreadWakeUp(respool);

    return 1;
}

/*
 * function name: WLMReserveGroupActiveStatement
 * description  : Workload Group Manager.
 *                Reserve resource in workload group to execute statement.
 * return value : void
 */
void WLMReserveGroupActiveStatement(void)
{
    int ret = 0;
    ResourcePool* respool = NULL;
    ResourcePool* respool_reserved = NULL;
    WLMQNodeInfo* qnode = &t_thrd.wlm_cxt.qnode;

    ParctlManager* parctl = &(t_thrd.wlm_cxt.thread_node_group->parctl);

    WLMAutoLWLock htab_lock(ResourcePoolHashLock, LW_SHARED);
    bool timeout = false;
    WLMGeneralParam* g_wlm_params = &u_sess->wlm_cxt->wlm_params;

    if ((g_wlm_params->rpdata.max_pts <= 0 && t_thrd.wlm_cxt.parctl_state.simple == 0) ||
        (g_wlm_params->rpdata.max_stmt_simple <= 1 && t_thrd.wlm_cxt.parctl_state.simple == 1) ||
        !OidIsValid(g_wlm_params->rpdata.rpoid) || g_wlm_params->rpdata.superuser) {
        return;
    }

    /* grab exclusive lock */
    htab_lock.AutoLWLockAcquire();

    /* it will alloc memory when not found. */
    respool = GetRespoolFromHTab(g_wlm_params->rpdata.rpoid, false);

    htab_lock.AutoLWLockRelease();

    qnode->removed = false;
    qnode->rp = respool;

    WLMContextLock rp_lock(&respool->mutex);

    /* reserver active statements */
    rp_lock.Lock();

    /* if session_respool_oldoid has changed,we should clean old respool info */
    if (u_sess->wlm_cxt->respool_old_oid == InvalidOid) {
        u_sess->wlm_cxt->respool_old_oid = g_wlm_params->rpdata.rpoid;
    } else if (u_sess->wlm_cxt->respool_old_oid != g_wlm_params->rpdata.rpoid) {
        htab_lock.AutoLWLockAcquire();
        respool_reserved = GetRespoolFromHTab(u_sess->wlm_cxt->respool_old_oid, false);
        htab_lock.AutoLWLockRelease();

        u_sess->wlm_cxt->respool_old_oid = g_wlm_params->rpdata.rpoid;
    } else {
        respool_reserved = respool;
    }
    /* adjust the count in the resource pool */
    if (u_sess->wlm_cxt->reserved_in_group_statements > 0) {
        ereport(LOG, (errmsg("When query is arriving, thread is reserved %d group statement and "
                    "the reserved debug query is %s.",
                    u_sess->wlm_cxt->reserved_in_group_statements, u_sess->wlm_cxt->reserved_debug_query)));

        if (respool_reserved != NULL) {
            respool_reserved->active_points =
                (respool_reserved->active_points > u_sess->wlm_cxt->reserved_in_group_statements)
                    ? (respool_reserved->active_points - u_sess->wlm_cxt->reserved_in_group_statements) : 0;
        }

        u_sess->wlm_cxt->reserved_in_group_statements = 0;
    }

    if (u_sess->wlm_cxt->reserved_in_group_statements_simple > 0) {
        ereport(LOG, (errmsg("When query is arriving, thread is reserved %d simple group statement and "
                    "the reserved debug query is %s.",
                    u_sess->wlm_cxt->reserved_in_group_statements_simple, u_sess->wlm_cxt->reserved_debug_query)));
        if (respool_reserved != NULL) {
            respool_reserved->running_count_simple =
                (respool_reserved->running_count_simple > u_sess->wlm_cxt->reserved_in_group_statements_simple)
                ? (respool_reserved->running_count_simple - u_sess->wlm_cxt->reserved_in_group_statements_simple) : 0;
        }

        u_sess->wlm_cxt->reserved_in_group_statements_simple = 0;
    }

    if (t_thrd.wlm_cxt.parctl_state.simple) {
        u_sess->wlm_cxt->reserved_in_group_statements_simple = 0;  // simple and complicated ones should be reset
    } else {
        u_sess->wlm_cxt->reserved_in_group_statements = 0;  // reset the value if the session respool is changed

    }
    ereport(DEBUG3, (errmsg("reserve active statement, "
                "respool->active_points: %d, "
                "g_wlm_params.rpdata.max_pts: %d. "
                "waiters: %d",
                respool->active_points, g_wlm_params->rpdata.max_pts, list_length(respool->waiters))));

    ret = CheckStatementsEnqueueCondition(parctl, PARCTL_RESPOOL);

    /* if the statement is special, we need not use the resource pool */
    if (t_thrd.wlm_cxt.parctl_state.simple == 0 && t_thrd.wlm_cxt.parctl_state.rp_reserve == 0) {
        (void)gs_atomic_add_32(&respool->ref_count, 1);
    }

    /* we should obey the user setting and FIFO rule at the same time. */
    if (ret != 0) {
        bool hasWakeUp = false;
        MemoryContext oldContext = MemoryContextSwitchTo(g_instance.wlm_cxt->workload_manager_mcxt);

        pgstat_report_waiting_on_resource(STATE_ACTIVE_STATEMENTS);

        if (t_thrd.wlm_cxt.parctl_state.simple) {
            qnode->max_pts = g_wlm_params->rpdata.max_stmt_simple;
            respool->waiters_simple = lappend(respool->waiters_simple, qnode);
        } else {
            qnode->max_pts = g_wlm_params->rpdata.max_pts;
            qnode->act_pts = g_wlm_params->rpdata.act_pts;
            respool->waiters = lappend(respool->waiters, qnode);
        }

        (void)MemoryContextSwitchTo(oldContext);

        /*
         * If the statement will wait in the resource pool,
         * we need wake up a statement from the global list,
         * that it will make current count of statements
         * in running is max active statements.
         */
        if (g_instance.wlm_cxt->dynamic_workload_inited) {
            /* for simple queries in dynamic workload */
            if (t_thrd.wlm_cxt.parctl_state.reserve && !t_thrd.wlm_cxt.parctl_state.transact &&
                !t_thrd.wlm_cxt.parctl_state.subquery) {
                WLMContextLock list_lock(&t_thrd.wlm_cxt.thread_climgr->statement_list_mutex);

                list_lock.Lock();

                t_thrd.wlm_cxt.thread_climgr->central_waiting_count++;

                reserved_in_central_waiting++;

                if (list_length(t_thrd.wlm_cxt.thread_climgr->statements_waiting_list) > 0 &&
                    t_thrd.wlm_cxt.thread_climgr->max_support_statements > 0 &&
                    t_thrd.wlm_cxt.thread_climgr->current_support_statements <
                        t_thrd.wlm_cxt.thread_climgr->max_support_statements) {
                    DynamicInfoNode* info =
                        (DynamicInfoNode*)linitial(t_thrd.wlm_cxt.thread_climgr->statements_waiting_list);

                    if (info != NULL) {
                        list_lock.ConditionWakeUp(&info->condition);
                    }
                }

                list_lock.UnLock();
            }
        } else if (t_thrd.wlm_cxt.parctl_state.global_reserve && !t_thrd.wlm_cxt.parctl_state.transact &&
                   !t_thrd.wlm_cxt.parctl_state.subquery) {
            USE_CONTEXT_LOCK(&parctl->statements_list_mutex);

            parctl->respool_waiting_count++;
            u_sess->wlm_cxt->reserved_in_respool_waiting++;

            WLMGlobalRequestListThreadWakeUp(parctl);
        }

        rp_lock.set(WLMEnqueueCleanHandler, qnode);

        t_thrd.wlm_cxt.parctl_state.respool_waiting = 1;

        pgstat_report_statement_wlm_status();

        /* check if we can run again. */
        do {
            bool saved_ImmediateInterruptOK = t_thrd.int_cxt.ImmediateInterruptOK;

            PG_TRY();
            {
                /* check for pending interrupts before waiting. */
                CHECK_FOR_INTERRUPTS();

                t_thrd.int_cxt.ImmediateInterruptOK = true;
                if (u_sess->attr.attr_resource.transaction_pending_time <= 0) {
                    rp_lock.ConditionWait(&qnode->condition);
                } else {
                    /* wait for the condition and timeout is set by user */
                    rp_lock.ConditionTimedWait(&qnode->condition, u_sess->attr.attr_resource.transaction_pending_time);
                    if ((t_thrd.wlm_cxt.parctl_state.transact || t_thrd.wlm_cxt.parctl_state.subquery) &&
                        CheckStatementsEnqueueCondition(parctl, PARCTL_RESPOOL)) {
                        /* wait timeout, transaction block will not continue to wait avoiding deadlock. */
                        timeout = true;
                    }
                }
                t_thrd.int_cxt.ImmediateInterruptOK = saved_ImmediateInterruptOK;
            }
            PG_CATCH();
            {
                t_thrd.int_cxt.ImmediateInterruptOK = saved_ImmediateInterruptOK;

                rp_lock.reset();
                rp_lock.UnLock(true);

                /* execute clean handler */
                WLMEnqueueCleanHandler(qnode);

                PG_RE_THROW();
            }
            PG_END_TRY();

        } while ((CheckStatementsEnqueueCondition(parctl, PARCTL_RESPOOL) ||
                     CheckStatementsEnqueueCondition(parctl, PARCTL_GLOBAL)) && !timeout);

        t_thrd.wlm_cxt.parctl_state.respool_waiting = 0;

        if (g_instance.wlm_cxt->dynamic_workload_inited) {
            /* for simple queries in dynamic workload */
            if (t_thrd.wlm_cxt.parctl_state.reserve && !t_thrd.wlm_cxt.parctl_state.transact &&
                !t_thrd.wlm_cxt.parctl_state.subquery) {
                USE_CONTEXT_LOCK(&t_thrd.wlm_cxt.thread_climgr->statement_list_mutex);
                if (t_thrd.wlm_cxt.thread_climgr->central_waiting_count > 0) {
                    t_thrd.wlm_cxt.thread_climgr->central_waiting_count--;
                    reserved_in_central_waiting--;
                }
            }
        } else if (t_thrd.wlm_cxt.parctl_state.global_reserve && !t_thrd.wlm_cxt.parctl_state.transact &&
                   !t_thrd.wlm_cxt.parctl_state.subquery) {
            USE_CONTEXT_LOCK(&parctl->statements_list_mutex);

            if (parctl->respool_waiting_count > 0) {
                parctl->respool_waiting_count--;
                u_sess->wlm_cxt->reserved_in_respool_waiting--;
            }
        }

        /* check if we can wake up another query */
        if (t_thrd.wlm_cxt.parctl_state.simple) {
            if ((respool->running_count_simple + 1) < g_wlm_params->rpdata.max_stmt_simple) {
                hasWakeUp = true;

                respool->waiters_simple = list_delete_ptr(respool->waiters_simple, qnode);

                rp_lock.reset();

                WLMGroupRequestListThreadWakeUp(respool);
            }
        } else {
            /* a large complicate stmt end,may wake up more than one small conplicate stmt */
            if ((respool->active_points + g_wlm_params->rpdata.act_pts) < g_wlm_params->rpdata.max_pts &&
                CheckStatementsEnqueueCondition(parctl, PARCTL_GLOBAL) == 0) {
                hasWakeUp = true;

                respool->waiters = list_delete_ptr(respool->waiters, qnode);

                rp_lock.reset();

                WLMGroupRequestListThreadWakeUp(respool);
            }
        }

        /* free the cond */
        if (!hasWakeUp) {
            if (t_thrd.wlm_cxt.parctl_state.simple) {
                respool->waiters_simple = list_delete_ptr(respool->waiters_simple, qnode);
            } else {
                respool->waiters = list_delete_ptr(respool->waiters, qnode);
            }

            rp_lock.reset();
        }
    }

    pgstat_report_waiting_on_resource(STATE_NO_ENQUEUE);

    if (t_thrd.wlm_cxt.parctl_state.rp_reserve == 0) {
        if (!CheckStatementsEnqueueCondition(parctl, PARCTL_RESPOOL)) {
            if (t_thrd.wlm_cxt.parctl_state.simple) {
                respool->running_count_simple++;
                t_thrd.wlm_cxt.parctl_state.rp_reserve = 1;
                u_sess->wlm_cxt->reserved_in_group_statements_simple++;
            } else {
                respool->active_points += g_wlm_params->rpdata.act_pts;
                respool->running_count++;
                t_thrd.wlm_cxt.parctl_state.rp_reserve = 1;
                u_sess->wlm_cxt->reserved_in_group_statements += g_wlm_params->rpdata.act_pts;
            }
        } else {
            u_sess->wlm_cxt->forced_running = true;
        }
        /* reserved the string for later checking */
        if (t_thrd.postgres_cxt.debug_query_string) {
            int rc = snprintf_truncated_s(u_sess->wlm_cxt->reserved_debug_query,
                sizeof(u_sess->wlm_cxt->reserved_debug_query), "%s", t_thrd.postgres_cxt.debug_query_string);
            securec_check_ss(rc, "\0", "\0");
        }
    } else {
        qnode->rp = NULL;
    }

    pgstat_report_statement_wlm_status();

    rp_lock.UnLock();
}

/*
 * function name: WLMReserveAcstatement
 * description  : Workload Manager.
 *                Reserve workload resource,
 *                include global workload manager
 *                and workload group manager.
 * return value : void
 */
void WLMReserveAcstatement(ParctlType eqtype)
{
    switch (eqtype) {
        case PARCTL_GLOBAL:
            WLMReserveGlobalActiveStatement();
            t_thrd.wlm_cxt.parctl_state.simple = 1;

            break;
        case PARCTL_RESPOOL:
            WLMReserveGroupActiveStatement();
            break;
        default:
            break;
    }
}

/*
 * function name: WLMReserveAcstatement
 * description  : Workload Manager.
 *                Release workload resource,
 *                include global workload manager
 *                and workload group manager.
 * return value : void
 */
void WLMReleaseAcstatement()
{
    int ret = WLMReleaseGroupActiveStatement();

    WLMReleaseGlobalActiveStatement(ret);

    // We need not report statement wlm status while thread exiting.
    if (u_sess->wlm_cxt->parctl_state_exit == 0) {
        pgstat_report_statement_wlm_status();
    }
}

/*
 * function name: WLMCheckResourcePool
 * description  : resource pool statement self wake up
 * return value : void
 */
void WLMCheckResourcePool()
{
    ResourcePool* rp = NULL;

    HASH_SEQ_STATUS hseq;

    /* search the resource pool hash table to check if need wake up statement */
    hash_seq_init(&hseq, g_instance.wlm_cxt->resource_pool_hashtbl);

    WLMAutoLWLock htab_lock(ResourcePoolHashLock, LW_SHARED);

    htab_lock.AutoLWLockAcquire();

    while ((rp = (ResourcePool*)hash_seq_search(&hseq)) != NULL) {
        if (list_length(rp->waiters) == 0 && list_length(rp->waiters_simple) == 0) {
            continue;
        }

        WLMContextLock rp_lock(&rp->mutex);
        rp_lock.Lock();

        if (list_length(rp->waiters_simple) > 0) {
            WLMQNodeInfo* qnode = (WLMQNodeInfo*)linitial(rp->waiters_simple);
            if (qnode->max_pts > rp->running_count_simple) {
                ereport(LOG, (errmsg("wake up simple job for respool:%u automatically.", rp->rpoid)));
                WLMGroupSimpleThreadWakeUp(rp);
            }
        }

        if (!g_instance.wlm_cxt->dynamic_workload_inited && list_length(rp->waiters) > 0) {
            WLMQNodeInfo* qnode = (WLMQNodeInfo*)linitial(rp->waiters);
            if (qnode->max_pts > qnode->act_pts + rp->active_points) {
                ereport(LOG, (errmsg("wake up complicate for respool:%u job automatically.", rp->rpoid)));
                WLMGroupComplicateThreadWakeUp(rp);
            }
        }

        rp_lock.UnLock();
    }

    htab_lock.AutoLWLockRelease();
}

/*
 * function name: WLMHandleDywlmSimpleExcept
 * description  : Check simple job exception count for dywlm,
 *                then clear exception count and wake up next stmt.
 * return value : void
 */
void WLMHandleDywlmSimpleExcept(bool proc_exit)
{
    /* u_sess->wlm_cxt->reserved_in_group_statements_simple > 0 means u_sess->wlm_cxt->respool_old_oid is not
     * InvalidOid */
    if (u_sess->wlm_cxt->reserved_in_group_statements_simple > 0) {
        ResourcePool* respool_reserved = NULL;
        WLMAutoLWLock htab_lock(ResourcePoolHashLock, LW_SHARED);
        htab_lock.AutoLWLockAcquire();
        respool_reserved = GetRespoolFromHTab(u_sess->wlm_cxt->respool_old_oid, true);
        htab_lock.AutoLWLockRelease();

        if (respool_reserved == NULL) {
            ereport(LOG, (errmsg("resource pool %u does not exist in htab.", u_sess->wlm_cxt->respool_old_oid)));
            return;
        }

        if (proc_exit) {
            ereport(LOG,
                (errmsg("When thread is exited, thread is reserved %d simple group statement and "
                        "the reserved debug query is %s.",
                    u_sess->wlm_cxt->reserved_in_group_statements_simple,
                    u_sess->wlm_cxt->reserved_debug_query)));
        } else {
            ereport(LOG,
                (errmsg("When dywlm query is arriving, thread is reserved %d simple group statement and "
                        "the reserved debug query is %s.",
                    u_sess->wlm_cxt->reserved_in_group_statements_simple,
                    u_sess->wlm_cxt->reserved_debug_query)));
        }

        respool_reserved->running_count_simple =
            (respool_reserved->running_count_simple > u_sess->wlm_cxt->reserved_in_group_statements_simple)
                ? (respool_reserved->running_count_simple - u_sess->wlm_cxt->reserved_in_group_statements_simple)
                : 0;

        u_sess->wlm_cxt->reserved_in_group_statements_simple = 0;

        WLMGroupSimpleThreadWakeUp(respool_reserved);
    }
}

/*
 * function name: WLMCheckResourcePoolIsIdle
 * description  : check resource pool in the hash table, if
 *                active statements still use it, it can not be removed.
 * return value : bool
 *                true: It's idle, it can be removed.
 *                false: It's still in use, cannot be removed.
 */
bool WLMCheckResourcePoolIsIdle(Oid rpoid)
{
    if (!OidIsValid(rpoid)) {
        return true;
    }

    if (!g_instance.wlm_cxt->dynamic_workload_inited) {
        USE_AUTO_LWLOCK(ResourcePoolHashLock, LW_SHARED);

        ResourcePool* respool =
            (ResourcePool*)hash_search(g_instance.wlm_cxt->resource_pool_hashtbl, &rpoid, HASH_FIND, NULL);

        if (respool == NULL || (respool->ref_count == 0 && respool->running_count_simple == 0)) {
            return true;
        }
    } else {
        char* rpname = get_resource_pool_name(rpoid);

        if (rpname == NULL) {
            return true;
        }

        int rcount = 0;
        int wcount = 0;

        dywlm_server_get_respool_params(rpname, &rcount, &wcount);

        pfree(rpname);

        if (rcount == 0 && wcount == 0) {
            USE_AUTO_LWLOCK(ResourcePoolHashLock, LW_SHARED);

            ResourcePool* respool =
                (ResourcePool*)hash_search(g_instance.wlm_cxt->resource_pool_hashtbl, &rpoid, HASH_FIND, NULL);

            if (respool && list_length(respool->waiters) > 0) {
                list_free_deep(respool->waiters);
            }

            return true;
        }
    }

    return false;
}

/*
 * function name: WLMSwitchQNodeList
 * description  : Switch waiting list
 *                because of the group percent changed.
 * return value : void
 */
void WLMSwitchQNodeList(ParctlManager* parctl, int priority_old, int priority_new)
{
    ListCell* curr = NULL;
    ListCell* lcnode_old = NULL;
    ListCell* lcnode_new = NULL;

    WLMListNode* node_old = NULL;
    WLMListNode* node_new = NULL;

    bool found = false;

    /* priority is not changed, nothing to do */
    if (priority_new == priority_old) {
        return;
    }

    /* search priority list for old node */
    lcnode_old = search_list<int, WLMListNode>(parctl->statements_waiting_list, &priority_old, &found);

    if (!found) {
        return;
    }

    /* search priority list for new node */
    lcnode_new = search_list<int, WLMListNode>(parctl->statements_waiting_list, &priority_new, &found);

    /* If new priority node doesn't exist, we will update priority of the old node. */
    if (!found) {
        node_old = (WLMListNode*)lfirst(lcnode_old);
        node_old->data = priority_new;

        foreach (curr, node_old->request_list) {
            WLMQNodeInfo* qnode = (WLMQNodeInfo*)lfirst(curr);

            qnode->priority = priority_new;
        }

        ereport(DEBUG3,
            (errmsg("switch qnode list from %d to %d. "
                    "length of waiting list is %d",
                priority_old,
                priority_new,
                list_length(parctl->statements_waiting_list))));
        return;
    }

    node_old = (WLMListNode*)lfirst(lcnode_old);
    node_new = (WLMListNode*)lfirst(lcnode_new);

    node_new->request_list = list_concat(node_new->request_list, node_old->request_list);

    foreach (curr, node_old->request_list) {
        WLMQNodeInfo* qnode = (WLMQNodeInfo*)lfirst(curr);

        qnode->lcnode = lcnode_new;
        qnode->priority = priority_new;
    }

    node_old->request_list = NULL;

    parctl->statements_waiting_list = list_delete_ptr(parctl->statements_waiting_list, node_old);

    pfree(node_old);

    ereport(DEBUG3,
        (errmsg("switch qnode list from %d to %d. "
                "length of waiting list is %d",
            priority_old,
            priority_new,
            list_length(parctl->statements_waiting_list))));
}

/*
 * function name: WLMMoveNodeToList
 * description  : move the qnode to the new priority node list
 * arguments    :
 *                _in_ sess_id: session id
 *                _in_ cgroup: group name
 * return value : void
 */
void WLMMoveNodeToList(WLMNodeGroupInfo* ng, uint64 sess_id, const char* cgroup)
{
    ParctlManager* parctl = &ng->parctl;

    USE_CONTEXT_LOCK(&parctl->statements_list_mutex);

    /* search the queue node from the global waiting list */
    WLMQNodeInfo* qnode = WLMSearchQNode(parctl, sess_id);

    if (qnode != NULL) {
        bool found = false;
        int priority = gscgroup_get_percent(ng, cgroup);

        /* priority is not changed, nothing to do */
        if (qnode->priority == priority) {
            return;
        }

        ListCell* lcnode = search_list<int, WLMListNode>(parctl->statements_waiting_list, &priority, &found);
        WLMListNode* node = (WLMListNode*)lfirst(qnode->lcnode);

        USE_MEMORY_CONTEXT(g_instance.wlm_cxt->workload_manager_mcxt);

        if (!found) {
            /*
             * If the statement to change has a different
             * priority and it's only one in this priority list,
             * only change its priority data.
             */
            if (list_length(node->request_list) == 1) {
                node->data = priority;
                qnode->priority = node->data;

                return;
            } else {
                /* new priority does not exist, we must create it */
                lcnode = append_to_list<int, WLMListNode, true>(&parctl->statements_waiting_list, &priority);

                if (lcnode == NULL) {
                    RELEASE_CONTEXT_LOCK();
                    REVERT_MEMORY_CONTEXT();
                    ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("move node failed, out of memory.")));
                }
            }
        }

        node = (WLMListNode*)lfirst(lcnode);

        node->data = priority;
        /* append the new priority to the request list */
        node->request_list = lappend(node->request_list, qnode);

        /* delete old priority node from the list */
        (void)WLMDeleteRequestListNode(qnode->lcnode, qnode);

        qnode->priority = node->data;
        qnode->lcnode = lcnode;
        qnode->removed = false;
    }

    return;
}

/*
 * function name: WLMSetMaxStatementsInternal
 * description  : we will set max statements for one logical cluster
 * return value : void
 */
void WLMSetMaxStatementsInternal(ParctlManager* parctl, int active_statements)
{
    if (active_statements < 0) {
        active_statements = 0;
    }

    USE_CONTEXT_LOCK(&parctl->statements_list_mutex);

    /* if memory protecting feature initialization failed, we will not use this control. */
    parctl->max_statements = t_thrd.utils_cxt.gs_mp_inited
                                 ? ((int)MAX_PARCTL_MEMORY * DYWLM_HIGH_QUOTA / FULL_PERCENT -
                                       active_statements * g_instance.wlm_cxt->parctl_process_memory) /
                                       PARCTL_MEMORY_UNIT
                                 : 0;
    parctl->max_active_statements = active_statements;

    WLMGlobalRequestListThreadWakeUp(parctl);
}

/*
 * function name: WLMSetMaxStatements
 * description  : we will set max statements
 * return value : void
 */
void WLMSetMaxStatements(int active_statements)
{
    ParctlManager* parctl = &g_instance.wlm_cxt->MyDefaultNodeGroup.parctl;

    if (AmPostmasterProcess()) {
        WLMSetMaxStatementsInternal(parctl, active_statements);
    } else if (AmWLMWorkerProcess()) {
        USE_AUTO_LWLOCK(WorkloadNodeGroupLock, LW_SHARED);

        WLMNodeGroupInfo* hdata = NULL;

        HASH_SEQ_STATUS hash_seq;
        hash_seq_init(&hash_seq, g_instance.wlm_cxt->stat_manager.node_group_hashtbl);

        while ((hdata = (WLMNodeGroupInfo*)hash_seq_search(&hash_seq)) != NULL) {
            if (!hdata->used) {
                continue;
            }

            parctl = &hdata->parctl;

            WLMSetMaxStatementsInternal(parctl, active_statements);
        }
    }
}

/*
 * function name: InitializeResourcePoolHashTable
 * description  : Workload Group Manager.
 *                Initialize workload group hash table.
 * return value : void
 */
void InitializeUserResourcePoolHashTable(void)
{
    HASHCTL hash_ctl;
    int rc;

    Assert(AmPostmasterProcess());

    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");

    hash_ctl.hash = oid_hash;
    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(ResourcePool);
    hash_ctl.hcxt = g_instance.wlm_cxt->workload_manager_mcxt;
    hash_ctl.alloc = WLMAlloc0NoExcept4Hash; /* use alloc function without exception */
    hash_ctl.dealloc = pfree;

    /* create resource pool hash table */
    g_instance.wlm_cxt->resource_pool_hashtbl = hash_create("Resource pool hash table",
        RESOURCE_POOL_HASH_SIZE,
        &hash_ctl,
        HASH_ELEM | HASH_SHRCTX | HASH_FUNCTION | HASH_ALLOC | HASH_DEALLOC);

    rc = memset_s(&g_instance.wlm_cxt->stat_manager, sizeof(WLMStatManager), 0, sizeof(WLMStatManager));
    securec_check(rc, "\0", "\0");

    rc = snprintf_s(g_instance.wlm_cxt->stat_manager.database, NAMEDATALEN, NAMEDATALEN - 1, "%s", DEFDBNAME);
    securec_check_ss(rc, "\0", "\0");

    rc = memset_s(&hash_ctl, sizeof(hash_ctl), 0, sizeof(hash_ctl));
    securec_check(rc, "\0", "\0");

    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.hcxt = g_instance.wlm_cxt->workload_manager_mcxt;
    hash_ctl.entrysize = sizeof(UserData);
    hash_ctl.hash = oid_hash;
    hash_ctl.alloc = WLMAlloc0NoExcept4Hash; /* use alloc function without exception */
    hash_ctl.dealloc = pfree;

    g_instance.wlm_cxt->stat_manager.user_info_hashtbl = hash_create("wlm user info hash table",
        WORKLOAD_STAT_HASH_SIZE,
        &hash_ctl,
        HASH_ELEM | HASH_SHRCTX | HASH_FUNCTION | HASH_ALLOC | HASH_DEALLOC);
}

/*
 * @Description: check sql is start transaction
 * @IN str: sql string
 * @Return: start or begin transaction
 * @See also:
 */
unsigned char is_transcation_start(const char* str)
{
    char tmp[256];
    char *p = NULL;
    char *q = NULL;

    if (!StringIsValid(str)) {
        return 0;
    }

    errno_t rc = strncpy_s(tmp, sizeof(tmp), str, sizeof(tmp) - 1);
    securec_check(rc, "\0", "\0");

    p = tmp;

    while (isspace(*p)) {
        p++;
    }

    if (pg_strncasecmp(p, "begin;", strlen("begin;")) == 0) {
        return 1;
    }

    if (NULL == (q = strchr(p, ' '))) {
        if (pg_strcasecmp(p, "begin") == 0) {
            return 1;
        } else {
            return 0;
        }
    }

    *q++ = '\0';

    if (pg_strcasecmp(p, "begin") != 0 && pg_strcasecmp(p, "start") != 0) {
        return 0;
    }

    while (isspace(*q)) {
        q++;
    }

    if (pg_strncasecmp(q, "transaction", strlen("transaction")) != 0 && (pg_strcasecmp(p, "begin") == 0 && *q != ';')) {
        return 0;
    }

    return 1;
}

/*
 * function name: WLMParctlReady
 * description  : ready to global reserve active statements.
 * return value : void
 */
void WLMParctlReady(const char* sqlText)
{
    errno_t rc = memset_s(
        &t_thrd.wlm_cxt.parctl_state, sizeof(t_thrd.wlm_cxt.parctl_state), 0, sizeof(t_thrd.wlm_cxt.parctl_state));
    securec_check(rc, "\0", "\0");

    /* we always handle exception. */
    u_sess->wlm_cxt->parctl_state_control = 1;
    t_thrd.wlm_cxt.parctl_state.except = 1;
    t_thrd.wlm_cxt.parctl_state.simple = 1;
    t_thrd.wlm_cxt.parctl_state.special = WLMIsSpecialQuery(sqlText) ? 1 : 0;
    /* Is the query in a transaction block now? */
    t_thrd.wlm_cxt.parctl_state.transact = IsTransactionBlock() ? 1 : 0;
    t_thrd.wlm_cxt.parctl_state.transact_begin = 0;
    t_thrd.wlm_cxt.parctl_state.subquery = 0;

    /*
     * If we are in a transaction block, we will make it
     * has reserved global and resource pool active statements,
     * so that we can release active statements while transaction
     * block is end.
     */
    if (t_thrd.wlm_cxt.parctl_state.transact && !t_thrd.wlm_cxt.parctl_state.transact_begin) {
        /* super user need not do global parallel control */
        if (u_sess->wlm_cxt->is_reserved_in_transaction) {
            t_thrd.wlm_cxt.parctl_state.global_reserve = 1;
        }

        if (!u_sess->attr.attr_resource.enable_transaction_parctl) {
            t_thrd.wlm_cxt.parctl_state.rp_reserve = 1;
        }

        if (OidIsValid(u_sess->wlm_cxt->wlm_params.rpdata.rpoid)) {
            USE_AUTO_LWLOCK(ResourcePoolHashLock, LW_SHARED);

            /*
             * We have to get resource pool here because we will not reserve
             * active statements in resource pool while in a transaction block.
             * we will use this resource pool to release active statement if
             * the transaction is finished.
             */
            t_thrd.wlm_cxt.qnode.rp = GetRespoolFromHTab(u_sess->wlm_cxt->wlm_params.rpdata.rpoid, true);
        }
    }

    if (IsAbortedTransactionBlockState()) {
        return;
    }

    // set current user info
    WLMSetUserInfo();

    /*
     * If the user is super user or it's a special
     * query it will not do the global parallel control.
     */
    if (t_thrd.wlm_cxt.parctl_state.special == 0 && !u_sess->wlm_cxt->wlm_params.rpdata.superuser) {
        t_thrd.wlm_cxt.parctl_state.enqueue = 1;
    }

    WLMSetStatInfo(sqlText);

    /* set parallel control debug info */
    u_sess->wlm_cxt->wlm_debug_info.parctl = &t_thrd.wlm_cxt.thread_node_group->parctl;
    u_sess->wlm_cxt->wlm_debug_info.pstate = &t_thrd.wlm_cxt.parctl_state;
    u_sess->wlm_cxt->wlm_debug_info.reserved_in_transaction = &u_sess->wlm_cxt->is_reserved_in_transaction;
}

/*
 * function name: WLMParctlReserve
 * description  : reserve active statements.
 * return value : void
 */
void WLMParctlReserve(ParctlType eqtype)
{
    /*
     * If the query in a transaction block,
     * it will not do the parallel control.
     */
    if (!IsQueuedSubquery() || (IsTransactionBlock() && eqtype == PARCTL_GLOBAL)) {
        if (!u_sess->wlm_cxt->is_reserved_in_transaction && t_thrd.wlm_cxt.parctl_state.enqueue &&
            eqtype == PARCTL_GLOBAL) {
            u_sess->wlm_cxt->is_reserved_in_transaction = true;
            t_thrd.wlm_cxt.parctl_state.global_reserve = 1;
        }

        return;
    }

    WLMReserveAcstatement(eqtype);
}

/*
 * function name: WLMParctlReserve
 * description  : release active statements.
 * return value : void
 */
void WLMParctlRelease(ParctlState* state)
{
    // If it has been released, ignore this time.
    if (state == NULL || state->release) {
        return;
    }

    /*
     * It will release active statement if it has done
     * global or resource pool parallel control and it's
     * not in any transaction block.
     */
    if (((IS_PGXC_COORDINATOR && !IsConnFromCoord()) || IS_SINGLE_NODE) &&
        (u_sess->wlm_cxt->parctl_state_exit || !(IsTransactionBlock() || t_thrd.wlm_cxt.parctl_state.subquery))) {
        WLMReleaseAcstatement();
        state->release = 1;

        if (t_thrd.wlm_cxt.parctl_state.global_reserve && u_sess->wlm_cxt->is_reserved_in_transaction) {
            u_sess->wlm_cxt->is_reserved_in_transaction = false;
        }

        t_thrd.wlm_cxt.parctl_state.global_reserve = 0;
    }

    // handle exception
    if (state->except) {
        WLMResetStatInfo4Exception();
        state->except = 0;
    }

    if ((IS_PGXC_COORDINATOR || IS_SINGLE_NODE) && t_thrd.wlm_cxt.collect_info->sdetail.statement) {
        pfree_ext(t_thrd.wlm_cxt.collect_info->sdetail.statement);
    }
}

/*
 * function name: WLMJumpQueue
 * description  : We will use this to make the statements waiting
 *                in the global queue jump the priority. This will
 *                avoid statements with high priority always executing.
 * return value :
 *               -1 : abnormal, not found
 *                0 : normal
 */
int WLMJumpQueue(ParctlManager* parctl, ThreadId tid)
{
    // Only super user can change the queue.
    if (!superuser()) {
        ereport(NOTICE, (errmsg("Only super user can change the queue.")));
        return 0;
    }

    USE_CONTEXT_LOCK(&parctl->statements_list_mutex);

    WLMQNodeInfo* qnode = WLMSearchQNode(parctl, tid);

    if (qnode != NULL) {
        ListCell* lcnode = WLMSearchAndCheckMaxNode(parctl->statements_waiting_list);
        WLMListNode* node = (WLMListNode*)lfirst(lcnode);

        /*
         * If the statement to change has the highest
         * priority and it's only one in the list, we
         * need not do anything.
         */
        if (qnode->priority == node->data && list_length(node->request_list) == 1) {
            return 0;
        }

        USE_MEMORY_CONTEXT(g_instance.wlm_cxt->workload_manager_mcxt);

        (void)WLMDeleteRequestListNode(qnode->lcnode, qnode);

        node->request_list = lcons(qnode, node->request_list);

        // Reset qnode info
        qnode->priority = node->data;
        qnode->lcnode = lcnode;
        qnode->removed = false;

        return 0;
    }

    return -1;
}

/*
 * function name: WLMGetResourcePoolDataInfo
 * description  : We get resource pool info in the hash table
 * return value : void
 */
void* WLMGetResourcePoolDataInfo(int* num)
{
    int i = 0;
    errno_t rc;

    HASH_SEQ_STATUS hash_seq;

    USE_AUTO_LWLOCK(ResourcePoolHashLock, LW_SHARED);

    /* get current session info count, we will do nothing if it's 0 */
    if (g_instance.wlm_cxt->resource_pool_hashtbl == NULL ||
        (*num = (int)hash_get_num_entries(g_instance.wlm_cxt->resource_pool_hashtbl)) == 0) {
        return NULL;
    }

    ResourcePool* rp = NULL;
    ResourcePool* respools = (ResourcePool*)palloc0(*num * (int)sizeof(ResourcePool));

    hash_seq_init(&hash_seq, g_instance.wlm_cxt->resource_pool_hashtbl);

    /* Get all real time session statistics from the register info hash table. */
    while ((rp = (ResourcePool*)hash_seq_search(&hash_seq)) != NULL) {
        rc = memcpy_s(respools + i, sizeof(ResourcePool), rp, sizeof(ResourcePool));
        securec_check(rc, "\0", "\0");

        // get the waiting count in this resource pool
        respools[i].waiting_count = list_length(rp->waiters);
        respools[i].running_count = rp->running_count;
        respools[i].ref_count = rp->ref_count;

        ++i;
    }

    *num = i;

    RELEASE_AUTO_LWLOCK();

    if (g_instance.wlm_cxt->dynamic_workload_inited) {
        for (i = 0; i < *num; ++i) {
            char* rpname = get_resource_pool_name(respools[i].rpoid);

            if (rpname == NULL) {
                continue;
            }

            respools[i].running_count = 0;
            respools[i].waiting_count = 0;

            dywlm_server_get_respool_params(rpname, &respools[i].running_count, &respools[i].waiting_count);

            respools[i].ref_count = respools[i].running_count + respools[i].waiting_count;

            pfree(rpname);
        }
    }

    return respools;
}

/*
 * @Description: verify global parallel control
 * @IN void
 * @Return: void
 * @See also:
 */
void WLMVerifyGlobalParallelControl(ParctlManager* parctl)
{
    int running_count = 0;
    int waiting_count = 0;

    if (parctl->statements_runtime_count <= 0 && parctl->statements_waiting_count <= 0) {
        return;
    }

    USE_CONTEXT_LOCK(&parctl->statements_list_mutex);

    foreach_cell(cell, parctl->statements_waiting_list)
    {
        WLMListNode* pnode = (WLMListNode*)lfirst(cell);

        waiting_count += list_length(pnode->request_list);
    }

    /* no statements is waiting , nothing to do */
    if (waiting_count <= 0) {
        return;
    }

    List* entries = NULL;

    PG_TRY();
    {
        parctl->statements_runtime_plus = 0;
        entries = pgstat_get_user_backend_entry(InvalidOid);

        if (entries == NULL) {
            ereport(LOG, (errmsg("cannot get backend entries or backend entry is empty")));
            running_count = 0;
        } else {
            foreach_cell(cell, entries)
            {
                PgBackendStatus* beentry = (PgBackendStatus*)lfirst(cell);

                if (!(superuser_arg(beentry->st_userid) || systemDBA_arg(beentry->st_userid))) {
                    ++running_count;
                }
            }

            if (entries != NULL) {
                list_free(entries);
            }
        }
    }
    PG_CATCH();
    {
        RELEASE_CONTEXT_LOCK();

        PG_RE_THROW();
    }
    PG_END_TRY();

    pgstat_reset_current_status();

    /* current running count does not match statements_runtime_count, fix it
     * minus the number of stmts that wake up since pgstat_get_user_backend_entry
     */
    if (parctl->statements_runtime_count != running_count) {
        ereport(LOG,
            (errmsg("verify running count: %d, waiting count: %d, runtime count: %d, increased count: %d",
                running_count,
                waiting_count,
                parctl->statements_runtime_count,
                parctl->statements_runtime_plus)));

        if (parctl->statements_runtime_count - parctl->statements_runtime_plus >
            (running_count + 2)) { // it is not accute
            parctl->statements_runtime_count = running_count;

            WLMGlobalRequestListThreadWakeUp(parctl);
        }

        parctl->statements_waiting_count = waiting_count;
    }
}

/*
 * @Description: init the parctl manager
 * @IN void
 * @Return: void
 * @See also:
 */
void WLMParctlInit(WLMNodeGroupInfo* info)
{
    ParctlManager* parctl = &info->parctl;

    parctl->max_active_statements = u_sess->attr.attr_resource.max_active_statements;

    parctl->statements_waiting_count = 0;

    parctl->statements_runtime_count = 0;

    parctl->statements_runtime_plus = 0;

    parctl->max_support_statements =
        (int)(t_thrd.utils_cxt.gs_mp_inited
            ? (MAX_PARCTL_MEMORY * PARCTL_ACTIVE_PERCENT) / (FULL_PERCENT * g_instance.wlm_cxt->parctl_process_memory)
            : 0);

    parctl->max_statements = (int)(t_thrd.utils_cxt.gs_mp_inited
                                 ? (MAX_PARCTL_MEMORY * DYWLM_HIGH_QUOTA / FULL_PERCENT -
                                       parctl->max_active_statements * g_instance.wlm_cxt->parctl_process_memory) /
                                       PARCTL_MEMORY_UNIT
                                 : 0);

    parctl->current_support_statements = 0;

    parctl->respool_waiting_count = 0;

    parctl->statements_waiting_list = NULL;

    parctl->resource_pool_hashtbl = NULL;  // not used

    (void)pthread_mutex_init(&parctl->statements_list_mutex, NULL);
}

/*
 * @Description: check DefaultXactReadOnly
 * @IN void
 * @Return: void
 * @See also:
 */
void WLMCheckDefaultXactReadOnly(void)
{
    int save_errno = errno;

    /* if IDLEINTRANSACTION no need to cancel stmt */
    if (!u_sess->attr.attr_storage.DefaultXactReadOnly) {
        if (t_thrd.shemem_ptr_cxt.MyBEEntry == NULL ||
            t_thrd.shemem_ptr_cxt.MyBEEntry->st_state == STATE_IDLEINTRANSACTION ||
            t_thrd.shemem_ptr_cxt.MyBEEntry->st_state == STATE_IDLEINTRANSACTION_ABORTED) {
            return;
        }
    }

    /*
     * Don't joggle the elbow of proc_exit
     */
    if (!t_thrd.proc_cxt.proc_exit_inprogress) {
        InterruptPending = true;
        t_thrd.int_cxt.QueryCancelPending = true;

        u_sess->wlm_cxt->cancel_from_defaultXact_readOnly = true;

        /*
         * in libcomm interrupt is not allow,
         * gs_r_cancel will signal libcomm and
         * libcomm will then check for interrupt.
         */
        gs_r_cancel();

        /*
         * If it's safe to interrupt, and we're waiting for input or a lock,
         * service the interrupt immediately
         */
        if (t_thrd.int_cxt.ImmediateInterruptOK && t_thrd.int_cxt.InterruptHoldoffCount == 0 &&
            t_thrd.int_cxt.CritSectionCount == 0) {
            /* bump holdoff count to make ProcessInterrupts() a no-op */
            /* until we are done getting ready for it */
            t_thrd.int_cxt.InterruptHoldoffCount++;
            LockErrorCleanup(); /* prevent CheckDeadLock from running */
            t_thrd.int_cxt.InterruptHoldoffCount--;
            ProcessInterrupts();
        }
    }

    u_sess->sig_cxt.got_SIGHUP = true;
    /* If we're still here, waken anything waiting on the process latch */
    if (t_thrd.proc) {
        SetLatch(&t_thrd.proc->procLatch);
    }

    errno = save_errno;
}

