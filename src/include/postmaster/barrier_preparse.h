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
 * barrier_preparse.h
 *      Synchronize the global min csn between all nodes, head file
 *
 *
 * IDENTIFICATION
 *        /Code/src/include/pgxc/barrier_preparse.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef BARRIER_PREPARSE_H
#define BARRIER_PREPARSE_H

#define INIBARRIERCACHESIZE 100

#define IS_BARRIER_HASH_INIT \
    (g_instance.csn_barrier_cxt.barrier_hash_table != NULL && \
    g_instance.csn_barrier_cxt.barrier_hashtbl_lock != NULL)

#define BarrierCacheInsertBarrierId(BARRIER) do { \
    CommitSeqNo *idhentry = NULL;                                                                                \
    bool found = false;                                                                                          \
    idhentry = (CommitSeqNo *)hash_search(g_instance.csn_barrier_cxt.barrier_hash_table, (void *)(BARRIER), HASH_ENTER, &found); \
} while (0)

#define BarrierCacheDeleteBarrierId(BARRIER) do { \
    CommitSeqNo *idhentry = NULL;                                                                               \
    idhentry = (CommitSeqNo *)hash_search(g_instance.csn_barrier_cxt.barrier_hash_table, (void *)(BARRIER), HASH_REMOVE, NULL); \
    if (idhentry == NULL)                                                                                       \
        ereport(WARNING, (errmsg("trying to delete a barrierID that does not exist")));                         \
} while (0)

extern void BarrierPreParseMain(void);
extern void WakeUpBarrierPreParseBackend(void);
extern void SetBarrierPreParseLsn(XLogRecPtr startptr);

#endif /* BARRIER_PREPARSE_H */
