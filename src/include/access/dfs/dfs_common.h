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
 * dfs_common.h
 *
 * IDENTIFICATION
 *    src/include/access/dfs/dfs_common.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DFS_COMMON_H
#define DFS_COMMON_H

#include "pg_config.h"

#ifndef ENABLE_LITE_MODE
#include "orc/Exceptions.hh"
#endif
#include "access/dfs/dfs_am.h"
#include "catalog/pg_collation.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "optimizer/clauses.h"
#include "optimizer/subselect.h"
#include "utils/biginteger.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/dfs_vector.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "utils/lsyscache.h"

#ifndef MIN
#define MIN(A, B) ((B) < (A) ? (B) : (A))
#endif

#define DFS_PRIVATE_ITEM "DfsPrivateItem"
#define DFS_NUMERIC64_MAX_PRECISION 18

/* MACROS which help to catch and print the exception. */
#define DFS_TRY()                                          \
    bool saveStatus = t_thrd.int_cxt.ImmediateInterruptOK; \
    t_thrd.int_cxt.ImmediateInterruptOK = false;           \
    bool errOccur = false;                                 \
    int errNo = ERRCODE_SYSTEM_ERROR;                      \
    StringInfo errMsg = makeStringInfo();                  \
    StringInfo errDetail = makeStringInfo();               \
    try
#define DFS_CATCH()                                                                                \
    catch (abi::__forced_unwind &)                                                                 \
    {                                                                                              \
        throw;                                                                                     \
    }                                                                                              \
    catch (orc::OrcException & ex)                                                                 \
    {                                                                                              \
        errOccur = true;                                                                           \
        errNo = ex.getErrNo();                                                                     \
        try {                                                                                      \
            appendStringInfo(errMsg, "%s", ex.what());                                             \
            appendStringInfo(errDetail, "%s", ex.msg().c_str());                                   \
        } catch (abi::__forced_unwind &) {                                                         \
            throw;                                                                                 \
        } catch (...) {                                                                            \
        }                                                                                          \
    }                                                                                              \
    catch (std::exception & ex)                                                                    \
    {                                                                                              \
        errOccur = true;                                                                           \
        try {                                                                                      \
            appendStringInfo(errMsg, "%s", ex.what());                                             \
        } catch (abi::__forced_unwind &) {                                                         \
            throw;                                                                                 \
        } catch (...) {                                                                            \
        }                                                                                          \
    }                                                                                              \
    catch (...)                                                                                    \
    {                                                                                              \
        errOccur = true;                                                                           \
    }                                                                                              \
    t_thrd.int_cxt.ImmediateInterruptOK = saveStatus;                                              \
    saveStatus = InterruptPending;                                                                 \
    InterruptPending = false;                                                                      \
    if (errOccur && errDetail->len > 0) {                                                          \
        ereport(LOG, (errmodule(MOD_DFS), errmsg("Caught exceptiion for: %s.", errDetail->data))); \
    }                                                                                              \
    InterruptPending = saveStatus;                                                                 \
    pfree_ext(errDetail->data);                                                                    \
    pfree_ext(errDetail);

#define DFS_ERRREPORT(msg, module)                                                             \
    if (errOccur) {                                                                            \
        destroy();                                                                             \
        ereport(ERROR, (errcode(errNo), errmodule(module),                                     \
                        errmsg(msg, errMsg->data, g_instance.attr.attr_common.PGXCNodeName))); \
    }                                                                                          \
    pfree_ext(errMsg->data);                                                                   \
    pfree_ext(errMsg);

#define DFS_ERRREPORT_WITHARGS(msg, module, ...)                                                            \
    if (errOccur) {                                                                                         \
        ereport(ERROR, (errcode(errNo), errmodule(module),                                                  \
                        errmsg(msg, __VA_ARGS__, errMsg->data, g_instance.attr.attr_common.PGXCNodeName))); \
    }                                                                                                       \
    pfree_ext(errMsg->data);                                                                                \
    pfree_ext(errMsg);

#define DFS_ERRREPORT_WITHOUTARGS(msg, module)                                                 \
    if (errOccur) {                                                                            \
        ereport(ERROR, (errcode(errNo), errmodule(module),                                     \
                        errmsg(msg, errMsg->data, g_instance.attr.attr_common.PGXCNodeName))); \
    }                                                                                          \
    pfree_ext(errMsg->data);                                                                   \
    pfree_ext(errMsg);


#define DEFAULT_HIVE_NULL "__HIVE_DEFAULT_PARTITION__"
#define DEFAULT_HIVE_NULL_LENGTH 26

/*
 * Check partition signature creation exception in case of the content exceeding
 * max allowed partition length
 */
#define partition_err_msg                                                                \
    "The length of the partition directory exceeds the current value(%d) of the option " \
    "\"dfs_partition_directory_length\", change the option to the greater value."
#define CHECK_PARTITION_SIGNATURE(rc, dirname) do { \
    if (rc != 0) {                                                                                  \
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmodule(MOD_DFS),                        \
                        errmsg(partition_err_msg, u_sess->attr.attr_storage.dfs_max_parsig_length), \
                        errdetail("the path name is \"%s\".", dirname)));                           \
    }                                                                                               \
    securec_check(rc, "\0", "\0");                                                                  \
} while (0)

#define strpos(p, s) (strstr(p, s) != NULL ? strstr(p, s) - p : -1)
#define basename_len(p, s) (strrchr(p, s) != NULL ? strrchr(p, s) - p : -1)

#define INT_CMP_HDFS(arg1, arg2, compare) do { \
    if ((arg1) < (arg2)) {            \
        compare = -1;                 \
    } else if ((arg1) > (arg2)) {     \
        compare = 1;                  \
    } else {                          \
        compare = 0;                  \
    }                                 \
} while (0)

/*
 * 1. NAN = NAN
 * 2. NAN > non-NAN
 * 3. non-NAN < NAN
 * 4. non-NAN cmp non-NAN
 * 5. arg2 will never be NAN here
 */
#define FLOAT_CMP_HDFS(arg1, arg2, compare) do { \
    if (isnan(arg1)) {                  \
        compare = 1;                    \
    } else {                            \
        if ((arg1) > (arg2)) {          \
            compare = 1;                \
        } else if ((arg1) < (arg2)) {   \
            compare = -1;               \
        } else {                        \
            compare = 0;                \
        }                               \
    }                                   \
} while (0)

#endif
