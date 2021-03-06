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
 * gtm_test_framework.h
 *        Routines for gtm distributed test framework.
 *
 *
 * IDENTIFICATION
 *        src/include/gtm/gtm_test_framework.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DISTRIBUTE_TEST_H
#define DISTRIBUTE_TEST_H

#include "gtm/gtm_c.h"
#include "gtm/gtm_opt.h"
#include "gtm/utils/elog.h"

#ifdef USE_ASSERT_CHECKING
#ifdef ENABLE_MULTIPLE_NODES
#define ENABLE_DISTRIBUTE_TEST
#endif

#define MAX_GID_STR_LEN (1024)
#define MAX_NAME_STR_LEN (256)

/* white-box log handle */
#define ereport_whitebox_domain(elevel, file, line, func, domain, rest) \
    (errstart(elevel, file, line, func, domain) ? (errfinish rest) : (void)0)

#define ereport_whitebox(elevel, file, line, func, rest) \
    ereport_whitebox_domain(elevel, file, line, func, TEXTDOMAIN, rest)

#define WHITEBOX_LOC __FILE__, __LINE__, PG_FUNCNAME_MACRO

typedef struct GtmDistributeTestParam {
    /* use as the guc-control white-box-fault probability
     *  range [0, 100000], probability 0, 1/100000 -> 1
     */
    int guc_probability;

    /* fault level, support ERROR and PANIC */
    int elevel;

    /* when white-box fault is WHITEBOX_WAIT, guc can provide "sleep_time"
     *  for sleep time control, if guc not contain this field, use the default value 30s
     */
    int sleep_time;
} GtmDistributeTestParam;

/* GTM white-box failure injection type definition */
typedef enum GtmWhiteboxFaillueType {
    GTM_WHITEBOX_DEFAULT = 0,
    GTM_WHITEBOX_WAIT,
    GTM_WHITEBOX_REPEAT,
    GTM_WHITEBOX_USERDEFINE,
    GTM_WHITEBOX_BUTT
} GtmWhiteboxFaillueType;

/* struct created for white-box tesing, support expansion */
typedef struct GtmWhiteBoxInjectStruct {
    /* test location record */
    char filename[MAX_NAME_STR_LEN];
    int lineno;
    char funcname[MAX_NAME_STR_LEN];

    /* failure type, defined above */
    int failureType;

    /* trigger as the probability, 0 -1, if set as 1, definitely trigger */
    double triggerProbability;

    /* white box test info needed */
    GlobalTransactionId currentXid;
    GTM_TransactionHandle handle;
} GtmWhiteBoxInjectStruct;

typedef void (*on_whitebox_callback)(GtmWhiteBoxInjectStruct* wbinfo);

extern GtmDistributeTestParam* gtmDistributeTestParam;

extern void gtm_default_whitebox_emit(GtmWhiteBoxInjectStruct* wbinfo);
extern bool gtm_check_distribute_test_param(char** newval, void** extra, GtmOptSource source);
extern void gtm_assign_distribute_test_param(const char* newval, void* extra);
extern bool gtm_execute_whitebox(const char* filename, int lineno, const char* funcname, GlobalTransactionId xid,
    GTM_TransactionHandle handle, int failureType, double probability);

/*GTM exception scenes*/

#endif

#endif /* DISTRIBUTE_TEST_H */
