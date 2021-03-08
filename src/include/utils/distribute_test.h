/* ---------------------------------------------------------------------------------------
 * 
 * distribute_test.h
 *		Routines for distributed test framework.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/distribute_test.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DISTRIBUTE_TEST_H
#define DISTRIBUTE_TEST_H

#include "utils/guc.h"
#include "utils/elog.h"

// work as a switch to enable the macro TEST_STUB
#ifdef USE_ASSERT_CHECKING
#ifdef ENABLE_MULTIPLE_NODES
#define ENABLE_DISTRIBUTE_TEST
#endif

#define MAX_GID_STR_LEN (1024)
#define MAX_NAME_STR_LEN (256)
#define DEFAULT_PROBABILITY (0.004)

/* white-box log handle */
#define ereport_whitebox_domain(elevel, file, line, func, domain, rest) \
    (errstart(elevel, file, line, func, domain) ? (errfinish rest) : (void)0)

#define ereport_whitebox(elevel, file, line, func, rest) \
    ereport_whitebox_domain(elevel, file, line, func, TEXTDOMAIN, rest)

#define WHITEBOX_LOC __FILE__, __LINE__, PG_FUNCNAME_MACRO

typedef struct DistributeTestParam {
    /* use as the guc-control white-box-fault probability
     *  range [0, 100000], probability 0, 1/100000 -> 1
     */
    int guc_probability;

    /* act as the white-box checkpoint tag,
     * eg. "WHITE_BOX_ALL_RANDOM_FAILED"
     */
    char test_stub_name[MAX_NAME_STR_LEN];

    /* fault level, support ERROR and PANIC */
    int elevel;

    /* when white-box fault is WHITEBOX_WAIT, guc can provide "sleep_time"
     *  for sleep time control, if guc not contain this field, use the default value 30s
     */
    int sleep_time;
} DistributeTestParam;

/* white-box failure injection type definition */
typedef enum WhiteboxFaillueType {
    WHITEBOX_DEFAULT = 0,
    WHITEBOX_WAIT,
    WHITEBOX_REPEAT,
    WHITEBOX_USERDEFINE,
    WHITEBOX_BUTT
} WhiteboxFaillueType;

/* struct created for white-box tesing, support expansion */
typedef struct WhiteBoxInjectStruct {
    /* test location record */
    char filename[MAX_NAME_STR_LEN];
    int lineno;
    char funcname[MAX_NAME_STR_LEN];

    /* failure type, defined above */
    int failureType;

    /* trigger as the probability, 0 -1, if set as 1, definitely trigger */
    double triggerProbability;

    /* white box test info needed */
    TransactionId currentXid;
    char currentGid[MAX_GID_STR_LEN];
} WhiteBoxInjectStruct;

typedef void (*on_whitebox_callback)(WhiteBoxInjectStruct* wbinfo);
typedef void (*on_add_stub_callback)(void);

extern bool check_distribute_test_param(char** newval, void** extra, GucSource source);
extern void assign_distribute_test_param(const char* newval, void* extra);
extern bool distribute_test_stub_activator(const char* name, on_add_stub_callback function);
extern void default_error_emit(void);
extern void twophase_default_error_emit(void);
extern bool distribute_whitebox_activator(
    const char* name, WhiteBoxInjectStruct* wbinfo, on_whitebox_callback WbCallbackFunc);
extern void default_whitebox_emit(WhiteBoxInjectStruct* wbinfo);
extern bool execute_whitebox(
    const char* filename, int lineno, const char* funcname, const char* gid, int failureType, double probability);
extern DistributeTestParam* get_distribute_test_param();
extern void stub_sleep_emit(void);

#define TEST_WHITEBOX(_stub_name, _white_box_info, _call_back_func) \
    distribute_whitebox_activator((_stub_name), (_white_box_info), (_call_back_func))
#define TEST_STUB(_stub_name, _call_back_func) distribute_test_stub_activator((_stub_name), (_call_back_func))

/* White-Box Handle Definition */
#define WHITE_BOX_ALL_RANDOM_FAILED "WHITE_BOX_ALL_RANDOM_FAILED"

/* GTM exception scenes */
#define CN_LOCAL_PREPARED_FAILED_A "CN_LOCAL_PREPARED_FAILED_A"
#define CN_LOCAL_PREPARED_FAILED_B "CN_LOCAL_PREPARED_FAILED_B"
#define CN_LOCAL_PREPARED_XLOG_FAILED "CN_LOCAL_PREPARED_XLOG_FAILED"
#define CN_LOCAL_PREPARED_CLOG_FAILED "CN_LOCAL_PREPARED_CLOG_FAILED"
#define CN_PREPARED_SEND_ALL_FAILED "CN_PREPARED_SEND_ALL_FAILED"
#define CN_PREPARED_SEND_PART_FAILED "CN_PREPARED_SEND_PART_FAILED"
#define CN_PREPARED_RESPONSE_FAILED "CN_PREPARED_RESPONSE_FAILED"
#define DN_PREPARED_FAILED "DN_PREPARED_FAILED"
#define CN_PREPARED_MESSAGE_REPEAT "CN_PREPARED_MESSAGE_REPEAT"
#define CN_COMMIT_PREPARED_FAILED "CN_COMMIT_PREPARED_FAILED"
#define DN_COMMIT_PREPARED_FAILED "DN_COMMIT_PREPARED_FAILED"
#define CN_COMMIT_PREPARED_SEND_ALL_FAILED "CN_COMMIT_PREPARED_SEND_ALL_FAILED"
#define CN_COMMIT_PREPARED_SEND_PART_FAILED "CN_COMMIT_PREPARED_SEND_PART_FAILED"
#define CN_COMMIT_PREPARED_RESPONSE_FAILED "CN_COMMIT_PREPARED_RESPONSE_FAILED"
#define CN_COMMIT_PREPARED_MESSAGE_REPEAT "CN_COMMIT_PREPARED_MESSAGE_REPEAT"
#define CN_ABORT_AFTER_ALL_COMMITTED "CN_ABORT_AFTER_ALL_COMMITTED"

#define CN_ABORT_PREPARED_FAILED "CN_ABORT_PREPARED_FAILED"
#define CN_ABORT_PREPARED_SEND_ALL_FAILED "CN_ABORT_PREPARED_SEND_ALL_FAILED"
#define CN_ABORT_PREPARED_SEND_PART_FAILED "CN_ABORT_PREPARED_SEND_PART_FAILED"
#define CN_ABORT_PREPARED_RESPONSE_FAILED "CN_ABORT_PREPARED_RESPONSE_FAILED"
#define DN_ABORT_PREPARED_FAILED "DN_ABORT_PREPARED_FAILED"

#define CN_COMMIT_PREPARED_SLEEP "CN_COMMIT_PREPARED_SLEEP"
#define CN_PREPARED_SLEEP "CN_PREPARED_SLEEP"
#define DN_COMMIT_PREPARED_SLEEP "DN_COMMIT_PREPARED_SLEEP"

/* Exception during subTransaction process */
#define NON_EXEC_CN_IS_DOWN "NON_EXEC_CN_IS_DOWN"
#define CN_SAVEPOINT_SEND_ALL_FAILED "CN_SAVEPOINT_SEND_ALL_FAILED"
#define CN_SAVEPOINT_SEND_PART_FAILED "CN_SAVEPOINT_SEND_PART_FAILED"
#define CN_SAVEPOINT_RESPONSE_ALL_FAILED "CN_SAVEPOINT_RESPONSE_ALL_FAILED"
#define CN_SAVEPOINT_RESPONSE_PART_FAILED "CN_SAVEPOINT_RESPONSE_PART_FAILED"
#define CN_SAVEPOINT_BEFORE_DEFINE_LOCAL_FAILED "CN_SAVEPOINT_BEFORE_DEFINE_LOCAL_FAILED"
#define DN_SAVEPOINT_BEFORE_DEFINE_LOCAL_FAILED "DN_SAVEPOINT_BEFORE_DEFINE_LOCAL_FAILED"
#define CN_SAVEPOINT_BEFORE_PUSHXACT_FAILED "CN_SAVEPOINT_BEFORE_PUSHXACT_FAILED"
#define DN_SAVEPOINT_BEFORE_PUSHXACT_FAILED "DN_SAVEPOINT_BEFORE_PUSHXACT_FAILED"
#define CN_SAVEPOINT_AFTER_PUSHXACT_FAILED "CN_SAVEPOINT_AFTER_PUSHXACT_FAILED"
#define DN_SAVEPOINT_AFTER_PUSHXACT_FAILED "DN_SAVEPOINT_AFTER_PUSHXACT_FAILED"

#define CN_ROLLBACKTOSAVEPOINT_BEFORE_SEND_FAILED "CN_ROLLBACKTOSAVEPOINT_BEFORE_SEND_FAILED"
#define DN_ROLLBACKTOSAVEPOINT_AFTER_LOCAL_DEAL_FAILED "DN_ROLLBACKTOSAVEPOINT_AFTER_LOCAL_DEAL_FAILED"

#define CN_RELEASESAVEPOINT_BEFORE_SEND_FAILED "CN_RELEASESAVEPOINT_BEFORE_SEND_FAILED"
#define DN_RELEASESAVEPOINT_AFTER_LOCAL_DEAL_FAILED "DN_RELEASESAVEPOINT_AFTER_LOCAL_DEAL_FAILED"
#define CN_RELEASESAVEPOINT_BEFORE_LOCAL_DEAL_FAILED "CN_RELEASESAVEPOINT_BEFORE_LOCAL_DEAL_FAILED"
#define DN_RELEASESAVEPOINT_BEFORE_LOCAL_DEAL_FAILED "DN_RELEASESAVEPOINT_BEFORE_LOCAL_DEAL_FAILED"

#define CN_COMMIT_SUBXACT_BEFORE_SEND_GTM_FAILED "CN_COMMIT_SUBXACT_BEFORE_SEND_GTM_FAILED"
#define CN_COMMIT_SUBXACT_AFTER_SEND_GTM_FAILED "CN_COMMIT_SUBXACT_AFTER_SEND_GTM_FAILED"

#define CN_CANCEL_SUBQUERY_FLUSH_FAILED "CN_CANCEL_SUBQUERY_FLUSH_FAILED"
#define CN_COMMIT_BEFORE_GTM_FAILED_AND_CANCEL_FLUSH_FAILED "CN_COMMIT_BEFORE_GTM_FAILED_AND_CANCEL_FLUSH_FAILED"

#define DN_STANDBY_SLEEPIN_SYNCCOMMIT "DN_STANDBY_SLEEPIN_SYNCCOMMIT"
#define DN_XLOGFLUSH "DN_XLOGFLUSH"
#define DN_WALSEND_MAINLOOP "DN_WALSEND_MAINLOOP"
#define DN_WALRECEIVE_MAINLOOP "DN_WALRECEIVE_MAINLOOP"
#define DN_CM_NEW_CONN "DN_CM_NEW_CONN"
#define DN_PRIMARY_CHECKPOINT_KEEPXLOG "DN_PRIMARY_CHECKPOINT_KEEPXLOG"
#define DN_SLAVE_CHECKPOINT_KEEPXLOG "DN_SLAVE_CHECKPOINT_KEEPXLOG"

#endif /* DEBUG */
#endif /* DISTRIBUTE_TEST_H */
