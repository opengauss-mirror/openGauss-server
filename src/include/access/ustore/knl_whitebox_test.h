/*
 * -------------------------------------------------------------------------
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
 * knl_uheaptest.h
 *      interface for uheap test framework.
 *
 * IDENTIFICATION
 *      src/include/utils/knl_uheaptest.h
 *
 * -------------------------------------------------------------------------
 */

#define MAX_NAME_STR_LEN (256)
#define MAX_UNIT_TEST (32)

typedef enum WhiteboxType {
    WHITEBOX_PRINT,           // Print warning message when hitting the breakpoint
    WHITEBOX_SLEEP,           // Add a delay at the breakpoint
    WHITEBOX_SUSPEND,         // Suspend at breakpoint until timeout or a resume has been triggered
    WHITEBOX_ENABLE,          // Trigger enabling another breakpoint
    WHITEBOX_DISABLE,         // Trigger disabling another breakpoint, for example, resume a suspended breakpoint
    WHITEBOX_ASSERT,          // Cause an assertion failure
    WHITEBOX_SEND_SIGNAL,         // Send signal
    WHITEBOX_CHANGE_RETURN_CODE,  // Change return code of the function
    WHITEBOX_ERROR
} WhiteboxType;

typedef struct WhiteboxAttribute {
    // passed by script
    int timeout;
    int skip_iteration;
    int return_code;
    char target_test_stub[MAX_NAME_STR_LEN];

    // updated in runtime
    bool enabled;
    int hit_count;
    char message[MAX_NAME_STR_LEN];
} WhiteboxAttribute;

typedef struct WhiteboxTestParam {
    /* act as the white-box checkpoint tag,
     * eg. "WHITE_BOX_ALL_RANDOM_FAILED"
     */
    char test_stub_name[MAX_NAME_STR_LEN];
    char test_file_name[MAX_NAME_STR_LEN];

    /* fault level, support ERROR and PANIC */
    int elevel;

    /* total number of testcases */
    int num_testcase;

    /* type of action to take at breakpoint */
    WhiteboxType type;

    /* list of parameters */
    WhiteboxAttribute attributes;
} WhiteboxTestParam;

typedef bool (*whitebox_stub_callback)(WhiteboxTestParam&);

extern char* TrimStr(const char* str);

extern void AssignUStoreUnitTest(const char* newval, const void* extra);
extern bool WhiteboxTestStubActivator(const char* name, whitebox_stub_callback function);
extern bool WhiteboxDefaultErrorEmit(WhiteboxTestParam& whitebox_test_param_instance);

extern bool WhiteboxTestPrint(const char* functionName);
extern bool WhiteboxTestSleep(const char* functionName, int timeout, int num_skip, int& hit_count);
extern bool WhiteboxTestSuspend(const char* functionName, int timeout, const bool& enabled);
extern bool WhiteboxTestSetEnable(const char* functionName, bool enable);
extern bool WhiteboxTestSetError(const char* functionName, const bool& enabled);

#ifdef ENABLE_WHITEBOX
#define WHITEBOX_TEST_STUB(_stub_name, _call_back_func) WhiteboxTestStubActivator((_stub_name), (_call_back_func))
#else
#define WHITEBOX_TEST_STUB(_stub_name, _call_back_func) {}
#endif

#define UHEAP_INSERT_FAILED "UHEAP_INSERT"
#define UHEAP_MULTI_INSERT_FAILED "UHEAP_MULTI_INSERT"
#define UHEAP_DELETE_FAILED "UHEAP_DELETE"
#define UHEAP_UPDATE_FAILED "UHEAP_UPDATE"
#define UHEAP_LOCK_TUPLE_FAILED "UHEAP_LOCK_TUPLE"
#define UHEAP_FETCH_FAILED "UHEAP_FETCH"
#define UHEAP_PAGE_PRUNE_FAILED "UHEAP_PAGE_PRUNE"
#define UHEAP_REPAIR_FRAGMENTATION_FAILED "UHEAP_REPAIR_FRAGMENTATION"

#define UHEAP_XLOG_INSERT_FAILED "UHEAP_XLOG_INSERT"
#define UHEAP_XLOG_DELETE_FAILED "UHEAP_XLOG_DELETE"
#define UHEAP_XLOG_LOCK_FAILED "UHEAP_XLOG_LOCK"
#define UHEAP_XLOG_CLEAN_FAILED "UHEAP_XLOG_CLEAN"
#define UHEAP_XLOG_UPDATE_FAILED "UHEAP_XLOG_UPDATE"
#define UHEAP_XLOG_MULTI_INSERT_FAILED "UHEAP_XLOG_MULTI_INSERT"
#define UHEAP_XLOG_FREEZE_TD_SLOT_FAILED "UHEAP_XLOG_FREEZE_TD_SLOT"
#define UHEAP_XLOG_INVALID_TD_SLOT_FAILED "UHEAP_XLOG_INVALID_TD_SLOT"

#define UHEAP_UNDO_ACTION_FAILED "UHEAP_UNDO_ACTION"
#define UHEAP_LAZY_VACUUM_FAILED "UHEAP_LAZY_VACUUM"
#define UHEAP_LAZY_VACUUM_REL_FAILED "UHEAP_LAZY_VACUUM_REL"
#define UHEAP_LAZY_SCAN_FAILED "UHEAP_LAZY_SCAN"

#define UHEAP_DEFORM_TUPLE_FAILED "UHEAP_DEFORM_TUPLE"
#define UHEAP_FORM_TUPLE_FAILED "UHEAP_FORM_TUPLE"
#define UHEAP_SEARCH_BUFFER_FAILED "UHEAP_SEARCH_BUFFER"

#define UHEAP_TOAST_DELETE_FAILED "UHEAP_TOAST_DELETE"
#define UHEAP_TOAST_INSERT_UPDATE_FAILED "UHEAP_TOAST_INSERT_UPDATE"

#define UNDO_UPDATE_BEFORE_UPDATE_FAILED "UNDO_UPDATE_BEFORE_UPDATE"
#define UNDO_UPDATE_AFTER_UPDATE_FAILED "UNDO_UPDATE_AFTER_UPDATE"
#define UNDO_RECYCL_ESPACE_FAILED "UNDO_RECYCL_ESPACE"
#define UNDO_EXTEND_FILE_FAILED "UNDO_EXTEND_FILE"
#define UNDO_CHECK_DIRECTORY_FAILED "UNDO_CHECK_DIRECTORY"
#define UNDO_CHECK_FILE_DIRECTORY_FAILED "UNDO_CHECK_FILE_DIRECTORY"
#define UNDO_GET_FILE_BLOCKS_FAILED "UNDO_GET_FILE_BLOCKS"
#define UNDO_READ_FILE_FAILED "UNDO_READ_FILE"
#define UNDO_OPEN_FILE_FAILED "UNDO_OPEN_FILE"
#define UNDO_WRITE_FILE_FAILED "UNDO_WRITE_FILE"
#define UNDO_SWITCH_ZONE_FAILED "UNDO_SWITCH_ZONE"
#define UNDO_EXTEND_LOG_FAILED "UNDO_EXTEND_LOG"
#define UNDO_USED_FAILED "UNDO_USED"
#define UNDO_UNLINK_LOG_FAILED "UNDO_UNLINK_LOG"
#define UNDO_CHECK_RECYCLE_FAILED "UNDO_CHECK_RECYCLE"
#define UNDO_PREPARE_SWITCH_FAILED "UNDO_PREPARE_SWITCH"
#define UNDO_CLEAN_SPACE_FAILED "UNDO_CLEAN_SPACE"
#define UNDO_CLEAN_SLOT_SPACE_FAILED "UNDO_CLEAN_SLOT_SPACE"
#define UNDO_ALLOCATE_ZONE_FAILED "UNDO_ALLOCATE_ZONE"
#define UNDO_RELEASE_ZONE_FAILED "UNDO_RELEASE_ZONE"
#define UNDO_GET_ZONE_FAILED "UNDO_GET_ZONE"
#define UNDO_ALLOCATE_TRANS_SLOT_FAILED "UNDO_ALLOCATE_TRANS_SLOT"
#define UNDO_PREPAR_ZONE_FAILED "UNDO_PREPAR_ZONE"
#define UNDO_UPDATE_TRANSACTION_SLOT_FAILED "UNDO_UPDATE_TRANSACTION_SLOT"
#define UNDO_UPDATE_SLOT_FAILED "UNDO_UPDATE_SLOT"
#define UNDO_CHECK_LAST_RECORD_SIZE_FAILED "UNDO_CHECK_LAST_RECORD_SIZE"
#define UNDO_PREPARE_RECORD_FAILED "UNDO_PREPARE_RECORD"
#define UNDO_INSERT_PREPARED_FAILED "UNDO_INSERT_PREPARED"
#define UNDO_ALLOCATE_ZONE_BEFO_XID_FAILED "UNDO_ALLOCATE_ZONE_BEFO_XID"
#define UNDO_PREPARE_TRANSACTION_SLOT_FAILED "UNDO_PREPARE_TRANSACTION_SLOT"
#define UNDO_FETCH_TRANSACTION_SLOT_FAILED "UNDO_FETCH_TRANSACTION_SLOT"

#define XLOG_FILE_INIT_REUSE_FAILED "XLOG_FILE_INIT_REUSE"
#define XLOG_FILE_INIT_ZEROFILL_NEW_FAILED "XLOG_FILE_INIT_ZEROFILL_NEW"
#define XLOG_FILE_INIT_ZEROFILL_WRITE_FAILED "XLOG_FILE_INIT_ZEROFILL_WRITE"
#define XLOG_FILE_INIT_ZEROFILL_SYNC_FAILED "XLOG_FILE_INIT_ZEROFILL_SYNC"
#define XLOG_FILE_INIT_CLOSE_FAILED "XLOG_FILE_INIT_CLOSE"
#define XLOG_FILE_INIT_OPEN_TARGET_FAILED "XLOG_FILE_OPEN_TARGET_FAILED"
#define XLOG_FILE_WRITE_INIT_FAILED "XLOG_FILE_WRITE_INIT"