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

typedef enum UHeapWhiteboxType {
    UHEAP_WHITEBOX_PRINT,           // Print warning message when hitting the breakpoint
    UHEAP_WHITEBOX_SLEEP,           // Add a delay at the breakpoint
    UHEAP_WHITEBOX_SUSPEND,         // Suspend at breakpoint until timeout or a resume has been triggered
    UHEAP_WHITEBOX_ENABLE,          // Trigger enabling another breakpoint
    UHEAP_WHITEBOX_DISABLE,         // Trigger disabling another breakpoint, for example, resume a suspended breakpoint
    UHEAP_WHITEBOX_ASSERT,          // Cause an assertion failure
    UHEAP_WHITEBOX_SEND_SIGNAL,         // Send signal
    UHEAP_WHITEBOX_CHANGE_RETURN_CODE   // Change return code of the function
} UheapWhiteboxType;

typedef struct UHeapWhiteboxAttribute {
    // passed by script
    int timeout;
    int skip_iteration;
    int return_code;
    char target_test_stub[MAX_NAME_STR_LEN];

    // updated in runtime
    bool enabled;
    int hit_count;
    char message[MAX_NAME_STR_LEN];
} UHeapWhiteboxAttribute;

typedef struct UHeapTestParam {
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
    UheapWhiteboxType type;

    /* list of parameters */
    UHeapWhiteboxAttribute attributes;
} UHeapTestParam;

typedef bool (*uheap_stub_callback)(UHeapTestParam&);

extern char* TrimStr(const char* str);

extern void AssignUStoreUnitTest(const char* newval, void* extra);
extern bool UHeapTestStubActivator(const char* name, uheap_stub_callback function);
extern bool UHeapDefaultErrorEmit(UHeapTestParam& uheap_test_param_instance);

extern bool UHeapTestPrint(char* functionName);
extern bool UHeapTestSleep(char* functionName, int timeout, int num_skip, int& hit_count);
extern bool UHeapTestSuspend(char* functionName, int timeout, bool& enabled);
extern bool UHeapTestSetEnable(char* functionName, bool enable);

#define UHEAP_TEST_STUB(_stub_name, _call_back_func) UHeapTestStubActivator((_stub_name), (_call_back_func))

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

