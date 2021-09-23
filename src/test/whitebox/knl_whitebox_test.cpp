/*
 * -------------------------------------------------------------------------
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * knl_uheaptest.cpp
 * Implementation for uheap test framework.
 *
 * IDENTIFICATION
 * src/gausskernel/cbb/utils/debug/knl_uheaptest.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "knl/knl_variable.h"
#include "utils/builtins.h"
#include "access/ustore/knl_whitebox_test.h"
#include "port.h"

const uint32 BUFSIZE = 1024;

bool VerifyBeforeTest (const char* newval) 
{
    if (g_instance.whitebox_test_param_instance &&
        strcmp(newval, g_instance.whitebox_test_param_instance->test_file_name) == 0) {
        elog(LOG, "AssignUstoreUnitTest: g_instance.whitebox_test_param_instance is already initialized");
        return false;
    }
    if (!g_instance.whitebox_test_param_instance) {
        g_instance.whitebox_test_param_instance =
            (WhiteboxTestParam *)palloc(sizeof(WhiteboxTestParam) * (MAX_UNIT_TEST));
    }
    g_instance.whitebox_test_param_instance->elevel = WARNING;
    int rc = sprintf_s(g_instance.whitebox_test_param_instance->test_file_name, MAX_NAME_STR_LEN, "%s", newval);
    securec_check_ss_c(rc, "\0", "\0");
    if (strcmp(newval, "") == 0) {
        g_instance.whitebox_test_param_instance->num_testcase = 0;
        elog(LOG, "AssignUstoreUnitTest: ustore_unit_test is empty");
        return false;
    }
    
    return true;
}


void AssignUStoreUnitTest(const char* newval, const void* extra)
{
    FILE *fp;
    char buffer[BUFSIZE];
    int i = 0;
    bool ret = false;

    ret = VerifyBeforeTest(newval);
    if (!ret) {
        return;
    }

    fp = fopen(newval, PG_BINARY_RW);
    if (!fp) {
        g_instance.whitebox_test_param_instance->num_testcase = 0;
        elog(LOG, "AssignUstoreUnitTest: ustore_unit_test does not exist");
        return;
    }
    
    while (fgets(buffer, BUFSIZE, fp) != NULL) {
        char* ptoken = NULL;
        char* psave = NULL;
        char* ptype = NULL;
        char* pvalue = NULL;
        const char* pcomma = ",";
        const char* pequal = "=";

        // skip comment
        if (!buffer || buffer[0] == '#') {
            continue;
        }
        elog(LOG, "ustore_unit_test: %s", buffer);

        // expand array of test param
        g_instance.whitebox_test_param_instance->num_testcase = i + 1;

        // test stub name
        ptoken = strtok_r(buffer, pcomma, &psave);
        int rc = sprintf_s(g_instance.whitebox_test_param_instance[i].test_stub_name, MAX_NAME_STR_LEN, "%s", ptoken);
        securec_check_ss_c(rc, "\0", "\0");
        // test stub type
        ptoken = strtok_r(NULL, pcomma, &psave);
        if (strcmp(ptoken, "PRINT") == 0) {
            g_instance.whitebox_test_param_instance[i].type = WHITEBOX_PRINT;
        } else if (strcmp(ptoken, "SLEEP") == 0) {
            g_instance.whitebox_test_param_instance[i].type = WHITEBOX_SLEEP;
        } else if (strcmp(ptoken, "SUSPEND") == 0) {
            g_instance.whitebox_test_param_instance[i].type = WHITEBOX_SUSPEND;
        } else if (strcmp(ptoken, "ENABLE") == 0) {
            g_instance.whitebox_test_param_instance[i].type = WHITEBOX_ENABLE;
        } else if (strcmp(ptoken, "DISABLE") == 0) {
            g_instance.whitebox_test_param_instance[i].type = WHITEBOX_DISABLE;
        } else if (strcmp(ptoken, "ERROR") == 0) {
            g_instance.whitebox_test_param_instance[i].type = WHITEBOX_ERROR;
        }

        // test stub attributes
        ptoken = strtok_r(NULL, pcomma, &psave);
        while (ptoken) {
            ptype = TrimStr(strtok_r(ptoken, pequal, &pvalue));
            if (!ptype || !pvalue) {
                break;
            }
            if (strcmp(ptype, "TIMEOUT") == 0) {
                g_instance.whitebox_test_param_instance[i].attributes.timeout = pg_strtoint32(pvalue);
            } else if (strcmp(ptype, "ENABLED") == 0) {
                g_instance.whitebox_test_param_instance[i].attributes.enabled = (bool) pg_strtoint32(pvalue);
            } else if (strcmp(ptype, "SKIP_ITERATION") == 0) {
                g_instance.whitebox_test_param_instance[i].attributes.skip_iteration = pg_strtoint32(pvalue);
            } else if (strcmp(ptype, "TARGET_TEST_STUB") == 0) {
                rc = sprintf_s(g_instance.whitebox_test_param_instance[i].attributes.target_test_stub, MAX_NAME_STR_LEN,
                    "%s", TrimStr(pvalue));
                securec_check_ss_c(rc, "\0", "\0");
            }
            ptoken = strtok_r(NULL, pcomma, &psave);
        }
        g_instance.whitebox_test_param_instance[i].attributes.hit_count = 0;
        i++;
    }
    fclose(fp);
}

/**
 * @Description: check whether the test stub can be activated.
 * @in name - the white-box single-point fault fire tag, is a string defined in GUC
 * @in function - callback function
 * @return - true if one or more breakpoints have been activated; false otherwise
 */
bool WhiteboxTestStubActivator(const char* name, whitebox_stub_callback function)
{
    bool result = false;
    if (g_instance.whitebox_test_param_instance == NULL) {
        return false;
    }

    if (t_thrd.proc_cxt.proc_exit_inprogress) {
        return false;
    }

    if (g_instance.whitebox_test_param_instance->num_testcase > MAX_UNIT_TEST) {
        return false;
    }

    for (int i = 0; i < g_instance.whitebox_test_param_instance->num_testcase; i++) {
        if (pg_strcasecmp(name, g_instance.whitebox_test_param_instance[i].test_stub_name) == 0) {
            result = result || (*function)(g_instance.whitebox_test_param_instance[i]);
        }
    }
    return result;
}

/**
 * @Description: default uheap-error emit callback function.
 * @in - a reference to test parameters
 * @return - true if one or more breakpoints have been activated; false otherwise
 */
bool WhiteboxDefaultErrorEmit(WhiteboxTestParam& uheapTestParamInstance)
{
    bool result = false;
    if (g_instance.whitebox_test_param_instance->elevel == PANIC) {
        exit(0);
    }

    switch (uheapTestParamInstance.type) {
        case WHITEBOX_PRINT:
            result = WhiteboxTestPrint(uheapTestParamInstance.test_stub_name);
            break;
        case WHITEBOX_SLEEP:
            result = WhiteboxTestSleep(
                uheapTestParamInstance.test_stub_name,
                uheapTestParamInstance.attributes.timeout,
                uheapTestParamInstance.attributes.skip_iteration,
                uheapTestParamInstance.attributes.hit_count);
            break;
        case WHITEBOX_SUSPEND:
            result = WhiteboxTestSuspend(
                uheapTestParamInstance.test_stub_name,
                uheapTestParamInstance.attributes.timeout,
                uheapTestParamInstance.attributes.enabled);
            break;
        case WHITEBOX_ENABLE:
            result = WhiteboxTestSetEnable(
                uheapTestParamInstance.attributes.target_test_stub, true);
            break;
        case WHITEBOX_DISABLE:
            result = WhiteboxTestSetEnable(
                uheapTestParamInstance.attributes.target_test_stub, false);
            break;
        case WHITEBOX_ERROR:
            result = WhiteboxTestSetError(
                uheapTestParamInstance.test_stub_name,
                uheapTestParamInstance.attributes.enabled);
            break;
        default:
            break;
    }
    return false; // make it always false for now, to mute the message printing at each breakpoint
}

bool WhiteboxTestPrint(const char* functionName)
{
    ereport(NOTICE, (errmsg("Hitting breakpoint %s", functionName)));
    return true;
}

bool WhiteboxTestSetError(const char* functionName, const bool& enabled)
{
    if (enabled) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("Hitting breakpoint %s", functionName)));
        return true;
    } else {
        return false;
    }
}

bool WhiteboxTestSleep(const char* functionName, int timeout, int skipIteration, int& hitCount)
{
    hitCount++;
    if (hitCount > skipIteration) {
        hitCount = 0;
        elog(g_instance.whitebox_test_param_instance->elevel,
            "Suspending at breakpoint %s timeout=%d", functionName, timeout);
        pg_usleep(timeout);
        return true;
    } else {
        return false;
    }
}

bool WhiteboxTestSuspend(const char* functionName, int timeout, const bool& enabled)
{
    int elapseTime  = 0;
    int elapseInterval  = 1000; // 1/1000 second
    if (enabled) {
        elog(g_instance.whitebox_test_param_instance->elevel, "Suspending at breakpoint %s timeout=%d",
            functionName, timeout);
        while (elapseTime < timeout) {
            // check for interrupts
            if (!enabled) {
                break;
            }
            elapseTime += elapseInterval;
            pg_usleep(elapseInterval);
        }
        return true;
    } else {
        return false;
    }
}

bool WhiteboxTestSetEnable(const char* functionName, bool enabled)
{
    for (int i = 0; i < g_instance.whitebox_test_param_instance->num_testcase; i++) {
        if (pg_strcasecmp(functionName, g_instance.whitebox_test_param_instance[i].test_stub_name) == 0) {
            g_instance.whitebox_test_param_instance[i].attributes.enabled = enabled;
            if (enabled) {
                elog(g_instance.whitebox_test_param_instance->elevel, "Enabling breakpoint %s type=%d", functionName,
                    g_instance.whitebox_test_param_instance[i].type);
            } else {
                elog(g_instance.whitebox_test_param_instance->elevel, "Disabling breakpoint %s type=%d", functionName,
                    g_instance.whitebox_test_param_instance[i].type);
            }
        }
    }
    return true;
}

