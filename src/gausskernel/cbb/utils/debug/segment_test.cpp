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
 * segment_test.cpp
 *		Routines for segment test framework.
 *
 * IDENTIFICATION
 *	  src/gausskernel/cbb/utils/debug/segment_test.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/exec/execStream.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/segment_test.h"
#include "distributelayer/streamProducer.h"
#include "commands/vacuum.h"
#include "access/transam.h"
#include "threadpool/threadpool.h"


#ifdef ENABLE_SEGMENT_TEST
#define PARAM_NOTCONTAIN_ELEVEL 2
#define PARAM_CONTAIN_ELEVEL 3
#define POS_ELEVEL 2
/**
 * @Description: transform elevel from string to int value.
 * @in elevelstr -  input str which indicates level
 * @return -  no return
 */
static int get_test_elevel(const char* elevelstr)
{
    if (pg_strcasecmp(elevelstr, "ERROR") == 0)
        return ERROR;
    else if (pg_strcasecmp(elevelstr, "FATAL") == 0)
        return FATAL;
    else if (pg_strcasecmp(elevelstr, "PANIC") == 0)
        return PANIC;
    else if (pg_strcasecmp(elevelstr, "DEFAULT") == 0)
        return PANIC;
    else
        return -1;  // this means doesn't match
}

/**
 * @Description: GUC check_hook for distribute_test_param.
 * @in newval -  raw guc define input string
 * @in extra -  get the guc defined balue
 * @in source -  no use in current guc distribute_test_param
 * @return -  guc input string valid or not
 */
bool check_segment_test_param(char** newval, void** extra, GucSource source)
{
    char* rawstring = NULL;
    List* elemlist = NIL;
    SegmentTestParam* myextra = NULL;
    int param_num = 0;

    /* Need a modifiable copy of string */
    rawstring = pstrdup(*newval);
    /* Parse string into list of identifiers */
    if (!SplitIdentifierString(rawstring, ',', &elemlist)) {
        /* syntax error in list */
        GUC_check_errdetail("List syntax is invalid.");
        pfree_ext(rawstring);
        list_free_ext(elemlist);
        return false;
    }

    /* Check the number of test param */
    param_num = list_length(elemlist);
    if (!(param_num == PARAM_NOTCONTAIN_ELEVEL || param_num == PARAM_CONTAIN_ELEVEL)) {
        /* syntax error in list */
        GUC_check_errdetail("Number of test param is invalid.");
        pfree_ext(rawstring);
        list_free_ext(elemlist);
        return false;
    }

    /* Check the elevel string */
    if (get_test_elevel((char*)list_nth(elemlist, PARAM_NOTCONTAIN_ELEVEL)) == -1) {
        /* syntax error in list */
        GUC_check_errdetail("String of elevel is invalid.");
        pfree_ext(rawstring);
        list_free_ext(elemlist);
        return false;
    }

    /* Set up the "extra" struct actually used by assign_distribute_test_param */
    if (get_segment_test_param() == NULL) {
        myextra = (SegmentTestParam*)palloc0(sizeof(SegmentTestParam));
        if (myextra == NULL) {
            goto error_ret;
        }
        char* name = (char*)list_nth(elemlist, 1);
        int rc = strcpy_s(myextra->test_stub_name, MAX_NAME_STR_LEN - 1, name);
        securec_check(rc, "\0", "\0");
        myextra->guc_probability = pg_strtoint32((char*)list_nth(elemlist, 0));
        myextra->elevel = get_test_elevel((char*)list_nth(elemlist, POS_ELEVEL));
        *extra = (void*)myextra;
        goto success_ret;
    } else {
        if (pg_strcasecmp("default", (char*)list_nth(elemlist, 1)) == 0 ||
            pg_strcasecmp("", (char*)list_nth(elemlist, 1)) == 0) {
            *extra = (void*)get_segment_test_param();
            goto success_ret;
        } else {
            char* name = (char*)list_nth(elemlist, 1);
            int rc = strcpy_s(get_segment_test_param()->test_stub_name, MAX_NAME_STR_LEN - 1, name);
            securec_check(rc, "\0", "\0");
            get_segment_test_param()->guc_probability = pg_strtoint32((char*)list_nth(elemlist, 0));
            get_segment_test_param()->elevel = get_test_elevel((char*)list_nth(elemlist, POS_ELEVEL));
            *extra = (void*)get_segment_test_param();
            goto success_ret;
        }
    }

success_ret:
    pfree_ext(rawstring);
    list_free_ext(elemlist);
    return true;

error_ret:
    pfree_ext(rawstring);
    list_free_ext(elemlist);
    return false;
}

/**
 * @Description: check whether the test stub can be activated.
 * @in name -  the white-box single-point fault fire tag, is a string defined in GUC
 * @in function - callback function
 * @return -  no return
 */
bool segment_test_stub_activator(const char* name)
{
    if (g_instance.segment_test_param_instance == NULL) {
        return false;
    }

    if ((g_instance.segment_test_param_instance->guc_probability != 0) &&
        ((u_sess->stream_cxt.producer_obj == NULL) || (g_instance.segment_test_param_instance->guc_probability !=
        u_sess->stream_cxt.producer_obj->getNodeGroupIdx() + 1))) {
        return false;
    }

    if (t_thrd.proc_cxt.proc_exit_inprogress)
        return false;

    errno_t errorno = EOK;
    char test_name[1024];
    errorno = memset_s(test_name, sizeof(test_name), '\0', sizeof(test_name));
    securec_check(errorno, "", "");

    errorno = snprintf_s(
        test_name, sizeof(test_name), sizeof(test_name) - 1, "%s_%s", name, g_instance.attr.attr_common.PGXCNodeName);
    securec_check_ss(errorno, "\0", "\0");

    if (pg_strcasecmp(name, g_instance.segment_test_param_instance->test_stub_name) == 0 ||
        pg_strcasecmp(test_name, g_instance.segment_test_param_instance->test_stub_name) == 0) {
        return true;
    }

    return false;
}

/**
 * @Description: assign_segment_test_param: GUC assign_hook for distribute_test_param
 * @in newval -  raw guc define input string
 * @in extra -  assign value for macro u_sess->utils_cxt.segment_test_param
 * @return -  guc input string valid or not
 */
void assign_segment_test_param(const char* newval, void* extra)
{
    g_instance.segment_test_param_instance = (SegmentTestParam*)extra;
}

SegmentTestParam* get_segment_test_param()
{
    return (SegmentTestParam*)g_instance.segment_test_param_instance;
}

#endif
