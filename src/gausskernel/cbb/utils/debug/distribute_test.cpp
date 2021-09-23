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
 * distribute_test.cpp
 *		Routines for distributed test framework.
 *
 * IDENTIFICATION
 *	  src/gausskernel/cbb/utils/debug/distribute_test.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "executor/exec/execStream.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/distribute_test.h"
#include "distributelayer/streamProducer.h"
#include "commands/vacuum.h"
#include "access/transam.h"
#include "threadpool/threadpool.h"


#ifdef ENABLE_DISTRIBUTE_TEST
#define DISTRIBUTE_UPPER_BOUND 1000000
#define DISTRIBUTE_LOWER_BOUND -1
#define DISTRIBUTE_SLEEP_TIME 30

/**
 * @Description: Run all-random anomaly white-box test with the corresponding info.
 * @in filename -  location info , record code's file name
 * @in lineno    -  location info , record code's file line number
 * @in funcname    -  location info , record code's function
 * @in gid        -  gid get from CN, xid_PGXCNodeName
 * @in failureType  -  type of failure at that point
 * @in probability  -  probability the anomaly occurs
 * @return -  true/false , execute whitebox fault constrct success or fail
 */
bool execute_whitebox(
    const char* filename, int lineno, const char* funcname, const char* gid, int failureType, double probability)
{
    errno_t errorno = EOK;
    char default_gid[MAX_GID_STR_LEN];
    double finalprobability = 0.0;
    bool result = false;
    WhiteBoxInjectStruct wbInfo = {"", 0, "", 0, 0, 0, ""};

    if (g_instance.distribute_test_param_instance == NULL) {
        return false;
    }

    if (g_instance.distribute_test_param_instance->guc_probability == DISTRIBUTE_LOWER_BOUND) {
        return false;
    }
    Assert(filename);
    Assert(funcname);
    Assert(strlen(filename) < MAX_NAME_STR_LEN);
    Assert(strlen(funcname) < MAX_NAME_STR_LEN);

    errorno = memset_s(default_gid, MAX_GID_STR_LEN, 0, MAX_GID_STR_LEN);
    securec_check(errorno, "", "");

    /* get current xid , if exists */
    wbInfo.currentXid = GetCurrentTransactionIdIfAny();

    /* check if gid is valid */
    if (gid == NULL || strlen(gid) > (MAX_GID_STR_LEN - 1))
        errorno = strcpy_s(default_gid, MAX_GID_STR_LEN, "NULL");
    else
        errorno = strcpy_s(default_gid, MAX_GID_STR_LEN, gid);

    securec_check(errorno, "", "");
    default_gid[MAX_GID_STR_LEN - 1] = '\0';

    /* control the probability by input ,range from 1/1000000 to 1 */
    finalprobability =
        (g_instance.distribute_test_param_instance->guc_probability) > 0
            ? (double)(g_instance.distribute_test_param_instance->guc_probability) / DISTRIBUTE_UPPER_BOUND
            : probability;

    if (failureType >= WHITEBOX_BUTT)
        return false;

    wbInfo.lineno = lineno;
    wbInfo.failureType = failureType;
    wbInfo.triggerProbability = finalprobability;

    errorno = strcpy_s(wbInfo.currentGid, MAX_GID_STR_LEN, default_gid);
    securec_check(errorno, "", "");
    errorno = strcpy_s(wbInfo.filename, MAX_NAME_STR_LEN, filename);
    securec_check(errorno, "", "");
    errorno = strcpy_s(wbInfo.funcname, MAX_NAME_STR_LEN, funcname);
    securec_check(errorno, "", "");

    /* white box test with WhiteBoxInjectStruct inject info */
    result = TEST_WHITEBOX(WHITE_BOX_ALL_RANDOM_FAILED, &wbInfo, default_whitebox_emit);

    return result;
}

/**
 * @Description: transform elevel from int to string
 * @in elevel -  input elevel
 * @return -  elevel string
 */
static char* get_elevel_string(int elevel)
{
    if (elevel == PANIC) {
        return "core";
    }

    if (elevel == FATAL) {
        return "fatal";
    }

    if (elevel == ERROR) {
        return "error";
    }

    if (elevel == NOTICE) {
        return "notice";
    }

    return "unkown";
}

/**
 * @Description: stub_sleep_emit callback function.
 * @in - no in
 * @return -  no return
 */
void stub_sleep_emit(void)
{
    ereport(get_distribute_test_param()->elevel,
        (errmsg("sleep_emit start time:%ds, stub_name:%s",
            get_distribute_test_param()->sleep_time,
            get_distribute_test_param()->test_stub_name)));
    HOLD_INTERRUPTS();
    /* sleep 30s or more */
    pg_usleep_retry(get_distribute_test_param()->sleep_time * 1000000L, 0);
    RESUME_INTERRUPTS();
    ereport(get_distribute_test_param()->elevel,
        (errmsg("sleep_emit end time:%ds, stub_name:%s",
            get_distribute_test_param()->sleep_time,
            get_distribute_test_param()->test_stub_name)));
}

/**
 * @Description: all-random white-box fault fire callback func, create CORE, WAIT,
                        or other USER DEFINE fault
 * @in wbInfo -  the white-box fault injection info
 * @return -  no return
 */
void default_whitebox_emit(WhiteBoxInjectStruct* wbInfo)
{
    switch (wbInfo->failureType) {
        case WHITEBOX_DEFAULT:
            /* if guc indicates panic level, thread exit and log left */
            ereport_whitebox(LOG,
                wbInfo->filename,
                wbInfo->lineno,
                wbInfo->funcname,
                (errmsg("xid is %lu, gid is %s at %s:%d, white-box testing %s situation.",
                    wbInfo->currentXid,
                    wbInfo->currentGid,
                    wbInfo->funcname,
                    wbInfo->lineno,
                    get_elevel_string(g_instance.distribute_test_param_instance->elevel))));
            if (g_instance.distribute_test_param_instance->elevel == PANIC) {
                fflush(stdout);
                fflush(stderr);
                exit(0);
            }
            break;

        case WHITEBOX_WAIT:
            /* default sleep 30s */
            ereport_whitebox(LOG,
                wbInfo->filename,
                wbInfo->lineno,
                wbInfo->funcname,
                (errmsg("xid is %lu, gid is %s at %s:%d, white-box testing wait %ds situation.",
                    wbInfo->currentXid,
                    wbInfo->currentGid,
                    wbInfo->funcname,
                    wbInfo->lineno,
                    g_instance.distribute_test_param_instance->sleep_time)));
            pg_usleep(g_instance.distribute_test_param_instance->sleep_time * 1000000);
            break;

        case WHITEBOX_REPEAT:
            /* do twice */
            ereport_whitebox(LOG,
                wbInfo->filename,
                wbInfo->lineno,
                wbInfo->funcname,
                (errmsg("xid is %lu, gid is %s, white-box testing send twice situation.",
                    wbInfo->currentXid,
                    wbInfo->currentGid)));

            break;

        case WHITEBOX_USERDEFINE:
            /* user define fault */
            ereport_whitebox(LOG,
                wbInfo->filename,
                wbInfo->lineno,
                wbInfo->funcname,
                (errmsg("xid is %lu, gid is %s, white-box testing user defined situation.",
                    wbInfo->currentXid,
                    wbInfo->currentGid)));
            break;

        default:
            break;
    }
}

/**
 * @Description: check whether the injected white-box failure will be fired
 * @in name -  the white-box all-random fault fire tag, is a string defined in GUC
 * @in wbInfo -  the white-box fault injection info
 * @in WbCallbackFunc - callback function
 * @return -  activator execute success or skip
 */
bool distribute_whitebox_activator(const char* name, WhiteBoxInjectStruct* wbInfo, on_whitebox_callback WbCallbackFunc)
{
    double randnum = 0.0;

    if (t_thrd.proc_cxt.proc_exit_inprogress || wbInfo == NULL)
        return false;

    if (pg_strcasecmp(name, g_instance.distribute_test_param_instance->test_stub_name) == 0) {
        /*
         * see whether trigger as the probability,
         * generate a random number, compare with input
         * anl_random_fract() guarantee a (0,1) uniformly distributed value
         */
        randnum = anl_random_fract();

        if (randnum >= wbInfo->triggerProbability) {
            /* not execute */
            return false;
        }

        (*WbCallbackFunc)(wbInfo);
        return true;
    }

    return false;
}

/**
 * @Description: default error emit callback function.
 * @in - no in
 * @return -  no return
 */
void default_error_emit(void)
{
    ereport(g_instance.distribute_test_param_instance->elevel, (errmsg("distribute test")));
}

/**
 * @Description: default twophase-error emit callback function.
 * @in - no in
 * @return -  no return
 */
void twophase_default_error_emit(void)
{
    if (g_instance.distribute_test_param_instance->elevel == PANIC)
        exit(0);
}

/**
 * @Description: check whether the test stub can be activated.
 * @in name -  the white-box single-point fault fire tag, is a string defined in GUC
 * @in function - callback function
 * @return -  no return
 */
bool distribute_test_stub_activator(const char* name, on_add_stub_callback function)
{
    
    if (g_instance.distribute_test_param_instance == NULL) {
        return false;
    }

    if ((g_instance.distribute_test_param_instance->guc_probability != 0) &&
        ((u_sess->stream_cxt.producer_obj == NULL) || (g_instance.distribute_test_param_instance->guc_probability !=
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

    if (pg_strcasecmp(name, g_instance.distribute_test_param_instance->test_stub_name) == 0 ||
        pg_strcasecmp(test_name, g_instance.distribute_test_param_instance->test_stub_name) == 0) {
        (*function)();
        return true;
    }

    return false;
}

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
    else if (pg_strcasecmp(elevelstr, "NOTICE") == 0)
        return NOTICE;
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
bool check_distribute_test_param(char** newval, void** extra, GucSource source)
{
    char* rawstring = NULL;
    List* elemlist = NIL;
    DistributeTestParam* myextra = NULL;
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
    if (!(param_num == 3 || param_num == 4)) {
        /* syntax error in list */
        GUC_check_errdetail("Number of test param is invalid.");
        pfree_ext(rawstring);
        list_free_ext(elemlist);
        return false;
    }

    /* Check the elevel string */
    if (get_test_elevel((char*)list_nth(elemlist, 2)) == -1) {
        /* syntax error in list */
        GUC_check_errdetail("String of elevel is invalid.");
        pfree_ext(rawstring);
        list_free_ext(elemlist);
        return false;
    }

    /* Set up the "extra" struct actually used by assign_distribute_test_param */
    if (get_distribute_test_param() == NULL) {
        myextra = (DistributeTestParam*)palloc0(sizeof(DistributeTestParam));
        if (myextra == NULL) {
            goto error_ret;
        }
        char* name = (char*)list_nth(elemlist, 1);
        int rc = strcpy_s(myextra->test_stub_name, MAX_NAME_STR_LEN - 1, name);
        securec_check(rc, "\0", "\0");
        myextra->guc_probability = pg_strtoint32((char*)list_nth(elemlist, 0));
        myextra->elevel = get_test_elevel((char*)list_nth(elemlist, 2));
        myextra->sleep_time = (param_num == 4) ? pg_strtoint32((char*)list_nth(elemlist, 3)) : DISTRIBUTE_SLEEP_TIME;
        *extra = (void*)myextra;
        goto success_ret;
    } else {
        if (pg_strcasecmp("default", (char*)list_nth(elemlist, 1)) == 0 ||
            pg_strcasecmp("", (char*)list_nth(elemlist, 1)) == 0) {
            *extra = (void*)get_distribute_test_param();
            goto success_ret;
        } else {
            char* name = (char*)list_nth(elemlist, 1);
            int rc = strcpy_s(get_distribute_test_param()->test_stub_name, MAX_NAME_STR_LEN - 1, name);
            securec_check(rc, "\0", "\0");
            get_distribute_test_param()->guc_probability = pg_strtoint32((char*)list_nth(elemlist, 0));
            get_distribute_test_param()->elevel = get_test_elevel((char*)list_nth(elemlist, 2));
            get_distribute_test_param()->sleep_time =
                (param_num == 4) ? pg_strtoint32((char*)list_nth(elemlist, 3)) : DISTRIBUTE_SLEEP_TIME;
            *extra = (void*)get_distribute_test_param();
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
 * @Description: assign_distribute_test_param: GUC assign_hook for distribute_test_param
 * @in newval -  raw guc define input string
 * @in extra -  assign value for macro u_sess->utils_cxt.distribute_test_param
 * @return -  guc input string valid or not
 */
void assign_distribute_test_param(const char* newval, void* extra)
{
    g_instance.distribute_test_param_instance = (DistributeTestParam*)extra;
}

DistributeTestParam* get_distribute_test_param()
{
    return (DistributeTestParam*)g_instance.distribute_test_param_instance;
}

#endif
