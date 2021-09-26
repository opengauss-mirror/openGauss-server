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
 * randomplan.cpp
 *	  Generate random plan according to random mode and seed.
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/util/randomplan.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include <limits.h>
#include "fmgr.h"
#include "optimizer/randomplan.h"

#define RAND48_SEED_0 0x330e
#define RAND48_SEED_1 0xabcd
#define RAND48_SEED_2 0x1234
/* The inital seed factor for choose random path */
static unsigned short path_inital_seed_factor[3] = {RAND48_SEED_0, RAND48_SEED_1, RAND48_SEED_2};

#define path_reset_srandom(xseed) pg_reset_srand48((unsigned short*)(xseed))
#define path_get_srandom() (unsigned short*)pg_get_srand48()

/*****************************************************************************
 *		FUNCTION
 *****************************************************************************/
/* Generate next random number using choose random path */
static long get_next_path_random(unsigned int seed)
{
    long random = 0;
    unsigned short zero[3] = {0};
    errno_t ret = 0;

    if (0 == memcmp(u_sess->opt_cxt.path_current_seed_factor, zero, sizeof(zero))) {
        path_reset_srandom(path_inital_seed_factor);
        if (seed > 0) {
            gs_srandom(seed);
        }
    } else {
        path_reset_srandom(u_sess->opt_cxt.path_current_seed_factor);
    }

    random = gs_random();
    ret = memcpy_s(u_sess->opt_cxt.path_current_seed_factor,
        sizeof(u_sess->opt_cxt.path_current_seed_factor),
        path_get_srandom(),
        sizeof(u_sess->opt_cxt.path_current_seed_factor));
    securec_check(ret, "\0", "\0");

    return random;
}

/* It should set path_current_seed_factor is zero when create plan finish. */
void set_path_seed_factor_zero()
{
    errno_t ret = 0;

    ret = memset_s(u_sess->opt_cxt.path_current_seed_factor,
        sizeof(u_sess->opt_cxt.path_current_seed_factor),
        0,
        sizeof(u_sess->opt_cxt.path_current_seed_factor));
    securec_check(ret, "\0", "\0");
}

/* Choose random option */
int choose_random_option(int optionnum)
{
    unsigned int random_pathno = 0;
    long random = 0;

    if (optionnum == 0) {
        ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by zero")));
    }

    random = get_next_path_random(u_sess->opt_cxt.plan_current_seed);
    random_pathno = random % optionnum;

    return random_pathno;
}

/* Set initalize plan seed according to guc plan_mode_seed */
void set_inital_plan_seed()
{
    /* Save the previous seed */
    u_sess->opt_cxt.plan_prev_seed = u_sess->opt_cxt.plan_current_seed;

    if (u_sess->attr.attr_sql.plan_mode_seed == OPTIMIZE_PLAN) {
        u_sess->opt_cxt.plan_current_seed = 0;
    } else {
        /* We should reset u_sess->opt_cxt.path_current_seed_factor as zero when execute one query. */
        set_path_seed_factor_zero();

        if (u_sess->attr.attr_sql.plan_mode_seed == RANDOM_PLAN_DEFAULT) {
            u_sess->opt_cxt.plan_current_seed = gs_random() % INT_MAX + 1;
        } else { /* plan_mode_seed>0, valid range for parameter "plan_mode_seed" (-1 .. 2147483647) */
            u_sess->opt_cxt.plan_current_seed = u_sess->attr.attr_sql.plan_mode_seed;
        }
    }
}

/* Get u_sess->opt_cxt.plan_current_seed for pg_proc supply to get current plan seed which using reproduce
     the plan stable */
uint32 get_inital_plan_seed()
{
    return u_sess->opt_cxt.plan_current_seed;
}

/* Return initalized plan seed */
Datum get_plan_seed(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT64(u_sess->opt_cxt.plan_prev_seed);
}

/* Add random path for cheapest_startup_path/cheapest_total_path/cheapest_param_path to list. */
static List* set_random_cheapest_path_list(const List* pathlist, List* cheapest_path_list, uint32* random_pathno)
{
    unsigned int pathnum = 0;
    long random = 0; /* pick up non param path */
    Path* randompath = NULL;

    pathnum = list_length(pathlist);
    if (pathnum == 0)
        return cheapest_path_list;

    random = get_next_path_random(u_sess->opt_cxt.plan_current_seed);
    *random_pathno = random % pathnum;
    randompath = (Path*)list_nth(pathlist, *random_pathno);
    cheapest_path_list = list_append_unique_ptr(cheapest_path_list, randompath);

    return cheapest_path_list;
}

/* Set random path if guc plan_mode_seed is not 0 */
List* get_random_path(RelOptInfo* parent_rel, Path** cheapest_startup_path, Path** cheapest_total_path)
{
    ListCell* p = NULL;
    List* cheapest_path_list = NIL;
    List* nonParamPathlist = NIL;
    List* ParamPathlist = NIL;
    Path* path = NULL;
    unsigned int random_pathno = 0;

    /* Handle random cheapest gather path */
    if (parent_rel->pathlist == NIL && parent_rel->cheapest_gather_path != NULL) {
        *cheapest_startup_path = parent_rel->cheapest_gather_path;
        *cheapest_total_path = parent_rel->cheapest_gather_path;
        cheapest_path_list = lappend(cheapest_path_list, parent_rel->cheapest_gather_path);

        return cheapest_path_list;
    }

    foreach (p, parent_rel->pathlist) {
        path = (Path*)lfirst(p);

        /* We only consider unparameterized paths in this step */
        if (path->param_info) {
            ParamPathlist = lappend(ParamPathlist, path);
            continue;
        }

        nonParamPathlist = lappend(nonParamPathlist, path);
    }

    /* when only ParamPath exist */
    if (nonParamPathlist == NIL) {
        cheapest_path_list = set_random_cheapest_path_list(ParamPathlist, cheapest_path_list, &random_pathno);
        *cheapest_startup_path = (Path*)list_nth(ParamPathlist, random_pathno);

        cheapest_path_list = set_random_cheapest_path_list(ParamPathlist, cheapest_path_list, &random_pathno);
        *cheapest_total_path = (Path*)list_nth(ParamPathlist, random_pathno);

        list_free_ext(ParamPathlist);
        return cheapest_path_list;
    }

    /* Choose cheapest_startup_path from nonParamPathlist and add it into cheapest_path_list. */
    cheapest_path_list = set_random_cheapest_path_list(nonParamPathlist, cheapest_path_list, &random_pathno);
    *cheapest_startup_path = (Path*)list_nth(nonParamPathlist, random_pathno);

    /* Choose cheapest_total_path from nonParamPathlist and add it into cheapest_path_list. */
    cheapest_path_list = set_random_cheapest_path_list(nonParamPathlist, cheapest_path_list, &random_pathno);
    *cheapest_total_path = (Path*)list_nth(nonParamPathlist, random_pathno);
    list_free_ext(nonParamPathlist);

    /* Choose random path from ParamPathlist and add it into cheapest_path_list. */
    if (ParamPathlist) {
        cheapest_path_list = set_random_cheapest_path_list(ParamPathlist, cheapest_path_list, &random_pathno);
        list_free_ext(ParamPathlist);
    }

    /* find random path from non param pathlist */
    return cheapest_path_list;
}

char* get_random_plan_string()
{
    char* randomPlanInfo = NULL;

    /* show random plan seed if plan_mode_seed is not OPTIMIZE_PLAN */
    if ((u_sess->attr.attr_sql.plan_mode_seed != OPTIMIZE_PLAN) && u_sess->opt_cxt.plan_current_seed) {
        randomPlanInfo = (char*)palloc0(NAMEDATALEN);
        int rc = snprintf_s(
            randomPlanInfo, NAMEDATALEN, NAMEDATALEN - 1, "Query random seed: %u", u_sess->opt_cxt.plan_current_seed);
        securec_check_ss(rc, "\0", "\0");
    }

    return randomPlanInfo;
}
