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
 * anls_opt.cpp
 *    Gauss200 analysis option information for SQL engine, used to show details in
 * performance and context information check.
 *
 * IDENTIFICATION
 *    src/common/backend/utils/misc/anls_opt.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "gs_threadlocal.h"
#include "port.h"
#include "utils/anls_opt.h"

const anls_opt_data anls_map[] = {{ANLS_ALL, "ALL"},

    /* add your analysis option following */
    {ANLS_LLVM_COMPILE, "LLVM_COMPILE"},
    {ANLS_HASH_CONFLICT, "HASH_CONFLICT"},
    {ANLS_STREAM_DATA_CHECK, "STREAM_DATA_CHECK"},

    /* add your analysis option above */
    {ANLS_MAX, "BACKEND"}};

/*
 * @Description	: Check whether analysis option is unique
 * @return		: If two analysis option have the same name, return false;
 * 				  otherwise any option name is unique, return true;
 */
bool check_anls_opt_unique(void)
{
    for (int i = 0; i <= (int)ANLS_MAX; ++i) {
        for (int j = i + 1; j <= (int)ANLS_MAX; ++j) {
            if (0 == pg_strncasecmp(anls_map[i].option_name, anls_map[j].option_name, ANLS_OPT_NAME_MAXLEN)) {
                /* Oops, two analysis option have the same name. */
                return false;
            }
        }
    }
    return true;
}

/*
 * @Description		: Find an analysis option's id according to its name.
 *					  1. analysis option name is case-insensitive;
 *					  2. simple line search is adopted because anls_map[] is not so big;
 * @in option_name	: option name
 * @return			: analysis option id
 */
AnalysisOpt get_anls_opt_id(const char* option_name)
{
    for (int dfx_opt_id = 0; dfx_opt_id < (int)ANLS_MAX; ++dfx_opt_id) {
        if (0 == pg_strncasecmp(anls_map[dfx_opt_id].option_name, option_name, ANLS_OPT_NAME_MAXLEN)) {
            return (AnalysisOpt)dfx_opt_id;
        }
    }
    /* invalid analysis option */
    return ANLS_MAX;
}

/* 1 byte --> 8 bit, so byte position is (_m/8) */
#define ANLS_BEMD_BITMAP_POS(_m) ((_m) >> 3)

/* ANLS_BEMD_BITMAP_OFF() should be in [0,7] */
#define ANLS_BEMD_BITMAP_OFF(_m) ((_m)&0x07)

/* mask is 2^x where x is in [0, 7] */
#define ANLS_BEMD_MASK(_m) (1 << ANLS_BEMD_BITMAP_OFF(_m))

/*
 * @Description	: Enable or disable all analysis options
 * @in turn_on	: true, enable all these analysis options;
 *				  false, disable all these analysis options;
 */
static void anls_opt_init(bool turn_on)
{
    const char v = turn_on ? 0xFF : 0x00;
    for (int i = 0; i < (int)ANLS_BEMD_BITMAP_SIZE; ++i) {
        u_sess->utils_cxt.analysis_options_configure[i] = v;
    }
}

/*
 * @Description	: Given option id, query whether its analysis is enabled.
 * @in anls_opt	: analysis option id
 * @return		: enable --> true; disable --> false;
 */
bool anls_opt_is_on(AnalysisOpt anls_opt)
{
    /* ANLS_MAX is a special id. at default it's on.
     * after 'off(ALL)' is set, it is turned off;
     * after 'on(ALL)' is set, it is switched to be on again.
     */
    return (
        0 != (ANLS_BEMD_MASK(anls_opt) & u_sess->utils_cxt.analysis_options_configure[ANLS_BEMD_BITMAP_POS(anls_opt)]));
}

/*
 * @Description	: enable the analysis option.
 * @in anls_opt	: analysis option id.
 */
static inline void enable_anls_opt(AnalysisOpt dfx_opt)
{
    u_sess->utils_cxt.analysis_options_configure[ANLS_BEMD_BITMAP_POS(dfx_opt)] |= ANLS_BEMD_MASK(dfx_opt);
}

/*
 * @Description	: disable the analysis option.
 * @in anls_opt	: analysis option id.
 */
static inline void disable_anls_opt(AnalysisOpt dfx_opt)
{
    u_sess->utils_cxt.analysis_options_configure[ANLS_BEMD_BITMAP_POS(dfx_opt)] &= (~ANLS_BEMD_MASK(dfx_opt));
}

/*
 * @Description	: batch enable/disable analysis options.
 * @in apply_all_opts	: enable/disable all these existing modules, its a fastpath.
 * @in turn_on	: enable analysis option which is in options.
 * @in options	: analysis options array
 * @in nopts		: size of analysis option array
 */
void anls_opt_batch_set(AnalysisOpt* options, int nopts, bool turn_on, bool apply_all_opts)
{
    if (apply_all_opts) {
        anls_opt_init(turn_on);
    } else if (turn_on) {
        for (int i = 0; i < nopts; ++i) {
            enable_anls_opt(options[i]);
        }
    } else {
        for (int i = 0; i < nopts; ++i) {
            disable_anls_opt(options[i]);
        }
    }
}
