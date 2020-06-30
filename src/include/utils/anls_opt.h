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
 * anls_opt.h
 *        analysis option information, used to show details in performance and
 *        context information check.
 * 
 * 
 * IDENTIFICATION
 *        src/include/utils/anls_opt.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SRC_INCLUDE_UTILS_ANLS_OPT_H
#define SRC_INCLUDE_UTILS_ANLS_OPT_H

/*
 * Add your analysis options as follows:
 * 1. add your analysis option before ANLS_MAX in AnalysisOpt;
 * 2. fill up anls_map[] about analysis option name, and keep their ordering;
 *
 */
enum AnalysisOpt {
    /* fastpath for all modules on/off */
    ANLS_ALL = 0,

    /* add your module id following */
    ANLS_LLVM_COMPILE,      /* print LLVM compilation time */
    ANLS_HASH_CONFLICT,     /* print length of hash link and hash confilct */
    ANLS_STREAM_DATA_CHECK, /* check buffer context after streaming */

    /* add your analysis option above */
    ANLS_MAX
};

#define ANLS_OPT_IS_VALID(_id) ((_id) >= ANLS_ALL && (_id) < ANLS_MAX)
#define ALL_OPTIONS(_id) (ANLS_ALL == (_id))

/* 1 bit <--> 1 analysis option, including ANLS_MAX. its size is
 *      ((ANLS_MAX+1)+7)/8 = ANLS_MAX/8 + 1
 */
#define ANLS_BEMD_BITMAP_SIZE (1 + (ANLS_MAX / 8))

/* max length of analysis option name */
#define ANLS_OPT_NAME_MAXLEN (32)

/* delimiter of dfx option list about GUC parameter */
#define OPTION_DELIMITER ','

/* map about analysis option id and its name */
typedef struct anls_opt_data {
    AnalysisOpt anls_opt;
    const char option_name[ANLS_OPT_NAME_MAXLEN];
} dfx_option_data;

/******************* analysis option id <--> analysis option name **********************/
extern AnalysisOpt get_anls_opt_id(const char* dfx_name);

/* Notice:
 *   declaration here only for the following inline functions,
 *   never use it within the other files directly.
 */
extern const anls_opt_data anls_map[];

/*
 * @Description	: find the analysis option name according to the option id.
 * @in dfx_opt	: analysis option id
 * @return		: analysis option name
 */
inline const char* get_valid_anls_opt_name(AnalysisOpt dfx_opt)
{
    return anls_map[dfx_opt].option_name;
}

/*******************  analysis options  **********************/
extern bool check_anls_opt_unique(void);
extern void anls_opt_batch_set(AnalysisOpt* options, int nopts, bool turn_on, bool apply_all_opts);
extern bool anls_opt_is_on(AnalysisOpt dfx_opt);

#endif /* SRC_INCLUDE_UTILS_ANLS_OPT_H */
