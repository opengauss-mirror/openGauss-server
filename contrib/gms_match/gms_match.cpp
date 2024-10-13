/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * --------------------------------------------------------------------------------------
 *
 * gms_match.cpp
 *  gms_match can match two records.
 *
 *
 * IDENTIFICATION
 *        contrib/gms_match/gms_match.cpp
 * 
 * --------------------------------------------------------------------------------------
 */
#include "utils/builtins.h"
#include "gms_match.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(edit_distance);
PG_FUNCTION_INFO_V1(edit_distance_similarity);

static int min_edit_distance(char* s1, char* s2);

Datum edit_distance(PG_FUNCTION_ARGS) {
    if (PG_ARGISNULL(0) || PG_ARGISNULL(1)) {
        PG_RETURN_INT32(-1);
    }
    char *str1 = text_to_cstring(PG_GETARG_TEXT_P(0));
    char *str2 = text_to_cstring(PG_GETARG_TEXT_P(1));
    int result = min_edit_distance(str1, str2);
    pfree_ext(str1);
    pfree_ext(str2);
    PG_RETURN_INT32(result);
}

Datum edit_distance_similarity(PG_FUNCTION_ARGS) {
    if (PG_ARGISNULL(0) && PG_ARGISNULL(1)) {
        PG_RETURN_INT32(100);
    }

    if (PG_ARGISNULL(0) || PG_ARGISNULL(1)) {
        PG_RETURN_INT32(0);
    }

    char *str1 = text_to_cstring(PG_GETARG_TEXT_P(0));
    char *str2 = text_to_cstring(PG_GETARG_TEXT_P(1));
    int edit_distance = min_edit_distance(str1, str2);
    int max_len = strlen(str1) > strlen(str2) ? strlen(str1) : strlen(str2);
    int similarly = 100 - edit_distance * 100 / max_len;
    pfree_ext(str1);
    pfree_ext(str2);
    PG_RETURN_INT32(similarly);
}

static int min_edit_distance(char* s1, char* s2) {
    if (strcmp(s1, s2) == 0) {
        return 0;
    }

    int len_s1 = strlen(s1);
    int len_s2 = strlen(s2);

    int dp_array[len_s1 + 1][len_s2 + 1];
    for (int i = 0; i <= len_s1; i++) {
        for (int j = 0; j <= len_s2; j++) {
            if (i == 0) {
                dp_array[i][j] = j;
            } else if (j == 0) {
                dp_array[i][j] = i;
            } else {
                int cost = (s1[i - 1] == s2[j - 1]) ? 0 : 1;
                dp_array[i][j] = Min(Min(dp_array[i - 1][j] + 1, dp_array[i][j - 1] + 1), dp_array[i - 1][j - 1] + cost);
            }
        }
    }
    return dp_array[len_s1][len_s2];
}
