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
 * gsqlerr.cpp
 *     Defines the entry point for the console application.
 * 
 * IDENTIFICATION
 *        src/bin/gsqlerr/gsqlerr.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "gsqlerr_errmsg.h"
#include <stdlib.h>
#include <string.h>

#define GSQLERROR_PARAM_NUM 3
#define PARAM_NOT_FOUND (-1)
#define GSQLERROE_MAX_ERRCODE_LENGTH 6

typedef enum tag_gsqlerr_param_e {
    GSQLERROR_HELP = 0,
    GSQLERROR_VERSION,
    GSQLERROR_NO_VALUE,
    GSQLERROR_NOT_FOUND = PARAM_NOT_FOUND
} gsqlerr_param_e;

char* gsqlerr_param[] = {
    "-h", "", "print online manual", "-v", "", "print version", "[errorcode]", "", "print errorcode description"};

void gsqlerr_errinfo_print(int errcode);
extern const mppdb_detail_errmsg_t* gsqlerr_detail_error(int code);
int gsqlerr_find_err_idx(int code, const gsqlerr_err_msg_t* err_reg, int err_cnt);
extern int gsqlerr_param_find(char* str, char** param, int num);
void gsqlerr_version();
void gsqlerr_help();
extern int gsqlerr_check_errcode(char* errstr);

int main(int argc, char* argv[])
{
    int errcode = 0;
    int ret = 0;
    gsqlerr_param_e optype;

    optype = (gsqlerr_param_e)gsqlerr_param_find(argv[1], gsqlerr_param, GSQLERROR_PARAM_NUM);
    switch (optype) {
        case GSQLERROR_HELP: {
            gsqlerr_help();
            return 0;
        }

        case GSQLERROR_VERSION: {
            gsqlerr_version();
            return 0;
        }

        case GSQLERROR_NOT_FOUND: {
            if (argc > 2) {
                printf("Invalid option, please use 'gsqlerr -h' for more information.\n");
                gsqlerr_help();
                return -1;
            }

            ret = gsqlerr_check_errcode(argv[1]);
            if (0 == ret) {
                errcode = atoi(argv[1]);
                gsqlerr_errinfo_print(errcode);
            } else {
                printf("Invalid option, please use 'gsqlerr -h' for more information.\n");
                gsqlerr_help();
                return -1;
            }

            return 0;
        }

        case GSQLERROR_NO_VALUE: {
            printf("Invalid option, please use 'gsqlerr -h' for more information.\n");
            gsqlerr_help();
            return -1;
        }

        /* could not go to here */
        default: {
            return -1;
        }
    }
}

int gsqlerr_param_find(char* str, char** param, int num)
{
    int i = 0;

    if ((NULL == str) || (NULL == param))
        return GSQLERROR_NO_VALUE;

    for (i = 0; i < num; i++) {
        if (NULL == param[i * 3])
            return GSQLERROR_NOT_FOUND;

        if (0 == strcmp(str, param[i * 3])) {
            return i;
        }
    }

    return GSQLERROR_NOT_FOUND;
}

void gsqlerr_version()
{
    printf("\nV1.0 for gsqlerror. \n");
}

void gsqlerr_help()
{
    int ilasterrno = 0;

    ilasterrno = sizeof(g_gsqlerr_errors) / sizeof(gsqlerr_err_msg_t);

    printf("Display errorcode description\n");
    printf("\n");
    printf("gsqlerror [options...]\n");
    printf("\n");
    printf("Options:\n");
    printf("  -h               print online manual\n");
    printf("  -v               print version\n");
    printf("  [errorcode]      print errorcode description\n");
    printf("                   the range of errorcode is [1...%d]\n", g_gsqlerr_errors[ilasterrno - 1].ulSqlErrcode);
    printf("\n");

    return;
}

void gsqlerr_errinfo_print(int errcode)
{
    const mppdb_detail_errmsg_t* errmsg = NULL;
    int ilasterrno = 0;

    ilasterrno = sizeof(g_gsqlerr_errors) / sizeof(gsqlerr_err_msg_t);
    errmsg = gsqlerr_detail_error(errcode);
    if (errmsg == NULL) {
        printf("\nError code %d does not exist.\n", errcode);
        printf("The correct range of error code is [1...%d].\n\n", g_gsqlerr_errors[ilasterrno - 1].ulSqlErrcode);
    } else {
        printf("\n%s", "[Description]");
        printf("\n%s\n", errmsg->msg);
        printf("\n%s", "[Causes]");
        printf("\n%s\n", errmsg->cause);
        printf("\n%s", "[Action]");
        printf("\n%s\n\n", errmsg->action);
    }

    return;
}

int gsqlerr_check_errcode(char* errstr)
{
    char* pPtr = NULL;

    if (errstr == NULL)
        return -1;

    if (strlen(errstr) >= GSQLERROE_MAX_ERRCODE_LENGTH) {
        return -1;
    }

    if ((1 == strlen(errstr)) && ('0' == *errstr)) {
        return -1;
    }

    /* check errno is valid */
    pPtr = errstr;
    while (*pPtr != '\0') {
        if (('0' <= *pPtr) && ('9' >= *pPtr)) {
            pPtr++;
        } else {
            /* invalid charactor */
            return -1;
        }
    }

    return 0;
}

const mppdb_detail_errmsg_t* gsqlerr_detail_error(int code)
{
    int idx = -1;

    idx = gsqlerr_find_err_idx(code, g_gsqlerr_errors, sizeof(g_gsqlerr_errors) / sizeof(gsqlerr_err_msg_t));
    if (idx < 0) {
        /* return internal error if error message not found */
        return NULL;
    }

    return &(g_gsqlerr_errors[idx].stErrmsg);
}

int gsqlerr_find_err_idx(int code, const gsqlerr_err_msg_t* err_reg, int err_cnt)
{
    int start = 0;
    int end = err_cnt - 1;
    int mid = (start + end) / 2;

    if (err_reg == NULL) {
        return -1;
    }

    /* binary search for code */
    while (err_reg[mid].ulSqlErrcode != code) {
        if (err_reg[mid].ulSqlErrcode < code) {
            if (mid == end) {
                /* search value is greate than end value, so not found */
                return -1;
            }

            /* because mid value is less than search value, next search right */
            start = mid + 1;
        } else {
            if (mid == start) {
                /* search value is less than start value, so not found */
                return -1;
            }

            /* because mid value is greater than search value, next search left */
            end = mid - 1;
        }

        /* re-calculate middle index value */
        mid = (start + end) / 2;
    }

    return mid;
}
