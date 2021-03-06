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
 * errmsg_st.h
 * 
 * 
 * IDENTIFICATION
 *        src/bin/gsqlerr/errmsg_st.h
 *
 * ---------------------------------------------------------------------------------------
 */
#include <stdio.h>
#include <stdlib.h>
#include "pg_config.h"

#define FILE_NAME_MAX_LEN (32)
#define ERR_LOCATION_MAX_NUM (128)
#define ERRMSG_MAX_LEN (512)
#define SQLSTATE_MAX_LEN (6)

/* Identify the location info of errmsg. */
typedef struct {
    char szFileName[FILE_NAME_MAX_LEN]; /* __FILE__ of ereport() call */
    unsigned int ulLineno;              /* __LINE__ of ereport() call */
} mppdb_err_msg_location_t;

/* Identify the detail errmsg. */
typedef struct {
    char msg[ERRMSG_MAX_LEN];    /* description of the error code */
    char cause[ERRMSG_MAX_LEN];  /* cause cases of the error code */
    char action[ERRMSG_MAX_LEN]; /* next action for the error code */
} mppdb_detail_errmsg_t;

/*The struct of errmsg information using for elog or ereport. */
typedef struct {
    int ulSqlErrcode;                                            /* mppdb error code */
    char cSqlState[SQLSTATE_MAX_LEN];                            /* sqlstate error code */
    mppdb_err_msg_location_t astErrLocate[ERR_LOCATION_MAX_NUM]; /* location of the error cause */
} mppdb_err_msg_t;

/* The struct of errmsg information using for gsqlerr. */
typedef struct {
    int ulSqlErrcode;                 /* mppdb error code */
    char cSqlState[SQLSTATE_MAX_LEN]; /* sqlstate error code */
    mppdb_detail_errmsg_t stErrmsg;   /* detail error message of the error code */
} gsqlerr_err_msg_t;
