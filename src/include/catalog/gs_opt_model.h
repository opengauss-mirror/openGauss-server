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
 * gs_opt_model.h
 * 
 * 
 * 
 * IDENTIFICATION
 *        src/include/catalog/gs_opt_model.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GS_OPT_MODEL_H
#define GS_OPT_MODEL_H

#include "catalog/genbki.h"

#define OptModelRelationId              9998
#define OptModelRelationId_Rowtype_Id   9996

CATALOG(gs_opt_model,9998) BKI_WITHOUT_OIDS BKI_SCHEMA_MACRO
{
    /* index information */
    NameData        template_name;
    NameData        model_name;             /* model's name */
    NameData        datname;
    /* connection information */
    NameData        ip;
    int4            port;
    /* user-set configuration */
    int4            max_epoch;
    float4          learning_rate;
    float4          dim_red;
    int4            hidden_units;
    int4            batch_size;
    /* model info, to be filled after training. */
    int4            feature_size;           /* size of the feature set */
    bool            available;
    bool            is_training;
#ifdef CATALOG_VARLEN                       /* variable-length fields start here */
    char            label[1];               /* meaning of label:    'S': Startup time
                                             *                      'T': Total time
                                             *                      'R': Rows
                                             *                      'M': Memory */
    int8            max[1];                 /* keep tracks of the max values occured for each label */
    float4          acc[1];                 /* ratio error accuracy */
    text            description;                   /* description */
#endif /* CATALOG_VARLEN */

} FormData_gs_opt_model;

typedef FormData_gs_opt_model *Form_gs_opt_model;

/* ----------------
 *      compiler constants for gs_opt_model
 * ----------------
 */
#define Natts_gs_opt_model                   17
#define Anum_gs_opt_model_template_name      1
#define Anum_gs_opt_model_model_name         2
#define Anum_gs_opt_model_datname            3
#define Anum_gs_opt_model_ip                 4
#define Anum_gs_opt_model_port               5
#define Anum_gs_opt_model_max_epoch          6
#define Anum_gs_opt_model_learning_rate      7
#define Anum_gs_opt_model_dim_red            8
#define Anum_gs_opt_model_hidden_units       9
#define Anum_gs_opt_model_batch_size         10
#define Anum_gs_opt_model_feature_size       11
#define Anum_gs_opt_model_available          12
#define Anum_gs_opt_model_is_training        13
#define Anum_gs_opt_model_label              14
#define Anum_gs_opt_model_max                15
#define Anum_gs_opt_model_acc                16
#define Anum_gs_opt_model_description        17

#define gs_opt_model_label_length            4
#define gs_opt_model_label_startup_time      'S'
#define gs_opt_model_label_total_time        'T'
#define gs_opt_model_label_rows              'R'
#define gs_opt_model_label_peak_memory       'M'

#define LABEL_START_TIME_INDEX               0
#define LABEL_TOTAL_TIME_INDEX               1
#define LABEL_ROWS_INDEX                     2
#define LABEL_PEAK_MEMEORY_INDEX             3

#endif   /* gs_opt_model_H */