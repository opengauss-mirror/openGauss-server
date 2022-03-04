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
 * be_module.cpp
 *
 * IDENTIFICATION
 *    src/distribute/cm/cm_common/be_module.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "cm/be_module.h"

const module_data module_map[] = {
    {MOD_ALL, "ALL"},
    /* add your module name following */
    /* add your module name above */
    {MOD_CMA, "CMAGENT"},
    {MOD_CMS, "CMSERVER"},
    {MOD_CMCTL, "CMCTL"},
    {MOD_OMMONITER, "OMMONITER"},
    {MOD_MAX, "BACKEND"}
};
