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
 * reg_cmkem_manager_main.cpp
 *      you can register your own key management tool/component/service here. 
 *      what's more, you can choose which to use according to the version and usage scenarios.
 * 
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_hooks/cmk_entity_manager_hooks/reg_cmkem_manager_main.cpp
 *
 * -------------------------------------------------------------------------
 */


#include "reg_cmkem_manager_main.h"

#include "cmkem_version_control.h"
#include "register_gs_ktool.h"
#include "register_huawei_kms.h"
#include "register_local_kms.h"
#include "register_gram_check.h"

static bool has_reg_hookfunc = false;

CmkemErrCode reg_all_cmk_entity_manager()
{
    if (has_reg_hookfunc) {
        return CMKEM_SUCCEED;
    }
    
#ifdef ENABLE_GS_KTOOL
    if (reg_cmke_manager_gs_ktool_main() < 0) {
        return CMKEM_REG_CMK_MANAGER_ERR;
    }
#endif

#ifdef ENABLE_HUAWEI_KMS
    if (reg_cmke_manager_huwei_kms_main() < 0) {
        return CMKEM_REG_CMK_MANAGER_ERR;
    }
#endif

#ifdef ENABLE_LOCAL_KMS
    if (reg_cmke_manager_local_kms_main() < 0) {
        return CMKEM_REG_CMK_MANAGER_ERR;
    }
#endif

    /* 
     * register new key management tool/component/service here
     */

    if (reg_grammar_check_main() < 0) {
        return CMKEM_REG_CMK_MANAGER_ERR;
    }

    has_reg_hookfunc = true;
    return CMKEM_SUCCEED;
}
