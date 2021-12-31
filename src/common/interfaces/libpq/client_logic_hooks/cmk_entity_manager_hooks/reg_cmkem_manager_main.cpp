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

#include<pthread.h>

#include "cmkem_version_control.h"
#include "register_gs_ktool.h"
#include "register_huawei_kms.h"
#include "register_local_kms.h"
#include "register_gram_check.h"

static bool has_reg_hookfunc = false;
static pthread_mutex_t reg_lock = {0};

CmkemErrCode reg_all_cmk_entity_manager()
{
    static bool has_init_lock = false;
    
    if (!has_init_lock) {
        if (pthread_mutex_init(&reg_lock, NULL) != 0) {
            cmkem_errmsg("failed to init the lock of cmk entity manager register.");
            return CMKEM_REG_CMK_MANAGER_ERR;
        }

        has_init_lock = true;
    }

    if (pthread_mutex_lock(&reg_lock) != 0) {
        return CMKEM_REG_CMK_MANAGER_ERR;
    }

    if (has_reg_hookfunc) {
        pthread_mutex_unlock(&reg_lock);
        return CMKEM_SUCCEED;
    }
    
#ifdef ENABLE_GS_KTOOL
    if (reg_cmke_manager_gs_ktool_main() < 0) {
        goto err_with_unlock;
    }
#endif

#ifdef ENABLE_HUAWEI_KMS
    if (reg_cmke_manager_huwei_kms_main() < 0) {
        goto err_with_unlock;
    }
#endif

#ifdef ENABLE_LOCAL_KMS
    if (reg_cmke_manager_local_kms_main() < 0) {
        goto err_with_unlock;
    }
#endif

    /* 
     * register new key management tool/component/service here
     */

    if (reg_grammar_check_main() < 0) {
        goto err_with_unlock;
    }

    has_reg_hookfunc = true;
    pthread_mutex_unlock(&reg_lock);

    return CMKEM_SUCCEED;

err_with_unlock:
    pthread_mutex_unlock(&reg_lock);
    return CMKEM_REG_CMK_MANAGER_ERR;
}

void exit_all_cmk_entity_manager()
{
    if (pthread_mutex_lock(&reg_lock) != 0) {
        return;
    }

    if (!has_reg_hookfunc) {
        pthread_mutex_unlock(&reg_lock);
        return;
    }

#ifdef ENABLE_GS_KTOOL
    exit_cmke_manager_gs_ktool();
#endif

    pthread_mutex_unlock(&reg_lock);
}
