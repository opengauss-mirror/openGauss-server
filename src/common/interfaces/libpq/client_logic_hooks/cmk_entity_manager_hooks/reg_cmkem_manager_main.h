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
 * registre_hook_func.h
 *      you can register your own key management tool/component/service here. 
 *      what's more, you can choose which to use according to the version and usage scenarios.
 * 
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_hooks/cmk_entity_manager_hooks/registre_hook_func.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef REG_HOOK_FUNC_H
#define REG_HOOK_FUNC_H

#include <stdlib.h>
#include "cmkem_comm.h"

extern CmkemErrCode reg_all_cmk_entity_manager();
extern void exit_all_cmk_entity_manager();

#endif /* REGISTER_HOOK_FUNC_H */
