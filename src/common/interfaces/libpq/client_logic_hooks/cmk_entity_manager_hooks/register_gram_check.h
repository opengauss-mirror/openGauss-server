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
 * register_gram_check.h
 *      we support users to register any number of key management tool/components/services, all of which
 *      have the right to process the CREATE CMKO statement.
 *      if a CREATE CMKO statement is not processed at all, we agree that it's a wrong and unexpected statement.
 *      so, we should put this grammar check at the end of hook function list, to make sure the unexpected SQL statement
 *      can be reported.
 * 
 * IDENTIFICATION
 *	  src/common/interfaces/libpq/client_logic_hooks/cmk_entity_manager_hooks/register_gram_check.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef REGISTER_GRAM_CHECK_H
#define REGISTER_GRAM_CHECK_H

extern int reg_grammar_check_main();

#endif /* REG_GRAM_CHECK_H */
