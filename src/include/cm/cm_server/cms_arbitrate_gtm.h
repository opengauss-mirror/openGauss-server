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
 * cms_arbitrate_gtm.h
 *
 * IDENTIFICATION
 *	  src/include/cm/cm_server/cms_arbitrate_gtm.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef CMS_ARBITRATE_GTM_H
#define CMS_ARBITRATE_GTM_H

#include "cm/cm_msg.h"

extern void gtm_instance_arbitrate(CM_Connection* con, agent_to_cm_gtm_status_report* agent_to_cm_gtm_status_ptr);
extern void gtm_instance_arbitrate_single(CM_Connection* con,
                                         agent_to_cm_gtm_status_report* agent_to_cm_gtm_status_ptr);

#endif