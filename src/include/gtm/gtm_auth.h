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
 * gtm_auth.h
 *        GTM server authentication module.
 *        Variables and function commands used for handling authentication between gtm client and gtm server.
 *
 *
 * IDENTIFICATION
 *        src/include/gtm/gtm_auth.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef GTM_AUTH_H
#define GTM_AUTH_H
#include "gtm/gtm_opt.h"

/* Gtm guc parameter name */
#define GTM_OPTNAME_AUTHENTICATION_TYPE "gtm_authentication_type"
#define GTM_OPTNAME_KRB_SERVER_KEYFILE "gtm_krb_server_keyfile"

/* define gtm server autnentication method */
#define GTM_AUTH_REJECT (0)
#define GTM_AUTH_TRUST (1)
#define GTM_AUTH_GSS (2)

extern int gtm_auth_method;
extern char* gtm_krb_server_keyfile;
extern const struct config_enum_entry gtm_auth_options[];

extern bool CheckGtmKrbKeyFilePath(char** newval, void** extra, GtmOptSource source);
extern bool CheckGtmAuthMethod(int* newval, void** extra, GtmOptSource source);
extern void GtmPerformAuthentication(GTM_ThreadInfo* thrinfo);
extern void GtmAuthCleanup();
#endif /* GTM_AUTH_H */
