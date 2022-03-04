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
 * knl_instance_attr_security.h
 *        Data struct to store all GUC variables.
 *
 *   When anyone try to added variable in this file, which means add a guc
 *   variable, there are several rules needed to obey:
 *
 *   add variable to struct 'knl_@level@_attr_@group@'
 *
 *   @level@:
 *   1. instance: the level of guc variable is PGC_POSTMASTER.
 *   2. session: the other level of guc variable.
 *
 *   @group@: sql, storage, security, network, memory, resource, common
 *   select the group according to the type of guc variable.
 * 
 * 
 * IDENTIFICATION
 *        src/include/knl/knl_guc/knl_instance_attr_security.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_INCLUDE_KNL_KNL_INSTANCE_ATTR_SECURITY_H_
#define SRC_INCLUDE_KNL_KNL_INSTANCE_ATTR_SECURITY_H_

#include "knl/knl_guc/knl_guc_common.h"

typedef struct knl_instance_attr_security {
    bool EnableSSL;
    bool enablePrivilegesSeparate;
    bool enable_nonsysadmin_execute_direct;
    bool enable_tde;
    char* ssl_cert_file;
    char* ssl_key_file;
    char* ssl_ca_file;
    char* ssl_crl_file;
    char* SSLCipherSuites;
    char* Audit_directory;
    char* Audit_data_format;
    char* transparent_encrypted_string;
    char* transparent_encrypt_kms_region;
    bool use_elastic_search;
    char* elastic_search_ip_addr;
    int audit_thread_num;   
} knl_instance_attr_security;

#endif /* SRC_INCLUDE_KNL_KNL_INSTANCE_ATTR_SECURITY_H_ */
