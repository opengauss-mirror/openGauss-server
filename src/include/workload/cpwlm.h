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
 * cpwlm.h
 * 
 * 
 * IDENTIFICATION
 *        src/include/workload/cpwlm.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef CPWLM_H
#define CPWLM_H

#include "lib/stringinfo.h"
#include "pgxc/pgxcnode.h"

/* core data in CCN of the compute pool */
typedef struct DNState {
    bool is_normal;
    int num_rp;
} DNState;

/* core data in CCN of the compute pool */
typedef struct {
    DNState* dn_state;
    int dn_num;
} ComputePoolState;

/* from the CN of the compute pool */
typedef struct {
    int dnnum;
    int freerp;
    char* version;
} CPRuntimeInfo;

/* from the conf file in CN data directory of DWS. */
typedef struct {
    char* cpip;
    char* cpport;
    char* username;
    char* password;
    char* version;
    int dnnum;
    int pl;
    int rpthreshold;
} ComputePoolConfig;

extern List* get_dnlist(int neededDNnum);
extern ComputePoolState* get_cluster_state();
extern void process_request(StringInfo input_message);
extern void get_cp_runtime_info(PGXCNodeHandle* handle);
extern char* get_version();
extern ComputePoolConfig** get_cp_conninfo(int* cnum = NULL);

extern char* trim(char* src);

extern bool check_version_compatibility(const char* remote_version);

/* just run on DN */
extern void decrease_rp_number();
extern void increase_rp_number();

#endif  // CPWLM_H
