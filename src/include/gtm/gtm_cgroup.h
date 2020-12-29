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
 * gtm_cgroup.h
 *        get the gaussdb cgroup relpath and initialize cgroup.
 *        Please note,caller should free the return value.
 * 
 * IDENTIFICATION
 *        src/include/gtm/gtm_cgroup.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SRC_INCLUDE_GTM_GTM_CGROUP_H
#define SRC_INCLUDE_GTM_GTM_CGROUP_H
extern char* gscgroup_gtm_init();

/* make the current thread attach to gaussdb cgroup */
extern void gscgroup_gtm_attach_task(const char* relpath);
/* just an unused function, reserving this function for future. */
extern void gscgroup_gtm_attach_task_pid(const char* relpath, pid_t tid);

#endif  // SRC_INCLUDE_GTM_GTM_CGROUP_H