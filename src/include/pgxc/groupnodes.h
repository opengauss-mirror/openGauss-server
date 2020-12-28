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
 * groupnodes.h
 *
 * IDENTIFICATION
 *	 src/include/pgxc/groupnodes.h
 *
 * -------------------------------------------------------------------------
 */
 
#ifndef GROUP_NODES_H
#define GROUP_NODES_H

#include "c.h"
#include "pgxc/locator.h"

extern oidvector* exclude_nodes(oidvector* nodes, oidvector* excluded);
extern bool oidvector_eq(oidvector* oid1, oidvector* oid2);
extern oidvector* oidvector_add(oidvector* nodes, oidvector* added);
extern oidvector* oidvector_remove(oidvector* nodes, oidvector* removed);
extern ExecNodes* create_exec_nodes(oidvector* gmember);
extern void delete_exec_nodes(ExecNodes* exec_nodes);
extern oidvector* get_group_member(Oid group_oid);
extern void PgxcOpenGroupRelation(Relation* pgxc_group_rel);
extern void PgxcChangeGroupMember(Oid group_oid, oidvector* gmember);
extern bool PgxcGroupResizing();

#endif
