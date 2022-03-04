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
 * gs_policy_labels.h
 *
 * IDENTIFICATION
 *    contrib/security_plugin/gs_policy_labels.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef GS_POLICY_GS_POLICY_LABELS_H_
#define GS_POLICY_GS_POLICY_LABELS_H_

#include <time.h>
#include <memory>
#include "catalog/gs_policy_label.h"
#include "gs_policy/policy_common.h"

#include "gs_policy/gs_string.h"
#include "gs_policy_object_types.h"
#include "gs_policy/gs_set.h"

#define GET_RELATION(ID, LOCKTYPE)    \
    Relation rel = heap_open(ID, LOCKTYPE);    \
    if(rel == NULL)                        \
        return false;                       \

typedef gs_stl::gs_set<gs_stl::gs_string> policy_default_str_uset;
typedef gs_stl::gs_set<gs_stl::gs_string> policy_default_str_set;

loaded_labels *get_policy_labels();
bool is_label_exist(const char *name);
bool load_policy_labels(bool reload = false);
/* check if some label include table */
bool check_label_has_object(const PolicyLabelItem *object,
                         bool (*CheckLabelBoundPolicy)(bool, const gs_stl::gs_string),
                         bool column_type_is_changed = false,
                         const policy_default_str_set *labels = nullptr);

/* update label value after schema/table/column rename */
bool update_label_value(const gs_stl::gs_string object_name,
                        const gs_stl::gs_string new_object_name,
                        int object_type);

void reset_policy_labels();
void clear_thread_local_label();
void verify_drop_column(AlterTableStmt *stmt);
#endif /* GS_POLICY_GS_POLICY_LABELS_H_ */
