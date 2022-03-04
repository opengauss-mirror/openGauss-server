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
 * gs_policy_object_types.h
 *
 * IDENTIFICATION
 *    contrib/security_plugin/gs_policy_object_types.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef GS_POLICY_OBJECT_TYPES_H_
#define GS_POLICY_OBJECT_TYPES_H_

#include <time.h>
#include <memory>
#include "gs_policy/gs_string.h"
#include "gs_policy/gs_vector.h"
#include "gs_policy/gs_set.h"
#include "gs_policy/gs_map.h"
#include "securec.h"
#include "nodes/pg_list.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#ifdef HAVE_INT64_TIMESTAMP
#define timestamp int64
#else
#define timestamp double
#endif

enum block_behaviour
{
    B_UNKNOWN,
    B_ACCESS_DENY,
    B_EMPTY_RESULT,
    B_RESET_CONN
};

enum PrivType {
    T_NONE,
    T_ALTER,
    T_CREATE,
    T_DROP,
    T_RENAME,
    T_GRANT,
    T_REVOKE,
    T_COMMENT,
    T_ANALYZE,
    T_SET,
    T_SHOW,
    T_LOGIN,
    T_LOGOUT,
    T_LOGIN_SUCCESS,
    T_LOGIN_FAILURE,
    T_COPY,
    T_CURSOR,
    T_FETCH,
    T_CLOSE,
    T_ALL
};

enum PrivObject {
    O_UNKNOWN,
    O_TABLE,
    O_SCHEMA,
    O_INDEX,
    O_COLUMN,
    O_LABEL,
    O_RELATION,
    O_SEQUENCE,
    O_DATABASE,
    O_DOMAIN,
    O_SERVER,
    O_FUNCTION,
    O_LANGUAGE,
    O_TABLESPACE,
    O_DATA_SOURCE,
    O_TRIGGER,
    O_ROLE,
    O_VIEW,
    O_FOREIGNTABLE,
    O_USER,
    O_GROUP,
    O_PARAMETER,
    O_CURSOR
};

Oid get_relation_schema(Oid relid);
Oid get_function_schema(Oid funcid);

struct PolicyPair {
        PolicyPair(long long _id=0, int _type=0):m_id(_id), m_block_type(_type){}
        bool operator == (const PolicyPair& arg) const 
        {
            return (m_id == arg.m_id) && (m_block_type == arg.m_block_type);
        }

        long long m_id;
        int       m_block_type;
};

int policy_pair_cmp(const void *key1, const void *key2);

struct PolicyLabelItem {
    PolicyLabelItem(const char *schema = "", const char *object = "",
                    const char *column = "", int obj_type = O_TABLE);
    PolicyLabelItem(Oid schema, Oid object, int obj_type, const char *column = "");
    PolicyLabelItem(const PolicyLabelItem &arg);
    PolicyLabelItem &operator=(const PolicyLabelItem &arg);

    void init(const PolicyLabelItem &arg);
    void get_fqdn_value(gs_stl::gs_string *value) const;

    bool operator < (const PolicyLabelItem& arg) const;
    bool operator == (const PolicyLabelItem& arg) const;
    bool empty() const {return strlen(m_column) == 0;}
    void set_object(const char *obj, int obj_type = 0);
    void set_object(Oid objid, int obj_type = 0);
    Oid            m_schema;
    Oid            m_object;
    char           m_column[256];
    int            m_obj_type;
};

int policy_label_item_cmp(const void *key1, const void *key2);

struct LabelDataHash {
    size_t operator()(const PolicyLabelItem& k) const;
};

struct EqualToLabelData {
    bool operator()(const PolicyLabelItem& l, const PolicyLabelItem& r) const;
};

typedef gs_stl::gs_set<PolicyLabelItem, policy_label_item_cmp> gs_policy_label_set;
/* label type */
typedef gs_stl::gs_map<int, gs_policy_label_set> typed_labels;
/* label_name */
typedef gs_stl::gs_map<gs_stl::gs_string, typed_labels> loaded_labels;
/* db id */
typedef gs_stl::gs_map<int, loaded_labels*> loaded_labels_by_db;
/* db id */
typedef gs_stl::gs_map<int, int> loaded_by_db;

struct PolicyPairHash {
    size_t operator()(const PolicyPair& k) const;
};

struct EqualToPolicyPair {
    bool operator()(const PolicyPair& l, const PolicyPair& r) const;
};

typedef gs_stl::gs_set<PolicyPair, policy_pair_cmp> policy_set;
typedef gs_stl::gs_map<PolicyPair, gs_stl::gs_string, policy_pair_cmp> policy_label_map;

using RenamePair    = std::pair<gs_stl::gs_string, gs_stl::gs_string>;
using RenameVector  = gs_stl::gs_vector<RenamePair>;
/* object_type */
using RenameMap     = gs_stl::gs_map<int, RenameVector>;

struct gs_base_policy {
    gs_base_policy(long long _id = 0):m_id(_id), m_enabled(true), m_modify_date(0){}
    bool operator == (const gs_base_policy &arg) const 
    {
        return m_id == arg.m_id;
    }
    void get_pol(PolicyPair& arg) const
    {
        arg.m_id            = m_id;
    }
    long long   m_id;
    gs_stl::gs_string m_name;
    bool        m_enabled;
    gs_stl::gs_string m_comment;
    time_t      m_modify_date;
};

int gs_base_policy_cmp(const void *key1, const void *key2);

struct PolicyHash {
    size_t operator()(const gs_base_policy& k) const;
};

struct EqualToPolicy {
    bool operator()(const gs_base_policy& l, const gs_base_policy& r) const;
};

typedef gs_stl::gs_set<gs_base_policy, gs_base_policy_cmp> gs_policy_set;
typedef gs_stl::gs_map<int, gs_policy_set*> gs_policy_map;

struct GsPolicyBase {
    GsPolicyBase():m_type(0), m_policy_id(0), m_modify_date(0){}
    bool operator == (const GsPolicyBase &arg) const 
    {
        return m_type == arg.m_type && (strcasecmp(m_label_name.c_str(), arg.m_label_name.c_str()) == 0);
    }

    ~GsPolicyBase(){}
    int         m_type;
    gs_stl::gs_string m_label_name;
    long long   m_policy_id;
    time_t      m_modify_date;
};

struct PolicyBaseHash {
    size_t operator()(const GsPolicyBase& k) const;
};

struct EqualToPolicyBase {
    bool operator()(const GsPolicyBase& l, const GsPolicyBase& r) const;
};

int gs_policy_base_cmp(const void *key1, const void *key2);

typedef gs_stl::gs_vector<Oid> func_types;
typedef gs_stl::gs_vector<gs_stl::gs_string> func_params;

bool get_function_parameters(HeapTuple tuple, func_types* types, int* default_params = NULL);
bool verify_proc_params(const func_params* func_params, const func_types* proc_types);

typedef gs_stl::gs_set<GsPolicyBase, gs_policy_base_cmp> gs_policy_base_set;
/* policy id */
typedef gs_stl::gs_map<long long, gs_policy_base_set> gs_policy_base_map;
/* db id */
typedef gs_stl::gs_map<int, gs_policy_base_map> gs_policy_base_map_by_db;
/* audited actions */
typedef gs_stl::gs_set<int> access_privilege_set;
/* db id */
typedef gs_stl::gs_map<int, access_privilege_set> pg_audited_actions_by_db;
/* role */
typedef gs_stl::gs_set<Oid> global_roles_in_use;

using AccessPair = std::pair<gs_stl::gs_string, /* name */
                             int>; /* access object */
struct AccessPairHash {
    size_t operator()(const AccessPair& k) const;
};

struct EqualToAccessPair {
    bool operator()(const AccessPair& l, const AccessPair& r) const;
};

int access_pair_cmp(const void *key1, const void *key2);

typedef gs_stl::gs_set<gs_stl::gs_string> column_set;
typedef gs_stl::gs_map<AccessPair, column_set, access_pair_cmp> table_policy_result;
typedef gs_stl::gs_map<long long, table_policy_result> policy_result;

uint32_t policy_hash_combine(uint32_t seed, uint32_t value);
size_t policy_str_hash(const gs_stl::gs_string arg);
bool get_function_name(long long funcid, PolicyLabelItem *name);

int get_privilege_type(const char *name);
int get_privilege_object_type(const char *name);
const char *get_privilege_object_name(int type);
void load_function_label(const Query *query, bool audit_exist);

bool name_list_to_string(List *names, gs_stl::gs_string *name, int max_const_count = -1 /* unlimited */);
bool name_list_to_label(PolicyLabelItem *item, List *names, char *name = NULL, size_t name_size = 0);

/* build PolicyLabelItem helper function*/
void gen_policy_labelitem(PolicyLabelItem &item, const ListCell *rel, int objtype);
void gen_policy_label_for_commentstmt(PolicyLabelItem &item, const CommentStmt *commentstmt);
int get_objtype(int object_type);
CmdType get_rte_commandtype(RangeTblEntry *rte);
const char *get_cursorinfo(CmdType type);

#endif /* GS_POLICY_OBJECT_TYPES_H_ */
