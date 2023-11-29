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
 * gs_policy_object_types.cpp
 *    get fqdn name of different kinds of objects, such as schema, relation, function
 *    fqdn(Fully Qualified Domain Name) means full name of objects
 * 
 * IDENTIFICATION
 *    contrib/security_plugin/gs_policy_object_types.cpp 
 *
 * -------------------------------------------------------------------------
 */
#include "parser/parse_func.h"
#include "postgres.h"
#include "access/htup.h"
#include "access/heapam.h"
#include "access/hash.h"
#include "c.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "commands/user.h"
#include "gs_policy_object_types.h"
#include "gs_policy_plugin.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "utils/acl.h"
#include "catalog/objectaddress.h"

/*
 * get_relation_schema
 *    Returns the schema oid of a given relation oid.
 *
 * Returns 0 if not find a valid relation tuple in RELOID cache.
 */
Oid get_relation_schema(Oid relid)
{
    if (!OidIsValid(relid)) {
        return InvalidOid;
    }
    Oid schemaid = 0;
    HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);
        schemaid = reltup->relnamespace;
        ReleaseSysCache(tp);
    }
    return schemaid;
}

/*
 * get_function_schema
 *    Returns the schema oid of a given function oid.
 *
 * Returns 0 if not find a valid function tuple in PROCOID cache.
 */
Oid get_function_schema(Oid funcid)
{
    if (!OidIsValid(funcid)) {
        return InvalidOid;
    }
    Oid schemaid = 0;
    HeapTuple tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_proc functup = (Form_pg_proc) GETSTRUCT(tp);
        schemaid = functup->pronamespace;
        ReleaseSysCache(tp);
    }
    return schemaid;
}

/*
 * get_objectname_with_schema
 *    Returns the relation name of a given relation oid.
 *
 * the return name like schema.relation
 */
static void get_objectname_with_schema(Oid relid, gs_stl::gs_string *value, const char *column = NULL)
{
    HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_class reltup = (Form_pg_class) GETSTRUCT(tp);
        Oid schemaid = reltup->relnamespace;
        if (OidIsValid(schemaid)) {
            *value = get_namespace_name(schemaid);
            const char *name = NameStr(reltup->relname);
            if (name != NULL && strlen(name) > 0) {
                value->push_back('.');
                value->append(name);
                if (column != NULL) {
                    value->push_back('.');
                    value->append(column);
                }
            }
        }
        ReleaseSysCache(tp);
    }
}

/*
 * get_funcname_with_schema
 *    Returns the function name of a given function oid.
 *
 * the return name like schema.function
 */
static void get_funcname_with_schema(Oid funcid, gs_stl::gs_string *value)
{
    HeapTuple tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (HeapTupleIsValid(tp)) {
        Form_pg_proc functup = (Form_pg_proc) GETSTRUCT(tp);
        Oid schemaid = functup->pronamespace;
        if (OidIsValid(schemaid)) {
            *value = get_namespace_name(schemaid);
            const char *name = NameStr(functup->proname);
            if (name != NULL && strlen(name) > 0) {
                value->push_back('.');
                value->append(name);
            }
        }
        ReleaseSysCache(tp);
    }
}


/*
 * get_fqdn_value
 *    Returns name of PolicyLabelItem with schema name.
 *
 * If PolicyLabelItem type is schema, return name of schema directly.
 * For function type, return schema_name.function_name.
 * For other types such as TABLE, COLUMN, VIEW, return schema_name.relation_name.
 */
void PolicyLabelItem::get_fqdn_value(gs_stl::gs_string *value) const
{
    if (!OidIsValid(m_object) && m_schema > 0) {
        const char* name = get_namespace_name(m_schema);
        if (name && strlen(name) > 0) {
            *value = name;
        }
        return;
    }

    switch (m_obj_type) {
        case O_SCHEMA: {
            if (!OidIsValid(m_schema)) {
                ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("wrong privileges object type")));
            }
            /* if faqn type is schema, just return it's name */
            const char *name = get_namespace_name(m_schema);
            if (name != NULL && strlen(name) > 0) {
                *value = name;
            }
            break;
        }
        case O_FUNCTION:
            get_funcname_with_schema(m_object, value);
            break;
        case O_TABLE:
        case O_VIEW:
            get_objectname_with_schema(m_object, value);
            break;
        case O_COLUMN:
            get_objectname_with_schema(m_object, value, m_column);
            break;
        default:
            break;
    }
}

/*
 * policy_hash_combine
 *    Returns a combine hash of 2 unit32 values.
 */
uint32_t policy_hash_combine(uint32_t seed, uint32_t value)
{
    seed ^= value + 0x9e3779b9 + (seed<<6) + (seed>>2);
    return seed;
}

/*
 * policy_str_hash
 *    combine each char of string and return the last hash
 */
size_t policy_str_hash(const gs_stl::gs_string arg)
{
    size_t seed = 0;
    for (size_t i = 0; i < arg.size(); ++i) {
        seed = policy_hash_combine(seed, hash_uint32(tolower(arg[i])));
    }
    return seed;
}

/*
 * PolicyLabelItem
 *    constructor of a PolicyLabelItem by it's object oid and obj_type.
 *    schema oid and column name are optional parameters.
 */
PolicyLabelItem::PolicyLabelItem(Oid schema, Oid object, int obj_type, const char *column)
    :m_schema(schema), m_object(object), m_obj_type(obj_type)
{
    errno_t rc = memset_s(m_column, sizeof(m_column), 0, sizeof(m_column));
    securec_check(rc, "\0", "\0");
    if (!OidIsValid(m_object)) {
        return;
    }
    switch (m_obj_type) {
        case O_FUNCTION:
            if (!OidIsValid(schema)) {
                m_schema = get_function_schema(m_object);
            }
            break;
        default:
            if (!OidIsValid(schema)) {
                m_schema = get_relation_schema(m_object);
            }
            break;
    }

    if (column != NULL && strlen(column) > 0) {
        rc = snprintf_s(m_column, sizeof(m_column), strlen(column), "%s", column);
        securec_check_ss(rc, "\0", "\0");
    }
}

/*
 * PolicyLabelItem
 *    constructor of a PolicyLabelItem by it's object name schema name and obj_type.
 *    column name is optional parameters.
 */
PolicyLabelItem::PolicyLabelItem(const char *schema, const char *object, const char *column, int obj_type)
    :m_object(InvalidOid), m_obj_type(obj_type)
{
    m_schema = SchemaNameGetSchemaOid(schema, true);
    errno_t rc = memset_s(m_column, sizeof(m_column), 0, sizeof(m_column));
    securec_check(rc, "\0", "\0");
    if (OidIsValid(m_schema) && object != NULL && strlen(object) > 0) {
        set_object(object, obj_type);
    }
    if (column != NULL && strlen(column) > 0) {
        rc = snprintf_s(m_column, sizeof(m_column), strlen(column), "%s", column);
        securec_check_ss(rc, "\0", "\0");
    }
}

void PolicyLabelItem::init(const PolicyLabelItem &arg)
{
    m_schema = arg.m_schema;
    m_object = arg.m_object;
    errno_t rc = memset_s(m_column, sizeof(m_column), 0, sizeof(m_column));
    securec_check(rc, "\0", "\0");
    if (strlen(arg.m_column) > 0)
    {
        rc = snprintf_s(m_column, sizeof(m_column), strlen(arg.m_column), "%s", arg.m_column);
        securec_check_ss(rc, "\0", "\0");
    }
    m_obj_type = arg.m_obj_type;
}

PolicyLabelItem::PolicyLabelItem(const PolicyLabelItem &arg)
{
    if (this == &arg) {
        return;
    }
    init(arg);
}

PolicyLabelItem &PolicyLabelItem::operator=(const PolicyLabelItem &arg)
{
    if (this == &arg) {
        return *this;
    }

    init(arg);
    return *this;
}

/*
 * set_object
 *    set m_obj_type and objid by given parameters.
 */
void PolicyLabelItem::set_object(Oid objid, int obj_type)
{
    if (obj_type != 0) {
        m_obj_type = obj_type;
    }
    m_object = objid;
}

/*
 * set_object
 *    set m_obj_type by given obj_type and search for obj id by its name.
 */
void PolicyLabelItem::set_object(const char *obj, int obj_type)
{
    if (obj_type && m_obj_type != O_COLUMN) {
        m_obj_type = obj_type;
    }
    if (obj == NULL || strlen(obj) == 0) {
        return;
    }
    switch (obj_type) {
        case O_FUNCTION:
            m_object = get_func_oid(obj, m_schema, NULL);
            break;
        default:
            m_object = get_relname_relid(obj, m_schema);
            break;
    }
}

bool PolicyLabelItem::operator < (const PolicyLabelItem& arg) const
{
    return true;
}

bool PolicyLabelItem::operator == (const PolicyLabelItem& arg) const
{
    bool res = (m_schema == arg.m_schema && m_object == arg.m_object && m_obj_type == arg.m_obj_type);
    if (res && arg.m_obj_type == O_COLUMN) {
        return !strcasecmp(m_column, arg.m_column);
    }
    return res;
}

size_t LabelDataHash::operator()(const PolicyLabelItem& item) const
{
    size_t seed = 0;
    seed = policy_hash_combine(seed, hash_uint32(item.m_schema));
    seed = policy_hash_combine(seed, hash_uint32(item.m_object));
    seed = policy_hash_combine(seed, hash_uint32(item.m_obj_type));
    if (item.m_obj_type == O_COLUMN) {
        seed = policy_hash_combine(seed, policy_str_hash(item.m_column));
    }
    return seed;
}

bool EqualToLabelData::operator()(const PolicyLabelItem& left, const PolicyLabelItem& right) const
{
    bool res = (left.m_schema == right.m_schema && left.m_object == right.m_object && 
                left.m_obj_type == right.m_obj_type);
    if (res && right.m_obj_type == O_COLUMN) {
        res = res && (strcasecmp(left.m_column, right.m_column) == 0);
    }
    return res;
}

size_t AccessPairHash::operator()(const AccessPair& pair) const
{
    size_t seed = 0;
    seed = policy_hash_combine(seed, hash_uint32(pair.second));
    seed = policy_hash_combine(seed, policy_str_hash(pair.first.c_str()));
    return seed;
}

bool EqualToAccessPair::operator()(const AccessPair& left, const AccessPair& right) const
{
    /* compare value of AccessPair name and value */
    return (left.second == right.second) && !strcasecmp(left.first.c_str(), right.first.c_str());
}

/*
 * get_function_name
 *    search PROCOID cache for function tuple by its oid, 
 *    then set m_shema and m_object of func_label.
 */
bool get_function_name(long long funcid, PolicyLabelItem *func_label)
{
    if (funcid == 0) {
        return false;
    }

    HeapTuple tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    if (!HeapTupleIsValid(tuple)) {
        return false;
    }
    Form_pg_proc func_rel = (Form_pg_proc) GETSTRUCT(tuple);
    const char *procname  = func_rel->proname.data;
    func_label->m_schema  = func_rel->pronamespace;

    func_label->set_object(procname, O_FUNCTION);
    ReleaseSysCache(tuple);
    return true;
}

size_t PolicyPairHash::operator()(const PolicyPair& pair) const
{
    return hash_uint32(pair.m_id);
}

bool EqualToPolicyPair::operator()(const PolicyPair& l, const PolicyPair& r) const
{
    /* compare value of PolicyPair id */
    return l.m_id == r.m_id;
}

size_t PolicyBaseHash::operator()(const GsPolicyBase& policy) const
{
    size_t seed = 0;
    seed = policy_hash_combine(seed, hash_uint32(policy.m_type));
    seed = policy_hash_combine(seed, policy_str_hash(policy.m_label_name));
    return seed;
}

bool EqualToPolicyBase::operator()(const GsPolicyBase& l, const GsPolicyBase& r) const
{
    return (l.m_type == r.m_type) && !strcasecmp(l.m_label_name.c_str(), r.m_label_name.c_str());
}

size_t PolicyHash::operator()(const gs_base_policy& policy) const
{
    return hash_uint32(policy.m_id);
}

bool EqualToPolicy::operator()(const gs_base_policy& l, const gs_base_policy& r) const
{
    return l.m_id == r.m_id;
}

/*
 * name_list_to_label
 *    assign item->m_schema and item->m_object by input name list.
 *
 * 1. if name list only contains the object name, we find m_object by its name, 
 *    and m_schema is activeCreationNamespace.
 * 2. if name list contains object name and schema name, we find m_schema and m_object by their names.
 */
bool name_list_to_label(PolicyLabelItem *item, List *names, char *name, size_t name_size)
{
    errno_t rc = EOK;
    int listsize = list_length(names);
    switch (listsize) {
        case 1:    /* specify the object name of policy label item */
        {
            Node *first = (Node *) linitial(names);
            item->m_schema = SchemaNameGetSchemaOid(NULL, true);
            if (name != NULL) {
                rc = snprintf_s(name, name_size, name_size - 1, "%s", strVal(first));
                securec_check_ss(rc, "\0", "\0");
            }
            switch (item->m_obj_type) {
                case O_FUNCTION:
                    item->m_object = get_func_oid(strVal(first), item->m_schema, NULL);
                    break;
                default:
                    item->m_object = get_relname_relid(strVal(first), item->m_schema);
                    break;
            }
            break;
        }
        case 2:    /* specify the schema name and object name of policy label item */
        {
            Node *first = (Node *) linitial(names);
            Node *second = (Node *) lsecond(names);
            item->m_schema = get_namespace_oid(strVal(first), true);
            if (name != NULL) {
                rc = snprintf_s(name, name_size, name_size - 1, "%s", strVal(second));
                securec_check_ss(rc, "\0", "\0");
            }
            switch (item->m_obj_type) {
                case O_FUNCTION:
                    item->m_object = get_func_oid(strVal(second), item->m_schema, NULL);
                    break;
                default:
                    item->m_object = get_relname_relid(strVal(second), item->m_schema);
                    break;
            }
            break;
        }
        default:
            break;
    }
    return listsize > 0;
}

/*
 * name_list_to_label
 *    use elements in name list to concatenate string.
 */
bool name_list_to_string(List *names, gs_stl::gs_string *value, int max_const_count)
{
    ListCell   *l = NULL;
    int         const_count = 0;

    foreach (l, names) {
        Node *name = (Node *) lfirst(l);
        if (IsA(name, String)) {
            value->append(strVal(name));
        } else if (IsA(name, A_Star)) {
            value->append("*");
        } else if (IsA(name, A_Const)) {
            if (const_count == max_const_count) {
                continue;
            }
            A_Const *con = (A_Const*)name;
            char tmp[512] = {0};
            switch (nodeTag(&con->val)) {
                case T_Integer:
                case T_Float:
                    {
                        errno_t rc = snprintf_s(tmp, sizeof(tmp), sizeof(tmp) - 1, "%ld", intVal(&con->val));
                        securec_check_ss(rc, "\0", "\0");
                        value->append(tmp);
                        break;
                    }
                case T_String:
                    value->append(strVal(&con->val));
                    break;
                default:
                    break;
            }
            ++const_count;
        }
    }
    return !value->empty();
}

typedef struct OperInfo
{
    const char* oper_name;
    PrivType    oper_type;
} OperInfo;

typedef struct ObjectInfo
{
    const char* object_name;
    PrivObject object_type;
} ObjectInfo;

typedef struct ObjectTypeInfo
{
    PrivObject object_type;
    const char* object_name;
} ObjectTypeInfo;

typedef struct CmdCursorInfo {
    CmdType cmd_type;
    const char *object_name;
} CmdCursorInfo;

static CmdCursorInfo cmd_cursorinfo[] = {
    {CMD_SELECT, "FOR SELECT FROM"},
    {CMD_INSERT, "FOR INSERT TO"},
    {CMD_UPDATE, "FOR UPDATE FROM"},
    {CMD_DELETE, "FOR DELETE FROM"},
    {CMD_UNKNOWN, NULL}
};

static OperInfo oper_infos[] = {
    {"create", T_CREATE},
    {"alter", T_ALTER},
    {"drop", T_DROP},
    {"rename", T_RENAME},
    {"grant", T_GRANT},
    {"revoke", T_REVOKE},
    {"comment", T_COMMENT},
    {"analyze", T_ANALYZE},
    {"set", T_SET},
    {"show", T_SHOW},
    {"login_any", T_LOGIN},
    {"logout", T_LOGOUT},
    {"login_success", T_LOGIN_SUCCESS},
    {"login_failure", T_LOGIN_FAILURE},
    {"copy", T_COPY},
    {"cursor", T_CURSOR},
    {"fetch", T_FETCH},
    {"close", T_CLOSE},
    {"all", T_ALL},
    {NULL, T_NONE}
};

static ObjectInfo object_infos[] = 
{
    {"table", O_TABLE},
    {"schema", O_SCHEMA},
    {"index", O_INDEX},
    {"column", O_COLUMN},
    {"label", O_LABEL},
    {"relation", O_RELATION},
    {"sequence", O_SEQUENCE},
    {"database", O_DATABASE},
    {"domain", O_DOMAIN},
    {"server", O_SERVER},
    {"function", O_FUNCTION},
    {"language", O_LANGUAGE},
    {"tablespace", O_TABLESPACE},
    {"data source", O_DATA_SOURCE},
    {"trigger", O_TRIGGER},
    {"role", O_ROLE},
    {"view", O_VIEW},
    {"foreigntable", O_FOREIGNTABLE},
    {"user", O_USER},
    {"group", O_GROUP},
    {"parameter", O_PARAMETER},
    {"cursor", O_CURSOR},
    {NULL, O_UNKNOWN}
};

static ObjectTypeInfo object_type_infos[] = 
{
    {O_TABLE, "TABLE"},
    {O_SCHEMA, "SCHEMA"},
    {O_INDEX, "INDEX"},
    {O_COLUMN, "COLUMN"},
    {O_LABEL, "LABEL"},
    {O_RELATION, "RELATION"},
    {O_SEQUENCE, "SEQUENCE"},
    {O_DATABASE, "DATABASE"},
    {O_DOMAIN, "DOMAIN"},
    {O_SERVER, "SERVER"},
    {O_FUNCTION, "FUNCTION"},
    {O_LANGUAGE, "LANGUAGE"},
    {O_TABLESPACE, "TABLESPACE"},
    {O_DATA_SOURCE, "DATA SOURCE"},
    {O_TRIGGER, "TRIGGER"},
    {O_ROLE, "ROLE"},
    {O_VIEW, "VIEW"},
    {O_FOREIGNTABLE, "FOREIGNTABLE"},
    {O_USER, "USER"},
    {O_GROUP, "GROUP"},
    {O_PARAMETER, "PARAMETER"},
    {O_CURSOR, "CURSOR"},
    {O_UNKNOWN, NULL}
};

/*
 * get_cursorinfo
 *    return cursor operation object
 */
const char *get_cursorinfo(CmdType type)
{
    for (int i = 0; cmd_cursorinfo[i].object_name != NULL; ++i) {
        if (cmd_cursorinfo[i].cmd_type == type) {
            return cmd_cursorinfo[i].object_name;
        }
    }
    return "UNKNOWN";
}

/*
 * get_privilege_type
 *    return privilege type in enum PrivType by its name
 */
int get_privilege_type(const char *name)
{
    for (int i = 0; oper_infos[i].oper_name != NULL; ++i) {
        if ((strlen(oper_infos[i].oper_name) == strlen(name)) && 
            (strcmp(oper_infos[i].oper_name, name) == 0)) {
            return (int)oper_infos[i].oper_type;
        }
    }
    return T_NONE;
}

/*
 * get_privilege_object_type
 *    convert object name to type
 */
int get_privilege_object_type(const char *name)
{
    for (int i = 0; object_infos[i].object_name != NULL; ++i) {
        if ((strlen(object_infos[i].object_name) == strlen(name)) &&
            (strcmp(object_infos[i].object_name, name) == 0)) {
                return object_infos[i].object_type;
            }
    }
    return O_UNKNOWN;
}

/*
 * get_privilege_object_name
 *    convert object type to name
 */
const char *get_privilege_object_name(int type)
{
    for (int i = 0; object_type_infos[i].object_name != NULL; ++i) {
        if (object_type_infos[i].object_type == type) {
            return object_type_infos[i].object_name;
        }
    }
    return "UNKNOWN";
}
int access_pair_cmp(const void *key1, const void *key2)
{
    AccessPair p1 = *(AccessPair *)key1;
    AccessPair p2 = *(AccessPair *)key2;
    if (p1.first < p2.first) return -1;
    if (p2.first < p1.first) return 1;
    return p1.second - p2.second;
}

int policy_pair_cmp(const void *key1, const void *key2)
{
    PolicyPair p1 = *(PolicyPair *) key1;
    PolicyPair p2 = *(PolicyPair *) key2;
    if (p1.m_id < p2.m_id) return -1;
    if (p1.m_id > p2.m_id) return 1;
    return p1.m_block_type - p2.m_block_type;
}

int policy_label_item_cmp(const void *key1, const void *key2)
{
    PolicyLabelItem *data1 = (PolicyLabelItem*)key1;
    PolicyLabelItem *data2 = (PolicyLabelItem*)key2;
    if (data1->m_schema < data2->m_schema) return -1;
    if (data2->m_schema < data1->m_schema) return 1;
    if (data1->m_object < data2->m_object) return -1;
    if (data2->m_object < data1->m_object) return 1;
    if (data1->m_obj_type < data2->m_obj_type) return -1;
    if (data2->m_obj_type < data1->m_obj_type) return 1;
    if (data2->m_obj_type == O_COLUMN) {
        return strcasecmp(data1->m_column, data2->m_column);
    }
    return 0;
}

int gs_base_policy_cmp(const void *key1, const void *key2)
{
    gs_base_policy *l = (gs_base_policy*)key1;
    gs_base_policy *r = (gs_base_policy*)key2;
    return l->m_id - r->m_id;
}

int gs_policy_base_cmp(const void *key1, const void *key2)
{
    GsPolicyBase *l = (GsPolicyBase*)key1;
    GsPolicyBase *r = (GsPolicyBase*)key2;
    if (l->m_type < r->m_type) return -1;
    if (r->m_type < l->m_type) return 1;
    return strcasecmp(l->m_label_name.c_str(), r->m_label_name.c_str());
}

/* get function param types */
bool get_function_parameters(HeapTuple tuple, func_types* types, int* default_params)
{
    if (types == NULL) {
        return false;
    }
    Form_pg_proc func_rel = (Form_pg_proc) GETSTRUCT(tuple);
    if (default_params != NULL) {
        *default_params = func_rel->pronargdefaults;
    }
    bool isNull = false;
    Datum pro_all_arg_types = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_proallargtypes, &isNull);
    Oid* pro_arg_types = NULL;
    if (!isNull) {
        ArrayType  *arr = DatumGetArrayTypeP(pro_all_arg_types);
        Assert(ARR_DIMS(arr)[0] >= func_rel->pronargs);
        pro_arg_types = (Oid*)ARR_DATA_PTR(arr);
    } else {
        pro_arg_types = func_rel->proargtypes.values;
    }
    Oid type;
    for (int i = 0; i < func_rel->pronargs; ++i) {
        type = pro_arg_types[i];
        types->push_back(type);
    }
    return !types->empty();
}

bool verify_proc_params(const func_params* func_params, const func_types* proc_types)
{
    if (!func_params || func_params->empty())
        return true;
    if (!proc_types || proc_types->empty()) {
        return false;
    }
    /* it is used to represent function's parameters in pg_proc */
    func_types::const_iterator it = proc_types->begin();

    ++it;
    /* fit is useed to represent user's input parameters when create masking policy */
    func_params::const_iterator fit = func_params->begin(), feit = func_params->end();
    int datatype_length = 2; /* data type length */
    for (; fit != feit; ++fit, ++it) {
        if (it == proc_types->end()) {
            return false;
        }
        switch (*it) {
            case BPCHAROID:
            case VARCHAROID:
            case NVARCHAR2OID:
            case TEXTOID:
            {
                if (strncasecmp(fit->c_str(), "s:", datatype_length) != 0) {
                    return false;
                }
            }
            break;
            case INT8OID:
            case INT4OID:
            case INT2OID:
            case INT1OID:
            {
                if (strncasecmp(fit->c_str(), "i:", datatype_length) != 0) {
                    return false;
                }
            }
            break;
            case FLOAT4OID:
            case FLOAT8OID:
            case NUMERICOID:
            {
                if (strncasecmp(fit->c_str(), "f:", datatype_length) != 0) {
                    return false;
                }
            }
            break;
            default:
                return false;
        }
    }
    return true;
}

void load_function_label(const Query *query, bool audit_exist)
{
    if (audit_exist && query->rtable != NIL) {
        ListCell *lc = NULL;
        foreach (lc, query->rtable) {
            RangeTblEntry *rte = (RangeTblEntry *)lfirst(lc);
            if (rte->rtekind == RTE_REMOTE_DUMMY) {
                continue;
            } else if (rte && rte->rtekind == RTE_FUNCTION && rte->funcexpr) {
                FuncExpr *fe = (FuncExpr *)rte->funcexpr;
                PolicyLabelItem func_label;
                get_function_name(fe->funcid, &func_label);
                set_result_set_function(func_label);
            }
        }
    }
}

/*
 * ListCell could be RangeVar nodes, FuncWithArgs nodes,
 * or plain names (as Value strings) according to objtype
 */
void gen_policy_labelitem(PolicyLabelItem &item, const ListCell *rel, int objtype)
{
    if (rel == NULL) {
        return;
    }

    switch (objtype) {
        case O_VIEW:
        case O_TABLE: {
            Oid relid = RangeVarGetRelid((const RangeVar *)rel, NoLock, true);
            if (!OidIsValid(relid)) {
                return;
            }

            item = PolicyLabelItem(0, relid, objtype, "");
            break;
        }
        case O_FUNCTION: {
            FuncWithArgs *func = (FuncWithArgs *)(rel);
            Oid funcid = LookupFuncNameTypeNames(func->funcname, func->funcargs, true);
            if (!OidIsValid(funcid)) {
                return;
            }
            item = PolicyLabelItem(0, funcid, objtype, "");
            break;
        }
        case O_SCHEMA: {
            char *nspname = strVal(rel);
            item = PolicyLabelItem(nspname, NULL, NULL, objtype);
            break;
        }
        default:
            break;
    }

    return;
}

void gen_policy_label_for_commentstmt(PolicyLabelItem &item, const CommentStmt *commentstmt)
{
    ObjectAddress address;
    Relation relation;
    address = get_object_address(commentstmt->objtype, commentstmt->objname, commentstmt->objargs, &relation,
        ShareUpdateExclusiveLock, false);
    switch (commentstmt->objtype) {
        case OBJECT_COLUMN: {
            item = PolicyLabelItem(0, address.objectId, O_COLUMN, strVal(lfirst(list_tail(commentstmt->objname))));
            break;
        }
        case OBJECT_TABLE: {
            item = PolicyLabelItem(0, address.objectId, O_TABLE, "");
            break;
        }
        case OBJECT_FUNCTION: {
            item = PolicyLabelItem(0, address.objectId, O_FUNCTION, "");
            break;
        }
        case OBJECT_SCHEMA: {
            item = PolicyLabelItem(address.objectId, 0, O_SCHEMA, "");
        }
        default:
            break;
    }
    if (relation != NULL) {
        relation_close(relation, NoLock);
    }
}

int get_objtype(int object_type)
{
    int objtype = O_UNKNOWN;
    switch (object_type) {
        case OBJECT_ROLE:
            objtype = O_ROLE;
            break;
        case OBJECT_USER:
            objtype = O_USER;
            break;
        case OBJECT_SCHEMA:
            objtype = O_SCHEMA;
            break;
        case OBJECT_SEQUENCE:
            objtype = O_SEQUENCE;
            break;
        case OBJECT_DATABASE:
            objtype = O_DATABASE;
            break;
        case OBJECT_FOREIGN_SERVER:
            objtype = O_SERVER;
            break;
        case OBJECT_FOREIGN_TABLE:
        case OBJECT_STREAM:
        case OBJECT_TABLE:
            objtype = (object_type == OBJECT_TABLE) ? O_TABLE : O_FOREIGNTABLE;
            break;
        case OBJECT_COLUMN:
            objtype = O_COLUMN;
            break;
        case OBJECT_FUNCTION:
            objtype = O_FUNCTION;
            break;
        case OBJECT_CONTQUERY:
        case OBJECT_VIEW:
            objtype = O_VIEW;
            break;
        case OBJECT_INDEX:
            objtype = O_INDEX;
            break;
        case OBJECT_TABLESPACE:
            objtype = O_TABLESPACE;
            break;
        default:
            break;
    }
    return objtype;
}

CmdType get_rte_commandtype(RangeTblEntry *rte)
{
    if (rte->selectedCols) {
        return CMD_SELECT;
    } else if (rte->insertedCols) {
        return CMD_INSERT;
    } else if (rte->updatedCols) {
        return CMD_UPDATE;
    } else {
        return CMD_UNKNOWN;
    }
}
