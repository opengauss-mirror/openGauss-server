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
 *---------------------------------------------------------------------------------------
 *
 * route.cpp
 *
 *        route
 
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/executor/route.cpp
 *
 *---------------------------------------------------------------------------------------
 */


#include "utils/date.h"
#include "utils/nabstime.h"
#include "utils/palloc.h"
#include "utils/int8.h"
#include "utils/syscache.h"
#include "parser/parse_type.h"
#include "pgxc/locator.h"
#include "pgxc/route.h"
#include "access/hash.h"
#include "catalog/namespace.h"
#include "commands/copy.h"
#include "fmgr.h"

extern void InitBuckets(RelationLocInfo* rel_loc_info, Relation relation);
extern char* getNodenameByIndex(int index);

/*
 * Convert a char* to Datum according to the data type oid.
 *
 * @_in param typeOid: The oid of the type in pg_type catalog.
 * @_in param typeMod: The mod of data type.
 * @_in param value: The string value which need to be converted to datum.
 * @return Return the datum converted from String.
 */
Datum GetDatumFromString(Oid typeOid, int4 typeMod, char *value)
{
    Datum datumValue = (Datum)0;

    switch (typeOid) {
        /* Numeric datatype */
        /* 1. Towards to TINYINT */
        case INT1OID: {
            datumValue = UInt8GetDatum(pg_atoi(value, sizeof(uint8), '\0'));
            break;
        }

        /* 2. Towards to SMALLINT */
        case INT2OID: {
            datumValue = Int16GetDatum(pg_strtoint16(value));
            break;
        }

        /* 3. Towards to INTEGER */
        case INT4OID: {
            datumValue = Int32GetDatum(pg_strtoint32(value));
            break;
        }

        /* 4. Towards to BIGINT */
        case INT8OID: {
            int64 result = 0;
            (void)scanint8(value, false, &result);
            datumValue = Int64GetDatum(result);
            break;
        }

        /* 5. Towards to NUMERIC./DECIMAL */
        case NUMERICOID: {
            datumValue = DirectFunctionCall3(numeric_in, CStringGetDatum(value), ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(typeMod));
            break;
        }

        /* Textual type conversion
         *
         * 6. Towards to CHAR
         */
        case CHAROID: {
            datumValue = DirectFunctionCall3(charin, CStringGetDatum(value), ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(typeMod));

            break;
        }

        /* 7. Towards to CHAR() */
        case BPCHAROID: {
            datumValue = DirectFunctionCall3(bpcharin, CStringGetDatum(value), ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(typeMod));
            break;
        }

        /* 8. Towards to VARCHAR(x) */
        case VARCHAROID: {
            datumValue = DirectFunctionCall3(varcharin, CStringGetDatum(value), ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(typeMod));
            break;
        }

        /* 9. Towards to NVARCHAR2 */
        case NVARCHAR2OID: {
            datumValue = DirectFunctionCall3(nvarchar2in, CStringGetDatum(value), ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(typeMod));
            break;
        }

        /* 10. Towards to TEXT */
        case CLOBOID:
        case TEXTOID: {
            datumValue = CStringGetTextDatum(value);
            break;
        }

        /* Temporal related type conversion */
        /* 11. Towards to DATE */
        case DATEOID: {
            datumValue = DirectFunctionCall1(date_in, CStringGetDatum(value));
            break;
        }

        /* 12. Towards to TIME WITHOUT TIME ZONE */
        case TIMEOID: {
            datumValue = DirectFunctionCall1(time_in, CStringGetDatum(value));
            break;
        }

        /* 13. Towards to TIME WITH TIME ZONE */
        case TIMETZOID: {
            datumValue = DirectFunctionCall1(timetz_in, CStringGetDatum(value));
            break;
        }

        /* 14. Towards to TIMESTAMP WITHOUT TIME ZONE */
        case TIMESTAMPOID: {
            datumValue = DirectFunctionCall3(timestamp_in, CStringGetDatum(value), ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(typeMod));
            break;
        }

        /* 15. Towards to TIMESTAMP WITH TIME ZONE */
        case TIMESTAMPTZOID: {
            datumValue = DirectFunctionCall3(timestamptz_in, CStringGetDatum(value), ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(typeMod));
            break;
        }

        /* 16. Towards to SMALLDATETIME */
        case SMALLDATETIMEOID: {
            datumValue = DirectFunctionCall3(smalldatetime_in, CStringGetDatum(value), ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(typeMod));
            break;
        }

        /* 17. Towards to INTERVAL */
        case INTERVALOID: {
            datumValue = DirectFunctionCall3(interval_in, CStringGetDatum(value), ObjectIdGetDatum(InvalidOid),
                                             Int32GetDatum(typeMod));
            break;
        }

        /* 18. Towards to float4 */
        case FLOAT4OID: {
            datumValue = DirectFunctionCall1(float4in, CStringGetDatum(value));
            break;
        }

        /* 19. Towards to float8 */
        case FLOAT8OID: {
            datumValue = DirectFunctionCall1(float8in, CStringGetDatum(value));
            break;
        }

        default: {
            /*
             * As we already blocked any un-supported datatype at table-creation
             * time, so we shouldn't get here, otherwise the catalog information
             * may gets corrupted.
             */
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_DFS),
                            errmsg("Unsupported data type on typeoid:%u when converting string to datum.", typeOid)));
        }
    }

    return datumValue;
}

// check router_attr is valid or not
bool check_router_attr(char** newval, void** extra, GucSource source)
{
    if (*newval == NULL || strcmp(*newval, "") == 0) {
        return true;
    }

    RouteMsg* attr = (RouteMsg*)palloc0(sizeof(RouteMsg));
    bool is_valid = SendRouter::ParseRouterAttr(*newval, attr);
    pfree_ext(attr);
    return is_valid;
}

void assign_router_attr(const char* newval, void* extra)
{
    if (u_sess->exec_cxt.CurrentRouter != NULL) {
        delete u_sess->exec_cxt.CurrentRouter;
        u_sess->exec_cxt.CurrentRouter = NULL;
    }
    if (newval == NULL || strcmp(newval, "") == 0) {
        u_sess->exec_cxt.is_dn_enable_router = false;
        return;
    }
    if (IS_SINGLE_NODE){
        ereport(NOTICE, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("not support router on singlenode")));
        return;
    }

    // if router is invalid ,will be report error on cn, so router will be valid on dn,
    // and we just set flag on dn, no need to set u_sess->exec_cxt.CurrentRouter
    if (IS_PGXC_DATANODE) {
        u_sess->exec_cxt.is_dn_enable_router = true;
        return;
    }

    char* att_str = pstrdup(newval);
    RouteMsg* attr = (RouteMsg*)palloc0(sizeof(RouteMsg));
    bool is_valid = SendRouter::ParseRouterAttr(att_str, attr);
    if (!is_valid) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("wrong input for router")));
    }
    Oid table_oid = SendRouter::GetRelationOid(attr->table_name);
    int node_id = SendRouter::CountNodeId(attr);
    SendRouter* router_obj =
        New(SESS_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_EXECUTOR)) SendRouter(table_oid, node_id);

    u_sess->exec_cxt.CurrentRouter = router_obj;
    if (IS_PGXC_COORDINATOR && IsConnFromApp()) {
        ereport(NOTICE, (errmsg("Satisfied query will push down to %s directly without sanity check",
                                getNodenameByIndex(node_id))));
    }
    pfree_ext(attr);
    pfree_ext(att_str);
}

SendRouter::SendRouter(Oid table_oid, int node_id)
{
    m_table_oid = table_oid;
    m_node_id = node_id;
}

bool SendRouter::ParseRouterAttr(const char *newval,RouteMsg* attr)
{
    char* attr_str = TrimStr(newval);
    if (attr_str == NULL || attr_str[0] == '\0') {
        return false;
    }
    char* ptr = NULL;
    // get table name
    char* table = TrimStr(strtok_r(attr_str, ",", &ptr));
    // get distribute key
    char *key_str = TrimStr(ptr);
    List* key_vals = NIL;
    ptr = NULL;
    key_str = strtok_r(key_str, "\"", &ptr); // key1,key2..."
    ptr = NULL;
    key_str = TrimStr(strtok_r(key_str, "\"", &ptr));
    char *key = TrimStr(strtok_r(key_str, ",", &ptr));
    while(key != NULL) {
        key_vals = lappend(key_vals, key);
        key = ptr;
        ptr = NULL;
        key = TrimStr(strtok_r(key, ",", &ptr));
    }
    if (table == NULL || strlen(table) == 0 || list_length(key_vals) < 1) {
        return false;
    }
    // guc is valid, set state in router
    attr->table_name = table;
    attr->key_vals = key_vals;
    attr->key_num = key_vals->length;
    return true;
}

Oid SendRouter::GetRelationOid(const char* table_name)
{
    Oid rel_oid = RangeVarGetRelid(makeRangeVarFromNameList(stringToQualifiedNameList(table_name)), NoLock, false);
    if (!OidIsValid(rel_oid)) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("cannot find valid relation oid from router table_name")));
    }
    return rel_oid;
}

int SendRouter::CountNodeId(struct RouteMsg* attr)
{
    Oid table_oid = GetRelationOid(attr->table_name);
    RelationLocInfo* rel_loc_info = GetRelationLocInfo(table_oid);
    int node_id = -1;
    if (rel_loc_info == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid relation locator info from router table_name")));
    }

    if (rel_loc_info->locatorType != LOCATOR_TYPE_HASH && rel_loc_info->locatorType != LOCATOR_TYPE_MODULO) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("only support hash or modulo locator type, the relation "
                       "locator type for router is %c\n", rel_loc_info->locatorType)));
    }

    int dist_keynum = list_length(rel_loc_info->partAttrNum);
    if (list_length(attr->key_vals) != dist_keynum) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("input key number cannot match distribute keys on table %s", attr->table_name)));
    }

    Relation rel = heap_open(table_oid, AccessShareLock);
    TupleDesc tupdesc = CreateTupleDescCopy(RelationGetDescr(rel));

    Oid* col_type = (Oid*)palloc0(dist_keynum * sizeof(Oid));
    int4* col_typemode = (int4*)palloc0(dist_keynum * sizeof(int4));
    Oid* col_conllation = (Oid*)palloc0(dist_keynum * sizeof(Oid));
    int i = 0;
    int t = 0;
    int dist_idx = list_nth_int(rel_loc_info->partAttrNum, t) - 1;
    for (i = 0; i < tupdesc->natts; i++) {
        if (dist_idx == i) {
            col_type[t] = tupdesc->attrs[i].atttypid;
            Type typ = typeidType(col_type[t]);
            col_typemode[t] = ((Form_pg_type)GETSTRUCT(typ))->typtypmod;
            col_conllation[t] = tupdesc->attrs[i].attcollation;
            ReleaseSysCache(typ);
            t++;
            if (t >= dist_keynum)
                break;
            dist_idx = list_nth_int(rel_loc_info->partAttrNum, t) - 1;
        }
    }

    uint32 hash_value = 0;
    int modulo = 0;
    ListCell* cell = NULL;
    i = 0;
    foreach(cell, attr->key_vals) {
        hash_value = hashValueCombination(hash_value,
                                          col_type[i],
                                          GetDatumFromString(col_type[i], col_typemode[i], (char *)lfirst(cell)),
                                          false,
                                          rel_loc_info->locatorType,
                                          col_conllation[i]);
        i++;
    }
    if (unlikely(rel_loc_info->buckets_ptr == NULL)) {
        InitBuckets(rel_loc_info, rel);
    }
    modulo = compute_modulo(abs((int)hash_value), rel_loc_info->buckets_cnt);
    modulo = rel_loc_info->buckets_ptr[modulo];
    node_id = get_node_from_modulo(modulo, rel_loc_info->nodeList);

    pfree_ext(col_type);
    pfree_ext(col_typemode);
    pfree_ext(col_conllation);
    heap_close(rel, AccessShareLock);

    if ((node_id >= list_length(rel_loc_info->nodeList)) || node_id < 0) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("invalid node id get from input key")));
    }

    return node_id;
}

int SendRouter::GetRouterNodeId()
{
    return m_node_id;
}

