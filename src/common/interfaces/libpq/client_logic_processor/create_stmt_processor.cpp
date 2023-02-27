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
 * create_stmt_processor.cpp
 *
 * IDENTIFICATION
 *      src\common\interfaces\libpq\client_logic_processor\create_stmt_processor.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "create_stmt_processor.h"
#include "raw_value.h"
#include "raw_values_cont.h"
#include "values_processor.h"
#include "prepared_statement.h"
#include "prepared_statements_list.h"
#include "client_logic_expressions/expr_processor.h"
#include "client_logic_expressions/expr_parts_list.h"
#include "client_logic_cache/icached_column_manager.h"
#include "client_logic_cache/icached_column.h"
#include "client_logic_cache/cached_columns.h"
#include "client_logic_cache/column_hook_executors_list.h"
#include "client_logic_cache/cache_loader.h"
#include "client_logic_cache/dataTypes.def"
#include "client_logic_cache/types_to_oid.h"
#include <iostream>
#include "client_logic_hooks/column_hook_executor.h"
#include "processor_utils.h"

/* add attributes to rawvalue and datatype to CacheColumn according to the ColumnDef */
RawValue *createStmtProcessor::add_cl_column_type(const ColumnDef * const column, StatementData *statement_data,
    ICachedColumn *cached_column, const int location)
{
    ColumnHookExecutorsList *column_hook_executors_list = cached_column->get_column_hook_executors();
    const char *data_type = column_hook_executors_list->at(0)->get_data_type(column);
    if (data_type == NULL) {
        return NULL;
    }
    RawValue *raw_value = new (std::nothrow) RawValue(statement_data->conn);
    if (raw_value == NULL) {
        fprintf(stderr, "failed to new RawValue object\n");
        free((char *)data_type);
        exit(EXIT_FAILURE);
    }
    if (strlen(data_type) == 0) {
        return raw_value;
    }
    if (strlen(data_type) > 0) {
        if (pg_strncasecmp(data_type, "byteawithoutorderwithequal", sizeof("byteawithoutorderwithequal")) == 0) {
            cached_column->set_data_type(BYTEAWITHOUTORDERWITHEQUALCOLOID);
        } else if (pg_strncasecmp(data_type, "byteawithoutorder", sizeof("byteawithoutorder")) == 0) {
            cached_column->set_data_type(BYTEAWITHOUTORDERCOLOID);
        }

        statement_data->params.new_query_size = strlen(statement_data->params.adjusted_query);
        libpq_free(statement_data->params.new_query);
        statement_data->params.new_query = (char *)malloc(statement_data->params.new_query_size + 1);
        if (statement_data->params.new_query == NULL) {
            free((char *)data_type);
            data_type = NULL;
            return raw_value;
        }
        check_memcpy_s(memcpy_s(statement_data->params.new_query, statement_data->params.new_query_size + 1,
            statement_data->params.adjusted_query, statement_data->params.new_query_size));
        statement_data->params.new_query[statement_data->params.new_query_size] = '\0';

        const char *datatype_cl = "DATATYPE_CL=";
        size_t total_add_len = strlen(datatype_cl) + strlen(data_type) + 1;
        char *chars_to_add = (char *)malloc((total_add_len + 1) * sizeof(char));
        if (chars_to_add != NULL) {
            chars_to_add[0] = 0;
            check_strncat_s(strncat_s(chars_to_add, total_add_len + 1, datatype_cl, strlen(datatype_cl) + 1));
            check_strncat_s(strncat_s(chars_to_add, total_add_len + 1, data_type, strlen(data_type) + 1));
            check_strncat_s(strncat_s(chars_to_add, total_add_len + 1, ",", strlen(",") + 1));
        }
        raw_value->m_location = location;
        raw_value->m_new_location = location + statement_data->offset;
        raw_value->m_processed_data = (unsigned char *)chars_to_add;
        raw_value->m_processed_data_size = total_add_len;

        raw_value->set_data(reinterpret_cast<const unsigned char *>(""), 0);
        statement_data->offset += total_add_len;
    }
    free((char *)data_type);
    data_type = NULL;
    return raw_value;
}

/*
 * find in column constraints (in CREATE TABLE) relevant expressions to Client Logic
 * build our exprVec and our cache accordingly so we can work with this information later
 */
RawValue *createStmtProcessor::trans_column_definition(const ColumnDef * const column_def, ExprPartsList *expr_vec,
    ICachedColumns *cached_columns, ICachedColumns *cached_columns_for_defaults, StatementData *statement_data,
    const char *column_key_name, bool &error)
{
    Constraint *constraint = NULL;
    RawValue *raw_value(NULL);
    if (column_def == NULL || column_def->clientLogicColumnRef == NULL) {
        return NULL;
    }
    /* the data type which is allowed to be encrypted */
    int location = column_def->clientLogicColumnRef->location;
    Oid attr_allow[] = {
        INT1OID, INT4OID, INT2OID, INT8OID, NUMERICOID,
        FLOAT4OID, FLOAT8OID, CHAROID, VARCHAROID, TEXTOID,
        BYTEAOID, BLOBOID, CLOBOID, BPCHAROID, NVARCHAR2OID
    };

    char temp_column_name[NAMEDATALEN * 4];
    errno_t rc = EOK;
    rc = memset_s(temp_column_name, NAMEDATALEN * 4, 0, NAMEDATALEN * 4);
    securec_check_c(rc, "\0", "\0");
    name_list_to_cstring(column_def->typname->names, temp_column_name, sizeof(temp_column_name));
    char *column_name = temp_column_name;
    char *dot_ptr = strchr(column_name, '.');
    if (dot_ptr != NULL) {
        column_name = dot_ptr + 1;
    }

    /* judge if the data type is allowed to be encrypted */
    Oid column_type = TypesMap::typesTextToOidMap.find(column_name);
    bool if_allowd = false;
    for (int i = 0; i < int(sizeof(attr_allow) / sizeof(attr_allow[0])); i++) {
        if (column_type == attr_allow[i])
            if_allowd = true;
    }
    if (!if_allowd) {
        printfPQExpBuffer(&statement_data->conn->errorMessage,
            libpq_gettext("ERROR(CLIENT): encrypted %s column is not implemented\n"), column_name);
        error = true;
        return NULL;
    }
    ICachedColumn *cached_column = statement_data->GetCacheManager()->create_cached_column(column_key_name);
    if (!cached_column) {
        return NULL;
    }
    raw_value = createStmtProcessor::add_cl_column_type(column_def, statement_data, cached_column, location);
    cached_column->set_col_name(column_def->colname);

    if (column_def->constraints != NULL) {
        ListCell *clist = NULL;
        foreach (clist, column_def->constraints) {
            constraint = (Constraint *)lfirst(clist);
            Assert(IsA(constraint, Constraint));
            /* not null or default and other column definition check */
            switch (constraint->contype) {
                /* for default */
                case CONSTR_DEFAULT: {
                    Assert(constraint->raw_expr);
                    if (!exprProcessor::expand_expr(constraint->raw_expr, statement_data, expr_vec)) {
                        fprintf(stderr, "failed to new default value of client_logic\n");
                    }
                    if (cached_column->get_column_hook_executors()->at(0) != NULL) {
                        cached_column->get_column_hook_executors()->at(0)->set_data_type(column_def, cached_column);
                    }
                    if (column_def->typname != NULL) {
                        name_list_to_cstring(column_def->typname->names, temp_column_name, sizeof(temp_column_name));
                    }
                    column_name = temp_column_name;
                    dot_ptr = strchr(column_name, '.');
                    if (dot_ptr != NULL) {
                        column_name = dot_ptr + 1;
                    }
                    cached_column->set_origdatatype_oid(TypesMap::typesTextToOidMap.find(column_name));
                    if (cached_columns_for_defaults != NULL) {
                        cached_columns_for_defaults->push(cached_column);
                    }
                    break;
                }
                default: {
                    if (!check_constraint(constraint, cached_column->get_data_type(), column_def->colname,
                        cached_columns, statement_data)) {
                        error = true;
                        if (cached_column) {
                            delete cached_column;
                            cached_column = NULL;
                        }
                        if (raw_value) {
                            delete raw_value;
                            raw_value = NULL;
                        }
                        return NULL;
                    }
                    break;
                }
            }
        }
    }
    cached_columns->push(cached_column);
    return raw_value;
}

bool createStmtProcessor::check_distributeby(const DistributeBy *distributeby, const char *colname,
    StatementData *statement_data)
{
    if (distributeby == NULL || distributeby->disttype == DistributionType::DISTTYPE_HASH) {
        return true;
    } else {
        ListCell *cell = NULL;
        foreach (cell, distributeby->colname) {
            char *distribute_colname = strVal(lfirst(cell));
            if (strcmp(distribute_colname, colname) == 0) {
                printfPQExpBuffer(&statement_data->conn->errorMessage,libpq_gettext(
                    "ERROR(CLIENT): encrypted column only support distribute by hash clause\n"));
                return false;
            }
        }
        return true;
    }
}

/*
 * process a CREATE TABLE query
 * this function is meant to be executed before the query is sent to the server
 */
bool createStmtProcessor::run_pre_create_statement(const CreateStmt * const stmt, StatementData *statement_data)
{
    if (!statement_data->GetCacheManager()->has_global_setting()) {
        return true;
    }

    /* search for columns in the CREATE TABLE command */
    ListCell *elements = NULL;
    ExprPartsList expr_vec;
    CachedColumns cached_columns(false, true);
    CachedColumns cached_columns_for_defaults(false);
    foreach (elements, stmt->tableElts) {
        Node *element = (Node *)lfirst(elements);
        switch (nodeTag(element)) {
            /* like id integer */
            case T_ColumnDef: {
                ColumnDef *column = (ColumnDef *)element;
                if (!column->clientLogicColumnRef) {
                    continue;
                }
                if (column->colname != NULL &&
                    !check_distributeby(stmt->distributeby, column->colname, statement_data)) {
                    return false;
                }
                if (!process_column_defintion(column, element, &expr_vec, &cached_columns, 
                    &cached_columns_for_defaults, statement_data)) {
                    return false;
                }
                break;
            }
            /* check, unique and other Constraint */
            case T_Constraint: {
                Constraint *constraint = (Constraint*)element;
                if (constraint->keys != NULL) {
                    ListCell *ixcell = NULL;
                    foreach (ixcell, constraint->keys) {
                        char *ikname = strVal(lfirst(ixcell));
                        for (size_t i = 0; i < cached_columns.size(); i++) {
                            if (strcmp((cached_columns.at(i))->get_col_name(), ikname) == 0 && !check_constraint(
                                constraint, cached_columns.at(i)->get_data_type(), 
                                ikname, &cached_columns, statement_data)) {
                                return false;
                            }
                        }
                    }
                } else if (constraint->raw_expr != NULL) {
                    if (!transform_expr(constraint->raw_expr, "", &cached_columns, statement_data)) {
                        return false;
                    }
                }
                break;
            }
            default:
                break;
        }
    }

    if (statement_data->conn->client_logic->rawValuesForReplace->size() > 0) {
        PreparedStatement *prepared_statement =
            statement_data->conn->client_logic->pendingStatements->get_or_create(statement_data->stmtName);
        if (!prepared_statement) {
            Assert(false);
            return false;
        }
        /* update cachecolumn in cache */
        prepared_statement->cacheRefresh |= CacheRefreshType::COLUMNS;
    }

    /* check if there are any columns with a DEFAULT that require further processing */
    if (expr_vec.empty()) {
        return true;
    }

    /*
     * process values in the CREATE TABLE statement - to support default values in columns
     * example query: CREATE TABLE distributors ( name varchar(40) DEFAULT 'Luso Films');
     */
    RawValuesList raw_values_list;
    if (!RawValues::get_raw_values_from_consts_vec(&expr_vec, statement_data, 0, &raw_values_list)) {
        return false;
    }
    return ValuesProcessor::process_values(statement_data, &cached_columns_for_defaults, 1,
        &raw_values_list);
}

bool createStmtProcessor::process_column_defintion(ColumnDef *column, Node *element, ExprPartsList *expr_vec,
    ICachedColumns *cached_columns, ICachedColumns *cached_columns_for_defaults, StatementData *statement_data)
{
    if (!(column->clientLogicColumnRef->column_key_name)) {
        fprintf(stderr, "ERROR(CLIENT): column encryption key name cannot be empty\n");
        return false;
    }

    if (column->typname != NULL && column->typname->arrayBounds != NULL) {
        fprintf(stderr, "ERROR(CLIENT): Creating encrypted columns of type array is not supported\n");
        return false;
    }

    char column_key_name[NAMEDATALEN * 4];
    errno_t rc = EOK;
    rc = memset_s(column_key_name, NAMEDATALEN * 4, 0, NAMEDATALEN * 4);
    securec_check_c(rc, "\0", "\0");
    if (!name_list_to_cstring(column->clientLogicColumnRef->column_key_name, column_key_name,
        sizeof(column_key_name))) {
        fprintf(stderr, "ERROR(CLIENT): column encryption key name cannot be empty\n");
        return false;
    }

    /* 4 is database + schema + object + extra padding */
    char object_fqdn[NAMEDATALEN * 4] = {0};
    size_t object_fqdn_size = statement_data->GetCacheManager()->get_object_fqdn(column_key_name, false, object_fqdn);
    if (object_fqdn_size == 0) {
        statement_data->conn->client_logic->cacheRefreshType = CacheRefreshType::CACHE_ALL;
        statement_data->GetCacheManager()->load_cache(statement_data->conn);
        object_fqdn_size = statement_data->GetCacheManager()->get_object_fqdn(column_key_name, false, object_fqdn);
        if (object_fqdn_size == 0) {
            printfPQExpBuffer(&(statement_data->conn->errorMessage), 
                libpq_gettext("ERROR(CLIENT):error while trying to retrieve column encryption key from cache\n"));
            return false;
        }
    }

    bool error = false;
    RawValue *raw_value = trans_column_definition((ColumnDef *)element, expr_vec, cached_columns,
        cached_columns_for_defaults, statement_data, object_fqdn, error);
    if (error) {
        if (raw_value) {
            delete raw_value;
            raw_value = NULL;
        }
        return false;
    }
    if (raw_value) {
        statement_data->conn->client_logic->rawValuesForReplace->add(raw_value);
    }
    return true;
}

/* for unique and check primary definiton in column */
bool createStmtProcessor::check_constraint(Constraint *constraint, const Oid type_id, char *name,
    ICachedColumns *cached_columns, StatementData* statement_data)
{
    if (constraint != NULL) {
        switch (constraint->contype) {
            case CONSTR_UNIQUE:
                if (type_id == BYTEAWITHOUTORDERCOLOID) {
                    printfPQExpBuffer(&statement_data->conn->errorMessage, libpq_gettext(
                        "ERROR(CLIENT): could not support random encrypted columns as unique constraints\n"));
                    return false;
                }
                break;
            case CONSTR_PRIMARY:
                if (type_id == BYTEAWITHOUTORDERCOLOID) {
                    printfPQExpBuffer(&statement_data->conn->errorMessage, libpq_gettext(
                        "ERROR(CLIENT): could not support random encrypted columns as primary key constraints\n"));
                    return false;
                }
                break;
            case CONSTR_CHECK:
                if (!transform_expr(constraint->raw_expr, name, cached_columns, statement_data)) {
                    return false;
                }
                break;
            default:
                break;
        }
    }
    return true;
}

/* check (id>0) encryption column doesn't support check */
bool createStmtProcessor::transform_expr(Node *expr, char *name, ICachedColumns *cached_columns,
    StatementData* statement_data)
{
    if (expr == NULL) {
        return false;
    }
    switch (nodeTag(expr)) {
        case T_ColumnRef: {
            ColumnRef *column = (ColumnRef *)expr;
            char *col_name = get_column_name(column);
            if (col_name == NULL) {
                return false;
            }
            if (name != NULL && strcmp(col_name, name) == 0) {
                printfPQExpBuffer(&statement_data->conn->errorMessage, libpq_gettext(
                    "ERROR(CLIENT): could not support encrypted columns as check constraints\n"));
                return false;
            }
            if (cached_columns != NULL) {
                for (size_t i = 0; i < cached_columns->size(); i++) {
                    if (cached_columns->at(i)->get_col_name() != NULL &&
                        strcmp(cached_columns->at(i)->get_col_name(), col_name) == 0) {
                        printfPQExpBuffer(&statement_data->conn->errorMessage, libpq_gettext(
                            "ERROR(CLIENT): could not support encrypted columns as check constraints\n"));
                        return false;
                    }
                }
            }
            break;
        }
        case T_A_Expr: {
            A_Expr *a = (A_Expr *)expr;
            Node *lexpr = a->lexpr;
            Node *rexpr = a->rexpr;
            bool lresult = transform_expr(lexpr, name, cached_columns, statement_data);
            bool rresult = transform_expr(rexpr, name, cached_columns, statement_data);
            if (lresult && rresult) {
                return true;
            } else {
                return false;
            }
            break;
        }
        case T_NullTest: {
            NullTest *ntest = (NullTest *)expr;
            if (ntest->nulltesttype != IS_NULL && ntest->nulltesttype != IS_NOT_NULL) {
                fprintf(stderr, "unrecognized nulltesttype: %d\n", (int)ntest->nulltesttype);
                return false;
            }
            break;
        }
        default:
            break;
    }
    return true;
}
char *createStmtProcessor::get_column_name(ColumnRef *column)
{
    char *col_name = NULL;
    switch (list_length(column->fields)) {
        case 1:
            col_name = strVal(linitial(column->fields));
            break;
        case 2:
            col_name = strVal(lsecond(column->fields));
            break;
        case 3:
            col_name = strVal(lthird(column->fields));
            break;
        case 4:
            col_name = strVal(lfourth(column->fields));
        default:
            char fullColumnFields[NAMEDATALEN * 4];
            name_list_to_cstring(column->fields, fullColumnFields, sizeof(fullColumnFields));
            fprintf(stderr, "improper relation name (too many dotted names): %s\n", fullColumnFields);
            break;
    }
    return col_name;
}
