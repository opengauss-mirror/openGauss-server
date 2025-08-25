/*
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * --------------------------------------------------------------------------------------
 *
 * varlena.c
 *	  Functions in common use.
 *
 * IDENTIFICATION
 *        contrib/shark/varlena.c
 * 
 * --------------------------------------------------------------------------------------
 */
#include "knl/knl_variable.h"

#include "postgres.h"
#include "miscadmin.h"
#include "knl/knl_instance.h"
#include <stdlib.h>
#include <cmath>

#include "access/sysattr.h"
#include "knl/knl_variable.h"
#include "commands/extension.h"
#include "commands/dbcommands.h"
#include "nodes/execnodes.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/fmgroids.h"
#include "parser/scansup.h"
#include "tcop/ddldeparse.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_attrdef.h"
#include "catalog/indexing.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_trigger.h"

typedef enum
{
	OBJECT_TYPE_AGGREGATE_FUNCTION = 1,
	OBJECT_TYPE_CHECK_CONSTRAINT,
	OBJECT_TYPE_DEFAULT_CONSTRAINT,
	OBJECT_TYPE_FOREIGN_KEY_CONSTRAINT,
	OBJECT_TYPE_TSQL_SCALAR_FUNCTION,
	OBJECT_TYPE_ASSEMBLY_SCALAR_FUNCTION,
	OBJECT_TYPE_ASSEMBLY_TABLE_VALUED_FUNCTION,
	OBJECT_TYPE_TSQL_INLINE_TABLE_VALUED_FUNCTION,
	OBJECT_TYPE_INTERNAL_TABLE,
	OBJECT_TYPE_TSQL_STORED_PROCEDURE,
	OBJECT_TYPE_ASSEMBLY_STORED_PROCEDURE,
	OBJECT_TYPE_PLAN_GUIDE,
	OBJECT_TYPE_PRIMARY_KEY_CONSTRAINT,
	OBJECT_TYPE_RULE,
	OBJECT_TYPE_REPLICATION_FILTER_PROCEDURE,
	OBJECT_TYPE_SYSTEM_BASE_TABLE,
	OBJECT_TYPE_SYNONYM,
	OBJECT_TYPE_SEQUENCE_OBJECT,
	OBJECT_TYPE_SERVICE_QUEUE,
	OBJECT_TYPE_ASSEMBLY_DML_TRIGGER,
	OBJECT_TYPE_TSQL_TABLE_VALUED_FUNCTION,
	OBJECT_TYPE_TSQL_DML_TRIGGER,
	OBJECT_TYPE_TABLE,
	OBJECT_TYPE_UNIQUE_CONSTRAINT,
	OBJECT_TYPE_VIEW,
    OBJECT_TYPE_MATERIALIZED_VIEW,
	OBJECT_TYPE_EXTENDED_STORED_PROCEDURE
} PropertyType;

Oid tsql_get_proc_nsp_oid(Oid object_id);
Oid tsql_get_constraint_nsp_oid(Oid object_id, Oid user_id);

extern char* GetPhysicalSchemaName(char *dbName, const char *schemaName);
static bool is_shared_schema(const char *name);
static char* GetCurrentPhysicalSchemaName(char* schemaName);
static Oid search_oid_in_class(char* obj_name, Oid schema_oid, char* type_name = NULL);
static Oid search_oid_in_proc(char* obj_name, Oid schema_oid);
static Oid search_oid_in_trigger(char* obj_name, Oid schema_oid);
static Oid search_oid_in_cons(char* obj_name, Oid schema_oid);
static Oid search_oid_in_schema(char* schema_name, char* obj_name, char *object_type);
static int search_type_in_class(Oid* schema_id, Oid object_id, char* object_name);
static int search_type_in_proc(Oid* schema_id, Oid object_id, char* object_name);
static int search_type_in_attr(Oid* schema_id, Oid object_id, char* object_name);
static int search_type_in_cons(Oid* schema_id, Oid object_id, char* object_name);
static int search_type_in_trigger(Oid* schema_id, Oid object_id, char* object_name);
static int dealwith_property(int type, Oid schema_id, Oid object_id, char* property);
static inline int dealwith_type_ownerid(int type, Oid schema_id);
static inline int dealwith_type_defcnst(int type);
static inline int dealwith_type_exec_bound(int type, char* property);
static inline int dealwith_type_status_format(int type);
static inline int dealwith_type_issysshipped(int type, Oid schema_id);
static inline int dealwith_type_dete(int type, Oid object_id);
static inline int dealwith_type_procedure(int type);
static inline int dealwith_type_table(int type);
static inline int dealwith_type_view(int type);
static inline int dealwith_type_usertable(int type, Oid schema_id);
static inline int dealwith_type_tablefunc(int type);
static inline int dealwith_type_inlinefunc(int type);
static inline int dealwith_type_scalarfunc(int type);
static inline int dealwith_type_pk(int type);
static inline int dealwith_type_indexed(int type, Oid object_id);
static inline int dealwith_type_trigger(int type);


static Oid search_oid_in_proc(char* obj_name, Oid schema_oid)
{
	Oid id = InvalidOid;
	CatCList* catlist = NULL;
	HeapTuple tuple;
	Form_pg_proc procform;
#ifndef ENABLE_MULTIPLE_NODES
	if (t_thrd.proc->workingVersionNum < 92470) {
		catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(obj_name));
	} else {
		catlist = SearchSysCacheList1(PROCALLARGS, CStringGetDatum(obj_name));
	}
#else
		catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(obj_name));
#endif

	for (int i = 0; i < catlist->n_members; i++) {
		tuple = t_thrd.lsc_cxt.FetchTupleFromCatCList(catlist, i);
		procform = (Form_pg_proc)GETSTRUCT(tuple);
		if (procform->pronamespace != schema_oid) {
			continue;
		}
		id = HeapTupleGetOid(tuple);
		break;
	}
	ReleaseSysCacheList(catlist);
	return id;
}

static Oid search_oid_in_trigger(char* obj_name, Oid schema_oid)
{
	Oid id = InvalidOid;
	Relation tgrel;
	ScanKeyData keys[1];
	SysScanDesc tgscan;
	HeapTuple tuple;

	ScanKeyInit(&keys[0], Anum_pg_trigger_tgname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(obj_name));

	tgrel = heap_open(TriggerRelationId, AccessShareLock);
	tgscan = systable_beginscan(tgrel, TriggerNameIndexId, true, SnapshotNow, 1, keys);
	if (HeapTupleIsValid(tuple = systable_getnext(tgscan))) {
		id = HeapTupleGetOid(tuple);
	}
	systable_endscan(tgscan);
	heap_close(tgrel, AccessShareLock);
	return id;
}

static Oid search_oid_in_cons(char* obj_name, Oid schema_oid)
{
	Oid id = InvalidOid;
	Relation tgrel;
	ScanKeyData skeys[2];
	SysScanDesc tgscan;
	HeapTuple tuple;

	ScanKeyInit(&skeys[0], Anum_pg_constraint_conname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(obj_name));

    ScanKeyInit(&skeys[1],
        Anum_pg_constraint_connamespace,
        BTEqualStrategyNumber,
        F_OIDEQ,
        ObjectIdGetDatum(schema_oid));

	tgrel = heap_open(ConstraintRelationId, AccessShareLock);
	tgscan = systable_beginscan(tgrel, ConstraintNameNspIndexId, true, SnapshotNow, 2, skeys);
	if (HeapTupleIsValid(tuple = systable_getnext(tgscan))) {
		id = HeapTupleGetOid(tuple);
	}
	systable_endscan(tgscan);
	heap_close(tgrel, AccessShareLock);
	return id;
}

static Oid search_oid_in_class(char* obj_name, Oid schema_oid, char* type_name)
{
    Oid id = InvalidOid;
    HeapTuple tuple;
    Form_pg_class classform;

    tuple = SearchSysCache2(RELNAMENSP, PointerGetDatum(obj_name), ObjectIdGetDatum(schema_oid));
    if (!HeapTupleIsValid(tuple)) {
        return InvalidOid;
    }
    classform = (Form_pg_class) GETSTRUCT(tuple);
    id = HeapTupleGetOid(tuple);
    if (type_name != NULL) {
        switch (classform->relkind) {
            case 'r':
                if ((strcmp(type_name, "U") != 0 && strcmp(type_name, "S") != 0) ||
                    (strcmp(type_name, "U") == 0 && IsSystemObjOid(id)) ||
                    (strcmp(type_name, "S") == 0 && !IsSystemObjOid(id))) {
                    id = InvalidOid;
                }
                break;
            case 'v':
                if (strcmp(type_name, "V") != 0) {
                    id = InvalidOid;
                }
                break;
            case 'S':
                if (strcmp(type_name, "SO") != 0) {
                    id = InvalidOid;
                }
                break;
            default:
                id = InvalidOid;
                break;
        }
    } else if (classform->relkind == 'i' || classform->relkind == 'l') {
        id = InvalidOid;
    }
    ReleaseSysCache(tuple);

    return id;
}

static Oid search_oid_in_schema(char* schema_name, char* obj_name, char *object_type)
{
	Oid schema_oid = InvalidOid;
	if (schema_name != NULL && strlen(schema_name) > 0) 
    { 
        schema_oid = get_namespace_oid(schema_name, true);
        if (!OidIsValid(schema_oid))
            return InvalidOid;
		if (!pg_namespace_ownercheck(schema_oid, GetUserId()))
        {
            ereport(NOTICE, 
                   (errmsg("Permission denied for schema %s", schema_name)));
            return InvalidOid;
        }
    } else {
		char *token, *saveptr;
		char* search_path = pstrdup(u_sess->attr.attr_common.namespace_search_path);
		token = strtok_r(search_path, ",", &saveptr);
		if (strcmp(token, "\"$user\"") == 0) {
			token = strtok_r(NULL, ",", &saveptr);
		}
		schema_oid = SchemaNameGetSchemaOid(token, false);
	}

	Oid id = InvalidOid;
	if (object_type != NULL)
	{
		char* type_name = pg_strtoupper(object_type);
		/* eg. D-database support "U ", but not "U  " with two spaces */
        if (strlen(type_name) == 2 && type_name[1] == ' ') {
            type_name[1] = '\0';
        }
        if (strcmp(type_name, "S") == 0 || strcmp(type_name, "U") == 0 || strcmp(type_name, "V") == 0 ||
            strcmp(type_name, "SO") == 0)
        {
            id = search_oid_in_class(obj_name, schema_oid, type_name);
        } else if (strcmp(type_name, "C") == 0 || strcmp(type_name, "D") == 0 || strcmp(type_name, "F") == 0 ||
            strcmp(type_name, "PK") == 0 || strcmp(type_name, "UQ") == 0) {
            id = search_oid_in_cons(obj_name, schema_oid);
        } else if (strcmp(type_name, "AF") == 0 || strcmp(type_name, "FN") == 0 ||
            strcmp(type_name, "P") == 0 || strcmp(type_name, "PC") == 0) {
            id = search_oid_in_proc(obj_name, schema_oid);
        } else if (strcmp(type_name, "TR") == 0) {
            id = search_oid_in_trigger(obj_name, schema_oid);
        } else {
            return InvalidOid;
        }
	} else {
		if (id == InvalidOid) {
			id = search_oid_in_class(obj_name, schema_oid);
		}
		if (id == InvalidOid) {
			id = search_oid_in_cons(obj_name, schema_oid);
		}
		if (id == InvalidOid) {
			id = search_oid_in_proc(obj_name, schema_oid);
		}
		if (id == InvalidOid) {
			id = search_oid_in_trigger(obj_name, schema_oid);
		}
	}
	return id;
}

static int search_type_in_class(Oid* schema_id, Oid object_id, char* object_name)
{
	int type;
	HeapTuple tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(object_id));
	if (HeapTupleIsValid(tuple))
	{
		Form_pg_class pg_class = (Form_pg_class) GETSTRUCT(tuple);

		object_name = NameStr(pg_class->relname);

		if (pg_class_aclcheck(object_id, GetUserId(), ACL_SELECT) == ACLCHECK_OK)
			*schema_id = get_rel_namespace(object_id);

		/* 
		 * Get the type of the object 
		 */
		if ((pg_class->relpersistence == 'p' || pg_class->relpersistence == 'u' || pg_class->relpersistence == 't' ||
				pg_class->relpersistence == 'g') && (pg_class->relkind == 'r'))
		{
            type = OBJECT_TYPE_TABLE;
		}
		else if (pg_class->relkind == 'v')
			type = OBJECT_TYPE_VIEW;
        else if (pg_class->relkind == 'm')
			type = OBJECT_TYPE_MATERIALIZED_VIEW;
		else if (pg_class->relkind == 's')
			type = OBJECT_TYPE_SEQUENCE_OBJECT;

		ReleaseSysCache(tuple);
	}
	return type;
}

static int search_type_in_proc(Oid* schema_id, Oid object_id, char* object_name)
{
	int type;
	HeapTuple tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(object_id));
	if (HeapTupleIsValid(tuple))
	{
		if (pg_proc_aclcheck(object_id, GetUserId(), ACL_EXECUTE) == ACLCHECK_OK)
		{
			Form_pg_proc procform = (Form_pg_proc) GETSTRUCT(tuple);

			object_name = NameStr(procform->proname);

			*schema_id = tsql_get_proc_nsp_oid(object_id);
			
			char prokind = get_func_prokind(HeapTupleGetOid(tuple));
			if (prokind == 'p')
			type = OBJECT_TYPE_TSQL_STORED_PROCEDURE;
			else if (prokind == 'a')
				type = OBJECT_TYPE_AGGREGATE_FUNCTION;
			else
			{
				/*
					* Check whether the object is SQL DML trigger(TR), SQL table-valued-function (TF),
					* SQL inline table-valued function (IF), SQL scalar function (FN).
					*/
				char	*temp = format_type_with_typemod(procform->prorettype, -1);
				/*
					* If the prorettype of the pg_proc object is "trigger", then the type of the object is "TR"
					*/
				if (pg_strcasecmp(temp, "trigger") == 0) 
					type = OBJECT_TYPE_TSQL_DML_TRIGGER;
				/*
					* For SQL table-valued-functions and SQL inline table-valued functions, re-implement the existing SQL.
					*/
				else if (procform->proretset)
				{
					HeapTuple tp;
					tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(procform->prorettype));
					if (HeapTupleIsValid(tp))
					{
						Form_pg_type typeform = (Form_pg_type) GETSTRUCT(tuple);

						if (typeform->typtype == 'c')
							type = OBJECT_TYPE_TSQL_TABLE_VALUED_FUNCTION;
						else
							type = OBJECT_TYPE_TSQL_INLINE_TABLE_VALUED_FUNCTION;

						ReleaseSysCache(tp);
					}
				}
				else
					type = OBJECT_TYPE_TSQL_SCALAR_FUNCTION;
				
				pfree_ext(temp);
			}
		}
		ReleaseSysCache(tuple);
	}
	return type;
}

static int search_type_in_attr(Oid* schema_id, Oid object_id, char* object_name)
{
	Relation	attrdefrel;
	ScanKeyData key;
	SysScanDesc attrscan;
	int type;

	attrdefrel = table_open(AttrDefaultRelationId, AccessShareLock);
	ScanKeyInit(&key,
				ObjectIdAttributeNumber,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(object_id));

	attrscan = systable_beginscan(attrdefrel, AttrDefaultOidIndexId, false,
								NULL, 1, &key);

	HeapTuple tuple = systable_getnext(attrscan);
	if (HeapTupleIsValid(tuple))
	{
		/*
			* scan pg_attribute catalog to find the corresponding row.
			* This pg_attribute pbject will be helpful to check whether the object is DEFAULT (D)
			* and to find the schema_id.
			*/
		Form_pg_attrdef atdform = (Form_pg_attrdef) GETSTRUCT(tuple);
		Relation	attrRel;
		ScanKeyData key[2];
		SysScanDesc scan;
		HeapTuple	tup;
		Oid user_id = GetUserId();
		if (pg_attribute_aclcheck(atdform->adrelid, atdform->adnum, user_id, ACL_SELECT) &&
			pg_attribute_aclcheck(atdform->adrelid, atdform->adnum, user_id, ACL_INSERT) &&
			pg_attribute_aclcheck(atdform->adrelid, atdform->adnum, user_id, ACL_UPDATE) &&
			pg_attribute_aclcheck(atdform->adrelid, atdform->adnum, user_id, ACL_REFERENCES))
		{
			attrRel = table_open(AttributeRelationId, RowExclusiveLock);

			ScanKeyInit(&key[0],
						Anum_pg_attribute_attrelid,
						BTEqualStrategyNumber, F_OIDEQ,
						ObjectIdGetDatum(atdform->adrelid));
			ScanKeyInit(&key[1],
						Anum_pg_attribute_attnum,
						BTEqualStrategyNumber, F_INT2EQ,
						Int16GetDatum(atdform->adnum));

			scan = systable_beginscan(attrRel, AttributeRelidNumIndexId, true,
									NULL, 2, key);

			if (HeapTupleIsValid(tup = systable_getnext(scan)))
			{
				Form_pg_attribute attrform = (Form_pg_attribute) GETSTRUCT(tup);

				if (attrform->atthasdef && !atdform->adgencol)
				{
					object_name = NameStr(attrform->attname);
					type = OBJECT_TYPE_DEFAULT_CONSTRAINT;
					if (pg_class_aclcheck(atdform->adrelid, user_id, ACL_SELECT) == ACLCHECK_OK)
						*schema_id = get_rel_namespace(atdform->adrelid);
				}
			}

			systable_endscan(scan);

			table_close(attrRel, RowExclusiveLock);
		}

	}
	systable_endscan(attrscan);
	table_close(attrdefrel, AccessShareLock);
	return type;
}

static int search_type_in_cons(Oid* schema_id, Oid object_id, char* object_name)
{
	int type;
	HeapTuple tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(object_id));
	if (HeapTupleIsValid(tuple))
	{
		Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(tuple);
		object_name = NameStr(con->conname);
		*schema_id = tsql_get_constraint_nsp_oid(object_id, GetUserId());
		/*
			* If the contype is 'f' on the pg_constraint object, then it is a Foreign key constraint
			*/
		if (con->contype == 'f')
			type = OBJECT_TYPE_FOREIGN_KEY_CONSTRAINT;
		/*
			* If the contype is 'p' on the pg_constraint object, then it is a Primary key constraint
			*/
		else if (con->contype == 'p')
			type = OBJECT_TYPE_PRIMARY_KEY_CONSTRAINT;
		/*
			* Reimplemented the existing SQL .
			* If the contype is 'c' and conrelid is 0 on the pg_constraint object, then it is a Check constraint
			*/
		else if (con->contype == 'c' && con->conrelid != 0)
			type = OBJECT_TYPE_CHECK_CONSTRAINT;
		
		ReleaseSysCache(tuple);
	}
	return type;
}

static int search_type_in_trigger(Oid* schema_id, Oid object_id, char* object_name)
{
	Relation    trigDesc;
	HeapTuple   tup;
	Form_pg_trigger trig;
	int type;

	trigDesc = table_open(TriggerRelationId, AccessShareLock);
	tup = get_catalog_object_by_oid(trigDesc, object_id);
	if (HeapTupleIsValid(tup)) {
		trig = (Form_pg_trigger) GETSTRUCT(tup);
		object_name = NameStr(trig->tgname);
		*schema_id = get_rel_namespace(trig->tgrelid);
		type = OBJECT_TYPE_ASSEMBLY_DML_TRIGGER;
	}

	table_close(trigDesc, AccessShareLock);
	return type;
}

extern "C" Datum rand_seed(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(rand_seed);
Datum rand_seed(PG_FUNCTION_ARGS)
{
    int64 n = PG_GETARG_INT64(0);

    gs_srandom((unsigned int)n);
    float8 result;
    /* result [0.0 - 1.0) */
    result = (double)gs_random() / ((double)MAX_RANDOM_VALUE + 1);

    PG_RETURN_FLOAT8(result);
}

extern "C" Datum object_id_internal(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(object_id_internal);
Datum
object_id_internal(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0)) {
		PG_RETURN_NULL();
	}
	char *object_name = text_to_cstring(PG_GETARG_TEXT_P(0));
    char *object_type = PG_NARGS() == 1 ? NULL : text_to_cstring(PG_GETARG_TEXT_P(1));

	if (strlen(object_name) < 1) {
		PG_RETURN_NULL();
	}
    
	char* db_name = NULL;
	char* schema_name = NULL;
	char* obj_name = NULL;

    char *tok = object_name;
    while (*tok) {
        // Detected two consecutive points
        if (*tok == '.' && *(tok + 1) == '.') {
            *tok = '\0';
            db_name = object_name;
            tok += 2;
            break;
        } else {
            tok++;
        }
    }
    if (db_name != NULL) {
        obj_name = tok;
    } else {
        List* nameList;
        PG_TRY();
        {
            nameList = stringToQualifiedNameList(object_name);
        }
        PG_CATCH();
        {
            FlushErrorState();
            PG_RETURN_NULL();
        }
        PG_END_TRY();
        switch (list_length(nameList)) {
            case 1:
                obj_name = strVal(linitial(nameList));
                break;
            case 2:
                obj_name = strVal(lsecond(nameList));
                schema_name = GetCurrentPhysicalSchemaName(strVal(linitial(nameList)));
                break;
            case 3:
                obj_name = strVal(lthird(nameList));
                schema_name = GetCurrentPhysicalSchemaName(strVal(lsecond(nameList)));
                db_name = strVal(linitial(nameList));
                break;
            default:
                PG_RETURN_NULL();
                break;
        }
	}

	if (obj_name == NULL || strlen(obj_name) < 1) {
		PG_RETURN_NULL();
	}

    char* current_db = get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId, false);
    if (db_name != NULL && strcmp(db_name, current_db) != 0) {
        pfree_ext(current_db);
        PG_RETURN_NULL();
	}
    pfree_ext(current_db);

	Oid id = search_oid_in_schema(schema_name, obj_name, object_type);
	if (id == InvalidOid) {
		PG_RETURN_NULL();
	}
	PG_RETURN_OID(id);
}

extern "C" Datum objectproperty_internal(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(objectproperty_internal);
Datum
objectproperty_internal(PG_FUNCTION_ARGS)
{
	Oid		object_id;
	Oid		schema_id = InvalidOid;
	char		*property;
	Oid		user_id = GetUserId();
	int		type = 0;
	char		*object_name = NULL;
	char		*nspname = NULL;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();
	else
	{
		object_id = (Oid) PG_GETARG_INT32(0);
		property = text_to_cstring(PG_GETARG_TEXT_P(1));
        if (strlen(property) < 1 || object_id == InvalidOid) {
            PG_RETURN_NULL();
        }
		property = downcase_truncate_identifier(property, strlen(property), true);
		property = remove_trailing_spaces(property);
	}

	/*
	 * Search for the object_id in pg_class, pg_proc, pg_attrdef, pg_constraint.
	 * If the object_id is not found in any of the above catalogs, return NULL.
	 * Else, get the object name, type of the object and the schema_id in which 
	 * the object is present.
	 */

	/* pg_class */
	type = search_type_in_class(&schema_id, object_id, object_name);
	/* pg_proc */
	if (!schema_id)
	{
		type = search_type_in_proc(&schema_id, object_id, object_name);
	}
	/* pg_attrdef */
	if (!schema_id)
	{
		type = search_type_in_attr(&schema_id, object_id, object_name);
	}
	/* pg_constraint */
	if (!schema_id)
	{
		type = search_type_in_cons(&schema_id, object_id, object_name);
	}
    /* pg_trigger */
	if (!schema_id)
	{
        type = search_type_in_trigger(&schema_id, object_id, object_name);
	}

	/*
	 * If the object_id is not found or user does not have enough privileges on the object and schema,
	 * Return NULL.
	 */
	if (!schema_id || !pg_namespace_ownercheck(schema_id, user_id))
	{
		pfree_ext(property);
		PG_RETURN_NULL();
	}

	/*
	 * schema_id found should be in sys.schemas view except 'sys'.
	 */
	nspname = get_namespace_name(schema_id);

	if (!(nspname && pg_strcasecmp(nspname, "sys") == 0) && 
		(!nspname || pg_strcasecmp(nspname, "pg_catalog") == 0 ||
		pg_strcasecmp(nspname, "pg_toast") == 0 ||
		pg_strcasecmp(nspname, "public") == 0))
	{
		pfree_ext(property);
		if (nspname)
			pfree_ext(nspname);

		PG_RETURN_NULL();
	}

	pfree_ext(nspname);

	int result = dealwith_property(type, schema_id, object_id, property);
	
	if (property)
		pfree_ext(property);
	if (result == -1) {
		PG_RETURN_NULL();
	} else {
		PG_RETURN_INT32(result);
	}
}

static int dealwith_property(int type, Oid schema_id, Oid object_id, char* property)
{
	int result = -1;
	/* OwnerId */
	if (pg_strcasecmp(property, "ownerid") == 0)
	{
		result = dealwith_type_ownerid(type, schema_id);
	}
	/* IsDefaultCnst */
	else if (pg_strcasecmp(property, "isdefaultcnst") == 0)
	{
		result = dealwith_type_defcnst(type);
	}
	/* ExecIsQuotedIdentOn, IsSchemaBound, ExecIsAnsiNullsOn */
	else if (pg_strcasecmp(property, "execisquotedidenton") == 0 ||
			pg_strcasecmp(property, "isschemabound") == 0 ||
			pg_strcasecmp(property, "execisansinullson") == 0)
	{
		result = dealwith_type_exec_bound(type, property);

	}
	/* TableFullTextPopulateStatus, TableHasVarDecimalStorageFormat */
	else if (pg_strcasecmp(property, "tablefulltextpopulatestatus") == 0 ||
			pg_strcasecmp(property, "tablehasvardecimalstorageformat") == 0)
	{
		result = dealwith_type_status_format(type);	
	}
	/* IsSYSShipped*/
	else if (pg_strcasecmp(property, "issysshipped") == 0)
	{
		result = dealwith_type_issysshipped(type, schema_id);
	}
	/* IsDeterministic */
	else if (pg_strcasecmp(property, "isdeterministic") == 0)
	{
		result = dealwith_type_dete(type, object_id);
	}
	/* IsProcedure */
	else if (pg_strcasecmp(property, "isprocedure") == 0)
	{
		result = dealwith_type_procedure(type);
	}
	/* IsTable */
	else if (pg_strcasecmp(property, "istable") == 0)
	{
		result = dealwith_type_table(type);
	}
	/* IsView */
	else if (pg_strcasecmp(property, "isview") == 0)
	{
		result = dealwith_type_view(type);
	}
	/* IsUserView */
	else if (pg_strcasecmp(property, "isusertable") == 0)
	{
		result = dealwith_type_usertable(type, schema_id);
	}
	/* IsTableFunction */
	else if (pg_strcasecmp(property, "istablefunction") == 0)
	{
		result = dealwith_type_tablefunc(type);
	}
	/* IsInlineFunction */
	else if (pg_strcasecmp(property, "isinlinefunction") == 0)
	{
		result = dealwith_type_inlinefunc(type);
	}
	/* IsScalarFunction */
	else if (pg_strcasecmp(property, "isscalarfunction") == 0)
	{
		result = dealwith_type_scalarfunc(type);
	}
	/* IsPrimaryKey */
	else if (pg_strcasecmp(property, "isprimarykey") == 0)
	{
		result = dealwith_type_pk(type);
	}
	/* IsIndexed */
	else if (pg_strcasecmp(property, "isindexed") == 0)
	{
		result = dealwith_type_indexed(type, object_id);
	}
	/* IsDefault */
	else if (pg_strcasecmp(property, "isdefault") == 0)
	{
		/*
		 * Currently hardcoded to 0.
		 */
		return 0;
	}
	/* IsOBJECT_TYPE_RULE */
	else if (pg_strcasecmp(property, "isrule") == 0)
	{
		/*
		 * Currently hardcoded to 0.
		 */
		return 0;
	}
	/* IsTrigger */
	else if (pg_strcasecmp(property, "istrigger") == 0)
	{
		result = dealwith_type_trigger(type);
	}
	return result;
}

static bool is_shared_schema(const char *name)
{
	if (strcmp("sys", name) == 0)
		return true;			/* D_FORMAT shared schema */
	else if ((strcmp("public", name) == 0)
			 || (strcmp("pg_catalog", name) == 0)
			 || (strcmp("pg_toast", name) == 0)
			 || (strcmp("information_schema", name) == 0))
		return true;
	else
		return false;
}

char* GetPhysicalSchemaName(char* dbName, const char* schemaName)
{
    char* name;
    int len;
    errno_t errorno = EOK;

    if (!schemaName) {
        return nullptr;
    }

    len = strlen(schemaName);
    if (len == 0) {
        return nullptr;
    }

    if ((get_database_oid(dbName, true)) == InvalidOid) {
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_DATABASE),
                 errmsg("database \"%s\" does not exist. Make sure that the name is entered correctly.", dbName)));
    }

    if (len >= NAMEDATALEN) {
        ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG), errmsg("type name too long"),
                        errdetail("schema name should be less the %d letters.", NAMEDATALEN)));
        u_sess->plsql_cxt.have_error = true;
    }
    name = static_cast<char*>(palloc0(len + 1));
    errorno = strncpy_s(name, len + 1, schemaName, len);
    securec_check(errorno, "\0", "\0");

    if (is_shared_schema(name)) {
        return name;
    }

    /*
     * Parser guarantees identifier will always be truncated to 64B. Schema
     * name that comes from other source (e.g scheam_id function) needs one
     * more truncate function call
     */
    truncate_identifier(name, strlen(name), false);

    /* all schema names are not prepended with db name on single-db */
    return name;
}

/*
 * tsql_get_proc_nsp_oid
 * Given Oid of pg_proc entry return namespace_oid
 * Returns InvalidOid if Oid is not found
 */
Oid
tsql_get_proc_nsp_oid(Oid object_id)
{
	Oid			namespace_oid = InvalidOid;
	HeapTuple	tuple;
	bool		isnull;

	/* retrieve pronamespace in pg_proc by oid */
	tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(object_id));

	if (HeapTupleIsValid(tuple))
	{
		(void) SysCacheGetAttr(PROCOID, tuple,
							   Anum_pg_proc_pronamespace,
							   &isnull);
		if (!isnull)
		{
			Form_pg_proc proc = (Form_pg_proc) GETSTRUCT(tuple);

			namespace_oid = proc->pronamespace;
		}
		ReleaseSysCache(tuple);
	}
	return namespace_oid;
}

/*
 * tsql_get_constraint_nsp_oid
 * Given Oid of pg_constraint entry return namespace_oid
 * Returns InvalidOid if Oid is not found
 */
Oid
tsql_get_constraint_nsp_oid(Oid object_id, Oid user_id)
{

	Oid			namespace_oid = InvalidOid;
	HeapTuple	tuple;
	bool		isnull;

	/* retrieve connamespace in pg_constraint by oid */
	tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(object_id));

	if (HeapTupleIsValid(tuple))
	{
		(void) SysCacheGetAttr(CONSTROID, tuple,
							   Anum_pg_constraint_connamespace,
							   &isnull);
		if (!isnull)
		{
			Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(tuple);

			if (OidIsValid(HeapTupleGetOid(tuple)))
			{
				/*
				 * user should have permission of table associated with
				 * constraint
				 */
				if (OidIsValid(con->conrelid))
				{
					if (pg_class_aclcheck(con->conrelid, user_id, ACL_SELECT) == ACLCHECK_OK)
						namespace_oid = con->connamespace;
				}
			}
		}
		ReleaseSysCache(tuple);
	}
	return namespace_oid;
}

static char* GetCurrentPhysicalSchemaName(char* schemaName)
{
    char* curDbName;
    char* ret;

    if (strlen(schemaName) < 1) {
        return nullptr;
    }

    curDbName = get_and_check_db_name(u_sess->proc_cxt.MyDatabaseId, true);
    ret = GetPhysicalSchemaName(curDbName, schemaName);

    return ret;
}

static inline int dealwith_type_ownerid(int type, Oid schema_id)
{
	/*
	 * Search for schema_id in pg_namespace catalog. Return nspowner from 
	 * the found pg_namespace object.
	 */
	if (OidIsValid(schema_id)) {
		HeapTuple	tp;
		int		result;

		tp = SearchSysCache1(NAMESPACEOID, ObjectIdGetDatum(schema_id));
		if (HeapTupleIsValid(tp)) {
			Form_pg_namespace nsptup = (Form_pg_namespace) GETSTRUCT(tp);
			result = ((int) nsptup->nspowner);
			ReleaseSysCache(tp);
			return result;
		}
	}
	return -1;
}

static inline int dealwith_type_defcnst(int type)
{
	/*
		* The type of the object should be OBJECT_TYPE_DEFAULT_CONSTRAINT.
		*/
	if (type == OBJECT_TYPE_DEFAULT_CONSTRAINT)
	{
		return 1;
	}
	return 0;
}

static inline int dealwith_type_exec_bound(int type, char* property)
{
	/*
	* These properties are only applicable to OBJECT_TYPE_TSQL_STORED_PROCEDURE, OBJECT_TYPE_REPLICATION_FILTER_PROCEDURE,
	* OBJECT_TYPE_VIEW, OBJECT_TYPE_TSQL_DML_TRIGGER, OBJECT_TYPE_TSQL_SCALAR_FUNCTION, OBJECT_TYPE_TSQL_INLINE_TABLE_VALUED_FUNCTION, 
	* OBJECT_TYPE_TSQL_TABLE_VALUED_FUNCTION and OBJECT_TYPE_RULE.
	* Hence, return NULL if the object is not from the above types.
	*/
	if (!(type == OBJECT_TYPE_TSQL_STORED_PROCEDURE || type == OBJECT_TYPE_REPLICATION_FILTER_PROCEDURE ||
		type == OBJECT_TYPE_VIEW || type == OBJECT_TYPE_TSQL_DML_TRIGGER || type == OBJECT_TYPE_TSQL_SCALAR_FUNCTION ||
		type == OBJECT_TYPE_TSQL_INLINE_TABLE_VALUED_FUNCTION || type == OBJECT_TYPE_TSQL_TABLE_VALUED_FUNCTION ||
		type == OBJECT_TYPE_RULE))
	{
		return -1;
	}

	/*
		* Currently, for IsSchemaBound property, we have hardcoded the value to 0
		*/
	if (pg_strcasecmp(property, "isschemabound") == 0)
	{
		return 0;
	}
	/*
	* For ExecIsQuotedIdentOn and ExecIsAnsiNullsOn, we hardcoded it to 1
	*/
	return 1;
}

static inline int dealwith_type_status_format(int type)
{
	/*
	* Currently, we have hardcoded the return value to 0.
	*/
	if (type == OBJECT_TYPE_TABLE)
	{
		return 0;
	}
	/*
	* These properties are only applicable if the type of the object is TABLE, 
	* Hence, return NULL if the object is not a TABLE.
	*/
	return -1;	
}

static inline int dealwith_type_issysshipped(int type, Oid schema_id)
{
	/*
	* Check whether the object is MS shipped. We are using is_ms_shipped helper function
	* to check the same.
	*/
	char* schema_name = get_namespace_name(schema_id);
	if (pg_strcasecmp(schema_name, "sys") == 0)
	{
		return 1;
	}
	return 0;
}

static inline int dealwith_type_dete(int type, Oid object_id)
{
	int result;
	if (type == OBJECT_TYPE_AGGREGATE_FUNCTION || type == OBJECT_TYPE_ASSEMBLY_SCALAR_FUNCTION ||
		type == OBJECT_TYPE_TSQL_SCALAR_FUNCTION || type == OBJECT_TYPE_ASSEMBLY_TABLE_VALUED_FUNCTION ||
		type == OBJECT_TYPE_TSQL_INLINE_TABLE_VALUED_FUNCTION || type == OBJECT_TYPE_TSQL_TABLE_VALUED_FUNCTION){
		HeapTuple tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(object_id));
		if (HeapTupleIsValid(tuple)){
			Form_pg_proc procform = (Form_pg_proc) GETSTRUCT(tuple);
			result = procform->provolatile == 'i' ? 1 : 0;

			ReleaseSysCache(tuple);
			return result;
			}
	}
	return -1;
}

static inline int dealwith_type_procedure(int type)
{
	/*
	* Check whether the type of the object is OBJECT_TYPE_TSQL_STORED_PROCEDURE.
	*/
	if (type == OBJECT_TYPE_TSQL_STORED_PROCEDURE)
	{
		return 1;
	}
	return 0;
}

static inline int dealwith_type_table(int type)
{
	/*
	* The type of the object should be OBJECT_TYPE_INTERNAL_TABLE or
	* TABLE or OBJECT_TYPE_SYSTEM_BASE_TABLE.
	*/
    if (type == OBJECT_TYPE_INTERNAL_TABLE ||
		type == OBJECT_TYPE_TABLE || type == OBJECT_TYPE_SYSTEM_BASE_TABLE)
	{
		return 1;
	}
	return 0;
}

static inline int dealwith_type_view(int type)
{
	/*
	* The type of the object should be OBJECT_TYPE_VIEW.
	*/
	if (type == OBJECT_TYPE_VIEW)
	{
		PG_RETURN_INT32(1);
	}
	PG_RETURN_INT32(0);
}

static inline int dealwith_type_usertable(int type, Oid schema_id)
{
	/*
	* The object should be of the type TABLE.
	*/
	char* schema_name = get_namespace_name(schema_id);
	if (type == OBJECT_TYPE_TABLE && pg_strcasecmp(schema_name, "sys") == 0)
	{
		return 1;
	}
	return 0;
}

static inline int dealwith_type_tablefunc(int type)
{
	/*
	* The object should be OBJECT_TYPE_TSQL_INLINE_TABLE_VALUED_FUNCTION or OBJECT_TYPE_TSQL_TABLE_VALUED_FUNCTION
	* OBJECT_TYPE_ASSEMBLY_TABLE_VALUED_FUNCTION.
	*/
	if (type == OBJECT_TYPE_TSQL_INLINE_TABLE_VALUED_FUNCTION || type == OBJECT_TYPE_TSQL_TABLE_VALUED_FUNCTION ||
		type == OBJECT_TYPE_ASSEMBLY_TABLE_VALUED_FUNCTION)
	{
		return 1;
	}
	return 0;
}

static inline int dealwith_type_inlinefunc(int type)
{
	/*
	* The object should be OBJECT_TYPE_TSQL_INLINE_TABLE_VALUED_FUNCTION.
	*/
	if (type == OBJECT_TYPE_TSQL_INLINE_TABLE_VALUED_FUNCTION)
	{
		return 1;
	}
	return 0;
}

static inline int dealwith_type_scalarfunc(int type)
{
	/*
	* The object should be either OBJECT_TYPE_TSQL_SCALAR_FUNCTION or OBJECT_TYPE_ASSEMBLY_SCALAR_FUNCTION.
	*/
	if (type == OBJECT_TYPE_TSQL_SCALAR_FUNCTION || type == OBJECT_TYPE_ASSEMBLY_SCALAR_FUNCTION)
	{
		return 1;
	}
	return 0;
}

static inline int dealwith_type_pk(int type)
{
	/*
	* The object should be a OBJECT_TYPE_PRIMARY_KEY_CONSTRAINT.
	*/
	if (type == OBJECT_TYPE_PRIMARY_KEY_CONSTRAINT)
	{
		return 1;
	}
	return 0;
}

static inline int dealwith_type_indexed(int type, Oid object_id)
{
	/*
	* Search for object_id in pg_index catalog by indrelid column.
	* The object is indexed if the entry exists in pg_index.
	*/
	Relation	indRel;
	ScanKeyData 	key;
	SysScanDesc 	scan;
	HeapTuple	tup;

	if (type != OBJECT_TYPE_TABLE && type != OBJECT_TYPE_MATERIALIZED_VIEW)
		return 0;

	indRel = table_open(IndexRelationId, RowExclusiveLock);

	ScanKeyInit(&key,
			Anum_pg_index_indrelid,
			BTEqualStrategyNumber, F_OIDEQ,
			ObjectIdGetDatum(object_id));

	scan = systable_beginscan(indRel, IndexIndrelidIndexId, true,
					NULL, 1, &key);

	if (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		systable_endscan(scan);
		table_close(indRel, RowExclusiveLock);
		return 1;
	}

	systable_endscan(scan);
	table_close(indRel, RowExclusiveLock);

	return 0;
}

static inline int dealwith_type_trigger(int type)
{
	/*
	* The type of the object should be OBJECT_TYPE_ASSEMBLY_DML_TRIGGER.
	*/
	if (type == OBJECT_TYPE_ASSEMBLY_DML_TRIGGER)
	{
		PG_RETURN_INT32(1);
	}
	PG_RETURN_INT32(0);
}