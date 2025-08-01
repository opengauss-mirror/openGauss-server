/* -------------------------------------------------------------------------
 *
 * pg_type.cpp
 *	  routines to support manipulation of the pg_type relation
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/catalog/pg_type.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/heapam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_fn.h"
#include "commands/defrem.h"
#include "commands/typecmds.h"
#include "miscadmin.h"
#include "parser/scansup.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "catalog/gs_package.h"
#include "parser/parse_type.h"
#include "catalog/gs_dependencies_fn.h"
#include "utils/pl_package.h"

/* ----------------------------------------------------------------
 *		TypeShellMake
 *
 *		This procedure inserts a "shell" tuple into the pg_type relation.
 *		The type tuple inserted has valid but dummy values, and its
 *		"typisdefined" field is false indicating it's not really defined.
 *
 *		This is used so that a tuple exists in the catalogs.  The I/O
 *		functions for the type will link to this tuple.  When the full
 *		CREATE TYPE command is issued, the bogus values will be replaced
 *		with correct ones, and "typisdefined" will be set to true.
 * ----------------------------------------------------------------
 */
ObjectAddress TypeShellMake(const char* typname, Oid typeNamespace, Oid ownerId)
{
    Relation pg_type_desc;
    TupleDesc tupDesc;
    int i;
    HeapTuple tup;
    Datum values[Natts_pg_type];
    bool nulls[Natts_pg_type];
    Oid typoid;
    NameData name;
    ObjectAddress address;

    Assert(PointerIsValid(typname));

    /*
     * open pg_type
     */
    pg_type_desc = heap_open(TypeRelationId, RowExclusiveLock);
    tupDesc = pg_type_desc->rd_att;

    /*
     * initialize our *nulls and *values arrays
     */
    for (i = 0; i < Natts_pg_type; ++i) {
        nulls[i] = false;
        values[i] = (Datum)NULL; /* redundant, but safe */
    }

    /*
     * initialize *values with the type name and dummy values
     *
     * The representational details are the same as int4 ... it doesn't really
     * matter what they are so long as they are consistent.  Also note that we
     * give it typtype = TYPTYPE_PSEUDO as extra insurance that it won't be
     * mistaken for a usable type.
     */
    (void)namestrcpy(&name, typname);
    values[Anum_pg_type_typname - 1] = NameGetDatum(&name);
    values[Anum_pg_type_typnamespace - 1] = ObjectIdGetDatum(typeNamespace);
    values[Anum_pg_type_typowner - 1] = ObjectIdGetDatum(ownerId);
    values[Anum_pg_type_typlen - 1] = Int16GetDatum(sizeof(int4));
    values[Anum_pg_type_typbyval - 1] = BoolGetDatum(true);
    values[Anum_pg_type_typtype - 1] = CharGetDatum(TYPTYPE_PSEUDO);
    values[Anum_pg_type_typcategory - 1] = CharGetDatum(TYPCATEGORY_PSEUDOTYPE);
    values[Anum_pg_type_typispreferred - 1] = BoolGetDatum(false);
    values[Anum_pg_type_typisdefined - 1] = BoolGetDatum(false);
    values[Anum_pg_type_typdelim - 1] = CharGetDatum(DEFAULT_TYPDELIM);
    values[Anum_pg_type_typrelid - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_type_typelem - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_type_typarray - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_type_typinput - 1] = ObjectIdGetDatum(F_SHELL_IN);
    values[Anum_pg_type_typoutput - 1] = ObjectIdGetDatum(F_SHELL_OUT);
    values[Anum_pg_type_typreceive - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_type_typsend - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_type_typmodin - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_type_typmodout - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_type_typanalyze - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_type_typalign - 1] = CharGetDatum('i');
    values[Anum_pg_type_typstorage - 1] = CharGetDatum('p');
    values[Anum_pg_type_typnotnull - 1] = BoolGetDatum(false);
    values[Anum_pg_type_typbasetype - 1] = ObjectIdGetDatum(InvalidOid);
    values[Anum_pg_type_typtypmod - 1] = Int32GetDatum(-1);
    values[Anum_pg_type_typndims - 1] = Int32GetDatum(0);
    values[Anum_pg_type_typcollation - 1] = ObjectIdGetDatum(InvalidOid);
    nulls[Anum_pg_type_typdefaultbin - 1] = true;
    nulls[Anum_pg_type_typdefault - 1] = true;
    nulls[Anum_pg_type_typacl - 1] = true;

    /*
     * create a new type tuple
     */
    tup = heap_form_tuple(tupDesc, values, nulls);

    /* Use binary or inplace upgrade override for pg_type.oid, if supplied. */
    if (u_sess->proc_cxt.IsBinaryUpgrade && OidIsValid(u_sess->upg_cxt.binary_upgrade_next_pg_type_oid)) {
        HeapTupleSetOid(tup, u_sess->upg_cxt.binary_upgrade_next_pg_type_oid);
        u_sess->upg_cxt.binary_upgrade_next_pg_type_oid = InvalidOid;
    } else if (u_sess->attr.attr_common.IsInplaceUpgrade &&
               OidIsValid(u_sess->upg_cxt.Inplace_upgrade_next_pg_type_oid)) {
        HeapTupleSetOid(tup, u_sess->upg_cxt.Inplace_upgrade_next_pg_type_oid);
        u_sess->upg_cxt.Inplace_upgrade_next_pg_type_oid = InvalidOid;
    }

    /*
     * insert the tuple in the relation and get the tuple's oid.
     */
    typoid = simple_heap_insert(pg_type_desc, tup);

    CatalogUpdateIndexes(pg_type_desc, tup);

    /*
     * Create dependencies.  We can/must skip this in bootstrap mode
     * and during inplace upgrade.
     */
    if (!IsBootstrapProcessingMode() &&
        !(u_sess->attr.attr_common.IsInplaceUpgrade && typoid < FirstBootstrapObjectId)) {
        GenerateTypeDependencies(typeNamespace,
            typoid,
            InvalidOid,
            0,
            ownerId,
            F_SHELL_IN,
            F_SHELL_OUT,
            InvalidOid,
            InvalidOid,
            InvalidOid,
            InvalidOid,
            InvalidOid,
            InvalidOid,
            false,
            InvalidOid,
            InvalidOid,
            NULL,
            false);
    }

    /* Post creation hook for new shell type */
    InvokeObjectAccessHook(OAT_POST_CREATE, TypeRelationId, typoid, 0, NULL);
    ObjectAddressSet(address, TypeRelationId, typoid);

    /*
     * clean up and return the type-oid
     */
    heap_freetuple_ext(tup);
    heap_close(pg_type_desc, RowExclusiveLock);

    return address;
}

/* ----------------------------------------------------------------
 *		TypeCreate
 *
 *		This does all the necessary work needed to define a new type.
 *
 *		Returns the OID assigned to the new type.  If newTypeOid is
 *		zero (the normal case), a new OID is created; otherwise we
 *		use exactly that OID.
 * ----------------------------------------------------------------
 */
ObjectAddress TypeCreate(Oid newTypeOid, const char* typname, Oid typeNamespace, Oid relationOid, /* only for relation rowtypes */
    char relationKind,                                                                  /* ditto */
    Oid ownerId, int16 internalSize, char typeType, char typeCategory, bool typePreferred, char typDelim,
    Oid inputProcedure, Oid outputProcedure, Oid receiveProcedure, Oid sendProcedure, Oid typmodinProcedure,
    Oid typmodoutProcedure, Oid analyzeProcedure, Oid elementType, bool isImplicitArray, Oid arrayType, Oid baseType,
    const char* defaultTypeValue,                                                    /* human readable rep */
    char* defaultTypeBin,                                                            /* cooked rep */
    bool passedByValue, char alignment, char storage, int32 typeMod, int32 typNDims, /* Array dimensions for baseType */
    bool typeNotNull, Oid typeCollation, TypeDependExtend* dependExtend)
{
    Relation pg_type_desc;
    Oid typeObjectId;
    bool rebuildDeps = false;
    HeapTuple tup;
    bool nulls[Natts_pg_type];
    bool replaces[Natts_pg_type];
    Datum values[Natts_pg_type];
    NameData name;
    int i;
    Acl* typacl = NULL;
    ObjectAddress address;

    /*
     * We assume that the caller validated the arguments individually, but did
     * not check for bad combinations.
     *
     * Validate size specifications: either positive (fixed-length) or -1
     * (varlena) or -2 (cstring).
     */
    if (!(internalSize > 0 || internalSize == -1 || internalSize == -2))
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("invalid type internal size %d", internalSize)));

    if (passedByValue) {
        /*
         * Pass-by-value types must have a fixed length that is one of the
         * values supported by fetch_att() and store_att_byval(); and the
         * alignment had better agree, too.  All this code must match
         * access/tupmacs.h!
         */
        if (internalSize == (int16)sizeof(char)) {
            if (alignment != 'c')
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                        errmsg("alignment \"%c\" is invalid for passed-by-value type of size %d",
                            alignment,
                            internalSize)));
        } else if (internalSize == (int16)sizeof(int16)) {
            if (alignment != 's')
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                        errmsg("alignment \"%c\" is invalid for passed-by-value type of size %d",
                            alignment,
                            internalSize)));
        } else if (internalSize == (int16)sizeof(int32)) {
            if (alignment != 'i')
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                        errmsg("alignment \"%c\" is invalid for passed-by-value type of size %d",
                            alignment,
                            internalSize)));
        }
#if SIZEOF_DATUM == 8
        else if (internalSize == (int16)sizeof(Datum)) {
            if (alignment != 'd')
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                        errmsg("alignment \"%c\" is invalid for passed-by-value type of size %d",
                            alignment,
                            internalSize)));
        }
#endif
        else
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("internal size %d is invalid for passed-by-value type", internalSize)));
    } else {
        /* varlena types must have int align or better */
        if (internalSize == -1 && !(alignment == 'i' || alignment == 'd'))
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("alignment \"%c\" is invalid for variable-length type", alignment)));
        /* cstring must have char alignment */
        if (internalSize == -2 && !(alignment == 'c'))
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("alignment \"%c\" is invalid for variable-length type", alignment)));
    }

    /* Only varlena types can be toasted */
    if (storage != 'p' && internalSize != -1)
        ereport(
            ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("fixed-size types must have storage PLAIN")));

    /*
     * initialize arrays needed for heap_form_tuple or heap_modify_tuple
     */
    for (i = 0; i < Natts_pg_type; ++i) {
        nulls[i] = false;
        replaces[i] = true;
        values[i] = (Datum)0;
    }

    /* During inplace upgrade, we may need type forms other than base type. */
    if (u_sess->attr.attr_common.IsInplaceUpgrade && u_sess->cmd_cxt.TypeCreateType) {
        /* So far, we support explicitly assigned pseudo type during inplace upgrade. */
        Assert(u_sess->cmd_cxt.TypeCreateType == TYPTYPE_PSEUDO ||
               u_sess->cmd_cxt.TypeCreateType == TYPTYPE_BASE ||
               u_sess->cmd_cxt.TypeCreateType == TYPTYPE_UNDEFINE ||
               u_sess->cmd_cxt.TypeCreateType == TYPTYPE_SET);

        typeType = u_sess->cmd_cxt.TypeCreateType;
        u_sess->cmd_cxt.TypeCreateType = '\0';
    }

    /*
     * insert data values
     */
    (void)namestrcpy(&name, typname);
    values[Anum_pg_type_typname - 1] = NameGetDatum(&name);
    values[Anum_pg_type_typnamespace - 1] = ObjectIdGetDatum(typeNamespace);
    values[Anum_pg_type_typowner - 1] = ObjectIdGetDatum(ownerId);
    values[Anum_pg_type_typlen - 1] = Int16GetDatum(internalSize);
    values[Anum_pg_type_typbyval - 1] = BoolGetDatum(passedByValue);
    values[Anum_pg_type_typtype - 1] = CharGetDatum(typeType);
    values[Anum_pg_type_typcategory - 1] = CharGetDatum(typeCategory);
    values[Anum_pg_type_typispreferred - 1] = BoolGetDatum(typePreferred);
    values[Anum_pg_type_typisdefined - 1] = BoolGetDatum(true);
    values[Anum_pg_type_typdelim - 1] = CharGetDatum(typDelim);
    values[Anum_pg_type_typrelid - 1] = ObjectIdGetDatum(relationOid);
    values[Anum_pg_type_typelem - 1] = ObjectIdGetDatum(elementType);
    values[Anum_pg_type_typarray - 1] = ObjectIdGetDatum(arrayType);
    values[Anum_pg_type_typinput - 1] = ObjectIdGetDatum(inputProcedure);
    values[Anum_pg_type_typoutput - 1] = ObjectIdGetDatum(outputProcedure);
    values[Anum_pg_type_typreceive - 1] = ObjectIdGetDatum(receiveProcedure);
    values[Anum_pg_type_typsend - 1] = ObjectIdGetDatum(sendProcedure);
    values[Anum_pg_type_typmodin - 1] = ObjectIdGetDatum(typmodinProcedure);
    values[Anum_pg_type_typmodout - 1] = ObjectIdGetDatum(typmodoutProcedure);
    values[Anum_pg_type_typanalyze - 1] = ObjectIdGetDatum(analyzeProcedure);
    values[Anum_pg_type_typalign - 1] = CharGetDatum(alignment);
    values[Anum_pg_type_typstorage - 1] = CharGetDatum(storage);
    values[Anum_pg_type_typnotnull - 1] = BoolGetDatum(typeNotNull);
    values[Anum_pg_type_typbasetype - 1] = ObjectIdGetDatum(baseType);
    values[Anum_pg_type_typtypmod - 1] = Int32GetDatum(typeMod);
    values[Anum_pg_type_typndims - 1] = Int32GetDatum(typNDims);
    values[Anum_pg_type_typcollation - 1] = ObjectIdGetDatum(typeCollation);

    /*
     * initialize the default binary value for this type.  Check for nulls of
     * course.
     */
    if (defaultTypeBin != NULL)
        values[Anum_pg_type_typdefaultbin - 1] = CStringGetTextDatum(defaultTypeBin);
    else
        nulls[Anum_pg_type_typdefaultbin - 1] = true;

    /*
     * initialize the default value for this type.
     */
    if (defaultTypeValue != NULL)
        values[Anum_pg_type_typdefault - 1] = CStringGetTextDatum(defaultTypeValue);
    else
        nulls[Anum_pg_type_typdefault - 1] = true;

    typacl = get_user_default_acl(ACL_OBJECT_TYPE, ownerId, typeNamespace);
    if (typacl != NULL)
        values[Anum_pg_type_typacl - 1] = PointerGetDatum(typacl);
    else
        nulls[Anum_pg_type_typacl - 1] = true;

    /*
     * open pg_type and prepare to insert or update a row.
     *
     * NOTE: updating will not work correctly in bootstrap mode; but we don't
     * expect to be overwriting any shell types in bootstrap mode.
     */
    pg_type_desc = heap_open(TypeRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy2(TYPENAMENSP, CStringGetDatum(typname), ObjectIdGetDatum(typeNamespace));
    if (HeapTupleIsValid(tup)) {
        /*
         * check that the type is not already defined.	It may exist as a
         * shell type, however.
         */
        if (((Form_pg_type)GETSTRUCT(tup))->typisdefined)
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("type \"%s\" already exists", typname)));

        /*
         * shell type must have been created by same owner
         */
        if (((Form_pg_type)GETSTRUCT(tup))->typowner != ownerId)
            aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_TYPE, typname);

        /* trouble if caller wanted to force the OID */
        if (OidIsValid(newTypeOid))
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("cannot assign new OID to existing shell type")));

        /*
         * Okay to update existing shell type tuple
         */
        tup = heap_modify_tuple(tup, RelationGetDescr(pg_type_desc), values, nulls, replaces);

        simple_heap_update(pg_type_desc, &tup->t_self, tup);

        typeObjectId = HeapTupleGetOid(tup);

        rebuildDeps = true; /* get rid of shell type's dependencies */
    } else {
        tup = heap_form_tuple(RelationGetDescr(pg_type_desc), values, nulls);

        /* Force the OID if requested by caller */
        if (OidIsValid(newTypeOid))
            HeapTupleSetOid(tup, newTypeOid);
        /* Use binary-upgrade override for pg_type.oid, if supplied. */
        else if (u_sess->proc_cxt.IsBinaryUpgrade && OidIsValid(u_sess->upg_cxt.binary_upgrade_next_pg_type_oid)) {
            HeapTupleSetOid(tup, u_sess->upg_cxt.binary_upgrade_next_pg_type_oid);
            u_sess->upg_cxt.binary_upgrade_next_pg_type_oid = InvalidOid;
        }
        /* Use inplace-upgrade override for pg_type.oid, if supplied. */
        else if (u_sess->attr.attr_common.IsInplaceUpgrade &&
                 OidIsValid(u_sess->upg_cxt.Inplace_upgrade_next_pg_type_oid)) {
            HeapTupleSetOid(tup, u_sess->upg_cxt.Inplace_upgrade_next_pg_type_oid);
            u_sess->upg_cxt.Inplace_upgrade_next_pg_type_oid = InvalidOid;
        }
        /* else allow system to assign oid */

        typeObjectId = simple_heap_insert(pg_type_desc, tup);
    }

    /* Update indexes */
    CatalogUpdateIndexes(pg_type_desc, tup);

    if (enable_plpgsql_gsdependency() && NULL != dependExtend) {
        dependExtend->typType = typeType;
        dependExtend->typCategory = typeCategory;
    }

    /*
     * Create dependencies.  We can/must skip this in bootstrap mode.
     * During inplace upgrade, we insert pinned dependency for the newly added type.
     */
    if (!IsBootstrapProcessingMode()) {
        if (u_sess->attr.attr_common.IsInplaceUpgrade &&
            (typeObjectId < FirstBootstrapObjectId ||
                (relationOid < FirstBootstrapObjectId && relationOid != InvalidOid)) &&
            !rebuildDeps) {
            ObjectAddress myself;
            myself.classId = TypeRelationId;
            myself.objectId = typeObjectId;
            myself.objectSubId = InvalidOid;
            recordPinnedDependency(&myself);
        } else
            GenerateTypeDependencies(typeNamespace,
                typeObjectId,
                relationOid,
                relationKind,
                ownerId,
                inputProcedure,
                outputProcedure,
                receiveProcedure,
                sendProcedure,
                typmodinProcedure,
                typmodoutProcedure,
                analyzeProcedure,
                elementType,
                isImplicitArray,
                baseType,
                typeCollation,
                (Node*)(defaultTypeBin ? stringToNode(defaultTypeBin) : NULL),
                rebuildDeps,
                typname,
                dependExtend);
    }

    /* Post creation hook for new type */
    InvokeObjectAccessHook(OAT_POST_CREATE, TypeRelationId, typeObjectId, 0, NULL);
    ObjectAddressSet(address, TypeRelationId, typeObjectId);
    /*
     * finish up
     */
    heap_close(pg_type_desc, RowExclusiveLock);
    CommandCounterIncrement();
    if (enable_plpgsql_gsdependency_guc() && !isImplicitArray &&
        ((TYPTYPE_BASE == typeType && TYPCATEGORY_ARRAY == typeCategory) ||
        TYPTYPE_TABLEOF == typeType || typeType == TYPTYPE_ENUM)) {
        if (CompileWhich() == PLPGSQL_COMPILE_NULL) {
            (void)gsplsql_build_ref_type_dependency(typeObjectId);
        }
    }
    return address;
}

/*
 * GenerateTypeDependencies: build the dependencies needed for a type
 *
 * If rebuild is true, we remove existing dependencies and rebuild them
 * from scratch.  This is needed for ALTER TYPE, and also when replacing
 * a shell type.  We don't remove an existing extension dependency, though.
 * (That means an extension can't absorb a shell type created in another
 * extension, nor ALTER a type created by another extension.  Also, if it
 * replaces a free-standing shell type or ALTERs a free-standing type,
 * that type will become a member of the extension.)
 */
void GenerateTypeDependencies(Oid typeNamespace, Oid typeObjectId, Oid relationOid, /* only for relation rowtypes */
    char relationKind,                                                              /* ditto */
    Oid owner, Oid inputProcedure, Oid outputProcedure, Oid receiveProcedure, Oid sendProcedure, Oid typmodinProcedure,
    Oid typmodoutProcedure, Oid analyzeProcedure, Oid elementType, bool isImplicitArray, Oid baseType,
    Oid typeCollation, Node* defaultExpr, bool rebuild, const char* typname, TypeDependExtend* dependExtend)
{
    ObjectAddress myself, referenced;

    /* If rebuild, first flush old dependencies, except extension deps */
    if (rebuild) {
        (void)deleteDependencyRecordsFor(TypeRelationId, typeObjectId, true);
        deleteSharedDependencyRecordsFor(TypeRelationId, typeObjectId, 0);
    }

    myself.classId = TypeRelationId;
    myself.objectId = typeObjectId;
    myself.objectSubId = 0;

    /*
     * Make dependencies on namespace, owner, extension.
     *
     * For a relation rowtype (that's not a composite type), we should skip
     * these because we'll depend on them indirectly through the pg_class
     * entry.  Likewise, skip for implicit arrays since we'll depend on them
     * through the element type.
     */
    if ((!OidIsValid(relationOid) || relationKind == RELKIND_COMPOSITE_TYPE) && !isImplicitArray) {
        referenced.classId = NamespaceRelationId;
        referenced.objectId = typeNamespace;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

        recordDependencyOnOwner(TypeRelationId, typeObjectId, owner);

        recordDependencyOnCurrentExtension(&myself, rebuild);
    }

    /* Normal dependencies on the I/O functions */
    if (OidIsValid(inputProcedure)) {
        referenced.classId = ProcedureRelationId;
        referenced.objectId = inputProcedure;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }

    if (OidIsValid(outputProcedure)) {
        referenced.classId = ProcedureRelationId;
        referenced.objectId = outputProcedure;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }

    if (OidIsValid(receiveProcedure)) {
        referenced.classId = ProcedureRelationId;
        referenced.objectId = receiveProcedure;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }

    if (OidIsValid(sendProcedure)) {
        referenced.classId = ProcedureRelationId;
        referenced.objectId = sendProcedure;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }

    if (OidIsValid(typmodinProcedure)) {
        referenced.classId = ProcedureRelationId;
        referenced.objectId = typmodinProcedure;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }

    if (OidIsValid(typmodoutProcedure)) {
        referenced.classId = ProcedureRelationId;
        referenced.objectId = typmodoutProcedure;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }

    if (OidIsValid(analyzeProcedure)) {
        referenced.classId = ProcedureRelationId;
        referenced.objectId = analyzeProcedure;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }

    /*
     * If the type is a rowtype for a relation, mark it as internally
     * dependent on the relation, *unless* it is a stand-alone composite type
     * relation. For the latter case, we have to reverse the dependency.
     *
     * In the former case, this allows the type to be auto-dropped when the
     * relation is, and not otherwise. And in the latter, of course we get the
     * opposite effect.
     */
    if (OidIsValid(relationOid)) {
        referenced.classId = RelationRelationId;
        referenced.objectId = relationOid;
        referenced.objectSubId = 0;

        if (relationKind != RELKIND_COMPOSITE_TYPE)
            recordDependencyOn(&myself, &referenced, DEPENDENCY_INTERNAL);
        else
            recordDependencyOn(&referenced, &myself, DEPENDENCY_INTERNAL);
    }

    /*
     * If the type is an implicitly-created array type, mark it as internally
     * dependent on the element type.  Otherwise, if it has an element type,
     * the dependency is a normal one.
     */
    if (OidIsValid(elementType)) {
        referenced.classId = TypeRelationId;
        referenced.objectId = elementType;
        referenced.objectSubId = 0;
        int cw = CompileWhich();
        if (enable_plpgsql_gsdependency() && NULL != dependExtend && cw != PLPGSQL_COMPILE_NULL &&
            ((TYPTYPE_BASE == dependExtend->typType && TYPCATEGORY_ARRAY == dependExtend->typCategory) ||
             TYPTYPE_TABLEOF == dependExtend->typType)) {
            GsDependParamBody gsDependParamBody;
            gsplsql_init_gs_depend_param_body(&gsDependParamBody);
            gsDependParamBody.dependNamespaceOid = typeNamespace;
            if (NULL != u_sess->plsql_cxt.curr_compile_context &&
                NULL != u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package) {
                gsDependParamBody.dependPkgOid =
                    u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid;
                gsDependParamBody.dependPkgName =
                    u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_signature;   
            }
            char* realTypName = ParseTypeName((char*)typname, gsDependParamBody.dependPkgOid);
            if (realTypName == NULL) {
                gsDependParamBody.dependName = (char*)typname;
            } else {
                gsDependParamBody.dependName = realTypName;
            }
            gsDependParamBody.refPosType = GSDEPEND_REFOBJ_POS_IN_TYPE;
            gsDependParamBody.type = GSDEPEND_OBJECT_TYPE_TYPE;
            gsDependParamBody.dependExtend = dependExtend;
            recordDependencyOn(&myself, &referenced, isImplicitArray ? DEPENDENCY_INTERNAL : DEPENDENCY_NORMAL,
                &gsDependParamBody);
            pfree_ext(realTypName);
        } else {
            recordDependencyOn(&myself, &referenced, isImplicitArray ? DEPENDENCY_INTERNAL : DEPENDENCY_NORMAL);
        }
    }

    /* Normal dependency from a domain to its base type. */
    if (OidIsValid(baseType)) {
        referenced.classId = TypeRelationId;
        referenced.objectId = baseType;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }

    /* Normal dependency from a domain to its collation. */
    /* We know the default collation is pinned, so don't bother recording it */
    if (OidIsValid(typeCollation) && typeCollation != DEFAULT_COLLATION_OID) {
        referenced.classId = CollationRelationId;
        referenced.objectId = typeCollation;
        referenced.objectSubId = 0;
        recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);
    }

    /* Normal dependency on the default expression. */
    if (defaultExpr != NULL)
        recordDependencyOnExpr(&myself, defaultExpr, NIL, DEPENDENCY_NORMAL);
}

/*
 * RenameTypeInternal
 *		This renames a type, as well as any associated array type.
 *
 * Caller must have already checked privileges.
 *
 * Currently this is used for renaming table rowtypes and for
 * ALTER TYPE RENAME TO command.
 */
void RenameTypeInternal(Oid typeOid, const char* newTypeName, Oid typeNamespace)
{
    Relation pg_type_desc;
    HeapTuple tuple;
    Form_pg_type typ;
    Oid arrayOid;

    pg_type_desc = heap_open(TypeRelationId, RowExclusiveLock);

    tuple = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(typeOid));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", typeOid)));
    typ = (Form_pg_type)GETSTRUCT(tuple);

    /* We are not supposed to be changing schemas here */
    Assert(typeNamespace == typ->typnamespace);

    arrayOid = typ->typarray;

    /* Just to give a more friendly error than unique-index violation */
    if (SearchSysCacheExists2(TYPENAMENSP, CStringGetDatum(newTypeName), ObjectIdGetDatum(typeNamespace)))
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("type \"%s\" already exists", newTypeName)));

    /* OK, do the rename --- tuple is a copy, so OK to scribble on it */
    (void)namestrcpy(&(typ->typname), newTypeName);

    simple_heap_update(pg_type_desc, &tuple->t_self, tuple);

    /* update the system catalog indexes */
    CatalogUpdateIndexes(pg_type_desc, tuple);

    heap_freetuple_ext(tuple);
    heap_close(pg_type_desc, RowExclusiveLock);

    /* If the type has an array type, recurse to handle that */
    if (OidIsValid(arrayOid)) {
        char* arrname = makeArrayTypeName(newTypeName, typeNamespace);

        RenameTypeInternal(arrayOid, arrname, typeNamespace);
        pfree_ext(arrname);
    }
}

/*
 * makeArrayTypeName
 *	  - given a base type name, make an array type name for it
 *
 * the caller is responsible for pfreeing the result
 */
char* makeArrayTypeName(const char* typname, Oid typeNamespace)
{
    char* arr = NULL;
    int pass = 0;
    char modlabel[NAMEDATALEN] = {0};
    char* postfix = NULL;
    Relation pg_type_desc;
    errno_t rc = EOK;

    pg_type_desc = heap_open(TypeRelationId, AccessShareLock);

    /* "_" + typename + "_seq num" if conflict */
    for (;;) {
        /* first here postfix == NULL means without "_seq num" */
        arr = makeObjectName("", typname, postfix);
        if (!SearchSysCacheExists2(TYPENAMENSP, CStringGetDatum(arr), ObjectIdGetDatum(typeNamespace)))
            break;

        /* found a conflict, so try a new name component */
        pfree_ext(arr);

        /*  if pass == INT_MAX then error report */
        if (pass == INT_MAX) {
            ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("could not form array type name for type \"%s\"", typname)));
            break;
        }

        rc = sprintf_s(modlabel, NAMEDATALEN, "%d", ++pass);
        securec_check_ss(rc, "", "");

        /* add "_seq num" */
        postfix = modlabel;
    }

    heap_close(pg_type_desc, AccessShareLock);

    return arr;
}

/*
 * moveArrayTypeName
 *	  - try to reassign an array type name that the user wants to use.
 *
 * The given type name has been discovered to already exist (with the given
 * OID).  If it is an autogenerated array type, change the array type's name
 * to not conflict.  This allows the user to create type "foo" followed by
 * type "_foo" without problems.  (Of course, there are race conditions if
 * two backends try to create similarly-named types concurrently, but the
 * worst that can happen is an unnecessary failure --- anything we do here
 * will be rolled back if the type creation fails due to conflicting names.)
 *
 * Note that this must be called *before* calling makeArrayTypeName to
 * determine the new type's own array type name; else the latter will
 * certainly pick the same name.
 *
 * Returns TRUE if successfully moved the type, FALSE if not.
 *
 * We also return TRUE if the given type is a shell type.  In this case
 * the type has not been renamed out of the way, but nonetheless it can
 * be expected that TypeCreate will succeed.  This behavior is convenient
 * for most callers --- those that need to distinguish the shell-type case
 * must do their own typisdefined test.
 */
bool moveArrayTypeName(Oid typeOid, const char* typname, Oid typeNamespace)
{
    Oid elemOid;
    char* newname = NULL;

    /* We need do nothing if it's a shell type. */
    if (!get_typisdefined(typeOid))
        return true;

    /* Can't change it if it's not an autogenerated array type. */
    elemOid = get_element_type(typeOid);
    if (!OidIsValid(elemOid) || get_array_type(elemOid) != typeOid)
        return false;

    /*
     * OK, use makeArrayTypeName to pick an unused modification of the name.
     * Note that since makeArrayTypeName is an iterative process, this will
     * produce a name that it might have produced the first time, had the
     * conflicting type we are about to create already existed.
     */
    newname = makeArrayTypeName(typname, typeNamespace);

    /* Apply the rename */
    RenameTypeInternal(typeOid, newname, typeNamespace);

    /*
     * We must bump the command counter so that any subsequent use of
     * makeArrayTypeName sees what we just did and doesn't pick the same name.
     */
    CommandCounterIncrement();

    pfree_ext(newname);

    return true;
}

Oid TypeNameGetOid(const char* schemaName, const char* packageName, const char* typeName)
{
    Oid pkgOid = InvalidOid;
    Oid typOid = InvalidOid;
    Oid namespaceOid = InvalidOid;
    if (schemaName != NULL) {
        namespaceOid = LookupNamespaceNoError(schemaName);
    }
    if (packageName != NULL) {
        pkgOid = PackageNameGetOid(packageName, namespaceOid);
        if (!OidIsValid(pkgOid)) {
            return InvalidOid;
        }
    }
    char* castTypeName = CastPackageTypeName(typeName, pkgOid, pkgOid != InvalidOid, true);
    typOid = TypenameGetTypidExtended(castTypeName, false);
    pfree_ext(castTypeName);
    return typOid;
}

char* MakeTypeNamesStrForTypeOid(Oid typOid, bool* dependUndefined, StringInfo concatName)
{
    char* ret = NULL;
    char* name = NULL;
    char* pkgName = NULL;
    char* schemaName = NULL;
    if (NULL != concatName) {
        MakeTypeNamesStrForTypeOid(concatName, typOid, &schemaName, &pkgName, &name);
    } else {
        StringInfoData curConcatName;
        initStringInfo(&curConcatName);
        MakeTypeNamesStrForTypeOid(&curConcatName, typOid, &schemaName, &pkgName, &name);
        ret = pstrdup(curConcatName.data);
        FreeStringInfo(&curConcatName);
    }
    if (NULL != dependUndefined) {
        if (UNDEFINEDOID == typOid) { // UNDEFINEDOID
            *dependUndefined = true;
        } else if (gsplsql_check_type_depend_undefined(schemaName, pkgName, name)) {
            *dependUndefined = true;
        }
    }
    pfree_ext(schemaName);
    pfree_ext(pkgName);
    pfree_ext(name);
    return ret;
}

void MakeTypeNamesStrForTypeOid(StringInfo concatName, Oid typOid,
                                      char** schemaName, char** pkgName, char** name)
{
    char* curTypeName = NULL;
    char* curPkgName = NULL;
    char* curSchemaName = NULL;

    curSchemaName = get_typenamespace(typOid);
    appendStringInfoString(concatName, curSchemaName == NULL ? "" : curSchemaName);
    if (NULL != schemaName) {
        *schemaName = curSchemaName;
    } else {
        pfree_ext(curSchemaName);
    }

    Oid pkgOid = GetTypePackageOid(typOid);
    if (OidIsValid(pkgOid)) {
        appendStringInfoString(concatName, ".");
        curPkgName = GetPackageName(pkgOid);
        appendStringInfoString(concatName, curPkgName == NULL ? "" : curPkgName);
        if (NULL != pkgName) {
            *pkgName = curPkgName;
        } else {
            pfree_ext(curPkgName);
        }
    }
    appendStringInfoString(concatName, ".");
    curTypeName = get_typename(typOid);
    if (NULL != curTypeName && OidIsValid(pkgOid)) {
        char* realName = ParseTypeName(curTypeName, pkgOid);
        if (NULL != realName) {
            pfree_ext(curTypeName);
            curTypeName = realName;
        }
    }
    appendStringInfoString(concatName, curTypeName == NULL ? "" : curTypeName);
    if (NULL != name) {
        *name = curTypeName;
    } else {
        pfree_ext(curTypeName);
    }
}

Oid GetTypePackageOid(Oid typoid)
{
    ObjectAddress objectAddress;
    objectAddress.classId = TypeRelationId;
    objectAddress.objectId = typoid;
    objectAddress.objectSubId = 0;
    Oid pkgOid = get_object_package(&objectAddress);// debug todo
    if (InvalidOid == pkgOid && type_is_array(typoid)) {
        objectAddress.objectId = get_element_type(typoid);
        pkgOid = get_object_package(&objectAddress);
    }
    return pkgOid;
}

void InstanceTypeNameDependExtend(TypeDependExtend** dependExtend)
{
    if (NULL != dependExtend && NULL == (*dependExtend)) {
        (*dependExtend) = (TypeDependExtend*)palloc(sizeof(TypeDependExtend));
        (*dependExtend)->typeOid = InvalidOid;
        (*dependExtend)->undefDependObjOid = InvalidOid;
        (*dependExtend)->dependUndefined = false;
        (*dependExtend)->schemaName = NULL;
        (*dependExtend)->packageName = NULL;
        (*dependExtend)->objectName = NULL;
        (*dependExtend)->typType = TYPTYPE_INVALID;
        (*dependExtend)->typCategory = TYPCATEGORY_INVALID;
    }
}

void ReleaseTypeNameDependExtend(TypeDependExtend** dependExtend)
{
    if (NULL != dependExtend && NULL != (*dependExtend)) {
        pfree_ext((*dependExtend)->schemaName);
        pfree_ext((*dependExtend)->packageName);
        pfree_ext((*dependExtend)->objectName);
        pfree_ext(*dependExtend);
        *dependExtend = NULL;
    }
}
