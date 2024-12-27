/* -------------------------------------------------------------------------
 *
 * typecmds.cpp
 *	  Routines for SQL commands that manipulate types (and domains).
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/optimizer/commands/typecmds.cpp
 *
 * DESCRIPTION
 *	  The "DefineFoo" routines take the parse tree and pick out the
 *	  appropriate arguments/flags, passing the results to the
 *	  corresponding "FooDefine" routines (in src/catalog) that do
 *	  the actual catalog-munging.  These routines also verify permission
 *	  of the user to execute the command.
 *
 * NOTES
 *	  These things must be defined and committed in the following order:
 *		"create function":
 *				input/output, recv/send functions
 *		"create type":
 *				type
 *		"create operator":
 *				operators
 *
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/gs_package.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_set.h"
#include "catalog/pg_language.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_proc_fn.h"
#include "catalog/pg_range.h"
#include "catalog/pg_synonym.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_fn.h"
#include "catalog/gs_db_privilege.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/typecmds.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/planner.h"
#include "optimizer/var.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "storage/tcap.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/fmgroids.h"
#include "utils/fmgrtab.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "utils/snapmgr.h"
#include "utils/typcache.h"
#include "catalog/gs_dependencies_fn.h"
#include "catalog/pg_object.h"

/* result structure for get_rels_with_domain() */
typedef struct {
    Relation rel; /* opened and locked relation */
    int natts;    /* number of attributes of interest */
    int* atts;    /* attribute numbers */
                  /* atts[] is of allocated length RelationGetNumberOfAttributes(rel) */
} RelToCheck;

static void makeRangeConstructors(const char* name, Oid nmspace, Oid rangeOid, Oid subtype);
static Oid findTypeInputFunction(List* procname, Oid typeOid);
static Oid findTypeOutputFunction(List* procname, Oid typeOid);
static Oid findTypeReceiveFunction(List* procname, Oid typeOid);
static Oid findTypeSendFunction(List* procname, Oid typeOid);
static Oid findTypeTypmodinFunction(List* procname);
static Oid findTypeTypmodoutFunction(List* procname);
static Oid findTypeAnalyzeFunction(List* procname, Oid typeOid);
static Oid findRangeSubOpclass(List* opcname, Oid subtype);
static Oid findRangeCanonicalFunction(List* procname, Oid typeOid);
static Oid findRangeSubtypeDiffFunction(List* procname, Oid subtype);
static void validateDomainConstraint(Oid domainoid, char* ccbin);
static List* get_rels_with_domain(Oid domainOid, LOCKMODE lockmode);
static void checkEnumAlterPrivilege(HeapTuple tup);
static char* domainAddConstraint(
    Oid domainOid, Oid domainNamespace, Oid baseTypeOid, int typMod, Constraint* constr, char* domainName, ObjectAddress *constrAddr);
static void CheckFuncParamType(Oid foid, Oid toid, bool isin);
static void CheckTypeMatch(Oid funcOid, bool isInputFunc, int16 typlen, bool typbyval);
static Oid GetBuiltinFuncTypeOid(Oid custumTypeFunc, bool isInputFunc);

/*
 * DefineType
 *		Registers a new base type.
 */
ObjectAddress DefineType(List* names, List* parameters)
{
    char* typname = NULL;
    Oid typeNamespace;
    int16 internalLength = -1; /* default: variable-length */
    List* inputName = NIL;
    List* outputName = NIL;
    List* receiveName = NIL;
    List* sendName = NIL;
    List* typmodinName = NIL;
    List* typmodoutName = NIL;
    List* analyzeName = NIL;
    char category = TYPCATEGORY_USER;
    bool preferred = false;
    char delimiter = DEFAULT_TYPDELIM;
    Oid elemType = InvalidOid;
    char* defaultValue = NULL;
    bool byValue = false;
    char alignment = 'i'; /* default alignment */
    char storage = 'p';   /* default TOAST storage method */
    Oid collation = InvalidOid;
    DefElem* likeTypeEl = NULL;
    DefElem* internalLengthEl = NULL;
    DefElem* inputNameEl = NULL;
    DefElem* outputNameEl = NULL;
    DefElem* receiveNameEl = NULL;
    DefElem* sendNameEl = NULL;
    DefElem* typmodinNameEl = NULL;
    DefElem* typmodoutNameEl = NULL;
    DefElem* analyzeNameEl = NULL;
    DefElem* categoryEl = NULL;
    DefElem* preferredEl = NULL;
    DefElem* delimiterEl = NULL;
    DefElem* elemTypeEl = NULL;
    DefElem* defaultValueEl = NULL;
    DefElem* byValueEl = NULL;
    DefElem* alignmentEl = NULL;
    DefElem* storageEl = NULL;
    DefElem* collatableEl = NULL;
    Oid inputOid;
    Oid outputOid;
    Oid receiveOid = InvalidOid;
    Oid sendOid = InvalidOid;
    Oid typmodinOid = InvalidOid;
    Oid typmodoutOid = InvalidOid;
    Oid analyzeOid = InvalidOid;
    char* array_type = NULL;
    Oid array_oid = InvalidOid;
    Oid typoid;
    Oid realtype = InvalidOid;
    Oid resulttype;
    ListCell* pl = NULL;
    Oid typowner = InvalidOid;
    ObjectAddress address;

    /*
     * isalter is true, change the owner of the objects as the owner of the
     * namespace, if the owner of the namespce has the same name as the namescpe
     */
    bool isalter = false;

    /*
     * As of Postgres 8.4, we require superuser privilege to create a base
     * type.  This is simple paranoia: there are too many ways to mess up the
     * system with an incorrect type definition (for instance, representation
     * parameters that don't match what the C code expects).  In practice it
     * takes superuser privilege to create the I/O functions, and so the
     * former requirement that you own the I/O functions pretty much forced
     * superuserness anyway.  We're just making doubly sure here.
     *
     * XXX re-enable NOT_USED code sections below if you remove this test.
     */
    /* Convert list of names to a name and namespace */
    typeNamespace = QualifiedNameGetCreationNamespace(names, &typname);
    /*
     * anyResult is true, explain that the current user is granted create any type permission
     */
    bool anyResult = CheckCreatePrivilegeInNamespace(typeNamespace, GetUserId(), CREATE_ANY_TYPE);

    if (u_sess->attr.attr_sql.enforce_a_behavior) {
        typowner = GetUserIdFromNspId(typeNamespace, false, anyResult);

        if (!OidIsValid(typowner))
            typowner = GetUserId();
        else if (typowner != GetUserId()) {
            isalter = true;
        }
    } else {
        typowner = GetUserId();
    }

    if (isalter) {
        (void)CheckCreatePrivilegeInNamespace(typeNamespace, typowner, CREATE_ANY_TYPE);
    }

    /*
     * Look to see if type already exists (presumably as a shell; if not,
     * TypeCreate will complain).
     */
    typoid = GetSysCacheOid2(TYPENAMENSP, CStringGetDatum(typname), ObjectIdGetDatum(typeNamespace));

    /*
     * If it's not a shell, see if it's an autogenerated array type, and if so
     * rename it out of the way.
     */
    if (OidIsValid(typoid) && get_typisdefined(typoid)) {
        if (moveArrayTypeName(typoid, typname, typeNamespace))
            typoid = InvalidOid;
    }

    /*
     * If it doesn't exist, create it as a shell, so that the OID is known for
     * use in the I/O function definitions.
     */
    if (!OidIsValid(typoid)) {
        address = TypeShellMake(typname, typeNamespace, typowner);
        /* Make new shell type visible for modification below */
        CommandCounterIncrement();

        /*
         * If the command was a parameterless CREATE TYPE, we're done ---
         * creating the shell type was all we're supposed to do.
         */
        if (parameters == NIL)
            return address;
    } else {
        /* Complain if dummy CREATE TYPE and entry already exists */
        if (parameters == NIL)
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("type \"%s\" already exists", typname)));
    }

    /* Extract the parameters from the parameter list */
    foreach (pl, parameters) {
        DefElem* defel = (DefElem*)lfirst(pl);
        DefElem** defelp;

        if (pg_strcasecmp(defel->defname, "like") == 0)
            defelp = &likeTypeEl;
        else if (pg_strcasecmp(defel->defname, "internallength") == 0)
            defelp = &internalLengthEl;
        else if (pg_strcasecmp(defel->defname, "input") == 0)
            defelp = &inputNameEl;
        else if (pg_strcasecmp(defel->defname, "output") == 0)
            defelp = &outputNameEl;
        else if (pg_strcasecmp(defel->defname, "receive") == 0)
            defelp = &receiveNameEl;
        else if (pg_strcasecmp(defel->defname, "send") == 0)
            defelp = &sendNameEl;
        else if (pg_strcasecmp(defel->defname, "typmod_in") == 0)
            defelp = &typmodinNameEl;
        else if (pg_strcasecmp(defel->defname, "typmod_out") == 0)
            defelp = &typmodoutNameEl;
        else if (pg_strcasecmp(defel->defname, "analyze") == 0 || pg_strcasecmp(defel->defname, "analyse") == 0)
            defelp = &analyzeNameEl;
        else if (pg_strcasecmp(defel->defname, "category") == 0)
            defelp = &categoryEl;
        else if (pg_strcasecmp(defel->defname, "preferred") == 0)
            defelp = &preferredEl;
        else if (pg_strcasecmp(defel->defname, "delimiter") == 0)
            defelp = &delimiterEl;
        else if (pg_strcasecmp(defel->defname, "element") == 0)
            defelp = &elemTypeEl;
        else if (pg_strcasecmp(defel->defname, "default") == 0)
            defelp = &defaultValueEl;
        else if (pg_strcasecmp(defel->defname, "passedbyvalue") == 0)
            defelp = &byValueEl;
        else if (pg_strcasecmp(defel->defname, "alignment") == 0)
            defelp = &alignmentEl;
        else if (pg_strcasecmp(defel->defname, "storage") == 0)
            defelp = &storageEl;
        else if (pg_strcasecmp(defel->defname, "collatable") == 0)
            defelp = &collatableEl;
        else {
            /* WARNING, not ERROR, for historical backwards-compatibility */
            ereport(WARNING,
                (errcode(ERRCODE_SYNTAX_ERROR), errmsg("type attribute \"%s\" not recognized", defel->defname)));
            continue;
        }
        if (*defelp != NULL)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
        *defelp = defel;
    }

    /*
     * Now interpret the options; we do this separately so that LIKE can be
     * overridden by other options regardless of the ordering in the parameter
     * list.
     */
    if (likeTypeEl != NULL) {
        Type likeType;
        Form_pg_type likeForm;

        likeType = typenameType(NULL, defGetTypeName(likeTypeEl), NULL);
        likeForm = (Form_pg_type)GETSTRUCT(likeType);
        internalLength = likeForm->typlen;
        byValue = likeForm->typbyval;
        alignment = likeForm->typalign;
        storage = likeForm->typstorage;
        realtype = typeTypeId(likeType);
        ReleaseSysCache(likeType);
    }
    if (internalLengthEl != NULL)
        internalLength = defGetTypeLength(internalLengthEl);
    if (inputNameEl != NULL)
        inputName = defGetQualifiedName(inputNameEl);
    if (outputNameEl != NULL)
        outputName = defGetQualifiedName(outputNameEl);
    if (receiveNameEl != NULL)
        receiveName = defGetQualifiedName(receiveNameEl);
    if (sendNameEl != NULL)
        sendName = defGetQualifiedName(sendNameEl);
    if (typmodinNameEl != NULL)
        typmodinName = defGetQualifiedName(typmodinNameEl);
    if (typmodoutNameEl != NULL)
        typmodoutName = defGetQualifiedName(typmodoutNameEl);
    if (analyzeNameEl != NULL)
        analyzeName = defGetQualifiedName(analyzeNameEl);
    if (categoryEl != NULL) {
        char* p = defGetString(categoryEl);

        category = p[0];
        /* restrict to non-control ASCII */
        if (category < 32 || category > 126)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("invalid type category \"%s\": must be simple ASCII", p)));
    }
    if (preferredEl != NULL)
        preferred = defGetBoolean(preferredEl);
    if (delimiterEl != NULL) {
        char* p = defGetString(delimiterEl);

        delimiter = p[0];
        /* XXX shouldn't we restrict the delimiter? */
    }
    if (elemTypeEl != NULL) {
        elemType = typenameTypeId(NULL, defGetTypeName(elemTypeEl));
        /* disallow arrays of pseudotypes */
        if (get_typtype(elemType) == TYPTYPE_PSEUDO)
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("array element type cannot be %s", format_type_be(elemType))));
    }
    if (defaultValueEl != NULL)
        defaultValue = defGetString(defaultValueEl);
    if (byValueEl != NULL)
        byValue = defGetBoolean(byValueEl);
    if (alignmentEl != NULL) {
        char* a = defGetString(alignmentEl);

        /*
         * Note: if argument was an unquoted identifier, parser will have
         * applied translations to it, so be prepared to recognize translated
         * type names as well as the nominal form.
         */
        if (pg_strcasecmp(a, "double") == 0 || pg_strcasecmp(a, "float8") == 0 ||
            pg_strcasecmp(a, "pg_catalog.float8") == 0)
            alignment = 'd';
        else if (pg_strcasecmp(a, "int4") == 0 || pg_strcasecmp(a, "pg_catalog.int4") == 0)
            alignment = 'i';
        else if (pg_strcasecmp(a, "int2") == 0 || pg_strcasecmp(a, "pg_catalog.int2") == 0)
            alignment = 's';
        else if (pg_strcasecmp(a, "char") == 0 || pg_strcasecmp(a, "pg_catalog.bpchar") == 0)
            alignment = 'c';
        else
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("alignment \"%s\" not recognized", a)));
    }
    if (storageEl != NULL) {
        char* a = defGetString(storageEl);

        if (pg_strcasecmp(a, "plain") == 0)
            storage = 'p';
        else if (pg_strcasecmp(a, "external") == 0)
            storage = 'e';
        else if (pg_strcasecmp(a, "extended") == 0)
            storage = 'x';
        else if (pg_strcasecmp(a, "main") == 0)
            storage = 'm';
        else
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("storage \"%s\" not recognized", a)));
    }
    if (collatableEl != NULL)
        collation = defGetBoolean(collatableEl) ? DEFAULT_COLLATION_OID : InvalidOid;

    /*
     * make sure we have our required definitions
     */
    if (inputName == NIL)
        ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("type input function must be specified")));
    if (outputName == NIL)
        ereport(ERROR, (errcode(ERRCODE_INVALID_OBJECT_DEFINITION), errmsg("type output function must be specified")));

    if (typmodinName == NIL && typmodoutName != NIL)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("type modifier output function is useless without a type modifier input function")));

    /*
     * Convert I/O proc names to OIDs
     */
    inputOid = findTypeInputFunction(inputName, typoid);
    outputOid = findTypeOutputFunction(outputName, typoid);
    if (receiveName != NULL)
        receiveOid = findTypeReceiveFunction(receiveName, typoid);
    if (sendName != NULL)
        sendOid = findTypeSendFunction(sendName, typoid);

    if (realtype != InvalidOid) {
        CheckFuncParamType(inputOid, realtype, true);
        CheckFuncParamType(outputOid, realtype, false);
    }

    /*
     * Verify that I/O procs return the expected thing.  If we see OPAQUE,
     * complain and change it to the correct type-safe choice.
     */
    resulttype = get_func_rettype(inputOid);
    if (resulttype != typoid) {
        if (resulttype == OPAQUEOID) {
            /* backwards-compatibility hack */
            ereport(WARNING,
                (errmsg("changing return type of function %s from \"opaque\" to %s",
                    NameListToString(inputName),
                    typname)));
            SetFunctionReturnType(inputOid, typoid);
        } else
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("type input function %s must return type %s", NameListToString(inputName), typname)));
    }
    resulttype = get_func_rettype(outputOid);
    if (resulttype != CSTRINGOID) {
        if (resulttype == OPAQUEOID) {
            /* backwards-compatibility hack */
            ereport(WARNING,
                (errmsg("changing return type of function %s from \"opaque\" to \"cstring\"",
                    NameListToString(outputName))));
            SetFunctionReturnType(outputOid, CSTRINGOID);
        } else
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("type output function %s must return type \"cstring\"", NameListToString(outputName))));
    }
    if (receiveOid) {
        resulttype = get_func_rettype(receiveOid);
        if (resulttype != typoid)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("type receive function %s must return type %s", NameListToString(receiveName), typname)));
    }
    if (sendOid) {
        resulttype = get_func_rettype(sendOid);
        if (resulttype != BYTEAOID)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                    errmsg("type send function %s must return type \"bytea\"", NameListToString(sendName))));
    }

    /*
     * Convert typmodin/out function proc names to OIDs.
     */
    if (typmodinName != NULL)
        typmodinOid = findTypeTypmodinFunction(typmodinName);
    if (typmodoutName != NULL)
        typmodoutOid = findTypeTypmodoutFunction(typmodoutName);

    /*
     * Convert analysis function proc name to an OID. If no analysis function
     * is specified, we'll use zero to select the built-in default algorithm.
     */
    if (analyzeName != NULL)
        analyzeOid = findTypeAnalyzeFunction(analyzeName, typoid);

    /*
     * Check permissions on functions.	We choose to require the creator/owner
     * of a type to also own the underlying functions.	Since creating a type
     * is tantamount to granting public execute access on the functions, the
     * minimum sane check would be for execute-with-grant-option.  But we
     * don't have a way to make the type go away if the grant option is
     * revoked, so ownership seems better.
     */
    /* XXX this is unnecessary given the superuser check above */
    if (inputOid && !pg_proc_ownercheck(inputOid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC, NameListToString(inputName));
    if (outputOid && !pg_proc_ownercheck(outputOid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC, NameListToString(outputName));
    if (receiveOid && !pg_proc_ownercheck(receiveOid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC, NameListToString(receiveName));
    if (sendOid && !pg_proc_ownercheck(sendOid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC, NameListToString(sendName));
    if (typmodinOid && !pg_proc_ownercheck(typmodinOid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC, NameListToString(typmodinName));
    if (typmodoutOid && !pg_proc_ownercheck(typmodoutOid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC, NameListToString(typmodoutName));
    if (analyzeOid && !pg_proc_ownercheck(analyzeOid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC, NameListToString(analyzeName));
    /*
     * if the owner of the namespce has the same name as the namescpe,
     * we must check acl of the owner too
     */
    if (isalter) {
        if (inputOid && !pg_proc_ownercheck(inputOid, typowner))
            aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC, NameListToString(inputName));
        if (outputOid && !pg_proc_ownercheck(outputOid, typowner))
            aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC, NameListToString(outputName));
        if (receiveOid && !pg_proc_ownercheck(receiveOid, typowner))
            aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC, NameListToString(receiveName));
        if (sendOid && !pg_proc_ownercheck(sendOid, typowner))
            aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC, NameListToString(sendName));
        if (typmodinOid && !pg_proc_ownercheck(typmodinOid, typowner))
            aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC, NameListToString(typmodinName));
        if (typmodoutOid && !pg_proc_ownercheck(typmodoutOid, typowner))
            aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC, NameListToString(typmodoutName));
        if (analyzeOid && !pg_proc_ownercheck(analyzeOid, typowner))
            aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_PROC, NameListToString(analyzeName));
    }

    /*
     * During upgrade, we may create pseudo type
     * which doesn't have array type.
     */
    char TypeCreateType = u_sess->cmd_cxt.TypeCreateType;
    if (!u_sess->attr.attr_common.IsInplaceUpgrade || !TypeCreateType ||
        (TypeCreateType != TYPTYPE_PSEUDO && TypeCreateType != TYPTYPE_SET && TypeCreateType != TYPTYPE_UNDEFINE))
        array_oid = AssignTypeArrayOid();

    /* check the input and output functions's real type whether match the custum attributes */
    if (!(u_sess->attr.attr_common.IsInplaceUpgrade) && !IsInitdb &&
        TypeCreateType != TYPTYPE_PSEUDO && TypeCreateType != TYPTYPE_SET) {
        CheckTypeMatch(inputOid, true, internalLength, byValue);
        CheckTypeMatch(outputOid, false, internalLength, byValue);
    }

    /*
     * now have TypeCreate do all the real work.
     *
     * Note: the pg_type.oid is stored in user tables as array elements (base
     * types) in ArrayType and in composite types in DatumTupleFields.	This
     * oid must be preserved by binary upgrades.
     */
    address = TypeCreate(InvalidOid, /* no predetermined type OID */
        typname,                    /* type name */
        typeNamespace,              /* namespace */
        InvalidOid,                 /* relation oid (n/a here) */
        0,                          /* relation kind (ditto) */
        typowner,                   /* owner's ID */
        internalLength,             /* internal size */
        TYPTYPE_BASE,               /* type-type (base type) */
        category,                   /* type-category */
        preferred,                  /* is it a preferred type? */
        delimiter,                  /* array element delimiter */
        inputOid,                   /* input procedure */
        outputOid,                  /* output procedure */
        receiveOid,                 /* receive procedure */
        sendOid,                    /* send procedure */
        typmodinOid,                /* typmodin procedure */
        typmodoutOid,               /* typmodout procedure */
        analyzeOid,                 /* analyze procedure */
        elemType,                   /* element type ID */
        false,                      /* this is not an array type */
        array_oid,                  /* array type we are about to create */
        InvalidOid,                 /* base type ID (only for domains) */
        defaultValue,               /* default type value */
        NULL,                       /* no binary form available */
        byValue,                    /* passed by value */
        alignment,                  /* required alignment */
        storage,                    /* TOAST strategy */
        -1,                         /* typMod (Domains only) */
        0,                          /* Array Dimensions of typbasetype */
        false,                      /* Type NOT NULL */
        collation);                 /* type's collation */

    /*
     * During upgrade, we may create pseudo type
     * which doesn't have array type.
     */
    if (!u_sess->attr.attr_common.IsInplaceUpgrade || !TypeCreateType ||
        (TypeCreateType != TYPTYPE_PSEUDO && TypeCreateType != TYPTYPE_SET && TypeCreateType != TYPTYPE_UNDEFINE)) {
        /*
         * Create the array type that goes with it.
         */
        array_type = makeArrayTypeName(typname, typeNamespace);

        /* alignment must be 'i' or 'd' for arrays */
        alignment = (alignment == 'd') ? 'd' : 'i';

        (void)TypeCreate(array_oid,   /* force assignment of this type OID */
            array_type,         /* type name */
            typeNamespace,      /* namespace */
            InvalidOid,         /* relation oid (n/a here) */
            0,                  /* relation kind (ditto) */
            typowner,           /* owner's ID */
            -1,                 /* internal size (always varlena) */
            TYPTYPE_BASE,       /* type-type (base type) */
            TYPCATEGORY_ARRAY,  /* type-category (array) */
            false,              /* array types are never preferred */
            delimiter,          /* array element delimiter */
            F_ARRAY_IN,         /* input procedure */
            F_ARRAY_OUT,        /* output procedure */
            F_ARRAY_RECV,       /* receive procedure */
            F_ARRAY_SEND,       /* send procedure */
            typmodinOid,        /* typmodin procedure */
            typmodoutOid,       /* typmodout procedure */
            F_ARRAY_TYPANALYZE, /* analyze procedure */
            typoid,             /* element type ID */
            true,               /* yes this is an array type */
            InvalidOid,         /* no further array type */
            InvalidOid,         /* base type ID */
            NULL,               /* never a default type value */
            NULL,               /* binary default isn't sent either */
            false,              /* never passed by value */
            alignment,          /* see above */
            'x',                /* ARRAY is always toastable */
            -1,                 /* typMod (Domains only) */
            0,                  /* Array dimensions of typbasetype */
            false,              /* Type NOT NULL */
            collation);         /* type's collation */

        pfree_ext(array_type);
    }
    return address;
}

/*
 * Guts of type deletion.
 */
void RemoveTypeById(Oid typeOid)
{
#ifndef ENABLE_MULTIPLE_NODES
    GsDependObjDesc ref_obj;
    if (enable_plpgsql_gsdependency_guc()) {
        gsplsql_init_gs_depend_obj_desc(&ref_obj);
        char relkind = get_rel_relkind(typeOid);
        if (relkind == RELKIND_COMPOSITE_TYPE || relkind == '\0') {
            ref_obj.name = NULL;
            Oid elem_oid = get_array_internal_depend_type_oid(typeOid);
            if (!OidIsValid(elem_oid)) {
                gsplsql_get_depend_obj_by_typ_id(&ref_obj, typeOid, InvalidOid, true);
            }
        }
    }
#endif
    Relation relation;
    HeapTuple tup;

    relation = heap_open(TypeRelationId, RowExclusiveLock);

    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typeOid));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", typeOid)));

    simple_heap_delete(relation, &tup->t_self);

    /*
     * If it is an enum, delete the pg_enum entries too; we don't bother with
     * making dependency entries for those, so it has to be done "by hand"
     * here.
     */
    if (((Form_pg_type)GETSTRUCT(tup))->typtype == TYPTYPE_ENUM)
        EnumValuesDelete(typeOid);

    /*
     * If it is a range type, delete the pg_range entry too; we don't bother
     * with making a dependency entry for that, so it has to be done "by hand"
     * here.
     */
    if (((Form_pg_type)GETSTRUCT(tup))->typtype == TYPTYPE_RANGE)
        RangeDelete(typeOid);

    /*
     * If it is a set type, delete the pg_set entry too; we don't bother
     * with making a dependency entry for that, so it has to be done "by hand"
     * here.
     */
    if (((Form_pg_type)GETSTRUCT(tup))->typtype == TYPTYPE_SET)
        SetValuesDelete(typeOid);

    ReleaseSysCache(tup);

    heap_close(relation, RowExclusiveLock);
#ifndef ENABLE_MULTIPLE_NODES
    if (enable_plpgsql_gsdependency_guc() && NULL != ref_obj.name) {
        CommandCounterIncrement();
        (void)gsplsql_remove_ref_dependency(&ref_obj);
        ref_obj.refPosType = GSDEPEND_REFOBJ_POS_IN_TYPE;
        (void)gsplsql_remove_gs_dependency(&ref_obj);
        Oid pkg_oid = GetTypePackageOid(typeOid);
        if (OidIsValid(pkg_oid)) {
            bool invalid_pkg = true;
            if (NULL != u_sess->plsql_cxt.curr_compile_context &&
                NULL != u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package) {
                invalid_pkg = pkg_oid ==
                    u_sess->plsql_cxt.curr_compile_context->plpgsql_curr_compile_package->pkg_oid;
            }
            if (invalid_pkg) {
                bool is_spec = ref_obj.name[0] != '$';
                SetPgObjectValid(pkg_oid, is_spec ? OBJECT_TYPE_PKGSPEC : OBJECT_TYPE_PKGBODY, false);
                if (is_spec) {
                    SetPgObjectValid(pkg_oid, OBJECT_TYPE_PKGBODY, false);
                }
                gsplsql_set_pkg_func_status(GetPackageNamespace(pkg_oid), pkg_oid, false);
            }
        }
        pfree_ext(ref_obj.schemaName);
        pfree_ext(ref_obj.packageName);
        pfree_ext(ref_obj.name);
    }
#endif
}

/*
 * DefineDomain
 *		Registers a new domain.
 */
ObjectAddress DefineDomain(CreateDomainStmt* stmt)
{
    char* domainName = NULL;
    Oid domainNamespace;
    AclResult aclresult;
    int16 internalLength;
    Oid inputProcedure;
    Oid outputProcedure;
    Oid receiveProcedure;
    Oid sendProcedure;
    Oid analyzeProcedure;
    bool byValue = false;
    char category;
    char delimiter;
    char alignment;
    char storage;
    char typtype;
    Datum datum;
    bool isnull = false;
    char* defaultValue = NULL;
    char* defaultValueBin = NULL;
    bool saw_default = false;
    bool typNotNull = false;
    bool nullDefined = false;
    int32 typNDims = list_length(stmt->typname->arrayBounds);
    HeapTuple typeTup;
    List* schema = stmt->constraints;
    ListCell* listptr = NULL;
    Oid basetypeoid;
    ObjectAddress address;
    Oid old_type_oid;
    Oid domaincoll;
    Form_pg_type baseType;
    int32 basetypeMod;
    Oid baseColl;
    Oid typowner = InvalidOid;
    /*
     * isalter is true, change the owner of the objects as the owner of the
     * namespace, if the owner of the namespce has the same name as the namescpe
     */
    bool isalter = false;

    /* Convert list of names to a name and namespace */
    domainNamespace = QualifiedNameGetCreationNamespace(stmt->domainname, &domainName);

    if (u_sess->attr.attr_sql.enforce_a_behavior) {
        typowner = GetUserIdFromNspId(domainNamespace);

        if (!OidIsValid(typowner))
            typowner = GetUserId();
        else if (typowner != GetUserId())
            isalter = true;
    } else {
        typowner = GetUserId();
    }

    /* Check we have creation rights in target namespace */
    aclresult = pg_namespace_aclcheck(domainNamespace, GetUserId(), ACL_CREATE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, ACL_KIND_NAMESPACE, get_namespace_name(domainNamespace));
    if (isalter) {
        aclresult = pg_namespace_aclcheck(domainNamespace, typowner, ACL_CREATE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_NAMESPACE, get_namespace_name(domainNamespace));
    }

    /*
     * Check for collision with an existing type name.	If there is one and
     * it's an autogenerated array, we can rename it out of the way.
     */
    old_type_oid = GetSysCacheOid2(TYPENAMENSP, CStringGetDatum(domainName), ObjectIdGetDatum(domainNamespace));
    if (OidIsValid(old_type_oid)) {
        if (!moveArrayTypeName(old_type_oid, domainName, domainNamespace))
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("type \"%s\" already exists", domainName)));
    }

    /*
     * Look up the base type.
     */
    typeTup = typenameType(NULL, stmt->typname, &basetypeMod);
    baseType = (Form_pg_type)GETSTRUCT(typeTup);
    basetypeoid = HeapTupleGetOid(typeTup);

    /*
     * Base type must be a plain base type, another domain, an enum or a range
     * type. Domains over pseudotypes would create a security hole.  Domains
     * over composite types might be made to work in the future, but not
     * today.
     */
    typtype = baseType->typtype;
    if (typtype != TYPTYPE_BASE && typtype != TYPTYPE_DOMAIN && typtype != TYPTYPE_ENUM && typtype != TYPTYPE_RANGE)
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("\"%s\" is not a valid base type for a domain", TypeNameToString(stmt->typname))));

    aclresult = pg_type_aclcheck(basetypeoid, GetUserId(), ACL_USAGE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error_type(aclresult, basetypeoid);
    if (isalter) {
        aclresult = pg_type_aclcheck(basetypeoid, typowner, ACL_USAGE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error_type(aclresult, basetypeoid);
    }

    /*
     * Identify the collation if any
     */
    baseColl = baseType->typcollation;
    if (stmt->collClause)
        domaincoll = get_collation_oid(stmt->collClause->collname, false);
    else
        domaincoll = baseColl;

    /* Complain if COLLATE is applied to an uncollatable type */
    if (OidIsValid(domaincoll) && !OidIsValid(baseColl))
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("collations are not supported by type %s", format_type_be(basetypeoid))));

    /* passed by value */
    byValue = baseType->typbyval;

    /* Required Alignment */
    alignment = baseType->typalign;

    /* TOAST Strategy */
    storage = baseType->typstorage;

    /* Storage Length */
    internalLength = baseType->typlen;

    /* Type Category */
    category = baseType->typcategory;

    /* Array element Delimiter */
    delimiter = baseType->typdelim;

    /* I/O Functions */
    inputProcedure = F_DOMAIN_IN;
    outputProcedure = baseType->typoutput;
    receiveProcedure = F_DOMAIN_RECV;
    sendProcedure = baseType->typsend;

    /* Domains never accept typmods, so no typmodin/typmodout needed */
    /* Analysis function */
    analyzeProcedure = baseType->typanalyze;

    /* Inherited default value */
    datum = SysCacheGetAttr(TYPEOID, typeTup, Anum_pg_type_typdefault, &isnull);
    if (!isnull)
        defaultValue = TextDatumGetCString(datum);

    /* Inherited default binary value */
    datum = SysCacheGetAttr(TYPEOID, typeTup, Anum_pg_type_typdefaultbin, &isnull);
    if (!isnull)
        defaultValueBin = TextDatumGetCString(datum);

    /*
     * Run through constraints manually to avoid the additional processing
     * conducted by DefineRelation() and friends.
     */
    foreach (listptr, schema) {
        Constraint* constr = (Constraint*)lfirst(listptr);

        if (!IsA(constr, Constraint))
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE), errmsg("unrecognized node type: %d", (int)nodeTag(constr))));

        switch (constr->contype) {
            case CONSTR_DEFAULT:

                /*
                 * The inherited default value may be overridden by the user
                 * with the DEFAULT <expr> clause ... but only once.
                 */
                if (saw_default)
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("multiple default expressions")));
                saw_default = true;

                if (constr->raw_expr) {
                    ParseState* pstate = NULL;
                    Node* defaultExpr = NULL;

                    /* Create a dummy ParseState for transformExpr */
                    pstate = make_parsestate(NULL);

                    /*
                     * Cook the constr->raw_expr into an expression. Note:
                     * name is strictly for error message
                     */
                    defaultExpr = cookDefault(pstate, constr->raw_expr, basetypeoid, basetypeMod,
                        baseType->typcollation, domainName, 0);

                    /*
                     * If the expression is just a NULL constant, we treat it
                     * like not having a default.
                     *
                     * Note that if the basetype is another domain, we'll see
                     * a CoerceToDomain expr here and not discard the default.
                     * This is critical because the domain default needs to be
                     * retained to override any default that the base domain
                     * might have.
                     */
                    if (defaultExpr == NULL || (IsA(defaultExpr, Const) && ((Const*)defaultExpr)->constisnull)) {
                        defaultValue = NULL;
                        defaultValueBin = NULL;
                    } else {
                        /*
                         * Expression must be stored as a nodeToString result,
                         * but we also require a valid textual representation
                         * (mainly to make life easier for pg_dump).
                         */
                        defaultValue =
                            deparse_expression(defaultExpr, deparse_context_for(domainName, InvalidOid), false, false);
                        defaultValueBin = nodeToString(defaultExpr);
                    }
                } else {
                    /* No default (can this still happen?) */
                    defaultValue = NULL;
                    defaultValueBin = NULL;
                }
                break;

            case CONSTR_NOTNULL:
                if (nullDefined && !typNotNull)
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting NULL/NOT NULL constraints")));
                typNotNull = true;
                nullDefined = true;
                break;

            case CONSTR_NULL:
                if (nullDefined && typNotNull)
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting NULL/NOT NULL constraints")));
                typNotNull = false;
                nullDefined = true;
                break;

            case CONSTR_CHECK:

                /*
                 * Check constraints are handled after domain creation, as
                 * they require the Oid of the domain; at this point we can
                 * only check that they're not marked NO INHERIT, because
                 * that would be bogus.
                 */
                if (constr->is_no_inherit)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                            errmsg("CHECK constraints for domains cannot be marked NO INHERIT")));
                break;

                /*
                 * All else are error cases
                 */
            case CONSTR_UNIQUE:
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("unique constraints not possible for domains")));
                break;

            case CONSTR_PRIMARY:
                ereport(
                    ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("primary key constraints not possible for domains")));
                break;

            case CONSTR_EXCLUSION:
                ereport(
                    ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("exclusion constraints not possible for domains")));
                break;

            case CONSTR_FOREIGN:
                ereport(
                    ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("foreign key constraints not possible for domains")));
                break;

            case CONSTR_ATTR_DEFERRABLE:
            case CONSTR_ATTR_NOT_DEFERRABLE:
            case CONSTR_ATTR_DEFERRED:
            case CONSTR_ATTR_IMMEDIATE:
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("specifying constraint deferrability not supported for domains")));
                break;

            default:
                ereport(ERROR,
                    (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                        errmsg("unrecognized constraint subtype: %d", (int)constr->contype)));
                break;
        }
    }

    /*
     * Have TypeCreate do all the real work.
     */
    address = TypeCreate(InvalidOid, /* no predetermined type OID */
        domainName,                    /* type name */
        domainNamespace,               /* namespace */
        InvalidOid,                    /* relation oid (n/a here) */
        0,                             /* relation kind (ditto) */
        typowner,                      /* owner's ID */
        internalLength,                /* internal size */
        TYPTYPE_DOMAIN,                /* type-type (domain type) */
        category,                      /* type-category */
        false,                         /* domain types are never preferred */
        delimiter,                     /* array element delimiter */
        inputProcedure,                /* input procedure */
        outputProcedure,               /* output procedure */
        receiveProcedure,              /* receive procedure */
        sendProcedure,                 /* send procedure */
        InvalidOid,                    /* typmodin procedure - none */
        InvalidOid,                    /* typmodout procedure - none */
        analyzeProcedure,              /* analyze procedure */
        InvalidOid,                    /* no array element type */
        false,                         /* this isn't an array */
        InvalidOid,                    /* no arrays for domains (yet) */
        basetypeoid,                   /* base type ID */
        defaultValue,                  /* default type value (text) */
        defaultValueBin,               /* default type value (binary) */
        byValue,                       /* passed by value */
        alignment,                     /* required alignment */
        storage,                       /* TOAST strategy */
        basetypeMod,                   /* typeMod value */
        typNDims,                      /* Array dimensions for base type */
        typNotNull,                    /* Type NOT NULL */
        domaincoll);                   /* type's collation */

    /*
     * Process constraints which refer to the domain ID returned by TypeCreate
     */
    foreach (listptr, schema) {
        Constraint* constr = (Constraint*)lfirst(listptr);

        /* it must be a Constraint, per check above */
        switch (constr->contype) {
            case CONSTR_CHECK:
                domainAddConstraint(address.objectId, domainNamespace, basetypeoid, basetypeMod, constr, domainName, NULL);
                break;

                /* Other constraint types were fully processed above */
            default:
                break;
        }

        /* CCI so we can detect duplicate constraint names */
        CommandCounterIncrement();
    }

    /*
     * Now we can clean up.
     */
    ReleaseSysCache(typeTup);
    return address;
}

/*
 * DefineEnum
 *		Registers a new enum.
 */
ObjectAddress DefineEnum(CreateEnumStmt* stmt)
{
    char* enumName = NULL;
    char* enumArrayName = NULL;
    Oid enumNamespace;
    //Oid enumTypeOid;
    AclResult aclresult;
    Oid old_type_oid;
    Oid enumArrayOid;
    Oid typowner = InvalidOid;
    ObjectAddress enumTypeAddr;

    /*
     * isalter is true, change the owner of the objects as the owner of the
     * namespace, if the owner of the namespce has the same name as the namescpe
     */
    bool isalter = false;

    /* Convert list of names to a name and namespace */
    enumNamespace = QualifiedNameGetCreationNamespace(stmt->typname, &enumName);
    /*
     * anyResult is true, explain that the current user is granted create any type permission
     */
    bool anyResult = false;
    if (!IsSysSchema(enumNamespace)) {
        anyResult = HasSpecAnyPriv(GetUserId(), CREATE_ANY_TYPE, false);
    }
    if (u_sess->attr.attr_sql.enforce_a_behavior) {
        typowner = GetUserIdFromNspId(enumNamespace, false, anyResult);

        if (!OidIsValid(typowner))
            typowner = GetUserId();
        else if (typowner != GetUserId())
            isalter = true;
    } else {
        typowner = GetUserId();
    }
    /* Check we have creation rights in target namespace */
    aclresult = pg_namespace_aclcheck(enumNamespace, GetUserId(), ACL_CREATE);
    if (aclresult != ACLCHECK_OK && !anyResult)
        aclcheck_error(aclresult, ACL_KIND_NAMESPACE, get_namespace_name(enumNamespace));
    if (isalter) {
        (void)CheckCreatePrivilegeInNamespace(enumNamespace, typowner, CREATE_ANY_TYPE);
    }

    /*
     * Check for collision with an existing type name.	If there is one and
     * it's an autogenerated array, we can rename it out of the way.
     */
    old_type_oid = GetSysCacheOid2(TYPENAMENSP, CStringGetDatum(enumName), ObjectIdGetDatum(enumNamespace));
    if (OidIsValid(old_type_oid)) {
        if (!moveArrayTypeName(old_type_oid, enumName, enumNamespace))
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("type \"%s\" already exists", enumName)));
    }

    enumArrayOid = AssignTypeArrayOid();

    /* Create the pg_type entry */
    enumTypeAddr = TypeCreate(InvalidOid, /* no predetermined type OID */
        enumName,                        /* type name */
        enumNamespace,                   /* namespace */
        InvalidOid,                      /* relation oid (n/a here) */
        0,                               /* relation kind (ditto) */
        typowner,                        /* owner's ID */
        sizeof(Oid),                     /* internal size */
        TYPTYPE_ENUM,                    /* type-type (enum type) */
        TYPCATEGORY_ENUM,                /* type-category (enum type) */
        false,                           /* enum types are never preferred */
        DEFAULT_TYPDELIM,                /* array element delimiter */
        F_ENUM_IN,                       /* input procedure */
        F_ENUM_OUT,                      /* output procedure */
        F_ENUM_RECV,                     /* receive procedure */
        F_ENUM_SEND,                     /* send procedure */
        InvalidOid,                      /* typmodin procedure - none */
        InvalidOid,                      /* typmodout procedure - none */
        InvalidOid,                      /* analyze procedure - default */
        InvalidOid,                      /* element type ID */
        false,                           /* this is not an array type */
        enumArrayOid,                    /* array type we are about to create */
        InvalidOid,                      /* base type ID (only for domains) */
        NULL,                            /* never a default type value */
        NULL,                            /* binary default isn't sent either */
        true,                            /* always passed by value */
        'i',                             /* int alignment */
        'p',                             /* TOAST strategy always plain */
        -1,                              /* typMod (Domains only) */
        0,                               /* Array dimensions of typbasetype */
        false,                           /* Type NOT NULL */
        InvalidOid);                     /* type's collation */

    /* Enter the enum's values into pg_enum */
    EnumValuesCreate(enumTypeAddr.objectId, stmt->vals);

    /*
     * Create the array type that goes with it.
     */
    enumArrayName = makeArrayTypeName(enumName, enumNamespace);

    (void)TypeCreate(enumArrayOid, /* force assignment of this type OID */
        enumArrayName,       /* type name */
        enumNamespace,       /* namespace */
        InvalidOid,          /* relation oid (n/a here) */
        0,                   /* relation kind (ditto) */
        typowner,            /* owner's ID */
        -1,                  /* internal size (always varlena) */
        TYPTYPE_BASE,        /* type-type (base type) */
        TYPCATEGORY_ARRAY,   /* type-category (array) */
        false,               /* array types are never preferred */
        DEFAULT_TYPDELIM,    /* array element delimiter */
        F_ARRAY_IN,          /* input procedure */
        F_ARRAY_OUT,         /* output procedure */
        F_ARRAY_RECV,        /* receive procedure */
        F_ARRAY_SEND,        /* send procedure */
        InvalidOid,          /* typmodin procedure - none */
        InvalidOid,          /* typmodout procedure - none */
        F_ARRAY_TYPANALYZE,  /* analyze procedure */
        enumTypeAddr.objectId,         /* element type ID */
        true,                /* yes this is an array type */
        InvalidOid,          /* no further array type */
        InvalidOid,          /* base type ID */
        NULL,                /* never a default type value */
        NULL,                /* binary default isn't sent either */
        false,               /* never passed by value */
        'i',                 /* enums have align i, so do their arrays */
        'x',                 /* ARRAY is always toastable */
        -1,                  /* typMod (Domains only) */
        0,                   /* Array dimensions of typbasetype */
        false,               /* Type NOT NULL */
        InvalidOid);         /* type's collation */

    pfree_ext(enumArrayName);
    return enumTypeAddr;
}

/*
 * AlterEnum
 *		Adds a new label to an existing enum.
 */
ObjectAddress AlterEnum(AlterEnumStmt* stmt)
{
    Oid enum_type_oid;
    TypeName* typname = NULL;
    HeapTuple tup;
    ObjectAddress address;

    /* Make a TypeName so we can use standard type lookup machinery */
    typname = makeTypeNameFromNameList(stmt->typname);
    enum_type_oid = typenameTypeId(NULL, typname);

    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(enum_type_oid));
    if (!HeapTupleIsValid(tup)) {
        ereport(
            ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", enum_type_oid)));
    }

    /* Check it's an enum and check user has permission to ALTER the enum */
    checkEnumAlterPrivilege(tup);

    if (stmt->oldVal) {
        /* Rename an existing label */
        RenameEnumLabel(enum_type_oid, stmt->oldVal, stmt->newVal);
    } else {
        /* Add the new label */
        AddEnumLabel(enum_type_oid, stmt->newVal, stmt->newValNeighbor, stmt->newValIsAfter, stmt->skipIfNewValExists);
    }

    ObjectAddressSet(address, TypeRelationId, enum_type_oid);
    ReleaseSysCache(tup);
    return address;
}

/*
 * checkEnumAlterPrivilege
 *
 * Check that the type is actually an enum and that the current user
 * has permission to do ALTER TYPE on it.  Throw an error if not.
 */
static void checkEnumAlterPrivilege(HeapTuple tup)
{
    Form_pg_type typTup = (Form_pg_type)GETSTRUCT(tup);

    /* Check that this is actually an enum */
    if (typTup->typtype != TYPTYPE_ENUM)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("%s is not an enum", format_type_be(HeapTupleGetOid(tup)))));

    /* Permission check. */
    AclResult aclresult = pg_type_aclcheck(HeapTupleGetOid(tup), GetUserId(), ACL_ALTER);
    if (aclresult != ACLCHECK_OK && !pg_type_ownercheck(HeapTupleGetOid(tup), GetUserId())) {
        aclcheck_error_type(ACLCHECK_NO_PRIV, HeapTupleGetOid(tup));
    }
}

ObjectAddress DefineSet(CreateSetStmt *stmt)
{
    char* setName = NULL;
    Oid setNamespace;
    Oid setTypeOid;
    AclResult aclresult;
    Oid old_type_oid;
    Oid typowner = InvalidOid;
    ObjectAddress address;
    /*
     * isalter is true, change the owner of the objects as the owner of the
     * namespace, if the owner of the namespce has the same name as the namescpe
     */
    bool isalter = false;

    /* Convert list of names to a name and namespace */
    setNamespace = QualifiedNameGetCreationNamespace(stmt->typname->names, &setName);
    /*
     * anyResult is true, explain that the current user is granted create any type permission
     */
    bool anyResult = false;
    if (!IsSysSchema(setNamespace)) {
        anyResult = HasSpecAnyPriv(GetUserId(), CREATE_ANY_TYPE, false);
    }
    if (u_sess->attr.attr_sql.enforce_a_behavior) {
        typowner = GetUserIdFromNspId(setNamespace, false, anyResult);
        if (!OidIsValid(typowner))
            typowner = GetUserId();
        else if (typowner != GetUserId())
            isalter = true;
    } else {
        typowner = GetUserId();
    }
    /* Check we have creation rights in target namespace */
    aclresult = pg_namespace_aclcheck(setNamespace, GetUserId(), ACL_CREATE);
    if (aclresult != ACLCHECK_OK && !anyResult)
        aclcheck_error(aclresult, ACL_KIND_NAMESPACE, get_namespace_name(setNamespace));
    if (isalter) {
        (void)CheckCreatePrivilegeInNamespace(setNamespace, typowner, CREATE_ANY_TYPE);
    }

    /*
     * Check for collision with an existing type name.	If there is one and
     * it's an autogenerated array, we can rename it out of the way.
     */
    old_type_oid = GetSysCacheOid2(TYPENAMENSP, CStringGetDatum(setName), ObjectIdGetDatum(setNamespace));
    if (OidIsValid(old_type_oid)) {
        ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("type \"%s\" already exists", setName)));
    }

    /* Create the pg_type entry */
    address = TypeCreate(InvalidOid, /* no predetermined type OID */
        setName,                         /* type name */
        setNamespace,                    /* namespace */
        InvalidOid,                      /* relation oid (n/a here) */
        0,                               /* relation kind (ditto) */
        typowner,                        /* owner's ID */
        -1,                              /* internal size */
        TYPTYPE_SET,                     /* type-type (set type) */
        TYPCATEGORY_SET,                 /* type-category (set type) */
        true,                            /* set types are preferred */
        DEFAULT_TYPDELIM,                /* array element delimiter */
        F_SET_IN,                        /* input procedure */
        F_SET_OUT,                       /* output procedure */
        F_SET_RECV,                      /* receive procedure */
        F_SET_SEND,                      /* send procedure */
        InvalidOid,                      /* typmodin procedure - none */
        InvalidOid,                      /* typmodout procedure - none */
        InvalidOid,                      /* analyze procedure - default */
        InvalidOid,                      /* element type ID */
        false,                           /* this is not an array type */
        InvalidOid,                      /* array type we are about to create */
        InvalidOid,                      /* base type ID (only for domains) */
        NULL,                            /* never a default type value */
        NULL,                            /* binary default isn't sent either */
        false,                           /* always passed by value */
        'i',                             /* int alignment */
        'p',                             /* TOAST strategy always plain */
        -1,                              /* typMod (Domains only) */
        0,                               /* Array dimensions of typbasetype */
        false,                           /* Type NOT NULL */
        100);                            /* type's collation */

    /* Enter the set's values into pg_set */
    setTypeOid=address.objectId;
    SetValuesCreate(setTypeOid, stmt->typname->typmods, stmt->set_collation);
    stmt->typname->typeOid = setTypeOid;
    stmt->typname->typmods = NIL;
    return address;
}

/*
 * DefineRange
 *		Registers a new range type.
 */
ObjectAddress DefineRange(CreateRangeStmt* stmt)
{
    char* typname = NULL;
    Oid typeNamespace;
    Oid typoid;
    char* rangeArrayName = NULL;
    Oid rangeArrayOid;
    Oid rangeSubtype = InvalidOid;
    List* rangeSubOpclassName = NIL;
    List* rangeCollationName = NIL;
    List* rangeCanonicalName = NIL;
    List* rangeSubtypeDiffName = NIL;
    Oid rangeSubOpclass;
    Oid rangeCollation;
    regproc rangeCanonical;
    regproc rangeSubtypeDiff;
    int16 subtyplen;
    bool subtypbyval = false;
    char subtypalign;
    char alignment;
    AclResult aclresult;
    ListCell* lc = NULL;
    Oid typowner = InvalidOid;
    ObjectAddress address;

    /*
     * isalter is true, change the owner of the objects as the owner of the
     * namespace, if the owner of the namespce has the same name as the namescpe
     */
    bool isalter = false;

    /* Convert list of names to a name and namespace */
    typeNamespace = QualifiedNameGetCreationNamespace(stmt->typname, &typname);
    /*
     * anyResult is true, explain that the current user is granted create any typepermission
     */
    bool anyResult = false;
    if (!IsSysSchema(typeNamespace)) {
        anyResult = HasSpecAnyPriv(GetUserId(), CREATE_ANY_TYPE, false);
    }
    if (u_sess->attr.attr_sql.enforce_a_behavior) {
        typowner = GetUserIdFromNspId(typeNamespace, false, anyResult);

        if (!OidIsValid(typowner)) {
            typowner = GetUserId();
        }
        else if (typowner != GetUserId()) {
            isalter = true;
        }
    } else {
        typowner = GetUserId();
    }
    /* Check we have creation rights in target namespace */
    aclresult = pg_namespace_aclcheck(typeNamespace, GetUserId(), ACL_CREATE);
    if (aclresult != ACLCHECK_OK && !anyResult)
        aclcheck_error(aclresult, ACL_KIND_NAMESPACE, get_namespace_name(typeNamespace));
    if (isalter) {
        (void)CheckCreatePrivilegeInNamespace(typeNamespace, typowner, CREATE_ANY_TYPE);
    }

    /*
     * Look to see if type already exists.
     */
    typoid = GetSysCacheOid2(TYPENAMENSP, CStringGetDatum(typname), ObjectIdGetDatum(typeNamespace));

    /*
     * If it's not a shell, see if it's an autogenerated array type, and if so
     * rename it out of the way.
     */
    if (OidIsValid(typoid) && get_typisdefined(typoid)) {
        if (moveArrayTypeName(typoid, typname, typeNamespace))
            typoid = InvalidOid;
        else
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("type \"%s\" already exists", typname)));
    }

    /*
     * If it doesn't exist, create it as a shell, so that the OID is known for
     * use in the range function definitions.
     */
    if (!OidIsValid(typoid)) {
        address = TypeShellMake(typname, typeNamespace, typowner);
        /* Make new shell type visible for modification below */
        CommandCounterIncrement();
        typoid = address.objectId;
    }

    /* Extract the parameters from the parameter list */
    foreach (lc, stmt->params) {
        DefElem* defel = (DefElem*)lfirst(lc);

        if (pg_strcasecmp(defel->defname, "subtype") == 0) {
            if (OidIsValid(rangeSubtype))
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            /* we can look up the subtype name immediately */
            rangeSubtype = typenameTypeId(NULL, defGetTypeName(defel));
        } else if (pg_strcasecmp(defel->defname, "subtype_opclass") == 0) {
            if (rangeSubOpclassName != NIL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            rangeSubOpclassName = defGetQualifiedName(defel);
        } else if (pg_strcasecmp(defel->defname, "collation") == 0) {
            if (rangeCollationName != NIL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            rangeCollationName = defGetQualifiedName(defel);
        } else if (pg_strcasecmp(defel->defname, "canonical") == 0) {
            if (rangeCanonicalName != NIL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            rangeCanonicalName = defGetQualifiedName(defel);
        } else if (pg_strcasecmp(defel->defname, "subtype_diff") == 0) {
            if (rangeSubtypeDiffName != NIL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            rangeSubtypeDiffName = defGetQualifiedName(defel);
        } else
            ereport(
                ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("type attribute \"%s\" not recognized", defel->defname)));
    }

    /* Must have a subtype */
    if (!OidIsValid(rangeSubtype))
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("type attribute \"subtype\" is required")));
    /* disallow ranges of pseudotypes */
    if (get_typtype(rangeSubtype) == TYPTYPE_PSEUDO)
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("range subtype cannot be %s", format_type_be(rangeSubtype))));

    /* Identify subopclass */
    rangeSubOpclass = findRangeSubOpclass(rangeSubOpclassName, rangeSubtype);

    /* Identify collation to use, if any */
    if (type_is_collatable(rangeSubtype)) {
        if (rangeCollationName != NIL)
            rangeCollation = get_collation_oid(rangeCollationName, false);
        else
            rangeCollation = get_typcollation(rangeSubtype);
    } else {
        if (rangeCollationName != NIL)
            ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                    errmsg("range collation specified but subtype does not support collation")));
        rangeCollation = InvalidOid;
    }

    /* Identify support functions, if provided */
    if (rangeCanonicalName != NIL)
        rangeCanonical = findRangeCanonicalFunction(rangeCanonicalName, typoid);
    else
        rangeCanonical = InvalidOid;

    if (rangeSubtypeDiffName != NIL)
        rangeSubtypeDiff = findRangeSubtypeDiffFunction(rangeSubtypeDiffName, rangeSubtype);
    else
        rangeSubtypeDiff = InvalidOid;

    get_typlenbyvalalign(rangeSubtype, &subtyplen, &subtypbyval, &subtypalign);

    /* alignment must be 'i' or 'd' for ranges */
    alignment = (subtypalign == 'd') ? 'd' : 'i';

    /* Allocate OID for array type */
    rangeArrayOid = AssignTypeArrayOid();

    /* Create the pg_type entry */
    address = TypeCreate(InvalidOid, /* no predetermined type OID */
        typname,                    /* type name */
        typeNamespace,              /* namespace */
        InvalidOid,                 /* relation oid (n/a here) */
        0,                          /* relation kind (ditto) */
        typowner,                   /* owner's ID */
        -1,                         /* internal size (always varlena) */
        TYPTYPE_RANGE,              /* type-type (range type) */
        TYPCATEGORY_RANGE,          /* type-category (range type) */
        false,                      /* range types are never preferred */
        DEFAULT_TYPDELIM,           /* array element delimiter */
        F_RANGE_IN,                 /* input procedure */
        F_RANGE_OUT,                /* output procedure */
        F_RANGE_RECV,               /* receive procedure */
        F_RANGE_SEND,               /* send procedure */
        InvalidOid,                 /* typmodin procedure - none */
        InvalidOid,                 /* typmodout procedure - none */
        F_RANGE_TYPANALYZE,         /* analyze procedure */
        InvalidOid,                 /* element type ID - none */
        false,                      /* this is not an array type */
        rangeArrayOid,              /* array type we are about to create */
        InvalidOid,                 /* base type ID (only for domains) */
        NULL,                       /* never a default type value */
        NULL,                       /* no binary form available either */
        false,                      /* never passed by value */
        alignment,                  /* alignment */
        'x',                        /* TOAST strategy (always extended) */
        -1,                         /* typMod (Domains only) */
        0,                          /* Array dimensions of typbasetype */
        false,                      /* Type NOT NULL */
        InvalidOid);                /* type's collation (ranges never have one) */

    typoid = address.objectId;
    /* Create the entry in pg_range */
    RangeCreate(typoid, rangeSubtype, rangeCollation, rangeSubOpclass, rangeCanonical, rangeSubtypeDiff);

    /*
     * Create the array type that goes with it.
     */
    rangeArrayName = makeArrayTypeName(typname, typeNamespace);

    (void)TypeCreate(rangeArrayOid, /* force assignment of this type OID */
        rangeArrayName,       /* type name */
        typeNamespace,        /* namespace */
        InvalidOid,           /* relation oid (n/a here) */
        0,                    /* relation kind (ditto) */
        typowner,             /* owner's ID */
        -1,                   /* internal size (always varlena) */
        TYPTYPE_BASE,         /* type-type (base type) */
        TYPCATEGORY_ARRAY,    /* type-category (array) */
        false,                /* array types are never preferred */
        DEFAULT_TYPDELIM,     /* array element delimiter */
        F_ARRAY_IN,           /* input procedure */
        F_ARRAY_OUT,          /* output procedure */
        F_ARRAY_RECV,         /* receive procedure */
        F_ARRAY_SEND,         /* send procedure */
        InvalidOid,           /* typmodin procedure - none */
        InvalidOid,           /* typmodout procedure - none */
        F_ARRAY_TYPANALYZE,   /* analyze procedure */
        typoid,               /* element type ID */
        true,                 /* yes this is an array type */
        InvalidOid,           /* no further array type */
        InvalidOid,           /* base type ID */
        NULL,                 /* never a default type value */
        NULL,                 /* binary default isn't sent either */
        false,                /* never passed by value */
        alignment,            /* alignment - same as range's */
        'x',                  /* ARRAY is always toastable */
        -1,                   /* typMod (Domains only) */
        0,                    /* Array dimensions of typbasetype */
        false,                /* Type NOT NULL */
        InvalidOid);          /* typcollation */

    pfree_ext(rangeArrayName);

    /* And create the constructor functions for this range type */
    makeRangeConstructors(typname, typeNamespace, typoid, rangeSubtype);
    return address;
}

/*
 * Because there may exist several range types over the same subtype, the
 * range type can't be uniquely determined from the subtype.  So it's
 * impossible to define a polymorphic constructor; we have to generate new
 * constructor functions explicitly for each range type.
 *
 * We actually define 4 functions, with 0 through 3 arguments.	This is just
 * to offer more convenience for the user.
 */
static void makeRangeConstructors(const char* name, Oid nmspace, Oid rangeOid, Oid subtype)
{
    static const char* const prosrc[2] = {"range_constructor2", "range_constructor3"};
    static const int pronargs[2] = {2, 3};

    Oid constructorArgTypes[3];
    ObjectAddress myself, referenced;
    uint32 i;

    constructorArgTypes[0] = subtype;
    constructorArgTypes[1] = subtype;
    constructorArgTypes[2] = TEXTOID;

    referenced.classId = TypeRelationId;
    referenced.objectId = rangeOid;
    referenced.objectSubId = 0;

    for (i = 0; i < lengthof(prosrc); i++) {
        oidvector* constructorArgTypesVector = NULL;

        constructorArgTypesVector = buildoidvector(constructorArgTypes, pronargs[i]);

        /*
         * add an argument to record the position of arguments with default value.
         * it is null here since that there is no argument with default value.
         */
        myself = ProcedureCreate(name, /* name: same as range type */
            nmspace,                    /* namespace */
            InvalidOid,                 /* package oid default invalid oid*/
            false,                      /* not A db compatible */
            false,                      /* replace */
            false,                      /* returns set */
            rangeOid,                   /* return type */
            BOOTSTRAP_SUPERUSERID,      /* proowner */
            INTERNALlanguageId,         /* language */
            F_FMGR_INTERNAL_VALIDATOR,  /* language validator */
            prosrc[i],                  /* prosrc */
            NULL,                       /* probin */
            false,                      /* isAgg */
            false,                      /* isWindowFunc */
            false,                      /* security_definer */
            false,                      /* leakproof */
            false,                      /* isStrict */
            PROVOLATILE_IMMUTABLE,      /* volatility */
            constructorArgTypesVector,  /* parameterTypes */
            PointerGetDatum(NULL),      /* allParameterTypes */
            PointerGetDatum(NULL),      /* parameterModes */
            PointerGetDatum(NULL),      /* parameterNames */
            NIL,                        /* parameterDefaults */
            PointerGetDatum(NULL),      /* proconfig */
            1.0,                        /* procost */
            0.0,                        /* prorows */
            NULL,                       /* prodefaultargpos */
            false,
            false,
            false,
            false,
            NULL);

        /*
         * Make the constructors internally-dependent on the range type so
         * that they go away silently when the type is dropped.  Note that
         * pg_dump depends on this choice to avoid dumping the constructors.
         */

        recordDependencyOn(&myself, &referenced, DEPENDENCY_INTERNAL);
    }
}

/*
 * Find suitable I/O functions for a type.
 *
 * typeOid is the type's OID (which will already exist, if only as a shell
 * type).
 */
static Oid findTypeInputFunction(List* procname, Oid typeOid)
{
    Oid argList[3];
    Oid procOid;

    /*
     * Input functions can take a single argument of type CSTRING, or three
     * arguments (string, typioparam OID, typmod).
     *
     * For backwards compatibility we allow OPAQUE in place of CSTRING; if we
     * see this, we issue a warning and fix up the pg_proc entry.
     */
    argList[0] = CSTRINGOID;

    procOid = LookupFuncName(procname, 1, argList, true);
    if (OidIsValid(procOid))
        return procOid;

    argList[1] = OIDOID;
    argList[2] = INT4OID;

    procOid = LookupFuncName(procname, 3, argList, true);
    if (OidIsValid(procOid))
        return procOid;

    /* No luck, try it with OPAQUE */
    argList[0] = OPAQUEOID;

    procOid = LookupFuncName(procname, 1, argList, true);

    if (!OidIsValid(procOid)) {
        argList[1] = OIDOID;
        argList[2] = INT4OID;

        procOid = LookupFuncName(procname, 3, argList, true);
    }

    if (OidIsValid(procOid)) {
        /* Found, but must complain and fix the pg_proc entry */
        ereport(WARNING,
            (errmsg(
                "changing argument type of function %s from \"opaque\" to \"cstring\"", NameListToString(procname))));
        SetFunctionArgType(procOid, 0, CSTRINGOID);

        /*
         * Need CommandCounterIncrement since DefineType will likely try to
         * alter the pg_proc tuple again.
         */
        CommandCounterIncrement();

        return procOid;
    }

    /* Use CSTRING (preferred) in the error message */
    argList[0] = CSTRINGOID;

    ereport(ERROR,
        (errcode(ERRCODE_UNDEFINED_FUNCTION),
            errmsg("function %s does not exist", func_signature_string(procname, 1, NIL, argList))));

    return InvalidOid; /* keep compiler quiet */
}

static Oid findTypeOutputFunction(List* procname, Oid typeOid)
{
    Oid argList[1];
    Oid procOid;

    /*
     * Output functions can take a single argument of the type.
     *
     * For backwards compatibility we allow OPAQUE in place of the actual type
     * name; if we see this, we issue a warning and fix up the pg_proc entry.
     */
    argList[0] = typeOid;

    procOid = LookupFuncName(procname, 1, argList, true);
    if (OidIsValid(procOid))
        return procOid;

    /* No luck, try it with OPAQUE */
    argList[0] = OPAQUEOID;

    procOid = LookupFuncName(procname, 1, argList, true);

    if (OidIsValid(procOid)) {
        /* Found, but must complain and fix the pg_proc entry */
        ereport(WARNING,
            (errmsg("changing argument type of function %s from \"opaque\" to %s",
                NameListToString(procname),
                format_type_be(typeOid))));
        SetFunctionArgType(procOid, 0, typeOid);

        /*
         * Need CommandCounterIncrement since DefineType will likely try to
         * alter the pg_proc tuple again.
         */
        CommandCounterIncrement();

        return procOid;
    }

    /* Use type name, not OPAQUE, in the failure message. */
    argList[0] = typeOid;

    ereport(ERROR,
        (errcode(ERRCODE_UNDEFINED_FUNCTION),
            errmsg("function %s does not exist", func_signature_string(procname, 1, NIL, argList))));

    return InvalidOid; /* keep compiler quiet */
}

static Oid findTypeReceiveFunction(List* procname, Oid typeOid)
{
    Oid argList[3];
    Oid procOid;

    /*
     * Receive functions can take a single argument of type INTERNAL, or three
     * arguments (internal, typioparam OID, typmod).
     */
    argList[0] = INTERNALOID;

    procOid = LookupFuncName(procname, 1, argList, true);
    if (OidIsValid(procOid))
        return procOid;

    argList[1] = OIDOID;
    argList[2] = INT4OID;

    procOid = LookupFuncName(procname, 3, argList, true);
    if (OidIsValid(procOid))
        return procOid;

    ereport(ERROR,
        (errcode(ERRCODE_UNDEFINED_FUNCTION),
            errmsg("function %s does not exist", func_signature_string(procname, 1, NIL, argList))));

    return InvalidOid; /* keep compiler quiet */
}

static Oid findTypeSendFunction(List* procname, Oid typeOid)
{
    Oid argList[1];
    Oid procOid;

    /*
     * Send functions can take a single argument of the type.
     */
    argList[0] = typeOid;

    procOid = LookupFuncName(procname, 1, argList, true);
    if (OidIsValid(procOid))
        return procOid;

    ereport(ERROR,
        (errcode(ERRCODE_UNDEFINED_FUNCTION),
            errmsg("function %s does not exist", func_signature_string(procname, 1, NIL, argList))));

    return InvalidOid; /* keep compiler quiet */
}

static Oid findTypeTypmodinFunction(List* procname)
{
    Oid argList[1];
    Oid procOid;

    /*
     * typmodin functions always take one cstring[] argument and return int4.
     */
    argList[0] = CSTRINGARRAYOID;

    procOid = LookupFuncName(procname, 1, argList, true);
    if (!OidIsValid(procOid))
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_FUNCTION),
                errmsg("function %s does not exist", func_signature_string(procname, 1, NIL, argList))));

    if (get_func_rettype(procOid) != INT4OID)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("typmod_in function %s must return type \"integer\"", NameListToString(procname))));

    return procOid;
}

static Oid findTypeTypmodoutFunction(List* procname)
{
    Oid argList[1];
    Oid procOid;

    /*
     * typmodout functions always take one int4 argument and return cstring.
     */
    argList[0] = INT4OID;

    procOid = LookupFuncName(procname, 1, argList, true);
    if (!OidIsValid(procOid))
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_FUNCTION),
                errmsg("function %s does not exist", func_signature_string(procname, 1, NIL, argList))));

    if (get_func_rettype(procOid) != CSTRINGOID)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("typmod_out function %s must return type \"cstring\"", NameListToString(procname))));

    return procOid;
}

static Oid findTypeAnalyzeFunction(List* procname, Oid typeOid)
{
    Oid argList[1];
    Oid procOid;

    /*
     * Analyze functions always take one INTERNAL argument and return bool.
     */
    argList[0] = INTERNALOID;

    procOid = LookupFuncName(procname, 1, argList, true);
    if (!OidIsValid(procOid))
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_FUNCTION),
                errmsg("function %s does not exist", func_signature_string(procname, 1, NIL, argList))));

    if (get_func_rettype(procOid) != BOOLOID)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("type analyze function %s must return type \"boolean\"", NameListToString(procname))));

    return procOid;
}

/*
 * Find suitable support functions and opclasses for a range type.
 *
 * Find named btree opclass for subtype, or default btree opclass if
 * opcname is NIL.
 */
static Oid findRangeSubOpclass(List* opcname, Oid subtype)
{
    Oid opcid;
    Oid opInputType;

    if (opcname != NIL) {
        opcid = get_opclass_oid(BTREE_AM_OID, opcname, false); 

        /*
         * Verify that the operator class accepts this datatype. Note we will
         * accept binary compatibility.
         */
        opInputType = get_opclass_input_type(opcid);
        if (!IsBinaryCoercible(subtype, opInputType))
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                    errmsg("operator class \"%s\" does not accept data type %s",
                        NameListToString(opcname),
                        format_type_be(subtype))));
    } else {
        opcid = GetDefaultOpClass(subtype, BTREE_AM_OID); 
        if (!OidIsValid(opcid)) {
            /* We spell the error message identically to GetIndexOpClass */
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("data type %s has no default operator class for access method \"%s\"",
                        format_type_be(subtype),
                        "btree"),
                    errhint("You must specify an operator class for the range type or define a default operator class "
                            "for the subtype.")));
        }
    }

    return opcid;
}

static Oid findRangeCanonicalFunction(List* procname, Oid typeOid)
{
    Oid argList[1];
    Oid procOid;
    AclResult aclresult;

    /*
     * Range canonical functions must take and return the range type, and must
     * be immutable.
     */
    argList[0] = typeOid;

    procOid = LookupFuncName(procname, 1, argList, true);

    if (!OidIsValid(procOid))
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_FUNCTION),
                errmsg("function %s does not exist", func_signature_string(procname, 1, NIL, argList))));

    if (get_func_rettype(procOid) != typeOid)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("range canonical function %s must return range type",
                    func_signature_string(procname, 1, NIL, argList))));

    if (func_volatile(procOid) != PROVOLATILE_IMMUTABLE)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("range canonical function %s must be immutable",
                    func_signature_string(procname, 1, NIL, argList))));

    /* Also, range type's creator must have permission to call function */
    aclresult = pg_proc_aclcheck(procOid, GetUserId(), ACL_EXECUTE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(procOid));

    return procOid;
}

static Oid findRangeSubtypeDiffFunction(List* procname, Oid subtype)
{
    Oid argList[2];
    Oid procOid;
    AclResult aclresult;

    /*
     * Range subtype diff functions must take two arguments of the subtype,
     * must return float8, and must be immutable.
     */
    argList[0] = subtype;
    argList[1] = subtype;

    procOid = LookupFuncName(procname, 2, argList, true);

    if (!OidIsValid(procOid))
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_FUNCTION),
                errmsg("function %s does not exist", func_signature_string(procname, 2, NIL, argList))));

    if (get_func_rettype(procOid) != FLOAT8OID)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("range subtype diff function %s must return type double precision",
                    func_signature_string(procname, 2, NIL, argList))));

    if (func_volatile(procOid) != PROVOLATILE_IMMUTABLE)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
                errmsg("range subtype diff function %s must be immutable",
                    func_signature_string(procname, 2, NIL, argList))));

    /* Also, range type's creator must have permission to call function */
    aclresult = pg_proc_aclcheck(procOid, GetUserId(), ACL_EXECUTE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(procOid));

    return procOid;
}

/*
 *	AssignTypeArrayOid
 *
 *	Pre-assign the type's array OID for use in pg_type.typarray
 */
Oid AssignTypeArrayOid(void)
{
    Oid type_array_oid;

    /* Use binary-upgrade override for pg_type.typarray, if supplied. */
    if (u_sess->proc_cxt.IsBinaryUpgrade && OidIsValid(u_sess->upg_cxt.binary_upgrade_next_array_pg_type_oid)) {
        type_array_oid = u_sess->upg_cxt.binary_upgrade_next_array_pg_type_oid;
        u_sess->upg_cxt.binary_upgrade_next_array_pg_type_oid = InvalidOid;
    } else if (u_sess->attr.attr_common.IsInplaceUpgrade &&
               OidIsValid(u_sess->upg_cxt.Inplace_upgrade_next_array_pg_type_oid)) {
        type_array_oid = u_sess->upg_cxt.Inplace_upgrade_next_array_pg_type_oid;
        u_sess->upg_cxt.Inplace_upgrade_next_array_pg_type_oid = InvalidOid;
    } else {
        Relation pg_type = heap_open(TypeRelationId, AccessShareLock);

        type_array_oid = GetNewOid(pg_type);
        heap_close(pg_type, AccessShareLock);
    }

    return type_array_oid;
}

static ObjectAddress ReplaceTableOfType(Oid oldTypeOid, Oid refTypeOid)
{
    Relation pg_type_desc = NULL;
    HeapTuple typtuple = NULL;
    Form_pg_type typform = NULL;
    Oid old_elemtype = InvalidOid;
    ObjectAddress address;

    ObjectAddressSet(address, TypeRelationId, oldTypeOid);
    /* if any table depend on this type, report ERROR */
    ReplaceTypeCheckRef(&address);

    /* change typelem in pg_type */
    pg_type_desc = heap_open(TypeRelationId, RowExclusiveLock);
    typtuple = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(oldTypeOid));
    if (!HeapTupleIsValid(typtuple)) {
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), 
            errmsg("cache lookup failed for type %u", oldTypeOid)));
    }
    typform = (Form_pg_type)GETSTRUCT(typtuple);
    if (typform->typtype != TYPTYPE_TABLEOF) {
        tableam_tops_free_tuple(typtuple);
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), 
            errmsg("type already exists but not a table of type")));
    }
    old_elemtype = typform->typelem;
    typform->typelem = refTypeOid;
    simple_heap_update(pg_type_desc, &typtuple->t_self, typtuple);
    /* update the system catalog indexes */
    CatalogUpdateIndexes(pg_type_desc, typtuple);

    tableam_tops_free_tuple(typtuple);
    heap_close(pg_type_desc, RowExclusiveLock);

    /* find record between type and old_elemtype in pg_depend, and remove it */
    Relation depRel = NULL;
    ScanKeyData key[2];
    int nkeys = 2;
    SysScanDesc scan = NULL;
    HeapTuple tup = NULL;
    depRel = heap_open(DependRelationId, RowExclusiveLock);
    ScanKeyInit(&key[0], Anum_pg_depend_classid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(TypeRelationId));
    ScanKeyInit(&key[1], Anum_pg_depend_objid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(oldTypeOid));
    scan = systable_beginscan(depRel, DependDependerIndexId, true, NULL, nkeys, key);

    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        Form_pg_depend foundDep = (Form_pg_depend)GETSTRUCT(tup);
        if (foundDep->refobjid == old_elemtype) {
            simple_heap_delete(depRel, &tup->t_self);
        }
    }
    systable_endscan(scan);
    heap_close(depRel, RowExclusiveLock);

    /* record with new elemtype */
    ObjectAddress myself, referenced;
    myself.classId = TypeRelationId;
    myself.objectId = oldTypeOid;
    myself.objectSubId = 0;
    referenced.classId = TypeRelationId;
    referenced.objectId = refTypeOid;
    referenced.objectSubId = 0;
    recordDependencyOn(&myself, &referenced, DEPENDENCY_NORMAL);

    CommandCounterIncrement();

    return address;
}

/*
 * DefineRange
 *		Registers a new table of type.
 * create typy A as table of B, there are B and _B in pg_type
 * add oid of _B in element
 */
ObjectAddress DefineTableOfType(const TableOfTypeStmt* stmt)
{
    char* typname = NULL;
    Oid typeNamespace;
    Oid typoid;
    AclResult aclresult;
    Oid typowner = InvalidOid;
    /*
     * isalter is true, change the owner of the objects as the owner of the
     * namespace, if the owner of the namespce has the same name as the namescpe
     */
    bool isalter = false;

    /* Convert list of names to a name and namespace */
    typeNamespace = QualifiedNameGetCreationNamespace(stmt->typname, &typname);
    if (u_sess->attr.attr_sql.enforce_a_behavior) {
        typowner = GetUserIdFromNspId(typeNamespace);

        if (!OidIsValid(typowner))
            typowner = GetUserId();
        else if (typowner != GetUserId())
            isalter = true;
    } else {
        typowner = GetUserId();
    }
    /* Check we have creation rights in target namespace */
    aclresult = pg_namespace_aclcheck(typeNamespace, GetUserId(), ACL_CREATE);
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, ACL_KIND_NAMESPACE, get_namespace_name(typeNamespace));
    if (isalter) {
        aclresult = pg_namespace_aclcheck(typeNamespace, typowner, ACL_CREATE);
        if (aclresult != ACLCHECK_OK)
            aclcheck_error(aclresult, ACL_KIND_NAMESPACE, get_namespace_name(typeNamespace));
    }

    /*
     * Check for collision with an existing type name.	If there is one and
     * it's an autogenerated array, we can rename it out of the way.
     */
    typoid = GetSysCacheOid2(TYPENAMENSP, CStringGetDatum(typname), ObjectIdGetDatum(typeNamespace));
    if (OidIsValid(typoid)) {
        if (!moveArrayTypeName(typoid, typname, typeNamespace) && !stmt->replace)
            ereport(ERROR, (errcode(ERRCODE_DUPLICATE_OBJECT), errmsg("type \"%s\" already exists", typname)));
    }

    /* look up the referenced type name */
    Oid typeOid = typenameTypeId(NULL, stmt->reftypname);
    /* Must have a referenced type */
    if (!OidIsValid(typeOid))
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("referenced type does not exist")));

#ifndef ENABLE_MULTIPLE_NODES
    /* don't allow package or procedure type as table of base type */
    if (IsPackageDependType(typeOid, InvalidOid)) {
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmodule(MOD_PLSQL),
                errmsg("type %s doesn't support table type.", TypeNameToString(stmt->reftypname)),
                errdetail("\"%s\" is a package or procedure type", TypeNameToString(stmt->reftypname)),
                errcause("feature not supported"),
                erraction("check type name")));
    }
#endif
    
    HeapTuple type_tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typeOid));
    if (!HeapTupleIsValid(type_tup)) {
        ereport(ERROR,
            (errmodule(MOD_PLSQL),
                errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                errmsg("cache lookup failed for type %u, type Oid is invalid", typeOid)));
    }

    if (((Form_pg_type)GETSTRUCT(type_tup))->typtype == TYPTYPE_TABLEOF) {
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("table type does not support nested table.")));
    }

    Oid refTypeOid = ((Form_pg_type)GETSTRUCT(type_tup))->typarray;
    if (!OidIsValid(refTypeOid)) {
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("type %s doesn't support table type.", (((Form_pg_type)GETSTRUCT(type_tup))->typname).data)));
    }
    ReleaseSysCache(type_tup);

    if (OidIsValid(typoid) && get_typisdefined(typoid)) {
        return ReplaceTableOfType(typoid, refTypeOid);
    } else {
        /* Create the pg_type entry */
        return TypeCreate(InvalidOid, /* no predetermined type OID */
            typname,                    /* type name */
            typeNamespace,              /* namespace */
            InvalidOid,                 /* relation oid (n/a here) */
            0,                          /* relation kind (ditto) */
            typowner,                   /* owner's ID */
            -1,                         /* internal size (always varlena) */
            TYPTYPE_TABLEOF,            /* type-type (table of type) */
            TYPCATEGORY_TABLEOF,        /* type-category (table of type) */
            false,                      /* table of types are never preferred */
            DEFAULT_TYPDELIM,           /* array element delimiter */
            F_ARRAY_IN,                 /* array input proc */
            F_ARRAY_OUT,                /* array output proc */
            F_ARRAY_RECV,               /* array recv (bin) proc */
            F_ARRAY_SEND,               /* array send (bin) proc */
            InvalidOid,                 /* typmodin procedure - none */
            InvalidOid,                 /* typmodout procedure - none */
            F_ARRAY_TYPANALYZE,         /* array analyze procedure */
            refTypeOid,                 /* element type ID - none */
            false,                      /* this is not an array type */
            InvalidOid,                 /* array type we are about to create */
            InvalidOid,                 /* base type ID (only for domains) */
            NULL,                       /* never a default type value */
            NULL,                       /* no binary form available either */
            false,                      /* never passed by value */
            'd',                        /* alignment */
            'x',                        /* TOAST strategy (always extended) */
            -1,                         /* typMod (Domains only) */
            0,                          /* Array dimensions of typbasetype */
            false,                      /* Type NOT NULL */
            InvalidOid);                /* type's collation (ranges never have one) */
    }
    
}

/* -------------------------------------------------------------------
 * DefineCompositeType
 *
 * Create a Composite Type relation.
 * `DefineRelation' does all the work, we just provide the correct
 * arguments!
 *
 * If the relation already exists, then 'DefineRelation' will abort
 * the xact...
 *
 * DefineCompositeType returns relid for use when creating
 * an implicit composite type during function creation
 * -------------------------------------------------------------------
 */
ObjectAddress DefineCompositeType(RangeVar* typevar, List* coldeflist)
{
    CreateStmt* createStmt = makeNode(CreateStmt);
    Oid old_type_oid;
    Oid typeNamespace;
    ObjectAddress address;
    /*
     * now set the parameters for keys/inheritance etc. All of these are
     * uninteresting for composite types...
     */
    createStmt->relation = typevar;
    createStmt->tableElts = coldeflist;
    createStmt->inhRelations = NIL;
    createStmt->constraints = NIL;
    createStmt->options = list_make1(defWithOids(false));
    createStmt->oncommit = ONCOMMIT_NOOP;
    createStmt->tablespacename = NULL;
    createStmt->if_not_exists = false;
    createStmt->charset = PG_INVALID_ENCODING;

    /*
     * Check for collision with an existing type name. If there is one and
     * it's an autogenerated array, we can rename it out of the way.  This
     * check is here mainly to get a better error message about a "type"
     * instead of below about a "relation".
     */
    typeNamespace = RangeVarGetAndCheckCreationNamespace(createStmt->relation, NoLock, NULL, RELKIND_COMPOSITE_TYPE);
    RangeVarAdjustRelationPersistence(createStmt->relation, typeNamespace);
    old_type_oid =
        GetSysCacheOid2(TYPENAMENSP, CStringGetDatum(createStmt->relation->relname), ObjectIdGetDatum(typeNamespace));
    if (OidIsValid(old_type_oid)) {
        if (!moveArrayTypeName(old_type_oid, createStmt->relation->relname, typeNamespace))
            ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                    errmsg("type \"%s\" already exists", createStmt->relation->relname)));
    }

    /*
     * Finally create the relation.  This also creates the type.
     */

    DefineRelation(createStmt, RELKIND_COMPOSITE_TYPE, InvalidOid, &address);
    return address;
}

/*
 * AlterDomainDefault
 *
 * Routine implementing ALTER DOMAIN SET/DROP DEFAULT statements.
 */
ObjectAddress AlterDomainDefault(List* names, Node* defaultRaw)
{
    TypeName* typname = NULL;
    Oid domainoid;
    HeapTuple tup;
    ParseState* pstate = NULL;
    Relation rel;
    char* defaultValue = NULL;
    Node* defaultExpr = NULL; /* NULL if no default specified */
    Datum new_record[Natts_pg_type];
    bool new_record_nulls[Natts_pg_type];
    bool new_record_repl[Natts_pg_type];
    HeapTuple newtuple;
    Form_pg_type typTup;
    ObjectAddress address;

    /* Make a TypeName so we can use standard type lookup machinery */
    typname = makeTypeNameFromNameList(names);
    domainoid = typenameTypeId(NULL, typname);

    /* Look up the domain in the type table */
    rel = heap_open(TypeRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(domainoid));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", domainoid)));
    typTup = (Form_pg_type)GETSTRUCT(tup);

    /* Check it's a domain and check user has permission for ALTER DOMAIN */
    checkDomainOwner(tup);

    /* Setup new tuple */
    errno_t errorno = memset_s(new_record, sizeof(new_record), (Datum)0, sizeof(new_record));
    securec_check(errorno, "\0", "\0");
    errorno = memset_s(new_record_nulls, sizeof(new_record_nulls), false, sizeof(new_record_nulls));
    securec_check(errorno, "\0", "\0");
    errorno = memset_s(new_record_repl, sizeof(new_record_repl), false, sizeof(new_record_repl));
    securec_check(errorno, "\0", "\0");

    /* Store the new default into the tuple */
    if (defaultRaw != NULL) {
        /* Create a dummy ParseState for transformExpr */
        pstate = make_parsestate(NULL);

        /*
         * Cook the colDef->raw_expr into an expression. Note: Name is
         * strictly for error message
         */
        defaultExpr = cookDefault(pstate, defaultRaw, typTup->typbasetype, typTup->typtypmod,
            typTup->typcollation, NameStr(typTup->typname), 0);
        /*
         * If the expression is just a NULL constant, we treat the command
         * like ALTER ... DROP DEFAULT.  (But see note for same test in
         * DefineDomain.)
         */
        if (defaultExpr == NULL || (IsA(defaultExpr, Const) && ((Const*)defaultExpr)->constisnull)) {
            /* Default is NULL, drop it */
            new_record_nulls[Anum_pg_type_typdefaultbin - 1] = true;
            new_record_repl[Anum_pg_type_typdefaultbin - 1] = true;
            new_record_nulls[Anum_pg_type_typdefault - 1] = true;
            new_record_repl[Anum_pg_type_typdefault - 1] = true;
        } else {
            /*
             * Expression must be stored as a nodeToString result, but we also
             * require a valid textual representation (mainly to make life
             * easier for pg_dump).
             */
            defaultValue = deparse_expression(
                defaultExpr, deparse_context_for(NameStr(typTup->typname), InvalidOid), false, false);

            /*
             * Form an updated tuple with the new default and write it back.
             */
            new_record[Anum_pg_type_typdefaultbin - 1] = CStringGetTextDatum(nodeToString(defaultExpr));

            new_record_repl[Anum_pg_type_typdefaultbin - 1] = true;
            new_record[Anum_pg_type_typdefault - 1] = CStringGetTextDatum(defaultValue);
            new_record_repl[Anum_pg_type_typdefault - 1] = true;
        }
    } else {
        /* ALTER ... DROP DEFAULT */
        new_record_nulls[Anum_pg_type_typdefaultbin - 1] = true;
        new_record_repl[Anum_pg_type_typdefaultbin - 1] = true;
        new_record_nulls[Anum_pg_type_typdefault - 1] = true;
        new_record_repl[Anum_pg_type_typdefault - 1] = true;
    }

    newtuple = (HeapTuple) tableam_tops_modify_tuple(tup, RelationGetDescr(rel), new_record, new_record_nulls, new_record_repl);

    simple_heap_update(rel, &tup->t_self, newtuple);

    CatalogUpdateIndexes(rel, newtuple);

    /* Rebuild dependencies */
    GenerateTypeDependencies(typTup->typnamespace,
        domainoid,
        InvalidOid, /* typrelid is n/a */
        0,          /* relation kind is n/a */
        typTup->typowner,
        typTup->typinput,
        typTup->typoutput,
        typTup->typreceive,
        typTup->typsend,
        typTup->typmodin,
        typTup->typmodout,
        typTup->typanalyze,
        InvalidOid,
        false, /* a domain isn't an implicit array */
        typTup->typbasetype,
        typTup->typcollation,
        defaultExpr,
        true); /* Rebuild is true */

    ObjectAddressSet(address, TypeRelationId, domainoid);
    /* Clean up */
    heap_close(rel, NoLock);
    tableam_tops_free_tuple(newtuple);
    return address;
}

/*
 * AlterDomainNotNull
 *
 * Routine implementing ALTER DOMAIN SET/DROP NOT NULL statements.
 */
ObjectAddress AlterDomainNotNull(List* names, bool notNull)
{
    TypeName* typname = NULL;
    Oid domainoid;
    Relation typrel;
    HeapTuple tup;
    Form_pg_type typTup;
    ObjectAddress address = InvalidObjectAddress;

    /* Make a TypeName so we can use standard type lookup machinery */
    typname = makeTypeNameFromNameList(names);
    domainoid = typenameTypeId(NULL, typname);

    /* Look up the domain in the type table */
    typrel = heap_open(TypeRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(domainoid));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", domainoid)));
    typTup = (Form_pg_type)GETSTRUCT(tup);

    /* Check it's a domain and check user has permission for ALTER DOMAIN */
    checkDomainOwner(tup);

    /* Is the domain already set to the desired constraint? */
    if (typTup->typnotnull == notNull) {
        heap_close(typrel, RowExclusiveLock);
        tableam_tops_free_tuple(tup);
        return address;
    }

    /* Adding a NOT NULL constraint requires checking existing columns */
    if (notNull) {
        List* rels = NIL;
        ListCell* rt = NULL;

        /* Fetch relation list with attributes based on this domain */
        /* ShareLock is sufficient to prevent concurrent data changes */
        rels = get_rels_with_domain(domainoid, ShareLock);

        foreach (rt, rels) {
            RelToCheck* rtc = (RelToCheck*)lfirst(rt);
            Relation testrel = rtc->rel;
            TupleDesc tupdesc = RelationGetDescr(testrel);
            TableScanDesc scan;
            HeapTuple tuple;

            /* Scan all tuples in this relation */
            scan = tableam_scan_begin(testrel, SnapshotNow, 0, NULL);
            while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
                int i;

                /* Test attributes that are of the domain */
                for (i = 0; i < rtc->natts; i++) {
                    int attnum = rtc->atts[i];

                    /* replace heap_attisnull with relationAttIsNull
                     * due to altering table instantly
                     */
                    if (relationAttIsNull(tuple, attnum, tupdesc))
                        ereport(ERROR,
                            (errcode(ERRCODE_NOT_NULL_VIOLATION),
                                errmsg("column \"%s\" of table \"%s\" contains null values",
                                    NameStr(tupdesc->attrs[attnum - 1].attname),
                                    RelationGetRelationName(testrel))));
                }
            }
            tableam_scan_end(scan);

            /* Close each rel after processing, but keep lock */
            heap_close(testrel, NoLock);
        }
    }

    /*
     * Okay to update pg_type row.	We can scribble on typTup because it's a
     * copy.
     */
    typTup->typnotnull = notNull;

    simple_heap_update(typrel, &tup->t_self, tup);

    CatalogUpdateIndexes(typrel, tup);

    ObjectAddressSet(address, TypeRelationId, domainoid);

    /* Clean up */
    tableam_tops_free_tuple(tup);
    heap_close(typrel, RowExclusiveLock);
    return address;
}

/*
 * AlterDomainDropConstraint
 *
 * Implements the ALTER DOMAIN DROP CONSTRAINT statement
 */
ObjectAddress AlterDomainDropConstraint(List* names, const char* constrName, DropBehavior behavior, bool missing_ok)
{
    TypeName* typname = NULL;
    Oid domainoid;
    HeapTuple tup;
    Relation rel;
    Relation conrel;
    SysScanDesc conscan;
    ScanKeyData key[1];
    HeapTuple contup;
    bool found = false;
    ObjectAddress address = InvalidObjectAddress;

    /* Make a TypeName so we can use standard type lookup machinery */
    typname = makeTypeNameFromNameList(names);
    domainoid = typenameTypeId(NULL, typname);

    /* Look up the domain in the type table */
    rel = heap_open(TypeRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(domainoid));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", domainoid)));

    /* Check it's a domain and check user has permission for ALTER DOMAIN */
    checkDomainOwner(tup);

    /* Grab an appropriate lock on the pg_constraint relation */
    conrel = heap_open(ConstraintRelationId, RowExclusiveLock);

    /* Use the index to scan only constraints of the target relation */
    ScanKeyInit(
        &key[0], Anum_pg_constraint_contypid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(HeapTupleGetOid(tup)));

    conscan = systable_beginscan(conrel, ConstraintTypidIndexId, true, NULL, 1, key);

    /*
     * Scan over the result set, removing any matching entries.
     */
    while ((contup = systable_getnext(conscan)) != NULL) {
        Form_pg_constraint con = (Form_pg_constraint)GETSTRUCT(contup);

        if (strcmp(NameStr(con->conname), constrName) == 0) {
            ObjectAddress conobj;

            conobj.classId = ConstraintRelationId;
            conobj.objectId = HeapTupleGetOid(contup);
            conobj.objectSubId = 0;

            performDeletion(&conobj, behavior, 0);
            found = true;
        }
    }
    ObjectAddressSet(address, TypeRelationId, domainoid);
    /* Clean up after the scan */
    systable_endscan(conscan);
    heap_close(conrel, RowExclusiveLock);

    heap_close(rel, NoLock);

    if (!found) {
        if (!missing_ok)
            ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg(
                        "constraint \"%s\" of domain \"%s\" does not exist", constrName, TypeNameToString(typname))));
        else
            ereport(NOTICE,
                (errmsg("constraint \"%s\" of domain \"%s\" does not exist, skipping",
                    constrName,
                    TypeNameToString(typname))));
    }
    return address;
}

/*
 * AlterDomainAddConstraint
 *
 * Implements the ALTER DOMAIN .. ADD CONSTRAINT statement.
 */
ObjectAddress AlterDomainAddConstraint(List* names, Node* newConstraint)
{
    TypeName* typname = NULL;
    Oid domainoid;
    Relation typrel;
    HeapTuple tup;
    Form_pg_type typTup;
    Constraint* constr = NULL;
    char* ccbin = NULL;
    ObjectAddress address = InvalidObjectAddress;

    /* Make a TypeName so we can use standard type lookup machinery */
    typname = makeTypeNameFromNameList(names);
    domainoid = typenameTypeId(NULL, typname);

    /* Look up the domain in the type table */
    typrel = heap_open(TypeRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(domainoid));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", domainoid)));
    typTup = (Form_pg_type)GETSTRUCT(tup);

    /* Check it's a domain and check user has permission for ALTER DOMAIN */
    checkDomainOwner(tup);

    if (!IsA(newConstraint, Constraint))
        ereport(ERROR,
            (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                errmsg("unrecognized node type: %d", (int)nodeTag(newConstraint))));

    constr = (Constraint*)newConstraint;

    switch (constr->contype) {
        case CONSTR_CHECK:
            /* processed below */
            break;

        case CONSTR_UNIQUE:
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("unique constraints not possible for domains")));
            break;

        case CONSTR_PRIMARY:
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("primary key constraints not possible for domains")));
            break;

        case CONSTR_EXCLUSION:
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("exclusion constraints not possible for domains")));
            break;

        case CONSTR_FOREIGN:
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("foreign key constraints not possible for domains")));
            break;

        case CONSTR_ATTR_DEFERRABLE:
        case CONSTR_ATTR_NOT_DEFERRABLE:
        case CONSTR_ATTR_DEFERRED:
        case CONSTR_ATTR_IMMEDIATE:
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                    errmsg("specifying constraint deferrability not supported for domains")));
            break;

        default:
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unrecognized constraint subtype: %d", (int)constr->contype)));
            break;
    }

    /*
     * Since all other constraint types throw errors, this must be a check
     * constraint.	First, process the constraint expression and add an entry
     * to pg_constraint.
     */
    ccbin = domainAddConstraint(HeapTupleGetOid(tup),
        typTup->typnamespace,
        typTup->typbasetype,
        typTup->typtypmod,
        constr,
        NameStr(typTup->typname),
        NULL);

    /*
     * If requested to validate the constraint, test all values stored in the
     * attributes based on the domain the constraint is being added to.
     */
    if (!constr->skip_validation)
        validateDomainConstraint(domainoid, ccbin);

    ObjectAddressSet(address, TypeRelationId, domainoid);
    /* Clean up */
    heap_close(typrel, RowExclusiveLock);
    return address;
}

/*
 * AlterDomainValidateConstraint
 *
 * Implements the ALTER DOMAIN .. VALIDATE CONSTRAINT statement.
 */
ObjectAddress AlterDomainValidateConstraint(List* names, char* constrName)
{
    TypeName* typname = NULL;
    Oid domainoid;
    Relation typrel;
    Relation conrel;
    HeapTuple tup;
    Form_pg_constraint con = NULL;
    Form_pg_constraint copy_con;
    char* conbin = NULL;
    SysScanDesc scan;
    Datum val;
    bool found = false;
    bool isnull = false;
    HeapTuple tuple;
    HeapTuple copyTuple;
    ScanKeyData key;
    ObjectAddress address;

    /* Make a TypeName so we can use standard type lookup machinery */
    typname = makeTypeNameFromNameList(names);
    domainoid = typenameTypeId(NULL, typname);

    /* Look up the domain in the type table */
    typrel = heap_open(TypeRelationId, AccessShareLock);

    tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(domainoid));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", domainoid)));

    /* Check it's a domain and check user has permission for ALTER DOMAIN */
    checkDomainOwner(tup);

    /*
     * Find and check the target constraint
     */
    conrel = heap_open(ConstraintRelationId, RowExclusiveLock);
    ScanKeyInit(&key, Anum_pg_constraint_contypid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(domainoid));
    scan = systable_beginscan(conrel, ConstraintTypidIndexId, true, NULL, 1, &key);

    while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
        con = (Form_pg_constraint)GETSTRUCT(tuple);
        if (strcmp(NameStr(con->conname), constrName) == 0) {
            found = true;
            break;
        }
    }

    if (!found || con == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT),
                errmsg("constraint \"%s\" of domain \"%s\" does not exist", constrName, TypeNameToString(typname))));

    if (con->contype != CONSTRAINT_CHECK)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("constraint \"%s\" of domain \"%s\" is not a check constraint",
                    constrName,
                    TypeNameToString(typname))));

    val = SysCacheGetAttr(CONSTROID, tuple, Anum_pg_constraint_conbin, &isnull);
    if (isnull)
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("null conbin for constraint %u", HeapTupleGetOid(tuple))));
    conbin = TextDatumGetCString(val);

    validateDomainConstraint(domainoid, conbin);

    /*
     * Now update the catalog, while we have the door open.
     */
    copyTuple = (HeapTuple)tableam_tops_copy_tuple(tuple);
    copy_con = (Form_pg_constraint)GETSTRUCT(copyTuple);
    copy_con->convalidated = true;
    simple_heap_update(conrel, &copyTuple->t_self, copyTuple);
    CatalogUpdateIndexes(conrel, copyTuple);
    tableam_tops_free_tuple(copyTuple);

    ObjectAddressSet(address, TypeRelationId, domainoid);
    systable_endscan(scan);

    heap_close(typrel, AccessShareLock);
    heap_close(conrel, RowExclusiveLock);

    ReleaseSysCache(tup);
    return address;
}

static void validateDomainConstraint(Oid domainoid, char* ccbin)
{
    Expr* expr = (Expr*)stringToNode(ccbin);
    List* rels = NIL;
    ListCell* rt = NULL;
    EState* estate = NULL;
    ExprContext* econtext = NULL;
    ExprState* exprstate = NULL;

    /* Need an EState to run ExecEvalExpr */
    estate = CreateExecutorState();
    econtext = GetPerTupleExprContext(estate);

    /* build execution state for expr */
    exprstate = ExecPrepareExpr(expr, estate);

    /* Fetch relation list with attributes based on this domain */
    /* ShareLock is sufficient to prevent concurrent data changes */
    rels = get_rels_with_domain(domainoid, ShareLock);

    foreach (rt, rels) {
        RelToCheck* rtc = (RelToCheck*)lfirst(rt);
        Relation testrel = rtc->rel;
        TupleDesc tupdesc = RelationGetDescr(testrel);
        TableScanDesc scan;
        HeapTuple tuple;

        /* Scan all tuples in this relation */
        scan = tableam_scan_begin(testrel, SnapshotNow, 0, NULL);
        while ((tuple = (HeapTuple) tableam_scan_getnexttuple(scan, ForwardScanDirection)) != NULL) {
            int i;

            /* Test attributes that are of the domain */
            for (i = 0; i < rtc->natts; i++) {
                int attnum = rtc->atts[i];
                Datum d;
                bool isNull = false;
                Datum conResult;

                d = heap_getattr(tuple, (unsigned int)attnum, tupdesc, &isNull);

                econtext->domainValue_datum = d;
                econtext->domainValue_isNull = isNull;

                conResult = ExecEvalExprSwitchContext(exprstate, econtext, &isNull);

                if (!isNull && !DatumGetBool(conResult))
                    ereport(ERROR,
                        (errcode(ERRCODE_CHECK_VIOLATION),
                            errmsg("column \"%s\" of table \"%s\" contains values that violate the new constraint",
                                NameStr(tupdesc->attrs[attnum - 1].attname),
                                RelationGetRelationName(testrel))));
            }

            ResetExprContext(econtext);
        }
        tableam_scan_end(scan);

        /* Hold relation lock till commit (XXX bad for concurrency) */
        heap_close(testrel, NoLock);
    }

    FreeExecutorState(estate);
}

/*
 * get_rels_with_domain
 *
 * Fetch all relations / attributes which are using the domain
 *
 * The result is a list of RelToCheck structs, one for each distinct
 * relation, each containing one or more attribute numbers that are of
 * the domain type.  We have opened each rel and acquired the specified lock
 * type on it.
 *
 * We support nested domains by including attributes that are of derived
 * domain types.  Current callers do not need to distinguish between attributes
 * that are of exactly the given domain and those that are of derived domains.
 *
 * XXX this is completely broken because there is no way to lock the domain
 * to prevent columns from being added or dropped while our command runs.
 * We can partially protect against column drops by locking relations as we
 * come across them, but there is still a race condition (the window between
 * seeing a pg_depend entry and acquiring lock on the relation it references).
 * Also, holding locks on all these relations simultaneously creates a non-
 * trivial risk of deadlock.  We can minimize but not eliminate the deadlock
 * risk by using the weakest suitable lock (ShareLock for most callers).
 *
 * XXX the API for this is not sufficient to support checking domain values
 * that are inside composite types or arrays.  Currently we just error out
 * if a composite type containing the target domain is stored anywhere.
 * There are not currently arrays of domains; if there were, we could take
 * the same approach, but it'd be nicer to fix it properly.
 *
 * Generally used for retrieving a list of tests when adding
 * new constraints to a domain.
 */
static List* get_rels_with_domain(Oid domainOid, LOCKMODE lockmode)
{
    List* result = NIL;
    Relation depRel;
    ScanKeyData key[2];
    SysScanDesc depScan;
    HeapTuple depTup;

    Assert(lockmode != NoLock);

    /*
     * We scan pg_depend to find those things that depend on the domain. (We
     * assume we can ignore refobjsubid for a domain.)
     */
    depRel = heap_open(DependRelationId, AccessShareLock);

    ScanKeyInit(&key[0], Anum_pg_depend_refclassid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(TypeRelationId));
    ScanKeyInit(&key[1], Anum_pg_depend_refobjid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(domainOid));

    depScan = systable_beginscan(depRel, DependReferenceIndexId, true, NULL, 2, key);

    while (HeapTupleIsValid(depTup = systable_getnext(depScan))) {
        Form_pg_depend pg_depend = (Form_pg_depend)GETSTRUCT(depTup);
        RelToCheck* rtc = NULL;
        ListCell* rellist = NULL;
        Form_pg_attribute pg_att;
        int ptr;

        /* Check for directly dependent types --- must be domains */
        if (pg_depend->classid == TypeRelationId) {
            Assert(get_typtype(pg_depend->objid) == TYPTYPE_DOMAIN);

            /*
             * Recursively add dependent columns to the output list.  This is
             * a bit inefficient since we may fail to combine RelToCheck
             * entries when attributes of the same rel have different derived
             * domain types, but it's probably not worth improving.
             */
            result = list_concat(result, get_rels_with_domain(pg_depend->objid, lockmode));
            continue;
        }

        /* Else, ignore dependees that aren't user columns of relations */
        /* (we assume system columns are never of domain types) */
        if (pg_depend->classid != RelationRelationId || pg_depend->objsubid <= 0)
            continue;

        /* See if we already have an entry for this relation */
        foreach (rellist, result) {
            RelToCheck* rt = (RelToCheck*)lfirst(rellist);

            if (RelationGetRelid(rt->rel) == pg_depend->objid) {
                rtc = rt;
                break;
            }
        }

        if (rtc == NULL) {
            /* First attribute found for this relation */
            Relation rel;

            /* Acquire requested lock on relation */
            rel = relation_open(pg_depend->objid, lockmode);

            /*
             * Check to see if rowtype is stored anyplace as a composite-type
             * column; if so we have to fail, for now anyway.
             */
            if (OidIsValid(rel->rd_rel->reltype))
                find_composite_type_dependencies(rel->rd_rel->reltype, NULL, format_type_be(domainOid));

            /* Otherwise we can ignore views, composite types, etc */
            if (rel->rd_rel->relkind != RELKIND_RELATION &&
                rel->rd_rel->relkind != RELKIND_MATVIEW) {
                relation_close(rel, lockmode);
                continue;
            }

            /* Build the RelToCheck entry with enough space for all atts */
            rtc = (RelToCheck*)palloc(sizeof(RelToCheck));
            rtc->rel = rel;
            rtc->natts = 0;
            rtc->atts = (int*)palloc(sizeof(int) * RelationGetNumberOfAttributes(rel));
            result = lcons(rtc, result);
        }

        /*
         * Confirm column has not been dropped, and is of the expected type.
         * This defends against an ALTER DROP COLUMN occurring just before we
         * acquired lock ... but if the whole table were dropped, we'd still
         * have a problem.
         */
        if (pg_depend->objsubid > RelationGetNumberOfAttributes(rtc->rel))
            continue;
        pg_att = &rtc->rel->rd_att->attrs[pg_depend->objsubid - 1];
        if (pg_att->attisdropped || pg_att->atttypid != domainOid)
            continue;

        /*
         * Okay, add column to result.	We store the columns in column-number
         * order; this is just a hack to improve predictability of regression
         * test output ...
         */
        Assert(rtc->natts < RelationGetNumberOfAttributes(rtc->rel));

        ptr = rtc->natts++;
        while (ptr > 0 && rtc->atts[ptr - 1] > pg_depend->objsubid) {
            rtc->atts[ptr] = rtc->atts[ptr - 1];
            ptr--;
        }
        rtc->atts[ptr] = pg_depend->objsubid;
    }

    systable_endscan(depScan);

    relation_close(depRel, AccessShareLock);

    return result;
}

/*
 * checkDomainOwner
 *
 * Check that the type is actually a domain and that the current user
 * has permission to do ALTER DOMAIN on it.  Throw an error if not.
 */
void checkDomainOwner(HeapTuple tup)
{
    Form_pg_type typTup = (Form_pg_type)GETSTRUCT(tup);

    /* Check that this is actually a domain */
    if (typTup->typtype != TYPTYPE_DOMAIN)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("%s is not a domain", format_type_be(HeapTupleGetOid(tup)))));

    /* Permission check: must own type */
    if (!pg_type_ownercheck(HeapTupleGetOid(tup), GetUserId()))
        aclcheck_error_type(ACLCHECK_NOT_OWNER, HeapTupleGetOid(tup));
}

/*
 * domainAddConstraint - code shared between CREATE and ALTER DOMAIN
 */
static char* domainAddConstraint(
    Oid domainOid, Oid domainNamespace, Oid baseTypeOid, int typMod, Constraint* constr, char* domainName, ObjectAddress *constrAddr)
{
    Node* expr = NULL;
    char* ccsrc = NULL;
    char* ccbin = NULL;
    ParseState* pstate = NULL;
    CoerceToDomainValue* domVal = NULL;
    Oid     ccoid;

    /*
     * Assign or validate constraint name
     */
    if (constr->conname) {
        if (ConstraintNameIsUsed(CONSTRAINT_DOMAIN, domainOid, domainNamespace, constr->conname))
            ereport(ERROR,
                (errcode(ERRCODE_DUPLICATE_OBJECT),
                    errmsg("constraint \"%s\" for domain \"%s\" already exists", constr->conname, domainName)));
    } else
        constr->conname = ChooseConstraintName(domainName, NULL, "check", domainNamespace, NIL);

    /*
     * Convert the A_EXPR in raw_expr into an EXPR
     */
    pstate = make_parsestate(NULL);

    /*
     * Set up a CoerceToDomainValue to represent the occurrence of VALUE in
     * the expression.	Note that it will appear to have the type of the base
     * type, not the domain.  This seems correct since within the check
     * expression, we should not assume the input value can be considered a
     * member of the domain.
     */
    domVal = makeNode(CoerceToDomainValue);
    domVal->typeId = baseTypeOid;
    domVal->typeMod = typMod;
    domVal->collation = get_typcollation(baseTypeOid);
    domVal->location = -1; /* will be set when/if used */

    pstate->p_value_substitute = (Node*)domVal;

    expr = transformExpr(pstate, constr->raw_expr, EXPR_KIND_DOMAIN_CHECK);

    /*
     * Make sure it yields a boolean result.
     */
    expr = coerce_to_boolean(pstate, expr, "CHECK");

    /*
     * Fix up collation information.
     */
    assign_expr_collations(pstate, expr);

    /*
     * Make sure no outside relations are referred to.
     */
    if (list_length(pstate->p_rtable) != 0)
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                errmsg("cannot use table references in domain check constraint")));

    /*
     * Domains don't allow var clauses (this should be redundant with the
     * above check, but make it anyway)
     */
    if (contain_var_clause(expr))
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
                errmsg("cannot use table references in domain check constraint")));

    /*
     * No subplans or aggregates, either...
     */
    if (pstate->p_hasSubLinks)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("cannot use subquery in check constraint")));
    if (pstate->p_hasAggs)
        ereport(ERROR, (errcode(ERRCODE_GROUPING_ERROR), errmsg("cannot use aggregate function in check constraint")));
    if (pstate->p_hasWindowFuncs)
        ereport(ERROR, (errcode(ERRCODE_WINDOWING_ERROR), errmsg("cannot use window function in check constraint")));

    /*
     * Convert to string form for storage.
     */
    ccbin = nodeToString(expr);

    /*
     * Deparse it to produce text for consrc.
     *
     * Since VARNOs aren't allowed in domain constraints, relation context
     * isn't required as anything other than a shell.
     */
    ccsrc = deparse_expression(expr, deparse_context_for(domainName, InvalidOid), false, false);

    /*
     * Store the constraint in pg_constraint
     */
   ccoid = CreateConstraintEntry(constr->conname, /* Constraint Name */
        domainNamespace,                   /* namespace */
        CONSTRAINT_CHECK,                  /* Constraint Type */
        false,                             /* Is Deferrable */
        false,                             /* Is Deferred */
        !constr->skip_validation,          /* Is Validated */
        InvalidOid,                        /* not a relation constraint */
        NULL,
        0,
        0,
        domainOid,  /* domain constraint */
        InvalidOid, /* no associated index */
        InvalidOid, /* Foreign key fields */
        NULL,
        NULL,
        NULL,
        NULL,
        0,
        ' ',
        ' ',
        ' ',
        NULL,  /* not an exclusion constraint */
        expr,  /* Tree form of check constraint */
        ccbin, /* Binary form of check constraint */
        ccsrc, /* Source form of check constraint */
        true,  /* is local */
        0,     /* inhcount */
        false, /* connoinherit */
        constr->inforConstraint) /* @hdfs, informational constraint information */;

    if (constrAddr)
        ObjectAddressSet(*constrAddr, ConstraintRelationId, ccoid);
    /*
     * Return the compiled constraint expression so the calling routine can
     * perform any additional required tests.
     */
    return ccbin;
}

/*
 * GetDomainConstraints - get a list of the current constraints of domain
 *
 * Returns a possibly-empty list of DomainConstraintState nodes.
 *
 * This is called by the executor during plan startup for a CoerceToDomain
 * expression node.  The given constraints will be checked for each value
 * passed through the node.
 *
 * We allow this to be called for non-domain types, in which case the result
 * is always NIL.
 */
List* GetDomainConstraints(Oid typeOid)
{
    List* result = NIL;
    bool notNull = false;
    Relation conRel;

    conRel = heap_open(ConstraintRelationId, AccessShareLock);

    for (;;) {
        HeapTuple tup;
        HeapTuple conTup;
        Form_pg_type typTup;
        ScanKeyData key[1];
        SysScanDesc scan;

        tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typeOid));
        if (!HeapTupleIsValid(tup))
            ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", typeOid)));
        typTup = (Form_pg_type)GETSTRUCT(tup);

        if (typTup->typtype != TYPTYPE_DOMAIN) {
            /* Not a domain, so done */
            ReleaseSysCache(tup);
            break;
        }

        /* Test for NOT NULL Constraint */
        if (typTup->typnotnull)
            notNull = true;

        /* Look for CHECK Constraints on this domain */
        ScanKeyInit(&key[0], Anum_pg_constraint_contypid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(typeOid));

        scan = systable_beginscan(conRel, ConstraintTypidIndexId, true, NULL, 1, key);

        while (HeapTupleIsValid(conTup = systable_getnext(scan))) {
            Form_pg_constraint c = (Form_pg_constraint)GETSTRUCT(conTup);
            Datum val;
            bool isNull = false;
            Expr* check_expr = NULL;
            DomainConstraintState* r = NULL;

            /* Ignore non-CHECK constraints (presently, shouldn't be any) */
            if (c->contype != CONSTRAINT_CHECK)
                continue;

            /*
             * Not expecting conbin to be NULL, but we'll test for it anyway
             */
            val = fastgetattr(conTup, Anum_pg_constraint_conbin, conRel->rd_att, &isNull);
            if (isNull)
                ereport(ERROR,
                    (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        errmsg("domain \"%s\" constraint \"%s\" has NULL conbin",
                            NameStr(typTup->typname),
                            NameStr(c->conname))));

            check_expr = (Expr*)stringToNode(TextDatumGetCString(val));

            /* ExecInitExpr assumes we've planned the expression */
            check_expr = expression_planner(check_expr);

            r = makeNode(DomainConstraintState);
            r->check_node = check_expr; /* for flattened expression frame */

            r->constrainttype = DOM_CONSTRAINT_CHECK;
            r->name = pstrdup(NameStr(c->conname));
            r->check_expr = ExecInitExpr(check_expr, NULL);

            /*
             * use lcons() here because constraints of lower domains should be
             * applied earlier.
             */
            result = lcons(r, result);
        }

        systable_endscan(scan);

        /* loop to next domain in stack */
        typeOid = typTup->typbasetype;
        ReleaseSysCache(tup);
    }

    heap_close(conRel, AccessShareLock);

    /*
     * Only need to add one NOT NULL check regardless of how many domains in
     * the stack request it.
     */
    if (notNull) {
        DomainConstraintState* r = makeNode(DomainConstraintState);

        r->constrainttype = DOM_CONSTRAINT_NOTNULL;
        r->name = pstrdup("NOT NULL");
        r->check_expr = NULL;
        r->check_node = NULL;

        /* lcons to apply the nullness check FIRST */
        result = lcons(r, result);
    }

    return result;
}

/*
 * Execute ALTER TYPE RENAME
 */
ObjectAddress RenameType(RenameStmt* stmt)
{
    List* names = stmt->object;
    const char* newTypeName = stmt->newname;
    TypeName* typname = NULL;
    Oid typeOid;
    Relation rel;
    HeapTuple tup;
    Form_pg_type typTup;
    ObjectAddress address;

    /* Make a TypeName so we can use standard type lookup machinery */
    typname = makeTypeNameFromNameList(names);
    typeOid = typenameTypeId(NULL, typname);

    /* Look up the type in the type table */
    rel = heap_open(TypeRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(typeOid));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", typeOid)));
    typTup = (Form_pg_type)GETSTRUCT(tup);

    TrForbidAccessRbObject(TypeRelationId, typeOid, NameStr(typTup->typname));

    /* check permissions on type */
    AclResult aclresult = pg_type_aclcheck(typeOid, GetUserId(), ACL_ALTER);
    if (aclresult != ACLCHECK_OK && !pg_type_ownercheck(typeOid, GetUserId())) {
        aclcheck_error_type(ACLCHECK_NO_PRIV, typeOid);
    }

    /* ALTER DOMAIN used on a non-domain? */
    if (stmt->renameType == OBJECT_DOMAIN && typTup->typtype != TYPTYPE_DOMAIN)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("\"%s\" is not a domain", format_type_be(typeOid))));

    /*
     * If it's a composite type, we need to check that it really is a
     * free-standing composite type, and not a table's rowtype. We want people
     * to use ALTER TABLE not ALTER TYPE for that case.
     */
    if (typTup->typtype == TYPTYPE_COMPOSITE && get_rel_relkind(typTup->typrelid) != RELKIND_COMPOSITE_TYPE)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("%s is a table's row type", format_type_be(typeOid)),
                errhint("Use ALTER TABLE instead.")));

    /* don't allow direct alteration of array types, either */
    if (OidIsValid(typTup->typelem) && get_array_type(typTup->typelem) == typeOid)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot alter array type %s", format_type_be(typeOid)),
                errhint("You can alter type %s, which will alter the array type as well.",
                    format_type_be(typTup->typelem))));

#ifndef ENABLE_MULTIPLE_NODES
    /* don't allow to alter package or procedure type */
    if (IsPackageDependType(typeTypeId(tup), InvalidOid)) {
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmodule(MOD_PLSQL),
                errmsg("Not allowed to alter type \"%s\"", TypeNameToString(typname)),
                errdetail("\"%s\" is a package or procedure type", TypeNameToString(typname)),
                errcause("feature not supported"),
                erraction("check type name")));
    }
#endif

    if (enable_plpgsql_gsdependency_guc() &&
        gsplsql_is_object_depend(typeOid, GSDEPEND_OBJECT_TYPE_TYPE)) {
        ereport(ERROR,
            (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                errmsg("The rename operator of %s is not allowed, because it is referenced by the other object.",
                    TypeNameToString(typname))));
    }

    /*
     * If type is composite we need to rename associated pg_class entry too.
     * RenameRelationInternal will call RenameTypeInternal automatically.
     */
    if (typTup->typtype == TYPTYPE_COMPOSITE)
        RenameRelationInternal(typTup->typrelid, newTypeName);
    else
        RenameTypeInternal(typeOid, newTypeName, typTup->typnamespace);

    ObjectAddressSet(address, TypeRelationId, typeOid);
    /* Clean up */
    heap_close(rel, RowExclusiveLock);
    return address;
}

/*
 * Change the owner of a type.
 */
ObjectAddress AlterTypeOwner(List* names, Oid newOwnerId, ObjectType objecttype, bool altertype)
{
    TypeName* typname = NULL;
    Oid typeOid;
    Relation rel;
    HeapTuple tup;
    HeapTuple newtup;
    Form_pg_type typTup;
    AclResult aclresult;
    ObjectAddress address;

    rel = heap_open(TypeRelationId, RowExclusiveLock);

    /* Make a TypeName so we can use standard type lookup machinery */
    typname = makeTypeNameFromNameList(names);

    /* Use LookupTypeName here so that shell types can be processed */
    tup = LookupTypeName(NULL, typname, NULL);
    if (tup == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("type \"%s\" does not exist", TypeNameToString(typname))));

    typeOid = typeTypeId(tup);
    if (altertype && type_is_set(typeOid)) {
        ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("not support to change owner of set type")));
    }

    TrForbidAccessRbObject(TypeRelationId, typeOid, NameStr(((Form_pg_type)GETSTRUCT(tup))->typname));

    /* Copy the syscache entry so we can scribble on it below */
    newtup = (HeapTuple)tableam_tops_copy_tuple(tup);
    ReleaseSysCache(tup);
    tup = newtup;
    typTup = (Form_pg_type)GETSTRUCT(tup);

    /* Don't allow ALTER DOMAIN on a type */
    if (objecttype == OBJECT_DOMAIN && typTup->typtype != TYPTYPE_DOMAIN)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("%s is not a domain", format_type_be(typeOid))));

    /*
     * If it's a composite type, we need to check that it really is a
     * free-standing composite type, and not a table's rowtype. We want people
     * to use ALTER TABLE not ALTER TYPE for that case.
     */
    if (typTup->typtype == TYPTYPE_COMPOSITE && get_rel_relkind(typTup->typrelid) != RELKIND_COMPOSITE_TYPE)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("%s is a table's row type", format_type_be(typeOid)),
                errhint("Use ALTER TABLE instead.")));

    /* don't allow direct alteration of array types, either */
    if (OidIsValid(typTup->typelem) && get_array_type(typTup->typelem) == typeOid)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot alter array type %s", format_type_be(typeOid)),
                errhint("You can alter type %s, which will alter the array type as well.",
                    format_type_be(typTup->typelem))));

#ifndef ENABLE_MULTIPLE_NODES
    /* don't allow to alter package or procedure type */
    if (IsPackageDependType(typeTypeId(tup), InvalidOid)) {
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmodule(MOD_PLSQL),
                errmsg("Not allowed to alter type \"%s\"", TypeNameToString(typname)),
                errdetail("\"%s\" is a package or procedure type", TypeNameToString(typname)),
                errcause("feature not supported"),
                erraction("check type name")));
    }
#endif

    /*
     * If the new owner is the same as the existing owner, consider the
     * command to have succeeded.  This is for dump restoration purposes.
     */
    if (typTup->typowner != newOwnerId) {
        /* Superusers can always do it */
        if (!superuser()) {
            /* Otherwise, must be owner of the existing object */
            if (!pg_type_ownercheck(HeapTupleGetOid(tup), GetUserId()))
                aclcheck_error_type(ACLCHECK_NOT_OWNER, HeapTupleGetOid(tup));

            /* Must be able to become new owner */
            check_is_member_of_role(GetUserId(), newOwnerId);

            /* New owner must have CREATE privilege on namespace */
            aclresult = pg_namespace_aclcheck(typTup->typnamespace, newOwnerId, ACL_CREATE);
            if (aclresult != ACLCHECK_OK)
                aclcheck_error(aclresult, ACL_KIND_NAMESPACE, get_namespace_name(typTup->typnamespace));
        }

        /*
         * If it's a composite type, invoke ATExecChangeOwner so that we fix
         * up the pg_class entry properly.	That will call back to
         * AlterTypeOwnerInternal to take care of the pg_type entry(s).
         */
        if (typTup->typtype == TYPTYPE_COMPOSITE)
            ATExecChangeOwner(typTup->typrelid, newOwnerId, true, AccessExclusiveLock);
        else {
            /*
             * We can just apply the modification directly.
             *
             * okay to scribble on typTup because it's a copy
             */
            typTup->typowner = newOwnerId;

            simple_heap_update(rel, &tup->t_self, tup);

            CatalogUpdateIndexes(rel, tup);

            /* Update owner dependency reference */
            changeDependencyOnOwner(TypeRelationId, typeOid, newOwnerId);

            /* If it has an array type, update that too */
            if (OidIsValid(typTup->typarray))
                AlterTypeOwnerInternal(typTup->typarray, newOwnerId, false);
        }
    }

    ObjectAddressSet(address, TypeRelationId, typeOid);
    /* Clean up */
    heap_close(rel, RowExclusiveLock);
    return address;
}

/*
 * AlterTypeOwnerInternal - change type owner unconditionally
 *
 * This is currently only used to propagate ALTER TABLE/TYPE OWNER to a
 * table's rowtype or an array type, and to implement REASSIGN OWNED BY.
 * It assumes the caller has done all needed checks.  The function will
 * automatically recurse to an array type if the type has one.
 *
 * hasDependEntry should be TRUE if type is expected to have a pg_shdepend
 * entry (ie, it's not a table rowtype nor an array type).
 */
void AlterTypeOwnerInternal(Oid typeOid, Oid newOwnerId, bool hasDependEntry)
{
    Relation rel;
    HeapTuple tup;
    Form_pg_type typTup;

    rel = heap_open(TypeRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(typeOid));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", typeOid)));
    typTup = (Form_pg_type)GETSTRUCT(tup);

    /*
     * Modify the owner --- okay to scribble on typTup because it's a copy
     */
    typTup->typowner = newOwnerId;

    simple_heap_update(rel, &tup->t_self, tup);

    CatalogUpdateIndexes(rel, tup);

    /* Update owner dependency reference, if it has one */
    if (hasDependEntry)
        changeDependencyOnOwner(TypeRelationId, typeOid, newOwnerId);

    /* If it has an array type, update that too */
    if (OidIsValid(typTup->typarray))
        AlterTypeOwnerInternal(typTup->typarray, newOwnerId, false);

    /* Clean up */
    heap_close(rel, RowExclusiveLock);
}

/*
 * AlterTypeOwnerByPkg - change package type owner
 *
 * This is currently only used to propagate ALTER PACKAGE OWNER to a
 * package. Package will build types, and add to pg_type.
 * It assumes the caller has done all needed checks.
 */
void AlterTypeOwnerByPkg(Oid pkgOid, Oid newOwnerId)
{
    if (!OidIsValid(pkgOid)) {
        return;
    }

    Relation depRel;
    ScanKeyData key[2];
    SysScanDesc scan;
    HeapTuple tup;
    const int keyNumber = 2;
    bool isPkgDepTyp = false;
    Form_pg_depend depTuple = NULL;

    depRel = heap_open(DependRelationId, RowExclusiveLock);
    ScanKeyInit(&key[0], Anum_pg_depend_refclassid,
        BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(PackageRelationId));
    ScanKeyInit(&key[1], Anum_pg_depend_refobjid,
        BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(pkgOid));
    scan = systable_beginscan(depRel, DependReferenceIndexId, true, NULL, keyNumber, key);

    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        depTuple = (Form_pg_depend)GETSTRUCT(tup);
        isPkgDepTyp = (depTuple->deptype == DEPENDENCY_AUTO) &&
            (depTuple->classid == TypeRelationId || depTuple->classid == PgSynonymRelationId);
        if (!isPkgDepTyp) {
            continue;
        }
        if (depTuple->classid == TypeRelationId) {
            AlterTypeOwnerInternal(depTuple->objid, newOwnerId, false);
        } else {
            AlterSynonymOwnerByOid(depTuple->objid, newOwnerId);
        }
    }

    systable_endscan(scan);
    heap_close(depRel, RowExclusiveLock);
    return;
}

/*
 * AlterTypeOwnerByFunc - change func type owner
 *
 * This is currently only used to propagate ALTER FUNCTION OWNER
 * Procedure will build types when the type is nested, and add to pg_type.
 * It assumes the caller has done all needed checks.
 */
void AlterTypeOwnerByFunc(Oid funcOid, Oid newOwnerId)
{
#ifdef ENABLE_MULTIPLE_NODES
    /* procedure type will only be build in centralized mode */
    return;
#endif
    if (!OidIsValid(funcOid)) {
        return;
    }

    Relation depRel;
    ScanKeyData key[2];
    SysScanDesc scan;
    HeapTuple tup;
    const int keyNumber = 2;
    bool isFuncDepTyp = false;
    Form_pg_depend depTuple = NULL;

    depRel = heap_open(DependRelationId, RowExclusiveLock);
    ScanKeyInit(&key[0], Anum_pg_depend_refclassid,
        BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(ProcedureRelationId));
    ScanKeyInit(&key[1], Anum_pg_depend_refobjid,
        BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(funcOid));
    scan = systable_beginscan(depRel, DependReferenceIndexId, true, NULL, keyNumber, key);

    while (HeapTupleIsValid(tup = systable_getnext(scan))) {
        depTuple = (Form_pg_depend)GETSTRUCT(tup);
        isFuncDepTyp = (depTuple->deptype == DEPENDENCY_AUTO) && depTuple->classid == TypeRelationId;
        if (!isFuncDepTyp) {
            continue;
        }
        AlterTypeOwnerInternal(depTuple->objid, newOwnerId, false);
    }

    systable_endscan(scan);
    heap_close(depRel, RowExclusiveLock);
    return;
}

/*
 * Execute ALTER TYPE SET SCHEMA
 */
ObjectAddress AlterTypeNamespace(List* names, const char* newschema, ObjectType objecttype)
{
    TypeName* typname = NULL;
    Oid typeOid;
    Oid nspOid;
    Oid oldNspOid;
    ObjectAddresses* objsMoved = NULL;
    ObjectAddress myself;

    /* Make a TypeName so we can use standard type lookup machinery */
    typname = makeTypeNameFromNameList(names);
    typeOid = typenameTypeId(NULL, typname);

#ifndef ENABLE_MULTIPLE_NODES
    /* don't allow to alter package or procedure type */
    if (IsPackageDependType(typeOid, InvalidOid)) {
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmodule(MOD_PLSQL),
                errmsg("Not allowed to alter type \"%s\"", TypeNameToString(typname)),
                errdetail("\"%s\" is a package or procedure type", TypeNameToString(typname)),
                errcause("feature not supported"),
                erraction("check type name")));
    }
#endif
    if (type_is_set(typeOid)) {
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("not support to change schema of set type")));
    }

    TrForbidAccessRbObject(TypeRelationId, typeOid);

    /* Don't allow ALTER DOMAIN on a type */
    if (objecttype == OBJECT_DOMAIN && get_typtype(typeOid) != TYPTYPE_DOMAIN)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("%s is not a domain", format_type_be(typeOid))));

    /* get schema OID and check its permissions */
    nspOid = LookupCreationNamespace(newschema);

    objsMoved = new_object_addresses();
    oldNspOid = AlterTypeNamespace_oid(typeOid, nspOid, objsMoved);
    free_object_addresses(objsMoved);
    
    ObjectAddressSet(myself, TypeRelationId, typeOid);
    
    return myself;
}

Oid AlterTypeNamespace_oid(Oid typeOid, Oid nspOid, ObjectAddresses* objsMoved)
{
    Oid elemOid;

    /* check permissions on type */
    if (!pg_type_ownercheck(typeOid, GetUserId()))
        aclcheck_error_type(ACLCHECK_NOT_OWNER, typeOid);

    /* don't allow direct alteration of array types */
    elemOid = get_element_type(typeOid);
    if (OidIsValid(elemOid) && get_array_type(elemOid) == typeOid)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("cannot alter array type %s", format_type_be(typeOid)),
                errhint("You can alter type %s, which will alter the array type as well.", format_type_be(elemOid))));

    if (enable_plpgsql_gsdependency_guc() &&
        gsplsql_is_object_depend(typeOid, GSDEPEND_OBJECT_TYPE_TYPE)) {
        ereport(ERROR,
            (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                errmsg("The set schema operator of %s is not allowed, because it is referenced by the other object.",
                    get_typename(typeOid))));
    }
    /* and do the work */
    return AlterTypeNamespaceInternal(typeOid, nspOid, false, true, objsMoved);
}

/*
 * Move specified type to new namespace.
 *
 * Caller must have already checked privileges.
 *
 * The function automatically recurses to process the type's array type,
 * if any.	isImplicitArray should be TRUE only when doing this internal
 * recursion (outside callers must never try to move an array type directly).
 *
 * If errorOnTableType is TRUE, the function errors out if the type is
 * a table type.  ALTER TABLE has to be used to move a table to a new
 * namespace.
 *
 * Returns the type's old namespace OID.
 */
Oid AlterTypeNamespaceInternal(
    Oid typeOid, Oid nspOid, bool isImplicitArray, bool errorOnTableType, ObjectAddresses* objsMoved, char* newTypeName)
{
    Relation rel;
    HeapTuple tup;
    Form_pg_type typform;
    Oid oldNspOid;
    Oid arrayOid;
    bool isCompositeType = false;
    ObjectAddress thisobj;

    /*
     * Make sure we haven't moved this object previously.
     */
    thisobj.classId = TypeRelationId;
    thisobj.objectId = typeOid;
    thisobj.objectSubId = 0;

    if (object_address_present(&thisobj, objsMoved))
        return InvalidOid;

    rel = heap_open(TypeRelationId, RowExclusiveLock);

    tup = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(typeOid));
    if (!HeapTupleIsValid(tup))
        ereport(ERROR, (errcode(ERRCODE_CACHE_LOOKUP_FAILED), errmsg("cache lookup failed for type %u", typeOid)));
    typform = (Form_pg_type)GETSTRUCT(tup);

    oldNspOid = typform->typnamespace;
    arrayOid = typform->typarray;

    /* common checks on switching namespaces */
    CheckSetNamespace(oldNspOid, nspOid, TypeRelationId, typeOid);

    /* check for duplicate name (more friendly than unique-index failure) */
    char* checkTypeName = (newTypeName == NULL) ? NameStr(typform->typname) : newTypeName;
    if (SearchSysCacheExists2(TYPENAMENSP, CStringGetDatum(checkTypeName), ObjectIdGetDatum(nspOid)))
        ereport(ERROR,
            (errcode(ERRCODE_DUPLICATE_OBJECT),
                errmsg("type \"%s\" already exists in schema \"%s\"",
                    NameStr(typform->typname),
                    get_namespace_name(nspOid))));

    /* Detect whether type is a composite type (but not a table rowtype) */
    isCompositeType =
        (typform->typtype == TYPTYPE_COMPOSITE && get_rel_relkind(typform->typrelid) == RELKIND_COMPOSITE_TYPE);
    /* Enforce not-table-type if requested */
    if (typform->typtype == TYPTYPE_COMPOSITE && !isCompositeType && errorOnTableType)
        ereport(ERROR,
            (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                errmsg("%s is a table's row type", format_type_be(typeOid)),
                errhint("Use ALTER TABLE instead.")));

    /* OK, modify the pg_type row
     *
     * tup is a copy, so we can scribble directly on it
     */
    typform->typnamespace = nspOid;
    if (newTypeName != NULL) {
    	(void)namestrcpy(&(typform->typname), newTypeName);
    }

    simple_heap_update(rel, &tup->t_self, tup);
    CatalogUpdateIndexes(rel, tup);

    /*
     * Composite types have pg_class entries.
     *
     * We need to modify the pg_class tuple as well to reflect the change of
     * schema.
     */
    if (isCompositeType) {
        Relation classRel;

        classRel = heap_open(RelationRelationId, RowExclusiveLock);

        AlterRelationNamespaceInternal(classRel, typform->typrelid, oldNspOid, nspOid, false, objsMoved);

        heap_close(classRel, RowExclusiveLock);

        /*
         * Check for constraints associated with the composite type (we don't
         * currently support this, but probably will someday).
         */
        AlterConstraintNamespaces(typform->typrelid, oldNspOid, nspOid, false, objsMoved);
    } else {
        /* If it's a domain, it might have constraints */
        if (typform->typtype == TYPTYPE_DOMAIN)
            AlterConstraintNamespaces(typeOid, oldNspOid, nspOid, true, objsMoved);
    }

    /*
     * Update dependency on schema, if any --- a table rowtype has not got
     * one, and neither does an implicit array.
     */
    if ((isCompositeType || typform->typtype != TYPTYPE_COMPOSITE) && !isImplicitArray)
        if (changeDependencyFor(TypeRelationId, typeOid, NamespaceRelationId, oldNspOid, nspOid) != 1)
            ereport(ERROR,
                (errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
                    errmsg("failed to change schema dependency for type %s", format_type_be(typeOid))));

    tableam_tops_free_tuple(tup);

    heap_close(rel, RowExclusiveLock);

    add_exact_object_address(&thisobj, objsMoved);

    /* Recursively alter the associated array type, if any */
    if (OidIsValid(arrayOid)) {
        AlterTypeNamespaceInternal(arrayOid, nspOid, true, true, objsMoved,
                                    (newTypeName == NULL) ? NULL : makeArrayTypeName(newTypeName, nspOid));
    }

    return oldNspOid;
}

static void CheckFuncParamType(Oid foid, Oid toid, bool isin)
{
    HeapTuple tup = NULL;
    Oid ftype = InvalidOid;
    bool isNull = true;
    Datum src = 0;
    const FuncGroup* fgroup = NULL;

    /* search funcs */
    tup = SearchSysCache1(PROCOID, ObjectIdGetDatum(foid));
    if (HeapTupleIsValid(tup))
        src = SysCacheGetAttr(PROCOID, tup, Anum_pg_proc_prosrc, &isNull);

    if (!isNull) {
        fgroup = SearchBuiltinFuncByName(TextDatumGetCString(src));
        if (fgroup != NULL) {
            if (isin) {
                ftype = fgroup->funcs->rettype;
            } else {
                if (fgroup->funcs->proargtypes.count == 1) {
                    ftype = fgroup->funcs->proargtypes.values[0];
                }
            }
        }
    }
    if (HeapTupleIsValid(tup))
        ReleaseSysCache(tup);

    if (ftype != InvalidOid) {
        HeapTuple tup1 = NULL;
        HeapTuple tup2 = NULL;
        Form_pg_type formtyp1 = NULL;
        Form_pg_type formtyp2 = NULL;

        tup1 = SearchSysCache1(TYPEOID, ObjectIdGetDatum(ftype));
        formtyp1 = HeapTupleIsValid(tup1) ? (Form_pg_type)GETSTRUCT(tup1) : NULL;
        tup2 = SearchSysCache1(TYPEOID, ObjectIdGetDatum(toid));
        formtyp2 = HeapTupleIsValid(tup2) ? (Form_pg_type)GETSTRUCT(tup2) : NULL;

        if (formtyp1 != NULL && formtyp2 != NULL) {
            /*
             * The values of internallength, passedbyvalue, alignment,
             * and storage must be the same for like_type.
             */
            if (formtyp1->typlen != formtyp2->typlen || formtyp1->typbyval != formtyp2->typbyval ||
                formtyp1->typalign != formtyp2->typalign || formtyp1->typstorage != formtyp2->typstorage) {
                ReleaseSysCache(tup1);
                ReleaseSysCache(tup2);
                const char* detail = isin ? "return type" : "parameter type";
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("The function's %s %u dismatch the type %u", detail, ftype, toid)));
            }
        }

        if (HeapTupleIsValid(tup1))
            ReleaseSysCache(tup1);
        if (HeapTupleIsValid(tup2))
            ReleaseSysCache(tup2);
    }
}

static void CheckTypeMatch(Oid funcOid, bool isInputFunc, int16 typlen, bool typbyval)
{
    Oid realTypeOid = GetBuiltinFuncTypeOid(funcOid, isInputFunc);
    if (realTypeOid == InvalidOid) {
        return;
    }

    HeapTuple tuple = NULL;
    Form_pg_type formType = NULL;
    tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(realTypeOid));
    if (!HeapTupleIsValid(tuple)) {
        return;
    }

    formType = (Form_pg_type)GETSTRUCT(tuple);
    int2 realTypeLen = formType->typlen;
    bool isByValue = formType->typbyval;
    formType = NULL; /* no longer used */
    ReleaseSysCache(tuple);

    const char* funcMsg = isInputFunc ?
                            "the input function's real return type" :
                            "the output function's real parameter type";

    if (realTypeLen != typlen) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("%s %s mismatch the type", funcMsg, format_type_be(realTypeOid)),
                errdetail("attribute typlen %d mismatch internallength %d", realTypeLen, typlen)));
    }
    if (isByValue != typbyval) {
        ereport(ERROR,
            (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("%s %s mismatch the type", funcMsg, format_type_be(realTypeOid)),
                errdetail("attribute typbyval %s mismatch passedbyvalue %s", 
                    isByValue ? "true" : "false", 
                    typbyval ? "true" : "false")));
    }
}

static Oid GetBuiltinFuncTypeOid(Oid custumTypeFunc, bool isInputFunc)
{
    Oid realTypeOid = InvalidOid;
    bool isNull = true;
    Datum src = 0;
    const FuncGroup* fgroup = NULL;

    HeapTuple tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(custumTypeFunc));
    if (!HeapTupleIsValid(tuple)) {
        return InvalidOid;
    }

    src = SysCacheGetAttr(PROCOID, tuple, Anum_pg_proc_prosrc, &isNull);
    if (!isNull) {
        fgroup = SearchBuiltinFuncByName(TextDatumGetCString(src));
        if (fgroup != NULL) {
            if (isInputFunc) {
                realTypeOid = fgroup->funcs->rettype;
            } else if (fgroup->funcs->proargtypes.count == 1) {
                realTypeOid = fgroup->funcs->proargtypes.values[0];
            }
        }
    }

    ReleaseSysCache(tuple);

    return realTypeOid;
}
