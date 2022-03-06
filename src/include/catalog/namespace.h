/* -------------------------------------------------------------------------
 *
 * namespace.h
 *	  prototypes for functions in backend/catalog/namespace.c
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/include/catalog/namespace.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef NAMESPACE_H
#define NAMESPACE_H

#ifndef FRONTEND
#include "access/htup.h"
#include "storage/lock/lock.h"
#include "utils/plpgsql.h"
#endif

#include "nodes/primnodes.h"
#include "lib/stringinfo.h"

static volatile uint32 gt_tempID_seed = 0;

/*
 *	This structure holds a list of possible functions or operators
 *	found by namespace lookup.	Each function/operator is identified
 *	by OID and by argument types; the list must be pruned by type
 *	resolution rules that are embodied in the parser, not here.
 *	See FuncnameGetCandidates's comments for more info.
 */
typedef struct _FuncCandidateList
{
	struct _FuncCandidateList *next;
	int			pathpos;		/* for internal use of namespace lookup */
	Oid			oid;			/* the function or operator's OID */
	int			nargs;			/* number of arg types returned */
	Oid         packageOid;     /* package oid */
	int			nvargs;			/* number of args to become variadic array */
	int			ndargs;			/* number of defaulted args */
	int		   *argnumbers;		/* args' positional indexes, if named call */
	Oid			refSynOid;		/* referenced synonym's OID if mapping successfully, and drop it when view decoupling */
        int 		allArgNum;      /* all param num including in/out/inout */
	Oid			args[FLEXIBLE_ARRAY_MEMBER];		/* arg types --- VARIABLE LENGTH ARRAY */
}	*FuncCandidateList;	/* VARIABLE LENGTH STRUCT */

#define MinSizeOfFuncCandidateList offsetof(struct _FuncCandidateList, args)

typedef struct _KeyCandidateList {
    struct _KeyCandidateList *next;
    int pathpos;      /* for internal use of namespace lookup */
    Oid oid;          /* the function or operator's OID */
} *KeyCandidateList; /* VARIABLE LENGTH STRUCT */

#define SEARCH_PATH_GUC_NAME "search_path"
#define CURRENT_SCHEMA_GUC_NAME "current_schema"
#define SCHEMA_TEMP_NAME "pg_temp"
#define SCHEMA_CATALOG_NAME "pg_catalog"

/*
 *	Structure for xxxOverrideSearchPath functions
 */
typedef struct OverrideSearchPath
{
	List	   *schemas;		/* OIDs of explicitly named schemas */
	bool		addCatalog;		/* implicitly prepend pg_catalog? */
	bool		addTemp;		/* implicitly prepend temp schema? */
	bool 		addUser;		/* implicitly postfix $user */
} OverrideSearchPath;

/* Override requests are remembered in a stack of OverrideStackEntry structs */

typedef struct
{
	List		*searchPath;		/* the desired search path */
	Oid		creationNamespace;	/* the desired creation namespace */
	int		nestLevel;		/* subtransaction nesting level */
	bool		inProcedure;		/* is true if in plpgsql */
} OverrideStackEntry;

typedef void (*RangeVarGetRelidCallback) (const RangeVar *relation, Oid relId,
										   Oid oldRelId, bool target_is_partition, void *callback_arg);

#define RangeVarGetRelid(relation, lockmode, missing_ok) \
	RangeVarGetRelidExtended(relation, lockmode, missing_ok, false, false, false, NULL, NULL, NULL, NULL)

#define isTempNamespaceName(name) \
	(strncasecmp((name), "pg_temp_", 8) == 0)

#define isTempNamespaceNameWithQuote(name) \
    (strncasecmp((name), "\"pg_temp_", 9) == 0)

#define isToastTempNamespaceName(name) \
	(strncasecmp((name), "pg_toast_temp_", 14) == 0)

#define IS_TEMP_NAMESPACE(name) \
    (strncasecmp((name), "pg_temp", 7) == 0)

#define IS_CATALOG_NAMESPACE(name) \
    (strncasecmp((name), "pg_catalog", 10) == 0)

#ifndef FRONTEND
extern Oid RangeVarGetRelidExtended(const RangeVar *relation,
						 LOCKMODE lockmode, bool missing_ok, bool nowait, bool target_is_partition, 
						 bool isSupportSynonym,
						 RangeVarGetRelidCallback callback,
						 void *callback_arg,
						 StringInfo detailInfo = NULL,
						 Oid *refSynOid = NULL);
extern Oid	RangeVarGetCreationNamespace(const RangeVar *newRelation);
extern bool CheckRelationCreateAnyPrivilege(Oid userId, char relkind);
extern bool CheckCreatePrivilegeInNamespace(Oid namespaceId, Oid roleId, const char* anyPrivilege);
extern Oid RangeVarGetAndCheckCreationNamespace(RangeVar *newRelation,
									 LOCKMODE lockmode,
									 Oid *existing_relation_id,
									 char relkind);
extern void RangeVarAdjustRelationPersistence(RangeVar *newRelation, Oid nspid);
extern Oid RelnameGetRelid(const char *relname, StringInfo detailInfo = NULL);
extern char* RelnameGetRelidExtended(const char *relname, Oid *relOid, Oid *refSynOid = NULL,
    StringInfo detailInfo = NULL);
extern bool RelationIsVisible(Oid relid);

extern Oid	TypenameGetTypid(const char *typname);
extern Oid  TypenameGetTypidExtended(const char *typname, bool temp_ok);
extern bool TypeIsVisible(Oid typid);
extern Oid TryLookForSynonymType(const char* typname, const Oid namespaceId);

extern void SetTempFromSearchPath(List* namelist);

extern FuncCandidateList FuncnameGetCandidates(List *names,
                      int nargs, List *argnames,
                      bool expand_variadic,
                      bool expand_defaults,
                      bool func_create,
                      bool  include_out = false,
                      char prokind = 'u');

extern bool FunctionIsVisible(Oid funcid);

extern Oid	OpernameGetOprid(List *names, Oid oprleft, Oid oprright);
extern FuncCandidateList OpernameGetCandidates(List *names, char oprkind);
extern bool OperatorIsVisible(Oid oprid);

extern Oid	OpclassnameGetOpcid(Oid amid, const char *opcname);
extern bool OpclassIsVisible(Oid opcid);

extern Oid	OpfamilynameGetOpfid(Oid amid, const char *opfname);
extern bool OpfamilyIsVisible(Oid opfid);

extern Oid	CollationGetCollid(const char *collname);
extern bool CollationIsVisible(Oid collid);

extern Oid	ConversionGetConid(const char *conname);
extern bool ConversionIsVisible(Oid conid);

extern Oid	get_ts_parser_oid(List *names, bool missing_ok);
extern bool TSParserIsVisible(Oid prsId);

extern Oid	get_ts_dict_oid(List *names, bool missing_ok);
extern bool TSDictionaryIsVisible(Oid dictId);

extern Oid	get_ts_template_oid(List *names, bool missing_ok);
extern bool TSTemplateIsVisible(Oid tmplId);

extern Oid	get_ts_config_oid(List *names, bool missing_ok);
extern bool TSConfigIsVisible(Oid cfgid);

extern void DeconstructQualifiedName(const List *names,
						 char **nspname_p,
						 char **objname_p,

						 char **pkgname_p = NULL);
extern Oid	LookupNamespaceNoError(const char *nspname);
extern Oid	LookupExplicitNamespace(const char *nspname, bool missing_ok = false);
extern Oid	get_namespace_oid(const char *nspname, bool missing_ok);
extern Oid	SchemaNameGetSchemaOid(const char *schemaname, bool missing_ok = false);
extern Oid GetOidBySchemaName(bool missing_ok = false);
extern Oid	LookupCreationNamespace(const char *nspname);
extern void CheckSetNamespace(Oid oldNspOid, Oid nspOid, Oid classid,
				  Oid objid);
extern void ResetNameSpace(const List* names, Oid* namespaceOid);
extern Oid	QualifiedNameGetCreationNamespace(const List *names, char **objname_p);
extern RangeVar *makeRangeVarFromNameList(List *names);
extern char *NameListToString(const List *names);
extern char *NameListToQuotedString(List *names);

extern bool isTempNamespace(Oid namespaceId);
extern bool isTempToastNamespace(Oid namespaceId);
extern bool isTempOrToastNamespace(Oid namespaceId);
extern bool isAnyTempNamespace(Oid namespaceId);
extern bool isOtherTempNamespace(Oid namespaceId);
extern Oid	GetTempToastNamespace(void);
extern void ResetTempTableNamespace(void);

extern OverrideSearchPath *GetOverrideSearchPath(MemoryContext context);
extern OverrideSearchPath *CopyOverrideSearchPath(OverrideSearchPath *path);
extern bool OverrideSearchPathMatchesCurrent(OverrideSearchPath *path);
extern void PushOverrideSearchPath(OverrideSearchPath *newpath, bool inProcedure=false);
extern void PopOverrideSearchPath(void);
extern void AddTmpNspToOverrideSearchPath(Oid tmpnspId);
extern void RemoveTmpNspFromSearchPath(Oid tmpnspId);

extern Oid	get_collation_oid(List *collname, bool missing_ok);
extern Oid	get_conversion_oid(List *conname, bool missing_ok);
extern Oid	FindDefaultConversionProc(int4 for_encoding, int4 to_encoding);

/* initialization & transaction cleanup code */
extern void InitializeSearchPath(void);
extern void AtEOXact_Namespace(bool isCommit);
extern void AtEOSubXact_Namespace(bool isCommit, SubTransactionId mySubid,
					  SubTransactionId parentSubid);


extern List *fetch_search_path(bool includeImplicit);
extern int	fetch_search_path_array(Oid *sarray, int sarray_len);
extern Oid get_my_temp_schema();

extern void FetchDefaultArgumentPos(int **defpos, int2vector *adefpos,
									const char *argmodes, int pronallargs);
extern Oid GetUserIdFromNspId(Oid nspid, bool is_securityadmin = false, bool anyPriv = false);

extern void SetTempNamespace(Node *stmt, Oid namespaceOid);
extern void setTempToastNspName();
extern void validateTempRelation(Relation rel);
extern bool checkGroup(Oid relid, bool missing_ok);

extern bool validateTempNamespace(Oid tmepNspId);

extern bool IsPackageFunction(List* funcname);

extern void recomputeNamespacePath(StringInfo error_info = NULL);
extern KeyCandidateList GlobalSettingGetCandidates(const List *names, bool);
extern KeyCandidateList CeknameGetCandidates(const List *names, bool);

extern bool isTableofType(Oid typeOid, Oid* base_oid, Oid* indexbyType);
extern bool isTableofIndexbyType(Oid typeOid);
extern bool IsPlpgsqlLanguageOid(Oid langoid);
#endif // !FRONTEND
#endif   /* NAMESPACE_H */
