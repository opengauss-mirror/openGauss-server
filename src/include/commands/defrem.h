/* -------------------------------------------------------------------------
 *
 * defrem.h
 *	  openGauss define and remove utility definitions.
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2021, openGauss Contributors
 *
 * src/include/commands/defrem.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef DEFREM_H
#define DEFREM_H

#ifndef FRONTEND_PARSER
#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"

/* commands/dropcmds.c */
extern void RemoveObjects(DropStmt* stmt, bool missing_ok, bool is_securityadmin = false);

/* commands/indexcmds.c */
extern ObjectAddress DefineIndex(Oid relationId, IndexStmt* stmt, Oid indexRelationId, bool is_alter_table, bool check_rights,
    bool skip_build, bool quiet, bool is_modify_primary = false);  
extern Oid ReindexIndex(RangeVar* indexRelation, const char* partition_name, AdaptMem* mem_info, bool concurrent);
extern Oid ReindexTable(RangeVar* relation, const char* partition_name, AdaptMem* mem_info, bool concurrent);
extern Oid ReindexInternal(RangeVar* relation, const char* partition_name);

extern Oid ReindexDatabase(const char* databaseName, bool do_system, bool do_user, AdaptMem* mem_info, bool concurrent);
extern char* makeObjectName(const char* name1, const char* name2, const char* label, bool reverseTruncate = false);
extern char* ChooseRelationName(
    const char* name1, const char* name2, const char* label, size_t labelLength, Oid namespaceid,
    bool reverseTruncate = false);
extern bool CheckIndexCompatible(Oid oldId, char* accessMethodName, List* attributeList, List* exclusionOpNames);
extern Oid GetDefaultOpClass(Oid type_id, Oid am_id);
extern void ComputeIndexAttrs(IndexInfo* indexInfo, Oid* typeOidP, Oid* collationOidP, Oid* classOidP,
    int16* colOptionP, List* attList, List* exclusionOpNames, Oid relId, const char* accessMethodName,
    Oid accessMethodId, bool amcanorder, bool isconstraint);
extern List* ChooseIndexColumnNames(const List* indexElems);

#ifdef ENABLE_MULTIPLE_NODES
extern void mark_indisvalid_local(char* schname, char* idxname);
extern void mark_indisvalid_all_cns(char* schname, char* idxname);
#endif

/* commands/functioncmds.c */
extern bool PrepareCFunctionLibrary(HeapTuple tup);
extern void InsertIntoPendingLibraryDelete(const char* filename, bool atCommit);
extern void libraryDoPendingDeletes(bool isCommit);
extern void ResetPendingLibraryDelete();
extern ObjectAddress CreateFunction(CreateFunctionStmt* stmt, const char* queryString, Oid pkg_oid = InvalidOid);
extern ObjectAddress CreateFunction_extend(CreateFunctionStmt* stmt, const char* queryString, void* getHeaderInfoCtx, void** pHeapTuple, bool isPackageFunc = false);
extern ObjectAddress RenameFunction(List* name, List* argtypes, const char* newname);
extern void RemoveFunctionById(Oid funcOid);
extern void remove_encrypted_proc_by_id(Oid funcOid);
extern void RemovePackageById(Oid pkgOid, bool isBody = false);
extern void DeleteFunctionByPackageOid(Oid package_oid);
extern void DeleteFunctionByFuncTuple(HeapTuple func_tup);
extern void SetFunctionReturnType(Oid funcOid, Oid newRetType);
extern void SetFunctionArgType(Oid funcOid, int argIndex, Oid newArgType);
extern ObjectAddress AlterFunctionOwner(List* name, List* argtypes, Oid newOwnerId);
extern void AlterFunctionOwner_oid(Oid procOid, Oid newOwnerId, bool byPackage = false);
extern bool IsFunctionTemp(AlterFunctionStmt* stmt);
extern ObjectAddress AlterFunction(AlterFunctionStmt* stmt);
extern ObjectAddress CreateCast(CreateCastStmt* stmt);
extern void DropCastById(Oid castOid);
extern ObjectAddress AlterFunctionNamespace(List* name, List* argtypes, bool isagg, const char* newschema);
extern Oid AlterFunctionNamespace_oid(Oid procOid, Oid nspOid);
extern void ExecuteDoStmt(DoStmt* stmt, bool atomic);
extern Oid get_cast_oid(Oid sourcetypeid, Oid targettypeid, bool missing_ok);
extern void IsThereFunctionInNamespace(const char *proname, int pronargs, oidvector *proargtypes, Oid nspOid);

extern void IsThereOpClassInNamespace(const char *opcname, Oid opcmethod,
                                      Oid opcnamespace);
extern void IsThereOpFamilyInNamespace(const char *opfname, Oid opfmethod,
                                       Oid opfnamespace);
extern void RecompileFunction(CompileStmt* stmt);

/* commands/operatorcmds.c */
extern void CreatePackageCommand(CreatePackageStmt* parsetree, const char* queryString);
extern void CreatePackageBodyCommand(CreatePackageBodyStmt* parsetree, const char* queryString);
extern ObjectAddress AlterPackageOwner(List* name, Oid newOwnerId);
extern void AlterFunctionOwnerByPkg(Oid package_oid, Oid newOwnerId);

extern ObjectAddress DefineOperator(List* names, List* parameters);
extern void RemoveOperatorById(Oid operOid);
extern ObjectAddress AlterOperatorOwner(List* name, TypeName* typeName1, TypeName* typename2, Oid newOwnerId);
extern void AlterOperatorOwner_oid(Oid operOid, Oid newOwnerId);
extern ObjectAddress AlterOperatorNamespace(List* names, List* argtypes, const char* newschema);
extern Oid AlterOperatorNamespace_oid(Oid operOid, Oid newNspOid);
extern void RecompilePackage(CompileStmt* stmt);

/* commands/aggregatecmds.c */
extern ObjectAddress DefineAggregate(List* name, List* args, bool oldstyle, List* parameters);
extern void RenameAggregate(List* name, List* args, const char* newname);
extern ObjectAddress AlterAggregateOwner(List* name, List* args, Oid newOwnerId);

/* commands/opclasscmds.c */
extern ObjectAddress DefineOpClass(CreateOpClassStmt* stmt);
extern ObjectAddress DefineOpFamily(CreateOpFamilyStmt* stmt);
extern Oid AlterOpFamily(AlterOpFamilyStmt* stmt);
extern void RemoveOpClassById(Oid opclassOid);
extern void RemoveOpFamilyById(Oid opfamilyOid);
extern void RemoveAmOpEntryById(Oid entryOid);
extern void RemoveAmProcEntryById(Oid entryOid);
extern void RenameOpClass(List* name, const char* access_method, const char* newname);
extern ObjectAddress AlterOpClassOwner(List* name, const char* access_method, Oid newOwnerId);
extern void AlterOpClassOwner_oid(Oid opclassOid, Oid newOwnerId);
extern ObjectAddress AlterOpClassNamespace(List* name, const char* access_method, const char* newschema);
extern Oid AlterOpClassNamespace_oid(Oid opclassOid, Oid newNspOid);
extern ObjectAddress AlterOpFamilyOwner(List* name, const char* access_method, Oid newOwnerId);
extern void AlterOpFamilyOwner_oid(Oid opfamilyOid, Oid newOwnerId);
extern ObjectAddress AlterOpFamilyNamespace(List* name, const char* access_method, const char* newschema);
extern Oid AlterOpFamilyNamespace_oid(Oid opfamilyOid, Oid newNspOid);
extern Oid get_am_oid(const char* amname, bool missing_ok);
extern Oid get_opclass_oid(Oid amID, List* opclassname, bool missing_ok);
extern Oid get_opfamily_oid(Oid amID, List* opfamilyname, bool missing_ok);

/* commands/tsearchcmds.c */
extern ObjectAddress DefineTSParser(List* names, List* parameters);
extern ObjectAddress AlterTSParserNamespace(List* name, const char* newschema);
extern Oid AlterTSParserNamespace_oid(Oid prsId, Oid newNspOid);
extern void RemoveTSParserById(Oid prsId);

extern ObjectAddress DefineTSDictionary(List* names, List* parameters);
extern void RenameTSDictionary(List* oldname, const char* newname);
extern void RemoveTSDictionaryById(Oid dictId);
extern ObjectAddress AlterTSDictionary(AlterTSDictionaryStmt* stmt);
extern ObjectAddress AlterTSDictionaryOwner(List* name, Oid newOwnerId);
extern void AlterTSDictionaryOwner_oid(Oid dictId, Oid newOwnerId);
extern ObjectAddress AlterTSDictionaryNamespace(List* name, const char* newschema);
extern Oid AlterTSDictionaryNamespace_oid(Oid dictId, Oid newNspOid);

extern ObjectAddress DefineTSTemplate(List* names, List* parameters);
extern ObjectAddress AlterTSTemplateNamespace(List* name, const char* newschema);
extern Oid AlterTSTemplateNamespace_oid(Oid tmplId, Oid newNspOid);
extern void RemoveTSTemplateById(Oid tmplId);

extern ObjectAddress DefineTSConfiguration(List* names, List* parameters, List* cfoptions);
extern void RemoveTSConfigurationById(Oid cfgId);
extern ObjectAddress AlterTSConfiguration(AlterTSConfigurationStmt* stmt);
extern ObjectAddress AlterTSConfigurationOwner(List* name, Oid newOwnerId);
extern ObjectAddress AlterTSConfigurationNamespace(List* name, const char* newschema);
extern Oid AlterTSConfigurationNamespace_oid(Oid cfgId, Oid newNspOid);

extern text* serialize_deflist(List* deflist);
extern List* deserialize_deflist(Datum txt);

/* commands/weak_password_dictioanry.c */
extern void CreateWeakPasswordDictionary(CreateWeakPasswordDictionaryStmt* stmt);
extern void DropWeakPasswordDictionary();

/* commands/foreigncmds.c */
extern ObjectAddress AlterForeignServerOwner(const char* name, Oid newOwnerId);
extern void AlterForeignServerOwner_oid(Oid, Oid newOwnerId);
extern ObjectAddress AlterForeignDataWrapperOwner(const char* name, Oid newOwnerId);
extern void AlterForeignDataWrapperOwner_oid(Oid fwdId, Oid newOwnerId);
extern ObjectAddress CreateForeignDataWrapper(CreateFdwStmt* stmt);
extern ObjectAddress AlterForeignDataWrapper(AlterFdwStmt* stmt);
extern void RemoveForeignDataWrapperById(Oid fdwId);
extern ObjectAddress CreateForeignServer(CreateForeignServerStmt* stmt);
extern ObjectAddress AlterForeignServer(AlterForeignServerStmt* stmt);
extern void RemoveForeignServerById(Oid srvId);
extern ObjectAddress CreateUserMapping(CreateUserMappingStmt* stmt);
extern ObjectAddress AlterUserMapping(AlterUserMappingStmt* stmt);
extern Oid RemoveUserMapping(DropUserMappingStmt* stmt);
extern void RemoveUserMappingById(Oid umId);
extern void CreateForeignTable(CreateForeignTableStmt* stmt, Oid relid);
#ifdef ENABLE_MOT
extern void CreateForeignIndex(IndexStmt* stmt, Oid indexRelationId);
#endif
extern Datum transformGenericOptions(Oid catalogId, Datum oldOptions, List* options, Oid fdwvalidator);
extern Datum optionListToArray(List* options);
extern List* FindOrRemoveForeignTableOption(List* optList, const char* optName, bool remove, bool* found);

/* support access method in commands/amcmds.cpp */
extern ObjectAddress CreateAccessMethod(CreateAmStmt *stmt);
extern void RemoveAccessMethodById(Oid amoid);
extern char* get_am_name(Oid amoid);


/* support routines in commands/define.c */

extern char* defGetString(DefElem* def);
extern double defGetNumeric(DefElem* def);
extern bool defGetBoolean(DefElem* def);
extern int defGetMixdInt(DefElem *def);
extern int64 defGetInt64(DefElem* def);
extern List* defGetQualifiedName(DefElem* def);
extern TypeName* defGetTypeName(DefElem* def);
extern int defGetTypeLength(DefElem* def);
extern List* defSetOption(List* options, const char* name, Node* value);
extern void delete_file_handle(const char* library_path);
extern int libraryGetPendingDeletes(bool forCommit, char** str_ptr, int* libraryLen);
extern void removeLibrary(const char* filename);
extern List *defGetStringList(DefElem *def);

/* support routines in commands/datasourcecmds.cpp */
extern void CreateDataSource(CreateDataSourceStmt* stmt);
extern void AlterDataSource(AlterDataSourceStmt* stmt);
extern ObjectAddress RenameDataSource(const char* oldname, const char* newname);
extern ObjectAddress AlterDataSourceOwner(const char* name, Oid newOwnerId);
extern void RemoveDataSourceById(Oid src_Id);

extern Oid GetFunctionNodeGroup(CreateFunctionStmt* stmt, bool* multi_group);
extern Oid GetFunctionNodeGroupByFuncid(Oid funcid);
extern Oid GetFunctionNodeGroup(AlterFunctionStmt* stmt);

/* commands/eventcmds.c */
extern void CreateEventCommand(CreateEventStmt* stmt);
extern void AlterEventCommand(AlterEventStmt* stmt);
extern void DropEventCommand(DropEventStmt* stmt);
 
#endif /* !FRONTEND_PARSER */
extern DefElem* defWithOids(bool value);
#endif /* DEFREM_H */
