/* -------------------------------------------------------------------------
 *
 * lsyscache.h
 *	  Convenience routines for common queries in the system catalog cache.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/lsyscache.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef LSYSCACHE_H
#define LSYSCACHE_H

#include "access/htup.h"
#include "nodes/primnodes.h"
#include "catalog/pg_resource_pool.h"
#include "catalog/pg_workload_group.h"
#include "catalog/pg_app_workloadgroup_mapping.h"
#include "catalog/pgxc_node.h"

/* Result list element for get_op_btree_interpretation */
typedef struct OpBtreeInterpretation {
    Oid opfamily_id; /* btree opfamily containing operator */
    int strategy;    /* its strategy number */
    Oid oplefttype;  /* declared left input datatype */
    Oid oprighttype; /* declared right input datatype */
} OpBtreeInterpretation;

/* I/O function selector for get_type_io_data */
typedef enum IOFuncSelector { IOFunc_input, IOFunc_output, IOFunc_receive, IOFunc_send } IOFuncSelector;

extern bool op_in_opfamily(Oid opno, Oid opfamily);
extern int get_op_opfamily_strategy(Oid opno, Oid opfamily);
extern Oid get_op_opfamily_sortfamily(Oid opno, Oid opfamily);
extern void get_op_opfamily_properties(
    Oid opno, Oid opfamily, bool ordering_op, int* strategy, Oid* lefttype, Oid* righttype);
extern Oid get_opfamily_member(Oid opfamily, Oid lefttype, Oid righttype, int16 strategy);
extern bool get_ordering_op_properties(Oid opno, Oid* opfamily, Oid* opcintype, int16* strategy);
extern bool get_compare_function_for_ordering_op(Oid opno, Oid* cmpfunc, bool* reverse);
extern bool get_sort_function_for_ordering_op(Oid opno, Oid* sortfunc, bool* issupport, bool* reverse);
extern Oid get_equality_op_for_ordering_op(Oid opno, bool* reverse);
extern Oid get_ordering_op_for_equality_op(Oid opno, bool use_lhs_type);
extern List* get_mergejoin_opfamilies(Oid opno);
extern bool get_compatible_hash_operators(Oid opno, Oid* lhs_opno, Oid* rhs_opno);
extern bool get_op_hash_functions(Oid opno, RegProcedure* lhs_procno, RegProcedure* rhs_procno);
extern List* get_op_btree_interpretation(Oid opno);
extern bool equality_ops_are_compatible(Oid opno1, Oid opno2);
extern Oid get_opfamily_proc(Oid opfamily, Oid lefttype, Oid righttype, int16 procnum);
extern char* get_attname(Oid relid, AttrNumber attnum);
extern int get_kvtype(Oid relid, AttrNumber attnum);
extern char* get_relid_attribute_name(Oid relid, AttrNumber attnum);
extern AttrNumber get_attnum(Oid relid, const char* attname);
extern char GetGenerated(Oid relid, AttrNumber attnum);
extern Oid get_atttype(Oid relid, AttrNumber attnum);
extern int32 get_atttypmod(Oid relid, AttrNumber attnum);
extern void get_atttypetypmodcoll(Oid relid, AttrNumber attnum, Oid* typid, int32* typmod, Oid* collid);
extern char* get_collation_name(Oid colloid);
extern char* get_constraint_name(Oid conoid);
extern Oid get_opclass_family(Oid opclass);
extern Oid get_opclass_input_type(Oid opclass);
extern RegProcedure get_opcode(Oid opno);
extern char* get_opname(Oid opno);
extern void op_input_types(Oid opno, Oid* lefttype, Oid* righttype);
extern bool op_mergejoinable(Oid opno, Oid inputtype);
extern bool op_hashjoinable(Oid opno, Oid inputtype);
extern bool op_strict(Oid opno);
extern char op_volatile(Oid opno);
extern Oid get_commutator(Oid opno);
extern Oid get_equal(Oid opno);
extern Oid get_negator(Oid opno);
extern RegProcedure get_oprrest(Oid opno);
extern RegProcedure get_oprjoin(Oid opno);
extern char* get_func_name(Oid funcid);
extern Oid get_func_namespace(Oid funcid);
extern Oid get_func_rettype(Oid funcid);
extern int get_func_nargs(Oid funcid);
extern Oid get_func_signature(Oid funcid, Oid** argtypes, int* nargs);
extern Oid get_func_variadictype(Oid funcid);
extern bool get_func_retset(Oid funcid);
extern bool func_strict(Oid funcid);
extern char func_volatile(Oid funcid);
extern char* get_func_langname(Oid funcid);
extern bool get_func_proshippable(Oid funcid);
extern bool get_func_leakproof(Oid funcid);
extern float4 get_func_cost(Oid funcid);
extern float4 get_func_rows(Oid funcid);
extern Oid get_func_lang(Oid funcid);
extern Oid get_relname_relid(const char* relname, Oid relnamespace);
extern char* get_relname_relid_extend(
    const char* relname, Oid relnamespace, Oid* relOid, bool isSupportSynonym, Oid* refSynOid);

#ifdef PGXC
extern int get_relnatts(Oid relid);
#endif
extern char* get_rel_name(Oid relid);
extern char* getPartitionName(Oid partid, bool missing_ok);
extern Oid get_rel_namespace(Oid relid);
extern char get_rel_persistence(Oid relid);
extern bool is_sys_table(Oid relid);
extern Oid get_rel_type_id(Oid relid);
extern char get_rel_relkind(Oid relid);
extern Oid get_rel_tablespace(Oid relid);
extern bool get_typisdefined(Oid typid);
extern int16 get_typlen(Oid typid);
extern bool get_typbyval(Oid typid);
extern void get_typlenbyval(Oid typid, int16* typlen, bool* typbyval);
extern void get_typlenbyvalalign(Oid typid, int16* typlen, bool* typbyval, char* typalign);
extern Oid getTypeIOParam(HeapTuple typeTuple);
extern void get_type_io_data(Oid typid, IOFuncSelector which_func, int16* typlen, bool* typbyval, char* typalign,
    char* typdelim, Oid* typioparam, Oid* func);
extern char get_typstorage(Oid typid);
extern Node* get_typdefault(Oid typid);
extern char get_typtype(Oid typid);
extern bool type_is_rowtype(Oid typid);
extern bool type_is_enum(Oid typid);
extern bool type_is_range(Oid typid);
extern void get_type_category_preferred(Oid typid, char* typcategory, bool* typispreferred);
extern Oid get_typ_typrelid(Oid typid);
extern Oid get_element_type(Oid typid);
extern Oid get_array_type(Oid typid);
extern Oid get_base_element_type(Oid typid);
extern void getTypeInputInfo(Oid type, Oid* typInput, Oid* typIOParam);
extern void getTypeOutputInfo(Oid type, Oid* typOutput, bool* typIsVarlena);
extern void getTypeBinaryInputInfo(Oid type, Oid* typReceive, Oid* typIOParam);
extern void getTypeBinaryOutputInfo(Oid type, Oid* typSend, bool* typIsVarlena);
extern Oid get_typmodin(Oid typid);
extern Oid get_typcollation(Oid typid);
extern bool type_is_collatable(Oid typid);
extern Oid getBaseType(Oid typid);
extern Oid getBaseTypeAndTypmod(Oid typid, int32* typmod);
#ifdef PGXC
extern char* get_cfgname(Oid cfgid);
extern char* get_typename(Oid typid);
extern char* get_enumlabelname(Oid enumlabelid);
extern char* get_exprtypename(Oid enumlabelid);
extern char* get_typename_with_namespace(Oid typid);
extern Oid get_typeoid_with_namespace(const char* typname);
extern char* get_pgxc_nodename(Oid nodeoid, NameData* namedata = NULL);
extern char* get_pgxc_nodename_noexcept(Oid nodeoid, NameData *nodename = NULL);
extern Oid get_pgxc_nodeoid(const char* nodename);
extern Oid get_pgxc_datanodeoid(const char* nodename, bool missingOK);
extern bool check_pgxc_node_name_is_exist(
    const char* nodename, const char* host, int port, int comm_sctp_port, int comm_control_port);
extern Oid get_pgxc_primary_datanode_oid(Oid nodeoid);
extern char* get_pgxc_node_name_by_node_id(int4 node_id, bool handle_error = true);
extern uint32 get_pgxc_node_id(Oid nodeid);
extern void get_node_info(const char* nodename, bool* node_is_ccn, ItemPointer tuple_pos);
extern bool is_pgxc_central_nodeid(Oid nodeid);
extern bool is_pgxc_central_nodename(const char*);
extern Oid get_pgxc_central_nodeid(void);
extern char get_pgxc_nodetype(Oid nodeid);
extern char get_pgxc_nodetype_refresh_cache(Oid nodeid);
extern int get_pgxc_nodeport(Oid nodeid);
extern int get_pgxc_nodesctpport(Oid nodeid);
extern int get_pgxc_nodestrmctlport(Oid nodeid);
extern char* get_pgxc_nodehost(Oid nodeid);
extern int get_pgxc_nodeport1(Oid nodeid);
extern int get_pgxc_nodesctpport1(Oid nodeid);
extern int get_pgxc_nodestrmctlport1(Oid nodeid);
extern char* get_pgxc_groupname(Oid groupoid, char* groupname = NULL);
extern char* get_pgxc_nodehost1(Oid nodeid);
extern char* get_pgxc_node_formdata(const char* nodename);
extern bool is_pgxc_nodepreferred(Oid nodeid);
extern bool is_pgxc_nodeprimary(Oid nodeid);
extern bool is_pgxc_nodeactive(Oid nodeid);
extern bool is_pgxc_hostprimary(Oid nodeid);
extern bool node_check_host(const char* host, Oid nodeid);
extern Oid get_pgxc_groupoid(const char* groupname, bool missing_ok = true);
extern char get_pgxc_groupkind(Oid group_oid);
extern char* get_pgxc_groupparent(Oid group_oid);
extern int get_pgxc_group_bucketcnt(Oid group_oid);
extern bool is_pgxc_group_bucketcnt_exists(Oid parentOid, int bucketCnt, char **groupName, Oid *groupOid);
extern int get_pgxc_groupmembers(Oid groupid, Oid** members);
extern int get_pgxc_groupmembers_redist(Oid groupid, Oid** members);
extern char get_pgxc_group_redistributionstatus(Oid groupid);
extern DistributeBy* getTableDistribution(Oid srcRelid);
extern DistributeBy* getTableHBucketDistribution(Relation rel);
extern int get_pgxc_classnodes(Oid tableid, Oid** nodes);
extern Oid get_pgxc_class_groupoid(Oid tableoid);
extern Oid get_resource_pool_oid(const char* poolname);
extern char* get_resource_pool_name(Oid poolid);
extern Form_pg_resource_pool get_resource_pool_param(Oid poolid, Form_pg_resource_pool rp);
extern char* get_resource_pool_ngname(Oid poolid);
extern bool is_resource_pool_foreign(Oid poolid);
extern Oid get_workload_group_oid(const char* groupname);
extern char* get_workload_group_name(Oid groupid);
extern Oid get_application_mapping_oid(const char* appname);
extern char* get_application_mapping_name(Oid appoid);
#endif
extern int32 get_typavgwidth(Oid typid, int32 typmod);
extern int32 get_attavgwidth(Oid relid, AttrNumber attnum, bool ispartition);
extern bool get_attstatsslot(HeapTuple statstuple, Oid atttype, int32 atttypmod, int reqkind, Oid reqop, Oid* actualop,
    Datum** values, int* nvalues, float4** numbers, int* nnumbers);
extern bool get_attmultistatsslot(HeapTuple statstuple, Oid atttype, int32 atttypmod, int reqkind, Oid reqop,
    Oid* actualop, Datum** values, int* nvalues, float4** numbers, int* nnumbers, bool** nulls = NULL);
extern double get_attstadndistinct(HeapTuple statstuple);
extern void free_attstatsslot(Oid atttype, Datum* values, int nvalues, float4* numbers, int nnumbers);
extern char* get_namespace_name(Oid nspid);
extern Oid get_range_subtype(Oid rangeOid);
extern char* get_cfgnamespace(Oid cfgid);
extern char* get_typenamespace(Oid typid);
extern char* get_enumtypenamespace(Oid enumlabelid);
extern Oid get_typeoid(Oid namespaceId, const char* typname);
extern Oid get_enumlabeloid(Oid enumtypoid, const char* enumlabelname);
extern Oid get_operator_oid(const char* operatorName, Oid operatorNamespace, Oid leftObjectId, Oid rightObjectId);
extern void get_oper_name_namespace_oprs(Oid operid, char** oprname, char** nspname, Oid* oprleft, Oid* oprright);
extern Oid get_func_oid(const char* funcname, Oid funcnamespace, Expr* expr);
extern Oid get_func_oid_ext(const char* funcname, Oid funcnamespace, Oid funcrettype, int funcnargs, Oid* funcargstype);
extern Oid get_my_temp_schema();
extern bool check_rel_is_partitioned(Oid relid);
extern Oid partid_get_parentid(Oid partid);
extern bool is_not_strict_agg(Oid funcOid);
extern bool is_pgxc_class_table(Oid tableoid);
extern Oid get_valid_relname_relid(const char* relnamespace, const char* relname, bool nsp_missing_ok = false);
extern bool get_func_iswindow(Oid funcid);
extern char get_func_prokind(Oid funcid);
extern char get_typecategory(Oid typid);

#define type_is_array(typid) (get_element_type(typid) != InvalidOid)
/* type_is_array_domain accepts both plain arrays and domains over arrays */
#define type_is_array_domain(typid) (get_base_element_type(typid) != InvalidOid)

#define TypeIsToastable(typid) (get_typstorage(typid) != 'p')

#endif /* LSYSCACHE_H */
