%{

/*#define YYDEBUG 1*/
/* -------------------------------------------------------------------------
 *
 * gram.y
 *	  POSTGRESQL BISON rules/actions
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2010-2012 Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/parser/gram.y
 *
 * HISTORY
 *	  AUTHOR			DATE			MAJOR EVENT
 *	  Andrew Yu			Sept, 1994		POSTQUEL to SQL conversion
 *	  Andrew Yu			Oct, 1994		lispy code conversion
 *
 * NOTES
 *	  CAPITALS are used to represent terminal symbols.
 *	  non-capitals are used to represent non-terminals.
 *	  SQL92-specific syntax is separated from plain SQL/Postgres syntax
 *	  to help isolate the non-extensible portions of the parser.
 *
 *	  In general, nothing in this file should initiate database accesses
 *	  nor depend on changeable state (such as SET variables).  If you do
 *	  database accesses, your code will fail when we have aborted the
 *	  current transaction and are just parsing commands to find the next
 *	  ROLLBACK or COMMIT.  If you make use of SET variables, then you
 *	  will do the wrong thing in multi-query strings like this:
 *			SET SQL_inheritance TO off; SELECT * FROM foo;
 *	  because the entire string is parsed by gram.y before the SET gets
 *	  executed.  Anything that depends on the database or changeable state
 *	  should be handled during parse analysis so that it happens at the
 *	  right time not the wrong time.  The handling of SQL_inheritance is
 *	  a good example.
 *
 * WARNINGS
 *	  If you use a list, make sure the datum is a node so that the printing
 *	  routines work.
 *
 *	  Sometimes we assign constants to makeStrings. Make sure we don't free
 *	  those.
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"

#include <ctype.h>
#include <limits.h>

#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_trigger.h"
#include "commands/defrem.h"
#ifdef ENABLE_MULTIPLE_NODES
#include "distribute_core.h"
#endif
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/gramparse.h"
#include "parser/parse_hint.h"
#include "pgxc/pgxc.h"
#include "nodes/nodes.h"
#include "pgxc/poolmgr.h"
#include "parser/parser.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/numeric.h"
#include "utils/syscache.h"
#include "utils/xml.h"
#include "catalog/pg_streaming_fn.h"

#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wunused-variable"

#define MAXFNAMELEN		64

#ifndef ENABLE_MULTIPLE_NODES
DB_CompatibilityAttr g_dbCompatArray[] = {
    {DB_CMPT_A, "A"},
    {DB_CMPT_B, "B"},
    {DB_CMPT_C, "C"},
    {DB_CMPT_PG, "PG"}
};

IntervalStylePack g_interStyleVal = {"a"};
#endif

/* Location tracking support --- simpler than bison's default */

#define YYLLOC_DEFAULT(Current, Rhs, N) \
	do { \
		if (N) \
			(Current) = (Rhs)[1]; \
		else \
			(Current) = (Rhs)[0]; \
	} while (0)

/*
 * Bison doesn't allocate anything that needs to live across parser calls,
 * so we can easily have it use palloc instead of malloc.  This prevents
 * memory leaks if we error out during parsing.  Note this only works with
 * bison >= 2.0.  However, in bison 1.875 the default is to use alloca()
 * if possible, so there's not really much problem anyhow, at least if
 * you're building with gcc.
 */
#define YYMALLOC palloc
#define YYFREE   pfree
#ifdef YYLEX_PARAM
# define YYLEX yylex (&yylval, &yylloc, YYLEX_PARAM)
#else
# define YYLEX yylex (&yylval, &yylloc, yyscanner)
#endif

/* Private struct for the result of privilege_target production */
typedef struct PrivTarget
{
	GrantTargetType targtype;
	GrantObjectType objtype;
	List	   *objs;
} PrivTarget;

/* ConstraintAttributeSpec yields an integer bitmask of these flags: */
#define CAS_NOT_DEFERRABLE			0x01
#define CAS_DEFERRABLE				0x02
#define CAS_INITIALLY_IMMEDIATE		0x04
#define CAS_INITIALLY_DEFERRED		0x08
#define CAS_NOT_VALID				0x10
#define CAS_NO_INHERIT				0x20

/*
 * In the IntoClause structure there is a char value which will eventually be
 * set to RELKIND_RELATION or RELKIND_MATVIEW based on the relkind field in
 * the statement-level structure, which is an ObjectType. Define the default
 * here, which should always be overridden later.
 */
#define INTO_CLAUSE_RELKIND_DEFAULT    '\0'

#define parser_yyerror(msg)  scanner_yyerror(msg, yyscanner)
#define parser_errposition(pos)  scanner_errposition(pos, yyscanner)

static void base_yyerror(YYLTYPE *yylloc, core_yyscan_t yyscanner,
						 const char *msg);
static Node *makeColumnRef(char *colname, List *indirection,
						   int location, core_yyscan_t yyscanner);
static Node *makeTypeCast(Node *arg, TypeName *typname, int location);
static Node *makeStringConst(char *str, int location);
static Node *makeStringConstCast(char *str, int location, TypeName *typname);
static Node *makeIntConst(int val, int location);
static Node *makeFloatConst(char *str, int location);
static Node *makeBitStringConst(char *str, int location);
Node *makeAConst(Value *v, int location);
Node *makeBoolAConst(bool state, int location);
static void check_qualified_name(List *names, core_yyscan_t yyscanner);
static List *check_func_name(List *names, core_yyscan_t yyscanner);
static List *check_setting_name(List *names, core_yyscan_t yyscanner);
static List *check_indirection(List *indirection, core_yyscan_t yyscanner);
static List *extractArgTypes(List *parameters);
static void insertSelectOptions(SelectStmt *stmt,
								List *sortClause, List *lockingClause,
								Node *limitOffset, Node *limitCount,
								WithClause *withClause,
								core_yyscan_t yyscanner);
static Node *makeSetOp(SetOperation op, bool all, Node *larg, Node *rarg);
static Node *doNegate(Node *n, int location);
static void doNegateFloat(Value *v);
static Node *makeAArrayExpr(List *elements, int location);
static Node *makeXmlExpr(XmlExprOp op, char *name, List *named_args,
						 List *args, int location);
static Node *makeCallFuncStmt(List* funcname, List* parameters);
static List *mergeTableFuncParameters(List *func_args, List *columns);
static TypeName *TableFuncTypeName(List *columns);
static RangeVar *makeRangeVarFromAnyName(List *names, int position, core_yyscan_t yyscanner);
static void SplitColQualList(List *qualList,
							 List **constraintList, CollateClause **collClause,
							 core_yyscan_t yyscanner);
static void SplitColQualList(List *qualList,
							 List **constraintList, CollateClause **collClause, ClientLogicColumnRef **clientLogicColumnRef,
							 core_yyscan_t yyscanner);
static void processCASbits(int cas_bits, int location, const char *constrType,
			   bool *deferrable, bool *initdeferred, bool *not_valid,
			   bool *no_inherit, core_yyscan_t yyscanner);
static Expr *makeNodeDecodeCondtion(Expr* firstCond,Expr* secondCond);
static List *make_action_func(List *arguments);
static List *get_func_args(char *sid);
static char *pg_strsep(char **stringp, const char *delim);
static long long get_pid(const char *strsid);
static Node *MakeAnonyBlockFuncStmt(int flag, const char * str);
#define  DECLARE_LEN     9 /* strlen(" DECLARE ") */
#define  DECLARE_STR     " DECLARE "
static int get_outarg_num(List *fun_args);
static int get_table_modes(int nargs, const char *p_argmodes);
static void get_arg_mode_by_name(const char *argname, const char * const *argnames,
				const char *argmodes, const int proargnum,
				bool *have_assigend, char *argmode);
static void get_arg_mode_by_pos(const int pos, const char *argmodes, const int narg,
				bool *have_assigend, char *argmode);
static List *append_inarg_list(const char argmode, const ListCell *cell,List *in_parameter);
static void check_outarg_info(const bool *have_assigend,
				const char *argmodes,const int proargnum);
bool IsValidIdentClientKey(const char *input);
bool IsValidIdent(char *input);
bool IsValidGroupname(const char *input);
static bool checkNlssortArgs(const char *argname);
static void ParseUpdateMultiSet(List *set_target_list, SelectStmt *stmt, core_yyscan_t yyscanner);
static void parameter_check_execute_direct(const char* query);
%}

%pure-parser
%expect 0
%name-prefix="base_yy"
%locations

%parse-param {core_yyscan_t yyscanner}
%lex-param   {core_yyscan_t yyscanner}

%union
{
	core_YYSTYPE		core_yystype;
	/* these fields must match core_YYSTYPE: */
	int					ival;
	char				*str;
	const char			*keyword;

	char				chr;
	bool				boolean;
	JoinType			jtype;
	DropBehavior		dbehavior;
	OnCommitAction		oncommit;
	List				*list;
	Node				*node;
	Value				*value;
	ObjectType			objtype;
	TypeName			*typnam;
	FunctionParameter   *fun_param;
	FunctionParameterMode fun_param_mode;
	FuncWithArgs		*funwithargs;
	DefElem				*defelt;
	SortBy				*sortby;
	WindowDef			*windef;
	JoinExpr			*jexpr;
	IndexElem			*ielem;
	Alias				*alias;
	RangeVar			*range;
	IntoClause			*into;
	WithClause			*with;
	A_Indices			*aind;
	ResTarget			*target;
	struct PrivTarget	*privtarget;
	AccessPriv			*accesspriv;
	InsertStmt			*istmt;
	VariableSetStmt		*vsetstmt;
/* PGXC_BEGIN */
	DistributeBy		*distby;
	PGXCSubCluster		*subclus;
/* PGXC_END */
    ForeignPartState    *foreignpartby;
	MergeWhenClause		*mergewhen;
	UpsertClause *upsert;
	EncryptionType algtype;
}

%type <node>	stmt schema_stmt
		AlterDatabaseStmt AlterDatabaseSetStmt AlterDataSourceStmt AlterDomainStmt AlterEnumStmt
		AlterFdwStmt AlterForeignServerStmt AlterGroupStmt
		AlterObjectSchemaStmt AlterOwnerStmt AlterSeqStmt AlterTableStmt
		AlterExtensionStmt AlterExtensionContentsStmt AlterForeignTableStmt
		AlterCompositeTypeStmt AlterUserStmt AlterUserMappingStmt AlterUserSetStmt
		AlterSystemStmt
		AlterRoleStmt AlterRoleSetStmt AlterRlsPolicyStmt
		AlterDefaultPrivilegesStmt DefACLAction AlterSessionStmt
		AnalyzeStmt CleanConnStmt ClosePortalStmt ClusterStmt CommentStmt
		ConstraintsSetStmt CopyStmt CreateAsStmt CreateCastStmt CreateContQueryStmt CreateDirectoryStmt 
		CreateDomainStmt CreateExtensionStmt CreateGroupStmt CreateKeyStmt CreateOpClassStmt
		CreateOpFamilyStmt AlterOpFamilyStmt CreatePLangStmt
		CreateSchemaStmt CreateSeqStmt CreateStmt CreateStreamStmt CreateTableSpaceStmt
		CreateFdwStmt CreateForeignServerStmt CreateForeignTableStmt
		CreateDataSourceStmt
		CreateAssertStmt CreateTrigStmt
		CreateUserStmt CreateUserMappingStmt CreateRoleStmt CreateRlsPolicyStmt CreateSynonymStmt
		CreatedbStmt DeclareCursorStmt DefineStmt DeleteStmt DiscardStmt DoStmt
		DropGroupStmt DropOpClassStmt DropOpFamilyStmt DropPLangStmt DropStmt
		DropAssertStmt DropSynonymStmt DropTrigStmt DropRuleStmt DropCastStmt DropRoleStmt DropRlsPolicyStmt
		DropUserStmt DropdbStmt DropTableSpaceStmt DropDataSourceStmt DropDirectoryStmt DropFdwStmt
		DropForeignServerStmt DropUserMappingStmt ExplainStmt ExecDirectStmt FetchStmt
		GrantStmt GrantRoleStmt IndexStmt InsertStmt ListenStmt LoadStmt
		LockStmt NotifyStmt ExplainableStmt PreparableStmt
		CreateFunctionStmt CreateProcedureStmt AlterFunctionStmt AlterProcedureStmt ReindexStmt RemoveAggrStmt
		RemoveFuncStmt RemoveOperStmt RenameStmt RevokeStmt RevokeRoleStmt
		RuleActionStmt RuleActionStmtOrEmpty RuleStmt
		SecLabelStmt SelectStmt TransactionStmt TruncateStmt CallFuncStmt
		UnlistenStmt UpdateStmt VacuumStmt
		VariableResetStmt VariableSetStmt VariableShowStmt VerifyStmt ShutdownStmt
		ViewStmt CheckPointStmt CreateConversionStmt
		DeallocateStmt PrepareStmt ExecuteStmt
		DropOwnedStmt ReassignOwnedStmt
		AlterTSConfigurationStmt AlterTSDictionaryStmt AnonyBlockStmt
		BarrierStmt AlterNodeStmt CreateNodeStmt DropNodeStmt AlterCoordinatorStmt
		CreateNodeGroupStmt AlterNodeGroupStmt DropNodeGroupStmt 
		CreatePolicyLabelStmt AlterPolicyLabelStmt DropPolicyLabelStmt 
        CreateAuditPolicyStmt AlterAuditPolicyStmt DropAuditPolicyStmt 
		CreateMaskingPolicyStmt AlterMaskingPolicyStmt DropMaskingPolicyStmt 
		CreateResourcePoolStmt AlterResourcePoolStmt DropResourcePoolStmt
		CreateWorkloadGroupStmt AlterWorkloadGroupStmt DropWorkloadGroupStmt
		CreateAppWorkloadGroupMappingStmt AlterAppWorkloadGroupMappingStmt DropAppWorkloadGroupMappingStmt
		MergeStmt CreateMatViewStmt RefreshMatViewStmt
		CreateWeakPasswordDictionaryStmt DropWeakPasswordDictionaryStmt

%type <node>	select_no_parens select_with_parens select_clause
				simple_select values_clause

%type <node>	alter_column_default opclass_item opclass_drop alter_using
%type <ival>	add_drop opt_asc_desc opt_nulls_order
%type <ival>	OptNoLog

%type <node>	alter_table_cmd alter_partition_cmd alter_type_cmd opt_collate_clause exchange_partition_cmd move_partition_cmd
				modify_column_cmd
				replica_identity
%type <list>	alter_table_cmds alter_partition_cmds alter_table_or_partition alter_type_cmds add_column_cmds modify_column_cmds

%type <dbehavior>	opt_drop_behavior

%type <list>	createdb_opt_list alterdb_opt_list copy_opt_list
				transaction_mode_list weak_password_string_list
				create_extension_opt_list alter_extension_opt_list
				pgxcnode_list pgxcnodes bucket_maps bucket_list
				opt_pgxcnodes
%type <defelt>	createdb_opt_item alterdb_opt_item copy_opt_item
				transaction_mode_item
				create_extension_opt_item alter_extension_opt_item

%type <ival>	opt_lock lock_type cast_context
%type <ival>	vacuum_option_list vacuum_option_elem opt_verify_options
%type <boolean>	opt_check opt_force opt_or_replace
				opt_grant_grant_option opt_grant_admin_option
				opt_nowait opt_if_exists opt_with_data

%type <list>	OptRoleList AlterOptRoleList
%type <defelt>	CreateOptRoleElem AlterOptRoleElem

%type <str>		opt_type
%type <str>		foreign_server_version opt_foreign_server_version
%type <str>		data_source_version opt_data_source_version data_source_type opt_data_source_type
%type <str>		auth_ident
%type <str>		opt_in_database

%type <str>		OptSchemaName
%type <list>	OptSchemaEltList

%type <boolean> TriggerForSpec TriggerForType ForeignTblWritable
%type <ival>	TriggerActionTime
%type <list>	TriggerEvents TriggerOneEvent
%type <value>	TriggerFuncArg
%type <node>	TriggerWhen

%type <str>		copy_file_name
				database_name access_method_clause access_method attr_name
				name namedata_string fdwName cursor_name file_name
				index_name cluster_index_specification
				pgxcnode_name pgxcgroup_name resource_pool_name workload_group_name
				application_name password_string hint_string
%type <list>	func_name func_name_opt_arg handler_name qual_Op qual_all_Op subquery_Op
				opt_class opt_inline_handler opt_validator validator_clause
				opt_collate

%type <range>	qualified_name OptConstrFromTable opt_index_name

%type <str>		all_Op MathOp

%type <str>		RowLevelSecurityPolicyName row_level_security_cmd RLSDefaultForCmd row_level_security_role
%type <boolean>	RLSDefaultPermissive
%type <node>	RLSOptionalUsingExpr
%type <list>	row_level_security_role_list RLSDefaultToRole RLSOptionalToRole

%type <str>		iso_level opt_encoding
%type <node>	grantee
%type <list>	grantee_list
%type <accesspriv> privilege
%type <list>	privileges privilege_list
%type <str>		privilege_str
%type <privtarget> privilege_target
%type <funwithargs> function_with_argtypes
%type <list>	function_with_argtypes_list
%type <ival>	defacl_privilege_target
%type <defelt>	DefACLOption
%type <list>	DefACLOptionList

%type <list>	stmtblock stmtmulti
				OptTableElementList TableElementList OptInherit definition tsconf_definition
				OptTypedTableElementList TypedTableElementList
				OptForeignTableElementList ForeignTableElementList
				reloptions opt_reloptions opt_tblspc_options tblspc_options opt_cfoptions cfoptions
				OptWith opt_distinct opt_definition func_args func_args_list
				func_args_with_defaults func_args_with_defaults_list proc_args
				func_as createfunc_opt_list opt_createproc_opt_list alterfunc_opt_list
				aggr_args old_aggr_definition old_aggr_list
				oper_argtypes RuleActionList RuleActionMulti
				opt_column_list columnList opt_name_list opt_analyze_column_define opt_multi_name_list
				opt_include opt_c_include index_including_params
				sort_clause opt_sort_clause sortby_list index_params
				name_list from_clause from_list opt_array_bounds
				qualified_name_list any_name any_name_list
				any_operator expr_list attrs callfunc_args
				target_list insert_column_list set_target_list
				set_clause_list set_clause multiple_set_clause
				ctext_expr_list ctext_row def_list tsconf_def_list indirection opt_indirection
				reloption_list tblspc_option_list cfoption_list group_clause TriggerFuncArgs select_limit
				opt_select_limit opt_delete_limit opclass_item_list opclass_drop_list
				opclass_purpose opt_opfamily transaction_mode_list_or_empty
				OptTableFuncElementList TableFuncElementList opt_type_modifiers
				prep_type_clause
				execute_param_clause using_clause returning_clause
				opt_enum_val_list enum_val_list table_func_column_list
				create_generic_options alter_generic_options
				relation_expr_list dostmt_opt_list
				merge_values_clause

%type <list>	group_by_list
%type <node>	group_by_item empty_grouping_set rollup_clause cube_clause
%type <node>	grouping_sets_clause

%type <list>	opt_fdw_options fdw_options
%type <defelt>	fdw_option

%type <range>	OptTempTableName
%type <into>	into_clause create_as_target create_mv_target

%type <defelt>	createfunc_opt_item createproc_opt_item common_func_opt_item dostmt_opt_item
%type <fun_param> func_arg func_arg_with_default table_func_column
%type <fun_param_mode> arg_class
%type <typnam>	func_return func_type

%type <boolean>  opt_trusted opt_restart_seqs
%type <ival>	 OptTemp OptKind
%type <oncommit> OnCommitOption

%type <node>	for_locking_item
%type <list>	for_locking_clause opt_for_locking_clause for_locking_items
%type <list>	locked_rels_list
%type <boolean>	opt_all

%type <node>	join_outer join_qual
%type <jtype>	join_type

%type <list>	extract_list timestamp_arg_list overlay_list position_list
%type <list>	substr_list trim_list
%type <list>	opt_interval interval_second
%type <node>	overlay_placing substr_from substr_for

%type <boolean> opt_instead opt_incremental
%type <boolean> opt_unique opt_concurrently opt_verbose opt_full opt_deltamerge opt_compact opt_hdfsdirectory opt_verify
%type <boolean> opt_freeze opt_default opt_recheck opt_cascade
%type <defelt>	opt_binary opt_oids copy_delimiter opt_noescaping
%type <defelt>	OptCopyLogError OptCopyRejectLimit

%type <boolean> opt_processed

%type <str>		DirectStmt CleanConnDbName CleanConnUserName
/* PGXC_END */
%type <boolean> copy_from

%type <ival>	opt_column event cursor_options opt_hold opt_set_data
%type <objtype>	reindex_type drop_type comment_type security_label_type

%type <node>	fetch_args limit_clause select_limit_value
				offset_clause select_offset_value
				select_offset_value2 opt_select_fetch_first_value
%type <list>	limit_offcnt_clause
%type <ival>	row_or_rows first_or_next

%type <list>	OptSeqOptList SeqOptList
%type <defelt>	SeqOptElem

/* INSERT */
%type <istmt>	insert_rest
%type <node>	upsert_clause

%type <mergewhen>	merge_insert merge_update

%type <vsetstmt> generic_set set_rest set_rest_more SetResetClause FunctionSetResetClause

%type <node>	TableElement TypedTableElement ConstraintElem TableFuncElement
				ForeignTableElement
%type <node>	columnDef columnOptions
%type <defelt>	def_elem tsconf_def_elem reloption_elem tblspc_option_elem old_aggr_elem cfoption_elem
%type <node>	def_arg columnElem where_clause where_or_current_clause
                                a_expr b_expr c_expr c_expr_noparen AexprConst indirection_el
                                columnref in_expr having_clause func_table array_expr
				ExclusionWhereClause
%type <list>	ExclusionConstraintList ExclusionConstraintElem
%type <list>	func_arg_list
%type <node>	func_arg_expr
%type <list>	row explicit_row implicit_row type_list array_expr_list
%type <node>	case_expr case_arg when_clause case_default
%type <list>	when_clause_list
%type <ival>	sub_type
%type <node>	ctext_expr
%type <value>	NumericOnly
%type <list>	NumericOnly_list
%type <alias>	alias_clause opt_alias_clause
%type <sortby>	sortby
%type <ielem>	index_elem
%type <node>	table_ref
%type <jexpr>	joined_table
%type <range>	relation_expr
%type <range>	relation_expr_opt_alias
%type <target>	target_el single_set_clause set_target insert_column_item
%type <node>	tablesample_clause opt_repeatable_clause

%type <str>		generic_option_name
%type <node>	generic_option_arg
%type <defelt>	generic_option_elem alter_generic_option_elem
%type <list>	generic_option_list alter_generic_option_list
%type <str>		explain_option_name
%type <node>	explain_option_arg
%type <defelt>	explain_option_elem
%type <list>	explain_option_list
%type <node>	copy_generic_opt_arg copy_generic_opt_arg_list_item
%type <defelt>	copy_generic_opt_elem
%type <list>	copy_generic_opt_list copy_generic_opt_arg_list
%type <list>	copy_options

%type <typnam>	Typename SimpleTypename ConstTypename
				GenericType Numeric opt_float
				Character ConstCharacter
				CharacterWithLength CharacterWithoutLength
				ConstDatetime ConstInterval
				Bit ConstBit BitWithLength BitWithoutLength client_logic_type
				datatypecl
%type <str>		character
%type <str>		extract_arg
%type <str>		timestamp_units
%type <str>		opt_charset
%type <boolean> opt_varying opt_timezone opt_no_inherit

%type <ival>	Iconst SignedIconst
%type <str>		Sconst comment_text notify_payload
%type <str>		RoleId TypeOwner opt_granted_by opt_boolean_or_string ColId_or_Sconst
%type <list>	var_list
%type <str>		ColId ColLabel var_name type_function_name param_name
%type <node>	var_value zone_value

%type <keyword> unreserved_keyword type_func_name_keyword
%type <keyword> col_name_keyword reserved_keyword

%type <node>	TableConstraint TableLikeClause ForeignTableLikeClause
%type <ival>	excluding_option_list TableLikeOptionList TableLikeIncludingOption TableLikeExcludingOption
%type <list>	ColQualList
%type <node>	ColConstraint ColConstraintElem ConstraintAttr InformationalConstraintElem
%type <ival>	key_actions key_delete key_match key_update key_action
%type <ival>	ConstraintAttributeSpec ConstraintAttributeElem
%type <str>		ExistingIndex

%type <list>	constraints_set_list
%type <boolean> constraints_set_mode
%type <boolean> OptRelative
%type <boolean> OptGPI
%type <str>		OptTableSpace OptConsTableSpace OptTableSpaceOwner LoggingStr size_clause OptMaxSize OptDatafileSize OptReuse OptAuto OptNextStr OptDatanodeName
%type <list>	opt_check_option

%type <str>		opt_provider security_label

%type <target>	xml_attribute_el
%type <list>	xml_attribute_list xml_attributes
%type <node>	xml_root_version opt_xml_root_standalone
%type <node>	xmlexists_argument
%type <ival>	document_or_content
%type <boolean> xml_whitespace_option

%type <node>	func_application func_expr_common_subexpr
%type <node>	func_expr func_expr_windowless
%type <node>	common_table_expr
%type <with>	with_clause opt_with_clause
%type <list>	cte_list

%type <list>	within_group_clause
%type <list>	window_clause window_definition_list opt_partition_clause
%type <windef>	window_definition over_clause window_specification
				opt_frame_clause frame_extent frame_bound
%type <str>		opt_existing_window_name
%type <boolean>	opt_if_not_exists
%type <chr>		OptCompress
%type <ival>	KVType
%type <ival>		ColCmprsMode
%type <str>		subprogram_body
%type <keyword> as_is
%type <node>	column_item opt_table_partitioning_clause
				opt_partition_index_def  range_partition_index_item  range_partition_index_list
				range_partitioning_clause value_partitioning_clause opt_interval_partition_clause
				interval_expr maxValueItem list_partitioning_clause hash_partitioning_clause
				range_start_end_item range_less_than_item list_partition_item hash_partition_item
%type <list>	range_partition_definition_list list_partition_definition_list hash_partition_definition_list maxValueList
			column_item_list tablespaceList opt_interval_tablespaceList
			split_dest_partition_define_list
			range_start_end_list range_less_than_list opt_range_every_list
%type <range> partition_name

%type <ival> opt_row_movement_clause
/* PGXC_BEGIN */
%type <str>		opt_barrier_id OptDistributeType SliceReferenceClause
%type <distby>	OptDistributeBy OptDistributeByInternal distribute_by_range_clause
				distribute_by_list_clause
%type <list>	range_slice_definition_list range_slice_less_than_list range_slice_start_end_list
				list_distribution_rules_list list_distribution_rule_row list_distribution_rule_single
%type <node>	range_slice_less_than_item range_slice_start_end_item
				list_dist_state OptListDistribution list_dist_value

%type <subclus> OptSubCluster OptSubClusterInternal
/* PGXC_END */

%type <str>		OptPartitionElement
%type <node>	OptForeignTableLogError OptForeignTableLogRemote
%type <node> 	ForeignPosition ForeignColDef copy_col_format_def
%type <node> 	OptPerNodeRejectLimit
%type <list> 	copy_foramtter_opt

/* FOREIGN_PARTITION */
%type <foreignpartby> OptForeignPartBy OptForeignPartAuto
%type <node>	partition_item
%type <list>	partition_item_list

/* NODE GROUP */
%type <boolean> opt_vcgroup opt_to_elastic_group
%type <ival>	opt_set_vcgroup
%type <str>	opt_redistributed opt_internal_data internal_data_body

/* MERGE INTO */
%type <node> merge_when_clause opt_merge_where_condition
%type <list> merge_when_list

/* ENCRYPTION */
%type <node> algorithm_desc CreateMasterKeyStmt CreateColumnKeyStmt master_key_elem column_key_elem with_algorithm
%type <list> master_key_params column_key_params
%type <list> columnEncryptionKey 
%type <algtype> encryptionType

/* CLIENT_LOGIC */
%type <list> setting_name

/* POLICY LABEL */
%type <str> 	policy_label_name policy_label_resource_type
				policy_label_filter_type policy_label_any_resource
%type <list> 	resources_to_label_list filters_to_label_list
				policy_label_items opt_add_resources_to_label
				resources_or_filters_to_label_list policy_labels_list
%type <defelt> resources_to_label_list_item filters_to_label_list_item
%type <range> policy_label_item policy_label_any_resource_item
/* AUDIT POLICY */
%type <list> alter_policy_filter_list alter_policy_privileges_list alter_policy_access_list
%type <str> policy_name policy_privilege_type policy_access_type policy_filter_type policy_filter_name policy_target_type
%type <range> policy_target_name
%type <boolean> policy_status_opt
%type <defelt> policy_privilege_elem policy_access_elem policy_target_elem_opt
%type <node> policy_filter_elem pp_policy_filter_elem filter_term pp_filter_term filter_expr pp_filter_expr filter_paren filter_expr_list filter_set policy_filter_value 
%type <list> policy_filters_list policy_filter_opt policy_privileges_list policy_access_list policy_targets_list
%type <list> policy_names_list
%type <defelt>  policy_status_alter_clause 
%type <str> alter_policy_action_clause policy_comments_alter_clause

/* MASKING POLICY */
%type <str> 	masking_func masking_policy_target_type alter_masking_policy_action_clause
				masking_policy_condition_operator
%type <list> 	masking_clause masking_func_params_opt masking_func_params_list
				alter_masking_policy_func_items_list
%type <defelt> 	masking_clause_elem masking_target
%type <defelt> 	masking_func_param alter_masking_policy_func_item
%type <node> 	policy_condition_opt
%type <node> 	alter_policy_condition
%type <node> 	masking_policy_condition_value

/*
 * Non-keyword token types.  These are hard-wired into the "flex" lexer.
 * They must be listed first so that their numeric codes do not depend on
 * the set of keywords.  PL/pgsql depends on this so that it can share the
 * same lexer.  If you add/change tokens here, fix PL/pgsql to match!
 *
 * DOT_DOT is unused in the core SQL grammar, and so will always provoke
 * parse errors.  It is needed by PL/pgsql.
 */
%token <str>	IDENT FCONST SCONST BCONST XCONST Op CmpOp COMMENTSTRING
%token <ival>	ICONST PARAM
%token			TYPECAST ORA_JOINOP DOT_DOT COLON_EQUALS PARA_EQUALS

/*
 * If you want to make any keyword changes, update the keyword table in
 * src/include/parser/kwlist.h and add new keywords to the appropriate one
 * of the reserved-or-not-so-reserved keyword lists, below; search
 * this file for "Keyword category lists".
 */

/* ordinary key words in alphabetical order */
/* PGXC - added DISTRIBUTE, DIRECT, COORDINATOR, CLEAN,  NODE, BARRIER, SLICE, DATANODE */
%token <keyword> ABORT_P ABSOLUTE_P ACCESS ACCOUNT ACTION ADD_P ADMIN AFTER
	AGGREGATE ALGORITHM ALL ALSO ALTER ALWAYS ANALYSE ANALYZE AND ANY APP ARRAY AS ASC
        ASSERTION ASSIGNMENT ASYMMETRIC AT ATTRIBUTE AUDIT AUTHID AUTHORIZATION AUTOEXTEND AUTOMAPPED

	BACKWARD BARRIER BEFORE BEGIN_NON_ANOYBLOCK BEGIN_P BETWEEN BIGINT BINARY BINARY_DOUBLE BINARY_INTEGER BIT BLOB_P BOGUS
	BOOLEAN_P BOTH BUCKETS BY BYTEAWITHOUTORDER BYTEAWITHOUTORDERWITHEQUAL

	CACHE CALL CALLED CASCADE CASCADED CASE CAST CATALOG_P CHAIN CHAR_P
	CHARACTER CHARACTERISTICS CHECK CHECKPOINT CLASS CLEAN CLIENT CLIENT_MASTER_KEY CLIENT_MASTER_KEYS CLOB CLOSE
	CLUSTER COALESCE COLLATE COLLATION COLUMN COLUMN_ARGS COLUMN_ENCRYPTION_KEY COLUMN_ENCRYPTION_KEYS COLUMN_FUNCTION COMMENT COMMENTS COMMIT
	COMMITTED COMPACT COMPATIBLE_ILLEGAL_CHARS COMPLETE COMPRESS CONCURRENTLY CONDITION CONFIGURATION CONNECTION CONSTRAINT CONSTRAINTS
	CONTENT_P CONTINUE_P CONTVIEW CONVERSION_P COORDINATOR COORDINATORS COPY COST CREATE
	CROSS CSV CUBE CURRENT_P
	CURRENT_CATALOG CURRENT_DATE CURRENT_ROLE CURRENT_SCHEMA
	CURRENT_TIME CURRENT_TIMESTAMP CURRENT_USER CURSOR CYCLE

	DATA_P DATABASE DATAFILE DATANODE DATANODES DATATYPE_CL DATE_P DATE_FORMAT_P DAY_P DBCOMPATIBILITY_P DEALLOCATE DEC DECIMAL_P DECLARE DECODE DEFAULT DEFAULTS
	DEFERRABLE DEFERRED DEFINER DELETE_P DELIMITER DELIMITERS DELTA DELTAMERGE DESC DETERMINISTIC
/* PGXC_BEGIN */
	DICTIONARY DIRECT DIRECTORY DISABLE_P DISCARD DISTINCT DISTRIBUTE DISTRIBUTION DO DOCUMENT_P DOMAIN_P DOUBLE_P
/* PGXC_END */
	DROP DUPLICATE DISCONNECT

	EACH ELASTIC ELSE ENABLE_P ENCODING ENCRYPTED ENCRYPTED_VALUE ENCRYPTION ENCRYPTION_TYPE END_P ENFORCED ENUM_P ERRORS ESCAPE EOL ESCAPING EVERY EXCEPT EXCHANGE
	EXCLUDE EXCLUDED EXCLUDING EXCLUSIVE EXECUTE EXISTS EXPIRED_P EXPLAIN
	EXTENSION EXTERNAL EXTRACT

	FALSE_P FAMILY FAST FENCED FETCH FILEHEADER_P FILL_MISSING_FIELDS FILTER FIRST_P FIXED_P FLOAT_P FOLLOWING FOR FORCE FOREIGN FORMATTER FORWARD
	FREEZE FROM FULL FUNCTION FUNCTIONS

	GLOBAL GLOBAL_FUNCTION GRANT GRANTED GREATEST GROUP_P GROUPING_P

	HANDLER HAVING HDFSDIRECTORY HEADER_P HOLD HOUR_P

	IDENTIFIED IDENTITY_P IF_P IGNORE_EXTRA_DATA ILIKE IMMEDIATE IMMUTABLE IMPLICIT_P IN_P INCLUDE
	INCLUDING INCREMENT INCREMENTAL INDEX INDEXES INHERIT INHERITS INITIAL_P INITIALLY INITRANS INLINE_P

	INNER_P INOUT INPUT_P INSENSITIVE INSERT INSTEAD INT_P INTEGER INTERNAL
	INTERSECT INTERVAL INTO INVOKER IP IS ISNULL ISOLATION

	JOIN

	KEY KILL KEY_PATH KEY_STORE

	LABEL LANGUAGE LARGE_P LAST_P LC_COLLATE_P LC_CTYPE_P LEADING LEAKPROOF
	LEAST LESS LEFT LEVEL LIKE LIMIT LIST LISTEN LOAD LOCAL LOCALTIME LOCALTIMESTAMP
	LOCATION LOCK_P LOG_P LOGGING LOGIN_ANY LOGIN_FAILURE LOGIN_SUCCESS LOGOUT LOOP
	MAPPING MASKING MASTER MATCH MATERIALIZED MATCHED MAXEXTENTS MAXSIZE MAXTRANS MAXVALUE MERGE MINUS_P MINUTE_P MINVALUE MINEXTENTS MODE MODIFY_P MONTH_P MOVE MOVEMENT
	NAME_P NAMES NATIONAL NATURAL NCHAR NEXT NLSSORT NO NOCOMPRESS NOCYCLE NODE NOLOGGING NOMAXVALUE NOMINVALUE NONE
	NOT NOTHING NOTIFY NOTNULL NOWAIT NULL_P NULLIF NULLS_P NUMBER_P NUMERIC NUMSTR NVARCHAR2 NVL

	OBJECT_P OF OFF OFFSET OIDS ON ONLY OPERATOR OPTIMIZATION OPTION OPTIONS OR
	ORDER OUT_P OUTER_P OVER OVERLAPS OVERLAY OWNED OWNER

	PACKAGE PARSER PARTIAL PARTITION PARTITIONS PASSING PASSWORD PCTFREE PER_P PERCENT PERFORMANCE PERM PLACING PLAN PLANS POLICY POSITION
/* PGXC_BEGIN */
	POOL PRECEDING PRECISION PREFERRED PREFIX PRESERVE PREPARE PREPARED PRIMARY
/* PGXC_END */
	PRIVATE PRIOR PRIVILEGES PRIVILEGE PROCEDURAL PROCEDURE PROFILE

	QUERY QUOTE

	RANDOMIZED RANGE RAW READ REAL REASSIGN REBUILD RECHECK RECURSIVE REDISANYVALUE REF REFERENCES REFRESH REINDEX REJECT_P
	RELATIVE_P RELEASE RELOPTIONS REMOTE_P REMOVE RENAME REPEATABLE REPLACE REPLICA
	RESET RESIZE RESOURCE RESTART RESTRICT RETURN RETURNING RETURNS REUSE REVOKE RIGHT ROLE ROLES ROLLBACK ROLLUP
	ROW ROWNUM ROWS RULE

	SAVEPOINT SCHEMA SCROLL SEARCH SECOND_P SECURITY SELECT SEQUENCE SEQUENCES
	SERIALIZABLE SERVER SESSION SESSION_USER SET SETS SETOF SHARE SHIPPABLE SHOW SHUTDOWN
	SIMILAR SIMPLE SIZE SLICE SMALLDATETIME SMALLDATETIME_FORMAT_P SMALLINT SNAPSHOT SOME SOURCE_P SPACE SPILL SPLIT STABLE STANDALONE_P START
	STATEMENT STATEMENT_ID STATISTICS STDIN STDOUT STORAGE STORE_P STREAM STRICT_P STRIP_P SUBSTRING
	SYMMETRIC SYNONYM SYSDATE SYSID SYSTEM_P SYS_REFCURSOR

	TABLE TABLES TABLESAMPLE TABLESPACE TEMP TEMPLATE TEMPORARY TEXT_P THAN THEN TIME TIME_FORMAT_P TIMESTAMP TIMESTAMP_FORMAT_P TIMESTAMPDIFF TINYINT
	TO TRAILING TRANSACTION TREAT TRIGGER TRIM TRUE_P
	TRUNCATE TRUSTED TSFIELD TSTAG TSTIME TYPE_P TYPES_P

	UNBOUNDED UNCOMMITTED UNENCRYPTED UNION UNIQUE UNKNOWN UNLIMITED UNLISTEN UNLOCK UNLOGGED
	UNTIL UNUSABLE UPDATE USER USING

	VACUUM VALID VALIDATE VALIDATION VALIDATOR VALUE_P VALUES VARCHAR VARCHAR2 VARIADIC VARRAY VARYING VCGROUP
	VERBOSE VERIFY VERSION_P VIEW VOLATILE

	WEAK WHEN WHERE WHITESPACE_P WINDOW WITH WITHIN WITHOUT WORK WORKLOAD WRAPPER WRITE

	XML_P XMLATTRIBUTES XMLCONCAT XMLELEMENT XMLEXISTS XMLFOREST XMLPARSE
	XMLPI XMLROOT XMLSERIALIZE

	YEAR_P YES_P

	ZONE

/*
 * The grammar thinks these are keywords, but they are not in the kwlist.h
 * list and so can never be entered directly.  The filter in parser.c
 * creates these tokens when required.
 */
%token		NULLS_FIRST NULLS_LAST WITH_TIME INCLUDING_ALL
			RENAME_PARTITION
			PARTITION_FOR
			ADD_PARTITION
			DROP_PARTITION
			REBUILD_PARTITION
			MODIFY_PARTITION
			NOT_ENFORCED
			VALID_BEGIN
			DECLARE_CURSOR

/* Precedence: lowest to highest */
%nonassoc   PARTIAL_EMPTY_PREC
%nonassoc   CLUSTER
%nonassoc	SET				/* see relation_expr_opt_alias */
%left		UNION EXCEPT MINUS_P
%left		INTERSECT
%left		OR
%left		AND
%right		NOT
%right		'='
%nonassoc	'<' '>' CmpOp
%nonassoc	LIKE ILIKE SIMILAR
%nonassoc	ESCAPE
%nonassoc	OVERLAPS
%nonassoc	BETWEEN
%nonassoc	IN_P
%left		POSTFIXOP		/* dummy for postfix Op rules */
/*
 * To support target_el without AS, we must give IDENT an explicit priority
 * between POSTFIXOP and Op.  We can safely assign the same priority to
 * various unreserved keywords as needed to resolve ambiguities (this can't
 * have any bad effects since obviously the keywords will still behave the
 * same as if they weren't keywords).  We need to do this for PARTITION,
 * RANGE, ROWS to support opt_existing_window_name; and for RANGE, ROWS
 * so that they can follow a_expr without creating postfix-operator problems;
 * and for NULL so that it can follow b_expr in ColQualList without creating
 * postfix-operator problems.
 *
 * To support CUBE and ROLLUP in GROUP BY without reserving them, we give them
 * an explicit priority lower than '(', so that a rule with CUBE '(' will shift
 * rather than reducing a conflicting rule that takes CUBE as a function name.
 * Using the same precedence as IDENT seems right for the reasons given above.
 *
 * The frame_bound productions UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING
 * are even messier: since UNBOUNDED is an unreserved keyword (per spec!),
 * there is no principled way to distinguish these from the productions
 * a_expr PRECEDING/FOLLOWING.  We hack this up by giving UNBOUNDED slightly
 * lower precedence than PRECEDING and FOLLOWING.  At present this doesn't
 * appear to cause UNBOUNDED to be treated differently from other unreserved
 * keywords anywhere else in the grammar, but it's definitely risky.  We can
 * blame any funny behavior of UNBOUNDED on the SQL standard, though.
 */
%nonassoc	UNBOUNDED		/* ideally should have same precedence as IDENT */
%nonassoc	IDENT NULL_P PARTITION RANGE ROWS PRECEDING FOLLOWING CUBE ROLLUP
%left		Op OPERATOR		/* multi-character ops and user-defined operators */
%nonassoc	NOTNULL
%nonassoc	ISNULL
%nonassoc	IS				/* sets precedence for IS NULL, etc */
%left		'+' '-'
%left		'*' '/' '%'
%left		'^'
/* Unary Operators */
%left		AT				/* sets precedence for AT TIME ZONE */
%left		COLLATE
%right		UMINUS
%left		'[' ']'
%left		'(' ')'
%left		TYPECAST
%left		'.'
/*
 * These might seem to be low-precedence, but actually they are not part
 * of the arithmetic hierarchy at all in their use as JOIN operators.
 * We make them high-precedence to support their use as function names.
 * They wouldn't be given a precedence at all, were it not that we need
 * left-associativity among the JOIN rules themselves.
 */
%left		JOIN CROSS LEFT FULL RIGHT INNER_P NATURAL ENCRYPTED
/* kluge to keep xml_whitespace_option from causing shift/reduce conflicts */
%right		PRESERVE STRIP_P

%%

/*
 *	The target production for the whole parse.
 */
stmtblock:	stmtmulti
			{
				pg_yyget_extra(yyscanner)->parsetree = $1;
			}
		;

/* the thrashing around here is to discard "empty" statements... */
stmtmulti:	stmtmulti ';' stmt
				{
					if ($3 != NULL)
					{
						if (IsA($3, List))
						{
							$$ = list_concat($1, (List*)$3);
						}
						else
						{
						$$ = lappend($1, $3);
						}
					}
					else
						$$ = $1;
				}
			| stmt
				{
					if ($1 != NULL)
					{
						if (IsA($1, List))
						{
							$$ = (List*)$1;
						}
						else
						{
						$$ = list_make1($1);
						}
					}
					else
						$$ = NIL;
				}
		;
stmt :
			AlterAppWorkloadGroupMappingStmt
			| AlterCoordinatorStmt
			| AlterDatabaseStmt
			| AlterDatabaseSetStmt
			| AlterDataSourceStmt
			| AlterDefaultPrivilegesStmt
			| AlterDomainStmt
			| AlterEnumStmt
			| AlterExtensionStmt
			| AlterExtensionContentsStmt
			| AlterFdwStmt
			| AlterForeignServerStmt
			| AlterForeignTableStmt
			| AlterFunctionStmt
			| AlterProcedureStmt
			| AlterGroupStmt
			| AlterNodeGroupStmt
			| AlterNodeStmt
			| AlterObjectSchemaStmt
			| AlterOwnerStmt
			| AlterRlsPolicyStmt
			| AlterResourcePoolStmt
			| AlterSeqStmt
			| AlterTableStmt
			| AlterSystemStmt
			| AlterCompositeTypeStmt
			| AlterRoleSetStmt
			| AlterRoleStmt
			| AlterSessionStmt
			| AlterTSConfigurationStmt
			| AlterTSDictionaryStmt
			| AlterUserMappingStmt
			| AlterUserSetStmt
			| AlterUserStmt
			| AlterWorkloadGroupStmt
			| AnalyzeStmt
			| AnonyBlockStmt
			| BarrierStmt
			| CreateAppWorkloadGroupMappingStmt
			| CallFuncStmt
			| CheckPointStmt
			| CleanConnStmt
			| ClosePortalStmt
			| ClusterStmt
			| CommentStmt
			| ConstraintsSetStmt
			| CopyStmt
			| CreateAsStmt
			| CreateAssertStmt
			| CreateCastStmt
                        | CreateContQueryStmt
                        | CreateStreamStmt
			| CreateConversionStmt
			| CreateDomainStmt
			| CreateDirectoryStmt
			| CreateExtensionStmt
			| CreateFdwStmt
			| CreateForeignServerStmt
			| CreateForeignTableStmt
			| CreateDataSourceStmt
			| CreateFunctionStmt
			| CreateGroupStmt
			| CreateMatViewStmt
			| CreateNodeGroupStmt
			| CreateNodeStmt
			| CreateOpClassStmt
			| CreateOpFamilyStmt
			| AlterOpFamilyStmt
			| CreateRlsPolicyStmt
			| CreatePLangStmt
			| CreateProcedureStmt
            | CreateKeyStmt
			| CreatePolicyLabelStmt
			| CreateWeakPasswordDictionaryStmt
			| DropWeakPasswordDictionaryStmt
			| AlterPolicyLabelStmt
			| DropPolicyLabelStmt
            | CreateAuditPolicyStmt
            | AlterAuditPolicyStmt
            | DropAuditPolicyStmt
			| CreateMaskingPolicyStmt
			| AlterMaskingPolicyStmt
			| DropMaskingPolicyStmt
			| CreateResourcePoolStmt
			| CreateSchemaStmt
			| CreateSeqStmt
			| CreateStmt
			| CreateSynonymStmt
			| CreateTableSpaceStmt
			| CreateTrigStmt
			| CreateRoleStmt
			| CreateUserStmt
			| CreateUserMappingStmt
			| CreateWorkloadGroupStmt
			| CreatedbStmt
			| DeallocateStmt
			| DeclareCursorStmt
			| DefineStmt
			| DeleteStmt
			| DiscardStmt
			| DoStmt
			| DropAppWorkloadGroupMappingStmt
			| DropAssertStmt
			| DropCastStmt
			| DropDataSourceStmt
			| DropDirectoryStmt
			| DropFdwStmt
			| DropForeignServerStmt
			| DropGroupStmt
			| DropNodeGroupStmt
			| DropNodeStmt
			| DropOpClassStmt
			| DropOpFamilyStmt
			| DropOwnedStmt
			| DropRlsPolicyStmt
			| DropPLangStmt
			| DropResourcePoolStmt
			| DropRuleStmt
			| DropStmt
			| DropSynonymStmt
			| DropTableSpaceStmt
			| DropTrigStmt
			| DropRoleStmt
			| DropUserStmt
			| DropUserMappingStmt
			| DropWorkloadGroupStmt
			| DropdbStmt
			| ExecuteStmt
			| ExecDirectStmt
			| ExplainStmt
			| FetchStmt
			| GrantStmt
			| GrantRoleStmt
			| IndexStmt
			| InsertStmt
			| ListenStmt
			| RefreshMatViewStmt
			| LoadStmt
			| LockStmt
			| MergeStmt
			| NotifyStmt
			| PrepareStmt
			| ReassignOwnedStmt
			| ReindexStmt
			| RemoveAggrStmt
			| RemoveFuncStmt
			| RemoveOperStmt
			| RenameStmt
			| RevokeStmt
			| RevokeRoleStmt
			| RuleStmt
			| SecLabelStmt
			| SelectStmt
                        | ShutdownStmt
			| TransactionStmt
			| TruncateStmt
			| UnlistenStmt
			| UpdateStmt
			| VacuumStmt
			| VariableResetStmt
			| VariableSetStmt
			| VariableShowStmt
			| VerifyStmt
			| ViewStmt
			| /*EMPTY*/
				{ $$ = NULL; }
		;

/*****************************************************************************
 *
 * Create a new Postgres DBMS role
 *
 *****************************************************************************/

CreateRoleStmt:
			CREATE ROLE RoleId opt_with OptRoleList
				{
					CreateRoleStmt *n = makeNode(CreateRoleStmt);
					n->stmt_type = ROLESTMT_ROLE;
					if (!isRestoreMode)
						IsValidIdent($3);
					n->role = $3;
					n->options = $5;
					$$ = (Node *)n;
				}
		;


opt_with:	WITH									{}
			| /*EMPTY*/								{}
		;

/*
 * Options for CREATE ROLE and ALTER ROLE (also used by CREATE/ALTER USER
 * for backwards compatibility).  Note: the only option required by SQL99
 * is "WITH ADMIN name".
 */
OptRoleList:
			OptRoleList CreateOptRoleElem			{ $$ = lappend($1, $2); }
			| /* EMPTY */							{ $$ = NIL; }
		;

AlterOptRoleList:
			AlterOptRoleList AlterOptRoleElem		{ $$ = lappend($1, $2); }
			| /* EMPTY */							{ $$ = NIL; }
		;

password_string:
			Sconst
				{
					t_thrd.postgres_cxt.clear_key_memory = true;
					$$ = $1;
				}
			| IDENT
				{
					t_thrd.postgres_cxt.clear_key_memory = true;
					core_yy_extra_type yyextra = pg_yyget_extra(yyscanner)->core_yy_extra;
					if (yyextra.ident_quoted)
						$$ = $1;
					else
						ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("Password must be quoted")));
				}
namedata_string:
            Sconst
                {
                    $$ = pg_strtolower($1);
                }
            | IDENT
                {
                    core_yy_extra_type yyextra = pg_yyget_extra(yyscanner)->core_yy_extra;
                    if (yyextra.ident_quoted)
                        $$ = $1;
                    else
                        ereport(ERROR,
                            (errcode(ERRCODE_SYNTAX_ERROR),
                             errmsg("name data must be quoted")));
                }
		;
AlterOptRoleElem:
			PASSWORD password_string
				{
					$$ = makeDefElem("password",
									 (Node *)list_make1(makeStringConst($2, @2)));
				}
			| IDENTIFIED BY password_string
				{
					$$ = makeDefElem("password",
						(Node *)list_make1(makeStringConst($3, @3)));
				}
			| ENCRYPTED IDENTIFIED BY password_string
				{
					$$ = makeDefElem("encryptedPassword",
						(Node *)list_make1(makeStringConst($4, @4)));
				}
			| UNENCRYPTED IDENTIFIED BY password_string
				{
					$$ = makeDefElem("unencryptedPassword",
						(Node *)list_make1(makeStringConst($4, @4)));
				}
			| IDENTIFIED BY password_string REPLACE password_string
				{
					$$ = makeDefElem("password",
						(Node *)list_make2(makeStringConst($3, @3), makeStringConst($5, @5)));
				}
			| ENCRYPTED IDENTIFIED BY password_string REPLACE password_string
				{
					$$ = makeDefElem("encryptedPassword",
						(Node *)list_make2(makeStringConst($4, @4), makeStringConst($6, @6)));
				}
			| UNENCRYPTED IDENTIFIED BY password_string REPLACE password_string
				{
					$$ = makeDefElem("unencryptedPassword",
						(Node *)list_make2(makeStringConst($4, @4), makeStringConst($6, @6)));
				}
			| IDENTIFIED BY DISABLE_P
				{
					$$ = makeDefElem("password", NULL);
				}
			| PASSWORD DISABLE_P
				{
					$$ = makeDefElem("password", NULL);
				}
			| PASSWORD EXPIRED_P
				{
					$$ = makeDefElem("expired", (Node *)makeInteger(TRUE));
				}
			| PASSWORD password_string EXPIRED_P
				{
					$$ = makeDefElem("expiredPassword", (Node *)list_make1(makeStringConst($2, @2)));
				}
			| IDENTIFIED BY password_string EXPIRED_P
				{
					$$ = makeDefElem("expiredPassword", (Node *)list_make1(makeStringConst($3, @3)));
				}
			| ENCRYPTED PASSWORD password_string
				{
					$$ = makeDefElem("encryptedPassword",
									 (Node *)list_make1(makeStringConst($3, @3)));
				}
			| UNENCRYPTED PASSWORD password_string
				{
					$$ = makeDefElem("unencryptedPassword",
									 (Node *)list_make1(makeStringConst($3, @3)));
				}
			| DEFAULT TABLESPACE name
				{
					$$ = makeDefElem("tablespace", (Node *)makeString($3));
				}
			| PROFILE DEFAULT
				{
					$$ = makeDefElem("profile", NULL);
				}
			| PROFILE name
				{
					$$ = makeDefElem("profile",
									 (Node *)list_make1(makeStringConst($2, @2)));
				}
			| INHERIT
				{
					$$ = makeDefElem("inherit", (Node *)makeInteger(TRUE));
				}
			| CONNECTION LIMIT SignedIconst
				{
					$$ = makeDefElem("connectionlimit", (Node *)makeInteger($3));
				}
			| RESOURCE POOL namedata_string
				{
					$$ = makeDefElem("respool", (Node *)makeString($3));
				}
			| NODE GROUP_P pgxcgroup_name
			    {
			        $$ = makeDefElem("node_group", (Node *)makeString($3));
			    }
			| USER GROUP_P namedata_string
				{
					$$ = makeDefElem("parent", (Node *)makeString($3));
				}
			| USER GROUP_P DEFAULT	
				{
					$$ = makeDefElem("parent_default", NULL);
				}
			| PERM SPACE Sconst
				{
					$$ = makeDefElem("space_limit", (Node *)makeString($3));
				}
			| TEMP SPACE Sconst
				{
					$$ = makeDefElem("temp_space_limit", (Node *)makeString($3));
				}
			| SPILL SPACE Sconst
				{
					$$ = makeDefElem("spill_space_limit", (Node *)makeString($3));
				}
			/* VALID and BEGIN is treated as a token to avoid confilict with BEGIN TRANSACTIOn and BEGIN ANONYMOUD BLOCK */
			| VALID_BEGIN Sconst
				{
					$$ = makeDefElem("validBegin", (Node *)makeString($2));
				}
			| VALID UNTIL Sconst
				{
					$$ = makeDefElem("validUntil", (Node *)makeString($3));
				}
		/*	Supported but not documented for roles, for use by ALTER GROUP. */
			| USER name_list
				{
					$$ = makeDefElem("rolemembers", (Node *)$2);
				}
			| IDENT
				{
					/*
					 * We handle identifiers that aren't parser keywords with
					 * the following special-case codes, to avoid bloating the
					 * size of the main parser.
					 */
					if (strcmp($1, "createrole") == 0)
						$$ = makeDefElem("createrole", (Node *)makeInteger(TRUE));
					else if (strcmp($1, "nocreaterole") == 0)
						$$ = makeDefElem("createrole", (Node *)makeInteger(FALSE));
					else if (strcmp($1, "replication") == 0)
						$$ = makeDefElem("isreplication", (Node *)makeInteger(TRUE));
					else if (strcmp($1, "noreplication") == 0)
						$$ = makeDefElem("isreplication", (Node *)makeInteger(FALSE));
					/* add audit admin privilege */
					else if (strcmp($1, "auditadmin") == 0)
						$$ = makeDefElem("isauditadmin", (Node *)makeInteger(TRUE));
					else if (strcmp($1, "noauditadmin") == 0)
						$$ = makeDefElem("isauditadmin", (Node *)makeInteger(FALSE));
					/* END <audit> */
					else if (strcmp($1, "sysadmin") == 0)
						$$ = makeDefElem("issystemadmin", (Node *)makeInteger(TRUE));
					else if (strcmp($1, "nosysadmin") == 0)
						$$ = makeDefElem("issystemadmin", (Node *)makeInteger(FALSE));
					/* Add system monitor privilege, operator privilege. */
					else if (strcmp($1, "monadmin") == 0)
						$$ = makeDefElem("ismonitoradmin", (Node *)makeInteger(TRUE));
					else if (strcmp($1, "nomonadmin") == 0)
						$$ = makeDefElem("ismonitoradmin", (Node *)makeInteger(FALSE));
					else if (strcmp($1, "opradmin") == 0)
						$$ = makeDefElem("isoperatoradmin", (Node *)makeInteger(TRUE));
					else if (strcmp($1, "noopradmin") == 0)
						$$ = makeDefElem("isoperatoradmin", (Node *)makeInteger(FALSE));
					else if (strcmp($1, "poladmin") == 0)
						$$ = makeDefElem("ispolicyadmin", (Node *)makeInteger(TRUE));
					else if (strcmp($1, "nopoladmin") == 0)
						$$ = makeDefElem("ispolicyadmin", (Node *)makeInteger(FALSE));
					/* End */
					else if (strcmp($1, "vcadmin") == 0)
						$$ = makeDefElem("isvcadmin", (Node *)makeInteger(TRUE));
					else if (strcmp($1, "novcadmin") == 0)
						$$ = makeDefElem("isvcadmin", (Node *)makeInteger(FALSE));
					else if (strcmp($1, "createdb") == 0)
						$$ = makeDefElem("createdb", (Node *)makeInteger(TRUE));
					else if (strcmp($1, "nocreatedb") == 0)
						$$ = makeDefElem("createdb", (Node *)makeInteger(FALSE));
					else if (strcmp($1, "useft") == 0)
						$$ = makeDefElem("useft", (Node *)makeInteger(TRUE));
					else if (strcmp($1, "nouseft") == 0)
						$$ = makeDefElem("useft", (Node *)makeInteger(FALSE));
					else if (strcmp($1, "login") == 0)
						$$ = makeDefElem("canlogin", (Node *)makeInteger(TRUE));
					else if (strcmp($1, "nologin") == 0)
						$$ = makeDefElem("canlogin", (Node *)makeInteger(FALSE));
					else if (strcmp($1, "independent") == 0)
						$$ = makeDefElem("independent", (Node *)makeInteger(TRUE));
					else if (strcmp($1, "noindependent") == 0)
						$$ = makeDefElem("independent", (Node *)makeInteger(FALSE));
					else if (strcmp($1, "persistence") == 0)
						$$ = makeDefElem("persistence", (Node *)makeInteger(TRUE));
					else if (strcmp($1, "nopersistence") == 0)
						$$ = makeDefElem("persistence", (Node *)makeInteger(FALSE));
					else if (strcmp($1, "noinherit") == 0)
					{
						/*
						 * Note that INHERIT is a keyword, so it's handled by main parser, but
						 * NOINHERIT is handled here.
						 */
						$$ = makeDefElem("inherit", (Node *)makeInteger(FALSE));
					}
					else if (strcmp($1, "pguser") == 0)
					{
						$$ = makeDefElem("pguser", (Node *)makeInteger(TRUE));
					}
					else
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("unrecognized role option \"%s\"", $1),
									 parser_errposition(@1)));
				}
		;

CreateOptRoleElem:
			AlterOptRoleElem			{ $$ = $1; }
			/* The following are not supported by ALTER ROLE/USER/GROUP */
			| SYSID Iconst
				{
					$$ = makeDefElem("sysid", (Node *)makeInteger($2));
				}
			| ADMIN name_list
				{
					$$ = makeDefElem("adminmembers", (Node *)$2);
				}
			| ROLE name_list
				{
					$$ = makeDefElem("rolemembers", (Node *)$2);
				}
			| IN_P ROLE name_list
				{
					$$ = makeDefElem("addroleto", (Node *)$3);
				}
			| IN_P GROUP_P name_list
				{
					$$ = makeDefElem("addroleto", (Node *)$3);
				}
		;


/*****************************************************************************
 *
 * Create a new Postgres DBMS user (role with implied login ability)
 *
 *****************************************************************************/

CreateUserStmt:
			CREATE USER RoleId opt_with OptRoleList
				{
					CreateRoleStmt *n = makeNode(CreateRoleStmt);
					n->stmt_type = ROLESTMT_USER;
					IsValidIdent($3);
					n->role = $3;
					n->options = $5;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 * Alter a postgresql DBMS role
 *
 *****************************************************************************/

AlterRoleStmt:
			ALTER ROLE RoleId opt_with AlterOptRoleList
				 {
					AlterRoleStmt *n = makeNode(AlterRoleStmt);
					n->role = $3;
					n->action = +1;	/* add, if there are members */
					n->options = $5;
					n->lockstatus = DO_NOTHING;
					$$ = (Node *)n;
				 }
			| ALTER ROLE RoleId opt_with ACCOUNT LOCK_P
				{
					AlterRoleStmt *n = makeNode(AlterRoleStmt);
					n->role = $3;
					n->action = +1;	/* add, if there are members */
					n->options = NIL;
					n->lockstatus = LOCK_ROLE;
					$$ = (Node *)n;
				}
			| ALTER ROLE RoleId opt_with ACCOUNT UNLOCK
				{
					AlterRoleStmt *n = makeNode(AlterRoleStmt);
					n->role = $3;
					n->action = +1;	/* add, if there are members */
					n->options = NIL;
					n->lockstatus = UNLOCK_ROLE;
					$$ = (Node *)n;
				}
		;

opt_in_database:
			   /* EMPTY */					{ $$ = NULL; }
			| IN_P DATABASE database_name	{ $$ = $3; }
		;

AlterRoleSetStmt:
			ALTER ROLE RoleId opt_in_database SetResetClause
				{
					AlterRoleSetStmt *n = makeNode(AlterRoleSetStmt);
					n->role = $3;
					n->database = $4;
					n->setstmt = $5;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 * Alter a postgresql DBMS user
 *
 *****************************************************************************/

AlterUserStmt:
			ALTER USER RoleId opt_with AlterOptRoleList
				 {
					AlterRoleStmt *n = makeNode(AlterRoleStmt);
					n->role = $3;
					n->action = +1;	/* add, if there are members */
					n->options = $5;
					n->lockstatus = DO_NOTHING;
					$$ = (Node *)n;
				 }
			| ALTER USER RoleId opt_with ACCOUNT LOCK_P
				{
					AlterRoleStmt *n = makeNode(AlterRoleStmt);
					n->role = $3;
					n->action = +1;	/* add, if there are members */
					n->options = NIL;
					n->lockstatus = LOCK_ROLE;
					$$ = (Node *)n;
				}
			| ALTER USER RoleId opt_with ACCOUNT UNLOCK
				{
					AlterRoleStmt *n = makeNode(AlterRoleStmt);
					n->role = $3;
					n->action = +1;	/* add, if there are members */
					n->options = NIL;
					n->lockstatus = UNLOCK_ROLE;
					$$ = (Node *)n;
				}
		;


AlterUserSetStmt:
			ALTER USER RoleId opt_in_database SetResetClause
				{
					AlterRoleSetStmt *n = makeNode(AlterRoleSetStmt);
					n->role = $3;
					n->database = $4;
					n->setstmt = $5;
					$$ = (Node *)n;
				}
			;


/*****************************************************************************
 *
 * Drop a postgresql DBMS role
 *
 * XXX Ideally this would have CASCADE/RESTRICT options, but since a role
 * might own objects in multiple databases, there is presently no way to
 * implement either cascading or restricting.  Caveat DBA.
 *****************************************************************************/

DropRoleStmt:
			DROP ROLE name_list
				{
					DropRoleStmt *n = makeNode(DropRoleStmt);
					n->missing_ok = FALSE;
					n->roles = $3;
					$$ = (Node *)n;
				}
			| DROP ROLE IF_P EXISTS name_list
				{
					DropRoleStmt *n = makeNode(DropRoleStmt);
					n->missing_ok = TRUE;
					n->roles = $5;
					$$ = (Node *)n;
				}
			;

/*****************************************************************************
 *
 * Drop a postgresql DBMS user
 *
 * XXX Ideally this would have CASCADE/RESTRICT options, but since a user
 * might own objects in multiple databases, there is presently no way to
 * implement either cascading or restricting.  Caveat DBA.
 *****************************************************************************/

DropUserStmt:
			DROP USER name_list opt_drop_behavior
				{
					DropRoleStmt *n = makeNode(DropRoleStmt);
					n->missing_ok = FALSE;
					n->is_user = TRUE;
					n->roles = $3;
					n->behavior = $4;
					$$ = (Node *)n;
				}
			| DROP USER IF_P EXISTS name_list opt_drop_behavior
				{
					DropRoleStmt *n = makeNode(DropRoleStmt);
					n->roles = $5;
					n->missing_ok = true;
					n->is_user = TRUE;
					n->behavior = $6;
					n->is_user = TRUE;
					n->behavior = $6;
					$$ = (Node *)n;
				}
			;

/*****************************************************************************
 *
 * Create a postgresql group (role without login ability)
 *
 *****************************************************************************/

CreateGroupStmt:
			CREATE GROUP_P RoleId opt_with OptRoleList
				{
					CreateRoleStmt *n = makeNode(CreateRoleStmt);
					n->stmt_type = ROLESTMT_GROUP;
					n->role = $3;
					n->options = $5;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 * Alter a postgresql group
 *
 *****************************************************************************/

AlterGroupStmt:
			ALTER GROUP_P RoleId add_drop USER name_list
				{
					AlterRoleStmt *n = makeNode(AlterRoleStmt);
					n->role = $3;
					n->action = $4;
					n->options = list_make1(makeDefElem("rolemembers",
														(Node *)$6));
					$$ = (Node *)n;
				}
		;

add_drop:	ADD_P									{ $$ = +1; }
			| DROP									{ $$ = -1; }
		;

/*****************************************************************************
 *
 * Alter SESSION
 *
 *****************************************************************************/

AlterSessionStmt:
			ALTER SESSION SET set_rest
				{
					VariableSetStmt *n = $4;
					n->is_local = false;
					$$ = (Node *) n;
				}
			;

/* "alter system" */
/*****************************************************************************
 *
 * Alter SYSTEM 
 * (1. kill a session by "select pg_terminate_backend(pid)", so it only needs a SelectStmt node.)
 * (2. disconnect a session. unsupported currently.)
 * (3. set system parameter to, this is used to change configuration parameters persistently.)
 *
 *****************************************************************************/

AlterSystemStmt:
			ALTER SYSTEM_P KILL SESSION Sconst altersys_option
				{
					SelectStmt *n = NULL;
					List *pid = NULL;

					pid = get_func_args($5);

					n = makeNode(SelectStmt);
					n->distinctClause = NIL;
					n->targetList = make_action_func(pid);
					n->intoClause = NULL;
					n->fromClause = NIL;
					n->whereClause = NULL;
					n->groupClause = NIL;
					n->havingClause = NULL;
					n->windowClause = NIL;
					$$ = (Node *)n;
				}

			| ALTER SYSTEM_P DISCONNECT SESSION Sconst altersys_option
				{
					ereport(ERROR,
					    (errcode(ERRCODE_UNDEFINED_OBJECT),
					     errmsg("unsupported action \"DISCONNECT\" for statement \" alter system \"")));
				}

			| ALTER SYSTEM_P SET generic_set
				{
#ifdef ENABLE_MULTIPLE_NODES
					ereport(ERROR,
					    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					     errmsg("ALTER SYSTEM SET is not supported in distributed mode.")));
#else
					AlterSystemStmt *n = makeNode(AlterSystemStmt);
					n->setstmt = $4;
					$$ = (Node *)n;
#endif
				}
			;

altersys_option:
			IMMEDIATE			{/* empty */}
			|				{/* empty */}
			;

/*****************************************************************************
 *
 * Drop a postgresql group
 *
 * XXX see above notes about cascading DROP USER; groups have same problem.
 *****************************************************************************/

DropGroupStmt:
			DROP GROUP_P name_list
				{
					DropRoleStmt *n = makeNode(DropRoleStmt);
					n->missing_ok = FALSE;
					n->roles = $3;
					$$ = (Node *)n;
				}
			| DROP GROUP_P IF_P EXISTS name_list
				{
					DropRoleStmt *n = makeNode(DropRoleStmt);
					n->missing_ok = TRUE;
					n->roles = $5;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 * Manipulate a schema
 *
 *****************************************************************************/

CreateSchemaStmt:
			CREATE SCHEMA OptSchemaName AUTHORIZATION RoleId OptSchemaEltList
				{
					CreateSchemaStmt *n = makeNode(CreateSchemaStmt);
					/* One can omit the schema name or the authorization id. */
					if ($3 != NULL)
						n->schemaname = $3;
					else
						n->schemaname = $5;
					n->authid = $5;
					n->schemaElts = $6;
					$$ = (Node *)n;
				}
			| CREATE SCHEMA ColId OptSchemaEltList
				{
					CreateSchemaStmt *n = makeNode(CreateSchemaStmt);
					/* ...but not both */
					n->schemaname = $3;
					n->authid = NULL;
					n->schemaElts = $4;
					$$ = (Node *)n;
				}
		;

OptSchemaName:
			ColId									{ $$ = $1; }
			| /* EMPTY */							{ $$ = NULL; }
		;

OptSchemaEltList:
			OptSchemaEltList schema_stmt			{ $$ = lappend($1, $2); }
			| /* EMPTY */							{ $$ = NIL; }
		;

/*
 *	schema_stmt are the ones that can show up inside a CREATE SCHEMA
 *	statement (in addition to by themselves).
 */
schema_stmt:
			CreateStmt
            | CreateKeyStmt
			| IndexStmt
			| CreateSeqStmt
			| CreateTrigStmt
			| GrantStmt
			| ViewStmt
		;


/*****************************************************************************
 *
 * Set PG internal variable
 *	  SET name TO 'var_value'
 * Include SQL92 syntax (thomas 1997-10-22):
 *	  SET TIME ZONE 'var_value'
 *
 *****************************************************************************/

VariableSetStmt:
			SET set_rest
				{
					VariableSetStmt *n = $2;
					n->is_local = false;
					$$ = (Node *) n;
				}
			| SET LOCAL set_rest
				{
					VariableSetStmt *n = $3;
					n->is_local = true;
					$$ = (Node *) n;
				}
			| SET SESSION set_rest
				{
					VariableSetStmt *n = $3;
					n->is_local = false;
					$$ = (Node *) n;
				}
		;

set_rest:
			TRANSACTION transaction_mode_list
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_MULTI;
					n->name = "TRANSACTION";
					n->args = $2;
					$$ = n;
				}
			| SESSION CHARACTERISTICS AS TRANSACTION transaction_mode_list
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_MULTI;
					n->name = "SESSION CHARACTERISTICS";
					n->args = $5;
					$$ = n;
				}
			| set_rest_more
			;

generic_set:
			var_name TO var_list
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = $1;
					n->args = $3;
					/* if we are setting role, we switch to the new syntax which check the password of role */
					if(!strcmp("role", n->name) || !pg_strcasecmp("session_authorization", n->name))
					{
						ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 	errmsg("\"SET %s TO rolename\" not yet supported", n->name),
								 	errhint("Use \"SET %s rolename\" clauses.", n->name)));
					}
					else
					{
						n->kind = VAR_SET_VALUE;
					}
					$$ = n;
				}
			| var_name '=' var_list
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = $1;
					n->args = $3;
					/* if we are setting role, we switch to the new syntax which check the password of role */
					if(!strcmp("role", n->name) || !pg_strcasecmp("session_authorization", n->name))
					{
						ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("\"SET %s = rolename\" not yet supported", n->name),
									 errhint("Use \"SET %s rolename\" clauses.", n->name)));
					}
					else
					{
						n->kind = VAR_SET_VALUE;
					}
					$$ = n;
				}
			| var_name TO DEFAULT
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_DEFAULT;
					n->name = $1;
					$$ = n;
				}
			| var_name '=' DEFAULT
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_DEFAULT;
					n->name = $1;
					$$ = n;
				}
			| CURRENT_SCHEMA TO var_list
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "current_schema";
					n->args = $3;
					$$ = n;
				}
			| CURRENT_SCHEMA '=' var_list
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "current_schema";
					n->args = $3;
					$$ = n;
				}
			| CURRENT_SCHEMA TO DEFAULT
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_DEFAULT;
					n->name = "current_schema";
					$$ = n;
				}
			| CURRENT_SCHEMA '=' DEFAULT
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_DEFAULT;
					n->name = "current_schema";
					$$ = n;
				}
                ;

set_rest_more:  /* Generic SET syntaxes: */
            generic_set
	            {
	                $$ = $1;
	            }
			| var_name FROM CURRENT_P
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_CURRENT;
					n->name = $1;
					$$ = n;
				}
			/* Special syntaxes mandated by SQL standard: */
			| TIME ZONE zone_value
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "timezone";
					if ($3 != NULL)
						n->args = list_make1($3);
					else
						n->kind = VAR_SET_DEFAULT;
					$$ = n;
				}
			| CATALOG_P Sconst
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("current database cannot be changed"),
							 parser_errposition(@2)));
					$$ = NULL; /*not reached*/
				}
			| SCHEMA Sconst
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "search_path";
					n->args = list_make1(makeStringConst($2, @2));
					$$ = n;
				}
			| NAMES opt_encoding
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "client_encoding";
					if ($2 != NULL)
						n->args = list_make1(makeStringConst($2, @2));
					else
						n->kind = VAR_SET_DEFAULT;
					$$ = n;
				}
			| ROLE ColId_or_Sconst
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_ROLEPWD;
					n->name = "role";
					n->args = list_make1(makeStringConst($2, @2));
					$$ = n;
				}
			| ROLE ColId_or_Sconst PASSWORD password_string
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_ROLEPWD;
					n->name = "role";
					n->args = list_make2(makeStringConst($2, @2), makeStringConst($4, @4));
					$$ = n;
				}
			| SESSION AUTHORIZATION ColId_or_Sconst PASSWORD password_string
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_ROLEPWD;
					n->name = "session_authorization";
					n->args = list_make2(makeStringConst($3, @3), makeStringConst($5,@5));
					$$ = n;
				}
			| SESSION AUTHORIZATION DEFAULT
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_DEFAULT;
					n->name = "session_authorization";
					$$ = n;
				}
			| XML_P OPTION document_or_content
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_VALUE;
					n->name = "xmloption";
					n->args = list_make1(makeStringConst(const_cast<char *>($3 == XMLOPTION_DOCUMENT ? "DOCUMENT" : "CONTENT"), @3));
					$$ = n;
				}
			/* Special syntaxes invented by PostgreSQL: */
			| TRANSACTION SNAPSHOT Sconst
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_SET_MULTI;
					n->name = "TRANSACTION SNAPSHOT";
					n->args = list_make1(makeStringConst($3, @3));
					$$ = n;
				}
		;

var_name:	ColId								{ $$ = $1; }
			| var_name '.' ColId
				{
					int rc = EOK;						  
					int len = strlen($1) + strlen($3) + 2;
					$$ = (char *)palloc(len);
					rc = sprintf_s($$, len, "%s.%s", $1, $3);
					securec_check_ss(rc, "\0", "\0");
				}
		;

var_list:	var_value								{ $$ = list_make1($1); }
			| var_list ',' var_value				{ $$ = lappend($1, $3); }
		;

var_value:	opt_boolean_or_string
				{ $$ = makeStringConst($1, @1); }
			| NumericOnly
				{ $$ = makeAConst($1, @1); }
		;

iso_level:	READ UNCOMMITTED						{ $$ = "read uncommitted"; }
			| READ COMMITTED						{ $$ = "read committed"; }
			| REPEATABLE READ						{ $$ = "repeatable read"; }
			| SERIALIZABLE							{ $$ = "serializable"; }
		;

opt_boolean_or_string:
			TRUE_P									{ $$ = "true"; }
			| FALSE_P								{ $$ = "false"; }
			| ON									{ $$ = "on"; }
			/*
			 * OFF is also accepted as a boolean value, but is handled
			 * by the ColId rule below. The action for booleans and strings
			 * is the same, so we don't need to distinguish them here.
			 */
			| ColId_or_Sconst						{ $$ = $1; }
		;

/* Timezone values can be:
 * - a string such as 'pst8pdt'
 * - an identifier such as "pst8pdt"
 * - an integer or floating point number
 * - a time interval per SQL99
 * ColId gives reduce/reduce errors against ConstInterval and LOCAL,
 * so use IDENT (meaning we reject anything that is a key word).
 */
zone_value:
			Sconst
				{
					$$ = makeStringConst($1, @1);
				}
			| IDENT
				{
					$$ = makeStringConst($1, @1);
				}
			| ConstInterval Sconst opt_interval
				{
					TypeName *t = $1;
					if ($3 != NIL)
					{
						A_Const *n = (A_Const *) linitial($3);
						if ((n->val.val.ival & ~(INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE))) != 0)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("time zone interval must be HOUR or HOUR TO MINUTE"),
									 parser_errposition(@3)));
					}
					t->typmods = $3;
					$$ = makeStringConstCast($2, @2, t);
				}
			| ConstInterval '(' Iconst ')' Sconst opt_interval
				{
					TypeName *t = $1;
					if ($6 != NIL)
					{
						A_Const *n = (A_Const *) linitial($6);
						if ((n->val.val.ival & ~(INTERVAL_MASK(HOUR) | INTERVAL_MASK(MINUTE))) != 0)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("time zone interval must be HOUR or HOUR TO MINUTE"),
									 parser_errposition(@6)));
						if (list_length($6) != 1)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("interval precision specified twice"),
									 parser_errposition(@1)));
						t->typmods = lappend($6, makeIntConst($3, @3));
					}
					else
						t->typmods = list_make2(makeIntConst(INTERVAL_FULL_RANGE, -1),
												makeIntConst($3, @3));
					$$ = makeStringConstCast($5, @5, t);
				}
			| NumericOnly							{ $$ = makeAConst($1, @1); }
			| DEFAULT								{ $$ = NULL; }
			| LOCAL									{ $$ = NULL; }
		;

opt_encoding:
			Sconst									{ $$ = $1; }
			| DEFAULT								{ $$ = NULL; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

ColId_or_Sconst:
			ColId									{ $$ = $1; }
			| Sconst								{ $$ = $1; }
		;

VariableResetStmt:
			RESET var_name
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET;
					n->name = $2;
					$$ = (Node *) n;
				}
			| RESET TIME ZONE
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET;
					n->name = "timezone";
					$$ = (Node *) n;
				}
			| RESET CURRENT_SCHEMA
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET;
					n->name = "current_schema";
					$$ = (Node *) n;
				}
			| RESET TRANSACTION ISOLATION LEVEL
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET;
					n->name = "transaction_isolation";
					$$ = (Node *) n;
				}
			| RESET SESSION AUTHORIZATION
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET;
					n->name = "session_authorization";
					$$ = (Node *) n;
				}
			| RESET ALL
				{
					VariableSetStmt *n = makeNode(VariableSetStmt);
					n->kind = VAR_RESET_ALL;
					$$ = (Node *) n;
				}
		;

/* SetResetClause allows SET or RESET without LOCAL */
SetResetClause:
			SET set_rest					{ $$ = $2; }
			| VariableResetStmt				{ $$ = (VariableSetStmt *) $1; }
		;

/* SetResetClause allows SET or RESET without LOCAL */
FunctionSetResetClause:
			SET set_rest_more				{ $$ = $2; }
			| VariableResetStmt				{ $$ = (VariableSetStmt *) $1; }
		;


VariableShowStmt:
			SHOW var_name
				{
					VariableShowStmt *n = makeNode(VariableShowStmt);
					n->name = $2;
					$$ = (Node *) n;
				}
			| SHOW CURRENT_SCHEMA
				{
					VariableShowStmt *n = makeNode(VariableShowStmt);
					n->name = "current_schema";
					$$ = (Node *) n;
				}
			| SHOW TIME ZONE
				{
					VariableShowStmt *n = makeNode(VariableShowStmt);
					n->name = "timezone";
					$$ = (Node *) n;
				}
			| SHOW TRANSACTION ISOLATION LEVEL
				{
					VariableShowStmt *n = makeNode(VariableShowStmt);
					n->name = "transaction_isolation";
					$$ = (Node *) n;
				}
			| SHOW SESSION AUTHORIZATION
				{
					VariableShowStmt *n = makeNode(VariableShowStmt);
					n->name = "session_authorization";
					$$ = (Node *) n;
				}
			| SHOW ALL
				{
					VariableShowStmt *n = makeNode(VariableShowStmt);
					n->name = "all";
					$$ = (Node *) n;
				}
		;


ConstraintsSetStmt:
			SET CONSTRAINTS constraints_set_list constraints_set_mode
				{
					ConstraintsSetStmt *n = makeNode(ConstraintsSetStmt);
					n->constraints = $3;
					n->deferred = $4;
					$$ = (Node *) n;
				}
		;

constraints_set_list:
			ALL										{ $$ = NIL; }
			| qualified_name_list					{ $$ = $1; }
		;

constraints_set_mode:
			DEFERRED								{ $$ = TRUE; }
			| IMMEDIATE								{ $$ = FALSE; }
		;

/*****************************************************************************
 *
 * SHUTDOWN STATEMENT
 *
 *****************************************************************************/
ShutdownStmt:
                        SHUTDOWN
                                {
                                       ShutdownStmt *n = makeNode(ShutdownStmt);
                                       n->mode = NULL;
                                       $$ = (Node *) n;
                                }
                        | SHUTDOWN var_name
                                {
                                       ShutdownStmt *n = makeNode(ShutdownStmt);
                                       n->mode = $2;
                                       $$ = (Node *) n;
                                }
                ;

/*
 * Checkpoint statement
 */
CheckPointStmt:
			CHECKPOINT
				{
					CheckPointStmt *n = makeNode(CheckPointStmt);
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 * DISCARD { ALL | TEMP | PLANS }
 *
 *****************************************************************************/

DiscardStmt:
			DISCARD ALL
				{
					DiscardStmt *n = makeNode(DiscardStmt);
					n->target = DISCARD_ALL;
					$$ = (Node *) n;
				}
			| DISCARD TEMP
				{
					DiscardStmt *n = makeNode(DiscardStmt);
					n->target = DISCARD_TEMP;
					$$ = (Node *) n;
				}
			| DISCARD TEMPORARY
				{
					DiscardStmt *n = makeNode(DiscardStmt);
					n->target = DISCARD_TEMP;
					$$ = (Node *) n;
				}
			| DISCARD PLANS
				{
					DiscardStmt *n = makeNode(DiscardStmt);
					n->target = DISCARD_PLANS;
					$$ = (Node *) n;
				}
		;


/*****************************************************************************
 *
 *	ALTER [ TABLE | INDEX | SEQUENCE | VIEW | MATERIALIZED VIEW| STREAM] variations
 *
 * Note: we accept all subcommands for each of the five variants, and sort
 * out what's really legal at execution time.
 *****************************************************************************/

AlterTableStmt:
		ALTER TABLE relation_expr MODIFY_P '(' modify_column_cmds ')'
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $3;
					n->cmds = $6;
					n->relkind = OBJECT_TABLE;
					n->missing_ok = false;
					n->need_rewrite_sql = false;
					$$ = (Node *)n;
				}
		|	ALTER TABLE relation_expr ADD_P '(' add_column_cmds ')'
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $3;
					n->cmds = $6;
					n->relkind = OBJECT_TABLE;
					n->missing_ok = false;
					n->need_rewrite_sql = false;
					$$ = (Node *)n;
				}
		/* REDISANYVALUE key value only used in tsdb redis command, it is used in OM code */
		|	ALTER TABLE relation_expr REDISANYVALUE
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $3;
					n->cmds = NIL;
					n->relkind = OBJECT_TABLE;
					n->missing_ok = false;
					n->need_rewrite_sql = false;
					$$ = (Node *)n;
				}
		/*
		 * ALTER TABLE IF_P EXISTS MODIFY_P '(' modify_column_cmds ')'
		 */
		|	ALTER TABLE IF_P EXISTS relation_expr MODIFY_P '(' modify_column_cmds ')'
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $5;
					n->cmds = $8;
					n->relkind = OBJECT_TABLE;
					n->missing_ok = true;
					n->need_rewrite_sql = false;
					$$ = (Node *)n;
				}
		/*
		 * ALTER TABLE IF_P EXISTS ADD_P '(' add_column_cmds ')'
		 */
		|	ALTER TABLE IF_P EXISTS relation_expr ADD_P '(' add_column_cmds ')'
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $5;
					n->cmds = $8;
					n->relkind = OBJECT_TABLE;
					n->missing_ok = true;
					n->need_rewrite_sql = false;
					$$ = (Node *)n;
				}
		|	ALTER TABLE relation_expr alter_table_or_partition
				{
					if ($4->length == 1 && ((AlterTableCmd*)lfirst($4->head))->subtype == AT_RebuildAllIndexOnPartition)
					{
						ReindexStmt *n = makeNode(ReindexStmt);
						n->kind = OBJECT_TABLE_PARTITION;
						n->relation = $3;
						n->name = ((AlterTableCmd*)lfirst($4->head))->name;
						$$ = (Node *)n;
					}
					else
					{
						AlterTableStmt *n = makeNode(AlterTableStmt);
						n->relation = $3;
						n->cmds = $4;
						n->relkind = OBJECT_TABLE;
						n->missing_ok = false;
						n->need_rewrite_sql = false;
						$$ = (Node *)n;
					}
				}
		|	ALTER TABLE IF_P EXISTS relation_expr alter_table_or_partition
				{
					if ($6->length == 1 && ((AlterTableCmd*)lfirst($6->head))->subtype == AT_RebuildAllIndexOnPartition)
					{
						ReindexStmt *n = makeNode(ReindexStmt);
						n->kind = OBJECT_TABLE_PARTITION;
						n->relation = $5;
						n->name = ((AlterTableCmd*)lfirst($6->head))->name;
						$$ = (Node *)n;
					}
					else
					{
						AlterTableStmt *n = makeNode(AlterTableStmt);
						n->relation = $5;
						n->cmds = $6;
						n->relkind = OBJECT_TABLE;
						n->missing_ok = true;
						n->need_rewrite_sql = false;
						$$ = (Node *)n;
					}
				}
		|	ALTER INDEX qualified_name alter_table_or_partition
				{
					if ($4->length == 1 && ((AlterTableCmd*)lfirst($4->head))->subtype == AT_RebuildIndex)
					{
						ReindexStmt *n = makeNode(ReindexStmt);
						n->kind = OBJECT_INDEX;
						n->relation = $3;
						n->name = NULL;
						$$ = (Node *)n;
					}
					else if ($4->length == 1 && ((AlterTableCmd*)lfirst($4->head))->subtype == AT_RebuildIndexPartition)
					{
						ReindexStmt *n = makeNode(ReindexStmt);
						n->kind = OBJECT_INDEX_PARTITION;
						n->relation = $3;
						n->name = ((AlterTableCmd*)lfirst($4->head))->name;
						$$ = (Node *)n;
					}
					else
					{
						ListCell   *cell;
						foreach(cell, $4)
						{
							AlterTableCmd* cmd = (AlterTableCmd*) lfirst(cell);
							if (cmd->subtype == AT_RebuildIndex
								|| cmd->subtype == AT_RebuildIndexPartition)
							{
								ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
									errmsg("REBUILD is not supported for multiple commands")));
							}
						}

						AlterTableStmt *n = makeNode(AlterTableStmt);
						n->relation = $3;
						n->cmds = $4;
						n->relkind = OBJECT_INDEX;
						n->missing_ok = false;
						$$ = (Node *)n;
					}
				}
		|	ALTER INDEX IF_P EXISTS qualified_name alter_table_or_partition
				{
					ListCell   *cell;
					foreach(cell, $6)
					{
						AlterTableCmd* cmd = (AlterTableCmd*) lfirst(cell);
						if (cmd->subtype == AT_RebuildIndex
							|| cmd->subtype == AT_RebuildIndexPartition)
						{
							ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("IF EXISTS is not supported for REBUILD")));
						}
					}

					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $5;
					n->cmds = $6;
					n->relkind = OBJECT_INDEX;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
		|	ALTER SEQUENCE qualified_name alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $3;
					n->cmds = $4;
					n->relkind = OBJECT_SEQUENCE;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
		|	ALTER SEQUENCE IF_P EXISTS qualified_name alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $5;
					n->cmds = $6;
					n->relkind = OBJECT_SEQUENCE;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
		|	ALTER VIEW qualified_name alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $3;
					n->cmds = $4;
					n->relkind = OBJECT_VIEW;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
		|	ALTER VIEW IF_P EXISTS qualified_name alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $5;
					n->cmds = $6;
					n->relkind = OBJECT_VIEW;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
		|	ALTER MATERIALIZED VIEW qualified_name alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $4;
					n->cmds = $5;
					n->relkind = OBJECT_MATVIEW;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
		|	ALTER MATERIALIZED VIEW IF_P EXISTS qualified_name alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $6;
					n->cmds = $7;
					n->relkind = OBJECT_MATVIEW;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
        |   ALTER STREAM  qualified_name alter_table_cmds
            {
                AlterTableStmt *n = makeNode(AlterTableStmt);
                n->relation = $3;
                n->cmds = $4;
                n->relkind = OBJECT_STREAM;
                n->missing_ok = false;
                $$ = (Node *)n;
            }
        ;

modify_column_cmds:
			modify_column_cmd							{ $$ = list_make1($1); }
			| modify_column_cmds ',' modify_column_cmd	{ $$ = lappend($$, $3); }
			;
modify_column_cmd:
			ColId Typename
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					ColumnDef *def = makeNode(ColumnDef);
					n->subtype = AT_AlterColumnType;
					n->name = $1;
					n->def = (Node *) def;
					/* We only use these three fields of the ColumnDef node */
					def->typname = $2;
					def->collClause = NULL;
					def->raw_default = NULL;
					def->clientLogicColumnRef=NULL;
					$$ = (Node *)n;
				}
			| ColId NOT NULL_P opt_enable
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetNotNull;
					n->name = $1;
					$$ = (Node *)n;
				}
			| ColId NULL_P
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropNotNull;
					n->name = $1;
					$$ = (Node *)n;
				}
			| ColId CONSTRAINT name NOT NULL_P opt_enable
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					ColumnDef *def = makeNode(ColumnDef);
					Constraint *cons = makeNode(Constraint);
					n->subtype = AT_SetNotNull;
					n->name = $1;
					n->def = (Node *) def;
					def->constraints = list_make1(cons);
					cons->contype = CONSTR_NOTNULL;
					cons->conname = $3;
					cons->location = @2;
					$$ = (Node *)n;
				}
			| ColId CONSTRAINT name NULL_P
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					ColumnDef *def = makeNode(ColumnDef);
					Constraint *cons = makeNode(Constraint);
					n->subtype = AT_DropNotNull;
					n->name = $1;
					n->def = (Node *) def;
					def->constraints = list_make1(cons);
					cons->contype = CONSTR_NULL;
					cons->conname = $3;
					cons->location = @2;
					$$ = (Node *)n;
				}
			;
opt_enable:	ENABLE_P		{}
			| /* empty */	{}
			;
add_column_cmds:
			columnDef
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddColumn;
					n->def = $1;
					$$ = list_make1(n);
				}
			| add_column_cmds ',' columnDef
				{
				 	AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddColumn;
					n->def = $3;
					$$ = lappend($1, n);
				}
			;

/* ALTER TABLE sql clause both for PARTITION and ordinary table */
alter_table_or_partition:
			alter_table_cmds        { $$ = ($1); }
			| alter_partition_cmds  { $$ = ($1); }
		;

/* ALTER TABLE sql clauses for ordinary table */
alter_table_cmds:
			alter_table_cmd                         { $$ = list_make1($1); }
			| alter_table_cmds ',' alter_table_cmd  { $$ = lappend($1, $3); }
		;

/* ALTER TABLE PARTITION sql clauses */
alter_partition_cmds:
			alter_partition_cmd                            { $$ = list_make1($1); }
			| alter_partition_cmds ',' alter_partition_cmd { $$ = lappend($1, $3); }
			| move_partition_cmd                           { $$ = list_make1($1); }
			| exchange_partition_cmd                       { $$ = list_make1($1); }
		;

alter_partition_cmd:
		/* ALTER INDEX index_name MODIFY PARTITION partition_name UNUSABLE */
		MODIFY_PARTITION ColId UNUSABLE
			{
				AlterTableCmd *n = makeNode(AlterTableCmd);
				n->subtype = AT_UnusableIndexPartition;
				n->name = $2;
				$$ = (Node *)n;
			}
		/* ALTER TABLE table_name MODIFY PARTITION partition_name UNUSABLE ALL INDEX */
		| MODIFY_PARTITION ColId UNUSABLE LOCAL INDEXES
			{
				AlterTableCmd *n = makeNode(AlterTableCmd);
				n->subtype = AT_UnusableAllIndexOnPartition;
				n->name = $2;
				$$ = (Node *)n;
			}
		/* ALTER INDEX index_name REBUILD PARTITION partition_name */
		| REBUILD_PARTITION ColId
			{
				AlterTableCmd *n = makeNode(AlterTableCmd);
				n->subtype = AT_RebuildIndexPartition;
				n->name = $2;
				$$ = (Node *)n;
			}
		/* ALTER TABLE table_name MODIFY PARTITION partition_name REBUILD ALL INDEX */
		| MODIFY_PARTITION ColId REBUILD UNUSABLE LOCAL INDEXES
			{
				AlterTableCmd *n = makeNode(AlterTableCmd);
				n->subtype = AT_RebuildAllIndexOnPartition;
				n->name = $2;
				$$ = (Node *)n;
			}
		/* ALTER TABLE ADD PARTITION: use less/than */
		| ADD_PARTITION name VALUES LESS THAN
		'(' maxValueList ')' OptTableSpace
			{
				RangePartitionDefState *p = makeNode(RangePartitionDefState);
				AlterTableCmd *n = makeNode(AlterTableCmd);
				AddPartitionState *s = makeNode(AddPartitionState);
				p->partitionName = $2;
				p->boundary = $7;
				p->tablespacename = $9;
				s->partitionList = list_make1(p);
				s->isStartEnd = false;
				n->subtype = AT_AddPartition;
				n->def = (Node*)s;
				$$ = (Node *)n;
			}
		/* ALTER TABLE ADD PARTITION: use START/END */
		| ADD_PARTITION name START '(' maxValueList ')'  END_P '(' maxValueList ')' opt_range_every_list OptTableSpace
			{
				RangePartitionStartEndDefState *p = makeNode(RangePartitionStartEndDefState);
				AlterTableCmd *n = makeNode(AlterTableCmd);
				AddPartitionState *s = makeNode(AddPartitionState);
				p->partitionName = $2;
				p->startValue = $5;
				p->endValue = $9;
				p->everyValue = $11;
				p->tableSpaceName = $12;
				s->partitionList = list_make1(p);
				s->isStartEnd = true;
				n->subtype = AT_AddPartition;
				n->def = (Node*)s;
				$$ = (Node *)n;
			}
		| ADD_PARTITION name END_P '(' maxValueList ')' OptTableSpace
			{
				RangePartitionStartEndDefState *p = makeNode(RangePartitionStartEndDefState);
				AlterTableCmd *n = makeNode(AlterTableCmd);
				AddPartitionState *s = makeNode(AddPartitionState);
				p->partitionName = $2;
				p->startValue = NIL;
				p->endValue = $5;
				p->everyValue = NIL;
				p->tableSpaceName = $7;
				s->partitionList = list_make1(p);
				s->isStartEnd = true;
				n->subtype = AT_AddPartition;
				n->def = (Node*)s;
				$$ = (Node *)n;
			}
		| ADD_PARTITION name START '(' maxValueList ')' OptTableSpace
			{
				RangePartitionStartEndDefState *p = makeNode(RangePartitionStartEndDefState);
				AlterTableCmd *n = makeNode(AlterTableCmd);
				AddPartitionState *s = makeNode(AddPartitionState);
				p->partitionName = $2;
				p->startValue = $5;
				p->endValue = NIL;
				p->everyValue = NIL;
				p->tableSpaceName = $7;
				s->partitionList = list_make1(p);
				s->isStartEnd = true;
				n->subtype = AT_AddPartition;
				n->def = (Node*)s;
				$$ = (Node *)n;
			}
		| ADD_PARTITION name VALUES '(' expr_list ')' OptTableSpace
			{
				ListPartitionDefState *p = makeNode(ListPartitionDefState);
				AlterTableCmd *n = makeNode(AlterTableCmd);
				AddPartitionState *s = makeNode(AddPartitionState);
				p->partitionName = $2;
				p->boundary = $5;
				p->tablespacename = $7;
				s->partitionList = list_make1(p);
				s->isStartEnd = false;
				n->subtype = AT_AddPartition;
				n->def = (Node*)s;
				$$ = (Node *)n;
			}
		/* ALTER TABLE DROP PARTITION */
		| DROP_PARTITION ColId OptGPI
			{
				AlterTableCmd *n = makeNode(AlterTableCmd);
				n->subtype = AT_DropPartition;
				n->name = $2;
				n->behavior = DROP_CASCADE;
				n->missing_ok = FALSE;
				n->alterGPI = $3;
				$$ = (Node *)n;
			}
		/* ALTER TABLE DROP PARTITION */
		| DROP_PARTITION FOR '(' maxValueList ')' OptGPI
			{
				RangePartitionDefState *p = makeNode(RangePartitionDefState);
				AlterTableCmd *n = makeNode(AlterTableCmd);

				p->boundary = $4;
				n->subtype = AT_DropPartition;
				n->def = (Node*)p;
				n->behavior = DROP_CASCADE;
				n->missing_ok = FALSE;
				n->alterGPI = $6;
				$$ = (Node *)n;
			}
		/* merge 2 or more partitions into 1 partition */
		| MERGE PARTITIONS name_list INTO PARTITION name OptTableSpace OptGPI
			{
				AlterTableCmd *n = makeNode(AlterTableCmd);
				n->def = (Node*)$3;
				n->name = $6;
				n->target_partition_tablespace = $7;
				n->subtype = AT_MergePartition;
				n->alterGPI = $8;
				$$ = (Node*)n;
			}
		/* split one partition into two partition */
		| SPLIT PARTITION name AT '(' maxValueList ')' INTO
		  '(' split_dest_partition_define_list ')' OptGPI
			{
				AlterTableCmd	*n = makeNode(AlterTableCmd);
				SplitPartitionState	*s = makeNode(SplitPartitionState);

				s->src_partition_name = $3;
				s->split_point = $6;
				s->dest_partition_define_list = $10;
				s->partition_for_values = NULL;

				n->def = (Node*)s;
				n->subtype = AT_SplitPartition;
				n->alterGPI = $12;
				$$ = (Node*)n;
			}
		/* split one partition into two partition */
		| SPLIT PARTITION_FOR '(' maxValueList ')' AT '(' maxValueList ')' INTO
		  '(' split_dest_partition_define_list ')' OptGPI
			{
				AlterTableCmd	*n = makeNode(AlterTableCmd);
				SplitPartitionState	*s = makeNode(SplitPartitionState);

				s->partition_for_values = $4;
				s->split_point = $8;
				s->dest_partition_define_list = $12;
				s->src_partition_name = NULL;

				n->def = (Node*)s;
				n->subtype = AT_SplitPartition;
				n->alterGPI = $14;
				$$ = (Node*)n;
			}
		/*split one partition into multiple partition*/
		| SPLIT PARTITION name INTO '(' range_partition_definition_list ')' OptGPI
			{
				AlterTableCmd	*n = makeNode(AlterTableCmd);
				SplitPartitionState	*s = makeNode(SplitPartitionState);

				s->src_partition_name = $3;
				s->dest_partition_define_list = $6;
				s->split_point = NULL;
				s->partition_for_values = NULL;

				n->def = (Node*)s;
				n->subtype = AT_SplitPartition;
				n->alterGPI = $8;
				$$ = (Node*)n;
			}
		| SPLIT PARTITION_FOR '(' maxValueList ')' INTO '(' range_partition_definition_list ')' OptGPI
			{
				AlterTableCmd	*n = makeNode(AlterTableCmd);
				SplitPartitionState	*s = makeNode(SplitPartitionState);

				s->partition_for_values = $4;
				s->dest_partition_define_list = $8;
				s->src_partition_name = NULL;
				s->split_point = NULL;

				n->def = (Node*)s;
				n->subtype = AT_SplitPartition;
				n->alterGPI = 10;
				$$ = (Node*)n;
			}
		/* truncate partition */
		| TRUNCATE PARTITION ColId OptGPI
			{
				AlterTableCmd *n = makeNode(AlterTableCmd);
				n->subtype = AT_TruncatePartition;
				n->missing_ok = FALSE;
				n->alterGPI = $4;
				$$ = (Node *)n;

				n->name = $3;

			}
		/* truncate partition */
		| TRUNCATE PARTITION_FOR '(' maxValueList ')' OptGPI
			{

				RangePartitionDefState *p = makeNode(RangePartitionDefState);
				AlterTableCmd *n = makeNode(AlterTableCmd);

				p->boundary = $4;
				n->subtype = AT_TruncatePartition;
				n->def = (Node*)p;
				n->missing_ok = FALSE;
				n->alterGPI = $6;
				$$ = (Node *)n;

			}
		/* ENABLE ROW MOVEMENT */
		| ENABLE_P ROW MOVEMENT
			{
				AlterTableCmd *n = makeNode(AlterTableCmd);

				n->subtype = AT_EnableRowMoveMent;
				n->missing_ok = FALSE;
				$$ = (Node *) n;
			}
		/* DISABLE ROW MOVEMENT */
		| DISABLE_P ROW MOVEMENT
			{
				AlterTableCmd *n = makeNode(AlterTableCmd);

				n->subtype = AT_DisableRowMoveMent;
				n->missing_ok = FALSE;
				$$ = (Node *) n;

			}
		;

move_partition_cmd:
		/* ALTER TABLE <name> MOVE PARTITION <part_name> TABLESPACE <tablespacename> */
		MOVE PARTITION partition_name TABLESPACE name
			{
				AlterTableCmd *n = makeNode(AlterTableCmd);
				n->subtype = AT_SetPartitionTableSpace;
				n->def = (Node*)$3;
				n->name = $5;
				$$ = (Node *)n;
			}
		/* ALTER TABLE <name> MOVE PARTITION FOR (...) TABLESPACE <tablespacename> */
		| MOVE PARTITION_FOR '(' maxValueList ')' TABLESPACE name
			{
				RangePartitionDefState *p = makeNode(RangePartitionDefState);
				AlterTableCmd *n = makeNode(AlterTableCmd);

				p->boundary = $4;

				n->subtype = AT_SetPartitionTableSpace;
				n->def = (Node*)p;
				n->name = $7;
				$$ = (Node *)n;
			}
		;

exchange_partition_cmd:
		/* exchange partition */
		EXCHANGE PARTITION '(' ColId ')'
		  WITH TABLE relation_expr opt_verbose OptGPI
			{
				AlterTableCmd *n = makeNode(AlterTableCmd);

				n->subtype = AT_ExchangePartition;
				n->name = $4;
				n->exchange_with_rel = $8;
				n->check_validation = TRUE;
				n->exchange_verbose = $9;
				n->missing_ok = FALSE;
				n->alterGPI = $10;
				$$ = (Node *)n;
			}
		/* exchange partition */
		| EXCHANGE PARTITION '(' ColId ')'
		  WITH TABLE relation_expr WITH VALIDATION opt_verbose OptGPI
			{
				AlterTableCmd *n = makeNode(AlterTableCmd);

				n->subtype = AT_ExchangePartition;
				n->name = $4;
				n->exchange_with_rel = $8;
				n->check_validation = TRUE;
				n->exchange_verbose = $11;
				n->missing_ok = FALSE;
				n->alterGPI = $12;
				$$ = (Node *)n;
			}
		/* exchange partition */
		| EXCHANGE PARTITION '(' ColId ')'
		  WITH TABLE relation_expr WITHOUT VALIDATION OptGPI
			{
				AlterTableCmd *n = makeNode(AlterTableCmd);

				n->subtype = AT_ExchangePartition;
				n->name = $4;
				n->exchange_with_rel = $8;
				n->check_validation = FALSE;
				n->missing_ok = FALSE;
				n->alterGPI = $11;
				$$ = (Node *)n;
			}

		/* exchange partition */
		| EXCHANGE PARTITION_FOR '(' maxValueList ')'
		  WITH TABLE relation_expr opt_verbose OptGPI
			{
				AlterTableCmd *n = makeNode(AlterTableCmd);
				RangePartitionDefState *p = makeNode(RangePartitionDefState);

				p->boundary = $4;

				n->subtype = AT_ExchangePartition;
				n->exchange_with_rel = $8;
				n->check_validation = TRUE;
				n->exchange_verbose = $9;
				n->def = (Node*)p;
				n->missing_ok = FALSE;
				n->alterGPI = $10;
				$$ = (Node *)n;
			}

		| EXCHANGE PARTITION_FOR '(' maxValueList ')'
		  WITH TABLE relation_expr WITH VALIDATION opt_verbose OptGPI
			{
				AlterTableCmd *n = makeNode(AlterTableCmd);
				RangePartitionDefState *p = makeNode(RangePartitionDefState);

				p->boundary = $4;

				n->subtype = AT_ExchangePartition;
				n->exchange_with_rel = $8;
				n->check_validation = TRUE;
				n->exchange_verbose = $11;
				n->def = (Node*)p;
				n->missing_ok = FALSE;
				n->alterGPI = $12;
				$$ = (Node *)n;
			}

		| EXCHANGE PARTITION_FOR '(' maxValueList ')'
		  WITH TABLE relation_expr WITHOUT VALIDATION OptGPI
			{
				AlterTableCmd *n = makeNode(AlterTableCmd);
				RangePartitionDefState *p = makeNode(RangePartitionDefState);

				p->boundary = $4;

				n->subtype = AT_ExchangePartition;
				n->exchange_with_rel = $8;
				n->check_validation = FALSE;
				n->def = (Node*)p;
				n->missing_ok = FALSE;
				n->alterGPI = $11;
				$$ = (Node *)n;
			}
		;

alter_table_cmd:
			/*ALTER INDEX index_name UNUSABLE*/
			UNUSABLE
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_UnusableIndex;
					$$ = (Node *)n;
				}
			|
			/*ALTER INDEX index_name REBUILD*/
			REBUILD
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_RebuildIndex;
					$$ = (Node *)n;
				}

			|
			/* ALTER TABLE <name> ADD <coldef> */
			ADD_P columnDef
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddColumn;
					n->def = $2;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ADD COLUMN <coldef> */
			| ADD_P COLUMN columnDef
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddColumn;
					n->def = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> {SET DEFAULT <expr>|DROP DEFAULT} */
			| ALTER opt_column ColId alter_column_default
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ColumnDefault;
					n->name = $3;
					n->def = $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> DROP NOT NULL */
			| ALTER opt_column ColId DROP NOT NULL_P
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropNotNull;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET NOT NULL */
			| ALTER opt_column ColId SET NOT NULL_P
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetNotNull;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET STATISTICS <SignedIconst> */
			| ALTER opt_column ColId SET STATISTICS SignedIconst
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetStatistics;
					n->name = $3;
					n->def = (Node *) makeInteger($6);
					n->additional_property = AT_CMD_WithoutPercent;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET STATISTICS PERCENT <SignedIconst> */
			| ALTER opt_column ColId SET STATISTICS PERCENT SignedIconst
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetStatistics;
					n->name = $3;
					n->def = (Node *) makeInteger($7);
					n->additional_property = AT_CMD_WithPercent;
					$$ = (Node *)n;
				}
			| ADD_P STATISTICS '(' opt_multi_name_list ')'
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddStatistics;
					n->def = (Node *) $4;
					$$ = (Node *)n;
				}
			| DELETE_P STATISTICS '(' opt_multi_name_list ')'
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DeleteStatistics;
					n->def = (Node *) $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET ( column_parameter = value [, ... ] ) */
			| ALTER opt_column ColId SET reloptions
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetOptions;
					n->name = $3;
					n->def = (Node *) $5;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET ( column_parameter = value [, ... ] ) */
			| ALTER opt_column ColId RESET reloptions
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ResetOptions;
					n->name = $3;
					n->def = (Node *) $5;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ALTER [COLUMN] <colname> SET STORAGE <storagemode> */
			| ALTER opt_column ColId SET STORAGE ColId
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetStorage;
					n->name = $3;
					n->def = (Node *) makeString($6);
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DROP [COLUMN] IF EXISTS <colname> [RESTRICT|CASCADE] */
			| DROP opt_column IF_P EXISTS ColId opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropColumn;
					n->name = $5;
					n->behavior = $6;
					n->missing_ok = TRUE;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DROP [COLUMN] <colname> [RESTRICT|CASCADE] */
			| DROP opt_column ColId opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropColumn;
					n->name = $3;
					n->behavior = $4;
					n->missing_ok = FALSE;
					$$ = (Node *)n;
				}
			/*
			 * ALTER TABLE <name> ALTER [COLUMN] <colname> [SET DATA] TYPE <typename>
			 *		[ USING <expression> ]
			 */
			| ALTER opt_column ColId opt_set_data TYPE_P Typename opt_collate_clause alter_using
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					ColumnDef *def = makeNode(ColumnDef);
					n->subtype = AT_AlterColumnType;
					n->name = $3;
					n->def = (Node *) def;
					/* We only use these three fields of the ColumnDef node */
					def->typname = $6;
					def->collClause = (CollateClause *) $7;
					def->raw_default = $8;
					def->clientLogicColumnRef=NULL;
					$$ = (Node *)n;
				}
			/* ALTER FOREIGN TABLE <name> ALTER [COLUMN] <colname> OPTIONS */
			| ALTER opt_column ColId alter_generic_options
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AlterColumnGenericOptions;
					n->name = $3;
					n->def = (Node *) $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ADD CONSTRAINT ... */
			| ADD_P TableConstraint
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddConstraint;
					n->def = $2;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> VALIDATE CONSTRAINT ... */
			| VALIDATE CONSTRAINT name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ValidateConstraint;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DROP CONSTRAINT IF EXISTS <name> [RESTRICT|CASCADE] */
			| DROP CONSTRAINT IF_P EXISTS name opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropConstraint;
					n->name = $5;
					n->behavior = $6;
					n->missing_ok = TRUE;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DROP CONSTRAINT <name> [RESTRICT|CASCADE] */
			| DROP CONSTRAINT name opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropConstraint;
					n->name = $3;
					n->behavior = $4;
					n->missing_ok = FALSE;
					$$ = (Node *)n;
				}
			| MODIFY_P modify_column_cmd
				{
					$$ = $2;
				}
			/* ALTER TABLE <name> SET WITH OIDS  */
			| SET WITH OIDS
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddOids;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET WITHOUT OIDS  */
			| SET WITHOUT OIDS
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropOids;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> CLUSTER ON <indexname> */
			| CLUSTER ON name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ClusterOn;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET WITHOUT CLUSTER */
			| SET WITHOUT CLUSTER
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropCluster;
					n->name = NULL;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE TRIGGER <trig> */
			| ENABLE_P TRIGGER name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableTrig;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE ALWAYS TRIGGER <trig> */
			| ENABLE_P ALWAYS TRIGGER name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableAlwaysTrig;
					n->name = $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE REPLICA TRIGGER <trig> */
			| ENABLE_P REPLICA TRIGGER name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableReplicaTrig;
					n->name = $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE TRIGGER ALL */
			| ENABLE_P TRIGGER ALL
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableTrigAll;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE TRIGGER USER */
			| ENABLE_P TRIGGER USER
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableTrigUser;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DISABLE TRIGGER <trig> */
			| DISABLE_P TRIGGER name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DisableTrig;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DISABLE TRIGGER ALL */
			| DISABLE_P TRIGGER ALL
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DisableTrigAll;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DISABLE TRIGGER USER */
			| DISABLE_P TRIGGER USER
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DisableTrigUser;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE RULE <rule> */
			| ENABLE_P RULE name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableRule;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE ALWAYS RULE <rule> */
			| ENABLE_P ALWAYS RULE name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableAlwaysRule;
					n->name = $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE REPLICA RULE <rule> */
			| ENABLE_P REPLICA RULE name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableReplicaRule;
					n->name = $4;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DISABLE RULE <rule> */
			| DISABLE_P RULE name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DisableRule;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> INHERIT <parent> */
			| INHERIT qualified_name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddInherit;
					n->def = (Node *) $2;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> NO INHERIT <parent> */
			| NO INHERIT qualified_name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropInherit;
					n->def = (Node *) $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> OF <type_name> */
			| OF any_name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					TypeName *def = makeTypeNameFromNameList($2);
					def->location = @2;
					n->subtype = AT_AddOf;
					n->def = (Node *) def;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> NOT OF */
			| NOT OF
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropOf;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> OWNER TO RoleId */
			| OWNER TO RoleId
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ChangeOwner;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET TABLESPACE <tablespacename> */
			| SET TABLESPACE name
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetTableSpace;
					n->name = $3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET COMPRESS/NOCOMPRESS */
			| SET COMPRESS
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SET_COMPRESS;
					n->name = "COMPRESS";
					$$ = (Node *)n;
				}
			| SET NOCOMPRESS
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SET_COMPRESS;
					n->name = "NOCOMPRESS";
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> SET (...) */
			| SET reloptions
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SetRelOptions;
					n->def = (Node *)$2;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> RESET (...) */
			| RESET reloptions
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ResetRelOptions;
					n->def = (Node *)$2;
					$$ = (Node *)n;
				}
				/* ALTER TABLE <name> REPLICA IDENTITY  */
				| REPLICA IDENTITY_P replica_identity
				    {
				        AlterTableCmd *n = makeNode(AlterTableCmd);
				        n->subtype = AT_ReplicaIdentity;
				        n->def = $3;
				        $$ = (Node *)n;
				    }
				| alter_generic_options
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_GenericOptions;
					n->def = (Node *)$1;
					$$ = (Node *) n;
				}

/* PGXC_BEGIN */
			/* ALTER TABLE <name> DISTRIBUTE BY ... */
			| OptDistributeByInternal
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DistributeBy;
					n->def = (Node *)$1;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> TO [ NODE (nodelist) | GROUP groupname ] */
			| OptSubClusterInternal
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_SubCluster;
					n->def = (Node *)$1;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ADD NODE (nodelist) */
			| ADD_P NODE pgxcnodes
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddNodeList;
					n->def = (Node *)$3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DELETE NODE (nodelist) */
			| DELETE_P NODE pgxcnodes
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DeleteNodeList;
					n->def = (Node *)$3;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> ENABLE ROW LEVEL SECURITY */
			| ENABLE_P ROW LEVEL SECURITY
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_EnableRls;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> DISABLE ROW LEVEL SECURITY */
			| DISABLE_P ROW LEVEL SECURITY
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DisableRls;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> FORCE ROW LEVEL SECURITY */
			| FORCE ROW LEVEL SECURITY
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_ForceRls;
					$$ = (Node *)n;
				}
			/* ALTER TABLE <name> NO FORCE ROW LEVEL SECURITY */
			| NO FORCE ROW LEVEL SECURITY
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_NoForceRls;
					$$ = (Node *)n;
				}
/* PGXC_END */
		;

alter_column_default:
			SET DEFAULT a_expr			{ $$ = $3; }
			| DROP DEFAULT				{ $$ = NULL; }
		;

opt_drop_behavior:
			CASCADE						{ $$ = DROP_CASCADE; }
			| RESTRICT					{ $$ = DROP_RESTRICT; }
			| CASCADE CONSTRAINTS				{ $$ = DROP_CASCADE; }
			| /* EMPTY */				{ $$ = DROP_RESTRICT; /* default */ }
		;

opt_collate_clause:
			COLLATE any_name
				{
					CollateClause *n = makeNode(CollateClause);
					n->arg = NULL;
					n->collname = $2;
					n->location = @1;
					$$ = (Node *) n;
				}
			| /* EMPTY */				{ $$ = NULL; }
		;

alter_using:
			USING a_expr				{ $$ = $2; }
			| /* EMPTY */				{ $$ = NULL; }
		;

reloptions:
			'(' reloption_list ')'					{ $$ = $2; }
		;

replica_identity:
           NOTHING
               {
                   ReplicaIdentityStmt *n = makeNode(ReplicaIdentityStmt);
                   n->identity_type = REPLICA_IDENTITY_NOTHING;
                   n->name = NULL;
                   $$ = (Node *) n;
               }
           | FULL
               {
                   ReplicaIdentityStmt *n = makeNode(ReplicaIdentityStmt);
                   n->identity_type = REPLICA_IDENTITY_FULL;
                   n->name = NULL;
                   $$ = (Node *) n;
               }
           | DEFAULT
               {
                   ReplicaIdentityStmt *n = makeNode(ReplicaIdentityStmt);
                   n->identity_type = REPLICA_IDENTITY_DEFAULT;
                   n->name = NULL;
                   $$ = (Node *) n;
               }
           | USING INDEX name
               {
                   ReplicaIdentityStmt *n = makeNode(ReplicaIdentityStmt);
                   n->identity_type = REPLICA_IDENTITY_INDEX;
                   n->name = $3;
                   $$ = (Node *) n;
               }
       ;

opt_reloptions:		WITH reloptions					{ $$ = $2; }
			 |		/* EMPTY */						{ $$ = NIL; }
		;

reloption_list:
			reloption_elem							{ $$ = list_make1($1); }
			| reloption_list ',' reloption_elem		{ $$ = lappend($1, $3); }
		;

/* This should match def_elem and also allow qualified names */
reloption_elem:
			ColLabel '=' def_arg
				{
					$$ = makeDefElem($1, (Node *) $3);
				}
			| ColLabel '=' ROW
				{
					$$ = makeDefElem($1, (Node *) makeString(pstrdup($3)));
				}
			| ColLabel
				{
					$$ = makeDefElem($1, NULL);
				}
			| ColLabel '.' ColLabel '=' def_arg
				{
					$$ = makeDefElemExtended($1, $3, (Node *) $5,
											 DEFELEM_UNSPEC);
				}
			| ColLabel '.' ColLabel
				{
					$$ = makeDefElemExtended($1, $3, NULL, DEFELEM_UNSPEC);
				}
		;

split_dest_partition_define_list:
	PARTITION name OptTableSpace ',' PARTITION name OptTableSpace
		{
			List	*result = NULL;
			RangePartitionDefState *p1 = makeNode(RangePartitionDefState);
			RangePartitionDefState *p2 = makeNode(RangePartitionDefState);

			p1->partitionName = $2;
			p1->tablespacename = $3;
			p1->boundary = NULL;

			p2->partitionName = $6;
			p2->tablespacename = $7;
			p2->boundary = NULL;

			result = lappend(result, p1);
			result = lappend(result, p2);

			$$ = result;
		}
	;

/*****************************************************************************
 *
 *	ALTER TYPE
 *
 * really variants of the ALTER TABLE subcommands with different spellings
 *****************************************************************************/

AlterCompositeTypeStmt:
			ALTER TYPE_P any_name alter_type_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);

					/* can't use qualified_name, sigh */
					n->relation = makeRangeVarFromAnyName($3, @3, yyscanner);
					n->cmds = $4;
					n->relkind = OBJECT_TYPE;
					$$ = (Node *)n;
				}
			;

alter_type_cmds:
			alter_type_cmd							{ $$ = list_make1($1); }
			| alter_type_cmds ',' alter_type_cmd	{ $$ = lappend($1, $3); }
		;

alter_type_cmd:
			/* ALTER TYPE <name> ADD ATTRIBUTE <coldef> [RESTRICT|CASCADE] */
			ADD_P ATTRIBUTE TableFuncElement opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_AddColumn;
					n->def = $3;
					n->behavior = $4;
					$$ = (Node *)n;
				}
			/* ALTER TYPE <name> DROP ATTRIBUTE IF EXISTS <attname> [RESTRICT|CASCADE] */
			| DROP ATTRIBUTE IF_P EXISTS ColId opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropColumn;
					n->name = $5;
					n->behavior = $6;
					n->missing_ok = TRUE;
					$$ = (Node *)n;
				}
			/* ALTER TYPE <name> DROP ATTRIBUTE <attname> [RESTRICT|CASCADE] */
			| DROP ATTRIBUTE ColId opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					n->subtype = AT_DropColumn;
					n->name = $3;
					n->behavior = $4;
					n->missing_ok = FALSE;
					$$ = (Node *)n;
				}
			/* ALTER TYPE <name> ALTER ATTRIBUTE <attname> [SET DATA] TYPE <typename> [RESTRICT|CASCADE] */
			| ALTER ATTRIBUTE ColId opt_set_data TYPE_P Typename opt_collate_clause opt_drop_behavior
				{
					AlterTableCmd *n = makeNode(AlterTableCmd);
					ColumnDef *def = makeNode(ColumnDef);
					n->subtype = AT_AlterColumnType;
					n->name = $3;
					n->def = (Node *) def;
					n->behavior = $8;
					/* We only use these three fields of the ColumnDef node */
					def->typname = $6;
					def->clientLogicColumnRef=NULL;
					def->collClause = (CollateClause *) $7;
					def->raw_default = NULL;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 *		QUERY :
 *				close <portalname>
 *
 *****************************************************************************/

ClosePortalStmt:
			CLOSE cursor_name
				{
					ClosePortalStmt *n = makeNode(ClosePortalStmt);
					n->portalname = $2;
					$$ = (Node *)n;
				}
			| CLOSE ALL
				{
					ClosePortalStmt *n = makeNode(ClosePortalStmt);
					n->portalname = NULL;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 *		QUERY :
 *				COPY relname [(columnList)] FROM/TO file [WITH] [(options)]
 *				COPY ( SELECT ... ) TO file [WITH] [(options)]
 *
 *				In the preferred syntax the options are comma-separated
 *				and use generic identifiers instead of keywords.  The pre-9.0
 *				syntax had a hard-wired, space-separated set of options.
 *
 *				Really old syntax, from versions 7.2 and prior:
 *				COPY [ BINARY ] table [ WITH OIDS ] FROM/TO file
 *					[ [ USING ] DELIMITERS 'delimiter' ] ]
 *					[ WITH NULL AS 'null string' ]
 *				This option placement is not supported with COPY (SELECT...).
 *
 *****************************************************************************/

CopyStmt:	COPY opt_binary qualified_name opt_column_list opt_oids
			copy_from copy_file_name copy_delimiter opt_noescaping OptCopyLogError OptCopyRejectLimit opt_with copy_options 
			opt_processed
				{
					CopyStmt *n = makeNode(CopyStmt);
					n->relation = $3;
					n->query = NULL;
					n->attlist = $4;
					n->is_from = $6;
					n->filename = $7;
					if ($4)
						n->relation->length = @4;
					else if ($5)
						n->relation->length = @5;
					else
						n->relation->length = @6;
					n->options = NIL;
					/* Concatenate user-supplied flags */
					if ($2)
						n->options = lappend(n->options, $2);
					if ($5)
						n->options = lappend(n->options, $5);
					if ($8)
						n->options = lappend(n->options, $8);
					if ($9)
						n->options = lappend(n->options, $9);
					if ($10)
						n->options = lappend(n->options, $10);
					if ($11)
						n->options = lappend(n->options, $11);
					if ($13)
						n->options = list_concat(n->options, $13);
					$$ = (Node *)n;
				}
			| COPY select_with_parens TO copy_file_name opt_noescaping opt_with copy_options opt_processed
				{
					CopyStmt *n = makeNode(CopyStmt);
					n->relation = NULL;
					n->query = $2;
					n->attlist = NIL;
					n->is_from = false;
					n->filename = $4;
					n->options = $7;
					if ($5)
						n->options = lappend(n->options, $5);
					$$ = (Node *)n;
				}
		;

opt_processed:
                ENCRYPTED {$$=TRUE;}
				|  /*EMPTY*/						{ $$ = FALSE; }

copy_from:
			FROM									{ $$ = TRUE; }
			| TO									{ $$ = FALSE; }
		;

/*
 * copy_file_name NULL indicates stdio is used. Whether stdin or stdout is
 * used depends on the direction. (It really doesn't make sense to copy from
 * stdout. We silently correct the "typo".)		 - AY 9/94
 */
copy_file_name:
			Sconst									{ $$ = $1; }
			| STDIN									{ $$ = NULL; }
			| STDOUT								{ $$ = NULL; }
			| REDISANYVALUE                              { $$ = NULL; }
		;

copy_options: copy_opt_list							{ $$ = $1; }
			| '(' copy_generic_opt_list ')'			{ $$ = $2; }
		;

/* old COPY option syntax */
copy_opt_list:
			copy_opt_list copy_opt_item				{ $$ = lappend($1, $2); }
			| /* EMPTY */							{ $$ = NIL; }
		;

copy_opt_item:
			BINARY
				{
					$$ = makeDefElem("format", (Node *)makeString("binary"));
				}
			| OIDS
				{
					$$ = makeDefElem("oids", (Node *)makeInteger(TRUE));
				}
			|FREEZE
				{
					$$ = makeDefElem("freeze", (Node *)makeInteger(TRUE));
				}
			| DELIMITER opt_as Sconst
				{
					$$ = makeDefElem("delimiter", (Node *)makeString($3));
				}
			| NULL_P opt_as Sconst
				{
					$$ = makeDefElem("null", (Node *)makeString($3));
				}
			| CSV
				{
					$$ = makeDefElem("format", (Node *)makeString("csv"));
				}
			| FIXED_P
				{
					$$ = makeDefElem("format", (Node *)makeString("fixed"));
				}
			| HEADER_P
				{
					$$ = makeDefElem("header", (Node *)makeInteger(TRUE));
				}
			| QUOTE opt_as Sconst
				{
					$$ = makeDefElem("quote", (Node *)makeString($3));
				}
			| ESCAPE opt_as Sconst
				{
					$$ = makeDefElem("escape", (Node *)makeString($3));
				}
			| FORCE QUOTE columnList
				{
					$$ = makeDefElem("force_quote", (Node *)$3);
				}
			| FORCE QUOTE '*'
				{
					$$ = makeDefElem("force_quote", (Node *)makeNode(A_Star));
				}
			| FORCE NOT NULL_P columnList
				{
					$$ = makeDefElem("force_not_null", (Node *)$4);
				}
			| ENCODING Sconst
				{
					$$ = makeDefElem("encoding", (Node *)makeString($2));
				}
			| EOL Sconst
				{
					$$ = makeDefElem("eol", (Node*)makeString($2));
				}
			| FILEHEADER_P Sconst
				{
					$$ = makeDefElem("fileheader", (Node*)makeString($2));
				}
			| FORMATTER '(' copy_foramtter_opt ')'
				{
					$$ = makeDefElem("formatter", (Node*)$3);
				}
			| IGNORE_EXTRA_DATA
				{
					$$ = makeDefElem("ignore_extra_data", (Node *)makeInteger(TRUE));
				}
			| DATE_FORMAT_P Sconst
				{
					$$ = makeDefElem("date_format", (Node *)makeString($2));
				}
			| TIME_FORMAT_P Sconst
				{
					$$ = makeDefElem("time_format", (Node *)makeString($2));
				}
			| TIMESTAMP_FORMAT_P Sconst
				{
					$$ = makeDefElem("timestamp_format", (Node *)makeString($2));
				}
			| SMALLDATETIME_FORMAT_P Sconst
				{
					$$ = makeDefElem("smalldatetime_format", (Node *)makeString($2));
				}
			| COMPATIBLE_ILLEGAL_CHARS
				{
					$$ = makeDefElem("compatible_illegal_chars", (Node *)makeInteger(TRUE));
				}
			| FILL_MISSING_FIELDS
			{
					$$ = makeDefElem("fill_missing_fields", (Node *)makeInteger(TRUE));
			}
		;

/* The following exist for backward compatibility with very old versions */

opt_binary:
			BINARY
				{
					$$ = makeDefElem("format", (Node *)makeString("binary"));
				}
			| /*EMPTY*/								{ $$ = NULL; }
		;

opt_oids:
			WITH OIDS
				{
					$$ = makeDefElem("oids", (Node *)makeInteger(TRUE));
				}
			| /*EMPTY*/								{ $$ = NULL; }
		;

copy_delimiter:
			opt_using DELIMITERS Sconst
				{
					$$ = makeDefElem("delimiter", (Node *)makeString($3));
				}
			| /*EMPTY*/								{ $$ = NULL; }
		;

opt_using:
			USING									{}
			| /*EMPTY*/								{}
		;

opt_noescaping:
			WITHOUT ESCAPING
				{
					$$ = makeDefElem("noescaping", (Node *)makeInteger(TRUE));
				}
			| /*EMPTY*/ 							{$$ = NULL;}
		;

OptCopyLogError:
			LOG_P ERRORS DATA_P
				{
					$$ = makeDefElem("log_errors_data", (Node *)makeInteger(TRUE));
				}
			| LOG_P ERRORS
             	{
             		$$ = makeDefElem("log_errors", (Node *)makeInteger(TRUE));
             	}
             	| /*EMPTY*/	                        { $$ = NULL; }
		;

OptCopyRejectLimit:
			REJECT_P LIMIT Sconst
				{
					$$ = makeDefElem("reject_limit", (Node*)makeString($3));
				}
			| /*EMPTY*/								{ $$ = NULL; }
		;

/* new COPY option syntax */
copy_generic_opt_list:
			copy_generic_opt_elem
				{
					$$ = list_make1($1);
				}
			| copy_generic_opt_list ',' copy_generic_opt_elem
				{
					$$ = lappend($1, $3);
				}
		;

copy_generic_opt_elem:
			ColLabel copy_generic_opt_arg
				{
					$$ = makeDefElem($1, $2);
				}
		;

copy_generic_opt_arg:
			opt_boolean_or_string			{ $$ = (Node *) makeString($1); }
			| NumericOnly					{ $$ = (Node *) $1; }
			| '*'							{ $$ = (Node *) makeNode(A_Star); }
			| '(' copy_generic_opt_arg_list ')'		{ $$ = (Node *) $2; }
			| /* EMPTY */					{ $$ = NULL; }
		;

copy_generic_opt_arg_list:
			  copy_generic_opt_arg_list_item
				{
					$$ = list_make1($1);
				}
			| copy_generic_opt_arg_list ',' copy_generic_opt_arg_list_item
				{
					$$ = lappend($1, $3);
				}
		;

/* beware of emitting non-string list elements here; see commands/define.c */
copy_generic_opt_arg_list_item:
			opt_boolean_or_string	{ $$ = (Node *) makeString($1); }
		;

copy_foramtter_opt:
			copy_col_format_def
				{
					$$ = list_make1($1);
				}
			| copy_foramtter_opt ',' copy_col_format_def
				{
					$$ = lappend($1, $3);
				}
		;

copy_col_format_def:
			ColId '(' Iconst ',' Iconst ')'
				{
					Position *arg = makeNode(Position);
					arg->colname = $1;
					arg->position = $3;
					arg->fixedlen = $5;
					$$ = (Node*)arg;
				}
		;
/*****************************************************************************
 *
 *       QUERY :
 *             CREATE STREAM relname
 *
 *     feature:
 *     1) create foreign table for streaming server
 *
 *
 *****************************************************************************/
 
CreateStreamStmt: 
        CREATE STREAM qualified_name '(' OptTableElementList ')'
            {
                CreateForeignTableStmt *n = makeNode(CreateForeignTableStmt);
                n->servername = STREAMING_SERVER;
                n->base.if_not_exists = false;
                n->base.relation = $3;
                n->base.tableElts = $5;
                $$ = (Node *)n;
            }
        | CREATE STREAM IF_P NOT EXISTS qualified_name '(' OptTableElementList ')'
            {
                CreateForeignTableStmt *n = makeNode(CreateForeignTableStmt);
                n->servername = STREAMING_SERVER;
                n->base.if_not_exists = true;
                n->base.relation = $6;
                n->base.tableElts = $8;
                $$ = (Node *)n;
            }
        ;

/*****************************************************************************
 *
 *		QUERY :
 *				CREATE TABLE relname
 *
 *		PGXC-related extensions:
 *		1) Distribution type of a table:
 *			DISTRIBUTE BY ( HASH(column) | MODULO(column) |
 *							REPLICATION | ROUNDROBIN )
 *		2) Subcluster for table
 *			TO ( GROUP groupname | NODE nodename1,...,nodenameN )
 *
 *		3) Internal additional data from CN to CN and CN to DN; the clause must be the last clause.
 *			INTERNAL DATA xxxxxxxx
 *
 *****************************************************************************/

CreateStmt:	CREATE OptTemp TABLE qualified_name '(' OptTableElementList ')'
			OptInherit OptWith OnCommitOption OptCompress OptPartitionElement
/* PGXC_BEGIN */
			OptDistributeBy OptSubCluster
/* PGXC_END */
			opt_table_partitioning_clause
			opt_internal_data OptKind
				{
					CreateStmt *n = makeNode(CreateStmt);
					$4->relpersistence = $2;
					n->relkind = $17;
					n->relation = $4;
					n->tableElts = $6;
					n->inhRelations = $8;
					n->constraints = NIL;
					n->options = $9;
					n->oncommit = $10;
					n->row_compress = $11;
					n->tablespacename = $12;
					n->if_not_exists = false;
/* PGXC_BEGIN */
					n->distributeby = $13;
					n->subcluster = $14;
/* PGXC_END */
					n->partTableState = (PartitionState *)$15;
					n->internalData = $16;
					$$ = (Node *)n;
				}
		| CREATE OptTemp TABLE IF_P NOT EXISTS qualified_name '('
			OptTableElementList ')' OptInherit OptWith OnCommitOption
			OptCompress OptPartitionElement
/* PGXC_BEGIN */
			OptDistributeBy OptSubCluster
/* PGXC_END */
			opt_table_partitioning_clause
			opt_internal_data
				{
					CreateStmt *n = makeNode(CreateStmt);
					$7->relpersistence = $2;
					n->relation = $7;
					n->tableElts = $9;
					n->inhRelations = $11;
					n->constraints = NIL;
					n->options = $12;
					n->oncommit = $13;
					n->row_compress = $14;
					n->tablespacename = $15;
					n->if_not_exists = true;
/* PGXC_BEGIN */
					n->distributeby = $16;
					n->subcluster = $17;
/* PGXC_END */
					n->partTableState = (PartitionState *)$18;
					n->internalData = $19;
					$$ = (Node *)n;
				}
		| CREATE OptTemp TABLE qualified_name OF any_name
			OptTypedTableElementList OptWith OnCommitOption OptCompress OptPartitionElement
/* PGXC_BEGIN */
			OptDistributeBy OptSubCluster
/* PGXC_END */
				{
					CreateStmt *n = makeNode(CreateStmt);
					$4->relpersistence = $2;
					n->relation = $4;
					n->tableElts = $7;
					n->ofTypename = makeTypeNameFromNameList($6);
					n->ofTypename->location = @6;
					n->constraints = NIL;
					n->options = $8;
					n->oncommit = $9;
					n->row_compress = $10;
					n->tablespacename = $11;
					n->if_not_exists = false;
/* PGXC_BEGIN */
					n->distributeby = $12;
					n->subcluster = $13;
/* PGXC_END */
					n->partTableState = NULL;
					n->internalData = NULL;
					$$ = (Node *)n;
				}
		| CREATE OptTemp TABLE IF_P NOT EXISTS qualified_name OF any_name
			OptTypedTableElementList OptWith OnCommitOption OptCompress OptPartitionElement
/* PGXC_BEGIN */
			OptDistributeBy OptSubCluster
/* PGXC_END */
				{
					CreateStmt *n = makeNode(CreateStmt);
					$7->relpersistence = $2;
					n->relation = $7;
					n->tableElts = $10;
					n->ofTypename = makeTypeNameFromNameList($9);
					n->ofTypename->location = @9;
					n->constraints = NIL;
					n->options = $11;
					n->oncommit = $12;
					n->row_compress = $13;
					n->tablespacename = $14;
					n->if_not_exists = true;
/* PGXC_BEGIN */
					n->distributeby = $15;
					n->subcluster = $16;
/* PGXC_END */
					n->partTableState = NULL;
					n->internalData = NULL;
					$$ = (Node *)n;
				}
		;

OptKind:
	FOR MATERIALIZED VIEW
		{
			$$ = OBJECT_MATVIEW;
		}
	| /* empty */
		{
			$$ = OBJECT_TABLE;
		}
	;

opt_table_partitioning_clause:
		range_partitioning_clause
			{
				$$ = $1;
			}
		|hash_partitioning_clause
			{
				$$ = $1;
			}
		|list_partitioning_clause
			{
				$$ = $1;
			}
		|value_partitioning_clause
			{
				$$ = $1;
			}
		| /* empty */			{ $$ = NULL; }
		;

range_partitioning_clause:
		PARTITION BY RANGE '(' column_item_list ')'
		opt_interval_partition_clause '(' range_partition_definition_list ')' opt_row_movement_clause
			{
				PartitionState *n = makeNode(PartitionState);
				n->partitionKey = $5;
				n->intervalPartDef = (IntervalPartitionDefState *)$7;
				n->partitionList = $9;

				if (n->intervalPartDef)
					n->partitionStrategy = 'i';
				else
					n->partitionStrategy = 'r';

				n->rowMovement = (RowMovementValue)$11;

				$$ = (Node *)n;
			}
		;

list_partitioning_clause:
		PARTITION BY LIST '(' column_item_list ')'
		'(' list_partition_definition_list ')'
			{
#ifdef ENABLE_MULTIPLE_NODES
				ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Un-support feature"),
						errdetail("The distributed capability is not supported currently.")));
#endif
				if (list_length($5) != 1) {
					ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("Un-support feature"),
							errdetail("The partition key's length should be 1.")));
				}
				if (list_length($8) > 64) {
					ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Un-support feature"),
						errdetail("The partition's length should be less than 65.")));
				}
				PartitionState *n = makeNode(PartitionState);
				n->partitionKey = $5;
				n->intervalPartDef = NULL;
				n->partitionList = $8;
				n->partitionStrategy = 'l';
				$$ = (Node *)n;

			}
		;

hash_partitioning_clause:
		PARTITION BY IDENT '(' column_item_list ')'
		'(' hash_partition_definition_list ')'
			{
#ifdef ENABLE_MULTIPLE_NODES
				ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Un-support feature"),
						errdetail("The distributed capability is not supported currently.")));
#endif
				if (list_length($5) != 1) {
					ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("Un-support feature"),
							errdetail("The partition key's length should be 1.")));
				}
				if (strcmp($3, "hash") != 0) {
					ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("unrecognized option \"%s\"", $3)));	
				}
				if (list_length($8) > 64) {
					ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Un-support feature"),
						errdetail("The partition's length should be less than 65.")));
				}
				PartitionState *n = makeNode(PartitionState);
				n->partitionKey = $5;
				n->intervalPartDef = NULL;
				n->partitionList = $8;
				n->partitionStrategy = 'h';
				int i = 0;
				ListCell *elem = NULL;
				List *parts = n->partitionList;
				foreach(elem, parts) {
					HashPartitionDefState *hashPart = (HashPartitionDefState*)lfirst(elem);
					hashPart->boundary = list_make1(makeIntConst(i, -1));
					i++;
				}
				$$ = (Node *)n;

			}
		;

value_partitioning_clause:
		PARTITION BY VALUES '(' column_item_list ')'
			{
				PartitionState *n = makeNode(PartitionState);
				n->partitionKey = $5;
				n->partitionStrategy = 'v';

				$$ = (Node *)n;
			}
		;

column_item_list:
		column_item
			{
				$$ = list_make1($1);
			}
		| column_item_list ',' column_item
			{
				$$ = lappend($1, $3);
			}
		;

column_item:
		ColId
			{
				$$ = makeColumnRef($1, NIL, @1, yyscanner);
			}
		;

opt_interval_partition_clause:
		INTERVAL '(' interval_expr ')' opt_interval_tablespaceList
			{
				IntervalPartitionDefState* n = makeNode(IntervalPartitionDefState);
				n->partInterval = $3;
				n->intervalTablespaces = $5;

				$$ = (Node *)n;
			}
		| /* empty */
			{
				$$ = NULL;
			}
		;

opt_interval_tablespaceList:
		STORE_P IN_P  '(' tablespaceList ')'
			{
				$$= $4;
			}
		|
			{
				$$ = NIL;
			}
		;

interval_expr:
		a_expr
			{
				$$ = $1;
			}
		;

tablespaceList:
		name_list
			{
				$$ = $1;
			}
		;

range_partition_definition_list: /* general range partition syntax: start/end or less/than */
		range_less_than_list
			{
				$$ = $1;
			}
		| range_start_end_list
			{
				$$ = $1;
			}
		;

list_partition_definition_list:
		list_partition_item
			{
				$$ = list_make1($1);
			}
		| list_partition_definition_list ',' list_partition_item
			{
				$$ = lappend($1, $3);
			}
		;

hash_partition_definition_list:
		hash_partition_item
			{
				$$ = list_make1($1);
			}
		| hash_partition_definition_list ',' hash_partition_item
			{
				$$ = lappend($1, $3);
			}
		;

range_less_than_list:
		range_less_than_item
			{
				$$ = list_make1($1);
			}
		| range_less_than_list ',' range_less_than_item
			{
				$$ = lappend($1, $3);
			}
		;

list_partition_item:
		PARTITION name VALUES '(' expr_list ')' OptTableSpace
			{
				ListPartitionDefState *n = makeNode(ListPartitionDefState);
				n->partitionName = $2;
				n->boundary = $5;
				n->tablespacename = $7;

				$$ = (Node *)n;
			}
		| PARTITION name VALUES '(' DEFAULT ')' OptTableSpace
			{
				ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Un-support feature"),
						errdetail("The default list's partition is not supported currently."))); 
				ListPartitionDefState *n = makeNode(ListPartitionDefState);
				n->partitionName = $2;
				Const *n_default = makeNode(Const);
				n_default->ismaxvalue = true;
				n_default->location = -1;
				n->boundary = list_make1(n_default);
				n->tablespacename = $7;
				$$ = (Node *)n;
			}
		;

hash_partition_item:
		PARTITION name OptTableSpace
			{
				HashPartitionDefState *n = makeNode(HashPartitionDefState);
				n->partitionName = $2;
				n->tablespacename = $3;

				$$ = (Node*)n;
			}

range_less_than_item:
		PARTITION name VALUES LESS THAN
		'(' maxValueList ')' OptTableSpace
			{
				RangePartitionDefState *n = makeNode(RangePartitionDefState);
				n->partitionName = $2;
				n->boundary = $7;
				n->tablespacename = $9;

				$$ = (Node *)n;
			}
		;
		
range_start_end_list:
		range_start_end_item
			{
				$$ = list_make1($1);
			}
		| range_start_end_list ',' range_start_end_item
			{
				$$ = lappend($1, $3);
			}
		;
	
range_start_end_item:
		PARTITION name START '(' maxValueList ')'  END_P '(' maxValueList ')' opt_range_every_list OptTableSpace
			{
				RangePartitionStartEndDefState *n = makeNode(RangePartitionStartEndDefState);
				n->partitionName = $2;
				n->startValue = $5;
				n->endValue = $9;
				n->everyValue = $11;
				n->tableSpaceName = $12;

				$$ = (Node *)n;
			}
		| PARTITION name END_P '(' maxValueList ')' OptTableSpace
			{
				RangePartitionStartEndDefState *n = makeNode(RangePartitionStartEndDefState);
				n->partitionName = $2;
				n->startValue = NIL;
				n->endValue = $5;
				n->everyValue = NIL;
				n->tableSpaceName = $7;

				$$ = (Node *)n;
			}
		| PARTITION name START '(' maxValueList ')' OptTableSpace
			{
				RangePartitionStartEndDefState *n = makeNode(RangePartitionStartEndDefState);
				n->partitionName = $2;
				n->startValue = $5;
				n->endValue = NIL;
				n->everyValue = NIL;
				n->tableSpaceName = $7;

				$$ = (Node *)n;
			}
		;

opt_range_every_list:
		EVERY '(' maxValueList ')' 
			{
				$$ = $3;
			}
		| /* empty */ { $$ = NIL; }
		
partition_name:
		ColId
			{
				$$ = makeRangeVar(NULL, $1, @1);
			}
		;

maxValueList:
		maxValueItem
			{
				$$ = list_make1($1);
			}
		| maxValueList ',' maxValueItem
			{
				$$ = lappend($1, $3);
			}
		;

maxValueItem:
		a_expr
			{
				$$ = $1;
			}
		| MAXVALUE
			{
				Const *n = makeNode(Const);

				n->ismaxvalue = true;
				n->location = @1;

				$$ = (Node *)n;
			}
		;


opt_row_movement_clause: ENABLE_P ROW MOVEMENT		{ $$ = ROWMOVEMENT_ENABLE; }
			| DISABLE_P ROW MOVEMENT					{ $$ = ROWMOVEMENT_DISABLE; }
			| /*EMPTY*/								{ $$ = ROWMOVEMENT_DEFAULT; }
		;

/*
 * Redundancy here is needed to avoid shift/reduce conflicts,
 * since TEMP is not a reserved word.  See also OptTempTableName.
 *
 * NOTE: we accept both GLOBAL and LOCAL options.  They currently do nothing,
 * but future versions might consider GLOBAL to request SQL-spec-compliant
 * temp table behavior, so warn about that.  Since we have no modules the
 * LOCAL keyword is really meaningless; furthermore, some other products
 * implement LOCAL as meaning the same as our default temp table behavior,
 * so we'll probably continue to treat LOCAL as a noise word.
 */
OptTemp:	TEMPORARY					{ $$ = RELPERSISTENCE_TEMP; }
			| TEMP						{ $$ = RELPERSISTENCE_TEMP; }
			| LOCAL TEMPORARY			{ $$ = RELPERSISTENCE_TEMP; }
			| LOCAL TEMP				{ $$ = RELPERSISTENCE_TEMP; }
			| GLOBAL TEMPORARY
				{
#ifdef ENABLE_MULTIPLE_NODES
					ereport(WARNING,
							(errmsg("GLOBAL is deprecated in temporary table creation"),
							 parser_errposition(@1)));
					$$ = RELPERSISTENCE_TEMP;
#else
                                        $$ = RELPERSISTENCE_GLOBAL_TEMP;
#endif
				}
			| GLOBAL TEMP
				{
#ifdef ENABLE_MULTIPLE_NODES
					ereport(WARNING,
							(errmsg("GLOBAL is deprecated in temporary table creation"),
							 parser_errposition(@1)));
					$$ = RELPERSISTENCE_TEMP;
#else
                                        $$ = RELPERSISTENCE_GLOBAL_TEMP;
#endif
				}
			| UNLOGGED					{ $$ = RELPERSISTENCE_UNLOGGED; }
			| /*EMPTY*/					{ $$ = RELPERSISTENCE_PERMANENT; }
		;

OptTableElementList:
			TableElementList					{ $$ = $1; }
			| /*EMPTY*/							{ $$ = NIL; }
		;

OptTypedTableElementList:
			'(' TypedTableElementList ')'		{ $$ = $2; }
			| /*EMPTY*/							{ $$ = NIL; }
		;

TableElementList:
			TableElement
				{
					$$ = list_make1($1);
				}
			| TableElementList ',' TableElement
				{
					$$ = lappend($1, $3);
				}
		;

TypedTableElementList:
			TypedTableElement
				{
					$$ = list_make1($1);
				}
			| TypedTableElementList ',' TypedTableElement
				{
					$$ = lappend($1, $3);
				}
		;

TableElement:
			columnDef							{ $$ = $1; }
			| TableLikeClause					{ $$ = $1; }
			| TableConstraint					{ $$ = $1; }
		;

TypedTableElement:
			columnOptions						{ $$ = $1; }
			| TableConstraint	 				{ $$ = $1; }
		;

columnDef:	ColId Typename KVType ColCmprsMode create_generic_options ColQualList
				{
					ColumnDef *n = makeNode(ColumnDef);
					n->colname = $1;
					n->typname = $2;
					n->kvtype = $3;
					n->inhcount = 0;
					n->is_local = true;
					n->is_not_null = false;
					n->is_from_type = false;
					n->storage = 0;
					n->cmprs_mode = $4;
					n->raw_default = NULL;
					n->cooked_default = NULL;
					n->collOid = InvalidOid;
					n->fdwoptions = $5;
					SplitColQualList($6, &n->constraints, &n->collClause, &n->clientLogicColumnRef,
									 yyscanner);
					$$ = (Node *)n;
				}
		;

KVType: TSTAG		{$$ = ATT_KV_TAG;}  /* tag for kv storage */
		| TSFIELD	{$$ = ATT_KV_FIELD;}  /* field for kv storage */
		| TSTIME	{$$ = ATT_KV_TIMETAG;}  /* field for kv storage */
		| /* EMPTY */	{$$ = ATT_KV_UNDEFINED;} /* not using kv storage */
;

ColCmprsMode:	DELTA		{$$ = ATT_CMPR_DELTA;}  /* delta compression */
		| PREFIX	{$$ = ATT_CMPR_PREFIX;}  /* prefix compression */
		| DICTIONARY		{$$ = ATT_CMPR_DICTIONARY;}  /* dictionary compression */
		| NUMSTR	{$$ = ATT_CMPR_NUMSTR;}  /* number-string compression */
		| NOCOMPRESS	{$$ = ATT_CMPR_NOCOMPRESS;}  /* don't compress */
		| /* EMPTY */	{$$ = ATT_CMPR_UNDEFINED;} /* not specified by user */
;

columnOptions:	ColId WITH OPTIONS ColQualList
				{
					ColumnDef *n = makeNode(ColumnDef);
					n->colname = $1;
					n->typname = NULL;
					n->inhcount = 0;
					n->is_local = true;
					n->is_not_null = false;
					n->is_from_type = false;
					n->storage = 0;
					n->raw_default = NULL;
					n->cooked_default = NULL;
					n->collOid = InvalidOid;
					SplitColQualList($4, &n->constraints, &n->collClause, &n->clientLogicColumnRef,
									 yyscanner);
					$$ = (Node *)n;
				}
		;

ColQualList:
			ColQualList ColConstraint				{ $$ = lappend($1, $2); }
			| /*EMPTY*/								{ $$ = NIL; }
		;

ColConstraint:
			CONSTRAINT name ColConstraintElem
				{
					Constraint *n = (Constraint *) $3;
					AssertEreport(IsA(n, Constraint),
									MOD_OPT,
									"check node type inconsistant");
					n->conname = $2;
					n->location = @1;
					$$ = (Node *) n;
				}
			| ColConstraintElem						{ $$ = $1; }
			| ConstraintAttr						{ $$ = $1; }
			| COLLATE any_name
				{
					/*
					 * Note: the CollateClause is momentarily included in
					 * the list built by ColQualList, but we split it out
					 * again in SplitColQualList.
					 */
					CollateClause *n = makeNode(CollateClause);
					n->arg = NULL;
					n->collname = $2;
					n->location = @1;
					$$ = (Node *) n;
				}
			| ENCRYPTED with_algorithm
				{ 
					$$=$2;
				}
		;	
with_algorithm:
                       WITH '(' algorithm_desc ')'
                       {
                        $$=$3;
                       }
                       | /*EMPTY*/ {
                               ClientLogicColumnRef  *n = makeNode(ClientLogicColumnRef);
                               n->column_key_name=NULL;
                               n->columnEncryptionAlgorithmType = EncryptionType::DETERMINISTIC_TYPE;
                               n->orig_typname=NULL;
							   n->location=0;
                               $$ = (Node *) n;
                       }
               ;
algorithm_desc:
        datatypecl columnEncryptionKey ',' encryptionType
        {
            ClientLogicColumnRef  *n = makeNode(ClientLogicColumnRef);
            n->column_key_name=$2;
            n->columnEncryptionAlgorithmType = $4;
            n->orig_typname=NULL;
			n->dest_typname=$1;
            n->location=@2;
            $$ = (Node *) n;
        }
        | datatypecl encryptionType ',' columnEncryptionKey
        {
            ClientLogicColumnRef  *n = makeNode(ClientLogicColumnRef);
            n->column_key_name=$4;
            n->columnEncryptionAlgorithmType = $2;
            n->orig_typname=NULL;
			n->dest_typname=$1;
            n->location=@2;
            $$ = (Node *) n;
        }
		;
columnEncryptionKey:  COLUMN_ENCRYPTION_KEY '=' setting_name  {$$=$3; };
encryptionType:
                ENCRYPTION_TYPE '=' RANDOMIZED {$$ =EncryptionType::RANDOMIZED_TYPE;}
    |           ENCRYPTION_TYPE '=' DETERMINISTIC {$$ =EncryptionType::DETERMINISTIC_TYPE;     }
;
setting_name:
        ColId { $$ = check_setting_name(list_make1(makeString($1)), yyscanner); }
        | ColId indirection
            {
                $$ = check_setting_name(lcons(makeString($1), $2), yyscanner);
            }
        ;

CreateKeyStmt:
        CreateMasterKeyStmt             { $$ = $1; }
        | CreateColumnKeyStmt          { $$ = $1; }
        ;

CreateMasterKeyStmt: 
        CREATE CLIENT MASTER KEY setting_name WITH '(' master_key_params ')'
        {   
            CreateClientLogicGlobal *n = makeNode(CreateClientLogicGlobal);
            n->global_key_name = $5;
            
            ClientLogicGlobalParam *n1 = makeNode (ClientLogicGlobalParam);
            n1->key = ClientLogicGlobalProperty::CLIENT_GLOBAL_FUNCTION;
            n1->value = "encryption";
            // len is not filled on purpose ??

            n->global_setting_params = lappend($8, (Node*)n1);
            $$=(Node*) n;
        }
        ;

master_key_params:
        master_key_elem                         { $$ = list_make1($1); }
        | master_key_params ',' master_key_elem { $$ = lappend($1, $3); }
        ;

master_key_elem: 
        KEY_STORE '=' ColId
        {
            ClientLogicGlobalParam *n = makeNode (ClientLogicGlobalParam);
            n->key = ClientLogicGlobalProperty::CMK_KEY_STORE;
            n->value = $3;
            // len is not filled on purpose ??
            $$ = (Node*) n;
        }
        | KEY_PATH '=' ColId
        {
            ClientLogicGlobalParam *n = makeNode (ClientLogicGlobalParam);
            n->key = ClientLogicGlobalProperty::CMK_KEY_PATH;
            n->value =$3;
            // len is not filled on purpose ??
            $$ = (Node*) n;
        }
        | ALGORITHM '=' ColId
        {
            ClientLogicGlobalParam *n = makeNode (ClientLogicGlobalParam);
            n->key = ClientLogicGlobalProperty::CMK_ALGORITHM;
            n->value=$3;
            // len is not filled on purpose ??
            $$ = (Node*) n;
        }
        ;

CreateColumnKeyStmt:
        CREATE COLUMN ENCRYPTION KEY setting_name WITH VALUES '(' column_key_params ')'
        {
            CreateClientLogicColumn *n = makeNode(CreateClientLogicColumn);
            n->column_key_name = $5;

            ClientLogicColumnParam *n1 = makeNode (ClientLogicColumnParam);
            n1->key = ClientLogicColumnProperty::COLUMN_COLUMN_FUNCTION;
            n1->value = "encryption";
            // len is not filled on purpose ??

            n->column_setting_params = lappend($9, (Node*)n1);;
            $$=(Node*)n;
        }
        ;

column_key_params:
        column_key_elem                         { $$ = list_make1($1); }
        | column_key_params ',' column_key_elem { $$ = lappend($1, $3); }
        ;

column_key_elem: 
        CLIENT_MASTER_KEY '=' setting_name  {   
            ClientLogicColumnParam *n = makeNode (ClientLogicColumnParam);
            n->key = ClientLogicColumnProperty::CLIENT_GLOBAL_SETTING;
            n->value = NULL;
            n->qualname = $3;
            $$ = (Node*) n;
        }  
        | ALGORITHM '=' ColId
        {
            ClientLogicColumnParam *n = makeNode (ClientLogicColumnParam);
            n->key = ClientLogicColumnProperty::CEK_ALGORITHM;
            n->value =$3;
            n->qualname = NIL;
            $$ = (Node*) n;
        }
        | ENCRYPTED_VALUE '=' Sconst
        {
            ClientLogicColumnParam *n = makeNode (ClientLogicColumnParam);
            n->key = ClientLogicColumnProperty::CEK_EXPECTED_VALUE;
            n->value=$3;
            n->qualname = NIL;
            $$ = (Node*) n;
        }
        ;

datatypecl:
	DATATYPE_CL  '=' client_logic_type','
	{   
    	    $$ = $3;
	}
	| {$$= NULL;}
	;

/*
 * @HDFS
 * InformationalConstraintElem is used for informational constraint.
 */
InformationalConstraintElem:
            NOT_ENFORCED
                {
                    InformationalConstraint *n = makeNode(InformationalConstraint);
                    n->nonforced = true;
                    n->enableOpt = true;
                    $$ = (Node *) n;
                }
            | NOT_ENFORCED  DISABLE_P QUERY OPTIMIZATION
                {
                    InformationalConstraint *n = makeNode(InformationalConstraint);
                    n->nonforced = true;
                    n->enableOpt = false;
                    $$ = (Node *) n;
                }
            | NOT_ENFORCED  ENABLE_P QUERY OPTIMIZATION
                {
                    InformationalConstraint *n = makeNode(InformationalConstraint);
                    n->nonforced = true;
                    n->enableOpt = true;
                    $$ = (Node *) n;
                }
            | ENFORCED
                {
                    InformationalConstraint *n = makeNode(InformationalConstraint);
                    n->nonforced = false;
                    n->enableOpt = false;
                    $$ = (Node *) n;
                }
            | /*EMPTY*/
                {
                    InformationalConstraint *n = makeNode(InformationalConstraint);
                    n->nonforced = false;
                    n->enableOpt = false;
                    $$ = (Node *) n;
                }
        ;

/* DEFAULT NULL is already the default for Postgres.
 * But define it here and carry it forward into the system
 * to make it explicit.
 * - thomas 1998-09-13
 *
 * WITH NULL and NULL are not SQL92-standard syntax elements,
 * so leave them out. Use DEFAULT NULL to explicitly indicate
 * that a column may have that value. WITH NULL leads to
 * shift/reduce conflicts with WITH TIME ZONE anyway.
 * - thomas 1999-01-08
 *
 * DEFAULT expression must be b_expr not a_expr to prevent shift/reduce
 * conflict on NOT (since NOT might start a subsequent NOT NULL constraint,
 * or be part of a_expr NOT LIKE or similar constructs).
 */
ColConstraintElem:
			NOT NULL_P
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_NOTNULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| NOT NULL_P ENABLE_P
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_NOTNULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| NULL_P
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_NULL;
					n->location = @1;
					$$ = (Node *)n;
				}
			| UNIQUE opt_definition OptConsTableSpace InformationalConstraintElem
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_UNIQUE;
					n->location = @1;
					n->keys = NULL;
					n->options = $2;
					n->indexname = NULL;
					n->indexspace = $3;
					n->inforConstraint = (InformationalConstraint *) $4;
					$$ = (Node *)n;
				}
			| UNIQUE opt_definition OptConsTableSpace ENABLE_P InformationalConstraintElem
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_UNIQUE;
					n->location = @1;
					n->keys = NULL;
					n->options = $2;
					n->indexname = NULL;
					n->indexspace = $3;
					n->inforConstraint = (InformationalConstraint *) $5;
					$$ = (Node *)n;
				}
			| PRIMARY KEY opt_definition OptConsTableSpace InformationalConstraintElem
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_PRIMARY;
					n->location = @1;
					n->keys = NULL;
					n->options = $3;
					n->indexname = NULL;
					n->indexspace = $4;
					n->inforConstraint = (InformationalConstraint *) $5;
					$$ = (Node *)n;
				}
			| PRIMARY KEY opt_definition OptConsTableSpace ENABLE_P InformationalConstraintElem
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_PRIMARY;
					n->location = @1;
					n->keys = NULL;
					n->options = $3;
					n->indexname = NULL;
					n->indexspace = $4;
					n->inforConstraint = (InformationalConstraint *) $6;
					$$ = (Node *)n;
				}
			| CHECK '(' a_expr ')' opt_no_inherit
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_CHECK;
					n->location = @1;
					n->is_no_inherit = $5;
					n->raw_expr = $3;
					n->cooked_expr = NULL;
					$$ = (Node *)n;
				}
			| CHECK '(' a_expr ')' opt_no_inherit ENABLE_P
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_CHECK;
					n->location = @1;
					n->is_no_inherit = $5;
					n->raw_expr = $3;
					n->cooked_expr = NULL;
					$$ = (Node *)n;
				}
			| DEFAULT b_expr
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_DEFAULT;
					n->location = @1;
					n->raw_expr = $2;
					n->cooked_expr = NULL;
					$$ = (Node *)n;
				}
			| REFERENCES qualified_name opt_column_list key_match key_actions
				{
#ifdef 			ENABLE_MULTIPLE_NODES
					ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("REFERENCES constraint is not yet supported.")));
#endif						
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_FOREIGN;
					n->location = @1;
					n->pktable			= $2;
					n->fk_attrs			= NIL;
					n->pk_attrs			= $3;
					n->fk_matchtype		= $4;
					n->fk_upd_action	= (char) ($5 >> 8);
					n->fk_del_action	= (char) ($5 & 0xFF);
					n->skip_validation  = false;
					n->initially_valid  = true;
					$$ = (Node *)n;
				}
			| REFERENCES qualified_name opt_column_list key_match key_actions ENABLE_P
				{
					ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("REFERENCES constraint is not yet supported.")));
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_FOREIGN;
					n->location = @1;
					n->pktable = $2;
					n->fk_attrs = NIL;
					n->pk_attrs = $3;
					n->fk_matchtype = $4;
					n->fk_upd_action = (char) ($5 >> 8);
					n->fk_del_action = (char) ($5 & 0xFF);
					n->skip_validation  = false;
					n->initially_valid = true;
					$$ = (Node *)n;
				}
		;

/*
 * ConstraintAttr represents constraint attributes, which we parse as if
 * they were independent constraint clauses, in order to avoid shift/reduce
 * conflicts (since NOT might start either an independent NOT NULL clause
 * or an attribute).  parse_utilcmd.c is responsible for attaching the
 * attribute information to the preceding "real" constraint node, and for
 * complaining if attribute clauses appear in the wrong place or wrong
 * combinations.
 *
 * See also ConstraintAttributeSpec, which can be used in places where
 * there is no parsing conflict.  (Note: currently, NOT VALID and NO INHERIT
 * are allowed clauses in ConstraintAttributeSpec, but not here.  Someday we
 * might need to allow them here too, but for the moment it doesn't seem
 * useful in the statements that use ConstraintAttr.)
 */
ConstraintAttr:
			DEFERRABLE
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_ATTR_DEFERRABLE;
					n->location = @1;
					$$ = (Node *)n;
				}
			| NOT DEFERRABLE
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_ATTR_NOT_DEFERRABLE;
					n->location = @1;
					$$ = (Node *)n;
				}
			| INITIALLY DEFERRED
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_ATTR_DEFERRED;
					n->location = @1;
					$$ = (Node *)n;
				}
			| INITIALLY IMMEDIATE
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_ATTR_IMMEDIATE;
					n->location = @1;
					$$ = (Node *)n;
				}
		;


TableLikeClause:
			LIKE qualified_name TableLikeOptionList
				{
					TableLikeClause *n = makeNode(TableLikeClause);
					n->relation = $2;
					n->options = $3;
#ifndef ENABLE_MULTIPLE_NODES					
					if (IS_SINGLE_NODE && (n->options & CREATE_TABLE_LIKE_DISTRIBUTION))
					{
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("Un-support feature"),                                          
									errdetail("The distributed capability is not supported currently."))); 
					}
#endif					
					$$ = (Node *)n;
				}
			| LIKE qualified_name INCLUDING_ALL excluding_option_list
				{
					TableLikeClause *n = makeNode(TableLikeClause);
					n->relation = $2;
					n->options = CREATE_TABLE_LIKE_ALL & ~$4;
#ifndef ENABLE_MULTIPLE_NODES					
					if (IS_SINGLE_NODE) 
					{
						n->options = n->options & ~CREATE_TABLE_LIKE_DISTRIBUTION;
					}
#endif					
					$$ = (Node *)n;
				}
		;

excluding_option_list:
				excluding_option_list EXCLUDING TableLikeExcludingOption	{ $$ = $1 | $3; }
				| /* EMPTY */						{ $$ = 0; }
		;

TableLikeOptionList:
				TableLikeOptionList INCLUDING TableLikeIncludingOption	{ $$ = $1 | $3; }
				| TableLikeOptionList EXCLUDING TableLikeExcludingOption	{ $$ = $1 & ~$3; }
				| /* EMPTY */						{ $$ = CREATE_TABLE_LIKE_DEFAULTS_SERIAL; }
		;

TableLikeIncludingOption:
				DEFAULTS			{ $$ = CREATE_TABLE_LIKE_DEFAULTS | CREATE_TABLE_LIKE_DEFAULTS_SERIAL; }
				| CONSTRAINTS		{ $$ = CREATE_TABLE_LIKE_CONSTRAINTS; }
				| INDEXES			{ $$ = CREATE_TABLE_LIKE_INDEXES; }
				| STORAGE			{ $$ = CREATE_TABLE_LIKE_STORAGE; }
				| COMMENTS			{ $$ = CREATE_TABLE_LIKE_COMMENTS; }
				| PARTITION			{ $$ = CREATE_TABLE_LIKE_PARTITION; }
				| RELOPTIONS		{ $$ = CREATE_TABLE_LIKE_RELOPTIONS; }
				| DISTRIBUTION		{ $$ = CREATE_TABLE_LIKE_DISTRIBUTION; }
				| OIDS				{ $$ = CREATE_TABLE_LIKE_OIDS;}
		;

TableLikeExcludingOption:
				DEFAULTS			{ $$ = CREATE_TABLE_LIKE_DEFAULTS | CREATE_TABLE_LIKE_DEFAULTS_SERIAL; }
				| CONSTRAINTS		{ $$ = CREATE_TABLE_LIKE_CONSTRAINTS; }
				| INDEXES			{ $$ = CREATE_TABLE_LIKE_INDEXES; }
				| STORAGE			{ $$ = CREATE_TABLE_LIKE_STORAGE; }
				| COMMENTS			{ $$ = CREATE_TABLE_LIKE_COMMENTS; }
				| PARTITION			{ $$ = CREATE_TABLE_LIKE_PARTITION; }
				| RELOPTIONS		{ $$ = CREATE_TABLE_LIKE_RELOPTIONS; }
				| DISTRIBUTION		{ $$ = CREATE_TABLE_LIKE_DISTRIBUTION; }
				| OIDS				{ $$ = CREATE_TABLE_LIKE_OIDS; }
				| ALL				{ $$ = CREATE_TABLE_LIKE_ALL; }
		;

opt_internal_data: 
            INTERNAL DATA_P 	internal_data_body		{$$ = $3;}
			| /* EMPTY */      	{$$ = NULL;}
		;

internal_data_body: 	{
				int		begin 		= 0;
				int		end			= 0;
				char	*body 		= NULL;
				int		body_len 	= 0;

				int	tok = YYEMPTY;
				base_yy_extra_type *yyextra = pg_yyget_extra(yyscanner);

				if (yychar == YYEOF || yychar == YYEMPTY)
					tok = YYLEX;

				begin = yylloc;
				while(tok != YYEOF)
				{
					if (tok == ';')
					{
						end = yylloc;
					}
					tok = YYLEX;
				}

				if (end == 0)
					parser_yyerror("internal data of create statment is not ended correctly");

				body_len = end - begin + 1 ;

				body = (char *)palloc0(body_len + 1);
				strncpy(body,
					yyextra->core_yy_extra.scanbuf + begin - 1, body_len);

				body[body_len] = '\0';
				$$ = body;
			}
		;

/* ConstraintElem specifies constraint syntax which is not embedded into
 *	a column definition. ColConstraintElem specifies the embedded form.
 * - thomas 1997-12-03
 */
TableConstraint:
			CONSTRAINT name ConstraintElem
				{
					Constraint *n = (Constraint *) $3;
					Assert(IsA(n, Constraint));
					n->conname = $2;
					n->location = @1;
					$$ = (Node *) n;
				}
			| ConstraintElem  						{ $$ = $1; }
		;

ConstraintElem:
			CHECK '(' a_expr ')' ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_CHECK;
					n->location = @1;
					n->raw_expr = $3;
					n->cooked_expr = NULL;
					processCASbits($5, @5, "CHECK",
								   NULL, NULL, &n->skip_validation,
								   &n->is_no_inherit, yyscanner);
					n->initially_valid = !n->skip_validation;
					$$ = (Node *)n;
				}
			| UNIQUE '(' columnList ')' opt_c_include opt_definition OptConsTableSpace
				ConstraintAttributeSpec InformationalConstraintElem
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_UNIQUE;
					n->location = @1;
					n->keys = $3;
					n->including = $5;
					n->options = $6;
					n->indexname = NULL;
					n->indexspace = $7;
					processCASbits($8, @8, "UNIQUE",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					n->inforConstraint = (InformationalConstraint *) $9; /* informational constraint info */
					$$ = (Node *)n;
				}
			| UNIQUE ExistingIndex ConstraintAttributeSpec InformationalConstraintElem
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_UNIQUE;
					n->location = @1;
					n->keys = NIL;
					n->including = NIL;
					n->options = NIL;
					n->indexname = $2;
					n->indexspace = NULL;
					processCASbits($3, @3, "UNIQUE",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					n->inforConstraint = (InformationalConstraint *) $4; /* informational constraint info */
					$$ = (Node *)n;
				}
			| PRIMARY KEY '(' columnList ')' opt_c_include opt_definition OptConsTableSpace
				ConstraintAttributeSpec InformationalConstraintElem
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_PRIMARY;
					n->location = @1;
					n->keys = $4;
					n->including = $6;
					n->options = $7;
					n->indexname = NULL;
					n->indexspace = $8;
					processCASbits($9, @9, "PRIMARY KEY",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					n->inforConstraint = (InformationalConstraint *) $10; /* informational constraint info */
					$$ = (Node *)n;
				}
			| PRIMARY KEY ExistingIndex ConstraintAttributeSpec InformationalConstraintElem
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_PRIMARY;
					n->location = @1;
					n->keys = NIL;
					n->including = NIL;
					n->options = NIL;
					n->indexname = $3;
					n->indexspace = NULL;
					processCASbits($4, @4, "PRIMARY KEY",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					n->inforConstraint = (InformationalConstraint*) $5; /* informational constraint info */
					$$ = (Node *)n;
				}
			| EXCLUDE access_method_clause '(' ExclusionConstraintList ')'
				opt_c_include opt_definition OptConsTableSpace ExclusionWhereClause
				ConstraintAttributeSpec
				{
					ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("EXCLUDE constraint is not yet supported.")));
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_EXCLUSION;
					n->location = @1;
					n->access_method	= $2;
					n->exclusions		= $4;
					n->including		= $6;
					n->options			= $7;
					n->indexname		= NULL;
					n->indexspace		= $8;
					n->where_clause		= $9;
					processCASbits($10, @10, "EXCLUDE",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					$$ = (Node *)n;
				}
			| FOREIGN KEY '(' columnList ')' REFERENCES qualified_name
				opt_column_list key_match key_actions ConstraintAttributeSpec
				{
#ifdef 			ENABLE_MULTIPLE_NODES				
					ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("FOREIGN KEY ... REFERENCES constraint is not yet supported.")));
#endif						
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_FOREIGN;
					n->location = @1;
					n->pktable			= $7;
					n->fk_attrs			= $4;
					n->pk_attrs			= $8;
					n->fk_matchtype		= $9;
					n->fk_upd_action	= (char) ($10 >> 8);
					n->fk_del_action	= (char) ($10 & 0xFF);
					processCASbits($11, @11, "FOREIGN KEY",
								   &n->deferrable, &n->initdeferred,
								   &n->skip_validation, NULL,
								   yyscanner);
					n->initially_valid = !n->skip_validation;
					$$ = (Node *)n;
				}
			| PARTIAL CLUSTER KEY '(' columnList ')' ConstraintAttributeSpec
				{
					Constraint *n = makeNode(Constraint);
					n->contype = CONSTR_CLUSTER;
					n->location = @1;
					n->keys = $5;
					processCASbits($7, @7, "PARTIAL CLUSTER KEY",
								   NULL, NULL, NULL, NULL,
								   yyscanner);
					$$ = (Node *)n;
				}
		;

opt_no_inherit:	NO INHERIT							{  $$ = TRUE; }
			| /* EMPTY */							{  $$ = FALSE; }
		;

opt_column_list:
			'(' columnList ')'						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

columnList:
			columnElem								{ $$ = list_make1($1); }
			| columnList ',' columnElem				{ $$ = lappend($1, $3); }
		;

columnElem: ColId
				{
					$$ = (Node *) makeString($1);
				}
		;

opt_c_include:	INCLUDE '(' columnList ')'			{ $$ = $3; }
			 |		/* EMPTY */						{ $$ = NIL; }
		;

key_match:  MATCH FULL
			{
				$$ = FKCONSTR_MATCH_FULL;
			}
		| MATCH PARTIAL
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("MATCH PARTIAL not yet implemented"),
						 parser_errposition(@1)));
				$$ = FKCONSTR_MATCH_PARTIAL;
			}
		| MATCH SIMPLE
			{
				$$ = FKCONSTR_MATCH_UNSPECIFIED;
			}
		| /*EMPTY*/
			{
				$$ = FKCONSTR_MATCH_UNSPECIFIED;
			}
		;

ExclusionConstraintList:
			ExclusionConstraintElem					{ $$ = list_make1($1); }
			| ExclusionConstraintList ',' ExclusionConstraintElem
													{ $$ = lappend($1, $3); }
		;

ExclusionConstraintElem: index_elem WITH any_operator
			{
				$$ = list_make2($1, $3);
			}
			/* allow OPERATOR() decoration for the benefit of ruleutils.c */
			| index_elem WITH OPERATOR '(' any_operator ')'
			{
				$$ = list_make2($1, $5);
			}
		;

ExclusionWhereClause:
			WHERE '(' a_expr ')'					{ $$ = $3; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

/*
 * We combine the update and delete actions into one value temporarily
 * for simplicity of parsing, and then break them down again in the
 * calling production.  update is in the left 8 bits, delete in the right.
 * Note that NOACTION is the default.
 */
key_actions:
			key_update
				{ $$ = ($1 << 8) | (FKCONSTR_ACTION_NOACTION & 0xFF); }
			| key_delete
				{ $$ = (FKCONSTR_ACTION_NOACTION << 8) | ($1 & 0xFF); }
			| key_update key_delete
				{ $$ = ($1 << 8) | ($2 & 0xFF); }
			| key_delete key_update
				{ $$ = ($2 << 8) | ($1 & 0xFF); }
			| /*EMPTY*/
				{ $$ = (FKCONSTR_ACTION_NOACTION << 8) | (FKCONSTR_ACTION_NOACTION & 0xFF); }
		;

key_update: ON UPDATE key_action		{ $$ = $3; }
		;

key_delete: ON DELETE_P key_action		{ $$ = $3; }
		;

key_action:
			NO ACTION					{ $$ = FKCONSTR_ACTION_NOACTION; }
			| RESTRICT					{ $$ = FKCONSTR_ACTION_RESTRICT; }
			| CASCADE					{ $$ = FKCONSTR_ACTION_CASCADE; }
			| SET NULL_P				{ $$ = FKCONSTR_ACTION_SETNULL; }
			| SET DEFAULT				{ $$ = FKCONSTR_ACTION_SETDEFAULT; }
		;

OptInherit: INHERITS '(' qualified_name_list ')'
			{
				ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("CREATE TABLE ... INHERITS is not yet supported.")));
				$$ = $3;
			}
			| /*EMPTY*/								{ $$ = NIL; }
		;

/* WITH (options) is preferred, WITH OIDS and WITHOUT OIDS are legacy forms */
OptWith:
			WITH reloptions				{ $$ = $2; }
			| WITH OIDS
			{
				if (!u_sess->attr.attr_common.IsInplaceUpgrade)
					ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("CREATE TABLE ... WITH OIDS is not yet supported.")));
				$$ = list_make1(defWithOids(true));
			}
			| WITHOUT OIDS				{ $$ = list_make1(defWithOids(false)); }
			| /*EMPTY*/					{ $$ = NIL; }
		;

OnCommitOption:  ON COMMIT DROP				{ $$ = ONCOMMIT_DROP; }
			| ON COMMIT DELETE_P ROWS		{ $$ = ONCOMMIT_DELETE_ROWS; }
			| ON COMMIT PRESERVE ROWS		{ $$ = ONCOMMIT_PRESERVE_ROWS; }
			| /*EMPTY*/						{ $$ = ONCOMMIT_NOOP; }
		;

OptTableSpace:   TABLESPACE name					{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;
OptGPI: 	UPDATE GLOBAL INDEX 	{ $$ = TRUE; }
			| /*EMPTY*/				{ $$ = FALSE; }
		;
OptCompress: COMPRESS	{ $$ = REL_CMPRS_FIELDS_EXTRACT; }
			| NOCOMPRESS { $$ = REL_CMPRS_PAGE_PLAIN; }
			| /* EMPTY */ { $$ = REL_CMPRS_PAGE_PLAIN; }
		;

/* PGXC_BEGIN */
OptDistributeBy: OptDistributeByInternal			{ $$ = $1; }
			| /* EMPTY */							{ $$ = NULL; }
		;
OptDatanodeName:    DATANODE name					{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

/*
 * For the distribution type, we use IDENT to limit the impact of keywords
 * related to distribution on other commands and to allow extensibility for
 * new distributions.
 */
OptDistributeType: IDENT							{ $$ = $1; }
		;

OptDistributeByInternal:  DISTRIBUTE BY OptDistributeType '(' name_list ')'
				{
					DistributeBy *n = makeNode(DistributeBy);
#ifndef ENABLE_MULTIPLE_NODES					
					if (IS_SINGLE_NODE)
					{
						ereport(ERROR,          
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("Un-support feature"),
								errdetail("The distributed capability is not supported currently.")));			
					}
#endif					
					if (strcmp($3, "modulo") == 0)
						n->disttype = DISTTYPE_MODULO;
					else if (strcmp($3, "hash") == 0)
						n->disttype = DISTTYPE_HASH;
					else if (strcmp($3, "list") == 0 || strcmp($3, "range") == 0) {
						ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("%s distribution needs user-defined slice clause", $3)));
					} else
                        			ereport(ERROR,
                                			(errcode(ERRCODE_SYNTAX_ERROR),
                                 				errmsg("unrecognized distribution option \"%s\"", $3)));
					n->colname = $5;
					if (list_length(n->colname) > 1 && strcmp($3, "hash") != 0)
					{
                        			ereport(ERROR,
                                			(errcode(ERRCODE_SYNTAX_ERROR),
                                 				errmsg("The number of %s distribute key can not exceed 1", $3)));

					}
					$$ = n;
				}
			| DISTRIBUTE BY OptDistributeType
				{
					DistributeBy *n = makeNode(DistributeBy);
#ifndef ENABLE_MULTIPLE_NODES					
					if (IS_SINGLE_NODE)
					{
						ereport(ERROR,          
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("Un-support feature"),
								errdetail("The distributed capability is not supported currently.")));			
					}
#endif					
					if (strcmp($3, "replication") == 0)
                        n->disttype = DISTTYPE_REPLICATION;
					else if (strcmp($3, "roundrobin") == 0)
						n->disttype = DISTTYPE_ROUNDROBIN;
                    else if (strcmp($3, "hidetag") == 0)
                        n->disttype = DISTTYPE_HIDETAG;
                    else
                        ereport(ERROR,
                                (errcode(ERRCODE_SYNTAX_ERROR),
                                 errmsg("unrecognized distribution option \"%s\"", $3)));
					n->colname = NULL;
					$$ = n;
				}
			| distribute_by_range_clause
				{
					$$ = $1;
				}
			| distribute_by_list_clause
				{
					$$ = $1;
				}
		;


distribute_by_list_clause: /* distribute by list ..., or distribute by list ... slice reference base_table */
			DISTRIBUTE BY LIST '(' name_list ')' OptListDistribution
				{
#ifndef ENABLE_MULTIPLE_NODES					

					ereport(ERROR,          
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("Un-support feature"),
							errdetail("The distributed capability is not supported currently.")));			

#endif					

					DistributeBy *n = makeNode(DistributeBy);
					n->disttype = DISTTYPE_LIST;
					n->colname = $5;
					n->distState = (DistState *)$7; 

					if (list_length(n->colname) > 4)
					{
                        			ereport(ERROR,
                                			(errcode(ERRCODE_SYNTAX_ERROR),
                                 				errmsg("The number of LIST distribution keys can not exceed 4")));

					}
					$$ = n;
				}
		;

OptListDistribution: 
		'(' list_dist_state ')'
			{ 
				$$ = $2; 
			}
		| SliceReferenceClause
			{
				DistState *n = makeNode(DistState);
				n->strategy = 'l';
				n->refTableName = $1;
				$$ = (Node *)n;
			}
		;

list_dist_state: /* DistState Struct for LIST distribution syntax */
		list_distribution_rules_list
			{
				DistState *n = makeNode(DistState);
				n->strategy = 'l';
				n->sliceList = $1;
				$$ = (Node *)n;
			}
		;

list_distribution_rules_list: /* list of DistSliceValue Struct for LIST distribution syntax */
		list_dist_value
			{
				$$ = list_make1($1);
			}
		| list_distribution_rules_list ',' list_dist_value
			{
				$$ = lappend($1, $3);
			}
		;

list_dist_value:
		SLICE name VALUES '(' list_distribution_rule_row ')' OptDatanodeName
			{
				ListSliceDefState *n = makeNode(ListSliceDefState);
				n->name = $2;
				n->boundaries = $5;
				n->datanode_name = $7;
				$$ = (Node *)n;
			}
		| SLICE name VALUES '(' DEFAULT ')' OptDatanodeName
			{
				Const *m = makeNode(Const);
				m->ismaxvalue = true;
				m->location = @1;

				ListSliceDefState *n = makeNode(ListSliceDefState);
				List *boundary = list_make1((void *)m); 
				n->boundaries = list_make1((void *)boundary);
				n->name = $2;
				n->datanode_name = $7;
				$$ = (Node *)n;
			}
		;

list_distribution_rule_row:  /* ListSliceDefState Struct for LIST distribution syntax */
		list_distribution_rule_single
			{
				$$ = list_make1($1); 
			}
		| list_distribution_rule_row ',' list_distribution_rule_single
			{
				$$ = lappend($1, $3);
			}
		;

list_distribution_rule_single:
		'(' expr_list ')' 
			{
				$$ = $2;
			}
		| c_expr_noparen
			{
				$$ = list_make1($1);
			}
		;



distribute_by_range_clause:
		DISTRIBUTE BY RANGE '(' name_list ')'  '(' range_slice_definition_list ')'
			{
#ifndef ENABLE_MULTIPLE_NODES					
					ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("Un-support feature"),
							errdetail("The distributed capability is not supported currently.")));			
#endif
				DistributeBy *n = makeNode(DistributeBy);
				n->disttype = DISTTYPE_RANGE;
				n->colname = $5;
				DistState *n1 = makeNode(DistState);
				n1->strategy = 'r';
				n1->sliceList = $8;
				n->distState = n1;

				if (list_length(n->colname) > 4) {
					ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("The number of range distribution key can not exceed 4")));
				}

				$$ = n;
			}
		| DISTRIBUTE BY RANGE '(' name_list ')' SliceReferenceClause
			{
#ifndef ENABLE_MULTIPLE_NODES					
				ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("Un-support feature"),
						errdetail("The distributed capability is not supported currently.")));			
#endif
				DistributeBy *n = makeNode(DistributeBy);
				n->disttype = DISTTYPE_RANGE;
				n->colname = $5;
				DistState *n1 = makeNode(DistState);
				n1->strategy = 'r';
				n1->refTableName = $7;
				n->distState = n1;
				$$ = n;
			}
		;

SliceReferenceClause:
		SLICE REFERENCES name
			{
				$$ = $3;
			}
		;

range_slice_definition_list: /* general range slice syntax: values less than or start .. end */
		range_slice_less_than_list
			{
				$$ = $1;
			}
		| range_slice_start_end_list
			{
				$$ = $1;
			}
		;

range_slice_less_than_list:
		range_slice_less_than_item
			{
				$$ = list_make1($1);
			}
		| range_slice_less_than_list ',' range_slice_less_than_item
			{
				$$ = lappend($1, $3);
			}
		;

range_slice_less_than_item:
		SLICE name VALUES LESS THAN '(' maxValueList ')' OptDatanodeName
			{
				RangePartitionDefState *n = makeNode(RangePartitionDefState);
				n->partitionName = $2;
				n->boundary = $7;
				n->tablespacename = $9;

				$$ = (Node *)n;
			}
		;

range_slice_start_end_list:
		range_slice_start_end_item
			{
				$$ = list_make1($1);
			}
		| range_slice_start_end_list ',' range_slice_start_end_item
			{
				$$ = lappend($1, $3);
			}
		;

range_slice_start_end_item:
		SLICE name START '(' maxValueList ')' END_P '(' maxValueList ')' opt_range_every_list
			{
				RangePartitionStartEndDefState *n = makeNode(RangePartitionStartEndDefState);
				n->partitionName = $2;
				n->startValue = $5;
				n->endValue = $9;
				n->everyValue = $11;

				$$ = (Node *)n;
			}
		| SLICE name END_P '(' maxValueList ')'
			{
				RangePartitionStartEndDefState *n = makeNode(RangePartitionStartEndDefState);
				n->partitionName = $2;
				n->startValue = NIL;
				n->endValue = $5;
				n->everyValue = NIL;

				$$ = (Node *)n;
			}
		| SLICE name START '(' maxValueList ')'
			{
				RangePartitionStartEndDefState *n = makeNode(RangePartitionStartEndDefState);
				n->partitionName = $2;
				n->startValue = $5;
				n->endValue = NIL;
				n->everyValue = NIL;

				$$ = (Node *)n;
			}
		;

OptSubCluster:
			OptSubClusterInternal
				{
					$$ = $1;
				}
			| /* EMPTY */							{ $$ = NULL; }
		;

OptSubClusterInternal:
			TO NODE pgxcnodes
				{
					PGXCSubCluster *n = makeNode(PGXCSubCluster);
					n->clustertype = SUBCLUSTER_NODE;
					n->members = $3;
					$$ = n;
				}
			| TO GROUP_P pgxcgroup_name
				{
					PGXCSubCluster *n = makeNode(PGXCSubCluster);
					n->clustertype = SUBCLUSTER_GROUP;
					n->members = list_make1(makeString($3));
					$$ = n;
				}
		;
/* PGXC_END */

OptConsTableSpace:   USING INDEX OptPartitionElement	{ $$ = $3; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

OptPartitionElement:
		OptTableSpace OptPctFree OptInitRans OptMaxTrans OptStorage
			{
				$$ = $1;
			}
		;

OptPctFree:
			PCTFREE Iconst
			| /* empty */
		;

OptInitRans:
			INITRANS Iconst
			| /* empty */
		;

OptMaxTrans:
			MAXTRANS Iconst
			| /* empty */
		;

OptStorage:
		STORAGE '(' OptInitial OptNext OptMinextents OptMaxextents ')'
		| /* empty */
		;
OptInitial:
		INITIAL_P Iconst IDENT
		| /* empty */
		;

OptNext:
		NEXT Iconst IDENT
		| /*empty*/
		;

OptMinextents:
		MINEXTENTS Iconst
		| /*empty*/
		;

OptMaxextents:
		MAXEXTENTS UNLIMITED
		| MAXEXTENTS Iconst
		| /*empty*/
		;

ExistingIndex:   USING INDEX index_name				{ $$ = $3; }
		;


/*****************************************************************************
 *
 *		QUERY :
 *				CREATE TABLE relname AS SelectStmt [ WITH [NO] DATA ]
 *
 *
 * Note: SELECT ... INTO is a now-deprecated alternative for this.
 *
 *****************************************************************************/

CreateAsStmt:
		CREATE OptTemp TABLE create_as_target AS SelectStmt opt_with_data
				{
					CreateTableAsStmt *ctas = makeNode(CreateTableAsStmt);
					ctas->query = $6;
					ctas->into = $4;
					ctas->relkind = OBJECT_TABLE;
					ctas->is_select_into = false;
					/* cram additional flags into the IntoClause */
					$4->rel->relpersistence = $2;
					$4->skipData = !($7);
					$$ = (Node *) ctas;
				}
		;

create_as_target:
			qualified_name opt_column_list OptWith OnCommitOption OptCompress OptTableSpace
/* PGXC_BEGIN */
			OptDistributeBy OptSubCluster
/* PGXC_END */
				{
					$$ = makeNode(IntoClause);
					$$->rel = $1;
					$$->colNames = $2;
					$$->options = $3;
					$$->onCommit = $4;
					$$->row_compress = $5;
					$$->tableSpaceName = $6;
					$$->skipData = false;		/* might get changed later */
/* PGXC_BEGIN */
					$$->distributeby = $7;
					$$->subcluster = $8;
					$$->relkind = INTO_CLAUSE_RELKIND_DEFAULT;
/* PGXC_END */
				}
		;

opt_with_data:
			WITH DATA_P								{ $$ = TRUE; }
			| WITH NO DATA_P						{ $$ = FALSE; }
			| /*EMPTY*/								{ $$ = TRUE; }
		;

/*****************************************************************************
 *
 *	   QUERY :
 *			   CREATE MATERIALIZED VIEW relname AS SelectStmt
 *
 *****************************************************************************/

CreateMatViewStmt:
	   CREATE OptNoLog opt_incremental MATERIALIZED VIEW create_mv_target AS SelectStmt opt_with_data
			   {
				   CreateTableAsStmt *ctas = makeNode(CreateTableAsStmt);
				   ctas->query = $8;
				   ctas->into = $6;
				   ctas->relkind = OBJECT_MATVIEW;
				   ctas->is_select_into = false;
				   /* cram additional flags into the IntoClause */
				   $6->rel->relpersistence = $2;
				   $6->skipData = !($9);
				   if ($6->skipData) {
				        ereport(ERROR,
				            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				                errmsg("WITH NO DATA for materialized views not yet supported")));
                   }
                   if ($3 && $6->options) {
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("options for incremental materialized views not yet supported")));
                   }
#ifndef ENABLE_MULTIPLE_NODES
                   if ($3 && $6->distributeby) {
                        ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                errmsg("It's not supported to specify distribute key on incremental materialized views")));
                   }
#endif
				   $6->ivm = $3;
				   $$ = (Node *) ctas;
			   }
	   ;

create_mv_target:
		   qualified_name opt_column_list
/* PGXC_BEGIN */
           OptDistributeBy
/* PGXC_END */
		   opt_reloptions OptTableSpace
			   {
				   $$ = makeNode(IntoClause);
				   $$->rel = $1;
				   $$->colNames = $2;
				   $$->options = $4;
				   $$->onCommit = ONCOMMIT_NOOP;
				   $$->tableSpaceName = $5;
				   $$->skipData = false;	   /* might get changed later */
				   $$->ivm = false;
/* PGXC_BEGIN */
                   $$->distributeby = $3;
/* PGXC_END */
				   $$->relkind = INTO_CLAUSE_RELKIND_DEFAULT;
			   }
	   ;

OptNoLog:  UNLOGGED 				   { $$ = RELPERSISTENCE_UNLOGGED; }
		   | /*EMPTY*/				   { $$ = RELPERSISTENCE_PERMANENT; }
	   ;

opt_incremental:
			INCREMENTAL							{ $$ = TRUE; }
			| /*EMPTY*/							{ $$ = FALSE; }
		;


/*****************************************************************************
 *
 *     QUERY :
 *             REFRESH MATERIALIZED VIEW qualified_name
 *
 *****************************************************************************/

RefreshMatViewStmt:
           REFRESH opt_incremental MATERIALIZED VIEW qualified_name opt_with_data
               {
                   RefreshMatViewStmt *n = makeNode(RefreshMatViewStmt);
                   n->relation = $5;
                   n->incremental = $2;
                   n->skipData = !($6);
                   if (n->skipData) {
                       ereport(ERROR,
                           (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						    errmsg("WITH NO DATA for materialized views not yet supported")));
                   }
                   $$ = (Node *) n;
               }
       ;


/*****************************************************************************
 *
 *		QUERY :
 *				CREATE SEQUENCE seqname
 *				ALTER SEQUENCE seqname
 *
 *****************************************************************************/

CreateSeqStmt:
			CREATE OptTemp SEQUENCE qualified_name OptSeqOptList
				{
					CreateSeqStmt *n = makeNode(CreateSeqStmt);
					$4->relpersistence = $2;
					n->sequence = $4;
					n->options = $5;
					n->ownerId = InvalidOid;
/* PGXC_BEGIN */
					n->is_serial = false;
/* PGXC_END */
					n->uuid = 0;
					n->canCreateTempSeq = false;
					$$ = (Node *)n;
				}
		;

AlterSeqStmt:
			ALTER SEQUENCE qualified_name SeqOptList
				{
					AlterSeqStmt *n = makeNode(AlterSeqStmt);
					n->sequence = $3;
					n->options = $4;
					n->missing_ok = false;
/* PGXC_BEGIN */
					n->is_serial = false;
/* PGXC_END */
					$$ = (Node *)n;
				}
			| ALTER SEQUENCE IF_P EXISTS qualified_name SeqOptList
				{
					AlterSeqStmt *n = makeNode(AlterSeqStmt);
					n->sequence = $5;
					n->options = $6;
					n->missing_ok = true;
					$$ = (Node *)n;
				}

		;

OptSeqOptList: SeqOptList							{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

SeqOptList: SeqOptElem								{ $$ = list_make1($1); }
			| SeqOptList SeqOptElem					{ $$ = lappend($1, $2); }
		;

SeqOptElem: CACHE NumericOnly
				{
					$$ = makeDefElem("cache", (Node *)$2);
				}
			| CYCLE
				{
					$$ = makeDefElem("cycle", (Node *)makeInteger(TRUE));
				}
			| NO CYCLE
				{
					$$ = makeDefElem("cycle", (Node *)makeInteger(FALSE));
				}
			| INCREMENT opt_by NumericOnly
				{
					$$ = makeDefElem("increment", (Node *)$3);
				}
			| MAXVALUE NumericOnly
				{
					$$ = makeDefElem("maxvalue", (Node *)$2);
				}
			| MINVALUE NumericOnly
				{
					$$ = makeDefElem("minvalue", (Node *)$2);
				}
			| NO MAXVALUE
				{
					$$ = makeDefElem("maxvalue", NULL);
				}
			| NO MINVALUE
				{
					$$ = makeDefElem("minvalue", NULL);
				}
			| OWNED BY any_name
				{
					$$ = makeDefElem("owned_by", (Node *)$3);
				}
			| START opt_with NumericOnly
				{
					$$ = makeDefElem("start", (Node *)$3);
				}
			| RESTART
				{
					$$ = makeDefElem("restart", NULL);
				}
			| RESTART opt_with NumericOnly
				{
					$$ = makeDefElem("restart", (Node *)$3);
				}
			| NOCYCLE
				{
					$$ = makeDefElem("cycle", (Node *)makeInteger(FALSE));
				}
			| NOMAXVALUE
				{
					$$ = makeDefElem("maxvalue", NULL);
				}
			| NOMINVALUE
				{
					$$ = makeDefElem("minvalue", NULL);
				}
		;

opt_by:		BY				{}
			| /* empty */	{}
	  ;

NumericOnly:
			FCONST								{ $$ = makeFloat($1); }
			| '+' FCONST						{ $$ = makeFloat($2); }
			| '-' FCONST
				{
					$$ = makeFloat($2);
					doNegateFloat($$);
				}
			| SignedIconst						{ $$ = makeInteger($1); }
		;

NumericOnly_list:	NumericOnly						{ $$ = list_make1($1); }
				| NumericOnly_list ',' NumericOnly	{ $$ = lappend($1, $3); }
		;

/*****************************************************************************
 *
 *		QUERIES :
 *				CREATE [OR REPLACE] [TRUSTED] [PROCEDURAL] LANGUAGE ...
 *				DROP [PROCEDURAL] LANGUAGE ...
 *
 *****************************************************************************/

CreatePLangStmt:
			CREATE opt_or_replace opt_trusted opt_procedural LANGUAGE ColId_or_Sconst
			{
				CreatePLangStmt *n = makeNode(CreatePLangStmt);
				n->replace = $2;
				n->plname = $6;
				/* parameters are all to be supplied by system */
				n->plhandler = NIL;
				n->plinline = NIL;
				n->plvalidator = NIL;
				n->pltrusted = false;
				$$ = (Node *)n;
			}
			| CREATE opt_or_replace opt_trusted opt_procedural LANGUAGE ColId_or_Sconst
			  HANDLER handler_name opt_inline_handler opt_validator
			{
				CreatePLangStmt *n = makeNode(CreatePLangStmt);
				n->replace = $2;
				n->plname = $6;
				n->plhandler = $8;
				n->plinline = $9;
				n->plvalidator = $10;
				n->pltrusted = $3;
				$$ = (Node *)n;
			}
		;

opt_trusted:
			TRUSTED									{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

/* This ought to be just func_name, but that causes reduce/reduce conflicts
 * (CREATE LANGUAGE is the only place where func_name isn't followed by '(').
 * Work around by using simple names, instead.
 */
handler_name:
			name						{ $$ = list_make1(makeString($1)); }
			| name attrs				{ $$ = lcons(makeString($1), $2); }
		;

opt_inline_handler:
			INLINE_P handler_name					{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

validator_clause:
			VALIDATOR handler_name					{ $$ = $2; }
			| NO VALIDATOR							{ $$ = NIL; }
		;

opt_validator:
			validator_clause						{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

DropPLangStmt:
			DROP opt_procedural LANGUAGE ColId_or_Sconst opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_LANGUAGE;
					n->objects = list_make1(list_make1(makeString($4)));
					n->arguments = NIL;
					n->behavior = $5;
					n->missing_ok = false;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP opt_procedural LANGUAGE IF_P EXISTS ColId_or_Sconst opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_LANGUAGE;
					n->objects = list_make1(list_make1(makeString($6)));
					n->behavior = $7;
					n->missing_ok = true;
					n->concurrent = false;
					$$ = (Node *)n;
				}
		;

opt_procedural:
			PROCEDURAL								{}
			| /*EMPTY*/								{}
		;

tblspc_options:
			'(' tblspc_option_list ')'					{ $$ = $2; }
		;
opt_tblspc_options:		WITH tblspc_options					{ $$ = $2; }
			 |	/* EMPTY */						{ $$ = NIL; }
		;

tblspc_option_list:
			tblspc_option_elem						{ $$ = list_make1($1); }
			| tblspc_option_list ',' tblspc_option_elem			{ $$ = lappend($1, $3); }
		;

tblspc_option_elem:
			ColLabel '=' Sconst
			{
				$$ = makeDefElem($1, (Node *) makeString($3));
			}
			| ColLabel '=' func_type
			{
				if(0 != pg_strcasecmp($1, "filesystem"))
				{
					ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("Must use single quoted string for \"%s\" option.", $1)));
				}
				$$ = makeDefElem($1, (Node *) $3);
			}
			| ColLabel '=' NumericOnly
			{
				$$ = makeDefElem($1, (Node *) $3);
			}
		;
/*****************************************************************************
 *
 *		QUERY:
 *             CREATE TABLESPACE tablespace LOCATION '/path/to/tablespace/'
 *
 *****************************************************************************/

CreateTableSpaceStmt: CREATE TABLESPACE name OptTableSpaceOwner OptRelative LOCATION Sconst OptMaxSize opt_tblspc_options
				{
					CreateTableSpaceStmt *n = makeNode(CreateTableSpaceStmt);
					n->tablespacename = $3;
					n->owner = $4;
					n->location = $7;
					n->maxsize = $8;
					n->options = $9;
					n->relative = $5;
					$$ = (Node *) n;
				}
				| CREATE TABLESPACE name LoggingStr DATAFILE Sconst OptDatafileSize OptReuse OptAuto
				{
					CreateTableSpaceStmt *n = makeNode(CreateTableSpaceStmt);
					n->tablespacename = $3;
					n->owner = NULL;
					n->location = $6;
					n->maxsize = $7;
					$$ = (Node *) n;
				}
				| CREATE TABLESPACE name DATAFILE Sconst OptDatafileSize OptReuse OptAuto LoggingStr
				{
					CreateTableSpaceStmt *n = makeNode(CreateTableSpaceStmt);
					n->tablespacename = $3;
					n->owner = NULL;
					n->location = $5;
					n->maxsize = $6;
					$$ = (Node *) n;
				}
				| CREATE TABLESPACE name DATAFILE Sconst OptDatafileSize OptReuse OptAuto
				{
					CreateTableSpaceStmt *n = makeNode(CreateTableSpaceStmt);
					n->tablespacename = $3;
					n->owner = NULL;
					n->location = $5;
					n->maxsize = $6;
					$$ = (Node *) n;
				}
		;

LoggingStr:
			LOGGING									{ $$ = NULL ;}
			| NOLOGGING								{ $$ = NULL ;}
		;

OptDatafileSize:
			SIZE Iconst IDENT 							{ $$ = NULL; }
			| /*EMPTY */								{ $$ = NULL; }
		;

OptReuse:
			REUSE									{ $$ = NULL; }
			| /*EMPTY */								{ $$ = NULL; }
		;

OptAuto:
			AUTOEXTEND ON OptNextStr OptMaxSize					{ $$ = NULL; }
			| AUTOEXTEND OFF							{ $$ = NULL; }
			| /*EMPTY */								{ $$ = NULL; }
		;

OptNextStr:
			NEXT Iconst IDENT							{ $$ = NULL; }
			| /*EMPTY */								{ $$ = NULL; }
		;

OptMaxSize:
			MAXSIZE Sconst								{ $$ = $2; }
 			| /*EMPTY */								{  $$ = NULL;}
		;
size_clause:
			Sconst						        		{ $$ = $1; }
			| UNLIMITED                                 { $$ = "unlimited"; }
			;

OptRelative:
			RELATIVE_P									{ $$ = true; }
			| /*EMPTY */	 							{ $$ = false; }


OptTableSpaceOwner: OWNER name			{ $$ = $2; }
			| /*EMPTY */				{ $$ = NULL; }
		;

/*****************************************************************************
 *
 *		QUERY :
 *				DROP TABLESPACE <tablespace>
 *
 *		No need for drop behaviour as we cannot implement dependencies for
 *		objects in other databases; we can only support RESTRICT.
 *
 ****************************************************************************/

DropTableSpaceStmt: DROP TABLESPACE name
				{
					DropTableSpaceStmt *n = makeNode(DropTableSpaceStmt);
					n->tablespacename = $3;
					n->missing_ok = false;
					$$ = (Node *) n;
				}
				|  DROP TABLESPACE IF_P EXISTS name
				{
					DropTableSpaceStmt *n = makeNode(DropTableSpaceStmt);
					n->tablespacename = $5;
					n->missing_ok = true;
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 *		QUERY:
 *             CREATE EXTENSION extension
 *             [ WITH ] [ SCHEMA schema ] [ VERSION version ] [ FROM oldversion ]
 *
 *****************************************************************************/

CreateExtensionStmt: CREATE EXTENSION name opt_with create_extension_opt_list
				{
					CreateExtensionStmt *n = makeNode(CreateExtensionStmt);
					n->extname = $3;
					n->if_not_exists = false;
					n->options = $5;
					$$ = (Node *) n;
				}
				| CREATE EXTENSION IF_P NOT EXISTS name opt_with create_extension_opt_list
				{
					CreateExtensionStmt *n = makeNode(CreateExtensionStmt);
					n->extname = $6;
					n->if_not_exists = true;
					n->options = $8;
					$$ = (Node *) n;
				}
		;

create_extension_opt_list:
			create_extension_opt_list create_extension_opt_item
				{ $$ = lappend($1, $2); }
			| /* EMPTY */
				{ $$ = NIL; }
		;

create_extension_opt_item:
			SCHEMA name
				{
					$$ = makeDefElem("schema", (Node *)makeString($2));
				}
			| VERSION_P ColId_or_Sconst
				{
					$$ = makeDefElem("new_version", (Node *)makeString($2));
				}
			| FROM ColId_or_Sconst
				{
					$$ = makeDefElem("old_version", (Node *)makeString($2));
				}
		;
/*****************************************************************************
 *
 *		QUERY:
 *             CREATE OR REPLACE DIRECTORY directory AS '/path/to/directory/'
 *
 *****************************************************************************/

CreateDirectoryStmt:  CREATE opt_or_replace DIRECTORY name AS Sconst
				{
					CreateDirectoryStmt *n = makeNode(CreateDirectoryStmt);
					n->replace = $2;
					n->directoryname = $4;
					n->location = $6;
					n->owner = NULL;
					$$ = (Node *)n;
				}
	       	;

/*****************************************************************************
 *
 *		QUERY :
 *				DROP DIRECTORY [IF EXISTS] directory
 *
 ****************************************************************************/

DropDirectoryStmt: DROP DIRECTORY name
				{
					DropDirectoryStmt *n = makeNode(DropDirectoryStmt);
					n->directoryname = $3;
					n->missing_ok = false;
					$$ = (Node *) n;
				}
		 | DROP DIRECTORY IF_P EXISTS name
                                { 
					DropDirectoryStmt *n = makeNode(DropDirectoryStmt);
					n->directoryname = $5;
					n->missing_ok = true;
					$$ = (Node *) n;
                                }
		;

/*****************************************************************************
 *
 * ALTER EXTENSION name UPDATE [ TO version ]
 *
 *****************************************************************************/

AlterExtensionStmt: ALTER EXTENSION name UPDATE alter_extension_opt_list
				{
					AlterExtensionStmt *n = makeNode(AlterExtensionStmt);
					n->extname = $3;
					n->options = $5;
					$$ = (Node *) n;
				}
		;

alter_extension_opt_list:
			alter_extension_opt_list alter_extension_opt_item
				{ $$ = lappend($1, $2); }
			| /* EMPTY */
				{ $$ = NIL; }
		;

alter_extension_opt_item:
			TO ColId_or_Sconst
				{
					$$ = makeDefElem("new_version", (Node *)makeString($2));
				}
		;

/*****************************************************************************
 *
 * ALTER EXTENSION name ADD/DROP object-identifier
 *
 *****************************************************************************/

AlterExtensionContentsStmt:
			ALTER EXTENSION name add_drop AGGREGATE func_name aggr_args
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_AGGREGATE;
					n->objname = $6;
					n->objargs = $7;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION name add_drop CAST '(' Typename AS Typename ')'
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_CAST;
					n->objname = list_make1($7);
					n->objargs = list_make1($9);
					$$ = (Node *) n;
				}
			| ALTER EXTENSION name add_drop COLLATION any_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_COLLATION;
					n->objname = $6;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION name add_drop CONVERSION_P any_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_CONVERSION;
					n->objname = $6;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION name add_drop DOMAIN_P any_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_DOMAIN;
					n->objname = $6;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION name add_drop FUNCTION function_with_argtypes
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_FUNCTION;
					n->objname = $6->funcname;
					n->objargs = $6->funcargs;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION name add_drop opt_procedural LANGUAGE name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_LANGUAGE;
					n->objname = list_make1(makeString($7));
					$$ = (Node *)n;
				}
			| ALTER EXTENSION name add_drop OPERATOR any_operator oper_argtypes
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_OPERATOR;
					n->objname = $6;
					n->objargs = $7;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION name add_drop OPERATOR CLASS any_name USING access_method
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_OPCLASS;
					n->objname = $7;
					n->objargs = list_make1(makeString($9));
					$$ = (Node *)n;
				}
			| ALTER EXTENSION name add_drop OPERATOR FAMILY any_name USING access_method
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_OPFAMILY;
					n->objname = $7;
					n->objargs = list_make1(makeString($9));
					$$ = (Node *)n;
				}
			| ALTER EXTENSION name add_drop SCHEMA name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_SCHEMA;
					n->objname = list_make1(makeString($6));
					$$ = (Node *)n;
				}
			| ALTER EXTENSION name add_drop TABLE any_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_TABLE;
					n->objname = $6;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION name add_drop TEXT_P SEARCH PARSER any_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_TSPARSER;
					n->objname = $8;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION name add_drop TEXT_P SEARCH DICTIONARY any_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_TSDICTIONARY;
					n->objname = $8;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION name add_drop TEXT_P SEARCH TEMPLATE any_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_TSTEMPLATE;
					n->objname = $8;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION name add_drop TEXT_P SEARCH CONFIGURATION any_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_TSCONFIGURATION;
					n->objname = $8;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION name add_drop SEQUENCE any_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_SEQUENCE;
					n->objname = $6;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION name add_drop VIEW any_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_VIEW;
					n->objname = $6;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION name add_drop MATERIALIZED VIEW any_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_MATVIEW;
					n->objname = $7;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION name add_drop FOREIGN TABLE any_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_FOREIGN_TABLE;
					n->objname = $7;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION name add_drop FOREIGN DATA_P WRAPPER name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_FDW;
					n->objname = list_make1(makeString($8));
					$$ = (Node *)n;
				}
			| ALTER EXTENSION name add_drop SERVER name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_FOREIGN_SERVER;
					n->objname = list_make1(makeString($6));
					$$ = (Node *)n;
				}
			| ALTER EXTENSION name add_drop TYPE_P any_name
				{
					AlterExtensionContentsStmt *n = makeNode(AlterExtensionContentsStmt);
					n->extname = $3;
					n->action = $4;
					n->objtype = OBJECT_TYPE;
					n->objname = $6;
					$$ = (Node *)n;
				}
		;
/*****************************************************************************
 *
 *             QUERY:
 *             CREATE WEAK PASSWORD DICTIONARY
 *
 *****************************************************************************/

CreateWeakPasswordDictionaryStmt:
        CREATE WEAK PASSWORD DICTIONARY opt_vals weak_password_string_list
               {
                       CreateWeakPasswordDictionaryStmt *n = makeNode(CreateWeakPasswordDictionaryStmt);
                       n->weak_password_string_list = $6;
                       $$ = (Node*)n;
               }
			   ;

opt_vals:               WITH VALUES                                            {}
                       | /*EMPTY*/                                             {}
			   ;

weak_password_string_list:  '(' password_string ')'                                 { $$ = list_make1(makeString($2)); }
                       | weak_password_string_list ',' '(' password_string ')'      { $$ = lappend($1, makeString($4)); } 
               ;

/*****************************************************************************
 *
 *		QUERY:
 *				DROP WEAK PASSWORD DICTIONARY
 *
 *****************************************************************************/

DropWeakPasswordDictionaryStmt:
			DROP WEAK PASSWORD DICTIONARY 
				{
					DropWeakPasswordDictionaryStmt *n = makeNode(DropWeakPasswordDictionaryStmt);
					$$ = (Node *)n;
				}
		;
					
/*****************************************************************************
 *
 *		QUERY:
 *             CREATE FOREIGN DATA WRAPPER name options
 *
 *****************************************************************************/

CreateFdwStmt: CREATE FOREIGN DATA_P WRAPPER name opt_fdw_options create_generic_options
				{
					CreateFdwStmt *n = makeNode(CreateFdwStmt);
					n->fdwname = $5;
					n->func_options = $6;
					n->options = $7;
					$$ = (Node *) n;
				}
		;

fdw_option:
			HANDLER handler_name				{ $$ = makeDefElem("handler", (Node *)$2); }
			| NO HANDLER						{ $$ = makeDefElem("handler", NULL); }
			| VALIDATOR handler_name			{ $$ = makeDefElem("validator", (Node *)$2); }
			| NO VALIDATOR						{ $$ = makeDefElem("validator", NULL); }
		;

fdw_options:
			fdw_option							{ $$ = list_make1($1); }
			| fdw_options fdw_option			{ $$ = lappend($1, $2); }
		;

opt_fdw_options:
			fdw_options							{ $$ = $1; }
			| /*EMPTY*/							{ $$ = NIL; }
		;

/*****************************************************************************
 *
 *		QUERY :
 *				DROP FOREIGN DATA WRAPPER name
 *
 ****************************************************************************/

DropFdwStmt: DROP FOREIGN DATA_P WRAPPER name opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_FDW;
					n->objects = list_make1(list_make1(makeString($5)));
					n->arguments = NIL;
					n->missing_ok = false;
					n->behavior = $6;
					n->concurrent = false;
					$$ = (Node *) n;
				}
				|  DROP FOREIGN DATA_P WRAPPER IF_P EXISTS name opt_drop_behavior
                {
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_FDW;
					n->objects = list_make1(list_make1(makeString($7)));
					n->arguments = NIL;
					n->missing_ok = true;
					n->behavior = $8;
					n->concurrent = false;
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 *		QUERY :
 *				ALTER FOREIGN DATA WRAPPER name options
 *
 ****************************************************************************/

AlterFdwStmt: ALTER FOREIGN DATA_P WRAPPER name opt_fdw_options alter_generic_options
				{
					AlterFdwStmt *n = makeNode(AlterFdwStmt);
					n->fdwname = $5;
					n->func_options = $6;
					n->options = $7;
					$$ = (Node *) n;
				}
			| ALTER FOREIGN DATA_P WRAPPER name fdw_options
				{
					AlterFdwStmt *n = makeNode(AlterFdwStmt);
					n->fdwname = $5;
					n->func_options = $6;
					n->options = NIL;
					$$ = (Node *) n;
				}
		;

/* Options definition for CREATE FDW, SERVER and USER MAPPING */
create_generic_options:
			OPTIONS '(' generic_option_list ')'			{ $$ = $3; }
			| /*EMPTY*/									{ $$ = NIL; }
		;

generic_option_list:
			generic_option_elem
				{
					$$ = list_make1($1);
				}
			| generic_option_list ',' generic_option_elem
				{
					$$ = lappend($1, $3);
				}
		;

/* Options definition for ALTER FDW, SERVER and USER MAPPING */
alter_generic_options:
			OPTIONS	'(' alter_generic_option_list ')'		{ $$ = $3; }
		;

alter_generic_option_list:
			alter_generic_option_elem
				{
					$$ = list_make1($1);
				}
			| alter_generic_option_list ',' alter_generic_option_elem
				{
					$$ = lappend($1, $3);
				}
		;

alter_generic_option_elem:
			generic_option_elem
				{
					$$ = $1;
				}
			| SET generic_option_elem
				{
					$$ = $2;
					$$->defaction = DEFELEM_SET;
				}
			| ADD_P generic_option_elem
				{
					$$ = $2;
					$$->defaction = DEFELEM_ADD;
				}
			| DROP generic_option_name
				{
					$$ = makeDefElemExtended(NULL, $2, NULL, DEFELEM_DROP);
				}
		;

generic_option_elem:
			generic_option_name generic_option_arg
				{
                    if (strcmp($1, "error_table") == 0)
                        ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("Invalid option %s", $1)));

					$$ = makeDefElem($1, $2);
				}
		;

generic_option_name:
				ColLabel			{ $$ = $1; }
		;

/* We could use def_arg here, but the spec only requires string literals */
generic_option_arg:
				Sconst				{ $$ = (Node *) makeString($1); }
		;

/*****************************************************************************
 *
 *		QUERY:
 *             CREATE SERVER name [TYPE] [VERSION] [OPTIONS]
 *
 *****************************************************************************/
fdwName:    IDENT           {$$ = $1;};

CreateForeignServerStmt: CREATE SERVER name opt_type opt_foreign_server_version
						 FOREIGN DATA_P WRAPPER fdwName create_generic_options
				{
					CreateForeignServerStmt *n = makeNode(CreateForeignServerStmt);
					n->servername = $3;
					n->servertype = $4;
					n->version = $5;
					n->fdwname = $9;
					n->options = $10;
					$$ = (Node *) n;
				}
		;

opt_type:
			TYPE_P Sconst			{ $$ = $2; }
			| /*EMPTY*/				{ $$ = NULL; }
		;


foreign_server_version:
			VERSION_P Sconst		{ $$ = $2; }
		|	VERSION_P NULL_P		{ $$ = NULL; }
		;

opt_foreign_server_version:
			foreign_server_version	{ $$ = $1; }
			| /*EMPTY*/				{ $$ = NULL; }
		;

/*****************************************************************************
 *
 *		QUERY :
 *				DROP SERVER name
 *
 ****************************************************************************/

DropForeignServerStmt: DROP SERVER name opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_FOREIGN_SERVER;
					n->objects = list_make1(list_make1(makeString($3)));
					n->arguments = NIL;
					n->missing_ok = false;
					n->behavior = $4;
					n->concurrent = false;
					$$ = (Node *) n;
				}
				|  DROP SERVER IF_P EXISTS name opt_drop_behavior
                {
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_FOREIGN_SERVER;
					n->objects = list_make1(list_make1(makeString($5)));
					n->arguments = NIL;
					n->missing_ok = true;
					n->behavior = $6;
					n->concurrent = false;
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 *		QUERY :
 *				ALTER SERVER name [VERSION] [OPTIONS]
 *
 ****************************************************************************/

AlterForeignServerStmt: ALTER SERVER name foreign_server_version alter_generic_options
				{
					AlterForeignServerStmt *n = makeNode(AlterForeignServerStmt);
					n->servername = $3;
					n->version = $4;
					n->options = $5;
					n->has_version = true;
					$$ = (Node *) n;
				}
			| ALTER SERVER name foreign_server_version
				{
					AlterForeignServerStmt *n = makeNode(AlterForeignServerStmt);
					n->servername = $3;
					n->version = $4;
					n->has_version = true;
					$$ = (Node *) n;
				}
			| ALTER SERVER name alter_generic_options
				{
					AlterForeignServerStmt *n = makeNode(AlterForeignServerStmt);
					n->servername = $3;
					n->options = $4;
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 *		QUERY:
 *             CREATE FOREIGN TABLE relname (...) SERVER name (...)
 *
 *****************************************************************************/

CreateForeignTableStmt:
		CREATE FOREIGN TABLE qualified_name
			OptForeignTableElementList
			SERVER name create_generic_options ForeignTblWritable
			OptForeignTableLogError OptForeignTableLogRemote OptPerNodeRejectLimit OptDistributeBy
/* PGXC_BEGIN */
			OptSubCluster
/* PGXC_END */
			OptForeignPartBy
				{
					CreateForeignTableStmt *n = makeNode(CreateForeignTableStmt);
					$4->relpersistence = RELPERSISTENCE_PERMANENT;
					n->base.relation = $4;
					n->base.tableElts = $5;
					n->base.inhRelations = NIL;
					n->base.if_not_exists = false;
					/* FDW-specific data */
					n->servername = $7;
					n->options = $8;

					n->write_only = $9;
					n->error_relation = (Node*)$10;
					if ($11 != NULL)
						n->extOptions = lappend(n->extOptions, $11);
					if ($12 != NULL)
						n->extOptions = lappend(n->extOptions, $12);
					n->base.distributeby = $13;
/* PGXC_BEGIN */
					n->base.subcluster = $14;
/* PGXC_END */
					if ($15 != NULL)
						n->part_state = $15;

					$$ = (Node *) n;
				}
		| CREATE FOREIGN TABLE IF_P NOT EXISTS qualified_name
			OptForeignTableElementList
			SERVER name create_generic_options ForeignTblWritable
			OptForeignTableLogError OptForeignTableLogRemote OptPerNodeRejectLimit OptDistributeBy
/* PGXC_BEGIN */
			OptSubCluster
/* PGXC_END */
			OptForeignPartBy
				{
					CreateForeignTableStmt *n = makeNode(CreateForeignTableStmt);
					$7->relpersistence = RELPERSISTENCE_PERMANENT;
					n->base.relation = $7;
					n->base.tableElts = $8;
					n->base.inhRelations = NIL;
					n->base.if_not_exists = true;
					/* FDW-specific data */
					n->servername = $10;
					n->options = $11;

					n->write_only = $12;
					n->error_relation = (Node*)$13;
					if ($14 != NULL)
						n->extOptions = lappend(n->extOptions, $14);
					if ($15 != NULL)
						n->extOptions = lappend(n->extOptions, $15);
					n->base.distributeby = $16;
/* PGXC_BEGIN */
					n->base.subcluster = $17;
/* PGXC_END */
					if ($18 != NULL)
						n->part_state = $18;

					$$ = (Node *) n;
				}
/* ENABLE_MOT BEGIN */
                | CREATE FOREIGN TABLE qualified_name
			OptForeignTableElementList
			create_generic_options ForeignTblWritable
			OptForeignTableLogError OptForeignTableLogRemote OptPerNodeRejectLimit OptDistributeBy
/* PGXC_BEGIN */
			OptSubCluster
/* PGXC_END */
			OptForeignPartBy
				{
					CreateForeignTableStmt *n = makeNode(CreateForeignTableStmt);
					$4->relpersistence = RELPERSISTENCE_PERMANENT;
					n->base.relation = $4;
					n->base.tableElts = $5;
					n->base.inhRelations = NIL;
					n->base.if_not_exists = false;
					/* FDW-specific data */
#ifdef ENABLE_MOT
					n->servername = pstrdup("mot_server");
#else
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("Foreign server is not specified")));
#endif
					n->options = $6;

					n->write_only = $7;
					n->error_relation = (Node*)$8;
					if ($9 != NULL)
						n->extOptions = lappend(n->extOptions, $9);
					if ($10 != NULL)
						n->extOptions = lappend(n->extOptions, $10);
					n->base.distributeby = $11;
/* PGXC_BEGIN */
					n->base.subcluster = $12;
/* PGXC_END */
					if ($13 != NULL)
						n->part_state = $13;

					$$ = (Node *) n;
				}
               | CREATE FOREIGN TABLE IF_P NOT EXISTS qualified_name
			OptForeignTableElementList
			create_generic_options ForeignTblWritable
			OptForeignTableLogError OptForeignTableLogRemote OptPerNodeRejectLimit OptDistributeBy
/* PGXC_BEGIN */
			OptSubCluster
/* PGXC_END */
			OptForeignPartBy
				{
					CreateForeignTableStmt *n = makeNode(CreateForeignTableStmt);
					$7->relpersistence = RELPERSISTENCE_PERMANENT;
					n->base.relation = $7;
					n->base.tableElts = $8;
					n->base.inhRelations = NIL;
					n->base.if_not_exists = true;
					/* FDW-specific data */
#ifdef ENABLE_MOT
					n->servername = pstrdup("mot_server");
#else
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("Foreign server is not specified")));
#endif
					n->options = $9;

					n->write_only = $10;
					n->error_relation = (Node*)$11;
					if ($12 != NULL)
						n->extOptions = lappend(n->extOptions, $12);
					if ($13 != NULL)
						n->extOptions = lappend(n->extOptions, $13);
					n->base.distributeby = $14;
/* PGXC_BEGIN */
					n->base.subcluster = $15;
/* PGXC_END */
					if ($16 != NULL)
						n->part_state = $16;

					$$ = (Node *) n;
				}
/* ENABLE_MOT END */
		;

ForeignTblWritable : WRITE ONLY  { $$ = true; }
					| READ ONLY   { $$ = false; }
					| /* EMPTY */ { $$ = false; }
				;

OptForeignTableElementList:
					'(' ForeignTableElementList ')'			{ $$ = $2; }
					| '(' ')'								{ $$ = NIL; }
				;

ForeignTableElementList:
				ForeignTableElement
					{
						$$ = list_make1($1);
					}
				| ForeignTableElementList ',' ForeignTableElement
					{
						$$ = lappend($1, $3);
					}
			;

ForeignTableElement:
			ForeignColDef				    { $$ = $1; }
			|ForeignTableLikeClause			{ $$ = $1; }
			|TableConstraint			    { $$ = $1; } /* @hdfs Add informational constraint syntax on the HDFS foreign table. */
		;
ForeignColDef: ColId Typename ForeignPosition create_generic_options  ColQualList
				{
					ColumnDef *n = makeNode(ColumnDef);
					n->colname = $1;
					n->typname = $2;
					n->inhcount = 0;
					n->is_local = true;
					n->is_not_null = false;
					n->is_from_type = false;
					n->storage = 0;
					n->raw_default = NULL;
					n->cooked_default = NULL;
					n->collOid = InvalidOid;
					n->fdwoptions = $4;
					n->clientLogicColumnRef=NULL;
					SplitColQualList($5, &n->constraints, &n->collClause,
										yyscanner);
					if ($3)
					{
						((Position*)$3)->colname = pstrdup($1);
						n->position = (Position*)$3;
					}
					$$ = (Node*)n;
				}
		;

ForeignPosition:
			POSITION '(' Iconst ',' Iconst ')'
				{
					Position *n = makeNode(Position);
					n->position = $3;
					n->fixedlen = $5;
					$$ = (Node*)n;
				}
			| /*EMPTY*/ 								{ $$ = NULL; }
		;

ForeignTableLikeClause:
			LIKE qualified_name				
				{					
					TableLikeClause *n = makeNode(TableLikeClause);
					n->relation = $2;
					$$ = (Node *)n;
				}
		;	

OptForeignTableLogError:
			LOG_P INTO qualified_name 	{ $$ = (Node*)$3; }
			| WITH  qualified_name 		{ $$ = (Node*)$2; }
			| /*EMPTY*/ 				{ $$ = NULL;}
		;

OptForeignTableLogRemote:
			REMOTE_P LOG_P Sconst
				{
					$$ = (Node*)makeDefElem("log_remote", (Node *)makeString($3));
				}
			|REMOTE_P LOG_P
				{
					$$ = (Node*)makeDefElem("log_remote", (Node*)makeString(""));
				}
			| /* EMPTY */				{ $$ = NULL; }
		;

OptPerNodeRejectLimit:
			PER_P NODE REJECT_P LIMIT Sconst
				{
					$$ = (Node*)makeDefElem("reject_limit", (Node*)makeString($5));
				}
			| /*EMPTY*/ 					{ $$ = NULL; }
		;

OptForeignPartBy:
			OptForeignPartAuto          { $$ = $1; }
			| /*EMPTY*/                 { $$ = NULL; }
		;

OptForeignPartAuto: PARTITION BY '(' partition_item_list ')' AUTOMAPPED
			{
				ForeignPartState *n = makeNode(ForeignPartState);
				n->partitionKey = $4;
				$$ = n;
			}
		| PARTITION BY '(' partition_item_list ')'
			{
				ForeignPartState *n = makeNode(ForeignPartState);
				n->partitionKey = $4;
				$$ = n;
			}
		;

partition_item_list:
		partition_item
			{
				$$ = list_make1($1);
			}
		| partition_item_list ',' partition_item
			{
				$$ = lappend($1, $3);
			}
		;

partition_item:
		ColId
			{
				$$ = makeColumnRef($1, NIL, @1, yyscanner);
			}
		;

/*****************************************************************************
 *
 *		QUERY:
 *             ALTER FOREIGN TABLE relname [...]
 *
 *****************************************************************************/

AlterForeignTableStmt:
			ALTER FOREIGN TABLE relation_expr alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $4;
					n->cmds = $5;
					n->relkind = OBJECT_FOREIGN_TABLE;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER FOREIGN TABLE IF_P EXISTS relation_expr alter_table_cmds
				{
					AlterTableStmt *n = makeNode(AlterTableStmt);
					n->relation = $6;
					n->cmds = $7;
					n->relkind = OBJECT_FOREIGN_TABLE;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
            | ALTER FOREIGN TABLE relation_expr MODIFY_P '(' modify_column_cmds ')'
                {
                    AlterTableStmt *n = makeNode(AlterTableStmt);
                    n->relation = $4;
                    n->cmds = $7;
                    n->relkind = OBJECT_FOREIGN_TABLE;
                    n->missing_ok = false;
                    $$ = (Node *)n;
                }
            | ALTER FOREIGN TABLE IF_P EXISTS relation_expr MODIFY_P '(' modify_column_cmds ')'
                {
                    AlterTableStmt *n = makeNode(AlterTableStmt);
                    n->relation = $6;
                    n->cmds = $9;
                    n->relkind = OBJECT_FOREIGN_TABLE;
                    n->missing_ok = true;
                    $$ = (Node *)n;
                }
		;

/*****************************************************************************
 *
 *		QUERY:
 *             CREATE USER MAPPING FOR auth_ident SERVER name [OPTIONS]
 *
 *****************************************************************************/

CreateUserMappingStmt: CREATE USER MAPPING FOR auth_ident SERVER name create_generic_options
				{
					CreateUserMappingStmt *n = makeNode(CreateUserMappingStmt);
					n->username = $5;
					n->servername = $7;
					n->options = $8;
					$$ = (Node *) n;
				}
		;

/* User mapping authorization identifier */
auth_ident:
			CURRENT_USER	{ $$ = "current_user"; }
		|	USER			{ $$ = "current_user"; }
		|	RoleId			{ $$ = (strcmp($1, "public") == 0) ? NULL : $1; }
		;

/*****************************************************************************
 *
 *		QUERY :
 *				DROP USER MAPPING FOR auth_ident SERVER name
 *
 ****************************************************************************/

DropUserMappingStmt: DROP USER MAPPING FOR auth_ident SERVER name
				{
					DropUserMappingStmt *n = makeNode(DropUserMappingStmt);
					n->username = $5;
					n->servername = $7;
					n->missing_ok = false;
					$$ = (Node *) n;
				}
				|  DROP USER MAPPING IF_P EXISTS FOR auth_ident SERVER name
				{
					DropUserMappingStmt *n = makeNode(DropUserMappingStmt);
					n->username = $7;
					n->servername = $9;
					n->missing_ok = true;
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 *		QUERY :
 *				ALTER USER MAPPING FOR auth_ident SERVER name OPTIONS
 *
 ****************************************************************************/

AlterUserMappingStmt: ALTER USER MAPPING FOR auth_ident SERVER name alter_generic_options
				{
					AlterUserMappingStmt *n = makeNode(AlterUserMappingStmt);
					n->username = $5;
					n->servername = $7;
					n->options = $8;
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 *      QUERIES For ROW LEVEL SECURITY:
 *              CREATE [ROW LEVEL SECURITY] POLICY name ON table
 *                  [AS { PERMISSIVE | RESTRICTIVE }]
 *                  [FOR { ALL | SELECT | INSERT | UPDATE | DELETE }]
 *                  [TO role, ...]
 *                  [USING (qual)]
 *
 *              ALTER [ROW LEVEL SECURITY] POLICY name ON table
 *                  [TO role, ...]
 *                  [USING (qual)]
 *
 *              DROP [ROW LEVEL SECURITY] POLIICY [IF EXISTS] name ON table [CASCADE | RESTRICT]
 *
 *****************************************************************************/

CreateRlsPolicyStmt:
			CREATE RowLevelSecurityPolicyName ON qualified_name RLSDefaultPermissive
				RLSDefaultForCmd RLSDefaultToRole
				RLSOptionalUsingExpr
				{
					/* Did not support INSERT, MERGE yet */
					if (strcmp($6, "insert") == 0 ||
						strcmp($6, "merge") == 0)
					{
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("Row Level Security is not yet supported for INSERT and MERGE")));
					}
					CreateRlsPolicyStmt *n = makeNode(CreateRlsPolicyStmt);
					n->fromExternal = true;
					n->policyName = $2;
					n->relation = $4;
					n->isPermissive = $5;
					n->cmdName = $6;
					n->roleList = $7;
					n->usingQual = $8;
					$$ = (Node *) n;
				}
		;

AlterRlsPolicyStmt:
			ALTER RowLevelSecurityPolicyName ON qualified_name RLSOptionalToRole
				RLSOptionalUsingExpr
				{
					AlterRlsPolicyStmt *n = makeNode(AlterRlsPolicyStmt);
					n->policyName = $2;
					n->relation = $4;
					n->roleList = $5;
					n->usingQual = $6;
					$$ = (Node *) n;
				}
		;

DropRlsPolicyStmt:
			DROP RowLevelSecurityPolicyName ON any_name opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_RLSPOLICY;
					n->objects = list_make1(lappend($4, makeString($2)));
					n->arguments = NIL;
					n->behavior = $5;
					n->missing_ok = false;
					n->concurrent = false;
					$$ = (Node *) n;
				}
			| DROP POLICY IF_P EXISTS name ON any_name opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_RLSPOLICY;
					n->objects = list_make1(lappend($7, makeString($5)));
					n->arguments = NIL;
					n->behavior = $8;
					n->missing_ok = true;
					n->concurrent = false;
					$$ = (Node *) n;
				}
			| DROP ROW LEVEL SECURITY POLICY IF_P EXISTS name ON any_name opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_RLSPOLICY;
					n->objects = list_make1(lappend($10, makeString($8)));
					n->arguments = NIL;
					n->behavior = $11;
					n->missing_ok = true;
					n->concurrent = false;
					$$ = (Node *) n;
				}
		;

RowLevelSecurityPolicyName:
			ROW LEVEL SECURITY POLICY name	{ $$ = $5; }
			| POLICY name						{ $$ = $2; }

RLSOptionalUsingExpr:
			USING '(' a_expr ')'	{ $$ = $3; }
			| /* EMPTY */			{ $$ = NULL; }
		;

RLSDefaultToRole:
			TO row_level_security_role_list			{ $$ = $2; }
			| /* EMPTY */			{ $$ = list_make1(makeString("public")); }
		;

RLSOptionalToRole:
			TO row_level_security_role_list			{ $$ = $2; }
			| /* EMPTY */			{ $$ = NULL; }
		;

row_level_security_role_list: row_level_security_role
							{ $$ = list_make1(makeString($1)); }
					|	row_level_security_role_list ',' row_level_security_role
							{ $$ = lappend($1, makeString($3)); }
					;

row_level_security_role:
			RoleId			{ $$ = $1; }
		|	CURRENT_USER	{ $$ = pstrdup($1); }
		|	SESSION_USER	{ $$ = pstrdup($1); }
		;

RLSDefaultPermissive:
			AS IDENT
				{
					if (strcmp($2, "permissive") == 0)
						$$ = true;
					else if (strcmp($2, "restrictive") == 0)
						$$ = false;
					else
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("unrecognized row security option \"%s\"", $2),
								 errhint("Only PERMISSIVE or RESTRICTIVE policies are supported currently."),
									 parser_errposition(@2)));
				}
			| /* EMPTY */			{ $$ = true; }
		;

RLSDefaultForCmd:
			FOR row_level_security_cmd	{ $$ = $2; }
			| /* EMPTY */			{ $$ = "all"; }
		;

row_level_security_cmd:
			ALL				{ $$ = "all"; }
		|	SELECT			{ $$ = "select"; }
		|	UPDATE			{ $$ = "update"; }
		|	DELETE_P		{ $$ = "delete"; }
		|	INSERT			{ $$ = "insert"; }
		|	MERGE			{ $$ = "merge"; }
		;

/*****************************************************************************
 *
 *		QUERIES:
 *				CREATE OR REPLACE SYNONYM STATEMENTS
 *
 *****************************************************************************/

CreateSynonymStmt:
			CREATE opt_or_replace SYNONYM any_name FOR any_name
				{
					CreateSynonymStmt *n = makeNode(CreateSynonymStmt);
					n->replace = $2;
					n->synName = $4;
					n->objName = $6;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 *		QUERIES:
 *				DROP SYNONYM STATEMENTS
 *
 *****************************************************************************/

DropSynonymStmt:
			DROP SYNONYM any_name  opt_drop_behavior
				{
					DropSynonymStmt *n = makeNode(DropSynonymStmt);
					n->synName = $3;
					n->behavior = $4;
					n->missing = false;
					$$ = (Node *) n;
				}
			| DROP SYNONYM IF_P EXISTS any_name  opt_drop_behavior
				{
					DropSynonymStmt *n = makeNode(DropSynonymStmt);
					n->synName = $5;
					n->behavior = $6;
					n->missing = true;
					$$ = (Node *) n;
				}
		;


/*****************************************************************************
 *
 *		QUERY:
 *             CREATE DATA SOURCE name [TYPE] [VERSION] [OPTIONS]
 *
 *****************************************************************************/

CreateDataSourceStmt: CREATE DATA_P SOURCE_P name opt_data_source_type 
				opt_data_source_version create_generic_options
				{
					CreateDataSourceStmt *n = makeNode(CreateDataSourceStmt);
					n->srcname = $4;
					n->srctype = $5;
					n->version = $6;
					n->options = $7;
					$$ = (Node *) n;
				}
		;

data_source_type:
			TYPE_P Sconst			{ $$ = $2; }
		;

opt_data_source_type:
			data_source_type			{ $$ = $1; }
			| /*EMPTY*/				{ $$ = NULL; }
		;

data_source_version:
			VERSION_P Sconst		{ $$ = $2; }
			| VERSION_P NULL_P		{ $$ = NULL; }
		;

opt_data_source_version:
			data_source_version	{ $$ = $1; }
			| /*EMPTY*/				{ $$ = NULL; }
		;

/*****************************************************************************
 *
 *		QUERY :
 *				ALTER DATA SOURCE name [TYPE] [VERSION] [OPTIONS]
 *
 ****************************************************************************/

AlterDataSourceStmt: ALTER DATA_P SOURCE_P name data_source_type data_source_version alter_generic_options
				{
					AlterDataSourceStmt *n = makeNode(AlterDataSourceStmt);
					n->srcname = $4;
					n->srctype = $5;
					n->version = $6;
					n->options = $7;
					n->has_version = true;
					$$ = (Node *) n;
				}
			| ALTER DATA_P SOURCE_P name data_source_type data_source_version
				{
					AlterDataSourceStmt *n = makeNode(AlterDataSourceStmt);
					n->srcname = $4;
					n->srctype = $5;
					n->version = $6;
					n->has_version = true;
					$$ = (Node *) n;
				}
			| ALTER DATA_P SOURCE_P name data_source_type alter_generic_options
				{
					AlterDataSourceStmt *n = makeNode(AlterDataSourceStmt);
					n->srcname = $4;
					n->srctype = $5;
					n->options = $6;
					n->has_version = false;
					$$ = (Node *) n;
				}
			| ALTER DATA_P SOURCE_P name data_source_version alter_generic_options
				{
					AlterDataSourceStmt *n = makeNode(AlterDataSourceStmt);
					n->srcname = $4;
					n->version = $5;
					n->options = $6;
					n->has_version = true;
					$$ = (Node *) n;
				}
			| ALTER DATA_P SOURCE_P name data_source_type
				{
					AlterDataSourceStmt *n = makeNode(AlterDataSourceStmt);
					n->srcname = $4;
					n->srctype = $5;
					n->has_version = false;
					$$ = (Node *) n;
				}
			| ALTER DATA_P SOURCE_P name data_source_version
				{
					AlterDataSourceStmt *n = makeNode(AlterDataSourceStmt);
					n->srcname = $4;
					n->version = $5;
					n->has_version = true;
					$$ = (Node *) n;
				}
			| ALTER DATA_P SOURCE_P name alter_generic_options
				{
					AlterDataSourceStmt *n = makeNode(AlterDataSourceStmt);
					n->srcname = $4;
					n->options = $5;
					n->has_version = false;
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 *		QUERY :
 *				DROP DATA SOURCE name
 *
 ****************************************************************************/

DropDataSourceStmt: DROP DATA_P SOURCE_P name opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_DATA_SOURCE;
					n->objects = list_make1(list_make1(makeString($4)));
					n->arguments = NIL;
					n->missing_ok = false;
					n->behavior = $5;
					n->concurrent = false;
					$$ = (Node *) n;
				}
				|  DROP DATA_P SOURCE_P IF_P EXISTS name opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_DATA_SOURCE;
					n->objects = list_make1(list_make1(makeString($6)));
					n->arguments = NIL;
					n->missing_ok = true;
					n->behavior = $7;
					n->concurrent = false;
					$$ = (Node *) n;
				}
		;

/*****************************************************************************
 *
 *		QUERIES :
 *				CREATE TRIGGER ...
 *				DROP TRIGGER ...
 *
 *****************************************************************************/

CreateTrigStmt:
			CREATE TRIGGER name TriggerActionTime TriggerEvents ON
			qualified_name TriggerForSpec TriggerWhen
			EXECUTE PROCEDURE func_name '(' TriggerFuncArgs ')'
				{
					CreateTrigStmt *n = makeNode(CreateTrigStmt);
					n->trigname = $3;
					n->relation = $7;
					n->funcname = $12;
					n->args = $14;
					n->row = $8;
					n->timing = $4;
					n->events = intVal(linitial($5));
					n->columns = (List *) lsecond($5);
					n->whenClause = $9;
					n->isconstraint  = FALSE;
					n->deferrable	 = FALSE;
					n->initdeferred  = FALSE;
					n->constrrel = NULL;
					$$ = (Node *)n;
				}
			| CREATE CONSTRAINT TRIGGER name AFTER TriggerEvents ON
			qualified_name OptConstrFromTable ConstraintAttributeSpec
			FOR EACH ROW TriggerWhen
			EXECUTE PROCEDURE func_name '(' TriggerFuncArgs ')'
				{
					CreateTrigStmt *n = makeNode(CreateTrigStmt);
					n->trigname = $4;
					n->relation = $8;
					n->funcname = $17;
					n->args = $19;
					n->row = TRUE;
					n->timing = TRIGGER_TYPE_AFTER;
					n->events = intVal(linitial($6));
					n->columns = (List *) lsecond($6);
					n->whenClause = $14;
					n->isconstraint  = TRUE;
					processCASbits($10, @10, "TRIGGER",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);
					n->constrrel = $9;
					$$ = (Node *)n;
				}
		;

TriggerActionTime:
			BEFORE								{ $$ = TRIGGER_TYPE_BEFORE; }
			| AFTER								{ $$ = TRIGGER_TYPE_AFTER; }
			| INSTEAD OF						{ $$ = TRIGGER_TYPE_INSTEAD; }
		;

TriggerEvents:
			TriggerOneEvent
				{ $$ = $1; }
			| TriggerEvents OR TriggerOneEvent
				{
					int		events1 = intVal(linitial($1));
					int		events2 = intVal(linitial($3));
					List   *columns1 = (List *) lsecond($1);
					List   *columns2 = (List *) lsecond($3);

					if (events1 & events2)
						parser_yyerror("duplicate trigger events specified");
					/*
					 * concat'ing the columns lists loses information about
					 * which columns went with which event, but so long as
					 * only UPDATE carries columns and we disallow multiple
					 * UPDATE items, it doesn't matter.  Command execution
					 * should just ignore the columns for non-UPDATE events.
					 */
					$$ = list_make2(makeInteger(events1 | events2),
									list_concat(columns1, columns2));
				}
		;

TriggerOneEvent:
			INSERT
				{ $$ = list_make2(makeInteger(TRIGGER_TYPE_INSERT), NIL); }
			| DELETE_P
				{ $$ = list_make2(makeInteger(TRIGGER_TYPE_DELETE), NIL); }
			| UPDATE
				{ $$ = list_make2(makeInteger(TRIGGER_TYPE_UPDATE), NIL); }
			| UPDATE OF columnList
				{ $$ = list_make2(makeInteger(TRIGGER_TYPE_UPDATE), $3); }
			| TRUNCATE
				{ $$ = list_make2(makeInteger(TRIGGER_TYPE_TRUNCATE), NIL); }
		;

TriggerForSpec:
			FOR TriggerForOptEach TriggerForType
				{
					$$ = $3;
				}
			| /* EMPTY */
				{
					/*
					 * If ROW/STATEMENT not specified, default to
					 * STATEMENT, per SQL
					 */
					$$ = FALSE;
				}
		;

TriggerForOptEach:
			EACH									{}
			| /*EMPTY*/								{}
		;

TriggerForType:
			ROW										{ $$ = TRUE; }
			| STATEMENT								{ $$ = FALSE; }
		;

TriggerWhen:
			WHEN '(' a_expr ')'						{ $$ = $3; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

TriggerFuncArgs:
			TriggerFuncArg							{ $$ = list_make1($1); }
			| TriggerFuncArgs ',' TriggerFuncArg	{ $$ = lappend($1, $3); }
			| /*EMPTY*/								{ $$ = NIL; }
		;

TriggerFuncArg:
			Iconst
				{
					char buf[64];
					snprintf(buf, sizeof(buf), "%d", $1);
					$$ = makeString(pstrdup(buf));
				}
			| FCONST								{ $$ = makeString($1); }
			| Sconst								{ $$ = makeString($1); }
			| ColLabel								{ $$ = makeString($1); }
		;

OptConstrFromTable:
			FROM qualified_name						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

ConstraintAttributeSpec:
			/*EMPTY*/
				{ $$ = 0; }
			| ConstraintAttributeSpec ConstraintAttributeElem
				{
					/*
					 * We must complain about conflicting options.
					 * We could, but choose not to, complain about redundant
					 * options (ie, where $2's bit is already set in $1).
					 */
					int		newspec = $1 | $2;

					/* special message for this case */
					if ((newspec & (CAS_NOT_DEFERRABLE | CAS_INITIALLY_DEFERRED)) == (CAS_NOT_DEFERRABLE | CAS_INITIALLY_DEFERRED))
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("constraint declared INITIALLY DEFERRED must be DEFERRABLE"),
								 parser_errposition(@2)));
					/* generic message for other conflicts */
					if ((newspec & (CAS_NOT_DEFERRABLE | CAS_DEFERRABLE)) == (CAS_NOT_DEFERRABLE | CAS_DEFERRABLE) ||
						(newspec & (CAS_INITIALLY_IMMEDIATE | CAS_INITIALLY_DEFERRED)) == (CAS_INITIALLY_IMMEDIATE | CAS_INITIALLY_DEFERRED))
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("conflicting constraint properties"),
								 parser_errposition(@2)));
					$$ = newspec;
				}
		;

ConstraintAttributeElem:
			NOT DEFERRABLE					{ $$ = CAS_NOT_DEFERRABLE; }
			| DEFERRABLE					{ $$ = CAS_DEFERRABLE; }
			| INITIALLY IMMEDIATE			{ $$ = CAS_INITIALLY_IMMEDIATE; }
			| INITIALLY DEFERRED			{ $$ = CAS_INITIALLY_DEFERRED; }
			| NOT VALID						{ $$ = CAS_NOT_VALID; }
			| NO INHERIT					{ $$ = CAS_NO_INHERIT; }
		;


DropTrigStmt:
			DROP TRIGGER name ON any_name opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_TRIGGER;
					n->objects = list_make1(lappend($5, makeString($3)));
					n->arguments = NIL;
					n->behavior = $6;
					n->missing_ok = false;
					n->concurrent = false;
					$$ = (Node *) n;
				}
			| DROP TRIGGER IF_P EXISTS name ON any_name opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_TRIGGER;
					n->objects = list_make1(lappend($7, makeString($5)));
					n->arguments = NIL;
					n->behavior = $8;
					n->missing_ok = true;
					n->concurrent = false;
					$$ = (Node *) n;
				}
		;


/*****************************************************************************
 *
 *		QUERIES :
 *				CREATE ASSERTION ...
 *				DROP ASSERTION ...
 *
 *****************************************************************************/

CreateAssertStmt:
			CREATE ASSERTION name CHECK '(' a_expr ')'
			ConstraintAttributeSpec
				{
					CreateTrigStmt *n = makeNode(CreateTrigStmt);
					n->trigname = $3;
					n->args = list_make1($6);
					n->isconstraint  = TRUE;
					processCASbits($8, @8, "ASSERTION",
								   &n->deferrable, &n->initdeferred, NULL,
								   NULL, yyscanner);

					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("CREATE ASSERTION is not yet implemented")));

					$$ = (Node *)n;
				}
		;

DropAssertStmt:
			DROP ASSERTION name opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->objects = NIL;
					n->arguments = NIL;
					n->behavior = $4;
					n->removeType = OBJECT_TRIGGER; /* XXX */
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("DROP ASSERTION is not yet implemented")));
					$$ = (Node *) n;
				}
		;


/*****************************************************************************
 *
 *		QUERY :
 *				define (aggregate,operator,type)
 *
 *****************************************************************************/

DefineStmt:
			CREATE AGGREGATE func_name aggr_args definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_AGGREGATE;
					n->oldstyle = false;
					n->defnames = $3;
					n->args = $4;
					n->definition = $5;
					$$ = (Node *)n;
				}
			| CREATE AGGREGATE func_name old_aggr_definition
				{
					/* old-style (pre-8.2) syntax for CREATE AGGREGATE */
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_AGGREGATE;
					n->oldstyle = true;
					n->defnames = $3;
					n->args = NIL;
					n->definition = $4;
					$$ = (Node *)n;
				}
			| CREATE OPERATOR any_operator definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_OPERATOR;
					n->oldstyle = false;
					n->defnames = $3;
					n->args = NIL;
					n->definition = $4;
					$$ = (Node *)n;
				}
			| CREATE TYPE_P any_name definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_TYPE;
					n->oldstyle = false;
					n->defnames = $3;
					n->args = NIL;
					n->definition = $4;
					$$ = (Node *)n;
				}
			| CREATE TYPE_P any_name
				{
					/* Shell type (identified by lack of definition) */
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_TYPE;
					n->oldstyle = false;
					n->defnames = $3;
					n->args = NIL;
					n->definition = NIL;
					$$ = (Node *)n;
				}
			| CREATE TYPE_P any_name AS '(' OptTableFuncElementList ')'
				{
					CompositeTypeStmt *n = makeNode(CompositeTypeStmt);

					/* can't use qualified_name, sigh */
					n->typevar = makeRangeVarFromAnyName($3, @3, yyscanner);
					n->coldeflist = $6;
					$$ = (Node *)n;
				}
			| CREATE TYPE_P any_name AS ENUM_P '(' opt_enum_val_list ')'
				{
					CreateEnumStmt *n = makeNode(CreateEnumStmt);
					n->typname = $3;
					n->vals = $7;
					$$ = (Node *)n;
				}
			| CREATE TYPE_P any_name AS RANGE definition
				{
					CreateRangeStmt *n = makeNode(CreateRangeStmt);
					n->typname = $3;
					n->params	= $6;
					$$ = (Node *)n;
				}
			| CREATE TEXT_P SEARCH PARSER any_name definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_TSPARSER;
					n->args = NIL;
					n->defnames = $5;
					n->definition = $6;
					$$ = (Node *)n;
				}
			| CREATE TEXT_P SEARCH DICTIONARY any_name definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_TSDICTIONARY;
					n->args = NIL;
					n->defnames = $5;
					n->definition = $6;
					$$ = (Node *)n;
				}
			| CREATE TEXT_P SEARCH TEMPLATE any_name definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_TSTEMPLATE;
					n->args = NIL;
					n->defnames = $5;
					n->definition = $6;
					$$ = (Node *)n;
				}
			| CREATE TEXT_P SEARCH CONFIGURATION any_name tsconf_definition opt_cfoptions
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_TSCONFIGURATION;
					n->args = $7; /* record configuration options */
					n->defnames = $5;
					n->definition = $6;
					$$ = (Node *)n;
				}
			| CREATE COLLATION any_name definition
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_COLLATION;
					n->args = NIL;
					n->defnames = $3;
					n->definition = $4;
					$$ = (Node *)n;
				}
			| CREATE COLLATION any_name FROM any_name
				{
					DefineStmt *n = makeNode(DefineStmt);
					n->kind = OBJECT_COLLATION;
					n->args = NIL;
					n->defnames = $3;
					n->definition = list_make1(makeDefElem("from", (Node *) $5));
					$$ = (Node *)n;
				}
		;

opt_cfoptions: WITH cfoptions							{ $$ = $2; }
			| /* EMPTY */								{ $$ = NIL; }
		;

cfoptions:'(' cfoption_list ')'							{ $$ = $2; }
		;

cfoption_list: cfoption_elem							{ $$ = list_make1($1); }
			| cfoption_list ',' cfoption_elem			{ $$ = lappend($1, $3); }
		;

cfoption_elem: ColLabel '=' def_arg						{ $$ = makeDefElem($1, (Node *) $3); }
			| ColLabel									{ $$ = makeDefElem($1, NULL); }
		;

tsconf_definition: '(' tsconf_def_list ')'				{ $$ = $2; }
		;

tsconf_def_list: tsconf_def_elem						{ $$ = list_make1($1); }
			| tsconf_def_list ',' tsconf_def_elem		{ $$ = lappend($1, $3); }
		;

tsconf_def_elem: PARSER '=' any_name					{ $$ = makeDefElem("parser", (Node *) $3); }
			| PARSER '=' DEFAULT						{ $$ = makeDefElem("parser", (Node *) list_make1(makeString("default"))); }
			| COPY '=' any_name							{ $$ = makeDefElem("copy", (Node *) $3); }
		;

definition: '(' def_list ')'						{ $$ = $2; }
		;

def_list:	def_elem								{ $$ = list_make1($1); }
			| def_list ',' def_elem					{ $$ = lappend($1, $3); }
		;

def_elem:	ColLabel '=' def_arg
				{
					$$ = makeDefElem($1, (Node *) $3);
				}
			| ColLabel
				{
					$$ = makeDefElem($1, NULL);
				}
		;

/* Note: any simple identifier will be returned as a type name! */
def_arg:	func_type						{ $$ = (Node *)$1; }
			| reserved_keyword				{ $$ = (Node *)makeString(pstrdup($1)); }
			| qual_all_Op					{ $$ = (Node *)$1; }
			| NumericOnly					{ $$ = (Node *)$1; }
			| Sconst						{ $$ = (Node *)makeString($1); }
		;

/*
 * aggregate function args.
 * (*)									- normal agg with no args
 * (aggr_arg,...)						- normal agg with args
 * (ORDER BY type_list,...)				- ordered-set agg with no direct args

 */
aggr_args:	'(' type_list ')'						{ $$ = list_make2($2, makeInteger(-1)); }
			| '(' '*' ')'							{ $$ = list_make2(NIL, makeInteger(-1)); }
			| '(' ORDER BY type_list ')'
				{
					$$ = list_make2($4, makeInteger(0));
				}
		;

old_aggr_definition: '(' old_aggr_list ')'			{ $$ = $2; }
		;

old_aggr_list: old_aggr_elem						{ $$ = list_make1($1); }
			| old_aggr_list ',' old_aggr_elem		{ $$ = lappend($1, $3); }
		;

/*
 * Must use IDENT here to avoid reduce/reduce conflicts; fortunately none of
 * the item names needed in old aggregate definitions are likely to become
 * SQL keywords.
 */
old_aggr_elem:  IDENT '=' def_arg
				{
					$$ = makeDefElem($1, (Node *)$3);
				}
		;

opt_enum_val_list:
		enum_val_list							{ $$ = $1; }
		| /*EMPTY*/								{ $$ = NIL; }
		;

enum_val_list:	Sconst
				{ $$ = list_make1(makeString($1)); }
			| enum_val_list ',' Sconst
				{ $$ = lappend($1, makeString($3)); }
		;

/*****************************************************************************
 *
 *	ALTER TYPE enumtype ADD ...
 *
 *****************************************************************************/

AlterEnumStmt:
		ALTER TYPE_P any_name ADD_P VALUE_P opt_if_not_exists Sconst
			{
				AlterEnumStmt *n = makeNode(AlterEnumStmt);
				n->typname = $3;
				n->oldVal = NULL;
				n->newVal = $7;
				n->newValNeighbor = NULL;
				n->newValIsAfter = true;
				n->skipIfNewValExists = $6;
				$$ = (Node *) n;
			}
		 | ALTER TYPE_P any_name ADD_P VALUE_P opt_if_not_exists Sconst BEFORE Sconst
			{
				AlterEnumStmt *n = makeNode(AlterEnumStmt);
				n->typname = $3;
				n->oldVal = NULL;
				n->newVal = $7;
				n->newValNeighbor = $9;
				n->newValIsAfter = false;
				n->skipIfNewValExists = $6;
				$$ = (Node *) n;
			}
		 | ALTER TYPE_P any_name ADD_P VALUE_P opt_if_not_exists Sconst AFTER Sconst
			{
				AlterEnumStmt *n = makeNode(AlterEnumStmt);
				n->typname = $3;
				n->oldVal = NULL;
				n->newVal = $7;
				n->newValNeighbor = $9;
				n->newValIsAfter = true;
				n->skipIfNewValExists = $6;
				$$ = (Node *) n;
			}
		 | ALTER TYPE_P any_name RENAME VALUE_P Sconst TO Sconst
			{
				AlterEnumStmt *n = makeNode(AlterEnumStmt);
				n->typname = $3;
				n->oldVal = $6;
				n->newVal = $8;
				n->newValNeighbor = NULL;
				n->newValIsAfter = false;
				n->skipIfNewValExists = false;
				$$ = (Node *) n;
			}
		 ;

opt_if_not_exists: IF_P NOT EXISTS 			{ $$ = true; }
		| /* empty */						{ $$ = false; }
		;

/*****************************************************************************
 *
 *		QUERIES :
 *				CREATE OPERATOR CLASS ...
 *				CREATE OPERATOR FAMILY ...
 *				ALTER OPERATOR FAMILY ...
 *				DROP OPERATOR CLASS ...
 *				DROP OPERATOR FAMILY ...
 *
 *****************************************************************************/

CreateOpClassStmt:
			CREATE OPERATOR CLASS any_name opt_default FOR TYPE_P Typename
			USING access_method opt_opfamily AS opclass_item_list
				{
					CreateOpClassStmt *n = makeNode(CreateOpClassStmt);
					n->opclassname = $4;
					n->isDefault = $5;
					n->datatype = $8;
					n->amname = $10;
					n->opfamilyname = $11;
					n->items = $13;
					$$ = (Node *) n;
				}
		;

opclass_item_list:
			opclass_item							{ $$ = list_make1($1); }
			| opclass_item_list ',' opclass_item	{ $$ = lappend($1, $3); }
		;

opclass_item:
			OPERATOR Iconst any_operator opclass_purpose opt_recheck
				{
					CreateOpClassItem *n = makeNode(CreateOpClassItem);
					n->itemtype = OPCLASS_ITEM_OPERATOR;
					n->name = $3;
					n->args = NIL;
					n->number = $2;
					n->order_family = $4;
					$$ = (Node *) n;
				}
			| OPERATOR Iconst any_operator oper_argtypes opclass_purpose
			  opt_recheck
				{
					CreateOpClassItem *n = makeNode(CreateOpClassItem);
					n->itemtype = OPCLASS_ITEM_OPERATOR;
					n->name = $3;
					n->args = $4;
					n->number = $2;
					n->order_family = $5;
					$$ = (Node *) n;
				}
			| FUNCTION Iconst func_name func_args
				{
					CreateOpClassItem *n = makeNode(CreateOpClassItem);
					n->itemtype = OPCLASS_ITEM_FUNCTION;
					n->name = $3;
					n->args = extractArgTypes($4);
					n->number = $2;
					$$ = (Node *) n;
				}
			| FUNCTION Iconst '(' type_list ')' func_name func_args
				{
					CreateOpClassItem *n = makeNode(CreateOpClassItem);
					n->itemtype = OPCLASS_ITEM_FUNCTION;
					n->name = $6;
					n->args = extractArgTypes($7);
					n->number = $2;
					n->class_args = $4;
					$$ = (Node *) n;
				}
			| STORAGE Typename
				{
					CreateOpClassItem *n = makeNode(CreateOpClassItem);
					n->itemtype = OPCLASS_ITEM_STORAGETYPE;
					n->storedtype = $2;
					$$ = (Node *) n;
				}
		;

opt_default:	DEFAULT						{ $$ = TRUE; }
			| /*EMPTY*/						{ $$ = FALSE; }
		;

opt_opfamily:	FAMILY any_name				{ $$ = $2; }
			| /*EMPTY*/						{ $$ = NIL; }
		;

opclass_purpose: FOR SEARCH					{ $$ = NIL; }
			| FOR ORDER BY any_name			{ $$ = $4; }
			| /*EMPTY*/						{ $$ = NIL; }
		;

opt_recheck:	RECHECK
				{
					/*
					 * RECHECK no longer does anything in opclass definitions,
					 * but we still accept it to ease porting of old database
					 * dumps.
					 */
					ereport(NOTICE,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("RECHECK is no longer required"),
							 errhint("Update your data type."),
							 parser_errposition(@1)));
					$$ = TRUE;
				}
			| /*EMPTY*/						{ $$ = FALSE; }
		;


CreateOpFamilyStmt:
			CREATE OPERATOR FAMILY any_name USING access_method
				{
					CreateOpFamilyStmt *n = makeNode(CreateOpFamilyStmt);
					n->opfamilyname = $4;
					n->amname = $6;
					$$ = (Node *) n;
				}
		;

AlterOpFamilyStmt:
			ALTER OPERATOR FAMILY any_name USING access_method ADD_P opclass_item_list
				{
					AlterOpFamilyStmt *n = makeNode(AlterOpFamilyStmt);
					n->opfamilyname = $4;
					n->amname = $6;
					n->isDrop = false;
					n->items = $8;
					$$ = (Node *) n;
				}
			| ALTER OPERATOR FAMILY any_name USING access_method DROP opclass_drop_list
				{
					AlterOpFamilyStmt *n = makeNode(AlterOpFamilyStmt);
					n->opfamilyname = $4;
					n->amname = $6;
					n->isDrop = true;
					n->items = $8;
					$$ = (Node *) n;
				}
		;

opclass_drop_list:
			opclass_drop							{ $$ = list_make1($1); }
			| opclass_drop_list ',' opclass_drop	{ $$ = lappend($1, $3); }
		;

opclass_drop:
			OPERATOR Iconst '(' type_list ')'
				{
					CreateOpClassItem *n = makeNode(CreateOpClassItem);
					n->itemtype = OPCLASS_ITEM_OPERATOR;
					n->number = $2;
					n->args = $4;
					$$ = (Node *) n;
				}
			| FUNCTION Iconst '(' type_list ')'
				{
					CreateOpClassItem *n = makeNode(CreateOpClassItem);
					n->itemtype = OPCLASS_ITEM_FUNCTION;
					n->number = $2;
					n->args = $4;
					$$ = (Node *) n;
				}
		;


DropOpClassStmt:
			DROP OPERATOR CLASS any_name USING access_method opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->objects = list_make1($4);
					n->arguments = list_make1(list_make1(makeString($6)));
					n->removeType = OBJECT_OPCLASS;
					n->behavior = $7;
					n->missing_ok = false;
					n->concurrent = false;
					$$ = (Node *) n;
				}
			| DROP OPERATOR CLASS IF_P EXISTS any_name USING access_method opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->objects = list_make1($6);
					n->arguments = list_make1(list_make1(makeString($8)));
					n->removeType = OBJECT_OPCLASS;
					n->behavior = $9;
					n->missing_ok = true;
					n->concurrent = false;
					$$ = (Node *) n;
				}
		;

DropOpFamilyStmt:
			DROP OPERATOR FAMILY any_name USING access_method opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->objects = list_make1($4);
					n->arguments = list_make1(list_make1(makeString($6)));
					n->removeType = OBJECT_OPFAMILY;
					n->behavior = $7;
					n->missing_ok = false;
					n->concurrent = false;
					$$ = (Node *) n;
				}
			| DROP OPERATOR FAMILY IF_P EXISTS any_name USING access_method opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->objects = list_make1($6);
					n->arguments = list_make1(list_make1(makeString($8)));
					n->removeType = OBJECT_OPFAMILY;
					n->behavior = $9;
					n->missing_ok = true;
					n->concurrent = false;
					$$ = (Node *) n;
				}
		;


/*****************************************************************************
 *
 *		QUERY:
 *
 *		DROP OWNED BY username [, username ...] [ RESTRICT | CASCADE ]
 *		REASSIGN OWNED BY username [, username ...] TO username
 *
 *****************************************************************************/
DropOwnedStmt:
			DROP OWNED BY name_list opt_drop_behavior
				{
					DropOwnedStmt *n = makeNode(DropOwnedStmt);
					n->roles = $4;
					n->behavior = $5;
					$$ = (Node *)n;
				}
		;

ReassignOwnedStmt:
			REASSIGN OWNED BY name_list TO name
				{
					ReassignOwnedStmt *n = makeNode(ReassignOwnedStmt);
					n->roles = $4;
					n->newrole = $6;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 *		QUERY:
 *
 *		DROP itemtype [ IF EXISTS ] itemname [, itemname ...]
 *           [ RESTRICT | CASCADE ]
 *
 *****************************************************************************/

DropStmt:	DROP drop_type IF_P EXISTS any_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->missing_ok = TRUE;
					n->objects = $5;
					n->arguments = NIL;
					n->behavior = $6;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP drop_type any_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = $2;
					n->missing_ok = FALSE;
					n->objects = $3;
					n->arguments = NIL;
					n->behavior = $4;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP INDEX CONCURRENTLY any_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_INDEX;
					n->missing_ok = FALSE;
					n->objects = $4;
					n->arguments = NIL;
					n->behavior = $5;
					n->concurrent = true;
					$$ = (Node *)n;
				}
			| DROP INDEX CONCURRENTLY IF_P EXISTS any_name_list opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_INDEX;
					n->missing_ok = TRUE;
					n->objects = $6;
					n->arguments = NIL;
					n->behavior = $7;
					n->concurrent = true;
					$$ = (Node *)n;
				}
		;


drop_type:	TABLE									{ $$ = OBJECT_TABLE; }
            | CONTVIEW                              { $$ = OBJECT_CONTQUERY; }
            | STREAM                                { $$ = OBJECT_STREAM; }
			| SEQUENCE								{ $$ = OBJECT_SEQUENCE; }
			| VIEW									{ $$ = OBJECT_VIEW; }
			| MATERIALIZED VIEW 					{ $$ = OBJECT_MATVIEW; }
			| INDEX									{ $$ = OBJECT_INDEX; }
			| FOREIGN TABLE							{ $$ = OBJECT_FOREIGN_TABLE; }
			| TYPE_P								{ $$ = OBJECT_TYPE; }
			| DOMAIN_P								{ $$ = OBJECT_DOMAIN; }
			| COLLATION								{ $$ = OBJECT_COLLATION; }
			| CONVERSION_P							{ $$ = OBJECT_CONVERSION; }
			| SCHEMA								{ $$ = OBJECT_SCHEMA; }
			| EXTENSION								{ $$ = OBJECT_EXTENSION; }
			| TEXT_P SEARCH PARSER					{ $$ = OBJECT_TSPARSER; }
			| TEXT_P SEARCH DICTIONARY				{ $$ = OBJECT_TSDICTIONARY; }
			| TEXT_P SEARCH TEMPLATE				{ $$ = OBJECT_TSTEMPLATE; }
			| TEXT_P SEARCH CONFIGURATION			{ $$ = OBJECT_TSCONFIGURATION; }
            | CLIENT MASTER KEY                     { $$ = OBJECT_GLOBAL_SETTING; }
            | COLUMN ENCRYPTION KEY                 { $$ = OBJECT_COLUMN_SETTING; }
		;

any_name_list:
			any_name								{ $$ = list_make1($1); }
			| any_name_list ',' any_name			{ $$ = lappend($1, $3); }
		;

any_name:	ColId						{ $$ = list_make1(makeString($1)); }
			| ColId attrs				{ $$ = lcons(makeString($1), $2); }
		;

attrs:		'.' attr_name
					{ $$ = list_make1(makeString($2)); }
			| attrs '.' attr_name
					{ $$ = lappend($1, makeString($3)); }
		;


/*****************************************************************************
 *
 *		QUERY:
 *				truncate table relname1, relname2, ...
 *
 *****************************************************************************/

TruncateStmt:
			TRUNCATE opt_table relation_expr_list opt_restart_seqs opt_drop_behavior
				{
					TruncateStmt *n = makeNode(TruncateStmt);
					n->relations = $3;
					n->restart_seqs = $4;
					n->behavior = $5;
					$$ = (Node *)n;
				}
		;

opt_restart_seqs:
			CONTINUE_P IDENTITY_P		{ $$ = false; }
			| RESTART IDENTITY_P		{ $$ = true; }
			| /* EMPTY */				{ $$ = false; }
		;

/*****************************************************************************
 *
 *	The COMMENT ON statement can take different forms based upon the type of
 *	the object associated with the comment. The form of the statement is:
 *
 *	COMMENT ON [ [ DATABASE | DOMAIN | INDEX | SEQUENCE | TABLE | TYPE | VIEW |
 *				   COLLATION | CONVERSION | LANGUAGE | OPERATOR CLASS |
 *				   LARGE OBJECT | CAST | COLUMN | SCHEMA | TABLESPACE |
 *				   EXTENSION | ROLE | TEXT SEARCH PARSER |
 *				   TEXT SEARCH DICTIONARY | TEXT SEARCH TEMPLATE |
 *				   TEXT SEARCH CONFIGURATION | FOREIGN TABLE |
 *				   FOREIGN DATA WRAPPER | SERVER | EVENT TRIGGER |
				   MATERIALIZED VIEW] <objname> |
 *				 AGGREGATE <aggname> (arg1, ...) |
 *				 FUNCTION <funcname> (arg1, arg2, ...) |
 *				 OPERATOR <op> (leftoperand_typ, rightoperand_typ) |
 *				 TRIGGER <triggername> ON <relname> |
 *				 CONSTRAINT <constraintname> ON <relname> |
 *				 RULE <rulename> ON <relname> ]
 *			   IS 'text'
 *
 *****************************************************************************/

CommentStmt:
			COMMENT ON comment_type any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = $3;
					n->objname = $4;
					n->objargs = NIL;
					n->comment = $6;
					$$ = (Node *) n;
				}
			| COMMENT ON AGGREGATE func_name aggr_args IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_AGGREGATE;
					n->objname = $4;
					n->objargs = $5;
					n->comment = $7;
					$$ = (Node *) n;
				}
			| COMMENT ON FUNCTION func_name func_args IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_FUNCTION;
					n->objname = $4;
					n->objargs = extractArgTypes($5);
					n->comment = $7;
					$$ = (Node *) n;
				}
			| COMMENT ON OPERATOR any_operator oper_argtypes IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_OPERATOR;
					n->objname = $4;
					n->objargs = $5;
					n->comment = $7;
					$$ = (Node *) n;
				}
			| COMMENT ON CONSTRAINT name ON any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_CONSTRAINT;
					n->objname = lappend($6, makeString($4));
					n->objargs = NIL;
					n->comment = $8;
					$$ = (Node *) n;
				}
			| COMMENT ON RULE name ON any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_RULE;
					n->objname = lappend($6, makeString($4));
					n->objargs = NIL;
					n->comment = $8;
					$$ = (Node *) n;
				}
			| COMMENT ON RULE name IS comment_text
				{
					/* Obsolete syntax supported for awhile for compatibility */
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_RULE;
					n->objname = list_make1(makeString($4));
					n->objargs = NIL;
					n->comment = $6;
					$$ = (Node *) n;
				}
			| COMMENT ON TRIGGER name ON any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_TRIGGER;
					n->objname = lappend($6, makeString($4));
					n->objargs = NIL;
					n->comment = $8;
					$$ = (Node *) n;
				}
			| COMMENT ON OPERATOR CLASS any_name USING access_method IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_OPCLASS;
					n->objname = $5;
					n->objargs = list_make1(makeString($7));
					n->comment = $9;
					$$ = (Node *) n;
				}
			| COMMENT ON OPERATOR FAMILY any_name USING access_method IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_OPFAMILY;
					n->objname = $5;
					n->objargs = list_make1(makeString($7));
					n->comment = $9;
					$$ = (Node *) n;
				}
			| COMMENT ON LARGE_P OBJECT_P NumericOnly IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_LARGEOBJECT;
					n->objname = list_make1($5);
					n->objargs = NIL;
					n->comment = $7;
					$$ = (Node *) n;
				}
			| COMMENT ON CAST '(' Typename AS Typename ')' IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_CAST;
					n->objname = list_make1($5);
					n->objargs = list_make1($7);
					n->comment = $10;
					$$ = (Node *) n;
				}
			| COMMENT ON opt_procedural LANGUAGE any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_LANGUAGE;
					n->objname = $5;
					n->objargs = NIL;
					n->comment = $7;
					$$ = (Node *) n;
				}
			| COMMENT ON TEXT_P SEARCH PARSER any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_TSPARSER;
					n->objname = $6;
					n->comment = $8;
					$$ = (Node *) n;
				}
			| COMMENT ON TEXT_P SEARCH DICTIONARY any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_TSDICTIONARY;
					n->objname = $6;
					n->comment = $8;
					$$ = (Node *) n;
				}
			| COMMENT ON TEXT_P SEARCH TEMPLATE any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_TSTEMPLATE;
					n->objname = $6;
					n->comment = $8;
					$$ = (Node *) n;
				}
			| COMMENT ON TEXT_P SEARCH CONFIGURATION any_name IS comment_text
				{
					CommentStmt *n = makeNode(CommentStmt);
					n->objtype = OBJECT_TSCONFIGURATION;
					n->objname = $6;
					n->comment = $8;
					$$ = (Node *) n;
				}
		;

comment_type:
			COLUMN								{ $$ = OBJECT_COLUMN; }
			| DATABASE							{ $$ = OBJECT_DATABASE; }
			| SCHEMA							{ $$ = OBJECT_SCHEMA; }
			| INDEX								{ $$ = OBJECT_INDEX; }
			| SEQUENCE							{ $$ = OBJECT_SEQUENCE; }
			| TABLE								{ $$ = OBJECT_TABLE; }
			| DOMAIN_P							{ $$ = OBJECT_DOMAIN; }
			| TYPE_P							{ $$ = OBJECT_TYPE; }
			| VIEW								{ $$ = OBJECT_VIEW; }
			| MATERIALIZED VIEW 				{ $$ = OBJECT_MATVIEW; }
			| COLLATION							{ $$ = OBJECT_COLLATION; }
			| CONVERSION_P						{ $$ = OBJECT_CONVERSION; }
			| TABLESPACE						{ $$ = OBJECT_TABLESPACE; }
			| EXTENSION							{ $$ = OBJECT_EXTENSION; }
			| ROLE								{ $$ = OBJECT_ROLE; }
			| USER								{ $$ = OBJECT_USER; }
			| FOREIGN TABLE						{ $$ = OBJECT_FOREIGN_TABLE; }
			| SERVER							{ $$ = OBJECT_FOREIGN_SERVER; }
			| FOREIGN DATA_P WRAPPER			{ $$ = OBJECT_FDW; }
		;

comment_text:
			Sconst								{ $$ = $1; }
			| NULL_P							{ $$ = NULL; }
		;


/*****************************************************************************
 *
 *  SECURITY LABEL [FOR <provider>] ON <object> IS <label>
 *
 *  As with COMMENT ON, <object> can refer to various types of database
 *  objects (e.g. TABLE, COLUMN, etc.).
 *
 *****************************************************************************/

SecLabelStmt:
			SECURITY LABEL opt_provider ON security_label_type any_name
			IS security_label
				{
					SecLabelStmt *n = makeNode(SecLabelStmt);
					n->provider = $3;
					n->objtype = $5;
					n->objname = $6;
					n->objargs = NIL;
					n->label = $8;
					$$ = (Node *) n;
				}
			| SECURITY LABEL opt_provider ON AGGREGATE func_name aggr_args
			  IS security_label
				{
					SecLabelStmt *n = makeNode(SecLabelStmt);
					n->provider = $3;
					n->objtype = OBJECT_AGGREGATE;
					n->objname = $6;
					n->objargs = $7;
					n->label = $9;
					$$ = (Node *) n;
				}
			| SECURITY LABEL opt_provider ON FUNCTION func_name func_args
			  IS security_label
				{
					SecLabelStmt *n = makeNode(SecLabelStmt);
					n->provider = $3;
					n->objtype = OBJECT_FUNCTION;
					n->objname = $6;
					n->objargs = extractArgTypes($7);
					n->label = $9;
					$$ = (Node *) n;
				}
			| SECURITY LABEL opt_provider ON LARGE_P OBJECT_P NumericOnly
			  IS security_label
				{
					SecLabelStmt *n = makeNode(SecLabelStmt);
					n->provider = $3;
					n->objtype = OBJECT_LARGEOBJECT;
					n->objname = list_make1($7);
					n->objargs = NIL;
					n->label = $9;
					$$ = (Node *) n;
				}
			| SECURITY LABEL opt_provider ON opt_procedural LANGUAGE any_name
			  IS security_label
				{
					SecLabelStmt *n = makeNode(SecLabelStmt);
					n->provider = $3;
					n->objtype = OBJECT_LANGUAGE;
					n->objname = $7;
					n->objargs = NIL;
					n->label = $9;
					$$ = (Node *) n;
				}
		;

opt_provider:	FOR ColId_or_Sconst	{ $$ = $2; }
				| /* empty */		{ $$ = NULL; }
		;

security_label_type:
			COLUMN								{ $$ = OBJECT_COLUMN; }
			| DATABASE							{ $$ = OBJECT_DATABASE; }
			| FOREIGN TABLE						{ $$ = OBJECT_FOREIGN_TABLE; }
			| SCHEMA							{ $$ = OBJECT_SCHEMA; }
			| SEQUENCE							{ $$ = OBJECT_SEQUENCE; }
			| TABLE								{ $$ = OBJECT_TABLE; }
			| DOMAIN_P							{ $$ = OBJECT_TYPE; }
			| ROLE								{ $$ = OBJECT_ROLE; }
			| USER								{ $$ = OBJECT_USER; }
			| TABLESPACE						{ $$ = OBJECT_TABLESPACE; }
			| TYPE_P							{ $$ = OBJECT_TYPE; }
			| VIEW								{ $$ = OBJECT_VIEW; }
			| MATERIALIZED VIEW 				{ $$ = OBJECT_MATVIEW; }
		;

security_label:	Sconst				{ $$ = $1; }
				| NULL_P			{ $$ = NULL; }
		;

/*****************************************************************************
 *
 *		QUERY:
 *			fetch/move
 *
 *****************************************************************************/

FetchStmt:	FETCH fetch_args
				{
					FetchStmt *n = (FetchStmt *) $2;
					n->ismove = FALSE;
					$$ = (Node *)n;
				}
			| MOVE fetch_args
				{
					FetchStmt *n = (FetchStmt *) $2;
					n->ismove = TRUE;
					$$ = (Node *)n;
				}
		;

fetch_args:	cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $1;
					n->direction = FETCH_FORWARD;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $2;
					n->direction = FETCH_FORWARD;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| NEXT opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $3;
					n->direction = FETCH_FORWARD;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| PRIOR opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $3;
					n->direction = FETCH_BACKWARD;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| FIRST_P opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $3;
					n->direction = FETCH_ABSOLUTE;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| LAST_P opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $3;
					n->direction = FETCH_ABSOLUTE;
					n->howMany = -1;
					$$ = (Node *)n;
				}
			| ABSOLUTE_P SignedIconst opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $4;
					n->direction = FETCH_ABSOLUTE;
					n->howMany = $2;
					$$ = (Node *)n;
				}
			| RELATIVE_P SignedIconst opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $4;
					n->direction = FETCH_RELATIVE;
					n->howMany = $2;
					$$ = (Node *)n;
				}
			| SignedIconst opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $3;
					n->direction = FETCH_FORWARD;
					n->howMany = $1;
					$$ = (Node *)n;
				}
			| ALL opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $3;
					n->direction = FETCH_FORWARD;
					n->howMany = FETCH_ALL;
					$$ = (Node *)n;
				}
			| FORWARD opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $3;
					n->direction = FETCH_FORWARD;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| FORWARD SignedIconst opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $4;
					n->direction = FETCH_FORWARD;
					n->howMany = $2;
					$$ = (Node *)n;
				}
			| FORWARD ALL opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $4;
					n->direction = FETCH_FORWARD;
					n->howMany = FETCH_ALL;
					$$ = (Node *)n;
				}
			| BACKWARD opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $3;
					n->direction = FETCH_BACKWARD;
					n->howMany = 1;
					$$ = (Node *)n;
				}
			| BACKWARD SignedIconst opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $4;
					n->direction = FETCH_BACKWARD;
					n->howMany = $2;
					$$ = (Node *)n;
				}
			| BACKWARD ALL opt_from_in cursor_name
				{
					FetchStmt *n = makeNode(FetchStmt);
					n->portalname = $4;
					n->direction = FETCH_BACKWARD;
					n->howMany = FETCH_ALL;
					$$ = (Node *)n;
				}
		;

from_in:	FROM									{}
			| IN_P									{}
		;

opt_from_in:	from_in								{}
			| /* EMPTY */							{}
		;


/*****************************************************************************
 *
 * GRANT and REVOKE statements
 *
 *****************************************************************************/

GrantStmt:	GRANT privileges ON privilege_target TO grantee_list
			opt_grant_grant_option
				{
					GrantStmt *n = makeNode(GrantStmt);
					n->is_grant = true;
					n->privileges = $2;
					n->targtype = ($4)->targtype;
					n->objtype = ($4)->objtype;
					n->objects = ($4)->objs;
					n->grantees = $6;
					n->grant_option = $7;
					$$ = (Node*)n;
				}
			| GRANT ALL privilege_str TO RoleId
				{
					AlterRoleStmt *n = makeNode(AlterRoleStmt);
					n->role = $5;
					n->action = +1;	/* add, if there are members */
					n->options =  lappend(NULL,makeDefElem("issystemadmin", (Node *)makeInteger(TRUE)));
					$$ = (Node *)n;
				}
		;

RevokeStmt:
			REVOKE privileges ON privilege_target
			FROM grantee_list opt_drop_behavior
				{
					GrantStmt *n = makeNode(GrantStmt);
					n->is_grant = false;
					n->grant_option = false;
					n->privileges = $2;
					n->targtype = ($4)->targtype;
					n->objtype = ($4)->objtype;
					n->objects = ($4)->objs;
					n->grantees = $6;
					n->behavior = $7;
					$$ = (Node *)n;
				}
			| REVOKE GRANT OPTION FOR privileges ON privilege_target
			FROM grantee_list opt_drop_behavior
				{
					GrantStmt *n = makeNode(GrantStmt);
					n->is_grant = false;
					n->grant_option = true;
					n->privileges = $5;
					n->targtype = ($7)->targtype;
					n->objtype = ($7)->objtype;
					n->objects = ($7)->objs;
					n->grantees = $9;
					n->behavior = $10;
					$$ = (Node *)n;
				}
			| REVOKE ALL privilege_str FROM RoleId
				{
					AlterRoleStmt *n = makeNode(AlterRoleStmt);
					n->role = $5;
					n->action = +1;	/* add, if there are members */
					n->options =  lappend(NULL,makeDefElem("issystemadmin", (Node *)makeInteger(FALSE)));
					$$ = (Node *)n;
				}
		;
privilege_str: PRIVILEGES { $$ = NULL;}
	| PRIVILEGE { $$ = NULL;}
		;


/*
 * Privilege names are represented as strings; the validity of the privilege
 * names gets checked at execution.  This is a bit annoying but we have little
 * choice because of the syntactic conflict with lists of role names in
 * GRANT/REVOKE.  What's more, we have to call out in the "privilege"
 * production any reserved keywords that need to be usable as privilege names.
 */

/* either ALL [PRIVILEGES] or a list of individual privileges */
privileges: privilege_list
				{ $$ = $1; }
			| ALL
				{ $$ = NIL; }
			| ALL PRIVILEGES
				{ $$ = NIL; }
			| ALL '(' columnList ')'
				{
					AccessPriv *n = makeNode(AccessPriv);
					n->priv_name = NULL;
					n->cols = $3;
					$$ = list_make1(n);
				}
			| ALL PRIVILEGES '(' columnList ')'
				{
					AccessPriv *n = makeNode(AccessPriv);
					n->priv_name = NULL;
					n->cols = $4;
					$$ = list_make1(n);
				}
		;

privilege_list:	privilege							{ $$ = list_make1($1); }
			| privilege_list ',' privilege			{ $$ = lappend($1, $3); }
		;

privilege:	SELECT opt_column_list
			{
				AccessPriv *n = makeNode(AccessPriv);
				n->priv_name = pstrdup($1);
				n->cols = $2;
				$$ = n;
			}
		| REFERENCES opt_column_list
			{
				AccessPriv *n = makeNode(AccessPriv);
				n->priv_name = pstrdup($1);
				n->cols = $2;
				$$ = n;
			}
		| CREATE opt_column_list
			{
				AccessPriv *n = makeNode(AccessPriv);
				n->priv_name = pstrdup($1);
				n->cols = $2;
				$$ = n;
			}
		| ColId opt_column_list
			{
				AccessPriv *n = makeNode(AccessPriv);
				n->priv_name = $1;
				n->cols = $2;
				$$ = n;
			}
		;


/* Don't bother trying to fold the first two rules into one using
 * opt_table.  You're going to get conflicts.
 */
privilege_target:
			qualified_name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_RELATION;
					n->objs = $1;
					$$ = n;
				}
			| TABLE qualified_name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_RELATION;
					n->objs = $2;
					$$ = n;
				}
			| SEQUENCE qualified_name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_SEQUENCE;
					n->objs = $2;
					$$ = n;
				}
			| FOREIGN DATA_P WRAPPER name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_FDW;
					n->objs = $4;
					$$ = n;
				}
			| FOREIGN SERVER name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_FOREIGN_SERVER;
					n->objs = $3;
					$$ = n;
				}
			| FUNCTION function_with_argtypes_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_FUNCTION;
					n->objs = $2;
					$$ = n;
				}
			| DATABASE name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_DATABASE;
					n->objs = $2;
					$$ = n;
				}
			| DOMAIN_P any_name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_DOMAIN;
					n->objs = $2;
					$$ = n;
				}
			| LANGUAGE name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_LANGUAGE;
					n->objs = $2;
					$$ = n;
				}
			| LARGE_P OBJECT_P NumericOnly_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_LARGEOBJECT;
					n->objs = $3;
					$$ = n;
				}
			| NODE GROUP_P name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_NODEGROUP;
					n->objs = $3;
					$$ = n;
				}
			| SCHEMA name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_NAMESPACE;
					n->objs = $2;
					$$ = n;
				}
			| TABLESPACE name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_TABLESPACE;
					n->objs = $2;
					$$ = n;
				}
			| DIRECTORY name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_DIRECTORY;
					n->objs = $2;
					$$ = n;
				}
			| TYPE_P any_name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_TYPE;
					n->objs = $2;
					$$ = n;
				}
			| ALL TABLES IN_P SCHEMA name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_ALL_IN_SCHEMA;
					n->objtype = ACL_OBJECT_RELATION;
					n->objs = $5;
					$$ = n;
				}
			| ALL SEQUENCES IN_P SCHEMA name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_ALL_IN_SCHEMA;
					n->objtype = ACL_OBJECT_SEQUENCE;
					n->objs = $5;
					$$ = n;
				}
			| ALL FUNCTIONS IN_P SCHEMA name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_ALL_IN_SCHEMA;
					n->objtype = ACL_OBJECT_FUNCTION;
					n->objs = $5;
					$$ = n;
				}
			| DATA_P SOURCE_P name_list
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_DATA_SOURCE;
					n->objs = $3;
					$$ = n;
				}
			| CLIENT_MASTER_KEY setting_name
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_GLOBAL_SETTING;
					n->objs = $2;
					$$ = n;
				}
			| COLUMN_ENCRYPTION_KEY setting_name
				{
					PrivTarget *n = (PrivTarget *) palloc(sizeof(PrivTarget));
					n->targtype = ACL_TARGET_OBJECT;
					n->objtype = ACL_OBJECT_COLUMN_SETTING;
					n->objs = $2;
					$$ = n;
				}
		;


grantee_list:
			grantee									{ $$ = list_make1($1); }
			| grantee_list ',' grantee				{ $$ = lappend($1, $3); }
		;

grantee:	RoleId
				{
					PrivGrantee *n = makeNode(PrivGrantee);
					/* This hack lets us avoid reserving PUBLIC as a keyword*/
					if (strcmp($1, "public") == 0)
						n->rolname = NULL;
					else
						n->rolname = $1;
					$$ = (Node *)n;
				}
			| GROUP_P RoleId
				{
					PrivGrantee *n = makeNode(PrivGrantee);
					/* Treat GROUP PUBLIC as a synonym for PUBLIC */
					if (strcmp($2, "public") == 0)
						n->rolname = NULL;
					else
						n->rolname = $2;
					$$ = (Node *)n;
				}
		;


opt_grant_grant_option:
			WITH GRANT OPTION 
				{ 
					if (isSecurityMode)
					{
						/* Do not support this grammar in security mode.*/
						ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("WITH GRANT OPTION is not supported in security mode.")));
					}
					$$ = TRUE;
				}
			| /*EMPTY*/ { $$ = FALSE; }
		;

function_with_argtypes_list:
			function_with_argtypes					{ $$ = list_make1($1); }
			| function_with_argtypes_list ',' function_with_argtypes
													{ $$ = lappend($1, $3); }
		;

function_with_argtypes:
			func_name func_args
				{
					FuncWithArgs *n = makeNode(FuncWithArgs);
					n->funcname = $1;
					n->funcargs = extractArgTypes($2);
					$$ = n;
				}
		;

/*****************************************************************************
 *
 * GRANT and REVOKE ROLE statements
 *
 *****************************************************************************/

GrantRoleStmt:
			GRANT privilege_list TO name_list opt_grant_admin_option opt_granted_by
				{
					GrantRoleStmt *n = makeNode(GrantRoleStmt);
					n->is_grant = true;
					n->granted_roles = $2;
					n->grantee_roles = $4;
					n->admin_opt = $5;
					n->grantor = $6;
					$$ = (Node*)n;
				}
		;

RevokeRoleStmt:
			REVOKE privilege_list FROM name_list opt_granted_by opt_drop_behavior
				{
					GrantRoleStmt *n = makeNode(GrantRoleStmt);
					n->is_grant = false;
					n->admin_opt = false;
					n->granted_roles = $2;
					n->grantee_roles = $4;
					n->behavior = $6;
					$$ = (Node*)n;
				}
			| REVOKE ADMIN OPTION FOR privilege_list FROM name_list opt_granted_by opt_drop_behavior
				{
					GrantRoleStmt *n = makeNode(GrantRoleStmt);
					n->is_grant = false;
					n->admin_opt = true;
					n->granted_roles = $5;
					n->grantee_roles = $7;
					n->behavior = $9;
					$$ = (Node*)n;
				}
		;

opt_grant_admin_option: WITH ADMIN OPTION				{ $$ = TRUE; }
			| /*EMPTY*/									{ $$ = FALSE; }
		;

opt_granted_by: GRANTED BY RoleId						{ $$ = $3; }
			| /*EMPTY*/									{ $$ = NULL; }
		;

/*****************************************************************************
 *
 * ALTER DEFAULT PRIVILEGES statement
 *
 *****************************************************************************/

AlterDefaultPrivilegesStmt:
			ALTER DEFAULT PRIVILEGES DefACLOptionList DefACLAction
				{
					AlterDefaultPrivilegesStmt *n = makeNode(AlterDefaultPrivilegesStmt);
					n->options = $4;
					n->action = (GrantStmt *) $5;
					$$ = (Node*)n;
				}
		;

DefACLOptionList:
			DefACLOptionList DefACLOption			{ $$ = lappend($1, $2); }
			| /* EMPTY */							{ $$ = NIL; }
		;

DefACLOption:
			IN_P SCHEMA name_list
				{
					$$ = makeDefElem("schemas", (Node *)$3);
				}
			| FOR ROLE name_list
				{
					$$ = makeDefElem("roles", (Node *)$3);
				}
			| FOR USER name_list
				{
					$$ = makeDefElem("roles", (Node *)$3);
				}
		;

/*
 * This should match GRANT/REVOKE, except that individual target objects
 * are not mentioned and we only allow a subset of object types.
 */
DefACLAction:
			GRANT privileges ON defacl_privilege_target TO grantee_list
			opt_grant_grant_option
				{
					GrantStmt *n = makeNode(GrantStmt);
					n->is_grant = true;
					n->privileges = $2;
					n->targtype = ACL_TARGET_DEFAULTS;
					n->objtype = (GrantObjectType)$4;
					n->objects = NIL;
					n->grantees = $6;
					n->grant_option = $7;
					$$ = (Node*)n;
				}
			| REVOKE privileges ON defacl_privilege_target
			FROM grantee_list opt_drop_behavior
				{
					GrantStmt *n = makeNode(GrantStmt);
					n->is_grant = false;
					n->grant_option = false;
					n->privileges = $2;
					n->targtype = ACL_TARGET_DEFAULTS;
					n->objtype = (GrantObjectType)$4;
					n->objects = NIL;
					n->grantees = $6;
					n->behavior = $7;
					$$ = (Node *)n;
				}
			| REVOKE GRANT OPTION FOR privileges ON defacl_privilege_target
			FROM grantee_list opt_drop_behavior
				{
					GrantStmt *n = makeNode(GrantStmt);
					n->is_grant = false;
					n->grant_option = true;
					n->privileges = $5;
					n->targtype = ACL_TARGET_DEFAULTS;
					n->objtype = (GrantObjectType)$7;
					n->objects = NIL;
					n->grantees = $9;
					n->behavior = $10;
					$$ = (Node *)n;
				}
		;

defacl_privilege_target:
			TABLES			{ $$ = ACL_OBJECT_RELATION; }
			| FUNCTIONS		{ $$ = ACL_OBJECT_FUNCTION; }
			| SEQUENCES		{ $$ = ACL_OBJECT_SEQUENCE; }
			| TYPES_P		{ $$ = ACL_OBJECT_TYPE; }
			| CLIENT_MASTER_KEYS			{ $$ = ACL_OBJECT_GLOBAL_SETTING; }
			| COLUMN_ENCRYPTION_KEYS		{ $$ = ACL_OBJECT_COLUMN_SETTING; }
		;


/*****************************************************************************
 *
 *		QUERY: CREATE INDEX
 *
 * Note: we cannot put TABLESPACE clause after WHERE clause unless we are
 * willing to make TABLESPACE a fully reserved word.
 *****************************************************************************/

IndexStmt:	CREATE opt_unique INDEX opt_concurrently opt_index_name
			ON qualified_name access_method_clause '(' index_params ')'
			opt_include opt_reloptions OptPartitionElement where_clause
				{
					IndexStmt *n = makeNode(IndexStmt);
					n->unique = $2;
					n->concurrent = $4;
                    n->schemaname = $5->schemaname;
					n->idxname = $5->relname;
					n->relation = $7;
					n->accessMethod = $8;
					n->indexParams = $10;
					n->indexIncludingParams = $12;
					n->options = $13;
					n->tableSpace = $14;
					n->whereClause = $15;
					n->excludeOpNames = NIL;
					n->idxcomment = NULL;
					n->indexOid = InvalidOid;
					n->oldNode = InvalidOid;
					n->partClause = NULL;
					n->isPartitioned = false;
					n->isGlobal = false;
					n->primary = false;
					n->isconstraint = false;
					n->deferrable = false;
					n->initdeferred = false;
					$$ = (Node *)n;
				}
				| CREATE opt_unique INDEX opt_concurrently opt_index_name
					ON qualified_name access_method_clause '(' index_params ')'
					LOCAL opt_partition_index_def opt_reloptions OptTableSpace
				{

					IndexStmt *n = makeNode(IndexStmt);
					n->unique = $2;
					n->concurrent = $4;
                    n->schemaname = $5->schemaname;
					n->idxname = $5->relname;
					n->relation = $7;
					n->accessMethod = $8;
					n->indexParams = $10;
					n->partClause  = $13;
					n->options = $14;
					n->tableSpace = $15;
					n->isPartitioned = true;
					n->isGlobal = false;
					n->excludeOpNames = NIL;
					n->idxcomment = NULL;
					n->indexOid = InvalidOid;
					n->oldNode = InvalidOid;
					n->primary = false;
					n->isconstraint = false;
					n->deferrable = false;
					n->initdeferred = false;
					$$ = (Node *)n;

				}
				| CREATE opt_unique INDEX opt_concurrently opt_index_name
					ON qualified_name access_method_clause '(' index_params ')'
					GLOBAL opt_reloptions OptTableSpace
				{

					IndexStmt *n = makeNode(IndexStmt);
					n->unique = $2;
					n->concurrent = $4;
                    n->schemaname = $5->schemaname;
					n->idxname = $5->relname;
					n->relation = $7;
					n->accessMethod = $8;
					n->indexParams = $10;
					n->partClause  = NULL;
					n->options = $13;
					n->tableSpace = $14;
					n->isPartitioned = true;
					n->isGlobal = true;
					n->excludeOpNames = NIL;
					n->idxcomment = NULL;
					n->indexOid = InvalidOid;
					n->oldNode = InvalidOid;
					n->primary = false;
					n->isconstraint = false;
					n->deferrable = false;
					n->initdeferred = false;
					$$ = (Node *)n;

				}
		;

opt_unique:
			UNIQUE									{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

opt_concurrently:
			CONCURRENTLY							{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

opt_index_name:
			index_name
				{
					$$ = makeRangeVar(NULL, $1, @1);
				}
			| ColId indirection
				{
					check_qualified_name($2, yyscanner);
					$$ = makeRangeVar(NULL, NULL, @1);
					switch (list_length($2))
					{
						case 1:
							$$->catalogname = NULL;
							$$->schemaname = $1;
							$$->relname = strVal(linitial($2));
							break;
						default:
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("improper qualified name (too many dotted names): %s",
											NameListToString(lcons(makeString($1), $2))),
									 parser_errposition(@1)));
							break;
					}
				}
			| /*EMPTY*/								{ $$ = makeRangeVar(NULL, NULL, -1); }
		;

access_method_clause:
			USING access_method						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

index_params:	index_elem							{ $$ = list_make1($1); }
			| index_params ',' index_elem			{ $$ = lappend($1, $3); }
		;

/*
 * Index attributes can be either simple column references, or arbitrary
 * expressions in parens.  For backwards-compatibility reasons, we allow
 * an expression that's just a function call to be written without parens.
 */
index_elem:	ColId opt_collate opt_class opt_asc_desc opt_nulls_order
				{
					$$ = makeNode(IndexElem);
					$$->name = $1;
					$$->expr = NULL;
					$$->indexcolname = NULL;
					$$->collation = $2;
					$$->opclass = $3;
					$$->ordering = (SortByDir)$4;
					$$->nulls_ordering = (SortByNulls)$5;
				}
			| func_expr_windowless opt_collate opt_class opt_asc_desc opt_nulls_order
				{
					$$ = makeNode(IndexElem);
					$$->name = NULL;
					$$->expr = $1;
					$$->indexcolname = NULL;
					$$->collation = $2;
					$$->opclass = $3;
					$$->ordering = (SortByDir)$4;
					$$->nulls_ordering = (SortByNulls)$5;
				}
			| '(' a_expr ')' opt_collate opt_class opt_asc_desc opt_nulls_order
				{
					$$ = makeNode(IndexElem);
					$$->name = NULL;
					$$->expr = $2;
					$$->indexcolname = NULL;
					$$->collation = $4;
					$$->opclass = $5;
					$$->ordering = (SortByDir)$6;
					$$->nulls_ordering = (SortByNulls)$7;
				}
		;

opt_include:		INCLUDE '(' index_including_params ')'			{ $$ = $3; }
			 |		/* EMPTY */						{ $$ = NIL; }
		;

index_including_params:	index_elem						{ $$ = list_make1($1); }
			| index_including_params ',' index_elem		{ $$ = lappend($1, $3); }
		;


opt_collate: COLLATE any_name						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

opt_class:	any_name								{ $$ = $1; }
			| USING any_name						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

opt_asc_desc: ASC							{ $$ = SORTBY_ASC; }
			| DESC							{ $$ = SORTBY_DESC; }
			| /*EMPTY*/						{ $$ = SORTBY_DEFAULT; }
		;

opt_nulls_order: NULLS_FIRST				{ $$ = SORTBY_NULLS_FIRST; }
			| NULLS_LAST					{ $$ = SORTBY_NULLS_LAST; }
			| /*EMPTY*/						{ $$ = SORTBY_NULLS_DEFAULT; }
		;


opt_partition_index_def:	'(' range_partition_index_list  ')'
							{
								$$ = (Node *)$2;
							}
							| 	{$$ = NULL;}
							;

range_partition_index_list:
							range_partition_index_item
							{
								$$ = (Node*)list_make1($1);
							}
							| range_partition_index_list ',' range_partition_index_item
							{
								$$ = (Node*)lappend((List*)$1, $3);
							}
							;


range_partition_index_item:
						PARTITION index_name OptTableSpace
						{
							RangePartitionindexDefState* def = makeNode(RangePartitionindexDefState);
							def->name = $2;
							def->tablespace = $3;
							$$ = (Node*)def;

						}
						;
/*****************************************************************************
 *
 *		QUERY:
 *				create [or replace] function <fname>
 *						[(<type-1> { , <type-n>})]
 *						returns <type-r>
 *						as <filename or code in language as appropriate>
 *						language <lang> [with parameters]
 *
 *****************************************************************************/

CreateFunctionStmt:
			CREATE opt_or_replace FUNCTION func_name_opt_arg proc_args
			RETURNS func_return createfunc_opt_list opt_definition
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->isOraStyle = false;
					n->replace = $2;
					n->funcname = $4;
					n->parameters = $5;
					n->returnType = $7;
					n->options = $8;
					n->withClause = $9;
					n->isProcedure = false;
					$$ = (Node *)n;
				}
			| CREATE opt_or_replace FUNCTION func_name_opt_arg proc_args
			  RETURNS TABLE '(' table_func_column_list ')' createfunc_opt_list opt_definition
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->isOraStyle = false;
					n->replace = $2;
					n->funcname = $4;
					n->parameters = mergeTableFuncParameters($5, $9);
					n->returnType = TableFuncTypeName($9);
					n->returnType->location = @7;
					n->options = $11;
					n->withClause = $12;
					n->isProcedure = false;
					$$ = (Node *)n;
				}
			| CREATE opt_or_replace FUNCTION func_name_opt_arg proc_args
			  createfunc_opt_list opt_definition
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->isOraStyle = false;
					n->replace = $2;
					n->funcname = $4;
					n->parameters = $5;
					n->returnType = NULL;
					n->options = $6;
					n->withClause = $7;
					n->isProcedure = false;
					$$ = (Node *)n;
				}
			| CREATE opt_or_replace FUNCTION func_name_opt_arg proc_args
			  RETURN func_return opt_createproc_opt_list as_is {u_sess->parser_cxt.eaten_declare = false; u_sess->parser_cxt.eaten_begin = false;} subprogram_body
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					n->isOraStyle = true;
					n->replace = $2;
					n->funcname = $4;
					n->parameters = $5;
					n->returnType = $7;
					n->options = $8;
					n->options = lappend(n->options, makeDefElem("as",
										(Node *)list_make1(makeString($11))));

					n->options = lappend(n->options, makeDefElem("language",
										(Node *)makeString("plpgsql")));
					n->withClause = NIL;
					n->withClause = NIL;
					n->isProcedure = false;
					$$ = (Node *)n;
				}
		;
CallFuncStmt:    CALL func_name '(' ')'
					{
						$$ = makeCallFuncStmt($2,NULL);
					}
				|	CALL func_name '(' callfunc_args ')'
					{
						$$ = makeCallFuncStmt($2,$4);
					}
				;
callfunc_args:   func_arg_expr
				{
					$$ = list_make1($1);
				}
			| callfunc_args ',' func_arg_expr
				{
					$$ = lappend($1, $3);
				}
			;
CreateProcedureStmt:
			CREATE opt_or_replace PROCEDURE func_name_opt_arg proc_args
			opt_createproc_opt_list as_is {u_sess->parser_cxt.eaten_declare = false; u_sess->parser_cxt.eaten_begin = false;} subprogram_body
				{
					CreateFunctionStmt *n = makeNode(CreateFunctionStmt);
					int count = get_outarg_num($5);
					n->isOraStyle = true;
					n->replace = $2;
					n->funcname = $4;
					n->parameters = $5;
					n->returnType = NULL;
					n->isProcedure = true;
					if (0 == count)
					{
						n->returnType = makeTypeName("void");
						n->returnType->typmods = NULL;
						n->returnType->arrayBounds = NULL;
					}
					n->options = $6;
					n->options = lappend(n->options, makeDefElem("as",
										(Node *)list_make1(makeString($9))));
					n->options = lappend(n->options, makeDefElem("language",
										(Node *)makeString("plpgsql")));
					n->withClause = NIL;
					$$ = (Node *)n;
				}

		;

opt_or_replace:
			OR REPLACE								{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

func_args:	'(' func_args_list ')'					{ $$ = $2; }
			| '(' ')'								{ $$ = NIL; }
		;

proc_args:   func_args_with_defaults				{ $$ = $1;  }
			|/*EMPTY*/								{ $$ = NIL; }
		;

func_args_list:
			func_arg								{ $$ = list_make1($1); }
			| func_args_list ',' func_arg			{ $$ = lappend($1, $3); }
		;

as_is:
		AS
		| IS
		;

/*
 * func_args_with_defaults is separate because we only want to accept
 * defaults in CREATE FUNCTION, not in ALTER etc.
 */
func_args_with_defaults:
		'(' func_args_with_defaults_list ')'		{ $$ = $2; }
		| '(' ')'									{ $$ = NIL; }
		;

func_args_with_defaults_list:
		func_arg_with_default						{ $$ = list_make1($1); }
		| func_args_with_defaults_list ',' func_arg_with_default
													{ $$ = lappend($1, $3); }
		;

/*
 * The style with arg_class first is SQL99 standard, but A db puts
 * param_name first; accept both since it's likely people will try both
 * anyway.  Don't bother trying to save productions by letting arg_class
 * have an empty alternative ... you'll get shift/reduce conflicts.
 *
 * We can catch over-specified arguments here if we want to,
 * but for now better to silently swallow typmod, etc.
 * - thomas 2000-03-22
 */
func_arg:
			arg_class param_name func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = $2;
					n->argType = $3;
					n->mode = $1;
					n->defexpr = NULL;
					$$ = n;
				}
			| param_name arg_class func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = $1;
					n->argType = $3;
					n->mode = $2;
					n->defexpr = NULL;
					$$ = n;
				}
			| param_name func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = $1;
					n->argType = $2;
					n->mode = FUNC_PARAM_IN;
					n->defexpr = NULL;
					$$ = n;
				}
			| arg_class func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = NULL;
					n->argType = $2;
					n->mode = $1;
					n->defexpr = NULL;
					$$ = n;
				}
			| func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = NULL;
					n->argType = $1;
					n->mode = FUNC_PARAM_IN;
					n->defexpr = NULL;
					$$ = n;
				}
		;

/* INOUT is SQL99 standard, IN OUT is for A db compatibility */
arg_class:	IN_P								{ $$ = FUNC_PARAM_IN; }
			| OUT_P								{ $$ = FUNC_PARAM_OUT; }
			| INOUT								{ $$ = FUNC_PARAM_INOUT; }
			| IN_P OUT_P						{ $$ = FUNC_PARAM_INOUT; }
			| VARIADIC							{ $$ = FUNC_PARAM_VARIADIC; }
		;

/*
 * Ideally param_name should be ColId, but that causes too many conflicts.
 */
param_name:	type_function_name
		;

func_return:
			func_type
				{
					/* We can catch over-specified results here if we want to,
					 * but for now better to silently swallow typmod, etc.
					 * - thomas 2000-03-22
					 */
					$$ = $1;
				}
			| func_type DETERMINISTIC
				{
					$$ = $1;
				}
		;

/*
 * We would like to make the %TYPE productions here be ColId attrs etc,
 * but that causes reduce/reduce conflicts.  type_function_name
 * is next best choice.
 */
func_type:	Typename								{ $$ = $1; }
			| type_function_name attrs '%' TYPE_P
				{
					$$ = makeTypeNameFromNameList(lcons(makeString($1), $2));
					$$->pct_type = true;
					$$->location = @1;
				}
			| SETOF type_function_name attrs '%' TYPE_P
				{
					$$ = makeTypeNameFromNameList(lcons(makeString($2), $3));
					$$->pct_type = true;
					$$->setof = TRUE;
					$$->location = @2;
				}
		;

func_arg_with_default:
		func_arg
				{
					$$ = $1;
				}
		| func_arg DEFAULT a_expr
				{
					$$ = $1;
					$$->defexpr = $3;
				}
		| func_arg '=' a_expr
				{
					$$ = $1;
					$$->defexpr = $3;
				}
		| func_arg COLON_EQUALS a_expr
				{
					$$ = $1;
					$$->defexpr = $3;
				}
		;


createfunc_opt_list:
			/* Must be at least one to prevent conflict */
			createfunc_opt_item						{ $$ = list_make1($1); }
			| createfunc_opt_list createfunc_opt_item { $$ = lappend($1, $2); }
	;
opt_createproc_opt_list:
			opt_createproc_opt_list createproc_opt_item
				{
					$$ = lappend($1, $2);
				}
			| /* EMPTY */
				{
					$$ = NIL;
				}
 	 ;

/*
 * Options common to both CREATE FUNCTION and ALTER FUNCTION
 */
common_func_opt_item:
			CALLED ON NULL_P INPUT_P
				{
					$$ = makeDefElem("strict", (Node *)makeInteger(FALSE));
				}
			| RETURNS NULL_P ON NULL_P INPUT_P
				{
					$$ = makeDefElem("strict", (Node *)makeInteger(TRUE));
				}
			| STRICT_P
				{
					$$ = makeDefElem("strict", (Node *)makeInteger(TRUE));
				}
			| IMMUTABLE
				{
					$$ = makeDefElem("volatility", (Node *)makeString("immutable"));
				}
			| STABLE
				{
					$$ = makeDefElem("volatility", (Node *)makeString("stable"));
				}
			| VOLATILE
				{
					$$ = makeDefElem("volatility", (Node *)makeString("volatile"));
				}
			| SHIPPABLE
				{
					$$ = makeDefElem("shippable", (Node*)makeInteger(TRUE));
				}
			| NOT SHIPPABLE
				{
					$$ = makeDefElem("shippable", (Node*)makeInteger(FALSE));
				}
			| EXTERNAL SECURITY DEFINER
				{
					$$ = makeDefElem("security", (Node *)makeInteger(TRUE));
				}
			| EXTERNAL SECURITY INVOKER
				{
					$$ = makeDefElem("security", (Node *)makeInteger(FALSE));
				}
			| SECURITY DEFINER
				{
					$$ = makeDefElem("security", (Node *)makeInteger(TRUE));
				}
			| SECURITY INVOKER
				{
					$$ = makeDefElem("security", (Node *)makeInteger(FALSE));
				}
			| AUTHID DEFINER
				{
					$$ = makeDefElem("security", (Node *)makeInteger(TRUE));
				}
			| AUTHID CURRENT_USER
				{
					$$ = makeDefElem("security", (Node *)makeInteger(FALSE));
				}
			| LEAKPROOF
				{
					$$ = makeDefElem("leakproof", (Node *)makeInteger(TRUE));
				}
			| NOT LEAKPROOF
				{
					$$ = makeDefElem("leakproof", (Node *)makeInteger(FALSE));
				}
			| COST NumericOnly
				{
					$$ = makeDefElem("cost", (Node *)$2);
				}
			| ROWS NumericOnly
				{
					$$ = makeDefElem("rows", (Node *)$2);
				}
			| FunctionSetResetClause
				{
					/* we abuse the normal content of a DefElem here */
					$$ = makeDefElem("set", (Node *)$1);
				}
			| FENCED
				{
					if (IS_SINGLE_NODE)
					{
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("FENCED mode function is not yet supported in current version.")));
					}

					$$ = makeDefElem("fenced", (Node *)makeInteger(TRUE));
				}
			| NOT FENCED
				{
					$$ = makeDefElem("fenced", (Node *)makeInteger(FALSE));
				}
			| PACKAGE
				{
					$$ = makeDefElem("package", (Node *)makeInteger(true));
				}
		;

createfunc_opt_item:
			AS func_as
				{
					$$ = makeDefElem("as", (Node *)$2);
				}
			| LANGUAGE ColId_or_Sconst
				{
					$$ = makeDefElem("language", (Node *)makeString($2));
				}
			| WINDOW
				{
					$$ = makeDefElem("window", (Node *)makeInteger(TRUE));
				}
			| common_func_opt_item
				{
					$$ = $1;
				}
		;


createproc_opt_item:
			 common_func_opt_item
				{
					$$ = $1;
				}
		;

func_as:	Sconst						{ $$ = list_make1(makeString($1)); }
			| Sconst ',' Sconst
				{
					$$ = list_make2(makeString($1), makeString($3));
				}
		;
subprogram_body: 	{
				int		proc_b	= 0;
				int		proc_e	= 0;
				char	*proc_body_str	= NULL;
				int		proc_body_len	= 0;
				int		blocklevel		= 0;
				bool	add_declare		= true;  /* Mark if need to add a DECLARE */

				int	tok = YYEMPTY;
				int	pre_tok = 0;
				base_yy_extra_type *yyextra = pg_yyget_extra(yyscanner);

				yyextra->core_yy_extra.in_slash_proc_body = true;
				/* the token BEGIN_P have been parsed */
				if (u_sess->parser_cxt.eaten_begin)
					blocklevel = 1;

				if (yychar == YYEOF || yychar == YYEMPTY)
					tok = YYLEX;
				else
				{
					tok = yychar;
					yychar = YYEMPTY;
				}

				if (u_sess->parser_cxt.eaten_declare || DECLARE == tok)
					add_declare = false;

				/* Save the beginning of procedure body. */
				proc_b = yylloc;
				while(true)
				{
					if (tok == YYEOF)
						parser_yyerror("subprogram body is not ended correctly");

					if (tok == BEGIN_P)
						blocklevel++;

					/*
					 * End of procedure rules:
					 *	;END [;]
					 * 	| BEGIN END[;]
					 */
					if (tok == END_P)
					{
						tok = YYLEX;

						/* adapt A db's label */
						if (!(tok == ';' || tok == 0)
							&& tok != IF_P
							&& tok != CASE
							&& tok != LOOP)
						{
							tok = END_P;
							continue;
						}

					 	if (blocklevel == 1
							&& (pre_tok == ';' || pre_tok == BEGIN_P)
							&& (tok == ';' || tok == 0))
						{
							/* Save the end of procedure body. */
							proc_e = yylloc;

							if (tok == ';' )
							{
								if (yyextra->lookahead_num != 0)
									parser_yyerror("subprogram body is not ended correctly");
								else
								{
									yyextra->lookahead_token[0] = tok;
									yyextra->lookahead_num = 1;
								}
							}
							break;
						}

						/* Cope with nested BEGIN/END pairs.
						 * In fact the tok can not be 0
						 */
					 	if (blocklevel > 1
							 && (pre_tok == ';' || pre_tok == BEGIN_P)
							 && (tok == ';' || tok == 0))
						{
							blocklevel--;
						}
					}

					pre_tok = tok;
					tok = YYLEX;
				}

				if (proc_e == 0)
					parser_yyerror("subprogram body is not ended correctly");

				proc_body_len = proc_e - proc_b + 1 ;

				/* Add a DECLARE in the start of the subprogram body
				 * 	to compatiable with the A db.
				 * 	XXX : It is best to change the gram.y in plpgsql.
				 */
				if (add_declare)
				{
					proc_body_str = (char *)palloc0(proc_body_len + DECLARE_LEN + 1);
					strncpy(proc_body_str, DECLARE_STR, DECLARE_LEN + 1);
					strncpy(proc_body_str + DECLARE_LEN,
							yyextra->core_yy_extra.scanbuf + proc_b - 1, proc_body_len);
					proc_body_len = DECLARE_LEN + proc_body_len;
				}
				else
				{
					proc_body_str = (char *)palloc0(proc_body_len + 1);
					strncpy(proc_body_str,
						yyextra->core_yy_extra.scanbuf + proc_b - 1, proc_body_len);
				}

				proc_body_str[proc_body_len] = '\0';

				/* Reset the flag which mark whether we are in slash proc. */
				yyextra->core_yy_extra.in_slash_proc_body = false;
				yyextra->core_yy_extra.dolqstart = NULL;

				/*
				 * Add the end location of slash proc to the locationlist for the multi-query 
				 * processed.
				 */
				yyextra->core_yy_extra.query_string_locationlist = 
					lappend_int(yyextra->core_yy_extra.query_string_locationlist, yylloc);

				$$ = proc_body_str;
			}
		;
opt_definition:
			WITH definition							{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

table_func_column:	param_name func_type
				{
					FunctionParameter *n = makeNode(FunctionParameter);
					n->name = $1;
					n->argType = $2;
					n->mode = FUNC_PARAM_TABLE;
					n->defexpr = NULL;
					$$ = n;
				}
		;

table_func_column_list:
			table_func_column
				{
					$$ = list_make1($1);
				}
			| table_func_column_list ',' table_func_column
				{
					$$ = lappend($1, $3);
				}
		;

/*****************************************************************************
 * ALTER FUNCTION
 *
 * RENAME and OWNER subcommands are already provided by the generic
 * ALTER infrastructure, here we just specify alterations that can
 * only be applied to functions.
 *
 *****************************************************************************/
AlterFunctionStmt:
			ALTER FUNCTION function_with_argtypes alterfunc_opt_list opt_restrict
				{
					AlterFunctionStmt *n = makeNode(AlterFunctionStmt);
					n->func = $3;
					n->actions = $4;
					$$ = (Node *) n;
				}
		;

AlterProcedureStmt:
			ALTER PROCEDURE function_with_argtypes alterfunc_opt_list opt_restrict
				{
					AlterFunctionStmt *n = makeNode(AlterFunctionStmt);
					n->func = $3;
					n->actions = $4;
					$$ = (Node *) n;
				}
		;

alterfunc_opt_list:
			/* At least one option must be specified */
			common_func_opt_item					{ $$ = list_make1($1); }
			| alterfunc_opt_list common_func_opt_item { $$ = lappend($1, $2); }
		;

/* Ignored, merely for SQL compliance */
opt_restrict:
			RESTRICT
			| /* EMPTY */
		;


/*****************************************************************************
 *
 *		QUERY:
 *
 *		DROP FUNCTION funcname (arg1, arg2, ...) [ RESTRICT | CASCADE ]
 *		DROP AGGREGATE aggname (arg1, ...) [ RESTRICT | CASCADE ]
 *		DROP OPERATOR opname (leftoperand_typ, rightoperand_typ) [ RESTRICT | CASCADE ]
 *
 *****************************************************************************/

RemoveFuncStmt:
			DROP FUNCTION func_name func_args opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_FUNCTION;
					n->objects = list_make1($3);
					n->arguments = list_make1(extractArgTypes($4));
					n->behavior = $5;
					n->missing_ok = false;
					n->concurrent = false;
					n->isProcedure = false;
					$$ = (Node *)n;
				}
			| DROP FUNCTION IF_P EXISTS func_name func_args opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_FUNCTION;
					n->objects = list_make1($5);
					n->arguments = list_make1(extractArgTypes($6));
					n->behavior = $7;
					n->missing_ok = true;
					n->concurrent = false;
					n->isProcedure = false;
					$$ = (Node *)n;
				}
            | DROP PROCEDURE func_name func_args opt_drop_behavior
                {
                    DropStmt *n = makeNode(DropStmt);
                    n->removeType = OBJECT_FUNCTION;
                    n->objects = list_make1($3);
                    n->arguments = list_make1(extractArgTypes($4));
                    n->behavior = $5; 
                    n->missing_ok = false;
                    n->concurrent = false;
                    n->isProcedure = true;
                    $$ = (Node *)n;
                }     
            | DROP PROCEDURE IF_P EXISTS func_name func_args opt_drop_behavior
                {
                    DropStmt *n = makeNode(DropStmt);
                    n->removeType = OBJECT_FUNCTION;
                    n->objects = list_make1($5);
                    n->arguments = list_make1(extractArgTypes($6));
                    n->behavior = $7; 
                    n->missing_ok = true; 
                    n->concurrent = false;
                    n->isProcedure = true;
                    $$ = (Node *)n;
                } 
			| DROP PROCEDURE func_name_opt_arg
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_FUNCTION;
					n->objects = list_make1($3);
					n->arguments = NULL;
					n->behavior = DROP_RESTRICT;
					n->missing_ok = false;
					n->concurrent = false;
					n->isProcedure = true;
					$$ = (Node *)n;
				}
			| DROP PROCEDURE IF_P EXISTS func_name_opt_arg
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_FUNCTION;
					n->objects = list_make1($5);
					n->arguments = NULL;
					n->behavior = DROP_RESTRICT;
					n->missing_ok = true;
					n->concurrent = false;
					n->isProcedure = true;
					$$ = (Node *)n;
				}
			| DROP FUNCTION func_name_opt_arg
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_FUNCTION;
					n->objects = list_make1($3);
					n->arguments = NULL;
					n->behavior = DROP_RESTRICT;
					n->missing_ok = false;
					n->concurrent = false;
					n->isProcedure = false;
					$$ = (Node *)n;
				}
			| DROP FUNCTION IF_P EXISTS func_name_opt_arg
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_FUNCTION;
					n->objects = list_make1($5);
					n->arguments = NULL;
					n->behavior = DROP_RESTRICT;
					n->missing_ok = true;
					n->concurrent = false;
					n->isProcedure = false;
					$$ = (Node *)n;
				}
		;

RemoveAggrStmt:
			DROP AGGREGATE func_name aggr_args opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_AGGREGATE;
					n->objects = list_make1($3);
					n->arguments = list_make1($4);
					n->behavior = $5;
					n->missing_ok = false;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP AGGREGATE IF_P EXISTS func_name aggr_args opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_AGGREGATE;
					n->objects = list_make1($5);
					n->arguments = list_make1($6);
					n->behavior = $7;
					n->missing_ok = true;
					n->concurrent = false;
					$$ = (Node *)n;
				}
		;

RemoveOperStmt:
			DROP OPERATOR any_operator oper_argtypes opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_OPERATOR;
					n->objects = list_make1($3);
					n->arguments = list_make1($4);
					n->behavior = $5;
					n->missing_ok = false;
					n->concurrent = false;
					$$ = (Node *)n;
				}
			| DROP OPERATOR IF_P EXISTS any_operator oper_argtypes opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_OPERATOR;
					n->objects = list_make1($5);
					n->arguments = list_make1($6);
					n->behavior = $7;
					n->missing_ok = true;
					n->concurrent = false;
					$$ = (Node *)n;
				}
		;

oper_argtypes:
			'(' Typename ')'
				{
				   ereport(ERROR,
						   (errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("missing argument"),
							errhint("Use NONE to denote the missing argument of a unary operator."),
							parser_errposition(@3)));
				}
			| '(' Typename ',' Typename ')'
					{ $$ = list_make2($2, $4); }
			| '(' NONE ',' Typename ')'					/* left unary */
					{ $$ = list_make2(NULL, $4); }
			| '(' Typename ',' NONE ')'					/* right unary */
					{ $$ = list_make2($2, NULL); }
		;

any_operator:
			all_Op
					{ $$ = list_make1(makeString($1)); }
			| ColId '.' any_operator
					{ $$ = lcons(makeString($1), $3); }
		;

/*****************************************************************************
 *
 *		DO <anonymous code block> [ LANGUAGE language ]
 *
 * We use a DefElem list for future extensibility, and to allow flexibility
 * in the clause order.
 *
 *****************************************************************************/

DoStmt: DO dostmt_opt_list
				{
					DoStmt *n = makeNode(DoStmt);
					n->args = $2;
					$$ = (Node *)n;
				}
		;

dostmt_opt_list:
			dostmt_opt_item						{ $$ = list_make1($1); }
			| dostmt_opt_list dostmt_opt_item	{ $$ = lappend($1, $2); }
		;

dostmt_opt_item:
			Sconst
				{
					$$ = makeDefElem("as", (Node *)makeString($1));
				}
			| LANGUAGE ColId_or_Sconst
				{
					$$ = makeDefElem("language", (Node *)makeString($2));
				}
		;

AnonyBlockStmt:
		DECLARE { u_sess->parser_cxt.eaten_declare = true; u_sess->parser_cxt.eaten_begin = false; } subprogram_body
			{
				$$ = (Node *)MakeAnonyBlockFuncStmt(DECLARE, $3);
			}
		| BEGIN_P { u_sess->parser_cxt.eaten_declare = true; u_sess->parser_cxt.eaten_begin = true; } subprogram_body
			{
				$$ = (Node *)MakeAnonyBlockFuncStmt(BEGIN_P, $3);
			}
		;
/*****************************************************************************
 *
 *		CREATE CAST / DROP CAST
 *
 *****************************************************************************/

CreateCastStmt: CREATE CAST '(' Typename AS Typename ')'
					WITH FUNCTION function_with_argtypes cast_context
				{
					CreateCastStmt *n = makeNode(CreateCastStmt);
					n->sourcetype = $4;
					n->targettype = $6;
					n->func = $10;
					n->context = (CoercionContext) $11;
					n->inout = false;
					$$ = (Node *)n;
				}
			| CREATE CAST '(' Typename AS Typename ')'
					WITHOUT FUNCTION cast_context
				{
					CreateCastStmt *n = makeNode(CreateCastStmt);
					n->sourcetype = $4;
					n->targettype = $6;
					n->func = NULL;
					n->context = (CoercionContext) $10;
					n->inout = false;
					$$ = (Node *)n;
				}
			| CREATE CAST '(' Typename AS Typename ')'
					WITH INOUT cast_context
				{
					CreateCastStmt *n = makeNode(CreateCastStmt);
					n->sourcetype = $4;
					n->targettype = $6;
					n->func = NULL;
					n->context = (CoercionContext) $10;
					n->inout = true;
					$$ = (Node *)n;
				}
		;

cast_context:  AS IMPLICIT_P					{ $$ = COERCION_IMPLICIT; }
		| AS ASSIGNMENT							{ $$ = COERCION_ASSIGNMENT; }
		| /*EMPTY*/								{ $$ = COERCION_EXPLICIT; }
		;


DropCastStmt: DROP CAST opt_if_exists '(' Typename AS Typename ')' opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_CAST;
					n->objects = list_make1(list_make1($5));
					n->arguments = list_make1(list_make1($7));
					n->behavior = $9;
					n->missing_ok = $3;
					n->concurrent = false;
					$$ = (Node *)n;
				}
		;

opt_if_exists: IF_P EXISTS						{ $$ = TRUE; }
		| /*EMPTY*/								{ $$ = FALSE; }
		;


/*****************************************************************************
 *
 *		QUERY:
 *
 *		REINDEX type <name> [FORCE]
 *
 * FORCE no longer does anything, but we accept it for backwards compatibility
 *****************************************************************************/

ReindexStmt:
			REINDEX reindex_type qualified_name opt_force
				{
					ReindexStmt *n = makeNode(ReindexStmt);
					n->kind = $2;
					n->relation = $3;
					n->name = NULL;
					$$ = (Node *)n;
				}
			|
			REINDEX reindex_type qualified_name PARTITION ColId opt_force
				{
					ReindexStmt *n = makeNode(ReindexStmt);
					if ($2 == OBJECT_INDEX)
						n->kind  = OBJECT_INDEX_PARTITION;
					else if($2 == OBJECT_TABLE)
						n->kind  = OBJECT_TABLE_PARTITION;
					else
						n->kind  = OBJECT_INTERNAL_PARTITION;
					n->relation = $3;
					n->name = $5;
					$$ = (Node *)n;
				}
			| REINDEX SYSTEM_P name opt_force
				{
					ReindexStmt *n = makeNode(ReindexStmt);
					n->kind = OBJECT_DATABASE;
					n->name = $3;
					n->relation = NULL;
					n->do_system = true;
					n->do_user = false;
					$$ = (Node *)n;
				}
			| REINDEX DATABASE name opt_force
				{
					ReindexStmt *n = makeNode(ReindexStmt);
					n->kind = OBJECT_DATABASE;
					n->name = $3;
					n->relation = NULL;
					n->do_system = true;
					n->do_user = true;
					$$ = (Node *)n;
				}
		;

reindex_type:
			INDEX									{ $$ = OBJECT_INDEX; }
			| TABLE									{ $$ = OBJECT_TABLE; }
			| INTERNAL TABLE						{ $$ = OBJECT_INTERNAL; }
		;

opt_force:	FORCE									{  $$ = TRUE; }
			| /* EMPTY */							{  $$ = FALSE; }
		;


/*****************************************************************************
 *
 * ALTER THING name RENAME TO newname
 *
 *****************************************************************************/

RenameStmt: ALTER AGGREGATE func_name aggr_args RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_AGGREGATE;
					n->object = $3;
					n->objarg = $4;
					n->newname = $7;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER COLLATION any_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_COLLATION;
					n->object = $3;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER CONVERSION_P any_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_CONVERSION;
					n->object = $3;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER DATABASE database_name RENAME TO database_name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_DATABASE;
					n->subname = $3;
					IsValidIdent($6);
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER DATA_P SOURCE_P name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_DATA_SOURCE;
					n->subname = $4;
					n->newname = $7;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER DOMAIN_P any_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_DOMAIN;
					n->object = $3;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER DOMAIN_P any_name RENAME CONSTRAINT name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_CONSTRAINT;
					n->relationType = OBJECT_DOMAIN;
					n->object = $3;
					n->subname = $6;
					n->newname = $8;
					$$ = (Node *)n;
				}
			| ALTER FOREIGN DATA_P WRAPPER name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_FDW;
					n->subname = $5;
					n->newname = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER FUNCTION function_with_argtypes RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_FUNCTION;
					n->object = $3->funcname;
					n->objarg = $3->funcargs;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER PROCEDURE function_with_argtypes RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_FUNCTION;
					n->object = $3->funcname;
					n->objarg = $3->funcargs;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER GROUP_P RoleId RENAME TO RoleId
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_ROLE;
					n->subname = $3;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER opt_procedural LANGUAGE name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_LANGUAGE;
					n->subname = $4;
					n->newname = $7;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER OPERATOR CLASS any_name USING access_method RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_OPCLASS;
					n->object = $4;
					n->subname = $6;
					n->newname = $9;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER OPERATOR FAMILY any_name USING access_method RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_OPFAMILY;
					n->object = $4;
					n->subname = $6;
					n->newname = $9;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			/* Rename Row Level Security Policy */
			| ALTER RowLevelSecurityPolicyName ON qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_RLSPOLICY;
					n->subname = $2;
					n->relation = $4;
					n->newname = $7;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER SCHEMA name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_SCHEMA;
					n->subname = $3;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER SERVER name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_FOREIGN_SERVER;
					n->subname = $3;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER MATERIALIZED VIEW qualified_name RENAME opt_column name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_COLUMN;
					n->relationType = OBJECT_MATVIEW;
					n->relation = $4;
					n->subname = $7;
					n->newname = $9;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER MATERIALIZED VIEW IF_P EXISTS qualified_name RENAME opt_column name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_COLUMN;
					n->relationType = OBJECT_MATVIEW;
					n->relation = $6;
					n->subname = $9;
					n->newname = $11;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER TABLE relation_expr RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TABLE;
					n->relation = $3;
					n->subname = NULL;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TABLE IF_P EXISTS relation_expr RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TABLE;
					n->relation = $5;
					n->subname = NULL;
					n->newname = $8;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER SEQUENCE qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_SEQUENCE;
					n->relation = $3;
					n->subname = NULL;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER SEQUENCE IF_P EXISTS qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_SEQUENCE;
					n->relation = $5;
					n->subname = NULL;
					n->newname = $8;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER VIEW qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_VIEW;
					n->relation = $3;
					n->subname = NULL;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER VIEW IF_P EXISTS qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_VIEW;
					n->relation = $5;
					n->subname = NULL;
					n->newname = $8;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER MATERIALIZED VIEW qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_MATVIEW;
					n->relation = $4;
					n->subname = NULL;
					n->newname = $7;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER MATERIALIZED VIEW IF_P EXISTS qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_MATVIEW;
					n->relation = $6;
					n->subname = NULL;
					n->newname = $9;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER INDEX qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_INDEX;
					n->relation = $3;
					n->subname = NULL;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER INDEX IF_P EXISTS qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_INDEX;
					n->relation = $5;
					n->subname = NULL;
					n->newname = $8;
					n->missing_ok = true;
					$$ = (Node *)n;
				}

			| ALTER INDEX qualified_name RENAME_PARTITION name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_PARTITION_INDEX;
					n->relation = $3;
					n->subname = $5;
					n->newname = $7;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER INDEX IF_P EXISTS qualified_name RENAME_PARTITION name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_PARTITION_INDEX;
					n->relation = $5;
					n->subname = $7;
					n->newname = $9;
					n->missing_ok = true;
					$$ = (Node *)n;
				}

			| ALTER FOREIGN TABLE relation_expr RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_FOREIGN_TABLE;
					n->relation = $4;
					n->subname = NULL;
					n->newname = $7;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER FOREIGN TABLE IF_P EXISTS relation_expr RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_FOREIGN_TABLE;
					n->relation = $6;
					n->subname = NULL;
					n->newname = $9;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER TABLE relation_expr RENAME opt_column name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_COLUMN;
					n->relationType = OBJECT_TABLE;
					n->relation = $3;
					n->subname = $6;
					n->newname = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TABLE IF_P EXISTS relation_expr RENAME opt_column name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_COLUMN;
					n->relationType = OBJECT_TABLE;
					n->relation = $5;
					n->subname = $8;
					n->newname = $10;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER TABLE relation_expr RENAME CONSTRAINT name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_CONSTRAINT;
					n->relationType = OBJECT_TABLE;
					n->relation = $3;
					n->subname = $6;
					n->newname = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}

			| ALTER TABLE IF_P EXISTS relation_expr RENAME CONSTRAINT name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_CONSTRAINT;
					n->relationType = OBJECT_TABLE;
					n->relation = $5;
					n->subname = $8;
					n->newname = $10;
					n->missing_ok = true;
					$$ = (Node *)n;
				}

			| ALTER TABLE relation_expr RENAME_PARTITION name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_PARTITION;
					n->relationType = OBJECT_TABLE;
					n->relation = $3;
					n->subname = $5;
					n->newname = $7;
					n->missing_ok = false;
					$$ = (Node *)n;
				}

			| ALTER TABLE IF_P EXISTS relation_expr RENAME_PARTITION name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_PARTITION;
					n->relationType = OBJECT_TABLE;
					n->relation = $5;
					n->subname = $7;
					n->newname = $9;
					n->missing_ok = true;
					$$ = (Node *)n;
				}

			| ALTER TABLE relation_expr RENAME_PARTITION FOR '(' maxValueList ')' TO name
			    {
			        RenameStmt *n = makeNode(RenameStmt);
			        n->renameType = OBJECT_PARTITION;
					n->relationType = OBJECT_TABLE;
			        n->relation = $3;
			        n->object = $7;
			        n->subname = NULL;
			        n->newname = $10;
			        n->missing_ok = false;
			        $$ = (Node *)n;
			    }

			| ALTER TABLE IF_P EXISTS relation_expr RENAME_PARTITION FOR '(' maxValueList ')' TO name
			    {
			        RenameStmt *n = makeNode(RenameStmt);
			        n->renameType = OBJECT_PARTITION;
					n->relationType = OBJECT_TABLE;
			        n->relation = $5;
			        n->object = $9;
			        n->subname = NULL;
			        n->newname = $12;
			        n->missing_ok = true;
			        $$ = (Node *)n;
			    }

			| ALTER FOREIGN TABLE relation_expr RENAME opt_column name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_COLUMN;
					n->relationType = OBJECT_FOREIGN_TABLE;
					n->relation = $4;
					n->subname = $7;
					n->newname = $9;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER FOREIGN TABLE IF_P EXISTS relation_expr RENAME opt_column name TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_COLUMN;
					n->relationType = OBJECT_FOREIGN_TABLE;
					n->relation = $6;
					n->subname = $9;
					n->newname = $11;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER TRIGGER name ON qualified_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TRIGGER;
					n->relation = $5;
					n->subname = $3;
					n->newname = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER ROLE RoleId RENAME TO RoleId
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_ROLE;
					n->subname = $3;
					IsValidIdent($6);
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER USER RoleId RENAME TO RoleId
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_USER;
					n->subname = $3;
					IsValidIdent($6);
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TABLESPACE name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TABLESPACE;
					n->subname = $3;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TABLESPACE name SET reloptions
				{
					AlterTableSpaceOptionsStmt *n =
						makeNode(AlterTableSpaceOptionsStmt);
					n->tablespacename = $3;
					n->options = $5;
					n->isReset = FALSE;
					$$ = (Node *)n;
				}
			| ALTER TABLESPACE name RESET reloptions
				{
					AlterTableSpaceOptionsStmt *n =
						makeNode(AlterTableSpaceOptionsStmt);
					n->tablespacename = $3;
					n->options = $5;
					n->isReset = TRUE;
					$$ = (Node *)n;
				}
			| ALTER TABLESPACE name RESIZE MAXSIZE size_clause
				{
					AlterTableSpaceOptionsStmt *n =
						makeNode(AlterTableSpaceOptionsStmt);
					n->tablespacename = $3;
					n->options = NIL;
					n->isReset = FALSE;
					n->maxsize = $6;
					$$ = (Node *)n;
				}
			| ALTER TEXT_P SEARCH PARSER any_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TSPARSER;
					n->object = $5;
					n->newname = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TEXT_P SEARCH DICTIONARY any_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TSDICTIONARY;
					n->object = $5;
					n->newname = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TEXT_P SEARCH TEMPLATE any_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TSTEMPLATE;
					n->object = $5;
					n->newname = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TEXT_P SEARCH CONFIGURATION any_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TSCONFIGURATION;
					n->object = $5;
					n->newname = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TYPE_P any_name RENAME TO name
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_TYPE;
					n->object = $3;
					n->newname = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TYPE_P any_name RENAME ATTRIBUTE name TO name opt_drop_behavior
				{
					RenameStmt *n = makeNode(RenameStmt);
					n->renameType = OBJECT_ATTRIBUTE;
					n->relationType = OBJECT_TYPE;
					n->relation = makeRangeVarFromAnyName($3, @3, yyscanner);
					n->subname = $6;
					n->newname = $8;
					n->behavior = $9;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
		;

opt_column: COLUMN									{ $$ = COLUMN; }
			| /*EMPTY*/								{ $$ = 0; }
		;

opt_set_data: SET DATA_P							{ $$ = 1; }
			| /*EMPTY*/								{ $$ = 0; }
		;

/*****************************************************************************
 *
 * ALTER THING name SET SCHEMA name
 *
 *****************************************************************************/

AlterObjectSchemaStmt:
			ALTER AGGREGATE func_name aggr_args SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_AGGREGATE;
					n->object = $3;
					n->objarg = $4;
					n->newschema = $7;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER COLLATION any_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_COLLATION;
					n->object = $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER CONVERSION_P any_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_CONVERSION;
					n->object = $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER DOMAIN_P any_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_DOMAIN;
					n->object = $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER EXTENSION any_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_EXTENSION;
					n->object = $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER FUNCTION function_with_argtypes SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_FUNCTION;
					n->object = $3->funcname;
					n->objarg = $3->funcargs;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER PROCEDURE function_with_argtypes SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_FUNCTION;
					n->object = $3->funcname;
					n->objarg = $3->funcargs;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER OPERATOR any_operator oper_argtypes SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_OPERATOR;
					n->object = $3;
					n->objarg = $4;
					n->newschema = $7;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER OPERATOR CLASS any_name USING access_method SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_OPCLASS;
					n->object = $4;
					n->addname = $6;
					n->newschema = $9;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER OPERATOR FAMILY any_name USING access_method SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_OPFAMILY;
					n->object = $4;
					n->addname = $6;
					n->newschema = $9;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TABLE relation_expr SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_TABLE;
					n->relation = $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TABLE IF_P EXISTS relation_expr SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_TABLE;
					n->relation = $5;
					n->newschema = $8;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER TEXT_P SEARCH PARSER any_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_TSPARSER;
					n->object = $5;
					n->newschema = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TEXT_P SEARCH DICTIONARY any_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_TSDICTIONARY;
					n->object = $5;
					n->newschema = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TEXT_P SEARCH TEMPLATE any_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_TSTEMPLATE;
					n->object = $5;
					n->newschema = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER TEXT_P SEARCH CONFIGURATION any_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_TSCONFIGURATION;
					n->object = $5;
					n->newschema = $8;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER SEQUENCE qualified_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_SEQUENCE;
					n->relation = $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER SEQUENCE IF_P EXISTS qualified_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_SEQUENCE;
					n->relation = $5;
					n->newschema = $8;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER VIEW qualified_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_VIEW;
					n->relation = $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER VIEW IF_P EXISTS qualified_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_VIEW;
					n->relation = $5;
					n->newschema = $8;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER MATERIALIZED VIEW qualified_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_MATVIEW;
					n->relation = $4;
					n->newschema = $7;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER MATERIALIZED VIEW IF_P EXISTS qualified_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_MATVIEW;
					n->relation = $6;
					n->newschema = $9;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER FOREIGN TABLE relation_expr SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_FOREIGN_TABLE;
					n->relation = $4;
					n->newschema = $7;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			| ALTER FOREIGN TABLE IF_P EXISTS relation_expr SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_FOREIGN_TABLE;
					n->relation = $6;
					n->newschema = $9;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			| ALTER TYPE_P any_name SET SCHEMA name
				{
					AlterObjectSchemaStmt *n = makeNode(AlterObjectSchemaStmt);
					n->objectType = OBJECT_TYPE;
					n->object = $3;
					n->newschema = $6;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 * ALTER THING name OWNER TO newname
 *
 *****************************************************************************/

AlterOwnerStmt: ALTER AGGREGATE func_name aggr_args OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_AGGREGATE;
					n->object = $3;
					n->objarg = $4;
					n->newowner = $7;
					$$ = (Node *)n;
				}
			| ALTER COLLATION any_name OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_COLLATION;
					n->object = $3;
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER CONVERSION_P any_name OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_CONVERSION;
					n->object = $3;
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER DATABASE database_name OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_DATABASE;
					n->object = list_make1(makeString($3));
					n->newowner = $6;
					$$ = (Node *)n;
				}

			| ALTER DIRECTORY name OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_DIRECTORY;
					n->object = list_make1(makeString($3));
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER DOMAIN_P any_name OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_DOMAIN;
					n->object = $3;
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER FUNCTION function_with_argtypes OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_FUNCTION;
					n->object = $3->funcname;
					n->objarg = $3->funcargs;
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER PROCEDURE function_with_argtypes OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_FUNCTION;
					n->object = $3->funcname;
					n->objarg = $3->funcargs;
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER opt_procedural LANGUAGE name OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_LANGUAGE;
					n->object = list_make1(makeString($4));
					n->newowner = $7;
					$$ = (Node *)n;
				}
			| ALTER LARGE_P OBJECT_P NumericOnly OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_LARGEOBJECT;
					n->object = list_make1($4);
					n->newowner = $7;
					$$ = (Node *)n;
				}
			| ALTER OPERATOR any_operator oper_argtypes OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_OPERATOR;
					n->object = $3;
					n->objarg = $4;
					n->newowner = $7;
					$$ = (Node *)n;
				}
			| ALTER OPERATOR CLASS any_name USING access_method OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_OPCLASS;
					n->object = $4;
					n->addname = $6;
					n->newowner = $9;
					$$ = (Node *)n;
				}
			| ALTER OPERATOR FAMILY any_name USING access_method OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_OPFAMILY;
					n->object = $4;
					n->addname = $6;
					n->newowner = $9;
					$$ = (Node *)n;
				}
			| ALTER SCHEMA name OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_SCHEMA;
					n->object = list_make1(makeString($3));
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER TYPE_P any_name OWNER TO TypeOwner
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_TYPE;
					n->object = $3;
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER TABLESPACE name OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_TABLESPACE;
					n->object = list_make1(makeString($3));
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER TEXT_P SEARCH DICTIONARY any_name OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_TSDICTIONARY;
					n->object = $5;
					n->newowner = $8;
					$$ = (Node *)n;
				}
			| ALTER TEXT_P SEARCH CONFIGURATION any_name OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_TSCONFIGURATION;
					n->object = $5;
					n->newowner = $8;
					$$ = (Node *)n;
				}
			| ALTER FOREIGN DATA_P WRAPPER name OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_FDW;
					n->object = list_make1(makeString($5));
					n->newowner = $8;
					$$ = (Node *)n;
				}
			| ALTER SERVER name OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_FOREIGN_SERVER;
					n->object = list_make1(makeString($3));
					n->newowner = $6;
					$$ = (Node *)n;
				}
			| ALTER DATA_P SOURCE_P name OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_DATA_SOURCE;
					n->object = list_make1(makeString($4));
					n->newowner = $7;
					$$ = (Node *)n;
				}
			| ALTER SYNONYM any_name OWNER TO RoleId
				{
					AlterOwnerStmt *n = makeNode(AlterOwnerStmt);
					n->objectType = OBJECT_SYNONYM;
					n->object = $3;
					n->newowner = $6;
					$$ = (Node *)n;
				}
		;

TypeOwner:	RoleId			{ $$ = $1; }
			| CURRENT_USER	{ $$ = pstrdup($1); }
			| SESSION_USER	{ $$ = pstrdup($1); }
		;

/*****************************************************************************
 *
 *		QUERY:	Define Rewrite Rule
 *
 *****************************************************************************/

RuleStmt:	CREATE opt_or_replace RULE name AS
			ON event TO qualified_name where_clause
			DO opt_instead RuleActionList
				{
					RuleStmt *n = makeNode(RuleStmt);
					n->replace = $2;
					n->relation = $9;
					n->rulename = $4;
					n->whereClause = $10;
					n->event = (CmdType)$7;
					n->instead = $12;
					n->actions = $13;
					$$ = (Node *)n;
				}
		;

RuleActionList:
			NOTHING									{ $$ = NIL; }
			| RuleActionStmt						{ $$ = list_make1($1); }
			| '(' RuleActionMulti ')'				{ $$ = $2; }
		;

/* the thrashing around here is to discard "empty" statements... */
RuleActionMulti:
			RuleActionMulti ';' RuleActionStmtOrEmpty
				{ if ($3 != NULL)
					$$ = lappend($1, $3);
				  else
					$$ = $1;
				}
			| RuleActionStmtOrEmpty
				{ if ($1 != NULL)
					$$ = list_make1($1);
				  else
					$$ = NIL;
				}
		;

RuleActionStmt:
			SelectStmt
			| InsertStmt
			| UpdateStmt
			| DeleteStmt
			| CopyStmt
			| NotifyStmt
            | AlterTableStmt
		;
RuleActionStmtOrEmpty:
			RuleActionStmt							{ $$ = $1; }
			|	/*EMPTY*/							{ $$ = NULL; }
		;

event:		SELECT									{ $$ = CMD_SELECT; }
			| UPDATE								{ $$ = CMD_UPDATE; }
			| DELETE_P								{ $$ = CMD_DELETE; }
			| INSERT								{ $$ = CMD_INSERT; }
			| COPY									{ $$ = CMD_UTILITY; }
			| ALTER									{ $$ = CMD_UTILITY; }                           
		 ;

opt_instead:
			INSTEAD									{ $$ = TRUE; }
			| ALSO									{ $$ = FALSE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;


DropRuleStmt:
			DROP RULE name ON any_name opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_RULE;
					n->objects = list_make1(lappend($5, makeString($3)));
					n->arguments = NIL;
					n->behavior = $6;
					n->missing_ok = false;
					n->concurrent = false;
					$$ = (Node *) n;
				}
			| DROP RULE IF_P EXISTS name ON any_name opt_drop_behavior
				{
					DropStmt *n = makeNode(DropStmt);
					n->removeType = OBJECT_RULE;
					n->objects = list_make1(lappend($7, makeString($5)));
					n->arguments = NIL;
					n->behavior = $8;
					n->missing_ok = true;
					n->concurrent = false;
					$$ = (Node *) n;
				}
		;


/*****************************************************************************
 *
 *		QUERY:
 *				NOTIFY <identifier> can appear both in rule bodies and
 *				as a query-level command
 *
 *****************************************************************************/

NotifyStmt: NOTIFY ColId notify_payload
				{
					NotifyStmt *n = makeNode(NotifyStmt);
					n->conditionname = $2;
					n->payload = $3;
					$$ = (Node *)n;
				}
		;

notify_payload:
			',' Sconst							{ $$ = $2; }
			| /*EMPTY*/							{ $$ = NULL; }
		;

ListenStmt: LISTEN ColId
				{
					ListenStmt *n = makeNode(ListenStmt);
					n->conditionname = $2;
					$$ = (Node *)n;
				}
		;

UnlistenStmt:
			UNLISTEN ColId
				{
					UnlistenStmt *n = makeNode(UnlistenStmt);
					n->conditionname = $2;
					$$ = (Node *)n;
				}
			| UNLISTEN '*'
				{
					UnlistenStmt *n = makeNode(UnlistenStmt);
					n->conditionname = NULL;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 *		Transactions:
 *
 *		BEGIN / COMMIT / ROLLBACK
 *		(also older versions END / ABORT)
 *
 *****************************************************************************/

TransactionStmt:
			ABORT_P opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| START TRANSACTION transaction_mode_list_or_empty
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_START;
					n->options = $3;
					$$ = (Node *)n;
				}
			| BEGIN_NON_ANOYBLOCK opt_transaction transaction_mode_list_or_empty
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_BEGIN;
					n->options = $3;
					$$ = (Node *)n;
				}
			| COMMIT opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| END_P opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| ROLLBACK opt_transaction
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK;
					n->options = NIL;
					$$ = (Node *)n;
				}
			| SAVEPOINT ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_SAVEPOINT;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($2)));
					$$ = (Node *)n;
				}
			| RELEASE SAVEPOINT ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_RELEASE;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($3)));
					$$ = (Node *)n;
				}
			| RELEASE ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_RELEASE;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($2)));
					$$ = (Node *)n;
				}
			| ROLLBACK opt_transaction TO SAVEPOINT ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK_TO;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($5)));
					$$ = (Node *)n;
				}
			| ROLLBACK opt_transaction TO ColId
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK_TO;
					n->options = list_make1(makeDefElem("savepoint_name",
														(Node *)makeString($4)));
					$$ = (Node *)n;
				}
			| PREPARE TRANSACTION Sconst
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_PREPARE;
					n->gid = $3;
					$$ = (Node *)n;
				}
			| COMMIT PREPARED Sconst
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT_PREPARED;
					n->gid = $3;
					n->csn = InvalidCommitSeqNo;
					$$ = (Node *)n;
				}
			| COMMIT PREPARED Sconst WITH Sconst
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_COMMIT_PREPARED;
					n->gid = $3;
					n->csn = strtoull($5, NULL, 10);;
					$$ = (Node *)n;
				}
			| ROLLBACK PREPARED Sconst
				{
					TransactionStmt *n = makeNode(TransactionStmt);
					n->kind = TRANS_STMT_ROLLBACK_PREPARED;
					n->gid = $3;
					$$ = (Node *)n;
				}
		;

opt_transaction:	WORK							{}
			| TRANSACTION							{}
			| /*EMPTY*/								{}
		;

transaction_mode_item:
			ISOLATION LEVEL iso_level
					{ $$ = makeDefElem("transaction_isolation",
									   makeStringConst($3, @3)); }
			| READ ONLY
					{ $$ = makeDefElem("transaction_read_only",
									   makeIntConst(TRUE, @1)); }
			| READ WRITE
					{ $$ = makeDefElem("transaction_read_only",
									   makeIntConst(FALSE, @1)); }
			| DEFERRABLE
					{ $$ = makeDefElem("transaction_deferrable",
									   makeIntConst(TRUE, @1)); }
			| NOT DEFERRABLE
					{ $$ = makeDefElem("transaction_deferrable",
									   makeIntConst(FALSE, @1)); }
		;

/* Syntax with commas is SQL-spec, without commas is Postgres historical */
transaction_mode_list:
			transaction_mode_item
					{ $$ = list_make1($1); }
			| transaction_mode_list ',' transaction_mode_item
					{ $$ = lappend($1, $3); }
			| transaction_mode_list transaction_mode_item
					{ $$ = lappend($1, $2); }
		;

transaction_mode_list_or_empty:
			transaction_mode_list
			| /* EMPTY */
					{ $$ = NIL; }
		;


/*****************************************************************************
 *
 *	CONTVIEW:
 *		CREATE [ OR REPLACE ] CONTVIEW <contquery_name> [WITH ([,...])]
 *			AS <query> 
 *
 *****************************************************************************/

CreateContQueryStmt: CREATE CONTVIEW qualified_name opt_reloptions AS SelectStmt
				{
					ViewStmt *n = makeNode(ViewStmt);
					n->view = $3;
					n->relkind = OBJECT_CONTQUERY;
					n->view->relpersistence = RELPERSISTENCE_PERMANENT;
					n->aliases = NIL;
					n->query = $6;
					n->replace = false;
					n->options = $4;
                    n->sql_statement = NULL;
					$$ = (Node *) n;
                }
		    | CREATE OR REPLACE CONTVIEW qualified_name opt_reloptions AS SelectStmt
				{
					ViewStmt *n = makeNode(ViewStmt);
					n->view = $5;
					n->relkind = OBJECT_CONTQUERY;
					n->view->relpersistence = RELPERSISTENCE_PERMANENT;
					n->aliases = NIL;
					n->query = $8;
					n->replace = true;
					n->options = $6;
                    n->sql_statement = NULL;
					$$ = (Node *) n;
				}
            ;

/*****************************************************************************
 *
 *	QUERY:
 *		CREATE [ OR REPLACE ] [ TEMP ] VIEW <viewname> '('target-list ')'
 *			AS <query> [ WITH [ CASCADED | LOCAL ] CHECK OPTION ]
 *
 *****************************************************************************/

ViewStmt: CREATE OptTemp VIEW qualified_name opt_column_list opt_reloptions
				AS SelectStmt opt_check_option
				{
					ViewStmt *n = makeNode(ViewStmt);
					n->view = $4;
					n->view->relpersistence = $2;
					n->aliases = $5;
					n->query = $8;
					n->replace = false;
					n->options = $6;
                                                                                n->sql_statement = NULL;
					$$ = (Node *) n;
				}
		| CREATE OR REPLACE OptTemp VIEW qualified_name opt_column_list opt_reloptions
				AS SelectStmt opt_check_option
				{
					ViewStmt *n = makeNode(ViewStmt);
					n->view = $6;
					n->view->relpersistence = $4;
					n->aliases = $7;
					n->query = $10;
					n->replace = true;
					n->options = $8;
                                                                                n->sql_statement = NULL;
					$$ = (Node *) n;
				}
		;

opt_check_option:
		WITH CHECK OPTION
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("WITH CHECK OPTION is not implemented")));
				}
		| WITH CASCADED CHECK OPTION
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("WITH CHECK OPTION is not implemented")));
				}
		| WITH LOCAL CHECK OPTION
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("WITH CHECK OPTION is not implemented")));
				}
		| /* EMPTY */							{ $$ = NIL; }
		;

/*****************************************************************************
 *
 *		QUERY:
 *				LOAD "filename"
 *
 *****************************************************************************/

LoadStmt:	LOAD file_name
				{
					LoadStmt *n = makeNode(LoadStmt);
					n->filename = $2;
					$$ = (Node *)n;
				}
		;


/*****************************************************************************
 *
 *		CREATE DATABASE
 *
 *****************************************************************************/

CreatedbStmt:
			CREATE DATABASE database_name opt_with createdb_opt_list
				{
					CreatedbStmt *n = makeNode(CreatedbStmt);
					IsValidIdent($3);
					n->dbname = $3;
					n->options = $5;
					$$ = (Node *)n;
				}
		;

createdb_opt_list:
			createdb_opt_list createdb_opt_item		{ $$ = lappend($1, $2); }
			| /* EMPTY */							{ $$ = NIL; }
		;

createdb_opt_item:
			TABLESPACE opt_equal name
				{
					$$ = makeDefElem("tablespace", (Node *)makeString($3));
				}
			| TABLESPACE opt_equal DEFAULT
				{
					$$ = makeDefElem("tablespace", NULL);
				}
			| LOCATION opt_equal Sconst
				{
					$$ = makeDefElem("location", (Node *)makeString($3));
				}
			| LOCATION opt_equal DEFAULT
				{
					$$ = makeDefElem("location", NULL);
				}
			| TEMPLATE opt_equal name
				{
					$$ = makeDefElem("template", (Node *)makeString($3));
				}
			| TEMPLATE opt_equal DEFAULT
				{
					$$ = makeDefElem("template", NULL);
				}
			| ENCODING opt_equal Sconst
				{
					$$ = makeDefElem("encoding", (Node *)makeString($3));
				}
			| ENCODING opt_equal Iconst
				{
					$$ = makeDefElem("encoding", (Node *)makeInteger($3));
				}
			| ENCODING opt_equal DEFAULT
				{
					$$ = makeDefElem("encoding", NULL);
				}
			| LC_COLLATE_P opt_equal Sconst
				{
					$$ = makeDefElem("lc_collate", (Node *)makeString($3));
				}
			| LC_COLLATE_P opt_equal DEFAULT
				{
					$$ = makeDefElem("lc_collate", NULL);
				}
			| DBCOMPATIBILITY_P opt_equal Sconst
				{
					if (checkCompArgs($3) == false)
					{
						ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("Compatibility args %s is invalid\n", $3)));
					}
					$$ = makeDefElem("dbcompatibility", (Node *)makeString($3));
				}
			| DBCOMPATIBILITY_P opt_equal DEFAULT
				{
					$$ = makeDefElem("dbcompatibility", NULL);
				}
			| LC_CTYPE_P opt_equal Sconst
				{
					$$ = makeDefElem("lc_ctype", (Node *)makeString($3));
				}
			| LC_CTYPE_P opt_equal DEFAULT
				{
					$$ = makeDefElem("lc_ctype", NULL);
				}
			| CONNECTION LIMIT opt_equal SignedIconst
				{
					$$ = makeDefElem("connectionlimit", (Node *)makeInteger($4));
				}
			| OWNER opt_equal name
				{
					$$ = makeDefElem("owner", (Node *)makeString($3));
				}
			| OWNER opt_equal DEFAULT
				{
					$$ = makeDefElem("owner", NULL);
				}
		;

/*
 *	Though the equals sign doesn't match other WITH options, pg_dump uses
 *	equals for backward compatibility, and it doesn't seem worth removing it.
 */
opt_equal:	'='										{}
			| /*EMPTY*/								{}
		;


/*****************************************************************************
 *
 *		ALTER DATABASE
 *
 *****************************************************************************/

AlterDatabaseStmt:
			ALTER DATABASE database_name opt_with alterdb_opt_list
				 {
					AlterDatabaseStmt *n = makeNode(AlterDatabaseStmt);
					n->dbname = $3;
					n->options = $5;
					$$ = (Node *)n;
				 }
			| ALTER DATABASE database_name SET TABLESPACE name
				 {
					AlterDatabaseStmt *n = makeNode(AlterDatabaseStmt);
					n->dbname = $3;
					n->options = list_make1(makeDefElem("tablespace",
													(Node *)makeString($6)));
					$$ = (Node *)n;
				 }
		;

AlterDatabaseSetStmt:
			ALTER DATABASE database_name SetResetClause
				{
					AlterDatabaseSetStmt *n = makeNode(AlterDatabaseSetStmt);
					n->dbname = $3;
					n->setstmt = $4;
					$$ = (Node *)n;
				}
		;


alterdb_opt_list:
			alterdb_opt_list alterdb_opt_item		{ $$ = lappend($1, $2); }
			| /* EMPTY */							{ $$ = NIL; }
		;

alterdb_opt_item:
			CONNECTION LIMIT opt_equal SignedIconst
				{
					$$ = makeDefElem("connectionlimit", (Node *)makeInteger($4));
				}
			| ENABLE_P PRIVATE OBJECT_P
				{
					$$ = makeDefElem("privateobject", (Node *)makeInteger(1));
				}
			| DISABLE_P PRIVATE OBJECT_P
				{
					$$ = makeDefElem("privateobject", (Node *)makeInteger(0));
				}
		;


/*****************************************************************************
 *
 *		DROP DATABASE [ IF EXISTS ]
 *
 * This is implicitly CASCADE, no need for drop behavior
 *****************************************************************************/

DropdbStmt: DROP DATABASE database_name
				{
					DropdbStmt *n = makeNode(DropdbStmt);
					n->dbname = $3;
					n->missing_ok = FALSE;
					$$ = (Node *)n;
				}
			| DROP DATABASE IF_P EXISTS database_name
				{
					DropdbStmt *n = makeNode(DropdbStmt);
					n->dbname = $5;
					n->missing_ok = TRUE;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 * Manipulate a domain
 *
 *****************************************************************************/

CreateDomainStmt:
			CREATE DOMAIN_P any_name opt_as Typename ColQualList
				{
					CreateDomainStmt *n = makeNode(CreateDomainStmt);
					n->domainname = $3;
					n->typname = $5;
					SplitColQualList($6, &n->constraints, &n->collClause,
									 yyscanner);
					$$ = (Node *)n;
				}
		;

AlterDomainStmt:
			/* ALTER DOMAIN <domain> {SET DEFAULT <expr>|DROP DEFAULT} */
			ALTER DOMAIN_P any_name alter_column_default
				{
					AlterDomainStmt *n = makeNode(AlterDomainStmt);
					n->subtype = 'T';
					n->typname = $3;
					n->def = $4;
					$$ = (Node *)n;
				}
			/* ALTER DOMAIN <domain> DROP NOT NULL */
			| ALTER DOMAIN_P any_name DROP NOT NULL_P
				{
					AlterDomainStmt *n = makeNode(AlterDomainStmt);
					n->subtype = 'N';
					n->typname = $3;
					$$ = (Node *)n;
				}
			/* ALTER DOMAIN <domain> SET NOT NULL */
			| ALTER DOMAIN_P any_name SET NOT NULL_P
				{
					AlterDomainStmt *n = makeNode(AlterDomainStmt);
					n->subtype = 'O';
					n->typname = $3;
					$$ = (Node *)n;
				}
			/* ALTER DOMAIN <domain> ADD CONSTRAINT ... */
			| ALTER DOMAIN_P any_name ADD_P TableConstraint
				{
					AlterDomainStmt *n = makeNode(AlterDomainStmt);
					n->subtype = 'C';
					n->typname = $3;
					n->def = $5;
					$$ = (Node *)n;
				}
			/* ALTER DOMAIN <domain> DROP CONSTRAINT <name> [RESTRICT|CASCADE] */
			| ALTER DOMAIN_P any_name DROP CONSTRAINT name opt_drop_behavior
				{
					AlterDomainStmt *n = makeNode(AlterDomainStmt);
					n->subtype = 'X';
					n->typname = $3;
					n->name = $6;
					n->behavior = $7;
					n->missing_ok = false;
					$$ = (Node *)n;
				}
			/* ALTER DOMAIN <domain> DROP CONSTRAINT IF EXISTS <name> [RESTRICT|CASCADE] */
			| ALTER DOMAIN_P any_name DROP CONSTRAINT IF_P EXISTS name opt_drop_behavior
				{
					AlterDomainStmt *n = makeNode(AlterDomainStmt);
					n->subtype = 'X';
					n->typname = $3;
					n->name = $8;
					n->behavior = $9;
					n->missing_ok = true;
					$$ = (Node *)n;
				}
			/* ALTER DOMAIN <domain> VALIDATE CONSTRAINT <name> */
			| ALTER DOMAIN_P any_name VALIDATE CONSTRAINT name
				{
					AlterDomainStmt *n = makeNode(AlterDomainStmt);
					n->subtype = 'V';
					n->typname = $3;
					n->name = $6;
					$$ = (Node *)n;
				}
			;

opt_as:		AS										{}
			| /* EMPTY */							{}
		;


/*****************************************************************************
 *
 * Manipulate a text search dictionary or configuration
 *
 *****************************************************************************/

AlterTSDictionaryStmt:
			ALTER TEXT_P SEARCH DICTIONARY any_name definition
				{
					AlterTSDictionaryStmt *n = makeNode(AlterTSDictionaryStmt);
					n->dictname = $5;
					n->options = $6;
					$$ = (Node *)n;
				}
		;

AlterTSConfigurationStmt:
			ALTER TEXT_P SEARCH CONFIGURATION any_name ADD_P MAPPING FOR name_list WITH any_name_list
				{
					AlterTSConfigurationStmt *n = makeNode(AlterTSConfigurationStmt);
					n->cfgname = $5;
					n->tokentype = $9;
					n->cfoptions = NIL;
					n->dicts = $11;
					n->override = false;
					n->replace = false;
					n->is_reset = false;
					$$ = (Node*)n;
				}
			| ALTER TEXT_P SEARCH CONFIGURATION any_name ALTER MAPPING FOR name_list WITH any_name_list
				{
					AlterTSConfigurationStmt *n = makeNode(AlterTSConfigurationStmt);
					n->cfgname = $5;
					n->tokentype = $9;
					n->cfoptions = NIL;
					n->dicts = $11;
					n->override = true;
					n->replace = false;
					n->is_reset = false;
					$$ = (Node*)n;
				}
			| ALTER TEXT_P SEARCH CONFIGURATION any_name ALTER MAPPING REPLACE any_name WITH any_name
				{
					AlterTSConfigurationStmt *n = makeNode(AlterTSConfigurationStmt);
					n->cfgname = $5;
					n->tokentype = NIL;
					n->cfoptions = NIL;
					n->dicts = list_make2($9,$11);
					n->override = false;
					n->replace = true;
					n->is_reset = false;
					$$ = (Node*)n;
				}
			| ALTER TEXT_P SEARCH CONFIGURATION any_name ALTER MAPPING FOR name_list REPLACE any_name WITH any_name
				{
					AlterTSConfigurationStmt *n = makeNode(AlterTSConfigurationStmt);
					n->cfgname = $5;
					n->tokentype = $9;
					n->cfoptions = NIL;
					n->dicts = list_make2($11,$13);
					n->override = false;
					n->replace = true;
					n->is_reset = false;
					$$ = (Node*)n;
				}
			| ALTER TEXT_P SEARCH CONFIGURATION any_name DROP MAPPING FOR name_list
				{
					AlterTSConfigurationStmt *n = makeNode(AlterTSConfigurationStmt);
					n->cfgname = $5;
					n->tokentype = $9;
					n->cfoptions = NIL;
					n->missing_ok = false;
					n->is_reset = false;
					$$ = (Node*)n;
				}
			| ALTER TEXT_P SEARCH CONFIGURATION any_name DROP MAPPING IF_P EXISTS FOR name_list
				{
					AlterTSConfigurationStmt *n = makeNode(AlterTSConfigurationStmt);
					n->cfgname = $5;
					n->tokentype = $11;
					n->cfoptions = NIL;
					n->missing_ok = true;
					n->is_reset = false;
					$$ = (Node*)n;
				}
			| ALTER TEXT_P SEARCH CONFIGURATION any_name SET cfoptions
				{
					AlterTSConfigurationStmt *n = makeNode(AlterTSConfigurationStmt);
					n->cfgname = $5;
					n->tokentype = NIL;
					n->cfoptions = $7;
					n->missing_ok = false;
					n->override = false;
					n->replace = false;
					n->is_reset = false;
					$$ = (Node*)n;
				}
			| ALTER TEXT_P SEARCH CONFIGURATION any_name RESET cfoptions
				{
					AlterTSConfigurationStmt *n = makeNode(AlterTSConfigurationStmt);
					n->cfgname = $5;
					n->tokentype = NIL;
					n->cfoptions = $7;
					n->override = false;
					n->replace = false;
					n->missing_ok = false;
					n->is_reset = true;
					$$ = (Node*)n;
				}
		;


/*****************************************************************************
 *
 * Manipulate a conversion
 *
 *		CREATE [DEFAULT] CONVERSION <conversion_name>
 *		FOR <encoding_name> TO <encoding_name> FROM <func_name>
 *
 *****************************************************************************/

CreateConversionStmt:
			CREATE opt_default CONVERSION_P any_name FOR Sconst
			TO Sconst FROM any_name
			{
				CreateConversionStmt *n = makeNode(CreateConversionStmt);
				n->conversion_name = $4;
				n->for_encoding_name = $6;
				n->to_encoding_name = $8;
				n->func_name = $10;
				n->def = $2;
				$$ = (Node *)n;
			}
		;

/*****************************************************************************
 *
 *		QUERY:
 *				CLUSTER [VERBOSE] <qualified_name> [ USING <index_name> ]
 *				CLUSTER [VERBOSE]
 *				CLUSTER [VERBOSE] <index_name> ON <qualified_name> (for pre-8.3)
 *
 *****************************************************************************/

ClusterStmt:
			CLUSTER opt_verbose qualified_name cluster_index_specification
				{
					ClusterStmt *n = makeNode(ClusterStmt);
					$3->partitionname = NULL;
					n->relation = $3;
					n->indexname = $4;
					n->verbose = $2;
					$$ = (Node*)n;
				}
			| CLUSTER opt_verbose qualified_name PARTITION '(' name ')' cluster_index_specification
				{
					ClusterStmt *n = makeNode(ClusterStmt);
					$3->partitionname = $6;
					n->relation = $3;
					n->indexname = $8;
					n->verbose = $2;
					$$ = (Node*)n;
				}
			| CLUSTER opt_verbose
				{
					ClusterStmt *n = makeNode(ClusterStmt);
					n->relation = NULL;
					n->indexname = NULL;
					n->verbose = $2;
					$$ = (Node*)n;
				}
			/* kept for pre-8.3 compatibility */
			| CLUSTER opt_verbose index_name ON qualified_name
				{
					ClusterStmt *n = makeNode(ClusterStmt);
					n->relation = $5;
					n->indexname = $3;
					n->verbose = $2;
					$$ = (Node*)n;
				}
		;

cluster_index_specification:
			USING index_name		{ $$ = $2; }
			| /*EMPTY*/				{ $$ = NULL; }
		;


/*****************************************************************************
 *
 *		QUERY:
 *				VACUUM
 *				ANALYZE
 *
 *****************************************************************************/

VacuumStmt:
			VACUUM opt_deltamerge
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					if ($2)
						n->options |= VACOPT_MERGE;
					$$ = (Node *)n;
				}
			| VACUUM opt_deltamerge qualified_name
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					if ($2)
						n->options |= VACOPT_MERGE;
					n->relation = $3;
					$$ = (Node *)n;
				}
			| VACUUM opt_hdfsdirectory
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_VACUUM;
					if ($2)
					{
						n->options |= VACOPT_HDFSDIRECTORY;
					}
					$$ = (Node *)n;
				}
			| VACUUM opt_hdfsdirectory qualified_name
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_VACUUM;
					if ($2)
					{
						n->options |= VACOPT_HDFSDIRECTORY;
					}
					n->relation = $3;
					$$ = (Node *)n;
				}
			| VACUUM opt_full opt_freeze opt_verbose opt_compact
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_VACUUM;
					if ($2)
						n->options |= VACOPT_FULL;
					if ($4)
						n->options |= VACOPT_VERBOSE;
					if ($5)
						n->options |= VACOPT_COMPACT;
					int options = 0;
					options |= VACOPT_VACUUM | VACOPT_FULL | VACOPT_COMPACT;
					if (n->options & VACOPT_COMPACT)
					{
						if (n->options != options)
						{
							ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("COMPACT can only be used with VACUUM FULL")));
						}
						if ($3)
						{
							ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("COMPACT can not be used with FREEZE")));
						}
					}
					n->freeze_min_age = $3 ? 0 : -1;
					n->freeze_table_age = $3 ? 0 : -1;
					n->relation = NULL;
					n->va_cols = NIL;
					$$ = (Node *)n;
				}
			| VACUUM opt_full opt_freeze opt_verbose opt_compact qualified_name
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_VACUUM;
					if ($2)
						n->options |= VACOPT_FULL;
					if ($4)
						n->options |= VACOPT_VERBOSE;
					if ($5)
						n->options |= VACOPT_COMPACT;
					int options = 0;
					options |= VACOPT_VACUUM | VACOPT_FULL | VACOPT_COMPACT;
					if (n->options & VACOPT_COMPACT)
					{
						if (n->options != options)
						{
							ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("COMPACT can only be used with VACUUM FULL")));
						}
						if ($3)
						{
							ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("COMPACT can not be used with FREEZE")));
						}
					}
					n->freeze_min_age = $3 ? 0 : -1;
					n->freeze_table_age = $3 ? 0 : -1;
					n->relation = $6;
					n->va_cols = NIL;
					$$ = (Node *)n;
				}
			| VACUUM opt_full opt_freeze opt_verbose opt_compact qualified_name PARTITION '('name')'
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_VACUUM;
					if ($2)
						n->options |= VACOPT_FULL;
					if ($4)
						n->options |= VACOPT_VERBOSE;
					if ($5)
					{
						ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("COMPACT can not be used with PARTITION")));
					}
					n->freeze_min_age = $3 ? 0 : -1;
					n->freeze_table_age = $3 ? 0 : -1;
					n->relation = $6;
					n->va_cols = NIL;
					$6->partitionname = $9;
					$$ = (Node *)n;
				}
			| VACUUM opt_full opt_freeze opt_verbose opt_compact AnalyzeStmt
				{
					VacuumStmt *n = (VacuumStmt *) $6;
					n->options |= VACOPT_VACUUM;
					if ($2)
						n->options |= VACOPT_FULL;
					if ($4)
						n->options |= VACOPT_VERBOSE;
					if ($5)
					{
						ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("COMPACT can not be used with ANALYZE")));
					}
					n->freeze_min_age = $3 ? 0 : -1;
					n->freeze_table_age = $3 ? 0 : -1;
					$$ = (Node *)n;
				}
			| VACUUM '(' vacuum_option_list ')'
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_VACUUM | $3;
					if (n->options & VACOPT_FREEZE)
						n->freeze_min_age = n->freeze_table_age = 0;
					else
						n->freeze_min_age = n->freeze_table_age = -1;
					n->relation = NULL;
					n->va_cols = NIL;
					$$ = (Node *) n;
				}
			| VACUUM '(' vacuum_option_list ')' qualified_name opt_name_list
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_VACUUM | $3;
					if (n->options & VACOPT_FREEZE)
						n->freeze_min_age = n->freeze_table_age = 0;
					else
						n->freeze_min_age = n->freeze_table_age = -1;
					n->relation = $5;
					n->va_cols = $6;
					if (n->va_cols != NIL)	/* implies analyze */
						n->options |= VACOPT_ANALYZE;
					$$ = (Node *) n;
				}
			| VACUUM '(' vacuum_option_list ')' qualified_name opt_name_list PARTITION '('name')'
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_VACUUM | $3;
					if (n->options & VACOPT_FREEZE)
						n->freeze_min_age = n->freeze_table_age = 0;
					else
						n->freeze_min_age = n->freeze_table_age = -1;
					n->relation = $5;
					n->va_cols = $6;
					if (n->va_cols != NIL)	/* implies analyze */
						n->options |= VACOPT_ANALYZE;
					$5->partitionname = $9;
					$$ = (Node *) n;
				}
		;

vacuum_option_list:
			vacuum_option_elem								{ $$ = $1; }
			| vacuum_option_list ',' vacuum_option_elem		{ $$ = $1 | $3; }
		;

vacuum_option_elem:
			analyze_keyword		{ $$ = VACOPT_ANALYZE; }
			| VERBOSE			{ $$ = VACOPT_VERBOSE; }
			| FREEZE			{ $$ = VACOPT_FREEZE; }
			| FULL				{ $$ = VACOPT_FULL; }
		;

AnalyzeStmt:
			analyze_keyword opt_verbose
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_ANALYZE;
					if ($2)
						n->options |= VACOPT_VERBOSE;
					n->freeze_min_age = -1;
					n->freeze_table_age = -1;
					n->relation = NULL;
					n->va_cols = NIL;
					$$ = (Node *)n;
				}
			| analyze_keyword opt_verbose qualified_name opt_analyze_column_define
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_ANALYZE;
					if ($2)
						n->options |= VACOPT_VERBOSE;
					n->freeze_min_age = -1;
					n->freeze_table_age = -1;
					n->relation = $3;
					n->va_cols = $4;
					$$ = (Node *)n;
				}
			| analyze_keyword opt_verbose qualified_name opt_name_list PARTITION '('name')'
				{
					VacuumStmt *n = makeNode(VacuumStmt);
					n->options = VACOPT_ANALYZE;
					if ($2)
						n->options |= VACOPT_VERBOSE;
					n->freeze_min_age = -1;
					n->freeze_table_age = -1;
					n->relation = $3;
					n->va_cols = $4;
					$3->partitionname = $7;
					$$ = (Node *)n;
				}
			/*
			 * @hdfs Support command "analyze [verbose] foreign tables"
			 */
			| analyze_keyword opt_verbose FOREIGN TABLES
				{
					VacuumStmt *n = (VacuumStmt*)makeNode(VacuumStmt);
					n->options = VACOPT_ANALYZE;
					if ($2)
						n->options |= VACOPT_VERBOSE;
					n->freeze_min_age = -1;
					n->freeze_table_age = -1;
					n->relation = NULL;
					n->va_cols = NIL;
					n->isForeignTables = TRUE;
					$$ = (Node *)n;

				}
		;

VerifyStmt:
            /* analyse verify fast|complete*/
            analyze_keyword opt_verify opt_verify_options
                {
                    VacuumStmt *n = makeNode(VacuumStmt);
                    n->options = VACOPT_VERIFY | $3;
                    $$ = (Node *)n;
                    n->isCascade = true;
                    n->curVerifyRel = InvalidOid;
                }

            /* analyse verify fast|complete index_name/table_name*/
            | analyze_keyword opt_verify opt_verify_options qualified_name opt_cascade
                {
                    VacuumStmt *n = makeNode(VacuumStmt);
                    n->options = VACOPT_VERIFY | $3;
                    n->relation = $4;
                    if ($5) {
                        n->isCascade = $5;
                    }
                    n->curVerifyRel = InvalidOid;
                    $$ = (Node *)n;
                }

            /* analyse verify fast|complete table_name partition (partition_name) cascade*/
            | analyze_keyword opt_verify opt_verify_options qualified_name PARTITION '('name')' opt_cascade
                {
                    VacuumStmt *n = makeNode(VacuumStmt);
                    n->options = VACOPT_VERIFY | $3;
                    n->relation = $4;
                    $4->partitionname = $7;
                    if ($9) {
                        n->isCascade = $9;
                    }
                    n->curVerifyRel = InvalidOid;
                    $$ = (Node *)n;
                }
        ;

analyze_keyword:
			ANALYZE									{}
			| ANALYSE /* British */					{}
		;

opt_verify_options:
        	FAST                                    { $$ = VACOPT_FAST;}
            |COMPLETE                               { $$ = VACOPT_COMPLETE;}
	    ;

opt_verbose:
			VERBOSE									{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

opt_full:	FULL									{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

opt_compact:	COMPACT								{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

opt_hdfsdirectory:	HDFSDIRECTORY 					{ $$ = TRUE; }
		;

opt_freeze: FREEZE									{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

opt_deltamerge: DELTAMERGE 							{$$ = TRUE;}
		;

opt_verify: 	VERIFY                                  { $$ = TRUE; }
	        ;

opt_cascade: 	CASCADE                                { $$ = TRUE; }
            	| /*EMPTY*/                            { $$ = FALSE; }
        	;
opt_name_list:
			'(' name_list ')'						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

opt_analyze_column_define:
			'(' opt_multi_name_list ')'
				{
					$$ = $2;
				}
			|'(' name_list ')'						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

opt_multi_name_list:
			'(' name_list ')'
				{
					$$ = list_make1($2);
				}
		;

/* PGXC_BEGIN */
BarrierStmt: CREATE BARRIER opt_barrier_id
				{
					BarrierStmt *n = makeNode(BarrierStmt);
					n->id = $3;
					$$ = (Node *)n;
				}
			;

opt_barrier_id:
				Sconst
				{
					$$ = pstrdup($1);
				}
			| /* EMPTY */
				{
					$$ = NULL;
				}
			;

/*****************************************************************************
 *
 *		QUERY:
 *
 *		CREATE NODE nodename WITH
 *				(
 *					[ TYPE = ('datanode' | 'coordinator'), ]
 *					[ RW [ = boolean ], ]
 *					[ HOST = 'hostname', ]
 *					[ PORT = portnum, ]
 *					[ HOST1 = 'hostname', ]
 *					[ PORT1 = portnum, ]
 *					[ HOSTPRIMARY [ = boolean ], ]
 *					[ PRIMARY [ = boolean ], ]
 *					[ PREFERRED [ = boolean ] ]
 *					[ SCTP_PORT = portnum, ]
 *					[ CONTROL_PORT = portnum, ]
 *					[ SCTP_PORT1 = portnum, ]
 *					[ CONTROL_PORT1 = portnum, ]
 *				)
 *
 *****************************************************************************/

CreateNodeStmt: CREATE NODE pgxcnode_name OptWith
				{
					CreateNodeStmt *n = makeNode(CreateNodeStmt);
					n->node_name = $3;
					n->options = $4;
					$$ = (Node *)n;
				}
		;

pgxcnode_name:
			ColId							{ $$ = $1; };

pgxcgroup_name:
			ColId							{ $$ = $1; };

pgxcnodes:
			'(' pgxcnode_list ')'			{ $$ = $2; }
		;

pgxcnode_list:
			pgxcnode_list ',' pgxcnode_name		{ $$ = lappend($1, makeString($3)); }
			| pgxcnode_name						{ $$ = list_make1(makeString($1)); }
		;

bucket_maps:
			BUCKETS '(' bucket_list ')'			{ $$ = $3; }
			| /*EMPTY*/ 			            { $$ = NIL; }
		;

bucket_list:
			bucket_list ',' Iconst		{ $$ = lappend($1, makeInteger($3)); }
			| Iconst					{ $$ = list_make1(makeInteger($1)); }
		;

opt_vcgroup: 
            		VCGROUP			{$$ = TRUE;}
			| /* EMPTY */		{$$ = FALSE;}
		;

opt_to_elastic_group: 
            TO ELASTIC GROUP_P	{$$ = TRUE;}
			| /* EMPTY */		{$$ = FALSE;}
		;

opt_redistributed: 
            		DISTRIBUTE FROM  ColId	{$$ = $3;}
			| /* EMPTY */      	{$$ = NULL;}
		;

opt_set_vcgroup: 
            		SET VCGROUP		{$$ = AG_SET_VCGROUP;}
            		| SET NOT VCGROUP	{$$ = AG_SET_NOT_VCGROUP;}
			| /* EMPTY */		{$$ = AG_SET_RENAME;}
		;

/*****************************************************************************
 *
 *		QUERY:
 *		ALTER NODE nodename WITH
 *				(
 *					[ TYPE = ('datanode' | 'coordinator'), ]
 *					[ HOST = 'hostname', ]
 *					[ RW [ = boolean ], ]
 *					[ PORT = portnum, ]
 *					[ HOST1 = 'hostname', ]
 *					[ PORT1 = portnum, ]
 *					[ HOSTPRIMARY [ = boolean ], ]
 *					[ PRIMARY [ = boolean ], ]
 *					[ PREFERRED [ = boolean ] ]
 *					[ SCTP_PORT = portnum, ]
 *					[ CONTROL_PORT = portnum, ]
 *					[ SCTP_PORT1 = portnum, ]
 *					[ CONTROL_PORT1 = portnum, ]
 *				)
 *
 *****************************************************************************/

AlterNodeStmt: ALTER NODE pgxcnode_name OptWith
				{
					AlterNodeStmt *n = makeNode(AlterNodeStmt);
					n->node_name = $3;
					n->options = $4;
					$$ = (Node *)n;
				}
		;
/* alter node coordinator set true/false with (cn1,...,cnN) */
AlterCoordinatorStmt: ALTER COORDINATOR pgxcnode_name SET opt_boolean_or_string WITH pgxcnodes
				{
					AlterCoordinatorStmt *n = makeNode(AlterCoordinatorStmt);
					n->node_name = $3;
					n->set_value = $5;
					n->coor_nodes = $7;
					$$ = (Node *)n;	
				}
		;
	

/*****************************************************************************
 *
 *		QUERY:
 *				DROP NODE [IF EXISTS] nodename [WITH (cn1,...,cnN)]
 *
 *****************************************************************************/

DropNodeStmt: DROP NODE pgxcnode_name opt_pgxcnodes
				{
					DropNodeStmt *n = makeNode(DropNodeStmt);
					n->node_name = $3;
					n->missing_ok = FALSE;
					n->remote_nodes = $4;
					$$ = (Node *)n;
				}
			| DROP NODE IF_P EXISTS pgxcnode_name opt_pgxcnodes
				{
					DropNodeStmt *n = makeNode(DropNodeStmt);
					n->node_name = $5;
					n->missing_ok = TRUE;
					n->remote_nodes = $6;
					$$ = (Node *)n;
				}
		;

opt_pgxcnodes:	WITH pgxcnodes
				{
					$$ = $2;
				}
				| /*EMPTY*/
				{
					$$ = NIL;
				}
		;

/*****************************************************************************
 *
 *		QUERY:
 *				CREATE NODE GROUP groupname WITH (node1,...,nodeN) [BUCKETS (0,1,2,...)]
 *
 *****************************************************************************/

CreateNodeGroupStmt: CREATE NODE GROUP_P pgxcgroup_name WITH pgxcnodes bucket_maps opt_vcgroup opt_redistributed
				{
					CreateGroupStmt *n = makeNode(CreateGroupStmt);
					IsValidGroupname($4);
					n->group_name = $4;
					n->nodes = $6;
					n->buckets = $7;
					n->vcgroup = $8;
					n->src_group_name = $9;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 *		QUERY:
 *				ALTER NODE GROUP groupname SET DEFAULT
 *
 *****************************************************************************/

AlterNodeGroupStmt: ALTER NODE GROUP_P pgxcgroup_name SET DEFAULT
				{
					AlterGroupStmt *n = makeNode(AlterGroupStmt);
					n->group_name = $4;
					n->alter_type = AG_SET_DEFAULT;
					$$ = (Node *)n;
				}
				| ALTER NODE GROUP_P pgxcgroup_name opt_set_vcgroup RENAME TO pgxcgroup_name
				{
					AlterGroupStmt *n = makeNode(AlterGroupStmt);
					n->group_name = $4;
					n->install_name = $8;
					n->alter_type = (AlterGroupType)$5;
					$$ = (Node *)n;
				}
				| ALTER NODE GROUP_P pgxcgroup_name SET NOT VCGROUP
				{
					AlterGroupStmt *n = makeNode(AlterGroupStmt);
					n->group_name = $4;
					n->install_name = NULL;
					n->alter_type = AG_SET_NOT_VCGROUP;
					$$ = (Node *)n;
				}
				| ALTER NODE GROUP_P pgxcgroup_name SET TABLE GROUP_P pgxcgroup_name
				{
					AlterGroupStmt *n = makeNode(AlterGroupStmt);
					n->group_name = $4;
					n->install_name = $8;
					n->alter_type = AG_SET_TABLE_GROUP;
					$$ = (Node *)n;
				}
				| ALTER NODE GROUP_P pgxcgroup_name COPY BUCKETS FROM pgxcgroup_name
				{
					AlterGroupStmt *n = makeNode(AlterGroupStmt);
					n->group_name = $4;
					n->install_name = $8;
					n->alter_type = AG_SET_BUCKETS;
					$$ = (Node *)n;
				}
				| ALTER NODE GROUP_P pgxcgroup_name ADD_P NODE pgxcnodes
				{
					AlterGroupStmt *n = makeNode(AlterGroupStmt);
					n->group_name = $4;
					n->install_name = NULL;
					n->alter_type = AG_ADD_NODES;
					n->nodes = $7;
					$$ = (Node *)n;
				}
				| ALTER NODE GROUP_P pgxcgroup_name DELETE_P NODE pgxcnodes
				{
					AlterGroupStmt *n = makeNode(AlterGroupStmt);
					n->group_name = $4;
					n->install_name = NULL;
					n->alter_type = AG_DELETE_NODES;
					n->nodes = $7;
					$$ = (Node *)n;
				}
				| ALTER NODE GROUP_P pgxcgroup_name RESIZE TO pgxcgroup_name
				{
					AlterGroupStmt *n = makeNode(AlterGroupStmt);
					n->group_name = $4;
					n->install_name = $7;
					n->alter_type = AG_RESIZE_GROUP;
					$$ = (Node *)n;
				}
				| ALTER NODE GROUP_P pgxcgroup_name opt_set_vcgroup WITH GROUP_P pgxcgroup_name
				{
					AlterGroupStmt *n = makeNode(AlterGroupStmt);
					n->group_name = $4;
					n->install_name = $8;
					n->alter_type = AG_CONVERT_VCGROUP;
					$$ = (Node *)n;
				}
				| ALTER NODE GROUP_P pgxcgroup_name SET SEQUENCE TO ALL NODE
                                {
                                	AlterGroupStmt *n = makeNode(AlterGroupStmt);
                                        n->group_name = $4;
                                        n->install_name = NULL;
                                        n->alter_type = AG_SET_SEQ_ALLNODES;
                                        $$ = (Node *)n;
                                }
                                | ALTER NODE GROUP_P pgxcgroup_name SET SEQUENCE TO LOCAL
                                {
                                        AlterGroupStmt *n = makeNode(AlterGroupStmt);
                                        n->group_name = $4;
                                        n->install_name = NULL;
                                        n->alter_type = AG_SET_SEQ_SELFNODES;
                                        $$ = (Node *)n;
                                }
		;

/*****************************************************************************
 *
 *		QUERY:
 *				DROP NODE GROUP groupname
 *
 *****************************************************************************/

DropNodeGroupStmt: DROP NODE GROUP_P pgxcgroup_name opt_redistributed opt_to_elastic_group
				{
					DropGroupStmt *n = makeNode(DropGroupStmt);
					n->group_name = $4;
					n->src_group_name = $5;
					n->to_elastic_group = $6;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 *		QUERY:
/*****************************************************************************
 *
 *     QUERY:
 *
 *     CREATE AUDIT POLICY policy_name
 *
 *****************************************************************************/
CreateAuditPolicyStmt:
        CREATE AUDIT POLICY policy_name PRIVILEGES policy_privileges_list policy_filter_opt policy_status_opt
                {
                    CreateAuditPolicyStmt *n = makeNode(CreateAuditPolicyStmt);
                    n->policy_type = "privileges";
                    n->if_not_exists = false;
                    IsValidIdent($4);
                    n->policy_name = $4;
                    n->policy_targets = $6;
                    n->policy_filters = $7;
                    n->policy_enabled = $8;
                    $$ = (Node *) n;
                }
        | CREATE AUDIT POLICY IF_P NOT EXISTS policy_name PRIVILEGES policy_privileges_list policy_filter_opt policy_status_opt
                {
                    CreateAuditPolicyStmt *n = makeNode(CreateAuditPolicyStmt);
                    n->policy_type = "privileges";
                    n->if_not_exists = true;
                    IsValidIdent($7);
                    n->policy_name = $7;
                    n->policy_targets = $9;
                    n->policy_filters = $10;
                    n->policy_enabled = $11;
                    $$ = (Node *) n;
                }
        | CREATE AUDIT POLICY policy_name ACCESS policy_access_list policy_filter_opt policy_status_opt
                {
                    CreateAuditPolicyStmt *n = makeNode(CreateAuditPolicyStmt);
                    n->policy_type = "access";
                    n->if_not_exists = false;
                    IsValidIdent($4);
                    n->policy_name = $4;
                    n->policy_targets = $6;
                    n->policy_filters = $7;
                    n->policy_enabled = $8;
                    $$ = (Node *) n;
                }
        | CREATE AUDIT POLICY IF_P NOT EXISTS policy_name ACCESS policy_access_list policy_filter_opt policy_status_opt
                {
                    CreateAuditPolicyStmt *n = makeNode(CreateAuditPolicyStmt);
                    n->policy_type = "access";
                    n->if_not_exists = true;
                    IsValidIdent($7);
                    n->policy_name = $7;
                    n->policy_targets = $9;
                    n->policy_filters = $10;
                    n->policy_enabled = $11;
                    $$ = (Node *) n;
                }
        ;

policy_privileges_list:
        policy_privilege_elem
            {
                $$ = list_make1($1);
            }
        | policy_privileges_list ',' policy_privilege_elem
            {
                $$ = lappend($1, $3);
            }
        ;

policy_privilege_elem:
        policy_privilege_type policy_target_elem_opt
            {
                $$ = makeDefElem($1, (Node *)$2);
            }
        ;

policy_privilege_type:
        ALL             { $$ = "all"; }
        | ALTER         { $$ = "alter"; }
        | ANALYZE       { $$ = "analyze"; }
        | COMMENT       { $$ = "comment"; }
        | CREATE        { $$ = "create"; }
        | DROP          { $$ = "drop"; }
        | GRANT         { $$ = "grant"; }
        | REVOKE        { $$ = "revoke"; }
        | SET           { $$ = "set"; }
        | SHOW          { $$ = "show"; }
        | LOGIN_ANY     { $$ = "login_any"; }
        | LOGIN_FAILURE { $$ = "login_failure"; }
        | LOGIN_SUCCESS { $$ = "login_success"; }
        | LOGOUT        { $$ = "logout"; }
        ;

policy_access_list:
        policy_access_elem
            {
                $$ = list_make1($1);
            }
        | policy_access_list ',' policy_access_elem
            {
                $$ = lappend($1, $3);
            }
        ;

policy_access_elem:
        policy_access_type policy_target_elem_opt
            {
                $$ = makeDefElem($1, (Node *)$2);
            }
        ;

// Commented out will be supported in future releases / upon client request
policy_access_type:
        ALL             { $$ = "all"; }
        | COPY          { $$ = "copy"; }
        | DEALLOCATE    { $$ = "deallocate"; }
        | DELETE_P      { $$ = "delete"; }
        | EXECUTE       { $$ = "execute"; }
        | INSERT        { $$ = "insert"; }
        | PREPARE       { $$ = "prepare"; }
        | REINDEX       { $$ = "reindex"; }
        | SELECT        { $$ = "select"; }
        | TRUNCATE      { $$ = "truncate"; }
        | UPDATE        { $$ = "update"; }
        ;

policy_target_elem_opt:
        ON policy_target_type '(' policy_targets_list ')'
            {
                $$ = makeDefElem($2, (Node *)$4);
            }
        | /* EMPTY */ { $$ = NULL; }
		;

policy_targets_list:
        policy_target_name
            {
                $$ = list_make1($1);
            }
        | policy_targets_list ',' policy_target_name
            {
                $$ = lappend($1, $3);
            }
        ;
policy_target_type:
        LABEL         { $$ = "label"; }
        ;

policy_target_name:
        qualified_name     { $$ = $1; }
        ;

policy_filter_opt:
         FILTER ON policy_filter_value
            {
                $$ = list_make1((Node *)$3);
            }
        | /* EMPTY */ { $$ = NULL; }
        ;

policy_filter_value:
        filter_expr     { $$ = $1; }
        | filter_set    { $$ = $1; }
        ; 

filter_set:
        filter_paren                { $$ = $1; }
        | '(' filter_expr_list ')'  { $$ = $2; }
        | filter_expr_list          { $$ = $1; }
        ;

filter_expr_list: 
        filter_expr ',' filter_expr
            {
                PolicyFilterNode *n = makeNode(PolicyFilterNode);
                n->node_type = "op";
                n->op_value = "and";
                n->left = $1;
                n->right = $3;
                $$ = (Node *) n;
            }
        | filter_expr_list ',' filter_expr
            {
                PolicyFilterNode *n = makeNode(PolicyFilterNode);
                n->node_type = "op";
                n->op_value = "and";
                n->left = $1;
                n->right = $3;
                $$ = (Node *) n;
            }
        ;

filter_paren:
        '(' filter_expr ')' { $$ = $2; }
        ;

policy_filters_list:
        policy_filter_name
            {
                $$ = list_make1(makeString($1));
            }
        | policy_filters_list ',' policy_filter_name
            {
                $$ = lappend($1, makeString($3));
            }
        ;

policy_filter_type:
          IP            { $$ = "ip"; }
        | APP           { $$ = "app"; }
        | ROLES         { $$ = "roles"; }
        ;

policy_filter_name:
        ColId_or_Sconst { $$ = $1; }
        ;

policy_name:
        ColLabel        { $$ = $1; }
        ;

policy_status_opt:
        ENABLE_P        {$$ = TRUE;}
        | DISABLE_P     {$$ = FALSE;}
        | /* EMPTY */   {$$ = TRUE;}


/*****************************************************************************
 *
 *     QUERY:
 *
 *     ALTER AUDIT POLICY policy_name
 *
 *****************************************************************************/
AlterAuditPolicyStmt:
        /* alter comments */
        ALTER AUDIT POLICY policy_name policy_comments_alter_clause
                {
                    AlterAuditPolicyStmt *n = makeNode(AlterAuditPolicyStmt);
                    n->policy_name = $4;
                    n->policy_action = "add";
                    n->policy_comments = $5;
                    $$ = (Node *) n;
                }
        |        
        ALTER AUDIT POLICY IF_P EXISTS policy_name policy_comments_alter_clause
                {
                    AlterAuditPolicyStmt *n = makeNode(AlterAuditPolicyStmt);
                    n->missing_ok = true;
                    n->policy_name = $6;
                    n->policy_action = "add";
                    n->policy_comments = $7;
                    $$ = (Node *) n;
                }
        |        
        /* alter add/remove privilege */
        ALTER AUDIT POLICY policy_name alter_policy_action_clause alter_policy_privileges_list
                {
                    AlterAuditPolicyStmt *n = makeNode(AlterAuditPolicyStmt);
                    n->policy_type = "privileges";
                    n->policy_name = $4;
                    n->policy_action = $5;
                    n->policy_items = $6;
                    $$ = (Node *) n;
                }
        |
        ALTER AUDIT POLICY IF_P EXISTS policy_name alter_policy_action_clause alter_policy_privileges_list
                {
                    AlterAuditPolicyStmt *n = makeNode(AlterAuditPolicyStmt);
                    n->missing_ok = true;
                    n->policy_type = "privileges";
                    n->policy_name = $6;
                    n->policy_action = $7;
                    n->policy_items = $8;
                    $$ = (Node *) n;
                }
        |
        /* alter add/remove access */
        ALTER AUDIT POLICY policy_name alter_policy_action_clause alter_policy_access_list
                {
                    AlterAuditPolicyStmt *n = makeNode(AlterAuditPolicyStmt);
                    n->policy_type = "access";
                    n->policy_name = $4;
                    n->policy_action = $5;
                    n->policy_items = $6;
                    $$ = (Node *) n;
                }
        |
        ALTER AUDIT POLICY IF_P EXISTS policy_name alter_policy_action_clause alter_policy_access_list
                {
                    AlterAuditPolicyStmt *n = makeNode(AlterAuditPolicyStmt);
                    n->missing_ok = true;
                    n->policy_type = "access";
                    n->policy_name = $6;
                    n->policy_action = $7;
                    n->policy_items = $8;
                    $$ = (Node *) n;
                }
        |
        /* alter modify filter */
        ALTER AUDIT POLICY policy_name MODIFY_P '(' alter_policy_filter_list ')'
                {
                    AlterAuditPolicyStmt *n = makeNode(AlterAuditPolicyStmt);
                    n->policy_name = $4;
                    n->policy_action = "modify";
                    n->policy_filters = $7;
                    $$ = (Node *) n;
                }
        |
        ALTER AUDIT POLICY IF_P EXISTS policy_name MODIFY_P '(' alter_policy_filter_list ')'
                {
                    AlterAuditPolicyStmt *n = makeNode(AlterAuditPolicyStmt);
                    n->missing_ok = true;
                    n->policy_name = $6;
                    n->policy_action = "modify";
                    n->policy_filters = $9;
                    $$ = (Node *) n;
                }
        |
        /* alter remove filter */
        ALTER AUDIT POLICY policy_name DROP FILTER
            {
                AlterAuditPolicyStmt *n = makeNode(AlterAuditPolicyStmt);
                n->policy_name = $4;
                n->policy_action = "drop_filter";
                $$ = (Node *) n;
            }

        |
        ALTER AUDIT POLICY IF_P EXISTS policy_name DROP FILTER
            {
                AlterAuditPolicyStmt *n = makeNode(AlterAuditPolicyStmt);
                n->missing_ok = true;
                n->policy_name = $6;
                n->policy_action = "drop_filter";
                $$ = (Node *) n;
            }

        |
        /* alter policy status */
        ALTER AUDIT POLICY policy_name policy_status_alter_clause
                {
                    AlterAuditPolicyStmt *n = makeNode(AlterAuditPolicyStmt);
                    n->policy_name = $4;
                    n->policy_action = "add";
                    n->policy_comments = "";
                    n->policy_enabled = (Node *)$5;
                    $$ = (Node *) n;
                }
        |
        ALTER AUDIT POLICY IF_P EXISTS policy_name policy_status_alter_clause
                {
                    AlterAuditPolicyStmt *n = makeNode(AlterAuditPolicyStmt);
                    n->missing_ok = true;
                    n->policy_name = $6;
                    n->policy_action = "add";
                    n->policy_comments = "";
                    n->policy_enabled = (Node *)$7;
                    $$ = (Node *) n;
                }

        ;


alter_policy_access_list:
        ACCESS '(' policy_access_list ')' {$$ = $3;}
        ;

alter_policy_privileges_list:
        PRIVILEGES '(' policy_privileges_list ')' {$$ = $3;}
        ;
        
alter_policy_filter_list:
        FILTER ON policy_filter_value
        {
            $$ = list_make1((Node *)$3);
        }

alter_policy_action_clause:
        ADD_P   { $$ = "add";  }
        | REMOVE  { $$ = "remove"; } 
        ;

policy_status_alter_clause:
        ENABLE_P        { $$ = makeDefElem("status", (Node *)makeString("enable")); } 
        | DISABLE_P     { $$ = makeDefElem("status", (Node *)makeString("disable")); }
        ;

policy_comments_alter_clause:
        COMMENTS Sconst   { $$ = $2; }
        ;
    
    

/*****************************************************************************
 *
 *     QUERY:
 *
 *     DROP AUDIT POLICY policy_name
 *
 *****************************************************************************/
DropAuditPolicyStmt:
        DROP AUDIT POLICY policy_names_list
                {
                    DropAuditPolicyStmt *n = makeNode(DropAuditPolicyStmt);
                    n->policy_names = $4;
                    $$ = (Node *) n;
                }
        | DROP AUDIT POLICY IF_P EXISTS policy_names_list
                {
                    DropAuditPolicyStmt *n = makeNode(DropAuditPolicyStmt);
                    n->missing_ok = true;
                    n->policy_names = $6;
                    $$ = (Node *) n;
                }
        ;
policy_names_list:
        policy_name { $$ = list_make1(makeString($1)); }
        | policy_names_list ',' policy_name { $$ = lappend($1, makeString($3)); }
        ;

/*****************************************************************************
 *
 *     QUERY:
 *
 *     CREATE MASKING POLICY policy_name
 *
 *****************************************************************************/
CreateMaskingPolicyStmt:
        CREATE MASKING POLICY policy_name masking_clause policy_condition_opt policy_filter_opt policy_status_opt
                {
                    CreateMaskingPolicyStmt *n = makeNode(CreateMaskingPolicyStmt);
                    n->if_not_exists = false;
                    IsValidIdent($4);
                    n->policy_name = $4;
                    n->policy_data = $5;
                    n->policy_condition = $6;
                    n->policy_filters = $7;
                    n->policy_enabled = $8;
                    $$ = (Node *) n;
                }
        ;

masking_clause:
        masking_clause_elem 
            {
                $$ = list_make1($1);
            }
        | masking_clause ',' masking_clause_elem
            {
                $$ = lappend($1, $3);
            }
        ;

masking_clause_elem:
        /*FUNCTION*/ masking_func masking_func_params_opt ON masking_target
            {
                $$ = makeDefElem($1, (Node *)lappend(list_make1($2) , $4));
            }
        ;

masking_func:
        ColLabel        { IsValidIdent($1); $$ = $1; }
        ;

masking_func_params_opt:
        '(' masking_func_params_list ')'    { $$ = $2; }
        | /* EMPTY */                       { $$ = list_make1(NIL); }
        ;

masking_func_params_list:
        masking_func_param
            { 
                 $$ = list_make1($1);
            }
        | masking_func_params_list ',' masking_func_param
            { 
                $$ = lappend($1, $3);
            }
        ;

masking_func_param:
        Iconst
            {
                char buf[64];
                snprintf(buf, sizeof(buf), "%d", $1);
                $$ = makeDefElem("i"/*int*/, (Node *)makeString(pstrdup(buf)));
            }
        | FCONST         { $$ = makeDefElem("f" /*float*/, (Node *)makeString($1)); }
        | Sconst         { $$ = makeDefElem("s" /*string*/, (Node *)makeString($1)); }
        | ColLabel       { $$ = makeDefElem("q" /*fqdn*/, (Node *)makeString($1)); }
        ;

masking_target:
        masking_policy_target_type '(' policy_targets_list ')'
            {
                $$ = makeDefElem($1, (Node *)$3);
            }
        ;

masking_policy_target_type:
        LABEL         { $$ = "label"; }
        ;

alter_policy_condition:

		    CONDITION '(' policy_label_item masking_policy_condition_operator masking_policy_condition_value ')'
            {
                MaskingPolicyCondition *n = makeNode(MaskingPolicyCondition);
                n->fqdn = $3;
                n->_operator = $4;
                n->arg = $5;
                $$ = (Node *) n;
            }
            
policy_condition_opt:
	
		    CONDITION '(' policy_label_item masking_policy_condition_operator masking_policy_condition_value ')'
            {
                MaskingPolicyCondition *n = makeNode(MaskingPolicyCondition);
                n->fqdn = $3;
                n->_operator = $4;
                n->arg = $5;
                $$ = (Node *) n;
            }
			;
	
        | /* EMPTY */
        {
            $$ = NULL;
        }
        ;

masking_policy_condition_operator:
    CmpOp       { $$ = $1; }
    | '<'       { $$ = "<"; }
    | '>'       { $$ = ">"; }
    | '='       { $$ = "="; }
    ;

masking_policy_condition_value:
		opt_boolean_or_string
			{ 
				$$ = makeStringConst($1, @1);
			}
		| NumericOnly
			{ 
				$$ = makeAConst($1, @1);
			}
		| CURRENT_USER
			{
				$$ = makeStringConst("current_user", @1);
			}
		;

pp_filter_expr:
        filter_expr         { $$ = $1; }
        | filter_paren      { $$ = $1; }
        ;

filter_expr:
        filter_term
            {
                $$ = $1;
            }
        | pp_filter_expr OR pp_filter_term
            {
                PolicyFilterNode *n = makeNode(PolicyFilterNode);
                n->node_type = "op";
                n->op_value = "or";
                n->left = $1;
                n->right = $3;
                $$ = (Node *) n;
            }
        ;

pp_filter_term:
        filter_term         { $$ = $1; }
        | filter_paren      { $$ = $1; }
        ;

filter_term:
        policy_filter_elem
            {
                $$ = $1;
            }
        | pp_filter_term AND pp_policy_filter_elem
            {
                PolicyFilterNode *n = makeNode(PolicyFilterNode);
                n->node_type = "op";
                n->op_value = "and";
                n->left = $1;
                n->right = $3;
                $$ = (Node *) n;
            }
        ;

pp_policy_filter_elem:
        policy_filter_elem  { $$ = $1; }
        | filter_paren      { $$ = $1; }
        ;

policy_filter_elem:
        policy_filter_type '(' policy_filters_list ')'
            {
                PolicyFilterNode *n = makeNode(PolicyFilterNode);
                n->node_type = "filter";
                n->filter_type = $1;
                n->values = $3;
                n->has_not_operator = false;
                $$ = (Node *) n;
            }
        | policy_filter_type NOT '(' policy_filters_list ')'
            {
                PolicyFilterNode *n = makeNode(PolicyFilterNode);
                n->node_type = "filter";
                n->filter_type = $1;
                n->values = $4;
                n->has_not_operator = true;
                $$ = (Node *) n;
            }
        ;

/*****************************************************************************
 *
 *     QUERY:
 *
 *     ALTER MASKING POLICY policy_name
 *
 *****************************************************************************/
AlterMaskingPolicyStmt:
		/* alter comments*/
 ALTER MASKING POLICY policy_name policy_comments_alter_clause
            {
                AlterMaskingPolicyStmt *n = makeNode(AlterMaskingPolicyStmt);
                n->policy_name = $4;
                n->policy_action = "add";
                n->policy_comments = $5;
                $$ = (Node *) n;
            }
		|
		
        /* alter modify/add/remove policy actions */
        ALTER MASKING POLICY policy_name alter_masking_policy_action_clause alter_masking_policy_func_items_list
        {
                AlterMaskingPolicyStmt *n = makeNode(AlterMaskingPolicyStmt);
                n->policy_name = $4;
                n->policy_action = $5;
                n->policy_items = $6;
                $$ = (Node *) n;
            }
		|

        /* alter modify filter */
        ALTER MASKING POLICY policy_name MODIFY_P '(' alter_policy_filter_list ')'
            {
                AlterMaskingPolicyStmt *n = makeNode(AlterMaskingPolicyStmt);
                n->policy_name = $4;
                n->policy_action = "modify";
                n->policy_filters = $7;
                $$ = (Node *) n;
            }
        |
        /* alter remove filter */
        ALTER MASKING POLICY policy_name DROP FILTER
            {
                AlterMaskingPolicyStmt *n = makeNode(AlterMaskingPolicyStmt);
                n->policy_name = $4;
                n->policy_action = "drop_filter";
                $$ = (Node *) n;
            }
        |
        /* alter modify condition */
        ALTER MASKING POLICY policy_name MODIFY_P '(' alter_policy_condition ')'
            {
                AlterMaskingPolicyStmt *n = makeNode(AlterMaskingPolicyStmt);
                n->policy_name = $4;
                n->policy_action = "modify";
                n->policy_condition = $7;
                $$ = (Node *) n;
            }
        |
        /* alter remove condition */
        ALTER MASKING POLICY policy_name DROP CONDITION
            {
                AlterMaskingPolicyStmt *n = makeNode(AlterMaskingPolicyStmt);
                n->policy_name = $4;
                n->policy_action = "drop_condition";
                $$ = (Node *) n;
            }
        |

        /* alter policy status */
        ALTER MASKING POLICY policy_name policy_status_alter_clause
            {
                AlterMaskingPolicyStmt *n = makeNode(AlterMaskingPolicyStmt);
                n->policy_name = $4;
                n->policy_action = "add";
                n->policy_comments = "";
                n->policy_enabled = (Node *)$5;
                $$ = (Node *) n;
            }
        ;

alter_masking_policy_action_clause:
        MODIFY_P { $$ = "modify"; }   /* Modifying is supported only by masking policy */
        | alter_policy_action_clause { $$ = $1; }
        ;

alter_masking_policy_func_items_list:
        alter_masking_policy_func_item { $$ =  list_make1($1); }
        | alter_masking_policy_func_items_list ',' alter_masking_policy_func_item { $$ = lappend($1, $3); }
        ;

alter_masking_policy_func_item:
         masking_clause_elem 
        	{
                $$ = $1;
            }
        ;

/*****************************************************************************
 *
 *     QUERY:
 *
 *     DROP MASKING POLICY policy_name 
 *
 *****************************************************************************/
DropMaskingPolicyStmt:
        DROP MASKING POLICY policy_names_list
                {
                    DropMaskingPolicyStmt *n = makeNode(DropMaskingPolicyStmt);
                    n->if_exists = false;
                    n->policy_names = $4;
                    $$ = (Node *) n;
                }
        |   DROP MASKING POLICY IF_P EXISTS policy_names_list
            {
                DropMaskingPolicyStmt *n = makeNode(DropMaskingPolicyStmt);
                n->if_exists = true;
                n->policy_names = $6;
                $$ = (Node *) n;
            }
        ;

/*****************************************************************************
 *
 *     QUERY:
 *
 *     CREATE RESOURCE LABEL label_name 
 *
 *****************************************************************************/
CreatePolicyLabelStmt:
        CREATE RESOURCE LABEL policy_label_name opt_add_resources_to_label
                {
                    CreatePolicyLabelStmt *n = makeNode(CreatePolicyLabelStmt);
                    n->label_type = "resource";
                    n->if_not_exists = false; 
                    IsValidIdent($4);
                    n->label_name = $4;
                    n->label_items = $5;
                    $$ = (Node *)n;
                }
        | CREATE RESOURCE LABEL IF_P NOT EXISTS policy_label_name opt_add_resources_to_label
                {
                    CreatePolicyLabelStmt *n = makeNode(CreatePolicyLabelStmt);
                    n->label_type = "resource";
                    n->if_not_exists = true;
                    IsValidIdent($7);
                    n->label_name = $7;
                    n->label_items = $8;
                    $$ = (Node *)n;
                }
        ;

policy_label_name:
        ColLabel        { $$ = $1; }
        ;

opt_add_resources_to_label:
        ADD_P resources_to_label_list { $$ = $2; }
        | /* EMPTY */ { $$ = NULL; }
        ;

resources_to_label_list:
        resources_to_label_list_item { $$ =  list_make1($1); }
        | resources_to_label_list ',' resources_to_label_list_item { $$ = lappend($1, $3); }
        ;

resources_to_label_list_item:
        policy_label_resource_type  '(' policy_label_items ')' { $$ = makeDefElem($1, (Node *)$3); }
        | policy_label_resource_type '(' policy_label_any_resource_item ')' { $$ = makeDefElem($1,  (Node *)list_make1($3)); }
        ;

policy_label_resource_type:
                TABLE     { $$ = "table"; }
                | COLUMN    { $$ = "column"; }
                | SCHEMA    { $$ = "schema"; }
                | VIEW      { $$ = "view"; }
                | FUNCTION  { $$ = "function"; }
        ;

policy_label_any_resource_item:
        policy_label_any_resource { $$ = makeRangeVar(NULL, $1, @1); }
        ;

policy_label_any_resource:
        ALL { $$ = "all"; }
        ;

policy_label_items:
        policy_label_item { $$ = list_make1($1); }
        | policy_label_items ',' policy_label_item { $$ =  lappend($1, $3); }
        ;

filters_to_label_list:
        filters_to_label_list_item { $$ =  list_make1($1); }
        | filters_to_label_list ',' filters_to_label_list_item { $$ = lappend($1, $3); }
        ;

filters_to_label_list_item:
        policy_label_filter_type  '(' policy_label_items ')' { $$ = makeDefElem($1, (Node *)$3); }
        ;

policy_label_filter_type: 
          IP            { $$ = "ip"; }
        | APP           { $$ = "app"; }
        | ROLES         { $$ = "roles"; }
        ;

policy_label_item:
        qualified_name     { $$ = $1; }
        ;

/*****************************************************************************
 *
 *     QUERY:
 *
 *     ALTER RESOURCE LABEL label_name
 *
 *****************************************************************************/
AlterPolicyLabelStmt:
        ALTER RESOURCE LABEL policy_label_name ADD_P resources_or_filters_to_label_list
                {
                    AlterPolicyLabelStmt *n = makeNode(AlterPolicyLabelStmt);
                    n->stmt_type = "add";
                    n->label_name = $4;
                    n->label_items = $6;
                    $$ = (Node *)n;
                }
        | ALTER RESOURCE LABEL policy_label_name REMOVE resources_or_filters_to_label_list
                {
                    AlterPolicyLabelStmt *n = makeNode(AlterPolicyLabelStmt);
                    n->stmt_type = "remove";
                    n->label_name = $4;
                    n->label_items = $6;
                    $$ = (Node *)n;
                }
        ;

resources_or_filters_to_label_list:
        resources_to_label_list  { $$ = $1; }
        | filters_to_label_list  { $$ = $1; }
        ;

/*****************************************************************************
 *
 *     QUERY:
 *
 *     DROP RESOURCE LABEL label_name
 *
 *****************************************************************************/
DropPolicyLabelStmt:
        DROP RESOURCE LABEL policy_labels_list
            {
                    DropPolicyLabelStmt *n = makeNode(DropPolicyLabelStmt);
                    n->if_exists = false;
                    n->label_names = $4;
                    $$ = (Node *) n;
            }
        | DROP RESOURCE LABEL IF_P EXISTS policy_labels_list
            {
                DropPolicyLabelStmt *n = makeNode(DropPolicyLabelStmt);
                n->if_exists = true;
                n->label_names = $6;
                $$ = (Node *) n;
            }
        ;

policy_labels_list:
        policy_label_name { $$ = list_make1(makeString($1)); }
        | policy_labels_list ',' policy_label_name { $$ =  lappend($1, makeString($3)); }
        ;

/*****************************************************************************
 *
 *		QUERY:
 *
 *		CREATE RESOURCE POOL pool_name WITH
 *				(
 *					[ MEM_PERCENT = mem_percent, ]
 *					[ CPU_AFFINITY = cpu_affinity, ]
 *				)
 *
 *****************************************************************************/

CreateResourcePoolStmt: CREATE RESOURCE POOL resource_pool_name OptWith
				{
					CreateResourcePoolStmt *n = makeNode(CreateResourcePoolStmt);
					n->pool_name = $4;
					n->options = $5;
					$$ = (Node *)n;
				}
		;

 /*****************************************************************************
 *
 *		QUERY:
 *
 *		ALTER RESOURCE POOL pool_name WITH
 *				(
 *					[ MEM_PERCENT = mem_percent, ]
 *					[ CPU_AFFINITY = cpu_affinity, ]
 *				)
 *
 *****************************************************************************/

AlterResourcePoolStmt: ALTER RESOURCE POOL resource_pool_name OptWith
				{
					AlterResourcePoolStmt *n = makeNode(AlterResourcePoolStmt);
					n->pool_name = $4;
					n->options = $5;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 *		QUERY:
 *				DROP RESOURCE POOL [IF EXISTS] pool_name
 *
 *****************************************************************************/

DropResourcePoolStmt: DROP RESOURCE POOL resource_pool_name
				{
					DropResourcePoolStmt *n = makeNode(DropResourcePoolStmt);
					n->missing_ok = false;
					n->pool_name = $4;
					$$ = (Node *)n;
				}
			| DROP RESOURCE POOL IF_P EXISTS resource_pool_name
				{
					DropResourcePoolStmt *n = makeNode(DropResourcePoolStmt);
					n->missing_ok = true;
					n->pool_name = $6;
					$$ = (Node *)n;
				}
		;

resource_pool_name:
			ColId							{ $$ = $1; };


/*****************************************************************************
 *
 *		QUERY:
 *
 *		CREATE WORKLOAD GROUP group_name USING RESOURCE POOL pool_name WITH
 *				(
 *					[ ACT_STATEMENTS  = act_statements, ]
 *				)
 *
 *****************************************************************************/

CreateWorkloadGroupStmt: CREATE WORKLOAD GROUP_P workload_group_name USING RESOURCE POOL resource_pool_name OptWith
				{
					CreateWorkloadGroupStmt *n = makeNode(CreateWorkloadGroupStmt);
					n->group_name = $4;
					n->pool_name = $8;
					n->options = $9;
					$$ = (Node *)n;
				}
			| CREATE WORKLOAD GROUP_P workload_group_name OptWith
				{
					CreateWorkloadGroupStmt *n = makeNode(CreateWorkloadGroupStmt);
					n->group_name = $4;
					n->pool_name = NULL;
					n->options = $5;
					$$ = (Node *)n;
				}
		;

 /*****************************************************************************
 *
 *		QUERY:
 *
 *		ALTER WORKLOAD GROUP group_name [USING RESOURCE POOL pool_name] WITH
 *				(
 *					[ ACT_STATEMENTS  = act_statements, ]
 *				)
 *
 *****************************************************************************/

AlterWorkloadGroupStmt: ALTER WORKLOAD GROUP_P workload_group_name USING RESOURCE POOL resource_pool_name OptWith
				{
					AlterWorkloadGroupStmt *n = makeNode(AlterWorkloadGroupStmt);
					n->group_name = $4;
					n->pool_name = $8;
					n->options = $9;
					$$ = (Node *)n;
				}
 			| ALTER WORKLOAD GROUP_P workload_group_name OptWith
 				{
 					AlterWorkloadGroupStmt *n = makeNode(AlterWorkloadGroupStmt);
					n->group_name = $4;
					n->pool_name = NULL;
					n->options = $5;
					$$ = (Node *)n;
 				}
		;

/*****************************************************************************
 *
 *		QUERY:
 *				DROP WORKLOAD GROUP [IF EXISTS] group_name
 *
 *****************************************************************************/

DropWorkloadGroupStmt: DROP WORKLOAD GROUP_P workload_group_name
				{
					DropWorkloadGroupStmt *n = makeNode(DropWorkloadGroupStmt);
					n->missing_ok = false;
					n->group_name = $4;
					$$ = (Node *)n;
				}
			| DROP WORKLOAD GROUP_P IF_P EXISTS workload_group_name
				{
					DropWorkloadGroupStmt *n = makeNode(DropWorkloadGroupStmt);
					n->missing_ok = true;
					n->group_name = $6;
					$$ = (Node *)n;
				}
		;

workload_group_name:
			ColId							{ $$ = $1; };


/*****************************************************************************
 *
 *		QUERY:
 *
 *		CREATE APP WORKLOAD GROUP MAPPING app_name WITH
 *				(
 *					[ WORKLOAD_GPNAME  = workload_gpname, ]
 *				)
 *
 *****************************************************************************/

CreateAppWorkloadGroupMappingStmt: CREATE APP WORKLOAD GROUP_P MAPPING application_name OptWith
				{
					CreateAppWorkloadGroupMappingStmt *n = makeNode(CreateAppWorkloadGroupMappingStmt);
					n->app_name = $6;
					n->options = $7;
					$$ = (Node *)n;
				}
		;

 /*****************************************************************************
 *
 *		QUERY:
 *
 *		ALTER APP WORKLOAD GROUP MAPPING app_name WITH
 *				(
 *					[ WORKLOAD_GPNAME  = workload_gpname, ]
 *				)
 *
 *****************************************************************************/

AlterAppWorkloadGroupMappingStmt: ALTER APP WORKLOAD GROUP_P MAPPING application_name OptWith
				{
					AlterAppWorkloadGroupMappingStmt *n = makeNode(AlterAppWorkloadGroupMappingStmt);
					n->app_name = $6;
					n->options = $7;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 *		QUERY:
 *				DROP APP WORKLOAD GROUP MAPPING [IF EXISTS] app_name
 *
 *****************************************************************************/

DropAppWorkloadGroupMappingStmt: DROP APP WORKLOAD GROUP_P MAPPING application_name
				{
					DropAppWorkloadGroupMappingStmt *n = makeNode(DropAppWorkloadGroupMappingStmt);
					n->missing_ok = false;
					n->app_name = $6;
					$$ = (Node *)n;
				}
			| DROP APP WORKLOAD GROUP_P MAPPING IF_P EXISTS application_name
				{
					DropAppWorkloadGroupMappingStmt *n = makeNode(DropAppWorkloadGroupMappingStmt);
					n->missing_ok = true;
					n->app_name = $8;
					$$ = (Node *)n;
				}
		;

application_name:
			ColId							{ $$ = $1; };

/* PGXC_END */

/*****************************************************************************
 *
 *		QUERY:
 *				EXPLAIN [ANALYZE] [VERBOSE] query
 *				EXPLAIN ( options ) query
 *
 *****************************************************************************/

ExplainStmt:
		EXPLAIN ExplainableStmt
				{
					ExplainStmt *n = makeNode(ExplainStmt);
					n->query = $2;
					n->options = NIL;
					$$ = (Node *) n;
				}
		| EXPLAIN PERFORMANCE ExplainableStmt
				{
					ExplainStmt *n = makeNode(ExplainStmt);
					n->query = $3;
					n->options = list_make1(makeDefElem("performance",NULL));
					$$ = (Node *) n;
				}
		| EXPLAIN analyze_keyword opt_verbose ExplainableStmt
				{
					ExplainStmt *n = makeNode(ExplainStmt);
					n->query = $4;
					n->options = list_make1(makeDefElem("analyze", NULL));
					if ($3)
						n->options = lappend(n->options,
											 makeDefElem("verbose", NULL));
					$$ = (Node *) n;
				}
		| EXPLAIN VERBOSE ExplainableStmt
				{
					ExplainStmt *n = makeNode(ExplainStmt);
					n->query = $3;
					n->options = list_make1(makeDefElem("verbose", NULL));
					$$ = (Node *) n;
				}
		| EXPLAIN '(' explain_option_list ')' ExplainableStmt
				{
					ExplainStmt *n = makeNode(ExplainStmt);
					n->query = $5;
					n->options = $3;
					$$ = (Node *) n;
				}
		| EXPLAIN PLAN SET STATEMENT_ID '=' Sconst FOR ExplainableStmt
				{
					ExplainStmt *n = makeNode(ExplainStmt);
					n->statement = makeStringConst($6, @6);
					n->query = $8;
					n->options = list_make1(makeDefElem("plan", NULL));
					$$ = (Node *) n;
				}
		| EXPLAIN PLAN FOR ExplainableStmt
				{
					ExplainStmt *n = makeNode(ExplainStmt);
					n->statement = NULL;
					n->query = $4;
					n->options = list_make1(makeDefElem("plan", NULL));
					$$ = (Node *) n;
				}
		;

ExplainableStmt:
			SelectStmt
			| InsertStmt
			| UpdateStmt
			| DeleteStmt
			| MergeStmt
			| DeclareCursorStmt
			| CreateAsStmt
			| ExecuteStmt					/* by default all are $$=$1 */
		;

explain_option_list:
			explain_option_elem
				{
					$$ = list_make1($1);
				}
			| explain_option_list ',' explain_option_elem
				{
					$$ = lappend($1, $3);
				}
		;

explain_option_elem:
			explain_option_name explain_option_arg
				{
					$$ = makeDefElem($1, $2);
				}
		;

explain_option_name:
			ColId					{ $$ = $1; }
			| analyze_keyword		{ $$ = "analyze"; }
			| VERBOSE				{ $$ = "verbose"; }
		;

explain_option_arg:
			opt_boolean_or_string	{ $$ = (Node *) makeString($1); }
			| NumericOnly			{ $$ = (Node *) $1; }
			| /* EMPTY */			{ $$ = NULL; }
		;

/*****************************************************************************
 *
 *		QUERY:
 *				EXECUTE DIRECT ON ( nodename [, ... ] ) query
 *				EXECUTE DIRECT ON  [ COORDINATORS | DATANODES | ALL ] query
 *
 *****************************************************************************/

ExecDirectStmt: 
		EXECUTE DIRECT ON pgxcnodes DirectStmt
			{
				ExecDirectStmt *n = makeNode(ExecDirectStmt);
			
				n->node_names = $4;
				n->exec_option = EXEC_DIRECT_ON_LIST;
				n->query = $5;
				n->location = @5;
				$$ = (Node *)n;
				parameter_check_execute_direct(n->query);
			}
		| EXECUTE DIRECT ON COORDINATORS DirectStmt
			{
				ExecDirectStmt *n = makeNode(ExecDirectStmt);
				
				n->exec_option = EXEC_DIRECT_ON_ALL_CN;
				n->query = $5;
				n->location = @5;
				$$ = (Node *)n;
				parameter_check_execute_direct(n->query);
			}
		| EXECUTE DIRECT ON DATANODES DirectStmt
			{
				ExecDirectStmt *n = makeNode(ExecDirectStmt);
			
				n->exec_option = EXEC_DIRECT_ON_ALL_DN;
				n->query = $5;
				n->location = @5;
				$$ = (Node *)n;
				parameter_check_execute_direct(n->query);
			}
		| EXECUTE DIRECT ON ALL DirectStmt
			{
				ExecDirectStmt *n = makeNode(ExecDirectStmt);
				
				n->exec_option = EXEC_DIRECT_ON_ALL_NODES;
				n->query = $5;
				n->location = @5;
				$$ = (Node *)n;
				parameter_check_execute_direct(n->query);
			}
		;

DirectStmt:
			Sconst					/* by default all are $$=$1 */
		;

/*****************************************************************************
 *
 *		QUERY:
 *
 *		CLEAN CONNECTION TO { COORDINATOR ( nodename ) | NODE ( nodename ) | ALL [ CHECK ] [ FORCE ] }
 *				[ FOR DATABASE dbname ]
 *				[ TO USER username ]
 *
 *****************************************************************************/

CleanConnStmt: CLEAN CONNECTION TO COORDINATOR pgxcnodes CleanConnDbName CleanConnUserName
				{
					CleanConnStmt *n = makeNode(CleanConnStmt);
					n->is_coord = true;
					n->nodes = $5;
					n->is_check = false;
					n->is_force = false;
					n->dbname = $6;
					n->username = $7;
					$$ = (Node *)n;
				}
				| CLEAN CONNECTION TO NODE pgxcnodes CleanConnDbName CleanConnUserName
				{
					CleanConnStmt *n = makeNode(CleanConnStmt);
					n->is_coord = false;
					n->nodes = $5;
					n->is_check = false;
					n->is_force = false;
					n->dbname = $6;
					n->username = $7;
					$$ = (Node *)n;
				}
				| CLEAN CONNECTION TO ALL opt_check opt_force CleanConnDbName CleanConnUserName
				{
					CleanConnStmt *n = makeNode(CleanConnStmt);
					n->is_coord = true;
					n->nodes = NIL;
					n->is_check = $5;
					n->is_force = $6;
					n->dbname = $7;
					n->username = $8;
					$$ = (Node *)n;
				}
		;

CleanConnDbName: FOR DATABASE database_name		{ $$ = $3; }
				| FOR database_name				{ $$ = $2; }
				| /* EMPTY */					{ $$ = NULL; }
		;

CleanConnUserName: TO USER RoleId				{ $$ = $3; }
				| TO RoleId						{ $$ = $2; }
				| /* EMPTY */					{ $$ = NULL; }
		;

opt_check:	CHECK								{  $$ = TRUE; }
			| /* EMPTY */						{  $$ = FALSE; }
		;
/* PGXC_END */

/*****************************************************************************
 *
 *		QUERY:
 *				PREPARE <plan_name> [(args, ...)] AS <query>
 *
 *****************************************************************************/

PrepareStmt: PREPARE name prep_type_clause AS PreparableStmt
				{
					PrepareStmt *n = makeNode(PrepareStmt);
					n->name = $2;
					n->argtypes = $3;
					n->query = $5;
					$$ = (Node *) n;
				}
		;

prep_type_clause: '(' type_list ')'			{ $$ = $2; }
				| /* EMPTY */				{ $$ = NIL; }
		;

PreparableStmt:
			SelectStmt
			| InsertStmt
			| UpdateStmt
			| DeleteStmt					/* by default all are $$=$1 */
			| MergeStmt
		;

/*****************************************************************************
 *
 * EXECUTE <plan_name> [(params, ...)]
 * CREATE TABLE <name> AS EXECUTE <plan_name> [(params, ...)]
 *
 *****************************************************************************/

ExecuteStmt: EXECUTE name execute_param_clause
				{
					ExecuteStmt *n = makeNode(ExecuteStmt);
					n->name = $2;
					n->params = $3;
					$$ = (Node *) n;
				}
			| CREATE OptTemp TABLE create_as_target AS
				EXECUTE name execute_param_clause opt_with_data
				{
					CreateTableAsStmt *ctas = makeNode(CreateTableAsStmt);
					ExecuteStmt *n = makeNode(ExecuteStmt);
					n->name = $7;
					n->params = $8;
					ctas->query = (Node *) n;
					ctas->into = $4;
					ctas->relkind = OBJECT_TABLE;
					ctas->is_select_into = false;
					/* cram additional flags into the IntoClause */
					$4->rel->relpersistence = $2;
#ifdef PGXC
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("CREATE TABLE AS EXECUTE not yet supported")));
#endif
					$4->skipData = !($9);
					$$ = (Node *) ctas;
				}
		;

execute_param_clause: '(' expr_list ')'				{ $$ = $2; }
					| /* EMPTY */					{ $$ = NIL; }
					;

/*****************************************************************************
 *
 *		QUERY:
 *				DEALLOCATE [PREPARE] <plan_name>
 *
 *****************************************************************************/

DeallocateStmt: DEALLOCATE name
					{
						DeallocateStmt *n = makeNode(DeallocateStmt);
						n->name = $2;
						$$ = (Node *) n;
					}
				| DEALLOCATE PREPARE name
					{
						DeallocateStmt *n = makeNode(DeallocateStmt);
						n->name = $3;
						$$ = (Node *) n;
					}
				| DEALLOCATE ALL
					{
						DeallocateStmt *n = makeNode(DeallocateStmt);
						n->name = NULL;
						$$ = (Node *) n;
					}
				| DEALLOCATE PREPARE ALL
					{
						DeallocateStmt *n = makeNode(DeallocateStmt);
						n->name = NULL;
						$$ = (Node *) n;
					}
		;

/*****************************************************************************
 *
 *		QUERY:
 *				INSERT STATEMENTS
 *
 *****************************************************************************/

InsertStmt: opt_with_clause INSERT hint_string INTO qualified_name insert_rest returning_clause
			{
				$6->relation = $5;
				$6->returningList = $7;
				$6->withClause = $1;
				$6->hintState = create_hintstate($3);
				$$ = (Node *) $6;
			}
			| opt_with_clause INSERT hint_string INTO qualified_name insert_rest upsert_clause returning_clause
				{
					if ($8 != NIL) {
						ereport(ERROR,
							(errmodule(MOD_PARSER),
							  errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							  errmsg("RETURNING clause is not yet supported whithin INSERT ON DUPLICATE KEY UPDATE statement.")));
					}
					if ($1 != NULL) {
						ereport(ERROR,
							(errmodule(MOD_PARSER),
							 errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("WITH clause is not yet supported whithin INSERT ON DUPLICATE KEY UPDATE statement.")));
					}

					if (u_sess->attr.attr_sql.enable_upsert_to_merge
#ifdef ENABLE_MULTIPLE_NODES					
					    ||t_thrd.proc->workingVersionNum < UPSERT_ROW_STORE_VERSION_NUM
#endif						
					    ) {

						if ($6 != NULL && $6->cols != NIL) {
							ListCell *c = NULL;
							List *cols = $6->cols;
							foreach (c, cols) {
								ResTarget *rt = (ResTarget *)lfirst(c);
								if (rt->indirection != NIL) {
									ereport(ERROR,
										(errmodule(MOD_PARSER),
										 errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
										 errmsg("Subfield name or array subscript of column \"%s\" "
											"is not yet supported whithin INSERT ON DUPLICATE KEY UPDATE statement.",
											rt->name),
										 errhint("Try assign a composite or an array expression to column \"%s\".", rt->name)));
								}
							}
						}

						MergeStmt *m = makeNode(MergeStmt);
						m->is_insert_update = true;

						/* for UPSERT, keep the INSERT statement as well */
						$6->relation = $5;
						$6->returningList = $8;
						$6->withClause = $1;
#ifdef ENABLE_MULTIPLE_NODES						
						if (t_thrd.proc->workingVersionNum >= UPSERT_ROW_STORE_VERSION_NUM) {
							UpsertClause *uc = makeNode(UpsertClause);
							if ($7 == NULL)
								uc->targetList = NIL;
							else
								uc->targetList = ((MergeWhenClause *)$7)->targetList;
							$6->upsertClause = uc;
						}
#endif						
						m->insert_stmt = (Node *)copyObject($6);

						/* fill a MERGE statement*/
						m->relation = $5;

						Alias *a1 = makeAlias(($5->relname), NIL);
						$5->alias = a1;

						Alias *a2 = makeAlias("excluded", NIL);
						RangeSubselect *r = makeNode(RangeSubselect);
						r->alias = a2;
						r->subquery = (Node *) ($6->selectStmt);
						m->source_relation = (Node *) r;

						MergeWhenClause *n = makeNode(MergeWhenClause);
						n->matched = false;
						n->commandType = CMD_INSERT;
						n->cols = $6->cols;
						n->values = NULL;

						m->mergeWhenClauses = list_make1((Node *) n);
						if ($7 != NULL)
							m->mergeWhenClauses = list_concat(list_make1($7), m->mergeWhenClauses);

						m->hintState = create_hintstate($3);

						$$ = (Node *)m;
					} else {
						$6->relation = $5;
						$6->returningList = $8;
						$6->withClause = $1;
						$6->upsertClause = (UpsertClause *)$7;
						$6->hintState = create_hintstate($3);
						$$ = (Node *) $6;
					}
				}
		;

insert_rest:
			SelectStmt
				{
					$$ = makeNode(InsertStmt);
					$$->cols = NIL;
					$$->selectStmt = $1;
					$$->isRewritten = false;
				}
			| '(' insert_column_list ')' SelectStmt
				{
					$$ = makeNode(InsertStmt);
					$$->cols = $2;
					$$->selectStmt = $4;
					$$->isRewritten = false;
				}
			| DEFAULT VALUES
				{
					$$ = makeNode(InsertStmt);
					$$->cols = NIL;
					$$->selectStmt = NULL;
					$$->isRewritten = false;
				}
		;

insert_column_list:
			insert_column_item
					{ $$ = list_make1($1); }
			| insert_column_list ',' insert_column_item
					{ $$ = lappend($1, $3); }
		;

insert_column_item:
			ColId opt_indirection
				{
					$$ = makeNode(ResTarget);
					$$->name = $1;
					$$->indirection = check_indirection($2, yyscanner);
					$$->val = NULL;
					$$->location = @1;
				}
		;

returning_clause:
			RETURNING target_list		{ $$ = $2; }
			| /* EMPTY */				{ $$ = NIL; }
		;

upsert_clause:
			ON DUPLICATE KEY UPDATE set_clause_list
				{
					if (u_sess->attr.attr_sql.enable_upsert_to_merge
#ifdef ENABLE_MULTIPLE_NODES					
					    || t_thrd.proc->workingVersionNum < UPSERT_ROW_STORE_VERSION_NUM
#endif					    
						) {
						MergeWhenClause *n = makeNode(MergeWhenClause);
						n->matched = true;
						n->commandType = CMD_UPDATE;
						n->targetList = $5;
						$$ = (Node *) n;
					} else {
						/* check subquery in set clause*/
						ListCell* cell = NULL;
						ResTarget* res = NULL;
						foreach (cell, $5) {
							res = (ResTarget*)lfirst(cell);
							if (IsA(res->val,SubLink)) {
								ereport(ERROR,
									(errmodule(MOD_PARSER),
									  errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									  errmsg("Update with subquery is not yet supported whithin INSERT ON DUPLICATE KEY UPDATE statement.")));
							}
						}

						UpsertClause *uc = makeNode(UpsertClause);
						uc->targetList = $5;
						uc->location = @1;
						$$ = (Node *) uc;
					}
				}
			| ON DUPLICATE KEY UPDATE NOTHING
				{
					if (unlikely(u_sess->attr.attr_sql.enable_upsert_to_merge ||
						t_thrd.proc->workingVersionNum < UPSERT_ROW_STORE_VERSION_NUM)) {
						$$ = NULL;
					} else {
						UpsertClause *uc = makeNode(UpsertClause);
						uc->targetList = NIL;
						uc->location = @1;
						$$ = (Node *) uc;
					}
				}
		;

/*****************************************************************************
 *
 *		QUERY:
 *				DELETE STATEMENTS
 *
 *****************************************************************************/

DeleteStmt: opt_with_clause DELETE_P hint_string FROM relation_expr_opt_alias
			using_clause where_or_current_clause opt_delete_limit returning_clause
				{
					DeleteStmt *n = makeNode(DeleteStmt);
					n->relation = $5;
					n->usingClause = $6;
					n->whereClause = $7;
					n->limitClause = (Node*)list_nth($8, 1);
					n->returningList = $9;
					n->withClause = $1;
					n->hintState = create_hintstate($3);					
					$$ = (Node *)n;
				}
		| opt_with_clause DELETE_P hint_string relation_expr_opt_alias
			using_clause where_or_current_clause opt_delete_limit returning_clause
				{
					DeleteStmt *n = makeNode(DeleteStmt);
					n->relation = $4;
					n->usingClause = $5;
					n->whereClause = $6;
					n->limitClause = (Node*)list_nth($7, 1);
					n->returningList = $8;
					n->withClause = $1;
					n->hintState = create_hintstate($3);				
					$$ = (Node *)n;
				}
		;

using_clause:
				USING from_list						{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;


/*****************************************************************************
 *
 *		QUERY:
 *				LOCK TABLE
 *
 *****************************************************************************/

LockStmt:	LOCK_P opt_table relation_expr_list opt_lock opt_nowait
				{
					LockStmt *n = makeNode(LockStmt);

					n->relations = $3;
					n->mode = $4;
					n->nowait = $5;
					$$ = (Node *)n;
				}
		;

opt_lock:	IN_P lock_type MODE				{ $$ = $2; }
			| /*EMPTY*/						{ $$ = AccessExclusiveLock; }
		;

lock_type:	ACCESS SHARE					{ $$ = AccessShareLock; }
			| ROW SHARE						{ $$ = RowShareLock; }
			| ROW EXCLUSIVE					{ $$ = RowExclusiveLock; }
			| SHARE UPDATE EXCLUSIVE		{ $$ = ShareUpdateExclusiveLock; }
			| SHARE							{ $$ = ShareLock; }
			| SHARE ROW EXCLUSIVE			{ $$ = ShareRowExclusiveLock; }
			| EXCLUSIVE						{ $$ = ExclusiveLock; }
			| ACCESS EXCLUSIVE				{ $$ = AccessExclusiveLock; }
		;

opt_nowait:	NOWAIT							{ $$ = TRUE; }
			| /*EMPTY*/						{ $$ = FALSE; }
		;


/*****************************************************************************
 *
 *		QUERY:
 *				UpdateStmt (UPDATE)
 *
 *****************************************************************************/

UpdateStmt: opt_with_clause UPDATE hint_string relation_expr_opt_alias
			SET set_clause_list
			from_clause
			where_or_current_clause
			returning_clause
				{
					UpdateStmt *n = makeNode(UpdateStmt);
					n->relation = $4;
					n->targetList = $6;
					n->fromClause = $7;
					n->whereClause = $8;
					n->returningList = $9;
					n->withClause = $1;
					n->hintState = create_hintstate($3);
					$$ = (Node *)n;
				}
		;

set_clause_list:
			set_clause							{ $$ = $1; }
			| set_clause_list ',' set_clause	{ $$ = list_concat($1,$3); }
		;

set_clause:
			single_set_clause						{ $$ = list_make1($1); }
			| multiple_set_clause					{ $$ = $1; }
		;

single_set_clause:
			set_target '=' ctext_expr
				{
					$$ = $1;
					$$->val = (Node *) $3;
				}
			/* this is only used in ON DUPLICATE KEY UPDATE col = VALUES(col) case
			 * for mysql compatibility
			 */
			| set_target '=' VALUES '(' columnref ')'
				{
					ColumnRef *c = NULL;
					int nfields = 0;
					if (IsA($5, ColumnRef))
						c = (ColumnRef *) $5;
					else if (IsA($5, A_Indirection))
						c = (ColumnRef *)(((A_Indirection *)$5)->arg);
					nfields = list_length(c->fields);
					/* only allow col.*, col[...], col */
					if (nfields > 1)
					{
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("only allow column name within VALUES"),
								 parser_errposition(@5)));
					}

					c->fields = lcons((Node *)makeString("excluded"), c->fields);
					$$ = $1;
					$$->val = (Node *) $5;
				}
		;

multiple_set_clause:
			'(' set_target_list ')' '=' ctext_row
				{
					ListCell *col_cell;
					ListCell *val_cell;

					/*
					 * Break the ctext_row apart, merge individual expressions
					 * into the destination ResTargets.  XXX this approach
					 * cannot work for general row expressions as sources.
					 */
					if (list_length($2) != list_length($5))
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("number of columns does not match number of values"),
								 parser_errposition(@1)));
					forboth(col_cell, $2, val_cell, $5)
					{
						ResTarget *res_col = (ResTarget *) lfirst(col_cell);
						Node *res_val = (Node *) lfirst(val_cell);

						res_col->val = res_val;
					}

					$$ = $2;
				}
			|	'(' set_target_list ')' '=' '(' SELECT hint_string opt_distinct target_list
					from_clause where_clause group_clause having_clause ')'
					{
						SelectStmt *select = makeNode(SelectStmt);
						select->distinctClause = $8;
						select->targetList = $9;
						select->intoClause = NULL;
						select->fromClause = $10;
						select->whereClause = $11;
						select->groupClause = $12;
						select->havingClause = $13;

						if (list_length($2) != list_length($9))
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									errmsg("number of columns does not match number of values")));

						ParseUpdateMultiSet($2, select, yyscanner);

						$$ = $2;
					}

		;

set_target:
			ColId opt_indirection
				{
					$$ = makeNode(ResTarget);
					$$->name = $1;
					$$->indirection = check_indirection($2, yyscanner);
					$$->val = NULL;	/* upper production sets this */
					$$->location = @1;
				}
		;

set_target_list:
			set_target								{ $$ = list_make1($1); }
			| set_target_list ',' set_target		{ $$ = lappend($1,$3); }
		;

/*****************************************************************************
 *
 *		QUERY:
 *				MERGE STATEMENTS
 *
 *****************************************************************************/
MergeStmt:
			MERGE hint_string INTO relation_expr_opt_alias
			USING table_ref
			ON a_expr
			merge_when_list
				{
					MergeStmt *m = makeNode(MergeStmt);

					m->relation = $4;
					m->source_relation = $6;
					m->join_condition = $8;
					m->mergeWhenClauses = $9;

					m->hintState = create_hintstate($2);

					$$ = (Node *)m;
				}
			;

merge_when_list:
			merge_when_clause						{ $$ = list_make1($1); }
			| merge_when_list merge_when_clause 	{ $$ = lappend($1,$2); }
			;

merge_when_clause:
			WHEN MATCHED THEN merge_update opt_merge_where_condition
				{
					$4->matched = true;
					$4->commandType = CMD_UPDATE;
					$4->condition = $5;

					$$ = (Node *) $4;
				}
			| WHEN NOT MATCHED THEN merge_insert opt_merge_where_condition
				{
					$5->matched = false;
					$5->commandType = CMD_INSERT;
					$5->condition = $6;

					$$ = (Node *) $5;
				}
			;

opt_merge_where_condition:
			WHERE a_expr			{ $$ = $2; }
			|						{ $$ = NULL; }
			;

merge_update:
			UPDATE SET set_clause_list
				{
					MergeWhenClause *n = makeNode(MergeWhenClause);
					n->targetList = $3;

					$$ = n;
				}
			;

merge_insert:
			INSERT merge_values_clause
				{
					MergeWhenClause *n = makeNode(MergeWhenClause);
					n->cols = NIL;
					n->values = $2;

					$$ = n;
				}
			| INSERT '(' insert_column_list ')' merge_values_clause
				{
					MergeWhenClause *n = makeNode(MergeWhenClause);
					n->cols = $3;
					n->values = $5;

					$$ = n;
				}
			| INSERT DEFAULT VALUES
				{
					MergeWhenClause *n = makeNode(MergeWhenClause);
					n->cols = NIL;
					n->values = NIL;
					$$ = n;
				}
			;

merge_values_clause:
			VALUES ctext_row
				{
					$$ = $2;
				}
			;

/*****************************************************************************
 *
 *		QUERY:
 *				CURSOR STATEMENTS
 *
 *****************************************************************************/
DeclareCursorStmt: CURSOR cursor_name cursor_options opt_hold FOR SelectStmt
				{
					DeclareCursorStmt *n = makeNode(DeclareCursorStmt);
					n->portalname = $2;
					/* currently we always set FAST_PLAN option */
					n->options = $3 | $4 | CURSOR_OPT_FAST_PLAN;
					n->query = $6;
					$$ = (Node *)n;
				}
			| DECLARE_CURSOR cursor_name cursor_options CURSOR opt_hold FOR SelectStmt
			{
				DeclareCursorStmt *n = makeNode(DeclareCursorStmt);
				n->portalname = $2;
				/* currently we always set FAST_PLAN option */
				n->options = $3 | $5 | CURSOR_OPT_FAST_PLAN;
				n->query = $7;
				$$ = (Node *)n;
			}
	;

cursor_name:	name						{ $$ = $1; }
		;

cursor_options: /*EMPTY*/					{ $$ = 0; }
			| cursor_options NO SCROLL		{ $$ = $1 | CURSOR_OPT_NO_SCROLL; }
			| cursor_options SCROLL
				{
					ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("SCROLL CURSOR is not yet supported.")));
					$$ = $1 | CURSOR_OPT_SCROLL;
				}
			| cursor_options BINARY			{ $$ = $1 | CURSOR_OPT_BINARY; }
			| cursor_options INSENSITIVE
				{
					ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("INSENSITIVE CURSOR is not yet supported.")));
					$$ = $1 | CURSOR_OPT_INSENSITIVE;
				}
		;

opt_hold: /* EMPTY */						{ $$ = 0; }
			| WITH HOLD				{$$ = CURSOR_OPT_HOLD; }
			| WITHOUT HOLD				{ $$ = 0; }
		;

/*****************************************************************************
 *
 *		QUERY:
 *				SELECT STATEMENTS
 *
 *****************************************************************************/

/* A complete SELECT statement looks like this.
 *
 * The rule returns either a single SelectStmt node or a tree of them,
 * representing a set-operation tree.
 *
 * There is an ambiguity when a sub-SELECT is within an a_expr and there
 * are excess parentheses: do the parentheses belong to the sub-SELECT or
 * to the surrounding a_expr?  We don't really care, but bison wants to know.
 * To resolve the ambiguity, we are careful to define the grammar so that
 * the decision is staved off as long as possible: as long as we can keep
 * absorbing parentheses into the sub-SELECT, we will do so, and only when
 * it's no longer possible to do that will we decide that parens belong to
 * the expression.	For example, in "SELECT (((SELECT 2)) + 3)" the extra
 * parentheses are treated as part of the sub-select.  The necessity of doing
 * it that way is shown by "SELECT (((SELECT 2)) UNION SELECT 2)".	Had we
 * parsed "((SELECT 2))" as an a_expr, it'd be too late to go back to the
 * SELECT viewpoint when we see the UNION.
 *
 * This approach is implemented by defining a nonterminal select_with_parens,
 * which represents a SELECT with at least one outer layer of parentheses,
 * and being careful to use select_with_parens, never '(' SelectStmt ')',
 * in the expression grammar.  We will then have shift-reduce conflicts
 * which we can resolve in favor of always treating '(' <select> ')' as
 * a select_with_parens.  To resolve the conflicts, the productions that
 * conflict with the select_with_parens productions are manually given
 * precedences lower than the precedence of ')', thereby ensuring that we
 * shift ')' (and then reduce to select_with_parens) rather than trying to
 * reduce the inner <select> nonterminal to something else.  We use UMINUS
 * precedence for this, which is a fairly arbitrary choice.
 *
 * To be able to define select_with_parens itself without ambiguity, we need
 * a nonterminal select_no_parens that represents a SELECT structure with no
 * outermost parentheses.  This is a little bit tedious, but it works.
 *
 * In non-expression contexts, we use SelectStmt which can represent a SELECT
 * with or without outer parentheses.
 */

SelectStmt: select_no_parens			%prec UMINUS
			| select_with_parens		%prec UMINUS
		;

select_with_parens:
			'(' select_no_parens ')'				{ $$ = $2; }
			| '(' select_with_parens ')'			{ $$ = $2; }
		;

/*
 * This rule parses the equivalent of the standard's <query expression>.
 * The duplicative productions are annoying, but hard to get rid of without
 * creating shift/reduce conflicts.
 *
 *	FOR UPDATE/SHARE may be before or after LIMIT/OFFSET.
 *	In <=7.2.X, LIMIT/OFFSET had to be after FOR UPDATE
 *	We now support both orderings, but prefer LIMIT/OFFSET before FOR UPDATE/SHARE
 *	2002-08-28 bjm
 */
select_no_parens:
			simple_select						{ $$ = $1; }
			| select_clause sort_clause
				{
					insertSelectOptions((SelectStmt *) $1, $2, NIL,
										NULL, NULL, NULL,
										yyscanner);
					$$ = $1;
				}
			| select_clause opt_sort_clause for_locking_clause opt_select_limit
				{
					insertSelectOptions((SelectStmt *) $1, $2, $3,
										(Node*)list_nth($4, 0), (Node*)list_nth($4, 1),
										NULL,
										yyscanner);
					$$ = $1;
				}
			| select_clause opt_sort_clause select_limit opt_for_locking_clause
				{
					insertSelectOptions((SelectStmt *) $1, $2, $4,
										(Node*)list_nth($3, 0), (Node*)list_nth($3, 1),
										NULL,
										yyscanner);
					$$ = $1;
				}
			| with_clause select_clause
				{
					insertSelectOptions((SelectStmt *) $2, NULL, NIL,
										NULL, NULL,
										$1,
										yyscanner);
					$$ = $2;
				}
			| with_clause select_clause sort_clause
				{
					insertSelectOptions((SelectStmt *) $2, $3, NIL,
										NULL, NULL,
										$1,
										yyscanner);
					$$ = $2;
				}
			| with_clause select_clause opt_sort_clause for_locking_clause opt_select_limit
				{
					insertSelectOptions((SelectStmt *) $2, $3, $4,
										(Node*)list_nth($5, 0), (Node*)list_nth($5, 1),
										$1,
										yyscanner);
					$$ = $2;
				}
			| with_clause select_clause opt_sort_clause select_limit opt_for_locking_clause
				{
					insertSelectOptions((SelectStmt *) $2, $3, $5,
										(Node*)list_nth($4, 0), (Node*)list_nth($4, 1),
										$1,
										yyscanner);
					$$ = $2;
				}
		;

select_clause:
			simple_select							{ $$ = $1; }
			| select_with_parens					{ $$ = $1; }
		;

/*
 * This rule parses SELECT statements that can appear within set operations,
 * including UNION, INTERSECT and EXCEPT.  '(' and ')' can be used to specify
 * the ordering of the set operations.	Without '(' and ')' we want the
 * operations to be ordered per the precedence specs at the head of this file.
 *
 * As with select_no_parens, simple_select cannot have outer parentheses,
 * but can have parenthesized subclauses.
 *
 * Note that sort clauses cannot be included at this level --- SQL92 requires
 *		SELECT foo UNION SELECT bar ORDER BY baz
 * to be parsed as
 *		(SELECT foo UNION SELECT bar) ORDER BY baz
 * not
 *		SELECT foo UNION (SELECT bar ORDER BY baz)
 * Likewise for WITH, FOR UPDATE and LIMIT.  Therefore, those clauses are
 * described as part of the select_no_parens production, not simple_select.
 * This does not limit functionality, because you can reintroduce these
 * clauses inside parentheses.
 *
 * NOTE: only the leftmost component SelectStmt should have INTO.
 * However, this is not checked by the grammar; parse analysis must check it.
 */
simple_select:
			SELECT hint_string opt_distinct target_list
			into_clause from_clause where_clause
			group_clause having_clause window_clause
				{
					SelectStmt *n = makeNode(SelectStmt);
					n->distinctClause = $3;
					n->targetList = $4;
					n->intoClause = $5;
					n->fromClause = $6;
					n->whereClause = $7;
					n->groupClause = $8;
					n->havingClause = $9;
					n->windowClause = $10;
					n->hintState = create_hintstate($2);
					n->hasPlus = getOperatorPlusFlag();
					$$ = (Node *)n;
				}
			| values_clause							{ $$ = $1; }
			| TABLE relation_expr
				{
					/* same as SELECT * FROM relation_expr */
					ColumnRef *cr = makeNode(ColumnRef);
					ResTarget *rt = makeNode(ResTarget);
					SelectStmt *n = makeNode(SelectStmt);

					cr->fields = list_make1(makeNode(A_Star));
					cr->location = -1;

					rt->name = NULL;
					rt->indirection = NIL;
					rt->val = (Node *)cr;
					rt->location = -1;

					n->targetList = list_make1(rt);
					n->fromClause = list_make1($2);
					$$ = (Node *)n;
				}
			| select_clause UNION opt_all select_clause
				{
					$$ = makeSetOp(SETOP_UNION, $3, $1, $4);
				}
			| select_clause INTERSECT opt_all select_clause
				{
					$$ = makeSetOp(SETOP_INTERSECT, $3, $1, $4);
				}
			| select_clause EXCEPT opt_all select_clause
				{
					$$ = makeSetOp(SETOP_EXCEPT, $3, $1, $4);
				}
			| select_clause MINUS_P opt_all select_clause
				{
					$$ = makeSetOp(SETOP_EXCEPT, $3, $1, $4);
				}
		;

hint_string:
		COMMENTSTRING
			{
				$$ = $1;
			}
		|
			{ 
				$$ = NULL;
			}
		;
/*
 * SQL standard WITH clause looks like:
 *
 * WITH [ RECURSIVE ] <query name> [ (<column>,...) ]
 *		AS (query) [ SEARCH or CYCLE clause ]
 *
 * We don't currently support the SEARCH or CYCLE clause.
 */
with_clause:
		WITH cte_list
			{
				$$ = makeNode(WithClause);
				$$->ctes = $2;
				$$->recursive = false;
				$$->location = @1;
			}
		| WITH RECURSIVE cte_list
			{
				$$ = makeNode(WithClause);
				$$->ctes = $3;
				$$->recursive = true;
				$$->location = @1;
			}
		;

cte_list:
		common_table_expr						{ $$ = list_make1($1); }
		| cte_list ',' common_table_expr		{ $$ = lappend($1, $3); }
		;

common_table_expr:  name opt_name_list AS '(' PreparableStmt ')'
			{
				CommonTableExpr *n = makeNode(CommonTableExpr);
				n->ctename = $1;
				n->aliascolnames = $2;
				n->ctequery = $5;
				n->location = @1;
				n->locator_type = LOCATOR_TYPE_NONE;
				$$ = (Node *) n;
			}
		;

opt_with_clause:
		with_clause								{ $$ = $1; }
		| /*EMPTY*/								{ $$ = NULL; }
		;

into_clause:
			INTO OptTempTableName
				{
					$$ = makeNode(IntoClause);
					$$->rel = $2;
					$$->colNames = NIL;
					$$->options = NIL;
					$$->onCommit = ONCOMMIT_NOOP;
					/* Here $$ is a temp table, so row_compress can be any value. To be safe, REL_CMPRS_PAGE_PLAIN is used. */
					$$->row_compress = REL_CMPRS_PAGE_PLAIN;
					$$->tableSpaceName = NULL;
					$$->skipData = false;
					$$->relkind = INTO_CLAUSE_RELKIND_DEFAULT;
				}
			| /*EMPTY*/
				{ $$ = NULL; }
		;

/*
 * Redundancy here is needed to avoid shift/reduce conflicts,
 * since TEMP is not a reserved word.  See also OptTemp.
 */
OptTempTableName:
			TEMPORARY opt_table qualified_name
				{
					$$ = $3;
					$$->relpersistence = RELPERSISTENCE_TEMP;
				}
			| TEMP opt_table qualified_name
				{
					$$ = $3;
					$$->relpersistence = RELPERSISTENCE_TEMP;
				}
			| LOCAL TEMPORARY opt_table qualified_name
				{
					$$ = $4;
					$$->relpersistence = RELPERSISTENCE_TEMP;
				}
			| LOCAL TEMP opt_table qualified_name
				{
					$$ = $4;
					$$->relpersistence = RELPERSISTENCE_TEMP;
				}
			| GLOBAL TEMPORARY opt_table qualified_name
				{
					$$ = $4;
#ifdef ENABLE_MULTIPLE_NODES
					ereport(WARNING,
							(errmsg("GLOBAL is deprecated in temporary table creation"),
							 parser_errposition(@1)));
					$$->relpersistence = RELPERSISTENCE_TEMP;
#else
					$$->relpersistence = RELPERSISTENCE_GLOBAL_TEMP;
#endif
				}
			| GLOBAL TEMP opt_table qualified_name
				{
					$$ = $4;
#ifdef ENABLE_MULTIPLE_NODES
					ereport(WARNING,
							(errmsg("GLOBAL is deprecated in temporary table creation"),
							 parser_errposition(@1)));
					$$->relpersistence = RELPERSISTENCE_TEMP;
#else
					$$->relpersistence = RELPERSISTENCE_GLOBAL_TEMP;
#endif
				}
			| UNLOGGED opt_table qualified_name
				{
					$$ = $3;
					$$->relpersistence = RELPERSISTENCE_UNLOGGED;
				}
			| TABLE qualified_name
				{
					$$ = $2;
					$$->relpersistence = RELPERSISTENCE_PERMANENT;
				}
			| qualified_name
				{
					$$ = $1;
					$$->relpersistence = RELPERSISTENCE_PERMANENT;
				}
		;

opt_table:	TABLE									{}
			| /*EMPTY*/								{}
		;

opt_all:	ALL										{ $$ = TRUE; }
			| DISTINCT								{ $$ = FALSE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

/* We use (NIL) as a placeholder to indicate that all target expressions
 * should be placed in the DISTINCT list during parsetree analysis.
 */
opt_distinct:
			DISTINCT								{ $$ = list_make1(NIL); }
			| DISTINCT ON '(' expr_list ')'			{ $$ = $4; }
			| ALL									{ $$ = NIL; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

opt_sort_clause:
			sort_clause								{ $$ = $1;}
			| /*EMPTY*/								{ $$ = NIL; }
		;

sort_clause:
			ORDER BY sortby_list					{ $$ = $3; }
		;

sortby_list:
			sortby									{ $$ = list_make1($1); }
			| sortby_list ',' sortby				{ $$ = lappend($1, $3); }
		;

sortby:		a_expr USING qual_all_Op opt_nulls_order
				{
					$$ = makeNode(SortBy);
					$$->node = $1;
					$$->sortby_dir = SORTBY_USING;
					$$->sortby_nulls = (SortByNulls)$4;
					$$->useOp = $3;
					$$->location = @3;
				}
			| a_expr opt_asc_desc opt_nulls_order
				{
					$$ = makeNode(SortBy);
					$$->node = $1;
					$$->sortby_dir = (SortByDir)$2;
					$$->sortby_nulls = (SortByNulls)$3;
					$$->useOp = NIL;
					$$->location = -1;		/* no operator */
				}
			| NLSSORT '(' a_expr ',' Sconst ')' opt_asc_desc opt_nulls_order
				{
					if (checkNlssortArgs($5))
				    {
						Node  *c = NULL;
						FuncCall *n = makeNode(FuncCall);
						c = $3;

						n->funcname = SystemFuncName("convert_to_nocase");
						n->args =list_make2(c,makeStringConst("gbk",-1));
						n->agg_order = NIL;
						n->agg_star = FALSE;
						n->agg_distinct = FALSE;
						n->func_variadic = FALSE;
						n->over = NULL;
						n->location = @1;
						n->call_func = false;

						$$ = makeNode(SortBy);
						$$->node = (Node*)n;
						$$->sortby_dir = (SortByDir)$7;
						$$->sortby_nulls = (SortByNulls)$8;
						$$->useOp = NIL;
						$$->location = @1;
					}
					else
					{
						$$ = NULL;
						ereport(ERROR,(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
									errmsg("Sort method %s  is not supported!",$5)));
					}
				}
		;


select_limit:
			limit_clause offset_clause				{ $$ = list_make2($2, $1); }
			| offset_clause limit_clause				{ $$ = list_make2($1, $2); }
			| limit_clause						{ $$ = list_make2(NULL, $1); }
			| limit_offcnt_clause					{ $$ = $1; }
			| offset_clause						{ $$ = list_make2($1, NULL); }
		;

opt_select_limit:
			select_limit						{ $$ = $1; }
			| /* EMPTY */						{ $$ = list_make2(NULL,NULL); }
		;

opt_delete_limit:
			LIMIT a_expr						{ $$ = list_make2(NULL, $2); }
			| /* EMPTY */						{ $$ = list_make2(NULL, NULL); }


limit_clause:
			LIMIT select_limit_value
				{ $$ = $2; }
			/* SQL:2008 syntax */
			| FETCH first_or_next opt_select_fetch_first_value row_or_rows ONLY
				{ $$ = $3; }
		;

limit_offcnt_clause:
			LIMIT select_offset_value ',' select_limit_value
				{
					$$ = list_make2($2, $4);
				}
		;

offset_clause:
			OFFSET select_offset_value
				{ $$ = $2; }
			/* SQL:2008 syntax */
			| OFFSET select_offset_value2 row_or_rows
				{ $$ = $2; }
		;

select_limit_value:
			a_expr									{ $$ = $1; }
			| ALL
				{
					/* LIMIT ALL is represented as a NULL constant */
					$$ = makeNullAConst(@1);
				}
		;

select_offset_value:
			a_expr									{ $$ = $1; }
		;

/*
 * Allowing full expressions without parentheses causes various parsing
 * problems with the trailing ROW/ROWS key words.  SQL only calls for
 * constants, so we allow the rest only with parentheses.  If omitted,
 * default to 1.
 */
opt_select_fetch_first_value:
			SignedIconst						{ $$ = makeIntConst($1, @1); }
			| '(' a_expr ')'					{ $$ = $2; }
			| /*EMPTY*/							{ $$ = makeIntConst(1, -1); }
		;

/*
 * Again, the trailing ROW/ROWS in this case prevent the full expression
 * syntax.  c_expr is the best we can do.
 */
select_offset_value2:
			c_expr									{ $$ = $1; }
		;

/* noise words */
row_or_rows: ROW									{ $$ = 0; }
			| ROWS									{ $$ = 0; }
		;

first_or_next: FIRST_P								{ $$ = 0; }
			| NEXT									{ $$ = 0; }
		;

/*
 * This syntax for group_clause tries to follow the spec quite closely.
 * However, the spec allows only column references, not expressions,
 * which introduces an ambiguity between implicit row constructors
 * (a,b) and lists of column references.
 *
 * We handle this by using the a_expr production for what the spec calls
 * <ordinary grouping set>, which in the spec represents either one column
 * reference or a parenthesized list of column references. Then, we check the
 * top node of the a_expr to see if it's an implicit RowExpr, and if so, just
 * grab and use the list, discarding the node. (this is done in parse analysis,
 * not here)
 *
 * (we abuse the row_format field of RowExpr to distinguish implicit and
 * explicit row constructors; it's debatable if anyone sanely wants to use them
 * in a group clause, but if they have a reason to, we make it possible.)
 *
 * Each item in the group_clause list is either an expression tree or a
 * GroupingSet node of some type.
 */

group_clause:
			GROUP_P BY group_by_list				{ $$ = $3; }
			| /*EMPTY*/								{ $$ = NIL; }
		;
group_by_list:
			group_by_item							{ $$ = list_make1($1); }
			| group_by_list ',' group_by_item		{ $$ = lappend($1,$3); }
		;

group_by_item:
			a_expr									{ $$ = $1; }
			| empty_grouping_set					{ $$ = $1; }
			| cube_clause							{ $$ = $1; }
			| rollup_clause 						{ $$ = $1; }
			| grouping_sets_clause					{ $$ = $1; }
		;

empty_grouping_set:
			'(' ')'
				{
					$$ = (Node *) makeGroupingSet(GROUPING_SET_EMPTY, NIL, @1);
				}
		;
/*
 * These hacks rely on setting precedence of CUBE and ROLLUP below that of '(',
 * so that they shift in these rules rather than reducing the conflicting
 * unreserved_keyword rule.
 */

rollup_clause:
			ROLLUP '(' expr_list ')'
				{
					$$ = (Node *) makeGroupingSet(GROUPING_SET_ROLLUP, $3, @1);
				}
		;

cube_clause:
			CUBE '(' expr_list ')'
				{
					$$ = (Node *) makeGroupingSet(GROUPING_SET_CUBE, $3, @1);
				}
		;

grouping_sets_clause:
			GROUPING_P SETS '(' group_by_list ')'
				{
					$$ = (Node *) makeGroupingSet(GROUPING_SET_SETS, $4, @1);
				}
		;


having_clause:
			HAVING a_expr							{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

for_locking_clause:
			for_locking_items						{ $$ = $1; }
			| FOR READ ONLY							{ $$ = NIL; }
		;

opt_for_locking_clause:
			for_locking_clause						{ $$ = $1; }
			| /* EMPTY */							{ $$ = NIL; }
		;

for_locking_items:
			for_locking_item						{ $$ = list_make1($1); }
			| for_locking_items for_locking_item	{ $$ = lappend($1, $2); }
		;

for_locking_item:
			FOR UPDATE locked_rels_list opt_nowait
				{
					LockingClause *n = makeNode(LockingClause);
					n->lockedRels = $3;
					n->forUpdate = TRUE;
					n->noWait = $4;
					$$ = (Node *) n;
				}
			| FOR SHARE locked_rels_list opt_nowait
				{
					LockingClause *n = makeNode(LockingClause);
					n->lockedRels = $3;
					n->forUpdate = FALSE;
					n->noWait = $4;
					$$ = (Node *) n;
				}
		;

locked_rels_list:
			OF qualified_name_list					{ $$ = $2; }
			| /* EMPTY */							{ $$ = NIL; }
		;


values_clause:
			VALUES ctext_row
				{
					SelectStmt *n = makeNode(SelectStmt);
					n->valuesLists = list_make1($2);
					$$ = (Node *) n;
				}
			| values_clause ',' ctext_row
				{
					SelectStmt *n = (SelectStmt *) $1;
					n->valuesLists = lappend(n->valuesLists, $3);
					$$ = (Node *) n;
				}
		;


/*****************************************************************************
 *
 *	clauses common to all Optimizable Stmts:
 *		from_clause		- allow list of both JOIN expressions and table names
 *		where_clause	- qualifications for joins or restrictions
 *
 *****************************************************************************/

from_clause:
			FROM from_list							{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

from_list:
			table_ref								{ $$ = list_make1($1); }
			| from_list ',' table_ref				{ $$ = lappend($1, $3); }
		;

/*
 * table_ref is where an alias clause can be attached.	Note we cannot make
 * alias_clause have an empty production because that causes parse conflicts
 * between table_ref := '(' joined_table ')' alias_clause
 * and joined_table := '(' joined_table ')'.  So, we must have the
 * redundant-looking productions here instead.
 */
table_ref:	relation_expr
				{
					$$ = (Node *) $1;
				}
			| relation_expr alias_clause
				{
					$1->alias = $2;
					$$ = (Node *) $1;
				}
			| relation_expr opt_alias_clause tablesample_clause
				{
					RangeTableSample *n = (RangeTableSample *) $3;
					$1->alias = $2;
					/* relation_expr goes inside the RangeTableSample node */
					n->relation = (Node *) $1;
					$$ = (Node *) n;
				}

			| relation_expr PARTITION '(' name ')'
				{
					$1->partitionname = $4;
					$1->ispartition = true;
					$$ = (Node *)$1;
				}
			| relation_expr BUCKETS '(' bucket_list ')'
				{
					$1->buckets = $4;
					$1->isbucket = true;
					$$ = (Node *)$1;
				}
			| relation_expr PARTITION_FOR '(' maxValueList ')'
				{
					$1->partitionKeyValuesList = $4;
					$1->ispartition = true;
					$$ = (Node *)$1;
				}

			| relation_expr PARTITION '(' name ')' alias_clause
				{
					$1->partitionname = $4;
					$1->alias = $6;
					$1->ispartition = true;
					$$ = (Node *)$1;
				}

			| relation_expr PARTITION_FOR '(' maxValueList ')' alias_clause
				{
					$1->partitionKeyValuesList = $4;
					$1->alias = $6;
					$1->ispartition = true;
					$$ = (Node *)$1;
				}

			| func_table
				{
					RangeFunction *n = makeNode(RangeFunction);
					n->funccallnode = $1;
					n->coldeflist = NIL;
					$$ = (Node *) n;
				}
			| func_table alias_clause
				{
					RangeFunction *n = makeNode(RangeFunction);
					n->funccallnode = $1;
					n->alias = $2;
					n->coldeflist = NIL;
					$$ = (Node *) n;
				}
			| func_table AS '(' TableFuncElementList ')'
				{
					RangeFunction *n = makeNode(RangeFunction);
					n->funccallnode = $1;
					n->coldeflist = $4;
					$$ = (Node *) n;
				}
			| func_table AS ColId '(' TableFuncElementList ')'
				{
					RangeFunction *n = makeNode(RangeFunction);
					Alias *a = makeNode(Alias);
					n->funccallnode = $1;
					a->aliasname = $3;
					n->alias = a;
					n->coldeflist = $5;
					$$ = (Node *) n;
				}
			| func_table ColId '(' TableFuncElementList ')'
				{
					RangeFunction *n = makeNode(RangeFunction);
					Alias *a = makeNode(Alias);
					n->funccallnode = $1;
					a->aliasname = $2;
					n->alias = a;
					n->coldeflist = $4;
					$$ = (Node *) n;
				}
			| select_with_parens
				{
					/*
					 * The SQL spec does not permit a subselect
					 * (<derived_table>) without an alias clause,
					 * so we don't either.  This avoids the problem
					 * of needing to invent a unique refname for it.
					 * That could be surmounted if there's sufficient
					 * popular demand, but for now let's just implement
					 * the spec and see if anyone complains.
					 * However, it does seem like a good idea to emit
					 * an error message that's better than "syntax error".
					 */
					/* add select_with_parens whthout alias_clause adapt A db for procedure dubug */
					$$ = NULL;
					if (IsA($1, SelectStmt) &&
						((SelectStmt *) $1)->valuesLists)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("VALUES in FROM must have an alias"),
								 errhint("For example, FROM (VALUES ...) [AS] foo."),
								 parser_errposition(@1)));
					else
					{
						/*
						* add a anonymous table name for this subquery
						* simulate A db to support no alias for subquery,
						* give the suqquery a default name "anonymous_table"
						*/
						RangeSubselect *n = makeNode(RangeSubselect);
						Alias *a = makeNode(Alias);
						n->subquery = $1;
						n->alias = NULL;
						a->aliasname = pstrdup("__unnamed_subquery__");
						n->alias = a;
						$$ = (Node *) n;
					}
				}
			| select_with_parens alias_clause
				{
					RangeSubselect *n = makeNode(RangeSubselect);
					n->subquery = $1;
					n->alias = $2;
					$$ = (Node *) n;
				}
			| joined_table
				{
					$$ = (Node *) $1;
				}
			| '(' joined_table ')' alias_clause
				{
					$2->alias = $4;
					$$ = (Node *) $2;
				}
		;


/*
 * It may seem silly to separate joined_table from table_ref, but there is
 * method in SQL92's madness: if you don't do it this way you get reduce-
 * reduce conflicts, because it's not clear to the parser generator whether
 * to expect alias_clause after ')' or not.  For the same reason we must
 * treat 'JOIN' and 'join_type JOIN' separately, rather than allowing
 * join_type to expand to empty; if we try it, the parser generator can't
 * figure out when to reduce an empty join_type right after table_ref.
 *
 * Note that a CROSS JOIN is the same as an unqualified
 * INNER JOIN, and an INNER JOIN/ON has the same shape
 * but a qualification expression to limit membership.
 * A NATURAL JOIN implicitly matches column names between
 * tables and the shape is determined by which columns are
 * in common. We'll collect columns during the later transformations.
 */

joined_table:
			'(' joined_table ')'
				{
					$$ = $2;
				}
			| table_ref CROSS JOIN table_ref
				{
					/* CROSS JOIN is same as unqualified inner join */
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = JOIN_INNER;
					n->isNatural = FALSE;
					n->larg = $1;
					n->rarg = $4;
					n->usingClause = NIL;
					n->quals = NULL;
					$$ = n;
				}
			| table_ref join_type JOIN table_ref join_qual
				{
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = $2;
					n->isNatural = FALSE;
					n->larg = $1;
					n->rarg = $4;
					if ($5 != NULL && IsA($5, List))
						n->usingClause = (List *) $5; /* USING clause */
					else
						n->quals = $5; /* ON clause */
					$$ = n;
				}
			| table_ref JOIN table_ref join_qual
				{
					/* letting join_type reduce to empty doesn't work */
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = JOIN_INNER;
					n->isNatural = FALSE;
					n->larg = $1;
					n->rarg = $3;
					if ($4 != NULL && IsA($4, List))
						n->usingClause = (List *) $4; /* USING clause */
					else
						n->quals = $4; /* ON clause */
					$$ = n;
				}
			| table_ref NATURAL join_type JOIN table_ref
				{
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = $3;
					n->isNatural = TRUE;
					n->larg = $1;
					n->rarg = $5;
					n->usingClause = NIL; /* figure out which columns later... */
					n->quals = NULL; /* fill later */
					$$ = n;
				}
			| table_ref NATURAL JOIN table_ref
				{
					/* letting join_type reduce to empty doesn't work */
					JoinExpr *n = makeNode(JoinExpr);
					n->jointype = JOIN_INNER;
					n->isNatural = TRUE;
					n->larg = $1;
					n->rarg = $4;
					n->usingClause = NIL; /* figure out which columns later... */
					n->quals = NULL; /* fill later */
					$$ = n;
				}
		;

alias_clause:
			AS ColId '(' name_list ')'
				{
					$$ = makeNode(Alias);
					$$->aliasname = $2;
					$$->colnames = $4;
				}
			| AS ColId
				{
					$$ = makeNode(Alias);
					$$->aliasname = $2;
				}
			| ColId '(' name_list ')'
				{
					$$ = makeNode(Alias);
					$$->aliasname = $1;
					$$->colnames = $3;
				}
			| ColId
				{
					$$ = makeNode(Alias);
					$$->aliasname = $1;
				}
		;

opt_alias_clause: alias_clause		{ $$ = $1; }
			| /*EMPTY*/	{ $$ = NULL; }
		;

join_type:	FULL join_outer							{ $$ = JOIN_FULL; }
			| LEFT join_outer						{ $$ = JOIN_LEFT; }
			| RIGHT join_outer						{ $$ = JOIN_RIGHT; }
			| INNER_P								{ $$ = JOIN_INNER; }
		;

/* OUTER is just noise... */
join_outer: OUTER_P									{ $$ = NULL; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

/* JOIN qualification clauses
 * Possibilities are:
 *	USING ( column list ) allows only unqualified column names,
 *						  which must match between tables.
 *	ON expr allows more general qualifications.
 *
 * We return USING as a List node, while an ON-expr will not be a List.
 */

join_qual:	USING '(' name_list ')'					{ $$ = (Node *) $3; }
			| ON a_expr								{ $$ = $2; }
		;


relation_expr:
			qualified_name
				{
					/* default inheritance */
					$$ = $1;
					$$->inhOpt = INH_DEFAULT;
					$$->alias = NULL;
				}
			| qualified_name '*'
				{
					/* inheritance query */
					$$ = $1;
					$$->inhOpt = INH_YES;
					$$->alias = NULL;
				}
			| ONLY qualified_name
				{
					/* no inheritance */
					$$ = $2;
					$$->inhOpt = INH_NO;
					$$->alias = NULL;
				}
			| ONLY '(' qualified_name ')'
				{
					/* no inheritance, SQL99-style syntax */
					$$ = $3;
					$$->inhOpt = INH_NO;
					$$->alias = NULL;
				}
		;


relation_expr_list:
			relation_expr							{ $$ = list_make1($1); }
			| relation_expr_list ',' relation_expr	{ $$ = lappend($1, $3); }
		;


/*
 * Given "UPDATE foo set set ...", we have to decide without looking any
 * further ahead whether the first "set" is an alias or the UPDATE's SET
 * keyword.  Since "set" is allowed as a column name both interpretations
 * are feasible.  We resolve the shift/reduce conflict by giving the first
 * relation_expr_opt_alias production a higher precedence than the SET token
 * has, causing the parser to prefer to reduce, in effect assuming that the
 * SET is not an alias.
 */
relation_expr_opt_alias: relation_expr					%prec UMINUS
				{
					$$ = $1;
				}
			| relation_expr ColId
				{
					Alias *alias = makeNode(Alias);
					alias->aliasname = $2;
					$1->alias = alias;
					$$ = $1;
				}
			| relation_expr AS ColId
				{
					Alias *alias = makeNode(Alias);
					alias->aliasname = $3;
					$1->alias = alias;
					$$ = $1;
				}
		;

/*
 * TABLESAMPLE decoration in a FROM item
 */
tablesample_clause:
			TABLESAMPLE func_name '(' expr_list ')' opt_repeatable_clause
				{
					RangeTableSample *n = makeNode(RangeTableSample);
					/* n->relation will be filled in later */
					n->method = $2;
					n->args = $4;
					n->repeatable = $6;
					n->location = @2;
					$$ = (Node *) n;
				}
		;

opt_repeatable_clause:
			REPEATABLE '(' a_expr ')'	{ $$ = (Node *) $3; }
			| /*EMPTY*/			{ $$ = NULL; }
		;


func_table: func_expr_windowless					{ $$ = $1; }
		;


where_clause:
			WHERE a_expr							{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

/* variant for UPDATE and DELETE */
where_or_current_clause:
			WHERE a_expr							{ $$ = $2; }
			| WHERE CURRENT_P OF cursor_name
				{
					CurrentOfExpr *n = makeNode(CurrentOfExpr);
					/* cvarno is filled in by parse analysis */
					n->cursor_name = $4;
					n->cursor_param = 0;
					$$ = (Node *) n;
				}
			| /*EMPTY*/								{ $$ = NULL; }
		;


OptTableFuncElementList:
			TableFuncElementList				{ $$ = $1; }
			| /*EMPTY*/							{ $$ = NIL; }
		;

TableFuncElementList:
			TableFuncElement
				{
					$$ = list_make1($1);
				}
			| TableFuncElementList ',' TableFuncElement
				{
					$$ = lappend($1, $3);
				}
		;

TableFuncElement:	ColId Typename opt_collate_clause
				{
					ColumnDef *n = makeNode(ColumnDef);
					n->colname = $1;
					n->typname = $2;
					n->inhcount = 0;
					n->is_local = true;
					n->is_not_null = false;
					n->is_from_type = false;
					n->storage = 0;
					n->raw_default = NULL;
					n->cooked_default = NULL;
					n->collClause = (CollateClause *) $3;
					n->clientLogicColumnRef=NULL;
					n->collOid = InvalidOid;
					n->constraints = NIL;
					$$ = (Node *)n;
				}
		;

/*****************************************************************************
 *
 *	Type syntax
 *		SQL92 introduces a large amount of type-specific syntax.
 *		Define individual clauses to handle these cases, and use
 *		 the generic case to handle regular type-extensible Postgres syntax.
 *		- thomas 1997-10-10
 *
 *****************************************************************************/

Typename:	SimpleTypename opt_array_bounds
				{
					$$ = $1;
					$$->arrayBounds = $2;
				}
			| SETOF SimpleTypename opt_array_bounds
				{
					$$ = $2;
					$$->arrayBounds = $3;
					$$->setof = TRUE;
				}
			/* SQL standard syntax, currently only one-dimensional */
			| SimpleTypename ARRAY '[' Iconst ']'
				{
					$$ = $1;
					$$->arrayBounds = list_make1(makeInteger($4));
				}
			| SETOF SimpleTypename ARRAY '[' Iconst ']'
				{
					$$ = $2;
					$$->arrayBounds = list_make1(makeInteger($5));
					$$->setof = TRUE;
				}
			| SimpleTypename ARRAY
				{
					$$ = $1;
					$$->arrayBounds = list_make1(makeInteger(-1));
				}
			| SETOF SimpleTypename ARRAY
				{
					$$ = $2;
					$$->arrayBounds = list_make1(makeInteger(-1));
					$$->setof = TRUE;
				}
		;

opt_array_bounds:
			opt_array_bounds '[' ']'
					{  $$ = lappend($1, makeInteger(-1)); }
			| opt_array_bounds '[' Iconst ']'
					{  $$ = lappend($1, makeInteger($3)); }
			| /*EMPTY*/
					{  $$ = NIL; }
		;

SimpleTypename:
			GenericType								{ $$ = $1; }
			| Numeric								{ $$ = $1; }
			| Bit									{ $$ = $1; }
			| Character								{ $$ = $1; }
			| ConstDatetime							{ $$ = $1; }
			| ConstInterval opt_interval
				{
					$$ = $1;
					$$->typmods = $2;
				}
			| ConstInterval '(' Iconst ')' opt_interval
				{
					$$ = $1;
					if ($5 != NIL)
					{
						if (list_length($5) != 1)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("interval precision specified twice"),
									 parser_errposition(@1)));
						$$->typmods = lappend($5, makeIntConst($3, @3));
					}
					else
						$$->typmods = list_make2(makeIntConst(INTERVAL_FULL_RANGE, -1),
												 makeIntConst($3, @3));
				}
		;

/* We have a separate ConstTypename to allow defaulting fixed-length
 * types such as CHAR() and BIT() to an unspecified length.
 * SQL9x requires that these default to a length of one, but this
 * makes no sense for constructs like CHAR 'hi' and BIT '0101',
 * where there is an obvious better choice to make.
 * Note that ConstInterval is not included here since it must
 * be pushed up higher in the rules to accommodate the postfix
 * options (e.g. INTERVAL '1' YEAR). Likewise, we have to handle
 * the generic-type-name case in AExprConst to avoid premature
 * reduce/reduce conflicts against function names.
 */
ConstTypename:
			Numeric									{ $$ = $1; }
			| ConstBit								{ $$ = $1; }
			| ConstCharacter						{ $$ = $1; }
			| ConstDatetime							{ $$ = $1; }
		;

/*
 * GenericType covers all type names that don't have special syntax mandated
 * by the standard, including qualified names.  We also allow type modifiers.
 * To avoid parsing conflicts against function invocations, the modifiers
 * have to be shown as expr_list here, but parse analysis will only accept
 * constants for them.
 */
GenericType:
			type_function_name opt_type_modifiers
				{
					$$ = makeTypeName($1);
					$$->typmods = $2;
					$$->location = @1;
				}
			| type_function_name attrs opt_type_modifiers
				{
					$$ = makeTypeNameFromNameList(lcons(makeString($1), $2));
					$$->typmods = $3;
					$$->location = @1;
				}
		;

opt_type_modifiers: '(' expr_list ')'				{ $$ = $2; }
					| /* EMPTY */					{ $$ = NIL; }
		;

/*
 * SQL92 numeric data types
 */
Numeric:	INT_P
				{
					$$ = SystemTypeName("int4");
					$$->location = @1;
				}
			| INTEGER opt_type_modifiers
				{
					if ($2 != NULL)
					{
						$$ = SystemTypeName("numeric");
						$$->typmods = $2;
						$$->location = @1;
					}
					else
				{
					$$ = SystemTypeName("int4");
					$$->location = @1;
				}
				}
			| SMALLINT
				{
					$$ = SystemTypeName("int2");
					$$->location = @1;
				}
			| TINYINT
				{
					$$ = SystemTypeName("int1");
					$$->location = @1;
				}
			| BIGINT
				{
					$$ = SystemTypeName("int8");
					$$->location = @1;
				}
			| REAL
				{
					$$ = SystemTypeName("float4");
					$$->location = @1;
				}
			| FLOAT_P opt_float
				{
					$$ = $2;
					$$->location = @1;
				}
			| BINARY_DOUBLE
				{
					$$ = SystemTypeName("float8");
					$$->location = @1;
				}
			| BINARY_INTEGER
				{
					$$ = SystemTypeName("int4");
					$$->location = @1;
				}
			| DOUBLE_P PRECISION
				{
					$$ = SystemTypeName("float8");
					$$->location = @1;
				}
			| DECIMAL_P opt_type_modifiers
				{
					$$ = SystemTypeName("numeric");
					$$->typmods = $2;
					$$->location = @1;
				}
			| NUMBER_P opt_type_modifiers
				{
					$$ = SystemTypeName("numeric");
					$$->typmods = $2;
					$$->location = @1;
				}
			| DEC opt_type_modifiers
				{
					$$ = SystemTypeName("numeric");
					$$->typmods = $2;
					$$->location = @1;
				}
			| NUMERIC opt_type_modifiers
				{
					$$ = SystemTypeName("numeric");
					$$->typmods = $2;
					$$->location = @1;
				}
			| BOOLEAN_P
				{
					$$ = SystemTypeName("bool");
					$$->location = @1;
				}
		;

opt_float:	'(' Iconst ')'
				{
					/*
					 * Check FLOAT() precision limits assuming IEEE floating
					 * types - thomas 1997-09-18
					 */
					if ($2 < 1)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("precision for type float must be at least 1 bit"),
								 parser_errposition(@2)));
					else if ($2 <= 24)
						$$ = SystemTypeName("float4");
					else if ($2 <= 53)
						$$ = SystemTypeName("float8");
					else
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("precision for type float must be less than 54 bits"),
								 parser_errposition(@2)));
				}
			| /*EMPTY*/
				{
					$$ = SystemTypeName("float8");
				}
		;

/*
 * SQL92 bit-field data types
 * The following implements BIT() and BIT VARYING().
 */
Bit:		BitWithLength
				{
					$$ = $1;
				}
			| BitWithoutLength
				{
					$$ = $1;
				}
		;

/* ConstBit is like Bit except "BIT" defaults to unspecified length */
/* See notes for ConstCharacter, which addresses same issue for "CHAR" */
ConstBit:	BitWithLength
				{
					$$ = $1;
				}
			| BitWithoutLength
				{
					$$ = $1;
					$$->typmods = NIL;
				}
		;

BitWithLength:
			BIT opt_varying '(' expr_list ')'
				{
					char *typname;

					typname = (char *)($2 ? "varbit" : "bit");
					$$ = SystemTypeName(typname);
					$$->typmods = $4;
					$$->location = @1;
				}
		;

BitWithoutLength:
			BIT opt_varying
				{
					/* bit defaults to bit(1), varbit to no limit */
					if ($2)
					{
						$$ = SystemTypeName("varbit");
					}
					else
					{
						$$ = SystemTypeName("bit");
						$$->typmods = list_make1(makeIntConst(1, -1));
					}
					$$->location = @1;
				}
		;


/*
 * SQL92 character data types
 * The following implements CHAR() and VARCHAR().
 */
Character:  CharacterWithLength
				{
					$$ = $1;
				}
			| CharacterWithoutLength
				{
					$$ = $1;
				}
		;

ConstCharacter:  CharacterWithLength
				{
					$$ = $1;
				}
			| CharacterWithoutLength
				{
					/* Length was not specified so allow to be unrestricted.
					 * This handles problems with fixed-length (bpchar) strings
					 * which in column definitions must default to a length
					 * of one, but should not be constrained if the length
					 * was not specified.
					 */
					$$ = $1;
					$$->typmods = NIL;
				}
		;

CharacterWithLength:  character '(' Iconst ')' opt_charset
				{
					if (($5 != NULL) && (strcmp($5, "sql_text") != 0))
					{
						char *type;

						type = (char *)palloc(strlen($1) + 1 + strlen($5) + 1);
						strcpy(type, $1);
						strcat(type, "_");
						strcat(type, $5);
						$1 = type;
					}

					$$ = SystemTypeName($1);
					$$->typmods = list_make1(makeIntConst($3, @3));
					$$->location = @1;
				}
		;

CharacterWithoutLength:	 character opt_charset
				{
					if (($2 != NULL) && (strcmp($2, "sql_text") != 0))
					{
						char *type;

						type = (char *)palloc(strlen($1) + 1 + strlen($2) + 1);
						strcpy(type, $1);
						strcat(type, "_");
						strcat(type, $2);
						$1 = type;
					}

					$$ = SystemTypeName($1);

					/* char defaults to char(1), varchar to no limit */
					if (strcmp($1, "bpchar") == 0)
						$$->typmods = list_make1(makeIntConst(1, -1));

					$$->location = @1;
				}
		;

character:	CHARACTER opt_varying
										{ $$ = (char *)($2 ? "varchar": "bpchar"); }
			| CHAR_P opt_varying
										{ $$ = (char *)($2 ? "varchar": "bpchar"); }
			| NVARCHAR2
										{ $$ = "nvarchar2"; }
			| VARCHAR
										{ $$ = "varchar"; }
			| VARCHAR2
										{ $$ = "varchar"; }
			| NATIONAL CHARACTER opt_varying
										{ $$ = (char *)($3 ? "varchar": "bpchar"); }
			| NATIONAL CHAR_P opt_varying
										{ $$ = (char *)($3 ? "varchar": "bpchar"); }
			| NCHAR opt_varying
										{ $$ = (char *)($2 ? "varchar": "bpchar"); }
		;

opt_varying:
			VARYING									{ $$ = TRUE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

opt_charset:
			CHARACTER SET ColId						{ $$ = $3; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

/*
 * SQL92 date/time types
 */
ConstDatetime:
			TIMESTAMP '(' Iconst ')' opt_timezone
				{
					if ($5)
						$$ = SystemTypeName("timestamptz");
					else
						$$ = SystemTypeName("timestamp");
					$$->typmods = list_make1(makeIntConst($3, @3));
					$$->location = @1;
				}
			| TIMESTAMP opt_timezone
				{
					if ($2)
						$$ = SystemTypeName("timestamptz");
					else
						$$ = SystemTypeName("timestamp");
					$$->location = @1;
				}
			| TIME '(' Iconst ')' opt_timezone
				{
					if ($5)
						$$ = SystemTypeName("timetz");
					else
						$$ = SystemTypeName("time");
					$$->typmods = list_make1(makeIntConst($3, @3));
					$$->location = @1;
				}
			| TIME opt_timezone
				{
					if ($2)
						$$ = SystemTypeName("timetz");
					else
						$$ = SystemTypeName("time");
					$$->location = @1;
				}
			| DATE_P
				{
					if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
					{
						$$ = SystemTypeName("timestamp");
						$$->typmods = list_make1(makeIntConst(0,-1));
					}
					else
						$$ = SystemTypeName("date");
					$$->location = @1;
				}
			| SMALLDATETIME
				{
					$$ = SystemTypeName("smalldatetime");
					$$->location = @1;
				}
		;

ConstInterval:
			INTERVAL
				{
					$$ = SystemTypeName("interval");
					$$->location = @1;
				}
		;

opt_timezone:
			WITH_TIME ZONE							{ $$ = TRUE; }
			| WITHOUT TIME ZONE						{ $$ = FALSE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

opt_interval:
			YEAR_P
				{ $$ = list_make1(makeIntConst(INTERVAL_MASK(YEAR), @1)); }
			| MONTH_P
				{ $$ = list_make1(makeIntConst(INTERVAL_MASK(MONTH), @1)); }
			| DAY_P
				{ $$ = list_make1(makeIntConst(INTERVAL_MASK(DAY), @1)); }
			| HOUR_P
				{ $$ = list_make1(makeIntConst(INTERVAL_MASK(HOUR), @1)); }
			| MINUTE_P
				{ $$ = list_make1(makeIntConst(INTERVAL_MASK(MINUTE), @1)); }
			| interval_second
				{ $$ = $1; }
			| YEAR_P TO MONTH_P
				{
					$$ = list_make1(makeIntConst(INTERVAL_MASK(YEAR) |
												 INTERVAL_MASK(MONTH), @1));
				}
			| DAY_P TO HOUR_P
				{
					$$ = list_make1(makeIntConst(INTERVAL_MASK(DAY) |
												 INTERVAL_MASK(HOUR), @1));
				}
			| DAY_P TO MINUTE_P
				{
					$$ = list_make1(makeIntConst(INTERVAL_MASK(DAY) |
												 INTERVAL_MASK(HOUR) |
												 INTERVAL_MASK(MINUTE), @1));
				}
			| DAY_P TO interval_second
				{
					$$ = $3;
					linitial($$) = makeIntConst(INTERVAL_MASK(DAY) |
												INTERVAL_MASK(HOUR) |
												INTERVAL_MASK(MINUTE) |
												INTERVAL_MASK(SECOND), @1);
				}
			| HOUR_P TO MINUTE_P
				{
					$$ = list_make1(makeIntConst(INTERVAL_MASK(HOUR) |
												 INTERVAL_MASK(MINUTE), @1));
				}
			| HOUR_P TO interval_second
				{
					$$ = $3;
					linitial($$) = makeIntConst(INTERVAL_MASK(HOUR) |
												INTERVAL_MASK(MINUTE) |
												INTERVAL_MASK(SECOND), @1);
				}
			| MINUTE_P TO interval_second
				{
					$$ = $3;
					linitial($$) = makeIntConst(INTERVAL_MASK(MINUTE) |
												INTERVAL_MASK(SECOND), @1);
				}
			| YEAR_P '(' Iconst ')'
				{ $$ = list_make1(makeIntConst(INTERVAL_MASK(YEAR), @1)); }
			| MONTH_P '(' Iconst ')'
				{ $$ = list_make1(makeIntConst(INTERVAL_MASK(MONTH), @1)); }
			| DAY_P '(' Iconst ')'
				{  $$ = list_make1(makeIntConst(INTERVAL_MASK(DAY), @1));   }
			| HOUR_P '(' Iconst ')'
				{ $$ = list_make1(makeIntConst(INTERVAL_MASK(HOUR), @1)); }
			| MINUTE_P '(' Iconst ')'
				{ $$ = list_make1(makeIntConst(INTERVAL_MASK(MINUTE), @1)); }
			| YEAR_P '(' Iconst ')' TO MONTH_P
				{
					$$ = list_make1(makeIntConst(INTERVAL_MASK(YEAR) |
												 INTERVAL_MASK(MONTH), @1));
				}
			| DAY_P '(' Iconst ')'  TO HOUR_P
				{
					$$ = list_make1(makeIntConst(INTERVAL_MASK(DAY) |
												 INTERVAL_MASK(HOUR), @1));
				}
			| DAY_P '(' Iconst ')'   TO MINUTE_P
				{
					$$ = list_make1(makeIntConst(INTERVAL_MASK(DAY) |
												 INTERVAL_MASK(HOUR) |
												 INTERVAL_MASK(MINUTE), @1));
				}
			| DAY_P '(' Iconst ')' TO interval_second
				{
					$$ = $6;
					linitial($$) = makeIntConst(INTERVAL_MASK(DAY) |
												INTERVAL_MASK(HOUR) |
												INTERVAL_MASK(MINUTE) |
												INTERVAL_MASK(SECOND), @1);
				}
			| HOUR_P '(' Iconst ')'  TO MINUTE_P
				{
					$$ = list_make1(makeIntConst(INTERVAL_MASK(HOUR) |
												 INTERVAL_MASK(MINUTE), @1));
				}
			| HOUR_P '(' Iconst ')'   TO interval_second
				{
					$$ = $6;
					linitial($$) = makeIntConst(INTERVAL_MASK(HOUR) |
												INTERVAL_MASK(MINUTE) |
												INTERVAL_MASK(SECOND), @1);
				}
			| MINUTE_P  '(' Iconst ')'  TO interval_second
				{
					$$ = $6;
					linitial($$) = makeIntConst(INTERVAL_MASK(MINUTE) |
												INTERVAL_MASK(SECOND), @1);
				}
			| /*EMPTY*/
				{ $$ = NIL; }
		;

interval_second:
			SECOND_P
				{
					$$ = list_make1(makeIntConst(INTERVAL_MASK(SECOND), @1));
				}
			| SECOND_P '(' Iconst ')'
				{
					$$ = list_make2(makeIntConst(INTERVAL_MASK(SECOND), @1),
									makeIntConst($3, @3));
				}
		;

client_logic_type:
			BYTEAWITHOUTORDER
			{
					$$ = SystemTypeName("byteawithoutordercol");
					$$->location = @1;
			}
			| BYTEAWITHOUTORDERWITHEQUAL
			{
					$$ = SystemTypeName("byteawithoutorderwithequalcol");
					$$->location = @1;
			}
;

/*****************************************************************************
 *
 *	expression grammar
 *
 *****************************************************************************/

/*
 * General expressions
 * This is the heart of the expression syntax.
 *
 * We have two expression types: a_expr is the unrestricted kind, and
 * b_expr is a subset that must be used in some places to avoid shift/reduce
 * conflicts.  For example, we can't do BETWEEN as "BETWEEN a_expr AND a_expr"
 * because that use of AND conflicts with AND as a boolean operator.  So,
 * b_expr is used in BETWEEN and we remove boolean keywords from b_expr.
 *
 * Note that '(' a_expr ')' is a b_expr, so an unrestricted expression can
 * always be used by surrounding it with parens.
 *
 * c_expr is all the productions that are common to a_expr and b_expr;
 * it's factored out just to eliminate redundant coding.
 */
a_expr:		c_expr									{ $$ = $1; }
			| a_expr TYPECAST Typename
					{ $$ = makeTypeCast($1, $3, @2); }
			| a_expr COLLATE any_name
				{
					CollateClause *n = makeNode(CollateClause);
					n->arg = $1;
					n->collname = $3;
					n->location = @2;
					$$ = (Node *) n;
				}
			| a_expr AT TIME ZONE a_expr			%prec AT
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("timezone");
					n->args = list_make2($5, $1);
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @2;
					n->call_func = false;
					$$ = (Node *) n;
				}
		/*
		 * These operators must be called out explicitly in order to make use
		 * of bison's automatic operator-precedence handling.  All other
		 * operator names are handled by the generic productions using "Op",
		 * below; and all those operators will have the same precedence.
		 *
		 * If you add more explicitly-known operators, be sure to add them
		 * also to b_expr and to the MathOp list above.
		 */
			| '+' a_expr					%prec UMINUS
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", NULL, $2, @1); }
			| '-' a_expr					%prec UMINUS
				{ $$ = doNegate($2, @1); }
			| a_expr '+' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", $1, $3, @2); }
			| a_expr '-' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "-", $1, $3, @2); }
			| a_expr '*' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "*", $1, $3, @2); }
			| a_expr '/' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "/", $1, $3, @2); }
			| a_expr '%' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "%", $1, $3, @2); }
			| a_expr '^' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "^", $1, $3, @2); }
			| a_expr '<' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<", $1, $3, @2); }
			| a_expr '>' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, ">", $1, $3, @2); }
			| a_expr '=' a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "=", $1, $3, @2); }
			| a_expr CmpOp a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, $2, $1, $3, @2); }
			| a_expr qual_Op a_expr				%prec Op
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, $3, @2); }
			| qual_Op a_expr					%prec Op
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $1, NULL, $2, @1); }
			| a_expr qual_Op					%prec POSTFIXOP
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, NULL, @2); }

			| a_expr AND a_expr
				{ $$ = (Node *) makeA_Expr(AEXPR_AND, NIL, $1, $3, @2); }
			| a_expr OR a_expr
				{ $$ = (Node *) makeA_Expr(AEXPR_OR, NIL, $1, $3, @2); }
			| NOT a_expr
				{ $$ = (Node *) makeA_Expr(AEXPR_NOT, NIL, NULL, $2, @1); }

			| a_expr LIKE a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "~~", $1, $3, @2); }
			| a_expr LIKE a_expr ESCAPE a_expr
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("like_escape");
					n->args = list_make2($3, $5);
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @2;
					n->call_func = false;
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "~~", $1, (Node *) n, @2);
				}
			| a_expr NOT LIKE a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "!~~", $1, $4, @2); }
			| a_expr NOT LIKE a_expr ESCAPE a_expr
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("like_escape");
					n->args = list_make2($4, $6);
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @2;
					n->call_func = false;
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "!~~", $1, (Node *) n, @2);
				}
			| a_expr ILIKE a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "~~*", $1, $3, @2); }
			| a_expr ILIKE a_expr ESCAPE a_expr
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("like_escape");
					n->args = list_make2($3, $5);
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @2;
					n->call_func = false;
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "~~*", $1, (Node *) n, @2);
				}
			| a_expr NOT ILIKE a_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "!~~*", $1, $4, @2); }
			| a_expr NOT ILIKE a_expr ESCAPE a_expr
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("like_escape");
					n->args = list_make2($4, $6);
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @2;
					n->call_func = false;
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "!~~*", $1, (Node *) n, @2);
				}

			| a_expr SIMILAR TO a_expr				%prec SIMILAR
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("similar_escape");
					n->args = list_make2($4, makeNullAConst(-1));
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @2;
					n->call_func = false;
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "~", $1, (Node *) n, @2);
				}
			| a_expr SIMILAR TO a_expr ESCAPE a_expr
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("similar_escape");
					n->args = list_make2($4, $6);
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @2;
					n->call_func = false;
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "~", $1, (Node *) n, @2);
				}
			| a_expr NOT SIMILAR TO a_expr			%prec SIMILAR
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("similar_escape");
					n->args = list_make2($5, makeNullAConst(-1));
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @2;
					n->call_func = false;
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "!~", $1, (Node *) n, @2);
				}
			| a_expr NOT SIMILAR TO a_expr ESCAPE a_expr
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("similar_escape");
					n->args = list_make2($5, $7);
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @2;
					n->call_func = false;
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "!~", $1, (Node *) n, @2);
				}

			/* NullTest clause
			 * Define SQL92-style Null test clause.
			 * Allow two forms described in the standard:
			 *	a IS NULL
			 *	a IS NOT NULL
			 * Allow two SQL extensions
			 *	a ISNULL
			 *	a NOTNULL
			 */
			| a_expr IS NULL_P							%prec IS
				{
					NullTest *n = makeNode(NullTest);
					n->arg = (Expr *) $1;
					n->nulltesttype = IS_NULL;
					$$ = (Node *)n;
				}
			| a_expr ISNULL
				{
					NullTest *n = makeNode(NullTest);
					n->arg = (Expr *) $1;
					n->nulltesttype = IS_NULL;
					$$ = (Node *)n;
				}
			| a_expr IS NOT NULL_P						%prec IS
				{
					NullTest *n = makeNode(NullTest);
					n->arg = (Expr *) $1;
					n->nulltesttype = IS_NOT_NULL;
					$$ = (Node *)n;
				}
			| a_expr NOTNULL
				{
					NullTest *n = makeNode(NullTest);
					n->arg = (Expr *) $1;
					n->nulltesttype = IS_NOT_NULL;
					$$ = (Node *)n;
				}
			| row OVERLAPS row
				{
					/* Create and populate a FuncCall node to support the OVERLAPS operator. */
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("overlaps");
					if (list_length($1) != 2)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("wrong number of parameters on left side of OVERLAPS expression"),
								parser_errposition(@1)));
					if (list_length($3) != 2)
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("wrong number of parameters on right side of OVERLAPS expression"),
								parser_errposition(@3)));
					n->args = list_concat($1, $3);
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @2;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| a_expr IS TRUE_P							%prec IS
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_TRUE;
					$$ = (Node *)b;
				}
			| a_expr IS NOT TRUE_P						%prec IS
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_NOT_TRUE;
					$$ = (Node *)b;
				}
			| a_expr IS FALSE_P							%prec IS
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_FALSE;
					$$ = (Node *)b;
				}
			| a_expr IS NOT FALSE_P						%prec IS
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_NOT_FALSE;
					$$ = (Node *)b;
				}
			| a_expr IS UNKNOWN							%prec IS
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_UNKNOWN;
					$$ = (Node *)b;
				}
			| a_expr IS NOT UNKNOWN						%prec IS
				{
					BooleanTest *b = makeNode(BooleanTest);
					b->arg = (Expr *) $1;
					b->booltesttype = IS_NOT_UNKNOWN;
					$$ = (Node *)b;
				}
			| a_expr IS DISTINCT FROM a_expr			%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_DISTINCT, "=", $1, $5, @2);
				}
			| a_expr IS NOT DISTINCT FROM a_expr		%prec IS
				{
					$$ = (Node *) makeA_Expr(AEXPR_NOT, NIL, NULL,
									(Node *) makeSimpleA_Expr(AEXPR_DISTINCT,
															  "=", $1, $6, @2),
											 @2);

				}
			| a_expr IS OF '(' type_list ')'			%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OF, "=", $1, (Node *) $5, @2);
				}
			| a_expr IS NOT OF '(' type_list ')'		%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OF, "<>", $1, (Node *) $6, @2);
				}
			/*
			 *	Ideally we would not use hard-wired operators below but
			 *	instead use opclasses.  However, mixed data types and other
			 *	issues make this difficult:
			 *	http://archives.postgresql.org/pgsql-hackers/2008-08/msg01142.php
			 */
			| a_expr BETWEEN opt_asymmetric b_expr AND b_expr		%prec BETWEEN
				{
					$$ = (Node *) makeA_Expr(AEXPR_AND, NIL,
						(Node *) makeSimpleA_Expr(AEXPR_OP, ">=", $1, $4, @2),
						(Node *) makeSimpleA_Expr(AEXPR_OP, "<=", $1, $6, @2),
											 @2);
				}
			| a_expr NOT BETWEEN opt_asymmetric b_expr AND b_expr	%prec BETWEEN
				{
					$$ = (Node *) makeA_Expr(AEXPR_OR, NIL,
						(Node *) makeSimpleA_Expr(AEXPR_OP, "<", $1, $5, @2),
						(Node *) makeSimpleA_Expr(AEXPR_OP, ">", $1, $7, @2),
											 @2);
				}
			| a_expr BETWEEN SYMMETRIC b_expr AND b_expr			%prec BETWEEN
				{
					$$ = (Node *) makeA_Expr(AEXPR_OR, NIL,
						(Node *) makeA_Expr(AEXPR_AND, NIL,
							(Node *) makeSimpleA_Expr(AEXPR_OP, ">=", $1, $4, @2),
							(Node *) makeSimpleA_Expr(AEXPR_OP, "<=", $1, $6, @2),
											@2),
						(Node *) makeA_Expr(AEXPR_AND, NIL,
							(Node *) makeSimpleA_Expr(AEXPR_OP, ">=", $1, $6, @2),
							(Node *) makeSimpleA_Expr(AEXPR_OP, "<=", $1, $4, @2),
											@2),
											 @2);
				}
			| a_expr NOT BETWEEN SYMMETRIC b_expr AND b_expr		%prec BETWEEN
				{
					$$ = (Node *) makeA_Expr(AEXPR_AND, NIL,
						(Node *) makeA_Expr(AEXPR_OR, NIL,
							(Node *) makeSimpleA_Expr(AEXPR_OP, "<", $1, $5, @2),
							(Node *) makeSimpleA_Expr(AEXPR_OP, ">", $1, $7, @2),
											@2),
						(Node *) makeA_Expr(AEXPR_OR, NIL,
							(Node *) makeSimpleA_Expr(AEXPR_OP, "<", $1, $7, @2),
							(Node *) makeSimpleA_Expr(AEXPR_OP, ">", $1, $5, @2),
											@2),
											 @2);
				}
			| a_expr IN_P in_expr
				{
					/* in_expr returns a SubLink or a list of a_exprs */
					if (IsA($3, SubLink))
					{
						/* generate foo = ANY (subquery) */
						SubLink *n = (SubLink *) $3;
						n->subLinkType = ANY_SUBLINK;
						n->testexpr = $1;
						n->operName = list_make1(makeString("="));
						n->location = @2;
						$$ = (Node *)n;
					}
					else
					{
						/* generate scalar IN expression */
						$$ = (Node *) makeSimpleA_Expr(AEXPR_IN, "=", $1, $3, @2);
					}
				}
			| a_expr NOT IN_P in_expr
				{
					/* in_expr returns a SubLink or a list of a_exprs */
					if (IsA($4, SubLink))
					{
						/* generate NOT (foo = ANY (subquery)) */
						/* Make an = ANY node */
						SubLink *n = (SubLink *) $4;
						n->subLinkType = ANY_SUBLINK;
						n->testexpr = $1;
						n->operName = list_make1(makeString("="));
						n->location = @3;
						/* Stick a NOT on top */
						$$ = (Node *) makeA_Expr(AEXPR_NOT, NIL, NULL, (Node *) n, @2);
					}
					else
					{
						/* generate scalar NOT IN expression */
						$$ = (Node *) makeSimpleA_Expr(AEXPR_IN, "<>", $1, $4, @2);
					}
				}
			| a_expr subquery_Op sub_type select_with_parens	%prec Op
				{
					SubLink *n = makeNode(SubLink);
					n->subLinkType = (SubLinkType)$3;
					n->testexpr = $1;
					n->operName = $2;
					n->subselect = $4;
					n->location = @2;
					$$ = (Node *)n;
				}
			| a_expr subquery_Op sub_type '(' a_expr ')'		%prec Op
				{
					if ($3 == ANY_SUBLINK)
						$$ = (Node *) makeA_Expr(AEXPR_OP_ANY, $2, $1, $5, @2);
					else
						$$ = (Node *) makeA_Expr(AEXPR_OP_ALL, $2, $1, $5, @2);
				}
			| UNIQUE select_with_parens
				{
					/* Not sure how to get rid of the parentheses
					 * but there are lots of shift/reduce errors without them.
					 *
					 * Should be able to implement this by plopping the entire
					 * select into a node, then transforming the target expressions
					 * from whatever they are into count(*), and testing the
					 * entire result equal to one.
					 * But, will probably implement a separate node in the executor.
					 */
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("UNIQUE predicate is not yet implemented"),
							 parser_errposition(@1)));
				}
			| a_expr IS DOCUMENT_P					%prec IS
				{
					$$ = makeXmlExpr(IS_DOCUMENT, NULL, NIL,
									 list_make1($1), @2);
				}
			| a_expr IS NOT DOCUMENT_P				%prec IS
				{
					$$ = (Node *) makeA_Expr(AEXPR_NOT, NIL, NULL,
											 makeXmlExpr(IS_DOCUMENT, NULL, NIL,
														 list_make1($1), @2),
											 @2);
				}
		;

/*
 * Restricted expressions
 *
 * b_expr is a subset of the complete expression syntax defined by a_expr.
 *
 * Presently, AND, NOT, IS, and IN are the a_expr keywords that would
 * cause trouble in the places where b_expr is used.  For simplicity, we
 * just eliminate all the boolean-keyword-operator productions from b_expr.
 */
b_expr:		c_expr
				{ $$ = $1; }
			| b_expr TYPECAST Typename
				{ $$ = makeTypeCast($1, $3, @2); }
			| '+' b_expr					%prec UMINUS
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", NULL, $2, @1); }
			| '-' b_expr					%prec UMINUS
				{ $$ = doNegate($2, @1); }
			| b_expr '+' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "+", $1, $3, @2); }
			| b_expr '-' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "-", $1, $3, @2); }
			| b_expr '*' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "*", $1, $3, @2); }
			| b_expr '/' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "/", $1, $3, @2); }
			| b_expr '%' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "%", $1, $3, @2); }
			| b_expr '^' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "^", $1, $3, @2); }
			| b_expr '<' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "<", $1, $3, @2); }
			| b_expr '>' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, ">", $1, $3, @2); }
			| b_expr '=' b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, "=", $1, $3, @2); }
			| b_expr CmpOp b_expr
				{ $$ = (Node *) makeSimpleA_Expr(AEXPR_OP, $2, $1, $3, @2); }
			| b_expr qual_Op b_expr				%prec Op
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, $3, @2); }
			| qual_Op b_expr					%prec Op
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $1, NULL, $2, @1); }
			| b_expr qual_Op					%prec POSTFIXOP
				{ $$ = (Node *) makeA_Expr(AEXPR_OP, $2, $1, NULL, @2); }
			| b_expr IS DISTINCT FROM b_expr		%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_DISTINCT, "=", $1, $5, @2);
				}
			| b_expr IS NOT DISTINCT FROM b_expr	%prec IS
				{
					$$ = (Node *) makeA_Expr(AEXPR_NOT, NIL,
						NULL, (Node *) makeSimpleA_Expr(AEXPR_DISTINCT, "=", $1, $6, @2), @2);
				}
			| b_expr IS OF '(' type_list ')'		%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OF, "=", $1, (Node *) $5, @2);
				}
			| b_expr IS NOT OF '(' type_list ')'	%prec IS
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_OF, "<>", $1, (Node *) $6, @2);
				}
			| b_expr IS DOCUMENT_P					%prec IS
				{
					$$ = makeXmlExpr(IS_DOCUMENT, NULL, NIL,
									 list_make1($1), @2);
				}
			| b_expr IS NOT DOCUMENT_P				%prec IS
				{
					$$ = (Node *) makeA_Expr(AEXPR_NOT, NIL, NULL,
											 makeXmlExpr(IS_DOCUMENT, NULL, NIL,
														 list_make1($1), @2),
											 @2);
				}
		;

/*
 * Productions that can be used in both a_expr and b_expr.
 *
 * Note: productions that refer recursively to a_expr or b_expr mostly
 * cannot appear here.	However, it's OK to refer to a_exprs that occur
 * inside parentheses, such as function arguments; that cannot introduce
 * ambiguity to the b_expr syntax.
 */
c_expr:		columnref								{ $$ = $1; }
			| AexprConst							{ $$ = $1; }
			| PARAM opt_indirection
				{
					ParamRef *p = makeNode(ParamRef);
					p->number = $1;
					p->location = @1;
					if ($2)
					{
						A_Indirection *n = makeNode(A_Indirection);
						n->arg = (Node *) p;
						n->indirection = check_indirection($2, yyscanner);
						$$ = (Node *) n;
					}
					else
						$$ = (Node *) p;
				}
			| '(' a_expr ')' opt_indirection
				{
					if ($4)
					{
						A_Indirection *n = makeNode(A_Indirection);
						n->arg = $2;
						n->indirection = check_indirection($4, yyscanner);
						$$ = (Node *)n;
					}
					else
						$$ = $2;
				}
			| case_expr
				{ $$ = $1; }
			| func_expr
				{ $$ = $1; }
			| select_with_parens			%prec UMINUS
				{
					SubLink *n = makeNode(SubLink);
					n->subLinkType = EXPR_SUBLINK;
					n->testexpr = NULL;
					n->operName = NIL;
					n->subselect = $1;
					n->location = @1;
					$$ = (Node *)n;
				}
			| select_with_parens indirection
				{
					/*
					 * Because the select_with_parens nonterminal is designed
					 * to "eat" as many levels of parens as possible, the
					 * '(' a_expr ')' opt_indirection production above will
					 * fail to match a sub-SELECT with indirection decoration;
					 * the sub-SELECT won't be regarded as an a_expr as long
					 * as there are parens around it.  To support applying
					 * subscripting or field selection to a sub-SELECT result,
					 * we need this redundant-looking production.
					 */
					SubLink *n = makeNode(SubLink);
					A_Indirection *a = makeNode(A_Indirection);
					n->subLinkType = EXPR_SUBLINK;
					n->testexpr = NULL;
					n->operName = NIL;
					n->subselect = $1;
					n->location = @1;
					a->arg = (Node *)n;
					a->indirection = check_indirection($2, yyscanner);
					$$ = (Node *)a;
				}
			| EXISTS select_with_parens
				{
					SubLink *n = makeNode(SubLink);
					n->subLinkType = EXISTS_SUBLINK;
					n->testexpr = NULL;
					n->operName = NIL;
					n->subselect = $2;
					n->location = @1;
					$$ = (Node *)n;
				}
			| ARRAY select_with_parens
				{
					SubLink *n = makeNode(SubLink);
					n->subLinkType = ARRAY_SUBLINK;
					n->testexpr = NULL;
					n->operName = NIL;
					n->subselect = $2;
					n->location = @1;
					$$ = (Node *)n;
				}
			| ARRAY array_expr
				{
					A_ArrayExpr *n = (A_ArrayExpr *) $2;
					AssertEreport(IsA(n, A_ArrayExpr),
									MOD_OPT,
									"Node type inconsistant");
					/* point outermost A_ArrayExpr to the ARRAY keyword */
					n->location = @1;
					$$ = (Node *)n;
				}
			| explicit_row
				{
					RowExpr *r = makeNode(RowExpr);
					r->args = $1;
					r->row_typeid = InvalidOid;	/* not analyzed yet */
					r->colnames = NIL;	/* to be filled in during analysis */
					r->row_format = COERCE_EXPLICIT_CALL; /* abuse */
					r->location = @1;
					$$ = (Node *)r;
				}
			| implicit_row
				{
					RowExpr *r = makeNode(RowExpr);
					r->args = $1;
					r->row_typeid = InvalidOid;	/* not analyzed yet */
					r->colnames = NIL;	/* to be filled in during analysis */
					r->row_format = COERCE_IMPLICIT_CAST; /* abuse */
					r->location = @1;
					$$ = (Node *)r;
				}
			| GROUPING_P '(' expr_list ')'
			  {
				  GroupingFunc *g = makeNode(GroupingFunc);
				  g->args = $3;
				  g->location = @1;
				  $$ = (Node *)g;
			  }
		;

/* Used for List Distribution to avoid reduce/reduce conflict. This is unavoidable, since Bison is LALR(1) compiler */
c_expr_noparen:		columnref								{ $$ = $1; }
			| AexprConst							{ $$ = $1; }
			| PARAM opt_indirection
				{
					ParamRef *p = makeNode(ParamRef);
					p->number = $1;
					p->location = @1;
					if ($2)
					{
						A_Indirection *n = makeNode(A_Indirection);
						n->arg = (Node *) p;
						n->indirection = check_indirection($2, yyscanner);
						$$ = (Node *) n;
					}
					else
						$$ = (Node *) p;
				}
			| case_expr
				{ $$ = $1; }
			| func_expr
				{ $$ = $1; }
			| select_with_parens			%prec UMINUS
				{
					SubLink *n = makeNode(SubLink);
					n->subLinkType = EXPR_SUBLINK;
					n->testexpr = NULL;
					n->operName = NIL;
					n->subselect = $1;
					n->location = @1;
					$$ = (Node *)n;
				}
			| select_with_parens indirection
				{
					/*
					 * Because the select_with_parens nonterminal is designed
					 * to "eat" as many levels of parens as possible, the
					 * '(' a_expr ')' opt_indirection production above will
					 * fail to match a sub-SELECT with indirection decoration;
					 * the sub-SELECT won't be regarded as an a_expr as long
					 * as there are parens around it.  To support applying
					 * subscripting or field selection to a sub-SELECT result,
					 * we need this redundant-looking production.
					 */
					SubLink *n = makeNode(SubLink);
					A_Indirection *a = makeNode(A_Indirection);
					n->subLinkType = EXPR_SUBLINK;
					n->testexpr = NULL;
					n->operName = NIL;
					n->subselect = $1;
					n->location = @1;
					a->arg = (Node *)n;
					a->indirection = check_indirection($2, yyscanner);
					$$ = (Node *)a;
				}
			| EXISTS select_with_parens
				{
					SubLink *n = makeNode(SubLink);
					n->subLinkType = EXISTS_SUBLINK;
					n->testexpr = NULL;
					n->operName = NIL;
					n->subselect = $2;
					n->location = @1;
					$$ = (Node *)n;
				}
			| ARRAY select_with_parens
				{
					SubLink *n = makeNode(SubLink);
					n->subLinkType = ARRAY_SUBLINK;
					n->testexpr = NULL;
					n->operName = NIL;
					n->subselect = $2;
					n->location = @1;
					$$ = (Node *)n;
				}
			| ARRAY array_expr
				{
					A_ArrayExpr *n = (A_ArrayExpr *) $2;
					AssertEreport(IsA(n, A_ArrayExpr),
									MOD_OPT,
									"Node type inconsistant");
					/* point outermost A_ArrayExpr to the ARRAY keyword */
					n->location = @1;
					$$ = (Node *)n;
				}
			| explicit_row
				{
					RowExpr *r = makeNode(RowExpr);
					r->args = $1;
					r->row_typeid = InvalidOid;	/* not analyzed yet */
					r->colnames = NIL;	/* to be filled in during analysis */
					r->row_format = COERCE_EXPLICIT_CALL; /* abuse */
					r->location = @1;
					$$ = (Node *)r;
				}
			| GROUPING_P '(' expr_list ')'
			  {
				  GroupingFunc *g = makeNode(GroupingFunc);
				  g->args = $3;
				  g->location = @1;
				  $$ = (Node *)g;
			  }
		;



/*
 * func_expr and its cousin func_expr_windowless are split out from c_expr just
 * so that we have a classification for "everything that is a function call or
 * looks like one". This isn't very important, but it saves us having to
 * document which variants are legal in places like "FROM functions()" or the
 * backwards-compatible functional-index syntax for CREATE INDEX.
 * (Note that many of the special SQL functions wouldn't actually make any
 * sense as functional index entries, but we ignore that consideration here.)
 */
func_expr:	func_application within_group_clause over_clause
				{
					FuncCall *n = (FuncCall *) $1;

					/*
					 * We currently only use listagg function with WITHIN GROUP. Besides, this function
					 * has up to 2 parameters and cannot use DISTINCT, VARIADIC and ORDER BY clauses.
					 */
					if (pg_strcasecmp(strVal(linitial(n->funcname)), "listagg") == 0)
					{
						if ($2 == NIL)
						{
							ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("missing WITHIN keyword."),
								 parser_errposition(@2)));
						}
						else
						{
							if (n->agg_order != NIL)
							{
								ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("cannot use multiple ORDER BY clauses with WITHIN GROUP."),
									 parser_errposition(@2)));
							}
							if (n->agg_distinct)
							{
								ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("cannot use DISTINCT with WITHIN GROUP."),
									 parser_errposition(@2)));
							}
							if (n->func_variadic)
							{
								ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("cannot use VARIADIC with WITHIN GROUP."),
									 parser_errposition(@2)));
							}
						}
						n->agg_order = $2;
						
						WindowDef *wd = (WindowDef*) $3;
						if (wd != NULL)
							wd->frameOptions = FRAMEOPTION_NONDEFAULT | FRAMEOPTION_ROWS | 
												FRAMEOPTION_START_UNBOUNDED_PRECEDING |
												(FRAMEOPTION_START_UNBOUNDED_FOLLOWING << 1) | 
												FRAMEOPTION_BETWEEN;
						n->over = wd;
					}
					else if ($2 != NIL)
					{
						if (n->agg_order != NIL)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("cannot use multiple ORDER BY clauses with WITHIN GROUP"),
									 parser_errposition(@2)));
						if (n->agg_distinct)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("cannot use DISTINCT with WITHIN GROUP"),
									 parser_errposition(@2)));
						if (n->func_variadic)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("cannot use VARIADIC with WITHIN GROUP"),
									 parser_errposition(@2)));
						n->agg_order = $2;
						n->agg_within_group = TRUE;
						n->over = $3;
						$$ = (Node *) n;
					}
					else
					{
						n->over = $3;
					}
				}
			| func_expr_common_subexpr
				{ $$ = $1; }
		;

func_application:	func_name '(' ')'
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = $1;
					n->args = NIL;
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| func_name '(' func_arg_list opt_sort_clause ')'
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = $1;
					n->args = $3;
					n->agg_order = $4;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| func_name '(' VARIADIC func_arg_expr opt_sort_clause ')'
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = $1;
					n->args = list_make1($4);
					n->agg_order = $5;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = TRUE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| func_name '(' func_arg_list ',' VARIADIC func_arg_expr opt_sort_clause ')'
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = $1;
					n->args = lappend($3, $6);
					n->agg_order = $7;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = TRUE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| func_name '(' ALL func_arg_list opt_sort_clause ')'
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = $1;
					n->args = $4;
					n->agg_order = $5;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					/* Ideally we'd mark the FuncCall node to indicate
					 * "must be an aggregate", but there's no provision
					 * for that in FuncCall at the moment.
					 */
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| func_name '(' DISTINCT func_arg_list opt_sort_clause ')'
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = $1;
					n->args = $4;
					n->agg_order = $5;
					n->agg_star = FALSE;
					n->agg_distinct = TRUE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| func_name '(' '*' ')'
				{
					/*
					 * We consider AGGREGATE(*) to invoke a parameterless
					 * aggregate.  This does the right thing for COUNT(*),
					 * and there are no other aggregates in SQL92 that accept
					 * '*' as parameter.
					 *
					 * The FuncCall node is also marked agg_star = true,
					 * so that later processing can detect what the argument
					 * really was.
					 */
					FuncCall *n = makeNode(FuncCall);
					n->funcname = $1;
					n->args = NIL;
					n->agg_order = NIL;
					n->agg_star = TRUE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
		;

/*
 * As func_expr but does not accept WINDOW functions directly
 * (but they can still be contained in arguments for functions etc).
 * Use this when window expressions are not allowed, where needed to disambiguate the grammar.
 * e.g. in FROM clause, window function or function with 'within group' clause are not allowed.
 * in CREATE INDEX, they are also not allowed.
 */
func_expr_windowless:
            func_application            { $$ = $1; }
            | func_expr_common_subexpr  { $$ = $1; }
        ;

/*
 * Special expressions that are considered to be functions;
 */
func_expr_common_subexpr:
			COLLATION FOR '(' a_expr ')'
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("pg_collation_for");
					n->args = list_make1($4);
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| CURRENT_DATE
				{
					/*
					 * Translate as "text_date('now'::text)".
					 *
					 * Notice that we cannot use 'now'::text::date because when 
					 * we go FQS plan, it will deparsed as 'now'::text::date which is
					 * equal to 'now'::text::timestamp under A_FORMAT.
					 *
					 * We cannot use "text_date('now')" because it will
					 * immediately reduce that to a constant representing
					 * today's date.  We need to delay the conversion until
					 * runtime, else the wrong things will happen when
					 * CURRENT_DATE is used in a column default value or rule.
					 *
					 * This could be simplified if we had a way to generate
					 * an expression tree representing runtime application
					 * of type-input conversion functions.  (As of PG 7.3
					 * that is actually possible, but not clear that we want
					 * to rely on it.)
					 *
					 * The token location is attached to the run-time
					 * typecast, not to the Const, for the convenience of
					 * pg_stat_statements (which doesn't want these constructs
					 * to appear to be replaceable constants).
					 */
					FuncCall *n = makeNode(FuncCall);
					Node *d = makeStringConstCast("now", -1, SystemTypeName("text"));

					n->funcname = SystemFuncName("text_date");
					n->colname = pstrdup("date");
					n->args = list_make1(d);
					n->agg_order = NIL;
					n->agg_star = false;
					n->agg_distinct = false;
					n->func_variadic = false;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| CURRENT_TIME
				{
					/*
					 * Translate as "'now'::text::timetz".
					 * See comments for CURRENT_DATE.
					 */
					Node *n;
					n = makeStringConstCast("now", -1, SystemTypeName("text"));
					$$ = makeTypeCast(n, SystemTypeName("timetz"), @1);
				}
			| CURRENT_TIME '(' Iconst ')'
				{
					/*
					 * Translate as "'now'::text::timetz(n)".
					 * See comments for CURRENT_DATE.
					 */
					Node *n;
					TypeName *d;
					n = makeStringConstCast("now", -1, SystemTypeName("text"));
					d = SystemTypeName("timetz");
					d->typmods = list_make1(makeIntConst($3, @3));
					$$ = makeTypeCast(n, d, @1);
				}
			| CURRENT_TIMESTAMP
				{
					/*
					 * Translate as "now()", since we have a function that
					 * does exactly what is needed.
					 */
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("pg_systimestamp");
					n->args = NIL;
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| CURRENT_TIMESTAMP '(' Iconst ')'
				{
					/*
					 * Translate as "'now'::text::timestamptz(n)".
					 * See comments for CURRENT_DATE.
					 */
					Node *n;
					TypeName *d;
					n = makeStringConstCast("now", -1, SystemTypeName("text"));
					d = SystemTypeName("timestamptz");
					d->typmods = list_make1(makeIntConst($3, @3));
					$$ = makeTypeCast(n, d, @1);
				}
			| LOCALTIME
				{
					/*
					 * Translate as "'now'::text::time".
					 * See comments for CURRENT_DATE.
					 */
					Node *n;
					n = makeStringConstCast("now", -1, SystemTypeName("text"));
					$$ = makeTypeCast((Node *)n, SystemTypeName("time"), @1);
				}
			| LOCALTIME '(' Iconst ')'
				{
					/*
					 * Translate as "'now'::text::time(n)".
					 * See comments for CURRENT_DATE.
					 */
					Node *n;
					TypeName *d;
					n = makeStringConstCast("now", -1, SystemTypeName("text"));
					d = SystemTypeName("time");
					d->typmods = list_make1(makeIntConst($3, @3));
					$$ = makeTypeCast((Node *)n, d, @1);
				}
			| LOCALTIMESTAMP
				{
					/*
					 * Translate as "'now'::text::timestamp".
					 * See comments for CURRENT_DATE.
					 */
					Node *n;
					n = makeStringConstCast("now", -1, SystemTypeName("text"));
					$$ = makeTypeCast(n, SystemTypeName("timestamp"), @1);
				}
			| LOCALTIMESTAMP '(' Iconst ')'
				{
					/*
					 * Translate as "'now'::text::timestamp(n)".
					 * See comments for CURRENT_DATE.
					 */
					Node *n;
					TypeName *d;
					n = makeStringConstCast("now", -1, SystemTypeName("text"));
					d = SystemTypeName("timestamp");
					d->typmods = list_make1(makeIntConst($3, @3));
					$$ = makeTypeCast(n, d, @1);
				}
			| SYSDATE
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("sysdate");
					n->args = NIL;
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| ROWNUM 
				{
#ifdef ENABLE_MULTIPLE_NODES
    					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("ROWNUM is not yet supported.")));
#endif
					Rownum *r = makeNode(Rownum);
				    	r->location = @1;
					$$ = (Node *)r;
				}			
			| CURRENT_ROLE
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("current_user");
					n->args = NIL;
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| CURRENT_USER
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("current_user");
					n->args = NIL;
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| SESSION_USER
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("session_user");
					n->args = NIL;
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| USER
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("current_user");
					n->args = NIL;
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| CURRENT_CATALOG
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("current_database");
					n->args = NIL;
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| CURRENT_SCHEMA
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("current_schema");
					n->args = NIL;
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| CAST '(' a_expr AS Typename ')'
				{ $$ = makeTypeCast($3, $5, @1); }
			| EXTRACT '(' extract_list ')'
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("date_part");
					n->args = $3;
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| TIMESTAMPDIFF '(' timestamp_arg_list ')'
				{
					if (u_sess->attr.attr_sql.sql_compatibility == B_FORMAT)
					{
						FuncCall *n = makeNode(FuncCall);
						n->funcname = SystemFuncName("timestamp_diff");
						n->args = $3;
						n->agg_order = NIL;
						n->agg_star = FALSE;
						n->agg_distinct = FALSE;
						n->func_variadic = FALSE;
						n->over = NULL;
						n->location = @1;
						n->call_func = false;
						$$ = (Node *)n;
					}
					else
					{
						ereport(ERROR,
								(errmodule(MOD_PARSER),
                                                                 errcode(ERRCODE_SYNTAX_ERROR),
                                                                 errmsg("timestampdiff syntax is not supported."),
                                                                 parser_errposition(@1)));
  					        $$ = NULL;/* not reached */
					}
				}
			| OVERLAY '(' overlay_list ')'
				{
					/* overlay(A PLACING B FROM C FOR D) is converted to
					 * overlay(A, B, C, D)
					 * overlay(A PLACING B FROM C) is converted to
					 * overlay(A, B, C)
					 */
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("overlay");
					n->args = $3;
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| POSITION '(' position_list ')'
				{
					/* position(A in B) is converted to position(B, A) */
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("position");
					n->args = $3;
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| SUBSTRING '(' substr_list ')'
				{
					/* substring(A from B for C) is converted to
					 * substring(A, B, C) - thomas 2000-11-28
					 */
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("substring");
					n->args = $3;
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| TREAT '(' a_expr AS Typename ')'
				{
					/* TREAT(expr AS target) converts expr of a particular type to target,
					 * which is defined to be a subtype of the original expression.
					 * In SQL99, this is intended for use with structured UDTs,
					 * but let's make this a generally useful form allowing stronger
					 * coercions than are handled by implicit casting.
					 */
					FuncCall *n = makeNode(FuncCall);
					/* Convert SystemTypeName() to SystemFuncName() even though
					 * at the moment they result in the same thing.
					 */
					n->funcname = SystemFuncName(((Value *)llast($5->names))->val.str);
					n->args = list_make1($3);
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| TRIM '(' BOTH trim_list ')'
				{
					/* various trim expressions are defined in SQL92
					 * - thomas 1997-07-19
					 */
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("btrim");
					n->args = $4;
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| TRIM '(' LEADING trim_list ')'
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("ltrim");
					n->args = $4;
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| TRIM '(' TRAILING trim_list ')'
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("rtrim");
					n->args = $4;
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| TRIM '(' trim_list ')'
				{
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("btrim");
					n->args = $3;
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| NULLIF '(' a_expr ',' a_expr ')'
				{
					$$ = (Node *) makeSimpleA_Expr(AEXPR_NULLIF, "=", $3, $5, @1);
				}
			| NVL '(' a_expr ',' a_expr ')'
				{
					CoalesceExpr *c = makeNode(CoalesceExpr);
					c->args = list_make2($3,$5);
					// modify NVL display to A db's style "NVL" instead of "COALESCE"
					c->isnvl = true;
					$$ = (Node *)c;
				}
			| COALESCE '(' expr_list ')'
				{
					CoalesceExpr *c = makeNode(CoalesceExpr);
					c->args = $3;
					c->location = @1;
					// modify NVL display to A db's style "NVL" instead of "COALESCE"
					c->isnvl = false;
					$$ = (Node *)c;
				}
			| GREATEST '(' expr_list ')'
				{
					MinMaxExpr *v = makeNode(MinMaxExpr);
					v->args = $3;
					v->op = IS_GREATEST;
					v->location = @1;
					$$ = (Node *)v;
				}
			| LEAST '(' expr_list ')'
				{
					MinMaxExpr *v = makeNode(MinMaxExpr);
					v->args = $3;
					v->op = IS_LEAST;
					v->location = @1;
					$$ = (Node *)v;
				}
			| XMLCONCAT '(' expr_list ')'
				{
					$$ = makeXmlExpr(IS_XMLCONCAT, NULL, NIL, $3, @1);
				}
			| XMLELEMENT '(' NAME_P ColLabel ')'
				{
					$$ = makeXmlExpr(IS_XMLELEMENT, $4, NIL, NIL, @1);
				}
			| XMLELEMENT '(' NAME_P ColLabel ',' xml_attributes ')'
				{
					$$ = makeXmlExpr(IS_XMLELEMENT, $4, $6, NIL, @1);
				}
			| XMLELEMENT '(' NAME_P ColLabel ',' expr_list ')'
				{
					$$ = makeXmlExpr(IS_XMLELEMENT, $4, NIL, $6, @1);
				}
			| XMLELEMENT '(' NAME_P ColLabel ',' xml_attributes ',' expr_list ')'
				{
					$$ = makeXmlExpr(IS_XMLELEMENT, $4, $6, $8, @1);
				}
			| XMLEXISTS '(' c_expr xmlexists_argument ')'
				{
					/* xmlexists(A PASSING [BY REF] B [BY REF]) is
					 * converted to xmlexists(A, B)*/
					FuncCall *n = makeNode(FuncCall);
					n->funcname = SystemFuncName("xmlexists");
					n->args = list_make2($3, $4);
					n->agg_order = NIL;
					n->agg_star = FALSE;
					n->agg_distinct = FALSE;
					n->func_variadic = FALSE;
					n->over = NULL;
					n->location = @1;
					n->call_func = false;
					$$ = (Node *)n;
				}
			| XMLFOREST '(' xml_attribute_list ')'
				{
					$$ = makeXmlExpr(IS_XMLFOREST, NULL, $3, NIL, @1);
				}
			| XMLPARSE '(' document_or_content a_expr xml_whitespace_option ')'
				{
					XmlExpr *x = (XmlExpr *)
						makeXmlExpr(IS_XMLPARSE, NULL, NIL,
									list_make2($4, makeBoolAConst($5, -1)),
									@1);
					x->xmloption = (XmlOptionType)$3;
					$$ = (Node *)x;
				}
			| XMLPI '(' NAME_P ColLabel ')'
				{
					$$ = makeXmlExpr(IS_XMLPI, $4, NULL, NIL, @1);
				}
			| XMLPI '(' NAME_P ColLabel ',' a_expr ')'
				{
					$$ = makeXmlExpr(IS_XMLPI, $4, NULL, list_make1($6), @1);
				}
			| XMLROOT '(' a_expr ',' xml_root_version opt_xml_root_standalone ')'
				{
					$$ = makeXmlExpr(IS_XMLROOT, NULL, NIL,
									 list_make3($3, $5, $6), @1);
				}
			| XMLSERIALIZE '(' document_or_content a_expr AS SimpleTypename ')'
				{
					XmlSerialize *n = makeNode(XmlSerialize);
					n->xmloption = (XmlOptionType)$3;
					n->expr = $4;
					n->typname = $6;
					n->location = @1;
					$$ = (Node *)n;
				}
		;

/*
 * SQL/XML support
 */
xml_root_version: VERSION_P a_expr
				{ $$ = $2; }
			| VERSION_P NO VALUE_P
				{ $$ = makeNullAConst(-1); }
		;

opt_xml_root_standalone: ',' STANDALONE_P YES_P
				{ $$ = makeIntConst(XML_STANDALONE_YES, -1); }
			| ',' STANDALONE_P NO
				{ $$ = makeIntConst(XML_STANDALONE_NO, -1); }
			| ',' STANDALONE_P NO VALUE_P
				{ $$ = makeIntConst(XML_STANDALONE_NO_VALUE, -1); }
			| /*EMPTY*/
				{ $$ = makeIntConst(XML_STANDALONE_OMITTED, -1); }
		;

xml_attributes: XMLATTRIBUTES '(' xml_attribute_list ')'	{ $$ = $3; }
		;

xml_attribute_list:	xml_attribute_el					{ $$ = list_make1($1); }
			| xml_attribute_list ',' xml_attribute_el	{ $$ = lappend($1, $3); }
		;

xml_attribute_el: a_expr AS ColLabel
				{
					$$ = makeNode(ResTarget);
					$$->name = $3;
					$$->indirection = NIL;
					$$->val = (Node *) $1;
					$$->location = @1;
				}
			| a_expr
				{
					$$ = makeNode(ResTarget);
					$$->name = NULL;
					$$->indirection = NIL;
					$$->val = (Node *) $1;
					$$->location = @1;
				}
		;

document_or_content: DOCUMENT_P						{ $$ = XMLOPTION_DOCUMENT; }
			| CONTENT_P								{ $$ = XMLOPTION_CONTENT; }
		;

xml_whitespace_option: PRESERVE WHITESPACE_P		{ $$ = TRUE; }
			| STRIP_P WHITESPACE_P					{ $$ = FALSE; }
			| /*EMPTY*/								{ $$ = FALSE; }
		;

/* We allow several variants for SQL and other compatibility. */
xmlexists_argument:
			PASSING c_expr
				{
					$$ = $2;
				}
			| PASSING c_expr BY REF
				{
					$$ = $2;
				}
			| PASSING BY REF c_expr
				{
					$$ = $4;
				}
			| PASSING BY REF c_expr BY REF
				{
					$$ = $4;
				}
		;

/*
 * Aggregate decoration clauses
 */
within_group_clause:
			WITHIN GROUP_P '(' sort_clause ')'		{ $$ = $4; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

/*
 * Window Definitions
 */
window_clause:
			WINDOW window_definition_list			{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

window_definition_list:
			window_definition						{ $$ = list_make1($1); }
			| window_definition_list ',' window_definition
													{ $$ = lappend($1, $3); }
		;

window_definition:
			ColId AS window_specification
				{
					WindowDef *n = $3;
					n->name = $1;
					$$ = n;
				}
		;

over_clause: OVER window_specification
				{ $$ = $2; }
			| OVER ColId
				{
					WindowDef *n = makeNode(WindowDef);
					n->name = $2;
					n->refname = NULL;
					n->partitionClause = NIL;
					n->orderClause = NIL;
					n->frameOptions = FRAMEOPTION_DEFAULTS;
					n->startOffset = NULL;
					n->endOffset = NULL;
					n->location = @2;
					$$ = n;
				}
			| /*EMPTY*/
				{ $$ = NULL; }
		;

window_specification: '(' opt_existing_window_name opt_partition_clause
						opt_sort_clause opt_frame_clause ')'
				{
					WindowDef *n = makeNode(WindowDef);
					n->name = NULL;
					n->refname = $2;
					n->partitionClause = $3;
					n->orderClause = $4;
					/* copy relevant fields of opt_frame_clause */
					n->frameOptions = $5->frameOptions;
					n->startOffset = $5->startOffset;
					n->endOffset = $5->endOffset;
					n->location = @1;
					$$ = n;
				}
		;

/*
 * If we see PARTITION, RANGE, or ROWS as the first token after the '('
 * of a window_specification, we want the assumption to be that there is
 * no existing_window_name; but those keywords are unreserved and so could
 * be ColIds.  We fix this by making them have the same precedence as IDENT
 * and giving the empty production here a slightly higher precedence, so
 * that the shift/reduce conflict is resolved in favor of reducing the rule.
 * These keywords are thus precluded from being an existing_window_name but
 * are not reserved for any other purpose.
 */
opt_existing_window_name: ColId						{ $$ = $1; }
			| /*EMPTY*/				%prec Op		{ $$ = NULL; }
		;

opt_partition_clause: PARTITION BY expr_list		{ $$ = $3; }
			| /*EMPTY*/								{ $$ = NIL; }
		;

/*
 * For frame clauses, we return a WindowDef, but only some fields are used:
 * frameOptions, startOffset, and endOffset.
 *
 * This is only a subset of the full SQL:2008 frame_clause grammar.
 * We don't support <window frame exclusion> yet.
 */
opt_frame_clause:
			RANGE frame_extent
				{
					WindowDef *n = $2;
					n->frameOptions |= FRAMEOPTION_NONDEFAULT | FRAMEOPTION_RANGE;
					if (n->frameOptions & (FRAMEOPTION_START_VALUE_PRECEDING |
										   FRAMEOPTION_END_VALUE_PRECEDING))
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("RANGE PRECEDING is only supported with UNBOUNDED"),
								 parser_errposition(@1)));
					if (n->frameOptions & (FRAMEOPTION_START_VALUE_FOLLOWING |
										   FRAMEOPTION_END_VALUE_FOLLOWING))
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("RANGE FOLLOWING is only supported with UNBOUNDED"),
								 parser_errposition(@1)));
					$$ = n;
				}
			| ROWS frame_extent
				{
					WindowDef *n = $2;
					n->frameOptions |= FRAMEOPTION_NONDEFAULT | FRAMEOPTION_ROWS;
					$$ = n;
				}
			| /*EMPTY*/
				{
					WindowDef *n = makeNode(WindowDef);
					n->frameOptions = FRAMEOPTION_DEFAULTS;
					n->startOffset = NULL;
					n->endOffset = NULL;
					$$ = n;
				}
		;

frame_extent: frame_bound
				{
					WindowDef *n = $1;
					/* reject invalid cases */
					if (n->frameOptions & FRAMEOPTION_START_UNBOUNDED_FOLLOWING)
						ereport(ERROR,
								(errcode(ERRCODE_WINDOWING_ERROR),
								 errmsg("frame start cannot be UNBOUNDED FOLLOWING"),
								 parser_errposition(@1)));
					if (n->frameOptions & FRAMEOPTION_START_VALUE_FOLLOWING)
						ereport(ERROR,
								(errcode(ERRCODE_WINDOWING_ERROR),
								 errmsg("frame starting from following row cannot end with current row"),
								 parser_errposition(@1)));
					n->frameOptions |= FRAMEOPTION_END_CURRENT_ROW;
					$$ = n;
				}
			| BETWEEN frame_bound AND frame_bound
				{
					WindowDef *n1 = $2;
					WindowDef *n2 = $4;
					/* form merged options */
					int		frameOptions = n1->frameOptions;
					/* shift converts START_ options to END_ options */
					frameOptions |= n2->frameOptions << 1;
					frameOptions |= FRAMEOPTION_BETWEEN;
					/* reject invalid cases */
					if (frameOptions & FRAMEOPTION_START_UNBOUNDED_FOLLOWING)
						ereport(ERROR,
								(errcode(ERRCODE_WINDOWING_ERROR),
								 errmsg("frame start cannot be UNBOUNDED FOLLOWING"),
								 parser_errposition(@2)));
					if (frameOptions & FRAMEOPTION_END_UNBOUNDED_PRECEDING)
						ereport(ERROR,
								(errcode(ERRCODE_WINDOWING_ERROR),
								 errmsg("frame end cannot be UNBOUNDED PRECEDING"),
								 parser_errposition(@4)));
					if ((frameOptions & FRAMEOPTION_START_CURRENT_ROW) &&
						(frameOptions & FRAMEOPTION_END_VALUE_PRECEDING))
						ereport(ERROR,
								(errcode(ERRCODE_WINDOWING_ERROR),
								 errmsg("frame starting from current row cannot have preceding rows"),
								 parser_errposition(@4)));
					if ((frameOptions & FRAMEOPTION_START_VALUE_FOLLOWING) &&
						(frameOptions & (FRAMEOPTION_END_VALUE_PRECEDING |
										 FRAMEOPTION_END_CURRENT_ROW)))
						ereport(ERROR,
								(errcode(ERRCODE_WINDOWING_ERROR),
								 errmsg("frame starting from following row cannot have preceding rows"),
								 parser_errposition(@4)));
					n1->frameOptions = frameOptions;
					n1->endOffset = n2->startOffset;
					$$ = n1;
				}
		;

/*
 * This is used for both frame start and frame end, with output set up on
 * the assumption it's frame start; the frame_extent productions must reject
 * invalid cases.
 */
frame_bound:
			UNBOUNDED PRECEDING
				{
					WindowDef *n = makeNode(WindowDef);
					n->frameOptions = FRAMEOPTION_START_UNBOUNDED_PRECEDING;
					n->startOffset = NULL;
					n->endOffset = NULL;
					$$ = n;
				}
			| UNBOUNDED FOLLOWING
				{
					WindowDef *n = makeNode(WindowDef);
					n->frameOptions = FRAMEOPTION_START_UNBOUNDED_FOLLOWING;
					n->startOffset = NULL;
					n->endOffset = NULL;
					$$ = n;
				}
			| CURRENT_P ROW
				{
					WindowDef *n = makeNode(WindowDef);
					n->frameOptions = FRAMEOPTION_START_CURRENT_ROW;
					n->startOffset = NULL;
					n->endOffset = NULL;
					$$ = n;
				}
			| a_expr PRECEDING
				{
					WindowDef *n = makeNode(WindowDef);
					n->frameOptions = FRAMEOPTION_START_VALUE_PRECEDING;
					n->startOffset = $1;
					n->endOffset = NULL;
					$$ = n;
				}
			| a_expr FOLLOWING
				{
					WindowDef *n = makeNode(WindowDef);
					n->frameOptions = FRAMEOPTION_START_VALUE_FOLLOWING;
					n->startOffset = $1;
					n->endOffset = NULL;
					$$ = n;
				}
		;


/*
 * Supporting nonterminals for expressions.
 */

/* Explicit row production.
 *
 * SQL99 allows an optional ROW keyword, so we can now do single-element rows
 * without conflicting with the parenthesized a_expr production.  Without the
 * ROW keyword, there must be more than one a_expr inside the parens.
 */
row:		ROW '(' expr_list ')'					{ $$ = $3; }
			| ROW '(' ')'							{ $$ = NIL; }
			| '(' expr_list ',' a_expr ')'			{ $$ = lappend($2, $4); }
		;
explicit_row:	ROW '(' expr_list ')'				{ $$ = $3; }
			| ROW '(' ')'							{ $$ = NIL; }
		;

implicit_row:	'(' expr_list ',' a_expr ')'		{ $$ = lappend($2, $4); }
		;

sub_type:	ANY										{ $$ = ANY_SUBLINK; }
			| SOME									{ $$ = ANY_SUBLINK; }
			| ALL									{ $$ = ALL_SUBLINK; }
		;

all_Op:		Op										{ $$ = $1; }
			| CmpOp									{ $$ = $1; }
			| MathOp								{ $$ = $1; }
		;

MathOp:		 '+'									{ $$ = "+"; }
			| '-'									{ $$ = "-"; }
			| '*'									{ $$ = "*"; }
			| '/'									{ $$ = "/"; }
			| '%'									{ $$ = "%"; }
			| '^'									{ $$ = "^"; }
			| '<'									{ $$ = "<"; }
			| '>'									{ $$ = ">"; }
			| '='									{ $$ = "="; }
		;

qual_Op:	Op
					{ $$ = list_make1(makeString($1)); }
			| OPERATOR '(' any_operator ')'
					{ $$ = $3; }
		;

qual_all_Op:
			all_Op
					{ $$ = list_make1(makeString($1)); }
			| OPERATOR '(' any_operator ')'
					{ $$ = $3; }
		;

subquery_Op:
			all_Op
					{ $$ = list_make1(makeString($1)); }
			| OPERATOR '(' any_operator ')'
					{ $$ = $3; }
			| LIKE
					{ $$ = list_make1(makeString("~~")); }
			| NOT LIKE
					{ $$ = list_make1(makeString("!~~")); }
			| ILIKE
					{ $$ = list_make1(makeString("~~*")); }
			| NOT ILIKE
					{ $$ = list_make1(makeString("!~~*")); }
/* cannot put SIMILAR TO here, because SIMILAR TO is a hack.
 * the regular expression is preprocessed by a function (similar_escape),
 * and the ~ operator for posix regular expressions is used.
 *        x SIMILAR TO y     ->    x ~ similar_escape(y)
 * this transformation is made on the fly by the parser upwards.
 * however the SubLink structure which handles any/some/all stuff
 * is not ready for such a thing.
 */
			;

expr_list:	a_expr
				{
					$$ = list_make1($1);
				}
			| expr_list ',' a_expr
				{
					$$ = lappend($1, $3);
				}
		;

/* function arguments can have names */
func_arg_list:  func_arg_expr
				{
					$$ = list_make1($1);
				}
			| func_arg_list ',' func_arg_expr
				{
					$$ = lappend($1, $3);
				}
		;

func_arg_expr:  a_expr
				{
					$$ = $1;
				}
			| param_name COLON_EQUALS a_expr
				{
					NamedArgExpr *na = makeNode(NamedArgExpr);
					na->name = $1;
					na->arg = (Expr *) $3;
					na->argnumber = -1;		/* until determined */
					na->location = @1;
					$$ = (Node *) na;
				}
			/* add PARA_EQUALS for simulating A db assignment with argument name as "=>"*/
			| param_name PARA_EQUALS a_expr
				{
					NamedArgExpr *na = makeNode(NamedArgExpr);
					na->name = $1;
					na->arg = (Expr *) $3;
					na->argnumber = -1;		/* until determined */
					na->location = @1;
					$$ = (Node *) na;
				}
		;

type_list:	Typename								{ $$ = list_make1($1); }
			| type_list ',' Typename				{ $$ = lappend($1, $3); }
		;

array_expr: '[' expr_list ']'
				{
					$$ = makeAArrayExpr($2, @1);
				}
			| '[' array_expr_list ']'
				{
					$$ = makeAArrayExpr($2, @1);
				}
			| '[' ']'
				{
					$$ = makeAArrayExpr(NIL, @1);
				}
		;

array_expr_list: array_expr							{ $$ = list_make1($1); }
			| array_expr_list ',' array_expr		{ $$ = lappend($1, $3); }
		;


extract_list:
			extract_arg FROM a_expr
				{
					$$ = list_make2(makeStringConst($1, @1), $3);
				}
			| /*EMPTY*/								{ $$ = NIL; }
		;

/* Allow delimited string Sconst in extract_arg as an SQL extension.
 * - thomas 2001-04-12
 */
extract_arg:
			IDENT									{ $$ = $1; }
			| YEAR_P								{ $$ = "year"; }
			| MONTH_P								{ $$ = "month"; }
			| DAY_P									{ $$ = "day"; }
			| HOUR_P								{ $$ = "hour"; }
			| MINUTE_P								{ $$ = "minute"; }
			| SECOND_P								{ $$ = "second"; }
			| Sconst								{ $$ = $1; }
		;

timestamp_arg_list:
			timestamp_units ',' a_expr ',' a_expr
				{
					$$ = list_make3(makeStringConst($1, @1), $3, $5);
				}
			| /*EMPTY*/								{ $$ = NIL; }
		;

timestamp_units:
			IDENT									{ $$ = $1; }
			| YEAR_P								{ $$ = "year"; }
			| MONTH_P								{ $$ = "month"; }
			| DAY_P									{ $$ = "day"; }
			| HOUR_P								{ $$ = "hour"; }
			| MINUTE_P								{ $$ = "minute"; }
			| SECOND_P								{ $$ = "second"; }
			| Sconst								{ $$ = $1; }
		;


/* OVERLAY() arguments
 * SQL99 defines the OVERLAY() function:
 * o overlay(text placing text from int for int)
 * o overlay(text placing text from int)
 * and similarly for binary strings
 */
overlay_list:
			a_expr overlay_placing substr_from substr_for
				{
					$$ = list_make4($1, $2, $3, $4);
				}
			| a_expr overlay_placing substr_from
				{
					$$ = list_make3($1, $2, $3);
				}
		;

overlay_placing:
			PLACING a_expr
				{ $$ = $2; }
		;

/* position_list uses b_expr not a_expr to avoid conflict with general IN */

position_list:
			b_expr IN_P b_expr						{ $$ = list_make2($3, $1); }
			| /*EMPTY*/								{ $$ = NIL; }
		;

/* SUBSTRING() arguments
 * SQL9x defines a specific syntax for arguments to SUBSTRING():
 * o substring(text from int for int)
 * o substring(text from int) get entire string from starting point "int"
 * o substring(text for int) get first "int" characters of string
 * o substring(text from pattern) get entire string matching pattern
 * o substring(text from pattern for escape) same with specified escape char
 * We also want to support generic substring functions which accept
 * the usual generic list of arguments. So we will accept both styles
 * here, and convert the SQL9x style to the generic list for further
 * processing. - thomas 2000-11-28
 */
substr_list:
			a_expr substr_from substr_for
				{
					$$ = list_make3($1, $2, $3);
				}
			| a_expr substr_for substr_from
				{
					/* not legal per SQL99, but might as well allow it */
					$$ = list_make3($1, $3, $2);
				}
			| a_expr substr_from
				{
					$$ = list_make2($1, $2);
				}
			| a_expr substr_for
				{
					/*
					 * Since there are no cases where this syntax allows
					 * a textual FOR value, we forcibly cast the argument
					 * to int4.  The possible matches in pg_proc are
					 * substring(text,int4) and substring(text,text),
					 * and we don't want the parser to choose the latter,
					 * which it is likely to do if the second argument
					 * is unknown or doesn't have an implicit cast to int4.
					 */
					$$ = list_make3($1, makeIntConst(1, -1),
									makeTypeCast($2,
												 SystemTypeName("int4"), -1));
				}
			| expr_list
				{
					$$ = $1;
				}
			| /*EMPTY*/
				{ $$ = NIL; }
		;

substr_from:
			FROM a_expr								{ $$ = $2; }
		;

substr_for: FOR a_expr								{ $$ = $2; }
		;

trim_list:	a_expr FROM expr_list					{ $$ = lappend($3, $1); }
			| FROM expr_list						{ $$ = $2; }
			| expr_list								{ $$ = $1; }
		;

in_expr:	select_with_parens
				{
					SubLink *n = makeNode(SubLink);
					n->subselect = $1;
					/* other fields will be filled later */
					$$ = (Node *)n;
				}
			| '(' expr_list ')'						{ $$ = (Node *)$2; }
		;

/*
 * Define SQL92-style case clause.
 * - Full specification
 *	CASE WHEN a = b THEN c ... ELSE d END
 * - Implicit argument
 *	CASE a WHEN b THEN c ... ELSE d END
 */
case_expr:	CASE case_arg when_clause_list case_default END_P
				{
					CaseExpr *c = makeNode(CaseExpr);
					c->casetype = InvalidOid; /* not analyzed yet */
					c->arg = (Expr *) $2;
					c->args = $3;
					c->defresult = (Expr *) $4;
					c->location = @1;
					$$ = (Node *)c;
				}
		| DECODE '(' a_expr ',' expr_list ')'
				{
					if(list_length($5) < 2)
					{
						FuncCall *n = makeNode(FuncCall);
						n->funcname = SystemFuncName("decode");
						n->args = list_concat(list_make1($3), $5);
						n->agg_order = NIL;
						n->agg_star = FALSE;
						n->agg_distinct = FALSE;
						n->func_variadic = FALSE;
						n->over = NULL;
						n->location = @1;
						n->call_func = false;
						$$ = (Node *)n;
					}
					else
					{
						ListCell *cell = NULL;
						CaseExpr *c = makeNode(CaseExpr);
						c->casetype = InvalidOid; /* not analyzed yet */
						c->arg = NULL;
						c->args = NULL;
						c->defresult = NULL;

						foreach(cell,$5)
						{
							Expr *expr1 = NULL;
							Expr *expr2 = NULL;
							CaseWhen *w = NULL;
							expr1 = (Expr*) lfirst(cell);
							cell = lnext(cell);
							if(NULL == cell)
							{
								c->defresult = expr1;
								break;
							}
							expr2 = (Expr*) lfirst(cell);
							w = makeNode(CaseWhen);
							w->expr = makeNodeDecodeCondtion((Expr*)$3,expr1);
							w->result = expr2;
							c->args = lappend(c->args,w);
						}
						$$ = (Node *)c;
					}
				}
		;

when_clause_list:
			/* There must be at least one */
			when_clause								{ $$ = list_make1($1); }
			| when_clause_list when_clause			{ $$ = lappend($1, $2); }
		;

when_clause:
			WHEN a_expr THEN a_expr
				{
					CaseWhen *w = makeNode(CaseWhen);
					w->expr = (Expr *) $2;
					w->result = (Expr *) $4;
					w->location = @1;
					$$ = (Node *)w;
				}
		;

case_default:
			ELSE a_expr								{ $$ = $2; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

case_arg:	a_expr									{ $$ = $1; }
			| /*EMPTY*/								{ $$ = NULL; }
		;

columnref:	ColId
				{
					$$ = makeColumnRef($1, NIL, @1, yyscanner);
				}
			| ColId indirection
				{
					$$ = makeColumnRef($1, $2, @1, yyscanner);
				}
			| EXCLUDED indirection
				{
					$$ = makeColumnRef("excluded", $2, @2, yyscanner);
				}
		;

indirection_el:
			'.' attr_name
				{
					$$ = (Node *) makeString($2);
				}
			| ORA_JOINOP
				{
					$$ = (Node *) makeString("(+)");
				}
			| '.' '*'
				{
					$$ = (Node *) makeNode(A_Star);
				}
			| '[' a_expr ']'
				{
					A_Indices *ai = makeNode(A_Indices);
					ai->lidx = NULL;
					ai->uidx = $2;
					$$ = (Node *) ai;
				}
			| '[' a_expr ':' a_expr ']'
				{
					A_Indices *ai = makeNode(A_Indices);
					ai->lidx = $2;
					ai->uidx = $4;
					$$ = (Node *) ai;
				}
			| '[' a_expr ',' a_expr ']'
				{
					A_Indices *ai = makeNode(A_Indices);
					ai->lidx = $2;
					ai->uidx = $4;
					$$ = (Node *) ai;
				}
		;

indirection:
			indirection_el							{ $$ = list_make1($1); }
			| indirection indirection_el			{ $$ = lappend($1, $2); }
		;

opt_indirection:
			/*EMPTY*/								{ $$ = NIL; }
			| opt_indirection indirection_el		{ $$ = lappend($1, $2); }
		;

opt_asymmetric: ASYMMETRIC
			| /*EMPTY*/
		;

/*
 * The SQL spec defines "contextually typed value expressions" and
 * "contextually typed row value constructors", which for our purposes
 * are the same as "a_expr" and "row" except that DEFAULT can appear at
 * the top level.
 */

ctext_expr:
			a_expr					{ $$ = (Node *) $1; }
			| DEFAULT
				{
					SetToDefault *n = makeNode(SetToDefault);
					n->location = @1;
					$$ = (Node *) n;
				}
		;

ctext_expr_list:
			ctext_expr								{ $$ = list_make1($1); }
			| ctext_expr_list ',' ctext_expr		{ $$ = lappend($1, $3); }
		;

/*
 * We should allow ROW '(' ctext_expr_list ')' too, but that seems to require
 * making VALUES a fully reserved word, which will probably break more apps
 * than allowing the noise-word is worth.
 */
ctext_row: '(' ctext_expr_list ')'					{ $$ = $2; }
		;


/*****************************************************************************
 *
 *	target list for SELECT
 *
 *****************************************************************************/

target_list:
			target_el								{ $$ = list_make1($1); }
			| target_list ',' target_el				{ $$ = lappend($1, $3); }
		;

target_el:	a_expr AS ColLabel
				{
					$$ = makeNode(ResTarget);
					$$->name = $3;
					$$->indirection = NIL;
					$$->val = (Node *)$1;
					$$->location = @1;
				}
			/*
			 * We support omitting AS only for column labels that aren't
			 * any known keyword.  There is an ambiguity against postfix
			 * operators: is "a ! b" an infix expression, or a postfix
			 * expression and a column label?  We prefer to resolve this
			 * as an infix expression, which we accomplish by assigning
			 * IDENT a precedence higher than POSTFIXOP.
			 */
			| a_expr IDENT
				{
					$$ = makeNode(ResTarget);
					$$->name = $2;
					$$->indirection = NIL;
					$$->val = (Node *)$1;
					$$->location = @1;
				}
			| a_expr
				{
					$$ = makeNode(ResTarget);
					$$->name = NULL;
					$$->indirection = NIL;
					$$->val = (Node *)$1;
					$$->location = @1;
				}
			| '*'
				{
					ColumnRef *n = makeNode(ColumnRef);
					n->fields = list_make1(makeNode(A_Star));
					n->location = @1;

					$$ = makeNode(ResTarget);
					$$->name = NULL;
					$$->indirection = NIL;
					$$->val = (Node *)n;
					$$->location = @1;
				}
			| c_expr VALUE_P
				{
					$$ = makeNode(ResTarget);
					$$->name = pstrdup($2);
					$$->indirection = NIL;
					$$->val = (Node *)$1;
					$$->location = @1;
				}
			| c_expr NAME_P
				{
					$$ = makeNode(ResTarget);
					$$->name = pstrdup($2);
					$$->indirection = NIL;
					$$->val = (Node *)$1;
					$$->location = @1;
				}
			| c_expr TYPE_P
				{
					$$ = makeNode(ResTarget);
					$$->name = pstrdup($2);
					$$->indirection = NIL;
					$$->val = (Node *)$1;
					$$->location = @1;
				}
		;


/*****************************************************************************
 *
 *	Names and constants
 *
 *****************************************************************************/

qualified_name_list:
			qualified_name							{ $$ = list_make1($1); }
			| qualified_name_list ',' qualified_name { $$ = lappend($1, $3); }
		;

/*
 * The production for a qualified relation name has to exactly match the
 * production for a qualified func_name, because in a FROM clause we cannot
 * tell which we are parsing until we see what comes after it ('(' for a
 * func_name, something else for a relation). Therefore we allow 'indirection'
 * which may contain subscripts, and reject that case in the C code.
 */
qualified_name:
			ColId
				{
					$$ = makeRangeVar(NULL, $1, @1);
				}
			| ColId indirection
				{
					check_qualified_name($2, yyscanner);
					$$ = makeRangeVar(NULL, NULL, @1);
					switch (list_length($2))
					{
						case 1:
							$$->catalogname = NULL;
							$$->schemaname = $1;
							$$->relname = strVal(linitial($2));
							break;
						case 2:
							$$->catalogname = $1;
							$$->schemaname = strVal(linitial($2));
							$$->relname = strVal(lsecond($2));
							break;
						default:
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("improper qualified name (too many dotted names): %s",
											NameListToString(lcons(makeString($1), $2))),
									 parser_errposition(@1)));
							break;
					}
				}
		;

name_list:	name
					{ $$ = list_make1(makeString($1)); }
			| name_list ',' name
					{ $$ = lappend($1, makeString($3)); }
		;


name:		ColId									{ $$ = $1; };

database_name:
			ColId									{ $$ = $1; };

access_method:
			ColId									{ $$ = $1; };

attr_name:	ColLabel								{ $$ = $1; };

index_name: ColId									{ $$ = $1; };

file_name:	Sconst									{ $$ = $1; };

/*
 * The production for a qualified func_name has to exactly match the
 * production for a qualified columnref, because we cannot tell which we
 * are parsing until we see what comes after it ('(' or Sconst for a func_name,
 * anything else for a columnref).  Therefore we allow 'indirection' which
 * may contain subscripts, and reject that case in the C code.  (If we
 * ever implement SQL99-like methods, such syntax may actually become legal!)
 */
func_name:	type_function_name
					{ $$ = list_make1(makeString($1)); }
			| ColId indirection
					{
						$$ = check_func_name(lcons(makeString($1), $2),
											 yyscanner);
					}
		;

func_name_opt_arg:
						func_name
						/* This rule is never used. */
						| IDENT BOGUS							{ $$ = NIL; }
						/* This rule is never used. */
						| unreserved_keyword BOGUS				{ $$ = NIL; };

/*
 * Constants
 */
AexprConst: Iconst
				{
					$$ = makeIntConst($1, @1);
				}
			| FCONST
				{
					$$ = makeFloatConst($1, @1);
				}
			| Sconst
				{
					$$ = makeStringConst($1, @1);
				}
			| BCONST
				{
					$$ = makeBitStringConst($1, @1);
				}
			| XCONST
				{
					/* This is a bit constant per SQL99:
					 * Without Feature F511, "BIT data type",
					 * a <general literal> shall not be a
					 * <bit string literal> or a <hex string literal>.
					 */
					$$ = makeBitStringConst($1, @1);
				}
			| func_name Sconst
				{
					/* generic type 'literal' syntax */
					TypeName *t = makeTypeNameFromNameList($1);
					t->location = @1;
					$$ = makeStringConstCast($2, @2, t);
				}
			| func_name '(' func_arg_list opt_sort_clause ')' Sconst
				{
					/* generic syntax with a type modifier */
					TypeName *t = makeTypeNameFromNameList($1);
					ListCell *lc;

					/*
					 * We must use func_arg_list and opt_sort_clause in the production to avoid
					 * reduce/reduce conflicts, but we don't actually wish
					 * to allow NamedArgExpr in this context, nor ORDER BY.
					 */
					foreach(lc, $3)
					{
						NamedArgExpr *arg = (NamedArgExpr *) lfirst(lc);

						if (IsA(arg, NamedArgExpr))
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("type modifier cannot have parameter name"),
									 parser_errposition(arg->location)));
					}

					if ($4 != NIL)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("type modifier cannot have ORDER BY"),
									 parser_errposition(@4)));
					t->typmods = $3;
					t->location = @1;
					$$ = makeStringConstCast($6, @6, t);
				}
			| ConstTypename Sconst
				{
					$$ = makeStringConstCast($2, @2, $1);
				}
			| ConstInterval Sconst opt_interval
				{
					TypeName *t = $1;
					t->typmods = $3;
					$$ = makeStringConstCast($2, @2, t);
				}
			| ConstInterval '(' Iconst ')' Sconst opt_interval
				{
					TypeName *t = $1;
					if ($6 != NIL)
					{
						if (list_length($6) != 1)
							ereport(ERROR,
									(errcode(ERRCODE_SYNTAX_ERROR),
									 errmsg("interval precision specified twice"),
									 parser_errposition(@1)));
						t->typmods = lappend($6, makeIntConst($3, @3));
					}
					else
						t->typmods = list_make2(makeIntConst(INTERVAL_FULL_RANGE, -1),
												makeIntConst($3, @3));
					$$ = makeStringConstCast($5, @5, t);
				}
			| TRUE_P
				{
					$$ = makeBoolAConst(TRUE, @1);
				}
			| FALSE_P
				{
					$$ = makeBoolAConst(FALSE, @1);
				}
			| NULL_P
				{
					$$ = makeNullAConst(@1);
				}
		;

Iconst:		ICONST									{ $$ = $1; };
Sconst:		SCONST									{ $$ = $1; };
RoleId:		ColId									{ $$ = $1; };

SignedIconst: Iconst								{ $$ = $1; }
			| '+' Iconst							{ $$ = + $2; }
			| '-' Iconst							{ $$ = - $2; }
		;

/*
 * Name classification hierarchy.
 *
 * IDENT is the lexeme returned by the lexer for identifiers that match
 * no known keyword.  In most cases, we can accept certain keywords as
 * names, not only IDENTs.	We prefer to accept as many such keywords
 * as possible to minimize the impact of "reserved words" on programmers.
 * So, we divide names into several possible classes.  The classification
 * is chosen in part to make keywords acceptable as names wherever possible.
 */

/* Column identifier --- names that can be column, table, etc names.
 */
ColId:		IDENT									{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
			| col_name_keyword						{ $$ = pstrdup($1); }
		;

/* Type/function identifier --- names that can be type or function names.
 */
type_function_name:	IDENT							{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
			| type_func_name_keyword				{ $$ = pstrdup($1); }
		;

/* Column label --- allowed labels in "AS" clauses.
 * This presently includes *all* Postgres keywords.
 */
ColLabel:	IDENT									{ $$ = $1; }
			| unreserved_keyword					{ $$ = pstrdup($1); }
			| col_name_keyword						{ $$ = pstrdup($1); }
			| type_func_name_keyword				{ $$ = pstrdup($1); }
			| reserved_keyword
				{
					/* ROWNUM can not be used as alias */
					if (strcmp($1, "rownum") == 0) {
						ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
								errmsg("ROWNUM cannot be used as an alias"),
										parser_errposition(@1)));
					}
					$$ = pstrdup($1);
				}
		;


/*
 * Keyword category lists.  Generally, every keyword present in
 * the Postgres grammar should appear in exactly one of these lists.
 *
 * Put a new keyword into the first list that it can go into without causing
 * shift or reduce conflicts.  The earlier lists define "less reserved"
 * categories of keywords.
 *
 * Make sure that each keyword's category in kwlist.h matches where
 * it is listed here.  (Someday we may be able to generate these lists and
 * kwlist.h's table from a common master list.)
 */

/* "Unreserved" keywords --- available for use as any kind of name.
 */
/* PGXC - added DISTRIBUTE, DIRECT, COORDINATOR, DATANODES, CLEAN, NODE, BARRIER, SLICE, DATANODE */
unreserved_keyword:
			  ABORT_P
			| ABSOLUTE_P
			| ACCESS
			| ACCOUNT
			| ACTION
			| ADD_P
			| ADMIN
			| AFTER
			| AGGREGATE
			| ALGORITHM
			| ALSO
			| ALTER
			| ALWAYS
			| APP
			| ASSERTION
			| ASSIGNMENT
			| AT
			| ATTRIBUTE
			| AUDIT
			| AUTOEXTEND
			| AUTOMAPPED
			| BACKWARD
/* PGXC_BEGIN */
			| BARRIER
/* PGXC_END */
			| BEFORE
			| BEGIN_NON_ANOYBLOCK
			| BEGIN_P
			| BLOB_P
			| BY
			| CACHE
			| CALL
			| CALLED
			| CASCADE
			| CASCADED
			| CATALOG_P
			| CHAIN
			| CHARACTERISTICS
			| CHECKPOINT
			| CLASS
			| CLEAN
			| CLIENT
            | CLIENT_MASTER_KEY
            | CLIENT_MASTER_KEYS
			| CLOB
			| CLOSE
			| CLUSTER
            | COLUMN_ENCRYPTION_KEY
            | COLUMN_ENCRYPTION_KEYS
			| COMMENT
			| COMMENTS
			| COMMIT
			| COMMITTED
			| COMPATIBLE_ILLEGAL_CHARS
			| COMPLETE
			| COMPRESS
			| CONDITION
			| CONFIGURATION
			| CONNECTION
			| CONSTRAINTS
			| CONTENT_P
			| CONTINUE_P
			| CONVERSION_P
            | CONTVIEW
			| COORDINATOR
			| COPY
			| COST
			| CSV
			| CUBE
			| CURRENT_P
			| CURSOR
			| CYCLE
			| DATA_P
			| DATABASE
			| DATAFILE
			| DATANODE
			| DATANODES
			| DATATYPE_CL
			| DAY_P
			| DATE_FORMAT_P
			| DBCOMPATIBILITY_P
			| DEALLOCATE
			| DECLARE
			| DEFAULTS
			| DEFERRED
			| DEFINER
			| DELETE_P
			| DELIMITER
			| DELIMITERS
			| DELTA
			| DETERMINISTIC
			| DICTIONARY
			| DIRECT
			| DIRECTORY
			| DISABLE_P
			| DISCARD
			| DISCONNECT
/* PGXC_BEGIN */
			| DISTRIBUTE
			| DISTRIBUTION
/* PGXC_END */
			| DOCUMENT_P
			| DOMAIN_P
			| DOUBLE_P
			| DROP
			| DUPLICATE
			| EACH
			| ENABLE_P
			| ENCODING
			| ENCRYPTED       
            | ENCRYPTED_VALUE
			| ENCRYPTION
            | ENCRYPTION_TYPE
			| ENFORCED
			| ENUM_P
			| EOL
			| ERRORS
			| ESCAPE
			| ESCAPING
			| EVERY
			| EXCHANGE
			| EXCLUDE
			| EXCLUDING
			| EXCLUSIVE
			| EXECUTE
			| EXPIRED_P
			| EXPLAIN
			| EXTENSION
			| EXTERNAL
			| FAMILY
			| FAST
			| FILEHEADER_P
			| FILL_MISSING_FIELDS
			| FILTER
			| FIRST_P
			| FIXED_P
			| FOLLOWING
			| FORCE
			| FORMATTER
			| FORWARD
			| FUNCTION
			| FUNCTIONS
			| GLOBAL
			| GLOBAL_FUNCTION
			| GRANTED
			| HANDLER
			| HEADER_P
			| HOLD
			| HOUR_P
			| IDENTIFIED
			| IDENTITY_P
			| IF_P
			| IGNORE_EXTRA_DATA
			| IMMEDIATE
			| IMMUTABLE
			| IMPLICIT_P
			| INCLUDE
			| INCLUDING
			| INCREMENT
			| INCREMENTAL
			| INDEX
			| INDEXES
			| INHERIT
			| INHERITS
			| INITIAL_P
			| INITRANS
			| INLINE_P
			| INPUT_P
			| INTERNAL
			| INSENSITIVE
			| INSERT
			| INSTEAD
			| INVOKER
			| IP
			| ISNULL
			| ISOLATION
			| KEY
			| KEY_PATH
			| KEY_STORE
			| KILL
			| LABEL
			| LANGUAGE
			| LARGE_P
			| LAST_P
			| LC_COLLATE_P
			| LC_CTYPE_P
			| LEAKPROOF
			| LEVEL
			| LIST
			| LISTEN
			| LOAD
			| LOCAL
			| LOCATION
			| LOCK_P
			| LOG_P
			| LOGGING
			| LOGIN_ANY
			| LOGIN_SUCCESS
			| LOGIN_FAILURE
			| LOGOUT
			| LOOP
			| MAPPING
			| MASKING
			| MASTER
			| MATCH
			| MATCHED
			| MATERIALIZED
			| MAXEXTENTS
			| MAXSIZE
			| MAXTRANS
			| MERGE
			| MINEXTENTS
			| MINUTE_P
			| MINVALUE
			| MODE
			| MONTH_P
			| MOVE
			| MOVEMENT
			| NAME_P
			| NAMES
			| NEXT
			| NO
			| NOCOMPRESS
			| NOCYCLE
			| NODE
			| NOLOGGING
			| NOMAXVALUE
			| NOMINVALUE
			| NOTHING
			| NOTIFY
			| NOWAIT
			| NULLS_P
			| NUMSTR
			| OBJECT_P
			| OF
			| OFF
			| OIDS
			| OPERATOR
			| OPTIMIZATION
			| OPTION
			| OPTIONS
			| OVER
			| OWNED
			| OWNER
			| PACKAGE
			| PARSER
			| PARTIAL %prec PARTIAL_EMPTY_PREC
			| PARTITION
			| PARTITIONS
			| PASSING
			| PASSWORD
			| PCTFREE
			| PER_P
			| PERCENT
			| PERM
			| PLAN
			| PLANS
			| POLICY
			| POOL
			| PRECEDING
/* PGXC_BEGIN */
			| PREFERRED
/* PGXC_END */
			| PREFIX
			| PREPARE
			| PREPARED
			| PRESERVE
			| PRIOR
			| PRIVATE
			| PRIVILEGE
			| PRIVILEGES
			| PROCEDURAL
			| PROFILE
			| QUERY
			| QUOTE
			| RANDOMIZED
			| RANGE
			| RAW  '(' Iconst ')'				{	$$ = "raw";}
			| RAW  %prec UNION				{	$$ = "raw";}
			| READ
			| REASSIGN
			| REBUILD
			| RECHECK
			| RECURSIVE
			| REDISANYVALUE
			| REF
			| REFRESH
			| REINDEX
			| RELATIVE_P
			| RELEASE
			| RELOPTIONS
			| REMOTE_P
			| REMOVE
			| RENAME
			| REPEATABLE
			| REPLACE
			| REPLICA
			| RESET
			| RESIZE
			| RESOURCE
			| RESTART
			| RESTRICT
			| RETURN
			| RETURNS
			| REUSE
			| REVOKE
			| ROLE
			| ROLES
			| ROLLBACK
			| ROLLUP
			| ROWS
			| RULE
			| SAVEPOINT
			| SCHEMA
			| SCROLL
			| SEARCH
			| SECOND_P
			| SECURITY
			| SEQUENCE
			| SEQUENCES
			| SERIALIZABLE
			| SERVER
			| SESSION
			| SET
			| SETS
			| SHARE
			| SHIPPABLE
			| SHOW
			| SHUTDOWN
			| SIMPLE
			| SIZE
			| SLICE
			| SMALLDATETIME_FORMAT_P
			| SNAPSHOT
			| SOURCE_P
			| SPACE
			| SPILL
			| SPLIT
			| STABLE
			| STANDALONE_P
			| START
			| STATEMENT
			| STATEMENT_ID
			| STATISTICS
			| STDIN
			| STDOUT
			| STORAGE
			| STORE_P
            | STREAM
			| STRICT_P
			| STRIP_P
			| SYNONYM
			| SYS_REFCURSOR					{ $$ = "refcursor"; }
			| SYSID
			| SYSTEM_P
			| TABLES
			| TABLESPACE
			| TEMP
			| TEMPLATE
			| TEMPORARY
			| TEXT_P
			| THAN
			| TIME_FORMAT_P
			| TIMESTAMP_FORMAT_P
			| TRANSACTION
			| TRIGGER
			| TRUNCATE
			| TRUSTED
			| TSFIELD
			| TSTAG
			| TSTIME 
			| TYPE_P
			| TYPES_P
			| UNBOUNDED
			| UNCOMMITTED
			| UNENCRYPTED
			| UNKNOWN
			| UNLIMITED
			| UNLISTEN
			| UNLOCK
			| UNLOGGED
			| UNTIL
			| UNUSABLE
			| UPDATE
			| VACUUM
			| VALID
			| VALIDATE
			| VALIDATION
			| VALIDATOR
			| VALUE_P
			| VARYING
			| VERSION_P
			| VIEW
			| VOLATILE
			| WEAK
			| WHITESPACE_P
			| WITHIN
			| WITHOUT
			| WORK
			| WORKLOAD
			| WRAPPER
			| WRITE
			| XML_P
			| YEAR_P
			| YES_P
			| ZONE
		;

/* Column identifier --- keywords that can be column, table, etc names.
 *
 * Many of these keywords will in fact be recognized as type or function
 * names too; but they have special productions for the purpose, and so
 * can't be treated as "generic" type or function names.
 *
 * The type names appearing here are not usable as function names
 * because they can be followed by '(' in typename productions, which
 * looks too much like a function call for an LR(1) parser.
 */
col_name_keyword:
			  BETWEEN
			| BIGINT
			| BINARY_DOUBLE
			| BINARY_INTEGER
			| BIT
			| BOOLEAN_P
			| BYTEAWITHOUTORDER
			| BYTEAWITHOUTORDERWITHEQUAL
			| CHAR_P
			| CHARACTER
			| COALESCE
			| DATE_P
			| DEC
			| DECIMAL_P
			| DECODE
			| EXISTS
			| EXTRACT
			| FLOAT_P
			| GREATEST
			| GROUPING_P
			| INOUT
			| INT_P
			| INTEGER
			| INTERVAL
			| LEAST
			| NATIONAL
			| NCHAR
			| NONE
			| NULLIF
			| NUMBER_P
			| NUMERIC
			| NVARCHAR2
			| NVL
			| OUT_P
			| OVERLAY
			| POSITION
			| PRECISION
			| REAL
			| ROW
			| SETOF
			| SMALLDATETIME
			| SMALLINT
			| SUBSTRING
			| TIME
			| TIMESTAMP
			| TIMESTAMPDIFF
			| TINYINT
			| TREAT
			| TRIM
			| VALUES
			| VARCHAR
			| VARCHAR2
			| XMLATTRIBUTES
			| XMLCONCAT
			| XMLELEMENT
			| XMLEXISTS
			| XMLFOREST
			| XMLPARSE
			| XMLPI
			| XMLROOT
			| XMLSERIALIZE
		;

/* Type/function identifier --- keywords that can be type or function names.
 *
 * Most of these are keywords that are used as operators in expressions;
 * in general such keywords can't be column names because they would be
 * ambiguous with variables, but they are unambiguous as function identifiers.
 *
 * Do not include POSITION, SUBSTRING, etc here since they have explicit
 * productions in a_expr to support the goofy SQL9x argument syntax.
 * - thomas 2000-11-28
 */
type_func_name_keyword:
			 AUTHORIZATION
			| BINARY
			| COLLATION
			| COMPACT
			| CONCURRENTLY
			| CROSS
			| CURRENT_SCHEMA
			| DELTAMERGE
			| FREEZE
			| FULL
			| HDFSDIRECTORY
			| ILIKE
			| INNER_P
			| JOIN
			| LEFT
			| LIKE
			| NATURAL
			| NOTNULL
			| OUTER_P
			| OVERLAPS
			| RIGHT
			| SIMILAR
			| TABLESAMPLE
			| VERBOSE
		;

/* Reserved keyword --- these keywords are usable only as a ColLabel.
 *
 * Keywords appear here if they could not be distinguished from variable,
 * type, or function names in some contexts.  Don't put things here unless
 * forced to.
 */
reserved_keyword:
			  ALL
			| ANALYSE
			| ANALYZE
			| AND
			| ANY
			| ARRAY
			| AS
			| ASC
			| ASYMMETRIC
			| AUTHID
			| BOTH
			| BUCKETS
			| CASE
			| CAST
			| CHECK
			| COLLATE
			| COLUMN
			| CONSTRAINT
			| CREATE
			| CURRENT_CATALOG
			| CURRENT_DATE
			| CURRENT_ROLE
			| CURRENT_TIME
			| CURRENT_TIMESTAMP
			| CURRENT_USER
			| DEFAULT
			| DEFERRABLE
			| DESC
			| DISTINCT
			| DO
			| ELSE
			| END_P
			| EXCEPT
			| EXCLUDED
			| FALSE_P
			| FETCH
			| FOR
			| FOREIGN
			| FROM
			| GRANT
			| GROUP_P
			| HAVING
			| IN_P
			| INITIALLY
			| INTERSECT
			| INTO
			| IS
			| LEADING
			| LESS
			| LIMIT
			| LOCALTIME
			| LOCALTIMESTAMP
			| MAXVALUE
			| MINUS_P
			| MODIFY_P
			| NLSSORT
			| NOT
			| NULL_P
			| OFFSET
			| ON
			| ONLY
			| OR
			| ORDER
			| PERFORMANCE
			| PLACING
			| PRIMARY
			| PROCEDURE
			| REFERENCES
			| REJECT_P
			| RETURNING
			| SELECT
			| SESSION_USER
			| SOME
			| SYMMETRIC
			| SYSDATE
			| TABLE
			| THEN
			| TO
			| TRAILING
			| TRUE_P
			| UNION
			| UNIQUE
			| USER
			| USING
			| VARIADIC
			| VERIFY
			| WHEN
			| WHERE
			| WINDOW
			| WITH
			| ROWNUM
		;

%%

/*
 * The signature of this function is required by bison.  However, we
 * ignore the passed yylloc and instead use the last token position
 * available from the scanner.
 */
static void
base_yyerror(YYLTYPE *yylloc, core_yyscan_t yyscanner, const char *msg)
{
	parser_yyerror(msg);
}

static Node *
makeColumnRef(char *colname, List *indirection,
			  int location, core_yyscan_t yyscanner)
{
	/*
	 * Generate a ColumnRef node, with an A_Indirection node added if there
	 * is any subscripting in the specified indirection list.  However,
	 * any field selection at the start of the indirection list must be
	 * transposed into the "fields" part of the ColumnRef node.
	 */
	ColumnRef  *c = makeNode(ColumnRef);
	int		nfields = 0;
	ListCell *l;

	c->location = location;
	foreach(l, indirection)
	{
		if (IsA(lfirst(l), A_Indices))
		{
			A_Indirection *i = makeNode(A_Indirection);

			if (nfields == 0)
			{
				/* easy case - all indirection goes to A_Indirection */
				c->fields = list_make1(makeString(colname));
				i->indirection = check_indirection(indirection, yyscanner);
			}
			else
			{
				/* got to split the list in two */
				i->indirection = check_indirection(list_copy_tail(indirection,
																  nfields),
												   yyscanner);
				indirection = list_truncate(indirection, nfields);
				c->fields = lcons(makeString(colname), indirection);
			}
			i->arg = (Node *) c;
			return (Node *) i;
		}
		else if (IsA(lfirst(l), A_Star))
		{
			/* We only allow '*' at the end of a ColumnRef */
			if (lnext(l) != NULL)
				parser_yyerror("improper use of \"*\"");
		}
		else if (IsA(lfirst(l), String) && strncmp(strVal(lfirst(l)), "(+)", 3) == 0)
		{
			u_sess->parser_cxt.stmt_contains_operator_plus = true;
		}
		nfields++;
	}
	/* No subscripting, so all indirection gets added to field list */
	c->fields = lcons(makeString(colname), indirection);
	return (Node *) c;
}

static Node *
makeTypeCast(Node *arg, TypeName *typname, int location)
{
	TypeCast *n = makeNode(TypeCast);
	n->arg = arg;
	n->typname = typname;
	n->location = location;
	return (Node *) n;
}

static Node *
makeStringConst(char *str, int location)
{
	A_Const *n = makeNode(A_Const);


	if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT)
	{
		if (NULL == str || 0 == strlen(str))
		{
			n->val.type = T_Null;
			n->val.val.str = str;
			n->location = location;
		}
		else
		{
			n->val.type = T_String;
			n->val.val.str = str;
			n->location = location;
		}
	}
	else
	{
		n->val.type = T_String;
		n->val.val.str = str;
		n->location = location;
	}

	return (Node *)n;
}

static Node *
makeStringConstCast(char *str, int location, TypeName *typname)
{
	Node *s = makeStringConst(str, location);

	return makeTypeCast(s, typname, -1);
}

static Node *
makeIntConst(int val, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_Integer;
	n->val.val.ival = val;
	n->location = location;

	return (Node *)n;
}

static Node *
makeFloatConst(char *str, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_Float;
	n->val.val.str = str;
	n->location = location;

	return (Node *)n;
}

static Node *
makeBitStringConst(char *str, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_BitString;
	n->val.val.str = str;
	n->location = location;

	return (Node *)n;
}

Node *
makeAConst(Value *v, int location)
{
	Node *n;

	switch (v->type)
	{
		case T_Float:
			n = makeFloatConst(v->val.str, location);
			break;

		case T_Integer:
			n = makeIntConst(v->val.ival, location);
			break;

		case T_String:
		default:
			n = makeStringConst(v->val.str, location);
			break;
	}

	return n;
}

/* makeBoolAConst()
 * Create an A_Const string node and put it inside a boolean cast.
 */
Node *
makeBoolAConst(bool state, int location)
{
	A_Const *n = makeNode(A_Const);

	n->val.type = T_String;
	n->val.val.str = (char *)(state ? "t" : "f");
	n->location = location;

	return makeTypeCast((Node *)n, SystemTypeName("bool"), -1);
}

/* check_qualified_name --- check the result of qualified_name production
 *
 * It's easiest to let the grammar production for qualified_name allow
 * subscripts and '*', which we then must reject here.
 */
static void
check_qualified_name(List *names, core_yyscan_t yyscanner)
{
	ListCell   *i;

	foreach(i, names)
	{
		if (!IsA(lfirst(i), String))
			parser_yyerror("syntax error");
	}
}

/* check_func_name --- check the result of func_name production
 *
 * It's easiest to let the grammar production for func_name allow subscripts
 * and '*', which we then must reject here.
 */
static List *
check_func_name(List *names, core_yyscan_t yyscanner)
{
	ListCell   *i;

	foreach(i, names)
	{
		if (!IsA(lfirst(i), String))
			parser_yyerror("syntax error");
	}
	return names;
}

bool
IsValidIdentClientKey(const char *input)
{
	if (input == NULL || strlen(input) <= 0) {
		return false;
	}
	char c = input[0];
	/*The first character id numbers or dollar or point*/
	if ((c >= '0' && c <= '9') || c == '$' || c == '.')
	{
		return false;
	}

	int len = strlen(input);
	for (int i = 0; i < len; i++)
	{
		c = input[i];
		if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '$'|| c=='.')
		{
			continue;
		}
		else
		{
			return false;
		}
	}
	return true;
}

static List *
check_setting_name(List *names, core_yyscan_t yyscanner)
{
	ListCell   *i;

	foreach(i, names)
	{
		Value* v = (Value *)lfirst(i);
		if (v == NULL || v->type != T_String) {
			ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("invalid name")));
		}
		if (!IsValidIdentClientKey(v->val.str)) {
			ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("invalid name")));
		}
	}
	return names;
}

/* check_indirection --- check the result of indirection production
 *
 * We only allow '*' at the end of the list, but it's hard to enforce that
 * in the grammar, so do it here.
 */
static List *
check_indirection(List *indirection, core_yyscan_t yyscanner)
{
	ListCell *l;

	foreach(l, indirection)
	{
		if (IsA(lfirst(l), A_Star))
		{
			if (lnext(l) != NULL)
				parser_yyerror("improper use of \"*\"");
		}
	}
	return indirection;
}

/* extractArgTypes()
 * Given a list of FunctionParameter nodes, extract a list of just the
 * argument types (TypeNames) for input parameters only.  This is what
 * is needed to look up an existing function, which is what is wanted by
 * the productions that use this call.
 */
static List *
extractArgTypes(List *parameters)
{
	List	   *result = NIL;
	ListCell   *i;

	foreach(i, parameters)
	{
		FunctionParameter *p = (FunctionParameter *) lfirst(i);

		if (p->mode != FUNC_PARAM_OUT && p->mode != FUNC_PARAM_TABLE)
			result = lappend(result, p->argType);
	}
	return result;
}

/* insertSelectOptions()
 * Insert ORDER BY, etc into an already-constructed SelectStmt.
 *
 * This routine is just to avoid duplicating code in SelectStmt productions.
 */
static void
insertSelectOptions(SelectStmt *stmt,
					List *sortClause, List *lockingClause,
					Node *limitOffset, Node *limitCount,
					WithClause *withClause,
					core_yyscan_t yyscanner)
{
	AssertEreport(IsA(stmt, SelectStmt), MOD_OPT, "Node type inconsistant");

	/*
	 * Tests here are to reject constructs like
	 *	(SELECT foo ORDER BY bar) ORDER BY baz
	 */
	if (sortClause)
	{
		if (stmt->sortClause)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple ORDER BY clauses not allowed"),
					 parser_errposition(exprLocation((Node *) sortClause))));
		stmt->sortClause = sortClause;
	}
	/* We can handle multiple locking clauses, though */
	stmt->lockingClause = list_concat(stmt->lockingClause, lockingClause);
	if (limitOffset)
	{
		if (stmt->limitOffset)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple OFFSET clauses not allowed"),
					 parser_errposition(exprLocation(limitOffset))));
		stmt->limitOffset = limitOffset;
	}
	if (limitCount)
	{
		if (stmt->limitCount)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple LIMIT clauses not allowed"),
					 parser_errposition(exprLocation(limitCount))));
		stmt->limitCount = limitCount;
	}
	if (withClause)
	{
		if (stmt->withClause)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("multiple WITH clauses not allowed"),
					 parser_errposition(exprLocation((Node *) withClause))));
		stmt->withClause = withClause;
	}
}

static Node *
makeSetOp(SetOperation op, bool all, Node *larg, Node *rarg)
{
	SelectStmt *n = makeNode(SelectStmt);

	n->op = op;
	n->all = all;
	n->larg = (SelectStmt *) larg;
	n->rarg = (SelectStmt *) rarg;
	return (Node *) n;
}

/* SystemFuncName()
 * Build a properly-qualified reference to a built-in function.
 */
List *
SystemFuncName(char *name)
{
	return list_make2(makeString("pg_catalog"), makeString(name));
}

/* SystemTypeName()
 * Build a properly-qualified reference to a built-in type.
 *
 * typmod is defaulted, but may be changed afterwards by caller.
 * Likewise for the location.
 */
TypeName *
SystemTypeName(char *name)
{
	return makeTypeNameFromNameList(list_make2(makeString("pg_catalog"),
											   makeString(name)));
}

/* doNegate()
 * Handle negation of a numeric constant.
 *
 * Formerly, we did this here because the optimizer couldn't cope with
 * indexquals that looked like "var = -4" --- it wants "var = const"
 * and a unary minus operator applied to a constant didn't qualify.
 * As of Postgres 7.0, that problem doesn't exist anymore because there
 * is a constant-subexpression simplifier in the optimizer.  However,
 * there's still a good reason for doing this here, which is that we can
 * postpone committing to a particular internal representation for simple
 * negative constants.	It's better to leave "-123.456" in string form
 * until we know what the desired type is.
 */
static Node *
doNegate(Node *n, int location)
{
	if (IsA(n, A_Const))
	{
		A_Const *con = (A_Const *)n;

		/* report the constant's location as that of the '-' sign */
		con->location = location;

		if (con->val.type == T_Integer)
		{
			con->val.val.ival = -con->val.val.ival;
			return n;
		}
		if (con->val.type == T_Float)
		{
			doNegateFloat(&con->val);
			return n;
		}
	}

	return (Node *) makeSimpleA_Expr(AEXPR_OP, "-", NULL, n, location);
}

static void
doNegateFloat(Value *v)
{
	char   *oldval = v->val.str;

	AssertEreport(IsA(v, Float), MOD_OPT, "Node Type inconsistant");
	if (*oldval == '+')
		oldval++;
	if (*oldval == '-')
		v->val.str = oldval+1;	/* just strip the '-' */
	else
	{
		char   *newval = (char *) palloc(strlen(oldval) + 2);

		*newval = '-';
		strcpy(newval+1, oldval);
		v->val.str = newval;
	}
}

static Node *
makeAArrayExpr(List *elements, int location)
{
	A_ArrayExpr *n = makeNode(A_ArrayExpr);

	n->elements = elements;
	n->location = location;
	return (Node *) n;
}

static Node *
makeXmlExpr(XmlExprOp op, char *name, List *named_args, List *args,
			int location)
{
	XmlExpr		*x = makeNode(XmlExpr);

	x->op = op;
	x->name = name;
	/*
	 * named_args is a list of ResTarget; it'll be split apart into separate
	 * expression and name lists in transformXmlExpr().
	 */
	x->named_args = named_args;
	x->arg_names = NIL;
	x->args = args;
	/* xmloption, if relevant, must be filled in by caller */
	/* type and typmod will be filled in during parse analysis */
	x->type = InvalidOid;			/* marks the node as not analyzed */
	x->location = location;
	return (Node *) x;
}

/*
 * Merge the input and output parameters of a table function.
 */
static List *
mergeTableFuncParameters(List *func_args, List *columns)
{
	ListCell   *lc;

	/* Explicit OUT and INOUT parameters shouldn't be used in this syntax */
	foreach(lc, func_args)
	{
		FunctionParameter *p = (FunctionParameter *) lfirst(lc);

		if (p->mode != FUNC_PARAM_IN && p->mode != FUNC_PARAM_VARIADIC)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("OUT and INOUT arguments aren't allowed in TABLE functions")));
	}

	return list_concat(func_args, columns);
}

/*
 * Determine return type of a TABLE function.  A single result column
 * returns setof that column's type; otherwise return setof record.
 */
static TypeName *
TableFuncTypeName(List *columns)
{
	TypeName *result;

	if (list_length(columns) == 1)
	{
		FunctionParameter *p = (FunctionParameter *) linitial(columns);

		result = (TypeName *) copyObject(p->argType);
	}
	else
		result = SystemTypeName("record");

	result->setof = true;

	return result;
}

/*
 * Convert a list of (dotted) names to a RangeVar (like
 * makeRangeVarFromNameList, but with position support).  The
 * "AnyName" refers to the any_name production in the grammar.
 */
static RangeVar *
makeRangeVarFromAnyName(List *names, int position, core_yyscan_t yyscanner)
{
	RangeVar *r = makeNode(RangeVar);

	switch (list_length(names))
	{
		case 1:
			r->catalogname = NULL;
			r->schemaname = NULL;
			r->relname = strVal(linitial(names));
			break;
		case 2:
			r->catalogname = NULL;
			r->schemaname = strVal(linitial(names));
			r->relname = strVal(lsecond(names));
			break;
		case 3:
			r->catalogname = strVal(linitial(names));;
			r->schemaname = strVal(lsecond(names));
			r->relname = strVal(lthird(names));
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("improper qualified name (too many dotted names): %s",
							NameListToString(names)),
					 parser_errposition(position)));
			break;
	}

	r->relpersistence = RELPERSISTENCE_PERMANENT;
	r->location = position;
	r->ispartition = false;
	r->isbucket = false;
	r->buckets = NIL;

	return r;
}

/* Separate Constraint nodes from COLLATE clauses in a ColQualList */
static void
SplitColQualList(List *qualList,
				 List **constraintList, CollateClause **collClause,
				 core_yyscan_t yyscanner)
{
	ListCell   *cell;
	ListCell   *prev;
	ListCell   *next;

	*collClause = NULL;
	prev = NULL;
	for (cell = list_head(qualList); cell; cell = next)
	{
		Node   *n = (Node *) lfirst(cell);

		next = lnext(cell);
		if (IsA(n, Constraint))
		{
			/* keep it in list */
			prev = cell;
			continue;
		}
		if (IsA(n, CollateClause))
		{
			CollateClause *c = (CollateClause *) n;

			if (*collClause)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("multiple COLLATE clauses not allowed"),
						 parser_errposition(c->location)));
			*collClause = c;
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
						errmsg("unexpected node type %d", (int) n->type)));
		/* remove non-Constraint nodes from qualList */
		qualList = list_delete_cell(qualList, cell, prev);
	}
	*constraintList = qualList;
}

/* Separate Constraint nodes from COLLATE clauses in a ColQualList */
static void
SplitColQualList(List *qualList,
				 List **constraintList, CollateClause **collClause,ClientLogicColumnRef **clientLogicColumnRef,
				 core_yyscan_t yyscanner)
{
	ListCell   *cell;
	ListCell   *prev;
	ListCell   *next;

	*collClause = NULL;
	*clientLogicColumnRef =NULL;
	prev = NULL;
	for (cell = list_head(qualList); cell; cell = next)
	{
		Node   *n = (Node *) lfirst(cell);

		next = lnext(cell);
		if (IsA(n, Constraint))
		{
			/* keep it in list */
			prev = cell;
			continue;
		}
		if (IsA(n, CollateClause))
		{
			CollateClause *c = (CollateClause *) n;

			if (*collClause)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("multiple COLLATE clauses not allowed"),
						 parser_errposition(c->location)));
			*collClause = c;
		}
		else if (IsA(n, ClientLogicColumnRef))
		{
			ClientLogicColumnRef *e = (ClientLogicColumnRef *) n;

			if (*clientLogicColumnRef)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("multiple encrypted columns are not allowed"),
						 parser_errposition(e->location)));
			*clientLogicColumnRef = e;
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
						errmsg("unexpected node type %d", (int) n->type)));
		/* remove non-Constraint nodes from qualList */
		qualList = list_delete_cell(qualList, cell, prev);
	}
	*constraintList = qualList;
}

/*
 * Process result of ConstraintAttributeSpec, and set appropriate bool flags
 * in the output command node.  Pass NULL for any flags the particular
 * command doesn't support.
 */
static void
processCASbits(int cas_bits, int location, const char *constrType,
			   bool *deferrable, bool *initdeferred, bool *not_valid,
			   bool *no_inherit, core_yyscan_t yyscanner)
{
	/* defaults */
	if (deferrable)
		*deferrable = false;
	if (initdeferred)
		*initdeferred = false;
	if (not_valid)
		*not_valid = false;

	if (cas_bits & (CAS_DEFERRABLE | CAS_INITIALLY_DEFERRED))
	{
		if (deferrable)
			*deferrable = true;
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 /* translator: %s is CHECK, UNIQUE, or similar */
					 errmsg("%s constraints cannot be marked DEFERRABLE",
							constrType),
					 parser_errposition(location)));
	}

	if (cas_bits & CAS_INITIALLY_DEFERRED)
	{
		if (initdeferred)
			*initdeferred = true;
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 /* translator: %s is CHECK, UNIQUE, or similar */
					 errmsg("%s constraints cannot be marked DEFERRABLE",
							constrType),
					 parser_errposition(location)));
	}

	if (cas_bits & CAS_NOT_VALID)
	{
		if (not_valid)
			*not_valid = true;
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 /* translator: %s is CHECK, UNIQUE, or similar */
					 errmsg("%s constraints cannot be marked NOT VALID",
							constrType),
					 parser_errposition(location)));
	}

	if (cas_bits & CAS_NO_INHERIT)
	{
		if (no_inherit)
			*no_inherit = true;
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 /* translator: %s is CHECK, UNIQUE, or similar */
					 errmsg("%s constraints cannot be marked NO INHERIT",
							constrType),
					 parser_errposition(location)));
	}
}

/* parser_init()
 * Initialize to parse one query string
 */
void
parser_init(base_yy_extra_type *yyext)
{
	yyext->parsetree = NIL;		/* in case grammar forgets to set it */
	yyext->core_yy_extra.query_string_locationlist = NIL;
	yyext->core_yy_extra.paren_depth = 0;
}

static Expr *
makeNodeDecodeCondtion(Expr* firstCond,Expr* secondCond)
{
	A_Expr   *equal_oper	= makeSimpleA_Expr(AEXPR_OP, "=", (Node*)copyObject( firstCond), (Node*)copyObject(secondCond), -1);
	NullTest *isnull_oper1	= makeNode(NullTest);
	NullTest *isnull_oper2	= makeNode(NullTest);
	A_Expr   *and_oper	= makeA_Expr(AEXPR_AND,NIL,(Node*)isnull_oper1, (Node*)isnull_oper2, -1);
	CaseExpr *c		= makeNode(CaseExpr);
	CaseWhen *w		= makeNode(CaseWhen);

	isnull_oper1->arg = (Expr*) copyObject( firstCond);
	isnull_oper1->nulltesttype = IS_NULL;
	isnull_oper2->arg = (Expr*) copyObject(secondCond);
	isnull_oper2->nulltesttype = IS_NULL;

	w->expr  = (Expr*)and_oper;
	w->result = (Expr*)makeBoolAConst(TRUE, -1);

	c->casetype = InvalidOid;
	c->arg = NULL;
	c->args = NULL;
	c->args = lappend(c->args,w);
	c->defresult = (Expr*)equal_oper;

	return (Expr*)c;
}

// make function infomation to kill the session
// make "ResTarget" for invoking function "pg_terminate_backend",
// only the first cell of arguments is effective, it is treated as pid
static List*
make_action_func(List *arguments)
{
	FuncCall		*func = NULL;
	ResTarget	*restarget = NULL;

	func = (FuncCall*)makeNode(FuncCall);
	func->funcname = list_make1(makeString("pg_terminate_backend"));
	func->args = arguments;
	func->agg_star = FALSE;
	func->agg_distinct = FALSE;
	func->location = -1;
	func->call_func = false;

	restarget = makeNode(ResTarget);
	restarget->name = NULL;
	restarget->indirection = NIL;
	restarget->val = (Node *)func;
	restarget->location = -1;

	return (list_make1(restarget));
}

/*
 * @Description:  get arguments of function "pg_terminate_backend" from a string.
 * @in sid : the pid and serial info which need be terminated.
 * @return : the list include pid and serial information.
 */
static List *
get_func_args(char *sid)
{
	char *token = NULL;
	List *sidlist = NIL;
	long long pid = 0;
	long long serial = 0;
	const char *sep = ",";

	/*
	 * split the string with denotation 'sep'.
	 */
	while ((token = pg_strsep(&sid, sep)))
		sidlist = lappend(sidlist, token);

	/*
	 * it is incorrect unless the number of parameter equals 2
	 */
	if (2 != sidlist->length)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("missing or invalid session ID")));
		return NIL;
	}

	pid = get_pid((const char *)linitial(sidlist));
	serial= get_pid((const char *)lsecond(sidlist));

	/*
	 * negative is illegal for session id
	 */
	if (pid < 0 || serial < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("missing or invalid session ID")));
		return NIL;
	}

	return list_make1(makeStringConst((char *)linitial(sidlist), -1));
}

// transform a string to a Oid
static long long
get_pid(const char *strsid)
{
	char cur_char = 0;
	int counter = 0;
	bool start =false;
	bool end = false;

	/*
	 * it is illegal format if the string is null
	 */
	if (!strsid)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("missing or invalid session ID")));
		return 0;
	}

	cur_char = strsid[counter];
	while (cur_char)
	{
		/*
		 * it is illegal if the string has a character that it is not a alphanumeric
		 */
		if (!isdigit(cur_char) && !isspace(cur_char))
		{
			ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("missing or invalid session ID")));
			return 0;
		}

		if (!start && isdigit(cur_char))
			start = true;

		if (!end && isspace(cur_char) && start)
			end = true;
		/*
		 * it is illegal if the string is aplited by a space
		 */
		if (start && end && !isspace(cur_char))
		{
			ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("missing or invalid session ID")));
			return 0;
		}
		cur_char = strsid[++counter];
	}

	if (!start)
	{
		ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_OBJECT),
			errmsg("missing or invalid session ID")));
		return 0;
	}

	return atoll(strsid);
}

// Get next token from string *stringp, where tokens are
// possibly-empty strings separated by characters from delim.
// Writes NULs into the string at *stringp to end tokens,
// delim need not remain constant from call to call.
// On return, *stringp points past the last NUL written (if there
// might be further tokens), or is NULL (if there are definitely no
// moretokens).
// If *stringp is NULL, strsep returns NULL
static char *
pg_strsep(char **stringp, const char *delim)
{
	char	 *s = NULL;
	const char *spanp = NULL;
	int c = 0;
	int sc = 0;
	char	 *tok = NULL;

	if (NULL == (s = *stringp))
		return NULL;

	for (tok = s;;)
	{
		c = *s++;
		spanp = delim;
		do
		{
			if ((sc =*spanp++) == c)
			{
				if (0 == c)
					s = NULL;
				else
					s[-1] = 0;
				*stringp = s;
				return (tok);
			}
		} while (0 != sc);
	}
}

static int
get_outarg_num (List *fun_args)
{
	int count = 0;
	FunctionParameter *arg = NULL;
	ListCell* cell = NULL;

	if (NIL == fun_args)
		return 0;

	foreach (cell, fun_args)
	{
		arg = (FunctionParameter*) lfirst(cell);
		if ( FUNC_PARAM_OUT == arg->mode || FUNC_PARAM_INOUT == arg->mode )
			count++;
	}
	return count;
}

// To make a node for anonymous block
static Node *
MakeAnonyBlockFuncStmt(int flag, const char *str)
{
	DoStmt *n = makeNode(DoStmt);
	char *str_body	= NULL;
	DefElem * body	= NULL;
	errno_t		rc = EOK;

	if (BEGIN_P == flag)
	{
		int len1 = strlen("DECLARE \nBEGIN ");
		int len2 = strlen(str);
		str_body = (char *)palloc(len1 + len2 + 1);
		rc = strncpy_s(str_body, len1 + len2 + 1, "DECLARE \nBEGIN ",len1);
		securec_check(rc, "\0", "\0");
		rc = strcpy_s(str_body + len1, len2 + 1, str);
		securec_check(rc, "\0", "\0");
	}
	else
	{
		int len1 = strlen("DECLARE ");
		int len2 = strlen(str);
		str_body = (char *)palloc(len1 + len2 + 1);
		rc = strncpy_s(str_body, len1 + len2 + 1, "DECLARE ", len1);
		securec_check(rc, "\0", "\0");
		rc = strcpy_s(str_body + len1, len2 + 1, str);
		securec_check(rc, "\0", "\0");
	}

	body = makeDefElem("as", (Node*)makeString(str_body));
	n->args = list_make1(makeDefElem("language", (Node *)makeString("plpgsql")));
	n->args = lappend( n->args, body);

	return (Node*)n;
}

// get arg info with arg position or arg name
static void
get_arg_mode_by_name(const char *argname, const char * const *argnames,
			const char *argmodes,const int proargnum, bool *have_assigend, char *argmode)
{
	int	curpos = 0;
	const char *paraname= NULL;

	if (argnames == NULL) {
		ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			errmsg("No function matches the given arguments names. "
			"You might need to add explicit declare arguments names.")));
	}

	if (unlikely(argname == NULL)) {
		ereport(ERROR, 
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE), 
			errmsg("argname should not be null")));
	}

	for (curpos = 0; curpos < proargnum; curpos++)
	{
		paraname = argnames[curpos];

		if (paraname && !strcmp(paraname, argname))
		{
			if (!argmodes)
				*argmode = FUNC_PARAM_IN;
			else
				*argmode = argmodes[curpos];

			break;
		}
	}

	if (curpos < proargnum && have_assigend[curpos])
	{
		ereport(ERROR,
			(errcode(ERRCODE_DUPLICATE_OBJECT),
			errmsg("parameter \"%s\" is assigned more than once", argname)));
		return;
	}

	if (curpos == proargnum)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_PARAMETER),
				errmsg("parameter \"%s\" is undefined", argname)));
		return;
	}

	have_assigend[curpos] = true;
}

// get arg info with arg position or arg position
static void
get_arg_mode_by_pos(const int pos, const char *argmodes,
			const int narg, bool *have_assigend, char *argmode)
{
	AssertEreport(pos >= 0, MOD_OPT, "para should not be negative");

	if (have_assigend[pos])
	{
		ereport(ERROR,
			(errcode(ERRCODE_DUPLICATE_OBJECT),
			errmsg("the parameter located \"%d\" have been assigned", pos + 1)));
		return;
	}

	if (argmodes)
		*argmode = argmodes[pos];
	else
		*argmode = FUNC_PARAM_IN;

	have_assigend[pos] = true;
}

// return count of table function output column
static int get_table_modes(int narg, const char *p_argmodes)
{
	int count = 0;
	if (p_argmodes == NULL)
		return 0;
	for (; narg > 0; narg--, p_argmodes++)
		if (*p_argmodes == FUNC_PARAM_TABLE)
			count++;
	return count;
}

// check and append a cell to a list
static List *
append_inarg_list(const char argmode,const ListCell *cell,List *in_parameters)
{
	switch(argmode)
	{
		case FUNC_PARAM_IN:
			in_parameters = lappend(in_parameters,lfirst(cell));
			break;
		case FUNC_PARAM_INOUT:
			in_parameters = lappend(in_parameters,lfirst(cell));
			break;
		case FUNC_PARAM_OUT:
			break;
		// get the all "in" parameters, except "out" or "table_colums" parameters
		case FUNC_PARAM_TABLE:
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
					errmsg("parameter mode %c doesn't exist",argmode)));
			break;
	}

	return in_parameters;
}

// check wheather all the out parameters have been assigned
static void
check_outarg_info(const bool *have_assigend, const char *argmodes,const int proargnum)
{
	int counter = 0;

	if (!argmodes)
		return;

	for (counter = 0; counter < proargnum; counter++)
	{
		if (!have_assigend[counter] && (FUNC_PARAM_OUT == argmodes[counter]))
		{
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("output argument located \"%d\" doesnot assigned",
				counter + 1)));
			return;
		}
	}
}

// Added CALL for procedure and function
static Node *
makeCallFuncStmt(List* funcname,List* parameters)
{
	SelectStmt *newm = NULL;
	ColumnRef *column = NULL;
	ResTarget *resTarget = NULL;
	FuncCall *funcCall = NULL;
	RangeFunction *rangeFunction = NULL;
	char *schemaname = NULL;
	char *name = NULL;
	FuncCandidateList clist = NULL;
	HeapTuple proctup = NULL;
	Form_pg_proc procStruct;
	Oid *p_argtypes = NULL;
	char **p_argnames = NULL;
	char *p_argmodes = NULL;
	List *in_parameters = NULL;
	int i = 0;
	ListCell *cell = NULL;
	int narg = 0;
	int ndefaultargs = 0;
	int ntable_colums = 0;
	bool *have_assigend = NULL;
	bool	has_overload_func = false;

	/* deconstruct the name list */
	DeconstructQualifiedName(funcname, &schemaname, &name);

	/* search the function */
	clist = FuncnameGetCandidates(funcname, -1, NIL, false, false, false);
	if (!clist)
	{
		ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_FUNCTION),
				errmsg("function \"%s\" doesn't exist ", name)));
		return NULL;
	}

	if (clist->next)
	{
		has_overload_func = true;
		if (!IsPackageFunction(funcname))
		{
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_FUNCTION),
					errmsg("function \"%s\" isn't exclusive ", name)));
			return NULL;
		}
	}

	if (!has_overload_func)
	{
		proctup = SearchSysCache(PROCOID,
								 ObjectIdGetDatum(clist->oid),
								 0, 0, 0);

		/*
		 * function may be deleted after clist be searched.
		 */
		if (!HeapTupleIsValid(proctup))
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("function \"%s\" doesn't exist ", name)));
			return NULL;
		}

		/* get the all args informations, only "in" parameters if p_argmodes is null */
		narg = get_func_arg_info(proctup,&p_argtypes,&p_argnames,&p_argmodes);

		/* get the all "in" parameters, except "out" or "table_colums" parameters */
		ntable_colums = get_table_modes(narg, p_argmodes);
		narg -= ntable_colums;

		procStruct = (Form_pg_proc) GETSTRUCT(proctup);
		ndefaultargs = procStruct->pronargdefaults;
		ReleaseSysCache(proctup);

		/* check the parameters' count*/
		if (narg - ndefaultargs > (parameters ? parameters->length : 0) )
		{
			ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				errmsg("function \"%s\" with %d parameters doesn't exist ",
					name,parameters? parameters->length : 0)));
		}

		if (parameters && (narg < parameters->length))
		{
			ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				errmsg("function \"%s\" with %d parameters doesn't exist ",
					name, parameters->length)));
		}

		/* analyse all parameters */
		i = 0;
		have_assigend = (bool *)palloc(sizeof(bool) * narg);
		memset(have_assigend, 0, sizeof(bool) * narg);

		foreach(cell,parameters)
		{
			Node	*arg = (Node *)lfirst(cell);
			char *argname = NULL;
			char	argmode = 0;

			if (IsA(arg, NamedArgExpr))
			{
				NamedArgExpr *na = (NamedArgExpr *) arg;

				argname = na->name;
				get_arg_mode_by_name(argname, p_argnames,
							p_argmodes, narg, have_assigend,&argmode);
				in_parameters = append_inarg_list(argmode, cell, in_parameters);
			}
			else
			{
				get_arg_mode_by_pos(i, p_argmodes, narg, have_assigend, &argmode);
				in_parameters = append_inarg_list(argmode, cell, in_parameters);
			}

			i++;
		}

		check_outarg_info(have_assigend, p_argmodes, narg);
	}
	else
	{
		in_parameters = parameters;
	}
	

	column = makeNode(ColumnRef);
	column->fields = list_make1(makeNode(A_Star));
	column->location = -1;

	resTarget = makeNode(ResTarget);
	resTarget->name = NULL;
	resTarget->indirection = NIL;
	resTarget->val = (Node *)column;
	resTarget->location = -1;

	funcCall = (FuncCall*)makeNode(FuncCall);
	funcCall->funcname = funcname;
	funcCall->args = in_parameters;
	funcCall->agg_star = FALSE;
	funcCall->func_variadic = false;
	funcCall->agg_distinct = FALSE;
	funcCall->agg_order = NIL;
	funcCall->over = NULL;
	funcCall->location = -1;
	if (has_overload_func)
		funcCall->call_func = true;
	else
		funcCall->call_func = false;

	rangeFunction = makeNode(RangeFunction);
	rangeFunction->funccallnode = (Node*)funcCall;
	rangeFunction->coldeflist = NIL;

	newm =  (SelectStmt*)makeNode(SelectStmt);
	newm->distinctClause = NIL;
	newm->intoClause  = NULL;
	newm->targetList  = list_make1(resTarget);
	newm->fromClause  = list_make1(rangeFunction);
	newm->whereClause = NULL;
	newm->havingClause= NULL;
        newm->groupClause = NIL;
	return (Node*)newm;
}

/* judge if ident is valid
 * Only letters, numbers, dollar signs ($) and the underscore are allowed in name
 * and The first character must be letter or underscore
*/
bool
IsValidIdent(char *input)
{
	char c = input[0];
	/*The first character id numbers or dollar*/
	if ((c >= '0' && c <= '9') || c == '$')
	{
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			errmsg("invalid name: %s", input)));
		return false;
	}

	int len = strlen(input);
	for (int i = 0; i < len; i++)
	{
		c = input[i];
		if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '$')
		{
			continue;
		}
		else
		{
			ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("invalid name: %s", input)));
			return false;
		}
	}
	return true;
}

/* judge if node group name is valid
 * Only ASCII character set is allowed in group name
*/
bool
IsValidGroupname(const char *input)
{
	int len = strlen(input);
	for (int i = 0; i < len; i++)
	{
		if (IS_HIGHBIT_SET(input[i]))
		{
			ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("node group name is not allowed to contain multibyte characters")));
		}	
	}
	return true;
}

static bool
checkNlssortArgs(const char *argname)
{
	List *sortlist = NULL;
	char *nlskey = NULL;
	char *nlsvalue = NULL;

	char *raw = pstrdup(argname);

	/*
	 * Split string like: NLS_SORT=SCHINESE_PINYIN_M,Converting strings to lowercase
	 * and removing back and forth spaces.
	 */
	if (!SplitIdentifierString(raw, '=', &sortlist) || list_length(sortlist) != 2)
	{
		return false;
	}

	nlskey = (char *) linitial(sortlist);

	if (strcmp(nlskey, "nls_sort"))
	{
		return false;
	}

	nlsvalue = (char *) lsecond(sortlist);

	if (strcmp(nlsvalue, "schinese_pinyin_m") && strcmp(nlsvalue, "generic_m_ci"))
	{
		return false;
	}

	FREE_POINTER(raw);

	return true;
}

static void ParseUpdateMultiSet(List *set_target_list, SelectStmt *stmt, core_yyscan_t yyscanner)
{
	/*
	 * Here we transfrom the original sql to handle multicolumn update, we transform the
	 * original sql like this:
	 * the original sql:      UPDATE t1 SET (c1,c2) = (SELECT AVG(d1), 154 a2 FROM t2 GROUP BY a2);
	 * after transformed sql: UPDATE t1 SET (c1,c2) =
								(SELECT s1,s2 FROM (SELECT AVG(d1), 154 a2 FROM t2 GROUP BY a2) as S(s1,s2));
	 */
	List *col_names = NIL;
	ListCell *col_cell = NULL;
	int loop = 1;
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfoChar(&buf, 's');

	/* we need construct complete alias at first */
	foreach(col_cell, set_target_list) {
		pg_ltoa(loop++, &buf.data[1]);
		col_names = lappend(col_names, makeString(pstrdup(buf.data)));
	}

	RangeSubselect *rsubselect = makeNode(RangeSubselect);
	rsubselect->subquery = (Node*)stmt;
	rsubselect->alias = makeAlias(pstrdup("S"), col_names);

	/* now we can separate the multi columns. */
	ListCell* colName = list_head(col_names);

	foreach(col_cell, set_target_list) {
		ResTarget *res_col = (ResTarget *) lfirst(col_cell);

		/* create a new selectstmt as : */
		SelectStmt *stmt_new = makeNode(SelectStmt);
		ResTarget *res_new = makeNode(ResTarget);
		SubLink *res_val = makeNode(SubLink);

		/* we need assign the column for each set_target according to the sequence of column name in alias */
		res_new->val = makeColumnRef(pstrdup(strVal(lfirst(colName))), NIL, res_col->location, yyscanner);
		res_new->location = res_col->location;

		stmt_new->targetList = list_make1(res_new);
		stmt_new->fromClause = list_make1(
			(res_col == linitial(set_target_list)) ? rsubselect : copyObject(rsubselect));

		res_val->subLinkType = EXPR_SUBLINK;
		res_val->testexpr = NULL;
		res_val->operName = NIL;
		res_val->subselect = (Node*)stmt_new;
		res_col->val = (Node*)res_val;

		colName = lnext(colName);
	}
	pfree_ext(buf.data);
}

static void parameter_check_execute_direct(const char* query)
{
#ifndef ENABLE_MULTIPLE_NODES					
    if (IS_SINGLE_NODE) {
		ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("Un-support feature"),
				errdetail("The distributed capability is not supported currently.")));
	}
#endif
	/*
	 * when enable_nonsysadmin_execute_direct is off, only system admin can use EXECUTE DIRECT;
	 * when enable_nonsysadmin_execute_direct is on, any user can use EXECUTE DIRECT;
	*/
	if (!g_instance.attr.attr_security.enable_nonsysadmin_execute_direct &&
		!CheckExecDirectPrivilege(query))
		ereport(ERROR,
			(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				errmsg("must be system admin or monitor admin to use EXECUTE DIRECT")));
}

/*
 * Must undefine this stuff before including scan.c, since it has different
 * definitions for these macros.
 */
#undef yyerror
#undef yylval
#undef yylloc

#include "scan.inc"
