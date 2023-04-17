/* header */
/* src/interfaces/ecpg/preproc/ecpg.header */

/* Copyright comment */
%{
#include "postgres_fe.h"

#include "extern.h"
#include "ecpg_config.h"
#include <unistd.h>

/* Location tracking support --- simpler than bison's default */
#define YYLLOC_DEFAULT(Current, Rhs, N) \
	do { \
		if (N)						\
			(Current) = (Rhs)[1];	\
		else						\
			(Current) = (Rhs)[0];	\
	} while (0)

/*
 * The %name-prefix option below will make bison call base_yylex, but we
 * really want it to call filtered_base_yylex (see parser.c).
 */
#define base_yylex filtered_base_yylex

/*
 * This is only here so the string gets into the POT.  Bison uses it
 * internally.
 */
#define bison_gettext_dummy gettext_noop("syntax error")

/*
 * Variables containing simple states.
 */
int struct_level = 0;
int braces_open; /* brace level counter */
char *current_function;
int ecpg_internal_var = 0;
char	*connection = NULL;
char	*input_filename = NULL;

static int	FoundInto = 0;
static int	initializer = 0;
static int	pacounter = 1;
static char	pacounter_buffer[sizeof(int) * CHAR_BIT * 10 / 3]; /* a rough guess at the size we need */
static struct this_type actual_type[STRUCT_DEPTH];
static char *actual_startline[STRUCT_DEPTH];
static int	varchar_counter = 1;

/* temporarily store struct members while creating the data structure */
struct ECPGstruct_member *struct_member_list[STRUCT_DEPTH] = { NULL };

/* also store struct type so we can do a sizeof() later */
static char *ECPGstruct_sizeof = NULL;

/* for forward declarations we have to store some data as well */
static char *forward_name = NULL;

struct ECPGtype ecpg_no_indicator = {ECPGt_NO_INDICATOR, NULL, NULL, NULL, {NULL}, 0};
struct variable no_indicator = {"no_indicator", &ecpg_no_indicator, 0, NULL};

struct ECPGtype ecpg_query = {ECPGt_char_variable, NULL, NULL, NULL, {NULL}, 0};

/*
 * Handle parsing errors and warnings
 */
void
mmerror(int error_code, enum errortype type, const char *error, ...)
{
	va_list ap;

	/* internationalize the error message string */
	error = _(error);

	fprintf(stderr, "%s:%d: ", input_filename, yylineno);

	switch(type)
	{
		case ET_WARNING:
			fprintf(stderr, _("WARNING: "));
			break;
		case ET_ERROR:
		case ET_FATAL:
			fprintf(stderr, _("ERROR: "));
			break;
	}

	va_start(ap, error);
	vfprintf(stderr, error, ap);
	va_end(ap);

	fprintf(stderr, "\n");

	switch(type)
	{
		case ET_WARNING:
			break;
		case ET_ERROR:
			ret_value = error_code;
			break;
		case ET_FATAL:
			if (yyin)
				fclose(yyin);
			if (yyout)
				fclose(yyout);

			if (strcmp(output_filename, "-") != 0 && unlink(output_filename) != 0)
				fprintf(stderr, _("could not remove output file \"%s\"\n"), output_filename);
			exit(error_code);
	}
}

/*
 * string concatenation
 */

static char *
cat2_str(char *str1, char *str2)
{
	char * res_str	= (char *)mm_alloc(strlen(str1) + strlen(str2) + 2);

	strcpy(res_str, str1);
	if (strlen(str1) != 0 && strlen(str2) != 0)
		strcat(res_str, " ");
	strcat(res_str, str2);
	free_current_memory(str1);
	free_current_memory(str2);
	return(res_str);
}

static char *
cat_str(int count, ...)
{
	va_list		args;
	int			i;
	char		*res_str;

	va_start(args, count);

	res_str = va_arg(args, char *);

	/* now add all other strings */
	for (i = 1; i < count; i++)
		res_str = cat2_str(res_str, va_arg(args, char *));

	va_end(args);

	return(res_str);
}

static char *
make2_str(char *str1, char *str2)
{
	char * res_str	= (char *)mm_alloc(strlen(str1) + strlen(str2) + 1);

	strcpy(res_str, str1);
	strcat(res_str, str2);
	free_current_memory(str1);
	free_current_memory(str2);
	return(res_str);
}

static char *
make3_str(char *str1, char *str2, char *str3)
{
	char * res_str	= (char *)mm_alloc(strlen(str1) + strlen(str2) +strlen(str3) + 1);

	strcpy(res_str, str1);
	strcat(res_str, str2);
	strcat(res_str, str3);
	free_current_memory(str1);
	free_current_memory(str2);
	free_current_memory(str3);
	return(res_str);
}

/* and the rest */
static char *
make_name(void)
{
	return mm_strdup(yytext);
}

static char *
create_questionmarks(char *name, bool array)
{
	struct variable *p = find_variable(name);
	int count;
	char *result = EMPTY;

	/* In case we have a struct, we have to print as many "?" as there are attributes in the struct
	 * An array is only allowed together with an element argument
	 * This is essantially only used for inserts, but using a struct as input parameter is an error anywhere else
	 * so we don't have to worry here. */

	if (p->type->type == ECPGt_struct || (array && p->type->type == ECPGt_array && p->type->u.element->type == ECPGt_struct))
	{
		struct ECPGstruct_member *m;

		if (p->type->type == ECPGt_struct)
			m = p->type->u.members;
		else
			m = p->type->u.element->u.members;

		for (count = 0; m != NULL; m=m->next, count++);
	}
	else
		count = 1;

	for (; count > 0; count --)
	{
		sprintf(pacounter_buffer, "$%d", pacounter++);
		result = cat_str(3, result, mm_strdup(pacounter_buffer), mm_strdup(" , "));
	}

	/* removed the trailing " ," */

	result[strlen(result)-3] = '\0';
	return(result);
}

static char *
adjust_outofscope_cursor_vars(struct cursor *cur)
{
	/* Informix accepts DECLARE with variables that are out of scope when OPEN is called.
	 * For instance you can DECLARE a cursor in one function, and OPEN/FETCH/CLOSE
	 * it in another functions. This is very useful for e.g. event-driver programming,
	 * but may also lead to dangerous programming. The limitation when this is allowed
	 * and doesn't cause problems have to be documented, like the allocated variables
	 * must not be realloc()'ed.
	 *
	 * We have to change the variables to our own struct and just store the pointer
	 * instead of the variable. Do it only for local variables, not for globals.
	 */

	char *result = EMPTY;
	int insert;

	for (insert = 1; insert >= 0; insert--)
	{
		struct arguments *list;
		struct arguments *ptr;
		struct arguments *newlist = NULL;
		struct variable *newvar, *newind;

		list = (insert ? cur->argsinsert : cur->argsresult);

		for (ptr = list; ptr != NULL; ptr = ptr->next)
		{
			char var_text[20];
			char *original_var;
			bool skip_set_var = false;
			bool var_ptr = false;

			/* change variable name to "ECPGget_var(<counter>)" */
			original_var = ptr->variable->name;
			sprintf(var_text, "%d))", ecpg_internal_var);

			/* Don't emit ECPGset_var() calls for global variables */
			if (ptr->variable->brace_level == 0)
			{
				newvar = ptr->variable;
				skip_set_var = true;
			}
			else if ((ptr->variable->type->type == ECPGt_char_variable)
					 && (strncmp(ptr->variable->name, "ECPGprepared_statement", strlen("ECPGprepared_statement")) == 0))
			{
				newvar = ptr->variable;
				skip_set_var = true;
			}
			else if ((ptr->variable->type->type != ECPGt_varchar
					  && ptr->variable->type->type != ECPGt_char
					  && ptr->variable->type->type != ECPGt_unsigned_char
					  && ptr->variable->type->type != ECPGt_string)
					 && atoi(ptr->variable->type->size) > 1)
			{
				newvar = new_variable(cat_str(4, mm_strdup("("),
											  mm_strdup(ecpg_type_name(ptr->variable->type->u.element->type)),
											  mm_strdup(" *)(ECPGget_var("),
											  mm_strdup(var_text)),
									  ECPGmake_array_type(ECPGmake_simple_type(ptr->variable->type->u.element->type,
																			   mm_strdup("1"),
																			   ptr->variable->type->u.element->counter),
														  ptr->variable->type->size),
									  0);
			}
			else if ((ptr->variable->type->type == ECPGt_varchar
					  || ptr->variable->type->type == ECPGt_char
					  || ptr->variable->type->type == ECPGt_unsigned_char
					  || ptr->variable->type->type == ECPGt_string)
					 && atoi(ptr->variable->type->size) > 1)
			{
				newvar = new_variable(cat_str(4, mm_strdup("("),
											  mm_strdup(ecpg_type_name(ptr->variable->type->type)),
											  mm_strdup(" *)(ECPGget_var("),
											  mm_strdup(var_text)),
									  ECPGmake_simple_type(ptr->variable->type->type,
														   ptr->variable->type->size,
														   ptr->variable->type->counter),
									  0);
				if (ptr->variable->type->type == ECPGt_varchar)
					var_ptr = true;
			}
			else if (ptr->variable->type->type == ECPGt_struct
					 || ptr->variable->type->type == ECPGt_union)
			{
				newvar = new_variable(cat_str(5, mm_strdup("(*("),
											  mm_strdup(ptr->variable->type->type_name),
											  mm_strdup(" *)(ECPGget_var("),
											  mm_strdup(var_text),
											  mm_strdup(")")),
									  ECPGmake_struct_type(ptr->variable->type->u.members,
														   ptr->variable->type->type,
														   ptr->variable->type->type_name,
														   ptr->variable->type->struct_sizeof),
									  0);
				var_ptr = true;
			}
			else if (ptr->variable->type->type == ECPGt_array)
			{
				if (ptr->variable->type->u.element->type == ECPGt_struct
					|| ptr->variable->type->u.element->type == ECPGt_union)
				{
					newvar = new_variable(cat_str(5, mm_strdup("(*("),
											  mm_strdup(ptr->variable->type->u.element->type_name),
											  mm_strdup(" *)(ECPGget_var("),
											  mm_strdup(var_text),
											  mm_strdup(")")),
										  ECPGmake_struct_type(ptr->variable->type->u.element->u.members,
															   ptr->variable->type->u.element->type,
															   ptr->variable->type->u.element->type_name,
															   ptr->variable->type->u.element->struct_sizeof),
										  0);
				}
				else
				{
					newvar = new_variable(cat_str(4, mm_strdup("("),
												  mm_strdup(ecpg_type_name(ptr->variable->type->type)),
												  mm_strdup(" *)(ECPGget_var("),
												  mm_strdup(var_text)),
										  ECPGmake_array_type(ECPGmake_simple_type(ptr->variable->type->u.element->type,
																				   ptr->variable->type->u.element->size,
																				   ptr->variable->type->u.element->counter),
															  ptr->variable->type->size),
										  0);
					var_ptr = true;
				}
			}
			else
			{
				newvar = new_variable(cat_str(4, mm_strdup("*("),
											  mm_strdup(ecpg_type_name(ptr->variable->type->type)),
											  mm_strdup(" *)(ECPGget_var("),
											  mm_strdup(var_text)),
									  ECPGmake_simple_type(ptr->variable->type->type,
														   ptr->variable->type->size,
														   ptr->variable->type->counter),
									  0);
				var_ptr = true;
			}

			/* create call to "ECPGset_var(<counter>, <connection>, <pointer>. <line number>)" */
			if (!skip_set_var)
			{
				sprintf(var_text, "%d, %s", ecpg_internal_var++, var_ptr ? "&(" : "(");
				result = cat_str(5, result, mm_strdup("ECPGset_var("),
								 mm_strdup(var_text), mm_strdup(original_var),
								 mm_strdup("), __LINE__);\n"));
			}

			/* now the indicator if there is one and it's not a global variable */
			if ((ptr->indicator->type->type == ECPGt_NO_INDICATOR) || (ptr->indicator->brace_level == 0))
			{
				newind = ptr->indicator;
			}
			else
			{
				/* change variable name to "ECPGget_var(<counter>)" */
				original_var = ptr->indicator->name;
				sprintf(var_text, "%d))", ecpg_internal_var);
				var_ptr = false;

				if (ptr->indicator->type->type == ECPGt_struct
					|| ptr->indicator->type->type == ECPGt_union)
				{
					newind = new_variable(cat_str(5, mm_strdup("(*("),
											  mm_strdup(ptr->indicator->type->type_name),
											  mm_strdup(" *)(ECPGget_var("),
											  mm_strdup(var_text),
											  mm_strdup(")")),
										  ECPGmake_struct_type(ptr->indicator->type->u.members,
															   ptr->indicator->type->type,
															   ptr->indicator->type->type_name,
															   ptr->indicator->type->struct_sizeof),
										  0);
					var_ptr = true;
				}
				else if (ptr->indicator->type->type == ECPGt_array)
				{
					if (ptr->indicator->type->u.element->type == ECPGt_struct
						|| ptr->indicator->type->u.element->type == ECPGt_union)
					{
						newind = new_variable(cat_str(5, mm_strdup("(*("),
											  mm_strdup(ptr->indicator->type->u.element->type_name),
											  mm_strdup(" *)(ECPGget_var("),
											  mm_strdup(var_text),
											  mm_strdup(")")),
											  ECPGmake_struct_type(ptr->indicator->type->u.element->u.members,
																   ptr->indicator->type->u.element->type,
																   ptr->indicator->type->u.element->type_name,
																   ptr->indicator->type->u.element->struct_sizeof),
											  0);
					}
					else
					{
						newind = new_variable(cat_str(4, mm_strdup("("),
													  mm_strdup(ecpg_type_name(ptr->indicator->type->u.element->type)),
													  mm_strdup(" *)(ECPGget_var("), mm_strdup(var_text)),
											  ECPGmake_array_type(ECPGmake_simple_type(ptr->indicator->type->u.element->type,
																					   ptr->indicator->type->u.element->size,
																					   ptr->indicator->type->u.element->counter),
																  ptr->indicator->type->size),
											  0);
						var_ptr = true;
					}
				}
				else if (atoi(ptr->indicator->type->size) > 1)
				{
					newind = new_variable(cat_str(4, mm_strdup("("),
												  mm_strdup(ecpg_type_name(ptr->indicator->type->type)),
												  mm_strdup(" *)(ECPGget_var("),
												  mm_strdup(var_text)),
										  ECPGmake_simple_type(ptr->indicator->type->type,
															   ptr->indicator->type->size,
															   ptr->variable->type->counter),
										  0);
				}
				else
				{
					newind = new_variable(cat_str(4, mm_strdup("*("),
												  mm_strdup(ecpg_type_name(ptr->indicator->type->type)),
												  mm_strdup(" *)(ECPGget_var("),
												  mm_strdup(var_text)),
										  ECPGmake_simple_type(ptr->indicator->type->type,
															   ptr->indicator->type->size,
															   ptr->variable->type->counter),
										  0);
					var_ptr = true;
				}

				/* create call to "ECPGset_var(<counter>, <pointer>. <line number>)" */
				sprintf(var_text, "%d, %s", ecpg_internal_var++, var_ptr ? "&(" : "(");
				result = cat_str(5, result, mm_strdup("ECPGset_var("),
								 mm_strdup(var_text), mm_strdup(original_var),
								 mm_strdup("), __LINE__);\n"));
			}

			add_variable_to_tail(&newlist, newvar, newind);
		}

		if (insert)
			cur->argsinsert_oos = newlist;
		else
			cur->argsresult_oos = newlist;
	}

	return result;
}

/* This tests whether the cursor was declared and opened in the same function. */
#define SAMEFUNC(cur)	\
	((cur->function == NULL) ||		\
	 (cur->function != NULL && strcmp(cur->function, current_function) == 0))

static struct cursor *
add_additional_variables(char *name, bool insert)
{
	struct cursor *ptr;
	struct arguments *p;
	int (* strcmp_fn)(const char *, const char *) = (name[0] == ':' ? strcmp : pg_strcasecmp);

	for (ptr = cur; ptr != NULL; ptr=ptr->next)
	{
		if (strcmp_fn(ptr->name, name) == 0)
			break;
	}

	if (ptr == NULL)
	{
		mmerror(PARSE_ERROR, ET_ERROR, "cursor \"%s\" does not exist", name);
		return NULL;
	}

	if (insert)
	{
		/* add all those input variables that were given earlier
		 * note that we have to append here but have to keep the existing order */
		for (p = (SAMEFUNC(ptr) ? ptr->argsinsert : ptr->argsinsert_oos); p; p = p->next)
			add_variable_to_tail(&argsinsert, p->variable, p->indicator);
	}

	/* add all those output variables that were given earlier */
	for (p = (SAMEFUNC(ptr) ? ptr->argsresult : ptr->argsresult_oos); p; p = p->next)
		add_variable_to_tail(&argsresult, p->variable, p->indicator);

	return ptr;
}

static void
add_typedef(char *name, char *dimension, char *length, enum ECPGttype type_enum,
			char *type_dimension, char *type_index, int initializer, int array)
{
	/* add entry to list */
	struct typedefs *ptr, *thisPtr;

	if ((type_enum == ECPGt_struct ||
		 type_enum == ECPGt_union) &&
		initializer == 1)
		mmerror(PARSE_ERROR, ET_ERROR, "initializer not allowed in type definition");
	else if (INFORMIX_MODE && strcmp(name, "string") == 0)
		mmerror(PARSE_ERROR, ET_ERROR, "type name \"string\" is reserved in Informix mode");
	else
	{
		for (ptr = types; ptr != NULL; ptr = ptr->next)
		{
			if (strcmp(name, ptr->name) == 0)
				/* re-definition is a bug */
				mmerror(PARSE_ERROR, ET_ERROR, "type \"%s\" is already defined", name);
		}
		adjust_array(type_enum, &dimension, &length, type_dimension, type_index, array, true);

		thisPtr = (struct typedefs *) mm_alloc(sizeof(struct typedefs));

		/* initial definition */
		thisPtr->next = types;
		thisPtr->name = name;
		thisPtr->brace_level = braces_open;
		thisPtr->type = (struct this_type *) mm_alloc(sizeof(struct this_type));
		thisPtr->type->type_enum = type_enum;
		thisPtr->type->type_str = mm_strdup(name);
		thisPtr->type->type_dimension = dimension; /* dimension of array */
		thisPtr->type->type_index = length;	/* length of string */
		thisPtr->type->type_sizeof = ECPGstruct_sizeof;
		thisPtr->struct_member_list = (type_enum == ECPGt_struct || type_enum == ECPGt_union) ?
		ECPGstruct_member_dup(struct_member_list[struct_level]) : NULL;

		if (type_enum != ECPGt_varchar &&
			type_enum != ECPGt_char &&
			type_enum != ECPGt_unsigned_char &&
			type_enum != ECPGt_string &&
			atoi(thisPtr->type->type_index) >= 0)
			mmerror(PARSE_ERROR, ET_ERROR, "multidimensional arrays for simple data types are not supported");

		types = thisPtr;
	}
}
%}

%expect 0
%name-prefix="base_yy"
%locations

%union {
	double	dval;
	char	*str;
	int		ival;
	struct	when		action;
	struct	index		index;
	int		tagname;
	struct	this_type	type;
	enum	ECPGttype	type_enum;
	enum	ECPGdtype	dtype_enum;
	struct	fetch_desc	descriptor;
	struct  su_symbol	struct_union;
	struct	prep		prep;
}
/* tokens */
/* src/interfaces/ecpg/preproc/ecpg.tokens */

/* special embedded SQL tokens */
%token  SQL_ALLOCATE SQL_AUTOCOMMIT SQL_BOOL SQL_BREAK
                SQL_CALL SQL_CARDINALITY
                SQL_COUNT
                SQL_DATETIME_INTERVAL_CODE
                SQL_DATETIME_INTERVAL_PRECISION SQL_DESCRIBE
                SQL_DESCRIPTOR SQL_FOUND
                SQL_FREE SQL_GET SQL_GO SQL_GOTO SQL_IDENTIFIED
                SQL_INDICATOR SQL_KEY_MEMBER SQL_LENGTH
                SQL_LONG SQL_NULLABLE SQL_OCTET_LENGTH
                SQL_OPEN SQL_OUTPUT SQL_REFERENCE
                SQL_RETURNED_LENGTH SQL_RETURNED_OCTET_LENGTH SQL_SCALE
                SQL_SECTION SQL_SHORT SQL_SIGNED SQL_SQL SQL_SQLERROR
                SQL_SQLPRINT SQL_SQLWARNING SQL_START SQL_STOP
                SQL_STRUCT SQL_UNSIGNED SQL_VAR SQL_WHENEVER

/* C tokens */
%token  S_ADD S_AND S_ANYTHING S_AUTO S_CONST S_DEC S_DIV
                S_DOTPOINT S_EQUAL S_EXTERN S_INC S_LSHIFT S_MEMPOINT
                S_MEMBER S_MOD S_MUL S_NEQUAL S_OR S_REGISTER S_RSHIFT
                S_STATIC S_SUB S_VOLATILE
                S_TYPEDEF

%token CSTRING CVARIABLE CPP_LINE SQL_IP
%token DOLCONST ECONST NCONST UCONST UIDENT
/* types */
%type <str> stmt
%type <str> CreateRoleStmt
%type <str> ShrinkStmt
%type <str> opt_with
%type <str> OptRoleList
%type <str> AlterOptRoleList
%type <str> password_string
%type <str> namedata_string
%type <str> AlterOptRoleElem
%type <str> CreateOptRoleElem
%type <str> UserId
%type <str> UserIdList
%type <str> CreateUserStmt
%type <str> AlterRoleStmt
%type <str> opt_in_database
%type <str> AlterRoleSetStmt
%type <str> AlterUserStmt
%type <str> AlterUserSetStmt
%type <str> DropRoleStmt
%type <str> DropUserStmt
%type <str> CreateGroupStmt
%type <str> AlterGroupStmt
%type <str> add_drop
%type <str> AlterSessionStmt
%type <str> AlterSystemStmt
%type <str> altersys_option
%type <str> DropGroupStmt
%type <str> CreateSchemaStmt
%type <str> OptSchemaName
%type <str> OptSchemaEltList
%type <str> OptBlockchainWith
%type <str> AlterSchemaStmt
%type <str> OptAlterToBlockchain
%type <str> schema_stmt
%type <str> VariableSetStmt
%type <str> set_rest
%type <str> generic_set
%type <str> set_rest_more
%type <str> VariableMultiSetStmt
%type <str> VariableSetElemsList
%type <str> VariableSetElem
%type <str> user_defined_single
%type <str> set_global
%type <str> generic_set_extension
%type <str> set_session_extension
%type <str> set_global_extension
%type <str> guc_variable_set
%type <str> guc_value_extension_list
%type <str> set_ident_expr
%type <str> set_expr
%type <str> set_expr_extension
%type <str> uservar_name
%type <str> var_name
%type <str> var_list
%type <str> var_value
%type <str> iso_level
%type <str> opt_boolean_or_string
%type <str> zone_value
%type <str> opt_encoding
%type <str> ColId_or_Sconst
%type <str> VariableResetStmt
%type <str> SetResetClause
%type <str> FunctionSetResetClause
%type <str> VariableShowStmt
%type <str> ConstraintsSetStmt
%type <str> constraints_set_list
%type <str> constraints_set_mode
%type <str> ShutdownStmt
%type <str> CheckPointStmt
%type <str> DiscardStmt
%type <str> AlterTableStmt
%type <str> modify_column_cmds
%type <str> modify_column_cmd
%type <str> opt_enable
%type <str> add_column_cmds
%type <str> alter_table_or_partition
%type <str> alter_table_cmds
%type <str> alter_partition_cmds
%type <str> alter_partition_cmd
%type <str> move_partition_cmd
%type <str> exchange_partition_cmd
%type <str> reset_partition_cmd
%type <str> alter_table_cmd
%type <str> alter_column_default
%type <str> opt_drop_behavior
%type <str> opt_collate_clause
%type <str> alter_using
%type <str> reloptions
%type <str> replica_identity
%type <str> opt_reloptions
%type <str> opt_index_options
%type <str> index_options
%type <str> index_option
%type <str> opt_table_index_options
%type <str> table_index_options
%type <str> table_index_option
%type <str> opt_table_options
%type <str> table_options
%type <str> table_option
%type <str> opt_comma
%type <str> opt_column_options
%type <str> column_options
%type <str> column_option
%type <str> opt_part_options
%type <str> part_options
%type <str> part_option
%type <str> reloption_list
%type <str> reloption_elem
%type <str> split_dest_partition_define_list
%type <str> split_dest_listsubpartition_define_list
%type <str> split_dest_rangesubpartition_define_list
%type <str> AlterCompositeTypeStmt
%type <str> alter_type_cmds
%type <str> alter_type_cmd
%type <str> ClosePortalStmt
%type <str> CopyStmt
%type <str> opt_processed
%type <str> opt_load
%type <str> opt_useeof
%type <str> copy_from
%type <str> copy_file_name
%type <str> copy_options
%type <str> copy_opt_list
%type <str> copy_opt_item
%type <str> opt_binary
%type <str> opt_oids
%type <str> copy_delimiter
%type <str> opt_using
%type <str> opt_noescaping
%type <str> OptCopyLogError
%type <str> OptCopyRejectLimit
%type <str> copy_generic_opt_list
%type <str> copy_generic_opt_elem
%type <str> copy_generic_opt_arg
%type <str> copy_generic_opt_arg_list
%type <str> copy_generic_opt_arg_list_item
%type <str> copy_foramtter_opt
%type <str> copy_col_format_def
%type <str> copy_column_expr_list
%type <str> copy_column_expr_item
%type <str> OptCopyColTypename
%type <str> OptCopyColExpr
%type <str> copy_column_sequence_list
%type <str> copy_column_sequence_item
%type <str> column_sequence_item_step
%type <str> column_sequence_item_sart
%type <str> copy_column_filler_list
%type <str> copy_column_filler_item
%type <str> copy_column_constant_list
%type <str> copy_column_constant_item
%type <str> CreateStreamStmt
%type <str> PurgeStmt
%type <str> TimeCapsuleStmt
%type <str> opt_rename
%type <str> CreateStmt
%type <str> OptKind
%type <str> opt_table_partitioning_clause
%type <str> range_partitioning_clause
%type <str> list_partitioning_clause
%type <str> hash_partitioning_clause
%type <str> opt_columns
%type <str> opt_partitions_num
%type <str> opt_subpartitions_num
%type <str> opt_hash_partition_definition_list
%type <str> value_partitioning_clause
%type <str> subpartitioning_clause
%type <str> range_subpartitioning_clause
%type <str> list_subpartitioning_clause
%type <str> hash_subpartitioning_clause
%type <str> subpartition_definition_list
%type <str> subpartition_item
%type <str> column_item_list
%type <str> column_item
%type <str> opt_interval_partition_clause
%type <str> opt_interval_tablespaceList
%type <str> interval_expr
%type <str> tablespaceList
%type <str> range_partition_definition_list
%type <str> list_partition_definition_list
%type <str> hash_partition_definition_list
%type <str> range_less_than_list
%type <str> list_partition_item
%type <str> opt_in_p
%type <str> hash_partition_item
%type <str> range_partition_boundary
%type <str> range_less_than_item
%type <str> range_start_end_list
%type <str> range_start_end_item
%type <str> opt_range_every_list
%type <str> partition_name
%type <str> maxValueList
%type <str> maxValueItem
%type <str> listValueList
%type <str> opt_row_movement_clause
%type <str> OptTemp
%type <str> OptTableElementList
%type <str> OptTypedTableElementList
%type <str> TableElementList
%type <str> TypedTableElementList
%type <str> TableElement
%type <str> TypedTableElement
%type <str> columnDef
%type <str> add_column_first_after
%type <str> KVType
%type <str> ColCmprsMode
%type <str> columnOptions
%type <str> WithOptions
%type <str> ColQualList
%type <str> ColConstraint
%type <str> with_algorithm
%type <str> algorithm_desc
%type <str> columnEncryptionKey
%type <str> encryptionType
%type <str> setting_name
%type <str> CreateKeyStmt
%type <str> CreateMasterKeyStmt
%type <str> master_key_params
%type <str> master_key_elem
%type <str> CreateColumnKeyStmt
%type <str> column_key_params
%type <str> column_key_elem
%type <str> datatypecl
%type <str> InformationalConstraintElem
%type <str> ColConstraintElem
%type <str> opt_unique_key
%type <str> generated_column_option
%type <str> ConstraintAttr
%type <str> TableLikeClause
%type <str> excluding_option_list
%type <str> TableLikeOptionList
%type <str> TableLikeIncludingOption
%type <str> TableLikeExcludingOption
%type <str> opt_internal_data
%type <str> internal_data_body
%type <str> TableConstraint
%type <str> ConstraintElem
%type <str> opt_no_inherit
%type <str> opt_column_list
%type <str> columnList
%type <str> columnElem
%type <str> opt_c_include
%type <str> key_match
%type <str> ExclusionConstraintList
%type <str> ExclusionConstraintElem
%type <str> ExclusionWhereClause
%type <str> key_actions
%type <str> key_update
%type <str> key_delete
%type <str> key_action
%type <str> OptInherit
%type <str> OptWith
%type <str> OnCommitOption
%type <str> AutoIncrementValue
%type <str> OptAutoIncrement
%type <str> OptTableSpace
%type <str> OptGPI
%type <str> OptCompress
%type <str> OptDistributeBy
%type <str> OptDatanodeName
%type <str> OptDistributeType
%type <str> OptDistributeByInternal
%type <str> distribute_by_list_clause
%type <str> OptListDistribution
%type <str> list_dist_state
%type <str> list_distribution_rules_list
%type <str> list_dist_value
%type <str> list_distribution_rule_row
%type <str> list_distribution_rule_single
%type <str> distribute_by_range_clause
%type <str> SliceReferenceClause
%type <str> range_slice_definition_list
%type <str> range_slice_less_than_list
%type <str> range_slice_less_than_item
%type <str> range_slice_start_end_list
%type <str> range_slice_start_end_item
%type <str> OptSubCluster
%type <str> OptSubClusterInternal
%type <str> OptConsTableSpace
%type <str> OptConsTableSpaceWithEmpty
%type <str> OptPartitionElement
%type <str> OptPctFree
%type <str> OptInitRans
%type <str> OptMaxTrans
%type <str> OptStorage
%type <str> OptInitial
%type <str> OptNext
%type <str> OptMinextents
%type <str> OptMaxextents
%type <str> ExistingIndex
%type <str> OptDuplicate
%type <str> create_as_target
%type <str> opt_with_data
%type <str> SnapshotStmt
%type <str> SnapshotVersion
%type <str> OptSnapshotVersion
%type <str> OptSnapshotComment
%type <str> AlterSnapshotCmdList
%type <str> AlterSnapshotCmdListWithParens
%type <str> AlterSnapshotCmdListNoParens
%type <str> AlterSnapshotCmdOrEmpty
%type <str> OptAlterUpdateSnapshot
%type <str> OptInsertIntoSnapshot
%type <str> OptDeleteFromSnapshot
%type <str> OptSnapshotAlias
%type <str> AlterSnapshotDdlList
%type <str> AlterSnapshotDdl
%type <str> SnapshotSample
%type <str> SnapshotSampleList
%type <str> OptSnapshotStratify
%type <str> CreateMatViewStmt
%type <str> create_mv_target
%type <str> OptNoLog
%type <str> opt_incremental
%type <str> RefreshMatViewStmt
%type <str> CreateSeqStmt
%type <str> AlterSeqStmt
%type <str> opt_large_seq
%type <str> OptSeqOptList
%type <str> SeqOptList
%type <str> SeqOptElem
%type <str> opt_by
%type <str> NumericOnly
%type <str> NumericOnly_list
%type <str> CreatePLangStmt
%type <str> opt_trusted
%type <str> handler_name
%type <str> opt_inline_handler
%type <str> validator_clause
%type <str> opt_validator
%type <str> DropPLangStmt
%type <str> opt_procedural
%type <str> tblspc_options
%type <str> opt_tblspc_options
%type <str> tblspc_option_list
%type <str> tblspc_option_elem
%type <str> CreateTableSpaceStmt
%type <str> LoggingStr
%type <str> OptDatafileSize
%type <str> OptReuse
%type <str> OptAuto
%type <str> OptNextStr
%type <str> OptMaxSize
%type <str> size_clause
%type <str> OptRelative
%type <str> OptTableSpaceOwner
%type <str> DropTableSpaceStmt
%type <str> CreateExtensionStmt
%type <str> create_extension_opt_list
%type <str> create_extension_opt_item
%type <str> CreateDirectoryStmt
%type <str> DropDirectoryStmt
%type <str> AlterExtensionStmt
%type <str> alter_extension_opt_list
%type <str> alter_extension_opt_item
%type <str> AlterExtensionContentsStmt
%type <str> CreateWeakPasswordDictionaryStmt
%type <str> opt_vals
%type <str> weak_password_string_list
%type <str> DropWeakPasswordDictionaryStmt
%type <str> CreateFdwStmt
%type <str> fdw_option
%type <str> fdw_options
%type <str> opt_fdw_options
%type <str> DropFdwStmt
%type <str> AlterFdwStmt
%type <str> create_generic_options
%type <str> generic_option_list
%type <str> alter_generic_options
%type <str> alter_generic_option_list
%type <str> alter_generic_option_elem
%type <str> generic_option_elem
%type <str> generic_option_name
%type <str> generic_option_arg
%type <str> fdwName
%type <str> CreateForeignServerStmt
%type <str> opt_type
%type <str> foreign_server_version
%type <str> opt_foreign_server_version
%type <str> DropForeignServerStmt
%type <str> AlterForeignServerStmt
%type <str> CreateForeignTableStmt
%type <str> ForeignTblWritable
%type <str> OptForeignTableElementList
%type <str> ForeignTableElementList
%type <str> ForeignTableElement
%type <str> ForeignColDef
%type <str> ForeignPosition
%type <str> ForeignTableLikeClause
%type <str> OptForeignTableLogError
%type <str> OptForeignTableLogRemote
%type <str> OptPerNodeRejectLimit
%type <str> OptForeignPartBy
%type <str> OptForeignPartAuto
%type <str> partition_item_list
%type <str> partition_item
%type <str> AlterForeignTableStmt
%type <str> CreateUserMappingStmt
%type <str> auth_ident
%type <str> DropUserMappingStmt
%type <str> AlterUserMappingStmt
%type <str> CreateModelStmt
%type <str> features_clause
%type <str> target_clause
%type <str> with_hyperparameters_clause
%type <str> hyperparameter_name_value_list
%type <str> hyperparameter_name_value
%type <str> DropModelStmt
%type <str> CreateRlsPolicyStmt
%type <str> AlterRlsPolicyStmt
%type <str> DropRlsPolicyStmt
%type <str> RowLevelSecurityPolicyName
%type <str> RLSOptionalUsingExpr
%type <str> RLSDefaultToRole
%type <str> RLSOptionalToRole
%type <str> row_level_security_role_list
%type <str> row_level_security_role
%type <str> RLSDefaultPermissive
%type <str> RLSDefaultForCmd
%type <str> row_level_security_cmd
%type <str> CreateSynonymStmt
%type <str> DropSynonymStmt
%type <str> CreateDataSourceStmt
%type <str> data_source_type
%type <str> opt_data_source_type
%type <str> data_source_version
%type <str> opt_data_source_version
%type <str> AlterDataSourceStmt
%type <str> DropDataSourceStmt
%type <str> CreateTrigStmt
%type <str> TriggerActionTime
%type <str> TriggerEvents
%type <str> TriggerOneEvent
%type <str> TriggerForSpec
%type <str> TriggerForOptEach
%type <str> TriggerForType
%type <str> TriggerWhen
%type <str> TriggerFuncArgs
%type <str> TriggerFuncArg
%type <str> trigger_order
%type <str> OptConstrFromTable
%type <str> ConstraintAttributeSpec
%type <str> ConstraintAttributeElem
%type <str> CreateEventTrigStmt
%type <str> event_trigger_when_list
%type <str> event_trigger_when_item
%type <str> event_trigger_value_list
%type <str> AlterEventTrigStmt
%type <str> enable_trigger
%type <str> DropTrigStmt
%type <str> CreateAssertStmt
%type <str> DropAssertStmt
%type <str> DefineStmt
%type <str> opt_cfoptions
%type <str> cfoptions'('
%type <str> cfoption_list
%type <str> cfoption_elem
%type <str> tsconf_definition
%type <str> tsconf_def_list
%type <str> tsconf_def_elem
%type <str> definition
%type <str> def_list
%type <str> def_elem
%type <str> def_arg
%type <str> aggr_args
%type <str> old_aggr_definition
%type <str> old_aggr_list
%type <str> old_aggr_elem
%type <str> opt_enum_val_list
%type <str> enum_val_list
%type <str> AlterEnumStmt
%type <str> opt_if_not_exists
%type <str> CreateOpClassStmt
%type <str> opclass_item_list
%type <str> opclass_item
%type <str> opt_default
%type <str> opt_opfamily
%type <str> opclass_purpose
%type <str> opt_recheck
%type <str> CreateOpFamilyStmt
%type <str> AlterOpFamilyStmt
%type <str> opclass_drop_list
%type <str> opclass_drop
%type <str> DropOpClassStmt
%type <str> DropOpFamilyStmt
%type <str> DropOwnedStmt
%type <str> ReassignOwnedStmt
%type <str> DropStmt
%type <str> opt_purge
%type <str> drop_type
%type <str> collate_name
%type <str> type_name_list
%type <str> any_name_list
%type <str> any_name
%type <str> attrs
%type <str> TruncateStmt
%type <str> opt_restart_seqs
%type <str> CommentStmt
%type <str> comment_type
%type <str> comment_text
%type <str> SecLabelStmt
%type <str> opt_provider
%type <str> security_label_type
%type <str> security_label
%type <str> FetchStmt
%type <str> fetch_args
%type <str> from_in
%type <str> opt_from_in
%type <str> GrantStmt
%type <str> RevokeStmt
%type <str> privilege_str
%type <str> privileges
%type <str> privilege_list
%type <str> privilege
%type <str> privilege_target
%type <str> grantee_list
%type <str> grantee
%type <str> opt_grant_grant_option
%type <str> function_with_argtypes_list
%type <str> function_with_argtypes
%type <str> GrantRoleStmt
%type <str> RevokeRoleStmt
%type <str> opt_grant_admin_option
%type <str> opt_granted_by
%type <str> GrantDbStmt
%type <str> RevokeDbStmt
%type <str> db_privileges
%type <str> db_privilege_list
%type <str> db_privilege
%type <str> AlterDefaultPrivilegesStmt
%type <str> DefACLOptionList
%type <str> DefACLOption
%type <str> DefACLAction
%type <str> defacl_privilege_target
%type <str> IndexStmt
%type <str> opt_unique
%type <str> opt_concurrently
%type <str> opt_index_name
%type <str> key_usage_list
%type <str> index_hint_definition
%type <str> index_hint_list
%type <str> opt_index_hint_list
%type <str> access_method_clause
%type <str> access_method_clause_without_keyword
%type <str> index_params
%type <str> index_elem
%type <str> constraint_params
%type <str> con_asc_desc
%type <str> constraint_elem
%type <str> index_functional_expr_key
%type <str> opt_include
%type <str> index_including_params
%type <str> collate
%type <str> opt_collate
%type <str> default_collate
%type <str> opt_collation
%type <str> opt_class
%type <str> opt_asc_desc
%type <str> opt_nulls_order
%type <str> opt_partition_index_def
%type <str> range_partition_index_list
%type <str> range_partition_index_item
%type <str> opt_subpartition_index_def
%type <str> range_subpartition_index_list
%type <str> range_subpartition_index_item
%type <str> CreateFunctionStmt
%type <str> CallFuncStmt
%type <str> callfunc_args
%type <str> CreateEventStmt
%type <str> definer_opt
%type <str> user
%type <str> every_interval
%type <str> start_expr
%type <str> end_expr
%type <str> ev_timeexpr
%type <str> interval_list
%type <str> interval_cell
%type <str> initime
%type <str> functime_app
%type <str> functime_expr
%type <str> interval_intexpr
%type <str> opt_ev_on_completion
%type <str> opt_ev_status
%type <str> comment_opt
%type <str> ev_body
%type <str> AlterEventStmt
%type <str> definer_name_opt
%type <str> end_opt
%type <str> start_opt
%type <str> preserve_opt
%type <str> rename_opt
%type <str> status_opt
%type <str> comments_opt
%type <str> action_opt
%type <str> DropEventStmt
%type <str> ShowEventStmt
%type <str> event_from_clause
%type <str> event_where_clause
%type <str> ev_where_body
%type <str> CreateProcedureStmt
%type <str> CreatePackageStmt
%type <str> pkg_name
%type <str> invoker_rights
%type <str> definer_expression
%type <str> definer_user
%type <str> pkg_body_subprogram
%type <str> CreatePackageBodyStmt
%type <str> opt_or_replace
%type <str> func_args
%type <str> proc_args
%type <str> func_args_list
%type <str> as_is
%type <str> as_empty
%type <str> func_args_with_defaults
%type <str> func_args_with_defaults_list
%type <str> func_arg
%type <str> arg_class
%type <str> param_name
%type <str> func_return
%type <str> func_type
%type <str> func_arg_with_default
%type <str> createfunc_opt_list
%type <str> opt_createproc_opt_list
%type <str> common_func_opt_item
%type <str> createfunc_opt_item
%type <str> createproc_opt_item
%type <str> func_as
%type <str> subprogram_body
%type <str> opt_definition
%type <str> table_func_column
%type <str> table_func_column_list
%type <str> RemovePackageStmt
%type <str> AlterFunctionStmt
%type <str> AlterProcedureStmt
%type <str> alterfunc_opt_list
%type <str> opt_restrict
%type <str> RemoveFuncStmt
%type <str> RemoveAggrStmt
%type <str> RemoveOperStmt
%type <str> oper_argtypes
%type <str> any_operator
%type <str> DoStmt
%type <str> dostmt_opt_list
%type <str> dostmt_opt_item
%type <str> CreateCastStmt
%type <str> cast_context
%type <str> DropCastStmt
%type <str> opt_if_exists
%type <str> ReindexStmt
%type <str> reindex_type
%type <str> opt_force
%type <str> RenameStmt
%type <str> rename_clause_list
%type <str> rename_clause
%type <str> opt_column
%type <str> opt_set_data
%type <str> AlterObjectSchemaStmt
%type <str> AlterOwnerStmt
%type <str> CreatePublicationStmt
%type <str> opt_publication_for_tables
%type <str> publication_for_tables
%type <str> AlterPublicationStmt
%type <str> CreateSubscriptionStmt
%type <str> publication_name_list
%type <str> publication_name_item
%type <str> AlterSubscriptionStmt
%type <str> DropSubscriptionStmt
%type <str> TypeOwner
%type <str> RuleStmt
%type <str> RuleActionList
%type <str> RuleActionMulti
%type <str> RuleActionStmt
%type <str> RuleActionStmtOrEmpty
%type <str> event
%type <str> opt_instead
%type <str> DropRuleStmt
%type <str> NotifyStmt
%type <str> notify_payload
%type <str> ListenStmt
%type <str> UnlistenStmt
%type <str> TransactionStmt
%type <str> opt_transaction
%type <str> transaction_mode_item
%type <str> transaction_mode_list
%type <str> transaction_mode_list_or_empty
%type <str> CreateContQueryStmt
%type <str> ViewStmt
%type <str> opt_check_option
%type <str> LoadStmt
%type <str> load_options_list
%type <str> load_options_item
%type <str> opt_load_data
%type <str> opt_load_data_options_list
%type <str> opt_load_data_options_item
%type <str> load_oper_table_type
%type <str> load_type_set
%type <str> load_table_options_list
%type <str> load_table_options_item
%type <str> load_column_expr_list
%type <str> load_column_expr_item
%type <str> load_col_sequence_item_sart
%type <str> load_col_sequence
%type <str> load_col_scalar_spec
%type <str> load_col_position_spec
%type <str> load_col_nullif_spec
%type <str> load_col_data_type
%type <str> load_col_sql_str
%type <str> load_when_option
%type <str> load_when_option_list
%type <str> load_when_option_item
%type <str> load_quote_str
%type <str> CreatedbStmt
%type <str> createdb_opt_list
%type <str> createdb_opt_item
%type <str> opt_equal
%type <str> AlterDatabaseStmt
%type <str> AlterDatabaseSetStmt
%type <str> alterdb_opt_list
%type <str> alterdb_opt_item
%type <str> DropdbStmt
%type <str> CreateDomainStmt
%type <str> AlterDomainStmt
%type <str> opt_as
%type <str> AlterTSDictionaryStmt
%type <str> AlterTSConfigurationStmt
%type <str> CreateConversionStmt
%type <str> ClusterStmt
%type <str> cluster_index_specification
%type <str> VacuumStmt
%type <str> vacuum_option_list
%type <str> vacuum_option_elem
%type <str> AnalyzeStmt
%type <str> VerifyStmt
%type <str> analyze_keyword
%type <str> opt_verify_options
%type <str> opt_verbose
%type <str> opt_full
%type <str> opt_compact
%type <str> opt_hdfsdirectory
%type <str> opt_freeze
%type <str> opt_deltamerge
%type <str> opt_verify
%type <str> opt_cascade
%type <str> opt_name_list
%type <str> opt_analyze_column_define
%type <str> opt_multi_name_list
%type <str> BarrierStmt
%type <str> opt_barrier_id
%type <str> CreateNodeStmt
%type <str> pgxcnode_name
%type <str> pgxcgroup_name
%type <str> pgxcnodes
%type <str> pgxcnode_list
%type <str> bucket_maps
%type <str> bucket_list
%type <str> bucket_cnt
%type <str> pgxcgroup_parent
%type <str> opt_vcgroup
%type <str> opt_to_elastic_group
%type <str> opt_redistributed
%type <str> opt_set_vcgroup
%type <str> AlterNodeStmt
%type <str> AlterCoordinatorStmt
%type <str> DropNodeStmt
%type <str> opt_pgxcnodes
%type <str> CreateNodeGroupStmt
%type <str> AlterNodeGroupStmt
%type <str> DropNodeGroupStmt
%type <str> CreateAuditPolicyStmt
%type <str> policy_privileges_list
%type <str> policy_privilege_elem
%type <str> policy_privilege_type
%type <str> policy_access_list
%type <str> policy_access_elem
%type <str> policy_access_type
%type <str> policy_target_elem_opt
%type <str> policy_targets_list
%type <str> policy_target_type
%type <str> policy_target_name
%type <str> policy_filter_opt
%type <str> policy_filter_value
%type <str> filter_set
%type <str> filter_expr_list
%type <str> filter_paren
%type <str> policy_filters_list
%type <str> policy_filter_type
%type <str> policy_filter_name
%type <str> policy_name
%type <str> policy_status_opt
%type <str> AlterAuditPolicyStmt
%type <str> alter_policy_access_list
%type <str> alter_policy_privileges_list
%type <str> alter_policy_filter_list
%type <str> alter_policy_action_clause
%type <str> policy_status_alter_clause
%type <str> policy_comments_alter_clause
%type <str> DropAuditPolicyStmt
%type <str> policy_names_list
%type <str> CreateMaskingPolicyStmt
%type <str> masking_clause
%type <str> masking_clause_elem
%type <str> masking_func_nsp
%type <str> masking_func
%type <str> masking_func_params_opt
%type <str> masking_func_params_list
%type <str> masking_func_param
%type <str> masking_target
%type <str> masking_policy_target_type
%type <str> alter_policy_condition
%type <str> policy_condition_opt
%type <str> masking_policy_condition_operator
%type <str> masking_policy_condition_value
%type <str> pp_filter_expr
%type <str> filter_expr
%type <str> pp_filter_term
%type <str> filter_term
%type <str> pp_policy_filter_elem
%type <str> policy_filter_elem
%type <str> AlterMaskingPolicyStmt
%type <str> alter_masking_policy_action_clause
%type <str> alter_masking_policy_func_items_list
%type <str> alter_masking_policy_func_item
%type <str> DropMaskingPolicyStmt
%type <str> CreatePolicyLabelStmt
%type <str> policy_label_name
%type <str> opt_add_resources_to_label
%type <str> resources_to_label_list
%type <str> resources_to_label_list_item
%type <str> policy_label_resource_type
%type <str> policy_label_any_resource_item
%type <str> policy_label_any_resource
%type <str> policy_label_items
%type <str> filters_to_label_list
%type <str> filters_to_label_list_item
%type <str> policy_label_filter_type
%type <str> policy_label_item
%type <str> AlterPolicyLabelStmt
%type <str> resources_or_filters_to_label_list
%type <str> DropPolicyLabelStmt
%type <str> policy_labels_list
%type <str> CreateResourcePoolStmt
%type <str> AlterResourcePoolStmt
%type <str> AlterGlobalConfigStmt
%type <str> DropResourcePoolStmt
%type <str> resource_pool_name
%type <str> DropGlobalConfigStmt
%type <str> CreateWorkloadGroupStmt
%type <str> AlterWorkloadGroupStmt
%type <str> DropWorkloadGroupStmt
%type <str> workload_group_name
%type <str> CreateAppWorkloadGroupMappingStmt
%type <str> AlterAppWorkloadGroupMappingStmt
%type <str> DropAppWorkloadGroupMappingStmt
%type <str> application_name
%type <str> ExplainStmt
%type <str> ExplainableStmt
%type <str> explain_option_list
%type <str> explain_option_elem
%type <str> explain_option_name
%type <str> explain_option_arg
%type <str> ExecDirectStmt
%type <str> DirectStmt
%type <str> CleanConnStmt
%type <str> CleanConnDbName
%type <str> CleanConnUserName
%type <str> opt_check
%type <prep> PrepareStmt
%type <str> prep_type_clause
%type <str> PreparableStmt
%type <str> ExecuteStmt
%type <str> execute_param_clause
%type <str> insert_partition_clause
%type <str> update_delete_partition_clause
%type <str> InsertStmt
%type <str> insert_target
%type <str> insert_rest
%type <str> insert_column_list
%type <str> insert_column_item
%type <str> returning_clause
%type <str> upsert_clause
%type <str> DeleteStmt
%type <str> using_clause
%type <str> LockStmt
%type <str> opt_lock
%type <str> lock_type
%type <str> opt_nowait
%type <str> opt_cancelable
%type <str> opt_wait
%type <str> opt_nowait_or_skip
%type <str> UpdateStmt
%type <str> set_clause_list
%type <str> set_clause
%type <str> single_set_clause
%type <str> multiple_set_clause
%type <str> set_target
%type <str> set_target_list
%type <str> MergeStmt
%type <str> merge_when_list
%type <str> merge_when_clause
%type <str> opt_merge_where_condition
%type <str> merge_update
%type <str> merge_insert
%type <str> merge_values_clause
%type <str> DeclareCursorStmt
%type <str> cursor_name
%type <str> cursor_options
%type <str> opt_hold
%type <str> SelectStmt
%type <str> select_with_parens
%type <str> select_no_parens
%type <str> select_clause
%type <str> simple_select
%type <str> hint_string
%type <str> with_clause
%type <str> cte_list
%type <str> common_table_expr
%type <str> opt_materialized
%type <str> opt_with_clause
%type <str> opt_into_clause
%type <str> into_clause
%type <str> characterset_option
%type <str> fields_options_fin
%type <str> fields_options_list
%type <str> fields_options_item
%type <str> lines_options_fin
%type <str> lines_options_list
%type <str> lines_option_item
%type <str> into_user_var_list
%type <str> OptTempTableName
%type <str> opt_table
%type <str> opt_all
%type <str> opt_distinct
%type <str> opt_sort_clause
%type <str> sort_clause
%type <str> siblings_clause
%type <str> sortby_list
%type <str> sortby
%type <str> select_limit
%type <str> opt_select_limit
%type <str> opt_delete_limit
%type <str> limit_clause
%type <str> limit_offcnt_clause
%type <str> offset_clause
%type <str> select_limit_value
%type <str> select_offset_value
%type <str> opt_select_fetch_first_value
%type <str> select_offset_value2
%type <str> row_or_rows
%type <str> first_or_next
%type <str> group_clause
%type <str> group_by_list
%type <str> group_by_item
%type <str> empty_grouping_set
%type <str> rollup_clause
%type <str> cube_clause
%type <str> grouping_sets_clause
%type <str> having_clause
%type <str> start_with_clause
%type <str> start_with_expr
%type <str> connect_by_expr
%type <str> for_locking_clause
%type <str> opt_for_locking_clause
%type <str> for_locking_items
%type <str> for_locking_item
%type <str> for_locking_strength
%type <str> locked_rels_list
%type <str> values_clause
%type <str> from_clause
%type <str> from_list
%type <str> table_ref
%type <str> joined_table
%type <str> alias_clause
%type <str> opt_alias_clause
%type <str> join_type
%type <str> join_outer
%type <str> join_qual
%type <str> relation_expr
%type <str> relation_expr_list
%type <str> delete_relation_expr_opt_alias
%type <str> relation_expr_opt_alias
%type <str> relation_expr_opt_alias_list
%type <str> tablesample_clause
%type <str> timecapsule_clause
%type <str> opt_timecapsule_clause
%type <str> opt_repeatable_clause
%type <str> func_table
%type <str> where_clause
%type <str> where_or_current_clause
%type <str> OptTableFuncElementList
%type <str> TableFuncElementList
%type <str> TableFuncElement
%type <str> Typename
%type <index> opt_array_bounds
%type <str> SimpleTypename
%type <str> ConstTypename
%type <str> GenericType
%type <str> opt_type_modifiers
%type <str> Numeric
%type <str> opt_float
%type <str> Bit
%type <str> ConstBit
%type <str> BitWithLength
%type <str> BitWithoutLength
%type <str> Character
%type <str> ConstCharacter
%type <str> CharacterWithLength
%type <str> CharacterWithoutLength
%type <str> character
%type <str> opt_varying
%type <str> character_set
%type <str> charset_collate_name
%type <str> charset
%type <str> convert_charset
%type <str> opt_charset
%type <str> default_charset
%type <str> optCharsetCollate
%type <str> CharsetCollate
%type <str> charset_collate
%type <str> ConstDatetime
%type <str> ConstSet
%type <str> ConstInterval
%type <str> opt_timezone
%type <str> opt_interval
%type <str> interval_second
%type <str> client_logic_type
%type <str> a_expr
%type <str> b_expr
%type <str> c_expr
%type <str> c_expr_noparen
%type <str> func_expr
%type <str> func_application
%type <str> func_application_special
%type <str> func_with_separator
%type <str> func_expr_windowless
%type <str> func_expr_common_subexpr
%type <str> xml_root_version
%type <str> opt_xml_root_standalone
%type <str> xml_attributes
%type <str> xml_attribute_list
%type <str> xml_attribute_el
%type <str> document_or_content
%type <str> xml_whitespace_option
%type <str> xmlexists_argument
%type <str> within_group_clause
%type <str> window_clause
%type <str> window_definition_list
%type <str> window_definition
%type <str> over_clause
%type <str> window_specification
%type <str> opt_existing_window_name
%type <str> opt_partition_clause
%type <str> opt_frame_clause
%type <str> frame_extent
%type <str> frame_bound
%type <str> row
%type <str> explicit_row
%type <str> implicit_row
%type <str> sub_type
%type <str> all_Op
%type <str> MathOp
%type <str> qual_Op
%type <str> qual_all_Op
%type <str> subquery_Op
%type <str> expr_list
%type <str> func_arg_list
%type <str> func_arg_expr
%type <str> type_list
%type <str> array_expr
%type <str> array_expr_list
%type <str> extract_list
%type <str> extract_arg
%type <str> timestamp_arg_list
%type <str> timestamp_units
%type <str> overlay_list
%type <str> overlay_placing
%type <str> position_list
%type <str> substr_list
%type <str> substr_from
%type <str> substr_for
%type <str> trim_list
%type <str> in_expr
%type <str> case_expr
%type <str> when_clause_list
%type <str> when_clause
%type <str> case_default
%type <str> case_arg
%type <str> columnref
%type <str> indirection_el
%type <str> indirection
%type <str> opt_indirection
%type <str> opt_asymmetric
%type <str> ctext_expr
%type <str> ctext_expr_list
%type <str> ctext_row
%type <str> target_list
%type <str> target_el
%type <str> connect_by_root_expr
%type <str> qualified_name_list
%type <str> qualified_name
%type <str> name_list
%type <str> name
%type <str> database_name
%type <str> access_method
%type <str> attr_name
%type <str> index_name
%type <str> file_name
%type <str> func_name
%type <str> func_name_opt_arg
%type <str> AexprConst
%type <str> Iconst
%type <str> RoleId
%type <str> SignedIconst
%type <str> DelimiterStmt
%type <str> delimiter_str_names
%type <str> delimiter_str_name
%type <str> unreserved_keyword
%type <str> col_name_keyword
%type <str> col_name_keyword_nonambiguous
%type <str> type_func_name_keyword
%type <str> reserved_keyword
/* ecpgtype */
/* src/interfaces/ecpg/preproc/ecpg.type */
%type <str> ECPGAllocateDescr
%type <str> ECPGCKeywords
%type <str> ECPGColId
%type <str> ECPGColLabel
%type <str> ECPGColLabelCommon
%type <str> ECPGConnect
%type <str> ECPGCursorStmt
%type <str> ECPGDeallocateDescr
%type <str> ECPGDeclaration
%type <str> ECPGDeclare
%type <str> ECPGDescribe
%type <str> ECPGDisconnect
%type <str> ECPGExecuteImmediateStmt
%type <str> ECPGFree
%type <str> ECPGGetDescHeaderItem
%type <str> ECPGGetDescItem
%type <str> ECPGGetDescriptorHeader
%type <str> ECPGKeywords
%type <str> ECPGKeywords_rest
%type <str> ECPGKeywords_vanames
%type <str> ECPGOpen
%type <str> ECPGSetAutocommit
%type <str> ECPGSetConnection
%type <str> ECPGSetDescHeaderItem
%type <str> ECPGSetDescItem
%type <str> ECPGSetDescriptorHeader
%type <str> ECPGTypeName
%type <str> ECPGTypedef
%type <str> ECPGVar
%type <str> ECPGVarDeclaration
%type <str> ECPGWhenever
%type <str> ECPGunreserved_interval
%type <str> UsingConst
%type <str> UsingValue
%type <str> all_unreserved_keyword
%type <str> c_anything
%type <str> c_args
%type <str> c_list
%type <str> c_stuff
%type <str> c_stuff_item
%type <str> c_term
%type <str> c_thing
%type <str> char_variable
%type <str> char_civar
%type <str> civar
%type <str> civarind
%type <str> ColId
%type <str> ColLabel
%type <str> connect_options
%type <str> connection_object
%type <str> connection_target
%type <str> coutputvariable
%type <str> cvariable
%type <str> db_prefix
%type <str> CreateAsStmt
%type <str> DeallocateStmt
%type <str> dis_name
%type <str> ecpg_bconst
%type <str> ecpg_fconst
%type <str> ecpg_ident
%type <str> ecpg_interval
%type <str> ecpg_into
%type <str> ecpg_fetch_into
%type <str> ecpg_param
%type <str> ecpg_sconst
%type <str> ecpg_using
%type <str> ecpg_xconst
%type <str> enum_definition
%type <str> enum_type
%type <str> execstring
%type <str> execute_rest
%type <str> indicator
%type <str> into_descriptor
%type <str> into_sqlda
%type <str> Iresult
%type <str> on_off
%type <str> opt_bit_field
%type <str> opt_connection_name
%type <str> opt_database_name
%type <str> opt_ecpg_fetch_into
%type <str> opt_ecpg_using
%type <str> opt_initializer
%type <str> opt_options
%type <str> opt_output
%type <str> opt_pointer
%type <str> opt_port
%type <str> opt_reference
%type <str> opt_scale
%type <str> opt_server
%type <str> opt_user
%type <str> opt_opt_value
%type <str> ora_user
%type <str> precision
%type <str> prepared_name
%type <str> quoted_ident_stringvar
%type <str> s_struct_union
%type <str> server
%type <str> server_name
%type <str> single_vt_declaration
%type <str> storage_clause
%type <str> storage_declaration
%type <str> storage_modifier
%type <str> struct_union_type
%type <str> struct_union_type_with_symbol
%type <str> symbol
%type <str> type_declaration
%type <str> type_function_name
%type <str> user_name
%type <str> using_descriptor
%type <str> var_declaration
%type <str> var_type_declarations
%type <str> variable
%type <str> variable_declarations
%type <str> variable_list
%type <str> vt_declarations

%type <str> Op
%type <str> IntConstVar
%type <str> AllConstVar
%type <str> CSTRING
%type <str> CPP_LINE
%type <str> CVARIABLE
%type <str> DOLCONST
%type <str> ECONST
%type <str> NCONST
%type <str> SCONST
%type <str> UCONST
%type <str> UIDENT

%type  <struct_union> s_struct_union_symbol

%type  <descriptor> ECPGGetDescriptor
%type  <descriptor> ECPGSetDescriptor

%type  <type_enum> simple_type
%type  <type_enum> signed_type
%type  <type_enum> unsigned_type

%type  <dtype_enum> descriptor_item
%type  <dtype_enum> desc_header_item

%type  <type>   var_type

%type  <action> action
/* orig_tokens */
 %token IDENT FCONST SCONST BCONST VCONST XCONST Op CmpOp CmpNullOp COMMENTSTRING SET_USER_IDENT SET_IDENT
 %token ICONST PARAM
 %token TYPECAST ORA_JOINOP DOT_DOT COLON_EQUALS PARA_EQUALS SET_IDENT_SESSION SET_IDENT_GLOBAL










 %token ABORT_P ABSOLUTE_P ACCESS ACCOUNT ACTION ADD_P ADMIN AFTER
 AGGREGATE ALGORITHM ALL ALSO ALTER ALWAYS ANALYSE ANALYZE AND ANY APP APPEND ARCHIVE ARRAY AS ASC
 ASSERTION ASSIGNMENT ASYMMETRIC AT ATTRIBUTE AUDIT AUTHID AUTHORIZATION AUTOEXTEND AUTOMAPPED AUTO_INCREMENT

 BACKWARD BARRIER BEFORE BEGIN_NON_ANOYBLOCK BEGIN_P BETWEEN BIGINT BINARY BINARY_DOUBLE BINARY_INTEGER BIT BLANKS
 BLOB_P BLOCKCHAIN BODY_P BOGUS BOOLEAN_P BOTH BUCKETCNT BUCKETS BY BYTEAWITHOUTORDER BYTEAWITHOUTORDERWITHEQUAL

 CACHE CALL CALLED CANCELABLE CASCADE CASCADED CASE CAST CATALOG_P CHAIN CHANGE CHAR_P
 CHARACTER CHARACTERISTICS CHARACTERSET CHARSET CHECK CHECKPOINT CLASS CLEAN CLIENT CLIENT_MASTER_KEY CLIENT_MASTER_KEYS CLOB CLOSE
 CLUSTER COALESCE COLLATE COLLATION COLUMN COLUMN_ENCRYPTION_KEY COLUMN_ENCRYPTION_KEYS COLUMNS COMMENT COMMENTS COMMIT
 COMMITTED COMPACT COMPATIBLE_ILLEGAL_CHARS COMPLETE COMPLETION COMPRESS CONCURRENTLY CONDITION CONFIGURATION CONNECTION CONSTANT CONSTRAINT CONSTRAINTS
 CONTENT_P CONTINUE_P CONTVIEW CONVERSION_P CONVERT_P CONNECT COORDINATOR COORDINATORS COPY COST CREATE
 CROSS CSN CSV CUBE CURRENT_P
 CURRENT_CATALOG CURRENT_DATE CURRENT_ROLE CURRENT_SCHEMA
 CURRENT_TIME CURRENT_TIMESTAMP CURRENT_USER CURSOR CYCLE
 SHRINK USE_P

 DATA_P DATABASE DATAFILE DATANODE DATANODES DATATYPE_CL DATE_P DATE_FORMAT_P DAY_P DBCOMPATIBILITY_P DEALLOCATE DEC DECIMAL_P DECLARE DECODE DEFAULT DEFAULTS
 DEFERRABLE DEFERRED DEFINER DELETE_P DELIMITER DELIMITERS DELTA DELTAMERGE DESC DETERMINISTIC

 DICTIONARY DIRECT DIRECTORY DISABLE_P DISCARD DISTINCT DISTRIBUTE DISTRIBUTION DO DOCUMENT_P DOMAIN_P DOUBLE_P

 DROP DUPLICATE DISCONNECT DUMPFILE

 EACH ELASTIC ELSE ENABLE_P ENCLOSED ENCODING ENCRYPTED ENCRYPTED_VALUE ENCRYPTION ENCRYPTION_TYPE END_P ENDS ENFORCED ENUM_P ERRORS ESCAPE EOL ESCAPING EVENT EVENTS EVERY EXCEPT EXCHANGE
 EXCLUDE EXCLUDED EXCLUDING EXCLUSIVE EXECUTE EXISTS EXPIRED_P EXPLAIN
 EXTENSION EXTERNAL EXTRACT ESCAPED

 FALSE_P FAMILY FAST FENCED FETCH FIELDS FILEHEADER_P FILL_MISSING_FIELDS FILLER FILTER FIRST_P FIXED_P FLOAT_P FOLLOWING FOLLOWS_P FOR FORCE FOREIGN FORMATTER FORWARD
 FEATURES // DB4AI
 FREEZE FROM FULL FUNCTION FUNCTIONS

 GENERATED GLOBAL GRANT GRANTED GREATEST GROUP_P GROUPING_P GROUPPARENT

 HANDLER HAVING HDFSDIRECTORY HEADER_P HOLD HOUR_P

 IDENTIFIED IDENTITY_P IF_P IGNORE IGNORE_EXTRA_DATA ILIKE IMMEDIATE IMMUTABLE IMPLICIT_P IN_P INCLUDE
 INCLUDING INCREMENT INCREMENTAL INDEX INDEXES INFILE INHERIT INHERITS INITIAL_P INITIALLY INITRANS INLINE_P

 INNER_P INOUT INPUT_P INSENSITIVE INSERT INSTEAD INT_P INTEGER INTERNAL
 INTERSECT INTERVAL INTO INVISIBLE INVOKER IP IS ISNULL ISOLATION

 JOIN

 KEY KILL KEY_PATH KEY_STORE

 LABEL LANGUAGE LARGE_P LAST_P LC_COLLATE_P LC_CTYPE_P LEADING LEAKPROOF LINES
 LEAST LESS LEFT LEVEL LIKE LIMIT LIST LISTEN LOAD LOCAL LOCALTIME LOCALTIMESTAMP
 LOCATION LOCK_P LOCKED LOG_P LOGGING LOGIN_ANY LOGIN_FAILURE LOGIN_SUCCESS LOGOUT LOOP
 MAPPING MASKING MASTER MATCH MATERIALIZED MATCHED MAXEXTENTS MAXSIZE MAXTRANS MAXVALUE MERGE MINUS_P MINUTE_P MINVALUE MINEXTENTS MODE MODIFY_P MONTH_P MOVE MOVEMENT
 MODEL // DB4AI
 NAME_P NAMES NATIONAL NATURAL NCHAR NEXT NO NOCOMPRESS NOCYCLE NODE NOLOGGING NOMAXVALUE NOMINVALUE NONE
 NOT NOTHING NOTIFY NOTNULL NOWAIT NULL_P NULLCOLS NULLIF NULLS_P NUMBER_P NUMERIC NUMSTR NVARCHAR NVARCHAR2 NVL

 OBJECT_P OF OFF OFFSET OIDS ON ONLY OPERATOR OPTIMIZATION OPTION OPTIONALLY OPTIONS OR
 ORDER OUT_P OUTER_P OVER OVERLAPS OVERLAY OWNED OWNER OUTFILE

 PACKAGE PACKAGES PARSER PARTIAL PARTITION PARTITIONS PASSING PASSWORD PCTFREE PER_P PERCENT PERFORMANCE PERM PLACING PLAN PLANS POLICY POSITION

 POOL PRECEDING PRECISION

 PREDICT // DB4AI

 PREFERRED PREFIX PRESERVE PREPARE PREPARED PRIMARY

 PRECEDES_P PRIVATE PRIOR PRIORER PRIVILEGES PRIVILEGE PROCEDURAL PROCEDURE PROFILE PUBLICATION PUBLISH PURGE

 QUERY QUOTE

 RANDOMIZED RANGE RATIO RAW READ REAL REASSIGN REBUILD RECHECK RECURSIVE RECYCLEBIN REDISANYVALUE REF REFERENCES REFRESH REINDEX REJECT_P
 RELATIVE_P RELEASE RELOPTIONS REMOTE_P REMOVE RENAME REPEAT REPEATABLE REPLACE REPLICA
 RESET RESIZE RESOURCE RESTART RESTRICT RETURN RETURNING RETURNS REUSE REVOKE RIGHT ROLE ROLES ROLLBACK ROLLUP
 ROTATION ROW ROWNUM ROWS ROWTYPE_P RULE

 SAMPLE SAVEPOINT SCHEDULE SCHEMA SCROLL SEARCH SECOND_P SECURITY SELECT SEPARATOR_P SEQUENCE SEQUENCES
 SERIALIZABLE SERVER SESSION SESSION_USER SET SETS SETOF SHARE SHIPPABLE SHOW SHUTDOWN SIBLINGS
 SIMILAR SIMPLE SIZE SKIP SLAVE SLICE SMALLDATETIME SMALLDATETIME_FORMAT_P SMALLINT SNAPSHOT SOME SOURCE_P SPACE SPILL SPLIT STABLE STANDALONE_P START STARTS STARTWITH
 STATEMENT STATEMENT_ID STATISTICS STDIN STDOUT STORAGE STORE_P STORED STRATIFY STREAM STRICT_P STRIP_P SUBPARTITION SUBPARTITIONS SUBSCRIPTION SUBSTRING
 SYMMETRIC SYNONYM SYSDATE SYSID SYSTEM_P SYS_REFCURSOR STARTING

 TABLE TABLES TABLESAMPLE TABLESPACE TARGET TEMP TEMPLATE TEMPORARY TERMINATED TEXT_P THAN THEN TIME TIME_FORMAT_P TIMECAPSULE TIMESTAMP TIMESTAMP_FORMAT_P TIMESTAMPDIFF TINYINT
 TO TRAILING TRANSACTION TRANSFORM TREAT TRIGGER TRIM TRUE_P
 TRUNCATE TRUSTED TSFIELD TSTAG TSTIME TYPE_P TYPES_P

 UNBOUNDED UNCOMMITTED UNENCRYPTED UNION UNIQUE UNKNOWN UNLIMITED UNLISTEN UNLOCK UNLOGGED
 UNTIL UNUSABLE UPDATE USEEOF USER USING

 VACUUM VALID VALIDATE VALIDATION VALIDATOR VALUE_P VALUES VARCHAR VARCHAR2 VARIABLES VARIADIC VARRAY VARYING VCGROUP
 VERBOSE VERIFY VERSION_P VIEW VISIBLE VOLATILE

 WAIT WARNINGS WEAK WHEN WHERE WHILE_P WHITESPACE_P WINDOW WITH WITHIN WITHOUT WORK WORKLOAD WRAPPER WRITE

 XML_P XMLATTRIBUTES XMLCONCAT XMLELEMENT XMLEXISTS XMLFOREST XMLPARSE
 XMLPI XMLROOT XMLSERIALIZE

 YEAR_P YES_P

 ZONE






 %token NULLS_FIRST NULLS_LAST WITH_TIME INCLUDING_ALL
 RENAME_PARTITION
 PARTITION_FOR
 SUBPARTITION_FOR
 ADD_PARTITION
 DROP_PARTITION
 REBUILD_PARTITION
 MODIFY_PARTITION
 ADD_SUBPARTITION
 DROP_SUBPARTITION
 NOT_ENFORCED
 VALID_BEGIN
 DECLARE_CURSOR ON_UPDATE_TIME
 START_WITH CONNECT_BY SHOW_ERRORS
 END_OF_INPUT
 END_OF_INPUT_COLON
 END_OF_PROC
 EVENT_TRIGGER
 NOT_IN NOT_BETWEEN NOT_LIKE NOT_ILIKE NOT_SIMILAR
 FORCE_INDEX USE_INDEX


 %nonassoc COMMENT
 %nonassoc FIRST_P AFTER
 %nonassoc PARTIAL_EMPTY_PREC
 %nonassoc CLUSTER
 %nonassoc SET
 %nonassoc AUTO_INCREMENT
 %right PRIOR SEPARATOR_P
 %right FEATURES TARGET // DB4AI
 %left UNION EXCEPT MINUS_P
 %left INTERSECT
 %left OR
 %left AND
 %right NOT
 %right '=' CmpNullOp COLON_EQUALS
 %nonassoc '<' '>' CmpOp
 %nonassoc LIKE ILIKE SIMILAR NOT_LIKE NOT_ILIKE NOT_SIMILAR
 %nonassoc ESCAPE
 %nonassoc OVERLAPS
 %nonassoc BETWEEN NOT_BETWEEN
 %nonassoc IN_P NOT_IN
 %left POSTFIXOP
 %nonassoc lower_than_index
 %nonassoc INDEX

























 %nonassoc UNBOUNDED
 %nonassoc IDENT
%nonassoc CSTRING
%nonassoc UIDENT GENERATED NULL_P PARTITION SUBPARTITION RANGE ROWS PRECEDING FOLLOWING CUBE ROLLUP
 %left Op OPERATOR '@'
 %nonassoc NOTNULL
 %nonassoc ISNULL
 %nonassoc IS
 %left '+' '-'
 %left '*' '/' '%'
 %left '^'

 %left AT
 %left COLLATE
 %right UMINUS BY NAME_P PASSING ROW TYPE_P VALUE_P
 %left '[' ']'
 %left '(' ')'
 %left EMPTY_FROM_CLAUSE
 %right INTO
 %left TYPECAST
 %left '.'







 %left JOIN CROSS LEFT FULL RIGHT INNER_P NATURAL ENCRYPTED

 %right PRESERVE STRIP_P
%%
prog: statements;
/* rules */
 stmt:
 AlterAppWorkloadGroupMappingStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterCoordinatorStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterDatabaseStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterDatabaseSetStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterDataSourceStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterDefaultPrivilegesStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterDomainStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterEnumStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterEventTrigStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterExtensionStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterExtensionContentsStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterFdwStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterForeignServerStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterForeignTableStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterFunctionStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterProcedureStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterPublicationStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterGroupStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterNodeGroupStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterNodeStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterObjectSchemaStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterOwnerStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterRlsPolicyStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterResourcePoolStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterGlobalConfigStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterSeqStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterSchemaStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterSubscriptionStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterTableStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterSystemStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterCompositeTypeStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterRoleSetStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterRoleStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterSessionStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterTSConfigurationStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterTSDictionaryStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterUserMappingStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterUserSetStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterUserStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterWorkloadGroupStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AnalyzeStmt
 { output_statement($1, 0, ECPGst_normal); }
|  BarrierStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateAppWorkloadGroupMappingStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CallFuncStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CheckPointStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CleanConnStmt
 { output_statement($1, 0, ECPGst_normal); }
|  ClosePortalStmt
	{
		if (INFORMIX_MODE)
		{
			if (pg_strcasecmp($1+strlen("close "), "database") == 0)
			{
				if (connection)
					mmerror(PARSE_ERROR, ET_ERROR, "AT option not allowed in CLOSE DATABASE statement");

				fprintf(yyout, "{ ECPGdisconnect(__LINE__, \"CURRENT\");");
				whenever_action(2);
				free_current_memory($1);
				break;
			}
		}

		output_statement($1, 0, ECPGst_normal);
	}
|  ClusterStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CommentStmt
 { output_statement($1, 0, ECPGst_normal); }
|  ConstraintsSetStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CopyStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateAsStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateAssertStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateCastStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateContQueryStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateStreamStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateConversionStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateDomainStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateDirectoryStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateEventTrigStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateExtensionStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateFdwStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateForeignServerStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateForeignTableStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateDataSourceStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateFunctionStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateEventStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterEventStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropEventStmt
 { output_statement($1, 0, ECPGst_normal); }
|  ShowEventStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreatePackageStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreatePackageBodyStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateGroupStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateMatViewStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateModelStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateNodeGroupStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateNodeStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateOpClassStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateOpFamilyStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterOpFamilyStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateRlsPolicyStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreatePLangStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateProcedureStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreatePublicationStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateKeyStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreatePolicyLabelStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateWeakPasswordDictionaryStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropWeakPasswordDictionaryStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterPolicyLabelStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropPolicyLabelStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateAuditPolicyStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterAuditPolicyStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropAuditPolicyStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateMaskingPolicyStmt
 { output_statement($1, 0, ECPGst_normal); }
|  AlterMaskingPolicyStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropMaskingPolicyStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateResourcePoolStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateSchemaStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateSeqStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateSubscriptionStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateSynonymStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateTableSpaceStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateTrigStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateRoleStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateUserStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateUserMappingStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreateWorkloadGroupStmt
 { output_statement($1, 0, ECPGst_normal); }
|  CreatedbStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DeallocateStmt
	{
		output_deallocate_prepare_statement($1);
	}
|  DeclareCursorStmt
	{ output_simple_statement($1); }
|  DefineStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DeleteStmt
	{ output_statement($1, 1, ECPGst_prepnormal); }
|  DiscardStmt
	{ output_statement($1, 1, ECPGst_normal); }
|  DoStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropAppWorkloadGroupMappingStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropAssertStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropCastStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropDataSourceStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropDirectoryStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropFdwStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropForeignServerStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropGroupStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropModelStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropNodeGroupStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropNodeStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropOpClassStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropOpFamilyStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropOwnedStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropRlsPolicyStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropPLangStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropResourcePoolStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropGlobalConfigStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropRuleStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropSubscriptionStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropSynonymStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropTableSpaceStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropTrigStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropRoleStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropUserStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropUserMappingStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropWorkloadGroupStmt
 { output_statement($1, 0, ECPGst_normal); }
|  DropdbStmt
 { output_statement($1, 0, ECPGst_normal); }
|  ExecuteStmt
	{ output_statement($1, 1, ECPGst_execute); }
|  ExecDirectStmt
 { output_statement($1, 0, ECPGst_normal); }
|  ExplainStmt
 { output_statement($1, 0, ECPGst_normal); }
|  FetchStmt
	{ output_statement($1, 1, ECPGst_normal); }
|  GrantStmt
 { output_statement($1, 0, ECPGst_normal); }
|  GrantRoleStmt
 { output_statement($1, 0, ECPGst_normal); }
|  GrantDbStmt
 { output_statement($1, 0, ECPGst_normal); }
|  IndexStmt
 { output_statement($1, 0, ECPGst_normal); }
|  InsertStmt
	{ output_statement($1, 1, ECPGst_prepnormal); }
|  ListenStmt
 { output_statement($1, 0, ECPGst_normal); }
|  RefreshMatViewStmt
 { output_statement($1, 0, ECPGst_normal); }
|  LoadStmt
 { output_statement($1, 0, ECPGst_normal); }
|  LockStmt
 { output_statement($1, 0, ECPGst_normal); }
|  MergeStmt
 { output_statement($1, 0, ECPGst_normal); }
|  NotifyStmt
 { output_statement($1, 0, ECPGst_normal); }
|  PrepareStmt
	{
		if ($1.type == NULL || strlen($1.type) == 0)
			output_prepare_statement($1.name, $1.stmt);
		else
			output_statement(cat_str(5, mm_strdup("prepare"), $1.name, $1.type, mm_strdup("as"), $1.stmt), 0, ECPGst_normal);
	}
|  PurgeStmt
 { output_statement($1, 0, ECPGst_normal); }
|  ReassignOwnedStmt
 { output_statement($1, 0, ECPGst_normal); }
|  ReindexStmt
 { output_statement($1, 0, ECPGst_normal); }
|  RemoveAggrStmt
 { output_statement($1, 0, ECPGst_normal); }
|  RemoveFuncStmt
 { output_statement($1, 0, ECPGst_normal); }
|  RemovePackageStmt
 { output_statement($1, 0, ECPGst_normal); }
|  RemoveOperStmt
 { output_statement($1, 0, ECPGst_normal); }
|  RenameStmt
 { output_statement($1, 0, ECPGst_normal); }
|  RevokeStmt
 { output_statement($1, 0, ECPGst_normal); }
|  RevokeRoleStmt
 { output_statement($1, 0, ECPGst_normal); }
|  RevokeDbStmt
 { output_statement($1, 0, ECPGst_normal); }
|  RuleStmt
 { output_statement($1, 0, ECPGst_normal); }
|  SecLabelStmt
 { output_statement($1, 0, ECPGst_normal); }
|  SelectStmt
	{ output_statement($1, 1, ECPGst_prepnormal); }
|  ShutdownStmt
 { output_statement($1, 0, ECPGst_normal); }
|  TimeCapsuleStmt
 { output_statement($1, 0, ECPGst_normal); }
|  SnapshotStmt
 { output_statement($1, 0, ECPGst_normal); }
|  TransactionStmt
	{
		fprintf(yyout, "{ ECPGtrans(__LINE__, %s, \"%s\");", connection ? connection : "NULL", $1);
		whenever_action(2);
		free_current_memory($1);
	}
|  TruncateStmt
 { output_statement($1, 0, ECPGst_normal); }
|  UnlistenStmt
 { output_statement($1, 0, ECPGst_normal); }
|  UpdateStmt
	{ output_statement($1, 1, ECPGst_prepnormal); }
|  VacuumStmt
 { output_statement($1, 0, ECPGst_normal); }
|  VariableResetStmt
 { output_statement($1, 0, ECPGst_normal); }
|  VariableSetStmt
 { output_statement($1, 0, ECPGst_normal); }
|  VariableMultiSetStmt
 { output_statement($1, 0, ECPGst_normal); }
|  VariableShowStmt
 { output_statement($1, 0, ECPGst_normal); }
|  VerifyStmt
 { output_statement($1, 0, ECPGst_normal); }
|  ViewStmt
 { output_statement($1, 0, ECPGst_normal); }
	| ECPGAllocateDescr
	{
		fprintf(yyout,"ECPGallocate_desc(__LINE__, %s);",$1);
		whenever_action(0);
		free_current_memory($1);
	}
	| ECPGConnect
	{
		if (connection)
			mmerror(PARSE_ERROR, ET_ERROR, "AT option not allowed in CONNECT statement");

		fprintf(yyout, "{ ECPGconnect(__LINE__, %d, %s, %d); ", compat, $1, autocommit);
		reset_variables();
		whenever_action(2);
		free_current_memory($1);
	}
	| ECPGCursorStmt
	{
		output_simple_statement($1);
	}
	| ECPGDeallocateDescr
	{
		fprintf(yyout,"ECPGdeallocate_desc(__LINE__, %s);",$1);
		whenever_action(0);
		free_current_memory($1);
	}
	| ECPGDeclare
	{
		output_simple_statement($1);
	}
	| ECPGDescribe
	{
		fprintf(yyout, "{ ECPGdescribe(__LINE__, %d, %s,", compat, $1);
		dump_variables(argsresult, 1);
		fputs("ECPGt_EORT);", yyout);
		fprintf(yyout, "}");
		output_line_number();

		free_current_memory($1);
	}
	| ECPGDisconnect
	{
		if (connection)
			mmerror(PARSE_ERROR, ET_ERROR, "AT option not allowed in DISCONNECT statement");

		fprintf(yyout, "{ ECPGdisconnect(__LINE__, %s);",
				$1 ? $1 : "\"CURRENT\"");
		whenever_action(2);
		free_current_memory($1);
	}
	| ECPGExecuteImmediateStmt	{ output_statement($1, 0, ECPGst_exec_immediate); }
	| ECPGFree
	{
		const char *con = connection ? connection : "NULL";

		if (strcmp($1, "all") == 0)
			fprintf(yyout, "{ ECPGdeallocate_all(__LINE__, %d, %s);", compat, con);
		else if ($1[0] == ':')
			fprintf(yyout, "{ ECPGdeallocate(__LINE__, %d, %s, %s);", compat, con, $1+1);
		else
			fprintf(yyout, "{ ECPGdeallocate(__LINE__, %d, %s, \"%s\");", compat, con, $1);

		whenever_action(2);
		free_current_memory($1);
	}
	| ECPGGetDescriptor
	{
		lookup_descriptor($1.name, connection);
		output_get_descr($1.name, $1.str);
		free_current_memory($1.name);
		free_current_memory($1.str);
	}
	| ECPGGetDescriptorHeader
	{
		lookup_descriptor($1, connection);
		output_get_descr_header($1);
		free_current_memory($1);
	}
	| ECPGOpen
	{
		struct cursor *ptr;

		if ((ptr = add_additional_variables($1, true)) != NULL)
		{
			connection = ptr->connection ? mm_strdup(ptr->connection) : NULL;
			output_statement(mm_strdup(ptr->command), 0, ECPGst_normal);
			ptr->opened = true;
		}
	}
	| ECPGSetAutocommit
	{
		fprintf(yyout, "{ ECPGsetcommit(__LINE__, \"%s\", %s);", $1, connection ? connection : "NULL");
		whenever_action(2);
		free_current_memory($1);
	}
	| ECPGSetConnection
	{
		if (connection)
			mmerror(PARSE_ERROR, ET_ERROR, "AT option not allowed in SET CONNECTION statement");

		fprintf(yyout, "{ ECPGsetconn(__LINE__, %s);", $1);
		whenever_action(2);
		free_current_memory($1);
	}
	| ECPGSetDescriptor
	{
		lookup_descriptor($1.name, connection);
		output_set_descr($1.name, $1.str);
		free_current_memory($1.name);
		free_current_memory($1.str);
	}
	| ECPGSetDescriptorHeader
	{
		lookup_descriptor($1, connection);
		output_set_descr_header($1);
		free_current_memory($1);
	}
	| ECPGTypedef
	{
		if (connection)
			mmerror(PARSE_ERROR, ET_ERROR, "AT option not allowed in TYPE statement");

		fprintf(yyout, "%s", $1);
		free_current_memory($1);
		output_line_number();
	}
	| ECPGVar
	{
		if (connection)
			mmerror(PARSE_ERROR, ET_ERROR, "AT option not allowed in VAR statement");

		output_simple_statement($1);
	}
	| ECPGWhenever
	{
		if (connection)
			mmerror(PARSE_ERROR, ET_ERROR, "AT option not allowed in WHENEVER statement");

		output_simple_statement($1);
	}
|  ShrinkStmt
 { output_statement($1, 0, ECPGst_normal); }
| 
 { $$ = NULL; }
|  DelimiterStmt
 { output_statement($1, 0, ECPGst_normal); }
;


 CreateRoleStmt:
 CREATE ROLE RoleId opt_with OptRoleList
 { 
 $$ = cat_str(4,mm_strdup("create role"),$3,$4,$5);
}
;


 ShrinkStmt:
 SHRINK TABLE relation_expr_list opt_nowait
 { 
 $$ = cat_str(3,mm_strdup("shrink table"),$3,$4);
}
|  SHRINK INDEX relation_expr_list opt_nowait
 { 
 $$ = cat_str(3,mm_strdup("shrink index"),$3,$4);
}
;


 opt_with:
 WITH
 { 
 $$ = mm_strdup("with");
}
| 
 { 
 $$=EMPTY; }
;


 OptRoleList:
 OptRoleList CreateOptRoleElem
 { 
 $$ = cat_str(2,$1,$2);
}
| 
 { 
 $$=EMPTY; }
;


 AlterOptRoleList:
 AlterOptRoleList AlterOptRoleElem
 { 
 $$ = cat_str(2,$1,$2);
}
| 
 { 
 $$=EMPTY; }
;


 password_string:
 ecpg_sconst
 { 
 $$ = $1;
}
|  ecpg_ident namedata_string:
 ecpg_sconst
 { 
 $$ = $1;
}
|  ecpg_ident
 { 
 $$ = $1;
}
;


 AlterOptRoleElem:
 PASSWORD password_string
 { 
 $$ = cat_str(2,mm_strdup("password"),$2);
}
|  IDENTIFIED BY password_string
 { 
 $$ = cat_str(2,mm_strdup("identified by"),$3);
}
|  ENCRYPTED IDENTIFIED BY password_string
 { 
 $$ = cat_str(2,mm_strdup("encrypted identified by"),$4);
}
|  UNENCRYPTED IDENTIFIED BY password_string
 { 
 $$ = cat_str(2,mm_strdup("unencrypted identified by"),$4);
}
|  IDENTIFIED BY password_string REPLACE password_string
 { 
 $$ = cat_str(4,mm_strdup("identified by"),$3,mm_strdup("replace"),$5);
}
|  ENCRYPTED IDENTIFIED BY password_string REPLACE password_string
 { 
 $$ = cat_str(4,mm_strdup("encrypted identified by"),$4,mm_strdup("replace"),$6);
}
|  UNENCRYPTED IDENTIFIED BY password_string REPLACE password_string
 { 
 $$ = cat_str(4,mm_strdup("unencrypted identified by"),$4,mm_strdup("replace"),$6);
}
|  IDENTIFIED BY DISABLE_P
 { 
 $$ = mm_strdup("identified by disable");
}
|  PASSWORD DISABLE_P
 { 
 $$ = mm_strdup("password disable");
}
|  PASSWORD EXPIRED_P
 { 
 $$ = mm_strdup("password expired");
}
|  PASSWORD password_string EXPIRED_P
 { 
 $$ = cat_str(3,mm_strdup("password"),$2,mm_strdup("expired"));
}
|  IDENTIFIED BY password_string EXPIRED_P
 { 
 $$ = cat_str(3,mm_strdup("identified by"),$3,mm_strdup("expired"));
}
|  ENCRYPTED PASSWORD password_string
 { 
 $$ = cat_str(2,mm_strdup("encrypted password"),$3);
}
|  UNENCRYPTED PASSWORD password_string
 { 
 $$ = cat_str(2,mm_strdup("unencrypted password"),$3);
}
|  DEFAULT TABLESPACE name
 { 
 $$ = cat_str(2,mm_strdup("default tablespace"),$3);
}
|  PROFILE DEFAULT
 { 
 $$ = mm_strdup("profile default");
}
|  PROFILE name
 { 
 $$ = cat_str(2,mm_strdup("profile"),$2);
}
|  INHERIT
 { 
 $$ = mm_strdup("inherit");
}
|  CONNECTION LIMIT SignedIconst
 { 
 $$ = cat_str(2,mm_strdup("connection limit"),$3);
}
|  RESOURCE POOL namedata_string
 { 
 $$ = cat_str(2,mm_strdup("resource pool"),$3);
}
|  NODE GROUP_P pgxcgroup_name
 { 
 $$ = cat_str(2,mm_strdup("node group"),$3);
}
|  USER GROUP_P namedata_string
 { 
 $$ = cat_str(2,mm_strdup("user group"),$3);
}
|  USER GROUP_P DEFAULT
 { 
 $$ = mm_strdup("user group default");
}
|  PERM SPACE ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("perm space"),$3);
}
|  TEMP SPACE ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("temp space"),$3);
}
|  SPILL SPACE ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("spill space"),$3);
}
|  VALID_BEGIN ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("valid_begin"),$2);
}
|  VALID UNTIL ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("valid until"),$3);
}
|  USER name_list
 { 
 $$ = cat_str(2,mm_strdup("user"),$2);
}
|  ecpg_ident
 { 
 $$ = $1;
}
;


 CreateOptRoleElem:
 AlterOptRoleElem
 { 
 $$ = $1;
}
|  SYSID Iconst
 { 
 $$ = cat_str(2,mm_strdup("sysid"),$2);
}
|  ADMIN name_list
 { 
 $$ = cat_str(2,mm_strdup("admin"),$2);
}
|  ROLE name_list
 { 
 $$ = cat_str(2,mm_strdup("role"),$2);
}
|  IN_P ROLE name_list
 { 
 $$ = cat_str(2,mm_strdup("in role"),$3);
}
|  IN_P GROUP_P name_list
 { 
 $$ = cat_str(2,mm_strdup("in group"),$3);
}
;


 UserId:
 SCONST SET_USER_IDENT
 { 
 $$ = mm_strdup("sconst set_user_ident");
}
|  SCONST '@' SCONST
 { 
 $$ = mm_strdup("sconst @ sconst");
}
|  SCONST
 { 
 $$ = mm_strdup("sconst");
}
|  RoleId SET_USER_IDENT
 { 
 $$ = cat_str(2,$1,mm_strdup("set_user_ident"));
}
|  RoleId
 { 
 $$ = $1;
}
;


 UserIdList:
 UserId
 { 
 $$ = $1;
}
|  UserIdList ',' UserId
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 CreateUserStmt:
 CREATE USER UserId opt_with OptRoleList
 { 
 $$ = cat_str(4,mm_strdup("create user"),$3,$4,$5);
}
|  CREATE USER IF_P NOT EXISTS UserId opt_with OptRoleList
 { 
 $$ = cat_str(4,mm_strdup("create user if not exists"),$6,$7,$8);
}
;


 AlterRoleStmt:
 ALTER ROLE RoleId opt_with AlterOptRoleList
 { 
 $$ = cat_str(4,mm_strdup("alter role"),$3,$4,$5);
}
|  ALTER ROLE RoleId opt_with ACCOUNT LOCK_P
 { 
 $$ = cat_str(4,mm_strdup("alter role"),$3,$4,mm_strdup("account lock"));
}
|  ALTER ROLE RoleId opt_with ACCOUNT UNLOCK
 { 
 $$ = cat_str(4,mm_strdup("alter role"),$3,$4,mm_strdup("account unlock"));
}
;


 opt_in_database:

 { 
 $$=EMPTY; }
|  IN_P DATABASE database_name
 { 
 $$ = cat_str(2,mm_strdup("in database"),$3);
}
;


 AlterRoleSetStmt:
 ALTER ROLE RoleId opt_in_database SetResetClause
 { 
 $$ = cat_str(4,mm_strdup("alter role"),$3,$4,$5);
}
;


 AlterUserStmt:
 ALTER USER UserId opt_with AlterOptRoleList
 { 
 $$ = cat_str(4,mm_strdup("alter user"),$3,$4,$5);
}
|  ALTER USER IF_P EXISTS UserId opt_with AlterOptRoleList
 { 
 $$ = cat_str(4,mm_strdup("alter user if exists"),$5,$6,$7);
}
|  ALTER USER UserId opt_with ACCOUNT LOCK_P
 { 
 $$ = cat_str(4,mm_strdup("alter user"),$3,$4,mm_strdup("account lock"));
}
|  ALTER USER UserId opt_with ACCOUNT UNLOCK
 { 
 $$ = cat_str(4,mm_strdup("alter user"),$3,$4,mm_strdup("account unlock"));
}
;


 AlterUserSetStmt:
 ALTER USER UserId opt_in_database SetResetClause
 { 
 $$ = cat_str(4,mm_strdup("alter user"),$3,$4,$5);
}
;


 DropRoleStmt:
 DROP ROLE name_list
 { 
 $$ = cat_str(2,mm_strdup("drop role"),$3);
}
|  DROP ROLE IF_P EXISTS name_list
 { 
 $$ = cat_str(2,mm_strdup("drop role if exists"),$5);
}
;


 DropUserStmt:
 DROP USER UserIdList opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("drop user"),$3,$4);
}
|  DROP USER IF_P EXISTS UserIdList opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("drop user if exists"),$5,$6);
}
;


 CreateGroupStmt:
 CREATE GROUP_P RoleId opt_with OptRoleList
 { 
 $$ = cat_str(4,mm_strdup("create group"),$3,$4,$5);
}
;


 AlterGroupStmt:
 ALTER GROUP_P RoleId add_drop USER name_list
 { 
 $$ = cat_str(5,mm_strdup("alter group"),$3,$4,mm_strdup("user"),$6);
}
;


 add_drop:
 ADD_P
 { 
 $$ = mm_strdup("add");
}
|  DROP
 { 
 $$ = mm_strdup("drop");
}
;


 AlterSessionStmt:
 ALTER SESSION SET set_rest
 { 
 $$ = cat_str(2,mm_strdup("alter session set"),$4);
}
;


 AlterSystemStmt:
 ALTER SYSTEM_P KILL SESSION ecpg_sconst altersys_option
 { 
 $$ = cat_str(3,mm_strdup("alter system kill session"),$5,$6);
}
|  ALTER SYSTEM_P DISCONNECT SESSION ecpg_sconst altersys_option
 { 
 $$ = cat_str(3,mm_strdup("alter system disconnect session"),$5,$6);
}
|  ALTER SYSTEM_P SET generic_set
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("alter system set"),$4);
}
;


 altersys_option:
 IMMEDIATE
 { 
 $$ = mm_strdup("immediate");
}
| 
 { 
 $$=EMPTY; }
;


 DropGroupStmt:
 DROP GROUP_P name_list
 { 
 $$ = cat_str(2,mm_strdup("drop group"),$3);
}
|  DROP GROUP_P IF_P EXISTS name_list
 { 
 $$ = cat_str(2,mm_strdup("drop group if exists"),$5);
}
;


 CreateSchemaStmt:
 CREATE SCHEMA OptSchemaName AUTHORIZATION RoleId OptBlockchainWith OptSchemaEltList
 { 
 $$ = cat_str(6,mm_strdup("create schema"),$3,mm_strdup("authorization"),$5,$6,$7);
}
|  CREATE SCHEMA ColId OptBlockchainWith OptSchemaEltList
 { 
 $$ = cat_str(4,mm_strdup("create schema"),$3,$4,$5);
}
|  CREATE SCHEMA IF_P NOT EXISTS ColId OptBlockchainWith OptSchemaEltList
 { 
 $$ = cat_str(4,mm_strdup("create schema if not exists"),$6,$7,$8);
}
|  CREATE SCHEMA IF_P NOT EXISTS OptSchemaName AUTHORIZATION RoleId OptBlockchainWith OptSchemaEltList
 { 
 $$ = cat_str(6,mm_strdup("create schema if not exists"),$6,mm_strdup("authorization"),$8,$9,$10);
}
|  CREATE SCHEMA ColId CharsetCollate
 { 
 $$ = cat_str(3,mm_strdup("create schema"),$3,$4);
}
;


 OptSchemaName:
 ColId
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 OptSchemaEltList:
 OptSchemaEltList schema_stmt
 { 
 $$ = cat_str(2,$1,$2);
}
| 
 { 
 $$=EMPTY; }
;


 OptBlockchainWith:
 WITH BLOCKCHAIN
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = mm_strdup("with blockchain");
}
| 
 { 
 $$=EMPTY; }
;


 AlterSchemaStmt:
 ALTER SCHEMA ColId OptAlterToBlockchain
 { 
 $$ = cat_str(3,mm_strdup("alter schema"),$3,$4);
}
|  ALTER SCHEMA ColId CharsetCollate
 { 
 $$ = cat_str(3,mm_strdup("alter schema"),$3,$4);
}
;


 OptAlterToBlockchain:
 WITH BLOCKCHAIN
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = mm_strdup("with blockchain");
}
|  WITHOUT BLOCKCHAIN
 { 
 $$ = mm_strdup("without blockchain");
}
;


 schema_stmt:
 CreateStmt
 { 
 $$ = $1;
}
|  IndexStmt
 { 
 $$ = $1;
}
|  CreateSeqStmt
 { 
 $$ = $1;
}
|  CreateTrigStmt
 { 
 $$ = $1;
}
|  GrantStmt
 { 
 $$ = $1;
}
|  ViewStmt
 { 
 $$ = $1;
}
;


 VariableSetStmt:
 SET set_rest
 { 
 $$ = cat_str(2,mm_strdup("set"),$2);
}
|  SET LOCAL set_rest
 { 
 $$ = cat_str(2,mm_strdup("set local"),$3);
}
|  SET SESSION set_rest
 { 
 $$ = cat_str(2,mm_strdup("set session"),$3);
}
|  SET GLOBAL TRANSACTION transaction_mode_list
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("set global transaction"),$4);
}
|  SET generic_set_extension
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("set"),$2);
}
|  SET SESSION generic_set_extension
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("set session"),$3);
}
;


 set_rest:
 TRANSACTION transaction_mode_list
 { 
 $$ = cat_str(2,mm_strdup("transaction"),$2);
}
|  SESSION CHARACTERISTICS AS TRANSACTION transaction_mode_list
 { 
 $$ = cat_str(2,mm_strdup("session characteristics as transaction"),$5);
}
|  set_rest_more
 { 
 $$ = $1;
}
;


 generic_set:
 var_name TO var_list
 { 
 $$ = cat_str(3,$1,mm_strdup("to"),$3);
}
|  var_name '=' var_list
 { 
 $$ = cat_str(3,$1,mm_strdup("="),$3);
}
|  var_name TO DEFAULT
 { 
 $$ = cat_str(2,$1,mm_strdup("to default"));
}
|  var_name '=' DEFAULT
 { 
 $$ = cat_str(2,$1,mm_strdup("= default"));
}
|  CURRENT_SCHEMA TO var_list
 { 
 $$ = cat_str(2,mm_strdup("current_schema to"),$3);
}
|  CURRENT_SCHEMA '=' var_list
 { 
 $$ = cat_str(2,mm_strdup("current_schema ="),$3);
}
|  CURRENT_SCHEMA TO DEFAULT
 { 
 $$ = mm_strdup("current_schema to default");
}
|  CURRENT_SCHEMA '=' DEFAULT
 { 
 $$ = mm_strdup("current_schema = default");
}
;


 set_rest_more:
 generic_set
 { 
 $$ = $1;
}
|  var_name FROM CURRENT_P
 { 
 $$ = cat_str(2,$1,mm_strdup("from current"));
}
|  TIME ZONE zone_value
 { 
 $$ = cat_str(2,mm_strdup("time zone"),$3);
}
|  CATALOG_P ecpg_sconst
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("catalog"),$2);
}
|  SCHEMA ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("schema"),$2);
}
|  NAMES opt_encoding
 { 
 $$ = cat_str(2,mm_strdup("names"),$2);
}
|  ROLE ColId_or_Sconst
 { 
 $$ = cat_str(2,mm_strdup("role"),$2);
}
|  ROLE ColId_or_Sconst PASSWORD password_string
 { 
 $$ = cat_str(4,mm_strdup("role"),$2,mm_strdup("password"),$4);
}
|  SESSION AUTHORIZATION ColId_or_Sconst PASSWORD password_string
 { 
 $$ = cat_str(4,mm_strdup("session authorization"),$3,mm_strdup("password"),$5);
}
|  SESSION AUTHORIZATION DEFAULT
 { 
 $$ = mm_strdup("session authorization default");
}
|  XML_P OPTION document_or_content
 { 
 $$ = cat_str(2,mm_strdup("xml option"),$3);
}
|  TRANSACTION SNAPSHOT ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("transaction snapshot"),$3);
}
;


 VariableMultiSetStmt:
 SET VariableSetElemsList
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("set"),$2);
}
;


 VariableSetElemsList:
 VariableSetElem
 { 
 $$ = $1;
}
|  VariableSetElemsList ',' VariableSetElem
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 VariableSetElem:
 set_session_extension
 { 
 $$ = $1;
}
|  set_global
 { 
 $$ = $1;
}
|  user_defined_single
 { 
 $$ = $1;
}
;


 user_defined_single:
 uservar_name COLON_EQUALS a_expr
 { 
 $$ = cat_str(3,$1,mm_strdup(":="),$3);
}
|  uservar_name '=' a_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("="),$3);
}
;


 set_global:
 set_global_extension generic_set_extension:
 var_name '=' guc_value_extension_list
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,$1,mm_strdup("="),$3);
}
|  CURRENT_SCHEMA '=' guc_value_extension_list
 { 
 $$ = cat_str(2,mm_strdup("current_schema ="),$3);
}
;


 set_session_extension:
 SET_IDENT_SESSION '.' guc_variable_set
 { 
 $$ = cat_str(2,mm_strdup("set_ident_session ."),$3);
}
|  SET_IDENT '=' set_expr
 { 
 $$ = cat_str(2,mm_strdup("set_ident ="),$3);
}
|  SET_IDENT '=' DEFAULT
 { 
 $$ = mm_strdup("set_ident = default");
}
;


 set_global_extension:
 GLOBAL guc_variable_set
 { 
 $$ = cat_str(2,mm_strdup("global"),$2);
}
|  SET_IDENT_GLOBAL '.' guc_variable_set
 { 
 $$ = cat_str(2,mm_strdup("set_ident_global ."),$3);
}
;


 guc_variable_set:
 var_name '=' set_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("="),$3);
}
|  var_name '=' DEFAULT
 { 
 $$ = cat_str(2,$1,mm_strdup("= default"));
}
|  CURRENT_SCHEMA '=' set_expr
 { 
 $$ = cat_str(2,mm_strdup("current_schema ="),$3);
}
|  CURRENT_SCHEMA '=' DEFAULT
 { 
 $$ = mm_strdup("current_schema = default");
}
;


 guc_value_extension_list:
 set_expr_extension
 { 
 $$ = $1;
}
|  guc_value_extension_list ',' set_expr_extension
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 set_ident_expr:
 SET_IDENT
 { 
 $$ = mm_strdup("set_ident");
}
|  SET_IDENT_SESSION '.' ecpg_ident
 { 
 $$ = cat_str(2,mm_strdup("set_ident_session ."),$3);
}
|  SET_IDENT_GLOBAL '.' ecpg_ident
 { 
 $$ = cat_str(2,mm_strdup("set_ident_global ."),$3);
}
;


 set_expr:
 var_value
 { 
 $$ = $1;
}
|  set_expr_extension
 { 
 $$ = $1;
}
;


 set_expr_extension:
 set_ident_expr
 { 
 $$ = $1;
}
|  '(' a_expr ')' opt_indirection
 { 
 $$ = cat_str(4,mm_strdup("("),$2,mm_strdup(")"),$4);
}
|  case_expr
 { 
 $$ = $1;
}
|  func_expr
 { 
 $$ = $1;
}
|  select_with_parens %prec UMINUS
 { 
 $$ = $1;
}
|  select_with_parens indirection
 { 
 $$ = cat_str(2,$1,$2);
}
|  uservar_name %prec UMINUS
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = $1;
}
|  b_expr TYPECAST Typename
 { 
 $$ = cat_str(3,$1,mm_strdup("::"),$3);
}
|  '@' b_expr
 { 
 $$ = cat_str(2,mm_strdup("@"),$2);
}
|  b_expr '+' b_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("+"),$3);
}
|  b_expr '-' b_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("-"),$3);
}
|  b_expr '*' b_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("*"),$3);
}
|  b_expr '/' b_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("/"),$3);
}
|  b_expr '%' b_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("%"),$3);
}
|  b_expr '^' b_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("^"),$3);
}
|  b_expr '<' b_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("<"),$3);
}
|  b_expr '>' b_expr
 { 
 $$ = cat_str(3,$1,mm_strdup(">"),$3);
}
|  b_expr '=' b_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("="),$3);
}
|  b_expr '@' b_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("@"),$3);
}
|  b_expr CmpOp b_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("cmpop"),$3);
}
|  b_expr qual_Op b_expr %prec Op
 { 
 $$ = cat_str(3,$1,$2,$3);
}
|  qual_Op b_expr %prec Op
 { 
 $$ = cat_str(2,$1,$2);
}
|  b_expr qual_Op %prec POSTFIXOP
 { 
 $$ = cat_str(2,$1,$2);
}
|  b_expr IS DISTINCT FROM b_expr %prec IS
 { 
 $$ = cat_str(3,$1,mm_strdup("is distinct from"),$5);
}
|  b_expr IS NOT DISTINCT FROM b_expr %prec IS
 { 
 $$ = cat_str(3,$1,mm_strdup("is not distinct from"),$6);
}
|  b_expr IS OF '(' type_list ')' %prec IS
 { 
 $$ = cat_str(4,$1,mm_strdup("is of ("),$5,mm_strdup(")"));
}
|  b_expr IS NOT OF '(' type_list ')' %prec IS
 { 
 $$ = cat_str(4,$1,mm_strdup("is not of ("),$6,mm_strdup(")"));
}
|  b_expr IS DOCUMENT_P %prec IS
 { 
 $$ = cat_str(2,$1,mm_strdup("is document"));
}
|  b_expr IS NOT DOCUMENT_P %prec IS
 { 
 $$ = cat_str(2,$1,mm_strdup("is not document"));
}
;


 uservar_name:
 SET_USER_IDENT
 { 
 $$ = mm_strdup("set_user_ident");
}
;


 var_name:
ECPGColId
 { 
 $$ = $1;
}
|  var_name '.' ColId
 { 
 $$ = cat_str(3,$1,mm_strdup("."),$3);
}
;


 var_list:
 var_value
 { 
 $$ = $1;
}
|  var_list ',' var_value
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 var_value:
 opt_boolean_or_string
 { 
 $$ = $1;
}
|  NumericOnly
 { 
		if ($1[0] == '$')
		{
			free_current_memory($1);
			$1 = mm_strdup("$0");
		}

 $$ = $1;
}
;


 iso_level:
 READ UNCOMMITTED
 { 
 $$ = mm_strdup("read uncommitted");
}
|  READ COMMITTED
 { 
 $$ = mm_strdup("read committed");
}
|  REPEATABLE READ
 { 
 $$ = mm_strdup("repeatable read");
}
|  SERIALIZABLE
 { 
 $$ = mm_strdup("serializable");
}
;


 opt_boolean_or_string:
 TRUE_P
 { 
 $$ = mm_strdup("true");
}
|  FALSE_P
 { 
 $$ = mm_strdup("false");
}
|  ON
 { 
 $$ = mm_strdup("on");
}
|  ColId_or_Sconst
 { 
 $$ = $1;
}
;


 zone_value:
 ecpg_sconst
 { 
 $$ = $1;
}
|  ecpg_ident
 { 
 $$ = $1;
}
|  ConstInterval ecpg_sconst opt_interval
 { 
 $$ = cat_str(3,$1,$2,$3);
}
|  ConstInterval '(' Iconst ')' ecpg_sconst opt_interval
 { 
 $$ = cat_str(6,$1,mm_strdup("("),$3,mm_strdup(")"),$5,$6);
}
|  NumericOnly
 { 
 $$ = $1;
}
|  DEFAULT
 { 
 $$ = mm_strdup("default");
}
|  LOCAL
 { 
 $$ = mm_strdup("local");
}
;


 opt_encoding:
 ecpg_sconst
 { 
 $$ = $1;
}
|  DEFAULT
 { 
 $$ = mm_strdup("default");
}
| 
 { 
 $$=EMPTY; }
;


 ColId_or_Sconst:
 ColId
 { 
 $$ = $1;
}
|  ecpg_sconst
 { 
 $$ = $1;
}
;


 VariableResetStmt:
 RESET var_name
 { 
 $$ = cat_str(2,mm_strdup("reset"),$2);
}
|  RESET TIME ZONE
 { 
 $$ = mm_strdup("reset time zone");
}
|  RESET CURRENT_SCHEMA
 { 
 $$ = mm_strdup("reset current_schema");
}
|  RESET TRANSACTION ISOLATION LEVEL
 { 
 $$ = mm_strdup("reset transaction isolation level");
}
|  RESET SESSION AUTHORIZATION
 { 
 $$ = mm_strdup("reset session authorization");
}
|  RESET ALL
 { 
 $$ = mm_strdup("reset all");
}
;


 SetResetClause:
 SET set_rest
 { 
 $$ = cat_str(2,mm_strdup("set"),$2);
}
|  VariableResetStmt
 { 
 $$ = $1;
}
;


 FunctionSetResetClause:
 SET set_rest_more
 { 
 $$ = cat_str(2,mm_strdup("set"),$2);
}
|  VariableResetStmt
 { 
 $$ = $1;
}
;


 VariableShowStmt:
 SHOW WARNINGS
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = mm_strdup("show warnings");
}
|  SHOW WARNINGS LIMIT Iconst
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("show warnings limit"),$4);
}
|  SHOW WARNINGS LIMIT Iconst ',' Iconst
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(4,mm_strdup("show warnings limit"),$4,mm_strdup(","),$6);
}
|  SHOW ecpg_ident '(' '*' ')' WARNINGS
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,mm_strdup("show"),$2,mm_strdup("( * ) warnings"));
}
|  SHOW_ERRORS
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = mm_strdup("show_errors");
}
|  SHOW_ERRORS LIMIT Iconst
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("show_errors limit"),$3);
}
|  SHOW_ERRORS LIMIT Iconst ',' Iconst
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(4,mm_strdup("show_errors limit"),$3,mm_strdup(","),$5);
}
|  SHOW ecpg_ident '(' '*' ')' ERRORS
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,mm_strdup("show"),$2,mm_strdup("( * ) errors"));
}
| SHOW var_name ecpg_into
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("show"),$2);
}
|  SHOW CURRENT_SCHEMA
 { 
 $$ = mm_strdup("show current_schema");
}
| SHOW TIME ZONE ecpg_into
 { 
 $$ = mm_strdup("show time zone");
}
| SHOW TRANSACTION ISOLATION LEVEL ecpg_into
 { 
 $$ = mm_strdup("show transaction isolation level");
}
| SHOW SESSION AUTHORIZATION ecpg_into
 { 
 $$ = mm_strdup("show session authorization");
}
|  SHOW VARIABLES LIKE var_name
 { 
 $$ = cat_str(2,mm_strdup("show variables like"),$4);
}
|  SHOW ALL
	{
		mmerror(PARSE_ERROR, ET_ERROR, "SHOW ALL is not implemented");
		$$ = EMPTY;
	}
;


 ConstraintsSetStmt:
 SET CONSTRAINTS constraints_set_list constraints_set_mode
 { 
 $$ = cat_str(3,mm_strdup("set constraints"),$3,$4);
}
;


 constraints_set_list:
 ALL
 { 
 $$ = mm_strdup("all");
}
|  qualified_name_list
 { 
 $$ = $1;
}
;


 constraints_set_mode:
 DEFERRED
 { 
 $$ = mm_strdup("deferred");
}
|  IMMEDIATE
 { 
 $$ = mm_strdup("immediate");
}
;


 ShutdownStmt:
 SHUTDOWN
 { 
 $$ = mm_strdup("shutdown");
}
|  SHUTDOWN var_name
 { 
 $$ = cat_str(2,mm_strdup("shutdown"),$2);
}
;


 CheckPointStmt:
 CHECKPOINT
 { 
 $$ = mm_strdup("checkpoint");
}
;


 DiscardStmt:
 DISCARD ALL
 { 
 $$ = mm_strdup("discard all");
}
|  DISCARD TEMP
 { 
 $$ = mm_strdup("discard temp");
}
|  DISCARD TEMPORARY
 { 
 $$ = mm_strdup("discard temporary");
}
|  DISCARD PLANS
 { 
 $$ = mm_strdup("discard plans");
}
;


 AlterTableStmt:
 ALTER TABLE relation_expr MODIFY_P '(' modify_column_cmds ')'
 { 
 $$ = cat_str(5,mm_strdup("alter table"),$3,mm_strdup("modify ("),$6,mm_strdup(")"));
}
|  ALTER TABLE relation_expr ADD_P '(' add_column_cmds ')'
 { 
 $$ = cat_str(5,mm_strdup("alter table"),$3,mm_strdup("add ("),$6,mm_strdup(")"));
}
|  ALTER TABLE relation_expr REDISANYVALUE
 { 
 $$ = cat_str(3,mm_strdup("alter table"),$3,mm_strdup("redisanyvalue"));
}
|  ALTER TABLE IF_P EXISTS relation_expr MODIFY_P '(' modify_column_cmds ')'
 { 
 $$ = cat_str(5,mm_strdup("alter table if exists"),$5,mm_strdup("modify ("),$8,mm_strdup(")"));
}
|  ALTER TABLE IF_P EXISTS relation_expr ADD_P '(' add_column_cmds ')'
 { 
 $$ = cat_str(5,mm_strdup("alter table if exists"),$5,mm_strdup("add ("),$8,mm_strdup(")"));
}
|  ALTER TABLE relation_expr alter_table_or_partition
 { 
 $$ = cat_str(3,mm_strdup("alter table"),$3,$4);
}
|  ALTER TABLE IF_P EXISTS relation_expr alter_table_or_partition
 { 
 $$ = cat_str(3,mm_strdup("alter table if exists"),$5,$6);
}
|  ALTER INDEX qualified_name alter_table_or_partition
 { 
 $$ = cat_str(3,mm_strdup("alter index"),$3,$4);
}
|  ALTER INDEX IF_P EXISTS qualified_name alter_table_or_partition
 { 
 $$ = cat_str(3,mm_strdup("alter index if exists"),$5,$6);
}
|  ALTER SEQUENCE qualified_name alter_table_cmds
 { 
 $$ = cat_str(3,mm_strdup("alter sequence"),$3,$4);
}
|  ALTER LARGE_P SEQUENCE qualified_name alter_table_cmds
 { 
 $$ = cat_str(3,mm_strdup("alter large sequence"),$4,$5);
}
|  ALTER SEQUENCE IF_P EXISTS qualified_name alter_table_cmds
 { 
 $$ = cat_str(3,mm_strdup("alter sequence if exists"),$5,$6);
}
|  ALTER LARGE_P SEQUENCE IF_P EXISTS qualified_name alter_table_cmds
 { 
 $$ = cat_str(3,mm_strdup("alter large sequence if exists"),$6,$7);
}
|  ALTER VIEW qualified_name alter_table_cmds
 { 
 $$ = cat_str(3,mm_strdup("alter view"),$3,$4);
}
|  ALTER VIEW IF_P EXISTS qualified_name alter_table_cmds
 { 
 $$ = cat_str(3,mm_strdup("alter view if exists"),$5,$6);
}
|  ALTER MATERIALIZED VIEW qualified_name alter_table_cmds
 { 
 $$ = cat_str(3,mm_strdup("alter materialized view"),$4,$5);
}
|  ALTER MATERIALIZED VIEW IF_P EXISTS qualified_name alter_table_cmds
 { 
 $$ = cat_str(3,mm_strdup("alter materialized view if exists"),$6,$7);
}
|  ALTER STREAM qualified_name alter_table_cmds
 { 
 $$ = cat_str(3,mm_strdup("alter stream"),$3,$4);
}
;


 modify_column_cmds:
 modify_column_cmd
 { 
 $$ = $1;
}
|  modify_column_cmds ',' modify_column_cmd
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 modify_column_cmd:
 ColId Typename opt_charset ColQualList opt_column_options add_column_first_after
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(6,$1,$2,$3,$4,$5,$6);
}
|  ColId NOT NULL_P opt_enable
 { 
 $$ = cat_str(3,$1,mm_strdup("not null"),$4);
}
|  ColId NULL_P
 { 
 $$ = cat_str(2,$1,mm_strdup("null"));
}
|  ColId CONSTRAINT name NOT NULL_P opt_enable
 { 
 $$ = cat_str(5,$1,mm_strdup("constraint"),$3,mm_strdup("not null"),$6);
}
|  ColId CONSTRAINT name NULL_P
 { 
 $$ = cat_str(4,$1,mm_strdup("constraint"),$3,mm_strdup("null"));
}
;


 opt_enable:
 ENABLE_P
 { 
 $$ = mm_strdup("enable");
}
| 
 { 
 $$=EMPTY; }
;


 add_column_cmds:
 columnDef
 { 
 $$ = $1;
}
|  add_column_cmds ',' columnDef
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 alter_table_or_partition:
 alter_table_cmds
 { 
 $$ = $1;
}
|  alter_partition_cmds
 { 
 $$ = $1;
}
;


 alter_table_cmds:
 alter_table_cmd
 { 
 $$ = $1;
}
|  alter_table_cmds ',' alter_table_cmd
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 alter_partition_cmds:
 alter_partition_cmd
 { 
 $$ = $1;
}
|  alter_partition_cmds ',' alter_partition_cmd
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
|  move_partition_cmd
 { 
 $$ = $1;
}
|  exchange_partition_cmd
 { 
 $$ = $1;
}
|  reset_partition_cmd
 { 
 $$ = $1;
}
;


 alter_partition_cmd:
 MODIFY_PARTITION ColId UNUSABLE
 { 
 $$ = cat_str(3,mm_strdup("modify partition"),$2,mm_strdup("unusable"));
}
|  MODIFY_PARTITION ColId UNUSABLE LOCAL INDEXES
 { 
 $$ = cat_str(3,mm_strdup("modify partition"),$2,mm_strdup("unusable local indexes"));
}
|  REBUILD_PARTITION ColId
 { 
 $$ = cat_str(2,mm_strdup("rebuild partition"),$2);
}
|  MODIFY_PARTITION ColId REBUILD UNUSABLE LOCAL INDEXES
 { 
 $$ = cat_str(3,mm_strdup("modify partition"),$2,mm_strdup("rebuild unusable local indexes"));
}
|  ADD_PARTITION name VALUES LESS THAN '(' maxValueList ')' opt_part_options
 { 
 $$ = cat_str(6,mm_strdup("add partition"),$2,mm_strdup("values less than ("),$7,mm_strdup(")"),$9);
}
|  ADD_PARTITION name START '(' maxValueList ')' END_P '(' maxValueList ')' opt_range_every_list opt_part_options
 { 
 $$ = cat_str(9,mm_strdup("add partition"),$2,mm_strdup("start ("),$5,mm_strdup(") end ("),$9,mm_strdup(")"),$11,$12);
}
|  ADD_PARTITION name END_P '(' maxValueList ')' opt_part_options
 { 
 $$ = cat_str(6,mm_strdup("add partition"),$2,mm_strdup("end ("),$5,mm_strdup(")"),$7);
}
|  ADD_PARTITION name START '(' maxValueList ')' opt_part_options
 { 
 $$ = cat_str(6,mm_strdup("add partition"),$2,mm_strdup("start ("),$5,mm_strdup(")"),$7);
}
|  ADD_PARTITION name VALUES '(' expr_list ')' opt_part_options
 { 
 $$ = cat_str(6,mm_strdup("add partition"),$2,mm_strdup("values ("),$5,mm_strdup(")"),$7);
}
|  ADD_PARTITION name VALUES '(' DEFAULT ')' opt_part_options
 { 
 $$ = cat_str(4,mm_strdup("add partition"),$2,mm_strdup("values ( default )"),$7);
}
|  ADD_PARTITION name VALUES LESS THAN '(' maxValueList ')' opt_part_options '(' subpartition_definition_list ')'
 { 
 $$ = cat_str(9,mm_strdup("add partition"),$2,mm_strdup("values less than ("),$7,mm_strdup(")"),$9,mm_strdup("("),$11,mm_strdup(")"));
}
|  ADD_PARTITION name VALUES '(' expr_list ')' opt_part_options '(' subpartition_definition_list ')'
 { 
 $$ = cat_str(9,mm_strdup("add partition"),$2,mm_strdup("values ("),$5,mm_strdup(")"),$7,mm_strdup("("),$9,mm_strdup(")"));
}
|  ADD_PARTITION name VALUES '(' DEFAULT ')' OptTableSpace '(' subpartition_definition_list ')'
 { 
 $$ = cat_str(7,mm_strdup("add partition"),$2,mm_strdup("values ( default )"),$7,mm_strdup("("),$9,mm_strdup(")"));
}
|  MODIFY_PARTITION name ADD_SUBPARTITION name VALUES LESS THAN '(' maxValueList ')' OptTableSpace
 { 
 $$ = cat_str(8,mm_strdup("modify partition"),$2,mm_strdup("add subpartition"),$4,mm_strdup("values less than ("),$9,mm_strdup(")"),$11);
}
|  MODIFY_PARTITION name ADD_SUBPARTITION name VALUES '(' expr_list ')' OptTableSpace
 { 
 $$ = cat_str(8,mm_strdup("modify partition"),$2,mm_strdup("add subpartition"),$4,mm_strdup("values ("),$7,mm_strdup(")"),$9);
}
|  MODIFY_PARTITION name ADD_SUBPARTITION name VALUES '(' DEFAULT ')' OptTableSpace
 { 
 $$ = cat_str(6,mm_strdup("modify partition"),$2,mm_strdup("add subpartition"),$4,mm_strdup("values ( default )"),$9);
}
|  DROP_PARTITION ColId OptGPI
 { 
 $$ = cat_str(3,mm_strdup("drop partition"),$2,$3);
}
|  DROP_PARTITION FOR '(' maxValueList ')' OptGPI
 { 
 $$ = cat_str(4,mm_strdup("drop partition for ("),$4,mm_strdup(")"),$6);
}
|  DROP_SUBPARTITION ColId OptGPI
 { 
 $$ = cat_str(3,mm_strdup("drop subpartition"),$2,$3);
}
|  DROP_SUBPARTITION FOR '(' expr_list ')' OptGPI
 { 
 $$ = cat_str(4,mm_strdup("drop subpartition for ("),$4,mm_strdup(")"),$6);
}
|  MERGE PARTITIONS name_list INTO PARTITION name OptTableSpace OptGPI
 { 
 $$ = cat_str(6,mm_strdup("merge partitions"),$3,mm_strdup("into partition"),$6,$7,$8);
}
|  SPLIT PARTITION name AT '(' maxValueList ')' INTO '(' split_dest_partition_define_list ')' OptGPI
 { 
 $$ = cat_str(8,mm_strdup("split partition"),$3,mm_strdup("at ("),$6,mm_strdup(") into ("),$10,mm_strdup(")"),$12);
}
|  SPLIT PARTITION_FOR '(' maxValueList ')' AT '(' maxValueList ')' INTO '(' split_dest_partition_define_list ')' OptGPI
 { 
 $$ = cat_str(8,mm_strdup("split partition for ("),$4,mm_strdup(") at ("),$8,mm_strdup(") into ("),$12,mm_strdup(")"),$14);
}
|  SPLIT PARTITION name INTO '(' range_partition_definition_list ')' OptGPI
 { 
 $$ = cat_str(6,mm_strdup("split partition"),$3,mm_strdup("into ("),$6,mm_strdup(")"),$8);
}
|  SPLIT PARTITION_FOR '(' maxValueList ')' INTO '(' range_partition_definition_list ')' OptGPI
 { 
 $$ = cat_str(6,mm_strdup("split partition for ("),$4,mm_strdup(") into ("),$8,mm_strdup(")"),$10);
}
|  SPLIT SUBPARTITION name VALUES '(' maxValueList ')' INTO '(' split_dest_listsubpartition_define_list ')' OptGPI
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(8,mm_strdup("split subpartition"),$3,mm_strdup("values ("),$6,mm_strdup(") into ("),$10,mm_strdup(")"),$12);
}
|  SPLIT SUBPARTITION name AT '(' maxValueList ')' INTO '(' split_dest_rangesubpartition_define_list ')' OptGPI
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(8,mm_strdup("split subpartition"),$3,mm_strdup("at ("),$6,mm_strdup(") into ("),$10,mm_strdup(")"),$12);
}
|  TRUNCATE PARTITION ColId OptGPI
 { 
 $$ = cat_str(3,mm_strdup("truncate partition"),$3,$4);
}
|  TRUNCATE PARTITION_FOR '(' maxValueList ')' OptGPI
 { 
 $$ = cat_str(4,mm_strdup("truncate partition for ("),$4,mm_strdup(")"),$6);
}
|  TRUNCATE SUBPARTITION ColId OptGPI
 { 
 $$ = cat_str(3,mm_strdup("truncate subpartition"),$3,$4);
}
|  ENABLE_P ROW MOVEMENT
 { 
 $$ = mm_strdup("enable row movement");
}
|  DISABLE_P ROW MOVEMENT
 { 
 $$ = mm_strdup("disable row movement");
}
;


 move_partition_cmd:
 MOVE PARTITION partition_name TABLESPACE name
 { 
 $$ = cat_str(4,mm_strdup("move partition"),$3,mm_strdup("tablespace"),$5);
}
|  MOVE PARTITION_FOR '(' maxValueList ')' TABLESPACE name
 { 
 $$ = cat_str(4,mm_strdup("move partition for ("),$4,mm_strdup(") tablespace"),$7);
}
;


 exchange_partition_cmd:
 EXCHANGE PARTITION '(' ColId ')' WITH TABLE relation_expr opt_verbose OptGPI
 { 
 $$ = cat_str(6,mm_strdup("exchange partition ("),$4,mm_strdup(") with table"),$8,$9,$10);
}
|  EXCHANGE PARTITION '(' ColId ')' WITH TABLE relation_expr WITH VALIDATION opt_verbose OptGPI
 { 
 $$ = cat_str(7,mm_strdup("exchange partition ("),$4,mm_strdup(") with table"),$8,mm_strdup("with validation"),$11,$12);
}
|  EXCHANGE PARTITION '(' ColId ')' WITH TABLE relation_expr WITHOUT VALIDATION OptGPI
 { 
 $$ = cat_str(6,mm_strdup("exchange partition ("),$4,mm_strdup(") with table"),$8,mm_strdup("without validation"),$11);
}
|  EXCHANGE PARTITION_FOR '(' maxValueList ')' WITH TABLE relation_expr opt_verbose OptGPI
 { 
 $$ = cat_str(6,mm_strdup("exchange partition for ("),$4,mm_strdup(") with table"),$8,$9,$10);
}
|  EXCHANGE PARTITION_FOR '(' maxValueList ')' WITH TABLE relation_expr WITH VALIDATION opt_verbose OptGPI
 { 
 $$ = cat_str(7,mm_strdup("exchange partition for ("),$4,mm_strdup(") with table"),$8,mm_strdup("with validation"),$11,$12);
}
|  EXCHANGE PARTITION_FOR '(' maxValueList ')' WITH TABLE relation_expr WITHOUT VALIDATION OptGPI
 { 
 $$ = cat_str(6,mm_strdup("exchange partition for ("),$4,mm_strdup(") with table"),$8,mm_strdup("without validation"),$11);
}
;


 reset_partition_cmd:
 RESET PARTITION
 { 
 $$ = mm_strdup("reset partition");
}
;


 alter_table_cmd:
 UNUSABLE
 { 
 $$ = mm_strdup("unusable");
}
|  ALTER INDEX index_name INVISIBLE
 { 
 $$ = cat_str(3,mm_strdup("alter index"),$3,mm_strdup("invisible"));
}
|  ALTER INDEX index_name VISIBLE
 { 
 $$ = cat_str(3,mm_strdup("alter index"),$3,mm_strdup("visible"));
}
|  REBUILD
 { 
 $$ = mm_strdup("rebuild");
}
|  ADD_P columnDef add_column_first_after
 { 
 $$ = cat_str(3,mm_strdup("add"),$2,$3);
}
|  ADD_P COLUMN columnDef add_column_first_after
 { 
 $$ = cat_str(3,mm_strdup("add column"),$3,$4);
}
|  ADD_P TABLE qualified_name
 { 
 $$ = cat_str(2,mm_strdup("add table"),$3);
}
|  ALTER opt_column ColId alter_column_default
 { 
 $$ = cat_str(4,mm_strdup("alter"),$2,$3,$4);
}
|  ALTER opt_column ColId DROP NOT NULL_P
 { 
 $$ = cat_str(4,mm_strdup("alter"),$2,$3,mm_strdup("drop not null"));
}
|  ALTER opt_column ColId SET NOT NULL_P
 { 
 $$ = cat_str(4,mm_strdup("alter"),$2,$3,mm_strdup("set not null"));
}
|  ALTER opt_column ColId SET STATISTICS SignedIconst
 { 
 $$ = cat_str(5,mm_strdup("alter"),$2,$3,mm_strdup("set statistics"),$6);
}
|  ALTER opt_column ColId SET STATISTICS PERCENT SignedIconst
 { 
 $$ = cat_str(5,mm_strdup("alter"),$2,$3,mm_strdup("set statistics percent"),$7);
}
|  ADD_P STATISTICS '(' opt_multi_name_list ')'
 { 
 $$ = cat_str(3,mm_strdup("add statistics ("),$4,mm_strdup(")"));
}
|  DELETE_P STATISTICS '(' opt_multi_name_list ')'
 { 
 $$ = cat_str(3,mm_strdup("delete statistics ("),$4,mm_strdup(")"));
}
|  ALTER opt_column ColId SET reloptions
 { 
 $$ = cat_str(5,mm_strdup("alter"),$2,$3,mm_strdup("set"),$5);
}
|  ALTER opt_column ColId RESET reloptions
 { 
 $$ = cat_str(5,mm_strdup("alter"),$2,$3,mm_strdup("reset"),$5);
}
|  ALTER opt_column ColId SET STORAGE ColId
 { 
 $$ = cat_str(5,mm_strdup("alter"),$2,$3,mm_strdup("set storage"),$6);
}
|  DROP opt_column IF_P EXISTS ColId opt_drop_behavior
 { 
 $$ = cat_str(5,mm_strdup("drop"),$2,mm_strdup("if exists"),$5,$6);
}
|  DROP opt_column ColId opt_drop_behavior
 { 
 $$ = cat_str(4,mm_strdup("drop"),$2,$3,$4);
}
|  ALTER opt_column ColId opt_set_data TYPE_P Typename opt_collate_clause alter_using
 { 
 $$ = cat_str(8,mm_strdup("alter"),$2,$3,$4,mm_strdup("type"),$6,$7,$8);
}
|  ALTER opt_column ColId alter_generic_options
 { 
 $$ = cat_str(4,mm_strdup("alter"),$2,$3,$4);
}
|  ADD_P TableConstraint
 { 
 $$ = cat_str(2,mm_strdup("add"),$2);
}
|  VALIDATE CONSTRAINT name
 { 
 $$ = cat_str(2,mm_strdup("validate constraint"),$3);
}
|  DROP CONSTRAINT IF_P EXISTS name opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("drop constraint if exists"),$5,$6);
}
|  DROP CONSTRAINT name opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("drop constraint"),$3,$4);
}
|  MODIFY_P modify_column_cmd
 { 
 $$ = cat_str(2,mm_strdup("modify"),$2);
}
|  MODIFY_P COLUMN ColId Typename opt_charset ColQualList opt_column_options add_column_first_after
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(7,mm_strdup("modify column"),$3,$4,$5,$6,$7,$8);
}
|  CHANGE opt_column ColId ColId Typename opt_charset ColQualList opt_column_options add_column_first_after
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(9,mm_strdup("change"),$2,$3,$4,$5,$6,$7,$8,$9);
}
|  SET WITH OIDS
 { 
 $$ = mm_strdup("set with oids");
}
|  SET WITHOUT OIDS
 { 
 $$ = mm_strdup("set without oids");
}
|  CLUSTER ON name
 { 
 $$ = cat_str(2,mm_strdup("cluster on"),$3);
}
|  SET WITHOUT CLUSTER
 { 
 $$ = mm_strdup("set without cluster");
}
|  ENABLE_P TRIGGER name
 { 
 $$ = cat_str(2,mm_strdup("enable trigger"),$3);
}
|  ENABLE_P ALWAYS TRIGGER name
 { 
 $$ = cat_str(2,mm_strdup("enable always trigger"),$4);
}
|  ENABLE_P REPLICA TRIGGER name
 { 
 $$ = cat_str(2,mm_strdup("enable replica trigger"),$4);
}
|  ENABLE_P TRIGGER ALL
 { 
 $$ = mm_strdup("enable trigger all");
}
|  ENABLE_P TRIGGER USER
 { 
 $$ = mm_strdup("enable trigger user");
}
|  DISABLE_P TRIGGER name
 { 
 $$ = cat_str(2,mm_strdup("disable trigger"),$3);
}
|  DISABLE_P TRIGGER ALL
 { 
 $$ = mm_strdup("disable trigger all");
}
|  DISABLE_P TRIGGER USER
 { 
 $$ = mm_strdup("disable trigger user");
}
|  ENABLE_P RULE name
 { 
 $$ = cat_str(2,mm_strdup("enable rule"),$3);
}
|  ENABLE_P ALWAYS RULE name
 { 
 $$ = cat_str(2,mm_strdup("enable always rule"),$4);
}
|  ENABLE_P REPLICA RULE name
 { 
 $$ = cat_str(2,mm_strdup("enable replica rule"),$4);
}
|  DISABLE_P RULE name
 { 
 $$ = cat_str(2,mm_strdup("disable rule"),$3);
}
|  INHERIT qualified_name
 { 
 $$ = cat_str(2,mm_strdup("inherit"),$2);
}
|  NO INHERIT qualified_name
 { 
 $$ = cat_str(2,mm_strdup("no inherit"),$3);
}
|  OF any_name
 { 
 $$ = cat_str(2,mm_strdup("of"),$2);
}
|  NOT OF
 { 
 $$ = mm_strdup("not of");
}
|  OWNER TO UserId
 { 
 $$ = cat_str(2,mm_strdup("owner to"),$3);
}
|  SET TABLESPACE name
 { 
 $$ = cat_str(2,mm_strdup("set tablespace"),$3);
}
|  SET COMPRESS
 { 
 $$ = mm_strdup("set compress");
}
|  SET NOCOMPRESS
 { 
 $$ = mm_strdup("set nocompress");
}
|  SET reloptions
 { 
 $$ = cat_str(2,mm_strdup("set"),$2);
}
|  RESET reloptions
 { 
 $$ = cat_str(2,mm_strdup("reset"),$2);
}
|  REPLICA IDENTITY_P replica_identity
 { 
 $$ = cat_str(2,mm_strdup("replica identity"),$3);
}
|  alter_generic_options
 { 
 $$ = $1;
}
|  OptDistributeByInternal
 { 
 $$ = $1;
}
|  OptSubClusterInternal
 { 
 $$ = $1;
}
|  ADD_P NODE pgxcnodes
 { 
 $$ = cat_str(2,mm_strdup("add node"),$3);
}
|  DELETE_P NODE pgxcnodes
 { 
 $$ = cat_str(2,mm_strdup("delete node"),$3);
}
|  UPDATE SLICE LIKE qualified_name
 { 
 $$ = cat_str(2,mm_strdup("update slice like"),$4);
}
|  ENABLE_P ROW LEVEL SECURITY
 { 
 $$ = mm_strdup("enable row level security");
}
|  DISABLE_P ROW LEVEL SECURITY
 { 
 $$ = mm_strdup("disable row level security");
}
|  FORCE ROW LEVEL SECURITY
 { 
 $$ = mm_strdup("force row level security");
}
|  NO FORCE ROW LEVEL SECURITY
 { 
 $$ = mm_strdup("no force row level security");
}
|  ENCRYPTION KEY ROTATION
 { 
 $$ = mm_strdup("encryption key rotation");
}
|  AutoIncrementValue
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = $1;
}
|  CharsetCollate
 { 
 $$ = $1;
}
|  CONVERT_P TO convert_charset opt_collate
 { 
 $$ = cat_str(3,mm_strdup("convert to"),$3,$4);
}
|  COMMENT opt_equal ecpg_sconst
 { 
 $$ = cat_str(3,mm_strdup("comment"),$2,$3);
}
;


 alter_column_default:
 SET DEFAULT a_expr
 { 
 $$ = cat_str(2,mm_strdup("set default"),$3);
}
|  DROP DEFAULT
 { 
 $$ = mm_strdup("drop default");
}
;


 opt_drop_behavior:
 CASCADE
 { 
 $$ = mm_strdup("cascade");
}
|  RESTRICT
 { 
 $$ = mm_strdup("restrict");
}
|  CASCADE CONSTRAINTS
 { 
 $$ = mm_strdup("cascade constraints");
}
| 
 { 
 $$=EMPTY; }
;


 opt_collate_clause:
 COLLATE collate_name
 { 
 $$ = cat_str(2,mm_strdup("collate"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 alter_using:
 USING a_expr
 { 
 $$ = cat_str(2,mm_strdup("using"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 reloptions:
 '(' reloption_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
;


 replica_identity:
 NOTHING
 { 
 $$ = mm_strdup("nothing");
}
|  FULL
 { 
 $$ = mm_strdup("full");
}
|  DEFAULT
 { 
 $$ = mm_strdup("default");
}
|  USING INDEX name
 { 
 $$ = cat_str(2,mm_strdup("using index"),$3);
}
;


 opt_reloptions:
 WITH reloptions
 { 
 $$ = cat_str(2,mm_strdup("with"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 opt_index_options:

 { 
 $$=EMPTY; }
|  index_options
 { 
 $$ = $1;
}
;


 index_options:
 index_option
 { 
 $$ = $1;
}
|  index_options index_option
 { 
 $$ = cat_str(2,$1,$2);
}
;


 index_option:
 COMMENT opt_equal ecpg_sconst
 { 
 $$ = cat_str(3,mm_strdup("comment"),$2,$3);
}
;


 opt_table_index_options:

 { 
 $$=EMPTY; }
|  table_index_options
 { 
 $$ = $1;
}
;


 table_index_options:
 table_index_option
 { 
 $$ = $1;
}
|  table_index_options table_index_option
 { 
 $$ = cat_str(2,$1,$2);
}
;


 table_index_option:
 COMMENT opt_equal ecpg_sconst
 { 
 $$ = cat_str(3,mm_strdup("comment"),$2,$3);
}
|  USING ecpg_ident
 { 
 $$ = cat_str(2,mm_strdup("using"),$2);
}
|  INVISIBLE
 { 
 $$ = mm_strdup("invisible");
}
|  VISIBLE
 { 
 $$ = mm_strdup("visible");
}
;


 opt_table_options:

 { 
 $$=EMPTY; }
|  table_options
 { 
 $$ = $1;
}
;


 table_options:
 table_option
 { 
 $$ = $1;
}
|  table_options opt_comma table_option
 { 
 $$ = cat_str(3,$1,$2,$3);
}
;


 table_option:
 COMMENT opt_equal ecpg_sconst
 { 
 $$ = cat_str(3,mm_strdup("comment"),$2,$3);
}
;


 opt_comma:

 { 
 $$=EMPTY; }
|  ','
 { 
 $$ = mm_strdup(",");
}
;


 opt_column_options:

 { 
 $$=EMPTY; }
|  column_options
 { 
 $$ = $1;
}
;


 column_options:
 column_option
 { 
 $$ = $1;
}
|  column_options column_option
 { 
 $$ = cat_str(2,$1,$2);
}
;


 column_option:
 COMMENT ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("comment"),$2);
}
;


 opt_part_options:
 OptTableSpace part_options
 { 
 $$ = cat_str(2,$1,$2);
}
|  OptTableSpace
 { 
 $$ = $1;
}
;


 part_options:
 part_option
 { 
 $$ = $1;
}
|  part_options part_option
 { 
 $$ = cat_str(2,$1,$2);
}
;


 part_option:
 COMMENT opt_equal ecpg_sconst
 { 
 $$ = cat_str(3,mm_strdup("comment"),$2,$3);
}
;


 reloption_list:
 reloption_elem
 { 
 $$ = $1;
}
|  reloption_list ',' reloption_elem
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 reloption_elem:
 ColLabel '=' def_arg
 { 
 $$ = cat_str(3,$1,mm_strdup("="),$3);
}
|  ColLabel '=' ROW
 { 
 $$ = cat_str(2,$1,mm_strdup("= row"));
}
|  ColLabel
 { 
 $$ = $1;
}
|  ColLabel '.' ColLabel '=' def_arg
 { 
 $$ = cat_str(5,$1,mm_strdup("."),$3,mm_strdup("="),$5);
}
|  ColLabel '.' ColLabel
 { 
 $$ = cat_str(3,$1,mm_strdup("."),$3);
}
;


 split_dest_partition_define_list:
 PARTITION name OptTableSpace ',' PARTITION name OptTableSpace
 { 
 $$ = cat_str(6,mm_strdup("partition"),$2,$3,mm_strdup(", partition"),$6,$7);
}
;


 split_dest_listsubpartition_define_list:
 SUBPARTITION name OptTableSpace ',' SUBPARTITION name OptTableSpace
 { 
 $$ = cat_str(6,mm_strdup("subpartition"),$2,$3,mm_strdup(", subpartition"),$6,$7);
}
;


 split_dest_rangesubpartition_define_list:
 SUBPARTITION name OptTableSpace ',' SUBPARTITION name OptTableSpace
 { 
 $$ = cat_str(6,mm_strdup("subpartition"),$2,$3,mm_strdup(", subpartition"),$6,$7);
}
;


 AlterCompositeTypeStmt:
 ALTER TYPE_P any_name alter_type_cmds
 { 
 $$ = cat_str(3,mm_strdup("alter type"),$3,$4);
}
;


 alter_type_cmds:
 alter_type_cmd
 { 
 $$ = $1;
}
|  alter_type_cmds ',' alter_type_cmd
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 alter_type_cmd:
 ADD_P ATTRIBUTE TableFuncElement opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("add attribute"),$3,$4);
}
|  DROP ATTRIBUTE IF_P EXISTS ColId opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("drop attribute if exists"),$5,$6);
}
|  DROP ATTRIBUTE ColId opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("drop attribute"),$3,$4);
}
|  ALTER ATTRIBUTE ColId opt_set_data TYPE_P Typename opt_collate_clause opt_drop_behavior
 { 
 $$ = cat_str(7,mm_strdup("alter attribute"),$3,$4,mm_strdup("type"),$6,$7,$8);
}
;


 ClosePortalStmt:
 CLOSE cursor_name
	{
		char *cursor_marker = $2[0] == ':' ? mm_strdup("$0") : $2;
		$$ = cat2_str(mm_strdup("close"), cursor_marker);
	}
|  CLOSE ALL
 { 
 $$ = mm_strdup("close all");
}
;


 CopyStmt:
 COPY opt_binary qualified_name opt_column_list opt_oids copy_from copy_file_name opt_load opt_useeof copy_delimiter opt_noescaping OptCopyLogError OptCopyRejectLimit opt_with copy_options opt_processed
 { 
			if (strcmp($6, "to") == 0 && strcmp($7, "stdin") == 0)
				mmerror(PARSE_ERROR, ET_ERROR, "COPY TO STDIN is not possible");
			else if (strcmp($6, "from") == 0 && strcmp($7, "stdout") == 0)
				mmerror(PARSE_ERROR, ET_ERROR, "COPY FROM STDOUT is not possible");
			else if (strcmp($6, "from") == 0 && strcmp($7, "stdin") == 0)
				mmerror(PARSE_ERROR, ET_WARNING, "COPY FROM STDIN is not implemented");

 $$ = cat_str(16,mm_strdup("copy"),$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16);
}
|  COPY select_with_parens TO copy_file_name opt_noescaping opt_with copy_options opt_processed
 { 
			if (strcmp($4, "stdin") == 0)
				mmerror(PARSE_ERROR, ET_ERROR, "COPY TO STDIN is not possible");

 $$ = cat_str(8,mm_strdup("copy"),$2,mm_strdup("to"),$4,$5,$6,$7,$8);
}
;


 opt_processed:
 ENCRYPTED
 { 
 $$ = mm_strdup("encrypted");
}
| 
 { 
 $$=EMPTY; }
;


 opt_load:
 LOAD
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = mm_strdup("load");
}
| 
 { 
 $$=EMPTY; }
;


 opt_useeof:
 USEEOF
 { 
 $$ = mm_strdup("useeof");
}
| 
 { 
 $$=EMPTY; }
;


 copy_from:
 FROM
 { 
 $$ = mm_strdup("from");
}
|  TO
 { 
 $$ = mm_strdup("to");
}
;


 copy_file_name:
 ecpg_sconst
 { 
 $$ = $1;
}
|  STDIN
 { 
 $$ = mm_strdup("stdin");
}
|  STDOUT
 { 
 $$ = mm_strdup("stdout");
}
|  REDISANYVALUE
 { 
 $$ = mm_strdup("redisanyvalue");
}
;


 copy_options:
 copy_opt_list
 { 
 $$ = $1;
}
|  '(' copy_generic_opt_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
;


 copy_opt_list:
 copy_opt_list copy_opt_item
 { 
 $$ = cat_str(2,$1,$2);
}
| 
 { 
 $$=EMPTY; }
;


 copy_opt_item:
 BINARY
 { 
 $$ = mm_strdup("binary");
}
|  OIDS
 { 
 $$ = mm_strdup("oids");
}
|  FREEZE
 { 
 $$ = mm_strdup("freeze");
}
|  DELIMITER opt_as ecpg_sconst
 { 
 $$ = cat_str(3,mm_strdup("delimiter"),$2,$3);
}
|  NULL_P opt_as ecpg_sconst
 { 
 $$ = cat_str(3,mm_strdup("null"),$2,$3);
}
|  CSV
 { 
 $$ = mm_strdup("csv");
}
|  FIXED_P
 { 
 $$ = mm_strdup("fixed");
}
|  HEADER_P
 { 
 $$ = mm_strdup("header");
}
|  QUOTE opt_as ecpg_sconst
 { 
 $$ = cat_str(3,mm_strdup("quote"),$2,$3);
}
|  ESCAPE opt_as ecpg_sconst
 { 
 $$ = cat_str(3,mm_strdup("escape"),$2,$3);
}
|  FORCE QUOTE columnList
 { 
 $$ = cat_str(2,mm_strdup("force quote"),$3);
}
|  FORCE QUOTE '*'
 { 
 $$ = mm_strdup("force quote *");
}
|  FORCE NOT NULL_P columnList
 { 
 $$ = cat_str(2,mm_strdup("force not null"),$4);
}
|  ENCODING ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("encoding"),$2);
}
|  EOL ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("eol"),$2);
}
|  FILEHEADER_P ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("fileheader"),$2);
}
|  FORMATTER '(' copy_foramtter_opt ')'
 { 
 $$ = cat_str(3,mm_strdup("formatter ("),$3,mm_strdup(")"));
}
|  IGNORE_EXTRA_DATA
 { 
 $$ = mm_strdup("ignore_extra_data");
}
|  DATE_FORMAT_P ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("date_format"),$2);
}
|  TIME_FORMAT_P ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("time_format"),$2);
}
|  TIMESTAMP_FORMAT_P ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("timestamp_format"),$2);
}
|  SMALLDATETIME_FORMAT_P ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("smalldatetime_format"),$2);
}
|  COMPATIBLE_ILLEGAL_CHARS
 { 
 $$ = mm_strdup("compatible_illegal_chars");
}
|  FILL_MISSING_FIELDS
 { 
 $$ = mm_strdup("fill_missing_fields");
}
|  FILL_MISSING_FIELDS ecpg_sconst
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("fill_missing_fields"),$2);
}
|  TRANSFORM '(' copy_column_expr_list ')'
 { 
 $$ = cat_str(3,mm_strdup("transform ("),$3,mm_strdup(")"));
}
|  load_when_option
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = $1;
}
|  SKIP Iconst
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("skip"),$2);
}
|  SEQUENCE '(' copy_column_sequence_list ')'
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,mm_strdup("sequence ("),$3,mm_strdup(")"));
}
|  FILLER '(' copy_column_filler_list ')'
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,mm_strdup("filler ("),$3,mm_strdup(")"));
}
|  CONSTANT '(' copy_column_constant_list ')'
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,mm_strdup("constant ("),$3,mm_strdup(")"));
}
;


 opt_binary:
 BINARY
 { 
 $$ = mm_strdup("binary");
}
| 
 { 
 $$=EMPTY; }
;


 opt_oids:
 WITH OIDS
 { 
 $$ = mm_strdup("with oids");
}
| 
 { 
 $$=EMPTY; }
;


 copy_delimiter:
 opt_using DELIMITERS ecpg_sconst
 { 
 $$ = cat_str(3,$1,mm_strdup("delimiters"),$3);
}
| 
 { 
 $$=EMPTY; }
;


 opt_using:
 USING
 { 
 $$ = mm_strdup("using");
}
| 
 { 
 $$=EMPTY; }
;


 opt_noescaping:
 WITHOUT ESCAPING
 { 
 $$ = mm_strdup("without escaping");
}
| 
 { 
 $$=EMPTY; }
;


 OptCopyLogError:
 LOG_P ERRORS DATA_P
 { 
 $$ = mm_strdup("log errors data");
}
|  LOG_P ERRORS
 { 
 $$ = mm_strdup("log errors");
}
| 
 { 
 $$=EMPTY; }
;


 OptCopyRejectLimit:
 REJECT_P LIMIT ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("reject limit"),$3);
}
| 
 { 
 $$=EMPTY; }
;


 copy_generic_opt_list:
 copy_generic_opt_elem
 { 
 $$ = $1;
}
|  copy_generic_opt_list ',' copy_generic_opt_elem
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 copy_generic_opt_elem:
 ColLabel copy_generic_opt_arg
 { 
 $$ = cat_str(2,$1,$2);
}
;


 copy_generic_opt_arg:
 opt_boolean_or_string
 { 
 $$ = $1;
}
|  NumericOnly
 { 
 $$ = $1;
}
|  '*'
 { 
 $$ = mm_strdup("*");
}
|  '(' copy_generic_opt_arg_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 copy_generic_opt_arg_list:
 copy_generic_opt_arg_list_item
 { 
 $$ = $1;
}
|  copy_generic_opt_arg_list ',' copy_generic_opt_arg_list_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 copy_generic_opt_arg_list_item:
 opt_boolean_or_string
 { 
 $$ = $1;
}
;


 copy_foramtter_opt:
 copy_col_format_def
 { 
 $$ = $1;
}
|  copy_foramtter_opt ',' copy_col_format_def
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 copy_col_format_def:
 ColId '(' Iconst ',' Iconst ')'
 { 
 $$ = cat_str(6,$1,mm_strdup("("),$3,mm_strdup(","),$5,mm_strdup(")"));
}
;


 copy_column_expr_list:
 copy_column_expr_item
 { 
 $$ = $1;
}
|  copy_column_expr_list ',' copy_column_expr_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 copy_column_expr_item:
 ColId OptCopyColTypename OptCopyColExpr
 { 
 $$ = cat_str(3,$1,$2,$3);
}
;


 OptCopyColTypename:
 Typename
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 OptCopyColExpr:
 AS b_expr
 { 
 $$ = cat_str(2,mm_strdup("as"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 copy_column_sequence_list:
 copy_column_sequence_item
 { 
 $$ = $1;
}
|  copy_column_sequence_list ',' copy_column_sequence_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 copy_column_sequence_item:
 ColId '(' column_sequence_item_sart column_sequence_item_step ')'
 { 
 $$ = cat_str(5,$1,mm_strdup("("),$3,$4,mm_strdup(")"));
}
;


 column_sequence_item_step:
 ',' Iconst
 { 
 $$ = cat_str(2,mm_strdup(","),$2);
}
|  ',' ecpg_fconst
 { 
 $$ = cat_str(2,mm_strdup(","),$2);
}
| 
 { 
 $$=EMPTY; }
;


 column_sequence_item_sart:
 Iconst
 { 
 $$ = $1;
}
|  ecpg_fconst
 { 
 $$ = $1;
}
;


 copy_column_filler_list:
 copy_column_filler_item
 { 
 $$ = $1;
}
|  copy_column_filler_list ',' copy_column_filler_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 copy_column_filler_item:
 ColId
 { 
 $$ = $1;
}
;


 copy_column_constant_list:
 copy_column_constant_item
 { 
 $$ = $1;
}
|  copy_column_constant_list ',' copy_column_constant_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 copy_column_constant_item:
 ColId ecpg_sconst
 { 
 $$ = cat_str(2,$1,$2);
}
;


 CreateStreamStmt:
 CREATE STREAM qualified_name '(' OptTableElementList ')'
 { 
 $$ = cat_str(5,mm_strdup("create stream"),$3,mm_strdup("("),$5,mm_strdup(")"));
}
|  CREATE STREAM IF_P NOT EXISTS qualified_name '(' OptTableElementList ')'
 { 
 $$ = cat_str(5,mm_strdup("create stream if not exists"),$6,mm_strdup("("),$8,mm_strdup(")"));
}
;


 PurgeStmt:
 PURGE TABLE qualified_name
 { 
 $$ = cat_str(2,mm_strdup("purge table"),$3);
}
|  PURGE INDEX qualified_name
 { 
 $$ = cat_str(2,mm_strdup("purge index"),$3);
}
|  PURGE TABLESPACE name
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("purge tablespace"),$3);
}
|  PURGE RECYCLEBIN
 { 
 $$ = mm_strdup("purge recyclebin");
}
;


 TimeCapsuleStmt:
 TIMECAPSULE TABLE qualified_name TO opt_timecapsule_clause
 { 
 $$ = cat_str(4,mm_strdup("timecapsule table"),$3,mm_strdup("to"),$5);
}
|  TIMECAPSULE TABLE qualified_name TO BEFORE DROP opt_rename
 { 
 $$ = cat_str(4,mm_strdup("timecapsule table"),$3,mm_strdup("to before drop"),$7);
}
|  TIMECAPSULE TABLE qualified_name TO BEFORE TRUNCATE
 { 
 $$ = cat_str(3,mm_strdup("timecapsule table"),$3,mm_strdup("to before truncate"));
}
;


 opt_rename:
 RENAME TO name
 { 
 $$ = cat_str(2,mm_strdup("rename to"),$3);
}
| 
 { 
 $$=EMPTY; }
;


 CreateStmt:
 CREATE OptTemp TABLE qualified_name '(' OptTableElementList ')' OptInherit OptAutoIncrement optCharsetCollate OptWith OnCommitOption OptCompress OptPartitionElement OptDistributeBy OptSubCluster opt_table_options opt_table_partitioning_clause opt_internal_data OptKind
 { 
 $$ = cat_str(20,mm_strdup("create"),$2,mm_strdup("table"),$4,mm_strdup("("),$6,mm_strdup(")"),$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20);
}
|  CREATE OptTemp TABLE IF_P NOT EXISTS qualified_name '(' OptTableElementList ')' OptInherit OptAutoIncrement optCharsetCollate OptWith OnCommitOption OptCompress OptPartitionElement OptDistributeBy OptSubCluster opt_table_options opt_table_partitioning_clause opt_internal_data
 { 
 $$ = cat_str(19,mm_strdup("create"),$2,mm_strdup("table if not exists"),$7,mm_strdup("("),$9,mm_strdup(")"),$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22);
}
|  CREATE OptTemp TABLE qualified_name OF any_name OptTypedTableElementList OptWith OnCommitOption OptCompress OptPartitionElement OptDistributeBy OptSubCluster opt_table_options
 { 
 $$ = cat_str(14,mm_strdup("create"),$2,mm_strdup("table"),$4,mm_strdup("of"),$6,$7,$8,$9,$10,$11,$12,$13,$14);
}
|  CREATE OptTemp TABLE IF_P NOT EXISTS qualified_name OF any_name OptTypedTableElementList OptWith OnCommitOption OptCompress OptPartitionElement OptDistributeBy OptSubCluster opt_table_options
 { 
 $$ = cat_str(14,mm_strdup("create"),$2,mm_strdup("table if not exists"),$7,mm_strdup("of"),$9,$10,$11,$12,$13,$14,$15,$16,$17);
}
;


 OptKind:
 FOR MATERIALIZED VIEW
 { 
 $$ = mm_strdup("for materialized view");
}
| 
 { 
 $$=EMPTY; }
;


 opt_table_partitioning_clause:
 range_partitioning_clause
 { 
 $$ = $1;
}
|  hash_partitioning_clause
 { 
 $$ = $1;
}
|  list_partitioning_clause
 { 
 $$ = $1;
}
|  value_partitioning_clause
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 range_partitioning_clause:
 PARTITION BY RANGE opt_columns '(' column_item_list ')' opt_interval_partition_clause opt_partitions_num subpartitioning_clause '(' range_partition_definition_list ')' opt_row_movement_clause
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(12,mm_strdup("partition by range"),$4,mm_strdup("("),$6,mm_strdup(")"),$8,$9,$10,mm_strdup("("),$12,mm_strdup(")"),$14);
}
;


 list_partitioning_clause:
 PARTITION BY LIST opt_columns '(' column_item_list ')' opt_partitions_num subpartitioning_clause '(' list_partition_definition_list ')' opt_row_movement_clause
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(11,mm_strdup("partition by list"),$4,mm_strdup("("),$6,mm_strdup(")"),$8,$9,mm_strdup("("),$11,mm_strdup(")"),$13);
}
;


 hash_partitioning_clause:
 PARTITION BY ecpg_ident '(' column_item_list ')' opt_partitions_num subpartitioning_clause opt_hash_partition_definition_list opt_row_movement_clause
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(9,mm_strdup("partition by"),$3,mm_strdup("("),$5,mm_strdup(")"),$7,$8,$9,$10);
}
|  PARTITION BY KEY '(' column_item_list ')' opt_partitions_num subpartitioning_clause opt_hash_partition_definition_list opt_row_movement_clause
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(7,mm_strdup("partition by key ("),$5,mm_strdup(")"),$7,$8,$9,$10);
}
;


 opt_columns:
 COLUMNS
 { 
 $$ = mm_strdup("columns");
}
| 
 { 
 $$=EMPTY; }
;


 opt_partitions_num:
 PARTITIONS Iconst
 { 
 $$ = cat_str(2,mm_strdup("partitions"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 opt_subpartitions_num:
 SUBPARTITIONS Iconst
 { 
 $$ = cat_str(2,mm_strdup("subpartitions"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 opt_hash_partition_definition_list:
 '(' hash_partition_definition_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 value_partitioning_clause:
 PARTITION BY VALUES '(' column_item_list ')'
 { 
 $$ = cat_str(3,mm_strdup("partition by values ("),$5,mm_strdup(")"));
}
;


 subpartitioning_clause:
 range_subpartitioning_clause
 { 
 $$ = $1;
}
|  hash_subpartitioning_clause
 { 
 $$ = $1;
}
|  list_subpartitioning_clause
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 range_subpartitioning_clause:
 SUBPARTITION BY RANGE '(' column_item_list ')'
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,mm_strdup("subpartition by range ("),$5,mm_strdup(")"));
}
;


 list_subpartitioning_clause:
 SUBPARTITION BY LIST '(' column_item_list ')'
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,mm_strdup("subpartition by list ("),$5,mm_strdup(")"));
}
;


 hash_subpartitioning_clause:
 SUBPARTITION BY ecpg_ident '(' column_item_list ')' opt_subpartitions_num
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(6,mm_strdup("subpartition by"),$3,mm_strdup("("),$5,mm_strdup(")"),$7);
}
|  SUBPARTITION BY KEY '(' column_item_list ')' opt_subpartitions_num
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(4,mm_strdup("subpartition by key ("),$5,mm_strdup(")"),$7);
}
;


 subpartition_definition_list:
 subpartition_item
 { 
 $$ = $1;
}
|  subpartition_definition_list ',' subpartition_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 subpartition_item:
 SUBPARTITION name VALUES '(' listValueList ')' opt_part_options
 { 
 $$ = cat_str(6,mm_strdup("subpartition"),$2,mm_strdup("values ("),$5,mm_strdup(")"),$7);
}
|  SUBPARTITION name opt_part_options
 { 
 $$ = cat_str(3,mm_strdup("subpartition"),$2,$3);
}
|  SUBPARTITION name VALUES LESS THAN '(' maxValueList ')' opt_part_options
 { 
 $$ = cat_str(6,mm_strdup("subpartition"),$2,mm_strdup("values less than ("),$7,mm_strdup(")"),$9);
}
;


 column_item_list:
 column_item
 { 
 $$ = $1;
}
|  column_item_list ',' column_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 column_item:
 a_expr
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = $1;
}
;


 opt_interval_partition_clause:
 INTERVAL '(' interval_expr ')' opt_interval_tablespaceList
 { 
 $$ = cat_str(4,mm_strdup("interval ("),$3,mm_strdup(")"),$5);
}
| 
 { 
 $$=EMPTY; }
;


 opt_interval_tablespaceList:
 STORE_P IN_P '(' tablespaceList ')'
 { 
 $$ = cat_str(3,mm_strdup("store in ("),$4,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
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


 range_partition_definition_list:
 range_less_than_list
 { 
 $$ = $1;
}
|  range_start_end_list
 { 
 $$ = $1;
}
;


 list_partition_definition_list:
 list_partition_item
 { 
 $$ = $1;
}
|  list_partition_definition_list ',' list_partition_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 hash_partition_definition_list:
 hash_partition_item
 { 
 $$ = $1;
}
|  hash_partition_definition_list ',' hash_partition_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 range_less_than_list:
 range_less_than_item
 { 
 $$ = $1;
}
|  range_less_than_list ',' range_less_than_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 list_partition_item:
 PARTITION name VALUES opt_in_p '(' listValueList ')' opt_part_options
 { 
 $$ = cat_str(8,mm_strdup("partition"),$2,mm_strdup("values"),$4,mm_strdup("("),$6,mm_strdup(")"),$8);
}
|  PARTITION name VALUES opt_in_p '(' listValueList ')' opt_part_options '(' subpartition_definition_list ')'
 { 
 $$ = cat_str(11,mm_strdup("partition"),$2,mm_strdup("values"),$4,mm_strdup("("),$6,mm_strdup(")"),$8,mm_strdup("("),$10,mm_strdup(")"));
}
;


 opt_in_p:
 IN_P
 { 
 $$ = mm_strdup("in");
}
| 
 { 
 $$=EMPTY; }
;


 hash_partition_item:
 PARTITION name opt_part_options
 { 
 $$ = cat_str(3,mm_strdup("partition"),$2,$3);
}
|  PARTITION name opt_part_options '(' subpartition_definition_list ')'
 { 
 $$ = cat_str(6,mm_strdup("partition"),$2,$3,mm_strdup("("),$5,mm_strdup(")"));
}
;


 range_partition_boundary:
 MAXVALUE
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = mm_strdup("maxvalue");
}
|  '(' maxValueList ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
;


 range_less_than_item:
 PARTITION name VALUES LESS THAN range_partition_boundary opt_part_options
 { 
 $$ = cat_str(5,mm_strdup("partition"),$2,mm_strdup("values less than"),$6,$7);
}
|  PARTITION name VALUES LESS THAN range_partition_boundary opt_part_options '(' subpartition_definition_list ')'
 { 
 $$ = cat_str(8,mm_strdup("partition"),$2,mm_strdup("values less than"),$6,$7,mm_strdup("("),$9,mm_strdup(")"));
}
;


 range_start_end_list:
 range_start_end_item
 { 
 $$ = $1;
}
|  range_start_end_list ',' range_start_end_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 range_start_end_item:
 PARTITION name START '(' maxValueList ')' END_P '(' maxValueList ')' opt_range_every_list opt_part_options
 { 
 $$ = cat_str(9,mm_strdup("partition"),$2,mm_strdup("start ("),$5,mm_strdup(") end ("),$9,mm_strdup(")"),$11,$12);
}
|  PARTITION name END_P '(' maxValueList ')' opt_part_options
 { 
 $$ = cat_str(6,mm_strdup("partition"),$2,mm_strdup("end ("),$5,mm_strdup(")"),$7);
}
|  PARTITION name START '(' maxValueList ')' opt_part_options
 { 
 $$ = cat_str(6,mm_strdup("partition"),$2,mm_strdup("start ("),$5,mm_strdup(")"),$7);
}
;


 opt_range_every_list:
 EVERY '(' maxValueList ')'
 { 
 $$ = cat_str(3,mm_strdup("every ("),$3,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 partition_name:
 ColId
 { 
 $$ = $1;
}
;


 maxValueList:
 maxValueItem
 { 
 $$ = $1;
}
|  maxValueList ',' maxValueItem
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 maxValueItem:
 a_expr
 { 
 $$ = $1;
}
|  MAXVALUE
 { 
 $$ = mm_strdup("maxvalue");
}
;


 listValueList:
 expr_list
 { 
 $$ = $1;
}
|  DEFAULT
 { 
 $$ = mm_strdup("default");
}
;


 opt_row_movement_clause:
 ENABLE_P ROW MOVEMENT
 { 
 $$ = mm_strdup("enable row movement");
}
|  DISABLE_P ROW MOVEMENT
 { 
 $$ = mm_strdup("disable row movement");
}
| 
 { 
 $$=EMPTY; }
;


 OptTemp:
 TEMPORARY
 { 
 $$ = mm_strdup("temporary");
}
|  TEMP
 { 
 $$ = mm_strdup("temp");
}
|  LOCAL TEMPORARY
 { 
 $$ = mm_strdup("local temporary");
}
|  LOCAL TEMP
 { 
 $$ = mm_strdup("local temp");
}
|  GLOBAL TEMPORARY
 { 
 $$ = mm_strdup("global temporary");
}
|  GLOBAL TEMP
 { 
 $$ = mm_strdup("global temp");
}
|  UNLOGGED
 { 
 $$ = mm_strdup("unlogged");
}
| 
 { 
 $$=EMPTY; }
;


 OptTableElementList:
 TableElementList
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 OptTypedTableElementList:
 '(' TypedTableElementList ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 TableElementList:
 TableElement
 { 
 $$ = $1;
}
|  TableElementList ',' TableElement
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 TypedTableElementList:
 TypedTableElement
 { 
 $$ = $1;
}
|  TypedTableElementList ',' TypedTableElement
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 TableElement:
 columnDef
 { 
 $$ = $1;
}
|  TableLikeClause
 { 
 $$ = $1;
}
|  TableConstraint
 { 
 $$ = $1;
}
;


 TypedTableElement:
 columnOptions
 { 
 $$ = $1;
}
|  TableConstraint
 { 
 $$ = $1;
}
;


 columnDef:
 ColId Typename opt_charset KVType ColCmprsMode create_generic_options ColQualList opt_column_options
 { 
 $$ = cat_str(8,$1,$2,$3,$4,$5,$6,$7,$8);
}
;


 add_column_first_after:
 FIRST_P
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = mm_strdup("first");
}
|  AFTER ColId
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("after"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 KVType:
 TSTAG
 { 
 $$ = mm_strdup("tstag");
}
|  TSFIELD
 { 
 $$ = mm_strdup("tsfield");
}
|  TSTIME
 { 
 $$ = mm_strdup("tstime");
}
| 
 { 
 $$=EMPTY; }
;


 ColCmprsMode:
 DELTA
 { 
 $$ = mm_strdup("delta");
}
|  PREFIX
 { 
 $$ = mm_strdup("prefix");
}
|  DICTIONARY
 { 
 $$ = mm_strdup("dictionary");
}
|  NUMSTR
 { 
 $$ = mm_strdup("numstr");
}
|  NOCOMPRESS
 { 
 $$ = mm_strdup("nocompress");
}
| 
 { 
 $$=EMPTY; }
;


 columnOptions:
 ColId WithOptions ColQualList
 { 
 $$ = cat_str(3,$1,$2,$3);
}
;


 WithOptions:
 WITH OPTIONS
 { 
 $$ = mm_strdup("with options");
}
| 
 { 
 $$=EMPTY; }
;


 ColQualList:
 ColQualList ColConstraint
 { 
 $$ = cat_str(2,$1,$2);
}
| 
 { 
 $$=EMPTY; }
;


 ColConstraint:
 CONSTRAINT name ColConstraintElem
 { 
 $$ = cat_str(3,mm_strdup("constraint"),$2,$3);
}
|  ColConstraintElem
 { 
 $$ = $1;
}
|  ConstraintAttr
 { 
 $$ = $1;
}
|  COLLATE collate_name
 { 
 $$ = cat_str(2,mm_strdup("collate"),$2);
}
|  ENCRYPTED with_algorithm
 { 
 $$ = cat_str(2,mm_strdup("encrypted"),$2);
}
|  AUTO_INCREMENT
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = mm_strdup("auto_increment");
}
;


 with_algorithm:
 WITH '(' algorithm_desc ')'
 { 
 $$ = cat_str(3,mm_strdup("with ("),$3,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 algorithm_desc:
 datatypecl columnEncryptionKey ',' encryptionType
 { 
 $$ = cat_str(4,$1,$2,mm_strdup(","),$4);
}
|  datatypecl encryptionType ',' columnEncryptionKey
 { 
 $$ = cat_str(4,$1,$2,mm_strdup(","),$4);
}
;


 columnEncryptionKey:
 COLUMN_ENCRYPTION_KEY '=' setting_name
 { 
 $$ = cat_str(2,mm_strdup("column_encryption_key ="),$3);
}
;


 encryptionType:
 ENCRYPTION_TYPE '=' RANDOMIZED
 { 
 $$ = mm_strdup("encryption_type = randomized");
}
|  ENCRYPTION_TYPE '=' DETERMINISTIC
 { 
 $$ = mm_strdup("encryption_type = deterministic");
}
;


 setting_name:
 ColId
 { 
 $$ = $1;
}
|  ColId indirection
 { 
 $$ = cat_str(2,$1,$2);
}
;


 CreateKeyStmt:
 CreateMasterKeyStmt
 { 
 $$ = $1;
}
|  CreateColumnKeyStmt
 { 
 $$ = $1;
}
;


 CreateMasterKeyStmt:
 CREATE CLIENT MASTER KEY setting_name WITH '(' master_key_params ')'
 { 
 $$ = cat_str(5,mm_strdup("create client master key"),$5,mm_strdup("with ("),$8,mm_strdup(")"));
}
;


 master_key_params:
 master_key_elem
 { 
 $$ = $1;
}
|  master_key_params ',' master_key_elem
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 master_key_elem:
 KEY_STORE '=' ColId
 { 
 $$ = cat_str(2,mm_strdup("key_store ="),$3);
}
|  KEY_PATH '=' ColId
 { 
 $$ = cat_str(2,mm_strdup("keyath ="),$3);
}
|  ALGORITHM '=' ColId
 { 
 $$ = cat_str(2,mm_strdup("algorithm ="),$3);
}
;


 CreateColumnKeyStmt:
 CREATE COLUMN ENCRYPTION KEY setting_name WITH VALUES '(' column_key_params ')'
 { 
 $$ = cat_str(5,mm_strdup("create column encryption key"),$5,mm_strdup("with values ("),$9,mm_strdup(")"));
}
;


 column_key_params:
 column_key_elem
 { 
 $$ = $1;
}
|  column_key_params ',' column_key_elem
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 column_key_elem:
 CLIENT_MASTER_KEY '=' setting_name
 { 
 $$ = cat_str(2,mm_strdup("client_master_key ="),$3);
}
|  ALGORITHM '=' ColId
 { 
 $$ = cat_str(2,mm_strdup("algorithm ="),$3);
}
|  ENCRYPTED_VALUE '=' ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("encrypted_value ="),$3);
}
;


 datatypecl:
 DATATYPE_CL '=' client_logic_type','
 { 
 $$ = mm_strdup("datatype_cl = client_logic_type,");
}
| 
 { 
 $$=EMPTY; }
;


 InformationalConstraintElem:
 NOT_ENFORCED
 { 
 $$ = mm_strdup("not_enforced");
}
|  NOT_ENFORCED DISABLE_P QUERY OPTIMIZATION
 { 
 $$ = mm_strdup("not_enforced disable query optimization");
}
|  NOT_ENFORCED ENABLE_P QUERY OPTIMIZATION
 { 
 $$ = mm_strdup("not_enforced enable query optimization");
}
|  ENFORCED
 { 
 $$ = mm_strdup("enforced");
}
| 
 { 
 $$=EMPTY; }
;


 ColConstraintElem:
 NOT NULL_P
 { 
 $$ = mm_strdup("not null");
}
|  NOT NULL_P ENABLE_P
 { 
 $$ = mm_strdup("not null enable");
}
|  NULL_P
 { 
 $$ = mm_strdup("null");
}
|  opt_unique_key opt_definition OptConsTableSpaceWithEmpty InformationalConstraintElem
 { 
 $$ = cat_str(4,$1,$2,$3,$4);
}
|  opt_unique_key opt_definition OptConsTableSpaceWithEmpty ENABLE_P InformationalConstraintElem
 { 
 $$ = cat_str(5,$1,$2,$3,mm_strdup("enable"),$5);
}
|  PRIMARY KEY opt_definition OptConsTableSpaceWithEmpty InformationalConstraintElem
 { 
 $$ = cat_str(4,mm_strdup("primary key"),$3,$4,$5);
}
|  PRIMARY KEY opt_definition OptConsTableSpaceWithEmpty ENABLE_P InformationalConstraintElem
 { 
 $$ = cat_str(5,mm_strdup("primary key"),$3,$4,mm_strdup("enable"),$6);
}
|  CHECK '(' a_expr ')' opt_no_inherit
 { 
 $$ = cat_str(4,mm_strdup("check ("),$3,mm_strdup(")"),$5);
}
|  CHECK '(' a_expr ')' opt_no_inherit ENABLE_P
 { 
 $$ = cat_str(5,mm_strdup("check ("),$3,mm_strdup(")"),$5,mm_strdup("enable"));
}
|  DEFAULT b_expr
 { 
 $$ = cat_str(2,mm_strdup("default"),$2);
}
|  ON_UPDATE_TIME UPDATE b_expr
 { 
 $$ = cat_str(2,mm_strdup("on_update_time update"),$3);
}
|  GENERATED ALWAYS AS '(' a_expr ')' generated_column_option
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(4,mm_strdup("generated always as ("),$5,mm_strdup(")"),$7);
}
|  REFERENCES qualified_name opt_column_list key_match key_actions
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(5,mm_strdup("references"),$2,$3,$4,$5);
}
|  REFERENCES qualified_name opt_column_list key_match key_actions ENABLE_P
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(6,mm_strdup("references"),$2,$3,$4,$5,mm_strdup("enable"));
}
;


 opt_unique_key:
 UNIQUE
 { 
 $$ = mm_strdup("unique");
}
|  UNIQUE KEY
 { 
 $$ = mm_strdup("unique key");
}
;


 generated_column_option:
 STORED
 { 
 $$ = mm_strdup("stored");
}
| 
 { 
 $$=EMPTY; }
;


 ConstraintAttr:
 DEFERRABLE
 { 
 $$ = mm_strdup("deferrable");
}
|  NOT DEFERRABLE
 { 
 $$ = mm_strdup("not deferrable");
}
|  INITIALLY DEFERRED
 { 
 $$ = mm_strdup("initially deferred");
}
|  INITIALLY IMMEDIATE
 { 
 $$ = mm_strdup("initially immediate");
}
;


 TableLikeClause:
 LIKE qualified_name TableLikeOptionList
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,mm_strdup("like"),$2,$3);
}
|  LIKE qualified_name INCLUDING_ALL excluding_option_list
 { 
 $$ = cat_str(4,mm_strdup("like"),$2,mm_strdup("including all"),$4);
}
;


 excluding_option_list:
 excluding_option_list EXCLUDING TableLikeExcludingOption
 { 
 $$ = cat_str(3,$1,mm_strdup("excluding"),$3);
}
| 
 { 
 $$=EMPTY; }
;


 TableLikeOptionList:
 TableLikeOptionList INCLUDING TableLikeIncludingOption
 { 
 $$ = cat_str(3,$1,mm_strdup("including"),$3);
}
|  TableLikeOptionList EXCLUDING TableLikeExcludingOption
 { 
 $$ = cat_str(3,$1,mm_strdup("excluding"),$3);
}
| 
 { 
 $$=EMPTY; }
;


 TableLikeIncludingOption:
 DEFAULTS
 { 
 $$ = mm_strdup("defaults");
}
|  CONSTRAINTS
 { 
 $$ = mm_strdup("constraints");
}
|  INDEXES
 { 
 $$ = mm_strdup("indexes");
}
|  STORAGE
 { 
 $$ = mm_strdup("storage");
}
|  COMMENTS
 { 
 $$ = mm_strdup("comments");
}
|  PARTITION
 { 
 $$ = mm_strdup("partition");
}
|  RELOPTIONS
 { 
 $$ = mm_strdup("reloptions");
}
|  DISTRIBUTION
 { 
 $$ = mm_strdup("distribution");
}
|  OIDS
 { 
 $$ = mm_strdup("oids");
}
|  GENERATED
 { 
 $$ = mm_strdup("generated");
}
;


 TableLikeExcludingOption:
 DEFAULTS
 { 
 $$ = mm_strdup("defaults");
}
|  CONSTRAINTS
 { 
 $$ = mm_strdup("constraints");
}
|  INDEXES
 { 
 $$ = mm_strdup("indexes");
}
|  STORAGE
 { 
 $$ = mm_strdup("storage");
}
|  COMMENTS
 { 
 $$ = mm_strdup("comments");
}
|  PARTITION
 { 
 $$ = mm_strdup("partition");
}
|  RELOPTIONS
 { 
 $$ = mm_strdup("reloptions");
}
|  DISTRIBUTION
 { 
 $$ = mm_strdup("distribution");
}
|  OIDS
 { 
 $$ = mm_strdup("oids");
}
|  GENERATED
 { 
 $$ = mm_strdup("generated");
}
|  ALL
 { 
 $$ = mm_strdup("all");
}
;


 opt_internal_data:
 INTERNAL DATA_P internal_data_body
 { 
 $$ = cat_str(2,mm_strdup("internal data"),$3);
}
| 
 { 
 $$=EMPTY; }
;


 internal_data_body:

 { 
 $$=EMPTY; }
;


 TableConstraint:
 CONSTRAINT name ConstraintElem
 { 
 $$ = cat_str(3,mm_strdup("constraint"),$2,$3);
}
|  CONSTRAINT ConstraintElem
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("constraint"),$2);
}
|  ConstraintElem
 { 
 $$ = $1;
}
;


 ConstraintElem:
 CHECK '(' a_expr ')' ConstraintAttributeSpec opt_index_options
 { 
 $$ = cat_str(5,mm_strdup("check ("),$3,mm_strdup(")"),$5,$6);
}
|  UNIQUE name access_method_clause_without_keyword '(' constraint_params ')' opt_c_include opt_definition OptConsTableSpace opt_table_index_options ConstraintAttributeSpec InformationalConstraintElem
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(12,mm_strdup("unique"),$2,$3,mm_strdup("("),$5,mm_strdup(")"),$7,$8,$9,$10,$11,$12);
}
|  UNIQUE name access_method_clause_without_keyword '(' constraint_params ')' opt_c_include opt_definition opt_table_index_options ConstraintAttributeSpec InformationalConstraintElem
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(11,mm_strdup("unique"),$2,$3,mm_strdup("("),$5,mm_strdup(")"),$7,$8,$9,$10,$11);
}
|  UNIQUE USING ecpg_ident '(' constraint_params ')' opt_c_include opt_definition OptConsTableSpace opt_table_index_options ConstraintAttributeSpec InformationalConstraintElem
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(11,mm_strdup("unique using"),$3,mm_strdup("("),$5,mm_strdup(")"),$7,$8,$9,$10,$11,$12);
}
|  UNIQUE USING ecpg_ident '(' constraint_params ')' opt_c_include opt_definition opt_table_index_options ConstraintAttributeSpec InformationalConstraintElem
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(10,mm_strdup("unique using"),$3,mm_strdup("("),$5,mm_strdup(")"),$7,$8,$9,$10,$11);
}
|  UNIQUE '(' constraint_params ')' opt_c_include opt_definition OptConsTableSpace opt_table_index_options ConstraintAttributeSpec InformationalConstraintElem
 { 
 $$ = cat_str(9,mm_strdup("unique ("),$3,mm_strdup(")"),$5,$6,$7,$8,$9,$10);
}
|  UNIQUE '(' constraint_params ')' opt_c_include opt_definition opt_table_index_options ConstraintAttributeSpec InformationalConstraintElem
 { 
 $$ = cat_str(8,mm_strdup("unique ("),$3,mm_strdup(")"),$5,$6,$7,$8,$9);
}
|  UNIQUE ExistingIndex ConstraintAttributeSpec InformationalConstraintElem opt_index_options
 { 
 $$ = cat_str(5,mm_strdup("unique"),$2,$3,$4,$5);
}
|  PRIMARY KEY USING ecpg_ident '(' constraint_params ')' opt_c_include opt_definition OptConsTableSpace opt_table_index_options ConstraintAttributeSpec InformationalConstraintElem
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(11,mm_strdup("primary key using"),$4,mm_strdup("("),$6,mm_strdup(")"),$8,$9,$10,$11,$12,$13);
}
|  PRIMARY KEY USING ecpg_ident '(' constraint_params ')' opt_c_include opt_definition opt_table_index_options ConstraintAttributeSpec InformationalConstraintElem
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(10,mm_strdup("primary key using"),$4,mm_strdup("("),$6,mm_strdup(")"),$8,$9,$10,$11,$12);
}
|  PRIMARY KEY '(' constraint_params ')' opt_c_include opt_definition OptConsTableSpace opt_table_index_options ConstraintAttributeSpec InformationalConstraintElem
 { 
 $$ = cat_str(9,mm_strdup("primary key ("),$4,mm_strdup(")"),$6,$7,$8,$9,$10,$11);
}
|  PRIMARY KEY '(' constraint_params ')' opt_c_include opt_definition opt_table_index_options ConstraintAttributeSpec InformationalConstraintElem
 { 
 $$ = cat_str(8,mm_strdup("primary key ("),$4,mm_strdup(")"),$6,$7,$8,$9,$10);
}
|  PRIMARY KEY ExistingIndex ConstraintAttributeSpec InformationalConstraintElem opt_index_options
 { 
 $$ = cat_str(5,mm_strdup("primary key"),$3,$4,$5,$6);
}
|  EXCLUDE access_method_clause '(' ExclusionConstraintList ')' opt_c_include opt_definition OptConsTableSpaceWithEmpty ExclusionWhereClause ConstraintAttributeSpec opt_index_options
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(11,mm_strdup("exclude"),$2,mm_strdup("("),$4,mm_strdup(")"),$6,$7,$8,$9,$10,$11);
}
|  FOREIGN KEY name '(' columnList ')' REFERENCES qualified_name opt_column_list key_match key_actions ConstraintAttributeSpec opt_index_options
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(11,mm_strdup("foreign key"),$3,mm_strdup("("),$5,mm_strdup(") references"),$8,$9,$10,$11,$12,$13);
}
|  FOREIGN KEY '(' columnList ')' REFERENCES qualified_name opt_column_list key_match key_actions ConstraintAttributeSpec opt_index_options
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(9,mm_strdup("foreign key ("),$4,mm_strdup(") references"),$7,$8,$9,$10,$11,$12);
}
|  PARTIAL CLUSTER KEY '(' columnList ')' ConstraintAttributeSpec opt_index_options
 { 
 $$ = cat_str(5,mm_strdup("partial cluster key ("),$5,mm_strdup(")"),$7,$8);
}
;


 opt_no_inherit:
 NO INHERIT
 { 
 $$ = mm_strdup("no inherit");
}
| 
 { 
 $$=EMPTY; }
;


 opt_column_list:
 '(' columnList ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 columnList:
 columnElem
 { 
 $$ = $1;
}
|  columnList ',' columnElem
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 columnElem:
 ColId
 { 
 $$ = $1;
}
;


 opt_c_include:
 INCLUDE '(' columnList ')'
 { 
 $$ = cat_str(3,mm_strdup("include ("),$3,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 key_match:
 MATCH FULL
 { 
 $$ = mm_strdup("match full");
}
|  MATCH PARTIAL
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = mm_strdup("match partial");
}
|  MATCH SIMPLE
 { 
 $$ = mm_strdup("match simple");
}
| 
 { 
 $$=EMPTY; }
;


 ExclusionConstraintList:
 ExclusionConstraintElem
 { 
 $$ = $1;
}
|  ExclusionConstraintList ',' ExclusionConstraintElem
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 ExclusionConstraintElem:
 index_elem WITH any_operator
 { 
 $$ = cat_str(3,$1,mm_strdup("with"),$3);
}
|  index_elem WITH OPERATOR '(' any_operator ')'
 { 
 $$ = cat_str(4,$1,mm_strdup("with operator ("),$5,mm_strdup(")"));
}
;


 ExclusionWhereClause:
 WHERE '(' a_expr ')'
 { 
 $$ = cat_str(3,mm_strdup("where ("),$3,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 key_actions:
 key_update
 { 
 $$ = $1;
}
|  key_delete
 { 
 $$ = $1;
}
|  key_update key_delete
 { 
 $$ = cat_str(2,$1,$2);
}
|  key_delete key_update
 { 
 $$ = cat_str(2,$1,$2);
}
| 
 { 
 $$=EMPTY; }
;


 key_update:
 ON UPDATE key_action
 { 
 $$ = cat_str(2,mm_strdup("on update"),$3);
}
;


 key_delete:
 ON DELETE_P key_action
 { 
 $$ = cat_str(2,mm_strdup("on delete"),$3);
}
;


 key_action:
 NO ACTION
 { 
 $$ = mm_strdup("no action");
}
|  RESTRICT
 { 
 $$ = mm_strdup("restrict");
}
|  CASCADE
 { 
 $$ = mm_strdup("cascade");
}
|  SET NULL_P
 { 
 $$ = mm_strdup("set null");
}
|  SET DEFAULT
 { 
 $$ = mm_strdup("set default");
}
;


 OptInherit:
 INHERITS '(' qualified_name_list ')'
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,mm_strdup("inherits ("),$3,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 OptWith:
 WITH reloptions
 { 
 $$ = cat_str(2,mm_strdup("with"),$2);
}
|  WITH OIDS
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = mm_strdup("with oids");
}
|  WITHOUT OIDS
 { 
 $$ = mm_strdup("without oids");
}
| 
 { 
 $$=EMPTY; }
;


 OnCommitOption:
 ON COMMIT DROP
 { 
 $$ = mm_strdup("on commit drop");
}
|  ON COMMIT DELETE_P ROWS
 { 
 $$ = mm_strdup("on commit delete rows");
}
|  ON COMMIT PRESERVE ROWS
 { 
 $$ = mm_strdup("on commit preserve rows");
}
| 
 { 
 $$=EMPTY; }
;


 AutoIncrementValue:
 AUTO_INCREMENT Iconst
 { 
 $$ = cat_str(2,mm_strdup("auto_increment"),$2);
}
|  AUTO_INCREMENT '=' Iconst
 { 
 $$ = cat_str(2,mm_strdup("auto_increment ="),$3);
}
|  AUTO_INCREMENT ecpg_fconst
 { 
 $$ = cat_str(2,mm_strdup("auto_increment"),$2);
}
|  AUTO_INCREMENT '=' ecpg_fconst
 { 
 $$ = cat_str(2,mm_strdup("auto_increment ="),$3);
}
;


 OptAutoIncrement:
 AutoIncrementValue
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 OptTableSpace:
 TABLESPACE opt_equal name
 { 
 $$ = cat_str(3,mm_strdup("tablespace"),$2,$3);
}
| 
 { 
 $$=EMPTY; }
;


 OptGPI:
 UPDATE GLOBAL INDEX
 { 
 $$ = mm_strdup("update global index");
}
| 
 { 
 $$=EMPTY; }
;


 OptCompress:
 COMPRESS
 { 
 $$ = mm_strdup("compress");
}
|  NOCOMPRESS
 { 
 $$ = mm_strdup("nocompress");
}
| 
 { 
 $$=EMPTY; }
;


 OptDistributeBy:
 OptDistributeByInternal
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 OptDatanodeName:
 DATANODE name
 { 
 $$ = cat_str(2,mm_strdup("datanode"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 OptDistributeType:
 ecpg_ident
 { 
 $$ = $1;
}
;


 OptDistributeByInternal:
 DISTRIBUTE BY OptDistributeType '(' name_list ')'
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(5,mm_strdup("distribute by"),$3,mm_strdup("("),$5,mm_strdup(")"));
}
|  DISTRIBUTE BY OptDistributeType
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("distribute by"),$3);
}
|  distribute_by_range_clause
 { 
 $$ = $1;
}
|  distribute_by_list_clause
 { 
 $$ = $1;
}
;


 distribute_by_list_clause:
 DISTRIBUTE BY LIST '(' name_list ')' OptListDistribution
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(4,mm_strdup("distribute by list ("),$5,mm_strdup(")"),$7);
}
;


 OptListDistribution:
 '(' list_dist_state ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
|  SliceReferenceClause
 { 
 $$ = $1;
}
;


 list_dist_state:
 list_distribution_rules_list
 { 
 $$ = $1;
}
;


 list_distribution_rules_list:
 list_dist_value
 { 
 $$ = $1;
}
|  list_distribution_rules_list ',' list_dist_value
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 list_dist_value:
 SLICE name VALUES '(' list_distribution_rule_row ')' OptDatanodeName
 { 
 $$ = cat_str(6,mm_strdup("slice"),$2,mm_strdup("values ("),$5,mm_strdup(")"),$7);
}
|  SLICE name VALUES '(' DEFAULT ')' OptDatanodeName
 { 
 $$ = cat_str(4,mm_strdup("slice"),$2,mm_strdup("values ( default )"),$7);
}
;


 list_distribution_rule_row:
 list_distribution_rule_single
 { 
 $$ = $1;
}
|  list_distribution_rule_row ',' list_distribution_rule_single
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 list_distribution_rule_single:
 '(' expr_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
|  c_expr_noparen
 { 
 $$ = $1;
}
;


 distribute_by_range_clause:
 DISTRIBUTE BY RANGE '(' name_list ')' '(' range_slice_definition_list ')'
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(5,mm_strdup("distribute by range ("),$5,mm_strdup(") ("),$8,mm_strdup(")"));
}
|  DISTRIBUTE BY RANGE '(' name_list ')' SliceReferenceClause
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(4,mm_strdup("distribute by range ("),$5,mm_strdup(")"),$7);
}
;


 SliceReferenceClause:
 SLICE REFERENCES name
 { 
 $$ = cat_str(2,mm_strdup("slice references"),$3);
}
;


 range_slice_definition_list:
 range_slice_less_than_list
 { 
 $$ = $1;
}
|  range_slice_start_end_list
 { 
 $$ = $1;
}
;


 range_slice_less_than_list:
 range_slice_less_than_item
 { 
 $$ = $1;
}
|  range_slice_less_than_list ',' range_slice_less_than_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 range_slice_less_than_item:
 SLICE name VALUES LESS THAN '(' maxValueList ')' OptDatanodeName
 { 
 $$ = cat_str(6,mm_strdup("slice"),$2,mm_strdup("values less than ("),$7,mm_strdup(")"),$9);
}
;


 range_slice_start_end_list:
 range_slice_start_end_item
 { 
 $$ = $1;
}
|  range_slice_start_end_list ',' range_slice_start_end_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 range_slice_start_end_item:
 SLICE name START '(' maxValueList ')' END_P '(' maxValueList ')' opt_range_every_list
 { 
 $$ = cat_str(8,mm_strdup("slice"),$2,mm_strdup("start ("),$5,mm_strdup(") end ("),$9,mm_strdup(")"),$11);
}
|  SLICE name END_P '(' maxValueList ')'
 { 
 $$ = cat_str(5,mm_strdup("slice"),$2,mm_strdup("end ("),$5,mm_strdup(")"));
}
|  SLICE name START '(' maxValueList ')'
 { 
 $$ = cat_str(5,mm_strdup("slice"),$2,mm_strdup("start ("),$5,mm_strdup(")"));
}
;


 OptSubCluster:
 OptSubClusterInternal
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 OptSubClusterInternal:
 TO NODE pgxcnodes
 { 
 $$ = cat_str(2,mm_strdup("to node"),$3);
}
|  TO GROUP_P pgxcgroup_name
 { 
 $$ = cat_str(2,mm_strdup("to group"),$3);
}
;


 OptConsTableSpace:
 USING INDEX OptPartitionElement
 { 
 $$ = cat_str(2,mm_strdup("using index"),$3);
}
;


 OptConsTableSpaceWithEmpty:
 USING INDEX OptPartitionElement
 { 
 $$ = cat_str(2,mm_strdup("using index"),$3);
}
| 
 { 
 $$=EMPTY; }
;


 OptPartitionElement:
 OptTableSpace OptPctFree OptInitRans OptMaxTrans OptStorage
 { 
 $$ = cat_str(5,$1,$2,$3,$4,$5);
}
;


 OptPctFree:
 PCTFREE Iconst
 { 
 $$ = cat_str(2,mm_strdup("pctfree"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 OptInitRans:
 INITRANS Iconst
 { 
 $$ = cat_str(2,mm_strdup("initrans"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 OptMaxTrans:
 MAXTRANS Iconst
 { 
 $$ = cat_str(2,mm_strdup("maxtrans"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 OptStorage:
 STORAGE '(' OptInitial OptNext OptMinextents OptMaxextents ')'
 { 
 $$ = cat_str(6,mm_strdup("storage ("),$3,$4,$5,$6,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 OptInitial:
 INITIAL_P Iconst ecpg_ident
 { 
 $$ = cat_str(3,mm_strdup("initial"),$2,$3);
}
| 
 { 
 $$=EMPTY; }
;


 OptNext:
 NEXT Iconst ecpg_ident
 { 
 $$ = cat_str(3,mm_strdup("next"),$2,$3);
}
| 
 { 
 $$=EMPTY; }
;


 OptMinextents:
 MINEXTENTS Iconst
 { 
 $$ = cat_str(2,mm_strdup("minextents"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 OptMaxextents:
 MAXEXTENTS UNLIMITED
 { 
 $$ = mm_strdup("maxextents unlimited");
}
|  MAXEXTENTS Iconst
 { 
 $$ = cat_str(2,mm_strdup("maxextents"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 ExistingIndex:
 USING INDEX index_name
 { 
 $$ = cat_str(2,mm_strdup("using index"),$3);
}
;


 OptDuplicate:

 { 
 $$=EMPTY; }
|  IGNORE
 { 
 $$ = mm_strdup("ignore");
}
|  REPLACE
 { 
 $$ = mm_strdup("replace");
}
;


 create_as_target:
 qualified_name opt_column_list OptWith OnCommitOption OptCompress OptTableSpace OptDistributeBy OptSubCluster
 { 
 $$ = cat_str(8,$1,$2,$3,$4,$5,$6,$7,$8);
}
|  qualified_name '(' OptTableElementList ')' OptInherit OptAutoIncrement optCharsetCollate OptWith OnCommitOption OptCompress OptPartitionElement OptDistributeBy OptSubCluster OptDuplicate
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(14,$1,mm_strdup("("),$3,mm_strdup(")"),$5,$6,$7,$8,$9,$10,$11,$12,$13,$14);
}
;


 opt_with_data:
 WITH DATA_P
 { 
 $$ = mm_strdup("with data");
}
|  WITH NO DATA_P
 { 
 $$ = mm_strdup("with no data");
}
| 
 { 
 $$=EMPTY; }
;


 SnapshotStmt:
 CREATE OptTemp SNAPSHOT qualified_name OptSnapshotVersion OptDistributeBy OptSubCluster OptSnapshotComment AS SelectStmt
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(10,mm_strdup("create"),$2,mm_strdup("snapshot"),$4,$5,$6,$7,$8,mm_strdup("as"),$10);
}
|  CREATE OptTemp SNAPSHOT qualified_name OptSnapshotVersion OptDistributeBy OptSubCluster FROM SnapshotVersion OptSnapshotComment USING '(' AlterSnapshotCmdList ')'
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(13,mm_strdup("create"),$2,mm_strdup("snapshot"),$4,$5,$6,$7,mm_strdup("from"),$9,$10,mm_strdup("using ("),$13,mm_strdup(")"));
}
|  SAMPLE SNAPSHOT qualified_name SnapshotVersion OptSnapshotStratify SnapshotSampleList
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(5,mm_strdup("sample snapshot"),$3,$4,$5,$6);
}
|  ARCHIVE SNAPSHOT qualified_name SnapshotVersion
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,mm_strdup("archive snapshot"),$3,$4);
}
|  PUBLISH SNAPSHOT qualified_name SnapshotVersion
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,mm_strdup("publish snapshot"),$3,$4);
}
|  PURGE SNAPSHOT qualified_name SnapshotVersion
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,mm_strdup("purge snapshot"),$3,$4);
}
;


 SnapshotVersion:
 '@' Iconst
 { 
 $$ = cat_str(2,mm_strdup("@"),$2);
}
|  '@' ecpg_fconst
 { 
 $$ = cat_str(2,mm_strdup("@"),$2);
}
|  '@' VCONST
 { 
 $$ = mm_strdup("@ vconst");
}
|  '@' ColId_or_Sconst
 { 
 $$ = cat_str(2,mm_strdup("@"),$2);
}
;


 OptSnapshotVersion:
 SnapshotVersion
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 OptSnapshotComment:
 COMMENT IS comment_text
 { 
 $$ = cat_str(2,mm_strdup("comment is"),$3);
}
| 
 { 
 $$=EMPTY; }
;


 AlterSnapshotCmdList:
 AlterSnapshotCmdListNoParens
 { 
 $$ = $1;
}
|  AlterSnapshotCmdListWithParens
 { 
 $$ = $1;
}
;


 AlterSnapshotCmdListWithParens:
 '(' AlterSnapshotCmdListNoParens ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
|  '(' AlterSnapshotCmdListWithParens ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
;


 AlterSnapshotCmdListNoParens:
 AlterSnapshotCmdOrEmpty
 { 
 $$ = $1;
}
|  AlterSnapshotCmdListNoParens ';' AlterSnapshotCmdOrEmpty
 { 
 $$ = cat_str(3,$1,mm_strdup(";"),$3);
}
;


 AlterSnapshotCmdOrEmpty:
 INSERT OptInsertIntoSnapshot insert_rest
 { 
 $$ = cat_str(3,mm_strdup("insert"),$2,$3);
}
|  UPDATE OptAlterUpdateSnapshot OptSnapshotAlias SET set_clause_list from_clause where_clause
 { 
 $$ = cat_str(7,mm_strdup("update"),$2,$3,mm_strdup("set"),$5,$6,$7);
}
|  DELETE_P OptDeleteFromSnapshot OptSnapshotAlias using_clause where_clause
 { 
 $$ = cat_str(5,mm_strdup("delete"),$2,$3,$4,$5);
}
|  ALTER OptAlterUpdateSnapshot AlterSnapshotDdlList
 { 
 $$ = cat_str(3,mm_strdup("alter"),$2,$3);
}
| 
 { 
 $$=EMPTY; }
;


 OptAlterUpdateSnapshot:
 SNAPSHOT
 { 
 $$ = mm_strdup("snapshot");
}
| 
 { 
 $$=EMPTY; }
;


 OptInsertIntoSnapshot:
 INTO SNAPSHOT
 { 
 $$ = mm_strdup("into snapshot");
}
| 
 { 
 $$=EMPTY; }
;


 OptDeleteFromSnapshot:
 FROM SNAPSHOT
 { 
 $$ = mm_strdup("from snapshot");
}
| 
 { 
 $$=EMPTY; }
;


 OptSnapshotAlias:
 AS ColId
 { 
 $$ = cat_str(2,mm_strdup("as"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 AlterSnapshotDdlList:
 AlterSnapshotDdl
 { 
 $$ = $1;
}
|  AlterSnapshotDdlList ',' AlterSnapshotDdl
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 AlterSnapshotDdl:
 ADD_P opt_column columnDef
 { 
 $$ = cat_str(3,mm_strdup("add"),$2,$3);
}
|  DROP opt_column IF_P EXISTS ColId
 { 
 $$ = cat_str(4,mm_strdup("drop"),$2,mm_strdup("if exists"),$5);
}
|  DROP opt_column ColId
 { 
 $$ = cat_str(3,mm_strdup("drop"),$2,$3);
}
;


 SnapshotSample:
 AS ColLabel AT RATIO ecpg_fconst OptSnapshotComment
 { 
 $$ = cat_str(5,mm_strdup("as"),$2,mm_strdup("at ratio"),$5,$6);
}
;


 SnapshotSampleList:
 SnapshotSample
 { 
 $$ = $1;
}
|  SnapshotSampleList ',' SnapshotSample
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 OptSnapshotStratify:
 STRATIFY BY column_item_list
 { 
 $$ = cat_str(2,mm_strdup("stratify by"),$3);
}
| 
 { 
 $$=EMPTY; }
;


 CreateMatViewStmt:
 CREATE OptNoLog opt_incremental MATERIALIZED VIEW create_mv_target AS SelectStmt opt_with_data
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(8,mm_strdup("create"),$2,$3,mm_strdup("materialized view"),$6,mm_strdup("as"),$8,$9);
}
;


 create_mv_target:
 qualified_name opt_column_list OptDistributeBy opt_reloptions OptTableSpace
 { 
 $$ = cat_str(5,$1,$2,$3,$4,$5);
}
;


 OptNoLog:
 UNLOGGED
 { 
 $$ = mm_strdup("unlogged");
}
| 
 { 
 $$=EMPTY; }
;


 opt_incremental:
 INCREMENTAL
 { 
 $$ = mm_strdup("incremental");
}
| 
 { 
 $$=EMPTY; }
;


 RefreshMatViewStmt:
 REFRESH opt_incremental MATERIALIZED VIEW qualified_name opt_with_data
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(5,mm_strdup("refresh"),$2,mm_strdup("materialized view"),$5,$6);
}
;


 CreateSeqStmt:
 CREATE OptTemp opt_large_seq SEQUENCE qualified_name OptSeqOptList
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(6,mm_strdup("create"),$2,$3,mm_strdup("sequence"),$5,$6);
}
;


 AlterSeqStmt:
 ALTER SEQUENCE qualified_name SeqOptList
 { 
 $$ = cat_str(3,mm_strdup("alter sequence"),$3,$4);
}
|  ALTER LARGE_P SEQUENCE qualified_name SeqOptList
 { 
 $$ = cat_str(3,mm_strdup("alter large sequence"),$4,$5);
}
|  ALTER SEQUENCE IF_P EXISTS qualified_name SeqOptList
 { 
 $$ = cat_str(3,mm_strdup("alter sequence if exists"),$5,$6);
}
|  ALTER LARGE_P SEQUENCE IF_P EXISTS qualified_name SeqOptList
 { 
 $$ = cat_str(3,mm_strdup("alter large sequence if exists"),$6,$7);
}
;


 opt_large_seq:
 LARGE_P
 { 
 $$ = mm_strdup("large");
}
| 
 { 
 $$=EMPTY; }
;


 OptSeqOptList:
 SeqOptList
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 SeqOptList:
 SeqOptElem
 { 
 $$ = $1;
}
|  SeqOptList SeqOptElem
 { 
 $$ = cat_str(2,$1,$2);
}
;


 SeqOptElem:
 CACHE NumericOnly
 { 
 $$ = cat_str(2,mm_strdup("cache"),$2);
}
|  CYCLE
 { 
 $$ = mm_strdup("cycle");
}
|  NO CYCLE
 { 
 $$ = mm_strdup("no cycle");
}
|  INCREMENT opt_by NumericOnly
 { 
 $$ = cat_str(3,mm_strdup("increment"),$2,$3);
}
|  MAXVALUE NumericOnly
 { 
 $$ = cat_str(2,mm_strdup("maxvalue"),$2);
}
|  MINVALUE NumericOnly
 { 
 $$ = cat_str(2,mm_strdup("minvalue"),$2);
}
|  NO MAXVALUE
 { 
 $$ = mm_strdup("no maxvalue");
}
|  NO MINVALUE
 { 
 $$ = mm_strdup("no minvalue");
}
|  OWNED BY any_name
 { 
 $$ = cat_str(2,mm_strdup("owned by"),$3);
}
|  START_WITH NumericOnly
 { 
 $$ = cat_str(2,mm_strdup("start_with"),$2);
}
|  START NumericOnly
 { 
 $$ = cat_str(2,mm_strdup("start"),$2);
}
|  RESTART
 { 
 $$ = mm_strdup("restart");
}
|  RESTART opt_with NumericOnly
 { 
 $$ = cat_str(3,mm_strdup("restart"),$2,$3);
}
|  NOCYCLE
 { 
 $$ = mm_strdup("nocycle");
}
|  NOMAXVALUE
 { 
 $$ = mm_strdup("nomaxvalue");
}
|  NOMINVALUE
 { 
 $$ = mm_strdup("nominvalue");
}
;


 opt_by:
 BY
 { 
 $$ = mm_strdup("by");
}
| 
 { 
 $$=EMPTY; }
;


 NumericOnly:
 ecpg_fconst
 { 
 $$ = $1;
}
|  '+' ecpg_fconst
 { 
 $$ = cat_str(2,mm_strdup("+"),$2);
}
|  '-' ecpg_fconst
 { 
 $$ = cat_str(2,mm_strdup("-"),$2);
}
|  SignedIconst
 { 
 $$ = $1;
}
;


 NumericOnly_list:
 NumericOnly
 { 
 $$ = $1;
}
|  NumericOnly_list ',' NumericOnly
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 CreatePLangStmt:
 CREATE opt_or_replace opt_trusted opt_procedural LANGUAGE ColId_or_Sconst
 { 
 $$ = cat_str(6,mm_strdup("create"),$2,$3,$4,mm_strdup("language"),$6);
}
|  CREATE opt_or_replace opt_trusted opt_procedural LANGUAGE ColId_or_Sconst HANDLER handler_name opt_inline_handler opt_validator
 { 
 $$ = cat_str(10,mm_strdup("create"),$2,$3,$4,mm_strdup("language"),$6,mm_strdup("handler"),$8,$9,$10);
}
;


 opt_trusted:
 TRUSTED
 { 
 $$ = mm_strdup("trusted");
}
| 
 { 
 $$=EMPTY; }
;


 handler_name:
 name
 { 
 $$ = $1;
}
|  name attrs
 { 
 $$ = cat_str(2,$1,$2);
}
;


 opt_inline_handler:
 INLINE_P handler_name
 { 
 $$ = cat_str(2,mm_strdup("inline"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 validator_clause:
 VALIDATOR handler_name
 { 
 $$ = cat_str(2,mm_strdup("validator"),$2);
}
|  NO VALIDATOR
 { 
 $$ = mm_strdup("no validator");
}
;


 opt_validator:
 validator_clause
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 DropPLangStmt:
 DROP opt_procedural LANGUAGE ColId_or_Sconst opt_drop_behavior
 { 
 $$ = cat_str(5,mm_strdup("drop"),$2,mm_strdup("language"),$4,$5);
}
|  DROP opt_procedural LANGUAGE IF_P EXISTS ColId_or_Sconst opt_drop_behavior
 { 
 $$ = cat_str(5,mm_strdup("drop"),$2,mm_strdup("language if exists"),$6,$7);
}
;


 opt_procedural:
 PROCEDURAL
 { 
 $$ = mm_strdup("procedural");
}
| 
 { 
 $$=EMPTY; }
;


 tblspc_options:
 '(' tblspc_option_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
;


 opt_tblspc_options:
 WITH tblspc_options
 { 
 $$ = cat_str(2,mm_strdup("with"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 tblspc_option_list:
 tblspc_option_elem
 { 
 $$ = $1;
}
|  tblspc_option_list ',' tblspc_option_elem
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 tblspc_option_elem:
 ColLabel '=' ecpg_sconst
 { 
 $$ = cat_str(3,$1,mm_strdup("="),$3);
}
|  ColLabel '=' func_type
 { 
 $$ = cat_str(3,$1,mm_strdup("="),$3);
}
|  ColLabel '=' NumericOnly
 { 
 $$ = cat_str(3,$1,mm_strdup("="),$3);
}
;


 CreateTableSpaceStmt:
 CREATE TABLESPACE name OptTableSpaceOwner OptRelative LOCATION ecpg_sconst OptMaxSize opt_tblspc_options
 { 
 $$ = cat_str(8,mm_strdup("create tablespace"),$3,$4,$5,mm_strdup("location"),$7,$8,$9);
}
|  CREATE TABLESPACE name LoggingStr DATAFILE ecpg_sconst OptDatafileSize OptReuse OptAuto
 { 
 $$ = cat_str(8,mm_strdup("create tablespace"),$3,$4,mm_strdup("datafile"),$6,$7,$8,$9);
}
|  CREATE TABLESPACE name DATAFILE ecpg_sconst OptDatafileSize OptReuse OptAuto LoggingStr
 { 
 $$ = cat_str(8,mm_strdup("create tablespace"),$3,mm_strdup("datafile"),$5,$6,$7,$8,$9);
}
|  CREATE TABLESPACE name DATAFILE ecpg_sconst OptDatafileSize OptReuse OptAuto
 { 
 $$ = cat_str(7,mm_strdup("create tablespace"),$3,mm_strdup("datafile"),$5,$6,$7,$8);
}
;


 LoggingStr:
 LOGGING
 { 
 $$ = mm_strdup("logging");
}
|  NOLOGGING
 { 
 $$ = mm_strdup("nologging");
}
;


 OptDatafileSize:
 SIZE Iconst ecpg_ident
 { 
 $$ = cat_str(3,mm_strdup("size"),$2,$3);
}
| 
 { 
 $$=EMPTY; }
;


 OptReuse:
 REUSE
 { 
 $$ = mm_strdup("reuse");
}
| 
 { 
 $$=EMPTY; }
;


 OptAuto:
 AUTOEXTEND ON OptNextStr OptMaxSize
 { 
 $$ = cat_str(3,mm_strdup("autoextend on"),$3,$4);
}
|  AUTOEXTEND OFF
 { 
 $$ = mm_strdup("autoextend off");
}
| 
 { 
 $$=EMPTY; }
;


 OptNextStr:
 NEXT Iconst ecpg_ident
 { 
 $$ = cat_str(3,mm_strdup("next"),$2,$3);
}
| 
 { 
 $$=EMPTY; }
;


 OptMaxSize:
 MAXSIZE ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("maxsize"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 size_clause:
 ecpg_sconst
 { 
 $$ = $1;
}
|  UNLIMITED
 { 
 $$ = mm_strdup("unlimited");
}
;


 OptRelative:
 RELATIVE_P
 { 
 $$ = mm_strdup("relative");
}
| 
 { 
 $$=EMPTY; }
;


 OptTableSpaceOwner:
 OWNER name
 { 
 $$ = cat_str(2,mm_strdup("owner"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 DropTableSpaceStmt:
 DROP TABLESPACE name
 { 
 $$ = cat_str(2,mm_strdup("drop tablespace"),$3);
}
|  DROP TABLESPACE IF_P EXISTS name
 { 
 $$ = cat_str(2,mm_strdup("drop tablespace if exists"),$5);
}
;


 CreateExtensionStmt:
 CREATE EXTENSION name opt_with create_extension_opt_list
 { 
 $$ = cat_str(4,mm_strdup("create extension"),$3,$4,$5);
}
|  CREATE EXTENSION IF_P NOT EXISTS name opt_with create_extension_opt_list
 { 
 $$ = cat_str(4,mm_strdup("create extension if not exists"),$6,$7,$8);
}
;


 create_extension_opt_list:
 create_extension_opt_list create_extension_opt_item
 { 
 $$ = cat_str(2,$1,$2);
}
| 
 { 
 $$=EMPTY; }
;


 create_extension_opt_item:
 SCHEMA name
 { 
 $$ = cat_str(2,mm_strdup("schema"),$2);
}
|  VERSION_P ColId_or_Sconst
 { 
 $$ = cat_str(2,mm_strdup("version"),$2);
}
|  FROM ColId_or_Sconst
 { 
 $$ = cat_str(2,mm_strdup("from"),$2);
}
;


 CreateDirectoryStmt:
 CREATE opt_or_replace DIRECTORY name AS ecpg_sconst
 { 
 $$ = cat_str(6,mm_strdup("create"),$2,mm_strdup("directory"),$4,mm_strdup("as"),$6);
}
;


 DropDirectoryStmt:
 DROP DIRECTORY name
 { 
 $$ = cat_str(2,mm_strdup("drop directory"),$3);
}
|  DROP DIRECTORY IF_P EXISTS name
 { 
 $$ = cat_str(2,mm_strdup("drop directory if exists"),$5);
}
;


 AlterExtensionStmt:
 ALTER EXTENSION name UPDATE alter_extension_opt_list
 { 
 $$ = cat_str(4,mm_strdup("alter extension"),$3,mm_strdup("update"),$5);
}
;


 alter_extension_opt_list:
 alter_extension_opt_list alter_extension_opt_item
 { 
 $$ = cat_str(2,$1,$2);
}
| 
 { 
 $$=EMPTY; }
;


 alter_extension_opt_item:
 TO ColId_or_Sconst
 { 
 $$ = cat_str(2,mm_strdup("to"),$2);
}
;


 AlterExtensionContentsStmt:
 ALTER EXTENSION name add_drop AGGREGATE func_name aggr_args
 { 
 $$ = cat_str(6,mm_strdup("alter extension"),$3,$4,mm_strdup("aggregate"),$6,$7);
}
|  ALTER EXTENSION name add_drop EVENT_TRIGGER name
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(5,mm_strdup("alter extension"),$3,$4,mm_strdup("event_trigger"),$6);
}
|  ALTER EXTENSION name add_drop CAST '(' Typename AS Typename ')'
 { 
 $$ = cat_str(8,mm_strdup("alter extension"),$3,$4,mm_strdup("cast ("),$7,mm_strdup("as"),$9,mm_strdup(")"));
}
|  ALTER EXTENSION name add_drop COLLATION any_name
 { 
 $$ = cat_str(5,mm_strdup("alter extension"),$3,$4,mm_strdup("collation"),$6);
}
|  ALTER EXTENSION name add_drop CONVERSION_P any_name
 { 
 $$ = cat_str(5,mm_strdup("alter extension"),$3,$4,mm_strdup("conversion"),$6);
}
|  ALTER EXTENSION name add_drop DOMAIN_P any_name
 { 
 $$ = cat_str(5,mm_strdup("alter extension"),$3,$4,mm_strdup("domain"),$6);
}
|  ALTER EXTENSION name add_drop FUNCTION function_with_argtypes
 { 
 $$ = cat_str(5,mm_strdup("alter extension"),$3,$4,mm_strdup("function"),$6);
}
|  ALTER EXTENSION name add_drop opt_procedural LANGUAGE name
 { 
 $$ = cat_str(6,mm_strdup("alter extension"),$3,$4,$5,mm_strdup("language"),$7);
}
|  ALTER EXTENSION name add_drop OPERATOR any_operator oper_argtypes
 { 
 $$ = cat_str(6,mm_strdup("alter extension"),$3,$4,mm_strdup("operator"),$6,$7);
}
|  ALTER EXTENSION name add_drop OPERATOR CLASS any_name USING access_method
 { 
 $$ = cat_str(7,mm_strdup("alter extension"),$3,$4,mm_strdup("operator class"),$7,mm_strdup("using"),$9);
}
|  ALTER EXTENSION name add_drop OPERATOR FAMILY any_name USING access_method
 { 
 $$ = cat_str(7,mm_strdup("alter extension"),$3,$4,mm_strdup("operator family"),$7,mm_strdup("using"),$9);
}
|  ALTER EXTENSION name add_drop SCHEMA name
 { 
 $$ = cat_str(5,mm_strdup("alter extension"),$3,$4,mm_strdup("schema"),$6);
}
|  ALTER EXTENSION name add_drop TABLE any_name
 { 
 $$ = cat_str(5,mm_strdup("alter extension"),$3,$4,mm_strdup("table"),$6);
}
|  ALTER EXTENSION name add_drop TEXT_P SEARCH PARSER any_name
 { 
 $$ = cat_str(5,mm_strdup("alter extension"),$3,$4,mm_strdup("text search parser"),$8);
}
|  ALTER EXTENSION name add_drop TEXT_P SEARCH DICTIONARY any_name
 { 
 $$ = cat_str(5,mm_strdup("alter extension"),$3,$4,mm_strdup("text search dictionary"),$8);
}
|  ALTER EXTENSION name add_drop TEXT_P SEARCH TEMPLATE any_name
 { 
 $$ = cat_str(5,mm_strdup("alter extension"),$3,$4,mm_strdup("text search template"),$8);
}
|  ALTER EXTENSION name add_drop TEXT_P SEARCH CONFIGURATION any_name
 { 
 $$ = cat_str(5,mm_strdup("alter extension"),$3,$4,mm_strdup("text search configuration"),$8);
}
|  ALTER EXTENSION name add_drop SEQUENCE any_name
 { 
 $$ = cat_str(5,mm_strdup("alter extension"),$3,$4,mm_strdup("sequence"),$6);
}
|  ALTER EXTENSION name add_drop VIEW any_name
 { 
 $$ = cat_str(5,mm_strdup("alter extension"),$3,$4,mm_strdup("view"),$6);
}
|  ALTER EXTENSION name add_drop MATERIALIZED VIEW any_name
 { 
 $$ = cat_str(5,mm_strdup("alter extension"),$3,$4,mm_strdup("materialized view"),$7);
}
|  ALTER EXTENSION name add_drop FOREIGN TABLE any_name
 { 
 $$ = cat_str(5,mm_strdup("alter extension"),$3,$4,mm_strdup("foreign table"),$7);
}
|  ALTER EXTENSION name add_drop FOREIGN DATA_P WRAPPER name
 { 
 $$ = cat_str(5,mm_strdup("alter extension"),$3,$4,mm_strdup("foreign data wrapper"),$8);
}
|  ALTER EXTENSION name add_drop SERVER name
 { 
 $$ = cat_str(5,mm_strdup("alter extension"),$3,$4,mm_strdup("server"),$6);
}
|  ALTER EXTENSION name add_drop TYPE_P any_name
 { 
 $$ = cat_str(5,mm_strdup("alter extension"),$3,$4,mm_strdup("type"),$6);
}
;


 CreateWeakPasswordDictionaryStmt:
 CREATE WEAK PASSWORD DICTIONARY opt_vals weak_password_string_list
 { 
 $$ = cat_str(3,mm_strdup("create weak password dictionary"),$5,$6);
}
;


 opt_vals:
 WITH VALUES
 { 
 $$ = mm_strdup("with values");
}
| 
 { 
 $$=EMPTY; }
;


 weak_password_string_list:
 '(' password_string ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
|  weak_password_string_list ',' '(' password_string ')'
 { 
 $$ = cat_str(4,$1,mm_strdup(", ("),$4,mm_strdup(")"));
}
;


 DropWeakPasswordDictionaryStmt:
 DROP WEAK PASSWORD DICTIONARY
 { 
 $$ = mm_strdup("drop weak password dictionary");
}
;


 CreateFdwStmt:
 CREATE FOREIGN DATA_P WRAPPER name opt_fdw_options create_generic_options
 { 
 $$ = cat_str(4,mm_strdup("create foreign data wrapper"),$5,$6,$7);
}
;


 fdw_option:
 HANDLER handler_name
 { 
 $$ = cat_str(2,mm_strdup("handler"),$2);
}
|  NO HANDLER
 { 
 $$ = mm_strdup("no handler");
}
|  VALIDATOR handler_name
 { 
 $$ = cat_str(2,mm_strdup("validator"),$2);
}
|  NO VALIDATOR
 { 
 $$ = mm_strdup("no validator");
}
;


 fdw_options:
 fdw_option
 { 
 $$ = $1;
}
|  fdw_options fdw_option
 { 
 $$ = cat_str(2,$1,$2);
}
;


 opt_fdw_options:
 fdw_options
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 DropFdwStmt:
 DROP FOREIGN DATA_P WRAPPER name opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("drop foreign data wrapper"),$5,$6);
}
|  DROP FOREIGN DATA_P WRAPPER IF_P EXISTS name opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("drop foreign data wrapper if exists"),$7,$8);
}
;


 AlterFdwStmt:
 ALTER FOREIGN DATA_P WRAPPER name opt_fdw_options alter_generic_options
 { 
 $$ = cat_str(4,mm_strdup("alter foreign data wrapper"),$5,$6,$7);
}
|  ALTER FOREIGN DATA_P WRAPPER name fdw_options
 { 
 $$ = cat_str(3,mm_strdup("alter foreign data wrapper"),$5,$6);
}
;


 create_generic_options:
 OPTIONS '(' generic_option_list ')'
 { 
 $$ = cat_str(3,mm_strdup("options ("),$3,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 generic_option_list:
 generic_option_elem
 { 
 $$ = $1;
}
|  generic_option_list ',' generic_option_elem
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 alter_generic_options:
 OPTIONS '(' alter_generic_option_list ')'
 { 
 $$ = cat_str(3,mm_strdup("options ("),$3,mm_strdup(")"));
}
;


 alter_generic_option_list:
 alter_generic_option_elem
 { 
 $$ = $1;
}
|  alter_generic_option_list ',' alter_generic_option_elem
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 alter_generic_option_elem:
 generic_option_elem
 { 
 $$ = $1;
}
|  SET generic_option_elem
 { 
 $$ = cat_str(2,mm_strdup("set"),$2);
}
|  ADD_P generic_option_elem
 { 
 $$ = cat_str(2,mm_strdup("add"),$2);
}
|  DROP generic_option_name
 { 
 $$ = cat_str(2,mm_strdup("drop"),$2);
}
;


 generic_option_elem:
 generic_option_name generic_option_arg
 { 
 $$ = cat_str(2,$1,$2);
}
;


 generic_option_name:
 ColLabel
 { 
 $$ = $1;
}
;


 generic_option_arg:
 ecpg_sconst
 { 
 $$ = $1;
}
;


 fdwName:
 ecpg_ident
 { 
 $$ = $1;
}
;


 CreateForeignServerStmt:
 CREATE SERVER name opt_type opt_foreign_server_version FOREIGN DATA_P WRAPPER fdwName create_generic_options
 { 
 $$ = cat_str(7,mm_strdup("create server"),$3,$4,$5,mm_strdup("foreign data wrapper"),$9,$10);
}
;


 opt_type:
 TYPE_P ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("type"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 foreign_server_version:
 VERSION_P ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("version"),$2);
}
|  VERSION_P NULL_P
 { 
 $$ = mm_strdup("version null");
}
;


 opt_foreign_server_version:
 foreign_server_version
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 DropForeignServerStmt:
 DROP SERVER name opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("drop server"),$3,$4);
}
|  DROP SERVER IF_P EXISTS name opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("drop server if exists"),$5,$6);
}
;


 AlterForeignServerStmt:
 ALTER SERVER name foreign_server_version alter_generic_options
 { 
 $$ = cat_str(4,mm_strdup("alter server"),$3,$4,$5);
}
|  ALTER SERVER name foreign_server_version
 { 
 $$ = cat_str(3,mm_strdup("alter server"),$3,$4);
}
|  ALTER SERVER name alter_generic_options
 { 
 $$ = cat_str(3,mm_strdup("alter server"),$3,$4);
}
;


 CreateForeignTableStmt:
 CREATE FOREIGN TABLE qualified_name OptForeignTableElementList SERVER name create_generic_options ForeignTblWritable OptForeignTableLogError OptForeignTableLogRemote OptPerNodeRejectLimit OptDistributeBy OptSubCluster OptForeignPartBy
 { 
 $$ = cat_str(13,mm_strdup("create foreign table"),$4,$5,mm_strdup("server"),$7,$8,$9,$10,$11,$12,$13,$14,$15);
}
|  CREATE FOREIGN TABLE IF_P NOT EXISTS qualified_name OptForeignTableElementList SERVER name create_generic_options ForeignTblWritable OptForeignTableLogError OptForeignTableLogRemote OptPerNodeRejectLimit OptDistributeBy OptSubCluster OptForeignPartBy
 { 
 $$ = cat_str(13,mm_strdup("create foreign table if not exists"),$7,$8,mm_strdup("server"),$10,$11,$12,$13,$14,$15,$16,$17,$18);
}
|  CREATE FOREIGN TABLE qualified_name OptForeignTableElementList create_generic_options ForeignTblWritable OptForeignTableLogError OptForeignTableLogRemote OptPerNodeRejectLimit OptDistributeBy OptSubCluster OptForeignPartBy
 { 
 $$ = cat_str(11,mm_strdup("create foreign table"),$4,$5,$6,$7,$8,$9,$10,$11,$12,$13);
}
|  CREATE FOREIGN TABLE IF_P NOT EXISTS qualified_name OptForeignTableElementList create_generic_options ForeignTblWritable OptForeignTableLogError OptForeignTableLogRemote OptPerNodeRejectLimit OptDistributeBy OptSubCluster OptForeignPartBy
 { 
 $$ = cat_str(11,mm_strdup("create foreign table if not exists"),$7,$8,$9,$10,$11,$12,$13,$14,$15,$16);
}
;


 ForeignTblWritable:
 WRITE ONLY
 { 
 $$ = mm_strdup("write only");
}
|  READ ONLY
 { 
 $$ = mm_strdup("read only");
}
| 
 { 
 $$=EMPTY; }
;


 OptForeignTableElementList:
 '(' ForeignTableElementList ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
|  '(' ')'
 { 
 $$ = mm_strdup("( )");
}
;


 ForeignTableElementList:
 ForeignTableElement
 { 
 $$ = $1;
}
|  ForeignTableElementList ',' ForeignTableElement
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 ForeignTableElement:
 ForeignColDef
 { 
 $$ = $1;
}
|  ForeignTableLikeClause
 { 
 $$ = $1;
}
|  TableConstraint
 { 
 $$ = $1;
}
;


 ForeignColDef:
 ColId Typename ForeignPosition create_generic_options ColQualList
 { 
 $$ = cat_str(5,$1,$2,$3,$4,$5);
}
;


 ForeignPosition:
 POSITION '(' Iconst ',' Iconst ')'
 { 
 $$ = cat_str(5,mm_strdup("position ("),$3,mm_strdup(","),$5,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 ForeignTableLikeClause:
 LIKE qualified_name
 { 
 $$ = cat_str(2,mm_strdup("like"),$2);
}
;


 OptForeignTableLogError:
 LOG_P INTO qualified_name
 { 
 $$ = cat_str(2,mm_strdup("log into"),$3);
}
|  WITH qualified_name
 { 
 $$ = cat_str(2,mm_strdup("with"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 OptForeignTableLogRemote:
 REMOTE_P LOG_P ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("remote log"),$3);
}
|  REMOTE_P LOG_P
 { 
 $$ = mm_strdup("remote log");
}
| 
 { 
 $$=EMPTY; }
;


 OptPerNodeRejectLimit:
 PER_P NODE REJECT_P LIMIT ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("per node reject limit"),$5);
}
| 
 { 
 $$=EMPTY; }
;


 OptForeignPartBy:
 OptForeignPartAuto
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 OptForeignPartAuto:
 PARTITION BY '(' partition_item_list ')' AUTOMAPPED
 { 
 $$ = cat_str(3,mm_strdup("partition by ("),$4,mm_strdup(") automapped"));
}
|  PARTITION BY '(' partition_item_list ')'
 { 
 $$ = cat_str(3,mm_strdup("partition by ("),$4,mm_strdup(")"));
}
;


 partition_item_list:
 partition_item
 { 
 $$ = $1;
}
|  partition_item_list ',' partition_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 partition_item:
 ColId
 { 
 $$ = $1;
}
;


 AlterForeignTableStmt:
 ALTER FOREIGN TABLE relation_expr alter_table_cmds
 { 
 $$ = cat_str(3,mm_strdup("alter foreign table"),$4,$5);
}
|  ALTER FOREIGN TABLE IF_P EXISTS relation_expr alter_table_cmds
 { 
 $$ = cat_str(3,mm_strdup("alter foreign table if exists"),$6,$7);
}
|  ALTER FOREIGN TABLE relation_expr MODIFY_P '(' modify_column_cmds ')'
 { 
 $$ = cat_str(5,mm_strdup("alter foreign table"),$4,mm_strdup("modify ("),$7,mm_strdup(")"));
}
|  ALTER FOREIGN TABLE IF_P EXISTS relation_expr MODIFY_P '(' modify_column_cmds ')'
 { 
 $$ = cat_str(5,mm_strdup("alter foreign table if exists"),$6,mm_strdup("modify ("),$9,mm_strdup(")"));
}
;


 CreateUserMappingStmt:
 CREATE USER MAPPING FOR auth_ident SERVER name create_generic_options
 { 
 $$ = cat_str(5,mm_strdup("create user mapping for"),$5,mm_strdup("server"),$7,$8);
}
;


 auth_ident:
 CURRENT_USER
 { 
 $$ = mm_strdup("current_user");
}
|  USER
 { 
 $$ = mm_strdup("user");
}
|  RoleId
 { 
 $$ = $1;
}
;


 DropUserMappingStmt:
 DROP USER MAPPING FOR auth_ident SERVER name
 { 
 $$ = cat_str(4,mm_strdup("drop user mapping for"),$5,mm_strdup("server"),$7);
}
|  DROP USER MAPPING IF_P EXISTS FOR auth_ident SERVER name
 { 
 $$ = cat_str(4,mm_strdup("drop user mapping if exists for"),$7,mm_strdup("server"),$9);
}
;


 AlterUserMappingStmt:
 ALTER USER MAPPING FOR auth_ident SERVER name alter_generic_options
 { 
 $$ = cat_str(5,mm_strdup("alter user mapping for"),$5,mm_strdup("server"),$7,$8);
}
;


 CreateModelStmt:
 CREATE MODEL ColId USING ColId features_clause target_clause from_clause with_hyperparameters_clause
 { 
 $$ = cat_str(8,mm_strdup("create model"),$3,mm_strdup("using"),$5,$6,$7,$8,$9);
}
;


 features_clause:
 FEATURES target_list
 { 
 $$ = cat_str(2,mm_strdup("features"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 target_clause:
 TARGET target_list
 { 
 $$ = cat_str(2,mm_strdup("target"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 with_hyperparameters_clause:
 WITH hyperparameter_name_value_list
 { 
 $$ = cat_str(2,mm_strdup("with"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 hyperparameter_name_value_list:
 hyperparameter_name_value
 { 
 $$ = $1;
}
|  hyperparameter_name_value_list ',' hyperparameter_name_value
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 hyperparameter_name_value:
 ColLabel '=' var_value
 { 
 $$ = cat_str(3,$1,mm_strdup("="),$3);
}
|  ColLabel '=' DEFAULT
 { 
 $$ = cat_str(2,$1,mm_strdup("= default"));
}
;


 DropModelStmt:
 DROP MODEL ColId opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("drop model"),$3,$4);
}
;


 CreateRlsPolicyStmt:
 CREATE RowLevelSecurityPolicyName ON qualified_name RLSDefaultPermissive RLSDefaultForCmd RLSDefaultToRole RLSOptionalUsingExpr
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(8,mm_strdup("create"),$2,mm_strdup("on"),$4,$5,$6,$7,$8);
}
;


 AlterRlsPolicyStmt:
 ALTER RowLevelSecurityPolicyName ON qualified_name RLSOptionalToRole RLSOptionalUsingExpr
 { 
 $$ = cat_str(6,mm_strdup("alter"),$2,mm_strdup("on"),$4,$5,$6);
}
;


 DropRlsPolicyStmt:
 DROP RowLevelSecurityPolicyName ON any_name opt_drop_behavior
 { 
 $$ = cat_str(5,mm_strdup("drop"),$2,mm_strdup("on"),$4,$5);
}
|  DROP POLICY IF_P EXISTS name ON any_name opt_drop_behavior
 { 
 $$ = cat_str(5,mm_strdup("drop policy if exists"),$5,mm_strdup("on"),$7,$8);
}
|  DROP ROW LEVEL SECURITY POLICY IF_P EXISTS name ON any_name opt_drop_behavior
 { 
 $$ = cat_str(5,mm_strdup("drop row level security policy if exists"),$8,mm_strdup("on"),$10,$11);
}
;


 RowLevelSecurityPolicyName:
 ROW LEVEL SECURITY POLICY name
 { 
 $$ = cat_str(2,mm_strdup("row level security policy"),$5);
}
|  POLICY name
 { 
 $$ = cat_str(2,mm_strdup("policy"),$2);
}
;


 RLSOptionalUsingExpr:
 USING '(' a_expr ')'
 { 
 $$ = cat_str(3,mm_strdup("using ("),$3,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 RLSDefaultToRole:
 TO row_level_security_role_list
 { 
 $$ = cat_str(2,mm_strdup("to"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 RLSOptionalToRole:
 TO row_level_security_role_list
 { 
 $$ = cat_str(2,mm_strdup("to"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 row_level_security_role_list:
 row_level_security_role
 { 
 $$ = $1;
}
|  row_level_security_role_list ',' row_level_security_role
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 row_level_security_role:
 RoleId
 { 
 $$ = $1;
}
|  CURRENT_USER
 { 
 $$ = mm_strdup("current_user");
}
|  SESSION_USER
 { 
 $$ = mm_strdup("session_user");
}
;


 RLSDefaultPermissive:
 AS ecpg_ident
 { 
 $$ = cat_str(2,mm_strdup("as"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 RLSDefaultForCmd:
 FOR row_level_security_cmd
 { 
 $$ = cat_str(2,mm_strdup("for"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 row_level_security_cmd:
 ALL
 { 
 $$ = mm_strdup("all");
}
|  SELECT
 { 
 $$ = mm_strdup("select");
}
|  UPDATE
 { 
 $$ = mm_strdup("update");
}
|  DELETE_P
 { 
 $$ = mm_strdup("delete");
}
|  INSERT
 { 
 $$ = mm_strdup("insert");
}
|  MERGE
 { 
 $$ = mm_strdup("merge");
}
;


 CreateSynonymStmt:
 CREATE opt_or_replace SYNONYM any_name FOR any_name
 { 
 $$ = cat_str(6,mm_strdup("create"),$2,mm_strdup("synonym"),$4,mm_strdup("for"),$6);
}
;


 DropSynonymStmt:
 DROP SYNONYM any_name opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("drop synonym"),$3,$4);
}
|  DROP SYNONYM IF_P EXISTS any_name opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("drop synonym if exists"),$5,$6);
}
;


 CreateDataSourceStmt:
 CREATE DATA_P SOURCE_P name opt_data_source_type opt_data_source_version create_generic_options
 { 
 $$ = cat_str(5,mm_strdup("create data source"),$4,$5,$6,$7);
}
;


 data_source_type:
 TYPE_P ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("type"),$2);
}
;


 opt_data_source_type:
 data_source_type
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 data_source_version:
 VERSION_P ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("version"),$2);
}
|  VERSION_P NULL_P
 { 
 $$ = mm_strdup("version null");
}
;


 opt_data_source_version:
 data_source_version
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 AlterDataSourceStmt:
 ALTER DATA_P SOURCE_P name data_source_type data_source_version alter_generic_options
 { 
 $$ = cat_str(5,mm_strdup("alter data source"),$4,$5,$6,$7);
}
|  ALTER DATA_P SOURCE_P name data_source_type data_source_version
 { 
 $$ = cat_str(4,mm_strdup("alter data source"),$4,$5,$6);
}
|  ALTER DATA_P SOURCE_P name data_source_type alter_generic_options
 { 
 $$ = cat_str(4,mm_strdup("alter data source"),$4,$5,$6);
}
|  ALTER DATA_P SOURCE_P name data_source_version alter_generic_options
 { 
 $$ = cat_str(4,mm_strdup("alter data source"),$4,$5,$6);
}
|  ALTER DATA_P SOURCE_P name data_source_type
 { 
 $$ = cat_str(3,mm_strdup("alter data source"),$4,$5);
}
|  ALTER DATA_P SOURCE_P name data_source_version
 { 
 $$ = cat_str(3,mm_strdup("alter data source"),$4,$5);
}
|  ALTER DATA_P SOURCE_P name alter_generic_options
 { 
 $$ = cat_str(3,mm_strdup("alter data source"),$4,$5);
}
;


 DropDataSourceStmt:
 DROP DATA_P SOURCE_P name opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("drop data source"),$4,$5);
}
|  DROP DATA_P SOURCE_P IF_P EXISTS name opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("drop data source if exists"),$6,$7);
}
;


 CreateTrigStmt:
 CREATE opt_or_replace definer_user TRIGGER name TriggerActionTime TriggerEvents ON qualified_name TriggerForSpec TriggerWhen EXECUTE PROCEDURE func_name '(' TriggerFuncArgs ')'
 { 
 $$ = cat_str(16,mm_strdup("create"),$2,$3,mm_strdup("trigger"),$5,$6,$7,mm_strdup("on"),$9,$10,$11,mm_strdup("execute procedure"),$14,mm_strdup("("),$16,mm_strdup(")"));
}
|  CREATE CONSTRAINT TRIGGER name AFTER TriggerEvents ON qualified_name OptConstrFromTable ConstraintAttributeSpec FOR EACH ROW TriggerWhen EXECUTE PROCEDURE func_name '(' TriggerFuncArgs ')'
 { 
 $$ = cat_str(15,mm_strdup("create constraint trigger"),$4,mm_strdup("after"),$6,mm_strdup("on"),$8,$9,$10,mm_strdup("for each row"),$14,mm_strdup("execute procedure"),$17,mm_strdup("("),$19,mm_strdup(")"));
}
|  CREATE opt_or_replace definer_user TRIGGER name TriggerActionTime TriggerEvents ON qualified_name TriggerForSpec TriggerWhen trigger_order subprogram_body
 { 
 $$ = cat_str(13,mm_strdup("create"),$2,$3,mm_strdup("trigger"),$5,$6,$7,mm_strdup("on"),$9,$10,$11,$12,$13);
}
|  CREATE opt_or_replace definer_user TRIGGER IF_P NOT EXISTS name TriggerActionTime TriggerEvents ON qualified_name TriggerForSpec TriggerWhen trigger_order subprogram_body
 { 
 $$ = cat_str(13,mm_strdup("create"),$2,$3,mm_strdup("trigger if not exists"),$8,$9,$10,mm_strdup("on"),$12,$13,$14,$15,$16);
}
;


 TriggerActionTime:
 BEFORE
 { 
 $$ = mm_strdup("before");
}
|  AFTER
 { 
 $$ = mm_strdup("after");
}
|  INSTEAD OF
 { 
 $$ = mm_strdup("instead of");
}
;


 TriggerEvents:
 TriggerOneEvent
 { 
 $$ = $1;
}
|  TriggerEvents OR TriggerOneEvent
 { 
 $$ = cat_str(3,$1,mm_strdup("or"),$3);
}
;


 TriggerOneEvent:
 INSERT
 { 
 $$ = mm_strdup("insert");
}
|  DELETE_P
 { 
 $$ = mm_strdup("delete");
}
|  UPDATE
 { 
 $$ = mm_strdup("update");
}
|  UPDATE OF columnList
 { 
 $$ = cat_str(2,mm_strdup("update of"),$3);
}
|  TRUNCATE
 { 
 $$ = mm_strdup("truncate");
}
;


 TriggerForSpec:
 FOR TriggerForOptEach TriggerForType
 { 
 $$ = cat_str(3,mm_strdup("for"),$2,$3);
}
| 
 { 
 $$=EMPTY; }
;


 TriggerForOptEach:
 EACH
 { 
 $$ = mm_strdup("each");
}
| 
 { 
 $$=EMPTY; }
;


 TriggerForType:
 ROW
 { 
 $$ = mm_strdup("row");
}
|  STATEMENT
 { 
 $$ = mm_strdup("statement");
}
;


 TriggerWhen:
 WHEN '(' a_expr ')'
 { 
 $$ = cat_str(3,mm_strdup("when ("),$3,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 TriggerFuncArgs:
 TriggerFuncArg
 { 
 $$ = $1;
}
|  TriggerFuncArgs ',' TriggerFuncArg
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
| 
 { 
 $$=EMPTY; }
;


 TriggerFuncArg:
 Iconst
 { 
 $$ = $1;
}
|  ecpg_fconst
 { 
 $$ = $1;
}
|  ecpg_sconst
 { 
 $$ = $1;
}
|  ColLabel
 { 
 $$ = $1;
}
;


 trigger_order:

 { 
 $$=EMPTY; }
|  FOLLOWS_P ColId
 { 
 $$ = cat_str(2,mm_strdup("follows"),$2);
}
|  PRECEDES_P ColId
 { 
 $$ = cat_str(2,mm_strdup("precedes"),$2);
}
;


 OptConstrFromTable:
 FROM qualified_name
 { 
 $$ = cat_str(2,mm_strdup("from"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 ConstraintAttributeSpec:

 { 
 $$=EMPTY; }
|  ConstraintAttributeSpec ConstraintAttributeElem
 { 
 $$ = cat_str(2,$1,$2);
}
;


 ConstraintAttributeElem:
 NOT DEFERRABLE
 { 
 $$ = mm_strdup("not deferrable");
}
|  DEFERRABLE
 { 
 $$ = mm_strdup("deferrable");
}
|  INITIALLY IMMEDIATE
 { 
 $$ = mm_strdup("initially immediate");
}
|  INITIALLY DEFERRED
 { 
 $$ = mm_strdup("initially deferred");
}
|  NOT VALID
 { 
 $$ = mm_strdup("not valid");
}
|  NO INHERIT
 { 
 $$ = mm_strdup("no inherit");
}
;


 CreateEventTrigStmt:
 CREATE EVENT_TRIGGER name ON ColLabel EXECUTE PROCEDURE func_name '(' ')'
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(7,mm_strdup("create event_trigger"),$3,mm_strdup("on"),$5,mm_strdup("execute procedure"),$8,mm_strdup("( )"));
}
|  CREATE EVENT_TRIGGER name ON ColLabel WHEN event_trigger_when_list EXECUTE PROCEDURE func_name '(' ')'
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(9,mm_strdup("create event_trigger"),$3,mm_strdup("on"),$5,mm_strdup("when"),$7,mm_strdup("execute procedure"),$10,mm_strdup("( )"));
}
;


 event_trigger_when_list:
 event_trigger_when_item
 { 
 $$ = $1;
}
|  event_trigger_when_list AND event_trigger_when_item
 { 
 $$ = cat_str(3,$1,mm_strdup("and"),$3);
}
;


 event_trigger_when_item:
 ColId IN_P '(' event_trigger_value_list ')'
 { 
 $$ = cat_str(4,$1,mm_strdup("in ("),$4,mm_strdup(")"));
}
;


 event_trigger_value_list:
 SCONST
 { 
 $$ = mm_strdup("sconst");
}
|  event_trigger_value_list ',' SCONST
 { 
 $$ = cat_str(2,$1,mm_strdup(", sconst"));
}
;


 AlterEventTrigStmt:
 ALTER EVENT_TRIGGER name enable_trigger
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,mm_strdup("alter event_trigger"),$3,$4);
}
;


 enable_trigger:
 ENABLE_P
 { 
 $$ = mm_strdup("enable");
}
|  ENABLE_P REPLICA
 { 
 $$ = mm_strdup("enable replica");
}
|  ENABLE_P ALWAYS
 { 
 $$ = mm_strdup("enable always");
}
|  DISABLE_P
 { 
 $$ = mm_strdup("disable");
}
;


 DropTrigStmt:
 DROP TRIGGER name ON any_name opt_drop_behavior
 { 
 $$ = cat_str(5,mm_strdup("drop trigger"),$3,mm_strdup("on"),$5,$6);
}
|  DROP TRIGGER IF_P EXISTS name ON any_name opt_drop_behavior
 { 
 $$ = cat_str(5,mm_strdup("drop trigger if exists"),$5,mm_strdup("on"),$7,$8);
}
|  DROP TRIGGER name opt_drop_behavior
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,mm_strdup("drop trigger"),$3,$4);
}
|  DROP TRIGGER IF_P EXISTS name opt_drop_behavior
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,mm_strdup("drop trigger if exists"),$5,$6);
}
;


 CreateAssertStmt:
 CREATE ASSERTION name CHECK '(' a_expr ')' ConstraintAttributeSpec
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(6,mm_strdup("create assertion"),$3,mm_strdup("check ("),$6,mm_strdup(")"),$8);
}
;


 DropAssertStmt:
 DROP ASSERTION name opt_drop_behavior
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,mm_strdup("drop assertion"),$3,$4);
}
;


 DefineStmt:
 CREATE AGGREGATE func_name aggr_args definition
 { 
 $$ = cat_str(4,mm_strdup("create aggregate"),$3,$4,$5);
}
|  CREATE AGGREGATE func_name old_aggr_definition
 { 
 $$ = cat_str(3,mm_strdup("create aggregate"),$3,$4);
}
|  CREATE OPERATOR any_operator definition
 { 
 $$ = cat_str(3,mm_strdup("create operator"),$3,$4);
}
|  CREATE TYPE_P any_name definition
 { 
 $$ = cat_str(3,mm_strdup("create type"),$3,$4);
}
|  CREATE TYPE_P any_name
 { 
 $$ = cat_str(2,mm_strdup("create type"),$3);
}
|  CREATE TYPE_P any_name as_is '(' OptTableFuncElementList ')'
 { 
 $$ = cat_str(6,mm_strdup("create type"),$3,$4,mm_strdup("("),$6,mm_strdup(")"));
}
|  CREATE TYPE_P any_name as_is TABLE OF func_type
 { 
 $$ = cat_str(5,mm_strdup("create type"),$3,$4,mm_strdup("table of"),$7);
}
|  CREATE TYPE_P any_name as_is ENUM_P '(' opt_enum_val_list ')'
 { 
 $$ = cat_str(6,mm_strdup("create type"),$3,$4,mm_strdup("enum ("),$7,mm_strdup(")"));
}
|  CREATE TYPE_P any_name as_is RANGE definition
 { 
 $$ = cat_str(5,mm_strdup("create type"),$3,$4,mm_strdup("range"),$6);
}
|  CREATE TEXT_P SEARCH PARSER any_name definition
 { 
 $$ = cat_str(3,mm_strdup("create text search parser"),$5,$6);
}
|  CREATE TEXT_P SEARCH DICTIONARY any_name definition
 { 
 $$ = cat_str(3,mm_strdup("create text search dictionary"),$5,$6);
}
|  CREATE TEXT_P SEARCH TEMPLATE any_name definition
 { 
 $$ = cat_str(3,mm_strdup("create text search template"),$5,$6);
}
|  CREATE TEXT_P SEARCH CONFIGURATION any_name tsconf_definition opt_cfoptions
 { 
 $$ = cat_str(4,mm_strdup("create text search configuration"),$5,$6,$7);
}
|  CREATE COLLATION any_name definition
 { 
 $$ = cat_str(3,mm_strdup("create collation"),$3,$4);
}
|  CREATE COLLATION any_name FROM any_name
 { 
 $$ = cat_str(4,mm_strdup("create collation"),$3,mm_strdup("from"),$5);
}
;


 opt_cfoptions:
 WITH cfoptions
 { 
 $$ = cat_str(2,mm_strdup("with"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 cfoptions:'('
 cfoption_list ')'
 { 
 $$ = cat_str(2,$1,mm_strdup(")"));
}
;


 cfoption_list:
 cfoption_elem
 { 
 $$ = $1;
}
|  cfoption_list ',' cfoption_elem
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 cfoption_elem:
 ColLabel '=' def_arg
 { 
 $$ = cat_str(3,$1,mm_strdup("="),$3);
}
|  ColLabel
 { 
 $$ = $1;
}
;


 tsconf_definition:
 '(' tsconf_def_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
;


 tsconf_def_list:
 tsconf_def_elem
 { 
 $$ = $1;
}
|  tsconf_def_list ',' tsconf_def_elem
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 tsconf_def_elem:
 PARSER '=' any_name
 { 
 $$ = cat_str(2,mm_strdup("parser ="),$3);
}
|  PARSER '=' DEFAULT
 { 
 $$ = mm_strdup("parser = default");
}
|  COPY '=' any_name
 { 
 $$ = cat_str(2,mm_strdup("copy ="),$3);
}
;


 definition:
 '(' def_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
;


 def_list:
 def_elem
 { 
 $$ = $1;
}
|  def_list ',' def_elem
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 def_elem:
 ColLabel '=' def_arg
 { 
 $$ = cat_str(3,$1,mm_strdup("="),$3);
}
|  ColLabel
 { 
 $$ = $1;
}
;


 def_arg:
 func_type
 { 
 $$ = $1;
}
|  reserved_keyword
 { 
 $$ = $1;
}
|  qual_all_Op
 { 
 $$ = $1;
}
|  NumericOnly
 { 
 $$ = $1;
}
|  ecpg_sconst
 { 
 $$ = $1;
}
;


 aggr_args:
 '(' type_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
|  '(' '*' ')'
 { 
 $$ = mm_strdup("( * )");
}
|  '(' ORDER BY type_list ')'
 { 
 $$ = cat_str(3,mm_strdup("( order by"),$4,mm_strdup(")"));
}
;


 old_aggr_definition:
 '(' old_aggr_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
;


 old_aggr_list:
 old_aggr_elem
 { 
 $$ = $1;
}
|  old_aggr_list ',' old_aggr_elem
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 old_aggr_elem:
 ecpg_ident '=' def_arg
 { 
 $$ = cat_str(3,$1,mm_strdup("="),$3);
}
;


 opt_enum_val_list:
 enum_val_list
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 enum_val_list:
 ecpg_sconst
 { 
 $$ = $1;
}
|  enum_val_list ',' ecpg_sconst
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 AlterEnumStmt:
 ALTER TYPE_P any_name ADD_P VALUE_P opt_if_not_exists ecpg_sconst
 { 
 $$ = cat_str(5,mm_strdup("alter type"),$3,mm_strdup("add value"),$6,$7);
}
|  ALTER TYPE_P any_name ADD_P VALUE_P opt_if_not_exists ecpg_sconst BEFORE ecpg_sconst
 { 
 $$ = cat_str(7,mm_strdup("alter type"),$3,mm_strdup("add value"),$6,$7,mm_strdup("before"),$9);
}
|  ALTER TYPE_P any_name ADD_P VALUE_P opt_if_not_exists ecpg_sconst AFTER ecpg_sconst
 { 
 $$ = cat_str(7,mm_strdup("alter type"),$3,mm_strdup("add value"),$6,$7,mm_strdup("after"),$9);
}
|  ALTER TYPE_P any_name RENAME VALUE_P ecpg_sconst TO ecpg_sconst
 { 
 $$ = cat_str(6,mm_strdup("alter type"),$3,mm_strdup("rename value"),$6,mm_strdup("to"),$8);
}
;


 opt_if_not_exists:
 IF_P NOT EXISTS
 { 
 $$ = mm_strdup("if not exists");
}
| 
 { 
 $$=EMPTY; }
;


 CreateOpClassStmt:
 CREATE OPERATOR CLASS any_name opt_default FOR TYPE_P Typename USING access_method opt_opfamily AS opclass_item_list
 { 
 $$ = cat_str(10,mm_strdup("create operator class"),$4,$5,mm_strdup("for type"),$8,mm_strdup("using"),$10,$11,mm_strdup("as"),$13);
}
;


 opclass_item_list:
 opclass_item
 { 
 $$ = $1;
}
|  opclass_item_list ',' opclass_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 opclass_item:
 OPERATOR Iconst any_operator opclass_purpose opt_recheck
 { 
 $$ = cat_str(5,mm_strdup("operator"),$2,$3,$4,$5);
}
|  OPERATOR Iconst any_operator oper_argtypes opclass_purpose opt_recheck
 { 
 $$ = cat_str(6,mm_strdup("operator"),$2,$3,$4,$5,$6);
}
|  FUNCTION Iconst func_name func_args
 { 
 $$ = cat_str(4,mm_strdup("function"),$2,$3,$4);
}
|  FUNCTION Iconst '(' type_list ')' func_name func_args
 { 
 $$ = cat_str(7,mm_strdup("function"),$2,mm_strdup("("),$4,mm_strdup(")"),$6,$7);
}
|  STORAGE Typename
 { 
 $$ = cat_str(2,mm_strdup("storage"),$2);
}
;


 opt_default:
 DEFAULT
 { 
 $$ = mm_strdup("default");
}
| 
 { 
 $$=EMPTY; }
;


 opt_opfamily:
 FAMILY any_name
 { 
 $$ = cat_str(2,mm_strdup("family"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 opclass_purpose:
 FOR SEARCH
 { 
 $$ = mm_strdup("for search");
}
|  FOR ORDER BY any_name
 { 
 $$ = cat_str(2,mm_strdup("for order by"),$4);
}
| 
 { 
 $$=EMPTY; }
;


 opt_recheck:
 RECHECK
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = mm_strdup("recheck");
}
| 
 { 
 $$=EMPTY; }
;


 CreateOpFamilyStmt:
 CREATE OPERATOR FAMILY any_name USING access_method
 { 
 $$ = cat_str(4,mm_strdup("create operator family"),$4,mm_strdup("using"),$6);
}
;


 AlterOpFamilyStmt:
 ALTER OPERATOR FAMILY any_name USING access_method ADD_P opclass_item_list
 { 
 $$ = cat_str(6,mm_strdup("alter operator family"),$4,mm_strdup("using"),$6,mm_strdup("add"),$8);
}
|  ALTER OPERATOR FAMILY any_name USING access_method DROP opclass_drop_list
 { 
 $$ = cat_str(6,mm_strdup("alter operator family"),$4,mm_strdup("using"),$6,mm_strdup("drop"),$8);
}
;


 opclass_drop_list:
 opclass_drop
 { 
 $$ = $1;
}
|  opclass_drop_list ',' opclass_drop
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 opclass_drop:
 OPERATOR Iconst '(' type_list ')'
 { 
 $$ = cat_str(5,mm_strdup("operator"),$2,mm_strdup("("),$4,mm_strdup(")"));
}
|  FUNCTION Iconst '(' type_list ')'
 { 
 $$ = cat_str(5,mm_strdup("function"),$2,mm_strdup("("),$4,mm_strdup(")"));
}
;


 DropOpClassStmt:
 DROP OPERATOR CLASS any_name USING access_method opt_drop_behavior
 { 
 $$ = cat_str(5,mm_strdup("drop operator class"),$4,mm_strdup("using"),$6,$7);
}
|  DROP OPERATOR CLASS IF_P EXISTS any_name USING access_method opt_drop_behavior
 { 
 $$ = cat_str(5,mm_strdup("drop operator class if exists"),$6,mm_strdup("using"),$8,$9);
}
;


 DropOpFamilyStmt:
 DROP OPERATOR FAMILY any_name USING access_method opt_drop_behavior
 { 
 $$ = cat_str(5,mm_strdup("drop operator family"),$4,mm_strdup("using"),$6,$7);
}
|  DROP OPERATOR FAMILY IF_P EXISTS any_name USING access_method opt_drop_behavior
 { 
 $$ = cat_str(5,mm_strdup("drop operator family if exists"),$6,mm_strdup("using"),$8,$9);
}
;


 DropOwnedStmt:
 DROP OWNED BY name_list opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("drop owned by"),$4,$5);
}
;


 ReassignOwnedStmt:
 REASSIGN OWNED BY name_list TO name
 { 
 $$ = cat_str(4,mm_strdup("reassign owned by"),$4,mm_strdup("to"),$6);
}
;


 DropStmt:
 DROP drop_type IF_P EXISTS any_name_list opt_drop_behavior opt_purge
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(6,mm_strdup("drop"),$2,mm_strdup("if exists"),$5,$6,$7);
}
|  DROP drop_type any_name_list opt_drop_behavior opt_purge
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(5,mm_strdup("drop"),$2,$3,$4,$5);
}
|  DROP TYPE_P type_name_list opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("drop type"),$3,$4);
}
|  DROP TYPE_P IF_P EXISTS type_name_list opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("drop type if exists"),$5,$6);
}
|  DROP DOMAIN_P type_name_list opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("drop domain"),$3,$4);
}
|  DROP DOMAIN_P IF_P EXISTS type_name_list opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("drop domain if exists"),$5,$6);
}
|  DROP INDEX CONCURRENTLY any_name_list opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("drop index concurrently"),$4,$5);
}
|  DROP INDEX CONCURRENTLY IF_P EXISTS any_name_list opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("drop index concurrently if exists"),$6,$7);
}
;


 opt_purge:
 PURGE
 { 
 $$ = mm_strdup("purge");
}
| 
 { 
 $$=EMPTY; }
;


 drop_type:
 TABLE
 { 
 $$ = mm_strdup("table");
}
|  CONTVIEW
 { 
 $$ = mm_strdup("contview");
}
|  STREAM
 { 
 $$ = mm_strdup("stream");
}
|  SEQUENCE
 { 
 $$ = mm_strdup("sequence");
}
|  LARGE_P SEQUENCE
 { 
 $$ = mm_strdup("large sequence");
}
|  VIEW
 { 
 $$ = mm_strdup("view");
}
|  MATERIALIZED VIEW
 { 
 $$ = mm_strdup("materialized view");
}
|  INDEX
 { 
 $$ = mm_strdup("index");
}
|  FOREIGN TABLE
 { 
 $$ = mm_strdup("foreign table");
}
|  COLLATION
 { 
 $$ = mm_strdup("collation");
}
|  CONVERSION_P
 { 
 $$ = mm_strdup("conversion");
}
|  SCHEMA
 { 
 $$ = mm_strdup("schema");
}
|  EVENT_TRIGGER
 { 
 $$ = mm_strdup("event_trigger");
}
|  EXTENSION
 { 
 $$ = mm_strdup("extension");
}
|  TEXT_P SEARCH PARSER
 { 
 $$ = mm_strdup("text search parser");
}
|  TEXT_P SEARCH DICTIONARY
 { 
 $$ = mm_strdup("text search dictionary");
}
|  TEXT_P SEARCH TEMPLATE
 { 
 $$ = mm_strdup("text search template");
}
|  TEXT_P SEARCH CONFIGURATION
 { 
 $$ = mm_strdup("text search configuration");
}
|  CLIENT MASTER KEY
 { 
 $$ = mm_strdup("client master key");
}
|  COLUMN ENCRYPTION KEY
 { 
 $$ = mm_strdup("column encryption key");
}
|  PUBLICATION
 { 
 $$ = mm_strdup("publication");
}
;


 collate_name:
 any_name
 { 
 $$ = $1;
}
|  BINARY
 { 
 $$ = mm_strdup("binary");
}
|  ecpg_sconst
 { 
 $$ = $1;
}
;


 type_name_list:
 Typename
 { 
 $$ = $1;
}
|  type_name_list ',' Typename any_name_list:
 any_name
 { 
 $$ = $1;
}
|  any_name_list ',' any_name
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 any_name:
 ColId
 { 
 $$ = $1;
}
|  ColId attrs
 { 
 $$ = cat_str(2,$1,$2);
}
;


 attrs:
 '.' attr_name
 { 
 $$ = cat_str(2,mm_strdup("."),$2);
}
|  attrs '.' attr_name
 { 
 $$ = cat_str(3,$1,mm_strdup("."),$3);
}
;


 TruncateStmt:
 TRUNCATE opt_table relation_expr_list opt_restart_seqs opt_drop_behavior opt_purge
 { 
 $$ = cat_str(6,mm_strdup("truncate"),$2,$3,$4,$5,$6);
}
;


 opt_restart_seqs:
 CONTINUE_P IDENTITY_P
 { 
 $$ = mm_strdup("continue identity");
}
|  RESTART IDENTITY_P
 { 
 $$ = mm_strdup("restart identity");
}
| 
 { 
 $$=EMPTY; }
;


 CommentStmt:
 COMMENT ON comment_type any_name IS comment_text
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(5,mm_strdup("comment on"),$3,$4,mm_strdup("is"),$6);
}
|  COMMENT ON AGGREGATE func_name aggr_args IS comment_text
 { 
 $$ = cat_str(5,mm_strdup("comment on aggregate"),$4,$5,mm_strdup("is"),$7);
}
|  COMMENT ON FUNCTION func_name func_args IS comment_text
 { 
 $$ = cat_str(5,mm_strdup("comment on function"),$4,$5,mm_strdup("is"),$7);
}
|  COMMENT ON OPERATOR any_operator oper_argtypes IS comment_text
 { 
 $$ = cat_str(5,mm_strdup("comment on operator"),$4,$5,mm_strdup("is"),$7);
}
|  COMMENT ON CONSTRAINT name ON any_name IS comment_text
 { 
 $$ = cat_str(6,mm_strdup("comment on constraint"),$4,mm_strdup("on"),$6,mm_strdup("is"),$8);
}
|  COMMENT ON CONSTRAINT name ON DOMAIN_P any_name IS comment_text
 { 
 $$ = cat_str(6,mm_strdup("comment on constraint"),$4,mm_strdup("on domain"),$7,mm_strdup("is"),$9);
}
|  COMMENT ON RULE name ON any_name IS comment_text
 { 
 $$ = cat_str(6,mm_strdup("comment on rule"),$4,mm_strdup("on"),$6,mm_strdup("is"),$8);
}
|  COMMENT ON RULE name IS comment_text
 { 
 $$ = cat_str(4,mm_strdup("comment on rule"),$4,mm_strdup("is"),$6);
}
|  COMMENT ON TRIGGER name ON any_name IS comment_text
 { 
 $$ = cat_str(6,mm_strdup("comment on trigger"),$4,mm_strdup("on"),$6,mm_strdup("is"),$8);
}
|  COMMENT ON OPERATOR CLASS any_name USING access_method IS comment_text
 { 
 $$ = cat_str(6,mm_strdup("comment on operator class"),$5,mm_strdup("using"),$7,mm_strdup("is"),$9);
}
|  COMMENT ON OPERATOR FAMILY any_name USING access_method IS comment_text
 { 
 $$ = cat_str(6,mm_strdup("comment on operator family"),$5,mm_strdup("using"),$7,mm_strdup("is"),$9);
}
|  COMMENT ON LARGE_P OBJECT_P NumericOnly IS comment_text
 { 
 $$ = cat_str(4,mm_strdup("comment on large object"),$5,mm_strdup("is"),$7);
}
|  COMMENT ON CAST '(' Typename AS Typename ')' IS comment_text
 { 
 $$ = cat_str(6,mm_strdup("comment on cast ("),$5,mm_strdup("as"),$7,mm_strdup(") is"),$10);
}
|  COMMENT ON opt_procedural LANGUAGE any_name IS comment_text
 { 
 $$ = cat_str(6,mm_strdup("comment on"),$3,mm_strdup("language"),$5,mm_strdup("is"),$7);
}
|  COMMENT ON TEXT_P SEARCH PARSER any_name IS comment_text
 { 
 $$ = cat_str(4,mm_strdup("comment on text search parser"),$6,mm_strdup("is"),$8);
}
|  COMMENT ON TEXT_P SEARCH DICTIONARY any_name IS comment_text
 { 
 $$ = cat_str(4,mm_strdup("comment on text search dictionary"),$6,mm_strdup("is"),$8);
}
|  COMMENT ON TEXT_P SEARCH TEMPLATE any_name IS comment_text
 { 
 $$ = cat_str(4,mm_strdup("comment on text search template"),$6,mm_strdup("is"),$8);
}
|  COMMENT ON TEXT_P SEARCH CONFIGURATION any_name IS comment_text
 { 
 $$ = cat_str(4,mm_strdup("comment on text search configuration"),$6,mm_strdup("is"),$8);
}
;


 comment_type:
 COLUMN
 { 
 $$ = mm_strdup("column");
}
|  DATABASE
 { 
 $$ = mm_strdup("database");
}
|  SCHEMA
 { 
 $$ = mm_strdup("schema");
}
|  INDEX
 { 
 $$ = mm_strdup("index");
}
|  SEQUENCE
 { 
 $$ = mm_strdup("sequence");
}
|  LARGE_P SEQUENCE
 { 
 $$ = mm_strdup("large sequence");
}
|  TABLE
 { 
 $$ = mm_strdup("table");
}
|  DOMAIN_P
 { 
 $$ = mm_strdup("domain");
}
|  TYPE_P
 { 
 $$ = mm_strdup("type");
}
|  VIEW
 { 
 $$ = mm_strdup("view");
}
|  MATERIALIZED VIEW
 { 
 $$ = mm_strdup("materialized view");
}
|  SNAPSHOT
 { 
 $$ = mm_strdup("snapshot");
}
|  COLLATION
 { 
 $$ = mm_strdup("collation");
}
|  CONVERSION_P
 { 
 $$ = mm_strdup("conversion");
}
|  TABLESPACE
 { 
 $$ = mm_strdup("tablespace");
}
|  EVENT_TRIGGER
 { 
 $$ = mm_strdup("event_trigger");
}
|  EXTENSION
 { 
 $$ = mm_strdup("extension");
}
|  ROLE
 { 
 $$ = mm_strdup("role");
}
|  USER
 { 
 $$ = mm_strdup("user");
}
|  FOREIGN TABLE
 { 
 $$ = mm_strdup("foreign table");
}
|  SERVER
 { 
 $$ = mm_strdup("server");
}
|  FOREIGN DATA_P WRAPPER
 { 
 $$ = mm_strdup("foreign data wrapper");
}
;


 comment_text:
 ecpg_sconst
 { 
 $$ = $1;
}
|  NULL_P
 { 
 $$ = mm_strdup("null");
}
;


 SecLabelStmt:
 SECURITY LABEL opt_provider ON security_label_type any_name IS security_label
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(7,mm_strdup("security label"),$3,mm_strdup("on"),$5,$6,mm_strdup("is"),$8);
}
|  SECURITY LABEL opt_provider ON AGGREGATE func_name aggr_args IS security_label
 { 
 $$ = cat_str(7,mm_strdup("security label"),$3,mm_strdup("on aggregate"),$6,$7,mm_strdup("is"),$9);
}
|  SECURITY LABEL opt_provider ON FUNCTION func_name func_args IS security_label
 { 
 $$ = cat_str(7,mm_strdup("security label"),$3,mm_strdup("on function"),$6,$7,mm_strdup("is"),$9);
}
|  SECURITY LABEL opt_provider ON LARGE_P OBJECT_P NumericOnly IS security_label
 { 
 $$ = cat_str(6,mm_strdup("security label"),$3,mm_strdup("on large object"),$7,mm_strdup("is"),$9);
}
|  SECURITY LABEL opt_provider ON opt_procedural LANGUAGE any_name IS security_label
 { 
 $$ = cat_str(8,mm_strdup("security label"),$3,mm_strdup("on"),$5,mm_strdup("language"),$7,mm_strdup("is"),$9);
}
;


 opt_provider:
 FOR ColId_or_Sconst
 { 
 $$ = cat_str(2,mm_strdup("for"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 security_label_type:
 COLUMN
 { 
 $$ = mm_strdup("column");
}
|  DATABASE
 { 
 $$ = mm_strdup("database");
}
|  EVENT_TRIGGER
 { 
 $$ = mm_strdup("event_trigger");
}
|  FOREIGN TABLE
 { 
 $$ = mm_strdup("foreign table");
}
|  SCHEMA
 { 
 $$ = mm_strdup("schema");
}
|  SEQUENCE
 { 
 $$ = mm_strdup("sequence");
}
|  TABLE
 { 
 $$ = mm_strdup("table");
}
|  DOMAIN_P
 { 
 $$ = mm_strdup("domain");
}
|  ROLE
 { 
 $$ = mm_strdup("role");
}
|  USER
 { 
 $$ = mm_strdup("user");
}
|  TABLESPACE
 { 
 $$ = mm_strdup("tablespace");
}
|  TYPE_P
 { 
 $$ = mm_strdup("type");
}
|  VIEW
 { 
 $$ = mm_strdup("view");
}
|  MATERIALIZED VIEW
 { 
 $$ = mm_strdup("materialized view");
}
;


 security_label:
 ecpg_sconst
 { 
 $$ = $1;
}
|  NULL_P
 { 
 $$ = mm_strdup("null");
}
;


 FetchStmt:
 FETCH fetch_args
 { 
 $$ = cat_str(2,mm_strdup("fetch"),$2);
}
|  MOVE fetch_args
 { 
 $$ = cat_str(2,mm_strdup("move"),$2);
}
	| FETCH fetch_args ecpg_fetch_into
	{
		$$ = cat2_str(mm_strdup("fetch"), $2);
	}
	| FETCH FORWARD cursor_name opt_ecpg_fetch_into
	{
		char *cursor_marker = $3[0] == ':' ? mm_strdup("$0") : $3;
		add_additional_variables($3, false);
		$$ = cat_str(2, mm_strdup("fetch forward"), cursor_marker);
	}
	| FETCH FORWARD from_in cursor_name opt_ecpg_fetch_into
	{
		char *cursor_marker = $4[0] == ':' ? mm_strdup("$0") : $4;
		add_additional_variables($4, false);
		$$ = cat_str(2, mm_strdup("fetch forward from"), cursor_marker);
	}
	| FETCH BACKWARD cursor_name opt_ecpg_fetch_into
	{
		char *cursor_marker = $3[0] == ':' ? mm_strdup("$0") : $3;
		add_additional_variables($3, false);
		$$ = cat_str(2, mm_strdup("fetch backward"), cursor_marker);
	}
	| FETCH BACKWARD from_in cursor_name opt_ecpg_fetch_into
	{
		char *cursor_marker = $4[0] == ':' ? mm_strdup("$0") : $4;
		add_additional_variables($4, false);
		$$ = cat_str(2, mm_strdup("fetch backward from"), cursor_marker);
	}
	| MOVE FORWARD cursor_name
	{
		char *cursor_marker = $3[0] == ':' ? mm_strdup("$0") : $3;
		add_additional_variables($3, false);
		$$ = cat_str(2, mm_strdup("move forward"), cursor_marker);
	}
	| MOVE FORWARD from_in cursor_name
	{
		char *cursor_marker = $4[0] == ':' ? mm_strdup("$0") : $4;
		add_additional_variables($4, false);
		$$ = cat_str(2, mm_strdup("move forward from"), cursor_marker);
	}
	| MOVE BACKWARD cursor_name
	{
		char *cursor_marker = $3[0] == ':' ? mm_strdup("$0") : $3;
		add_additional_variables($3, false);
		$$ = cat_str(2, mm_strdup("move backward"), cursor_marker);
	}
	| MOVE BACKWARD from_in cursor_name
	{
		char *cursor_marker = $4[0] == ':' ? mm_strdup("$0") : $4;
		add_additional_variables($4, false);
		$$ = cat_str(2, mm_strdup("move backward from"), cursor_marker);
	}
;


 fetch_args:
 cursor_name
 { 
		add_additional_variables($1, false);
		if ($1[0] == ':')
		{
			free_current_memory($1);
			$1 = mm_strdup("$0");
		}

 $$ = $1;
}
|  from_in cursor_name
 { 
		add_additional_variables($2, false);
		if ($2[0] == ':')
		{
			free_current_memory($2);
			$2 = mm_strdup("$0");
		}

 $$ = cat_str(2,$1,$2);
}
|  NEXT opt_from_in cursor_name
 { 
		add_additional_variables($3, false);
		if ($3[0] == ':')
		{
			free_current_memory($3);
			$3 = mm_strdup("$0");
		}

 $$ = cat_str(3,mm_strdup("next"),$2,$3);
}
|  PRIOR opt_from_in cursor_name
 { 
		add_additional_variables($3, false);
		if ($3[0] == ':')
		{
			free_current_memory($3);
			$3 = mm_strdup("$0");
		}

 $$ = cat_str(3,mm_strdup("prior"),$2,$3);
}
|  FIRST_P opt_from_in cursor_name
 { 
		add_additional_variables($3, false);
		if ($3[0] == ':')
		{
			free_current_memory($3);
			$3 = mm_strdup("$0");
		}

 $$ = cat_str(3,mm_strdup("first"),$2,$3);
}
|  LAST_P opt_from_in cursor_name
 { 
		add_additional_variables($3, false);
		if ($3[0] == ':')
		{
			free_current_memory($3);
			$3 = mm_strdup("$0");
		}

 $$ = cat_str(3,mm_strdup("last"),$2,$3);
}
|  ABSOLUTE_P SignedIconst opt_from_in cursor_name
 { 
		add_additional_variables($4, false);
		if ($4[0] == ':')
		{
			free_current_memory($4);
			$4 = mm_strdup("$0");
		}
		if ($2[0] == '$')
		{
			free_current_memory($2);
			$2 = mm_strdup("$0");
		}

 $$ = cat_str(4,mm_strdup("absolute"),$2,$3,$4);
}
|  RELATIVE_P SignedIconst opt_from_in cursor_name
 { 
		add_additional_variables($4, false);
		if ($4[0] == ':')
		{
			free_current_memory($4);
			$4 = mm_strdup("$0");
		}
		if ($2[0] == '$')
		{
			free_current_memory($2);
			$2 = mm_strdup("$0");
		}

 $$ = cat_str(4,mm_strdup("relative"),$2,$3,$4);
}
|  SignedIconst opt_from_in cursor_name
 { 
		add_additional_variables($3, false);
		if ($3[0] == ':')
		{
			free_current_memory($3);
			$3 = mm_strdup("$0");
		}
		if ($1[0] == '$')
		{
			free_current_memory($1);
			$1 = mm_strdup("$0");
		}

 $$ = cat_str(3,$1,$2,$3);
}
|  ALL opt_from_in cursor_name
 { 
		add_additional_variables($3, false);
		if ($3[0] == ':')
		{
			free_current_memory($3);
			$3 = mm_strdup("$0");
		}

 $$ = cat_str(3,mm_strdup("all"),$2,$3);
}
|  FORWARD SignedIconst opt_from_in cursor_name
 { 
		add_additional_variables($4, false);
		if ($4[0] == ':')
		{
			free_current_memory($4);
			$4 = mm_strdup("$0");
		}
		if ($2[0] == '$')
		{
			free_current_memory($2);
			$2 = mm_strdup("$0");
		}

 $$ = cat_str(4,mm_strdup("forward"),$2,$3,$4);
}
|  FORWARD ALL opt_from_in cursor_name
 { 
		add_additional_variables($4, false);
		if ($4[0] == ':')
		{
			free_current_memory($4);
			$4 = mm_strdup("$0");
		}

 $$ = cat_str(3,mm_strdup("forward all"),$3,$4);
}
|  BACKWARD SignedIconst opt_from_in cursor_name
 { 
		add_additional_variables($4, false);
		if ($4[0] == ':')
		{
			free_current_memory($4);
			$4 = mm_strdup("$0");
		}
		if ($2[0] == '$')
		{
			free_current_memory($2);
			$2 = mm_strdup("$0");
		}

 $$ = cat_str(4,mm_strdup("backward"),$2,$3,$4);
}
|  BACKWARD ALL opt_from_in cursor_name
 { 
		add_additional_variables($4, false);
		if ($4[0] == ':')
		{
			free_current_memory($4);
			$4 = mm_strdup("$0");
		}

 $$ = cat_str(3,mm_strdup("backward all"),$3,$4);
}
;


 from_in:
 FROM
 { 
 $$ = mm_strdup("from");
}
|  IN_P
 { 
 $$ = mm_strdup("in");
}
;


 opt_from_in:
 from_in
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 GrantStmt:
 GRANT privileges ON privilege_target TO grantee_list opt_grant_grant_option
 { 
 $$ = cat_str(7,mm_strdup("grant"),$2,mm_strdup("on"),$4,mm_strdup("to"),$6,$7);
}
|  GRANT ALL privilege_str TO UserId
 { 
 $$ = cat_str(4,mm_strdup("grant all"),$3,mm_strdup("to"),$5);
}
;


 RevokeStmt:
 REVOKE privileges ON privilege_target FROM grantee_list opt_drop_behavior
 { 
 $$ = cat_str(7,mm_strdup("revoke"),$2,mm_strdup("on"),$4,mm_strdup("from"),$6,$7);
}
|  REVOKE GRANT OPTION FOR privileges ON privilege_target FROM grantee_list opt_drop_behavior
 { 
 $$ = cat_str(7,mm_strdup("revoke grant option for"),$5,mm_strdup("on"),$7,mm_strdup("from"),$9,$10);
}
|  REVOKE ALL privilege_str FROM UserId
 { 
 $$ = cat_str(4,mm_strdup("revoke all"),$3,mm_strdup("from"),$5);
}
;


 privilege_str:
 PRIVILEGES
 { 
 $$ = mm_strdup("privileges");
}
|  PRIVILEGE
 { 
 $$ = mm_strdup("privilege");
}
;


 privileges:
 privilege_list
 { 
 $$ = $1;
}
|  ALL
 { 
 $$ = mm_strdup("all");
}
|  ALL PRIVILEGES
 { 
 $$ = mm_strdup("all privileges");
}
|  ALL '(' columnList ')'
 { 
 $$ = cat_str(3,mm_strdup("all ("),$3,mm_strdup(")"));
}
|  ALL PRIVILEGES '(' columnList ')'
 { 
 $$ = cat_str(3,mm_strdup("all privileges ("),$4,mm_strdup(")"));
}
;


 privilege_list:
 privilege
 { 
 $$ = $1;
}
|  privilege_list ',' privilege
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 privilege:
 SELECT opt_column_list
 { 
 $$ = cat_str(2,mm_strdup("select"),$2);
}
|  REFERENCES opt_column_list
 { 
 $$ = cat_str(2,mm_strdup("references"),$2);
}
|  CREATE opt_column_list
 { 
 $$ = cat_str(2,mm_strdup("create"),$2);
}
|  ColId opt_column_list
 { 
 $$ = cat_str(2,$1,$2);
}
;


 privilege_target:
 qualified_name_list
 { 
 $$ = $1;
}
|  TABLE qualified_name_list
 { 
 $$ = cat_str(2,mm_strdup("table"),$2);
}
|  SEQUENCE qualified_name_list
 { 
 $$ = cat_str(2,mm_strdup("sequence"),$2);
}
|  LARGE_P SEQUENCE qualified_name_list
 { 
 $$ = cat_str(2,mm_strdup("large sequence"),$3);
}
|  FOREIGN DATA_P WRAPPER name_list
 { 
 $$ = cat_str(2,mm_strdup("foreign data wrapper"),$4);
}
|  FOREIGN SERVER name_list
 { 
 $$ = cat_str(2,mm_strdup("foreign server"),$3);
}
|  FUNCTION function_with_argtypes_list
 { 
 $$ = cat_str(2,mm_strdup("function"),$2);
}
|  PACKAGE any_name_list
 { 
 $$ = cat_str(2,mm_strdup("package"),$2);
}
|  PROCEDURE function_with_argtypes_list
 { 
 $$ = cat_str(2,mm_strdup("procedure"),$2);
}
|  DATABASE name_list
 { 
 $$ = cat_str(2,mm_strdup("database"),$2);
}
|  DOMAIN_P any_name_list
 { 
 $$ = cat_str(2,mm_strdup("domain"),$2);
}
|  LANGUAGE name_list
 { 
 $$ = cat_str(2,mm_strdup("language"),$2);
}
|  LARGE_P OBJECT_P NumericOnly_list
 { 
 $$ = cat_str(2,mm_strdup("large object"),$3);
}
|  NODE GROUP_P name_list
 { 
 $$ = cat_str(2,mm_strdup("node group"),$3);
}
|  SCHEMA name_list
 { 
 $$ = cat_str(2,mm_strdup("schema"),$2);
}
|  TABLESPACE name_list
 { 
 $$ = cat_str(2,mm_strdup("tablespace"),$2);
}
|  DIRECTORY name_list
 { 
 $$ = cat_str(2,mm_strdup("directory"),$2);
}
|  TYPE_P any_name_list
 { 
 $$ = cat_str(2,mm_strdup("type"),$2);
}
|  ALL TABLES IN_P SCHEMA name_list
 { 
 $$ = cat_str(2,mm_strdup("all tables in schema"),$5);
}
|  ALL SEQUENCES IN_P SCHEMA name_list
 { 
 $$ = cat_str(2,mm_strdup("all sequences in schema"),$5);
}
|  ALL FUNCTIONS IN_P SCHEMA name_list
 { 
 $$ = cat_str(2,mm_strdup("all functions in schema"),$5);
}
|  ALL PACKAGES IN_P SCHEMA name_list
 { 
 $$ = cat_str(2,mm_strdup("all packages in schema"),$5);
}
|  DATA_P SOURCE_P name_list
 { 
 $$ = cat_str(2,mm_strdup("data source"),$3);
}
|  CLIENT_MASTER_KEY setting_name
 { 
 $$ = cat_str(2,mm_strdup("client_master_key"),$2);
}
|  COLUMN_ENCRYPTION_KEY setting_name
 { 
 $$ = cat_str(2,mm_strdup("column_encryption_key"),$2);
}
;


 grantee_list:
 grantee
 { 
 $$ = $1;
}
|  grantee_list ',' grantee
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 grantee:
 UserId
 { 
 $$ = $1;
}
|  GROUP_P RoleId
 { 
 $$ = cat_str(2,mm_strdup("group"),$2);
}
;


 opt_grant_grant_option:
 WITH GRANT OPTION
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = mm_strdup("with grant option");
}
| 
 { 
 $$=EMPTY; }
;


 function_with_argtypes_list:
 function_with_argtypes
 { 
 $$ = $1;
}
|  function_with_argtypes_list ',' function_with_argtypes
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 function_with_argtypes:
 func_name func_args
 { 
 $$ = cat_str(2,$1,$2);
}
;


 GrantRoleStmt:
 GRANT privilege_list TO name_list opt_grant_admin_option opt_granted_by
 { 
 $$ = cat_str(6,mm_strdup("grant"),$2,mm_strdup("to"),$4,$5,$6);
}
;


 RevokeRoleStmt:
 REVOKE privilege_list FROM name_list opt_granted_by opt_drop_behavior
 { 
 $$ = cat_str(6,mm_strdup("revoke"),$2,mm_strdup("from"),$4,$5,$6);
}
|  REVOKE ADMIN OPTION FOR privilege_list FROM name_list opt_granted_by opt_drop_behavior
 { 
 $$ = cat_str(6,mm_strdup("revoke admin option for"),$5,mm_strdup("from"),$7,$8,$9);
}
;


 opt_grant_admin_option:
 WITH ADMIN OPTION
 { 
 $$ = mm_strdup("with admin option");
}
| 
 { 
 $$=EMPTY; }
;


 opt_granted_by:
 GRANTED BY UserId
 { 
 $$ = cat_str(2,mm_strdup("granted by"),$3);
}
| 
 { 
 $$=EMPTY; }
;


 GrantDbStmt:
 GRANT db_privileges TO grantee_list opt_grant_admin_option
 { 
 $$ = cat_str(5,mm_strdup("grant"),$2,mm_strdup("to"),$4,$5);
}
;


 RevokeDbStmt:
 REVOKE db_privileges FROM grantee_list
 { 
 $$ = cat_str(4,mm_strdup("revoke"),$2,mm_strdup("from"),$4);
}
|  REVOKE ADMIN OPTION FOR db_privileges FROM grantee_list
 { 
 $$ = cat_str(4,mm_strdup("revoke admin option for"),$5,mm_strdup("from"),$7);
}
;


 db_privileges:
 db_privilege_list
 { 
 $$ = $1;
}
;


 db_privilege_list:
 db_privilege
 { 
 $$ = $1;
}
|  db_privilege_list ',' db_privilege
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 db_privilege:
 CREATE ANY TABLE
 { 
 $$ = mm_strdup("create any table");
}
|  ALTER ANY TABLE
 { 
 $$ = mm_strdup("alter any table");
}
|  DROP ANY TABLE
 { 
 $$ = mm_strdup("drop any table");
}
|  SELECT ANY TABLE
 { 
 $$ = mm_strdup("select any table");
}
|  INSERT ANY TABLE
 { 
 $$ = mm_strdup("insert any table");
}
|  UPDATE ANY TABLE
 { 
 $$ = mm_strdup("update any table");
}
|  DELETE_P ANY TABLE
 { 
 $$ = mm_strdup("delete any table");
}
|  CREATE ANY SEQUENCE
 { 
 $$ = mm_strdup("create any sequence");
}
|  CREATE ANY INDEX
 { 
 $$ = mm_strdup("create any index");
}
|  CREATE ANY FUNCTION
 { 
 $$ = mm_strdup("create any function");
}
|  EXECUTE ANY FUNCTION
 { 
 $$ = mm_strdup("execute any function");
}
|  CREATE ANY PACKAGE
 { 
 $$ = mm_strdup("create any package");
}
|  EXECUTE ANY PACKAGE
 { 
 $$ = mm_strdup("execute any package");
}
|  CREATE ANY TYPE_P
 { 
 $$ = mm_strdup("create any type");
}
|  ALTER ANY TYPE_P
 { 
 $$ = mm_strdup("alter any type");
}
|  DROP ANY TYPE_P
 { 
 $$ = mm_strdup("drop any type");
}
|  ALTER ANY SEQUENCE
 { 
 $$ = mm_strdup("alter any sequence");
}
|  DROP ANY SEQUENCE
 { 
 $$ = mm_strdup("drop any sequence");
}
|  SELECT ANY SEQUENCE
 { 
 $$ = mm_strdup("select any sequence");
}
|  ALTER ANY INDEX
 { 
 $$ = mm_strdup("alter any index");
}
|  DROP ANY INDEX
 { 
 $$ = mm_strdup("drop any index");
}
|  CREATE ANY SYNONYM
 { 
 $$ = mm_strdup("create any synonym");
}
|  DROP ANY SYNONYM
 { 
 $$ = mm_strdup("drop any synonym");
}
|  CREATE ANY TRIGGER
 { 
 $$ = mm_strdup("create any trigger");
}
|  ALTER ANY TRIGGER
 { 
 $$ = mm_strdup("alter any trigger");
}
|  DROP ANY TRIGGER
 { 
 $$ = mm_strdup("drop any trigger");
}
;


 AlterDefaultPrivilegesStmt:
 ALTER DEFAULT PRIVILEGES DefACLOptionList DefACLAction
 { 
 $$ = cat_str(3,mm_strdup("alter default privileges"),$4,$5);
}
;


 DefACLOptionList:
 DefACLOptionList DefACLOption
 { 
 $$ = cat_str(2,$1,$2);
}
| 
 { 
 $$=EMPTY; }
;


 DefACLOption:
 IN_P SCHEMA name_list
 { 
 $$ = cat_str(2,mm_strdup("in schema"),$3);
}
|  FOR ROLE name_list
 { 
 $$ = cat_str(2,mm_strdup("for role"),$3);
}
|  FOR USER name_list
 { 
 $$ = cat_str(2,mm_strdup("for user"),$3);
}
;


 DefACLAction:
 GRANT privileges ON defacl_privilege_target TO grantee_list opt_grant_grant_option
 { 
 $$ = cat_str(7,mm_strdup("grant"),$2,mm_strdup("on"),$4,mm_strdup("to"),$6,$7);
}
|  REVOKE privileges ON defacl_privilege_target FROM grantee_list opt_drop_behavior
 { 
 $$ = cat_str(7,mm_strdup("revoke"),$2,mm_strdup("on"),$4,mm_strdup("from"),$6,$7);
}
|  REVOKE GRANT OPTION FOR privileges ON defacl_privilege_target FROM grantee_list opt_drop_behavior
 { 
 $$ = cat_str(7,mm_strdup("revoke grant option for"),$5,mm_strdup("on"),$7,mm_strdup("from"),$9,$10);
}
;


 defacl_privilege_target:
 TABLES
 { 
 $$ = mm_strdup("tables");
}
|  FUNCTIONS
 { 
 $$ = mm_strdup("functions");
}
|  SEQUENCES
 { 
 $$ = mm_strdup("sequences");
}
|  TYPES_P
 { 
 $$ = mm_strdup("types");
}
|  CLIENT_MASTER_KEYS
 { 
 $$ = mm_strdup("client_master_keys");
}
|  COLUMN_ENCRYPTION_KEYS
 { 
 $$ = mm_strdup("column_encryption_keys");
}
|  PACKAGES
 { 
 $$ = mm_strdup("packages");
}
;


 IndexStmt:
 CREATE opt_unique INDEX opt_concurrently opt_index_name ON qualified_name access_method_clause '(' index_params ')' opt_include opt_reloptions OptPartitionElement opt_table_index_options where_clause
 { 
 $$ = cat_str(16,mm_strdup("create"),$2,mm_strdup("index"),$4,$5,mm_strdup("on"),$7,$8,mm_strdup("("),$10,mm_strdup(")"),$12,$13,$14,$15,$16);
}
|  CREATE opt_unique INDEX opt_concurrently opt_index_name ON qualified_name access_method_clause '(' index_params ')' LOCAL opt_partition_index_def opt_include opt_reloptions OptTableSpace opt_table_index_options
 { 
 $$ = cat_str(16,mm_strdup("create"),$2,mm_strdup("index"),$4,$5,mm_strdup("on"),$7,$8,mm_strdup("("),$10,mm_strdup(") local"),$13,$14,$15,$16,$17);
}
|  CREATE opt_unique INDEX opt_concurrently opt_index_name ON qualified_name access_method_clause '(' index_params ')' GLOBAL opt_include opt_reloptions OptTableSpace opt_table_index_options
 { 
 $$ = cat_str(15,mm_strdup("create"),$2,mm_strdup("index"),$4,$5,mm_strdup("on"),$7,$8,mm_strdup("("),$10,mm_strdup(") global"),$13,$14,$15,$16);
}
|  CREATE opt_unique INDEX opt_concurrently IF_P NOT EXISTS opt_index_name ON qualified_name access_method_clause '(' index_params ')' opt_include opt_reloptions OptPartitionElement opt_index_options where_clause
 { 
 $$ = cat_str(17,mm_strdup("create"),$2,mm_strdup("index"),$4,mm_strdup("if not exists"),$8,mm_strdup("on"),$10,$11,mm_strdup("("),$13,mm_strdup(")"),$15,$16,$17,$18,$19);
}
|  CREATE opt_unique INDEX opt_concurrently IF_P NOT EXISTS opt_index_name ON qualified_name access_method_clause '(' index_params ')' LOCAL opt_partition_index_def opt_include opt_reloptions OptTableSpace opt_index_options
 { 
 $$ = cat_str(17,mm_strdup("create"),$2,mm_strdup("index"),$4,mm_strdup("if not exists"),$8,mm_strdup("on"),$10,$11,mm_strdup("("),$13,mm_strdup(") local"),$16,$17,$18,$19,$20);
}
|  CREATE opt_unique INDEX opt_concurrently IF_P NOT EXISTS opt_index_name ON qualified_name access_method_clause '(' index_params ')' GLOBAL opt_include opt_reloptions OptTableSpace opt_index_options
 { 
 $$ = cat_str(16,mm_strdup("create"),$2,mm_strdup("index"),$4,mm_strdup("if not exists"),$8,mm_strdup("on"),$10,$11,mm_strdup("("),$13,mm_strdup(") global"),$16,$17,$18,$19);
}
;


 opt_unique:
 UNIQUE
 { 
 $$ = mm_strdup("unique");
}
| 
 { 
 $$=EMPTY; }
;


 opt_concurrently:
 CONCURRENTLY
 { 
 $$ = mm_strdup("concurrently");
}
| 
 { 
 $$=EMPTY; }
;


 opt_index_name:
 index_name
 { 
 $$ = $1;
}
|  ColId indirection
 { 
 $$ = cat_str(2,$1,$2);
}
| 
 { 
 $$=EMPTY; }
;


 key_usage_list:
 index_name
 { 
 $$ = $1;
}
|  key_usage_list ',' index_name
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 index_hint_definition:
 USE_INDEX '(' key_usage_list ')'
 { 
 $$ = cat_str(3,mm_strdup("use_index ("),$3,mm_strdup(")"));
}
|  USE_INDEX '(' ')'
 { 
 $$ = mm_strdup("use_index ( )");
}
|  FORCE_INDEX '(' key_usage_list ')'
 { 
 $$ = cat_str(3,mm_strdup("force_index ("),$3,mm_strdup(")"));
}
;


 index_hint_list:
 index_hint_definition
 { 
 $$ = $1;
}
|  index_hint_list index_hint_definition
 { 
 $$ = cat_str(2,$1,$2);
}
;


 opt_index_hint_list:
 index_hint_list
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 access_method_clause:
 USING access_method
 { 
 $$ = cat_str(2,mm_strdup("using"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 access_method_clause_without_keyword:
 USING ecpg_ident
 { 
 $$ = cat_str(2,mm_strdup("using"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 index_params:
 index_elem
 { 
 $$ = $1;
}
|  index_params ',' index_elem
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 index_elem:
 ColId opt_collation opt_class opt_asc_desc opt_nulls_order
 { 
 $$ = cat_str(5,$1,$2,$3,$4,$5);
}
|  index_functional_expr_key opt_collation opt_class opt_asc_desc opt_nulls_order
 { 
 $$ = cat_str(5,$1,$2,$3,$4,$5);
}
|  '(' a_expr ')' opt_collation opt_class opt_asc_desc opt_nulls_order
 { 
 $$ = cat_str(7,mm_strdup("("),$2,mm_strdup(")"),$4,$5,$6,$7);
}
;


 constraint_params:
 constraint_elem
 { 
 $$ = $1;
}
|  constraint_params ',' constraint_elem
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 con_asc_desc:
 ASC
 { 
 $$ = mm_strdup("asc");
}
|  DESC
 { 
 $$ = mm_strdup("desc");
}
;


 constraint_elem:
 ColId con_asc_desc
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,$1,$2);
}
|  ColId
 { 
 $$ = $1;
}
|  '(' a_expr ')' opt_asc_desc
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(4,mm_strdup("("),$2,mm_strdup(")"),$4);
}
;


 index_functional_expr_key:
 col_name_keyword_nonambiguous '(' Iconst ')'
 { 
 $$ = cat_str(4,$1,mm_strdup("("),$3,mm_strdup(")"));
}
|  func_name '(' func_arg_list opt_sort_clause ')'
 { 
 $$ = cat_str(5,$1,mm_strdup("("),$3,$4,mm_strdup(")"));
}
|  func_application_special
 { 
 $$ = $1;
}
|  func_expr_common_subexpr
 { 
 $$ = $1;
}
;


 opt_include:
 INCLUDE '(' index_including_params ')'
 { 
 $$ = cat_str(3,mm_strdup("include ("),$3,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 index_including_params:
 index_elem
 { 
 $$ = $1;
}
|  index_including_params ',' index_elem
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 collate:
 COLLATE opt_equal charset_collate_name
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,mm_strdup("collate"),$2,$3);
}
;


 opt_collate:
 collate
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 default_collate:
 collate
 { 
 $$ = $1;
}
|  DEFAULT collate
 { 
 $$ = cat_str(2,mm_strdup("default"),$2);
}
;


 opt_collation:
 COLLATE collate_name
 { 
 $$ = cat_str(2,mm_strdup("collate"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 opt_class:
 any_name
 { 
 $$ = $1;
}
|  USING any_name
 { 
 $$ = cat_str(2,mm_strdup("using"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 opt_asc_desc:
 ASC
 { 
 $$ = mm_strdup("asc");
}
|  DESC
 { 
 $$ = mm_strdup("desc");
}
| 
 { 
 $$=EMPTY; }
;


 opt_nulls_order:
 NULLS_FIRST
 { 
 $$ = mm_strdup("nulls first");
}
|  NULLS_LAST
 { 
 $$ = mm_strdup("nulls last");
}
| 
 { 
 $$=EMPTY; }
;


 opt_partition_index_def:
 '(' range_partition_index_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 range_partition_index_list:
 range_partition_index_item
 { 
 $$ = $1;
}
|  range_partition_index_list ',' range_partition_index_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 range_partition_index_item:
 PARTITION index_name opt_part_options
 { 
 $$ = cat_str(3,mm_strdup("partition"),$2,$3);
}
|  PARTITION index_name opt_part_options opt_subpartition_index_def
 { 
 $$ = cat_str(4,mm_strdup("partition"),$2,$3,$4);
}
;


 opt_subpartition_index_def:
 '(' range_subpartition_index_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
;


 range_subpartition_index_list:
 range_subpartition_index_item
 { 
 $$ = $1;
}
|  range_subpartition_index_list ',' range_subpartition_index_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 range_subpartition_index_item:
 SUBPARTITION index_name opt_part_options
 { 
 $$ = cat_str(3,mm_strdup("subpartition"),$2,$3);
}
;


 CreateFunctionStmt:
 CREATE opt_or_replace definer_user FUNCTION func_name_opt_arg proc_args RETURNS func_return createfunc_opt_list opt_definition
 { 
 $$ = cat_str(10,mm_strdup("create"),$2,$3,mm_strdup("function"),$5,$6,mm_strdup("returns"),$8,$9,$10);
}
|  CREATE opt_or_replace definer_user FUNCTION func_name_opt_arg proc_args RETURNS TABLE '(' table_func_column_list ')' createfunc_opt_list opt_definition
 { 
 $$ = cat_str(11,mm_strdup("create"),$2,$3,mm_strdup("function"),$5,$6,mm_strdup("returns table ("),$10,mm_strdup(")"),$12,$13);
}
|  CREATE opt_or_replace definer_user FUNCTION func_name_opt_arg proc_args createfunc_opt_list opt_definition
 { 
 $$ = cat_str(8,mm_strdup("create"),$2,$3,mm_strdup("function"),$5,$6,$7,$8);
}
|  CREATE opt_or_replace definer_user FUNCTION func_name_opt_arg proc_args RETURN func_return opt_createproc_opt_list as_is subprogram_body
 { 
 $$ = cat_str(11,mm_strdup("create"),$2,$3,mm_strdup("function"),$5,$6,mm_strdup("return"),$8,$9,$10,$11);
}
;


 CallFuncStmt:
 CALL func_name '(' ')'
 { 
 $$ = cat_str(3,mm_strdup("call"),$2,mm_strdup("( )"));
}
|  CALL func_name '(' callfunc_args ')'
 { 
 $$ = cat_str(5,mm_strdup("call"),$2,mm_strdup("("),$4,mm_strdup(")"));
}
;


 callfunc_args:
 func_arg_expr
 { 
 $$ = $1;
}
|  callfunc_args ',' func_arg_expr
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 CreateEventStmt:
 CREATE opt_or_replace definer_opt EVENT qualified_name ON SCHEDULE start_expr opt_ev_on_completion opt_ev_status comment_opt DO ev_body
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(12,mm_strdup("create"),$2,$3,mm_strdup("event"),$5,mm_strdup("on schedule"),$8,$9,$10,$11,mm_strdup("do"),$13);
}
|  CREATE opt_or_replace definer_opt EVENT IF_P NOT EXISTS qualified_name ON SCHEDULE start_expr opt_ev_on_completion opt_ev_status comment_opt DO ev_body
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(12,mm_strdup("create"),$2,$3,mm_strdup("event if not exists"),$8,mm_strdup("on schedule"),$11,$12,$13,$14,mm_strdup("do"),$16);
}
|  CREATE opt_or_replace definer_opt EVENT qualified_name ON SCHEDULE EVERY every_interval start_expr end_expr opt_ev_on_completion opt_ev_status comment_opt DO ev_body
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(14,mm_strdup("create"),$2,$3,mm_strdup("event"),$5,mm_strdup("on schedule every"),$9,$10,$11,$12,$13,$14,mm_strdup("do"),$16);
}
|  CREATE opt_or_replace definer_opt EVENT IF_P NOT EXISTS qualified_name ON SCHEDULE EVERY every_interval start_expr end_expr opt_ev_on_completion opt_ev_status comment_opt DO ev_body
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(14,mm_strdup("create"),$2,$3,mm_strdup("event if not exists"),$8,mm_strdup("on schedule every"),$12,$13,$14,$15,$16,$17,mm_strdup("do"),$19);
}
;


 definer_opt:
 DEFINER '=' user
 { 
 $$ = cat_str(2,mm_strdup("definer ="),$3);
}
| 
 { 
 $$=EMPTY; }
;


 user:
 ColId
 { 
 $$ = $1;
}
;


 every_interval:
 Iconst opt_interval
 { 
 $$ = cat_str(2,$1,$2);
}
|  ecpg_sconst opt_interval
 { 
 $$ = cat_str(2,$1,$2);
}
|  ecpg_fconst opt_interval
 { 
 $$ = cat_str(2,$1,$2);
}
;


 start_expr:
 STARTS ev_timeexpr
 { 
 $$ = cat_str(2,mm_strdup("starts"),$2);
}
|  AT ev_timeexpr
 { 
 $$ = cat_str(2,mm_strdup("at"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 end_expr:
 ENDS ev_timeexpr
 { 
 $$ = cat_str(2,mm_strdup("ends"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 ev_timeexpr:
 initime
 { 
 $$ = $1;
}
|  initime '+' interval_list
 { 
 $$ = cat_str(3,$1,mm_strdup("+"),$3);
}
|  initime '-' interval_list
 { 
 $$ = cat_str(3,$1,mm_strdup("-"),$3);
}
;


 interval_list:
 interval_cell
 { 
 $$ = $1;
}
|  interval_list '+' interval_list
 { 
 $$ = cat_str(3,$1,mm_strdup("+"),$3);
}
|  interval_list '-' interval_list
 { 
 $$ = cat_str(3,$1,mm_strdup("-"),$3);
}
;


 interval_cell:
 ConstInterval ICONST opt_interval
 { 
 $$ = cat_str(3,$1,mm_strdup("iconst"),$3);
}
|  ConstInterval ecpg_sconst opt_interval
 { 
 $$ = cat_str(3,$1,$2,$3);
}
|  ConstInterval ecpg_fconst opt_interval
 { 
 $$ = cat_str(3,$1,$2,$3);
}
| 
 { 
 $$=EMPTY; }
;


 initime:
 interval_intexpr
 { 
 $$ = $1;
}
|  functime_expr
 { 
 $$ = $1;
}
|  functime_app
 { 
 $$ = $1;
}
;


 functime_app:
 ecpg_ident '(' ')'
 { 
 $$ = cat_str(2,$1,mm_strdup("( )"));
}
|  ecpg_ident '(' func_arg_list ')'
 { 
 $$ = cat_str(4,$1,mm_strdup("("),$3,mm_strdup(")"));
}
;


 functime_expr:
 CURRENT_TIMESTAMP
 { 
 $$ = mm_strdup("current_timestamp");
}
|  CURRENT_TIMESTAMP '(' Iconst ')'
 { 
 $$ = cat_str(3,mm_strdup("current_timestamp ("),$3,mm_strdup(")"));
}
|  LOCALTIMESTAMP
 { 
 $$ = mm_strdup("localtimestamp");
}
|  LOCALTIMESTAMP '(' Iconst ')'
 { 
 $$ = cat_str(3,mm_strdup("localtimestamp ("),$3,mm_strdup(")"));
}
|  SYSDATE
 { 
 $$ = mm_strdup("sysdate");
}
;


 interval_intexpr:
 ecpg_sconst
 { 
 $$ = $1;
}
;


 opt_ev_on_completion:
 ON COMPLETION PRESERVE
 { 
 $$ = mm_strdup("on completion preserve");
}
|  ON COMPLETION NOT PRESERVE
 { 
 $$ = mm_strdup("on completion not preserve");
}
| 
 { 
 $$=EMPTY; }
;


 opt_ev_status:
 ENABLE_P
 { 
 $$ = mm_strdup("enable");
}
|  DISABLE_P
 { 
 $$ = mm_strdup("disable");
}
|  DISABLE_P ON SLAVE
 { 
 $$ = mm_strdup("disable on slave");
}
| 
 { 
 $$=EMPTY; }
;


 comment_opt:
 COMMENT SCONST
 { 
 $$ = mm_strdup("comment sconst");
}
| 
 { 
 $$=EMPTY; }
;


 ev_body:

 { 
 $$=EMPTY; }
;


 AlterEventStmt:
 ALTER definer_name_opt EVENT qualified_name preserve_opt rename_opt status_opt comments_opt action_opt
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(9,mm_strdup("alter"),$2,mm_strdup("event"),$4,$5,$6,$7,$8,$9);
}
|  ALTER definer_name_opt EVENT qualified_name ON SCHEDULE AT ev_timeexpr preserve_opt rename_opt status_opt comments_opt action_opt
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(11,mm_strdup("alter"),$2,mm_strdup("event"),$4,mm_strdup("on schedule at"),$8,$9,$10,$11,$12,$13);
}
|  ALTER definer_name_opt EVENT qualified_name ON SCHEDULE EVERY every_interval start_opt end_opt preserve_opt rename_opt status_opt comments_opt action_opt
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(13,mm_strdup("alter"),$2,mm_strdup("event"),$4,mm_strdup("on schedule every"),$8,$9,$10,$11,$12,$13,$14,$15);
}
;


 definer_name_opt:
 DEFINER '=' user
 { 
 $$ = cat_str(2,mm_strdup("definer ="),$3);
}
| 
 { 
 $$=EMPTY; }
;


 end_opt:
 ENDS ev_timeexpr
 { 
 $$ = cat_str(2,mm_strdup("ends"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 start_opt:
 STARTS ev_timeexpr
 { 
 $$ = cat_str(2,mm_strdup("starts"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 preserve_opt:
 ON COMPLETION PRESERVE
 { 
 $$ = mm_strdup("on completion preserve");
}
|  ON COMPLETION NOT PRESERVE
 { 
 $$ = mm_strdup("on completion not preserve");
}
| 
 { 
 $$=EMPTY; }
;


 rename_opt:
 RENAME TO qualified_name
 { 
 $$ = cat_str(2,mm_strdup("rename to"),$3);
}
| 
 { 
 $$=EMPTY; }
;


 status_opt:
 ENABLE_P
 { 
 $$ = mm_strdup("enable");
}
|  DISABLE_P
 { 
 $$ = mm_strdup("disable");
}
|  DISABLE_P ON SLAVE
 { 
 $$ = mm_strdup("disable on slave");
}
| 
 { 
 $$=EMPTY; }
;


 comments_opt:
 COMMENT ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("comment"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 action_opt:
 DO ev_body
 { 
 $$ = cat_str(2,mm_strdup("do"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 DropEventStmt:
 DROP EVENT IF_P EXISTS qualified_name
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("drop event if exists"),$5);
}
|  DROP EVENT qualified_name
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("drop event"),$3);
}
;


 ShowEventStmt:
 SHOW EVENTS event_from_clause event_where_clause
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,mm_strdup("show events"),$3,$4);
}
|  SHOW EVENTS event_from_clause
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("show events"),$3);
}
|  SHOW EVENTS event_where_clause
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("show events"),$3);
}
;


 event_from_clause:
 FROM ColId
 { 
 $$ = cat_str(2,mm_strdup("from"),$2);
}
|  IN_P ColId
 { 
 $$ = cat_str(2,mm_strdup("in"),$2);
}
;


 event_where_clause:
 WHERE ev_where_body
 { 
 $$ = cat_str(2,mm_strdup("where"),$2);
}
|  LIKE ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("like"),$2);
}
;


 ev_where_body:

 { 
 $$=EMPTY; }
;


 CreateProcedureStmt:
 CREATE opt_or_replace definer_user PROCEDURE func_name_opt_arg proc_args opt_createproc_opt_list as_is subprogram_body
 { 
 $$ = cat_str(9,mm_strdup("create"),$2,$3,mm_strdup("procedure"),$5,$6,$7,$8,$9);
}
;


 CreatePackageStmt:
 CREATE opt_or_replace PACKAGE pkg_name invoker_rights as_is
 { 
 $$ = cat_str(6,mm_strdup("create"),$2,mm_strdup("package"),$4,$5,$6);
}
;


 pkg_name:
 ColId
 { 
 $$ = $1;
}
|  ColId indirection
 { 
 $$ = cat_str(2,$1,$2);
}
;


 invoker_rights:
 AUTHID DEFINER
 { 
 $$ = mm_strdup("authid definer");
}
|  AUTHID CURRENT_USER
 { 
 $$ = mm_strdup("authid current_user");
}
| 
 { 
 $$=EMPTY; }
;


 definer_expression:
 DEFINER '=' UserId
 { 
 $$ = cat_str(2,mm_strdup("definer ="),$3);
}
;


 definer_user:
 definer_expression
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 pkg_body_subprogram:

 { 
 $$=EMPTY; }
;


 CreatePackageBodyStmt:
 CREATE opt_or_replace PACKAGE BODY_P pkg_name as_is pkg_body_subprogram
 { 
 $$ = cat_str(6,mm_strdup("create"),$2,mm_strdup("package body"),$5,$6,$7);
}
;


 opt_or_replace:
 OR REPLACE
 { 
 $$ = mm_strdup("or replace");
}
| 
 { 
 $$=EMPTY; }
;


 func_args:
 '(' func_args_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
|  '(' ')'
 { 
 $$ = mm_strdup("( )");
}
;


 proc_args:
 func_args_with_defaults
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 func_args_list:
 func_arg
 { 
 $$ = $1;
}
|  func_args_list ',' func_arg
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 as_is:
 AS
 { 
 $$ = mm_strdup("as");
}
|  IS
 { 
 $$ = mm_strdup("is");
}
;


 as_empty:
 AS
 { 
 $$ = mm_strdup("as");
}
| 
 { 
 $$=EMPTY; }
;


 func_args_with_defaults:
 '(' func_args_with_defaults_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
|  '(' ')'
 { 
 $$ = mm_strdup("( )");
}
;


 func_args_with_defaults_list:
 func_arg_with_default
 { 
 $$ = $1;
}
|  func_args_with_defaults_list ',' func_arg_with_default
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 func_arg:
 arg_class param_name func_type
 { 
 $$ = cat_str(3,$1,$2,$3);
}
|  param_name arg_class func_type
 { 
 $$ = cat_str(3,$1,$2,$3);
}
|  param_name func_type
 { 
 $$ = cat_str(2,$1,$2);
}
|  arg_class func_type
 { 
 $$ = cat_str(2,$1,$2);
}
|  func_type
 { 
 $$ = $1;
}
;


 arg_class:
 IN_P
 { 
 $$ = mm_strdup("in");
}
|  OUT_P
 { 
 $$ = mm_strdup("out");
}
|  INOUT
 { 
 $$ = mm_strdup("inout");
}
|  IN_P OUT_P
 { 
 $$ = mm_strdup("in out");
}
|  VARIADIC
 { 
 $$ = mm_strdup("variadic");
}
;


 param_name:
 type_function_name
 { 
 $$ = $1;
}
;


 func_return:
 func_type
 { 
 $$ = $1;
}
|  func_type DETERMINISTIC
 { 
 $$ = cat_str(2,$1,mm_strdup("deterministic"));
}
;


 func_type:
 Typename
 { 
 $$ = $1;
}
|  type_function_name attrs '%' ROWTYPE_P opt_array_bounds
 { 
 $$ = cat_str(4,$1,$2,mm_strdup("% rowtype"),$5);
}
|  type_function_name '%' ROWTYPE_P opt_array_bounds
 { 
 $$ = cat_str(3,$1,mm_strdup("% rowtype"),$4);
}
|  type_function_name attrs '%' TYPE_P opt_array_bounds
 { 
 $$ = cat_str(4,$1,$2,mm_strdup("% type"),$5);
}
|  type_function_name '%' TYPE_P opt_array_bounds
 { 
 $$ = cat_str(3,$1,mm_strdup("% type"),$4);
}
|  SETOF type_function_name attrs '%' TYPE_P opt_array_bounds
 { 
 $$ = cat_str(5,mm_strdup("setof"),$2,$3,mm_strdup("% type"),$6);
}
;


 func_arg_with_default:
 func_arg
 { 
 $$ = $1;
}
|  func_arg DEFAULT a_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("default"),$3);
}
|  func_arg '=' a_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("="),$3);
}
|  func_arg COLON_EQUALS a_expr
 { 
 $$ = cat_str(3,$1,mm_strdup(":="),$3);
}
;


 createfunc_opt_list:
 createfunc_opt_item
 { 
 $$ = $1;
}
|  createfunc_opt_list createfunc_opt_item
 { 
 $$ = cat_str(2,$1,$2);
}
;


 opt_createproc_opt_list:
 opt_createproc_opt_list createproc_opt_item
 { 
 $$ = cat_str(2,$1,$2);
}
| 
 { 
 $$=EMPTY; }
;


 common_func_opt_item:
 CALLED ON NULL_P INPUT_P
 { 
 $$ = mm_strdup("called on null input");
}
|  RETURNS NULL_P ON NULL_P INPUT_P
 { 
 $$ = mm_strdup("returns null on null input");
}
|  STRICT_P
 { 
 $$ = mm_strdup("strict");
}
|  IMMUTABLE
 { 
 $$ = mm_strdup("immutable");
}
|  STABLE
 { 
 $$ = mm_strdup("stable");
}
|  VOLATILE
 { 
 $$ = mm_strdup("volatile");
}
|  SHIPPABLE
 { 
 $$ = mm_strdup("shippable");
}
|  NOT SHIPPABLE
 { 
 $$ = mm_strdup("not shippable");
}
|  EXTERNAL SECURITY DEFINER
 { 
 $$ = mm_strdup("external security definer");
}
|  EXTERNAL SECURITY INVOKER
 { 
 $$ = mm_strdup("external security invoker");
}
|  SECURITY DEFINER
 { 
 $$ = mm_strdup("security definer");
}
|  SECURITY INVOKER
 { 
 $$ = mm_strdup("security invoker");
}
|  AUTHID DEFINER
 { 
 $$ = mm_strdup("authid definer");
}
|  AUTHID CURRENT_USER
 { 
 $$ = mm_strdup("authid current_user");
}
|  LEAKPROOF
 { 
 $$ = mm_strdup("leakproof");
}
|  NOT LEAKPROOF
 { 
 $$ = mm_strdup("not leakproof");
}
|  COST NumericOnly
 { 
 $$ = cat_str(2,mm_strdup("cost"),$2);
}
|  ROWS NumericOnly
 { 
 $$ = cat_str(2,mm_strdup("rows"),$2);
}
|  FunctionSetResetClause
 { 
 $$ = $1;
}
|  FENCED
 { 
 $$ = mm_strdup("fenced");
}
|  NOT FENCED
 { 
 $$ = mm_strdup("not fenced");
}
|  PACKAGE
 { 
 $$ = mm_strdup("package");
}
|  COMMENT ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("comment"),$2);
}
;


 createfunc_opt_item:
 AS func_as
 { 
 $$ = cat_str(2,mm_strdup("as"),$2);
}
|  LANGUAGE ColId_or_Sconst
 { 
 $$ = cat_str(2,mm_strdup("language"),$2);
}
|  WINDOW
 { 
 $$ = mm_strdup("window");
}
|  common_func_opt_item
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


 func_as:
 ecpg_sconst
 { 
 $$ = $1;
}
|  ecpg_sconst ',' ecpg_sconst
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 subprogram_body:

 { 
 $$=EMPTY; }
;


 opt_definition:
 WITH definition
 { 
 $$ = cat_str(2,mm_strdup("with"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 table_func_column:
 param_name func_type
 { 
 $$ = cat_str(2,$1,$2);
}
;


 table_func_column_list:
 table_func_column
 { 
 $$ = $1;
}
|  table_func_column_list ',' table_func_column
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 RemovePackageStmt:
 DROP PACKAGE pkg_name
 { 
 $$ = cat_str(2,mm_strdup("drop package"),$3);
}
|  DROP PACKAGE IF_P EXISTS pkg_name
 { 
 $$ = cat_str(2,mm_strdup("drop package if exists"),$5);
}
|  DROP PACKAGE BODY_P pkg_name
 { 
 $$ = cat_str(2,mm_strdup("drop package body"),$4);
}
|  DROP PACKAGE BODY_P IF_P EXISTS pkg_name
 { 
 $$ = cat_str(2,mm_strdup("drop package body if exists"),$6);
}
;


 AlterFunctionStmt:
 ALTER FUNCTION function_with_argtypes alterfunc_opt_list opt_restrict
 { 
 $$ = cat_str(4,mm_strdup("alter function"),$3,$4,$5);
}
;


 AlterProcedureStmt:
 ALTER PROCEDURE function_with_argtypes alterfunc_opt_list opt_restrict
 { 
 $$ = cat_str(4,mm_strdup("alter procedure"),$3,$4,$5);
}
;


 alterfunc_opt_list:
 common_func_opt_item
 { 
 $$ = $1;
}
|  alterfunc_opt_list common_func_opt_item
 { 
 $$ = cat_str(2,$1,$2);
}
;


 opt_restrict:
 RESTRICT
 { 
 $$ = mm_strdup("restrict");
}
| 
 { 
 $$=EMPTY; }
;


 RemoveFuncStmt:
 DROP FUNCTION func_name func_args opt_drop_behavior
 { 
 $$ = cat_str(4,mm_strdup("drop function"),$3,$4,$5);
}
|  DROP FUNCTION IF_P EXISTS func_name func_args opt_drop_behavior
 { 
 $$ = cat_str(4,mm_strdup("drop function if exists"),$5,$6,$7);
}
|  DROP PROCEDURE func_name func_args opt_drop_behavior
 { 
 $$ = cat_str(4,mm_strdup("drop procedure"),$3,$4,$5);
}
|  DROP PROCEDURE IF_P EXISTS func_name func_args opt_drop_behavior
 { 
 $$ = cat_str(4,mm_strdup("drop procedure if exists"),$5,$6,$7);
}
|  DROP PROCEDURE func_name_opt_arg
 { 
 $$ = cat_str(2,mm_strdup("drop procedure"),$3);
}
|  DROP PROCEDURE IF_P EXISTS func_name_opt_arg
 { 
 $$ = cat_str(2,mm_strdup("drop procedure if exists"),$5);
}
|  DROP FUNCTION func_name_opt_arg
 { 
 $$ = cat_str(2,mm_strdup("drop function"),$3);
}
|  DROP FUNCTION IF_P EXISTS func_name_opt_arg
 { 
 $$ = cat_str(2,mm_strdup("drop function if exists"),$5);
}
;


 RemoveAggrStmt:
 DROP AGGREGATE func_name aggr_args opt_drop_behavior
 { 
 $$ = cat_str(4,mm_strdup("drop aggregate"),$3,$4,$5);
}
|  DROP AGGREGATE IF_P EXISTS func_name aggr_args opt_drop_behavior
 { 
 $$ = cat_str(4,mm_strdup("drop aggregate if exists"),$5,$6,$7);
}
;


 RemoveOperStmt:
 DROP OPERATOR any_operator oper_argtypes opt_drop_behavior
 { 
 $$ = cat_str(4,mm_strdup("drop operator"),$3,$4,$5);
}
|  DROP OPERATOR IF_P EXISTS any_operator oper_argtypes opt_drop_behavior
 { 
 $$ = cat_str(4,mm_strdup("drop operator if exists"),$5,$6,$7);
}
;


 oper_argtypes:
 '(' Typename ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
|  '(' Typename ',' Typename ')'
 { 
 $$ = cat_str(5,mm_strdup("("),$2,mm_strdup(","),$4,mm_strdup(")"));
}
|  '(' NONE ',' Typename ')'
 { 
 $$ = cat_str(3,mm_strdup("( none ,"),$4,mm_strdup(")"));
}
|  '(' Typename ',' NONE ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(", none )"));
}
;


 any_operator:
 all_Op
 { 
 $$ = $1;
}
|  ColId '.' any_operator
 { 
 $$ = cat_str(3,$1,mm_strdup("."),$3);
}
;


 DoStmt:
 DO dostmt_opt_list
 { 
 $$ = cat_str(2,mm_strdup("do"),$2);
}
;


 dostmt_opt_list:
 dostmt_opt_item
 { 
 $$ = $1;
}
|  dostmt_opt_list dostmt_opt_item
 { 
 $$ = cat_str(2,$1,$2);
}
;


 dostmt_opt_item:
 ecpg_sconst
 { 
 $$ = $1;
}
|  LANGUAGE ColId_or_Sconst
 { 
 $$ = cat_str(2,mm_strdup("language"),$2);
}
;


 CreateCastStmt:
 CREATE CAST '(' Typename AS Typename ')' WITH FUNCTION function_with_argtypes cast_context
 { 
 $$ = cat_str(7,mm_strdup("create cast ("),$4,mm_strdup("as"),$6,mm_strdup(") with function"),$10,$11);
}
|  CREATE CAST '(' Typename AS Typename ')' WITHOUT FUNCTION cast_context
 { 
 $$ = cat_str(6,mm_strdup("create cast ("),$4,mm_strdup("as"),$6,mm_strdup(") without function"),$10);
}
|  CREATE CAST '(' Typename AS Typename ')' WITH INOUT cast_context
 { 
 $$ = cat_str(6,mm_strdup("create cast ("),$4,mm_strdup("as"),$6,mm_strdup(") with inout"),$10);
}
;


 cast_context:
 AS IMPLICIT_P
 { 
 $$ = mm_strdup("as implicit");
}
|  AS ASSIGNMENT
 { 
 $$ = mm_strdup("as assignment");
}
| 
 { 
 $$=EMPTY; }
;


 DropCastStmt:
 DROP CAST opt_if_exists '(' Typename AS Typename ')' opt_drop_behavior
 { 
 $$ = cat_str(8,mm_strdup("drop cast"),$3,mm_strdup("("),$5,mm_strdup("as"),$7,mm_strdup(")"),$9);
}
;


 opt_if_exists:
 IF_P EXISTS
 { 
 $$ = mm_strdup("if exists");
}
| 
 { 
 $$=EMPTY; }
;


 ReindexStmt:
 REINDEX reindex_type opt_concurrently qualified_name opt_force
 { 
 $$ = cat_str(5,mm_strdup("reindex"),$2,$3,$4,$5);
}
|  REINDEX reindex_type opt_concurrently qualified_name PARTITION ColId opt_force
 { 
 $$ = cat_str(7,mm_strdup("reindex"),$2,$3,$4,mm_strdup("partition"),$6,$7);
}
|  REINDEX SYSTEM_P opt_concurrently name opt_force
 { 
 $$ = cat_str(4,mm_strdup("reindex system"),$3,$4,$5);
}
|  REINDEX DATABASE opt_concurrently name opt_force
 { 
 $$ = cat_str(4,mm_strdup("reindex database"),$3,$4,$5);
}
;


 reindex_type:
 INDEX
 { 
 $$ = mm_strdup("index");
}
|  TABLE
 { 
 $$ = mm_strdup("table");
}
|  INTERNAL TABLE
 { 
 $$ = mm_strdup("internal table");
}
;


 opt_force:
 FORCE
 { 
 $$ = mm_strdup("force");
}
| 
 { 
 $$=EMPTY; }
;


 RenameStmt:
 ALTER AGGREGATE func_name aggr_args RENAME TO name
 { 
 $$ = cat_str(5,mm_strdup("alter aggregate"),$3,$4,mm_strdup("rename to"),$7);
}
|  ALTER COLLATION any_name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter collation"),$3,mm_strdup("rename to"),$6);
}
|  ALTER CONVERSION_P any_name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter conversion"),$3,mm_strdup("rename to"),$6);
}
|  ALTER DATABASE database_name RENAME TO database_name
 { 
 $$ = cat_str(4,mm_strdup("alter database"),$3,mm_strdup("rename to"),$6);
}
|  ALTER DATA_P SOURCE_P name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter data source"),$4,mm_strdup("rename to"),$7);
}
|  ALTER DOMAIN_P any_name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter domain"),$3,mm_strdup("rename to"),$6);
}
|  ALTER DOMAIN_P any_name RENAME CONSTRAINT name TO name
 { 
 $$ = cat_str(6,mm_strdup("alter domain"),$3,mm_strdup("rename constraint"),$6,mm_strdup("to"),$8);
}
|  ALTER EVENT_TRIGGER name RENAME TO name
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(4,mm_strdup("alter event_trigger"),$3,mm_strdup("rename to"),$6);
}
|  ALTER EVENT_TRIGGER name OWNER TO RoleId
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(4,mm_strdup("alter event_trigger"),$3,mm_strdup("owner to"),$6);
}
|  ALTER FOREIGN DATA_P WRAPPER name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter foreign data wrapper"),$5,mm_strdup("rename to"),$8);
}
|  ALTER FUNCTION function_with_argtypes RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter function"),$3,mm_strdup("rename to"),$6);
}
|  ALTER PROCEDURE function_with_argtypes RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter procedure"),$3,mm_strdup("rename to"),$6);
}
|  ALTER GROUP_P RoleId RENAME TO RoleId
 { 
 $$ = cat_str(4,mm_strdup("alter group"),$3,mm_strdup("rename to"),$6);
}
|  ALTER opt_procedural LANGUAGE name RENAME TO name
 { 
 $$ = cat_str(6,mm_strdup("alter"),$2,mm_strdup("language"),$4,mm_strdup("rename to"),$7);
}
|  ALTER OPERATOR CLASS any_name USING access_method RENAME TO name
 { 
 $$ = cat_str(6,mm_strdup("alter operator class"),$4,mm_strdup("using"),$6,mm_strdup("rename to"),$9);
}
|  ALTER OPERATOR FAMILY any_name USING access_method RENAME TO name
 { 
 $$ = cat_str(6,mm_strdup("alter operator family"),$4,mm_strdup("using"),$6,mm_strdup("rename to"),$9);
}
|  ALTER PUBLICATION name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter publication"),$3,mm_strdup("rename to"),$6);
}
|  ALTER SUBSCRIPTION name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter subscription"),$3,mm_strdup("rename to"),$6);
}
|  ALTER RowLevelSecurityPolicyName ON qualified_name RENAME TO name
 { 
 $$ = cat_str(6,mm_strdup("alter"),$2,mm_strdup("on"),$4,mm_strdup("rename to"),$7);
}
|  ALTER SCHEMA name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter schema"),$3,mm_strdup("rename to"),$6);
}
|  ALTER SERVER name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter server"),$3,mm_strdup("rename to"),$6);
}
|  ALTER MATERIALIZED VIEW qualified_name RENAME opt_column name TO name
 { 
 $$ = cat_str(7,mm_strdup("alter materialized view"),$4,mm_strdup("rename"),$6,$7,mm_strdup("to"),$9);
}
|  ALTER MATERIALIZED VIEW IF_P EXISTS qualified_name RENAME opt_column name TO name
 { 
 $$ = cat_str(7,mm_strdup("alter materialized view if exists"),$6,mm_strdup("rename"),$8,$9,mm_strdup("to"),$11);
}
|  ALTER TABLE relation_expr RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter table"),$3,mm_strdup("rename to"),$6);
}
|  ALTER TABLE IF_P EXISTS relation_expr RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter table if exists"),$5,mm_strdup("rename to"),$8);
}
|  ALTER SEQUENCE qualified_name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter sequence"),$3,mm_strdup("rename to"),$6);
}
|  ALTER LARGE_P SEQUENCE qualified_name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter large sequence"),$4,mm_strdup("rename to"),$7);
}
|  ALTER SEQUENCE IF_P EXISTS qualified_name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter sequence if exists"),$5,mm_strdup("rename to"),$8);
}
|  ALTER LARGE_P SEQUENCE IF_P EXISTS qualified_name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter large sequence if exists"),$6,mm_strdup("rename to"),$9);
}
|  ALTER VIEW qualified_name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter view"),$3,mm_strdup("rename to"),$6);
}
|  ALTER VIEW IF_P EXISTS qualified_name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter view if exists"),$5,mm_strdup("rename to"),$8);
}
|  ALTER VIEW qualified_name opt_column_list AS SelectStmt opt_check_option
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(6,mm_strdup("alter view"),$3,$4,mm_strdup("as"),$6,$7);
}
|  ALTER definer_expression VIEW qualified_name opt_column_list AS SelectStmt opt_check_option
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(8,mm_strdup("alter"),$2,mm_strdup("view"),$4,$5,mm_strdup("as"),$7,$8);
}
|  ALTER MATERIALIZED VIEW qualified_name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter materialized view"),$4,mm_strdup("rename to"),$7);
}
|  ALTER MATERIALIZED VIEW IF_P EXISTS qualified_name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter materialized view if exists"),$6,mm_strdup("rename to"),$9);
}
|  ALTER INDEX qualified_name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter index"),$3,mm_strdup("rename to"),$6);
}
|  ALTER INDEX IF_P EXISTS qualified_name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter index if exists"),$5,mm_strdup("rename to"),$8);
}
|  ALTER INDEX qualified_name RENAME_PARTITION name TO name
 { 
 $$ = cat_str(6,mm_strdup("alter index"),$3,mm_strdup("rename partition"),$5,mm_strdup("to"),$7);
}
|  ALTER INDEX IF_P EXISTS qualified_name RENAME_PARTITION name TO name
 { 
 $$ = cat_str(6,mm_strdup("alter index if exists"),$5,mm_strdup("rename partition"),$7,mm_strdup("to"),$9);
}
|  ALTER FOREIGN TABLE relation_expr RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter foreign table"),$4,mm_strdup("rename to"),$7);
}
|  ALTER FOREIGN TABLE IF_P EXISTS relation_expr RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter foreign table if exists"),$6,mm_strdup("rename to"),$9);
}
|  ALTER TABLE relation_expr RENAME opt_column name TO name
 { 
 $$ = cat_str(7,mm_strdup("alter table"),$3,mm_strdup("rename"),$5,$6,mm_strdup("to"),$8);
}
|  ALTER TABLE IF_P EXISTS relation_expr RENAME opt_column name TO name
 { 
 $$ = cat_str(7,mm_strdup("alter table if exists"),$5,mm_strdup("rename"),$7,$8,mm_strdup("to"),$10);
}
|  ALTER TABLE relation_expr RENAME CONSTRAINT name TO name
 { 
 $$ = cat_str(6,mm_strdup("alter table"),$3,mm_strdup("rename constraint"),$6,mm_strdup("to"),$8);
}
|  ALTER TABLE IF_P EXISTS relation_expr RENAME CONSTRAINT name TO name
 { 
 $$ = cat_str(6,mm_strdup("alter table if exists"),$5,mm_strdup("rename constraint"),$8,mm_strdup("to"),$10);
}
|  ALTER TABLE relation_expr RENAME_PARTITION name TO name
 { 
 $$ = cat_str(6,mm_strdup("alter table"),$3,mm_strdup("rename partition"),$5,mm_strdup("to"),$7);
}
|  ALTER TABLE IF_P EXISTS relation_expr RENAME_PARTITION name TO name
 { 
 $$ = cat_str(6,mm_strdup("alter table if exists"),$5,mm_strdup("rename partition"),$7,mm_strdup("to"),$9);
}
|  ALTER TABLE relation_expr RENAME_PARTITION FOR '(' maxValueList ')' TO name
 { 
 $$ = cat_str(6,mm_strdup("alter table"),$3,mm_strdup("rename partition for ("),$7,mm_strdup(") to"),$10);
}
|  ALTER TABLE IF_P EXISTS relation_expr RENAME_PARTITION FOR '(' maxValueList ')' TO name
 { 
 $$ = cat_str(6,mm_strdup("alter table if exists"),$5,mm_strdup("rename partition for ("),$9,mm_strdup(") to"),$12);
}
|  ALTER FOREIGN TABLE relation_expr RENAME opt_column name TO name
 { 
 $$ = cat_str(7,mm_strdup("alter foreign table"),$4,mm_strdup("rename"),$6,$7,mm_strdup("to"),$9);
}
|  ALTER FOREIGN TABLE IF_P EXISTS relation_expr RENAME opt_column name TO name
 { 
 $$ = cat_str(7,mm_strdup("alter foreign table if exists"),$6,mm_strdup("rename"),$8,$9,mm_strdup("to"),$11);
}
|  ALTER TRIGGER name ON qualified_name RENAME TO name
 { 
 $$ = cat_str(6,mm_strdup("alter trigger"),$3,mm_strdup("on"),$5,mm_strdup("rename to"),$8);
}
|  ALTER ROLE RoleId RENAME TO RoleId
 { 
 $$ = cat_str(4,mm_strdup("alter role"),$3,mm_strdup("rename to"),$6);
}
|  ALTER USER UserId RENAME TO UserId
 { 
 $$ = cat_str(4,mm_strdup("alter user"),$3,mm_strdup("rename to"),$6);
}
|  ALTER TABLESPACE name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter tablespace"),$3,mm_strdup("rename to"),$6);
}
|  ALTER TABLESPACE name SET reloptions
 { 
 $$ = cat_str(4,mm_strdup("alter tablespace"),$3,mm_strdup("set"),$5);
}
|  ALTER TABLESPACE name RESET reloptions
 { 
 $$ = cat_str(4,mm_strdup("alter tablespace"),$3,mm_strdup("reset"),$5);
}
|  ALTER TABLESPACE name RESIZE MAXSIZE size_clause
 { 
 $$ = cat_str(4,mm_strdup("alter tablespace"),$3,mm_strdup("resize maxsize"),$6);
}
|  ALTER TEXT_P SEARCH PARSER any_name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter text search parser"),$5,mm_strdup("rename to"),$8);
}
|  ALTER TEXT_P SEARCH DICTIONARY any_name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter text search dictionary"),$5,mm_strdup("rename to"),$8);
}
|  ALTER TEXT_P SEARCH TEMPLATE any_name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter text search template"),$5,mm_strdup("rename to"),$8);
}
|  ALTER TEXT_P SEARCH CONFIGURATION any_name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter text search configuration"),$5,mm_strdup("rename to"),$8);
}
|  ALTER TYPE_P any_name RENAME TO name
 { 
 $$ = cat_str(4,mm_strdup("alter type"),$3,mm_strdup("rename to"),$6);
}
|  ALTER TYPE_P any_name RENAME ATTRIBUTE name TO name opt_drop_behavior
 { 
 $$ = cat_str(7,mm_strdup("alter type"),$3,mm_strdup("rename attribute"),$6,mm_strdup("to"),$8,$9);
}
|  RENAME TABLE rename_clause_list
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("rename table"),$3);
}
;


 rename_clause_list:
 rename_clause
 { 
 $$ = $1;
}
|  rename_clause_list ',' rename_clause
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 rename_clause:
 qualified_name TO qualified_name
 { 
 $$ = cat_str(3,$1,mm_strdup("to"),$3);
}
;


 opt_column:
 COLUMN
 { 
 $$ = mm_strdup("column");
}
|  %prec lower_than_index
 { 
 $$=EMPTY; }
;


 opt_set_data:
 SET DATA_P
 { 
 $$ = mm_strdup("set data");
}
| 
 { 
 $$=EMPTY; }
;


 AlterObjectSchemaStmt:
 ALTER AGGREGATE func_name aggr_args SET SCHEMA name
 { 
 $$ = cat_str(5,mm_strdup("alter aggregate"),$3,$4,mm_strdup("set schema"),$7);
}
|  ALTER COLLATION any_name SET SCHEMA name
 { 
 $$ = cat_str(4,mm_strdup("alter collation"),$3,mm_strdup("set schema"),$6);
}
|  ALTER CONVERSION_P any_name SET SCHEMA name
 { 
 $$ = cat_str(4,mm_strdup("alter conversion"),$3,mm_strdup("set schema"),$6);
}
|  ALTER DOMAIN_P any_name SET SCHEMA name
 { 
 $$ = cat_str(4,mm_strdup("alter domain"),$3,mm_strdup("set schema"),$6);
}
|  ALTER EXTENSION any_name SET SCHEMA name
 { 
 $$ = cat_str(4,mm_strdup("alter extension"),$3,mm_strdup("set schema"),$6);
}
|  ALTER FUNCTION function_with_argtypes SET SCHEMA name
 { 
 $$ = cat_str(4,mm_strdup("alter function"),$3,mm_strdup("set schema"),$6);
}
|  ALTER PROCEDURE function_with_argtypes SET SCHEMA name
 { 
 $$ = cat_str(4,mm_strdup("alter procedure"),$3,mm_strdup("set schema"),$6);
}
|  ALTER OPERATOR any_operator oper_argtypes SET SCHEMA name
 { 
 $$ = cat_str(5,mm_strdup("alter operator"),$3,$4,mm_strdup("set schema"),$7);
}
|  ALTER OPERATOR CLASS any_name USING access_method SET SCHEMA name
 { 
 $$ = cat_str(6,mm_strdup("alter operator class"),$4,mm_strdup("using"),$6,mm_strdup("set schema"),$9);
}
|  ALTER OPERATOR FAMILY any_name USING access_method SET SCHEMA name
 { 
 $$ = cat_str(6,mm_strdup("alter operator family"),$4,mm_strdup("using"),$6,mm_strdup("set schema"),$9);
}
|  ALTER TABLE relation_expr SET SCHEMA name
 { 
 $$ = cat_str(4,mm_strdup("alter table"),$3,mm_strdup("set schema"),$6);
}
|  ALTER TABLE IF_P EXISTS relation_expr SET SCHEMA name
 { 
 $$ = cat_str(4,mm_strdup("alter table if exists"),$5,mm_strdup("set schema"),$8);
}
|  ALTER TEXT_P SEARCH PARSER any_name SET SCHEMA name
 { 
 $$ = cat_str(4,mm_strdup("alter text search parser"),$5,mm_strdup("set schema"),$8);
}
|  ALTER TEXT_P SEARCH DICTIONARY any_name SET SCHEMA name
 { 
 $$ = cat_str(4,mm_strdup("alter text search dictionary"),$5,mm_strdup("set schema"),$8);
}
|  ALTER TEXT_P SEARCH TEMPLATE any_name SET SCHEMA name
 { 
 $$ = cat_str(4,mm_strdup("alter text search template"),$5,mm_strdup("set schema"),$8);
}
|  ALTER TEXT_P SEARCH CONFIGURATION any_name SET SCHEMA name
 { 
 $$ = cat_str(4,mm_strdup("alter text search configuration"),$5,mm_strdup("set schema"),$8);
}
|  ALTER SEQUENCE qualified_name SET SCHEMA name
 { 
 $$ = cat_str(4,mm_strdup("alter sequence"),$3,mm_strdup("set schema"),$6);
}
|  ALTER LARGE_P SEQUENCE qualified_name SET SCHEMA name
 { 
 $$ = cat_str(4,mm_strdup("alter large sequence"),$4,mm_strdup("set schema"),$7);
}
|  ALTER SEQUENCE IF_P EXISTS qualified_name SET SCHEMA name
 { 
 $$ = cat_str(4,mm_strdup("alter sequence if exists"),$5,mm_strdup("set schema"),$8);
}
|  ALTER LARGE_P SEQUENCE IF_P EXISTS qualified_name SET SCHEMA name
 { 
 $$ = cat_str(4,mm_strdup("alter large sequence if exists"),$6,mm_strdup("set schema"),$9);
}
|  ALTER VIEW qualified_name SET SCHEMA name
 { 
 $$ = cat_str(4,mm_strdup("alter view"),$3,mm_strdup("set schema"),$6);
}
|  ALTER VIEW IF_P EXISTS qualified_name SET SCHEMA name
 { 
 $$ = cat_str(4,mm_strdup("alter view if exists"),$5,mm_strdup("set schema"),$8);
}
|  ALTER MATERIALIZED VIEW qualified_name SET SCHEMA name
 { 
 $$ = cat_str(4,mm_strdup("alter materialized view"),$4,mm_strdup("set schema"),$7);
}
|  ALTER MATERIALIZED VIEW IF_P EXISTS qualified_name SET SCHEMA name
 { 
 $$ = cat_str(4,mm_strdup("alter materialized view if exists"),$6,mm_strdup("set schema"),$9);
}
|  ALTER FOREIGN TABLE relation_expr SET SCHEMA name
 { 
 $$ = cat_str(4,mm_strdup("alter foreign table"),$4,mm_strdup("set schema"),$7);
}
|  ALTER FOREIGN TABLE IF_P EXISTS relation_expr SET SCHEMA name
 { 
 $$ = cat_str(4,mm_strdup("alter foreign table if exists"),$6,mm_strdup("set schema"),$9);
}
|  ALTER TYPE_P any_name SET SCHEMA name
 { 
 $$ = cat_str(4,mm_strdup("alter type"),$3,mm_strdup("set schema"),$6);
}
;


 AlterOwnerStmt:
 ALTER AGGREGATE func_name aggr_args OWNER TO RoleId
 { 
 $$ = cat_str(5,mm_strdup("alter aggregate"),$3,$4,mm_strdup("owner to"),$7);
}
|  ALTER COLLATION any_name OWNER TO RoleId
 { 
 $$ = cat_str(4,mm_strdup("alter collation"),$3,mm_strdup("owner to"),$6);
}
|  ALTER CONVERSION_P any_name OWNER TO RoleId
 { 
 $$ = cat_str(4,mm_strdup("alter conversion"),$3,mm_strdup("owner to"),$6);
}
|  ALTER DATABASE database_name OWNER TO RoleId
 { 
 $$ = cat_str(4,mm_strdup("alter database"),$3,mm_strdup("owner to"),$6);
}
|  ALTER DIRECTORY name OWNER TO RoleId
 { 
 $$ = cat_str(4,mm_strdup("alter directory"),$3,mm_strdup("owner to"),$6);
}
|  ALTER DOMAIN_P any_name OWNER TO RoleId
 { 
 $$ = cat_str(4,mm_strdup("alter domain"),$3,mm_strdup("owner to"),$6);
}
|  ALTER FUNCTION function_with_argtypes OWNER TO RoleId
 { 
 $$ = cat_str(4,mm_strdup("alter function"),$3,mm_strdup("owner to"),$6);
}
|  ALTER PROCEDURE function_with_argtypes OWNER TO RoleId
 { 
 $$ = cat_str(4,mm_strdup("alter procedure"),$3,mm_strdup("owner to"),$6);
}
|  ALTER PACKAGE pkg_name OWNER TO RoleId
 { 
 $$ = cat_str(4,mm_strdup("alter package"),$3,mm_strdup("owner to"),$6);
}
|  ALTER opt_procedural LANGUAGE name OWNER TO RoleId
 { 
 $$ = cat_str(6,mm_strdup("alter"),$2,mm_strdup("language"),$4,mm_strdup("owner to"),$7);
}
|  ALTER LARGE_P OBJECT_P NumericOnly OWNER TO RoleId
 { 
 $$ = cat_str(4,mm_strdup("alter large object"),$4,mm_strdup("owner to"),$7);
}
|  ALTER OPERATOR any_operator oper_argtypes OWNER TO RoleId
 { 
 $$ = cat_str(5,mm_strdup("alter operator"),$3,$4,mm_strdup("owner to"),$7);
}
|  ALTER OPERATOR CLASS any_name USING access_method OWNER TO RoleId
 { 
 $$ = cat_str(6,mm_strdup("alter operator class"),$4,mm_strdup("using"),$6,mm_strdup("owner to"),$9);
}
|  ALTER OPERATOR FAMILY any_name USING access_method OWNER TO RoleId
 { 
 $$ = cat_str(6,mm_strdup("alter operator family"),$4,mm_strdup("using"),$6,mm_strdup("owner to"),$9);
}
|  ALTER SCHEMA name OWNER TO RoleId
 { 
 $$ = cat_str(4,mm_strdup("alter schema"),$3,mm_strdup("owner to"),$6);
}
|  ALTER TYPE_P any_name OWNER TO TypeOwner
 { 
 $$ = cat_str(4,mm_strdup("alter type"),$3,mm_strdup("owner to"),$6);
}
|  ALTER TABLESPACE name OWNER TO RoleId
 { 
 $$ = cat_str(4,mm_strdup("alter tablespace"),$3,mm_strdup("owner to"),$6);
}
|  ALTER TEXT_P SEARCH DICTIONARY any_name OWNER TO RoleId
 { 
 $$ = cat_str(4,mm_strdup("alter text search dictionary"),$5,mm_strdup("owner to"),$8);
}
|  ALTER TEXT_P SEARCH CONFIGURATION any_name OWNER TO RoleId
 { 
 $$ = cat_str(4,mm_strdup("alter text search configuration"),$5,mm_strdup("owner to"),$8);
}
|  ALTER FOREIGN DATA_P WRAPPER name OWNER TO RoleId
 { 
 $$ = cat_str(4,mm_strdup("alter foreign data wrapper"),$5,mm_strdup("owner to"),$8);
}
|  ALTER SERVER name OWNER TO RoleId
 { 
 $$ = cat_str(4,mm_strdup("alter server"),$3,mm_strdup("owner to"),$6);
}
|  ALTER DATA_P SOURCE_P name OWNER TO RoleId
 { 
 $$ = cat_str(4,mm_strdup("alter data source"),$4,mm_strdup("owner to"),$7);
}
|  ALTER SYNONYM any_name OWNER TO RoleId
 { 
 $$ = cat_str(4,mm_strdup("alter synonym"),$3,mm_strdup("owner to"),$6);
}
|  ALTER PUBLICATION name OWNER TO RoleId
 { 
 $$ = cat_str(4,mm_strdup("alter publication"),$3,mm_strdup("owner to"),$6);
}
|  ALTER SUBSCRIPTION name OWNER TO RoleId
 { 
 $$ = cat_str(4,mm_strdup("alter subscription"),$3,mm_strdup("owner to"),$6);
}
;


 CreatePublicationStmt:
 CREATE PUBLICATION name opt_publication_for_tables opt_definition
 { 
 $$ = cat_str(4,mm_strdup("create publication"),$3,$4,$5);
}
;


 opt_publication_for_tables:
 publication_for_tables
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 publication_for_tables:
 FOR TABLE relation_expr_list
 { 
 $$ = cat_str(2,mm_strdup("for table"),$3);
}
|  FOR ALL TABLES
 { 
 $$ = mm_strdup("for all tables");
}
;


 AlterPublicationStmt:
 ALTER PUBLICATION name SET definition
 { 
 $$ = cat_str(4,mm_strdup("alter publication"),$3,mm_strdup("set"),$5);
}
|  ALTER PUBLICATION name ADD_P TABLE relation_expr_list
 { 
 $$ = cat_str(4,mm_strdup("alter publication"),$3,mm_strdup("add table"),$6);
}
|  ALTER PUBLICATION name SET TABLE relation_expr_list
 { 
 $$ = cat_str(4,mm_strdup("alter publication"),$3,mm_strdup("set table"),$6);
}
|  ALTER PUBLICATION name DROP TABLE relation_expr_list
 { 
 $$ = cat_str(4,mm_strdup("alter publication"),$3,mm_strdup("drop table"),$6);
}
;


 CreateSubscriptionStmt:
 CREATE SUBSCRIPTION name CONNECTION ecpg_sconst PUBLICATION publication_name_list opt_definition
 { 
 $$ = cat_str(7,mm_strdup("create subscription"),$3,mm_strdup("connection"),$5,mm_strdup("publication"),$7,$8);
}
;


 publication_name_list:
 publication_name_item
 { 
 $$ = $1;
}
|  publication_name_list ',' publication_name_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 publication_name_item:
 ColLabel
 { 
 $$ = $1;
}
;


 AlterSubscriptionStmt:
 ALTER SUBSCRIPTION name SET definition
 { 
 $$ = cat_str(4,mm_strdup("alter subscription"),$3,mm_strdup("set"),$5);
}
|  ALTER SUBSCRIPTION name CONNECTION ecpg_sconst
 { 
 $$ = cat_str(4,mm_strdup("alter subscription"),$3,mm_strdup("connection"),$5);
}
|  ALTER SUBSCRIPTION name SET PUBLICATION publication_name_list
 { 
 $$ = cat_str(4,mm_strdup("alter subscription"),$3,mm_strdup("set publication"),$6);
}
|  ALTER SUBSCRIPTION name REFRESH PUBLICATION opt_definition
 { 
 $$ = cat_str(4,mm_strdup("alter subscription"),$3,mm_strdup("refresh publication"),$6);
}
|  ALTER SUBSCRIPTION name ENABLE_P
 { 
 $$ = cat_str(3,mm_strdup("alter subscription"),$3,mm_strdup("enable"));
}
;


 DropSubscriptionStmt:
 DROP SUBSCRIPTION name opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("drop subscription"),$3,$4);
}
|  DROP SUBSCRIPTION IF_P EXISTS name opt_drop_behavior
 { 
 $$ = cat_str(3,mm_strdup("drop subscription if exists"),$5,$6);
}
;


 TypeOwner:
 RoleId
 { 
 $$ = $1;
}
|  CURRENT_USER
 { 
 $$ = mm_strdup("current_user");
}
|  SESSION_USER
 { 
 $$ = mm_strdup("session_user");
}
;


 RuleStmt:
 CREATE opt_or_replace RULE name AS ON event TO qualified_name where_clause DO opt_instead RuleActionList
 { 
 $$ = cat_str(12,mm_strdup("create"),$2,mm_strdup("rule"),$4,mm_strdup("as on"),$7,mm_strdup("to"),$9,$10,mm_strdup("do"),$12,$13);
}
;


 RuleActionList:
 NOTHING
 { 
 $$ = mm_strdup("nothing");
}
|  RuleActionStmt
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = $1;
}
|  '(' RuleActionMulti ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
;


 RuleActionMulti:
 RuleActionMulti ';' RuleActionStmtOrEmpty
 { 
 $$ = cat_str(3,$1,mm_strdup(";"),$3);
}
|  RuleActionStmtOrEmpty
 { 
 $$ = $1;
}
;


 RuleActionStmt:
 SelectStmt
 { 
 $$ = $1;
}
|  InsertStmt
 { 
 $$ = $1;
}
|  UpdateStmt
 { 
 $$ = $1;
}
|  DeleteStmt
 { 
 $$ = $1;
}
|  CopyStmt
 { 
 $$ = $1;
}
|  NotifyStmt
 { 
 $$ = $1;
}
|  AlterTableStmt
 { 
 $$ = $1;
}
;


 RuleActionStmtOrEmpty:
 RuleActionStmt
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 event:
 SELECT
 { 
 $$ = mm_strdup("select");
}
|  UPDATE
 { 
 $$ = mm_strdup("update");
}
|  DELETE_P
 { 
 $$ = mm_strdup("delete");
}
|  INSERT
 { 
 $$ = mm_strdup("insert");
}
|  COPY
 { 
 $$ = mm_strdup("copy");
}
|  ALTER
 { 
 $$ = mm_strdup("alter");
}
;


 opt_instead:
 INSTEAD
 { 
 $$ = mm_strdup("instead");
}
|  ALSO
 { 
 $$ = mm_strdup("also");
}
| 
 { 
 $$=EMPTY; }
;


 DropRuleStmt:
 DROP RULE name ON any_name opt_drop_behavior
 { 
 $$ = cat_str(5,mm_strdup("drop rule"),$3,mm_strdup("on"),$5,$6);
}
|  DROP RULE IF_P EXISTS name ON any_name opt_drop_behavior
 { 
 $$ = cat_str(5,mm_strdup("drop rule if exists"),$5,mm_strdup("on"),$7,$8);
}
;


 NotifyStmt:
 NOTIFY ColId notify_payload
 { 
 $$ = cat_str(3,mm_strdup("notify"),$2,$3);
}
;


 notify_payload:
 ',' ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup(","),$2);
}
| 
 { 
 $$=EMPTY; }
;


 ListenStmt:
 LISTEN ColId
 { 
 $$ = cat_str(2,mm_strdup("listen"),$2);
}
;


 UnlistenStmt:
 UNLISTEN ColId
 { 
 $$ = cat_str(2,mm_strdup("unlisten"),$2);
}
|  UNLISTEN '*'
 { 
 $$ = mm_strdup("unlisten *");
}
;


 TransactionStmt:
 ABORT_P opt_transaction
 { 
 $$ = cat_str(2,mm_strdup("abort"),$2);
}
|  START TRANSACTION transaction_mode_list_or_empty
 { 
 $$ = cat_str(2,mm_strdup("start transaction"),$3);
}
	| BEGIN_P opt_transaction transaction_mode_list_or_empty
	{ 
	 $$ = cat_str(3,mm_strdup("begin"),$2,$3);
	}
|  COMMIT opt_transaction
 { 
 $$ = cat_str(2,mm_strdup("commit"),$2);
}
|  END_P opt_transaction
 { 
 $$ = cat_str(2,mm_strdup("end"),$2);
}
|  ROLLBACK opt_transaction
 { 
 $$ = cat_str(2,mm_strdup("rollback"),$2);
}
|  SAVEPOINT ColId
 { 
 $$ = cat_str(2,mm_strdup("savepoint"),$2);
}
|  RELEASE SAVEPOINT ColId
 { 
 $$ = cat_str(2,mm_strdup("release savepoint"),$3);
}
|  RELEASE ColId
 { 
 $$ = cat_str(2,mm_strdup("release"),$2);
}
|  ROLLBACK opt_transaction TO SAVEPOINT ColId
 { 
 $$ = cat_str(4,mm_strdup("rollback"),$2,mm_strdup("to savepoint"),$5);
}
|  ROLLBACK opt_transaction TO ColId
 { 
 $$ = cat_str(4,mm_strdup("rollback"),$2,mm_strdup("to"),$4);
}
|  PREPARE TRANSACTION ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("prepare transaction"),$3);
}
|  COMMIT PREPARED ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("commit prepared"),$3);
}
|  COMMIT PREPARED ecpg_sconst WITH ecpg_sconst
 { 
 $$ = cat_str(4,mm_strdup("commit prepared"),$3,mm_strdup("with"),$5);
}
|  ROLLBACK PREPARED ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("rollback prepared"),$3);
}
;


 opt_transaction:
 WORK
 { 
 $$ = mm_strdup("work");
}
|  TRANSACTION
 { 
 $$ = mm_strdup("transaction");
}
| 
 { 
 $$=EMPTY; }
;


 transaction_mode_item:
 ISOLATION LEVEL iso_level
 { 
 $$ = cat_str(2,mm_strdup("isolation level"),$3);
}
|  READ ONLY
 { 
 $$ = mm_strdup("read only");
}
|  READ WRITE
 { 
 $$ = mm_strdup("read write");
}
|  DEFERRABLE
 { 
 $$ = mm_strdup("deferrable");
}
|  NOT DEFERRABLE
 { 
 $$ = mm_strdup("not deferrable");
}
;


 transaction_mode_list:
 transaction_mode_item
 { 
 $$ = $1;
}
|  transaction_mode_list ',' transaction_mode_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
|  transaction_mode_list transaction_mode_item
 { 
 $$ = cat_str(2,$1,$2);
}
;


 transaction_mode_list_or_empty:
 transaction_mode_list
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 CreateContQueryStmt:
 CREATE CONTVIEW qualified_name opt_reloptions AS SelectStmt
 { 
 $$ = cat_str(5,mm_strdup("create contview"),$3,$4,mm_strdup("as"),$6);
}
|  CREATE OR REPLACE CONTVIEW qualified_name opt_reloptions AS SelectStmt
 { 
 $$ = cat_str(5,mm_strdup("create or replace contview"),$5,$6,mm_strdup("as"),$8);
}
;


 ViewStmt:
 CREATE OptTemp VIEW qualified_name opt_column_list opt_reloptions AS SelectStmt opt_check_option
 { 
 $$ = cat_str(9,mm_strdup("create"),$2,mm_strdup("view"),$4,$5,$6,mm_strdup("as"),$8,$9);
}
|  CREATE OR REPLACE OptTemp VIEW qualified_name opt_column_list opt_reloptions AS SelectStmt opt_check_option
 { 
 $$ = cat_str(9,mm_strdup("create or replace"),$4,mm_strdup("view"),$6,$7,$8,mm_strdup("as"),$10,$11);
}
|  CREATE opt_or_replace definer_expression OptTemp VIEW qualified_name opt_column_list opt_reloptions AS SelectStmt opt_check_option
 { 
 $$ = cat_str(11,mm_strdup("create"),$2,$3,$4,mm_strdup("view"),$6,$7,$8,mm_strdup("as"),$10,$11);
}
;


 opt_check_option:
 WITH CHECK OPTION
 { 
 $$ = mm_strdup("with check option");
}
|  WITH CASCADED CHECK OPTION
 { 
 $$ = mm_strdup("with cascaded check option");
}
|  WITH LOCAL CHECK OPTION
 { 
 $$ = mm_strdup("with local check option");
}
| 
 { 
 $$=EMPTY; }
;


 LoadStmt:
 LOAD file_name
 { 
 $$ = cat_str(2,mm_strdup("load"),$2);
}
|  LOAD opt_load_data opt_load_data_options_list load_type_set qualified_name load_oper_table_type load_table_options_list
 { 
 $$ = cat_str(7,mm_strdup("load"),$2,$3,$4,$5,$6,$7);
}
|  OPTIONS '(' load_options_list ')' LOAD opt_load_data opt_load_data_options_list load_type_set qualified_name load_oper_table_type load_table_options_list
 { 
 $$ = cat_str(9,mm_strdup("options ("),$3,mm_strdup(") load"),$6,$7,$8,$9,$10,$11);
}
;


 load_options_list:
 load_options_item
 { 
 $$ = $1;
}
|  load_options_list ',' load_options_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
| 
 { 
 $$=EMPTY; }
;


 load_options_item:
 ERRORS '=' NumericOnly
 { 
 $$ = cat_str(2,mm_strdup("errors ="),$3);
}
|  SKIP '=' NumericOnly
 { 
 $$ = cat_str(2,mm_strdup("skip ="),$3);
}
|  DATA_P '=' load_quote_str
 { 
 $$ = cat_str(2,mm_strdup("data ="),$3);
}
;


 opt_load_data:
 DATA_P
 { 
 $$ = mm_strdup("data");
}
| 
 { 
 $$=EMPTY; }
;


 opt_load_data_options_list:
 opt_load_data_options_list opt_load_data_options_item
 { 
 $$ = cat_str(2,$1,$2);
}
| 
 { 
 $$=EMPTY; }
;


 opt_load_data_options_item:
 INFILE load_quote_str
 { 
 $$ = cat_str(2,mm_strdup("infile"),$2);
}
|  CHARACTERSET load_quote_str
 { 
 $$ = cat_str(2,mm_strdup("characterset"),$2);
}
;


 load_oper_table_type:
 TRUNCATE
 { 
 $$ = mm_strdup("truncate");
}
|  APPEND
 { 
 $$ = mm_strdup("append");
}
|  REPLACE
 { 
 $$ = mm_strdup("replace");
}
|  INSERT
 { 
 $$ = mm_strdup("insert");
}
| 
 { 
 $$=EMPTY; }
;


 load_type_set:
 load_oper_table_type INTO TABLE
 { 
 $$ = cat_str(2,$1,mm_strdup("into table"));
}
;


 load_table_options_list:
 load_table_options_list load_table_options_item
 { 
 $$ = cat_str(2,$1,$2);
}
| 
 { 
 $$=EMPTY; }
;


 load_table_options_item:
 TRAILING NULLCOLS
 { 
 $$ = mm_strdup("trailing nullcols");
}
|  FIELDS CSV
 { 
 $$ = mm_strdup("fields csv");
}
|  FIELDS TERMINATED load_quote_str
 { 
 $$ = cat_str(2,mm_strdup("fields terminated"),$3);
}
|  FIELDS TERMINATED BY load_quote_str
 { 
 $$ = cat_str(2,mm_strdup("fields terminated by"),$4);
}
|  OPTIONALLY ENCLOSED BY load_quote_str
 { 
 $$ = cat_str(2,mm_strdup("optionally enclosed by"),$4);
}
|  load_when_option
 { 
 $$ = $1;
}
|  '(' load_column_expr_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
;


 load_column_expr_list:
 load_column_expr_item
 { 
 $$ = $1;
}
|  load_column_expr_list ',' load_column_expr_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 load_column_expr_item:
 ColId CONSTANT load_quote_str
 { 
 $$ = cat_str(3,$1,mm_strdup("constant"),$3);
}
|  ColId load_col_sequence
 { 
 $$ = cat_str(2,$1,$2);
}
|  ColId FILLER load_col_data_type
 { 
 $$ = cat_str(3,$1,mm_strdup("filler"),$3);
}
|  ColId load_col_scalar_spec
 { 
 $$ = cat_str(2,$1,$2);
}
;


 load_col_sequence_item_sart:
 Iconst
 { 
 $$ = $1;
}
|  ecpg_fconst
 { 
 $$ = $1;
}
|  MAXVALUE
 { 
 $$ = mm_strdup("maxvalue");
}
|  ROWS
 { 
 $$ = mm_strdup("rows");
}
;


 load_col_sequence:
 SEQUENCE '(' load_col_sequence_item_sart column_sequence_item_step ')'
 { 
 $$ = cat_str(4,mm_strdup("sequence ("),$3,$4,mm_strdup(")"));
}
;


 load_col_scalar_spec:
 load_col_position_spec load_col_data_type load_col_nullif_spec load_col_sql_str
 { 
 $$ = cat_str(4,$1,$2,$3,$4);
}
;


 load_col_position_spec:
 POSITION '(' Iconst '-' Iconst ')'
 { 
 $$ = cat_str(5,mm_strdup("position ("),$3,mm_strdup("-"),$5,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 load_col_nullif_spec:
 NULLIF '(' ColId '=' BLANKS ')'
 { 
 $$ = cat_str(3,mm_strdup("nullif ("),$3,mm_strdup("= blanks )"));
}
| 
 { 
 $$=EMPTY; }
;


 load_col_data_type:
 Numeric
 { 
 $$ = $1;
}
|  Numeric EXTERNAL
 { 
 $$ = cat_str(2,$1,mm_strdup("external"));
}
|  Character
 { 
 $$ = $1;
}
|  ConstDatetime
 { 
 $$ = $1;
}
|  ConstSet
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 load_col_sql_str:
 ecpg_ident
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 load_when_option:
 WHEN load_when_option_list
 { 
 $$ = cat_str(2,mm_strdup("when"),$2);
}
;


 load_when_option_list:
 load_when_option_item
 { 
 $$ = $1;
}
|  load_when_option_list AND load_when_option_item
 { 
 $$ = cat_str(3,$1,mm_strdup("and"),$3);
}
;


 load_when_option_item:
 '(' Iconst '-' Iconst ')' masking_policy_condition_operator load_quote_str
 { 
 $$ = cat_str(7,mm_strdup("("),$2,mm_strdup("-"),$4,mm_strdup(")"),$6,$7);
}
|  '(' Iconst ')' masking_policy_condition_operator load_quote_str
 { 
 $$ = cat_str(5,mm_strdup("("),$2,mm_strdup(")"),$4,$5);
}
|  ColId masking_policy_condition_operator load_quote_str
 { 
 $$ = cat_str(3,$1,$2,$3);
}
;


 load_quote_str:
 ecpg_ident
 { 
 $$ = $1;
}
|  ecpg_sconst CreatedbStmt:
 CREATE DATABASE database_name opt_with createdb_opt_list
 { 
 $$ = cat_str(4,mm_strdup("create database"),$3,$4,$5);
}
|  CREATE DATABASE IF_P NOT EXISTS database_name opt_with createdb_opt_list
 { 
 $$ = cat_str(4,mm_strdup("create database if not exists"),$6,$7,$8);
}
;


 createdb_opt_list:
 createdb_opt_list createdb_opt_item
 { 
 $$ = cat_str(2,$1,$2);
}
| 
 { 
 $$=EMPTY; }
;


 createdb_opt_item:
 TABLESPACE opt_equal name
 { 
 $$ = cat_str(3,mm_strdup("tablespace"),$2,$3);
}
|  TABLESPACE opt_equal DEFAULT
 { 
 $$ = cat_str(3,mm_strdup("tablespace"),$2,mm_strdup("default"));
}
|  LOCATION opt_equal ecpg_sconst
 { 
 $$ = cat_str(3,mm_strdup("location"),$2,$3);
}
|  LOCATION opt_equal DEFAULT
 { 
 $$ = cat_str(3,mm_strdup("location"),$2,mm_strdup("default"));
}
|  TEMPLATE opt_equal name
 { 
 $$ = cat_str(3,mm_strdup("template"),$2,$3);
}
|  TEMPLATE opt_equal DEFAULT
 { 
 $$ = cat_str(3,mm_strdup("template"),$2,mm_strdup("default"));
}
|  ENCODING opt_equal ecpg_sconst
 { 
 $$ = cat_str(3,mm_strdup("encoding"),$2,$3);
}
|  ENCODING opt_equal Iconst
 { 
 $$ = cat_str(3,mm_strdup("encoding"),$2,$3);
}
|  ENCODING opt_equal DEFAULT
 { 
 $$ = cat_str(3,mm_strdup("encoding"),$2,mm_strdup("default"));
}
|  LC_COLLATE_P opt_equal ecpg_sconst
 { 
 $$ = cat_str(3,mm_strdup("lc_collate"),$2,$3);
}
|  LC_COLLATE_P opt_equal DEFAULT
 { 
 $$ = cat_str(3,mm_strdup("lc_collate"),$2,mm_strdup("default"));
}
|  DBCOMPATIBILITY_P opt_equal ecpg_sconst
 { 
 $$ = cat_str(3,mm_strdup("dbcompatibility"),$2,$3);
}
|  DBCOMPATIBILITY_P opt_equal DEFAULT
 { 
 $$ = cat_str(3,mm_strdup("dbcompatibility"),$2,mm_strdup("default"));
}
|  LC_CTYPE_P opt_equal ecpg_sconst
 { 
 $$ = cat_str(3,mm_strdup("lc_ctype"),$2,$3);
}
|  LC_CTYPE_P opt_equal DEFAULT
 { 
 $$ = cat_str(3,mm_strdup("lc_ctype"),$2,mm_strdup("default"));
}
|  CONNECTION LIMIT opt_equal SignedIconst
 { 
 $$ = cat_str(3,mm_strdup("connection limit"),$3,$4);
}
|  OWNER opt_equal name
 { 
 $$ = cat_str(3,mm_strdup("owner"),$2,$3);
}
|  OWNER opt_equal DEFAULT
 { 
 $$ = cat_str(3,mm_strdup("owner"),$2,mm_strdup("default"));
}
;


 opt_equal:
 '='
 { 
 $$ = mm_strdup("=");
}
| 
 { 
 $$=EMPTY; }
;


 AlterDatabaseStmt:
 ALTER DATABASE database_name opt_with alterdb_opt_list
 { 
 $$ = cat_str(4,mm_strdup("alter database"),$3,$4,$5);
}
|  ALTER DATABASE database_name SET TABLESPACE name
 { 
 $$ = cat_str(4,mm_strdup("alter database"),$3,mm_strdup("set tablespace"),$6);
}
;


 AlterDatabaseSetStmt:
 ALTER DATABASE database_name SetResetClause
 { 
 $$ = cat_str(3,mm_strdup("alter database"),$3,$4);
}
;


 alterdb_opt_list:
 alterdb_opt_list alterdb_opt_item
 { 
 $$ = cat_str(2,$1,$2);
}
| 
 { 
 $$=EMPTY; }
;


 alterdb_opt_item:
 CONNECTION LIMIT opt_equal SignedIconst
 { 
 $$ = cat_str(3,mm_strdup("connection limit"),$3,$4);
}
|  ENABLE_P PRIVATE OBJECT_P
 { 
 $$ = mm_strdup("enable private object");
}
|  DISABLE_P PRIVATE OBJECT_P
 { 
 $$ = mm_strdup("disable private object");
}
;


 DropdbStmt:
 DROP DATABASE database_name
 { 
 $$ = cat_str(2,mm_strdup("drop database"),$3);
}
|  DROP DATABASE IF_P EXISTS database_name
 { 
 $$ = cat_str(2,mm_strdup("drop database if exists"),$5);
}
;


 CreateDomainStmt:
 CREATE DOMAIN_P any_name opt_as Typename ColQualList
 { 
 $$ = cat_str(5,mm_strdup("create domain"),$3,$4,$5,$6);
}
;


 AlterDomainStmt:
 ALTER DOMAIN_P any_name alter_column_default
 { 
 $$ = cat_str(3,mm_strdup("alter domain"),$3,$4);
}
|  ALTER DOMAIN_P any_name DROP NOT NULL_P
 { 
 $$ = cat_str(3,mm_strdup("alter domain"),$3,mm_strdup("drop not null"));
}
|  ALTER DOMAIN_P any_name SET NOT NULL_P
 { 
 $$ = cat_str(3,mm_strdup("alter domain"),$3,mm_strdup("set not null"));
}
|  ALTER DOMAIN_P any_name ADD_P TableConstraint
 { 
 $$ = cat_str(4,mm_strdup("alter domain"),$3,mm_strdup("add"),$5);
}
|  ALTER DOMAIN_P any_name DROP CONSTRAINT name opt_drop_behavior
 { 
 $$ = cat_str(5,mm_strdup("alter domain"),$3,mm_strdup("drop constraint"),$6,$7);
}
|  ALTER DOMAIN_P any_name DROP CONSTRAINT IF_P EXISTS name opt_drop_behavior
 { 
 $$ = cat_str(5,mm_strdup("alter domain"),$3,mm_strdup("drop constraint if exists"),$8,$9);
}
|  ALTER DOMAIN_P any_name VALIDATE CONSTRAINT name
 { 
 $$ = cat_str(4,mm_strdup("alter domain"),$3,mm_strdup("validate constraint"),$6);
}
;


 opt_as:
 AS
 { 
 $$ = mm_strdup("as");
}
| 
 { 
 $$=EMPTY; }
;


 AlterTSDictionaryStmt:
 ALTER TEXT_P SEARCH DICTIONARY any_name definition
 { 
 $$ = cat_str(3,mm_strdup("alter text search dictionary"),$5,$6);
}
;


 AlterTSConfigurationStmt:
 ALTER TEXT_P SEARCH CONFIGURATION any_name ADD_P MAPPING FOR name_list WITH any_name_list
 { 
 $$ = cat_str(6,mm_strdup("alter text search configuration"),$5,mm_strdup("add mapping for"),$9,mm_strdup("with"),$11);
}
|  ALTER TEXT_P SEARCH CONFIGURATION any_name ALTER MAPPING FOR name_list WITH any_name_list
 { 
 $$ = cat_str(6,mm_strdup("alter text search configuration"),$5,mm_strdup("alter mapping for"),$9,mm_strdup("with"),$11);
}
|  ALTER TEXT_P SEARCH CONFIGURATION any_name ALTER MAPPING REPLACE any_name WITH any_name
 { 
 $$ = cat_str(6,mm_strdup("alter text search configuration"),$5,mm_strdup("alter mapping replace"),$9,mm_strdup("with"),$11);
}
|  ALTER TEXT_P SEARCH CONFIGURATION any_name ALTER MAPPING FOR name_list REPLACE any_name WITH any_name
 { 
 $$ = cat_str(8,mm_strdup("alter text search configuration"),$5,mm_strdup("alter mapping for"),$9,mm_strdup("replace"),$11,mm_strdup("with"),$13);
}
|  ALTER TEXT_P SEARCH CONFIGURATION any_name DROP MAPPING FOR name_list
 { 
 $$ = cat_str(4,mm_strdup("alter text search configuration"),$5,mm_strdup("drop mapping for"),$9);
}
|  ALTER TEXT_P SEARCH CONFIGURATION any_name DROP MAPPING IF_P EXISTS FOR name_list
 { 
 $$ = cat_str(4,mm_strdup("alter text search configuration"),$5,mm_strdup("drop mapping if exists for"),$11);
}
|  ALTER TEXT_P SEARCH CONFIGURATION any_name SET cfoptions
 { 
 $$ = cat_str(4,mm_strdup("alter text search configuration"),$5,mm_strdup("set"),$7);
}
|  ALTER TEXT_P SEARCH CONFIGURATION any_name RESET cfoptions
 { 
 $$ = cat_str(4,mm_strdup("alter text search configuration"),$5,mm_strdup("reset"),$7);
}
;


 CreateConversionStmt:
 CREATE opt_default CONVERSION_P any_name FOR ecpg_sconst TO ecpg_sconst FROM any_name
 { 
 $$ = cat_str(10,mm_strdup("create"),$2,mm_strdup("conversion"),$4,mm_strdup("for"),$6,mm_strdup("to"),$8,mm_strdup("from"),$10);
}
;


 ClusterStmt:
 CLUSTER opt_verbose qualified_name cluster_index_specification
 { 
 $$ = cat_str(4,mm_strdup("cluster"),$2,$3,$4);
}
|  CLUSTER opt_verbose qualified_name PARTITION '(' name ')' cluster_index_specification
 { 
 $$ = cat_str(7,mm_strdup("cluster"),$2,$3,mm_strdup("partition ("),$6,mm_strdup(")"),$8);
}
|  CLUSTER opt_verbose
 { 
 $$ = cat_str(2,mm_strdup("cluster"),$2);
}
|  CLUSTER opt_verbose index_name ON qualified_name
 { 
 $$ = cat_str(5,mm_strdup("cluster"),$2,$3,mm_strdup("on"),$5);
}
;


 cluster_index_specification:
 USING index_name
 { 
 $$ = cat_str(2,mm_strdup("using"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 VacuumStmt:
 VACUUM opt_deltamerge
 { 
 $$ = cat_str(2,mm_strdup("vacuum"),$2);
}
|  VACUUM opt_deltamerge qualified_name
 { 
 $$ = cat_str(3,mm_strdup("vacuum"),$2,$3);
}
|  VACUUM opt_hdfsdirectory
 { 
 $$ = cat_str(2,mm_strdup("vacuum"),$2);
}
|  VACUUM opt_hdfsdirectory qualified_name
 { 
 $$ = cat_str(3,mm_strdup("vacuum"),$2,$3);
}
|  VACUUM opt_full opt_freeze opt_verbose opt_compact
 { 
 $$ = cat_str(5,mm_strdup("vacuum"),$2,$3,$4,$5);
}
|  VACUUM opt_full opt_freeze opt_verbose opt_compact qualified_name
 { 
 $$ = cat_str(6,mm_strdup("vacuum"),$2,$3,$4,$5,$6);
}
|  VACUUM opt_full opt_freeze opt_verbose opt_compact qualified_name PARTITION '('name')'
 { 
 $$ = cat_str(7,mm_strdup("vacuum"),$2,$3,$4,$5,$6,mm_strdup("partition (name)"));
}
|  VACUUM opt_full opt_freeze opt_verbose opt_compact qualified_name SUBPARTITION '('name')'
 { 
 $$ = cat_str(7,mm_strdup("vacuum"),$2,$3,$4,$5,$6,mm_strdup("subpartition (name)"));
}
|  VACUUM opt_full opt_freeze opt_verbose opt_compact AnalyzeStmt
 { 
 $$ = cat_str(6,mm_strdup("vacuum"),$2,$3,$4,$5,$6);
}
|  VACUUM '(' vacuum_option_list ')'
 { 
 $$ = cat_str(3,mm_strdup("vacuum ("),$3,mm_strdup(")"));
}
|  VACUUM '(' vacuum_option_list ')' qualified_name opt_name_list
 { 
 $$ = cat_str(5,mm_strdup("vacuum ("),$3,mm_strdup(")"),$5,$6);
}
|  VACUUM '(' vacuum_option_list ')' qualified_name opt_name_list PARTITION '('name')'
 { 
 $$ = cat_str(6,mm_strdup("vacuum ("),$3,mm_strdup(")"),$5,$6,mm_strdup("partition (name)"));
}
|  VACUUM '(' vacuum_option_list ')' qualified_name opt_name_list SUBPARTITION '('name')'
 { 
 $$ = cat_str(6,mm_strdup("vacuum ("),$3,mm_strdup(")"),$5,$6,mm_strdup("subpartition (name)"));
}
;


 vacuum_option_list:
 vacuum_option_elem
 { 
 $$ = $1;
}
|  vacuum_option_list ',' vacuum_option_elem
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 vacuum_option_elem:
 analyze_keyword
 { 
 $$ = $1;
}
|  VERBOSE
 { 
 $$ = mm_strdup("verbose");
}
|  FREEZE
 { 
 $$ = mm_strdup("freeze");
}
|  FULL
 { 
 $$ = mm_strdup("full");
}
;


 AnalyzeStmt:
 analyze_keyword opt_verbose
 { 
 $$ = cat_str(2,$1,$2);
}
|  analyze_keyword opt_verbose qualified_name opt_analyze_column_define
 { 
 $$ = cat_str(4,$1,$2,$3,$4);
}
|  analyze_keyword opt_verbose qualified_name opt_name_list PARTITION '('name')'
 { 
 $$ = cat_str(5,$1,$2,$3,$4,mm_strdup("partition (name)"));
}
|  analyze_keyword opt_verbose FOREIGN TABLES
 { 
 $$ = cat_str(3,$1,$2,mm_strdup("foreign tables"));
}
;


 VerifyStmt:
 analyze_keyword opt_verify opt_verify_options
 { 
 $$ = cat_str(3,$1,$2,$3);
}
|  analyze_keyword opt_verify opt_verify_options qualified_name opt_cascade
 { 
 $$ = cat_str(5,$1,$2,$3,$4,$5);
}
|  analyze_keyword opt_verify opt_verify_options qualified_name PARTITION '('name')' opt_cascade
 { 
 $$ = cat_str(6,$1,$2,$3,$4,mm_strdup("partition (name)"),$7);
}
;


 analyze_keyword:
 ANALYZE
 { 
 $$ = mm_strdup("analyze");
}
|  ANALYSE
 { 
 $$ = mm_strdup("analyse");
}
;


 opt_verify_options:
 FAST
 { 
 $$ = mm_strdup("fast");
}
|  COMPLETE
 { 
 $$ = mm_strdup("complete");
}
;


 opt_verbose:
 VERBOSE
 { 
 $$ = mm_strdup("verbose");
}
| 
 { 
 $$=EMPTY; }
;


 opt_full:
 FULL
 { 
 $$ = mm_strdup("full");
}
| 
 { 
 $$=EMPTY; }
;


 opt_compact:
 COMPACT
 { 
 $$ = mm_strdup("compact");
}
| 
 { 
 $$=EMPTY; }
;


 opt_hdfsdirectory:
 HDFSDIRECTORY
 { 
 $$ = mm_strdup("hdfsdirectory");
}
;


 opt_freeze:
 FREEZE
 { 
 $$ = mm_strdup("freeze");
}
| 
 { 
 $$=EMPTY; }
;


 opt_deltamerge:
 DELTAMERGE
 { 
 $$ = mm_strdup("deltamerge");
}
;


 opt_verify:
 VERIFY
 { 
 $$ = mm_strdup("verify");
}
;


 opt_cascade:
 CASCADE
 { 
 $$ = mm_strdup("cascade");
}
| 
 { 
 $$=EMPTY; }
;


 opt_name_list:
 '(' name_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 opt_analyze_column_define:
 '(' opt_multi_name_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
|  '(' name_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 opt_multi_name_list:
 '(' name_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
;


 BarrierStmt:
 CREATE BARRIER opt_barrier_id
 { 
 $$ = cat_str(2,mm_strdup("create barrier"),$3);
}
;


 opt_barrier_id:
 ecpg_sconst
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 CreateNodeStmt:
 CREATE NODE pgxcnode_name OptWith
 { 
 $$ = cat_str(3,mm_strdup("create node"),$3,$4);
}
;


 pgxcnode_name:
 ColId
 { 
 $$ = $1;
}
;


 pgxcgroup_name:
 ColId
 { 
 $$ = $1;
}
;


 pgxcnodes:
 '(' pgxcnode_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
;


 pgxcnode_list:
 pgxcnode_list ',' pgxcnode_name
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
|  pgxcnode_name
 { 
 $$ = $1;
}
;


 bucket_maps:
 BUCKETS '(' bucket_list ')'
 { 
 $$ = cat_str(3,mm_strdup("buckets ("),$3,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 bucket_list:
 bucket_list ',' Iconst
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
|  Iconst
 { 
 $$ = $1;
}
;


 bucket_cnt:
 BUCKETCNT Iconst
 { 
 $$ = cat_str(2,mm_strdup("bucketcnt"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 pgxcgroup_parent:
 GROUPPARENT pgxcnode_name
 { 
 $$ = cat_str(2,mm_strdup("groupparent"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 opt_vcgroup:
 VCGROUP
 { 
 $$ = mm_strdup("vcgroup");
}
| 
 { 
 $$=EMPTY; }
;


 opt_to_elastic_group:
 TO ELASTIC GROUP_P
 { 
 $$ = mm_strdup("to elastic group");
}
| 
 { 
 $$=EMPTY; }
;


 opt_redistributed:
 DISTRIBUTE FROM ColId
 { 
 $$ = cat_str(2,mm_strdup("distribute from"),$3);
}
| 
 { 
 $$=EMPTY; }
;


 opt_set_vcgroup:
 SET VCGROUP
 { 
 $$ = mm_strdup("set vcgroup");
}
|  SET NOT VCGROUP
 { 
 $$ = mm_strdup("set not vcgroup");
}
| 
 { 
 $$=EMPTY; }
;


 AlterNodeStmt:
 ALTER NODE pgxcnode_name OptWith
 { 
 $$ = cat_str(3,mm_strdup("alter node"),$3,$4);
}
;


 AlterCoordinatorStmt:
 ALTER COORDINATOR pgxcnode_name SET opt_boolean_or_string WITH pgxcnodes
 { 
 $$ = cat_str(6,mm_strdup("alter coordinator"),$3,mm_strdup("set"),$5,mm_strdup("with"),$7);
}
;


 DropNodeStmt:
 DROP NODE pgxcnode_name opt_pgxcnodes
 { 
 $$ = cat_str(3,mm_strdup("drop node"),$3,$4);
}
|  DROP NODE IF_P EXISTS pgxcnode_name opt_pgxcnodes
 { 
 $$ = cat_str(3,mm_strdup("drop node if exists"),$5,$6);
}
;


 opt_pgxcnodes:
 WITH pgxcnodes
 { 
 $$ = cat_str(2,mm_strdup("with"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 CreateNodeGroupStmt:
 CREATE NODE GROUP_P pgxcgroup_name opt_pgxcnodes bucket_maps opt_vcgroup opt_redistributed bucket_cnt pgxcgroup_parent
 { 
 $$ = cat_str(8,mm_strdup("create node group"),$4,$5,$6,$7,$8,$9,$10);
}
;


 AlterNodeGroupStmt:
 ALTER NODE GROUP_P pgxcgroup_name SET DEFAULT
 { 
 $$ = cat_str(3,mm_strdup("alter node group"),$4,mm_strdup("set default"));
}
|  ALTER NODE GROUP_P pgxcgroup_name opt_set_vcgroup RENAME TO pgxcgroup_name
 { 
 $$ = cat_str(5,mm_strdup("alter node group"),$4,$5,mm_strdup("rename to"),$8);
}
|  ALTER NODE GROUP_P pgxcgroup_name SET NOT VCGROUP
 { 
 $$ = cat_str(3,mm_strdup("alter node group"),$4,mm_strdup("set not vcgroup"));
}
|  ALTER NODE GROUP_P pgxcgroup_name SET TABLE GROUP_P pgxcgroup_name
 { 
 $$ = cat_str(4,mm_strdup("alter node group"),$4,mm_strdup("set table group"),$8);
}
|  ALTER NODE GROUP_P pgxcgroup_name COPY BUCKETS FROM pgxcgroup_name
 { 
 $$ = cat_str(4,mm_strdup("alter node group"),$4,mm_strdup("copy buckets from"),$8);
}
|  ALTER NODE GROUP_P pgxcgroup_name ADD_P NODE pgxcnodes
 { 
 $$ = cat_str(4,mm_strdup("alter node group"),$4,mm_strdup("add node"),$7);
}
|  ALTER NODE GROUP_P pgxcgroup_name DELETE_P NODE pgxcnodes
 { 
 $$ = cat_str(4,mm_strdup("alter node group"),$4,mm_strdup("delete node"),$7);
}
|  ALTER NODE GROUP_P pgxcgroup_name RESIZE TO pgxcgroup_name
 { 
 $$ = cat_str(4,mm_strdup("alter node group"),$4,mm_strdup("resize to"),$7);
}
|  ALTER NODE GROUP_P pgxcgroup_name opt_set_vcgroup WITH GROUP_P pgxcgroup_name
 { 
 $$ = cat_str(5,mm_strdup("alter node group"),$4,$5,mm_strdup("with group"),$8);
}
|  ALTER NODE GROUP_P pgxcgroup_name SET SEQUENCE TO ALL NODE
 { 
 $$ = cat_str(3,mm_strdup("alter node group"),$4,mm_strdup("set sequence to all node"));
}
|  ALTER NODE GROUP_P pgxcgroup_name SET SEQUENCE TO LOCAL
 { 
 $$ = cat_str(3,mm_strdup("alter node group"),$4,mm_strdup("set sequence to local"));
}
;


 DropNodeGroupStmt:
 DROP NODE GROUP_P pgxcgroup_name opt_redistributed opt_to_elastic_group
 { 
 $$ = cat_str(4,mm_strdup("drop node group"),$4,$5,$6);
}
;


 CreateAuditPolicyStmt:
 CREATE AUDIT POLICY policy_name PRIVILEGES policy_privileges_list policy_filter_opt policy_status_opt
 { 
 $$ = cat_str(6,mm_strdup("create audit policy"),$4,mm_strdup("privileges"),$6,$7,$8);
}
|  CREATE AUDIT POLICY IF_P NOT EXISTS policy_name PRIVILEGES policy_privileges_list policy_filter_opt policy_status_opt
 { 
 $$ = cat_str(6,mm_strdup("create audit policy if not exists"),$7,mm_strdup("privileges"),$9,$10,$11);
}
|  CREATE AUDIT POLICY policy_name ACCESS policy_access_list policy_filter_opt policy_status_opt
 { 
 $$ = cat_str(6,mm_strdup("create audit policy"),$4,mm_strdup("access"),$6,$7,$8);
}
|  CREATE AUDIT POLICY IF_P NOT EXISTS policy_name ACCESS policy_access_list policy_filter_opt policy_status_opt
 { 
 $$ = cat_str(6,mm_strdup("create audit policy if not exists"),$7,mm_strdup("access"),$9,$10,$11);
}
;


 policy_privileges_list:
 policy_privilege_elem
 { 
 $$ = $1;
}
|  policy_privileges_list ',' policy_privilege_elem
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 policy_privilege_elem:
 policy_privilege_type policy_target_elem_opt
 { 
 $$ = cat_str(2,$1,$2);
}
;


 policy_privilege_type:
 ALL
 { 
 $$ = mm_strdup("all");
}
|  ALTER
 { 
 $$ = mm_strdup("alter");
}
|  ANALYZE
 { 
 $$ = mm_strdup("analyze");
}
|  COMMENT
 { 
 $$ = mm_strdup("comment");
}
|  CREATE
 { 
 $$ = mm_strdup("create");
}
|  DROP
 { 
 $$ = mm_strdup("drop");
}
|  GRANT
 { 
 $$ = mm_strdup("grant");
}
|  REVOKE
 { 
 $$ = mm_strdup("revoke");
}
|  SET
 { 
 $$ = mm_strdup("set");
}
|  SHOW
 { 
 $$ = mm_strdup("show");
}
|  LOGIN_ANY
 { 
 $$ = mm_strdup("login_any");
}
|  LOGIN_FAILURE
 { 
 $$ = mm_strdup("login_failure");
}
|  LOGIN_SUCCESS
 { 
 $$ = mm_strdup("login_success");
}
|  LOGOUT
 { 
 $$ = mm_strdup("logout");
}
;


 policy_access_list:
 policy_access_elem
 { 
 $$ = $1;
}
|  policy_access_list ',' policy_access_elem
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 policy_access_elem:
 policy_access_type policy_target_elem_opt
 { 
 $$ = cat_str(2,$1,$2);
}
;


 policy_access_type:
 ALL
 { 
 $$ = mm_strdup("all");
}
|  COPY
 { 
 $$ = mm_strdup("copy");
}
|  DEALLOCATE
 { 
 $$ = mm_strdup("deallocate");
}
|  DELETE_P
 { 
 $$ = mm_strdup("delete");
}
|  EXECUTE
 { 
 $$ = mm_strdup("execute");
}
|  INSERT
 { 
 $$ = mm_strdup("insert");
}
|  PREPARE
 { 
 $$ = mm_strdup("prepare");
}
|  REINDEX
 { 
 $$ = mm_strdup("reindex");
}
|  SELECT
 { 
 $$ = mm_strdup("select");
}
|  TRUNCATE
 { 
 $$ = mm_strdup("truncate");
}
|  UPDATE
 { 
 $$ = mm_strdup("update");
}
;


 policy_target_elem_opt:
 ON policy_target_type '(' policy_targets_list ')'
 { 
 $$ = cat_str(5,mm_strdup("on"),$2,mm_strdup("("),$4,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 policy_targets_list:
 policy_target_name
 { 
 $$ = $1;
}
|  policy_targets_list ',' policy_target_name
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 policy_target_type:
 LABEL
 { 
 $$ = mm_strdup("label");
}
;


 policy_target_name:
 qualified_name
 { 
 $$ = $1;
}
;


 policy_filter_opt:
 FILTER ON policy_filter_value
 { 
 $$ = cat_str(2,mm_strdup("filter on"),$3);
}
| 
 { 
 $$=EMPTY; }
;


 policy_filter_value:
 filter_expr
 { 
 $$ = $1;
}
|  filter_set
 { 
 $$ = $1;
}
;


 filter_set:
 filter_paren
 { 
 $$ = $1;
}
|  '(' filter_expr_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
|  filter_expr_list
 { 
 $$ = $1;
}
;


 filter_expr_list:
 filter_expr ',' filter_expr
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
|  filter_expr_list ',' filter_expr
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 filter_paren:
 '(' filter_expr ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
;


 policy_filters_list:
 policy_filter_name
 { 
 $$ = $1;
}
|  policy_filters_list ',' policy_filter_name
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 policy_filter_type:
 IP
 { 
 $$ = mm_strdup("ip");
}
|  APP
 { 
 $$ = mm_strdup("app");
}
|  ROLES
 { 
 $$ = mm_strdup("roles");
}
;


 policy_filter_name:
 ColId_or_Sconst
 { 
 $$ = $1;
}
;


 policy_name:
 ColLabel
 { 
 $$ = $1;
}
;


 policy_status_opt:
 ENABLE_P
 { 
 $$ = mm_strdup("enable");
}
|  DISABLE_P
 { 
 $$ = mm_strdup("disable");
}
| 
 { 
 $$=EMPTY; }
;


 AlterAuditPolicyStmt:
 ALTER AUDIT POLICY policy_name policy_comments_alter_clause
 { 
 $$ = cat_str(3,mm_strdup("alter audit policy"),$4,$5);
}
|  ALTER AUDIT POLICY IF_P EXISTS policy_name policy_comments_alter_clause
 { 
 $$ = cat_str(3,mm_strdup("alter audit policy if exists"),$6,$7);
}
|  ALTER AUDIT POLICY policy_name alter_policy_action_clause alter_policy_privileges_list
 { 
 $$ = cat_str(4,mm_strdup("alter audit policy"),$4,$5,$6);
}
|  ALTER AUDIT POLICY IF_P EXISTS policy_name alter_policy_action_clause alter_policy_privileges_list
 { 
 $$ = cat_str(4,mm_strdup("alter audit policy if exists"),$6,$7,$8);
}
|  ALTER AUDIT POLICY policy_name alter_policy_action_clause alter_policy_access_list
 { 
 $$ = cat_str(4,mm_strdup("alter audit policy"),$4,$5,$6);
}
|  ALTER AUDIT POLICY IF_P EXISTS policy_name alter_policy_action_clause alter_policy_access_list
 { 
 $$ = cat_str(4,mm_strdup("alter audit policy if exists"),$6,$7,$8);
}
|  ALTER AUDIT POLICY policy_name MODIFY_P '(' alter_policy_filter_list ')'
 { 
 $$ = cat_str(5,mm_strdup("alter audit policy"),$4,mm_strdup("modify ("),$7,mm_strdup(")"));
}
|  ALTER AUDIT POLICY IF_P EXISTS policy_name MODIFY_P '(' alter_policy_filter_list ')'
 { 
 $$ = cat_str(5,mm_strdup("alter audit policy if exists"),$6,mm_strdup("modify ("),$9,mm_strdup(")"));
}
|  ALTER AUDIT POLICY policy_name DROP FILTER
 { 
 $$ = cat_str(3,mm_strdup("alter audit policy"),$4,mm_strdup("drop filter"));
}
|  ALTER AUDIT POLICY IF_P EXISTS policy_name DROP FILTER
 { 
 $$ = cat_str(3,mm_strdup("alter audit policy if exists"),$6,mm_strdup("drop filter"));
}
|  ALTER AUDIT POLICY policy_name policy_status_alter_clause
 { 
 $$ = cat_str(3,mm_strdup("alter audit policy"),$4,$5);
}
|  ALTER AUDIT POLICY IF_P EXISTS policy_name policy_status_alter_clause
 { 
 $$ = cat_str(3,mm_strdup("alter audit policy if exists"),$6,$7);
}
;


 alter_policy_access_list:
 ACCESS '(' policy_access_list ')'
 { 
 $$ = cat_str(3,mm_strdup("access ("),$3,mm_strdup(")"));
}
;


 alter_policy_privileges_list:
 PRIVILEGES '(' policy_privileges_list ')'
 { 
 $$ = cat_str(3,mm_strdup("privileges ("),$3,mm_strdup(")"));
}
;


 alter_policy_filter_list:
 FILTER ON policy_filter_value
 { 
 $$ = cat_str(2,mm_strdup("filter on"),$3);
}
;


 alter_policy_action_clause:
 ADD_P
 { 
 $$ = mm_strdup("add");
}
|  REMOVE
 { 
 $$ = mm_strdup("remove");
}
;


 policy_status_alter_clause:
 ENABLE_P
 { 
 $$ = mm_strdup("enable");
}
|  DISABLE_P
 { 
 $$ = mm_strdup("disable");
}
;


 policy_comments_alter_clause:
 COMMENTS ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("comments"),$2);
}
;


 DropAuditPolicyStmt:
 DROP AUDIT POLICY policy_names_list
 { 
 $$ = cat_str(2,mm_strdup("drop audit policy"),$4);
}
|  DROP AUDIT POLICY IF_P EXISTS policy_names_list
 { 
 $$ = cat_str(2,mm_strdup("drop audit policy if exists"),$6);
}
;


 policy_names_list:
 policy_name
 { 
 $$ = $1;
}
|  policy_names_list ',' policy_name
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 CreateMaskingPolicyStmt:
 CREATE MASKING POLICY policy_name masking_clause policy_condition_opt policy_filter_opt policy_status_opt
 { 
 $$ = cat_str(6,mm_strdup("create masking policy"),$4,$5,$6,$7,$8);
}
;


 masking_clause:
 masking_clause_elem
 { 
 $$ = $1;
}
|  masking_clause ',' masking_clause_elem
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 masking_clause_elem:
 masking_func masking_func_params_opt ON masking_target
 { 
 $$ = cat_str(4,$1,$2,mm_strdup("on"),$4);
}
|  masking_func_nsp '.' masking_func masking_func_params_opt ON masking_target
 { 
 $$ = cat_str(6,$1,mm_strdup("."),$3,$4,mm_strdup("on"),$6);
}
;


 masking_func_nsp:
 ColLabel
 { 
 $$ = $1;
}
;


 masking_func:
 ColLabel
 { 
 $$ = $1;
}
;


 masking_func_params_opt:
 '(' masking_func_params_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 masking_func_params_list:
 masking_func_param
 { 
 $$ = $1;
}
|  masking_func_params_list ',' masking_func_param
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 masking_func_param:
 Iconst
 { 
 $$ = $1;
}
|  ecpg_fconst
 { 
 $$ = $1;
}
|  ecpg_sconst
 { 
 $$ = $1;
}
|  ColLabel
 { 
 $$ = $1;
}
;


 masking_target:
 masking_policy_target_type '(' policy_targets_list ')'
 { 
 $$ = cat_str(4,$1,mm_strdup("("),$3,mm_strdup(")"));
}
;


 masking_policy_target_type:
 LABEL
 { 
 $$ = mm_strdup("label");
}
;


 alter_policy_condition:
 CONDITION '(' policy_label_item masking_policy_condition_operator masking_policy_condition_value ')'
 { 
 $$ = cat_str(5,mm_strdup("condition ("),$3,$4,$5,mm_strdup(")"));
}
;


 policy_condition_opt:
 CONDITION '(' policy_label_item masking_policy_condition_operator masking_policy_condition_value ')'
 { 
 $$ = cat_str(5,mm_strdup("condition ("),$3,$4,$5,mm_strdup(")"));
}
;


;


 masking_policy_condition_operator:
 CmpOp
 { 
 $$ = mm_strdup("cmpop");
}
|  '<'
 { 
 $$ = mm_strdup("<");
}
|  '>'
 { 
 $$ = mm_strdup(">");
}
|  '='
 { 
 $$ = mm_strdup("=");
}
|  '@'
 { 
 $$ = mm_strdup("@");
}
;


 masking_policy_condition_value:
 opt_boolean_or_string
 { 
 $$ = $1;
}
|  NumericOnly
 { 
 $$ = $1;
}
|  CURRENT_USER
 { 
 $$ = mm_strdup("current_user");
}
;


 pp_filter_expr:
 filter_expr
 { 
 $$ = $1;
}
|  filter_paren
 { 
 $$ = $1;
}
;


 filter_expr:
 filter_term
 { 
 $$ = $1;
}
|  pp_filter_expr OR pp_filter_term
 { 
 $$ = cat_str(3,$1,mm_strdup("or"),$3);
}
;


 pp_filter_term:
 filter_term
 { 
 $$ = $1;
}
|  filter_paren
 { 
 $$ = $1;
}
;


 filter_term:
 policy_filter_elem
 { 
 $$ = $1;
}
|  pp_filter_term AND pp_policy_filter_elem
 { 
 $$ = cat_str(3,$1,mm_strdup("and"),$3);
}
;


 pp_policy_filter_elem:
 policy_filter_elem
 { 
 $$ = $1;
}
|  filter_paren
 { 
 $$ = $1;
}
;


 policy_filter_elem:
 policy_filter_type '(' policy_filters_list ')'
 { 
 $$ = cat_str(4,$1,mm_strdup("("),$3,mm_strdup(")"));
}
|  policy_filter_type NOT '(' policy_filters_list ')'
 { 
 $$ = cat_str(4,$1,mm_strdup("not ("),$4,mm_strdup(")"));
}
;


 AlterMaskingPolicyStmt:
 ALTER MASKING POLICY policy_name policy_comments_alter_clause
 { 
 $$ = cat_str(3,mm_strdup("alter masking policy"),$4,$5);
}
|  ALTER MASKING POLICY policy_name alter_masking_policy_action_clause alter_masking_policy_func_items_list
 { 
 $$ = cat_str(4,mm_strdup("alter masking policy"),$4,$5,$6);
}
|  ALTER MASKING POLICY policy_name MODIFY_P '(' alter_policy_filter_list ')'
 { 
 $$ = cat_str(5,mm_strdup("alter masking policy"),$4,mm_strdup("modify ("),$7,mm_strdup(")"));
}
|  ALTER MASKING POLICY policy_name DROP FILTER
 { 
 $$ = cat_str(3,mm_strdup("alter masking policy"),$4,mm_strdup("drop filter"));
}
|  ALTER MASKING POLICY policy_name MODIFY_P '(' alter_policy_condition ')'
 { 
 $$ = cat_str(5,mm_strdup("alter masking policy"),$4,mm_strdup("modify ("),$7,mm_strdup(")"));
}
|  ALTER MASKING POLICY policy_name DROP CONDITION
 { 
 $$ = cat_str(3,mm_strdup("alter masking policy"),$4,mm_strdup("drop condition"));
}
|  ALTER MASKING POLICY policy_name policy_status_alter_clause
 { 
 $$ = cat_str(3,mm_strdup("alter masking policy"),$4,$5);
}
;


 alter_masking_policy_action_clause:
 MODIFY_P
 { 
 $$ = mm_strdup("modify");
}
|  alter_policy_action_clause
 { 
 $$ = $1;
}
;


 alter_masking_policy_func_items_list:
 alter_masking_policy_func_item
 { 
 $$ = $1;
}
|  alter_masking_policy_func_items_list ',' alter_masking_policy_func_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 alter_masking_policy_func_item:
 masking_clause_elem
 { 
 $$ = $1;
}
;


 DropMaskingPolicyStmt:
 DROP MASKING POLICY policy_names_list
 { 
 $$ = cat_str(2,mm_strdup("drop masking policy"),$4);
}
|  DROP MASKING POLICY IF_P EXISTS policy_names_list
 { 
 $$ = cat_str(2,mm_strdup("drop masking policy if exists"),$6);
}
;


 CreatePolicyLabelStmt:
 CREATE RESOURCE LABEL policy_label_name opt_add_resources_to_label
 { 
 $$ = cat_str(3,mm_strdup("create resource label"),$4,$5);
}
|  CREATE RESOURCE LABEL IF_P NOT EXISTS policy_label_name opt_add_resources_to_label
 { 
 $$ = cat_str(3,mm_strdup("create resource label if not exists"),$7,$8);
}
;


 policy_label_name:
 ColLabel
 { 
 $$ = $1;
}
;


 opt_add_resources_to_label:
 ADD_P resources_to_label_list
 { 
 $$ = cat_str(2,mm_strdup("add"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 resources_to_label_list:
 resources_to_label_list_item
 { 
 $$ = $1;
}
|  resources_to_label_list ',' resources_to_label_list_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 resources_to_label_list_item:
 policy_label_resource_type '(' policy_label_items ')'
 { 
 $$ = cat_str(4,$1,mm_strdup("("),$3,mm_strdup(")"));
}
|  policy_label_resource_type '(' policy_label_any_resource_item ')'
 { 
 $$ = cat_str(4,$1,mm_strdup("("),$3,mm_strdup(")"));
}
;


 policy_label_resource_type:
 TABLE
 { 
 $$ = mm_strdup("table");
}
|  COLUMN
 { 
 $$ = mm_strdup("column");
}
|  SCHEMA
 { 
 $$ = mm_strdup("schema");
}
|  VIEW
 { 
 $$ = mm_strdup("view");
}
|  FUNCTION
 { 
 $$ = mm_strdup("function");
}
;


 policy_label_any_resource_item:
 policy_label_any_resource
 { 
 $$ = $1;
}
;


 policy_label_any_resource:
 ALL
 { 
 $$ = mm_strdup("all");
}
;


 policy_label_items:
 policy_label_item
 { 
 $$ = $1;
}
|  policy_label_items ',' policy_label_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 filters_to_label_list:
 filters_to_label_list_item
 { 
 $$ = $1;
}
|  filters_to_label_list ',' filters_to_label_list_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 filters_to_label_list_item:
 policy_label_filter_type '(' policy_label_items ')'
 { 
 $$ = cat_str(4,$1,mm_strdup("("),$3,mm_strdup(")"));
}
;


 policy_label_filter_type:
 IP
 { 
 $$ = mm_strdup("ip");
}
|  APP
 { 
 $$ = mm_strdup("app");
}
|  ROLES
 { 
 $$ = mm_strdup("roles");
}
;


 policy_label_item:
 qualified_name
 { 
 $$ = $1;
}
;


 AlterPolicyLabelStmt:
 ALTER RESOURCE LABEL policy_label_name ADD_P resources_or_filters_to_label_list
 { 
 $$ = cat_str(4,mm_strdup("alter resource label"),$4,mm_strdup("add"),$6);
}
|  ALTER RESOURCE LABEL policy_label_name REMOVE resources_or_filters_to_label_list
 { 
 $$ = cat_str(4,mm_strdup("alter resource label"),$4,mm_strdup("remove"),$6);
}
;


 resources_or_filters_to_label_list:
 resources_to_label_list
 { 
 $$ = $1;
}
|  filters_to_label_list
 { 
 $$ = $1;
}
;


 DropPolicyLabelStmt:
 DROP RESOURCE LABEL policy_labels_list
 { 
 $$ = cat_str(2,mm_strdup("drop resource label"),$4);
}
|  DROP RESOURCE LABEL IF_P EXISTS policy_labels_list
 { 
 $$ = cat_str(2,mm_strdup("drop resource label if exists"),$6);
}
;


 policy_labels_list:
 policy_label_name
 { 
 $$ = $1;
}
|  policy_labels_list ',' policy_label_name
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 CreateResourcePoolStmt:
 CREATE RESOURCE POOL resource_pool_name OptWith
 { 
 $$ = cat_str(3,mm_strdup("create resource pool"),$4,$5);
}
;


 AlterResourcePoolStmt:
 ALTER RESOURCE POOL resource_pool_name OptWith
 { 
 $$ = cat_str(3,mm_strdup("alter resource pool"),$4,$5);
}
;


 AlterGlobalConfigStmt:
 ALTER GLOBAL CONFIGURATION OptWith
 { 
 $$ = cat_str(2,mm_strdup("alter global configuration"),$4);
}
;


 DropResourcePoolStmt:
 DROP RESOURCE POOL resource_pool_name
 { 
 $$ = cat_str(2,mm_strdup("drop resource pool"),$4);
}
|  DROP RESOURCE POOL IF_P EXISTS resource_pool_name
 { 
 $$ = cat_str(2,mm_strdup("drop resource pool if exists"),$6);
}
;


 resource_pool_name:
 ColId
 { 
 $$ = $1;
}
;


 DropGlobalConfigStmt:
 DROP GLOBAL CONFIGURATION name_list
 { 
 $$ = cat_str(2,mm_strdup("drop global configuration"),$4);
}
;


 CreateWorkloadGroupStmt:
 CREATE WORKLOAD GROUP_P workload_group_name USING RESOURCE POOL resource_pool_name OptWith
 { 
 $$ = cat_str(5,mm_strdup("create workload group"),$4,mm_strdup("using resource pool"),$8,$9);
}
|  CREATE WORKLOAD GROUP_P workload_group_name OptWith
 { 
 $$ = cat_str(3,mm_strdup("create workload group"),$4,$5);
}
;


 AlterWorkloadGroupStmt:
 ALTER WORKLOAD GROUP_P workload_group_name USING RESOURCE POOL resource_pool_name OptWith
 { 
 $$ = cat_str(5,mm_strdup("alter workload group"),$4,mm_strdup("using resource pool"),$8,$9);
}
|  ALTER WORKLOAD GROUP_P workload_group_name OptWith
 { 
 $$ = cat_str(3,mm_strdup("alter workload group"),$4,$5);
}
;


 DropWorkloadGroupStmt:
 DROP WORKLOAD GROUP_P workload_group_name
 { 
 $$ = cat_str(2,mm_strdup("drop workload group"),$4);
}
|  DROP WORKLOAD GROUP_P IF_P EXISTS workload_group_name
 { 
 $$ = cat_str(2,mm_strdup("drop workload group if exists"),$6);
}
;


 workload_group_name:
 ColId
 { 
 $$ = $1;
}
;


 CreateAppWorkloadGroupMappingStmt:
 CREATE APP WORKLOAD GROUP_P MAPPING application_name OptWith
 { 
 $$ = cat_str(3,mm_strdup("create app workload group mapping"),$6,$7);
}
;


 AlterAppWorkloadGroupMappingStmt:
 ALTER APP WORKLOAD GROUP_P MAPPING application_name OptWith
 { 
 $$ = cat_str(3,mm_strdup("alter app workload group mapping"),$6,$7);
}
;


 DropAppWorkloadGroupMappingStmt:
 DROP APP WORKLOAD GROUP_P MAPPING application_name
 { 
 $$ = cat_str(2,mm_strdup("drop app workload group mapping"),$6);
}
|  DROP APP WORKLOAD GROUP_P MAPPING IF_P EXISTS application_name
 { 
 $$ = cat_str(2,mm_strdup("drop app workload group mapping if exists"),$8);
}
;


 application_name:
 ColId
 { 
 $$ = $1;
}
;


 ExplainStmt:
 EXPLAIN ExplainableStmt
 { 
 $$ = cat_str(2,mm_strdup("explain"),$2);
}
|  EXPLAIN PERFORMANCE ExplainableStmt
 { 
 $$ = cat_str(2,mm_strdup("explain performance"),$3);
}
|  EXPLAIN analyze_keyword opt_verbose ExplainableStmt
 { 
 $$ = cat_str(4,mm_strdup("explain"),$2,$3,$4);
}
|  EXPLAIN VERBOSE ExplainableStmt
 { 
 $$ = cat_str(2,mm_strdup("explain verbose"),$3);
}
|  EXPLAIN '(' explain_option_list ')' ExplainableStmt
 { 
 $$ = cat_str(4,mm_strdup("explain ("),$3,mm_strdup(")"),$5);
}
|  EXPLAIN PLAN SET STATEMENT_ID '=' ecpg_sconst FOR ExplainableStmt
 { 
 $$ = cat_str(4,mm_strdup("explain plan set statement_id ="),$6,mm_strdup("for"),$8);
}
|  EXPLAIN PLAN FOR ExplainableStmt
 { 
 $$ = cat_str(2,mm_strdup("explain plan for"),$4);
}
;


 ExplainableStmt:
 SelectStmt
 { 
 $$ = $1;
}
|  InsertStmt
 { 
 $$ = $1;
}
|  UpdateStmt
 { 
 $$ = $1;
}
|  DeleteStmt
 { 
 $$ = $1;
}
|  MergeStmt
 { 
 $$ = $1;
}
|  DeclareCursorStmt
 { 
 $$ = $1;
}
|  CreateAsStmt
 { 
 $$ = $1;
}
|  CreateModelStmt
 { 
 $$ = $1;
}
|  SnapshotStmt
 { 
 $$ = $1;
}
|  ExecuteStmt
 { 
 $$ = $1;
}
;


 explain_option_list:
 explain_option_elem
 { 
 $$ = $1;
}
|  explain_option_list ',' explain_option_elem
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 explain_option_elem:
 explain_option_name explain_option_arg
 { 
 $$ = cat_str(2,$1,$2);
}
;


 explain_option_name:
 ColId
 { 
 $$ = $1;
}
|  analyze_keyword
 { 
 $$ = $1;
}
|  VERBOSE
 { 
 $$ = mm_strdup("verbose");
}
;


 explain_option_arg:
 opt_boolean_or_string
 { 
 $$ = $1;
}
|  NumericOnly
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 ExecDirectStmt:
 EXECUTE DIRECT ON pgxcnodes DirectStmt
 { 
 $$ = cat_str(3,mm_strdup("execute direct on"),$4,$5);
}
|  EXECUTE DIRECT ON COORDINATORS DirectStmt
 { 
 $$ = cat_str(2,mm_strdup("execute direct on coordinators"),$5);
}
|  EXECUTE DIRECT ON DATANODES DirectStmt
 { 
 $$ = cat_str(2,mm_strdup("execute direct on datanodes"),$5);
}
|  EXECUTE DIRECT ON ALL DirectStmt
 { 
 $$ = cat_str(2,mm_strdup("execute direct on all"),$5);
}
;


 DirectStmt:
 ecpg_sconst
 { 
 $$ = $1;
}
;


 CleanConnStmt:
 CLEAN CONNECTION TO COORDINATOR pgxcnodes CleanConnDbName CleanConnUserName
 { 
 $$ = cat_str(4,mm_strdup("clean connection to coordinator"),$5,$6,$7);
}
|  CLEAN CONNECTION TO NODE pgxcnodes CleanConnDbName CleanConnUserName
 { 
 $$ = cat_str(4,mm_strdup("clean connection to node"),$5,$6,$7);
}
|  CLEAN CONNECTION TO ALL opt_check opt_force CleanConnDbName CleanConnUserName
 { 
 $$ = cat_str(5,mm_strdup("clean connection to all"),$5,$6,$7,$8);
}
;


 CleanConnDbName:
 FOR DATABASE database_name
 { 
 $$ = cat_str(2,mm_strdup("for database"),$3);
}
|  FOR database_name
 { 
 $$ = cat_str(2,mm_strdup("for"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 CleanConnUserName:
 TO USER RoleId
 { 
 $$ = cat_str(2,mm_strdup("to user"),$3);
}
|  TO RoleId
 { 
 $$ = cat_str(2,mm_strdup("to"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 opt_check:
 CHECK
 { 
 $$ = mm_strdup("check");
}
| 
 { 
 $$=EMPTY; }
;


 PrepareStmt:
PREPARE prepared_name prep_type_clause AS PreparableStmt
	{
		$$.name = $2;
		$$.type = $3;
		$$.stmt = cat_str(3, mm_strdup("\""), $5, mm_strdup("\""));
	}
	| PREPARE prepared_name FROM execstring
	{
		$$.name = $2;
		$$.type = NULL;
		$$.stmt = $4;
	}
;


 prep_type_clause:
 '(' type_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 PreparableStmt:
 SelectStmt
 { 
 $$ = $1;
}
|  InsertStmt
 { 
 $$ = $1;
}
|  UpdateStmt
 { 
 $$ = $1;
}
|  DeleteStmt
 { 
 $$ = $1;
}
|  MergeStmt
 { 
 $$ = $1;
}
|  uservar_name
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = $1;
}
;


 ExecuteStmt:
EXECUTE prepared_name execute_param_clause execute_rest
	{ $$ = $2; }
|  CREATE OptTemp TABLE create_as_target AS EXECUTE name execute_param_clause opt_with_data
 { 
 $$ = cat_str(8,mm_strdup("create"),$2,mm_strdup("table"),$4,mm_strdup("as execute"),$7,$8,$9);
}
;


 execute_param_clause:
 '(' expr_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 insert_partition_clause:
 update_delete_partition_clause
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 update_delete_partition_clause:
 PARTITION '(' name ')'
 { 
 $$ = cat_str(3,mm_strdup("partition ("),$3,mm_strdup(")"));
}
|  SUBPARTITION '(' name ')'
 { 
 $$ = cat_str(3,mm_strdup("subpartition ("),$3,mm_strdup(")"));
}
|  PARTITION_FOR '(' expr_list ')'
 { 
 $$ = cat_str(3,mm_strdup("partition for ("),$3,mm_strdup(")"));
}
|  SUBPARTITION_FOR '(' expr_list ')'
 { 
 $$ = cat_str(3,mm_strdup("subpartition for ("),$3,mm_strdup(")"));
}
;


 InsertStmt:
 opt_with_clause INSERT hint_string INTO insert_target insert_rest returning_clause
 { 
 $$ = cat_str(7,$1,mm_strdup("insert"),$3,mm_strdup("into"),$5,$6,$7);
}
|  REPLACE hint_string INTO insert_target insert_rest returning_clause
 { 
 $$ = cat_str(6,mm_strdup("replace"),$2,mm_strdup("into"),$4,$5,$6);
}
|  REPLACE hint_string INTO insert_target SET set_clause_list
 { 
 $$ = cat_str(6,mm_strdup("replace"),$2,mm_strdup("into"),$4,mm_strdup("set"),$6);
}
|  opt_with_clause INSERT hint_string INTO insert_target insert_rest upsert_clause returning_clause
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(8,$1,mm_strdup("insert"),$3,mm_strdup("into"),$5,$6,$7,$8);
}
;


 insert_target:
 qualified_name insert_partition_clause
 { 
 $$ = cat_str(2,$1,$2);
}
|  qualified_name insert_partition_clause AS ColId
 { 
 $$ = cat_str(4,$1,$2,mm_strdup("as"),$4);
}
;


 insert_rest:
 SelectStmt
 { 
 $$ = $1;
}
|  '(' insert_column_list ')' SelectStmt
 { 
 $$ = cat_str(4,mm_strdup("("),$2,mm_strdup(")"),$4);
}
|  DEFAULT VALUES
 { 
 $$ = mm_strdup("default values");
}
;


 insert_column_list:
 insert_column_item
 { 
 $$ = $1;
}
|  insert_column_list ',' insert_column_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 insert_column_item:
 ColId opt_indirection
 { 
 $$ = cat_str(2,$1,$2);
}
;


 returning_clause:
RETURNING target_list ecpg_into
 { 
 $$ = cat_str(2,mm_strdup("returning"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 upsert_clause:
 ON DUPLICATE KEY UPDATE set_clause_list where_clause
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,mm_strdup("on duplicate key update"),$5,$6);
}
|  ON DUPLICATE KEY UPDATE NOTHING
 { 
 $$ = mm_strdup("on duplicate key update nothing");
}
;


 DeleteStmt:
 opt_with_clause DELETE_P hint_string FROM relation_expr_opt_alias_list using_clause where_or_current_clause opt_sort_clause opt_delete_limit returning_clause
 { 
 $$ = cat_str(10,$1,mm_strdup("delete"),$3,mm_strdup("from"),$5,$6,$7,$8,$9,$10);
}
|  opt_with_clause DELETE_P hint_string relation_expr_opt_alias_list using_clause where_or_current_clause opt_sort_clause opt_delete_limit returning_clause
 { 
 $$ = cat_str(9,$1,mm_strdup("delete"),$3,$4,$5,$6,$7,$8,$9);
}
|  opt_with_clause DELETE_P hint_string relation_expr_opt_alias_list FROM from_list where_or_current_clause
 { 
 $$ = cat_str(7,$1,mm_strdup("delete"),$3,$4,mm_strdup("from"),$6,$7);
}
;


 using_clause:
 USING from_list
 { 
 $$ = cat_str(2,mm_strdup("using"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 LockStmt:
 LOCK_P opt_table relation_expr_list opt_lock opt_nowait opt_cancelable
 { 
 $$ = cat_str(6,mm_strdup("lock"),$2,$3,$4,$5,$6);
}
;


 opt_lock:
 IN_P lock_type MODE
 { 
 $$ = cat_str(3,mm_strdup("in"),$2,mm_strdup("mode"));
}
| 
 { 
 $$=EMPTY; }
;


 lock_type:
 ACCESS SHARE
 { 
 $$ = mm_strdup("access share");
}
|  ROW SHARE
 { 
 $$ = mm_strdup("row share");
}
|  ROW EXCLUSIVE
 { 
 $$ = mm_strdup("row exclusive");
}
|  SHARE UPDATE EXCLUSIVE
 { 
 $$ = mm_strdup("share update exclusive");
}
|  SHARE
 { 
 $$ = mm_strdup("share");
}
|  SHARE ROW EXCLUSIVE
 { 
 $$ = mm_strdup("share row exclusive");
}
|  EXCLUSIVE
 { 
 $$ = mm_strdup("exclusive");
}
|  ACCESS EXCLUSIVE
 { 
 $$ = mm_strdup("access exclusive");
}
;


 opt_nowait:
 NOWAIT
 { 
 $$ = mm_strdup("nowait");
}
| 
 { 
 $$=EMPTY; }
;


 opt_cancelable:
 CANCELABLE
 { 
 $$ = mm_strdup("cancelable");
}
| 
 { 
 $$=EMPTY; }
;


 opt_wait:
 WAIT Iconst opt_nowait_or_skip:
 NOWAIT
 { 
 $$ = mm_strdup("nowait");
}
|  SKIP LOCKED
 { 
 $$ = mm_strdup("skip locked");
}
| 
 { 
 $$=EMPTY; }
;


 UpdateStmt:
 opt_with_clause UPDATE hint_string from_list SET set_clause_list from_clause where_or_current_clause opt_sort_clause opt_delete_limit returning_clause
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(11,$1,mm_strdup("update"),$3,$4,mm_strdup("set"),$6,$7,$8,$9,$10,$11);
}
;


 set_clause_list:
 set_clause
 { 
 $$ = $1;
}
|  set_clause_list ',' set_clause
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 set_clause:
 single_set_clause
 { 
 $$ = $1;
}
|  multiple_set_clause
 { 
 $$ = $1;
}
;


 single_set_clause:
 set_target '=' ctext_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("="),$3);
}
|  set_target '=' VALUES '(' columnref ')'
 { 
 $$ = cat_str(4,$1,mm_strdup("= values ("),$5,mm_strdup(")"));
}
;


 multiple_set_clause:
 '(' set_target_list ')' '=' ctext_row
 { 
 $$ = cat_str(4,mm_strdup("("),$2,mm_strdup(") ="),$5);
}
|  '(' set_target_list ')' '=' '(' SELECT hint_string opt_distinct target_list from_clause where_clause group_clause having_clause ')'
 { 
 $$ = cat_str(11,mm_strdup("("),$2,mm_strdup(") = ( select"),$7,$8,$9,$10,$11,$12,$13,mm_strdup(")"));
}
;


 set_target:
 ColId opt_indirection
 { 
 $$ = cat_str(2,$1,$2);
}
;


 set_target_list:
 set_target
 { 
 $$ = $1;
}
|  set_target_list ',' set_target
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 MergeStmt:
 MERGE hint_string INTO relation_expr_opt_alias USING table_ref ON a_expr merge_when_list
 { 
 $$ = cat_str(9,mm_strdup("merge"),$2,mm_strdup("into"),$4,mm_strdup("using"),$6,mm_strdup("on"),$8,$9);
}
;


 merge_when_list:
 merge_when_clause
 { 
 $$ = $1;
}
|  merge_when_list merge_when_clause
 { 
 $$ = cat_str(2,$1,$2);
}
;


 merge_when_clause:
 WHEN MATCHED THEN merge_update opt_merge_where_condition
 { 
 $$ = cat_str(3,mm_strdup("when matched then"),$4,$5);
}
|  WHEN NOT MATCHED THEN merge_insert opt_merge_where_condition
 { 
 $$ = cat_str(3,mm_strdup("when not matched then"),$5,$6);
}
;


 opt_merge_where_condition:
 WHERE a_expr
 { 
 $$ = cat_str(2,mm_strdup("where"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 merge_update:
 UPDATE SET set_clause_list
 { 
 $$ = cat_str(2,mm_strdup("update set"),$3);
}
;


 merge_insert:
 INSERT merge_values_clause
 { 
 $$ = cat_str(2,mm_strdup("insert"),$2);
}
|  INSERT '(' insert_column_list ')' merge_values_clause
 { 
 $$ = cat_str(4,mm_strdup("insert ("),$3,mm_strdup(")"),$5);
}
|  INSERT DEFAULT VALUES
 { 
 $$ = mm_strdup("insert default values");
}
;


 merge_values_clause:
 VALUES ctext_row
 { 
 $$ = cat_str(2,mm_strdup("values"),$2);
}
;


 DeclareCursorStmt:
 CURSOR cursor_name cursor_options opt_hold FOR SelectStmt
	{
		struct cursor *ptr, *thisPtr;
		char *cursor_marker = $2[0] == ':' ? mm_strdup("$0") : mm_strdup($2);
		char *comment, *c1, *c2;
		int (* strcmp_fn)(const char *, const char *) = ($2[0] == ':' ? strcmp : pg_strcasecmp);

		for (ptr = cur; ptr != NULL; ptr = ptr->next)
		{
			if (strcmp_fn($2, ptr->name) == 0)
			{
				if ($2[0] == ':')
					mmerror(PARSE_ERROR, ET_ERROR, "using variable \"%s\" in different declare statements is not supported", $2+1);
				else
					mmerror(PARSE_ERROR, ET_ERROR, "cursor \"%s\" is already defined", $2);
			}
		}

		thisPtr = (struct cursor *) mm_alloc(sizeof(struct cursor));

		thisPtr->next = cur;
		thisPtr->name = $2;
		thisPtr->function = (current_function ? mm_strdup(current_function) : NULL);
		thisPtr->connection = connection;
		thisPtr->opened = false;
		thisPtr->command =  cat_str(6, mm_strdup("CURSOR"), cursor_marker, $3, $4, mm_strdup("for"), $6);
		thisPtr->argsinsert = argsinsert;
		thisPtr->argsinsert_oos = NULL;
		thisPtr->argsresult = argsresult;
		thisPtr->argsresult_oos = NULL;
		argsinsert = argsresult = NULL;
		cur = thisPtr;

		c1 = mm_strdup(thisPtr->command);
		if ((c2 = strstr(c1, "*/")) != NULL)
		{
			/* We put this text into a comment, so we better remove [*][/]. */
			c2[0] = '.';
			c2[1] = '.';
		}
		comment = cat_str(3, mm_strdup("/*"), c1, mm_strdup("*/"));

		if ((braces_open > 0) && INFORMIX_MODE) /* we're in a function */
			$$ = cat_str(3, adjust_outofscope_cursor_vars(thisPtr),
				mm_strdup("ECPG_informix_reset_sqlca();"),
				comment);
		else
			$$ = cat2_str(adjust_outofscope_cursor_vars(thisPtr), comment);
	}
	| DECLARE cursor_name cursor_options CURSOR opt_hold FOR SelectStmt
	{
		struct cursor *ptr, *thisPtr;
		char *cursor_marker = $2[0] == ':' ? mm_strdup("$0") : mm_strdup($2);
		char *comment, *c1, *c2;
		int (* strcmp_fn)(const char *, const char *) = (($2[0] == ':' || $2[0] == '"') ? strcmp : pg_strcasecmp);

		for (ptr = cur; ptr != NULL; ptr = ptr->next)
		{
			if (strcmp_fn($2, ptr->name) == 0)
			{
				if ($2[0] == ':')
					mmerror(PARSE_ERROR, ET_ERROR, "using variable \"%s\" in different declare statements is not supported", $2+1);
				else
					mmerror(PARSE_ERROR, ET_ERROR, "cursor \"%s\" is already defined", $2);
			}
		}

		thisPtr = (struct cursor *) mm_alloc(sizeof(struct cursor));

		thisPtr->next = cur;
		thisPtr->name = $2;
		thisPtr->function = (current_function ? mm_strdup(current_function) : NULL);
		thisPtr->connection = connection;
		thisPtr->opened = false;
		thisPtr->command =  cat_str(7, mm_strdup("declare"), cursor_marker, $3, mm_strdup("cursor"), $5, mm_strdup("for"), $7);
		thisPtr->argsinsert = argsinsert;
		thisPtr->argsinsert_oos = NULL;
		thisPtr->argsresult = argsresult;
		thisPtr->argsresult_oos = NULL;
		argsinsert = argsresult = NULL;
		cur = thisPtr;

		c1 = mm_strdup(thisPtr->command);
		if ((c2 = strstr(c1, "*/")) != NULL)
		{
			/* We put this text into a comment, so we better remove [*][/]. */
			c2[0] = '.';
			c2[1] = '.';
		}
		comment = cat_str(3, mm_strdup("/*"), c1, mm_strdup("*/"));

		if ((braces_open > 0) && INFORMIX_MODE) /* we're in a function */
			$$ = cat_str(3, adjust_outofscope_cursor_vars(thisPtr),
				mm_strdup("ECPG_informix_reset_sqlca();"),
				comment);
		else
			$$ = cat2_str(adjust_outofscope_cursor_vars(thisPtr), comment);
	}
;


 cursor_name:
 name
 { 
 $$ = $1;
}
	| char_civar
		{
			char *curname = (char *)mm_alloc(strlen($1) + 2);
			sprintf(curname, ":%s", $1);
			free_current_memory($1);
			$1 = curname;
			$$ = $1;
		}
;


 cursor_options:

 { 
 $$=EMPTY; }
|  cursor_options NO SCROLL
 { 
 $$ = cat_str(2,$1,mm_strdup("no scroll"));
}
|  cursor_options SCROLL
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,$1,mm_strdup("scroll"));
}
|  cursor_options BINARY
 { 
 $$ = cat_str(2,$1,mm_strdup("binary"));
}
|  cursor_options INSENSITIVE
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,$1,mm_strdup("insensitive"));
}
;


 opt_hold:

	{
		if (compat == ECPG_COMPAT_INFORMIX_SE && autocommit)
			$$ = mm_strdup("with hold");
		else
			$$ = EMPTY;
	}
|  WITH HOLD
 { 
 $$ = mm_strdup("with hold");
}
|  WITHOUT HOLD
 { 
 $$ = mm_strdup("without hold");
}
;


 SelectStmt:
 select_no_parens %prec UMINUS
 { 
 $$ = $1;
}
|  select_with_parens %prec UMINUS
 { 
 $$ = $1;
}
;


 select_with_parens:
 '(' select_no_parens ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
|  '(' select_with_parens ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
;


 select_no_parens:
 simple_select
 { 
 $$ = $1;
}
|  select_clause siblings_clause opt_select_limit
 { 
 $$ = cat_str(3,$1,$2,$3);
}
|  select_clause siblings_clause sort_clause opt_select_limit
 { 
 $$ = cat_str(4,$1,$2,$3,$4);
}
|  select_clause sort_clause
 { 
 $$ = cat_str(2,$1,$2);
}
|  select_clause opt_sort_clause for_locking_clause opt_select_limit opt_into_clause
 { 
 $$ = cat_str(5,$1,$2,$3,$4,$5);
}
|  select_clause opt_sort_clause select_limit opt_for_locking_clause
 { 
 $$ = cat_str(4,$1,$2,$3,$4);
}
|  select_clause opt_sort_clause select_limit for_locking_clause into_clause
 { 
 $$ = cat_str(5,$1,$2,$3,$4,$5);
}
|  select_clause opt_sort_clause opt_select_limit into_clause opt_for_locking_clause
 { 
 $$ = cat_str(5,$1,$2,$3,$4,$5);
}
|  with_clause select_clause
 { 
 $$ = cat_str(2,$1,$2);
}
|  with_clause select_clause sort_clause
 { 
 $$ = cat_str(3,$1,$2,$3);
}
|  with_clause select_clause siblings_clause
 { 
 $$ = cat_str(3,$1,$2,$3);
}
|  with_clause select_clause siblings_clause sort_clause
 { 
 $$ = cat_str(4,$1,$2,$3,$4);
}
|  with_clause select_clause opt_sort_clause for_locking_clause opt_select_limit opt_into_clause
 { 
 $$ = cat_str(6,$1,$2,$3,$4,$5,$6);
}
|  with_clause select_clause opt_sort_clause select_limit for_locking_clause into_clause
 { 
 $$ = cat_str(6,$1,$2,$3,$4,$5,$6);
}
|  with_clause select_clause opt_sort_clause opt_select_limit into_clause opt_for_locking_clause
 { 
 $$ = cat_str(6,$1,$2,$3,$4,$5,$6);
}
|  with_clause select_clause opt_sort_clause select_limit opt_for_locking_clause
 { 
 $$ = cat_str(5,$1,$2,$3,$4,$5);
}
;


 select_clause:
 simple_select
 { 
 $$ = $1;
}
|  select_with_parens
 { 
 $$ = $1;
}
;


 simple_select:
 SELECT hint_string opt_distinct target_list opt_into_clause from_clause where_clause start_with_clause group_clause having_clause window_clause
 { 
 $$ = cat_str(11,mm_strdup("select"),$2,$3,$4,$5,$6,$7,$8,$9,$10,$11);
}
|  values_clause
 { 
 $$ = $1;
}
|  TABLE relation_expr
 { 
 $$ = cat_str(2,mm_strdup("table"),$2);
}
|  select_clause UNION opt_all select_clause
 { 
 $$ = cat_str(4,$1,mm_strdup("union"),$3,$4);
}
|  select_clause INTERSECT opt_all select_clause
 { 
 $$ = cat_str(4,$1,mm_strdup("intersect"),$3,$4);
}
|  select_clause EXCEPT opt_all select_clause
 { 
 $$ = cat_str(4,$1,mm_strdup("except"),$3,$4);
}
|  select_clause MINUS_P opt_all select_clause
 { 
 $$ = cat_str(4,$1,mm_strdup("minus"),$3,$4);
}
;


 hint_string:
 COMMENTSTRING
 { 
 $$ = mm_strdup("commentstring");
}
| 
 { 
 $$=EMPTY; }
;


 with_clause:
 WITH cte_list
 { 
 $$ = cat_str(2,mm_strdup("with"),$2);
}
|  WITH RECURSIVE cte_list
 { 
 $$ = cat_str(2,mm_strdup("with recursive"),$3);
}
;


 cte_list:
 common_table_expr
 { 
 $$ = $1;
}
|  cte_list ',' common_table_expr
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 common_table_expr:
 name opt_name_list AS opt_materialized '(' PreparableStmt ')'
 { 
 $$ = cat_str(7,$1,$2,mm_strdup("as"),$4,mm_strdup("("),$6,mm_strdup(")"));
}
;


 opt_materialized:
 MATERIALIZED
 { 
 $$ = mm_strdup("materialized");
}
|  NOT MATERIALIZED
 { 
 $$ = mm_strdup("not materialized");
}
| 
 { 
 $$=EMPTY; }
;


 opt_with_clause:
 with_clause
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 opt_into_clause:
 into_clause
 { 
 $$ = $1;
}
|  %prec INTO
 { 
 $$=EMPTY; }
;


 into_clause:
 INTO OptTempTableName
	{
		FoundInto = 1;
		$$= cat2_str(mm_strdup("into"), $2);
	}
	| ecpg_into { $$ = EMPTY; }
|  INTO into_user_var_list
 { 
 $$ = cat_str(2,mm_strdup("into"),$2);
}
|  INTO OUTFILE ecpg_sconst characterset_option fields_options_fin lines_options_fin
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(5,mm_strdup("into outfile"),$3,$4,$5,$6);
}
|  INTO DUMPFILE ecpg_sconst
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("into dumpfile"),$3);
}
;


 characterset_option:
 CHARACTER SET ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("character set"),$3);
}
| 
 { 
 $$=EMPTY; }
;


 fields_options_fin:
 FIELDS fields_options_list
 { 
 $$ = cat_str(2,mm_strdup("fields"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 fields_options_list:
 fields_options_list fields_options_item
 { 
 $$ = cat_str(2,$1,$2);
}
| 
 { 
 $$=EMPTY; }
;


 fields_options_item:
 TERMINATED BY ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("terminated by"),$3);
}
|  OPTIONALLY ENCLOSED BY ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("optionally enclosed by"),$4);
}
|  ENCLOSED BY ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("enclosed by"),$3);
}
|  ESCAPED BY ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("escaped by"),$3);
}
;


 lines_options_fin:
 LINES lines_options_list
 { 
 $$ = cat_str(2,mm_strdup("lines"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 lines_options_list:
 lines_options_list lines_option_item
 { 
 $$ = cat_str(2,$1,$2);
}
| 
 { 
 $$=EMPTY; }
;


 lines_option_item:
 STARTING BY ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("starting by"),$3);
}
|  TERMINATED BY ecpg_sconst
 { 
 $$ = cat_str(2,mm_strdup("terminated by"),$3);
}
;


 into_user_var_list:
 uservar_name
 { 
 $$ = $1;
}
|  into_user_var_list ',' uservar_name
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 OptTempTableName:
 TEMPORARY opt_table qualified_name
 { 
 $$ = cat_str(3,mm_strdup("temporary"),$2,$3);
}
|  TEMP opt_table qualified_name
 { 
 $$ = cat_str(3,mm_strdup("temp"),$2,$3);
}
|  LOCAL TEMPORARY opt_table qualified_name
 { 
 $$ = cat_str(3,mm_strdup("local temporary"),$3,$4);
}
|  LOCAL TEMP opt_table qualified_name
 { 
 $$ = cat_str(3,mm_strdup("local temp"),$3,$4);
}
|  GLOBAL TEMPORARY opt_table qualified_name
 { 
 $$ = cat_str(3,mm_strdup("global temporary"),$3,$4);
}
|  GLOBAL TEMP opt_table qualified_name
 { 
 $$ = cat_str(3,mm_strdup("global temp"),$3,$4);
}
|  UNLOGGED opt_table qualified_name
 { 
 $$ = cat_str(3,mm_strdup("unlogged"),$2,$3);
}
|  TABLE qualified_name
 { 
 $$ = cat_str(2,mm_strdup("table"),$2);
}
|  qualified_name
 { 
 $$ = $1;
}
;


 opt_table:
 TABLE
 { 
 $$ = mm_strdup("table");
}
| 
 { 
 $$=EMPTY; }
;


 opt_all:
 ALL
 { 
 $$ = mm_strdup("all");
}
|  DISTINCT
 { 
 $$ = mm_strdup("distinct");
}
| 
 { 
 $$=EMPTY; }
;


 opt_distinct:
 DISTINCT
 { 
 $$ = mm_strdup("distinct");
}
|  DISTINCT ON '(' expr_list ')'
 { 
 $$ = cat_str(3,mm_strdup("distinct on ("),$4,mm_strdup(")"));
}
|  ALL
 { 
 $$ = mm_strdup("all");
}
| 
 { 
 $$=EMPTY; }
;


 opt_sort_clause:
 sort_clause
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 sort_clause:
 ORDER BY sortby_list
 { 
 $$ = cat_str(2,mm_strdup("order by"),$3);
}
;


 siblings_clause:
 ORDER SIBLINGS BY sortby_list
 { 
 $$ = cat_str(2,mm_strdup("order siblings by"),$4);
}
;


 sortby_list:
 sortby
 { 
 $$ = $1;
}
|  sortby_list ',' sortby
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 sortby:
 a_expr USING qual_all_Op opt_nulls_order
 { 
 $$ = cat_str(4,$1,mm_strdup("using"),$3,$4);
}
|  a_expr opt_asc_desc opt_nulls_order
 { 
 $$ = cat_str(3,$1,$2,$3);
}
;


 select_limit:
 limit_clause offset_clause
 { 
 $$ = cat_str(2,$1,$2);
}
|  offset_clause limit_clause
 { 
 $$ = cat_str(2,$1,$2);
}
|  limit_clause
 { 
 $$ = $1;
}
|  limit_offcnt_clause
 { 
 $$ = $1;
}
|  offset_clause
 { 
 $$ = $1;
}
;


 opt_select_limit:
 select_limit
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 opt_delete_limit:
 LIMIT a_expr
 { 
 $$ = cat_str(2,mm_strdup("limit"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 limit_clause:
 LIMIT select_limit_value
 { 
 $$ = cat_str(2,mm_strdup("limit"),$2);
}
|  FETCH first_or_next opt_select_fetch_first_value row_or_rows ONLY
 { 
 $$ = cat_str(5,mm_strdup("fetch"),$2,$3,$4,mm_strdup("only"));
}
;


 limit_offcnt_clause:
 LIMIT select_offset_value ',' select_limit_value
 { 
 $$ = cat_str(4,mm_strdup("limit"),$2,mm_strdup(","),$4);
}
;


 offset_clause:
 OFFSET select_offset_value
 { 
 $$ = cat_str(2,mm_strdup("offset"),$2);
}
|  OFFSET select_offset_value2 row_or_rows
 { 
 $$ = cat_str(3,mm_strdup("offset"),$2,$3);
}
;


 select_limit_value:
 a_expr
 { 
 $$ = $1;
}
|  ALL
 { 
 $$ = mm_strdup("all");
}
;


 select_offset_value:
 a_expr
 { 
 $$ = $1;
}
;


 opt_select_fetch_first_value:
 SignedIconst
 { 
 $$ = $1;
}
|  '(' a_expr ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 select_offset_value2:
 c_expr
 { 
 $$ = $1;
}
;


 row_or_rows:
 ROW
 { 
 $$ = mm_strdup("row");
}
|  ROWS
 { 
 $$ = mm_strdup("rows");
}
;


 first_or_next:
 FIRST_P
 { 
 $$ = mm_strdup("first");
}
|  NEXT
 { 
 $$ = mm_strdup("next");
}
;


 group_clause:
 GROUP_P BY group_by_list
 { 
 $$ = cat_str(2,mm_strdup("group by"),$3);
}
| 
 { 
 $$=EMPTY; }
;


 group_by_list:
 group_by_item
 { 
 $$ = $1;
}
|  group_by_list ',' group_by_item
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 group_by_item:
 a_expr
 { 
 $$ = $1;
}
|  empty_grouping_set
 { 
 $$ = $1;
}
|  cube_clause
 { 
 $$ = $1;
}
|  rollup_clause
 { 
 $$ = $1;
}
|  grouping_sets_clause
 { 
 $$ = $1;
}
;


 empty_grouping_set:
 '(' ')'
 { 
 $$ = mm_strdup("( )");
}
;


 rollup_clause:
 ROLLUP '(' expr_list ')'
 { 
 $$ = cat_str(3,mm_strdup("rollup ("),$3,mm_strdup(")"));
}
;


 cube_clause:
 CUBE '(' expr_list ')'
 { 
 $$ = cat_str(3,mm_strdup("cube ("),$3,mm_strdup(")"));
}
;


 grouping_sets_clause:
 GROUPING_P SETS '(' group_by_list ')'
 { 
 $$ = cat_str(3,mm_strdup("grouping sets ("),$4,mm_strdup(")"));
}
;


 having_clause:
 HAVING a_expr
 { 
 $$ = cat_str(2,mm_strdup("having"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 start_with_clause:
 START_WITH start_with_expr connect_by_expr
 { 
 $$ = cat_str(3,mm_strdup("start_with"),$2,$3);
}
|  START_WITH start_with_expr CONNECT_BY NOCYCLE a_expr
 { 
 $$ = cat_str(4,mm_strdup("start_with"),$2,mm_strdup("connect_by nocycle"),$5);
}
|  connect_by_expr START_WITH start_with_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("start_with"),$3);
}
|  CONNECT_BY NOCYCLE a_expr START_WITH start_with_expr
 { 
 $$ = cat_str(4,mm_strdup("connect_by nocycle"),$3,mm_strdup("start_with"),$5);
}
|  CONNECT_BY NOCYCLE a_expr
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("connect_by nocycle"),$3);
}
|  connect_by_expr
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 start_with_expr:
 a_expr
 { 
 $$ = $1;
}
;


 connect_by_expr:
 CONNECT_BY a_expr
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("connect_by"),$2);
}
;


 for_locking_clause:
 for_locking_items
 { 
 $$ = $1;
}
|  FOR READ ONLY
 { 
 $$ = mm_strdup("for read only");
}
;


 opt_for_locking_clause:
 for_locking_clause
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 for_locking_items:
 for_locking_item
 { 
 $$ = $1;
}
|  for_locking_items for_locking_item
 { 
 $$ = cat_str(2,$1,$2);
}
;


 for_locking_item:
 FOR UPDATE hint_string locked_rels_list opt_nowait_or_skip
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(4,mm_strdup("for update"),$3,$4,$5);
}
|  FOR UPDATE hint_string locked_rels_list opt_wait
 { 
 $$ = cat_str(4,mm_strdup("for update"),$3,$4,$5);
}
|  for_locking_strength locked_rels_list opt_nowait_or_skip
 { 
 $$ = cat_str(3,$1,$2,$3);
}
;


 for_locking_strength:
 FOR NO KEY UPDATE
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = mm_strdup("for no key update");
}
|  FOR SHARE
 { 
 $$ = mm_strdup("for share");
}
|  FOR KEY SHARE
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = mm_strdup("for key share");
}
;


 locked_rels_list:
 OF qualified_name_list
 { 
 $$ = cat_str(2,mm_strdup("of"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 values_clause:
 VALUES ctext_row
 { 
 $$ = cat_str(2,mm_strdup("values"),$2);
}
|  values_clause ',' ctext_row
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 from_clause:
 FROM from_list
 { 
 $$ = cat_str(2,mm_strdup("from"),$2);
}
|  %prec EMPTY_FROM_CLAUSE
 { 
 $$=EMPTY; }
;


 from_list:
 table_ref
 { 
 $$ = $1;
}
|  from_list ',' table_ref
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 table_ref:
 relation_expr %prec UMINUS
 { 
 $$ = $1;
}
|  relation_expr alias_clause opt_index_hint_list
 { 
 $$ = cat_str(3,$1,$2,$3);
}
|  relation_expr index_hint_list
 { 
 $$ = cat_str(2,$1,$2);
}
|  relation_expr opt_alias_clause tablesample_clause
 { 
 $$ = cat_str(3,$1,$2,$3);
}
|  relation_expr opt_alias_clause timecapsule_clause
 { 
 $$ = cat_str(3,$1,$2,$3);
}
|  relation_expr PARTITION '(' name ')'
 { 
 $$ = cat_str(4,$1,mm_strdup("partition ("),$4,mm_strdup(")"));
}
|  relation_expr SUBPARTITION '(' name ')'
 { 
 $$ = cat_str(4,$1,mm_strdup("subpartition ("),$4,mm_strdup(")"));
}
|  relation_expr BUCKETS '(' bucket_list ')'
 { 
 $$ = cat_str(4,$1,mm_strdup("buckets ("),$4,mm_strdup(")"));
}
|  relation_expr PARTITION_FOR '(' expr_list ')'
 { 
 $$ = cat_str(4,$1,mm_strdup("partition for ("),$4,mm_strdup(")"));
}
|  relation_expr SUBPARTITION_FOR '(' expr_list ')'
 { 
 $$ = cat_str(4,$1,mm_strdup("subpartition for ("),$4,mm_strdup(")"));
}
|  relation_expr PARTITION '(' name ')' index_hint_list
 { 
 $$ = cat_str(5,$1,mm_strdup("partition ("),$4,mm_strdup(")"),$6);
}
|  relation_expr SUBPARTITION '(' name ')' index_hint_list
 { 
 $$ = cat_str(5,$1,mm_strdup("subpartition ("),$4,mm_strdup(")"),$6);
}
|  relation_expr BUCKETS '(' bucket_list ')' index_hint_list
 { 
 $$ = cat_str(5,$1,mm_strdup("buckets ("),$4,mm_strdup(")"),$6);
}
|  relation_expr PARTITION_FOR '(' expr_list ')' index_hint_list
 { 
 $$ = cat_str(5,$1,mm_strdup("partition for ("),$4,mm_strdup(")"),$6);
}
|  relation_expr SUBPARTITION_FOR '(' expr_list ')' index_hint_list
 { 
 $$ = cat_str(5,$1,mm_strdup("subpartition for ("),$4,mm_strdup(")"),$6);
}
|  relation_expr PARTITION '(' name ')' alias_clause opt_index_hint_list
 { 
 $$ = cat_str(6,$1,mm_strdup("partition ("),$4,mm_strdup(")"),$6,$7);
}
|  relation_expr SUBPARTITION '(' name ')' alias_clause opt_index_hint_list
 { 
 $$ = cat_str(6,$1,mm_strdup("subpartition ("),$4,mm_strdup(")"),$6,$7);
}
|  relation_expr PARTITION_FOR '(' expr_list ')' alias_clause opt_index_hint_list
 { 
 $$ = cat_str(6,$1,mm_strdup("partition for ("),$4,mm_strdup(")"),$6,$7);
}
|  relation_expr SUBPARTITION_FOR '(' expr_list ')' alias_clause opt_index_hint_list
 { 
 $$ = cat_str(6,$1,mm_strdup("subpartition for ("),$4,mm_strdup(")"),$6,$7);
}
|  func_table %prec UMINUS
 { 
 $$ = $1;
}
|  func_table alias_clause
 { 
 $$ = cat_str(2,$1,$2);
}
|  func_table AS '(' TableFuncElementList ')'
 { 
 $$ = cat_str(4,$1,mm_strdup("as ("),$4,mm_strdup(")"));
}
|  func_table AS ColId '(' TableFuncElementList ')'
 { 
 $$ = cat_str(6,$1,mm_strdup("as"),$3,mm_strdup("("),$5,mm_strdup(")"));
}
|  func_table ColId '(' TableFuncElementList ')'
 { 
 $$ = cat_str(5,$1,$2,mm_strdup("("),$4,mm_strdup(")"));
}
|  select_with_parens %prec UMINUS
 { 
 $$ = $1;
}
|  select_with_parens alias_clause
 { 
 $$ = cat_str(2,$1,$2);
}
|  joined_table
 { 
 $$ = $1;
}
|  '(' joined_table ')' alias_clause
 { 
 $$ = cat_str(4,mm_strdup("("),$2,mm_strdup(")"),$4);
}
;


 joined_table:
 '(' joined_table ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
|  table_ref CROSS JOIN table_ref
 { 
 $$ = cat_str(3,$1,mm_strdup("cross join"),$4);
}
|  table_ref join_type JOIN table_ref join_qual
 { 
 $$ = cat_str(5,$1,$2,mm_strdup("join"),$4,$5);
}
|  table_ref JOIN table_ref join_qual
 { 
 $$ = cat_str(4,$1,mm_strdup("join"),$3,$4);
}
|  table_ref NATURAL join_type JOIN table_ref
 { 
 $$ = cat_str(5,$1,mm_strdup("natural"),$3,mm_strdup("join"),$5);
}
|  table_ref NATURAL JOIN table_ref
 { 
 $$ = cat_str(3,$1,mm_strdup("natural join"),$4);
}
;


 alias_clause:
 AS ColId '(' name_list ')'
 { 
 $$ = cat_str(5,mm_strdup("as"),$2,mm_strdup("("),$4,mm_strdup(")"));
}
|  AS ColId
 { 
 $$ = cat_str(2,mm_strdup("as"),$2);
}
|  ColId '(' name_list ')'
 { 
 $$ = cat_str(4,$1,mm_strdup("("),$3,mm_strdup(")"));
}
|  ColId
 { 
 $$ = $1;
}
;


 opt_alias_clause:
 alias_clause
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 join_type:
 FULL join_outer
 { 
 $$ = cat_str(2,mm_strdup("full"),$2);
}
|  LEFT join_outer
 { 
 $$ = cat_str(2,mm_strdup("left"),$2);
}
|  RIGHT join_outer
 { 
 $$ = cat_str(2,mm_strdup("right"),$2);
}
|  INNER_P
 { 
 $$ = mm_strdup("inner");
}
;


 join_outer:
 OUTER_P
 { 
 $$ = mm_strdup("outer");
}
| 
 { 
 $$=EMPTY; }
;


 join_qual:
 USING '(' name_list ')'
 { 
 $$ = cat_str(3,mm_strdup("using ("),$3,mm_strdup(")"));
}
|  ON a_expr
 { 
 $$ = cat_str(2,mm_strdup("on"),$2);
}
;


 relation_expr:
 qualified_name OptSnapshotVersion
 { 
 $$ = cat_str(2,$1,$2);
}
|  qualified_name '*'
 { 
 $$ = cat_str(2,$1,mm_strdup("*"));
}
|  ONLY qualified_name
 { 
 $$ = cat_str(2,mm_strdup("only"),$2);
}
|  ONLY '(' qualified_name ')'
 { 
 $$ = cat_str(3,mm_strdup("only ("),$3,mm_strdup(")"));
}
;


 relation_expr_list:
 relation_expr
 { 
 $$ = $1;
}
|  relation_expr_list ',' relation_expr
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 delete_relation_expr_opt_alias:
 relation_expr_opt_alias %prec UMINUS
 { 
 $$ = $1;
}
|  relation_expr PARTITION '(' name ',' name_list ')'
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(6,$1,mm_strdup("partition ("),$4,mm_strdup(","),$6,mm_strdup(")"));
}
|  relation_expr ColId PARTITION '(' name_list ')'
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(5,$1,$2,mm_strdup("partition ("),$5,mm_strdup(")"));
}
|  relation_expr AS ColId PARTITION '(' name_list ')'
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(6,$1,mm_strdup("as"),$3,mm_strdup("partition ("),$6,mm_strdup(")"));
}
;


 relation_expr_opt_alias:
 relation_expr %prec UMINUS
 { 
 $$ = $1;
}
|  relation_expr ColId
 { 
 $$ = cat_str(2,$1,$2);
}
|  relation_expr AS ColId
 { 
 $$ = cat_str(3,$1,mm_strdup("as"),$3);
}
|  relation_expr update_delete_partition_clause %prec UMINUS
 { 
 $$ = cat_str(2,$1,$2);
}
|  relation_expr update_delete_partition_clause ColId
 { 
 $$ = cat_str(3,$1,$2,$3);
}
|  relation_expr update_delete_partition_clause AS ColId
 { 
 $$ = cat_str(4,$1,$2,mm_strdup("as"),$4);
}
;


 relation_expr_opt_alias_list:
 delete_relation_expr_opt_alias
 { 
 $$ = $1;
}
|  relation_expr_opt_alias_list ',' delete_relation_expr_opt_alias
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 tablesample_clause:
 TABLESAMPLE func_name '(' expr_list ')' opt_repeatable_clause
 { 
 $$ = cat_str(6,mm_strdup("tablesample"),$2,mm_strdup("("),$4,mm_strdup(")"),$6);
}
;


 timecapsule_clause:
 TIMECAPSULE opt_timecapsule_clause
 { 
 $$ = cat_str(2,mm_strdup("timecapsule"),$2);
}
;


 opt_timecapsule_clause:
 CSN a_expr
 { 
 $$ = cat_str(2,mm_strdup("csn"),$2);
}
|  TIMESTAMP a_expr
 { 
 $$ = cat_str(2,mm_strdup("timestamp"),$2);
}
;


 opt_repeatable_clause:
 REPEATABLE '(' a_expr ')'
 { 
 $$ = cat_str(3,mm_strdup("repeatable ("),$3,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 func_table:
 func_expr_windowless
 { 
 $$ = $1;
}
;


 where_clause:
 WHERE a_expr
 { 
 $$ = cat_str(2,mm_strdup("where"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 where_or_current_clause:
 WHERE a_expr
 { 
 $$ = cat_str(2,mm_strdup("where"),$2);
}
|  WHERE CURRENT_P OF cursor_name
	{
		char *cursor_marker = $4[0] == ':' ? mm_strdup("$0") : $4;
		$$ = cat_str(2,mm_strdup("where current of"), cursor_marker);
	}
| 
 { 
 $$=EMPTY; }
;


 OptTableFuncElementList:
 TableFuncElementList
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 TableFuncElementList:
 TableFuncElement
 { 
 $$ = $1;
}
|  TableFuncElementList ',' TableFuncElement
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 TableFuncElement:
 ColId func_type opt_collate_clause
 { 
 $$ = cat_str(3,$1,$2,$3);
}
;


 Typename:
 SimpleTypename opt_array_bounds
	{	$$ = cat2_str($1, $2.str); }
|  SETOF SimpleTypename opt_array_bounds
	{	$$ = cat_str(3, mm_strdup("setof"), $2, $3.str); }
|  SimpleTypename ARRAY '[' Iconst ']'
 { 
 $$ = cat_str(4,$1,mm_strdup("array ["),$4,mm_strdup("]"));
}
|  SETOF SimpleTypename ARRAY '[' Iconst ']'
 { 
 $$ = cat_str(5,mm_strdup("setof"),$2,mm_strdup("array ["),$5,mm_strdup("]"));
}
|  SimpleTypename ARRAY
 { 
 $$ = cat_str(2,$1,mm_strdup("array"));
}
|  SETOF SimpleTypename ARRAY
 { 
 $$ = cat_str(3,mm_strdup("setof"),$2,mm_strdup("array"));
}
;


 opt_array_bounds:
 opt_array_bounds '[' ']'
	{
		$$.index1 = $1.index1;
		$$.index2 = $1.index2;
		if (strcmp($$.index1, "-1") == 0)
			$$.index1 = mm_strdup("0");
		else if (strcmp($1.index2, "-1") == 0)
			$$.index2 = mm_strdup("0");
		$$.str = cat_str(2, $1.str, mm_strdup("[]"));
	}
	| opt_array_bounds '[' Iresult ']'
	{
		$$.index1 = $1.index1;
		$$.index2 = $1.index2;
		if (strcmp($1.index1, "-1") == 0)
			$$.index1 = mm_strdup($3);
		else if (strcmp($1.index2, "-1") == 0)
			$$.index2 = mm_strdup($3);
		$$.str = cat_str(4, $1.str, mm_strdup("["), $3, mm_strdup("]"));
	}
| 
	{
		$$.index1 = mm_strdup("-1");
		$$.index2 = mm_strdup("-1");
		$$.str= EMPTY;
	}
;


 SimpleTypename:
 GenericType
 { 
 $$ = $1;
}
|  Numeric
 { 
 $$ = $1;
}
|  Bit
 { 
 $$ = $1;
}
|  Character
 { 
 $$ = $1;
}
|  ConstDatetime
 { 
 $$ = $1;
}
|  ConstSet
 { 
 $$ = $1;
}
|  ConstInterval opt_interval
 { 
 $$ = cat_str(2,$1,$2);
}
|  ConstInterval '(' Iconst ')' opt_interval
 { 
 $$ = cat_str(5,$1,mm_strdup("("),$3,mm_strdup(")"),$5);
}
;


 ConstTypename:
 Numeric
 { 
 $$ = $1;
}
|  ConstBit
 { 
 $$ = $1;
}
|  ConstCharacter
 { 
 $$ = $1;
}
|  ConstDatetime
 { 
 $$ = $1;
}
|  ConstSet
 { 
 $$ = $1;
}
;


 GenericType:
 type_function_name opt_type_modifiers
 { 
 $$ = cat_str(2,$1,$2);
}
|  type_function_name attrs opt_type_modifiers
 { 
 $$ = cat_str(3,$1,$2,$3);
}
;


 opt_type_modifiers:
 '(' expr_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 Numeric:
 INT_P
 { 
 $$ = mm_strdup("int");
}
|  INTEGER opt_type_modifiers
 { 
 $$ = cat_str(2,mm_strdup("integer"),$2);
}
|  SMALLINT
 { 
 $$ = mm_strdup("smallint");
}
|  TINYINT
 { 
 $$ = mm_strdup("tinyint");
}
|  BIGINT
 { 
 $$ = mm_strdup("bigint");
}
|  REAL
 { 
 $$ = mm_strdup("real");
}
|  FLOAT_P opt_float
 { 
 $$ = cat_str(2,mm_strdup("float"),$2);
}
|  BINARY_DOUBLE
 { 
 $$ = mm_strdup("binary_double");
}
|  BINARY_INTEGER
 { 
 $$ = mm_strdup("binary_integer");
}
|  DOUBLE_P PRECISION
 { 
 $$ = mm_strdup("double precision");
}
|  DECIMAL_P opt_type_modifiers
 { 
 $$ = cat_str(2,mm_strdup("decimal"),$2);
}
|  NUMBER_P opt_type_modifiers
 { 
 $$ = cat_str(2,mm_strdup("number"),$2);
}
|  DEC opt_type_modifiers
 { 
 $$ = cat_str(2,mm_strdup("dec"),$2);
}
|  NUMERIC opt_type_modifiers
 { 
 $$ = cat_str(2,mm_strdup("numeric"),$2);
}
|  BOOLEAN_P
 { 
 $$ = mm_strdup("boolean");
}
;


 opt_float:
 '(' Iconst ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 Bit:
 BitWithLength
 { 
 $$ = $1;
}
|  BitWithoutLength
 { 
 $$ = $1;
}
;


 ConstBit:
 BitWithLength
 { 
 $$ = $1;
}
|  BitWithoutLength
 { 
 $$ = $1;
}
;


 BitWithLength:
 BIT opt_varying '(' expr_list ')'
 { 
 $$ = cat_str(5,mm_strdup("bit"),$2,mm_strdup("("),$4,mm_strdup(")"));
}
;


 BitWithoutLength:
 BIT opt_varying
 { 
 $$ = cat_str(2,mm_strdup("bit"),$2);
}
;


 Character:
 CharacterWithLength
 { 
 $$ = $1;
}
|  CharacterWithoutLength
 { 
 $$ = $1;
}
;


 ConstCharacter:
 CharacterWithLength
 { 
 $$ = $1;
}
|  CharacterWithoutLength
 { 
 $$ = $1;
}
;


 CharacterWithLength:
 character '(' Iconst ')'
 { 
 $$ = cat_str(4,$1,mm_strdup("("),$3,mm_strdup(")"));
}
;


 CharacterWithoutLength:
 character
 { 
 $$ = $1;
}
;


 character:
 CHARACTER opt_varying
 { 
 $$ = cat_str(2,mm_strdup("character"),$2);
}
|  CHAR_P opt_varying
 { 
 $$ = cat_str(2,mm_strdup("char"),$2);
}
|  NVARCHAR
 { 
 $$ = mm_strdup("nvarchar");
}
|  NVARCHAR2
 { 
 $$ = mm_strdup("nvarchar2");
}
|  VARCHAR
 { 
 $$ = mm_strdup("varchar");
}
|  VARCHAR2
 { 
 $$ = mm_strdup("varchar2");
}
|  NATIONAL CHARACTER opt_varying
 { 
 $$ = cat_str(2,mm_strdup("national character"),$3);
}
|  NATIONAL CHAR_P opt_varying
 { 
 $$ = cat_str(2,mm_strdup("national char"),$3);
}
|  NCHAR opt_varying
 { 
 $$ = cat_str(2,mm_strdup("nchar"),$2);
}
;


 opt_varying:
 VARYING
 { 
 $$ = mm_strdup("varying");
}
| 
 { 
 $$=EMPTY; }
;


 character_set:
 CHARACTER SET
 { 
 $$ = mm_strdup("character set");
}
|  CHARSET
 { 
 $$ = mm_strdup("charset");
}
;


 charset_collate_name:
 ColId
 { 
 $$ = $1;
}
|  BINARY
 { 
 $$ = mm_strdup("binary");
}
|  ecpg_sconst
 { 
 $$ = $1;
}
;


 charset:
 character_set charset_collate_name
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,$1,$2);
}
;


 convert_charset:
 charset
 { 
 $$ = $1;
}
|  character_set DEFAULT
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,$1,mm_strdup("default"));
}
;


 opt_charset:
 charset
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 default_charset:
 DEFAULT character_set opt_equal charset_collate_name
 { 
 $$ = cat_str(4,mm_strdup("default"),$2,$3,$4);
}
|  character_set opt_equal charset_collate_name
 { 
 $$ = cat_str(3,$1,$2,$3);
}
;


 optCharsetCollate:
 CharsetCollate
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 CharsetCollate:
 charset_collate
 { 
 $$ = $1;
}
|  CharsetCollate charset_collate
 { 
 $$ = cat_str(2,$1,$2);
}
;


 charset_collate:
 default_charset
 { 
 $$ = $1;
}
|  default_collate
 { 
 $$ = $1;
}
;


 ConstDatetime:
 TIMESTAMP '(' Iconst ')' opt_timezone
 { 
 $$ = cat_str(4,mm_strdup("timestamp ("),$3,mm_strdup(")"),$5);
}
|  TIMESTAMP opt_timezone
 { 
 $$ = cat_str(2,mm_strdup("timestamp"),$2);
}
|  TIME '(' Iconst ')' opt_timezone
 { 
 $$ = cat_str(4,mm_strdup("time ("),$3,mm_strdup(")"),$5);
}
|  TIME opt_timezone
 { 
 $$ = cat_str(2,mm_strdup("time"),$2);
}
|  DATE_P
 { 
 $$ = mm_strdup("date");
}
|  SMALLDATETIME
 { 
 $$ = mm_strdup("smalldatetime");
}
;


 ConstSet:
 SET '(' opt_enum_val_list ')'
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,mm_strdup("set ("),$3,mm_strdup(")"));
}
;


 ConstInterval:
 INTERVAL
 { 
 $$ = mm_strdup("interval");
}
;


 opt_timezone:
 WITH_TIME ZONE
 { 
 $$ = mm_strdup("with time zone");
}
|  WITHOUT TIME ZONE
 { 
 $$ = mm_strdup("without time zone");
}
| 
 { 
 $$=EMPTY; }
;


 opt_interval:
 YEAR_P
 { 
 $$ = mm_strdup("year");
}
|  MONTH_P
 { 
 $$ = mm_strdup("month");
}
|  DAY_P
 { 
 $$ = mm_strdup("day");
}
|  HOUR_P
 { 
 $$ = mm_strdup("hour");
}
|  MINUTE_P
 { 
 $$ = mm_strdup("minute");
}
|  interval_second
 { 
 $$ = $1;
}
|  YEAR_P TO MONTH_P
 { 
 $$ = mm_strdup("year to month");
}
|  DAY_P TO HOUR_P
 { 
 $$ = mm_strdup("day to hour");
}
|  DAY_P TO MINUTE_P
 { 
 $$ = mm_strdup("day to minute");
}
|  DAY_P TO interval_second
 { 
 $$ = cat_str(2,mm_strdup("day to"),$3);
}
|  HOUR_P TO MINUTE_P
 { 
 $$ = mm_strdup("hour to minute");
}
|  HOUR_P TO interval_second
 { 
 $$ = cat_str(2,mm_strdup("hour to"),$3);
}
|  MINUTE_P TO interval_second
 { 
 $$ = cat_str(2,mm_strdup("minute to"),$3);
}
|  YEAR_P '(' Iconst ')'
 { 
 $$ = cat_str(3,mm_strdup("year ("),$3,mm_strdup(")"));
}
|  MONTH_P '(' Iconst ')'
 { 
 $$ = cat_str(3,mm_strdup("month ("),$3,mm_strdup(")"));
}
|  DAY_P '(' Iconst ')'
 { 
 $$ = cat_str(3,mm_strdup("day ("),$3,mm_strdup(")"));
}
|  HOUR_P '(' Iconst ')'
 { 
 $$ = cat_str(3,mm_strdup("hour ("),$3,mm_strdup(")"));
}
|  MINUTE_P '(' Iconst ')'
 { 
 $$ = cat_str(3,mm_strdup("minute ("),$3,mm_strdup(")"));
}
|  YEAR_P '(' Iconst ')' TO MONTH_P
 { 
 $$ = cat_str(3,mm_strdup("year ("),$3,mm_strdup(") to month"));
}
|  DAY_P '(' Iconst ')' TO HOUR_P
 { 
 $$ = cat_str(3,mm_strdup("day ("),$3,mm_strdup(") to hour"));
}
|  DAY_P '(' Iconst ')' TO MINUTE_P
 { 
 $$ = cat_str(3,mm_strdup("day ("),$3,mm_strdup(") to minute"));
}
|  DAY_P '(' Iconst ')' TO interval_second
 { 
 $$ = cat_str(4,mm_strdup("day ("),$3,mm_strdup(") to"),$6);
}
|  HOUR_P '(' Iconst ')' TO MINUTE_P
 { 
 $$ = cat_str(3,mm_strdup("hour ("),$3,mm_strdup(") to minute"));
}
|  HOUR_P '(' Iconst ')' TO interval_second
 { 
 $$ = cat_str(4,mm_strdup("hour ("),$3,mm_strdup(") to"),$6);
}
|  MINUTE_P '(' Iconst ')' TO interval_second
 { 
 $$ = cat_str(4,mm_strdup("minute ("),$3,mm_strdup(") to"),$6);
}
| 
 { 
 $$=EMPTY; }
;


 interval_second:
 SECOND_P
 { 
 $$ = mm_strdup("second");
}
|  SECOND_P '(' Iconst ')'
 { 
 $$ = cat_str(3,mm_strdup("second ("),$3,mm_strdup(")"));
}
;


 client_logic_type:
 BYTEAWITHOUTORDER
 { 
 $$ = mm_strdup("byteawithoutorder");
}
|  BYTEAWITHOUTORDERWITHEQUAL
 { 
 $$ = mm_strdup("byteawithoutorderwithequal");
}
;


 a_expr:
 c_expr
 { 
 $$ = $1;
}
|  PRIOR '(' a_expr ')'
 { 
 $$ = cat_str(3,mm_strdup("prior ("),$3,mm_strdup(")"));
}
|  a_expr TYPECAST Typename
 { 
 $$ = cat_str(3,$1,mm_strdup("::"),$3);
}
|  a_expr COLLATE collate_name
 { 
 $$ = cat_str(3,$1,mm_strdup("collate"),$3);
}
|  a_expr AT TIME ZONE a_expr %prec AT
 { 
 $$ = cat_str(3,$1,mm_strdup("at time zone"),$5);
}
|  '+' a_expr %prec UMINUS
 { 
 $$ = cat_str(2,mm_strdup("+"),$2);
}
|  '-' a_expr %prec UMINUS
 { 
 $$ = cat_str(2,mm_strdup("-"),$2);
}
|  '@' a_expr
 { 
 $$ = cat_str(2,mm_strdup("@"),$2);
}
|  a_expr '+' a_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("+"),$3);
}
|  a_expr '-' a_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("-"),$3);
}
|  a_expr '*' a_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("*"),$3);
}
|  a_expr '/' a_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("/"),$3);
}
|  a_expr '%' a_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("%"),$3);
}
|  a_expr '^' a_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("^"),$3);
}
|  a_expr '<' a_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("<"),$3);
}
|  a_expr '>' a_expr
 { 
 $$ = cat_str(3,$1,mm_strdup(">"),$3);
}
|  a_expr '=' a_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("="),$3);
}
|  a_expr '@' a_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("@"),$3);
}
|  a_expr CmpNullOp a_expr %prec IS
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,$1,mm_strdup("cmpnullop"),$3);
}
|  uservar_name COLON_EQUALS a_expr
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,$1,mm_strdup(":="),$3);
}
|  a_expr CmpOp a_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("cmpop"),$3);
}
|  a_expr qual_Op a_expr %prec Op
 { 
 $$ = cat_str(3,$1,$2,$3);
}
|  qual_Op a_expr %prec Op
 { 
 $$ = cat_str(2,$1,$2);
}
|  a_expr qual_Op %prec POSTFIXOP
 { 
 $$ = cat_str(2,$1,$2);
}
|  a_expr AND a_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("and"),$3);
}
|  a_expr OR a_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("or"),$3);
}
|  NOT a_expr
 { 
 $$ = cat_str(2,mm_strdup("not"),$2);
}
|  a_expr LIKE a_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("like"),$3);
}
|  a_expr LIKE a_expr ESCAPE a_expr
 { 
 $$ = cat_str(5,$1,mm_strdup("like"),$3,mm_strdup("escape"),$5);
}
|  a_expr NOT_LIKE a_expr %prec NOT_LIKE
 { 
 $$ = cat_str(3,$1,mm_strdup("not_like"),$3);
}
|  a_expr NOT_LIKE a_expr ESCAPE a_expr %prec NOT_LIKE
 { 
 $$ = cat_str(5,$1,mm_strdup("not_like"),$3,mm_strdup("escape"),$5);
}
|  a_expr ILIKE a_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("ilike"),$3);
}
|  a_expr ILIKE a_expr ESCAPE a_expr
 { 
 $$ = cat_str(5,$1,mm_strdup("ilike"),$3,mm_strdup("escape"),$5);
}
|  a_expr NOT_ILIKE a_expr %prec NOT_ILIKE
 { 
 $$ = cat_str(3,$1,mm_strdup("not_ilike"),$3);
}
|  a_expr NOT_ILIKE a_expr ESCAPE a_expr %prec NOT_ILIKE
 { 
 $$ = cat_str(5,$1,mm_strdup("not_ilike"),$3,mm_strdup("escape"),$5);
}
|  a_expr SIMILAR TO a_expr %prec SIMILAR
 { 
 $$ = cat_str(3,$1,mm_strdup("similar to"),$4);
}
|  a_expr SIMILAR TO a_expr ESCAPE a_expr
 { 
 $$ = cat_str(5,$1,mm_strdup("similar to"),$4,mm_strdup("escape"),$6);
}
|  a_expr NOT_SIMILAR TO a_expr %prec NOT_SIMILAR
 { 
 $$ = cat_str(3,$1,mm_strdup("not_similar to"),$4);
}
|  a_expr NOT_SIMILAR TO a_expr ESCAPE a_expr %prec NOT_SIMILAR
 { 
 $$ = cat_str(5,$1,mm_strdup("not_similar to"),$4,mm_strdup("escape"),$6);
}
|  a_expr IS NULL_P %prec IS
 { 
 $$ = cat_str(2,$1,mm_strdup("is null"));
}
|  a_expr ISNULL
 { 
 $$ = cat_str(2,$1,mm_strdup("isnull"));
}
|  a_expr IS NOT NULL_P %prec IS
 { 
 $$ = cat_str(2,$1,mm_strdup("is not null"));
}
|  a_expr NOTNULL
 { 
 $$ = cat_str(2,$1,mm_strdup("notnull"));
}
|  row OVERLAPS row
 { 
 $$ = cat_str(3,$1,mm_strdup("overlaps"),$3);
}
|  a_expr IS TRUE_P %prec IS
 { 
 $$ = cat_str(2,$1,mm_strdup("is true"));
}
|  a_expr IS NOT TRUE_P %prec IS
 { 
 $$ = cat_str(2,$1,mm_strdup("is not true"));
}
|  a_expr IS FALSE_P %prec IS
 { 
 $$ = cat_str(2,$1,mm_strdup("is false"));
}
|  a_expr IS NOT FALSE_P %prec IS
 { 
 $$ = cat_str(2,$1,mm_strdup("is not false"));
}
|  a_expr IS UNKNOWN %prec IS
 { 
 $$ = cat_str(2,$1,mm_strdup("is unknown"));
}
|  a_expr IS NOT UNKNOWN %prec IS
 { 
 $$ = cat_str(2,$1,mm_strdup("is not unknown"));
}
|  a_expr IS DISTINCT FROM a_expr %prec IS
 { 
 $$ = cat_str(3,$1,mm_strdup("is distinct from"),$5);
}
|  a_expr IS NOT DISTINCT FROM a_expr %prec IS
 { 
 $$ = cat_str(3,$1,mm_strdup("is not distinct from"),$6);
}
|  a_expr IS OF '(' type_list ')' %prec IS
 { 
 $$ = cat_str(4,$1,mm_strdup("is of ("),$5,mm_strdup(")"));
}
|  a_expr IS NOT OF '(' type_list ')' %prec IS
 { 
 $$ = cat_str(4,$1,mm_strdup("is not of ("),$6,mm_strdup(")"));
}
|  a_expr BETWEEN opt_asymmetric b_expr AND b_expr %prec BETWEEN
 { 
 $$ = cat_str(6,$1,mm_strdup("between"),$3,$4,mm_strdup("and"),$6);
}
|  a_expr NOT_BETWEEN opt_asymmetric b_expr AND b_expr %prec NOT_BETWEEN
 { 
 $$ = cat_str(6,$1,mm_strdup("not_between"),$3,$4,mm_strdup("and"),$6);
}
|  a_expr BETWEEN SYMMETRIC b_expr AND b_expr %prec BETWEEN
 { 
 $$ = cat_str(5,$1,mm_strdup("between symmetric"),$4,mm_strdup("and"),$6);
}
|  a_expr NOT_BETWEEN SYMMETRIC b_expr AND b_expr %prec NOT_BETWEEN
 { 
 $$ = cat_str(5,$1,mm_strdup("not_between symmetric"),$4,mm_strdup("and"),$6);
}
|  a_expr IN_P in_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("in"),$3);
}
|  a_expr NOT_IN in_expr %prec NOT_IN
 { 
 $$ = cat_str(3,$1,mm_strdup("not_in"),$3);
}
|  a_expr subquery_Op sub_type select_with_parens %prec Op
 { 
 $$ = cat_str(4,$1,$2,$3,$4);
}
|  a_expr subquery_Op sub_type '(' a_expr ')' %prec Op
 { 
 $$ = cat_str(6,$1,$2,$3,mm_strdup("("),$5,mm_strdup(")"));
}
|  UNIQUE select_with_parens
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("unique"),$2);
}
|  a_expr IS DOCUMENT_P %prec IS
 { 
 $$ = cat_str(2,$1,mm_strdup("is document"));
}
|  a_expr IS NOT DOCUMENT_P %prec IS
 { 
 $$ = cat_str(2,$1,mm_strdup("is not document"));
}
|  PREDICT BY ColId '(' FEATURES func_arg_list ')'
 { 
 $$ = cat_str(5,mm_strdup("predict by"),$3,mm_strdup("( features"),$6,mm_strdup(")"));
}
;


 b_expr:
 c_expr
 { 
 $$ = $1;
}
|  b_expr TYPECAST Typename
 { 
 $$ = cat_str(3,$1,mm_strdup("::"),$3);
}
|  '+' b_expr %prec UMINUS
 { 
 $$ = cat_str(2,mm_strdup("+"),$2);
}
|  '-' b_expr %prec UMINUS
 { 
 $$ = cat_str(2,mm_strdup("-"),$2);
}
|  '@' b_expr
 { 
 $$ = cat_str(2,mm_strdup("@"),$2);
}
|  b_expr '+' b_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("+"),$3);
}
|  b_expr '-' b_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("-"),$3);
}
|  b_expr '*' b_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("*"),$3);
}
|  b_expr '/' b_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("/"),$3);
}
|  b_expr '%' b_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("%"),$3);
}
|  b_expr '^' b_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("^"),$3);
}
|  b_expr '<' b_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("<"),$3);
}
|  b_expr '>' b_expr
 { 
 $$ = cat_str(3,$1,mm_strdup(">"),$3);
}
|  b_expr '=' b_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("="),$3);
}
|  b_expr '@' b_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("@"),$3);
}
|  b_expr CmpOp b_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("cmpop"),$3);
}
|  b_expr qual_Op b_expr %prec Op
 { 
 $$ = cat_str(3,$1,$2,$3);
}
|  b_expr CmpNullOp b_expr %prec IS
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,$1,mm_strdup("cmpnullop"),$3);
}
|  qual_Op b_expr %prec Op
 { 
 $$ = cat_str(2,$1,$2);
}
|  b_expr qual_Op %prec POSTFIXOP
 { 
 $$ = cat_str(2,$1,$2);
}
|  b_expr IS DISTINCT FROM b_expr %prec IS
 { 
 $$ = cat_str(3,$1,mm_strdup("is distinct from"),$5);
}
|  b_expr IS NOT DISTINCT FROM b_expr %prec IS
 { 
 $$ = cat_str(3,$1,mm_strdup("is not distinct from"),$6);
}
|  b_expr IS OF '(' type_list ')' %prec IS
 { 
 $$ = cat_str(4,$1,mm_strdup("is of ("),$5,mm_strdup(")"));
}
|  b_expr IS NOT OF '(' type_list ')' %prec IS
 { 
 $$ = cat_str(4,$1,mm_strdup("is not of ("),$6,mm_strdup(")"));
}
|  b_expr IS DOCUMENT_P %prec IS
 { 
 $$ = cat_str(2,$1,mm_strdup("is document"));
}
|  b_expr IS NOT DOCUMENT_P %prec IS
 { 
 $$ = cat_str(2,$1,mm_strdup("is not document"));
}
;


 c_expr:
 columnref %prec UMINUS
 { 
 $$ = $1;
}
|  AexprConst
 { 
 $$ = $1;
}
|  PRIOR '(' columnref ')'
 { 
 $$ = cat_str(3,mm_strdup("prior ("),$3,mm_strdup(")"));
}
|  PRIOR '(' c_expr ',' func_arg_list ')'
 { 
 $$ = cat_str(5,mm_strdup("prior ("),$3,mm_strdup(","),$5,mm_strdup(")"));
}
|  PRIOR columnref
 { 
 $$ = cat_str(2,mm_strdup("prior"),$2);
}
|  ecpg_param opt_indirection
 { 
 $$ = cat_str(2,$1,$2);
}
|  '(' a_expr ')' opt_indirection
 { 
 $$ = cat_str(4,mm_strdup("("),$2,mm_strdup(")"),$4);
}
|  case_expr
 { 
 $$ = $1;
}
|  func_expr
 { 
 $$ = $1;
}
|  select_with_parens %prec UMINUS
 { 
 $$ = $1;
}
|  select_with_parens indirection
 { 
 $$ = cat_str(2,$1,$2);
}
|  EXISTS select_with_parens
 { 
 $$ = cat_str(2,mm_strdup("exists"),$2);
}
|  ARRAY select_with_parens
 { 
 $$ = cat_str(2,mm_strdup("array"),$2);
}
|  ARRAY array_expr
 { 
 $$ = cat_str(2,mm_strdup("array"),$2);
}
|  explicit_row
 { 
 $$ = $1;
}
|  implicit_row
 { 
 $$ = $1;
}
|  GROUPING_P '(' expr_list ')'
 { 
 $$ = cat_str(3,mm_strdup("grouping ("),$3,mm_strdup(")"));
}
|  uservar_name %prec UMINUS
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = $1;
}
|  set_ident_expr
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = $1;
}
;


 c_expr_noparen:
 columnref
 { 
 $$ = $1;
}
|  AexprConst
 { 
 $$ = $1;
}
|  ecpg_param opt_indirection
 { 
 $$ = cat_str(2,$1,$2);
}
|  case_expr
 { 
 $$ = $1;
}
|  func_expr
 { 
 $$ = $1;
}
|  select_with_parens %prec UMINUS
 { 
 $$ = $1;
}
|  select_with_parens indirection
 { 
 $$ = cat_str(2,$1,$2);
}
|  EXISTS select_with_parens
 { 
 $$ = cat_str(2,mm_strdup("exists"),$2);
}
|  ARRAY select_with_parens
 { 
 $$ = cat_str(2,mm_strdup("array"),$2);
}
|  ARRAY array_expr
 { 
 $$ = cat_str(2,mm_strdup("array"),$2);
}
|  explicit_row
 { 
 $$ = $1;
}
|  GROUPING_P '(' expr_list ')'
 { 
 $$ = cat_str(3,mm_strdup("grouping ("),$3,mm_strdup(")"));
}
;


 func_expr:
 func_application within_group_clause over_clause
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(3,$1,$2,$3);
}
|  func_with_separator
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = $1;
}
|  func_expr_common_subexpr
 { 
 $$ = $1;
}
;


 func_application:
 func_name '(' func_arg_list opt_sort_clause ')'
 { 
 $$ = cat_str(5,$1,mm_strdup("("),$3,$4,mm_strdup(")"));
}
|  func_application_special
 { 
 $$ = $1;
}
;


 func_application_special:
 func_name '(' ')'
 { 
 $$ = cat_str(2,$1,mm_strdup("( )"));
}
|  func_name '(' VARIADIC func_arg_expr opt_sort_clause ')'
 { 
 $$ = cat_str(5,$1,mm_strdup("( variadic"),$4,$5,mm_strdup(")"));
}
|  func_name '(' func_arg_list ',' VARIADIC func_arg_expr opt_sort_clause ')'
 { 
 $$ = cat_str(7,$1,mm_strdup("("),$3,mm_strdup(", variadic"),$6,$7,mm_strdup(")"));
}
|  func_name '(' ALL func_arg_list opt_sort_clause ')'
 { 
 $$ = cat_str(5,$1,mm_strdup("( all"),$4,$5,mm_strdup(")"));
}
|  func_name '(' DISTINCT func_arg_list opt_sort_clause ')'
 { 
 $$ = cat_str(5,$1,mm_strdup("( distinct"),$4,$5,mm_strdup(")"));
}
|  func_name '(' '*' ')'
 { 
 $$ = cat_str(2,$1,mm_strdup("( * )"));
}
;


 func_with_separator:
 func_name '(' func_arg_list opt_sort_clause SEPARATOR_P ecpg_sconst ')'
 { 
 $$ = cat_str(7,$1,mm_strdup("("),$3,$4,mm_strdup("separator"),$6,mm_strdup(")"));
}
|  func_name '(' DISTINCT func_arg_list opt_sort_clause SEPARATOR_P ecpg_sconst ')'
 { 
 $$ = cat_str(7,$1,mm_strdup("( distinct"),$4,$5,mm_strdup("separator"),$7,mm_strdup(")"));
}
;


 func_expr_windowless:
 func_application
 { 
 $$ = $1;
}
|  func_expr_common_subexpr
 { 
 $$ = $1;
}
;


 func_expr_common_subexpr:
 COLLATION FOR '(' a_expr ')'
 { 
 $$ = cat_str(3,mm_strdup("collation for ("),$4,mm_strdup(")"));
}
|  CURRENT_DATE
 { 
 $$ = mm_strdup("current_date");
}
|  CURRENT_TIME
 { 
 $$ = mm_strdup("current_time");
}
|  CURRENT_TIME '(' Iconst ')'
 { 
 $$ = cat_str(3,mm_strdup("current_time ("),$3,mm_strdup(")"));
}
|  CURRENT_TIMESTAMP
 { 
 $$ = mm_strdup("current_timestamp");
}
|  CURRENT_TIMESTAMP '(' Iconst ')'
 { 
 $$ = cat_str(3,mm_strdup("current_timestamp ("),$3,mm_strdup(")"));
}
|  LOCALTIME
 { 
 $$ = mm_strdup("localtime");
}
|  LOCALTIME '(' Iconst ')'
 { 
 $$ = cat_str(3,mm_strdup("localtime ("),$3,mm_strdup(")"));
}
|  LOCALTIMESTAMP
 { 
 $$ = mm_strdup("localtimestamp");
}
|  LOCALTIMESTAMP '(' Iconst ')'
 { 
 $$ = cat_str(3,mm_strdup("localtimestamp ("),$3,mm_strdup(")"));
}
|  SYSDATE
 { 
 $$ = mm_strdup("sysdate");
}
|  ROWNUM
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = mm_strdup("rownum");
}
|  CURRENT_ROLE
 { 
 $$ = mm_strdup("current_role");
}
|  CURRENT_USER
 { 
 $$ = mm_strdup("current_user");
}
|  SESSION_USER
 { 
 $$ = mm_strdup("session_user");
}
|  USER
 { 
 $$ = mm_strdup("user");
}
|  CURRENT_CATALOG
 { 
 $$ = mm_strdup("current_catalog");
}
|  CURRENT_SCHEMA
 { 
 $$ = mm_strdup("current_schema");
}
|  CAST '(' a_expr AS Typename ')'
 { 
 $$ = cat_str(5,mm_strdup("cast ("),$3,mm_strdup("as"),$5,mm_strdup(")"));
}
|  EXTRACT '(' extract_list ')'
 { 
 $$ = cat_str(3,mm_strdup("extract ("),$3,mm_strdup(")"));
}
|  TIMESTAMPDIFF '(' timestamp_arg_list ')'
 { 
 $$ = cat_str(3,mm_strdup("timestampdiff ("),$3,mm_strdup(")"));
}
|  OVERLAY '(' overlay_list ')'
 { 
 $$ = cat_str(3,mm_strdup("overlay ("),$3,mm_strdup(")"));
}
|  POSITION '(' position_list ')'
 { 
 $$ = cat_str(3,mm_strdup("position ("),$3,mm_strdup(")"));
}
|  SUBSTRING '(' substr_list ')'
 { 
 $$ = cat_str(3,mm_strdup("substring ("),$3,mm_strdup(")"));
}
|  TREAT '(' a_expr AS Typename ')'
 { 
 $$ = cat_str(5,mm_strdup("treat ("),$3,mm_strdup("as"),$5,mm_strdup(")"));
}
|  TRIM '(' BOTH trim_list ')'
 { 
 $$ = cat_str(3,mm_strdup("trim ( both"),$4,mm_strdup(")"));
}
|  TRIM '(' LEADING trim_list ')'
 { 
 $$ = cat_str(3,mm_strdup("trim ( leading"),$4,mm_strdup(")"));
}
|  TRIM '(' TRAILING trim_list ')'
 { 
 $$ = cat_str(3,mm_strdup("trim ( trailing"),$4,mm_strdup(")"));
}
|  TRIM '(' trim_list ')'
 { 
 $$ = cat_str(3,mm_strdup("trim ("),$3,mm_strdup(")"));
}
|  NULLIF '(' a_expr ',' a_expr ')'
 { 
 $$ = cat_str(5,mm_strdup("nullif ("),$3,mm_strdup(","),$5,mm_strdup(")"));
}
|  NVL '(' a_expr ',' a_expr ')'
 { 
 $$ = cat_str(5,mm_strdup("nvl ("),$3,mm_strdup(","),$5,mm_strdup(")"));
}
|  COALESCE '(' expr_list ')'
 { 
 $$ = cat_str(3,mm_strdup("coalesce ("),$3,mm_strdup(")"));
}
|  GREATEST '(' expr_list ')'
 { 
 $$ = cat_str(3,mm_strdup("greatest ("),$3,mm_strdup(")"));
}
|  LEAST '(' expr_list ')'
 { 
 $$ = cat_str(3,mm_strdup("least ("),$3,mm_strdup(")"));
}
|  XMLCONCAT '(' expr_list ')'
 { 
 $$ = cat_str(3,mm_strdup("xmlconcat ("),$3,mm_strdup(")"));
}
|  XMLELEMENT '(' NAME_P ColLabel ')'
 { 
 $$ = cat_str(3,mm_strdup("xmlelement ( name"),$4,mm_strdup(")"));
}
|  XMLELEMENT '(' NAME_P ColLabel ',' xml_attributes ')'
 { 
 $$ = cat_str(5,mm_strdup("xmlelement ( name"),$4,mm_strdup(","),$6,mm_strdup(")"));
}
|  XMLELEMENT '(' NAME_P ColLabel ',' expr_list ')'
 { 
 $$ = cat_str(5,mm_strdup("xmlelement ( name"),$4,mm_strdup(","),$6,mm_strdup(")"));
}
|  XMLELEMENT '(' NAME_P ColLabel ',' xml_attributes ',' expr_list ')'
 { 
 $$ = cat_str(7,mm_strdup("xmlelement ( name"),$4,mm_strdup(","),$6,mm_strdup(","),$8,mm_strdup(")"));
}
|  XMLEXISTS '(' c_expr xmlexists_argument ')'
 { 
 $$ = cat_str(4,mm_strdup("xmlexists ("),$3,$4,mm_strdup(")"));
}
|  XMLFOREST '(' xml_attribute_list ')'
 { 
 $$ = cat_str(3,mm_strdup("xmlforest ("),$3,mm_strdup(")"));
}
|  XMLPARSE '(' document_or_content a_expr xml_whitespace_option ')'
 { 
 $$ = cat_str(5,mm_strdup("xmlparse ("),$3,$4,$5,mm_strdup(")"));
}
|  XMLPI '(' NAME_P ColLabel ')'
 { 
 $$ = cat_str(3,mm_strdup("xmlpi ( name"),$4,mm_strdup(")"));
}
|  XMLPI '(' NAME_P ColLabel ',' a_expr ')'
 { 
 $$ = cat_str(5,mm_strdup("xmlpi ( name"),$4,mm_strdup(","),$6,mm_strdup(")"));
}
|  XMLROOT '(' a_expr ',' xml_root_version opt_xml_root_standalone ')'
 { 
 $$ = cat_str(6,mm_strdup("xmlroot ("),$3,mm_strdup(","),$5,$6,mm_strdup(")"));
}
|  XMLSERIALIZE '(' document_or_content a_expr AS SimpleTypename ')'
 { 
 $$ = cat_str(6,mm_strdup("xmlserialize ("),$3,$4,mm_strdup("as"),$6,mm_strdup(")"));
}
;


 xml_root_version:
 VERSION_P a_expr
 { 
 $$ = cat_str(2,mm_strdup("version"),$2);
}
|  VERSION_P NO VALUE_P
 { 
 $$ = mm_strdup("version no value");
}
;


 opt_xml_root_standalone:
 ',' STANDALONE_P YES_P
 { 
 $$ = mm_strdup(", standalone yes");
}
|  ',' STANDALONE_P NO
 { 
 $$ = mm_strdup(", standalone no");
}
|  ',' STANDALONE_P NO VALUE_P
 { 
 $$ = mm_strdup(", standalone no value");
}
| 
 { 
 $$=EMPTY; }
;


 xml_attributes:
 XMLATTRIBUTES '(' xml_attribute_list ')'
 { 
 $$ = cat_str(3,mm_strdup("xmlattributes ("),$3,mm_strdup(")"));
}
;


 xml_attribute_list:
 xml_attribute_el
 { 
 $$ = $1;
}
|  xml_attribute_list ',' xml_attribute_el
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 xml_attribute_el:
 a_expr AS ColLabel
 { 
 $$ = cat_str(3,$1,mm_strdup("as"),$3);
}
|  a_expr
 { 
 $$ = $1;
}
;


 document_or_content:
 DOCUMENT_P
 { 
 $$ = mm_strdup("document");
}
|  CONTENT_P
 { 
 $$ = mm_strdup("content");
}
;


 xml_whitespace_option:
 PRESERVE WHITESPACE_P
 { 
 $$ = mm_strdup("preserve whitespace");
}
|  STRIP_P WHITESPACE_P
 { 
 $$ = mm_strdup("strip whitespace");
}
| 
 { 
 $$=EMPTY; }
;


 xmlexists_argument:
 PASSING c_expr
 { 
 $$ = cat_str(2,mm_strdup("passing"),$2);
}
|  PASSING c_expr BY REF
 { 
 $$ = cat_str(3,mm_strdup("passing"),$2,mm_strdup("by ref"));
}
|  PASSING BY REF c_expr
 { 
 $$ = cat_str(2,mm_strdup("passing by ref"),$4);
}
|  PASSING BY REF c_expr BY REF
 { 
 $$ = cat_str(3,mm_strdup("passing by ref"),$4,mm_strdup("by ref"));
}
;


 within_group_clause:
 WITHIN GROUP_P '(' sort_clause ')'
 { 
 $$ = cat_str(3,mm_strdup("within group ("),$4,mm_strdup(")"));
}
| 
 { 
 $$=EMPTY; }
;


 window_clause:
 WINDOW window_definition_list
 { 
 $$ = cat_str(2,mm_strdup("window"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 window_definition_list:
 window_definition
 { 
 $$ = $1;
}
|  window_definition_list ',' window_definition
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 window_definition:
 ColId AS window_specification
 { 
 $$ = cat_str(3,$1,mm_strdup("as"),$3);
}
;


 over_clause:
 OVER window_specification
 { 
 $$ = cat_str(2,mm_strdup("over"),$2);
}
|  OVER ColId
 { 
 $$ = cat_str(2,mm_strdup("over"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 window_specification:
 '(' opt_existing_window_name opt_partition_clause opt_sort_clause opt_frame_clause ')'
 { 
 $$ = cat_str(6,mm_strdup("("),$2,$3,$4,$5,mm_strdup(")"));
}
;


 opt_existing_window_name:
 ColId
 { 
 $$ = $1;
}
|  %prec Op
 { 
 $$=EMPTY; }
;


 opt_partition_clause:
 PARTITION BY expr_list
 { 
 $$ = cat_str(2,mm_strdup("partition by"),$3);
}
| 
 { 
 $$=EMPTY; }
;


 opt_frame_clause:
 RANGE frame_extent
 { 
mmerror(PARSE_ERROR, ET_WARNING, "unsupported feature will be passed to server");
 $$ = cat_str(2,mm_strdup("range"),$2);
}
|  ROWS frame_extent
 { 
 $$ = cat_str(2,mm_strdup("rows"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 frame_extent:
 frame_bound
 { 
 $$ = $1;
}
|  BETWEEN frame_bound AND frame_bound
 { 
 $$ = cat_str(4,mm_strdup("between"),$2,mm_strdup("and"),$4);
}
;


 frame_bound:
 UNBOUNDED PRECEDING
 { 
 $$ = mm_strdup("unbounded preceding");
}
|  UNBOUNDED FOLLOWING
 { 
 $$ = mm_strdup("unbounded following");
}
|  CURRENT_P ROW
 { 
 $$ = mm_strdup("current row");
}
|  a_expr PRECEDING
 { 
 $$ = cat_str(2,$1,mm_strdup("preceding"));
}
|  a_expr FOLLOWING
 { 
 $$ = cat_str(2,$1,mm_strdup("following"));
}
;


 row:
 ROW '(' expr_list ')'
 { 
 $$ = cat_str(3,mm_strdup("row ("),$3,mm_strdup(")"));
}
|  ROW '(' ')'
 { 
 $$ = mm_strdup("row ( )");
}
|  '(' expr_list ',' a_expr ')'
 { 
 $$ = cat_str(5,mm_strdup("("),$2,mm_strdup(","),$4,mm_strdup(")"));
}
;


 explicit_row:
 ROW '(' expr_list ')'
 { 
 $$ = cat_str(3,mm_strdup("row ("),$3,mm_strdup(")"));
}
|  ROW '(' ')'
 { 
 $$ = mm_strdup("row ( )");
}
;


 implicit_row:
 '(' expr_list ',' a_expr ')'
 { 
 $$ = cat_str(5,mm_strdup("("),$2,mm_strdup(","),$4,mm_strdup(")"));
}
;


 sub_type:
 ANY
 { 
 $$ = mm_strdup("any");
}
|  SOME
 { 
 $$ = mm_strdup("some");
}
|  ALL
 { 
 $$ = mm_strdup("all");
}
;


 all_Op:
 Op
 { 
 $$ = $1;
}
|  CmpOp
 { 
 $$ = mm_strdup("cmpop");
}
|  CmpNullOp
 { 
 $$ = mm_strdup("cmpnullop");
}
|  MathOp
 { 
 $$ = $1;
}
;


 MathOp:
 '+'
 { 
 $$ = mm_strdup("+");
}
|  '-'
 { 
 $$ = mm_strdup("-");
}
|  '*'
 { 
 $$ = mm_strdup("*");
}
|  '/'
 { 
 $$ = mm_strdup("/");
}
|  '%'
 { 
 $$ = mm_strdup("%");
}
|  '^'
 { 
 $$ = mm_strdup("^");
}
|  '<'
 { 
 $$ = mm_strdup("<");
}
|  '>'
 { 
 $$ = mm_strdup(">");
}
|  '='
 { 
 $$ = mm_strdup("=");
}
|  '@'
 { 
 $$ = mm_strdup("@");
}
;


 qual_Op:
 Op
 { 
 $$ = $1;
}
|  OPERATOR '(' any_operator ')'
 { 
 $$ = cat_str(3,mm_strdup("operator ("),$3,mm_strdup(")"));
}
;


 qual_all_Op:
 all_Op
 { 
 $$ = $1;
}
|  OPERATOR '(' any_operator ')'
 { 
 $$ = cat_str(3,mm_strdup("operator ("),$3,mm_strdup(")"));
}
;


 subquery_Op:
 all_Op
 { 
 $$ = $1;
}
|  OPERATOR '(' any_operator ')'
 { 
 $$ = cat_str(3,mm_strdup("operator ("),$3,mm_strdup(")"));
}
|  LIKE
 { 
 $$ = mm_strdup("like");
}
|  NOT_LIKE %prec NOT_LIKE
 { 
 $$ = mm_strdup("not_like");
}
|  ILIKE
 { 
 $$ = mm_strdup("ilike");
}
|  NOT_ILIKE %prec NOT_ILIKE
 { 
 $$ = mm_strdup("not_ilike");
}
;


 expr_list:
 a_expr
 { 
 $$ = $1;
}
|  expr_list ',' a_expr
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 func_arg_list:
 func_arg_expr
 { 
 $$ = $1;
}
|  func_arg_list ',' func_arg_expr
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 func_arg_expr:
 a_expr
 { 
 $$ = $1;
}
|  param_name COLON_EQUALS a_expr
 { 
 $$ = cat_str(3,$1,mm_strdup(":="),$3);
}
|  param_name PARA_EQUALS a_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("para_equals"),$3);
}
;


 type_list:
 Typename
 { 
 $$ = $1;
}
|  type_list ',' Typename
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 array_expr:
 '[' expr_list ']'
 { 
 $$ = cat_str(3,mm_strdup("["),$2,mm_strdup("]"));
}
|  '[' array_expr_list ']'
 { 
 $$ = cat_str(3,mm_strdup("["),$2,mm_strdup("]"));
}
|  '[' ']'
 { 
 $$ = mm_strdup("[ ]");
}
;


 array_expr_list:
 array_expr
 { 
 $$ = $1;
}
|  array_expr_list ',' array_expr
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 extract_list:
 extract_arg FROM a_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("from"),$3);
}
| 
 { 
 $$=EMPTY; }
;


 extract_arg:
 ecpg_ident
 { 
 $$ = $1;
}
|  YEAR_P
 { 
 $$ = mm_strdup("year");
}
|  MONTH_P
 { 
 $$ = mm_strdup("month");
}
|  DAY_P
 { 
 $$ = mm_strdup("day");
}
|  HOUR_P
 { 
 $$ = mm_strdup("hour");
}
|  MINUTE_P
 { 
 $$ = mm_strdup("minute");
}
|  SECOND_P
 { 
 $$ = mm_strdup("second");
}
|  ecpg_sconst
 { 
 $$ = $1;
}
;


 timestamp_arg_list:
 timestamp_units ',' a_expr ',' a_expr
 { 
 $$ = cat_str(5,$1,mm_strdup(","),$3,mm_strdup(","),$5);
}
| 
 { 
 $$=EMPTY; }
;


 timestamp_units:
 ecpg_ident
 { 
 $$ = $1;
}
|  YEAR_P
 { 
 $$ = mm_strdup("year");
}
|  MONTH_P
 { 
 $$ = mm_strdup("month");
}
|  DAY_P
 { 
 $$ = mm_strdup("day");
}
|  HOUR_P
 { 
 $$ = mm_strdup("hour");
}
|  MINUTE_P
 { 
 $$ = mm_strdup("minute");
}
|  SECOND_P
 { 
 $$ = mm_strdup("second");
}
|  ecpg_sconst
 { 
 $$ = $1;
}
;


 overlay_list:
 a_expr overlay_placing substr_from substr_for
 { 
 $$ = cat_str(4,$1,$2,$3,$4);
}
|  a_expr overlay_placing substr_from
 { 
 $$ = cat_str(3,$1,$2,$3);
}
;


 overlay_placing:
 PLACING a_expr
 { 
 $$ = cat_str(2,mm_strdup("placing"),$2);
}
;


 position_list:
 b_expr IN_P b_expr
 { 
 $$ = cat_str(3,$1,mm_strdup("in"),$3);
}
| 
 { 
 $$=EMPTY; }
;


 substr_list:
 a_expr substr_from substr_for
 { 
 $$ = cat_str(3,$1,$2,$3);
}
|  a_expr substr_for substr_from
 { 
 $$ = cat_str(3,$1,$2,$3);
}
|  a_expr substr_from
 { 
 $$ = cat_str(2,$1,$2);
}
|  a_expr substr_for
 { 
 $$ = cat_str(2,$1,$2);
}
|  expr_list
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 substr_from:
 FROM a_expr
 { 
 $$ = cat_str(2,mm_strdup("from"),$2);
}
;


 substr_for:
 FOR a_expr
 { 
 $$ = cat_str(2,mm_strdup("for"),$2);
}
;


 trim_list:
 a_expr FROM expr_list
 { 
 $$ = cat_str(3,$1,mm_strdup("from"),$3);
}
|  FROM expr_list
 { 
 $$ = cat_str(2,mm_strdup("from"),$2);
}
|  expr_list
 { 
 $$ = $1;
}
;


 in_expr:
 select_with_parens
 { 
 $$ = $1;
}
|  '(' expr_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
;


 case_expr:
 CASE case_arg when_clause_list case_default END_P
 { 
 $$ = cat_str(5,mm_strdup("case"),$2,$3,$4,mm_strdup("end"));
}
|  DECODE '(' a_expr ',' expr_list ')'
 { 
 $$ = cat_str(5,mm_strdup("decode ("),$3,mm_strdup(","),$5,mm_strdup(")"));
}
;


 when_clause_list:
 when_clause
 { 
 $$ = $1;
}
|  when_clause_list when_clause
 { 
 $$ = cat_str(2,$1,$2);
}
;


 when_clause:
 WHEN a_expr THEN a_expr
 { 
 $$ = cat_str(4,mm_strdup("when"),$2,mm_strdup("then"),$4);
}
;


 case_default:
 ELSE a_expr
 { 
 $$ = cat_str(2,mm_strdup("else"),$2);
}
| 
 { 
 $$=EMPTY; }
;


 case_arg:
 a_expr
 { 
 $$ = $1;
}
| 
 { 
 $$=EMPTY; }
;


 columnref:
 ColId
 { 
 $$ = $1;
}
|  ColId indirection
 { 
 $$ = cat_str(2,$1,$2);
}
|  EXCLUDED indirection
 { 
 $$ = cat_str(2,mm_strdup("excluded"),$2);
}
;


 indirection_el:
 '.' attr_name
 { 
 $$ = cat_str(2,mm_strdup("."),$2);
}
|  ORA_JOINOP
 { 
 $$ = mm_strdup("ora_joinop");
}
|  '.' '*'
 { 
 $$ = mm_strdup(". *");
}
|  '[' a_expr ']'
 { 
 $$ = cat_str(3,mm_strdup("["),$2,mm_strdup("]"));
}
|  '[' a_expr ':' a_expr ']'
 { 
 $$ = cat_str(5,mm_strdup("["),$2,mm_strdup(":"),$4,mm_strdup("]"));
}
|  '[' a_expr ',' a_expr ']'
 { 
 $$ = cat_str(5,mm_strdup("["),$2,mm_strdup(","),$4,mm_strdup("]"));
}
;


 indirection:
 indirection_el
 { 
 $$ = $1;
}
|  indirection indirection_el
 { 
 $$ = cat_str(2,$1,$2);
}
;


 opt_indirection:

 { 
 $$=EMPTY; }
|  opt_indirection indirection_el
 { 
 $$ = cat_str(2,$1,$2);
}
;


 opt_asymmetric:
 ASYMMETRIC
 { 
 $$ = mm_strdup("asymmetric");
}
| 
 { 
 $$=EMPTY; }
;


 ctext_expr:
 a_expr
 { 
 $$ = $1;
}
|  DEFAULT
 { 
 $$ = mm_strdup("default");
}
;


 ctext_expr_list:
 ctext_expr
 { 
 $$ = $1;
}
|  ctext_expr_list ',' ctext_expr
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 ctext_row:
 '(' ctext_expr_list ')'
 { 
 $$ = cat_str(3,mm_strdup("("),$2,mm_strdup(")"));
}
;


 target_list:
 target_el
 { 
 $$ = $1;
}
|  target_list ',' target_el
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 target_el:
 a_expr AS ColLabel
 { 
 $$ = cat_str(3,$1,mm_strdup("as"),$3);
}
|  a_expr ecpg_ident
 { 
 $$ = cat_str(2,$1,$2);
}
|  a_expr
 { 
 $$ = $1;
}
|  '*'
 { 
 $$ = mm_strdup("*");
}
|  c_expr VALUE_P
 { 
 $$ = cat_str(2,$1,mm_strdup("value"));
}
|  c_expr NAME_P
 { 
 $$ = cat_str(2,$1,mm_strdup("name"));
}
|  c_expr TYPE_P
 { 
 $$ = cat_str(2,$1,mm_strdup("type"));
}
|  connect_by_root_expr
 { 
 $$ = $1;
}
;


 connect_by_root_expr:
 a_expr ecpg_ident '.' ecpg_ident
 { 
 $$ = cat_str(4,$1,$2,mm_strdup("."),$4);
}
|  a_expr ecpg_ident '.' ecpg_ident as_empty ecpg_ident
 { 
 $$ = cat_str(6,$1,$2,mm_strdup("."),$4,$5,$6);
}
|  a_expr ecpg_ident as_empty ecpg_ident
 { 
 $$ = cat_str(4,$1,$2,$3,$4);
}
;


 qualified_name_list:
 qualified_name
 { 
 $$ = $1;
}
|  qualified_name_list ',' qualified_name
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 qualified_name:
 ColId
 { 
 $$ = $1;
}
|  ColId indirection
 { 
 $$ = cat_str(2,$1,$2);
}
;


 name_list:
 name
 { 
 $$ = $1;
}
|  name_list ',' name
 { 
 $$ = cat_str(3,$1,mm_strdup(","),$3);
}
;


 name:
 ColId
 { 
 $$ = $1;
}
;


 database_name:
 ColId
 { 
 $$ = $1;
}
;


 access_method:
 ColId
 { 
 $$ = $1;
}
;


 attr_name:
 ColLabel
 { 
 $$ = $1;
}
;


 index_name:
 ColId
 { 
 $$ = $1;
}
;


 file_name:
 ecpg_sconst
 { 
 $$ = $1;
}
;


 func_name:
 type_function_name
 { 
 $$ = $1;
}
|  ColId indirection
 { 
 $$ = cat_str(2,$1,$2);
}
;


 func_name_opt_arg:
 func_name
 { 
 $$ = $1;
}
	| ECPGKeywords BOGUS
	{ 
	 $$ = cat_str(2,$1,mm_strdup("bogus"));
	}
	| ECPGCKeywords BOGUS
	{ 
	 $$ = cat_str(2,$1,mm_strdup("bogus"));
	}
|  ecpg_ident BOGUS
 { 
 $$ = cat_str(2,$1,mm_strdup("bogus"));
}
| all_unreserved_keyword BOGUS
 { 
 $$ = cat_str(2,$1,mm_strdup("bogus"));
}
;


 AexprConst:
 Iconst
 { 
 $$ = $1;
}
|  ecpg_fconst
 { 
 $$ = $1;
}
|  ecpg_sconst
 { 
 $$ = $1;
}
|  ecpg_bconst
 { 
 $$ = $1;
}
|  XCONST
 { 
 $$ = mm_strdup("xconst");
}
|  func_name ecpg_sconst
 { 
 $$ = cat_str(2,$1,$2);
}
|  func_name '(' func_arg_list opt_sort_clause ')' ecpg_sconst
 { 
 $$ = cat_str(6,$1,mm_strdup("("),$3,$4,mm_strdup(")"),$6);
}
|  ConstTypename ecpg_sconst
 { 
 $$ = cat_str(2,$1,$2);
}
|  ConstInterval ecpg_sconst opt_interval
 { 
 $$ = cat_str(3,$1,$2,$3);
}
|  ConstInterval '(' Iconst ')' ecpg_sconst opt_interval
 { 
 $$ = cat_str(6,$1,mm_strdup("("),$3,mm_strdup(")"),$5,$6);
}
|  TRUE_P
 { 
 $$ = mm_strdup("true");
}
|  FALSE_P
 { 
 $$ = mm_strdup("false");
}
|  NULL_P
 { 
 $$ = mm_strdup("null");
}
	| civar			{ $$ = $1; }
	| civarind		{ $$ = $1; }
;


 Iconst:
 ICONST
	{ $$ = make_name(); }
;


 RoleId:
 ColId
 { 
 $$ = $1;
}
;


 SignedIconst:
 Iconst
 { 
 $$ = $1;
}
	| civar	{ $$ = $1; }
|  '+' Iconst
 { 
 $$ = cat_str(2,mm_strdup("+"),$2);
}
|  '-' Iconst
 { 
 $$ = cat_str(2,mm_strdup("-"),$2);
}
;


 DelimiterStmt:
 DELIMITER delimiter_str_names END_OF_INPUT
 { 
 $$ = cat_str(3,mm_strdup("delimiter"),$2,mm_strdup("end_of_input"));
}
|  DELIMITER delimiter_str_names END_OF_INPUT_COLON
 { 
 $$ = cat_str(3,mm_strdup("delimiter"),$2,mm_strdup("end_of_input_colon"));
}
;


 delimiter_str_names:
 delimiter_str_names delimiter_str_name
 { 
 $$ = cat_str(2,$1,$2);
}
|  delimiter_str_name
 { 
 $$ = $1;
}
;


 delimiter_str_name:
 ColId_or_Sconst
 { 
 $$ = $1;
}
|  all_Op
 { 
 $$ = $1;
}
|  ';'
 { 
 $$ = mm_strdup(";");
}
;


 unreserved_keyword:
 ABORT_P
 { 
 $$ = mm_strdup("abort");
}
|  ABSOLUTE_P
 { 
 $$ = mm_strdup("absolute");
}
|  ACCESS
 { 
 $$ = mm_strdup("access");
}
|  ACCOUNT
 { 
 $$ = mm_strdup("account");
}
|  ACTION
 { 
 $$ = mm_strdup("action");
}
|  ADD_P
 { 
 $$ = mm_strdup("add");
}
|  ADMIN
 { 
 $$ = mm_strdup("admin");
}
|  AFTER
 { 
 $$ = mm_strdup("after");
}
|  AGGREGATE
 { 
 $$ = mm_strdup("aggregate");
}
|  ALGORITHM
 { 
 $$ = mm_strdup("algorithm");
}
|  ALSO
 { 
 $$ = mm_strdup("also");
}
|  ALTER
 { 
 $$ = mm_strdup("alter");
}
|  ALWAYS
 { 
 $$ = mm_strdup("always");
}
|  APP
 { 
 $$ = mm_strdup("app");
}
|  APPEND
 { 
 $$ = mm_strdup("append");
}
|  ARCHIVE
 { 
 $$ = mm_strdup("archive");
}
|  ASSERTION
 { 
 $$ = mm_strdup("assertion");
}
|  ASSIGNMENT
 { 
 $$ = mm_strdup("assignment");
}
|  AT
 { 
 $$ = mm_strdup("at");
}
|  ATTRIBUTE
 { 
 $$ = mm_strdup("attribute");
}
|  AUDIT
 { 
 $$ = mm_strdup("audit");
}
|  AUTOEXTEND
 { 
 $$ = mm_strdup("autoextend");
}
|  AUTOMAPPED
 { 
 $$ = mm_strdup("automapped");
}
|  AUTO_INCREMENT
 { 
 $$ = mm_strdup("auto_increment");
}
|  BACKWARD
 { 
 $$ = mm_strdup("backward");
}
|  BARRIER
 { 
 $$ = mm_strdup("barrier");
}
|  BEFORE
 { 
 $$ = mm_strdup("before");
}
|  BEGIN_P
 { 
 $$ = mm_strdup("begin");
}
|  BEGIN_NON_ANOYBLOCK
 { 
 $$ = mm_strdup("begin_non_anoyblock");
}
|  BLANKS
 { 
 $$ = mm_strdup("blanks");
}
|  BLOB_P
 { 
 $$ = mm_strdup("blob");
}
|  BLOCKCHAIN
 { 
 $$ = mm_strdup("blockchain");
}
|  BODY_P
 { 
 $$ = mm_strdup("body");
}
|  BY
 { 
 $$ = mm_strdup("by");
}
|  CACHE
 { 
 $$ = mm_strdup("cache");
}
|  CALL
 { 
 $$ = mm_strdup("call");
}
|  CALLED
 { 
 $$ = mm_strdup("called");
}
|  CANCELABLE
 { 
 $$ = mm_strdup("cancelable");
}
|  CASCADE
 { 
 $$ = mm_strdup("cascade");
}
|  CASCADED
 { 
 $$ = mm_strdup("cascaded");
}
|  CATALOG_P
 { 
 $$ = mm_strdup("catalog");
}
|  CHAIN
 { 
 $$ = mm_strdup("chain");
}
|  CHANGE
 { 
 $$ = mm_strdup("change");
}
|  CHARACTERISTICS
 { 
 $$ = mm_strdup("characteristics");
}
|  CHARACTERSET
 { 
 $$ = mm_strdup("characterset");
}
|  CHECKPOINT
 { 
 $$ = mm_strdup("checkpoint");
}
|  CLASS
 { 
 $$ = mm_strdup("class");
}
|  CLEAN
 { 
 $$ = mm_strdup("clean");
}
|  CLIENT
 { 
 $$ = mm_strdup("client");
}
|  CLIENT_MASTER_KEY
 { 
 $$ = mm_strdup("client_master_key");
}
|  CLIENT_MASTER_KEYS
 { 
 $$ = mm_strdup("client_master_keys");
}
|  CLOB
 { 
 $$ = mm_strdup("clob");
}
|  CLOSE
 { 
 $$ = mm_strdup("close");
}
|  CLUSTER
 { 
 $$ = mm_strdup("cluster");
}
|  COLUMN_ENCRYPTION_KEY
 { 
 $$ = mm_strdup("column_encryption_key");
}
|  COLUMN_ENCRYPTION_KEYS
 { 
 $$ = mm_strdup("column_encryption_keys");
}
|  COLUMNS
 { 
 $$ = mm_strdup("columns");
}
|  COMMENT
 { 
 $$ = mm_strdup("comment");
}
|  COMMENTS
 { 
 $$ = mm_strdup("comments");
}
|  COMMIT
 { 
 $$ = mm_strdup("commit");
}
|  COMMITTED
 { 
 $$ = mm_strdup("committed");
}
|  COMPATIBLE_ILLEGAL_CHARS
 { 
 $$ = mm_strdup("compatible_illegal_chars");
}
|  COMPLETE
 { 
 $$ = mm_strdup("complete");
}
|  COMPLETION
 { 
 $$ = mm_strdup("completion");
}
|  COMPRESS
 { 
 $$ = mm_strdup("compress");
}
|  CONDITION
 { 
 $$ = mm_strdup("condition");
}
|  CONFIGURATION
 { 
 $$ = mm_strdup("configuration");
}
|  CONNECT
 { 
 $$ = mm_strdup("connect");
}
|  CONSTANT
 { 
 $$ = mm_strdup("constant");
}
|  CONSTRAINTS
 { 
 $$ = mm_strdup("constraints");
}
|  CONTENT_P
 { 
 $$ = mm_strdup("content");
}
|  CONTINUE_P
 { 
 $$ = mm_strdup("continue");
}
|  CONTVIEW
 { 
 $$ = mm_strdup("contview");
}
|  CONVERSION_P
 { 
 $$ = mm_strdup("conversion");
}
|  CONVERT_P
 { 
 $$ = mm_strdup("convert");
}
|  COORDINATOR
 { 
 $$ = mm_strdup("coordinator");
}
|  COORDINATORS
 { 
 $$ = mm_strdup("coordinators");
}
|  COPY
 { 
 $$ = mm_strdup("copy");
}
|  COST
 { 
 $$ = mm_strdup("cost");
}
|  CSV
 { 
 $$ = mm_strdup("csv");
}
|  CUBE
 { 
 $$ = mm_strdup("cube");
}
|  CURSOR
 { 
 $$ = mm_strdup("cursor");
}
|  CYCLE
 { 
 $$ = mm_strdup("cycle");
}
|  DATA_P
 { 
 $$ = mm_strdup("data");
}
|  DATABASE
 { 
 $$ = mm_strdup("database");
}
|  DATAFILE
 { 
 $$ = mm_strdup("datafile");
}
|  DATANODE
 { 
 $$ = mm_strdup("datanode");
}
|  DATANODES
 { 
 $$ = mm_strdup("datanodes");
}
|  DATATYPE_CL
 { 
 $$ = mm_strdup("datatype_cl");
}
|  DATE_FORMAT_P
 { 
 $$ = mm_strdup("date_format");
}
|  DBCOMPATIBILITY_P
 { 
 $$ = mm_strdup("dbcompatibility");
}
|  DEALLOCATE
 { 
 $$ = mm_strdup("deallocate");
}
|  DECLARE
 { 
 $$ = mm_strdup("declare");
}
|  DEFAULTS
 { 
 $$ = mm_strdup("defaults");
}
|  DEFERRED
 { 
 $$ = mm_strdup("deferred");
}
|  DEFINER
 { 
 $$ = mm_strdup("definer");
}
|  DELETE_P
 { 
 $$ = mm_strdup("delete");
}
|  DELIMITER
 { 
 $$ = mm_strdup("delimiter");
}
|  DELIMITERS
 { 
 $$ = mm_strdup("delimiters");
}
|  DELTA
 { 
 $$ = mm_strdup("delta");
}
|  DETERMINISTIC
 { 
 $$ = mm_strdup("deterministic");
}
|  DICTIONARY
 { 
 $$ = mm_strdup("dictionary");
}
|  DIRECT
 { 
 $$ = mm_strdup("direct");
}
|  DIRECTORY
 { 
 $$ = mm_strdup("directory");
}
|  DISABLE_P
 { 
 $$ = mm_strdup("disable");
}
|  DISCARD
 { 
 $$ = mm_strdup("discard");
}
|  DISCONNECT
 { 
 $$ = mm_strdup("disconnect");
}
|  DISTRIBUTE
 { 
 $$ = mm_strdup("distribute");
}
|  DISTRIBUTION
 { 
 $$ = mm_strdup("distribution");
}
|  DOCUMENT_P
 { 
 $$ = mm_strdup("document");
}
|  DOMAIN_P
 { 
 $$ = mm_strdup("domain");
}
|  DOUBLE_P
 { 
 $$ = mm_strdup("double");
}
|  DROP
 { 
 $$ = mm_strdup("drop");
}
|  DUMPFILE
 { 
 $$ = mm_strdup("dumpfile");
}
|  DUPLICATE
 { 
 $$ = mm_strdup("duplicate");
}
|  EACH
 { 
 $$ = mm_strdup("each");
}
|  ELASTIC
 { 
 $$ = mm_strdup("elastic");
}
|  ENABLE_P
 { 
 $$ = mm_strdup("enable");
}
|  ENCLOSED
 { 
 $$ = mm_strdup("enclosed");
}
|  ENCODING
 { 
 $$ = mm_strdup("encoding");
}
|  ENCRYPTED
 { 
 $$ = mm_strdup("encrypted");
}
|  ENCRYPTED_VALUE
 { 
 $$ = mm_strdup("encrypted_value");
}
|  ENCRYPTION
 { 
 $$ = mm_strdup("encryption");
}
|  ENCRYPTION_TYPE
 { 
 $$ = mm_strdup("encryption_type");
}
|  ENDS
 { 
 $$ = mm_strdup("ends");
}
|  ENFORCED
 { 
 $$ = mm_strdup("enforced");
}
|  ENUM_P
 { 
 $$ = mm_strdup("enum");
}
|  EOL
 { 
 $$ = mm_strdup("eol");
}
|  ERRORS
 { 
 $$ = mm_strdup("errors");
}
|  ESCAPE
 { 
 $$ = mm_strdup("escape");
}
|  ESCAPED
 { 
 $$ = mm_strdup("escaped");
}
|  ESCAPING
 { 
 $$ = mm_strdup("escaping");
}
|  EVENT
 { 
 $$ = mm_strdup("event");
}
|  EVENTS
 { 
 $$ = mm_strdup("events");
}
|  EVERY
 { 
 $$ = mm_strdup("every");
}
|  EXCHANGE
 { 
 $$ = mm_strdup("exchange");
}
|  EXCLUDE
 { 
 $$ = mm_strdup("exclude");
}
|  EXCLUDING
 { 
 $$ = mm_strdup("excluding");
}
|  EXCLUSIVE
 { 
 $$ = mm_strdup("exclusive");
}
|  EXECUTE
 { 
 $$ = mm_strdup("execute");
}
|  EXPIRED_P
 { 
 $$ = mm_strdup("expired");
}
|  EXPLAIN
 { 
 $$ = mm_strdup("explain");
}
|  EXTENSION
 { 
 $$ = mm_strdup("extension");
}
|  EXTERNAL
 { 
 $$ = mm_strdup("external");
}
|  FAMILY
 { 
 $$ = mm_strdup("family");
}
|  FAST
 { 
 $$ = mm_strdup("fast");
}
|  FEATURES
 { 
 $$ = mm_strdup("features");
}
|  FENCED
 { 
 $$ = mm_strdup("fenced");
}
|  FIELDS
 { 
 $$ = mm_strdup("fields");
}
|  FILEHEADER_P
 { 
 $$ = mm_strdup("fileheader");
}
|  FILLER
 { 
 $$ = mm_strdup("filler");
}
|  FILL_MISSING_FIELDS
 { 
 $$ = mm_strdup("fill_missing_fields");
}
|  FILTER
 { 
 $$ = mm_strdup("filter");
}
|  FIRST_P
 { 
 $$ = mm_strdup("first");
}
|  FIXED_P
 { 
 $$ = mm_strdup("fixed");
}
|  FOLLOWING
 { 
 $$ = mm_strdup("following");
}
|  FOLLOWS_P
 { 
 $$ = mm_strdup("follows");
}
|  FORCE
 { 
 $$ = mm_strdup("force");
}
|  FORMATTER
 { 
 $$ = mm_strdup("formatter");
}
|  FORWARD
 { 
 $$ = mm_strdup("forward");
}
|  FUNCTION
 { 
 $$ = mm_strdup("function");
}
|  FUNCTIONS
 { 
 $$ = mm_strdup("functions");
}
|  GENERATED
 { 
 $$ = mm_strdup("generated");
}
|  GLOBAL
 { 
 $$ = mm_strdup("global");
}
|  GRANTED
 { 
 $$ = mm_strdup("granted");
}
|  HANDLER
 { 
 $$ = mm_strdup("handler");
}
|  HEADER_P
 { 
 $$ = mm_strdup("header");
}
|  HOLD
 { 
 $$ = mm_strdup("hold");
}
|  IDENTIFIED
 { 
 $$ = mm_strdup("identified");
}
|  IDENTITY_P
 { 
 $$ = mm_strdup("identity");
}
|  IF_P
 { 
 $$ = mm_strdup("if");
}
|  IGNORE
 { 
 $$ = mm_strdup("ignore");
}
|  IGNORE_EXTRA_DATA
 { 
 $$ = mm_strdup("ignore_extra_data");
}
|  IMMEDIATE
 { 
 $$ = mm_strdup("immediate");
}
|  IMMUTABLE
 { 
 $$ = mm_strdup("immutable");
}
|  IMPLICIT_P
 { 
 $$ = mm_strdup("implicit");
}
|  INCLUDE
 { 
 $$ = mm_strdup("include");
}
|  INCLUDING
 { 
 $$ = mm_strdup("including");
}
|  INCREMENT
 { 
 $$ = mm_strdup("increment");
}
|  INCREMENTAL
 { 
 $$ = mm_strdup("incremental");
}
|  INDEX
 { 
 $$ = mm_strdup("index");
}
|  INDEXES
 { 
 $$ = mm_strdup("indexes");
}
|  INFILE
 { 
 $$ = mm_strdup("infile");
}
|  INHERIT
 { 
 $$ = mm_strdup("inherit");
}
|  INHERITS
 { 
 $$ = mm_strdup("inherits");
}
|  INITIAL_P
 { 
 $$ = mm_strdup("initial");
}
|  INITRANS
 { 
 $$ = mm_strdup("initrans");
}
|  INLINE_P
 { 
 $$ = mm_strdup("inline");
}
|  INSENSITIVE
 { 
 $$ = mm_strdup("insensitive");
}
|  INSERT
 { 
 $$ = mm_strdup("insert");
}
|  INSTEAD
 { 
 $$ = mm_strdup("instead");
}
|  INTERNAL
 { 
 $$ = mm_strdup("internal");
}
|  INVISIBLE
 { 
 $$ = mm_strdup("invisible");
}
|  INVOKER
 { 
 $$ = mm_strdup("invoker");
}
|  IP
 { 
 $$ = mm_strdup("ip");
}
|  ISNULL
 { 
 $$ = mm_strdup("isnull");
}
|  ISOLATION
 { 
 $$ = mm_strdup("isolation");
}
|  KEY
 { 
 $$ = mm_strdup("key");
}
|  KEY_PATH
 { 
 $$ = mm_strdup("keyath");
}
|  KEY_STORE
 { 
 $$ = mm_strdup("key_store");
}
|  KILL
 { 
 $$ = mm_strdup("kill");
}
|  LABEL
 { 
 $$ = mm_strdup("label");
}
|  LANGUAGE
 { 
 $$ = mm_strdup("language");
}
|  LARGE_P
 { 
 $$ = mm_strdup("large");
}
|  LAST_P
 { 
 $$ = mm_strdup("last");
}
|  LC_COLLATE_P
 { 
 $$ = mm_strdup("lc_collate");
}
|  LC_CTYPE_P
 { 
 $$ = mm_strdup("lc_ctype");
}
|  LEAKPROOF
 { 
 $$ = mm_strdup("leakproof");
}
|  LEVEL
 { 
 $$ = mm_strdup("level");
}
|  LINES
 { 
 $$ = mm_strdup("lines");
}
|  LIST
 { 
 $$ = mm_strdup("list");
}
|  LISTEN
 { 
 $$ = mm_strdup("listen");
}
|  LOAD
 { 
 $$ = mm_strdup("load");
}
|  LOCAL
 { 
 $$ = mm_strdup("local");
}
|  LOCATION
 { 
 $$ = mm_strdup("location");
}
|  LOCK_P
 { 
 $$ = mm_strdup("lock");
}
|  LOCKED
 { 
 $$ = mm_strdup("locked");
}
|  LOG_P
 { 
 $$ = mm_strdup("log");
}
|  LOGGING
 { 
 $$ = mm_strdup("logging");
}
|  LOGIN_ANY
 { 
 $$ = mm_strdup("login_any");
}
|  LOGIN_FAILURE
 { 
 $$ = mm_strdup("login_failure");
}
|  LOGIN_SUCCESS
 { 
 $$ = mm_strdup("login_success");
}
|  LOGOUT
 { 
 $$ = mm_strdup("logout");
}
|  LOOP
 { 
 $$ = mm_strdup("loop");
}
|  MAPPING
 { 
 $$ = mm_strdup("mapping");
}
|  MASKING
 { 
 $$ = mm_strdup("masking");
}
|  MASTER
 { 
 $$ = mm_strdup("master");
}
|  MATCH
 { 
 $$ = mm_strdup("match");
}
|  MATCHED
 { 
 $$ = mm_strdup("matched");
}
|  MATERIALIZED
 { 
 $$ = mm_strdup("materialized");
}
|  MAXEXTENTS
 { 
 $$ = mm_strdup("maxextents");
}
|  MAXSIZE
 { 
 $$ = mm_strdup("maxsize");
}
|  MAXTRANS
 { 
 $$ = mm_strdup("maxtrans");
}
|  MERGE
 { 
 $$ = mm_strdup("merge");
}
|  MINEXTENTS
 { 
 $$ = mm_strdup("minextents");
}
|  MINVALUE
 { 
 $$ = mm_strdup("minvalue");
}
|  MODE
 { 
 $$ = mm_strdup("mode");
}
|  MODEL
 { 
 $$ = mm_strdup("model");
}
|  MOVE
 { 
 $$ = mm_strdup("move");
}
|  MOVEMENT
 { 
 $$ = mm_strdup("movement");
}
|  NAME_P
 { 
 $$ = mm_strdup("name");
}
|  NAMES
 { 
 $$ = mm_strdup("names");
}
|  NEXT
 { 
 $$ = mm_strdup("next");
}
|  NO
 { 
 $$ = mm_strdup("no");
}
|  NOCOMPRESS
 { 
 $$ = mm_strdup("nocompress");
}
|  NODE
 { 
 $$ = mm_strdup("node");
}
|  NOLOGGING
 { 
 $$ = mm_strdup("nologging");
}
|  NOMAXVALUE
 { 
 $$ = mm_strdup("nomaxvalue");
}
|  NOMINVALUE
 { 
 $$ = mm_strdup("nominvalue");
}
|  NOTHING
 { 
 $$ = mm_strdup("nothing");
}
|  NOTIFY
 { 
 $$ = mm_strdup("notify");
}
|  NOWAIT
 { 
 $$ = mm_strdup("nowait");
}
|  NULLCOLS
 { 
 $$ = mm_strdup("nullcols");
}
|  NULLS_P
 { 
 $$ = mm_strdup("nulls");
}
|  NUMSTR
 { 
 $$ = mm_strdup("numstr");
}
|  OBJECT_P
 { 
 $$ = mm_strdup("object");
}
|  OF
 { 
 $$ = mm_strdup("of");
}
|  OFF
 { 
 $$ = mm_strdup("off");
}
|  OIDS
 { 
 $$ = mm_strdup("oids");
}
|  OPERATOR
 { 
 $$ = mm_strdup("operator");
}
|  OPTIMIZATION
 { 
 $$ = mm_strdup("optimization");
}
|  OPTION
 { 
 $$ = mm_strdup("option");
}
|  OPTIONALLY
 { 
 $$ = mm_strdup("optionally");
}
|  OPTIONS
 { 
 $$ = mm_strdup("options");
}
|  OVER
 { 
 $$ = mm_strdup("over");
}
|  OUTFILE
 { 
 $$ = mm_strdup("outfile");
}
|  OWNED
 { 
 $$ = mm_strdup("owned");
}
|  OWNER
 { 
 $$ = mm_strdup("owner");
}
|  PACKAGE
 { 
 $$ = mm_strdup("package");
}
|  PACKAGES
 { 
 $$ = mm_strdup("packages");
}
|  PARSER
 { 
 $$ = mm_strdup("parser");
}
|  PARTIAL %prec PARTIAL_EMPTY_PREC
 { 
 $$ = mm_strdup("partial");
}
|  PARTITION
 { 
 $$ = mm_strdup("partition");
}
|  PARTITIONS
 { 
 $$ = mm_strdup("partitions");
}
|  PASSING
 { 
 $$ = mm_strdup("passing");
}
|  PASSWORD
 { 
 $$ = mm_strdup("password");
}
|  PCTFREE
 { 
 $$ = mm_strdup("pctfree");
}
|  PER_P
 { 
 $$ = mm_strdup("per");
}
|  PERCENT
 { 
 $$ = mm_strdup("percent");
}
|  PERM
 { 
 $$ = mm_strdup("perm");
}
|  PLAN
 { 
 $$ = mm_strdup("plan");
}
|  PLANS
 { 
 $$ = mm_strdup("plans");
}
|  POLICY
 { 
 $$ = mm_strdup("policy");
}
|  POOL
 { 
 $$ = mm_strdup("pool");
}
|  PRECEDES_P
 { 
 $$ = mm_strdup("precedes");
}
|  PRECEDING
 { 
 $$ = mm_strdup("preceding");
}
|  PREDICT
 { 
 $$ = mm_strdup("predict");
}
|  PREFERRED
 { 
 $$ = mm_strdup("preferred");
}
|  PREFIX
 { 
 $$ = mm_strdup("prefix");
}
|  PREPARE
 { 
 $$ = mm_strdup("prepare");
}
|  PREPARED
 { 
 $$ = mm_strdup("prepared");
}
|  PRESERVE
 { 
 $$ = mm_strdup("preserve");
}
|  PRIOR
 { 
 $$ = mm_strdup("prior");
}
|  PRIORER
 { 
 $$ = mm_strdup("priorer");
}
|  PRIVATE
 { 
 $$ = mm_strdup("private");
}
|  PRIVILEGE
 { 
 $$ = mm_strdup("privilege");
}
|  PRIVILEGES
 { 
 $$ = mm_strdup("privileges");
}
|  PROCEDURAL
 { 
 $$ = mm_strdup("procedural");
}
|  PROFILE
 { 
 $$ = mm_strdup("profile");
}
|  PUBLICATION
 { 
 $$ = mm_strdup("publication");
}
|  PUBLISH
 { 
 $$ = mm_strdup("publish");
}
|  PURGE
 { 
 $$ = mm_strdup("purge");
}
|  QUERY
 { 
 $$ = mm_strdup("query");
}
|  QUOTE
 { 
 $$ = mm_strdup("quote");
}
|  RANDOMIZED
 { 
 $$ = mm_strdup("randomized");
}
|  RANGE
 { 
 $$ = mm_strdup("range");
}
|  RATIO
 { 
 $$ = mm_strdup("ratio");
}
|  RAW '(' Iconst ')'
 { 
 $$ = cat_str(3,mm_strdup("raw ("),$3,mm_strdup(")"));
}
|  RAW %prec UNION
 { 
 $$ = mm_strdup("raw");
}
|  READ
 { 
 $$ = mm_strdup("read");
}
|  REASSIGN
 { 
 $$ = mm_strdup("reassign");
}
|  REBUILD
 { 
 $$ = mm_strdup("rebuild");
}
|  RECHECK
 { 
 $$ = mm_strdup("recheck");
}
|  RECURSIVE
 { 
 $$ = mm_strdup("recursive");
}
|  REDISANYVALUE
 { 
 $$ = mm_strdup("redisanyvalue");
}
|  REF
 { 
 $$ = mm_strdup("ref");
}
|  REFRESH
 { 
 $$ = mm_strdup("refresh");
}
|  REINDEX
 { 
 $$ = mm_strdup("reindex");
}
|  RELATIVE_P
 { 
 $$ = mm_strdup("relative");
}
|  RELEASE
 { 
 $$ = mm_strdup("release");
}
|  RELOPTIONS
 { 
 $$ = mm_strdup("reloptions");
}
|  REMOTE_P
 { 
 $$ = mm_strdup("remote");
}
|  REMOVE
 { 
 $$ = mm_strdup("remove");
}
|  RENAME
 { 
 $$ = mm_strdup("rename");
}
|  REPEAT
 { 
 $$ = mm_strdup("repeat");
}
|  REPEATABLE
 { 
 $$ = mm_strdup("repeatable");
}
|  REPLACE
 { 
 $$ = mm_strdup("replace");
}
|  REPLICA
 { 
 $$ = mm_strdup("replica");
}
|  RESET
 { 
 $$ = mm_strdup("reset");
}
|  RESIZE
 { 
 $$ = mm_strdup("resize");
}
|  RESOURCE
 { 
 $$ = mm_strdup("resource");
}
|  RESTART
 { 
 $$ = mm_strdup("restart");
}
|  RESTRICT
 { 
 $$ = mm_strdup("restrict");
}
|  RETURN
 { 
 $$ = mm_strdup("return");
}
|  RETURNS
 { 
 $$ = mm_strdup("returns");
}
|  REUSE
 { 
 $$ = mm_strdup("reuse");
}
|  REVOKE
 { 
 $$ = mm_strdup("revoke");
}
|  ROLE
 { 
 $$ = mm_strdup("role");
}
|  ROLES
 { 
 $$ = mm_strdup("roles");
}
|  ROLLBACK
 { 
 $$ = mm_strdup("rollback");
}
|  ROLLUP
 { 
 $$ = mm_strdup("rollup");
}
|  ROTATION
 { 
 $$ = mm_strdup("rotation");
}
|  ROWS
 { 
 $$ = mm_strdup("rows");
}
|  ROWTYPE_P
 { 
 $$ = mm_strdup("rowtype");
}
|  RULE
 { 
 $$ = mm_strdup("rule");
}
|  SAMPLE
 { 
 $$ = mm_strdup("sample");
}
|  SAVEPOINT
 { 
 $$ = mm_strdup("savepoint");
}
|  SCHEDULE
 { 
 $$ = mm_strdup("schedule");
}
|  SCHEMA
 { 
 $$ = mm_strdup("schema");
}
|  SCROLL
 { 
 $$ = mm_strdup("scroll");
}
|  SEARCH
 { 
 $$ = mm_strdup("search");
}
|  SECURITY
 { 
 $$ = mm_strdup("security");
}
|  SEPARATOR_P
 { 
 $$ = mm_strdup("separator");
}
|  SEQUENCE
 { 
 $$ = mm_strdup("sequence");
}
|  SEQUENCES
 { 
 $$ = mm_strdup("sequences");
}
|  SERIALIZABLE
 { 
 $$ = mm_strdup("serializable");
}
|  SERVER
 { 
 $$ = mm_strdup("server");
}
|  SESSION
 { 
 $$ = mm_strdup("session");
}
|  SET
 { 
 $$ = mm_strdup("set");
}
|  SETS
 { 
 $$ = mm_strdup("sets");
}
|  SHARE
 { 
 $$ = mm_strdup("share");
}
|  SHIPPABLE
 { 
 $$ = mm_strdup("shippable");
}
|  SHOW
 { 
 $$ = mm_strdup("show");
}
|  SHUTDOWN
 { 
 $$ = mm_strdup("shutdown");
}
|  SIBLINGS
 { 
 $$ = mm_strdup("siblings");
}
|  SIMPLE
 { 
 $$ = mm_strdup("simple");
}
|  SIZE
 { 
 $$ = mm_strdup("size");
}
|  SKIP
 { 
 $$ = mm_strdup("skip");
}
|  SLAVE
 { 
 $$ = mm_strdup("slave");
}
|  SLICE
 { 
 $$ = mm_strdup("slice");
}
|  SMALLDATETIME_FORMAT_P
 { 
 $$ = mm_strdup("smalldatetime_format");
}
|  SNAPSHOT
 { 
 $$ = mm_strdup("snapshot");
}
|  SOURCE_P
 { 
 $$ = mm_strdup("source");
}
|  SPACE
 { 
 $$ = mm_strdup("space");
}
|  SPILL
 { 
 $$ = mm_strdup("spill");
}
|  SPLIT
 { 
 $$ = mm_strdup("split");
}
|  STABLE
 { 
 $$ = mm_strdup("stable");
}
|  STANDALONE_P
 { 
 $$ = mm_strdup("standalone");
}
|  START
 { 
 $$ = mm_strdup("start");
}
|  STARTING
 { 
 $$ = mm_strdup("starting");
}
|  STARTS
 { 
 $$ = mm_strdup("starts");
}
|  STATEMENT
 { 
 $$ = mm_strdup("statement");
}
|  STATEMENT_ID
 { 
 $$ = mm_strdup("statement_id");
}
|  STATISTICS
 { 
 $$ = mm_strdup("statistics");
}
|  STDIN
 { 
 $$ = mm_strdup("stdin");
}
|  STDOUT
 { 
 $$ = mm_strdup("stdout");
}
|  STORAGE
 { 
 $$ = mm_strdup("storage");
}
|  STORE_P
 { 
 $$ = mm_strdup("store");
}
|  STORED
 { 
 $$ = mm_strdup("stored");
}
|  STRATIFY
 { 
 $$ = mm_strdup("stratify");
}
|  STREAM
 { 
 $$ = mm_strdup("stream");
}
|  STRICT_P
 { 
 $$ = mm_strdup("strict");
}
|  STRIP_P
 { 
 $$ = mm_strdup("strip");
}
|  SUBPARTITION
 { 
 $$ = mm_strdup("subpartition");
}
|  SUBPARTITIONS
 { 
 $$ = mm_strdup("subpartitions");
}
|  SUBSCRIPTION
 { 
 $$ = mm_strdup("subscription");
}
|  SYNONYM
 { 
 $$ = mm_strdup("synonym");
}
|  SYSID
 { 
 $$ = mm_strdup("sysid");
}
|  SYS_REFCURSOR
 { 
 $$ = mm_strdup("sys_refcursor");
}
|  SYSTEM_P
 { 
 $$ = mm_strdup("system");
}
|  TABLES
 { 
 $$ = mm_strdup("tables");
}
|  TABLESPACE
 { 
 $$ = mm_strdup("tablespace");
}
|  TARGET
 { 
 $$ = mm_strdup("target");
}
|  TEMP
 { 
 $$ = mm_strdup("temp");
}
|  TEMPLATE
 { 
 $$ = mm_strdup("template");
}
|  TEMPORARY
 { 
 $$ = mm_strdup("temporary");
}
|  TERMINATED
 { 
 $$ = mm_strdup("terminated");
}
|  TEXT_P
 { 
 $$ = mm_strdup("text");
}
|  THAN
 { 
 $$ = mm_strdup("than");
}
|  TIMESTAMP_FORMAT_P
 { 
 $$ = mm_strdup("timestamp_format");
}
|  TIME_FORMAT_P
 { 
 $$ = mm_strdup("time_format");
}
|  TRANSACTION
 { 
 $$ = mm_strdup("transaction");
}
|  TRANSFORM
 { 
 $$ = mm_strdup("transform");
}
|  TRIGGER
 { 
 $$ = mm_strdup("trigger");
}
|  TRUNCATE
 { 
 $$ = mm_strdup("truncate");
}
|  TRUSTED
 { 
 $$ = mm_strdup("trusted");
}
|  TSFIELD
 { 
 $$ = mm_strdup("tsfield");
}
|  TSTAG
 { 
 $$ = mm_strdup("tstag");
}
|  TSTIME
 { 
 $$ = mm_strdup("tstime");
}
|  TYPE_P
 { 
 $$ = mm_strdup("type");
}
|  TYPES_P
 { 
 $$ = mm_strdup("types");
}
|  UNBOUNDED
 { 
 $$ = mm_strdup("unbounded");
}
|  UNCOMMITTED
 { 
 $$ = mm_strdup("uncommitted");
}
|  UNENCRYPTED
 { 
 $$ = mm_strdup("unencrypted");
}
|  UNKNOWN
 { 
 $$ = mm_strdup("unknown");
}
|  UNLIMITED
 { 
 $$ = mm_strdup("unlimited");
}
|  UNLISTEN
 { 
 $$ = mm_strdup("unlisten");
}
|  UNLOCK
 { 
 $$ = mm_strdup("unlock");
}
|  UNLOGGED
 { 
 $$ = mm_strdup("unlogged");
}
|  UNTIL
 { 
 $$ = mm_strdup("until");
}
|  UNUSABLE
 { 
 $$ = mm_strdup("unusable");
}
|  UPDATE
 { 
 $$ = mm_strdup("update");
}
|  USE_P
 { 
 $$ = mm_strdup("use");
}
|  USEEOF
 { 
 $$ = mm_strdup("useeof");
}
|  VACUUM
 { 
 $$ = mm_strdup("vacuum");
}
|  VALID
 { 
 $$ = mm_strdup("valid");
}
|  VALIDATE
 { 
 $$ = mm_strdup("validate");
}
|  VALIDATION
 { 
 $$ = mm_strdup("validation");
}
|  VALIDATOR
 { 
 $$ = mm_strdup("validator");
}
|  VALUE_P
 { 
 $$ = mm_strdup("value");
}
|  VARIABLES
 { 
 $$ = mm_strdup("variables");
}
|  VARYING
 { 
 $$ = mm_strdup("varying");
}
|  VCGROUP
 { 
 $$ = mm_strdup("vcgroup");
}
|  VERSION_P
 { 
 $$ = mm_strdup("version");
}
|  VIEW
 { 
 $$ = mm_strdup("view");
}
|  VISIBLE
 { 
 $$ = mm_strdup("visible");
}
|  VOLATILE
 { 
 $$ = mm_strdup("volatile");
}
|  WAIT
 { 
 $$ = mm_strdup("wait");
}
|  WEAK
 { 
 $$ = mm_strdup("weak");
}
|  WHILE_P
 { 
 $$ = mm_strdup("while");
}
|  WHITESPACE_P
 { 
 $$ = mm_strdup("whitespace");
}
|  WITHIN
 { 
 $$ = mm_strdup("within");
}
|  WITHOUT
 { 
 $$ = mm_strdup("without");
}
|  WORK
 { 
 $$ = mm_strdup("work");
}
|  WORKLOAD
 { 
 $$ = mm_strdup("workload");
}
|  WRAPPER
 { 
 $$ = mm_strdup("wrapper");
}
|  WRITE
 { 
 $$ = mm_strdup("write");
}
|  XML_P
 { 
 $$ = mm_strdup("xml");
}
|  YES_P
 { 
 $$ = mm_strdup("yes");
}
|  ZONE
 { 
 $$ = mm_strdup("zone");
}
;


 col_name_keyword:
 BETWEEN
 { 
 $$ = mm_strdup("between");
}
|  BIGINT
 { 
 $$ = mm_strdup("bigint");
}
|  BINARY_DOUBLE
 { 
 $$ = mm_strdup("binary_double");
}
|  BINARY_INTEGER
 { 
 $$ = mm_strdup("binary_integer");
}
|  BIT
 { 
 $$ = mm_strdup("bit");
}
|  BOOLEAN_P
 { 
 $$ = mm_strdup("boolean");
}
|  BUCKETCNT
 { 
 $$ = mm_strdup("bucketcnt");
}
|  BYTEAWITHOUTORDER
 { 
 $$ = mm_strdup("byteawithoutorder");
}
|  BYTEAWITHOUTORDERWITHEQUAL
 { 
 $$ = mm_strdup("byteawithoutorderwithequal");
}
|  CHARACTER
 { 
 $$ = mm_strdup("character");
}
|  COALESCE
 { 
 $$ = mm_strdup("coalesce");
}
|  DATE_P
 { 
 $$ = mm_strdup("date");
}
|  DEC
 { 
 $$ = mm_strdup("dec");
}
|  DECIMAL_P
 { 
 $$ = mm_strdup("decimal");
}
|  DECODE
 { 
 $$ = mm_strdup("decode");
}
|  EXISTS
 { 
 $$ = mm_strdup("exists");
}
|  EXTRACT
 { 
 $$ = mm_strdup("extract");
}
|  FLOAT_P
 { 
 $$ = mm_strdup("float");
}
|  GREATEST
 { 
 $$ = mm_strdup("greatest");
}
|  GROUPING_P
 { 
 $$ = mm_strdup("grouping");
}
|  INOUT
 { 
 $$ = mm_strdup("inout");
}
|  INTEGER
 { 
 $$ = mm_strdup("integer");
}
|  INTERVAL
 { 
 $$ = mm_strdup("interval");
}
|  LEAST
 { 
 $$ = mm_strdup("least");
}
|  NATIONAL
 { 
 $$ = mm_strdup("national");
}
|  NCHAR
 { 
 $$ = mm_strdup("nchar");
}
|  NONE
 { 
 $$ = mm_strdup("none");
}
|  NULLIF
 { 
 $$ = mm_strdup("nullif");
}
|  NUMBER_P
 { 
 $$ = mm_strdup("number");
}
|  NUMERIC
 { 
 $$ = mm_strdup("numeric");
}
|  NVARCHAR
 { 
 $$ = mm_strdup("nvarchar");
}
|  NVARCHAR2
 { 
 $$ = mm_strdup("nvarchar2");
}
|  NVL
 { 
 $$ = mm_strdup("nvl");
}
|  OUT_P
 { 
 $$ = mm_strdup("out");
}
|  OVERLAY
 { 
 $$ = mm_strdup("overlay");
}
|  POSITION
 { 
 $$ = mm_strdup("position");
}
|  PRECISION
 { 
 $$ = mm_strdup("precision");
}
|  REAL
 { 
 $$ = mm_strdup("real");
}
|  ROW
 { 
 $$ = mm_strdup("row");
}
|  SETOF
 { 
 $$ = mm_strdup("setof");
}
|  SMALLDATETIME
 { 
 $$ = mm_strdup("smalldatetime");
}
|  SMALLINT
 { 
 $$ = mm_strdup("smallint");
}
|  SUBSTRING
 { 
 $$ = mm_strdup("substring");
}
|  TIME
 { 
 $$ = mm_strdup("time");
}
|  TIMESTAMP
 { 
 $$ = mm_strdup("timestamp");
}
|  TIMESTAMPDIFF
 { 
 $$ = mm_strdup("timestampdiff");
}
|  TINYINT
 { 
 $$ = mm_strdup("tinyint");
}
|  TREAT
 { 
 $$ = mm_strdup("treat");
}
|  TRIM
 { 
 $$ = mm_strdup("trim");
}
|  VARCHAR
 { 
 $$ = mm_strdup("varchar");
}
|  VARCHAR2
 { 
 $$ = mm_strdup("varchar2");
}
|  XMLATTRIBUTES
 { 
 $$ = mm_strdup("xmlattributes");
}
|  XMLCONCAT
 { 
 $$ = mm_strdup("xmlconcat");
}
|  XMLELEMENT
 { 
 $$ = mm_strdup("xmlelement");
}
|  XMLEXISTS
 { 
 $$ = mm_strdup("xmlexists");
}
|  XMLFOREST
 { 
 $$ = mm_strdup("xmlforest");
}
|  XMLPARSE
 { 
 $$ = mm_strdup("xmlparse");
}
|  XMLPI
 { 
 $$ = mm_strdup("xmlpi");
}
|  XMLROOT
 { 
 $$ = mm_strdup("xmlroot");
}
|  XMLSERIALIZE
 { 
 $$ = mm_strdup("xmlserialize");
}
;


 col_name_keyword_nonambiguous:
 BETWEEN
 { 
 $$ = mm_strdup("between");
}
|  BIGINT
 { 
 $$ = mm_strdup("bigint");
}
|  BINARY_DOUBLE
 { 
 $$ = mm_strdup("binary_double");
}
|  BINARY_INTEGER
 { 
 $$ = mm_strdup("binary_integer");
}
|  BIT
 { 
 $$ = mm_strdup("bit");
}
|  BOOLEAN_P
 { 
 $$ = mm_strdup("boolean");
}
|  BUCKETCNT
 { 
 $$ = mm_strdup("bucketcnt");
}
|  BYTEAWITHOUTORDER
 { 
 $$ = mm_strdup("byteawithoutorder");
}
|  BYTEAWITHOUTORDERWITHEQUAL
 { 
 $$ = mm_strdup("byteawithoutorderwithequal");
}
|  CHAR_P
 { 
 $$ = mm_strdup("char");
}
|  CHARACTER
 { 
 $$ = mm_strdup("character");
}
|  DATE_P
 { 
 $$ = mm_strdup("date");
}
|  DEC
 { 
 $$ = mm_strdup("dec");
}
|  DECIMAL_P
 { 
 $$ = mm_strdup("decimal");
}
|  DECODE
 { 
 $$ = mm_strdup("decode");
}
|  EXISTS
 { 
 $$ = mm_strdup("exists");
}
|  FLOAT_P
 { 
 $$ = mm_strdup("float");
}
|  GROUPING_P
 { 
 $$ = mm_strdup("grouping");
}
|  INOUT
 { 
 $$ = mm_strdup("inout");
}
|  INT_P
 { 
 $$ = mm_strdup("int");
}
|  INTEGER
 { 
 $$ = mm_strdup("integer");
}
|  INTERVAL
 { 
 $$ = mm_strdup("interval");
}
|  NATIONAL
 { 
 $$ = mm_strdup("national");
}
|  NCHAR
 { 
 $$ = mm_strdup("nchar");
}
|  NONE
 { 
 $$ = mm_strdup("none");
}
|  NUMBER_P
 { 
 $$ = mm_strdup("number");
}
|  NUMERIC
 { 
 $$ = mm_strdup("numeric");
}
|  NVARCHAR2
 { 
 $$ = mm_strdup("nvarchar2");
}
|  OUT_P
 { 
 $$ = mm_strdup("out");
}
|  PRECISION
 { 
 $$ = mm_strdup("precision");
}
|  REAL
 { 
 $$ = mm_strdup("real");
}
|  ROW
 { 
 $$ = mm_strdup("row");
}
|  SETOF
 { 
 $$ = mm_strdup("setof");
}
|  SMALLDATETIME
 { 
 $$ = mm_strdup("smalldatetime");
}
|  SMALLINT
 { 
 $$ = mm_strdup("smallint");
}
|  TIME
 { 
 $$ = mm_strdup("time");
}
|  TIMESTAMP
 { 
 $$ = mm_strdup("timestamp");
}
|  TINYINT
 { 
 $$ = mm_strdup("tinyint");
}
|  VALUES
 { 
 $$ = mm_strdup("values");
}
|  VARCHAR
 { 
 $$ = mm_strdup("varchar");
}
|  VARCHAR2
 { 
 $$ = mm_strdup("varchar2");
}
|  XMLATTRIBUTES
 { 
 $$ = mm_strdup("xmlattributes");
}
;


 type_func_name_keyword:
 AUTHORIZATION
 { 
 $$ = mm_strdup("authorization");
}
|  BINARY
 { 
 $$ = mm_strdup("binary");
}
|  COLLATION
 { 
 $$ = mm_strdup("collation");
}
|  COMPACT
 { 
 $$ = mm_strdup("compact");
}
|  CONCURRENTLY
 { 
 $$ = mm_strdup("concurrently");
}
|  CROSS
 { 
 $$ = mm_strdup("cross");
}
|  CSN
 { 
 $$ = mm_strdup("csn");
}
|  CURRENT_SCHEMA
 { 
 $$ = mm_strdup("current_schema");
}
|  DELTAMERGE
 { 
 $$ = mm_strdup("deltamerge");
}
|  FREEZE
 { 
 $$ = mm_strdup("freeze");
}
|  FULL
 { 
 $$ = mm_strdup("full");
}
|  HDFSDIRECTORY
 { 
 $$ = mm_strdup("hdfsdirectory");
}
|  ILIKE
 { 
 $$ = mm_strdup("ilike");
}
|  INNER_P
 { 
 $$ = mm_strdup("inner");
}
|  JOIN
 { 
 $$ = mm_strdup("join");
}
|  LEFT
 { 
 $$ = mm_strdup("left");
}
|  LIKE
 { 
 $$ = mm_strdup("like");
}
|  NATURAL
 { 
 $$ = mm_strdup("natural");
}
|  NOTNULL
 { 
 $$ = mm_strdup("notnull");
}
|  OUTER_P
 { 
 $$ = mm_strdup("outer");
}
|  OVERLAPS
 { 
 $$ = mm_strdup("overlaps");
}
|  RECYCLEBIN
 { 
 $$ = mm_strdup("recyclebin");
}
|  RIGHT
 { 
 $$ = mm_strdup("right");
}
|  SIMILAR
 { 
 $$ = mm_strdup("similar");
}
|  TABLESAMPLE
 { 
 $$ = mm_strdup("tablesample");
}
|  VERBOSE
 { 
 $$ = mm_strdup("verbose");
}
;


 reserved_keyword:
 ALL
 { 
 $$ = mm_strdup("all");
}
|  ANALYSE
 { 
 $$ = mm_strdup("analyse");
}
|  ANALYZE
 { 
 $$ = mm_strdup("analyze");
}
|  AND
 { 
 $$ = mm_strdup("and");
}
|  ANY
 { 
 $$ = mm_strdup("any");
}
|  ARRAY
 { 
 $$ = mm_strdup("array");
}
|  AS
 { 
 $$ = mm_strdup("as");
}
|  ASC
 { 
 $$ = mm_strdup("asc");
}
|  ASYMMETRIC
 { 
 $$ = mm_strdup("asymmetric");
}
|  AUTHID
 { 
 $$ = mm_strdup("authid");
}
|  BOTH
 { 
 $$ = mm_strdup("both");
}
|  BUCKETS
 { 
 $$ = mm_strdup("buckets");
}
|  CASE
 { 
 $$ = mm_strdup("case");
}
|  CAST
 { 
 $$ = mm_strdup("cast");
}
|  CHECK
 { 
 $$ = mm_strdup("check");
}
|  COLLATE
 { 
 $$ = mm_strdup("collate");
}
|  COLUMN
 { 
 $$ = mm_strdup("column");
}
|  CONSTRAINT
 { 
 $$ = mm_strdup("constraint");
}
|  CREATE
 { 
 $$ = mm_strdup("create");
}
|  CURRENT_CATALOG
 { 
 $$ = mm_strdup("current_catalog");
}
|  CURRENT_DATE
 { 
 $$ = mm_strdup("current_date");
}
|  CURRENT_ROLE
 { 
 $$ = mm_strdup("current_role");
}
|  CURRENT_TIME
 { 
 $$ = mm_strdup("current_time");
}
|  CURRENT_TIMESTAMP
 { 
 $$ = mm_strdup("current_timestamp");
}
|  CURRENT_USER
 { 
 $$ = mm_strdup("current_user");
}
|  DEFAULT
 { 
 $$ = mm_strdup("default");
}
|  DEFERRABLE
 { 
 $$ = mm_strdup("deferrable");
}
|  DESC
 { 
 $$ = mm_strdup("desc");
}
|  DISTINCT
 { 
 $$ = mm_strdup("distinct");
}
|  DO
 { 
 $$ = mm_strdup("do");
}
|  ELSE
 { 
 $$ = mm_strdup("else");
}
|  END_P
 { 
 $$ = mm_strdup("end");
}
|  EXCEPT
 { 
 $$ = mm_strdup("except");
}
|  EXCLUDED
 { 
 $$ = mm_strdup("excluded");
}
|  FALSE_P
 { 
 $$ = mm_strdup("false");
}
|  FETCH
 { 
 $$ = mm_strdup("fetch");
}
|  FOR
 { 
 $$ = mm_strdup("for");
}
|  FOREIGN
 { 
 $$ = mm_strdup("foreign");
}
|  FROM
 { 
 $$ = mm_strdup("from");
}
|  GRANT
 { 
 $$ = mm_strdup("grant");
}
|  GROUP_P
 { 
 $$ = mm_strdup("group");
}
|  GROUPPARENT
 { 
 $$ = mm_strdup("groupparent");
}
|  HAVING
 { 
 $$ = mm_strdup("having");
}
|  IN_P
 { 
 $$ = mm_strdup("in");
}
|  INITIALLY
 { 
 $$ = mm_strdup("initially");
}
|  INTERSECT
 { 
 $$ = mm_strdup("intersect");
}
|  INTO
 { 
 $$ = mm_strdup("into");
}
|  IS
 { 
 $$ = mm_strdup("is");
}
|  LEADING
 { 
 $$ = mm_strdup("leading");
}
|  LESS
 { 
 $$ = mm_strdup("less");
}
|  LIMIT
 { 
 $$ = mm_strdup("limit");
}
|  LOCALTIME
 { 
 $$ = mm_strdup("localtime");
}
|  LOCALTIMESTAMP
 { 
 $$ = mm_strdup("localtimestamp");
}
|  MAXVALUE
 { 
 $$ = mm_strdup("maxvalue");
}
|  MINUS_P
 { 
 $$ = mm_strdup("minus");
}
|  MODIFY_P
 { 
 $$ = mm_strdup("modify");
}
|  NOCYCLE
 { 
 $$ = mm_strdup("nocycle");
}
|  NOT
 { 
 $$ = mm_strdup("not");
}
|  NULL_P
 { 
 $$ = mm_strdup("null");
}
|  OFFSET
 { 
 $$ = mm_strdup("offset");
}
|  ON
 { 
 $$ = mm_strdup("on");
}
|  ONLY
 { 
 $$ = mm_strdup("only");
}
|  OR
 { 
 $$ = mm_strdup("or");
}
|  ORDER
 { 
 $$ = mm_strdup("order");
}
|  PERFORMANCE
 { 
 $$ = mm_strdup("performance");
}
|  PLACING
 { 
 $$ = mm_strdup("placing");
}
|  PRIMARY
 { 
 $$ = mm_strdup("primary");
}
|  PROCEDURE
 { 
 $$ = mm_strdup("procedure");
}
|  REFERENCES
 { 
 $$ = mm_strdup("references");
}
|  REJECT_P
 { 
 $$ = mm_strdup("reject");
}
|  RETURNING
 { 
 $$ = mm_strdup("returning");
}
|  ROWNUM
 { 
 $$ = mm_strdup("rownum");
}
|  SELECT
 { 
 $$ = mm_strdup("select");
}
|  SESSION_USER
 { 
 $$ = mm_strdup("session_user");
}
|  SHRINK
 { 
 $$ = mm_strdup("shrink");
}
|  SOME
 { 
 $$ = mm_strdup("some");
}
|  SYMMETRIC
 { 
 $$ = mm_strdup("symmetric");
}
|  SYSDATE
 { 
 $$ = mm_strdup("sysdate");
}
|  TABLE
 { 
 $$ = mm_strdup("table");
}
|  THEN
 { 
 $$ = mm_strdup("then");
}
|  TRAILING
 { 
 $$ = mm_strdup("trailing");
}
|  TRUE_P
 { 
 $$ = mm_strdup("true");
}
|  UNIQUE
 { 
 $$ = mm_strdup("unique");
}
|  USER
 { 
 $$ = mm_strdup("user");
}
|  USING
 { 
 $$ = mm_strdup("using");
}
|  VARIADIC
 { 
 $$ = mm_strdup("variadic");
}
|  VERIFY
 { 
 $$ = mm_strdup("verify");
}
|  WHEN
 { 
 $$ = mm_strdup("when");
}
|  WHERE
 { 
 $$ = mm_strdup("where");
}
|  WINDOW
 { 
 $$ = mm_strdup("window");
}
|  WITH
 { 
 $$ = mm_strdup("with");
}
;


/* trailer */
/* src/interfaces/ecpg/preproc/ecpg.trailer */

statements: /*EMPTY*/
				| statements statement
		;

statement: ecpgstart at stmt ';' { connection = NULL; }
				| ecpgstart stmt ';'
				| ecpgstart ECPGVarDeclaration
				{
					fprintf(yyout, "%s", $2);
					free_current_memory($2);
					output_line_number();
				}
				| ECPGDeclaration
				| c_thing               { fprintf(yyout, "%s", $1); free_current_memory($1); }
				| CPP_LINE              { fprintf(yyout, "%s", $1); free_current_memory($1); }
				| '{'                   { braces_open++; fputs("{", yyout); }
				| '}'
		{
			remove_typedefs(braces_open);
			remove_variables(braces_open--);
			if (braces_open == 0)
			{
				free_current_memory(current_function);
				current_function = NULL;
			}
			fputs("}", yyout);
		}
		;

CreateAsStmt: CREATE OptTemp TABLE create_as_target AS {FoundInto = 0;} SelectStmt opt_with_data
		{
			if (FoundInto == 1)
				mmerror(PARSE_ERROR, ET_ERROR, "CREATE TABLE AS cannot specify INTO");

			$$ = cat_str(6, mm_strdup("create"), $2, mm_strdup("table"), $4, mm_strdup("as"), $7);
		}
		;

at: AT connection_object
		{
			connection = $2;
			/*
			 * Do we have a variable as connection target?  Remove the variable
			 * from the variable list or else it will be used twice.
			 */
			if (argsinsert != NULL)
				argsinsert = NULL;
		}
		;

/*
 * the exec sql connect statement: connect to the given database
 */
ECPGConnect: CONNECT TO connection_target opt_connection_name opt_user
			{ $$ = cat_str(5, $3, mm_strdup(","), $5, mm_strdup(","), $4); }
		| CONNECT TO DEFAULT
			{ $$ = mm_strdup("NULL, NULL, NULL, \"DEFAULT\""); }
		  /* also allow ORACLE syntax */
		| CONNECT ora_user
			{ $$ = cat_str(3, mm_strdup("NULL,"), $2, mm_strdup(", NULL")); }
		| DATABASE connection_target
			{ $$ = cat2_str($2, mm_strdup(", NULL, NULL, NULL")); }
		;

connection_target: opt_database_name opt_server opt_port
		{
			/* old style: dbname[@server][:port] */
			if (strlen($2) > 0 && *($2) != '@')
				mmerror(PARSE_ERROR, ET_ERROR, "expected \"@\", found \"%s\"", $2);

			/* C strings need to be handled differently */
			if ($1[0] == '\"')
				$$ = $1;
			else
				$$ = make3_str(mm_strdup("\""), make3_str($1, $2, $3), mm_strdup("\""));
		}
		|  db_prefix ':' server opt_port '/' opt_database_name opt_options
		{
			/* new style: <tcp|unix>:postgresql://server[:port][/dbname] */
			if (strncmp($1, "unix:postgresql", strlen("unix:postgresql")) != 0 && strncmp($1, "tcp:postgresql", strlen("tcp:postgresql")) != 0)
				mmerror(PARSE_ERROR, ET_ERROR, "only protocols \"tcp\" and \"unix\" and database type \"postgresql\" are supported");

			if (strncmp($3, "//", strlen("//")) != 0)
				mmerror(PARSE_ERROR, ET_ERROR, "expected \"://\", found \"%s\"", $3);

			if (strncmp($1, "unix", strlen("unix")) == 0 &&
				strncmp($3 + strlen("//"), "localhost", strlen("localhost")) != 0 &&
				strncmp($3 + strlen("//"), "127.0.0.1", strlen("127.0.0.1")) != 0)
				mmerror(PARSE_ERROR, ET_ERROR, "Unix-domain sockets only work on \"localhost\" but not on \"%s\"", $3 + strlen("//"));

			$$ = make3_str(make3_str(mm_strdup("\""), $1, mm_strdup(":")), $3, make3_str(make3_str($4, mm_strdup("/"), $6),	$7, mm_strdup("\"")));
		}
		| char_variable
		{
			$$ = $1;
		}
		| ecpg_sconst
		{
			/* We can only process double quoted strings not single quotes ones,
			 * so we change the quotes.
			 * Note, that the rule for ecpg_sconst adds these single quotes. */
			$1[0] = '\"';
			$1[strlen($1)-1] = '\"';
			$$ = $1;
		}
		;

opt_database_name: database_name		{ $$ = $1; }
		| /*EMPTY*/			{ $$ = EMPTY; }
		;

db_prefix: ecpg_ident cvariable
		{
			if (strcmp($2, "postgresql") != 0 && strcmp($2, "postgres") != 0)
				mmerror(PARSE_ERROR, ET_ERROR, "expected \"postgresql\", found \"%s\"", $2);

			if (strcmp($1, "tcp") != 0 && strcmp($1, "unix") != 0)
				mmerror(PARSE_ERROR, ET_ERROR, "invalid connection type: %s", $1);

			$$ = make3_str($1, mm_strdup(":"), $2);
		}
		;

server: Op server_name
		{
			if (strcmp($1, "@") != 0 && strcmp($1, "//") != 0)
				mmerror(PARSE_ERROR, ET_ERROR, "expected \"@\" or \"://\", found \"%s\"", $1);

			$$ = make2_str($1, $2);
		}
		;

opt_server: server			{ $$ = $1; }
		| /*EMPTY*/			{ $$ = EMPTY; }
		;

server_name: ColId					{ $$ = $1; }
		| ColId '.' server_name		{ $$ = make3_str($1, mm_strdup("."), $3); }
		| SQL_IP						{ $$ = make_name(); }
		;

opt_port: ':' Iconst		{ $$ = make2_str(mm_strdup(":"), $2); }
		| /*EMPTY*/	{ $$ = EMPTY; }
		;

opt_connection_name: AS connection_object	{ $$ = $2; }
		| /*EMPTY*/			{ $$ = mm_strdup("NULL"); }
		;

opt_user: USER ora_user		{ $$ = $2; }
		| /*EMPTY*/			{ $$ = mm_strdup("NULL, NULL"); }
		;

ora_user: user_name
			{ $$ = cat2_str($1, mm_strdup(", NULL")); }
		| user_name '/' user_name
			{ $$ = cat_str(3, $1, mm_strdup(","), $3); }
		| user_name SQL_IDENTIFIED BY user_name
			{ $$ = cat_str(3, $1, mm_strdup(","), $4); }
		| user_name USING user_name
			{ $$ = cat_str(3, $1, mm_strdup(","), $3); }
		;

user_name: RoleId
		{
			if ($1[0] == '\"')
				$$ = $1;
			else
				$$ = make3_str(mm_strdup("\""), $1, mm_strdup("\""));
		}
		| ecpg_sconst
		{
			if ($1[0] == '\"')
				$$ = $1;
			else
				$$ = make3_str(mm_strdup("\""), $1, mm_strdup("\""));
		}
		| civar
		{
			enum ECPGttype type = argsinsert->variable->type->type;

			/* if array see what's inside */
			if (type == ECPGt_array)
				type = argsinsert->variable->type->u.element->type;

			/* handle varchars */
			if (type == ECPGt_varchar)
				$$ = make2_str(mm_strdup(argsinsert->variable->name), mm_strdup(".arr"));
			else
				$$ = mm_strdup(argsinsert->variable->name);
		}
		;

char_variable: cvariable
		{
			/* check if we have a string variable */
			struct variable *p = find_variable($1);
			enum ECPGttype type = p->type->type;

			/* If we have just one character this is not a string */
			if (atol(p->type->size) == 1)
					mmerror(PARSE_ERROR, ET_ERROR, "invalid data type");
			else
			{
				/* if array see what's inside */
				if (type == ECPGt_array)
					type = p->type->u.element->type;

				switch (type)
				{
					case ECPGt_char:
					case ECPGt_unsigned_char:
					case ECPGt_string:
						$$ = $1;
						break;
					case ECPGt_varchar:
						$$ = make2_str($1, mm_strdup(".arr"));
						break;
					default:
						mmerror(PARSE_ERROR, ET_ERROR, "invalid data type");
						$$ = $1;
						break;
				}
			}
		}
		;

opt_options: Op connect_options
		{
			if (strlen($1) == 0)
				mmerror(PARSE_ERROR, ET_ERROR, "incomplete statement");

			if (strcmp($1, "?") != 0)
				mmerror(PARSE_ERROR, ET_ERROR, "unrecognized token \"%s\"", $1);

			$$ = make2_str(mm_strdup("?"), $2);
		}
		| /*EMPTY*/	{ $$ = EMPTY; }
		;

connect_options:  ColId opt_opt_value
			{
				$$ = make2_str($1, $2);
			}
		| ColId opt_opt_value Op connect_options
			{
				if (strlen($3) == 0)
					mmerror(PARSE_ERROR, ET_ERROR, "incomplete statement");

				if (strcmp($3, "&") != 0)
					mmerror(PARSE_ERROR, ET_ERROR, "unrecognized token \"%s\"", $3);

				$$ = cat_str(3, make2_str($1, $2), $3, $4);
			}
		;

opt_opt_value: /*EMPTY*/
			{ $$ = EMPTY; }
		| '=' Iconst
			{ $$ = make2_str(mm_strdup("="), $2); }
		| '=' ecpg_ident
			{ $$ = make2_str(mm_strdup("="), $2); }
		| '=' civar
			{ $$ = make2_str(mm_strdup("="), $2); }
		;

prepared_name: name
		{
			if ($1[0] == '\"' && $1[strlen($1)-1] == '\"') /* already quoted? */
				$$ = $1;
			else /* not quoted => convert to lowercase */
			{
				size_t i;

				for (i = 0; i< strlen($1); i++)
					$1[i] = tolower((unsigned char) $1[i]);

				$$ = make3_str(mm_strdup("\""), $1, mm_strdup("\""));
			}
		}
		| char_variable { $$ = $1; }
		;

/*
 * Declare a prepared cursor. The syntax is different from the standard
 * declare statement, so we create a new rule.
 */
ECPGCursorStmt:  DECLARE cursor_name cursor_options CURSOR opt_hold FOR prepared_name
		{
			struct cursor *ptr, *thisPtr;
			char *cursor_marker = $2[0] == ':' ? mm_strdup("$0") : mm_strdup($2);
			int (* strcmp_fn)(const char *, const char *) = ($2[0] == ':' ? strcmp : pg_strcasecmp);
			struct variable *thisquery = (struct variable *)mm_alloc(sizeof(struct variable));
			const char *con = connection ? connection : "NULL";
			char *comment;

			for (ptr = cur; ptr != NULL; ptr = ptr->next)
			{
				if (strcmp_fn($2, ptr->name) == 0)
				{
					/* re-definition is a bug */
					if ($2[0] == ':')
						mmerror(PARSE_ERROR, ET_ERROR, "using variable \"%s\" in different declare statements is not supported", $2+1);
					else
						mmerror(PARSE_ERROR, ET_ERROR, "cursor \"%s\" is already defined", $2);
				}
			}

			thisPtr = (struct cursor *) mm_alloc(sizeof(struct cursor));

			/* initial definition */
			thisPtr->next = cur;
			thisPtr->name = $2;
			thisPtr->function = (current_function ? mm_strdup(current_function) : NULL);
			thisPtr->connection = connection;
			thisPtr->command =  cat_str(6, mm_strdup("declare"), cursor_marker, $3, mm_strdup("cursor"), $5, mm_strdup("for $1"));
			thisPtr->argsresult = NULL;
			thisPtr->argsresult_oos = NULL;

			thisquery->type = &ecpg_query;
			thisquery->brace_level = 0;
			thisquery->next = NULL;
			thisquery->name = (char *) mm_alloc(sizeof("ECPGprepared_statement(, , __LINE__)") + strlen(con) + strlen($7));
			sprintf(thisquery->name, "ECPGprepared_statement(%s, %s, __LINE__)", con, $7);

			thisPtr->argsinsert = NULL;
			thisPtr->argsinsert_oos = NULL;
			if ($2[0] == ':')
			{
				struct variable *var = find_variable($2 + 1);
				remove_variable_from_list(&argsinsert, var);
				add_variable_to_head(&(thisPtr->argsinsert), var, &no_indicator);
			}
			add_variable_to_head(&(thisPtr->argsinsert), thisquery, &no_indicator);

			cur = thisPtr;

			comment = cat_str(3, mm_strdup("/*"), mm_strdup(thisPtr->command), mm_strdup("*/"));

			if ((braces_open > 0) && INFORMIX_MODE) /* we're in a function */
				$$ = cat_str(3, adjust_outofscope_cursor_vars(thisPtr),
					mm_strdup("ECPG_informix_reset_sqlca();"),
					comment);
			else
				$$ = cat2_str(adjust_outofscope_cursor_vars(thisPtr), comment);
		}
		;

ECPGExecuteImmediateStmt: EXECUTE IMMEDIATE execstring
			{
			  /* execute immediate means prepare the statement and
			   * immediately execute it */
			  $$ = $3;
			};
/*
 * variable decalartion outside exec sql declare block
 */
ECPGVarDeclaration: single_vt_declaration;

single_vt_declaration: type_declaration		{ $$ = $1; }
		| var_declaration		{ $$ = $1; }
		;

precision:	NumericOnly	{ $$ = $1; };

opt_scale:	',' NumericOnly	{ $$ = $2; }
		| /* EMPTY */	{ $$ = EMPTY; }
		;

ecpg_interval:	opt_interval	{ $$ = $1; }
		| YEAR_P TO MINUTE_P	{ $$ = mm_strdup("year to minute"); }
		| YEAR_P TO SECOND_P	{ $$ = mm_strdup("year to second"); }
		| DAY_P TO DAY_P		{ $$ = mm_strdup("day to day"); }
		| MONTH_P TO MONTH_P	{ $$ = mm_strdup("month to month"); }
		;

/*
 * variable declaration inside exec sql declare block
 */
ECPGDeclaration: sql_startdeclare
		{ fputs("/* exec sql begin declare section */", yyout); }
		var_type_declarations sql_enddeclare
		{
			fprintf(yyout, "%s/* exec sql end declare section */", $3);
			free_current_memory($3);
			output_line_number();
		}
		;

sql_startdeclare: ecpgstart BEGIN_P DECLARE SQL_SECTION ';' {};

sql_enddeclare: ecpgstart END_P DECLARE SQL_SECTION ';' {};

var_type_declarations:	/*EMPTY*/			{ $$ = EMPTY; }
		| vt_declarations			{ $$ = $1; }
		;

vt_declarations:  single_vt_declaration			{ $$ = $1; }
		| CPP_LINE				{ $$ = $1; }
		| vt_declarations single_vt_declaration	{ $$ = cat2_str($1, $2); }
		| vt_declarations CPP_LINE		{ $$ = cat2_str($1, $2); }
		;

variable_declarations:	var_declaration	{ $$ = $1; }
		| variable_declarations var_declaration	{ $$ = cat2_str($1, $2); }
		;

type_declaration: S_TYPEDEF
	{
		/* reset this variable so we see if there was */
		/* an initializer specified */
		initializer = 0;
	}
	var_type opt_pointer ECPGColLabelCommon opt_array_bounds ';'
	{
		add_typedef($5, $6.index1, $6.index2, $3.type_enum, $3.type_dimension, $3.type_index, initializer, *$4 ? 1 : 0);

		fprintf(yyout, "typedef %s %s %s %s;\n", $3.type_str, *$4 ? "*" : "", $5, $6.str);
		output_line_number();
		$$ = mm_strdup("");
	};

var_declaration: storage_declaration
		var_type
		{
			actual_type[struct_level].type_enum = $2.type_enum;
			actual_type[struct_level].type_str = $2.type_str;
			actual_type[struct_level].type_dimension = $2.type_dimension;
			actual_type[struct_level].type_index = $2.type_index;
			actual_type[struct_level].type_sizeof = $2.type_sizeof;

			actual_startline[struct_level] = hashline_number();
		}
		variable_list ';'
		{
			$$ = cat_str(5, actual_startline[struct_level], $1, $2.type_str, $4, mm_strdup(";\n"));
		}
		| var_type
		{
			actual_type[struct_level].type_enum = $1.type_enum;
			actual_type[struct_level].type_str = $1.type_str;
			actual_type[struct_level].type_dimension = $1.type_dimension;
			actual_type[struct_level].type_index = $1.type_index;
			actual_type[struct_level].type_sizeof = $1.type_sizeof;

			actual_startline[struct_level] = hashline_number();
		}
		variable_list ';'
		{
			$$ = cat_str(4, actual_startline[struct_level], $1.type_str, $3, mm_strdup(";\n"));
		}
		| struct_union_type_with_symbol ';'
		{
			$$ = cat2_str($1, mm_strdup(";"));
		}
		;

opt_bit_field:	':' Iconst	{ $$ =cat2_str(mm_strdup(":"), $2); }
		| /* EMPTY */	{ $$ = EMPTY; }
		;

storage_declaration: storage_clause storage_modifier
			{$$ = cat2_str ($1, $2); }
		| storage_clause		{$$ = $1; }
		| storage_modifier		{$$ = $1; }
		;

storage_clause : S_EXTERN	{ $$ = mm_strdup("extern"); }
		| S_STATIC			{ $$ = mm_strdup("static"); }
		| S_REGISTER		{ $$ = mm_strdup("register"); }
		| S_AUTO			{ $$ = mm_strdup("auto"); }
		;

storage_modifier : S_CONST	{ $$ = mm_strdup("const"); }
		| S_VOLATILE		{ $$ = mm_strdup("volatile"); }
		;

var_type:	simple_type
		{
			$$.type_enum = $1;
			$$.type_str = mm_strdup(ecpg_type_name($1));
			$$.type_dimension = mm_strdup("-1");
			$$.type_index = mm_strdup("-1");
			$$.type_sizeof = NULL;
		}
		| struct_union_type
		{
			$$.type_str = $1;
			$$.type_dimension = mm_strdup("-1");
			$$.type_index = mm_strdup("-1");

			if (strncmp($1, "struct", sizeof("struct")-1) == 0)
			{
				$$.type_enum = ECPGt_struct;
				$$.type_sizeof = ECPGstruct_sizeof;
			}
			else
			{
				$$.type_enum = ECPGt_union;
				$$.type_sizeof = NULL;
			}
		}
		| enum_type
		{
			$$.type_str = $1;
			$$.type_enum = ECPGt_int;
			$$.type_dimension = mm_strdup("-1");
			$$.type_index = mm_strdup("-1");
			$$.type_sizeof = NULL;
		}
		| ECPGColLabelCommon '(' precision opt_scale ')'
		{
			if (strcmp($1, "numeric") == 0)
			{
				$$.type_enum = ECPGt_numeric;
				$$.type_str = mm_strdup("numeric");
			}
			else if (strcmp($1, "decimal") == 0)
			{
				$$.type_enum = ECPGt_decimal;
				$$.type_str = mm_strdup("decimal");
			}
			else
			{
				mmerror(PARSE_ERROR, ET_ERROR, "only data types numeric and decimal have precision/scale argument");
				$$.type_enum = ECPGt_numeric;
				$$.type_str = mm_strdup("numeric");
			}

			$$.type_dimension = mm_strdup("-1");
			$$.type_index = mm_strdup("-1");
			$$.type_sizeof = NULL;
		}
		| ECPGColLabelCommon ecpg_interval
		{
			if (strlen($2) != 0 && strcmp ($1, "datetime") != 0 && strcmp ($1, "interval") != 0)
				mmerror (PARSE_ERROR, ET_ERROR, "interval specification not allowed here");

			/*
			 * Check for type names that the SQL grammar treats as
			 * unreserved keywords
			 */
			if (strcmp($1, "varchar") == 0)
			{
				$$.type_enum = ECPGt_varchar;
				$$.type_str = EMPTY; /*mm_strdup("varchar");*/
				$$.type_dimension = mm_strdup("-1");
				$$.type_index = mm_strdup("-1");
				$$.type_sizeof = NULL;
			}
			else if (strcmp($1, "float") == 0)
			{
				$$.type_enum = ECPGt_float;
				$$.type_str = mm_strdup("float");
				$$.type_dimension = mm_strdup("-1");
				$$.type_index = mm_strdup("-1");
				$$.type_sizeof = NULL;
			}
			else if (strcmp($1, "double") == 0)
			{
				$$.type_enum = ECPGt_double;
				$$.type_str = mm_strdup("double");
				$$.type_dimension = mm_strdup("-1");
				$$.type_index = mm_strdup("-1");
				$$.type_sizeof = NULL;
			}
			else if (strcmp($1, "numeric") == 0)
			{
				$$.type_enum = ECPGt_numeric;
				$$.type_str = mm_strdup("numeric");
				$$.type_dimension = mm_strdup("-1");
				$$.type_index = mm_strdup("-1");
				$$.type_sizeof = NULL;
			}
			else if (strcmp($1, "decimal") == 0)
			{
				$$.type_enum = ECPGt_decimal;
				$$.type_str = mm_strdup("decimal");
				$$.type_dimension = mm_strdup("-1");
				$$.type_index = mm_strdup("-1");
				$$.type_sizeof = NULL;
			}
			else if (strcmp($1, "date") == 0)
			{
				$$.type_enum = ECPGt_date;
				$$.type_str = mm_strdup("date");
				$$.type_dimension = mm_strdup("-1");
				$$.type_index = mm_strdup("-1");
				$$.type_sizeof = NULL;
			}
			else if (strcmp($1, "timestamp") == 0)
			{
				$$.type_enum = ECPGt_timestamp;
				$$.type_str = mm_strdup("timestamp");
				$$.type_dimension = mm_strdup("-1");
				$$.type_index = mm_strdup("-1");
				$$.type_sizeof = NULL;
			}
			else if (strcmp($1, "interval") == 0)
			{
				$$.type_enum = ECPGt_interval;
				$$.type_str = mm_strdup("interval");
				$$.type_dimension = mm_strdup("-1");
				$$.type_index = mm_strdup("-1");
				$$.type_sizeof = NULL;
			}
			else if (strcmp($1, "datetime") == 0)
			{
				$$.type_enum = ECPGt_timestamp;
				$$.type_str = mm_strdup("timestamp");
				$$.type_dimension = mm_strdup("-1");
				$$.type_index = mm_strdup("-1");
				$$.type_sizeof = NULL;
			}
			else if ((strcmp($1, "string") == 0) && INFORMIX_MODE)
			{
				$$.type_enum = ECPGt_string;
				$$.type_str = mm_strdup("char");
				$$.type_dimension = mm_strdup("-1");
				$$.type_index = mm_strdup("-1");
				$$.type_sizeof = NULL;
			}
			else
			{
				/* this is for typedef'ed types */
				struct typedefs *thisPtr = get_typedef($1);

				$$.type_str = (thisPtr->type->type_enum == ECPGt_varchar) ? EMPTY : mm_strdup(thisPtr->name);
				$$.type_enum = thisPtr->type->type_enum;
				$$.type_dimension = thisPtr->type->type_dimension;
				$$.type_index = thisPtr->type->type_index;
				if (thisPtr->type->type_sizeof && strlen(thisPtr->type->type_sizeof) != 0)
					$$.type_sizeof = thisPtr->type->type_sizeof;
				else
					$$.type_sizeof = cat_str(3, mm_strdup("sizeof("), mm_strdup(thisPtr->name), mm_strdup(")"));

				struct_member_list[struct_level] = ECPGstruct_member_dup(thisPtr->struct_member_list);
			}
		}
		| s_struct_union_symbol
		{
			/* this is for named structs/unions */
			char *name;
			struct typedefs *thisPtr;
			bool forward = (forward_name != NULL && strcmp($1.symbol, forward_name) == 0 && strcmp($1.su, "struct") == 0);

			name = cat2_str($1.su, $1.symbol);
			/* Do we have a forward definition? */
			if (!forward)
			{
				/* No */

				thisPtr = get_typedef(name);
				$$.type_str = mm_strdup(thisPtr->name);
				$$.type_enum = thisPtr->type->type_enum;
				$$.type_dimension = thisPtr->type->type_dimension;
				$$.type_index = thisPtr->type->type_index;
				$$.type_sizeof = thisPtr->type->type_sizeof;
				struct_member_list[struct_level] = ECPGstruct_member_dup(thisPtr->struct_member_list);
				free_current_memory(name);
			}
			else
			{
				$$.type_str = name;
				$$.type_enum = ECPGt_long;
				$$.type_dimension = mm_strdup("-1");
				$$.type_index = mm_strdup("-1");
				$$.type_sizeof = mm_strdup("");
				struct_member_list[struct_level] = NULL;
			}
		}
		;

enum_type: ENUM_P symbol enum_definition
			{ $$ = cat_str(3, mm_strdup("enum"), $2, $3); }
		| ENUM_P enum_definition
			{ $$ = cat2_str(mm_strdup("enum"), $2); }
		| ENUM_P symbol
			{ $$ = cat2_str(mm_strdup("enum"), $2); }
		;

enum_definition: '{' c_list '}'
			{ $$ = cat_str(3, mm_strdup("{"), $2, mm_strdup("}")); };

struct_union_type_with_symbol: s_struct_union_symbol
		{
			struct_member_list[struct_level++] = NULL;
			if (struct_level >= STRUCT_DEPTH)
				 mmerror(PARSE_ERROR, ET_ERROR, "too many levels in nested structure/union definition");
			forward_name = mm_strdup($1.symbol);
		}
		'{' variable_declarations '}'
		{
			struct typedefs *ptr, *thisPtr;
			struct this_type su_type;

			ECPGfree_struct_member(struct_member_list[struct_level]);
			struct_member_list[struct_level] = NULL;
			struct_level--;
			if (strncmp($1.su, "struct", sizeof("struct")-1) == 0)
				su_type.type_enum = ECPGt_struct;
			else
				su_type.type_enum = ECPGt_union;
			su_type.type_str = cat2_str($1.su, $1.symbol);
			free_current_memory(forward_name);
			forward_name = NULL;

			/* This is essantially a typedef but needs the keyword struct/union as well.
			 * So we create the typedef for each struct definition with symbol */
			for (ptr = types; ptr != NULL; ptr = ptr->next)
			{
					if (strcmp(su_type.type_str, ptr->name) == 0)
							/* re-definition is a bug */
							mmerror(PARSE_ERROR, ET_ERROR, "type \"%s\" is already defined", su_type.type_str);
			}

			thisPtr = (struct typedefs *) mm_alloc(sizeof(struct typedefs));

			/* initial definition */
			thisPtr->next = types;
			thisPtr->name = mm_strdup(su_type.type_str);
			thisPtr->brace_level = braces_open;
			thisPtr->type = (struct this_type *) mm_alloc(sizeof(struct this_type));
			thisPtr->type->type_enum = su_type.type_enum;
			thisPtr->type->type_str = mm_strdup(su_type.type_str);
			thisPtr->type->type_dimension = mm_strdup("-1"); /* dimension of array */
			thisPtr->type->type_index = mm_strdup("-1");	/* length of string */
			thisPtr->type->type_sizeof = ECPGstruct_sizeof;
			thisPtr->struct_member_list = struct_member_list[struct_level];

			types = thisPtr;
			$$ = cat_str(4, su_type.type_str, mm_strdup("{"), $4, mm_strdup("}"));
		}
		;

struct_union_type: struct_union_type_with_symbol	{ $$ = $1; }
		| s_struct_union
		{
			struct_member_list[struct_level++] = NULL;
			if (struct_level >= STRUCT_DEPTH)
				 mmerror(PARSE_ERROR, ET_ERROR, "too many levels in nested structure/union definition");
		}
		'{' variable_declarations '}'
		{
			ECPGfree_struct_member(struct_member_list[struct_level]);
			struct_member_list[struct_level] = NULL;
			struct_level--;
			$$ = cat_str(4, $1, mm_strdup("{"), $4, mm_strdup("}"));
		}
		;

s_struct_union_symbol: SQL_STRUCT symbol
		{
			$$.su = mm_strdup("struct");
			$$.symbol = $2;
			ECPGstruct_sizeof = cat_str(3, mm_strdup("sizeof("), cat2_str(mm_strdup($$.su), mm_strdup($$.symbol)), mm_strdup(")"));
		}
		| UNION symbol
		{
			$$.su = mm_strdup("union");
			$$.symbol = $2;
		}
		;

s_struct_union: SQL_STRUCT
		{
			ECPGstruct_sizeof = mm_strdup(""); /* This must not be NULL to distinguish from simple types. */
			$$ = mm_strdup("struct");
		}
		| UNION
		{
			$$ = mm_strdup("union");
		}
		;

simple_type: unsigned_type					{ $$=$1; }
		|	opt_signed signed_type			{ $$=$2; }
		;

unsigned_type: SQL_UNSIGNED SQL_SHORT		{ $$ = ECPGt_unsigned_short; }
		| SQL_UNSIGNED SQL_SHORT INT_P	{ $$ = ECPGt_unsigned_short; }
		| SQL_UNSIGNED						{ $$ = ECPGt_unsigned_int; }
		| SQL_UNSIGNED INT_P				{ $$ = ECPGt_unsigned_int; }
		| SQL_UNSIGNED SQL_LONG				{ $$ = ECPGt_unsigned_long; }
		| SQL_UNSIGNED SQL_LONG INT_P		{ $$ = ECPGt_unsigned_long; }
		| SQL_UNSIGNED SQL_LONG SQL_LONG
		{
#ifdef HAVE_LONG_LONG_INT
			$$ = ECPGt_unsigned_long_long;
#else
			$$ = ECPGt_unsigned_long;
#endif
		}
		| SQL_UNSIGNED SQL_LONG SQL_LONG INT_P
		{
#ifdef HAVE_LONG_LONG_INT
			$$ = ECPGt_unsigned_long_long;
#else
			$$ = ECPGt_unsigned_long;
#endif
		}
		| SQL_UNSIGNED CHAR_P			{ $$ = ECPGt_unsigned_char; }
		;

signed_type: SQL_SHORT				{ $$ = ECPGt_short; }
		| SQL_SHORT INT_P			{ $$ = ECPGt_short; }
		| INT_P						{ $$ = ECPGt_int; }
		| SQL_LONG					{ $$ = ECPGt_long; }
		| SQL_LONG INT_P			{ $$ = ECPGt_long; }
		| SQL_LONG SQL_LONG
		{
#ifdef HAVE_LONG_LONG_INT
			$$ = ECPGt_long_long;
#else
			$$ = ECPGt_long;
#endif
		}
		| SQL_LONG SQL_LONG INT_P
		{
#ifdef HAVE_LONG_LONG_INT
			$$ = ECPGt_long_long;
#else
			$$ = ECPGt_long;
#endif
		}
		| SQL_BOOL					{ $$ = ECPGt_bool; }
		| CHAR_P					{ $$ = ECPGt_char; }
		| DOUBLE_P					{ $$ = ECPGt_double; }
		;

opt_signed: SQL_SIGNED
		|	/* EMPTY */
		;

variable_list: variable
			{ $$ = $1; }
		| variable_list ',' variable
			{ $$ = cat_str(3, $1, mm_strdup(","), $3); }
		;

variable: opt_pointer ECPGColLabel opt_array_bounds opt_bit_field opt_initializer
		{
			struct ECPGtype * type;
			char *dimension = $3.index1;	/* dimension of array */
			char *length = $3.index2;		/* length of string */
			char *dim_str;
			char *vcn;

			adjust_array(actual_type[struct_level].type_enum, &dimension, &length, actual_type[struct_level].type_dimension, actual_type[struct_level].type_index, strlen($1), false);

			switch (actual_type[struct_level].type_enum)
			{
				case ECPGt_struct:
				case ECPGt_union:
					if (atoi(dimension) < 0)
						type = ECPGmake_struct_type(struct_member_list[struct_level], actual_type[struct_level].type_enum, actual_type[struct_level].type_str, actual_type[struct_level].type_sizeof);
					else
						type = ECPGmake_array_type(ECPGmake_struct_type(struct_member_list[struct_level], actual_type[struct_level].type_enum, actual_type[struct_level].type_str, actual_type[struct_level].type_sizeof), dimension);

					$$ = cat_str(5, $1, mm_strdup($2), $3.str, $4, $5);
					break;

				case ECPGt_varchar:
					if (atoi(dimension) < 0)
						type = ECPGmake_simple_type(actual_type[struct_level].type_enum, length, varchar_counter);
					else
						type = ECPGmake_array_type(ECPGmake_simple_type(actual_type[struct_level].type_enum, length, varchar_counter), dimension);

					if (strcmp(dimension, "0") == 0 || abs(atoi(dimension)) == 1)
							dim_str=mm_strdup("");
					else
							dim_str=cat_str(3, mm_strdup("["), mm_strdup(dimension), mm_strdup("]"));
					/* cannot check for atoi <= 0 because a defined constant will yield 0 here as well */
					if (atoi(length) < 0 || strcmp(length, "0") == 0)
						mmerror(PARSE_ERROR, ET_ERROR, "pointers to varchar are not implemented");

					/* make sure varchar struct name is unique by adding a unique counter to its definition */
					vcn = (char *) mm_alloc(sizeof(int) * CHAR_BIT * 10 / 3);
					sprintf(vcn, "%d", varchar_counter);
					if (strcmp(dimension, "0") == 0)
						$$ = cat_str(7, make2_str(mm_strdup(" struct varchar_"), vcn), mm_strdup(" { int len; char arr["), mm_strdup(length), mm_strdup("]; } *"), mm_strdup($2), $4, $5);
					else
						$$ = cat_str(8, make2_str(mm_strdup(" struct varchar_"), vcn), mm_strdup(" { int len; char arr["), mm_strdup(length), mm_strdup("]; } "), mm_strdup($2), dim_str, $4, $5);
					varchar_counter++;
					break;

				case ECPGt_char:
				case ECPGt_unsigned_char:
				case ECPGt_string:
					if (atoi(dimension) == -1)
					{
						int i = strlen($5);

						if (atoi(length) == -1 && i > 0) /* char <var>[] = "string" */
						{
							/* if we have an initializer but no string size set, let's use the initializer's length */
							free_current_memory(length);
							length = (char *)mm_alloc(i+sizeof("sizeof()"));
							sprintf(length, "sizeof(%s)", $5+2);
						}
						type = ECPGmake_simple_type(actual_type[struct_level].type_enum, length, 0);
					}
					else
						type = ECPGmake_array_type(ECPGmake_simple_type(actual_type[struct_level].type_enum, length, 0), dimension);

					$$ = cat_str(5, $1, mm_strdup($2), $3.str, $4, $5);
					break;

				default:
					if (atoi(dimension) < 0)
						type = ECPGmake_simple_type(actual_type[struct_level].type_enum, mm_strdup("1"), 0);
					else
						type = ECPGmake_array_type(ECPGmake_simple_type(actual_type[struct_level].type_enum, mm_strdup("1"), 0), dimension);

					$$ = cat_str(5, $1, mm_strdup($2), $3.str, $4, $5);
					break;
			}

			if (struct_level == 0)
				new_variable($2, type, braces_open);
			else
				ECPGmake_struct_member($2, type, &(struct_member_list[struct_level - 1]));

			free_current_memory($2);
		}
		;

opt_initializer: /*EMPTY*/
			{ $$ = EMPTY; }
		| '=' c_term
		{
			initializer = 1;
			$$ = cat2_str(mm_strdup("="), $2);
		}
		;

opt_pointer: /*EMPTY*/				{ $$ = EMPTY; }
		| '*'						{ $$ = mm_strdup("*"); }
		| '*' '*'					{ $$ = mm_strdup("**"); }
		;

/*
 * We try to simulate the correct DECLARE syntax here so we get dynamic SQL
 */
ECPGDeclare: DECLARE STATEMENT ecpg_ident
		{
			/* this is only supported for compatibility */
			$$ = cat_str(3, mm_strdup("/* declare statement"), $3, mm_strdup("*/"));
		}
		;
/*
 * the exec sql disconnect statement: disconnect from the given database
 */
ECPGDisconnect: DISCONNECT dis_name { $$ = $2; }
		;

dis_name: connection_object			{ $$ = $1; }
		| CURRENT_P			{ $$ = mm_strdup("\"CURRENT\""); }
		| ALL				{ $$ = mm_strdup("\"ALL\""); }
		| /* EMPTY */			{ $$ = mm_strdup("\"CURRENT\""); }
		;

connection_object: database_name		{ $$ = make3_str(mm_strdup("\""), $1, mm_strdup("\"")); }
		| DEFAULT			{ $$ = mm_strdup("\"DEFAULT\""); }
		| char_variable			{ $$ = $1; }
		;

execstring: char_variable
			{ $$ = $1; }
		|	CSTRING
			{ $$ = make3_str(mm_strdup("\""), $1, mm_strdup("\"")); }
		;

/*
 * the exec sql free command to deallocate a previously
 * prepared statement
 */
ECPGFree:	SQL_FREE cursor_name	{ $$ = $2; }
		| SQL_FREE ALL	{ $$ = mm_strdup("all"); }
		;

/*
 * open is an open cursor, at the moment this has to be removed
 */
ECPGOpen: SQL_OPEN cursor_name opt_ecpg_using
		{
			if ($2[0] == ':')
				remove_variable_from_list(&argsinsert, find_variable($2 + 1));
			$$ = $2;
		}
		;

opt_ecpg_using: /*EMPTY*/	{ $$ = EMPTY; }
		| ecpg_using		{ $$ = $1; }
		;

ecpg_using:	USING using_list	{ $$ = EMPTY; }
		| using_descriptor		{ $$ = $1; }
		;

using_descriptor: USING SQL_SQL SQL_DESCRIPTOR quoted_ident_stringvar
		{
			add_variable_to_head(&argsinsert, descriptor_variable($4,0), &no_indicator);
			$$ = EMPTY;
		}
		| USING SQL_DESCRIPTOR name
		{
			add_variable_to_head(&argsinsert, sqlda_variable($3), &no_indicator);
			$$ = EMPTY;
		}
		;

into_descriptor: INTO SQL_SQL SQL_DESCRIPTOR quoted_ident_stringvar
		{
			add_variable_to_head(&argsresult, descriptor_variable($4,1), &no_indicator);
			$$ = EMPTY;
		}
		| INTO SQL_DESCRIPTOR name
		{
			add_variable_to_head(&argsresult, sqlda_variable($3), &no_indicator);
			$$ = EMPTY;
		}
		;

into_sqlda: INTO name
		{
			add_variable_to_head(&argsresult, sqlda_variable($2), &no_indicator);
			$$ = EMPTY;
		}
		;

using_list: UsingValue | UsingValue ',' using_list;

UsingValue: UsingConst
		{
			char *length = (char *)mm_alloc(32);

			sprintf(length, "%d", (int) strlen($1));
			add_variable_to_head(&argsinsert, new_variable($1, ECPGmake_simple_type(ECPGt_const, length, 0), 0), &no_indicator);
		}
		| civar { $$ = EMPTY; }
		| civarind { $$ = EMPTY; }
		;

UsingConst: Iconst			{ $$ = $1; }
		| '+' Iconst		{ $$ = cat_str(2, mm_strdup("+"), $2); }
		| '-' Iconst		{ $$ = cat_str(2, mm_strdup("-"), $2); }
		| ecpg_fconst		{ $$ = $1; }
		| '+' ecpg_fconst	{ $$ = cat_str(2, mm_strdup("+"), $2); }
		| '-' ecpg_fconst	{ $$ = cat_str(2, mm_strdup("-"), $2); }
		| ecpg_sconst		{ $$ = $1; }
		| ecpg_bconst		{ $$ = $1; }
		| ecpg_xconst		{ $$ = $1; }
		;

/*
 * We accept DESCRIBE [OUTPUT] but do nothing with DESCRIBE INPUT so far.
 */
ECPGDescribe: SQL_DESCRIBE INPUT_P prepared_name using_descriptor
	{
		const char *con = connection ? connection : "NULL";
		mmerror(PARSE_ERROR, ET_WARNING, "using unsupported DESCRIBE statement");
		$$ = (char *) mm_alloc(sizeof("1, , ") + strlen(con) + strlen($3));
		sprintf($$, "1, %s, %s", con, $3);
	}
	| SQL_DESCRIBE opt_output prepared_name using_descriptor
	{
		const char *con = connection ? connection : "NULL";
		struct variable *var;

		var = argsinsert->variable;
		remove_variable_from_list(&argsinsert, var);
		add_variable_to_head(&argsresult, var, &no_indicator);

		$$ = (char *) mm_alloc(sizeof("0, , ") + strlen(con) + strlen($3));
		sprintf($$, "0, %s, %s", con, $3);
	}
	| SQL_DESCRIBE opt_output prepared_name into_descriptor
	{
		const char *con = connection ? connection : "NULL";
		$$ = (char *) mm_alloc(sizeof("0, , ") + strlen(con) + strlen($3));
		sprintf($$, "0, %s, %s", con, $3);
	}
	| SQL_DESCRIBE INPUT_P prepared_name into_sqlda
	{
		const char *con = connection ? connection : "NULL";
		mmerror(PARSE_ERROR, ET_WARNING, "using unsupported DESCRIBE statement");
		$$ = (char *) mm_alloc(sizeof("1, , ") + strlen(con) + strlen($3));
		sprintf($$, "1, %s, %s", con, $3);
	}
	| SQL_DESCRIBE opt_output prepared_name into_sqlda
	{
		const char *con = connection ? connection : "NULL";
		$$ = (char *) mm_alloc(sizeof("0, , ") + strlen(con) + strlen($3));
		sprintf($$, "0, %s, %s", con, $3);
	}
	;

opt_output:	SQL_OUTPUT	{ $$ = mm_strdup("output"); }
	|	/* EMPTY */	{ $$ = EMPTY; }
	;

/*
 * dynamic SQL: descriptor based access
 *	originally written by Christof Petig <christof.petig@wtal.de>
 *			and Peter Eisentraut <peter.eisentraut@credativ.de>
 */

/*
 * allocate a descriptor
 */
ECPGAllocateDescr: SQL_ALLOCATE SQL_DESCRIPTOR quoted_ident_stringvar
		{
			add_descriptor($3,connection);
			$$ = $3;
		}
		;


/*
 * deallocate a descriptor
 */
ECPGDeallocateDescr:	DEALLOCATE SQL_DESCRIPTOR quoted_ident_stringvar
		{
			drop_descriptor($3,connection);
			$$ = $3;
		}
		;

/*
 * manipulate a descriptor header
 */

ECPGGetDescriptorHeader: SQL_GET SQL_DESCRIPTOR quoted_ident_stringvar ECPGGetDescHeaderItems
			{  $$ = $3; }
		;

ECPGGetDescHeaderItems: ECPGGetDescHeaderItem
		| ECPGGetDescHeaderItems ',' ECPGGetDescHeaderItem
		;

ECPGGetDescHeaderItem: cvariable '=' desc_header_item
			{ push_assignment($1, $3); }
		;


ECPGSetDescriptorHeader: SET SQL_DESCRIPTOR quoted_ident_stringvar ECPGSetDescHeaderItems
			{ $$ = $3; }
		;

ECPGSetDescHeaderItems: ECPGSetDescHeaderItem
		| ECPGSetDescHeaderItems ',' ECPGSetDescHeaderItem
		;

ECPGSetDescHeaderItem: desc_header_item '=' IntConstVar
		{
			push_assignment($3, $1);
		}
		;

IntConstVar: Iconst
		{
			char *length = (char *)mm_alloc(sizeof(int) * CHAR_BIT * 10 / 3);

			sprintf(length, "%d", (int) strlen($1));
			new_variable($1, ECPGmake_simple_type(ECPGt_const, length, 0), 0);
			$$ = $1;
		}
		| cvariable
		{
			$$ = $1;
		}
		;

desc_header_item:	SQL_COUNT			{ $$ = ECPGd_count; }
		;

/*
 * manipulate a descriptor
 */

ECPGGetDescriptor:	SQL_GET SQL_DESCRIPTOR quoted_ident_stringvar VALUE_P IntConstVar ECPGGetDescItems
			{  $$.str = $5; $$.name = $3; }
		;

ECPGGetDescItems: ECPGGetDescItem
		| ECPGGetDescItems ',' ECPGGetDescItem
		;

ECPGGetDescItem: cvariable '=' descriptor_item	{ push_assignment($1, $3); };


ECPGSetDescriptor:	SET SQL_DESCRIPTOR quoted_ident_stringvar VALUE_P IntConstVar ECPGSetDescItems
			{  $$.str = $5; $$.name = $3; }
		;

ECPGSetDescItems: ECPGSetDescItem
		| ECPGSetDescItems ',' ECPGSetDescItem
		;

ECPGSetDescItem: descriptor_item '=' AllConstVar
		{
			push_assignment($3, $1);
		}
		;

AllConstVar: ecpg_fconst
		{
			char *length = (char *)mm_alloc(sizeof(int) * CHAR_BIT * 10 / 3);

			sprintf(length, "%d", (int) strlen($1));
			new_variable($1, ECPGmake_simple_type(ECPGt_const, length, 0), 0);
			$$ = $1;
		}

		| IntConstVar
		{
			$$ = $1;
		}

		| '-' ecpg_fconst
		{
			char *length = (char *)mm_alloc(sizeof(int) * CHAR_BIT * 10 / 3);
			char *var = cat2_str(mm_strdup("-"), $2);

			sprintf(length, "%d", (int) strlen(var));
			new_variable(var, ECPGmake_simple_type(ECPGt_const, length, 0), 0);
			$$ = var;
		}

		| '-' Iconst
		{
			char *length = (char *)mm_alloc(sizeof(int) * CHAR_BIT * 10 / 3);
			char *var = cat2_str(mm_strdup("-"), $2);

			sprintf(length, "%d", (int) strlen(var));
			new_variable(var, ECPGmake_simple_type(ECPGt_const, length, 0), 0);
			$$ = var;
		}

		| ecpg_sconst
		{
			char *length = (char *)mm_alloc(sizeof(int) * CHAR_BIT * 10 / 3);
			char *var = $1 + 1;

			var[strlen(var) - 1] = '\0';
			sprintf(length, "%d", (int) strlen(var));
			new_variable(var, ECPGmake_simple_type(ECPGt_const, length, 0), 0);
			$$ = var;
		}
		;

descriptor_item:	SQL_CARDINALITY			{ $$ = ECPGd_cardinality; }
		| DATA_P				{ $$ = ECPGd_data; }
		| SQL_DATETIME_INTERVAL_CODE		{ $$ = ECPGd_di_code; }
		| SQL_DATETIME_INTERVAL_PRECISION	{ $$ = ECPGd_di_precision; }
		| SQL_INDICATOR				{ $$ = ECPGd_indicator; }
		| SQL_KEY_MEMBER			{ $$ = ECPGd_key_member; }
		| SQL_LENGTH				{ $$ = ECPGd_length; }
		| NAME_P				{ $$ = ECPGd_name; }
		| SQL_NULLABLE				{ $$ = ECPGd_nullable; }
		| SQL_OCTET_LENGTH			{ $$ = ECPGd_octet; }
		| PRECISION				{ $$ = ECPGd_precision; }
		| SQL_RETURNED_LENGTH			{ $$ = ECPGd_length; }
		| SQL_RETURNED_OCTET_LENGTH		{ $$ = ECPGd_ret_octet; }
		| SQL_SCALE				{ $$ = ECPGd_scale; }
		| TYPE_P				{ $$ = ECPGd_type; }
		;

/*
 * set/reset the automatic transaction mode, this needs a differnet handling
 * as the other set commands
 */
ECPGSetAutocommit:	SET SQL_AUTOCOMMIT '=' on_off	{ $$ = $4; }
		|  SET SQL_AUTOCOMMIT TO on_off   { $$ = $4; }
		;

on_off: ON				{ $$ = mm_strdup("on"); }
		| OFF			{ $$ = mm_strdup("off"); }
		;

/*
 * set the actual connection, this needs a differnet handling as the other
 * set commands
 */
ECPGSetConnection:	SET CONNECTION TO connection_object { $$ = $4; }
		| SET CONNECTION '=' connection_object { $$ = $4; }
		| SET CONNECTION  connection_object { $$ = $3; }
		;

/*
 * define a new type for embedded SQL
 */
ECPGTypedef: TYPE_P
		{
			/* reset this variable so we see if there was */
			/* an initializer specified */
			initializer = 0;
		}
		ECPGColLabelCommon IS var_type opt_array_bounds opt_reference
		{
			add_typedef($3, $6.index1, $6.index2, $5.type_enum, $5.type_dimension, $5.type_index, initializer, *$7 ? 1 : 0);

			if (auto_create_c == false)
				$$ = cat_str(7, mm_strdup("/* exec sql type"), mm_strdup($3), mm_strdup("is"), mm_strdup($5.type_str), mm_strdup($6.str), $7, mm_strdup("*/"));
			else
				$$ = cat_str(6, mm_strdup("typedef "), mm_strdup($5.type_str), *$7?mm_strdup("*"):mm_strdup(""), mm_strdup($6.str), mm_strdup($3), mm_strdup(";"));
		}
		;

opt_reference: SQL_REFERENCE		{ $$ = mm_strdup("reference"); }
		| /*EMPTY*/					{ $$ = EMPTY; }
		;

/*
 * define the type of one variable for embedded SQL
 */
ECPGVar: SQL_VAR
		{
			/* reset this variable so we see if there was */
			/* an initializer specified */
			initializer = 0;
		}
		ColLabel IS var_type opt_array_bounds opt_reference
		{
			struct variable *p = find_variable($3);
			char *dimension = $6.index1;
			char *length = $6.index2;
			struct ECPGtype * type;

			if (($5.type_enum == ECPGt_struct ||
				 $5.type_enum == ECPGt_union) &&
				initializer == 1)
				mmerror(PARSE_ERROR, ET_ERROR, "initializer not allowed in EXEC SQL VAR command");
			else
			{
				adjust_array($5.type_enum, &dimension, &length, $5.type_dimension, $5.type_index, *$7?1:0, false);

				switch ($5.type_enum)
				{
					case ECPGt_struct:
					case ECPGt_union:
						if (atoi(dimension) < 0)
							type = ECPGmake_struct_type(struct_member_list[struct_level], $5.type_enum, $5.type_str, $5.type_sizeof);
						else
							type = ECPGmake_array_type(ECPGmake_struct_type(struct_member_list[struct_level], $5.type_enum, $5.type_str, $5.type_sizeof), dimension);
						break;

					case ECPGt_varchar:
						if (atoi(dimension) == -1)
							type = ECPGmake_simple_type($5.type_enum, length, 0);
						else
							type = ECPGmake_array_type(ECPGmake_simple_type($5.type_enum, length, 0), dimension);
						break;

					case ECPGt_char:
					case ECPGt_unsigned_char:
					case ECPGt_string:
						if (atoi(dimension) == -1)
							type = ECPGmake_simple_type($5.type_enum, length, 0);
						else
							type = ECPGmake_array_type(ECPGmake_simple_type($5.type_enum, length, 0), dimension);
						break;

					default:
						if (atoi(length) >= 0)
							mmerror(PARSE_ERROR, ET_ERROR, "multidimensional arrays for simple data types are not supported");

						if (atoi(dimension) < 0)
							type = ECPGmake_simple_type($5.type_enum, mm_strdup("1"), 0);
						else
							type = ECPGmake_array_type(ECPGmake_simple_type($5.type_enum, mm_strdup("1"), 0), dimension);
						break;
				}

				ECPGfree_type(p->type);
				p->type = type;
			}

			$$ = cat_str(7, mm_strdup("/* exec sql var"), mm_strdup($3), mm_strdup("is"), mm_strdup($5.type_str), mm_strdup($6.str), $7, mm_strdup("*/"));
		}
		;

/*
 * whenever statement: decide what to do in case of error/no data found
 * according to SQL standards we lack: SQLSTATE, CONSTRAINT and SQLEXCEPTION
 */
ECPGWhenever: SQL_WHENEVER SQL_SQLERROR action
		{
			when_error.code = $<action>3.code;
			when_error.command = $<action>3.command;
			$$ = cat_str(3, mm_strdup("/* exec sql whenever sqlerror "), $3.str, mm_strdup("; */"));
		}
		| SQL_WHENEVER NOT SQL_FOUND action
		{
			when_nf.code = $<action>4.code;
			when_nf.command = $<action>4.command;
			$$ = cat_str(3, mm_strdup("/* exec sql whenever not found "), $4.str, mm_strdup("; */"));
		}
		| SQL_WHENEVER SQL_SQLWARNING action
		{
			when_warn.code = $<action>3.code;
			when_warn.command = $<action>3.command;
			$$ = cat_str(3, mm_strdup("/* exec sql whenever sql_warning "), $3.str, mm_strdup("; */"));
		}
		;

action : CONTINUE_P
		{
			$<action>$.code = W_NOTHING;
			$<action>$.command = NULL;
			$<action>$.str = mm_strdup("continue");
		}
		| SQL_SQLPRINT
		{
			$<action>$.code = W_SQLPRINT;
			$<action>$.command = NULL;
			$<action>$.str = mm_strdup("sqlprint");
		}
		| SQL_STOP
		{
			$<action>$.code = W_STOP;
			$<action>$.command = NULL;
			$<action>$.str = mm_strdup("stop");
		}
		| SQL_GOTO name
		{
			$<action>$.code = W_GOTO;
			$<action>$.command = mm_strdup($2);
			$<action>$.str = cat2_str(mm_strdup("goto "), $2);
		}
		| SQL_GO TO name
		{
			$<action>$.code = W_GOTO;
			$<action>$.command = mm_strdup($3);
			$<action>$.str = cat2_str(mm_strdup("goto "), $3);
		}
		| DO name '(' c_args ')'
		{
			$<action>$.code = W_DO;
			$<action>$.command = cat_str(4, $2, mm_strdup("("), $4, mm_strdup(")"));
			$<action>$.str = cat2_str(mm_strdup("do"), mm_strdup($<action>$.command));
		}
		| DO SQL_BREAK
		{
			$<action>$.code = W_BREAK;
			$<action>$.command = NULL;
			$<action>$.str = mm_strdup("break");
		}
		| SQL_CALL name '(' c_args ')'
		{
			$<action>$.code = W_DO;
			$<action>$.command = cat_str(4, $2, mm_strdup("("), $4, mm_strdup(")"));
			$<action>$.str = cat2_str(mm_strdup("call"), mm_strdup($<action>$.command));
		}
		| SQL_CALL name
		{
			$<action>$.code = W_DO;
			$<action>$.command = cat2_str($2, mm_strdup("()"));
			$<action>$.str = cat2_str(mm_strdup("call"), mm_strdup($<action>$.command));
		}
		;

/* some other stuff for ecpg */

/* additional unreserved keywords */
ECPGKeywords: ECPGKeywords_vanames	{ $$ = $1; }
		| ECPGKeywords_rest	{ $$ = $1; }
		;

ECPGKeywords_vanames:  SQL_BREAK		{ $$ = mm_strdup("break"); }
		| SQL_CALL						{ $$ = mm_strdup("call"); }
		| SQL_CARDINALITY				{ $$ = mm_strdup("cardinality"); }
		| SQL_COUNT						{ $$ = mm_strdup("count"); }
		| SQL_DATETIME_INTERVAL_CODE	{ $$ = mm_strdup("datetime_interval_code"); }
		| SQL_DATETIME_INTERVAL_PRECISION	{ $$ = mm_strdup("datetime_interval_precision"); }
		| SQL_FOUND						{ $$ = mm_strdup("found"); }
		| SQL_GO						{ $$ = mm_strdup("go"); }
		| SQL_GOTO						{ $$ = mm_strdup("goto"); }
		| SQL_IDENTIFIED				{ $$ = mm_strdup("identified"); }
		| SQL_INDICATOR				{ $$ = mm_strdup("indicator"); }
		| SQL_KEY_MEMBER			{ $$ = mm_strdup("key_member"); }
		| SQL_LENGTH				{ $$ = mm_strdup("length"); }
		| SQL_NULLABLE				{ $$ = mm_strdup("nullable"); }
		| SQL_OCTET_LENGTH			{ $$ = mm_strdup("octet_length"); }
		| SQL_RETURNED_LENGTH		{ $$ = mm_strdup("returned_length"); }
		| SQL_RETURNED_OCTET_LENGTH	{ $$ = mm_strdup("returned_octet_length"); }
		| SQL_SCALE					{ $$ = mm_strdup("scale"); }
		| SQL_SECTION				{ $$ = mm_strdup("section"); }
		| SQL_SQL				{ $$ = mm_strdup("sql"); }
		| SQL_SQLERROR				{ $$ = mm_strdup("sqlerror"); }
		| SQL_SQLPRINT				{ $$ = mm_strdup("sqlprint"); }
		| SQL_SQLWARNING			{ $$ = mm_strdup("sqlwarning"); }
		| SQL_STOP					{ $$ = mm_strdup("stop"); }
		;

ECPGKeywords_rest:  
		  SQL_DESCRIBE				{ $$ = mm_strdup("describe"); }
		| SQL_OPEN					{ $$ = mm_strdup("open"); }
		| SQL_VAR					{ $$ = mm_strdup("var"); }
		| SQL_WHENEVER				{ $$ = mm_strdup("whenever"); }
		;

/* additional keywords that can be SQL type names (but not ECPGColLabels) */
ECPGTypeName:  SQL_BOOL				{ $$ = mm_strdup("bool"); }
		| SQL_LONG					{ $$ = mm_strdup("long"); }
		| SQL_OUTPUT				{ $$ = mm_strdup("output"); }
		| SQL_SHORT					{ $$ = mm_strdup("short"); }
		| SQL_STRUCT				{ $$ = mm_strdup("struct"); }
		| SQL_SIGNED				{ $$ = mm_strdup("signed"); }
		| SQL_UNSIGNED				{ $$ = mm_strdup("unsigned"); }
		;

symbol: ColLabel					{ $$ = $1; }
		;

ECPGColId: ecpg_ident				{ $$ = $1; }
		| unreserved_keyword		{ $$ = $1; }
		| col_name_keyword			{ $$ = $1; }
		| ECPGunreserved_interval	{ $$ = $1; }
		| ECPGKeywords				{ $$ = $1; }
		| ECPGCKeywords				{ $$ = $1; }
		| CHAR_P					{ $$ = mm_strdup("char"); }
		| VALUES					{ $$ = mm_strdup("values"); }
		;

/*
 * Name classification hierarchy.
 *
 * These productions should match those in the core grammar, except that
 * we use all_unreserved_keyword instead of unreserved_keyword, and
 * where possible include ECPG keywords as well as core keywords.
 */

/* Column identifier --- names that can be column, table, etc names.
 */
ColId:	ecpg_ident					{ $$ = $1; }
		| all_unreserved_keyword	{ $$ = $1; }
		| col_name_keyword			{ $$ = $1; }
		| ECPGKeywords				{ $$ = $1; }
		| ECPGCKeywords				{ $$ = $1; }
		| CHAR_P					{ $$ = mm_strdup("char"); }
		| VALUES					{ $$ = mm_strdup("values"); }
		;

/* Type/function identifier --- names that can be type or function names.
 */
type_function_name:	ecpg_ident		{ $$ = $1; }
		| all_unreserved_keyword	{ $$ = $1; }
		| type_func_name_keyword	{ $$ = $1; }
		| ECPGKeywords				{ $$ = $1; }
		| ECPGCKeywords				{ $$ = $1; }
		| ECPGTypeName				{ $$ = $1; }
		;

/* Column label --- allowed labels in "AS" clauses.
 * This presently includes *all* Postgres keywords.
 */
ColLabel:  ECPGColLabel				{ $$ = $1; }
		| ECPGTypeName				{ $$ = $1; }
		| CHAR_P					{ $$ = mm_strdup("char"); }
		| CURRENT_P					{ $$ = mm_strdup("current"); }
		| INPUT_P					{ $$ = mm_strdup("input"); }
		| INT_P						{ $$ = mm_strdup("int"); }
		| TO						{ $$ = mm_strdup("to"); }
		| UNION						{ $$ = mm_strdup("union"); }
		| VALUES					{ $$ = mm_strdup("values"); }
		| ECPGCKeywords				{ $$ = $1; }
		| ECPGunreserved_interval	{ $$ = $1; }
		;

ECPGColLabel:  ECPGColLabelCommon	{ $$ = $1; }
		| unreserved_keyword		{ $$ = $1; }
		| reserved_keyword			{ $$ = $1; }
		| ECPGKeywords_rest			{ $$ = $1; }
		| CONNECTION				{ $$ = mm_strdup("connection"); }
		;

ECPGColLabelCommon:  ecpg_ident		{ $$ = $1; }
		| col_name_keyword			{ $$ = $1; }
		| type_func_name_keyword	{ $$ = $1; }
		| ECPGKeywords_vanames		{ $$ = $1; }
		;

ECPGCKeywords: S_AUTO				{ $$ = mm_strdup("auto"); }
		| S_CONST					{ $$ = mm_strdup("const"); }
		| S_EXTERN					{ $$ = mm_strdup("extern"); }
		| S_REGISTER				{ $$ = mm_strdup("register"); }
		| S_STATIC					{ $$ = mm_strdup("static"); }
		| S_TYPEDEF					{ $$ = mm_strdup("typedef"); }
		| S_VOLATILE				{ $$ = mm_strdup("volatile"); }
		;

/* "Unreserved" keywords --- available for use as any kind of name.
 */

/*
 * The following symbols must be excluded from ECPGColLabel and directly
 * included into ColLabel to enable C variables to get names from ECPGColLabel:
 * DAY_P, HOUR_P, MINUTE_P, MONTH_P, SECOND_P, YEAR_P.
 *
 * We also have to exclude CONNECTION, CURRENT, and INPUT for various reasons.
 * CONNECTION can be added back in all_unreserved_keyword, but CURRENT and
 * INPUT are reserved for ecpg purposes.
 *
 * The mentioned exclusions are done by $replace_line settings in parse.pl.
 */
all_unreserved_keyword: unreserved_keyword	{ $$ = $1; }
		| ECPGunreserved_interval			{ $$ = $1; }
		| CONNECTION						{ $$ = mm_strdup("connection"); }
		;

ECPGunreserved_interval: DAY_P				{ $$ = mm_strdup("day"); }
		| HOUR_P							{ $$ = mm_strdup("hour"); }
		| MINUTE_P							{ $$ = mm_strdup("minute"); }
		| MONTH_P							{ $$ = mm_strdup("month"); }
		| SECOND_P							{ $$ = mm_strdup("second"); }
		| YEAR_P							{ $$ = mm_strdup("year"); }
		;


into_list: coutputvariable | into_list ',' coutputvariable 
		;

ecpgstart: SQL_START	{
				reset_variables();
				pacounter = 1;
			}
		;

c_args: /*EMPTY*/		{ $$ = EMPTY; }
		| c_list		{ $$ = $1; }
		;

coutputvariable: cvariable indicator
			{ add_variable_to_head(&argsresult, find_variable($1), find_variable($2)); }
		| cvariable
			{ add_variable_to_head(&argsresult, find_variable($1), &no_indicator); }
		;


civarind: cvariable indicator
		{
			if (find_variable($2)->type->type == ECPGt_array)
				mmerror(PARSE_ERROR, ET_ERROR, "arrays of indicators are not allowed on input");

			add_variable_to_head(&argsinsert, find_variable($1), find_variable($2));
			$$ = create_questionmarks($1, false);
		}
		;

char_civar: char_variable
		{
			char *ptr = strstr($1, ".arr");

			if (ptr) /* varchar, we need the struct name here, not the struct element */
				*ptr = '\0';
			add_variable_to_head(&argsinsert, find_variable($1), &no_indicator);
			$$ = $1;
		}
		;

civar: cvariable
		{
			add_variable_to_head(&argsinsert, find_variable($1), &no_indicator);
			$$ = create_questionmarks($1, false);
		}
		;

indicator: cvariable				{ check_indicator((find_variable($1))->type); $$ = $1; }
		| SQL_INDICATOR cvariable	{ check_indicator((find_variable($2))->type); $$ = $2; }
		| SQL_INDICATOR name		{ check_indicator((find_variable($2))->type); $$ = $2; }
		;

cvariable:	CVARIABLE
		{
			/* As long as multidimensional arrays are not implemented we have to check for those here */
			char *ptr = $1;
			int brace_open=0, brace = false;

			for (; *ptr; ptr++)
			{
				switch (*ptr)
				{
					case '[':
							if (brace)
								mmerror(PARSE_ERROR, ET_FATAL, "multidimensional arrays for simple data types are not supported");
							brace_open++;
							break;
					case ']':
							brace_open--;
							if (brace_open == 0)
								brace = true;
							break;
					case '\t':
					case ' ':
							break;
					default:
							if (brace_open == 0)
								brace = false;
							break;
				}
			}
			$$ = $1;
		}
		;

ecpg_param:	PARAM		{ $$ = make_name(); } ;

ecpg_bconst:	BCONST		{ $$ = make_name(); } ;

ecpg_fconst:	FCONST		{ $$ = make_name(); } ;

ecpg_sconst:
		SCONST
		{
			/* could have been input as '' or $$ */
			$$ = (char *)mm_alloc(strlen($1) + 3);
			$$[0]='\'';
			strcpy($$+1, $1);
			$$[strlen($1)+1]='\'';
			$$[strlen($1)+2]='\0';
			free_current_memory($1);
		}
		| ECONST
		{
			$$ = (char *)mm_alloc(strlen($1) + 4);
			$$[0]='E';
			$$[1]='\'';
			strcpy($$+2, $1);
			$$[strlen($1)+2]='\'';
			$$[strlen($1)+3]='\0';
			free_current_memory($1);
		}
		| NCONST
		{
			$$ = (char *)mm_alloc(strlen($1) + 4);
			$$[0]='N';
			$$[1]='\'';
			strcpy($$+2, $1);
			$$[strlen($1)+2]='\'';
			$$[strlen($1)+3]='\0';
			free_current_memory($1);
		}
		| UCONST	{ $$ = $1; }
		| DOLCONST	{ $$ = $1; }
		;

ecpg_xconst:	XCONST		{ $$ = make_name(); } ;

ecpg_ident:	IDENT		{ $$ = make_name(); }
		| CSTRING	{ $$ = make3_str(mm_strdup("\""), $1, mm_strdup("\"")); }
		| UIDENT	{ $$ = $1; }
		;

quoted_ident_stringvar: name
			{ $$ = make3_str(mm_strdup("\""), $1, mm_strdup("\"")); }
		| char_variable
			{ $$ = make3_str(mm_strdup("("), $1, mm_strdup(")")); }
		;

/*
 * C stuff
 */

c_stuff_item: c_anything			{ $$ = $1; }
		| '(' ')'			{ $$ = mm_strdup("()"); }
		| '(' c_stuff ')'
			{ $$ = cat_str(3, mm_strdup("("), $2, mm_strdup(")")); }
		;

c_stuff: c_stuff_item			{ $$ = $1; }
		| c_stuff c_stuff_item
			{ $$ = cat2_str($1, $2); }
		;

c_list: c_term				{ $$ = $1; }
		| c_list ',' c_term	{ $$ = cat_str(3, $1, mm_strdup(","), $3); }
		;

c_term:  c_stuff			{ $$ = $1; }
		| '{' c_list '}'	{ $$ = cat_str(3, mm_strdup("{"), $2, mm_strdup("}")); }
		;

c_thing:	c_anything		{ $$ = $1; }
		|	'('		{ $$ = mm_strdup("("); }
		|	')'		{ $$ = mm_strdup(")"); }
		|	','		{ $$ = mm_strdup(","); }
		|	';'		{ $$ = mm_strdup(";"); }
		;

c_anything:  ecpg_ident				{ $$ = $1; }
		| Iconst			{ $$ = $1; }
		| ecpg_fconst			{ $$ = $1; }
		| ecpg_sconst			{ $$ = $1; }
		| '*'				{ $$ = mm_strdup("*"); }
		| '+'				{ $$ = mm_strdup("+"); }
		| '-'				{ $$ = mm_strdup("-"); }
		| '/'				{ $$ = mm_strdup("/"); }
		| '%'				{ $$ = mm_strdup("%"); }
		| NULL_P			{ $$ = mm_strdup("NULL"); }
		| S_ADD				{ $$ = mm_strdup("+="); }
		| S_AND				{ $$ = mm_strdup("&&"); }
		| S_ANYTHING			{ $$ = make_name(); }
		| S_AUTO			{ $$ = mm_strdup("auto"); }
		| S_CONST			{ $$ = mm_strdup("const"); }
		| S_DEC				{ $$ = mm_strdup("--"); }
		| S_DIV				{ $$ = mm_strdup("/="); }
		| S_DOTPOINT			{ $$ = mm_strdup(".*"); }
		| S_EQUAL			{ $$ = mm_strdup("=="); }
		| S_EXTERN			{ $$ = mm_strdup("extern"); }
		| S_INC				{ $$ = mm_strdup("++"); }
		| S_LSHIFT			{ $$ = mm_strdup("<<"); }
		| S_MEMBER			{ $$ = mm_strdup("->"); }
		| S_MEMPOINT			{ $$ = mm_strdup("->*"); }
		| S_MOD				{ $$ = mm_strdup("%="); }
		| S_MUL				{ $$ = mm_strdup("*="); }
		| S_NEQUAL			{ $$ = mm_strdup("!="); }
		| S_OR				{ $$ = mm_strdup("||"); }
		| S_REGISTER			{ $$ = mm_strdup("register"); }
		| S_RSHIFT			{ $$ = mm_strdup(">>"); }
		| S_STATIC			{ $$ = mm_strdup("static"); }
		| S_SUB				{ $$ = mm_strdup("-="); }
		| S_TYPEDEF			{ $$ = mm_strdup("typedef"); }
		| S_VOLATILE			{ $$ = mm_strdup("volatile"); }
		| SQL_BOOL			{ $$ = mm_strdup("bool"); }
		| ENUM_P			{ $$ = mm_strdup("enum"); }
		| HOUR_P			{ $$ = mm_strdup("hour"); }
		| INT_P				{ $$ = mm_strdup("int"); }
		| SQL_LONG			{ $$ = mm_strdup("long"); }
		| MINUTE_P			{ $$ = mm_strdup("minute"); }
		| MONTH_P			{ $$ = mm_strdup("month"); }
		| SECOND_P			{ $$ = mm_strdup("second"); }
		| SQL_SHORT			{ $$ = mm_strdup("short"); }
		| SQL_SIGNED			{ $$ = mm_strdup("signed"); }
		| SQL_STRUCT			{ $$ = mm_strdup("struct"); }
		| SQL_UNSIGNED			{ $$ = mm_strdup("unsigned"); }
		| YEAR_P			{ $$ = mm_strdup("year"); }
		| CHAR_P			{ $$ = mm_strdup("char"); }
		| FLOAT_P			{ $$ = mm_strdup("float"); }
		| TO				{ $$ = mm_strdup("to"); }
		| UNION				{ $$ = mm_strdup("union"); }
		| VARCHAR			{ $$ = mm_strdup("varchar"); }
		| '['				{ $$ = mm_strdup("["); }
		| ']'				{ $$ = mm_strdup("]"); }
		| '='				{ $$ = mm_strdup("="); }
		| ':'				{ $$ = mm_strdup(":"); }
		;

DeallocateStmt: DEALLOCATE prepared_name                { $$ = $2; }
                | DEALLOCATE PREPARE prepared_name      { $$ = $3; }
                | DEALLOCATE ALL                        { $$ = mm_strdup("all"); }
                | DEALLOCATE PREPARE ALL                { $$ = mm_strdup("all"); }
                ;

Iresult:        Iconst			{ $$ = $1; }
                | '(' Iresult ')'       { $$ = cat_str(3, mm_strdup("("), $2, mm_strdup(")")); }
                | Iresult '+' Iresult   { $$ = cat_str(3, $1, mm_strdup("+"), $3); }
                | Iresult '-' Iresult   { $$ = cat_str(3, $1, mm_strdup("-"), $3); }
                | Iresult '*' Iresult   { $$ = cat_str(3, $1, mm_strdup("*"), $3); }
                | Iresult '/' Iresult   { $$ = cat_str(3, $1, mm_strdup("/"), $3); }
                | Iresult '%' Iresult   { $$ = cat_str(3, $1, mm_strdup("%"), $3); }
                | ecpg_sconst		{ $$ = $1; }
                | ColId                 { $$ = $1; }
                ;

execute_rest: /* EMPTY */	{ $$ = EMPTY; }
	| ecpg_using ecpg_into  { $$ = EMPTY; }
	| ecpg_into ecpg_using  { $$ = EMPTY; }
	| ecpg_using			{ $$ = EMPTY; }
	| ecpg_into				{ $$ = EMPTY; }
	;

ecpg_into: INTO into_list	{ $$ = EMPTY; }
	| into_descriptor		{ $$ = $1; }
	;

ecpg_fetch_into: ecpg_into	{ $$ = $1; }
	| using_descriptor
	{
		struct variable *var;

		var = argsinsert->variable;
		remove_variable_from_list(&argsinsert, var);
		add_variable_to_head(&argsresult, var, &no_indicator);
		$$ = $1;
	}
	;

opt_ecpg_fetch_into:	/* EMPTY */	{ $$ = EMPTY; }
	| ecpg_fetch_into		{ $$ = $1; }
	;

%%

void base_yyerror(const char *error)
{
	/* translator: %s is typically the translation of "syntax error" */
	mmerror(PARSE_ERROR, ET_ERROR, "%s at or near \"%s\"",
			_(error), token_start ? token_start : yytext);
}

void parser_init(void)
{
 /* This function is empty. It only exists for compatibility with the backend parser right now. */
}

/*
 * Must undefine base_yylex before including pgc.c, since we want it
 * to create the function base_yylex not filtered_base_yylex.
 */
#undef base_yylex

#include "pgc.cpp"
