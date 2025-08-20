#include "commands/dbcommands.h"
static void pgtsql_base_yyerror(YYLTYPE * yylloc, core_yyscan_t yyscanner, const char *msg);
List *TsqlSystemFuncName2(char *name);
static List* make_no_reseed_func(char* table_name, bool with_no_msgs, bool reseed_to_max);
static List* make_reseed_func(char* table_name, Node* new_seed, bool with_no_msgs);
static List* make_func_call_func(List* funcname,  List* args);
static char* quote_identifier_wrapper(char* ident, core_yyscan_t yyscanner);
static Node* TsqlMakeAnonyBlockFuncStmt(int flag, const char *str);
extern Oid get_language_oid(const char* langname, bool missing_ok);
static Node* makeTSQLHexStringConst(char *str, int location);
static Node* TsqlFunctionTryCast(Node* arg, TypeName* typname, int location);
static Node* TsqlFunctionConvert(TypeName* typname, Node* arg, Node* style, bool is_try, int location);
static Node* DoTypeCast(TypeName* typname, bool is_try, Node* arg, List* args, int location);
static bool is_qualifed_char_type(char* typename_string);