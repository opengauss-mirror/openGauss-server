%{
/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2021, openGauss Contributors
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
 * jsonpath_gram.y
 *
 * IDENTIFICATION
 *    src/common/backend/utils/adt/jsonpath_gram.y
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "utils/jsonpath.h"
#include "nodes/pg_list.h"
#include "regex/regex.h"
#include "parser/scanner.h"


/* Location tracking support --- simpler than bison's default */

#define YYLLOC_DEFAULT(Current, Rhs, N) \
	do { \
		if (N) \
			(Current) = (Rhs)[1]; \
		else \
			(Current) = (Rhs)[0]; \
	} while (0)

#define YYLTYPE int
extern int jsonpath_yylex(YYSTYPE* lvalp, YYLTYPE* llocp);
#define YYMALLOC palloc
#define YYFREE pfree

static void jsonpath_yyerror(const char *msg);
#define yyerror(yylloc, result, msg) jsonpath_yyerror(msg)

%}

/* BISON Declarations */
%define api.pure
%expect 0
%name-prefix="jsonpath_yy"
%locations

%parse-param {JsonPathItem **result}

%union
{
    JsonPathItem* value;
    JsonPathParseItem* parse;
    char*   str;
    bool    boolean;
    int     integer;
    List*   list;
}

%token <str> IDENT_P NUMERIC_P STRING_P
%token <integer> INT_P 
%token <str> TO_P 
%token <str> ABS_P BOOLEAN_P CEILING_P DATE_P DOUBLE_P FLOOR_P

%type <value> basic_path absolute_path //relative_path
              object_step array_step filter_expr opt_filter_expr function_steps
%type <parse> non_function_steps
%type <str> fieldName
%type <integer> index
%type <list> index_list array_index

/* Grammar follows */
%%
basic_path:
    absolute_path { $$ = $1; };
    //relative_path { $$ = $1; };

absolute_path:  '$'
                { 
                    *result = MakeItemType(JPI_ABSOLUTE_START);
                }
                | '$' non_function_steps
                {
                    JsonPathItem* item = MakeItemType(JPI_ABSOLUTE_START);

                    item->next = $2->head;

                    *result = item;
                }
                | '$' function_steps
                {
                    JsonPathItem* item = MakeItemType(JPI_ABSOLUTE_START);

                    item->next = $2;

                    $$ = item;
                }
                | '$' non_function_steps function_steps
                {
                    JsonPathItem* item = MakeItemType(JPI_ABSOLUTE_START);

                    item->next = $2->head;
                    $2->tail->next = $3;

                    $$ = item;
                }
                ;

non_function_steps: object_step opt_filter_expr
                    {
                        JsonPathParseItem* item = MakePathParseItem();
                        
                        item->head = $1;
                        item->tail = $2 == NULL ? $1 : $2;
                        $1->next = $2;

                        $$ = item;
                    }
                  | non_function_steps object_step opt_filter_expr
                    {
                        $2->next = $3;
                        $1->tail->next = $2;
                        $1->tail = $3 == NULL ? $2 : $3;
                    }
                  | array_step opt_filter_expr
                    {
                        JsonPathParseItem* item = MakePathParseItem();
                        
                        item->head = $1;
                        item->tail = $2 == NULL ? $1 : $2;
                        $1->next = $2;

                        $$ = item;
                    }
                  | non_function_steps array_step opt_filter_expr 
                    {
                        JsonPathParseItem* item = $1;

                        item->tail->next = $2 != NULL ? $2 : $3;
                        item->tail = $3 != NULL ? $3 :
                                     $2 != NULL ? $2 : $1->tail;
                        if ($2 != NULL)
                            $2->next = $3;

                        $$ = item;
                    }
                    ;

opt_filter_expr:  filter_expr { $$ = $1; }
                | /* EMPTY */ { $$ = NULL; };

filter_expr: '?' { $$ = NULL; };

array_step:   '[' '*' ']'
            {
                $$ = MakeItemType(JPI_ARRAY);
            }
            | '[' index_list ']'
            {
                JsonPathItem* v = MakeItemType(JPI_ARRAY);
                ((JsonPathArrayStep*)v)->indexes = $2;
                $$ = v;
            }

index_list:   array_index { $$ = $1; }
            | index_list ',' array_index { $$ = list_concat($1, $3); };

array_index:  index
            {
                $$ = list_make1_int($1);
            }
            | index TO_P index
            {
                int max = $3 > $1 ? $3 : $1;
                int min = $3 > $1 ? $1 : $3;
                List* l = list_make1_int(min);

                if (max < 0 || min < 0)
                    ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("array index cannot be negative")));

                for (int i = min + 1; i <= max; i++) {
                    l = lappend_int(l, i);
                }

                $$ = l;
            }
            ;

index:  INT_P 
        {
            if ($1 < 0)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("array index cannot be negative")));

            $$ = $1;
        }
        ;

object_step:  '.' '*' 
            {
                $$ = MakeItemType(JPI_OBJECT);
            }
            | '.' fieldName 
            {
                JsonPathItem* v = MakeItemType(JPI_OBJECT);
                ((JsonPathObjectStep*)v)->fieldName = $2;
                $$ = v;
            }
            ;
fieldName: IDENT_P { $$ = $1; };

function_steps: '.' method '(' ')'
                {
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                         errmsg("using methods in json path is not supported yet.")));
                }
                ;

method: 
    ABS_P
    | BOOLEAN_P
    | CEILING_P
    | DATE_P
    | DOUBLE_P
    | FLOOR_P
%%

static void
jsonpath_yyerror(const char *msg)
{
    ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
            errmsg("%s in json path expression", _(msg))));
}

#undef yyerror
#undef yylval
#undef yylloc
#undef yylex

#include "jsonpath_scan.inc"
