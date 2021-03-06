/* -------------------------------------------------------------------------
 *
 * print.h
 *	  definitions for nodes/print.c
 *
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/nodes/print.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PRINT_H
#define PRINT_H

#include "executor/tuptable.h"

#define nodeDisplay(x) pprint(x)

extern void print(const void* obj);
extern void pprint(const void* obj);
extern void elog_node_display(int lev, const char* title, const void* obj, bool pretty);
extern char* format_node_dump(const char* dump);
extern char* pretty_format_node_dump(const char* dump);
void format_debug_print_plan(char *force_line, int index, int length);
void mask_position(char *cmp_line, int index, int length, const char* cmp_str);
void add_mask_policy(char *cmp_line, int scan_index, int length);
void tolower(char *tolower_query);
extern void print_rt(const List* rtable);
extern void print_expr(const Node* expr, const List* rtable);
extern void print_pathkeys(const List* pathkeys, const List* rtable);
extern void print_tl(const List* tlist, const List* rtable);
extern void print_slot(TupleTableSlot* slot);
extern char* ExprToString(const Node* expr, const List* rtable);

#endif /* PRINT_H */
