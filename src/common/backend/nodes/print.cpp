/* -------------------------------------------------------------------------
 *
 * print.cpp
 *	  various print routines (used mostly for debugging)
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/common/backend/nodes/print.cpp
 *
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/printtup.h"
#include "nodes/print.h"
#include "optimizer/clauses.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"

/*
 * print
 *	  print contents of Node to stdout
 */
void print(const void* obj)
{
    char* s = NULL;
    char* f = NULL;

    s = nodeToString(obj);
    f = format_node_dump(s);
    pfree_ext(s);
    printf("%s\n", f);
    fflush(stdout);
    pfree_ext(f);
}

/*
 * pprint
 *	  pretty-print contents of Node to stdout
 */
void pprint(const void* obj)
{
    char* s = NULL;
    char* f = NULL;

    s = nodeToString(obj);
    f = pretty_format_node_dump(s);
    pfree_ext(s);
    printf("%s\n", f);
    fflush(stdout);
    pfree_ext(f);
}

/*
 * elog_node_display
 *	  send pretty-printed contents of Node to postmaster log
 */
void elog_node_display(int lev, const char* title, const void* obj, bool pretty)
{
    char* s = NULL;
    char* f = NULL;

    s = nodeToString(obj);
    if (pretty) {
        f = pretty_format_node_dump(s);
    } else {
        f = format_node_dump(s);
    }
    pfree_ext(s);
    ereport(lev, (errmsg_internal("%s:", title), errdetail_internal("%s", f)));
    pfree_ext(f);
}

/*
 * Format a nodeToString output for display on a terminal.
 *
 * The result is a palloc'd string.
 *
 * This version just tries to break at whitespace.
 */
char* format_node_dump(const char* dump)
{
#define LINELEN 78
    char line[LINELEN + 1];
    StringInfoData str;
    int i;
    int j;
    int k;

    initStringInfo(&str);
    i = 0;
    for (;;) {
        for (j = 0; j < LINELEN && dump[i] != '\0'; i++, j++) {
            line[j] = dump[i];
        }
        if (dump[i] == '\0') {
            break;
        }
        if (dump[i] == ' ') {
            /* ok to break at adjacent space */
            i++;
        } else {
            for (k = j - 1; k > 0; k--) {
                if (line[k] == ' ') {
                    break;
                }
            }
            if (k > 0) {
                /* back up; will reprint all after space */
                i -= (j - k - 1);
                j = k;
            }
        }
        line[j] = '\0';
        appendStringInfo(&str, "%s\n", line);
    }
    if (j > 0) {
        line[j] = '\0';
        appendStringInfo(&str, "%s\n", line);
    }
    return str.data;
#undef LINELEN
}

/*
 * Format a nodeToString output for display on a terminal.
 *
 * The result is a palloc'd string.
 *
 * This version tries to indent intelligently.
 */
char* pretty_format_node_dump(const char* dump)
{
#define INDENTSTOP 3
#define MAXINDENT 60
#define LINELEN 78
    char line[LINELEN + 1];
    StringInfoData str;
    int indentLev;
    int indentDist;
    int i;
    int j;

    initStringInfo(&str);
    indentLev = 0;  /* logical indent level */
    indentDist = 0; /* physical indent distance */
    i = 0;
    for (;;) {
        for (j = 0; j < indentDist; j++) {
            line[j] = ' ';
        }
        for (; j < LINELEN && dump[i] != '\0'; i++, j++) {
            line[j] = dump[i];
            switch (line[j]) {
                case '}':
                    if (j != indentDist) {
                        /* print data before the } */
                        line[j] = '\0';
                        appendStringInfo(&str, "%s\n", line);
                    }
                    /* print the } at indentDist */
                    line[indentDist] = '}';
                    line[indentDist + 1] = '\0';
                    appendStringInfo(&str, "%s\n", line);
                    /* outdent */
                    if (indentLev > 0) {
                        indentLev--;
                        indentDist = Min(indentLev * INDENTSTOP, MAXINDENT);
                    }
                    j = indentDist - 1;
                    /* j will equal indentDist on next loop iteration */
                    /* suppress whitespace just after } */
                    while (dump[i + 1] == ' ') {
                        i++;
                    }
                    break;
                case ')':
                    /* force line break after ), unless another ) follows */
                    if (dump[i + 1] != ')') {
                        line[j + 1] = '\0';
                        format_debug_print_plan(line, indentDist, j);
                        appendStringInfo(&str, "%s\n", line);
                        j = indentDist - 1;
                        while (dump[i + 1] == ' ') {
                            i++;
                        }
                    }
                    break;
                case '{':
                    /* force line break before { */
                    if (j != indentDist) {
                        line[j] = '\0';
                        appendStringInfo(&str, "%s\n", line);
                    }
                    /* indent */
                    indentLev++;
                    indentDist = Min(indentLev * INDENTSTOP, MAXINDENT);
                    for (j = 0; j < indentDist; j++) {
                        line[j] = ' ';
                    }
                    line[j] = dump[i];
                    break;
                case ':':
                    /* force line break before : */
                    if (j != indentDist) {
                        line[j] = '\0';
                        format_debug_print_plan(line, indentDist, j);
                        appendStringInfo(&str, "%s\n", line);
                    }
                    j = indentDist;
                    line[j] = dump[i];
                    break;
                default:
                    break;
            }
        }
        line[j] = '\0';
        if (dump[i] == '\0') {
            break;
        }
        appendStringInfo(&str, "%s\n", line);
    }
    if (j > 0) {
        appendStringInfo(&str, "%s\n", line);
    }
    return str.data;
#undef INDENTSTOP
#undef MAXINDENT
#undef LINELEN
}

/*
 * format_debug_print_plan
 *    format plan contents for debug_print_plan parameter
 *    to handle En/Decryption info
 */
void format_debug_print_plan(char *force_line, int index, int length)
{
    if (u_sess->attr.attr_sql.Debug_print_plan) {
        char *result = NULL;
        char *encrypt = NULL;
        char *decrypt = NULL;
        char *format_str = NULL;
        errno_t rc = EOK;
        
        result = strstr(force_line, "sql_statement");
        if (result != NULL) {
            format_str = (char*)palloc0(length + 1);
            rc = memcpy_s(format_str, (length + 1), force_line, (length + 1));
            securec_check(rc, "\0", "\0");
            tolower(format_str);
            encrypt = strstr(format_str, "gs_encrypt_aes128");
            if (encrypt != NULL) {
                mask_position(force_line, index, length, "gs_encrypt_aes128\\");
            }
            encrypt = strstr(format_str, "gs_encrypt");
            if (encrypt != NULL) {
                mask_position(force_line, index, length, "gs_encrypt\\");
            }
            decrypt = strstr(format_str, "gs_decrypt_aes128");
            if (decrypt != NULL) {
                mask_position(force_line, index, length, "gs_decrypt_aes128\\");
            }
            decrypt = strstr(format_str, "gs_decrypt");
            if (decrypt != NULL) {
                mask_position(force_line, index, length, "gs_decrypt\\");
            }
            pfree_ext(format_str);
        }
    }
    return;
}

/*
 * mask_position
 *    find out En/Decryption info postion and add mask info
 */
void mask_position(char *cmp_line, int index, int length, const char* cmp_str)
{
    char *subchar = NULL;
    char *format_str = NULL;
    int remain_pos = 0;
    int scan_index = 0;
    int cmp_len = 0;
    errno_t rc = EOK;
    
    scan_index = index;
    cmp_len = strlen(cmp_str);
    subchar = (char*)palloc0(cmp_len);
    format_str = (char*)palloc0(length + 1);
    rc = memcpy_s(format_str, (length + 1), cmp_line, (length + 1));
    securec_check(rc, "\0", "\0");
    tolower(format_str);

    while (scan_index != length) {
        if (format_str[scan_index] == cmp_str[0]) {
            rc = memcpy_s(subchar, cmp_len, (format_str + scan_index), cmp_len);
            securec_check(rc, "\0", "\0");
            if (strncmp(subchar, cmp_str, cmp_len) == 0) {
                add_mask_policy(cmp_line, scan_index, length);
            }
            pfree_ext(subchar);
            pfree_ext(format_str);
            break;
        }
        scan_index++;
        remain_pos = length - scan_index;
        if (remain_pos < cmp_len) {
            pfree_ext(subchar);
            pfree_ext(format_str);
            break;
        }
    }
}

/*
 * add_mask_policy
 *   add char '*' as mask policy
 */
void add_mask_policy(char *cmp_line, int scan_index, int length)
{
    /* str_len is char "gs_encrypt_aes128\" length */
    const int str_len = 18;
    int index = scan_index + str_len;

    while (index <= length) {
        cmp_line[index] = '*';
        index++;
    }
}

/*
 * tolower
 *    tolower function to change char
 */
void tolower(char *tolower_query)
{
    /* to_lower is converting to lowercase number */
    const int to_lower = 32;
    char *ret = tolower_query;
    for (int i = 0; ret[i] != '\0'; i++) {
        if ((ret[i] >= 'A') && (ret[i] <= 'Z')) {
            ret[i] = ret[i] + to_lower;
        }
    }
}

/*
 * print_rt
 *	  print contents of range table
 */
void print_rt(const List* rtable)
{
    const ListCell* l = NULL;
    int i = 1;

    printf("resno\trefname  \trelid\tinFromCl\n");
    printf("-----\t---------\t-----\t--------\n");
    foreach (l, rtable) {
        RangeTblEntry* rte = (RangeTblEntry*)lfirst(l);

        switch (rte->rtekind) {
            case RTE_RELATION:
#ifdef PGXC
            case RTE_REMOTE_DUMMY:
#endif /* PGXC */
                printf("%d\t%s\t%u\t%c", i, rte->eref->aliasname, rte->relid, rte->relkind);
                break;
            case RTE_SUBQUERY:
                printf("%d\t%s\t[subquery]", i, rte->eref->aliasname);
                break;
            case RTE_JOIN:
                printf("%d\t%s\t[join]", i, rte->eref->aliasname);
                break;
            case RTE_FUNCTION:
                printf("%d\t%s\t[rangefunction]", i, rte->eref->aliasname);
                break;
            case RTE_VALUES:
                printf("%d\t%s\t[values list]", i, rte->eref->aliasname);
                break;
            case RTE_CTE:
                printf("%d\t%s\t[cte]", i, rte->eref->aliasname);
                break;
            case RTE_RESULT:
                printf("%d\t%s\t[result]", i, rte->eref->aliasname);
                break;
            default:
                printf("%d\t%s\t[unknown rtekind]", i, rte->eref->aliasname);
                break;
        }

        printf("\t%s\t%s\n", (rte->inh ? "inh" : ""), (rte->inFromCl ? "inFromCl" : ""));
        i++;
    }
}

/*
 * print_expr
 *	  print an expression
 */
char* ExprToString(const Node* expr, const List* rtable)
{
    StringInfoData buf;
    initStringInfo(&buf);

    if (expr == NULL) {
        appendStringInfoString(&buf, "<>");
        return buf.data;
    }

    if (IsA(expr, Var)) {
        const Var* var = (const Var*)expr;
        char* relname = NULL;
        char* attname = NULL;

        switch (var->varno) {
            case INNER_VAR:
                relname = "INNER";
                attname = pstrdup("?");
                break;
            case OUTER_VAR:
                relname = "OUTER";
                attname = pstrdup("?");
                break;
            case INDEX_VAR:
                relname = "INDEX";
                attname = pstrdup("?");
                break;
            default: {
                RangeTblEntry* rte = NULL;

                if (!(var->varno > 0 && (int)var->varno <= list_length(rtable))) {
                    ereport(ERROR, (errmodule(MOD_OPT), errmsg("varno is unvalid")));
                }
                rte = rt_fetch(var->varno, rtable);
                relname = rte->eref->aliasname;
                attname = get_rte_attribute_name(rte, var->varattno);
            } break;
        }
        appendStringInfo(&buf, "%s.%s", relname, attname);
        if (attname != NULL) {
            pfree_ext(attname);
        }
    } else if (IsA(expr, Const)) {
        const Const* c = (const Const*)expr;
        Oid typoutput;
        bool typIsVarlena = false;
        char* outputstr = NULL;

        if (c->constisnull) {
            appendStringInfoString(&buf, "NULL");
            return buf.data;
        }

        getTypeOutputInfo(c->consttype, &typoutput, &typIsVarlena);

        outputstr = OidOutputFunctionCall(typoutput, c->constvalue);
        appendStringInfo(&buf, "%s", outputstr);
        pfree_ext(outputstr);
    } else if (IsA(expr, OpExpr)) {
        const OpExpr* e = (const OpExpr*)expr;
        char* opname = NULL;
        char* expr = NULL;

        opname = get_opname(e->opno);
        if (list_length(e->args) > 1) {
            expr = ExprToString(get_leftop((const Expr*)e), rtable);
            appendStringInfoString(&buf, expr);
            pfree_ext(expr);

            appendStringInfo(&buf, " %s ", ((opname != NULL) ? opname : "(invalid operator)"));

            expr = ExprToString(get_rightop((const Expr*)e), rtable);
            appendStringInfoString(&buf, expr);
            pfree_ext(expr);
        } else {
            /* we print prefix and postfix ops the same... */
            appendStringInfo(&buf, "%s ", ((opname != NULL) ? opname : "(invalid operator)"));
            expr = ExprToString(get_leftop((const Expr*)e), rtable);
            appendStringInfoString(&buf, expr);
            pfree_ext(expr);
        }
    } else if (IsA(expr, FuncExpr)) {
        const FuncExpr* e = (const FuncExpr*)expr;
        char* funcname = NULL;
        ListCell* l = NULL;
        char* expr = NULL;

        funcname = get_func_name(e->funcid);
        appendStringInfo(&buf, "%s(", ((funcname != NULL) ? funcname : "(invalid function)"));
        foreach (l, e->args) {
            expr = ExprToString((Node*)lfirst(l), rtable);
            appendStringInfoString(&buf, expr);
            pfree_ext(expr);
            if (lnext(l)) {
                appendStringInfoChar(&buf, ',');
            }
        }
        appendStringInfoChar(&buf, ')');
    } else
        appendStringInfoString(&buf, "unknown expr");

    return buf.data;
}

/*
 * print_expr
 *	  print an expression
 */
void print_expr(const Node* expr, const List* rtable)
{
    printf("%s", ExprToString(expr, rtable));
}

/*
 * print_pathkeys -
 *	  pathkeys list of PathKeys
 */
void print_pathkeys(const List* pathkeys, const List* rtable)
{
    const ListCell* i = NULL;

    printf("(");
    foreach (i, pathkeys) {
        PathKey* pathkey = (PathKey*)lfirst(i);
        EquivalenceClass* eclass = pathkey->pk_eclass;
        ListCell* k = NULL;
        bool first = true;

        /* chase up, in case pathkey is non-canonical */
        while (eclass->ec_merged)
            eclass = eclass->ec_merged;

        printf("(");
        foreach (k, eclass->ec_members) {
            EquivalenceMember* mem = (EquivalenceMember*)lfirst(k);

            if (first) {
                first = false;
            } else {
                printf(", ");
            }
            print_expr((Node*)mem->em_expr, rtable);
        }
        printf(")");
        if (lnext(i)) {
            printf(", ");
        }
    }
    printf(")\n");
}

/*
 * print_tl
 *	  print targetlist in a more legible way.
 */
void print_tl(const List* tlist, const List* rtable)
{
    const ListCell* tl = NULL;

    printf("(\n");
    foreach (tl, tlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(tl);

        printf("\t%d %s\t", tle->resno, tle->resname ? tle->resname : "<null>");
        if (tle->ressortgroupref != 0) {
            printf("(%u):\t", tle->ressortgroupref);
        } else {
            printf("    :\t");
        }
        print_expr((Node*)tle->expr, rtable);
        printf("\n");
    }
    printf(")\n");
}

/*
 * print_slot
 *	  print out the tuple with the given TupleTableSlot
 */
void print_slot(TupleTableSlot* slot)
{
    if (TupIsNull(slot)) {
        printf("tuple is null.\n");
        return;
    }
    if (!slot->tts_tupleDescriptor) {
        printf("no tuple descriptor.\n");
        return;
    }

    debugtup(slot, NULL);
}
