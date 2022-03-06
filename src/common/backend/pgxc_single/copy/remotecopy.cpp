/* -------------------------------------------------------------------------
 *
 * remotecopy.c
 *		Implements an extension of COPY command for remote management
 *
 * Portions Copyright (c) 1996-2011, PostgreSQL Global Development Group
 * Portions Copyright (c) 2010-2012, Postgres-XC Development Group
 *
 *
 * IDENTIFICATION
 *		src/backend/pgxc/copy/remotecopy.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"
#include "knl/knl_variable.h"
#include "miscadmin.h"
#include "lib/stringinfo.h"
#include "optimizer/pgxcship.h"
#include "optimizer/planner.h"
#include "pgxc/pgxcnode.h"
#include "pgxc/remotecopy.h"
#include "rewrite/rewriteHandler.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/lsyscache.h"
#include "commands/copy.h"
#include "utils/elog.h"

static void RemoteCopy_QuoteStr(StringInfo query_buf, char* value);

/*
 * RemoteCopy_GetRelationLoc
 * Get relation node list based on COPY data involved. An empty list is
 * returned to caller if relation involved has no locator information
 * as it is the case of a system relation.
 */
void RemoteCopy_GetRelationLoc(RemoteCopyData* state, Relation rel, List* attnums)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else
    ExecNodes* exec_nodes = NULL;

    /*
     * If target table does not exists on nodes (e.g. system table)
     * the location info returned is NULL. This is the criteria, when
     * we need to run COPY on Coordinator
     */
    state->rel_loc = GetRelationLocInfo(RelationGetRelid(rel));

    if (state->rel_loc) {
        /*
         * Pick up one node only
         * This case corresponds to a replicated table with COPY TO
         *
         */
        exec_nodes = makeNode(ExecNodes);
        if (!state->is_from && IsRelationReplicated(state->rel_loc))
            exec_nodes->nodeList = GetPreferredReplicationNode(state->rel_loc->nodeList);
        else {
            /* All nodes necessary */
            exec_nodes->nodeList = list_concat(exec_nodes->nodeList, state->rel_loc->nodeList);
        }
    }

    state->idx_dist_by_col = NULL;
    if (state->rel_loc && state->rel_loc->partAttrNum != NULL) {
        /*
         * Find the column used as key for data distribution.
         * First scan attributes of tuple descriptor with the list
         * of attributes used in COPY if any list is specified.
         * If no list is specified, set this value to the one of
         * locator information.
         */
        if (attnums != NIL) {
            ListCell* cur = NULL;
            foreach (cur, state->rel_loc->partAttrNum) {
                int attnum = lfirst_int(cur);
                if (list_member_int(attnums, attnum)) {
                    state->idx_dist_by_col = lappend_int(state->idx_dist_by_col, attnum - 1);
                }
            }
        } else {
            ListCell* cell = NULL;
            AttrNumber num;
            foreach (cell, state->rel_loc->partAttrNum) {
                num = lfirst_int(cell);
                state->idx_dist_by_col = lappend_int(state->idx_dist_by_col, num - 1);
            }
        }
    }

    /* Then save obtained result */
    state->exec_nodes = exec_nodes;
#endif
}

/*
 * RemoteCopy_BuildStatement
 * Build a COPY query for remote management
 */
void RemoteCopy_BuildStatement(
    RemoteCopyData* state, Relation rel, RemoteCopyOptions* options, List* attnamelist, List* attnums)
{
    int attnum;
    TupleDesc tupDesc = RelationGetDescr(rel);

    /*
     * Build up query string for the Datanodes, it should match
     * to original string, but should have STDIN/STDOUT instead
     * of filename.
     */
    initStringInfo(&state->query_buf);
    appendStringInfoString(&state->query_buf, "COPY ");

    /*
     * The table name should be qualified, unless if the table is
     * a temporary one
     */
    if (RelationIsLocalTemp(rel))
        appendStringInfo(&state->query_buf, "%s", quote_identifier(RelationGetRelationName(rel)));
    else
        appendStringInfo(&state->query_buf,
            "%s",
            quote_qualified_identifier(get_namespace_name(RelationGetNamespace(rel)), RelationGetRelationName(rel)));

    if (attnamelist != NIL) {
        appendStringInfoString(&state->query_buf, " (");

        /*
         * For COPY FROM, we need to append unspecified attributes that have
         * default expressions associated.
         */
        if (state->is_from) {
            for (attnum = 1; attnum <= tupDesc->natts; attnum++) {
                /* Don't let dropped attributes go into the column list */
                if (tupDesc->attrs[attnum - 1]->attisdropped)
                    continue;

                if (!list_member_int(attnums, attnum)) {
                    /* Append only if the default expression is not shippable. */
                    Expr* defexpr = (Expr*)build_column_default(rel, attnum);
                    if (defexpr && ((!pgxc_is_expr_shippable(expression_planner(defexpr), NULL)) ||
                                       (list_member_int(state->idx_dist_by_col, attnum - 1)))) {
                        appendStringInfoString(
                            &state->query_buf, quote_identifier(NameStr(tupDesc->attrs[attnum - 1]->attname)));
                        appendStringInfoString(&state->query_buf, ", ");
                    }
                }
            }
        }

        ListCell* cell = NULL;
        foreach (cell, attnamelist) {
            appendStringInfoString(&state->query_buf, quote_identifier(strVal(lfirst(cell))));
            appendStringInfoString(&state->query_buf, ", ");
        }
        int blankPos = 1;
        int delimPos = 1;
        state->query_buf.data[state->query_buf.len - blankPos] = '\0';
        state->query_buf.data[state->query_buf.len - blankPos - delimPos] = '\0';
        state->query_buf.len -= (blankPos + delimPos);

        appendStringInfoChar(&state->query_buf, ')');
    }

    if (state->is_from)
        appendStringInfoString(&state->query_buf, " FROM STDIN");
    else
        appendStringInfoString(&state->query_buf, " TO STDOUT");

    if (options->rco_format == FORMAT_BINARY)
        appendStringInfoString(&state->query_buf, " BINARY");

    if (options->rco_oids)
        appendStringInfoString(&state->query_buf, " OIDS");

    if (options->rco_freeze)
        appendStringInfoString(&state->query_buf, " FREEZE");

    if (options->rco_without_escaping)
        appendStringInfoString(&state->query_buf, " WITHOUT ESCAPING ");

    if (options->rco_delim) {
        if ((options->rco_format != FORMAT_CSV && strcmp(options->rco_delim, "\t") != 0) ||
            (options->rco_format == FORMAT_CSV && strcmp(options->rco_delim, ",") != 0)) {
            appendStringInfoString(&state->query_buf, " DELIMITER AS ");
            RemoteCopy_QuoteStr(&state->query_buf, options->rco_delim);
        }
    }

    if (options->rco_null_print) {
        if ((options->rco_format != FORMAT_CSV && strcmp(options->rco_null_print, "\\N")) ||
            (options->rco_format == FORMAT_CSV && strcmp(options->rco_null_print, ""))) {
            appendStringInfoString(&state->query_buf, " NULL AS ");
            RemoteCopy_QuoteStr(&state->query_buf, options->rco_null_print);
        }
    }

    if (options->rco_format == FORMAT_CSV)
        appendStringInfoString(&state->query_buf, " CSV");
    else if (options->rco_format == FORMAT_FIXED) {
        FixFormatter* formatter = (FixFormatter*)(options->rco_formatter);
        FieldDesc* descs = formatter->fieldDesc;
        int nfields = formatter->nfield;

        appendStringInfoString(&state->query_buf, " FORMATTER( ");
        for (int i = 0; i < nfields; i++) {
            char* fieldname = descs[i].fieldname;
            appendStringInfo(&state->query_buf, "%s(%d,%d)", fieldname, descs[i].fieldPos, descs[i].fieldSize);
            if (i != nfields - 1)
                appendStringInfoString(&state->query_buf, ",");
        }
        appendStringInfoString(&state->query_buf, " ) ");
        appendStringInfoString(&state->query_buf, "FIXED");
    }

    if (options->rco_eol_type == EOL_CRNL)
        appendStringInfoString(&state->query_buf, " eol '0x0D0A'");
    else if (options->rco_eol_type == EOL_CR)
        appendStringInfoString(&state->query_buf, " eol '0x0D'");
    else if (options->rco_eol_type == EOL_UD) {
        appendStringInfoString(&state->query_buf, " eol ");
        RemoteCopy_QuoteStr(&state->query_buf, options->rco_eol);
    }

    /*
     * It is not necessary to send the HEADER part to Datanodes.
     * Sending data is sufficient.
     */
    if (options->rco_quote && options->rco_quote[0] != '"') {
        appendStringInfoString(&state->query_buf, " QUOTE AS ");
        RemoteCopy_QuoteStr(&state->query_buf, options->rco_quote);
    }

    if (options->rco_escape && options->rco_quote && options->rco_escape[0] != options->rco_quote[0]) {
        appendStringInfoString(&state->query_buf, " ESCAPE AS ");
        RemoteCopy_QuoteStr(&state->query_buf, options->rco_escape);
    }

    if (options->rco_force_quote) {
        ListCell* cell = NULL;
        ListCell* prev = NULL;
        appendStringInfoString(&state->query_buf, " FORCE QUOTE ");
        foreach (cell, options->rco_force_quote) {
            if (prev != NULL)
                appendStringInfoString(&state->query_buf, ", ");
            appendStringInfoString(&state->query_buf, quote_identifier(strVal(lfirst(cell))));
            prev = cell;
        }
    }

    if (options->rco_force_notnull) {
        ListCell* cell = NULL;
        ListCell* prev = NULL;
        appendStringInfoString(&state->query_buf, " FORCE NOT NULL ");
        foreach (cell, options->rco_force_notnull) {
            if (prev != NULL)
                appendStringInfoString(&state->query_buf, ", ");
            appendStringInfoString(&state->query_buf, quote_identifier(strVal(lfirst(cell))));
            prev = cell;
        }
    }

    if (options->rco_date_format) {
        appendStringInfoString(&state->query_buf, " date_format ");
        RemoteCopy_QuoteStr(&state->query_buf, options->rco_date_format);
    }

    if (options->rco_time_format) {
        appendStringInfoString(&state->query_buf, " time_format ");
        RemoteCopy_QuoteStr(&state->query_buf, options->rco_time_format);
    }

    if (options->rco_timestamp_format) {
        appendStringInfoString(&state->query_buf, " timestamp_format ");
        RemoteCopy_QuoteStr(&state->query_buf, options->rco_timestamp_format);
    }

    if (options->rco_smalldatetime_format) {
        appendStringInfoString(&state->query_buf, " smalldatetime_format ");
        RemoteCopy_QuoteStr(&state->query_buf, options->rco_smalldatetime_format);
    }

    if (options->rco_compatible_illegal_chars)
        appendStringInfoString(&state->query_buf, " COMPATIBLE_ILLEGAL_CHARS");

    if (options->rco_ignore_extra_data)
        appendStringInfoString(&state->query_buf, " IGNORE_EXTRA_DATA");

    if (options->rco_fill_missing_fields)
        appendStringInfoString(&state->query_buf, " FILL_MISSING_FIELDS");

    if (options->transform_query_string) {
        appendStringInfoChar(&state->query_buf, ' ');
        appendStringInfoString(&state->query_buf, options->transform_query_string);
    }
}

/*
 * Build a default set for RemoteCopyOptions
 */
RemoteCopyOptions* makeRemoteCopyOptions(void)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return NULL;
#else
    RemoteCopyOptions* res = (RemoteCopyOptions*)palloc(sizeof(RemoteCopyOptions));
    res->rco_format = FORMAT_UNKNOWN;
    res->rco_oids = false;
    res->rco_without_escaping = false;
    res->rco_delim = NULL;
    res->rco_null_print = NULL;
    res->rco_quote = NULL;
    res->rco_escape = NULL;
    res->rco_eol = NULL;
    res->rco_force_quote = NIL;
    res->rco_force_notnull = NIL;
    res->rco_eol_type = EOL_NL;
    res->rco_date_format = NULL;
    res->rco_time_format = NULL;
    res->rco_timestamp_format = NULL;
    res->rco_smalldatetime_format = NULL;
    res->rco_compatible_illegal_chars = false;
    res->rco_ignore_extra_data = false;
    res->rco_fill_missing_fields = false;
    return res;
#endif
}

/*
 * FreeRemoteCopyOptions
 * Free remote COPY options structure
 */
void FreeRemoteCopyOptions(RemoteCopyOptions* options)
{
    /* Leave if nothing */
    if (options == NULL)
        return;

    /* Free field by field */
    if (options->rco_delim)
        pfree(options->rco_delim);
    if (options->rco_null_print)
        pfree(options->rco_null_print);
    if (options->rco_quote)
        pfree(options->rco_quote);
    if (options->rco_escape)
        pfree(options->rco_escape);
    if (options->rco_eol)
        pfree(options->rco_eol);
    if (options->rco_force_quote)
        list_free(options->rco_force_quote);
    if (options->rco_force_notnull)
        list_free(options->rco_force_notnull);

    /* Then finish the work */
    pfree(options);
}

/*
 * FreeRemoteCopyData
 * Free remote COPY state data structure
 */
void FreeRemoteCopyData(RemoteCopyData* state)
{
#ifndef ENABLE_MULTIPLE_NODES
    Assert(false);
    DISTRIBUTED_FEATURE_NOT_SUPPORTED();
    return;
#else
    /* Leave if nothing */
    if (state == NULL)
        return;

    if (state->connections)
        pfree(state->connections);
    if (state->query_buf.data)
        pfree(state->query_buf.data);
    FreeRelationLocInfo(state->rel_loc);
    pfree(state);
#endif
}

#define APPENDSOFAR(query_buf, start, current) \
    if ((current) > (start))                   \
    appendBinaryStringInfo((query_buf), (start), (current) - (start))

/*
 * RemoteCopy_QuoteStr
 * Append quoted value to the query buffer. Value is escaped if needed
 * When rewriting query to be sent down to nodes we should escape special
 * characters, that may present in the value. The characters are backslash(\)
 * and single quote ('). These characters are escaped by doubling. We do not
 * have to escape characters like \t, \v, \b, etc. because Datanode interprets
 * them properly.
 * We use E'...' syntax for literals containing backslashes.
 */
static void RemoteCopy_QuoteStr(StringInfo query_buf, char* value)
{
    char* start = value;
    char* current = value;
    char c;
    bool has_backslash = (strchr(value, '\\') != NULL);
    if (has_backslash)
        appendStringInfoChar(query_buf, 'E');

    appendStringInfoChar(query_buf, '\'');

    while ((c = *current) != '\0') {
        switch (c) {
            case '\\':
            case '\'':
                APPENDSOFAR(query_buf, start, current);
                /* Double current */
                appendStringInfoChar(query_buf, c);
                /* Second current will be appended next time */
                start = current;
                /* fallthru */
            default:
                current++;
        }
    }
    APPENDSOFAR(query_buf, start, current);
    appendStringInfoChar(query_buf, '\'');
}
