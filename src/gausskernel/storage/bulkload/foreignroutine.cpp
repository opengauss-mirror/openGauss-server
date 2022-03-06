/*
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 2010-2011, PostgreSQL Global Development Group
 *
 *
 *  foreignroutine.cpp
 *
 * IDENTIFICATION
 *        src/gausskernel/storage/bulkload/foreignroutine.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <vector>
#include <ctype.h>
#include <string.h>

#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "bulkload/dist_fdw.h"
#include "bulkload/foreignroutine.h"
#include "bulkload/utils.h"
#include "catalog/namespace.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "catalog/pgxc_node.h"
#include "cipher.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "foreign/dummyserver.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "pgxc/nodemgr.h"
#include "pgxc/pgxc.h"
#include "pgxc/pgxcnode.h"
#include "parser/parsetree.h"
#include "storage/smgr/fd.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/formatting.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"

using std::vector;

#define DIST_OBS_CHUNKSIZE (16 * 1024 * 1024)

extern IdGen gt_sessionId;

extern void distGetOptions(Oid foreigntableid, char **locaionts, List **other_options);
extern bool check_selective_binary_conversion(RelOptInfo *baserel, Oid foreigntableid, List **columns);
extern List *assignTaskToDataNode(List *urllist, ImportMode mode, List *nodeList, int dop,
                                  DistImportPlanState *planstate, int64 *fileNum = NULL);

extern void InitDistImport(DistImportExecutionState *importstate, Relation rel, const char *filename, List *attnamelist,
                           List *options, List *totalTask);
void EndDistImport(DistImportExecutionState *importstate);

extern bool is_valid_location(const char *location);
extern bool TrySaveImportError(DistImportExecutionState *importState, ForeignScanState *node);

/*
 * all stuffs used for bulkload, which comments being boundary.
 */
extern void SyncBulkloadStates(CopyState cstate);
extern void CleanBulkloadStates();
extern char *TrimStr(const char *str);
extern Oid GetUserId(void);

void checkGSOBSPrefixNoAllowRegionOption(bool hasRegion, List *LocationOPt);

#define ILLEGAL_CHARACTERS_ERR_THRESHOLD 10

/*
 * all stuffs used for bulkload(end).
 */
static void estimate_costs(PlannerInfo *root, const RelOptInfo *baserel, const DistImportPlanState *fdw_private, Cost *startup_cost,
                           Cost *total_cost);
static void estimate_size(PlannerInfo *root, RelOptInfo *baserel, DistImportPlanState *fdw_private)
{
    struct stat stat_buf;
    BlockNumber pages;
    double ntuples;
    double nrows;

    /*
     * Estimate the number of tuples in the file.
     */
    if (baserel->pages > 0) {
        /*
         * We have # of pages and # of tuples from pg_class (that is, from a
         * previous ANALYZE), so compute a tuples-per-page estimate and scale
         * that by the current file size.
         */
        ntuples = clamp_row_est(baserel->tuples);
        fdw_private->pages = (BlockNumber)baserel->pages;
    } else {
        /*
         * Get size of the file.  It might not be there at plan time, though, in
         * which case we have to use a default estimate.
         */
        stat_buf.st_size = 10 * BLCKSZ;

        /*
         * Convert size to pages for use in I/O cost estimate later.
         */
        pages = (BlockNumber)((stat_buf.st_size + (BLCKSZ - 1)) / BLCKSZ);
        if (pages < 1)
            pages = 1;
        fdw_private->pages = pages;

        /*
         * Otherwise we have to fake it.  We back into this estimate using the
         * planner's idea of the relation width; which is bogus if not all
         * columns are being read, not to mention that the text representation
         * of a row probably isn't the same size as its internal
         * representation.  Possibly we could do something better, but the
         * real answer to anyone who complains is "ANALYZE" ...
         */
        double tuple_width;

        tuple_width = MAXALIGN((intptr_t)baserel->width) + MAXALIGN(sizeof(HeapTupleHeaderData));
        ntuples = clamp_row_est((double)stat_buf.st_size / (double)tuple_width);
        baserel->tuples = ntuples;
    }
    fdw_private->ntuples = ntuples;

    /*
     * Now estimate the number of rows returned by the scan after applying the
     * baserestrictinfo quals.
     */
    nrows = ntuples * clauselist_selectivity(root, baserel->baserestrictinfo, 0, JOIN_INNER, NULL);

    nrows = clamp_row_est(nrows);

    /* Save the output-rows estimate for the planner */
    baserel->rows = nrows;
}

static void estimate_costs(PlannerInfo *root, const RelOptInfo *baserel, const DistImportPlanState *fdw_private, Cost *startup_cost,
                           Cost *total_cost)
{
    BlockNumber pages = fdw_private->pages;
    double ntuples = fdw_private->ntuples;
    Cost run_cost = 0;
    Cost cpu_per_tuple;

    /*
     * We estimate costs almost the same way as cost_seqscan(), thus assuming
     * that I/O costs are equivalent to a regular table file of the same size.
     * However, we take per-tuple CPU costs as 10x of a seqscan, to account
     * for the cost of parsing records.
     */
    run_cost += u_sess->attr.attr_sql.seq_page_cost * pages;

    *startup_cost = baserel->baserestrictcost.startup;
    cpu_per_tuple = u_sess->attr.attr_sql.cpu_tuple_cost * 10 + baserel->baserestrictcost.per_tuple;
    run_cost += cpu_per_tuple * ntuples;
    *total_cost = *startup_cost + run_cost;
}

void UntransformFormatterOption(DefElem *def)
{
    char *s = pstrdup(strVal(def->arg));
    List *entries = NIL;
    List *result = NIL;
    ListCell *lc = NULL;
    char *p = NULL;
    char *token = NULL;

    // get position string
    token = strtok_r(s, ".", &p);
    while (token != NULL) {
        entries = lappend(entries, token);
        token = strtok_r(NULL, ".", &p);
    }

    foreach (lc, entries) {
        Position *pos = (Position *)palloc0(sizeof(Position));
        char *end = NULL;

        s = (char *)lfirst(lc);

        // Get column name
        p = strchr(s, '(');
        if (p == NULL)
            ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                            errmsg("Invalid formatter options \"%s\"", (char *)lfirst(lc))));
        pos->colname = pnstrdup(s, (Size)(p - s));

        // Get begin offset
        s = p + 1;
        p = strchr(s, ',');
        if (p == NULL)
            ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                            errmsg("Invalid formatter options \"%s\"", (char *)lfirst(lc))));
        s = pnstrdup(s, p - s);
        pos->position = (int)strtol(s, &end, 10);
        if ((end == NULL) || (*end != '\0'))
            ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                            errmsg("Invalid formatter options \"%s\"", (char *)lfirst(lc))));
        pfree(s);

        // Get length of field
        s = p + 1;
        p = strchr(s, ')');
        if (p == NULL)
            ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                            errmsg("Invalid formatter options \"%s\"", (char *)lfirst(lc))));
        s = pnstrdup(s, p - s);
        pos->fixedlen = (int)strtol(s, &end, 10);
        if ((end == NULL) || *end != '\0')
            ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                            errmsg("Invalid formatter options \"%s\"", (char *)lfirst(lc))));
        pfree(s);
        result = lappend(result, pos);
    }
    list_free_ext(entries);
    def->arg = (Node *)result;
}

/* URI protocols set about LOCATION option */
static const char *URI_protocols[] = {
    /* Must local prefix be the first */
    "file",  /* => LOCAL_PREFIX */
    "gsfs",  /* => GSFS_PREFIX */
    "gsfss", /* => GSFSS_PREFIX */
    "gsobs", /* => GSOBS_PREFIX */
    "obs",   /* => OBS_PREFIX */
    "roach"  /* => ROACH_PREFIX */
};
static const int URI_protocols_num = sizeof(URI_protocols) / sizeof(URI_protocols[0]);

/*
 * @Description: caller must make sure this protocol is valid.
 *   this functin will search this protocol from global maps, and
 *   return its index/position.
 * @IN protocol: protocol string
 * @Return: index within global protocol map
 * @See also:
 */
static int search_existing_procotols(const char *protocol)
{
    int i = 0;
    for (; i < URI_protocols_num; i++) {
        if (0 == strcmp(protocol, URI_protocols[i])) {
            break;
        }
    }
    Assert(i >= 0 && i <= URI_protocols_num - 1);
    return i;
}

static bool ProtocolHasConflict(const GDSUri &tmpuri, int first_protocol_idx)
{
    /* conflict: the first is not LOCAL_PREFIX but the later one is */
    if (tmpuri.m_protocol == NULL && first_protocol_idx != 0) {
        return true;
    }

    /* conflict: the remaining are different from the first */
    if (tmpuri.m_protocol != NULL && strcmp(tmpuri.m_protocol, URI_protocols[first_protocol_idx]) != 0) {
        return true;
    }
    return false;
}

/*
 * @Description: all the locations within the same foreign table should
 *    use the same protocol. if not so, report error message.
 * @IN lcs: LOCATION options list
 * @See also:
 */
static void VerifyLocations(const List *lcs)
{
    ListCell *lc = NULL;
    char *first = strVal(lfirst(list_head(lcs)));
    int first_protocol_idx = -1;

    /* check the first location validition ahead */
    if (!is_valid_location(first)) {
        ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("location \"%s\" is invalid", first)));
    }

    GDSUri uri;
    uri.Parse(first);
    if (uri.m_protocol) {
        first_protocol_idx = search_existing_procotols(uri.m_protocol);
    } else {
        /* at default it's a local protocol */
        first_protocol_idx = 0;
    }
    Assert(first_protocol_idx >= 0 && first_protocol_idx <= URI_protocols_num - 1);

    foreach (lc, lcs) {
        char *str = strVal(lfirst(lc));
        GDSUri tmpuri;
        tmpuri.Parse(str);
        if (ProtocolHasConflict(tmpuri, first_protocol_idx)) {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("locations can not use different protocols")));
        }
    }
}

/* check whether duplicated locations exist or not for gds import/export
 *
 *	@param lcs location options list
 *	@return void
 */
static void CheckDupLocations(const List *lcs)
{
    ListCell *lc = NULL;
    GDSUri *gds_uri = NULL;
    List *gds_uri_list = NIL;
    ListCell *gds_uri_cell = NULL;
    bool is_duplicated = false;
    char *location = NULL;
    char *dup_location = NULL;

    foreach (lc, lcs) {
        location = strVal(lfirst(lc));
        gds_uri = New(CurrentMemoryContext) GDSUri;
        gds_uri->Parse(location);

        /* ignore invalid location info */
        if ((gds_uri->m_protocol == NULL) || (gds_uri->m_host == NULL) || (gds_uri->m_port == -1)) {
            delete gds_uri;
            continue;
        }

        /*
         * check duplicated gds uri info
         */
        foreach (gds_uri_cell, gds_uri_list) {
            GDSUri *uri_in_list = (GDSUri *)lfirst(gds_uri_cell);
            if ((uri_in_list->m_port == gds_uri->m_port) && (!strcmp(uri_in_list->m_host, gds_uri->m_host))) {
                gds_uri_list = lappend(gds_uri_list, gds_uri);
                dup_location = pstrdup(location);
                is_duplicated = true;
                goto DesErr;
            }
        }

        gds_uri_list = lappend(gds_uri_list, gds_uri);
    }

DesErr:
    foreach (gds_uri_cell, gds_uri_list) {
        GDSUri *uri_in_list = (GDSUri *)lfirst(gds_uri_cell);
        delete uri_in_list;
    }

    if (is_duplicated) {
        ereport(ERROR,
                (errcode(ERRCODE_OPERATE_INVALID_PARAM), errmsg("duplicated URL \"%s\" in LOCATION", dup_location)));
    }
}

static void VerifyFileHeader(const char *path)
{
    GDSUri uri;

    uri.Parse(path);
    if (uri.m_protocol != NULL) {
        if (uri.m_path == NULL || uri.m_path[0] != '/') {
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("invalid file header location \"%s\"", uri.m_uri)));
        }
    }
}

static void VerifyFilenamePrefix(const char *name, bool isExport)
{
    Assert(PointerIsValid(name));

    if (!isExport) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                        errmsg("out_filename_prefix is only allowed in write-only foreign tables")));
    }

    if (strlen(name) == 0) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM), errmsg("out_filename_prefix should not be empty")));
    }

    const char illegalChars[16] = { '/', '?', '*', ':', '|', '\\', '<', '>', '@', '#', '$', '&', '(', ')', '+', '-' };

    for (int i = 0; i < 16; i++) {
        if (strchr(name, illegalChars[i]) != NULL) {
            ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                            errmsg("out_filename_prefix is not supposed to contain \"%c\"", illegalChars[i])));
        }
    }

    const char *illegalStrings[24] = {
        "con",  "aux",  "nul",  "prn",  "com0", "com1", "com2", "com3", "com4", "com5", "com6", "com7",
        "com8", "com9", "lpt0", "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9"
    };
    for (int i = 0; i < 24; i++) {
        if (strcmp(name, illegalStrings[i]) == 0) {
            ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                            errmsg("out_filename_prefix is not supposed to be \"%s\"", illegalStrings[i])));
        }
    }

    for (const char *c = name; *c; c++) {
        if (!((isalnum(*c) || (*c == '_')))) {
            ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                            errmsg("Only alphanumeric characters and \'_\' is allowed in out_filename_prefix option")));
        }
    }
}

static void VerifyFixAlignment(const char *alignment, bool isExport, FileFormat format)
{
    Assert(PointerIsValid(alignment));
    if (!isExport) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                        errmsg("out_fix_alignment is only allowed in write-only foreign tables")));
    }

    if (format != FORMAT_FIXED) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                        errmsg("out_fix_alignment is only allowed with fixed format")));
    }

    if (!(((strcmp(alignment, "align_left") == 0) || (strcmp(alignment, "align_right") == 0)))) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                        errmsg("Only \"align_left\" and \"align_right\" is allowed in out_fix_alignment option")));
    }
}

static bool IsValidUserDefineName(const char *input)
{
    char c = input[0];
    /* The first character id numbers or dollar */
    if ((c >= '1' && c <= '9') || c == '$') {
        return false;
    }

    int len = (int)strlen(input);
    for (int i = 0; i < len; i++) {
        c = input[i];
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '$' ||
            c == '.') {
            continue;
        } else {
            return false;
        }
    }
    return true;
}

/**
 * @Description: when location prefix is gsobs, do not allow to set the region
 * option.
 * @in LocationOPt, the location option list to be given.
 * @return none.
 */
void checkGSOBSPrefixNoAllowRegionOption(bool hasRegion, List *LocationOPt)
{
    if (hasRegion == false) {
        return;
    }
    char *firstLocation = strVal(lfirst(list_head(LocationOPt)));
    char *tmpLocation = TrimStr(firstLocation);

    if (0 == pg_strncasecmp(tmpLocation, GSOBS_PREFIX, strlen(GSOBS_PREFIX))) {
        pfree(tmpLocation);
        ereport(ERROR, (errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED), errmodule(MOD_OBS),
                        errmsg("Do not allow to set region option when the \"gsobs\" prefix is specified"
                               " for the location option.")));
    }

    pfree(tmpLocation);
}

void processOBSNoAllowOptions(bool specifyMode, const char *fileheader,
                              const char *OutputFilenamePrefix, const char *OutputFixAlignment, bool doLogRemote)
{
    if (specifyMode) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                        errmsg("OBS foreign table does not support '%s' option", optMode)));
    } else if (fileheader != NULL) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                        errmsg("OBS foreign table does not support '%s' option", optFileHeader)));
    } else if (OutputFilenamePrefix != NULL) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                        errmsg("OBS foreign table does not support '%s' option", optOutputFilePrefix)));
    } else if (OutputFixAlignment != NULL) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                        errmsg("OBS foreign table does not support '%s' option", optOutputFixAlignment)));
    } else if (doLogRemote) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                        errmsg("OBS foreign table does not support remote_log option")));
    }
}

void processOBSHaveToOptions(const char *accessKeyStr, const char *secretAccessKeyStr,
                             FileFormat format, const char *encryptStr, const char *chunksizeStr, bool writeOnly)
{
    // default FORMAT_UNKNOWN  value is TEXT
    if (format == FORMAT_UNKNOWN) {
        format = FORMAT_TEXT;
    }

    if (accessKeyStr == NULL) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                        errmsg("OBS foreign table have to specify '%s' option", optAccessKey)));
    } else if (secretAccessKeyStr == NULL) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                        errmsg("OBS foreign table have to specify '%s' option", optSecretAccessKey)));
    }

    /* Check valid option for format on OBS table */
    if (format != FORMAT_TEXT && format != FORMAT_CSV) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                        (errmsg("This OBS foreign table only supports CSV/TEXT format"))));
    }

    /* Check valid option for encrypt on OBS table */
    if (encryptStr != NULL && (pg_strcasecmp(encryptStr, "on") != 0 && pg_strcasecmp(encryptStr, "off") != 0)) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                        (errmsg("Invalid 'encrypt' option value '%s' for OBS foreign table", encryptStr))));
    }

    if (chunksizeStr != NULL) {
        int chunksize = 0;

        /* Error-out invalid numeric value */
        if (!parse_int(chunksizeStr, &chunksize, 0, NULL)) {
            ereport(ERROR,
                    (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                     (errmsg("Invalid 'chunksize' option value '%s', only numeric value can be set", chunksizeStr))));
        }

        /* Error-out unallowed rage */
        if (chunksize < 8 || chunksize > 512) {
            ereport(ERROR,
                    (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                     (errmsg("Invalid 'chunksize' option value '%s' for OBS Read-Only table, valid range [8, 512] in MB",
                             chunksizeStr))));
        } else if (writeOnly) {
            ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                            (errmsg("Option 'chunksize' is not allowed in OBS write-only table"))));
        }
    }
}

void processOBSLocationOptions(bool writeOnly, List *source)
{
    if (writeOnly) {
        /* Do write only table verification */
        if (list_length(source) > 1) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("can not specify multiple locations")));
        }
    } else {
        /* check the duplicate locations of OBS foreign table */
        ListCell *lc = NULL;
        bool is_duplicated = false;
        char *location = NULL;
        char *temp_location = NULL;
        List *temp_location_list = NIL;
        ListCell *temp_location_cell = NULL;

        char *hostname = NULL;
        char *bucket = NULL;
        char *prefix = NULL;

        foreach (lc, source) {
            location = strVal(lfirst(lc));
            temp_location = pstrdup(location);

            /* Check hostname, bucket is vaild. Parse uri into hostname, bucket, prefix */
            FetchUrlProperties(temp_location, &hostname, &bucket, &prefix);
            Assert(hostname && bucket && prefix);

            pfree_ext(hostname);
            pfree_ext(bucket);
            pfree_ext(prefix);

            /*
             * check duplicated locations
             */
            foreach (temp_location_cell, temp_location_list) {
                char *location_in_list = strVal(lfirst(temp_location_cell));
                if (strlen(location_in_list) == strlen(location)) {
                    if (strncmp(location, location_in_list, strlen(location_in_list)) == 0) {
                        is_duplicated = true;
                        goto DesErr;
                    }
                }
            }
            temp_location_list = lappend(temp_location_list, makeString(temp_location));
        }

DesErr:
        foreach (temp_location_cell, temp_location_list) {
            char *tempstr = strVal(lfirst(temp_location_cell));
            pfree(tempstr);
        }

        if (is_duplicated)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("duplicated URL \"%s\" in LOCATION", location)));
    }
}

void processNoneOBSOptions(const char *chunksizeStr, const char *encryptStr,
                           const char *accessKeyStr, const char *secretAccessKeyStr, bool hasRegion)
{
    if (chunksizeStr != NULL) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                        errmsg("None OBS foreign table does not support '%s' option", optChunkSize)));
    } else if (encryptStr != NULL) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                        errmsg("None OBS foreign table does not support '%s' option", optEncrypt)));
    } else if (accessKeyStr != NULL) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                        errmsg("None OBS foreign table does not support '%s' option", optAccessKey)));
    } else if (secretAccessKeyStr != NULL) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                        errmsg("None OBS foreign table does not support '%s' option", optSecretAccessKey)));
    } else if (hasRegion) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                        errmsg("None OBS foreign table does not support '%s' option", OPTION_NAME_REGION)));
    }
}

void ProcessDistImportOptions(DistImportPlanState *planstate, List *options, bool isPropagateToFE, bool isValidate)
{
    ListCell *lc = NULL;
    char *locations = NULL;
    FileFormat format = FORMAT_UNKNOWN;
    bool writeOnlySpecified = false;
    bool specifyFillMissing = false;
    bool specifyIgnoreExtraData = false;
    /*
     * the flag used to indicate whether bulkload compatible illegal chars option exists or not.
     */
    bool specifyCompatibleIllegalChars = false;
    /*
     * the flag used to indicate whether bulkload datetime format options exists or not.
     */
    bool specifyDateFormat = false;
    bool specifyTimeFormat = false;
    bool specifyTimestampFormat = false;
    bool specifySmalldatetimeFormat = false;
    bool specifyMode = false;
    bool rejectLimitSpecified = false;
    List *rmList = NIL;
    char *fileheader = NULL;
    char *OutputFilenamePrefix = NULL;
    char *OutputFixAlignment = NULL;
    bool hasHeader = false;
    bool hasRegion = false;

    /* validate OBS source */
    char *chunksizeStr = NULL;
    char *encryptStr = NULL;
    char *accessKeyStr = NULL;
    char *secretAccessKeyStr = NULL;

    int force_fix_width = 0;

    if (planstate == NULL)
        planstate = (DistImportPlanState *)palloc0(sizeof(DistImportPlanState));

    planstate->fileEncoding = -1;

    foreach (lc, options) {
        DefElem *def = (DefElem *)lfirst(lc);
        if (pg_strcasecmp(def->defname, optChunkSize) == 0) {
            chunksizeStr = defGetString(def);
        } else if (pg_strcasecmp(def->defname, optEncrypt) == 0) {
            encryptStr = defGetString(def);
        } else if (pg_strcasecmp(def->defname, optAccessKey) == 0) {
            accessKeyStr = defGetString(def);
        } else if (pg_strcasecmp(def->defname, optSecretAccessKey) == 0) {
            secretAccessKeyStr = defGetString(def);
        } else if (pg_strcasecmp(def->defname, optLocation) == 0) {
            locations = defGetString(def);
            planstate->filename = pstrdup(locations);
            planstate->source = DeserializeLocations(locations);
        } else if (pg_strcasecmp(def->defname, OPTION_NAME_REGION) == 0) {
            hasRegion = true;
        } else if (pg_strcasecmp(def->defname, optMode) == 0) {
            if (strcasecmp(strVal(def->arg), "normal") == 0)
                planstate->mode = MODE_NORMAL;
            else if (strcasecmp(strVal(def->arg), "shared") == 0)
                planstate->mode = MODE_SHARED;
            else if (strcasecmp(strVal(def->arg), "private") == 0)
                planstate->mode = MODE_PRIVATE;
            else
                ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM),
                                errmsg("Loading mode \"%s\" not recognized", strVal(def->arg))));
            specifyMode = true;
            rmList = lappend(rmList, def);
        } else if (pg_strcasecmp(def->defname, optRejectLimit) == 0) {
            if (rejectLimitSpecified)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            if (pg_strcasecmp(defGetString(def), "unlimited") == 0)
                planstate->rejectLimit = REJECT_UNLIMITED;
            else {
                char *value = defGetString(def);
                int limit = pg_strtoint32(value);
                planstate->rejectLimit = (limit > 0 ? limit : 0);
            }
            rejectLimitSpecified = true;
            rmList = lappend(rmList, def);
        } else if (pg_strcasecmp(def->defname, optErrorRel) == 0) {
            if (planstate->errorName != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            planstate->errorName = defGetString(def);
            rmList = lappend(rmList, def);
        } else if (pg_strcasecmp(def->defname, optWriteOnly) == 0) {
            if (writeOnlySpecified)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            writeOnlySpecified = true;
            planstate->writeOnly = defGetBoolean(def);
            rmList = lappend(rmList, def);
        } else if (pg_strcasecmp(def->defname, optEncoding) == 0) {
            // We just get the encoding here. Do not remove it from the options list.
            if (planstate->fileEncoding >= 0)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            planstate->fileEncoding = pg_char_to_encoding(defGetString(def));
            if (planstate->fileEncoding < 0)
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("argument to option \"%s\" must be a valid encoding name", def->defname)));
        } else if (pg_strcasecmp(def->defname, optFormat) == 0) {
            char *fmt = defGetString(def);

            if (strcasecmp(fmt, "text") == 0)
                format = FORMAT_TEXT;
            else if (strcasecmp(fmt, "csv") == 0)
                format = FORMAT_CSV;
            else if (strcasecmp(fmt, "fixed") == 0)
                format = FORMAT_FIXED;
            else
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("LOAD format \"%s\" not recognized", fmt)));
        } else if (pg_strcasecmp(def->defname, optFormatter) == 0)
            UntransformFormatterOption(def);
        else if (pg_strcasecmp(def->defname, optFileHeader) == 0) {
            if (fileheader != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            fileheader = defGetString(def);
        } else if (pg_strcasecmp(def->defname, optOutputFilePrefix) == 0) {
            if (OutputFilenamePrefix != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            OutputFilenamePrefix = defGetString(def);
        } else if (pg_strcasecmp(def->defname, optOutputFixAlignment) == 0) {
            if (OutputFixAlignment != NULL)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            OutputFixAlignment = defGetString(def);
        } else if (pg_strcasecmp(def->defname, optHeader) == 0) {
            hasHeader = defGetBoolean(def);
        } else if (pg_strcasecmp(def->defname, optLogRemote) == 0) {
            if (planstate->doLogRemote)
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("conflicting or redundant options")));
            planstate->remoteName = defGetString(def);
            planstate->doLogRemote = true;
            rmList = lappend(rmList, def);
        } else if (pg_strcasecmp(def->defname, optFillMissFields) == 0)
            specifyFillMissing = true;
        else if (pg_strcasecmp(def->defname, optIgnoreExtraData) == 0)
            specifyIgnoreExtraData = true;
        /*
         * check whether bulkload compatible illegal chars option exists or not.
         */
        else if (pg_strcasecmp(def->defname, optCompatibleIllegalChars) == 0)
            specifyCompatibleIllegalChars = true;
        /*
         * check whether bulkload datetime format options exists or not.
         */
        else if (pg_strcasecmp(def->defname, optDateFormat) == 0) {
            specifyDateFormat = true;
            /*
             * check whether date format is valid;
             */
            check_datetime_format(defGetString(def));
        } else if (pg_strcasecmp(def->defname, optTimeFormat) == 0) {
            specifyTimeFormat = true;
            /*
             * check whether time format is valid;
             */
            check_datetime_format(defGetString(def));
        } else if (pg_strcasecmp(def->defname, optTimestampFormat) == 0) {
            specifyTimestampFormat = true;
            /*
             * check whether timestamp format is valid;
             */
            check_datetime_format(defGetString(def));
        } else if (pg_strcasecmp(def->defname, optSmalldatetimeFormat) == 0) {
            specifySmalldatetimeFormat = true;
            /*
             * check whether smalldatetime format is valid;
             */
            check_datetime_format(defGetString(def));
        } else if (pg_strcasecmp(def->defname, optFix) == 0) {
            char *end = NULL;
            force_fix_width = (int)strtol(defGetString(def), &end, 10);
            if (*end != '\0')
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid value of FIX")));
        }
    }

    /*
     * Validate OBS related optionsi, we only have to so in coodinator node like
     * DDL planning for a OBS fdw table access.
     */
    if (IS_PGXC_COORDINATOR && !isValidate) {
        if (locations == NULL || planstate->source == NULL)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("LOCATION is required for the foreign tables")));

        if (is_obs_protocol(locations)) {
            /* First, blocks all OBS foreign table not allowed options */
            processOBSNoAllowOptions(specifyMode, fileheader, OutputFilenamePrefix, OutputFixAlignment,
                                     planstate->doLogRemote);

            checkGSOBSPrefixNoAllowRegionOption(hasRegion, planstate->source);

            /* Second, blocking any OBS have-to have options that user doesn't specify */
            processOBSHaveToOptions(accessKeyStr, secretAccessKeyStr, format, encryptStr, chunksizeStr,
                                    planstate->writeOnly);

            /* Verify duplicated location and write only options */
            processOBSLocationOptions(planstate->writeOnly, planstate->source);

            if (planstate->writeOnly) {
                /* Do write only table verification */
                if (list_length(planstate->source) > 1) {
                    ereport(ERROR,
                            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("can not specify multiple locations")));
                }
            }
        } else {
            /*
             * Also not allow none none-OBS table with chunksize, async, access_key,
             * secure_access_key, encrypt, region options
             */
            processNoneOBSOptions(chunksizeStr, encryptStr, accessKeyStr, secretAccessKeyStr, hasRegion);
        }
    }

    // set default values
    if (planstate->writeOnly)
        planstate->mode = MODE_INVALID;
    else if (planstate->mode == MODE_INVALID)
        planstate->mode = MODE_NORMAL;

    if (!rejectLimitSpecified)
        planstate->rejectLimit = 0;

    if (planstate->fileEncoding < 0)
        planstate->fileEncoding = pg_get_client_encoding();

    /* In dist_fdw_validator(), verify options just for FOREIGN TABLE. */
    if (!isValidate) {
        // verify options
        if (locations == NULL || planstate->source == NULL)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("LOCATION is required for the foreign tables")));

        VerifyLocations(planstate->source);

        if (fileheader != NULL) {
            VerifyFileHeader(fileheader);
        }
        if (OutputFilenamePrefix != NULL) {
            VerifyFilenamePrefix(OutputFilenamePrefix, planstate->writeOnly);
        }
        if (OutputFixAlignment != NULL) {
            VerifyFixAlignment(OutputFixAlignment, planstate->writeOnly, format);
        }

        if (rejectLimitSpecified && (planstate->rejectLimit == 0 || planstate->rejectLimit < REJECT_UNLIMITED))
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("PER NODE REJECT LIMIT must be greater than 0")));

        if (planstate->remoteName && !IsValidUserDefineName(planstate->remoteName))
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("Invalid name \'%s\' in REMOTE LOG", planstate->remoteName)));
        if (rejectLimitSpecified && planstate->writeOnly)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("PER NODE REJECT LIMIT only available on READ ONLY foreign table")));
        if (rejectLimitSpecified && !planstate->errorName && !planstate->doLogRemote)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("PER NODE REJECT LIMIT only available with LOG INTO or REMOTE LOG")));
        if (planstate->doLogRemote && planstate->writeOnly)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR), errmsg("REMOTE LOG only available on READ ONLY foreign table")));
        if (planstate->doLogRemote && is_local_location(strVal(lfirst(list_head(planstate->source)))))
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("REMOTE LOG only available on in NORMAL mode")));
        if (planstate->doLogRemote && planstate->writeOnly)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR), errmsg("REMOTE LOG only available on READ ONLY foreign table")));
        if (specifyFillMissing && planstate->writeOnly)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("FILL_MISSING_FIELDS only available on READ ONLY foreign table")));
        if (specifyIgnoreExtraData && planstate->writeOnly)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("IGNORE_EXTRA_DATA only available on READ ONLY foreign table")));
        /*
         * bulkload compatible illegal chars option isn't allowed for exporting.
         */
        if (specifyCompatibleIllegalChars && planstate->writeOnly)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("COMPATIBLE_ILLEGAL_CHARS only available on READ ONLY foreign table")));
        /*
         * bulkload datetime format options aren't allowed for exporting.
         */
        if (specifyDateFormat && planstate->writeOnly)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR), errmsg("DATE_FORMAT only available on READ ONLY foreign table")));
        if (specifyTimeFormat && planstate->writeOnly)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR), errmsg("TIME_FORMAT only available on READ ONLY foreign table")));
        if (specifyTimestampFormat && planstate->writeOnly)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("TIMESTAMP_FORMAT only available on READ ONLY foreign table")));
        if (specifySmalldatetimeFormat && planstate->writeOnly)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("SMALLDATETIME_FORMAT only available on READ ONLY foreign table")));
        if (specifyMode && planstate->writeOnly)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("MODE only available on READ ONLY foreign table")));
        if (planstate->errorName && planstate->writeOnly)
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR), errmsg("LOG INTO only available on READ ONLY foreign table")));
        /*
         * Verify the location option.
         * Remote location(gsfs://xxxx) can not be used in SHARED and PRIVATE mode.
         * */
        foreach (lc, planstate->source) {
            if (!is_valid_location(strVal(lfirst(lc))))
                ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR), errmsg("location \"%s\" is invalid", strVal(lfirst(lc)))));
            if (!is_local_location(strVal(lfirst(lc))) && IS_SHARED_MODE(planstate->mode))
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("SHARED mode can not use location \"%s\"", strVal(lfirst(lc)))));
            if (!is_local_location(strVal(lfirst(lc))) && !is_roach_location(strVal(lfirst(lc))) &&
                IS_PRIVATE_MODE(planstate->mode))
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("PRIVATE mode can not use location \"%s\"", strVal(lfirst(lc)))));
            if (is_local_location(strVal(lfirst(lc))) && IS_NORMAL_MODE(planstate->mode))
                ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                                errmsg("Normal mode can not use location \"%s\"", strVal(lfirst(lc)))));
        }

        // maybe multi-locations are given, just gsfs locations are avaliable.
        // can only specify one local location.
        Assert(planstate->source);
        Assert(list_length(planstate->source) >= 1);
        if (planstate->writeOnly && is_local_location(strVal(lfirst(list_head(planstate->source))))) {
            if (list_length(planstate->source) > 1) {
                ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("can not specify multiple local locations")));
            }

            const char *localpath = strVal(lfirst(list_head(planstate->source)));
            if (localpath[0] == '.') {
                ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("can not specify relative local locations")));
            }
        }

        if (is_roach_location(strVal(lfirst(list_head(planstate->source))))) {
            if (list_length(planstate->source) > 1) {
                ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("can not specify multiple local locations")));
            }
        }

        // for gsfs locations, we should specify FILEHEADER at a time
        if (planstate->writeOnly && hasHeader && (fileheader == NULL) &&
            !is_local_location(strVal(lfirst(list_head(planstate->source)))))
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("HEADER needs FILEHEADER specification in WRITE ONLY foreign table")));

        if (format == FORMAT_CSV && planstate->mode == MODE_SHARED)
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("SHARED mode can not be used with CSV format")));

        /* for gds import/export,  duplication locations are not allowed.
         * Note: This check depends on the above verifications.
         */
        if (IS_NORMAL_MODE(planstate->mode) || IS_INVALID_MODE(planstate->mode))
            CheckDupLocations(planstate->source);

#ifndef ENABLE_MULTIPLE_NODES
        /* GDS foreign tables are simply no longer needed in single node mode */
        if ((planstate->mode == MODE_NORMAL) && (!is_obs_protocol(locations))) {
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Un-supported feature"),
                            errdetail("Gauss Data Service(GDS) are not supported in single node mode.")));
        }
#endif

        /*
         * To achieve warning checking and propagation only when a read-only GDS foreign table
         * is created with format fixed but no specified fix option, we will have to do duplicate
         * save and check in here and above, due to our poorly-implemented option check. But
         * still we cannot skip this warning if any other option pop an error due to the current
         * option check as well as the error stack mechanism.
         */
        if (isPropagateToFE && !planstate->writeOnly && (format == FORMAT_FIXED) && force_fix_width == 0)
            ereport(WARNING,
                    (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                     errmsg("The above Read-Only foreign table is using FIXED mode without specifying 'fix' option."),
                     errhint("Please use 'fix' option to specify expected fixed record length in order to parser the "
                             "data file correctly.")));

        if (((IS_SHARED_MODE(planstate->mode) || IS_PRIVATE_MODE(planstate->mode)) && !initialuser())
            && !(isOperatoradmin(GetUserId()) && u_sess->attr.attr_security.operation_mode)) {
            ereport(ERROR,
                    (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                     errmsg("Shared mode and private mode are only available for the supper user and Operatoradmin")));
        }
    }

    // Remove the options that only available in foreign table
    foreach (lc, rmList)
        options = list_delete(options, lfirst(lc));
    if (rmList != NIL)
        list_free_deep(rmList);

    planstate->options = options;
}

void decryptKeyString(const char *keyStr, char destplainStr[], uint32 destplainLength, const char *obskey)
{
#define ENCRYPT_STR_PREFIX "encryptstr"

    if (unlikely(keyStr == NULL)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Invalid key string")));
    }
    if (strncmp(keyStr, ENCRYPT_STR_PREFIX, strlen(ENCRYPT_STR_PREFIX)) == 0) {
        keyStr = keyStr + strlen(ENCRYPT_STR_PREFIX);
    } else {
        errno_t rc = memcpy_s(destplainStr, destplainLength, keyStr, strlen(keyStr) + 1);
        securec_check(rc, "\0", "\0");
        return;
    }
    /*
     * step 2: use cipher key to decrypt option and store
     * results into decryptAccessKeyStr/decryptSecretAccessKeyStr
     */
    decryptOBS(keyStr, destplainStr, destplainLength, obskey);
}

/*
 * @Description: Encrpyt access key and security access key in options before insert
 * tuple into pg_foreign_table when creating foreign tables in obs protocol.
 * @Input: pointer to options list
 */
void decryptOBSForeignTableOption(List **options)
{
    char *keyStr = NULL;
    GS_UINT32 keyStrLen = 0;

    /* The maximum string length of the encrypt access key or encrypt access key is 1024 */
    char decryptSecretAccessKeyStr[DEST_CIPHER_LENGTH] = "\0";
    char decryptpassWordStr[DEST_CIPHER_LENGTH] = "\0";
    bool haveSecretAccessKey = false;
    errno_t rc = EOK;

    ListCell *lc = NULL;
    ListCell *prev = NULL;
    foreach (lc, *options) {
        DefElem *def = (DefElem *)lfirst(lc);
        if (pg_strcasecmp(def->defname, optSecretAccessKey) == 0) {
            haveSecretAccessKey = true;

            keyStr = defGetString(def);
            decryptKeyString(keyStr, decryptSecretAccessKeyStr, DEST_CIPHER_LENGTH, NULL);

            *options = list_delete_cell(*options, lc, prev);
            break;
        }
        prev = lc;
    }

    if (haveSecretAccessKey) {
        *options = lappend(*options, makeDefElem(pstrdup(optSecretAccessKey),
                                                 (Node *)makeString(pstrdup(decryptSecretAccessKeyStr))));
    }

    if (keyStr != NULL) {
        /* safty concern, empty keyStr manually. */
        keyStrLen = strlen(keyStr);
        rc = memset_s(keyStr, keyStrLen, 0, keyStrLen);
        securec_check(rc, "", "");
        pfree(keyStr);
    }

    /* safty concern, empty decryptAccessKeyStr & decryptSecretAccessKeyStr manually. */
    rc = memset_s(decryptSecretAccessKeyStr, DEST_CIPHER_LENGTH, 0, DEST_CIPHER_LENGTH);
    securec_check(rc, "", "");
    rc = memset_s(decryptpassWordStr, DEST_CIPHER_LENGTH, '\0', DEST_CIPHER_LENGTH);
    securec_check(rc, "", "");
}

void GetDistImportOptions(Oid relOid, DistImportPlanState *planstate, ForeignOptions *fOptions = NULL)
{
    List *options = NIL;
    if (fOptions == NULL) {
        ForeignTable *table = NULL;
        ForeignServer *server = NULL;
        ForeignDataWrapper *wrapper = NULL;

        /*
         * Extract options from FDW objects.  We ignore user mappings because
         * file_fdw doesn't have any options that can be specified there.
         *
         * (XXX Actually, given the current contents of valid_options[], there's
         * no point in examining anything except the foreign table's own options.
         * Simplify?)
         */
        table = GetForeignTable(relOid);
        server = GetForeignServer(table->serverid);
        wrapper = GetForeignDataWrapper(server->fdwid);

        options = NIL;
        options = list_concat(options, wrapper->options);
        options = list_concat(options, server->options);
        options = list_concat(options, table->options);

        options = adaptOBSURL(options);

        if (table->write_only)
            options = lappend(options, makeDefElem(pstrdup(optWriteOnly), (Node *)makeString(pstrdup("true"))));
    } else {
        Assert(fOptions->fOptions != NULL);
        options = fOptions->fOptions;
    }

    // Add decrpyt function for obs access key and security access key in obs options
    decryptOBSForeignTableOption(&options);

    ProcessDistImportOptions(planstate, options, false);
}

/*
 *   distImportGetRelSize
 * 	 Obtain relation size estimates for a foreign table
 */
void distImportGetRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    DistImportPlanState *planstate = NULL;

    /*
     * Fetch options.	We only need filename at this point, but we might as
     * well get everything and not need to re-fetch it later in planning.
     */
    planstate = (DistImportPlanState *)palloc0(sizeof(DistImportPlanState));
    GetDistImportOptions(foreigntableid, planstate);
    baserel->fdw_private = (void *)planstate;

    /* Estimate relation size */
    estimate_size(root, baserel, planstate);
}

/*
 * distGetForeignPaths
 * 	 Create possible access paths for a scan on the foreign table
 *
 * 	 Currently we don't support any push-down feature, so there is only one
 * 	 possible access path, which simply returns all records in the order in
 * 	 the data file.
 */
void distImportGetPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    DistImportPlanState *planstate = (DistImportPlanState *)baserel->fdw_private;
    Cost startup_cost;
    Cost total_cost;
    List *columns = NIL;
    List *coptions = NIL;

    /* Decide whether to selectively perform binary conversion */
    if (check_selective_binary_conversion(baserel, foreigntableid, &columns))
        coptions = list_make1(makeDefElem("convert_selectively", (Node *)columns));

    /* Estimate costs */
    estimate_costs(root, baserel, planstate, &startup_cost, &total_cost);

    /*
     * Create a ForeignPath node and add it as only possible path.  We use the
     * fdw_private list of the path to carry the convert_selectively option;
     * it will be propagated into the fdw_private list of the Plan node.
     */
    add_path(root, baserel,
             (Path *)create_foreignscan_path(root, baserel, startup_cost, total_cost, NIL, /* no pathkeys */
                                             NULL,                                         /* no outer rel either */
                                             coptions, u_sess->opt_cxt.query_dop));
}

/*
 * distGetForeignPlan
 * 	 Create a ForeignScan plan node for scanning the foreign table
 */
ForeignScan *distImportGetPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path,
                               List *tlist, List *scan_clauses)
{
    Index scan_relid = baserel->relid;
    DistImportPlanState *planstate = (DistImportPlanState *)baserel->fdw_private;
    List *fdw_data = best_path->fdw_private;
    List *tasklist = NIL;
    uint32 distSessionKey;
    const int64 fileNum = 0;

    if (IS_PGXC_COORDINATOR && !IS_STREAM)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Un-support feature"),
                        errdetail("foreign table scan can not run on stream mode due to unsupported clauses")));

    if (IS_PGXC_DATANODE)
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Un-support feature"),
                        errdetail("foreign table scan can not direct execute on datanode")));

    if (planstate->writeOnly)
        ereport(ERROR, (errcode(ERRCODE_OPERATE_INVALID_PARAM), errmsg("can not scan a WRITE ONLY foreign table")));

    /*
     * There is no need to compute the tasklist here because it should be
     * recomputed if dynamic smp is enabled. To prevent unnecessary costs,
     * we only save an empty tasklist here, and compute it at executor.
     * Please refer to distImportBegin for details.
     */
    if (!IS_PRIVATE_MODE(planstate->mode)) {
        fdw_data = defSetOption(fdw_data, optTaskList, (Node *)tasklist);
    }

    distSessionKey = generate_unique_id(&gt_sessionId);
    fdw_data = defSetOption(fdw_data, optSessionKey, (Node *)makeInteger((long)distSessionKey));
    best_path->fdw_private = fdw_data;

    /*
     * We have no native ability to evaluate restriction clauses, so we just
     * put all the scan_clauses into the plan node's qual list for the
     * executor to check.	So all we have to do here is strip RestrictInfo
     * nodes from the clauses and ignore pseudoconstants (which will be
     * handled elsewhere).
     */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    /* Create the ForeignScan node */
    ForeignScan *fScan = make_foreignscan(tlist, scan_clauses, scan_relid, NIL, /* no expressions to evaluate */
                                          fdw_data, EXEC_ON_DATANODES);         /* no private state either */

    fScan->objectNum = fileNum;
    if (root->parse->commandType != CMD_INSERT && is_obs_protocol(HdfsGetOptionValue(foreigntableid, optLocation))) {
        ((Plan *)fScan)->vec_output = true;
    }

    /*
     * Judge whether using the infomational constraint on scan_qual and hdfsqual.
     */
    if (u_sess->attr.attr_sql.enable_constraint_optimization) {
        ListCell *l = NULL;
        fScan->scan.scan_qual_optimized = useInformationalConstraint(root, scan_clauses, NULL);
        /*
         * Mark the foreign scan whether has unique results on one of its output columns.
         */
        foreach (l, fScan->scan.plan.targetlist) {
            TargetEntry *tle = (TargetEntry *)lfirst(l);

            if (IsA(tle->expr, Var)) {
                Var *var = (Var *)tle->expr;
                RangeTblEntry *rtable = planner_rt_fetch(var->varno, root);

                if (RTE_RELATION == rtable->rtekind && findConstraintByVar(var, rtable->relid, UNIQUE_CONSTRAINT)) {
                    fScan->scan.plan.hasUniqueResults = true;
                    break;
                }
            }
        }
    }

    return fScan;
}

/*
 * distExplainForeignScan
 * 	 Produce extra output for EXPLAIN
 */
void distImportExplain(ForeignScanState *node, ExplainState *es)
{
    char *filename = NULL;
    List *options = NIL;

    /* Fetch options --- we only need filename at this point */
    distGetOptions(RelationGetRelid(node->ss.ss_currentRelation), &filename, &options);
    ExplainPropertyText("Foreign File", filename, es);

    List *fdw_private = (List *)((ForeignScan *)node->ss.ps.plan)->fdw_private;
    /* output task assignment in debug */
    StringInfoData str;
    initStringInfo(&str);

    List *totalTask = NIL;
    ListCell *lc = NULL;
    foreach (lc, fdw_private) {
        DefElem *defElem = (DefElem *)lfirst(lc);
        if (strcmp(optTaskList, defElem->defname) == 0)
            totalTask = (List *)defElem->arg;
    }

    foreach (lc, totalTask) {
        resetStringInfo(&str);
        DistFdwDataNodeTask *dnTask = (DistFdwDataNodeTask *)lfirst(lc);
        Assert(dnTask);
        appendStringInfo(&str, "%s\n", dnTask->dnName);
        List *tasks = dnTask->task;
        ListCell *task = NULL;

        foreach (task, tasks) {
            DistFdwFileSegment *segment = (DistFdwFileSegment *)lfirst(task);
            appendStringInfo(&str, "%s\t%ld\t%ld\n", segment->filename, segment->begin, segment->end);
        }
        ExplainPropertyText("Task", str.data, es);
    }
}

/*
 * @Description:  get obs option value from planstate option list
 * @OUT obs_copy_options: struct for ObsCopyOptions
 * @IN  options: options list
 */
void getOBSOptions(ObsCopyOptions *obs_copy_options, List *options)
{
    char *accessKeyStr = NULL;
    char *secretAccessKeyStr = NULL;
    ListCell *lc = NULL;

    /* For OBS bulkloading, specified default value at first */
    obs_copy_options->encrypt = false;
    obs_copy_options->chunksize = (uint32_t)DIST_OBS_CHUNKSIZE;
    obs_copy_options->access_key = NULL;
    obs_copy_options->secret_access_key = NULL;

    foreach (lc, options) {
        DefElem *def = (DefElem *)lfirst(lc);
        if (pg_strcasecmp(def->defname, optChunkSize) == 0) {
            uint32_t chunksize = atoi(defGetString(def));
            if (chunksize > (PG_UINT32_MAX / (1024 * 1024))) {
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("obs option chunksize is too large")));
            }
            obs_copy_options->chunksize = chunksize * 1024 * 1024;
        } else if (pg_strcasecmp(def->defname, optEncrypt) == 0) {
            obs_copy_options->encrypt = defGetBoolean(def);
        } else if (pg_strcasecmp(def->defname, optAccessKey) == 0) {
            accessKeyStr = defGetString(def);
            obs_copy_options->access_key = pstrdup(accessKeyStr);
        } else if (pg_strcasecmp(def->defname, optSecretAccessKey) == 0) {
            secretAccessKeyStr = defGetString(def);
            obs_copy_options->secret_access_key = pstrdup(secretAccessKeyStr);
        }
    }
}

/* Set obs options for data structure */
void distOBSImportBegin(DistImportExecutionState *importState, DistImportPlanState *planstate)
{
    Assert(importState && planstate);

    if (is_obs_protocol(planstate->filename)) {
        getOBSOptions(&importState->obs_copy_options, planstate->options);
    }
}

/*
 * distBeginForeignScan
 * 	 Initiate access to the file by creating CopyState
 */
void distImportBegin(ForeignScanState *node, int eflags)
{
    DistImportExecutionState *importState = NULL;
    List *totalTask = NULL;
    ListCell *lc = NULL;
    List *fdw_private = (List *)((ForeignScan *)node->ss.ps.plan)->fdw_private;
    DistImportPlanState planstate;
    uint32 distSessionKey = 0;
    ForeignScan *foreignScan = (ForeignScan *)node->ss.ps.plan;

    /*
     * Do nothing in EXPLAIN (no ANALYZE) case.  node->fdw_state stays NULL.
     */
    if ((uint32)eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    /*
     * block the initialization of OBS(txt/csv) ForeignScan node in DWS DN.
     *
     * "foreignScan->rel" means the ForeignScan node will really run on
     * DN of the compute pool.
     *
     * "foreignScan->in_compute_pool" will be set to true in
     * do_query_for_planrouter() of PLANROUTER node.
     *
     * Currently, "IS_PGXC_DATANODE == true and false == foreignScan->in_compute_pool"
     * means that we are still on DWS DN, and "foreignScan->rel == true" means
     * that the ForeignScan of HDFS foreign table should NOT run on DWS DN.
     */
    if (IS_PGXC_DATANODE && foreignScan->rel && false == foreignScan->in_compute_pool) {
        return;
    }

    if (NULL == foreignScan->rel) {
        /*
         * It means that the current node has meta data. need to get options
         * from catalog.
         */
        GetDistImportOptions(RelationGetRelid(node->ss.ss_currentRelation), &planstate, NULL);
    } else {
        GetDistImportOptions(RelationGetRelid(node->ss.ss_currentRelation), &planstate, foreignScan->options);
    }

    foreach (lc, fdw_private) {
        DefElem *defElem = (DefElem *)lfirst(lc);
        if (strcmp(optTaskList, defElem->defname) == 0) {
            /*
             *  The foreignscan relies on u_sess->opt_cxt.query_dop to generate task_list and all those
             *  are done in create_plan. However in dynamic smp, u_sess->opt_cxt.query_dop changed,
             *  that will make the task_list generate in create_plan out of date.
             *  To solve the problem, we have to generate the plan after the
             *  u_sess->opt_cxt.query_dop changed. This is done by calling assignTaskToDataNode again.
             *  Note that only coordinator need to be handled.
             */
            if (IS_PGXC_COORDINATOR) {
                List *dn_list = (node->ss.ps.plan)->exec_nodes->nodeList;

                if (!IS_PRIVATE_MODE(planstate.mode)) {
                    totalTask = assignTaskToDataNode(planstate.source, planstate.mode, dn_list, (node->ss.ps.plan)->dop,
                                                     &planstate);
                    defElem->arg = (Node *)totalTask;
                }
            } else {
                totalTask = (List *)defElem->arg;
            }
        } else if (strcmp(optSessionKey, defElem->defname) == 0) {
            /*
             *  When we are on a coordinator, we want to regenerate an new sessionId for each
             *  import task to avoid using the same sessionId when executing an anonymous
             *  block or storage procedure. (In this case it will lead to possible "Session
             *  already exists" Error on GDS).
             *  If we are on a datanode, simply use the sessionId that was generated by the
             *  coordinator and sent with the execution plan.
             */
            if (IS_PGXC_COORDINATOR) {
                distSessionKey = generate_unique_id(&gt_sessionId);
                defElem->arg = (Node *)makeInteger((long)distSessionKey);
            } else {
                distSessionKey = (uint32)(intVal(defElem->arg));
            }
            ereport(DEBUG1, (errcode(ERRCODE_DEBUG), errmsg("Session id: %u", distSessionKey)));
        }
    }

    /*
     * Save state in node->fdw_state.	We must save enough information to call
     * begin_dist_copy_from() again.
     */
    importState = (DistImportExecutionState *)palloc0(sizeof(DistImportExecutionState));
    importState->mode = planstate.mode;
    importState->options = planstate.options;
    importState->source = planstate.source;
    importState->distSessionKey = distSessionKey;

    /* Get OBS buckload options */
    distOBSImportBegin(importState, &planstate);

    importState->rejectLimit = planstate.rejectLimit;
    importState->beginTime = TimestampTzGetDatum(GetCurrentTimestamp());
    InitDistImport(importState, node->ss.ss_currentRelation, planstate.filename, NIL, planstate.options, totalTask);
    importState->isLogRemote = planstate.doLogRemote;
    node->fdw_state = (void *)importState;

    importState->isExceptionShutdown = false;

    if (IS_PGXC_DATANODE) {
        ForeignScan *scan = (ForeignScan *)node->ss.ps.plan;
        ImportErrorLogger *logger = NULL;

        /*
         * Foreign Scan Thread don't unlink this cache file,
         * it just open and write error data into this cache file.
         */
        ErrLogInfo err_info = { u_sess->stream_cxt.smp_id, false };

        importState->needSaveError = scan->needSaveError;
        if (!importState->needSaveError)
            return;

        if (planstate.doLogRemote) {
            char *remoteName = planstate.remoteName;

            logger = New(CurrentMemoryContext) GDSErrorLogger;
            importState->elogger = lappend(importState->elogger, logger);
            if (strlen(planstate.remoteName) == 0)
                remoteName = (char*)importState->cur_relname;
            logger->Initialize((void*)importState->io_stream, (TupleDesc)remoteName, err_info);
        }

        if (scan->errCache != NULL) {
            ErrorCacheEntry *entry = scan->errCache;

            importState->errLogRel = relation_open(entry->rte->relid, AccessShareLock);
            logger = New(CurrentMemoryContext) LocalErrorLogger;
            importState->elogger = lappend(importState->elogger, logger);
            logger->Initialize(entry->filename, RelationGetDescr(importState->errLogRel), err_info);
        }
    }
}

void ReportIllegalCharExceptionThreshold()
{
    if (t_thrd.bulk_cxt.illegal_character_err_cnt >= ILLEGAL_CHARACTERS_ERR_THRESHOLD &&
        !t_thrd.bulk_cxt.illegal_character_err_threshold_reported) {
        ereport(WARNING,
                (errmodule(MOD_GDS), errmsg("Number of illegal character exceptions exceed the threshold."),
                 errdetail("Please make sure that you are using right encoding setting to decode incomming data.")));
        t_thrd.bulk_cxt.illegal_character_err_threshold_reported = true;
    }
}

/*
 * distterateForeignScan
 * 	 Read next record from the data file and store it into the
 * 	 ScanTupleSlot as a virtual tuple
 */
TupleTableSlot *distExecImport(ForeignScanState *node)
{
    DistImportExecutionState *importState = (DistImportExecutionState *)node->fdw_state;
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    bool found = false;
    ErrorContextCallback errcontext;
    MemoryContext precontext;
    MemoryContext scanMcxt = node->scanMcxt;

    /* Set up callback to identify error line number. */
    errcontext.callback = BulkloadErrorCallback;
    errcontext.arg = (void *)importState;
    errcontext.previous = t_thrd.log_cxt.error_context_stack;
    t_thrd.log_cxt.error_context_stack = &errcontext;
    precontext = MemoryContextSwitchTo(scanMcxt);

    /*
     * The protocol for loading a virtual tuple into a slot is first
     * ExecClearTuple, then fill the values/isnull arrays, then
     * ExecStoreVirtualTuple.	If we don't find another row in the file, we
     * just skip the last step, leaving the slot empty as required.
     *
     * We can pass ExprContext = NULL because we read all columns from the
     * file, so no need to evaluate default expressions.
     *
     * We can also pass tupleOid = NULL because we don't allow oids for
     * foreign tables.
     */
retry:
    CHECK_FOR_INTERRUPTS();
    (void)ExecClearTuple(slot);
    MemoryContextReset(node->scanMcxt);
#ifndef ENABLE_LITE_MODE
    SetObsMemoryContext(((CopyState)importState)->copycontext);
#endif
    ReportIllegalCharExceptionThreshold();

    PG_TRY();
    {
        /*
         * Synchronize the current bulkload states.
         */
        SyncBulkloadStates((CopyState)importState);
        found = NextCopyFrom((CopyState)importState, NULL, slot->tts_values, slot->tts_isnull, NULL);
    }
    PG_CATCH();
    {
        /*
         * Clean the current bulkload states.
         */
        CleanBulkloadStates();

        if (TrySaveImportError(importState, node)) {
            MemoryContextSwitchTo(scanMcxt);
            goto retry;
        } else {
            /* clean copy state and re throw */
            importState->isExceptionShutdown = true;
            EndDistImport(importState);
            PG_RE_THROW();
        }
    }
    PG_END_TRY();

    /*
     * Clean the current bulkload states.
     */
    CleanBulkloadStates();

    if (found) {
        ExecStoreVirtualTuple(slot);

        /*
         * Optimize foreign scan by using informational constraint.
         */
        if (((ForeignScan *)node->ss.ps.plan)->scan.predicate_pushdown_optimized && false == slot->tts_isempty) {
            /*
             * If we find a suitable tuple, set is_scan_end value is true.
             * It means that we do not find suitable tuple in the next iteration,
             * the iteration is over.
             */
            node->ss.is_scan_end = true;
        }
    }

    MemoryContextSwitchTo(precontext);
    /* Remove error callback. */
    t_thrd.log_cxt.error_context_stack = errcontext.previous;

    return slot;
}

/*
 * distEndForeignScan
 * 	 Finish scanning foreign table and dispose objects used for this scan
 */
void distImportEnd(ForeignScanState *node)
{
    DistImportExecutionState *importState = (DistImportExecutionState *)node->fdw_state;

    if (importState != NULL)
        EndDistImport(importState);
}

/*
 * distReScanForeignScan
 * 	 Rescan table, possibly with new parameters
 */
void distReImport(ForeignScanState *node)
{
    // not implement yet
}

