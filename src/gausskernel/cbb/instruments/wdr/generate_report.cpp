/* -------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * generate_report.cpp
 *   functions for user stat, such as login/logout counter
 *
 * IDENTIFICATION
 *	  src/gausskernel/cbb/instruments/wdr/generate_report.cpp
 *
 * -------------------------------------------------------------------------
 */
#include <math.h>
#include "postgres.h"
#include "knl/knl_variable.h"
#include "pgstat.h"
#include "miscadmin.h"
#include "fmgr.h"
#include "pgtime.h"
#include "utils/lsyscache.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/array.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/xml.h"
#include "instruments/generate_report.h"
#include "instruments/snapshot.h"

/* report type - summary/detail/summary + detail  */
const char* g_summaryType = "summary";
const char* g_detailType = "detail";
const char* g_allType = "all";

/* report scope - cluster, node */
const char* g_nodeScope = "node";
const char* g_clusterScope = "cluster";

/* reportpart array is the part of the report other than report detail  */
const int NUM_REPROT_PART = 2;
const char* g_reportpart[NUM_REPROT_PART] = {"Report info", "Summary"};

/* const variables */
const char* g_metric_name = "Metric";
const char* g_per_second_str = "Per Second";
const char* g_per_trx_str = "Per Transaction";
const char* g_per_exec_str = "Per Exec";
const char* g_value_str = "Value";

const char* g_per_sec_io_rw = "Read + Write Per Sec";
const char* g_per_sec_io_r = "Read Per Sec";
const char* g_per_sec_io_w = "Write Per Sec";
const char* cache_io_sys_schema = "'pg_catalog', 'information_schema', 'snapshot'"
                                  ", 'dbe_pldebugger', 'db4ai', 'dbe_pldeveloper'"
                                  ", 'cstore'";

const char* TableIOField[] = {
    "\"%Heap Blks Hit Ratio\"", "\"Heap Blks Read\"", "\"Heap Blks Hit\"",
    "\"Idx Blks Read\"", "\"Idx Blks Hit\"", "\"Toast Blks Read\"",
    "\"Toast Blks Hit\"", "\"Tidx Blks Read\"", "\"Tidx Blks Hit\""
    };
const char* IndexIOField[] = {
    "\"%Idx Blks Hit Ratio\"",
    "\"Idx Blks Read\"", "\"Idx Blks Hit\""
    };
typedef struct CacheIODash {
    char* dashTitle;     /* Module title */
    char* tableTitle;    /* Table title */
    char* dash1;    /* Table description */
    char* dash2;    /* Table description */
    char* sortkey;  /* sort key */
    char* sortByDir; /* order by asc or desc */
    char* sort;  /* index or table */
} CacheIODash;
struct CacheIODash cacheIODashModule[] = {
    {
    "Cache IO Stats", "User table IO activity ordered by %Heap Blks Hit Ratio",
    "User table IO activity ordered by %Heap Blks Hit Ratio asc", "List top 200 records",
    "\"%Heap Blks Hit Ratio\"", "asc", "table"
    },
    {
    "Cache IO Stats", "User table IO activity ordered by Heap Blks Read",
    "User table IO activity ordered by Heap Blks Read desc", "List top 200 records",
    "\"Heap Blks Read\"", "desc", "table"
    },
    {
    "Cache IO Stats", "User table IO activity ordered by Heap Blks Hit",
    "User table IO activity ordered by Heap Blks Hit desc", "List top 200 records",
    "\"Heap Blks Hit\"", "desc", "table"
    },
    {
    "Cache IO Stats", "User table IO activity ordered by Idx Blks Read",
    "User table IO activity ordered by Idx Blks Read desc", "List top 200 records",
    "\"Idx Blks Read\"", "desc", "table"
    },
    {
    "Cache IO Stats", "User table IO activity ordered by Idx Blks Hit",
    "User table IO activity ordered by Idx Blks Hit desc", "List top 200 records",
    "\"Idx Blks Hit\"", "desc", "table"
    },
    {
    "Cache IO Stats", "User table IO activity ordered by Toast Blks Read",
    "User table IO activity ordered by Toast Blks Read desc", "List top 200 records",
    "\"Toast Blks Read\"", "desc", "table"
    },
    {
    "Cache IO Stats", "User table IO activity ordered by Toast Blks Hit",
    "User table IO activity ordered by Toast Blks Hit desc", "List top 200 records",
    "\"Toast Blks Hit\"", "desc", "table"
    },
    {
    "Cache IO Stats", "User table IO activity ordered by Tidx Blks Read",
    "User table IO activity ordered by Tidx Blks Read desc", "List top 200 records",
    "\"Tidx Blks Read\"", "desc", "table"
    },
    {
    "Cache IO Stats", "User table IO activity ordered by Tidx Blks Hit",
    "User table IO activity ordered by Tidx Blks Hit desc", "List top 200 records",
    "\"Tidx Blks Hit\"", "desc", "table"
    },
    {
    "Cache IO Stats", "User index IO activity ordered by %Idx Blks Hit Ratio",
    "User index IO activity ordered by %Idx Blks Hit Ratio asc", "List top 200 records",
    "\"%Idx Blks Hit Ratio\"", "asc", "index"
    },
    {
    "Cache IO Stats", "User index IO activity ordered by Idx Blks Read",
    "User index IO activity ordered by Idx Blks Read desc", "List top 200 records",
    "\"Idx Blks Read\"", "desc", "index"
    },
    {
    "Cache IO Stats", "User index IO activity ordered by Idx Blks Hit",
    "User index IO activity ordered by Idx Blks Hit desc", "List top 200 records",
    "\"Idx Blks Hit\"", "desc", "index"
    }
    };


void DeepListFree(List* deepList, bool deep);
typedef struct dashboard {
    List* table;      /* Store the query result in string format */
    List* type;       /* record the type of the table column */
    List* desc;       /* Description of the table */
    char* dashTitle;  /* dashboard title */
    char* tableTitle; /* table title */
} dashboard;

typedef enum { HAVE_SQLID, HAVE_SQLTEXT, HAVE_OTHER } ColName;

/* generate_wdr_report proc's parameter */
typedef struct _report_params {
    int64 begin_snap_id; /* begin snapshot oid */
    int64 end_snap_id;   /* end snapshot oid */

    char* report_type;  /* REPORT_TYPE_SUMMARY/REPORT_TYPE_DETAIL/REPORT_TYPE_ALL */
    char* report_scope; /* REPORT_SCOPE_CLUSTER/REPORT_SCOPE_NODE */
    char* report_node;  /* if report_scope is node, report_node cann't be null */

    int64 snap_gap;             /* diff timestamps of two snapshots(seconds) */
    int64 snap_diff_trx_count;  /* diff trx count of two snapshots */
    uint64 snap_diff_sql_count; /* diff sql count of two snapshots */

    int64 snap_diff_commit_trx_count;   /* diff commit trx */
    int64 snap_diff_rollback_trx_count; /* diff commit trx */
    List* Contents;                     /* structured data pool */
} report_params;

static bool is_cluster_report(report_params* params);

/*
 * Declare function
 */
namespace GenReport {
/* Store query results as structured data */
void add_data(dashboard* spec, List** Contents);

/* Generate a report in Html format */
char* GenerateHtmlReport(report_params* params);

/* Generate index for h1 title */
void GenerateListByHtml(uint32 detailrow, List* Contents, StringInfoData& listHtml);

/* Convert all dashboard data to html format */
void GenerateDashboadsHtml(uint32 detailrow, List* Contents, StringInfoData& dashboadHtml);

/* generate the part of detail in report to html */
void GenerateDetailHtml(List* Contents, StringInfoData& detailHtml);

/* Convert one dashboard data to html format */
void dashboad_to_html(List* rowList, StringInfoData& dashboadHtml);

/* Convert table-level data to html format */
void table_to_Html(List* tableList, List* typeList, ColName COL_HAVE, StringInfoData& tableHtml);

/* Get report data by calculating the difference between two snapshots */
void get_report_data(report_params* params);
void TableToHtml(dashboard* dash, StringInfoData& tableHtml);
void DescToHtml(List* descList, StringInfoData& descHtml);
void TableTranspose(List* tabledata, List** overTurn);
List* ShowReportType(report_params* params);
void get_query_data(char* query, bool with_column_name, List** cstring_values, List** columnType);

/* summary function */
void get_snapinfo_data(report_params* params);
void GenerateSummaryHtml(const char* partname, List* Contents, StringInfoData& summary);
bool IsPartSummary(const char* partname, uint32* summaryrow, List* Contents);

/* detail function */
void GetWaitEventsData(report_params* params);
void GetSqlStatisticsData(report_params* params);
void GetNodeSQLStatisticsData(report_params* params);
void GetClusterSQLStatisticsData(report_params* params);
void GetTimeModelData(report_params* params);

/* ------ Summary Report------- */
void get_summary_database_stat(report_params* params);
void get_summary_load_profile(report_params* params);
}  // namespace GenReport

static dashboard* CreateDash(void)
{
    dashboard* dash = NULL;
    dash = (dashboard*)palloc(sizeof(dashboard));
    dash->table = NIL;
    dash->desc = NIL;
    dash->type = NIL;
    dash->dashTitle = NULL;
    dash->tableTitle = NULL;
    return dash;
}
/*
 * generate the type of html report
 */
char* GenReport::GenerateHtmlReport(report_params* params)
{
    StringInfoData result;

    /*
     * declare css for html
     */
    const char* css =
        "<html lang=\"en\"><head><title>openGauss WDR Workload Diagnosis Report</title>\n"
        "<meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\" />\n"
        "<style type=\"text/css\">\n"
        "a.wdr {font:bold 8pt Arial,Helvetica,sans-serif;color:#663300;vertical-align:top;"
        "margin-top:0pt; margin-bottom:0pt;}\n"
        "table.tdiff {  border_collapse: collapse; } \n"
        "body.wdr {font:bold 10pt Arial,Helvetica,Geneva,sans-serif;color:black; background:White;}\n"
        "h1.wdr {font:bold 20pt Arial,Helvetica,Geneva,sans-serif;color:#8B0000;background-color:White;"
        "border-bottom:1px solid #cccc99;margin-top:0pt; margin-bottom:0pt;padding:0px 0px 0px 0px;}\n"
        "h2.wdr {font:bold 18pt Arial,Helvetica,Geneva,sans-serif;color:#8B0000;"
        "background-color:White;margin-top:4pt; margin-bottom:0pt;}\n"
        "h3.wdr {font:bold 16pt Arial,Helvetica,Geneva,sans-serif;color:#8B0000;"
        "background-color:White;margin-top:4pt; margin-bottom:0pt;}\n"
        "li.wdr {font: 8pt Arial,Helvetica,Geneva,sans-serif; color:black; background:White;}\n"
        "th.wdrnobg {font:bold 8pt Arial,Helvetica,Geneva,sans-serif; color:black; "
        "background:White;padding-left:4px; padding-right:4px;padding-bottom:2px}\n"
        "th.wdrbg {font:bold 8pt Arial,Helvetica,Geneva,sans-serif; color:White; "
        "background:#8F170B;padding-left:4px; padding-right:4px;padding-bottom:2px}\n"
        "td.wdrnc {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;"
        "background:#F4F6F6; vertical-align:top;word-break: break-word; max-width: 300px;}\n"
        "td.wdrc {font:8pt Arial,Helvetica,Geneva,sans-serif;color:black;"
        "background:White; vertical-align:top;word-break: break-word; max-width: 300px;}\n"
        "</style>\n";

    const char* js = "<script type=\"text/javascript\">\nfunction msg(htmlId, inputname, objname, titlename) {\n"
                     "if (document.getElementById(inputname).value == \"1\") {\n"
                     "    document.getElementById(objname).style.display=\"block\";\n"
                     "    document.getElementById(htmlId).innerHTML = \"-\" + titlename;\n"
                     "    document.getElementById(inputname).value = \"0\";\n"
                     "} else {\n"
                     "    document.getElementById(objname).style.display=\"none\";\n"
                     "    document.getElementById(htmlId).innerHTML = \"+\" + titlename;\n"
                     "    document.getElementById(inputname).value = \"1\";\n"
                     "}}</script>\n"
                     "</head><body class=\"wdr\"\n>";

    initStringInfo(&result);
    appendStringInfoString(&result, "<style type=\"text/css\">\n#SQL_Text2>table{width:100%}\n");
    appendStringInfo(&result, "#SQL_Text2>table tr td:nth-child(-n+%s)", is_cluster_report(params) ? "3" : "2");
    appendStringInfoString(&result, " {width:8%;word-break:break-all;}\n</style>\n");

    const char* specialStyle = result.data;

    initStringInfo(&result);
    appendStringInfo(&result, "%s%s%s<h1 class=\"wdr\">Workload Diagnosis Report</h1>", css, specialStyle, js);
    pfree_ext(specialStyle);

    for (uint32 i = 0; i < NUM_REPROT_PART; i++)
        GenReport::GenerateSummaryHtml(g_reportpart[i], params->Contents, result);
    GenReport::GenerateDetailHtml(params->Contents, result);
    appendStringInfoString(&result, "\n</body></html>");
    return result.data;
}

void GenReport::GenerateSummaryHtml(const char* partname, List* Contents, StringInfoData& summary)
{
    uint32 summaryRow = 0;
    if (!GenReport::IsPartSummary(partname, &summaryRow, Contents)) {
        return;
    }
    List* rowList = (List*)list_nth(Contents, (int)summaryRow);
    if (strcmp(partname, "Summary") == 0) {
        appendStringInfo(&summary,
            "<a class=\"wdr\" name=\"Summary\"></a><h2 class=\"wdr\">%s</h2><ul>\n",
            ((dashboard*)linitial(rowList))->dashTitle);
        GenReport::dashboad_to_html(rowList, summary);
        appendStringInfo(&summary, "</ul>");
    }
    if (strcmp(partname, "Report info") == 0) {
        foreach_cell(rowCell, rowList)
        {
            appendStringInfoString(&summary, "<p />\n");
            GenReport::TableToHtml((dashboard*)lfirst(rowCell), summary);
            appendStringInfoString(&summary, "</div>\n<p />\n<hr align=\"left\" width=\"20%\" /><p />\n");
        }
        appendStringInfo(&summary, "</ul>");
    }
}

bool GenReport::IsPartSummary(const char* partname, uint32* summaryRow, List* Contents)
{
    bool haveSummary = false;
    uint32 i = 0;
    foreach_cell(cell, Contents)
    {
        List* rowList = (List*)lfirst(cell);
        if (!haveSummary && strcmp(((dashboard*)linitial(rowList))->dashTitle, partname) == 0) {
            *summaryRow = i;
            haveSummary = true;
            break;
        }
        i++;
    }
    return haveSummary;
}

List* GenReport::ShowReportType(report_params* params)
{
    if (params == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("report params is null")));
    }
    List* str_params = NIL;
    if (strcmp(params->report_type, "summary") == 0) {
        str_params = lappend(str_params, pstrdup("Summary"));
    } else if (strcmp(params->report_type, "detail") == 0) {
        str_params = lappend(str_params, pstrdup("Detail"));
    } else if (strcmp(params->report_type, "all") == 0) {
        str_params = lappend(str_params, pstrdup("Summary + Detail"));
    } else {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("report type can not = %s", params->report_type)));
    }

    /* show the report Scope of report */
    if (strcmp(params->report_scope, "cluster") == 0) {
        str_params = lappend(str_params, pstrdup("Cluster"));
    } else if (strcmp(params->report_scope, "node") == 0) {
        str_params = lappend(str_params, pstrdup("Node"));
    } else {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("report scope can not = %s", params->report_scope)));
    }

    if (params->report_node != NULL) {
        str_params = lappend(str_params, pstrdup(params->report_node));
    } else {
        str_params = lappend(str_params, pstrdup(""));
    }
    return str_params;
}

static void GetReportInfo(report_params* params)
{
    dashboard* dash1 = CreateDash();
    char* desc = NULL;
    StringInfoData query;
    initStringInfo(&query);

    /* report type info */
    List* str_params = GenReport::ShowReportType(params);
    appendStringInfo(&query,
        "select '%s' as \"Report Type\", '%s' as \"Report Scope\", '%s' as \"Report Node\" ",
        (char*)linitial(str_params),
        (char*)lsecond(str_params),
        (char*)lthird(str_params));
    list_free_deep(str_params);
    GenReport::get_query_data(query.data, true, &dash1->table, &dash1->type);
    dash1->dashTitle = "Report info";
    dash1->tableTitle = "report type";
    desc = "report type info";
    dash1->desc = lappend(dash1->desc, desc);
    GenReport::add_data(dash1, &params->Contents);

    /* snapshot info */
    dashboard* dash2 = CreateDash();
    resetStringInfo(&query);
    appendStringInfo(&query,
        "select snapshot_id as \"Snapshot Id\", "
        "pg_catalog.to_char(start_ts, 'YYYY-MM-DD HH24:MI:SS') as \"Start Time\", "
        "pg_catalog.to_char(end_ts, 'YYYY-MM-DD HH24:MI:SS') as \"End Time\""
        " from snapshot.snapshot where snapshot_id = %ld or snapshot_id = %ld",
        params->begin_snap_id,
        params->end_snap_id);
    GenReport::get_query_data(query.data, true, &dash2->table, &dash2->type);
    dash2->dashTitle = "Report info";
    dash2->tableTitle = "snapshot info";
    desc = "come from snapshot";
    dash2->desc = lappend(dash2->desc, desc);
    GenReport::add_data(dash2, &params->Contents);
    pfree(query.data);
}

static void GetOsInfo(List** osInfoList, report_params* params, List** typeList)
{
    StringInfoData query;
    initStringInfo(&query);

    List* cpusList = NIL;
    appendStringInfo(&query,
        "select 'CPUs', x.snap_value  from (select * from pg_node_env) t,"
        " (select * from snapshot.snap_global_os_runtime) x where x.snap_node_name = t.node_name and"
        " x.snapshot_id = %ld and (x.snap_name = 'NUM_CPUS');",
        params->end_snap_id);
    GenReport::get_query_data(query.data, false, &cpusList, typeList);
    *osInfoList = list_concat(*osInfoList, cpusList);

    List* cpuCoresList = NIL;
    resetStringInfo(&query);
    appendStringInfo(&query,
        "select 'Cores', x.snap_value  from (select * from pg_node_env) t,"
        " (select * from snapshot.snap_global_os_runtime) x where x.snap_node_name = t.node_name and"
        " x.snapshot_id = %ld and x.snap_name = 'NUM_CPU_CORES';",
        params->end_snap_id);
    GenReport::get_query_data(query.data, false, &cpuCoresList, typeList);
    *osInfoList = list_concat(*osInfoList, cpuCoresList);

    List* cpuSocketsList = NIL;
    resetStringInfo(&query);
    appendStringInfo(&query,
        "select 'Sockets', x.snap_value  from (select * from pg_node_env) t,"
        " (select * from snapshot.snap_global_os_runtime) x where x.snap_node_name = t.node_name and"
        " x.snapshot_id = %ld and  x.snap_name = 'NUM_CPU_SOCKETS';",
        params->end_snap_id);
    GenReport::get_query_data(query.data, false, &cpuSocketsList, typeList);
    *osInfoList = list_concat(*osInfoList, cpuSocketsList);

    /* node memory of host info */
    List* memList = NIL;
    resetStringInfo(&query);
    appendStringInfo(&query,
        "select 'Physical Memory', pg_catalog.pg_size_pretty(x.snap_value)"
        " from (select * from pg_node_env) t, (select * from snapshot.snap_global_os_runtime) x"
        " where x.snap_node_name = t.node_name and  x.snapshot_id = %ld and  x.snap_name = 'PHYSICAL_MEMORY_BYTES';",
        params->end_snap_id);
    GenReport::get_query_data(query.data, false, &memList, typeList);
    *osInfoList = list_concat(*osInfoList, memList);
    pfree(query.data);
}

void GenReport::get_snapinfo_data(report_params* params)
{
    List* last_data = NIL;
    List* overturn_data = NIL;
    List* tmpList = NIL;
    dashboard* dash = CreateDash();
    char* desc = NULL;
    StringInfoData query;
    initStringInfo(&query);

    /* get snapshot info and report type info */
    GetReportInfo(params);
    /* host node info */
    appendStringInfo(&query, "select node_name as \"Host Node Name\" from pg_node_env;");
    GenReport::get_query_data(query.data, true, &last_data, &dash->type);
    GenReport::TableTranspose(last_data, &tmpList);
    DeepListFree(last_data, false);
    last_data = NULL;

    /* NUM_CPUS/NUM_CPU_CORES/NUM_CPU_SOCKETS of host info */
    GetOsInfo(&tmpList, params, &dash->type);

    /* host version of host info */
    List* verList = NIL;
    resetStringInfo(&query);
#ifndef ENABLE_MULTIPLE_NODES
    appendStringInfo(&query, "select pg_catalog.version() as \"openGauss Version\";");
#else
    appendStringInfo(&query, "select pg_catalog.version() as \"GaussDB Version\";");
#endif
    GenReport::get_query_data(query.data, true, &verList, &dash->type);
    GenReport::TableTranspose(verList, &overturn_data);
    tmpList = list_concat(tmpList, overturn_data);

    GenReport::TableTranspose(tmpList, &dash->table);

    dash->dashTitle = "Report info";
    dash->tableTitle = "host info";
    desc = "host node info";
    dash->desc = lappend(dash->desc, desc);
    GenReport::add_data(dash, &params->Contents);

    /* free two-dimensional list of memory */
    DeepListFree(tmpList, false);
    pfree(query.data);
}

/*
 * Store data in structured memory
 */
void GenReport::add_data(dashboard* spec, List** Contents)
{
    bool H1_no_created = true;
    ListCell* dashCell = NULL;
    if (spec->dashTitle == NULL || spec->tableTitle == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("dashTitle or tableTitle  is null")));
    }

    /* check the kind of dash has create in Contents */
    foreach (dashCell, *Contents) {
        List* dashList = (List*)lfirst(dashCell);
        if (strcmp(spec->dashTitle, ((dashboard*)linitial(dashList))->dashTitle) == 0) {
            H1_no_created = false;
            dashList = lappend(dashList, spec);
            break;
        }
    }

    /* create new kind of dash */
    if (H1_no_created) {
        *Contents = lappend(*Contents, list_make1(spec));
    }
}

/*
 * Generate index for h1 title
 */
void GenReport::GenerateListByHtml(uint32 detailRow, List* Contents, StringInfoData& listHtml)
{
    if (detailRow > (uint32)list_length(Contents)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("out of the Contents")));
    }
    appendStringInfo(&listHtml, "<a class=\"wdr\" name=\"top\"></a><h2 class=\"wdr\">Report Details</h2>\n<ul>\n");
    uint32 i = 0;
    foreach_cell(cell, Contents)
    {
        if (i < detailRow) {
            i++;
            continue;
        }
        List* rowList = (List*)lfirst(cell);
        dashboard* dash = (dashboard*)linitial(rowList);
        appendStringInfo(&listHtml,
            "<li class=\"wdr\"><a class=\"wdr\" href=\"#%s\">%s</a></li>\n",
            dash->dashTitle,
            dash->dashTitle);
        i++;
    }
    appendStringInfo(&listHtml, "</ul>");
}

/*
 * Turn the space in the table title into an underscore,
 * and use the converted String for ID generation in HTML elements.
 * This method calls pstrdup method so you need to use pfree to free memory.
 * Input parameters:
 *        table -- the structed data
 */
static char* SpaceToUnderline(char* tableTitle)
{
    if (!tableTitle) {
        return tableTitle;
    }
    char* resultStr = pstrdup(tableTitle);
    for (unsigned i = 0; i < strlen(resultStr); i++) {
        if (resultStr[i] == ' ') {
            resultStr[i] = '_';
        }
    }
    return resultStr;
}

/*
 * GenerateTableList -- generate table list for dashboard
 */
static void GenerateTableList(List* rowList, StringInfoData& listHtml)
{
    char* htmlId = NULL;
    appendStringInfo(&listHtml, "<ul type=\"disc\">\n");
    foreach_cell(cell, rowList)
    {
        dashboard* dash = (dashboard*)lfirst(cell);
        htmlId = SpaceToUnderline(dash->tableTitle);
        appendStringInfo(&listHtml,
            "<li class=\"wdr\"><a class=\"wdr\" href=\"#%s\">%s</a></li>\n",
            htmlId,
            dash->tableTitle);
    }
    appendStringInfo(&listHtml, "</ul>");
    pfree_ext(htmlId);
}
/*
 *
 * generate index and detail psrt for report
 */
void GenReport::GenerateDetailHtml(List* Contents, StringInfoData& detailHtml)
{
    if (Contents == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("no available data for report")));
    }

    /* when report not need detail part */
    if (NUM_REPROT_PART == 0) {
        return;
    }
    GenReport::GenerateListByHtml(NUM_REPROT_PART, Contents, detailHtml);
    GenReport::GenerateDashboadsHtml(NUM_REPROT_PART, Contents, detailHtml);
}

/*
 * Convert all dashboard data to html format
 * One h1 corresponds to a dashboard
 * Input parameters:
 * stat : the mode of report
 */
void GenReport::GenerateDashboadsHtml(uint32 detailRow, List* Contents, StringInfoData& dashboadHtml)
{
    if (detailRow > (uint32)list_length(Contents)) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("out of the Contents")));
    }
    uint32 i = 0;
    foreach_cell(cell, Contents)
    {
        if (i < detailRow) {
            i++;
            continue;
        }
        List* rowList = (List*)lfirst(cell);
        dashboard* dash = (dashboard*)linitial(rowList);
        if (((dashboard*)linitial(rowList))->dashTitle == NULL) {
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("dashboard title is not assigned")));
        }
        appendStringInfo(&dashboadHtml,
            "<a class=\"wdr\" name=\"%s\"></a><h2 class=\"wdr\">%s</h2><ul>\n",
            dash->dashTitle,
            dash->dashTitle);
        GenerateTableList(rowList, dashboadHtml);
        GenReport::dashboad_to_html(rowList, dashboadHtml);
        appendStringInfoString(&dashboadHtml, "</ul>");
        i++;
    }
}
/*
 * DescToHtml
 * Convert the description of the table to html
 */
void GenReport::DescToHtml(List* descList, StringInfoData& descHtml)
{
    appendStringInfo(&descHtml, "<ul type=\"disc\">\n");

    foreach_cell(cell, descList)
    {
        char* desc = (char*)lfirst(cell);
        appendStringInfo(&descHtml, "<li class=\"wdr\">%s</li>\n", desc);
    }
    appendStringInfo(&descHtml, "</ul>\n");
}

/*
 * Convert one dashboard data to html format
 * Input parameters:
 *        row -- represents a dashboard
 */
void GenReport::dashboad_to_html(List* rowList, StringInfoData& dashboadHtml)
{
    char* prevTableTitle = NULL;
    foreach_cell(cell, rowList)
    {
        if (lfirst(cell) == NULL) {
            ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("table title is not assigned")));
        }
        dashboard* dash = (dashboard*)lfirst(cell);
        char* tableTitle = (char*)dash->tableTitle;
        char* htmlId = SpaceToUnderline(tableTitle);
        if (prevTableTitle == NULL || strcmp(tableTitle, prevTableTitle) != 0) {
            appendStringInfo(&dashboadHtml, "<a class=\"wdr\"></a><h3 class=\"wdr\" id=\"%s\" ", htmlId);
            appendStringInfo(&dashboadHtml,
                "onclick=\"return msg('%s', '%s1', '%s2', '%s')\">-%s</h3>\n",
                htmlId,
                htmlId,
                htmlId,
                tableTitle,
                tableTitle);

            /* Convert the description corresponding to the table to HTML format */
            appendStringInfo(&dashboadHtml,
                "<form>\n<input  id = '%s1'"
                " type=\"hidden\" value=\"0\"/>\n</form>\n<div id = '%s2'>\n",
                htmlId,
                htmlId);
        } else {
            appendStringInfo(&dashboadHtml, "<br>");
        }
        pfree_ext(htmlId);
        GenReport::DescToHtml(dash->desc, dashboadHtml);

        /* Convert the table data to HTML format (include the colname corresponding to the data) */
        if (dash->table == NULL) {
            ereport(WARNING, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("there is no available data in table")));
        } else {
            GenReport::TableToHtml(dash, dashboadHtml);
        }
        prevTableTitle = tableTitle;
        if (lnext(cell) != NULL) {
            dashboard* dashNext = (dashboard*)lfirst(lnext(cell));
            if (strcmp(tableTitle, dashNext->tableTitle) == 0) {
                continue;
            }
        }
        appendStringInfoString(&dashboadHtml,
            "</div>\n<p />\n<hr align=\"left\" width=\"20%\" /><p />\n"
            "<a class=\"wdr\" href=\"#");
        appendStringInfo(&dashboadHtml, "%s\">Back to %s", dash->dashTitle, dash->dashTitle);
        appendStringInfoString(&dashboadHtml, "</a><br /><a class=\"wdr\" href=\"#top\">Back to Top</a><p />\n<p />\n");
    }
}
/*
 * TableToHtml
 * Convert one table data to html
 *         row -- the row of table in Contents
 *         col -- the col of table in Contents
 */
void GenReport::TableToHtml(dashboard* dash, StringInfoData& tableHtml)
{
    appendStringInfo(
        &tableHtml, "<table border=\"0\" class=\"tdiff\" summary=\"This table displays %s\"><tr>\n", dash->tableTitle);

    /* Unique Query need set index from uniqueid to unique text */
    if (strcmp(dash->dashTitle, "SQL Statistics") == 0) {
        GenReport::table_to_Html(dash->table, dash->type, HAVE_SQLID, tableHtml);
    } else if (strcmp(dash->dashTitle, "SQL Detail") == 0) {
        GenReport::table_to_Html(dash->table, dash->type, HAVE_SQLTEXT, tableHtml);
    } else {
        GenReport::table_to_Html(dash->table, dash->type, HAVE_OTHER, tableHtml);
    }
    appendStringInfo(&tableHtml, "</table>");
}
/*
 * Traversing the tupe structure to generate html data
 *
 * Input parameters:
 *        table -- the structed data
 */
void GenReport::table_to_Html(List* tableList, List* typeList, ColName COL_HAVE, StringInfoData& tableHtml)
{
    const int TEXT_TYPE = 25;
    const int NAME_TYPE = 19;
    char* tableRowClz = NULL;

    Assert(list_length(tableList) >= 1);

    /* Traversing to get colname */
    foreach_cell(rowCell, (List*)linitial(tableList))
    {
        char* colName = (char*)lfirst(rowCell);
        appendStringInfo(&tableHtml, "<th class=\"wdrbg\" scope=\"col\">%s</th>", colName);
    }
    appendStringInfo(&tableHtml, "</tr>\n");

    /* Traversing the table to get the values */
    uint32 i = 0;
    for (ListCell* cell = lnext(list_head(tableList)); cell != NULL; cell = lnext(cell), i++) {
        /* Parity lines display different formats */
        if ((i & 1) == 0) {
            tableRowClz = "<td class=\"wdrnc\" ";
        } else {
            tableRowClz = "<td class=\"wdrc\" ";
        }
        uint32 j = 0;
        ListCell* typeCell = list_head(typeList);
        foreach_cell(rowCell, (List*)lfirst(cell))
        {
            char* cellData = (char*)lfirst(rowCell);
            if (j == 0 && COL_HAVE == HAVE_SQLID) {
                appendStringInfo(&tableHtml,
                    "%salign=\"center\" ><a class=\"wdr\" href=\"#%s\">%s",
                    tableRowClz,
                    cellData,
                    cellData);
                appendStringInfoString(&tableHtml, "</a></td>");
            } else if (j == 0 && COL_HAVE == HAVE_SQLTEXT) {
                appendStringInfo(
                    &tableHtml, "%s><a class=\"wdr\" name=\"%s\">%s", tableRowClz, cellData, cellData);
                appendStringInfoString(&tableHtml, "</a></td>");
            } else {
                if (*(Oid*)lfirst(typeCell) == NAME_TYPE || *(Oid*)lfirst(typeCell) == TEXT_TYPE) {
                    appendStringInfo(&tableHtml, "%salign=\"left\" >%s", tableRowClz, cellData);
                    appendStringInfoString(&tableHtml, "</td>");
                } else {
                    appendStringInfo(&tableHtml, "%salign=\"right\" >%s", tableRowClz, cellData);
                    appendStringInfoString(&tableHtml, "</td>");
                }
            }
            j++;
            typeCell = lnext(typeCell);
        }
        appendStringInfo(&tableHtml, "</tr>\n");
    }
}
static char* TimestampToStr(Datum value)
{
    Timestamp timestamp;
    struct pg_tm tm;
    fsec_t fsec;
    char buf[MAXDATELEN + 1] = {'\0'};

    timestamp = DatumGetTimestamp(value);
    /* XSD doesn't support infinite values */
    if (TIMESTAMP_NOT_FINITE(timestamp)) {
        ereport(ERROR,
            (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                errmsg("timestamp out of range"),
                errdetail("HTML does not support infinite timestamp values.")));
    } else if (timestamp2tm(timestamp, NULL, &tm, &fsec, NULL, NULL) == 0) {
        EncodeDateTime(&tm, fsec, false, 0, NULL, USE_XSD_DATES, buf);
    } else {
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("timestamp out of range")));
    }
    return pstrdup(buf);
}

static char* TimestampTzToStr(Datum value)
{
    TimestampTz timestamp;
    struct pg_tm tm;
    int tz;
    fsec_t fsec;
    const char* tzn = NULL;
    char buf[MAXDATELEN + 1] = {'\0'};

    timestamp = DatumGetTimestamp(value);
    /* XSD doesn't support infinite values */
    if (TIMESTAMP_NOT_FINITE(timestamp)) {
        ereport(ERROR,
            (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                errmsg("timestamp out of range"),
                errdetail("HTML does not support infinite timestamp values.")));
    } else if (timestamp2tm(timestamp, &tz, &tm, &fsec, &tzn, NULL) == 0) {
        EncodeDateTime(&tm, fsec, true, tz, tzn, USE_XSD_DATES, buf);
    } else {
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("timestamp out of range")));
    }
    return pstrdup(buf);
}
static char* GetStrVal(Datum value, Oid type, bool isnull)
{
    Oid typeOut;
    bool isvarlena = false;
    char* str = NULL;
    char* result = NULL;
    /*
     * Special XSD formatting for some data types
     */
    switch (type) {
        case BOOLOID:
            if (DatumGetBool(value)) {
                return "true";
            } else {
                return "false";
            }
        case DATEOID: {
            DateADT date;
            struct pg_tm tm;
            char buf[MAXDATELEN + 1];

            date = DatumGetDateADT(value);
            /* XSD doesn't support infinite values */
            if (DATE_NOT_FINITE(date)) {
                ereport(ERROR,
                    (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                        errmsg("date out of range"),
                        errdetail("HTML does not support infinite date values.")));
            }
            j2date(date + POSTGRES_EPOCH_JDATE, &(tm.tm_year), &(tm.tm_mon), &(tm.tm_mday));
            EncodeDateOnly(&tm, USE_XSD_DATES, buf);

            return pstrdup(buf);
        }
        case TIMESTAMPOID: {
            return TimestampToStr(value);
        }
        case TIMESTAMPTZOID: {
            return TimestampTzToStr(value);
        }
        case BYTEAOID: {
            result = text_to_cstring(DatumGetByteaPP(value));
            return result;
        }
        default:
            break;
    }

    /* otherwise, just use the type's native text representation */
    getTypeOutputInfo(type, &typeOut, &isvarlena);
    str = OidOutputFunctionCall(typeOut, value);
    if (type == XMLOID) { // to do?
        return str;
    }

    /* otherwise, translate special characters as needed */
    return escape_xml(str);
}

/*
 * Convert the value of the datum type of the tupe traversal to a string
 *  Input parameters:
 *         value -- Traversing the value of the datum type obtained by the table
 *         type -- The oid of pg_type corresponding to value
 */
char* Datum_to_string(Datum value, Oid type, bool isnull)
{
    if (isnull) {
        return "";
    }
    if (type_is_array_domain(type)) {
        ArrayType* array = NULL;
        Oid elmtype;
        int16 elmlen;
        bool elmbyval = false;
        char elmalign;
        int num_elems;
        Datum* elem_values = NULL;
        bool* elem_nulls = NULL;
        StringInfoData buf;
        int i;

        array = DatumGetArrayTypeP(value);
        elmtype = ARR_ELEMTYPE(array);
        get_typlenbyvalalign(elmtype, &elmlen, &elmbyval, &elmalign);

        deconstruct_array(array, elmtype, elmlen, elmbyval, elmalign, &elem_values, &elem_nulls, &num_elems);

        initStringInfo(&buf);

        for (i = 0; i < num_elems; i++) {
            if (elem_nulls[i]) {
                continue;
            }
            const char* tmp_var = Datum_to_string(elem_values[i], elmtype, true);
            if (tmp_var != NULL) {
                appendStringInfoString(&buf, tmp_var);
            }
        }
        pfree(elem_values);
        pfree(elem_nulls);

        return buf.data;
    } else {
        char* str = GetStrVal(value, type, isnull);
        return str;
    }
}

static char* transDisplayStr(Oid colType, char* valStr)
{
    int blankNum = 0;
    const Oid textOid = 25;
    if (valStr == NULL || colType == textOid) {
        return valStr;
    }

    /*
     * Check if the first character in the string is " ",
     * and remove the first space of the line for html display.
     */
    while (valStr[blankNum] == ' ') {
        blankNum++;
    }
    if (valStr[blankNum] == '.') {
        StringInfoData resetStr;
        initStringInfo(&resetStr);
        appendStringInfo(&resetStr, "0%s", valStr + blankNum);
        pfree(valStr);
        return resetStr.data;
    }
    return valStr;
}

/*
 * get_query_data - get query's result set as vector<vector>
 * every inner vector is one row in the report.
 *
 * if dashbord->table is empty vector, we need set column title for report,
 * if not, we will append record to existed vector, so should ignore
 * resultset column title
 */
void GenReport::get_query_data(char* query, bool with_column_name, List** cstring_values, List** columnType)
{
    Datum colval;
    List* colname_cstring = NIL;

    /* Establish SPI connection and get query execution result */
    if (query == NULL) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("query is null")));
    }

    ereport(DEBUG1, (errmodule(MOD_INSTR), errmsg("[Instruments/Report] query: %s", query)));

    if (SPI_execute(query, false, 0) != SPI_OK_SELECT) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid query")));
    }
    /* get colname */
    if (with_column_name) {
        for (int32 i = 0; i < SPI_tuptable->tupdesc->natts; i++) {
            size_t maxSize = strlen(SPI_tuptable->tupdesc->attrs[i].attname.data) + 1;
            char* strName = (char*)palloc(maxSize * sizeof(char));
            errno_t rc = strncpy_s(strName, maxSize, SPI_tuptable->tupdesc->attrs[i].attname.data, maxSize - 1);
            securec_check(rc, "\0", "\0");
            colname_cstring = lappend(colname_cstring, strName);
        }
        *cstring_values = lappend(*cstring_values, colname_cstring);
    }

    /* Get the data in the table and convert it to string format */
    for (uint32 i = 0; i < SPI_processed; i++) {
        List* row_string = NIL;
        uint32 colNum = (uint32)SPI_tuptable->tupdesc->natts;
        for (uint32 j = 1; j <= colNum; j++) {
            Oid* type = (Oid*)palloc(sizeof(Oid));
            *type = SPI_gettypeid(SPI_tuptable->tupdesc, (int)j);
            if (with_column_name) {
                if ((i + 1) == SPI_processed) {
                    *columnType = lappend(*columnType, type);
                }
            } else {
                if (j == colNum) {
                    *columnType = lappend(*columnType, type);
                }
            }
            bool isnull = false;
            colval = SPI_getbinval(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, (int)j, &isnull);
            char* originStr = Datum_to_string(colval, *type, isnull);
            char* string_value = transDisplayStr(*type, originStr);
            row_string = lappend(row_string, string_value);
        }
        *cstring_values = lappend(*cstring_values, row_string);
    }
}

/*
 * wapper for report type
 */
static bool is_summary_report(report_params* params)
{
    return (params != NULL) && (strncmp(params->report_type, g_summaryType, strlen(g_summaryType)) == 0);
}

static bool is_full_report(report_params* params)
{
    return params != NULL && (strncmp(params->report_type, g_allType, strlen(g_allType)) == 0);
}

/*
 * wrapper method for report scope
 */
static bool is_cluster_report(report_params* params)
{
    return params != NULL && (strncmp(params->report_scope, g_clusterScope, strlen(g_clusterScope)) == 0);
}

static bool is_single_node_report(report_params* params)
{
    return params != NULL && (strncmp(params->report_scope, g_nodeScope, strlen(g_nodeScope)) == 0);
}

/*
 * wrapper for report node
 */
static const char* get_report_node(report_params* params)
{
    if (params != NULL) {
        return params->report_node;
    }
    return NULL;
}

static int64 get_report_snap_gap(report_params* params)
{
    if (params != NULL) {
        return params->snap_gap;
    }
    return -1;
}

static int64 get_report_snap_diff_trx_count(report_params* params)
{
    if (params != NULL) {
        return params->snap_diff_trx_count;
    }
    return -1;
}

static uint64 get_report_snap_diff_sql_count(report_params* params)
{
    if (params != NULL) {
        return params->snap_diff_sql_count;
    }
    return 0;
}

/*
 * free Two-dimensional list
 */
void DeepListFree(List* deepList, bool deep)
{
    if (deepList == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("list is null, can not free")));
    }
    ListCell* cell = NULL;
    cell = list_head(deepList);
    while (cell != NULL) {
        ListCell* tmpCell = cell;
        cell = lnext(cell);
        if (deep) {
            list_free_deep((List*)lfirst(tmpCell));
        } else {
            list_free((List*)lfirst(tmpCell));
        }
        pfree(tmpCell);
    }
    if (deepList != NULL) {
        pfree(deepList);
    }
}

/*
 * TableTranspose -- overturn the table
 * Report table comes from a combination of multiple tables,
 * some tables may need to be overturned
 */
void GenReport::TableTranspose(List* tableData, List** overTurn)
{
    if (tableData == NULL) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("table is not null when overturn table")));
    }
    int rows = list_length(tableData);
    List** tmpOverturn = (List**)palloc0(sizeof(List*) * rows);
    foreach_cell(rowCell, tableData)
    {
        List* rowList = (List*)lfirst(rowCell);
        int j = 0;
        foreach_cell(cell, rowList)
        {
            tmpOverturn[j] = lappend(tmpOverturn[j], (char*)lfirst(cell));
            j++;
        }
    }
    for (int j = 0; j < rows; j++) {
        *overTurn = lappend(*overTurn, tmpOverturn[j]);
    }
}

/*
 * get wait event data for one node
 */
void GenReport::GetWaitEventsData(report_params* params)
{
    /* supported report type: detail/all */
    /* supported report scope: node */
    if (is_summary_report(params) || is_cluster_report(params)) {
        return;
    }
    dashboard* dash = CreateDash();
    char* desc = NULL;
    StringInfoData query;
    initStringInfo(&query);

    appendStringInfo(&query,
        "select t2.snap_type as \"Type\", t2.snap_event as \"Event\", "
        "(t2.snap_total_wait_time - coalesce(t1.snap_total_wait_time, 0)) as \"Total Wait Time (us)\","
        " (t2.snap_wait - coalesce(t1.snap_wait, 0)) as \"Waits\", (t2.snap_failed_wait - "
        "coalesce(t1.snap_failed_wait, 0)) as \"Failed Waits\", (case \"Waits\" when  0 then 0 else "
        "pg_catalog.round(\"Total Wait Time (us)\" / \"Waits\", 2) end) as \"Avg Wait Time (us)\", "
        "t2.snap_max_wait_time as \"Max Wait Time (us)\" from (select * from snapshot.snap_global_wait_events "
        "where snapshot_id = %ld and snap_nodename = '%s' and snap_event != 'unknown_lwlock_event' "
        "and snap_event != 'none') t1 right join "
        "(select * from snapshot.snap_global_wait_events where snapshot_id = %ld "
        "and snap_nodename = '%s' and snap_event != 'unknown_lwlock_event' and snap_event != 'none') t2"
        " on t1.snap_event = t2.snap_event order by \"Total Wait Time (us)\" desc limit 200",
        params->begin_snap_id,
        params->report_node,
        params->end_snap_id,
        params->report_node);

    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "Wait Events";
    dash->tableTitle = "Wait Events(by wait time)";
    desc = "The statistics of the wait event of a node are sorted by total_wait_time";
    dash->desc = lappend(dash->desc, desc);

    GenReport::add_data(dash, &params->Contents);

    dashboard* dash1 = CreateDash();
    resetStringInfo(&query);

    appendStringInfo(&query,
        "select t2.snap_type as \"Type\", t2.snap_event as  \"Event\", "
        "(t2.snap_wait - coalesce(t1.snap_wait, 0)) as \"Waits\","
        " (t2.snap_failed_wait - coalesce(t1.snap_failed_wait, 0)) "
        "as \"Failed Waits\", coalesce(t2.snap_total_wait_time - coalesce(t1.snap_total_wait_time, 0), 0) "
        "as \"Total Wait Time (us)\", (case \"Waits\" when  0 then 0 else pg_catalog.round(\"Total Wait Time (us)\" "
        "/ \"Waits\", 2) end) as \"Avg Wait Time (us)\", t2.snap_max_wait_time as \"Max Wait Time (us)\" "
        " from (select * from snapshot.snap_global_wait_events where snapshot_id = %ld and snap_nodename = '%s' "
        "and snap_event != 'unknown_lwlock_event' and snap_event != 'none') t1 right join (select * from "
        "snapshot.snap_global_wait_events where snapshot_id = %ld and snap_nodename = '%s' and "
        "snap_event != 'unknown_lwlock_event' and snap_event != 'none') t2 on t1.snap_event = t2.snap_event order "
        "by \"Waits\" desc limit 200",
        params->begin_snap_id,
        params->report_node,
        params->end_snap_id,
        params->report_node);
    GenReport::get_query_data(query.data, true, &dash1->table, &dash1->type);
    dash1->dashTitle = "Wait Events";
    dash1->tableTitle = "Wait Events(by waits)";
    desc = "The statistics of the wait event of a node are sorted by waits";
    dash1->desc = lappend(dash1->desc, desc);
    GenReport::add_data(dash1, &params->Contents);

    pfree_ext(query.data);
}

static void SQLNodeTotalElapsedTime(report_params* params)
{
    dashboard* dash = CreateDash();
    char* desc = NULL;
    char* note = NULL;
    StringInfoData query;
    initStringInfo(&query);

    /* SQL ordered by Total Elapsed Time */
    appendStringInfo(&query,
        "select t2.snap_unique_sql_id as \"Unique SQL Id\", t2.snap_user_name as \"User Name\","
        " (t2.snap_total_elapse_time - coalesce(t1.snap_total_elapse_time, 0)) as \"Total Elapse Time(us)\", "
        " (t2.snap_n_calls - coalesce(t1.snap_n_calls, 0)) as \"Calls\", "
        " pg_catalog.round(\"Total Elapse Time(us)\"/greatest(\"Calls\", 1), 0) as \"Avg Elapse Time(us)\", "
        " t2.snap_min_elapse_time as \"Min Elapse Time(us)\", t2.snap_max_elapse_time as \"Max Elapse Time(us)\", "
        " (t2.snap_n_returned_rows - coalesce(t1.snap_n_returned_rows, 0)) as \"Returned Rows\", "
        " ((t2.snap_n_tuples_fetched - coalesce(t1.snap_n_tuples_fetched, 0)) + "
        " (t2.snap_n_tuples_returned - coalesce(t1.snap_n_tuples_returned, 0))) as \"Tuples Read\", "
        " ((t2.snap_n_tuples_inserted - coalesce(t1.snap_n_tuples_inserted, 0)) + "
        " (t2.snap_n_tuples_updated - coalesce(t1.snap_n_tuples_updated, 0)) + "
        " (t2.snap_n_tuples_deleted - coalesce(t1.snap_n_tuples_deleted, 0))) as \"Tuples Affected\", "
        " (t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) as \"Logical Read\", "
        " ((t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) - "
        " (t2.snap_n_blocks_hit - coalesce(t1.snap_n_blocks_hit, 0))) as \"Physical Read\", "
        " (t2.snap_cpu_time - coalesce(t1.snap_cpu_time, 0)) as \"CPU Time(us)\", "
        " (t2.snap_data_io_time - coalesce(t1.snap_data_io_time, 0)) as \"Data IO Time(us)\", "
        " (t2.snap_sort_count - coalesce(t1.snap_sort_count, 0)) as \"Sort Count\", "
        " (t2.snap_sort_time - coalesce(t1.snap_sort_time, 0)) as \"Sort Time(us)\", "
        " (t2.snap_sort_mem_used - coalesce(t1.snap_sort_mem_used, 0)) as \"Sort Mem Used(KB)\", "
        " (t2.snap_sort_spill_count - coalesce(t1.snap_sort_spill_count, 0)) as \"Sort Spill Count\", "
        " (t2.snap_sort_spill_size - coalesce(t1.snap_sort_spill_size, 0)) as \"Sort Spill Size(KB)\", "
        " (t2.snap_hash_count - coalesce(t1.snap_hash_count, 0)) as \"Hash Count\", "
        " (t2.snap_hash_time - coalesce(t1.snap_hash_time, 0)) as \"Hash Time(us)\", "
        " (t2.snap_hash_mem_used - coalesce(t1.snap_hash_mem_used, 0)) as \"Hash Mem Used(KB)\", "
        " (t2.snap_hash_spill_count - coalesce(t1.snap_hash_spill_count, 0)) as \"Hash Spill Count\", "
        " (t2.snap_hash_spill_size - coalesce(t1.snap_hash_spill_size, 0)) as \"Hash Spill Size(KB)\", "
        " LEFT(t2.snap_query, 25) as \"SQL Text\" "
        "  from (select * from snapshot.snap_summary_statement where snapshot_id = %ld and snap_node_name = '%s') t1"
        " right join "
        " (select * from snapshot.snap_summary_statement where snapshot_id = %ld and snap_node_name = '%s') t2"
        " on t1.snap_unique_sql_id = t2.snap_unique_sql_id and t1.snap_user_id = t2.snap_user_id order by \"Total "
        "Elapse Time(us)\" desc limit 200;",
        params->begin_snap_id,
        params->report_node,
        params->end_snap_id,
        params->report_node);

    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "SQL Statistics";
    dash->tableTitle = "SQL ordered by Elapsed Time";
    desc = "SQL ordered by Elapsed Time";
    note = "List top 200 records";
    dash->desc = lappend(dash->desc, desc);
    dash->desc = lappend(dash->desc, note);
    note = "Avg Elapse Time(us) represents the average elapsed time of sql between two snapshots";
    dash->desc = lappend(dash->desc, note);
    GenReport::add_data(dash, &params->Contents);
    pfree_ext(query.data);
}

static void SQLNodeCPUTime(report_params* params)
{
    dashboard* dash = CreateDash();
    char* desc = NULL;
    char* note = NULL;
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "select t2.snap_unique_sql_id as \"Unique SQL Id\", t2.snap_user_name as \"User Name\","
        " (t2.snap_cpu_time - coalesce(t1.snap_cpu_time, 0)) as \"CPU Time(us)\", "
        " (t2.snap_n_calls - coalesce(t1.snap_n_calls, 0)) as \"Calls\", "
        " t2.snap_min_elapse_time as \"Min Elapse Time(us)\", t2.snap_max_elapse_time as \"Max Elapse Time(us)\", "
        " (t2.snap_total_elapse_time - coalesce(t1.snap_total_elapse_time, 0)) as \"Total Elapse Time(us)\", "
        " pg_catalog.round(\"Total Elapse Time(us)\"/greatest(\"Calls\", 1), 0) as \"Avg Elapse Time(us)\", "
        " (t2.snap_n_returned_rows - coalesce(t1.snap_n_returned_rows, 0)) as \"Returned Rows\", "
        " ((t2.snap_n_tuples_fetched - coalesce(t1.snap_n_tuples_fetched, 0)) + "
        " (t2.snap_n_tuples_returned - coalesce(t1.snap_n_tuples_returned, 0))) as \"Tuples Read\", "
        " ((t2.snap_n_tuples_inserted - coalesce(t1.snap_n_tuples_inserted, 0)) + "
        " (t2.snap_n_tuples_updated - coalesce(t1.snap_n_tuples_updated, 0)) + "
        " (t2.snap_n_tuples_deleted - coalesce(t1.snap_n_tuples_deleted, 0))) as \"Tuples Affected\", "
        " (t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) as \"Logical Read\", "
        " ((t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) - "
        " (t2.snap_n_blocks_hit - coalesce(t1.snap_n_blocks_hit, 0))) as \"Physical Read\", "
        " (t2.snap_data_io_time - coalesce(t1.snap_data_io_time, 0)) as \"Data IO Time(us)\", "
        " (t2.snap_sort_count - coalesce(t1.snap_sort_count, 0)) as \"Sort Count\", "
        " (t2.snap_sort_time - coalesce(t1.snap_sort_time, 0)) as \"Sort Time(us)\", "
        " (t2.snap_sort_mem_used - coalesce(t1.snap_sort_mem_used, 0)) as \"Sort Mem Used(KB)\", "
        " (t2.snap_sort_spill_count - coalesce(t1.snap_sort_spill_count, 0)) as \"Sort Spill Count\", "
        " (t2.snap_sort_spill_size - coalesce(t1.snap_sort_spill_size, 0)) as \"Sort Spill Size(KB)\", "
        " (t2.snap_hash_count - coalesce(t1.snap_hash_count, 0)) as \"Hash Count\", "
        " (t2.snap_hash_time - coalesce(t1.snap_hash_time, 0)) as \"Hash Time(us)\", "
        " (t2.snap_hash_mem_used - coalesce(t1.snap_hash_mem_used, 0)) as \"Hash Mem Used(KB)\", "
        " (t2.snap_hash_spill_count - coalesce(t1.snap_hash_spill_count, 0)) as \"Hash Spill Count\", "
        " (t2.snap_hash_spill_size - coalesce(t1.snap_hash_spill_size, 0)) as \"Hash Spill Size(KB)\", "
        " LEFT(t2.snap_query, 25) as \"SQL Text\" "
        "  from (select * from snapshot.snap_summary_statement where snapshot_id = %ld and snap_node_name = '%s') t1"
        " right join "
        " (select * from snapshot.snap_summary_statement where snapshot_id = %ld and snap_node_name = '%s') t2"
        " on t1.snap_unique_sql_id = t2.snap_unique_sql_id and t1.snap_user_id = t2.snap_user_id "
        "order by \"CPU Time(us)\" desc limit 200;",
        params->begin_snap_id,
        params->report_node,
        params->end_snap_id,
        params->report_node);
    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "SQL Statistics";
    dash->tableTitle = "SQL ordered by CPU Time";
    desc = "SQL ordered by CPU Time";
    note = "List top 200 records";
    dash->desc = lappend(dash->desc, desc);
    dash->desc = lappend(dash->desc, note);
    note = "Avg Elapse Time(us) represents the average elapsed time of sql between two snapshots";
    dash->desc = lappend(dash->desc, note);
    GenReport::add_data(dash, &params->Contents);

    pfree_ext(query.data);
}

static void SQLNodeRowsReturned(report_params* params)
{
    dashboard* dash = CreateDash();
    char* desc = NULL;
    char* note = NULL;
    StringInfoData query;
    initStringInfo(&query);

    appendStringInfo(&query,
        "select t2.snap_unique_sql_id as \"Unique SQL Id\", t2.snap_user_name as \"User Name\","
        " (t2.snap_n_returned_rows - coalesce(t1.snap_n_returned_rows, 0)) as \"Returned Rows\", "
        " (t2.snap_n_calls - coalesce(t1.snap_n_calls, 0)) as \"Calls\", "
        " t2.snap_min_elapse_time as \"Min Elapse Time(us)\", t2.snap_max_elapse_time as \"Max Elapse Time(us)\","
        " (t2.snap_total_elapse_time - coalesce(t1.snap_total_elapse_time, 0)) as \"Total Elapse Time(us)\", "
        " pg_catalog.round(\"Total Elapse Time(us)\"/greatest(\"Calls\", 1), 0) as \"Avg Elapse Time(us)\","
        " ((t2.snap_n_tuples_fetched - coalesce(t1.snap_n_tuples_fetched, 0)) + "
        " (t2.snap_n_tuples_returned - coalesce(t1.snap_n_tuples_returned, 0))) as \"Tuples Read\", "
        " ((t2.snap_n_tuples_inserted - coalesce(t1.snap_n_tuples_inserted, 0)) + "
        " (t2.snap_n_tuples_updated - coalesce(t1.snap_n_tuples_updated, 0)) + "
        " (t2.snap_n_tuples_deleted - coalesce(t1.snap_n_tuples_deleted, 0))) as \"Tuples Affected\", "
        " (t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) as \"Logical Read\", "
        " ((t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) - "
        " (t2.snap_n_blocks_hit - coalesce(t1.snap_n_blocks_hit, 0))) as \"Physical Read\", "
        " (t2.snap_cpu_time - coalesce(t1.snap_cpu_time, 0)) as \"CPU Time(us)\", "
        " (t2.snap_data_io_time - coalesce(t1.snap_data_io_time, 0)) as \"Data IO Time(us)\", "
        " (t2.snap_sort_count - coalesce(t1.snap_sort_count, 0)) as \"Sort Count\", "
        " (t2.snap_sort_time - coalesce(t1.snap_sort_time, 0)) as \"Sort Time(us)\", "
        " (t2.snap_sort_mem_used - coalesce(t1.snap_sort_mem_used, 0)) as \"Sort Mem Used(KB)\", "
        " (t2.snap_sort_spill_count - coalesce(t1.snap_sort_spill_count, 0)) as \"Sort Spill Count\", "
        " (t2.snap_sort_spill_size - coalesce(t1.snap_sort_spill_size, 0)) as \"Sort Spill Size(KB)\", "
        " (t2.snap_hash_count - coalesce(t1.snap_hash_count, 0)) as \"Hash Count\", "
        " (t2.snap_hash_time - coalesce(t1.snap_hash_time, 0)) as \"Hash Time(us)\", "
        " (t2.snap_hash_mem_used - coalesce(t1.snap_hash_mem_used, 0)) as \"Hash Mem Used(KB)\", "
        " (t2.snap_hash_spill_count - coalesce(t1.snap_hash_spill_count, 0)) as \"Hash Spill Count\", "
        " (t2.snap_hash_spill_size - coalesce(t1.snap_hash_spill_size, 0)) as \"Hash Spill Size(KB)\", "
        " LEFT(t2.snap_query, 25) as \"SQL Text\" "
        "  from (select * from snapshot.snap_summary_statement where snapshot_id = %ld and snap_node_name = '%s') t1"
        " right join "
        " (select * from snapshot.snap_summary_statement where snapshot_id = %ld and snap_node_name = '%s') t2"
        " on t1.snap_unique_sql_id = t2.snap_unique_sql_id and t1.snap_user_id = t2.snap_user_id "
        "order by \"Returned Rows\" desc limit 200;",
        params->begin_snap_id,
        params->report_node,
        params->end_snap_id,
        params->report_node);
    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "SQL Statistics";
    dash->tableTitle = "SQL ordered by Rows Returned";
    desc = "SQL ordered by Rows Returned";
    note = "List top 200 records";
    dash->desc = lappend(dash->desc, desc);
    dash->desc = lappend(dash->desc, note);
    note = "Avg Elapse Time(us) represents the average elapsed time of sql between two snapshots";
    dash->desc = lappend(dash->desc, note);
    GenReport::add_data(dash, &params->Contents);

    pfree_ext(query.data);
}

static void SQLNodeRowRead(report_params* params)
{
    dashboard* dash = CreateDash();
    char* desc = NULL;
    char* note = NULL;
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "select t2.snap_unique_sql_id as \"Unique SQL Id\", t2.snap_user_name as \"User Name\","
        " ((t2.snap_n_tuples_fetched - coalesce(t1.snap_n_tuples_fetched, 0)) + "
        " (t2.snap_n_tuples_returned - coalesce(t1.snap_n_tuples_returned, 0))) as \"Tuples Read\", "
        " (t2.snap_n_calls - coalesce(t1.snap_n_calls, 0)) as \"Calls\", "
        " t2.snap_min_elapse_time as \"Min Elapse Time(us)\", t2. snap_max_elapse_time as \"Max Elapse Time(us)\", "
        " (t2.snap_total_elapse_time - coalesce(t1.snap_total_elapse_time, 0)) as \"Total Elapse Time(us)\", "
        " pg_catalog.round(\"Total Elapse Time(us)\"/greatest(\"Calls\", 1), 0) as \"Avg Elapse Time(us)\", "
        " (t2.snap_n_returned_rows - coalesce(t1.snap_n_returned_rows, 0)) as \"Returned Rows\", "
        " ((t2.snap_n_tuples_inserted - coalesce(t1.snap_n_tuples_inserted, 0)) + "
        " (t2.snap_n_tuples_updated - coalesce(t1.snap_n_tuples_updated, 0)) + "
        " (t2.snap_n_tuples_deleted - coalesce(t1.snap_n_tuples_deleted, 0))) as \"Tuples Affected\", "
        " (t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) as \"Logical Read\", "
        " ((t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) - "
        " (t2.snap_n_blocks_hit - coalesce(t1.snap_n_blocks_hit, 0))) as \"Physical Read\", "
        " (t2.snap_cpu_time - coalesce(t1.snap_cpu_time, 0)) as \"CPU Time(us)\", "
        " (t2.snap_data_io_time - coalesce(t1.snap_data_io_time, 0)) as \"Data IO Time(us)\", "
        " (t2.snap_sort_count - coalesce(t1.snap_sort_count, 0)) as \"Sort Count\", "
        " (t2.snap_sort_time - coalesce(t1.snap_sort_time, 0)) as \"Sort Time(us)\", "
        " (t2.snap_sort_mem_used - coalesce(t1.snap_sort_mem_used, 0)) as \"Sort Mem Used(KB)\", "
        " (t2.snap_sort_spill_count - coalesce(t1.snap_sort_spill_count, 0)) as \"Sort Spill Count\", "
        " (t2.snap_sort_spill_size - coalesce(t1.snap_sort_spill_size, 0)) as \"Sort Spill Size(KB)\", "
        " (t2.snap_hash_count - coalesce(t1.snap_hash_count, 0)) as \"Hash Count\", "
        " (t2.snap_hash_time - coalesce(t1.snap_hash_time, 0)) as \"Hash Time(us)\", "
        " (t2.snap_hash_mem_used - coalesce(t1.snap_hash_mem_used, 0)) as \"Hash Mem Used(KB)\", "
        " (t2.snap_hash_spill_count - coalesce(t1.snap_hash_spill_count, 0)) as \"Hash Spill Count\", "
        " (t2.snap_hash_spill_size - coalesce(t1.snap_hash_spill_size, 0)) as \"Hash Spill Size(KB)\", "
        " LEFT(t2.snap_query, 25) as \"SQL Text\" "
        "  from (select * from snapshot.snap_summary_statement where snapshot_id = %ld and snap_node_name = '%s') t1"
        " right join "
        " (select * from snapshot.snap_summary_statement where snapshot_id = %ld and snap_node_name = '%s') t2"
        " on t1.snap_unique_sql_id = t2.snap_unique_sql_id and t1.snap_user_id = t2.snap_user_id "
        "order by \"Tuples Read\" desc limit 200;",
        params->begin_snap_id,
        params->report_node,
        params->end_snap_id,
        params->report_node);
    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "SQL Statistics";
    dash->tableTitle = "SQL ordered by Tuples Reads";
    desc = "SQL ordered by Tuples Reads";
    note = "List top 200 records";
    dash->desc = lappend(dash->desc, desc);
    dash->desc = lappend(dash->desc, note);
    note = "Avg Elapse Time(us) represents the average elapsed time of sql between two snapshots";
    dash->desc = lappend(dash->desc, note);
    GenReport::add_data(dash, &params->Contents);
    pfree_ext(query.data);
}

static void SQLNodeExecutions(report_params* params)
{
    dashboard* dash = CreateDash();
    char* desc = NULL;
    char* note = NULL;
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "select t2.snap_unique_sql_id as \"Unique SQL Id\", t2.snap_user_name as \"User Name\","
        " (t2.snap_n_calls - coalesce(t1.snap_n_calls, 0)) as \"Calls\", "
        " t2.snap_min_elapse_time as \"Min Elapse Time(us)\", t2.snap_max_elapse_time as \"Max Elapse Time(us)\","
        " (t2.snap_total_elapse_time - coalesce(t1.snap_total_elapse_time, 0)) as \"Total Elapse Time(us)\", "
        " pg_catalog.round(\"Total Elapse Time(us)\"/greatest(\"Calls\", 1), 0) as \"Avg Elapse Time(us)\","
        " (t2.snap_n_returned_rows - coalesce(t1.snap_n_returned_rows, 0)) as \"Returned Rows\", "
        " ((t2.snap_n_tuples_fetched - coalesce(t1.snap_n_tuples_fetched, 0)) + "
        " (t2.snap_n_tuples_returned - coalesce(t1.snap_n_tuples_returned, 0))) as \"Tuples Read\", "
        " ((t2.snap_n_tuples_inserted - coalesce(t1.snap_n_tuples_inserted, 0)) + "
        " (t2.snap_n_tuples_updated - coalesce(t1.snap_n_tuples_updated, 0)) + "
        " (t2.snap_n_tuples_deleted - coalesce(t1.snap_n_tuples_deleted, 0))) as \"Tuples Affected\", "
        " (t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) as \"Logical Read\", "
        " ((t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) - "
        " (t2.snap_n_blocks_hit - coalesce(t1.snap_n_blocks_hit, 0))) as \"Physical Read\", "
        " (t2.snap_cpu_time - coalesce(t1.snap_cpu_time, 0)) as \"CPU Time(us)\", "
        " (t2.snap_data_io_time - coalesce(t1.snap_data_io_time, 0)) as \"Data IO Time(us)\", "
        " (t2.snap_sort_count - coalesce(t1.snap_sort_count, 0)) as \"Sort Count\", "
        " (t2.snap_sort_time - coalesce(t1.snap_sort_time, 0)) as \"Sort Time(us)\", "
        " (t2.snap_sort_mem_used - coalesce(t1.snap_sort_mem_used, 0)) as \"Sort Mem Used(KB)\", "
        " (t2.snap_sort_spill_count - coalesce(t1.snap_sort_spill_count, 0)) as \"Sort Spill Count\", "
        " (t2.snap_sort_spill_size - coalesce(t1.snap_sort_spill_size, 0)) as \"Sort Spill Size(KB)\", "
        " (t2.snap_hash_count - coalesce(t1.snap_hash_count, 0)) as \"Hash Count\", "
        " (t2.snap_hash_time - coalesce(t1.snap_hash_time, 0)) as \"Hash Time(us)\", "
        " (t2.snap_hash_mem_used - coalesce(t1.snap_hash_mem_used, 0)) as \"Hash Mem Used(KB)\", "
        " (t2.snap_hash_spill_count - coalesce(t1.snap_hash_spill_count, 0)) as \"Hash Spill Count\", "
        " (t2.snap_hash_spill_size - coalesce(t1.snap_hash_spill_size, 0)) as \"Hash Spill Size(KB)\", "
        " LEFT(t2.snap_query, 25) as \"SQL Text\" "
        "  from (select * from snapshot.snap_summary_statement where snapshot_id = %ld and snap_node_name = '%s') t1"
        " right join "
        " (select * from snapshot.snap_summary_statement where snapshot_id = %ld and snap_node_name = '%s') t2"
        " on t1.snap_unique_sql_id = t2.snap_unique_sql_id and t1.snap_user_id = t2.snap_user_id "
        "order by \"Calls\" desc limit 200;",
        params->begin_snap_id,
        params->report_node,
        params->end_snap_id,
        params->report_node);
    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "SQL Statistics";
    dash->tableTitle = "SQL ordered by Executions";
    desc = "SQL ordered by Executions";
    note = "List top 200 records";
    dash->desc = lappend(dash->desc, desc);
    dash->desc = lappend(dash->desc, note);
    note = "Avg Elapse Time(us) represents the average elapsed time of sql between two snapshots";
    dash->desc = lappend(dash->desc, note);
    GenReport::add_data(dash, &params->Contents);

    pfree_ext(query.data);
}

static void SQLNodePhysicalReads(report_params* params)
{
    dashboard* dash = CreateDash();
    char* desc = NULL;
    char* note = NULL;
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "select t2.snap_unique_sql_id as \"Unique SQL Id\", t2.snap_user_name as \"User Name\","
        " ((t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) - "
        " (t2.snap_n_blocks_hit - coalesce(t1.snap_n_blocks_hit, 0))) as \"Physical Read\", "
        " (t2.snap_n_calls - coalesce(t1.snap_n_calls, 0)) as \"Calls\", "
        " t2.snap_min_elapse_time as \"Min Elapse Time(us)\", t2. snap_max_elapse_time as \"Max Elapse Time(us)\", "
        " (t2.snap_total_elapse_time - coalesce(t1.snap_total_elapse_time, 0)) as \"Total Elapse Time(us)\", "
        " pg_catalog.round(\"Total Elapse Time(us)\"/greatest(\"Calls\", 1), 0) as \"Avg Elapse Time(us)\", "
        " (t2.snap_n_returned_rows - coalesce(t1.snap_n_returned_rows, 0)) as \"Returned Rows\", "
        " ((t2.snap_n_tuples_fetched - coalesce(t1.snap_n_tuples_fetched, 0)) + "
        " (t2.snap_n_tuples_returned - coalesce(t1.snap_n_tuples_returned, 0))) as \"Tuples Read\", "
        " ((t2.snap_n_tuples_inserted - coalesce(t1.snap_n_tuples_inserted, 0)) + "
        " (t2.snap_n_tuples_updated - coalesce(t1.snap_n_tuples_updated, 0)) + "
        " (t2.snap_n_tuples_deleted - coalesce(t1.snap_n_tuples_deleted, 0))) as \"Tuples Affected\", "
        " (t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) as \"Logical Read\", "
        " (t2.snap_cpu_time - coalesce(t1.snap_cpu_time, 0)) as \"CPU Time(us)\", "
        " (t2.snap_data_io_time - coalesce(t1.snap_data_io_time, 0)) as \"Data IO Time(us)\", "
        " (t2.snap_sort_count - coalesce(t1.snap_sort_count, 0)) as \"Sort Count\", "
        " (t2.snap_sort_time - coalesce(t1.snap_sort_time, 0)) as \"Sort Time(us)\", "
        " (t2.snap_sort_mem_used - coalesce(t1.snap_sort_mem_used, 0)) as \"Sort Mem Used(KB)\", "
        " (t2.snap_sort_spill_count - coalesce(t1.snap_sort_spill_count, 0)) as \"Sort Spill Count\", "
        " (t2.snap_sort_spill_size - coalesce(t1.snap_sort_spill_size, 0)) as \"Sort Spill Size(KB)\", "
        " (t2.snap_hash_count - coalesce(t1.snap_hash_count, 0)) as \"Hash Count\", "
        " (t2.snap_hash_time - coalesce(t1.snap_hash_time, 0)) as \"Hash Time(us)\", "
        " (t2.snap_hash_mem_used - coalesce(t1.snap_hash_mem_used, 0)) as \"Hash Mem Used(KB)\", "
        " (t2.snap_hash_spill_count - coalesce(t1.snap_hash_spill_count, 0)) as \"Hash Spill Count\", "
        " (t2.snap_hash_spill_size - coalesce(t1.snap_hash_spill_size, 0)) as \"Hash Spill Size(KB)\", "
        " LEFT(t2.snap_query, 25) as \"SQL Text\" "
        "  from (select * from snapshot.snap_summary_statement where snapshot_id = %ld and snap_node_name = '%s') t1"
        " right join "
        " (select * from snapshot.snap_summary_statement where snapshot_id = %ld and snap_node_name = '%s') t2"
        " on t1.snap_unique_sql_id = t2.snap_unique_sql_id and t1.snap_user_id = t2.snap_user_id "
        "order by \"Physical Read\" desc limit 200;",
        params->begin_snap_id,
        params->report_node,
        params->end_snap_id,
        params->report_node);
    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "SQL Statistics";
    dash->tableTitle = "SQL ordered by Physical Reads";
    desc = "SQL ordered by Physical Reads";
    note = "List top 200 records";
    dash->desc = lappend(dash->desc, desc);
    dash->desc = lappend(dash->desc, note);
    note = "Avg Elapse Time(us) represents the average elapsed time of sql between two snapshots";
    dash->desc = lappend(dash->desc, note);
    GenReport::add_data(dash, &params->Contents);
    pfree_ext(query.data);
}
static void SQLNodeLogicalReads(report_params* params)
{
    dashboard* dash = CreateDash();
    char* desc = NULL;
    char* note = NULL;
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "select t2.snap_unique_sql_id as \"Unique SQL Id\", t2.snap_user_name as \"User Name\","
        " (t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) as \"Logical Read\", "
        " (t2.snap_n_calls - coalesce(t1.snap_n_calls, 0)) as \"Calls\", "
        " t2.snap_min_elapse_time as \"Min Elapse Time(us)\", t2.snap_max_elapse_time as \"Max Elapse Time(us)\","
        " (t2.snap_total_elapse_time - coalesce(t1.snap_total_elapse_time, 0)) as \"Total Elapse Time(us)\", "
        " pg_catalog.round(\"Total Elapse Time(us)\"/greatest(\"Calls\", 1), 0) as \"Avg Elapse Time(us)\","
        " (t2.snap_n_returned_rows - coalesce(t1.snap_n_returned_rows, 0)) as \"Returned Rows\", "
        " ((t2.snap_n_tuples_fetched - coalesce(t1.snap_n_tuples_fetched, 0)) + "
        " (t2.snap_n_tuples_returned - coalesce(t1.snap_n_tuples_returned, 0))) as \"Tuples Read\", "
        " ((t2.snap_n_tuples_inserted - coalesce(t1.snap_n_tuples_inserted, 0)) + "
        " (t2.snap_n_tuples_updated - coalesce(t1.snap_n_tuples_updated, 0)) + "
        " (t2.snap_n_tuples_deleted - coalesce(t1.snap_n_tuples_deleted, 0))) as \"Tuples Affected\", "
        " ((t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) - "
        " (t2.snap_n_blocks_hit - coalesce(t1.snap_n_blocks_hit, 0))) as \"Physical Read\", "
        " (t2.snap_cpu_time - coalesce(t1.snap_cpu_time, 0)) as \"CPU Time(us)\", "
        " (t2.snap_data_io_time - coalesce(t1.snap_data_io_time, 0)) as \"Data IO Time(us)\", "
        " (t2.snap_sort_count - coalesce(t1.snap_sort_count, 0)) as \"Sort Count\", "
        " (t2.snap_sort_time - coalesce(t1.snap_sort_time, 0)) as \"Sort Time(us)\", "
        " (t2.snap_sort_mem_used - coalesce(t1.snap_sort_mem_used, 0)) as \"Sort Mem Used(KB)\", "
        " (t2.snap_sort_spill_count - coalesce(t1.snap_sort_spill_count, 0)) as \"Sort Spill Count\", "
        " (t2.snap_sort_spill_size - coalesce(t1.snap_sort_spill_size, 0)) as \"Sort Spill Size(KB)\", "
        " (t2.snap_hash_count - coalesce(t1.snap_hash_count, 0)) as \"Hash Count\", "
        " (t2.snap_hash_time - coalesce(t1.snap_hash_time, 0)) as \"Hash Time(us)\", "
        " (t2.snap_hash_mem_used - coalesce(t1.snap_hash_mem_used, 0)) as \"Hash Mem Used(KB)\", "
        " (t2.snap_hash_spill_count - coalesce(t1.snap_hash_spill_count, 0)) as \"Hash Spill Count\", "
        " (t2.snap_hash_spill_size - coalesce(t1.snap_hash_spill_size, 0)) as \"Hash Spill Size(KB)\", "
        " LEFT(t2.snap_query, 25) as \"SQL Text\" "
        "  from (select * from snapshot.snap_summary_statement where snapshot_id = %ld and snap_node_name = '%s') t1"
        " right join "
        " (select * from snapshot.snap_summary_statement where snapshot_id = %ld and snap_node_name = '%s') t2"
        " on t1.snap_unique_sql_id = t2.snap_unique_sql_id and t1.snap_user_id = t2.snap_user_id "
        "order by \"Logical Read\" desc limit 200;",
        params->begin_snap_id,
        params->report_node,
        params->end_snap_id,
        params->report_node);
    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "SQL Statistics";
    dash->tableTitle = "SQL ordered by Logical Reads";
    desc = "SQL ordered by Logical Reads";
    note = "List top 200 records";
    dash->desc = lappend(dash->desc, desc);
    dash->desc = lappend(dash->desc, note);
    note = "Avg Elapse Time(us) represents the average elapsed time of sql between two snapshots";
    dash->desc = lappend(dash->desc, note);
    GenReport::add_data(dash, &params->Contents);
    pfree_ext(query.data);
}
void GenReport::GetNodeSQLStatisticsData(report_params* params)
{
    /* SQL ordered by Total Elapsed Time */
    SQLNodeTotalElapsedTime(params);
    /* SQL ordered by CPU Time */
    SQLNodeCPUTime(params);
    /* SQL ordered by Rows Returned */
    SQLNodeRowsReturned(params);

    /* SQL ordered by Row Read */
    SQLNodeRowRead(params);
    /* SQL ordered by Executions */
    SQLNodeExecutions(params);

    /* SQL ordered by Physical Reads */
    SQLNodePhysicalReads(params);

    /* SQL ordered by Logical Reads */
    SQLNodeLogicalReads(params);
}

static void SQLclusterElapsedTime(report_params* params)
{
    dashboard* dash = CreateDash();
    char* desc = NULL;
    StringInfoData query;
    initStringInfo(&query);

    appendStringInfo(&query,
        "select t2.snap_unique_sql_id as \"Unique SQL Id\", "
        " t2.snap_node_name as \"Node Name\", t2.snap_user_name as \"User Name\","
        " (t2.snap_total_elapse_time - coalesce(t1.snap_total_elapse_time, 0)) as \"Total Elapse Time(us)\", "
        " (t2.snap_n_calls - coalesce(t1.snap_n_calls, 0)) as \"Calls\", "
        " pg_catalog.round(\"Total Elapse Time(us)\"/greatest(\"Calls\", 1), 0) as \"Avg Elapse Time(us)\", "
        " t2.snap_min_elapse_time as \"Min Elapse Time(us)\", t2.snap_max_elapse_time as \"Max Elapse Time(us)\", "
        " (t2.snap_n_returned_rows - coalesce(t1.snap_n_returned_rows, 0)) as \"Returned Rows\", "
        " ((t2.snap_n_tuples_fetched - coalesce(t1.snap_n_tuples_fetched, 0)) + "
        " (t2.snap_n_tuples_returned - coalesce(t1.snap_n_tuples_returned, 0))) as \"Tuples Read\", "
        " ((t2.snap_n_tuples_inserted - coalesce(t1.snap_n_tuples_inserted, 0)) + "
        " (t2.snap_n_tuples_updated - coalesce(t1.snap_n_tuples_updated, 0)) + "
        " (t2.snap_n_tuples_deleted - coalesce(t1.snap_n_tuples_deleted, 0))) as \"Tuples Affected\", "
        " (t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) as \"Logical Read\", "
        " ((t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) - "
        " (t2.snap_n_blocks_hit - coalesce(t1.snap_n_blocks_hit, 0))) as \"Physical Read\", "
        " (t2.snap_cpu_time - coalesce(t1.snap_cpu_time, 0)) as \"CPU Time(us)\", "
        " (t2.snap_data_io_time - coalesce(t1.snap_data_io_time, 0)) as \"Data IO Time(us)\", "
        " (t2.snap_sort_count - coalesce(t1.snap_sort_count, 0)) as \"Sort Count\", "
        " (t2.snap_sort_time - coalesce(t1.snap_sort_time, 0)) as \"Sort Time(us)\", "
        " (t2.snap_sort_mem_used - coalesce(t1.snap_sort_mem_used, 0)) as \"Sort Mem Used(KB)\", "
        " (t2.snap_sort_spill_count - coalesce(t1.snap_sort_spill_count, 0)) as \"Sort Spill Count\", "
        " (t2.snap_sort_spill_size - coalesce(t1.snap_sort_spill_size, 0)) as \"Sort Spill Size(KB)\", "
        " (t2.snap_hash_count - coalesce(t1.snap_hash_count, 0)) as \"Hash Count\", "
        " (t2.snap_hash_time - coalesce(t1.snap_hash_time, 0)) as \"Hash Time(us)\", "
        " (t2.snap_hash_mem_used - coalesce(t1.snap_hash_mem_used, 0)) as \"Hash Mem Used(KB)\", "
        " (t2.snap_hash_spill_count - coalesce(t1.snap_hash_spill_count, 0)) as \"Hash Spill Count\", "
        " (t2.snap_hash_spill_size - coalesce(t1.snap_hash_spill_size, 0)) as \"Hash Spill Size(KB)\", "
        " LEFT(t2.snap_query, 25) as \"SQL Text\" "
        "  from (select * from snapshot.snap_summary_statement where snapshot_id = %ld) t1"
        " right join (select * from snapshot.snap_summary_statement where snapshot_id = %ld) t2"
        " on t1.snap_unique_sql_id = t2.snap_unique_sql_id and t1.snap_user_id = t2.snap_user_id "
        " and t1.snap_node_name = t2.snap_node_name order by \"Total Elapse Time(us)\" desc limit 200;",
        params->begin_snap_id,
        params->end_snap_id);

    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "SQL Statistics";
    dash->tableTitle = "SQL ordered by Elapsed Time";
    desc = "SQL ordered by Elapsed Time";
    dash->desc = lappend(dash->desc, desc);
    desc = "List top 200 records";
    dash->desc = lappend(dash->desc, desc);
    desc = "Avg Elapse Time(us) represents the average elapsed time of sql between two snapshots";
    dash->desc = lappend(dash->desc, desc);
    GenReport::add_data(dash, &params->Contents);
    pfree_ext(query.data);
}

static void SQLClusterCPUTime(report_params* params)
{
    dashboard* dash = CreateDash();
    char* desc = NULL;
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "select t2.snap_unique_sql_id as \"Unique SQL Id\", "
        " t2.snap_node_name as \"Node Name\", t2.snap_user_name as \"User Name\","
        " (t2.snap_cpu_time - coalesce(t1.snap_cpu_time, 0)) as \"CPU Time(us)\", "
        " (t2.snap_n_calls - coalesce(t1.snap_n_calls, 0)) as \"Calls\", "
        " t2.snap_min_elapse_time as \"Min Elapse Time(us)\", t2.snap_max_elapse_time as \"Max Elapse Time(us)\", "
        " (t2.snap_total_elapse_time - coalesce(t1.snap_total_elapse_time, 0)) as \"Total Elapse Time(us)\", "
        " pg_catalog.round(\"Total Elapse Time(us)\"/greatest(\"Calls\", 1), 0) as \"Avg Elapse Time(us)\", "
        " (t2.snap_n_returned_rows - coalesce(t1.snap_n_returned_rows, 0)) as \"Returned Rows\", "
        " ((t2.snap_n_tuples_fetched - coalesce(t1.snap_n_tuples_fetched, 0)) + "
        " (t2.snap_n_tuples_returned - coalesce(t1.snap_n_tuples_returned, 0))) as \"Tuples Read\", "
        " ((t2.snap_n_tuples_inserted - coalesce(t1.snap_n_tuples_inserted, 0)) + "
        " (t2.snap_n_tuples_updated - coalesce(t1.snap_n_tuples_updated, 0)) + "
        " (t2.snap_n_tuples_deleted - coalesce(t1.snap_n_tuples_deleted, 0))) as \"Tuples Affected\", "
        " (t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) as \"Logical Read\", "
        " ((t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) - "
        " (t2.snap_n_blocks_hit - coalesce(t1.snap_n_blocks_hit, 0))) as \"Physical Read\", "
        " (t2.snap_data_io_time - coalesce(t1.snap_data_io_time, 0)) as \"Data IO Time(us)\", "
        " (t2.snap_sort_count - coalesce(t1.snap_sort_count, 0)) as \"Sort Count\", "
        " (t2.snap_sort_time - coalesce(t1.snap_sort_time, 0)) as \"Sort Time(us)\", "
        " (t2.snap_sort_mem_used - coalesce(t1.snap_sort_mem_used, 0)) as \"Sort Mem Used(KB)\", "
        " (t2.snap_sort_spill_count - coalesce(t1.snap_sort_spill_count, 0)) as \"Sort Spill Count\", "
        " (t2.snap_sort_spill_size - coalesce(t1.snap_sort_spill_size, 0)) as \"Sort Spill Size(KB)\", "
        " (t2.snap_hash_count - coalesce(t1.snap_hash_count, 0)) as \"Hash Count\", "
        " (t2.snap_hash_time - coalesce(t1.snap_hash_time, 0)) as \"Hash Time(us)\", "
        " (t2.snap_hash_mem_used - coalesce(t1.snap_hash_mem_used, 0)) as \"Hash Mem Used(KB)\", "
        " (t2.snap_hash_spill_count - coalesce(t1.snap_hash_spill_count, 0)) as \"Hash Spill Count\", "
        " (t2.snap_hash_spill_size - coalesce(t1.snap_hash_spill_size, 0)) as \"Hash Spill Size(KB)\", "
        " LEFT(t2.snap_query, 25) as \"SQL Text\" "
        "  from (select * from snapshot.snap_summary_statement where snapshot_id = %ld) t1"
        " right join (select * from snapshot.snap_summary_statement where snapshot_id = %ld) t2"
        " on t1.snap_unique_sql_id = t2.snap_unique_sql_id and t1.snap_user_id = t2.snap_user_id "
        " and t1.snap_node_name = t2.snap_node_name order by \"CPU Time(us)\" desc limit 200;",
        params->begin_snap_id,
        params->end_snap_id);
    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "SQL Statistics";
    dash->tableTitle = "SQL ordered by CPU Time";
    desc = "SQL ordered by CPU Time";
    dash->desc = lappend(dash->desc, desc);
    desc = "List top 200 records";
    dash->desc = lappend(dash->desc, desc);
    desc = "Avg Elapse Time(us) represents the average elapsed time of sql between two snapshots";
    dash->desc = lappend(dash->desc, desc);
    GenReport::add_data(dash, &params->Contents);
    pfree_ext(query.data);
}

static void SQLClusterRowsReturned(report_params* params)
{
    dashboard* dash = CreateDash();
    char* desc = NULL;
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "select t2.snap_unique_sql_id as \"Unique SQL Id\", "
        " t2.snap_node_name as \"Node Name\", t2.snap_user_name as \"User Name\","
        " (t2.snap_n_returned_rows - coalesce(t1.snap_n_returned_rows, 0)) as \"Returned Rows\", "
        " (t2.snap_n_calls - coalesce(t1.snap_n_calls, 0)) as \"Calls\", "
        " t2.snap_min_elapse_time as \"Min Elapse Time(us)\", t2.snap_max_elapse_time as \"Max Elapse Time(us)\","
        " (t2.snap_total_elapse_time - coalesce(t1.snap_total_elapse_time, 0)) as \"Total Elapse Time(us)\", "
        " pg_catalog.round(\"Total Elapse Time(us)\"/greatest(\"Calls\", 1), 0) as \"Avg Elapse Time(us)\","
        " ((t2.snap_n_tuples_fetched - coalesce(t1.snap_n_tuples_fetched, 0)) + "
        " (t2.snap_n_tuples_returned - coalesce(t1.snap_n_tuples_returned, 0))) as \"Tuples Read\", "
        " ((t2.snap_n_tuples_inserted - coalesce(t1.snap_n_tuples_inserted, 0)) + "
        " (t2.snap_n_tuples_updated - coalesce(t1.snap_n_tuples_updated, 0)) + "
        " (t2.snap_n_tuples_deleted - coalesce(t1.snap_n_tuples_deleted, 0))) as \"Tuples Affected\", "
        " (t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) as \"Logical Read\", "
        " ((t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) - "
        " (t2.snap_n_blocks_hit - coalesce(t1.snap_n_blocks_hit, 0))) as \"Physical Read\", "
        " (t2.snap_cpu_time - coalesce(t1.snap_cpu_time, 0)) as \"CPU Time(us)\", "
        " (t2.snap_data_io_time - coalesce(t1.snap_data_io_time, 0)) as \"Data IO Time(us)\", "
        " (t2.snap_sort_count - coalesce(t1.snap_sort_count, 0)) as \"Sort Count\", "
        " (t2.snap_sort_time - coalesce(t1.snap_sort_time, 0)) as \"Sort Time(us)\", "
        " (t2.snap_sort_mem_used - coalesce(t1.snap_sort_mem_used, 0)) as \"Sort Mem Used(KB)\", "
        " (t2.snap_sort_spill_count - coalesce(t1.snap_sort_spill_count, 0)) as \"Sort Spill Count\", "
        " (t2.snap_sort_spill_size - coalesce(t1.snap_sort_spill_size, 0)) as \"Sort Spill Size(KB)\", "
        " (t2.snap_hash_count - coalesce(t1.snap_hash_count, 0)) as \"Hash Count\", "
        " (t2.snap_hash_time - coalesce(t1.snap_hash_time, 0)) as \"Hash Time(us)\", "
        " (t2.snap_hash_mem_used - coalesce(t1.snap_hash_mem_used, 0)) as \"Hash Mem Used(KB)\", "
        " (t2.snap_hash_spill_count - coalesce(t1.snap_hash_spill_count, 0)) as \"Hash Spill Count\", "
        " (t2.snap_hash_spill_size - coalesce(t1.snap_hash_spill_size, 0)) as \"Hash Spill Size(KB)\", "
        " LEFT(t2.snap_query, 25) as \"SQL Text\" "
        "  from (select * from snapshot.snap_summary_statement where snapshot_id = %ld) t1"
        " right join (select * from snapshot.snap_summary_statement where snapshot_id = %ld) t2"
        " on t1.snap_unique_sql_id = t2.snap_unique_sql_id and t1.snap_user_id = t2.snap_user_id "
        " and t1.snap_node_name = t2.snap_node_name order by \"Returned Rows\"desc limit 200;",
        params->begin_snap_id,
        params->end_snap_id);
    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "SQL Statistics";
    dash->tableTitle = "SQL ordered by Rows Returned";
    desc = "SQL ordered by Rows Returned";
    dash->desc = lappend(dash->desc, desc);
    desc = "List top 200 records";
    dash->desc = lappend(dash->desc, desc);
    desc = "Avg Elapse Time(us) represents the average elapsed time of sql between two snapshots";
    dash->desc = lappend(dash->desc, desc);
    GenReport::add_data(dash, &params->Contents);
    pfree_ext(query.data);
}

static void SQLClusterRowRead(report_params* params)
{
    dashboard* dash = CreateDash();
    char* desc = NULL;
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "select t2.snap_unique_sql_id as \"Unique SQL Id\", "
        " t2.snap_node_name as \"Node Name\", t2.snap_user_name as \"User Name\","
        " ((t2.snap_n_tuples_fetched - coalesce(t1.snap_n_tuples_fetched, 0)) + "
        " (t2.snap_n_tuples_returned - coalesce(t1.snap_n_tuples_returned, 0))) as \"Tuples Read\", "
        " (t2.snap_n_calls - coalesce(t1.snap_n_calls, 0)) as \"Calls\", "
        " t2.snap_min_elapse_time as \"Min Elapse Time(us)\", t2. snap_max_elapse_time as \"Max Elapse Time(us)\", "
        " (t2.snap_total_elapse_time - coalesce(t1.snap_total_elapse_time, 0)) as \"Total Elapse Time(us)\", "
        " pg_catalog.round(\"Total Elapse Time(us)\"/greatest(\"Calls\", 1), 0) as \"Avg Elapse Time(us)\", "
        " (t2.snap_n_returned_rows - coalesce(t1.snap_n_returned_rows, 0)) as \"Returned Rows\", "
        " ((t2.snap_n_tuples_inserted - coalesce(t1.snap_n_tuples_inserted, 0)) + "
        " (t2.snap_n_tuples_updated - coalesce(t1.snap_n_tuples_updated, 0)) + "
        " (t2.snap_n_tuples_deleted - coalesce(t1.snap_n_tuples_deleted, 0))) as \"Tuples Affected\", "
        " (t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) as \"Logical Read\", "
        " ((t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) - "
        " (t2.snap_n_blocks_hit - coalesce(t1.snap_n_blocks_hit, 0))) as \"Physical Read\", "
        " (t2.snap_cpu_time - coalesce(t1.snap_cpu_time, 0)) as \"CPU Time(us)\", "
        " (t2.snap_data_io_time - coalesce(t1.snap_data_io_time, 0)) as \"Data IO Time(us)\", "
        " (t2.snap_sort_count - coalesce(t1.snap_sort_count, 0)) as \"Sort Count\", "
        " (t2.snap_sort_time - coalesce(t1.snap_sort_time, 0)) as \"Sort Time(us)\", "
        " (t2.snap_sort_mem_used - coalesce(t1.snap_sort_mem_used, 0)) as \"Sort Mem Used(KB)\", "
        " (t2.snap_sort_spill_count - coalesce(t1.snap_sort_spill_count, 0)) as \"Sort Spill Count\", "
        " (t2.snap_sort_spill_size - coalesce(t1.snap_sort_spill_size, 0)) as \"Sort Spill Size(KB)\", "
        " (t2.snap_hash_count - coalesce(t1.snap_hash_count, 0)) as \"Hash Count\", "
        " (t2.snap_hash_time - coalesce(t1.snap_hash_time, 0)) as \"Hash Time(us)\", "
        " (t2.snap_hash_mem_used - coalesce(t1.snap_hash_mem_used, 0)) as \"Hash Mem Used(KB)\", "
        " (t2.snap_hash_spill_count - coalesce(t1.snap_hash_spill_count, 0)) as \"Hash Spill Count\", "
        " (t2.snap_hash_spill_size - coalesce(t1.snap_hash_spill_size, 0)) as \"Hash Spill Size(KB)\", "
        " LEFT(t2.snap_query, 25) as \"SQL Text\" "
        "  from (select * from snapshot.snap_summary_statement where snapshot_id = %ld) t1"
        " right join (select * from snapshot.snap_summary_statement where snapshot_id = %ld) t2"
        " on t1.snap_unique_sql_id = t2.snap_unique_sql_id and t1.snap_user_id = t2.snap_user_id "
        " and t1.snap_node_name = t2.snap_node_name order by \"Tuples Read\" desc limit 200;",
        params->begin_snap_id,
        params->end_snap_id);
    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "SQL Statistics";
    dash->tableTitle = "SQL ordered by Row Read";
    desc = "SQL ordered by Row Read";
    dash->desc = lappend(dash->desc, desc);
    desc = "List top 200 records";
    dash->desc = lappend(dash->desc, desc);
    desc = "Avg Elapse Time(us) represents the average elapsed time of sql between two snapshots";
    dash->desc = lappend(dash->desc, desc);
    GenReport::add_data(dash, &params->Contents);
    pfree_ext(query.data);
}

static void SQLClusterExecutions(report_params* params)
{
    dashboard* dash = CreateDash();
    char* desc = NULL;
    StringInfoData query;
    initStringInfo(&query);

    appendStringInfo(&query,
        "select t2.snap_unique_sql_id as \"Unique SQL Id\", "
        " t2.snap_node_name as \"Node Name\", t2.snap_user_name as \"User Name\","
        " (t2.snap_n_calls - coalesce(t1.snap_n_calls, 0)) as \"Calls\", "
        " t2.snap_min_elapse_time as \"Min Elapse Time(us)\", t2.snap_max_elapse_time as \"Max Elapse Time(us)\","
        " (t2.snap_total_elapse_time - coalesce(t1.snap_total_elapse_time, 0)) as \"Total Elapse Time(us)\", "
        " pg_catalog.round(\"Total Elapse Time(us)\"/greatest(\"Calls\", 1), 0) as \"Avg Elapse Time\","
        " (t2.snap_n_returned_rows - coalesce(t1.snap_n_returned_rows, 0)) as \"Returned Rows\", "
        " ((t2.snap_n_tuples_fetched - coalesce(t1.snap_n_tuples_fetched, 0)) + "
        " (t2.snap_n_tuples_returned - coalesce(t1.snap_n_tuples_returned, 0))) as \"Tuples Read\", "
        " ((t2.snap_n_tuples_inserted - coalesce(t1.snap_n_tuples_inserted, 0)) + "
        " (t2.snap_n_tuples_updated - coalesce(t1.snap_n_tuples_updated, 0)) + "
        " (t2.snap_n_tuples_deleted - coalesce(t1.snap_n_tuples_deleted, 0))) as \"Tuples Affected\", "
        " (t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) as \"Logical Read\", "
        " ((t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) - "
        " (t2.snap_n_blocks_hit - coalesce(t1.snap_n_blocks_hit, 0))) as \"Physical Read\", "
        " (t2.snap_cpu_time - coalesce(t1.snap_cpu_time, 0)) as \"CPU Time(us)\", "
        " (t2.snap_data_io_time - coalesce(t1.snap_data_io_time, 0)) as \"Data IO Time(us)\", "
        " (t2.snap_sort_count - coalesce(t1.snap_sort_count, 0)) as \"Sort Count\", "
        " (t2.snap_sort_time - coalesce(t1.snap_sort_time, 0)) as \"Sort Time(us)\", "
        " (t2.snap_sort_mem_used - coalesce(t1.snap_sort_mem_used, 0)) as \"Sort Mem Used(KB)\", "
        " (t2.snap_sort_spill_count - coalesce(t1.snap_sort_spill_count, 0)) as \"Sort Spill Count\", "
        " (t2.snap_sort_spill_size - coalesce(t1.snap_sort_spill_size, 0)) as \"Sort Spill Size(KB)\", "
        " (t2.snap_hash_count - coalesce(t1.snap_hash_count, 0)) as \"Hash Count\", "
        " (t2.snap_hash_time - coalesce(t1.snap_hash_time, 0)) as \"Hash Time(us)\", "
        " (t2.snap_hash_mem_used - coalesce(t1.snap_hash_mem_used, 0)) as \"Hash Mem Used(KB)\", "
        " (t2.snap_hash_spill_count - coalesce(t1.snap_hash_spill_count, 0)) as \"Hash Spill Count\", "
        " (t2.snap_hash_spill_size - coalesce(t1.snap_hash_spill_size, 0)) as \"Hash Spill Size(KB)\", "
        " LEFT(t2.snap_query, 25) as \"SQL Text\" "
        "  from (select * from snapshot.snap_summary_statement where snapshot_id = %ld) t1"
        " right join (select * from snapshot.snap_summary_statement where snapshot_id = %ld) t2"
        " on t1.snap_unique_sql_id = t2.snap_unique_sql_id and t1.snap_user_id = t2.snap_user_id "
        " and t1.snap_node_name = t2.snap_node_name order by \"Calls\" desc limit 200;",
        params->begin_snap_id,
        params->end_snap_id);
    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "SQL Statistics";
    dash->tableTitle = "SQL ordered by Executions";
    desc = "SQL ordered by Executions";
    dash->desc = lappend(dash->desc, desc);
    desc = "List top 200 records";
    dash->desc = lappend(dash->desc, desc);
    desc = "Avg Elapse Time(us) represents the average elapsed time of sql between two snapshots";
    dash->desc = lappend(dash->desc, desc);
    GenReport::add_data(dash, &params->Contents);

    pfree_ext(query.data);
}

static void SQLClusterPhysicalReads(report_params* params)
{
    dashboard* dash = CreateDash();
    char* desc = NULL;
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "select t2.snap_unique_sql_id as \"Unique SQL Id\", "
        " t2.snap_node_name as \"Node Name\", t2.snap_user_name as \"User Name\","
        " ((t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) - "
        " (t2.snap_n_blocks_hit - coalesce(t1.snap_n_blocks_hit, 0))) as \"Physical Read\", "
        " (t2.snap_n_calls - coalesce(t1.snap_n_calls, 0)) as \"Calls\", "
        " t2.snap_min_elapse_time as \"Min Elapse Time(us)\", t2. snap_max_elapse_time as \"Max Elapse Time(us)\", "
        " (t2.snap_total_elapse_time - coalesce(t1.snap_total_elapse_time, 0)) as \"Total Elapse Time(us)\", "
        " pg_catalog.round(\"Total Elapse Time(us)\"/greatest(\"Calls\", 1), 0) as \"Avg Elapse Time(us)\", "
        " (t2.snap_n_returned_rows - coalesce(t1.snap_n_returned_rows, 0)) as \"Returned Rows\", "
        " ((t2.snap_n_tuples_fetched - coalesce(t1.snap_n_tuples_fetched, 0)) + "
        " (t2.snap_n_tuples_returned - coalesce(t1.snap_n_tuples_returned, 0))) as \"Tuples Read\", "
        " ((t2.snap_n_tuples_inserted - coalesce(t1.snap_n_tuples_inserted, 0)) + "
        " (t2.snap_n_tuples_updated - coalesce(t1.snap_n_tuples_updated, 0)) + "
        " (t2.snap_n_tuples_deleted - coalesce(t1.snap_n_tuples_deleted, 0))) as \"Tuples Affected\", "
        " (t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) as \"Logical Read\", "
        " (t2.snap_cpu_time - coalesce(t1.snap_cpu_time, 0)) as \"CPU Time(us)\", "
        " (t2.snap_data_io_time - coalesce(t1.snap_data_io_time, 0)) as \"Data IO Time(us)\", "
        " (t2.snap_sort_count - coalesce(t1.snap_sort_count, 0)) as \"Sort Count\", "
        " (t2.snap_sort_time - coalesce(t1.snap_sort_time, 0)) as \"Sort Time(us)\", "
        " (t2.snap_sort_mem_used - coalesce(t1.snap_sort_mem_used, 0)) as \"Sort Mem Used(KB)\", "
        " (t2.snap_sort_spill_count - coalesce(t1.snap_sort_spill_count, 0)) as \"Sort Spill Count\", "
        " (t2.snap_sort_spill_size - coalesce(t1.snap_sort_spill_size, 0)) as \"Sort Spill Size(KB)\", "
        " (t2.snap_hash_count - coalesce(t1.snap_hash_count, 0)) as \"Hash Count\", "
        " (t2.snap_hash_time - coalesce(t1.snap_hash_time, 0)) as \"Hash Time(us)\", "
        " (t2.snap_hash_mem_used - coalesce(t1.snap_hash_mem_used, 0)) as \"Hash Mem Used(KB)\", "
        " (t2.snap_hash_spill_count - coalesce(t1.snap_hash_spill_count, 0)) as \"Hash Spill Count\", "
        " (t2.snap_hash_spill_size - coalesce(t1.snap_hash_spill_size, 0)) as \"Hash Spill Size(KB)\", "
        " LEFT(t2.snap_query, 25) as \"SQL Text\" "
        "  from (select * from snapshot.snap_summary_statement where snapshot_id = %ld) t1"
        " right join (select * from snapshot.snap_summary_statement where snapshot_id = %ld) t2"
        " on t1.snap_unique_sql_id = t2.snap_unique_sql_id and t1.snap_user_id = t2.snap_user_id "
        " and t1.snap_node_name = t2.snap_node_name order by \"Physical Read\" desc limit 200;",
        params->begin_snap_id,
        params->end_snap_id);
    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "SQL Statistics";
    dash->tableTitle = "SQL ordered by Physical Reads";
    desc = "SQL ordered by Physical Reads";
    dash->desc = lappend(dash->desc, desc);
    desc = "List top 200 records";
    dash->desc = lappend(dash->desc, desc);
    desc = "Avg Elapse Time(us) represents the average elapsed time of sql between two snapshots";
    dash->desc = lappend(dash->desc, desc);
    GenReport::add_data(dash, &params->Contents);

    pfree_ext(query.data);
}

static void SQLClusterLogicalReads(report_params* params)
{
    dashboard* dash = CreateDash();
    char* desc = NULL;
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "select t2.snap_unique_sql_id as \"Unique SQL Id\", "
        " t2.snap_node_name as \"Node Name\", t2.snap_user_name as \"User Name\", "
        "(t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) as \"Logical Read\", "
        "(t2.snap_n_calls - coalesce(t1.snap_n_calls, 0)) as \"Calls\", "
        " t2.snap_min_elapse_time as \"Min Elapse Time(us)\", t2.snap_max_elapse_time as \"Max Elapse Time(us)\","
        " (t2.snap_total_elapse_time - coalesce(t1.snap_total_elapse_time, 0)) as \"Total Elapse Time(us)\", "
        " pg_catalog.round(\"Total Elapse Time(us)\"/greatest(\"Calls\", 1), 0) as \"Avg Elapse Time(us)\","
        " (t2.snap_n_returned_rows - coalesce(t1.snap_n_returned_rows, 0)) as \"Returned Rows\", "
        " ((t2.snap_n_tuples_fetched - coalesce(t1.snap_n_tuples_fetched, 0)) + "
        " (t2.snap_n_tuples_returned - coalesce(t1.snap_n_tuples_returned, 0))) as \"Tuples Read\", "
        " ((t2.snap_n_tuples_inserted - coalesce(t1.snap_n_tuples_inserted, 0)) + "
        " (t2.snap_n_tuples_updated - coalesce(t1.snap_n_tuples_updated, 0)) + "
        " (t2.snap_n_tuples_deleted - coalesce(t1.snap_n_tuples_deleted, 0))) as \"Tuples Affected\", "
        " ((t2.snap_n_blocks_fetched - coalesce(t1.snap_n_blocks_fetched, 0)) - "
        " (t2.snap_n_blocks_hit - coalesce(t1.snap_n_blocks_hit, 0))) as \"Physical Read\", "
        " (t2.snap_cpu_time - coalesce(t1.snap_cpu_time, 0)) as \"CPU Time(us)\", "
        " (t2.snap_data_io_time - coalesce(t1.snap_data_io_time, 0)) as \"Data IO Time(us)\", "
        " (t2.snap_sort_count - coalesce(t1.snap_sort_count, 0)) as \"Sort Count\", "
        " (t2.snap_sort_time - coalesce(t1.snap_sort_time, 0)) as \"Sort Time(us)\", "
        " (t2.snap_sort_mem_used - coalesce(t1.snap_sort_mem_used, 0)) as \"Sort Mem Used(KB)\", "
        " (t2.snap_sort_spill_count - coalesce(t1.snap_sort_spill_count, 0)) as \"Sort Spill Count\", "
        " (t2.snap_sort_spill_size - coalesce(t1.snap_sort_spill_size, 0)) as \"Sort Spill Size(KB)\", "
        " (t2.snap_hash_count - coalesce(t1.snap_hash_count, 0)) as \"Hash Count\", "
        " (t2.snap_hash_time - coalesce(t1.snap_hash_time, 0)) as \"Hash Time(us)\", "
        " (t2.snap_hash_mem_used - coalesce(t1.snap_hash_mem_used, 0)) as \"Hash Mem Used(KB)\", "
        " (t2.snap_hash_spill_count - coalesce(t1.snap_hash_spill_count, 0)) as \"Hash Spill Count\", "
        " (t2.snap_hash_spill_size - coalesce(t1.snap_hash_spill_size, 0)) as \"Hash Spill Size(KB)\", "
        " LEFT(t2.snap_query, 25) as \"SQL Text\" "
        "  from (select * from snapshot.snap_summary_statement where snapshot_id = %ld) t1"
        " right join (select * from snapshot.snap_summary_statement where snapshot_id = %ld) t2"
        " on t1.snap_unique_sql_id = t2.snap_unique_sql_id and t1.snap_user_id = t2.snap_user_id "
        " and t1.snap_node_name = t2.snap_node_name order by \"Logical Read\" desc limit 200;",
        params->begin_snap_id,
        params->end_snap_id);
    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "SQL Statistics";
    dash->tableTitle = "SQL ordered by Logical Reads";
    desc = "SQL ordered by Logical Reads";
    dash->desc = lappend(dash->desc, desc);
    desc = "List top 200 records";
    dash->desc = lappend(dash->desc, desc);
    desc = "Avg Elapse Time(us) represents the average elapsed time of sql between two snapshots";
    dash->desc = lappend(dash->desc, desc);
    GenReport::add_data(dash, &params->Contents);

    pfree_ext(query.data);
}

void GenReport::GetClusterSQLStatisticsData(report_params* params)
{
    /* SQL ordered by Elapsed Time */
    SQLclusterElapsedTime(params);

    /* SQL ordered by CPU Time */
    SQLClusterCPUTime(params);

    /* SQL ordered by Rows Returned */
    SQLClusterRowsReturned(params);

    /* SQL ordered by Row Read */
    SQLClusterRowRead(params);
    /* SQL ordered by Executions */
    SQLClusterExecutions(params);

    /* SQL ordered by Physical Reads */
    SQLClusterPhysicalReads(params);

    /* SQL ordered by Logical Reads */
    SQLClusterLogicalReads(params);
}

static void AllTablesStat(report_params* params)
{
    dashboard* dash = CreateDash();
    char* desc = NULL;
    char* note = NULL;
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT snap_2.db_name as \"DB Name\", snap_2.snap_schemaname AS \"Schema\","
        " snap_2.snap_relname AS \"Relname\","
        " (snap_2.snap_seq_scan - coalesce(snap_1.snap_seq_scan, 0)) AS \"Seq Scan\","
        " (snap_2.snap_seq_tup_read - coalesce(snap_1.snap_seq_tup_read, 0)) AS \"Seq Tup Read\","
        " (snap_2.snap_idx_scan - coalesce(snap_1.snap_idx_scan, 0)) AS \"Index Scan\","
        " (snap_2.snap_idx_tup_fetch - coalesce(snap_1.snap_idx_tup_fetch, 0)) AS \"Index Tup Fetch\","
        " (snap_2.snap_n_tup_ins - coalesce(snap_1.snap_n_tup_ins, 0)) AS \"Tuple Insert\","
        " (snap_2.snap_n_tup_upd - coalesce(snap_1.snap_n_tup_upd, 0)) AS \"Tuple Update\","
        " (snap_2.snap_n_tup_del - coalesce(snap_1.snap_n_tup_del, 0)) AS \"Tuple Delete\","
        " (snap_2.snap_n_tup_hot_upd - coalesce(snap_1.snap_n_tup_hot_upd, 0)) AS \"Tuple Hot Update\","
        " snap_2.snap_n_live_tup AS \"Live Tuple\", snap_2.snap_n_dead_tup AS \"Dead Tuple\","
        " pg_catalog.to_char(snap_2.snap_last_vacuum, 'YYYY-MM-DD HH24:MI:SS') AS \"Last Vacuum\","
        " pg_catalog.to_char(snap_2.snap_last_autovacuum, 'YYYY-MM-DD HH24:MI:SS') AS \"Last Autovacuum\","
        " pg_catalog.to_char(snap_2.snap_last_analyze, 'YYYY-MM-DD HH24:MI:SS') AS \"Last Analyze\","
        " pg_catalog.to_char(snap_2.snap_last_autoanalyze, 'YYYY-MM-DD HH24:MI:SS') AS \"Last Autoanalyze\","
        " (snap_2.snap_vacuum_count - coalesce(snap_1.snap_vacuum_count, 0)) AS \"Vacuum Count\","
        " (snap_2.snap_autovacuum_count - coalesce(snap_1.snap_autovacuum_count, 0)) AS \"Autovacuum Count\","
        " (snap_2.snap_analyze_count - coalesce(snap_1.snap_analyze_count, 0)) AS \"Analyze Count\","
        " (snap_2.snap_autoanalyze_count - coalesce(snap_1.snap_autoanalyze_count, 0)) AS \"Autoanalyze Count\" FROM"
        " (SELECT * FROM snapshot.snap_global_stat_all_tables WHERE snapshot_id = %ld and snap_node_name = '%s' and"
        " snap_schemaname NOT IN (%s) AND snap_schemaname !~ '^pg_toast') snap_2"
        " LEFT JOIN (SELECT * FROM snapshot.snap_global_stat_all_tables WHERE snapshot_id = %ld and "
        " snap_node_name = '%s' and snap_schemaname NOT IN (%s) AND"
        " snap_schemaname !~ '^pg_toast') snap_1 ON snap_2.snap_relid = snap_1.snap_relid AND snap_2.snap_schemaname = "
        "snap_1.snap_schemaname AND snap_2.snap_relname = snap_1.snap_relname AND snap_2.db_name = snap_1.db_name "
        " order by snap_2.db_name, snap_2.snap_schemaname limit 200;",
        params->end_snap_id,
        params->report_node,
        cache_io_sys_schema,
        params->begin_snap_id,
        params->report_node,
        cache_io_sys_schema);

    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "Object stats";
    dash->tableTitle = "User Tables stats";
    desc = "The information of statistics for user tables stats";
    note = "The Live Tuple, Dead Tuple are instantaneous values";
    dash->desc = lappend(dash->desc, desc);
    dash->desc = lappend(dash->desc, note);
    GenReport::add_data(dash, &params->Contents);
    pfree_ext(query.data);
}
static void GetObjectNodeStat(report_params* params)
{
    dashboard* dash = CreateDash();
    char* desc = NULL;
    StringInfoData query;
    initStringInfo(&query);

    /* all tables stat */
    AllTablesStat(params);

    /* table index stat */
    appendStringInfo(&query,
        "SELECT snap_2.db_name as \"DB Name\", snap_2.snap_schemaname AS \"Schema\","
        " snap_2.snap_relname AS \"Relname\", snap_2.snap_indexrelname AS \"Index Relname\","
        " (snap_2.snap_idx_scan - coalesce(snap_1.snap_idx_scan, 0)) AS \"Index Scan\","
        " (snap_2.snap_idx_tup_read - coalesce(snap_1.snap_idx_tup_read, 0)) AS \"Index Tuple Read\","
        " (snap_2.snap_idx_tup_fetch - coalesce(snap_1.snap_idx_tup_fetch, 0)) AS \"Index Tuple Fetch\" FROM"
        " (SELECT * FROM snapshot.snap_global_stat_all_indexes "
        " WHERE snapshot_id = %ld and snap_node_name = '%s' and snap_schemaname NOT IN"
        " (%s) AND snap_schemaname !~ '^pg_toast') snap_2"
        " LEFT JOIN (SELECT * FROM snapshot.snap_global_stat_all_indexes WHERE snapshot_id = %ld "
        "and snap_node_name = '%s' and snap_schemaname NOT IN (%s)"
        " AND snap_schemaname !~ '^pg_toast') snap_1 ON snap_2.snap_relid = snap_1.snap_relid AND "
        "snap_2.snap_indexrelid = snap_1.snap_indexrelid AND snap_2.snap_schemaname = snap_1.snap_schemaname "
        "AND snap_2.snap_relname = snap_1.snap_relname AND snap_2.snap_indexrelname = snap_1.snap_indexrelname "
        "AND snap_2.db_name = snap_1.db_name order by snap_2.db_name, snap_2.snap_schemaname limit 200;",
        params->end_snap_id,
        params->report_node,
        cache_io_sys_schema,
        params->begin_snap_id,
        params->report_node,
        cache_io_sys_schema);
    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "Object stats";
    dash->tableTitle = "User index stats";
    desc = "The detail information of user index stats";
    dash->desc = lappend(dash->desc, desc);
    GenReport::add_data(dash, &params->Contents);

    /* bad block stat */
    dashboard* dash1 = CreateDash();
    resetStringInfo(&query);
    appendStringInfo(&query,
        "SELECT snap_2.snap_databaseid AS \"DB Id\", snap_2.snap_tablespaceid AS \"Tablespace Id\","
        " snap_2.snap_relfilenode AS \"Relfilenode\", snap_2.snap_forknum AS \"Fork Number\","
        " (snap_2.snap_error_count - coalesce(snap_1.snap_error_count, 0)) AS \"Error Count\","
        " snap_2.snap_first_time AS \"First Time\", snap_2.snap_last_time AS \"Last Time\" FROM"
        " (SELECT * FROM snapshot.snap_global_stat_bad_block WHERE snapshot_id = %ld and snap_node_name = '%s') snap_2"
        " LEFT JOIN (SELECT * FROM snapshot.snap_global_stat_bad_block WHERE snapshot_id = %ld "
        " and snap_node_name = '%s') snap_1 ON snap_2.snap_databaseid = snap_1.snap_databaseid AND "
        " snap_2.snap_tablespaceid = snap_1.snap_tablespaceid AND"
        " snap_2.snap_relfilenode = snap_1.snap_relfilenode limit 200;",
        params->end_snap_id,
        params->report_node,
        params->begin_snap_id,
        params->report_node);
    GenReport::get_query_data(query.data, true, &dash1->table, &dash1->type);
    dash1->dashTitle = "Object stats";
    dash1->tableTitle = "Bad lock stats";
    desc = "The detail information of bad lock stats";
    dash1->desc = lappend(dash1->desc, desc);
    GenReport::add_data(dash1, &params->Contents);

    pfree_ext(query.data);
}

static void GlobalTablesStat(report_params* params)
{
    dashboard* dash = CreateDash();
    char* desc = NULL;
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT snap_2.db_name as \"DB Name\", snap_2.snap_schemaname AS \"Schema\","
        "snap_2.snap_relname AS \"Relname\","
        " (snap_2.snap_seq_scan - coalesce(snap_1.snap_seq_scan, 0)) AS \"Seq Scan\","
        " (snap_2.snap_seq_tup_read - coalesce(snap_1.snap_seq_tup_read, 0)) AS  \"Seq Tup Read\","
        " (snap_2.snap_idx_scan - coalesce(snap_1.snap_idx_scan, 0)) AS \"Index Scan\","
        " (snap_2.snap_idx_tup_fetch - coalesce(snap_1.snap_idx_tup_fetch, 0)) AS \"Index Tup Fetch\","
        " (snap_2.snap_n_tup_ins - coalesce(snap_1.snap_n_tup_ins, 0)) AS  \"Tuple Insert\","
        " (snap_2.snap_n_tup_upd - coalesce(snap_1.snap_n_tup_upd, 0)) AS \"Tuple Update\","
        " (snap_2.snap_n_tup_del - coalesce(snap_1.snap_n_tup_del, 0)) AS \"Tuple Delete\","
        " (snap_2.snap_n_tup_hot_upd - coalesce(snap_1.snap_n_tup_hot_upd, 0)) AS \"Tuple Hot Update\","
        " snap_2.snap_n_live_tup AS \"Live Tuple\", snap_2.snap_n_dead_tup AS \"Dead Tuple\","
        " pg_catalog.to_char(snap_2.snap_last_vacuum, 'YYYY-MM-DD HH24:MI:SS') AS \"Last Vacuum\","
        " pg_catalog.to_char(snap_2.snap_last_autovacuum, 'YYYY-MM-DD HH24:MI:SS') AS \"Last Autovacuum\","
        " pg_catalog.to_char(snap_2.snap_last_analyze, 'YYYY-MM-DD HH24:MI:SS') AS \"Last Analyze\","
        " pg_catalog.to_char(snap_2.snap_last_autoanalyze, 'YYYY-MM-DD HH24:MI:SS') AS \"Last Autoanalyze\","
        " (snap_2.snap_vacuum_count - coalesce(snap_1.snap_vacuum_count, 0)) AS \"Vacuum Count\","
        " (snap_2.snap_autovacuum_count - coalesce(snap_1.snap_autovacuum_count, 0)) AS \"Autovacuum Count\","
        " (snap_2.snap_analyze_count - coalesce(snap_1.snap_analyze_count, 0)) AS \"Analyze Count\","
        " (snap_2.snap_autoanalyze_count - coalesce(snap_1.snap_autoanalyze_count, 0)) AS \"Autoanalyze Count\" FROM"
        " (SELECT C.snap_relid, T.* FROM (select * from snapshot.snap_summary_stat_all_tables where snap_schemaname NOT IN"
        " (%s) and snap_schemaname !~ '^pg_toast') as T"
        " LEFT JOIN snapshot.snap_class_vital_info AS C"
        " ON T.snap_relname = C.snap_relname AND T.snap_schemaname = C.snap_schemaname"
        " AND T.snapshot_id = C.snapshot_id AND T.db_name = C.db_name WHERE C.snapshot_id = %ld) AS snap_2 LEFT JOIN"
        " (SELECT C.snap_relid, T.* FROM (select * from snapshot.snap_summary_stat_all_tables where snap_schemaname NOT IN"
        " (%s) and snap_schemaname !~ '^pg_toast') as T"
        " LEFT JOIN snapshot.snap_class_vital_info AS C"
        " ON T.snap_relname = C.snap_relname AND T.snap_schemaname = C.snap_schemaname"
        " AND T.snapshot_id = C.snapshot_id AND T.db_name = C.db_name WHERE C.snapshot_id = %ld) AS snap_1"
        " ON snap_2.snap_relid = snap_1.snap_relid AND snap_2.db_name = snap_1.db_name AND "
        "snap_1.snap_schemaname = snap_2.snap_schemaname order by snap_2.db_name, snap_2.snap_schemaname limit 200;",
        cache_io_sys_schema,
        params->end_snap_id,
        cache_io_sys_schema,
        params->begin_snap_id);

    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "Object stats";
    dash->tableTitle = "User Tables stats";
    desc = "The cluster information of statistics for tables stats";
    dash->desc = lappend(dash->desc, desc);
    desc = "The Live Tuple, Dead Tuple are instantaneous values";
    dash->desc = lappend(dash->desc, desc);
    GenReport::add_data(dash, &params->Contents);
    pfree_ext(query.data);
}

static void GlobalTableIndex(report_params* params)
{
    dashboard* dash = CreateDash();
    char* desc = NULL;
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT snap_2.db_name as  \"DB Name\", snap_2.snap_schemaname AS \"Schema\", "
        "snap_2.snap_relname AS \"Relname\","
        " snap_2.snap_indexrelname AS \"Index Relname\", (snap_2.snap_idx_scan - coalesce(snap_1.snap_idx_scan, 0)) "
        "AS \"Index Scan\", "
        "(snap_2.snap_idx_tup_read - coalesce(snap_1.snap_idx_tup_read, 0)) AS \"Index Tuple Read\","
        " (snap_2.snap_idx_tup_fetch - coalesce(snap_1.snap_idx_tup_fetch, 0)) AS \"Index Tuple Fetch\" FROM"
        " (SELECT Ti.*, I.snap_relid as snap_indexrelid FROM"
        " (SELECT C.snap_relid, T.* FROM ( select * from snapshot.snap_summary_stat_all_indexes where snap_schemaname NOT IN"
        " (%s) AND snap_schemaname !~ '^pg_toast') as T"
        " LEFT JOIN snapshot.snap_class_vital_info AS C ON (T.snap_relname = C.snap_relname AND"
        " T.snap_schemaname = C.snap_schemaname AND T.db_name = C.db_name AND T.snapshot_id = C.snapshot_id) "
        "WHERE T.snapshot_id = %ld) AS Ti LEFT JOIN snapshot.snap_class_vital_info as I ON "
        "(Ti.snap_indexrelname = I.snap_relname AND"
        " Ti.snap_schemaname = I.snap_schemaname AND Ti.db_name = I.db_name AND Ti.snapshot_id = I.snapshot_id) "
        "WHERE Ti.snapshot_id = %ld) AS snap_2"
        " LEFT JOIN (SELECT Ti.*, I.snap_relid as snap_indexrelid FROM"
        " (SELECT C.snap_relid, T.* FROM (select * from snapshot.snap_summary_stat_all_indexes where snap_schemaname NOT IN"
        " (%s) AND snap_schemaname !~ '^pg_toast') as T"
        " LEFT JOIN snapshot.snap_class_vital_info AS C ON (T.snap_relname = C.snap_relname AND"
        " T.snap_schemaname = C.snap_schemaname AND T.db_name = C.db_name AND T.snapshot_id = C.snapshot_id) "
        "WHERE T.snapshot_id = %ld) AS Ti"
        " LEFT JOIN snapshot.snap_class_vital_info as I ON (Ti.snap_indexrelname = I.snap_relname AND"
        " Ti.snap_schemaname = I.snap_schemaname AND Ti.db_name = I.db_name AND Ti.snapshot_id = I.snapshot_id) "
        "WHERE i.snapshot_id = %ld) AS snap_1"
        " ON (snap_2.snap_relid = snap_1.snap_relid AND snap_2.snap_indexrelid = snap_1.snap_indexrelid AND "
        " snap_2.db_name = snap_1.db_name AND snap_2.snap_schemaname = snap_1.snap_schemaname) "
        " order by \"Index Tuple Read\" limit 200;",
        cache_io_sys_schema,
        params->end_snap_id,
        params->end_snap_id,
        cache_io_sys_schema,
        params->begin_snap_id,
        params->begin_snap_id);
    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "Object stats";
    dash->tableTitle = "User Index stats";
    desc = "The cluster information of index stats";
    dash->desc = lappend(dash->desc, desc);
    GenReport::add_data(dash, &params->Contents);

    pfree_ext(query.data);
}

static void GetObjectClusterStat(report_params* params)
{
    dashboard* dash = CreateDash();
    char* desc = NULL;
    StringInfoData query;
    initStringInfo(&query);

    /* global tables stat based on database */
    GlobalTablesStat(params);

    /* global table index stat based on database */
    GlobalTableIndex(params);

    /* bad block stat */
    appendStringInfo(&query,
        "SELECT snap_2.snap_databaseid AS \"DB Id\", snap_2.snap_tablespaceid AS \"Tablespace Id\","
        " snap_2.snap_relfilenode AS \"Relfilenode\", snap_2.snap_forknum AS \"Fork Number\","
        " (snap_2.snap_error_count - coalesce(snap_1.snap_error_count, 0)) AS \"Error Count\","
        " snap_2.snap_first_time AS  \"First Time\", snap_2.snap_last_time AS \"Last Time\" FROM"
        " (SELECT * FROM snapshot.snap_summary_stat_bad_block WHERE snapshot_id = %ld) snap_2"
        " LEFT JOIN (SELECT * FROM snapshot.snap_summary_stat_bad_block WHERE snapshot_id = %ld) snap_1"
        " ON snap_2.snap_databaseid = snap_1.snap_databaseid AND snap_2.snap_tablespaceid = snap_1.snap_tablespaceid"
        " AND snap_2.snap_relfilenode = snap_1.snap_relfilenode limit 200;",
        params->end_snap_id,
        params->begin_snap_id);
    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "Object stats";
    dash->tableTitle = "Bad lock stats";
    desc = "The detail information of cluster bad lock stats";
    dash->desc = lappend(dash->desc, desc);
    GenReport::add_data(dash, &params->Contents);

    pfree_ext(query.data);
}
static void GetObjectStatData(report_params* params)
{
    if (is_summary_report(params)) {
        return;
    }
    if (is_single_node_report(params)) {
        GetObjectNodeStat(params);
    } else if (is_cluster_report(params)) {
        GetObjectClusterStat(params);
    } else {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("no this type of report_scope")));
    }
}


static void ReplicationStat(report_params* params)
{
    dashboard* dash = CreateDash();
    char* desc = NULL;
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "SELECT snap_pid as \"Thread Id\", snap_usesysid as \"Usesys Id\", "
        "snap_usename as \"Usename\", "
        " snap_application_name as \"Application Name\", snap_client_addr as \"Client Addr\", snap_client_hostname "
        "as \"Client Hostname\","
        " snap_client_port as \"Client Port\", snap_backend_start as \"Backend Start\", snap_state as \"State\","
        " snap_sender_sent_location as \"Sender Sent Location\", snap_receiver_write_location "
        "as \"Receiver Write Location\","
        " snap_receiver_flush_location as \"Receiver Flush Location\", snap_receiver_replay_location "
        "as \"Receiver Replay Location\","
        " snap_sync_priority as \"Sync Priority\", snap_sync_state as \"Sync State\""
        " FROM snapshot.snap_global_replication_stat WHERE snapshot_id = %ld and snap_node_name = '%s' limit 200;",
        params->end_snap_id,
        params->report_node);
    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "Utility status";
    dash->tableTitle = "Replication stat";
    desc = "The detail information of replication stat on the given moment";
    dash->desc = lappend(dash->desc, desc);
    GenReport::add_data(dash, &params->Contents);

    pfree_ext(query.data);
}
static void GetUtilityStatus(report_params* params)
{
    /* supported report type: detail/all */
    /* supported report scope: cluster */
    if (is_summary_report(params) || is_cluster_report(params)) {
        return;
    }
    dashboard* dash = CreateDash();
    char* desc = NULL;
    StringInfoData query;
    initStringInfo(&query);

    /* replication slot */
    appendStringInfo(&query,
        "SELECT snap_slot_name as \"Slot Name\", snap_slot_type as \"Slot Type\","
        " snap_database as \"DB Name\", snap_active as \"Active\", snap_x_min as \"Xmin\","
        " snap_restart_lsn as \"Restart Lsn\", snap_dummy_standby as \"Dummy Standby\""
        " FROM snapshot.snap_global_replication_slots WHERE snapshot_id = %ld and snap_node_name = '%s' limit 200;",
        params->end_snap_id,
        params->report_node);
    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "Utility status";
    dash->tableTitle = "Replication slot";
    desc = "The detail information of replication slot";
    dash->desc = lappend(dash->desc, desc);
    GenReport::add_data(dash, &params->Contents);

    /* replication stat */
    ReplicationStat(params);

    pfree_ext(query.data);
}

static void GetConfigSettings(report_params* params)
{
    /* supported report type: detail/all */
    /* supported report scope: cluster */
    if (is_summary_report(params) || is_cluster_report(params)) {
        return;
    }
    dashboard* dash = CreateDash();
    char* desc = NULL;
    StringInfoData query;
    initStringInfo(&query);

    appendStringInfo(&query,
        "select snap_name AS \"Name\", snap_short_desc AS \"Abstract\", snap_vartype "
        "AS \"Type\", snap_setting AS \"Curent Value\", snap_min_val AS \"Min Value\", snap_max_val "
        "AS \"Max Value\", snap_category AS \"Category\", snap_enumvals AS \"Enum Values\", "
        "snap_boot_val AS \"Default Value\", snap_reset_val AS \"Reset Value\" "
        "FROM snapshot.snap_global_config_settings WHERE snapshot_id = %ld and snap_node_name = '%s';",
        params->end_snap_id,
        params->report_node);

    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "Configuration settings";
    dash->tableTitle = "Settings";
    desc = "The information of configuration settings for database";
    dash->desc = lappend(dash->desc, desc);
    GenReport::add_data(dash, &params->Contents);
    pfree_ext(query.data);
}

void GenReport::GetSqlStatisticsData(report_params* params)
{
    if (is_summary_report(params)) {
        return;
    }
    if (is_single_node_report(params)) {
        GenReport::GetNodeSQLStatisticsData(params);
    } else if (is_cluster_report(params)) {
        GenReport::GetClusterSQLStatisticsData(params);
    } else {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("no this type of report_scope")));
    }
}
static char* GetUniqueIDStr(List* Contents)
{
    StringInfoData strQueryId;
    initStringInfo(&strQueryId);
    /* Get unique id from dash that dashTitle is "SQL Statistics" in Contents */
    foreach_cell(dashCell, Contents)
    {
        List* dashList = (List*)lfirst(dashCell);
        if (strcmp(((dashboard*)linitial(dashList))->dashTitle, "SQL Statistics") != 0) {
            continue;
        }
        foreach_cell(cell, dashList)
        {
            /* The first column of the table is unique id in SQL Statistics tables */
            for (ListCell* row = lnext(list_head(((dashboard*)lfirst(cell))->table)); row != NULL; row = lnext(row)) {
                char* uniqueId = (char*)linitial((List*)lfirst(row));
                appendStringInfo(&strQueryId, "%s", uniqueId);
                if (row->next != NULL || cell->next != NULL) {
                    appendStringInfo(&strQueryId, ", ");
                }
            }
        }
    }
    return strQueryId.data;
}

static void GetClusterSqlDetailData(report_params* params)
{
    dashboard* dash = CreateDash();
    char* desc = NULL;
    StringInfoData query;
    initStringInfo(&query);

    appendStringInfo(&query,
        "select (t2.snap_unique_sql_id) as \"Unique SQL Id\", "
        "(t2.snap_node_name) as \"Node Name\", (t2.snap_user_name) as \"User Name\", "
        "(t2.snap_query) as \"SQL Text\" "
        "from snapshot.snap_summary_statement t2 where snapshot_id = %ld ",
        params->end_snap_id);
    char* uniqueIDStr = GetUniqueIDStr(params->Contents);
    if (strcmp(uniqueIDStr, "") != 0) {
        appendStringInfo(&query, "and snap_unique_sql_id in(%s)", uniqueIDStr);
        pfree(uniqueIDStr);
    }

    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "SQL Detail";
    dash->tableTitle = "SQL Text";
    desc = "unique sql text";
    dash->desc = lappend(dash->desc, desc);
    GenReport::add_data(dash, &params->Contents);
    pfree_ext(query.data);
}

static void GetNodeSqlDetailData(report_params* params)
{
    dashboard* dash = CreateDash();
    char* desc = NULL;
    StringInfoData query;
    initStringInfo(&query);

    appendStringInfo(&query,
        "select (t2.snap_unique_sql_id) as \"Unique SQL Id\", "
        "(t2.snap_user_name) as \"User Name\", "
        "(t2.snap_query) as \"SQL Text\" "
        " from snapshot.snap_summary_statement t2 where snapshot_id = %ld "
        "and snap_node_name = '%s' ",
        params->end_snap_id,
        params->report_node);
    char* uniqueIDStr = GetUniqueIDStr(params->Contents);
    if (strcmp(uniqueIDStr, "") != 0) {
        appendStringInfo(&query, "and snap_unique_sql_id in(%s)", uniqueIDStr);
        pfree(uniqueIDStr);
    }
    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "SQL Detail";
    dash->tableTitle = "SQL Text";
    desc = "unique sql text";
    dash->desc = lappend(dash->desc, desc);
    GenReport::add_data(dash, &params->Contents);

    pfree_ext(query.data);
}

static void GetSqlDetailData(report_params* params)
{
    if (is_summary_report(params)) {
        return;
    }
    if (is_single_node_report(params)) {
        GetNodeSqlDetailData(params);
    } else if (is_cluster_report(params)) {
        GetClusterSqlDetailData(params);
    } else {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("no this type of report_scope")));
    }
}

void GenReport::GetTimeModelData(report_params* params)
{
    /* supported report type: detail/all */
    /* supported report scope: cluster */
    if (is_summary_report(params) || is_cluster_report(params)) {
        return;
    }
    dashboard* dash = CreateDash();
    char* desc = NULL;
    StringInfoData query;
    initStringInfo(&query);

    /* Time Model order by value */
    appendStringInfo(&query,
        "select t2.snap_stat_name as \"Stat Name\", (t2.snap_value - coalesce(t1.snap_value, 0)) as \"Value(us)\" "
        " from (select * from snapshot.snap_global_instance_time where snapshot_id = %ld and snap_node_name = '%s') t1"
        " right join (select * from snapshot.snap_global_instance_time where snapshot_id = %ld and"
        " snap_node_name = '%s') t2 on t1.snap_stat_name = t2.snap_stat_name order by \"Value(us)\" desc limit 200;",
        params->begin_snap_id,
        params->report_node,
        params->end_snap_id,
        params->report_node);
    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    dash->dashTitle = "Time Model";
    dash->tableTitle = "Time model";
    desc = "time model order by value in node";
    dash->desc = lappend(dash->desc, desc);
    GenReport::add_data(dash, &params->Contents);

    pfree_ext(query.data);
}

static char* AdjustFieldOrder(const char* field[], char* sortkey, int length)
{
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query, ",data.%s", sortkey);
    for (int i = 0; i < length; i++) {
        if (strcmp(sortkey, field[i]) == 0) {
            continue;
        }
        appendStringInfo(&query, ",data.%s", field[i]);
    }
    return query.data;
}
static void NodeTableIOStrPart(StringInfo query, report_params* params, char* sortkey)
{
    char* field = AdjustFieldOrder(TableIOField, sortkey, lengthof(TableIOField));
    appendStringInfo(query,
        "select data.\"DB Name\",data.\"Schema Name\",data.\"Table Name\""
        "%s from ("
        "SELECT table_io.db_name as \"DB Name\", table_io.snap_schemaname as \"Schema Name\","
        " table_io.snap_relname as \"Table Name\", table_io.heap_blks_hit_ratio as \"%%Heap Blks Hit Ratio\","
        " table_io.heap_blks_read as \"Heap Blks Read\", table_io.heap_blks_hit as  \"Heap Blks Hit\","
        " idx_io.idx_blks_read as \"Idx Blks Read\", idx_io.idx_blks_hit as \"Idx Blks Hit\", "
        " table_io.toast_blks_read as \"Toast Blks Read\",  table_io.toast_blks_hit as \"Toast Blks Hit\","
        " table_io.tidx_blks_read as \"Tidx Blks Read\", table_io.tidx_blks_hit as  \"Tidx Blks Hit\" "
        " FROM (Select t2.db_name, t2.snap_schemaname , t2.snap_relname ,"
        " (case when ((t2.snap_heap_blks_read - coalesce(t1.snap_heap_blks_read, 0)) + "
        " (t2.snap_heap_blks_hit - coalesce(t1.snap_heap_blks_hit, 0))) = 0 then 0 else "
        " pg_catalog.round((t2.snap_heap_blks_hit - coalesce(t1.snap_heap_blks_hit, 0))/ "
        " ((t2.snap_heap_blks_read - coalesce(t1.snap_heap_blks_read, 0)) + "
        " (t2.snap_heap_blks_hit - coalesce(t1.snap_heap_blks_hit, 0))) * 100, 2) end ) as heap_blks_hit_ratio,"
        " (t2.snap_heap_blks_read - coalesce(t1.snap_heap_blks_read, 0)) as heap_blks_read,"
        " (t2.snap_heap_blks_hit - coalesce(t1.snap_heap_blks_hit, 0)) as heap_blks_hit,"
        " (t2.snap_idx_blks_read - coalesce(t1.snap_idx_blks_read, 0)) as idx_blks_read,"
        " (t2.snap_idx_blks_hit - coalesce(t1.snap_idx_blks_hit, 0)) as idx_blks_hit,"
        " (t2.snap_toast_blks_read - coalesce(t1.snap_toast_blks_read, 0)) as toast_blks_read,"
        " (t2.snap_toast_blks_hit - coalesce(t1.snap_toast_blks_hit, 0)) as toast_blks_hit,"
        " (t2.snap_tidx_blks_read - coalesce(t1.snap_tidx_blks_read, 0)) as tidx_blks_read,"
        " (t2.snap_tidx_blks_hit - coalesce(t1.snap_tidx_blks_hit, 0)) as tidx_blks_hit from"
        " (select * from snapshot.snap_global_statio_all_tables where snapshot_id = %ld and snap_node_name = '%s' and"
        " snap_schemaname NOT IN (%s) AND snap_schemaname !~ '^pg_toast') t1"
        " right join"
        " (select * from snapshot.snap_global_statio_all_tables where snapshot_id = %ld and snap_node_name = '%s' and"
        " snap_schemaname NOT IN (%s) AND snap_schemaname !~ '^pg_toast') t2"
        " on t1.snap_relid = t2.snap_relid and t2.db_name = t1.db_name and"
        " t2.snap_schemaname = t1.snap_schemaname )  as table_io left join",
        field,
        params->begin_snap_id,
        params->report_node,
        cache_io_sys_schema,
        params->end_snap_id,
        params->report_node,
        cache_io_sys_schema);
    pfree(field);
}
static void NodeTableIODataOrderbySortKey(report_params* params, char* sortkey,
                                          char* sortByDir, dashboard* dash)
{
    StringInfoData query;
    initStringInfo(&query);
    NodeTableIOStrPart(&query, params, sortkey);
    appendStringInfo(&query,
        " (Select t2.db_name , t2.snap_schemaname , t2.snap_relname,"
        " (t2.snap_idx_blks_read - coalesce(t1.snap_idx_blks_read, 0)) as idx_blks_read,"
        " (t2.snap_idx_blks_hit - coalesce(t1.snap_idx_blks_hit, 0)) as idx_blks_hit"
        " from"
        " (select * from snapshot.snap_global_statio_all_indexes"
        " where snapshot_id = %ld and snap_node_name = '%s' and snap_schemaname NOT IN"
        " (%s) AND snap_schemaname !~ '^pg_toast') t1"
        " right join"
        " (select * from snapshot.snap_global_statio_all_indexes"
        " where snapshot_id = %ld and snap_node_name = '%s' and snap_schemaname NOT IN"
        " (%s) AND snap_schemaname !~ '^pg_toast') t2"
        " on t1.snap_relid = t2.snap_relid and t2.snap_indexrelid = t1.snap_indexrelid and "
        "t2.db_name = t1.db_name and t2.snap_schemaname = t1.snap_schemaname) as idx_io"
        " on table_io.db_name = idx_io.db_name and table_io.snap_schemaname = idx_io.snap_schemaname"
        " and table_io.snap_relname = idx_io.snap_relname) data  where "
        "data.\"%%Heap Blks Hit Ratio\" <>0 or data.\"Heap Blks Read\" <>0 or data.\"Heap Blks Hit\" <>0"
        "or data.\"Idx Blks Read\" <>0 or data.\"Idx Blks Hit\" <>0 or data.\"Toast Blks Read\" <>0"
        "or data.\"Toast Blks Hit\" <>0 or data.\"Tidx Blks Read\" <>0 or data.\"Tidx Blks Hit\" <>0"
        "order by %s %s limit 200 ",
        params->begin_snap_id,
        params->report_node,
        cache_io_sys_schema,
        params->end_snap_id,
        params->report_node,
        cache_io_sys_schema,
        sortkey,
        sortByDir);
    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    GenReport::add_data(dash, &params->Contents);

    pfree(query.data);
}

/*
 * get Cache IO stat info of one node ordered by SortKey
 */
static void NodeIndexIODataOrderbySortKey(report_params* params, char* sortkey,
                                          char* sortByDir, dashboard* dash)
{
    StringInfoData query;
    initStringInfo(&query);
    char* field = AdjustFieldOrder(IndexIOField, sortkey, lengthof(IndexIOField));

    appendStringInfo(&query,
        "select data.\"DB Name\",data.\"Schema Name\",data.\"Table Name\",data.\"Index Name\"%s  from ("
        "Select t2.db_name as \"DB Name\", t2.snap_schemaname as \"Schema Name\", "
        " t2.snap_relname as \"Table Name\", t2.snap_indexrelname as \"Index Name\", "
        " (case when ((t2.snap_idx_blks_read - coalesce(t1.snap_idx_blks_read, 0)) + "
        " (t2.snap_idx_blks_hit - coalesce(t1.snap_idx_blks_hit, 0))) = 0 then 0 else "
        " pg_catalog.round((t2.snap_idx_blks_hit - coalesce(t1.snap_idx_blks_hit, 0))/"
        " ((t2.snap_idx_blks_hit - coalesce(t1.snap_idx_blks_hit, 0)) + "
        " (t2.snap_idx_blks_read - coalesce(t1.snap_idx_blks_read, 0))) * 100, 2) end) as \"%%Idx Blks Hit Ratio\", "
        " (t2.snap_idx_blks_read - coalesce(t1.snap_idx_blks_read, 0)) as \"Idx Blks Read\", "
        " (t2.snap_idx_blks_hit - coalesce(t1.snap_idx_blks_hit, 0)) as \"Idx Blks Hit\" "
        " from "
        "(select * from snapshot.snap_global_statio_all_indexes where snapshot_id = %ld and snap_node_name = '%s' and"
        " snap_schemaname NOT IN (%s) AND snap_schemaname !~ '^pg_toast') t1"
        "  right join"
        " (select * from snapshot.snap_global_statio_all_indexes where snapshot_id = %ld and snap_node_name = '%s' and"
        " snap_schemaname NOT IN (%s) AND snap_schemaname !~ '^pg_toast') t2"
        " on t1.snap_relid = t2.snap_relid and t2.snap_indexrelid = t1.snap_indexrelid and "
        " t2.db_name = t1.db_name and t2.snap_schemaname = t1.snap_schemaname ) data "
        "where data.\"%%Idx Blks Hit Ratio\" <> 0 or data.\"Idx Blks Read\" or data.\"Idx Blks Hit\""
        " order by %s %s limit 200",
        field,
        params->begin_snap_id,
        params->report_node,
        cache_io_sys_schema,
        params->end_snap_id,
        params->report_node,
        cache_io_sys_schema,
        sortkey,
        sortByDir);
    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    GenReport::add_data(dash, &params->Contents);

    pfree(query.data);
    pfree(field);
}

/*
 * get Cache IO stat info of one node
 */
static void GetNodeCacheIOData(report_params* params)
{
    char* desc = NULL;
    for (unsigned i = 0; i < lengthof(cacheIODashModule); i++) {
        dashboard* dash = CreateDash();
        dash->dashTitle = cacheIODashModule[i].dashTitle;
        dash->tableTitle = cacheIODashModule[i].tableTitle;
        desc = cacheIODashModule[i].dash1;
        dash->desc = lappend(dash->desc, desc);
        desc = cacheIODashModule[i].dash2;
        dash->desc = lappend(dash->desc, desc);
        if (strcmp(cacheIODashModule[i].sort, "table") == 0) {
            NodeTableIODataOrderbySortKey(params, cacheIODashModule[i].sortkey,
                                          cacheIODashModule[i].sortByDir, dash);
        } else {
            NodeIndexIODataOrderbySortKey(params, cacheIODashModule[i].sortkey,
                                          cacheIODashModule[i].sortByDir, dash);
        }
    }
}

static void ClusterTableIOStrPart(StringInfo query, report_params* params, char* sortkey)
{
    char* field = AdjustFieldOrder(TableIOField, sortkey, lengthof(TableIOField));
    appendStringInfo(query,
        "select data.\"DB Name\",data.\"Schema Name\",data.\"Table Name\""
        "%s from ("
        "SELECT table_io.db_name as \"DB Name\", table_io.snap_schemaname as \"Schema Name\","
        " table_io.snap_relname as \"Table Name\", table_io.heap_blks_hit_ratio as \"%%Heap Blks Hit Ratio\","
        " table_io.heap_blks_read as \"Heap Blks Read\", table_io.heap_blks_hit as  \"Heap Blks Hit\","
        " coalesce(idx_io.idx_blks_read, 0) as \"Idx Blks Read\", coalesce(idx_io.idx_blks_hit, 0) as \"Idx Blks Hit\","
        " table_io.toast_blks_read as \"Toast Blks Read\",  table_io.toast_blks_hit as \"Toast Blks Hit\","
        " table_io.tidx_blks_read as \"Tidx Blks Read\", table_io.tidx_blks_hit as  \"Tidx Blks Hit\" "
        " FROM "
        "(SELECT t2.db_name, t2.snap_schemaname, t2.snap_relname ,"
        " (case when ((t2.snap_heap_blks_read - coalesce(t1.snap_heap_blks_read, 0)) +"
        " (t2.snap_heap_blks_hit - coalesce(t1.snap_heap_blks_hit, 0)))  = 0 then  0 else"
        " pg_catalog.round((t2.snap_heap_blks_hit - coalesce(t1.snap_heap_blks_hit, 0))/"
        " ((t2.snap_heap_blks_read - coalesce(t1.snap_heap_blks_read, 0)) +"
        " (t2.snap_heap_blks_hit - coalesce(t1.snap_heap_blks_hit, 0))) * 100, 2) end ) as heap_blks_hit_ratio,"
        " (t2.snap_heap_blks_read - coalesce(t1.snap_heap_blks_read, 0)) as heap_blks_read,"
        " (t2.snap_heap_blks_hit - coalesce(t1.snap_heap_blks_hit, 0)) as heap_blks_hit,"
        " (t2.snap_toast_blks_read - coalesce(t1.snap_toast_blks_read, 0)) as toast_blks_read,"
        " (t2.snap_toast_blks_hit - coalesce(t1.snap_toast_blks_hit, 0)) as toast_blks_hit,"
        " (t2.snap_tidx_blks_read - coalesce(t1.snap_tidx_blks_read, 0)) as tidx_blks_read,"
        " (t2.snap_tidx_blks_hit - coalesce(t1.snap_tidx_blks_hit, 0)) as tidx_blks_hit"
        " FROM (SELECT C.snap_relid, T.* FROM (select * from snapshot.snap_summary_statio_all_tables where snap_schemaname NOT IN"
        " (%s) AND snap_schemaname !~ '^pg_toast') as T"
        " LEFT JOIN snapshot.snap_class_vital_info AS C"
        " ON T.snap_relname = C.snap_relname AND T.snap_schemaname = C.snap_schemaname and"
        " T.db_name = C.db_name and T.snapshot_id = C.snapshot_id WHERE C.snapshot_id = %ld) AS t1"
        " right join (SELECT C.snap_relid, T.* FROM (select * from snapshot.snap_summary_statio_all_tables where snap_schemaname NOT IN"
        " (%s) AND snap_schemaname !~ '^pg_toast') as T"
        " LEFT JOIN snapshot.snap_class_vital_info AS C ON T.snap_relname = C.snap_relname and"
        " T.snap_schemaname = C.snap_schemaname and T.db_name = C.db_name and T.snapshot_id = C.snapshot_id"
        " WHERE C.snapshot_id = %ld) AS t2 ON (t1.snap_relid = t2.snap_relid  and t1.db_name = t2.db_name"
        " and t1.snap_schemaname = t2.snap_schemaname and t1.snap_relname = t2.snap_relname)) as table_io"
        " LEFT JOIN ",
        field,
        cache_io_sys_schema,
        params->begin_snap_id,
        cache_io_sys_schema,
        params->end_snap_id);
    pfree(field);
}
/*
 * idx_blks_read and idx_blks_hit come from snapshot.snap_summary_statio_user_indexes
 * idx_blks_read and idx_blks_hit will reduce when the index in tables is deleted in
 * snapshot.snap_summary_statio_user_tables
 */
static void ClusterTableIODataOrderBySortKey(report_params* params, char* sortkey,
                                             char* sortByDir, dashboard* dash)
{
    StringInfoData query;
    initStringInfo(&query);
    ClusterTableIOStrPart(&query, params, sortkey);
    appendStringInfo(&query,
        "(Select t2.db_name, t2.snap_schemaname, t2.snap_relname,"
        " pg_catalog.sum(t2.snap_idx_blks_read - coalesce(t1.snap_idx_blks_read, 0)) as idx_blks_read,"
        " pg_catalog.sum(t2.snap_idx_blks_hit - coalesce(t1.snap_idx_blks_hit, 0)) as idx_blks_hit"
        " FROM (SELECT Ti.*, I.snap_relid as snap_indexrelid FROM"
        " (SELECT C.snap_relid, T.* FROM (select * from snapshot.snap_summary_statio_all_indexes"
        " where snap_schemaname NOT IN"
        " (%s) AND snap_schemaname !~ '^pg_toast') as T"
        " LEFT JOIN snapshot.snap_class_vital_info AS C ON (T.snap_relname = C.snap_relname AND"
        " T.snap_schemaname = C.snap_schemaname AND T.db_name = C.db_name AND T.snapshot_id = C.snapshot_id)"
        " WHERE T.snapshot_id = %ld) AS Ti"
        " LEFT JOIN snapshot.snap_class_vital_info as I ON (Ti.snap_indexrelname = I.snap_relname AND"
        " Ti.snap_schemaname = I.snap_schemaname AND Ti.db_name = I.db_name AND Ti.snapshot_id = I.snapshot_id)"
        " WHERE Ti.snapshot_id = %ld) AS t2 LEFT JOIN (SELECT Ti.*, I.snap_relid as snap_indexrelid FROM"
        " (SELECT C.snap_relid, T.* FROM (select * from snapshot.snap_summary_statio_all_indexes "
        "where snap_schemaname NOT IN"
        " (%s) AND snap_schemaname !~ '^pg_toast') as T"
        " LEFT JOIN snapshot.snap_class_vital_info AS C ON (T.snap_relname = C.snap_relname AND"
        " T.snap_schemaname = C.snap_schemaname AND T.db_name = C.db_name AND T.snapshot_id = C.snapshot_id)"
        " WHERE T.snapshot_id = %ld) AS Ti"
        " LEFT JOIN snapshot.snap_class_vital_info as I ON (Ti.snap_indexrelname = I.snap_relname AND"
        " Ti.snap_schemaname = I.snap_schemaname AND Ti.db_name = I.db_name AND I.snapshot_id = Ti.snapshot_id)"
        " WHERE Ti.snapshot_id = %ld) AS t1"
        " ON (t2.snap_relid = t1.snap_relid AND t2.snap_indexrelid = t1.snap_indexrelid AND t1.db_name = t2.db_name"
        " AND t1.snap_schemaname = t2.snap_schemaname) group by t2.db_name, t2.snap_schemaname,"
        " t2.snap_relname) as idx_io"
        " on table_io.db_name = idx_io.db_name and table_io.snap_schemaname =idx_io.snap_schemaname and"
        " table_io.snap_relname = idx_io. snap_relname "
        ") data where "
        "data.\"%%Heap Blks Hit Ratio\" <>0 or data.\"Heap Blks Read\" <>0 or data.\"Heap Blks Hit\" <>0"
        "or data.\"Idx Blks Read\" <>0 or data.\"Idx Blks Hit\" <>0 or data.\"Toast Blks Read\" <>0"
        "or data.\"Toast Blks Hit\" <>0 or data.\"Tidx Blks Read\" <>0 or data.\"Tidx Blks Hit\" <>0"
        "order by %s %s limit 200 ",
        cache_io_sys_schema,
        params->end_snap_id,
        params->end_snap_id,
        cache_io_sys_schema,
        params->begin_snap_id,
        params->begin_snap_id,
        sortkey,
        sortByDir);

    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    GenReport::add_data(dash, &params->Contents);

    pfree(query.data);
}

/*
 * get  Cache IO stat info of Cluster ordered by SortKey
 */
static void ClusterIndexIODataOrderbySortKey(report_params* params, char* sortkey,
                                             char* sortByDir, dashboard* dash)
{
    StringInfoData query;
    initStringInfo(&query);
    char* field = AdjustFieldOrder(IndexIOField, sortkey, lengthof(IndexIOField));
    appendStringInfo(&query,
        "select data.\"DB Name\",data.\"Schema Name\",data.\"Table Name\",data.\"Index Name\"%s  from ("
        "Select t2.db_name as \"DB Name\", t2.snap_schemaname as \"Schema Name\", "
        " t2.snap_relname as \"Table Name\", t2.snap_indexrelname as \"Index Name\", "
        " (case when ((t2.snap_idx_blks_read - coalesce(t1.snap_idx_blks_read, 0)) + "
        " (t2.snap_idx_blks_hit - coalesce(t1.snap_idx_blks_hit, 0))) = 0 then 0 else "
        " pg_catalog.round((t2.snap_idx_blks_hit - coalesce(t1.snap_idx_blks_hit, 0))/"
        " ((t2.snap_idx_blks_hit - coalesce(t1.snap_idx_blks_hit, 0)) + "
        " (t2.snap_idx_blks_read - coalesce(t1.snap_idx_blks_read, 0))) * 100, 2) end) as \"%%Idx Blks Hit Ratio\", "
        " (t2.snap_idx_blks_read - coalesce(t1.snap_idx_blks_read, 0)) as \"Idx Blks Read\", "
        " (t2.snap_idx_blks_hit - coalesce(t1.snap_idx_blks_hit, 0)) as \"Idx Blks Hit\" "
        " FROM (SELECT Ti.*, I.snap_relid as snap_indexrelid FROM"
        " (SELECT C.snap_relid, T.* FROM (select * from snapshot.snap_summary_statio_all_indexes"
        " where snap_schemaname NOT IN"
        " (%s) AND snap_schemaname !~ '^pg_toast') as T"
        " LEFT JOIN snapshot.snap_class_vital_info AS C ON (T.snap_relname = C.snap_relname AND"
        " T.snap_schemaname = C.snap_schemaname AND T.db_name = C.db_name AND T.snapshot_id = C.snapshot_id) "
        "WHERE T.snapshot_id = %ld) AS Ti"
        " LEFT JOIN snapshot.snap_class_vital_info as I ON (Ti.snap_indexrelname = I.snap_relname AND"
        " Ti.snap_schemaname = I.snap_schemaname AND Ti.db_name = I.db_name AND Ti.snapshot_id = I.snapshot_id) "
        "WHERE Ti.snapshot_id = %ld) AS t2"
        " LEFT JOIN (SELECT Ti.*, I.snap_relid as snap_indexrelid FROM"
        " (SELECT C.snap_relid, T.* FROM (select * from snapshot.snap_summary_statio_all_indexes"
        " where snap_schemaname NOT IN"
        " (%s) AND snap_schemaname !~ '^pg_toast') as T"
        " LEFT JOIN snapshot.snap_class_vital_info AS C ON (T.snap_relname = C.snap_relname AND"
        " T.snap_schemaname = C.snap_schemaname AND T.db_name = C.db_name AND T.snapshot_id = C.snapshot_id) "
        "WHERE T.snapshot_id = %ld) AS Ti"
        " LEFT JOIN snapshot.snap_class_vital_info as I ON (Ti.snap_indexrelname = I.snap_relname AND"
        " Ti.snap_schemaname = I.snap_schemaname AND Ti.db_name = I.db_name AND I.snapshot_id = Ti.snapshot_id) "
        "WHERE Ti.snapshot_id = %ld) AS t1"
        " ON (t2.snap_relid = t1.snap_relid AND t2.snap_indexrelid = t1.snap_indexrelid AND t1.db_name = t2.db_name "
        "AND t1.snap_schemaname = t2.snap_schemaname) ) data "
        "where data.\"%%Idx Blks Hit Ratio\" <> 0 or data.\"Idx Blks Read\" or data.\"Idx Blks Hit\""
        " order by %s %s limit 200",
        field,
        cache_io_sys_schema,
        params->end_snap_id,
        params->end_snap_id,
        cache_io_sys_schema,
        params->begin_snap_id,
        params->begin_snap_id,
        sortkey,
        sortByDir);
    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    GenReport::add_data(dash, &params->Contents);

    pfree(query.data);
    pfree(field);
}

/*
 * get  Cache IO stat info of Cluster
 */
static void GetClusterCacheIOData(report_params* params)
{
    char* desc = NULL;
    for (unsigned i = 0; i < lengthof(cacheIODashModule); i++) {
        dashboard* dash = CreateDash();
        dash->dashTitle = cacheIODashModule[i].dashTitle;
        dash->tableTitle = cacheIODashModule[i].tableTitle;
        desc = cacheIODashModule[i].dash1;
        dash->desc = lappend(dash->desc, desc);
        desc = cacheIODashModule[i].dash2;
        dash->desc = lappend(dash->desc, desc);
        if (strcmp(cacheIODashModule[i].sort, "table") == 0) {
            ClusterTableIODataOrderBySortKey(params, cacheIODashModule[i].sortkey,
                                             cacheIODashModule[i].sortByDir, dash);
        } else {
            ClusterIndexIODataOrderbySortKey(params, cacheIODashModule[i].sortkey,
                                             cacheIODashModule[i].sortByDir, dash);
        }
    }
}

static void GetCacheIOData(report_params* params)
{
    if (is_summary_report(params)) {
        return;
    }
    if (is_single_node_report(params)) {
        GetNodeCacheIOData(params);
    } else if (is_cluster_report(params)) {
        GetClusterCacheIOData(params);
    } else {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("no this type of report_scope")));
    }
}

/*
 * generate sql to get the diff seconds for two snapshots
 *
 * return: -1 query with no matched result, > 0 successful
 */
int64 get_snap_gap_by_table(report_params* params, const char* table_name)
{
    Assert(params);
    Assert(table_name);
    int64 result = -1;
    bool is_null = false;

    // SQL - get snapshot gap(seconds)
    StringInfoData snap_diff_sec_sql;
    initStringInfo(&snap_diff_sec_sql);

    // using table's snapshot timestamp
    appendStringInfo(
        &snap_diff_sec_sql, "SELECT greatest(EXTRACT(EPOCH FROM (snap_2.start_ts - snap_1.start_ts)), 1)::int8 from ");

    appendStringInfo(&snap_diff_sec_sql,
        "(select * from snapshot.tables_snap_timestamp "
        "    where snapshot_id = %ld and db_name = 'postgres' and tablename = '%s') snap_1, ",
        params->begin_snap_id,
        table_name);

    appendStringInfo(&snap_diff_sec_sql,
        "(select * from snapshot.tables_snap_timestamp "
        "    where snapshot_id = %ld and db_name = 'postgres' and tablename = '%s') snap_2",
        params->end_snap_id,
        table_name);

    Datum datum = GetDatumValue(snap_diff_sec_sql.data, 0, 0, &is_null);
    pfree(snap_diff_sec_sql.data);
    if (SPI_processed > 0) {
        // if null, return 1(as snap gap will be divisor)
        if (is_null) {
            result = 1;
        } else {
            result = (int64)Int64GetDatum(datum);
            if (result == 0) {
                result = 1;
            }
        }
    }

    return result;
}

/*
 * generate sql to get the diff trx count for two snapshots,
 * and also update commit/rollback trx counter in report_params
 *
 * return: -1 query with no matched result, > 0 successful
 */
int64 get_snap_diff_trx_count(report_params* params)
{
    Assert(params);
    int64 result = -1;
    int fno = 1;
    bool is_null = false;

    /* SQL: get diff of two snapshots(trx count) */
    StringInfoData snap_diff_trx_count;
    initStringInfo(&snap_diff_trx_count);

    // if two snapshot's trx counts are same, return 1
    appendStringInfo(&snap_diff_trx_count,
        "select "
        "    coalesce((snap_2.commit_counter - coalesce(snap_1.commit_counter, 0)), 0)::int8, "
        "    coalesce((snap_2.rollback_counter - coalesce(snap_1.rollback_counter, 0)), 0)::int8 from "
        "    (select (pg_catalog.sum(snap_commit_counter) + pg_catalog.sum(snap_bg_commit_counter)) "
        "    as commit_counter,"
        "        (pg_catalog.sum(snap_rollback_counter) + pg_catalog.sum(snap_bg_rollback_counter)) "
        "        as rollback_counter "
        "        from snapshot.snap_summary_workload_transaction where snapshot_id = %ld) snap_1, "
        "    (select (pg_catalog.sum(snap_commit_counter) + pg_catalog.sum(snap_bg_commit_counter)) as commit_counter,"
        "        (pg_catalog.sum(snap_rollback_counter) + pg_catalog.sum(snap_bg_rollback_counter)) "
        "        as rollback_counter "
        "        from snapshot.snap_summary_workload_transaction  where snapshot_id = %ld) snap_2 ",
        params->begin_snap_id,
        params->end_snap_id);
    if (SPI_execute(snap_diff_trx_count.data, false, 0) != SPI_OK_SELECT) {
        pfree(snap_diff_trx_count.data);
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("calc trx diff count failed!")));
    }
    pfree(snap_diff_trx_count.data);

    if (SPI_processed == 1) {
        /* get commit counter */
        Datum colval = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, fno, &is_null);
        if (is_null) {
            result = 1;
        } else {
            result = (int64)Int64GetDatum(colval);
        }

        params->snap_diff_commit_trx_count = result;
        fno++;
        /* get rollback counter */
        colval = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, fno, &is_null);
        if (is_null) {
            result = 1;
        } else {
            result = (int64)Int64GetDatum(colval);
        }

        params->snap_diff_rollback_trx_count = result;

        result = params->snap_diff_commit_trx_count + params->snap_diff_rollback_trx_count;
        // as trx counter will be used as divisor, cann't be zero
        if (result == 0) {
            result = 1;
        }
    }
    return result;
}

/*
 * get diff sql counts of two snapshots
 *
 * return:
 *   0  - empty resultset
 *   >0 - valid result
 */
uint64 get_snap_diff_sql_count(report_params* params)
{
    Assert(params);
    uint64 result = 0;
    bool is_null = false;

    /* SQL: get diff sql count of two snapshots */
    StringInfoData snap_diff_sql_count;
    initStringInfo(&snap_diff_sql_count);

    /* In order to get cluster summary, the sql count is devided to two parts:
       1. ddl, dcl count are calculated with order by node_name and workload to ensure
          the data in the same node between snap_1 and snap_2;
       2. dml count is caculated by sum function with group by workload. */
    appendStringInfo(
        &snap_diff_sql_count, "select (snap_2.sql_count - coalesce(snap_1.sql_count, 0))::int8 as sql_count from ");
    appendStringInfo(&snap_diff_sql_count,
        " (select (coalesce(dc.ddl_count, 0) + coalesce(dc.dcl_count, 0) + coalesce(dm.dml_count, 0)) as sql_count from"
        " (select snap_node_name, pg_catalog.sum(snap_ddl_count) as ddl_count, "
        " pg_catalog.sum(snap_dcl_count) as dcl_count from "
        " snapshot.snap_summary_workload_sql_count where snapshot_id =%ld "
        "      group by snap_node_name limit 1) as dc, "
        "    (select pg_catalog.sum(snap_dml_count) as dml_count from "
        "      snapshot.snap_summary_workload_sql_count where snapshot_id=%ld) as dm) snap_1, ",
        params->begin_snap_id,
        params->begin_snap_id);
    appendStringInfo(&snap_diff_sql_count,
        " (select (coalesce(dc.ddl_count, 0) + coalesce(dc.dcl_count, 0) + coalesce(dm.dml_count, 0)) as sql_count from"
        " (select snap_node_name, pg_catalog.sum(snap_ddl_count) as ddl_count, "
        " pg_catalog.sum(snap_dcl_count) as dcl_count from "
        "      snapshot.snap_summary_workload_sql_count where snapshot_id =%ld "
        "      group by snap_node_name limit 1) as dc, "
        "    (select pg_catalog.sum(snap_dml_count) as dml_count from "
        "      snapshot.snap_summary_workload_sql_count where snapshot_id=%ld) as dm) snap_2",
        params->end_snap_id,
        params->end_snap_id);

    Datum datum = GetDatumValue(snap_diff_sql_count.data, 0, 0, &is_null);
    pfree(snap_diff_sql_count.data);

    if (SPI_processed > 0) {
        if (is_null) {
            return 1;
        } else {
            result = UInt64GetDatum(datum);
            if (result == 0) {
                result = 1;
            }
        }
    }

    return result;
}

/*
 * update snapshot gap of report params by table name
 *
 *
 * return true if get snapshot gap successfully
 *
 */
static bool update_report_snap_gap_param(report_params* params, const char* table_name)
{
    if (params == NULL || table_name == NULL) {
        return false;
    }

    int64 snap_tbl_gap = get_snap_gap_by_table(params, table_name);
    if (snap_tbl_gap <= 0) {
        return false;
    }

    params->snap_gap = snap_tbl_gap;

    return true;
}

/* summary report - database stat information */
void GenReport::get_summary_database_stat(report_params* params)
{
    /* supported report type: summary/all */
    /* supported report scope: cluster */
    if (!is_cluster_report(params)) {
        return;
    }
    if (!is_summary_report(params) && !is_full_report(params)) {
        return;
    }

    dashboard* dash = CreateDash();

    const char* desc = "Show database stat information";
    dash->dashTitle = "Summary";
    dash->tableTitle = "Database Stat";
    dash->desc = lappend(dash->desc, (void*)desc);

    StringInfoData query;
    initStringInfo(&query);

    appendStringInfo(&query,
        "select snap_2.snap_datname as \"DB Name\", "
        " snap_2.snap_numbackends as \"Backends\","
        " (snap_2.snap_xact_commit - coalesce(snap_1.snap_xact_commit, 0)) as \"Xact Commit\", "
        " (snap_2.snap_xact_rollback - coalesce(snap_1.snap_xact_rollback, 0)) as \"Xact Rollback\", "
        " (snap_2.snap_blks_read - coalesce(snap_1.snap_blks_read, 0)) as \"Blks Read\", "
        " (snap_2.snap_blks_hit - coalesce(snap_1.snap_blks_hit, 0)) as \"Blks Hit\", "
        " (snap_2.snap_tup_returned - coalesce(snap_1.snap_tup_returned, 0)) as \"Tuple Returned\", "
        " (snap_2.snap_tup_fetched - coalesce(snap_1.snap_tup_fetched, 0)) as \"Tuple Fetched\", "
        " (snap_2.snap_tup_inserted - coalesce(snap_1.snap_tup_inserted, 0)) as \"Tuple Inserted\", "
        " (snap_2.snap_tup_updated - coalesce(snap_1.snap_tup_updated, 0)) as \"Tuple Updated\", "
        " (snap_2.snap_tup_deleted - coalesce(snap_1.snap_tup_deleted, 0)) as \"Tup Deleted\", "
        " (snap_2.snap_conflicts - coalesce(snap_1.snap_conflicts, 0)) as \"Conflicts\", "
        " (snap_2.snap_temp_files - coalesce(snap_1.snap_temp_files, 0)) as \"Temp Files\", "
        " (snap_2.snap_temp_bytes - coalesce(snap_1.snap_temp_bytes, 0)) as \"Temp Bytes\", "
        " (snap_2.snap_deadlocks - coalesce(snap_1.snap_deadlocks, 0)) as \"Deadlocks\", "
        " (snap_2.snap_blk_read_time - coalesce(snap_1.snap_blk_read_time, 0)) as \"Blk Read Time\", "
        " (snap_2.snap_blk_write_time - coalesce(snap_1.snap_blk_write_time, 0)) as \"Blk Write Time\", "
        " pg_catalog.to_char(snap_2.snap_stats_reset, 'YYYY-MM-DD HH24:MI:SS') AS \"Stats Reset\" ");
    appendStringInfo(&query,
        "from (select * from snapshot.snap_summary_stat_database where snapshot_id = %ld "
        "and snap_datname != 'template0' and snap_datname != 'template1') snap_2 ",
        params->end_snap_id);
    appendStringInfo(&query,
        "left join (select * from snapshot.snap_summary_stat_database "
        "where snapshot_id = %ld) snap_1 ",
        params->begin_snap_id);
    appendStringInfo(&query, "on snap_1.snap_datname = snap_2.snap_datname order by "
        "\"Xact Commit\" desc;");

    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    pfree(query.data);

    GenReport::add_data(dash, &params->Contents);
}

/*
 * pre-check for load profile
 *
 * return:
 *   true - passed
 *   false - failed
 */
static bool pre_check_load_profile(report_params* params, dashboard* dash)
{
    if (params == NULL || dash == NULL) {
        return false;
    }
    if (get_report_snap_gap(params) <= 0) {
        return false;
    }
    if (get_report_snap_diff_trx_count(params) <= 0) {
        return false;
    }
    if (get_report_snap_diff_sql_count(params) == 0) {
        return false;
    }

    return true;
}

/*
 * add row to summary - load profile report
 */
static void get_summary_load_profile_db_cpu_time(report_params* params, dashboard* dash)
{
    if (!pre_check_load_profile(params, dash)) {
        return;
    }

    List* query_result = NIL;

    StringInfoData query;
    initStringInfo(&query);

    // diff (snap_2 db cpu and snap_1 db cpu)
    appendStringInfo(&query, "select 'CPU Time(us)' as \"%s\", ", g_metric_name);
    appendStringInfo(&query,
        "((snap_2.cpu_time - snap_1.cpu_time) / (%ld))::int8 as \"%s\", ",
        get_report_snap_gap(params),
        g_per_second_str);
    appendStringInfo(&query,
        "((snap_2.cpu_time - snap_1.cpu_time) / (%ld))::int8 as \"%s\", ",
        get_report_snap_diff_trx_count(params),
        g_per_trx_str);
    appendStringInfo(&query,
        "((snap_2.cpu_time - snap_1.cpu_time) / (%lu))::int8 as \"%s\" from ",
        get_report_snap_diff_sql_count(params),
        g_per_exec_str);
    // -- snap 2 - db cpu time
    appendStringInfo(&query, "    (select pg_catalog.SUM(snap_value) as cpu_time from "
                             "    snapshot.snap_global_instance_time ");
    appendStringInfo(
        &query, "        where snap_stat_name = 'CPU_TIME' and snapshot_id = %ld) snap_2, ", params->end_snap_id);
    // -- snap 1 - db cpu time
    appendStringInfo(&query, "    (select pg_catalog.SUM(snap_value) as cpu_time from "
                             "    snapshot.snap_global_instance_time ");
    appendStringInfo(
        &query, "        where snap_stat_name = 'CPU_TIME' and snapshot_id = %ld) snap_1;", params->begin_snap_id);

    GenReport::get_query_data(query.data, false, &query_result, &dash->type);
    dash->table = list_concat(dash->table, query_result);

    pfree(query.data);
}

static void get_summary_load_profile_db_time(report_params* params, dashboard* dash)
{
    if (!pre_check_load_profile(params, dash)) {
        return;
    }

    List* query_result = NIL;

    StringInfoData query;
    initStringInfo(&query);

    // diff (snap_2 db time and snap_1 db time)
    appendStringInfo(&query, "select 'DB Time(us)' as \"%s\", ", g_metric_name);
    appendStringInfo(&query,
        "((snap_2.db_time - snap_1.db_time) / (%ld))::int8 as \"%s\", ",
        get_report_snap_gap(params),
        g_per_second_str);
    appendStringInfo(&query,
        "((snap_2.db_time - snap_1.db_time) / (%ld))::int8 as \"%s\", ",
        get_report_snap_diff_trx_count(params),
        g_per_trx_str);
    appendStringInfo(&query,
        "((snap_2.db_time - snap_1.db_time) / (%lu))::int8 as \"%s\" from ",
        get_report_snap_diff_sql_count(params),
        g_per_exec_str);
    // -- snap 2 - db time
    appendStringInfo(&query, "    (select pg_catalog.SUM(snap_value) as db_time from "
                             "    snapshot.snap_global_instance_time ");
    appendStringInfo(
        &query, "        where snap_stat_name = 'DB_TIME' and snapshot_id = %ld) snap_2, ", params->end_snap_id);
    // -- snap 1 - db time
    appendStringInfo(&query, "    (select pg_catalog.SUM(snap_value) as db_time from "
                             "    snapshot.snap_global_instance_time ");
    appendStringInfo(
        &query, "        where snap_stat_name = 'DB_TIME' and snapshot_id = %ld) snap_1;", params->begin_snap_id);

    GenReport::get_query_data(query.data, !list_length(dash->table), &query_result, &dash->type);
    dash->table = list_concat(dash->table, query_result);

    pfree(query.data);
}

static void get_summary_load_profile_redo(report_params* params, dashboard* dash)
{
    if (!pre_check_load_profile(params, dash)) {
        return;
    }

    List* query_result = NIL;

    StringInfoData query;
    initStringInfo(&query);

    appendStringInfo(&query,
        "select 'Redo size(blocks)' as \"%s\","
        "    ((snap_2.phyblkwrt - snap_1.phyblkwrt) / (%ld))::int8 as \"%s\", "
        "    ((snap_2.phyblkwrt - snap_1.phyblkwrt) / (%ld))::int8 as \"%s\", "
        "    ((snap_2.phyblkwrt - snap_1.phyblkwrt) / (%lu))::int8 as \"%s\" "
        "from "
        "    (select snap_phyblkwrt as phyblkwrt  "
        "        from snapshot.snap_summary_file_redo_iostat "
        "        where snapshot_id = %ld) snap_1, "
        "    (select snap_phyblkwrt as phyblkwrt "
        "        from snapshot.snap_summary_file_redo_iostat "
        "        where snapshot_id = %ld) snap_2",
        g_metric_name,
        get_report_snap_gap(params),
        g_per_second_str,
        get_report_snap_diff_trx_count(params),
        g_per_trx_str,
        get_report_snap_diff_sql_count(params),
        g_per_exec_str,
        params->begin_snap_id,
        params->end_snap_id);

    GenReport::get_query_data(query.data, !list_length(dash->table), &query_result, &dash->type);
    dash->table = list_concat(dash->table, query_result);
    pfree(query.data);
}

/* get metric(logical read) within two snapshots */
static void get_summary_load_profile_logical_read(report_params* params, dashboard* dash)
{
    if (!pre_check_load_profile(params, dash)) {
        return;
    }

    List* query_result = NIL;

    StringInfoData query;
    initStringInfo(&query);

    appendStringInfo(&query,
        "select 'Logical read (blocks)' as \"%s\", "
        "    ((snap_2.logical_read - snap_1.logical_read) / (%ld))::int8 as \"%s\", "
        "    ((snap_2.logical_read - snap_1.logical_read) / (%ld))::int8 as \"%s\", "
        "    ((snap_2.logical_read - snap_1.logical_read) / (%lu))::int8 as \"%s\" "
        "from "
        "    (select pg_catalog.sum(coalesce(snap_blks_read, 0) + coalesce(snap_blks_hit, 0)) "
        "        as logical_read "
        "        from snapshot.snap_summary_stat_database "
        "        where snapshot_id = %ld) snap_1, "
        "    (select pg_catalog.sum(coalesce(snap_blks_read, 0) + coalesce(snap_blks_hit, 0)) "
        "        as logical_read "
        "        from snapshot.snap_summary_stat_database "
        "        where snapshot_id = %ld) snap_2",
        g_metric_name,
        get_report_snap_gap(params),
        g_per_second_str,
        get_report_snap_diff_trx_count(params),
        g_per_trx_str,
        get_report_snap_diff_sql_count(params),
        g_per_exec_str,
        params->begin_snap_id,
        params->end_snap_id);

    GenReport::get_query_data(query.data, !list_length(dash->table), &query_result, &dash->type);
    dash->table = list_concat(dash->table, query_result);

    pfree(query.data);
}

static void get_summary_load_profile_file_io(report_params* params, dashboard* dash)
{
    if (!pre_check_load_profile(params, dash)) {
        return;
    }

    List* query_result = NIL;
    StringInfoData query;

    initStringInfo(&query);
    appendStringInfo(&query,
        "select   pg_catalog.unnest(array[ 'Physical read (blocks)', 'Physical write (blocks)', "
        "  'Read IO requests', 'Write IO requests', 'Read IO (MB)',  'Write IO (MB)']) as \"%s\", "
        "    pg_catalog.round(pg_catalog.unnest( array[  phyblkrd, phyblkwrt,  phyrds, "
        "                 phywrts,  (phyblkrd * %d) >> 20,  (phyblkwrt * %d) >> 20 ]  ) / %ld) as \"%s\", "
        "    pg_catalog.round(pg_catalog.unnest(  array[  phyblkrd, phyblkwrt, phyrds, phywrts, (phyblkrd * %d) >> 20, "
        "                 (phyblkwrt * %d) >> 20] ) / %ld) as \"%s\", "
        "    pg_catalog.round(pg_catalog.unnest( array[ phyblkrd, phyblkwrt,  phyrds, phywrts,  (phyblkrd * %d) >> 20, "
        "                 (phyblkwrt * %d) >> 20  ] ) / %lu) as \"%s\" "
        "from  (select (snap_2.phyblkrd - snap_1.phyblkrd) as phyblkrd, "
        "        (snap_2.phyblkwrt - snap_1.phyblkwrt) as phyblkwrt, "
        "        (snap_2.phyrds - snap_1.phyrds) as phyrds,  (snap_2.phywrts - snap_1.phywrts) as phywrts "
        "    from   (select snap_phyblkwrt as phyblkwrt, "
        "            snap_phyblkrd as phyblkrd, snap_phyrds as phyrds, "
        "            snap_phywrts as phywrts from snapshot.snap_summary_rel_iostat "
        "            where snapshot_id = %ld) snap_1, (select snap_phyblkwrt as phyblkwrt, "
        "            snap_phyblkrd as phyblkrd,  snap_phyrds as phyrds, "
        "            snap_phywrts as phywrts from snapshot.snap_summary_rel_iostat "
        "            where snapshot_id = %ld) snap_2) snap_diff",
        g_metric_name,
        BLCKSZ,
        BLCKSZ,
        get_report_snap_gap(params),
        g_per_second_str,
        BLCKSZ,
        BLCKSZ,
        get_report_snap_diff_trx_count(params),
        g_per_trx_str,
        BLCKSZ,
        BLCKSZ,
        get_report_snap_diff_sql_count(params),
        g_per_exec_str,
        params->begin_snap_id,
        params->end_snap_id);

    GenReport::get_query_data(query.data, !list_length(dash->table), &query_result, &dash->type);
    dash->table = list_concat(dash->table, query_result);

    pfree(query.data);
}

/* load profile - logons */
static void get_summary_load_profile_logins(report_params* params, dashboard* dash)
{
    if (!pre_check_load_profile(params, dash)) {
        return;
    }

    List* query_result = NULL;
    StringInfoData query;

    initStringInfo(&query);
    appendStringInfo(&query,
        "select 'Logins' as \"%s\", "
        "    pg_catalog.round((snap_2.login_counter - snap_1.login_counter) / %ld)"
        "        as \"%s\", "
        "    pg_catalog.round((snap_2.login_counter - snap_1.login_counter) / %ld)"
        "        as \"%s\", "
        "    pg_catalog.round((snap_2.login_counter - snap_1.login_counter) / %lu)"
        "        as \"%s\" "
        "from "
        "    (select pg_catalog.sum(snap_login_counter) as login_counter "
        "        from snapshot.snap_summary_user_login where snapshot_id = %ld) snap_1, "
        "    (select pg_catalog.sum(snap_login_counter) as login_counter "
        "        from snapshot.snap_summary_user_login where snapshot_id = %ld) snap_2",
        g_metric_name,
        get_report_snap_gap(params),
        g_per_second_str,
        get_report_snap_diff_trx_count(params),
        g_per_trx_str,
        get_report_snap_diff_sql_count(params),
        g_per_exec_str,
        params->begin_snap_id,
        params->end_snap_id);

    GenReport::get_query_data(query.data, !list_length(dash->table), &query_result, &dash->type);
    dash->table = list_concat(dash->table, query_result);

    pfree(query.data);
}

/* load profile - show executes(SQL) */
static void get_summary_load_profile_executes(report_params* params, dashboard* dash)
{
    if (!pre_check_load_profile(params, dash)) {
        return;
    }

    List* query_result = NIL;
    StringInfoData query;

    initStringInfo(&query);
    appendStringInfo(&query,
        "select 'Executes (SQL)' as \"%s\", "
        "    pg_catalog.round(%lu / %ld) as \"%s\", "
        "    pg_catalog.round(%lu / %ld) as \"%s\", "
        "    pg_catalog.round(%lu / %lu) as \"%s\"",
        g_metric_name,
        get_report_snap_diff_sql_count(params),
        get_report_snap_gap(params),
        g_per_second_str,
        get_report_snap_diff_sql_count(params),
        get_report_snap_diff_trx_count(params),
        g_per_trx_str,
        get_report_snap_diff_sql_count(params),
        get_report_snap_diff_sql_count(params),
        g_per_exec_str);

    GenReport::get_query_data(query.data, !list_length(dash->table), &query_result, &dash->type);
    dash->table = list_concat(dash->table, query_result);

    pfree(query.data);
}

/* load profile - rollback/transaction stat */
static void get_summary_load_profile_trx(report_params* params, dashboard* dash)
{
    if (!pre_check_load_profile(params, dash)) {
        return;
    }

    List* query_result = NIL;
    StringInfoData query;

    initStringInfo(&query);
    appendStringInfo(&query,
        "select "
        "    pg_catalog.unnest(array['Rollbacks', 'Transactions']) as \"%s\" ,"
        "    pg_catalog.round(pg_catalog.unnest(array[%ld, %ld]) / %ld ) as \"%s\", "
        "    pg_catalog.unnest(array[' ', ' ']) as \"%s\", "
        "    pg_catalog.unnest(array[' ', ' ']) as \"%s\" ",
        g_metric_name,
        params->snap_diff_rollback_trx_count,
        get_report_snap_diff_trx_count(params),
        get_report_snap_gap(params),
        g_per_second_str,

        g_per_trx_str,
        g_per_exec_str);
    GenReport::get_query_data(query.data, !list_length(dash->table), &query_result, &dash->type);
    dash->table = list_concat(dash->table, query_result);
    pfree(query.data);
}

/* load profile - sql response time */
static void get_summary_load_profile_sql_resp_time(report_params* params, dashboard* dash)
{
    if (!pre_check_load_profile(params, dash)) {
        return;
    }

    List* query_result = NIL;
    StringInfoData query;

    initStringInfo(&query);
    appendStringInfo(&query,
        "select "
        "pg_catalog.unnest(array['SQL response time P95(us)', 'SQL response time P80(us)']) as \"%s\", "
        "pg_catalog.round(pg_catalog.unnest(array[snap_P95, snap_P80])) as \"%s\" "
        "from "
        "snapshot.snap_statement_responsetime_percentile where snapshot_id = %ld",
        g_metric_name,
        g_value_str,
        params->end_snap_id);

    GenReport::get_query_data(query.data, !list_length(dash->table), &query_result, &dash->type);
    dash->table = list_concat(dash->table, query_result);
    pfree(query.data);
}
static void get_summary_load_profile_part(report_params* params, dashboard* dash)
{
    if (update_report_snap_gap_param(params, "snap_global_instance_time")) {
        /* load profile - DB TIME */
        get_summary_load_profile_db_time(params, dash);

        /* load profile - cput time */
        get_summary_load_profile_db_cpu_time(params, dash);
    }

    /* load profile - redo size */
    if (update_report_snap_gap_param(params, "snap_summary_file_redo_iostat")) {
        get_summary_load_profile_redo(params, dash);
    }

    /* load profile - logical read */
    if (update_report_snap_gap_param(params, "snap_summary_stat_database")) {
        get_summary_load_profile_logical_read(params, dash);
    }

    /*
     * load profile - file statio related
     *
     * Block changes(blocks)
     * Physical read (blocks)
     * Physical write (blocks)
     * Read IO requests
     * Write IO requests
     * Read IO (MB)
     * Write IO (MB)
     */
    if (update_report_snap_gap_param(params, "snap_summary_rel_iostat")) {
        get_summary_load_profile_file_io(params, dash);
    }

    /* load profile - logins */
    if (update_report_snap_gap_param(params, "snap_summary_user_login")) {
        get_summary_load_profile_logins(params, dash);
    }

    /* load profile - Executes(SQL) */
    if (update_report_snap_gap_param(params, "snap_summary_workload_transaction")) {
        get_summary_load_profile_executes(params, dash);
    }

    /* load profile - Rollbacks/Transaction */
    if (update_report_snap_gap_param(params, "snap_summary_workload_transaction")) {
        get_summary_load_profile_trx(params, dash);
    }
}

/* Used to store tables with only one column of values */
static void get_summary_load_profile_part_single_value(report_params* params, dashboard* dash)
{
    /* SQL response time P90/P85 */
    if (update_report_snap_gap_param(params, "snap_statement_responsetime_percentile")) {
        get_summary_load_profile_sql_resp_time(params, dash);
    }
}

/* summary report - load profile information */
void GenReport::get_summary_load_profile(report_params* params)
{
    /* supported report type: summary/all */
    /* supported report scope: cluster */
    if (!is_cluster_report(params)) {
        return;
    }
    if (!is_summary_report(params) && !is_full_report(params)) {
        return;
    }

    dashboard* dash = CreateDash();

    const char* desc = "Get database workload activity during the snapshot interval";
    dash->dashTitle = "Summary";
    dash->tableTitle = "Load Profile";
    dash->desc = lappend(dash->desc, (void*)desc);

    // do we need to control load profile's cell visibility by the diff values?
    // for example, if snap gap is 0 or -1, maybe we can hide 'Per Seconds' column
    /* prepare - get Per Seconds/Pre Trx/Per Exec info */
    int64 snap_diff_trx_count = get_snap_diff_trx_count(params);
    if (snap_diff_trx_count <= 0) {
        ereport(WARNING, (errmsg("diff trx count between two snapshots <= 0")));
        return;
    }

    uint64 snap_diff_sql_count = get_snap_diff_sql_count(params);
    if (snap_diff_sql_count == 0) {
        ereport(WARNING, (errmsg("diff SQL count between two snapshots is 0")));
        return;
    }

    params->snap_diff_trx_count = snap_diff_trx_count;
    params->snap_diff_sql_count = snap_diff_sql_count;
    get_summary_load_profile_part(params, dash);
    GenReport::add_data(dash, &params->Contents);

    dash = CreateDash();
    desc = "SQL response time P80/P95";
    dash->dashTitle = "Summary";
    dash->tableTitle = "Load Profile";
    dash->desc = lappend(dash->desc, (void*)desc);
    get_summary_load_profile_part_single_value(params, dash);
    GenReport::add_data(dash, &params->Contents);
}

#ifdef ENABLE_MULTIPLE_NODES
static void get_summary_instance_efficiency_percentages(report_params* params, dashboard* dash)
{
    List* query_result = NIL;
    StringInfoData query;
    initStringInfo(&query);

    appendStringInfo(&query,
        "select 'Buffer Hit %%: ' as \"Metric Name\", "
        "    case when s.all_reads = 0 then 1 else pg_catalog.round(s.blks_hit * 100 / s.all_reads, 2) "
        "    end as \"Metric Value\" "
        "from "
        "  (select (snap_2.all_reads - coalesce(snap_1.all_reads, 0)) as all_reads, "
        "       (snap_2.blks_hit - coalesce(snap_1.blks_hit, 0)) as blks_hit "
        "   from"
        "     (select pg_catalog.sum(coalesce(snap_blks_read, 0) + coalesce(snap_blks_hit, 0)) as all_reads, "
        "        coalesce(pg_catalog.sum(snap_blks_hit), 0) as blks_hit "
        "        from snapshot.snap_summary_stat_database "
        "        where snapshot_id = %ld) snap_1, "
        "     (select pg_catalog.sum(coalesce(snap_blks_read, 0) + coalesce(snap_blks_hit, 0)) as all_reads, "
        "        coalesce(pg_catalog.sum(snap_blks_hit), 0) as blks_hit "
        "        from snapshot.snap_summary_stat_database "
        "        where snapshot_id = %ld) snap_2"
        "   ) as s",
        params->begin_snap_id,
        params->end_snap_id);

    GenReport::get_query_data(query.data, true, &query_result, &dash->type);
    dash->table = list_concat(dash->table, query_result);

    pfree(query.data);
}
#else
/* summary ratios about instance effciency: buffer hit ratio, cpu efficiency ratio, radio of redo
 *    with nowait, soft parse ratio and excution without parse ratio */
static void get_summary_instance_efficiency_percentages(report_params* params, dashboard* dash)
{
    List* query_result = NIL;
    StringInfoData query;
    initStringInfo(&query);

    appendStringInfo(&query,
        "select "
        "    pg_catalog.unnest(array['Buffer Hit %%', 'Effective CPU %%', 'WalWrite NoWait %%', 'Soft Parse %%',"
        " 'Non-Parse CPU %%']) as \"Metric Name\", "
        "    pg_catalog.unnest(array[case when s1.all_reads = 0 then 1 "
        "else pg_catalog.round(s1.blks_hit * 100 / s1.all_reads, 2) end, s2.cpu_to_elapsd, s3.walwrite_nowait,"
        " s4.soft_parse, s5.non_parse]) as \"Metric Value\" "
        "from "
        "  (select (snap_2.all_reads - coalesce(snap_1.all_reads, 0)) as all_reads, "
        "       (snap_2.blks_hit - coalesce(snap_1.blks_hit, 0)) as blks_hit "
        "   from "
        "     (select pg_catalog.sum(coalesce(snap_blks_read, 0) + coalesce(snap_blks_hit, 0)) as all_reads, "
        "        coalesce(pg_catalog.sum(snap_blks_hit), 0) as blks_hit "
        "        from snapshot.snap_summary_stat_database "
        "        where snapshot_id = %ld) snap_1, "
        "     (select pg_catalog.sum(coalesce(snap_blks_read, 0) + coalesce(snap_blks_hit, 0)) as all_reads, "
        "        coalesce(pg_catalog.sum(snap_blks_hit), 0) as blks_hit "
        "        from snapshot.snap_summary_stat_database "
        "        where snapshot_id = %ld) snap_2 "
        "   ) s1, "
        "  (select pg_catalog.round(cpu_time.snap_value * 100 / greatest(db_time.snap_value, 1)) as cpu_to_elapsd "
        "   from "
        "     (select coalesce(snap_2.snap_value, 0) - coalesce(snap_1.snap_value, 0) as snap_value "
        "      from "
        "        (select snap_stat_name, snap_value from snapshot.snap_global_instance_time "
        "           where snapshot_id = %ld and snap_stat_name = 'CPU_TIME') snap_1, "
        "        (select snap_stat_name, snap_value from snapshot.snap_global_instance_time "
        "           where snapshot_id = %ld and snap_stat_name = 'CPU_TIME') snap_2) cpu_time, "
        "     (select coalesce(snap_2.snap_value, 0) - coalesce(snap_1.snap_value, 0) as snap_value "
        "      from "
        "        (select snap_stat_name, snap_value from snapshot.snap_global_instance_time "
        "           where snapshot_id = %ld and snap_stat_name = 'DB_TIME') snap_1, "
        "        (select snap_stat_name, snap_value from snapshot.snap_global_instance_time "
        "           where snapshot_id = %ld and snap_stat_name = 'DB_TIME') snap_2) db_time "
        "  ) s2, "
        "  (select pg_catalog.round((bufferAccess.snap_wait - bufferFull.snap_wait) * 100 /"
        " greatest(bufferAccess.snap_wait, 1)) as walwrite_nowait "
        "   from "
        "     (select coalesce(snap_2.snap_wait) - coalesce(snap_1.snap_wait, 0) as snap_wait "
        "      from "
        "        (select snap_wait from snapshot.snap_global_wait_events "
        "           where snapshot_id = %ld and snap_event  = 'WALBufferFull') snap_1, "
        "        (select snap_wait from snapshot.snap_global_wait_events "
        "           where snapshot_id = %ld and snap_event  = 'WALBufferFull') snap_2) bufferFull, "
        "     (select coalesce(snap_2.snap_wait) - coalesce(snap_1.snap_wait, 0) as snap_wait "
        "      from "
        "        (select snap_wait from snapshot.snap_global_wait_events "
        "           where snapshot_id = %ld and snap_event = 'WALBufferAccess') snap_1, "
        "        (select snap_wait from snapshot.snap_global_wait_events "
        "           where snapshot_id = %ld and snap_event = 'WALBufferAccess') snap_2) bufferAccess "
        "  ) s3, "
        "  (select pg_catalog.round((snap_2.soft_parse - snap_1.soft_parse) * 100 "
        " / greatest((snap_2.hard_parse + snap_2.soft_parse)-(snap_1.hard_parse + snap_1.soft_parse), 1))"
        " as soft_parse "
        "   from "
        "     (select pg_catalog.sum(snap_n_soft_parse) as soft_parse, pg_catalog.sum(snap_n_hard_parse) as hard_parse"
        " from snapshot.snap_summary_statement "
        "        where snapshot_id = %ld ) snap_1, "
        "     (select pg_catalog.sum(snap_n_soft_parse) as soft_parse, pg_catalog.sum(snap_n_hard_parse) as hard_parse"
        " from snapshot.snap_summary_statement "
        "        where snapshot_id = %ld ) snap_2 "
        "  ) s4, "
        "  (select pg_catalog.round((snap_2.elapse_time - snap_1.elapse_time) * 100 /"
        " greatest((snap_2.elapse_time + snap_2.parse_time)-(snap_1.elapse_time + snap_1.parse_time), 1)) as non_parse "
        "   from "
        "     (select pg_catalog.sum(snap_total_elapse_time) as elapse_time, pg_catalog.sum(snap_parse_time) "
        " as parse_time from snapshot.snap_summary_statement "
        "        where snapshot_id = %ld ) snap_1, "
        "     (select pg_catalog.sum(snap_total_elapse_time) as elapse_time, pg_catalog.sum(snap_parse_time) "
        " as parse_time from snapshot.snap_summary_statement "
        "        where snapshot_id = %ld ) snap_2 "
        "  ) s5; ",
        params->begin_snap_id,
        params->end_snap_id,
        params->begin_snap_id,
        params->end_snap_id,
        params->begin_snap_id,
        params->end_snap_id,
        params->begin_snap_id,
        params->end_snap_id,
        params->begin_snap_id,
        params->end_snap_id,
        params->begin_snap_id,
        params->end_snap_id,
        params->begin_snap_id,
        params->end_snap_id);

    GenReport::get_query_data(query.data, true, &query_result, &dash->type);
    dash->table = list_concat(dash->table, query_result);

    pfree(query.data);
}
#endif

/* summary report - instance efficiency information */
static void get_summary_instance_efficiency(report_params* params)
{
    /* supported report type: summary/all */
    /* supported report scope: cluster/node */
    if (!is_summary_report(params) && !is_full_report(params)) {
        return;
    }

    dashboard* dash = CreateDash();

    const char* desc = "Get instance efficiency percentages.";
    dash->dashTitle = "Summary";
    dash->tableTitle = "Instance Efficiency Percentages (Target 100%)";
    dash->desc = lappend(dash->desc, (void*)desc);

    /* instance efficiency, Buffer Hit %: */
    get_summary_instance_efficiency_percentages(params, dash);
    GenReport::add_data(dash, &params->Contents);
}

/* summary report - Top 10 Events by Total Wait Time */
static void get_summary_top10event_waitevent(report_params* params)
{
    /* supported report scope: node */
    if (!is_single_node_report(params)) {
        return;
    }

    /* supported report type: summary/all */
    if (!is_summary_report(params) && !is_full_report(params)) {
        return;
    }

    dashboard* dash = CreateDash();

    const char* desc = "Top 10 Events by Total Wait Time.";
    dash->dashTitle = "Summary";
    dash->tableTitle = "Top 10 Events by Total Wait Time";

    dash->desc = lappend(dash->desc, (void*)desc);

    /* wait event total time - Top 10 */
    if (update_report_snap_gap_param(params, "snap_global_wait_events")) {
        List* query_result = NIL;
        StringInfoData query;
        initStringInfo(&query);

        appendStringInfo(&query,
            "select snap_event as \"Event\", snap_wait as \"Waits\", "
            "       snap_total_wait_time as \"Total Wait Time(us)\", "
            "       pg_catalog.round(snap_total_wait_time/snap_wait) as \"Avg Wait Time(us)\", "
            "       snap_type as \"Type\" "
            "from ("
            "  select snap_2.snap_event as snap_event, snap_2.snap_type snap_type, "
            "         snap_2.snap_wait - snap_1.snap_wait as snap_wait, "
            "         snap_2.total_time - snap_1.total_time as snap_total_wait_time "
            "  from "
            "     (select snap_event, snap_wait, snap_total_wait_time as total_time, snap_type "
            "        from snapshot.snap_global_wait_events "
            "        where snapshot_id = %ld and snap_event != 'none' and snap_event != 'wait cmd' "
            "              and snap_event != 'unknown_lwlock_event' and snap_nodename = '%s') snap_1, "
            "     (select snap_event, snap_wait, snap_total_wait_time as total_time, snap_type "
            "        from snapshot.snap_global_wait_events "
            "        where snapshot_id = %ld and snap_event != 'none' and snap_event != 'wait cmd' "
            "              and snap_event != 'unknown_lwlock_event' and snap_nodename = '%s') snap_2 "
            "  where snap_2.snap_event = snap_1.snap_event "
            "  order by snap_total_wait_time desc limit 10) "
            "where snap_wait != 0",
            params->begin_snap_id,
            params->report_node,
            params->end_snap_id,
            params->report_node);

        GenReport::get_query_data(query.data, true, &query_result, &dash->type);
        dash->table = list_concat(dash->table, query_result);

        pfree(query.data);
    }
    GenReport::add_data(dash, &params->Contents);
}

/* summary - wait class by total wait time */
static void get_summary_wait_classes(report_params* params)
{
    /* supported report type: summary/all */
    /* supported report scope: node */
    if (!is_single_node_report(params)) {
        return;
    }
    if (!is_summary_report(params) && !is_full_report(params)) {
        return;
    }

    if (!get_report_node(params)) {
        return;
    }

    dashboard* dash = CreateDash();

    const char* desc = "show wait classes stat order by total wait time desc";
    dash->dashTitle = "Summary";
    dash->tableTitle = "Wait Classes by Total Wait Time";
    dash->desc = lappend(dash->desc, (void*)desc);

    StringInfoData query;
    initStringInfo(&query);

    appendStringInfo(&query,
        "select "
        "    snap_2.type as \"Type\", "
        "    (snap_2.wait - snap_1.wait) as \"Waits\", "
        "    (snap_2.total_wait_time - snap_1.total_wait_time) "
        "        as \"Total Wait Time(us)\", "
        "    pg_catalog.round((snap_2.total_wait_time - snap_1.total_wait_time) / "
        "        greatest((snap_2.wait - snap_1.wait), 1)) as \"Avg Wait Time(us)\" "
        "from "
        "    (select "
        "        snap_type as type, "
        "        pg_catalog.sum(snap_total_wait_time) as total_wait_time, "
        "        pg_catalog.sum(snap_wait) as wait from snapshot.snap_global_wait_events "
        "    where snapshot_id = %ld and  snap_nodename = '%s' and"
        " snap_event != 'unknown_lwlock_event' and snap_event != 'none' "
        "    group by snap_type) snap_2 "
        "    left join "
        "    (select "
        "        snap_type as type, "
        "        pg_catalog.sum(snap_total_wait_time) as total_wait_time, "
        "        pg_catalog.sum(snap_wait) as wait from snapshot.snap_global_wait_events "
        "    where snapshot_id = %ld and  snap_nodename = '%s' and"
        " snap_event != 'unknown_lwlock_event' and snap_event != 'none' "
        "    group by snap_type) snap_1 "
        "    on snap_2.type = snap_1.type "
        "    order by \"Total Wait Time(us)\" desc",
        params->end_snap_id, get_report_node(params), params->begin_snap_id, get_report_node(params));

    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    pfree(query.data);

    GenReport::add_data(dash, &params->Contents);
}

static void AppendQueryOne(StringInfoData& query)
{
    appendStringInfo(&query,
        "select "
        "    snap_2.cpus as \"CPUs\", "
        "    snap_2.cores as \"Cores\", "
        "    snap_2.sockets as \"Sockets\", "
        "    snap_1.load as \"Load Average Begin\", "
        "    snap_2.load as \"Load Average End\", "
        "    pg_catalog.round(coalesce((snap_2.user_time - snap_1.user_time), 0) / "
        "        greatest(coalesce((snap_2.total_time - snap_1.total_time), 0), 1)  * 100, 2) as \"%%User\", "
        "    pg_catalog.round(coalesce((snap_2.sys_time - snap_1.sys_time), 0) / "
        "        greatest(coalesce((snap_2.total_time - snap_1.total_time), 0), 1) * 100, 2) as \"%%System\", "
        "    pg_catalog.round(coalesce((snap_2.iowait_time - snap_1.iowait_time), 0) / "
        "        greatest(coalesce((snap_2.total_time - snap_1.total_time), 0), 1) * 100, 2) as \"%%WIO\", "
        "    pg_catalog.round(coalesce((snap_2.idle_time - snap_1.idle_time), 0) / "
        "        greatest(coalesce((snap_2.total_time - snap_1.total_time), 0), 1) * 100, 2) as \"%%Idle\" "
        "from ");
}

static void AppendQueryTwo(StringInfoData& query, report_params* params)
{
    appendStringInfo(&query,
        "    (select H.cpus, H.cores, H.sockets, H.idle_time, H.user_time, H.sys_time, H.iowait_time, "
        "    (H.idle_time + H.user_time + H.sys_time + H.iowait_time) AS total_time, H.load from "
        "    (select C.cpus, E.cores, T.sockets, I.idle_time, U.user_time, S.sys_time, W.iowait_time, L.load from "
        "    (select snap_value as cpus from snapshot.snap_global_os_runtime "
        "     where (snapshot_id = %ld and snap_node_name = '%s' and snap_name = 'NUM_CPUS')) AS C, "
        "    (select snap_value as cores from snapshot.snap_global_os_runtime "
        "     where (snapshot_id = %ld and snap_node_name = '%s' and snap_name = 'NUM_CPU_CORES')) AS E, "
        "    (select snap_value as sockets from snapshot.snap_global_os_runtime "
        "     where (snapshot_id = %ld and snap_node_name = '%s' and snap_name = 'NUM_CPU_SOCKETS')) AS T, "
        "    (select snap_value as idle_time from snapshot.snap_global_os_runtime "
        "     where (snapshot_id = %ld and snap_node_name = '%s' and snap_name = 'IDLE_TIME')) AS I, "
        "    (select snap_value as user_time from snapshot.snap_global_os_runtime "
        "     where (snapshot_id = %ld and snap_node_name = '%s' and snap_name = 'USER_TIME')) AS U, "
        "    (select snap_value as sys_time from snapshot.snap_global_os_runtime "
        "     where (snapshot_id = %ld and snap_node_name = '%s' and snap_name = 'SYS_TIME')) AS S, "
        "    (select snap_value as iowait_time from snapshot.snap_global_os_runtime "
        "     where (snapshot_id = %ld and snap_node_name = '%s' and snap_name = 'IOWAIT_TIME')) AS W, "
        "    (select snap_value as load from snapshot.snap_global_os_runtime "
        "     where (snapshot_id = %ld and snap_node_name = '%s' and snap_name = 'LOAD')) AS L ) as H ) as snap_2, "
        "    (select H.cpus, H.cores, H.sockets, H.idle_time, H.user_time, H.sys_time, H.iowait_time, "
        "    (H.idle_time + H.user_time + H.sys_time + H.iowait_time) AS total_time, H.load from "
        "    (select C.cpus, E.cores, T.sockets, I.idle_time, U.user_time, S.sys_time, W.iowait_time, L.load from "
        "    (select snap_value as cpus from snapshot.snap_global_os_runtime "
        "     where (snapshot_id = %ld and snap_node_name = '%s' and snap_name = 'NUM_CPUS')) AS C, "
        "    (select snap_value as cores from snapshot.snap_global_os_runtime "
        "     where (snapshot_id = %ld and snap_node_name = '%s' and snap_name = 'NUM_CPU_CORES')) AS E, "
        "    (select snap_value as sockets from snapshot.snap_global_os_runtime "
        "     where (snapshot_id = %ld and snap_node_name = '%s' and snap_name = 'NUM_CPU_SOCKETS')) AS T, "
        "    (select snap_value as idle_time from snapshot.snap_global_os_runtime "
        "     where (snapshot_id = %ld and snap_node_name = '%s' and snap_name = 'IDLE_TIME')) AS I, "
        "    (select snap_value as user_time from snapshot.snap_global_os_runtime "
        "     where (snapshot_id = %ld and snap_node_name = '%s' and snap_name = 'USER_TIME')) AS U, "
        "    (select snap_value as sys_time from snapshot.snap_global_os_runtime "
        "     where (snapshot_id = %ld and snap_node_name = '%s' and snap_name = 'SYS_TIME')) AS S, "
        "    (select snap_value as iowait_time from snapshot.snap_global_os_runtime "
        "     where (snapshot_id = %ld and snap_node_name = '%s' and snap_name = 'IOWAIT_TIME')) AS W, "
        "    (select snap_value as load from snapshot.snap_global_os_runtime "
        "     where (snapshot_id = %ld and snap_node_name = '%s' and snap_name = 'LOAD')) AS L ) as H ) as snap_1 ",
        params->end_snap_id,
        get_report_node(params),
        params->end_snap_id,
        get_report_node(params),
        params->end_snap_id,
        get_report_node(params),
        params->end_snap_id,
        get_report_node(params),
        params->end_snap_id,
        get_report_node(params),
        params->end_snap_id,
        get_report_node(params),
        params->end_snap_id,
        get_report_node(params),
        params->end_snap_id,
        get_report_node(params),
        params->begin_snap_id,
        get_report_node(params),
        params->begin_snap_id,
        get_report_node(params),
        params->begin_snap_id,
        get_report_node(params),
        params->begin_snap_id,
        get_report_node(params),
        params->begin_snap_id,
        get_report_node(params),
        params->begin_snap_id,
        get_report_node(params),
        params->begin_snap_id,
        get_report_node(params),
        params->begin_snap_id,
        get_report_node(params));
}

/* summary -host cpu */
static void get_summary_host_cpu(report_params* params)
{
    /* supported report type: summary/all */
    /* supported report scope: node */
    if (!is_single_node_report(params)) {
        return;
    }
    if (!is_summary_report(params) && !is_full_report(params)) {
        return;
    }

    if (!get_report_node(params)) {
        return;
    }

    dashboard* dash = CreateDash();

    const char* desc = "show the node host cpu";
    dash->dashTitle = "Summary";
    dash->tableTitle = "Host CPU";
    dash->desc = lappend(dash->desc, (void*)desc);

    StringInfoData query;
    initStringInfo(&query);
    AppendQueryOne(query);
    AppendQueryTwo(query, params);
    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    pfree(query.data);

    GenReport::add_data(dash, &params->Contents);
}

/* summary -node io profile */
static void get_summary_node_file_iostat(report_params* params, dashboard* dash)
{
    StringInfoData query;
    List* query_result = NIL;

    initStringInfo(&query);
    appendStringInfo(&query,
        "select "
        "    pg_catalog.unnest(array['Database requests', 'Database (MB)', 'Database (blocks)']) as \"%s\", "
        "    pg_catalog.round(pg_catalog.unnest(array[phytotal, (phyblktotal * %d) >> 20, phyblktotal]) / %ld) "
        "    as \"%s\", "
        "    pg_catalog.round(pg_catalog.unnest(array[phyrds, (phyblkrd * %d) >> 20, phyblkrd]) / %ld) as \"%s\", "
        "    pg_catalog.round(pg_catalog.unnest(array[phywrts, (phyblkwrt * %d) >> 20, phyblkwrt]) / %ld) as \"%s\" "
        "from "
        "    (select "
        "        (snap_2.phytotal - snap_1.phytotal) as phytotal, "
        "        (snap_2.phyblktotal - snap_1.phyblktotal) as phyblktotal, "
        "        (snap_2.phyrds - snap_1.phyrds) as phyrds, "
        "        (snap_2.phyblkrd - snap_1.phyblkrd) as phyblkrd, "
        "        (snap_2.phywrts - snap_1.phywrts) as phywrts, "
        "        (snap_2.phyblkwrt - snap_1.phyblkwrt) as phyblkwrt "
        "    from "
        "        (select (snap_phyrds + snap_phywrts) as phytotal, "
        "            (snap_phyblkwrt + snap_phyblkrd) as phyblktotal, "
        "            snap_phyrds as phyrds, snap_phyblkrd as phyblkrd, "
        "            snap_phywrts as phywrts, snap_phyblkwrt as phyblkwrt "
        "            from snapshot.snap_global_rel_iostat "
        "            where snapshot_id = %ld and snap_node_name = '%s') snap_1, "
        "        (select (snap_phyrds + snap_phywrts) as phytotal, "
        "            (snap_phyblkwrt + snap_phyblkrd) as phyblktotal, "
        "            snap_phyrds as phyrds, snap_phyblkrd as phyblkrd, "
        "            snap_phywrts as phywrts, snap_phyblkwrt as phyblkwrt "
        "            from snapshot.snap_global_rel_iostat "
        "            where snapshot_id = %ld and snap_node_name = '%s') snap_2) snap_diff",
        g_metric_name,
        BLCKSZ,
        get_report_snap_gap(params),
        g_per_sec_io_rw,
        BLCKSZ,
        get_report_snap_gap(params),
        g_per_sec_io_r,
        BLCKSZ,
        get_report_snap_gap(params),
        g_per_sec_io_w,
        params->begin_snap_id,
        get_report_node(params),
        params->end_snap_id,
        get_report_node(params));

    GenReport::get_query_data(query.data, !list_length(dash->table), &query_result, &dash->type);
    dash->table = list_concat(dash->table, query_result);

    pfree(query.data);
}

static void get_summary_node_redo_iostat(report_params* params, dashboard* dash)
{
    StringInfoData query;
    List* query_result = NIL;

    initStringInfo(&query);
    appendStringInfo(&query,
        "select "
        "    pg_catalog.unnest(array['Redo requests', 'Redo (MB)']) as \"%s\", "
        "    pg_catalog.unnest(array[' ', ' ']) as \"%s\", "
        "    pg_catalog.unnest(array[' ', ' ']) as \"%s\", "
        "    pg_catalog.round(pg_catalog.unnest(array[phywrts, (phyblkwrt * %d) >> 20]) / %ld) as \"%s\" "
        "from "
        "    (select "
        "        (snap_2.phywrts - snap_1.phywrts) as phywrts, "
        "        (snap_2.phyblkwrt - snap_1.phyblkwrt) as phyblkwrt "
        "    from "
        "        (select pg_catalog.sum(snap_phywrts) as phywrts, pg_catalog.sum(snap_phyblkwrt) as phyblkwrt "
        "            from snapshot.snap_global_file_redo_iostat "
        "            where snapshot_id = %ld and snap_node_name = '%s') snap_1, "
        "        (select pg_catalog.sum(snap_phywrts) as phywrts, pg_catalog.sum(snap_phyblkwrt) as phyblkwrt "
        "            from snapshot.snap_global_file_redo_iostat "
        "            where snapshot_id = %ld and snap_node_name = '%s') snap_2) snap_diff",
        g_metric_name,
        g_per_sec_io_rw,
        g_per_sec_io_r,
        BLCKSZ,
        get_report_snap_gap(params),
        g_per_sec_io_w,
        params->begin_snap_id,
        get_report_node(params),
        params->end_snap_id,
        get_report_node(params));

    GenReport::get_query_data(query.data, !list_length(dash->table), &query_result, &dash->type);
    dash->table = list_concat(dash->table, query_result);

    pfree(query.data);
}

/* summary report - io profile information */
static void get_summary_node_io_profile(report_params* params)
{
    if (!get_report_node(params)) {
        return;
    }

    dashboard* dash = CreateDash();

    const char* desc = "show the node io profile";
    dash->dashTitle = "Summary";
    dash->tableTitle = "IO Profile";
    dash->desc = lappend(dash->desc, (void*)desc);

    if (update_report_snap_gap_param(params, "snap_global_rel_iostat")) {
        /* load io profile - node file iostat */
        get_summary_node_file_iostat(params, dash);
    }
    if (update_report_snap_gap_param(params, "snap_global_file_redo_iostat")) {
        /* load io profile - node file redo iostat */
        get_summary_node_redo_iostat(params, dash);
    }
    GenReport::add_data(dash, &params->Contents);
}

/* summary -cluster io profile */
static void get_summary_cluster_file_iostat(report_params* params, dashboard* dash)
{
    StringInfoData query;
    List* query_result = NIL;

    initStringInfo(&query);
    appendStringInfo(&query,
        "select "
        "    pg_catalog.unnest(array['Database requests', 'Database (MB)', 'Database (blocks)']) as \"%s\", "
        "    pg_catalog.round(pg_catalog.unnest(array[phytotal, (phyblktotal * %d) >> 20, phyblktotal]) / %ld) "
        "    as \"%s\", "
        "    pg_catalog.round(pg_catalog.unnest(array[phyrds, (phyblkrd * %d) >> 20, phyblkrd]) / %ld) as \"%s\", "
        "    pg_catalog.round(pg_catalog.unnest(array[phywrts, (phyblkwrt * %d) >> 20, phyblkwrt]) / %ld) as \"%s\" "
        "from "
        "    (select "
        "        (snap_2.phytotal - snap_1.phytotal) as phytotal, "
        "        (snap_2.phyblktotal - snap_1.phyblktotal) as phyblktotal, "
        "        (snap_2.phyrds - snap_1.phyrds) as phyrds, "
        "        (snap_2.phyblkrd - snap_1.phyblkrd) as phyblkrd, "
        "        (snap_2.phywrts - snap_1.phywrts) as phywrts, "
        "        (snap_2.phyblkwrt - snap_1.phyblkwrt) as phyblkwrt "
        "    from "
        "        (select (snap_phyrds + snap_phywrts) as phytotal, "
        "            (snap_phyblkwrt + snap_phyblkrd) as phyblktotal, "
        "            snap_phyrds as phyrds, snap_phyblkrd as phyblkrd, "
        "            snap_phywrts as phywrts, snap_phyblkwrt as phyblkwrt "
        "            from snapshot.snap_summary_rel_iostat "
        "            where snapshot_id = %ld) snap_1, "
        "        (select (snap_phyrds + snap_phywrts) as phytotal, "
        "            (snap_phyblkwrt + snap_phyblkrd) as phyblktotal, "
        "            snap_phyrds as phyrds, snap_phyblkrd as phyblkrd, "
        "            snap_phywrts as phywrts, snap_phyblkwrt as phyblkwrt "
        "            from snapshot.snap_summary_rel_iostat "
        "            where snapshot_id = %ld) snap_2) snap_diff",
        g_metric_name,
        BLCKSZ,
        get_report_snap_gap(params),
        g_per_sec_io_rw,
        BLCKSZ,
        get_report_snap_gap(params),
        g_per_sec_io_r,
        BLCKSZ,
        get_report_snap_gap(params),
        g_per_sec_io_w,
        params->begin_snap_id,
        params->end_snap_id);

    GenReport::get_query_data(query.data, !list_length(dash->table), &query_result, &dash->type);
    dash->table = list_concat(dash->table, query_result);

    pfree(query.data);
}

static void get_summary_cluster_redo_iostat(report_params* params, dashboard* dash)
{
    StringInfoData query;
    List* query_result = NIL;

    initStringInfo(&query);
    appendStringInfo(&query,
        "select "
        "    pg_catalog.unnest(array['Redo requests', 'Redo (MB)']) as \"%s\", "
        "    pg_catalog.unnest(array[' ',' ']) as \"%s\", "
        "    pg_catalog.unnest(array[' ',' ']) as \"%s\", "
        "    pg_catalog.round(pg_catalog.unnest(array[phywrts, (phyblkwrt * %d) >> 20]) / %ld) as \"%s\" "
        "from "
        "    (select "
        "        (snap_2.phywrts - snap_1.phywrts) as phywrts, "
        "        (snap_2.phyblkwrt - snap_1.phyblkwrt) as phyblkwrt "
        "    from "
        "        (select pg_catalog.sum(snap_phywrts) as phywrts, pg_catalog.sum(snap_phyblkwrt) as phyblkwrt "
        "            from snapshot.snap_summary_file_redo_iostat "
        "            where snapshot_id = %ld) snap_1, "
        "        (select pg_catalog.sum(snap_phywrts) as phywrts, pg_catalog.sum(snap_phyblkwrt) as phyblkwrt "
        "            from snapshot.snap_summary_file_redo_iostat "
        "            where snapshot_id = %ld) snap_2) snap_diff",
        g_metric_name,
        g_per_sec_io_rw,
        g_per_sec_io_r,
        BLCKSZ,
        get_report_snap_gap(params),
        g_per_sec_io_w,
        params->begin_snap_id,
        params->end_snap_id);

    GenReport::get_query_data(query.data, !list_length(dash->table), &query_result, &dash->type);
    dash->table = list_concat(dash->table, query_result);

    pfree(query.data);
}

/* summary report - io profile information */
static void get_summary_cluster_io_profile(report_params* params)
{
    dashboard* dash = CreateDash();

    const char* desc = "show io profile";
    dash->dashTitle = "Summary";
    dash->tableTitle = "IO Profile";
    dash->desc = lappend(dash->desc, (void*)desc);

    if (update_report_snap_gap_param(params, "snap_summary_rel_iostat")) {
        /* load io profile - file iostat */
        get_summary_cluster_file_iostat(params, dash);
    }
    if (update_report_snap_gap_param(params, "snap_summary_file_redo_iostat")) {
        /* load io profile - file redo iostat */
        get_summary_cluster_redo_iostat(params, dash);
    }
    GenReport::add_data(dash, &params->Contents);
}

static void get_summary_io_profile(report_params* params)
{
    /* supported report type: summary/all */
    /* supported report scope: node */
    if (!is_summary_report(params) && !is_full_report(params)) {
        return;
    }
    if (is_cluster_report(params)) {
        get_summary_cluster_io_profile(params);
    } else if (is_single_node_report(params)) {
        get_summary_node_io_profile(params);
    }
}

/* summary -memmory statistics */
static void get_summary_memory_stat(report_params* params)
{
    /* supported report type: summary/all */
    /* supported report scope: node */
    if (!is_single_node_report(params)) {
        return;
    }
    if (!is_summary_report(params) && !is_full_report(params)) {
        return;
    }

    if (!get_report_node(params)) {
        return;
    }

    dashboard* dash = CreateDash();

    const char* desc = "show the node memory statistics";
    dash->dashTitle = "Summary";
    dash->tableTitle = "Memory Statistics";
    dash->desc = lappend(dash->desc, (void*)desc);

    StringInfoData query;
    initStringInfo(&query);

    appendStringInfo(&query,
        "select "
        "    snap_2.snap_memorytype as \"Memory Type\", snap_1.snap_memorymbytes as \"Begin(MB)\", "
        "    snap_2.snap_memorymbytes as \"End(MB)\" from "
        "    (select snap_memorytype, snap_memorymbytes from snapshot.snap_global_memory_node_detail "
        "    where (snapshot_id = %ld and snap_nodename = '%s') and "
        "    (snap_memorytype = 'max_process_memory' or snap_memorytype = 'process_used_memory' or "
        "     snap_memorytype = 'max_shared_memory' or  snap_memorytype = 'shared_used_memory'))"
        "    as snap_2  left join  (select snap_memorytype, snap_memorymbytes "
        "    from snapshot.snap_global_memory_node_detail  where (snapshot_id = %ld and snap_nodename = '%s') and "
        "    (snap_memorytype = 'max_process_memory' or  snap_memorytype = 'process_used_memory' or "
        "     snap_memorytype = 'max_shared_memory' or snap_memorytype = 'shared_used_memory'))"
        "    as snap_1 on snap_2.snap_memorytype = snap_1.snap_memorytype",
        params->end_snap_id,
        get_report_node(params),
        params->begin_snap_id,
        get_report_node(params));

    GenReport::get_query_data(query.data, true, &dash->table, &dash->type);
    pfree(query.data);

    GenReport::add_data(dash, &params->Contents);
}

/*
 * some cases aren't allowed to generate the awr report:
 * 1. database statistics has been reset between start and end
 * 2. some nodes restarted with some resones
 * 3. some node occurs error which produces ha operation
 * 4. database is dropped during the snap gaps
 */
static bool IsSnapStatNormal(report_params* params)
{
    bool isNull = false;
    StringInfoData query;
    initStringInfo(&query);

    /* using global_node reset time */
    appendStringInfo(&query,
        "select pg_catalog.MAX(EXTRACT(EPOCH FROM (t.snap_1_time- t.snap_2_time)))::int8  from ("
        "SELECT snap_1.snap_reset_time as snap_1_time,"
        "case  when snap_2.snap_reset_time is null then '1000-01-01 00:00:00.695729+08' "
        "else snap_2.snap_reset_time end as snap_2_time  FROM ("
        "("
        "(SELECT * FROM snapshot.snap_global_record_reset_time WHERE snapshot_id = %ld) AS snap_1 left join"
        "(SELECT * from snapshot.snap_global_record_reset_time WHERE snapshot_id = %ld) AS snap_2 "
        "on snap_2.snap_node_name = snap_1.snap_node_name )"
        ") union"
        "( ("
        "SELECT snap_1.snap_reset_time as snap_1_time,"
        "case  when snap_2.snap_reset_time is null then '1000-01-01 00:00:00.695729+08' "
        "else snap_2.snap_reset_time end as snap_2_time  FROM ("
        "(SELECT * FROM snapshot.snap_global_record_reset_time WHERE snapshot_id = %ld) AS snap_1 left join"
        "(SELECT * from snapshot.snap_global_record_reset_time WHERE snapshot_id = %ld) AS snap_2 "
        "on snap_2.snap_node_name = snap_1.snap_node_name )"
        ")))t;",
        params->begin_snap_id,
        params->end_snap_id,
        params->end_snap_id,
        params->begin_snap_id);

    Datum datum = GetDatumValue(query.data, 0, 0, &isNull);
    pfree(query.data);
    if (SPI_processed > 0) {
        if (isNull || DatumGetInt32(datum)) {
            return false;
        } else {
            return true;
        }
    }
    return false;
}

/*
 * check the valid of snap oid by the timestamp stored in the snapshot table
 */
static bool IsSnapIdValid(report_params* params)
{
    bool isNull = false;
    StringInfoData query;
    initStringInfo(&query);

    /* using global_node reset time */
    appendStringInfo(&query,
        "SELECT pg_catalog.MAX(EXTRACT(EPOCH FROM (snap_2.start_ts - snap_1.start_ts)))::int8 FROM "
        "(SELECT * FROM snapshot.snapshot where snapshot_id = %ld) AS snap_1, "
        "(SELECT * from snapshot.snapshot where snapshot_id = %ld) AS snap_2",
        params->begin_snap_id,
        params->end_snap_id);

    Datum datum = GetDatumValue(query.data, 0, 0, &isNull);
    pfree(query.data);
    if (SPI_processed > 0) {
        if (isNull || (DatumGetInt32(datum) <= 0)) {
            return false;
        } else {
            return true;
        }
    }
    return false;
}

/* check if the node name is valid */
static bool IsNodeNameValid(const report_params* params)
{
    if (params->report_node == NULL) {
        return false;
    }

#ifndef ENABLE_MULTIPLE_NODES
    const char *currentNodeName = NULL;
    if (!g_instance.attr.attr_common.PGXCNodeName || g_instance.attr.attr_common.PGXCNodeName[0] == '\0') {
        currentNodeName = "unknown name";
    } else {
        currentNodeName = g_instance.attr.attr_common.PGXCNodeName;
    }

    int curLen = strlen(currentNodeName);
    int reportLen = strlen(params->report_node);
    int compareLen = curLen > reportLen ? curLen : reportLen;
    if (strncmp(params->report_node, currentNodeName, compareLen) != 0) {
        return false;
    }
#else
    if (!OidIsValid(get_pgxc_nodeoid(params->report_node))) {
        return false;
    }
#endif

    return true;
}

/*
 * check generate_wdr_report proc's parameters
 */
static void check_report_parameter(report_params* params)
{
    if (params == NULL) {
        return;
    }

    if (!params->begin_snap_id || !params->end_snap_id) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("set the snapshotid")));
    }
    if (!IsSnapIdValid(params)) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("snapshot id is invalid")));
    }
    if (!(IsSnapStatNormal(params))) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("Instance reset time is different")));
    }

    // check report type/scope and node
    if ((strncmp(params->report_type, g_summaryType, strlen(g_summaryType)) != 0) &&
        (strncmp(params->report_type, g_detailType, strlen(g_detailType)) != 0) &&
        (strncmp(params->report_type, g_allType, strlen(g_allType)) != 0)) {
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
                errmsg("invalid report type, should be %s or %s or %s", g_summaryType, g_detailType, g_allType)));
    }

    if ((strncmp(params->report_scope, g_clusterScope, strlen(g_clusterScope)) != 0) &&
        (strncmp(params->report_scope, g_nodeScope, strlen(g_nodeScope)) != 0)) {
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
                errmsg("invalid report scope, should be %s or %s", g_clusterScope, g_nodeScope)));
    }

    if ((strncmp(params->report_scope, g_nodeScope, strlen(g_nodeScope)) == 0) && !IsNodeNameValid(params)) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("invalid report node name.")));
    }
}

/*
 * Used to execute the query and get the report data and store it in structured memory
 * Input parameters:
 * params - report parameters
 */
void GenReport::get_report_data(report_params* params)
{
    if (params == NULL) {
        return;
    }

    // check parameter is valid or not
    check_report_parameter(params);

    GenReport::get_snapinfo_data(params);

    /* --------------- SUMMARY REPORT AREA--------------------- */
    /* Summery information must be placed in front */
    /* summary - Database Stat */
    GenReport::get_summary_database_stat(params);

    /* summary - Load Profile */
    GenReport::get_summary_load_profile(params);

    /* summary - Instance Efficiency Percentages */
    get_summary_instance_efficiency(params);

    /* summary - Top 10 Events by Total Wait Time */
    get_summary_top10event_waitevent(params);

    /* summary - Wait Classes by Total Wait Time */
    get_summary_wait_classes(params);

    /* summary - Host CPU */
    get_summary_host_cpu(params);

    /* summary - IO Profile */
    get_summary_io_profile(params);

    /* summary - Memory Statistics */
    get_summary_memory_stat(params);

    /* --------------- DETAIL REPORT AREA--------------------- */
    /* detail - Time Model */
    GenReport::GetTimeModelData(params);

    /* detail - sql statistics */
    GenReport::GetSqlStatisticsData(params);

    /* detail - wait event */
    GenReport::GetWaitEventsData(params);

    /* detail - cache IO stats */
    GetCacheIOData(params);

    /* detail - utility status */
    GetUtilityStatus(params);

    /* detail - object stats */
    GetObjectStatData(params);

    /* detail - configure settings */
    GetConfigSettings(params);

    /* detail - sql detail */
    GetSqlDetailData(params);
}

static void set_nestloop_param(const char* nestloop_sql)
{
    int rc;
    if ((rc = SPI_execute(nestloop_sql, false, 0)) != SPI_OK_UTILITY) {
        ereport(ERROR,
            (errmodule(MOD_WDR_SNAPSHOT),
            errcode(ERRCODE_INTERNAL_ERROR),
            errmsg("set enable_nestloop failed: %s", nestloop_sql),
            errdetail("error code: %s", SPI_result_code_string(rc)),
            errcause("System error."),
            erraction("Contact engineer to support.")));
    }
}

static bool get_nestloop_param()
{
    const char* query = "show enable_nestloop";

    if (SPI_execute(query, false, 0) != SPI_OK_UTILITY) {
        ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("invalid query")));
    }

    bool isnull = false;
    Datum colval = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, (int)1, &isnull);
    if (isnull) {
        return false;
    }
    Oid* type = (Oid*)palloc(sizeof(Oid));
    *type = SPI_gettypeid(SPI_tuptable->tupdesc, (int)1);
    char* originStr = Datum_to_string(colval, *type, isnull);
    bool isNestloopOn = strcmp(originStr, "on") == 0;
    pfree(type);
    pfree(originStr);
    return isNestloopOn;
}

/*
 * generate analysis reports
 * Input parameters:
 *   baseid -- The first snapshot id
 *   lastid -- The second snapshot id
 *   report_type -- report type: summary, detail, summary + detail
 *   report_scope -- for cluster or single node
 *   node_name -- for single node mode: means node_name
 */
Datum generate_wdr_report(PG_FUNCTION_ARGS)
{
    /* before generate_report, need check snapshot repository existed or not */
    int rc = 0;
    int index = 0;
    report_params params = {0};
    params.begin_snap_id = PG_GETARG_INT64(index);
    index++;
    params.end_snap_id = PG_GETARG_INT64(index);

    // summary/detail/summary + detail
    index++;
    params.report_type = PG_GETARG_CSTRING(index);

    // report scope - cluster or single node
    index++;
    params.report_scope = PG_GETARG_CSTRING(index);

    // node_name - only for single node mode
    index++;

    if ((params.report_type == NULL) || (params.report_scope == NULL)) {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("The 3rd argument 'report_type' and 4th argument 'report_scope' should not be null")));
    }

    if (strncmp(params.report_scope, g_nodeScope, strlen(g_nodeScope)) == 0) {
        params.report_node = PG_GETARG_CSTRING(index);
    }

    if (!isMonitoradmin(GetUserId())) {
        ereport(ERROR, (errcode(ERRCODE_OPERATE_FAILED),
            errmsg("Monitor admin privilege is neended to generate WDR report")));
    }
    ereport(LOG, (errmsg("generate WDR report start")));
    ereport(LOG, (errmsg("begin_snapshot_id:%ld, end_snapshot_id:%ld, report_type:%s, report_scope:%s, report_node:%s",
        params.begin_snap_id, params.end_snap_id, params.report_type, params.report_scope,
        (params.report_node == NULL) ? "NULL" : params.report_node)));
    MemoryContext old_context = CurrentMemoryContext;
    SPI_STACK_LOG("connect", NULL, NULL);
    if ((rc = SPI_connect()) != SPI_OK_CONNECT) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("SPI_connect failed: %s", SPI_result_code_string(rc))));
    }

    bool isNestloopOn = get_nestloop_param();
    if (isNestloopOn) {
        set_nestloop_param("set enable_nestloop = off");
    }

    GenReport::get_report_data(&params);
    MemoryContext spi_context = MemoryContextSwitchTo(old_context);
    char* result_str = GenReport::GenerateHtmlReport(&params);
    (void)MemoryContextSwitchTo(spi_context);

    if (isNestloopOn) {
        set_nestloop_param("set enable_nestloop = on");
    }
    SPI_STACK_LOG("finish", NULL, NULL);
    (void)SPI_finish();
    ereport(LOG, (errmsg("generate WDR report end")));
    text* textStr = cstring_to_text(result_str);
    pfree(result_str);
    PG_RETURN_TEXT_P(textStr);
}
