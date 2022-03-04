# Index_advisor
**Index_advisor** is a tool to recommend indexes for workload. A workload consists
of a set of SQL data manipulation statements, i.e., Select, Insert, Delete and Update.
First, some candidate indexes are generated based on query syntax and database
statistics. Then the optimal index set is determined by estimating the cost and
benefit of it for the workload.

## Usage

    python index_advisor_workload.py [p PORT] [d DATABASE] [f FILE] [--h HOST] [-U USERNAME] 
    [-W PASSWORD] [--schema SCHEMA] [--max_index_num MAX_INDEX_NUM] [--max_index_storage MAX_INDEX_STORAGE] 
    [--multi_iter_mode] [--multi_node] [--json] [--driver] [--show_detail]

# Extract_log

**extract_log**  is a tool for extracting business data from pg_log.

## Usage

    python extract_log.py [l LOG_DIRECTORY] [f OUTPUT_FILE] [-d DATABASE] [-U USERNAME] [--start_time]
    [--sql_amount] [--statement] [--json]

## Dependencies

    python3.x
