# Index_advisor
**Index_advisor** is a tool to recommend indexes for workload. A workload consists
of a set of SQL data manipulation statements, i.e., Select, Insert, Delete and Update.
First, some candidate indexes are generated based on query syntax and database
statistics. Then the optimal index set is determined by estimating the cost and
benefit of it for the workload.

## Dependencies

    python3.x

## Usage

    python index_advisor_workload.py [p PORT] [d DATABASE] [f FILE] [--h HOST] [-U USERNAME] [-W PASSWORD][--schema SCHEMA]
    [--max_index_num MAX_INDEX_NUM][--max_index_storage MAX_INDEX_STORAGE] [--multi_iter_mode] [--multi_node]

