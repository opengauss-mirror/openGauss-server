# SQL Rewriter
## Description:
**SQL Rewriter** is a tool to transform a relational database query into an
equivalent but more efficient one, which is crucial for the performance
of database-backed applications. Such rewriting relies on
pre-specified rewrite rules.
## Usage:
    gs_dbmind component sql_rewriter --help
    usage: [-h] [--db-host DB_HOST] [--db-user DB_USER] db_port database file

    SQL Rewriter

    positional arguments:
      db_port            Port for database
      database           Name for database
      file               File containing SQL statements which need to rewrite

    optional arguments:
      -h, --help         show this help message and exit
      --db-host DB_HOST  Host for database
      --db-user DB_USER  Username for database log-in
      --schema SCHEMA    Schema name for the current business data

