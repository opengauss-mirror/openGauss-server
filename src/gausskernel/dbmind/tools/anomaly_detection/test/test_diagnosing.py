import logging
import os
import sqlite3
import sys
import time
import unittest
from datetime import timedelta

sys.path.append("../")

from utils import TimeString, convert_to_mb, unify_sql, extract_table_from_sql, wdr_sql_processing, input_sql_processing
from detector.tools.slow_sql.diagnosing import diagnose_auto
from detector.tools.slow_sql.diagnosing import diagnose_user
from detector.service.storage import sqlite_storage
from detector.service.storage.sqlite_storage import SQLiteStorage
from detector.algorithm.auto_arima import AutoArima

logging.basicConfig(level=logging.INFO)


class TestDataHandler(unittest.TestCase):
    def test_10_sqlite3(self):
        database_dir = os.path.realpath("./data")
        if not os.path.exists(database_dir):
            os.mkdir(database_dir)
        database_path = os.path.join(database_dir, '127_0_0_1_8000')
        conn = SQLiteStorage(database_path)
        conn.connect()
        try:
            conn.execute("select count(*) from wdr")
            conn.execute("DROP table database_exporter")
            conn.execute("DROP table OSExporter")
            conn.execute("DROP table wdr")
            logging.info('create tables')
            conn.execute("create table  database_exporter(timestamp bigint, guc_parameter text, "
                         "connrent_connections int, qps text, process text, temp_file text)")
            conn.execute("create table  os_exporter(timestamp bigint, cpu_usage text, io_wait text,"
                         " io_read text, io_write text, memory_usage text, disk_space text)")
            conn.execute("create table  wdr(timestamp bigint, db_name text, table_name text, query text,"
                         " explain text, start_time text, finish_time text, indexes text)")
        except sqlite3.OperationalError:
            logging.info('create tables')
            conn.execute("create table  database_exporter(timestamp bigint, guc_parameter text, "
                         "connrent_connections int, qps text, process text, temp_file text)")
            conn.execute("create table  os_exporter(timestamp bigint, cpu_usage text, io_wait text,"
                         " io_read text, io_write text, memory_usage text, disk_space text)")
            conn.execute("create table  wdr(timestamp bigint, db_name text, table_name text, query text,"
                         " explain text, start_time text, finish_time text, indexes text)")

        self.assertIsNotNone(conn)

    def test_11_full_scan(self):
        conn = SQLiteStorage(os.path.realpath("./data/127_0_0_1_8000"))
        conn.connect()
        query = "select * from bmsql_item"
        if conn.execute("SELECT * from wdr where timestamp == 1617885101"):
            logging.info("EXIST timestamp == 1617885101 ")
            conn.execute('DELETE FROM wdr WHERE timestamp == 1617885101')

        conn.execute("insert into wdr values ({timestamp}, \"{db_name}\", \"{table_name}\", \"{query}\","
                     " \"{explain}\", \"{start_time}\", \"{finish_time}\", \"{indexes}\")"
                     .format(timestamp=1617885101, db_name='tpcc', table_name='bmsql_item',
                             query="select * from bmsql_item",
                             explain="Aggregate  (cost=2559.00..2559.01 rows=1 width=8)\n   "
                                     "->  Seq Scan on bmsql_item  (cost=0.00..2309.00 rows=100000 width=6)",
                             start_time="2021-04-08 20:31:41",
                             finish_time="2021-04-08 20:31:44",
                             indexes="{'bmsql_order_line': {'bmsql_order_line_pkey': 'CREATE UNIQUE INDEX "
                                     "bmsql_order_line_pkey ON bmsql_order_line USING btree (ol_w_id, ol_d_id,"
                                     " ol_o_id, ol_number) TABLESPACE pg_default'}}"))
        conn.execute("insert into database_exporter values ({timestamp}, \"{guc_parameter}\", "
                     "{connrent_connections}, {qps}, \"{process}\", \"{temp_file}\")"
                     .format(timestamp=1617885101, guc_parameter='128.0,40.0078125,200', connrent_connections=18,
                             qps=1,
                             process="{'java': '14.2:0.2', 'containerd': '0.4:0.0', 'python': '0.3:0.0', "
                                     "'/usr/lib/systemd/systemd': '0.1:0.0', '/usr/sbin/irqbalance': '0.1:0.0'}",
                             temp_file='t'))
        rca = diagnose_user(conn, query, start_time="2021-04-08 20:31:41")
        logging.info('RCA:%s', rca)
        self.assertEqual(rca, ["2021-04-08 20:31:41", "2021-04-08 20:31:44",
                               ['FullScan: Select all columns from the table, which causes the full scan',
                                'Please stop this operation if it is not necessary']])

    def test_12_fuzzy_query(self):
        conn = SQLiteStorage(os.path.realpath("./data/127_0_0_1_8000"))
        conn.connect()
        query = "select i_id from bmsql_item where i_id like 100"
        if conn.execute("SELECT * from wdr where timestamp == 1617885105"):
            logging.info("EXIST timestamp == 1617885105 ")
            conn.execute('DELETE FROM wdr WHERE timestamp == 1617885105')

        conn.execute("insert into wdr values ({timestamp}, \"{db_name}\", \"{table_name}\", \"{query}\","
                     " \"{explain}\", \"{start_time}\", \"{finish_time}\", \"{indexes}\")"
                     .format(timestamp=1617885105, db_name='tpcc', table_name='bmsql_item',
                             query="select i_id from bmsql_item where i_id like 100",
                             explain="Seq Scan on bmsql_item  (cost=0.00..2809.00 rows=500 width=4)\n"
                                     "   Filter: ((i_id)::text ~~ '100'::text)",
                             start_time="2021-04-08 20:31:45",
                             finish_time="2021-04-08 20:31:49",
                             indexes="{'bmsql_order_line': {'bmsql_order_line_pkey': 'CREATE UNIQUE INDEX "
                                     "bmsql_order_line_pkey ON bmsql_order_line USING btree (ol_w_id, ol_d_id,"
                                     " ol_o_id, ol_number) TABLESPACE pg_default'}}"))
        conn.execute("insert into database_exporter values ({timestamp}, \"{guc_parameter}\", "
                     "{connrent_connections}, {qps}, \"{process}\", \"{temp_file}\")"
                     .format(timestamp=1617885105, guc_parameter='128.0,40.0078125,200', connrent_connections=18,
                             qps=1,
                             process="{'java': '14.2:0.2', 'containerd': '0.4:0.0', 'python': '0.3:0.0', "
                                     "'/usr/lib/systemd/systemd': '0.1:0.0', '/usr/sbin/irqbalance': '0.1:0.0'}",
                             temp_file='t'))
        rca = diagnose_user(conn, query, start_time="2021-04-08 20:31:45")
        logging.info('RCA:%s', rca)
        self.assertEqual(rca, ["2021-04-08 20:31:45", "2021-04-08 20:31:49",
                               ['FuzzyQuery: SQL statement uses the keyword "like" causes fuzzy query',
                                'Avoid fuzzy queries or do not use full fuzzy queries']])

    def test_13_is_not_null(self):
        conn = SQLiteStorage(os.path.realpath("./data/127_0_0_1_8000"))
        conn.connect()
        query = "select i_id from bmsql_item where i_id is not null"
        if conn.execute("SELECT * from wdr where timestamp == 1617885110"):
            logging.info("EXIST timestamp == 1617885110 ")
            conn.execute('DELETE FROM wdr WHERE timestamp == 1617885110')

        conn.execute("insert into wdr values ({timestamp}, \"{db_name}\", \"{table_name}\", \"{query}\","
                     " \"{explain}\", \"{start_time}\", \"{finish_time}\", \"{indexes}\")"
                     .format(timestamp=1617885110, db_name='tpcc', table_name='bmsql_item',
                             query="select i_id from bmsql_item where i_id is not null",
                             explain="Seq Scan on bmsql_item  (cost=0.00..2309.00 rows=100000 width=4)\n (1 row)",
                             start_time="2021-04-08 20:31:50",
                             finish_time="2021-04-08 20:31:54",
                             indexes="{'bmsql_order_line': {'bmsql_order_line_pkey': 'CREATE UNIQUE INDEX "
                                     "bmsql_order_line_pkey ON bmsql_order_line USING btree (ol_w_id, ol_d_id,"
                                     " ol_o_id, ol_number) TABLESPACE pg_default'}}"))
        conn.execute("insert into database_exporter values ({timestamp}, \"{guc_parameter}\", "
                     "{connrent_connections}, {qps}, \"{process}\", \"{temp_file}\")"
                     .format(timestamp=1617885110, guc_parameter='128.0,40.0078125,200', connrent_connections=18,
                             qps=1,
                             process="{'java': '14.2:0.2', 'containerd': '0.4:0.0', 'python': '0.3:0.0', "
                                     "'/usr/lib/systemd/systemd': '0.1:0.0', '/usr/sbin/irqbalance': '0.1:0.0'}",
                             temp_file='t'))
        rca = diagnose_user(conn, query, start_time="2021-04-08 20:31:50")
        logging.info('RCA:%s', rca)
        self.assertEqual(rca, ["2021-04-08 20:31:50", "2021-04-08 20:31:54",
                               ['IndexFailure: SQL statement uses the keyword "is not null" causes indexes failure',
                                'Do not use "is not null", otherwise, the index will be invalid']])

    def test_14_unequal(self):
        conn = SQLiteStorage(os.path.realpath("./data/127_0_0_1_8000"))
        conn.connect()
        query = "select i_id from bmsql_item where i_id != 100"
        if conn.execute("SELECT * from wdr where timestamp == 1617885115"):
            logging.info("EXIST timestamp == 1617885115 ")
            conn.execute('DELETE FROM wdr WHERE timestamp == 1617885115')

        conn.execute("insert into wdr values ({timestamp}, \"{db_name}\", \"{table_name}\", \"{query}\","
                     " \"{explain}\", \"{start_time}\", \"{finish_time}\", \"{indexes}\")"
                     .format(timestamp=1617885115, db_name='tpcc', table_name='bmsql_item',
                             query="select i_id from bmsql_item where i_id != 100",
                             explain="Seq Scan on bmsql_item  (cost=0.00..2559.00 rows=99999"
                                     " width=4)\n   Filter: (i_id <> 100)",
                             start_time="2021-04-08 20:31:55",
                             finish_time="2021-04-08 20:31:59",
                             indexes="{'bmsql_order_line': {'bmsql_order_line_pkey': 'CREATE UNIQUE INDEX "
                                     "bmsql_order_line_pkey ON bmsql_order_line USING btree (ol_w_id, ol_d_id,"
                                     " ol_o_id, ol_number) TABLESPACE pg_default'}}"))
        conn.execute("insert into database_exporter values ({timestamp}, \"{guc_parameter}\", "
                     "{connrent_connections}, {qps}, \"{process}\", \"{temp_file}\")"
                     .format(timestamp=1617885115, guc_parameter='128.0,40.0078125,200', connrent_connections=18,
                             qps=1,
                             process="{'java': '14.2:0.2', 'containerd': '0.4:0.0', 'python': '0.3:0.0', "
                                     "'/usr/lib/systemd/systemd': '0.1:0.0', '/usr/sbin/irqbalance': '0.1:0.0'}",
                             temp_file='t'))
        rca = diagnose_user(conn, query, start_time="2021-04-08 20:31:55")
        logging.info('RCA:%s', rca)
        self.assertEqual(rca, ["2021-04-08 20:31:55", "2021-04-08 20:31:59",
                               ['IndexFailure: SQL statement uses the keyword "!=" causes indexes failure',
                                'Change the unequal sign to "or"']])

    def test_15_function(self):
        conn = SQLiteStorage(os.path.realpath("./data/127_0_0_1_8000"))
        conn.connect()
        query = "select i_id from bmsql_item where substring(i_name,1,3)='abc'"
        if conn.execute("SELECT * from wdr where timestamp == 1617885120"):
            logging.info("EXIST timestamp == 1617885120 ")
            conn.execute('DELETE FROM wdr WHERE timestamp == 1617885120')

        conn.execute("insert into wdr values ({timestamp}, \"{db_name}\", \"{table_name}\", \"{query}\","
                     " \"{explain}\", \"{start_time}\", \"{finish_time}\", \"{indexes}\")"
                     .format(timestamp=1617885120, db_name='tpcc', table_name='bmsql_item',
                             query="select i_id from bmsql_item where substring(i_name,1,3)='abc'",
                             explain="Seq Scan on bmsql_item  (cost=0.00..2809.00 rows=500 width=4)\n"
                                     "   Filter: (‘substring’((i_name)::text, 1, 3) = 'abc'::text)",
                             start_time="2021-04-08 20:32:00",
                             finish_time="2021-04-08 20:32:04",
                             indexes="{'bmsql_order_line': {'bmsql_order_line_pkey': 'CREATE UNIQUE INDEX "
                                     "bmsql_order_line_pkey ON bmsql_order_line USING btree (ol_w_id, ol_d_id,"
                                     " ol_o_id, ol_number) TABLESPACE pg_default'}}"))
        conn.execute("insert into database_exporter values ({timestamp}, \"{guc_parameter}\", "
                     "{connrent_connections}, {qps}, \"{process}\", \"{temp_file}\")"
                     .format(timestamp=1617885120, guc_parameter='128.0,40.0078125,200', connrent_connections=18,
                             qps=1,
                             process="{'java': '14.2:0.2', 'containerd': '0.4:0.0', 'python': '0.3:0.0', "
                                     "'/usr/lib/systemd/systemd': '0.1:0.0', '/usr/sbin/irqbalance': '0.1:0.0'}",
                             temp_file='t'))
        rca = diagnose_user(conn, query, start_time="2021-04-08 20:32:00")
        logging.info('RCA:%s', rca)
        self.assertEqual(rca, ["2021-04-08 20:32:00", "2021-04-08 20:32:04",
                               ['IndexFailure: Function operations are used in the WHERE '
                                'clause causes the SQL engine to abandon '
                                ' indexes and use the full table scan',
                                'Avoid using function operations in the where clause']])

    def test_16_or(self):
        conn = SQLiteStorage(os.path.realpath("./data/127_0_0_1_8000"))
        conn.connect()
        query = "select i_id from bmsql_item where i_id =100 or i_price =100"
        if conn.execute("SELECT * from wdr where timestamp == 1617885125"):
            logging.info("EXIST timestamp == {1617885125} ")
            conn.execute('DELETE FROM wdr WHERE timestamp == 1617885125')

        conn.execute("insert into wdr values ({timestamp}, \"{db_name}\", \"{table_name}\", \"{query}\","
                     " \"{explain}\", \"{start_time}\", \"{finish_time}\", \"{indexes}\")"
                     .format(timestamp=1617885125, db_name='tpcc', table_name='bmsql_item',
                             query="select i_id from bmsql_item where i_id =100 or i_price =100",
                             explain="Seq Scan on bmsql_item  (cost=0.00..2809.00 rows=28 width=4)\n"
                                     "   Filter: ((i_id = 100) OR (i_price = 100::numeric))",
                             start_time="2021-04-08 20:32:05",
                             finish_time="2021-04-08 20:32:09",
                             indexes="{'bmsql_item': {'index_item': 'CREATE INDEX index_item ON bmsql_item"
                                     " USING btree (i_name) TABLESPACE pg_default', "
                                     "'index_item2': 'CREATE INDEX index_item2 ON bmsql_item USING btree "
                                     "(i_name) TABLESPACE pg_default', "
                                     "'index_item3': 'CREATE INDEX index_item3 ON bmsql_item USING btree "
                                     "(i_name) TABLESPACE pg_default', "
                                     "'index_item4': 'CREATE INDEX index_item4 ON bmsql_item USING btree "
                                     "(i_name) TABLESPACE pg_default', "
                                     "'index_item5': 'CREATE INDEX index_item5 ON bmsql_item USING btree "
                                     "(i_name) TABLESPACE pg_default', "
                                     "'index_item6': 'CREATE INDEX index_item6 ON bmsql_item USING btree "
                                     "(i_name) TABLESPACE pg_default', "
                                     "'index_item7': 'CREATE INDEX index_item7 ON bmsql_item USING btree "
                                     "(i_name) TABLESPACE pg_default', "
                                     "'index_item_id': 'CREATE INDEX index_item_id ON bmsql_item USING btree "
                                     "(i_im_id) TABLESPACE pg_default', "
                                     "'index_comb': 'CREATE INDEX index_comb ON bmsql_item USING btree "
                                     "(i_id, i_name) TABLESPACE pg_default', "
                                     "'bmsql_item_pkey': 'CREATE UNIQUE INDEX bmsql_item_pkey ON bmsql_item "
                                     "USING btree (i_id) TABLESPACE pg_default'}}"))
        conn.execute("insert into database_exporter values ({timestamp}, \"{guc_parameter}\", "
                     "{connrent_connections}, {qps}, \"{process}\", \"{temp_file}\")"
                     .format(timestamp=1617885125, guc_parameter='128.0,40.0078125,200', connrent_connections=18,
                             qps=1,
                             process="{'java': '14.2:0.2', 'containerd': '0.4:0.0', 'python': '0.3:0.0', "
                                     "'/usr/lib/systemd/systemd': '0.1:0.0', '/usr/sbin/irqbalance': '0.1:0.0'}",
                             temp_file='t'))
        rca = diagnose_user(conn, query, start_time="2021-04-08 20:32:05")
        logging.info('RCA:%s', rca)
        self.assertEqual(rca, ["2021-04-08 20:32:05", "2021-04-08 20:32:09",
                               ['NeedIndex: The SQL or condition contains columns which are not all created indexes',
                                'Create indexes on all related columns'],
                               ['External Resources: There are a large number of redundant indexes in related columns,'
                                ' resulting in slow insert performance', 'Delete the duplicate index before insert']])

    def test_17_update(self):
        conn = SQLiteStorage(os.path.realpath("./data/127_0_0_1_8000"))
        conn.connect()
        query = "update bmsql_item set i_id = 1, i_name = 'name', i_price = 100, i_data = 'sadsad', i_im_id = 123"
        if conn.execute("SELECT * from wdr where timestamp == 1617885130"):
            logging.info("EXIST timestamp == 1617885130 ")
            conn.execute('DELETE FROM wdr WHERE timestamp == 1617885130')

        conn.execute("insert into wdr values ({timestamp}, \"{db_name}\", \"{table_name}\", \"{query}\","
                     " \"{explain}\", \"{start_time}\", \"{finish_time}\", \"{indexes}\")"
                     .format(timestamp=1617885130, db_name='tpcc', table_name='bmsql_item',
                             query="update bmsql_item set i_id = 1, i_name = 'name', i_price = 100, i_data = 'sadsad', i_im_id = 123",
                             explain=" Update on bmsql_item  (cost=0.00..2309.00 rows=100000 width=6)\n"
                                     "   ->  Seq Scan on bmsql_item  (cost=0.00..2309.00 rows=100000 width=6)",
                             start_time="2021-04-08 20:32:10",
                             finish_time="2021-04-08 20:32:14",
                             indexes="{'bmsql_order_line': {'bmsql_order_line_pkey': 'CREATE UNIQUE INDEX "
                                     "bmsql_order_line_pkey ON bmsql_order_line USING btree (ol_w_id, ol_d_id,"
                                     " ol_o_id, ol_number) TABLESPACE pg_default'}}"))
        conn.execute("insert into database_exporter values ({timestamp}, \"{guc_parameter}\", "
                     "{connrent_connections}, {qps}, \"{process}\", \"{temp_file}\")"
                     .format(timestamp=1617885130, guc_parameter='128.0,40.0078125,200', connrent_connections=18,
                             qps=1,
                             process="{'java': '14.2:0.2', 'containerd': '0.4:0.0', 'python': '0.3:0.0', "
                                     "'/usr/lib/systemd/systemd': '0.1:0.0', '/usr/sbin/irqbalance': '0.1:0.0'}",
                             temp_file='t'))
        rca = diagnose_user(conn, query, start_time="2021-04-08 20:32:10")
        logging.info('RCA:%s', rca)
        self.assertEqual(rca, ["2021-04-08 20:32:10", "2021-04-08 20:32:14",
                               ['FullScan: The UPDATE statement updates all columns',
                                'If only one or two columns are changed, do not update all columns, otherwise, '
                                'frequent calls will cause significant performance consumption']])

    def test_18_expr_in_where(self):
        conn = SQLiteStorage(os.path.realpath("./data/127_0_0_1_8000"))
        conn.connect()
        query = "select i_id from bmsql_item where i_id/2=40"
        if conn.execute("SELECT * from wdr where timestamp == 1617885135"):
            logging.info("EXIST timestamp == 1617885135 ")
            conn.execute('DELETE FROM wdr WHERE timestamp == 1617885135')

        conn.execute("insert into wdr values ({timestamp}, \"{db_name}\", \"{table_name}\", \"{query}\","
                     " \"{explain}\", \"{start_time}\", \"{finish_time}\", \"{indexes}\")"
                     .format(timestamp=1617885135, db_name='tpcc', table_name='bmsql_item',
                             query="select i_id from bmsql_item where i_id/2=40",
                             explain=" Seq Scan on bmsql_item  (cost=0.00..2809.00 rows=500 width=4)\n"
                                     "   Filter: ((i_id / 2) = 40::double precision)",
                             start_time="2021-04-08 20:32:15",
                             finish_time="2021-04-08 20:32:19",
                             indexes="{'bmsql_order_line': {'bmsql_order_line_pkey': 'CREATE UNIQUE INDEX "
                                     "bmsql_order_line_pkey ON bmsql_order_line USING btree (ol_w_id, ol_d_id,"
                                     " ol_o_id, ol_number) TABLESPACE pg_default'}}"))
        conn.execute("insert into database_exporter values ({timestamp}, \"{guc_parameter}\", "
                     "{connrent_connections}, {qps}, \"{process}\", \"{temp_file}\")"
                     .format(timestamp=1617885135, guc_parameter='128.0,40.0078125,200', connrent_connections=18,
                             qps=1,
                             process="{'java': '14.2:0.2', 'containerd': '0.4:0.0', 'python': '0.3:0.0', "
                                     "'/usr/lib/systemd/systemd': '0.1:0.0', '/usr/sbin/irqbalance': '0.1:0.0'}",
                             temp_file='t'))
        rca = diagnose_user(conn, query, start_time="2021-04-08 20:32:15")
        logging.info('RCA:%s', rca)
        self.assertEqual(rca, ["2021-04-08 20:32:15", "2021-04-08 20:32:19",
                               ['IndexFailure: Expression manipulation of the WHERE '
                                'clause causes the SQL engine to abandon '
                                'the use of indexes in favor of full table scans',
                                'Change the WHERE clause, do not perform expression operations on the columns']])

    def test_19_not_in(self):
        conn = SQLiteStorage(os.path.realpath("./data/127_0_0_1_8000"))
        conn.connect()
        query = "select i_id from bmsql_item where i_id not in(1,100)"
        if conn.execute("SELECT * from wdr where timestamp == 1617885140"):
            logging.info("EXIST timestamp == 1617885140 ")
            conn.execute('DELETE FROM wdr WHERE timestamp == 1617885140')

        conn.execute("insert into wdr values ({timestamp}, \"{db_name}\", \"{table_name}\", \"{query}\","
                     " \"{explain}\", \"{start_time}\", \"{finish_time}\", \"{indexes}\")"
                     .format(timestamp=1617885140, db_name='tpcc', table_name='bmsql_item',
                             query="select i_id from bmsql_item where i_id not in(1,100)",
                             explain="Seq Scan on bmsql_item  (cost=0.00..2559.00 rows=99998 width=4)\n"
                                     "   Filter: (i_id <> ALL ('{1,100}'::integer[]))",
                             start_time="2021-04-08 20:32:20",
                             finish_time="2021-04-08 20:32:24",
                             indexes="{'bmsql_order_line': {'bmsql_order_line_pkey': 'CREATE UNIQUE INDEX "
                                     "bmsql_order_line_pkey ON bmsql_order_line USING btree (ol_w_id, ol_d_id,"
                                     " ol_o_id, ol_number) TABLESPACE pg_default'}}"))
        conn.execute("insert into database_exporter values ({timestamp}, \"{guc_parameter}\", "
                     "{connrent_connections}, {qps}, \"{process}\", \"{temp_file}\")"
                     .format(timestamp=1617885140, guc_parameter='128.0,40.0078125,200', connrent_connections=18,
                             qps=1,
                             process="{'java': '14.2:0.2', 'containerd': '0.4:0.0', 'python': '0.3:0.0', "
                                     "'/usr/lib/systemd/systemd': '0.1:0.0', '/usr/sbin/irqbalance': '0.1:0.0'}",
                             temp_file='t'))
        rca = diagnose_user(conn, query, start_time="2021-04-08 20:32:20")
        logging.info('RCA:%s', rca)
        self.assertEqual(rca,
                         ["2021-04-08 20:32:20", "2021-04-08 20:32:24", ['FullScan: SQL statement uses the keyword "IN"'
                                                                         ' or "NOT IN" causes the full scan',
                                                                         'For continuity values, you can use the '
                                                                         'keyword "between" instead']])

    def test_20_range_too_large(self):
        conn = SQLiteStorage(os.path.realpath("./data/127_0_0_1_8000"))
        conn.connect()
        query = "select i_id from bmsql_item where i_id < 90000 and i_id > 0"
        if conn.execute("SELECT * from wdr where timestamp == 1617885145"):
            logging.info("EXIST timestamp == 1617885140 ")
            conn.execute('DELETE FROM wdr WHERE timestamp == 1617885145')

        conn.execute("insert into wdr values ({timestamp}, \"{db_name}\", \"{table_name}\", \"{query}\","
                     " \"{explain}\", \"{start_time}\", \"{finish_time}\", \"{indexes}\")"
                     .format(timestamp=1617885145, db_name='tpcc', table_name='bmsql_item',
                             query="select i_id from bmsql_item where i_id < 90000 and i_id > 0",
                             explain="Seq Scan on bmsql_item  (cost=0.00..2559.00 rows=99998 width=4)\n"
                                     "   Filter: (i_id <> ALL ('{1,100}'::integer[]))",
                             start_time="2021-04-08 20:32:25",
                             finish_time="2021-04-08 20:32:29",
                             indexes="{'bmsql_order_line': {'bmsql_order_line_pkey': 'CREATE UNIQUE INDEX "
                                     "bmsql_order_line_pkey ON bmsql_order_line USING btree (ol_w_id, ol_d_id,"
                                     " ol_o_id, ol_number) TABLESPACE pg_default'}}"))
        conn.execute("insert into database_exporter values ({timestamp}, \"{guc_parameter}\", "
                     "{connrent_connections}, {qps}, \"{process}\", \"{temp_file}\")"
                     .format(timestamp=1617885145, guc_parameter='128.0,40.0078125,200', connrent_connections=18,
                             qps=1,
                             process="{'java': '14.2:0.2', 'containerd': '0.4:0.0', 'python': '0.3:0.0', "
                                     "'/usr/lib/systemd/systemd': '0.1:0.0', '/usr/sbin/irqbalance': '0.1:0.0'}",
                             temp_file='t'))
        rca = diagnose_user(conn, query, start_time="2021-04-08 20:32:25")
        logging.info('RCA:%s', rca)
        self.assertEqual(rca,
                         ["2021-04-08 20:32:25", "2021-04-08 20:32:29", ['RangeTooLarge: Condition range is too large',
                                                                         'Please reduce query range']])

    def test_21_sort(self):
        conn = SQLiteStorage(os.path.realpath("./data/127_0_0_1_8000"))
        conn.connect()
        query = "select h_c_id from bmsql_history order by hist_id desc"
        if conn.execute("SELECT * from database_exporter where timestamp == 1617885150"):
            logging.info("[database_exporter] EXIST timestamp == 1617885150")
            conn.execute('DELETE FROM database_exporter WHERE timestamp == 1617885150')
        if conn.execute("SELECT * from wdr where timestamp == 1617885150"):
            logging.info("[wdr] EXIST timestamp == 1617885150")
            conn.execute('DELETE FROM wdr WHERE timestamp == 1617885150')

        conn.execute("insert into wdr values ({timestamp}, \"{db_name}\", \"{table_name}\", \"{query}\","
                     " \"{explain}\", \"{start_time}\", \"{finish_time}\", \"{indexes}\")"
                     .format(timestamp=1617885150, db_name='tpcc', table_name='bmsql_history',
                             query="select h_c_id from bmsql_history order by hist_id desc",
                             explain=" Sort  (cost=706347.33..715885.54 rows=3815283 width=8)\n"
                                     "   Sort Key: hist_id DESC\n"
                                     "   ->  Seq Scan on bmsql_history  (cost=0.00..80623.83 rows=3815283 width=8)",
                             start_time="2021-04-08 20:32:30",
                             finish_time="2021-04-08 20:32:34",
                             indexes="{'bmsql_order_line': {'bmsql_order_line_pkey': 'CREATE UNIQUE INDEX "
                                     "bmsql_order_line_pkey ON bmsql_order_line USING btree (ol_w_id, ol_d_id,"
                                     " ol_o_id, ol_number) TABLESPACE pg_default'}}"))
        conn.execute("insert into database_exporter values ({timestamp}, \"{guc_parameter}\", "
                     "{connrent_connections}, {qps}, \"{process}\", \"{temp_file}\")"
                     .format(timestamp=1617885150, guc_parameter='128.0,40.0078125,200', connrent_connections=18,
                             qps=1,
                             process="{'java': '14.2:0.2', 'containerd': '0.4:0.0', 'python': '0.3:0.0', "
                                     "'/usr/lib/systemd/systemd': '0.1:0.0', '/usr/sbin/irqbalance': '0.1:0.0'}",
                             temp_file='t'))
        rca = diagnose_user(conn, query, start_time="2021-04-08 20:32:30")
        logging.info('RCA:%s', rca)
        self.assertEqual(rca, ["2021-04-08 20:32:30", "2021-04-08 20:32:34",
                               ['ExternalSorting: The cost of query statement sorting is too high, '
                                'resulting in slow SQL', 'Adjust the size of work_mem']])

    def test_22_join(self):
        conn = SQLiteStorage(os.path.realpath("./data/127_0_0_1_8000"))
        conn.connect()
        query = "select hist_id,s_i_id from bmsql_history, bmsql_stock where hist_id = s_i_id"
        if conn.execute("SELECT * from wdr where timestamp == 1617885155"):
            logging.info("EXIST timestamp == 1617885155 ")
            conn.execute('DELETE FROM wdr WHERE timestamp == 1617885155')

        conn.execute("insert into wdr values ({timestamp}, \"{db_name}\", \"{table_name}\", \"{query}\","
                     " \"{explain}\", \"{start_time}\", \"{finish_time}\", \"{indexes}\")"
                     .format(timestamp=1617885155, db_name='tpcc', table_name='bmsql_history',
                             query="select hist_id,s_i_id from bmsql_history, bmsql_stock where hist_id = s_i_id",
                             explain=" Nested Loop  (cost=0.00..1443045369679.17 rows=10006168 width=8)\n"
                                     "   ->  Seq Scan on bmsql_history  (cost=0.00..168981.63 rows=7774563 width=4)\n"
                                     "   ->  Index Only Scan using bmsql_stock_pkey on bmsql_stock  (cost=0.00..185610.10 rows=100 width=4)\n"
                                     "         Index Cond: (s_i_id = bmsql_history.hist_id)",
                             start_time="2021-04-08 20:32:35",
                             finish_time="2021-04-08 20:32:39",
                             indexes="{'bmsql_order_line': {'bmsql_order_line_pkey': 'CREATE UNIQUE INDEX "
                                     "bmsql_order_line_pkey ON bmsql_order_line USING btree (ol_w_id, ol_d_id,"
                                     " ol_o_id, ol_number) TABLESPACE pg_default'}}"))
        conn.execute("insert into database_exporter values ({timestamp}, \"{guc_parameter}\", "
                     "{connrent_connections}, {qps}, \"{process}\", \"{temp_file}\")"
                     .format(timestamp=1617885155, guc_parameter='128.0,40.0078125,200', connrent_connections=18,
                             qps=1,
                             process="{'java': '14.2:0.2', 'containerd': '0.4:0.0', 'python': '0.3:0.0', "
                                     "'/usr/lib/systemd/systemd': '0.1:0.0', '/usr/sbin/irqbalance': '0.1:0.0'}",
                             temp_file='t'))
        rca = diagnose_user(conn, query, start_time="2021-04-08 20:32:35")
        logging.info('RCA:%s', rca)
        self.assertEqual(rca, ["2021-04-08 20:32:35", "2021-04-08 20:32:39",
                               ['NestLoop: The slow execution of "NestLoop" operator during JOIN '
                                'operation of a large table, resulting in slow SQL',
                                'Turn off NestLoop by setting the GUC parameter "enable_nestloop" to off, '
                                'and let the optimizer choose other join methods']])

    def test_23_aggregate(self):
        conn = SQLiteStorage(os.path.realpath("./data/127_0_0_1_8000"))
        conn.connect()
        query = "select h_data from bmsql_history group by h_data"
        if conn.execute("SELECT * from wdr where timestamp == 1617885160"):
            logging.info("EXIST timestamp == 1617885160 ")
            conn.execute('DELETE FROM wdr WHERE timestamp == 1617885160')

        conn.execute("insert into wdr values ({timestamp}, \"{db_name}\", \"{table_name}\", \"{query}\","
                     " \"{explain}\", \"{start_time}\", \"{finish_time}\", \"{indexes}\")"
                     .format(timestamp=1617885160, db_name='tpcc', table_name='bmsql_history',
                             query="select h_data from bmsql_history group by h_data",
                             explain=" SortAggregate  (cost=873904.59..1189659.38 rows=10003079 width=33)\n"
                                     "   Group By Key: s_dist_01\n"
                                     "   ->  Seq Scan on bmsql_stock  (cost=0.00..585154.79 rows=10003079 width=25)",
                             start_time="2021-04-08 20:32:40",
                             finish_time="2021-04-08 20:32:44",
                             indexes="{'bmsql_order_line': {'bmsql_order_line_pkey': 'CREATE UNIQUE INDEX "
                                     "bmsql_order_line_pkey ON bmsql_order_line USING btree (ol_w_id, ol_d_id,"
                                     " ol_o_id, ol_number) TABLESPACE pg_default'}}"))
        conn.execute("insert into database_exporter values ({timestamp}, \"{guc_parameter}\", "
                     "{connrent_connections}, {qps}, \"{process}\", \"{temp_file}\")"
                     .format(timestamp=1617885160, guc_parameter='128.0,40.0078125,200', connrent_connections=18,
                             qps=1,
                             process="{'java': '14.2:0.2', 'containerd': '0.4:0.0', 'python': '0.3:0.0', "
                                     "'/usr/lib/systemd/systemd': '0.1:0.0', '/usr/sbin/irqbalance': '0.1:0.0'}",
                             temp_file='t'))
        rca = diagnose_user(conn, query, start_time="2021-04-08 20:32:40")
        logging.info('RCA:%s', rca)
        self.assertEqual(rca, ["2021-04-08 20:32:40", "2021-04-08 20:32:44",
                               ['SortAggregate: For the aggregate operation of the large result set, '
                                '"Sort Aggregate" operator has poor performance',
                                'By setting the GUC parameter enable_sort to off, let '
                                'the optimizer select the HashAgg operator']])

    def test_24_redistribute(self):
        conn = SQLiteStorage(os.path.realpath("./data/127_0_0_1_8000"))
        conn.connect()
        query = "select o_custkey, l_partkey from orders, lineitem where o_custkey = l_partkey"
        if conn.execute("SELECT * from wdr where timestamp == 1617885165"):
            logging.info("EXIST timestamp == 1617885165 ")
            conn.execute('DELETE FROM wdr WHERE timestamp == 1617885165')

        conn.execute("insert into wdr values ({timestamp}, \"{db_name}\", \"{table_name}\", \"{query}\","
                     " \"{explain}\", \"{start_time}\", \"{finish_time}\", \"{indexes}\")"
                     .format(timestamp=1617885165, db_name='tpch', table_name='{orders, lineitem}',
                             query="select o_custkey, l_partkey from orders, lineitem where o_custkey = l_partkey",
                             explain=" Row Adapter  (cost=4535992602.55..4535992602.55 rows=22266252160 width=16)\n"
                                     "   ->  Vector Streaming (type: GATHER)  (cost=63865790.87..4535992602.55"
                                     " rows=22266252160 width=16)\n"
                                     "         Node/s: All datanodes\n"
                                     "         ->  Vector Sonic Hash Join  (cost=63865786.87..3724202159.14"
                                     " rows=22266252160 width=16)\n"
                                     "               Hash Cond: (orders.o_custkey = lineitem.l_partkey)\n"
                                     "               ->  Vector Streaming(type: REDISTRIBUTE)  "
                                     "(cost=0.00..12298155.00 rows=750000000 width=8)\n"
                                     "                     Spawn on: All datanodes\n"
                                     "                     ->  Vector Partition Iterator  "
                                     "(cost=0.00..1985655.00 rows=750000000 width=8)\n"
                                     "                           Iterations: 7\n"
                                     "                           ->  Partitioned CStore Scan on orders  "
                                     "(cost=0.00..1985655.00 rows=750000000 width=8)\n"
                                     "                                 Selected Partitions:  1..7\n"
                                     "               ->  Vector Streaming(type: REDISTRIBUTE)  "
                                     "(cost=0.00..51268011.03 rows=3000028242 width=8)\n"
                                     "                     Spawn on: All datanodes\n"
                                     "                     ->  Vector Partition Iterator  (cost=0.00..10017622.71"
                                     " rows=3000028242 width=8)\n"
                                     "                           Iterations: 7\n"
                                     "                           ->  Partitioned CStore Scan on lineitem  "
                                     "(cost=0.00..10017622.71 rows=3000028242 width=8)\n"
                                     "                                 Selected Partitions:  1..7",
                             start_time="2021-04-08 20:32:45",
                             finish_time="2021-04-08 20:32:49",
                             indexes="{'bmsql_order_line': {'bmsql_order_line_pkey': 'CREATE UNIQUE INDEX "
                                     "bmsql_order_line_pkey ON bmsql_order_line USING btree (ol_w_id, ol_d_id,"
                                     " ol_o_id, ol_number) TABLESPACE pg_default'}}"))
        conn.execute("insert into database_exporter values ({timestamp}, \"{guc_parameter}\", "
                     "{connrent_connections}, {qps}, \"{process}\", \"{temp_file}\")"
                     .format(timestamp=1617885165, guc_parameter='128.0,40.0078125,200', connrent_connections=18,
                             qps=1,
                             process="{'java': '14.2:0.2', 'containerd': '0.4:0.0', 'python': '0.3:0.0', "
                                     "'/usr/lib/systemd/systemd': '0.1:0.0', '/usr/sbin/irqbalance': '0.1:0.0'}",
                             temp_file='t'))
        rca = diagnose_user(conn, query, start_time="2021-04-08 20:32:45")
        logging.info('RCA:%s', rca)
        self.assertEqual(rca, ["2021-04-08 20:32:45", "2021-04-08 20:32:49",
                               ['DataSkew: Data redistribution during the query execution',
                                'It is recommended to use the distribution key recommending tool '
                                'to recommend appropriate distribution keys to avoid data skew']])

    def test_25_redundant_index(self):
        conn = SQLiteStorage(os.path.realpath("./data/127_0_0_1_8000"))
        conn.connect()
        query = "select i_name from bmsql_item where i_name = 'frank'"
        if conn.execute("SELECT * from wdr where timestamp == 1617885170"):
            logging.info("EXIST timestamp == 1617885160 ")
            conn.execute('DELETE FROM wdr WHERE timestamp == 1617885170')
            conn.execute('DELETE FROM database_exporter WHERE timestamp == 1617885170')

        conn.execute("insert into database_exporter values ({timestamp}, \"{guc_parameter}\", "
                     "{connrent_connections}, {qps}, \"{process}\", \"{temp_file}\")"
                     .format(timestamp=1617885170, guc_parameter='128.0,40.0078125,200', connrent_connections=18,
                             qps=1,
                             process="{'java': '14.2:0.2', 'containerd': '4.4:0.0', 'python': '0.3:0.0', "
                                     "'/usr/lib/systemd/systemd': '0.1:0.0', '/usr/sbin/irqbalance': '0.1:0.0'}",
                             temp_file='f'))

        conn.execute("insert into wdr values ({timestamp}, \"{db_name}\", \"{table_name}\", \"{query}\","
                     " \"{explain}\", \"{start_time}\", \"{finish_time}\", \"{indexes}\")"
                     .format(timestamp=1617885170, db_name='tpcc', table_name='bmsql_item',
                             query="select i_name from bmsql_item where i_name = 'frank'",
                             explain="Seq Scan on bmsql_item  (cost=0.00..2559.00 rows=99999 width=4)\n"
                                     "   Filter: ((i_name)::text = 'frank'::text)",
                             start_time="2021-04-08 20:32:50",
                             finish_time="2021-04-08 20:32:54",
                             indexes="{'bmsql_item': {'index_item': 'CREATE INDEX index_item ON bmsql_item"
                                     " USING btree (i_name) TABLESPACE pg_default', "
                                     "'index_item2': 'CREATE INDEX index_item2 ON bmsql_item USING btree "
                                     "(i_name) TABLESPACE pg_default', "
                                     "'index_item3': 'CREATE INDEX index_item3 ON bmsql_item USING btree "
                                     "(i_name) TABLESPACE pg_default', "
                                     "'index_item4': 'CREATE INDEX index_item4 ON bmsql_item USING btree "
                                     "(i_name) TABLESPACE pg_default', "
                                     "'index_item5': 'CREATE INDEX index_item5 ON bmsql_item USING btree "
                                     "(i_name) TABLESPACE pg_default', "
                                     "'index_item6': 'CREATE INDEX index_item6 ON bmsql_item USING btree "
                                     "(i_name) TABLESPACE pg_default', "
                                     "'index_item7': 'CREATE INDEX index_item7 ON bmsql_item USING btree "
                                     "(i_name) TABLESPACE pg_default', "
                                     "'index_item_id': 'CREATE INDEX index_item_id ON bmsql_item USING btree "
                                     "(i_im_id) TABLESPACE pg_default', "
                                     "'index_comb': 'CREATE INDEX index_comb ON bmsql_item USING btree "
                                     "(i_id, i_name) TABLESPACE pg_default', "
                                     "'bmsql_item_pkey': 'CREATE UNIQUE INDEX bmsql_item_pkey ON bmsql_item "
                                     "USING btree (i_id) TABLESPACE pg_default'}}"))
        rca = diagnose_user(conn, query, start_time="2021-04-08 20:32:50")
        logging.info('RCA:%s', rca)
        self.assertEqual(rca, ["2021-04-08 20:32:50", "2021-04-08 20:32:54",
                               ['External Resources: There are a large number of redundant '
                                'indexes in related columns, resulting in slow insert performance',
                                'Delete the duplicate index before insert']])

    def test_26_resource_shortage(self):
        conn = SQLiteStorage(os.path.realpath("./data/127_0_0_1_8000"))
        conn.connect()
        query = "select i_im_id from bmsql_item where i_im_id = 123"
        if conn.execute("SELECT * from database_exporter where timestamp == 1617885175"):
            logging.info("[database_exporter] EXIST timestamp == 1617885175")
            conn.execute('DELETE FROM database_exporter WHERE timestamp == 16178851275')
        if conn.execute("SELECT * from wdr where timestamp == 1617885175"):
            logging.info("[wdr] EXIST timestamp == 1617885150")
            conn.execute('DELETE FROM wdr WHERE timestamp == 16178851275')

        conn.execute("insert into wdr values ({timestamp}, \"{db_name}\", \"{table_name}\", \"{query}\","
                     " \"{explain}\", \"{start_time}\", \"{finish_time}\", \"{indexes}\")"
                     .format(timestamp=1617885175, db_name='tpcc', table_name='bmsql_history',
                             query="select i_im_id from bmsql_item where i_im_id = 123",
                             explain="Seq Scan on bmsql_item  (cost=0.00..2559.00 rows=99999 width=4)\n"
                                     "   Filter: (i_im_id = 123)",
                             start_time="2021-04-08 20:32:55",
                             finish_time="2021-04-08 20:32:59",
                             indexes="{'bmsql_order_line': {'bmsql_order_line_pkey': 'CREATE UNIQUE INDEX "
                                     "bmsql_order_line_pkey ON bmsql_order_line USING btree (ol_w_id, ol_d_id,"
                                     " ol_o_id, ol_number) TABLESPACE pg_default'}}"))
        conn.execute("insert into database_exporter values ({timestamp}, \"{guc_parameter}\", "
                     "{connrent_connections}, {qps}, \"{process}\", \"{temp_file}\")"
                     .format(timestamp=1617885175, guc_parameter='128.0,40.0078125,200', connrent_connections=18,
                             qps=1,
                             process="{'java': '14.2:0.2', 'containerd': '24.4:0.0', 'python': '0.3:0.0', "
                                     "'/usr/lib/systemd/systemd': '0.1:0.0', '/usr/sbin/irqbalance': '0.1:0.0'}",
                             temp_file='f'))
        rca = diagnose_user(conn, query, start_time="2021-04-08 20:32:55")
        logging.info('RCA:%s', rca)
        self.assertEqual(rca, ["2021-04-08 20:32:55", "2021-04-08 20:32:59",
                               ['External Resources: External processes occupy a large number of system resources,'
                                ' resulting in a database resource shortage',
                                'Stop unnecessary large processes']])

    def test_27_others(self):
        conn = SQLiteStorage(os.path.realpath("./data/127_0_0_1_8000"))
        conn.connect()
        query = "select i_id from bmsql_item where i_id = 123"
        if conn.execute("SELECT * from database_exporter where timestamp == 1617885180"):
            logging.info("[database_exporter] EXIST timestamp == 1617885180")
            conn.execute('DELETE FROM database_exporter WHERE timestamp == 16178851280')
        if conn.execute("SELECT * from wdr where timestamp == 1617885180"):
            logging.info("[wdr] EXIST timestamp == 1617885180")
            conn.execute('DELETE FROM wdr WHERE timestamp == 16178851280')

        conn.execute("insert into wdr values ({timestamp}, \"{db_name}\", \"{table_name}\", \"{query}\","
                     " \"{explain}\", \"{start_time}\", \"{finish_time}\", \"{indexes}\")"
                     .format(timestamp=1617885180, db_name='tpcc', table_name='bmsql_history',
                             query="select i_id from bmsql_item where i_id = 123",
                             explain="Seq Scan on bmsql_item  (cost=0.00..2559.00 rows=99999 width=4)\n"
                                     "   Filter: (i_id = 123)",
                             start_time="2021-04-08 20:33:00",
                             finish_time="2021-04-08 20:33:04",
                             indexes="{'bmsql_order_line': {'bmsql_order_line_pkey': 'CREATE UNIQUE INDEX "
                                     "bmsql_order_line_pkey ON bmsql_order_line USING btree (ol_w_id, ol_d_id,"
                                     " ol_o_id, ol_number) TABLESPACE pg_default'}}"))
        conn.execute("insert into database_exporter values ({timestamp}, \"{guc_parameter}\", "
                     "{connrent_connections}, {qps}, \"{process}\", \"{temp_file}\")"
                     .format(timestamp=1617885180, guc_parameter='128.0,40.0078125,200', connrent_connections=18,
                             qps=1,
                             process="{'java': '14.2:0.2', 'containerd': '4.4:0.0', 'python': '0.3:0.0', "
                                     "'/usr/lib/systemd/systemd': '0.1:0.0', '/usr/sbin/irqbalance': '0.1:0.0'}",
                             temp_file='f'))
        rca = diagnose_user(conn, query, start_time="2021-04-08 20:33:00")
        logging.info('RCA:%s', rca)
        self.assertEqual(rca, ["2021-04-08 20:33:00", "2021-04-08 20:33:04",
                               ['External Resources: Database load request crowded',
                                'It is recommended to change the free time to execute']])

    def test_28_insert(self):
        conn = SQLiteStorage(os.path.realpath("./data/127_0_0_1_8000"))
        conn.connect()
        query = "insert into bmsql_item values(11321321,'123',300.00,'wsqddas',12)"
        if conn.execute("SELECT * from wdr where timestamp == 1617885200"):
            logging.info("[wdr] EXIST timestamp == 1617885200")
            conn.execute('DELETE FROM wdr WHERE timestamp == 1617885200')

        conn.execute("insert into wdr values ({timestamp}, \"{db_name}\", \"{table_name}\", \"{query}\","
                     " \"{explain}\", \"{start_time}\", \"{finish_time}\", \"{indexes}\")"
                     .format(timestamp=1617885200, db_name='tpcc', table_name='bmsql_item',
                             query="insert into bmsql_item values(11321321,'123',300.00,'wsqddas',12)",
                             explain=" Insert on bmsql_item  (cost=0.00..0.01 rows=1 width=0)\n"
                                     "   ->  Result  (cost=0.00..0.01 rows=1 width=0)",
                             start_time="2021-04-08 20:33:20",
                             finish_time="2021-04-08 20:33:24",
                             indexes="{'bmsql_order_line': {'bmsql_order_line_pkey': 'CREATE UNIQUE INDEX "
                                     "bmsql_order_line_pkey ON bmsql_order_line USING btree (ol_w_id, ol_d_id,"
                                     " ol_o_id, ol_number) TABLESPACE pg_default'}}"))


        conn.execute("insert into database_exporter values ({timestamp}, \"{guc_parameter}\", "
                     "{connrent_connections}, {qps}, \"{process}\", \"{temp_file}\")"
                     .format(timestamp=1617885200, guc_parameter='128.0,40.0078125,200', connrent_connections=18,
                             qps=-1,
                             process="{'java': '14.2:0.2', 'containerd': '4.4:0.0', 'python': '0.3:0.0', "
                                     "'/usr/lib/systemd/systemd': '0.1:0.0', '/usr/sbin/irqbalance': '0.1:0.0'}",
                             temp_file='f'))
        rca = diagnose_user(conn, query, start_time="2021-04-08 20:33:20")
        logging.info('RCA:%s', rca)
        self.assertEqual(rca, ["2021-04-08 20:33:20", "2021-04-08 20:33:24",
                               ['Database Resources: A large number of data or indexes are involved',
                                'You can use "copy" instead of "insert"']])

    def test_29_delete(self):
        conn = SQLiteStorage(os.path.realpath("./data/127_0_0_1_8000"))
        conn.connect()
        query = "delete from bmsql_item where i_id = 1"
        if conn.execute("SELECT * from wdr where timestamp == 1617885205"):
            logging.info("[wdr] EXIST timestamp == 1617885205")
            conn.execute('DELETE FROM wdr WHERE timestamp == 1617885205')

        conn.execute("insert into wdr values ({timestamp}, \"{db_name}\", \"{table_name}\", \"{query}\","
                     " \"{explain}\", \"{start_time}\", \"{finish_time}\", \"{indexes}\")"
                     .format(timestamp=1617885205, db_name='tpcc', table_name='bmsql_item',
                             query="delete from bmsql_item where i_id = 1",
                             explain=" Delete on bmsql_item  (cost=0.00..8.28 rows=1 width=6)\n"
                                     "   ->  Index Scan using bmsql_item_pkey on bmsql_item  (cost=0.00..8.28 rows=1 width=6)\n"
                                     "         Index Cond: (i_id = 1)",
                             start_time="2021-04-08 20:33:25",
                             finish_time="2021-04-08 20:33:29",
                             indexes="{'bmsql_order_line': {'bmsql_order_line_pkey': 'CREATE UNIQUE INDEX "
                                     "bmsql_order_line_pkey ON bmsql_order_line USING btree (ol_w_id, ol_d_id,"
                                     " ol_o_id, ol_number) TABLESPACE pg_default'}}"))


        conn.execute("insert into database_exporter values ({timestamp}, \"{guc_parameter}\", "
                     "{connrent_connections}, {qps}, \"{process}\", \"{temp_file}\")"
                     .format(timestamp=1617885205, guc_parameter='128.0,40.0078125,200', connrent_connections=18,
                             qps=-1,
                             process="{'java': '14.2:0.2', 'containerd': '4.4:0.0', 'python': '0.3:0.0', "
                                     "'/usr/lib/systemd/systemd': '0.1:0.0', '/usr/sbin/irqbalance': '0.1:0.0'}",
                             temp_file='f'))
        rca = diagnose_user(conn, query, start_time="2021-04-08 20:33:25")
        logging.info('RCA:%s', rca)
        self.assertEqual(rca, ["2021-04-08 20:33:25", "2021-04-08 20:33:29",
                               ['Database Resources: A large number of data or indexes are involved',
                                'You may launch batch deletions or remove and recreate indexes']])

    def test_30_load_request_centralization(self):
        conn = SQLiteStorage(os.path.realpath("./data/127_0_0_1_8000"))
        conn.connect()
        query = "select i_id from bmsql_item where i_id =100"
        if conn.execute("SELECT * from wdr where timestamp == 1617885185"):
            logging.info("EXIST timestamp == 1617885185 ")
            conn.execute('DELETE FROM wdr WHERE timestamp == 1617885185')

        conn.execute("insert into wdr values ({timestamp}, \"{db_name}\", \"{table_name}\", \"{query}\","
                     " \"{explain}\", \"{start_time}\", \"{finish_time}\", \"{indexes}\")"
                     .format(timestamp=1617885185, db_name='tpcc', table_name='bmsql_item',
                             query="select i_id from bmsql_item where i_id =100",
                             explain="Seq Scan on bmsql_item  (cost=0.00..2809.00 rows=28 width=4)\n"
                                     "   Filter: ((i_id = 100))",
                             start_time="2021-04-08 20:33:05",
                             finish_time="2021-04-08 20:33:09",
                             indexes="{'bmsql_order_line': {'bmsql_order_line_pkey': 'CREATE UNIQUE INDEX "
                                     "bmsql_order_line_pkey ON bmsql_order_line USING btree (ol_w_id, ol_d_id,"
                                     " ol_o_id, ol_number) TABLESPACE pg_default'}}"))
        conn.execute("insert into database_exporter values ({timestamp}, \"{guc_parameter}\", "
                     "{connrent_connections}, {qps}, \"{process}\", \"{temp_file}\")"
                     .format(timestamp=1617885185, guc_parameter='128.0,40.0078125,200', connrent_connections=18,
                             qps=6037,
                             process="{'java': '14.2:0.2', 'containerd': '4.4:0.0', 'python': '0.3:0.0', "
                                     "'/usr/lib/systemd/systemd': '0.1:0.0', '/usr/sbin/irqbalance': '0.1:0.0'}",
                             temp_file='f'))
        rca = diagnose_user(conn, query, start_time="2021-04-08 20:33:05")
        logging.info('RCA:%s', rca)
        self.assertEqual(rca, ["2021-04-08 20:33:05", "2021-04-08 20:33:09",
                               ['External Resources: Database request crowded',
                                'It is recommended to change the free time to execute']])

    def test_31_auto(self):
        conn = SQLiteStorage(os.path.realpath("./data/127_0_0_1_8000"))
        conn.connect()
        query = "select i_id from bmsql_item where i_id =100"
        if conn.execute("SELECT * from wdr where timestamp == 1617885190"):
            logging.info("EXIST timestamp == 1617885190 ")
            conn.execute('DELETE FROM wdr WHERE timestamp == 1617885190')

        conn.execute("insert into wdr values ({timestamp}, \"{db_name}\", \"{table_name}\", \"{query}\","
                     " \"{explain}\", \"{start_time}\", \"{finish_time}\", \"{indexes}\")"
                     .format(timestamp=1617885190, db_name='tpcc', table_name='bmsql_item',
                             query="select i_id from bmsql_item where i_id =100",
                             explain="Seq Scan on bmsql_item  (cost=0.00..2809.00 rows=28 width=4)\n"
                                     "   Filter: ((i_id = 100))",
                             start_time="2021-04-08 20:33:10",
                             finish_time="2021-04-08 20:33:14",
                             indexes="{'bmsql_order_line': {'bmsql_order_line_pkey': 'CREATE UNIQUE INDEX "
                                     "bmsql_order_line_pkey ON bmsql_order_line USING btree (ol_w_id, ol_d_id,"
                                     " ol_o_id, ol_number) TABLESPACE pg_default'}}"))
        conn.execute("insert into database_exporter values ({timestamp}, \"{guc_parameter}\", "
                     "{connrent_connections}, {qps}, \"{process}\", \"{temp_file}\")"
                     .format(timestamp=1617885190, guc_parameter='128.0,40.0078125,200', connrent_connections=18,
                             qps=1,
                             process="{'java': '14.2:0.2', 'containerd': '4.4:0.0', 'python': '0.3:0.0', "
                                     "'/usr/lib/systemd/systemd': '0.1:0.0', '/usr/sbin/irqbalance': '0.1:0.0'}",
                             temp_file='f'))
        rca = diagnose_auto(conn, query, start_time="2021-04-08 20:33:10")
        logging.info('RCA:%s', rca)
        self.assertEqual(rca, [['External Resources: Database request crowded',
                                'It is recommended to change the free time to execute']])




    def test_40_select_timeseries_by_timestamp(self, table='wdr', database_path='./data/127_0_0_1_8000'):
        timestamp = int(time.time())
        with sqlite_storage.SQLiteStorage(database_path) as db:
            timeseries_by_timestamp = db.select_timeseries_by_timestamp(table=table, field='query', period='10000W',
                                                                        timestamp=timestamp)
            self.assertGreater(len(timeseries_by_timestamp), 0)

    def test_41_select_timeseries_by_number(self, table='wdr', database_path='./data/127_0_0_1_8000'):
        with sqlite_storage.SQLiteStorage(database_path) as db:
            timeseries_by_number = db.select_timeseries_by_number(table=table, field='query', number=10)
            self.assertGreater(len(timeseries_by_number), 0)

    def test_42_get_table_rows(self, table='wdr', database_path='./data/127_0_0_1_8000'):
        with sqlite_storage.SQLiteStorage(database_path) as db:
            rows = db.get_table_rows(table=table)
            self.assertGreater(rows, 0)

    def test_43_get_all_tables(self, database_path='./data/127_0_0_1_8000'):
        with sqlite_storage.SQLiteStorage(database_path) as db:
            tables = db.get_all_tables()
            self.assertEqual(set(tables), set(['wdr', 'database_exporter', 'os_exporter']))

    def test_44_check_tables(self, database_path='./data/127_0_0_1_8000'):
        with sqlite_storage.SQLiteStorage(database_path) as db:
            flag = db.check_table('wdr')
            self.assertTrue(flag)

    def test_45_get_all_fields(self, table='wdr', database_path='./data/127_0_0_1_8000'):
        with sqlite_storage.SQLiteStorage(database_path) as db:
            fields = db.get_all_fields(table=table)
            self.assertEqual(set(fields),
                             set(['db_name', 'table_name', 'query', 'explain', 'start_time', 'finish_time', 'indexes']))

    def test_46_get_earliest_timestamp(self, table='wdr', database_path='./data/127_0_0_1_8000'):
        with sqlite_storage.SQLiteStorage(database_path) as db:
            timestamp = db.get_earliest_timestamp(table=table)
            self.assertEqual(timestamp, 1617885101)

    def test_47_get_lastest_timestamp(self, table='wdr', database_path='./data/127_0_0_1_8000'):
        with sqlite_storage.SQLiteStorage(database_path) as db:
            timestamp = db.get_latest_timestamp(table=table)
            self.assertEqual(timestamp, 1617885205)

    def test_48_load_sql_operation(self, table='wdr', database_path='./data/127_0_0_1_8000'):
        with sqlite_storage.SQLiteStorage(database_path) as db:
            self.assertGreater(len(db.sql_operation), 0)

    def test_49_fetch_all_result(self, table='wdr', database_path='./data/127_0_0_1_8000'):
        with sqlite_storage.SQLiteStorage(database_path) as db:
            result = db.fetch_all_result('select * from {table}'.format(table=table))
            self.assertGreater(len(result), 0)

    def test_50_TimeString(self):
        transform_result_1 = TimeString('10S').to_second()
        transform_result_2 = TimeString('2M').to_second()
        transform_result_3 = TimeString('3H').to_second()
        transform_result_4 = TimeString('1D').to_second()
        transform_result_5 = TimeString('2W').to_second()

        transform_result_6 = TimeString('10S').to_timedelta()
        transform_result_7 = TimeString('2M').to_timedelta()
        transform_result_8 = TimeString('3H').to_timedelta()
        transform_result_9 = TimeString('1D').to_timedelta()
        transform_result_10 = TimeString('2W').to_timedelta()

        self.assertEqual(transform_result_1, 10)
        self.assertEqual(transform_result_2, 2 * 60)
        self.assertEqual(transform_result_3, 3 * 60 * 60)
        self.assertEqual(transform_result_4, 1 * 24 * 60 * 60)
        self.assertEqual(transform_result_5, 2 * 7 * 24 * 60 * 60)

        self.assertEqual(transform_result_6, timedelta(seconds=10))
        self.assertEqual(transform_result_7, timedelta(minutes=2))
        self.assertEqual(transform_result_8, timedelta(hours=3))
        self.assertEqual(transform_result_9, timedelta(days=1))
        self.assertEqual(transform_result_10, timedelta(weeks=2))

    def test_51_unify_byte_info(self):
        unify_result_1 = convert_to_mb('10K')
        unify_result_2 = convert_to_mb('10M')
        unify_result_3 = convert_to_mb('10G')
        unify_result_4 = convert_to_mb('10T')
        unify_result_5 = convert_to_mb('10P')

        self.assertEqual(unify_result_1, 0.009765625)
        self.assertEqual(unify_result_2, 10)
        self.assertEqual(unify_result_3, 10 * 1024)
        self.assertEqual(unify_result_4, 10 * 1024 * 1024)
        self.assertEqual(unify_result_5, 10 * 1024 * 1024 * 1024)

    def test_54_unify_byte_unit(self):
        res1 = extract_table_from_sql("select name from table1 where id=3;")
        res2 = extract_table_from_sql("select id, age from table2 where id=3;")
        self.assertEqual(res1[0], 'table1')
        self.assertEqual(res2[0], 'table2')

    def test_55_input_sql_processing(self):
        sql1 = input_sql_processing("select  id from item where  name='jack'")
        sql2 = input_sql_processing("SELECT i_price, i_name, i_data     FROM bmsql_item     WHERE i_id = 3")
        self.assertEqual(sql1, "SELECT ID FROM ITEM WHERE NAME = ?")
        self.assertEqual(sql2, "SELECT I_PRICE , I_NAME , I_DATA FROM BMSQL_ITEM WHERE I_ID = ?")

    def test_56_wdr_sql_processing(self):
        sql1 = wdr_sql_processing("select  id from item where  name=$1")
        sql2 = wdr_sql_processing(
            "SELECT i_price, i_name, i_data     FROM bmsql_item     WHERE i_id = $1 and i_i_id = $2")
        self.assertEqual(sql1, "SELECT ID FROM ITEM WHERE NAME = ?")
        self.assertEqual(sql2, "SELECT I_PRICE , I_NAME , I_DATA FROM BMSQL_ITEM WHERE I_ID = ? AND I_I_ID = ?")

    def test_57_unify_sql(self):
        sql = unify_sql("select  id from  item where name = 'jack'")
        self.assertEqual(sql, "SELECT ID FROM ITEM WHERE NAME = 'JACK'")

    def test_58_auto_arima(self):
        timeseries = [(1606189212, 66.0), (1606189213, 55.0), (1606189214, 47.0), (1606189215, 13.0),
                      (1606189216, 107.0),
                      (1606189217, 46.0), (1606189218, 39.0), (1606189219, 10.0), (1606189220, 53.0),
                      (1606189221, 54.0),
                      (1606189222, 13.0), (1606189223, 50.0), (1606189224, 109.0), (1606189225, 49.0),
                      (1606189226, 46.0),
                      (1606189227, 29.0), (1606189228, 97.0), (1606189229, 9.0), (1606189230, 99.0), (1606189231, 26.0),
                      (1606189232, 49.0), (1606189233, 12.0), (1606189234, 111.0), (1606189235, 1.0),
                      (1606189236, 63.0),
                      (1606189237, 39.0), (1606189238, 114.0), (1606189239, 10.0), (1606189240, 0.0),
                      (1606189241, 43.0),
                      (1606189242, 25.0), (1606189243, 91.0), (1606189244, 92.0), (1606189245, 28.0),
                      (1606189246, 87.0),
                      (1606189247, 38.0), (1606189248, 43.0), (1606189249, 19.0), (1606189250, 26.0),
                      (1606189251, 118.0)]
        am = AutoArima()
        am.fit(timeseries)
        result = am.forecast(period=10, freq='2S')
        self.assertEqual(len(result[0]), 10)


if __name__ == '__main__':
    unittest.main()
