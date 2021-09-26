import psycopg2
import re
from .execute_factory import ExecuteFactory
from .execute_factory import IndexInfo


class DriverExecute(ExecuteFactory):
    def __init__(self, *arg):
        super(DriverExecute, self).__init__(*arg)
        self.conn = None
        self.cur = None

    def init_conn_handle(self):
        self.conn = psycopg2.connect(dbname=self.dbname,
                                     user=self.user,
                                     password=self.password,
                                     host=self.host,
                                     port=self.port)
        self.cur = self.conn.cursor()

    def execute(self, sql):
        try:
            self.cur.execute(sql)
            self.conn.commit()
            return self.cur.fetchall()
        except Exception:
            self.conn.commit()

    def close_conn(self):
        if self.conn and self.cur:
            self.cur.close()
            self.conn.close()

    @staticmethod
    def parse_single_advisor_result(res, workload_table_name):
        table_index_dict = {}
        items = res.strip('()').split(',', 2)
        if len(items) == 3:
            table = items[0] + '.' + items[1]
            workload_table_name[items[0]] = workload_table_name.get(items[0], set())
            workload_table_name[items[0]].add(items[1])
            indexes = re.split('[()]', items[2].strip('\"'))
            for columns in indexes:
                if columns == '':
                    continue
                if table not in table_index_dict.keys():
                    table_index_dict[table] = []
                table_index_dict[table].append(columns)

        return table_index_dict

    @staticmethod
    def make_single_advisor_sql(ori_sql):
        sql = 'select gs_index_advise(\''
        for elem in ori_sql:
            if elem == '\'':
                sql += '\''
            sql += elem
        sql += '\');'

        return sql

    # call the single-index-advisor in the database
    def query_index_advisor(self, query, workload_table_name):
        table_index_dict = {}

        if 'select' not in query.lower():
            return table_index_dict
        if self.schema:
            sql = 'SET current_schema = %s;' % self.schema
        sql += DriverExecute.make_single_advisor_sql(query)
        result = self.execute(sql=sql)
        if not result:
            return table_index_dict
        for res in result:
            table_index_dict.update(DriverExecute.parse_single_advisor_result(res[0],
                                                                              workload_table_name))

        return table_index_dict

    # judge whether the index is used by the optimizer
    def query_index_check(self, query, query_index_dict, multi_node):
        valid_indexes = {}
        if len(query_index_dict) == 0:
            return valid_indexes

        # create hypo-indexes
        if self.schema:
            sqls = 'SET current_schema = %s;' % self.schema
        sqls += 'SET enable_hypo_index = on;'
        if multi_node:
            sqls += 'SET enable_fast_query_shipping = off;SET enable_stream_operator = on;'
        for table in query_index_dict.keys():
            for columns in query_index_dict[table]:
                if columns != '':
                    sqls += "SELECT hypopg_create_index('CREATE INDEX ON %s(%s)');" % \
                            (table, columns)
        sqls += 'SELECT * from hypopg_display_index();'
        result = self.execute(sqls)
        if not result:
            return valid_indexes
        hypoid_table_column = {}
        for item in result:
            if len(item) == 4:
                table_name = re.search(r'btree_(.*%s)' % item[2], item[0]).group(1)
                schema_table_name = '.'.join(table_name.split('_', 1))
                if schema_table_name in query_index_dict.keys():
                    table_name = schema_table_name
                elif 'public.' + table_name in query_index_dict.keys():
                    table_name = 'public.' + table_name
                else:
                    return valid_indexes
                hypoid_table_column[str(item[1])] = \
                    table_name + ':' + item[3].strip('()')
        sqls = "SET explain_perf_mode = 'normal'; explain %s" % query
        result = self.execute(sqls)
        if not result:
            return valid_indexes
        # parse the result of explain plan
        for item in result:
            if 'Index' in item[0] and 'Scan' in item[0] and 'btree' in item[0]:
                super().get_valid_indexes(item[0], hypoid_table_column, valid_indexes)
        self.execute('SELECT hypopg_reset_index()')
        return valid_indexes

    @staticmethod
    # parse the explain plan to get estimated cost by database optimizer
    def parse_explain_plan(plan, index_config, ori_indexes_name):
        cost_total = -1
        cost_flag = True
        for line in plan:
            if '(cost=' in line[0] and cost_flag:
                cost_flag = False
                pattern = re.compile(r'\(cost=([^\)]*)\)', re.S)
                matched_res = re.search(pattern, line[0])
                if matched_res:
                    cost_list = matched_res.group(1).split()
                    if len(cost_list) == 3:
                        cost_total = float(cost_list[0].split('..')[-1])
            if 'Index' in line[0] and 'Scan' in line[0] and not index_config:
                ind1, ind2 = re.search(r'Index.*Scan(.*)on ([^\s]+)',
                                       line[0].strip(), re.IGNORECASE).groups()
                if ind1.strip():
                    ori_indexes_name.add(ind1.strip().split(' ')[1])
                else:
                    ori_indexes_name.add(ind2)

        return cost_total

    def update_index_storage(self, index_id, index_config, hypo_index_num):
        index_size_sql = 'select * from hypopg_estimate_size(%s);' % index_id
        res = self.execute(index_size_sql)
        if res:
            index_config[hypo_index_num].storage = float(res[0][0]) / 1024 / 1024

    def estimate_workload_cost_file(self, workload, index_config=None, ori_indexes_name=None):
        total_cost = 0
        hypo_index_num = 0
        is_computed = False
        self.execute('SET current_schema = %s' % self.schema)
        if index_config:
            if len(index_config) == 1 and index_config[0].positive_pos:
                is_computed = True
            # create hypo-indexes
            self.execute('SET enable_hypo_index = on')
            for index in index_config:
                res = self.execute("SELECT * from hypopg_create_index('CREATE INDEX ON %s(%s)')" %
                                   (index.table, index.columns))
                if self.max_index_storage and res:
                    self.update_index_storage(res[0][0], index_config, hypo_index_num)
                hypo_index_num += 1
        if self.multi_node:
            self.execute('SET enable_fast_query_shipping = off;SET enable_stream_operator = on')
        self.execute("SET explain_perf_mode = 'normal'")

        for ind, query in enumerate(workload):
            # record ineffective sql and negative sql for candidate indexes
            if is_computed:
                super().record_ineffective_negative_sql(index_config[0], query, ind)
            if 'select ' not in query.statement.lower():
                workload[ind].cost_list.append(0)
            else:
                res = self.execute('EXPLAIN ' + query.statement)
                if res:
                    query_cost = DriverExecute.parse_explain_plan(res, index_config, ori_indexes_name)
                    query_cost *= workload[ind].frequency
                    workload[ind].cost_list.append(query_cost)
                    total_cost += query_cost
                else:
                    workload[ind].cost_list.append(0)
        if index_config:
            self.execute('SELECT hypopg_reset_index()')
        return total_cost

    def check_useless_index(self, tables, history_indexes, history_invalid_indexes):
        schemas = list(filter(None, self.schema.split(',')))
        whole_indexes = list()
        redundant_indexes = list()
        matched_table_name = set()
        if not tables:
            return whole_indexes, redundant_indexes
        for schema in schemas:
            if not tables.get(schema.strip()):
                continue
            tables_string = ','.join(["'%s'" % table for table in tables[schema.strip()]])
            # query all table index information and primary key information
            sql = "SELECT c.relname AS tablename, i.relname AS indexname, " \
                  "pg_get_indexdef(i.oid) AS indexdef, p.contype AS pkey from " \
                  "pg_index x JOIN pg_class c ON c.oid = x.indrelid JOIN " \
                  "pg_class i ON i.oid = x.indexrelid LEFT JOIN pg_namespace n " \
                  "ON n.oid = c.relnamespace LEFT JOIN pg_constraint p ON i.oid = p.conindid " \
                  "WHERE (c.relkind = ANY (ARRAY['r'::\"char\", 'm'::\"char\"])) AND " \
                  "(i.relkind = ANY (ARRAY['i'::\"char\", 'I'::\"char\"])) AND " \
                  "n.nspname = '%s' AND c.relname in (%s) order by c.relname;" % \
                  (schema, tables_string)
            res = self.execute(sql)
            if not res:
                continue
            cur_table_indexes = list()
            for item in res:
                cur_columns = re.search(r'\(([^\(\)]*)\)', item[2]).group(1)
                cur_index_obj = IndexInfo(schema, item[0], item[1], cur_columns, item[2])
                if item[3]:
                    cur_index_obj.primary_key = True
                # record all indexes
                whole_indexes.append(cur_index_obj)
                # match the last recommendation result
                tbl_name = schema + '.' + item[0]
                if history_indexes.get(tbl_name):
                    matched_table_name.add(tbl_name)
                super().match_last_result(tbl_name, cur_columns, history_indexes,
                                          history_invalid_indexes)
                # record redundant indexes
                if cur_table_indexes and cur_table_indexes[-1].table != item[0]:
                    super().record_redundant_indexes(cur_table_indexes, redundant_indexes)
                    cur_table_indexes = []
                cur_table_indexes.append(cur_index_obj)
            if cur_table_indexes:
                # record redundant indexes
                super().record_redundant_indexes(cur_table_indexes, redundant_indexes)
        # match history indexes to find invalid indexes
        check_table_list = list(set(history_indexes.keys()).difference(matched_table_name))
        for table in check_table_list:
            sql = "select indexdef from pg_indexes where schemaname='%s' and tablename='%s';" % \
                  (table.split('.')[0], table.split('.')[1])
            res = self.execute(sql)
            for item in res:
                cur_columns = re.search(r'\(([^\(\)]*)\)', item[0]).group(1)
                super().match_last_result(table, cur_columns, history_indexes,
                                          history_invalid_indexes)
        return whole_indexes, redundant_indexes

