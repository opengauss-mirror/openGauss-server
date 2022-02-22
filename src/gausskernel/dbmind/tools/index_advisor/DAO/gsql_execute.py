import os
import re
import shlex
import subprocess
import sys
import time
from .execute_factory import ExecuteFactory
from .execute_factory import IndexInfo

BASE_CMD = None


class GSqlExecute(ExecuteFactory):
    def __init__(self, *args):
        super(GSqlExecute, self).__init__(*args)

    def init_conn_handle(self):
        global BASE_CMD
        BASE_CMD = 'gsql -p ' + str(self.port) + ' -d ' + self.dbname
        if self.host:
            BASE_CMD += ' -h ' + self.host
        if self.user:
            BASE_CMD += ' -U ' + self.user
        if self.password:
            BASE_CMD += ' -W ' + self.password

    def run_shell_cmd(self, target_sql_list):
        cmd = BASE_CMD + ' -c \"'
        if self.schema:
            cmd += 'set current_schema = %s; ' % self.schema
        for target_sql in target_sql_list:
            cmd += target_sql + ';'
        cmd += '\"'
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        (stdout, stderr) = proc.communicate()
        stdout, stderr = stdout.decode(), stderr.decode()
        if 'gsql' in stderr or 'failed to connect' in stderr:
            raise ConnectionError("An error occurred while connecting to the database.\n"
                                  + "Details: " + stderr)
        return stdout

    @staticmethod
    def run_shell_sql_cmd(sql_file):
        cmd = BASE_CMD + ' -f ./' + sql_file
        try:
            ret = subprocess.check_output(shlex.split(cmd), stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            print(e.output, file=sys.stderr)

        return ret.decode()

    @staticmethod
    def parse_single_advisor_result(res, workload_table_name):
        table_index_dict = {}
        if len(res) > 2 and res[0:2] == ' (':
            items = res.split(',', 2)
            table = items[0][2:] + '.' + items[1]
            workload_table_name[items[0][2:]] = workload_table_name.get(items[0][2:], set())
            workload_table_name[items[0][2:]].add(items[1])
            indexes = re.split('[()]', items[2][:-1].strip('\"'))
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

        sql = self.make_single_advisor_sql(query)
        result = self.run_shell_cmd([sql]).split('\n')

        for res in result:
            table_index_dict.update(self.parse_single_advisor_result(res, workload_table_name))

        return table_index_dict

    # judge whether the index is used by the optimizer
    def query_index_check(self, query, query_index_dict, multi_node):
        valid_indexes = {}
        if len(query_index_dict) == 0:
            return valid_indexes

        # create hypo-indexes
        sql_list = ['SET enable_hypo_index = on;']
        if multi_node:
            sql_list.append('SET enable_fast_query_shipping = off;')
            sql_list.append('SET enable_stream_operator = on;')
        for table in query_index_dict.keys():
            for columns in query_index_dict[table]:
                if columns != '':
                    sql_list.append("SELECT hypopg_create_index('CREATE INDEX ON %s(%s)')" %
                                    (table, columns))
        sql_list.append('SELECT hypopg_display_index()')
        sql_list.append("SET explain_perf_mode = 'normal'; explain " + query)
        sql_list.append('SELECT hypopg_reset_index()')
        result = self.run_shell_cmd(sql_list).split('\n')

        # parse the result of explain plan
        hypoid_table_column = {}
        hypo_display = False
        for line in result:
            if hypo_display and 'btree' in line:
                hypo_index_info = line.split(',', 3)
                if len(hypo_index_info) == 4:
                    table_name = re.search(r'btree_(.*%s)' % hypo_index_info[2],
                                           hypo_index_info[0]).group(1)
                    match_flag, table_name = ExecuteFactory.match_table_name(table_name,
                                                                             query_index_dict)
                    if not match_flag:
                        return valid_indexes
                    hypoid_table_column[hypo_index_info[1]] = \
                        table_name + ':' + hypo_index_info[3].strip('"()')
            if hypo_display and re.search(r'\d+ rows', line):
                hypo_display = False
            if 'hypopg_display_index' in line:
                hypo_display = True
            if 'Index' in line and 'Scan' in line and 'btree' in line:
                super().get_valid_indexes(line, hypoid_table_column, valid_indexes)
        return valid_indexes

    def update_index_storage(self, index_id, index_config, hypo_index_num):
        index_size_sql = 'select * from hypopg_estimate_size(%s);' % index_id
        res = self.run_shell_cmd([index_size_sql]).split('\n')
        for line in res:
            if re.match(r'\d+', line.strip()):
                index_config[hypo_index_num].storage = float(line.strip()) / 1024 / 1024

    @staticmethod
    # parse the explain plan to get estimated cost by database optimizer
    def parse_plan_cost(plan):
        cost_total = -1

        plan_list = plan.split('\n')
        for line in plan_list:
            if '(cost=' in line:
                pattern = re.compile(r'\(cost=([^\)]*)\)', re.S)
                matched_res = re.search(pattern, line)
                if matched_res:
                    cost_list = matched_res.group(1).split()
                    if len(cost_list) == 3:
                        cost_total = float(cost_list[0].split('..')[-1])
                break

        return cost_total

    def parse_explain_plan(self, workload, index_config, res, ori_indexes_name, select_sql_pos):
        i = 0
        hypo_index_num = 0
        total_cost = 0
        found_plan = False
        hypo_index = False
        for line in res:
            if 'QUERY PLAN' in line:
                found_plan = True
            if 'ERROR' in line:
                if i >= len(select_sql_pos):
                    raise ValueError("The size of workload is not correct!")
                workload[select_sql_pos[i]].cost_list.append(0)
                i += 1
            if 'hypopg_create_index' in line:
                hypo_index = True
            if found_plan and '(cost=' in line:
                if i >= len(select_sql_pos):
                    raise ValueError("The size of workload is not correct!")
                query_cost = GSqlExecute.parse_plan_cost(line)
                query_cost *= workload[select_sql_pos[i]].frequency
                workload[select_sql_pos[i]].cost_list.append(query_cost)
                if index_config and len(index_config) == 1 and query_cost < workload[select_sql_pos[i]].cost_list[0]:
                    index_config[0].positive_pos.append(select_sql_pos[i])
                total_cost += query_cost
                found_plan = False
                i += 1
            if hypo_index:
                if 'btree' in line and self.max_index_storage:
                    hypo_index = False
                    self.update_index_storage(line, index_config, hypo_index_num)
                    hypo_index_num += 1
            if 'Index' in line and 'Scan' in line and not index_config:
                ind1, ind2 = re.search(r'Index.*Scan(.*)on ([^\s]+)',
                                       line.strip(), re.IGNORECASE).groups()
                if ind1.strip():
                    ori_indexes_name.add(ind1.strip().split(' ')[1])
                else:
                    ori_indexes_name.add(ind2)
        while i < len(select_sql_pos):
            workload[select_sql_pos[i]].cost_list.append(0)
            i += 1
        return total_cost

    def estimate_workload_cost_file(self, workload, index_config=None, ori_indexes_name=None):
        sql_file = str(time.time()) + '.sql'
        is_computed = False
        select_sql_pos = []
        with open(sql_file, 'w') as file:
            if self.schema:
                file.write('SET current_schema = %s;\n' % self.schema)
            if index_config:
                if len(index_config) == 1 and index_config[0].is_candidate:
                    is_computed = True
                # create hypo-indexes
                file.write('SET enable_hypo_index = on;\n')
                for index in index_config:
                    file.write("SELECT hypopg_create_index('CREATE INDEX ON %s(%s)');\n" %
                               (index.table, index.columns))
            if self.multi_node:
                file.write('set enable_fast_query_shipping = off;\n')
                file.write('set enable_stream_operator = on; \n')
            file.write("set explain_perf_mode = 'normal'; \n")
            for ind, query in enumerate(workload):
                if 'select ' not in query.statement.lower():
                    workload[ind].cost_list.append(0)
                else:
                    file.write('EXPLAIN ' + query.statement + ';\n')
                    select_sql_pos.append(ind)
                # record ineffective sql and negative sql for candidate indexes
                if is_computed:
                    super().record_ineffective_negative_sql(index_config[0], query, ind)

        result = self.run_shell_sql_cmd(sql_file).split('\n')
        if os.path.exists(sql_file):
            os.remove(sql_file)

        # parse the result of explain plans
        total_cost = self.parse_explain_plan(workload, index_config, result,
                                             ori_indexes_name, select_sql_pos)
        if index_config:
            self.run_shell_cmd(['SELECT hypopg_reset_index();'])

        return total_cost

    def check_useless_index(self, tables, history_indexes, history_invalid_indexes):
        schemas = [elem.lower() for elem in filter(None, self.schema.split(','))]
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
                  "ON n.oid = c.relnamespace LEFT JOIN pg_constraint p ON (i.oid = p.conindid " \
                  "AND p.contype = 'p') WHERE (c.relkind = ANY (ARRAY['r'::\"char\", " \
                  "'m'::\"char\"])) AND (i.relkind = ANY (ARRAY['i'::\"char\", 'I'::\"char\"])) " \
                  "AND n.nspname = '%s' AND c.relname in (%s) order by c.relname;" % \
                  (schema, tables_string)
            res = self.run_shell_cmd([sql]).split('\n')
            if not res:
                continue
            cur_table_indexes = list()
            for line in res:
                if 'tablename' in line or re.match(r'-+', line):
                    continue
                elif re.match(r'\(\d+ rows?\)', line):
                    continue
                elif '|' in line:
                    table, index, indexdef, pkey = [item.strip() for item in line.split('|')]
                    cur_columns = re.search(r'\(([^\(\)]*)\)', indexdef).group(1)
                    cur_index_obj = IndexInfo(schema, table, index, cur_columns, indexdef)
                    if pkey:
                        cur_index_obj.primary_key = True
                    # record all indexes
                    whole_indexes.append(cur_index_obj)
                    # match the last recommendation result
                    tbl_name = schema + '.' + table
                    if history_indexes.get(tbl_name):
                        matched_table_name.add(tbl_name)
                    super().match_last_result(tbl_name, cur_columns, history_indexes, history_invalid_indexes)
                    # record redundant indexes
                    if cur_table_indexes and cur_table_indexes[-1].table != table:
                        super().record_redundant_indexes(cur_table_indexes, redundant_indexes)
                        cur_table_indexes = []
                    cur_table_indexes.append(cur_index_obj)
            if cur_table_indexes:
                # record redundant indexes
                super().record_redundant_indexes(cur_table_indexes, redundant_indexes)
        # match history indexes to find invalid indexes
        check_table_list = list(set(history_indexes.keys()).difference(matched_table_name))
        sql_list = []
        for table in check_table_list:
            sql = "select indexdef from pg_indexes where schemaname='%s' and tablename='%s'" % \
                  (table.split('.')[0], table.split('.')[1])
            sql_list.append(sql)
        res = self.run_shell_cmd(sql_list).split('\n')
        for line in res:
            if 'CREATE INDEX' in line:
                cur_columns = re.search(r'\(([^\(\)]*)\)', line).group(1)
                super().match_last_result(table, cur_columns, history_indexes,
                                          history_invalid_indexes)
        return whole_indexes, redundant_indexes

