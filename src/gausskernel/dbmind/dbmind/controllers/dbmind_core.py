# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
"""This is only a template file that helps
 users implement the web interfaces for DBMind.
 And some implementations are only demonstrations.
"""
import time

from pydantic import BaseModel

from dbmind.common.http import request_mapping, OAuth2
from dbmind.common.http import standardized_api_output
from dbmind.service import web

# authorization
token_url = '/api/token'
oauth2 = OAuth2.get_instance(token_url, web.check_credential, ttl=600)
request_mapping(token_url, methods=['POST'], api=True)(oauth2.login_handler)


@request_mapping('/api/status/running', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_running_status():
    return web.get_running_status()


@request_mapping('/api/status/transaction', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_xact_status():
    return web.get_xact_status()


@request_mapping('/api/status/alert', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_alert():
    return web.get_latest_alert()


@request_mapping('/api/status/node', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_node_status():
    return web.get_cluster_node_status()


@request_mapping('/api/status/host', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_host_status():
    return web.get_host_status()


@request_mapping('/api/summary/cluster', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_cluster_summary():
    return web.get_cluster_summary()


@request_mapping('/api/list/metric', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_all_metrics():
    return web.get_all_metrics()


@request_mapping('/api/sequence/{name}', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_metric_sequence(name, start: int = None, end: int = None, step: int = None):
    return web.get_metric_sequence(name, start, end, step)


@request_mapping('/api/summary/workload_forecasting', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def workload_forecasting_summary():
    return web.get_forecast_sequence_info(metric_name=None)


@request_mapping('/api/workload_forecasting/sequence/{name}', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def workload_forecasting_get_metric_sequence(name, start: int = None, end: int = None, step: int = None):
    return web.get_metric_sequence(name, start, end, step)


@request_mapping('/api/workload_forecasting/sequence/forecast/{name}', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def workload_forecasting_forecast(name: str, start: int = None, end: int = None, step: int = None):
    if start is None and end is None:
        r = web.get_forecast_sequence_info(name)['rows']
        if len(r) > 0:
            rv = web.get_stored_forecast_sequence(name, start_at=int(time.time() * 1000), limit=500)
            return rv
    return web.get_metric_forecast_sequence(name, start, end, step)


@request_mapping('/api/alarm/history', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_history_alarms(host: str = None, alarm_type: str = None, alarm_level: str = None, group: bool = False):
    return web.get_history_alarms(host, alarm_type, alarm_level, group)


@request_mapping('/api/alarm/future', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_future_alarms(metric_name: str = None, host: str = None, start: int = None, group: bool = False):
    return web.get_future_alarms(metric_name, host, start, group)


@request_mapping('/api/alarm/healing', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_healing_info_for_alarms(action: str = None, success: bool = None, min_occurrence: int = None):
    return web.get_healing_info(action, success, min_occurrence)


@request_mapping('/api/query/slow/recent', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_recent_slow_queries(
        query: str = None, start: int = None, end: int = None, limit: int = None, group: bool = False
):
    return web.get_slow_queries(query, start, end, limit, group)


@request_mapping('/api/query/slow/killed', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_killed_slow_queries(query: str = None, start: int = None, end: int = None, limit: int = None):
    return web.get_killed_slow_queries(query, start, end, limit)


@request_mapping('/api/query/top', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_top_queries():
    username, password = oauth2.credential
    return web.get_top_queries(username, password)


@request_mapping('/api/query/active', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_active_queries():
    username, password = oauth2.credential
    return web.get_active_query(username, password)


@request_mapping('/api/query/locking', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_locking_queries():
    username, password = oauth2.credential
    return web.get_holding_lock_query(username, password)


@request_mapping('/api/list/database', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_db_list():
    return web.get_database_list()


@request_mapping('/api/summary/index_advisor', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_index_advisor_summary():
    return web.get_index_advisor_summary()


@request_mapping('/api/summary/knob_tuning', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_knob_tuning_summary():
    return web.toolkit_recommend_knobs_by_metrics()


@request_mapping('/api/summary/slow_query', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_slow_query_summary():
    return web.get_slow_query_summary()


@request_mapping('/api/summary/slow_query/projection', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_security_summary():
    return {
        'dbname': {
            'x': [],
            'y': []
        }
    }


@request_mapping('/api/summary/security', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_security_summary(host: str = None):
    return web.get_security_alarms(host)


@request_mapping('/api/summary/log', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_log_information():
    import datetime

    text = """\
    testr postgres [local] 140714833868544 0[0:0#0]  0 [BACKEND] FATAL:  Invalid username/password,login denied.
    sectest postgres [local] 140714865325824 0[0:0#0]  2251799813687293 [BACKEND] ERROR:  SQL_INJECTION: TAUTOLOGY
    """
    lines = text.splitlines()
    timestamp = int(time.time()) - len(lines) * 10
    for i, line in enumerate(lines):
        lines[i] = datetime.datetime.fromtimestamp(timestamp + i * 10).strftime('%Y-%m-%d %H:%M:%S.%f') + ' ' \
                   + line.strip()
    return '\n'.join(lines)


@request_mapping('/api/toolkit/advise/index', methods=['POST'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def advise_indexes(database: str, sqls: list):
    return web.toolkit_index_advise(database, sqls)


@request_mapping('/api/toolkit/advise/query', methods=['POST'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def tune_query(database: str, sql: str,
               use_rewrite: bool = True,
               use_hinter: bool = True,
               use_materialized: bool = True):
    return web.toolkit_rewrite_sql(database, sql)


@request_mapping('/api/toolkit/predict/query', methods=['POST'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def sqldiag(database: str, sql: str):
    return {
        'cost_time': 1.1,
        'coordinate': [1.1, 2.1]
    }


@request_mapping('/api/toolkit/forecast/sequence/{name}', methods=['POST', 'GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def forecast(name: str, start: int = None, end: int = None, step: int = None):
    return web.get_metric_forecast_sequence(name, start, end, step)


@request_mapping('/api/setting/set', methods=['POST', 'GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def set_setting(config: str, name: str, value: str, dynamic: bool = True):
    if dynamic:
        if '' in (config.strip(), name.strip(), value.strip()):
            raise Exception('You should input correct setting.')
        web.global_vars.dynamic_configs.set(config, name, value)
        return 'success'
    else:
        raise Exception('Currently, DBMind cannot modify the static configurations. '
                        'You should modify the configuration by using the command line or'
                        ' text editor.')


@request_mapping('/api/setting/get', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_setting(config: str, name: str, dynamic: bool = True):
    if dynamic:
        return web.global_vars.dynamic_configs.get(config, name)
    else:
        raise Exception("Currently, DBMind doesn't support showing the static "
                        "configurations due to security. Instead, you should "
                        "log in to the deployment machine and "
                        "see the configuration file.")


@request_mapping('/api/setting/list', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def list_setting():
    return {'dynamic': web.global_vars.dynamic_configs.list()}


class SlowSQLItem(BaseModel):
    sql: str
    database: str
    schemaname: str
    start_time: str
    end_time: str
    wdr: str


@request_mapping('/api/toolkit/slow_sql_rca', methods=['POST'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def diagnosis_slow_sql(item: SlowSQLItem):
    sql = item.sql if len(item.sql) else None
    database = item.database if len(item.database) else None
    schema = item.schemaname if len(item.schemaname) else 'public'
    start_time = item.start_time if len(item.start_time) else None
    end_time = item.end_time if len(item.end_time) else None
    wdr = item.wdr if len(item.wdr) else None
    return web.toolkit_slow_sql_rca(sql,
                                    database=database,
                                    schema=schema,
                                    start_time=start_time,
                                    end_time=end_time,
                                    wdr=wdr)


@request_mapping('/api/summary/metric_statistic', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_metric_statistic():
    return web.get_metric_statistic()


@request_mapping('/api/summary/regular_inspections', methods=['GET'], api=True)
@oauth2.token_authentication()
@standardized_api_output
def get_regular_inspections(inspection_type):
    return web.get_regular_inspections(inspection_type)
