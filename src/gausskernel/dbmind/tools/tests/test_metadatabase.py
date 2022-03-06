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
import os
import time

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dbmind.constants import DYNAMIC_CONFIG
from dbmind.metadatabase import business_db, Base
from dbmind.metadatabase import create_dynamic_config_schema
from dbmind.metadatabase.dao.dynamic_config import dynamic_config_get, dynamic_config_set
from dbmind.metadatabase.dao.forecasting_metrics import *
from dbmind.metadatabase.dao.slow_queries import *

# Clear the last testing db.
os.path.exists('test_metadatabase.db') and os.remove('test_metadatabase.db')
os.path.exists(DYNAMIC_CONFIG) and os.remove(DYNAMIC_CONFIG)

engine = create_engine('sqlite:///test_metadatabase.db')
session_maker = sessionmaker(autocommit=False, autoflush=False, bind=engine)

business_db.session_clz.update(
    engine=engine,
    session_maker=session_maker,
    db_type='sqlite'
)

Base.metadata.create_all(engine)


def test_slow_queries():
    insert_slow_query('schema', 'db0', 'query0', int(time.time() * 1000), 10)
    insert_slow_query('schema', 'db0', 'query1', int(time.time() * 1000), 11)
    insert_slow_query('schema', 'db0', 'query1', int(time.time() * 1000), 11)
    insert_slow_query('schema', 'db0', 'query1', int(time.time() * 1000), 11)
    count = 0
    for query in select_slow_queries():
        count += 1
        assert query.schema_name == 'schema'
        assert query.db_name == 'db0'
        assert query.start_at <= int(time.time() * 1000)
    assert count == count_slow_queries()
    truncate_slow_queries()
    assert count_slow_queries() == 0


def test_forecasting_metrics():
    truncate_forecasting_metrics()  # clear

    batch_insert_forecasting_metric(
        metric_name='metric0',
        host_ip='127.0.0.1',
        metric_value=tuple(range(0, 1000)),
        metric_time=tuple(range(0, 1000))
    )
    batch_insert_forecasting_metric(
        metric_name='metric1',
        host_ip='127.0.0.1',
        metric_value=tuple(range(0, 1000)),
        metric_time=tuple(range(0, 1000))
    )
    batch_insert_forecasting_metric(
        metric_name='metric2',
        host_ip='127.0.0.1',
        metric_value=tuple(range(0, 1000)),
        metric_time=tuple(range(0, 1000))
    )
    assert count_forecasting_metric(metric_name='metric0') == 1000
    assert count_forecasting_metric() == 1000 * 3
    for i, metric in enumerate(select_forecasting_metric(
            metric_name='metric1', host_ip='127.0.0.1',
            min_metric_time=500, max_metric_time=800
    )):
        assert metric.metric_value == 500 + i
    delete_timeout_forecasting_metrics(oldest_metric_time=500)
    assert count_forecasting_metric() == 1000 * 3 // 2
    truncate_forecasting_metrics()
    assert count_forecasting_metric() == 0


def test_dynamic_config_db():
    create_dynamic_config_schema()
    assert dynamic_config_get('slow_sql_threshold', 'cpu_usage_limit') == 0.5
    dynamic_config_set('slow_sql_threshold', 'cpu_usage_limit', 1)
    assert dynamic_config_get('slow_sql_threshold', 'cpu_usage_limit') == 1

    dynamic_config_set('slow_sql_threshold', 'no_this_name', 1)
    assert dynamic_config_get('slow_sql_threshold', 'no_this_name') == 1

    try:
        dynamic_config_set('no_this_table', 'no_this_name', 1)
    except AssertionError:
        pass
    else:
        assert False
