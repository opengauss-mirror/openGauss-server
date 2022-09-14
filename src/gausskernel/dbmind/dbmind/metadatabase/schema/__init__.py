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
from .existing_index import ExistingIndexes
from .forecasting_metrics import ForecastingMetrics
from .future_alarms import FutureAlarms
from .healing_records import HealingRecords
from .history_alarms import HistoryAlarms
from .index_recomm import IndexRecommendation
from .index_recomm_stats import IndexRecommendationStats
from .index_recomm_stmt_details import IndexRecommendationStmtDetails
from .index_recomm_stmt_templates import IndexRecommendationStmtTemplates
from .knob_recomm_details import KnobRecommendationDetails
from .knob_recomm_metric_snapshot import KnobRecommendationMetricSnapshot
from .knob_recomm_warnings import KnobRecommendationWarnings
from .slow_queries import SlowQueries
from .slow_queries_journal import SlowQueriesJournal
from .slow_queries_killed import SlowQueriesKilled
from .regular_inspections import RegularInspection
from .statistical_metric import StatisticalMetric


def load_all_schema_models():
    """Dummy function:
    Loading all the table schema models can be realized by ```import *```. This
    function only serves as a self-comment in the form and has no actual action. """
