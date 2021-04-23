"""
Copyright (c) 2020 Huawei Technologies Co.,Ltd.

openGauss is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:

         http://license.coscl.org.cn/MulanPSL2

THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
"""

from prettytable import PrettyTable

from tuner.character import OpenGaussMetric
from tuner.character import WORKLOAD_TYPE
from tuner.knob import Knob
from tuner.knob import RecommendedKnobs
from tuner.utils import GREEN_FMT, YELLOW_FMT, RED_FMT
from tuner.utils import cached_property
from tuner.utils import clip

# The basic storage unit is kB.
SIZE_UNIT_MAP = {"kB": 1,
                 "MB": 1024,
                 "GB": 1024 * 1024}

def round4(v):
    return v + (4 - v % 4)

def recommend_knobs(mode, metric):
    advisor = OpenGaussKnobAdvisor(metric)

    knobs = RecommendedKnobs()
    if mode == "recommend":
        if metric.uptime < 1:
            advisor.report.print_bad(
                "The database runs for a short period of time, and the database description may not be accumulated. "
                "The recommendation result may be inaccurate."
            )
        elif metric.uptime < 12:
            advisor.report.print_warn(
                "The database runs for a short period of time, and the database description may not be accumulated. "
                "The recommendation result may be inaccurate."
            )
        knobs.append_need_tune_knobs(advisor.shared_buffers,
                                     advisor.max_connections,
                                     advisor.max_prepared_transactions,
                                     advisor.work_mem,
                                     advisor.maintenance_work_mem,
                                     advisor.effective_cache_size,
                                     advisor.effective_io_concurrency,
                                     advisor.wal_buffers,
                                     advisor.random_page_cost,
                                     advisor.default_statistics_target)
    elif mode == "tune":
        knobs.append_need_tune_knobs(advisor.random_page_cost,
                                     advisor.effective_io_concurrency,
                                     advisor.work_mem)
        knobs.append_only_report_knobs(advisor.shared_buffers,
                                       advisor.max_connections,
                                       advisor.max_prepared_transactions,
                                       advisor.maintenance_work_mem,
                                       advisor.effective_cache_size,
                                       advisor.wal_buffers,
                                       advisor.default_statistics_target)
    elif mode == "train":
        knobs.append_need_tune_knobs(advisor.work_mem,
                                     advisor.shared_buffers)
        knobs.append_only_report_knobs(advisor.max_connections,
                                       advisor.max_prepared_transactions,
                                       advisor.maintenance_work_mem,
                                       advisor.effective_cache_size,
                                       advisor.effective_io_concurrency,
                                       advisor.wal_buffers,
                                       advisor.random_page_cost,
                                       advisor.default_statistics_target)

    knobs.report = advisor.report.generate
    return knobs


class ReportMsg:
    def __init__(self):
        self._info = dict()
        self._warn = set()
        self._bad = set()

    def print_info(self, _dict):
        self._info.update(_dict)

    def print_warn(self, msg):
        self._warn.add(msg)

    def print_bad(self, msg):
        self._bad.add(msg)

    @property
    def generate(self):
        outlines = list()
        if len(self._info) > 0:
            outlines.append(GREEN_FMT.format("INFO:"))
            info = PrettyTable()
            info.field_names = ["Metric", "Value"]
            for name, value in self._info.items():
                info.add_row([name, value])
            outlines.append(info.get_string(sortby='Value', sort_key=lambda x: str(x)))
            outlines.append("p.s: The unit of storage is kB.")
        if len(self._warn) > 0:
            outlines.append(YELLOW_FMT.format("WARN:"))
            for i, line in enumerate(self._warn):
                outlines.append("[%s]. " % i + line)
        if len(self._bad) > 0:
            outlines.append(RED_FMT.format("BAD:"))
            for i, line in enumerate(self._bad):
                outlines.append("[%s]. " % i + line)

        return "\n".join(outlines)


class OpenGaussKnobAdvisor:
    def __init__(self, metric):
        if not isinstance(metric, OpenGaussMetric):
            raise TypeError("The field type 'metric' is incorrect.")

        self.metric = metric
        self.report = ReportMsg()

        # Append metric and workload info to ReportMsg.
        self.report.print_info(self.metric.to_dict())

    # Allocation of memory or storage I/O resources.
    @cached_property
    def max_connections(self):
        max_conn = self.metric["max_connections"]
        cur_conn = self.metric.current_connections
        percent = cur_conn / max_conn
        last_1_min, last_5_min, last_15_min = self.metric.load_average
        recommend = max_conn

        if percent > 0.7:
            self.report.print_warn("Currently using more than 70% of the connections slots. "
                                   "Need increase max_connections.")
            if last_1_min > 5:
                self.report.print_bad(
                    "Your memory may not be enough to support the current service scenario. Upgrade the memory.")
            elif last_1_min > 3:
                self.report.print_bad(
                    "Your memory may not be enough to support the current service scenario. Upgrade the memory.")
            elif last_1_min > 1:
                recommend *= 2
            else:
                recommend *= 3
        elif percent > 0.9:
            self.report.print_bad("Currently using more than 90% of the connections slots. "
                                  "Need increase max_connections.")
            if last_1_min > 5:
                self.report.print_bad(
                    "Your memory may not be enough to support the current service scenario. Upgrade the memory.")
            elif last_1_min > 3:
                self.report.print_bad(
                    "Your memory may not be enough to support the current service scenario. Upgrade the memory.")
            elif last_1_min > 1:
                recommend *= 1.5
            else:
                recommend *= 3.5

        cores = self.metric.os_cpu_count
        if cores <= 8:
            self.report.print_warn("The number of CPU cores is a little small. "
                                   "Please do not run too high concurrency. "
                                   "You are recommended to set max_connections based on the number of CPU cores. "
                                   "If your job does not consume much CPU, you can also increase it.")

        if self.metric.workload_type == WORKLOAD_TYPE.TP:
            upper = max(500, cores * 8)
            lower = max(20, cores * 3)
            recommend = clip(recommend, max(20, cores * 5), max(100, cores * 7))

            return Knob.new_instance(name="max_connections",
                                     value_default=recommend,
                                     knob_type=Knob.TYPE.INT,
                                     value_max=upper,
                                     value_min=lower,
                                     restart=True)

        # Should be based on work_mem.
        if self.metric.os_mem_total > 16 * SIZE_UNIT_MAP["GB"]:
            remain_mem = self.metric.os_mem_total * 0.85 - self.shared_buffers.default
        elif self.metric.os_mem_total > 8 * SIZE_UNIT_MAP["GB"]:
            remain_mem = self.metric.os_mem_total * 0.75 - self.shared_buffers.default
        elif self.metric.os_mem_total < 4 * SIZE_UNIT_MAP["GB"]:
            remain_mem = self.metric.os_mem_total * 0.6 - self.shared_buffers.default
        else:
            remain_mem = self.metric.os_mem_total * 0.7 - self.shared_buffers.default

        # AP and HTAP
        # The value of work_mem is adapted based on the value of max_connections.
        work_mem = max(self.metric["work_mem"], self.metric.temp_file_size * 4)
        lower = max(15, cores * 3)
        recommend = max(remain_mem / (work_mem + 0.01), lower)
        return Knob.new_instance(name="max_connections",
                                 value_default=recommend,
                                 knob_type=Knob.TYPE.INT,
                                 value_max=recommend * 2,
                                 value_min=lower,
                                 restart=True)

    @property
    def max_prepared_transactions(self):
        """
        If you are using prepared transactions, you will probably want max_prepared_transactions to be at
        least as large as max_connections, so that every session can have a prepared transaction pending.
        """
        max_pt = self.metric["max_prepared_transactions"]
        max_conn = self.max_connections
        if max_pt == 0:
            self.report.print_warn("The value of max_prepared_transactions is 0, "
                                   "indicating that the two-phase commit function is not used.")
            return

        if max_pt < max_conn.default:
            self.report.print_bad("Most applications do not use XA prepared transactions, "
                                  "so should set the max_prepared_transactions to 0. "
                                  "If you do require prepared transactions, "
                                  "you should set this equal to max_connections to avoid blocking. "
                                  "May require increasing kernel memory parameters.")
            return Knob.new_instance(name="max_prepared_transactions",
                                     value_default=max_conn.default,
                                     knob_type=Knob.TYPE.INT,
                                     value_max=max_conn.max,
                                     value_min=max_conn.min,
                                     restart=True)

    @cached_property
    def shared_buffers(self):
        """If you have a dedicated database server with 1GB or more of RAM,
        a reasonable starting value for shared_buffers is 25% of the memory in your system.
        There are some workloads where even large settings for shared_buffers are effective,
        but because database also relies on the operating system cache,
        it is unlikely that an allocation of more than 40% of RAM to shared_buffers will
         work better than a smaller amount. """
        mem_total = self.metric.os_mem_total  # unit: kB
        if mem_total < 1 * SIZE_UNIT_MAP['GB']:
            default = 0.15 * mem_total
        elif mem_total > 8 * SIZE_UNIT_MAP['GB']:
            default = 0.4 * mem_total
        else:
            default = 0.25 * mem_total

        # The value of this knob means the number of maximum cached blocks.
        recommend = round4(default / self.metric.block_size)
        if self.metric.is_64bit:
            database_blocks = self.metric.all_database_size / self.metric.block_size
            if database_blocks < recommend:
                self.report.print_warn("The total size of all databases is less than the memory size. "
                                       "Therefore, it is unnecessary to set shared_buffers to a large value.")

            recommend = round4(min(database_blocks, recommend))
            upper = round4(recommend * 1.15)
            lower = round4(min(0.15 * mem_total / self.metric.block_size, recommend))

            return Knob.new_instance(name="shared_buffers",
                                     value_default=recommend,
                                     knob_type=Knob.TYPE.INT,
                                     value_max=upper,
                                     value_min=lower,
                                     restart=True)
        else:
            upper = round4(min(recommend, 2 * SIZE_UNIT_MAP["GB"] / self.metric.block_size))  # 32-bit OS only can use 2 GB mem.
            lower = round4(min(0.15 * mem_total / self.metric.block_size, recommend))
            return Knob.new_instance(name="shared_buffers",
                                     value_default=recommend,
                                     knob_type=Knob.TYPE.INT,
                                     value_max=upper,
                                     value_min=lower,
                                     restart=True)

    @property
    def work_mem(self):
        temp_file_size = self.metric.temp_file_size
        max_conn = self.max_connections.default

        # This knob does not need to be modified.
        if temp_file_size < 16 * SIZE_UNIT_MAP["MB"]:
            return

        if self.metric.workload_type == WORKLOAD_TYPE.TP:
            self.report.print_warn(
                "Perhaps the current scenario is not a mixed load. "
                "Ensure that the current environment supports sufficient concurrent complex query statements."
            )

            if max_conn > 500:
                return

            #  conservative operations
            recommend = (self.metric.os_mem_total - self.shared_buffers.default) / (max_conn * 2)
            upper = max(recommend, 256 * SIZE_UNIT_MAP["MB"])
            lower = min(recommend, 64 * SIZE_UNIT_MAP["MB"])
            return Knob.new_instance(name="work_mem",
                                     value_default=recommend,
                                     knob_type=Knob.TYPE.INT,
                                     value_max=upper,
                                     value_min=lower,
                                     restart=False)
        else:
            recommend = (self.metric.os_mem_total - self.shared_buffers.default) / max_conn
            upper = max(recommend, 1 * SIZE_UNIT_MAP["GB"])
            lower = min(recommend, 64 * SIZE_UNIT_MAP["MB"])
            return Knob.new_instance(name="work_mem",
                                     value_default=recommend,
                                     knob_type=Knob.TYPE.INT,
                                     value_max=upper,
                                     value_min=lower,
                                     restart=False)

    @property
    def maintenance_work_mem(self):
        pass

    @property
    def effective_cache_size(self):
        upper = self.metric.os_mem_total * 0.75
        lower = self.shared_buffers.default

        if self.metric.workload_type == WORKLOAD_TYPE.TP:
            return Knob.new_instance(name="effective_cache_size",
                                     value_default=lower,
                                     knob_type=Knob.TYPE.INT,
                                     value_max=upper,
                                     value_min=lower,
                                     restart=False)
        else:
            return Knob.new_instance(name="effective_cache_size",
                                     value_default=upper,
                                     knob_type=Knob.TYPE.INT,
                                     value_max=upper,
                                     value_min=lower,
                                     restart=False)

    @property
    def effective_io_concurrency(self):
        if self.metric.is_hdd:
            if 0 <= self.metric["effective_io_concurrency"] <= 2:  # No need for recommendation.
                return

            return Knob.new_instance(name="effective_io_concurrency",
                                     value_default=2,
                                     knob_type=Knob.TYPE.INT,
                                     value_max=4,
                                     value_min=0,
                                     restart=False)
        else:
            return Knob.new_instance(name="effective_io_concurrency",
                                     value_default=200,
                                     knob_type=Knob.TYPE.INT,
                                     value_max=250,
                                     value_min=150,
                                     restart=False)

    # Background writer.
    @property
    def wal_buffers(self):
        wal_buffers = self.metric["wal_buffers"]
        if wal_buffers == -1:
            return

        blocks_16m = 16 * SIZE_UNIT_MAP["MB"] / self.metric.block_size
        # Generally, this value is sufficient. A large value does not bring better performance.
        if wal_buffers >= blocks_16m:
            if wal_buffers > self.shared_buffers.default * 1 / 32:
                self.report.print_bad(
                    "The value of wal_buffers is too high. Generally, a large value does not bring better performance.")
                return Knob.new_instance(name="wal_buffers",
                                         value_default=self.shared_buffers.default * 1 / 32,
                                         knob_type=Knob.TYPE.INT,
                                         value_max=max(self.shared_buffers.default * 1 / 32, blocks_16m),
                                         value_min=min(self.shared_buffers.default * 1 / 64, blocks_16m),
                                         restart=True)
            else:
                self.report.print_warn(
                    "The value of wal_buffers is a bit high. Generally, an excessively large value does not bring "
                    "better performance. You can also set this parameter to -1. "
                    "The database automatically performs adaptation. "
                )
                return Knob.new_instance(name="wal_buffers",
                                         value_default=self.shared_buffers.default * 1 / 32,
                                         knob_type=Knob.TYPE.INT,
                                         value_max=self.shared_buffers.default * 1 / 32,
                                         value_min=blocks_16m,
                                         restart=True)
        elif wal_buffers < self.shared_buffers.default * 1 / 64:
            return Knob.new_instance(name="wal_buffers",
                                     value_default=-1,
                                     knob_type=Knob.TYPE.INT,
                                     value_max=-1,
                                     value_min=-1,
                                     restart=True)

    # Optimizer
    @property
    def random_page_cost(self):
        if self.metric.is_hdd:
            # Currently, with the rise of storage technology, the default value of 4 is too large.
            return Knob.new_instance(name="random_page_cost",
                                     value_default=3,
                                     knob_type=Knob.TYPE.FLOAT,
                                     value_max=3,
                                     value_min=2,
                                     restart=False)
        else:
            return Knob.new_instance(name="random_page_cost",
                                     value_default=1,
                                     knob_type=Knob.TYPE.FLOAT,
                                     value_max=2,
                                     value_min=1,
                                     restart=False)

    @property
    def default_statistics_target(self):
        workload_type = self.metric.workload_type
        search_modify_ratio = self.metric.search_modify_ratio
        read_write_ratio = self.metric.read_write_ratio
        fetch_return_ratio = self.metric.fetched_returned_ratio

        if workload_type == WORKLOAD_TYPE.AP:
            if search_modify_ratio > 20:
                recommend = 1000
            elif search_modify_ratio < 2:
                recommend = 100
            else:
                if fetch_return_ratio > 0.0005:
                    recommend = 200
                elif fetch_return_ratio > 0.0004:
                    recommend = 250
                elif fetch_return_ratio > 0.0003:
                    recommend = 350
                elif fetch_return_ratio > 0.0002:
                    recommend = 500
                elif fetch_return_ratio > 0.0001:
                    recommend = 600
                else:
                    recommend = 800
            return Knob.new_instance(name="default_statistics_target",
                                     value_default=recommend,
                                     knob_type=Knob.TYPE.INT,
                                     value_max=1000,
                                     value_min=100,
                                     restart=False)
        elif workload_type == WORKLOAD_TYPE.TP:
            if read_write_ratio < 0.5:
                recommend = 10
            elif read_write_ratio < 1:
                recommend = 50
            elif read_write_ratio < 2:
                recommend = 80
            else:
                recommend = 100

            return Knob.new_instance(name="default_statistics_target",
                                     value_default=recommend,
                                     knob_type=Knob.TYPE.INT,
                                     value_max=150,
                                     value_min=10,
                                     restart=False)
        else:
            return Knob.new_instance(name="default_statistics_target",
                                     value_default=100,
                                     knob_type=Knob.TYPE.INT,
                                     value_max=300,
                                     value_min=80,
                                     restart=False)
