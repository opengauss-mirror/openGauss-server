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
from sqlalchemy import desc

from ._common import truncate_table
from ..business_db import get_session
from ..schema import RegularInspection


def insert_regular_inspection(inspection_type, start, end, report=None, conclusion=None):
    with get_session() as session:
        session.add(
            RegularInspection(
                inspection_type=inspection_type,
                start=start,
                end=end,
                report=report,
                conclusion=conclusion
            )
        )


def truncate_knob_regular_inspections():
    truncate_table(RegularInspection.__tablename__)


def count_knob_regular_inspections():
    with get_session() as session:
        result = session.query(RegularInspection)
        return result.count()


def select_knob_regular_inspections(inspection_type, limit=None):
    with get_session() as session:
        result = session.query(RegularInspection)
        if inspection_type:
            result = result.filter(RegularInspection.inspection_type == inspection_type)
        if limit is None:
            result = result.order_by(desc(RegularInspection.id))
        else:
            result = result.order_by(desc(RegularInspection.id)).limit(limit)
        return result
