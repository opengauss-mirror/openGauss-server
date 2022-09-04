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

from sqlalchemy import func

from ._common import truncate_table
from ..business_db import get_session
from ..schema import ExistingIndexes
from ..schema import IndexRecommendation
from ..schema import IndexRecommendationStats
from ..schema import IndexRecommendationStmtDetails
from ..schema import IndexRecommendationStmtTemplates


def clear_data():
    truncate_table(ExistingIndexes.__tablename__)
    truncate_table(IndexRecommendation.__tablename__)
    truncate_table(IndexRecommendationStmtDetails.__tablename__)
    truncate_table(IndexRecommendationStmtTemplates.__tablename__)


def insert_recommendation_stat(host, db_name, stmt_count, positive_stmt_count,
                               table_count, rec_index_count,
                               redundant_index_count, invalid_index_count):
    with get_session() as session:
        session.add(IndexRecommendationStats(
            host=host,
            db_name=db_name,
            recommend_index_count=rec_index_count,
            redundant_index_count=redundant_index_count,
            invalid_index_count=invalid_index_count,
            stmt_count=stmt_count,
            positive_stmt_count=positive_stmt_count,
            table_count=table_count,
        ))


def get_latest_recommendation_stat():
    with get_session() as session:
        return session.query(IndexRecommendationStats).filter(
            IndexRecommendationStats.occurrence_time == func.max(
                IndexRecommendationStats.occurrence_time).select())


def get_recommendation_stat():
    with get_session() as session:
        recommendation_stat = session.query(IndexRecommendationStats)
    return recommendation_stat


def get_advised_index():
    with get_session() as session:
        advised_index = session.query(IndexRecommendation).filter(IndexRecommendation.index_type == 1)
    return advised_index


def get_advised_index_details():
    with get_session() as session:
        return session.query(IndexRecommendationStmtDetails, IndexRecommendationStmtTemplates,
                             IndexRecommendation).filter(
            IndexRecommendationStmtDetails.template_id == IndexRecommendationStmtTemplates.id).filter(
            IndexRecommendationStmtDetails.index_id == IndexRecommendation.id).filter(
            IndexRecommendationStmtDetails.correlation_type == 0)


def get_existing_indexes():
    with get_session() as session:
        return session.query(ExistingIndexes)


def insert_existing_index(host, db_name, tb_name, columns, index_stmt):
    with get_session() as session:
        session.add(ExistingIndexes(host=host,
                                    db_name=db_name,
                                    tb_name=tb_name,
                                    columns=columns,
                                    index_stmt=index_stmt))


def insert_recommendation(host, db_name, schema_name, tb_name, columns, index_type, index_stmt, optimized=None,
                          stmt_count=None, select_ratio=None, insert_ratio=None, update_ratio=None,
                          delete_ratio=None):
    with get_session() as session:
        session.add(IndexRecommendation(host=host,
                                        db_name=db_name,
                                        schema_name=schema_name,
                                        tb_name=tb_name,
                                        columns=columns,
                                        optimized=optimized,
                                        index_type=index_type,
                                        stmt_count=stmt_count,
                                        select_ratio=select_ratio,
                                        insert_ratio=insert_ratio,
                                        update_ratio=update_ratio,
                                        delete_ratio=delete_ratio,
                                        index_stmt=index_stmt))


def get_template_count():
    with get_session() as session:
        return session.query(IndexRecommendationStmtTemplates).count()


def insert_recommendation_stmt_details(template_id, db_name, stmt, optimized, correlation_type, stmt_count):
    with get_session() as session:
        session.add(IndexRecommendationStmtDetails(
            index_id=session.query(IndexRecommendation).count(),
            template_id=template_id,
            db_name=db_name,
            stmt=stmt,
            optimized=optimized,
            correlation_type=correlation_type,
            stmt_count=stmt_count
        ))


def insert_recommendation_stmt_templates(template, db_name):
    with get_session() as session:
        session.add(IndexRecommendationStmtTemplates(
            db_name=db_name,
            template=template
        ))
