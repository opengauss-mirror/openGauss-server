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
from flask import request
from flask_restful import Resource

from ..server_logger import logger


class ResponseTuple:
    """
    This class is used for generating a response tuple.
    """

    @staticmethod
    def success(result=None):
        if result is None:
            return {"status": "success"}, 200

        return {"status": "success", "result": result}

    @staticmethod
    def error(msg="", status_code=400):
        return {"status": "error", "msg": msg}, status_code


class Source(Resource):
    """
    This class is used for acquiring metric data from agent and save data 
    in sqlite database.
    """

    def __init__(self, db, table_class_relation):
        self.db = db
        self.table_class_relation = table_class_relation

    def post(self):
        content = request.json
        try:
            for metric_name, event in content.items():
                if metric_name in self.table_class_relation:
                    tup_obj = self.table_class_relation[metric_name](timestamp=event['timestamp'], value=event['value'])
                    self.db.session.add(tup_obj)
            self.db.session.commit()
            return ResponseTuple.success()
        except Exception as e:
            logger.error('error when receive data from agent: ' + str(e))
            self.db.session.rollback()
            return ResponseTuple.error(msg=str(e))

    def get(self):
        return ResponseTuple.error(status_code=400)

    def delete(self):
        return ResponseTuple.error(status_code=400)
