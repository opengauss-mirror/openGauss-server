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
import logging
import os

from flask import request
from flask_restful import Resource

from detector.service.storage import sqlite_storage

service_logger = logging.getLogger('service')


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

    def __init__(self, database_dir, valid_dbname):
        self.database_dir = database_dir
        self.valid_dbname = valid_dbname

    def post(self):
        content = request.json
        try:
            flag = content.pop('flag')
            dbname = flag['host'] + ':' + str(flag['port'])
            timestamp = content.pop('timestamp')
            if dbname not in self.valid_dbname:
                # not in whitelist
                return ResponseTuple.error(status_code=400)
            os.makedirs(self.database_dir, exist_ok=True)
            database_path = os.path.join(self.database_dir, dbname)
            db_agent = sqlite_storage.SQLiteStorage(dbpath=database_path)
            db_agent.connect()
            db_agent.create_table()
            for table, event in content.items():
                # if do not collect data, then continue
                if not event:
                    continue
                if table == 'wdr':
                    for tup in event:
                        tup.insert(0, timestamp)
                        db_agent.insert(table, *tup)
                else:
                    event.insert(0, timestamp)
                    db_agent.insert(table, *event)
            db_agent.close()
            return ResponseTuple.success()
        except Exception as e:
            service_logger.error('Error when receive data from agent: ' + str(e), exc_info=True)
            return ResponseTuple.error(msg=str(e))

    def get(self):
        return ResponseTuple.success()

    def delete(self):
        return ResponseTuple.error(status_code=400)
