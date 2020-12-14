from flask import request
from flask_restful import Resource

from ..server_logger import logger


class ResponseTuple:
    """generate a response tuple."""

    @staticmethod
    def success(result=None):
        if result is None:
            return {"status": "success"}, 200

        return {"status": "success", "result": result}

    @staticmethod
    def error(msg="", status_code=400):
        return {"status": "error", "msg": msg}, status_code


class Source(Resource):

    def __init__(self, db, table_class_relation):
        self.db = db
        self.table_class_relation = table_class_relation

    def post(self):
        content = request.json
        try:
            for name, event in content.items():
                tup_obj = self.table_class_relation[name](timestamp=event['timestamp'], value=event['value'])
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


class Index(Resource):
    def get(self):
        return ResponseTuple.success()

    def delete(self):
        return ResponseTuple.error(status_code=400)
