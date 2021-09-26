#!/usr/bin/python3
# -*- coding: utf-8 -*-
#############################################################################
# Copyright (c): 2021, Huawei Tech. Co., Ltd.
# FileName     : agent_collect.py
# Version      :
# Date         : 2021-4-7
# Description  : Receives and stores agent data.
#############################################################################

try:
    import sys
    import os
    from flask import request, Response
    from flask_restful import Resource

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../"))
    from common.logger import CreateLogger
    from service.datafactory.storage.insert_data_to_database import SaveData
except ImportError as err:
    sys.exit("agent_collect.py: Failed to import module: %s." % str(err))

LOGGER = CreateLogger("debug", "server.log").create_log()


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

    def __init__(self):
        pass

    @staticmethod
    def post():
        content = request.json
        client_ip = request.remote_addr
        LOGGER.info("Successfully received request from: %s." % client_ip)
        try:
            insert_db = SaveData(LOGGER)
            insert_db.run(content)
            return ResponseTuple.success()
        except Exception as e:
            return ResponseTuple.error(msg=str(e), status_code=Response.status_code)

    @staticmethod
    def get():
        return ResponseTuple.success(result="Server service is normal.")

    @staticmethod
    def delete():
        return ResponseTuple.error(status_code=400)
