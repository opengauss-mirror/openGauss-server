#!/usr/bin/python3
# -*- coding: utf-8 -*-
#############################################################################
# Copyright (c): 2021, Huawei Tech. Co., Ltd.
# FileName     : my_app.py
# Version      :
# Date         : 2021-4-7
# Description  : Server Service Management
#############################################################################

try:
    import sys
    import os
    import signal
    from flask import Flask
    from flask_restful import Api
    from configparser import ConfigParser

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
    from common.utils import Common
    from service.app import App
    from service.datafactory.collector.agent_collect import Source
except ImportError as err:
    sys.exit("my_app.py: Failed to import module, \nError: %s" % str(err))


class MyApp(App):

    def __init__(self, pid_file, logger):
        App.__init__(self)
        self.app = None
        self.api = None
        self.pid_file = pid_file
        self.logger = logger

    def initialize_app(self):
        self.app = Flask(__name__)
        self.app.config['debug'] = False
        self.api = Api(self.app)

    def add_resource(self):
        self.api.add_resource(Source, '/sink')

    def start_service(self, config_path):
        # check service is running or not.
        if os.path.isfile(self.pid_file):
            pid = Common.check_proc_exist("role server")
            if pid:
                raise Exception("Error: Process already running, can't start again.")
            else:
                os.remove(self.pid_file)
        # check config file exists
        if not os.path.isfile(config_path):
            raise Exception("Config file: %s does not exists." % config_path)
        # get listen host and port
        config = ConfigParser()
        config.read(config_path)
        listen_host = config.get("server", "listen_host")
        port = config.get("server", "listen_port")
        # write process pid to file
        if not os.path.isdir(os.path.dirname(self.pid_file)):
            os.makedirs(os.path.dirname(self.pid_file), 0o700)
        with open(self.pid_file, mode='w') as f:
            f.write(str(os.getpid()))
        # start service
        self.initialize_app()
        self.add_resource()
        try:
            context = Common.check_certificate_setting(self.logger, config_path, "server")
            self.logger.info("Start service...")
            self.app.run(host=listen_host, port=int(port), ssl_context=context)
            self.logger.warn("Service stopped, please check main.log for more information.")
        except (Exception, KeyboardInterrupt) as err_msg:
            self.logger.error(str(err_msg))
            raise Exception(err_msg)
        finally:
            if os.path.isfile(self.pid_file):
                os.remove(self.pid_file)

    def stop_service(self):
        try:
            if not os.path.isfile(self.pid_file):
                std = Common.check_proc_exist("role server")
                if not std:
                    raise Exception("ERROR: Process not running.")
                else:
                    kill_proc = "kill -9 %s" % std
                    Common.execute_cmd(kill_proc)
            else:
                with open(self.pid_file, mode='r') as f:
                    pid = int(f.read())
                    os.kill(pid, signal.SIGTERM)
                    os.remove(self.pid_file)
            self.logger.info("Successfully stopped server.")
        except Exception as err_msg:
            self.logger.error("Failed to stop service, Error: %s" % str(err_msg))
            sys.stdout.write("Error: " + str(err_msg) + "\n")
            if os.path.isfile(self.pid_file):
                os.remove(self.pid_file)
