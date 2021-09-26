#!/usr/bin/python3
# -*- coding: utf-8 -*-
#############################################################################
# Copyright (c): 2021, Huawei Tech. Co., Ltd.
# FileName     : manage_agent.py
# Version      :
# Date         : 2021-4-7
# Description  : Agent service management
#############################################################################

try:
    import sys
    import os
    import ssl
    import signal
    from configparser import ConfigParser, NoOptionError, NoSectionError

    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../"))
    from agent.task.os_exporter import OSExporter
    from agent.task.database_exporter import DatabaseExporter
    from common.utils import Common
    from common.logger import CreateLogger
    from agent.channel import ChannelManager
    from agent.db_source import DBSource
    from agent.http_sink import HttpSink
except ImportError as err:
    sys.exit("manage_agent.py: Failed to import module: %s." % str(err))

# LOGGER is start log object
LOGGER = CreateLogger("debug", "agent.log").create_log()


class Agent:
    def __init__(self, pid_file, logger):
        self.pid_file = pid_file
        # logger is agent service log object
        self.logger = logger

    def check_agent_parameter(self, config):
        """
        Check if the agent parameter is valid, if the parameter is valid,
        then return parameters dict, otherwise exit process.
        :param config: config handler for config file.
        :return: agent parameters dict.
        """
        agent_parameters = {}
        try:
            host = config.get('server', 'host')
            listen_port = config.get('server', 'listen_port')
            Common.check_ip_and_port(host, listen_port)
        except (NoOptionError, NoSectionError) as e:
            self.logger.error(e)
            sys.exit(1)
        else:
            agent_parameters['host'] = host
            agent_parameters['listen_port'] = listen_port

        default_agent_parameter_dicts = {'sink_timer_interval': '10S',
                                         'source_timer_interval': '10S',
                                         'channel_capacity': 1000}
        for parameter, default_value in default_agent_parameter_dicts.items():
            try:
                if parameter == 'channel_capacity':
                    agent_parameter_value = config.getint('agent', parameter)
                    agent_parameters[parameter] = agent_parameter_value
                else:
                    agent_parameter_value = config.get('agent', parameter)
                    agent_parameters[parameter] = Common.transform_time_string(
                        agent_parameter_value, mode='to_second')
            except Exception as e:
                self.logger.error("error occur when acquire %s: %s, use default_value: %s" % (
                    parameter, str(e), default_value))
                agent_parameters[parameter] = default_agent_parameter_dicts[parameter]

        return agent_parameters

    def start_agent(self, config_path):
        """
        Start agent service.
        :param config_path: string, config path.
        :return: NA
        """
        if not os.path.isfile(config_path):
            raise Exception('Config file: %s does not exists.' % config_path)
        # check agent is running or not.
        if os.path.isfile(self.pid_file):
            pid = Common.check_proc_exist("role agent")
            if pid:
                self.logger.warn("Process already exist, pid:[%s]" % pid)
                raise Exception("Process already running, can't start again.")
            else:
                os.remove(self.pid_file)
        # write process pid to file
        if not os.path.isdir(os.path.dirname(self.pid_file)):
            os.makedirs(os.path.dirname(self.pid_file), 0o700)
        with open(self.pid_file, mode='w') as f:
            f.write(str(os.getpid()))
        try:
            config = ConfigParser()
            config.read(config_path)

            collection_type = config.get("agent", "collection_type")

            agent_parameters = self.check_agent_parameter(config)
            context = Common.check_certificate_setting(self.logger, config_path, "agent")

            protocol = "http"
            if context is not None:
                protocol = "https"
            url = "%s://" % protocol + agent_parameters["host"] + ":" + \
                  agent_parameters["listen_port"] + "/sink"

            chan = ChannelManager(LOGGER)
            source = DBSource()
            http_sink = HttpSink(interval=agent_parameters['sink_timer_interval'], url=url,
                                 context=context, logger=LOGGER)
            source.channel_manager = chan
            http_sink.channel_manager = chan

            if collection_type == "all":
                tasks = [("OSExporter", OSExporter(LOGGER).__call__),
                         ("DatabaseExporter", DatabaseExporter(LOGGER).__call__)]
            elif collection_type == "os":
                tasks = [("OSExporter", OSExporter(LOGGER).__call__)]
            else:
                tasks = [("DatabaseExporter", DatabaseExporter(LOGGER).__call__)]

            for task_name, task_func in tasks:
                source.add_task(name=task_name,
                                interval=agent_parameters['source_timer_interval'],
                                task=task_func,
                                maxsize=agent_parameters['channel_capacity'],
                                logger=LOGGER)
            try:
                # start to collect data
                source.start()
            except Exception as e:
                self.logger.error("Failed to start agent task, Error: %s." % e)
                raise Exception("Failed to start agent, Error: %s." % e)
            # push data to server
            http_sink.start()
        except Exception as err_msg:
            self.logger.error(str(err_msg))
            sys.stdout.write("Error: " + str(err_msg) + "\n")
        except KeyboardInterrupt:
            self.logger.warn("Keyboard exception is received. The process ends.")
        finally:
            if os.path.isfile(self.pid_file):
                os.remove(self.pid_file)

    def stop_agent(self):
        try:
            if not os.path.exists(self.pid_file):
                self.logger.warn("The pid file does not exists.")
                std = Common.check_proc_exist("role agent")
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
            self.logger.info("Successfully stopped agent.")
        except Exception as e:
            self.logger.error("Failed to stop agent, Error: %s" % str(e))
            sys.stdout.write("Error: " + str(e) + "\n")
            if os.path.exists(self.pid_file):
                os.remove(self.pid_file)
