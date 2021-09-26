#!/usr/bin/python3
# -*- coding: utf-8 -*-
#############################################################################
# Copyright (c): 2021, Huawei Tech. Co., Ltd.
# FileName     : main.py
# Version      :
# Date         : 2021-4-7
# Description  : Function entry file
#############################################################################

try:
    import sys
    import os
    import argparse

    sys.path.insert(0, os.path.dirname(__file__))
    from common.utils import Common, CONFIG_PATH
    from common.logger import CreateLogger
except ImportError as err:
    sys.exit("main.py: Failed to import module: %s." % str(err))

LOGGER = CreateLogger("debug", "start_service.log").create_log()

current_dirname = os.path.dirname(os.path.realpath(__file__))

__version__ = '1.0.0'
__description__ = 'anomaly_detection: anomaly detection tool.'
__epilog__ = """
epilog:
     the 'a-detection.conf' will be read when the program is running,
     the location of them is:
     dbmind.conf: {detection}.
     """.format(detection=CONFIG_PATH)


def usage():
    usage_message = """
        # start service.
        python main.py start [--role {{agent,server}}]
        # stop service.
        python main.py stop [--role {{agent,server}}]
        """
    return usage_message


def parse_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description=__description__,
                                     usage=usage(),
                                     epilog=__epilog__)

    parser.add_argument('mode', choices=['start', 'stop'])
    parser.add_argument('--role', choices=['agent', 'server'],
                        help="Run as 'agent', 'server'. "
                             "notes: ensure the normal operation of the openGauss in agent.")
    parser.add_argument('-v', '--version', action='version')
    parser.version = __version__
    return parser.parse_args()


def manage_service(args):
    server_pid_file = os.path.join(current_dirname, './tmp/server.pid')
    agent_pid_file = os.path.join(current_dirname, './tmp/agent.pid')
    if args.role == 'server':
        from service.my_app import MyApp
        if args.mode == 'start':
            MyApp(server_pid_file, LOGGER).start_service(CONFIG_PATH)
        else:
            MyApp(server_pid_file, LOGGER).stop_service()
    elif args.role == 'agent':
        from agent.manage_agent import Agent
        if args.mode == 'start':
            get_data_path = "ps -ux | grep -v grep | grep gaussdb"
            std, _ = Common.execute_cmd(get_data_path)
            if not std:
                raise Exception("The GaussDb process does not exists, please check it.")
            Agent(agent_pid_file, LOGGER).start_agent(CONFIG_PATH)
        else:
            Agent(agent_pid_file, LOGGER).stop_agent()
    else:
        print('FATAL: incorrect parameter.')
        print(usage())
        return -1


def main():
    args = parse_args()
    if args.mode in ('start', 'stop') and args.role:
        try:
            manage_service(args)
        except Exception as err_msg:
            print(err_msg)
            sys.exit(1)
    else:
        print("FATAL: incorrect parameter.")
        print(usage())
        return -1


if __name__ == '__main__':
    main()
