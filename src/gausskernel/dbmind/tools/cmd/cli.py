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
"""Command Line Interface"""

import argparse
import os

from dbmind import components as components_module
from dbmind.common.exceptions import SetupError, ConfigSettingError
from dbmind.constants import __description__, __version__
from . import edbmind
from . import setup
from . import config_utils
from .. import constants
from .. import global_vars


def build_parser():
    actions = ['setup', 'start', 'stop']
    # Create the top-level parser to parse the common action.
    parser = argparse.ArgumentParser(
        description=__description__
    )
    parser.add_argument('--version', action='version', version=__version__)

    # Add sub-commands:
    subparsers = parser.add_subparsers(title='available subcommands',
                                       help="type '<subcommand> -h' for help on a specific subcommand",
                                       dest='subcommand')
    # Create the parser for the "service" command.
    parser_service = subparsers.add_parser('service', help='send a command to DBMind to change the status of '
                                                           'the service')
    parser_service.add_argument('action', choices=actions, help='perform an action for service')
    parser_service.add_argument('-c', '--conf', metavar='DIRECTORY', required=True, type=os.path.realpath,
                                help='set the directory of configuration files')
    parser_service.add_argument('--only-run', choices=constants.TIMED_TASK_NAMES,
                                help='explicitly set a certain task running in the backend')
    config_mode_group = parser_service.add_mutually_exclusive_group()
    config_mode_group.add_argument('--interactive', action='store_true',
                                   help='configure and initialize with interactive mode')
    config_mode_group.add_argument('--initialize', action='store_true',
                                   help='initialize and check configurations after configuring.')

    # Create the parser for the "set" command.
    parser_set = subparsers.add_parser('set', help='set a parameter')
    parser_set.add_argument('section', help='which section (case sensitive) to set')
    parser_set.add_argument('option', help='which option to set')
    parser_set.add_argument('target', help='the parameter target to set')
    parser_set.add_argument('-c', '--conf', metavar='DIRECTORY', required=True,
                            help='set the directory of configuration files')

    # Create the parser for the "component" command.
    # This component includes Prometheus-exporter and other components that can be
    # run independently through the command line.
    # Components can be easily extended, similar to a plug-in.
    # The component need to be called can import DBMind packages directly.
    components = components_module.list_components()
    parser_component = subparsers.add_parser('component',
                                             help='pass command line arguments to each sub-component.')
    parser_component.add_argument('name', metavar='COMPONENT_NAME', choices=components,
                                  help='choice a component to start. '
                                       + str(components))
    parser_component.add_argument('arguments', metavar='ARGS', nargs=argparse.REMAINDER,
                                  help='arguments for the component to start')
    return parser


class DBMindRun:
    """Helper class to use as main for DBMind:

    DBMindRun(*sys.argv[1:])
    """

    def __init__(self, argv):
        os.umask(0o0077)

        parser = build_parser()
        args = parser.parse_args(argv)
        try:
            if args.subcommand == 'service':
                if args.action == 'setup':
                    if args.interactive:
                        setup.setup_directory_interactive(args.conf)
                    elif args.initialize:
                        setup.initialize_and_check_config(args.conf, interactive=False)
                    else:
                        setup.setup_directory(args.conf)
                elif args.action == 'start':
                    # Determine which task runs in the backend.
                    if args.only_run is None:
                        global_vars.backend_timed_task.extend(constants.TIMED_TASK_NAMES)
                    else:
                        global_vars.backend_timed_task.append(args.only_run)
                    edbmind.DBMindMain(args.conf).start()
                elif args.action == 'stop':
                    edbmind.DBMindMain(args.conf).stop(level='mid')
                elif args.action == 'reload':
                    edbmind.DBMindMain(args.conf).reload()
                else:
                    parser.print_usage()
            elif args.subcommand == 'show':
                pass
            elif args.subcommand == 'set':
                config_utils.set_config_parameter(args.conf, args.section, args.option, args.target)
            elif args.subcommand == 'component':
                components_module.call_component(args.name, args.arguments)
            else:
                parser.print_usage()
        except SetupError as e:
            parser.error(message=e.msg)
        except ConfigSettingError as e:
            parser.error(message=str(e))
