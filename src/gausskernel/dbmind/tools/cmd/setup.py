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
import getpass
import os
import shutil
from configparser import ConfigParser

from dbmind import constants, global_vars
from dbmind.cmd.config_utils import (
    ConfigUpdater, check_config_validity,
    DynamicConfig, load_sys_configs,
    NULL_TYPE, ENCRYPTED_SIGNAL, DBMIND_CONF_HEADER
)
from dbmind.cmd.edbmind import SKIP_LIST
from dbmind.common import utils, security
from dbmind.common.exceptions import SetupError, SQLExecutionError
from dbmind.metadatabase import (
    create_dynamic_config_schema,
    create_metadatabase_schema,
    destroy_metadatabase
)
from dbmind.metadatabase.dao.dynamic_config import dynamic_config_set, dynamic_config_get


def initialize_and_check_config(confpath, interactive=False):
    if not os.path.exists(confpath):
        raise SetupError('Not found the directory %s.' % confpath)
    confpath = os.path.realpath(confpath)  # in case of dir changed.
    os.chdir(confpath)
    dbmind_conf_path = os.path.join(confpath, constants.CONFILE_NAME)
    dynamic_config_path = os.path.join(confpath, constants.DYNAMIC_CONFIG)

    def _create_dynamic_config_schema_and_generate_keys():
        utils.write_to_terminal('Starting to generate a dynamic config file...', color='green')
        create_dynamic_config_schema()
        s1_ = security.safe_random_string(16)
        s2_ = security.safe_random_string(16)
        dynamic_config_set('dbmind_config', 'cipher_s1', s1_)
        dynamic_config_set('dbmind_config', 'cipher_s2', s2_)
        return s1_, s2_

    if not os.path.exists(dynamic_config_path):
        # If dynamic config file does not exist, create a new one.
        s1, s2 = _create_dynamic_config_schema_and_generate_keys()
    else:
        # If exists, need not create a new dynamic config file
        # and directly load hash key s1 and s2 from it.
        s1 = dynamic_config_get('dbmind_config', 'cipher_s1')
        s2 = dynamic_config_get('dbmind_config', 'cipher_s2')
        if not (s1 and s2):
            # If s1 or s2 is invalid, it indicates that an broken event may occurred while generating
            # the dynamic config file. Hence, the whole process of generation is unreliable and we have to
            # generate a new dynamic config file.
            os.unlink(dynamic_config_path)
            s1, s2 = _create_dynamic_config_schema_and_generate_keys()

    # Check some configurations and encrypt passwords.
    with ConfigUpdater(dbmind_conf_path) as config:
        if not interactive:
            for section, section_comment in config.sections(SKIP_LIST):
                for option, value, inline_comment in config.items(section):
                    valid, invalid_reason = check_config_validity(
                        section, option, value
                    )
                    if not valid:
                        raise SetupError(
                            "Wrong %s-%s in the file dbmind.conf due to '%s'. Please revise it." % (
                                section, option, invalid_reason
                            )
                        )

        utils.write_to_terminal('Starting to encrypt the plain-text passwords in the config file...', color='green')
        for section, section_comment in config.sections(SKIP_LIST):
            for option, value, inline_comment in config.items(section):
                if 'password' in option and value != NULL_TYPE:
                    # Skip when the password has encrypted.
                    if value.startswith(ENCRYPTED_SIGNAL):
                        continue
                    # Every time a new password is generated, update the IV.
                    iv = security.generate_an_iv()
                    dynamic_config_set('iv_table', '%s-%s' % (section, option), iv)
                    cipher_text = security.encrypt(s1, s2, iv, value)
                    # Use a signal ENCRYPTED_SIGNAL to mark the password that has been encrypted.
                    decorated_cipher_text = ENCRYPTED_SIGNAL + cipher_text
                    config.set(section, option, decorated_cipher_text, inline_comment)

    # config and initialize meta-data database.
    utils.write_to_terminal('Starting to initialize and check the essential variables...', color='green')
    global_vars.dynamic_configs = DynamicConfig
    global_vars.configs = load_sys_configs(
        constants.CONFILE_NAME
    )
    utils.write_to_terminal('Starting to connect to meta-database and create tables...', color='green')
    try:
        create_metadatabase_schema(check_first=False)
    except SQLExecutionError:
        utils.write_to_terminal('The given database has duplicate tables. '
                                'If you want to reinitialize the database, press [R]. '
                                'If you want to keep the existent tables, press [K].', color='red')
        input_char = ''
        while input_char not in ('R', 'K'):
            input_char = input('Press [R] to reinitialize; Press [K] to keep and ignore:').upper()
        if input_char == 'R':
            utils.write_to_terminal('Starting to drop existent tables in meta-database...', color='green')
            destroy_metadatabase()
            utils.write_to_terminal('Starting to create tables for meta-database...', color='green')
            create_metadatabase_schema(check_first=True)
        if input_char == 'K':
            utils.write_to_terminal('Ignoring...', color='green')
    utils.write_to_terminal('The setup process finished successfully.', color='green')


def setup_directory_interactive(confpath):
    # Determine whether the directory is empty.
    if os.path.exists(confpath) and len(os.listdir(confpath)) > 0:
        raise SetupError("Given setup directory '%s' already exists." % confpath)

    # Make the confpath directory and copy all files
    # (basically all files are config files) from MISC directory.
    shutil.copytree(
        src=constants.MISC_PATH,
        dst=confpath
    )

    utils.write_to_terminal('Starting to configure...', color='green')
    # Generate an initial configuration file.
    config_src = os.path.join(constants.MISC_PATH, constants.CONFILE_NAME)
    config_dst = os.path.join(confpath, constants.CONFILE_NAME)
    # read configurations
    config = ConfigParser(inline_comment_prefixes=None)
    with open(file=config_src, mode='r', errors='ignore') as fp:
        config.read_file(fp)

    try:
        # Modify configuration items by user's typing.
        for section in config.sections():
            if section in SKIP_LIST:
                continue
            section_comment = config.get('COMMENT', section, fallback='')
            utils.write_to_terminal('[%s]' % section, color='white')
            utils.write_to_terminal(section_comment, color='yellow')
            # Get each configuration item.
            for option, values in config.items(section):
                try:
                    default_value, inline_comment = map(str.strip, values.rsplit('#', 1))
                except ValueError:
                    default_value, inline_comment = values.strip(), ''
                # If not set default value, the default value is null.
                if default_value.strip() == '':
                    default_value = NULL_TYPE
                # hidden password
                input_value = ''
                if 'password' in option:
                    input_func = getpass.getpass
                else:
                    input_func = input

                while input_value.strip() == '':
                    # Ask for options.
                    input_value = input_func('%s (%s) [default: %s]:' % (option, inline_comment, default_value))
                    # If user does not set the option, set default target.
                    if input_value.strip() == '':
                        input_value = default_value

                    valid, invalid_reason = check_config_validity(
                        section, option, input_value
                    )
                    if not valid:
                        utils.write_to_terminal(
                            "Please retype due to '%s'." % invalid_reason,
                            level='error',
                            color='red'
                        )
                        input_value = ''
                config.set(section, option, '%s  # %s' % (input_value, inline_comment))
    except (KeyboardInterrupt, EOFError):
        utils.write_to_terminal('Removing generated files due to keyboard interrupt.')
        shutil.rmtree(
            path=confpath
        )
        return

    # output configurations
    with open(file=config_dst, mode='w+') as fp:
        # Add header comments (including license and notice).
        fp.write(DBMIND_CONF_HEADER)
        config.write(fp)

    initialize_and_check_config(confpath, interactive=True)


def setup_directory(confpath):
    # Determine whether the directory is empty.
    if os.path.exists(confpath) and len(os.listdir(confpath)) > 0:
        raise SetupError("Given setup directory '%s' already exists." % confpath)

    utils.write_to_terminal(
        "You are not in the interactive mode so you must modify configurations manually.\n"
        "The file you need to modify is '%s'.\n"
        "After configuring, you should continue to set up and initialize the directory with --initialize option, "
        "e.g.,\n "
        "'... service setup -c %s --initialize'"
        % (os.path.join(confpath, constants.CONFILE_NAME), confpath),
        color='yellow')

    # Make the confpath directory and copy all files
    # (basically all files are config files) from MISC directory.
    shutil.copytree(
        src=constants.MISC_PATH,
        dst=confpath
    )
    # output configurations
    with open(file=os.path.join(confpath, constants.CONFILE_NAME), mode='r+') as fp:
        old = fp.readlines()
        # Add header comments (including license and notice).
        fp.seek(0)
        fp.write(DBMIND_CONF_HEADER)
        fp.writelines(old)
    utils.write_to_terminal("Configure directory '%s' has been created successfully." % confpath, color='green')
