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
import logging
import os
import configparser
from configparser import ConfigParser
from configparser import NoSectionError, NoOptionError

import dbmind.common.utils.checking
from dbmind import constants
from dbmind.common import security
from dbmind.common.exceptions import InvalidCredentialException, ConfigSettingError
from dbmind.common.rpc import ping_rpc_url
from dbmind.common.utils.checking import check_ip_valid, check_port_valid
from dbmind.common.utils.cli import write_to_terminal, raise_fatal_and_exit
from dbmind.metadatabase.dao.dynamic_config import dynamic_config_get, dynamic_config_set, dynamic_configs_list

DBMIND_CONF_HEADER = """\
# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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

# Notice:
# 1. (null) explicitly represents empty or null. Meanwhile blank represents undefined.
# 2. DBMind encrypts password parameters. Hence, there is no plain-text password after initialization.
# 3. Users can only configure the plain-text password in this file before initializing
#    (that is, using the --initialize option),
#    and then if users want to modify the password-related information,
#    users need to use the 'set' sub-command to achieve.
# 4. If users use relative path in this file, the current working directory is the directory where this file is located.
"""

NULL_TYPE = '(null)'  # empty text.
ENCRYPTED_SIGNAL = 'Encrypted->'

# Used by check_config_validity().
CONFIG_OPTIONS = {
    'TSDB-name': ['prometheus'],
    'METADATABASE-dbtype': ['sqlite', 'opengauss', 'postgresql'],
    'WORKER-type': ['local', 'dist'],
    'LOG-level': ['DEBUG', 'INFO', 'WARNING', 'ERROR']
}

# Used by check_config_validity().
POSITIVE_INTEGER_CONFIG = ['SELF-MONITORING-detection_interval',
                           'SELF-MONITORING-last_detection_time',
                           'SELF-MONITORING-forecasting_future_time',
                           'SELF-MONITORING-result_storage_retention',
                           'SELF-OPTIMIZATION-optimization_interval',
                           'SELF-OPTIMIZATION-max_reserved_period',
                           'SELF-OPTIMIZATION-max_index_num',
                           'SELF-OPTIMIZATION-max_index_storage',
                           'SELF-OPTIMIZATION-max_template_num',
                           'LOG-maxbytes',
                           'LOG-backupcount']

# Used by check_config_validity().
BOOLEAN_CONFIG = [
    'SELF-OPTIMIZATION-kill_slow_query'
]


def check_config_validity(section, option, value, silent=False):
    config_item = '%s-%s' % (section, option)
    # exceptional cases:
    if config_item in ('METADATABASE-port', 'METADATABASE-host'):
        if value.strip() == '' or value == NULL_TYPE:
            return True, None

    if config_item == 'AGENT-master_url':
        if value.strip() == '':
            return False, 'You should give the URL.'
        success = ping_rpc_url(value)
        if not success:
            write_to_terminal(
                'WARNING: Failed to ping this RPC url.',
                color='yellow'
            )

    # normal inspection process:
    if 'port' in option:
        valid_port = check_port_valid(value)
        if not valid_port:
            return False, 'Invalid port for %s: %s(1024-65535)' % (config_item, value)
    if 'host' in option:
        valid_host = check_ip_valid(value)
        if not valid_host:
            return False, 'Invalid IP Address for %s: %s' % (config_item, value)
    if 'database' in option:
        if value == NULL_TYPE or value.strip() == '':
            return False, 'Unspecified database name %s' % value
    if config_item in POSITIVE_INTEGER_CONFIG:
        if not str.isdigit(value) or int(value) <= 0:
            return False, 'Invalid value for %s: %s' % (config_item, value)
    if config_item in BOOLEAN_CONFIG:
        if value.lower() not in ConfigParser.BOOLEAN_STATES:
            return False, 'Invalid boolean value for %s.' % config_item
    options = CONFIG_OPTIONS.get(config_item)
    if options and value not in options:
        return False, 'Invalid choice for %s: %s' % (config_item, value)

    if 'dbtype' in option and value == 'opengauss' and not silent:
        write_to_terminal(
            'WARN: default PostgreSQL connector (psycopg2-binary) does not support openGauss.\n'
            'It would help if you compiled psycopg2 with openGauss manually or '
            'created a connection user after setting the GUC password_encryption_type to 1.',
            color='yellow'
        )
    if 'dbtype' in option and value == 'sqlite' and not silent:
        write_to_terminal(
            'NOTE: SQLite currently only supports local deployment, so you only need to provide '
            'METADATABASE-database information. if you provide other information, DBMind will '
            'ignore them.',
            color='yellow'
        )
    if value != NULL_TYPE and 'ssl_certfile' in option:
        dbmind.common.utils.checking.warn_ssl_certificate(value, None)
    if value != NULL_TYPE and 'ssl_keyfile' == option:
        dbmind.common.utils.checking.warn_ssl_certificate(None, value)

    # Add more checks here.
    return True, None


def load_sys_configs(confile):
    # Note: To facilitate the user to modify the configuration items through the
    # configuration file easily, we add inline comments to the file, but we need
    # to remove the inline comments while parsing.
    # Otherwise, it will cause the read configuration items to be wrong.
    configs = ConfigParser(inline_comment_prefixes='#')
    with open(file=confile, mode='r') as fp:
        configs.read_file(fp)

    class ConfigWrapper(object):
        def __getattribute__(self, name):
            try:
                return object.__getattribute__(self, name)
            except (AttributeError, KeyError):
                return configs.__getattribute__(name)

        # Self-defined converters:
        @staticmethod
        def get(section, option, *args, **kwargs):
            """Faked get() for ConfigParser class."""
            kwargs.setdefault('fallback', None)
            try:
                value = configs.get(section, option, *args, **kwargs)
            except configparser.InterpolationSyntaxError as e:
                raise configparser.InterpolationSyntaxError(
                    e.section, e.option, 'Found bad configuration: %s-%s.' % (e.section, e.option)
                ) from None
            if value is None:
                logging.warning('Not set %s-%s.', section, option)
                return value

            if value == NULL_TYPE:
                value = ''
            if 'password' in option and value != '':
                s1 = dynamic_config_get('dbmind_config', 'cipher_s1')
                s2 = dynamic_config_get('dbmind_config', 'cipher_s2')
                iv = dynamic_config_get('iv_table', '%s-%s' % (section, option))
                real_value = None
                if value.startswith(ENCRYPTED_SIGNAL):
                    real_value = value[len(ENCRYPTED_SIGNAL):]
                else:
                    raise configparser.InterpolationSyntaxError(
                        section, option, 'DBMind only supports encrypted password. '
                                         'Please set %s-%s and initialize the configuration file.' % (section, option),
                    )

                try:
                    value = security.decrypt(s1, s2, iv, real_value)
                except Exception as e:
                    raise InvalidCredentialException(
                        'An exception %s raised while decrypting.' % type(e)
                    ) from None

            else:
                valid, reason = check_config_validity(section, option, value, silent=True)
                if not valid:
                    raise ConfigSettingError('DBMind failed to start due to %s.' % reason)

            return value

        @staticmethod
        def getint(section, option, *args, **kwargs):
            """Faked getint() for ConfigParser class."""
            value = configs.get(section, option, *args, **kwargs)
            valid, reason = check_config_validity(section, option, value, silent=True)
            if not valid:
                raise ConfigSettingError('DBMind failed to start due to %s.' % reason)

            return int(value)

    return ConfigWrapper()


class ConfigUpdater:
    def __init__(self, filepath):
        self.config = ConfigParser(inline_comment_prefixes=None)
        self.filepath = os.path.realpath(filepath)
        self.fp = None
        self.readonly = True

    def get(self, section, option):
        value = self.config.get(section, option)
        try:
            default_value, inline_comment = map(str.strip, value.rsplit('#', 1))
        except ValueError:
            default_value, inline_comment = value.strip(), ''
        if default_value == '':
            default_value = NULL_TYPE
        return default_value, inline_comment

    def set(self, section, option, value, inline_comment):
        self.readonly = False
        self.config.set(section, option, '%s  # %s' % (value, inline_comment))

    def sections(self, skip_list=()):
        for section in self.config.sections():
            if section not in skip_list:
                comment = self.config.get('COMMENT', section, fallback='')
                yield section, comment

    def items(self, section):
        for option in self.config.options(section):
            default_value, inline_comment = self.get(section, option)
            yield option, default_value, inline_comment

    def __enter__(self):
        self.fp = open(file=self.filepath, mode='r+', errors='ignore')
        self.config.read_file(self.fp)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.readonly:
            # output configurations
            self.fp.truncate(0)
            self.fp.seek(0)
            self.fp.write(DBMIND_CONF_HEADER)
            self.config.write(self.fp)
            self.fp.flush()
        self.fp.close()


class DynamicConfig:
    @staticmethod
    def get(*args, **kwargs):
        return dynamic_config_get(*args, **kwargs)

    @staticmethod
    def set(*args, **kwargs):
        return dynamic_config_set(*args, **kwargs)

    @staticmethod
    def list():
        return dynamic_configs_list()


def set_config_parameter(confpath, section: str, option: str, value: str):
    if not os.path.exists(confpath):
        raise ConfigSettingError("Invalid directory '%s', please set up first." % confpath)

    # Section is case sensitive.
    if section.isupper():
        with ConfigUpdater(os.path.join(confpath, constants.CONFILE_NAME)) as config:
            # If not found, raise NoSectionError or NoOptionError.
            try:
                old_value, comment = config.get(section, option)
            except (NoSectionError, NoOptionError):
                raise ConfigSettingError('Not found the parameter %s-%s.' % (section, option))
            valid, reason = check_config_validity(section, option, value)
            if not valid:
                raise ConfigSettingError('Incorrect value due to %s.' % reason)
            # If user wants to change password, we should encrypt the plain-text password first.
            if 'password' in option:
                # dynamic_config_xxx searches file from current working directory.
                os.chdir(confpath)
                s1 = dynamic_config_get('dbmind_config', 'cipher_s1')
                s2 = dynamic_config_get('dbmind_config', 'cipher_s2')
                # Every time a new password is generated, update the IV.
                iv = security.generate_an_iv()
                dynamic_config_set('iv_table', '%s-%s' % (section, option), iv)
                cipher = security.encrypt(s1, s2, iv, value)
                value = ENCRYPTED_SIGNAL + cipher
            config.set(section, option, value, comment)
    elif section.islower():
        # dynamic_config_xxx searches file from current working directory.
        os.chdir(confpath)
        try:
            old_value = dynamic_config_get(section, option)
        except ValueError:
            raise ConfigSettingError('Not found the parameter %s-%s.' % (section, option))
        if not old_value:
            raise ConfigSettingError('Not found the parameter %s-%s.' % (section, option))
        dynamic_config_set(section, option, value)
    else:
        # If run here, it seems that the format of section string is not correct.
        raise ConfigSettingError('%s is an incorrect section. '
                                 'Please take note that section string is case sensitive.' % section)

    write_to_terminal('Success to modify parameter %s-%s.' % (section, option), color='green')
