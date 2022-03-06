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
import os
import re
from configparser import ConfigParser
from configparser import NoSectionError, NoOptionError

from dbmind import constants
from dbmind.common import security
from dbmind.common.exceptions import InvalidPasswordException, ConfigSettingError
from dbmind.metadatabase.dao.dynamic_config import dynamic_config_get, dynamic_config_set
from dbmind.common.utils import write_to_terminal

NULL_TYPE = '(null)'  # empty text.
ENCRYPTED_SIGNAL = 'Encrypted->'


def check_config_validity(section, option, value, inline_comment=None):
    config_item = '%s-%s' % (section, option)
    # exceptional cases:
    if config_item == 'METADATABASE-port':
        return True, None
    # normal inspection process:
    if 'port' in option:
        valid_port = str.isdigit(value) and 0 < int(value) <= 65535
        if not valid_port:
            return False, 'Invalid port %s' % value
    if 'database' in option:
        if value == NULL_TYPE or value.strip() == '':
            return False, 'Unspecified database name'
    if 'Options:' in inline_comment:
        # determine setting option whether choose from option list.
        results = re.findall(r'Options: (.*)?\.', inline_comment)
        if len(results) > 0:
            options = list(map(str.strip, results[0].split(',')))
            if value not in options:
                return False, 'Invalid choice: %s' % value
    if 'dbtype' in option and value == 'opengauss':
        write_to_terminal(
            'WARN: default PostgresSQL connector (psycopg2-binary) does not support openGauss.\n'
            'It would help if you compiled psycopg2 with openGauss manually or '
            'created a connection user after setting the GUC password_encryption_type to 1.',
            color='yellow'
        )

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
            value = configs.get(section, option, *args, **kwargs)
            if value == NULL_TYPE:
                value = ''
            if 'password' in option and value.startswith(ENCRYPTED_SIGNAL):
                s1 = dynamic_config_get('dbmind_config', 'cipher_s1')
                s2 = dynamic_config_get('dbmind_config', 'cipher_s2')
                iv = dynamic_config_get('iv_table', '%s-%s' % (section, option))
                try:
                    value = security.decrypt(s1, s2, iv, value.lstrip(ENCRYPTED_SIGNAL))
                except Exception as e:
                    raise InvalidPasswordException(e)
            return value

    return ConfigWrapper()


class ConfigUpdater:
    def __init__(self, filepath):
        self.config = ConfigParser(inline_comment_prefixes=None)
        self.filepath = os.path.abspath(filepath)
        self.fp = None
        self.readonly = True

    def get(self, section, option):
        value = self.config.get(section, option)
        default_value, inline_comment = map(str.strip, value.rsplit('#', 1))
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
            with open(
                    file=os.path.join(constants.MISC_PATH, constants.CONFILE_HEADER_NAME)
            ) as header_fp:
                self.fp.writelines(header_fp.readlines())
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
            valid, reason = check_config_validity(section, option, value, comment)
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
