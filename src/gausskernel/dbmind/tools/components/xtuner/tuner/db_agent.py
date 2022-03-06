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

from .character import OpenGaussMetric
from .exceptions import DBStatusError, SecurityError, ExecutionError, OptionError
from .executor import ExecutorFactory
from .knob import RecommendedKnobs, Knob
from .utils import clip
from .utils import construct_dividing_line
from .utils import to_tuples


def check_special_character(phrase):
    """
    This function checks whether a phrase contains invalid characters.
    This function is used for security verification to prevent potential security risks.

    :param phrase: String type. Phrase to be checked
    :raise: Raise SecurityError if security risks exists.
    """
    check_list = (' ', '|', ';', '&', '$', '<', '>', '`',
                  '\\', '\'', '"', '{', '}', '(', ')', '[',
                  ']', '~', '*', '?', '!', '\n')
    if phrase.strip() == '':
        return

    for char in check_list:
        if phrase.find(char) >= 0:
            raise SecurityError


def new_db_agent(db_info):
    return DB_Agent(db_name=db_info['db_name'],
                    db_port=db_info['port'],
                    db_user=db_info['db_user'],
                    db_user_pwd=db_info['db_user_pwd'],
                    host=db_info['host'],
                    host_user=db_info['host_user'],
                    host_user_pwd=db_info['host_user_pwd'],
                    ssh_port=db_info['ssh_port'])


class DB_Agent:
    def __init__(self, host, host_user, host_user_pwd,
                 db_user, db_user_pwd, db_name, db_port, ssh_port=22):
        """
        This class is used to abstract the database instance and interact with the database.
        All operations such as obtaining database information and setting knobs
        need to be implemented through this class.

        :param host: String type, meaning the same as variable name.
        :param host_user: Same as above.
        :param host_user_pwd: Same as above.
        :param db_user: Same as above.
        :param db_user_pwd: Same as above.
        :param db_name: Same as above.
        :param db_port: Int type, meaning the same as variable name.
        :param ssh_port: Same as above.
        """
        # Set database authorization information:
        self.ssh = ExecutorFactory() \
            .set_host(host) \
            .set_user(host_user) \
            .set_pwd(host_user_pwd) \
            .set_port(ssh_port) \
            .get_executor()

        self.host_user = host_user
        self.db_user = host_user if not db_user else db_user
        self.db_user_pwd = db_user_pwd
        self.db_name = db_name
        self.db_port = db_port
        self.data_path = None
        self.knobs = None
        self.ordered_knob_list = None

        self.check_connection_params()

        # Set a connection session as unlimited.
        self.set_knob_value("statement_timeout", 0)

        # Initialize database metrics.
        self.metric = OpenGaussMetric(self)

    def check_connection_params(self):
        # Check whether the binary files of the database can be located through environment variables.
        try:
            self.exec_command_on_host("which gsql")
            self.exec_command_on_host("which gaussdb")
            self.exec_command_on_host("which gs_guc")
            self.exec_command_on_host("which gs_ctl")
        except ExecutionError as e:
            logging.exception("An exception occurred while checking connection parameters: %s", e)
            raise OptionError("The parameters about SSH login user are incorrect. Please check.\n"
                              "Hint: The binary file path of the database cannot be obtained after the login."
                              "Please ensure that the paths of tools such as gsql and gs_ctl are added to environment "
                              "variables ($PATH).") from None

        # Check whether the third-party libraries can be properly loaded.
        try:
            self.exec_command_on_host("gaussdb --version")
            self.exec_command_on_host("gsql --version")
        except ExecutionError as e:
            logging.exception("An exception occurred while checking connection parameters: %s", e)
            raise DBStatusError("The database environment is incorrectly configured. "
                                "login to the database host as the user and check whether "
                                "the database environment can be used properly. "
                                "For example, check environment variables $LD_LIBRARY_PATH.") from None

        # Check whether the database is started.
        # And check whether the user name and password are correct.
        try:
            if not self.is_alive():
                raise DBStatusError("Failed to login to the database. "
                                    "Check whether the database is started. ")

            # Get database instance pid and data_path.
            self.data_path = self.exec_statement(
                "SELECT datapath FROM pg_node_env;"
            )[0][0]
        except ExecutionError as e:
            logging.exception("An exception occurred while checking connection parameters: %s", e)
            raise DBStatusError("Failed to login to the database. "
                                "Check whether the user name or password is correct, "
                                "and whether the database information passed to the X-Tuner is correct.") from None

        # Check whether the current user has sufficient permission to perform tuning.
        try:
            self.exec_statement("select * from pg_user;")
        except ExecutionError as e:
            logging.exception("An exception occurred while checking connection parameters: %s", e)
            raise DBStatusError("The current database user may not have the permission to tune best_knobs. "
                                "Please assign the administrator permission temporarily so that you can "
                                "obtain more information from the database.") from None

    def set_tuning_knobs(self, knobs):
        if not isinstance(knobs, RecommendedKnobs):
            raise TypeError

        self.knobs = knobs
        self.ordered_knob_list = self.knobs.names()

        # Check whether the knob value exceeds the upper limit and lower limit.
        wherein_list = list()
        for knob in self.ordered_knob_list:
            check_special_character(knob)
            wherein_list.append("'%s'" % knob)

        sql = "SELECT name, setting, min_val, max_val FROM pg_settings WHERE name IN ({})".format(
            ','.join(wherein_list)
        )

        # If the value is missing, use the default value obtained in the system table to fill it.
        tuples = self.exec_statement(sql)
        for name, setting, min_val, max_val in tuples:
            knob = self.knobs[name]
            if knob.type != Knob.TYPE.BOOL:
                min_val = float(min_val)
                max_val = float(max_val)
                knob.min = min_val if knob.min is None else max(knob.min, min_val)
                knob.max = max_val if knob.max is None else min(knob.max, max_val)
            knob.original = setting
            # If user did not set default field, then make the original value as the default value. 
            # The default value (knob.current) is the starting point while tuning. 
            if knob.current is None:
                knob.current = knob.to_numeric(setting)

    def exec_statement(self, sql, timeout=None):
        """
        Connect to the remote database host through SSH,
        run the `gsql` command to execute SQL statements, and parse the execution result.

        p.s This is why we want users who log in to the Linux host
        to have environment variables that can find `gsql` commands.

        :param sql: SQL statement
        :param timeout: Int type. Unit second.
        :return: The parsed result from SQL statement execution.
        """
        command = "gsql -p {db_port} -U {db_user} -d {db_name} -W {db_user_pwd} -c \"{sql}\";".format(
            db_port=self.db_port,
            db_user=self.db_user,
            db_name=self.db_name,
            db_user_pwd=self.db_user_pwd,
            sql=sql
        )

        stdout, stderr = self.ssh.exec_command_sync(command, timeout)
        if len(stderr) > 0 or self.ssh.exit_status != 0:
            logging.error("Cannot execute SQL statement: %s. Error message: %s.", sql, stderr)
            raise ExecutionError("Cannot execute SQL statement: %s." % sql)

        return to_tuples(stdout)

    def is_alive(self):
        """
        Check whether the database is running.

        :return: True means running and vice versa.
        """
        try:
            stdout = self.exec_command_on_host("ps -ux | grep gaussdb | wc -l")
            at_least_count = 1  # Includes one 'grep gaussdb' command.
            if int(stdout.strip()) <= at_least_count:
                return False
        except ExecutionError:
            return False

        sql = "SELECT now();"
        try:
            self.exec_statement(sql, timeout=1)
            return True
        except ExecutionError:
            return False

    def exec_command_on_host(self, cmd, timeout=None, ignore_status_code=False):
        """
        The SSH connection is reused to execute shell commands on the Linux host.
        This method can be used to restart the database, collect database running environment information,
        and set database knobs.

        :param cmd: Shell command.
        :param timeout: Int type. Unit second.
        :param ignore_status_code: If the exit status code returned after the command is executed is unequal to 0,
                                    determine whether to ignore it. By default, it is not ignored, which may throw an
                                    exception, and some operations are insignificant or even have to be ignored.
        :return: Returns the standard output stream result after the command is executed.
        :raise: Raise ExecutionError if found error.
        """
        stdout, stderr = self.ssh.exec_command_sync(cmd, timeout=timeout)

        if len(stderr) > 0 or self.ssh.exit_status != 0:
            error_msg = "An error occurred when executing the command '%s'. " \
                        "The error information is: %s, the output information is %s." % (cmd, stderr, stdout)
            if ignore_status_code and not (len(stderr) > 0 and self.ssh.exit_status != 0):
                logging.warning(error_msg)
            else:
                raise ExecutionError(error_msg)
        return stdout

    def get_knob_normalized_vector(self):
        nv = list()
        for name in self.ordered_knob_list:
            val = self.get_knob_value(name)
            nv.append(self.knobs[name].to_numeric(val))
        return nv

    def get_default_normalized_vector(self):
        """
        In order to get the initial performance and rollback the settings while tuning is finished.
        :return: the vector that normalized from original/initial knobs.
        """
        nv = list()
        for name in self.ordered_knob_list:
            val = self.knobs[name].original
            nv.append(self.knobs[name].to_numeric(val))
        return nv

    def set_knob_normalized_vector(self, nv):
        restart = False
        for i, val in enumerate(nv):
            name = self.ordered_knob_list[i]
            knob = self.knobs[name]
            self.set_knob_value(name, knob.to_string(val))
            restart = True if knob.restart else restart

        if restart:
            self.restart()

    def get_knob_value(self, name):
        check_special_character(name)
        sql = "SELECT setting FROM pg_settings WHERE name = '{}';".format(name)
        value = self.exec_statement(sql)[0][0]
        return value

    def set_knob_value(self, name, value):
        logging.info("change knob: [%s=%s]", name, value)
        try:
            self.exec_command_on_host("gs_guc reload -c \"%s=%s\" -D %s" % (name, value, self.data_path))
        except ExecutionError as e:
            if str(e).find('Success to perform gs_guc!') < 0:
                logging.warning(e)

    def reset_state(self):
        self.metric.reset()

    def restart(self):
        logging.info(construct_dividing_line("Restarting database.", padding="*"))
        try:
            self.exec_statement("checkpoint;")  # Prevent the database from being shut down for a long time.
        except ExecutionError:
            logging.warning("Cannot checkpoint perhaps due to bad GUC settings.")
        self.exec_command_on_host("gs_ctl stop -D {data_path}".format(data_path=self.data_path),
                                  ignore_status_code=True)
        self.exec_command_on_host("gs_ctl start -D {data_path}".format(data_path=self.data_path),
                                  ignore_status_code=True)

        if self.is_alive():
            logging.info("The database restarted successfully.")
        else:
            logging.fatal("The database restarted failed.")
            raise DBStatusError("The database restarted failed.")

        logging.info(construct_dividing_line())

    def drop_cache(self):
        try:
            # Check whether frequent input password is required.
            user_desc = self.exec_command_on_host('sudo -n -l -U %s' % self.host_user, ignore_status_code=True)
            if (not user_desc) or user_desc.find('NOPASSWD') < 0:
                logging.warning("Hint: You must add this line '%s ALL=(root)   NOPASSWD: /usr/bin/tee /proc/sys/vm/drop_caches'"
                                " to the file '/etc/sudoers' with administrator permission.", self.host_user)
                return False

            self.exec_command_on_host('sync')
            self.exec_command_on_host('echo 1 | sudo /usr/bin/tee /proc/sys/vm/drop_caches')
            return True
        except Exception as e:
            logging.warning("Cannot drop cache. %s.", e)
            return False
