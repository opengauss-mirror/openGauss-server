try:
    import sys
    import os
    import argparse
    import logging
    import time
    import signal
    import re
    import select
    import urllib
    import json
    import datetime
    from urllib.request import Request
    from subprocess import Popen, PIPE
    from threading import Thread, Event, Timer
    from configparser import ConfigParser
    from logging import handlers
except ImportError as err:
    sys.exit("index_server.py: Failed to import module: %s." % str(err))


current_dirname = os.path.dirname(os.path.realpath(__file__))
__description__ = 'index advise: index server tool.'


class RepeatTimer(Thread):
    """
    This class inherits from threading.Thread, it is used for periodic execution
    function at a specified time interval.
    """

    def __init__(self, interval, function, *args, **kwargs):
        Thread.__init__(self)
        self._interval = interval
        self._function = function
        self._args = args
        self._kwargs = kwargs
        self._finished = Event()

    def run(self):
        while not self._finished.is_set():
            # Execute first, wait later.
            self._function(*self._args, **self._kwargs)
            self._finished.wait(self._interval)
        self._finished.set()

    def cancel(self):
        self._finished.set()


class CreateLogger:
    def __init__(self, level, log_name):
        self.level = level
        self.log_name = log_name

    def create_log(self):
        logger = logging.getLogger(self.log_name)
        log_path = os.path.join(current_dirname, 'log')
        if os.path.exists(log_path):
            if os.path.isfile(log_path):
                os.remove(log_path)
                os.mkdir(log_path)
        else:
            os.mkdir(log_path)
        agent_handler = handlers.RotatingFileHandler(filename=os.path.join(log_path, self.log_name),
                                                     maxBytes=1024 * 1024 * 100,
                                                     backupCount=5)
        agent_handler.setFormatter(logging.Formatter(
            "[%(asctime)s %(levelname)s]-[%(filename)s][%(lineno)d]: %(message)s"))
        logger.addHandler(agent_handler)
        logger.setLevel(getattr(logging, self.level.upper())
                        if hasattr(logging, self.level.upper()) else logging.INFO)
        return logger


class IndexServer:
    def __init__(self, pid_file, logger, password):
        self.pid_file = pid_file
        self.logger = logger
        self.app_name = None
        self.database = None
        self.port = None
        self.host = None
        self.user = None
        self.password = password
        self.wl_user = None
        self.schema = None
        self.max_index_num = 0
        self.max_index_storage = 0
        self.driver = False
        self.index_intervals = 0
        self.sql_amount = 0
        self.max_generate_log_size = 0
        self.statement = False
        self.output_sql_file = None
        self.pg_log_path = None
        self.datanode = None
        self.ai_monitor_url = None
        self.log_min_duration_statement = None
        self.log_statement = None

    def check_proc_exist(self, proc_name):
        """
        check proc exist
        :param proc_name: proc name
        :return: proc pid
        """
        check_proc = "ps ux | grep '%s' | grep -v grep | grep -v nohup | awk \'{print $2}\'" % proc_name
        _, std = self.execute_cmd(check_proc)
        current_pid = str(os.getpid())
        pid_list = [pid for pid in std.split("\n") if pid and pid != current_pid]
        if not pid_list:
            return ""
        return " ".join(pid_list)

    def execute_cmd(self, cmd):
        """
        execute cmd
        :param cmd: cmd str
        :param shell: execute shell mode, True or False
        :return: execute result
        """
        proc = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
        std, err_msg = proc.communicate()
        if proc.returncode != 0:
            self.logger.error("Failed to execute command: %s, \nError: %s." % (cmd, str(err_msg)))
        return proc.returncode, std.decode()

    def save_recommendation_infos(self, recommendation_infos):
        headers = {'Content-Type': 'application/json'}
        data = json.dumps(recommendation_infos, default=lambda o: o.__dict__, sort_keys=True,
                          indent=4).encode()
        request = Request(url=self.ai_monitor_url, headers=headers,
                          data=data)

        response = None
        try:
            response = urllib.request.urlopen(request, timeout=600)
            result = json.loads(response.read())
        finally:
            if response:
                response.close()

        return result

    def convert_output_to_recommendation_infos(self, sql_lines):
        detail_info_pos = 0
        index_info = sql_lines.splitlines()
        for pos, line in enumerate(index_info):
            if 'Display detail information' in line:
                detail_info_pos = pos + 1
                break
        detail_info_json = json.loads('\n'.join(index_info[detail_info_pos:]))
        detail_info_json['appName'] = self.app_name
        detail_info_json['nodeHost'] = self.host
        detail_info_json['dbName'] = self.database
        return detail_info_json

    def execute_index_advisor(self):
        self.logger.info('Index advisor task starting.')
        try:
            cmd = 'echo %s | python3 %s/index_advisor_workload.py %s %s %s -U %s --h %s -W ' \
                  '--schema %s --json --multi_iter_mode --show_detail' % (
                      self.password, current_dirname, self.port, self.database,
                      self.output_sql_file, self.user, self.host, self.schema)
            if self.max_index_storage:
                cmd += ' --max_index_storage %s ' % self.max_index_storage
            if self.max_index_num:
                cmd += ' --max_index_num %s ' % self.max_index_num
            if self.driver:
                try:
                    import psycopg2
                    cmd = cmd.replace('index_advisor_workload.py', 
                                      'index_advisor_workload_driver.py')
                except ImportError:
                    self.logger.warning('Driver import failed, use gsql to connect to the database.')
            self.logger.info('Index advisor cmd:%s' % cmd)
            if os.path.exists(self.output_sql_file):
                _, res = self.execute_cmd(cmd)
                detail_info_json = self.convert_output_to_recommendation_infos(res)

                self.logger.info('Index advisor result: %s.' % detail_info_json)
                result = self.save_recommendation_infos(detail_info_json)
                if result['status'] is not True:
                    self.logger.error('Fail to upload index result, Error: %s' % result['message'])
                else:
                    self.logger.info('Success to upload index result.')
        except Exception as e:
            self.logger.error(e)

    def get_directory_size(self):
        files = os.listdir(self.pg_log_path)
        total_size = 0
        for file in files:
            total_size += os.path.getsize(os.path.join(self.pg_log_path, file))
        return total_size

    def execute_log_index_advisor(self):
        self.logger.info('Start checking guc.')
        try:
            guc_check = 'gs_guc check -Z datanode -D {datanode} -c "log_min_duration_statement" && ' \
                        'gs_guc check -Z datanode -D {datanode} -c "log_statement" '.format(datanode=self.datanode)
            returncode, res = self.execute_cmd(guc_check)
            origin_min_duration = self.log_min_duration_statement
            origin_log_statement = self.log_statement
            if returncode == 0:
                self.logger.info('Original GUC settings is: %s' % res)
                match_res = re.findall(r'log_min_duration_statement=(\'?[a-zA-Z0-9]+\'?)', res)
                if match_res:
                    origin_min_duration = match_res[-1]
                    if 'NULL' in origin_min_duration:
                        origin_min_duration = '30min'
                match_res = re.findall(r'log_statement=(\'?[a-zA-Z]+\'?)', res)
                if match_res:
                    origin_log_statement = match_res[-1]
                    if 'NULL' in origin_log_statement:
                        origin_log_statement = 'none'
            self.logger.info('Parsed (log_min_duration_statement, log_statement) GUC params are (%s, %s)' %
                             (origin_min_duration, origin_log_statement))
            self.logger.info('Test reseting GUC command...')
            guc_reset = 'gs_guc reload -Z datanode -D %s -c "log_min_duration_statement = %s" && ' \
                        'gs_guc reload -Z datanode -D %s -c "log_statement= %s"' % \
                        (self.datanode, origin_min_duration, self.datanode, origin_log_statement)
            returncode, res = self.execute_cmd(guc_reset)
            if returncode != 0:
                guc_reset = 'gs_guc reload -Z datanode -D %s -c "log_min_duration_statement = %s" && ' \
                            'gs_guc reload -Z datanode -D %s -c "log_statement= %s"' % \
                            (self.datanode, self.log_min_duration_statement, self.datanode, self.log_statement)
                ret, res = self.execute_cmd(guc_reset)
                if ret != 0:
                    raise Exception('Cannot reset GUC initial value, please check it.')
            self.logger.info('Test successfully')

            self.logger.info('Open GUC params.')
            # get original all file size
            original_total_size = self.get_directory_size()
            self.logger.info('Original total file size: %sM' % (original_total_size/1024/1024))
            deviation_size = 0
            # open guc
            guc_reload = 'gs_guc reload -Z datanode -D {datanode} -c "log_min_duration_statement = 0" && ' \
                         'gs_guc reload -Z datanode -D {datanode} -c "log_statement= \'all\'"'.format(datanode=self.datanode)
            self.execute_cmd(guc_reload)
            start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(time.time())))
            # caculate log size
            count = 0
            while deviation_size < self.max_generate_log_size:
                time.sleep(5)
                current_size = self.get_directory_size()
                deviation_size = (current_size - original_total_size)/1024/1024
                if current_size - original_total_size < 0:
                    if count >= 60:
                        break
                    count += 1
                self.logger.info('Current log size difference: %sM' % deviation_size)
            self.logger.info('Start to reset GUC, cmd: %s' % guc_reset)
            returncode, res = self.execute_cmd(guc_reset)
            if returncode == 0:
                self.logger.info('Success to reset GUC setting.')
            else:
                self.logger.error('Failed to reset GUC params. please check it.')

            # extract log
            extract_log_cmd = 'python3 %s %s %s -d %s -U %s --start_time "%s"' % \
                              (os.path.join(current_dirname, 'extract_log.py'),
                               self.pg_log_path, self.output_sql_file,
                               self.database, self.wl_user, start_time)
            if self.sql_amount:
                extract_log_cmd += ' --sql_amount %s ' % self.sql_amount
            if self.statement:
                extract_log_cmd += ' --statement '
            self.logger.info('Extracting log cmd: %s' % extract_log_cmd)
            self.execute_cmd(extract_log_cmd)
            self.logger.info('The current log extraction is complete.')

            # index adviss
            self.execute_index_advisor()
        except Exception as e:
            self.logger.error(e)
            guc_reset = 'gs_guc reload -Z datanode -D %s -c "log_min_duration_statement = %s" && ' \
                        'gs_guc reload -Z datanode -D %s -c "log_statement= %s"' % \
                        (self.datanode, self.log_min_duration_statement, self.datanode,
                         self.log_statement)
            self.execute_cmd(guc_reset)

    def parse_check_conf(self, config_path):
        config = ConfigParser()
        config.read(config_path)
        self.app_name = config.get("server", "app_name")
        self.database = config.get("server", "database")
        self.port = config.get("server", "port")
        self.host = config.get("server", "host")
        self.user = config.get("server", "user")
        self.wl_user = config.get("server", "workload_user")
        self.schema = config.get("server", "schema")
        self.max_index_num = config.getint("server", "max_index_num")
        self.max_index_storage = config.get("server", "max_index_storage")
        self.driver = config.getboolean("server", "driver")
        self.index_intervals = config.getint("server", "index_intervals")
        self.sql_amount = config.getint("server", "sql_amount")
        self.output_sql_file = config.get("server", "output_sql_file")
        self.datanode = config.get("server", "datanode")
        self.pg_log_path = config.get("server", "pg_log_path")
        self.ai_monitor_url = config.get("server", "ai_monitor_url")
        self.max_generate_log_size = config.getfloat("server", "max_generate_log_size")
        self.statement = config.getboolean("server", "statement")
        self.log_min_duration_statement = config.get("server", "log_min_duration_statement")
        self.log_statement = config.get("server", "log_statement")
        if not self.log_min_duration_statement or not re.match(r'[a-zA-Z0-9]+',
                                                               self.log_min_duration_statement):
            raise ValueError("Please enter a legal value of [log_min_duration_statement]")
        legal_log_statement = ['none', 'all', 'ddl', 'mod']
        if self.log_statement not in legal_log_statement:
            raise ValueError("Please enter a legal value of [log_statement]")

    def start_service(self, config_path):
        # check service is running or not.
        if os.path.isfile(self.pid_file):
            pid = self.check_proc_exist("index_server")
            if pid:
                raise Exception("Error: Process already running, can't start again.")
            else:
                os.remove(self.pid_file)
        # check config file exists
        if not os.path.isfile(config_path):
            raise Exception("Config file: %s does not exists." % config_path)

        # get listen host and port
        self.logger.info("Start service...")
        # write process pid to file
        if not os.path.isdir(os.path.dirname(self.pid_file)):
            os.makedirs(os.path.dirname(self.pid_file), 0o700)
        with open(self.pid_file, mode='w') as f:
            f.write(str(os.getpid()))

        self.parse_check_conf(config_path)

        self.logger.info("Index advisor execution intervals is: %sh" % self.index_intervals)
        index_advisor_thread = RepeatTimer(self.index_intervals*60*60, self.execute_log_index_advisor)
        self.logger.info("Start timer...")
        index_advisor_thread.start()


def read_input_from_pipe():
    """
    Read stdin input if there is "echo 'str1 str2' | python xx.py",
     return the input string
    """
    input_str = ""
    r_handle, _, _ = select.select([sys.stdin], [], [], 0)
    if not r_handle:
        return ""

    for item in r_handle:
        if item == sys.stdin:
            input_str = sys.stdin.read().strip()
    return input_str


def manage_service():
    LOGGER = CreateLogger("debug", "start_service.log").create_log()
    server_pid_file = os.path.join(current_dirname, 'index_server.pid')
    password = read_input_from_pipe()
    IndexServer(server_pid_file, LOGGER, password).start_service(
        os.path.join(current_dirname, 'database-info.conf'))


def main():
    try:
        manage_service()
    except Exception as err_msg:
        print(err_msg)
        sys.exit(1)


if __name__ == '__main__':
    main()

