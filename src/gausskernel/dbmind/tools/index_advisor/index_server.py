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

    def create_log(self, log_path):
        logger = logging.getLogger(self.log_name)
        log_path = os.path.join(os.path.dirname(log_path), 'log')
        if os.path.exists(log_path):
            if os.path.isfile(log_path):
                os.remove(log_path)
                os.mkdir(log_path)
        else:
            os.makedirs(log_path)
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
    def __init__(self, pid_file, logger, password, **kwargs):
        self.pid_file = pid_file
        self.logger = logger
        self.password = password
        self._kwargs = kwargs

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
            self.logger.error("Failed to execute command. Error: %s." % str(err_msg))
        return proc.returncode, std.decode()

    def save_recommendation_infos(self, recommendation_infos):
        headers = {'Content-Type': 'application/json'}
        data = json.dumps(recommendation_infos, default=lambda o: o.__dict__, sort_keys=True,
                          indent=4).encode()
        request = Request(url=self._kwargs['ai_monitor_url'], headers=headers,
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
        detail_info_json['appName'] = self._kwargs.get('app_name')
        detail_info_json['nodeHost'] = self._kwargs.get('host')
        detail_info_json['dbName'] = self._kwargs.get('database')
        return detail_info_json

    def execute_index_advisor(self):
        self.logger.info('Index advisor task starting.')
        try:
            cmd = 'echo %s | python3 %s/index_advisor_workload.py %s %s %s -U %s -W ' \
                  '--schema %s --json --multi_iter_mode --show_detail' % (
                      self.password, current_dirname, self._kwargs['port'], self._kwargs['database'],
                      self._kwargs['output_sql_file'], self._kwargs['user'], self._kwargs['schema'])
            if self._kwargs['max_index_storage']:
                cmd += ' --max_index_storage %s ' % self._kwargs['max_index_storage']
            if self._kwargs['max_index_num']:
                cmd += ' --max_index_num %s ' % self._kwargs['max_index_num']
            if self._kwargs['driver']:
                try:
                    import psycopg2
                    cmd = cmd.replace('index_advisor_workload.py', 
                                      'index_advisor_workload_driver.py')
                except ImportError:
                    self.logger.warning('Driver import failed, use gsql to connect to the database.')
            self.logger.info('Index advisor cmd:%s' % cmd.split('|')[-1])
            if os.path.exists(self._kwargs['output_sql_file']):
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

    def extract_log(self, start_time):
        extract_log_cmd = 'python3 %s %s %s --start_time "%s" --json ' % \
                          (os.path.join(current_dirname, 'extract_log.py'),
                           self._kwargs['pg_log_path'],
                           self._kwargs['output_sql_file'], start_time)
        if self._kwargs['database']:
            extract_log_cmd += ' -d %s ' % self._kwargs['database']
        if self._kwargs['wl_user']:
            extract_log_cmd += ' -U %s ' % self._kwargs['wl_user']
        if self._kwargs['sql_amount']:
            extract_log_cmd += ' --sql_amount %s ' % self._kwargs['sql_amount']
        if self._kwargs['statement']:
            extract_log_cmd += ' --statement '
        self.logger.info('Extracting log cmd: %s' % extract_log_cmd)
        self.execute_cmd(extract_log_cmd)
        self.logger.info('The current log extraction is complete.')

    def monitor_log_size(self, guc_reset):
        self.logger.info('Open GUC params.')
        # get original all file size
        original_total_size = self.get_directory_size()
        self.logger.info('Original total file size: %sM' % (original_total_size / 1024 / 1024))
        deviation_size = 0
        # open guc
        guc_reload = 'gs_guc reload -Z datanode -D {datanode} -c "log_min_duration_statement = 0" && ' \
                     'gs_guc reload -Z datanode -D {datanode} -c "log_statement= \'all\'"' \
            .format(datanode=self._kwargs['datanode'])
        self.execute_cmd(guc_reload)
        start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(time.time())))
        # caculate log size
        count = 0
        while deviation_size < self._kwargs['max_generate_log_size']:
            time.sleep(5)
            current_size = self.get_directory_size()
            deviation_size = (current_size - original_total_size) / 1024 / 1024
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
        return start_time

    def get_directory_size(self):
        files = os.listdir(self._kwargs['pg_log_path'])
        total_size = 0
        for file in files:
            total_size += os.path.getsize(os.path.join(self._kwargs['pg_log_path'], file))
        return total_size

    def execute_index_recommendation(self):
        self.logger.info('Start checking guc.')
        try:
            guc_check = 'gs_guc check -Z datanode -D {datanode} -c "log_min_duration_statement" && ' \
                        'gs_guc check -Z datanode -D {datanode} -c "log_statement" '\
                .format(datanode=self._kwargs['datanode'])
            returncode, res = self.execute_cmd(guc_check)
            origin_min_duration = self._kwargs['log_min_duration_statement']
            origin_log_statement = self._kwargs['log_statement']
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
                        (self._kwargs['datanode'], origin_min_duration,
                         self._kwargs['datanode'], origin_log_statement)
            returncode, res = self.execute_cmd(guc_reset)
            if returncode != 0:
                guc_reset = 'gs_guc reload -Z datanode -D %s -c "log_min_duration_statement = %s" && ' \
                            'gs_guc reload -Z datanode -D %s -c "log_statement= %s"' % \
                            (self._kwargs['datanode'], self._kwargs['log_min_duration_statement'],
                             self._kwargs['datanode'], self._kwargs['log_statement'])
                ret, res = self.execute_cmd(guc_reset)
                if ret != 0:
                    raise Exception('Cannot reset GUC initial value, please check it.')
            self.logger.info('Test successfully')
            # open guc and monitor log real-time size
            start_time = self.monitor_log_size(guc_reset)
            # extract log
            self.extract_log(start_time)
            # index advise
            self.execute_index_advisor()
        except Exception as e:
            self.logger.error(e)
            guc_reset = 'gs_guc reload -Z datanode -D %s -c "log_min_duration_statement = %s" && ' \
                        'gs_guc reload -Z datanode -D %s -c "log_statement= %s"' % \
                        (self._kwargs['datanode'], self._kwargs['log_min_duration_statement'],
                         self._kwargs['datanode'], self._kwargs['log_statement'])
            self.execute_cmd(guc_reset)

    def start_service(self):
        # check service is running or not.
        if os.path.isfile(self.pid_file):
            pid = self.check_proc_exist("index_server")
            if pid:
                raise Exception("Error: Process already running, can't start again.")
            else:
                os.remove(self.pid_file)

        # get listen host and port
        self.logger.info("Start service...")
        # write process pid to file
        if not os.path.isdir(os.path.dirname(self.pid_file)):
            os.makedirs(os.path.dirname(self.pid_file), 0o700)
        with open(self.pid_file, mode='w') as f:
            f.write(str(os.getpid()))

        self.logger.info("Index advisor execution intervals is: %sh" %
                         self._kwargs['index_intervals'])
        index_recommendation_thread = RepeatTimer(self._kwargs['index_intervals']*60*60,
                                           self.execute_index_recommendation)
        self.logger.info("Start timer...")
        index_recommendation_thread.start()


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


def parse_check_conf(config_path):
    config = ConfigParser()
    config.read(config_path)
    config_dict = dict()
    config_dict['app_name'] = config.get("server", "app_name")
    config_dict['database'] = config.get("server", "database")
    config_dict['port'] = config.get("server", "port")
    config_dict['host'] = config.get("server", "host")
    config_dict['user'] = config.get("server", "user")
    config_dict['wl_user'] = config.get("server", "workload_user")
    config_dict['schema'] = config.get("server", "schema")
    config_dict['max_index_num'] = config.getint("server", "max_index_num")
    config_dict['max_index_storage'] = config.get("server", "max_index_storage")
    config_dict['driver'] = config.getboolean("server", "driver")
    config_dict['index_intervals'] = config.getint("server", "index_intervals")
    config_dict['sql_amount'] = config.getint("server", "sql_amount")
    config_dict['output_sql_file'] = config.get("server", "output_sql_file")
    config_dict['datanode'] = config.get("server", "datanode")
    config_dict['pg_log_path'] = config.get("server", "pg_log_path")
    config_dict['ai_monitor_url'] = config.get("server", "ai_monitor_url")
    config_dict['max_generate_log_size'] = config.getfloat("server", "max_generate_log_size")
    config_dict['statement'] = config.getboolean("server", "statement")
    config_dict['log_min_duration_statement'] = config.get("server", "log_min_duration_statement")
    config_dict['log_statement'] = config.get("server", "log_statement")
    if not config_dict['log_min_duration_statement'] or \
            not re.match(r'[a-zA-Z0-9]+', config_dict['log_min_duration_statement']):
        raise ValueError("Please enter a legal value of [log_min_duration_statement]")
    legal_log_statement = ['none', 'all', 'ddl', 'mod']
    if config_dict['log_statement'] not in legal_log_statement:
        raise ValueError("Please enter a legal value of [log_statement]")
    return config_dict


def manage_service():
    config_path = os.path.join(current_dirname, 'database-info.conf')
    config_dict = parse_check_conf(config_path)
    LOGGER = CreateLogger("debug", "start_service.log").create_log(config_dict.get('output_sql_file'))
    server_pid_file = os.path.join(current_dirname, 'index_server.pid')
    password = read_input_from_pipe()
    IndexServer(server_pid_file, LOGGER, password, **config_dict).start_service()


def main():
    try:
        manage_service()
    except Exception as err_msg:
        print(err_msg)
        sys.exit(1)


if __name__ == '__main__':
    main()


