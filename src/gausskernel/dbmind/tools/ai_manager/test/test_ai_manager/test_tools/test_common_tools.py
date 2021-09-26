import time
import pytest
import os
from unittest.mock import patch
from unittest.mock import mock_open
from ai_manager.tools.common_tools import CommonTools
VERSION_INFO_CORRECT = """GaussDB-Kernel-V500R001C20
92.301
cbafe281"""
VERSION_INFO_WRONG = """GaussDB-Kernel-V500R001C20
92.301"""
JSON_INFO_CORRECT = """{"A":"B"}"""
JSON_INFO_WRONG = """{"A":'B'}"""
PATH_CHECK_LIST = ["|", ";", "&", "$", "<", ">", "`", "\\", "'", "\"",
                   "{", "}", "(", ")", "[", "]", "~", "*", "?", " ", "!", "\n"]


class TestCommonTools:

    @patch("builtins.open", new_callable=mock_open, read_data=VERSION_INFO_CORRECT)
    def test_correct_get_version_info_from_file(self, mock_read_file):
        version = CommonTools.get_version_info_from_file('./test')
        assert version == '92301'

    @patch("builtins.open", new_callable=mock_open, read_data=VERSION_INFO_WRONG)
    def test_wrong_get_version_info_from_file(self, mock_read_file):
        with pytest.raises(Exception, match=r"is wrong."):
            CommonTools.get_version_info_from_file('./test')

    @patch('subprocess.getstatusoutput')
    def test_extract_file_to_dir(self, result):
        result.return_value = (1, 'aa')
        with pytest.raises(Exception, match=r"extract file"):
            CommonTools.extract_file_to_dir('path1', 'path2')

    @patch('subprocess.getstatusoutput')
    def test_copy_file_to_dest_path(self, result):
        result.return_value = (1, 'aa')
        with pytest.raises(Exception, match=r"copy files"):
            CommonTools.copy_file_to_dest_path('path1', 'path2')

    def test_check_dir_access(self):
        # current path check full authority
        CommonTools.check_dir_access('./', 'full')
        # check exist
        with pytest.raises(Exception, match=r"can not access"):
            CommonTools.check_dir_access('./notexitpath', 'exist')
        # check rw
        with pytest.raises(Exception, match=r"can not access"):
            CommonTools.check_dir_access('./notexitpath', 'rw')
        # check wrong params
        with pytest.raises(Exception, match=r"is wrong"):
            CommonTools.check_dir_access('./notexitpath', 'wrong')

    @patch('subprocess.getstatusoutput')
    def test_mkdir_with_mode(self, result):
        result.return_value = (1, 'aa')
        with pytest.raises(Exception, match=r"mkdir"):
            CommonTools.mkdir_with_mode('path1', '0700')

    @patch('ai_manager.tools.common_tools.CommonTools.remote_execute_cmd')
    def test_remote_mkdir_with_mode(self, result):
        result.return_value = (1, 'aa')
        with pytest.raises(Exception, match=r"remote mkdir"):
            CommonTools.remote_mkdir_with_mode('path', 'mode', 'ip', 'username', 'password')

    @patch('subprocess.getstatusoutput')
    def test_clean_dir(self, result):
        result.return_value = (1, 'aa')
        with pytest.raises(Exception, match=r"clean dir"):
            CommonTools.clean_dir('path')

    @patch('ai_manager.tools.common_tools.CommonTools.remote_execute_cmd')
    def test_remote_clean_dir(self, result):
        result.return_value = (1, 'aa')
        with pytest.raises(Exception, match=r"remote mkdir"):
            CommonTools.remote_clean_dir('ip', 'username', 'password', 'cmd')

    @patch('subprocess.getstatusoutput')
    def test_remove_files(self, result):
        result.return_value = (1, 'aa')
        with pytest.raises(Exception, match=r"remove file"):
            CommonTools.remove_files('path')

    @patch("builtins.open", new_callable=mock_open, read_data=JSON_INFO_CORRECT)
    @patch("os.path.isfile")
    def test_correct_json_file_to_dict(self, is_file, mock_read_file):
        is_file.return_value = True
        ret = CommonTools.json_file_to_dict('/test')
        assert ret == {'A': 'B'}

    @patch('subprocess.getstatusoutput')
    def test_add_cron(self, result):
        result.return_value = (1, 'aa')
        CommonTools.add_cron('install_path', 'cmd', 'frequency')

    @patch('subprocess.getstatusoutput')
    def test_del_cron(self, result):
        result.return_value = (1, 'aa')
        CommonTools.del_cron('install_path', 'cmd', 'frequency')

    def test_delete_early_record(self):
        with pytest.raises(Exception, match=r"is not exist."):
            CommonTools.delete_early_record('path', 1024)

    @patch("builtins.open", new_callable=mock_open, read_data=VERSION_INFO_WRONG)
    def test_read_last_line_from_file(self, mock_read_file):
        ret = CommonTools.read_last_line_from_file('./')
        assert ret == '92.301'

    @patch('subprocess.getstatusoutput')
    def test_grep_process_and_kill(self, result):
        result.return_value = (1, 'aa')
        CommonTools.grep_process_and_kill('process')

    @patch('subprocess.getstatusoutput')
    def test_check_is_root(self, result):
        result.return_value = True
        ret = CommonTools.check_is_root()
        assert ret in [True, False]

    @patch('subprocess.getstatusoutput')
    def test_check_process(self, result):
        result.return_value = (1, 'aa')
        with pytest.raises(Exception, match=r"check process"):
            CommonTools.check_process('path')
        result.return_value = (0, 'aa')
        with pytest.raises(Exception, match=r"check process num"):
            CommonTools.check_process('path')
        result.return_value = (0, '5')
        ret = CommonTools.check_process('path')
        assert ret == 5

    def test_check_path_valid(self):
        for item in PATH_CHECK_LIST:
            wrong_path = os.path.realpath('./') + item
            with pytest.raises(Exception, match=r"There are illegal character"):
                CommonTools.check_path_valid(wrong_path)

    def test_get_funcs(self):
        from ai_manager.tools import params_checkers
        from ai_manager.tools.params_checkers import check_ip
        ret = CommonTools.get_funcs(params_checkers)
        assert check_ip in ret.values()

    def test_parallel_execute(self):
        para_list = [0.5] * 10
        start = time.time()
        CommonTools.parallel_execute(lambda x: time.sleep(x), para_list)
        end = time.time()
        assert end - start < 0.5 * 10

    @patch('ai_manager.tools.common_tools.CommonTools.get_status_output_error')
    def test_remote_copy_files(self, result):
        result.return_value = (1, 'aa')
        CommonTools.remote_copy_files('remote_ip', 'user', 'password', 'path_from', 'path_to')

    @patch('ai_manager.tools.common_tools.CommonTools.get_status_output_error')
    def test_remote_execute_cmd(self, result):
        result.return_value = (1, 'aa')
        CommonTools.remote_execute_cmd('remote_ip', 'user', 'password', 'cmd')

    @patch('ai_manager.tools.common_tools.CommonTools.get_status_output_error')
    def test_get_local_ips(self, result):
        result.return_value = (0, "eth0 : df df 10.10.72.57 test "
                                  "string 10.10.72.57 test string 10.10.7.5")
        ret = CommonTools.get_local_ips()
        assert ret.sort() == ['10.10.72.57', '10.10.7.5'].sort()

    @patch("ai_manager.tools.common_tools.CommonTools.get_local_ips")
    def test_modify_agent_config_file(self, ips):
        ips.return_value = ['10.10.72.57', '10.10.7.5']
        content = {"security": {"tls": "False"}}
        with pytest.raises(Exception, match=r"not found in the config file or section"):
            CommonTools.modify_agent_config_file('config_path', content, allow_add=False)

    @patch("ai_manager.tools.common_tools.CommonTools.get_local_ips")
    def test_modify_config_file(self, ips):
        ips.return_value = ['10.10.72.57', '10.10.7.5']
        content = {"security": {"tls": "False"}}
        with pytest.raises(Exception, match=r"not found in the config file or section"):
            CommonTools.modify_config_file('config_path', content, allow_add=False)

    @patch('subprocess.getstatusoutput')
    def test_get_current_usr(self, result):
        result.return_value = (1, 'a')
        with pytest.raises(Exception, match=r"get user"):
            CommonTools.get_current_usr()

    @patch('os.mknod')
    def test_create_file_if_not_exist(self, result):
        ret = CommonTools.create_file_if_not_exist('path')
        assert ret is True

    @patch('subprocess.getstatusoutput')
    def test_read_info_from_config_file(self, result):
        result.return_value = (1, 'a')
        with pytest.raises(Exception, match=r"not found in the config file or section"):
            CommonTools.read_info_from_config_file('file_path', 'section', 'option')

    @patch('subprocess.getstatusoutput')
    def test_get_local_ip(self, result):
        result.return_value = (0, 'eth0 : inet aa 10.30.30.30 bb eth1: cc dd 10.10.10.10')
        ret = CommonTools.get_local_ip(ignore=True)
        assert ret == '127.0.0.1'
        result.return_value = {0, """eth0: flags=4163<UP,BROADCAST,RUNNING,MULTICAST>  mtu 1500
        inet 10.30.30.30  netmask 255.255.254.0"""}
        ret = CommonTools.get_local_ip(ignore=True)
        assert ret == '10.30.30.30'

    @patch("ai_manager.tools.common_tools.CommonTools.chmod_files_with_execute_permission")
    @patch("platform.machine")
    @patch('ai_manager.tools.common_tools.CommonTools.get_status_output_error')
    def test_encrypt_with_path(self, result, plat, execute):
        with pytest.raises(Exception, match=r"Unsupported operating system"):
            CommonTools.encrypt_with_path('password', 'path')
        plat.return_value = 'x86_64'
        result.return_value = (1, 'a')
        with pytest.raises(Exception, match=r"Failed to encrypt random string"):
            CommonTools.encrypt_with_path('password', 'path')

    @patch('ai_manager.tools.common_tools.CommonTools.get_status_output_error')
    def test_chmod_files_with_execute_permission(self, result):
        result.return_value = (1, 'a')
        with pytest.raises(Exception, match=r"change file authority"):
            CommonTools.chmod_files_with_execute_permission(['file_list'])













