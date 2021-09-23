import pytest
from collections import defaultdict


class TestParamsCheckers:

    def test_check_string(self):
        from ai_manager.tools.params_checkers import check_string
        valid_str = 'This is valid string'
        check_string(valid_str)
        invalid_str = ['invalid input because of the type is list, not string']
        with pytest.raises(Exception, match=r"should be"):
            check_string(invalid_str)

    def test_check_valid_string(self):
        from ai_manager.tools.params_checkers import check_valid_string
        valid_str = 'This is valid string'
        check_valid_string(valid_str)
        invalid_str = 'This is invalid string because of "|"'
        with pytest.raises(Exception, match=r"There are illegal character"):
            check_valid_string(invalid_str)

    def test_check_digit(self):
        from ai_manager.tools.params_checkers import check_digit
        valid_str = '123'
        check_digit(valid_str)
        invalid_str = 'This is invalid string not digit'
        with pytest.raises(Exception, match=r"digit"):
            check_digit(invalid_str)

    def test_check_list(self):
        from ai_manager.tools.params_checkers import check_list
        valid_str = ['123']
        check_list(valid_str)
        invalid_str = 'This is invalid string not list'
        with pytest.raises(Exception, match=r"list"):
            check_list(invalid_str)

    def test_check_dict(self):
        from ai_manager.tools.params_checkers import check_dict
        valid_str = {'123': 'aaa'}
        check_dict(valid_str)
        invalid_str = 'This is invalid string not dict'
        with pytest.raises(Exception, match=r"dict"):
            check_dict(invalid_str)

    def test_check_password(self):
        from ai_manager.tools.params_checkers import check_password
        valid_str = 'test@password'
        check_password(valid_str)
        invalid_str = 'test wrong password'
        with pytest.raises(Exception, match=r"password character"):
            check_password(invalid_str)

    def test_check_ip(self):
        from ai_manager.tools.params_checkers import check_ip
        valid_str = '10.13.12.111'
        check_ip(valid_str)
        invalid_str = '111.111.111.1111'
        with pytest.raises(Exception, match=r"is illegal"):
            check_ip(invalid_str)

    def test_check_port(self):
        from ai_manager.tools.params_checkers import check_port
        valid_str = '2378'
        check_port(valid_str)
        invalid_str = '66666'
        with pytest.raises(Exception, match=r"is illegal"):
            check_port(invalid_str)

    def test_check_scene(self):
        from ai_manager.tools.params_checkers import check_scene
        valid_str = 'opengauss'
        check_scene(valid_str)
        invalid_str = 'invalidsence'
        with pytest.raises(Exception, match=r"is not in the valid scene list"):
            check_scene(invalid_str)

    def test_check_path(self):
        from ai_manager.tools.params_checkers import check_path
        valid_str = '/home/Ruby/aa'
        check_path(valid_str)
        invalid_str = '/home/$aa/^bb'
        with pytest.raises(Exception, match=r"There are illegal character"):
            check_path(invalid_str)

    def test_check_module(self):
        from ai_manager.tools.params_checkers import check_module
        valid_str = 'anomaly_detection'
        check_module(valid_str)
        invalid_str = 'invalid_module'
        with pytest.raises(Exception, match=r"is illegal"):
            check_module(invalid_str)

    def test_check_action(self):
        from ai_manager.tools.params_checkers import check_action
        valid_str = 'install'
        check_action(valid_str)
        invalid_str = 'invalid_opt'
        with pytest.raises(Exception, match=r"is illegal"):
            check_action(invalid_str)

    def test_check_tls(self):
        from ai_manager.tools.params_checkers import check_tls
        valid_str = 'True'
        check_tls(valid_str)
        invalid_str = 'on'
        with pytest.raises(Exception, match=r"is illegal"):
            check_tls(invalid_str)

    def test_check_config_info(self):
        from ai_manager.tools.params_checkers import check_config_info
        valid_obj = defaultdict(dict)
        valid_obj['security']['tls'] = 'True'
        check_config_info(valid_obj)

        valid_obj['server']['host'] = '10.30.30.30'
        check_config_info(valid_obj)

        valid_obj['database']['port'] = '1936'
        check_config_info(valid_obj)

        invalid_obj = defaultdict(dict)
        invalid_obj['section_error']['port'] = '1936'
        with pytest.raises(Exception, match=r"not in the valid section list"):
            check_config_info(invalid_obj)

        invalid_obj.pop('section_error')
        invalid_obj['database']['option_error'] = 'database1'
        with pytest.raises(Exception, match=r"not in the valid option list"):
            check_config_info(invalid_obj)

        invalid_obj.pop('database')
        invalid_obj['server']['host'] = '10.30.30.301'
        with pytest.raises(Exception, match=r"is illegal"):
            check_config_info(invalid_obj)

    def test_check_agent_nodes(self):
        from ai_manager.tools.params_checkers import check_agent_nodes
        valid_obj = [defaultdict(str)]
        valid_obj[0]['node_ip'] = '10.10.10.10'
        valid_obj[0]['username'] = 'Ruby'
        valid_obj[0]['password'] = 'aaa@aaa'
        check_agent_nodes(valid_obj)

        invalid_obj = [defaultdict(str)]
        invalid_obj[0]['node_ip'] = '10.10.10.1011'
        invalid_obj[0]['username'] = 'aaa@aaa'
        invalid_obj[0]['password'] = 'aaa@aaa'
        with pytest.raises(Exception, match=r"is illegal"):
            check_agent_nodes(invalid_obj)

        invalid_obj[0]['node_ip'] = '10.10.10.101'
        invalid_obj[0]['username'] = 'aaa|aaa'
        with pytest.raises(Exception, match=r"There are illegal character"):
            check_agent_nodes(invalid_obj)

        invalid_obj[0]['username'] = 'aaa@aaa'
        invalid_obj[0]['password'] = 'aaa|aaa'
        with pytest.raises(Exception, match=r"is illegal"):
            check_agent_nodes(invalid_obj)

    def test_check_install_path(self):
        from ai_manager.tools.params_checkers import check_install_path
        valid_str = '/home/Ruby/aa'
        check_install_path(valid_str)
        invalid_str = '/home/$aa/^bb'
        with pytest.raises(Exception, match=r"There are illegal character"):
            check_install_path(invalid_str)

    def test_check_version(self):
        from ai_manager.tools.params_checkers import check_version
        valid_str = '92237'
        check_version(valid_str)
        invalid_str = '92-237'
        with pytest.raises(Exception, match=r"digit"):
            check_version(invalid_str)

    def test_check_service_list(self):
        from ai_manager.tools.params_checkers import check_service_list
        valid_str = ['%s cd %s && nohup python3 %s start --role agent']
        check_service_list(valid_str)
        invalid_str = ['test command']
        with pytest.raises(Exception, match=r"service cron params"):
            check_service_list(invalid_str)



