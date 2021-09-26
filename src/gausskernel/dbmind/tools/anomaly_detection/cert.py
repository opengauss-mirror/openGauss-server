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
import ssl
import subprocess
from datetime import datetime

import dateutil.parser


def check_certificate(certificate_path):
    """
    Check whether the certificate is expired or invalid.
    :param certificate_path: path of certificate.
    output: dict, check result which include 'check status' and 'check information'.
    """
    check_result = {}
    certificate_warn_threshold = 365
    child = subprocess.Popen(['openssl', 'x509', '-in', certificate_path, '-noout', '-dates'],
                             shell=False, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
    sub_chan = child.communicate()
    if sub_chan[1] or not sub_chan[0]:
        check_result['status'] = 'fail'
    else:
        check_result['status'] = 'success'
        not_after = sub_chan[0].decode('utf-8').split('\n')[1].split('=')[1].strip()
        end_time = dateutil.parser.parse(not_after).replace(tzinfo=None)
        certificate_remaining_days = (end_time - datetime.now()).days
        if 0 < certificate_remaining_days < certificate_warn_threshold:
            check_result['level'] = 'warn'
            check_result['info'] = "the '{certificate}' has {certificate_remaining_days} days before out of date." \
                .format(certificate=certificate_path,
                        certificate_remaining_days=certificate_remaining_days)
        elif certificate_remaining_days >= certificate_warn_threshold:
            check_result['level'] = 'info'
            check_result['info'] = "the '{certificate}' has {certificate_remaining_days} days before out of date." \
                .format(certificate=certificate_path,
                        certificate_remaining_days=certificate_remaining_days)
        else:
            check_result['level'] = 'error'
            check_result['info'] = "the '{certificate}' is out of date.".format(certificate=certificate_path)
    return check_result


def get_agent_ssl_context(params):
    context = None
    if {'agent_cert', 'agent_key', 'ca', 'cert_pwd'}.issubset(set(params.keys())):
        cert = params['agent_cert']
        key = params['agent_key']
        pwd = params['cert_pwd']
        ca = params['ca']
        ssl_certificate_status = check_certificate(cert)
        ca_certificate_status = check_certificate(ca)
        if ssl_certificate_status['status'] == 'fail' or ca_certificate_status['status'] == 'fail':
            logging.error("error occur when check 'certificate'.")
            return
        else:
            if ssl_certificate_status['level'] == 'error' or ca_certificate_status['level'] == 'error':
                logging.error("{ssl_certificate_info}; {ca_certificate_info}"
                              .format(ssl_certificate_info=ssl_certificate_status['info'],
                                      ca_certificate_info=ca_certificate_status['info']))
                return
            else:
                logging.warning("{ssl_certificate_info}; {ca_certificate_info}"
                                .format(ssl_certificate_info=ssl_certificate_status['info'],
                ca_certificate_info=ca_certificate_status['info']))
                context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca)
                context.check_hostname = False
                context.load_cert_chain(certfile=cert, keyfile=key, password=pwd)

    return context


def get_server_ssl_context(params):
    context = None
    if {'server_cert', 'server_key', 'ca', 'cert_pwd'}.issubset(set(params.keys())):
        cert = params['server_cert']
        key = params['server_key']
        pwd = params['cert_pwd']
        ca = params['ca']
        ssl_certificate_status = check_certificate(cert)
        ca_certificate_status = check_certificate(ca)
        if ssl_certificate_status['status'] == 'fail' or ca_certificate_status['status'] == 'fail':
            logging.error("error occur when check 'certificate'.")
            return
        else:
            if ssl_certificate_status['level'] == 'error' or ca_certificate_status['level'] == 'error':
                logging.error("{ssl_certificate_info}; {ca_certificate_info}"
                              .format(ssl_certificate_info=ssl_certificate_status['info'],
                                      ca_certificate_info=ca_certificate_status['info']))
                return
            else:
                logging.warning("{ssl_certificate_info}; {ca_certificate_info}"
                                .format(ssl_certificate_info=ssl_certificate_status['info'],
                                        ca_certificate_info=ca_certificate_status['info']))
                context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH, cafile=ca)
                context.verify_mode = ssl.CERT_REQUIRED
                context.load_cert_chain(certfile=cert, keyfile=key, password=pwd)

    return context
