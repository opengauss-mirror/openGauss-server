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


class SSLContext:
    def __init__(self, cert, key, key_pwd=None, ca=None):
        self.ssl_certfile = cert
        self.ssl_keyfile = key
        self.ssl_keyfile_password = key_pwd
        self.ssl_ca_file = ca

    def __bool__(self):
        return bool(self.ssl_certfile) and bool(self.ssl_keyfile)
