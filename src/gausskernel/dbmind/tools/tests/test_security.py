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
import random

from dbmind.common import security


def test_encryption_and_decryption():
    for i in range(100):
        s1 = security.safe_random_string(16)
        s2 = security.safe_random_string(16)
        # Test whether the function supports unfixed length.
        plain = security.unsafe_random_string(random.randint(0, 64))
        iv = security.generate_an_iv()
        cipher = security.encrypt(s1, s2, iv, plain)
        decrypted_text = security.decrypt(s1, s2, iv, cipher)
        assert plain == decrypted_text
