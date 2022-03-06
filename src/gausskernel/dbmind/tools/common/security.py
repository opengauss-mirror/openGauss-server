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
import base64
import hmac
import random
import secrets
import string

from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad


def check_path_valid(path):
    char_black_list = (' ', '|', ';', '&', '$', '<', '>', '`', '\\',
                       '\'', '"', '{', '}', '(', ')', '[', ']', '~',
                       '*', '?', '!', '\n')

    if path.strip() == '':
        return True

    for char in char_black_list:
        if path.find(char) >= 0:
            return False

    return True


def unsafe_random_string(length):
    """Used to generate a fixed-length random
    string which is not used in the sensitive scenarios."""
    alphabet = string.ascii_letters + string.digits
    return ''.join(random.choice(alphabet) for _ in range(length))


def safe_random_string(length):
    """Used to generate a fixed-length random
    string which is used in the security and cryptography."""
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))


def generate_an_iv() -> str:
    """Generate and return an initialization vector for AES."""
    return safe_random_string(16)


def encrypt(s1: str, s2: str, iv: str, pt: str) -> str:
    """Encrypt a series of plain text with two strings.
    :param s1: string #1
    :param s2: string #2
    :param iv: initialization vector, used by AES256-CBC
    :param pt: plain text
    :return: cipher text
    """
    if pt == '':
        return ''
    nb = 16  # the number of block including cipher and plain text
    h = hmac.new(s1.encode(), s2.encode(), digestmod='sha256')
    master_key = h.hexdigest()[:32].encode()  # 32 bytes means AES256
    cipher = AES.new(master_key, AES.MODE_CBC, iv.encode())
    pt = pt.encode()
    ct = cipher.encrypt(pad(pt, nb))
    return base64.b64encode(ct).decode()


def decrypt(s1: str, s2: str, iv: str, ct: str) -> str:
    """Decrypt a series of cipher text with two strings.
    :param s1: string #1
    :param s2: string #2
    :param iv: initialization vector, used by AES256-CBC
    :param ct: cipher text
    :return: plain text
    """
    if ct == '':
        return ''
    nb = 16  # the number of block including cipher and plain text
    h = hmac.new(s1.encode(), s2.encode(), digestmod='sha256')
    master_key = h.hexdigest()[:32].encode()  # 32 bytes means AES256
    cipher = AES.new(master_key, AES.MODE_CBC, iv.encode())
    ct = base64.b64decode(ct)
    pt = unpad(cipher.decrypt(ct), nb)
    return pt.decode()
