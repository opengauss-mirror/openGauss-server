"""
 openGauss is licensed under Mulan PSL v2.
 You can use this software according to the terms and conditions of the Mulan PSL v2.
 You may obtain a copy of Mulan PSL v2 at:

 http://license.coscl.org.cn/MulanPSL2

 THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 See the Mulan PSL v2 for more details.
 
 Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 Description: The certificate functions for AiEngine.
"""

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

import os
import hashlib

def check_content_key(content, key):
    if not (type(content) == bytes):
        raise Exception("content's type must be bytes.")
    elif not (type(key) in (bytes, str)):
        raise Exception("bytes's type must be in (bytes, str).")

    iv_len = 16
    if not (len(content) >= (iv_len + 16)):
        raise Exception("content's len must >= (iv_len + 16).")

def aes_cbc_decrypt(content, key):
    check_content_key(content, key)
    if type(key) == str:
        key = bytes(key)
    iv_len = 16
    # pre shared key iv
    iv = content[16 + 1 + 16 + 1:16 + 1 + 16 + 1 + 16]

    # pre shared key  enctryt
    enc_content = content[:iv_len]
    backend = default_backend()
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=backend)
    decrypter = cipher.decryptor()
    dec_content = decrypter.update(enc_content) + decrypter.finalize()
    server_decipher_key = dec_content.decode('utf-8','ignore').rstrip(b'\x00'.decode())
    return server_decipher_key

def aes_cbc_decrypt_with_path(path):
    ciper_path = os.path.realpath(path + '/server.key.cipher')
    with open(ciper_path, 'rb') as f:
        cipher_txt = f.read()
    rand_path = os.path.realpath(path + '/server.key.rand')
    with open(rand_path, 'rb') as f:
        rand_txt = f.read()
    if cipher_txt is None or cipher_txt == "":
        return None

    server_vector_cipher_vector = cipher_txt[16 + 1:16 + 1 + 16]
    # pre shared key rand
    server_key_rand = rand_txt[:16]

    # worker key
    server_decrypt_key = hashlib.pbkdf2_hmac('sha256', server_key_rand,
                                             server_vector_cipher_vector,
                                             10000, 16)
    server_key = aes_cbc_decrypt(cipher_txt, server_decrypt_key)
    return server_key
