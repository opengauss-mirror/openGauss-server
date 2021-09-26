#!/usr/bin/env python3
# -*- coding:utf-8 -*-
#############################################################################
# Copyright (c): 2021, Huawei Tech. Co., Ltd.
# FileName     : aes_cbs_util.py
# Version      : V1.0.0
# Date         : 2021-03-01
# Description  : aes_cbs_util
#############################################################################
import sys
import hashlib

try:
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    from cryptography.hazmat.backends import default_backend
except Exception as err:
    sys.stdout.write(str(err))


class AesCbcUtil(object):
    @staticmethod
    def aes_cbc_decrypt_with_path(path):
        with open(path + '/server.key.cipher', 'rb') as f:
            cipher_txt = f.read()
        with open(path + '/server.key.rand', 'rb') as f:
            rand_txt = f.read()
        if cipher_txt is None or cipher_txt == "":
            return None
        server_vector_cipher_vector = cipher_txt[16 + 1:16 + 1 + 16]
        # pre shared key rand
        server_key_rand = rand_txt[:16]
        # worker key
        server_decrypt_key = hashlib.pbkdf2_hmac('sha256', server_key_rand,
                                                 server_vector_cipher_vector, 10000,
                                                 16)
        enc = AesCbcUtil.aes_cbc_decrypt(cipher_txt, server_decrypt_key)
        return enc

    @staticmethod
    def aes_cbc_decrypt(content, key):
        AesCbcUtil.check_content_key(content, key)
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
        dec_content = dec_content.rstrip(b'\x00')[:-1].decode()
        return dec_content

    @staticmethod
    def check_content_key(content, key):
        if not (type(content) == bytes):
            raise Exception("content's type must be bytes.")
        elif not (type(key) in (bytes, str)):
            raise Exception("bytes's type must be in (bytes, str).")
        iv_len = 16
        if not (len(content) >= (iv_len + 16)):
            raise Exception("content's len must >= (iv_len + 16).")

    @staticmethod
    def aes_cbc_decrypt_with_multi(root_path):
        """
        decrypt message with multi depth
        """
        num = 0
        decrypt_str = ""
        while True:
            path = root_path + "/key_" + str(num)
            part = AesCbcUtil.aes_cbc_decrypt_with_path(path)
            if part is None or part == "":
                break
            elif len(part) < 15:
                decrypt_str = decrypt_str + AesCbcUtil.aes_cbc_decrypt_with_path(path)
                break
            else:
                decrypt_str = decrypt_str + AesCbcUtil.aes_cbc_decrypt_with_path(path)

            num = num + 1
        if decrypt_str == "":
            return None
        return decrypt_str




