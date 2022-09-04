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
import ctypes
import logging
import sys

# Explicitly mask sub-command's process title.
if len(sys.argv) > 1 and sys.argv[1] == 'set':
    new_name = b'DBMind Setting'
    try:
        libc = ctypes.CDLL('libc.so.6')
        progname = ctypes.c_char_p.in_dll(libc, '__progname_full')
        with open('/proc/self/cmdline') as fp:
            old_progname_len = len(fp.readline())
        if old_progname_len > len(new_name):
            # padding blank chars
            new_name += b' ' * (old_progname_len - len(new_name))

        libc.strcpy(progname, ctypes.c_char_p(new_name))
        buff = ctypes.create_string_buffer(len(new_name) + 1)
        buff.value = new_name
        libc.prctl(15, ctypes.byref(buff), 0, 0, 0)
    except Exception as e:
        logging.warning(
            'Cannot mask the process title due to %s. '
            'There may be a security risk, please take notice of it.', e
        )

try:
    from dbmind.cmd import main
except ImportError:
    import sys
    import os

    curr_path = os.path.dirname(os.path.realpath(__file__))
    root_path = os.path.dirname(curr_path)
    sys.path.append(root_path)
    from dbmind.cmd import main

main()
