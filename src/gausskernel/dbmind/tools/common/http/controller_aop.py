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


import json
from functools import wraps

skip_filter_paths = ["/metrics", "/favicon.ico"]


def do_before():
    """Nothing"""

def do_after(rt_result):
    """Nothing"""

def do_exception(exception):
    """Nothing"""

def around(func, *args, **kw):
    @wraps(func)
    def wrapper():
        do_before()
        try:
            rt_result = func(*args, **kw)
            final_rt = do_after(rt_result)
        except BaseException as exception:
            final_rt = do_exception(exception)
        return final_rt

    return wrapper
