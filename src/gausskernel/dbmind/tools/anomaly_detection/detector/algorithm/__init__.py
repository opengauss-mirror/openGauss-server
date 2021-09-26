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


def get_fcst_alg(name):
    if name == 'fbprophet':
        from .fb_prophet import FbProphet
        return FbProphet
    elif name == 'auto_arima':
        from .auto_arima import AutoArima
        return AutoArima
    else:
        raise ValueError('No {name} time-series forecast algorithm.'.format(name=name))
