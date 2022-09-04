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
import importlib
import os
import pkgutil
import sys

from dbmind.common.utils import where_am_i


def list_components():
    """Return all components in current directory."""
    curr_dir = os.path.realpath(os.path.dirname(__file__))
    components = list(
        map(lambda tup: tup[1],
            pkgutil.iter_modules((curr_dir,)))
    )

    return components


def call_component(name, arguments):
    component = importlib.import_module('.' + name, where_am_i(globals()))
    if not hasattr(component, 'main'):
        print('FATAL: Component %s must define function main() in the __init__.py.',
              file=sys.stderr)
        exit(1)
    component.main(arguments)
