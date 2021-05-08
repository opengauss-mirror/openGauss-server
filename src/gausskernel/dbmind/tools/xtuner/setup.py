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

import os
import sys
import platform

from setuptools import setup, find_packages


def read_requirements():
    """Parse requirements.txt."""
    if 'aarch64' in platform.uname().machine:
        filepath = os.path.join('.', 'requirements-aarch64.txt')
    else:
        filepath = os.path.join('.', 'requirements-x86.txt')
    with open(filepath, 'r') as f:
        requirements = [_line.rstrip() for _line in f]
    requirements.reverse()
    return requirements

def check_version():
    version_info = sys.version_info
    major, minor = version_info.major, version_info.minor
    # At least, the Python version is (3, 6)
    if major < 3 or minor <= 5:
        return False
    return True

if not check_version():
    print("Requires Python >= 3.6")
    exit(-1)

# Read the package information from the main.py.
pkginfo = dict()
with open(os.path.join('tuner', 'main.py')) as pkginfo_fp:
    for line in pkginfo_fp.readlines():
        if line.startswith(('__version__', '__description__')):
            exec(line, pkginfo)

setup(
    name="openGauss-xtuner",
    version=pkginfo['__version__'],
    description=pkginfo['__description__'],
    author="Huawei Technologies Co.,Ltd.",
    url='https://gitee.com/opengauss/openGauss-server',
    license='Mulan PSL v2',
    install_requires=read_requirements(),
    packages=find_packages(exclude='test'),
    package_data={'': ['*']},
    entry_points={
        'console_scripts': [
            'gs_xtuner = tuner.main: main',
        ],
    },
)
