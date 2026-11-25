#!/bin/bash
# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

# run_python.sh - a wrapper script that directly replaces the python command

# find available Python interpreters
PYTHON_CMD=$(command -v python3 2>/dev/null || command -v python 2>/dev/null)

if [ -z "$PYTHON_CMD" ]; then
    echo "error: No Python interpreter found (python3 or python)" >&2
    exit 1
fi

# execute the Python interpreter directly and pass all parameters.
exec $PYTHON_CMD "$@"
