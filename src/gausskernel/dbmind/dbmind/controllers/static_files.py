# Copyright (c) 2022 Huawei Technologies Co.,Ltd.
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
from starlette.responses import RedirectResponse

from dbmind.common.http import request_mapping
from dbmind.common.utils import dbmind_assert


# Serve static assets.
# It would be better if the user deploys with Nginx.
@request_mapping('/', methods=['GET'])
def index(req):
    dbmind_assert(req)
    return RedirectResponse(url='/index.html')
