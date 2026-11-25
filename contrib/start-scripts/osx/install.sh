# Copyright (c) 2026 Huawei Technologies Co., Ltd.
# openGauss-server is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
# Portions Copyright (c) 2001-2004, PostgreSQL Global Development Group

sudo sh -c 'echo "POSTGRESQL=-YES-" >> /etc/hostconfig'
sudo mkdir /Library/StartupItems/PostgreSQL
sudo cp PostgreSQL /Library/StartupItems/PostgreSQL
sudo cp StartupParameters.plist /Library/StartupItems/PostgreSQL
if [ -e /Library/StartupItems/PostgreSQL/PostgreSQL ]
then
  echo "Startup Item Installed Successfully . . . "
  echo "Starting PostgreSQL Server . . . "
  SystemStarter restart PostgreSQL
fi
