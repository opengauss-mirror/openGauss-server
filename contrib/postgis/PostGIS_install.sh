#!/bin/bash
#
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
# ---------------------------------------------------------------------------------------
# 
# sh
#        Distribute compiled binaries among all nodes
#        Return 0 means OK.
#        Return 1 means failed.
# 
# IDENTIFICATION
#        contrib/postgis/PostGIS_install.sh
# 
# ---------------------------------------------------------------------------------------

mv $GAUSSHOME/lib/postgresql/postgis-2.4.so $GAUSSHOME/install/postgis-2.4.so
python $GAUSSHOME/bin/transfer.py 1 $GAUSSHOME/install/postgis-2.4.so $GAUSSHOME/lib/postgresql/postgis-2.4.so
python $GAUSSHOME/bin/transfer.py 1 $GAUSSHOME/install/json/lib/libjson-c.so.2 $GAUSSHOME/lib/libjson-c.so.2
python $GAUSSHOME/bin/transfer.py 1 $GAUSSHOME/install/geos/lib/libgeos_c.so.1 $GAUSSHOME/lib/libgeos_c.so.1
python $GAUSSHOME/bin/transfer.py 1 $GAUSSHOME/install/proj/lib/libproj.so.9 $GAUSSHOME/lib/libproj.so.9
python $GAUSSHOME/bin/transfer.py 1 $GAUSSHOME/install/geos/lib/libgeos-3.6.2.so $GAUSSHOME/lib/libgeos-3.6.2.so
python $GAUSSHOME/bin/transfer.py 1 $GAUSSHOME/install/postgis2.4.2/lib/liblwgeom-2.4.so.0 $GAUSSHOME/lib/liblwgeom-2.4.so.0
python $GAUSSHOME/bin/transfer.py 1 $GAUSSHOME/postgis-xc/postgis-2.4.2/postgis--2.4.2.sql $GAUSSHOME/share/postgresql/extension/postgis--2.4.2.sql
python $GAUSSHOME/bin/transfer.py 1 $GAUSSHOME/postgis-xc/postgis-2.4.2/postgis.control $GAUSSHOME/share/postgresql/extension/postgis.control
python $GAUSSHOME/bin/transfer.py 1 $GAUSSHOME/bin/pgsql2shp $GAUSSHOME/bin/pgsql2shp
python $GAUSSHOME/bin/transfer.py 1 $GAUSSHOME/bin/shp2pgsql $GAUSSHOME/bin/shp2pgsql
