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
# Makefile
# 
# 
# IDENTIFICATION
#        src/bin/gs_plan_simulator/Makefile
# 
# ---------------------------------------------------------------------------------------
top_builddir=../../..
include $(top_builddir)/src/Makefile.global
subdir=src/bin/gs_plan_simulator

install: all installdirs
	$(INSTALL_PROGRAM) gs_plan_simulator.sh$(X) '$(DESTDIR)$(bindir)/gs_plan_simulator.sh$(X)'
	$(INSTALL_DATA) dependences/clean_temp.sql$(X)                   '$(DESTDIR)$(bindir)/dependences/clean_temp.sql$(X)'
	$(INSTALL_DATA) dependences/initdb.py$(X)                        '$(DESTDIR)$(bindir)/dependences/initdb.py$(X)'
	$(INSTALL_DATA) dependences/restore_temp.sql$(X)                 '$(DESTDIR)$(bindir)/dependences/restore_temp.sql$(X)'
	$(INSTALL_DATA) dependences/store_pg_class_stats.sql$(X)         '$(DESTDIR)$(bindir)/dependences/store_pg_class_stats.sql$(X)'
	$(INSTALL_DATA) dependences/store_pg_statistic_ext_stats.sql$(X) '$(DESTDIR)$(bindir)/dependences/store_pg_statistic_ext_stats.sql$(X)'
	$(INSTALL_DATA) dependences/store_pg_statistic_stats.sql$(X)     '$(DESTDIR)$(bindir)/dependences/store_pg_statistic_stats.sql$(X)'
	$(INSTALL_DATA) dependences/store_pgxc_class.sql$(X)             '$(DESTDIR)$(bindir)/dependences/store_pgxc_class.sql$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)/dependences'

uninstall:
	rm -f  '$(DESTDIR)$(bindir)/gs_plan_simulator.sh$(X)'
	rm -fr '$(DESTDIR)$(bindir)/dependences$(X)'
