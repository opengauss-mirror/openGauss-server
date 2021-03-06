-- clean before test
\! gs_ktool -d all
\! rm -rf export_import_ksf && mkdir export_import_ksf


-- generate cmk
\! gs_ktool -g
\! gs_ktool -g
\! gs_ktool -g


-- generate cmk with set len
\! gs_ktool -g -l 1
\! gs_ktool -g -l 32
\! gs_ktool -g -l 99
\! gs_ktool -g -l 112


-- select cmk
\! gs_ktool -s 1
\! gs_ktool -s 2
\! gs_ktool -s 6
\! gs_ktool -s all


-- delete cmk (&& generate to be deleted)
\! gs_ktool -d 3
\! gs_ktool -d 6
\! gs_ktool -g && gs_ktool -d all
\! gs_ktool -g && gs_ktool -d all && gs_ktool -d all
--
\! gs_ktool -s all


\! gs_ktool -g && gs_ktool -g && gs_ktool -g
-- export cmk
\! gs_ktool -e -p gs_ktool_123
\! gs_ktool -e -f ./export_import_ksf/export_ksf_1.dat -p g...123..
\! gs_ktool -e -f ./export_import_ksf/export_ksf_2.dat -p GGGGG...1


-- import cmk
\! gs_ktool -i -f ./export_import_ksf/export_ksf_2.dat -p GGGGG...1

-- select rk
\! gs_ktool -R -s
\! gs_ktool -R -s


-- update rk
\! gs_ktool -R -u
\! gs_ktool -R -u


-- print version
\! gs_ktool -v


-- print help
\! gs_ktool -h
\! gs_ktool -?


-- uninstall gs_ktool (cannot support to test this command)
-- \! gs_ktool -u


-- clean after test
\! gs_ktool -d all
\! rm -rf export_import_ksf