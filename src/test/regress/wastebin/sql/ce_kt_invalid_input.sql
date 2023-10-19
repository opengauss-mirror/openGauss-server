-- clean before test
\! gs_ktool -d all
\! rm -rf export_import_ksf && mkdir export_import_ksf


-- genereate with set invalid len (support [1, 112])
\! gs_ktool -g -l -1
\! gs_ktool -g -l 0
\! gs_ktool -g -l 15
\! gs_ktool -g -l 113
\! gs_ktool -g -l 999999999
\! gs_ktool -g -l .
\! gs_ktool -g -l g
\! gs_ktool -g -l gs_ktool
\! gs_ktool -g -l 32g
\! gs_ktool -g -l gs_ktool...
\! gs_ktool -g -l
\! gs_ktool -g 32
\! gs_ktool -g -l -l 32
\! gs_ktool -g -d 
\! gs_ktool -g -s all


\! gs_ktool -d all && gs_ktool -g && gs_ktool -g
-- delete nonexistent cmk
\! gs_ktool -d 0
\! gs_ktool -d -1
\! gs_ktool -d 3
\! gs_ktool -d 100
\! gs_ktool -d 1g
\! gs_ktool -d gs_ktool
\! gs_ktool -d ..
\! gs_ktool -d 1.
\! gs_ktool -d -d
\! gs_ktool -d -g
\! gs_ktool -d 1 -g
\! gs_ktool -d -s 1
\! gs_ktool -d all0
\! gs_ktool -d al
\! gs_ktool -d
\! gs_ktool -d all && gs_ktool -d 1
\! gs_ktool -d 0


\! gs_ktool -d all && gs_ktool -g && gs_ktool -g
-- select nonexistent cmk
\! gs_ktool -s
\! gs_ktool -s -1
\! gs_ktool -s 0
\! gs_ktool -s 3
\! gs_ktool -s 9999999
\! gs_ktool -s 1.
\! gs_ktool -s @
\! gs_ktool -s gs_ktool
\! gs_ktool -s all1
\! gs_ktool -s all_
\! gs_ktool -s -g
\! gs_ktool -s -s
\! gs_ktool -s 1 -s 1
\! gs_ktool -s all -g


\! rm -f ./export_import_ksf/export_ksf.dat &&  gs_ktool -d all && gs_ktool -g && gs_ktool -g
-- exprot cmk err
\! gs_ktool -e -f ./export_import_ksf/export_ksf_1.dat -p
\! gs_ktool -e -f ./export_import_ksf/export_ksf_2.dat -p shrot_1
\! gs_ktool -e -f ./export_import_ksf/export_ksf_3.dat -p only_two_type
\! gs_ktool -e -f ./export_import_ksf/export_ksf_4.dat -p only2type
\! gs_ktool -e -p loss_para_f_1
\! gs_ktool -e -f ./export_import_ksf/export_ksf_5.dat -p unexpected_para_1 -g
\! gs_ktool -e -f ./export_import_ksf/export_ksf_6.dat -p -p two_para_p_1
\! gs_ktool -e -f ./export_import_ksf/export_ksf_7.dat -p two_para_p_1 -p two_para_p_1
\! ls ./export_import_ksf/


\! rm -f ./export_import_ksf/export_ksf.dat &&  gs_ktool -d all 
\! gs_ktool -g && gs_ktool -e -f ./export_import_ksf/import_ksf.dat -p gauss_1234
-- import cmk err
\! gs_ktool -i -f ./export_import_ksf/import_ksf.dat -p
\! gs_ktool -i -f ./export_import_ksf/import_ksf.dat -p err_passwd_1234
\! gs_ktool -i -f ./export_import_ksf/import_ksf.dat -p gauss_1234_
\! gs_ktool -i -f ./export_import_ksf/import_ksf.dat -p gauss_12
\! gs_ktool -i -f ./export_import_ksf/import_ksf.dat -p short_1
\! gs_ktool -i -f -p gauss_1234
\! gs_ktool -i -f ./export_import_ksf/import_ksf.dat -p gauss_1234 -g


\! gs_ktool -d all
-- select rk err
\! gs_ktool -s -R
\! gs_ktool -R -s 0
\! gs_ktool -R -s 1
\! gs_ktool -R -s -s
\! gs_ktool -R -g


-- update rk err
\! gs_ktool -R -u 0
\! gs_ktool -R -u -u
\! gs_ktool -R -R -u


-- print help
\! gs_ktool -h -g
\! gs_ktool -h -h
\! gs_ktool -h 0


-- print version 
\! gs_ktool -v -v
\! gs_ktool -v 0
\! gs_ktool -V


-- uninstall gs_ktool (cannot support to test this command)
-- \! gs_ktool -u -u


-- clean after test
\! gs_ktool -d all
\! rm -rf export_import_ksf
