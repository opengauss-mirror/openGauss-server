\! rm -f $GAUSSHOME/etc/gs_ktool.log

-- 1 primary file : not exist, secondary file : not exist | succeed
\! rm -f $GAUSSHOME/etc/gs_ktool_file/*.dat && ls $GAUSSHOME/etc/gs_ktool_file/

\! gs_ktool -g && ls $GAUSSHOME/etc/gs_ktool_file/

-- 2 primary file : exist, secondary file : exist | succeed
\! gs_ktool -g && ls $GAUSSHOME/etc/gs_ktool_file/

-- 3 primary file : not exist, secondary file : exist | succeed
\! rm -f $GAUSSHOME/etc/gs_ktool_file/primary_ksf.dat && ls $GAUSSHOME/etc/gs_ktool_file/

\! gs_ktool -g && ls $GAUSSHOME/etc/gs_ktool_file/

-- 4 primary file : exist, secondary file : not exist | succeed
\! rm -f $GAUSSHOME/etc/gs_ktool_file/secondary_ksf.dat && ls $GAUSSHOME/etc/gs_ktool_file/

\! gs_ktool -g && ls $GAUSSHOME/etc/gs_ktool_file/

-- 5 primary file : tainted, secondary file ：normal | succeed
\! echo 'invalid data' > $GAUSSHOME/etc/gs_ktool_file/primary_ksf.dat && cat $GAUSSHOME/etc/gs_ktool_file/primary_ksf.dat

\! gs_ktool -g && cat $GAUSSHOME/etc/gs_ktool_file/primary_ksf.dat | grep 'invalid data'

-- 6 primary file : normal, secondary file tainted | succeed
\! echo 'invalid data' > $GAUSSHOME/etc/gs_ktool_file/secondary_ksf.dat && cat $GAUSSHOME/etc/gs_ktool_file/secondary_ksf.dat

\! gs_ktool -g && cat $GAUSSHOME/etc/gs_ktool_file/secondary_ksf.dat | grep 'invalid data'

-- 7 primary file : tainted, secondary file tainted | falied
\! echo 'invalid data' > $GAUSSHOME/etc/gs_ktool_file/primary_ksf.dat && cat $GAUSSHOME/etc/gs_ktool_file/primary_ksf.dat
\! echo 'invalid data' > $GAUSSHOME/etc/gs_ktool_file/secondary_ksf.dat && cat $GAUSSHOME/etc/gs_ktool_file/secondary_ksf.dat
\! gs_ktool -g && cat $GAUSSHOME/etc/gs_ktool_file/primary_ksf.dat | grep 'invalid data'
\! gs_ktool -g && cat $GAUSSHOME/etc/gs_ktool_file/secondary_ksf.dat | grep 'invalid data'

-- clear
\! rm -f $GAUSSHOME/etc/gs_ktool_file/*.dat