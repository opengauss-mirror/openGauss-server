DROP SCHEMA bicoredata_002 CASCADE;
CREATE SCHEMA bicoredata_002;
SET CURRENT_SCHEMA = bicoredata_002;

--FUNC_012
CREATE FUNCTION GetLastMonth(text)
	RETURNS text
	AS 'com.huawei.udf.GetLastMonth.evaluate'
	LANGUAGE java;
SELECT GetLastMonth('20160102');

--FUNC_013
CREATE FUNCTION dateudf(text)
	RETURNS text
	AS 'com.huawei.udf.DateUDF.evaluate'
	LANGUAGE java;
SELECT dateudf('2016-10-16 10:11:12');

CREATE FUNCTION dateudf(text, integer)
	RETURNS text
	AS 'com.huawei.udf.DateUDF.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT dateudf('2016-6-16 10:11:12',0);
SELECT dateudf('2016-6-16 10:11:12',1);

CREATE FUNCTION dateudf(text, text, integer)
	RETURNS bool
	AS 'com.huawei.udf.DateUDF.evaluate(java.lang.String, java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT dateudf('20160616 10', '20160520',1);

CREATE FUNCTION dateudf(text, text, text, text)
	RETURNS bool
	AS 'com.huawei.udf.DateUDF.evaluate'
	LANGUAGE java;
SELECT dateudf('20160616-12','20160616','20','20160616-24');

--FUNC_014
CREATE FUNCTION TimeAdd8h(text, text)
	RETURNS text
	AS 'com.huawei.udf.TimeAdd8h.evaluate'
	LANGUAGE java;
SELECT TimeAdd8h('2016-02-02 10:12:23:002','yyyy-MM-dd HH:mm:ss:SSS');

--!FUNC_15: hdfs:/hadoop-NJ/data/DW/hispace/t_dw_hispace_app_snap_dm2
--!FUNC_16: hdfs:/hadoop-NJ/data/DW/hispace/t_dw_hispace_app_snap_mm2
--!FUNC_17: hdfs:/hadoop-NJ/data/DW/hispace/t_dw_hispace_app_snap_wm2
--!FUNC_18: hdfs:/hadoop-NJ/data/DW/hispace/t_dw_hispace_dev_app_snap_dm
--!FUNC_19: hdfs:/hadoop-NJ/data/DW/hispace/t_dw_hispace_dev_app_snap_mm
--!FUNC_20: hdfs:/hadoop-NJ/data/DW/hispace/t_dw_hispace_dev_app_snap_wm
--!FUNC_21: hdfs:/user/hive-NJ/warehouse/dw_maa_experience_user_chk_key_dm/pt_d=，在maa.config.properties中获取

--FUNC_022
CREATE FUNCTION RepeatDirectGroupCid(integer, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.RepeatDirectGroupCid.evaluate(java.lang.Integer, java.lang.String)'
	LANGUAGE java;
SELECT i,str, RepeatDirectGroupCid (i,str)
FROM
(
SELECT 1 AS i,null AS str
UNION ALL
SELECT 1 AS i,'a' AS str
UNION ALL
SELECT 2 AS i,'b' AS str
UNION ALL
SELECT 3 AS i,null AS str
)t;

--!FUNC_023: return org.apache.hadoop.io.LongWritable
--!FUNC_024: passing parameter type obj...

--FUNC_025
CREATE FUNCTION Rank(text)
	RETURNS int4
	AS 'com.huawei.udf.Rank.evaluate'
	LANGUAGE java;
SELECT str, Rank (str) FROM
(
SELECT 'b' AS str
UNION ALL
SELECT 'a' AS str
UNION ALL
SELECT 'a' AS str
)t;

CREATE FUNCTION Rank(text, text)
	RETURNS int4
	AS 'com.huawei.udf.Rank.evaluate'
	LANGUAGE java;
SELECT str1,str2, rank (str1,str2) FROM
(
SELECT '1' AS str1,'a' AS str2
UNION ALL
SELECT '1' AS str1,'a' AS str2
UNION ALL
SELECT '2' AS str1,'b' AS str2
UNION ALL
SELECT '2' AS str1,'c' AS str2
)t;

--!FUNC_26: hdfs:/hadoop-NJ/data/DW/hispace/t_dw_hispace_search_fact_dm
--!FUNC_27: hdfs:/hadoop-NJ/data/DW/hispace/t_dw_hispace_search_fact_wm
--!FUNC_28: hdfs:/hadoop-NJ/data/DW/hispace_portal/tmp_dw_hispace_search_word_dm
--!FUNC_29: hdfs:/hadoop-NJ/data/DW/hispace_portal/tmp_dw_hispace_search_word_wm
--!FUNC_30: hdfs:/hadoop-NJ/data/DW/hispace_portal/tmp_dw_hispace_search_word_mm

--FUNC_031
CREATE FUNCTION RomVersionCheck(text)
	RETURNS bool
	AS 'com.huawei.udf.RomVersionCheckUDF.evaluate'
	LANGUAGE java;
SELECT RomVersionCheck ('erw434_sa');

--FUNC_032
CREATE FUNCTION DeviceId(text)
	RETURNS text
	AS 'com.huawei.udf.DeviceIDUDF.evaluate'
	LANGUAGE java;
SELECT DeviceId ('123@abc');

CREATE FUNCTION DeviceId(text, integer)
	RETURNS text
	AS 'com.huawei.udf.DeviceIDUDF.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT DeviceId ('1234567891234567',1);

CREATE FUNCTION DeviceId(text, text, integer)
	RETURNS text
	AS 'com.huawei.udf.DeviceIDUDF.evaluate(java.lang.String, java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT DeviceId ('1234567891@234567', '@',1);

CREATE FUNCTION DeviceId(text, text, integer, integer)
	RETURNS text
	AS 'com.huawei.udf.DeviceIDUDF.evaluate(java.lang.String, java.lang.String, java.lang.Integer, java.lang.Integer)'
	LANGUAGE java;
SELECT DeviceId ('123456789@1234567', '@',-1,3);

CREATE FUNCTION DeviceId(text, text, text)
	RETURNS text
	AS 'com.huawei.udf.DeviceIDUDF.evaluate'
	LANGUAGE java;
SELECT DeviceId ('57521021u11110000000@861519010958541', '@','sim_mobile_oper');

--FUNC_033
CREATE FUNCTION TerminalFormate(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.TerminalFormateUDF.evaluate'
	LANGUAGE java;
SELECT TerminalFormate (' Huawei c8815');

DROP SCHEMA bicoredata_002;
