DROP SCHEMA bicoredata_001 CASCADE;
CREATE SCHEMA bicoredata_001;
SET CURRENT_SCHEMA = bicoredata_001;

--FUNC_001
CREATE FUNCTION RmvDupstrWithSep(text, text, text)
	RETURNS text
	AS 'com.huawei.udf.RmvDupstrWithSep.evaluate'
	LANGUAGE java;
SELECT RmvDupstrWithSep('AS,de,frd,s,de',',','&');

--FUNC_002
CREATE FUNCTION SplitStr(text, text, integer)
	RETURNS text
	AS 'com.huawei.udf.SplitStrUDF.evaluate(java.lang.String, java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT SplitStr('gf-dj|232|sdaASF', '\|',1);

CREATE FUNCTION SplitStr(text, integer, integer)
	RETURNS text
	AS 'com.huawei.udf.SplitStrUDF.evaluate(java.lang.String, java.lang.Integer, java.lang.Integer)'
	LANGUAGE java;
SELECT SplitStr('gf-dj,232,sdaASF',1,1);

--!FUNC_003: passing parameter type Object

--FUNC_004
CREATE FUNCTION getjsonobj(varchar, varchar) RETURNS varchar AS 'com.huawei.platform.bi.udf.common.GetJsonObj.evaluate' LANGUAGE java;
SELECT GetJsonObj ('{"1":["cid","550"]}','$.1[0]');
SELECT GetJsonObj ('{selectedMode:click,scanMode:qr_code}','$.scanMode');

--FUNC_005
CREATE FUNCTION GetJsonObject(text, text)
	RETURNS text
	AS 'com.huawei.udf.GetJsonObject.evaluate'
	LANGUAGE java;
SELECT getJsonObject('{"a":"cid","b":"550"}','a');

--FUNC_006
CREATE FUNCTION GetParam(text, text, text, text, text)
	RETURNS text
	AS 'com.huawei.udf.GetTradeLogParam.evaluate'
	LANGUAGE java;
SELECT getParam('skey1=value1,key2=value2f','key1','query','s','f');
SELECT getParam('{"1":["cid","550"]}','1','json','{','}');

--FUNC_007
CREATE FUNCTION month_sub_udf(text, integer)
	RETURNS text
	AS 'com.huawei.udf.MonthSubUDF.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT month_sub_udf('201612', 1);

--FUNC_008
CREATE FUNCTION month_add_udf(text, integer)
	RETURNS text
	AS 'com.huawei.udf.MonthAddUDF.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT month_add_udf('201612', 1);

--FUNC_009
CREATE FUNCTION GetWeekWithDate(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.GetWeekWithDateUDF.evaluate'
	LANGUAGE java;
SELECT GetWeekWithDate('20161206');

CREATE FUNCTION GetWeekWithDate(text, integer)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.GetWeekWithDateUDF.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT GetWeekWithDate('20161206', 1);

--FUNC_010
CREATE FUNCTION DateFormatSlash(text)
	RETURNS text
	AS 'com.huawei.udf.DateFormatSlashUDF.evaluate'
	LANGUAGE java;
SELECT DateFormatSlash ('2016/12/06');

--FUNC_011
CREATE FUNCTION DateFormat(text)
	RETURNS text
	AS 'com.huawei.udf.DateFormatUDF.evaluate'
	LANGUAGE java;
SELECT DateFormat ('20161206');
SELECT DateFormat ('2016-12-06');
SELECT DateFormat ('20161206010101');

DROP SCHEMA bicoredata_001 CASCADE;

