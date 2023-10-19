DROP SCHEMA bicoredata_008 CASCADE;
CREATE SCHEMA bicoredata_008;
SET CURRENT_SCHEMA = bicoredata_008;

--FUNC_112.Result differs from CloudBU because the order outcome is different from HIVE 
CREATE FUNCTION GetLastValue(text, text)
	RETURNS text
	AS'com.huawei.udf.GetLastValue.evaluate'
	LANGUAGE java;
SELECT GetLastValue('', 'testvalue');
SELECT GetLastValue('', '');
SELECT GetLastValue('testuser', 'testvalue');
SELECT GetLastValue(user1,value) FROM 
(
	SELECT user1,value FROM
	(
	SELECT 'testuser' AS user1,'testvalue' AS value
	UNION ALL
	SELECT 'testuser' AS user1,'' AS value
	)t
	GROUP BY t.value,t.user1
	ORDER BY t.value DESC
)t2;
SELECT GetLastValue(user,value) FROM
(
	SELECT user,value FROM 
	(
	SELECT 'testuser' AS user,'testvalue' AS value
	UNION ALL
	SELECT 'testuser2' AS user,'' AS value
	)t 
	GROUP BY t.value,t.user
	ORDER BY t.value DESC
)t2;

--!FUNC_113.	GetPushPre input ArrayList
--!FUNC_114.	OsVauleSort dependent on file
--!FUNC_115.	PushMsgProcess input ArrayList<String>

--FUNC_116
CREATE FUNCTION ReplaceTeminal(text)
	RETURNS text
	AS'com.huawei.udf.ReplaceTeminal.evaluate'
	LANGUAGE java;
SELECT replaceteminal('2.2.1') ;

--FUNC_117
CREATE FUNCTION replaceTerminalInfo(text)
	RETURNS text
	AS'com.huawei.udf.ReplaceTerminalInfoUDF.evaluate'
	LANGUAGE java;
SELECT replaceTerminalInfo('coolpad5860');

--FUNC_118
CREATE FUNCTION replaceTerminalOs(text)
	RETURNS text
	AS'com.huawei.udf.ReplaceTerminalOsUDF.evaluate'
	LANGUAGE java;
SELECT replaceTerminalOs('4.2');

--!FUNC_119.	versionUpgrade input ArrayList

--FUNC_120
CREATE FUNCTION GetAdPushMsg(text, int4)
	RETURNS text
	AS'com.huawei.udf.GetAdPushMsg.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
--LACK OF TESTCASE

--!FUNC_121 GetHonorTimeAndVersions returns map<String,String>
--CREATE FUNCTION GetHonorTimeAndVersions RETURNS text AS'com.huawei.udf.GetHonorTimeAndVersions.evaluate' LANGUAGE java;
--SELECT GetHonorTimeAndVersions('20151001');

--FUNC_122 GetMinSeconds returns Long
CREATE FUNCTION GetMinSeconds(text, text)
	RETURNS int8
	AS'com.huawei.udf.GetMinSecondsUDF.evaluate'
	LANGUAGE java;
SELECT GetMinSeconds('20161010','yyyyMMdd');

--!FUNC_123  has dependent file

--FUNC_124
CREATE FUNCTION GetOperatorFromNumber(text)
	RETURNS text
	AS 'com.huawei.udf.GetOperatorFromNumber.evaluate'
	LANGUAGE java;
SELECT getOperatorFromNumber('13665696273');

--FUNC_125
CREATE FUNCTION IsDeviceIdLegal(varchar)
	RETURNS bool
	AS 'com.huawei.udf.IsDeviceIdLegal.evaluate'
	LANGUAGE java;
SELECT isdeviceidlegal('0123456789abcde');

--FUNC_126
CREATE FUNCTION LongToIP(int8)
	RETURNS text
	AS 'com.huawei.udf.LongToIPUDF.evaluate(java.lang.Long)'
	LANGUAGE java;
SELECT LongToIP(19216811);

--FUNC_127
CREATE FUNCTION LinkRelativeRatio(float8, float8, int4)
	RETURNS float8
	AS 'com.huawei.udf.LinkRelativeRatioUDF.evaluate(java.lang.Double, java.lang.Double, java.lang.Integer)'
	LANGUAGE java;
SELECT LinkRelativeRatio(1.1,2.3,5);

CREATE FUNCTION LinkRelativeRatio(float8,float8)
	RETURNS float8
	AS 'com.huawei.udf.LinkRelativeRatioUDF.evaluate(java.lang.Double, java.lang.Double)'
	LANGUAGE java;
SELECT LinkRelativeRatio(1.1,2.3);

--FUNC_128
CREATE FUNCTION RevertDeviceId(varchar)
	RETURNS varchar
	AS 'com.huawei.udf.RevertDeviceId.evaluate'
	LANGUAGE java;
SELECT revertDeviceId('asd1234123456789');

DROP SCHEMA bicoredata_008 CASCADE;
