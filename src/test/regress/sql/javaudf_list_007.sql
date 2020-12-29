DROP SCHEMA bicoredata_007 CASCADE;
CREATE SCHEMA bicoredata_007;
SET CURRENT_SCHEMA = bicoredata_007;

--FUNC_096
CREATE FUNCTION DateUtil(integer)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.DateUtilUDF.evaluate(java.lang.Integer)'
	LANGUAGE java;
SELECT DateUtil (666);

CREATE FUNCTION DateUtil(int8, integer, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.DateUtilUDF.evaluate(java.lang.Long, java.lang.Integer, java.lang.String)'
	LANGUAGE java;
SELECT DateUtil (1111111111111 ,222222222, 'YYYYMM');

CREATE FUNCTION DateUtil(text, text, integer)
	RETURNS int8
	AS 'com.huawei.platform.bi.udf.common.DateUtilUDF.evaluate(java.lang.String, java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT DateUtil ('201712' ,'yyyyMM',2);

CREATE FUNCTION DateUtil(text, text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.DateUtilUDF.evaluate'
	LANGUAGE java;
SELECT DateUtil ('201712' ,'yyyyMM','yyyy-MM');

--FUNC_097
CREATE FUNCTION SHA256(varchar)
	RETURNS varchar
	AS 'com.huawei.udf.SHA256.evaluate'
	LANGUAGE java;
SELECT SHA256 ('aad');

--FUNC_099
CREATE FUNCTION UpDecryption(text)
	RETURNS text
	AS 'com.huawei.udf.UpDecryptionUDF.evaluate'
	LANGUAGE java;
SELECT UpDecryption('CC8185CABC7982A15270677A83758AB6');

CREATE FUNCTION UpDecryption(text, text)
	RETURNS text
	AS 'com.huawei.udf.UpDecryptionUDF.evaluate'
	LANGUAGE java;
SELECT UpDecryption('CC8185CABC7982A15270677A83758AB6','up');

--FUNC_100
CREATE FUNCTION RepeatGroupCid(int4, text)
	RETURNS text
	AS'com.huawei.platform.bi.udf.service.vmall.RepeatGroupCidUDF.evaluate(java.lang.Integer, java.lang.String)'
	LANGUAGE java;
SELECT RepeatGroupCid (1,'abc');

--!FUNC_101.	DbankNpDownload returns Map

--!FUNC_102.	groupByLogin input Object, can not change to static 

--!FUNC_103.	dbankDnUpload  returns map

--FUNC_104
CREATE FUNCTION SlideAndScreen(text, text)
	RETURNS text
	AS'com.huawei.platform.bi.udf.service.hota.SlideAndScreenUDF.evaluate'
	LANGUAGE java;
SELECT SlideAndScreen('000000000000000000000000000000000000000000000000000000000000000000000000000101010100000000000000000000000000000000020300010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000020201000000000000000000000000000000000202000200020000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000', '2016-11-24 08:12:54');

--!FUNC_105.	UUID   DeferredObject
--!FUNC_106.	UrlDecoder  can not change to static
--!FUNC_107.	IsCustomCommit  dependent file

--FUNC_108
CREATE FUNCTION GetAdPushReportMsg(text, int4)
	RETURNS text
	AS'com.huawei.udf.GetAdPushReportMsg.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT GetAdPushReportMsg ('timeStamp|PS|deviceid|msgId|eventid|agent version|msg cmd',1);

--FUNC_109
CREATE FUNCTION GetFileId(text)
	RETURNS text
	AS'com.huawei.udf.DbankGetFileId.evaluate'
	LANGUAGE java;
SELECT getFileId('/file/x/xxx.sql');

--FUNC_110
CREATE FUNCTION GetFileName(text)
	RETURNS text
	AS'com.huawei.udf.DbankGetFileName.evaluate'
	LANGUAGE java;
SELECT getFileName('/file/x/y/xxx.sql');

--FUNC_111
CREATE FUNCTION GetHonorVersion(text)
	RETURNS text
	AS'com.huawei.udf.GetHonorVersion.evaluate'
	LANGUAGE java;
SELECT GetHonorVersion('EMUI2.0_G750-T00_4.4.16') ;

DROP SCHEMA bicoredata_007 CASCADE;
