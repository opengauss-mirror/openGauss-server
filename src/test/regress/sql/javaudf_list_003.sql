DROP SCHEMA bicoredata_003 CASCADE;
CREATE SCHEMA bicoredata_003;
SET CURRENT_SCHEMA = bicoredata_003;

--FUNC_034
CREATE FUNCTION GetPushDeviceId(text)
	RETURNS text
	AS 'com.huawei.udf.GetPushDeviceId.evaluate'
	LANGUAGE java;
SELECT GetPushDeviceId('0021166310211663');

--FUNC_035
CREATE FUNCTION DeviceIdFormat(text)
	RETURNS text
	AS 'com.huawei.udf.DeviceIdFormat.evaluate'
	LANGUAGE java;
SELECT DeviceIdFormat ('0123456789ABcdef');

--FUNC_036
CREATE FUNCTION ImeiTokenCheck(text, text)
	RETURNS bool
	AS 'com.huawei.udf.ImeiTokenCheckUDF.evaluate'
	LANGUAGE java;
SELECT ImeiTokenCheck ('1234567891234567','12345678912345672314sdf165000001');

--!FUNC_037: hdfs:/AppData/CloudPlusProd/CloudPlus/data/DIM/dbank/DIM_KEYWORD_DS
CREATE FUNCTION IsContainsKeyword(text)
	RETURNS text
	AS 'com.huawei.udf.IsContainsKeyword.evaluate'
	LANGUAGE java;
--SELECT IsContainsKeyword ('b50cef4b8c9dfc3688b9531b908c5a56');
--[Expected Result]：
--b50cef4b8c9dfc3688b9531b908c5a56

--FUNC_038
CREATE FUNCTION IsEmpty(varchar)
	RETURNS bool
	AS 'com.huawei.platform.bi.udf.common.IsEmptyUDF.evaluate'
	LANGUAGE java;
SELECT IsEmpty('');
SELECT IsEmpty('null');

--FUNC_039
CREATE FUNCTION NormalMobilePhoneNumber(text)
	RETURNS text
	AS 'com.huawei.udf.NormalMobilePhoneNumber.evaluate'
	LANGUAGE java;
SELECT NormalMobilePhoneNumber ('8613665696273');

--FUNC_040
CREATE FUNCTION IsMessyCode(text)
	RETURNS bool
	AS 'com.huawei.platform.bi.udf.common.IsMessyCodeUDF.evaluate'
	LANGUAGE java;
SELECT IsMessyCode('�й�');

--!FUNC_041: hdfs:/AppData/CloudPlusProd/CloudPlus/data/DIM/cloud/DIM_HW_IMEI_RANGE_DS
CREATE FUNCTION IsHuaweiPhone(text)
	RETURNS text
	AS 'com.huawei.udf.IsHuaweiPhoneUDF.evaluate'
	LANGUAGE java;
--SELECT IsHuaweiPhone('004401720000001');
--[Expected Result]:
--true

--!FUNC_042: hdfs:/hadoop-NJ/data/ODS/maa/ODS_MAA_EXPERIENCE_EMUI_DM/pt_d= 配置路径存放在：maa.config.properties
--!FUNC_043: hdfs:/hadoop-NJ/data/common/ODS_MAA_CONF_APP_INFO_DM/pt_d= 配置路径存放在：maa.config.properties
--!FUNC_044: hdfs:/hadoop-NJ/data/ODS/maa/ODS_MAA_AUTO_REPORT_KEY_DM/pt_d=20161201配置路径存放在：maa.config.properties

--FUNC_045
CREATE FUNCTION ArraySort(text)
	RETURNS text
	AS 'com.huawei.udf.ArraySort.evaluate'
	LANGUAGE java;
SELECT arraysort('a,2,2;b,2,3;c,1,3');

CREATE FUNCTION ArraySort(text, integer)
	RETURNS text
	AS 'com.huawei.udf.ArraySort.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT arraysort('a,2,2;b,2,3;c,1,3',2);

--!FUNC_046: passing parameter type ArrayList<String>
--!FUNC_047: passing parameter type ArrayList<String>

--FUNC_048
CREATE FUNCTION VideoCloudDecrypt(text, text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.videoCloud.VideoCloudDecryptUDF.evaluate'
	LANGUAGE java;
SELECT VideoCloudDecrypt('4c5OSDuE0sMzEs+XghTRv8YQ81J1UhPrmGBZMolgT7c7LUAg6YUjVHBeWC3K+FSiDQ==', 'videocloud1', 'zsj');

--FUNC_049
CREATE FUNCTION VMALLDecryptUtil(text, text, integer)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.vmall.VMALLDecryptUtilUDF.evaluate(java.lang.String, java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT VMALLDecryptUtil('C18ED1F4E614C971613772DDFFF93492','vmall', 2);

--FUNC_050
CREATE FUNCTION aescbcencryptudf(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.AesCBCEncryptUDF.evaluate'
	LANGUAGE java;
---SELECT AesCBCEncryptUDF('123');

--FUNC_051
CREATE FUNCTION OtherSecInfoDecrypt(text)
	RETURNS text
	AS 'com.huawei.udf.OtherSecInfoDecrypt.evaluate'
	LANGUAGE java;
SELECT OtherSecInfoDecrypt('8F94CBC7CB3BC952C781B9A257B9D989735BFEB4938373FD94169F813F365093C2689665ABC2A0EF109E931AEA01A3F4');

CREATE FUNCTION OtherSecInfoDecrypt(text, text)
	RETURNS text
	AS 'com.huawei.udf.OtherSecInfoDecrypt.evaluate'
	LANGUAGE java;
---LACK OF TESTCASE

DROP SCHEMA bicoredata_003 CASCADE;
