DROP SCHEMA bicoredata_006 CASCADE;
CREATE SCHEMA bicoredata_006;
SET CURRENT_SCHEMA = bicoredata_006;

--FUNC_076: Result should differ every time.
CREATE FUNCTION CommonAesCBCEncrypt(text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.CommonAesCBCEncryptUDF.evaluate'
	LANGUAGE java;
--SELECT CommonAesCBCEncrypt('862268039012356','game');

--FUNC_077
CREATE FUNCTION CommonAesCBCDecrypt(text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.CommonAesCBCDecryptUDF.evaluate'
	LANGUAGE java;
SELECT CommonAesCBCDecrypt('ZmQzZjNlOGI4N2M0OTJlMLx7hs/wIlRBi7uUA1s1Sh0=','game');
SELECT CommonAesCBCDecrypt('7bD0ccb9X2JrCGWo1pOSysUXyrfCi35IxqAKRbycKFc=','hwvideo');

--FUNC_078
CREATE FUNCTION AesCBCUpDecry(text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.up.AesCBCUpDecry.evaluate'
	LANGUAGE java;
SELECT AesCBCUpDecry('0FE34B0926754D082736BC788199D0AE:9345F35F4424877FA325693874ACD88B','game');

--FUNC_079
CREATE FUNCTION DecryptAES(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.wlan.DecryptAES.evaluate'
	LANGUAGE java;
SELECT DecryptAES('1f4d3b181819d475de19b821aa3d5b9ad65ff3a930cc5421d4bcba1cea0b420a');

--FUNC_080
CREATE FUNCTION TradeDecrypt(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.trade.Decrypt.evaluate'
	LANGUAGE java;
SELECT TradeDecrypt('1-AES-03f5e53aaa2d98b3113872bf40567885debcee9f64a044622abda9de36890f29');

--FUNC_081
CREATE FUNCTION HealthGPSDecrypt(text)
	RETURNS text
	AS 'com.huawei.udf.HealthGPSDecryptUDF.evaluate'
	LANGUAGE java;
SELECT HealthGPSDecrypt('1cro7uN373zw56cv4BoE9uzmyXSB3ZZx9ogs/UXgl/rCnGFfNcaBvz+MnGYVS7IshJwNLK1RzKl1M5euBCSQH7wCy+PTFFXHmXlFdwBS3MEF11hDhkWJcLnXOJV3ZKTWKScMw1xAOjeT7fgTvKRWRnUemfBGGQuil6p8ylRMvVf6TejogSm6M5EF+oNGZXh3JTrQAtZ3zm27onOhKswCQEIdXqyGCcBQmb6IiKe13hl7kHGOubO/iTk8aYVmk4a/TdY+EK0vYip/IItn3BOiVlpSjo2AnwTMryWL7UbdKZBCPvtZQBP5zSD1WzHpv1GC11kGYryxCNaO0BZwa8yga4PXIIZ0HQkmdJgV+O8+YCLBYrzdPsQVESw8d+aH2FaF');

--FUNC_082
CREATE FUNCTION AesHiadDecrypt(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.hispace.AesHiadDecryptUDF.evaluate'
	LANGUAGE java;
SELECT AesHiadDecrypt('7A8F91E05019225902356B67AB78FB2E');

--FUNC_083: needs bcprov-ext-jdk15on-153.jar in $JAVA_HOME/jre/lib/ext
CREATE FUNCTION HmacSha256Encrypt(text)
	RETURNS text
	AS 'com.huawei.udf.HmacSha256EncryptUDF.evaluate'
	LANGUAGE java;
--SELECT HmacSha256Encrypt ('ss');

--FUNC_083
CREATE FUNCTION IP2AreaInfo(text)
	RETURNS text
	AS 'com.huawei.udf.IP2AreaInfoUDF.evaluate'
	LANGUAGE java;
SELECT IP2AreaInfo('221.226.2.254');

--FUNC_084
CREATE FUNCTION IP2AreaInfo(text, integer)
	RETURNS text
	AS 'com.huawei.udf.IP2AreaInfoUDF.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT IP2AreaInfo('221.226.2.254',2);

--FUNC_085
CREATE FUNCTION GeoIpCountryApiUDF(text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.GeoIpCountryApiUDF.evaluate'
	LANGUAGE java;
SELECT GeoIpCountryApiUDF('125.107.106.170','code');
SELECT GeoIpCountryApiUDF('125.107.106.170','chineseName');

--!FUNC_086:hdfs：/AppData/BIProd/datamining/DS/LOCATION/dim_area_global_ip2area_v131001_ds/
--!FUNC_087:hdfs:/hadoop-NJ/data/DW/Recommend/t_dw_rcm_ds
--!FUNC_088:hdfs:/hadoop-NJ/data/DIM/SYS/dim_mobile_province_ds
--!FUNC_089:hdfs:/hadoop-NJ/data/DIM/dim_hota_unknown_ip2loc_ds

--!FUNC_090: result differs from CloudBU but agrees with Eclipse
CREATE FUNCTION GetAreaInfo(text, integer)
	RETURNS int8
	AS 'com.huawei.platform.bi.udf.common.GetAreaInfoUDF.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT GetAreaInfo ('113.200.205.45', 1);
--[Results Expected]:
--520250170000000
DROP FUNCTION GetAreaInfo;

--FUNC_091: needs bcprov-ext-jdk15on-153.jar in $JAVA_HOME/jre/lib/ext
CREATE FUNCTION GeoIpCountry(text,text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.GeoIpCountryUDF.evaluate'
	LANGUAGE java;
--SELECT  GeoIpCountry('203.81.93.101','code');
--SELECT  GeoIpCountry('203.81.93.105','englishName');

--!FUNC_092.	GeoIpCountryApi dependent file
--!FUNC_093.	GetChinaProvinceName dependent file
--CREATE FUNCTION GetChinaProvinceName(text) RETURNS text AS 'com.huawei.platform.bi.udf.common.GetChinaProvinceNameUDF.evaluate' LANGUAGE java;
--SELECT GetChinaProvinceName('1.0.1.0') ;
--DROP FUNCTION GetChinaProvinceName(text);

--!FUNC_094.	GeoIpCityApi returns List<String>

--FUNC_095
CREATE FUNCTION MD5UDF(varchar)
	RETURNS varchar
	AS 'com.huawei.platform.bi.udf.common.MD5UDF.evaluate'
	LANGUAGE java;
SELECT MD5UDF ('aa');

DROP SCHEMA bicoredata_006 CASCADE;
