DROP SCHEMA bicoredata_005 CASCADE;
CREATE SCHEMA bicoredata_005;
SET CURRENT_SCHEMA = bicoredata_005;

--FUNC_066
CREATE FUNCTION Base64Decode(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.Base64DecodeUDF.evaluate'
	LANGUAGE java;
SELECT Base64Decode('MTIzNDU2');

--FUNC_067
CREATE FUNCTION AesDecryptUDF2(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.openAlliance.AesDecryptUDF2.evaluate'
	LANGUAGE java;
SELECT AesDecryptUDF2('4602DA72EE4A09011E5D5B4FD72827AA');

CREATE FUNCTION AesDecryptUDF2(text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.openAlliance.AesDecryptUDF2.evaluate'
	LANGUAGE java;
SELECT AesDecryptUDF2('85DB238A2DD76F3600D859B49234624F','def');

--FUNC_068
CREATE FUNCTION CryptUtil(text, text, text)
	RETURNS text
	AS 'com.huawei.udf.CryptUtilUDF.evaluate'
	LANGUAGE java;
SELECT CryptUtil('sha256', '123', '');
SELECT CryptUtil('aesDec', 'ECFECFC2361FC782712718D0F46462B4', 'hwuser');

--FUNC_069
CREATE FUNCTION udf_md5(text)
	RETURNS text
	AS 'com.huawei.bi.hive_udf.UdfMd5.evaluate([java.lang.String)'
	LANGUAGE java;
SELECT md5('123');

--FUNC_70
CREATE FUNCTION ToSHA256(text)
	RETURNS text
	AS 'com.huawei.udf.ToSHA256.evaluate'
	LANGUAGE java;
SELECT ToSHA256 ('abc');

--FUNC_071
CREATE FUNCTION AesCBCOpenAlliance(text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.openAlliance.AesCBCOpenAlliance.evaluate'
	LANGUAGE java;
SELECT AesCBCOpenAlliance('/SMpylBB2VaK2iBzCeyv9mKKZFkT671ePjjRgpBglBU=', 'ads');

--FUNC_072: 'enc' result should differ every time
CREATE FUNCTION AESCBC128(text, text,text)
	RETURNS text
	AS 'com.huawei.udf.AESCBC128.evaluate'
	LANGUAGE java;
--SELECT AESCBC128('enc','123','push');
SELECT AESCBC128('dec', str,'push') FROM (
	SELECT AESCBC128('enc','123','push') AS str
);

--FUNC_073
CREATE FUNCTION AesDecrypt(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.AesDecryptUDF.evaluate'
	LANGUAGE java;
SELECT aesDecrypt('79C33E95A5484297E542859CD37AF711');

CREATE FUNCTION AesDecrypt(text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.AesDecryptUDF.evaluate'
	LANGUAGE java;
SELECT aesDecrypt('60DB73AF04E5EAEE23E1F97643F23BBE','hota');

--FUNC_074
CREATE FUNCTION HitopDecryptAES(text)
	RETURNS text
	AS 'com.huawei.udf.HitopDecryptAES.evaluate'
	LANGUAGE java;
SELECT HitopDecryptAES('53CD1163AD1F80DE0196DE5C832A5036');

CREATE FUNCTION HitopDecryptAES(text, text)
	RETURNS text
	AS 'com.huawei.udf.HitopDecryptAES.evaluate'
	LANGUAGE java;
--LACK OF TESTCASE

--FUNC_075
CREATE FUNCTION ShaEncrypt(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.ShaEncryptUDF.evaluate'
	LANGUAGE java;
SELECT ShaEncrypt('123');

DROP SCHEMA javaudf_list_005 CASCADE;
