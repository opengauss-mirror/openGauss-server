DROP SCHEMA bicoredata CASCADE;
CREATE SCHEMA bicoredata;
SET CURRENT_SCHEMA = bicoredata;

--FUNC_001
CREATE FUNCTION RmvDupstrWithSep(text, text, text)
	RETURNS text
	AS 'com.huawei.udf.RmvDupstrWithSep.evaluate'
	LANGUAGE java;
SELECT RmvDupstrWithSep('AS,de,frd,s,de',',','&');
DROP FUNCTION RmvDupstrWithSep;

--FUNC_002
CREATE FUNCTION SplitStr(text, text, integer)
	RETURNS text
	AS 'com.huawei.udf.SplitStrUDF.evaluate(java.lang.String, java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT SplitStr('gf-dj|232|sdaASF', '\|',1);
DROP FUNCTION SplitStr(text, text, integer);

CREATE FUNCTION SplitStr(text, integer, integer)
	RETURNS text
	AS 'com.huawei.udf.SplitStrUDF.evaluate(java.lang.String, java.lang.Integer, java.lang.Integer)'
	LANGUAGE java;
SELECT SplitStr('gf-dj,232,sdaASF',1,1);
DROP FUNCTION SplitStr(text, integer, integer);

--!FUNC_003: passing parameter type Object

--FUNC_004
CREATE FUNCTION getjsonobj(varchar, varchar) RETURNS varchar AS 'com.huawei.platform.bi.udf.common.GetJsonObj.evaluate' LANGUAGE java;
SELECT GetJsonObj ('{"1":["cid","550"]}','$.1[0]');
SELECT GetJsonObj ('{selectedMode:click,scanMode:qr_code}','$.scanMode');
DROP FUNCTION getjsonobj;

--FUNC_005
CREATE FUNCTION GetJsonObject(text, text)
	RETURNS text
	AS 'com.huawei.udf.GetJsonObject.evaluate'
	LANGUAGE java;
SELECT getJsonObject('{"a":"cid","b":"550"}','a');
DROP FUNCTION GetJsonObject;

--FUNC_006
CREATE FUNCTION GetParam(text, text, text, text, text)
	RETURNS text
	AS 'com.huawei.udf.GetTradeLogParam.evaluate'
	LANGUAGE java;
SELECT getParam('skey1=value1,key2=value2f','key1','query','s','f');
SELECT getParam('{"1":["cid","550"]}','1','json','{','}');
DROP FUNCTION GetParam;

--FUNC_007
CREATE FUNCTION month_sub_udf(text, integer)
	RETURNS text
	AS 'com.huawei.udf.MonthSubUDF.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT month_sub_udf('201612', 1);
DROP FUNCTION month_sub_udf(text, integer);

--FUNC_008
CREATE FUNCTION month_add_udf(text, integer)
	RETURNS text
	AS 'com.huawei.udf.MonthAddUDF.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT month_add_udf('201612', 1);
DROP FUNCTION month_add_udf(text, integer);

--FUNC_009
CREATE FUNCTION GetWeekWithDate(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.GetWeekWithDateUDF.evaluate'
	LANGUAGE java;
SELECT GetWeekWithDate('20161206');
DROP FUNCTION GetWeekWithDate(text);

CREATE FUNCTION GetWeekWithDate(text, integer)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.GetWeekWithDateUDF.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT GetWeekWithDate('20161206', 1);
DROP FUNCTION GetWeekWithDate(text, integer);

--FUNC_010
CREATE FUNCTION DateFormatSlash(text)
	RETURNS text
	AS 'com.huawei.udf.DateFormatSlashUDF.evaluate'
	LANGUAGE java;
SELECT DateFormatSlash ('2016/12/06');
DROP FUNCTION DateFormatSlash;

--FUNC_011
CREATE FUNCTION DateFormat(text)
	RETURNS text
	AS 'com.huawei.udf.DateFormatUDF.evaluate'
	LANGUAGE java;
SELECT DateFormat ('20161206');
SELECT DateFormat ('2016-12-06');
SELECT DateFormat ('20161206010101');
DROP FUNCTION DateFormat(text);

--FUNC_012
CREATE FUNCTION GetLastMonth(text)
	RETURNS text
	AS 'com.huawei.udf.GetLastMonth.evaluate'
	LANGUAGE java;
SELECT GetLastMonth('20160102');
DROP FUNCTION GetLastMonth;

--FUNC_013
CREATE FUNCTION dateudf(text)
	RETURNS text
	AS 'com.huawei.udf.DateUDF.evaluate'
	LANGUAGE java;
SELECT dateudf('2016-10-16 10:11:12');
DROP FUNCTION dateudf(text);

CREATE FUNCTION dateudf(text, integer)
	RETURNS text
	AS 'com.huawei.udf.DateUDF.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT dateudf('2016-6-16 10:11:12',0);
SELECT dateudf('2016-6-16 10:11:12',1);
DROP FUNCTION dateudf(text, integer);

CREATE FUNCTION dateudf(text, text, integer)
	RETURNS bool
	AS 'com.huawei.udf.DateUDF.evaluate(java.lang.String, java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT dateudf('20160616 10', '20160520',1);
DROP FUNCTION dateudf(text, text, integer);

CREATE FUNCTION dateudf(text, text, text, text)
	RETURNS bool
	AS 'com.huawei.udf.DateUDF.evaluate'
	LANGUAGE java;
SELECT dateudf('20160616-12','20160616','20','20160616-24');
DROP FUNCTION dateudf(text, text, text, text);

--FUNC_014
CREATE FUNCTION TimeAdd8h(text, text)
	RETURNS text
	AS 'com.huawei.udf.TimeAdd8h.evaluate'
	LANGUAGE java;
SELECT TimeAdd8h('2016-02-02 10:12:23:002','yyyy-MM-dd HH:mm:ss:SSS');
DROP FUNCTION TimeAdd8h;

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
DROP FUNCTION RepeatDirectGroupCid;

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
DROP FUNCTION Rank(text);

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
DROP FUNCTION Rank(text, text);

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
DROP FUNCTION RomVersionCheck();

--FUNC_032
CREATE FUNCTION DeviceId(text)
	RETURNS text
	AS 'com.huawei.udf.DeviceIDUDF.evaluate'
	LANGUAGE java;
SELECT DeviceId ('123@abc');
DROP FUNCTION DeviceId(text);

CREATE FUNCTION DeviceId(text, integer)
	RETURNS text
	AS 'com.huawei.udf.DeviceIDUDF.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT DeviceId ('1234567891234567',1);
DROP FUNCTION DeviceId(text, integer);

CREATE FUNCTION DeviceId(text, text, integer)
	RETURNS text
	AS 'com.huawei.udf.DeviceIDUDF.evaluate(java.lang.String, java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT DeviceId ('1234567891@234567', '@',1);
DROP FUNCTION DeviceId(text, text, integer);

CREATE FUNCTION DeviceId(text, text, integer, integer)
	RETURNS text
	AS 'com.huawei.udf.DeviceIDUDF.evaluate(java.lang.String, java.lang.String, java.lang.Integer, java.lang.Integer)'
	LANGUAGE java;
SELECT DeviceId ('123456789@1234567', '@',-1,3);
DROP FUNCTION DeviceId(text, text, integer, integer);

CREATE FUNCTION DeviceId(text, text, text)
	RETURNS text
	AS 'com.huawei.udf.DeviceIDUDF.evaluate'
	LANGUAGE java;
SELECT DeviceId ('57521021u11110000000@861519010958541', '@','sim_mobile_oper');
DROP FUNCTION DeviceId(text, text, text);

--FUNC_033
CREATE FUNCTION TerminalFormate(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.TerminalFormateUDF.evaluate'
	LANGUAGE java;
SELECT TerminalFormate (' Huawei c8815');
DROP FUNCTION TerminalFormate;

--FUNC_034
CREATE FUNCTION GetPushDeviceId(text)
	RETURNS text
	AS 'com.huawei.udf.GetPushDeviceId.evaluate'
	LANGUAGE java;
SELECT GetPushDeviceId('0021166310211663');
DROP FUNCTION GetPushDeviceId;

--FUNC_035
CREATE FUNCTION DeviceIdFormat(text)
	RETURNS text
	AS 'com.huawei.udf.DeviceIdFormat.evaluate'
	LANGUAGE java;
SELECT DeviceIdFormat ('0123456789ABcdef');
DROP FUNCTION DeviceIdFormat;

--FUNC_036
CREATE FUNCTION ImeiTokenCheck(text, text)
	RETURNS bool
	AS 'com.huawei.udf.ImeiTokenCheckUDF.evaluate'
	LANGUAGE java;
SELECT ImeiTokenCheck ('1234567891234567','12345678912345672314sdf165000001');
DROP FUNCTION ImeiTokenCheck;

--!FUNC_037: hdfs:/AppData/CloudPlusProd/CloudPlus/data/DIM/dbank/DIM_KEYWORD_DS
CREATE FUNCTION IsContainsKeyword(text)
	RETURNS text
	AS 'com.huawei.udf.IsContainsKeyword.evaluate'
	LANGUAGE java;
--SELECT IsContainsKeyword ('b50cef4b8c9dfc3688b9531b908c5a56');
--[Expected Result]：
--b50cef4b8c9dfc3688b9531b908c5a56
DROP FUNCTION IsContainsKeyword;

--FUNC_038
CREATE FUNCTION IsEmpty(varchar)
	RETURNS bool
	AS 'com.huawei.platform.bi.udf.common.IsEmptyUDF.evaluate'
	LANGUAGE java;
SELECT IsEmpty('');
SELECT IsEmpty('null');
DROP FUNCTION IsEmpty(varchar);

--FUNC_039
CREATE FUNCTION NormalMobilePhoneNumber(text)
	RETURNS text
	AS 'com.huawei.udf.NormalMobilePhoneNumber.evaluate'
	LANGUAGE java;
SELECT NormalMobilePhoneNumber ('8613665696273');
DROP FUNCTION NormalMobilePhoneNumber;

--FUNC_040
CREATE FUNCTION IsMessyCode(text)
	RETURNS bool
	AS 'com.huawei.platform.bi.udf.common.IsMessyCodeUDF.evaluate'
	LANGUAGE java;
SELECT IsMessyCode('�й�');
DROP FUNCTION IsMessyCode;

--!FUNC_041: hdfs:/AppData/CloudPlusProd/CloudPlus/data/DIM/cloud/DIM_HW_IMEI_RANGE_DS
CREATE FUNCTION IsHuaweiPhone(text)
	RETURNS text
	AS 'com.huawei.udf.IsHuaweiPhoneUDF.evaluate'
	LANGUAGE java;
--SELECT IsHuaweiPhone('004401720000001');
--[Expected Result]:
--true
DROP FUNCTION IsHuaweiPhone;

--!FUNC_042: hdfs:/hadoop-NJ/data/ODS/maa/ODS_MAA_EXPERIENCE_EMUI_DM/pt_d= 配置路径存放在：maa.config.properties
--!FUNC_043: hdfs:/hadoop-NJ/data/common/ODS_MAA_CONF_APP_INFO_DM/pt_d= 配置路径存放在：maa.config.properties
--!FUNC_044: hdfs:/hadoop-NJ/data/ODS/maa/ODS_MAA_AUTO_REPORT_KEY_DM/pt_d=20161201配置路径存放在：maa.config.properties

--FUNC_045
CREATE FUNCTION ArraySort(text)
	RETURNS text
	AS 'com.huawei.udf.ArraySort.evaluate'
	LANGUAGE java;
SELECT arraysort('a,2,2;b,2,3;c,1,3');
DROP FUNCTION ArraySort(text);

CREATE FUNCTION ArraySort(text, integer)
	RETURNS text
	AS 'com.huawei.udf.ArraySort.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT arraysort('a,2,2;b,2,3;c,1,3',2);
DROP FUNCTION ArraySort(text, integer);

--!FUNC_046: passing parameter type ArrayList<String>
--!FUNC_047: passing parameter type ArrayList<String>

--FUNC_048
CREATE FUNCTION VideoCloudDecrypt(text, text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.videoCloud.VideoCloudDecryptUDF.evaluate'
	LANGUAGE java;
SELECT VideoCloudDecrypt('4c5OSDuE0sMzEs+XghTRv8YQ81J1UhPrmGBZMolgT7c7LUAg6YUjVHBeWC3K+FSiDQ==', 'videocloud1', 'zsj');
DROP FUNCTION VideoCloudDecrypt;

--FUNC_049
CREATE FUNCTION VMALLDecryptUtil(text, text, integer)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.vmall.VMALLDecryptUtilUDF.evaluate(java.lang.String, java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT VMALLDecryptUtil('C18ED1F4E614C971613772DDFFF93492','vmall', 2);
DROP FUNCTION VMALLDecryptUtil(text, text, integer);

--FUNC_050
CREATE FUNCTION aescbcencryptudf(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.AesCBCEncryptUDF.evaluate'
	LANGUAGE java;
---SELECT AesCBCEncryptUDF('123');
DROP FUNCTION aescbcencryptudf;

--FUNC_051
CREATE FUNCTION OtherSecInfoDecrypt(text)
	RETURNS text
	AS 'com.huawei.udf.OtherSecInfoDecrypt.evaluate'
	LANGUAGE java;
SELECT OtherSecInfoDecrypt('8F94CBC7CB3BC952C781B9A257B9D989735BFEB4938373FD94169F813F365093C2689665ABC2A0EF109E931AEA01A3F4');
DROP FUNCTION OtherSecInfoDecrypt(text);

CREATE FUNCTION OtherSecInfoDecrypt(text, text)
	RETURNS text
	AS 'com.huawei.udf.OtherSecInfoDecrypt.evaluate'
	LANGUAGE java;
---LACK OF TESTCASE
DROP FUNCTION OtherSecInfoDecrypt(text, text);

--FUNC_052
CREATE FUNCTION VideoCloudAESDecrypt(text, text)
	RETURNS text
	AS 'com.huawei.udf.VideoCloudAESDecryptUDF.evaluate'
	LANGUAGE java;
SELECT VideoCloudAESDecrypt('+u2nOW8zwKNa2Puz/4pl+8gWh7XHiwGErYRCK+/LDihvKRGKgXK/sYskEdaLvQHgAQ==','videocloudaes');
DROP FUNCTION VideoCloudAESDecrypt;

--FUNC_053
CREATE FUNCTION AdDecryptNew(text)
	RETURNS text
	AS 'com.huawei.udf.AdDecryptNew.evaluate'
	LANGUAGE java;
SELECT AdDecryptNew('74037f9013222a1d20f2a22881426ceeff46e8d5971002a53be28962d9ec9a4f');
DROP FUNCTION AdDecryptNew;

--FUNC_054: needs bcprov-ext-jdk15on-153.jar in $JAVA_HOME/jre/lib/ext
CREATE FUNCTION MovieVMOSDecrypt(text, text, text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.movie.MovieVMOSDecryptUDF.evaluate'
	LANGUAGE java;
--SELECT MovieVMOSDecrypt('381b566d-b9e6-8121-d211-942d1a1c2e94','BTV-W09','Android 6.0','CBC_FD374FF928DE468F9886C0F1C65D411A7E4338FCFBB62D0DA66A5DF9A790A30CDBEE233267B9CD2A80F6A6DC87C0E5DB');
--SELECT MovieVMOSDecrypt('9c9c7372-a06c-2ca2-cff2-1a781e1237c3', 'JDN-AL00', 'Android 6.0.1', 'CBC_58A328A662A8667D767F1D2732E7595CFE7C5D9620B8CFEA99FB4567228DA42D');
DROP FUNCTION MovieVMOSDecrypt;

--FUNC_055: result should differ every time
CREATE FUNCTION AESEncrypt4AD(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.hnas.AESEncrypt4ADUDF.evaluate'
	LANGUAGE java;
--SELECT AESEncrypt4AD('123');
DROP FUNCTION AESEncrypt4AD(text);

CREATE FUNCTION AESEncrypt4AD(text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.hnas.AESEncrypt4ADUDF.evaluate'
	LANGUAGE java;
---LACK OF TESTCASE
DROP FUNCTION AESEncrypt4AD(text, text);

--FUNC_056
CREATE FUNCTION AESDecrypt4AD(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.hnas.AESDecrypt4ADUDF.evaluate'
	LANGUAGE java;
SELECT aesdecrypt4ad('uquVZDIEoHF1ZHzi+mTgAl07tTvQUa02ZHSpmCJOxpE=');
DROP FUNCTION AESDecrypt4AD(text);

CREATE FUNCTION AESDecrypt4AD(text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.hnAS.AESDecrypt4ADUDF.evaluate'
	LANGUAGE java;
---LACK OF TESTCASE
DROP FUNCTION AESDecrypt4AD(text, text);

--FUNC_057
CREATE FUNCTION HuaweiPayDecrypt(text)
	RETURNS text
	AS 'com.huawei.udf.HuaweiPayDecryptUDF.evaluate'
	LANGUAGE java;
SELECT HuaweiPayDecrypt('encrypt1-AES-979c03f8e480fced44b2ab428e988ba6:e82b79815370075f9c3339633c186442');
DROP FUNCTION HuaweiPayDecrypt;

--FUNC_58: needs bcprov-ext-jdk15on-153.jar in $JAVA_HOME/jre/lib/ext
CREATE FUNCTION AES256Decrypt(text, text)
	RETURNS text
	AS 'com.huawei.udf.AES256DecryptUDF.evaluate'
	LANGUAGE java;
--SELECT AES256Decrypt('0e493becc693142f7e8f94833d46390c77a6e21a1e770ea479bc9579c7255c75','def');
DROP FUNCTION AES256Decrypt;

--FUNC_59
CREATE FUNCTION OpenAesDecrypt(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.openAlliance.AesDecryptUDF.evaluate'
	LANGUAGE java;
SELECT OpenAesDecrypt ('45128950BC0832DD45D0D82DB954D3E2697F2507458BE388F45280AECFC29A8B9906F5747E983751E727C8F3CD9AA9F75CCC2516D0636F8E7190A07A95F54A41'); 
DROP FUNCTION OpenAesDecrypt(text);

CREATE FUNCTION OpenAesDecrypt(text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.openAlliance.AesDecryptUDF.evaluate'
	LANGUAGE java;
SELECT OpenAesDecrypt ('01e9a6fb24eb3b73e79a30ad3c6ddc8c','ads');
DROP FUNCTION OpenAesDecrypt(text, text);

--FUNC_60
CREATE FUNCTION HiboardAesDecrypt(text)
	RETURNS text
	AS 'com.huawei.udf.NegativeScreenDecrypt.evaluate'
	LANGUAGE java;
SELECT  HiboardAesDecrypt('llhiF1EMO+3qtBGefg0xB7S81HG8kMoMXWiV2gP2Lc4=');
DROP FUNCTION HiboardAesDecrypt;

--FUNC_061
CREATE FUNCTION AESDeCryptVmall(text, text)
	RETURNS text
	AS 'com.huawei.udf.AESDeCryptVmallUDF.evaluate'
	LANGUAGE java;
SELECT AESDeCryptVmall('73260498265bf42d79a5e10586d3df7c','vmall');
DROP FUNCTION AESDeCryptVmall;

--FUNC_062
CREATE FUNCTION AESCBCnopadding(text)
	RETURNS text
	AS 'com.huawei.udf.AESCBCnopadding.evaluate'
	LANGUAGE java;
SELECT AESCBCnopadding('cbc_7CFFCD04AD90D89BCCF45BED977C9E2B2CA1937CEE4C2AA99B2093BECCBC6587 ');
DROP FUNCTION AESCBCnopadding;

--FUNC_063
CREATE FUNCTION VerifyAndDecrypt(text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.hota.VerifyAndDecrypt.evaluate'
	LANGUAGE java;
SELECT VerifyAndDecrypt('93B6807E1D314F6FAB18FED05E6AD62DC6C0309EFD94B374CBFC9315392C032F0F000000B6E76C6EF29AFF144C9686DA9DA73E9E', '07b824c417057ef847db3be8071601ce'); 
DROP FUNCTION VerifyAndDecrypt;

--FUNC_064
CREATE FUNCTION VideoImeiDesCrypt(text)
	RETURNS text
	AS 'com.huawei.videoimei.udf.VideoImeiDesCryptUDF.evaluate'
	LANGUAGE java;
SELECT videoImeidescrypt('HMc0rp1SAKeBQtv19W6R1tgjGlsAQghm2MrcjBCTPjc=');
DROP FUNCTION VideoImeiDesCrypt;

--FUNC_065
CREATE FUNCTION PushRsaCryptUtils(text)
	RETURNS text
	AS 'com.huawei.udf.PushRsaCryptUtils.evaluate'
	LANGUAGE java;
SELECT pushrsacryptutils ('3d7a3ba7d2e39e0b052b24c32d6b7e1cd852c8ca9a5a3352d1630becfc2bace09e38c362bf5ea1dc1200eace91f88969f6ea511ca8395ce1bf67779a30b5c257f5a872ba14d10525eae6a64b65d4c77e013cdc5feddec5b5c2075806b16c9affab102c2cb19a43de52c8916ec4c3ca6be0f68b40e18f4ab77b674a15e7fe63cb');
DROP FUNCTION PushRsaCryptUtils;

--FUNC_066
CREATE FUNCTION Base64Decode(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.Base64DecodeUDF.evaluate'
	LANGUAGE java;
SELECT Base64Decode('MTIzNDU2');
DROP FUNCTION Base64Decode;

--FUNC_067
CREATE FUNCTION AesDecryptUDF2(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.openAlliance.AesDecryptUDF2.evaluate'
	LANGUAGE java;
SELECT AesDecryptUDF2('4602DA72EE4A09011E5D5B4FD72827AA');
DROP FUNCTION AesDecryptUDF2(text);

CREATE FUNCTION AesDecryptUDF2(text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.openAlliance.AesDecryptUDF2.evaluate'
	LANGUAGE java;
SELECT AesDecryptUDF2('85DB238A2DD76F3600D859B49234624F','def');
DROP FUNCTION AesDecryptUDF2(text, text);

--FUNC_068
CREATE FUNCTION CryptUtil(text, text, text)
	RETURNS text
	AS 'com.huawei.udf.CryptUtilUDF.evaluate'
	LANGUAGE java;
SELECT CryptUtil('sha256', '123', '');
SELECT CryptUtil('aesDec', 'ECFECFC2361FC782712718D0F46462B4', 'hwuser');
DROP FUNCTION CryptUtil;

--FUNC_069
CREATE FUNCTION udf_md5(text)
	RETURNS text
	AS 'com.huawei.bi.hive_udf.UdfMd5.evaluate([java.lang.String)'
	LANGUAGE java;
SELECT md5('123');
DROP FUNCTION udf_md5;

--FUNC_70
CREATE FUNCTION ToSHA256(text)
	RETURNS text
	AS 'com.huawei.udf.ToSHA256.evaluate'
	LANGUAGE java;
SELECT ToSHA256 ('abc');
DROP FUNCTION ToSHA256;

--FUNC_071
CREATE FUNCTION AesCBCOpenAlliance(text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.openAlliance.AesCBCOpenAlliance.evaluate'
	LANGUAGE java;
SELECT AesCBCOpenAlliance('/SMpylBB2VaK2iBzCeyv9mKKZFkT671ePjjRgpBglBU=', 'ads');
DROP FUNCTION AesCBCOpenAlliance;

--FUNC_072: 'enc' result should differ every time
CREATE FUNCTION AESCBC128(text, text,text)
	RETURNS text
	AS 'com.huawei.udf.AESCBC128.evaluate'
	LANGUAGE java;
--SELECT AESCBC128('enc','123','push');
SELECT AESCBC128('dec', str,'push') FROM (
	SELECT AESCBC128('enc','123','push') AS str
);
DROP FUNCTION AESCBC128;

--FUNC_073
CREATE FUNCTION AesDecrypt(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.AesDecryptUDF.evaluate'
	LANGUAGE java;
SELECT aesDecrypt('79C33E95A5484297E542859CD37AF711');
DROP FUNCTION AesDecrypt(text);

CREATE FUNCTION AesDecrypt(text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.AesDecryptUDF.evaluate'
	LANGUAGE java;
SELECT aesDecrypt('60DB73AF04E5EAEE23E1F97643F23BBE','hota');
DROP FUNCTION AesDecrypt(text, text);

--FUNC_074
CREATE FUNCTION HitopDecryptAES(text)
	RETURNS text
	AS 'com.huawei.udf.HitopDecryptAES.evaluate'
	LANGUAGE java;
SELECT HitopDecryptAES('53CD1163AD1F80DE0196DE5C832A5036');
DROP FUNCTION HitopDecryptAES;

CREATE FUNCTION HitopDecryptAES(text, text)
	RETURNS text
	AS 'com.huawei.udf.HitopDecryptAES.evaluate'
	LANGUAGE java;
--LACK OF TESTCASE
DROP FUNCTION HitopDecryptAES;

--FUNC_075
CREATE FUNCTION ShaEncrypt(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.ShaEncryptUDF.evaluate'
	LANGUAGE java;
SELECT ShaEncrypt('123');
DROP FUNCTION ShaEncrypt(text);

CREATE FUNCTION ShaEncrypt(text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.ShaEncryptUDF.evaluate'
	LANGUAGE java;
SELECT ShaEncrypt('123', 'base64');
DROP FUNCTION ShaEncrypt(text, text);

--FUNC_076: Result should differ every time.
CREATE FUNCTION CommonAesCBCEncrypt(text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.CommonAesCBCEncryptUDF.evaluate'
	LANGUAGE java;
--SELECT CommonAesCBCEncrypt('862268039012356','game');
DROP FUNCTION CommonAesCBCEncrypt;

--FUNC_077
CREATE FUNCTION CommonAesCBCDecrypt(text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.CommonAesCBCDecryptUDF.evaluate'
	LANGUAGE java;
SELECT CommonAesCBCDecrypt('ZmQzZjNlOGI4N2M0OTJlMLx7hs/wIlRBi7uUA1s1Sh0=','game');
SELECT CommonAesCBCDecrypt('7bD0ccb9X2JrCGWo1pOSysUXyrfCi35IxqAKRbycKFc=','hwvideo');
DROP FUNCTION CommonAesCBCDecrypt;

--FUNC_078
CREATE FUNCTION AesCBCUpDecry(text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.up.AesCBCUpDecry.evaluate'
	LANGUAGE java;
SELECT AesCBCUpDecry('0FE34B0926754D082736BC788199D0AE:9345F35F4424877FA325693874ACD88B','game');
DROP FUNCTION AesCBCUpDecry(text, text);

--FUNC_079
CREATE FUNCTION DecryptAES(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.wlan.DecryptAES.evaluate'
	LANGUAGE java;
SELECT DecryptAES('1f4d3b181819d475de19b821aa3d5b9ad65ff3a930cc5421d4bcba1cea0b420a');
DROP FUNCTION DecryptAES;

--FUNC_080
CREATE FUNCTION TradeDecrypt(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.trade.Decrypt.evaluate'
	LANGUAGE java;
SELECT TradeDecrypt('1-AES-03f5e53aaa2d98b3113872bf40567885debcee9f64a044622abda9de36890f29');
DROP FUNCTION TradeDecrypt;

--FUNC_081
CREATE FUNCTION HealthGPSDecrypt(text)
	RETURNS text
	AS 'com.huawei.udf.HealthGPSDecryptUDF.evaluate'
	LANGUAGE java;
SELECT HealthGPSDecrypt('1cro7uN373zw56cv4BoE9uzmyXSB3ZZx9ogs/UXgl/rCnGFfNcaBvz+MnGYVS7IshJwNLK1RzKl1M5euBCSQH7wCy+PTFFXHmXlFdwBS3MEF11hDhkWJcLnXOJV3ZKTWKScMw1xAOjeT7fgTvKRWRnUemfBGGQuil6p8ylRMvVf6TejogSm6M5EF+oNGZXh3JTrQAtZ3zm27onOhKswCQEIdXqyGCcBQmb6IiKe13hl7kHGOubO/iTk8aYVmk4a/TdY+EK0vYip/IItn3BOiVlpSjo2AnwTMryWL7UbdKZBCPvtZQBP5zSD1WzHpv1GC11kGYryxCNaO0BZwa8yga4PXIIZ0HQkmdJgV+O8+YCLBYrzdPsQVESw8d+aH2FaF');
DROP FUNCTION HealthGPSDecrypt;

--FUNC_082
CREATE FUNCTION AesHiadDecrypt(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.hispace.AesHiadDecryptUDF.evaluate'
	LANGUAGE java;
SELECT AesHiadDecrypt('7A8F91E05019225902356B67AB78FB2E');
DROP FUNCTION AesHiadDecrypt;

--FUNC_083: needs bcprov-ext-jdk15on-153.jar in $JAVA_HOME/jre/lib/ext
CREATE FUNCTION HmacSha256Encrypt(text)
	RETURNS text
	AS 'com.huawei.udf.HmacSha256EncryptUDF.evaluate'
	LANGUAGE java;
--SELECT HmacSha256Encrypt ('ss');
DROP FUNCTION HmacSha256Encrypt;

--FUNC_083
CREATE FUNCTION IP2AreaInfo(text)
	RETURNS text
	AS 'com.huawei.udf.IP2AreaInfoUDF.evaluate'
	LANGUAGE java;
SELECT IP2AreaInfo('221.226.2.254');
DROP FUNCTION IP2AreaInfo(text);

--FUNC_084
CREATE FUNCTION IP2AreaInfo(text, integer)
	RETURNS text
	AS 'com.huawei.udf.IP2AreaInfoUDF.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT IP2AreaInfo('221.226.2.254',2);
DROP FUNCTION IP2AreaInfo(text, integer);

--FUNC_085
CREATE FUNCTION GeoIpCountryApiUDF(text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.GeoIpCountryApiUDF.evaluate'
	LANGUAGE java;
SELECT GeoIpCountryApiUDF('125.107.106.170','code');
SELECT GeoIpCountryApiUDF('125.107.106.170','chineseName');
DROP FUNCTION GeoIpCountryApiUDF;

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
--预期结果：
--520250170000000
DROP FUNCTION GetAreaInfo;

--FUNC_091: needs bcprov-ext-jdk15on-153.jar in $JAVA_HOME/jre/lib/ext
CREATE FUNCTION GeoIpCountry(text,text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.GeoIpCountryUDF.evaluate'
	LANGUAGE java;
--SELECT  GeoIpCountry('203.81.93.101','code');
--SELECT  GeoIpCountry('203.81.93.105','englishName');
DROP FUNCTION GeoIpCountry;

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
DROP FUNCTION MD5UDF;

--FUNC_096
CREATE FUNCTION DateUtil(integer)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.DateUtilUDF.evaluate(java.lang.Integer)'
	LANGUAGE java;
SELECT DateUtil (666);
DROP FUNCTION DateUtil(integer);

CREATE FUNCTION DateUtil(int8, integer, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.DateUtilUDF.evaluate(java.lang.Long, java.lang.Integer, java.lang.String)'
	LANGUAGE java;
SELECT DateUtil (1111111111111 ,222222222, 'YYYYMM');
DROP FUNCTION DateUtil(int8, integer, text);

CREATE FUNCTION DateUtil(text, text, integer)
	RETURNS int8
	AS 'com.huawei.platform.bi.udf.common.DateUtilUDF.evaluate(java.lang.String, java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT DateUtil ('201712' ,'yyyyMM',2);
DROP FUNCTION DateUtil(text, text, integer);

CREATE FUNCTION DateUtil(text, text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.common.DateUtilUDF.evaluate'
	LANGUAGE java;
SELECT DateUtil ('201712' ,'yyyyMM','yyyy-MM');
DROP FUNCTION DateUtil(text, text, text);

--FUNC_097
CREATE FUNCTION SHA256(varchar)
	RETURNS varchar
	AS 'com.huawei.udf.SHA256.evaluate'
	LANGUAGE java;
SELECT SHA256 ('aad');
DROP FUNCTION SHA256;

--FUNC_099
CREATE FUNCTION UpDecryption(text)
	RETURNS text
	AS 'com.huawei.udf.UpDecryptionUDF.evaluate'
	LANGUAGE java;
SELECT UpDecryption('CC8185CABC7982A15270677A83758AB6');
DROP FUNCTION UpDecryption(text);

CREATE FUNCTION UpDecryption(text, text)
	RETURNS text
	AS 'com.huawei.udf.UpDecryptionUDF.evaluate'
	LANGUAGE java;
SELECT UpDecryption('CC8185CABC7982A15270677A83758AB6','up');
DROP FUNCTION UpDecryption(text, text);

--FUNC_100
CREATE FUNCTION RepeatGroupCid(int4, text)
	RETURNS text
	AS'com.huawei.platform.bi.udf.service.vmall.RepeatGroupCidUDF.evaluate(java.lang.Integer, java.lang.String)'
	LANGUAGE java;
SELECT RepeatGroupCid (1,'abc');
DROP FUNCTION RepeatGroupCid(int4, text);

--!FUNC_101.	DbankNpDownload returns Map

--!FUNC_102.	groupByLogin input Object, can not change to static 

--!FUNC_103.	dbankDnUpload  returns map

--FUNC_104
CREATE FUNCTION SlideAndScreen(text, text)
	RETURNS text
	AS'com.huawei.platform.bi.udf.service.hota.SlideAndScreenUDF.evaluate'
	LANGUAGE java;
SELECT SlideAndScreen('000000000000000000000000000000000000000000000000000000000000000000000000000101010100000000000000000000000000000000020300010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000020201000000000000000000000000000000000202000200020000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000', '2016-11-24 08:12:54');
DROP FUNCTION SlideAndScreen(text, text);
--!FUNC_105.	UUID   DeferredObject

--!FUNC_106.	UrlDecoder  can not change to static

--!FUNC_107.	IsCustomCommit  dependent file

--FUNC_108
CREATE FUNCTION GetAdPushReportMsg(text, int4)
	RETURNS text
	AS'com.huawei.udf.GetAdPushReportMsg.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT GetAdPushReportMsg ('timeStamp|PS|deviceid|msgId|eventid|agent version|msg cmd',1);
DROP FUNCTION GetAdPushReportMsg(text,int4);

--FUNC_109
CREATE FUNCTION GetFileId(text)
	RETURNS text
	AS'com.huawei.udf.DbankGetFileId.evaluate'
	LANGUAGE java;
SELECT getFileId('/file/x/xxx.sql');
DROP FUNCTION GetFileId(text);

--FUNC_110
CREATE FUNCTION GetFileName(text)
	RETURNS text
	AS'com.huawei.udf.DbankGetFileName.evaluate'
	LANGUAGE java;
SELECT getFileName('/file/x/y/xxx.sql');
DROP FUNCTION GetFileName(text) ;

--FUNC_111
CREATE FUNCTION GetHonorVersion(text)
	RETURNS text
	AS'com.huawei.udf.GetHonorVersion.evaluate'
	LANGUAGE java;
SELECT GetHonorVersion('EMUI2.0_G750-T00_4.4.16') ;
DROP FUNCTION GetHonorVersion(text);

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
DROP FUNCTION GetLastValue;

--!FUNC_113.	GetPushPre input ArrayList
--!FUNC_114.	OsVauleSort dependent on file 
--!FUNC_115.	PushMsgProcess input ArrayList<String>

--FUNC_116
CREATE FUNCTION ReplaceTeminal(text)
	RETURNS text
	AS'com.huawei.udf.ReplaceTeminal.evaluate'
	LANGUAGE java;
SELECT replaceteminal('2.2.1') ;
DROP FUNCTION ReplaceTeminal(text) ;

--FUNC_117
CREATE FUNCTION replaceTerminalInfo(text)
	RETURNS text
	AS'com.huawei.udf.ReplaceTerminalInfoUDF.evaluate'
	LANGUAGE java;
SELECT replaceTerminalInfo('coolpad5860');
DROP FUNCTION replaceTerminalInfo(text);

--FUNC_118
CREATE FUNCTION replaceTerminalOs(text)
	RETURNS text
	AS'com.huawei.udf.ReplaceTerminalOsUDF.evaluate'
	LANGUAGE java;
SELECT replaceTerminalOs('4.2');
DROP FUNCTION replaceTerminalOs(text);

--!FUNC_119.	versionUpgrade input ArrayList

--FUNC_120
CREATE FUNCTION GetAdPushMsg(text, int4)
	RETURNS text
	AS'com.huawei.udf.GetAdPushMsg.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
--LACK OF TESTCASE
DROP FUNCTION GetAdPushMsg(text, int4);

--!FUNC_121 GetHonorTimeAndVersions returns map<String,String>
--CREATE FUNCTION GetHonorTimeAndVersions RETURNS text AS'com.huawei.udf.GetHonorTimeAndVersions.evaluate' LANGUAGE java;
--SELECT GetHonorTimeAndVersions('20151001');

--FUNC_122 GetMinSeconds returns Long
CREATE FUNCTION GetMinSeconds(text, text)
	RETURNS int8 
	AS'com.huawei.udf.GetMinSecondsUDF.evaluate'
	LANGUAGE java; 
SELECT GetMinSeconds('20161010','yyyyMMdd');
DROP FUNCTION GetMinSeconds(text, text);

--!FUNC_123  has dependent file

--FUNC_124
CREATE FUNCTION GetOperatorFromNumber(text)
	RETURNS text
	AS 'com.huawei.udf.GetOperatorFromNumber.evaluate'
	LANGUAGE java;
SELECT getOperatorFromNumber('13665696273');
DROP FUNCTION GetOperatorFromNumber(text);

--FUNC_125
CREATE FUNCTION IsDeviceIdLegal(varchar)
	RETURNS bool
	AS 'com.huawei.udf.IsDeviceIdLegal.evaluate'
	LANGUAGE java;
SELECT isdeviceidlegal('0123456789abcde');
DROP FUNCTION IsDeviceIdLegal;

--FUNC_126
CREATE FUNCTION LongToIP(int8)
	RETURNS text
	AS 'com.huawei.udf.LongToIPUDF.evaluate(java.lang.Long)'
	LANGUAGE java;
SELECT LongToIP(19216811);
DROP FUNCTION LongToIP;

--FUNC_127
CREATE FUNCTION LinkRelativeRatio(float8, float8, int4)
	RETURNS float8
	AS 'com.huawei.udf.LinkRelativeRatioUDF.evaluate(java.lang.Double, java.lang.Double, java.lang.Integer)'
	LANGUAGE java;
SELECT LinkRelativeRatio(1.1,2.3,5);
DROP FUNCTION LinkRelativeRatio;

CREATE FUNCTION LinkRelativeRatio(float8,float8)
	RETURNS float8
	AS 'com.huawei.udf.LinkRelativeRatioUDF.evaluate(java.lang.Double, java.lang.Double)'
	LANGUAGE java;
SELECT LinkRelativeRatio(1.1,2.3);
DROP FUNCTION LinkRelativeRatio;

--FUNC_128
CREATE FUNCTION RevertDeviceId(varchar)
	RETURNS varchar
	AS 'com.huawei.udf.RevertDeviceId.evaluate'
	LANGUAGE java;
SELECT revertDeviceId('asd1234123456789');
DROP FUNCTION RevertDeviceId;

--FUNC_129
CREATE FUNCTION ServiceEntry(text, text)
	RETURNS int4
	AS 'com.huawei.udf.ServiceEntryUDF.evaluate'
	LANGUAGE java;
SELECT ServiceEntry ('','123');
SELECT ServiceEntry ('mw','');
SELECT ServiceEntry ('mw','4000000') ;
SELECT ServiceEntry ('mw','400000') ;
DROP FUNCTION ServiceEntry(text, text);

CREATE FUNCTION ServiceEntry(text)
	RETURNS text
	AS 'com.huawei.udf.ServiceEntryUDF.evaluate'
	LANGUAGE java;
SELECT ServiceEntry ('mw123');
DROP FUNCTION ServiceEntry;

--!FUNC_130 GetSearchType returns ArrayList

--FUNC_131
CREATE FUNCTION RomListExplode(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.hota.RomListExplodeUDF.evaluate'
	LANGUAGE java;
SELECT RomListExplode('[{\"romVersion\":\"android:1.0\",\"updateTime\":\"20161111\"},{\"romVersion\":\"android:2.0\",\"updateTime\":\"20161112\"},{\"romVersion\":\"android:3.0\",\"updateTime\":\"20161113\"}]') ;
DROP FUNCTION RomListExplode;

--FUNC_132 ItemID
CREATE FUNCTION ItemID(text)
	RETURNS text
	AS 'com.huawei.udf.ItemIDUDF.evaluate'
	LANGUAGE java;
SELECT ItemID('abc') ;
DROP FUNCTION ItemID(text);

CREATE FUNCTION ItemID(text, int4)
	RETURNS text
	AS 'com.huawei.udf.ItemIDUDF.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT ItemID('abc',1);
DROP FUNCTION ItemID(text, int4);

CREATE FUNCTION ItemID(text, text)
	RETURNS text
	AS 'com.huawei.udf.ItemIDUDF.evaluate'
	LANGUAGE java;
SELECT ItemID('9016225876650000000','abc');
DROP FUNCTION ItemID(text, text);

CREATE FUNCTION ItemID(text, text, int4)
	RETURNS text
	AS 'com.huawei.udf.ItemIDUDF.evaluate(java.lang.String, java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT ItemID(null,'abc',1);
DROP FUNCTION ItemID(text, text, int4);

--FUNC_133
CREATE FUNCTION GetTerminalOs(text, int4)
	RETURNS text
	AS 'com.huawei.udf.GetTerminalOsUDF.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT GetTerminalOs ('android1.5abc',1);
DROP FUNCTION GetTerminalOs;

CREATE FUNCTION GetTerminalOs(text)
	RETURNS text
	AS 'com.huawei.udf.GetTerminalOsUDF.evaluate'
	LANGUAGE java;
SELECT GetTerminalOs ('2') ;
DROP FUNCTION GetTerminalOs(text);

--FUNC_134
CREATE FUNCTION HotaExtraInfo(text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.hota.HotaExtraInfoUDF.evaluate'
	LANGUAGE java;
SELECT HotaExtraInfo('VIE-L29','860864033123322');
DROP FUNCTION HotaExtraInfo;

CREATE FUNCTION HotaExtraInfo(text, text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.hota.HotaExtraInfoUDF.evaluate'
	LANGUAGE java;
SELECT HotaExtraInfo('44:6E:E5:70:DB:44','VIE-L29','860864033123322');
DROP FUNCTION HotaExtraInfo(text,text,text);

--!FUNC_135 DivEmui_PsInterval return ArrayList

--!FUNC_136 DiviDeInterval return ArrayList

--FUNC_137
CREATE FUNCTION GetCver(text, text)
	RETURNS text
	AS 'com.huawei.udf.GetCver.evaluate'
	LANGUAGE java;
SELECT GetCver('gf.232','233');
DROP FUNCTION GetCver;

--FUNC_138
CREATE FUNCTION GetForumId(text, text)
	RETURNS int4
	AS 'com.huawei.udf.GetForumId.evaluate'
	LANGUAGE java;
SELECT GetForumId ('fid', 'www.baidu.com&fid=123');
DROP FUNCTION GetForumId;

--!FUNC_139 HispaceVersionVauleSort dependent file

--FUNC_140
CREATE FUNCTION SearchWord(text)
	RETURNS text
	AS 'com.huawei.udf.SearchWordUDF.evaluate'
	LANGUAGE java;
SELECT searchWord('asd/sd2');
DROP FUNCTION SearchWord;

--!FUNC_141 GetOperators dependent file

--FUNC_142
CREATE FUNCTION userId(int8)
	RETURNS int4
	AS 'com.huawei.udf.UserIDUDF.evaluate(java.lang.Long)'
	LANGUAGE java;
SELECT userId (1243401);
DROP FUNCTION userId(int8);

CREATE FUNCTION userId(text)
	RETURNS text
	AS 'com.huawei.udf.UserIDUDF.evaluate'
	LANGUAGE java;
SELECT userId ('a124343');
DROP FUNCTION userId(text);

CREATE FUNCTION userId(text, text)
	RETURNS text
	AS 'com.huawei.udf.UserIDUDF.evaluate'
	LANGUAGE java;
SELECT userId ('ASD123','a124343');
DROP FUNCTION userId(text, text);

CREATE FUNCTION userId(text, text, int4)
	RETURNS text
	AS 'com.huawei.udf.UserIDUDF.evaluate(java.lang.String, java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT userId ('ASD123','a124343',2);
DROP FUNCTION userId(text, text, int4) ;

CREATE FUNCTION userId(text, int4)
	RETURNS text
	AS 'com.huawei.udf.UserIDUDF.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT userId ('a124343',2);
DROP FUNCTION userId(text, int4);

--FUNC_143
CREATE FUNCTION GetHiCloudAppId(varchar)
	RETURNS varchar
	AS 'com.huawei.platform.bi.udf.service.hispace.GetHiCloudAppId.evaluate'
	LANGUAGE java;
SELECT getHiCloudAppId('1000123');
DROP FUNCTION GetHiCloudAppId;

--FUNC_144
CREATE FUNCTION GetPhoneBrandUDF(text)
	RETURNS text
	AS 'com.huawei.udf.GetPhoneBrandUDF.evaluate'
	LANGUAGE java;
SELECT GetPhoneBrandUDF('HUAWEI P10') ;
DROP FUNCTION GetPhoneBrandUDF;

--FUNC_145
CREATE FUNCTION CloudPlusAes(text)
	RETURNS text
	AS 'com.huawei.udf.CloudPlusAes.evaluate'
	LANGUAGE java;
SELECT CloudPlusAes('ad9baacdefaa91afd8eae7cef52e39d3');
DROP FUNCTION CloudPlusAes;

--FUNC_146
CREATE FUNCTION QUOTA(text[])
	RETURNS text
	AS 'com.huawei.bi.hive_udf.UdfQuota.evaluate'
	LANGUAGE java;
SELECT QUOTA(ARRAY['{a:"1"}']);
DROP FUNCTION QUOTA;

--FUNC_147
CREATE FUNCTION GetDistanceByGPS(float8,float8,float8,float8)
	RETURNS float8
	AS 'com.huawei.udf.GetDistanceByGPS.evaluate(java.lang.Double, java.lang.Double,java.lang.Double, java.lang.Double)'
	LANGUAGE java;
SELECT GetDistanceByGPS(0.1, 0.2, 0.3, 0.4);
SELECT GetDistanceByGPS(NULL,NULL,NULL,NULL) IS NULL;
DROP FUNCTION GetDistanceByGPS;

--FUNC148
CREATE FUNCTION InsertSQLFactory(text[])
	RETURNS text
	AS 'com.huawei.bi.hive_udf.InsertSQLFactoryUDF.evaluate'
	LANGUAGE java;
SELECT insertSQLFactory(ARRAY['dual','value1']);
DROP FUNCTION InsertSQLFactory;

--FUNC_149
CREATE FUNCTION AESDeCryptUtil(text)
	RETURNS text
	AS 'com.huawei.udf.AESDeCryptUtilUDF.evaluate'
	LANGUAGE java;
SELECT AESDeCryptUtil('E00BED28C373A83F9EBC5FD565A561A0');
DROP FUNCTION AESDeCryptUtil;

--!FUNC_150 explodeString UDTF

--FUNC_151
CREATE FUNCTION udf_md5(text[])
	RETURNS text
	AS 'com.huawei.bi.hive_udf.UdfMd5.evaluate'
	LANGUAGE java;
SELECT udf_md5(ARRAY['abc']);
SELECT udf_md5(ARRAY['a,b,c',',']);
SELECT udf_md5(array['a,b,c',',','-']);
DROP FUNCTION udf_md5;

DROP SCHEMA bicoredata CASCADE;
