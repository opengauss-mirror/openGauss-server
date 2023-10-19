DROP SCHEMA bicoredata_009 CASCADE;
CREATE SCHEMA bicoredata_009;
SET CURRENT_SCHEMA = bicoredata_009;

--FUNC_129
CREATE FUNCTION ServiceEntry(text, text)
	RETURNS int4
	AS 'com.huawei.udf.ServiceEntryUDF.evaluate'
	LANGUAGE java;
SELECT ServiceEntry ('','123');
SELECT ServiceEntry ('mw','');
SELECT ServiceEntry ('mw','4000000') ;
SELECT ServiceEntry ('mw','400000') ;

CREATE FUNCTION ServiceEntry(text)
	RETURNS text
	AS 'com.huawei.udf.ServiceEntryUDF.evaluate'
	LANGUAGE java;
SELECT ServiceEntry ('mw123');

--!FUNC_130 GetSearchType returns ArrayList

--FUNC_131
CREATE FUNCTION RomListExplode(text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.hota.RomListExplodeUDF.evaluate'
	LANGUAGE java;
SELECT RomListExplode('[{\"romVersion\":\"android:1.0\",\"updateTime\":\"20161111\"},{\"romVersion\":\"android:2.0\",\"updateTime\":\"20161112\"},{\"romVersion\":\"android:3.0\",\"updateTime\":\"20161113\"}]') ;

--FUNC_132 ItemID
CREATE FUNCTION ItemID(text)
	RETURNS text
	AS 'com.huawei.udf.ItemIDUDF.evaluate'
	LANGUAGE java;
SELECT ItemID('abc') ;

CREATE FUNCTION ItemID(text, int4)
	RETURNS text
	AS 'com.huawei.udf.ItemIDUDF.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT ItemID('abc',1);

CREATE FUNCTION ItemID(text, text)
	RETURNS text
	AS 'com.huawei.udf.ItemIDUDF.evaluate'
	LANGUAGE java;
SELECT ItemID('9016225876650000000','abc');

CREATE FUNCTION ItemID(text, text, int4)
	RETURNS text
	AS 'com.huawei.udf.ItemIDUDF.evaluate(java.lang.String, java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT ItemID(null,'abc',1);

--FUNC_133
CREATE FUNCTION GetTerminalOs(text, int4)
	RETURNS text
	AS 'com.huawei.udf.GetTerminalOsUDF.evaluate(java.lang.String, java.lang.Integer)'
	LANGUAGE java;
SELECT GetTerminalOs ('android1.5abc',1);

CREATE FUNCTION GetTerminalOs(text)
	RETURNS text
	AS 'com.huawei.udf.GetTerminalOsUDF.evaluate'
	LANGUAGE java;
SELECT GetTerminalOs ('2') ;

--FUNC_134
CREATE FUNCTION HotaExtraInfo(text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.hota.HotaExtraInfoUDF.evaluate'
	LANGUAGE java;
SELECT HotaExtraInfo('VIE-L29','860864033123322');

CREATE FUNCTION HotaExtraInfo(text, text, text)
	RETURNS text
	AS 'com.huawei.platform.bi.udf.service.hota.HotaExtraInfoUDF.evaluate'
	LANGUAGE java;
SELECT HotaExtraInfo('44:6E:E5:70:DB:44','VIE-L29','860864033123322');

--!FUNC_135 DivEmui_PsInterval return ArrayList
--!FUNC_136 DiviDeInterval return ArrayList

--FUNC_137
CREATE FUNCTION GetCver(text, text)
	RETURNS text
	AS 'com.huawei.udf.GetCver.evaluate'
	LANGUAGE java;
SELECT GetCver('gf.232','233');

--FUNC_138
CREATE FUNCTION GetForumId(text, text)
	RETURNS int4
	AS 'com.huawei.udf.GetForumId.evaluate'
	LANGUAGE java;
SELECT GetForumId ('fid', 'www.baidu.com&fid=123');

--!FUNC_139 HispaceVersionVauleSort dependent file

--FUNC_140
CREATE FUNCTION SearchWord(text)
	RETURNS text
	AS 'com.huawei.udf.SearchWordUDF.evaluate'
	LANGUAGE java;
SELECT searchWord('asd/sd2');
DROP SCHEMA bicoredata_009 CASCADE;
