create table TMP_CUST_ASSET_SUM_1_col
(
Party_Id                 VARCHAR(30)    NOT NULL,
Zone_Num                 CHAR(5)        NOT NULL,
Asset_Max_Belong_Org_Num VARCHAR(30)    NOT NULL
)with(orientation = column);

insert into TMP_CUST_ASSET_SUM_1_col select * from TMP_CUST_ASSET_SUM_1;
analyze TMP_CUST_ASSET_SUM_1_col;
explain (analyze on, costs off)
SELECT Party_Id, Zone_Num, Asset_Max_Belong_Org_Num FROM(SELECT 
   T1.Party_Id                                            
  ,T1.Zone_Num                                            
  ,T1.Asset_Max_Belong_Org_Num                                         
   ,ROW_NUMBER() OVER(PARTITION BY T1.Party_Id) AS QUA_ROW_NUM_1 
FROM TMP_CUST_ASSET_SUM_1_col T1 group by 1,2,3
)AA WHERE QUA_ROW_NUM_1 <= 1
;

set work_mem=64;
explain (analyze on, costs off)
SELECT
   max(T1.Party_Id)
  ,T1.Zone_Num
FROM TMP_CUST_ASSET_SUM_1 T1 
group by T1.Zone_Num;


\o xml_explain_temp_1.txt
set explain_perf_mode=pretty;
explain performance
SELECT Party_Id, Zone_Num, Asset_Max_Belong_Org_Num FROM(SELECT 
   T1.Party_Id                                            
  ,T1.Zone_Num                                            
  ,T1.Asset_Max_Belong_Org_Num                                         
   ,ROW_NUMBER() OVER(PARTITION BY T1.Party_Id) AS QUA_ROW_NUM_1 
FROM TMP_CUST_ASSET_SUM_1_col T1 group by 1,2,3
)AA WHERE QUA_ROW_NUM_1 <= 1
;
explain analyze
SELECT Party_Id, Zone_Num, Asset_Max_Belong_Org_Num FROM(SELECT 
   T1.Party_Id                                            
  ,T1.Zone_Num                                            
  ,T1.Asset_Max_Belong_Org_Num                                         
   ,ROW_NUMBER() OVER(PARTITION BY T1.Party_Id) AS QUA_ROW_NUM_1 
FROM TMP_CUST_ASSET_SUM_1_col T1 group by 1,2,3
)AA WHERE QUA_ROW_NUM_1 <= 1
;
reset explain_perf_mode;
explain (analyze on, costs off, FORMAT xml)
SELECT Party_Id, Zone_Num, Asset_Max_Belong_Org_Num FROM(SELECT 
   T1.Party_Id                                            
  ,T1.Zone_Num                                            
  ,T1.Asset_Max_Belong_Org_Num                                         
   ,ROW_NUMBER() OVER(PARTITION BY T1.Party_Id) AS QUA_ROW_NUM_1 
FROM TMP_CUST_ASSET_SUM_1_col T1 group by 1,2,3
)AA WHERE QUA_ROW_NUM_1 <= 1
;
\o
\! rm xml_explain_temp_1.txt

drop table TMP_CUST_ASSET_SUM_1_col;
