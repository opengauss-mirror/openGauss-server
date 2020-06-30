\o xml_explain_temp.txt
set explain_perf_mode=pretty;
explain performance
SELECT Party_Id, Zone_Num, Asset_Max_Belong_Org_Num FROM(SELECT 
   T1.Party_Id                                            
  ,T1.Zone_Num                                            
  ,T1.Asset_Max_Belong_Org_Num                                         
   ,ROW_NUMBER() OVER(PARTITION BY T1.Party_Id) AS QUA_ROW_NUM_1 
FROM TMP_CUST_ASSET_SUM_1 T1 group by 1,2,3
)AA WHERE QUA_ROW_NUM_1 <= 1
;
explain analyze
SELECT Party_Id, Zone_Num, Asset_Max_Belong_Org_Num FROM(SELECT 
   T1.Party_Id                                            
  ,T1.Zone_Num                                            
  ,T1.Asset_Max_Belong_Org_Num                                         
   ,ROW_NUMBER() OVER(PARTITION BY T1.Party_Id) AS QUA_ROW_NUM_1 
FROM TMP_CUST_ASSET_SUM_1 T1 group by 1,2,3
)AA WHERE QUA_ROW_NUM_1 <= 1
;
reset explain_perf_mode;
explain (analyze on, detail on, costs off, format xml)
SELECT Party_Id, Zone_Num, Asset_Max_Belong_Org_Num FROM(SELECT 
   T1.Party_Id                                            
  ,T1.Zone_Num                                            
  ,T1.Asset_Max_Belong_Org_Num                                         
   ,ROW_NUMBER() OVER(PARTITION BY T1.Party_Id) AS QUA_ROW_NUM_1 
FROM TMP_CUST_ASSET_SUM_1 T1 group by 1,2,3
)AA WHERE QUA_ROW_NUM_1 <= 1
;
\o
\! rm xml_explain_temp.txt
explain (analyze on, costs off)
SELECT Party_Id, Zone_Num, Asset_Max_Belong_Org_Num FROM(SELECT 
   T1.Party_Id                                            
  ,T1.Zone_Num                                            
  ,T1.Asset_Max_Belong_Org_Num                                         
   ,ROW_NUMBER() OVER(PARTITION BY T1.Party_Id) AS QUA_ROW_NUM_1 
FROM TMP_CUST_ASSET_SUM_1 T1 group by 1,2,3
)AA WHERE QUA_ROW_NUM_1 <= 1
;

SELECT Party_Id, Zone_Num, Asset_Max_Belong_Org_Num FROM(SELECT 
   T1.Party_Id                                            
  ,T1.Zone_Num                                            
  ,T1.Asset_Max_Belong_Org_Num                                         
   ,ROW_NUMBER() OVER(PARTITION BY T1.Party_Id) AS QUA_ROW_NUM_1 
FROM TMP_CUST_ASSET_SUM_1 T1 group by 1,2,3
)AA WHERE QUA_ROW_NUM_1 <= 1 limit 1
;

set enable_compress_spill = off;
\o xml_explain_temp.txt
SELECT Party_Id, Zone_Num, Asset_Max_Belong_Org_Num FROM(SELECT 
   T1.Party_Id                                            
  ,T1.Zone_Num                                            
  ,T1.Asset_Max_Belong_Org_Num                                         
   ,ROW_NUMBER() OVER(PARTITION BY T1.Party_Id) AS QUA_ROW_NUM_1 
FROM TMP_CUST_ASSET_SUM_1 T1 group by 1,2,3
)AA WHERE QUA_ROW_NUM_1 <= 1 limit 1
;
\o
\! rm xml_explain_temp.txt
reset enable_compress_spill;
select count(*) from TMP_ASSET_MAX_BELONG;
drop table TMP_ASSET_MAX_BELONG;

