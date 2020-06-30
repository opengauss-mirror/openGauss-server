CREATE UNLOGGED TABLE TMP_ASSET_MAX_BELONG(
   Party_Id                 VARCHAR(30)    NOT NULL       
  ,Zone_Num                 CHAR(5)        NOT NULL       
  ,Asset_Max_Belong_Org_Num VARCHAR(30)    NOT NULL       
);

select count(*) from TMP_CUST_ASSET_SUM_1;

set work_mem='64kB';
set query_mem=0;
set explain_perf_mode=pretty;
\o write_file_cn.out
explain (analyze on,detail on, timing off)select count(*) from tmp_cust_asset_sum_1 group by zone_num;
\o
\! rm write_file_cn.out
set explain_perf_mode=normal;

analyze TMP_CUST_ASSET_SUM_1;
explain (costs off)
INSERT INTO TMP_ASSET_MAX_BELONG(
   Party_Id                                               
  ,Zone_Num                                              
  ,Asset_Max_Belong_Org_Num                               
)
SELECT 
   T1.Party_Id                                            
  ,T1.Zone_Num                                            
  ,T1.asset_max_belong_org_num                                         
FROM TMP_CUST_ASSET_SUM_1 T1
GROUP BY 1,2,3
;

INSERT INTO TMP_ASSET_MAX_BELONG(
   Party_Id                                               
  ,Zone_Num                                              
  ,Asset_Max_Belong_Org_Num                               
)
SELECT 
   T1.Party_Id                                            
  ,T1.Zone_Num                                            
  ,T1.asset_max_belong_org_num                                         
FROM TMP_CUST_ASSET_SUM_1 T1
GROUP BY 1,2,3
;

select count(*) from TMP_ASSET_MAX_BELONG;
INSERT INTO TMP_ASSET_MAX_BELONG(
   Party_Id                                               
  ,Zone_Num                                               
  ,Asset_Max_Belong_Org_Num                               
)
SELECT Party_Id, Zone_Num, Asset_Max_Belong_Org_Num FROM(SELECT 
   T1.Party_Id                                            
  ,T1.Zone_Num                                            
  ,T1.Asset_Max_Belong_Org_Num                                         
   ,ROW_NUMBER() OVER(PARTITION BY T1.Party_Id) AS QUA_ROW_NUM_1 
FROM TMP_CUST_ASSET_SUM_1 T1 group by 1,2,3
)AA WHERE QUA_ROW_NUM_1 <= 1
;

select count(*) from TMP_ASSET_MAX_BELONG;

explain (analyze on, detail on, costs off)
SELECT Party_Id, Zone_Num, Asset_Max_Belong_Org_Num FROM(SELECT 
   T1.Party_Id                                            
  ,T1.Zone_Num                                            
  ,T1.Asset_Max_Belong_Org_Num                                         
   ,ROW_NUMBER() OVER(PARTITION BY T1.Party_Id) AS QUA_ROW_NUM_1 
FROM TMP_CUST_ASSET_SUM_1 T1 group by 1,2,3
)AA WHERE QUA_ROW_NUM_1 <= 1
;