EXPLAIN(costs off)
INSERT INTO EDISUM.C00_ICBC_INT_ORG_OVERVIEW_H
(
      Int_Org_Num
     ,Start_Dt
     ,Int_Org_Num_Nm
     ,Int_Org_Num_Type
     ,Int_Org_Num_Subbranch
     ,Int_Org_Num_Subbranch_Nm
     ,Int_Org_Num_Branch
     ,Int_Org_Num_Branch_Nm
     ,Int_Org_Num_Probranch
     ,Int_Org_Num_Probranch_Nm
     ,End_Dt
     ,Etl_Job
)
SELECT
     COALESCE(((lpad(T06.ZONE_ID, 5, '0'))||(lpad(T06.BRCH_ID, 5, '0'))),'') as Int_Org_Num
    ,CAST('19000101' AS DATE)
    ,COALESCE(T05.STRU_NAME,'')
    ,'004'
    ,CAST(COALESCE((lpad(T04.ZONE_ID, 5, '0')),'') AS CHAR(5))||CAST(COALESCE((lpad(T04.BRCH_ID, 5, '0')),'') AS CHAR(5))
    ,COALESCE(T04.STRU_NAME,'')
    ,CAST(COALESCE((lpad(T03.ZONE_ID, 5, '0')),'') AS CHAR(5))||CAST(COALESCE((lpad(T03.BRCH_ID, 5, '0')),'') AS CHAR(5))   
    ,COALESCE(T03.STRU_NAME,'')                         
    ,CAST(COALESCE((lpad(T02.ZONE_ID, 5, '0')),'') AS CHAR(5))||CAST(COALESCE((lpad(T02.BRCH_ID, 5, '0')),'') AS CHAR(5))    
    ,COALESCE(T02.STRU_NAME,'')                         
     ,CAST('30001231' AS DATE)          
     ,'C00_ICBC_INT_ORG_OVERVIEW_H|ED2_PBM_C_BRANCH'                        
FROM EDIODS.ED2_PBM_C_BRANCH      T01                                
LEFT JOIN EDIODS.ED2_PBM_C_BRANCH T02                                
ON
     T01.STRU_ID=T02.PARENT_STRU_ID 
     AND T02.STRU_LEVEL='2' 
     AND T02.STRU_STATUS='1'
     AND substr(T02.SPEC_MASK,1,1)=1
LEFT JOIN EDIODS.ED2_PBM_C_BRANCH T03                                 
ON
     T02.STRU_ID=T03.PARENT_STRU_ID 
     AND T03.STRU_LEVEL='3' 
     AND T03.STRU_STATUS='1'
     AND substr(T03.SPEC_MASK,1,1)=1
LEFT JOIN EDIODS.ED2_PBM_C_BRANCH T04                                
ON
     T03.STRU_ID=T04.PARENT_STRU_ID 
     AND T04.STRU_LEVEL='4' 
     AND T04.STRU_STATUS='1'
     AND substr(T04.SPEC_MASK,1,1)=1
LEFT JOIN EDIODS.ED2_PBM_C_BRANCH T05                                 
ON
     T04.STRU_ID=T05.PARENT_STRU_ID 
     AND T05.STRU_LEVEL='5' 
     AND T05.STRU_STATUS='1'
     AND substr(T05.SPEC_MASK,1,1)=1
LEFT JOIN EDIODS.ED2_PBM_C_STRU_NOVA_REL T06
ON   T05.STRU_ID = T06.STRU_ID
and  T06.SPEC_ID='1'
WHERE
     T01.STRU_LEVEL='1' 
     AND T01.STRU_STATUS='1'
     AND substr(T01.SPEC_MASK,1,1)=1
UNION
SELECT
     COALESCE(((lpad(T06.ZONE_ID, 5, '0'))||(lpad(T06.BRCH_ID, 5, '0'))),'') as Int_Org_Num
    ,CAST('19000101' AS DATE)
    ,COALESCE(T05.STRU_NAME,'')
    ,'004'                                         
    ,CAST(COALESCE((lpad(T04.ZONE_ID, 5, '0')),'') AS CHAR(5))||CAST(COALESCE((lpad(T04.BRCH_ID, 5, '0')),'') AS CHAR(5)) 
    ,COALESCE(T04.STRU_NAME,'')                          
    ,CAST(COALESCE((lpad(T02.ZONE_ID, 5, '0')),'') AS CHAR(5))||CAST(COALESCE((lpad(T02.BRCH_ID, 5, '0')),'') AS CHAR(5))   
    ,COALESCE(T02.STRU_NAME,'')                     
    ,CAST(COALESCE((lpad(T02.ZONE_ID, 5, '0')),'') AS CHAR(5))||CAST(COALESCE((lpad(T02.BRCH_ID, 5, '0')),'') AS CHAR(5))
    ,COALESCE(T02.STRU_NAME,'')               
    ,CAST('30001231' AS DATE)        
    ,'C00_ICBC_INT_ORG_OVERVIEW_H|ED2_PBM_C_BRANCH'                       
FROM EDIODS.ED2_PBM_C_BRANCH       T01                     
LEFT JOIN EDIODS.ED2_PBM_C_BRANCH T02                      
ON
     T01.STRU_ID=T02.PARENT_STRU_ID 
     AND T02.STRU_LEVEL='9' 
     AND T02.STRU_STATUS='1'
     AND substr(T02.SPEC_MASK,1,1)=1
LEFT JOIN EDIODS.ED2_PBM_C_BRANCH T04                               
ON
     T02.STRU_ID=T04.PARENT_STRU_ID 
     AND T04.STRU_LEVEL='4' 
     AND T04.STRU_STATUS='1'
     AND substr(T04.SPEC_MASK,1,1)=1
LEFT JOIN EDIODS.ED2_PBM_C_BRANCH T05                       
ON
     T04.STRU_ID=T05.PARENT_STRU_ID 
     AND T05.STRU_LEVEL='5' 
     AND T05.STRU_STATUS='1'
     AND substr(T04.SPEC_MASK,1,1)=1
LEFT JOIN EDIODS.ED2_PBM_C_STRU_NOVA_REL T06
ON   T05.STRU_ID = T06.STRU_ID
and  T06.SPEC_ID='1'
WHERE
     T01.STRU_LEVEL='1' 
     AND T01.STRU_STATUS='1'
     AND substr(T01.SPEC_MASK,1,1)=1
;