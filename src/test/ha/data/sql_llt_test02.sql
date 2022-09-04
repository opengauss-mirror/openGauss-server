EXPLAIN(costs off)
INSERT INTO EDISUM.C03_CORP_LOAN_AGT(
   DATA_DT                
  ,ORGANNO                
  ,LOANNO                 
  ,LOANSQNO               
  ,AGACCNO                
  ,LOAN_CONTR_NUM         
  ,CINO                   
   ,STATUS                
  ,BALANCE                
  ,OVRBALANCE             
  ,CURRTYPE               
  ,SUBCODE                
  ,BUSI_KIND              
  ,LOAN_MODE              
  ,LOAN_MODE_DTL          
  ,LOAN_DIR               
  ,LEVEL12_CLASS_CD       
  ,CONTR_LEVEL5_CLASS_CD  
  ,GUAR_COEF              
  ,VALUEDAY               
  ,MATUDATE               
  ,EXHMATUD               
  ,EXECRATE               
  ,RATE                   
  ,RATEFLTT               
  ,FLOATRATE              
  ,OVREXECRATE            
  ,OVRRATE                
  ,FINRFLTT               
  ,OVRFLOATRATE           
  ,CUST_BREACH_PROB       
    ,DEBT_BREACH_LOSSRATE 
)
SELECT
    date_11, ORGANNO, LOANNO, LOANSQNO, AGACCNO, TA200261002_11, TA200261001_11, STATUS_11, LSTBAL_11
    ,LSTOVRBAL_11,CURRTYPE_11,SUBCODE_11,TA200251009_11,TA200251011_11                          
    ,TA200251012_11,TA200268004_11,TA200261037_11,TA200261038_11,ASSURE_COEFF_11,VALUEDAY_11,MATUDATE_11,EXHMATUD_11                                
     ,COALESCE(CASE WHEN RATEFLTT1 = 9 
                    THEN RATE1 + FLOATRATE1
                    WHEN RATEFLTT1 = 0 
                    THEN CAST(RATE1 AS DECIMAL(30,6))*(1+FLOATRATE1/100.00000000)
                    ELSE RATE1
               END,0) AS EXECRATE1                     
      ,RATE1, RATEFLTT1, FLOATRATE1                     
     ,COALESCE(CASE WHEN FINRFLTT1 = 9 
                    THEN OVRRATE1 + FINFRATE1
                    WHEN FINRFLTT1 = 0 
                    THEN CAST(OVRRATE1 AS DECIMAL(30,6))*(1+FINFRATE1/100.00000000)
                    ELSE OVRRATE1
               END,0) AS OVREXECRATE1                   
      ,OVRRATE1, FINRFLTT1, FINFRATE1, co1_11, co2_11  
FROM
(
SELECT
    CAST('20140501' AS DATE)  as date_11             
    ,T1.ORGANNO   as   ORGANNO                                        
    ,T1.LOANNO    as    LOANNO                                        
    ,T1.LOANSQNO as     LOANSQNO                                      
    ,T1.AGACCNO      as   AGACCNO                                     
    ,COALESCE(T4.TA200261002,'')   as TA200261002_11                  
    ,COALESCE(T4.TA200261001,'')  as  TA200261001_11                  
    ,T1.STATUS  as   STATUS_11                                        
    ,CASE WHEN (T1.LSTTRAND > CAST('20140501' AS DATE) OR 
                  T1.LSTCINTD > CAST('20140501' AS DATE ))
            THEN T1.LSTBAL / 100.00 
            ELSE T1.BALANCE / 100.00 
       END     as   LSTBAL_11                                         
      ,CASE WHEN (T1.LSTTRAND > CAST('20140501' AS DATE) OR 
                  T1.LSTCINTD > CAST('20140501' AS DATE))
            THEN T1.LSTOVRBAL / 100.00 
            ELSE T1.OVRBALANCE / 100.00 
       END   as   LSTOVRBAL_11                                        
      ,T1.CURRTYPE  as    CURRTYPE_11                                 
      ,lpad(T1.SUBCODE, 6, ' ') as  SUBCODE_11                        
      ,COALESCE(T5.TA200251009,'') as  TA200251009_11                 
      ,COALESCE(T5.TA200251011,'')  as  TA200251011_11                
      ,COALESCE(T5.TA200251012,'')  as  TA200251012_11                
      ,COALESCE(T6.TA200268004,'')  as  TA200268004_11                
      ,COALESCE(T4.TA200261037,'')  as  TA200261037_11                
      ,COALESCE(T4.TA200261038,'')  as TA200261038_11                 
      ,COALESCE(T7.ASSURE_COEFF,0)  as  ASSURE_COEFF_11               
      ,COALESCE(T2.VALUEDAY,CAST('19000102' AS DATE)) as   VALUEDAY_11
      ,COALESCE(T2.MATUDATE,CAST('19000102' AS DATE))  as  MATUDATE_11
    ,COALESCE(T2.EXHMATUD,CAST('19000102' AS DATE)) as EXHMATUD_11    
      ,COALESCE(T8.RATE,T81.RATE,0)/1000000.000000 AS RATE1   
      ,CASE                                    
            WHEN ABS(COALESCE(T8.FLOARATE,T81.FLOARATE)) >= 900000000 THEN
            '9'
            ELSE
            '0'
       END AS RATEFLTT1                                     
      ,COALESCE(CASE
            WHEN ABS(COALESCE(T8.FLOARATE,T81.FLOARATE)) >= 900000000 THEN
            COALESCE(T8.FLOARATE,T81.FLOARATE) % 900000000
            ELSE
            COALESCE(T8.FLOARATE,T81.FLOARATE)
       END,0)/1000000.000000 AS FLOATRATE1                     
      ,COALESCE(T8.OVRRATE,0)/1000000.000000 AS OVRRATE1       
      ,CASE                                    
            WHEN ABS(T8.FINFRATE) >= 900000000 THEN
            '9'
            ELSE
            '0'
       END AS FINRFLTT1                                        
      ,COALESCE(CASE 
            WHEN ABS(T8.FINFRATE) >= 900000000 THEN
            T8.FINFRATE % 900000000
            ELSE
            T8.FINFRATE
       END,0)/1000000.000000 AS FINFRATE1                      
      ,CASE WHEN (T13.TA351307005 IS NOT NULL AND T17.TA351307005 IS NOT NULL)
            THEN (CASE WHEN T13.Etl_Tx_Dt >= T17.Etl_Tx_Dt
                       THEN T13.TA351307005
                       ELSE T17.TA351307005
                  END)     
            ELSE COALESCE(T13.TA351307005,T17.TA351307005,0)             
       END       as  co1_11                                             
      ,CASE WHEN (T13.TA351307010 IS NOT NULL AND T17.TA351307010 IS NOT NULL)
            THEN (CASE WHEN T13.Etl_Tx_Dt >= T17.Etl_Tx_Dt
                       THEN T13.TA351307010
                       ELSE T17.TA351307010
                  END)     
            ELSE COALESCE(T13.TA351307010,T17.TA351307010,-1)             
       END    as co2_11                                 
FROM  EDIODS.MF1_NTHLNSUB T1                            
LEFT  JOIN EDIODS.MF1_NTHLNCON T2                       
   ON T1.AGACCNO = T2.AGACCNO                           
  AND T1.LOANNO = T2.LOANNO                             
  AND T1.LOANSQNO = T2.LOANSQNO                         
LEFT  JOIN EDIODS.CCM_TA200261 T4                       
   ON T1.AGACCNO = SUBSTR(T4.TA200261004,1,17)          
  AND T1.LOANNO = T4.TA200261021                        
  AND T1.LOANSQNO = T4.TA200261028                      
  --AND T4.TA200261009 NOT IN (SELECT OLD_SEQ
  --                             FROM EDIODS.CCM_CM_NEW_SEQ
  --                            WHERE SEQ_CODE = 'TA200261009' )
  AND NOT EXISTS (SELECT OLD_SEQ
                               FROM EDIODS.CCM_CM_NEW_SEQ SEQ
                              WHERE SEQ_CODE = 'TA200261009'
                              AND SEQ.OLD_SEQ = T4.TA200261009)
  AND T4.TA200261011 > 0  
LEFT  JOIN EDIODS.CCM_TA200251 T5                
   ON T4.TA200261001 = T5.TA200251001            
  AND T4.TA200261002 = T5.TA200251032            
LEFT  JOIN EDIODS.CCM_TA200268 T6                
   ON T4.TA200261001 = T6.TA200268001            
  AND T4.TA200261009 = T6.TA200268003            
LEFT JOIN (
					SELECT BUSI_BELONG_CINO
                  ,BUSI_NO
                  ,APP_NO
                  ,ASSURE_COEFF 
          FROM        
						(SELECT BUSI_BELONG_CINO
	                  ,BUSI_NO
	                  ,APP_NO
	                  ,ASSURE_COEFF 
	                  ,ROW_NUMBER() OVER (PARTITION BY BUSI_BELONG_CINO,BUSI_NO,APP_NO ORDER BY ASSURE_COEFF desc) AS QUA_ROW_NUM_1
	           FROM EDIODS.GCC_ACR_CREDIT_LIST
	           ) AA WHERE QUA_ROW_NUM_1 = 1 
          ) T7
  ON T4.TA200261001 = T7.BUSI_BELONG_CINO
 AND T4.TA200261009 = T7.BUSI_NO
 AND T7.APP_NO='F-CM2002'
LEFT JOIN EDIODS.EDW_NFLNRAT T8                        
  ON T1.AGACCNO = T8.ACCNO                             
 AND T1.LOANNO = T8.LOANNO                             
 AND T1.LOANSQNO = T8.LOANSQNO                         
LEFT JOIN EDISUM.C03_CORP_LOAN_OPENRATE T81            
  ON T1.AGACCNO = T81.AGACCNO                          
 AND T1.LOANNO = T81.LOANNO                            
 AND T1.LOANSQNO = T81.LOANSQNO                        
LEFT  JOIN PDLGD_OF_TA351307 T13                       
   ON T4.TA200261001 = T13.TA351307001                 
  AND T4.TA200261002 = T13.TA351307003                 
LEFT  JOIN PDLGD_OF_TA351307_NEW T17                   
   ON T4.TA200261001 = T17.TA351307001                 
  AND T4.TA200261002 = T17.TA351307003                 
LEFT  JOIN Provi_Amt_OF_TA490461 T16
   ON T4.TA200261001 = T16.TA490461001
  AND T4.TA200261009 = T16.TA490461003) AA
;