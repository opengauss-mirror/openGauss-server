load data 

truncate into table SQLLDR_COL_001 
fields terminated by "," 

( id constant "", 
name constant abc, 
TIMESTAMP CHAR "TRIM(:TIMESTAMP)")
