load data 
INFILE * "str X'0A'"
characterset "AL32UTF8" 
truncate into table sqlldr_issue_hide_passwd
fields terminated by "," 
when(2:2) = ','  
( id , name , con )
