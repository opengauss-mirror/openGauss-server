BADFILE '/u02/tansfer/bad/athorpad.bad'
load data 
characterset "AL32UTF8" 
truncate into table sqlldr_issue_badfile 
fields terminated by "," 
when(2:2) = ','  
( id , name , con )
