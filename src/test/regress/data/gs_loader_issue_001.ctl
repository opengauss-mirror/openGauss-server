load data 
characterset "AL32UTF8" 
truncate into table sqlldr_issue_001 
fields terminated by "," 
when(2:2) = ','  
( id , name , con )
