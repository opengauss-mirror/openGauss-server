OPTIONS(DIRECT=FALSE)
load data 
characterset "AL32UTF8" 
truncate into table sqlldr_issue_options 
fields terminated by "," 
when(2:2) = ','  
( id , name , con )
