load data 
characterset "AL32UTF8" 
truncate into table gs_loader_issue_permission 
fields terminated by "," 
when(2:2) = ','  
( id , name , con )
