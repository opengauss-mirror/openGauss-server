insert into Serial_Table_1(C2) VALUES(generate_series(1,2000));
select nextval('SEQ_LLT_1');
select nextval('SEQ_LLT_1');
select setval('SEQ_LLT_1',100);
select nextval('SEQ_LLT_CACHE500');
select nextval('SEQ_LLT_CACHE500');
select setval('SEQ_LLT_CACHE500',100);
