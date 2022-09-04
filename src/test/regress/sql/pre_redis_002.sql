declare
 col_num integer := 210;
 star_date date  := '2010-10-01';
begin
for i in 1..col_num loop
insert into redistable.redis_table_000 values(i,i-35,i+10000,'3.'||i+4096||'','21.'||i+10088||'',9999-i*16,1234+i,cast(''||i+4090||'.'||i*21||'' as money),'a~'||i*2||'','b!@#'||i+21||'','c$%'||656-i||'','turkey^&*'||i+4080||'',cast('mpp'||i||'' as bytea),(select timestamp without time zone 'epoch' +  (i*1024) * interval '31 second')::timestamp without time zone,(select timestamp with time zone 'epoch' +  (i*4096) * interval '61 second')::timestamp without time zone,i%2,cast(''||(i*45)%255||'.'||i%198||'.'||(i*45)%255||'.'||(i*102)%255||'/32' as cidr),cast('2001:'||i%99||'f8:'||i%155+1||':2d4:fe'||(i*45)%25+2||':23f:a'||i%55+3||':'||(i*145)%125+4||'/128' as inet),cast(''||i%12+1||':'||i%12+34||':'||i%12+56||':'||i%78||':'||i%90+3||':ab' as macaddr),(i+4087)::bit(20),(i+1000)::bit(20),i+10,'int8and','to_timestamp(double precision)','*(integer,integer)','pg_type','integer','abc'||i+190||'',cast(''||i+4090||'.4096' as interval),star_date-666+i,(select timestamp 'epoch' +  (i*2121) * interval '11 second')::time without time zone,(select timestamp with time zone 'epoch' +  (i*1024) * interval '21 second')::time with time zone,i+22,'101.'||i+2048||'',''||i+4093||'.'||i+5401||'',''||i+1013||'.'||i+1687||'',cast('e'||i||'' as raw),'gau/:'||i+6754||'');
end loop;   
end; 
/