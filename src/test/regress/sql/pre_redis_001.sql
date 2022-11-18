declare 
    star_tsw timestamp without time zone := '2012-08-21 12:11:50';
    star_date date  := '1999-01-21';
    rownums integer := 168;
begin
    for i in 1..rownums loop
        insert into redistable.redis_table_0000 values(i,i-35,i+10,i*3,i%2,cast(''||i+4090||' '||i||' '||i-4090||'' as int2vector),cast(''||i-21||' '||i*2||' '||i+21||' '||i*21||'' as oidvector),'=+:-O'||i||'','asdf'||i*322||'',' DasA?'||i*112||'',' *|D] '||i*22||'',cast('mpp'||i||'' as bytea),'asdf'||i*98||'','10.'||i+2||'','23.'||i+5||'','1024.'||i+16||'',star_date-i,cast(''||(i%4096)||'' as reltime),star_date-666+i,(select timestamp 'epoch' +  (i*2121) * interval '11 second')::time,i+star_tsw,star_date+i,cast(''||i+4090||'.4096' as interval),(select timestamp with time zone 'epoch' +  (i*1024) * interval '21 second')::timetz,cast('('||i+4090||','||i||'),('||i-4090||','||i+1024||')' as box),cast(''||i+4090||'.'||i*21||'' as money),cast(''||i||' mpp aDB'||i+1024||' vrspcb' as tsvector));
    end loop;    
end;
/