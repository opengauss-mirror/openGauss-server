set plsql_show_all_error to on;
begin 
forall i in 1 .. v1.count save exceptions insert into tb values v1(i); 
end;
/

create procedure test_plsql_core()
is
begin
forall i in 1 .. v1.count save exceptions insert into tb values v1(i);
end;
/
