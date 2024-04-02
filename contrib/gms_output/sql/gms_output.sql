create extension gms_output;
create schema gms_output_test;
set search_path=gms_output_test;

-- test gms_output.disable
select gms_output.DISABLE;
select gms_output.DISABLE(null);
select gms_output.DISABLE();
select gms_output.DISABLE('');
select gms_output.DISABLE(0);
select gms_output.DISABLE("""");
select gms_output.DISABLE(@);
select gms_output.DISABLE(あ);
select gms_output.DISABLE(%);
select gms_output.DISABLE('qqqq');
select gms_output.DISABLE;
select gms_output.PUT_LINE('adsfds');
select gms_output.GET_LINE(0,1);

-- test gms_output.enable
select gms_output.enable(1000000);
select gms_output.enable(20000);
select gms_output.enable(1000001);
select gms_output.enable(19999);
select gms_output.enable(30000);
select gms_output.enable(power(2,31));
select gms_output.enable(-555.55);
select gms_output.enable(0.555.55);
select gms_output.enable(000.555);
select gms_output.enable(ssss);
select gms_output.enable('ssss');
select gms_output.enable(-power(2,32));
select gms_output.enable(-power(2,21));
select gms_output.enable(a);
select gms_output.enable('a');
select gms_output.enable(' ');
select gms_output.enable(null);
select gms_output.enable(0);
select gms_output.enable(あ);
select gms_output.enable('あ');
select gms_output.enable('@');
select gms_output.enable(1000,1000);
select gms_output.enable(1000,null);
select gms_output.enable('null');
select gms_output.enable('山东东海偶然打赏');
select gms_output.enable(0);

create table t(a int, b int, c int);
insert into t values(111111,222222,333333);
insert into t values(444444,555555,666666);
insert into t values(777777,888888,999999);

create table tt(aa int,bb varchar2(100));
declare
msg varchar2(120);
cursor t_cur is select * from t order by a;
v_line varchar2(100);
 v_status integer := 0;
begin
 gms_output.enable;
for i in t_cur loop
 msg := i.a || ',' || i.b || ',' || i.c;
 gms_output.put_line(msg);
end loop;

 gms_output.get_line(v_line, v_status);
 while v_status = 0 loop
 insert into tt values(v_status, v_line);
 gms_output.get_line(v_line, v_status);
end loop;
end;
/
--查看结果
select * from tt;

--清理资源
drop table t;
drop table tt;

-- test gms_output.get_line
begin
  gms_output.ENABLE(100);
  gms_output.put('This '); 
end;
/
select gms_output.GET_LINE();

begin
  gms_output.ENABLE(100);
  gms_output.put();  
end;
/
select gms_output.GET_LINE();

begin
  gms_output.ENABLE(100);
  gms_output.put(null); 
end;
/
select gms_output.GET_LINE();

begin
  gms_output.ENABLE(100);
  gms_output.put(null); 
end;
/
select gms_output.GET_LINE();

begin
  gms_output.ENABLE(100);
  gms_output.put(-999888,pppp); 
end;
/
select gms_output.GET_LINE();

begin
  gms_output.ENABLE(100);
  gms_output.put(123213123); 
end;
/
select gms_output.GET_LINE();

begin
  gms_output.ENABLE(100);
  gms_output.put('未来更快更高更强,hsdufdsnjfs,shfusdfjfjs,12313213');
end;
/
select gms_output.GET_LINE();

begin
  gms_output.ENABLE(100);
  gms_output.put('未来更快更高更强');
end;
/
select gms_output.GET_LINE();

begin
  gms_output.ENABLE(100);
  gms_output.put('@#￥#@@#！@#！#￥@%%');
end;
/
select gms_output.GET_LINE(); 

begin
  gms_output.ENABLE(100);
  gms_output.put('2017-09-23'); 
end;
/
select gms_output.GET_LINE();
 
begin
  gms_output.ENABLE(100);
  gms_output.put('2017-09-23 12:23:29,2000/07/99'); 
end;
/
select gms_output.GET_LINE();

begin
  gms_output.ENABLE(100);
  gms_output.put('Time Started: ' || TO_CHAR('2001-09-28 01:00:00'::timestamp, 'DD-MON-YYYY HH24:MI:SS'));
end;
/
select gms_output.GET_LINE();

begin
  gms_output.ENABLE(100);
  gms_output.put('Time Started: ' || TO_CHAR('2001-09-28 01:00:00'::timestamp, 'DD-MON-YYYY HH24:MI:SS'));
end;
/

begin
  gms_output.ENABLE(100);
  gms_output.put('∵∴∷♂♀°℃＄¤￠￡‰§№☆*〇○*◎** 回□*△▽⊿▲▼▁▂▃▄▆**▉▊▋▌▍▎▏※→←↑↓↖↗↘↙** ⅰⅱⅲⅳⅴⅵⅶⅷⅸⅹ①②③④⑤⑥⑦⑧⑨⑩⒈⒉⒊⒋ ⒌⒍⒎⒏⒐⒑⒒⒓⒔⒕⒖⒗⒘⒙⒚⒛⑴⑵⑶⑷⑸⑹⑺⑻⑼⑽⑾⑿⒀⒁⒂⒃⒄⒅⒆⒇㈠㈡㈢㈣㈤㈥㈦㈧㈨㈩ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩⅪⅫぁあぃいぅうぇえぉおかがきぎくぐけげこごさざしじすずせぜそぞただちぢっつづてでとどなにぬねのはばぱひびぴふぶぷへべぺほぼぽまみむめもゃやゅゆょよらりるれろゎわゐゑをんァアィイゥウェエォオカガキギクグケゲコゴサザシジスズセゼソゾタダチヂッツヅテデトドナニヌネノハバパヒビピフブプヘベペホボポマミムメモャヤュユョヨラリルレロヮワヰヱヲンヴヵヶΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΤΥΦΧΨΩαβγδεζηθ ικλμνξοπρστυφχψ ω︵︶︹︺︿﹀︽︾﹁﹂﹃﹄︻');
end;
/
select gms_output.GET_LINE();

begin
  gms_output.ENABLE(100);
  gms_output.put('This '); 
  gms_output.put('is ');
  gms_output.new_line();  
end;
/
select gms_output.GET_LINE();

begin
  gms_output.ENABLE(100);
  gms_output.put('This '); 
  gms_output.put('is ');
  gms_output.put('函数测试历史的反思的');
end;
/
select gms_output.GET_LINE();

select gms_output.GET_LINE(power(2,32));
select  gms_output.GET_LINE(power(2,9999));
select  gms_output.GET_LINE(あ@ 、iohgfvcs大富大贵);
select gms_output.GET_LINE();
select gms_output.GET_LINE('');
select gms_output.GET_LINE(null);
select gms_output.GET_LINE('aaaa');
select gms_output.GET_LINE('和士大夫大师傅即使对方');
select gms_output.GET_LINE(111);
select gms_output.GET_LINE(0);

SET search_path to gms_output;
create table t(a int, b int, c int);
create table tt(aa int,bb varchar2(100));
insert into t values(111111,222222,333333);
insert into t values(444444,555555,666666);
insert into t values(777777,888888,999999);

declare
msg varchar2(120);
cursor t_cur is select * from t order by a;
v_line varchar2(100);
v_status integer := 0;
begin
gms_output.disable();
gms_output.enable;
for i in t_cur loop
msg := i.a || ',' || i.b || ',' || i.c;
raise notice 'msg=%',msg;
gms_output.put_line(msg);
end loop;

gms_output.get_line(v_line, v_status);
while v_status = 0 loop
raise notice 'v_line=%,v_status=%',v_line,v_status;
insert into tt values(v_status, v_line);
gms_output.get_line(v_line, v_status);
end loop;
end;
/
select * from tt;

delete from t;
delete from tt;

declare
v_line varchar2(100);
v_status integer := 0;
begin
gms_output.enable;
gms_output.put('This ');
gms_output.new_line;
gms_output.put('end ');

gms_output.get_line(v_line, v_status);
while v_status = 0 loop
raise notice 'v_line=%,v_status=%',v_line,v_status;
insert into tt values(v_status, v_line);
gms_output.get_line(v_line, v_status);
end loop;
end;
/
select * from tt;

-- test gms_output.get_lines
begin
 gms_output.enable(100);
 gms_output.PUT_LINE('{131231321312313},{dhsfsdjfsdf}');
 gms_output.PUT_LINE('{好还是打发士大夫},{dhsfsdjfsdf}');
end;
/

select  gms_output.get_lines('{lines}',3);

begin
 gms_output.enable(100);
 gms_output.PUT_LINE('{131231321312313,wrewerwr,werwerw},{dhsfsdjfsdf}');
 gms_output.PUT_LINE('88无任何为维护差旅费{dhsfsdjfsdf}');
 gms_output.PUT_LINE('u威力五二年初的v下');
end;
/

select  gms_output.get_lines('{lines}',3);

begin
 gms_output.enable(100);
 gms_output.PUT_LINE('[131231321312313,wrewerwr,werwerw},{dhsfsdjfsdf]');
 gms_output.PUT_LINE('∵∴∷♂♀°℃＄¤￠￡‰§№☆*〇○*◎** 回□*△▽⊿▲▼▁▂▃▄▆**▉▊▋▌▍▎▏※→←↑↓↖↗↘↙** ⅰⅱⅲⅳⅴⅵⅶⅷⅸⅹ①②③④⑤⑥⑦⑧⑨⑩⒈⒉⒊⒋ ⒌⒍⒎⒏⒐⒑⒒⒓⒔⒕⒖⒗⒘⒙⒚⒛⑴⑵⑶⑷⑸⑹⑺⑻⑼⑽⑾⑿⒀⒁⒂⒃⒄⒅⒆⒇㈠㈡㈢㈣㈤㈥㈦㈧㈨㈩ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩⅪⅫぁあぃいぅうぇえぉおかがきぎくぐけげこごさざしじすずせぜそぞただちぢっつづてでとどなにぬねのはばぱひびぴふぶぷへべぺほぼぽまみむめもゃやゅゆょよらりるれろゎわゐゑをんァアィイゥウェエォオカガキギクグケゲコゴサザシジスズセゼソゾタダチヂッツヅテデトドナニヌネノハバパヒビピフブプヘベペホボポマミムメモャヤュユョヨラリルレロヮワヰヱヲンヴヵヶΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΤΥΦΧΨΩαβγδεζηθ ικλμνξοπρστυφχψ ω︵︶︹︺︿﹀︽︾﹁﹂﹃﹄︻');
end;
/
select  gms_output.get_lines('{lines}',3);
begin
 gms_output.enable(100);
 gms_output.PUT_LINE('{[131231321312313,wrewerwr,werwerw]},{dhsfsdjfsdf]},{美丽泸沽湖}');
end;
/
select  gms_output.get_lines('{lines}',3);

--设置v_data为int形式
declare
v_data text[];
  v_numlines number;
begin
  gms_output.enable(100);
  gms_output.put_line('好好学习');
  gms_output.put_line('天天向上');
  gms_output.put_line('oh ye');
  v_numlines:=3;
  gms_output.get_lines(v_data,v_numlines);
for v_counter in 1..v_numlines loop
  gms_output.put_line(v_data(v_counter));
end loop;
end;
/
select  gms_output.get_lines('{lines}',3);

select  gms_output.get_lines(3);
select  gms_output.get_lines('aaaa');
select  gms_output.get_lines();
select  gms_output.get_lines(0);
select  gms_output.get_lines(-1);
select  gms_output.get_lines(-1.666);
select  gms_output.get_lines('-1.666');
select  gms_output.get_lines(null);
select  gms_output.get_lines({123432});
select  gms_output.get_lines('{123432}');
select  gms_output.get_lines(3);
select  gms_output.get_lines('a');
select  gms_output.get_lines(null);
select  gms_output.get_lines(power(2,31));
select  gms_output.get_lines(3,99);
select  gms_output.get_lines();
select gms_output.GET_LINES('{hhh}',1);
select  gms_output.get_lines('{123432}',3);
select  gms_output.get_lines('{123432}','{12312312}');
select  gms_output.get_lines('{入户为了未来}',1);
select  gms_output.get_lines('{入户为了未来}','1');
select  gms_output.get_lines('{入户为了未来}','hhh');
select  gms_output.get_lines('{}','');
select  gms_output.get_lines(null,null);
select  gms_output.get_lines('','');
select  gms_output.get_lines(0,0);


--#61857668 [Oracle - gms_output.get_lines] Not support gms_output.chararr type
SET search_path to gms_output;
create table tt(aa int,bb varchar2(100));
declare
lines gms_output.chararr;
   v_numlines number;
begin
  gms_output.put_line('line one');
  gms_output.put_line('line two');
  gms_output.new_line();
  gms_output.put_line('line three');
  v_numlines := 4;
  gms_output.get_lines(lines, v_numlines);
for v_counter in 1..v_numlines loop
       insert into tt values(v_counter, lines(v_counter));
       gms_output.put_line(lines(v_counter));
end loop;
end;
/

--查看结果
select * from tt;


DECLARE
lines GMSOUTPUT_LINESARRAY;
   v_numlines number;
begin
  --gms_output.put('This ');
  --gms_output.put('is ');
  gms_output.put_line('line one');
  gms_output.put_line('line two');
  gms_output.new_line();
  --gms_output.put('end.');
  gms_output.put_line('line three');
  v_numlines := 4;
  gms_output.get_lines(lines, v_numlines);
for v_counter in 1..v_numlines loop
       insert into tt values(v_counter, lines(v_counter));
       gms_output.put_line(lines(v_counter));
end loop;
end;
/

--查看结果
select * from tt;

--#61857707 [Oracle - gms_output.get_lines] Accept with only one parameter
select gms_output.put_line('line one');
select gms_output.put_line('line two');
select gms_output.new_line();
select gms_output.put_line('line three');
select gms_output.get_lines(4);
select gms_output.get_lines('{0}',4);


--执行存储过程
declare
lines varchar[];
  v_numlines number;
begin
  --gms_output.put('This ');
  --gms_output.put('is ');
    gms_output.put_line('line one');
    gms_output.put_line('line two');
    gms_output.new_line();
  --gms_output.put('end.');
    gms_output.put_line('line three');
    v_numlines := 4;
    gms_output.get_lines(4);
for v_counter in 1..v_numlines loop
    insert into tt values(v_counter, lines(v_counter));
    gms_output.put_line(lines(v_counter));
end loop;
end;
/

--查看结果
select * from tt;

--清理资源
drop table tt;

-- test gms_output.new_line
select gms_output.put('This '); 
select gms_output.new_line;  
select gms_output.new_line();
select gms_output.new_line(null);  
select gms_output.new_line('');  
select gms_output.new_line('hhh');
select gms_output.new_line('""where,where"". :???????');
select gms_output.new_line(systime);
select gms_output.new_line(8888);
select gms_output.new_line('Time Started: ' || TO_CHAR('2001-09-28 01:00:00'::timestamp, 'DD-MON-YYYY HH24:MI:SS'));
select gms_output.new_line（88889）;
begin
  gms_output.ENABLE(100);
  gms_output.put('This '); 
  gms_output.put('is ');
  gms_output.new_line();  
end;
/
select gms_output.GET_LINE();

-- test gms_output.put
select gms_output.put('This '); 
select gms_output.put(); 
select gms_output.put(null); 
select gms_output.put(''); 
select gms_output.put(-999888,pppp); 
select gms_output.put(1,21);
select gms_output.put(123213123); 
select gms_output.put('未来更快更高更强','hsdufdsnjfs','shfusdfjfjs',12313213);
select gms_output.put('@#￥#@@#！@#！#￥@%%'); 
select gms_output.put(2017-09-23); 
select gms_output.put(2017-09-23 12:23:29); 
select gms_output.put(89%);
select gms_output.put(’あ@ ‘);
select gms_output.put(2000/07/99);
select gms_output.put(power(2,32));
select gms_output.put(power(2,9999));
select gms_output.put('Time Started: ' || TO_CHAR('2001-09-28 01:00:00'::timestamp, 'DD-MON-YYYY HH24:MI:SS'));
select gms_output.put(あ@ 、iohgfvcs大富大贵);
select gms_output.put('、。·ˉˇ¨〃々—～‖…〔〕〈〉《》「」『』〖〗【】±＋－×÷∧∨∑∏∪∩∈√⊥∥∠⌒⊙∫∮≡≌≈∽∝≠≮≯≤≥∞∶ ∵∴∷♂♀°℃＄¤￠￡‰§№☆*〇○*◎** 回□*△▽⊿▲▼▁▂▃▄▆**▉▊▋▌▍▎▏※→←↑↓↖↗↘↙** ⅰⅱⅲⅳⅴⅵⅶⅷⅸⅹ①②③④⑤⑥⑦⑧⑨⑩⒈⒉⒊⒋ ⒌⒍⒎⒏⒐⒑⒒⒓⒔⒕⒖⒗⒘⒙⒚⒛⑴⑵⑶⑷⑸⑹⑺⑻⑼⑽⑾⑿⒀⒁⒂⒃⒄⒅⒆⒇㈠㈡㈢㈣㈤㈥㈦㈧㈨㈩ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩⅪⅫ！＃￥％＆（）＊＋，－．／０１２３４５６７８９：；＜＝＞？＠ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ［＼］＾＿｀ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ｛｜｝ぁあぃいぅうぇえぉおかがきぎくぐけげこごさざしじすずせぜそぞただちぢっつづてでとどなにぬねのはばぱひびぴふぶぷへべぺほぼぽまみむめもゃやゅゆょよらりるれろゎわゐゑをんァアィイゥウェエォオカガキギクグケゲコゴサザシジスズセゼソゾタダチヂッツヅテデトドナニヌネノハバパヒビピフブプヘベペホボポマミムメモャヤュユョヨラリルレロヮワヰヱヲンヴヵヶΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΤΥΦΧΨΩαβγδεζηθ ικλμνξοπρστυφχψ ω︵︶︹︺︿﹀︽︾﹁﹂﹃﹄︻');
select gms_output.put('123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890
');
select gms_output.put('** ⅰⅱⅲⅳⅴⅵⅶⅷⅸⅹ①②③④⑤⑥⑦⑧⑨⑩⒈⒉⒊⒋ ⒌⒍⒎⒏⒐⒑⒒⒓⒔⒕⒖⒗⒘⒙⒚⒛⑴⑵⑶⑷⑸⑹⑺⑻⑼⑽⑾⑿⒀⒁⒂⒃⒄⒅⒆⒇㈠㈡㈢㈣㈤㈥㈦㈧㈨㈩ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩⅪⅫ！＃￥％＆（）＊＋，－．／０１２３４５６７８９：；＜＝＞？＠ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ［＼］＾＿｀ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ｛｜｝ぁあぃいぅうぇえぉおかがきぎくぐけげこごさざしじすずせぜそぞただちぢっつづてでとどなにぬねのはばぱひびぴふぶぷへべぺほぼぽまみむめもゃやゅゆょよらりるれろゎわゐゑをんァアィイゥウェエォオカガキギクグケゲコゴサザシジスズセゼソゾタダチヂッツヅテデトドナニヌネノハバパヒビピフブプヘベペホボポマミムメモャヤュユョヨラリルレロヮワヰヱヲンヴヵヶΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΤΥΦΧΨΩαβγδεζηθ ικλμνξοπρστυφχψ ω︵︶︹︺︿﹀︽︾﹁﹂﹃﹄︻');


create table tt(aa int,bb varchar2(100));
declare
   v_line varchar2(100);
   v_status integer := 0;
begin
  gms_output.enable(100);
  gms_output.put('This ');
  gms_output.put('is ');
  gms_output.new_line;
  gms_output.put('end.');
  gms_output.get_line(v_line, v_status);
  while v_status = 0 loop
       insert into tt values(v_status, v_line);
       gms_output.get_line(v_line, v_status);
   end loop;
end;
/

--清理资源
drop table tt;

-- test gms_output.put_line
select gms_output.PUT_LINE('......');
select gms_output.PUT_LINE(null);
select gms_output.PUT_LINE(131231321312313);
select gms_output.PUT_LINE('akfdskfsf??Oooo&&&^%**(');
select gms_output.PUT_LINE('');
select gms_output.PUT_LINE(89239432094%%&&*&&**&^);
select gms_output.PUT_LINE(-0.99923332);
select gms_output.PUT_LINE(power(2,31));
select gms_output.PUT_LINE(max(2,3));
select gms_output.PUT_LINE('2001-09-28 01:00:00'::timestamp);
select gms_output.PUT_LINE(0);
select gms_output.PUT_LINE('adsfds');
select gms_output.PUT_LINE(adsfds);
select gms_output.PUT_LINE(power(2,9999));
select gms_output.PUT_LINE（power(2,9999)）;
select gms_output.PUT_LINE(131231321312313,24232432);
select gms_output.PUT_LINE('疫情早日过去，国泰民安，风调雨顺，平安健康常在');
select gms_output.PUT_LINE('123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890
');
select gms_output.PUT_LINE('** ⅰⅱⅲⅳⅴⅵⅶⅷⅸⅹ①②③④⑤⑥⑦⑧⑨⑩⒈⒉⒊⒋ ⒌⒍⒎⒏⒐⒑⒒⒓⒔⒕⒖⒗⒘⒙⒚⒛⑴⑵⑶⑷⑸⑹⑺⑻⑼⑽⑾⑿⒀⒁⒂⒃⒄⒅⒆⒇㈠㈡㈢㈣㈤㈥㈦㈧㈨㈩ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩⅪⅫ！＃￥％＆（）＊＋，－．／０１２３４５６７８９：；＜＝＞？＠ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ［＼］＾＿｀ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ｛｜｝ぁあぃいぅうぇえぉおかがきぎくぐけげこごさざしじすずせぜそぞただちぢっつづてでとどなにぬねのはばぱひびぴふぶぷへべぺほぼぽまみむめもゃやゅゆょよらりるれろゎわゐゑをんァアィイゥウェエォオカガキギクグケゲコゴサザシジスズセゼソゾタダチヂッツヅテデトドナニヌネノハバパヒビピフブプヘベペホボポマミムメモャヤュユョヨラリルレロヮワヰヱヲンヴヵヶΑΒΓΔΕΖΗΘΙΚΛΜΝΞΟΠΡΣΤΥΦΧΨΩαβγδεζηθ ικλμνξοπρστυφχψ ω︵︶︹︺︿﹀︽︾﹁﹂﹃﹄︻');

drop database if exists testdb_utf8;
create database testdb_utf8;
\c testdb_utf8;
create extension gms_output;
declare
    a int := 10;
    b varchar2(100) := 'abcdef';
    c text := '测试用例，，，,,,.. ..。。。。';
begin
    gms_output.enable(4000);
    gms_output.put_line('test case');
    gms_output.put_line('test case' || a);
    gms_output.put_line('test case' || b);
    gms_output.put_line('test case' || c);
    gms_output.put_line('测试用例');
    gms_output.put_line('测试用例，，，,ABCD,,....。。。。');
    gms_output.put_line('测试用例，，，,ABCD,,....。。。。' || a);
    gms_output.put_line('测试用例，，，,ABCD,,....。。。。' || b);
    gms_output.put_line('测试用例，，，,ABCD,,....。。。。' || c);
end;
/

\c contrib_regression
drop database testdb_utf8;

drop database if exists testdb_gbk;
create database testdb_gbk encoding='GBK' LC_COLLATE='C' LC_CTYPE='C';
\c testdb_gbk
create extension gms_output;
set client_encoding to 'UTF8';
declare
    a int := 10;
    b varchar2(100) := 'abcdef';
    c text := '测试用例，，，,,,.. ..。。。。';
begin
    gms_output.enable(4000);
    gms_output.put_line('test case');
    gms_output.put_line('test case' || a);
    gms_output.put_line('test case' || b);
    gms_output.put_line('test case' || c);
    gms_output.put_line('测试用例');
    gms_output.put_line('测试用例，，，,ABCD,,....。。。。');
    gms_output.put_line('测试用例，，，,ABCD,,....。。。。' || a);
    gms_output.put_line('测试用例，，，,ABCD,,....。。。。' || b);
    gms_output.put_line('测试用例，，，,ABCD,,....。。。。' || c);
end;
/
reset client_encoding;
\c contrib_regression
drop database testdb_gbk;

-- test gms_output.serveroutput
set search_path=gms_output_test;
begin
gms_output.put('123');
gms_output.put_line('abc');
end;
/

begin
gms_output.enable(4000);
gms_output.put('123');
gms_output.new_line();
gms_output.put_line('abc');
end;
/

begin
gms_output.put('222');
gms_output.new_line();
end;
/

begin
gms_output.enable(4000);
gms_output.put_line('abc');
end;
/

begin
gms_output.disable();
gms_output.enable(4000);
gms_output.put('456');
gms_output.put('7');
gms_output.new_line();
gms_output.put('89');
gms_output.put_line('abc');
end;
/

begin
gms_output.disable();
gms_output.enable(4000);
gms_output.new_line();
gms_output.put('456');
gms_output.put('7');
gms_output.put('89');
gms_output.put_line('abc');
gms_output.put_line('fff');
gms_output.put_line('ggg');
end;
/

begin
gms_output.enable();
gms_output.put_l ine('abc');
end;
/

begin
gms_output.put('123');
gms_output.new_line();
end;
/

begin
gms_output.put('123');
end;
/

begin
gms_output.new_line();
end;
/


set gms_output.serveroutput to off;
show gms_output.serveroutput;

begin
gms_output.put_line('2');
end;
/

begin
gms_output.enable(4000);
gms_output.put_line('abc');
end;
/

set gms_output.serveroutput to on;
begin
gms_output.enable(4000);
gms_output.put('123');
gms_output.put_line('YYY');
end;
/


begin
gms_output.disable();
gms_output.enable(4000);
gms_output.put('33');
end;
/

begin
gms_output.put('44');
gms_output.new_line();
end;
/

create table gms_output_tt3(aa int,bb varchar2(100));
declare
v_line varchar2(100);
v_status integer := 0;
begin
gms_output.put_line('55');
gms_output.get_line(v_line, v_status);
insert into gms_output_tt3 values(v_status, v_line);
end;
/
select * from gms_output_tt3;


begin
  gms_output.ENABLE;
  gms_output.put_line('This ');
end;
/
select gms_output.GET_LINE(0,1);


begin
 gms_output.enable(100);
 gms_output.PUT_LINE('{131231321312313},{dhsfsdjfsdf}');
 gms_output.PUT_LINE('{好还是打发士大夫},{dhsfsdjfsdf}');
end;
/
select  gms_output.get_lines('{lines}',3);


reset search_path;
drop schema gms_output_test cascade;
