set enable_global_stats = true;

/*
 * This file is used to test the vector function of ExecStream()
 */
----
--- Create Table and Insert Data
----
create schema vector_stream_engine;
set current_schema = vector_stream_engine;

create table vector_stream_engine.VECTOR_STREAM_TABLE_01_00
(
   cint1	int1
  ,cint2	int1
) with (orientation = orc) tablespace hdfs_ts  distribute by hash(cint1);

insert into vector_stream_table_01_00 values (1,0);

create table vector_stream_engine.VECTOR_STREAM_TABLE_01_01
(
   cint1	int1
  ,cint2	int1
) with (orientation = orc) tablespace hdfs_ts  distribute by hash(cint1);

insert into vector_stream_table_01_01 values (0,1);

create table vector_stream_engine.VECTOR_STREAM_TABLE_02_00
(
	col_char	char(1)
   ,col_char2	char(1)
) with (orientation = orc) tablespace hdfs_ts  distribute by hash(col_char);

insert into vector_stream_table_02_00 values ('a','c');

create table vector_stream_engine.VECTOR_STREAM_TABLE_02_01
(
   col_char		char(1)
  ,col_char2	char(1)
) with (orientation = orc) tablespace hdfs_ts  distribute by hash(col_char);

insert into vector_stream_table_02_01 values ('z','a');

create table vector_stream_engine.VECTOR_STREAM_TABLE_03_00
(
   cint1	int2
  ,cint2	int2
) with (orientation = orc) tablespace hdfs_ts  distribute by hash(cint1);

insert into vector_stream_table_03_00 values (1,0);

create table vector_stream_engine.VECTOR_STREAM_TABLE_03_01
(
   cint1	int2
  ,cint2	int2
) with (orientation = orc) tablespace hdfs_ts  distribute by hash(cint1);

insert into vector_stream_table_03_01 values (0,1);

create table vector_stream_engine.VECTOR_STREAM_TABLE_04_00
(
   col_nvchar1	nvarchar2
  ,col_nvchar2	nvarchar2
) with (orientation = orc) tablespace hdfs_ts  distribute by hash(col_nvchar1);

insert into vector_stream_table_04_00 values ('ab','cde');

create table vector_stream_engine.VECTOR_STREAM_TABLE_04_01
(
   col_nvchar1	nvarchar2
  ,col_nvchar2	nvarchar2
) with (orientation = orc) tablespace hdfs_ts  distribute by hash(col_nvchar1);;

insert into vector_stream_table_04_01 values ('cde','ab');

create table vector_stream_engine.VECTOR_STREAM_TABLE_05_00
(
   col_date1	date
  ,col_date2	date
) with (orientation = orc) tablespace hdfs_ts  distribute by hash(col_date1);

insert into vector_stream_table_05_00 values ('1986-12-21','1987-12-21');

create table vector_stream_engine.VECTOR_STREAM_TABLE_05_01
(
   col_date1	date
  ,col_date2	date
) with (orientation = orc) tablespace hdfs_ts  distribute by hash(col_date1);

insert into vector_stream_table_05_01 values ('1987-12-21','1986-12-21');

create table vector_stream_engine.VECTOR_STREAM_TABLE_06_00
(
   col_time1	time
  ,col_time2	time
) with (orientation = orc) tablespace hdfs_ts  distribute by hash(col_time1);

insert into vector_stream_table_06_00 values ('08:00:02','12:00:02');

create table vector_stream_engine.VECTOR_STREAM_TABLE_06_01
(
   col_time1	time
  ,col_time2	time
) with (orientation = orc) tablespace hdfs_ts  distribute by hash(col_time1);

insert into vector_stream_table_06_01 values ('15:00:02','08:00:02');

create table vector_stream_engine.VECTOR_STREAM_TABLE_07_00
(
   col_times1	timestamptz
  ,col_times2	timestamptz
) with (orientation = orc) tablespace hdfs_ts  distribute by hash(col_times1);

insert into vector_stream_table_07_00 values ('2009-04-09 00:24:37+08','2009-09-09 00:26:37+08');

create table vector_stream_engine.VECTOR_STREAM_TABLE_07_01
(
   col_times1	timestamptz
  ,col_times2	timestamptz
) with (orientation = orc) tablespace hdfs_ts  distribute by hash(col_times1);

insert into vector_stream_table_07_01 values ('2002-02-08 00:24:36+08','2009-04-09 00:24:37+08');

create table vector_stream_engine.VECTOR_STREAM_TABLE_08_00
(
   col_interval1	interval
  ,col_interval2	interval
) with (orientation = orc) tablespace hdfs_ts  distribute by hash(col_interval1);

insert into vector_stream_table_08_00 values ('2 day 13:34:56','4 day 13:34:56');

create table vector_stream_engine.VECTOR_STREAM_TABLE_08_01
(
   col_interval1	interval
  ,col_interval2	interval
) with (orientation = orc) tablespace hdfs_ts  distribute by hash(col_interval1);

insert into vector_stream_table_08_01 values ('7 day 13:34:56','2 day 13:34:56');

create table vector_stream_engine.VECTOR_STREAM_TABLE_09_00
(
   col_timetz1	timetz
  ,col_timetz2	timetz
) with (orientation = orc) tablespace hdfs_ts  distribute by hash(col_timetz1);

insert into vector_stream_table_09_00 values ('01:00:30+08','02:00:30+08');

create table vector_stream_engine.VECTOR_STREAM_TABLE_09_01
(
   col_timetz1	timetz
  ,col_timetz2	timetz
) with (orientation = orc) tablespace hdfs_ts  distribute by hash(col_timetz1);

insert into vector_stream_table_09_01 values ('06:00:30+08','01:00:30+08');

create table vector_stream_engine.VECTOR_STREAM_TABLE_10_00
(
   col_sdt1	smalldatetime
  ,col_sdt2	smalldatetime
) with (orientation = orc) tablespace hdfs_ts  distribute by hash(col_sdt1);

insert into vector_stream_table_10_00 values ('1962-01-01','1972-01-12');

create table vector_stream_engine.VECTOR_STREAM_TABLE_10_01
(
   col_sdt1	smalldatetime
  ,col_sdt2	smalldatetime
) with (orientation = orc) tablespace hdfs_ts  distribute by hash(col_sdt1);

insert into vector_stream_table_10_01 values ('1965-01-01','1962-01-01');

----
--- test 1: Redistribution for different type
----
select cint1 from vector_stream_table_01_00 intersect all select cint2 from vector_stream_table_01_01;

select col_char from vector_stream_table_02_00 intersect all select col_char2 from vector_stream_table_02_01;

select cint1 from vector_stream_table_03_00 intersect all select cint2 from vector_stream_table_03_01;

select col_nvchar1 from vector_stream_table_04_00 intersect all select col_nvchar2 from vector_stream_table_04_01;

select col_date1 from vector_stream_table_05_00 intersect all select col_date2 from vector_stream_table_05_01;

select col_time1 from vector_stream_table_06_00 intersect all select col_time2 from vector_stream_table_06_01;

select col_times1 from vector_stream_table_07_00 intersect all select col_times2 from vector_stream_table_07_01;

select col_interval1 from vector_stream_table_08_00 intersect all select col_interval2 from vector_stream_table_08_01;

select col_timetz1 from vector_stream_table_09_00 intersect all select col_timetz2 from vector_stream_table_09_01;

select col_sdt1 from vector_stream_table_10_00 intersect all select col_sdt2 from vector_stream_table_10_01;


----
--- test: stream plan's subplan can't do projections, when distribute_keys is not in subplan's targetlists, need to add a result node to help it available.
----
CREATE TABLE fvt_distribute_query_base_tables_01 (
    w_name character(10),
    w_street_1 character varying(20),
    w_zip character(9),
    w_id integer
)
WITH (orientation = column, compression=low)
DISTRIBUTE BY HASH (w_id)
PARTITION BY RANGE (w_id)
(
    PARTITION fvt_distribute_query_base_tables_01_p1 VALUES LESS THAN (6),
    PARTITION fvt_distribute_query_base_tables_01_p2 VALUES LESS THAN (8),
    PARTITION fvt_distribute_query_base_tables_01_p3 VALUES LESS THAN (MAXVALUE)
)
ENABLE ROW MOVEMENT;

CREATE TABLE fvt_distribute_query_base_tables_02 (
    c_id character varying,
    c_street_1 character varying(20),
    c_city text,
    c_zip character varying(9),
    c_d_id numeric,
    c_w_id text
)
WITH (orientation = orc, compression=no) tablespace hdfs_ts
DISTRIBUTE BY HASH (c_id);

CREATE TABLE fvt_distribute_query_base_tables_03 (
    d_w_id integer,
    d_name character varying(10),
    d_street_2 character varying(20),
    d_city character varying(20),
    d_id integer
)
WITH (orientation = orc, compression=no) tablespace hdfs_ts
DISTRIBUTE BY HASH (d_w_id);

CREATE TABLE fvt_distribute_query_base_tables_04 (
    w_id integer,
    w_name character varying(20),
    w_zip integer
)
WITH (orientation = orc, compression=no) tablespace hdfs_ts
DISTRIBUTE BY HASH (w_id);
  
COPY fvt_distribute_query_base_tables_01 (w_name, w_street_1, w_zip, w_id) FROM stdin;
marvelly  	\N	pperymin 	\N
jcanwmh   	zvcqkejsttimeijkd	950211111	7
wzdnxwhm  	dxnwdkomunescoop	979511111	8
jaqspofube	owgdhnucqyqbbfk	496611111	2
escpbk    	pgldmwzurmpcsgkngae	784411111	6
qcscbhkkql	uzeznxwryyxnnafma	545511111	3
hvrjzuvnqg	pjoeqzpqqw	669911111	1
vsfcguexuf	otgxyehwdfeuijpq	329711111	5
dmkczswa  	qnyjknerugc	522411111	4
\.
;
  
COPY fvt_distribute_query_base_tables_02 (c_id, c_street_1, c_city, c_zip, c_d_id, c_w_id) FROM stdin;
806	pekjrdgxzcyxylhtfy	oveqietkswdd	523911111	1	9
992	hnojcedaovqjtfqfklj	vbdsmxjbyuncck	817111111	1	7
2250	gbcedyljdo	zxpkgykmebpzepn	936511111	2	7
2431	sjufedphced	wjunxifzskgse	932311111	4	5
1036	kkgmujscyx	xufgoclgargqso	631411111	10	1
289	majzivdebfszgrtrcyxx	xjghjicgavgqlqxdnkb	025411111	7	5
1801	gozoxcftydyge	nclouprdua	556011111	7	5
1570	nxgmftyjupsons	xdvrkydnowftwu	463411111	8	3
2805	cnlsckucedzbend	rnvgczolsenswmbt	952711111	8	2
610	kbahsetzvncyxzr	cnjkbiphpjaeb	707011111	10	3
2278	ecelvnncdkkmvoftyt	kysqlakiskqzmjlrge	533011111	10	3
2659	gftypibrka	cqyiekpjpwbsrwvl	227111111	10	2
2991	smlfzcyxyx	ucybptkqwdmtzbz	546211111	2	4
1036	tziycyxqniritnkyhbo	slkzbvftyglpthydrslg	517411111	3	6
839	iigaeniteqmpmsewced	wzqttbhwjsi	100411111	4	4
2596	tvsflcfkijawdcyxs	aaywtvphiccnvgfc	177511111	5	4
1765	zgtncsxvqnhcyxj	kvkogwmkgeigxexijowb	817511111	6	4
2589	jtxosstxfyegftyae	xzhtycnxrirabjug	289011111	7	4
1672	yrdypvtxftycvdfuv	dsbjmjcphxpjclzymfh	095011111	7	6
2286	qmfwfftyxdsdcclc	uzzifhnqwwx	056011111	7	6
2993	srcediobigacvlpp	ugacuknklaokhx	791311111	2	10
1122	nozahdpmxbdhcedb	ylgwxinzrmwoviqqhx	944511111	5	10
839	excedoebktt	wheeicmmsvu	107411111	10	10
\N	cantonpgxca	ttpbnmv	\N	90876	lostcitizen
\N	\N	asdfiuopqd	lacationd	90886	clockdually
1026	jcedfjimxunygsshlpy	sfghawfokctgtgnlknsm	607211111	1	7
1119	txmwqekcedqpkwuae	jjenaewuop	852811111	1	7
2914	depftyoqqfbtsdotupfb	zaeureendvpp	940711111	1	7
1048	flmcedqcdiyoabl	uquwvcgjbff	377011111	1	1
1734	qiguftyepkau	kjaybpviympyv	586111111	4	7
2774	khgftyqbhdrz	epdgmdxggrs	507411111	4	7
2136	mkzhfrqydzgcyxmgc	hzvrvryore	420511111	1	5
460	oyadkcedoymakghf	vtuqwgwmjaqgu	499111111	2	3
2172	qoffprnlcedlbbsj	ntjceyswjfphlem	683011111	7	9
1039	zqhfxjicrcftylegbsuy	jafiwucijmgwemfj	954611111	7	7
2255	lynogfkvyqbcedoebask	gzylvogfyomnquhzdxt	540611111	3	3
2588	qqjcyxkvfakidp	jiuymkbimdbmgnmgzlik	165911111	10	9
287	tcedwleshesmc	qajfhdxfaeyoewia	682811111	8	1
2858	abonqwkktftymwewyv	lbukxljwxniiblaa	956011111	10	7
379	ukteftyfmfnvixlttz	teufqigescstilth	619211111	6	2
2738	bvdupstftyaapevnsfj	phzghgkwnflwj	263311111	6	5
2844	gmzevcyxfh	dgnzwtqqfaxmiziqcchz	645911111	10	5
61	tsllexdwkqqucyx	bbhhdefmvqotqcyyx	713911111	1	6
1048	anyuwvxoced	ralzcwshkkjyhsw	235111111	10	4
534	xdzprvxfykpcedd	auorhqwmnqsnlj	496411111	6	10
2487	bsiumnqgced	qfhvjjppspbfiktgm	057111111	10	10
558	juiwzcyxiifemc	ccjuupkixndtmoezyl	337411111	3	7
2758	gngiplcedphqhfanbrua	bolpknynbgealkgovp	775211111	3	7
2707	knhvozfbhoquicyxqx	wcbqepmdivmn	522711111	4	9
1410	hutwicedxuntlwfhyiya	rdjxnouhnvilelp	917311111	3	1
1520	sdwftymdrhqdcfhqfb	rxnjtxjlhuxbi	137811111	1	5
1994	cedkrjyhflteryszdjxp	ftamzdlcofvbj	072011111	4	1
33	ncyxyxqdszhhezyfm	qgodpvfjcq	428111111	1	2
349	uftyqmznbweoy	odbpwsdmojg	414911111	2	3
2212	dbftyvfvpf	exszzoqhmm	054611111	3	2
180	dgauwbnbncyxcpjfluw	cknmqnjlafwhcrg	943111111	7	1
1702	bzeppwonuthzdcedyk	twbaboaasevannb	557211111	10	1
2835	pzbceduypmicmghwpmnc	tdxbarxoqhndp	021911111	6	5
1410	qcyxnzcfawbqoa	qbjothcvhootewdtmt	168511111	10	2
2226	ftyunrfmhyyozaih	qtlcyzhadixcpnw	311111111	3	6
1343	vnxmhugcednp	ikksfuydedltktoljh	436911111	4	4
1267	zfrcnodftyemwz	ncnczdmvqgosbh	173011111	4	6
2987	lstwmxasftyaqiklzged	qttlmkhrucwosxux	040911111	5	6
2691	kcftyricyuyx	wloktglatyylbjhql	632911111	6	4
838	ftylymxcaov	gcghvgaknfxdsaa	232311111	7	6
1279	vwejlldokwhssftygi	xddgyvmhqsgeccv	014911111	1	10
2583	xuggsjdwbuftypb	yhrvwxnodosuda	453411111	1	10
2801	wlvvfrvzlcedejxy	hqfpiwtjuczpbgngaj	765011111	7	10
1702	vvmwmscedgphdix	cektphjosilvxfrgj	206311111	10	10
2000	wdpzqrybrwgcced	xfzeuysufflrwaffo	180811111	2	7
2348	dftypnlhqzpayq	gkmbyavqeslp	335711111	1	1
1762	lugtzbsvproucftyz	lfvhwzvstrs	991111111	4	9
2710	wiektmepcegqftyxjeqv	wfogchynykq	150011111	5	9
2002	jbakwtrjarneaaqpnfty	txbhgodjcqql	078611111	6	9
1094	mcedjkivkbkrixte	iocsrdqvbfbtvdoisah	572211111	1	2
2764	lttiftycjpchbksu	uovtppxfdiysgbdqaoi	769211111	6	1
2179	cuvnuyvkcyxqyb	hcyqvyjqfhuv	591511111	8	7
299	amgiztcedmrhuojcbjb	ruikngddpmkimqrxk	342011111	6	2
1094	kegtvxpwvylzqocyxksn	ubgwzniqlzt	837611111	8	2
1126	zpfoswcedhvaecdnr	dwkfovbtkmtb	374611111	8	5
1175	kyaawqxqlnfasgrfty	ybtpzhufzotao	870411111	10	3
1833	vvcedhsnquvdrmp	xcdebsugclflzti	777011111	10	3
2401	cyxeuvxffnqyschai	onjghwnnyolfu	825911111	5	4
2641	pdpjiifftyv	vauqhvnxyflrkiuvcvcj	773011111	10	6
2678	ilfcxacceddlezve	aukaerobigsrubznpp	459311111	3	10
1925	nertgeaftywac	mkcwheplhfeeugbk	943511111	1	1
2631	kfbiopqxbftydufwgii	lmrjrcztvwopoqwodku	510711111	4	7
1382	oxyqoccnxxxftyenmm	vrkvexlxtdktjjcjhmem	763011111	5	7
665	cxczywacedabgs	qbuykrkdru	850211111	4	1
1937	uimctoeccedkdufsa	coiwxfercysjxkibv	202511111	1	5
76	tiwuioyuoycyxxsbg	pwlgviawtwe	096211111	5	1
251	wzdkftyzrzl	qxdoewjaibtpon	285611111	7	7
325	mciftyiyfnoztg	xodvzgbfqdrjhjhyeft	601611111	6	1
690	qzcedepjffziqrnuup	hqnurrhisajyb	799911111	3	5
1118	cedqhbtjcqbvwtcbqdpj	opzoqpatdby	843611111	3	3
690	hmplocedcxnbpdekkl	otitdrfesfkg	574811111	3	2
1194	wfjdpmhkrftytmvysjt	yqkoxljqhxfkxb	864211111	4	3
1754	tlwotqeftya	urifuhywipnojkuac	885311111	4	3
250	mjwlptybqthfftyfeb	zjtjsucrlvegi	570811111	7	5
1948	ojiiftyhamliicoxy	dqmfwwddmkkahwhhzgyn	885411111	7	3
2323	cedjzboafsbysedrara	qkzwxkyjivid	963011111	8	3
402	ynnoftyygrgaqe	bqzvwqnwotasbfc	868711111	1	6
2831	adxftycwuosgjrmg	ffvrqqqbcbcscdkdcdwr	510911111	10	2
1382	bgcedyhmyxvymjle	ljszpaqfzjocpw	198211111	1	6
1426	kehsnhahdkncyxcl	qakgkncykbbyrxwkszee	111711111	2	6
2323	hqvqenxzjzaced	ehigghixfmpjapcimy	958311111	4	4
2037	lzqcedrdxiazekrmzpwy	lltzscpjbgprpibizlgs	712911111	4	6
1719	zrvuqxbgfaieftyqh	muhdrnfxwq	953311111	7	6
1569	dhjacyxzllziqcwnakf	jcdrzvwodfh	237211111	10	4
2466	ercyxqwyqpjraabwvxqp	rolzxihjdivmxfq	552411111	1	10
2409	cyxeqicqrefslwefijf	ogcassagjylk	402011111	3	10
1945	dpatcjnqhmqvhpfty	atkcbfubchjeyjvn	009311111	8	10
2271	wcedlxdxhffu	gnngxvixqm	829911111	10	10
2806	syhqovocedhhzy	llmgbehkemtvbcronvrv	913111111	4	9
1392	vbktdopbsbmewftyfgyn	tlmwucpqqpoux	500811111	4	7
1033	dxedzachiruhcyxdsh	abzimulbemszge	336811111	5	7
1527	swaxppqklzcedwsa	knfuwalzyws	045711111	6	9
2806	cabhzgcjqcedvongne	zqpdvuqdmxmepy	989311111	1	3
1768	hwrngtyacyxetpivafia	nquxliygmivvacfvb	258311111	5	1
34	eutefcedyuciynb	sixumipymnvocd	200511111	5	5
2808	kyunoppmmprkftyrg	uslwbvyhokb	846711111	4	2
1768	cedjdcalmzqnbfbbcf	pcgmjrgkpcxrigxpi	240011111	10	2
2482	bzsltyatfty	gwyeorfnedsl	511811111	5	6
970	lycyxcqgiacn	fprabefzwzpcmpuvex	680011111	6	6
1480	gbdlcyxjyohkrtr	rurymksmyheflsxru	936011111	7	4
1725	cedmiwinqowppydexksk	rxqrzvzlugfei	851511111	8	4
2755	uaftylychz	lpmwqdtjftpcgtfljvt	437511111	6	10
1143	vqcyxszynwnboowktq	zawuyudlvxhntj	225711111	1	7
1198	jicyxyiroowznristmpz	epyhlnokrkydknq	657511111	2	9
351	ellcyxhooxdmszzuajv	mxdlwebmopqwuplzxg	100611111	3	7
2	zujptddcedguji	vfczcudxxmvmgm	233511111	5	9
831	zftychcjfdumdh	uazwuhwigzzcmc	507211111	1	5
2530	ystxormcyxs	xlklfqqbcgx	389111111	6	9
2885	zdsfvkcyxyjpbeuyzxch	utkdnmfadzibpgz	956211111	6	9
1838	dmlrgxnkwziftyolvc	esxcqyilyrymtktdqs	581511111	5	1
2053	flsbdldqzcyxyivev	mbzosoyilgnvzcs	808911111	3	5
1781	lhwqmiqqahxuftymd	txukidcpckv	366411111	3	2
2158	xjiipzhygugscedyws	vfyvjlkhbqqulxik	245711111	4	3
351	kyffgbyqdzeqlscediq	jopfuaseiovpn	599111111	5	3
120	rfpxtulhavcyxkolal	ronadpvspzjodll	354011111	10	2
2075	cyxwfflbdjqxsqrowzfn	qcxiryqdoyddmtjyv	000211111	10	2
1206	klvtbgwspceddplhypl	nqiynchpaujugcjjfm	680911111	7	6
1244	uwfpeaftypm	ytruebbxawhgdutls	690811111	8	4
1771	maftydvsjmfydhzzdht	fiynjphseek	449011111	8	6
1450	wzvcyxxqmbgkc	bipmbourmqwrxkpm	319511111	10	4
1058	kbaxijthomcukvalbfty	wbyawshypspyjvr	535211111	3	10
854	xwcyxemexcblijnaleo	qvcwhclutxanvoqhbxcn	318911111	4	10
345	xdfyiqcyxxkqkxll	tkuftoblrmmayampt	883611111	8	10
2369	czxzlcyxosojv	znzsiutusicxvsfslkt	407511111	8	10
90871	\N	\N	daswwwqer	\N	massiveas
560	diezricyxcxzuqlahb	iyygvxtgsiywu	192411111	2	9
1446	ajxfcqghcyxhz	jmqpxbxlzcluqpzhsml	859311111	2	7
2652	hgpccedkpleefbvsmwoz	snrqaxlmjvovfsaxiw	735811111	2	9
77	qlpcyxvkyecvjmkv	kyqnozalquunjjya	632411111	2	1
667	ccyxljsipnu	upvhyvorwprpvotjcsih	280511111	6	9
1053	eszcedmbfvqy	boiduadvmkgf	708511111	6	7
2195	djrbqqvyagkcedq	qhsrpvjxolzpfcr	138511111	6	1
1684	blghcedgqu	sjvuufshjrhlxaj	576311111	4	2
2504	yjojnjajcedbcrqhwtsd	rfmqhlyakivapwtnv	186511111	8	5
1960	cedhedsfazhcg	bgdgxxbzwwujfm	519811111	10	2
2504	kwcyxxqgaufrlmzazbwt	vnmvdmhxapwmaue	897711111	1	4
108	cyxfrtobfqptjzx	wmqomnnvgdaddys	100211111	6	6
2765	osicpkjcyxqixikj	bwohhuvsqz	182511111	6	4
2553	atfpvgltwhqzybcyxdc	cdavsgpxeotqwlw	985711111	7	6
2765	gsmbcyxyhrelqcvb	tkcpyanldxpejkubzn	165711111	8	4
108	xhqacedixtkfik	plgfejqxrdodgshor	506111111	3	10
2750	wmmuvhylpcaftyxjj	lsqidjrhnsdaftfe	114211111	5	10
1379	hftytxwevwwm	zczywofasdizm	962711111	6	10
1312	mjtgcedkdcw	iqzyuagngcowikd	510011111	7	10
1724	uwoyhlmtcyxj	emebtovwkccorm	187311111	1	9
1326	mkpldhpqftyzajfoqzef	rxyqsnrhyoplpurdl	427111111	1	7
2184	uzjiafrulpwymmppcyx	wwemuxddekdvkxhbh	118411111	2	7
393	ujcyxgmvidffbnfu	zekxsfqvcsdeslj	676911111	3	7
881	sbjtpiuqzcedebzn	ulgyamucbmqhgcswn	234311111	2	1
516	tjiinzhiduoftyao	msmnninoxl	603111111	3	1
2881	cyxzipspefvqg	vwfhrkhdbzszthxjtv	714811111	3	1
119	echnjwfcfhocyxfyeal	mpnbonxjlix	198611111	1	3
2591	flafkddrzrcedzpkr	gowivrhushzjehzmnni	450811111	4	1
2861	hwxtnhfceddnjtmtaqzw	dfbrlezctnpumo	124011111	7	9
2031	rqrwjrsftyu	ubckogwahdud	644111111	2	3
48	cedghmwtxzm	umxrejtkszd	506311111	3	2
646	phicyxibistz	bqowvquqzc	721411111	7	1
2262	ylihcogyzfcyxprqgbb	zbajdpmojnlaz	374011111	4	2
2695	nhpgiugwftyhftgghfw	aigsmeiowseloie	882611111	10	9
103	lrhcedkfecnkbmlf	desogvjcyelimjyjwmjp	787311111	5	2
1794	edjicyxlilpcyfksgpd	knzcmitoopqfrqbrueq	830411111	8	3
2743	wolkclcyxgbqjzn	pjiaimgemxlwqlqgx	758611111	8	3
821	brstficyxctddwxp	luhszfvmwmulmcnlf	292211111	10	3
205	idyecedfrbjpowbcrjy	hyfwqswvjly	997411111	3	4
2775	fnfzieccedpdxdowvoev	puoifwhnrkkauj	820011111	5	6
2031	qxxavccceddghfhk	tsprodnmtbjpgbotbrqz	607411111	6	6
2881	hmvwgscyxehxzzmmynh	zpcvdernmfvz	487511111	6	6
562	ftykgaplkx	roituxmfuayzxmkyoi	960611111	8	4
1841	eepluchjfty	njhjqmvnnjephqudywhh	367311111	8	4
688	fomtlcbcdtjiwjfty	quipevjyxfrx	240011111	1	10
227	rlvsfovqnxftyc	bqfwrvdudwnqnxs	813011111	2	10
1682	lgupmdziazvftyaqdmn	ymaxnvxabmvrrvvone	490111111	7	10
1286	mrqoqscyxtvcreqpy	glcijmoplwimztejb	637211111	3	7
1533	ydwcyxnojlql	hmparzgiafckxhxb	506211111	3	7
383	ocedhbcwrdp	ektmzeaiazvqivi	137911111	5	9
1589	recyxmqqjixjhhuicgca	fsugqfcvfyhhmbikwtyy	537911111	6	9
111	ofkojyjycyxmmoybqyk	kuizcgirkbmpmtrmdzkh	439611111	5	1
2494	lftyxpoubiny	dpyjkmymkodnkve	083611111	1	3
1911	ccedxqtwijnvstkzkab	pebgzgvvun	425011111	2	5
2624	hmsrmjcyxgnjfkkc	aejeramksjc	170811111	2	5
1518	pdtfvlftifrcyxmrutkh	hthndwerufpi	097211111	10	7
541	dokydjhvrcyxolo	vcdoptvfyhwskspt	948111111	7	5
1280	mpavbxxnsvokemobhfty	nieqlhmimhrujqpi	994411111	10	2
1677	lqcnglwcedmwubnvb	fydoujyufowm	725511111	10	2
1347	ctbhftylobnubresjq	tatosvapfjb	880211111	1	6
1804	jyoswvrnzjpcedvd	olnyitobaqrodjrdn	493611111	1	6
894	iqccyxsycqnnecnobph	mxlqvdzqmpd	905211111	5	6
100	vvkvhmgmftyuxuqmt	gvkutyeeulvqvbbmuds	063011111	6	4
2095	ddfstfapdfluwncyxjy	bvmredpxjogihpgihmkj	406411111	6	4
1972	gftyybihcctahzfn	hpoyrtmccc	387711111	7	4
2896	jjekcyxtfja	dtyltnzjzobsvs	842311111	8	6
2562	xyftyzlxvnsm	bvicggsclcqziym	829611111	10	6
1395	kcedilqqluut	updwfhtzltzpmqof	634611111	7	10
12	eewmfgftyc	fbmzujngjnjg	140811111	8	10
11	gvfocmcvjngqpocyxsg	oidsxfcbehrb	837311111	2	9
480	cedsdgqpnxuko	dijpbkceeqxpf	162911111	4	7
1304	otsqscyxak	tknvptcrzis	067511111	4	1
1111	tencedutfrqsfglko	qbmchntlfkejc	955311111	1	5
370	dbtekslzkgcedovzplp	kxehlllgrrkjsz	818211111	2	2
2461	yzglvypkhqccyxcqsc	ukmomxhyxbsjjttdgnj	316811111	7	7
722	ftypywkespg	rlgxglwrwqw	095811111	3	3
2199	adgyntuacqftyjyggkdv	todaspghxeqncara	548411111	3	2
1955	lefdlzwcedhzydy	fbakymrhrjzkalpdpcgd	765511111	6	3
2178	qteisgftylnsrh	aucmkmrynrzflrchjmhf	682111111	6	5
1738	ukmqokxydvurucedyl	jcluzffkikxftr	850311111	7	2
2499	mdiucgmnrypcyxdtb	rgrtewhjkwmmgvattk	327111111	7	5
29	vvhimihhcyxf	uojjelrzssep	311811111	8	2
639	cedlquhtysazz	yrgmqqfyyqxjjqiaovb	850511111	8	5
156	sivesimhdnlmttjftybh	xrqesfysjuutmdf	553511111	10	3
528	yzyleftyzkgyzxd	lqmpbdivzxxqcly	040411111	3	10
1815	dlxhkdwxpuftyeda	mgyancngnmayxwe	195811111	8	10
2564	glkowmikcedsikx	voxkroiyjaiewtg	359611111	8	10
853	xfwpecacqcedldnsgfdz	kfprruefkniff	721411111	10	10
225	ufqiylltjwwkmgzojfty	mgqqwkerkthsjbxsxf	493611111	2	9
2282	jxvjcedxwjk	zxmygszxmwipaiunqt	192111111	1	7
2539	melwsbtlcedbc	ykuyusypvaouj	614411111	2	9
2479	lccyxogidju	ipmvmmvqcg	152211111	2	7
2689	zbaeytizfcxbftyna	qhzzfzpidb	926211111	2	7
1469	pptlnftygnsmsvdc	ykbqheczrsjgiihkgulo	878911111	3	9
1422	tvkrncuarbnkskycyxlp	eyvdjiwtpsdqzxxfv	805011111	1	3
2931	ftygxkhjku	sdtffdfzpwgtdcyxf	321211111	6	9
1632	spqccyxnzk	rseoisewqporolpbjwe	003511111	6	7
2928	hscjnrcedyaesvwksk	ymfdrujbsmhbh	737211111	1	3
1027	sltbecedhlbvoyskvc	xszslxprwetdl	233011111	5	1
1680	lroutftyusxlyomij	dadqjheyrxy	685111111	2	2
117	rjycyxqraqbmsfvlau	bxecybgpbqzyixzr	439811111	4	5
435	pmhjtlxwbopsftylymz	lsculrauej	335411111	10	9
455	sgdtfmzdcedlvil	ypzmfbseljvmtknyzdp	843711111	4	5
693	mceduogudu	guxzkilyzhhefmsik	006411111	4	5
1079	hydqwcedoxvws	gfkdwqiryusgtjhsm	740811111	10	9
2757	hamejcedqvswvaddylqy	bsfganfonhcts	114811111	10	9
114	kftymjawqx	gwodjiataa	449711111	5	2
194	hyxacedjzgodvbswql	dmrjyzpppcz	063411111	6	3
2133	eosybijvohaqcedp	cjsjbcxevoxqhyl	693911111	10	5
2974	qhtddftyxnqv	aftaudwageyikfuniqae	027811111	2	4
2078	cvxftyqyggz	gtytnhkeww	503111111	3	6
1766	tgbfqwmkacedrox	szulezyyiyfpktbgrow	167411111	7	4
1386	coikcedaxoz	gxcndgasnigmz	111911111	8	6
1422	wdvzlrlmdyrjhdcyxnuz	pzeissxbtqqh	464311111	8	6
859	cyxazqxikybck	bgaytpbbzboxhflwg	752211111	10	6
445	lbrlyjzisijgazcyxj	yjlfdcngcuolrhy	763611111	1	1
171	kmcyxwyvkadgyaa	lcgvtortdeahjahv	370111111	2	1
519	egpaqtkefbkgijcyx	sjerjjhanmggcnivk	926911111	4	1
520	lejabmokftybni	dytzrytkvitavpsk	081811111	1	3
698	wcyxuxxgxhkdeiwwx	mrpokoosgugypuwwscbx	034111111	1	3
320	xlbcyxfnhsrvhecnschh	gwydhgpajvelu	771211111	2	3
2120	snthqjftyvoxxczke	kgsltsvxfbyeaa	928311111	3	3
400	jgdoodyqpttanoahcyxb	cjvbxleoxrmvdlf	613511111	4	2
615	dlzaijcyxjat	lvedrjudllzhvtcnrzn	127211111	5	5
1587	tzusqoihbiebwcednay	fnrrmeqqhwcvwtngzyd	548511111	10	7
109	cqvqpqlabayacedvxffd	lzydisodguhpmwqfxzl	631511111	6	5
2422	lvciucyxcwhtwhh	zvensessskkktydrtnk	490511111	6	5
142	ayxolcyxrisadbbmdrj	hzatmfcjsweq	918411111	1	4
1444	mebroscyxh	puogupuddpchhiuzm	167811111	4	4
2140	brsgouoxfty	cljjmcpzzyaidvoa	044011111	8	4
1427	hpiyovgcvtdxokmgftyd	yqcfezuhomdcfdltxyxm	685011111	10	4
2288	rldmevqecyxgaban	zpnmgcaooutllpyfgtx	711111111	6	10
641	fvlvvftyniokaxvp	mccospuktverqv	165511111	7	10
1144	esjpoxqhobcedzydt	aoamdnsseeos	565911111	2	1
2227	ihvceddgqb	uoozkairih	823611111	4	7
2214	ddlmpsdftyhgt	gxpdwfhjpohiocr	515211111	5	9
1874	ftyiodgcajsyntniuu	baenwhrfau	409011111	7	9
2227	dbxcnwjqemuftyle	nulepbihogmuqesh	928111111	2	5
1339	dypbzbitaftyxotnmr	gwkjgcpogg	327111111	2	2
1917	ncppsvvvvgpogcedmgy	owksmdvgzzpwp	595511111	8	9
1543	uomqhrsircyxkgm	ejoruwdoqtmz	928811111	6	1
1865	iiavohpnjisqocyxgk	jkjzoiselg	901611111	10	9
2115	pdgqpfbyytkwbfty	jdptrltzidkog	613311111	8	5
1367	eckjdtrcyxbzymjbv	frwgeilkepwd	586011111	10	5
97	zklftymlrzwx	wxlerljixxd	966611111	1	4
2227	svicmgxhbjmcedsddulr	ydaxsgvtrmukkjywvuhw	661511111	1	6
675	vfftyddcwympcmgc	rrbhotcwngcnzybtnoqu	583711111	2	4
1425	cedsjvprmup	kmhmpexzgspvlksev	440811111	3	6
2272	cyxgbnscmkiq	pqczfdqgugmnnazdv	757911111	4	6
1928	xftyaongbn	vfrjkarkfx	793411111	7	4
2039	udcrewcedaklnqfb	aupclygnzblz	140711111	10	6
1183	acedsxphnedr	coqsahisfthwhvcaiadl	785911111	6	10
1665	fdrqkcyxdpwmjozr	itosuenlpicgkjqkswr	359311111	2	9
240	lavyqpcyxofbhecp	esocdvkbcmvuxwuyel	262911111	1	2
2252	adcwbzcyxwssrapbe	cfwdiqjpzwgfrts	362411111	5	1
413	msogftyprjudgkvfb	umqwtgvkktx	869011111	10	9
397	rsxiftytskhtiaqofdcn	kpqyxslwllmz	525411111	4	2
1259	emhzqicyxq	anhttwtephilbtr	456611111	5	5
2061	pmhyuwaqcedwto	bfhtmpqivxq	872811111	5	3
2330	rulteqpojftys	lytqghflywziphtexzu	966711111	7	5
2252	brzpftydxji	plxxfyvjuhat	759711111	8	3
1653	ftywarmfuyrjosgsc	tkznoujghcmqzrjuealc	758511111	8	5
429	ldvezcyxcgwvbcuq	ofswflnqnoii	139911111	2	4
1443	avkqamvwftyhfb	yvqnthyjipnoxyjq	280711111	3	4
730	olsmxudjftyzumxn	xnzdxqyxvp	471011111	5	4
2167	tkvcyxouwugfswav	jjjkxhazxdehofrppzd	838011111	6	4
800	nxcyxrecyyjpcmje	tsmcowntcoianuacgep	675111111	7	6
1665	qceddljuyhivrrqg	yzajdhokocmbgdsjruen	989411111	7	6
1817	qoefuijftyxofms	axhtldosxjgpesgtlwt	391911111	7	6
1619	slhhxvyftyysv	lsekuzakkd	686211111	8	6
1524	grojvcyxrmoeviu	fegoaxeqkozxvr	352511111	10	4
964	yqnilpofincwhofty	qymgudsjvuu	311411111	3	10
1358	ktphbdscyxrazbd	npgkxtqktwgkshvqq	234111111	7	10
1825	bauxxjohipcyxkjcak	rozmecoaqlxydewiqt	927811111	1	7
2218	khydwuoqcyxjhkfp	vsdtowaqmkiidpvao	070211111	2	9
440	upcyxfmxlcru	cpmcoitpqyp	360911111	4	7
2469	cedkvsxncapzrsgevatb	zjjaumrerk	430211111	1	5
758	ljirdircyxummfwlnwh	qahvrhfvlw	336011111	2	5
1370	nxspaujpshpftyzey	vpubczxmkjdnoy	483011111	2	3
2880	cyxhpitqczyiqovsf	tjoybuickkhvicswlcod	286611111	2	2
183	hpspsqhcyxxdln	jduzeskzqjungtjnpqb	112611111	5	3
264	kxxaajtpfty	wttxfstrkujjryyuq	385311111	5	5
1936	pmuezddrcyxgrqney	mophvdhlknoss	914511111	5	3
2923	culqebkzzccyxvhcsa	dsbspvjysae	741111111	8	3
1989	cedegyahxsgh	jwsgonkjinmginz	771011111	10	5
306	vcedwdikkiz	fhvubinsygd	885811111	1	6
2923	ikfhdgakgfty	endhnupwpi	183311111	3	6
1002	bsvggxgalqypdcgcyxmo	biaxtguibqt	379311111	6	4
2347	fwxwzdsftymrrag	xbdvfsgccwwye	063711111	8	4
2086	jdgerjcyxoumyrtgu	oigfeaqznczlrgjnpp	249611111	6	10
1210	voocmrqokevnrfty	omhhxmfjgsreizqjpu	054811111	5	9
2561	aqivnwftyrf	uqrlwaqrwxheo	294311111	5	7
2791	azrftyhcznyjbct	hkfgnlnsuswlgxhd	057811111	5	7
1197	gcjijlxaupxzuzvced	rfklrxtbjidekkvbwr	024011111	1	2
1368	smmhbeediylkoftyxoj	fwcvwshzejfnyqtejzon	463811111	2	2
2326	fjzlecedzaq	tvmebnkmxbhliluyrjyx	997311111	2	5
2501	cedjztryegl	edfilmlunc	502411111	7	7
2630	cedgzbisutptpq	ogrqqzzxnwd	197311111	8	9
768	ulqdkicedhtxpp	coydgbewrfagciyetn	774511111	8	7
1715	psyojftyzxaxkmoqhy	tykjnufkdeeywquig	228811111	8	1
2263	wtiigpfzcyxhwbpunpbq	jyipohhclir	611311111	5	2
812	rgrkfoskjogeesrcyxo	tnfnemeaijrb	469711111	6	3
2570	tcdokcedcvyuqqvse	quhcrcbjdrg	755811111	6	2
2319	epfnsuftyazpholvmk	wvlddazqmqv	798911111	8	2
527	vizfcyxpufiehrigcxzv	oephulscaumxcbdykh	010211111	10	5
783	elqvaahviybzhukxhced	gdkuouohxkc	360511111	10	2
2296	egppljhcedj	gcsxhwnxxw	835811111	10	3
2501	cedkxhwpelg	htawmkczshxpwgjmkx	297611111	1	4
2319	myuonlqcedvadtazhvq	ydawncdbefsjkyjhfjnv	814611111	3	6
2150	jxfedqrumftysvvt	ljaddmdmvyv	627111111	7	6
832	nfzcyxcghpfhnzot	qkckifboapmp	342911111	7	10
929	cyxdydkbamyzn	ljxsojefoomzwynmrhvx	972411111	10	10
908	dwsvcyxodfpn	dgjjfccqcujd	015611111	2	7
2063	icedvmlnertt	kdbwialaod	987611111	3	1
634	wabdzxakdzaakkcyxnkw	slagmrlhhj	042711111	5	7
1741	zyxscubacyxmvxtb	srrfabddvraredisqbnw	628311111	1	2
368	lkpxcwiraqcedxpgqrsb	czqkctqckaleqkycivq	769911111	2	5
2036	ftyakqwgvbhfryzu	kupxwcdznvjm	997911111	7	9
458	sjcedpvuflekecupm	wqbwchawhsdfwoqkvvaj	367111111	7	7
600	lcedvtzyvrcclydttg	gjxwbgrfwjfdqlcbk	136911111	7	7
1889	yvkcyxszvqxi	fhhkvswtosao	842211111	6	1
140	sftymujjkicrzku	veunleikavl	567211111	4	3
572	reefaqfmftyctm	ovwytsrbhjdr	998511111	5	3
2526	uivkrafghrkdammjaced	ognyspjgmacf	372111111	8	1
694	naftyksyzzv	swxmswzkvtwx	075711111	6	5
1497	qcncgudrcyxsjotdwwb	kzethqcmcprugqjfp	612911111	7	5
420	ihpqynceddf	xjxyxkrbhc	410111111	6	6
2829	tyyjyplxcyx	ojaacjhsusxcz	660011111	6	6
1741	lroscedmvbkg	jxchuuruxtspwiejcbre	498111111	7	4
1879	tcedqqufcsn	nqmudhnqnt	098411111	2	10
634	ftyfocptrhkjbciqlifw	iyggrzottvztpji	341311111	4	10
1283	ajlblggiftyldj	kleyacczicatjnq	403711111	4	10
1887	jsvbaosftyorxmvoksox	btfkpcdxljhlh	716311111	5	9
1624	cedjkiloccd	kdgdllsknunwegvpgcy	552511111	1	5
2266	hftyzxiljgaoasfmnfa	gxdbiykoer	689711111	4	1
442	qqlhwcedch	jfclwrkwtfjs	298111111	2	3
1534	pxlgacedytfh	xhupcypfuxxbwv	112211111	2	3
56	zcedsocxnsnef	nqxjobbaxwljxuy	058911111	8	7
1648	dcqmckcyxxvnfwrg	sohtcqknab	554611111	4	5
2563	rgmmiqginrwjnqocyxe	joybdnvirzmslg	509911111	5	5
292	dqemdcedoiwtcqgjhf	pfzarbdkvq	481511111	10	1
1616	tluoftyhbhif	kszjdzkjph	957711111	6	3
1811	iunyoaadgfftyvdrxtg	uhbhosmlovxsvkljm	073811111	8	5
1690	rhbpndkhjgcedwsrauh	pymsbffpmxkeiqrnj	074111111	10	2
2168	xxyaeygezaftyry	buluazyaolefqxgr	034711111	10	2
1265	zpvuylsfyuxcedcqi	jenuwujtgh	260911111	5	4
1098	zcgwizftykcefyz	filmadcpatvy	419711111	5	6
2992	mftyxjmmweggbgf	kycdyzfgpefgpwd	506111111	5	4
1204	xzpasonipjoftyydk	matskilkvkaitrgczhj	114211111	7	4
619	oqecpcyxixjlqlsee	mgztrzyksnhvyzbl	523211111	8	4
1624	mceddqknoqxdni	wnjqlnddpgllgpryt	108611111	8	6
789	ncaydnxwcyxnpwuutcb	nvdenhwgczotyl	947911111	2	10
217	sbevxbstcyxlpoxm	lsmqgprsknlozvakqwv	770711111	5	7
643	ryufxgcedocvbtdeeg	jfvyhmgposserqjqak	280511111	4	1
438	tcedkcbngwv	wwjzeqjlcgbuo	809611111	6	7
2193	cniftygzstv	givkgjzzbqcvhxt	430211111	1	5
2623	krlkwwyszfyftys	qtmchhwyoeyarajb	303111111	2	3
2521	jofcedcuvmfxqpzjuync	srstuontyfwnhb	878511111	8	9
1336	guvkqpicyxjlmzqhkny	cgbgxyiivyamckhdkcid	841011111	8	7
68	nwzxupcyxjn	qnhpnmahlgtlmdkldq	554811111	4	2
2668	ikaebvyxubatcyxurt	pokiboftgdjf	035511111	8	7
369	ukepcyxhqgsukzi	rbhsipqcwjd	159311111	8	1
1192	paovccyxqf	zfxfosjuyna	956111111	10	7
2521	bsqcyxhhcuwchtpp	vqnpkgzrdoqqdubjk	140311111	8	3
2490	tprxokmqfbmgoqftyjjm	bwpgoldilfu	647811111	2	4
1387	nihcedfwzzvths	seofvgzswhs	258011111	4	6
2490	cedpiekbebuxcegcfv	oswgcndtze	687311111	4	6
2100	ahftywgedtmqsyexu	oszkbsyjdpvj	284911111	10	6
2327	cyxkcuvoebjtwhrwt	evuhyqovmfifgbonvpg	300811111	7	10
93172	oiuyeacya	\N	hwaopvcx	90909	\N
912	tftylafemreomwbmsj	fkaukacsnsfkszln	563111111	3	7
28	naeassocyxnpvs	stqdkivyeefv	553811111	2	1
1142	wnftyozpavmgnpggf	nmmlppxzkxgwmwz	885811111	5	9
1142	lzzodtosdfryvced	yljsifcxiizh	769711111	6	7
409	fcedbahblmzy	alssttjwpwb	769611111	2	3
224	uceddlymyv	nirgkrdjboeqifmlebge	414411111	3	2
2344	kzdgoflhghftyr	cssnorgdulbtmfb	283511111	6	1
2847	cyxtosaacfkvboxe	cxhhvxwcummdm	344711111	7	1
1625	ejjfzwglctcedduwhorf	tmesslnaak	486911111	5	5
2587	icedtzqsawtuxjpfq	aleiicjqwbaxnpzvfuhz	605211111	5	5
2919	cinbcyxovtsdyppsaasm	lhtwidvpad	090911111	6	2
2798	cedsuwysynagpzh	puuadlsbbohvwbhb	002011111	8	5
1476	ruqtcivnced	ehprhpnhhxnyvz	483211111	1	4
505	prvcrpngocyxssqbnnyv	mhoigbbotieukyft	142511111	1	6
1438	vjgkjnwwcotbuncyxqt	fkjqtjvzlrdsik	400311111	1	6
2919	occwerhqcqasgsyafty	cdplscqfxwwqjjrqhph	042411111	3	6
2543	npknkffpqdftymc	dflpyjwodscf	567211111	6	4
161	tmxzmcepuneftylmi	qoymazupnvxdttuov	316611111	8	6
2491	utnxcyxjrjsxrukdelk	wvbbygpircmmjgor	180711111	7	10
2339	layukycyxcl	faygatbjqlasfpdlscja	964811111	1	9
1474	qowgwqwced	gazucgzvvvliihyck	160711111	3	9
2103	bdamztzhcedvlu	eixlgzwbqi	811611111	4	9
2448	qtbciyftyfiq	bojquckllwbufqawxzjg	016611111	4	7
1675	mzcyxkfzagyuwdvhuxu	tnkbetslwvzzvzmjg	675311111	5	9
1474	gstpezcyxzqevygu	jhjmoojdian	962711111	3	1
2338	cyxoinxryvodqfybulpa	jckwllxevlxgpmyu	319411111	5	9
1080	ldfplrlovncedxet	zbghzbfapu	151211111	5	7
2658	puvfkdwftystplcgteq	iobgxgthcnenpjjholhq	697511111	3	1
2285	wrekhzcyxlamtrewpnp	uipwxjagupxnkrzelqz	354111111	1	5
338	rlcyxacotpwxk	voorlypihuooljndoh	248011111	2	5
2629	skeeiueoscoqbcyxnj	anbtvryorvopxaipoutk	812011111	2	2
17	uxcmwwvftymgffghsyu	abwdqfbbfnllafubzqd	013411111	4	2
2455	uuqhsxqkhcedp	whtuiyajbgmxaor	460711111	5	3
17	ptmbcyxfikvk	rupxkauczdjnqeaxqvba	600111111	10	1
1836	cedszyfcwmfmiljldwry	zlovvujajojsxrqnh	650711111	6	2
1556	dvncyxfoxqdxygwwqav	chiwvxrivdluy	858711111	10	2
262	bcqowppxftyn	uezronipghz	317611111	1	6
228	ecedqciojamcmyez	zpzpfgqaluzry	913811111	2	4
1742	bpakxgvoaxftyrfu	jxxotwuhlucu	390011111	2	4
633	fvcyxtknyhvow	giinclqalk	480611111	2	6
1357	rdihtqrnckpzftyoatt	mgjtodmqyihbfzqytj	742211111	2	6
1982	mcedlxdivrdaszdh	hmfcqsmymdgdtia	651811111	3	4
957	jintndklaycyxc	gxaskrlxxfbqpicuc	809011111	3	6
2103	qjlcxwdcyxokyhlwws	nqlwcdpkznqv	124611111	6	4
2018	ijsftychvbrdgezraf	gidfecpigws	654711111	7	6
2854	vdxcedsjbxt	pmhdhnzzisyyezklc	332411111	8	6
1262	jzfyapnurbcedleioa	wfrjnlzhiucrfell	576711111	4	10
900	vayrlyzcedmicilych	eljytwbuwkn	114611111	8	10
2713	ivceddqlsuvb	ifpvcmhlxomoh	431511111	8	10
1776	seqywuuwahmcceddiga	okvgklnxffuawlvbnxn	427711111	4	9
1862	swprcyxwonrfrqo	chadmwhsao	236811111	7	9
684	dpenisijhvkdtzeced	lzfngwywyw	993311111	2	5
245	ftyrhaotbrugylpqgsq	dmvhowsqjchgvgjjuqdd	300711111	7	7
608	tvxgougnnpdjftyrvlfu	ysabgqeyoricpa	927211111	8	9
376	owvoafbcyxabvfalalh	kewxqfubwkkb	646011111	6	1
652	yolptgtquucedbgfu	mdzqtdvsowdimo	923811111	6	5
1919	ghsqnkyeivlegsxiwcyx	hfhskcubmi	798111111	6	5
2699	bxocyxpiin	jcqkpdnzpscwvhwhpk	603311111	6	5
330	aokaouuzicyxpcihi	vtvozwhtqevzbajmq	843611111	7	3
1371	socycedtocdeenwt	idsvrfysjhkpzruh	175311111	7	2
2041	lcwoprftyqwmirzvvcjn	ykdizfbnwyinpsttzqf	595911111	7	3
2633	brenqvzjptfty	jbggmmmthfoimjcsmbww	665211111	7	2
50	ocedlkninihebuomng	azvhqkucpvxqlcnpf	619811111	8	5
684	adyaihcpgrdntfty	zcqfiwtkhztonnamtah	198211111	3	6
2240	iguzaubcedkflrkvqg	elbhytuoidrnjr	520611111	8	4
2090	utqqncedaslkyaycixjk	ifbdkefjkf	076511111	4	10
2600	spuipakcyxvbjxhvroyl	atsrujgxdoiavi	467611111	5	10
1526	bftyqfjhvhrxbmmdpqax	vlgdjkxdflph	858411111	1	7
2954	yftygkxookprmusyogz	ogqvueyjfkonh	845111111	2	9
1470	myqjulybsmqcyxzaf	irycddfcwtz	052211111	1	1
1022	swojlsgccyxzywh	ujdaduehztamfrnoept	920211111	3	7
2425	pcifeqfmulsmycedw	blygbighclluykognvs	341111111	3	9
2425	ymamcedxjishxsz	droxecpnbvrimksiwkp	821711111	5	9
1658	tjjapcyxjygn	rtzaxdwadphoi	522211111	1	5
2156	nqqkintrgjyhnuayfty	sxiicrysxfltzyysl	711311111	4	1
2371	uidooqcicqfruczfrfty	zrxfuxjcwefpbsqhqck	351211111	1	5
594	bolfxudwoxyrcedptgfi	unyuyjlrxldfazvnhvg	174611111	5	1
272	sacyxyclazqktepl	ujqyarfgddcx	985011111	2	3
98	udaecyxekvpoasenoj	zivnbnhvtigfaev	161011111	2	2
1658	dftyryekbunvzgrjj	kkxlvsaxpsbcxmhim	723811111	7	7
2358	pftjftydrsoll	ncohnhjkgbkpmihgz	113811111	2	5
1186	ehlerswazsscyxgm	eqftadbpswsdyk	269311111	6	1
2951	vmhwgfgsxftyjogx	opbcmixtxnjiqv	391711111	7	1
252	wcotewydcyxi	szlrksudhbyg	356911111	10	1
1083	txszvrxcyxpccstobgti	vxfetnpwkun	151411111	6	2
2079	qgcedmaqnh	wxhguqoktybekexh	808411111	7	5
1141	cyxdhodxoumpeni	liyspjvejsi	973911111	8	2
2951	gftykuusshne	igwiyzbxrxrisqk	179011111	8	5
336	kjnernrmrrtiaqwplfty	cvbsugeper	561311111	10	2
1921	glaodihmexgkcyxeydr	pfqagqhpxorzdklmxq	060811111	10	2
537	cedpatlqkdelyzt	jxsbsqbkcz	853611111	2	4
824	dgtetcwcyxa	vbakzztutjfgaa	276911111	2	6
2241	riijcyxopixxyeumvfgv	usswkqpyaqt	002111111	4	6
1775	bewaczhcyxbzspt	crgsqsjaylowhz	319611111	6	4
2425	escwbqdnbnovcedonccv	qzcnnpnnfso	994211111	10	10
\.
;

COPY fvt_distribute_query_base_tables_03 (d_w_id, d_name, d_street_2, d_city, d_id) FROM stdin;
7	ztdmtgo	oyjingifjlof	dlzcbggjwuxaisjtq	1
7	gpcocsmje	phkybcknrmeeoapyz	xkuqlaarvgeoido	2
7	gtigpwhlfa	igzfxrvivtr	ohztfcsvfeasfsv	3
7	hvfgdobl	stlencwesrxpvgufe	fhtdotxnjhrydahzjs	4
7	rmxhqc	kbsyypdqgn	atpqcxpzapfa	5
7	nsmnszohc	wpmbezfuixfo	wwnjevpclrmdsxaezbmc	6
7	efikabx	zszhbqmkucxnqv	zimnqexvkggc	7
7	kexewboiue	hnwsmwzowtpxgpdeo	fwxqcynwolgsdxay	8
7	lqblvxm	scrjnhhrsazgitebq	cyokshpzcsvevzgi	9
7	ozizzagz	wqmclrivfiaxlbz	acmuvgtlwjejfbft	10
8	mmdsbia	einieshhqvynub	hlqdwbutehu	1
8	gvgwhgtpja	nhamhsvrinxpg	vcpamxcmlhtftn	2
8	mklxitc	tfpumzctswjuyk	exprkxasftios	3
8	tlaomocexy	ynkkjjrhhesayh	eaylgkrmqfoulbpi	4
8	fbfemppq	rsnmwxnynlluvdiw	phswaokguognso	5
8	nrfxofzvbi	cciayoevwhbiegqwxyoo	crzuxqkfphc	6
8	shpmgwdnqz	oawmfejxvqmz	hjhaldxgumgmucw	7
8	gxddrdn	vilpnqeovbkj	vwihpjzlekjzpguzjgj	8
8	krnwsrj	jyygorrkbsufh	dfbehhgtrhcedwnau	9
8	bobdqmg	wzxquilmlhhehbigqbd	xozuqqnotbww	10
9	ijtuszzrq	hbkeyoxgqqjlehnw	iifvxxjbxjywdemh	1
9	iavkghx	skxbgpaoyzsixdef	bxqirywxugxxu	2
9	evyqqblokp	hiyvxupzdbtoynkgrs	eqhyjodskxev	3
9	ygptsjv	igsdddwzslq	bomjteumbhhpsfgabfdq	4
9	ekmspmzfea	wlexhorumglsk	gmodnmveekdksgjm	5
9	biptkakung	jhokvedgjlsnzmfmmqno	lwhwtclkcfazqyu	6
9	gooxlns	cbfjbgfqoxvhcugiyoe	lkcabpcvcwgcsfuma	7
9	svoazvjcnf	ixcijhutjgd	nnwfxzvrwllphvzovmn	8
9	silzxg	rvhzlkeaba	hqwrxttroawonlivaco	9
9	kwtbsuguhm	smsxavietbqusvho	pevscjuzbv	10
2	hqpwuvosy	tktsebeuitpau	cioejsreouallmrwhjb	1
2	lddvtz	knhogjcjxyoxkqcn	cteadumukr	2
2	qmnagzh	qsmokswjfwb	lcrlvihudqkjl	3
2	faelfr	dprhhewqsjcdsve	shkspklvjungqai	4
2	vzglpg	hgffdorrndnsfszfkoa	oihafkcckgndx	5
2	extclapdr	dqmntscldng	mvgljxfnbfpiffhq	6
2	tetatfq	ttbsvjxbvhfsu	xkqxgqrtizeorobri	7
2	gsjbdx	xupqqgucso	kcvhrmmdshec	8
2	fucjtrfj	zxvewygcaamsizpvn	kaevoxzjfo	9
2	chisnplo	vzvrukolbga	usuuqaevyay	10
6	eyttjf	nqgmmfbxgr	pvgiebemnkfyvnunqbjm	1
6	wqpjybw	cfdgznqwvabt	pmebaukthhxfpudypuy	2
6	etybsh	xhljxlomyjditziiumx	edfkadulaadrirvz	3
6	zgetnp	udcgdkgoyvamkvusjptl	yrpeazubjjjmsby	4
6	ywxzwjlxys	gmzsfgwrswsnxj	eqqmwlwncaegh	5
6	lhlqhw	ntzdqwdsweqnu	bcwxgakati	6
6	dvvpyupca	xspiukkzbriqewzp	evqywlmqmec	7
6	ydkvrah	qngfettasxzyex	srzgoascjw	8
6	xbqigxdol	splyrjxszsodmiiie	kqslckpryuitgvxqnsm	9
6	tqsqzbjri	gojwaietjvttfunqg	jurmiouooyj	10
3	whmwhy	hvnnmupabulalinehcxt	jirdbzwefgptmoeni	1
3	phivrwwdgy	omsgjzgivxyhanvuqjn	xqmquawxlmd	2
3	didwdzuzn	qqfemywpdjnxjqoqd	ctjjuetlif	3
3	ojwgrmuifw	sbadffeixthcdsnlayk	smqobjdswln	4
3	qftxljo	sfkiqegfdfhbxcs	reqtpctfwipcfllippij	5
3	qenqtql	qvdtljzybkrec	pxitdrgqfxwnuq	6
3	thljrhb	hhhnnhfqnequ	zxxdijccmtp	7
3	alhjfmwl	reiwqwjuykvm	ewomakxkoc	8
3	ppvtexbicb	tdttzdgrjbpvriro	cmaruicvcglhloyfv	9
3	dzkvjjq	thwukjavgiwnalu	wnrsnhayowxomxubdbx	10
1	swotbb	utwvubqsuhikkpefz	vpwhmiiwxjvzqad	1
1	ntqarolc	aenneibwcgejkwpa	txiuglqcgsunehor	2
1	dmvecgmor	eddapvbccowszjdx	mgfwgtycyxwui	3
1	cayzjsdtio	wesurkukrxoii	rmxcijrbrmtewnp	4
1	zskgwbq	dvcsdvloau	nqwcundkxfuikagfyybn	5
1	ugwscdmy	jzmogdnmdoo	gotjbatmirk	6
1	vlsywc	rfspzqlurgduazqt	uthvwjrinzaxuzq	7
1	hlysyik	okzjalhyrzcxtsbvhdqn	whhxarkdvqseugecwb	8
1	ddcaijlkq	qjiusmbhvkufegpzb	anyvweicbzoc	9
1	osmgkxgssu	zpsflfeeldlzfxx	rwokxjeczcu	10
5	vrwvitreoe	otrcqtohfmmhfjmeq	mzmmgrytimbhyg	1
5	azqjfcsiw	ycrxhqyvngeqj	dasdvcejjxjkdyhp	2
5	uzcyxhfeug	xlwuwhmmcw	kxtsctaqnelhgaeqlzhv	3
5	awtfcvt	fwkqefnuattuiot	mevnewkfywdfqkzmgax	4
5	xelzyodwi	wepsztfhgtnti	mkspbxpwslyikmydtsno	5
5	ueigupb	xzbqygdeuczmtx	prlcrrnkaxxbbsrf	6
5	gaoglyghes	tefycbhbfqlwygowj	rmhwvtuseovbsok	7
5	efcbsmydh	itmzfcukctpooydsgb	gbqpaeobnynpqfaxpgu	8
5	rkvsdl	kaiwvqhefod	dnxvfzxsksx	9
5	ygsqhoctc	nfralajljknhdixkshs	phrorygivs	10
4	tlkkzn	eucsjjkgsqkilg	swldsibhkxipxbc	1
4	wonkkazby	qatuadqzppcl	kujazktprhtqtzkiwsry	2
4	lwlxypoap	zdokznqsuxerecnbqiq	oufvqkesmxue	3
4	oymnvdfit	ftbzthqrgneodtdhcwt	bzzjsnztiknvw	4
4	dhcbedta	exprshimtwropqasbw	blourzcnlzjdztbm	5
4	oxwkfeow	mnelcqnkazqtqlpz	yjzcnxpijiztrd	6
4	ozwhcvkec	oublrntbtzmawnzpkskl	smyydawqmlzkxegj	7
4	xzwxdnxkq	prqsfwsckkcbpgctpi	urbieoacpzdvrthesi	8
4	odrqvl	cdmnjdvzpvutpmy	jwqhzxexpktyqlwz	9
4	fgyshw	dtejiqcbnk	mwxutdleyifjpdi	10
90123	\N	tanngyanludeshuo	\N	90181
\.
;

COPY fvt_distribute_query_base_tables_04 (w_id, w_name, w_zip) FROM stdin;
7	jcanwmh	950211111
8	wzdnxwhm	979511111
9	ydcuynmyud	684011111
2	jaqspofube	496611111
90991	costlvmure	\N
6	escpbk	784411111
0	hvrjzuvnqg	\N
3	qcscbhkkql	545511111
1	hvrjzuvnqg	669911111
5	vsfcguexuf	329711111
4	dmkczswa	522411111
\.
;

create view fvt_distribute_query_tables_01 as select * from fvt_distribute_query_base_tables_01 where w_name in (select w_name from fvt_distribute_query_base_tables_04) union select * from fvt_distribute_query_base_tables_01;
create view fvt_distribute_query_tables_02 as select * from fvt_distribute_query_base_tables_02 where c_zip like '%111' or c_zip like  '%a%' or c_w_id like 'lost%';
create view fvt_distribute_query_tables_03 as select * from fvt_distribute_query_base_tables_03 where exists (select * from fvt_distribute_query_tables_01 where d_w_id = w_id) union all select * from fvt_distribute_query_base_tables_03 where d_w_id >=9;

analyze fvt_distribute_query_base_tables_01;
analyze fvt_distribute_query_base_tables_02;
analyze fvt_distribute_query_base_tables_03;
analyze fvt_distribute_query_base_tables_04;

explain (verbose on, costs off)
select all table_02.c_d_id<8 t2 , table_02.c_city , table_03.d_w_id 
from fvt_distribute_query_tables_02 as table_02
right outer join fvt_distribute_query_tables_03 as table_03 on table_02.c_d_id = table_03.d_id order by 1,2,3 limit 50;

select all table_02.c_d_id<8 t2 , table_02.c_city , table_03.d_w_id 
from fvt_distribute_query_tables_02 as table_02
right outer join fvt_distribute_query_tables_03 as table_03 on table_02.c_d_id = table_03.d_id order by 1,2,3 limit 50;

--test for table alais
select a from fvt_distribute_query_base_tables_03 a where a.d_w_id=90123;
select fvt_distribute_query_base_tables_03 from fvt_distribute_query_base_tables_03 where d_w_id=90123;

----
--- Clean Resources and Tables
----
drop schema vector_stream_engine cascade;
