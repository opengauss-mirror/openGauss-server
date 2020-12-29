/*
 * This file is used to test the function of ExecVecMaterial()
 */
----
--- Create Table and Insert Data
----
create schema vector_material_engine_002;
set current_schema=vector_material_engine_002;
SET TIME ZONE 'PRC';set datestyle to iso;
SET log_min_messages=debug1;
set current_schema=vector_material_engine_002;

-- add llt test for mergejoin + material

set convert_string_to_digit =false;
\parallel on
create table  vector_material_engine_002.fvt_distribute_query_tables_01(
        w_name  char(10),
        w_street_1      character varying(20),
        w_zip   char(9),
        w_id    integer , partial cluster key(w_name)) with (orientation=column) distribute by hash(w_name)
partition by range (w_id)
(
        partition fvt_distribute_query_tables_01_p1 values less than (6),
        partition fvt_distribute_query_tables_01_p2 values less than (8),
        partition fvt_distribute_query_tables_01_p3 values less than (maxvalue)
);

create table  vector_material_engine_002.fvt_distribute_query_tables_02(
        c_id    varchar,
        c_street_1      varchar(20),
        c_city  text,
        c_zip   varchar(9),
        c_d_id  numeric,
        c_w_id  text , partial cluster key(c_id)) with (orientation=column)
distribute by hash(c_id);

create table  vector_material_engine_002.fvt_distribute_query_tables_03(
        d_w_id  integer,
        d_name  character varying(10),
        d_street_2      character varying(20),
        d_city  character varying(20),
        d_id    integer) with (orientation=column)
distribute by hash(d_w_id);

create table  vector_material_engine_002.fvt_distribute_query_tables_04(
        w_id    integer,
        w_name  varchar(20),
        w_zip   integer, partial cluster key(w_id)) with (orientation=column) distribute by hash(w_id);

create table  vector_material_engine_002.fvt_distribute_query_tables_05(
        c_w_id  integer,
        c_street_1      varchar(20),
        c_city  varchar(20),
        c_zip   char(9),
        c_d_id  integer,
      c_id    integer, partial cluster key(c_w_id,c_id,c_d_id)) with (orientation=column) distribute by hash(c_id)
partition by range (c_w_id)
(
        partition fvt_distribute_query_tables_05_p1 values less than (6),
        partition fvt_distribute_query_tables_05_p2 values less than (8),
        partition fvt_distribute_query_tables_05_p3 values less than (maxvalue)
);

create table  vector_material_engine_002.fvt_distribute_query_tables_06(
        d_id    integer,
        d_street_1      character varying(20),
        d_street_2      character varying(20),
        d_name  character varying(10),
        d_w_id  integer,
        d_city  character varying(20)
 ) with (orientation=column)
distribute by hash(d_id);

create table  vector_material_engine_002.fvt_distribute_query_tables_07(
        c_id    integer,
        c_first         varchar(20),
        c_middle        character(2),
        c_zip   char(9),
        c_d_id  integer,
        c_street_1      character varying(20),
        c_city  character varying(20),
        c_w_id  integer,
        c_street_2      character varying(20)
        ) with (orientation=column)
distribute by hash(c_id);
\parallel off

\parallel on
create index fvt_distribute_query_indexes_01 on vector_material_engine_002.fvt_distribute_query_tables_01(w_id) local;
create index fvt_distribute_query_indexes_02 on vector_material_engine_002.fvt_distribute_query_tables_02(c_d_id,c_id);
create index fvt_distribute_query_indexes_03 on vector_material_engine_002.fvt_distribute_query_tables_03(d_w_id,d_id);
create index fvt_distribute_query_indexes_04 on vector_material_engine_002.fvt_distribute_query_tables_04(w_id);
create index fvt_distribute_query_indexes_04_02 on vector_material_engine_002.fvt_distribute_query_tables_04(w_zip);
create index fvt_distribute_query_indexes_05 on vector_material_engine_002.fvt_distribute_query_tables_05(c_id,c_w_id) local;
create index fvt_distribute_query_indexes_05_02 on vector_material_engine_002.fvt_distribute_query_tables_05(c_d_id,c_w_id,c_id) local;
create index fvt_distribute_query_indexes_06 on vector_material_engine_002.fvt_distribute_query_tables_06(d_id,d_w_id);
create index fvt_distribute_query_indexes_07 on vector_material_engine_002.fvt_distribute_query_tables_07(c_id,c_d_id,c_w_id);
\parallel off

COPY fvt_distribute_query_tables_01 (w_name, w_street_1, w_zip, w_id) FROM stdin;
qcscbhkkql	uzeznxwryyxnnafma	545511111	3
jcanwmh   	zvcqkejsttimeijkd	950211111	7
dmkczswa  	qnyjknerugc	522411111	4
hvrjzuvnqg	pjoeqzpqqw	669911111	1
wzdnxwhm  	dxnwdkomunescoop	979511111	8
vsfcguexuf	otgxyehwdfeuijpq	329711111	5
marvelly  	\N	pperymin 	\N
jaqspofube	owgdhnucqyqbbfk	496611111	2
escpbk    	pgldmwzurmpcsgkngae	784411111	6
\.
;

COPY fvt_distribute_query_tables_02 (c_id, c_street_1, c_city, c_zip, c_d_id, c_w_id) FROM stdin;
1036	kkgmujscyx	xufgoclgargqso	631411111	10	1
1036	tziycyxqniritnkyhbo	slkzbvftyglpthydrslg	517411111	3	6
109	cqvqpqlabayacedvxffd	lzydisodguhpmwqfxzl	631511111	6	5
1122	nozahdpmxbdhcedb	ylgwxinzrmwoviqqhx	944511111	5	10
142	ayxolcyxrisadbbmdrj	hzatmfcjsweq	918411111	1	4
1427	hpiyovgcvtdxokmgftyd	yqcfezuhomdcfdltxyxm	685011111	10	4
1444	mebroscyxh	puogupuddpchhiuzm	167811111	4	4
1570	nxgmftyjupsons	xdvrkydnowftwu	463411111	8	3
1587	tzusqoihbiebwcednay	fnrrmeqqhwcvwtngzyd	548511111	10	7
1672	yrdypvtxftycvdfuv	dsbjmjcphxpjclzymfh	095011111	7	6
171	kmcyxwyvkadgyaa	lcgvtortdeahjahv	370111111	2	1
1765	zgtncsxvqnhcyxj	kvkogwmkgeigxexijowb	817511111	6	4
1801	gozoxcftydyge	nclouprdua	556011111	7	5
2120	snthqjftyvoxxczke	kgsltsvxfbyeaa	928311111	3	3
2140	brsgouoxfty	cljjmcpzzyaidvoa	044011111	8	4
2250	gbcedyljdo	zxpkgykmebpzepn	936511111	2	7
2278	ecelvnncdkkmvoftyt	kysqlakiskqzmjlrge	533011111	10	3
2286	qmfwfftyxdsdcclc	uzzifhnqwwx	056011111	7	6
2288	rldmevqecyxgaban	zpnmgcaooutllpyfgtx	711111111	6	10
2422	lvciucyxcwhtwhh	zvensessskkktydrtnk	490511111	6	5
2431	sjufedphced	wjunxifzskgse	932311111	4	5
2589	jtxosstxfyegftyae	xzhtycnxrirabjug	289011111	7	4
2596	tvsflcfkijawdcyxs	aaywtvphiccnvgfc	177511111	5	4
2659	gftypibrka	cqyiekpjpwbsrwvl	227111111	10	2
2805	cnlsckucedzbend	rnvgczolsenswmbt	952711111	8	2
289	majzivdebfszgrtrcyxx	xjghjicgavgqlqxdnkb	025411111	7	5
2991	smlfzcyxyx	ucybptkqwdmtzbz	546211111	2	4
2993	srcediobigacvlpp	ugacuknklaokhx	791311111	2	10
320	xlbcyxfnhsrvhecnschh	gwydhgpajvelu	771211111	2	3
400	jgdoodyqpttanoahcyxb	cjvbxleoxrmvdlf	613511111	4	2
445	lbrlyjzisijgazcyxj	yjlfdcngcuolrhy	763611111	1	1
519	egpaqtkefbkgijcyx	sjerjjhanmggcnivk	926911111	4	1
520	lejabmokftybni	dytzrytkvitavpsk	081811111	1	3
610	kbahsetzvncyxzr	cnjkbiphpjaeb	707011111	10	3
615	dlzaijcyxjat	lvedrjudllzhvtcnrzn	127211111	5	5
641	fvlvvftyniokaxvp	mccospuktverqv	165511111	7	10
698	wcyxuxxgxhkdeiwwx	mrpokoosgugypuwwscbx	034111111	1	3
806	pekjrdgxzcyxylhtfy	oveqietkswdd	523911111	1	9
839	excedoebktt	wheeicmmsvu	107411111	10	10
839	iigaeniteqmpmsewced	wzqttbhwjsi	100411111	4	4
992	hnojcedaovqjtfqfklj	vbdsmxjbyuncck	817111111	1	7
\N	cantonpgxca	ttpbnmv	\N	90876	lostcitizen
\N	\N	asdfiuopqd	lacationd	90886	clockdually
1026	jcedfjimxunygsshlpy	sfghawfokctgtgnlknsm	607211111	1	7
1039	zqhfxjicrcftylegbsuy	jafiwucijmgwemfj	954611111	7	7
1048	anyuwvxoced	ralzcwshkkjyhsw	235111111	10	4
1048	flmcedqcdiyoabl	uquwvcgjbff	377011111	1	1
1119	txmwqekcedqpkwuae	jjenaewuop	852811111	1	7
1144	esjpoxqhobcedzydt	aoamdnsseeos	565911111	2	1
1183	acedsxphnedr	coqsahisfthwhvcaiadl	785911111	6	10
1339	dypbzbitaftyxotnmr	gwkjgcpogg	327111111	2	2
1367	eckjdtrcyxbzymjbv	frwgeilkepwd	586011111	10	5
1425	cedsjvprmup	kmhmpexzgspvlksev	440811111	3	6
1543	uomqhrsircyxkgm	ejoruwdoqtmz	928811111	6	1
1734	qiguftyepkau	kjaybpviympyv	586111111	4	7
1865	iiavohpnjisqocyxgk	jkjzoiselg	901611111	10	9
1874	ftyiodgcajsyntniuu	baenwhrfau	409011111	7	9
1917	ncppsvvvvgpogcedmgy	owksmdvgzzpwp	595511111	8	9
1928	xftyaongbn	vfrjkarkfx	793411111	7	4
2039	udcrewcedaklnqfb	aupclygnzblz	140711111	10	6
2115	pdgqpfbyytkwbfty	jdptrltzidkog	613311111	8	5
2136	mkzhfrqydzgcyxmgc	hzvrvryore	420511111	1	5
2172	qoffprnlcedlbbsj	ntjceyswjfphlem	683011111	7	9
2214	ddlmpsdftyhgt	gxpdwfhjpohiocr	515211111	5	9
2227	dbxcnwjqemuftyle	nulepbihogmuqesh	928111111	2	5
2227	ihvceddgqb	uoozkairih	823611111	4	7
2227	svicmgxhbjmcedsddulr	ydaxsgvtrmukkjywvuhw	661511111	1	6
2255	lynogfkvyqbcedoebask	gzylvogfyomnquhzdxt	540611111	3	3
2272	cyxgbnscmkiq	pqczfdqgugmnnazdv	757911111	4	6
2487	bsiumnqgced	qfhvjjppspbfiktgm	057111111	10	10
2588	qqjcyxkvfakidp	jiuymkbimdbmgnmgzlik	165911111	10	9
2738	bvdupstftyaapevnsfj	phzghgkwnflwj	263311111	6	5
2774	khgftyqbhdrz	epdgmdxggrs	507411111	4	7
2844	gmzevcyxfh	dgnzwtqqfaxmiziqcchz	645911111	10	5
2858	abonqwkktftymwewyv	lbukxljwxniiblaa	956011111	10	7
287	tcedwleshesmc	qajfhdxfaeyoewia	682811111	8	1
2914	depftyoqqfbtsdotupfb	zaeureendvpp	940711111	1	7
379	ukteftyfmfnvixlttz	teufqigescstilth	619211111	6	2
460	oyadkcedoymakghf	vtuqwgwmjaqgu	499111111	2	3
534	xdzprvxfykpcedd	auorhqwmnqsnlj	496411111	6	10
61	tsllexdwkqqucyx	bbhhdefmvqotqcyyx	713911111	1	6
675	vfftyddcwympcmgc	rrbhotcwngcnzybtnoqu	583711111	2	4
97	zklftymlrzwx	wxlerljixxd	966611111	1	4
1259	emhzqicyxq	anhttwtephilbtr	456611111	5	5
1267	zfrcnodftyemwz	ncnczdmvqgosbh	173011111	4	6
1279	vwejlldokwhssftygi	xddgyvmhqsgeccv	014911111	1	10
1343	vnxmhugcednp	ikksfuydedltktoljh	436911111	4	4
1358	ktphbdscyxrazbd	npgkxtqktwgkshvqq	234111111	7	10
1410	hutwicedxuntlwfhyiya	rdjxnouhnvilelp	917311111	3	1
1410	qcyxnzcfawbqoa	qbjothcvhootewdtmt	168511111	10	2
1443	avkqamvwftyhfb	yvqnthyjipnoxyjq	280711111	3	4
1520	sdwftymdrhqdcfhqfb	rxnjtxjlhuxbi	137811111	1	5
1524	grojvcyxrmoeviu	fegoaxeqkozxvr	352511111	10	4
1619	slhhxvyftyysv	lsekuzakkd	686211111	8	6
1653	ftywarmfuyrjosgsc	tkznoujghcmqzrjuealc	758511111	8	5
1665	fdrqkcyxdpwmjozr	itosuenlpicgkjqkswr	359311111	2	9
1665	qceddljuyhivrrqg	yzajdhokocmbgdsjruen	989411111	7	6
1702	bzeppwonuthzdcedyk	twbaboaasevannb	557211111	10	1
1702	vvmwmscedgphdix	cektphjosilvxfrgj	206311111	10	10
180	dgauwbnbncyxcpjfluw	cknmqnjlafwhcrg	943111111	7	1
1817	qoefuijftyxofms	axhtldosxjgpesgtlwt	391911111	7	6
1994	cedkrjyhflteryszdjxp	ftamzdlcofvbj	072011111	4	1
2061	pmhyuwaqcedwto	bfhtmpqivxq	872811111	5	3
2167	tkvcyxouwugfswav	jjjkxhazxdehofrppzd	838011111	6	4
2212	dbftyvfvpf	exszzoqhmm	054611111	3	2
2226	ftyunrfmhyyozaih	qtlcyzhadixcpnw	311111111	3	6
2252	adcwbzcyxwssrapbe	cfwdiqjpzwgfrts	362411111	5	1
2252	brzpftydxji	plxxfyvjuhat	759711111	8	3
2330	rulteqpojftys	lytqghflywziphtexzu	966711111	7	5
240	lavyqpcyxofbhecp	esocdvkbcmvuxwuyel	262911111	1	2
2583	xuggsjdwbuftypb	yhrvwxnodosuda	453411111	1	10
2691	kcftyricyuyx	wloktglatyylbjhql	632911111	6	4
2707	knhvozfbhoquicyxqx	wcbqepmdivmn	522711111	4	9
2758	gngiplcedphqhfanbrua	bolpknynbgealkgovp	775211111	3	7
2801	wlvvfrvzlcedejxy	hqfpiwtjuczpbgngaj	765011111	7	10
2835	pzbceduypmicmghwpmnc	tdxbarxoqhndp	021911111	6	5
2987	lstwmxasftyaqiklzged	qttlmkhrucwosxux	040911111	5	6
33	ncyxyxqdszhhezyfm	qgodpvfjcq	428111111	1	2
349	uftyqmznbweoy	odbpwsdmojg	414911111	2	3
397	rsxiftytskhtiaqofdcn	kpqyxslwllmz	525411111	4	2
413	msogftyprjudgkvfb	umqwtgvkktx	869011111	10	9
429	ldvezcyxcgwvbcuq	ofswflnqnoii	139911111	2	4
558	juiwzcyxiifemc	ccjuupkixndtmoezyl	337411111	3	7
730	olsmxudjftyzumxn	xnzdxqyxvp	471011111	5	4
800	nxcyxrecyyjpcmje	tsmcowntcoianuacgep	675111111	7	6
838	ftylymxcaov	gcghvgaknfxdsaa	232311111	7	6
964	yqnilpofincwhofty	qymgudsjvuu	311411111	3	10
1002	bsvggxgalqypdcgcyxmo	biaxtguibqt	379311111	6	4
1094	kegtvxpwvylzqocyxksn	ubgwzniqlzt	837611111	8	2
1094	mcedjkivkbkrixte	iocsrdqvbfbtvdoisah	572211111	1	2
1126	zpfoswcedhvaecdnr	dwkfovbtkmtb	374611111	8	5
1175	kyaawqxqlnfasgrfty	ybtpzhufzotao	870411111	10	3
1370	nxspaujpshpftyzey	vpubczxmkjdnoy	483011111	2	3
1762	lugtzbsvproucftyz	lfvhwzvstrs	991111111	4	9
1825	bauxxjohipcyxkjcak	rozmecoaqlxydewiqt	927811111	1	7
183	hpspsqhcyxxdln	jduzeskzqjungtjnpqb	112611111	5	3
1833	vvcedhsnquvdrmp	xcdebsugclflzti	777011111	10	3
1936	pmuezddrcyxgrqney	mophvdhlknoss	914511111	5	3
1989	cedegyahxsgh	jwsgonkjinmginz	771011111	10	5
2000	wdpzqrybrwgcced	xfzeuysufflrwaffo	180811111	2	7
2002	jbakwtrjarneaaqpnfty	txbhgodjcqql	078611111	6	9
2086	jdgerjcyxoumyrtgu	oigfeaqznczlrgjnpp	249611111	6	10
2179	cuvnuyvkcyxqyb	hcyqvyjqfhuv	591511111	8	7
2218	khydwuoqcyxjhkfp	vsdtowaqmkiidpvao	070211111	2	9
2347	fwxwzdsftymrrag	xbdvfsgccwwye	063711111	8	4
2348	dftypnlhqzpayq	gkmbyavqeslp	335711111	1	1
2401	cyxeuvxffnqyschai	onjghwnnyolfu	825911111	5	4
2469	cedkvsxncapzrsgevatb	zjjaumrerk	430211111	1	5
264	kxxaajtpfty	wttxfstrkujjryyuq	385311111	5	5
2641	pdpjiifftyv	vauqhvnxyflrkiuvcvcj	773011111	10	6
2678	ilfcxacceddlezve	aukaerobigsrubznpp	459311111	3	10
2710	wiektmepcegqftyxjeqv	wfogchynykq	150011111	5	9
2764	lttiftycjpchbksu	uovtppxfdiysgbdqaoi	769211111	6	1
2880	cyxhpitqczyiqovsf	tjoybuickkhvicswlcod	286611111	2	2
2923	culqebkzzccyxvhcsa	dsbspvjysae	741111111	8	3
2923	ikfhdgakgfty	endhnupwpi	183311111	3	6
299	amgiztcedmrhuojcbjb	ruikngddpmkimqrxk	342011111	6	2
306	vcedwdikkiz	fhvubinsygd	885811111	1	6
440	upcyxfmxlcru	cpmcoitpqyp	360911111	4	7
758	ljirdircyxummfwlnwh	qahvrhfvlw	336011111	2	5
1118	cedqhbtjcqbvwtcbqdpj	opzoqpatdby	843611111	3	3
1194	wfjdpmhkrftytmvysjt	yqkoxljqhxfkxb	864211111	4	3
1197	gcjijlxaupxzuzvced	rfklrxtbjidekkvbwr	024011111	1	2
1210	voocmrqokevnrfty	omhhxmfjgsreizqjpu	054811111	5	9
1368	smmhbeediylkoftyxoj	fwcvwshzejfnyqtejzon	463811111	2	2
1382	bgcedyhmyxvymjle	ljszpaqfzjocpw	198211111	1	6
1382	oxyqoccnxxxftyenmm	vrkvexlxtdktjjcjhmem	763011111	5	7
1426	kehsnhahdkncyxcl	qakgkncykbbyrxwkszee	111711111	2	6
1569	dhjacyxzllziqcwnakf	jcdrzvwodfh	237211111	10	4
1715	psyojftyzxaxkmoqhy	tykjnufkdeeywquig	228811111	8	1
1719	zrvuqxbgfaieftyqh	muhdrnfxwq	953311111	7	6
1754	tlwotqeftya	urifuhywipnojkuac	885311111	4	3
1925	nertgeaftywac	mkcwheplhfeeugbk	943511111	1	1
1937	uimctoeccedkdufsa	coiwxfercysjxkibv	202511111	1	5
1945	dpatcjnqhmqvhpfty	atkcbfubchjeyjvn	009311111	8	10
1948	ojiiftyhamliicoxy	dqmfwwddmkkahwhhzgyn	885411111	7	3
2037	lzqcedrdxiazekrmzpwy	lltzscpjbgprpibizlgs	712911111	4	6
2150	jxfedqrumftysvvt	ljaddmdmvyv	627111111	7	6
2263	wtiigpfzcyxhwbpunpbq	jyipohhclir	611311111	5	2
2271	wcedlxdxhffu	gnngxvixqm	829911111	10	10
2296	egppljhcedj	gcsxhwnxxw	835811111	10	3
2319	epfnsuftyazpholvmk	wvlddazqmqv	798911111	8	2
2319	myuonlqcedvadtazhvq	ydawncdbefsjkyjhfjnv	814611111	3	6
2323	hqvqenxzjzaced	ehigghixfmpjapcimy	958311111	4	4
2323	cedjzboafsbysedrara	qkzwxkyjivid	963011111	8	3
2326	fjzlecedzaq	tvmebnkmxbhliluyrjyx	997311111	2	5
2409	cyxeqicqrefslwefijf	ogcassagjylk	402011111	3	10
2466	ercyxqwyqpjraabwvxqp	rolzxihjdivmxfq	552411111	1	10
250	mjwlptybqthfftyfeb	zjtjsucrlvegi	570811111	7	5
2501	cedkxhwpelg	htawmkczshxpwgjmkx	297611111	1	4
2501	cedjztryegl	edfilmlunc	502411111	7	7
251	wzdkftyzrzl	qxdoewjaibtpon	285611111	7	7
2561	aqivnwftyrf	uqrlwaqrwxheo	294311111	5	7
2570	tcdokcedcvyuqqvse	quhcrcbjdrg	755811111	6	2
2630	cedgzbisutptpq	ogrqqzzxnwd	197311111	8	9
2631	kfbiopqxbftydufwgii	lmrjrcztvwopoqwodku	510711111	4	7
2791	azrftyhcznyjbct	hkfgnlnsuswlgxhd	057811111	5	7
2831	adxftycwuosgjrmg	ffvrqqqbcbcscdkdcdwr	510911111	10	2
325	mciftyiyfnoztg	xodvzgbfqdrjhjhyeft	601611111	6	1
402	ynnoftyygrgaqe	bqzvwqnwotasbfc	868711111	1	6
527	vizfcyxpufiehrigcxzv	oephulscaumxcbdykh	010211111	10	5
665	cxczywacedabgs	qbuykrkdru	850211111	4	1
690	hmplocedcxnbpdekkl	otitdrfesfkg	574811111	3	2
690	qzcedepjffziqrnuup	hqnurrhisajyb	799911111	3	5
76	tiwuioyuoycyxxsbg	pwlgviawtwe	096211111	5	1
768	ulqdkicedhtxpp	coydgbewrfagciyetn	774511111	8	7
783	elqvaahviybzhukxhced	gdkuouohxkc	360511111	10	2
812	rgrkfoskjogeesrcyxo	tnfnemeaijrb	469711111	6	3
832	nfzcyxcghpfhnzot	qkckifboapmp	342911111	7	10
929	cyxdydkbamyzn	ljxsojefoomzwynmrhvx	972411111	10	10
1033	dxedzachiruhcyxdsh	abzimulbemszge	336811111	5	7
1283	ajlblggiftyldj	kleyacczicatjnq	403711111	4	10
1392	vbktdopbsbmewftyfgyn	tlmwucpqqpoux	500811111	4	7
140	sftymujjkicrzku	veunleikavl	567211111	4	3
1480	gbdlcyxjyohkrtr	rurymksmyheflsxru	936011111	7	4
1497	qcncgudrcyxsjotdwwb	kzethqcmcprugqjfp	612911111	7	5
1527	swaxppqklzcedwsa	knfuwalzyws	045711111	6	9
1725	cedmiwinqowppydexksk	rxqrzvzlugfei	851511111	8	4
1741	lroscedmvbkg	jxchuuruxtspwiejcbre	498111111	7	4
1741	zyxscubacyxmvxtb	srrfabddvraredisqbnw	628311111	1	2
1768	cedjdcalmzqnbfbbcf	pcgmjrgkpcxrigxpi	240011111	10	2
1768	hwrngtyacyxetpivafia	nquxliygmivvacfvb	258311111	5	1
1879	tcedqqufcsn	nqmudhnqnt	098411111	2	10
1889	yvkcyxszvqxi	fhhkvswtosao	842211111	6	1
2036	ftyakqwgvbhfryzu	kupxwcdznvjm	997911111	7	9
2063	icedvmlnertt	kdbwialaod	987611111	3	1
2482	bzsltyatfty	gwyeorfnedsl	511811111	5	6
2526	uivkrafghrkdammjaced	ognyspjgmacf	372111111	8	1
2755	uaftylychz	lpmwqdtjftpcgtfljvt	437511111	6	10
2806	cabhzgcjqcedvongne	zqpdvuqdmxmepy	989311111	1	3
2806	syhqovocedhhzy	llmgbehkemtvbcronvrv	913111111	4	9
2808	kyunoppmmprkftyrg	uslwbvyhokb	846711111	4	2
2829	tyyjyplxcyx	ojaacjhsusxcz	660011111	6	6
34	eutefcedyuciynb	sixumipymnvocd	200511111	5	5
368	lkpxcwiraqcedxpgqrsb	czqkctqckaleqkycivq	769911111	2	5
420	ihpqynceddf	xjxyxkrbhc	410111111	6	6
458	sjcedpvuflekecupm	wqbwchawhsdfwoqkvvaj	367111111	7	7
572	reefaqfmftyctm	ovwytsrbhjdr	998511111	5	3
600	lcedvtzyvrcclydttg	gjxwbgrfwjfdqlcbk	136911111	7	7
634	ftyfocptrhkjbciqlifw	iyggrzottvztpji	341311111	4	10
634	wabdzxakdzaakkcyxnkw	slagmrlhhj	042711111	5	7
694	naftyksyzzv	swxmswzkvtwx	075711111	6	5
908	dwsvcyxodfpn	dgjjfccqcujd	015611111	2	7
970	lycyxcqgiacn	fprabefzwzpcmpuvex	680011111	6	6
1058	kbaxijthomcukvalbfty	wbyawshypspyjvr	535211111	3	10
1098	zcgwizftykcefyz	filmadcpatvy	419711111	5	6
1143	vqcyxszynwnboowktq	zawuyudlvxhntj	225711111	1	7
1198	jicyxyiroowznristmpz	epyhlnokrkydknq	657511111	2	9
120	rfpxtulhavcyxkolal	ronadpvspzjodll	354011111	10	2
1204	xzpasonipjoftyydk	matskilkvkaitrgczhj	114211111	7	4
1206	klvtbgwspceddplhypl	nqiynchpaujugcjjfm	680911111	7	6
1244	uwfpeaftypm	ytruebbxawhgdutls	690811111	8	4
1265	zpvuylsfyuxcedcqi	jenuwujtgh	260911111	5	4
1450	wzvcyxxqmbgkc	bipmbourmqwrxkpm	319511111	10	4
1534	pxlgacedytfh	xhupcypfuxxbwv	112211111	2	3
1616	tluoftyhbhif	kszjdzkjph	957711111	6	3
1624	mceddqknoqxdni	wnjqlnddpgllgpryt	108611111	8	6
1624	cedjkiloccd	kdgdllsknunwegvpgcy	552511111	1	5
1648	dcqmckcyxxvnfwrg	sohtcqknab	554611111	4	5
1690	rhbpndkhjgcedwsrauh	pymsbffpmxkeiqrnj	074111111	10	2
1771	maftydvsjmfydhzzdht	fiynjphseek	449011111	8	6
1781	lhwqmiqqahxuftymd	txukidcpckv	366411111	3	2
1811	iunyoaadgfftyvdrxtg	uhbhosmlovxsvkljm	073811111	8	5
1838	dmlrgxnkwziftyolvc	esxcqyilyrymtktdqs	581511111	5	1
1887	jsvbaosftyorxmvoksox	btfkpcdxljhlh	716311111	5	9
2	zujptddcedguji	vfczcudxxmvmgm	233511111	5	9
2053	flsbdldqzcyxyivev	mbzosoyilgnvzcs	808911111	3	5
2075	cyxwfflbdjqxsqrowzfn	qcxiryqdoyddmtjyv	000211111	10	2
2158	xjiipzhygugscedyws	vfyvjlkhbqqulxik	245711111	4	3
2168	xxyaeygezaftyry	buluazyaolefqxgr	034711111	10	2
2266	hftyzxiljgaoasfmnfa	gxdbiykoer	689711111	4	1
2369	czxzlcyxosojv	znzsiutusicxvsfslkt	407511111	8	10
2530	ystxormcyxs	xlklfqqbcgx	389111111	6	9
2563	rgmmiqginrwjnqocyxe	joybdnvirzmslg	509911111	5	5
2885	zdsfvkcyxyjpbeuyzxch	utkdnmfadzibpgz	956211111	6	9
292	dqemdcedoiwtcqgjhf	pfzarbdkvq	481511111	10	1
2992	mftyxjmmweggbgf	kycdyzfgpefgpwd	506111111	5	4
345	xdfyiqcyxxkqkxll	tkuftoblrmmayampt	883611111	8	10
351	kyffgbyqdzeqlscediq	jopfuaseiovpn	599111111	5	3
351	ellcyxhooxdmszzuajv	mxdlwebmopqwuplzxg	100611111	3	7
442	qqlhwcedch	jfclwrkwtfjs	298111111	2	3
56	zcedsocxnsnef	nqxjobbaxwljxuy	058911111	8	7
619	oqecpcyxixjlqlsee	mgztrzyksnhvyzbl	523211111	8	4
789	ncaydnxwcyxnpwuutcb	nvdenhwgczotyl	947911111	2	10
831	zftychcjfdumdh	uazwuhwigzzcmc	507211111	1	5
854	xwcyxemexcblijnaleo	qvcwhclutxanvoqhbxcn	318911111	4	10
90871	\N	\N	daswwwqer	\N	massiveas
1053	eszcedmbfvqy	boiduadvmkgf	708511111	6	7
108	cyxfrtobfqptjzx	wmqomnnvgdaddys	100211111	6	6
108	xhqacedixtkfik	plgfejqxrdodgshor	506111111	3	10
1192	paovccyxqf	zfxfosjuyna	956111111	10	7
1312	mjtgcedkdcw	iqzyuagngcowikd	510011111	7	10
1336	guvkqpicyxjlmzqhkny	cgbgxyiivyamckhdkcid	841011111	8	7
1379	hftytxwevwwm	zczywofasdizm	962711111	6	10
1387	nihcedfwzzvths	seofvgzswhs	258011111	4	6
1446	ajxfcqghcyxhz	jmqpxbxlzcluqpzhsml	859311111	2	7
1684	blghcedgqu	sjvuufshjrhlxaj	576311111	4	2
1960	cedhedsfazhcg	bgdgxxbzwwujfm	519811111	10	2
2100	ahftywgedtmqsyexu	oszkbsyjdpvj	284911111	10	6
217	sbevxbstcyxlpoxm	lsmqgprsknlozvakqwv	770711111	5	7
2193	cniftygzstv	givkgjzzbqcvhxt	430211111	1	5
2195	djrbqqvyagkcedq	qhsrpvjxolzpfcr	138511111	6	1
2327	cyxkcuvoebjtwhrwt	evuhyqovmfifgbonvpg	300811111	7	10
2490	cedpiekbebuxcegcfv	oswgcndtze	687311111	4	6
2490	tprxokmqfbmgoqftyjjm	bwpgoldilfu	647811111	2	4
2504	kwcyxxqgaufrlmzazbwt	vnmvdmhxapwmaue	897711111	1	4
2504	yjojnjajcedbcrqhwtsd	rfmqhlyakivapwtnv	186511111	8	5
2521	jofcedcuvmfxqpzjuync	srstuontyfwnhb	878511111	8	9
2521	bsqcyxhhcuwchtpp	vqnpkgzrdoqqdubjk	140311111	8	3
2553	atfpvgltwhqzybcyxdc	cdavsgpxeotqwlw	985711111	7	6
2623	krlkwwyszfyftys	qtmchhwyoeyarajb	303111111	2	3
2652	hgpccedkpleefbvsmwoz	snrqaxlmjvovfsaxiw	735811111	2	9
2668	ikaebvyxubatcyxurt	pokiboftgdjf	035511111	8	7
2750	wmmuvhylpcaftyxjj	lsqidjrhnsdaftfe	114211111	5	10
2765	osicpkjcyxqixikj	bwohhuvsqz	182511111	6	4
2765	gsmbcyxyhrelqcvb	tkcpyanldxpejkubzn	165711111	8	4
369	ukepcyxhqgsukzi	rbhsipqcwjd	159311111	8	1
438	tcedkcbngwv	wwjzeqjlcgbuo	809611111	6	7
560	diezricyxcxzuqlahb	iyygvxtgsiywu	192411111	2	9
643	ryufxgcedocvbtdeeg	jfvyhmgposserqjqak	280511111	4	1
667	ccyxljsipnu	upvhyvorwprpvotjcsih	280511111	6	9
68	nwzxupcyxjn	qnhpnmahlgtlmdkldq	554811111	4	2
77	qlpcyxvkyecvjmkv	kyqnozalquunjjya	632411111	2	1
93172	oiuyeacya	\N	hwaopvcx	90909	\N
103	lrhcedkfecnkbmlf	desogvjcyelimjyjwmjp	787311111	5	2
1142	wnftyozpavmgnpggf	nmmlppxzkxgwmwz	885811111	5	9
1142	lzzodtosdfryvced	yljsifcxiizh	769711111	6	7
119	echnjwfcfhocyxfyeal	mpnbonxjlix	198611111	1	3
1326	mkpldhpqftyzajfoqzef	rxyqsnrhyoplpurdl	427111111	1	7
1438	vjgkjnwwcotbuncyxqt	fkjqtjvzlrdsik	400311111	1	6
1476	ruqtcivnced	ehprhpnhhxnyvz	483211111	1	4
161	tmxzmcepuneftylmi	qoymazupnvxdttuov	316611111	8	6
1625	ejjfzwglctcedduwhorf	tmesslnaak	486911111	5	5
1682	lgupmdziazvftyaqdmn	ymaxnvxabmvrrvvone	490111111	7	10
1724	uwoyhlmtcyxj	emebtovwkccorm	187311111	1	9
1794	edjicyxlilpcyfksgpd	knzcmitoopqfrqbrueq	830411111	8	3
1841	eepluchjfty	njhjqmvnnjephqudywhh	367311111	8	4
2031	rqrwjrsftyu	ubckogwahdud	644111111	2	3
2031	qxxavccceddghfhk	tsprodnmtbjpgbotbrqz	607411111	6	6
205	idyecedfrbjpowbcrjy	hyfwqswvjly	997411111	3	4
2184	uzjiafrulpwymmppcyx	wwemuxddekdvkxhbh	118411111	2	7
224	uceddlymyv	nirgkrdjboeqifmlebge	414411111	3	2
2262	ylihcogyzfcyxprqgbb	zbajdpmojnlaz	374011111	4	2
227	rlvsfovqnxftyc	bqfwrvdudwnqnxs	813011111	2	10
2344	kzdgoflhghftyr	cssnorgdulbtmfb	283511111	6	1
2491	utnxcyxjrjsxrukdelk	wvbbygpircmmjgor	180711111	7	10
2543	npknkffpqdftymc	dflpyjwodscf	567211111	6	4
2587	icedtzqsawtuxjpfq	aleiicjqwbaxnpzvfuhz	605211111	5	5
2591	flafkddrzrcedzpkr	gowivrhushzjehzmnni	450811111	4	1
2695	nhpgiugwftyhftgghfw	aigsmeiowseloie	882611111	10	9
2743	wolkclcyxgbqjzn	pjiaimgemxlwqlqgx	758611111	8	3
2775	fnfzieccedpdxdowvoev	puoifwhnrkkauj	820011111	5	6
2798	cedsuwysynagpzh	puuadlsbbohvwbhb	002011111	8	5
28	naeassocyxnpvs	stqdkivyeefv	553811111	2	1
2847	cyxtosaacfkvboxe	cxhhvxwcummdm	344711111	7	1
2861	hwxtnhfceddnjtmtaqzw	dfbrlezctnpumo	124011111	7	9
2881	hmvwgscyxehxzzmmynh	zpcvdernmfvz	487511111	6	6
2881	cyxzipspefvqg	vwfhrkhdbzszthxjtv	714811111	3	1
2919	occwerhqcqasgsyafty	cdplscqfxwwqjjrqhph	042411111	3	6
2919	cinbcyxovtsdyppsaasm	lhtwidvpad	090911111	6	2
393	ujcyxgmvidffbnfu	zekxsfqvcsdeslj	676911111	3	7
409	fcedbahblmzy	alssttjwpwb	769611111	2	3
48	cedghmwtxzm	umxrejtkszd	506311111	3	2
505	prvcrpngocyxssqbnnyv	mhoigbbotieukyft	142511111	1	6
516	tjiinzhiduoftyao	msmnninoxl	603111111	3	1
562	ftykgaplkx	roituxmfuayzxmkyoi	960611111	8	4
646	phicyxibistz	bqowvquqzc	721411111	7	1
688	fomtlcbcdtjiwjfty	quipevjyxfrx	240011111	1	10
821	brstficyxctddwxp	luhszfvmwmulmcnlf	292211111	10	3
881	sbjtpiuqzcedebzn	ulgyamucbmqhgcswn	234311111	2	1
912	tftylafemreomwbmsj	fkaukacsnsfkszln	563111111	3	7
100	vvkvhmgmftyuxuqmt	gvkutyeeulvqvbbmuds	063011111	6	4
1080	ldfplrlovncedxet	zbghzbfapu	151211111	5	7
111	ofkojyjycyxmmoybqyk	kuizcgirkbmpmtrmdzkh	439611111	5	1
12	eewmfgftyc	fbmzujngjnjg	140811111	8	10
1262	jzfyapnurbcedleioa	wfrjnlzhiucrfell	576711111	4	10
1280	mpavbxxnsvokemobhfty	nieqlhmimhrujqpi	994411111	10	2
1286	mrqoqscyxtvcreqpy	glcijmoplwimztejb	637211111	3	7
1347	ctbhftylobnubresjq	tatosvapfjb	880211111	1	6
1357	rdihtqrnckpzftyoatt	mgjtodmqyihbfzqytj	742211111	2	6
1395	kcedilqqluut	updwfhtzltzpmqof	634611111	7	10
1474	qowgwqwced	gazucgzvvvliihyck	160711111	3	9
1474	gstpezcyxzqevygu	jhjmoojdian	962711111	3	1
1518	pdtfvlftifrcyxmrutkh	hthndwerufpi	097211111	10	7
1533	ydwcyxnojlql	hmparzgiafckxhxb	506211111	3	7
1556	dvncyxfoxqdxygwwqav	chiwvxrivdluy	858711111	10	2
1589	recyxmqqjixjhhuicgca	fsugqfcvfyhhmbikwtyy	537911111	6	9
1675	mzcyxkfzagyuwdvhuxu	tnkbetslwvzzvzmjg	675311111	5	9
1677	lqcnglwcedmwubnvb	fydoujyufowm	725511111	10	2
17	uxcmwwvftymgffghsyu	abwdqfbbfnllafubzqd	013411111	4	2
17	ptmbcyxfikvk	rupxkauczdjnqeaxqvba	600111111	10	1
1742	bpakxgvoaxftyrfu	jxxotwuhlucu	390011111	2	4
1804	jyoswvrnzjpcedvd	olnyitobaqrodjrdn	493611111	1	6
1836	cedszyfcwmfmiljldwry	zlovvujajojsxrqnh	650711111	6	2
1911	ccedxqtwijnvstkzkab	pebgzgvvun	425011111	2	5
1972	gftyybihcctahzfn	hpoyrtmccc	387711111	7	4
1982	mcedlxdivrdaszdh	hmfcqsmymdgdtia	651811111	3	4
2018	ijsftychvbrdgezraf	gidfecpigws	654711111	7	6
2095	ddfstfapdfluwncyxjy	bvmredpxjogihpgihmkj	406411111	6	4
2103	bdamztzhcedvlu	eixlgzwbqi	811611111	4	9
2103	qjlcxwdcyxokyhlwws	nqlwcdpkznqv	124611111	6	4
228	ecedqciojamcmyez	zpzpfgqaluzry	913811111	2	4
2285	wrekhzcyxlamtrewpnp	uipwxjagupxnkrzelqz	354111111	1	5
2338	cyxoinxryvodqfybulpa	jckwllxevlxgpmyu	319411111	5	9
2339	layukycyxcl	faygatbjqlasfpdlscja	964811111	1	9
2448	qtbciyftyfiq	bojquckllwbufqawxzjg	016611111	4	7
2455	uuqhsxqkhcedp	whtuiyajbgmxaor	460711111	5	3
2494	lftyxpoubiny	dpyjkmymkodnkve	083611111	1	3
2562	xyftyzlxvnsm	bvicggsclcqziym	829611111	10	6
262	bcqowppxftyn	uezronipghz	317611111	1	6
2624	hmsrmjcyxgnjfkkc	aejeramksjc	170811111	2	5
2629	skeeiueoscoqbcyxnj	anbtvryorvopxaipoutk	812011111	2	2
2658	puvfkdwftystplcgteq	iobgxgthcnenpjjholhq	697511111	3	1
2713	ivceddqlsuvb	ifpvcmhlxomoh	431511111	8	10
2854	vdxcedsjbxt	pmhdhnzzisyyezklc	332411111	8	6
2896	jjekcyxtfja	dtyltnzjzobsvs	842311111	8	6
338	rlcyxacotpwxk	voorlypihuooljndoh	248011111	2	5
383	ocedhbcwrdp	ektmzeaiazvqivi	137911111	5	9
541	dokydjhvrcyxolo	vcdoptvfyhwskspt	948111111	7	5
633	fvcyxtknyhvow	giinclqalk	480611111	2	6
894	iqccyxsycqnnecnobph	mxlqvdzqmpd	905211111	5	6
900	vayrlyzcedmicilych	eljytwbuwkn	114611111	8	10
957	jintndklaycyxc	gxaskrlxxfbqpicuc	809011111	3	6
11	gvfocmcvjngqpocyxsg	oidsxfcbehrb	837311111	2	9
1111	tencedutfrqsfglko	qbmchntlfkejc	955311111	1	5
1304	otsqscyxak	tknvptcrzis	067511111	4	1
1371	socycedtocdeenwt	idsvrfysjhkpzruh	175311111	7	2
156	sivesimhdnlmttjftybh	xrqesfysjuutmdf	553511111	10	3
1738	ukmqokxydvurucedyl	jcluzffkikxftr	850311111	7	2
1776	seqywuuwahmcceddiga	okvgklnxffuawlvbnxn	427711111	4	9
1815	dlxhkdwxpuftyeda	mgyancngnmayxwe	195811111	8	10
1862	swprcyxwonrfrqo	chadmwhsao	236811111	7	9
1919	ghsqnkyeivlegsxiwcyx	hfhskcubmi	798111111	6	5
1955	lefdlzwcedhzydy	fbakymrhrjzkalpdpcgd	765511111	6	3
2041	lcwoprftyqwmirzvvcjn	ykdizfbnwyinpsttzqf	595911111	7	3
2090	utqqncedaslkyaycixjk	ifbdkefjkf	076511111	4	10
2178	qteisgftylnsrh	aucmkmrynrzflrchjmhf	682111111	6	5
2199	adgyntuacqftyjyggkdv	todaspghxeqncara	548411111	3	2
2240	iguzaubcedkflrkvqg	elbhytuoidrnjr	520611111	8	4
245	ftyrhaotbrugylpqgsq	dmvhowsqjchgvgjjuqdd	300711111	7	7
2461	yzglvypkhqccyxcqsc	ukmomxhyxbsjjttdgnj	316811111	7	7
2499	mdiucgmnrypcyxdtb	rgrtewhjkwmmgvattk	327111111	7	5
2564	glkowmikcedsikx	voxkroiyjaiewtg	359611111	8	10
2600	spuipakcyxvbjxhvroyl	atsrujgxdoiavi	467611111	5	10
2633	brenqvzjptfty	jbggmmmthfoimjcsmbww	665211111	7	2
2699	bxocyxpiin	jcqkpdnzpscwvhwhpk	603311111	6	5
29	vvhimihhcyxf	uojjelrzssep	311811111	8	2
330	aokaouuzicyxpcihi	vtvozwhtqevzbajmq	843611111	7	3
370	dbtekslzkgcedovzplp	kxehlllgrrkjsz	818211111	2	2
376	owvoafbcyxabvfalalh	kewxqfubwkkb	646011111	6	1
480	cedsdgqpnxuko	dijpbkceeqxpf	162911111	4	7
50	ocedlkninihebuomng	azvhqkucpvxqlcnpf	619811111	8	5
528	yzyleftyzkgyzxd	lqmpbdivzxxqcly	040411111	3	10
608	tvxgougnnpdjftyrvlfu	ysabgqeyoricpa	927211111	8	9
639	cedlquhtysazz	yrgmqqfyyqxjjqiaovb	850511111	8	5
652	yolptgtquucedbgfu	mdzqtdvsowdimo	923811111	6	5
684	adyaihcpgrdntfty	zcqfiwtkhztonnamtah	198211111	3	6
684	dpenisijhvkdtzeced	lzfngwywyw	993311111	2	5
722	ftypywkespg	rlgxglwrwqw	095811111	3	3
853	xfwpecacqcedldnsgfdz	kfprruefkniff	721411111	10	10
1022	swojlsgccyxzywh	ujdaduehztamfrnoept	920211111	3	7
1027	sltbecedhlbvoyskvc	xszslxprwetdl	233011111	5	1
1079	hydqwcedoxvws	gfkdwqiryusgtjhsm	740811111	10	9
1083	txszvrxcyxpccstobgti	vxfetnpwkun	151411111	6	2
114	kftymjawqx	gwodjiataa	449711111	5	2
1141	cyxdhodxoumpeni	liyspjvejsi	973911111	8	2
117	rjycyxqraqbmsfvlau	bxecybgpbqzyixzr	439811111	4	5
1186	ehlerswazsscyxgm	eqftadbpswsdyk	269311111	6	1
1386	coikcedaxoz	gxcndgasnigmz	111911111	8	6
1422	tvkrncuarbnkskycyxlp	eyvdjiwtpsdqzxxfv	805011111	1	3
1422	wdvzlrlmdyrjhdcyxnuz	pzeissxbtqqh	464311111	8	6
1469	pptlnftygnsmsvdc	ykbqheczrsjgiihkgulo	878911111	3	9
1470	myqjulybsmqcyxzaf	irycddfcwtz	052211111	1	1
1526	bftyqfjhvhrxbmmdpqax	vlgdjkxdflph	858411111	1	7
1632	spqccyxnzk	rseoisewqporolpbjwe	003511111	6	7
1658	dftyryekbunvzgrjj	kkxlvsaxpsbcxmhim	723811111	7	7
1658	tjjapcyxjygn	rtzaxdwadphoi	522211111	1	5
1680	lroutftyusxlyomij	dadqjheyrxy	685111111	2	2
1766	tgbfqwmkacedrox	szulezyyiyfpktbgrow	167411111	7	4
1775	bewaczhcyxbzspt	crgsqsjaylowhz	319611111	6	4
1921	glaodihmexgkcyxeydr	pfqagqhpxorzdklmxq	060811111	10	2
194	hyxacedjzgodvbswql	dmrjyzpppcz	063411111	6	3
2078	cvxftyqyggz	gtytnhkeww	503111111	3	6
2079	qgcedmaqnh	wxhguqoktybekexh	808411111	7	5
2133	eosybijvohaqcedp	cjsjbcxevoxqhyl	693911111	10	5
2156	nqqkintrgjyhnuayfty	sxiicrysxfltzyysl	711311111	4	1
2241	riijcyxopixxyeumvfgv	usswkqpyaqt	002111111	4	6
225	ufqiylltjwwkmgzojfty	mgqqwkerkthsjbxsxf	493611111	2	9
2282	jxvjcedxwjk	zxmygszxmwipaiunqt	192111111	1	7
2358	pftjftydrsoll	ncohnhjkgbkpmihgz	113811111	2	5
2371	uidooqcicqfruczfrfty	zrxfuxjcwefpbsqhqck	351211111	1	5
2425	escwbqdnbnovcedonccv	qzcnnpnnfso	994211111	10	10
2425	pcifeqfmulsmycedw	blygbighclluykognvs	341111111	3	9
2425	ymamcedxjishxsz	droxecpnbvrimksiwkp	821711111	5	9
2479	lccyxogidju	ipmvmmvqcg	152211111	2	7
252	wcotewydcyxi	szlrksudhbyg	356911111	10	1
2539	melwsbtlcedbc	ykuyusypvaouj	614411111	2	9
2689	zbaeytizfcxbftyna	qhzzfzpidb	926211111	2	7
272	sacyxyclazqktepl	ujqyarfgddcx	985011111	2	3
2757	hamejcedqvswvaddylqy	bsfganfonhcts	114811111	10	9
2928	hscjnrcedyaesvwksk	ymfdrujbsmhbh	737211111	1	3
2931	ftygxkhjku	sdtffdfzpwgtdcyxf	321211111	6	9
2951	vmhwgfgsxftyjogx	opbcmixtxnjiqv	391711111	7	1
2951	gftykuusshne	igwiyzbxrxrisqk	179011111	8	5
2954	yftygkxookprmusyogz	ogqvueyjfkonh	845111111	2	9
2974	qhtddftyxnqv	aftaudwageyikfuniqae	027811111	2	4
336	kjnernrmrrtiaqwplfty	cvbsugeper	561311111	10	2
435	pmhjtlxwbopsftylymz	lsculrauej	335411111	10	9
455	sgdtfmzdcedlvil	ypzmfbseljvmtknyzdp	843711111	4	5
537	cedpatlqkdelyzt	jxsbsqbkcz	853611111	2	4
594	bolfxudwoxyrcedptgfi	unyuyjlrxldfazvnhvg	174611111	5	1
693	mceduogudu	guxzkilyzhhefmsik	006411111	4	5
824	dgtetcwcyxa	vbakzztutjfgaa	276911111	2	6
859	cyxazqxikybck	bgaytpbbzboxhflwg	752211111	10	6
98	udaecyxekvpoasenoj	zivnbnhvtigfaev	161011111	2	2
\.
;

COPY fvt_distribute_query_tables_03 (d_w_id, d_name, d_street_2, d_city, d_id) FROM stdin;
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

COPY fvt_distribute_query_tables_04 (w_id, w_name, w_zip) FROM stdin;
7	jcanwmh	950211111
8	wzdnxwhm	979511111
1	hvrjzuvnqg	669911111
2	jaqspofube	496611111
9	ydcuynmyud	684011111
90991	costlvmure	\N
5	vsfcguexuf	329711111
6	escpbk	784411111
0	hvrjzuvnqg	\N
3	qcscbhkkql	545511111
4	dmkczswa	522411111
\.
;

COPY fvt_distribute_query_tables_05 (c_w_id, c_street_1, c_city, c_zip, c_d_id, c_id) FROM stdin;
1	ofcqytstawvaztbbq	hembgyltsmalafod	524011111	5	798
1	ecdcycotzpinifbnwhb	qxpqzdxuze	347711111	5	977
1	icrnfllzuxezhcjzsl	xgchchrgmn	929211111	4	1650
2	ihpuuwafwesnzxwfwda	amgikquzsjp	081211111	9	801
2	uwoonoovulj	gnotblmkosrb	489111111	2	923
2	rximcrqwpc	wiucedvqtlwbm	682511111	5	1077
2	jqrtjthvyep	vpljwdopkynwh	265411111	9	1166
2	molhjugdicvnvivxtpsg	cptdjhfnjyjiqy	513211111	6	1412
2	niknxpqzebtkpw	evjkalymsphlvya	550911111	8	2034
2	rkfhoatrpdpdnnfg	kyosqjjfjzs	633711111	6	2087
2	yftdwmemmhzruthmbhb	clxzcjiqbydaxhjqlq	939711111	1	2542
2	kzueilunytbwx	uggbmpdacfux	500211111	6	2679
3	uqnznludetftdyt	pfwhxovbwslcyshx	047211111	5	19
3	vkdlepzbneykxybed	nxwkpiqnfcfpjiqj	341011111	5	102
3	ikcbkxqkvonupdk	dlchzkiuyzhgvny	187911111	2	498
3	qabgbszftcpqbphuqcls	gprehdvdynkrfdbyr	874011111	6	1733
3	qnyypnvuoxyryqiqpxdz	twencuefze	813111111	8	1820
3	aqhjajeycgyxdib	anuphivzgjwb	746711111	4	2275
3	vdfgsmoqcbtsjekpxxsn	aborbmyaluij	666311111	5	2581
3	hkzdmadtxdwy	dtxmvjoenadn	917111111	6	2848
4	bkrfuiqiajcx	umpgowdiljlxrgp	948511111	5	1032
4	sazaxhayvwzaz	docfqjqyozjmv	694611111	7	1520
4	fdjmauobashiafxccw	lzoasoamxv	911111111	10	2232
4	tmtebfoftfuww	kjgxsxfyoz	252511111	7	2266
4	rwfnagcypmsmgofqz	pjoztokikfdmendz	510211111	4	2856
6	sggepgtxhdbkg	oidbgaqjvlcjfe	298511111	10	879
6	paybxpblzzgvhhymbk	mldjplwwpjaiqh	044311111	1	1180
6	kmrfcrtrtotetbhrrmi	gtcnvmcmtptlqkdm	032411111	7	1266
6	dxwskbqlxupyzko	evkmroitdrv	053111111	8	1443
6	ndftwjpmuqrtlvx	vsynikyuyjsjzvnfofuy	477511111	8	1597
6	eeymxmvxxmxi	lmuoadqmiqgqsu	340511111	5	1738
6	oemmeljmhpjwag	pgogakzqnzedvwr	279511111	7	2481
8	tbbhqsaoogiewyoj	vfoslbvykccvvdsu	945211111	2	374
8	bnuzfzykpne	wwaazwmdkeqdcey	101911111	2	904
9	zoeometviqvdas	vmgfwfnvpch	733611111	5	1838
9	rmlsotfulju	fiawuphoeffsworbekt	865611111	10	2764
9	rrocewfsnziwlbn	msoolfogoh	752611111	1	2940
10	gmzaqfibhi	bnjvlizwde	213911111	10	295
10	pzowfqvyrfrng	xfvwysrjzfspjvjvgy	350311111	7	987
10	mkepovpozgvzilpqla	nhflmrfsetyhdlp	219611111	8	1166
10	qbyrpsoszgdhotxzhb	zrncxfoemmsqmztup	663811111	7	1511
10	xuxgphvnpjivgmgdel	iwhihijgqmywsgnzouo	045111111	10	1820
10	mulhqimfzsoslh	vypqnxxdcspyfahjw	053111111	2	1987
10	qzpgpqschm	pwomfkkywtnzgjouh	170811111	5	2764
92892	xzaetzi	yidfaexo	\N	91378	\N
1	mvolrhehydcdutyplmks	hoajbepoahfmi	512411111	1	783
1	oarybwfkjszsya	lpcweugljekjd	423911111	5	925
1	fivapowltbhygqllmtb	elwqhcbktpvcpmkvj	772911111	4	984
1	srdiebxydpefhsclfus	eougcycajebsyfbtsru	187911111	1	2415
1	sefkenioikuykaza	cpmjdkavvcos	034611111	5	2875
1	hufrvcxwzcxszbbq	ywnccwzssxv	457511111	9	2902
2	ueiqcwabeitybmsed	rhadbjlukwldhlnli	105411111	9	864
2	ubffshozda	qwvmqjfonrjd	126111111	5	974
2	qxukggtqdt	pldzcxwdaifqqmipl	553011111	7	1186
2	rvrwaizpmcycb	iekfdtxaklrmvqeq	219211111	6	2583
2	lkmdmxqflhfgn	fmopywythoxhpvw	266811111	2	2671
3	wjdaaydjxdw	rcyrlwutcqczgjh	610711111	5	488
3	cufusaqjkap	ccqukgqrae	806711111	6	757
3	imuffvkmsklrify	xdeosjfujgsroibxf	205011111	6	991
3	vvacwnhfvdlmhcgfc	sjfhmkcudbfrlgvjq	030711111	9	1299
3	vusswvcozgmlr	uqspayjpzhhp	774711111	8	1433
3	aqyoxfqebnzoxsksucuw	vmigogjsiaucwyyawwoo	603211111	8	1515
3	nfjicqfgvumjdgu	jgfvshpwqvel	101411111	7	2043
3	fujibqsvpobh	liyqhegwjtvpvxaezqxf	383311111	1	2832
4	wonevjdmihlkzz	pccxegxxrfrvktxw	577011111	2	335
4	ixwekvlfdpovwcoqboi	rqzwplhouxjkmbcokltf	028711111	9	696
4	mwesggzesyjlxu	jdbubizstvgqwt	701011111	6	1938
4	wsaoslgjawaakypiyaoz	ygynnlgqgqld	943111111	6	2523
4	freocssxmttay	owkarpeujjowzlkblhuf	594911111	6	2866
6	wuzymldegtrc	hmkcvyocnxthnmt	224311111	2	269
6	vnvyjecwfcvm	qeuftddfzi	563411111	4	758
6	uvdlirgopq	tvacrgnwpsuakcjcy	868811111	8	2454
7	gimpamoodhsayvvez	bxwseiqiqjgbtggpul	286211111	4	89
7	jedfngfedlourgcavge	ylrjfldxtpgcdyhr	799111111	2	96
7	zvpnckndkqktoyzarchd	cmrzssmkuis	119511111	5	764
7	ywnwdciuwlcj	weagvjmnmzqig	961511111	5	828
7	dxincdfqmqqohndrho	eapsxphtjffgnrfy	625911111	5	997
7	tzrsenttzmod	fxdjqtutomja	746911111	1	1353
7	grtbgxuqpxvycazyicbb	pipcimtpgh	223711111	10	1404
7	zhoqvaeaofk	lppouvczknmbemwhqldk	177711111	7	2431
8	lmtskaqiyvd	vavajyaeim	257711111	5	364
8	tbgksocsphbiyajzhq	rmboppkvgwjn	265511111	7	606
8	rprjqnycel	kqpflbxrowypihqvnvs	407811111	10	757
8	rkbghdcfhhumjquyovkx	apngljsfip	783311111	9	1812
8	jleoprdnmitltogsabxo	ictytblnzynrqr	316211111	9	2332
8	fzxquyytevfjyrmuzht	jglzgprxnusgmhci	010811111	8	2670
9	cctcudwrfwmv	iufwffdptislp	773111111	10	1094
9	fkpqhoafzsfypcei	xuasxxrlpvegnpgoh	949711111	6	2960
10	elzmfmxhfmq	zwbavqcbqhziueef	265411111	4	29
10	btosccmwgguetevdgq	cfzqdgoimvtmwjepecas	588811111	8	402
10	vsvacqtkqez	gvrbbiydaekgdamteqh	998411111	5	453
10	hhvkylxkuyifa	qiyznihgvanjcieupmm	861011111	5	568
10	dxgvsgrhpa	dkcjunohmgmqvpbphg	793411111	1	1075
1	latcimcznqdhnxhek	jsrufsbgrakjhongjz	705811111	4	515
1	wlfqjieljvhd	jyjyfgsmxixucpabtykd	252611111	9	1575
2	ryjplcssxhxocvvbxgqi	fwmlshxnpmvbxwhked	193011111	8	752
2	ytpdunvdtzmf	axgovqhygrqzna	308611111	4	870
2	resgsevnqzunxsph	xnohippszvqys	232911111	4	902
2	sqjqczsduetkhazhmh	flxkhoyyur	252811111	4	2258
3	tsmimhifth	xhozpisrxwgcijnkpkr	805211111	5	1810
4	iaifyawnkyrabmkzkdzm	qemmtrkflyydmeefppe	991511111	9	749
4	ucztdtodfpfpp	otsyuvzsset	574811111	7	902
4	uvwlkejzamdnam	erxwvbrrlvukfzeh	923611111	2	934
4	pomxibznjnsqurfu	tdfksatxzrxv	020111111	10	1148
4	mhkvkdbfvybo	xxvkqpryyk	475311111	7	1262
4	btgoeawqapmcheit	ubecrhhevfl	795711111	2	1553
4	lthoqpgefmtgmsgz	wfsjxuwqdtlomdd	076311111	6	1878
4	kjipdqbzce	yfhamtsfqb	471711111	8	2946
6	bztgqemmcdytzw	wewfaiyzkxmrmfwhum	603111111	1	372
6	edthfgddexf	siieapgcnnla	040811111	10	1231
6	gilfdovpxdalxwekc	ozsyvidsvmeraquujeja	512611111	2	1554
6	vgpluidluiaur	kuhintjbdbr	177911111	1	2608
7	ukolxgfucho	sloddlnkungcgustuby	857311111	6	148
7	veiuztbouqsqg	nkeooirlnfdoufalfmo	999011111	2	386
7	mqaywhbmud	jvsiwnloqz	413811111	8	1338
7	kmmofrulmjtcgkdhxewl	ldywgutwbunkiau	894711111	10	1450
7	kwtanufrlziaqzzzi	dqpqmgafeenalyhw	057011111	6	1545
7	pdijeivngstpcr	zczlerniqivkzpx	113511111	1	1752
7	amhuadpvlxmpua	rzsdfiipvdnordfp	305211111	5	2370
7	vdjkbxrtasyxq	kxmtmzntmpqdac	267711111	8	2515
8	nofgisetrro	oegtflixfame	305111111	2	1158
8	ggpmnwaskfrwd	aruxjfkafwo	923311111	10	1559
9	psgbbyulvk	hbmtowalruvwweacwn	891211111	10	168
9	wgzdcasejohi	hfgkjopjjuxcosix	206911111	10	298
9	mdwnhpywbcoodgwhuz	twoasxbkkuit	039111111	5	1187
9	wwgqxmsshuuwnqosyqm	hblxgdhskqnlpygfmu	298311111	6	1808
9	pstvlimhoebwghq	jlfgjtvedwpfojt	510611111	10	2279
9	czipqolcmlfmoxb	nogdopfpeebhfxrwplkj	556011111	2	2897
9	vmytoodpjh	gxwllebgsczjc	637811111	8	2946
10	hjrbpxhmgxignemsy	zwmiatcowgdgjczassc	116511111	9	383
10	ijhwblscjxhpkpp	ohmwhnzkpfcvg	177611111	2	470
10	gvimbzdaphdhzlcgbus	jadnnfwvvnjhx	616811111	7	595
10	gixltemguctvrir	glgabcpbjzjda	868011111	10	842
10	znedgkqchfotviu	ypwjumayewsvsgai	208811111	2	995
10	ofgjpplpffn	sylvzcqdyhzetat	656711111	7	2155
1	ironafuuxrbrpc	pmfrkdzisgv	903211111	10	225
1	fckqpdsctarhgr	ierxcijaeykpbg	582111111	9	1191
1	ixsexuywhfzbj	fxsbmkcjfr	579011111	10	1608
1	iysowxisetfjbqlrh	ttjrjsqtvuwvb	651111111	10	1965
1	hxfmkmujdzbjmhvkzjcz	uzmqkmmczf	295311111	8	2110
1	lyxkatchijuifuyihur	amkhxfkixsxvwobl	092811111	5	2450
2	efzzeizbnafsc	vbtocrymgequyxnjh	082111111	10	220
2	gfjakkqzockxxm	bazkergqigmgxkb	514111111	7	486
2	mslivkvcrcyfnlnu	zgszvafbyzu	948611111	10	2400
2	uyrusyenggj	ltxuwdopmigetoa	710711111	8	2933
3	indsglyursadxn	ogrildywzivyesabrz	209211111	5	1431
3	aduatlnbmrvcsq	bhauitxnexswmkvssbve	629611111	8	2076
3	ijejsofzepo	zpiyiygasrjjtebg	013111111	1	2110
3	ieoxolfcljpxdsvfpt	jxpwmbohvfvzvwr	197711111	4	2450
3	ftgmqdfpvdstj	nutkqsstwlwqjj	296311111	4	2560
3	cqtjjuwbjglpghlfou	bhjcybazfqhmdntk	375211111	7	2617
4	ysxalweixxujnm	drvcmjwjechmtivpzdd	345911111	6	494
4	pizfmpmjosj	igezaaumatexdygiu	903711111	1	1085
4	ygnvudcpagpc	fqjjbwdonlrsycxih	257311111	4	2460
4	zczyjtznomdvoh	xwmzwnudoaitpcv	635911111	6	2708
6	bdjlscljpvflvonm	jflpzkhquvuannxtyz	753911111	7	584
6	znyrlbgrwhy	fioxwkiphrsrpnqydk	107811111	2	2174
6	wwegneonet	rfnsqededztqdlmtyzjg	594411111	10	2290
6	lfnnkvlhxjstmvhqjz	xbzrjuqccppcxc	425711111	7	2643
7	pbpdjhvarszz	hycyrtqtpxpnlyntns	094311111	6	30
7	jlpehhbweetddauhbilf	voqldvomrdcrtipsvv	756211111	8	494
7	vnzmnftfusqjpa	atezsbwpkmiyknfg	733911111	5	1756
7	fmrtffnxthbwuc	dgupzykgnctdnzub	474211111	9	1864
7	aalsaoaruebkfi	qhoiwzjbkhxhnn	839711111	4	2328
8	igsrwqkghbpjvnxkq	lyysxwvyia	636311111	5	267
8	stnitemkuihabxdwrld	oolhxrhmoai	013711111	9	1226
8	nqabfmvlsnzzlbawbo	vojesehiilhirrri	536011111	6	1448
8	jfzlgmclvhlbv	sqydmqshzlptt	871311111	2	2197
8	prlnltscnxbgpm	ifzrifpjbkcdyrksebbd	972311111	7	2326
9	peawknyhiwnqylverb	kqfgnetxhnssdtm	454011111	9	225
9	oyprbqoouiajcddixg	nlawknpceurtljwwi	083011111	6	892
9	efoqknxtuedijfx	uekoqueepwfpn	599411111	1	1360
9	ezozujyxbgnqhncqs	ywaknzndsdlegxq	210211111	7	1632
9	alapahfmpiacnpijojht	tpjxdpomxeyuwzwod	407911111	8	2933
10	crdzgzhjokvaxcjvarc	ddxeouxfpgnlr	072211111	9	388
10	evgpkbwvknr	rcchkghjwrct	268911111	8	1984
10	yjntlimiwfybpjvhlji	mmfvadtxilgujdkzgm	754311111	4	2400
10	vdsmgexxqqehymqigk	dwdydktiduwwmal	679611111	2	2560
10	dwxskgghbcivwdcuwqx	kmmjkwoxxgu	066911111	8	2913
1	kjsygiofbzc	kedhcrdzrahnmfo	646011111	5	403
1	yeswagdgbt	lzueoeaizjcvyh	565111111	1	1806
1	ubareisxzeuompb	uqhshiskzfysshp	154511111	5	2084
1	bvirlufdjkcxlxcudpi	pzrwmlrmtebegltf	174911111	9	2972
2	rluiuaisoeam	mqnsrmgsepneo	862311111	5	338
2	zugchbflczalsecruhtq	sdltzpvafht	160211111	7	692
2	cvfbypolkcrgfyydhbqe	wnrsiaixeglfnltj	725311111	5	2714
2	egjpwenguzywpv	eigxnvfmwnqxkojjamwm	692811111	6	2994
3	bpvyxbnpjiphqbot	sfourikpedvlpsd	725511111	7	539
3	jtegycqwwd	xdlvqehwwnw	517911111	8	973
3	uoloraeeeknlxkr	sdneznxywoqmu	910811111	8	1598
4	aclaenonzwb	uujbanbffnsjzbaf	598711111	8	534
4	mjaqsrkxgbxkmus	mjnejyvzgwptgxmqlkz	565211111	2	2198
4	zjqjfaekigxrcpl	etzlgrfsywyauebe	049611111	9	2408
6	cflwkeuihfyxqrzf	pbffwvdwbh	219911111	8	175
6	dimetsoapaglg	wjrgjsphtzco	015011111	2	403
6	czsafpfyatalnt	gzrhxmsrefirjv	056511111	4	1070
6	dyuypkcsru	slmbktogzxyvaomlxa	361611111	4	2714
6	rbkaqxphhzqoyyn	zrxtekpjgaieywwni	332711111	8	2767
7	aswboxblksrezpovluu	mgkhkkiuseyynezaaz	924011111	10	292
7	yciamlycvhlebx	ksxiqwnsrphk	858211111	8	340
7	bgafgezkvpzwmodvr	tmuxbmbxxqs	923211111	1	1732
7	geljhwtvsmbrmb	ubnnlarmkbjmjst	404711111	8	2398
7	mnnltsicdxozhssqfv	mroglhqttxggcmo	083911111	6	2694
7	pthigwfvcm	gearugxsscuprhayre	525311111	10	2798
8	svbypbgvsrbvwzprk	hqtfgfnxulynwigdjwbn	606211111	8	126
8	rmjcndzsix	gskegcwtjqripbosegwe	787911111	5	132
8	cnvzuhegakmtnj	izcmmiebztlxpmc	165311111	6	429
8	rpwwstxlruin	vrbbqpwmusivpjetq	318811111	9	586
8	wwibdwfljqrdyewayzn	lfoodfzyxdhsoqak	710111111	2	1292
8	aqfrhgdirzojihjqmvv	wnnajzaxfbpwbrrenha	123611111	1	2007
8	ppqqxmutxvvgweqw	suzjblmyunmirgdpvei	853811111	2	2213
9	qnqpyyvclpopyktkhe	ezdojqvpmnyhkuogi	427311111	2	658
9	zcycevjhdihii	mnefapuhxw	461511111	6	1598
10	pbsruttcukgypddllrul	fbifqeadybntajzt	400911111	9	296
10	izlcsyppdeyiyekh	birmleorepiixru	465911111	5	303
10	sczafveieaucpo	ivlhsvlmrvqjxhh	553911111	6	778
10	mkanliaojkxonj	omuziipjowoskpmb	297911111	5	865
10	pthdfeuart	kznwyurglgpw	598711111	6	1055
10	bsvsavwmcpyrvelymzw	bqiapjqxmxitndd	796711111	6	1064
10	hfyynzshhj	qqxhjipvtzjfnna	677311111	6	1132
10	uuncajdnkqkd	hlqlmffawzjjq	479011111	10	2011
10	ysqtwssqvpowspj	muvdgnxktrcxfrf	386611111	4	2125
10	iyrqznplkfrbwdppd	zwyjuqfyymcsrau	748211111	6	2243
10	aexvjbzlxfaqi	aiadqjankfitmebpufnq	400811111	8	2486
1	nfllldrfaqyo	glmrvfxnbofiiemvhxnv	987411111	4	24
1	xlcynstsabj	mmcewiwukjkvrcokzekc	515311111	7	2937
2	mhzcbzbkgmyroopsloz	joujjzowpfwdwrkif	250011111	9	1428
2	vtinitxeuw	zucmonadptricppfmw	881411111	10	1658
2	izdbteaxrq	qlxorcswappudtp	800711111	10	2052
2	ooxitpjxuhzlyl	hhgzsdenlioa	873711111	4	2435
2	himibvtgluk	mrxnuvibhvov	436411111	9	2736
2	plfqzntcucpjq	kqbtjdiklgfrqvgsb	097511111	1	2829
2	yhkphdecszlhfbyweqg	yaprxpckftjahcv	121411111	7	2947
3	zxewbjxiklmrpop	ssmmmtjnwv	466711111	5	1213
3	uziotjdnob	ntzcmvkjwlw	007411111	10	1879
3	ndiotqllpfiyjijw	gaysvyhtciegbnff	282311111	6	2066
3	hbqdsmglirvlcxlw	ggcypmxxmiyz	751111111	9	2249
3	qirthwvyjnz	iazwjzmuauyhcm	525511111	9	2434
3	yfwnppkpnjloinratcnz	lnuopwqvkjgnxveqfn	777411111	6	2724
4	jtjtzisdjfoqlzpc	dmsgnnbvmwuwfaplkl	229011111	9	548
4	jpvwzfaulklntjwrms	sfjvhxlbavfuh	036911111	2	762
4	mcmjxpzcekfyx	glhaalkhrxsrvzthbmda	208111111	4	1665
4	ujvznnaibi	zdsakvrufxdqvokyserq	005911111	2	2876
6	mwcqkjahybmg	ocrezffuubeufwujpl	134311111	1	705
6	owjwfomibbz	gcpjoqdlotkdahj	874911111	7	829
6	wbwaqfzqpzzsdduae	pvuovzpqbhz	200311111	1	1626
6	vddzvvqaeamgcrmjb	jhmtoonhyigrqytwuz	281811111	9	1767
6	inewxhxyrrccbfpslvgc	dodzocttfmvytq	575811111	6	2432
7	sanctroehacjkdqwpfx	vpibuveyqe	152811111	10	884
7	lixfvhdggcifdwwpkb	lmzawdgkhjqes	001511111	5	915
7	gntvsavmijtplz	zaxgwuhcqx	182011111	7	1458
7	gtqbfbfxplok	mdkardzikswi	093511111	1	1671
7	ufobqwvrihciporpn	ifukgcwlzr	576911111	8	2041
8	bjpvdwtepa	uzasyaaksttseucdf	482411111	9	240
8	zpjktqqtez	teswhpkibrplczjmcdu	132611111	2	266
8	hcmfbcehztdbdr	szcwwcfisnfos	380511111	7	857
8	rhxdubpcyk	glwghspsydsdjvdfafeh	421811111	1	2009
9	npptcnitzkye	fjvrxncstlfg	397311111	4	181
9	lvuzokotbhc	airctxwckq	648111111	1	884
10	fidfnyeismrffoiep	bjzvvuhnmeikjwquz	953511111	5	1581
92892	\N	xzaeqeoi	ytrcxpo  	\N	98318
\N	halaimazi	lanilolia	\N	92391	98546
1	upinytewvxqwdxgmgh	bfrpptikznezlavuje	065011111	8	1751
1	kdjceudsmiw	puvtdriytqhjkrtxe	507711111	5	2070
1	xisqifshbiw	nnssxeaxxqstcz	123411111	7	2070
2	uldvyhqlzqmpp	bzmachxfbmpkokqur	249711111	4	903
2	rejlbacwzpx	qithgaoqsri	142511111	8	1445
2	oulgyizdbaquvf	mdryulvmrxuqihhekvoa	782911111	6	2381
3	vsuathbpwq	zglwkhduco	456511111	2	463
3	mdqbffdxxiacznrxlcps	mullxncsosnqrglxjbqi	400511111	10	738
3	eaxhjjbqmwjkmvo	okjztiaarqbzwnszagir	348611111	9	887
3	ujdrdslkgpygumb	ltbpziabalsm	940811111	7	2065
3	aztbytelojblky	ugfckwnjwcmvjjfr	354811111	5	2672
4	qpmsncztnbmtcazt	jonfmrcclduqdsmzfsh	861511111	8	299
4	fcgigdashdfzfmmrnr	npckwjpahlveufoh	781211111	2	2207
4	wohtrisocxuknsygg	qckvtmtsszwjmgqo	934711111	8	2971
6	ztqpkcjdkuyrgkft	dksvyixpdwwawi	896911111	5	2821
7	uclyqzjzshphrtrr	bzxklgqmlvzyyma	420611111	6	367
7	hslkjjfkuuklmkidgixq	nitbjxffoxjnvrpmu	082811111	8	1083
8	bwewxiwaczoi	bfbvpzwpvdakxyq	661711111	7	1901
9	frqxwhpyxku	jtplcvisbrxcrhyna	883111111	2	282
9	qwdbdagovrktwrcsigih	xpxwpfnilhtqt	741111111	5	536
9	bonrfiaotsk	ammcobvrhhhouikfopz	889911111	10	564
9	lnnlguytufwzvnznvqlw	zepoyglukoeirchczugn	800111111	1	1091
9	ilpmrpcvkbppxrreq	qjuvxkqrinpwsazm	833711111	1	1912
9	lrfvhhfexdk	ufaunrrulqeqnjsgkzkr	062111111	9	1958
9	ukrqluywxryqhcbg	zimoeqxyii	888411111	2	1970
10	qvrbneskkkpjxpj	izeadikgjfncihnl	059511111	2	193
10	wjlrgwpqop	noylorsrhokrxlwcdlah	335911111	2	953
10	tcgqxfxvleb	mqgtpmtalwc	615811111	1	1311
10	fjwlvtacztszhut	bjfuorbzrdcapmtqbyxv	940411111	4	1348
10	gkmaiwievqbe	scsybzmbmwf	836411111	1	2291
10	cemdtenzylb	pslzbgxbog	491311111	5	2347
10	ebvudempyhgbj	dxxzhcykufogzxedumsx	840711111	9	2403
10	rdqupdwslfn	ftwjubakyanwyezathi	345411111	9	2984
1	fowywchknfiokzxm	bivuavxcwczipj	275511111	6	138
1	vsoynwlksfrgx	nfizaoreblaizzgx	218111111	8	1041
1	fnksqvgktnlodil	umurdxzzbbr	996011111	4	2040
1	ekjejnxgwvascaeshc	tsivklfyjir	108811111	5	2139
1	ysqumlekqjzmwj	zxaqippzktenqxybpf	243811111	1	2426
1	xsqnrbceabqhtyh	rfzggohaqfjb	353011111	4	2923
1	ndlimmnfwrgpgnmptm	fqkkvcsaroqovpdizi	241511111	7	2926
2	tcbrtqsxqfufsfb	owbpqbjdoirlwvihlepi	016111111	2	313
2	kpqaamklusxop	ngxpsaczfphjf	369111111	6	512
2	vwpptvzucxnwwdp	kdtoxndpyvzgxeeir	563811111	6	1425
2	evbcsbgtpvskqaqw	ytaudzlwusvlgpoztk	062511111	5	1529
2	szmzqsahgfrxbhfh	qryzofzektzurmx	589511111	1	2055
2	niubklqejmnvdubmbu	vdlceegmkzrgquvebu	486811111	10	2406
2	bwzagwbkisgfs	dnyhnmddinxddpa	087211111	7	2423
3	fydwwtvplqrknhsexah	ofyhqfieghda	624011111	6	838
3	btfmeimjtilsesvixp	yrbianeocihi	820711111	7	929
3	yeprnvcwpckqei	njunytjncilemx	994411111	7	1052
3	tvuzfbrvhzvrwbs	clvhtmotsoirwwghy	751611111	1	1069
3	saljqwwxdmdiwahie	hfnfbvgyazmvdgpkrby	688111111	1	1635
3	qsgeqgzdahjkqtrnglzl	nlljkofemikbwa	100711111	8	1870
3	yghbpyyxtwmd	fdqncalgzoqsajcek	793911111	7	2212
3	xvcvnvgeausibltm	zlxpsujyuh	294111111	5	2982
4	uwcfkxphlnp	hvbfgdmrkydvpcis	607311111	9	343
4	szqonltcpd	mktbqhdlgbazneuqej	459311111	10	927
4	joeytcytgpfrsle	kaoulmxjgypbiabhxakr	414911111	4	1627
6	orfaakpczis	ooqzkafxrwrcekvjjsvf	282011111	7	265
6	njwfnzdhvahqxzeydc	dmqykzwvikpxf	629611111	2	1255
6	bhseteiuzkwvrfm	pbeaopmtamy	307211111	7	2551
6	gpdigpovun	ffvlbaxtvx	549511111	9	2673
6	agktxiuawsjagnbj	eydkcxxxrpkmme	248211111	8	2982
7	niglthmyfx	faltpyzfieyedzfvjlj	753111111	8	662
7	siwqnqtqspgugdto	nkggmzhchiglnnfmaguu	121711111	7	1113
7	zcdymbsmffneooqqpc	xotfkehzgqoimnllgs	655411111	8	1116
7	uuwotllswyqb	fkqiibegrqtxnsekpiy	612511111	6	1172
7	zioainlokidkxsanfi	qyncppeebrq	771311111	6	1564
7	vaatodtubrsftvxp	smjkcbmtfvx	322811111	2	1675
7	nlnjiciyopbyjlpqy	pqywnfqelmcbusoccix	318411111	8	2424
8	kjhddjklkjyngzehr	yrwthdhadt	563311111	10	248
8	offeovbdkfkoypmnnvye	kdwqwdmctpjxn	588511111	2	1090
8	cxzectvxweptdrr	kdkahtnlcim	592911111	4	1362
8	ugjdtwtjltg	ymsocjvavfy	572311111	10	2061
9	ucajqlftxv	zxkywywampovxlfsz	154811111	7	554
9	yjpuaxqbpfhamdz	nmkjxpgqwxlsacq	334211111	4	585
9	pqhoodxthawd	zfwqelnxrckcbeg	395511111	6	657
9	hvdnlcxybikzwwqq	siclznxvvzdchaole	357611111	7	689
9	krfvzzjizaaewm	csxljffuyohilgm	802311111	4	1115
9	olahdynqznb	xvrmwgedgvo	986211111	6	1364
9	xgogmjhemaiebnmgeerx	blxnvqgodueavbuncrj	603811111	2	1546
9	qruruxdpsgbhua	pwfhikukhialcobplmfo	859911111	7	1580
9	tjzanxkihl	cilczajdvfdh	703611111	10	2130
9	xpnazwoefer	wjrbjpndpetz	587911111	5	2320
9	icekxuqzqsbjvwr	rzizzdrnurips	842911111	1	2612
10	ntmheknzygex	ujxdqqjpbtifokiywbw	688911111	9	645
10	iseufuzackck	fzhpujonwglvs	554111111	7	893
10	sruapzwfgibcnp	uzhuwhdcqylzgigq	152811111	8	1585
10	iybpvyehniuu	bzflzliuvnqomebm	306211111	4	1988
1	fxiuvmvjrurbtnoixj	czjcijkshcbalh	851711111	9	113
1	opsdruwvfppvxne	ncdunnchhnhouedqdzd	927311111	7	195
1	zzaakzzqvvoludqvj	tikuacbodtw	754611111	1	201
1	qaxjkwxsfay	oebwezhejxhttyh	695811111	4	441
1	grputspzikuaonb	ebvnsfaqqrvoeugd	757211111	10	1306
1	rdnnajswrpaqlvflyoyy	zfgqmuixaxhh	054411111	9	1346
1	exauaoflszhvja	exmgmtxepsbvt	082011111	2	1774
1	bxibsejtnrxzyh	fxqkrynidpco	206211111	8	2657
1	rktjxulgboo	etzxjdxpvenxknum	389911111	6	2804
2	leajngnixabx	hmkhhlgmhwxkx	334811111	5	281
2	pljwgfdvfmxlt	hqfaxulbqpxdmh	014111111	5	517
2	njeuirljdqqkodxwxmyk	jxagetdrccgpnvvrh	174111111	8	517
2	ivpyeyvvvmxd	dbqcijjbaqdvfppuj	074211111	6	651
2	sdcjslnloiwwlousxbr	sqgesjydrexwiqgyebky	531411111	2	2402
3	ufbcctibmhqimagtdu	svpimmatkgnujmeekhq	184611111	6	597
3	unijkaozpugmfypejcm	pyqnawhhijmy	287811111	7	673
3	ntrjmmoytkyxk	yjharozpovn	909911111	9	1003
3	dshtqtqhrxli	hpsrgknumvqwmzqh	438111111	6	1204
3	ulfgxpaxbpfkz	etcryzojxlwylpc	241111111	6	1233
3	skceavuynukbbemdpi	gwfebjmhohxf	185311111	8	1315
3	alkfjsjmugrg	wkpfysbkzrwwfxqdtgpf	456111111	7	1960
3	ydfdprhzvpdaujt	lhvhxzwnilppa	727111111	6	2002
3	bkweidksjdr	jwmovqrqbg	057711111	1	2050
3	mnlujvumxusi	abhgikbmujavtmddgiu	050011111	7	2698
3	ooolvuetiibjulzalmg	thbwjupoccganhy	690011111	9	2999
4	ngrvevygwb	vktppsbeimfy	299511111	1	113
4	xpcnjdljmonuftas	vpslkqreyquzrzk	304511111	4	776
4	gerwyqjxfaxfjlsgur	pptgvtavyiiuymc	698611111	10	1583
4	kskwuptqsyyveyhiu	qognmkdjbz	451111111	9	1935
4	ikztgcboycfvb	tmaasfzoaivlc	322411111	5	2048
4	knzygpegsdswdlzuqwl	vcsnpzdkclufrcxe	573311111	2	2609
4	lgvfzcmhxzjjnrujuz	wrgmtyexlcahv	218111111	10	2680
6	bpascezdkjwmxyphipv	flqwvcbgskoeosxkzk	413411111	4	647
6	wnnqkwrvow	xooesvwyaeogugcia	614611111	1	930
6	adftvwodcymypwkm	gunczdzypczk	731811111	1	1119
6	zfkmvoezfcwuogop	vptzvbzzdutglsht	052411111	8	2975
7	xdyyhigmgjmulqohsw	tckxocymdbxqxk	475611111	5	5
7	pnzpbacsuwqsxldsyss	xmsizbcnlfpuzpxwx	844611111	6	422
7	wvpquvcxvnwzsmtii	rsajzverxfqn	362511111	8	444
7	evrugzcbqzubpczl	idceagptyehojpfa	906911111	7	988
7	jtwtkemblkj	gpnggfdvixcmxhwwvqgo	274911111	4	1119
7	cskmlstcyuwtlepueiv	cbjlmprvaxoyqya	273811111	8	1728
7	atscfyehkpc	ijscykxhftbxglbia	429511111	4	2752
8	ydsmhxedoytyisht	pyllyssskoyommgrtdm	260811111	5	277
8	qkizrbjazaslxkgkohp	zaiscvgbjivcopwrk	002611111	6	572
8	uzdvzuybgx	qvdmmdclymycvxfgtlh	464811111	2	770
8	whjzcgznjasbeynxrjm	xkdsssrnyrchlf	911911111	10	1935
8	qxlwwsxperos	twilijjvhwau	818811111	6	2122
9	hbeyrbyxnfivzozzlu	iwtpryyerqoth	935111111	7	1129
9	ibsxygcsqq	dscngwazfyjixifttn	076711111	8	1144
9	rytskymwwimdisl	akxejtgjbg	174311111	5	1260
9	hteuhfwglhkvkgezrwgn	bygrrsfsddxoa	927111111	10	1381
9	kaenrfuqwj	zscisftejdpqbozv	895311111	6	2518
10	fxptmqafld	obsyropptfhlxz	764011111	4	422
10	rbtagqvzgn	otearcwbdmrc	525811111	7	1306
10	sgkbtfwunkvvezyyt	mjvzfublwisk	124111111	2	2420
1	liqzcaxzpiarkl	ieuvuwrjofdrojrhku	529011111	9	59
1	wjbkuojvcgiqtiojy	qkxpxxrrbtxzu	394911111	8	889
1	cdtefdrkoykiduqh	vpbsxidubulabdgfmgjt	103711111	10	1133
1	gsshxsfyevkummopaha	icowkfxheqshlhzztp	343611111	7	1837
1	pgcjvkrdbp	mkhokasfrqnzjz	660111111	10	1990
1	nvpwtwxckixsennmv	uxqymxwcgeogyiinrnwn	001911111	7	2095
1	wzkhcqjntteugluzfomb	akytktmevmwnrgpn	589611111	10	2575
2	qnkfccqqphmlphthb	cgsvccjzntopljgmqobj	477811111	4	59
2	pjoxyjwajyqxwpmn	uvijmipbqdkqsqexo	631211111	4	1196
2	lzpkatdoripkmlet	gwhykuxkgnet	597211111	1	2872
2	xkpcddijkzpez	jvmesxmgqenb	269011111	1	2895
3	kavvtgxcsrnlfz	cprnanxajbfcschr	243211111	4	22
3	kcvuxjycrdvcmokzlmtw	dykgyxkdxdptnqbhn	058711111	6	347
3	cyqlzcppmvjbaigobew	jvfalphcadbuabbeyqrx	021711111	6	458
3	mqdwqteyclejcesuuz	sfnophoznybghqhcl	964011111	4	2038
3	qgeybieawg	pddgpmpcotprpq	942511111	9	2240
3	rvaadwiuxgdvdyflgx	yvghgjucvziwvfqyvuc	736211111	2	2996
4	sbjxljgsshrq	lsvrknndcckwtoqffy	547511111	10	813
4	ipxalakcmlydljhctm	ikpeuhrqsi	572211111	4	999
4	hidkweowcaoreo	xtjqpcrimbwrrqycmkb	250511111	10	1133
6	ahxwgsylmzumi	mjrbcyyhytzx	892211111	7	830
6	sizryxgukyql	mqtpanexeshb	849911111	4	1196
6	atbkpnnwibhesdk	mabmufksmdfweorxw	000811111	9	1562
6	btczglyyhh	zqspfpbgmrlxtjc	060511111	8	2295
6	kvpayvarpgupztk	loqxbignmckso	992511111	5	2516
6	zxkosrhhexmkzphsl	qmahmeklxryzot	391011111	7	2872
7	vfolvayoiskokwjjzj	chulgsfdqfhvaf	200211111	5	4
7	xqubchucwwxhuwx	xpptxjvwkcxxi	430011111	8	491
7	rmzeniwnpituclu	zbpmqkocpedtt	426111111	2	2295
7	preygakqkmazo	dcgsaygcvez	823611111	9	2526
8	uegogfsvmnkijpmeerqt	cciipmdynmpzo	943211111	9	1538
8	orhvcitjkisik	nevwfrzhukx	967811111	10	1622
8	rofqortbhdqosuheoy	xvwsnwbzmpqysjp	675711111	7	2012
9	axvhkvuvatqlsrqfdwvi	gzzylirlwwligrjvm	016811111	4	1721
9	qqggzumubuoba	xkmshvojgm	803711111	8	2532
9	esbcuojmrkesbfig	flqutxcxuqaepddgqi	155411111	6	2546
10	awekjmrujhvmisrpmo	bcbuurgmqcmtw	852911111	10	1847
10	phzjteabrqimmolfek	xhazrahtvbkhj	127811111	8	2448
1	uxduljdchjpfuuu	poitxjsopfrugjpraah	424211111	7	214
1	gnzonpymmslocnte	crrhskfzicvpegook	994411111	9	474
1	czmgxogztjankucrf	nnkwhdrjct	304111111	7	2021
2	nfmjuwkzzwwaxrcnfadp	xnvgotvijznaxgmx	408011111	10	52
2	timcviwpqcbmuaeqb	jgatkavdjxyusbmfkifx	990611111	9	449
2	offfkrqulaqlwbwq	javsqbyitizrs	440711111	4	2633
3	ooijojiidxlbr	gpococrebuckd	132711111	4	218
3	keegqabbsmozn	axxcrizhsayrutqm	076011111	5	573
3	gutjkoyuewk	qoispiqjeflwpi	622111111	6	754
3	mvjbzizjvinbmcc	pemgoxkrrh	825311111	2	1318
3	yvebkoubucz	enaqwlisudnowvhkyb	917011111	4	2636
4	csasmckizo	mdkpjxgiumglwyplimq	742011111	10	966
4	jtuwozbckczu	bypwwfbztdtmwh	983811111	7	1057
4	dbzdptioprhladsbwh	gihgmsbgzhlbvgoq	068311111	9	2136
6	puhoviwjztffo	uvrctnrumaj	330011111	5	214
6	hzxxvqemhfgdbivgqh	jihabbivqbganms	495111111	10	333
6	gupxekckudy	tsgdvxtjsluyewyfc	887911111	6	449
6	pakuylgkkjthshauwbp	aorjwvradcrlpxr	902411111	2	716
7	icowajfvlwunyrsxl	vhoxigcdttgenjdiz	424611111	7	360
7	cqlwdyfzuwswvzsyls	xfkhbfackbxnndfot	347611111	9	922
7	umrxnwdqgffyhqlo	ipohwhkzdmnkxh	857611111	6	1672
7	xtfilckddwfdxof	uuwbmxrwtwn	310311111	5	1954
8	oizoepbyzjprts	xblncvxltfdiefiq	166911111	8	1714
8	gbaepdpksozcwnso	qvtsatginqg	077711111	7	2184
8	iadhdonawge	uusfjbqobqdafwnohxrz	617311111	9	2413
9	nobmaggbplxvojhootj	chqgeirkuihkqq	732911111	5	218
9	myeujnjfvbgf	ndrhosahfow	580911111	10	283
9	mfmhbmtgsqlookxxlpnu	ourhhfmtwatzkpzuqnt	505711111	4	1313
9	ceaoicvubzefgjmuwd	tumybyflycujxddzluwi	478711111	9	1672
9	pphijphfqqhfgcz	dczijalksybhavtrp	728411111	2	2796
10	ydxrgikfjmq	dzmisfvdiu	648411111	1	590
10	mmgluuxetkzf	zxhulylyvecp	123611111	8	767
10	norditdpwvctdqt	rdegjontopkkhkxpsz	608511111	5	852
10	bzktgmabxyoszy	dfsnuxsabserdmi	875611111	9	2163
10	ijonqbooey	ybxarjdkthh	753911111	2	2735
1	yumiqipqcywsggdthpf	gsypiebxxqnkxagdwo	921211111	5	634
1	rmcthipzskthit	zhsstteamdfmmvhks	689311111	10	1223
1	ymhriyqtekx	sgfhmxiamcrlp	363111111	6	1637
1	gjbcixbrgeoktvf	xszdbsagdtzr	403311111	7	1781
2	rjphxabbtbyususktd	iplukijddqwkkffepmfd	717911111	9	127
2	tzcjkfkkcheffwx	kmxxajlmgtq	864711111	6	576
2	jblhkcyzjkym	pyulsnrvymrequhh	020611111	10	2195
2	fybruazoptayhsrfktw	puwhjwqkeevenfxughm	300111111	6	2205
2	ljvgcjnownjx	fcegvcqzugetijwpla	390511111	6	2433
2	ggupfssfqlx	hmabymcznviyzptjry	656811111	5	2641
2	bespexwskxbhpyccfkte	vzsjfsyczisrzhpbj	824811111	1	2993
3	ygzsgqrlolejaskvtf	jxmgrhhyxih	447411111	8	473
3	eoeeyaxfttk	jckjafdsrhtiu	950611111	10	1698
3	cnvlkpdcbv	tccmgzkhjgciflbzrjpv	574211111	8	1852
3	teyieleepvsapjj	mpnzofqvxhacs	585211111	10	2209
3	fdibiibjifs	ngbwtdgkbyangvxpx	769311111	2	2281
3	idkhneiaxjveilplpwpk	yqmklejjeavhvbpkqic	763211111	5	2407
3	gsljgdmwgmnhi	farqjouyhfnvlfehiyi	066811111	2	2776
4	ohepudwjdqk	uqomkdohnghn	408711111	5	626
4	ayucvpalbloe	gwaagbddpsomsylwq	816511111	9	715
4	jonuldtxughf	lcsrsjnnms	212311111	4	1296
4	vpnvugbcixa	jzsbdrynrww	016811111	2	2100
4	ibpoedsrupbwitg	njswonihykdwiib	812111111	8	2660
4	znajtutoiorochwq	ldpmjkslubusyhig	554011111	4	2867
4	plpvgcoyhveiajxc	fkpklazakxmonfdm	535911111	1	2981
6	vsmsqpgucnfkoodvhzir	sdrppmoixr	687511111	7	127
6	nsoysukuacu	imgcexaryenrqtztaqy	273711111	4	473
6	uraokqmcnzc	mztogqawmfamkod	232911111	5	942
6	rasupbcciakgqfouhlq	wzyyelmxxysi	194311111	4	1150
6	kowazkrfany	bgnujjghnk	019311111	9	1488
6	ljnbkttpcnbkjm	jeqwdhlhhig	997411111	5	2277
6	lpeozwoawlnyxkrjbhf	oezewejncgkkawmbveyc	087611111	6	2827
7	rxydmlvenhsvrehdz	giktguthlayowkxjparu	033511111	1	63
7	kfbyghmulf	wqijfhhlsyiutmf	427211111	10	594
7	ytnmzgxcaszripxnnnyc	gigteidbrznqxeqnc	382811111	2	626
7	hstduodzmwcios	bhrrmhclsscqyloa	566911111	2	1676
7	yeuupdfsimljgm	qdyihbjbzblsccvvlka	833311111	4	2339
8	zrphcfyrzuy	abidsxfggjkmojsbuu	796811111	9	489
8	anwfftlhmzl	pzgpddgbntddjld	961211111	5	1143
8	ypmevgqlzgftwdtm	lwcaufmnrxibpmbyhs	350111111	5	1507
8	fgqgkewtvggts	jxnroncxicpy	332711111	9	1698
8	qqtptvokyhvahif	bddsgyjfihcvlwqcpbj	236011111	4	2106
8	jxlsdhikekawkeqig	bzmjfrorubtclchsao	745211111	6	2227
9	yacjmxyjeosksbgii	kawqoztdhauctq	107011111	4	14
9	rmmbmbounlrias	lxnsqaqaqyc	628711111	7	384
9	ugynuuhlwixog	lzoavqnjuruexws	506711111	9	1944
9	xhzhyxjsfdfponf	fuimsunsdb	317311111	8	2669
10	bpfmqrjjnjgf	tgvgdslmrbuvhbsxktx	821711111	7	926
10	bycanhldnhwltdq	wizrcyhcivq	640811111	1	989
10	wywiomrhpmbijeji	yihbkaojnaj	314511111	4	1305
10	secdsmbwuxmjqivskj	zskwlqfuzxijlfu	844611111	9	2627
\.
;

COPY fvt_distribute_query_tables_06 (d_id, d_street_1, d_street_2, d_name, d_w_id, d_city) FROM stdin;
7	noazlftprk	zszhbqmkucxnqv	efikabx	7	zimnqexvkggc
7	kdzinruvshsnoputoxj	oawmfejxvqmz	shpmgwdnqz	8	hjhaldxgumgmucw
7	mtmgjffkmsatzl	rfspzqlurgduazqt	vlsywc	1	uthvwjrinzaxuzq
7	dalquhejntt	ttbsvjxbvhfsu	tetatfq	2	xkqxgqrtizeorobri
7	gxcytscfbk	cbfjbgfqoxvhcugiyoe	gooxlns	9	lkcabpcvcwgcsfuma
7	ggawnjrasiijvi	tefycbhbfqlwygowj	gaoglyghes	5	rmhwvtuseovbsok
7	eexavordkxhkbo	xspiukkzbriqewzp	dvvpyupca	6	evqywlmqmec
7	uenvarvnig	hhhnnhfqnequ	thljrhb	3	zxxdijccmtp
7	cvklkhrqtyybniykuk	oublrntbtzmawnzpkskl	ozwhcvkec	4	smyydawqmlzkxegj
8	gttptvetbpvy	hnwsmwzowtpxgpdeo	kexewboiue	7	fwxqcynwolgsdxay
8	cyfzjsaxeolv	vilpnqeovbkj	gxddrdn	8	vwihpjzlekjzpguzjgj
8	uslotvsjfagtix	okzjalhyrzcxtsbvhdqn	hlysyik	1	whhxarkdvqseugecwb
8	ntclwbhjctluplkbgzwq	xupqqgucso	gsjbdx	2	kcvhrmmdshec
8	ckdliqkjmejlpxqe	ixcijhutjgd	svoazvjcnf	9	nnwfxzvrwllphvzovmn
8	pnqdpbvopsqidwbyow	itmzfcukctpooydsgb	efcbsmydh	5	gbqpaeobnynpqfaxpgu
8	wxzovpmoogjos	qngfettasxzyex	ydkvrah	6	srzgoascjw
8	lmffbjuwxzyvc	reiwqwjuykvm	alhjfmwl	3	ewomakxkoc
8	tyxheriveufuixgn	prqsfwsckkcbpgctpi	xzwxdnxkq	4	urbieoacpzdvrthesi
1	ypyymqxripxhb	oyjingifjlof	ztdmtgo	7	dlzcbggjwuxaisjtq
2	zjhmdxvihdr	phkybcknrmeeoapyz	gpcocsmje	7	xkuqlaarvgeoido
9	jeyfmmprhwzn	scrjnhhrsazgitebq	lqblvxm	7	cyokshpzcsvevzgi
1	uaqiijjtzwydhncsl	einieshhqvynub	mmdsbia	8	hlqdwbutehu
2	jsdcqytpxrgftgblwy	nhamhsvrinxpg	gvgwhgtpja	8	vcpamxcmlhtftn
9	laqhpkzzxqsbv	jyygorrkbsufh	krnwsrj	8	dfbehhgtrhcedwnau
1	ogsxoekiohenrovqcr	utwvubqsuhikkpefz	swotbb	1	vpwhmiiwxjvzqad
2	ksnfrqaeubqgaodliopy	aenneibwcgejkwpa	ntqarolc	1	txiuglqcgsunehor
9	kqualmlmiihcbgg	qjiusmbhvkufegpzb	ddcaijlkq	1	anyvweicbzoc
1	qaobjrprypislzgkhnz	tktsebeuitpau	hqpwuvosy	2	cioejsreouallmrwhjb
2	ogtxorjjvsrtw	knhogjcjxyoxkqcn	lddvtz	2	cteadumukr
9	bwujieludvojohrxbpgf	zxvewygcaamsizpvn	fucjtrfj	2	kaevoxzjfo
1	lqikvnlljsekw	hbkeyoxgqqjlehnw	ijtuszzrq	9	iifvxxjbxjywdemh
2	okbixjuzrmoafuksgwk	skxbgpaoyzsixdef	iavkghx	9	bxqirywxugxxu
9	ctqystiokwolfgfoth	rvhzlkeaba	silzxg	9	hqwrxttroawonlivaco
1	aelymupryeq	otrcqtohfmmhfjmeq	vrwvitreoe	5	mzmmgrytimbhyg
2	wwndhzxkhovdtgqf	ycrxhqyvngeqj	azqjfcsiw	5	dasdvcejjxjkdyhp
9	csopbdsjyosa	kaiwvqhefod	rkvsdl	5	dnxvfzxsksx
1	eoaagkycmg	nqgmmfbxgr	eyttjf	6	pvgiebemnkfyvnunqbjm
2	smrzajrznqn	cfdgznqwvabt	wqpjybw	6	pmebaukthhxfpudypuy
9	sftebtbabdtuwvtdrx	splyrjxszsodmiiie	xbqigxdol	6	kqslckpryuitgvxqnsm
1	reebdyaozaxksnup	hvnnmupabulalinehcxt	whmwhy	3	jirdbzwefgptmoeni
2	kqqnxlbstluvzzirg	omsgjzgivxyhanvuqjn	phivrwwdgy	3	xqmquawxlmd
9	kbnvbzmsfob	tdttzdgrjbpvriro	ppvtexbicb	3	cmaruicvcglhloyfv
1	oclenihdtssprz	eucsjjkgsqkilg	tlkkzn	4	swldsibhkxipxbc
2	oyxrtamcnjmsu	qatuadqzppcl	wonkkazby	4	kujazktprhtqtzkiwsry
9	pvpczfdstgbw	cdmnjdvzpvutpmy	odrqvl	4	jwqhzxexpktyqlwz
5	wehtdwmfgzrq	kbsyypdqgn	rmxhqc	7	atpqcxpzapfa
6	mnbenwtqhy	wpmbezfuixfo	nsmnszohc	7	wwnjevpclrmdsxaezbmc
5	agobgvrsrclu	rsnmwxnynlluvdiw	fbfemppq	8	phswaokguognso
6	aogmjfjzch	cciayoevwhbiegqwxyoo	nrfxofzvbi	8	crzuxqkfphc
5	dywdmnvyci	dvcsdvloau	zskgwbq	1	nqwcundkxfuikagfyybn
6	hfgiduaqisbmkinhi	jzmogdnmdoo	ugwscdmy	1	gotjbatmirk
5	qnsvqaarnaayxotrqcm	hgffdorrndnsfszfkoa	vzglpg	2	oihafkcckgndx
6	hhrneiwtlzvan	dqmntscldng	extclapdr	2	mvgljxfnbfpiffhq
5	bzvwhbchnyuimensa	wlexhorumglsk	ekmspmzfea	9	gmodnmveekdksgjm
6	ejtrhjjzkegkxrimaf	jhokvedgjlsnzmfmmqno	biptkakung	9	lwhwtclkcfazqyu
5	bwybvwxyrxqtumi	sfkiqegfdfhbxcs	qftxljo	3	reqtpctfwipcfllippij
6	eafzuypnunaxvk	qvdtljzybkrec	qenqtql	3	pxitdrgqfxwnuq
5	zqovoksfdzctaz	exprshimtwropqasbw	dhcbedta	4	blourzcnlzjdztbm
6	kjyiipvbzxlv	mnelcqnkazqtqlpz	oxwkfeow	4	yjzcnxpijiztrd
5	tscpjnvxmyczubrxvrw	wepsztfhgtnti	xelzyodwi	5	mkspbxpwslyikmydtsno
6	builqzzqfdgyui	xzbqygdeuczmtx	ueigupb	5	prlcrrnkaxxbbsrf
5	cakyiulgxgmku	gmzsfgwrswsnxj	ywxzwjlxys	6	eqqmwlwncaegh
6	vgvnvuqovxmmabrvyyt	ntzdqwdsweqnu	lhlqhw	6	bcwxgakati
4	vrpixtpapgpstsfs	stlencwesrxpvgufe	hvfgdobl	7	fhtdotxnjhrydahzjs
10	deoltofiegzyypfi	wqmclrivfiaxlbz	ozizzagz	7	acmuvgtlwjejfbft
4	ytxabkddrdgzbostt	ynkkjjrhhesayh	tlaomocexy	8	eaylgkrmqfoulbpi
10	wsfukdtmhgxzxeykerg	wzxquilmlhhehbigqbd	bobdqmg	8	xozuqqnotbww
4	wiojkmfcmp	wesurkukrxoii	cayzjsdtio	1	rmxcijrbrmtewnp
10	qjrlstdsqgwbirqefi	zpsflfeeldlzfxx	osmgkxgssu	1	rwokxjeczcu
4	kydmzwgnsnrdtpvuztm	dprhhewqsjcdsve	faelfr	2	shkspklvjungqai
10	uipprcxfvyuwug	vzvrukolbga	chisnplo	2	usuuqaevyay
4	nshiuzehbxoticobyid	igsdddwzslq	ygptsjv	9	bomjteumbhhpsfgabfdq
10	dfzukkyqjwytsjxitzty	smsxavietbqusvho	kwtbsuguhm	9	pevscjuzbv
4	yrtotgptcospfdlopmc	fwkqefnuattuiot	awtfcvt	5	mevnewkfywdfqkzmgax
10	hgeocdffbawa	nfralajljknhdixkshs	ygsqhoctc	5	phrorygivs
4	tqwklknljwkwck	udcgdkgoyvamkvusjptl	zgetnp	6	yrpeazubjjjmsby
10	afowiivutvfbpyzc	gojwaietjvttfunqg	tqsqzbjri	6	jurmiouooyj
4	agcukqfjxixvnmttb	sbadffeixthcdsnlayk	ojwgrmuifw	3	smqobjdswln
10	urtfrtmlpunpf	thwukjavgiwnalu	dzkvjjq	3	wnrsnhayowxomxubdbx
4	quyfqefcjejw	ftbzthqrgneodtdhcwt	oymnvdfit	4	bzzjsnztiknvw
10	poqdhqlwut	dtejiqcbnk	fgyshw	4	mwxutdleyifjpdi
98761	\N	tyrghbcd	\N	98757	\N
\.
;

COPY fvt_distribute_query_tables_07 (c_id, c_first, c_middle, c_zip, c_d_id, c_street_1, c_city, c_w_id, c_street_2) FROM stdin;
19	gvolhlgytnl	OE	231811111	1	odudiqvzngbhqwlyxn	nbnugfywgdxl	8	jemtywkydve
19	upbaumev	OE	892011111	2	ewfbutkopyumyalhydi	jnvxmvjpwyjjhalqdn	8	usesbmwzpfngxanv
19	hxeepiiadgkoal	OE	722011111	3	pucziiopyhswbecxcucb	tnvffnodttxihijftq	8	uinmbissqdm
19	ziajyaesgovqezi	OE	523511111	4	vtyoxrcggpxv	nrabenvnosnmhjhq	8	somfzghmpmmv
19	xlgbjhofkqd	OE	004611111	5	tzancjrxxoz	kdggjstywthalg	8	vodsbfjxlokttsavz
19	fhgwcdxxqlxdol	OE	657011111	6	sjomsoujxlywpwd	iwtrmhjhlsljimniuskf	8	caljgmwllmhfyfvde
19	unagwrnbizdp	OE	258411111	7	ediyvimqkonsizrlge	lghswxfrxksii	8	hgsqhbjrvzkernanrmfe
19	refuyfhzpmxvw	OE	487811111	8	shrdwcvnpxukhvtxcxpb	cqcrzntssk	8	sahpdromyaurv
19	eupiobdz	OE	345211111	9	xrejdrumxrsam	qfosbnnwsq	8	realbiekimgxcxj
19	ttgntyuwxogxkrm	OE	902911111	10	arljmudfihbnnswvj	yuokeheftqszjuqzehf	8	xeedktrnyhmbxobwr
19	acyxxlmzsdxweqk	OE	634611111	1	qfvrwxvjgjtsbx	laayugzxjz	7	jirotbqjzskj
19	fhebzystsmkt	OE	278211111	2	excpapakqoajr	bwbeehmzadty	7	scjfxltsnfcebwiwjlm
19	ozmidozrrvl	OE	990211111	3	noiaqfjcuzmhvjrdyyh	qdehbydlvcvsn	7	mfrqnticztwkjjjlgg
19	lexapppqcym	OE	373611111	4	eydqvhczuhf	mjbokfvnofhigsqtqw	7	reobmaemigkpfuuvsux
19	overcoybuyo	OE	873711111	5	sruioilruepseakfn	zszfhaddtknhjla	7	iqpbyhnqdxpamnfimvu
19	hdxfumpogj	OE	595011111	6	mzwpqjcmme	hvmdjqyowsyzsow	7	xrmdcrwbsrbcadhmtun
19	obujpgisbheaglh	OE	705711111	7	alvsunowbckuughqcmke	agpxeigexjzbznne	7	nxwazkpwbz
19	ntmjjhfzxev	OE	259111111	8	tqsdwecayndgxscphy	znxpfvudgormbgxthsq	7	ybnoixcuborsmkv
19	hzpzayfcjfs	OE	926511111	9	nuapymwiiir	olryzrnvtzqvmoatut	7	byjguvpwvteijhrcx
19	fblplufjqkudhr	OE	678811111	10	parninlunadsmovsff	tepacxeexv	7	rptpvjalclleeh
19	xelxqghjzhwguaej	OE	872111111	1	mehqkylnoipstg	mfjjxxcgagdeeinuuv	5	okkzgpibbb
19	eloblxien	OE	414111111	2	valwsmdxbz	ehznheercyzf	5	ihqckkxiylxrmiw
19	wfkaktdywrd	OE	644511111	3	zzgewqbrsneejncqr	ujtaoabkybuhpdchvras	5	vftvcdcxemwkuhffaofo
19	ljjqfwolmxf	OE	940211111	4	yahzdutajsiu	lfvocjktjruy	5	eudhpveurrpqr
19	tlczscsgjwrykkc	OE	305811111	5	mfbglwghxxcckxhuvu	grxdoqpqtgljsdhey	5	fycdqjuadywfparkv
19	exvixezws	OE	857311111	6	yoactlpmnfmvvsjq	bpajxuqimjfhojrii	5	pxtkajmgpb
19	zmptlwfozad	OE	133211111	7	gvogqjrrastnjjy	vkxzxekclfokfgwetonx	5	lzdyqwbtxjxasrfgq
19	ltilubmarfrxetn	OE	507411111	8	lyxqsodvruk	yqksysuiizt	5	meiywtzbomlhpat
19	ijqnyqtxydexwxg	OE	650411111	9	kurkmbbsuvpbgs	ftccqthnylrklezvyq	5	puiiyyfmwqgzhie
19	sdjacnnhxdrr	OE	401211111	10	umfysvezskzkj	tljbryxzagalxibzbmv	5	jsmgwgelap
19	ogfrblsrb	OE	943611111	1	dzlxnzscjhzo	xibnvntgzrg	6	jmarlpvbneufnutnglx
19	umxfbobpisab	OE	214911111	2	tarlyxuxou	rnmumzuicqadwzre	6	nghtvqlwfxnxapy
19	qndcnhzkdgzou	OE	131411111	3	vzskhtpgkpqyzfnl	zpcpgkiphyvindpam	6	gdlcvhzygztixzsvpwod
19	nxglodydgi	OE	492511111	4	rwatwgvdblytdazph	fgczjufbdaxgc	6	ronjxpzlmnslfglv
19	guolecosywuspndp	OE	304011111	5	ccvqyiftqmhjzpb	nnrmykccsspvjh	6	fqedskjscly
19	zyojrdxnlta	OE	377311111	6	bmqftidjiemozrgmbx	mkysvlpdvwamnhlrexk	6	ajifulgiesdgfocccyn
19	yuwiqmmifxmhs	OE	221411111	7	gvkybvvhjltdlbykppt	gqjufsladwbjmmst	6	nicnrldpbravzz
19	skrfwtcpwfhsaxo	OE	773511111	8	zhwztwzodzvrzwblbcss	qygdjfvxzsgsf	6	zfgxlxgxcum
19	zguhextrnamflai	OE	291911111	9	clnoffnbbgkvklk	wpjjyeoacimcko	6	opixmimdbxjfcsns
19	dhltytpmxf	OE	874311111	10	acyqlvdfujhszqulqe	eatncnyjufaz	6	jjkdsqmpksdbqti
19	jecnrvhrgpho	OE	096411111	1	zcdppjsmtmiyohkefuv	dljsrqdyjofuw	1	jizreeqlxtaitu
13	culzqmebrruehaqz	OE	768111111	2	atlxrxfthyd	lutojniinxqzape	1	itxlrnrcsdyxqp
19	eikuchzbsrkwhlr	OE	757311111	2	mzcgnpcynlsjhayf	eongznmzojhbitxrgcc	1	ebnbaahymnqjlvzkykuv
13	ryspbbpum	OE	142411111	3	cylkeylorbpuace	dgviizakagxnl	1	hwjbqzgzxtg
19	rngczkkupauhog	OE	747711111	3	bywofairoiibwav	gagytivmgdrquplq	1	xccehrxxsufernpli
13	szvcsyclg	OE	456711111	4	gnnhjfrpsmcerf	kvlsploxesgfkodsfczg	1	kmzudtwohw
19	bxcrzyiqtr	OE	035011111	4	rqxalsinsbqkpbpoqf	mtfogelazgv	1	pyvtwuniwwhk
13	kiekjhidri	OE	122911111	5	iqivndatovbwxwjsco	mrtznmwvey	1	nxmdfnhspl
19	xhytntxjyl	OE	263211111	5	quctvagifemu	wxcqjntnmsyiooyte	1	zklmzdfeblothmxwjc
13	vjookxrwgkricxa	OE	373211111	6	iwslwdxwzhoosn	ygbuktqcqqyapkp	1	gltqaldrhxf
19	ihzmxtocwv	OE	900411111	6	aouzivxruicvonsapimr	kmmmthtxwhzikasrysq	1	ozooqkevqsrmm
13	tdoaiwtztqninrua	OE	791211111	7	hsgzdtjztxyalptkbzg	vozdhdzyrved	1	xtsmmifcbrqyd
19	rbscxedfu	OE	912611111	7	jowhaimniiu	xgkxfrqrwvvw	1	jxrcwpcnxunkivixh
13	rszjppgykfpz	OE	152011111	8	eqvoqqaduuhlnvm	twbarqlgqe	1	sqweeijphpjs
19	vhkglvfiom	OE	438011111	8	jurvhypewngwnyr	otbwfgkvkitwffykf	1	ueqzvpcnmdngaabjo
13	lukpflfiscuqd	OE	486811111	9	bzsexwtqcgdrayoy	aihnoxftnwcvicgqrtor	1	ypkvjqmqdwkrdqli
19	ftpuagncxizlohpc	OE	483911111	9	ixbytqibslrir	qkglrsxgosfwdbzswlil	1	jensdodnkoaq
13	zmdpzwiuusbqpt	OE	596111111	10	zxqpjaqxgsutt	niiksfgpoluypvg	1	tistgpnlabgiifodjxkr
19	qznjjtdpnruuuc	OE	168111111	10	lkmmmmmjwezdozpx	lunvvtqrskzvirwhdn	1	qjxvqbimpzkghtslei
19	iwzqvjbgzyckgw	OE	621311111	1	ndcnugaecdbuzklb	qyzmogxfbpefwky	2	ylubfmwtcqsoffwrljlj
13	wnfsguutpawmyst	OE	559011111	2	rnxfkgbbnenwprbnp	zhtfckditi	2	znkvzdipbf
19	gtingiiwhxmtrkkb	OE	321411111	2	wmziuhrvbeujfsqlogyx	hsgtavbiyfz	2	yicuscnsmioqpsxylnv
13	bxvgtysmz	OE	007211111	3	jlbklzouviclshay	zdtwpdbzexoyry	2	oiswbbhchwppqph
19	fnytmgrlfojalj	OE	123911111	3	hdvcdldonubut	uykprcltozitufxreo	2	ynnipnfhosphqnsqgsx
13	bnhezrcfrmpnhby	OE	351111111	4	gkkeglndvwhkttyhhrfz	ygiafnpearzwjhv	2	odqlhzkibe
19	uvhjelaiwwwvtxe	OE	229211111	4	wuktlbktrbhaxcdozm	nectakktuyii	2	esvdybncfci
13	jaahwvjxa	OE	618111111	5	gmrpmnkngwahff	jvzopyyivjlxilknwjc	2	edypnwjviog
19	akbtfcxqcdkbfx	OE	502111111	5	cdzawyygijrgfuzfahj	cydmrnpntano	2	futgkawhcliibc
13	evornbkw	OE	050611111	6	ytqykizmqsawabk	hzanxsfnmpavflqeh	2	yrtjoaxedeu
19	cihtiamcwi	OE	240611111	6	upueupltrih	hwnksphpuvilswny	2	kuskgkghwccpmtfjtai
13	anywtlehg	OE	979711111	7	zlxdniophif	jedhgkineto	2	iieyarhulzbscclsc
19	hemmqtcdeiylytgl	OE	314211111	7	jpqpnshsfqpqsyby	iprbeuttzgo	2	cijulrvqlraeslgxjnp
13	pqlgcxcxlsgh	OE	194811111	8	ypxjrcgaeaecjir	fonlbuvwuz	2	xxgwbafahjwfrcbrkyu
19	lnsfkmvmxds	OE	624411111	8	pfifvjnqacfshfjev	idxvobvgokkkbizncxt	2	vshxtvvcxvhbvwiac
13	qvqdfupuebe	OE	833711111	9	ffjkkwgyvefh	kwedpaertdvrajtawt	2	fdshoksbfqprbgm
19	olkrexrhsfjjxbeb	OE	458611111	9	cldgumyyidmixu	zgfgghxbwayfbtybl	2	suyrrmbihvanuydfmkei
13	nrmxwqasnniy	OE	023011111	10	wqldwwhweqahr	xxnogtdjakxdys	2	szowfcsrrgbi
19	mzgqlralcoddq	OE	959011111	10	ouvqejlmbowa	iqmsdobabhrp	2	eejeqjhpppon
19	grttvrwvx	OE	781611111	1	ttlbvyytotukjpcbm	twfdcziemjycv	9	vztcpfwdvkkri
19	bjheukqacl	OE	776611111	2	xpbyrvhcqovglfcj	rowbgazsaj	9	drgbyeuqmjjeblep
19	ajhomexdqaobfknp	OE	157111111	3	cuhigkscycomx	qrxwixetqynf	9	cngqpquycukp
19	ldipjjcdpgqeic	OE	140211111	4	ktpkoprmsgoagnzaggb	pugdmlrhgrwccrqve	9	xoyxjfidqhugurgfpjsz
19	fmluqlguqcdxs	OE	304211111	5	dguxosufcmmawo	hbbbrovqzhflqvrj	9	wlqzygbnwpsotph
19	dcpguswtmz	OE	302111111	6	amovgqkmxombgugydzu	jjgtwsidovsocm	9	tscxmmpatojvnjonu
19	lqrsqblem	OE	714411111	7	eruhtsyjvjpuq	vfskabilaelldwkfngzl	9	ugeffjmtapbaohzo
19	efldcjukyos	OE	130111111	8	ajuhnxqmrcx	heawupqvmf	9	abjgbbjiwxbqmuomfcri
19	sqftedft	OE	228411111	9	tgllivscpsonl	fxxyayeaukx	9	vvdoapnumytuakby
19	uxebqgzdhxwuhoam	OE	194111111	10	hfahxtrxejnx	nwdeevyzhztfshaurmwi	9	xjbmbgajvcvvjrhb
19	gdgawegyxwoui	OE	111211111	1	swzhsbuawgtefp	ozpuhjzowwuntivagpgc	3	hympsmqepu
13	hhhxqrkvrg	OE	116211111	2	eaplyexyjcmlbax	awoaofzehpeslulqxlt	3	yaarrdgbanlwq
19	zqqbbbasugdrjdb	OE	925311111	2	qtmkxsijhff	drnjdkgaydliyu	3	qwkfvuzygfqnbpokce
13	zophvgwinxttz	OE	615611111	3	khfhzbfnmvtis	elxhwfkbgr	3	sotuftcbfombjsxmle
19	rwmafpfviaduobag	OE	351211111	3	urbcdixcbvoo	jawblekgrtctzbs	3	nevokblafcf
13	ozkrcklxj	OE	658511111	4	zidlmskhdzija	jgthbpbjapqf	3	hkjnmvmdkeqly
19	tmnfemdwlyiay	OE	000011111	4	dguwbcdodlhkntzaeifh	kwzwbxogqbtgvdncu	3	nzwfydrlgsijgu
13	nmgieffsmhybxhsn	OE	844411111	5	qravaiwemdwm	pcwdszimypslpwjwr	3	vpzqjyjadebtx
19	gnhalluogpc	OE	047211111	5	uqnznludetftdyt	pfwhxovbwslcyshx	3	bqzexdhceakgj
13	zwidmdianbxu	OE	244911111	6	lvngqqenheloyjddcycw	yxzkdhfegdprseknflw	3	xrvgelybnjpgyf
19	kghthndcl	OE	621611111	6	fmpjamskiytwp	rtbmyftlnc	3	whikiwlhgjej
13	tbdnrkjiksibbpy	OE	718711111	7	ybvarilrrfynjan	ibwceehmhkusmfkrtg	3	odgfrsilabslakgib
19	jdvfchgwlwmya	OE	490011111	7	qcssmuwnhsldoj	pjpmbzkduteizpq	3	rgdhewjmakdng
13	scivujnuzx	OE	603111111	8	nogmbzcvnniofiseths	uabrnskzbwizl	3	lrxvfalhgkd
19	nixhbqqo	OE	028911111	8	ybkkaleuiqb	jvnvaflolbmkbxmhwhx	3	shqazwflbrxjei
13	aiokqszdc	OE	634911111	9	jvnqmyjorqudsajqsep	lunfnmrjifhaomlm	3	qajqtiuhchu
19	xcqjvkup	OE	514111111	9	jsamfkyemjk	zpbitzjsseyiu	3	ussybqvzfpljn
13	sxaecdgmjwrxu	OE	587611111	10	ynzsvwszwtnsaufk	htcllaksqsat	3	vtflspplsyjppalcvtf
19	fzhduvipjjfebm	OE	241211111	10	ctwmqzaibwbavsdksmsn	npquudgjvinvpa	3	chxfzgovmfygwnyoknk
19	anharvvbon	OE	923311111	1	drzlfaxnjgub	pwmtaynhdvo	4	pdfxtrkidhfigjoqyeks
13	nbeyrruq	OE	979911111	2	dqsqktmbdpldmcnftmn	diqwjdexyww	4	bgsvkfcylixml
19	kuqzlalimljwns	OE	412711111	2	hdygslrevcr	xlvxmiwcznuazqvnpooa	4	armwsbuqcowlvvluriaj
13	eiytmqdsctqeaem	OE	006511111	3	cawoqeuqiput	fdnsqqevlkrfdithpab	4	gipicodavdgotrfre
19	fznxrwfudg	OE	076811111	3	uecjcwgtaye	mhlbcanpinosztdddaj	4	ipmzahannxwwxn
13	kfhuwxhkk	OE	079011111	4	ibpocppzyrviinqy	iixyhuarwnojstmh	4	uqsqdgrazygatrh
19	riylzwujbmtznq	OE	189111111	4	gabpalgutytlkjrni	fnqtdyoaeliyjoyalyya	4	xjipuzdzcjrakp
13	dkrlsghwyvtdrpob	OE	549311111	5	jqlhifteffonhxhspzt	ganjegkgvsmrhoonrnxd	4	fzctexqdwfhlm
19	mndpsvwbxewdqp	OE	481411111	5	bhskqgadkp	jsoqujmoigspnndk	4	cagsvisomcnilwdqf
13	xpcktjsfep	OE	540211111	6	qwpzfihfrezpwlqyp	ohttabztos	4	cdacnmukssvwopdf
19	wnhlieznss	OE	061611111	6	akvflgtytrzvibyrlb	xwthcjnzlyxwlp	4	ujbebwjeqpparlq
13	sylelqqp	OE	189211111	7	clwnyjbkhpecdrr	qdohahrupqc	4	nvvbglevzclnbjpfjvpw
19	ibedebeygsztyk	OE	013111111	7	wltrcyziiz	uhsjtdlsjzdxkxucfzx	4	xyctkgyytvdhzexfzhrd
13	abeykgttcs	OE	038111111	8	enxytnrccghdqj	goagsqibdodedwrbscz	4	xpnbkrdgpczllsxlh
19	febfzllojjduw	OE	625511111	8	ubfolvlwfkmacyg	mmsfozkrohgdxwga	4	xfhtdxgwazexlzpffbsk
13	geeqfqdhffrqjk	OE	440611111	9	nvjxvicabfjdyzxvfr	btgjcygugfwjd	4	foszshlambadsnqzlfe
19	axzarsyngnqb	OE	326411111	9	klrdqcpxjpvnyauloqv	imncijolfchnpfcc	4	wjulgbasxgrcncr
13	dnitzslcuzrlg	OE	043111111	10	ziffnsdxewcaycmcc	vcyxuerxjvcnjxe	4	entdxiqfyhsnbjkkm
19	brytzrztrhfo	OE	353811111	10	jnyuliqqsr	qiigcnxwnvlgxne	4	xbhuejmntxdjpiojejk
19	lijwxaasfozt	OE	885611111	1	zbfljgdkwxrckgdsu	qiicxgzbwgoszz	10	gbihozyqsjjwroym
19	yuffxqoemlbpob	OE	132311111	2	cnemuksbutjzloh	sdojztttnzsxenzwi	10	sdajsgodidpkkso
19	lqntbcqnbczdmag	OE	528211111	3	yqauvwplpkmcfmjgg	kesjdhbktnyi	10	kufuclmiguyzoonwar
19	ndwmmrydb	OE	500611111	4	mlmxguarbmptghgje	ueqpyinvjsesuajhnw	10	xiuxusoyelri
19	xrwktxfjejk	OE	467311111	5	doshbyzrxgsgl	gruvjamdwvhwqttm	10	jyrnbqefxtmgimvstmp
19	bnkehkyn	OE	420411111	6	jjhupurpqofw	qnawxxfaal	10	uhrgkucdnnwatzlhgcye
19	qcjatkei	OE	727911111	7	bbidrcjqyhtp	pjywcmpovp	10	ufdohmbsgqjzhdxltlf
19	weoggebysua	OE	363411111	8	levuixwift	bwjctetqemifzd	10	kmuqztegwbmyuvsaf
19	zfbtpitwaymtxi	OE	839811111	9	nwbtsbwtqexqymdts	vmbapyqbbixvynaexo	10	yiqytbjgppsbhuvadozh
19	bguomivff	OE	464411111	10	cphjoxiofiu	fwofuyojctdwxkychhx	10	mtpsnurbrjumjgfh
\N	\N	OE	\N	1890	naniluyale	\N	2	\N
7	wqjqwxmjxn	OE	407011111	2	phywokmossjh	wgdeyswmthdbfpfavirl	1	rjlqrfdenoniog
7	amywzgwmdgl	OE	815311111	3	fodzozblbfbp	lktsbwsuhoosfo	1	nifnscqspyjw
7	okumjuogcghw	OE	766211111	4	geobsylncyk	kjyvmaesozmrohwcnel	1	tmsztehqikakxkplpiw
7	jharvvxesyx	OE	308311111	5	exyhyrkldglh	gcbksejckovh	1	imvvruduhhaaz
7	vhqdipehqqsfwsj	OE	767311111	6	vnywjichweatlzhpkxqp	dkutfewbfld	1	ozxcutslyajloyrb
7	ozdjgmeajz	OE	579211111	7	hblidiqrgarjxrieu	lwgcydhqumfk	1	jduzeyhuljjeseqrfvl
7	frgfnsweqjejo	OE	502411111	8	xhqvzbrmph	liwihsynxqvozw	1	ozgwjptmzobqdsra
7	lujssleursxqkrc	OE	746611111	9	hilhzgjorobecrcc	xeavvbijclgwqmwwcwxp	1	jwdvjabmfrzdovvoqvnl
7	fvtwwufxy	OE	646811111	10	ecqmpgnhlyogrp	fagphugrodxhs	1	hxzpiwthrkqwknzv
7	cladwrfqb	OE	156811111	2	ejhznkjnatufbsxlwok	ourorokojlbt	2	bvfbcpmlspnjgkkhqi
7	lznsjeffddcjel	OE	131611111	3	quwhjbbubhjht	cwkcasiitywbcyxqkzp	2	gceohewpksuw
7	yfcrylqsqqfibi	OE	519311111	4	cbytmyzwkhtrehyj	apevnextmrihucpza	2	jxuhgozuicqltikfgg
7	eobsagaulxeqq	OE	210811111	5	udqldohulc	qsagjqdwmqbrvdoupztz	2	rpssnqahckkljs
7	bbfeyrixo	OE	924011111	6	ctxghdipewsu	apkqrfvpenih	2	ztbcjgnycxvykupzqe
7	qvqdhqcbfrbk	OE	086511111	7	oxhzlkejtxjhedpf	iwrouwubaqbhedj	2	putwjwlyhzd
7	abapxrnhvlirusa	OE	973311111	8	lixoejoqtmwyrdkedhf	qkenauvvwyv	2	bkhcnkwdxjlyrgzy
7	unptkbvafmmguwy	OE	705511111	9	ocidbmgykamedws	dirmmnbpwaexntcuodx	2	hoanyslkfgragirkjga
7	qggrtjubqkpdw	OE	856511111	10	hewabyexgtwhu	uoehptisbr	2	ohmksfoxthuunyhcqn
7	ugozkzcchjyvck	OE	865511111	2	empfkwqgsztbarfyvra	mldantoihomhtiwnn	3	gumwalurhzxuctjynuws
7	ykjupzacejsrtb	OE	163211111	3	aefqevixzhtswd	qqjfskwvwpnmlu	3	mdpfcvcgnpgbapkhik
7	qlyrcynqdniun	OE	891011111	4	vhooutpfexoufecuiy	yhamfyzerl	3	myiprhdhlfwgpqt
7	cwlxaqkgglt	OE	980311111	5	rjlozancnxtdfjq	zuvdcxeudosvppjlrf	3	kcixygcljggzitmta
7	lqxtanxprpqu	OE	794411111	6	tyuzdxfitofbkwnazx	kzinpvqzlnvnkh	3	fehrxkcoxmqiomldk
7	uzsgfocyj	OE	266511111	7	lckrdpzyhkahtzi	pwjcaafwaacofjvmy	3	ietangcibbmihhais
7	dftfmmkxj	OE	676911111	8	jwzfrfleqfjrl	lrenmvwyotqslgqgie	3	pildwbvqswrnt
7	nsrhquqmrbsrev	OE	027011111	9	byjfjcdfowjldedquro	jzghheejfxws	3	qfmuppirvjfrdetcdxdt
7	zizcaiffpd	OE	962711111	10	agbgwjctqsxfhuffkfdq	cojojdklctkppu	3	mwpeoggqomiovpyam
7	kivflkevtdg	OE	320211111	2	vbotojavatebnsfsgrle	wgsqzvqpzsfdiymf	4	bjahcqfoheoyzdfu
7	ajuvcinubo	OE	118211111	3	kmgxkiwjlyswesk	lrzccliszaivvwx	4	bejcefpjasstoqncoq
7	fbfuclpqneg	OE	507311111	4	tdjdaryhmue	zmgttuigdpdgr	4	mgghrasgffbl
7	bxvuvhmpwwhfvk	OE	371611111	5	brxhkhsqdpfvqzdbunry	ffdmalohjeanlyoegpq	4	gdiszuwwnjzlrcakwra
7	pisgcwllhbku	OE	353211111	6	noerokbllhyfgumhr	roqlflmerifklhle	4	xoybkxtqgvwbbmqxsva
7	wxeadakcp	OE	867611111	7	vudhaglidqshconftk	qoprmnewuggrmz	4	htpuckvfqwbchmesrvpj
7	clyctqhl	OE	475611111	8	rmjmsngbpecbcrnf	xezndgvpabtpzw	4	kdkoflazcdjxdqguftl
7	pzdoooszivgbt	OE	312511111	9	rmdlmbbchvhjacdoy	aybnctrskmstxmqrhxi	4	aaufybkdggo
7	wxtyxefgft	OE	085311111	10	rhozuyynwtwcpgnid	wryznohgtwxwo	4	ciksehkfuri
8	uzsrihorkzjg	OE	226011111	2	dsqhexzweykiqj	kvixqybvktqpqeub	1	rkgkoxtjebyd
12	pwivjryew	OE	143511111	2	twmaalwhrkiynkwkrlbs	fqwsmezdldj	1	emycinztvfnrbkneg
8	yyamivpianuwvyml	OE	747811111	3	ropapwcuqqwucrzsajz	jhjbwlhtnxmm	1	bcacacrbfxkfcjqdfg
12	plponjzfubuv	OE	349211111	3	bdowsieuogxnhz	jyrjulvvpt	1	dywqcinguwtyfz
8	ugxzacqcpzeazlmj	OE	679211111	4	gewvjbwegcjoiukaquz	ulmgmhbhqvaredwwu	1	svawllwnnrufrnjtbus
12	aeebsdixqommykc	OE	250811111	4	bqvlavsshrdgmgxzb	caxoauksqxwdkhm	1	xvkawqlafvpei
8	hlkqohaysngzcb	OE	119011111	5	jejuzafuaezdmq	wcfxvebaqb	1	pdtzpkuglilvadzlhv
12	cwblbsuxvyq	OE	711111111	5	qqtznhfubig	tzlqpjgmdrvnsz	1	snmxiudjapptjc
8	cjalbxvepdmzog	OE	257711111	6	ctlyqjuhujksga	ozpifulnex	1	ckjcqwkdolamig
12	rpyduvhquos	OE	102311111	6	fpnbfggsyxssai	krndbxxrzwgm	1	fnwifijjcejxdmna
8	hbijyfovkf	OE	448111111	7	xpogfwgdvvx	vkcblmycmcoqbyr	1	csdmqxjznlyhhfdg
12	ffilipmgmrkydpw	OE	172311111	7	ycaefeygnogjkx	lvgdsxbffmifhdyxl	1	lotyskxxwdx
8	vpprgkzhabshau	OE	190811111	8	niiuplqybokeyqgq	eskipnwuuxhcghmohz	1	pssfljqslduwsda
12	ltngapqgzfvs	OE	448911111	8	xzpnzbdiqebxscnam	drncpwdwohcxfqnj	1	nirumccvbmhjm
8	sjsfqwlkzifjuvmf	OE	775911111	9	moxbrybclbnxr	mropqnardo	1	fwgqsznpop
12	qjzyvycbdsoeahw	OE	244411111	9	dsusogogddaf	zqwabofgdqczr	1	xvspafujqmjrmwhhpe
8	gjbwdtubfpblcer	OE	178111111	10	ojxvmmbgfftezlv	wclhyrrrfwx	1	vrmihookfxilqlo
12	smyybwusgqu	OE	846711111	10	mjwmclqejkuuwukvjgvq	tihkwbmmvannnpmw	1	vdbreubsvsmajrkj
8	wobbtwfchxwstolo	OE	671411111	2	ylrmojhtmevk	opmuauqqsnoyn	2	arhrqyirtrggyu
12	ywsenenbooulnv	OE	283911111	2	djaqgqpdbxqys	uyphsyolerir	2	zseethtykaeyhtf
8	onsinexhtpr	OE	774311111	3	lfowvhnkfgqvle	dnzbxzuhwwlveqomc	2	ukungeaywezsfphfhvgg
12	ytqpmjgvvjysd	OE	858811111	3	pwweuutlhuryot	gjwgiiitzuwu	2	msaywjpqienurxepfuak
8	sfwdftfqqwhhtv	OE	256111111	4	qyuklacjsmverdjfpyv	yfpcxqsyqwtbrvazoo	2	pvtrjvjfzvmdb
12	yxobwjeojsqrokz	OE	409011111	4	nnuzlaquat	axzcvxhdqtygjx	2	vdrttjwfdjy
8	qxvwqdcnoiqe	OE	011711111	5	lrqrbbeccgj	lwvdssbqgybgrb	2	leqjmpacukxmsnwpgbmm
12	wxgevknyaiqdz	OE	381211111	5	cnopiyynxmqdcvi	xyivjdmfvorlurms	2	ubnnmbgpccmebi
8	vqbqohygkyu	OE	917911111	6	ltvburxgrrbglxmhvb	oiogcrsfttou	2	ekygiyjhhgdzhwkhc
12	dbpapsbodga	OE	260211111	6	xeilbgkjwidjxjiwx	yiqemhrppuezdjjmcwn	2	tfxuhtqgmphjo
8	rkbiufaiamlcp	OE	900711111	7	tmvzskltcvjuuy	siizqngubgom	2	bayngqimorku
12	vnjbgmwqp	OE	429511111	7	xrermjfvtkwkcnq	awxvlvlitjzpyidom	2	ljpdrmmkzkmldd
8	ngsnoutdyjgz	OE	027711111	8	nxcnqobedn	cpuekxlkanrnhniopg	2	ubatkdkwphu
12	bmtmwmrwycbbjrh	OE	553011111	8	vnhzzohkhvtabwf	mnlllvrxdxvtmdkq	2	axuiwdaejb
8	qnuwdhqa	OE	466311111	9	ypisvjewibu	nbktdpjjabfkmi	2	ohmqhmxfexr
12	amlwrkxhof	OE	374911111	9	calimsglocgn	bgxmjbksodhcaq	2	rewdewitlcd
8	cqoanblf	OE	559111111	10	yjlfxjxzhif	zulgrlfdvfmnf	2	rnkuvixzphzks
12	nvuztagzqpdzzm	OE	903411111	10	mtlenetjdffx	upfqjbftkhpfoewyjbev	2	cdnimizcrzxf
8	aumiygwkmy	OE	093511111	2	stfnxodrlj	txonciaxnub	3	zsqqvjipbdnvatizdj
12	zmhkyqsjzvu	OE	066011111	2	xtdtathuonzwlohjycn	kbknoafyzmmbfgud	3	dykipyjseoae
8	cxdysmtrmrifwe	OE	106911111	3	itsbetamkbp	erlkzpaatch	3	beiplbfuikzazr
12	wqdfqnyqo	OE	579511111	3	kqooiizwvlt	uezcuffukh	3	uxcdvhfttwsi
8	zeuveqgom	OE	412311111	4	ozgbdnzvhmcdcrr	jgqwmtcujrbpjtjnreg	3	sxyosvwmlhdrxy
12	bmksjqhaedp	OE	015611111	4	msaqfamlsbqoyucnnsi	gyzqlybximskxmp	3	rbklnniigrw
8	ddlmpbkgim	OE	452611111	5	kxfpadtpqlcyvujsupi	dkxpembxeypchqgx	3	hlvziqcyuwuf
12	gwlknqxy	OE	610811111	5	upuhxgiqei	okodcciukqk	3	ypyqxpwlca
8	laucqfyxsjntquuj	OE	644711111	6	esovqmwvjddfign	fkkaguzyvkqtczrh	3	itkienfcrq
12	vkdhmvqnwinxoifz	OE	534511111	6	uweitfydycgv	werrbhtvsjpyvhxdu	3	losrfzopimcloegafga
8	rgaftbulcgepe	OE	323411111	7	xaqgmiiriodusjn	slxksuecamfecms	3	xcvqnuamcspfgpw
12	jtanktzhzizw	OE	765411111	7	erqwpzlzki	shewhagmocvwemenuuw	3	dvpcfvianbpach
8	jqcuwaupulyy	OE	581211111	8	ncrtcsxvaguqszaffg	jrqaauzopwgpsmy	3	wfrhuxhjalbw
12	mxorprulfxj	OE	368811111	8	hudidkqnbbvymdwja	irgxxycsmptm	3	ejuaslvztmia
8	rbwnimxq	OE	712611111	9	ejysyihbcb	eyysyapopxfxjncf	3	uvrkwvehengqqy
12	jnttqmyawn	OE	580111111	9	avexieacyytf	rolbtvyfxrwubpzb	3	ghoeblxncirogiwj
8	wbmceyanzxdeyq	OE	761511111	10	airpgoeentz	kznqzvbudxxlyswq	3	agjcywwgfiabptqt
12	minftvicxkjious	OE	755011111	10	geqfhpdceiygx	qhmfvtmmosonrsz	3	anvbilismhzlc
8	zqqafepknmglvty	OE	982111111	2	iqivmwnjidbnyas	jqmfhvosksg	4	kmwkhifhozvhbfupxcac
12	yugzacwwvponzhq	OE	452111111	2	xttrbdbfpulp	nwrlllursptgrgr	4	puybpaxiinqa
8	zsalgybf	OE	675911111	3	eabywitluc	zooadqlxlc	4	lejeqxpcgwflxnhoeasm
12	xwjqprmxqwmi	OE	779811111	3	mbucqofekvcw	xmqoskwesagvjqrfygn	4	qvprudovfiqtqc
8	epcguvmqgmyaj	OE	501911111	4	jednygbtgexbpjrwk	cbepawhammizvbfug	4	azdgyavuvoyrcu
12	rgrzrlwdapwp	OE	468411111	4	pctvurfgiexsdcaecvmt	rkjajgsbeclehsdncw	4	ezxlhqmrxs
8	bualaojgnsngneai	OE	032011111	5	zvxmvvaiobuumn	tgxqrtpdnroqjulyudk	4	xnlhhusnoovnqtwpmph
12	oeiqjepjcbrndr	OE	402011111	5	qhuznicjsohg	rmrsczvbsgw	4	duwskutegdmqckpuzesu
8	jbzbwoassaxsjhbi	OE	696711111	6	avtbylkvetnfmzu	qrgjskmwlbgfdag	4	wiovtqkljvfginfd
12	ucfahzvdqeow	OE	864911111	6	svmidmmaotlfpt	xounuzsamevxlocymv	4	pywubdjadipuzmgjvd
8	ogfuwaenxqnf	OE	672311111	7	ppfbszound	uplvtwhwawklq	4	npxixquigsufpeep
12	zbsiogadsioxwyfm	OE	141911111	7	pdqjwsumxynob	fukzvedcjttcgrb	4	tfiiwuplzedy
8	qdiaujtpjhnhb	OE	538111111	8	bvfaamwzqttyyhyvl	admoghjpryru	4	vhmgcjsjagqyfblk
12	uxxbkkzif	OE	976011111	8	kkkrbmhffzeywyzpqmgc	gzswpzpsfkfmgxfkx	4	puhdynepwq
8	sohcrlmnla	OE	143011111	9	iiczshvbuuq	imunptvchvomo	4	xvuwedrcijuws
12	czofgnlodnxg	OE	504511111	9	xaquciwgdwryxkvz	gnmqyqaekzr	4	zraymfyewedltcqdov
8	czwxwbqcbtofz	OE	587411111	10	mvrncuzoni	leoyvyvbkhbxujjj	4	qjdyzditqotmswpb
12	xtxwtcovbw	OE	115011111	10	njlqnwweaazjzvunqxht	bpooexqmvysxql	4	wriyfhfcnybpcu
11	mubynuejylncgl	OE	917911111	2	vmpsumtutxwoyckmwbsb	iohnmocketndfz	1	aaquenvowbo
11	lxgjptcjrahayh	OE	392911111	3	nsyknrxmrmiq	qxazosytztzsrkji	1	xowlnlgrrebj
11	hrmohntwipcm	OE	055911111	4	dzqrtvjauhrrzaydw	emzxjrwvufmcuoddbegp	1	bpebhhnnvpaw
11	wcgtowffdbanb	OE	782311111	5	stjkgehgitrvjzixrr	npcsxhjkzhgr	1	ccrrqtuuqia
11	xvgtxfgldfe	OE	317911111	6	sxooccisozklekcchsbd	hmbhbcmonwyoooydfzvg	1	hskinozhupcolkiretd
11	nlmusvnykepnz	OE	012811111	7	qexsgyewqbqbcmfy	ldfzrzddbzeqsjsmr	1	kcfpliyvfnq
11	lreklwcdat	OE	031011111	8	sdjenuiebzobn	phlmhkwdvqfk	1	dmyzvxnoqmgxg
11	netbjlpif	OE	727311111	9	mpgesdokjgemhnwwais	xncftmdxiyfzistx	1	wpcnkwbsjqjbshbv
11	uohspudssnigjvoy	OE	831711111	10	amlnamgtjtojy	oecqdoynydpvrt	1	mzyttevtnjzm
11	axakaiwexxf	OE	375711111	2	rmeunlnqzd	tfhkrkxyrs	2	hygpjmorbtlaubzclbmq
11	xqdsvfpdjvmx	OE	684511111	3	qtumljsrjfzsamdnvqj	xsgelkzrjahmbigwc	2	dzpmdgujymjrwz
11	lkqnodbwwqiy	OE	590511111	4	mffxinrind	uykbhbgdswwow	2	wzqzeediwgavumpv
11	rscobchgazx	OE	527811111	5	ykezmnahqmgxf	lmuuhiozjevcyg	2	wwgaqodorcizf
11	uvurezgezqiyz	OE	184311111	6	kbbbfnfydbxzusrlhuxh	chufbnpdegxyoa	2	invpufgqsywabmtk
11	vxvyrnsgh	OE	224411111	7	vfunszafyr	byzgvpodqetezw	2	pkljngckxfbd
11	mdrhmowkhr	OE	148411111	8	tfyagdodzu	wspcgkjdfbbzutt	2	nldwdusaetxwn
11	xviktltsbcbtyq	OE	088811111	9	njqkszzaofg	qblaldkhctztfpjfy	2	eqdwkzvbpuynkfeanu
11	yuvatrrg	OE	222911111	10	soerekcznz	fqixsghlgxa	2	bfdibsdndrspzvenpp
11	rgubrllvikp	OE	280211111	2	skqcunhfjsirudkkvz	rkwfiwlchkeok	3	etkhjlnfalskmv
11	gdfzqhgiog	OE	312111111	3	msjquqwmgfyphfvnevxu	txarersitaonm	3	rhkxfkmsnsc
11	czdsxpnkdncumk	OE	521011111	4	hjvtbezfbc	kkzhotjzrbrzo	3	bhvzcyfbjt
11	irutphoz	OE	438211111	5	ddmpcaugdqu	jfdkxmbgvcemeq	3	xcpwsptjnkim
11	kwnplewvcvwaruck	OE	894811111	6	bidrakazrx	znzsoiimgugh	3	ipwomrilumoffgqexrs
11	ftgvooudxhcg	OE	561111111	7	dttjqzrlavqcijpm	vfdlihqonswmgwzff	3	pgfzchgrbpgyfq
11	gkhfjcfbpkourgli	OE	080511111	8	csusocfeknfxnsnv	amtekbnrcsttlwnw	3	fxrhwmohdf
11	rizulsrwtrqv	OE	595211111	9	xvavzihwis	wzhiswkqoqxdprru	3	qeisyyecawa
11	nxrtlqgtwclrfl	OE	282911111	10	gvnnqmpqthxlnownml	rzpykgvkkmzd	3	tezjgphxmynyz
11	wvgplioq	OE	843611111	2	eusyhunceomjkfnedwe	omiycfixhx	4	hiaoavethddeczxl
11	goswvgxcojecwud	OE	288811111	3	trsunnpitj	nhirhdwjeqhgqemb	4	wwvadcrcxnjcgnpkjbrw
11	axhswrynjtbxzvuy	OE	564811111	4	uuerbjmtzcqkue	bupvpkgxamvbvj	4	szjaumfzuntj
11	wtxrisrqfqnb	OE	812711111	5	uuqisnmvoqepzhiifbz	incasjxnlelqhnwwr	4	ujsgkmahpguosnzrlqvs
11	hxnutighij	OE	816711111	6	tjuloizwhiudc	rdhkvkwuqv	4	vavbzkugnfmmxucjamm
11	vyejqfgfqtcgwt	OE	865911111	7	ifqiuwsqcsn	eyemgmdaqnk	4	plmkiktbmfcmbaot
11	veqyhzxpizwnt	OE	642311111	8	tfqlazoblgtgulgzhfz	filsnqchfiziultqud	4	kzmgyrbnyvzb
11	bqjhzwhaob	OE	346411111	9	ukfpkllkkcqjfowjb	krhfuhcxyxloqwpra	4	utpadzlylkszhinyvhd
11	qaelmtbbqgkynx	OE	331011111	10	ounpvozjuclfanezuur	dywoauilzbdhawlzqojt	4	enhxzbnihinwvk
9	peyivkinlubbaty	OE	842411111	2	xpdtolphjoledvcr	bdrjvnljgaj	1	mtlhxwjlrvplojmnenh
9	cabyyqfyxm	OE	267211111	3	vcjgmkzcahqgcznr	minnyrkviwptq	1	ackzhzvbvkvuyur
9	xhigedavg	OE	273511111	4	vpwoznhccggfynchlyp	mgxhrwluovitfnajqocl	1	tzocxivfoyvneo
9	kzqmzstgnlitfeeq	OE	830211111	5	tarkajmalgdtleu	jqwwsqucuikdziry	1	cexhdtwgatlw
9	nnkzveyvdihucov	OE	913511111	6	heseskudoh	toakppzdlgtppgmgtu	1	pmzimncspynqrrqwkezz
9	ixukmpaxdjkdnfb	OE	602611111	7	braonzjjbtxcfgzgdfsd	eojgshrzhctzsdfxpm	1	wzcvijfjjfdnxy
9	afdkyjucqt	OE	108111111	8	pejvacbplubgjribtxqu	idlrapqwalpdf	1	jqtwjfsxncei
9	tcalgbxlottpcz	OE	377311111	9	kmeuijkrzc	muxyznqtnqq	1	uhunhjinarhqderomrkb
9	qquhpzjhjlspaqpa	OE	081511111	10	dusptwaflbogvszdw	xwufwezpaxpi	1	xnxwdxqpbaopzz
9	qspzqdsrlhyoouw	OE	298711111	2	nihgofcizwmxaycxylmu	nkxpfvwjfmpafxbd	2	kskkrbggdtcjicvjkvq
9	syqcaovsj	OE	597411111	3	ikfhmrfyhqjd	isbqkrxofmpinyudnf	2	hchfrxezoudfg
9	ziozmzjltbwnw	OE	040511111	4	lcwkeurrramlcrrjl	ddelvniklolqvlujsxb	2	nafbyrfkzbpaqrsbu
9	zwzdpgcoyi	OE	061611111	5	lczevwwrrdom	xqifagdbpbvrj	2	bwtlbvbuhjlhgzj
9	roiivkckd	OE	935411111	6	pnqvmrkusgwqcvjdiv	lgyhzbtfmfrme	2	sbxqxqazfhy
9	pbxcbzbvemeh	OE	448211111	7	fapjbqmefoqhxwacabhd	dticqwxyybrulioqruqy	2	gsiqjibdxluzc
9	xlvcqznycfqmkykn	OE	956111111	8	xialuckszhfdqi	qsxlraaqgm	2	dfvvvbvivaggbivi
9	dblraivrgwwzi	OE	199511111	9	fciijcpxbmgtpxlsfiih	nllazeufkn	2	wdaauvuwhymn
9	akeirtkvuk	OE	134911111	10	lffajzzkhz	xpluzclkzfylmepnd	2	tazkprjjpoagcz
9	lspjapsj	OE	543511111	2	sxplrqkpybgaxwuqli	idycgbetlohfqqbmmc	3	ocnfvaxcuofngienk
9	agiebnnzblli	OE	203111111	3	ectyqleqjiupqyl	ogjmrbjsmuiftd	3	dhpcguainixiaqzbjday
9	mfscsdrtonpsgap	OE	100911111	4	xhmbzsuvlfddd	xvjimhajypqkb	3	hyforlxzksyrvftjh
9	moqkdmopriglifk	OE	292611111	5	dajpjvlcrlxnbmku	osliulmskbhqo	3	kybtauvrumrcsddgj
9	dlgbaikzxuiuib	OE	972811111	6	sxvhlcnqlqaiisxfpy	ddujliqwcgimwbsjskjt	3	hfglckuumkicbpmzi
9	vifuljopdi	OE	660611111	7	fszazzsudtviwbu	mydibgptuhkpzvzrs	3	ysgqbjppaftnqyx
9	ehnmwwpgdu	OE	622711111	8	rhzqrylcqu	lxlhgxbvij	3	mnqgqkajhwenmd
9	utgtcubnsmzjkvp	OE	820811111	9	nhktlvxmadcymhzxsdnk	tkgorwoenwjehwxoxkwh	3	mlipnnskncanvem
9	hakjltfvragppd	OE	673311111	10	evqrswsijotslffwur	tzuaiwmlbgxpm	3	nokmalvcfuumnouijh
9	jewznqvkjuzfw	OE	431711111	2	tuqggxkaeq	vvxmsprzqnripgerborl	4	tyehprcrhppa
9	soiidlrm	OE	076711111	3	riohdnxuzayjlwoen	obxreyodmj	4	leychshqirrsdpvx
9	nocjhrogpf	OE	423811111	4	kaiepxwvmvt	wcvzrnxqqjuliqxaf	4	bxohnsmsecxajizmgbmd
9	vxyhlwpfcnikj	OE	913411111	5	xwhmmnktps	erlddcydwcvorlidz	4	hkmokavgyc
9	vzsavtnqdnjsj	OE	972211111	6	ktozmauwcxybrkah	lhnnxrixmcjo	4	tqolmafctmeqmnqf
9	gxpnnvgududuqds	OE	219411111	7	uqibtbkrqsh	ifjnmkafoghlhidum	4	ueyoedzrrie
9	anrawiit	OE	348111111	8	morfbgdhndjwr	rghgzyaxfmpkz	4	nmirdtlzmw
9	fxhfevgxk	OE	126911111	9	hswrwmeefockoiql	zewpyszcfxuzix	4	ywuwrznhontx
9	dbmeeftrq	OE	572411111	10	xheqziyuhs	wfzlrhqfombmwhjby	4	fasrshyihbmhnpb
1	safewereti	OE	\N	\N	sosidelali	\N	89765	\N
17	xgsyqsjsw	OE	152511111	1	aptusvgygbd	eyyrkfkghianxbvqco	8	splqskxuvekibg
23	fowfjkvzhe	OE	731711111	1	eocpzxpbykjauergddt	vmsprbrxkauk	8	nbnoixsigyvgbotmlf
17	eourpxjaqrsb	OE	478911111	2	shbipzgrbu	ooxgebtlasxxtbijs	8	upcrwocyfdwcfkdllayv
23	rflfbvmzh	OE	296211111	2	gelawkazlxcdpz	vlcqlkpshotxq	8	hpffjppdwmeuwfxyvcu
17	kkgmmpglco	OE	294711111	3	ppzjoifjzwqy	ftwwcmbttsybuquzc	8	kcjsfsfdrivhtg
23	dewbayrglodfvz	OE	877911111	3	qotjnjoiwcgm	dgfznaiqjbeolhvboft	8	xxinbagxnqxadpie
17	rritqtzoyjfxybc	OE	620811111	4	jqnjhisqufvbnlcvyb	lbglcmnwgdjpslhjde	8	nsrmvijtespvuqv
23	zqlixromyognapws	OE	025411111	4	yupzcvvlsfxyys	mwujcxsqjfumst	8	zkthdahiclkqqetpr
17	nchsndxmx	OE	164611111	5	zbviujumltijlukomtm	vqncuaqzoo	8	ghxjbdjuvdnitkqybowm
23	qxpnlltyxmlqu	OE	184911111	5	pkqaqdgiozbbttntuqfu	zfpncazlgah	8	hlozzitckooqaaoj
17	nheiuzjimufhbf	OE	683011111	6	mpbpbcaypkg	nixxsedwfhhheerue	8	hykpaokint
23	rjirmlrkkvkd	OE	604111111	6	rixjbuzvadsnsahprd	nduylirwsaapnnnpdeop	8	ixdurikdllkw
17	qifdrkaycac	OE	559511111	7	antcidtvkcqypnjljmtl	pkerxzgkzsupd	8	kaqrnzynpz
23	wwcsmsewria	OE	582511111	7	mpvuahahlsrve	kjcxfpityyhkbetna	8	deqqazmruq
17	aoeersocpbckaqw	OE	785611111	8	urnwvoblfqsjaniw	anhbiiolfy	8	kelvzejrdhihuvtfvjah
23	gjddmeavvtty	OE	862311111	8	tgclwcyckkqzci	thiyvnxzrr	8	scmainvomkefphacd
17	xblhzodocxa	OE	252611111	9	kgglmmtgxzuiynrz	qconwaaqrovqcfmehiv	8	hwqrljfvgpztrzc
23	evbalhfdrs	OE	005711111	9	rzdhigkxwwa	wrrgprarzyktjpmxoe	8	bkneinwzzepy
17	lwupbydbl	OE	719111111	10	ykwoqucbehzkyhxtsbse	rdszhxlfoas	8	zgeaevhhdxkdfhifljt
23	ktkbzkiup	OE	749311111	10	hpxebyrtkcxhwa	fwbwopynkffhu	8	zvuopbklgzmunrsthgqq
17	pfyjnhhxrqrlika	OE	346011111	1	yrybginowfssolylneh	mxhtnnpfhekcwlbaqct	7	fzgseqldwz
23	ucisqwokytwsxw	OE	656511111	1	ezshlbkuxvzkjwlf	wntalxmevfegrqe	7	ivtnlzjoypkumvm
17	yjexrrgkikhbbhrx	OE	733911111	2	zwbmhutjng	afusdilppvuxik	7	pzyrgfpxdkoiimaie
23	ifbrtdptabmidbxx	OE	061311111	2	tzuigyjonclofuec	imecelyxrtaykbkkoumo	7	wukbtyugwkopzh
17	swdvbvnmbb	OE	565211111	3	xppvuojvwjmbaaknk	heqexffoyz	7	nrpspdaftdjde
23	eowrljfc	OE	496411111	3	vjlwsauuzgbsgkhmadh	qowjbklqwypusyfiiaf	7	mirhtsmjmamufgptj
17	kesyzibvq	OE	859611111	4	hacvernnfxblgfvncqj	vidwwepvikh	7	lkwisqkbiykbugfd
23	eoeiftljqxlfuxf	OE	056711111	4	otglbzhduskzyajgfnb	xebdbegxnlhrbjywxi	7	milcdfdpwxixb
17	ucfzfbezcshey	OE	338711111	5	wftjkmvjcmzgwcnu	uqktcimthnogityh	7	vkjqbahxlemmv
23	bbltugsrznwlicrc	OE	355311111	5	ytkseakqnu	rbgvlxvdmrkzgedvb	7	lemsflqydlxknadxjr
17	mykiedyyrq	OE	389311111	6	dsabickribj	wzzjffgcogdukeq	7	okygouzxnbdmsjvkglzt
23	ifpcqwlu	OE	631811111	6	akhmaffvvglxrqty	suqlltcpktwblpa	7	rlrwmszevnalzhegzaa
17	teythezuambr	OE	200911111	7	jjohynlmcrdifz	xafniygnudhjp	7	dlgqxsmtmwjxmtmau
23	skcxswwkzyjgliq	OE	751111111	7	trlrhbhmoyoawbhth	irqbstxmzxprbkrvcunq	7	svxpmvmgwmfwydiwgc
17	hbferdbru	OE	896011111	8	kcweahbdtagcozokuu	fjblotedjkwqhbexn	7	jexbfdhzzwdrpgrsp
23	axatryyrihpjugxx	OE	121711111	8	ywifhmitnetgkf	ncmkavmvirermzksp	7	tncxkekqlvet
17	wkverzqprhpntn	OE	745111111	9	soyqfsyjycdaz	kkiygcqftukpukllxzfu	7	bbbjfoxekoktlrbmjchz
23	zamfeplxcyubw	OE	209411111	9	khclqgxfvyocjewnuism	urliwqiwwtdwzeycrjx	7	fljzxdfkxwduzphey
17	npajfehqgce	OE	849111111	10	llkjuegkfdmiiuv	zgtrhcxtlcuge	7	cxdtiasfdavafvxjofz
23	yfkitkmzha	OE	668111111	10	cfrfhjcymqfrb	sqhnuudwuqkmfcigzjag	7	dntivflrgeceomup
17	ugrgdkyym	OE	898511111	1	ibfeeikdbuzh	sgdrkfyqxwosxo	1	tzaxlsargckzzump
23	gplmsbdtufaokv	OE	833011111	1	zvleqlaoypvlgi	jjxycdxrgga	1	gjjivujxwddebezi
5	nhoxgtmvoforxi	OE	364511111	2	udmbonkybb	fvcshnguckmnuvusrj	1	gjzcqyojorurvz
6	onvtnzoxljhzhau	OE	500011111	2	igpdwsptyuvsqfbiusl	dpegjnuzjlgbycz	1	kjikaqrjnxelvd
17	tztlwlpgu	OE	609511111	2	bolstruanrdigcmioh	evgvhnucfx	1	lxyimdjubjr
23	yojagwvpeftmksp	OE	881511111	2	mkcicmsaswckj	xvvfxwsungbd	1	medvwmisidnxfiiytjb
5	nhnmjgzqjammgh	OE	542811111	3	bzivvdbbfjhxfqye	shatqqmozycnhgi	1	rncejaqufkqqfkwnu
6	tzctxuhala	OE	814211111	3	mgljhykupjurrxwoh	kwcwdizgdppwwvljkjbh	1	fvotilkgomnymgfwfof
17	rbwvivzzixjqkko	OE	072311111	3	tktvorfgbgyxgd	phhekkkppdfmxgcmgvy	1	kuigpqmqamfjbtdl
23	gyvdnicagvsz	OE	580411111	3	lunvdlstlaa	boytteegsl	1	nrpfbixbqnvtn
5	uvobawusekwdhrq	OE	898211111	4	jftxcrcensfoy	ydhfnpccghlx	1	fqwimbkmoa
6	msazgkhssq	OE	843911111	4	ktgaukaetjvmvtr	yvevhugwimkwmn	1	zdnqgtqftdakhx
17	hlwdwrgiwb	OE	751511111	4	euhvrywigeh	lrgvwknbdawzqqmh	1	hyygttbkhp
23	xdzsqopkz	OE	295111111	4	ramovyomiqdjbbtpanf	ngltfkxoydeis	1	rnxnvbdyvefpkgzqfd
5	wownejugmiofhpa	OE	639211111	5	mrvbvlnlczw	xepwymmlkljo	1	pjyatwutsdy
6	phqtnqkkyg	OE	689311111	5	tzlmjfehkaikkmywoin	csbxunackdmzufoatwr	1	gwqmxvcxnbosykuesjl
17	azhyhseynsb	OE	688311111	5	ncdrfxzdxjqipetskxj	gydmxhbutktthp	1	jndsmexwupwqpyefpxx
23	pmewsjrvn	OE	179511111	5	vptrnytjviidqmieasx	cezeeroopcdp	1	mqrcatujijh
5	rmilqasrx	OE	989011111	6	gqvvrqdzuogkgxzfqa	xihtpaycotdgvf	1	eqavgvtgkthteecz
6	ikhgsiwawumzhz	OE	208011111	6	uvcyrphiqlldlzl	ikbolqzycwwjm	1	aqhqypvkzbwimszniqd
17	woylksfikpftkc	OE	916211111	6	lsqpvidzqhcjngtfinb	unwbhzttxrzohyddbfnh	1	htxauhtstsdq
23	uihgbhvxgb	OE	353111111	6	yrurzjylfel	jqjyugcfvzor	1	djxrroimfrmgnwwuy
5	dtdbwksh	OE	778511111	7	xfgiloesglakzvzzk	angxytodcqctgrxauvx	1	uywaqoxrvhz
6	yewcuprnosfju	OE	328711111	7	dfboznxecepuva	mwfrqagvxbjnhvclpthe	1	gsyrwpzcmufspkzmtf
17	ykwunuurba	OE	802211111	7	zrntyqtssrdwtu	arzrlolhfgwujwbxee	1	tnfcemdfuilhplbxufhi
23	tvngyalpce	OE	916011111	7	oreqcpiskzhqlvltb	fpigpnesdc	1	gpkreysqxnkckjmo
5	pqlxdbzsddlf	OE	295711111	8	ckkhmwkbfexscdwntu	kqhdiidbpfzsmajzmnfd	1	yjedrihzipnienblm
6	qwlpbgdwxvuxqf	OE	832311111	8	onvpgvgfvzhxbkvrwgut	uilorbayjsld	1	xwvxcdyfubiozaern
17	cxbvucbhopknu	OE	889011111	8	qblgyfmndsuszjjiap	zwsaonldpqou	1	byrfzhtitsxsdzsnfxc
23	mzievdzbu	OE	864111111	8	xbnbzrzkurg	jfkxkccgxpdqtgbqajis	1	ftcmequnrtd
5	lmixteik	OE	647911111	9	niubdiozqyuvymdztuds	fqthdfqzrbeurels	1	gggjmvgfbfxxl
6	kumbymhfhzq	OE	392411111	9	vufasknklfhvazyhgsku	zytmnapzwn	1	senngjkrlodgbdbtzst
17	wnhwjczbculc	OE	775611111	9	ulzzjtcauegh	lxnmbahkngwsevpauwm	1	ulztudmkwceycjpg
23	xkinpfcwsthyxlmk	OE	766011111	9	vkfcpwbshdtwpcfxzfp	dbcgdrenqfftltclilic	1	fsknhseqwfpdzpowjje
5	vbczpped	OE	840311111	10	jpogfojlupq	wpwsvheabs	1	cawmdxiaqmaz
6	ylcjvhjow	OE	916311111	10	ldzvonfcjsbtzja	epblsjdehhj	1	pvklrtlqsfljpbs
17	ewjnodbptocry	OE	600111111	10	ptmbcyxfikvk	rupxkauczdjnqeaxqvba	1	oyhfjfddrdguaxe
23	nuwqqnfjd	OE	476911111	10	xkvntnewqwfoptrld	tzhgmvqutzyubfnbmnzn	1	koqlcctvtdcwxhdffm
17	uvthrrxzgpav	OE	321311111	1	mxysmuenrtrtbfzzxje	hwmuazryllm	2	rymywyqbcns
23	ekwmqdojytdgub	OE	715711111	1	yzyxkrcgogtv	fljaztewznm	2	pterwmpxvmrcjxjvlb
5	sbupskznqwytdw	OE	181011111	2	pldnhuqodnkcfrd	qfxkkznnpgxq	2	fytmukfvohx
6	sojdcgoneocoq	OE	799311111	2	vosaocpvflchtpfex	ryuibcqopdhim	2	ivyjjkgzfjnrvfliaq
17	zuenpugnqhgzxhsu	OE	079511111	2	wkzvwkqhgbxujtv	dvxdismrvtphmqwuz	2	pciudepblgigotdhythh
23	amopjnqdqbdpm	OE	954211111	2	ywflodsjokjd	bquklgqcgnhykdnq	2	fxmsfvudclrmuerlxz
5	etltqcmjqxmcx	OE	586611111	3	ysdciwbzryxy	zktzyhertzv	2	pcaqhveupaupnywl
6	vvmxatghtrylmwns	OE	701111111	3	bkhwertflyqgmsoytni	knwyopvfduptvxrryb	2	ptsjrahybqe
17	foepuntgedgfi	OE	951211111	3	zimljlaejojsz	mwajkpcgrzmmjlgpanfv	2	royvfdriymcdrwo
23	ascewqizvayvje	OE	230611111	3	tjwkbctmtun	dwfzevaqnkcpcadsc	2	jveuxhgvcfvkazjrit
5	joxljixxvgjrex	OE	901011111	4	fdajcujivtbitthob	xmnmvkspopmsqws	2	fvrrvoijtacwmmtyltof
6	ytkvynjuxmfvmkfc	OE	467011111	4	ffkgzjzcqaz	xupoheoocxktmhmj	2	ekdzupoohxrfefmiem
17	olhkkdnzbkob	OE	013411111	4	uxcmwwvftymgffghsyu	abwdqfbbfnllafubzqd	2	mfpvmenritpaigyxumes
23	fupajjuktf	OE	013811111	4	mcgaidpskiio	aqrkpcpugugu	2	uctgwakgtjdsrmerne
5	biqkchnaevxu	OE	224211111	5	xssazswjgm	ofpnjsustky	2	aoyueccczupmdlqgvg
6	hsvkcjlpxwx	OE	426711111	5	qlelxhzmvvbmtco	dtqgcbapjvnqfu	2	npeqeuuhhlprtucu
17	qyzzbohcmcmebkr	OE	572611111	5	bzhdxoihbmpjcwqlntyg	rdcapsehaloujopjepxg	2	wdwyhjwpehiujscni
23	iokohrokpagnqjnd	OE	259711111	5	pjbltsfkibozplrt	kpklouleyfygbky	2	svjgrisktw
5	xsazuymukotve	OE	755711111	6	libhwkrfuw	byjzfvpqyzmceewbbtpt	2	ozvyqhfruiyjtkdfrti
6	xxsoctxegsnae	OE	308011111	6	udvxvvtjbfcop	gantibqycmvokneimaz	2	xfgiizourtvg
17	kuvmdlhimj	OE	016911111	6	ghtxbfsaiawafrplafm	hiuwndaiye	2	shpzqylcziw
23	hktwcncl	OE	215411111	6	zyvsbaizxvsxtomvun	zobuyctdzx	2	zrzvnrnxtixtbdej
5	dcgmjrcrspisivql	OE	072111111	7	bdktvbfbhokeyqadidg	qmalxcxfjprdv	2	lwpqxpososjtajungw
6	snfmusml	OE	818011111	7	rnaoqhcznipqukerr	dhsgvuxearpp	2	judiixipngtcpvmf
17	iqgbzcxnmzawdjrm	OE	247411111	7	ijpbxnrbbjfa	aermcdhcuv	2	xwqlxidcthdndzjsbplq
23	fiaiaiexoqeas	OE	162911111	7	ttomsuhdtaedhv	mlixorlrogtl	2	qlicdjlltpiqtnwvxras
5	oweyjbvgecaapd	OE	521211111	8	yflxyvysmt	sqlffbkgawnwqxtvuxmf	2	vwcdxgrnpcrmowk
6	omjkfhxal	OE	124411111	8	vrbotlhqblcxsxfusdou	rbhcqompdyum	2	vzxpyifbjg
17	egolidpegl	OE	485411111	8	xqbfhrsvclfgk	lxsmhdhfczws	2	szypggnmuij
23	dvwkgiptngfu	OE	512811111	8	curxxthlyvq	wzfcchercpzokol	2	jpxchzsqeru
5	dcvuqrqduxgywn	OE	372011111	9	wnjevczzjhyskby	mrctpfporltmmaypfgow	2	udifivfjpil
6	eximverjwja	OE	969711111	9	zilkatmuozlhganpqbxs	ecokdlggxojxj	2	xvwasilmrndnucsxg
17	xbfveglfypdrhy	OE	397311111	9	sbzadoirjntcvmyzfh	azkbvuncqjqnhjaf	2	bsaklgoflpjkdnomtu
17	ljwsrudmgas	OE	284711111	1	jaulxjzezbvlcfxo	ctqjbefcrwlrmanz	5	yyaabxdkplc
23	edpyllwlvzpagj	OE	647411111	1	ethpoehitcvozz	quzeltkdpk	5	ikhnqxriggqjxxiwmdeq
17	oienaucbasimezj	OE	006011111	2	isburzojnfnnwjmf	ahdjxvfmcivzbg	5	dneymqvfuhjj
23	eytfqnvvlkdzqssw	OE	422611111	2	kyunibugcy	itnqnxlnubsijn	5	qejsmkzqhlocnnjde
17	tawulspktrnf	OE	325111111	3	jjboholdvvtfnjyfqq	wfgujhpqxolowdk	5	dvhlrtihgeenifufgi
23	cyivcubpjcgf	OE	053111111	3	tuyfgcrnglxjs	parxwmunkyuutvpl	5	dcotfymuajcpub
17	qyaczcgvgk	OE	964211111	4	xfxlqratfnyuvczknsl	oukprnyshht	5	slywlfytrkmeqxp
23	ibrtlpjshnw	OE	813511111	4	uurztbdvqabiqyqii	foovojndzeuugvly	5	pkpdabgcsaq
17	cmnqcucphhgrmvky	OE	321011111	5	dhenyqhabctbwd	cdluchlhzqubyrthjke	5	zwxnrxhlyqx
23	jtmujjydrxqq	OE	740611111	5	zqdyhsiznfgopmaj	bniymqjvhjddieh	5	fxwcuaqexenp
17	rawhwoyhwxu	OE	581711111	6	egbytklvby	uzqwhtkglihohsirjxx	5	cvpmxcpadaja
23	cwnhsixqsdpmbyug	OE	937211111	6	bnbqarzfcquq	xeihsjviay	5	bnbjsrebvzxctmal
17	ffthfrcbnpwsx	OE	004611111	7	asesmodutb	xzomfsljydnjj	5	ctghsjlwlkzdo
23	rnlzpvgw	OE	602511111	7	kxzmeuhyaqui	mlkubhamplk	5	totimkxftespnd
17	grxrxjynyu	OE	381711111	8	ytaizcfvzvtwyghdzvik	jcgafzvgzwydbhokcz	5	awvgjiirlr
23	bghitpsqywydvwiz	OE	006211111	8	ihmokbdqbxwojtp	miyicotlhkuwi	5	qegfrbinblhelwyuyclo
17	owjksglfwgsbjou	OE	422211111	9	fpxnftuxlrmhcujw	ibfzfgosihwojlq	5	mmlycniratthtwuozo
23	ktlilecv	OE	235111111	9	vvwbktnabdinmrq	zhscudhocuclqsijyio	5	ntvwrtktlhsmd
17	rybaipnptmsfmijg	OE	500011111	10	vwmbbwpntokzrsidfmmd	uzgshxavouwmfxtos	5	ddqfnvcpwkxav
23	uupvfgal	OE	739211111	10	zcfrhaindoo	pcjhotzymurmtsprv	5	sjjrhrbfas
17	txxokyhyrksma	OE	392511111	1	rynemzpzlid	wowcqgpshuyxvtoer	6	zfziiqtwoiaorw
23	xbeoenhnjwahozqx	OE	989011111	1	ipihbdqckaotrovwxdn	cbqhmimddfpcwtcgi	6	btpersfppseavskgml
17	lygplmmnqavtpd	OE	214611111	2	atodhxuoricuch	bacniekgsovafednpnly	6	ccmawwdhqnzl
23	vjvbnjgrggcpz	OE	307011111	2	xpqcgjbixvbjoxad	vbxyozbzbvcp	6	fgomficmcwgxihhal
17	sluptwcj	OE	928211111	3	slhxokdjtpzvuw	xawyljtpryxbvzgmh	6	gilbsomgtbmzylr
23	wboslbwgeriyqr	OE	752811111	3	ciobojowfdiwe	nxyoimllfiqraou	6	jpfneiuegpvwkwda
17	njdwbyizm	OE	292811111	4	ichdsbeuxogftov	cmiojeqfyzwdijlfsdn	6	odebbakfcn
23	nlcqfocgjygahjfd	OE	820011111	4	vrvzmfdfcbwtmbo	odqlzflnxokyiudxg	6	fwupcjetnuisxyif
17	wzkcjkwiaawauc	OE	567611111	5	kzqsdwycoasu	qrgvpwknfgwathgfkih	6	tmsvnuxljxxuwkibafmi
23	jwraeiddllxzw	OE	326211111	5	ojkaoavvfq	kxqvttcldn	6	mgrplfxeurij
17	wuyqbhynv	OE	406211111	6	lcylbkdvanrax	kqlrehmdhlmssnmnec	6	dgeesduybwsrblozfdpa
23	lyoqxncqi	OE	740511111	6	nvcpraimkr	gkxoxifqdwpgqn	6	jobyihumoieziplno
17	xiwpsuvrh	OE	619411111	7	afebzuazidtgjymolvq	wfpuakfdonkkrkoqhw	6	spxpwkqslcaf
23	ekhbejbcgiqtm	OE	627111111	7	jiyrrfmzgdxjsjszh	lvpelscstcxomusgga	6	unouyijroy
17	izjmusci	OE	286511111	8	zdijnebezftxdzd	qdrwnyoxaredkqnyooh	6	gzzraodnfr
23	qiqmbpixn	OE	067611111	8	texfkzstnkau	nmnqslwdqlzlmqlwkj	6	vnmmaehcckad
17	zunrqrupgls	OE	980911111	9	cenpweharaod	wokimctptpu	6	zxxmfvoegggu
23	yuscrpibvh	OE	451411111	9	pmxkhbgcpwptchgv	pjneryrmesgqdbhykznc	6	iliopcqgtpltbczddu
17	odeylvkncnfrg	OE	578811111	10	bfdcrmlevdftvdnpbgej	grvmcerhwevphxizzyv	6	dowxsfhpahe
23	jxgwrass	OE	877511111	10	nnxgsqgziip	izjuvkemuhw	6	veakkadkokxukwbgn
17	ijxebumenxyhyl	OE	533311111	1	tqceqjmwvoqvdusszjs	ndwvbaiyciidh	3	iifcswfkcd
23	uucapkxb	OE	031811111	1	vfsfuqrexwyapuwut	lmqdybmouh	3	csuxznlflgyn
5	ydoubnhqwkpsjqdy	OE	660911111	2	iqypthxmler	buhqvjleznkkyxsib	3	keuwejmiuecocnpo
6	yahfdwkyva	OE	734011111	2	aracadgfyrhh	jcvbvwmgzkwmsjonfro	3	ctpptzzxgyjxjyazdir
17	irjhrdeplrs	OE	651911111	2	yaqzvfuzzgz	jqxiqmquajjvxiimoxi	3	xxqnxdynmxggevgxrn
23	cqiwgbvnakbvgqe	OE	063711111	2	ulyhnbksdyuhbeuxfmaw	yrpwaewjyjbayeh	3	jiffkahohbogoafs
5	crdsyvjxakm	OE	117611111	3	wmyyfiwyphi	pcfhklcyhu	3	norereceylv
6	jpqgpiha	OE	322711111	3	hxanaiiwtplgtk	ybitiddnbdxzei	3	hcokgueugnpwvh
17	obyiugdo	OE	633511111	3	dkzaezpkecusekqt	gvakoymcat	3	hyyrflatjtuycx
23	hleltarfseebhc	OE	684411111	3	oqewcqbkhbbpqsj	zzenbzybexvm	3	ddrowtzkeezzrx
5	ogepkdungq	OE	103811111	4	uffoeylruv	cuymvwdacwvpf	3	vmnyolytnacwgzm
6	fcloovzrbwbtemoq	OE	139911111	4	qqsureogjlcehc	gczxjkrfprzcprf	3	rwythscegd
17	tnpamoadbixzvmne	OE	148511111	4	esaamftrvbjygohm	xmtmnbsawphy	3	nnbhhneaukiidod
23	hrjposcriamh	OE	661011111	4	jlueuimjcirtqxjitrpc	ykujlktikum	3	cgvxdnjvpjs
5	mdnremarkyyykhd	OE	841711111	5	jntzudaekdzayfldbgb	zmfgnjkfuqgrrr	3	rxcfvosmslugxbe
6	uiriwglcmtws	OE	707011111	5	gujnvfjplib	rwawswoemdmthjwpftuv	3	ttsgggrlopng
17	kjdoxfjfkzakkzu	OE	503511111	5	mlvsaxbgcqvxokv	ctkbohusiebht	3	mokzdvsozfqnuruybbmz
23	fpjqndmwtzpst	OE	569811111	5	jgorfvpuexbypouxmox	crltgyagokxgknstul	3	fvgseqjcegsmygkjg
5	okhppziwlfawjkv	OE	533111111	6	hvwtepapfrqlxokl	uyaktzgfhbdkfwmbs	3	ukrvtgeeeya
6	wdkgyaoinc	OE	555911111	6	jlppfqhyrmntxthoxgk	dlywfhvvfcjghqpzbahj	3	izeimzeaskqitr
17	hxoltpssjon	OE	412011111	6	hpfyjxnfalivdiroftq	drvpfzhznajhaqphwy	3	bupxtkchls
23	iopjuqnuqc	OE	740711111	6	hbepvneqscwmww	jgmaogjgeataq	3	mbwonpsszzsfrmjdmvue
5	loebxwioph	OE	837911111	7	rzuaiojbtzeecbk	crenruwtmixwe	3	edamzrxvjscl
6	wywhcrhblheofv	OE	642611111	7	pszbyljqcjhe	lxdpmxbkcssawbggkdct	3	kwjicwgdqb
17	hyeqkngghsie	OE	114711111	7	nxelcntxknd	hevuujxxncpgtldp	3	fjisaeqthir
23	iospxgrfdiw	OE	466111111	7	oirfmfaoeepde	eveflphhczaus	3	tponptspkyqecmnknbjm
5	lxxqrflsevxyjda	OE	438611111	8	tvpgpoazrxgrvztwbfe	nwqcjlkjlpxptoukd	3	gglmwmfuxpqeyjzfona
6	uujjbftroskgrnm	OE	680211111	8	nzmeebtxkeotsgq	avktgjdbfr	3	pijklbrmntxxtibvlzg
17	cwnwdbwabmnnd	OE	699111111	8	hvtnkfeukcjv	awzyazihee	3	sovfdodmimvqwfa
23	iyznedkcsqz	OE	881311111	8	qwncofyazkzfborbq	oignidcekfhape	3	febtadnimvu
5	dlnjcddi	OE	732811111	9	myicebynddorbwjlp	lixghbhxmcddzgzfrm	3	btnuolljwehrzmauqxr
6	qaxpzxhikc	OE	745911111	9	wcviqtczpor	zldajmehujbpnkbqejjp	3	yrscyhsbpytjkveucy
17	dzcqhvlirc	OE	772311111	9	deebphfxrjmisgno	dfyzqtvqss	3	dpibugwttkuxri
23	ycowgcvawyl	OE	123911111	9	khwhmghzkmmfo	mbsanmcjlbolccchhy	3	uldovuizhntciyqrj
5	sziuceosz	OE	697211111	10	mivqcksixuubgcqj	lslgoiopdv	3	tquknsmydw
6	mdkknywzwe	OE	554911111	10	cerizdzevmespss	sbqjkeulhmdrzaw	3	ogkgdxdbwbavleowqgvn
17	mnbknocyersyurkb	OE	536511111	10	dzvadgahwqzsnuhlho	ccdselhewvjxqi	3	muixgwlnjawyacjyyr
23	afkaffeu	OE	666511111	10	pftnjeucuslncuij	exeqepgvfycenxgmkllz	3	ntdisqkttd
17	yshezdecylbpkqhn	OE	367611111	1	yuukpskwcxlo	cqtvawrahqznmxadmiw	4	pwycuynokxheq
23	lsaocvgdn	OE	618711111	1	jwnyvnnhordhpuz	xnorwveqaynyxkqqnsx	4	qftfjyidaqhp
5	dflltimuesrycd	OE	000811111	2	aiueqetdfqrvgtnfls	giechnwwziohi	4	akrxmcdhic
6	ljqqsuydwcgp	OE	954411111	2	vemwdgugusrvuscr	tpclvtufhgnd	4	gzhuonfioyoqkndopmlt
17	rnhikqympqkwrup	OE	603111111	2	vsihodmxbipvfc	ddzjyjuexgyawqsmao	4	anekwkcpkyfwvehrsgi
23	lpwrcyeksesfoz	OE	738811111	2	liosdyfmgzqdpba	seqsviadlrffgdqirt	4	aikdggyuyyccugew
5	hpegkoajxcygmuyy	OE	472711111	3	uqwuxlevsxstsre	lywjduucdnr	4	pfhgkiljudgw
6	isvfojopguast	OE	497311111	3	jmoaejyeseflgpqgy	cjzlnfbwpztkce	4	vxkkpfzkfwhncaprvsuf
17	gdlhjpiu	OE	636411111	3	tsdyuoubauehiug	dtspbuyfhxwzskdsgo	4	whlgokemekyide
23	aqycetjmwdracq	OE	860511111	3	sfzinqjjoezehie	fdeecfnyjcm	4	fbvtwaieapfpjcl
5	rfxugdqkauzan	OE	177411111	4	madcfdmmudx	opebjhhgrdfb	4	euigqalpmt
6	zjynnfgigyurhx	OE	771311111	4	axkwrjlnevcifbeu	wbbfsaqrqdbvstwtl	4	vslgcuhmxoehlqvrlp
17	sujkjftbkxxynk	OE	831911111	4	wuceoloasvj	mwnhythdbxtmcgmi	4	jpjpgvqenpfitpnlea
23	ulsrocmbdlfypdn	OE	367811111	4	ihwqekgfsvmkwokhjee	oxkowgxvphxs	4	omxotfqwjhbrjvykqfm
5	iyigbesfwumdefk	OE	006211111	5	zzlagrxmslktqiln	wlrpaaodxgb	4	yjczpvvjxiyzyl
6	smrfqbiarqvk	OE	586111111	5	eynbvnvtfwpccbvkltyv	qwnnfnhdjjc	4	chpltqrzourkkjobjf
17	hknjbbyiwheybfuk	OE	842611111	5	agdoqhfjudqu	jddzrryouzmkudjhed	4	wrttrgrgjxi
23	gnhpvfjuv	OE	845511111	5	mahfdwygzapavea	oqynpcuexlynwjjvvq	4	vcvehfnkczp
5	ibvwyvmasgxdurqk	OE	681711111	6	zjpirbgkkk	osojssgdhdywnodzzd	4	ogaxqefzvszzs
6	rtezgrcrfwi	OE	099111111	6	ameqfpekupksxnwi	ajbcqjgecpvhclsovdml	4	qhyikkscapujkfqqjz
17	oqizzgny	OE	547811111	6	wbokhyajlipvycbqd	wvweoipxedtdcqao	4	xpiuzqqmqbmidyszoej
23	alckedsrgawc	OE	037311111	6	mqctobsbnrulbfgokwg	movgboiinoageveony	4	gmtlqjcteguo
5	lpilgxzeshpwcb	OE	315611111	7	cbasczrqwbftjnh	hekffwnirnh	4	oddajgynhficfd
6	ypmkxzoazhkuywi	OE	542011111	7	sidrjothmruoyuzsyor	chgpbnqxnarok	4	idfzhhgwszbfeawvynjs
17	bidbbpmsn	OE	752511111	7	wkyrimupfwpbfrfbauqc	mzutlcggqlqtzl	4	qgoetyqkpp
23	wxytvwyqeaxn	OE	024811111	7	ckibjnuqdgtsfkx	espdkjvhtnqqq	4	cnpkuxhumlepewmeoqux
5	fhcugwjkiw	OE	060711111	8	xzgadkticssxjlqyppp	ljotdcibudowtpuoi	4	gxgbsxeteidzfhrl
6	wjgdzsrmbjgjkmf	OE	637611111	8	oqvpzjvkzphibnh	zgabvsmtszxkepvez	4	uzgngokkwl
17	ojankabkrdzd	OE	521511111	8	cglppdyfjoepbfxtlgx	kfsixmbsgpqjlp	4	wrhtqqrzbkhtji
23	rwrofscn	OE	777111111	8	aeljsxezwfrbtvo	ralriugmvsu	4	phbvoumvwypjpqqsho
5	banbcyguu	OE	251411111	9	wkyvjeszmkxpktzup	tbxiikbknrftuzmjuq	4	wrvrepgozx
6	xbuaykqigpqtpu	OE	684511111	9	bgfibamnewkmbhtrvxa	objhbobzrmqm	4	hniafdpfbzj
17	fzqrnqxwzinz	OE	793211111	9	cnpbxmpnnf	iqrquxycurowkf	4	xebxgqzmnftdbelfy
23	ccsfhpfffkai	OE	743211111	9	vrymqfbtjeepfoa	pvvhawchgxswuwwcobd	2	uqmywtyramlgbnq
5	zftbjfcdpdiqns	OE	813311111	10	eoohtdvzfjpxit	wbfqzvekwsqdossmfqye	2	cizzobtowedjc
6	lrksuqbdmjoquc	OE	499911111	10	biicnedpmrcvk	orsnsxkmyppngx	2	vzgytqignkvcybmntkkp
17	rssdzhyimm	OE	364711111	10	plgluqvfjhffiasxgo	wubjgtprakvfx	2	ukvfodsmvrkofur
23	rmgiqfnis	OE	669111111	10	jmbdaqxjdhzfvdykxqt	lpmqmmawmz	2	budzwguiuqagkujfiqii
17	yxltevwfuubncrb	OE	003811111	1	rpwmxmkgelgzg	bkauxwmfdto	9	tounpuunacwulv
23	weboeduikzdbfaj	OE	618311111	1	jwvfhielwv	yewywvpitfhts	9	yhcvablmhishynatbs
17	bzthiseovqfge	OE	429811111	2	dtrurhmmqujhrzwq	ftoroyugmgtkurnypqod	9	bqoyltysaspufjanqxt
23	ypgxzdmvhno	OE	482811111	2	juogutkcwdltkbai	rgraqtrcxfulgdrlzlk	9	ndkgcynoiaqoidfkgq
17	bbejelupgrorsb	OE	506611111	3	xxoyyorlrpa	udkcibgrfgdhzukeslw	9	rtafobeokaeoljlbwcb
23	skxqdzqzxsj	OE	908211111	3	tgqtnwiguapsd	vtoppqiyqtbigkuofcw	9	ttqeckxizohgfzjapzc
17	tukkwtjnn	OE	354111111	4	opnkxzutruxwrxshk	jpdelkycyhmml	9	eoxdqtmuggqmbjiy
23	auzbnmhjeexeoi	OE	174711111	4	gcsregedugnwva	crdrqsecinxjvwec	9	opaqmxteknbylejr
17	nvkujmgbmunxdf	OE	961411111	5	sujcbkapfvalfagii	ckzfrnmzqoh	9	uwnvygwbooh
23	tshkwnidna	OE	050711111	5	warmppgacje	htgjevxwkhixpt	9	ckfoswilrbtqalvm
17	jtqnsaupbee	OE	810111111	6	iledrrflwryoptb	gzammljzyuzclrghtub	9	iebtucmadujakojx
23	zfulwvgozfqrhcc	OE	030311111	6	jnijekxqbonr	uatmqnxkbhweisrz	9	yanhgtfeaxf
17	xmwymynn	OE	456911111	7	kfkyxawnyxgrovc	ttvrzskbmcbdkxwtofp	9	anrxbknnowgxkasd
23	soavteqxikgenqx	OE	465511111	7	akapdgbwznffqcneoo	abrvggiezd	9	pggqvrzqpaiucxmha
17	rnzwjdnfywszpv	OE	945011111	8	fqgxdmqepovfjscqzjk	dfyuugjjbhuhzg	9	zsysfejnxd
23	ctqeurlmxbo	OE	206111111	8	mwhvndcrlrgowvdzfi	fhamlefztgjvzwkciu	9	isudpzalcqhamoopax
17	dvuaxtfgluivnisv	OE	714111111	9	ohsythsrddhyliu	hhbpkfcsrfuam	9	jejyftrxsneyv
23	hijtqsgiz	OE	572511111	9	eyvnccbrqa	bsfbxkhfls	9	edgqltaniautcgq
17	mtcnvsyti	OE	436311111	10	vvpabpegadevgqoinezp	vrkqwtqpbeesmijyio	9	peiemvjdppq
23	zkvkdfea	OE	753111111	10	uuelwqiigl	bnzxbiinsgwvyttdhga	9	fbwgtplukorhoh
23	agpynbsoryrj	OE	835311111	9	pfssidqbfxuyxbckl	bwrihfkcirjduwejl	4	ekkgmdubtgkrnnqrhvl
5	taodkprdznacaamk	OE	066911111	10	hefjoeglegpujaivrew	ieuoampfjtmzocz	4	kfparsigvmwrm
6	cbehcglc	OE	225211111	10	mvbwjfarxbyel	jyhgnzhultrxjiwv	4	umugltoxqvr
17	fopylrowekkc	OE	756211111	10	bvwyggbezsmoeaaomhkn	xmhciziwzpgtn	4	hvauyblvigu
23	ukdkfuwrlakwe	OE	582011111	10	zzzsaazade	bezffpbnnqso	4	ourrlsyzechzehozg
17	dtksrlpx	OE	176911111	1	txsarglykhtf	wfetjxovqcjfeukdhf	10	kcdmfgjxfeouz
23	pnieeutkhn	OE	890611111	1	zryfxdkicjtaibbhhrak	bkabjubbxnn	10	gyqribkdsnpvb
17	ipwdddeqcnsftc	OE	562911111	2	evtagjnqytvxlb	hjiyrxcgldnofwsishm	10	rcgljlwxcbd
23	inwqunsecadxi	OE	666311111	2	wqnybnxpxn	pdssabcyypkzb	10	kbnzllwjfqrnmojbsbcn
17	oiczbchtatbdkfnm	OE	274111111	3	optepinkdumhdvtzgal	qdlrgcwiqdappzda	10	engmldommdewzhnuxi
23	bxcbwymgdzdwp	OE	602611111	3	zfxrxhqrjxpywrgx	btsamzyriwk	10	opclrkrluzugjmnwyoz
17	ipujwbdaxea	OE	624511111	4	fvphhsfesf	sabkxqdccmlfesglyzz	10	rpnxcvvvbz
23	sfaxzzsl	OE	264311111	4	yynodcbgqc	jydwyqcgxrjtyq	10	chktttnocnjvc
17	cuqumzrdj	OE	640311111	5	lsveupghizirt	xdxxdhqkqsf	10	hckqvguuhf
23	cageghje	OE	009711111	5	mfxcwkyajaulgzpa	ykanhrcqxq	10	qlepzovfhapybzrasc
17	xxwnidrz	OE	673611111	6	jxpdteardqcpvhmyrw	sfgvffevscdhkbs	10	rohedmqiuwbh
23	cvqdgdldtalma	OE	456311111	6	ubdlbuzyejgbi	xqglxcgfhbpvukproo	10	hgjvmoescaayvvaohp
17	ntabtzfjcjub	OE	516811111	7	sitwdxahqjjkv	hevbdlsvqlcokkuvrv	10	qpqkijvdxxkz
23	kpyyqcildsdyxhix	OE	995311111	7	nzlioytivfiefznhzo	zamoxcbvghrkor	10	matnypewypbdu
17	vhldlbqgjdoxp	OE	013111111	8	efisffhfayihgcfxo	nsitafgcwgmmfad	10	emwoffcoaffwzzshtt
23	vwnukhwuigwwe	OE	544611111	8	ajqgllpfqretrshabah	qmtugurdpizlf	10	rqnvxmzacny
17	xtavuicy	OE	689711111	9	ddnvtbhiyxp	nqhfnklhwsaymwfigp	10	qohmbcuiavjrvozayle
23	fiexmpsirdgu	OE	634811111	9	kvlksldffkuh	iwgnykmovdybirfqu	10	uphhiiuxsxojcsppwlno
17	abpfoqmbdeyhq	OE	503011111	10	azxocfogrutstepjr	mzqsjbbrtotvzew	10	oesqrwnckob
23	lmfmsrwnket	OE	906711111	10	gjszlemqdipgbjfup	bujhwmqpytpuduzs	10	euqoxglcznqqwtkavh
10	wjmappgseinpntz	OE	618211111	2	slxirpjcep	gazjfbalptyhlsn	1	srmplrzfqfkleaqppme
10	nbhlvpfjuifvr	OE	651111111	3	edwtijnltol	xcmuzwxhlhdcywlcgy	1	lhzbbszxylkqk
10	fleeoaxbbmfn	OE	396711111	4	sxbbmalllrpt	uvpdermsbhkewiu	1	bqtcapfstyhmxiryxgr
10	oxqmorngmhedy	OE	017411111	5	abuunpmsxdmicdotw	geugftwbnkukruisjzk	1	jslhdxkkqxziviafcbh
10	yrlcwuixqyalfnz	OE	841811111	6	hwlnlefyyyxkialpuv	qiwklbhongalm	1	ywurmmohxmsuinbmd
10	taqksyfzmm	OE	480911111	7	fyrfxgybukotwpsz	vdnbvhvewyucbybfh	1	inzglechcmjnjkqqetqi
10	cgcmpptmjbtghlm	OE	958811111	8	vxbhtadtpvrhesdoe	qjognydqxgjeqsxzbteq	1	umgrjeqqvno
10	wapnqipu	OE	491611111	9	blmjfpelve	jmmbcgyhorjpwcrkmxx	1	csrsrwmdgvzshddpoeps
10	opklnybaobnma	OE	717111111	10	cabzlpwlsjq	tdwzvpooqz	1	wgmxjvbtvciif
10	wvvwvlavsgn	OE	653211111	2	yqwwqinnarnymoqimdu	mwziroupdrny	2	zwmxeekeihmsunbw
10	dqdtmvuidppki	OE	488711111	3	duppmxqykmbsojpvjpnu	ajilpynwnvoxsgguav	2	bcvgaxsgdqtuvlr
10	hvrvsawl	OE	394511111	4	tijfmwtnvkgjoobiqrim	upamuywjkmwrwihmzkd	2	mkfaoyqliyygwjsli
10	okewmxfcqyxn	OE	106911111	5	fjdtosjinrxwlnerdxbn	ozdjdrqurstfj	2	dtygnjounkm
10	usqvuejx	OE	640711111	6	xqvmhiumzx	fiqbrllkwjienxpfoud	2	knubwwysgswoxh
10	rdkrwzvtqpfgle	OE	255811111	7	wwztvbeootqafstwe	vfoovkrzrccbe	2	mqsdbwrutyect
10	tkougvkwvmnkt	OE	871011111	8	lbwdhcmotivmikxw	tmtmvveugroauxknmfah	2	bbnmtmpvvkuesxo
10	gbuddxjqjtjttvu	OE	617811111	9	lzinrofjuefecvpfzigr	jwsxrwsggjmuxm	2	dupqohyooejbfobfr
10	boqksehgfklfwgac	OE	059611111	10	yvknarumarvrc	tzqxjlqnxjjcjgf	2	rntoeecjepgr
10	pkumzlgvrhmgz	OE	172711111	2	fyhsjrpbziujze	hpsxmnhdfokf	3	jmaiamejumtky
10	jtzanbzyej	OE	015711111	3	vsgbcuovkfjku	wtkdnduyebxuakzxrm	3	kjawiwdnfqffsfnrfhkd
10	njyjbndtbywxvxl	OE	096511111	4	gkarpiwmkdinzmz	qccttaiylfzvani	3	cglmckbxdazm
10	qpctwayrgq	OE	027211111	5	oefiwtygpjfp	elquzixpeyfn	3	jwvlqzlvnvmw
10	ceoljraevig	OE	907811111	6	ugoqfayghjgxanmwwz	pslpqeqgwqh	3	dlcnhchapqw
10	ypslfuhkrgjshaq	OE	935711111	7	leydvibswloskaf	gepreyqxfwzgvntxms	3	nmhmlepcuzomq
10	xwvplpzxqdnvy	OE	976311111	8	rgbzkqnqmxdzjiibw	cizviwosjgqlga	3	sdqgzeoounljshbhy
10	yjqgdezzfkwj	OE	246311111	9	ensfvbsrtikalntwrnph	zqjfovoqcvgogtm	3	ysupdqxmuxmljmvl
10	hrhsrruwuwqwph	OE	067511111	10	vvckdwvefvnsmirargp	qlfemalkckjjgeb	3	souhreopdkvzfk
10	rnmxeghey	OE	851911111	2	myvouuetnqiznmgxtj	wosgmzpuxuvsjfulfo	4	xnnbkoypzgbykscgqlaq
10	fgnoagtlf	OE	331511111	3	slejauolae	tnrgkffcvy	4	pbisbpawdw
10	eorzltubqeaurgi	OE	865311111	4	ewrgqakatogcs	gydzjbwqprj	4	mscclqzliavqqmbyb
10	rvfplyirq	OE	490211111	5	zkzgzynlxponzptkrper	jcvfrnqoozytacwdhz	4	ladmhhrpiwvujo
10	juekcbcaedhfdyuz	OE	533411111	6	rircintobipxbijsiya	uxwklkiupwbecsbsr	4	vlgyufrnghzaf
10	hnzqfvbuuxvhnhsm	OE	681011111	7	phbscfizydjzu	mailljhtxxkrh	4	kwsjabbjlpcbjp
10	ycpuwctmeidt	OE	485711111	8	jauqvxueunivqypxtvvr	fnfktwlqhvuhkr	4	lwqifnssruffflreod
10	ogksxiilmms	OE	820311111	9	tzixiolqcjpbpi	upyppuutbwnemwvs	4	lpqxlofqvotjnccjrw
10	ghzyqtfoqbho	OE	644211111	10	vzkmveoxuiwgwnohwtvf	pbjcvlvxycyrbo	4	ywdvyfjkrxfcuy
15	xvznopemsbjyddvn	OE	999711111	2	pkjvxpdwssvukwmfxnz	nuxcubegjfmtzysb	1	rbowtqtnyosjbvda
15	okxecpxzdgayhyc	OE	369211111	3	mhfgpyfunobosxly	jdekhinvuw	1	wrpydlknibwe
15	jpwpeoaseerwekch	OE	222411111	4	izcrfwcsasuce	vtpravybbtymqwp	1	pivsejunvtcpvdhlxth
15	lehyeueruln	OE	008411111	5	mbqnhhqjsjmzxwbfmyp	hoccpixzslkyqz	1	wkgtvsycyvrbyxx
15	sdivdwnzjdemikpu	OE	663311111	6	tfemablisxirrdyu	sbpobuieyfs	1	gunrrjnuxfvguo
15	iwwktsxqimuxwukf	OE	441711111	7	arvtpyexllnpemdyqi	suzfxkmuarmgzbpk	1	gmwneseqvdrxoxi
15	dyohccpufyuaxb	OE	980011111	8	kwnbggkxdnaxdzuzb	iqjqotfahmsxn	1	yyphlfnmqxuhh
15	mcernkhym	OE	139411111	9	usqmkbijjo	rvrckhaobgkcm	1	luisuifumgojeyprzop
15	kuxkkblvzqabfu	OE	346211111	10	nteqfukwzuphuejqtmg	rzaihvjopfn	1	lfcgczkvgvuo
15	gezqaiqg	OE	043911111	2	vlhdshbqryecthqsw	rijhaatwxelibi	2	cvpkfmadslqjgi
15	dlpqdnhnzptz	OE	207511111	3	znwmflsdnvihvrzv	atyesepwiaugzxmzy	2	domkkwrqzmvcuxlyi
15	bffjkaofikv	OE	691611111	4	fvlmamaqnbe	vztialxhlovtluawdu	2	hqspwieginaatzla
15	pyzqcssxdwmumko	OE	324611111	5	jgoalbxqanvwxclo	zjwryuavgjwlf	2	vbmgstjvawdttrdncyfx
15	zrofosfprjlu	OE	208711111	6	ycuqilivqcgj	beubdkjjbxfysx	2	rlsiovsnyiyz
15	zeixgvdnjhtrar	OE	314211111	7	srdcwjasatrsp	jugzhctzwssrrsotryh	2	wrhtysszth
15	llblgwgpkxjyj	OE	918611111	8	ciaanwskknlkjd	sogkipnzdvknjutz	2	zxexqbxtsqdbugok
15	rijnbsxmhe	OE	431411111	9	sjwbvlabkxojiavwfhs	sitxbopfrrxcfzlrk	2	pphldefdqiswmewytxd
15	fidbbdrnmejwsj	OE	468811111	10	hnksfxdgkpyfbnbfu	zkefqvzhxtwisdyqmp	2	lkzszefnmx
15	cibgzzkefhapz	OE	766111111	2	kdqjucvkar	fpqxukbauomqmu	3	kekqpbnmujyc
15	vdregobbbwyiw	OE	881711111	3	ybfcwfnrrmghr	hfimqxqdnnwcnsgopkp	3	ubxnqvwxzsvrsxzwows
15	fwdemcudmd	OE	969711111	4	mafcmmmsgxvsx	dnwxqrzevujbrutcfb	3	mapsxqtuisg
15	evnzullmblrwj	OE	034311111	5	logcmiajxyadyxthbuux	godlifqwbea	3	klvinrklgcy
15	dkgcumhqlcaitnv	OE	947211111	6	ukqasfbvmd	ulchkhimeypz	3	vddujbwcjgdaemxi
15	szbtglehn	OE	182011111	7	sbuvzxuwzccn	kbggitqqszxspaw	3	sldimkufuqyvtlpnbkvd
15	cyuuuuculpzqwmfd	OE	694911111	8	qjpbjtsbhvhaibernytw	dzfssugeaebtaezjph	3	rigmrdvfbfovmk
15	uwgnstifuois	OE	070211111	9	mlrkfgntokvjdsyofc	lyjqrqbpxftnoindyid	3	uskrsanyxyoemlpwrhm
15	wilnbhwy	OE	908511111	10	ddsjumjfeq	pqmkdqtwemmtuflw	3	mieiifrxgazehtt
15	ofczqfnbdocoo	OE	415111111	2	ntvzsegxqhjdeto	pdbdzwukoy	4	tkhzgbedsnvzzppegd
15	jxkvxucyxknwso	OE	418911111	3	taxfwlbbreqdxcvdq	hyqkqaipxyk	4	ejkenwsjkjcrlk
15	ovofscgg	OE	952711111	4	gdaaqkvgrs	rjmrbuxdwj	4	grleeqkjnhwwopgedpao
15	wpnwygpabdmv	OE	174611111	5	lvxaoqdmtewmjrtr	sdnymhkknmigrzjw	4	xozpffsqpp
15	bjeuwobacstaoa	OE	388811111	6	tqllpiijamq	uhvtmakgytyk	4	jsslheiuegfvti
15	ziqnkfekxs	OE	100311111	7	uwkzqfukvxev	jjippzpvvolrgcoamlcb	4	ctlnxcdvavkjdvta
15	snpekcbrksh	OE	827211111	8	opysbijippssttf	qelxjllqsy	4	pvfjeeacdhho
15	ewfxfhksrbtvbn	OE	297411111	9	vbuqxewydbgoubiphcsx	zbowdqjkkmazoxdutiey	4	yidozvsqtoommh
15	qzonmygvdgshraec	OE	509611111	10	luxyizgbhad	jxopwwhgntqfll	4	vcfbtglqojbnb
14	nkpiwtqswsung	OE	495411111	2	coqddtowxsxkk	dbqfvftxbsepdguhb	1	tmzvoaiztbkju
14	bupalupwkuxfkhd	OE	514911111	3	xbgnpdypoowgucl	pbhdgjnosbzse	1	cufqpbxlnvbp
14	kiytgwhwqkc	OE	743511111	4	vjwfolqgdhulnlbp	qmlehelnwskwdjik	1	liaqnpozzoengawh
14	zghrgcgvimoqehp	OE	336411111	5	fcghlpjejh	pmfidiizsfuufy	1	hehnidrvmcog
14	kjdjslxcnunss	OE	009711111	6	tcfrcrgszysdfv	dttqswrdrepc	1	tdpwbseqmu
14	lfbthqbzl	OE	000711111	7	afwweidiai	cceyoxmriqjnqelcupig	1	ilmicemxnagnm
14	kcphpsbczbdhbu	OE	694311111	8	nyxfxaxhzvlgw	tnqnxhynox	1	oxptvtyahbrjouj
14	iegkkgbkbmyvpmo	OE	299111111	9	vfpvkozzazucmautolzx	ibtzdzammckqsfhvny	1	zrvoktponhujyhn
14	cmirarhwlrllh	OE	169411111	10	yrvhbzjixpkpnjaw	pmnqjmmuhtiu	1	robdevtcgsabdqilq
14	hrmpwjbdxzol	OE	372111111	2	iyfcnyxkbbzdil	irrxusqlgxbxzfkyft	2	qtxegdadqlyc
14	avjhwivjyox	OE	332411111	3	rzulomwciyi	dcizveiewf	2	bikrhaiigoekgmxg
14	galwsqwouvlbra	OE	180711111	4	prdjjafsaehdt	jkqfiwqcrccrx	2	gawbcnesjujfkmos
14	owwslzfrs	OE	875911111	5	kzxglwovllxlsqrkio	nilrbewqvbtce	2	hlagauvzoyemm
14	vzvtttthejljionk	OE	113311111	6	yiqkioamozf	hwxpgfdosd	2	tqmicmukwmkbupreif
14	qireeveaxdnzannm	OE	480511111	7	rvrbyizorwkleerezf	kpqxqodtkhcoy	2	cgirmgefhecbh
14	stqmocwwygnoyqg	OE	648511111	8	kgkvlizittwvh	iookwmebijibqjwkirq	2	ymzoyvjrhm
14	tcvhmmhwhefzyjgj	OE	861411111	9	gbssvqadeeyverceehm	mjqtuxotewjh	2	moinztsdqwbpk
14	nykzpfvnkeuixugc	OE	028411111	10	klhyrgdcnnlgzdyd	ddirovkkkbzfczu	2	akciztekzrewbdtlwcnq
14	qrccwhdstovg	OE	439811111	2	airgxnewateptfs	mpxwvajabkc	3	adoqpcztldeyzsixjtyb
14	wfalncdocb	OE	796911111	3	nohhzpvjfzgnmstoq	afhvxbmkkvpwqv	3	jppmxdgnhxhmhgsyyw
14	yseyfxgexjc	OE	380111111	4	zvsbhhtzargv	odnqodykjlpc	3	gayaynjcvcccgztuxs
14	idqsqptszkojsvml	OE	865611111	5	argtolufzdssao	unmlxkiwrjlaanfd	3	thfeksizicn
14	tjomskexkyjlrfn	OE	834611111	6	ynartwtgenw	zebafopohpdvfs	3	gclaswxcmhbjoduudy
14	kzfhyiwi	OE	580511111	7	pvoztwqmdmu	lvvcuygmkxqrnbah	3	muohhcpdocrfgrkk
14	yawlwkpgxupne	OE	821611111	8	lsqlzcfgylcomi	ghgctpqcambux	3	czauoyfrgpupfhkvalz
14	wmuaxxhqyibsa	OE	459511111	9	mguwafadkdy	whezsrutgc	3	xmobnnzoor
14	evbgmyyxi	OE	812611111	10	ebwgsdzgjmfethxtk	jcypljzcekmphbqzly	3	iaofsayysfrpk
14	szzijdqsspymbghn	OE	185511111	2	iurslralzmishogeg	cvbfrgkyxogaasridwr	4	wauquzmsdkiyly
14	oiqgithzkzrgcm	OE	448911111	3	arglpiuevvodn	jlrtctmwykvwnz	4	fvidbloulyu
14	kfcltwrhd	OE	777311111	4	riechjibckkmnapmolu	unawnufswnhhlvgo	4	hauyflcdcvocatyognvs
14	vasjuohitsotyni	OE	946011111	5	ccfazvwizyb	gmejockuhbbzpftw	4	qppokxczsdcn
14	ddojreybhzoxjr	OE	315911111	6	btgpelumciycpvufcuq	dfkneojpgnvtb	4	qlzyszefgyenclgevaf
14	giuilessisqawiw	OE	232011111	7	xjepakbhmsv	irxzrnhuqnlpjdrqqifl	4	mhkbvpivhtnvnkmopxk
14	bhtlotsquohzgoa	OE	241111111	8	myfmpthbpg	tfiamkibauowtrwlc	4	igrzvanlfaaolj
14	cobirurspn	OE	804211111	9	tnedbcvjypdcqjf	twcxxutqmsk	4	bkkobdtvqovk
14	ldiytbcdolxlz	OE	842011111	10	tsiqnuthyf	mbkavwjkmcvcge	4	fctbkawtpxltuvrjkx
18	\N	\N	guang    	1893	tangyan	\N	6	gaoxin
\.
;

\parallel on
analyze fvt_distribute_query_tables_01;
analyze fvt_distribute_query_tables_02;
analyze fvt_distribute_query_tables_03;
analyze fvt_distribute_query_tables_04;
analyze fvt_distribute_query_tables_05;
analyze fvt_distribute_query_tables_06;
analyze fvt_distribute_query_tables_07;
\parallel off

set enable_seqscan =  off   ;
set enable_hashjoin =  off  ;
set enable_nestloop =  off  ;
set work_mem =  64        ;

select all max(table_01.w_id),
           min(table_02.c_d_id),
           sum(table_03.d_w_id),
           count(table_04.w_id),
           min(table_05.c_id),
           max(table_06.d_id),
           table_07.c_d_id
  from vector_material_engine_002.fvt_distribute_query_tables_01 as table_01
  left outer join vector_material_engine_002.fvt_distribute_query_tables_02 as table_02 on coalesce(table_01.w_id,
                                                                                          1) =
                                                                                 table_02.c_w_id
 right outer join vector_material_engine_002.fvt_distribute_query_tables_03 as table_03 on table_02.c_w_id =
                                                                                 table_03.d_w_id
                                                                             and table_02.c_d_id =
                                                                                 table_03.d_id
 right outer join vector_material_engine_002.fvt_distribute_query_tables_04 as table_04 on table_04.w_id =
                                                                                 table_03.d_w_id
                                                                             and table_04.w_id <
                                                                                 table_02.c_id
 right outer join vector_material_engine_002.fvt_distribute_query_tables_05 as table_05 on table_05.c_w_id =
                                                                                 table_04.w_id
  left outer join vector_material_engine_002.fvt_distribute_query_tables_06 as table_06 on table_06.d_id =
                                                                                 table_05.c_d_id
                                                                             and table_06.d_w_id =
                                                                                 table_05.c_w_id
                                                                             and table_05.c_id < 200
  left outer join vector_material_engine_002.fvt_distribute_query_tables_07 as table_07 on table_07.c_w_id =
                                                                                 table_04.w_id
                                                                             and table_07.c_id < 20
 group by table_07.c_d_id
having table_07.c_d_id < 10 or table_07.c_d_id is null
 order by max(table_01.w_id),
          min(table_02.c_d_id),
          sum(table_03.d_w_id),
          count(table_04.w_id),
          min(table_05.c_id),
          max(table_06.d_id),
          table_07.c_d_id NULLS FIRST;

----
--- Clean Table and Resource 
----
RESET log_min_messages;
drop table fvt_distribute_query_tables_01;
drop table fvt_distribute_query_tables_02;
drop table fvt_distribute_query_tables_03;
drop table fvt_distribute_query_tables_04;
drop table fvt_distribute_query_tables_05;
drop table fvt_distribute_query_tables_06;
drop table fvt_distribute_query_tables_07;
drop schema vector_material_engine_002 cascade;
