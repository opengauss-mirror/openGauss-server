/* -----------------------------------------------------------------------
 * formatting.c
 *
 * src/backend/utils/adt/formatting.c
 *
 *
 *	 Portions Copyright (c) 1999-2012, PostgreSQL Global Development Group
 *
 *
 *	 TO_CHAR(); TO_TIMESTAMP(); TO_DATE(); TO_NUMBER();
 *
 *	 The openGauss routines for a timestamp/int/float/numeric formatting,
 *	 inspired by the A db TO_CHAR() / TO_DATE() / TO_NUMBER() routines.
 *
 *
 *	 Cache & Memory:
 *	Routines use (itself) internal cache for format pictures.
 *
 *	The cache uses a static buffer and is persistent across transactions.  If
 *	the format-picture is bigger than the cache buffer, the parser is called
 *	always.
 *
 *	 NOTE for Number version:
 *	All in this version is implemented as keywords ( => not used
 *	suffixes), because a format picture is for *one* item (number)
 *	only. It not is as a timestamp version, where each keyword (can)
 *	has suffix.
 *
 *	 NOTE for Timestamp routines:
 *	In this module the POSIX 'struct tm' type is *not* used, but rather
 *	PgSQL type, which has tm_mon based on one (*non* zero) and
 *	year *not* based on 1900, but is used full year number.
 *	Module supports AD / BC / AM / PM.
 *
 *	Supported types for to_char():
 *
 *		Timestamp, Numeric, int4, int8, float4, float8
 *
 *	Supported types for reverse conversion:
 *
 *		Timestamp	- to_timestamp()
 *		Date		- to_date()
 *		Numeric		- to_number()
 *
 *
 *	Karel Zak
 *
 *	- better number building (formatting) / parsing, now it isn't
 *		  ideal code
 *	- use Assert()
 *	- add support for abstime
 *	- add support for roman number to standard number conversion
 *	- add support for number spelling
 *	- add support for string to string formatting (we must be better
 *	  than A db :-),
 *		to_char('Hello', 'X X X X X') -> 'H e l l o'
 *
 * -----------------------------------------------------------------------
 */

#ifdef DEBUG_TO_FROM_CHAR
#define DEBUG_elog_output DEBUG3
#endif

#include "postgres.h"
#include "knl/knl_variable.h"

#include <math.h>
#include <float.h>
#include <limits.h>

/*
 * towlower() and friends should be in <wctype.h>, but some pre-C99 systems
 * declare them in <wchar.h>.
 */
#ifdef HAVE_WCHAR_H
#endif
#ifdef HAVE_WCTYPE_H
#include <wctype.h>
#endif

#include "catalog/pg_collation.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/formatting.h"
#include "utils/int8.h"
#include "utils/numeric.h"
#include "utils/numeric_gs.h"
#include "utils/pg_locale.h"
#include "miscadmin.h"

/* just keep compiler silent */
#define UNUSED_ARG(_arg_) ((void)(_arg_))

// support 16-byte hexadecimal to decimal conversion
#define MAX_HEX_BYTES 16
#define MAX_POWER (2 * MAX_HEX_BYTES - 1)
#define MAX_CHAR_FOR_HEX 16
#define MAX_HEX_LEN_FOR_INT32 8
#ifdef HAVE_LONG_INT_64
#ifndef HAVE_INT64
#define INT64_FORMAT_x "%lx"
#define INT64_FORMAT_X "%lX"
#endif
#elif defined(HAVE_LONG_LONG_INT_64)
#ifndef HAVE_INT64
#define INT64_FORMAT_x "%llx"
#define INT64_FORMAT_X "%llX"
#endif
#else
/* neither HAVE_LONG_INT_64 nor HAVE_LONG_LONG_INT_64 */
#error must have a working 64-bit integer datatype
#endif

#define PRINT_LOG_OVERFLOW                                                                                        \
    if ((MAX_HEX_LEN_FOR_INT32 == plen) && (MAX_HEX_LEN_FOR_INT32 - 1 == power)) {                                \
        if (*(Np->inout_p) == '8' || *(Np->inout_p) == '9' || (*(Np->inout_p) >= 'A' && *(Np->inout_p) <= 'F') || \
            (*(Np->inout_p) >= 'a' && *(Np->inout_p) <= 'f')) {                                                   \
            elog(LOG, " to_number: 2^31 < number < 2^32, convert to negative number.");                           \
        }                                                                                                         \
    }

/* the max length of decimal converted from 16-byte hexadecimal is 39, so MAX_DECIMAL_LEN is defined 50 */
/*to_number('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF') = 340282366920938463463374607431768211455*/
#define MAX_DECIMAL_LEN 50

/* to improve processing speed, (16 ^ m) * n matrix (0<= m <=31, 0<= n <= 15) is defined. */
/*static variable 'g_HexToDecMatrix' has size '2048'? */
const char* g_HexToDecMatrix[MAX_POWER + 1][MAX_CHAR_FOR_HEX] = {
    /*16^31*/
    {/*0*/ "0",
        /*1*/ "21267647932558653966460912964485513216",
        /*2*/ "42535295865117307932921825928971026432",
        /*3*/ "63802943797675961899382738893456539648",
        /*4*/ "85070591730234615865843651857942052864",
        /*5*/ "106338239662793269832304564822427566080",
        /*6*/ "127605887595351923798765477786913079296",
        /*7*/ "148873535527910577765226390751398592512",
        /*8*/ "170141183460469231731687303715884105728",
        /*9*/ "191408831393027885698148216680369618944",
        /*A*/ "212676479325586539664609129644855132160",
        /*B*/ "233944127258145193631070042609340645376",
        /*C*/ "255211775190703847597530955573826158592",
        /*D*/ "276479423123262501563991868538311671808",
        /*E*/ "297747071055821155530452781502797185024",
        /*F*/ "319014718988379809496913694467282698240"},
    /*16^30*/
    {/*0*/ "0",
        /*1*/ "1329227995784915872903807060280344576",
        /*2*/ "2658455991569831745807614120560689152",
        /*3*/ "3987683987354747618711421180841033728",
        /*4*/ "5316911983139663491615228241121378304",
        /*5*/ "6646139978924579364519035301401722880",
        /*6*/ "7975367974709495237422842361682067456",
        /*7*/ "9304595970494411110326649421962412032",
        /*8*/ "10633823966279326983230456482242756608",
        /*9*/ "11963051962064242856134263542523101184",
        /*A*/ "13292279957849158729038070602803445760",
        /*B*/ "14621507953634074601941877663083790336",
        /*C*/ "15950735949418990474845684723364134912",
        /*D*/ "17279963945203906347749491783644479488",
        /*E*/ "18609191940988822220653298843924824064",
        /*F*/ "19938419936773738093557105904205168640"},
    /*16^29*/
    {/*0*/ "0",
        /*1*/ "83076749736557242056487941267521536",
        /*2*/ "166153499473114484112975882535043072",
        /*3*/ "249230249209671726169463823802564608",
        /*4*/ "332306998946228968225951765070086144",
        /*5*/ "415383748682786210282439706337607680",
        /*6*/ "498460498419343452338927647605129216",
        /*7*/ "581537248155900694395415588872650752",
        /*8*/ "664613997892457936451903530140172288",
        /*9*/ "747690747629015178508391471407693824",
        /*A*/ "830767497365572420564879412675215360",
        /*B*/ "913844247102129662621367353942736896",
        /*C*/ "996920996838686904677855295210258432",
        /*D*/ "1079997746575244146734343236477779968",
        /*E*/ "1163074496311801388790831177745301504",
        /*F*/ "1246151246048358630847319119012823040"},
    /*16^28*/
    {/*0*/ "0",
        /*1*/ "5192296858534827628530496329220096",
        /*2*/ "10384593717069655257060992658440192",
        /*3*/ "15576890575604482885591488987660288",
        /*4*/ "20769187434139310514121985316880384",
        /*5*/ "25961484292674138142652481646100480",
        /*6*/ "31153781151208965771182977975320576",
        /*7*/ "36346078009743793399713474304540672",
        /*8*/ "41538374868278621028243970633760768",
        /*9*/ "46730671726813448656774466962980864",
        /*A*/ "51922968585348276285304963292200960",
        /*B*/ "57115265443883103913835459621421056",
        /*C*/ "62307562302417931542365955950641152",
        /*D*/ "67499859160952759170896452279861248",
        /*E*/ "72692156019487586799426948609081344",
        /*F*/ "77884452878022414427957444938301440"},
    /*16^27*/
    {/*0*/ "0",
        /*1*/ "324518553658426726783156020576256",
        /*2*/ "649037107316853453566312041152512",
        /*3*/ "973555660975280180349468061728768",
        /*4*/ "1298074214633706907132624082305024",
        /*5*/ "1622592768292133633915780102881280",
        /*6*/ "1947111321950560360698936123457536",
        /*7*/ "2271629875608987087482092144033792",
        /*8*/ "2596148429267413814265248164610048",
        /*9*/ "2920666982925840541048404185186304",
        /*A*/ "3245185536584267267831560205762560",
        /*B*/ "3569704090242693994614716226338816",
        /*C*/ "3894222643901120721397872246915072",
        /*D*/ "4218741197559547448181028267491328",
        /*E*/ "4543259751217974174964184288067584",
        /*F*/ "4867778304876400901747340308643840"},
    /*16^26*/
    {/*0*/ "0",
        /*1*/ "20282409603651670423947251286016",
        /*2*/ "40564819207303340847894502572032",
        /*3*/ "60847228810955011271841753858048",
        /*4*/ "81129638414606681695789005144064",
        /*5*/ "101412048018258352119736256430080",
        /*6*/ "121694457621910022543683507716096",
        /*7*/ "141976867225561692967630759002112",
        /*8*/ "162259276829213363391578010288128",
        /*9*/ "182541686432865033815525261574144",
        /*A*/ "202824096036516704239472512860160",
        /*B*/ "223106505640168374663419764146176",
        /*C*/ "243388915243820045087367015432192",
        /*D*/ "263671324847471715511314266718208",
        /*E*/ "283953734451123385935261518004224",
        /*F*/ "304236144054775056359208769290240"},
    /*16^25*/
    {/*0*/ "0",
        /*1*/ "1267650600228229401496703205376",
        /*2*/ "2535301200456458802993406410752",
        /*3*/ "3802951800684688204490109616128",
        /*4*/ "5070602400912917605986812821504",
        /*5*/ "6338253001141147007483516026880",
        /*6*/ "7605903601369376408980219232256",
        /*7*/ "8873554201597605810476922437632",
        /*8*/ "10141204801825835211973625643008",
        /*9*/ "11408855402054064613470328848384",
        /*A*/ "12676506002282294014967032053760",
        /*B*/ "13944156602510523416463735259136",
        /*C*/ "15211807202738752817960438464512",
        /*D*/ "16479457802966982219457141669888",
        /*E*/ "17747108403195211620953844875264",
        /*F*/ "19014759003423441022450548080640"},
    /*16^24*/
    {/*0*/ "0",
        /*1*/ "79228162514264337593543950336",
        /*2*/ "158456325028528675187087900672",
        /*3*/ "237684487542793012780631851008",
        /*4*/ "316912650057057350374175801344",
        /*5*/ "396140812571321687967719751680",
        /*6*/ "475368975085586025561263702016",
        /*7*/ "554597137599850363154807652352",
        /*8*/ "633825300114114700748351602688",
        /*9*/ "713053462628379038341895553024",
        /*A*/ "792281625142643375935439503360",
        /*B*/ "871509787656907713528983453696",
        /*C*/ "950737950171172051122527404032",
        /*D*/ "1029966112685436388716071354368",
        /*E*/ "1109194275199700726309615304704",
        /*F*/ "1188422437713965063903159255040"},
    /*16^23*/
    {/*0*/ "0",
        /*1*/ "4951760157141521099596496896",
        /*2*/ "9903520314283042199192993792",
        /*3*/ "14855280471424563298789490688",
        /*4*/ "19807040628566084398385987584",
        /*5*/ "24758800785707605497982484480",
        /*6*/ "29710560942849126597578981376",
        /*7*/ "34662321099990647697175478272",
        /*8*/ "39614081257132168796771975168",
        /*9*/ "44565841414273689896368472064",
        /*A*/ "49517601571415210995964968960",
        /*B*/ "54469361728556732095561465856",
        /*C*/ "59421121885698253195157962752",
        /*D*/ "64372882042839774294754459648",
        /*E*/ "69324642199981295394350956544",
        /*F*/ "74276402357122816493947453440"},
    /*16^22*/
    {/*0*/ "0",
        /*1*/ "309485009821345068724781056",
        /*2*/ "618970019642690137449562112",
        /*3*/ "928455029464035206174343168",
        /*4*/ "1237940039285380274899124224",
        /*5*/ "1547425049106725343623905280",
        /*6*/ "1856910058928070412348686336",
        /*7*/ "2166395068749415481073467392",
        /*8*/ "2475880078570760549798248448",
        /*9*/ "2785365088392105618523029504",
        /*A*/ "3094850098213450687247810560",
        /*B*/ "3404335108034795755972591616",
        /*C*/ "3713820117856140824697372672",
        /*D*/ "4023305127677485893422153728",
        /*E*/ "4332790137498830962146934784",
        /*F*/ "4642275147320176030871715840"},
    /*16^21*/
    {/*0*/ "0",
        /*1*/ "19342813113834066795298816",
        /*2*/ "38685626227668133590597632",
        /*3*/ "58028439341502200385896448",
        /*4*/ "77371252455336267181195264",
        /*5*/ "96714065569170333976494080",
        /*6*/ "116056878683004400771792896",
        /*7*/ "135399691796838467567091712",
        /*8*/ "154742504910672534362390528",
        /*9*/ "174085318024506601157689344",
        /*A*/ "193428131138340667952988160",
        /*B*/ "212770944252174734748286976",
        /*C*/ "232113757366008801543585792",
        /*D*/ "251456570479842868338884608",
        /*E*/ "270799383593676935134183424",
        /*F*/ "290142196707511001929482240"},
    /*16^20*/
    {/*0*/ "0",
        /*1*/ "1208925819614629174706176",
        /*2*/ "2417851639229258349412352",
        /*3*/ "3626777458843887524118528",
        /*4*/ "4835703278458516698824704",
        /*5*/ "6044629098073145873530880",
        /*6*/ "7253554917687775048237056",
        /*7*/ "8462480737302404222943232",
        /*8*/ "9671406556917033397649408",
        /*9*/ "10880332376531662572355584",
        /*A*/ "12089258196146291747061760",
        /*B*/ "13298184015760920921767936",
        /*C*/ "14507109835375550096474112",
        /*D*/ "15716035654990179271180288",
        /*E*/ "16924961474604808445886464",
        /*F*/ "18133887294219437620592640"},
    /*16^19*/
    {/*0*/ "0",
        /*1*/ "75557863725914323419136",
        /*2*/ "151115727451828646838272",
        /*3*/ "226673591177742970257408",
        /*4*/ "302231454903657293676544",
        /*5*/ "377789318629571617095680",
        /*6*/ "453347182355485940514816",
        /*7*/ "528905046081400263933952",
        /*8*/ "604462909807314587353088",
        /*9*/ "680020773533228910772224",
        /*A*/ "755578637259143234191360",
        /*B*/ "831136500985057557610496",
        /*C*/ "906694364710971881029632",
        /*D*/ "982252228436886204448768",
        /*E*/ "1057810092162800527867904",
        /*F*/ "1133367955888714851287040"},
    /*16^18*/
    {/*0*/ "0",
        /*1*/ "4722366482869645213696",
        /*2*/ "9444732965739290427392",
        /*3*/ "14167099448608935641088",
        /*4*/ "18889465931478580854784",
        /*5*/ "23611832414348226068480",
        /*6*/ "28334198897217871282176",
        /*7*/ "33056565380087516495872",
        /*8*/ "37778931862957161709568",
        /*9*/ "42501298345826806923264",
        /*A*/ "47223664828696452136960",
        /*B*/ "51946031311566097350656",
        /*C*/ "56668397794435742564352",
        /*D*/ "61390764277305387778048",
        /*E*/ "66113130760175032991744",
        /*F*/ "70835497243044678205440"},
    /*16^17*/
    {/*0*/ "0",
        /*1*/ "295147905179352825856",
        /*2*/ "590295810358705651712",
        /*3*/ "885443715538058477568",
        /*4*/ "1180591620717411303424",
        /*5*/ "1475739525896764129280",
        /*6*/ "1770887431076116955136",
        /*7*/ "2066035336255469780992",
        /*8*/ "2361183241434822606848",
        /*9*/ "2656331146614175432704",
        /*A*/ "2951479051793528258560",
        /*B*/ "3246626956972881084416",
        /*C*/ "3541774862152233910272",
        /*D*/ "3836922767331586736128",
        /*E*/ "4132070672510939561984",
        /*F*/ "4427218577690292387840"},
    /*16^16*/
    {/*0*/ "0",
        /*1*/ "18446744073709551616",
        /*2*/ "36893488147419103232",
        /*3*/ "55340232221128654848",
        /*4*/ "73786976294838206464",
        /*5*/ "92233720368547758080",
        /*6*/ "110680464442257309696",
        /*7*/ "129127208515966861312",
        /*8*/ "147573952589676412928",
        /*9*/ "166020696663385964544",
        /*A*/ "184467440737095516160",
        /*B*/ "202914184810805067776",
        /*C*/ "221360928884514619392",
        /*D*/ "239807672958224171008",
        /*E*/ "258254417031933722624",
        /*F*/ "276701161105643274240"},
    /*16^15*/
    {/*0*/ "0",
        /*1*/ "1152921504606846976",
        /*2*/ "2305843009213693952",
        /*3*/ "3458764513820540928",
        /*4*/ "4611686018427387904",
        /*5*/ "5764607523034234880",
        /*6*/ "6917529027641081856",
        /*7*/ "8070450532247928832",
        /*8*/ "9223372036854775808",
        /*9*/ "10376293541461622784",
        /*A*/ "11529215046068469760",
        /*B*/ "12682136550675316736",
        /*C*/ "13835058055282163712",
        /*D*/ "14987979559889010688",
        /*E*/ "16140901064495857664",
        /*F*/ "17293822569102704640"},
    /*16^14*/
    {/*0*/ "0",
        /*1*/ "72057594037927936",
        /*2*/ "144115188075855872",
        /*3*/ "216172782113783808",
        /*4*/ "288230376151711744",
        /*5*/ "360287970189639680",
        /*6*/ "432345564227567616",
        /*7*/ "504403158265495552",
        /*8*/ "576460752303423488",
        /*9*/ "648518346341351424",
        /*A*/ "720575940379279360",
        /*B*/ "792633534417207296",
        /*C*/ "864691128455135232",
        /*D*/ "936748722493063168",
        /*E*/ "1008806316530991104",
        /*F*/ "1080863910568919040"},
    /*16^13*/
    {/*0*/ "0",
        /*1*/ "4503599627370496",
        /*2*/ "9007199254740992",
        /*3*/ "13510798882111488",
        /*4*/ "18014398509481984",
        /*5*/ "22517998136852480",
        /*6*/ "27021597764222976",
        /*7*/ "31525197391593472",
        /*8*/ "36028797018963968",
        /*9*/ "40532396646334464",
        /*A*/ "45035996273704960",
        /*B*/ "49539595901075456",
        /*C*/ "54043195528445952",
        /*D*/ "58546795155816448",
        /*E*/ "63050394783186944",
        /*F*/ "67553994410557440"},
    /*16^12*/
    {/*0*/ "0",
        /*1*/ "281474976710656",
        /*2*/ "562949953421312",
        /*3*/ "844424930131968",
        /*4*/ "1125899906842624",
        /*5*/ "1407374883553280",
        /*6*/ "1688849860263936",
        /*7*/ "1970324836974592",
        /*8*/ "2251799813685248",
        /*9*/ "2533274790395904",
        /*A*/ "2814749767106560",
        /*B*/ "3096224743817216",
        /*C*/ "3377699720527872",
        /*D*/ "3659174697238528",
        /*E*/ "3940649673949184",
        /*F*/ "4222124650659840"},
    /*16^11*/
    {/*0*/ "0",
        /*1*/ "17592186044416",
        /*2*/ "35184372088832",
        /*3*/ "52776558133248",
        /*4*/ "70368744177664",
        /*5*/ "87960930222080",
        /*6*/ "105553116266496",
        /*7*/ "123145302310912",
        /*8*/ "140737488355328",
        /*9*/ "158329674399744",
        /*A*/ "175921860444160",
        /*B*/ "193514046488576",
        /*C*/ "211106232532992",
        /*D*/ "228698418577408",
        /*E*/ "246290604621824",
        /*F*/ "263882790666240"},
    /*16^10*/
    {/*0*/ "0",
        /*1*/ "1099511627776",
        /*2*/ "2199023255552",
        /*3*/ "3298534883328",
        /*4*/ "4398046511104",
        /*5*/ "5497558138880",
        /*6*/ "6597069766656",
        /*7*/ "7696581394432",
        /*8*/ "8796093022208",
        /*9*/ "9895604649984",
        /*A*/ "10995116277760",
        /*B*/ "12094627905536",
        /*C*/ "13194139533312",
        /*D*/ "14293651161088",
        /*E*/ "15393162788864",
        /*F*/ "16492674416640"},
    /*16^9*/
    {/*0*/ "0",
        /*1*/ "68719476736",
        /*2*/ "137438953472",
        /*3*/ "206158430208",
        /*4*/ "274877906944",
        /*5*/ "343597383680",
        /*6*/ "412316860416",
        /*7*/ "481036337152",
        /*8*/ "549755813888",
        /*9*/ "618475290624",
        /*A*/ "687194767360",
        /*B*/ "755914244096",
        /*C*/ "824633720832",
        /*D*/ "893353197568",
        /*E*/ "962072674304",
        /*F*/ "1030792151040"},
    /*16^8*/
    {/*0*/ "0",
        /*1*/ "4294967296",
        /*2*/ "8589934592",
        /*3*/ "12884901888",
        /*4*/ "17179869184",
        /*5*/ "21474836480",
        /*6*/ "25769803776",
        /*7*/ "30064771072",
        /*8*/ "34359738368",
        /*9*/ "38654705664",
        /*A*/ "42949672960",
        /*B*/ "47244640256",
        /*C*/ "51539607552",
        /*D*/ "55834574848",
        /*E*/ "60129542144",
        /*F*/ "64424509440"},
    /*16^7*/
    {/*0*/ "0",
        /*1*/ "268435456",
        /*2*/ "536870912",
        /*3*/ "805306368",
        /*4*/ "1073741824",
        /*5*/ "1342177280",
        /*6*/ "1610612736",
        /*7*/ "1879048192",
        /*8*/ "2147483648",
        /*9*/ "2415919104",
        /*A*/ "2684354560",
        /*B*/ "2952790016",
        /*C*/ "3221225472",
        /*D*/ "3489660928",
        /*E*/ "3758096384",
        /*F*/ "4026531840"},
    /*16^6*/
    {/*0*/ "0",
        /*1*/ "16777216",
        /*2*/ "33554432",
        /*3*/ "50331648",
        /*4*/ "67108864",
        /*5*/ "83886080",
        /*6*/ "100663296",
        /*7*/ "117440512",
        /*8*/ "134217728",
        /*9*/ "150994944",
        /*A*/ "167772160",
        /*B*/ "184549376",
        /*C*/ "201326592",
        /*D*/ "218103808",
        /*E*/ "234881024",
        /*F*/ "251658240"},
    /*16^5*/
    {/*0*/ "0",
        /*1*/ "1048576",
        /*2*/ "2097152",
        /*3*/ "3145728",
        /*4*/ "4194304",
        /*5*/ "5242880",
        /*6*/ "6291456",
        /*7*/ "7340032",
        /*8*/ "8388608",
        /*9*/ "9437184",
        /*A*/ "10485760",
        /*B*/ "11534336",
        /*C*/ "12582912",
        /*D*/ "13631488",
        /*E*/ "14680064",
        /*F*/ "15728640"},
    /*16^4*/
    {/*0*/ "0",
        /*1*/ "65536",
        /*2*/ "131072",
        /*3*/ "196608",
        /*4*/ "262144",
        /*5*/ "327680",
        /*6*/ "393216",
        /*7*/ "458752",
        /*8*/ "524288",
        /*9*/ "589824",
        /*A*/ "655360",
        /*B*/ "720896",
        /*C*/ "786432",
        /*D*/ "851968",
        /*E*/ "917504",
        /*F*/ "983040"},
    /*16^3*/
    {/*0*/ "0",
        /*1*/ "4096",
        /*2*/ "8192",
        /*3*/ "12288",
        /*4*/ "16384",
        /*5*/ "20480",
        /*6*/ "24576",
        /*7*/ "28672",
        /*8*/ "32768",
        /*9*/ "36864",
        /*A*/ "40960",
        /*B*/ "45056",
        /*C*/ "49152",
        /*D*/ "53248",
        /*E*/ "57344",
        /*F*/ "61440"},
    /*16^2*/
    {/*0*/ "0",
        /*1*/ "256",
        /*2*/ "512",
        /*3*/ "768",
        /*4*/ "1024",
        /*5*/ "1280",
        /*6*/ "1536",
        /*7*/ "1792",
        /*8*/ "2048",
        /*9*/ "2304",
        /*A*/ "2560",
        /*B*/ "2816",
        /*C*/ "3072",
        /*D*/ "3328",
        /*E*/ "3584",
        /*F*/ "3840"},
    /*16^1*/
    {/*0*/ "0",
        /*1*/ "16",
        /*2*/ "32",
        /*3*/ "48",
        /*4*/ "64",
        /*5*/ "80",
        /*6*/ "96",
        /*7*/ "112",
        /*8*/ "128",
        /*9*/ "144",
        /*A*/ "160",
        /*B*/ "176",
        /*C*/ "192",
        /*D*/ "208",
        /*E*/ "224",
        /*F*/ "240"},
    /*16^0*/
    {/*0*/ "0",
        /*1*/ "1",
        /*2*/ "2",
        /*3*/ "3",
        /*4*/ "4",
        /*5*/ "5",
        /*6*/ "6",
        /*7*/ "7",
        /*8*/ "8",
        /*9*/ "9",
        /*A*/ "10",
        /*B*/ "11",
        /*C*/ "12",
        /*D*/ "13",
        /*E*/ "14",
        /*F*/ "15"}};

/* ----------
 * Routines type
 * ----------
 */
#define DCH_TO_CHAR_TYPE 1 /* DATE-TIME version	*/
#define NUM_TYPE 2         /* NUMBER version	*/
#define DCH_TO_TIMESTAMP_TYPE 3

/* ----------
 * KeyWord Index (ascii from position 32 (' ') to 126 (~))
 * ----------
 */
#define KeyWord_INDEX_SIZE ('~' - ' ')
#define KeyWord_INDEX_FILTER(_c) ((((_c) <= ' ') || ((_c) >= '~')) ? 0 : 1)

/* ----------
 * Maximal length of one node
 * ----------
 */
#define DCH_MAX_ITEM_SIZ 12 /* max localized day name */
#define NUM_MAX_ITEM_SIZ 8  /* roman number (RN has 15 chars)	*/

/* ----------
 * More is in float.c
 * ----------
 */
#define MAXFLOATWIDTH 60
#define MAXDOUBLEWIDTH 500

/* ----------
 * External (defined in PgSQL datetime.c (timestamp utils))
 * ----------
 */
extern char *months[], /* month abbreviation	*/
    *days[];           /* full days		*/

/* ----------
 * Format parser structs
 * ----------
 */
typedef struct {
    char* name; /* suffix string		*/
    int len,    /* suffix length		*/
        id,     /* used in node->suffix */
        type;   /* prefix / postfix			*/
} KeySuffix;

#define NODE_TYPE_END 1
#define NODE_TYPE_ACTION 2
#define NODE_TYPE_CHAR 3

#define SUFFTYPE_PREFIX 1
#define SUFFTYPE_POSTFIX 2

#define CLOCK_24_HOUR 0
#define CLOCK_12_HOUR 1

/* ----------
 * Full months
 * ----------
 */
static char* months_full[] = {"January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
    NULL};

static char* days_short[] = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", NULL};

/* ----------
 * AD / BC
 * ----------
 *	There is no 0 AD.  Years go from 1 BC to 1 AD, so we make it
 *	positive and map year == -1 to year zero, and shift all negative
 *	years up one.  For interval years, we just return the year.
 */
#define ADJUST_YEAR(year, is_interval) ((is_interval) ? (year) : (((year) <= 0) ? -((year)-1) : (year)))

#define A_D_STR "A.D."
#define a_d_STR "a.d."
#define AD_STR "AD"
#define ad_STR "ad"

#define B_C_STR "B.C."
#define b_c_STR "b.c."
#define BC_STR "BC"
#define bc_STR "bc"

/*
 * AD / BC strings for seq_search.
 *
 * These are given in two variants, a long form with periods and a standard
 * form without.
 *
 * The array is laid out such that matches for AD have an even index, and
 * matches for BC have an odd index.  So the boolean value for BC is given by
 * taking the array index of the match, modulo 2.
 */
static char* adbc_strings[] = {ad_STR, bc_STR, AD_STR, BC_STR, NULL};
static char* adbc_strings_long[] = {a_d_STR, b_c_STR, A_D_STR, B_C_STR, NULL};

/* ----------
 * AM / PM
 * ----------
 */
#define A_M_STR "A.M."
#define a_m_STR "a.m."
#ifdef AM_STR
#undef AM_STR
#endif
#define AM_STR "AM"
#define am_STR "am"

#define P_M_STR "P.M."
#define p_m_STR "p.m."
#ifdef PM_STR
#undef PM_STR
#endif
#define PM_STR "PM"
#define pm_STR "pm"

/*
 * AM / PM strings for seq_search.
 *
 * These are given in two variants, a long form with periods and a standard
 * form without.
 *
 * The array is laid out such that matches for AM have an even index, and
 * matches for PM have an odd index.  So the boolean value for PM is given by
 * taking the array index of the match, modulo 2.
 */
static char* ampm_strings[] = {am_STR, pm_STR, AM_STR, PM_STR, NULL};
static char* ampm_strings_long[] = {a_m_STR, p_m_STR, A_M_STR, P_M_STR, NULL};

/* ----------
 * Months in roman-numeral
 * (Must be in reverse order for seq_search (in FROM_CHAR), because
 *	'VIII' must have higher precedence than 'V')
 * ----------
 */
static char* rm_months_upper[] = {"XII", "XI", "X", "IX", "VIII", "VII", "VI", "V", "IV", "III", "II", "I", NULL};

static char* rm_months_lower[] = {"xii", "xi", "x", "ix", "viii", "vii", "vi", "v", "iv", "iii", "ii", "i", NULL};

/* ----------
 * Roman numbers
 * ----------
 */
static char* rm1[] = {"I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", NULL};
static char* rm10[] = {"X", "XX", "XXX", "XL", "L", "LX", "LXX", "LXXX", "XC", NULL};
static char* rm100[] = {"C", "CC", "CCC", "CD", "D", "DC", "DCC", "DCCC", "CM", NULL};

/* ----------
 * Ordinal postfixes
 * ----------
 */
static char* numTH[] = {"ST", "ND", "RD", "TH", NULL};
static char* numth[] = {"st", "nd", "rd", "th", NULL};

/* ----------
 * Flags & Options:
 * ----------
 */
#define ONE_UPPER 1 /* Name */
#define ALL_UPPER 2 /* NAME */
#define ALL_LOWER 3 /* name */

#define FULL_SIZ 0

#define MAX_MONTH_LEN 9
#define MAX_MON_LEN 3
#define MAX_DAY_LEN 9
#define MAX_DY_LEN 3
#define MAX_RM_LEN 4

#define TH_UPPER 1
#define TH_LOWER 2

/* ----------
 * Flags for NUMBER version
 * ----------
 */
#define NUM_F_DECIMAL (1 << 1)
#define NUM_F_LDECIMAL (1 << 2)
#define NUM_F_ZERO (1 << 3)
#define NUM_F_BLANK (1 << 4)
#define NUM_F_FILLMODE (1 << 5)
#define NUM_F_LSIGN (1 << 6)
#define NUM_F_BRACKET (1 << 7)
#define NUM_F_MINUS (1 << 8)
#define NUM_F_PLUS (1 << 9)
#define NUM_F_ROMAN (1 << 10)
#define NUM_F_MULTI (1 << 11)
#define NUM_F_PLUS_POST (1 << 12)
#define NUM_F_MINUS_POST (1 << 13)
#define NUM_F_EEEE (1 << 14)
#define NUM_F_x16 (1 << 15)
#define NUM_F_X16 (1 << 16)

#define NUM_LSIGN_PRE (-1)
#define NUM_LSIGN_POST 1
#define NUM_LSIGN_NONE 0

/* ----------
 * Tests
 * ----------
 */
#define IS_DECIMAL(_f) ((unsigned int)(_f)->flag & NUM_F_DECIMAL)
#define IS_LDECIMAL(_f) ((unsigned int)(_f)->flag & NUM_F_LDECIMAL)
#define IS_ZERO(_f) ((unsigned int)(_f)->flag & NUM_F_ZERO)
#define IS_BLANK(_f) ((unsigned int)(_f)->flag & NUM_F_BLANK)
#define IS_FILLMODE(_f) ((unsigned int)(_f)->flag & NUM_F_FILLMODE)
#define IS_BRACKET(_f) ((unsigned int)(_f)->flag & NUM_F_BRACKET)
#define IS_MINUS(_f) ((unsigned int)(_f)->flag & NUM_F_MINUS)
#define IS_LSIGN(_f) ((unsigned int)(_f)->flag & NUM_F_LSIGN)
#define IS_PLUS(_f) ((unsigned int)(_f)->flag & NUM_F_PLUS)
#define IS_ROMAN(_f) ((unsigned int)(_f)->flag & NUM_F_ROMAN)
#define IS_MULTI(_f) ((unsigned int)(_f)->flag & NUM_F_MULTI)
#define IS_EEEE(_f) ((unsigned int)(_f)->flag & NUM_F_EEEE)
#define IS_x16(_f) ((unsigned int)(_f)->flag & NUM_F_x16)
#define IS_X16(_f) ((unsigned int)(_f)->flag & NUM_F_X16)
/* ----------
 * Format picture cache
 *	(cache size:
 *		Number part = NUM_CACHE_SIZE * NUM_CACHE_FIELDS
 *		Date-time part	= DCH_CACHE_SIZE * DCH_CACHE_FIELDS
 *	)
 * ----------
 */

#define MAX_INT32 2147483600

/* ----------
 * For char->date/time conversion
 * ----------
 */
typedef struct {
    FromCharDateMode mode;
    int hh, pm, mi, ss, sssss, d, dd, ddd, mm, ms, year, bc, ww, w, cc, j, us, yysz, /* is it YY or YYYY ? */
        clock;                                                                       /* 12 or 24 hour clock? */
} TmFromChar;

/*
 * to denote whether the value in struct TmFromChar has been evaluated
 */
typedef struct {
    bool hh_flag;
    bool pm_flag;
    bool mi_flag;
    bool ss_flag;
    bool sssss_flag;
    bool d_flag;
    bool dd_flag;
    bool ddd_flag;
    bool mm_flag;
    bool ms_flag;
    bool year_flag;
    bool bc_flag;
    bool ww_flag;
    bool w_flag;
    bool cc_flag;
    bool j_flag;
    bool us_flag;
    bool yysz_flag;
    bool clock_flag;
} TmFromCharFlag;

/* ----------
 * Debug
 * ----------
 */
#ifdef DEBUG_TO_FROM_CHAR
#define DEBUG_TMFC(_X)                                                                                           \
    elog(DEBUG_elog_output,                                                                                      \
        "TMFC:\nmode %d\nhh %d\npm %d\nmi %d\nss %d\nsssss %d\nd %d\ndd %d\nddd %d\nmm %d\nms: %d\nyear %d\nbc " \
        "%d\nww %d\nw %d\ncc %d\nj %d\nus: %d\nyysz: %d\nclock: %d",                                             \
        (_X)->mode,                                                                                              \
        (_X)->hh,                                                                                                \
        (_X)->pm,                                                                                                \
        (_X)->mi,                                                                                                \
        (_X)->ss,                                                                                                \
        (_X)->sssss,                                                                                             \
        (_X)->d,                                                                                                 \
        (_X)->dd,                                                                                                \
        (_X)->ddd,                                                                                               \
        (_X)->mm,                                                                                                \
        (_X)->ms,                                                                                                \
        (_X)->year,                                                                                              \
        (_X)->bc,                                                                                                \
        (_X)->ww,                                                                                                \
        (_X)->w,                                                                                                 \
        (_X)->cc,                                                                                                \
        (_X)->j,                                                                                                 \
        (_X)->us,                                                                                                \
        (_X)->yysz,                                                                                              \
        (_X)->clock);
#define DEBUG_TM(_X)                                                                             \
    elog(DEBUG_elog_output,                                                                      \
        "TM:\nsec %d\nyear %d\nmin %d\nwday %d\nhour %d\nyday %d\nmday %d\nnisdst %d\nmon %d\n", \
        (_X)->tm_sec,                                                                            \
        (_X)->tm_year,                                                                           \
        (_X)->tm_min,                                                                            \
        (_X)->tm_wday,                                                                           \
        (_X)->tm_hour,                                                                           \
        (_X)->tm_yday,                                                                           \
        (_X)->tm_mday,                                                                           \
        (_X)->tm_isdst,                                                                          \
        (_X)->tm_mon)
#else
#define DEBUG_TMFC(_X)
#define DEBUG_TM(_X)
#endif

#define tmtcTm(_X) (&(_X)->tm)
#define tmtcTzn(_X) ((_X)->tzn)
#define tmtcFsec(_X) ((_X)->fsec)

/*
 *	to_char(time) appears to to_char() as an interval, so this check
 *	is really for interval and time data types.
 */
#define INVALID_FOR_INTERVAL                                                         \
    do {                                                                             \
        if (is_interval)                                                             \
            ereport(ERROR,                                                           \
                (errcode(ERRCODE_INVALID_DATETIME_FORMAT),                           \
                    errmsg("invalid format specification for an interval value"),    \
                    errhint("Intervals are not tied to specific calendar dates."))); \
    } while (0)

/*****************************************************************************
 *			KeyWord definitions
 *****************************************************************************/

/* ----------
 * Suffixes:
 * ----------
 */
#define DCH_S_FM 0x01
#define DCH_S_TH 0x02
#define DCH_S_th 0x04
#define DCH_S_SP 0x08
#define DCH_S_TM 0x10

/* ----------
 * Suffix tests
 * ----------
 */
#define S_THth(_s) ((((unsigned int)(_s)&DCH_S_TH) || ((unsigned int)(_s)&DCH_S_th)) ? 1 : 0)
#define S_TH(_s) (((unsigned int)(_s)&DCH_S_TH) ? 1 : 0)
#define S_th(_s) (((unsigned int)(_s)&DCH_S_th) ? 1 : 0)
#define S_TH_TYPE(_s) (((unsigned int)(_s)&DCH_S_TH) ? TH_UPPER : TH_LOWER)

/* A db toggles FM behavior, we don't; see docs. */
#define S_FM(_s) (((unsigned int)(_s)&DCH_S_FM) ? 1 : 0)
#define S_SP(_s) (((unsigned int)(_s)&DCH_S_SP) ? 1 : 0)
#define S_TM(_s) (((unsigned int)(_s)&DCH_S_TM) ? 1 : 0)

/* ----------
 * Suffixes definition for DATE-TIME TO/FROM CHAR
 * ----------
 */
#define TM_SUFFIX_LEN 2

static KeySuffix DCH_suff[] = {{"FM", 2, DCH_S_FM, SUFFTYPE_PREFIX},
    {"fm", 2, DCH_S_FM, SUFFTYPE_PREFIX},
    {"TM", TM_SUFFIX_LEN, DCH_S_TM, SUFFTYPE_PREFIX},
    {"tm", 2, DCH_S_TM, SUFFTYPE_PREFIX},
    {"TH", 2, DCH_S_TH, SUFFTYPE_POSTFIX},
    {"th", 2, DCH_S_th, SUFFTYPE_POSTFIX},
    {"SP", 2, DCH_S_SP, SUFFTYPE_POSTFIX},
    /* last */
    {NULL, 0, 0, 0}};

/* ----------
 * Format-pictures (KeyWord).
 *
 * The KeyWord field; alphabetic sorted, *BUT* strings alike is sorted
 *		  complicated -to-> easy:
 *
 *	(example: "DDD","DD","Day","D" )
 *
 * (this specific sort needs the algorithm for sequential search for strings,
 * which not has exact end; -> How keyword is in "HH12blabla" ? - "HH"
 * or "HH12"? You must first try "HH12", because "HH" is in string, but
 * it is not good.
 *
 * (!)
 *	 - Position for the keyword is similar as position in the enum DCH/NUM_poz.
 * (!)
 *
 * For fast search is used the 'int index[]', index is ascii table from position
 * 32 (' ') to 126 (~), in this index is DCH_ / NUM_ enums for each ASCII
 * position or -1 if char is not used in the KeyWord. Search example for
 * string "MM":
 *	1)	see in index to index['M' - 32],
 *	2)	take keywords position (enum DCH_MI) from index
 *	3)	run sequential search in keywords[] from this position
 *
 * ----------
 */

typedef enum {
    /* parse_field_map[] requires that DCH_A_D must be 0 */
    DCH_A_D = 0,
    DCH_A_M,
    DCH_AD,
    DCH_AM,
    DCH_B_C,
    DCH_BC,
    DCH_CC,
    DCH_DAY,
    DCH_DDD,
    DCH_DD,
    DCH_DY,
    DCH_Day,
    DCH_Dy,
    DCH_D,
    DCH_FF1,
    DCH_FF2,
    DCH_FF3,
    DCH_FF4,
    DCH_FF5,
    DCH_FF6,
    DCH_FF,
    DCH_FX,
    DCH_HH24,
    DCH_HH12,
    DCH_HH,
    DCH_IDDD,
    DCH_ID,
    DCH_IW,
    DCH_IYYY,
    DCH_IYY,
    DCH_IY,
    DCH_I,
    DCH_J,
    DCH_MI,
    DCH_MM,
    DCH_MONTH,
    DCH_MON,
    DCH_MS,
    DCH_Month,
    DCH_Mon,
    DCH_P_M,
    DCH_PM,
    DCH_Q,
    DCH_RM,
    DCH_RRRR,
    DCH_RR,
    DCH_SSSSS,
    DCH_SS,
    DCH_SYYYY,
    DCH_TZ,
    DCH_US,
    DCH_WW,
    DCH_W,
    DCH_X,
    DCH_Y_YYY,
    DCH_YYYY,
    DCH_YYY,
    DCH_YY,
    DCH_Y,
    DCH_a_d,
    DCH_a_m,
    DCH_ad,
    DCH_am,
    DCH_b_c,
    DCH_bc,
    DCH_cc,
    DCH_day,
    DCH_ddd,
    DCH_dd,
    DCH_dy,
    DCH_d,
    DCH_ff1,
    DCH_ff2,
    DCH_ff3,
    DCH_ff4,
    DCH_ff5,
    DCH_ff6,
    DCH_ff,
    DCH_fx,
    DCH_hh24,
    DCH_hh12,
    DCH_hh,
    DCH_iddd,
    DCH_id,
    DCH_iw,
    DCH_iyyy,
    DCH_iyy,
    DCH_iy,
    DCH_i,
    DCH_j,
    DCH_mi,
    DCH_mm,
    DCH_month,
    DCH_mon,
    DCH_ms,
    DCH_p_m,
    DCH_pm,
    DCH_q,
    DCH_rm,
    DCH_rrrr,
    DCH_rr,
    DCH_sssss,
    DCH_ss,
    DCH_syyyy,
    DCH_tz,
    DCH_us,
    DCH_ww,
    DCH_w,
    DCH_x,
    DCH_y_yyy,
    DCH_yyyy,
    DCH_yyy,
    DCH_yy,
    DCH_y,

    /* last */
    _DCH_last_
} DCH_poz;

typedef enum {
    NUM_COMMA,
    NUM_DEC,
    NUM_0,
    NUM_9,
    NUM_B,
    NUM_C,
    NUM_D,
    NUM_E,
    NUM_FM,
    NUM_G,
    NUM_L,
    NUM_MI,
    NUM_PL,
    NUM_PR,
    NUM_RN,
    NUM_SG,
    NUM_SP,
    NUM_S,
    NUM_TH,
    NUM_V,
    NUM_X,
    NUM_b,
    NUM_c,
    NUM_d,
    NUM_e,
    NUM_fm,
    NUM_g,
    NUM_l,
    NUM_mi,
    NUM_pl,
    NUM_pr,
    NUM_rn,
    NUM_sg,
    NUM_sp,
    NUM_s,
    NUM_th,
    NUM_v,
    NUM_x,

    /* last */
    _NUM_last_
} NUM_poz;

/* ----------
 * KeyWords for DATE-TIME version
 * ----------
 */
static const KeyWord DCH_keywords[] = {
    /*	name, len, id, is_digit, date_mode */
    {"A.D.", 4, DCH_A_D, FALSE, FROM_CHAR_DATE_NONE}, /* A */
    {"A.M.", 4, DCH_A_M, FALSE, FROM_CHAR_DATE_NONE},
    {"AD", 2, DCH_AD, FALSE, FROM_CHAR_DATE_NONE},
    {"AM", 2, DCH_AM, FALSE, FROM_CHAR_DATE_NONE},
    {"B.C.", 4, DCH_B_C, FALSE, FROM_CHAR_DATE_NONE}, /* B */
    {"BC", 2, DCH_BC, FALSE, FROM_CHAR_DATE_NONE},
    {"CC", 2, DCH_CC, TRUE, FROM_CHAR_DATE_NONE},    /* C */
    {"DAY", 3, DCH_DAY, FALSE, FROM_CHAR_DATE_NONE}, /* D */
    {"DDD", 3, DCH_DDD, TRUE, FROM_CHAR_DATE_GREGORIAN},
    {"DD", 2, DCH_DD, TRUE, FROM_CHAR_DATE_GREGORIAN},
    {"DY", 2, DCH_DY, FALSE, FROM_CHAR_DATE_NONE},
    {"Day", 3, DCH_Day, FALSE, FROM_CHAR_DATE_NONE},
    {"Dy", 2, DCH_Dy, FALSE, FROM_CHAR_DATE_NONE},
    {"D", 1, DCH_D, TRUE, FROM_CHAR_DATE_GREGORIAN},
    {"FF1", 3, DCH_FF1, TRUE, FROM_CHAR_DATE_NONE}, /* F */
    {"FF2", 3, DCH_FF2, TRUE, FROM_CHAR_DATE_NONE},
    {"FF3", 3, DCH_FF3, TRUE, FROM_CHAR_DATE_NONE},
    {"FF4", 3, DCH_FF4, TRUE, FROM_CHAR_DATE_NONE},
    {"FF5", 3, DCH_FF5, TRUE, FROM_CHAR_DATE_NONE},
    {"FF6", 3, DCH_FF6, TRUE, FROM_CHAR_DATE_NONE},
    {"FF", 2, DCH_US, TRUE, FROM_CHAR_DATE_NONE},
    {"FX", 2, DCH_FX, FALSE, FROM_CHAR_DATE_NONE},
    {"HH24", 4, DCH_HH24, TRUE, FROM_CHAR_DATE_NONE}, /* H */
    {"HH12", 4, DCH_HH12, TRUE, FROM_CHAR_DATE_NONE},
    {"HH", 2, DCH_HH, TRUE, FROM_CHAR_DATE_NONE},
    {"IDDD", 4, DCH_IDDD, TRUE, FROM_CHAR_DATE_ISOWEEK}, /* I */
    {"ID", 2, DCH_ID, TRUE, FROM_CHAR_DATE_ISOWEEK},
    {"IW", 2, DCH_IW, TRUE, FROM_CHAR_DATE_ISOWEEK},
    {"IYYY", 4, DCH_IYYY, TRUE, FROM_CHAR_DATE_ISOWEEK},
    {"IYY", 3, DCH_IYY, TRUE, FROM_CHAR_DATE_ISOWEEK},
    {"IY", 2, DCH_IY, TRUE, FROM_CHAR_DATE_ISOWEEK},
    {"I", 1, DCH_I, TRUE, FROM_CHAR_DATE_ISOWEEK},
    {"J", 1, DCH_J, TRUE, FROM_CHAR_DATE_NONE},   /* J */
    {"MI", 2, DCH_MI, TRUE, FROM_CHAR_DATE_NONE}, /* M */
    {"MM", 2, DCH_MM, TRUE, FROM_CHAR_DATE_GREGORIAN},
    {"MONTH", 5, DCH_MONTH, FALSE, FROM_CHAR_DATE_GREGORIAN},
    {"MON", 3, DCH_MON, FALSE, FROM_CHAR_DATE_GREGORIAN},
    {"MS", 2, DCH_MS, TRUE, FROM_CHAR_DATE_NONE},
    {"Month", 5, DCH_Month, FALSE, FROM_CHAR_DATE_GREGORIAN},
    {"Mon", 3, DCH_Mon, FALSE, FROM_CHAR_DATE_GREGORIAN},
    {"P.M.", 4, DCH_P_M, FALSE, FROM_CHAR_DATE_NONE}, /* P */
    {"PM", 2, DCH_PM, FALSE, FROM_CHAR_DATE_NONE},
    {"Q", 1, DCH_Q, TRUE, FROM_CHAR_DATE_NONE},         /* Q */
    {"RM", 2, DCH_RM, FALSE, FROM_CHAR_DATE_GREGORIAN}, /* R */
    {"RRRR", 4, DCH_RRRR, TRUE, FROM_CHAR_DATE_NONE},
    {"RR", 2, DCH_RR, TRUE, FROM_CHAR_DATE_NONE},
    {"SSSSS", 5, DCH_SSSSS, TRUE, FROM_CHAR_DATE_NONE}, /* S */
    {"SS", 2, DCH_SS, TRUE, FROM_CHAR_DATE_NONE},
    {"SYYYY", 5, DCH_SYYYY, TRUE, FROM_CHAR_DATE_NONE},
    {"TZ", 2, DCH_TZ, FALSE, FROM_CHAR_DATE_NONE},     /* T */
    {"US", 2, DCH_US, TRUE, FROM_CHAR_DATE_NONE},      /* U */
    {"WW", 2, DCH_WW, TRUE, FROM_CHAR_DATE_GREGORIAN}, /* W */
    {"W", 1, DCH_W, TRUE, FROM_CHAR_DATE_GREGORIAN},
    {"X", 1, DCH_X, TRUE, FROM_CHAR_DATE_NONE}, /* X */
    {"Y,YYY", 5, DCH_Y_YYY, TRUE, FROM_CHAR_DATE_GREGORIAN}, /* Y */
    {"YYYY", 4, DCH_YYYY, TRUE, FROM_CHAR_DATE_GREGORIAN},
    {"YYY", 3, DCH_YYY, TRUE, FROM_CHAR_DATE_GREGORIAN},
    {"YY", 2, DCH_YY, TRUE, FROM_CHAR_DATE_GREGORIAN},
    {"Y", 1, DCH_Y, TRUE, FROM_CHAR_DATE_GREGORIAN},
    {"a.d.", 4, DCH_a_d, FALSE, FROM_CHAR_DATE_NONE}, /* a */
    {"a.m.", 4, DCH_a_m, FALSE, FROM_CHAR_DATE_NONE},
    {"ad", 2, DCH_ad, FALSE, FROM_CHAR_DATE_NONE},
    {"am", 2, DCH_am, FALSE, FROM_CHAR_DATE_NONE},
    {"b.c.", 4, DCH_b_c, FALSE, FROM_CHAR_DATE_NONE}, /* b */
    {"bc", 2, DCH_bc, FALSE, FROM_CHAR_DATE_NONE},
    {"cc", 2, DCH_CC, TRUE, FROM_CHAR_DATE_NONE},    /* c */
    {"day", 3, DCH_day, FALSE, FROM_CHAR_DATE_NONE}, /* d */
    {"ddd", 3, DCH_DDD, TRUE, FROM_CHAR_DATE_GREGORIAN},
    {"dd", 2, DCH_DD, TRUE, FROM_CHAR_DATE_GREGORIAN},
    {"dy", 2, DCH_dy, FALSE, FROM_CHAR_DATE_NONE},
    {"d", 1, DCH_D, TRUE, FROM_CHAR_DATE_GREGORIAN},
    {"ff1", 3, DCH_FF1, TRUE, FROM_CHAR_DATE_NONE}, /* f */
    {"ff2", 3, DCH_FF2, TRUE, FROM_CHAR_DATE_NONE},
    {"ff3", 3, DCH_FF3, TRUE, FROM_CHAR_DATE_NONE},
    {"ff4", 3, DCH_FF4, TRUE, FROM_CHAR_DATE_NONE},
    {"ff5", 3, DCH_FF5, TRUE, FROM_CHAR_DATE_NONE},
    {"ff6", 3, DCH_FF6, TRUE, FROM_CHAR_DATE_NONE},
    {"ff", 2, DCH_US, TRUE, FROM_CHAR_DATE_NONE},
    {"fx", 2, DCH_FX, FALSE, FROM_CHAR_DATE_NONE},
    {"hh24", 4, DCH_HH24, TRUE, FROM_CHAR_DATE_NONE}, /* h */
    {"hh12", 4, DCH_HH12, TRUE, FROM_CHAR_DATE_NONE},
    {"hh", 2, DCH_HH, TRUE, FROM_CHAR_DATE_NONE},
    {"iddd", 4, DCH_IDDD, TRUE, FROM_CHAR_DATE_ISOWEEK}, /* i */
    {"id", 2, DCH_ID, TRUE, FROM_CHAR_DATE_ISOWEEK},
    {"iw", 2, DCH_IW, TRUE, FROM_CHAR_DATE_ISOWEEK},
    {"iyyy", 4, DCH_IYYY, TRUE, FROM_CHAR_DATE_ISOWEEK},
    {"iyy", 3, DCH_IYY, TRUE, FROM_CHAR_DATE_ISOWEEK},
    {"iy", 2, DCH_IY, TRUE, FROM_CHAR_DATE_ISOWEEK},
    {"i", 1, DCH_I, TRUE, FROM_CHAR_DATE_ISOWEEK},
    {"j", 1, DCH_J, TRUE, FROM_CHAR_DATE_NONE},   /* j */
    {"mi", 2, DCH_MI, TRUE, FROM_CHAR_DATE_NONE}, /* m */
    {"mm", 2, DCH_MM, TRUE, FROM_CHAR_DATE_GREGORIAN},
    {"month", 5, DCH_month, FALSE, FROM_CHAR_DATE_GREGORIAN},
    {"mon", 3, DCH_mon, FALSE, FROM_CHAR_DATE_GREGORIAN},
    {"ms", 2, DCH_MS, TRUE, FROM_CHAR_DATE_NONE},
    {"p.m.", 4, DCH_p_m, FALSE, FROM_CHAR_DATE_NONE}, /* p */
    {"pm", 2, DCH_pm, FALSE, FROM_CHAR_DATE_NONE},
    {"q", 1, DCH_Q, TRUE, FROM_CHAR_DATE_NONE},         /* q */
    {"rm", 2, DCH_rm, FALSE, FROM_CHAR_DATE_GREGORIAN}, /* r */
    {"rrrr", 4, DCH_RRRR, TRUE, FROM_CHAR_DATE_NONE},
    {"rr", 2, DCH_RR, TRUE, FROM_CHAR_DATE_NONE},
    {"sssss", 5, DCH_SSSSS, TRUE, FROM_CHAR_DATE_NONE}, /* s */
    {"ss", 2, DCH_SS, TRUE, FROM_CHAR_DATE_NONE},
    {"syyyy", 5, DCH_SYYYY, TRUE, FROM_CHAR_DATE_NONE},
    {"tz", 2, DCH_tz, FALSE, FROM_CHAR_DATE_NONE},     /* t */
    {"us", 2, DCH_US, TRUE, FROM_CHAR_DATE_NONE},      /* u */
    {"ww", 2, DCH_WW, TRUE, FROM_CHAR_DATE_GREGORIAN}, /* w */
    {"w", 1, DCH_W, TRUE, FROM_CHAR_DATE_GREGORIAN},
    {"x", 1, DCH_X, TRUE, FROM_CHAR_DATE_NONE}, /* x */
    {"y,yyy", 5, DCH_Y_YYY, TRUE, FROM_CHAR_DATE_GREGORIAN}, /* y */
    {"yyyy", 4, DCH_YYYY, TRUE, FROM_CHAR_DATE_GREGORIAN},
    {"yyy", 3, DCH_YYY, TRUE, FROM_CHAR_DATE_GREGORIAN},
    {"yy", 2, DCH_YY, TRUE, FROM_CHAR_DATE_GREGORIAN},
    {"y", 1, DCH_Y, TRUE, FROM_CHAR_DATE_GREGORIAN},

    /* last */
    {NULL, 0, 0, 0, (FromCharDateMode)0}};

/* ----------
 * KeyWords for NUMBER version
 *
 * The is_digit and date_mode fields are not relevant here.
 * ----------
 */
static const KeyWord NUM_keywords[] = {
    /*	name, len, id			is in Index */
    {",", 1, NUM_COMMA}, /* , */
    {".", 1, NUM_DEC},   /* . */
    {"0", 1, NUM_0},     /* 0 */
    {"9", 1, NUM_9},     /* 9 */
    {"B", 1, NUM_B},     /* B */
    {"C", 1, NUM_C},     /* C */
    {"D", 1, NUM_D},     /* D */
    {"EEEE", 4, NUM_E},  /* E */
    {"FM", 2, NUM_FM},   /* F */
    {"G", 1, NUM_G},     /* G */
    {"L", 1, NUM_L},     /* L */
    {"MI", 2, NUM_MI},   /* M */
    {"PL", 2, NUM_PL},   /* P */
    {"PR", 2, NUM_PR},
    {"RN", 2, NUM_RN}, /* R */
    {"SG", 2, NUM_SG}, /* S */
    {"SP", 2, NUM_SP},
    {"S", 1, NUM_S},
    {"TH", 2, NUM_TH},  /* T */
    {"V", 1, NUM_V},    /* V */
    {"X", 1, NUM_X},    /* X */
    {"b", 1, NUM_B},    /* b */
    {"c", 1, NUM_C},    /* c */
    {"d", 1, NUM_D},    /* d */
    {"eeee", 4, NUM_E}, /* e */
    {"fm", 2, NUM_FM},  /* f */
    {"g", 1, NUM_G},    /* g */
    {"l", 1, NUM_L},    /* l */
    {"mi", 2, NUM_MI},  /* m */
    {"pl", 2, NUM_PL},  /* p */
    {"pr", 2, NUM_PR},
    {"rn", 2, NUM_rn}, /* r */
    {"sg", 2, NUM_SG}, /* s */
    {"sp", 2, NUM_SP},
    {"s", 1, NUM_S},
    {"th", 2, NUM_th}, /* t */
    {"v", 1, NUM_V},   /* v */
    {"x", 1, NUM_x},   /* x */

    /* last */
    {NULL, 0, 0}};

/* ----------
 * KeyWords index for DATE-TIME version
 * ----------
 */
static const int DCH_index[KeyWord_INDEX_SIZE] = {
    /*
    0	1	2	3	4	5	6	7	8	9
    */
    /* ---- first 0..31 chars are skipped ---- */

    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    DCH_A_D,
    DCH_B_C,
    DCH_CC,
    DCH_DAY,
    -1,
    DCH_FF1,
    -1,
    DCH_HH24,
    DCH_IDDD,
    DCH_J,
    -1,
    -1,
    DCH_MI,
    -1,
    -1,
    DCH_P_M,
    DCH_Q,
    DCH_RM,
    DCH_SSSSS,
    DCH_TZ,
    DCH_US,
    -1,
    DCH_WW,
    DCH_X,
    DCH_Y_YYY,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    DCH_a_d,
    DCH_b_c,
    DCH_cc,
    DCH_day,
    -1,
    DCH_ff1,
    -1,
    DCH_hh24,
    DCH_iddd,
    DCH_j,
    -1,
    -1,
    DCH_mi,
    -1,
    -1,
    DCH_p_m,
    DCH_q,
    DCH_rm,
    DCH_sssss,
    DCH_tz,
    DCH_us,
    -1,
    DCH_ww,
    DCH_x,
    DCH_y_yyy,
    -1,
    -1,
    -1,
    -1

    /* ---- chars over 126 are skipped ---- */
};

/* ----------
 * KeyWords index for NUMBER version
 * ----------
 */
static const int NUM_index[KeyWord_INDEX_SIZE] = {
    /*
    0	1	2	3	4	5	6	7	8	9
    */
    /* ---- first 0..31 chars are skipped ---- */

    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    NUM_COMMA,
    -1,
    NUM_DEC,
    -1,
    NUM_0,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    NUM_9,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    NUM_B,
    NUM_C,
    NUM_D,
    NUM_E,
    NUM_FM,
    NUM_G,
    -1,
    -1,
    -1,
    -1,
    NUM_L,
    NUM_MI,
    -1,
    -1,
    NUM_PL,
    -1,
    NUM_RN,
    NUM_SG,
    NUM_TH,
    -1,
    NUM_V,
    -1,
    NUM_X,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    NUM_b,
    NUM_c,
    NUM_d,
    NUM_e,
    NUM_fm,
    NUM_g,
    -1,
    -1,
    -1,
    -1,
    NUM_l,
    NUM_mi,
    -1,
    -1,
    NUM_pl,
    -1,
    NUM_rn,
    NUM_sg,
    NUM_th,
    -1,
    NUM_v,
    -1,
    NUM_x,
    -1,
    -1,
    -1,
    -1,
    -1
    /* ---- chars over 126 are skipped ---- */
};

/* ----------
 * Number processor struct
 * ----------
 */
typedef struct NUMProc {
    bool is_to_char;
    NUMDesc* Num; /* number description		*/

    int sign,       /* '-' or '+'			*/
        sign_wrote, /* was sign write		*/
        num_count,  /* number of write digits	*/
        num_in,     /* is inside number		*/
        num_curr,   /* current position in number	*/
        num_pre,    /* space before first number	*/

        read_dec,  /* to_number - was read dec. point	*/
        read_post, /* to_number - number of dec. digit */
        read_pre;  /* to_number - number non-dec. digit */

    char *number,       /* string with number	*/
        *number_p,      /* pointer to current number position */
        *inout,         /* in / out buffer	*/
        *inout_p,       /* pointer to current inout position */
        *last_relevant, /* last relevant number after decimal point */

        *L_negative_sign, /* Locale */
        *L_positive_sign, *decimal, *L_thousands_sep, *L_currency_symbol;
} NUMProc;

/* function pointer defination */
typedef void (*func_time_from_format)(struct pg_tm*, fsec_t*, char*, void*);

struct TmFormatConstraint {
    /*
     * valid range is [ min_val, max_val ], and make sure that:
     *      INT_MIN <= min_val < max_val <= INT_MAX
     * so here INT type is enough to hold min/max value.
     */
    int max_val;
    int min_val;

    /* is-next-separator flag, see is_next_separator() function */
    bool next_sep;
};

struct TimeFormatInfo {
    /* function pointer to either general_to_timestamp_from_user_format()
     * or optimized_to_timestamp_from_user_format().
     */
    func_time_from_format tm_func;

    /* FormatNode chain, whose max size is max_num */
    FormatNode* tm_format;

    /* more details about FormatNode from its chain,
     * whose max size is max_num
     */
    TmFormatConstraint* tm_constraint;
    int max_num;

    /* TmFromChar common info from FormatNode chain */
    FromCharDateMode mode;

    /* TmFromCharFlag info from FormatNode chain */
    TmFromCharFlag tm_flags;
};

#define ERROR_VALUE_MUST_BE_INTEGER(__value, __field)                        \
    ereport(ERROR,                                                           \
        (errcode(ERRCODE_INVALID_DATETIME_FORMAT),                           \
            errmsg("invalid value \"%s\" for \"%s\"", (__value), (__field)), \
            errdetail("Value must be an integer.")))

#define ERROR_FIELD_MUST_BE_FIXED(__value, __field, __req, __real)           \
    ereport(ERROR,                                                           \
        (errcode(ERRCODE_INVALID_DATETIME_FORMAT),                           \
            errmsg("invalid value \"%s\" for \"%s\"", (__value), (__field)), \
            errdetail("Field requires %d characters, but only %d "           \
                      "could be parsed.",                                    \
                (__req),                                                     \
                (__real)),                                                   \
            errhint("If your source string is not fixed-width, try "         \
                    "using the \"FM\" modifier.")))

#define ERROR_VALUE_MUST_BE_BETWEEN(__field, __min, __max)                          \
    ereport(ERROR,                                                                  \
        (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),                              \
            errmsg("value for \"%s\" in source string is out of range", (__field)), \
            errdetail("Value must be in the range %d to %d.", (__min), (__max))))

#define ERROR_SOURCE_STRING_TOO_SHORT(__field, __req, __real)                         \
    ereport(ERROR,                                                                    \
        (errcode(ERRCODE_INVALID_DATETIME_FORMAT),                                    \
            errmsg("source string too short for \"%s\" formatting field", (__field)), \
            errdetail("Field requires %d characters, but only %d "                    \
                      "remain.",                                                      \
                __req,                                                                \
                __real),                                                              \
            errhint("If your source string is not fixed-width, try "                  \
                    "using the \"FM\" modifier.")))

#define ERROR_FIELD_VALUE_HAS_BEEN_SET(__field)                                            \
    ereport(ERROR,                                                                         \
        (errcode(ERRCODE_INVALID_DATETIME_FORMAT),                                         \
            errmsg("conflicting values for \"%s\" field in formatting string", (__field)), \
            errdetail("This value contradicts a previous setting for "                     \
                      "the same field type.")))

#define ERROR_USFF_LENGTH_INVALID()                              \
    ereport(ERROR,                                               \
        (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),           \
            errmsg("input length of format 'US'/'us'/'FF'/'ff' " \
                   "must between 0 and 6")))

#define ERROR_NOT_SUPPORT_TZ()                   \
    ereport(ERROR,                               \
        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
            errmsg("\"TZ\"/\"tz\" format patterns are not supported in to_date")))

#define CHECK_RR_MUST_BE_BEWTEEN(__year)                                                                               \
    if ((__year) < 0 || (__year) > MAX_VALUE_YEAR) {                                                                   \
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg(" RR/RRRR should be between 0 - 9999"))); \
    }

static const int us_multi_factor[7] = {
    0,      /* case: no US digits */
    100000, /* case: US 1 digit */
    10000,  /* case: US 2 digits */
    1000,   /* case: US 3 digits */
    100,    /* case: US 4 digits */
    10,     /* case: US 5 digits */
    1       /* case: US 6 digits */
};

static const int ms_multi_factor[4] = {
    0,   /* case: no MS digits */
    100, /* case: MS 1 digit   */
    10,  /* case: MS 2 digits  */
    1    /* case: MS 3 digits  */
};

static FormatNode* get_format(char* fmt_str, int fmt_len, bool* in_cache);
template <bool optimized>
static int dch_parse_Y_YYY(const char* s, TmFromChar* out, FormatNode* node, bool iterance);
static bool optimized_dck_check(
    TmFromChar* datetime, TmFromCharFlag* datetime_flag, char* invalid_format, int* invalid_value, char* valid_range);

/* ----------
 * Functions
 * ----------
 */
static const KeyWord* index_seq_search(const char* str, const KeyWord* kw, const int* index);
static KeySuffix* suff_search(const char* str, KeySuffix* suf, int type);
static void NUMDesc_prepare(NUMDesc* num, FormatNode* n);
static void parse_format(
    FormatNode* node, char* str, const KeyWord* kw, KeySuffix* suf, const int* index, int ver, NUMDesc* Num);

static void DCH_to_char(FormatNode* node, bool is_interval, TmToChar* in, char* out, int buflen, Oid collid);

static void DCH_from_char(FormatNode* node, char* in, TmFromChar* out, bool* non_match, TmFromCharFlag* out_flag);
static bool DCH_check(TmFromChar* datetime, FormatNode* formatnode, TmFromCharFlag* datetime_flag, char* invalid_format,
    int* invalid_value, char* valid_range);

#ifdef DEBUG_TO_FROM_CHAR
static void dump_index(const KeyWord* k, const int* index);
static void dump_node(FormatNode* node, int max);
#endif

static char* get_th(char* num, int type);
static char* str_numth(char* dest, char* num, int type);
static int adjust_partial_year_to_2020(int year);
template <bool bef_check, bool aft_check>
static int adjust_partial_year_to_current_year(int year);
static int strspace_len(const char* str);
static void from_char_set_mode(TmFromChar* tmfc, const FromCharDateMode mode);
static void from_char_set_int(int* dest, const int value, const FormatNode* node, bool iterance);
static int from_char_parse_int_len(int* dest, char** src, const int len, FormatNode* node, bool iterance);
static int from_char_parse_int(int* dest, char** src, FormatNode* node, bool iterance);
static int seq_search(char* name, char** array, int type, int max, int* len);
static int from_char_seq_search(int* dest, char** src, char** array, int type, int max, FormatNode* node);
/*
 * exposed for bulkload datetime formatting.
 */
static void do_to_timestamp(text* date_txt, text* fmt, struct pg_tm* tm, fsec_t* fsec);
static char* fill_str(char* str, int c, int max);
static FormatNode* NUM_cache(int len, NUMDesc* Num, text* pars_str, bool* shouldFree);
static char* int_to_roman(int number);
static void NUM_prepare_locale(NUMProc* Np);
static char* get_last_relevant_decnum(char* num);
static void NUM_numpart_from_char(NUMProc* Np, int id, int plen, int& tmp_len);
static void NUM_numpart_to_char(NUMProc* Np, int id, int& tmp_len);
static char* NUM_processor(FormatNode* node, NUMDesc* Num, char* inout, char* number, int tmp_len, int plen, int sign,
    bool is_to_char, Oid collid);
void long_int_add(char* addend, const char* summand);

static DCHCacheEntry* DCH_cache_search(const char* str);
static DCHCacheEntry* DCH_cache_getnew(const char* str);

static NUMCacheEntry* NUM_cache_search(const char* str);
static NUMCacheEntry* NUM_cache_getnew(const char* str);
static void NUM_cache_remove(NUMCacheEntry* ent);

static char* str_toupper_locale_encode(const char* buff, size_t nbytes, Oid collid);
#ifdef USE_WIDE_UPPER_LOWER
static char* str_toupper_database_encode(const char* buff, size_t nbytes, Oid collid);
#endif
static char* str_toupper_c_encode(const char* buff, size_t nbytes);

/* ----------
 * Fast sequential search, use index for data selection which
 * go to seq. cycle (it is very fast for unwanted strings)
 * (can't be used binary search in format parsing)
 * ----------
 */
static const KeyWord* index_seq_search(const char* str, const KeyWord* kw, const int* index)
{
    int poz;

    if (!KeyWord_INDEX_FILTER(*str))
        return NULL;

    if ((poz = *(index + (*str - ' '))) > -1) {
        const KeyWord* k = kw + poz;

        do {
            if (strncmp(str, k->name, k->len) == 0)
                return k;
            k++;
            if (NULL == k->name)
                return NULL;
        } while (*str == *k->name);
    }
    return NULL;
}

static KeySuffix* suff_search(const char* str, KeySuffix* suf, int type)
{
    KeySuffix* s = NULL;

    for (s = suf; s->name != NULL; s++) {
        if (s->type != type)
            continue;

        if (strncmp(str, s->name, s->len) == 0)
            return s;
    }
    return NULL;
}

/* ----------
 * Prepare NUMDesc (number description struct) via FormatNode struct
 * ----------
 */
static void NUMDesc_prepare(NUMDesc* num, FormatNode* n)
{

    if (n->type != NODE_TYPE_ACTION)
        return;

    /*
     * In case of an error, we need to remove the numeric from the cache.  Use
     * a PG_TRY block to ensure that this happens.
     */
    PG_TRY();
    {
        if (IS_EEEE(num) && n->key->id != NUM_E)
            ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("\"EEEE\" must be the last pattern used")));

        switch (n->key->id) {
            case NUM_x:
                num->flag |= NUM_F_x16; /* lowercase 'x': format char 'x' */
                num->flag |= NUM_F_X16; /* uppercase 'X': format char 'X' */
                ++num->pre;
                break;
            case NUM_X:
                num->flag |= NUM_F_X16;
                ++num->pre;
                break;
            case NUM_9:
                if (IS_BRACKET(num))
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("\"9\" must be ahead of \"PR\"")));
                if (IS_MULTI(num)) {
                    ++num->multi;
                    break;
                }
                if (IS_DECIMAL(num))
                    ++num->post;
                else
                    ++num->pre;
                break;

            case NUM_0:
                if (IS_BRACKET(num))
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("\"0\" must be ahead of \"PR\"")));
                if (!IS_ZERO(num) && !IS_DECIMAL(num)) {
                    num->flag |= NUM_F_ZERO;
                    num->zero_start = num->pre + 1;
                }
                if (!IS_DECIMAL(num))
                    ++num->pre;
                else
                    ++num->post;

                num->zero_end = num->pre + num->post;
                break;

            case NUM_B:
                if (num->pre == 0 && num->post == 0 && (!IS_ZERO(num)))
                    num->flag |= NUM_F_BLANK;
                break;

            case NUM_D:
                num->flag |= NUM_F_LDECIMAL;
                num->need_locale = TRUE;
                /* FALLTHROUGH */
            case NUM_DEC:
                if (IS_DECIMAL(num))
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("multiple decimal points")));
                if (IS_MULTI(num))
                    ereport(
                        ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("cannot use \"V\" and decimal point together")));
                num->flag |= NUM_F_DECIMAL;
                break;

            case NUM_FM:
                num->flag |= NUM_F_FILLMODE;
                break;

            case NUM_S:
                if (IS_LSIGN(num))
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("cannot use \"S\" twice")));
                if (IS_PLUS(num) || IS_MINUS(num) || IS_BRACKET(num))
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("cannot use \"S\" and \"PL\"/\"MI\"/\"SG\"/\"PR\" together")));
                if (!IS_DECIMAL(num)) {
                    num->lsign = NUM_LSIGN_PRE;
                    num->pre_lsign_num = num->pre;
                    num->need_locale = TRUE;
                    num->flag |= NUM_F_LSIGN;
                } else if (num->lsign == NUM_LSIGN_NONE) {
                    num->lsign = NUM_LSIGN_POST;
                    num->need_locale = TRUE;
                    num->flag |= NUM_F_LSIGN;
                }
                break;

            case NUM_MI:
                if (IS_LSIGN(num))
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("cannot use \"S\" and \"MI\" together")));
                num->flag |= NUM_F_MINUS;
                if (IS_DECIMAL(num))
                    num->flag |= NUM_F_MINUS_POST;
                break;

            case NUM_PL:
                if (IS_LSIGN(num))
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("cannot use \"S\" and \"PL\" together")));
                num->flag |= NUM_F_PLUS;
                if (IS_DECIMAL(num))
                    num->flag |= NUM_F_PLUS_POST;
                break;

            case NUM_SG:
                if (IS_LSIGN(num))
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("cannot use \"S\" and \"SG\" together")));
                num->flag |= NUM_F_MINUS;
                num->flag |= NUM_F_PLUS;
                break;

            case NUM_PR:
                if (IS_LSIGN(num) || IS_PLUS(num) || IS_MINUS(num))
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("cannot use \"PR\" and \"S\"/\"PL\"/\"MI\"/\"SG\" together")));
                num->flag |= NUM_F_BRACKET;
                break;

            case NUM_rn:
            case NUM_RN:
                num->flag |= NUM_F_ROMAN;
                break;

            case NUM_L:
            case NUM_G:
                num->need_locale = TRUE;
                break;

            case NUM_V:
                if (IS_DECIMAL(num))
                    ereport(
                        ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("cannot use \"V\" and decimal point together")));
                num->flag |= NUM_F_MULTI;
                break;

            case NUM_E:
                if (IS_EEEE(num))
                    ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), errmsg("cannot use \"EEEE\" twice")));
                if (IS_BLANK(num) || IS_FILLMODE(num) || IS_LSIGN(num) || IS_BRACKET(num) || IS_MINUS(num) ||
                    IS_PLUS(num) || IS_ROMAN(num) || IS_MULTI(num))
                    ereport(ERROR,
                        (errcode(ERRCODE_SYNTAX_ERROR),
                            errmsg("\"EEEE\" is incompatible with other formats"),
                            errdetail("\"EEEE\" may only be used together with digit and decimal point patterns.")));
                num->flag |= NUM_F_EEEE;
                break;
            default:
                break;
        }
    }
    PG_CATCH();
    {
        NUM_cache_remove(t_thrd.format_cxt.last_NUM_cache_entry);
        PG_RE_THROW();
    }
    PG_END_TRY();

    return;
}

/* ----------
 * Format parser, search small keywords and keyword's suffixes, and make
 * format-node tree.
 *
 * for DATE-TIME & NUMBER version
 * ----------
 */
static void parse_format(
    FormatNode* node, char* str, const KeyWord* kw, KeySuffix* suf, const int* index, int ver, NUMDesc* Num)
{
    FormatNode* n = NULL;

    char* src_str = str;
    int str_len = strlen(src_str);
    DCHCacheEntry* ent = NULL;

#ifdef DEBUG_TO_FROM_CHAR
    elog(DEBUG_elog_output, "to_char/number(): run parser");
#endif

    n = node;

    while (*str) {
        int suffix = 0;
        const KeySuffix* s = NULL;
        /*
         * Prefix
         * overwrite to distinguish to char or to timestamp
         */
        if (((DCH_TO_CHAR_TYPE == ver) || (DCH_TO_TIMESTAMP_TYPE == ver)) && (NULL != suf) &&
            (s = suff_search(str, suf, SUFFTYPE_PREFIX)) != NULL) {
            suffix |= s->id;
            if (s->len)
                str += s->len;
        }

        /*
         * Keyword
         */
        if (*str && (n->key = index_seq_search(str, kw, index)) != NULL) {
            n->type = NODE_TYPE_ACTION;
            n->suffix = suffix;
            if (n->key->len)
                str += n->key->len;

            /*
             * NUM version: Prepare global NUMDesc struct
             */
            if (ver == NUM_TYPE)
                NUMDesc_prepare(Num, n);

            /*
             * Postfix
             * overwrite to distinguish to char or to timestamp
             */
            if (((DCH_TO_CHAR_TYPE == ver) || (DCH_TO_TIMESTAMP_TYPE == ver)) && *str && (NULL != suf) &&
                (s = suff_search(str, suf, SUFFTYPE_POSTFIX)) != NULL) {
                n->suffix |= s->id;
                if (s->len)
                    str += s->len;
            }
            n++;
        } else if (*str) {
            /*
             * Process double-quoted literal string, if any
             */
            if (*str == '"') {
                while (*(++str)) {
                    if (*str == '"') {
                        str++;
                        break;
                    }
                    if (*str == '\\' && *(str + 1))
                        str++;
                    n->type = NODE_TYPE_CHAR;
                    n->character = *str;
                    n->key = NULL;
                    n->suffix = 0;
                    n++;
                }
            } else {
                //
                // give the error report if  the format is wrong when the transfering
                // function is do_to_timestamp
                //
                if ((DCH_TO_TIMESTAMP_TYPE == ver) && (NULL != suf))
                    if ((((*str >= 'A' && *str <= 'Z') || (*str >= 'a' && *str <= 'z') ||
                             (*str >= '0' && *str <= '9')) &&
                            (NULL == suff_search(str, suf, SUFFTYPE_POSTFIX)) &&
                            (NULL == suff_search(str, suf, SUFFTYPE_PREFIX)) &&
                            (NULL == index_seq_search(str, kw, index)))) {
                        // free the cache if there is a invalid
                        //	letter in the format string
                        if (str_len > DCH_CACHE_SIZE) {
                            pfree_ext(node);
                            pfree_ext(src_str);
                        } else {
                            ent = DCH_cache_search(src_str);
                            if (NULL != ent) {
                                errno_t rc = EOK;
                                rc = memset_s(ent->format, DCH_CACHE_SIZE + 1, 0, DCH_CACHE_SIZE + 1);
                                securec_check(rc, "\0", "\0");
                                rc = memset_s(ent->str, DCH_CACHE_SIZE + 1, 0, DCH_CACHE_SIZE + 1);
                                securec_check(rc, "\0", "\0");

                                /*
                                 * assign the age appropriate value to make sure that the cache will
                                 * be invoked first if n_DCH_Counter > DCH_CACHE_FIELDS next time
                                 */
                                if (t_thrd.format_cxt.n_DCH_cache > DCH_CACHE_FIELDS)
                                    ent->age = INT_MAX - DCH_CACHE_FIELDS - 2;
                                else
                                    t_thrd.format_cxt.n_DCH_cache--;
                                pfree_ext(src_str);
                            }
                        }

                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_DATETIME_FORMAT),
                                errmsg("invalid data for match in format string")));
                    }
                /*
                 * Outside double-quoted strings, backslash is only special if
                 * it immediately precedes a double quote.
                 */
                if (*str == '\\' && *(str + 1) == '"')
                    str++;

                n->type = NODE_TYPE_CHAR;
                n->character = *str;
                n->key = NULL;
                n->suffix = 0;
                n++;
                str++;
            }
        }
    }

    n->type = NODE_TYPE_END;
    n->suffix = 0;
    return;
}

/* ----------
 * DEBUG: Dump the FormatNode Tree (debug)
 * ----------
 */
#ifdef DEBUG_TO_FROM_CHAR

#define DUMP_THth(_suf) (S_TH(_suf) ? "TH" : (S_th(_suf) ? "th" : " "))
#define DUMP_FM(_suf) (S_FM(_suf) ? "FM" : " ")

static void dump_node(FormatNode* node, int max)
{
    FormatNode* n = NULL;
    int a;

    elog(DEBUG_elog_output, "to_from-char(): DUMP FORMAT");

    for (a = 0, n = node; a <= max; n++, a++) {
        if (n->type == NODE_TYPE_ACTION)
            elog(DEBUG_elog_output,
                "%d:\t NODE_TYPE_ACTION '%s'\t(%s,%s)",
                a,
                n->key->name,
                DUMP_THth(n->suffix),
                DUMP_FM(n->suffix));
        else if (n->type == NODE_TYPE_CHAR)
            elog(DEBUG_elog_output, "%d:\t NODE_TYPE_CHAR '%c'", a, n->character);
        else if (n->type == NODE_TYPE_END) {
            elog(DEBUG_elog_output, "%d:\t NODE_TYPE_END", a);
            return;
        } else
            elog(DEBUG_elog_output, "%d:\t unknown NODE!", a);
    }
}
#endif /* DEBUG */

/*****************************************************************************
 *			Private utils
 *****************************************************************************/

/* ----------
 * Return ST/ND/RD/TH for simple (1..9) numbers
 * type --> 0 upper, 1 lower
 * ----------
 */
static char* get_th(char* num, int type)
{
    int len = strlen(num), last, seclast;

    last = *(num + (len - 1));
    if (!isdigit((unsigned char)last))
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION), errmsg("\"%s\" is not a number", num)));

    /*
     * All "teens" (<x>1[0-9]) get 'TH/th', while <x>[02-9][123] still get
     * 'ST/st', 'ND/nd', 'RD/rd', respectively
     */
    if ((len > 1) && ((seclast = num[len - 2]) == '1'))
        last = 0;

    switch (last) {
        case '1':
            if (type == TH_UPPER)
                return numTH[0];
            return numth[0];
        case '2':
            if (type == TH_UPPER)
                return numTH[1];
            return numth[1];
        case '3':
            if (type == TH_UPPER)
                return numTH[2];
            return numth[2];
        default:
            if (type == TH_UPPER)
                return numTH[3];
            return numth[3];
    }
    return NULL;
}

/* ----------
 * Convert string-number to ordinal string-number
 * type --> 0 upper, 1 lower
 * ----------
 */
static char* str_numth(char* dest, char* num, int type)
{
    int rc = 0;
    if (dest != num) {
        rc = strcpy_s(dest, strlen(dest) + 1, num);
        securec_check(rc, "\0", "\0");
    }
    char* src = get_th(num, type);
    rc = strcat_s(dest, strlen(dest) + strlen(src) + 1, src);
    securec_check(rc, "\0", "\0");
    return dest;
}

/*****************************************************************************
 *			upper/lower/initcap functions
 *****************************************************************************/

/*
 * If the system provides the needed functions for wide-character manipulation
 * (which are all standardized by C99), then we implement upper/lower/initcap
 * using wide-character functions, if necessary.  Otherwise we use the
 * traditional <ctype.h> functions, which of course will not work as desired
 * in multibyte character sets.  Note that in either case we are effectively
 * assuming that the database character encoding matches the encoding implied
 * by LC_CTYPE.
 *
 * If the system provides locale_t and associated functions (which are
 * standardized by Open Group's XBD), we can support collations that are
 * neither default nor C.  The code is written to handle both combinations
 * of have-wide-characters and have-locale_t, though it's rather unlikely
 * a platform would have the latter without the former.
 */

/*
 * collation-aware, wide-character-aware lower function
 *
 * We pass the number of bytes so we can pass varlena and char*
 * to this function.  The result is a palloc'd, null-terminated string.
 */
char* str_tolower(const char* buff, size_t nbytes, Oid collid)
{
    char* result = NULL;

    if (NULL == buff)
        return NULL;

    /* C/POSIX collations use this path regardless of database encoding */
    if (lc_ctype_is_c(collid) || COLLATION_IN_B_FORMAT(collid)) {
        char* p = NULL;

        result = pnstrdup(buff, nbytes);

        for (p = result; *p; p++)
            *p = pg_ascii_tolower((unsigned char)*p);
    }
#ifdef USE_WIDE_UPPER_LOWER
    else if (pg_database_encoding_max_length() > 1) {
        pg_locale_t mylocale = 0;
        wchar_t* workspace = NULL;
        size_t curr_char;
        size_t result_size;

        if (collid != DEFAULT_COLLATION_OID) {
            if (!OidIsValid(collid)) {
                /*
                 * This typically means that the parser could not resolve a
                 * conflict of implicit collations, so report it that way.
                 */
                ereport(ERROR,
                    (errcode(ERRCODE_INDETERMINATE_COLLATION),
                        errmsg("could not determine which collation to use for lower() function"),
                        errhint("Use the COLLATE clause to set the collation explicitly.")));
            }
            mylocale = pg_newlocale_from_collation(collid);
        }

        /* Overflow paranoia */
        if ((nbytes + 1) > (INT_MAX / sizeof(wchar_t)))
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));

        /* Output workspace cannot have more codes than input bytes */
        workspace = (wchar_t*)palloc((nbytes + 1) * sizeof(wchar_t));

        char2wchar(workspace, nbytes + 1, buff, nbytes, mylocale);

        for (curr_char = 0; workspace[curr_char] != 0; curr_char++) {
#ifdef HAVE_LOCALE_T
            if (mylocale)
                workspace[curr_char] = towlower_l(workspace[curr_char], mylocale);
            else
#endif
                workspace[curr_char] = towlower(workspace[curr_char]);
        }

        /* Make result large enough; case change might change number of bytes */
        result_size = curr_char * pg_database_encoding_max_length() + 1;
        result = (char*)palloc(result_size);

        wchar2char(result, workspace, result_size, mylocale);
        pfree_ext(workspace);
    }
#endif /* USE_WIDE_UPPER_LOWER */
    else {
#ifdef HAVE_LOCALE_T
        pg_locale_t mylocale = 0;
#endif
        char* p = NULL;

        if (collid != DEFAULT_COLLATION_OID) {
            if (!OidIsValid(collid)) {
                /*
                 * This typically means that the parser could not resolve a
                 * conflict of implicit collations, so report it that way.
                 */
                ereport(ERROR,
                    (errcode(ERRCODE_INDETERMINATE_COLLATION),
                        errmsg("could not determine which collation to use for lower() function"),
                        errhint("Use the COLLATE clause to set the collation explicitly.")));
            }
#ifdef HAVE_LOCALE_T
            mylocale = pg_newlocale_from_collation(collid);
#endif
        }

        result = pnstrdup(buff, nbytes);

        /*
         * Note: we assume that tolower_l() will not be so broken as to need
         * an isupper_l() guard test.  When using the default collation, we
         * apply the traditional openGauss behavior that forces ASCII-style
         * treatment of I/i, but in non-default collations you get exactly
         * what the collation says.
         */
        for (p = result; *p; p++) {
#ifdef HAVE_LOCALE_T
            if (mylocale)
                *p = tolower_l((unsigned char)*p, mylocale);
            else
#endif
                *p = pg_tolower((unsigned char)*p);
        }
    }

    return result;
}

/*
 * collation-aware, wide-character-aware upper function
 *
 * We pass the number of bytes so we can pass varlena and char*
 * to this function.  The result is a palloc'd, null-terminated string.
 */
char* str_toupper(const char* buff, size_t nbytes, Oid collid)
{
    char* result = NULL;

    if (NULL == buff)
        return NULL;

    /* C/POSIX collations use this path regardless of database encoding */
    if (lc_ctype_is_c(collid) || COLLATION_IN_B_FORMAT(collid)) {
        result = str_toupper_c_encode(buff, nbytes);
    }
#ifdef USE_WIDE_UPPER_LOWER
    else if (pg_database_encoding_max_length() > 1) {
        result = str_toupper_database_encode(buff, nbytes, collid);
    }
#endif /* USE_WIDE_UPPER_LOWER */
    else {
        result = str_toupper_locale_encode(buff, nbytes, collid);
    }

    return result;
}

/*
 * str_toupper for rawout, no need to trans to wchar_t for database encode
 */
char* str_toupper_for_raw(const char* buff, size_t nbytes, Oid collid)
{
    char* result = NULL;
    if (NULL == buff)
        return NULL;

    /* C/POSIX collations use this path regardless of database encoding */
    if (lc_ctype_is_c(collid) || COLLATION_IN_B_FORMAT(collid)) {
        result = str_toupper_c_encode(buff, nbytes);
    } else {
        result = str_toupper_locale_encode(buff, nbytes, collid);
    }

    return result;
}

static char* str_toupper_c_encode(const char* buff, size_t nbytes)
{
    char* p = NULL;
    char* result = pnstrdup(buff, nbytes);

    for (p = result; *p; p++)
        *p = pg_ascii_toupper((unsigned char)*p);
    return result;
}

#ifdef USE_WIDE_UPPER_LOWER
static char* str_toupper_database_encode(const char* buff, size_t nbytes, Oid collid)
{
    pg_locale_t mylocale = 0;
    wchar_t* workspace = NULL;
    size_t curr_char;
    size_t result_size;
    char* result = NULL;

    if (collid != DEFAULT_COLLATION_OID) {
        if (!OidIsValid(collid)) {
            /*
             * This typically means that the parser could not resolve a
             * conflict of implicit collations, so report it that way.
             */
            ereport(ERROR,
                (errcode(ERRCODE_INDETERMINATE_COLLATION),
                    errmsg("could not determine which collation to use for upper() function"),
                    errhint("Use the COLLATE clause to set the collation explicitly.")));
        }
        mylocale = pg_newlocale_from_collation(collid);
    }

    /* Overflow paranoia */
    if ((nbytes + 1) > (INT_MAX / sizeof(wchar_t)))
        ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));

    /* Output workspace cannot have more codes than input bytes */
    workspace = (wchar_t*)palloc((nbytes + 1) * sizeof(wchar_t));

    char2wchar(workspace, nbytes + 1, buff, nbytes, mylocale);

    for (curr_char = 0; workspace[curr_char] != 0; curr_char++) {
#ifdef HAVE_LOCALE_T
        if (mylocale)
            workspace[curr_char] = towupper_l(workspace[curr_char], mylocale);
        else
#endif
            workspace[curr_char] = towupper(workspace[curr_char]);
    }

    /* Make result large enough; case change might change number of bytes */
    result_size = curr_char * pg_database_encoding_max_length() + 1;
    result = (char*)palloc(result_size);

    wchar2char(result, workspace, result_size, mylocale);
    pfree_ext(workspace);
    return result;
}
#endif /* USE_WIDE_UPPER_LOWER */

static char* str_toupper_locale_encode(const char* buff, size_t nbytes, Oid collid)
{
#ifdef HAVE_LOCALE_T
        pg_locale_t mylocale = 0;
#endif
        char* p = NULL;

        if (collid != DEFAULT_COLLATION_OID) {
            if (!OidIsValid(collid)) {
                /*
                 * This typically means that the parser could not resolve a
                 * conflict of implicit collations, so report it that way.
                 */
                ereport(ERROR,
                    (errcode(ERRCODE_INDETERMINATE_COLLATION),
                        errmsg("could not determine which collation to use for upper() function"),
                        errhint("Use the COLLATE clause to set the collation explicitly.")));
            }
#ifdef HAVE_LOCALE_T
            mylocale = pg_newlocale_from_collation(collid);
#endif
        }

        char *result = pnstrdup(buff, nbytes);

        /*
         * Note: we assume that toupper_l() will not be so broken as to need
         * an islower_l() guard test.  When using the default collation, we
         * apply the traditional openGauss behavior that forces ASCII-style
         * treatment of I/i, but in non-default collations you get exactly
         * what the collation says.
         */
        for (p = result; *p; p++) {
#ifdef HAVE_LOCALE_T
            if (mylocale)
                *p = toupper_l((unsigned char)*p, mylocale);
            else
#endif
                *p = pg_toupper((unsigned char)*p);
        }
    return result;
}

/*
 * collation-aware, wide-character-aware initcap function
 *
 * We pass the number of bytes so we can pass varlena and char*
 * to this function.  The result is a palloc'd, null-terminated string.
 */
char* str_initcap(const char* buff, size_t nbytes, Oid collid)
{
    char* result = NULL;
    int wasalnum = false;

    if (NULL == buff)
        return NULL;

    /* C/POSIX collations use this path regardless of database encoding */
    if (lc_ctype_is_c(collid) || COLLATION_IN_B_FORMAT(collid)) {
        char* p = NULL;

        result = pnstrdup(buff, nbytes);

        for (p = result; *p; p++) {
            char c;

            if (wasalnum)
                *p = c = pg_ascii_tolower((unsigned char)*p);
            else
                *p = c = pg_ascii_toupper((unsigned char)*p);
            /* we don't trust isalnum() here */
            wasalnum = ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'));
        }
    }
#ifdef USE_WIDE_UPPER_LOWER
    else if (pg_database_encoding_max_length() > 1) {
        pg_locale_t mylocale = 0;
        wchar_t* workspace = NULL;
        size_t curr_char;
        size_t result_size;

        if (collid != DEFAULT_COLLATION_OID) {
            if (!OidIsValid(collid)) {
                /*
                 * This typically means that the parser could not resolve a
                 * conflict of implicit collations, so report it that way.
                 */
                ereport(ERROR,
                    (errcode(ERRCODE_INDETERMINATE_COLLATION),
                        errmsg("could not determine which collation to use for initcap() function"),
                        errhint("Use the COLLATE clause to set the collation explicitly.")));
            }
            mylocale = pg_newlocale_from_collation(collid);
        }

        /* Overflow paranoia */
        if ((nbytes + 1) > (INT_MAX / sizeof(wchar_t)))
            ereport(ERROR, (errcode(ERRCODE_OUT_OF_MEMORY), errmsg("out of memory")));

        /* Output workspace cannot have more codes than input bytes */
        workspace = (wchar_t*)palloc((nbytes + 1) * sizeof(wchar_t));

        char2wchar(workspace, nbytes + 1, buff, nbytes, mylocale);

        for (curr_char = 0; workspace[curr_char] != 0; curr_char++) {
#ifdef HAVE_LOCALE_T
            if (mylocale) {
                if (wasalnum)
                    workspace[curr_char] = towlower_l(workspace[curr_char], mylocale);
                else
                    workspace[curr_char] = towupper_l(workspace[curr_char], mylocale);
                wasalnum = iswalnum_l(workspace[curr_char], mylocale);
            } else
#endif
            {
                if (wasalnum)
                    workspace[curr_char] = towlower(workspace[curr_char]);
                else
                    workspace[curr_char] = towupper(workspace[curr_char]);
                wasalnum = iswalnum(workspace[curr_char]);
            }
        }

        /* Make result large enough; case change might change number of bytes */
        result_size = curr_char * pg_database_encoding_max_length() + 1;
        result = (char*)palloc(result_size);

        wchar2char(result, workspace, result_size, mylocale);
        pfree_ext(workspace);
    }
#endif /* USE_WIDE_UPPER_LOWER */
    else {
#ifdef HAVE_LOCALE_T
        pg_locale_t mylocale = 0;
#endif
        char* p = NULL;

        if (collid != DEFAULT_COLLATION_OID) {
            if (!OidIsValid(collid)) {
                /*
                 * This typically means that the parser could not resolve a
                 * conflict of implicit collations, so report it that way.
                 */
                ereport(ERROR,
                    (errcode(ERRCODE_INDETERMINATE_COLLATION),
                        errmsg("could not determine which collation to use for initcap() function"),
                        errhint("Use the COLLATE clause to set the collation explicitly.")));
            }
#ifdef HAVE_LOCALE_T
            mylocale = pg_newlocale_from_collation(collid);
#endif
        }

        result = pnstrdup(buff, nbytes);

        /*
         * Note: we assume that toupper_l()/tolower_l() will not be so broken
         * as to need guard tests.	When using the default collation, we apply
         * the traditional openGauss behavior that forces ASCII-style treatment
         * of I/i, but in non-default collations you get exactly what the
         * collation says.
         */
        for (p = result; *p; p++) {
#ifdef HAVE_LOCALE_T
            if (mylocale) {
                if (wasalnum)
                    *p = tolower_l((unsigned char)*p, mylocale);
                else
                    *p = toupper_l((unsigned char)*p, mylocale);
                wasalnum = isalnum_l((unsigned char)*p, mylocale);
            } else
#endif
            {
                if (wasalnum)
                    *p = pg_tolower((unsigned char)*p);
                else
                    *p = pg_toupper((unsigned char)*p);
                wasalnum = isalnum((unsigned char)*p);
            }
        }
    }

    return result;
}

/* convenience routines for when the input is null-terminated */

static char* str_tolower_z(const char* buff, Oid collid)
{
    return str_tolower(buff, strlen(buff), collid);
}

static char* str_toupper_z(const char* buff, Oid collid)
{
    return str_toupper(buff, strlen(buff), collid);
}

static char* str_initcap_z(const char* buff, Oid collid)
{
    return str_initcap(buff, strlen(buff), collid);
}

/* ----------
 * Skip TM / th in FROM_CHAR
 *
 * If S_THth is on, skip two chars, assuming there are two available
 * ----------
 */
#define SKIP_THth(ptr, _suf) \
    do {                     \
        if (S_THth(_suf)) {  \
            if (*(ptr))      \
                (ptr)++;     \
            if (*(ptr))      \
                (ptr)++;     \
        }                    \
    } while (0)

#ifdef DEBUG_TO_FROM_CHAR
/* -----------
 * DEBUG: Call for debug and for index checking; (Show ASCII char
 * and defined keyword for each used position
 * ----------
 */
static void dump_index(const KeyWord* k, const int* index)
{
    int i, count = 0, free_i = 0;

    elog(DEBUG_elog_output, "TO-FROM_CHAR: Dump KeyWord Index:");

    for (i = 0; i < KeyWord_INDEX_SIZE; i++) {
        if (index[i] != -1) {
            elog(DEBUG_elog_output, "\t%c: %s, ", i + 32, k[index[i]].name);
            count++;
        } else {
            free_i++;
            elog(DEBUG_elog_output, "\t(%d) %c %d", i, i + 32, index[i]);
        }
    }
    elog(DEBUG_elog_output, "\n\t\tUsed positions: %d,\n\t\tFree positions: %d", count, free_i);
}
#endif /* DEBUG */

/* ----------
 * Return TRUE if next format picture is not digit value
 * ----------
 */
static bool is_next_separator(FormatNode* n)
{
    if (n->type == NODE_TYPE_END)
        return FALSE;

    if (n->type == NODE_TYPE_ACTION && S_THth(n->suffix))
        return TRUE;

    /*
     * Next node
     */
    n++;

    /* end of format string is treated like a non-digit separator */
    if (n->type == NODE_TYPE_END)
        return TRUE;

    if (n->type == NODE_TYPE_ACTION) {
        if (n->key->is_digit)
            return FALSE;

        return TRUE;
    } else if (isdigit((unsigned char)n->character))
        return FALSE;

    return TRUE; /* some non-digit input (separator) */
}

static int adjust_partial_year_to_2020(int year)
{
    /*
     * Adjust all dates toward 2020; this is effectively what happens when we
     * assume '70' is 1970 and '69' is 2069.
     */
    /* Force 0-69 into the 2000's */
    if (year < 70)
        return year + 2000;
    /* Force 70-99 into the 1900's */
    else if (year < 100)
        return year + 1900;
    /* Force 100-519 into the 2000's */
    else if (year < 520)
        return year + 2000;
    /* Force 520-999 into the 1000's */
    else if (year < 1000)
        return year + 1000;
    else
        return year;
}

/*
 * @Description: Adjust all dates toward current year
 * @IN year: input number about year to adjust
 * @IN bef_check: check input year value from input number string.
 *                if caller doesn't check this value, set bef_check be true.
 *                if caller has checked this value, set bef_check be false.
 * @IN aft_check: check output year value.
 *                if caller doesn't check this value, set aft_check be true.
 *                if caller has checked this value, set aft_check be false.
 * @Return: adjusted year value
 * @See also: from_char_parse_int() and optimized_parse_int().
 */
template <bool bef_check, bool aft_check>
static int adjust_partial_year_to_current_year(int year)
{
    struct pg_tm tm;
    int century = 0;
    int last_two_digits = -1;
    int result = -9999;

    GetCurrentDateTime(&tm);

    Assert(tm.tm_year > 100);

    if (bef_check) {
        CHECK_RR_MUST_BE_BEWTEEN(year);
    }

    century = tm.tm_year / 100;
    last_two_digits = tm.tm_year % 100;

    if (MIN_VALUE_RR <= year && year <= MID_VALUE_RR) {
        if (last_two_digits <= MID_VALUE_RR && last_two_digits >= MIN_VALUE_RR)
            result = century * 100 + year;
        else if (last_two_digits <= MAX_VALUE_RR && last_two_digits > MID_VALUE_RR)
            result = (century + 1) * 100 + year;

        if (aft_check) {
            CHECK_RR_MUST_BE_BEWTEEN(result);
        }
    } else if (MID_VALUE_RR < year && year <= MAX_VALUE_RR) {
        if (last_two_digits <= MID_VALUE_RR && last_two_digits >= MIN_VALUE_RR)
            result = (century - 1) * 100 + year;
        else if (last_two_digits <= MAX_VALUE_RR && last_two_digits > MID_VALUE_RR)
            result = century * 100 + year;

        if (aft_check) {
            CHECK_RR_MUST_BE_BEWTEEN(result);
        }
    }

    return result;
}

static int strspace_len(const char* str)
{
    int len = 0;

    while (*str && isspace((unsigned char)*str)) {
        str++;
        len++;
    }
    return len;
}

/*
 * Set the date mode of a from-char conversion.
 *
 * Puke if the date mode has already been set, and the caller attempts to set
 * it to a conflicting mode.
 */
static void from_char_set_mode(TmFromChar* tmfc, const FromCharDateMode mode)
{
    if (mode != FROM_CHAR_DATE_NONE) {
        if (tmfc->mode == FROM_CHAR_DATE_NONE)
            tmfc->mode = mode;
        else if (tmfc->mode != mode)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_DATETIME_FORMAT),
                    errmsg("invalid combination of date conventions"),
                    errhint("Do not mix Gregorian and ISO week date "
                            "conventions in a formatting template.")));
    }
}

/*
 * Set the integer pointed to by 'dest' to the given value.
 *
 * Puke if the destination integer has previously been set to some other
 * non-zero value.
 */
static void from_char_set_int(int* dest, const int value, const FormatNode* node, bool iterance)
{
    if (iterance && *dest != value) {
        ERROR_FIELD_VALUE_HAS_BEEN_SET(node->key->name);
    }
    *dest = value;
}

/* Check if all the source string are digits.*/
static bool if_all_digits(char* src)
{
    if (NULL == src)
        return false;
    char* tmp = src;

    while (*tmp) {
        if (*tmp < '0' || *tmp > '9')
            return false;
        tmp++;
    }
    return true;
}

/* ----------
 * Return TRUE if next format picture is END
 * ----------
 */
static bool is_next_separator_end(FormatNode* n)
{
    if (n->type == NODE_TYPE_END)
        return FALSE;

    if (n->type == NODE_TYPE_ACTION && S_THth(n->suffix))
        return TRUE;

    /*
     * Next node
     */
    n++;

    /* end of format string is treated like a non-digit separator */
    if (n->type == NODE_TYPE_END)
        return TRUE;

    return FALSE;
}

/*
 * Read a single integer from the source string, into the int pointed to by
 * 'dest'. If 'dest' is NULL, the result is discarded.
 *
 * In fixed-width mode (the node does not have the FM suffix), consume at most
 * 'len' characters.  However, any leading whitespace isn't counted in 'len'.
 *
 * We use strtol() to recover the integer value from the source string, in
 * accordance with the given FormatNode.
 *
 * If the conversion completes successfully, src will have been advanced to
 * point at the character immediately following the last character used in the
 * conversion.
 *
 * Return the number of characters consumed.
 *
 * Note that from_char_parse_int() provides a more convenient wrapper where
 * the length of the field is the same as the length of the format keyword (as
 * with DD and MI).
 */
static int from_char_parse_int_len(int* dest, char** src, const int len, FormatNode* node, bool iterance)
{
    long result;
    char copy[DCH_MAX_ITEM_SIZ + 1];
    char* init = *src;
    int used;
    int tmp_errno = 0;

    /*
     * Skip any whitespace before parsing the integer.
     */
    *src += strspace_len(*src);

    Assert(len <= DCH_MAX_ITEM_SIZ);
    used = (int)strlcpy(copy, *src, len + 1);

    /*
     * sophisticated logic explain: S_FM means accept 'fm' titled format so that
     *  the length of the date is flexible. is_next_separator() checks if the node
     *  comes to a separator or an end. is_next_separator_end() only checks if the
     *  format comes to an end. all_digits checks if the date string is composed
     *  all by digits.
     *  Thus, if an all digits date comes in,(is_next_separator(node) && !all_digits)
     *  can lead the process to the else part to match the date format one by one.
     *  (is_next_separator(node) && (used < len) checks if the date is too short
     *  to match the format, if so, accept it in Fill Mode.
     */
    if (S_FM(node->suffix) || /* accept 'fm' titled format for flexible length */
        (is_next_separator(node) && !t_thrd.format_cxt.all_digits) || /* the date string has non-digit character(s) */
        is_next_separator_end(node) ||                                /* the format comes to an end */
        (is_next_separator(node) && (used < len))) /* if the date string is too short to match the format */
    {
        /*
         * This node is in Fill Mode, or the next node is known to be a
         * non-digit value, so we just slurp as many characters as we can get.
         */
        errno = 0;
        result = strtol(init, src, 10);
        tmp_errno = errno;
    } else {
        /*
         * We need to pull exactly the number of characters given in 'len' out
         * of the string, and convert those.
         */
        char* last = NULL;

        if (used < len) {
            ERROR_SOURCE_STRING_TOO_SHORT(node->key->name, len, used);
        }

        errno = 0;
        result = strtol(copy, &last, 10);
        tmp_errno = errno;
        used = last - copy;

        if (used > 0 && used < len) {
            ERROR_FIELD_MUST_BE_FIXED(copy, node->key->name, len, used);
        }

        *src += used;
    }

    if (*src == init) {
        ERROR_VALUE_MUST_BE_INTEGER(copy, node->key->name);
    }
    if (tmp_errno == ERANGE || result < INT_MIN || result > INT_MAX) {
        ERROR_VALUE_MUST_BE_BETWEEN(node->key->name, INT_MIN, INT_MAX);
    }

    if (dest != NULL)
        from_char_set_int(dest, (int)result, node, iterance);
    return *src - init;
}

/*
 * Call from_char_parse_int_len(), using the length of the format keyword as
 * the expected length of the field.
 *
 * Don't call this function if the field differs in length from the format
 * keyword (as with HH24; the keyword length is 4, but the field length is 2).
 * In such cases, call from_char_parse_int_len() instead to specify the
 * required length explicitly.
 */
static int from_char_parse_int(int* dest, char** src, FormatNode* node, bool iterance)
{
    return from_char_parse_int_len(dest, src, node->key->len, node, iterance);
}

/* ----------
 * Sequential search with to upper/lower conversion
 * ----------
 */
static int seq_search(char* name, char** array, int type, int max, int* len)
{
    char *p = NULL, *n = NULL, **a = NULL;
    int last, i;

    *len = 0;

    if (!*name)
        return -1;

    /* set first char */
    if (type == ONE_UPPER || type == ALL_UPPER)
        *name = pg_toupper((unsigned char)*name);
    else if (type == ALL_LOWER)
        *name = pg_tolower((unsigned char)*name);

    for (last = 0, a = array; *a != NULL; a++) {
        /* comperate first chars */
        if (*name != **a)
            continue;

        for (i = 1, p = *a + 1, n = name + 1;; n++, p++, i++) {
            /* search fragment (max) only */
            if (max && i == max) {
                *len = i;
                return a - array;
            }
            /* full size */
            if (*p == '\0') {
                *len = i;
                return a - array;
            }
            /* Not found in array 'a' */
            if (*n == '\0')
                break;

            /*
             * Convert (but convert new chars only)
             */
            if (i > last) {
                if (type == ONE_UPPER || type == ALL_LOWER)
                    *n = pg_tolower((unsigned char)*n);
                else if (type == ALL_UPPER)
                    *n = pg_toupper((unsigned char)*n);
                last = i;
            }

#ifdef DEBUG_TO_FROM_CHAR
            elog(DEBUG_elog_output, "N: %c, P: %c, A: %s (%s)", *n, *p, *a, name);
#endif
            if (*n != *p)
                break;
        }
    }

    return -1;
}

/*
 * Perform a sequential search in 'array' for text matching the first 'max'
 * characters of the source string.
 *
 * If a match is found, copy the array index of the match into the integer
 * pointed to by 'dest', advance 'src' to the end of the part of the string
 * which matched, and return the number of characters consumed.
 *
 * If the string doesn't match, throw an error.
 */
static int from_char_seq_search(int* dest, char** src, char** array, int type, int max, FormatNode* node)
{
    int len;

    *dest = seq_search(*src, array, type, max, &len);
    if (len <= 0) {
        char copy[DCH_MAX_ITEM_SIZ + 1];

        Assert(max <= DCH_MAX_ITEM_SIZ);
        strlcpy(copy, *src, max + 1);

        ereport(ERROR,
            (errcode(ERRCODE_INVALID_DATETIME_FORMAT),
                errmsg("invalid value \"%s\" for \"%s\"", copy, node->key->name),
                errdetail("The given value did not match any of the allowed "
                          "values for this field.")));
    }
    *src += len;
    return len;
}

#define CopyLocalizedString(dst, src)                                                                              \
    do {                                                                                                           \
        int m_len = (n->key->len + TM_SUFFIX_LEN) * DCH_MAX_ITEM_SIZ;                                              \
        if ((int)strlen(src) < m_len) {                                                                            \
            errno_t ret;                                                                                           \
            ret = strcpy_s(dst, m_len, src);                                                                       \
            securec_check(ret, "\0", "\0");                                                                        \
        } else                                                                                                     \
            ereport(ERROR,                                                                                         \
                (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("localized string format value too long"))); \
    } while (0)

/* ----------
 * Process a TmToChar struct as denoted by a list of FormatNodes.
 * The formatted data is written to the string pointed to by 'out'.
 * ----------
 */
static void DCH_to_char(FormatNode* node, bool is_interval, TmToChar* in, char* out, int buflen, Oid collid)
{
    FormatNode* n = NULL;
    char* s = NULL;
    struct pg_tm* tm = &in->tm;
    int i;
    int len;
    errno_t rc = EOK;

    /* cache localized days and months */
    cache_locale_time();

    s = out;
    for (n = node; n->type != NODE_TYPE_END; n++) {
        if (n->type != NODE_TYPE_ACTION) {
            *s = n->character;
            s++;
            continue;
        }

        len = buflen - (s - out);
        switch (n->key->id) {
            case DCH_A_M:
            case DCH_P_M:
                rc = strcpy_s(s, len, (tm->tm_hour % HOURS_PER_DAY >= HOURS_PER_DAY / 2) ? P_M_STR : A_M_STR);
                securec_check(rc, "\0", "\0");
                s += strlen(s);
                break;
            case DCH_AM:
            case DCH_PM:
                rc = strcpy_s(s, len, (tm->tm_hour % HOURS_PER_DAY >= HOURS_PER_DAY / 2) ? PM_STR : AM_STR);
                securec_check(rc, "\0", "\0");
                s += strlen(s);
                break;
            case DCH_a_m:
            case DCH_p_m:
                rc = strcpy_s(s, len, (tm->tm_hour % HOURS_PER_DAY >= HOURS_PER_DAY / 2) ? p_m_STR : a_m_STR);
                securec_check(rc, "\0", "\0");
                s += strlen(s);
                break;
            case DCH_am:
            case DCH_pm:
                rc = strcpy_s(s, len, (tm->tm_hour % HOURS_PER_DAY >= HOURS_PER_DAY / 2) ? pm_STR : am_STR);
                securec_check(rc, "\0", "\0");
                s += strlen(s);
                break;
            case DCH_HH:
            case DCH_HH12:

                /*
                 * display time as shown on a 12-hour clock, even for
                 * intervals
                 */
                rc = sprintf_s(s,
                    len,
                    "%0*d",
                    S_FM(n->suffix) ? 0 : 2,
                    (tm->tm_hour % (HOURS_PER_DAY / 2) == 0) ? (HOURS_PER_DAY / 2) : (tm->tm_hour % (HOURS_PER_DAY / 2)));
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_HH24:
                rc = sprintf_s(s, len, "%0*d", S_FM(n->suffix) ? 0 : 2, tm->tm_hour);
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_MI:
                rc = sprintf_s(s, len, "%0*d", S_FM(n->suffix) ? 0 : 2, tm->tm_min);
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_SS:
                rc = sprintf_s(s, len, "%0*d", S_FM(n->suffix) ? 0 : 2, tm->tm_sec);
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_MS: /* millisecond */
            case DCH_FF3:
#ifdef HAVE_INT64_TIMESTAMP
                rc = sprintf_s(s, len, "%03d", (int)(in->fsec / INT64CONST(1000)));
#else
                /* No rint() because we can't overflow and we might print US */
                rc = sprintf_s(s, len, "%03d", (int)(in->fsec * 1000));
#endif
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_FF1: /* millisecond */
#ifdef HAVE_INT64_TIMESTAMP
                rc = sprintf_s(s, len, "%01d", (int)(in->fsec / INT64CONST(100000)));
#else
                rc = sprintf_s(s, len, "%01d", (int)(in->fsec * 1000000 / 100000));
#endif
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_FF2: /* millisecond */
#ifdef HAVE_INT64_TIMESTAMP
                rc = sprintf_s(s, len, "%02d", (int)(in->fsec / INT64CONST(10000)));
#else
                rc = sprintf_s(s, len, "%02d", (int)(in->fsec * 1000000 / 10000));
#endif
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_FF4: /* millisecond */
#ifdef HAVE_INT64_TIMESTAMP
                rc = sprintf_s(s, len, "%04d", (int)(in->fsec / INT64CONST(100)));
#else
                rc = sprintf_s(s, len, "%04d", (int)(in->fsec * 1000000 / 100));
#endif
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_FF5: /* millisecond */
#ifdef HAVE_INT64_TIMESTAMP
                rc = sprintf_s(s, len, "%05d", (int)(in->fsec / INT64CONST(10)));
#else
                rc = sprintf_s(s, len, "%05d", (int)(in->fsec * 1000000 / 10));
#endif
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_US: /* microsecond */
            case DCH_FF6:
#ifdef HAVE_INT64_TIMESTAMP
                rc = sprintf_s(s, len, "%06d", (int)in->fsec);
#else
                /* don't use rint() because we can't overflow 1000 */
                rc = sprintf_s(s, len, "%06d", (int)(in->fsec * 1000000));
#endif
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_SSSSS:
                rc = sprintf_s(s, len, "%d", tm->tm_hour * SECS_PER_HOUR + tm->tm_min * SECS_PER_MINUTE + tm->tm_sec);
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_tz:
                INVALID_FOR_INTERVAL;
                if (tmtcTzn(in)) {
                    char* p = str_tolower_z(tmtcTzn(in), collid);

                    rc = strcpy_s(s, len, p);
                    securec_check(rc, "\0", "\0");
                    pfree_ext(p);
                    s += strlen(s);
                }
                break;
            case DCH_TZ:
                INVALID_FOR_INTERVAL;
                if (tmtcTzn(in)) {
                    rc = strcpy_s(s, len, tmtcTzn(in));
                    securec_check(rc, "\0", "\0");
                    s += strlen(s);
                }
                break;
            case DCH_A_D:
            case DCH_B_C:
                INVALID_FOR_INTERVAL;
                rc = strcpy_s(s, len, ((tm->tm_year <= 0) ? B_C_STR : A_D_STR));
                securec_check(rc, "\0", "\0");
                s += strlen(s);
                break;
            case DCH_AD:
            case DCH_BC:
                INVALID_FOR_INTERVAL;
                rc = strcpy_s(s, len, ((tm->tm_year <= 0) ? BC_STR : AD_STR));
                securec_check(rc, "\0", "\0");
                s += strlen(s);
                break;
            case DCH_a_d:
            case DCH_b_c:
                INVALID_FOR_INTERVAL;
                rc = strcpy_s(s, len, ((tm->tm_year <= 0) ? b_c_STR : a_d_STR));
                securec_check(rc, "\0", "\0");
                s += strlen(s);
                break;
            case DCH_ad:
            case DCH_bc:
                INVALID_FOR_INTERVAL;
                rc = strcpy_s(s, len, ((tm->tm_year <= 0) ? bc_STR : ad_STR));
                securec_check(rc, "\0", "\0");
                s += strlen(s);
                break;
            case DCH_MONTH:
                INVALID_FOR_INTERVAL;
                if (!tm->tm_mon)
                    break;
                if (S_TM(n->suffix)) {
                    char* str = str_toupper_z(u_sess->lc_cxt.localized_full_months[tm->tm_mon - 1], collid);
                    CopyLocalizedString(s, str);
                } else {
                    rc = sprintf_s(
                        s, len, "%*s", S_FM(n->suffix) ? 0 : -9, str_toupper_z(months_full[tm->tm_mon - 1], collid));
                    securec_check_ss(rc, "\0", "\0");
                }
                s += strlen(s);
                break;
            case DCH_Month:
                INVALID_FOR_INTERVAL;
                if (!tm->tm_mon)
                    break;
                if (S_TM(n->suffix)) {
                    char* str = str_initcap_z(u_sess->lc_cxt.localized_full_months[tm->tm_mon - 1], collid);
                    CopyLocalizedString(s, str);
                } else {
                    rc = sprintf_s(s, len, "%*s", S_FM(n->suffix) ? 0 : -9, months_full[tm->tm_mon - 1]);
                    securec_check_ss(rc, "\0", "\0");
                }
                s += strlen(s);
                break;
            case DCH_month:
                INVALID_FOR_INTERVAL;
                if (!tm->tm_mon)
                    break;
                if (S_TM(n->suffix)) {
                    char* str = str_tolower_z(u_sess->lc_cxt.localized_full_months[tm->tm_mon - 1], collid);
                    CopyLocalizedString(s, str);
                } else {
                    rc = sprintf_s(s, len, "%*s", S_FM(n->suffix) ? 0 : -9, months_full[tm->tm_mon - 1]);
                    securec_check_ss(rc, "\0", "\0");
                    *s = pg_tolower((unsigned char)*s);
                }
                s += strlen(s);
                break;
            case DCH_MON:
                INVALID_FOR_INTERVAL;
                if (!tm->tm_mon)
                    break;
                if (S_TM(n->suffix)) {
                    char* str = str_toupper_z(u_sess->lc_cxt.localized_abbrev_months[tm->tm_mon - 1], collid);
                    CopyLocalizedString(s, str);
                } else {
                    rc = strcpy_s(s, len, str_toupper_z(months[tm->tm_mon - 1], collid));
                    securec_check(rc, "\0", "\0");
                }
                s += strlen(s);
                break;
            case DCH_Mon:
                INVALID_FOR_INTERVAL;
                if (!tm->tm_mon)
                    break;
                if (S_TM(n->suffix)) {
                    char* str = str_initcap_z(u_sess->lc_cxt.localized_abbrev_months[tm->tm_mon - 1], collid);
                    CopyLocalizedString(s, str);
                } else {
                    rc = strcpy_s(s, len, months[tm->tm_mon - 1]);
                    securec_check(rc, "\0", "\0");
                }
                s += strlen(s);
                break;
            case DCH_mon:
                INVALID_FOR_INTERVAL;
                if (!tm->tm_mon)
                    break;
                if (S_TM(n->suffix)) {
                    char* str = str_tolower_z(u_sess->lc_cxt.localized_abbrev_months[tm->tm_mon - 1], collid);
                    CopyLocalizedString(s, str);
                } else {
                    rc = strcpy_s(s, len, months[tm->tm_mon - 1]);
                    securec_check(rc, "\0", "\0");
                    *s = pg_tolower((unsigned char)*s);
                }
                s += strlen(s);
                break;
            case DCH_MM:
                rc = sprintf_s(s, len, "%0*d", S_FM(n->suffix) ? 0 : 2, tm->tm_mon);
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_DAY:
                INVALID_FOR_INTERVAL;
                if (S_TM(n->suffix)) {
                    char* str = str_toupper_z(u_sess->lc_cxt.localized_full_days[tm->tm_wday], collid);
                    CopyLocalizedString(s, str);
                } else {
                    rc = sprintf_s(s, len, "%*s", S_FM(n->suffix) ? 0 : -9, str_toupper_z(days[tm->tm_wday], collid));
                    securec_check_ss(rc, "\0", "\0");
                }
                s += strlen(s);
                break;
            case DCH_Day:
                INVALID_FOR_INTERVAL;
                if (S_TM(n->suffix)) {
                    char* str = str_initcap_z(u_sess->lc_cxt.localized_full_days[tm->tm_wday], collid);
                    CopyLocalizedString(s, str);
                } else {
                    rc = sprintf_s(s, len, "%*s", S_FM(n->suffix) ? 0 : -9, days[tm->tm_wday]);
                    securec_check_ss(rc, "\0", "\0");
                }
                s += strlen(s);
                break;
            case DCH_day:
                INVALID_FOR_INTERVAL;
                if (S_TM(n->suffix)) {
                    char* str = str_tolower_z(u_sess->lc_cxt.localized_full_days[tm->tm_wday], collid);
                    CopyLocalizedString(s, str);
                } else {
                    rc = sprintf_s(s, len, "%*s", S_FM(n->suffix) ? 0 : -9, days[tm->tm_wday]);
                    securec_check_ss(rc, "\0", "\0");
                    *s = pg_tolower((unsigned char)*s);
                }
                s += strlen(s);
                break;
            case DCH_DY:
                INVALID_FOR_INTERVAL;
                if (S_TM(n->suffix)) {
                    char* str = str_toupper_z(u_sess->lc_cxt.localized_abbrev_days[tm->tm_wday], collid);
                    CopyLocalizedString(s, str);
                } else {
                    rc = strcpy_s(s, len, str_toupper_z(days_short[tm->tm_wday], collid));
                    securec_check(rc, "\0", "\0");
                }
                s += strlen(s);
                break;
            case DCH_Dy:
                INVALID_FOR_INTERVAL;
                if (S_TM(n->suffix)) {
                    char* str = str_initcap_z(u_sess->lc_cxt.localized_abbrev_days[tm->tm_wday], collid);
                    CopyLocalizedString(s, str);
                } else {
                    rc = strcpy_s(s, len, days_short[tm->tm_wday]);
                    securec_check(rc, "\0", "\0");
                }
                s += strlen(s);
                break;
            case DCH_dy:
                INVALID_FOR_INTERVAL;
                if (S_TM(n->suffix)) {
                    char* str = str_tolower_z(u_sess->lc_cxt.localized_abbrev_days[tm->tm_wday], collid);
                    CopyLocalizedString(s, str);
                } else {
                    rc = strcpy_s(s, len, days_short[tm->tm_wday]);
                    securec_check(rc, "\0", "\0");
                    *s = pg_tolower((unsigned char)*s);
                }
                s += strlen(s);
                break;
            case DCH_DDD:
            case DCH_IDDD:
                rc = sprintf_s(s,
                    len,
                    "%0*d",
                    S_FM(n->suffix) ? 0 : 3,
                    (n->key->id == DCH_DDD) ? tm->tm_yday : date2isoyearday(tm->tm_year, tm->tm_mon, tm->tm_mday));
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_DD:
                rc = sprintf_s(s, len, "%0*d", S_FM(n->suffix) ? 0 : 2, tm->tm_mday);
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_D:
                INVALID_FOR_INTERVAL;
                rc = sprintf_s(s, len, "%d", tm->tm_wday + 1);
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_ID:
                INVALID_FOR_INTERVAL;
                rc = sprintf_s(s, len, "%d", (tm->tm_wday == 0) ? 7 : tm->tm_wday);
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_WW:
                rc = sprintf_s(s, len, "%0*d", S_FM(n->suffix) ? 0 : 2, (tm->tm_yday - 1) / 7 + 1);
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_IW:
                rc = sprintf_s(
                    s, len, "%0*d", S_FM(n->suffix) ? 0 : 2, date2isoweek(tm->tm_year, tm->tm_mon, tm->tm_mday));
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_Q:
                if (!tm->tm_mon)
                    break;
                rc = sprintf_s(s, len, "%d", (tm->tm_mon - 1) / 3 + 1);
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_CC:
                if (is_interval) /* straight calculation */
                    i = tm->tm_year / 100;
                else /* century 21 starts in 2001 */
                    i = (tm->tm_year - 1) / 100 + 1;
                if (i <= 99 && i >= -99)
                    rc = sprintf_s(s, len, "%0*d", S_FM(n->suffix) ? 0 : 2, i);
                else
                    rc = sprintf_s(s, len, "%d", i);
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_Y_YYY:
                i = ADJUST_YEAR(tm->tm_year, is_interval) / 1000;
                rc = sprintf_s(s, len, "%d,%03d", i, ADJUST_YEAR(tm->tm_year, is_interval) - (i * 1000));
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_YYYY:
            case DCH_IYYY:
                rc = sprintf_s(s,
                    len,
                    "%0*d",
                    S_FM(n->suffix) ? 0 : 4,
                    ((n->key->id == DCH_YYYY)
                            ? ADJUST_YEAR(tm->tm_year, is_interval)
                            : ADJUST_YEAR(date2isoyear(tm->tm_year, tm->tm_mon, tm->tm_mday), is_interval)));
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_YYY:
            case DCH_IYY:
                rc = sprintf_s(s,
                    len,
                    "%0*d",
                    S_FM(n->suffix) ? 0 : 3,
                    ((n->key->id == DCH_YYY)
                            ? ADJUST_YEAR(tm->tm_year, is_interval)
                            : ADJUST_YEAR(date2isoyear(tm->tm_year, tm->tm_mon, tm->tm_mday), is_interval)) %
                        1000);
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_YY:
            case DCH_IY:
                rc = sprintf_s(s,
                    len,
                    "%0*d",
                    S_FM(n->suffix) ? 0 : 2,
                    ((n->key->id == DCH_YY)
                            ? ADJUST_YEAR(tm->tm_year, is_interval)
                            : ADJUST_YEAR(date2isoyear(tm->tm_year, tm->tm_mon, tm->tm_mday), is_interval)) %
                        100);
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_Y:
            case DCH_I:
                rc = sprintf_s(s,
                    len,
                    "%1d",
                    ((n->key->id == DCH_Y)
                            ? ADJUST_YEAR(tm->tm_year, is_interval)
                            : ADJUST_YEAR(date2isoyear(tm->tm_year, tm->tm_mon, tm->tm_mday), is_interval)) %
                        10);
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_RM:
            case DCH_rm:
                /*
                * For intervals, values like '12 month' will be reduced to 0
                * month and some years.  These should be processed.
                */
                if (!tm->tm_mon && !tm->tm_year) {
                    break;
                } else {
                    int mon = 0;
                    char** months = NULL;

                    if (n->key->id == DCH_RM) {
                        months = rm_months_upper;
                    } else {
                        months = rm_months_lower;
                    }

                    /*
                    * Compute the position in the roman-numeral array.  Note
                    * that the contents of the array are reversed, December
                    * being first and January last.
                    */
                    if (tm->tm_mon == 0) {
                        /*
                        * This case is special, and tracks the case of full
                        * interval years.
                        */
                        mon = tm->tm_year >= 0 ? 0 : MONTHS_PER_YEAR - 1;
                    } else if (tm->tm_mon < 0) {
                        /*
                        * Negative case.  In this case, the calculation is
                        * reversed, where -1 means December, -2 November,
                        * etc.
                        */
                        mon = -1 * (tm->tm_mon + 1);
                    } else {
                        /*
                        * Common case, with a strictly positive value.  The
                        * position in the array matches with the value of
                        * tm_mon.
                        */
                        mon = MONTHS_PER_YEAR - tm->tm_mon;
                    }

                    rc = sprintf_s(s, len, "%*s", S_FM(n->suffix) ? 0 : -4, months[mon]);
                    securec_check_ss(rc, "\0", "\0");
                    s += strlen(s);
                    break;
                }
            case DCH_W:
                rc = sprintf_s(s, len, "%d", (tm->tm_mday - 1) / 7 + 1);
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            case DCH_X:
                /*
                * Convert 'X' and 'x' to '.' when "sql_compatibility == A_FORMAT".
                */
                if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT) {
                    rc = strcpy_s(s, len, ".");
                    securec_check(rc, "\0", "\0");
                    s += strlen(s);
                }
                break;
            case DCH_J:
                rc = sprintf_s(s, len, "%d", date2j(tm->tm_year, tm->tm_mon, tm->tm_mday));
                securec_check_ss(rc, "\0", "\0");
                if (S_THth(n->suffix))
                    str_numth(s, s, S_TH_TYPE(n->suffix));
                s += strlen(s);
                break;
            default:
                break;
        }
    }

    *s = '\0';
}

/* ----------
 * Process a string as denoted by a list of FormatNodes.
 * The TmFromChar struct pointed to by 'out' is populated with the results.
 *
 * Note: we currently don't have any to_interval() function, so there
 * is no need here for INVALID_FOR_INTERVAL checks.
 * ----------
 */
static void DCH_from_char(FormatNode* node, char* in, TmFromChar* out, bool* non_match, TmFromCharFlag* out_flag)
{
    FormatNode* n = NULL;
    char* s = NULL;
    int len, value;
    bool fx_mode = false;

    t_thrd.format_cxt.all_digits = if_all_digits(in);

    for (n = node, s = in; n->type != NODE_TYPE_END && *s != '\0'; n++) {
        if (n->type != NODE_TYPE_ACTION) {
            /*
             * Separator, so consume one character from input string. When
             * the input string's format is not the same as the requiered format,
             * report an error.
             */
            if (*s != n->character) {
                ereport(DEBUG1,
                    (errcode(ERRCODE_INVALID_DATETIME_FORMAT), errmsg("character does not match format string")));
                if (*s >= '0' && *s <= '9')
                    continue;
            }
            s++;
            continue;
        }

        /* Ignore spaces before fields when not in FX (fixed width) mode */
        if (!fx_mode && n->key->id != DCH_FX) {
            while (*s != '\0' && isspace((unsigned char)*s))
                s++;
        }

        from_char_set_mode(out, n->key->date_mode);

        switch (n->key->id) {
            case DCH_FX:
                fx_mode = true;
                break;
            case DCH_A_M:
            case DCH_P_M:
            case DCH_a_m:
            case DCH_p_m:
                (void)from_char_seq_search(&value, &s, ampm_strings_long, ALL_UPPER, n->key->len, n);
                from_char_set_int(&out->pm, value % 2, n, out_flag->pm_flag);
                out->clock = CLOCK_12_HOUR;
                out_flag->clock_flag = true;
                out_flag->pm_flag = true;
                break;
            case DCH_AM:
            case DCH_PM:
            case DCH_am:
            case DCH_pm:
                (void)from_char_seq_search(&value, &s, ampm_strings, ALL_UPPER, n->key->len, n);
                from_char_set_int(&out->pm, value % 2, n, out_flag->pm_flag);
                out->clock = CLOCK_12_HOUR;
                out_flag->clock_flag = true;
                out_flag->pm_flag = true;
                break;
            case DCH_HH:
            case DCH_HH12:
                from_char_parse_int_len(&out->hh, &s, 2, n, out_flag->hh_flag);
                out->clock = CLOCK_12_HOUR;
                SKIP_THth(s, n->suffix);
                out_flag->clock_flag = true;
                out_flag->hh_flag = true;
                break;
            case DCH_HH24:
                from_char_parse_int_len(&out->hh, &s, 2, n, out_flag->hh_flag);
                SKIP_THth(s, n->suffix);
                out_flag->clock_flag = true;
                out_flag->hh_flag = true;
                break;
            case DCH_MI:
                from_char_parse_int(&out->mi, &s, n, out_flag->mi_flag);
                SKIP_THth(s, n->suffix);
                out_flag->mi_flag = true;
                break;
            case DCH_SS:
                from_char_parse_int(&out->ss, &s, n, out_flag->ss_flag);
                SKIP_THth(s, n->suffix);
                out_flag->ss_flag = true;
                break;
            case DCH_MS: /* millisecond */
                len = from_char_parse_int_len(&out->ms, &s, 3, n, out_flag->ms_flag);

                /*
                 * 25 is 0.25 and 250 is 0.25 too; 025 is 0.025 and not 0.25
                 */
                out->ms *= ms_multi_factor[Min(len, 3)];
                SKIP_THth(s, n->suffix);
                out_flag->ms_flag = true;
                break;
            case DCH_US: /* microsecond */
                len = from_char_parse_int_len(&out->us, &s, 6, n, out_flag->us_flag);
                if (len > 6) {
                    ERROR_USFF_LENGTH_INVALID();
                }
                out->us *= us_multi_factor[len];
                SKIP_THth(s, n->suffix);
                out_flag->us_flag = true;
                break;
            case DCH_SSSSS:
                from_char_parse_int(&out->sssss, &s, n, out_flag->sssss_flag);
                SKIP_THth(s, n->suffix);
                out_flag->sssss_flag = true;
                break;
            case DCH_tz:
            case DCH_TZ:
                ERROR_NOT_SUPPORT_TZ();
                break;
            case DCH_A_D:
            case DCH_B_C:
            case DCH_a_d:
            case DCH_b_c:
                (void)from_char_seq_search(&value, &s, adbc_strings_long, ALL_UPPER, n->key->len, n);
                from_char_set_int(&out->bc, value % 2, n, out_flag->bc_flag);
                out_flag->bc_flag = true;
                break;
            case DCH_AD:
            case DCH_BC:
            case DCH_ad:
            case DCH_bc:
                (void)from_char_seq_search(&value, &s, adbc_strings, ALL_UPPER, n->key->len, n);
                from_char_set_int(&out->bc, value % 2, n, out_flag->bc_flag);
                out_flag->bc_flag = true;
                break;
            case DCH_MONTH:
            case DCH_Month:
            case DCH_month:
                (void)from_char_seq_search(&value, &s, months_full, ONE_UPPER, MAX_MONTH_LEN, n);
                from_char_set_int(&out->mm, value + 1, n, out_flag->mm_flag);
                out_flag->mm_flag = true;
                break;
            case DCH_MON:
            case DCH_Mon:
            case DCH_mon:
                (void)from_char_seq_search(&value, &s, months, ONE_UPPER, MAX_MON_LEN, n);
                from_char_set_int(&out->mm, value + 1, n, out_flag->mm_flag);
                out_flag->mm_flag = true;
                break;
            case DCH_MM:
                from_char_parse_int(&out->mm, &s, n, out_flag->mm_flag);
                SKIP_THth(s, n->suffix);
                out_flag->mm_flag = true;
                break;
            case DCH_DAY:
            case DCH_Day:
            case DCH_day:
                (void)from_char_seq_search(&value, &s, days, ONE_UPPER, MAX_DAY_LEN, n);
                from_char_set_int(&out->d, value, n, out_flag->d_flag);
                out_flag->d_flag = true;
                break;
            case DCH_DY:
            case DCH_Dy:
            case DCH_dy:
                (void)from_char_seq_search(&value, &s, days, ONE_UPPER, MAX_DY_LEN, n);
                from_char_set_int(&out->d, value, n, out_flag->d_flag);
                out_flag->d_flag = true;
                break;
            case DCH_DDD:
                from_char_parse_int(&out->ddd, &s, n, out_flag->ddd_flag);
                SKIP_THth(s, n->suffix);
                out_flag->ddd_flag = true;
                break;
            case DCH_IDDD:
                from_char_parse_int_len(&out->ddd, &s, 3, n, out_flag->ddd_flag);
                SKIP_THth(s, n->suffix);
                out_flag->ddd_flag = true;
                break;
            case DCH_DD:
                from_char_parse_int(&out->dd, &s, n, out_flag->dd_flag);
                SKIP_THth(s, n->suffix);
                out_flag->dd_flag = true;
                break;
            case DCH_D:
                from_char_parse_int(&out->d, &s, n, out_flag->d_flag);
                out->d--;
                SKIP_THth(s, n->suffix);
                out_flag->d_flag = true;
                break;
            case DCH_ID:
                from_char_parse_int_len(&out->d, &s, 1, n, out_flag->d_flag);
                SKIP_THth(s, n->suffix);
                out_flag->d_flag = true;
                break;
            case DCH_WW:
            case DCH_IW:
                from_char_parse_int(&out->ww, &s, n, out_flag->ww_flag);
                SKIP_THth(s, n->suffix);
                out_flag->ww_flag = true;
                break;
            case DCH_Q:

                /*
                 * We ignore 'Q' when converting to date because it is unclear
                 * which date in the quarter to use, and some people specify
                 * both quarter and month, so if it was honored it might
                 * conflict with the supplied month. That is also why we don't
                 * throw an error.
                 *
                 * We still parse the source string for an integer, but it
                 * isn't stored anywhere in 'out'.
                 */
                from_char_parse_int((int*)NULL, &s, n, false);
                SKIP_THth(s, n->suffix);
                break;
            case DCH_CC:
                from_char_parse_int(&out->cc, &s, n, out_flag->cc_flag);
                SKIP_THth(s, n->suffix);
                out_flag->cc_flag = true;
                break;
            case DCH_Y_YYY:
                s += dch_parse_Y_YYY<false>(s, out, n, out_flag->year_flag);
                SKIP_THth(s, n->suffix);
                out_flag->year_flag = true;
                out_flag->yysz_flag = true;
                break;
            case DCH_YYYY:
            case DCH_IYYY:
                from_char_parse_int(&out->year, &s, n, out_flag->year_flag);
                out->yysz = 4;
                SKIP_THth(s, n->suffix);
                out_flag->year_flag = true;
                out_flag->yysz_flag = true;
                break;
            case DCH_YYY:
            case DCH_IYY:
                if (from_char_parse_int(&out->year, &s, n, out_flag->year_flag) < 4)
                    out->year = adjust_partial_year_to_2020(out->year);
                out->yysz = 3;
                SKIP_THth(s, n->suffix);
                out_flag->year_flag = true;
                out_flag->yysz_flag = true;
                break;
            case DCH_YY:
            case DCH_IY:
                if (from_char_parse_int(&out->year, &s, n, out_flag->year_flag) < 4)
                    out->year = adjust_partial_year_to_2020(out->year);
                out->yysz = 2;
                SKIP_THth(s, n->suffix);
                out_flag->year_flag = true;
                out_flag->yysz_flag = true;
                break;
            case DCH_Y:
            case DCH_I:
                if (from_char_parse_int(&out->year, &s, n, out_flag->year_flag) < 4)
                    out->year = adjust_partial_year_to_2020(out->year);
                out->yysz = 1;
                SKIP_THth(s, n->suffix);
                out_flag->year_flag = true;
                out_flag->yysz_flag = true;
                break;
            case DCH_RM:
                (void)from_char_seq_search(&value, &s, rm_months_upper, ALL_UPPER, MAX_RM_LEN, n);
                from_char_set_int(&out->mm, MONTHS_PER_YEAR - value, n, out_flag->mm_flag);
                out_flag->mm_flag = true;
                break;
            case DCH_rm:
                (void)from_char_seq_search(&value, &s, rm_months_lower, ALL_LOWER, MAX_RM_LEN, n);
                from_char_set_int(&out->mm, MONTHS_PER_YEAR - value, n, out_flag->mm_flag);
                out_flag->mm_flag = true;
                break;
            case DCH_W:
                from_char_parse_int(&out->w, &s, n, out_flag->w_flag);
                SKIP_THth(s, n->suffix);
                out_flag->w_flag = true;
                break;
            case DCH_J:
                from_char_parse_int(&out->j, &s, n, out_flag->j_flag);
                SKIP_THth(s, n->suffix);
                out_flag->j_flag = true;
                break;
            // to adapt FF in A db. there is no digit after FF ,we specify 6 acquiescently
            case DCH_FF:
                len = from_char_parse_int_len(&out->us, &s, 6, n, out_flag->us_flag);
                out->us *= us_multi_factor[Min(len, 6)];
                SKIP_THth(s, n->suffix);
                out_flag->us_flag = true;
                break;
            // to adapt RRRR in A db
            case DCH_RRRR:
                if (from_char_parse_int(&out->year, &s, n, out_flag->year_flag) < 3) {
                    out->year = adjust_partial_year_to_current_year<true, false>(out->year);
                }
                out->yysz = 4;
                SKIP_THth(s, n->suffix);
                out_flag->year_flag = true;
                out_flag->yysz_flag = true;
                break;
            case DCH_RR:
                if (from_char_parse_int(&out->year, &s, n, out_flag->year_flag) < 3) {
                    out->year = adjust_partial_year_to_current_year<true, false>(out->year);
                }
                out->yysz = 2;
                SKIP_THth(s, n->suffix);
                out_flag->year_flag = true;
                out_flag->yysz_flag = true;
                break;
            case DCH_SYYYY:
                from_char_parse_int(&out->year, &s, n, out_flag->year_flag);
                out->yysz = 4;
                SKIP_THth(s, n->suffix);
                out_flag->year_flag = true;
                out_flag->yysz_flag = true;
                break;
            default:
                break;
        }
    }

    if (*s)
        *non_match = true;
    else
        *non_match = false;
}

static DCHCacheEntry* DCH_cache_getnew(const char* str)
{
    DCHCacheEntry* ent = NULL;

    /* counter overflow check - paranoia? */
    if (t_thrd.format_cxt.DCH_counter >= (INT_MAX - DCH_CACHE_FIELDS - 1)) {
        t_thrd.format_cxt.DCH_counter = 0;

        for (ent = t_thrd.format_cxt.DCH_cache; ent <= (t_thrd.format_cxt.DCH_cache + DCH_CACHE_FIELDS); ent++)
            ent->age = (++t_thrd.format_cxt.DCH_counter);
    }

    /*
     * If cache is full, remove oldest entry
     */
    if (t_thrd.format_cxt.n_DCH_cache > DCH_CACHE_FIELDS) {
        DCHCacheEntry* old = t_thrd.format_cxt.DCH_cache + 0;

#ifdef DEBUG_TO_FROM_CHAR
        elog(DEBUG_elog_output, "cache is full (%d)", t_thrd.format_cxt.n_DCH_cache);
#endif
        for (ent = t_thrd.format_cxt.DCH_cache + 1; ent <= (t_thrd.format_cxt.DCH_cache + DCH_CACHE_FIELDS); ent++) {
            if (ent->age < old->age)
                old = ent;
        }
#ifdef DEBUG_TO_FROM_CHAR
        elog(DEBUG_elog_output, "OLD: '%s' AGE: %d", old->str, old->age);
#endif
        errno_t errorno = EOK;
        errorno = strncpy_s(old->str, DCH_CACHE_SIZE + 1, str, DCH_CACHE_SIZE);
        securec_check(errorno, "\0", "\0");
        /* old->format fill parser */
        old->age = (++t_thrd.format_cxt.DCH_counter);
        return old;
    } else {
#ifdef DEBUG_TO_FROM_CHAR
        elog(DEBUG_elog_output, "NEW (%d)", t_thrd.format_cxt.n_DCH_cache);
#endif
        ent = t_thrd.format_cxt.DCH_cache + t_thrd.format_cxt.n_DCH_cache;
        errno_t errorno = EOK;
        errorno = strncpy_s(ent->str, DCH_CACHE_SIZE + 1, str, DCH_CACHE_SIZE);
        securec_check(errorno, "\0", "\0");
        /* ent->format fill parser */
        ent->age = (++t_thrd.format_cxt.DCH_counter);
        ++t_thrd.format_cxt.n_DCH_cache;
        return ent;
    }
}

static DCHCacheEntry* DCH_cache_search(const char* str)
{
    int i;
    DCHCacheEntry* ent = NULL;

    /* counter overflow check - paranoia? */
    if (t_thrd.format_cxt.DCH_counter >= (INT_MAX - DCH_CACHE_FIELDS - 1)) {
        t_thrd.format_cxt.DCH_counter = 0;

        for (ent = t_thrd.format_cxt.DCH_cache; ent <= (t_thrd.format_cxt.DCH_cache + DCH_CACHE_FIELDS); ent++)
            ent->age = (++t_thrd.format_cxt.DCH_counter);
    }

    for (i = 0, ent = t_thrd.format_cxt.DCH_cache; i < t_thrd.format_cxt.n_DCH_cache; i++, ent++) {
        if (strcmp(ent->str, str) == 0) {
            ent->age = (++t_thrd.format_cxt.DCH_counter);
            return ent;
        }
    }

    return NULL;
}

/*
 * Format a date/time or interval into a string according to fmt.
 * We parse fmt into a list of FormatNodes.  This is then passed to DCH_to_char
 * for formatting.
 */
text* datetime_to_char_body(TmToChar* tmtc, text* fmt, bool is_interval, Oid collid)
{
    FormatNode* format = NULL;
    char *fmt_str = NULL, *result = NULL;
    bool incache = false;
    int fmt_len;
    text* res = NULL;

    /*
     * Convert fmt to C string
     */
    fmt_str = text_to_cstring(fmt);
    fmt_len = strlen(fmt_str);

    /*
     * Allocate workspace for result as C string
     */
    result = (char*)palloc((fmt_len * DCH_MAX_ITEM_SIZ) + 1);
    *result = '\0';

    /*
     * Allocate new memory if format picture is bigger than static cache and
     * not use cache (call parser always)
     */
    if (fmt_len > DCH_CACHE_SIZE) {
        format = (FormatNode*)palloc((fmt_len + 1) * sizeof(FormatNode));
        incache = FALSE;

        parse_format(format, fmt_str, DCH_keywords, DCH_suff, DCH_index, DCH_TO_CHAR_TYPE, NULL);

        (format + fmt_len)->type = NODE_TYPE_END; /* Paranoia? */
    } else {
        /*
         * Use cache buffers
         */
        DCHCacheEntry* ent = NULL;

        incache = TRUE;

        if ((ent = DCH_cache_search(fmt_str)) == NULL) {
            ent = DCH_cache_getnew(fmt_str);

            /*
             * Not in the cache, must run parser and save a new format-picture
             * to the cache.
             */

            parse_format(ent->format, fmt_str, DCH_keywords, DCH_suff, DCH_index, DCH_TO_CHAR_TYPE, NULL);

            (ent->format + fmt_len)->type = NODE_TYPE_END; /* Paranoia? */
        }
        format = ent->format;
    }

    /* The real work is here */
    DCH_to_char(format, is_interval, tmtc, result, (fmt_len * DCH_MAX_ITEM_SIZ) + 1, collid);

    if (!incache)
        pfree_ext(format);

    pfree_ext(fmt_str);

    /* convert C-string result to TEXT format */
    res = cstring_to_text(result);

    pfree_ext(result);
    return res;
}

/****************************************************************************
 *				Public routines
 ***************************************************************************/

/* -------------------
 * TIMESTAMP to_char()
 * -------------------
 */
Datum timestamp_to_char(PG_FUNCTION_ARGS)
{
    Timestamp dt = PG_GETARG_TIMESTAMP(0);
    text *fmt = PG_GETARG_TEXT_P(1), *res = NULL;
    TmToChar tmtc;
    struct pg_tm* tm = NULL;
    int thisdate;

    if ((VARSIZE(fmt) - VARHDRSZ) <= 0 || TIMESTAMP_NOT_FINITE(dt))
        PG_RETURN_NULL();

    ZERO_tmtc(&tmtc);
    tm = tmtcTm(&tmtc);

    if (timestamp2tm(dt, NULL, tm, &tmtcFsec(&tmtc), NULL, NULL) != 0)
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("timestamp out of range")));

    thisdate = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday);
    tm->tm_wday = (thisdate + 1) % 7;
    tm->tm_yday = thisdate - date2j(tm->tm_year, 1, 1) + 1;

    if (!(res = datetime_to_char_body(&tmtc, fmt, false, PG_GET_COLLATION())))
        PG_RETURN_NULL();

    PG_RETURN_TEXT_P(res);
}

Datum timestamptz_to_char(PG_FUNCTION_ARGS)
{
    TimestampTz dt = PG_GETARG_TIMESTAMP(0);
    text *fmt = PG_GETARG_TEXT_P(1), *res = NULL;
    TmToChar tmtc;
    int tz;
    struct pg_tm* tm = NULL;
    int thisdate;

    if ((VARSIZE(fmt) - VARHDRSZ) <= 0 || TIMESTAMP_NOT_FINITE(dt))
        PG_RETURN_NULL();

    ZERO_tmtc(&tmtc);
    tm = tmtcTm(&tmtc);

    if (timestamp2tm(dt, &tz, tm, &tmtcFsec(&tmtc), &tmtcTzn(&tmtc), NULL) != 0)
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("timestamp out of range")));

    thisdate = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday);
    tm->tm_wday = (thisdate + 1) % 7;
    tm->tm_yday = thisdate - date2j(tm->tm_year, 1, 1) + 1;

    if (!(res = datetime_to_char_body(&tmtc, fmt, false, PG_GET_COLLATION())))
        PG_RETURN_NULL();

    PG_RETURN_TEXT_P(res);
}

// to_char(timestamp) with u_sess->time_cxt.DateStyle as default format param
Datum timestamp_to_char_default_format(PG_FUNCTION_ARGS)
{
    char* buf = NULL;
    text* res = NULL;

    if (!OidIsValid(fcinfo->fncollation))
        fcinfo->fncollation = DEFAULT_COLLATION_OID;
    buf = (char*)DirectFunctionCall1Coll(timestamp_out, PG_GET_COLLATION(), PG_GETARG_DATUM(0));
    res = cstring_to_text(buf);

    PG_RETURN_TEXT_P(res);
}

// to_char(timestamp) with u_sess->time_cxt.DateStyle as default format param
Datum timestamptz_to_char_default_format(PG_FUNCTION_ARGS)
{
    char* buf = NULL;
    text* res = NULL;

    if (!OidIsValid(fcinfo->fncollation))
        fcinfo->fncollation = DEFAULT_COLLATION_OID;
    buf = (char*)DirectFunctionCall1Coll(timestamptz_out, PG_GET_COLLATION(), PG_GETARG_DATUM(0));
    res = cstring_to_text(buf);

    PG_RETURN_TEXT_P(res);
}

/* -------------------
 * INTERVAL to_char()
 * -------------------
 */
Datum interval_to_char(PG_FUNCTION_ARGS)
{
    Interval* it = PG_GETARG_INTERVAL_P(0);
    text *fmt = PG_GETARG_TEXT_P(1), *res = NULL;
    TmToChar tmtc;
    struct pg_tm* tm = NULL;

    if ((VARSIZE(fmt) - VARHDRSZ) <= 0)
        PG_RETURN_NULL();

    ZERO_tmtc(&tmtc);
    tm = tmtcTm(&tmtc);

    if (interval2tm(*it, tm, &tmtcFsec(&tmtc)) != 0)
        PG_RETURN_NULL();

    /* wday is meaningless, yday approximates the total span in days */
    tm->tm_yday = (tm->tm_year * MONTHS_PER_YEAR + tm->tm_mon) * DAYS_PER_MONTH + tm->tm_mday;

    if (!(res = datetime_to_char_body(&tmtc, fmt, true, PG_GET_COLLATION())))
        PG_RETURN_NULL();

    PG_RETURN_TEXT_P(res);
}

/* ---------------------
 * TO_TIMESTAMP()
 *
 * Make Timestamp from date_str which is formatted at argument 'fmt'
 * ( to_timestamp is reverse to_char() )
 * ---------------------
 */
Datum to_timestamp(PG_FUNCTION_ARGS)
{
    text* date_txt = PG_GETARG_TEXT_P(0);
    text* fmt = PG_GETARG_TEXT_P(1);
    Timestamp result;
    int tz = 0;

    struct pg_tm tm;
    fsec_t fsec;

    do_to_timestamp(date_txt, fmt, &tm, &fsec);

    if (tm2timestamp(&tm, fsec, &tz, &result) != 0)
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("timestamp out of range")));

    PG_RETURN_TIMESTAMP(result);
}

/* ----------
 * TO_DATE
 * Make Date from date_str which is formated at argument 'fmt'
 * ----------
 */
Datum to_date(PG_FUNCTION_ARGS)
{
    text* date_txt = PG_GETARG_TEXT_P(0);
    text* fmt = PG_GETARG_TEXT_P(1);
    DateADT result;
    struct pg_tm tm;
    fsec_t fsec;

    do_to_timestamp(date_txt, fmt, &tm, &fsec);

    if (!IS_VALID_JULIAN(tm.tm_year, tm.tm_mon, tm.tm_mday))
        ereport(ERROR,
            (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                errmsg("date out of range: \"%s\"", text_to_cstring(date_txt))));

    result = date2j(tm.tm_year, tm.tm_mon, tm.tm_mday) - POSTGRES_EPOCH_JDATE;

    PG_RETURN_DATEADT(result);
}

static bool DCH_check(TmFromChar* datetime, FormatNode* formatnode, TmFromCharFlag* datetime_flag, char* invalid_format,
    int* invalid_value, char* valid_range)
{
    FormatNode* dateformatnode = NULL;

    if (NULL == formatnode)
        return false;
    for (dateformatnode = formatnode; (dateformatnode->type != NODE_TYPE_END); dateformatnode++) {
        if (dateformatnode->type == NODE_TYPE_ACTION) {
            switch (dateformatnode->key->id) {
                case DCH_HH:
                case DCH_HH12:
                    if (datetime_flag->hh_flag &&
                        (datetime->hh > MAX_VALUE_12_CLOCK || datetime->hh < MIN_VALUE_12_CLOCK)) {

                        errno_t rc = EOK;
                        rc = strncpy_s(invalid_format, DCH_CACHE_SIZE + 1, "hour", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        *invalid_value = datetime->hh;

                        rc = strncpy_s(valid_range, DCH_CACHE_SIZE + 1, "between 1 and 12", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        invalid_format[DCH_CACHE_SIZE] = '\0';
                        valid_range[DCH_CACHE_SIZE] = '\0';

                        return true;
                    }
                    break;
                case DCH_HH24:
                    if (datetime_flag->hh_flag &&
                        (datetime->hh > MAX_VALUE_24_CLOCK || datetime->hh < MIN_VALUE_24_CLOCK)) {

                        errno_t rc = EOK;
                        rc = strncpy_s(invalid_format, DCH_CACHE_SIZE + 1, "hour", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        *invalid_value = datetime->hh;

                        rc = strncpy_s(valid_range, DCH_CACHE_SIZE + 1, "between 0 and 24", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        invalid_format[DCH_CACHE_SIZE] = '\0';
                        valid_range[DCH_CACHE_SIZE] = '\0';

                        return true;
                    }
                    break;
                case DCH_MI:
                    if (datetime_flag->mi_flag && (datetime->mi > MAX_VALUE_MI || datetime->mi < MIN_VALUE_MI)) {

                        errno_t rc = EOK;
                        rc = strncpy_s(invalid_format, DCH_CACHE_SIZE + 1, "minute", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        *invalid_value = datetime->mi;

                        rc = strncpy_s(valid_range, DCH_CACHE_SIZE + 1, "between 0 and 60", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        invalid_format[DCH_CACHE_SIZE] = '\0';
                        valid_range[DCH_CACHE_SIZE] = '\0';

                        return true;
                    }
                    break;
                case DCH_SS:
                    if (datetime_flag->ss_flag && (datetime->ss > MAX_VALUE_SS || datetime->ss < MIN_VALUE_SS)) {

                        errno_t rc = EOK;
                        rc = strncpy_s(invalid_format, DCH_CACHE_SIZE + 1, "second", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        *invalid_value = datetime->ss;

                        rc = strncpy_s(valid_range, DCH_CACHE_SIZE + 1, "between 0 and 60", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        invalid_format[DCH_CACHE_SIZE] = '\0';
                        valid_range[DCH_CACHE_SIZE] = '\0';

                        return true;
                    }
                    break;
                case DCH_SSSSS:
                case DCH_sssss:
                    if (datetime_flag->sssss_flag &&
                        (datetime->sssss > MAX_VALUE_SSSSS || datetime->sssss < MIN_VALUE_SSSSS)) {

                        errno_t rc = EOK;
                        rc = strncpy_s(invalid_format, DCH_CACHE_SIZE + 1, "seconds past midnight", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        *invalid_value = datetime->sssss;

                        rc = strncpy_s(valid_range, DCH_CACHE_SIZE + 1, "between 0 and 86400", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        invalid_format[DCH_CACHE_SIZE] = '\0';
                        valid_range[DCH_CACHE_SIZE] = '\0';

                        return true;
                    }
                    break;
                case DCH_DAY:
                case DCH_DY:
                case DCH_Dy:
                case DCH_day:
                case DCH_dy:
                case DCH_D:
                    if (datetime_flag->d_flag && (datetime->d > MAX_VALUE_D - 1 || datetime->d < MIN_VALUE_D - 1)) {

                        errno_t rc = EOK;
                        rc = strncpy_s(invalid_format, DCH_CACHE_SIZE + 1, "day", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        *invalid_value = datetime->d + 1;

                        rc = strncpy_s(valid_range, DCH_CACHE_SIZE + 1, "between 1 and 7", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        invalid_format[DCH_CACHE_SIZE] = '\0';
                        valid_range[DCH_CACHE_SIZE] = '\0';

                        return true;
                    }
                    break;
                case DCH_DD:
                    if (datetime_flag->dd_flag && (datetime->dd > MAX_VALUE_DD || datetime->dd < MIN_VALUE_DD)) {

                        errno_t rc = EOK;
                        rc = strncpy_s(invalid_format, DCH_CACHE_SIZE + 1, "day of month", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        *invalid_value = datetime->dd;

                        rc = strncpy_s(valid_range, DCH_CACHE_SIZE + 1, "between 1 and 31", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        invalid_format[DCH_CACHE_SIZE] = '\0';
                        valid_range[DCH_CACHE_SIZE] = '\0';

                        return true;
                    }
                    break;
                case DCH_DDD:
                    if (datetime_flag->ddd_flag && (datetime->ddd > MAX_VALUE_DDD || datetime->ddd < MIN_VALUE_DDD)) {

                        errno_t rc = EOK;
                        rc = strncpy_s(invalid_format, DCH_CACHE_SIZE + 1, "days of year", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        *invalid_value = datetime->ddd;

                        rc = strncpy_s(valid_range, DCH_CACHE_SIZE + 1, "between 1 and 366", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        invalid_format[DCH_CACHE_SIZE] = '\0';
                        valid_range[DCH_CACHE_SIZE] = '\0';

                        return true;
                    }
                    break;
                case DCH_MM:
                case DCH_MONTH:
                case DCH_MON:
                case DCH_Month:
                case DCH_Mon:
                case DCH_month:
                case DCH_mon:
                case DCH_RM:
                case DCH_rm:
                    if (datetime_flag->mm_flag && (datetime->mm > MAX_VALUE_MM || datetime->mm < MIN_VALUE_MM)) {

                        errno_t rc = EOK;
                        rc = strncpy_s(invalid_format, DCH_CACHE_SIZE + 1, "month", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        *invalid_value = datetime->mm;

                        rc = strncpy_s(valid_range, DCH_CACHE_SIZE + 1, "between 1 and 12", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        invalid_format[DCH_CACHE_SIZE] = '\0';
                        valid_range[DCH_CACHE_SIZE] = '\0';
                        return true;
                    }
                    break;
                case DCH_MS:
                    if (datetime_flag->ms_flag && (datetime->ms > MAX_VALUE_MS || datetime->ms < MIN_VALUE_MS)) {

                        errno_t rc = EOK;
                        rc = strncpy_s(invalid_format, DCH_CACHE_SIZE + 1, "millisecond", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        *invalid_value = datetime->ms;

                        rc = strncpy_s(valid_range, DCH_CACHE_SIZE + 1, "between 0 and 999", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        invalid_format[DCH_CACHE_SIZE] = '\0';
                        valid_range[DCH_CACHE_SIZE] = '\0';

                        return true;
                    }
                    break;
                case DCH_Y_YYY:
                case DCH_YYYY:
                case DCH_YYY:
                case DCH_YY:
                case DCH_Y:
                    if (datetime_flag->year_flag && (datetime->year > MAX_VALUE_YEAR || datetime->year <= 0)) {

                        errno_t rc = EOK;
                        rc = strncpy_s(invalid_format, DCH_CACHE_SIZE + 1, "year", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        *invalid_value = datetime->year;

                        rc = strncpy_s(
                            valid_range, DCH_CACHE_SIZE + 1, "between 1 and 9999, and not be 0", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        invalid_format[DCH_CACHE_SIZE] = '\0';
                        valid_range[DCH_CACHE_SIZE] = '\0';

                        return true;
                    }
                    break;
                case DCH_WW:
                    if (datetime_flag->ww_flag && (datetime->ww > MAX_VALUE_WW || datetime->ww < MIN_VALUE_WW)) {

                        errno_t rc = EOK;
                        rc = strncpy_s(invalid_format, DCH_CACHE_SIZE + 1, "week number of year", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        *invalid_value = datetime->ww;

                        rc = strncpy_s(valid_range, DCH_CACHE_SIZE + 1, "between 1 and 53", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        invalid_format[DCH_CACHE_SIZE] = '\0';
                        valid_range[DCH_CACHE_SIZE] = '\0';

                        return true;
                    }
                    break;
                case DCH_W:
                    if (datetime_flag->w_flag && (datetime->w > MAX_VALUE_W || datetime->w < MIN_VALUE_W)) {

                        errno_t rc = EOK;
                        rc = strncpy_s(invalid_format, DCH_CACHE_SIZE + 1, "week of month ", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        *invalid_value = datetime->w;

                        rc = strncpy_s(valid_range, DCH_CACHE_SIZE + 1, "between 1 and 5", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        invalid_format[DCH_CACHE_SIZE] = '\0';
                        valid_range[DCH_CACHE_SIZE] = '\0';

                        return true;
                    }
                    break;
                case DCH_J:
                    if (datetime_flag->j_flag && (datetime->j > MAX_VALUE_J || datetime->j < MIN_VALUE_J)) {

                        errno_t rc = EOK;
                        rc = strncpy_s(invalid_format, DCH_CACHE_SIZE + 1, "julian day ", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        *invalid_value = datetime->j;

                        rc = strncpy_s(valid_range, DCH_CACHE_SIZE + 1, "between 1 and 5373484", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        invalid_format[DCH_CACHE_SIZE] = '\0';
                        valid_range[DCH_CACHE_SIZE] = '\0';

                        return true;
                    }
                    break;
                case DCH_US:
                    if (datetime_flag->us_flag && (datetime->us > MAX_VALUE_US || datetime->us < MIN_VALUE_US)) {
                        errno_t rc = EOK;
                        rc = strncpy_s(invalid_format, DCH_CACHE_SIZE + 1, "microsecond", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        *invalid_value = datetime->us;

                        rc = strncpy_s(valid_range, DCH_CACHE_SIZE + 1, "between 1 and 999999", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        invalid_format[DCH_CACHE_SIZE] = '\0';
                        valid_range[DCH_CACHE_SIZE] = '\0';

                        return true;
                    }
                    break;
                case DCH_SYYYY:
                    if (datetime_flag->year_flag &&
                        (datetime->year < MIN_VALUE_YEAR || datetime->year > MAX_VALUE_YEAR || datetime->year == 0)) {

                        errno_t rc = EOK;
                        rc = strncpy_s(invalid_format, DCH_CACHE_SIZE + 1, "year", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        *invalid_value = datetime->year;

                        rc = strncpy_s(
                            valid_range, DCH_CACHE_SIZE + 1, "between -4712 and 9999, and not be 0", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        invalid_format[DCH_CACHE_SIZE] = '\0';
                        valid_range[DCH_CACHE_SIZE] = '\0';

                        return true;
                    }
                    break;
                case DCH_RR:
                case DCH_RRRR:
                    if (datetime_flag->year_flag && (datetime->year <= 0 || datetime->year > MAX_VALUE_YEAR)) {
                        errno_t rc = EOK;
                        rc = strncpy_s(invalid_format, DCH_CACHE_SIZE + 1, "year(RR/RRRR)", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        *invalid_value = datetime->year;

                        rc = strncpy_s(valid_range, DCH_CACHE_SIZE + 1, "between 0 and 9999", DCH_CACHE_SIZE);
                        securec_check(rc, "\0", "\0");

                        invalid_format[DCH_CACHE_SIZE] = '\0';
                        valid_range[DCH_CACHE_SIZE] = '\0';

                        return true;
                    }
                    break;
                default:
                    break;
            }
        }
    }

    return optimized_dck_check(datetime, datetime_flag, invalid_format, invalid_value, valid_range);
}

/*
 * @Description: check whether bulkload datetime format options are valid or not.
 * @IN fmt: bulkload datetime format string
 * @Return: void
 * @See also: PG original do_to_timestamp() function; get_time_format();
 */
void check_datetime_format(char* fmt)
{
    if (strlen(fmt) > 0) {
        bool incache = true;
        FormatNode* format = get_format(fmt, strlen(fmt), &incache);

        /*
         * free the memory newly allocated for format.
         */
        if (!incache) {
            pfree_ext(format);
            format = NULL;
        }
    }
}

/*
 * @Description: parse the given formatting string and return a
 *         FormatNode object to represent this time format.
 * @IN fmt_str: date/time/timestamp/smalldatetime formatting string.
 * @IN fmt_len: length of format string.
 * @OUT in_cache: whether FormatNode object is in cache buffer.
 *         if not, caller should free the new FormatNode space.
 * @Return: FormatNode object.
 * @See also: PG original do_to_timestamp() function
 */
static FormatNode* get_format(char* fmt_str, int fmt_len, bool* in_cache)
{
    FormatNode* format = NULL;

    /*
     * Allocate new memory if format picture is bigger than static cache
     * and not use cache (call parser always)
     */
    if (fmt_len > DCH_CACHE_SIZE) {
        format = (FormatNode*)palloc((fmt_len + 1) * sizeof(FormatNode));
        *in_cache = false;

        parse_format(format, fmt_str, DCH_keywords, DCH_suff, DCH_index, DCH_TO_TIMESTAMP_TYPE, NULL);

        (format + fmt_len)->type = NODE_TYPE_END; /* Paranoia? */
    } else {
        /* Use cache buffers */
        DCHCacheEntry* ent = NULL;

        *in_cache = true;
        if ((ent = DCH_cache_search(fmt_str)) == NULL) {
            ent = DCH_cache_getnew(fmt_str);

            /*
             * Not in the cache, must run parser and save a new
             * format-picture to the cache.
             */
            parse_format(ent->format, fmt_str, DCH_keywords, DCH_suff, DCH_index, DCH_TO_TIMESTAMP_TYPE, NULL);

            (ent->format + fmt_len)->type = NODE_TYPE_END; /* Paranoia? */
        }
        format = ent->format;
    }

    return format;
}

/*
 * @Description: Given tmfc and tmfc_flag, convert their value to PG
 *    time info including fsec_t data.
 * @OUT fsec: fsec_t info.
 * @OUT tm: PG time struct info.
 * @IN tmfc: input time from string
 * @IN tmfc_flag: input time flag from string
 * @See also: PG original do_to_timestamp() function
 */
static void convert_values_to_pgtime(struct pg_tm* tm, fsec_t* fsec, TmFromChar& tmfc, TmFromCharFlag& tmfc_flag)
{
    DEBUG_TMFC(&tmfc);

    /*
     * Convert values that user define for FROM_CHAR (to_date/to_timestamp) to
     * standard 'tm'
     */
    if (tmfc.sssss) {
        int x = tmfc.sssss;

        tm->tm_hour = x / SECS_PER_HOUR;
        x %= SECS_PER_HOUR;
        tm->tm_min = x / SECS_PER_MINUTE;
        x %= SECS_PER_MINUTE;
        tm->tm_sec = x;
    }

    if (tmfc.ss)
        tm->tm_sec = tmfc.ss;
    if (tmfc.mi)
        tm->tm_min = tmfc.mi;
    if (tmfc.hh)
        tm->tm_hour = tmfc.hh;

    if (tmfc_flag.sssss_flag) {
        if (tmfc_flag.ss_flag && (tmfc.ss != ((tmfc.sssss % SECS_PER_HOUR) % SECS_PER_MINUTE))) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_DATETIME_FORMAT), errmsg(" seconds of minute conflicts with seconds in day")));
        }
        if (tmfc_flag.mi_flag && (tmfc.mi != ((tmfc.sssss % SECS_PER_HOUR) / SECS_PER_MINUTE))) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_DATETIME_FORMAT), errmsg(" minutes of hour conflicts with seconds in day")));
        }
        if (tmfc_flag.hh_flag && (tmfc.hh != tmfc.sssss / SECS_PER_HOUR)) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_DATETIME_FORMAT), errmsg("hour conflicts with seconds in day")));
        }
    }

    if (tmfc.clock == CLOCK_12_HOUR) {
        if (tm->tm_hour < 1 || tm->tm_hour > HOURS_PER_DAY / 2)
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_DATETIME_FORMAT),
                    errmsg("hour \"%d\" is invalid for the 12-hour clock", tm->tm_hour),
                    errhint("Use the 24-hour clock, or give an hour between 1 and 12.")));

        if (tmfc.pm && tm->tm_hour < HOURS_PER_DAY / 2)
            tm->tm_hour += HOURS_PER_DAY / 2;
        else if (!tmfc.pm && tm->tm_hour == HOURS_PER_DAY / 2)
            tm->tm_hour = 0;
    }

    if (tmfc.year) {
        /*
         * If CC and YY (or Y) are provided, use YY as 2 low-order digits for
         * the year in the given century.  Keep in mind that the 21st century
         * runs from 2001-2100, not 2000-2099.
         *
         * If a 4-digit year is provided, we use that and ignore CC.
         */
        if (tmfc.cc && tmfc.yysz <= 2) {
            tm->tm_year = tmfc.year % 100;
            if (tm->tm_year)
                tm->tm_year += (tmfc.cc - 1) * 100;
            else
                tm->tm_year = tmfc.cc * 100;
        } else
            tm->tm_year = tmfc.year;

        /* modify the value if  tmfc.year is negative */
        if (!tmfc.bc && tmfc.year < 0)
            tm->tm_year++;
    } else if (tmfc.cc) /* use first year of century */
        tm->tm_year = (tmfc.cc - 1) * 100 + 1;

    if (tmfc.bc) {
        if (tm->tm_year > 0)
            tm->tm_year = -(tm->tm_year - 1);
        else
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_DATETIME_FORMAT),
                    errmsg("inconsistent use of year %04d and \"BC\"", tm->tm_year)));
    }

    if (tmfc.j)
        j2date(tmfc.j, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);

    /* check conflict for j,year,month,day */
    if (tmfc_flag.j_flag) {
        if (tmfc_flag.year_flag && (tm->tm_year != tmfc.year)) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_DATETIME_FORMAT), errmsg("year conflicts with Julian date")));
        }
        if (tmfc_flag.mm_flag && (tm->tm_mon != tmfc.mm)) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_DATETIME_FORMAT), errmsg("month conflicts with Julian date")));
        }
        if (tmfc_flag.dd_flag && (tm->tm_mday != tmfc.dd)) {
            ereport(ERROR, (errcode(ERRCODE_INVALID_DATETIME_FORMAT), errmsg("day conflicts with Julian date")));
        }
    }

    if (tmfc.ww) {
        if (tmfc.mode == FROM_CHAR_DATE_ISOWEEK) {
            /*
             * If tmfc.d is not set, then the date is left at the beginning of
             * the ISO week (Monday).
             */
            if (tmfc.d)
                isoweekdate2date(tmfc.ww, tmfc.d, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);
            else
                isoweek2date(tmfc.ww, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);
        } else
            tmfc.ddd = (tmfc.ww - 1) * 7 + 1;
    }

    if (tmfc.w)
        tmfc.dd = (tmfc.w - 1) * 7 + 1;
    if (tmfc.d)
        tm->tm_wday = tmfc.d;
    if (tmfc.dd)
        tm->tm_mday = tmfc.dd;
    if (tmfc.mm)
        tm->tm_mon = tmfc.mm;

    if (tmfc.ddd) {
        tm->tm_yday = tmfc.ddd;

        if (tm->tm_mon <= 1 || tm->tm_mday <= 1) {
            /*
             * The month and day field have not been set, so we use the
             * day-of-year field to populate them.	Depending on the date mode,
             * this field may be interpreted as a Gregorian day-of-year, or an ISO
             * week date day-of-year.
             */

            if (!tm->tm_year && !tmfc.bc)
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_DATETIME_FORMAT),
                        errmsg("cannot calculate day of year without year information")));

            if (tmfc.mode == FROM_CHAR_DATE_ISOWEEK) {
                int j0; /* zeroth day of the ISO year, in Julian */

                j0 = isoweek2j(tm->tm_year, 1) - 1;

                j2date(j0 + tmfc.ddd, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);
            } else {
                const int* y = NULL;
                int i;

                static const int ysum[2][13] = {{0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365},
                    {0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366}};

                y = ysum[isleap(tm->tm_year)];

                for (i = 1; i <= MONTHS_PER_YEAR; i++) {
                    if (tmfc.ddd < y[i])
                        break;
                }

                if (tmfc_flag.ddd_flag && tmfc_flag.mm_flag)
                    if (i != tmfc.mm)
                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_DATETIME_FORMAT), errmsg("month conflicts with days of year")));

                if (tm->tm_mon <= 1)
                    tm->tm_mon = i;

                /* check conflict for ddd,day,month */
                if (tmfc_flag.ddd_flag && tmfc_flag.dd_flag)
                    if ((tmfc.ddd - y[i - 1]) != tmfc.dd)
                        ereport(ERROR,
                            (errcode(ERRCODE_INVALID_DATETIME_FORMAT),
                                errmsg("days of month conflicts with days of year")));

                if (tm->tm_mday <= 1)
                    tm->tm_mday = tmfc.ddd - y[i - 1];
            }
        }
    }

#ifdef HAVE_INT64_TIMESTAMP
    if (tmfc.ms)
        *fsec += tmfc.ms * 1000;
    if (tmfc.us)
        *fsec += tmfc.us;
#else
    if (tmfc.ms)
        *fsec += (double)tmfc.ms / 1000;
    if (tmfc.us)
        *fsec += (double)tmfc.us / 1000000;
#endif

    DEBUG_TM(tm);
}

/*
 * @Description: Given time format node, parse time string and return PG time info.
 * @IN date_str: date/time C string.
 * @OUT fsec: fsec_t info
 * @IN in_format: input time format node.
 * @OUT tm: PG time info.
 * @See also:
 */
#ifndef ENABLE_UT
static
#endif
    void
    general_to_timestamp_from_user_format(struct pg_tm* tm, fsec_t* fsec, char* date_str, void* in_format)
{
    FormatNode* format = ((TimeFormatInfo*)in_format)->tm_format;
    TmFromChar tmfc;
    TmFromCharFlag tmfc_flag;
    int invalid_value = 0;
    errno_t rc = EOK;
    char invalid_format[DCH_CACHE_SIZE + 1];
    char valid_range[DCH_CACHE_SIZE + 1];
    bool non_match = false;
    bool non_licet = false;

    rc = memset_s(&tmfc_flag, sizeof(TmFromCharFlag), 0, sizeof(TmFromCharFlag));
    securec_check(rc, "\0", "\0");
    rc = memset_s(valid_range, sizeof(valid_range), 0, DCH_CACHE_SIZE + 1);
    securec_check(rc, "\0", "\0");
    rc = memset_s(invalid_format, sizeof(invalid_format), 0, DCH_CACHE_SIZE + 1);
    securec_check(rc, "\0", "\0");

    /* reset tmfc && tm && fsec */
    *fsec = 0;
    ZERO_tm(tm);
    rc = memset_s(&tmfc, sizeof(TmFromChar), 0, sizeof(TmFromChar));
    securec_check(rc, "\0", "\0");

    /* parse this time string by given formatting node */
    DCH_from_char(format, date_str, &tmfc, &non_match, &tmfc_flag);
    if (non_match) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_DATETIME_FORMAT), errmsg("invalid data for match	in date string")));
    }

    non_licet = DCH_check(&tmfc, format, &tmfc_flag, invalid_format, &invalid_value, valid_range);
    if (non_licet) {
        ereport(ERROR,
            (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                errmsg("invalid data for \"%s =  %d\" ,value must be %s", invalid_format, invalid_value, valid_range)));
    }

    convert_values_to_pgtime(tm, fsec, tmfc, tmfc_flag);
}

/*
 * do_to_timestamp: shared code for to_timestamp and to_date
 *
 * Parse the 'date_txt' according to 'fmt', return results as a struct pg_tm
 * and fractional seconds.
 *
 * We parse 'fmt' into a list of FormatNodes, which is then passed to
 * DCH_from_char to populate a TmFromChar with the parsed contents of
 * 'date_txt'.
 *
 * The TmFromChar is then analysed and converted into the final results in
 * struct 'tm' and 'fsec'.
 *
 * This function does very little error checking, e.g.
 * to_timestamp('20096040','YYYYMMDD') works
 */
static void do_to_timestamp(text* date_txt, text* fmt, struct pg_tm* tm, fsec_t* fsec)
{
    FormatNode* format = NULL;
    TmFromChar tmfc;
    int fmt_len;

    bool non_match = false;
    bool non_licet = false;

    TmFromCharFlag tmfc_flag;
    int invalid_value = 0;
    char invalid_format[DCH_CACHE_SIZE + 1];

    char valid_range[DCH_CACHE_SIZE + 1];
    errno_t rc = EOK;

    rc = memset_s(invalid_format, sizeof(invalid_format), 0, DCH_CACHE_SIZE + 1);
    securec_check(rc, "\0", "\0");
    rc = memset_s(valid_range, sizeof(valid_range), 0, DCH_CACHE_SIZE + 1);
    securec_check(rc, "\0", "\0");
    rc = memset_s(&tmfc_flag, sizeof(TmFromCharFlag), 0, sizeof(TmFromCharFlag));
    securec_check(rc, "\0", "\0");
    rc = memset_s(&tmfc, sizeof(TmFromChar), 0, sizeof(TmFromChar));
    securec_check(rc, "\0", "\0");
    ZERO_tm(tm);
    *fsec = 0;

    fmt_len = VARSIZE_ANY_EXHDR(fmt);

    if (fmt_len) {
        char* fmt_str = NULL;
        char* date_str = NULL;
        bool incache = true;

        fmt_str = text_to_cstring(fmt);
        format = get_format(fmt_str, fmt_len, &incache);
        date_str = text_to_cstring(date_txt);
        DCH_from_char(format, date_str, &tmfc, &non_match, &tmfc_flag);

        if (non_match) {
            pfree_ext(date_str);
            pfree_ext(fmt_str);
            if (!incache)
                pfree_ext(format);
            ereport(
                ERROR, (errcode(ERRCODE_INVALID_DATETIME_FORMAT), errmsg("invalid data for match  in date string")));
        }

        non_licet = DCH_check(&tmfc, format, &tmfc_flag, invalid_format, &invalid_value, valid_range);

        if (non_licet) {
            pfree_ext(date_str);
            pfree_ext(fmt_str);
            if (!incache)
                pfree_ext(format);
            ereport(ERROR,
                (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                    errmsg("invalid data for \"%s =  %d\" ,value must be %s",
                        invalid_format,
                        invalid_value,
                        valid_range)));
        }

        pfree_ext(date_str);
        pfree_ext(fmt_str);
        if (!incache)
            pfree_ext(format);
    }

    convert_values_to_pgtime(tm, fsec, tmfc, tmfc_flag);
}

/**********************************************************************
 *	the NUMBER version part
 *********************************************************************/

static char* fill_str(char* str, int c, int max)
{
    errno_t rc = EOK;
    rc = memset_s(str, max, c, max);
    securec_check(rc, "\0", "\0");
    *(str + max) = '\0';
    return str;
}

#define zeroize_NUM(_n)          \
    do {                         \
        (_n)->flag = 0;          \
        (_n)->lsign = 0;         \
        (_n)->pre = 0;           \
        (_n)->post = 0;          \
        (_n)->pre_lsign_num = 0; \
        (_n)->need_locale = 0;   \
        (_n)->multi = 0;         \
        (_n)->zero_start = 0;    \
        (_n)->zero_end = 0;      \
    } while (0)

static NUMCacheEntry* NUM_cache_getnew(const char* str)
{
    NUMCacheEntry* ent = NULL;

    /* counter overflow check - paranoia? */
    if (t_thrd.format_cxt.NUM_counter >= (INT_MAX - NUM_CACHE_FIELDS - 1)) {
        t_thrd.format_cxt.NUM_counter = 0;

        for (ent = t_thrd.format_cxt.NUM_cache; ent <= (t_thrd.format_cxt.NUM_cache + NUM_CACHE_FIELDS); ent++)
            ent->age = (++t_thrd.format_cxt.NUM_counter);
    }

    /*
     * If cache is full, remove oldest entry
     */
    if (t_thrd.format_cxt.n_NUM_cache > NUM_CACHE_FIELDS) {
        NUMCacheEntry* old = t_thrd.format_cxt.NUM_cache + 0;

#ifdef DEBUG_TO_FROM_CHAR
        elog(DEBUG_elog_output, "Cache is full (%d)", t_thrd.format_cxt.n_NUM_cache);
#endif
        for (ent = t_thrd.format_cxt.NUM_cache; ent <= (t_thrd.format_cxt.NUM_cache + NUM_CACHE_FIELDS); ent++) {
            /*
             * entry removed via NUM_cache_remove() can be used here, which is
             * why it's worth scanning first entry again
             */
            if (ent->str[0] == '\0') {
                old = ent;
                break;
            }
            if (ent->age < old->age)
                old = ent;
        }
#ifdef DEBUG_TO_FROM_CHAR
        elog(DEBUG_elog_output, "OLD: \"%s\" AGE: %d", old->str, old->age);
#endif
        errno_t rc = EOK;
        rc = strncpy_s(old->str, NUM_CACHE_SIZE + 1, str, NUM_CACHE_SIZE);
        securec_check(rc, "\0", "\0");
        /* old->format fill parser */
        old->age = (++t_thrd.format_cxt.NUM_counter);
        ent = old;
    } else {
#ifdef DEBUG_TO_FROM_CHAR
        elog(DEBUG_elog_output, "NEW (%d)", t_thrd.format_cxt.n_NUM_cache);
#endif
        ent = t_thrd.format_cxt.NUM_cache + t_thrd.format_cxt.n_NUM_cache;
        errno_t rc = EOK;
        rc = strncpy_s(ent->str, NUM_CACHE_SIZE + 1, str, NUM_CACHE_SIZE);
        securec_check(rc, "\0", "\0");
        /* ent->format fill parser */
        ent->age = (++t_thrd.format_cxt.NUM_counter);
        ++t_thrd.format_cxt.n_NUM_cache;
    }

    zeroize_NUM(&ent->Num);

    t_thrd.format_cxt.last_NUM_cache_entry = ent;
    return ent;
}

static NUMCacheEntry* NUM_cache_search(const char* str)
{
    int i;
    NUMCacheEntry* ent = NULL;

    /* counter overflow check - paranoia? */
    if (t_thrd.format_cxt.NUM_counter >= (INT_MAX - NUM_CACHE_FIELDS - 1)) {
        t_thrd.format_cxt.NUM_counter = 0;

        for (ent = t_thrd.format_cxt.NUM_cache; ent <= (t_thrd.format_cxt.NUM_cache + NUM_CACHE_FIELDS); ent++)
            ent->age = (++t_thrd.format_cxt.NUM_counter);
    }

    for (i = 0, ent = t_thrd.format_cxt.NUM_cache; i < t_thrd.format_cxt.n_NUM_cache; i++, ent++) {
        if (strcmp(ent->str, str) == 0) {
            ent->age = (++t_thrd.format_cxt.NUM_counter);
            t_thrd.format_cxt.last_NUM_cache_entry = ent;
            return ent;
        }
    }

    return NULL;
}

static void NUM_cache_remove(NUMCacheEntry* ent)
{
    if (ent == NULL) {
        return;
    }
#ifdef DEBUG_TO_FROM_CHAR
    elog(DEBUG_elog_output, "REMOVING ENTRY (%s)", ent->str);
#endif
    ent->str[0] = '\0';
    ent->age = 0;
}

/* ----------
 * Cache routine for NUM to_char version
 * ----------
 */
static FormatNode* NUM_cache(int len, NUMDesc* Num, text* pars_str, bool* shouldFree)
{
    FormatNode* format = NULL;
    char* str = NULL;

    str = text_to_cstring(pars_str);

    /*
     * Allocate new memory if format picture is bigger than static cache and
     * not use cache (call parser always). This branches sets shouldFree to
     * true, accordingly.
     */
    if (len > NUM_CACHE_SIZE) {
        format = (FormatNode*)palloc((len + 1) * sizeof(FormatNode));

        *shouldFree = true;

        zeroize_NUM(Num);

        parse_format(format, str, NUM_keywords, NULL, NUM_index, NUM_TYPE, Num);

        (format + len)->type = NODE_TYPE_END; /* Paranoia? */
    } else {
        /*
         * Use cache buffers
         */
        NUMCacheEntry* ent = NULL;

        *shouldFree = false;

        if ((ent = NUM_cache_search(str)) == NULL) {
            ent = NUM_cache_getnew(str);

            /*
             * Not in the cache, must run parser and save a new format-picture
             * to the cache.
             */
            parse_format(ent->format, str, NUM_keywords, NULL, NUM_index, NUM_TYPE, &ent->Num);

            (ent->format + len)->type = NODE_TYPE_END; /* Paranoia? */
        }

        format = ent->format;

        /*
         * Copy cache to used struct
         */
        Num->flag = ent->Num.flag;
        Num->lsign = ent->Num.lsign;
        Num->pre = ent->Num.pre;
        Num->post = ent->Num.post;
        Num->pre_lsign_num = ent->Num.pre_lsign_num;
        Num->need_locale = ent->Num.need_locale;
        Num->multi = ent->Num.multi;
        Num->zero_start = ent->Num.zero_start;
        Num->zero_end = ent->Num.zero_end;
    }

#ifdef DEBUG_TO_FROM_CHAR
    dump_index(NUM_keywords, NUM_index);
#endif

    pfree_ext(str);
    return format;
}

static char* int_to_roman(int number)
{
    int len = 0, num = 0;
    char *p = NULL, *result = NULL, numstr[5] = {0};
    errno_t ret = EOK;
    const int result_size = 16;
    result = (char*)palloc(result_size);
    *result = '\0';

    if (number > 3999 || number < 1) {
        fill_str(result, '#', 15);
        return result;
    }
    ret = snprintf_s(numstr, sizeof(numstr), sizeof(numstr) - 1, "%d", number);
    securec_check_ss(ret, "\0", "\0");
    len = strlen(numstr);

    for (p = numstr; *p != '\0'; p++, --len) {
        num = *p - 49; /* 48 ascii + 1 */
        if (num < 0)
            continue;

        if (len > 3) {
            while (num-- != -1) {
                ret = strcat_s(result, result_size, "M");
                securec_check(ret, "\0", "\0");
            }
        } else {
            if (len == 3)
                ret = strcat_s(result, result_size, rm100[num]);
            else if (len == 2)
                ret = strcat_s(result, result_size, rm10[num]);
            else if (len == 1)
                ret = strcat_s(result, result_size, rm1[num]);
            securec_check(ret, "\0", "\0");
        }
    }
    return result;
}

/* ----------
 * Locale
 * ----------
 */
static void NUM_prepare_locale(NUMProc* Np)
{
    if (Np->Num->need_locale) {
        struct lconv* lconv;

        /*
         * Get locales
         */
        lconv = PGLC_localeconv();

        /*
         * Positive / Negative number sign
         */
        if (lconv->negative_sign && *lconv->negative_sign)
            Np->L_negative_sign = lconv->negative_sign;
        else
            Np->L_negative_sign = "-";

        if (lconv->positive_sign && *lconv->positive_sign)
            Np->L_positive_sign = lconv->positive_sign;
        else
            Np->L_positive_sign = "+";

        /*
         * Number decimal point
         */
        if (lconv->decimal_point && *lconv->decimal_point)
            Np->decimal = lconv->decimal_point;

        else
            Np->decimal = ".";

        if (!IS_LDECIMAL(Np->Num))
            Np->decimal = ".";

        /*
         * Number thousands separator
         *
         * Some locales (e.g. broken glibc pt_BR), have a comma for decimal,
         * but "" for thousands_sep, so we set the thousands_sep too.
         * http://archives.postgresql.org/pgsql-hackers/2007-11/msg00772.php
         */
        if (lconv->thousands_sep && *lconv->thousands_sep)
            Np->L_thousands_sep = lconv->thousands_sep;
        /* Make sure thousands separator doesn't match decimal point symbol. */
        else if (strcmp(Np->decimal, ",") != 0)
            Np->L_thousands_sep = ",";
        else
            Np->L_thousands_sep = ".";

        /*
         * Currency symbol
         */
        if (lconv->currency_symbol && *lconv->currency_symbol)
            Np->L_currency_symbol = lconv->currency_symbol;
        else
            Np->L_currency_symbol = " ";
    } else {
        /*
         * Default values
         */
        Np->L_negative_sign = "-";
        Np->L_positive_sign = "+";
        Np->decimal = ".";

        Np->L_thousands_sep = ",";
        Np->L_currency_symbol = " ";
    }
}

/* ----------
 * Return pointer of last relevant number after decimal point
 *	12.0500 --> last relevant is '5'
 *	12.0000 --> last relevant is '.'
 * If there is no decimal point, return NULL (which will result in same
 * behavior as if FM hadn't been specified).
 * ----------
 */
static char* get_last_relevant_decnum(char* num)
{
    char *result = NULL, *p = strchr(num, '.');

#ifdef DEBUG_TO_FROM_CHAR
    elog(DEBUG_elog_output, "get_last_relevant_decnum()");
#endif

    if (NULL == p)
        return NULL;

    result = p;

    while (*(++p)) {
        if (*p != '0')
            result = p;
    }

    return result;
}

/*
 * These macros are used in NUM_processor() and its subsidiary routines.
 * OVERLOAD_TEST: true if we've reached end of input string
 * AMOUNT_TEST(s): true if at least s characters remain in string
 */
#define OVERLOAD_TEST (Np->inout_p >= Np->inout + plen)
#define AMOUNT_TEST(s) (Np->inout_p <= Np->inout + (plen - (s)))

/* ----------
 * Number extraction for TO_NUMBER()
 * ----------
 */
static void NUM_numpart_from_char(NUMProc* Np, int id, int plen, int& tmp_len)
{
    bool isread = FALSE;

#ifdef DEBUG_TO_FROM_CHAR
    elog(DEBUG_elog_output,
        " --- scan start --- id=%s",
        ((id == NUM_0) || (id == NUM_9)) ? "NUM_0/9" : ((id == NUM_DEC) ? "NUM_DEC" : "???"));
#endif

    if (*Np->inout_p == ' ') {
        Np->inout_p++;
        tmp_len--;
    }

    if (*Np->inout_p == ' ') {
        Np->inout_p++;
        tmp_len--;
    }

    if (OVERLOAD_TEST)
        return;

    /*
     * read sign before number
     */
    if (*Np->number == ' ' && (id == NUM_0 || id == NUM_9) && (Np->read_pre + Np->read_post) == 0) {
#ifdef DEBUG_TO_FROM_CHAR
        elog(DEBUG_elog_output,
            "Try read sign (%c), locale positive: %s, negative: %s",
            *Np->inout_p,
            Np->L_positive_sign,
            Np->L_negative_sign);
#endif

        /*
         * locale sign
         */
        if (IS_LSIGN(Np->Num) && Np->Num->lsign == NUM_LSIGN_PRE) {
            int x = 0;

#ifdef DEBUG_TO_FROM_CHAR
            elog(DEBUG_elog_output, "Try read locale pre-sign (%c)", *Np->inout_p);
#endif
            if ((x = strlen(Np->L_negative_sign)) && AMOUNT_TEST(x) &&
                strncmp(Np->inout_p, Np->L_negative_sign, x) == 0) {
                Np->inout_p += x;
                tmp_len -= x;
                *Np->number = '-';
            } else if ((x = strlen(Np->L_positive_sign)) && AMOUNT_TEST(x) &&
                       strncmp(Np->inout_p, Np->L_positive_sign, x) == 0) {
                Np->inout_p += x;
                tmp_len -= x;
                *Np->number = '+';
            }
        } else {
#ifdef DEBUG_TO_FROM_CHAR
            elog(DEBUG_elog_output, "Try read simple sign (%c)", *Np->inout_p);
#endif

            /*
             * simple + - < >
             */
            if (*Np->inout_p == '-' || (IS_BRACKET(Np->Num) && *Np->inout_p == '<')) {
                *Np->number = '-'; /* set - */
                Np->inout_p++;
                tmp_len--;
            } else if (*Np->inout_p == '+') {
                *Np->number = '+'; /* set + */
                Np->inout_p++;
                tmp_len--;
            }
        }
    }

    if (OVERLOAD_TEST)
        return;

#ifdef DEBUG_TO_FROM_CHAR
    elog(DEBUG_elog_output, "Scan for numbers (%c), current number: '%s'", *Np->inout_p, Np->number);
#endif

    /*
     * read digit
     */
    if (isdigit((unsigned char)*Np->inout_p)) {
        if (Np->read_dec && Np->read_post == Np->Num->post)
            return;

        *Np->number_p = *Np->inout_p;
        Np->number_p++;
        tmp_len--;
        if (Np->read_dec)
            Np->read_post++;
        else
            Np->read_pre++;

        isread = TRUE;

#ifdef DEBUG_TO_FROM_CHAR
        elog(DEBUG_elog_output, "Read digit (%c)", *Np->inout_p);
#endif

        /*
         * read decimal point
         */
    } else if (IS_DECIMAL(Np->Num) && Np->read_dec == FALSE) {
#ifdef DEBUG_TO_FROM_CHAR
        elog(DEBUG_elog_output, "Try read decimal point (%c)", *Np->inout_p);
#endif
        if (*Np->inout_p == '.') {
            *Np->number_p = '.';
            Np->number_p++;
            tmp_len--;
            Np->read_dec = TRUE;
            isread = TRUE;
        } else {
            int x = strlen(Np->decimal);

#ifdef DEBUG_TO_FROM_CHAR
            elog(DEBUG_elog_output, "Try read locale point (%c)", *Np->inout_p);
#endif
            if (x && AMOUNT_TEST(x) && strncmp(Np->inout_p, Np->decimal, x) == 0) {
                Np->inout_p += x - 1;
                *Np->number_p = '.';
                Np->number_p++;
                tmp_len--;
                Np->read_dec = TRUE;
                isread = TRUE;
            }
        }
    }

    if (OVERLOAD_TEST)
        return;

    /*
     * Read sign behind "last" number
     *
     * We need sign detection because determine exact position of post-sign is
     * difficult:
     *
     * FM9999.9999999S	   -> 123.001- 9.9S			   -> .5- FM9.999999MI ->
     * 5.01-
     */
    if (*Np->number == ' ' && Np->read_pre + Np->read_post > 0) {
        /*
         * locale sign (NUM_S) is always anchored behind a last number, if: -
         * locale sign expected - last read char was NUM_0/9 or NUM_DEC - and
         * next char is not digit
         */
        if (IS_LSIGN(Np->Num) && isread && (Np->inout_p + 1) < Np->inout + plen &&
            !isdigit((unsigned char)*(Np->inout_p + 1))) {
            int x;
            char* tmp = Np->inout_p++;

#ifdef DEBUG_TO_FROM_CHAR
            elog(DEBUG_elog_output, "Try read locale post-sign (%c)", *Np->inout_p);
#endif
            if ((x = strlen(Np->L_negative_sign)) && AMOUNT_TEST(x) &&
                strncmp(Np->inout_p, Np->L_negative_sign, x) == 0) {
                Np->inout_p += x - 1; /* -1 .. NUM_processor() do inout_p++ */
                tmp_len -= x - 1;
                *Np->number = '-';
            } else if ((x = strlen(Np->L_positive_sign)) && AMOUNT_TEST(x) &&
                       strncmp(Np->inout_p, Np->L_positive_sign, x) == 0) {
                Np->inout_p += x - 1; /* -1 .. NUM_processor() do inout_p++ */
                tmp_len -= x - 1;
                *Np->number = '+';
            }
            if (*Np->number == ' ')
                /* no sign read */
                Np->inout_p = tmp;
        }

        /*
         * try read non-locale sign, it's happen only if format is not exact
         * and we cannot determine sign position of MI/PL/SG, an example:
         *
         * FM9.999999MI			   -> 5.01-
         *
         * if (.... && IS_LSIGN(Np->Num)==FALSE) prevents read wrong formats
         * like to_number('1 -', '9S') where sign is not anchored to last
         * number.
         */
        else if (isread == FALSE && IS_LSIGN(Np->Num) == FALSE && (IS_PLUS(Np->Num) || IS_MINUS(Np->Num))) {
#ifdef DEBUG_TO_FROM_CHAR
            elog(DEBUG_elog_output, "Try read simple post-sign (%c)", *Np->inout_p);
#endif

            /*
             * simple + -
             */
            if (*Np->inout_p == '-' || *Np->inout_p == '+')
                /* NUM_processor() do inout_p++ */
                *Np->number = *Np->inout_p;
        }
    }
}

#define IS_PREDEC_SPACE(_n) \
    (IS_ZERO((_n)->Num) == FALSE && (_n)->number == (_n)->number_p && *(_n)->number == '0' && (_n)->Num->post != 0)

/*
 * Skip over "n" input characters, but only if they aren't numeric data
 */
static void NUM_eat_non_data_chars(NUMProc* Np, int n, int plen, int& tmp_len)
{
    while (n-- > 0) {
        if (OVERLOAD_TEST)
            break; /* end of input */
        if (strchr("0123456789.,+-", *Np->inout_p) != NULL)
            break; /* its a data character */
        Np->inout_p += pg_mblen(Np->inout_p);
        if (OVERLOAD_TEST)
            break; /* end of input */
        tmp_len -= pg_mblen(Np->inout_p);
    }
}

/* ----------
 * Add digit or sign to number-string
 * ----------
 */
static void NUM_numpart_to_char(NUMProc* Np, int id, int& tmp_len)
{
    int end;
    errno_t rc = 0;

    if (IS_ROMAN(Np->Num))
        return;

        /* Note: in this elog() output not set '\0' in 'inout' */

#ifdef DEBUG_TO_FROM_CHAR

    /*
     * Np->num_curr is number of current item in format-picture, it is not
     * current position in inout!
     */
    elog(DEBUG_elog_output,
        "SIGN_WROTE: %d, CURRENT: %d, NUMBER_P: \"%s\", INOUT: \"%s\"",
        Np->sign_wrote,
        Np->num_curr,
        Np->number_p,
        Np->inout);
#endif
    Np->num_in = FALSE;

    /*
     * Write sign if real number will write to output Note: IS_PREDEC_SPACE()
     * handle "9.9" --> " .1"
     */
    if (Np->sign_wrote == FALSE &&
        (Np->num_curr >= Np->num_pre || (IS_ZERO(Np->Num) && Np->Num->zero_start == Np->num_curr)) &&
        (IS_PREDEC_SPACE(Np) == FALSE || (Np->last_relevant && *Np->last_relevant == '.'))) {
        if (IS_LSIGN(Np->Num)) {
            if (Np->Num->lsign == NUM_LSIGN_PRE) {
                if (Np->sign == '-') {
                    rc = strcpy_s(Np->inout_p, tmp_len, Np->L_negative_sign);
                    securec_check(rc, "\0", "\0");
                } else {
                    rc = strcpy_s(Np->inout_p, tmp_len, Np->L_positive_sign);
                    securec_check(rc, "\0", "\0");
                }
                Np->inout_p += strlen(Np->inout_p);
                tmp_len -= strlen(Np->inout_p);
                Np->sign_wrote = TRUE;
            }
        } else if (IS_BRACKET(Np->Num)) {
            *Np->inout_p = (Np->sign == '+') ? ' ' : '<';
            ++Np->inout_p;
            --tmp_len;
            Np->sign_wrote = TRUE;
        } else if (Np->sign == '+') {
            if (!IS_FILLMODE(Np->Num)) {
                *Np->inout_p = ' '; /* Write + */
                ++Np->inout_p;
                --tmp_len;
            }
            Np->sign_wrote = TRUE;
        } else if (Np->sign == '-') { /* Write - */
            *Np->inout_p = '-';
            ++Np->inout_p;
            --tmp_len;
            Np->sign_wrote = TRUE;
        }
    }

    /*
     * digits / FM / Zero / Dec. point
     */
    if (id == NUM_9 || id == NUM_0 || id == NUM_D || id == NUM_DEC) {
        if (Np->num_curr < Np->num_pre && (Np->Num->zero_start > Np->num_curr || !IS_ZERO(Np->Num))) {
            /*
             * Write blank space
             */
            if (!IS_FILLMODE(Np->Num)) {
                *Np->inout_p = ' '; /* Write ' ' */
                ++Np->inout_p;
                --tmp_len;
            }
        } else if (IS_ZERO(Np->Num) && Np->num_curr < Np->num_pre && Np->Num->zero_start <= Np->num_curr) {
            /*
             * Write ZERO
             */
            *Np->inout_p = '0'; /* Write '0' */
            ++Np->inout_p;
            --tmp_len;
            Np->num_in = TRUE;
        } else {
            /*
             * Write Decimal point
             */
            if (*Np->number_p == '.') {
                if ((NULL == Np->last_relevant) || *Np->last_relevant != '.') {
                    rc = strcpy_s(Np->inout_p, tmp_len, Np->decimal); /* Write DEC/D */
                    securec_check(rc, "\0", "\0");
                    Np->inout_p += strlen(Np->inout_p);
                    tmp_len -= strlen(Np->inout_p);
                }
                /*
                 * Ora 'n' -- FM9.9 --> 'n.'
                 */
                else if (IS_FILLMODE(Np->Num) && Np->last_relevant && *Np->last_relevant == '.') {
                    rc = strcpy_s(Np->inout_p, tmp_len, Np->decimal); /* Write DEC/D */
                    securec_check(rc, "\0", "\0");
                    Np->inout_p += strlen(Np->inout_p);
                    tmp_len -= strlen(Np->inout_p);
                }
            } else {
                /*
                 * Write Digits
                 */
                if (Np->last_relevant && Np->number_p > Np->last_relevant && id != NUM_0)
                    ;

                /*
                 * '0.1' -- 9.9 --> '  .1'
                 */
                else if (IS_PREDEC_SPACE(Np)) {
                    if (!IS_FILLMODE(Np->Num)) {
                        *Np->inout_p = ' ';
                        ++Np->inout_p;
                        --tmp_len;
                    }

                    /*
                     * '0' -- FM9.9 --> '0.'
                     */
                    else if (Np->last_relevant && *Np->last_relevant == '.') {
                        *Np->inout_p = '0';
                        ++Np->inout_p;
                        --tmp_len;
                    }
                } else {
                    *Np->inout_p = *Np->number_p; /* Write DIGIT */
                    ++Np->inout_p;
                    --tmp_len;
                    Np->num_in = TRUE;
                }
            }
            /* do no exceed string length */
            if (*Np->number_p) {
                ++Np->number_p;
                --tmp_len;
            }
        }

        end = Np->num_count + (Np->num_pre ? 1 : 0) + (IS_DECIMAL(Np->Num) ? 1 : 0);

        if (Np->last_relevant && Np->last_relevant == Np->number_p)
            end = Np->num_curr;

        if (Np->num_curr + 1 == end) {
            if (Np->sign_wrote == TRUE && IS_BRACKET(Np->Num)) {
                *Np->inout_p = (Np->sign == '+') ? ' ' : '>';
                ++Np->inout_p;
                --tmp_len;
            } else if (IS_LSIGN(Np->Num) && Np->Num->lsign == NUM_LSIGN_POST) {
                if (Np->sign == '-') {
                    rc = strcpy_s(Np->inout_p, tmp_len, Np->L_negative_sign);
                    securec_check(rc, "\0", "\0");
                } else {
                    rc = strcpy_s(Np->inout_p, tmp_len, Np->L_positive_sign);
                    securec_check(rc, "\0", "\0");
                }
                Np->inout_p += strlen(Np->inout_p);
                tmp_len -= strlen(Np->inout_p);
            }
        }
    }

    ++Np->num_curr;
}

/*
 * the long-integer addition, used in the conversion
 * from 16-byte hexadecimal to decimal,
 * summand returns the result
 */
void long_int_add(char* addend, const char* summand)
{
    /*[Possible access of out-of-bounds pointer (1 beyond end of data) by operator '['*/
    /*Possible use of null pointer 'addend' in left argument to operator '['*/
    char sum[MAX_DECIMAL_LEN] = {0};
    int len1 = 0;
    int len2 = 0;
    int tmp = 0;
    int carry = 0;
    int i = 0;
    const int len = MAX_DECIMAL_LEN;
    char* pStart = NULL;
    errno_t rc = EOK;

    if ((NULL == addend) || (NULL == summand)) {
        ereport(ERROR,
            (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                errmsg("long_int_add inner error, addend or summand is null pointer")));
        return;
    }

    len1 = (int)strnlen(addend, (unsigned int)len);
    len2 = (int)strnlen(summand, (unsigned int)len);

    /* both addend and summand are 0, return 0 */
    if ((0 == len1) && (0 == len2)) {
        rc = memset_s(addend, MAX_DECIMAL_LEN, '\0', (unsigned long)len);
        securec_check(rc, "\0", "\0");
        return;
    }

    /* addend==0, copy summand to addend */
    if ((0 == len1) && (len2 > 0)) {
        rc = memcpy_s(addend, MAX_DECIMAL_LEN, summand, (unsigned long)(len2 + 1));
        securec_check(rc, "\0", "\0");
        return;
    }

    /* Summand==0, return addend */
    if ((0 == len2) && (len1 > 0))
        return;

    /* Consider the carry and terminator, 2 bytes reserved*/
    if ((len1 > len - 2) || (len2 > len - 2))
        ereport(ERROR,
            (errcode(ERRCODE_DATA_EXCEPTION), errmsg("long_int_add inner error, length of long_int not support.")));

    /* len2 >= len1 */
    if (len2 >= len1) {
        if (len2 + 1 < MAX_DECIMAL_LEN) {
            *(sum + len2 + 1) = '\0';
        }
        carry = 0;

        /*calculate the sum of the long integer, sum[] returns the result */
        for (i = (int)(len2 - 1); i >= 0; i--) {
            if (len1 + i >= len2) {
                tmp = carry + (*(addend + i + len1 - len2) - '0') + (*(summand + i) - '0');
            } else {
                tmp = carry + (*(summand + i) - '0');
            }

            /* calculate the value of the current bit */
            sum[i + 1] = tmp % 10 + '0';

            /*calculate carry*/
            carry = tmp / 10;
        }
    } else {
        if (len1 + 1 < MAX_DECIMAL_LEN) {
            *(sum + len1 + 1) = '\0';
        }
        carry = 0;

        /*calculate the sum of the long integer, sum[] returns the result */
        for (i = len1 - 1; i >= 0; i--) {
            if (i + len2 >= len1)
                tmp = carry + (*(addend + i) - '0') + (*(summand + i + len2 - len1) - '0');
            else
                tmp = carry + (*(addend + i) - '0');

            /* calculate the value of the current bit */
            sum[i + 1] = tmp % 10 + '0';

            /*calculate carry*/
            carry = tmp / 10;
        }
    }

    /*dealing with the highest bit carry*/
    sum[0] = (char)(carry + '0');

    /* copy sum to addend */
    pStart = (sum[0] == '0') ? (sum + 1) : sum;
    rc = memset_s(addend, MAX_DECIMAL_LEN, '\0', (unsigned long)len);
    securec_check(rc, "\0", "\0");
    rc = memcpy_s(addend, MAX_DECIMAL_LEN, pStart, (unsigned long)(len - 1));
    securec_check(rc, "\0", "\0");
    return;
}

/*
 * Note: 'plen' is used in FROM_CHAR conversion and it's length of
 * input (inout). In TO_CHAR conversion it's space before first number.
 */
static char* NUM_processor(FormatNode* node, NUMDesc* Num, char* inout, char* number, int tmp_len, int plen, int sign,
    bool is_to_char, Oid collid)
{
    FormatNode* n = NULL;
    NUMProc _Np, *Np = &_Np;

    const char* pattern = NULL;
    int pattern_len;
    int max_len = tmp_len;
    // long_int_buffer is used in long_int_add as addend
    char long_int_buffer[MAX_DECIMAL_LEN] = {0};
    int power = 0;
    errno_t rc = 0;
    rc = memset_s(Np, sizeof(NUMProc), 0, sizeof(NUMProc));
    securec_check(rc, "\0", "\0");

    Np->Num = Num;
    Np->is_to_char = is_to_char;
    Np->number = number;
    Np->inout = inout;
    Np->last_relevant = NULL;
    Np->read_post = 0;
    Np->read_pre = 0;
    Np->read_dec = FALSE;

    if (Np->Num->zero_start)
        --Np->Num->zero_start;

    if (IS_EEEE(Np->Num)) {
        if (!Np->is_to_char)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("\"EEEE\" not supported for input")));
        rc = strcpy_s(inout, tmp_len, number);
        securec_check(rc, "\0", "\0");
        return inout;
    }

    /*
     * Roman correction
     */
    if (IS_ROMAN(Np->Num)) {
        if (!Np->is_to_char)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("\"RN\" not supported for input")));

        Np->Num->lsign = Np->Num->pre_lsign_num = Np->Num->post = Np->Num->pre = Np->num_pre = Np->sign = 0;

        if (IS_FILLMODE(Np->Num)) {
            Np->Num->flag = 0;
            Np->Num->flag = (unsigned int)Np->Num->flag | NUM_F_FILLMODE;
        } else
            Np->Num->flag = 0;
        Np->Num->flag = (unsigned int)Np->Num->flag | NUM_F_ROMAN;
    }

    /*
     * Sign
     */
    if (is_to_char) {
        Np->sign = sign;

        /* MI/PL/SG - write sign itself and not in number */
        if (IS_PLUS(Np->Num) || IS_MINUS(Np->Num)) {
            if (IS_PLUS(Np->Num) && IS_MINUS(Np->Num) == FALSE)
                Np->sign_wrote = FALSE; /* need sign */
            else
                Np->sign_wrote = TRUE; /* needn't sign */
        } else {
            if (Np->sign != '-') {
                if (IS_BRACKET(Np->Num) && IS_FILLMODE(Np->Num))
                    Np->Num->flag = (unsigned int)Np->Num->flag & ~NUM_F_BRACKET;
                if (IS_MINUS(Np->Num))
                    Np->Num->flag = (unsigned int)Np->Num->flag & ~NUM_F_MINUS;
            } else if (Np->sign != '+' && IS_PLUS(Np->Num))
                Np->Num->flag = (unsigned int)Np->Num->flag & ~NUM_F_PLUS;

            if (Np->sign == '+' && IS_FILLMODE(Np->Num) && IS_LSIGN(Np->Num) == FALSE)
                Np->sign_wrote = TRUE; /* needn't sign */
            else
                Np->sign_wrote = FALSE; /* need sign */

            if (Np->Num->lsign == NUM_LSIGN_PRE && Np->Num->pre == Np->Num->pre_lsign_num)
                Np->Num->lsign = NUM_LSIGN_POST;
        }
    } else
        Np->sign = FALSE;

    /*
     * Count
     */
    Np->num_count = Np->Num->post + Np->Num->pre - 1;

    if (is_to_char) {
        Np->num_pre = plen;

        if (IS_FILLMODE(Np->Num) && IS_DECIMAL(Np->Num)) {
            Np->last_relevant = get_last_relevant_decnum(Np->number);

            /*
             * If any '0' specifiers are present, make sure we don't strip
             * those digits.
             */
            if (Np->last_relevant && Np->Num->zero_end > Np->num_pre) {
                char* last_zero = NULL;

                last_zero = Np->number + (Np->Num->zero_end - Np->num_pre);
                if (Np->last_relevant < last_zero)
                    Np->last_relevant = last_zero;
            }
        }

        if (Np->sign_wrote == FALSE && 0 == Np->num_pre)
            ++Np->num_count;
    } else {
        Np->num_pre = 0;
        *Np->number = ' '; /* sign space */
        *(Np->number + 1) = '\0';
    }

    Np->num_in = 0;
    Np->num_curr = 0;

#ifdef DEBUG_TO_FROM_CHAR
    elog(DEBUG_elog_output,
        "\n\tSIGN: '%c'\n\tNUM: '%s'\n\tPRE: %d\n\tPOST: %d\n\tNUM_COUNT: %d\n\tNUM_PRE: %d\n\tSIGN_WROTE: %s\n\tZERO: "
        "%s\n\tZERO_START: %d\n\tZERO_END: %d\n\tLAST_RELEVANT: %s\n\tBRACKET: %s\n\tPLUS: %s\n\tMINUS: "
        "%s\n\tFILLMODE: %s\n\tROMAN: %s\n\tEEEE: %s",
        Np->sign,
        Np->number,
        Np->Num->pre,
        Np->Num->post,
        Np->num_count,
        Np->num_pre,
        Np->sign_wrote ? "Yes" : "No",
        IS_ZERO(Np->Num) ? "Yes" : "No",
        Np->Num->zero_start,
        Np->Num->zero_end,
        Np->last_relevant ? Np->last_relevant : "<not set>",
        IS_BRACKET(Np->Num) ? "Yes" : "No",
        IS_PLUS(Np->Num) ? "Yes" : "No",
        IS_MINUS(Np->Num) ? "Yes" : "No",
        IS_FILLMODE(Np->Num) ? "Yes" : "No",
        IS_ROMAN(Np->Num) ? "Yes" : "No",
        IS_EEEE(Np->Num) ? "Yes" : "No");
#endif

    /*
     * Locale
     */
    NUM_prepare_locale(Np);

    /*
     * Processor direct cycle
     */
    if (Np->is_to_char)
        Np->number_p = Np->number;
    else
        Np->number_p = Np->number + 1; /* first char is space for sign */

    for (n = node, Np->inout_p = Np->inout; n->type != NODE_TYPE_END; n++) {
        if (!Np->is_to_char) {
            /*
             * Check at least one character remains to be scanned.	(In
             * actions below, must use AMOUNT_TEST if we want to read more
             * characters than that.)
             */
            if (OVERLOAD_TEST)
                break;
        }
        if (unlikely(Np->inout_p - Np->inout >= max_len)) {
            ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("input format of numeric shouldn't bigger than length of buffer.")));
        }

        /*
         * Format pictures actions
         */
        if (n->type == NODE_TYPE_ACTION) {
            /*
             * Create/read digit/zero/blank/sign/special-case
             *
             * 'NUM_S' note: The locale sign is anchored to number and we
             * read/write it when we work with first or last number
             * (NUM_0/NUM_9). This is reason why NUM_S missing in follow
             * switch().
             * (NUM_0/NUM_9).  This is why NUM_S is missing in switch().
             *
             * Notice the "Np->inout_p++" at the bottom of the loop.  This is
             * why most of the actions advance inout_p one less than you might
             * expect.  In cases where we don't want that increment to happen,
             * a switch case ends with "continue" not "break".
             */
            switch (n->key->id) {
                case NUM_9:
                case NUM_0:
                case NUM_DEC:
                case NUM_D:
                    if (Np->is_to_char) {
                        NUM_numpart_to_char(Np, n->key->id, tmp_len);
                        continue; /* for() */
                    } else {
                        if (strchr("0123456789.D-+<", *Np->inout_p) == NULL) {
                            Np->inout_p++;
                            tmp_len--;
                        }
                        NUM_numpart_from_char(Np, n->key->id, plen, tmp_len);
                        break; /* switch() case: */
                    }

                case NUM_COMMA:
                    if (Np->is_to_char) {
                        if (!Np->num_in) {
                            if (IS_FILLMODE(Np->Num))
                                continue;
                            else
                                *Np->inout_p = ' ';
                        } else
                            *Np->inout_p = ',';
                    } else {
                        if (!Np->num_in) {
                            if (IS_FILLMODE(Np->Num))
                                continue;
                        }
                        if (!CORRECT_TO_NUMBER) {
                            if (u_sess->attr.attr_sql.sql_compatibility == A_FORMAT && *Np->inout_p != ',') {
                                ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION), errmsg("invalid data.")));
                            }
                        } else if (*Np->inout_p != ',')
                            continue;
                    }
                    break;

                case NUM_G:
                    pattern = Np->L_thousands_sep;
                    pattern_len = strlen(pattern);
                    if (Np->is_to_char) {
                        if (!Np->num_in) {
                            if (IS_FILLMODE(Np->Num))
                                continue;
                            else {
                                /* just in case there are MB chars */
                                pattern_len = pg_mbstrlen(pattern);
                                rc = memset_s(Np->inout_p, pattern_len, ' ', pattern_len);
                                securec_check(rc, "\0", "\0");
                                Np->inout_p += pattern_len - 1;
                                tmp_len -= pattern_len - 1;
                            }
                        } else {
                            rc = strcpy_s(Np->inout_p, tmp_len, pattern);
                            securec_check(rc, "\0", "\0");
                            Np->inout_p += pattern_len - 1;
                            tmp_len -= pattern_len - 1;
                        }
                    } else {
                        if (!Np->num_in) {
                            if (IS_FILLMODE(Np->Num))
                                continue;
                        }
                        /*
                         * Because L_thousands_sep typically contains data
                         * characters (either '.' or ','), we cannot use
                         * NUM_eat_non_data_chars here. Instead skip only if
                         * the input matches L_thousands_sep.
                         */
                        if (AMOUNT_TEST(pattern_len) && strncmp(Np->inout_p, pattern, pattern_len) == 0)
                            Np->inout_p += pattern_len - 1;
                        else
                            continue;
                    }
                    break;

                case NUM_L:
                    pattern = Np->L_currency_symbol;
                    if (Np->is_to_char) {
                        rc = strcpy_s(Np->inout_p, tmp_len, pattern);
                        securec_check(rc, "\0", "\0");
                        Np->inout_p += strlen(Np->inout_p) - 1;
                        tmp_len -= strlen(Np->inout_p) - 1;
                    } else {
                        NUM_eat_non_data_chars(Np, pg_mbstrlen(pattern), plen, tmp_len);
                        continue;
                    }
                    break;

                case NUM_RN:
                    if (IS_FILLMODE(Np->Num)) {
                        rc = strcpy_s(Np->inout_p, tmp_len, Np->number_p);
                        securec_check(rc, "\0", "\0");
                        Np->inout_p += strlen(Np->inout_p) - 1;
                        tmp_len -= strlen(Np->inout_p) - 1;
                    } else {
                        rc = sprintf_s(Np->inout_p, tmp_len, "%15s", Np->number_p);
                        securec_check_ss(rc, "\0", "\0");
                        Np->inout_p += strlen(Np->inout_p) - 1;
                        tmp_len += strlen(Np->inout_p) - 1;
                    }
                    break;

                case NUM_rn:
                    if (IS_FILLMODE(Np->Num)) {
                        rc = strcpy_s(Np->inout_p, tmp_len, str_tolower_z(Np->number_p, collid));
                        securec_check(rc, "\0", "\0");
                        Np->inout_p += strlen(Np->inout_p) - 1;
                        tmp_len -= strlen(Np->inout_p) - 1;
                    } else {
                        rc = sprintf_s(Np->inout_p, tmp_len, "%15s", str_tolower_z(Np->number_p, collid));
                        securec_check_ss(rc, "\0", "\0");
                        Np->inout_p += strlen(Np->inout_p) - 1;
                        tmp_len += strlen(Np->inout_p) - 1;
                    }
                    break;

                case NUM_th:
                    if (IS_ROMAN(Np->Num) || *Np->number == '#' || Np->sign == '-' || IS_DECIMAL(Np->Num))
                        continue;

                    if (Np->is_to_char) {
                        rc = strcpy_s(Np->inout_p, tmp_len, get_th(Np->number, TH_LOWER));
                        securec_check(rc, "\0", "\0");
                        Np->inout_p += 1;
                        tmp_len -= 1;
                    } else {
                        /* All variants of 'th' occupy 2 characters */
                        NUM_eat_non_data_chars(Np, 2, plen, tmp_len);
                        continue;
                    }
                    break;

                case NUM_TH:
                    if (IS_ROMAN(Np->Num) || *Np->number == '#' || Np->sign == '-' || IS_DECIMAL(Np->Num))
                        continue;

                    if (Np->is_to_char) {
                        rc = strcpy_s(Np->inout_p, tmp_len, get_th(Np->number, TH_UPPER));
                        securec_check(rc, "\0", "\0");
                        Np->inout_p += 1;
                        tmp_len -= 1;
                    } else {
                        /* All variants of 'TH' occupy 2 characters */
                        NUM_eat_non_data_chars(Np, 2, plen, tmp_len);
                        continue;
                    }
                    break;

                case NUM_MI:
                    if (Np->is_to_char) {
                        if (Np->sign == '-')
                            *Np->inout_p = '-';
                        else if (IS_FILLMODE(Np->Num))
                            continue;
                        else
                            *Np->inout_p = ' ';
                    } else {
                        if (*Np->inout_p == '-')
                            *Np->number = '-';
                        else {
                            NUM_eat_non_data_chars(Np, 1, plen, tmp_len);
                            continue;
                        }
                    }
                    break;

                case NUM_PL:
                    if (Np->is_to_char) {
                        if (Np->sign == '+')
                            *Np->inout_p = '+';
                        else if (IS_FILLMODE(Np->Num))
                            continue;
                        else
                            *Np->inout_p = ' ';
                    } else {
                        if (*Np->inout_p == '+')
                            *Np->number = '+';
                        else {
                            NUM_eat_non_data_chars(Np, 1, plen, tmp_len);
                            continue;
                        }
                    }
                    break;

                case NUM_SG:
                    if (Np->is_to_char)
                        *Np->inout_p = Np->sign;

                    else {
                        if (*Np->inout_p == '-')
                            *Np->number = '-';
                        else if (*Np->inout_p == '+')
                            *Np->number = '+';
                        else {
                            NUM_eat_non_data_chars(Np, 1, plen, tmp_len);
                            continue;
                        }
                    }
                    break;

                // convert 16-byte hexadecimal to decimal
                case NUM_X:
                case NUM_x:
                    if (Np->is_to_char) {
                        if ((!IS_FILLMODE(Np->Num)) && (Np->sign_wrote == false)) {
                            // a space for the sign bit, and other spaces for filling
                            errno_t rc = EOK;
                            rc = memset_s(Np->inout_p, (plen + 1), ' ', (plen + 1));
                            securec_check(rc, "\0", "\0");
                            Np->inout_p += plen + 1;
                            Np->sign_wrote = TRUE;
                        }
                        *(Np->inout_p) = *(Np->number_p);
                        Np->number_p++;
                    } else {
                        int curNum = 0;
                        int ret = 0;

                        if (*(Np->inout_p) >= '0' && *(Np->inout_p) <= '9')
                            curNum = *(Np->inout_p) - '0';
                        else if (*(Np->inout_p) >= 'a' && *(Np->inout_p) <= 'f')
                            curNum = 10 + *(Np->inout_p) - 'a';
                        else if (*(Np->inout_p) >= 'A' && *(Np->inout_p) <= 'F')
                            curNum = 10 + *(Np->inout_p) - 'A';
                        else
                            ereport(ERROR,
                                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                                    errmsg("\"%c\" not supported", *(Np->inout_p))));

                        /* The conversion of an 8-bit hexadecimal number to a decimal number may be greater than
                         * INT_MAX. Therefore, we only process a 7-bit hexadecimal number in the INT_MAX range.
                         */
                        if (plen > 0 && plen < MAX_HEX_LEN_FOR_INT32) {
                            Np->read_pre = Np->read_pre * 16 + curNum;
                            int pre_len = 1, dup_read_pre = Np->read_pre;

                            while ((dup_read_pre = dup_read_pre / 10))
                                ++pre_len;

                            ret = sprintf_s(Np->number, pre_len + 1, "%d", Np->read_pre);
                            securec_check_ss(ret, "\0", "\0");
                        } else if (plen >= MAX_HEX_LEN_FOR_INT32 && plen <= MAX_POWER + 1) {
                            long_int_add(long_int_buffer, g_HexToDecMatrix[MAX_POWER + 1 + power - plen][curNum]);
                            ret = sprintf_s(Np->number, strlen(long_int_buffer) + 1, "%s", long_int_buffer);
                            securec_check_ss(ret, "\0", "\0");
                        } else {
                            /* Supports up to 16-byte hexadecimal conversion */
                            ereport(ERROR,
                                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                    errmsg("to_number only support 16 bytes hex to decimal conversion or plen less "
                                           "than 0.")));
                        }

                        power++;
                        Np->read_post = 0;
                        Np->Num->pre = Np->num_count = strlen(Np->number);
                        Np->number_p = Np->number + Np->num_count;
                    }
                    break;
                default:
                    continue;
                    break;
            }
        } else {
            /*
             * In TO_CHAR, non-pattern characters in the format are copied to
             * the output.  In TO_NUMBER, we skip one input character for each
             * non-pattern format character, whether or not it matches the
             * format character.
             */
            if (Np->is_to_char)
                *Np->inout_p = n->character;
        }
        Np->inout_p++;
        tmp_len--;
    }

    if (Np->is_to_char) {
        *Np->inout_p = '\0';
        return Np->inout;
    } else {
        if (*(Np->number_p - 1) == '.')
            *(Np->number_p - 1) = '\0';
        else
            *Np->number_p = '\0';

        /*
         * Correction - precision of dec. number
         */
        Np->Num->post = Np->read_post;

#ifdef DEBUG_TO_FROM_CHAR
        elog(DEBUG_elog_output, "TO_NUMBER (number): '%s'", Np->number);
#endif
        return Np->number;
    }
}

/* ----------
 * MACRO: Start part of NUM - for all NUM's to_char variants
 *	(sorry, but I hate copy same code - macro is better..)
 * ----------
 */
#define NUM_TOCHAR_prepare                                              \
    do {                                                                \
        len = VARSIZE_ANY_EXHDR(fmt);                                   \
        if (len <= 0 || len >= (INT_MAX - VARHDRSZ) / NUM_MAX_ITEM_SIZ) \
            PG_RETURN_TEXT_P(cstring_to_text(""));                      \
        result_alloc_len = (len * NUM_MAX_ITEM_SIZ) + 1 + VARHDRSZ;     \
        result = (text*)palloc0(result_alloc_len);                      \
        format = NUM_cache(len, &Num, fmt, &shouldFree);                \
    } while (0)

/* ----------
 * MACRO: Finish part of NUM
 * ----------
 */
#define NUM_TOCHAR_finish                                                                   \
    do {                                                                                    \
        NUM_processor(format, &Num, VARDATA(result), numstr, result_alloc_len - VARHDRSZ,   \
                        plen, sign, true, PG_GET_COLLATION());                              \
                                                                                            \
        if (shouldFree)                                                                     \
            pfree_ext(format);                                                              \
                                                                                            \
        /*                                                                                  \
         * Convert null-terminated representation of result to standard text.               \
         * The result is usually much bigger than it needs to be, but there                 \
         * seems little point in realloc'ing it smaller.                                    \
         */                                                                                 \
        len = strlen(VARDATA(result));                                                      \
        SET_VARSIZE(result, len + VARHDRSZ);                                                \
    } while (0)

/* -------------------
 * NUMERIC to_number() (convert string to numeric)
 * -------------------
 */
Datum numeric_to_number(PG_FUNCTION_ARGS)
{
    text* value = PG_GETARG_TEXT_P(0);
    text* fmt = PG_GETARG_TEXT_P(1);
    NUMDesc Num;
    Datum result;
    FormatNode* format = NULL;
    char* numstr = NULL;
    bool shouldFree = false;
    int len = 0;
    int valuelen = 0;
    char* fmtstr = NULL;

    unsigned int scale;
    unsigned int precision;

    len = VARSIZE(fmt) - VARHDRSZ;

    if (len <= 0 || len >= INT_MAX / NUM_MAX_ITEM_SIZ)
        PG_RETURN_NULL();

    fmtstr = (char*)palloc(len + 1);
    errno_t rc = EOK;
    rc = strncpy_s(fmtstr, len + 1, VARDATA(fmt), len);
    securec_check(rc, "\0", "\0");
    fmtstr[len] = '\0';
    if (NULL != strchr(fmtstr, 'x') || NULL != strchr(fmtstr, 'X')) {
        int i = 0;
        for (i = 0; i < len; i++) {
            if ('x' != fmtstr[i] && 'X' != fmtstr[i]) {
                pfree_ext(fmtstr);
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid number format model")));
            }
        }
        valuelen = VARSIZE(value) - VARHDRSZ;
        if (valuelen > len) {
            pfree_ext(fmtstr);
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("invalid number")));
        }
    }
    pfree_ext(fmtstr);

    format = NUM_cache(len, &Num, fmt, &shouldFree);

    numstr = (char*)palloc((len * NUM_MAX_ITEM_SIZ) + 1);

    (void)NUM_processor(format,
        &Num,
        VARDATA(value),
        numstr,
        (len * NUM_MAX_ITEM_SIZ) + 1,
        VARSIZE(value) - VARHDRSZ,
        0,
        false,
        PG_GET_COLLATION());

    scale = (unsigned int)Num.post;
    precision = Max(0, Num.pre) + scale;

    if (shouldFree)
        pfree_ext(format);

    result = DirectFunctionCall3(numeric_in,
        CStringGetDatum(numstr),
        ObjectIdGetDatum(InvalidOid),
        Int32GetDatum(((precision << 16) | scale) + VARHDRSZ));
    pfree_ext(numstr);
    return result;
}

/* ------------------
 * NUMERIC to_char()
 * ------------------
 */
// the max length of INT64 string
#define INT64_STRING_MAXLEN 24
Datum numeric_to_char(PG_FUNCTION_ARGS)
{
    Numeric value = PG_GETARG_NUMERIC(0);
    text* fmt = PG_GETARG_TEXT_P(1);
    NUMDesc Num;
    FormatNode* format = NULL;
    text* result = NULL;
    bool shouldFree = false;
    int len = 0, plen = 0, sign = 0;
    char *numstr = NULL, *orgnum = NULL, *p = NULL;
    Numeric x;
    errno_t ret = EOK;
    int result_alloc_len = 0;

    NUM_TOCHAR_prepare;
    /*
     * On DateType depend part (numeric)
     */
    if (IS_ROMAN(&Num)) {
        x = DatumGetNumeric(DirectFunctionCall2(numeric_round, NumericGetDatum(value), Int32GetDatum(0)));
        numstr = orgnum = int_to_roman(DatumGetInt32(DirectFunctionCall1(numeric_int4, NumericGetDatum(x))));
    } else if (IS_EEEE(&Num)) {
        orgnum = numeric_out_sci(value, Num.post);

        /*
         * numeric_out_sci() does not emit a sign for positive numbers.  We
         * need to add a space in this case so that positive and negative
         * numbers are aligned.  We also have to do the right thing for NaN.
         */
        if (strcmp(orgnum, "NaN") == 0) {
            /*
             * Allow 6 characters for the leading sign, the decimal point,
             * "e", the exponent's sign and two exponent digits.
             */
            numstr = (char*)palloc(Num.pre + Num.post + 7);
            fill_str(numstr, '#', Num.pre + Num.post + 6);
            *numstr = ' ';
            *(numstr + Num.pre + 1) = '.';
        } else if (*orgnum != '-') {
            numstr = (char*)palloc(strlen(orgnum) + 2);
            *numstr = ' ';
            ret = strcpy_s(numstr + 1, strlen(orgnum) + 1, orgnum);
            securec_check(ret, "\0", "\0");
            len = strlen(numstr);
        } else {
            numstr = orgnum;
            len = strlen(orgnum);
        }
    } else {
        Numeric val = value;
        int64 val_int64;

        if (IS_x16(&Num)) {
            orgnum = (char*)palloc(INT64_STRING_MAXLEN);
            val_int64 = DatumGetInt64(DirectFunctionCall1(numeric_int8, NumericGetDatum(val)));
            ret = snprintf_s(orgnum, INT64_STRING_MAXLEN, INT64_STRING_MAXLEN - 1, INT64_FORMAT_x, val_int64);
            securec_check_ss(ret, "\0", "\0");
        } else if (IS_X16(&Num)) {
            orgnum = (char*)palloc(INT64_STRING_MAXLEN);
            val_int64 = DatumGetInt64(DirectFunctionCall1(numeric_int8, NumericGetDatum(val)));
            ret = snprintf_s(orgnum, INT64_STRING_MAXLEN, INT64_STRING_MAXLEN - 1, INT64_FORMAT_X, val_int64);
            securec_check_ss(ret, "\0", "\0");
        }

        else {
            if (IS_MULTI(&Num)) {
                Numeric a = DatumGetNumeric(DirectFunctionCall1(int4_numeric, Int32GetDatum(10)));
                Numeric b = DatumGetNumeric(DirectFunctionCall1(int4_numeric, Int32GetDatum(Num.multi)));

                x = DatumGetNumeric(DirectFunctionCall2(numeric_power, NumericGetDatum(a), NumericGetDatum(b)));
                val = DatumGetNumeric(DirectFunctionCall2(numeric_mul, NumericGetDatum(value), NumericGetDatum(x)));
                Num.pre += Num.multi;
            }

            x = DatumGetNumeric(DirectFunctionCall2(numeric_round, NumericGetDatum(val), Int32GetDatum(Num.post)));
            orgnum = DatumGetCString(DirectFunctionCall1(numeric_out_with_zero, NumericGetDatum(x)));
        }

        if (*orgnum == '-') {
            sign = '-';
            numstr = orgnum + 1;
        } else {
            sign = '+';
            numstr = orgnum;
        }
        if ((p = strchr(numstr, '.')))
            len = p - numstr;
        else
            len = strlen(numstr);

        if (Num.pre > len)
            plen = Num.pre - len;
        else if (len > Num.pre) {
            numstr = (char*)palloc(Num.pre + Num.post + 2);
            fill_str(numstr, '#', Num.pre + Num.post + 1);
            *(numstr + Num.pre) = '.';
        }
    }

    NUM_TOCHAR_finish;
    if (orgnum[0] == '0' && orgnum[1] == '\0' && HIDE_TAILING_ZERO) {
        SET_VARSIZE(result, strlen(orgnum) + VARHDRSZ);
        int errorno = memcpy_s(VARDATA(result), strlen(orgnum), orgnum, strlen(orgnum));
        securec_check(errorno, "\0", "\0");
    }
    PG_RETURN_TEXT_P(result);
}

/* ---------------
 * INT4 to_char()
 * ---------------
 */
// the max length of INT32 string
#define INT32_STRING_MAXLEN 12
Datum int4_to_char(PG_FUNCTION_ARGS)
{
    int32 value = PG_GETARG_INT32(0);
    text* fmt = PG_GETARG_TEXT_P(1);
    NUMDesc Num;
    FormatNode* format = NULL;
    text* result = NULL;
    bool shouldFree = false;
    int len = 0, plen = 0, sign = 0;
    char *numstr = NULL, *orgnum = NULL;
    errno_t ret = EOK;
    int result_alloc_len = 0;

    NUM_TOCHAR_prepare;

    /*
     * On DateType depend part (int32)
     */
    if (IS_ROMAN(&Num))
        numstr = orgnum = int_to_roman(value);
    else if (IS_EEEE(&Num)) {
        /* we can do it easily because float8 won't lose any precision */
        float8 val = (float8)value;

        orgnum = (char*)palloc(MAXDOUBLEWIDTH + 1);
        ret = snprintf_s(orgnum, MAXDOUBLEWIDTH + 1, MAXDOUBLEWIDTH, "%+.*e", Num.post, val);
        securec_check_ss(ret, "\0", "\0");

        /*
         * Swap a leading positive sign for a space.
         */
        if (*orgnum == '+')
            *orgnum = ' ';

        len = strlen(orgnum);
        numstr = orgnum;
    } else {
        if (IS_MULTI(&Num)) {
            orgnum = DatumGetCString(
                DirectFunctionCall1(int4out, Int32GetDatum(value * ((int32)pow((double)10, (double)Num.multi)))));
            Num.pre += Num.multi;
        }

        // deal with format 'x'/'X', convert int32 to hexadecimal string
        else if (IS_x16(&Num)) {
            orgnum = (char*)palloc(INT32_STRING_MAXLEN);
            ret = snprintf_s(orgnum, INT32_STRING_MAXLEN, INT32_STRING_MAXLEN - 1, "%x", value);
            securec_check_ss(ret, "\0", "\0");
        } else if (IS_X16(&Num)) {
            orgnum = (char*)palloc(INT32_STRING_MAXLEN);
            ret = snprintf_s(orgnum, INT32_STRING_MAXLEN, INT32_STRING_MAXLEN - 1, "%X", value);
            securec_check_ss(ret, "\0", "\0");
        } else {
            orgnum = DatumGetCString(DirectFunctionCall1(int4out, Int32GetDatum(value)));
        }

        if (*orgnum == '-') {
            sign = '-';
            orgnum++;
        } else
            sign = '+';
        len = strlen(orgnum);

        if (Num.post) {
            numstr = (char*)palloc(len + Num.post + 2);
            ret = strcpy_s(numstr, len + Num.post + 2, orgnum);
            securec_check(ret, "\0", "\0");
            *(numstr + len) = '.';
            ret = memset_s(numstr + len + 1, Num.post + 1, '0', Num.post);
            securec_check(ret, "\0", "\0");
            *(numstr + len + Num.post + 1) = '\0';
        } else
            numstr = orgnum;

        if (Num.pre > len)
            plen = Num.pre - len;
        else if (len > Num.pre) {
            numstr = (char*)palloc(Num.pre + Num.post + 2);
            fill_str(numstr, '#', Num.pre + Num.post + 1);
            *(numstr + Num.pre) = '.';
        }
    }

    NUM_TOCHAR_finish;
    PG_RETURN_TEXT_P(result);
}

/* ---------------
 * INT8 to_char()
 * ---------------
 */
Datum int8_to_char(PG_FUNCTION_ARGS)
{
    int64 value = PG_GETARG_INT64(0);
    text* fmt = PG_GETARG_TEXT_P(1);
    NUMDesc Num;
    FormatNode* format = NULL;
    text* result = NULL;
    bool shouldFree = false;
    int len = 0, plen = 0, sign = 0;
    char *numstr = NULL, *orgnum = NULL;
    errno_t ret = EOK;
    int result_alloc_len = 0;

    NUM_TOCHAR_prepare;

    /*
     * On DateType depend part (int32)
     */
    if (IS_ROMAN(&Num)) {
        /* Currently don't support int8 conversion to roman... */
        numstr = orgnum = int_to_roman(DatumGetInt32(DirectFunctionCall1(int84, Int64GetDatum(value))));
    } else if (IS_EEEE(&Num)) {
        /* to avoid loss of precision, must go via numeric not float8 */
        Numeric val;

        val = DatumGetNumeric(DirectFunctionCall1(int8_numeric, Int64GetDatum(value)));
        orgnum = numeric_out_sci(val, Num.post);

        /*
         * numeric_out_sci() does not emit a sign for positive numbers.  We
         * need to add a space in this case so that positive and negative
         * numbers are aligned.  We don't have to worry about NaN here.
         */
        if (*orgnum != '-') {
            numstr = (char*)palloc(strlen(orgnum) + 2);
            *numstr = ' ';
            ret = strcpy_s(numstr + 1, strlen(orgnum) + 1, orgnum);
            securec_check(ret, "\0", "\0");
            len = strlen(numstr);
        } else {
            numstr = orgnum;
            len = strlen(orgnum);
        }
    } else {
        if (IS_MULTI(&Num)) {
            double multi = pow((double)10, (double)Num.multi);

            value = DatumGetInt64(
                DirectFunctionCall2(int8mul, Int64GetDatum(value), DirectFunctionCall1(dtoi8, Float8GetDatum(multi))));
            Num.pre += Num.multi;
        }

        // deal with format 'x'/'X', convert int8 to hexadecimal string
        int ret = 0;
        if (IS_x16(&Num)) {
            orgnum = (char*)palloc(INT64_STRING_MAXLEN);
            ret = snprintf_s(orgnum, INT64_STRING_MAXLEN, INT64_STRING_MAXLEN - 1, INT64_FORMAT_x, value);
            securec_check_ss(ret, "\0", "\0");
        } else if (IS_X16(&Num)) {
            orgnum = (char*)palloc(INT64_STRING_MAXLEN);
            ret = snprintf_s(orgnum, INT64_STRING_MAXLEN, INT64_STRING_MAXLEN - 1, INT64_FORMAT_X, value);
            securec_check_ss(ret, "\0", "\0");
        } else {
            orgnum = DatumGetCString(DirectFunctionCall1(int8out, Int64GetDatum(value)));
        }

        if (*orgnum == '-') {
            sign = '-';
            orgnum++;
        } else
            sign = '+';
        len = strlen(orgnum);

        if (Num.post) {
            numstr = (char*)palloc(len + Num.post + 2);
            ret = strcpy_s(numstr, len + Num.post + 2, orgnum);
            securec_check(ret, "\0", "\0");
            *(numstr + len) = '.';
            ret = memset_s(numstr + len + 1, Num.post + 1, '0', Num.post);
            securec_check(ret, "\0", "\0");
            *(numstr + len + Num.post + 1) = '\0';
        } else
            numstr = orgnum;

        if (Num.pre > len)
            plen = Num.pre - len;
        else if (len > Num.pre) {
            numstr = (char*)palloc(Num.pre + Num.post + 2);
            fill_str(numstr, '#', Num.pre + Num.post + 1);
        }
    }

    NUM_TOCHAR_finish;
    PG_RETURN_TEXT_P(result);
}

/* -----------------
 * FLOAT4 to_char()
 * -----------------
 */
Datum float4_to_char(PG_FUNCTION_ARGS)
{
    float4 value = PG_GETARG_FLOAT4(0);
    text* fmt = PG_GETARG_TEXT_P(1);
    NUMDesc Num;
    FormatNode* format = NULL;
    text* result = NULL;
    bool shouldFree = false;
    int len = 0, plen = 0, sign = 0;
    char *numstr = NULL, *orgnum = NULL, *p = NULL;
    errno_t ret = EOK;
    int result_alloc_len = 0;

    NUM_TOCHAR_prepare;

    if (IS_ROMAN(&Num))
        numstr = orgnum = int_to_roman((int)rint(value));
    else if (IS_EEEE(&Num)) {
        numstr = orgnum = (char*)palloc(MAXDOUBLEWIDTH + 1);
        if (isnan(value) || is_infinite(value)) {
            /*
             * Allow 6 characters for the leading sign, the decimal point,
             * "e", the exponent's sign and two exponent digits.
             */
            numstr = (char*)palloc(Num.pre + Num.post + 7);
            fill_str(numstr, '#', Num.pre + Num.post + 6);
            *numstr = ' ';
            *(numstr + Num.pre + 1) = '.';
        } else {
            ret = snprintf_s(orgnum, MAXDOUBLEWIDTH + 1, MAXDOUBLEWIDTH, "%+.*e", Num.post, value);
            securec_check_ss(ret, "\0", "\0");

            /*
             * Swap a leading positive sign for a space.
             */
            if (*orgnum == '+')
                *orgnum = ' ';

            len = strlen(orgnum);
            numstr = orgnum;
        }
    } else {
        float4 val = value;
        Numeric val_numeric;
        int32 val_int32;

        if (IS_x16(&Num)) {
            orgnum = (char*)palloc(INT32_STRING_MAXLEN);
            val_numeric = DatumGetNumeric(DirectFunctionCall1(float4_numeric, Float4GetDatum(val)));
            val_int32 = DatumGetInt32(DirectFunctionCall1(numeric_int4, NumericGetDatum(val_numeric)));
            ret = snprintf_s(orgnum, INT32_STRING_MAXLEN, INT32_STRING_MAXLEN - 1, "%x", val_int32);
            securec_check_ss(ret, "\0", "\0");
        } else if (IS_X16(&Num)) {
            orgnum = (char*)palloc(INT32_STRING_MAXLEN);
            val_numeric = DatumGetNumeric(DirectFunctionCall1(float4_numeric, Float4GetDatum(val)));
            val_int32 = DatumGetInt32(DirectFunctionCall1(numeric_int4, NumericGetDatum(val_numeric)));
            ret = snprintf_s(orgnum, INT32_STRING_MAXLEN, INT32_STRING_MAXLEN - 1, "%X", val_int32);
            securec_check_ss(ret, "\0", "\0");
        } else {
            if (IS_MULTI(&Num)) {
                float multi = pow((double)10, (double)Num.multi);

                val = value * multi;
                Num.pre += Num.multi;
            }

            orgnum = (char*)palloc(MAXFLOATWIDTH + 1);
            ret = snprintf_s(orgnum, MAXFLOATWIDTH + 1, MAXFLOATWIDTH, "%.0f", fabs(val));
            securec_check_ss(ret, "\0", "\0");
            len = strlen(orgnum);
            if (Num.pre > len)
                plen = Num.pre - len;
            if (len >= FLT_DIG)
                Num.post = 0;
            else if (Num.post + len > FLT_DIG)
                Num.post = FLT_DIG - len;
            ret = snprintf_s(orgnum, MAXFLOATWIDTH + 1, MAXFLOATWIDTH, "%.*f", Num.post, val);
            securec_check_ss(ret, "\0", "\0");
        }

        if (*orgnum == '-') { /* < 0 */
            sign = '-';
            numstr = orgnum + 1;
        } else {
            sign = '+';
            numstr = orgnum;
        }
        if ((p = strchr(numstr, '.')))
            len = p - numstr;
        else
            len = strlen(numstr);

        if (Num.pre > len)
            plen = Num.pre - len;
        else if (len > Num.pre) {
            numstr = (char*)palloc(Num.pre + Num.post + 2);
            fill_str(numstr, '#', Num.pre + Num.post + 1);
        }
    }

    NUM_TOCHAR_finish;
    PG_RETURN_TEXT_P(result);
}

/* -----------------
 * FLOAT8 to_char()
 * -----------------
 */
Datum float8_to_char(PG_FUNCTION_ARGS)
{
    float8 value = PG_GETARG_FLOAT8(0);
    text* fmt = PG_GETARG_TEXT_P(1);
    NUMDesc Num;
    FormatNode* format = NULL;
    text* result = NULL;
    bool shouldFree = false;
    int len = 0, plen = 0, sign = 0;
    char *numstr = NULL, *orgnum = NULL, *p = NULL;
    errno_t ret = EOK;
    int result_alloc_len = 0;

    NUM_TOCHAR_prepare;

    if (IS_ROMAN(&Num))
        numstr = orgnum = int_to_roman((int)rint(value));
    else if (IS_EEEE(&Num)) {
        numstr = orgnum = (char*)palloc(MAXDOUBLEWIDTH + 1);
        if (isnan(value) || is_infinite(value)) {
            /*
             * Allow 6 characters for the leading sign, the decimal point,
             * "e", the exponent's sign and two exponent digits.
             */
            numstr = (char*)palloc(Num.pre + Num.post + 7);
            fill_str(numstr, '#', Num.pre + Num.post + 6);
            *numstr = ' ';
            *(numstr + Num.pre + 1) = '.';
        } else {
            ret = snprintf_s(orgnum, MAXDOUBLEWIDTH + 1, MAXDOUBLEWIDTH, "%+.*e", Num.post, value);
            securec_check_ss(ret, "\0", "\0");

            /*
             * Swap a leading positive sign for a space.
             */
            if (*orgnum == '+')
                *orgnum = ' ';

            len = strlen(orgnum);
            numstr = orgnum;
        }
    } else {
        float8 val = value;
        // deal with format 'x'/'X', convert float8 to hexadecimal string
        Numeric val_numeric;
        int64 val_int64;

        if (IS_x16(&Num)) {
            orgnum = (char*)palloc(INT64_STRING_MAXLEN);
            val_numeric = DatumGetNumeric(DirectFunctionCall1(float8_numeric, Float8GetDatum(val)));
            val_int64 = DatumGetInt64(DirectFunctionCall1(numeric_int8, NumericGetDatum(val_numeric)));
            ret = snprintf_s(orgnum, INT64_STRING_MAXLEN, INT64_STRING_MAXLEN - 1, INT64_FORMAT_x, val_int64);
            securec_check_ss(ret, "\0", "\0");
        } else if (IS_X16(&Num)) {
            orgnum = (char*)palloc(INT64_STRING_MAXLEN);
            val_numeric = DatumGetNumeric(DirectFunctionCall1(float8_numeric, Float8GetDatum(val)));
            val_int64 = DatumGetInt64(DirectFunctionCall1(numeric_int8, NumericGetDatum(val_numeric)));
            ret = snprintf_s(orgnum, INT64_STRING_MAXLEN, INT64_STRING_MAXLEN - 1, INT64_FORMAT_X, val_int64);
            securec_check_ss(ret, "\0", "\0");
        } else {
            if (IS_MULTI(&Num)) {
                double multi = pow((double)10, (double)Num.multi);

                val = value * multi;
                Num.pre += Num.multi;
            }
            orgnum = (char*)palloc(MAXDOUBLEWIDTH + 1);
            ret = snprintf_s(orgnum, MAXDOUBLEWIDTH + 1, MAXDOUBLEWIDTH, "%.0f", fabs(val));
            securec_check_ss(ret, "\0", "\0");
            len = strlen(orgnum);
            if (Num.pre > len)
                plen = Num.pre - len;
            if (len >= DBL_DIG)
                Num.post = 0;
            else if (Num.post + len > DBL_DIG)
                Num.post = DBL_DIG - len;
            ret = snprintf_s(orgnum, MAXDOUBLEWIDTH + 1, MAXDOUBLEWIDTH, "%.*f", Num.post, val);
            securec_check_ss(ret, "\0", "\0");
        }

        if (*orgnum == '-') { /* < 0 */
            sign = '-';
            numstr = orgnum + 1;
        } else {
            sign = '+';
            numstr = orgnum;
        }
        if ((p = strchr(numstr, '.')))
            len = p - numstr;
        else
            len = strlen(numstr);

        if (Num.pre > len)
            plen = Num.pre - len;
        else if (len > Num.pre) {
            numstr = (char*)palloc(Num.pre + Num.post + 2);
            fill_str(numstr, '#', Num.pre + Num.post + 1);
            *(numstr + Num.pre) = '.';
        }
    }

    NUM_TOCHAR_finish;
    PG_RETURN_TEXT_P(result);
}

void Init_NUM_cache(void)
{
    t_thrd.format_cxt.last_NUM_cache_entry = t_thrd.format_cxt.NUM_cache + 0;
}

Datum to_timestamp_default_format(PG_FUNCTION_ARGS)
{
    text* date_txt = PG_GETARG_TEXT_P(0);
    text* fmt = cstring_to_text(u_sess->attr.attr_common.nls_timestamp_format_string);

    Timestamp result = 0;
    int tz = 0;

    struct pg_tm tm;
    fsec_t fsec = 0;

    do_to_timestamp(date_txt, fmt, &tm, &fsec);

    if (tm2timestamp(&tm, fsec, &tz, &result) != 0)
        ereport(ERROR, (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("timestamp out of range")));

    PG_RETURN_TIMESTAMP(result);
}

/*
 * @Description: Given time pattern, time string and its constraint,
 *               check and parse time string to time value.
 * @OUT dest: output value
 * @IN tm_const: TmFormatConstraint for valid range and is-next-separator flag
 * @IN maxlen: max length of this field
 * @IN node: FormatNode info about time pattern
 * @IN/OUT src: time field string
 * @Return: how many chars consumed
 * @See also:
 */
static int optimized_parse_int_len(
    int* dest, char** src, const int maxlen, FormatNode* node, TmFormatConstraint* tm_const)
{
    long result = 0;
    char* init = *src;
    int used = 0;
    int tmp_errno = 0;

    /* caller has skipped any whitespace before parsing the integer */

    Assert(maxlen <= DCH_MAX_ITEM_SIZ);
    Assert(!S_FM(node->suffix));
    Assert(tm_const);

    if (tm_const->next_sep) {
        /*
         * the next node is known to be a non-digit value,
         * so we just slurp as many characters as we can get.
         */
        errno = 0;
        result = strtol(init, src, 10);
        tmp_errno = errno;

        if (*src == init) {
            ERROR_VALUE_MUST_BE_INTEGER(init, node->key->name);
        }
    } else {
        /* We need to pull exactly the number of characters given in 'maxlen' out
         * of the string, and convert those. different from from_char_parse_int_len(),
         * we don't copy that string again, and just make it ending with '\0'.
         */
        char* last = NULL;
        char* tmp_src = *src;
        int tmp_size = maxlen;
        char tmp_char = 0;

        /* loop until tmp_size is 0 or *tmp_src is 0 */
        while (tmp_size != 0 && *tmp_src != '\0') {
            --tmp_size;
            ++tmp_src;
        }
        /* remember this char and resotre it later. */
        tmp_char = *tmp_src;
        /* terminate number string with C terminator */
        *tmp_src = '\0';
        /* update valid length of this number string */
        used = tmp_src - *src;

        if (used >= maxlen) {
            errno = 0;
            result = strtol(*src, &last, 10);
            tmp_errno = errno;
            used = last - *src;

            if (used >= maxlen) {
                /* resotre this end char here */
                *tmp_src = tmp_char;
                /* advance strig pointer */
                *src += used;
            } else {
                if (used == 0)
                    ERROR_VALUE_MUST_BE_INTEGER(*src, node->key->name);
                else
                    ERROR_FIELD_MUST_BE_FIXED(*src, node->key->name, maxlen, used);
            }
        } else {
            ERROR_SOURCE_STRING_TOO_SHORT(node->key->name, maxlen, used);
        }
    }

    /* error report if overflow or not in valid range */
    if (tmp_errno == ERANGE || result < tm_const->min_val || result > tm_const->max_val) {
        ERROR_VALUE_MUST_BE_BETWEEN(node->key->name, tm_const->min_val, tm_const->max_val);
    }

    /* caller will ensure that dest is not null pointer and unique. */
    Assert(dest != NULL);
    *dest = (int)result;
    return *src - init;
}

#define optimized_parse_int(__dest, __src, __node, __extra) \
    optimized_parse_int_len((__dest), (__src), ((__node)->key->len), (__node), (__extra))

/*
 * @Description: check the valid of date/time value (TmFromChar)
 * @IN datetime: TmFromChar value
 * @IN datetime_flag: TmFromChar flag info
 * @OUT invalid_format: invalid field for displaying error detail
 * @OUT invalid_value: invalid value for displaying error detail
 * @OUT valid_range: valid range for displaying detail
 * @Return: true if TmFromChar value is valid; otherwise false.
 * @See also:
 */
static bool optimized_dck_check(
    TmFromChar* datetime, TmFromCharFlag* datetime_flag, char* invalid_format, int* invalid_value, char* valid_range)
{
    /* the value of month and day have been set */
    if (datetime_flag->mm_flag && datetime_flag->dd_flag) {
        int idx = datetime_flag->year_flag ? isleap(datetime->year) : 1;
        if (datetime->dd > (day_tab[idx][datetime->mm - 1])) {

            errno_t rc = EOK;
            rc = strncpy_s(invalid_format, DCH_CACHE_SIZE + 1, "day of month", DCH_CACHE_SIZE);
            securec_check(rc, "\0", "\0");

            *invalid_value = datetime->dd;

            rc = strncpy_s(valid_range, DCH_CACHE_SIZE + 1, "be fit for current month", DCH_CACHE_SIZE);
            securec_check(rc, "\0", "\0");

            invalid_format[DCH_CACHE_SIZE] = '\0';
            valid_range[DCH_CACHE_SIZE] = '\0';
            return true;
        }
    }

    /* the value of ddd and year have been set */
    if (datetime_flag->ddd_flag && datetime_flag->year_flag) {
        if ((!isleap(datetime->year) && (MAX_VALUE_DDD == datetime->ddd))) {

            errno_t rc = EOK;
            rc = strncpy_s(invalid_format, DCH_CACHE_SIZE + 1, "days of year", DCH_CACHE_SIZE);
            securec_check(rc, "\0", "\0");

            *invalid_value = datetime->ddd;

            rc = strncpy_s(valid_range, DCH_CACHE_SIZE + 1, "be fit for current year", DCH_CACHE_SIZE);
            securec_check(rc, "\0", "\0");

            invalid_format[DCH_CACHE_SIZE] = '\0';
            valid_range[DCH_CACHE_SIZE] = '\0';
            return true;
        }
    }

    return false;
}

/*
 * @Description: Given Y_YYY pattern and field string, parse and get its value.
 * @IN iterance: whether this field has been set before.
 * @IN node: Y_YYY pattern
 * @OUT out: year value
 * @IN s: year field string
 * @Return: how many chars to read
 * @See also:
 */
template <bool optimized>
static int dch_parse_Y_YYY(const char* s, TmFromChar* out, FormatNode* node, bool iterance)
{
    int years = 0;
    int millenia = 0;
    int nch = 0;
    int i = 0;

    /* pattern "Y,YYY" require its string length must be 5. */
    int const fixed_len = 5;

    for (i = 0; i < fixed_len; i++) {
        if (i == 1 && s[i] == ',') {
            continue;
        } else if (s[i] >= '0' && s[i] <= '9') {
            continue;
        } else {
            break;
        }
    }
    if (i < fixed_len) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_DATETIME_FORMAT), errmsg("invalid input string for \"Y,YYY\"")));
    }

    /* %n indicates the number of characters written so far. */
    if (sscanf_s(s, "%d,%03d%n", &millenia, &years, &nch) < 2)
        ereport(ERROR, (errcode(ERRCODE_INVALID_DATETIME_FORMAT), errmsg("invalid input string for \"Y,YYY\"")));

    /* set YEAR value */
    if (optimized) {
        out->year = years + (millenia * 1000);
    } else {
        years += (millenia * 1000);
        from_char_set_int(&out->year, years, node, iterance);
    }
    out->yysz = 4;

    /* return the number of consumed chars */
    return nch;
}

/*
 * @Description: define function pointer proto-type. function implements for
 *               each pattern field will be listed following.
 * @IN node: FormatNode info about this pattern field.
 * @OUT out: returned value will be recorded in this argument. for different
 *           field, different struct member will be used. for example,
 *            YEAR --> out->year
 *            HOUR --> out->hh
 * @IN/OUT src_str: input date string about this pattern field. and it will
 *                  move ahead after this field is done.
 * @IN tm_const: constraint info about this pattern field.
 * @See also: parse_field_map[] and functions parse_field_xxxx()
 */
typedef void (*parse_field)(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out);

static void parse_field_ampm_long(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    int tmp_val = 0;
    (void)from_char_seq_search(&tmp_val, src_str, ampm_strings_long, ALL_UPPER, node->key->len, node);
    out->pm = tmp_val % 2;
    out->clock = CLOCK_12_HOUR;
    UNUSED_ARG(tm_const);
}

static void parse_field_ampm(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    int tmp_val = 0;
    (void)from_char_seq_search(&tmp_val, src_str, ampm_strings, ALL_UPPER, node->key->len, node);
    out->pm = tmp_val % 2;
    out->clock = CLOCK_12_HOUR;
    UNUSED_ARG(tm_const);
}

static void parse_field_adbc_long(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    int tmp_val = 0;
    (void)from_char_seq_search(&tmp_val, src_str, adbc_strings_long, ALL_UPPER, node->key->len, node);
    out->bc = tmp_val % 2;
    UNUSED_ARG(tm_const);
}

static void parse_field_adbc(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    int tmp_val = 0;
    (void)from_char_seq_search(&tmp_val, src_str, adbc_strings, ALL_UPPER, node->key->len, node);
    out->bc = tmp_val % 2;
    UNUSED_ARG(tm_const);
}

static void parse_field_hh12(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    (void)optimized_parse_int_len(&out->hh, src_str, 2, node, tm_const);
    out->clock = CLOCK_12_HOUR;
}

static void parse_field_hh24(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    (void)optimized_parse_int_len(&out->hh, src_str, 2, node, tm_const);
    out->clock = CLOCK_24_HOUR;
}

static void parse_field_mi(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    (void)optimized_parse_int(&out->mi, src_str, node, tm_const);
}

static void parse_field_ss(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    (void)optimized_parse_int(&out->ss, src_str, node, tm_const);
}

static void parse_field_ms(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    int tmp_len = optimized_parse_int_len(&out->ms, src_str, 3, node, tm_const);

    /* 25 is 0.25 and 250 is 0.25 too; 025 is 0.025 and not 0.25 */
    out->ms *= ms_multi_factor[Min(tmp_len, 3)];
}

template <int accuracy>
static void parse_field_usffn(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    int tmp_len = optimized_parse_int_len(&out->us, src_str, accuracy, node, tm_const);
    if (tmp_len != accuracy) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("The raw data length is not match.")));
    }
    out->us *= us_multi_factor[tmp_len];
}

static void parse_field_usff(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    int tmp_len = optimized_parse_int_len(&out->us, src_str, 6, node, tm_const);
    /*
     * tmp_len is the real number of digits exluding head spaces.
     * we have checked US value validation and make that
     * tmp_len is between 1 and 6.
     */
    if (tmp_len < 1 || tmp_len > 6) {
        ereport(ERROR,
            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("The raw data length is not match.")));
    }
    out->us *= us_multi_factor[tmp_len];
}

static void parse_field_sssss(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    (void)optimized_parse_int(&out->sssss, src_str, node, tm_const);
}

static void parse_field_mm_full(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    (void)from_char_seq_search(&out->mm, src_str, months_full, ONE_UPPER, MAX_MONTH_LEN, node);
    ++out->mm;
    UNUSED_ARG(tm_const);
}

static void parse_field_mm_short(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    (void)from_char_seq_search(&out->mm, src_str, months, ONE_UPPER, MAX_MON_LEN, node);
    ++out->mm;
    UNUSED_ARG(tm_const);
}

static void parse_field_mm(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    (void)optimized_parse_int(&out->mm, src_str, node, tm_const);
}

static void parse_field_d(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    (void)from_char_seq_search(&out->d, src_str, days, ONE_UPPER, MAX_DAY_LEN, node);
    UNUSED_ARG(tm_const);
}

static void parse_field_ddd(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    (void)optimized_parse_int(&out->ddd, src_str, node, tm_const);
}

static void parse_field_iddd(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    (void)optimized_parse_int_len(&out->ddd, src_str, 3, node, tm_const);
}

static void parse_field_dd(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    (void)optimized_parse_int(&out->dd, src_str, node, tm_const);
}

static void parse_field_d_int(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    (void)optimized_parse_int(&out->d, src_str, node, tm_const);
    out->d--;
}

static void parse_field_id(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    (void)optimized_parse_int_len(&out->d, src_str, 1, node, tm_const);
}

static void parse_field_yyyy(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    (void)optimized_parse_int(&out->year, src_str, node, tm_const);
    out->yysz = 4;
}

static void parse_field_syyyy(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    (void)optimized_parse_int(&out->year, src_str, node, tm_const);
    if (0 == out->year) {
        ereport(ERROR,
            (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                errmsg("invalid data for \"year = 0\" ,"
                       "value must be between -4712 and 9999, and not be 0")));
    }
    out->yysz = 4;
}

static void parse_field_cc(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    (void)optimized_parse_int(&out->cc, src_str, node, tm_const);
}

static void parse_field_iw(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    (void)optimized_parse_int(&out->ww, src_str, node, tm_const);
}

static void parse_field_yyy(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    if (optimized_parse_int(&out->year, src_str, node, tm_const) < 4) {
        out->year = adjust_partial_year_to_2020(out->year);
    }
    out->yysz = 3;
}

static void parse_field_yy(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    if (optimized_parse_int(&out->year, src_str, node, tm_const) < 4) {
        out->year = adjust_partial_year_to_2020(out->year);
    }
    out->yysz = 2;
}

static void parse_field_y(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    if (optimized_parse_int(&out->year, src_str, node, tm_const) < 4) {
        out->year = adjust_partial_year_to_2020(out->year);
    }
    out->yysz = 1;
}

static void parse_field_j(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    (void)optimized_parse_int(&out->j, src_str, node, tm_const);
}

static void parse_field_q(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    int tmp_val = 0;
    /* don't care returned tmp_val, and just let src_str move ahead */
    (void)optimized_parse_int(&tmp_val, src_str, node, tm_const);
}

static void parse_field_RM(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    int tmp_val = 0;
    (void)from_char_seq_search(&tmp_val, src_str, rm_months_upper, ALL_UPPER, MAX_RM_LEN, node);
    out->mm = MONTHS_PER_YEAR - tmp_val;
    UNUSED_ARG(tm_const);
}

static void parse_field_rm(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    int tmp_val = 0;
    (void)from_char_seq_search(&tmp_val, src_str, rm_months_lower, ALL_LOWER, MAX_RM_LEN, node);
    out->mm = MONTHS_PER_YEAR - tmp_val;
    UNUSED_ARG(tm_const);
}

static void parse_field_rrrr(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    /* to adapt RRRR time_str A db */
    if (optimized_parse_int(&out->year, src_str, node, tm_const) < 3) {
        out->year = adjust_partial_year_to_current_year<false, true>(out->year);
    }
    out->yysz = 4;
}

static void parse_field_rr(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    if (optimized_parse_int(&out->year, src_str, node, tm_const) < 3) {
        out->year = adjust_partial_year_to_current_year<false, true>(out->year);
    }
    out->yysz = 2;
}

static void parse_field_w(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    (void)optimized_parse_int(&out->w, src_str, node, tm_const);
}

static void parse_field_y_yyy(FormatNode* node, TmFormatConstraint* tm_const, char** src_str, TmFromChar* out)
{
    *src_str += dch_parse_Y_YYY<true>(*src_str, out, node, false);
    UNUSED_ARG(tm_const);
}

static const parse_field parse_field_map[] = {
    /* -----  0~9  ----- */
    parse_field_adbc_long, /* DCH_A_D   */
    parse_field_ampm_long, /* DCH_A_M   */
    parse_field_adbc,      /* DCH_AD    */
    parse_field_ampm,      /* DCH_AM    */
    parse_field_adbc_long, /* DCH_B_C   */
    parse_field_adbc,      /* DCH_BC    */
    parse_field_cc,        /* DCH_CC    */
    parse_field_d,         /* DCH_DAY   */
    parse_field_ddd,       /* DCH_DDD   */
    parse_field_dd,        /* DCH_DD    */

    /* -----  10~19  ----- */
    parse_field_d,     /* DCH_DY    */
    parse_field_d,     /* DCH_Day   */
    parse_field_d,     /* DCH_Dy    */
    parse_field_d_int, /* DCH_D     */
    parse_field_usffn<1>,  /* DCH_FF1   */
    parse_field_usffn<2>,  /* DCH_FF2   */
    parse_field_usffn<3>,  /* DCH_FF3   */
    parse_field_usffn<4>,  /* DCH_FF4   */
    parse_field_usffn<5>,  /* DCH_FF5   */
    parse_field_usffn<6>,  /* DCH_FF6   */

    /* -----  20~29  ----- */
    parse_field_usff,    /* DCH_FF    */
    NULL,                /* DCH_FX    */
    parse_field_hh24,    /* DCH_HH24  */
    parse_field_hh12,    /* DCH_HH12  */
    parse_field_hh12,    /* DCH_HH    */
    parse_field_iddd,    /* DCH_IDDD  */
    parse_field_id,      /* DCH_ID    */
    parse_field_iw,      /* DCH_IW    */
    parse_field_yyyy,    /* DCH_IYYY  */
    parse_field_yyy,     /* DCH_IYY   */

    /* -----  30~39  ----- */
    parse_field_yy,        /* DCH_IY    */
    parse_field_y,         /* DCH_I     */
    parse_field_j,         /* DCH_J     */
    parse_field_mi,        /* DCH_MI    */
    parse_field_mm,        /* DCH_MM    */
    parse_field_mm_full,   /* DCH_MONTH */
    parse_field_mm_short,  /* DCH_MON   */
    parse_field_ms,        /* DCH_MS    */
    parse_field_mm_full,   /* DCH_Month */
    parse_field_mm_short,  /* DCH_Mon   */

    /* -----  40~49  ----- */
    parse_field_ampm_long, /* DCH_P_M   */
    parse_field_ampm,      /* DCH_PM    */
    parse_field_q,         /* DCH_Q     */
    parse_field_RM,        /* DCH_RM    */
    parse_field_rrrr,      /* DCH_RRRR  */
    parse_field_rr,        /* DCH_RR    */
    parse_field_sssss,     /* DCH_SSSSS */
    parse_field_ss,        /* DCH_SS    */
    parse_field_syyyy,     /* DCH_SYYYY */
    NULL,                  /* DCH_TZ    */

    /* -----  50~59  ----- */
    parse_field_usff,      /* DCH_US    */
    parse_field_iw,        /* DCH_WW    */
    parse_field_w,         /* DCH_W     */
    NULL,                  /* DCH_X     */
    parse_field_y_yyy,     /* DCH_Y_YYY */
    parse_field_yyyy,      /* DCH_YYYY  */
    parse_field_yyy,       /* DCH_YYY   */
    parse_field_yy,        /* DCH_YY    */
    parse_field_y,         /* DCH_Y     */
    parse_field_adbc_long, /* DCH_a_d   */

    /* -----  60~69  ----- */
    parse_field_ampm_long, /* DCH_a_m   */
    parse_field_adbc,      /* DCH_ad    */
    parse_field_ampm,      /* DCH_am    */
    parse_field_adbc_long, /* DCH_b_c   */
    parse_field_adbc,      /* DCH_bc    */
    parse_field_cc,        /* DCH_cc    */
    parse_field_d,         /* DCH_day   */
    parse_field_ddd,       /* DCH_ddd   */
    parse_field_dd,        /* DCH_dd    */
    parse_field_d,         /* DCH_dy    */

    /* -----  70~79  ----- */
    parse_field_d_int, /* DCH_d     */
    parse_field_usffn<1>,  /* DCH_ff1   */
    parse_field_usffn<2>,  /* DCH_ff2   */
    parse_field_usffn<3>,  /* DCH_ff3   */
    parse_field_usffn<4>,  /* DCH_ff4   */
    parse_field_usffn<5>,  /* DCH_ff5   */
    parse_field_usffn<6>,  /* DCH_ff6   */
    parse_field_usff,  /* DCH_ff    */
    NULL,              /* DCH_fx    */
    parse_field_hh24,  /* DCH_hh24  */

    /* -----  80~89  ----- */
    parse_field_hh12,    /* DCH_hh12  */
    parse_field_hh12,    /* DCH_hh    */
    parse_field_iddd,    /* DCH_iddd  */
    parse_field_id,      /* DCH_id    */
    parse_field_iw,      /* DCH_iw    */
    parse_field_yyyy,    /* DCH_iyyy  */
    parse_field_yyy,     /* DCH_iyy   */
    parse_field_yy,      /* DCH_iy    */
    parse_field_y,       /* DCH_i     */
    parse_field_j,       /* DCH_j     */

    /* -----  90~99  ----- */
    parse_field_mi,        /* DCH_mi    */
    parse_field_mm,        /* DCH_mm    */
    parse_field_mm_full,   /* DCH_month */
    parse_field_mm_short,  /* DCH_mon   */
    parse_field_ms,        /* DCH_ms    */
    parse_field_ampm_long, /* DCH_p_m   */
    parse_field_ampm,      /* DCH_pm    */
    parse_field_q,         /* DCH_q     */
    parse_field_rm,        /* DCH_rm    */
    parse_field_rrrr,      /* DCH_rrrr  */

    /* ----  100~110  ---- */
    parse_field_rr,        /* DCH_rr    */
    parse_field_sssss,     /* DCH_sssss */
    parse_field_ss,        /* DCH_ss    */
    parse_field_syyyy,     /* DCH_syyyy */
    NULL,                  /* DCH_tz    */
    parse_field_usff,      /* DCH_us    */
    parse_field_iw,        /* DCH_ww    */
    parse_field_w,         /* DCH_w     */
    NULL,                  /* DCH_x     */
    parse_field_y_yyy,     /* DCH_y_yyy */

    /* -----  110~  ----- */
    parse_field_yyyy,  /* DCH_yyyy  */
    parse_field_yyy,   /* DCH_yyy   */
    parse_field_yy,    /* DCH_yy    */
    parse_field_y,     /* DCH_y     */

    NULL               /* _DCH_last_ */
};

/*
 * @Description: optimized version of DCH_from_char().
 *               in this function we don't handle
 *               1. FILL Mode
 *               2. FIXED WIDTH Mode
 *               3. TH/th Mode
 * @IN tm_const: constraint of this format node.
 * @IN time_str: input time string
 * @IN nodes: input time pattern
 * @OUT out: time info extracted from input time string.
 * @Return: true if time string is matched with time format pattern;
 *          false if time string is not matched with time format pattern.
 * @See also: DCH_from_char().
 */
static bool optimized_dch_from_char(FormatNode* nodes, char* time_str, TmFromChar* out, TmFormatConstraint* tm_const)
{
    FormatNode* node = nodes;
    char* tm_str = time_str;

    for (; node->type != NODE_TYPE_END && *tm_str != '\0'; ++node, ++tm_const) {
        if (node->type != NODE_TYPE_ACTION) {
            if (*tm_str == node->character) {
                tm_str++;
                continue;
            } else {
                ereport(ERROR,
                    (errcode(ERRCODE_INVALID_DATETIME_FORMAT), errmsg("character does not match format string")));
            }
        }

        /* Ignore spaces before fields when not time_str FX (fixed width) mode */
        while (*tm_str != '\0' && isspace((unsigned char)*tm_str))
            tm_str++;

        /* parse each field of this pattern */
        Assert(node->key->id >= DCH_A_D && node->key->id < _DCH_last_);
        Assert(parse_field_map[node->key->id]);
        (*parse_field_map[node->key->id])(node, tm_const, &tm_str, out);
    }

    return (*tm_str ? true : false);
}

/*
 * @Description: Given date/timestamp string and its format pattern info,
 *               parse and get its PG time value and corresponding flags.
 *               this is an optimized function, which is more faster than
 *               function general_to_timestamp_from_user_format().
 * @IN date_str: date/timestamp string, like '2016-11-14 13:48:20'
 * @IN/OUT fsec: fsec_t flag about PG time
 * @IN in_format: date/timestamp format pattern, like 'YYYY-MM-DD HH24:MI:SS'.
 *               this argument is gotten by calling get_time_format().
 * @IN/OUT tm:   corresponding PG time value
 * @See also:
 */
#ifndef ENABLE_UT
static
#endif
    void
    optimized_to_timestamp_from_user_format(struct pg_tm* tm, fsec_t* fsec, char* date_str, void* in_format)
{
    TimeFormatInfo* tm_fmt_info = (TimeFormatInfo*)in_format;

    int invalid_value = 0;
    errno_t rc = EOK;
    char invalid_format[DCH_CACHE_SIZE + 1];
    char valid_range[DCH_CACHE_SIZE + 1];

    TmFromChar tmfc;
    /* reset tmfc */
    rc = memset_s(&tmfc, sizeof(TmFromChar), 0, sizeof(TmFromChar));
    securec_check(rc, "\0", "\0");
    tmfc.mode = tm_fmt_info->mode; /* set mode info from TimeFormatInfo */

    /* parse this time string by given formatting node */
    if (optimized_dch_from_char(tm_fmt_info->tm_format, date_str, &tmfc, tm_fmt_info->tm_constraint)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_DATETIME_FORMAT), errmsg("invalid data for match	in date string")));
    }

    rc = memset_s(valid_range, sizeof(valid_range), 0, DCH_CACHE_SIZE + 1);
    securec_check(rc, "\0", "\0");
    rc = memset_s(invalid_format, sizeof(invalid_format), 0, DCH_CACHE_SIZE + 1);
    securec_check(rc, "\0", "\0");

    /* check valid of timestamp value */
    if (optimized_dck_check(&tmfc, &tm_fmt_info->tm_flags, invalid_format, &invalid_value, valid_range)) {
        ereport(ERROR,
            (errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
                errmsg("invalid data for \"%s =  %d\" ,value must be %s", invalid_format, invalid_value, valid_range)));
    }

    /* reset tm && fsec */
    *fsec = 0;
    ZERO_tm(tm);
    convert_values_to_pgtime(tm, fsec, tmfc, tm_fmt_info->tm_flags);
}

/*
 * @Description: check the same field of given time format pattern is set repeated.
 *               for each node of given time format pattern, it's only permitted
 *               set once. if this rule is broken, report error.
 * @IN iterance: whether this node value is set repeated.
 * @IN node: which field of FormatNode to be set.
 * @See also:
 */
static inline void check_format_field_iterance(FormatNode* node, bool iterance)
{
    if (iterance) {
        ERROR_FIELD_VALUE_HAS_BEEN_SET(node->key->name);
    }
}

/*
 * @Description: Given timestamp format pattern(see TimeFormatInfo::tm_format),
 *               set its corresponding function. Because FX/TH/th/FILL mode is not
 *               common, so we use optimized_to_timestamp_from_user_format() function
 *               to handle allmost common cases, which is much faster than the other
 *               function general_to_timestamp_from_user_format().
 * @IN tm_fmt_info: time format pattern holding FormatNode chain.
 * @See also:
 */
static void init_time_format_info(TimeFormatInfo* tm_fmt_info)
{
    int rc = 0;

    /* Must mode to be FROM_CHAR_DATE_NONE.
     * see tmfc in do_to_timestamp().
     */
    TmFromChar tm_mode;
    TmFromChar* tm_mode_ptr = &tm_mode;
    rc = memset_s(tm_mode_ptr, sizeof(TmFromChar), 0, sizeof(TmFromChar));
    securec_check(rc, "\0", "\0");

    /* init TimeFormatInfo::tm_flags here. see also do_to_timestamp() */
    TmFromCharFlag* tm_flag_ptr = &tm_fmt_info->tm_flags;
    rc = memset_s(tm_flag_ptr, sizeof(TmFromCharFlag), 0, sizeof(TmFromCharFlag));
    securec_check(rc, "\0", "\0");

    TmFormatConstraint* tm_const = tm_fmt_info->tm_constraint;
    FormatNode* node = tm_fmt_info->tm_format;

    /* check this time format is in FX (fixed width) mode. */
    bool fx_mode = false;

    /* check this time format has any th/TH suffix */
    bool any_th_suffix = false;

    /* check this time format has any FM prefix */
    bool any_fill_prefix = false;

    for (; node->type != NODE_TYPE_END; ++node, ++tm_const) {
        tm_const->next_sep = false;

        /* valid range is >= INT_MIN and <= INT_MAX.
         * see function from_char_parse_int_len() body.
         */
        tm_const->min_val = INT_MIN;
        tm_const->max_val = INT_MAX;

        if (node->type != NODE_TYPE_ACTION) {
            continue;
        }

        /* check and set date mode. */
        from_char_set_mode(tm_mode_ptr, node->key->date_mode);

        /* set is-next-separator flag for this FormatNode.
         * because this flag is used by from_char_parse_int() or
         * from_char_parse_int_len(), some FormatNode can ignore this flag
         * even this flag has been set, if it doesn't call any one of the two
         * functions. for the other callers, just set this flag correctly.
         */
        tm_const->next_sep = is_next_separator(node);

        /* FM suppresses leading zeroes and trailing blanks that would otherwise
         * be added to make the output of a pattern be fixed-width. In openGauss,
         * FM modifies only the next specification. this information can be fetch
         * from time format pattern.
         */
        if (!any_fill_prefix) {
            any_fill_prefix = S_FM(node->suffix);
        }

        switch (node->key->id) {
            case DCH_FX:
                /*
                 * FX must be specified as the first item in the template.
                 * So we can know whether it's in FX mode or not from the
                 * first FormatNode info.
                 */
                fx_mode = true;
                break;
            case DCH_A_M: /* AM/PM info */
            case DCH_P_M:
            case DCH_a_m:
            case DCH_p_m:
            case DCH_AM:
            case DCH_PM:
            case DCH_am:
            case DCH_pm:
                check_format_field_iterance(node, tm_flag_ptr->pm_flag);
                tm_flag_ptr->clock_flag = true;
                tm_flag_ptr->pm_flag = true;
                /* AM/PM doesn't have th/TH suffix */
                break;
            case DCH_HH: /* HH12 info */
            case DCH_HH12:
                check_format_field_iterance(node, tm_flag_ptr->hh_flag);
                tm_flag_ptr->clock_flag = true;
                tm_flag_ptr->hh_flag = true;
                tm_const->min_val = MIN_VALUE_12_CLOCK;
                tm_const->max_val = MAX_VALUE_12_CLOCK;
                if (!any_th_suffix)
                    any_th_suffix = S_THth(node->suffix);
                break;
            case DCH_HH24: /* HH24 info */
                check_format_field_iterance(node, tm_flag_ptr->hh_flag);
                tm_flag_ptr->clock_flag = true;
                tm_flag_ptr->hh_flag = true;
                tm_const->min_val = MIN_VALUE_24_CLOCK;
                tm_const->max_val = MAX_VALUE_24_CLOCK;
                if (!any_th_suffix)
                    any_th_suffix = S_THth(node->suffix);
                break;
            case DCH_MI: /* MI info */
                check_format_field_iterance(node, tm_flag_ptr->mi_flag);
                tm_flag_ptr->mi_flag = true;
                tm_const->min_val = MIN_VALUE_MI;
                tm_const->max_val = MAX_VALUE_MI;
                if (!any_th_suffix)
                    any_th_suffix = S_THth(node->suffix);
                break;
            case DCH_SS:
                check_format_field_iterance(node, tm_flag_ptr->ss_flag);
                tm_flag_ptr->ss_flag = true;
                tm_const->min_val = MIN_VALUE_SS;
                tm_const->max_val = MAX_VALUE_SS;
                if (!any_th_suffix)
                    any_th_suffix = S_THth(node->suffix);
                break;
            case DCH_MS: /* millisecond info */
                check_format_field_iterance(node, tm_flag_ptr->ms_flag);
                tm_flag_ptr->ms_flag = true;
                tm_const->min_val = MIN_VALUE_MS;
                tm_const->max_val = MAX_VALUE_MS;
                if (!any_th_suffix)
                    any_th_suffix = S_THth(node->suffix);
                break;
            case DCH_US: /* microsecond */
                check_format_field_iterance(node, tm_flag_ptr->us_flag);
                tm_flag_ptr->us_flag = true;
                tm_const->min_val = MIN_VALUE_US;
                tm_const->max_val = MAX_VALUE_US;
                if (!any_th_suffix)
                    any_th_suffix = S_THth(node->suffix);
                break;
            case DCH_SSSSS:
            case DCH_sssss:
                check_format_field_iterance(node, tm_flag_ptr->sssss_flag);
                tm_flag_ptr->sssss_flag = true;
                tm_const->min_val = MIN_VALUE_SSSSS;
                tm_const->max_val = MAX_VALUE_SSSSS;
                if (!any_th_suffix)
                    any_th_suffix = S_THth(node->suffix);
                break;
            case DCH_A_D:
            case DCH_B_C:
            case DCH_a_d:
            case DCH_b_c:
            case DCH_AD:
            case DCH_BC:
            case DCH_ad:
            case DCH_bc:
                check_format_field_iterance(node, tm_flag_ptr->bc_flag);
                tm_flag_ptr->bc_flag = true;
                /* AD/BC doesn't have th/TH suffix */
                break;
            case DCH_MONTH:
            case DCH_Month:
            case DCH_month:
            case DCH_MON:
            case DCH_Mon:
            case DCH_mon:
            case DCH_MM:
                check_format_field_iterance(node, tm_flag_ptr->mm_flag);
                tm_flag_ptr->mm_flag = true;
                tm_const->min_val = MIN_VALUE_MM;
                tm_const->max_val = MAX_VALUE_MM;
                if (!any_th_suffix)
                    any_th_suffix = S_THth(node->suffix);
                break;
            case DCH_DAY:
            case DCH_Day:
            case DCH_day:
            case DCH_DY:
            case DCH_Dy:
            case DCH_dy:
            case DCH_D:
            case DCH_ID:
                check_format_field_iterance(node, tm_flag_ptr->d_flag);
                tm_flag_ptr->d_flag = true;
                tm_const->min_val = MIN_VALUE_D;
                tm_const->max_val = MAX_VALUE_D;
                if (!any_th_suffix)
                    any_th_suffix = S_THth(node->suffix);
                break;
            case DCH_DDD:
            case DCH_IDDD:
                check_format_field_iterance(node, tm_flag_ptr->ddd_flag);
                tm_flag_ptr->ddd_flag = true;
                tm_const->min_val = MIN_VALUE_DDD;
                tm_const->max_val = MAX_VALUE_DDD;
                if (!any_th_suffix)
                    any_th_suffix = S_THth(node->suffix);
                break;
            case DCH_DD:
                check_format_field_iterance(node, tm_flag_ptr->dd_flag);
                tm_flag_ptr->dd_flag = true;
                tm_const->min_val = MIN_VALUE_DD;
                tm_const->max_val = MAX_VALUE_DD;
                if (!any_th_suffix)
                    any_th_suffix = S_THth(node->suffix);
                break;
            case DCH_WW:
            case DCH_IW:
                check_format_field_iterance(node, tm_flag_ptr->ww_flag);
                tm_flag_ptr->ww_flag = true;
                tm_const->min_val = MIN_VALUE_WW;
                tm_const->max_val = MAX_VALUE_WW;
                if (!any_th_suffix)
                    any_th_suffix = S_THth(node->suffix);
                break;
            case DCH_CC:
                check_format_field_iterance(node, tm_flag_ptr->cc_flag);
                tm_flag_ptr->cc_flag = true;
                if (!any_th_suffix)
                    any_th_suffix = S_THth(node->suffix);
                break;
            case DCH_Y_YYY:
            case DCH_YYYY:
            case DCH_IYYY:
            case DCH_YYY:
            case DCH_IYY:
            case DCH_YY:
            case DCH_IY:
            case DCH_Y:
            case DCH_I:
                check_format_field_iterance(node, tm_flag_ptr->year_flag);
                tm_flag_ptr->year_flag = true;
                tm_flag_ptr->yysz_flag = true;
                if (DCH_I == node->key->id || DCH_YY == node->key->id || DCH_IYY == node->key->id ||
                    DCH_Y == node->key->id || DCH_IY == node->key->id || DCH_YYY == node->key->id) {
                    /* value 0 about these IDs is allowed because real year value will be
                     * adjusted and returned by function adjust_partial_year_to_2020().
                     */
                    tm_const->min_val = 0;
                } else {
                    tm_const->min_val = 1;
                }
                tm_const->max_val = MAX_VALUE_YEAR;
                if (!any_th_suffix)
                    any_th_suffix = S_THth(node->suffix);
                break;
            case DCH_RM:
            case DCH_rm:
                check_format_field_iterance(node, tm_flag_ptr->mm_flag);
                tm_flag_ptr->mm_flag = true;
                tm_const->min_val = MIN_VALUE_MM;
                tm_const->max_val = MAX_VALUE_MM;
                /* RM/rm doesn't have th/TH suffix */
                break;
            case DCH_W:
                check_format_field_iterance(node, tm_flag_ptr->w_flag);
                tm_flag_ptr->w_flag = true;
                tm_const->min_val = MIN_VALUE_W;
                tm_const->max_val = MAX_VALUE_W;
                if (!any_th_suffix)
                    any_th_suffix = S_THth(node->suffix);
                break;
            case DCH_J:
                check_format_field_iterance(node, tm_flag_ptr->j_flag);
                tm_flag_ptr->j_flag = true;
                tm_const->min_val = MIN_VALUE_J;
                tm_const->max_val = MAX_VALUE_J;
                if (!any_th_suffix)
                    any_th_suffix = S_THth(node->suffix);
                break;
            case DCH_FF:
                /* to adapt FF in A db.
                 * there is no digit after FF ,we specify 6 acquiescently.
                 */
                check_format_field_iterance(node, tm_flag_ptr->us_flag);
                tm_flag_ptr->us_flag = true;
                if (!any_th_suffix)
                    any_th_suffix = S_THth(node->suffix);
                break;
            case DCH_RRRR:
            case DCH_RR:
                /* to adapt RRRR in A db  */
                check_format_field_iterance(node, tm_flag_ptr->year_flag);
                tm_flag_ptr->year_flag = true;
                tm_flag_ptr->yysz_flag = true;

                /* optimized_parse_int() will use the two values for input number.
                 * adjust_partial_year_to_current_year() will adjust this value,
                 * and re-check its new range.
                 */
                tm_const->min_val = 0;
                tm_const->max_val = MAX_VALUE_YEAR;
                if (!any_th_suffix)
                    any_th_suffix = S_THth(node->suffix);
                break;
            case DCH_SYYYY:
                check_format_field_iterance(node, tm_flag_ptr->year_flag);
                tm_flag_ptr->year_flag = true;
                tm_flag_ptr->yysz_flag = true;

                /*
                 * this pattern's valid range is in [ MIN_VALUE_YEAR, -1]
                 * or [1, MAX_VALUE_YEAR). special 0 will be check later.
                 */
                tm_const->min_val = MIN_VALUE_YEAR;
                tm_const->max_val = MAX_VALUE_YEAR;
                if (!any_th_suffix)
                    any_th_suffix = S_THth(node->suffix);
                break;
            case DCH_tz:
            case DCH_TZ:
                ERROR_NOT_SUPPORT_TZ();
                break;
            case DCH_Q:
                /*
                 * We ignore 'Q' when converting to date because it is unclear
                 * which date in the quarter to use, and some people specify
                 * both quarter and month, so if it was honored it might
                 * conflict with the supplied month. That is also why we don't
                 * throw an error.
                 *
                 * We still parse the source string for an integer, but it
                 * isn't stored anywhere in 'tm_mode_ptr'.
                 *
                 * Q doesn't have th/TH suffix
                 */
            default:
                /* parse_format() ensures that this branch is not touched */
                break;
        }
    }

    /* set MODE info */
    tm_fmt_info->mode = tm_mode_ptr->mode;

    if (!any_fill_prefix && !any_th_suffix && !fx_mode) {
        tm_fmt_info->tm_func = &optimized_to_timestamp_from_user_format;
    } else {
        tm_fmt_info->tm_func = &general_to_timestamp_from_user_format;
    }
}

/*
 * @Description: Given time format pattern, parse and get its TimeFormatInfo.
 *               Always its returned value will be passed in to_timestamp_from_format().
 * @IN tm_format_str: date/timestamp format pattern, like 'YYYY-MM-DD HH24:MI:SS'.
 * @See also: get_format()
 * Return:
 */
void* get_time_format(char* fmt_pattern)
{
    TimeFormatInfo* tm_info = NULL;
    FormatNode* tm_nodes = NULL;
    int tm_nodes_maxnum = 0;

    /* we ignore output arguments of get_format(),
     * so use a local var to handle this case.
     */
    bool in_cache = false;

    /* the number is max when each char represents one node pattern */
    tm_nodes_maxnum = strlen(fmt_pattern);
    tm_nodes = get_format(fmt_pattern, tm_nodes_maxnum, &in_cache);

    /* struct TimeFormatInfo + tm_nodes_maxnum * TmFormatConstraint */
    tm_info = (TimeFormatInfo*)palloc(sizeof(TimeFormatInfo) + sizeof(TmFormatConstraint) * tm_nodes_maxnum);

    tm_info->tm_format = tm_nodes;
    tm_info->max_num = tm_nodes_maxnum;
    tm_info->tm_constraint = (TmFormatConstraint*)((char*)tm_info + sizeof(TimeFormatInfo));

    /* fill the other informations about TimeFormatInfo */
    init_time_format_info(tm_info);

    return (void*)tm_info;
}

/*
 * @Description: Given date/timestamp string and its format pattern info,
 *               parse and get its PG time value and corresponding flags.
 * @IN date_str: date/timestamp string, like '2016-11-14 13:48:20'
 * @IN/OUT fsec: fsec_t flag about PG time
 * @IN in_format: date/timestamp format pattern, like 'YYYY-MM-DD HH24:MI:SS'.
 *               this argument is gotten by calling get_time_format().
 * @IN/OUT tm:   corresponding PG time value
 * @See also:
 */
void to_timestamp_from_format(struct pg_tm* tm, fsec_t* fsec, char* date_str, void* in_format)
{
    TimeFormatInfo* tm_info = (TimeFormatInfo*)in_format;
    (*tm_info->tm_func)(tm, fsec, date_str, in_format);
}
