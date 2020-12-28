package Mkvcbuild;

#
# Package that generates build files for msvc build
#
# src/tools/msvc/Mkvcbuild.pm
#
use Carp;
use Win32;
use strict;
use warnings;
use Project;
use Solution;
use Cwd;
use File::Copy;
use Config;
use VSObjectFactory;
use List::Util qw(first);

use Exporter;
our (@ISA, @EXPORT_OK);
@ISA       = qw(Exporter);
@EXPORT_OK = qw(Mkvcbuild);

my $solution;
my $libpgport;
my $postgres;
my $libpq;

my $contrib_defines = { 'refint' => 'REFINT_VERBOSE' };
my @contrib_uselibpq =
  ('dblink', 'oid2name', 'pgbench', 'pg_upgrade', 'vacuumlo');
my @contrib_uselibpgport = (
	'oid2name',      'pgbench',
	'pg_standby',    'pg_archivecleanup',
	'pg_test_fsync', 'pg_test_timing',
	'pg_upgrade',    'vacuumlo');
my $contrib_extralibs = { 'pgbench' => ['wsock32.lib'] };
my $contrib_extraincludes =
  { 'tsearch2' => ['contrib/tsearch2'], 'dblink' => ['src/common/backend'] };
my $contrib_extrasource = {
	'cube' => [ 'cubescan.l', 'cubeparse.y' ],
	'seg'  => [ 'segscan.l',  'segparse.y' ] };
my @contrib_excludes = ('pgcrypto', 'intagg', 'sepgsql');

sub mkvcbuild
{
	our $config = shift;

	chdir('..\..\..') if (-d '..\msvc' && -d '..\..\..\src');
	die 'Must run from root or msvc directory'
	  unless (-d 'src\tools\msvc' && -d 'src');

	my $vsVersion = DetermineVisualStudioVersion();

	$solution = CreateSolution($vsVersion, $config);

	our @pgportfiles = qw(
	  chklocale.cpp crypt.cpp fls.cpp fseeko.cpp getrusage.cpp inet_aton.cpp random.cpp
	  srandom.cpp getaddrinfo.cpp gettimeofday.cpp inet_net_ntop.cpp kill.cpp open.cpp
	  erand48.cpp snprintf.cpp strlcat.cpp strlcpy.cpp dirmod.cpp exec.cpp noblock.cpp path.cpp
	  pgcheckdir.cpp pgmkdirp.cpp pgsleep.cpp pgstrcasecmp.cpp qsort.cpp qsort_arg.cpp
	  sprompt.cpp thread.cpp getopt.cpp getopt_long.cpp dirent.cpp rint.cpp win32env.cpp
	  win32error.cpp win32setlocale.cpp);

	$libpgport = $solution->AddProject('libpgport', 'lib', 'misc');
	$libpgport->AddDefine('FRONTEND');
	$libpgport->AddFiles('src\common\port', @pgportfiles);

	$postgres = $solution->AddProject('postgres', 'exe', '', 'src\gausskernel');	
	$postgres->AddIncludeDir('src\common\backend');
	$postgres->AddDir('src\common\backend\port\win32');
	$postgres->AddFile('src\common\backend\utils\fmgrtab.cpp');
	$postgres->ReplaceFile(
		'src\common\backend\port\dynloader.cpp',
		'src\common\backend\port\dynloader\win32.cpp');
	$postgres->ReplaceFile('src\common\backend\port\pg_sema.cpp',
		'src\common\backend\port\win32_sema.cpp');
	$postgres->ReplaceFile('src\common\backend\port\pg_shmem.cpp',
		'src\common\backend\port\win32_shmem.cpp');
	$postgres->ReplaceFile('src\common\backend\port\pg_latch.cpp',
		'src\common\backend\port\win32_latch.cpp');
	$postgres->AddFiles('src\common\port', @pgportfiles);
	$postgres->AddDir('src\common\timezone');
	$postgres->AddFiles('src\common\backend\parser', 'scan.l', 'gram.y');
	$postgres->AddFiles('src\common\backend\bootstrap', 'bootscanner.l',
		'bootparse.y');
	$postgres->AddFiles('src\common\backend\utils\misc', 'guc-file.l');
	$postgres->AddFiles('src\common\backend\replication', 'repl_scanner.l',
		'repl_gram.y');
	$postgres->AddDefine('BUILDING_DLL');
	$postgres->AddLibrary('wsock32.lib');
	$postgres->AddLibrary('ws2_32.lib');
	$postgres->AddLibrary('secur32.lib');
	$postgres->AddLibrary('wldap32.lib') if ($solution->{options}->{ldap});
	$postgres->FullExportDLL('postgres.lib');

	my $snowball = $solution->AddProject('dict_snowball', 'dll', '',
		'src\common\backend\snowball');
	$snowball->RelocateFiles(
		'src\common\backend\snowball\libstemmer',
		sub {
			return shift !~ /dict_snowball.cpp$/;
		});
	$snowball->AddIncludeDir('src\include\snowball');
	$snowball->AddReference($postgres);

	my $plpgsql =
	  $solution->AddProject('plpgsql', 'dll', 'PLs', 'src\common\pl\plpgsql\src');
	$plpgsql->AddFiles('src\common\pl\plpgsql\src', 'gram.y');
	$plpgsql->AddReference($postgres);

	if ($solution->{options}->{perl})
	{
		my $plperlsrc = "src\\common\\pl\\plperl\\";
		my $plperl =
		  $solution->AddProject('plperl', 'dll', 'PLs', 'src\common\pl\plperl');
		$plperl->AddIncludeDir($solution->{options}->{perl} . '/lib/CORE');
		$plperl->AddDefine('PLPERL_HAVE_UID_GID');
		foreach my $xs ('SPI.xs', 'Util.xs')
		{
			(my $xsc = $xs) =~ s/\.xs/.cpp/;
			if (Solution::IsNewer("$plperlsrc$xsc", "$plperlsrc$xs"))
			{
				my $xsubppdir = first { -e "$_\\ExtUtils\\xsubpp" } @INC;
				print "Building $plperlsrc$xsc...\n";
				system( $solution->{options}->{perl}
					  . '/bin/perl '
					  . "$xsubppdir/ExtUtils/xsubpp -typemap "
					  . $solution->{options}->{perl}
					  . '/lib/ExtUtils/typemap '
					  . "$plperlsrc$xs "
					  . ">$plperlsrc$xsc");
				if ((!(-f "$plperlsrc$xsc")) || -z "$plperlsrc$xsc")
				{
					unlink("$plperlsrc$xsc");    # if zero size
					die "Failed to create $xsc.\n";
				}
			}
		}
		if (Solution::IsNewer(
				'src\common\pl\plperl\perlchunks.h',
				'src\common\pl\plperl\plc_perlboot.pl')
			|| Solution::IsNewer(
				'src\common\pl\plperl\perlchunks.h',
				'src\common\pl\plperl\plc_trusted.pl'))
		{
			print 'Building src\common\pl\plperl\perlchunks.h ...' . "\n";
			my $basedir = getcwd;
			chdir 'src\common\pl\plperl';
			system( $solution->{options}->{perl}
				  . '/bin/perl '
				  . 'text2macro.pl '
				  . '--strip="^(\#.*|\s*)$$" '
				  . 'plc_perlboot.pl plc_trusted.pl '
				  . '>perlchunks.h');
			chdir $basedir;
			if ((!(-f 'src\common\pl\plperl\perlchunks.h'))
				|| -z 'src\common\pl\plperl\perlchunks.h')
			{
				unlink('src\common\pl\plperl\perlchunks.h');    # if zero size
				die 'Failed to create perlchunks.h' . "\n";
			}
		}
		if (Solution::IsNewer(
				'src\common\pl\plperl\plperl_opmask.h',
				'src\common\pl\plperl\plperl_opmask.pl'))
		{
			print 'Building src\common\pl\plperl\plperl_opmask.h ...' . "\n";
			my $basedir = getcwd;
			chdir 'src\common\pl\plperl';
			system( $solution->{options}->{perl}
				  . '/bin/perl '
				  . 'plperl_opmask.pl '
				  . 'plperl_opmask.h');
			chdir $basedir;
			if ((!(-f 'src\common\pl\plperl\plperl_opmask.h'))
				|| -z 'src\common\pl\plperl\plperl_opmask.h')
			{
				unlink('src\common\pl\plperl\plperl_opmask.h');    # if zero size
				die 'Failed to create plperl_opmask.h' . "\n";
			}
		}
		$plperl->AddReference($postgres);
		my @perl_libs =
		  grep { /perl\d+.lib$/ }
		  glob($solution->{options}->{perl} . '\lib\CORE\perl*.lib');
		if (@perl_libs == 1)
		{
			$plperl->AddLibrary($perl_libs[0]);
		}
		else
		{
			die "could not identify perl library version";
		}
	}

	if ($solution->{options}->{python})
	{

		# Attempt to get python version and location.
		# Assume python.exe in specified dir.
		open(P,
			$solution->{options}->{python}
			  . "\\python -c \"import sys;print(sys.prefix);print(str(sys.version_info[0])+str(sys.version_info[1]))\" |"
		) || die "Could not query for python version!\n";
		my $pyprefix = <P>;
		chomp($pyprefix);
		my $pyver = <P>;
		chomp($pyver);
		close(P);

		# Sometimes (always?) if python is not present, the execution
		# appears to work, but gives no data...
		die "Failed to query python for version information\n"
		  if (!(defined($pyprefix) && defined($pyver)));

		my $pymajorver = substr($pyver, 0, 1);
		my $plpython = $solution->AddProject('plpython' . $pymajorver,
			'dll', 'PLs', 'src\common\pl\plpython');
		$plpython->AddIncludeDir($pyprefix . '\include');
		$plpython->AddLibrary($pyprefix . "\\Libs\\python$pyver.lib");
		$plpython->AddReference($postgres);
	}

	if ($solution->{options}->{tcl})
	{
		my $pltcl =
		  $solution->AddProject('pltcl', 'dll', 'PLs', 'src\common\pl\tcl');
		$pltcl->AddIncludeDir($solution->{options}->{tcl} . '\include');
		$pltcl->AddReference($postgres);
		if (-e $solution->{options}->{tcl} . '\lib\tcl85.lib')
		{
			$pltcl->AddLibrary(
				$solution->{options}->{tcl} . '\lib\tcl85.lib');
		}
		else
		{
			$pltcl->AddLibrary(
				$solution->{options}->{tcl} . '\lib\tcl84.lib');
		}
	}

	$libpq = $solution->AddProject('libpq', 'dll', 'interfaces',
		'src\common\interfaces\libpq');
	$libpq->AddDefine('FRONTEND');
	$libpq->AddDefine('UNSAFE_STAT_OK');
	$libpq->AddIncludeDir('src\common\port');
	$libpq->AddLibrary('wsock32.lib');
	$libpq->AddLibrary('secur32.lib');
	$libpq->AddLibrary('ws2_32.lib');
	$libpq->AddLibrary('wldap32.lib') if ($solution->{options}->{ldap});
	$libpq->UseDef('src\common\interfaces\libpq\libpqdll.def');
	$libpq->ReplaceFile('src\common\interfaces\libpq\libpqrc.cpp',
		'src\common\interfaces\libpq\libpq.rc');
	$libpq->AddReference($libpgport);

	my $pgtypes = $solution->AddProject(
		'libpgtypes', 'dll',
		'interfaces', 'src\common\interfaces\ecpg\pgtypeslib');
	$pgtypes->AddDefine('FRONTEND');
	$pgtypes->AddReference($libpgport);
	$pgtypes->UseDef('src\common\interfaces\ecpg\pgtypeslib\pgtypeslib.def');
	$pgtypes->AddIncludeDir('src\common\interfaces\ecpg\include');

	my $libecpg = $solution->AddProject('libecpg', 'dll', 'interfaces',
		'src\common\interfaces\ecpg\ecpglib');
	$libecpg->AddDefine('FRONTEND');
	$libecpg->AddIncludeDir('src\common\interfaces\ecpg\include');
	$libecpg->AddIncludeDir('src\common\interfaces\libpq');
	$libecpg->AddIncludeDir('src\common\port');
	$libecpg->UseDef('src\common\interfaces\ecpg\ecpglib\ecpglib.def');
	$libecpg->AddLibrary('wsock32.lib');
	$libecpg->AddReference($libpq, $pgtypes, $libpgport);
=cut;
	# Do not support ecpg 

	my $libecpgcompat = $solution->AddProject(
		'libecpg_compat', 'dll',
		'interfaces',     'src\common\interfaces\ecpg\compatlib');
	$libecpgcompat->AddIncludeDir('src\common\interfaces\ecpg\include');
	$libecpgcompat->AddIncludeDir('src\common\interfaces\libpq');
	$libecpgcompat->UseDef('src\common\interfaces\ecpg\compatlib\compatlib.def');
	$libecpgcompat->AddReference($pgtypes, $libecpg, $libpgport);

	my $ecpg = $solution->AddProject('ecpg', 'exe', 'interfaces',
		'src\common\interfaces\ecpg\preproc');
	$ecpg->AddIncludeDir('src\common\interfaces\ecpg\include');
	$ecpg->AddIncludeDir('src\common\interfaces\libpq');
	$ecpg->AddPrefixInclude('src\common\interfaces\ecpg\preproc');
	$ecpg->AddFiles('src\common\interfaces\ecpg\preproc', 'pgc.l', 'preproc.y');
	$ecpg->AddDefine('MAJOR_VERSION=4');
	$ecpg->AddDefine('MINOR_VERSION=2');
	$ecpg->AddDefine('PATCHLEVEL=1');
	$ecpg->AddDefine('ECPG_COMPILE');
	$ecpg->AddReference($libpgport);

	my $pgregress_ecpg =
	  $solution->AddProject('pg_regress_ecpg', 'exe', 'misc');
	$pgregress_ecpg->AddFile('src\common\interfaces\ecpg\test\pg_regress_ecpg.cpp');
	$pgregress_ecpg->AddFile('src\test\regress\pg_regress.cpp');
	$pgregress_ecpg->AddIncludeDir('src\common\port');
	$pgregress_ecpg->AddIncludeDir('src\test\regress');
	$pgregress_ecpg->AddDefine('HOST_TUPLE="i686-pc-win32vc"');
	$pgregress_ecpg->AddDefine('FRONTEND');
	$pgregress_ecpg->AddReference($libpgport);
=cut;

	my $isolation_tester =
	  $solution->AddProject('isolationtester', 'exe', 'misc');
	$isolation_tester->AddFile('src\test\isolation\isolationtester.cpp');
	$isolation_tester->AddFile('src\test\isolation\specparse.y');
	$isolation_tester->AddFile('src\test\isolation\specscanner.l');
	$isolation_tester->AddFile('src\test\isolation\specparse.cpp');
	$isolation_tester->AddIncludeDir('src\test\isolation');
	$isolation_tester->AddIncludeDir('src\common\port');
	$isolation_tester->AddIncludeDir('src\test\regress');
	$isolation_tester->AddIncludeDir('src\common\interfaces\libpq');
	$isolation_tester->AddDefine('HOST_TUPLE="i686-pc-win32vc"');
	$isolation_tester->AddDefine('FRONTEND');
	$isolation_tester->AddLibrary('wsock32.lib');
	$isolation_tester->AddReference($libpq, $libpgport);

	my $pgregress_isolation =
	  $solution->AddProject('pg_isolation_regress', 'exe', 'misc');
	$pgregress_isolation->AddFile('src\test\isolation\isolation_main.cpp');
	$pgregress_isolation->AddFile('src\test\regress\pg_regress.cpp');
	$pgregress_isolation->AddIncludeDir('src\common\port');
	$pgregress_isolation->AddIncludeDir('src\test\regress');
	$pgregress_isolation->AddDefine('HOST_TUPLE="i686-pc-win32vc"');
	$pgregress_isolation->AddDefine('FRONTEND');
	$pgregress_isolation->AddReference($libpgport);

	# src/bin
	my $initdb = AddSimpleFrontend('initdb');
	$initdb->AddIncludeDir('src\common\interfaces\libpq');
	$initdb->AddIncludeDir('src\common\timezone');
	$initdb->AddDefine('FRONTEND');
	$initdb->AddLibrary('wsock32.lib');
	$initdb->AddLibrary('ws2_32.lib');

	my $pgbasebackup = AddSimpleFrontend('pg_basebackup', 1);
	$pgbasebackup->AddFile('src\bin\pg_basebackup\pg_basebackup.cpp');
	$pgbasebackup->AddLibrary('ws2_32.lib');

	my $pgreceivexlog = AddSimpleFrontend('pg_basebackup', 1);
	$pgreceivexlog->{name} = 'pg_receivexlog';
	$pgreceivexlog->AddFile('src\bin\pg_basebackup\pg_receivexlog.cpp');
	$pgreceivexlog->AddLibrary('ws2_32.lib');

	my $pgconfig = AddSimpleFrontend('pg_config');

	my $pgcontrol = AddSimpleFrontend('pg_controldata');

	my $pgctl = AddSimpleFrontend('pg_ctl', 1);

	my $pgreset = AddSimpleFrontend('pg_resetxlog');

	my $pgevent = $solution->AddProject('pgevent', 'dll', 'bin');
	$pgevent->AddFiles('src\bin\pgevent', 'pgevent.cpp', 'pgmsgevent.rc');
	$pgevent->AddResourceFile('src\bin\pgevent',
		'Eventlog message formatter');
	$pgevent->RemoveFile('src\bin\pgevent\win32ver.rc');
	$pgevent->UseDef('src\bin\pgevent\pgevent.def');
	$pgevent->DisableLinkerWarnings('4104');

	my $psql = AddSimpleFrontend('psql', 1);
	$psql->AddIncludeDir('src\bin\pg_dump');
	$psql->AddIncludeDir('src\backend');
	$psql->AddFile('src\bin\psql\psqlscan.l');

	my $pgdump = AddSimpleFrontend('pg_dump', 1);
	$pgdump->AddIncludeDir('src\common\backend');
	$pgdump->AddFile('src\bin\pg_dump\pg_dump.cpp');
	$pgdump->AddFile('src\bin\pg_dump\common.cpp');
	$pgdump->AddFile('src\bin\pg_dump\pg_dump_sort.cpp');
	$pgdump->AddFile('src\bin\pg_dump\keywords.cpp');
	$pgdump->AddFile('src\common\backend\parser\kwlookup.cpp');

	my $pgdumpall = AddSimpleFrontend('pg_dump', 1);

	# pg_dumpall doesn't use the files in the Makefile's $(OBJS), unlike
	# pg_dump and pg_restore.
	# So remove their sources from the object, keeping the other setup that
	# AddSimpleFrontend() has done.
	my @nodumpall = grep { m/src\\bin\\pg_dump\\.*\.cpp$/ }
	  keys %{ $pgdumpall->{files} };
	delete @{ $pgdumpall->{files} }{@nodumpall};
	$pgdumpall->{name} = 'pg_dumpall';
	$pgdumpall->AddIncludeDir('src\common\backend');
	$pgdumpall->AddFile('src\bin\pg_dump\pg_dumpall.cpp');
	$pgdumpall->AddFile('src\bin\pg_dump\dumputils.cpp');
	$pgdumpall->AddFile('src\bin\pg_dump\dumpmem.cpp');
	$pgdumpall->AddFile('src\bin\pg_dump\keywords.cpp');
	$pgdumpall->AddFile('src\common\backend\parser\kwlookup.cpp');

	my $pgrestore = AddSimpleFrontend('pg_dump', 1);
	$pgrestore->{name} = 'pg_restore';
	$pgrestore->AddIncludeDir('src\common\backend');
	$pgrestore->AddFile('src\bin\pg_dump\pg_restore.cpp');
	$pgrestore->AddFile('src\bin\pg_dump\keywords.cpp');
	$pgrestore->AddFile('src\common\backend\parser\kwlookup.cpp');

	my $zic = $solution->AddProject('zic', 'exe', 'utils');
	$zic->AddFiles('src\common\timezone', 'zic.cpp', 'ialloc.cpp', 'scheck.cpp',
		'localtime.cpp');
	$zic->AddReference($libpgport);

	if ($solution->{options}->{xml})
	{
		$contrib_extraincludes->{'pgxml'} = [
			$solution->{options}->{xml} . '\include',
			$solution->{options}->{xslt} . '\include',
			$solution->{options}->{iconv} . '\include' ];

		$contrib_extralibs->{'pgxml'} = [
			$solution->{options}->{xml} . '\lib\libxml2.lib',
			$solution->{options}->{xslt} . '\lib\libxslt.lib' ];
	}
	else
	{
		push @contrib_excludes, 'xml2';
	}

	if (!$solution->{options}->{openssl})
	{
		push @contrib_excludes, 'sslinfo';
	}

	if ($solution->{options}->{uuid})
	{
		$contrib_extraincludes->{'uuid-ossp'} =
		  [ $solution->{options}->{uuid} . '\include' ];
		$contrib_extralibs->{'uuid-ossp'} =
		  [ $solution->{options}->{uuid} . '\lib\uuid.lib' ];
	}
	else
	{
		push @contrib_excludes, 'uuid-ossp';
	}

	# Pgcrypto makefile too complex to parse....
	my $pgcrypto = $solution->AddProject('pgcrypto', 'dll', 'crypto');
	$pgcrypto->AddFiles(
		'contrib\pgcrypto', 'pgcrypto.cpp',
		'px.cpp',             'px-hmac.cpp',
		'px-crypt.cpp',       'crypt-gensalt.cpp',
		'crypt-blowfish.cpp', 'crypt-des.cpp',
		'crypt-md5.cpp',      'mbuf.cpp',
		'pgp.cpp',            'pgp-armor.cpp',
		'pgp-cfb.cpp',        'pgp-compress.cpp',
		'pgp-decrypt.cpp',    'pgp-encrypt.cpp',
		'pgp-info.cpp',       'pgp-mpi.cpp',
		'pgp-pubdec.cpp',     'pgp-pubenc.cpp',
		'pgp-pubkey.cpp',     'pgp-s2k.cpp',
		'pgp-pgsql.cpp');
	if ($solution->{options}->{openssl})
	{
		$pgcrypto->AddFiles('contrib\pgcrypto', 'openssl.cpp',
			'pgp-mpi-openssl.cpp');
	}
	else
	{
		$pgcrypto->AddFiles(
			'contrib\pgcrypto',   'md5.cpp',
			'sha1.cpp',             'sha2.cpp',
			'internal.cpp',         'internal-sha2.cpp',
			'blf.cpp',              'rijndael.cpp',
			'fortuna.cpp',          'random.cpp',
			'pgp-mpi-internal.cpp', 'imath.cpp');
	}
	$pgcrypto->AddReference($postgres);
	$pgcrypto->AddLibrary('wsock32.lib');
	my $mf = Project::read_file('contrib/pgcrypto/Makefile');
	GenerateContribSqlFiles('pgcrypto', $mf);

	my $D;
	opendir($D, 'contrib') || croak "Could not opendir on contrib!\n";
	while (my $d = readdir($D))
	{
		next if ($d =~ /^\./);
		next unless (-f "contrib/$d/Makefile");
		next if (grep { /^$d$/ } @contrib_excludes);
#		AddContrib($d);
	}
	closedir($D);

	$mf =
	  Project::read_file('src\common\backend\utils\mb\conversion_procs\Makefile');
	$mf =~ s{\\s*[\r\n]+}{}mg;
	$mf =~ m{SUBDIRS\s*=\s*(.*)$}m
	  || die 'Could not match in conversion makefile' . "\n";
	foreach my $sub (split /\s+/, $1)
	{
		my $mf = Project::read_file(
			'src\common\backend\utils\mb\conversion_procs\\' . $sub . '\Makefile');
		my $p = $solution->AddProject($sub, 'dll', 'conversion procs');
		$p->AddFile('src\common\backend\utils\mb\conversion_procs\\' 
			  . $sub . '\\' 
			  . $sub
			  . '.cpp');
		if ($mf =~ m{^SRCS\s*\+=\s*(.*)$}m)
		{
			$p->AddFile(
				'src\common\backend\utils\mb\conversion_procs\\' . $sub . '\\' . $1);
		}
		$p->AddReference($postgres);
	}

	$mf = Project::read_file('src\bin\scripts\Makefile');
	$mf =~ s{\\s*[\r\n]+}{}mg;
	$mf =~ m{PROGRAMS\s*=\s*(.*)$}m
	  || die 'Could not match in bin\scripts\Makefile' . "\n";
	foreach my $prg (split /\s+/, $1)
	{
		my $proj = $solution->AddProject($prg, 'exe', 'bin');
		$mf =~ m{$prg\s*:\s*(.*)$}m
		  || die 'Could not find script define for $prg' . "\n";
		my @files = split /\s+/, $1;
		foreach my $f (@files)
		{
			$f =~ s/\.o$/\.cpp/;
			if ($f eq 'keywords.cpp')
			{
				$proj->AddFile('src\bin\pg_dump\keywords.cpp');
			}
			elsif ($f eq 'kwlookup.cpp')
			{
				$proj->AddFile('src\common\backend\parser\kwlookup.cpp');
			}
			elsif ($f eq 'dumputils.cpp')
			{
				$proj->AddFile('src\bin\pg_dump\dumputils.cpp');
			}
			elsif ($f =~ /print\.cpp$/)
			{    # Also catches mbprint.cpp
				$proj->AddFile('src\bin\psql\\' . $f);
			}
			elsif ($f =~ /\.cpp$/)
			{
				$proj->AddFile('src\bin\scripts\\' . $f);
			}
		}
		$proj->AddIncludeDir('src\common\interfaces\libpq');
		$proj->AddIncludeDir('src\bin\pg_dump');
		$proj->AddIncludeDir('src\bin\psql');
		$proj->AddReference($libpq, $libpgport);
		$proj->AddResourceFile('src\bin\scripts', 'PostgreSQL Utility');
	}

	# Regression DLL and EXE
	my $regress = $solution->AddProject('regress', 'dll', 'misc');
	$regress->AddFile('src\test\regress\regress.cpp');
	$regress->AddReference($postgres);

	my $pgregress = $solution->AddProject('pg_regress', 'exe', 'misc');
	$pgregress->AddFile('src\test\regress\pg_regress.cpp');
	$pgregress->AddFile('src\test\regress\pg_regress_main.cpp');
	$pgregress->AddIncludeDir('src\common\port');
	$pgregress->AddDefine('HOST_TUPLE="i686-pc-win32vc"');
	$pgregress->AddReference($libpgport);

	$solution->Save();
	return $solution->{vcver};
}

#####################
# Utility functions #
#####################

# Add a simple frontend project (exe)
sub AddSimpleFrontend
{
	my $n        = shift;
	my $uselibpq = shift;

	my $p = $solution->AddProject($n, 'exe', 'bin');
	$p->AddDir('src\bin\\' . $n);
	$p->AddReference($libpgport);
	if ($uselibpq)
	{
		$p->AddIncludeDir('src\common\interfaces\libpq');
		$p->AddReference($libpq);
	}
	return $p;
}

# Add a simple contrib project
sub AddContrib
{
	my $n  = shift;
	my $mf = Project::read_file('contrib\\' . $n . '\Makefile');

	if ($mf =~ /^MODULE_big\s*=\s*(.*)$/mg)
	{
		my $dn = $1;
		$mf =~ s{\\\s*[\r\n]+}{}mg;
		my $proj = $solution->AddProject($dn, 'dll', 'contrib');
		$mf =~ /^OBJS\s*=\s*(.*)$/gm
		  || croak "Could not find objects in MODULE_big for $n\n";
		my $objs = $1;
		while ($objs =~ /\b([\w-]+\.o)\b/g)
		{
			my $o = $1;
			$o =~ s/\.o$/.cpp/;
			$proj->AddFile('contrib\\' . $n . '\\' . $o);
		}
		$proj->AddReference($postgres);
		if ($mf =~ /^SUBDIRS\s*:?=\s*(.*)$/mg)
		{
			foreach my $d (split /\s+/, $1)
			{
				my $mf2 = Project::read_file(
					'contrib\\' . $n . '\\' . $d . '\Makefile');
				$mf2 =~ s{\\\s*[\r\n]+}{}mg;
				$mf2 =~ /^SUBOBJS\s*=\s*(.*)$/gm
				  || croak
				  "Could not find objects in MODULE_big for $n, subdir $d\n";
				$objs = $1;
				while ($objs =~ /\b([\w-]+\.o)\b/g)
				{
					my $o = $1;
					$o =~ s/\.o$/.cpp/;
					$proj->AddFile('contrib\\' . $n . '\\' . $d . '\\' . $o);
				}
			}
		}
		AdjustContribProj($proj);
	}
	elsif ($mf =~ /^MODULES\s*=\s*(.*)$/mg)
	{
		foreach my $mod (split /\s+/, $1)
		{
			my $proj = $solution->AddProject($mod, 'dll', 'contrib');
			$proj->AddFile('contrib\\' . $n . '\\' . $mod . '.cpp');
			$proj->AddReference($postgres);
			AdjustContribProj($proj);
		}
	}
	elsif ($mf =~ /^PROGRAM\s*=\s*(.*)$/mg)
	{
		my $proj = $solution->AddProject($1, 'exe', 'contrib');
		$mf =~ s{\\\s*[\r\n]+}{}mg;
		$mf =~ /^OBJS\s*=\s*(.*)$/gm
		  || croak "Could not find objects in PROGRAM for $n\n";
		my $objs = $1;
		while ($objs =~ /\b([\w-]+\.o)\b/g)
		{
			my $o = $1;
			$o =~ s/\.o$/.cpp/;
			$proj->AddFile('contrib\\' . $n . '\\' . $o);
		}
		AdjustContribProj($proj);
	}
	else
	{
		croak "Could not determine contrib module type for $n\n";
	}

	# Are there any output data files to build?
	GenerateContribSqlFiles($n, $mf);
}

sub GenerateContribSqlFiles
{
	my $n  = shift;
	my $mf = shift;
	if ($mf =~ /^DATA_built\s*=\s*(.*)$/mg)
	{
		my $l = $1;

		# Strip out $(addsuffix) rules
		if (index($l, '$(addsuffix ') >= 0)
		{
			my $pcount = 0;
			my $i;
			for ($i = index($l, '$(addsuffix ') + 12; $i < length($l); $i++)
			{
				$pcount++ if (substr($l, $i, 1) eq '(');
				$pcount-- if (substr($l, $i, 1) eq ')');
				last      if ($pcount < 0);
			}
			$l =
			  substr($l, 0, index($l, '$(addsuffix ')) . substr($l, $i + 1);
		}

		foreach my $d (split /\s+/, $l)
		{
			my $in  = "$d.in";
			my $out = "$d";

			if (Solution::IsNewer("contrib/$n/$out", "contrib/$n/$in"))
			{
				print "Building $out from $in (contrib/$n)...\n";
				my $cont = Project::read_file("contrib/$n/$in");
				my $dn   = $out;
				$dn   =~ s/\.sql$//;
				$cont =~ s/MODULE_PATHNAME/\$libdir\/$dn/g;
				my $o;
				open($o, ">contrib/$n/$out")
				  || croak "Could not write to contrib/$n/$d";
				print $o $cont;
				close($o);
			}
		}
	}
}

sub AdjustContribProj
{
	my $proj = shift;
	my $n    = $proj->{name};

	if ($contrib_defines->{$n})
	{
		foreach my $d ($contrib_defines->{$n})
		{
			$proj->AddDefine($d);
		}
	}
	if (grep { /^$n$/ } @contrib_uselibpq)
	{
		$proj->AddIncludeDir('src\common\interfaces\libpq');
		$proj->AddReference($libpq);
	}
	if (grep { /^$n$/ } @contrib_uselibpgport)
	{
		$proj->AddReference($libpgport);
	}
	if ($contrib_extralibs->{$n})
	{
		foreach my $l (@{ $contrib_extralibs->{$n} })
		{
			$proj->AddLibrary($l);
		}
	}
	if ($contrib_extraincludes->{$n})
	{
		foreach my $i (@{ $contrib_extraincludes->{$n} })
		{
			$proj->AddIncludeDir($i);
		}
	}
	if ($contrib_extrasource->{$n})
	{
		$proj->AddFiles('contrib\\' . $n, @{ $contrib_extrasource->{$n} });
	}
}

1;
