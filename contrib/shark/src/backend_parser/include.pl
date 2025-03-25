#!/usr/bin/perl

my $num_args = @ARGV;
if ($num_args != 2) {
	print STDERR "usage: perl include.pl <search path> <base file name>"
}

my $pg_yy_name_prefix = 'base_yy';
my $pgtsql_yy_name_prefix = 'pgtsql_base_yy';

#
# Read from STDIN (line by line)
#---------------------------------------------------------------------
#
$TAG= '\s*/\*\$\$include';
my $base_file_name = @ARGV[1];
my $linecnt = 1;
my $line;
my $in_prologue = 0;
my $in_comment = 0;
my $brace_indent = 0;
my $brace_started = 0; # did prev line start a new brace?
my $prior_can_be_added = 0;

while ($line = <STDIN>) {
	$line =~ s/\r//g;
	$linecnt++;

	if ($line =~ /^$TAG\s+"(.*?)"\s*?\*\/\n/) {
		my $fname = grp($line,1);
		open(F, "< $ARGV[0]/$fname") || die("error opening input file $fname: $!");

		print " /** START OF include: $fname **/\n";
		if ($fname =~ /(.*).(h|c)$/) {
			print "\#line 1 \"$fname\"\n";
		}

		my $inc_linecnt = 1;
		while (<F>) {
			$inc_linecnt++;
			my $inc_line = $_;

			simple_parse($inc_line, $in_comment, $in_prologue, $brace_indent, $brace_started);
			print $inc_line;

			if ($brace_started eq 1 && $brace_indent eq 1)
			{
				# add #line directive for debugging convenience
				print "\#line $inc_linecnt \"$fname\"\n";
			}

			$brace_started = 0;
		}

		print " /** END OF include: $fname **/\n";
		if ($fname =~ /(.*).(h|c)$/) {
			print "\#line $linecnt \"$base_file_name\"\n";
		}
		close(F);
		next;
	}

    if ($line =~ /col_name_keyword_nonambiguous:/) {
        $prior_can_be_added = 1;
    }

	# in higher version of bison, duplicate name-prefix is ignored. replace the name-prefix to pgtsql here. This can be refactored later.
	if ($line =~ "%name-prefix \"$pg_yy_name_prefix\"")
	{
		print "%name-prefix \"$pgtsql_yy_name_prefix\"\n"
	}
	elsif ($line =~ "#include \"parser/gramparse.h\"") {
		print "#include \"gramparse.h\"\n"
	}
	elsif ($line =~ /\| a_expr IDENT\n/) {
		print "| a_expr DirectColLabel\n"
	}
    elsif ($line =~ /\| PRECISION\n/ && $prior_can_be_added == 1) {
        $prior_can_be_added = 0;
        print "| PRECISION\n| PRIOR\n"
    }
    # remove postfix expression syntax in D compatibility database
    elsif ($line =~ /\| PRIOR\n/ ||
           $line =~ /%left\s+POSTFIXOP/ ||
           $line =~ /\| b_expr qual_Op\s+%prec POSTFIXOP/ ||
           $line =~ /\| a_expr qual_Op\s+%prec POSTFIXOP/ ||
           $line =~ m{
        \{ \s* 
        \$\$ \s* = \s* 
        \( \s* Node \s* \* \s* \) \s* 
        makeA_Expr \s* 
        \( \s* 
            AEXPR_OP \s* , \s* 
            \$2 \s* , \s* 
            \$1 \s* , \s* 
            NULL \s* , \s* 
            \@2 
        \s* \) \s* ; \s* 
        \}
    }x) {
        next;
    } 
    else
	{
		simple_parse($line, $in_comment, $in_prologue, $brace_indent, $brace_started);
		#print "in_comment: $in_comment, in_prologue: $in_prologue, brace_indent: $brace_indent, brace_started: $brace_started || ";
		print $line;
	}

	if ($in_prologue eq 0 && $brace_started eq 1 && $brace_indent eq 1)
	{
		# add #line directive for debugging convenience
		print "\#line $linecnt \"$base_file_name\"\n";
	}
	$brace_started = 0;
}
exit;

sub grp {
	my ($orig_string, $grp_nr) = @_;
	return substr($orig_string, @-[$grp_nr], (@+[$grp_nr] - @-[$grp_nr]));
}

sub simple_parse {
	my $line = $_[0];
	my $in_comment = $_[1];
	my $in_prologue = $_[2];
	my $brace_indent = $_[3];
	my $brace_started = $_[4];

	my @arr = split(' ', $line);

	# some harded-coded pattern incldues comment "/*" with quote, which has to be skipped.
	# o.w. we really need a complex parsing logic.
	if ($line =~ /^\s*yyerror\(\"unterminated \/\* comment\"\);/ ||
	    $line =~ /^\s*char\s*\*slashstar = strstr\(yytext, \"\/\*\"\);/)
	{
		return;
	}

	for (my $fieldIndexer = 0; $fieldIndexer < scalar(@arr); $fieldIndexer++)
	{
		if ($arr[$fieldIndexer] =~ /\/*.*\*\// && not $_[1]) # pattern like /*XXX*/
		{
			next;
		}
		elsif ($arr[$fieldIndexer] =~ /.*\*\// && $_[1])
		{
			$_[1] = 0; # $in_comment
			next;
		}
		elsif ($in_comment)
		{
			next;
		}
		elsif ($arr[$fieldIndexer] =~ /\/\*.*/)
		{
			$_[1] = 1; # $in_comment
			next;
		}
		elsif ($arr[$fieldIndexer] eq '//')
		{
			last;
		}
		elsif ($arr[$fieldIndexer] eq '\\' && $fieldIndexer eq scalar(@arr) -1)
		{
			$_[4] = 0; # reset $brace_started not to add #line directive
			next;
		}
		elsif ($arr[$fieldIndexer] eq '%}')
		{
			$_[2] = 0;
			next;
		}
		elsif ($arr[$fieldIndexer] eq '%{')
		{
			$_[2] = 1;
			next;
		}
		elsif ($arr[$fieldIndexer] =~ /^<.*>\{$/) # special handling for lex grammar "<...>{" not to add #line directive
		{
			$_[3]++; # $brace_indent
			next;
		}
		elsif ($arr[$fieldIndexer] =~ '{.*}')
		{
			next;
		}
		elsif ($arr[$fieldIndexer] =~ '.*}')
		{
			$_[3]--; # $brace_indent
			next;
		}
		elsif ($arr[$fieldIndexer] =~ '{.*')
		{
			$_[3]++; # $brace_indent
			$_[4] = 1;
			next;
		}
	}
}
