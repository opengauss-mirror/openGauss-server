#!/usr/bin/perl


# smartmatch:
#
# This script is extracted from Gurjeet Singh ( singh.gurjeet@gmail.com ) NEUROdiff patch.
#
# 04 Apr 2013 : First implementation

use strict;
use warnings;

sub usage
{
	print "Usage: smartmatch.pl <expected-filename> <result-filename> <smartmatch-expected-filename>\n";
	return;
}

# file handles for expected and results files
my $EXPECTED;
my $RESULT;
my $NEW_EXPECTED;

my $expected; # line iterator for EXPECTED file
my $result;   # line iterator for RESULT file

my $re;       # the Regular Expression part of a line which starts with ?

my $insideuo; # boolean, representing if we are INSIDE an UnOrdered set of lines

my $bFirstLine; # Indicates whether the line going to be printed is the first or not

my $iuo;             # counter I for counting lines within an UnOrdered set
my $seenspecialinuo; # Seen special marker inside unordered group
my $smartmatch;	     # seen any special match syntax	   	

my $rc = 0;              # Return Code

my @earr = ( [], [] ); # 2-dimensional ARRay to keep Expected file's unmatched lines from an unordered set
my @rarr = ( [], [] ); # 2-dimensional ARRay to keep Result file's unmatched lines from unordered set

my @searr = ( [], [] ); # 2-dimensional ARRay to keep Expected file's sorted lines from an unordered set
my @srarr = ( [], [] ); # 2-dimensional ARRay to keep Result file's sorted lines from unordered set

# we require exactly 3 arguments
if( @ARGV != 3 )
{
	usage();
	exit(2);
}

# initialize (almost) everything
open $EXPECTED	    , "<", $ARGV[0] or die $!;
open $RESULT  	    , "<", $ARGV[1] or die $!;
open $NEW_EXPECTED  , ">", $ARGV[2] or die $!;

$insideuo = 0;
$iuo = 0;
$smartmatch = 0;

$bFirstLine = 1;

# process all lines from both the files
while( 1 )
{
	undef $!;

	my $matched = 1;

	$expected = <$EXPECTED>;

	undef $!;

	$result = <$RESULT>;


	# one file finished but not the other
	if( ( !defined( $expected ) || !defined( $result ) )
		&& ( defined( $expected ) || defined( $result ) ) )
	{
		$rc = 2;

		if( defined( $expected ) )
		{
			if( $bFirstLine )
			{
				print $NEW_EXPECTED "$expected";
				$bFirstLine = 0;
			}
			else
			{
				print $NEW_EXPECTED "\n$expected";
			}
		}

		last; # while( 1 )
	}

	# both files finished
	if( !defined( $expected ) && !defined( $result ) )
	{
		last; # while( 1 )
	}

	# chomp away...
	# Apart from getting rid of extra newlines in messages, this will also help
	# us be agnostic about platform specific newline sequences.
	# 
	# Correction: Apparently the above assumption is not true (found the hard
	# way :( ).
	# If the file was generated on Windows (CRLF), the Linux version of chomp
	# will trim only \n and leave \r. Had to use dos2unix on the out files to
	# make this script work.
	chomp( $expected );
	chomp( $result );

	# if the line from expected file starts with a ?, treat it specially
	if( $expected =~ /^--\?.*/ )
	{
		$smartmatch=1;
			
		# extract the Regular Expression
		$re = substr $expected, 3;

		# If this is the beginning of an UnOrdered set of lines
		if( $re eq 'unordered: start' )
		{
			if( $insideuo )
			{
				
				if( $bFirstLine )
				{
					print $NEW_EXPECTED "Nesting of 'unordered: start' blocks is not allowed";
					$bFirstLine = 0;
				}
				else
				{
					print $NEW_EXPECTED "\nNesting of 'unordered: start' blocks is not allowed";
				}
			
				exit( 2 );
			}

			# reset the variables for the UO set.
			$iuo = 0;
			$insideuo = 1;
			$seenspecialinuo = 0;
			
			if( $bFirstLine )
			{
				print $NEW_EXPECTED "$expected";
				$bFirstLine = 0;
			}
			else
			{
				print $NEW_EXPECTED "\n$expected";
			}

			next;
		}

		# end of an UnOrderd set of lines
		if( $re eq 'unordered: end' )
		{
			if( !$insideuo )
			{
				print $NEW_EXPECTED "'unordered: end' line found without a matching 'unordered: start' line\n";
				exit( 2 );
			}

			$insideuo = 0;

			# If there were some lines containing RE, do comparison the hard way
			if( $seenspecialinuo )
			{
				# begin the (m*n) processing of the two arrays. These arrays
				# contain the set of unmatched lines from respective files
				foreach my $eelemref ( @earr )
				{
					my $i = 0;

					my $eelem = $eelemref->[0];

					foreach my $relemref ( @rarr )
					{
						my $relem = $relemref->[0];

						$matched = 1;

						# treat these lines the same as we threat the others;
						# that is, if an 'expected' line starts with a '?', we
						# perform Regular Expression match, else we perform
						# normal comparison.

						if( $eelem =~ /^--\?.*/ )
						{
							my $tmpre = substr $eelem, 3;

							if( $relem !~ /^$tmpre$/ )
							{
								$matched = 0;
							}
							else
							{
			                    if( $bFirstLine )
			                    {
			                    	print $NEW_EXPECTED "$relem";
			                    	$bFirstLine = 0;
			                    }
			                    else
			                    {
			                    	print $NEW_EXPECTED "\n$relem";
			                    }
								
								last;
							}
						}
						elsif( $eelem ne $relem )
						{
							$matched = 0;
						}
						else
						{
							if( $bFirstLine )
							{
								print $NEW_EXPECTED "$relem";
								$bFirstLine = 0;
							}
							else
							{
								print $NEW_EXPECTED "\n$relem";
							}
							
							last;
						}

						++$i;
					} # foreach @rarr

					if( !$matched )
					{
						$rc = 2;
						if( $bFirstLine )
						{
							print $NEW_EXPECTED "$eelem";
							$bFirstLine = 0;
						}
						else
						{
							print $NEW_EXPECTED "\n$eelem";
						}
							
					}
					else
					{						
						splice @rarr, $i, 0;
					}
				} # foreach @earr
			}
			else	# if there's no line containing an RE in this UO group,
					# do it efficiently
			{
				# sort both arrays based on the text.
				@searr = sort { $a->[0] cmp $b->[0] } @earr;
				@srarr = sort { $a->[0] cmp $b->[0] } @rarr;

				my $min_len = (scalar(@searr) <= scalar(@srarr) ? scalar(@searr) : scalar(@srarr) );
				my $i;
				$matched = 1;
				
				for( $i = 0; $i < $min_len; ++$i )
				{
					my $eelem = $searr[$i][0];
					my $relem = $srarr[$i][0];

					# treat these lines the same as we threat the others; that is, if an
					# 'expected' line starts with a '?', we perform Regular Expression
					# match, else we perform normal comparison.

					if( $eelem =~ /^--\?.*/ )
					{
						my $tmpre = substr $eelem, 3;

						if( $relem !~ /^$tmpre$/ )
						{
							$matched = 0;
						}
					}
					elsif( $eelem ne $relem )
					{
						$matched = 0;
					}
				}
				
				if ((scalar(@searr) > $i) || (scalar(@srarr) > $i))
				{
					$matched = 0;
				}
				
				if ( !$matched )
				{	
					$rc = 2;
					for( my $i = 0; $i < scalar(@earr); ++$i )
					{
						if( $bFirstLine )
						{
							print $NEW_EXPECTED "$earr[$i][0]";
							$bFirstLine = 0;
						}
						else
						{
							print $NEW_EXPECTED "\n$earr[$i][0]";
						}
					}
				}
				else
				{
					for( my $i = 0; $i < scalar(@rarr); ++$i )
					{
						if( $bFirstLine )
						{
							print $NEW_EXPECTED "$rarr[$i][0]";
							$bFirstLine = 0;
						}
						else
						{
							print $NEW_EXPECTED "\n$rarr[$i][0]";
						}
					}
				}

				$matched = 0;
			} # else part of if( $seenspecialinuo )

			if( $bFirstLine )
			{
				$bFirstLine = 0;
				print $NEW_EXPECTED "$expected";
			}
			else			
			{
				print $NEW_EXPECTED "\n$expected";
			}
			
			# reset the array variables to reclaim memory
			@searr = @srarr = ();
			@earr = @rarr = ();

			next; # while( 1 )

		} # if re == 'unordered: end'

		# it is not an 'unordered' marker, so do regular Regular Expression match
		else
		{
			my $re_1;
		
			if ($result !~ /^$re/)
			{
				if ($re =~ /(.*)datanode.*/)
				{
					$re_1 = quotemeta($1);
					if ($result !~ /$re_1.*/)
					{
						$matched = 0;
					}
				}
				elsif ($re =~ /(.*)\(cost=.*/ )
				{
					$re_1 = quotemeta($1);
					if ($result !~ /$re_1.*/)
					{
						$matched = 0;
					}	
				}
				elsif ($re =~ /(.*)\(actual time=.*/)
				{
					$re_1 = quotemeta($1);
					if ($result !~ /$re_1.*/)
					{
						$matched = 0;
					}
				}
				elsif ($re =~ /(.*)\(CPU:.*/)
				{
					$re_1 = quotemeta($1);
					if ($result !~ /$re_1.*/)
					{
						$matched = 0;
					}
				}
				elsif ($re =~ /(.*)\(RoughCheck CU:.*/)
				{
					$re_1 = quotemeta($1);
					if ($result !~ /$re_1.*/)
					{
						$matched = 0;
					}
				}
				elsif ($re =~ /(.*)Buffers:.*/)
				{
					$re_1 = quotemeta($1);
					if ($result !~ /$re_1.*/)
					{
						$matched = 0;
					}
				}
				elsif ($re =~ /(.*)<Actual-Total-Time>.*/)
				{
					$re_1 = quotemeta($1);
					if ($result !~ /$re_1.*/)
					{
						$matched = 0;
					}
				}
				elsif ($re =~ /[.*]/)
				{
					$re_1 = quotemeta($1);
					if ($result !~ /$re_1.*/)
					{
						$matched = 0;
					}
				}
				elsif ($re =~ /(.*)<Actual-Startup-Time>.*/)
				{
					$re_1 = quotemeta($1);
					if ($result !~ /$re_1.*/)
					{
						$matched = 0;
					}
				}
				elsif ($re =~ /(.*)<Actual-Min-Startup-Time>.*/)
				{
					$re_1 = quotemeta($1);
					if ($result !~ /$re_1.*/)
					{
						$matched = 0;
					}
				}
				elsif ($re =~ /(.*)<Actual-Max-Startup-Time>.*/)
				{
					$re_1 = quotemeta($1);
					if ($result !~ /$re_1.*/)
					{
						$matched = 0;
					}
				}
				elsif ($re =~ /(.*)<Actual-Min-Total-Time>.*/)
				{
					$re_1 = quotemeta($1);
					if ($result !~ /$re_1.*/)
					{
						$matched = 0;
					}
				}
				elsif ($re =~ /(.*)<Actual-Max-Total-Time>.*/)
				{
					$re_1 = quotemeta($1);
					if ($result !~ /$re_1.*/)
					{
						$matched = 0;
					}
				}
				elsif ($re =~ /(.*)<Exclusive-Cycles\/Row>.*/)
				{
					$re_1 = quotemeta($1);
					if ($result !~ /$re_1.*/)
					{
						$matched = 0;
					}
				}
				elsif ($re =~ /(.*)<Exclusive-Cycles>.*/)
				{
					$re_1 = quotemeta($1);
					if ($result !~ /$re_1.*/)
					{
						$matched = 0;
					}
				}
				elsif ($re =~ /(.*)<Inclusive-Cycles>.*/)
				{
					$re_1 = quotemeta($1);
					if ($result !~ /$re_1.*/)
					{
						$matched = 0;
					}
				}
				elsif ($re =~ /^\s*Sort\s+Method.*/)
				{
					if ($result !~ /^\s*Sort\s+Method.*/)
					{
						$matched = 0;
					}
				}


				elsif ($re =~ /Total runtime:.*/)
				{
					if ($result !~ /^\s*Total runtime:.*/)
					{
						$matched = 0;
					}
				}	
				elsif ($re =~ /^\s*QUERY PLAN\s*$/)
				{
					if ($result !~ /^\s*QUERY PLAN\s*$/)
					{
						$matched = 0;
					}
				}	
				elsif ($re =~ /^\-+$/)
				{
					if ($result !~ /^\-+$/)
					{
						$matched = 0;
					}
				}	
				else
				{
					$matched = 0;
				}
			}
		}

	} # if $expected like ?.*

	# $expected doesn't begin with the special marker, so do normal comparison
	elsif( $expected ne $result )
	{
		$matched = 0;
	}

	if( !$matched || $insideuo )
	{
		# if the lines did not match, and if we are comparing an unordered set of lines,
		# then save the lines for processing later.
		if( $insideuo )
		{
			$earr[$iuo][0] = $expected;
			$rarr[$iuo][0] = $result;
			

			if( !$seenspecialinuo && $expected =~ /^--\?.*/ )
			{
				$seenspecialinuo = 1;
			}

			++$iuo;
		}
		else # print out the difference
		{
			$rc = 2;
			if( $bFirstLine )
			{
				$bFirstLine = 0;
				print $NEW_EXPECTED "$expected";
			}
			else
			{
				print $NEW_EXPECTED "\n$expected";
			}
		}
	}
	else
	{
		if( $bFirstLine )
		{
			$bFirstLine = 0;
			print $NEW_EXPECTED "$result";
		}
		else
		{
			print $NEW_EXPECTED "\n$result";
		}
		
	}
}

close $EXPECTED;
close $RESULT;
close $NEW_EXPECTED;

exit( $rc + $smartmatch );
