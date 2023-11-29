#! /usr/bin/perl
#
# Copyright (c) 2007-2012, 2023, PostgreSQL Global Development Group
#
# src/backend/utils/mb/Unicode/UCS_to_GB18030_2022.pl
#
# Generate UTF-8 <--> GB18030-2022 code conversion tables from
# "gb-18030-2022.xml"
#
# The lines we care about in the source file look like
#    <a u="009A" b="81 30 83 36"/>
# where the "u" field is the Unicode code point in hex,
# and the "b" field is the hex byte sequence for GB18030
 
require "ucs2utf.pl";
 
 
$change_file = "gb-18030-2022.xml";
 
open(CODE_TABLE, $change_file) || die("cannot open $change_file");
 
while (<CODE_TABLE>)
{
	next if (! m/<a u="([0-9A-F]+)" b="([0-9A-F ]+)"/);
	$u_code = $1;
	$c_code = $2;
	$c_code =~ s/ //g;
	$ucs_code  = hex($u_code);
	$code_gb = hex($c_code);
	if ($code_gb >= 0x80 && $ucs_code >= 0x0080)
	{
		$utf_code = &ucs2utf($ucs_code);
		if ($code_u{$utf_code} ne "")
		{
			printf STDERR "Warning: duplicate UTF8: %04x\n", $ucs_code;
			next;
		}
		if ($code_c{$code_gb} ne "")
		{
			printf STDERR "Warning: duplicate GB18030: %08x\n", $code_gb;
			next;
		}
		$code_u{$utf_code} = $code_gb;
		$code_c{$code_gb} = $utf_code;
		$number++;
	}
}
close(CODE_TABLE);
 
$change_map_file = "gb18030_to_utf8_2022.map";
open(CHANGE_MAP, "> $change_map_file") || die("cannot open $change_map_file");
print CHANGE_MAP "static pg_local_to_utf LUmapGB18030_2022[ $number ] = {\n";
 
$count = $number;
for $pos (sort { $a <=> $b } keys(%code_c))
{
	$utf_code = $code_c{$pos};
	$count--;
	if ($count == 0)
	{
		printf CHANGE_MAP "  {0x%04x, 0x%04x}\n", $pos, $utf_code;
	}
	else
	{
		printf CHANGE_MAP "  {0x%04x, 0x%04x},\n", $pos, $utf_code;
	}
}
 
print CHANGE_MAP "};\n";
close(CHANGE_MAP);
 
$change_map_file = "utf8_to_gb18030_2022.map";
open(CHANGE_MAP, "> $change_map_file") || die("cannot open $change_map_file");
print CHANGE_MAP "static pg_utf_to_local ULmapGB18030_2022[ $number ] = {\n";
 
$count = $number;
for $pos (sort { $a <=> $b } keys(%code_u))
{
	$code_gb = $code_u{$pos};
	$count--;
	if ($count == 0)
	{
		printf CHANGE_MAP "  {0x%04x, 0x%04x}\n", $pos, $code_gb;
	}
	else
	{
		printf CHANGE_MAP "  {0x%04x, 0x%04x},\n", $pos, $code_gb;
	}
}
 
print CHANGE_MAP "};\n";
close(CHANGE_MAP);
