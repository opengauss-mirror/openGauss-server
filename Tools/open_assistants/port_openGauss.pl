#!/usr/bin/perl

use strict;
use warnings;
use File::Basename;
use File::Path qw(make_path remove_tree);
use Cwd;

my $gausskernel_dir = $ARGV[0];
my $opengauss_dir   = $ARGV[1];

sub usage
{
    print "  usage:\n";
    print "    perl port_openGauss.pl GaussDBKernel-server-directory openGauss-server-directory\n";
    print "  \n";
}

sub valid_line
{
    my ($l) = @_;
    $l =~ s/^\s+//g;
    $l =~ s/\s+$//g;
    return 1 if $l;
    return 0;
}

sub prepare_parentdir
{
    my $dir = $_[0];
    $dir =~ s/\/*$//g;
    die "there is no such a directory $dir" unless $dir;

    my $parentdir = dirname($dir);
    make_path $parentdir unless -d $parentdir;
}

if ( !$opengauss_dir || !$gausskernel_dir || $gausskernel_dir eq "-h" || $gausskernel_dir eq "--help" ) {
    usage();
    exit(-1);
}
if (! -d $opengauss_dir || ! -d $gausskernel_dir ) {
    print "ERROR: $opengauss_dir or $gausskernel_dir does not exist!";
}

$opengauss_dir =~ s{/+$}{}g;
$gausskernel_dir =~ s{/+$}{}g;

my $open_assist_dir = dirname(__FILE__);
if ($open_assist_dir !~ m/^\//) {
    $open_assist_dir = cwd() . '/' . $open_assist_dir;
}
$open_assist_dir =~ s/\/\.$//;

my $opengauss_fileset = "$open_assist_dir/opengauss_fileset";
my @overwrite_fileset;
my @delete_fileset;

open my $fset, "<", $opengauss_fileset or die "cannot open $opengauss_fileset: $!\n";
my $file_type = "none";
while(my $line=<$fset>) {
    chomp $line;
    if ($line =~ /\[overwrite\]/) {
        $file_type = "overwrite";
        next;
    }
    elsif ($line =~ /\[delete\]/) {
        $file_type = "delete";
        next;
    }

    if ($file_type eq "overwrite") {
        push @overwrite_fileset, $line;
    }
    elsif ($file_type eq "delete") {
        push @delete_fileset, $line;
    }
}

print "[" . localtime() . "] synchronizing directories and files.\n";
foreach my $d(qw/src contrib/) {
    if ( -d "$opengauss_dir/$d" ) {
        remove_tree("$opengauss_dir/$d");
        print "removed $opengauss_dir/$d\n";
    }
    make_path("$opengauss_dir/$d");
    print "created $opengauss_dir/$d\n";
}

foreach my $f(@overwrite_fileset) {
    next unless valid_line($f);
    if ( -d "$gausskernel_dir/$f") {
        prepare_parentdir("$opengauss_dir/$f");
        remove_tree("$opengauss_dir/$f") if -d "$opengauss_dir/$f";
        system("cp -fr $gausskernel_dir/$f $opengauss_dir/$f") == 0 or print "ERROR: copy $gausskernel_dir/$f failed\n";
        print "copied $opengauss_dir/$f\n";
    }
    elsif ( -f "$gausskernel_dir/$f") {
        system("cp -f $gausskernel_dir/$f $opengauss_dir/$f") == 0 or print "ERROR: copy $gausskernel_dir/$f failed\n";
        print "copied $opengauss_dir/$f\n";
    }                                         
}

foreach my $f(@delete_fileset) {
    next unless valid_line($f);
    if ( -d "$opengauss_dir/$f") {
        remove_tree("$opengauss_dir/$f");
        print "deleted $opengauss_dir/$f\n";
    }
    elsif ( -f "$opengauss_dir/$f") {
        unlink "$opengauss_dir/$f";
        print "deleted $opengauss_dir/$f\n";
    }
}

print "[" . localtime() . "] synchronized directories and files.\n";
