#!/usr/bin/perl

use warnings;
use strict;
use Getopt::Long;
use File::Basename qw(dirname);

our $show_usage;
our $verbose;
our $ignored_dir;
our $asanlog_dir;
our $output;

use constant {
    SEARCH_KEY_LEN => 64,
};

sub usage() {
    print "perl asan_report.pl --asanlog-dir /Directory/to/AddressSanitize/Log/ \n";
    print "perl asan_report.pl --asanlog-dir /Directory/to/AddressSanitize/Log/ --output /Path/to/Output/File --ignore-dir /Directory/to/Ignored/Results/ \n";
    print "perl asan_report.pl --help \n";
}

sub get_asan_log_dir {
    my $log_file_dir = $asanlog_dir;
    
    unless ($asanlog_dir) {
        my $asan_option  = $ENV{'ASAN_OPTIONS'};
        if ($asan_option =~ /log_path=([\w\/\.\-\s]+)/) {
            $log_file_dir = dirname $1;
        }
    }

    $log_file_dir =~ s/\/+$//g;

    $log_file_dir;
}

sub check_component {
    my($line_block) = @_;

    my $component = '';
    for my $line(@$line_block) {
        if ($line =~ m/^\s+#(\d+)\s+(\w+)\s+in\s+(\w+)\s+(\S+)$/) {
            my($index, $addr, $func, $file_lineno) = ($1, $2, $3, $4);
            my ($file, $lno) = split (/:/, $file_lineno);
            if ($func eq 'main') {
                if ($file =~ m{/Code/src/}) {
                    my $fn = $';
                    if ($fn =~ m{^(gtm|cm)/}) {
                        $component = $1;
                    }
                    elsif ($fn =~ m{^backend/}) {
                        $component = 'gaussdb';
                    }
                    elsif ($fn =~ m{^bin/(\w+)/}) {
                        $component = $1
                    }
                    else {
                        print "Unknown main location: $line\n"
                    }

                    return $component if $component;
                }
            }
            else {
                # last result will win
                if ($file =~ m{/Code/src/}) {
                    my $fn = $';
                    if ($fn =~ m{^(gtm|cm)/}) {
                        $component = $1;
                    }
                    elsif ($fn =~ m{^backend/}) {
                        $component = 'gaussdb';
                    }
                    elsif ($fn =~ m{^bin/(\w+)/}) {
                        $component = $1
                    }
                }
            }
        }
    }

    $component = 'unknown' unless $component;
    $component;
}

sub is_call_stack_unique {
    my ($ignored_call_stacks, $uniq_call_stacks, $line_block) = @_;

    my $search_key = '';
    my $signature  = '';
    for my $line(@$line_block) {
        if ($line =~ m/^\s+#(\d+)\s+(\w+)\s+in\s+(\w+)\s+(\S+)$/) {
            my($index, $addr, $func, $file_lineno) = ($1, $2, $3, $4);
            my ($file, $lno) = split (/:/, $file_lineno);
            if ($file =~ m{/Code/src/}) {
                $file = "src/" . $';
                if (defined $lno) {
                    $search_key = $search_key . "$func:$file:$lno";
                }
                else {
                    $search_key = $search_key . "$func:$file";
                }
            }
            if (defined $lno) {
                $signature = $signature . "$func:$file:$lno";
            }
            else {
                $signature = $signature . "$func:$file";
            }
        }
    }

    if (ref $ignored_call_stacks eq 'HASH') {
        if ( exists $ignored_call_stacks->{substr($search_key, 0, SEARCH_KEY_LEN)}  ) {
            for my $sig(@{ $ignored_call_stacks->{substr($search_key, 0, SEARCH_KEY_LEN)} } ) {
                return 0 if ($sig eq $signature);
            }
        }
    }
    
    if ( exists $uniq_call_stacks->{substr($search_key, 0, SEARCH_KEY_LEN)}  ) {
        for my $sig(@{ $uniq_call_stacks->{substr($search_key, 0, SEARCH_KEY_LEN)} } ) {
            return 0 if ($sig eq $signature);
        }
    }
    else {
        $uniq_call_stacks->{substr($search_key, 0, SEARCH_KEY_LEN)}  = []
    }
    
    push @{$uniq_call_stacks->{substr($search_key, 0, SEARCH_KEY_LEN)}}, $signature;
    
    return 1;
}

sub gen_report {
    my ($log_dir, $ignored_call_stacks, $output_file) = @_;

    return {} unless $log_dir;
    
    my %uniq_call_stacks;
    my @mem_leak_block = ();
    my @addr_issue_block = ();

    for my $file(glob "${log_dir}/*") {
        open my $fh, "<$file" or next;
        my $type = 'none';
        my @file_content = ();
        while(<$fh>) {
            push @file_content, $_;
        }
        close $fh;

        my $component = check_component(\@file_content);
        if ($output_file) {
        }
        foreach my $line(@file_content) {
            chomp $line;
            next unless $line =~ /\s*\S+/;

            # headline
            # assuming one type error
            if ($line =~ /^==\d+==ERROR:/) {
                if ($line =~ /LeakSanitizer:/) {
                    $type = 'memory-leak';
                }
                elsif ($line =~ /AddressSanitizer:/ && $line =~ /double-free/) {
                    $type = 'double-free';
                }
                elsif ($line =~ /AddressSanitizer:/ && $line =~ /attempting free/) {
                    $type = 'bad-free';
                }
                elsif ($line =~ /AddressSanitizer:\s+([-\w]+)/) {
                    $type = $1;
                }
                else {
                    print "Unknown error type in $file:\n$line\n" if $verbose;
                }
            }
            # block header
            elsif ($line =~ /(Direct|Indirect) leak/) {
                $type = 'memory-leak' if $type eq 'none';
                if (scalar @mem_leak_block) {
                    if (is_call_stack_unique($ignored_call_stacks, \%uniq_call_stacks, \@mem_leak_block)) {
                        if ($output_file) {
                            open MEMLEAK, ">>$output_file.memory-leak.$component";
                            print MEMLEAK "$_\n" foreach (@mem_leak_block);
                            print MEMLEAK "\n\n";
                            close MEMLEAK;
                        }
                    }
                    
                    @mem_leak_block = ();
                }

                # $type = 'memory-leak';
                push @mem_leak_block, $line;
            }
            elsif ($line =~ /^\s+#\d+/) {
                if ($type eq 'memory-leak') {
                    push @mem_leak_block, $line 
                }
                elsif ($type eq 'none') {
                    print "Unknown issue type for $file\n" if $verbose;
                }
                else {
                    push @addr_issue_block, $line 
                }
            }

            # start another block
            elsif ( $line =~ /^\w+/ || $line =~ /^={3,}/ ) {
                if ($type eq 'memory-leak' && scalar @mem_leak_block) {
                    if (is_call_stack_unique($ignored_call_stacks, \%uniq_call_stacks, \@mem_leak_block)) {
                        if ($output_file) {
                            open MEMLEAK, ">>$output_file.memory-leak.$component";
                            print MEMLEAK "$_\n" foreach (@mem_leak_block);
                            print MEMLEAK "\n\n";
                            close MEMLEAK;
                        }
                    }
                    
                    @mem_leak_block = ();
                }
                elsif ($type eq 'none' && (scalar @addr_issue_block || scalar @mem_leak_block)) {
                    print "\n\n***FATAL***: you should not see me at all.\n";
                    print "Check in $file:\n$line\n";
                        
                }
                elsif (scalar @addr_issue_block) {
                    # Fatal address issue:
                    # 1. stack-buffer-overflow/heap-buffer-overflow
                    # 2. double-free
                    #
                    # Assuming only ONE issue in each log file for above issue type

                    last
                }
                else {
                    # first block for fatal address issue is not yet filled
                    # second or subsequent blocks for memory leak
                }
            }
        }

        close $fh;

        if ($type eq 'memory-leak' && scalar @mem_leak_block) {
            if (is_call_stack_unique($ignored_call_stacks, \%uniq_call_stacks, \@mem_leak_block)) {
                if ($output_file) {
                    open MEMLEAK, ">>$output_file.memory-leak.$component";
                    print MEMLEAK "$_\n" foreach (@mem_leak_block);
                    print MEMLEAK "\n\n";
                    close MEMLEAK;
                }
            }
        }
        elsif ($type ne 'none' && scalar @addr_issue_block) {
            if (is_call_stack_unique($ignored_call_stacks, \%uniq_call_stacks, \@addr_issue_block)) {
                open my $ifh, "<$file" or next;
                open my $ofh, ">>$output.$type.$component" or next;
                while (<$ifh>) {
                    print $ofh $_;
                }
                print $ofh "\n\n";
                close $ifh;
                close $ofh;
            }
        }

        @mem_leak_block = ();
        @addr_issue_block = ()
    } 
    ## for my $file(glob "${log_file_trunk}*") {

    close MEMLEAK;

    \%uniq_call_stacks
}


GetOptions (
    "ignore-dir=s"  => \$ignored_dir,
    "asanlog-dir=s" => \$asanlog_dir,
    "output|o=s"    => \$output,
    "verbose"       => \$verbose,
    "help|h"        => \$show_usage
) or die("Error in command line arguments\n");

if ($show_usage) {
    usage();
    exit 0;
}
        
$output = "$$" unless $output;

my $ignored_call_stacks = gen_report($ignored_dir, undef, undef);
my $log_file_dir      = get_asan_log_dir();

gen_report($log_file_dir, $ignored_call_stacks, $output);
