#!/usr/bin/perl -w
use strict;

my @entries;

foreach(<ARGV>)
{
    chomp;
    s/#.*//;
    s/^\s+//;
    s/\s+$//;
    
    my $source = substr($_,0,11);
    $source =~ s/\s//;
    
    my $rest = substr($_,12);
    $rest =~ s/^\s+//;
    my ($ra,$dec,$epoch,$pra,$pdec,$pepoch,$code) = split /\s+/,$rest;
    
    push @entries,[lc $code, lc $source, $ra, $dec, $epoch];
}

foreach(sort { $a->[0] cmp $b->[0] } @entries)
{
    printf("insert into source_catalog values ( '%s', '%s', %s, %s, %s );\n",@$_);
}

