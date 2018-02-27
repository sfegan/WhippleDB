#!/usr/bin/perl -w

use strict;

use File::stat;
use FileHandle;

use Whipple::WhipDB;

my $db=new Whipple::WhipDB(0);

my $ndel=$db->Search->delete_old;
if((not $ndel) && ($db->Search->err))
  {
    my $err=$db->Search->errstr; chomp $err;
    print("Error deleting from Search table\n");
    print($err,"\n");
    $db->rollback;
    next;
  }

print STDERR "Deleted ",$ndel," entries from the Search table\n";

$ndel=$db->SearchResults->delete_unmatched;
if((not $ndel) && ($db->Search->err))
  {
    my $err=$db->SearchResults->errstr; chomp $err;
    print("Error deleting from SearchResults table\n");
    print($err,"\n");
    $db->rollback;
    next;
  }

print STDERR "Deleted ",$ndel," entries from the Search Results table\n";

$ndel=$db->SearchTerm->delete_unmatched;
if((not $ndel) && ($db->Search->err))
  {
    my $err=$db->SearchTerm->errstr; chomp $err;
    print("Error deleting from SearchTerm table\n");
    print($err,"\n");
    $db->rollback;
    next;
  }

print STDERR "Deleted ",$ndel," entries from the Search Term table\n";

$db->commit;

system("vacuumdb whipple");

