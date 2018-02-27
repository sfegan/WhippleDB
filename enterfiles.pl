#!/usr/bin/perl -w

use strict;

use File::stat;
use FileHandle;
use Digest::MD5;

use Whipple::WhipDB;

use vars qw($ARGV);

use constant DATA_DIR => '/data/raw10';
use constant GLOBBER  => 'gt*';

sub GetUT;
sub IdFile;

my %UTDATES;
my $utdate="default";

$UTDATES{"default"}=[] if ( scalar(@ARGV) == 0 );

my $arg;
foreach $arg ( @ARGV )
  {
    if ( $arg =~ /^d(\d{6})$/ )
      {
	$utdate=$1;
	$utdate += 20000000 if($utdate < 900000);
	$utdate += 19000000 if($utdate < 1000000);
	$UTDATES{$utdate}=[] if ( not exists $UTDATES{$utdate} );
      }
    elsif ( $arg =~ /^(\d+)$/ )
      {
	push @{$UTDATES{$utdate}},$1+0;
      }
#    elsif ( $arg =~ /d(\d{6})\/gt(\d+)/ )
#      {
#	$utdate=$1;
#	$utdate += 20000000 if($utdate < 900000);
#	$utdate += 19000000 if($utdate < 1000000);
#	push @{$UTDATES{$utdate}},$2+0;
#      }
    else
      {
	print STDERR "Do not recognise option: ",$arg,"\n";
      }
  }

push(@{$UTDATES{GetUT()}},@{delete $UTDATES{"default"}})
  if exists ( $UTDATES{"default"} );

my $db=new Whipple::WhipDB(0);
my $codes_ft=$db->Descriptor->get("File_Type_Codes");
my $codes_co=$db->Descriptor->get("Compress_Codes");

foreach $utdate ( sort keys %UTDATES )
  {
    my $utshort=sprintf("%6.6d",$utdate%1000000);
    my $Dir=DATA_DIR."/d".$utshort."/";
    my $utstring=$utdate;
    $utstring =~ s/^(....)(..)(..)/$1-$2-$3/;

    if ( not -d $Dir )
      {
	print $utstring,": No directory ".$Dir." .. skipping\n";
	next;
      }
    
    if(scalar(@{$UTDATES{$utdate}})==0)
      {
	foreach ( glob($Dir.GLOBBER) )
	  {
	    next unless ( /gt(\d+)/ );
	    push @{$UTDATES{$utdate}},$1+0;
	  }
	print $utstring,": Found ",scalar @{$UTDATES{$utdate}}," files\n";
      }


    my $runno;
    foreach $runno ( @{$UTDATES{$utdate}} )
      {
	my $file;
	foreach $file ( glob(sprintf("%sgt%6.6d*",$Dir,$runno)) )
	  {
	    my ($file_type, $compress)=IdFile $file;
	    if(not defined $file_type)
	      {
		print $utstring,": Don't recognise file: ",$file,"\n";
		next;
	      }

	    my $ftcode=$codes_ft->ID($file_type);
	    my $cocode=$codes_co->ID($compress);

	    if( $db->DataFile->select_run_fmt($runno,$utstring,
					      $ftcode,$cocode) > 0 )
	      {
		print($utstring,": Run ",$runno," already in DB as ",
		      $file_type,"/",$compress," .. skipping\n");
		next;
	      }
	    $db->commit;

	    my $sb=stat($file);
	    my $bytes=$sb->size;

	    my $fh=new FileHandle $file,"r";
	    if(not defined $fh)
	      {
		print $utstring,": Cannot open file: ",$file," ",$!,"\n";
		next;
	      }

	    my $MD5=new Digest::MD5;
	    $MD5->addfile($fh);
	    $fh->close;
	    
	    my $md5digest=$MD5->hexdigest;
	    undef $MD5;
	    
	    print($utstring,
		   ": Run ",$runno,
		   " Size ",sprintf("%-8d",$bytes),
		   " MD5 ",$md5digest,"\n");


	    my $ID=$db->RunIdent->GetOrCreateID($runno,$utstring);
	    if(not $ID)
	      {
		my $err=$db->RunIdent->errstr; chomp $err;
		print($utstring,": Error entering data into DB .. skipping\n");
		print($utstring,": ",$err,"\n");
		$db->rollback;
		next;
	      }
	    
	    if( not $db->DataFile->insert($ID,$ftcode,$cocode,
					  $bytes,$md5digest) )
	      {
		my $err=$db->DataFile->errstr; chomp $err;
		print($utstring,": Error entering data into DB .. skipping\n");
		print($utstring,": ",$err,"\n");
		$db->rollback;
		next;
	      }

	    $db->commit;
	  }
      }
  }

undef $db;

sub IdFile
  {
    my $File=shift;
    $File=~/^[^.]*[.](.*)$/;
    my $type=lc $1;

    my %Types = ( "fz.bz2" => [ "gdf", "bzip2" ],
		  "fz.gz"  => [ "gdf", "gzip" ],
		  "fz.z"   => [ "gdf", "compress" ],
		  "fz"     => [ "gdf", "raw" ],
		  "fzg"    => [ "gdf", "gzip" ],
		  "fzz"    => [ "gdf", "compress" ] );
    
    return (exists $Types{$type})?@{$Types{$type}}:undef;
  }

sub GetUT
{
  my $time=time;
  my @datecomponents=gmtime;
  my $UT=sprintf("%4.4d%2.2d%2.2d",$datecomponents[5]+1900,
		 $datecomponents[4]+1,$datecomponents[3]);
  return $UT;
}
