#!/usr/bin/perl -w

use strict;

use File::stat;
use FileHandle;

use Whipple::WhipDB;

use vars qw($ARGV);

use constant DATA_DIR => '/data/raw10';
use constant GLOBBER  => 'gt*';
use constant TMP_DIR  => '/data/archive/iso/makecd_tmp_dir';
use constant ISOFS    => '/data/archive/iso/archive.iso';

my $db=new Whipple::WhipDB(0);

my $ArchID=$ARGV[0];
if (not $ArchID)
  {
    print STDERR "Must supply Archive ID\n";
    undef $db;
    exit;
  }

my $Found=$db->ArchInfo->select_archid($ArchID);
if(not $Found)
  {
    if($db->ArchInfo->err)
      {
	my $err=$db->ArchInfo->errstr;
	chomp $err;
	print STDERR "Could not retreive archive info\n";
	$db->rollback;
	die $err;
      }
    
    print "Could not find archive ",$ArchID,"\n";
    exit;
  }
$db->commit;

my %Runs=();

print "Querying database for runs\n";
my $ArchCount=$db->ArchFile->select_archid($ArchID);
if(not $ArchCount)
  {
    if($db->ArchFile->err)
      {
	my $err=$db->ArchFile->errstr;
	chomp $err;
	print STDERR "Could not retrieve list of archived files\n";
	$db->rollback;
	die $err;
      }

    print STDERR "No files found to archive\n";
  }

print "Found ".$ArchCount." files in archive\n";

my $rv;
while(defined($rv=$db->ArchFile->fetchrow_hashref))
  {
    foreach ( keys %{$rv} )
      {
	$Runs{$rv->{"run_id"}}->{$_}=$rv->{$_};
      }
  }

if($db->ArchFile->err)
  {
    my $err=$db->ArchFile->errstr;
    chomp $err;
    print STDERR "Error while extracting list of archived files\n";
    $db->rollback;
    die $err;
  }
$db->commit;

print "Retrieving datafile info for archived runs from the database\n";
my $DataFileRecsFound=$db->DataFile->select_archid($ArchID);
if(not $DataFileRecsFound)
  {
    if($db->DataFile->err)
      {
	my $err=$db->DataFile->errstr;
	chomp $err;
	print STDERR "Could not get datafile info for archived files\n";
	$db->rollback;
	die $err;
      }
  }
else
  {
    print "Found ",$DataFileRecsFound," datafile records\n";
    while(defined($rv=$db->DataFile->fetchrow_hashref))
      {
	my $run_id=$rv->{"run_id"};
	foreach ( keys %{$rv} )
	  {
	    $Runs{$run_id}->{$_}=$rv->{$_};
	  }
      }
  }

if($db->DataFile->err)
  {
    my $err=$db->DataFile->errstr;
    chomp $err;
    print STDERR "Error while extracting datafile info\n";
    $db->rollback;
    die $err;
  }
$db->commit;

#
# See if we can find the files we need... actually we don't need to do this
# it has to be done in the writecd program
#

my $FileTypes=$db->Descriptor->get("File_Type_Codes");
if(not defined $FileTypes)
  {
    my $err=$db->Descriptor->errstr;
    chomp $err;
    print STDERR "Error while extracting file_type_codes info\n";
    $db->rollback;
    die $err;
  }
$db->commit;

my $Compress=$db->Descriptor->get("Compress_Codes");
if(not defined $Compress)
  {
    my $err=$db->Descriptor->errstr;
    chomp $err;
    print STDERR "Error while extracting compress_codes info\n";
    $db->rollback;
    die $err;
  }
$db->commit;

#
# See if the files are still around
#

my $run_id;
my @RunIDList=
  sort({ ( $Runs{$a}->{"utc_date"} eq $Runs{$b}->{"utc_date"} ) ?
	 ( $Runs{$a}->{"run_no"} <=> $Runs{$b}->{"run_no"} ) :
	 ( $Runs{$a}->{"utc_date"} cmp $Runs{$b}->{"utc_date"} ) }
       keys %Runs);

print "Locating files on disk...\n";
my @FileCantFind;
my @FileWrongSize;
foreach $run_id ( @RunIDList )
  {
    my $ft=$FileTypes->Val($Runs{$run_id}->{"file_type"});
    my $comp=$Compress->Val($Runs{$run_id}->{"compress"});
    my $runno=$Runs{$run_id}->{"run_no"};
    my $date=$Runs{$run_id}->{"utc_date"};
    my $bytes=$Runs{$run_id}->{"bytes"};
    my $filename=AssembleFileName($date,$runno,$ft,$comp);

    my $st=stat($filename);
    push @FileCantFind,$run_id if(not defined($st));
    
    push @FileWrongSize,$run_id if((defined($st)) and ($st->size != $bytes));
  }

if(scalar(@FileCantFind))
  {
    print STDERR
      join("\n",
	   "Some runs have no disk files:",
	   map({ "\t".$Runs{$_}->{"utc_date"}."\t".$Runs{$_}->{"run_no"} }
	       @FileCantFind),"");
    undef $db;
    exit;
  }

if(scalar(@FileWrongSize))
  {
    print STDERR
      join("\n",
	   "Some disk files are of unexpected size:",
	   map({ "\t".$Runs{$_}->{"utc_date"}."\t".$Runs{$_}->{"run_no"} }
	       @FileWrongSize),"");
    undef $db;
    exit;
  }

#
# Make the Links
#

system("echo","rm","-rf",TMP_DIR);
system("rm","-rf",TMP_DIR);

system("echo","mkdir",TMP_DIR);
system("mkdir",TMP_DIR);

my $old_date='bogus-date';
foreach $run_id ( @RunIDList )
  {
    my $ft=$FileTypes->Val($Runs{$run_id}->{"file_type"});
    my $comp=$Compress->Val($Runs{$run_id}->{"compress"});
    my $runno=$Runs{$run_id}->{"run_no"};
    my $date=$Runs{$run_id}->{"utc_date"};
    my $bytes=$Runs{$run_id}->{"bytes"};
    my $filename=AssembleFileName($date,$runno,$ft,$comp);

    $date =~ s/\d\d(\d\d)-(\d\d)-(\d\d)/d$1$2$3/;

    my $dir=$filename;
    $dir =~ s/^.*(d\d{6}\/[^\/]*)$/$1/;
    $dir = TMP_DIR."/".$dir;

    system("mkdir",TMP_DIR."/".$date) if ($old_date ne $date);
    $old_date=$date;
    symlink($filename,$dir);
  }

system("echo","mkisofs","-f","-R","-o",ISOFS,TMP_DIR);
system("mkisofs","-f","-R","-o",ISOFS,TMP_DIR);

print("Now you can record the image in the lower cd writer\n",
      "with the following command:\n",
      "    cdrecord -v speed=48 ",ISOFS,"\n",
      "\n",
      "and check afterwards, check it with:\n",
      "    diff /dev/cdrom ",ISOFS,"\n");

sub AssembleFileName
{
  my ($date,$runno,$ft,$comp)=@_;
  my $ext;

  my %CC=( "compress" => "Z",
	   "gzip"     => "gz",
	   "bzip2"    => "bz2" );

  $date =~ s/\d\d(\d\d)-(\d\d)-(\d\d)/$1$2$3/;
  $runno = sprintf("%6.6d",$runno);
  
  $ext="fz" if ( $ft eq "gdf" );
  $ext.=".".$CC{$comp} unless ( $comp eq "none" );

  return DATA_DIR."/d".$date."/gt".$runno.".".$ext;
}
