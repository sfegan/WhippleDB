#!/usr/bin/perl -w

use lib qw(/home/observer/database/code);

use strict;

use File::stat;
use FileHandle;

use Whipple::WhipDB;

use vars qw($ARGV);

use constant DATA_DIR => '/data/raw10';
use constant GLOBBER  => 'gt*';
use constant PREFERRED_MEDIUM => 'cd';
use constant PREFERRED_FSFORMAT => 'iso9660/rr';

my $db=new Whipple::WhipDB(0);

my %Runs=();

my $ListNoDataFileEntries=0;

foreach ( @ARGV )
  {
    if ( $_ eq "-L" )
      {
	$ListNoDataFileEntries=1;
      }
  }

print "Querying database for unarchived runs\n";
my $UnArchCount=$db->RunIdent->get_runs_without_archive;
if(not $UnArchCount)
  {
    if($db->RunIdent->err)
      {
	my $err=$db->RunIdent->errstr;
	chomp $err;
	print STDERR "Could not retrieve list of unarchived files\n";
	$db->rollback;
	die $err;
      }

    print STDERR "No files found to archive\n";
  }

print "Found ".$UnArchCount." files that are not in any archive\n";

my $rv;
while(defined($rv=$db->RunIdent->fetchrow_hashref))
  {
    foreach ( keys %{$rv} )
      {
	$Runs{$rv->{"run_id"}}->{$_}=$rv->{$_};
      }
  }

if($db->RunIdent->err)
  {
    my $err=$db->RunIdent->errstr;
    chomp $err;
    print STDERR "Error while extracting list of unarchived files\n";
    $db->rollback;
    die $err;
  }
$db->commit;

print "Retrieving datafile info for unarchived runs from the database\n";
my $DataFileRecsFound=$db->DataFile->get_runs_without_archive;
if(not $DataFileRecsFound)
  {
    if($db->DataFile->err)
      {
	my $err=$db->DataFile->errstr;
	chomp $err;
	print STDERR "Could not get datafile info for unarchived files\n";
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
	if((not exists($Runs{$run_id}->{"bytes"})) or
	   ($rv->{"bytes"} < $Runs{$run_id}->{"bytes"}))
	  {
	    foreach ( keys %{$rv} )
	      {
		$Runs{$run_id}->{$_}=$rv->{$_};
	      }
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

#use Data::Dumper;
#print STDERR Dumper(\%Runs);

my @RunIDList=
  sort({ ( $Runs{$a}->{"utc_date"} eq $Runs{$b}->{"utc_date"} ) ?
	 ( $Runs{$a}->{"run_no"} <=> $Runs{$b}->{"run_no"} ) :
	 ( $Runs{$a}->{"utc_date"} cmp $Runs{$b}->{"utc_date"} ) }
       keys %Runs);

#
# Verify that we have datafile entries for all files 
#

my @NoDataFileList;
my $run_id;
foreach $run_id ( @RunIDList )
  {
    push @NoDataFileList,$run_id
      if(not exists $Runs{$run_id}->{"bytes"});
  }

if(scalar(@NoDataFileList))
  {
    print STDERR
      "Found ",scalar(@NoDataFileList)," runs which have no datafile entry:\n";

    if($ListNoDataFileEntries==1)
      {
	print STDERR
	  join("\n",
	       map({ "\t".$Runs{$_}->{"utc_date"}."\t".$Runs{$_}->{"run_no"} }
		   @NoDataFileList),"");
	undef $db;
	exit;
      }
    else
      {
	print STDERR " ... run again with \"-L\" option to list these files\n";
      }
  }

foreach $run_id ( @NoDataFileList )
  {
    delete $Runs{$run_id};
  }

@RunIDList=
  sort({ ( $Runs{$a}->{"utc_date"} eq $Runs{$b}->{"utc_date"} ) ?
	 ( $Runs{$a}->{"run_no"} <=> $Runs{$b}->{"run_no"} ) :
	 ( $Runs{$a}->{"utc_date"} cmp $Runs{$b}->{"utc_date"} ) }
       keys %Runs);

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

my $Media=$db->Descriptor->get("Medium_Codes");
if(not defined $Media)
  {
    my $err=$db->Descriptor->errstr;
    chomp $err;
    print STDERR "Error while extracting media_codes info\n";
    $db->rollback;
    die $err;
  }
$db->commit;
my $FSFormat=$db->Descriptor->get("FS_Format_Codes");
if(not defined $FSFormat)
  {
    my $err=$db->Descriptor->errstr;
    chomp $err;
    print STDERR "Error while extracting media_codes info\n";
    $db->rollback;
    die $err;
  }
$db->commit;

my $PMedia=$Media->ID(PREFERRED_MEDIUM);
if(not defined $PMedia)
  {
    print STDERR "Medium ",PREFERRED_MEDIUM," not found!";
    print STDERR "We only have ",join(" ",$Media->Vals),"\n";
    exit;
  }
my $MediumSize=$Media->Item($PMedia)->{"kb_capacity"};
my $MediumCanWaste=$Media->Item($PMedia)->{"kb_can_waste"};
print("Preferred medium is ",uc(PREFERRED_MEDIUM),
      ", capacity is ",PrintKB($MediumSize),
      " (max empty space is ",PrintKB($MediumCanWaste),")\n");

while(1) # LOOP UNTIL WE FINISH ALL FILES
  {
    my @RunIDList=
      sort({ ( $Runs{$a}->{"utc_date"} eq $Runs{$b}->{"utc_date"} ) ?
	     ( $Runs{$a}->{"run_no"} <=> $Runs{$b}->{"run_no"} ) :
	     ( $Runs{$a}->{"utc_date"} cmp $Runs{$b}->{"utc_date"} )}
	   keys %Runs);

    my $KBytesSum=0;
    my $FilesRemaining=0;
    foreach $run_id ( @RunIDList )
      {
	$KBytesSum+=KB($Runs{$run_id}->{"bytes"});
	$FilesRemaining++;
      }

    print("Unarchived files: ",$FilesRemaining," with ",
	  PrintKB($KBytesSum),"\n");

    if($KBytesSum < $MediumSize)
      {
	print("Insufficient data to record a ",PREFERRED_MEDIUM,
	      " at this time.\n");
	last;
      }

    my $SpaceRemaining=$MediumSize;
    
    my %SizeByDate;
    foreach(@RunIDList)
      {
	$SizeByDate{$Runs{$_}->{"utc_date"}}=0 
	  unless exists $SizeByDate{$Runs{$_}->{"utc_date"}};
	$SizeByDate{$Runs{$_}->{"utc_date"}}+=KB($Runs{$_}->{"bytes"});
      }

    my $LastDate="bogus-date";
    my @TakeMe;

    foreach $run_id (@RunIDList)
      {
	my $kbytes_me=KB($Runs{$run_id}->{"bytes"});
	my $my_date=$Runs{$run_id}->{"utc_date"};
	last if($kbytes_me > $SpaceRemaining);

	if($LastDate ne $my_date)
	  {
	    last if
	      (($SizeByDate{$my_date} < $MediumSize) and
	       ($SpaceRemaining < $MediumCanWaste) and
	       ($SizeByDate{$my_date} > $SpaceRemaining));
	  }
	$LastDate=$my_date;

	push(@TakeMe,$run_id);
	$SpaceRemaining-=$kbytes_me;
      }
    
    my $ArchID=$db->ArchInfo->GetLastArchive;
    if(not defined $ArchID)
      {
	my $err=$db->ArchInfo->errstr;
	chomp $err;
	print STDERR "Error while finding last Arch_ID info\n";
	$db->rollback;
	die $err;
      }
    $ArchID++;

    if(not $db->ArchInfo->insert($ArchID,$Media->ID(PREFERRED_MEDIUM),
				 $FSFormat->ID(PREFERRED_FSFORMAT)))
      {
	my $err=$db->ArchInfo->errstr;
	chomp $err;
	print STDERR "Error while creating Arch_Info record\n";
	$db->rollback;
	die $err;
      }

    my $ArchFile=$db->ArchFile;
    foreach $run_id (@TakeMe)
      {
	if(not $ArchFile->insert($run_id,$Runs{$run_id}->{"file_type"},
				 $Runs{$run_id}->{"compress"},$ArchID))
	  {
	    my $err=$db->ArchInfo->errstr;
	    chomp $err;
	    print(STDERR "Error while inserting run_id ",$run_id,
		  "into Arch_File\n");
	    $db->rollback;
	    die $err;
	  }
      }
    $db->commit;

    print("Created archive ",$ArchID,", ",scalar(@TakeMe)," files, ",
	  "space used ".PrintKB($MediumSize-$SpaceRemaining),"\n");

    foreach $run_id ( @TakeMe )
      {
	delete $Runs{$run_id};
      }
  }

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

sub KB
  {
    my $bytes=shift;
    return int(($bytes+1023)/1024);
  }

sub PrintKB
  {
    my $KB=shift;
    return sprintf("%.1f kB",$KB) if($KB<1024);
    $KB=$KB/1024;
    return sprintf("%.1f MB",$KB) if($KB<1024);
    $KB=$KB/1024;
    return sprintf("%.1f GB",$KB) if($KB<1024);
    $KB=$KB/1024;
    return sprintf("%.1f TB",$KB) if($KB<1024);
  }
