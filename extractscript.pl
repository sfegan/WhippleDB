#!/usr/bin/perl -w

use lib qw(/home/sfegan/database/code);

use strict;

use File::stat;
use FileHandle;

use Whipple::WhipDB;

use vars qw($ARGV);

my $db=new Whipple::WhipDB(0);

my $codes_ft=$db->Descriptor->get("File_Type_Codes");
my $codes_co=$db->Descriptor->get("Compress_Codes");
my $codes_me=$db->Descriptor->get("Medium_Codes");
my $codes_fs=$db->Descriptor->get("FS_Format_Codes");

sub AssembleFileName;
sub getArchive;    

my %Archives;

my %MakeDirectory;

foreach ( <ARGV> )
  {
    chomp;
    
    next if((!/^pr/)&&(!/^n2/));

    my ($pr,$on,$off,$n2,$date)=("pr","none","none","","");
    
    if(/^pr/)
      {
	($pr,$on,$off,$n2,$date)=split /\s+/;
	$pr="ON";
      }
    elsif(/^n2/)
      {
	($pr,$on,$date)=split /\s+/;
	$pr="NITROGEN";
      }

    my ($archid,$filename);
    
    ($archid,$filename)=getArchive($on,$date);
    if(defined $archid)
      {
	$MakeDirectory{$date}=1;
	push @{$Archives{$archid}},$filename;
      }
    else
      {
	print STDERR "No record of ",$pr," file ",$on," on date ",$date,"\n";
      }
    
    if($off ne "none")
      {
	($archid,$filename)=getArchive($off,$date);
	if(defined $archid)
	  {
	    push @{$Archives{$archid}},$filename;
	  }
	else
	  {
	    print STDERR "No record of OFF file ",$off," on date ",$date,"\n";
	  }
      }
  }

$db->disconnect;

print "#!/bin/bash\n";

my $makedir;
foreach $makedir ( sort { $a <=> $b } keys %MakeDirectory )
  {
    print "mkdir d",$makedir,"\n"
  }

my $archid;
foreach $archid ( sort { $a <=> $b } keys %Archives )
  {
    print 
      join("\n",
	   '# ARCHIVE '.$archid,
	   'echo -n Please insert archive '.$archid.' and press enter --',
	   'read',
	   'echo Loading CD...',
	   'eject -t',
	   'echo Waiting 5 seconds',
	   'sleep 5',
	   'echo Mounting CD',
	   'mount /mnt/cdrom',
	   '');

    my $filename;
    foreach $filename ( @{$Archives{$archid}} )
      {
	print "echo cp /mnt/cdrom",$filename->[0]," ",$filename->[1],"\n";
	print "cp /mnt/cdrom",$filename->[0]," ",$filename->[1],"\n";
      }
    
    print 
      join("\n",
	   'echo Unmounting CD',
	   'umount /mnt/cdrom',
	   'echo Ejecting',
	   'eject',
	   '');
  }

sub getArchive
  {
    my ($runno,$shortdate)=@_;
    
    if(! $runno=~/^[g][t]([01-9]+)$/ )
      {
	die "Unrecognised runno ".$runno;
      }
    $runno=~/^[g][t]([01-9]+)$/;
    $runno=int($1);

    my $utdate=$shortdate;
    $utdate += 20000000 if($utdate < 900000);
    $utdate += 19000000 if($utdate < 1000000);

    my $ArchFileCount=$db->ArchFile->archfileinfo_run($runno,$utdate."");
    if(not $ArchFileCount)
      {
	if($db->ArchFile->err)
	  {
	    my $err=$db->ArchFile->errstr;
	    chomp $err;
	    print STDERR "Could not get archfile info for run_no ".$runno;
	    $db->rollback;
	    die $err;
	  }
        $db->ArchFile->finish;
	return undef;
      }

    my $rv=$db->ArchFile->fetchrow_hashref;
    
    my $runid=$rv->{"run_id"};
    my $ft=$codes_ft->Val($rv->{"file_type"});
    my $comp=$codes_co->Val($rv->{"compress"});
    my $fs=$codes_fs->Val($rv->{"fs_format"});
    my $archid=$rv->{"arch_id"};

    my $filename=AssembleFileName($utdate,$runno,$fs,$ft,$comp);

    $db->ArchFile->finish;
    $db->commit;

    return $archid,$filename;
  }

sub AssembleFileName
{
  my ($date,$runno,$fs,$ft,$comp)=@_;
  my $key;
  my ($iext,$oext);
  
  my %ICC=(
	   "iso9660/gdf/none"        => "fz",
	   "iso9660/gdf/compress"    => "fzz",
	   "iso9660/gdf/gzip"        => "fzg",
	   "iso9660/rr/gdf/none"     => "fz",
	   "iso9660/rr/gdf/compress" => "fz.Z",
	   "iso9660/rr/gdf/gzip"     => "fz.gz",
	   "iso9660/rr/gdf/bzip2"    => "fz.bz2",
	  );

  my %OCC=(
	   "gdf/none"        => "fz",
	   "gdf/compress"    => "fz.Z",
	   "gdf/gzip"        => "fz.gz",
	   "gdf/bzip2"       => "fz.bz2",
	  );

  $date =~ s/\d\d(\d\d)(\d\d)(\d\d)/$1$2$3/;
  $runno = sprintf("%6.6d",$runno);
  
  $key=join("/",$fs,$ft,$comp);
  $iext=$ICC{$key};

  $key=join("/",$ft,$comp);
  $oext=$OCC{$key};

  return ["/d".$date."/gt".$runno.".".$iext , "d".$date."/gt".$runno.".".$oext];
}
