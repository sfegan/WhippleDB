#!/usr/bin/perl -w

use strict;

#
# Declare constants
#

my $contents_file      = 'contents';
my $dst_base           = '/var/www/html/log/';
my $src_base           = 'http://veritas.sao.arizona.edu/log/';
my $src_user           = 'veritas';
my $src_pass           = '*Nizamu';

#
# Declare variables
#

my @UT=();

#
# Gather command line options
#

while(defined(my $arg=shift(@ARGV)))
{
  if ( $arg =~ /^\d{6,7}$/ ) 
    {
      # If the arguement is composed of digits only then
      # presume that it is a UT date.
      push @UT,$arg;
    }
  elsif ( $arg =~ /^d(\d{6,7})$/ ) 
    {
      # If the arguement is composed of something like d000101 then
      # presume that it is a UT date.
      push @UT,$1;
    }
  else 
    { 
      print($0,": Arguement does not seem to be UT date: ",
	    $arg,"\n\n");
    }
}

#
# Figure out the current UT date if none is given
#

if ( scalar(@UT) == 0 )
  {
    # Take the current time and subtract 1 hour so that you
    # can transfer_10 last nights stuff until about 6pm
    # the following day (during winter)
    my $time=time-1*60*60;
    my @datecomponents=gmtime;
    my $UT=sprintf("%2.2d%2.2d%2.2d",$datecomponents[5]%100,
		   $datecomponents[4]+1,$datecomponents[3]);
    print("* -- No UT dates given, will transfer log for ",
	  $UT,"\n\n");
    push @UT,$UT
}

#
# Tidy some variables up
#

$src_base.="/" unless ( $src_base=~/\/$/ );
$dst_base.="/" unless ( $dst_base=~/\/$/ );

#
# Loop through all the UT dates given.
#

foreach my $UT ( @UT )
  {
    $UT += 20000000 if($UT < 900000);
    $UT += 19000000 if($UT < 1000000);

    my ($year,$month,$date)=($UT=~/(....)(..)(..)/);
    my $yearshort=sprintf("%2.2d",$year%100);

    my $src_url=
      join("",$src_base,'logs_',$year,'/d',$yearshort,$month,
	   '/d',$yearshort,$month,$date,'.log_10');

    my $dst_dir = join("",$dst_base,'logs_',$year,'/d',$yearshort,$month);

    my $dst_file = join("",$dst_dir,'/d',$yearshort,$month,$date,'.log_10');
    
    # Make sure the destination directory exists.   
    if ( not -d $dst_dir )
      {
	system('mkdir','-p',$dst_dir);
	print("Could not make directory: ",$dst_dir) if ( not -d $dst_dir );
	next;
    }

    unlink($dst_file) if ( -e $dst_file );

    my @args;
    #push(@args,'echo');
    push(@args,'wget');
    push(@args,'--http-user',$src_user) if ( $src_user );
    push(@args,'--http-password',$src_pass) if ( $src_user  &&  $src_pass );
    push(@args,'-O',$dst_file);
    push(@args,$src_url);
    system(@args);

    unlink($dst_file) if ( ( not -e $dst_file ) or ( -z $dst_file ) );
  }
