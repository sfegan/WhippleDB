#!/usr/bin/perl -w

use lib qw(/home/observer/database/code);

use strict;

use File::stat;
use FileHandle;

use Whipple::WhipDB;

use vars qw($ARGV);

my $ArchID=$ARGV[0];
if (not $ArchID)
  {
    print STDERR "Must supply Archive ID\n";
    exit;
  }

my $db=new Whipple::WhipDB(0);

my %Runs=();

print STDERR "Querying database for runs\n";

my $rv;
my $RunIdentsFound=$db->RunIdent->select_archid($ArchID);
if(not $RunIdentsFound)
  {
    if($db->RunIdent->err)
      {
	my $err=$db->RunIdent->errstr;
	chomp $err;
	print STDERR "Could not get runident info for archived files\n";
	$db->rollback;
	die $err;
      }
  }
else
  {
    print STDERR "Found ",$RunIdentsFound," records\n";
    while(defined($rv=$db->RunIdent->fetchrow_hashref))
      {
	my $run_id=$rv->{"run_id"};
	foreach ( keys %{$rv} )
	  {
	    $Runs{$run_id}->{$_}=$rv->{$_};
	  }
      }
  }

if($db->RunIdent->err)
  {
    my $err=$db->RunIdent->errstr;
    chomp $err;
    print STDERR "Error while extracting RuNIdent info\n";
    $db->rollback;
    die $err;
  }
$db->commit;

my @Latex;

my @Stuff;
my %DateRuns;
my $run_id;

foreach $run_id ( keys %Runs )
  {
    my $date=$Runs{$run_id}->{"utc_date"};
    my $runno=$Runs{$run_id}->{"run_no"};
    push @{$DateRuns{$date}},$runno;
  }

my $date;
foreach $date ( sort { $a cmp $b } keys %DateRuns )
  {
    my $runtext=BunchNos(\@{$DateRuns{$date}});
    push @Stuff,'\item['.$date.'] '.$runtext;
  }

my $InW=11.8;
my $InH=11.8;

my @Dates=sort { $a cmp $b } keys %DateRuns;
my $CovW=13.5;
my $CovH=11.6;
my $CovS=0.45;
my $CovTitle=
  '\sbf Archive: '.$ArchID.' --- Dates: '.$Dates[0].' -- '.$Dates[-1];

push @Latex,
  '\documentclass{article}',
  '\newlength{\sjfhmargin}',
  '\newlength{\sjfvmargin}',
  '\setlength{\sjfhmargin}{0.4in}',
  '\setlength{\sjfvmargin}{0.4in}',
  '\setlength{\voffset}{0in}',
  '\setlength{\hoffset}{0in}',
  '\setlength{\headheight}{0ex}',
  '\setlength{\headsep}{0in}',
  '\setlength{\topskip}{0in}',
  '\setlength{\textwidth}{\paperwidth}',
  '\addtolength{\textwidth}{-2\sjfhmargin}',
  '\setlength{\evensidemargin}{-1in}',
  '\addtolength{\evensidemargin}{\sjfhmargin}',
  '\setlength{\oddsidemargin}{\evensidemargin}',
  '\setlength{\textheight}{\paperheight}',
  '\addtolength{\textheight}{-2\sjfvmargin}',
  '\addtolength{\textheight}{\headheight}',
  '\addtolength{\textheight}{\headsep}',
  '\setlength{\topmargin}{-1in}',
  '\addtolength{\topmargin}{\sjfvmargin}',
  '\usepackage{graphicx}',
  '\newfont{\titlefont}{cmssi17 at 25pt}',
  '\newfont{\smf}{cmss10 at 0.4cm}',
  '\newfont{\sbf}{cmssbx10 at 0.4cm}',
  '\pagestyle{empty}',
  '\begin{document}',
  '\vspace*{\fill}',
  '\begin{centering}',
  Inlay($InW,$InH,$ArchID,@Stuff),
  '',
  '\vspace*{0.5cm}',
  '\fbox{\begin{minipage}[c]['.$CovH.'cm][c]{'.$CovS.'cm}%',
  '\hspace*{\fill}%',
  '\rotatebox{90}{\parbox[c]['.$CovS.'cm][c]{'.$CovH.'cm}{%',
  '\centerline{'.$CovTitle.'}}}%',
  '\hspace*{\fill}%',
  '\end{minipage}}%',
  Inlay($CovW,$CovH,$ArchID,@Stuff),
  '\fbox{\begin{minipage}[c]['.$CovH.'cm][c]{'.$CovS.'cm}%',
  '\rotatebox{270}{\parbox[c]['.$CovS.'cm][c]{'.$CovH.'cm}{%',
  '\centerline{'.$CovTitle.'}}}%',
  '\end{minipage}}',
  '',
  '\end{centering}',
  '\vspace*{\fill}',
  '\end{document}%',"";

print join("\n",@Latex);

sub Inlay
  {
    my $W=shift;
    my $H=shift;
    my $ArchID=shift;
    my @Stuff=@_;

    my @Inlay;
    
    push @Inlay,
    '\fbox{\begin{minipage}[c]['.$H.'cm][c]{'.$W.'cm}',
    '\hspace*{\fill}',
    '\begin{minipage}[l]['.($H-1).'cm][t]{'.($W-1).'cm}',
    '{\titlefont Whipple 10m Archive}','',
    '\rule{'.($W-1).'cm}{3pt}','','\vspace{3ex}',
    '{\Large \textbf{Index No:} '.$ArchID.'}','\vspace{2ex}',
    '\begin{list}{}{\setlength{\leftmargin}{6.5em}',
    '\setlength{\labelwidth}{\leftmargin} \setlength{\labelsep}{0em}',
    '\setlength{\parsep}{-1ex}',
    '\renewcommand{\makelabel}[1]{\makebox[\labelwidth][l]{\bf #1:}}}',
    @Stuff,
    '\end{list}',
    '\end{minipage}',
    '\hspace*{\fill}',
    '\end{minipage}}%';

    return @Inlay;
  }

sub BunchNos($)
{
    my $RunNos=shift;
    my $return;
    my $bunching;
    my $run;
    my ($runstart,$runend)=(undef,undef);
    my $runs=undef;
    my $first=1;
    
    foreach $run ( sort { $a <=> $b } @$RunNos )
    {
	if ( not defined $runstart )
	{
	    $runstart=$runend=$run;
	}
	elsif ( $run != $runend+1 )
	{
	    $return.=", " unless defined $first;
	    undef $first;
	    
	    if ( $runstart==$runend ) { $return.=$runstart; }
	    elsif ( $runstart==$runend-1 ) { $return.=$runstart.",".$runend }
	    else { $return.=$runstart."--".$runend };
	    
	    $runstart=$runend=$run;
	}
	else
	{
	    $runend=$run;
	}
    }
    
    $return.="," unless defined $first;
    if ( $runstart==$runend ) { $return.=$runstart; }
    elsif ( $runstart==$runend-1 ) { $return.=$runstart.",".$runend }
    else { $return.=$runstart."--".$runend };

    return $return;
}
