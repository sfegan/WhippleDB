#!/usr/bin/perl -w

use strict;

use File::stat;
use FileHandle;

use Whipple::WhipDB;
use Text::Wrap qw(wrap);
use Data::Dumper;
use Text::Tabs;

use vars qw($ARGV $EditFirst $ViewOnly $ReEdit $RecordInDB);

my $db=new Whipple::WhipDB(0);
my $codes_mo=$db->Descriptor->get("Mode_Codes");
my $codes_sq=$db->Descriptor->get("Sky_Codes");

use Term::ReadLine;
my $term = new Term::ReadLine 'LogSheet';
my $prompt = "LS> ";
my $OUT = $term->OUT || *main::STDOUT;
my $line;
while ( defined ($line = $term->readline($prompt)) ) 
  {
    chomp $line;
    my @args=split /\s+/,$line;
    process_log(@args);
  }

sub process_log
  {
    my @ARGV=@_;
    
    my $EditFirst=0;
    my $ReEdit=0;
    my $ViewOnly=0;
    my $RecordInDB=0;
    
    my @Files;

    system("clear");
    
    my $arg;
    foreach $arg ( @ARGV )
      {
	if ( $arg eq "-e" )
	  {
	    $EditFirst=1;
	  }
	elsif ( $arg eq "-re" )
	  {
	    $EditFirst=1;
	    $ReEdit=1;
	  }
	elsif ( $arg eq "-v" )
	  {
	    $ViewOnly=1;
	  }
	elsif ( $arg eq "-db" )
	  {
	    $RecordInDB=1;
	  }
	elsif ( $arg =~ /^d(\d{6})$/ )
	  {
	    my $utdate;
	    $utdate=$1;
	    $utdate += 20000000 if($utdate < 900000);
	    $utdate += 19000000 if($utdate < 1000000);
	    my ($year,$month,$date)=($utdate=~/(....)(..)(..)/);
	    my $yearshort=sprintf("%2.2d",$year%100);
	    my $path=join("",'/var/www/html/log/logs_',$year,'/d',$yearshort,$month,
			  '/d',$yearshort,$month,$date);
	    if( $utdate >= 19951019 )
	    {	$path = $path.".log_10";}
	    if( $utdate < 19951019 )
	    {	$path = $path.".log";}
	    push @Files,$path;
	  }
	else
	  {
	    push @Files,$arg;
	  }
      }
    
    
    my $file;
    foreach $file ( @Files )
      {
	my $utshort;
	if ( not $file =~ /d(\d{6})/ )
	  {
	    print STDERR ("ERR: Could not extract UT date of file ",$file,"\n",
		"ERR: Filename must contain dYYMMDD somewhere in it!\n");
	    next;
	  }
	$utshort = $1;

	my $utdate = $utshort;
	$utdate += 20000000 if($utdate < 900000);
	$utdate += 19000000 if($utdate < 1000000);
	$utdate =~ s/^(....)(..)(..)$/$1-$2-$3/;

	if ( not -e $file )
	  {
	    # File does not exist -- try to run getlog.pl
	    my $dir = "./";
	    $dir = $1 if ( $ARGV[0] =~ /(.*)\//);
	    system($dir."getlog.pl","d".$utshort);
	  }

	if ( not -e $file )
	  {
	    # File does not exist -- give up on it
	    print $utdate,": Log file does not exist: ",$file,"\n";
	    next;
	  }
	
	print STDERR $utdate,": Querying database for expected runs\n";
	
	my %RunID;
	my %ExpectedRuns;
	my $EnteredRuns=$db->RunIdent->select_date($utdate);
	if( $EnteredRuns )
	  {
	    my $rv;
	    while(defined($rv=$db->RunIdent->fetchrow_hashref))
	      {
		foreach ( keys %{$rv} )
		  {
		    $ExpectedRuns{$rv->{"run_no"}}=$rv->{"run_id"};
		    $RunID{$rv->{"run_no"}}=$rv->{"run_id"};
		  }
	      }
	  }
	elsif($db->RunIdent->err)
	  {
	    my $err=$db->RunIdent->errstr;
	    chomp $err;
	    print STDERR $utdate,": Could not retrieve list of run numbers\n";
	    $db->rollback;
	    die $err;
	  }
	$db->commit;
	
	print STDERR $utdate,": Found ",$EnteredRuns," runs in database\n";
	
	my %RunInfo;
	
	if ( $ViewOnly == 1 )
	  {
	    system("less",$file);
	  }
	
	if ( $EditFirst == 1 )
	  {
	    my $nfile="/tmp/editfile";
	    system("cp",$file,$nfile) if ( $ReEdit != 1 );
	    system("emacs","-nw",$nfile);
	    $file=$nfile;
	  }
	
	my $fh=new FileHandle $file,"r";
	if(not defined $fh)
	  {
	    print $utdate,": Cannot open file: ",$file," ",$!,"\n";
	    next;
	  }
	
	my $line;
	$line=$fh->getline;
	$line=$fh->getline 
	  while ( ( defined $line ) and ( not $line =~ /^id/i ));
	
	if(not defined $line)
	  {
	    print STDERR $utdate,": No ID line found.. skipping file!\n";
	    next;
	  }
	
	chomp($line);
	$line =~ s/\s*$//;
	$line=expand($line);
	my @splitline=map { lc } split /(\s+)/,$line;
	
	my $i;
	my $column=0;
	my @fieldsarray;
	my %fieldshash;
	
	for($i=0;$i<scalar(@splitline);$i++)
	  {
	    if(($i%2)==0)
	      {
		push @fieldsarray,$splitline[$i];
		$fieldshash{$splitline[$i]}=[$i/2, $column];
	      }
	    $column+=length $splitline[$i];
	  }
	
	print STDERR 
	  ( wrap($utdate.': Fields ',$utdate.':        ',
		 '"'.join('", "',
			  sort { $fieldshash{$a} <=> $fieldshash{$b} } 
			  keys %fieldshash).'"'),
	    "\n");
	
	
	#    print Dumper(\%fieldshash);
	#    print Dumper(\@fieldsarray);
	
	$line=$fh->getline;
	if(defined $line) {  chomp($line);  $line =~ s/\s*$//; }
	
	while(defined $line)
	  {
	    $line=expand($line);
	    
	    last if ( $line =~ /^sky/i );
	    last if ( $line =~ /^observer/i );
	    
	    if ( ( not $line ) ) #or ( not $line =~ /^\w\w\w?\s+\d+/ ) )
	      {
		$line=$fh->getline;
		if(defined $line) {  chomp($line);  $line =~ s/\s*$//; }
		next;
	      }
	    
	    @splitline=map { lc } split /(\s+)/,$line;
	    
	    $column=0;
	    my %LineStuff;
	    my $LineCount=0;
	    my @linebits;
	    
	    my $e=0;
	    my $ne=scalar @splitline;
	    my $nf=scalar(@fieldsarray);
	    while($e<$ne)
	      {
		if(($e%2)==0)
		  {
		    my $myl=$column;
		    my $myr=length($splitline[$e])+$myl-1;
		    
		    my $likelyfield=-1;
		    my $f=0;
		    while ( $f < $nf )
		      {
			my $fl=$fieldshash{$fieldsarray[$f]}->[1];
			my $fr=length($fieldsarray[$f])+$fl-1;
			if((($myl<=$fl)and($myr>=$fl))or
			   (($myl<=$fr)and($myr>=$fr))or
			   (($myl>=$fl)and($myr<=$fr)))
			  {
			    $likelyfield=$f;
			    last;
			  }
			
			if (($f==($nf-1)) or 
			($myr<$fieldshash{$fieldsarray[$f+1]}->[1]))
			  {
			    $likelyfield=$f;
			    last;
			  }
			
			$f++;
		      }
		    
		    push(@linebits,$splitline[$e]);
		    push(@{$LineStuff{$fieldsarray[$likelyfield]}},
			 $splitline[$e]);
		  }
		$column += length($splitline[$e]);
		$e++;
	      }
	    
	    while(1)
	      {
		$line=$fh->getline;
		last if ( not defined $line );
		chomp($line);
		$line =~ s/\s*$//;
		last if ( not $line =~ /^\s/ );
		$line =~ s/^\s+//;
		@splitline=map { lc } split /\s+/,$line;
		push(@linebits,@splitline);
		push(@{$LineStuff{"comments"}},@splitline);
	      }
	    
	    foreach ( keys %LineStuff )
	      {
		$LineStuff{$_}=join(" ",@{$LineStuff{$_}});
	      }
	    
	    my $Run_No=$LineStuff{"run"};
	    my $Source_ID=$LineStuff{"id"};
	    my $Source_Name=$LineStuff{"source"};
	    my $UTC_Time=$LineStuff{"utc"};
	    my $Duration=$LineStuff{"dur"};
	    my $Mode=$LineStuff{"mode"};
	    my $Sky=$LineStuff{"sky"};
	    my $Starting_El=$LineStuff{"el"};
	    my $Comments=$LineStuff{"comments"};
	    
	    my $AllLine=join(" ",@linebits);
	    
	    if((not defined $Run_No)or
	       (not defined $Source_ID)or
	       (not defined $Source_Name))
	      {
		print STDERR $utdate,": Skipping line: ",$AllLine,"\n";
		next;
	      }
	    
	    $Mode="home" if ((defined $Mode) and ($Mode eq "stow"));
	    $Mode="home" if ((defined $Mode) and ($Mode eq "park"));
	    $Mode="track" if ((defined $Mode) and ($Mode eq "trk"));
	    $Mode="track" if ((defined $Mode) and ($Mode eq "tr"));
	    $Mode="drift" if ((defined $Mode) and ($Mode eq "dft"));
	    $Mode="drift" if ((defined $Mode) and ($Mode eq "drf"));
	    $Mode="drift" if ((defined $Mode) and ($Mode eq "drft"));
	    $Mode="drift" if ((defined $Mode) and ($Mode eq "dr"));
	    $Mode="drift" if ((defined $Mode) and ($Mode eq "zn"));
	    
	    $Sky=~tr[123][abc] if ((defined $Sky) and ($Sky =~ /[123]/));
            $Sky="ng" if ((defined $Sky) and ($Sky =~ /^[dDeEfF]/));
	    
	    $UTC_Time =~ s/^(\d\d?)(\d\d)$/$1:$2/ if ( defined $UTC_Time );
	    $UTC_Time =~ s/^(\d):/0$1:/ if ( defined $UTC_Time );
	    $UTC_Time =~ s/: (\d)/:0$1/ if ( defined $UTC_Time );
	    
	    $UTC_Time =~ s/^(\d\d?).(\d\d)$/$1:$2/ if ( defined $UTC_Time );
	    $UTC_Time =~ s/^(\d\d?).(\d\d).(\d\d)$/$1:$2:$3/ 
	      if ( defined $UTC_Time );
	    $UTC_Time =~ s/^(\d\d)(\d\d)(\d\d)$/$1:$2:$3/ 
	      if ( defined $UTC_Time );
	    
	    $Starting_El =~ s/(\d+[.]\d+)/int($1+0.5)/e 
	      if ( defined $Starting_El );;
	    $Starting_El =~ s/^(\d+)-\d+$/$1/ if ( defined $Starting_El );;
	    
	    if(not $Run_No =~ /^\d+$/)
	      {
		print STDERR ($utdate,': Run Number "',$Run_No,
			      '" is not numeric on line ',$AllLine,
			      ".. skipping this entry\n");
		next;
	      }
	    
	    if(not $Source_ID =~ /^\w[\w+-]{1,2}[\!\@\#\$\%\#^\&\*]?$/)
	      {
		print STDERR ($utdate,': Unconventional Source ID "',
			      $Source_ID,
			      '" on line ',$AllLine,
			      ".. skipping this entry\n");
		next;
	      }
	    
	    if((defined $UTC_Time) and (not $UTC_Time =~ /^\d\d(:\d\d){1,2}$/))
	      {
		print STDERR ($utdate,': Unconventional UTC_Time "',$UTC_Time,
			      '" on line ',$AllLine,".. setting to null\n");
		undef $UTC_Time;
	      }
	    
	    if((defined $Duration) and (not $Duration =~ /^\d+$/))
	      {
		print STDERR ($utdate,': Weird Duration "',$Duration,
			      '" on line ',$AllLine,".. setting to null\n");
		undef $Duration;
	      }
	    
	    if((defined $Mode) and (not defined $codes_mo->ID($Mode)) and
	       ($Mode ne "on*") and ($Mode ne "off*"))
	      {
		print STDERR ($utdate,': Unknown Mode "',$Mode,
			      '" on line ',$AllLine,".. setting to null\n");
		undef $Mode;
	      }
	    
	    if((defined $Sky) and (not defined $codes_sq->ID($Sky)))
	      {
		print STDERR ($utdate,': Unknown Sky Quality "',$Sky,
			      '" on line ',$AllLine,".. setting to null\n");
		undef $Sky;
	      }
	    
	    if((defined $Starting_El) and 
	       ( (not $Starting_El =~ /^\d+$/) or
		 ( $Starting_El < 0 ) or ( $Starting_El > 90) ) )
	      {
		print STDERR ($utdate,': Weird Starting Elevation "',
			      $Starting_El,
			      '" on line ',$AllLine,".. setting to null\n");
		undef $Starting_El;
	      }
	    
	    if(exists $RunInfo{$Run_No})
	      {
		print STDERR ($utdate,': Duplicate run ',$Run_No,
			      ".. no data will be stored in DB\n");
		$RecordInDB=0;
	      }

	    if(defined $Comments)
	      {
		$Comments =~ s/^\s+//;
		$Comments =~ s/\s+$//;
		$Comments = substr($Comments,39) if ( length($Comments)>39 );
	      }

	    $RunInfo{$Run_No} = 
	      { 
	       "run_no"        => $Run_No,
	       "source_id"     => $Source_ID,
	       "source_name"   => $Source_Name,
	       "utc_time"      => $UTC_Time,
	       "duration"      => $Duration,
	       "mode"          => $Mode,
	       "sky_q"         => $Sky,
	       "starting_el"   => $Starting_El,
	       "comments"      => $Comments,
	      };
	    
	  }
	
	$fh->close;
	
	#
	# Find the Nitrogen Files
	#
	
	my %n2files;
	my %n2linkage;
	my $Run_No;
	foreach $Run_No ( sort { $a <=> $b } keys %RunInfo )
	  {
	    my $Source_ID   = $RunInfo{$Run_No}->{"source_id"};
	    if ( ( $Source_ID =~ /^n2/ ) or ( $Source_ID =~ /^ls/ ) )
	      {
		my $n2set="default";
		if ( $Source_ID =~ /^\w{2,3}([\!\@\#\$\%\^\&\*])$/ )
		  {
		    $n2set="defined".$1;
		    $RunInfo{$Run_No}->{"source_id"} =~ 
		      s/[\!\@\#\$\%\^\&\*]$//;
		  }
		print STDERR $utdate,": WARNING - MULTIPLE NITROGENS\n"
		  if (exists $n2files{$n2set});
		$n2files{$n2set}=$Run_No;
	      }
	    else
	      {
		my $n2set="default";
		if ( $Source_ID =~ /^\w{2,3}([\!\@\#\$\%\^\&\*])$/ )
		  {
		    $n2set="defined".$1;
		    $RunInfo{$Run_No}->{"source_id"} =~ 
		      s/[\!\@\#\$\%\^\&\*]$//;
		  }
		$n2linkage{$Run_No}=$n2set;
	      }
	  }
	$n2files{"default"}=undef if ( not exists $n2files{"default"} );
	$n2files{"default"}=$n2files{"defined*"} 
	  if(exists $n2files{"defined*"});
	
	#
	# Find the On/Off correspondence
	#
	
	my %source_onoff_runs;
	foreach $Run_No ( sort { $a <=> $b } keys %RunInfo )
	  {
	    my $Source_ID   = $RunInfo{$Run_No}->{"source_id"};
	    my $Mode        = $RunInfo{$Run_No}->{"mode"};
	    
	    next if ( $Source_ID =~ /^n2/ );
	    
	    if ( ( defined $Mode ) and 
		 ( ( $Mode eq "on*" ) or ( $Mode eq "off*" ) ) )
	      {
		$RunInfo{$Run_No}->{"mode"} =~ s/[*]$//;
		next;
	      }
	    
	    next if ( ( not defined $Mode ) or 
		      ( ( $Mode ne "on" ) and ( $Mode ne "off" ) ) );
	    
	    push @{$source_onoff_runs{$Source_ID}},$Run_No;
	  }
	
	my %offlinkage;
	foreach ( keys %source_onoff_runs )
	  {
	    my $Source_ID = $_;
	    my $counter=0;
	    my $nruns=scalar @{$source_onoff_runs{$Source_ID}};
	    while ( $counter < $nruns )
	      {
		my $My_Run_No = $source_onoff_runs{$Source_ID}->[$counter];
		my $My_Source_Name = $RunInfo{$My_Run_No}->{"source_name"};
		my $My_UTC_Time    = $RunInfo{$My_Run_No}->{"utc_time"};
		my $My_Duration    = $RunInfo{$My_Run_No}->{"duration"};
		my $My_Mode        = $RunInfo{$My_Run_No}->{"mode"};
		
		if($counter == ($nruns-1))
		  {
		    print STDERR ($utdate,": WARNING ",$My_Run_No,' is an "',
				  $My_Mode,'" ',
				 "run but there are no more runs following\n");
		    $counter++;
		    next;
		  }
		
		my $Next_Run_No = $source_onoff_runs{$Source_ID}->[$counter+1];
		
		if($Next_Run_No != ($My_Run_No+1))
		  {
		    print STDERR ($utdate,": WARNING ",$My_Run_No,' is an "',
				  $My_Mode,'" followed by run ',$Next_Run_No,
				  "\n");
		  }
		
		my $Next_Source_Name = $RunInfo{$Next_Run_No}->{"source_name"};
		my $Next_UTC_Time    = $RunInfo{$Next_Run_No}->{"utc_time"};
		my $Next_Duration    = $RunInfo{$Next_Run_No}->{"duration"};
		my $Next_Mode        = $RunInfo{$Next_Run_No}->{"mode"};
		
		if ( $My_Mode eq $Next_Mode ) # ie ON follwed by ON etc
		  {
		    print STDERR ($utdate,": WARNING ",$My_Run_No,' is an "',
				  $My_Mode,'" run but is not followed by an "',
				  (($My_Mode eq "on")?"off":"on"),
				  '" run',"\n");
		    $counter++;
		    next;
		  }
		
		if($My_Source_Name ne $Next_Source_Name)
		  {
		    print STDERR ($utdate,": WARNING ",$My_Run_No,' is an "',
				  $My_Mode,'" run on "',$My_Source_Name,
				  '" matched with a run on "',
				  $Next_Source_Name,"\n");
		  }
		
		if($My_Mode eq "on")
		  {
		    $offlinkage{$My_Run_No}=$Next_Run_No;
		  }
		else
		  {
		    $offlinkage{$Next_Run_No}=$My_Run_No;
		  }
		
		$counter+=2;
	      }
	  }
	
	print STDERR "\n";
	
	print STDERR ("----------------------------- ",$utdate,
		      " -----------------------------","\n");
	printf STDERR 
      ("%-3.3s %-6s %-12.12s %-8.8s %3s %-7s %-2.2s %-2.2s %5s %5s (%5s)\n",
       "ID","Run No","Source Name","UTC Time","Dur","Mode","SQ","El","N2 ID",
       "OffID","DB ID");
	printf STDERR 
      ("%-3.3s %-6s %-12.12s %-8.8s %3s %-7s %-2.2s %-2.2s %5s %5s -%5s-\n",
       "---","------","------------","--------","---","-------","--","--",
       "-----","-----","-----");
	foreach $Run_No ( sort { $a <=> $b } keys %RunInfo )
	  {
	    my $Source_ID   = $RunInfo{$Run_No}->{"source_id"};
	    my $Source_Name = $RunInfo{$Run_No}->{"source_name"};
	    my $UTC_Time    = $RunInfo{$Run_No}->{"utc_time"};
	    my $Duration    = $RunInfo{$Run_No}->{"duration"};
	    my $Mode        = $RunInfo{$Run_No}->{"mode"};
	    my $Sky         = $RunInfo{$Run_No}->{"sky_q"};
	    my $Starting_El = $RunInfo{$Run_No}->{"starting_el"};
	    my $Comments    = $RunInfo{$Run_No}->{"comments"};
	    
	    printf STDERR 
	("%-3.3s %-6d %-12.12s %-8.8s  %2s %-7s %-2.2s %-2.2s %5s %5s (%5s)\n",
	 $Source_ID,$Run_No,$Source_Name,
	 (defined($UTC_Time)?$UTC_Time:"??:??:??"),
	 (defined($Duration)?$Duration:"??"),
	 (defined($Mode)?$Mode:"?????"),(defined($Sky)?$Sky:"??"),
	 (defined($Starting_El)?$Starting_El:"??"),
	 (((defined($n2linkage{$Run_No}))and
	   (defined($n2files{$n2linkage{$Run_No}})))?
	  $n2files{$n2linkage{$Run_No}}:"*****"),
	 (exists($offlinkage{$Run_No})?$offlinkage{$Run_No}:"*****"),
	 (exists($ExpectedRuns{$Run_No})?$ExpectedRuns{$Run_No}:"*****"),
	);
	  }
	
	foreach $Run_No ( sort { $a <=> $b } keys %ExpectedRuns )
	  {
	    next if ( exists $RunInfo{$Run_No} );
	    print STDERR ($utdate,": Run ",$Run_No,
			  " is in DB (ID=",$ExpectedRuns{$Run_No},
			  ") but wasn't found in log sheet\n");
	  }
	
	if($RecordInDB==1)
	  {
	    system('cp',$file,"/home/sfegan/Projects/WhipDB/sheets/".$utdate);
	    
	    foreach $Run_No ( sort { $a <=> $b } keys %RunInfo )
	      {
		my $Source_ID   = $RunInfo{$Run_No}->{"source_id"};
		my $Source_Name = $RunInfo{$Run_No}->{"source_name"};
		my $UTC_Time    = $RunInfo{$Run_No}->{"utc_time"};
		my $Duration    = $RunInfo{$Run_No}->{"duration"};
		my $Mode        = $RunInfo{$Run_No}->{"mode"};
		my $Sky         = $RunInfo{$Run_No}->{"sky_q"};
		my $Starting_El = $RunInfo{$Run_No}->{"starting_el"};
		my $Comments    = $RunInfo{$Run_No}->{"comments"};
		
		my $Run_ID;
		
		if(exists $RunID{$Run_No+0})
		  {
		    $Run_ID = $RunID{$Run_No+0};
		  }
		else
		  {
		    $Run_ID = $db->RunIdent->GetOrCreateID($Run_No+0,$utdate);
		    if(not $Run_ID)
		      {
			my $err=$db->RunIdent->errstr; chomp $err;
			print STDERR ($utdate,
				": Error entering data into DB .. skipping\n");
			print($utdate,": ",$err,"\n");
			$db->rollback;
			next;
		      }
		    $RunID{$Run_No+0}=$Run_ID;
		  }
		
		$Mode=$codes_mo->ID($Mode) if ( defined $Mode ); 
		$Sky=$codes_sq->ID($Sky) if ( defined $Sky );
		
		if( not $db->RunInfo->insert($Run_ID, $Source_ID, $Source_Name,
					     $UTC_Time, $Duration, $Mode, $Sky,
					     $Starting_El, $Comments))
		  {
		    my $err=$db->RunInfo->errstr; chomp $err;
		    print($utdate,
			  ": Error entering data into DB .. skipping\n");
		    print($utdate,": ",$err,"\n");
		    $db->rollback;
		    next;
		  }
		$db->commit;
	      }
	    
	    foreach $Run_No ( sort { $a <=> $b } keys %RunInfo )
	      {
		my $Run_ID=$RunID{$Run_No};
		my $OFF_ID=undef;
		my $N2_ID=undef;
		
		if ( (defined $n2linkage{$Run_No}) and
		     (defined $n2files{$n2linkage{$Run_No}}) )
		  {
		    my $N2_No=$n2files{$n2linkage{$Run_No}};
		    
		    if(exists $RunID{$N2_No})
		      {
			$N2_ID = $RunID{$n2files{$n2linkage{$Run_No}}};
		      }
		    else
		      {
			$N2_ID = $db->RunIdent->GetOrCreateID($N2_No+0,
							      $utdate);
			if(not $N2_ID)
			  {
			    my $err=$db->RunIdent->errstr; chomp $err;
			    print STDERR ($utdate,
			": Could not find db ID for N2 run.. skipping\n");
			    print($utdate,": ",$err,"\n");
			    $db->rollback;
			    next;
			  }
			$RunID{$N2_No}=$N2_ID;
		      }
		  }		
		
		if(defined $offlinkage{$Run_No})
		  {
		    my $OFF_No=$offlinkage{$Run_No};
		    
		    if(exists $RunID{$OFF_No})
		      {
			$OFF_ID = $RunID{$OFF_No};
		      }
		    else
		      {
			$OFF_ID = $db->RunIdent->GetOrCreateID($OFF_No+0,
							       $utdate);
			if(not $OFF_ID)
			  {
			    my $err=$db->RunIdent->errstr; chomp $err;
			    print STDERR ($utdate,
			": Could not find db ID for OFF run.. skipping\n");
			    print($utdate,": ",$err,"\n");
			    $db->rollback;
			    next;
			  }
			$RunID{$OFF_No}=$OFF_ID;
		      }
		  }		
		
		if( not $db->RunLinkage->insert($Run_ID, $OFF_ID, $N2_ID) )
		  {
		    my $err=$db->RunILinkage->errstr; chomp $err;
		    print($utdate,
			  ": Error entering data into RunLinkage DB ",
			  "... skipping\n");
		    print($utdate,": ",$err,"\n");
		    $db->rollback;
		    next;
		  }
		$db->commit;
	      }
	  }
      }
  }
