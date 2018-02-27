package Whipple::ApacheHandlers::Summary;

use strict;

use Apache2::Log;
use Apache2::ServerUtil;
use Apache2::RequestRec ();
use Apache2::RequestIO ();
use Apache2::Response ();
use Apache2::SubRequest ();
use Apache2::Connection ();
use Apache2::Filter ();
use Apache2::Const qw(:common :methods HTTP_BAD_REQUEST REDIRECT);

use Whipple::Misc qw(get_args);
use Whipple::WhipDB;
use Whipple::HTML qw(:funcs);

my @criteria_fields =
  (
   {
    "id"      => 'date_l',
    "desc"    => 'First Date (yyyy-mm-dd)',
    "size"    => 10,
    "op"      => '>=',
    "field"   => 'UTC_Date',
    "blank"   => '1900-01-01',
   },
   {
    "id"      => 'date_u',
    "desc"    => "Final Date (yyyy-mm-dd)",
    "size"    => 10,
    "op"      => "<=",
    "field"   => "UTC_Date",
    "blank"   => 'tomorrow',
   },
  );

use constant SQLTRUE => '1';
use constant SQLFALSE => '0';

my $dark_runs;
my $obs_seasons;

sub load_dark_runs
  {
    my $db=shift;
    my $r=shift;

    return OK if((defined $dark_runs) and (defined $obs_seasons));

    $dark_runs = $db->Misc->GetDarkRuns()
      if(not defined $dark_runs);

    if(not defined $dark_runs)
      {
	my $err=$db->Misc->errstr;
	chomp $err;
	$r->log_error("load_dark_runs: error dark run catalog: ".$err);
	
	my $HTML=
	  join("\n",
	       '<H2 class="warn">Database Error</H2>',
	       '<P class="warn">Could not get source catalog !',
	       'Error returned was: <SAMP>'.$err.'</SAMP></P>');
	
	$r->no_cache(1);
	$r->err_headers_out->add("Content-type","text/html");
	$r->custom_response(SERVER_ERROR,
			    whipple_page(-title   => "Query Error",
					 -body    => $HTML));
	
	return SERVER_ERROR;
      }

    $obs_seasons = $db->Misc->GetObservingSeasons()
      if(not defined $obs_seasons);

    if(not defined $obs_seasons)
      {
	my $err=$db->Misc->errstr;
	chomp $err;
	$r->log_error("load_dark_runs: error observing season catalog: ".$err);
	
	my $HTML=
	  join("\n",
	       '<H2 class="warn">Database Error</H2>',
	       '<P class="warn">Could not get source catalog !',
	       'Error returned was: <SAMP>'.$err.'</SAMP></P>');
	
	$r->no_cache(1);
	$r->err_headers_out->add("Content-type","text/html");
	$r->custom_response(SERVER_ERROR,
			    whipple_page(-title   => "Query Error",
					 -body    => $HTML));
	
	return SERVER_ERROR;
      }

    return OK;
  }

sub make_query_url
  {
    my $basepath = shift;
    my $dates = shift;
    my $i = shift;

    my $result =
      $basepath."?date_l_val=".$dates->[0]."&date_u_val=".$dates->[1];
    $result .= "&dr_number_val=".$i if ( defined $i );
    return $result."&generate=Generate+Summary";
  }

sub generate_summary
  {
    my $db=shift;
    my $r=shift;
    my $uid=shift;
    my $basepath=shift;
    my @args=@_;

    my $invocation=shift @args;

    my %form_fields=get_args($r);

    my %search_components;

    my $source_catalog = $db->SourceCatalog->GetCatalog();
    if(not defined $source_catalog)
      {
	my $err=$db->SourceCatalog->errstr;
	chomp $err;
	$r->log_error("generate_summary: error getting catalog: ".$err);
	
	my $HTML=
	  join("\n",
	       '<H2 class="warn">Database Error</H2>',
	       '<P class="warn">Could not get source catalog !',
	       'Error returned was: <SAMP>'.$err.'</SAMP></P>');
	
	$r->no_cache(1);
	$r->err_headers_out->add("Content-type","text/html");
	$r->custom_response(SERVER_ERROR,
			    whipple_page(-title   => "Query Error",
					 -body    => $HTML));
	
	return SERVER_ERROR;
      }

    my $F;
    foreach $F ( @criteria_fields )
      {
	my $id=$F->{"id"};

	my $def_incnull=SQLTRUE;
	my $def_val="";
	$def_val=$F->{"blank"} if ( exists $F->{"blank"} );
	my $def_op=$F->{"op"};
	$def_op=$def_op->[0] if ( ref $def_op );
	my $allownull=0;
	$allownull=$F->{"null"} if ( exists $F->{"null"} );

	my $incnull=($allownull)?SQLFALSE:SQLTRUE;
	my $val=$def_val;
	my $op=$def_op;
	
	if((not exists $form_fields{$id."_val"}) or
	   ((ref $F->{"op"}) and (not exists $form_fields{$id."_op"})))
	  {
	    my $HTML=join("\n",
			  '<H2 class="warn">Bad form submission</H2>',
			  '<P class="warn">Fields missing in form !</P>');

	    $db->rollback;

	    $r->no_cache(1);
	    $r->err_headers_out->add("Content-type","text/html");
	    $r->custom_response(HTTP_BAD_REQUEST,
				whipple_page(-title   => "Invalid form",
					     -body    => $HTML));
	    return HTTP_BAD_REQUEST;
	  }

	$val=$form_fields{$id."_val"} if ( $form_fields{$id."_val"} );
	$val=~s/^\s*//;
	$val=~s/\s*$//;

	$op=$form_fields{$id."_op"} if (ref $F->{"op"});
	$op='~*' if ( $op eq "regex" );
	
	$incnull=$form_fields{$id."_null"} if
	  ( ($allownull) and (exists $form_fields{$id."_null"}));
	#	$incnull='f' if ( $incnull ne SQLTRUE );
	
	$search_components{$id} = [ $F->{"field"}, $op, $val ];
      }

    my $ldate = $search_components{"date_l"}->[2];
    my $udate = $search_components{"date_u"}->[2];

    my $dr_number = $form_fields{"dr_number_val"};

    my $result = $db->RunInfo->summary_dates($ldate,$udate);
    if((not($result)) and ($db->RunInfo->err))
      {
	my $err=$db->RunInfo->errstr;
	chomp $err;
	$r->log_error("generate_summary: error getting results: ".$err);
	
	my $HTML=
	  join("\n",
	       '<H2 class="warn">Database Error</H2>',
	       '<P class="warn">Could not get summary results !',
	       'Error returned was: <SAMP>'.$err.'</SAMP></P>');
	
	$r->no_cache(1);
	$r->err_headers_out->add("Content-type","text/html");
	$r->custom_response(SERVER_ERROR,
			    whipple_page(-title   => "Query Error",
					 -body    => $HTML));
	
	return SERVER_ERROR;
      }
    $db->commit;

    my %source_results;

    my @total = ( 0,0,0,0,0,0 );

    my $row;
    while(defined($row=$db->RunInfo->fetchrow_hashref))
      {
	my $source = $row->{"source_id"};
	my $sky =    $row->{"sky_q"};
	my $runs =   $row->{"runs"};
	my $time =   $row->{"time"};

	my $skyc;
	if(($sky==1)||($sky==2)||($sky==3)) { $skyc = 0; }     # A weather
	elsif(($sky==4)||($sky==5)||($sky==6)) { $skyc = 1; }  # B weather
	elsif(($sky==7)||($sky==8)||($sky==9)) { $skyc = 2; }  # C weather
	else { $skyc = 3; }
	
	$source_results{$source} = [0,0,0,0,0,0]
	  if ( not exists $source_results{$source} );

	$source_results{$source}->[$skyc] += $time;
	$source_results{$source}->[4]     += $time;
	$source_results{$source}->[5]     += $time
	  if(($skyc==0) or ($skyc==1));                        # A/B only

	$total[$skyc]                     += $time;
	$total[4]                         += $time;
	$total[5]                         += $time
	  if(($skyc==0) or ($skyc==1));                        # A/B only
      }

    my @TABLE;
    push(@TABLE,
	 '<COLGROUP span="1" width=50/>',
	 '<COLGROUP span="1" width=150/>',
	 '<COLGROUP span="4" width=50/>',
	 '<COLGROUP span="2" width=75/>',
	 );

    push(@TABLE,
	 '<THEAD>',
	 '<TR align="center">',
	 '<TH rowspan="2">Source ID</TH>',
	 '<TH rowspan="2">Catalog Name</TH>',
	 '<TH colspan="5">Weather</TH>',
	 '<TH colspan="2">TOTAL</TH>',
	 '</TR>'.
	 '<TR align="center">',
	 '<TH>A</TH><TH>B</TH><TH>C</TH><TH>?</TH><TH>A/B</TH><TH>Hours</TH><TH>%</TH>',
	 '</TR>',
	 '</THEAD>',
	 '<TBODY>'
	);

    my $trodd=1;
    my $source;
    foreach $source ( sort { $a cmp $b } keys %source_results )
      {
	my $results = $source_results{$source};
	my @Fields=("", "-", "-", "-", "-", "-", "-", "", "");

	$Fields[0] = $source;
	$Fields[1] = $source_catalog->{$source}->{"source_name"}
	  if ( exists $source_catalog->{$source} );
	$Fields[2] = sprintf("%.1f",$results->[0]/60) if ( $results->[0] );
	$Fields[3] = sprintf("%.1f",$results->[1]/60) if ( $results->[1] );
	$Fields[4] = sprintf("%.1f",$results->[2]/60) if ( $results->[2] );
	$Fields[5] = sprintf("%.1f",$results->[3]/60) if ( $results->[3] );
	$Fields[6] = sprintf("%.1f",$results->[5]/60) if ( $results->[5] );
	$Fields[7] = sprintf("%.1f",$results->[4]/60) if ( $results->[4] );
	$Fields[8] = sprintf("%.1f",$results->[4]/$total[4]*100)
	  if ( ( $results->[4] ) && ( $total[4] ) );
	
	@Fields = ( map { escape_html($_) } @Fields );
#	$Fields[3]='<A HREF="'.$logurl.'">'.$Fields[3].'</A>';
	
	push(@TABLE,
	     '<TR align="center" class="'.(($trodd)?"trodd":"treven").'">',
	     map ( { '<TD>'.$_.'</TD>' } @Fields ),
	     '</TR>');
	$trodd = not $trodd;
      }

    push(@TABLE,
	 '</TBODY>',
	 '<TFOOT>',
	 '<TR align="center">',
	 '<TH rowspan="2" colspan="2">TOTAL</TH>',
	 map ( { '<TH>'.$_.'</TH>' }
	       sprintf("%.1f",$total[0]/60),
	       sprintf("%.1f",$total[1]/60),
	       sprintf("%.1f",$total[2]/60),
	       sprintf("%.1f",$total[3]/60),
	       sprintf("%.1f",$total[5]/60),
	       sprintf("%.1f",$total[4]/60),
	       ""
	     ),
	 '</TR>');

    if($total[4])
      {
	push(@TABLE,
	     '<TR align="center">',
	     map ( { '<TH>'.$_.'</TH>' }
		   sprintf("%.0f%%",$total[0]/$total[4]*100),
		   sprintf("%.0f%%",$total[1]/$total[4]*100),
		   sprintf("%.0f%%",$total[2]/$total[4]*100),
		   sprintf("%.0f%%",$total[3]/$total[4]*100),
		   sprintf("%.0f%%",$total[5]/$total[4]*100),
		   "",""
		 ),
	     '</TR>'
	    );
      }
    else
      {
	push(@TABLE,
	     '<TR align="center">',
	     map ( { '<TH>'.$_.'</TH>' }
		   "","","","","",""
		 ),
	     '</TR>'
	    );
	
      }

    push(@TABLE,'</TFOOT>');


    my $HTML=
      join("\n",
	   '<H2>Data Summary Results</H2>',
	   '<P>Runs are grouped by <B>Source ID</B> code and by weather.',
	   'Time spent on each source under each weather condition for the',
	   'selected time period is given in hours. The total for the time',
	   'period is also listed.</P>',
	   '<P>Summary from <B>'.$ldate.'</B> to <B>'.$udate.'</B></P>',
	   '<DIV align="center">',
#	   '<TABLE border="1" frame="none" rules="groups" align="center" cellpadding=3>',
	   '<TABLE align="center" cellpadding=4 cellspacing=0>',
	   @TABLE,
	   '</TABLE>','</DIV>');

    if(defined $dr_number)
      {
	return SERVER_ERROR if(load_dark_runs($db,$r) == SERVER_ERROR);

	my $generate_path = join("/",$basepath,$invocation);

	my $prev_dr_url =
	  make_query_url($generate_path,
			 $dark_runs->[$dr_number-1],$dr_number-1);
	my $next_dr_url =
	  make_query_url($generate_path,
			 $dark_runs->[$dr_number+1],$dr_number+1);
	
	$HTML .=
	  '<P>Take me to the <A HREF="'.$prev_dr_url.'">previous</A> or '.
	    '<A HREF="'.$next_dr_url.'">next</A> dark run.</P>';
      }

    $r->no_cache(1);
    $r->content_type("text/html");
    $r->print(whipple_page(-title   => "Data Summary Results",
			   -body    => $HTML));
    return OK;
  }

sub handler
  {
    my $db=shift;
    my $r=shift;
    my $uid=shift;
    my $basepath=shift;
    my @args=@_;

    my $invocation=shift @args;

    if($args[0] eq "generate")
      {
	my $result;
	$basepath .= "/".$invocation;
	$result = generate_summary($db,$r,$uid,$basepath,@args);
	return $result;
      }

    my @FORM_HTML;
    push(@FORM_HTML,'<TABLE class="noindent">');

    my $F;
    foreach $F ( @criteria_fields )
      {
	my $id=$F->{"id"};
	my $label=$F->{"desc"};
	my $ops=$F->{"op"};
	my $vals;
	my $nulls=(exists $F->{"null"})?$F->{"null"}:0;
	$vals=$F->{"options"} if ( exists $F->{"options"} );
	
	my $htmlid=escape_html($id);
	
	my $zval="";
	my $zop="";
	
	$zval=$F->{"zeroval"} if ( exists $F->{"zeroval"} );
	$zop=$F->{"op"} if ( (exists $F->{"op"}) and (not ref $F->{"op"}) );
	$zop=$F->{"op"}->[0] if ( (exists $F->{"op"}) and (ref $F->{"op"}) );
	
	my $val=$zval;
	my $op=$zop;
	my $incnull=SQLTRUE;
	
	push(@FORM_HTML,
	     '<TR>','<TD>',
	     '<LABEL for="'.$htmlid.'">'.
	     escape_html($label).
	     '</LABEL>','</TD>','<TD>');
	
	if(ref $ops)
	  {
	    push(@FORM_HTML,'<SELECT size="1" name="'.$htmlid.'_op">');
	    foreach ( @$ops )
	      {
		my $selected="";
		$selected="selected " if ( $op eq $_ );
		my $opval=$_;
		$opval = 'regex' if ( $opval eq '~*' );
		$opval = escape_html($opval);
		
		push(@FORM_HTML,
		     '<OPTION '.$selected.'value="'.$opval.'">'.
		     $opval.
		     '</OPTION>');
	      }
	    push(@FORM_HTML,'</SELECT>');
	  }
	
	if(defined $vals)
	  {
	    push(@FORM_HTML,'<SELECT size="1" name="'.$htmlid.'_val">');
	    foreach ( @$vals )
	      {
		my $selected="";
		$selected="selected " if ( $val eq $_ );
		my $fval=escape_html($_);
		push(@FORM_HTML,
		     '<OPTION '.$selected.'value="'.$fval.'">'.
		     $fval.
		     '</OPTION>');
	      }
	    push(@FORM_HTML,'</SELECT>');
	  }
	else
	  {
	    my $size=40;
	    $size = $F->{"size"} if (exists $F->{"size"});
	    push(@FORM_HTML,
		 '<INPUT type="text" size="'.$size.'" maxlength="'.$size.'" '.
		 'name="'.$htmlid.'_val" '.
		 'value="'.$val.'" />');
	  }
	
	if($nulls)
	  {
	    my $selected=($incnull eq SQLTRUE)?"checked ":"";
	    push(@FORM_HTML,
		 '<LABEL for="'.$htmlid.'_null"> '.
		 '(Include unknown values:</LABEL>',
		 '<INPUT type="checkbox" '.$selected.
		 'name="'.$htmlid.'_null" '.
		 'value="'.SQLTRUE.'" />',
		 '<LABEL>)</LABEL>');
	  }
	
	push(@FORM_HTML,'</TD>','</TR>');
      }

    push(@FORM_HTML,
	 '<TR>','<TD>',
	 '<INPUT name="generate" type="submit" value="Generate Summary" />',
	 '</TD>','<TD>',
	 '<INPUT type="reset" value="Reset" />',
	 '</TD>','</TR>',
	 '</TABLE>');

    #
    #
    #

    return SERVER_ERROR if(load_dark_runs($db,$r) == SERVER_ERROR);

    my @ltime=localtime;
    my $UTC_DATE=
      sprintf("%04d-%02d-%02d",$ltime[5]+1900,$ltime[4]+1,$ltime[3]);

    my $generate_path = join("/",$basepath,$invocation,"generate");

    my $n = scalar(@$dark_runs);
    my $i;
    for($i=0;$i<$n;$i++)
      { last if(($UTC_DATE cmp $dark_runs->[$i]->[0])<=0); }

    my $this_dr_url = make_query_url($generate_path,$dark_runs->[$i-1],$i-1);
    my $last_dr_url = make_query_url($generate_path,$dark_runs->[$i-2],$i-2);

    $n = scalar(@$obs_seasons);
    for($i=0;$i<$n;$i++)
      { last if(($UTC_DATE cmp $obs_seasons->[$i]->[0])<=0); }

    print STDERR $UTC_DATE,"  ",$i,"\n";

    my $this_os_url = make_query_url($generate_path,$obs_seasons->[$i-1]);
    my $last_os_url = make_query_url($generate_path,$obs_seasons->[$i-2]);

    my @HTML;
    @HTML = (
	     "<H3>Data Summary</H3>",
	     '<P>You can generate an observing summary for any range of dates',
	     'by filling in the form below. If you prefer, you can select',
	     'the range from the list of seasons or dark runs',
	     '<UL>',
	     '<LI><H4 class="noindent"><A HREF="'.$this_dr_url.'">This dark run</A></H4></LI>',
	     '<LI><H4 class="noindent"><A HREF="'.$last_dr_url.'">Last dark run</A></H4></LI>',
	     '<LI><H4 class="noindent"><A HREF="'.$this_os_url.'">This season</A></H4></LI>',
	     '<LI><H4 class="noindent"><A HREF="'.$last_os_url.'">Last season</A></H4></LI>',
	     '<LI><H4 class="noindent">Specific Dates</H4>',
	     '<FORM action="'.$generate_path.
	     '" method="get">',
	     @FORM_HTML,
	     '</FORM></LI>','</UL>',
#	     '<P><A HREF="'.$this_dr_url.'">This dark run</A></P>'
	    );

    $r->content_type("text/html");
    $r->print(whipple_page(-title   => "Data Summary",
			   -body    => join("\n",@HTML)));
    return OK;
  }

1;
