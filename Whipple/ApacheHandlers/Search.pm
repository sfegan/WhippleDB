package Whipple::ApacheHandlers::Search;

use strict;

use POSIX;

use Apache2::Log;
use Apache2::ServerUtil;
use Apache2::RequestRec ();
use Apache2::RequestIO ();
use Apache2::Response ();
use Apache2::SubRequest ();
use Apache2::Connection ();
use Apache2::Filter ();
use Apache2::Const qw(:common :methods HTTP_BAD_REQUEST REDIRECT);

use Whipple::Misc qw(get_content);
use Whipple::WhipDB;
use Whipple::HTML qw(:funcs);

#
# Validate a uid/sid combination, ie see if a search exists with
# search_id=sid and uid=uid
#

use constant SQLTRUE => '1';
use constant SQLFALSE => '0';

sub validate_search
  {
    my $db=shift;
    my $r=shift;
    my $uid=shift;
    my $sid=shift;

    my $db_search=$db->Search;

    if($db_search->select_suid($sid,$uid) != 1)
      {
	if($db_search->err)
	  {
	    my $err=$db_search->errstr;
	    chomp $err;
	    $r->log_error("run_search: could not find search sid=".$sid);
	    $r->log_error("run_search: database returned ".$err);

	    my $HTML=join("\n",
			  '<H2 class="warn">Database Error</H2>',
			  '<P class="warn">Could not find search !',
			  'Error returned was: <SAMP>'.$err.'</SAMP></P>');

	    $r->no_cache(1);
	    $r->err_headers_out->add("Content-type","text/html");
	    $r->custom_response(SERVER_ERROR,
				whipple_page(-title   => "Database Error",
					     -body    => $HTML));
	    $db->commit;
	    return SERVER_ERROR;
	  }	
	$db->commit;
	
	my $HTML=join("\n",
		      '<H2 class="warn">Search not found</H2>',
		      '<P class="warn">Search ID '.$sid.' was not found.</P>');

	$r->no_cache(1);
	$r->err_headers_out->add("Content-type","text/html");
	$r->custom_response(NOT_FOUND,
			    whipple_page(-title   => "Not Found",
					 -body    => $HTML));
	return NOT_FOUND;
      }

    my $row;
    $row=$db_search->fetchrow_hashref;

    return (OK,$row);
  }

#
# List all the searches
#

sub list_searches
{
  my $db=shift;
  my $r=shift;
  my $uid=shift;
  my $searchpath=shift;
  my @args=shift;

  my %search_results_count;
  my $ncount=$db->SearchResults->count($uid);
  if(($ncount == 0)and($db->SearchResults->err))
    {
      my $err=$db->SearchResults->errstr;
      chomp $err;
      $r->log_error("list_searches: database error ".$err);
      my $HTML=join("\n",
		    '<H2 class="warn">Database Error</H2>',
		    '<P class="warn">Could not get list of searches !',
		    'Error returned was: <SAMP>'.$err.'</SAMP></P>');

      $r->no_cache(1);
      $r->err_headers_out->add("Content-type","text/html");
      $r->custom_response(SERVER_ERROR,
			  whipple_page(-title   => "Database Error",
				       -body    => $HTML));
      return SERVER_ERROR;
    }
  $db->commit;

  my $row;
  while($row=$db->SearchResults->fetchrow_hashref)
    {
      $search_results_count{$row->{"search_id"}} = $row->{"count"};
    }

  my $db_search=$db->Search;

  my $nsearch=$db_search->select_uid($uid);
  if(($nsearch == 0)and($db_search->err))
    {
      my $err=$db_search->errstr;
      chomp $err;
      $r->log_error("list_searches: database error ".$err);
      my $HTML=join("\n",
		    '<H2 class="warn">Database Error</H2>',
		    '<P class="warn">Could not get list of searches !',
		    'Error returned was: <SAMP>'.$err.'</SAMP></P>');

      $r->no_cache(1);
      $r->err_headers_out->add("Content-type","text/html");
      $r->custom_response(SERVER_ERROR,
			  whipple_page(-title   => "Database Error",
				       -body    => $HTML));
      return SERVER_ERROR;
    }
  $db->commit;


  my @HTML;

  push(@HTML,
       << 'ENDHERE',
<H2>Search</H2>
<P>Search criteria and results can be saved in the database and replayed at a
later time. Either start a
ENDHERE
       '<B><A href="'.$searchpath.'/new">new search</A></B>',
       << 'ENDHERE',
or click on one of the saved ones below (if there are any). The number
of results found when the search was last executed is listed in brackets
following the search name.</P>
<H3>Saved searches</H3>
ENDHERE
      );

  if($nsearch)
    {
      push(@HTML,
	   '<DIV align="center">',
	   '<TABLE align="center" cellpadding=4 cellspacing=0>',
	   '<TR align="center">',
	   '<TH rowspan="2">Search Name</TH>',
	   '<TH colspan="3">Results</TH>',
	   '<TH rowspan="2">Date</TH>',
	   '</TR>','<TR align="center">',
	   '<TH>Number</TH>','<TH>HTML</TH>','<TH>Export</TH>','</TR>');

      my $trodd=1;
      my $row;
      while($row=$db_search->fetchrow_hashref)
	{
	  my @Fields;

	  my @name_bits;
	  push(@name_bits,'<I>') if($row->{"save"} ne SQLTRUE);
	  push(@name_bits,
	       '<A href="'.$searchpath."/run/".$row->{"search_id"}.'">',
	       escape_html($row->{"search_name"}),'</A>');
	  push(@HTML,'</I>') if($row->{"save"} ne SQLTRUE);
	  push(@Fields,join("",@name_bits));

	  push(@Fields,'['.$search_results_count{$row->{"search_id"}}.']')
	    if(exists $search_results_count{$row->{"search_id"}});
	  push(@Fields,'[<I>none</I>]')
	    unless(exists $search_results_count{$row->{"search_id"}});

	  push(@Fields,
	       '<A href="'.$searchpath."/export/".$row->{"search_id"}.
	       '/html/limit0+100">[HTML]</A>')
	    if((exists($search_results_count{$row->{"search_id"}})) and
	       ($search_results_count{$row->{"search_id"}} > 100));
	  push(@Fields,
	       '<A href="'.$searchpath."/export/".$row->{"search_id"}.
	       '/html">[HTML]</A>')
	    if((exists($search_results_count{$row->{"search_id"}})) and
	       ($search_results_count{$row->{"search_id"}} <= 100));
	  push(@Fields,"")
	    unless(exists $search_results_count{$row->{"search_id"}});

	  push(@Fields,
	       '<A href="'.$searchpath."/export/".$row->{"search_id"}.
	       '">[Export]</A>')
	    if(exists($search_results_count{$row->{"search_id"}}));
	  push(@Fields,"")
	    unless(exists $search_results_count{$row->{"search_id"}});

	  push(@Fields,escape_html(substr($row->{"time_stamp"},0,19)));

	  push(@HTML,
	       '<TR align="left" class="'.(($trodd)?"trodd":"treven").'">',
	       map ( { '<TD>'.$_.'</TD>' } @Fields ),
	       '</TR>');
	  $trodd = not $trodd;
	}
      push(@HTML,'</TABLE></DIV>');
    }
  else
    {
      push(@HTML,'<UL><LI>No saved searches found</LI></UL>');
    }
  push(@HTML,
       '<H3><A href="'.$searchpath.'/new">New search</A></H3>');

  $r->no_cache(1);
  $r->content_type("text/html");
  $r->print(whipple_page(-title   => "Search: create or review",
			 -body    => join("\n",@HTML)));
  return OK;
}

#
# Create a new search and REDIRECT user to it
#

sub new_search
{
  my $db=shift;
  my $r=shift;
  my $uid=shift;
  my $searchpath=shift;
  my @args=@_;

  my $db_search=$db->Search;

  if($db_search->select_nextid != 1)
    {
      my $err=$db_search->errstr;
      chomp $err;
      $r->log_error("new_search: could not get next search id");
      $r->log_error("new_search: database returned ".$err);

      my $HTML=join("\n",
		    '<H2 class="warn">Database Error</H2>',
		    '<P class="warn">Could not get next search id !',
		    'Error returned was: <SAMP>'.$err.'</SAMP></P>');

      $r->no_cache(1);
      $r->err_headers_out->add("Content-type","text/html");
      $r->custom_response(SERVER_ERROR,whipple_page(-title   => "Not Found",
						    -body    => $HTML));
      return SERVER_ERROR;
    }
  $db->commit;

  my $row=$db_search->fetchrow_arrayref;
  my $sid=$row->[0];

  if(not $db_search->insert_simple($sid,$uid,
				   "Temporary: ".scalar(localtime)))
    {
      my $err=$db_search->errstr;
      chomp $err;
      $r->log_error("new_search: could not insert new search: sid=".$sid);
      $r->log_error("new_search: database returned ".$err);

      my $HTML=join("\n",
		    '<H2 class="warn">Database Error</H2>',
		    '<P class="warn">Could not create new search !',
		    'Error returned was: <SAMP>'.$err.'</SAMP></P>');

      $r->no_cache(1);
      $r->err_headers_out->add("Content-type","text/html");
      $r->custom_response(SERVER_ERROR,
			  whipple_page(-title   => "Database Error",
				       -body    => $HTML));
      return SERVER_ERROR;
    }
  $db->commit;

  my @pathcpts=grep { $_ ne "" } split "/",$r->path_info;
  my $invocation=shift @pathcpts;

  $r->no_cache(1);
  $r->headers_out->add("Location",$searchpath."/run/".$sid);
  return REDIRECT;
}

#
# Delete a search
#

sub del_search
  {
    my $db=shift;
    my $r=shift;
    my $uid=shift;
    my $searchpath=shift;
    my @args=@_;

    my $invocation=shift @args;
    my $sid=shift @args;

    if($sid)
      {
	my $db_search=$db->Search;
	
	my ($valsearch,$row)=validate_search($db,$r,$uid,$sid);
	return $valsearch if ( $valsearch != OK );
	
	if((not $db_search->delete_suid($sid,$uid)) and
	   ( $db_search->err))
	  {
	    my $err=$db_search->errstr;
	    chomp $err;
	    $r->log_error("del_search: could not delete sid=".$sid);
	    $r->log_error("del_search: database returned ".$err);

	    my $HTML=join("\n",
			  '<H2 class="warn">Database Error</H2>',
			  '<P class="warn">Could not delete search !',
			  'Error returned was: <SAMP>'.$err.'</SAMP></P>');

	    $r->no_cache(1);
	    $r->err_headers_out->add("Content-type","text/html");
	    $r->custom_response(SERVER_ERROR,
				whipple_page(-title   => "Database Error",
					     -body    => $HTML));
	    return SERVER_ERROR;
	  }
	$db->commit;
	
	$db->SearchResults->delete_sid($sid);
	$db->commit;
	
	$db->SearchTerm->delete_sid($sid);
	$db->commit;
      }

    $r->no_cache(1);
    $r->headers_out->add("Location",$searchpath);
    return REDIRECT;
  }

#
# Run the search and output the results
#

my @criteria_fields =
(
{
"id"      => "source_id",
"desc"    => "Source ID",
"op"      => [ '=', '~*', '<>', '!~*' ],
"field"   => 'Source_ID',
},
{
"id"      => "source_name",
"desc"    => "Source Name",
"op"      => [ '~*', '=', '!~*' ],
"field"   => 'Source_Name',
},
{
"id"      => "duration",
"desc"    => "Run Duration",
"size"    => 2,
"op"      => [ '>=', '=', '<=' ],
"field"   => 'Duration',
"null"    => SQLTRUE,
},
{
"id"      => 'mode',
"desc"    => "Tracking mode",
"op"      => '=',
"field"   => 'Mode',
"options" => [ "all", "on/track", "on", "on/off", "off" ],
"null"    => SQLTRUE,
"zeroval" => "all",
},
{
"id"      => 'date_l',
"desc"    => 'First Date (yyyy-mm-dd)',
"size"    => 10,
"op"      => '>=',
"field"   => 'UTC_Date',
},
{
"id"      => 'date_u',
"desc"    => "Final Date (yyyy-mm-dd)",
"size"    => 10,
"op"      => "<=",
"field"   => "UTC_Date",
},
{
"id"      => 'sky',
"desc"    => "Sky Quality",
"op"      => '=',
"field"   => 'Sky_Q',
"options" => [ "all", "a", "a/b", "b/c" ],
"null"    => SQLTRUE,
"zeroval" => "all",
},
{
"id"      => 'el',
"desc"    => "Starting elevation",
"size"    => 2,
"op"      => [ '>=', '<=' ],
"field"   => 'Starting_EL',
"null"    => SQLTRUE,
},
{
"id"      => 'offdur',
"desc"    => "Minimum OFF duration",
"size"    => 2,
"op"      => '>=',
"field"   => 'Off_Duration',
"null"    => SQLTRUE,
}
);

sub generate_criteria_form
{
  my $db=shift;
  my $r=shift;
  my $uid=shift;
  my $sid=shift;

  my $search_opers=$db->Descriptor->get("Search_Term_OP");
  my $search_field=$db->Descriptor->get("Search_Term_Field");

  my $db_term=$db->SearchTerm;

  if((not $db_term->select_sid($sid)) and ($db_term->err))
    {
      my $err=$db_term->errstr;
      chomp $err;
      $r->log_error("generate_criteria_form: database returned error".$err);

      my $HTML=join("\n",
		    '<H2 class="warn">Database Error</H2>',
		    '<P class="warn">Could not get search criteria !',
		    'Error returned was: <SAMP>'.$err.'</SAMP></P>');

      $r->no_cache(1);
      $r->err_headers_out->add("Content-type","text/html");
      $r->custom_response(SERVER_ERROR,
			  whipple_page(-title   => "Database Error",
				       -body    => $HTML));
      return SERVER_ERROR;
    }

  my $row;
  my %criteria;
  while(defined($row=$db_term->fetchrow_hashref))
    {
      my $cfieldcode=$row->{"field"};
      my $copcode=$row->{"op_code"};
      my $cvalue=$row->{"value_rep"};
      my $cnullcode=$row->{"includenulls"};
      my $cfield=$search_field->Val($cfieldcode);
      my $cop=$search_opers->Val($copcode);

      my $F;
      foreach $F ( @criteria_fields )
	{
	  my $ffield=$F->{"field"};
	  my $fops=$F->{"op"};
	  if($ffield eq $cfield)
	    {
	      if(((not ref $fops) and ($cop eq $fops)) or
		 ((ref $fops) and (grep { /$cop/ } @$fops)))
		{
		  $criteria{$F->{"id"}} = [$cvalue,$cop,$cnullcode];
		  last;
		}
	    }
	}
    }

  my @HTML;
  push(@HTML,'<TABLE>');

  my $F;
  foreach $F ( @criteria_fields )
    {
      my $id=$F->{"id"};
      my $label=$F->{"desc"};
      my $ops=$F->{"op"};
      my $vals;
      my $nulls=(exists $F->{"null"})?$F->{"null"}:SQLFALSE;
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

      if(exists $criteria{$id})
	{
	  $val=$criteria{$id}->[0];
	  $op=$criteria{$id}->[1];
	  $incnull=$criteria{$id}->[2];
	}

      push(@HTML,
	   '<TR>','<TD>',
	   '<LABEL for="'.$htmlid.'">'.
	   escape_html($label).
	   '</LABEL>','</TD>','<TD>');

      if(ref $ops)
	{
	  push(@HTML,'<SELECT size="1" name="'.$htmlid.'_op">');
	  foreach ( @$ops )
	    {
	      my $selected="";
	      $selected="selected " if ( $op eq $_ );
	      my $opval=$_;
	      $opval = 'regex' if ( $opval eq '~*' );
	      $opval = 'not regex' if ( $opval eq '!~*' );
	      $opval = escape_html($opval);
	
	      push(@HTML,
		   '<OPTION '.$selected.'value="'.$opval.'">'.
		   $opval.
		   '</OPTION>');
	    }
	  push(@HTML,'</SELECT>');
	}

      if(defined $vals)
	{
	  push(@HTML,'<SELECT size="1" name="'.$htmlid.'_val">');
	  foreach ( @$vals )
	    {
	      my $selected="";
	      $selected="selected " if ( $val eq $_ );
	      my $fval=escape_html($_);
	      push(@HTML,
		   '<OPTION '.$selected.'value="'.$fval.'">'.
		   $fval.
		   '</OPTION>');
	    }
	  push(@HTML,'</SELECT>');
	}
      else
	{
	  my $size=40;
	  $size = $F->{"size"} if (exists $F->{"size"});
	  push(@HTML,
	       '<INPUT type="text" size="'.$size.'" maxlength="'.$size.'" '.
	       'name="'.$htmlid.'_val" '.
	       'value="'.$val.'" />')
	}

      if($nulls eq SQLTRUE)
	{
	  my $selected=($incnull eq SQLTRUE)?"checked ":"";
	  push(@HTML,
	 '<LABEL for="'.$htmlid.'_null"> '.
	       '(Include unknown values:</LABEL>',
	       '<INPUT type="checkbox" '.$selected.
	       'name="'.$htmlid.'_null" '.
	       'value="'.SQLTRUE.'" />',
	       '<LABEL>)</LABEL>');
	}

      push(@HTML,'</TD>','</TR>');
    }

  push(@HTML,
       '<TR>','<TD>','</TD>','<TD>',
       '<INPUT name="search_replace" type="submit" value="Search (REPLACE)" />',
       '<INPUT name="search_update" type="submit" value="Search (MERGE)" />',
       '<INPUT type="reset" value="Reset" />',
       '</TD>','</TR>',
       '</TABLE>');

  return (OK, join("\n",@HTML));
}

sub parse_search_form
{
  my $db=shift;
  my $r=shift;
  my $uid=shift;
  my $sid=shift;

  my $search_opers=$db->Descriptor->get("Search_Term_OP");
  my $search_field=$db->Descriptor->get("Search_Term_Field");

  my $db_term=$db->SearchTerm;
  $db_term->delete_sid($sid);
  my $termid=0;

  my %form_fields=get_content($r);

  my $F;
  foreach $F ( @criteria_fields )
    {
      my $id=$F->{"id"};

      my $def_val="";
      $def_val=$F->{"zeroval"} if ( exists $F->{"zeroval"} );
      my $def_op=$F->{"op"};
      $def_op=$def_op->[0] if ( ref $def_op );
      my $allownull=SQLFALSE;
      $allownull=$F->{"null"} if ( exists $F->{"null"} );

      my $def_incnull=($allownull eq SQLTRUE)?SQLTRUE:SQLFALSE;
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

      $val=$form_fields{$id."_val"};
      $val=~s/^\s*//;
      $val=~s/\s*$//;

      $op=$form_fields{$id."_op"} if (ref $F->{"op"});
      $op='~*' if ( $op eq "regex" );
      $op='!~*' if ( $op eq "not regex" );

      my $incnull=SQLFALSE;
      $incnull=SQLTRUE if
        ( ($allownull eq SQLTRUE) and (exists $form_fields{$id."_null"}));
#      $incnull=SQLFALSE if ( $incnull ne SQLTRUE );

      my $opcode=$search_opers->ID($op);
      my $fieldcode=$search_field->ID($F->{"field"});

#      $r->warn(join(",","Insert SC: ".$sid,$termid+1,$fieldcode,
#                    $opcode,$val,$incnull));

      if((($val ne $def_val) or ($incnull ne $def_incnull)) and
	 (not $db_term->insert($sid,++$termid,$fieldcode,
			       $opcode,$val,($incnull eq SQLTRUE)?'t':'f')))
	{
	  my $err=$db_term->errstr;
	  chomp $err;
	  $r->log_error("parse_search_form: database returned error".$err);
	
	  my $HTML=join("\n",
			'<H2 class="warn">Database Error</H2>',
			'<P class="warn">Could not enter search criteria !',
			'Error returned was: <SAMP>'.$err.'</SAMP></P>');
	
	  $r->no_cache(1);
	  $r->err_headers_out->add("Content-type","text/html");
	  $r->custom_response(SERVER_ERROR,
			      whipple_page(-title   => "Database Error",
					   -body    => $HTML));
	
	  $db->rollback;
	  return SERVER_ERROR;
	}
    }

  $db->Search->update_stamp_suid($sid,$uid);
  $db->commit;

  if(exists $form_fields{"search_replace"})
    {
      $db->SearchResults->delete_sid($sid);
      if($db->SearchResults->err)
	{
	  my $err=$db_term->errstr;
	  chomp $err;
	  $r->log_error("parse_search_form: database returned error ".
			"on delete of old results: ".$err);
	
	  my $HTML=join("\n",
			'<H2 class="warn">Database Error</H2>',
			'<P class="warn">Could not delete old results !',
			'Error returned was: <SAMP>'.$err.'</SAMP></P>');
	
	  $r->no_cache(1);
	  $r->err_headers_out->add("Content-type","text/html");
	  $r->custom_response(SERVER_ERROR,
			      whipple_page(-title   => "Database Error",
					   -body    => $HTML));
	
	  $db->rollback;
	  return SERVER_ERROR;
	}
      $db->commit;
    }

  return OK,exists $form_fields{"search_replace"};
}

sub compile_and_run_search
{
  my $db=shift;
  my $r=shift;
  my $uid=shift;
  my $sid=shift;
  my $replace=shift;

  my $codes_mo=$db->Descriptor->get("Mode_Codes");
  my $search_opers=$db->Descriptor->get("Search_Term_OP");
  my $search_field=$db->Descriptor->get("Search_Term_Field");

  my $db_term=$db->SearchTerm;

  if((not $db_term->select_sid($sid)) and ($db_term->err))
    {
      my $err=$db_term->errstr;
      chomp $err;
      $r->log_error("compile_search: database returned error".$err);

      my $HTML=join("\n",
		    '<H2 class="warn">Database Error</H2>',
		    '<P class="warn">Could not get search criteria !',
		    'Error returned was: <SAMP>'.$err.'</SAMP></P>');

      $r->no_cache(1);
      $r->err_headers_out->add("Content-type","text/html");
      $r->custom_response(SERVER_ERROR,
			  whipple_page(-title   => "Database Error",
				       -body    => $HTML));
      return SERVER_ERROR;
    }

  my @SearchTerms;
  push(@SearchTerms,
       qq{NOT EXISTS (SELECT sr.Run_ID from Search_Results sr WHERE Search_ID = $sid AND sr.Run_ID = ri.Run_ID)})
    if ( not $replace );

  my $row;
  my %criteria;
  while(defined($row=$db_term->fetchrow_hashref))
    {
      my $cfieldcode=$row->{"field"};
      my $copcode=$row->{"op_code"};
      my $cvalue=$row->{"value_rep"};
      my $cnullcode=$row->{"includenulls"};

      my $cfield=$search_field->Val($cfieldcode);
      my $cop=$search_opers->Val($copcode);

      my $fzero="";
      my $fnull=SQLFALSE;
      my $fid;

      my $F;
      foreach $F ( @criteria_fields )
	{
	  my $ffield=$F->{"field"};
	  my $fops=$F->{"op"};
	  if($ffield eq $cfield)
	    {
	      if(((not ref $fops) and ($cop eq $fops)) or
		 ((ref $fops) and (grep { /$cop/ } @$fops)))
		{
		  $fzero=$F->{"zeroval"} if ( exists $F->{"zeroval"} );
		  $fnull=$F->{"null"} if ( exists $F->{"null"} );
		  $fid=$F->{"id"};
		  last;
		}
	    }
	}

      if($fid eq "sky")
	{
	  my $CLAUSE;
	  if($cvalue ne $fzero)
	    {
	      $CLAUSE="(Sky_Q <= 3)" if ($cvalue eq "a");
	      $CLAUSE="(Sky_Q <= 6)" if ($cvalue eq "a/b");
	      $CLAUSE="(Sky_Q >=4)AND(Sky_Q <= 9)"
		if ($cvalue eq "b/c");
	
	      if($cnullcode eq SQLTRUE)
		{
		  push(@SearchTerms,
		       join(" ",
			    $CLAUSE,,"OR",
			    "(Sky_Q IS NULL)"));
		}
	      else
		{
		  push(@SearchTerms,$CLAUSE);
		}
	    }
	  elsif($cnullcode ne SQLTRUE)
	    {
	      push(@SearchTerms,"Sky_Q IS NOT NULL");
	    }
	}
      elsif($fid eq "mode")
	{
	  my $CLAUSE;
	  if($cvalue ne $fzero)
	    {
	      my @modes=split("/",$cvalue);
	      $CLAUSE=join(" OR ",
			   map { "(Mode = '".$codes_mo->ID($_)."')" }
			   @modes);
	
	      if($cnullcode eq SQLTRUE)
		{
		  push(@SearchTerms,
		       join(" ",
			    $CLAUSE,"OR",
			    "(Mode IS NULL)"));
		}
	      else
		{
		  push(@SearchTerms,$CLAUSE);
		}
	    }
	  elsif($cnullcode ne SQLTRUE)
	    {
	      push(@SearchTerms,"Mode IS NOT NULL");
	    }
	}
      elsif($fid eq "offdur")
	{
	  my $CLAUSE;
	
	  if($cvalue ne $fzero)
	    {
	      $CLAUSE = "b.Duration >= ".$cvalue;
	      $CLAUSE = "(".$CLAUSE." OR b.Duration IS NULL)"
		if ( $cnullcode eq SQLTRUE );
	    }
	  elsif($cnullcode ne SQLTRUE)
	    {
	      $CLAUSE .= "b.Duration IS NOT NULL"
	    }
	
	  push(@SearchTerms,
	       'EXISTS ( SELECT a.Off_ID FROM Run_Linkage a, Run_Info b '.
	       'WHERE a.Run_ID = ri.Run_ID AND a.Off_ID = b.Run_ID AND '.
	       $CLAUSE.')')
	    if ($CLAUSE);
	}
      else
	{
	  if(($cvalue eq $fzero)and($fnull eq SQLTRUE)and
	     ($cnullcode ne SQLTRUE))
	    {
	      # no restriction on value... just must not be NULL
	      push(@SearchTerms,$cfield." IS NOT NULL");
	    }
	  elsif(($cvalue ne $fzero)and
		(($fnull ne SQLTRUE)or($cnullcode ne SQLTRUE)))
	    {
	      # only non NULL values in given range
	      $cvalue =~ s/\'/\'\'/g;
	      push(@SearchTerms,
		   join(" ",
		  $cfield,$cop,"'".$cvalue."'"));
	    }
	  elsif(($cvalue ne $fzero)and($fnull eq SQLTRUE)and
		($cnullcode eq SQLTRUE))
	    {
	      # values in given range OR ones which are NULL
	      $cvalue =~ s/\'/\'\'/g;
	      push(@SearchTerms,
		   join(" ",
			'('.$cfield,$cop,"'".$cvalue."')","OR",
			"(".$cfield," IS NULL)"));
	    }
	}
    }

  my $WHERE=join(" AND ",map { "(".$_.")" } @SearchTerms);
  $WHERE=" AND ".$WHERE if ( $WHERE );

  my $SQL_SEL=
    "SELECT ".$sid.", ri.Run_ID, 't' FROM Run_Info ri, Run_Ident rid WHERE ".
      "rid.Run_ID = ri.Run_ID".$WHERE;

  my $SQL='INSERT INTO Search_Results '.$SQL_SEL;
  $r->warn("Running query: ".$SQL);

  my $qh=$db->prepare($SQL);
  if(not $qh)
    {
      my $err=$db->errstr;
      chomp $err;
      $r->log_error("compile_search: compile error:".$err);
	$r->log_error("compile_search: query:".$SQL);

      my $HTML=
	join("\n",
	     '<H2 class="warn">Database Error</H2>',
	     '<P class="warn">Could not compile query !',
	     'Error returned was: <SAMP>'.$err.'</SAMP></P>',
	     '<P class="warn>"Query: <SAMP>'.$SQL.'</SAMP></P>');

      $r->no_cache(1);
      $r->err_headers_out->add("Content-type","text/html");
      $r->custom_response(SERVER_ERROR,
			  whipple_page(-title   => "Query Error",
				       -body    => $HTML));
      return SERVER_ERROR;
    }

  my $nfound=$qh->execute;
  if((not $nfound) and ($qh->err))
    {
      my $err=$db->errstr;
      chomp $err;
      $r->log_error("compile_search: query run error".$err);

      my $HTML=
	join("\n",
	     '<H2 class="warn">Database Error</H2>',
	     '<P class="warn">Could not run query !',
	     'Error returned was: <SAMP>'.$err.'</SAMP></P>');

      $r->no_cache(1);
      $r->err_headers_out->add("Content-type","text/html");
      $r->custom_response(SERVER_ERROR,
			  whipple_page(-title   => "Query Error",
				       -body    => $HTML));

	$db->rollback;
      return SERVER_ERROR;
    }
  $db->commit;

  $db->Search->update_haver_suid('t',$sid,$uid);
  $db->commit;

  return OK;
}

sub run_search
  {
    my $db=shift;
    my $r=shift;
    my $uid=shift;
    my $searchpath=shift;
    my @args=@_;

    my $invocation=shift @args;
    my $sid=shift @args;

    if(not $sid)
      {
	my $HTML=join("\n",
		      '<H2 class="warn">No search ID</H2>',
		      '<P class="warn">Must supply search id !</P>');
	
	$r->no_cache(1);
	$r->err_headers_out->add("Content-type","text/html");
	$r->custom_response(NOT_FOUND,
			    whipple_page(-title   => "Error",
					 -body    => $HTML));
	return NOT_FOUND;
      }

    my $db_search=$db->Search;

    my ($valsearch,$row)=validate_search($db,$r,$uid,$sid);
    return $valsearch if ( $valsearch != OK );

    my $search_name=$row->{"search_name"};
    my $search_have_criteria=$row->{"have_criteria"};
    my $search_have_results=$row->{"have_results"};
    my $search_save=$row->{"save"};
    my $search_timestamp=$row->{"timestamp"};

    if($r->method_number == M_POST)
      {
	my ($result,$replace)=parse_search_form($db,$r,$uid,$sid);
	return $result if ( $result != OK );

	$result=compile_and_run_search($db,$r,$uid,$sid,$replace);
	return $result if ( $result != OK );

	$search_have_results=SQLTRUE;
      }

    my @HTML;

    if($search_save == SQLTRUE)
      {
	push(@HTML,'<H2>Search - <I>'.escape_html($search_name).'</I></H2>');
      }
    else
      {
	push(@HTML,'<H2>Search</H2>');
      }

    push(@HTML,
	 '<P>',
	 << 'ENDHERE',
The database remembers both the criteria and results of previous searches.
Listed below are the criteria that make up this search, or a blank form for
new searches. If you have run this search before than you will also see a
summary of the results.</P>
ENDHERE
         );
#    $r->warn("Search save = ".$search_save);
    if($search_save == SQLTRUE)
      {
	push(@HTML,
	     '<P>This search is <B>permanent</B> which means that it will be',
	     'stored in the database indefinately.');
      }
    else
      {
	push(@HTML,
	     '<P>At present this search is classed as <B>temporary</B> which',
	     'means that it will be deleted if not used for more than a',
	     'couple of days.')
      }

    push(@HTML,
	 'If you would like to change this classification you can do so on',
	 'the <A href="'.join("/",$searchpath,"profile",$sid).'">',
	 'search profile</A> page. If you have finished with the search',
	 'you can <A href="'.join("/",$searchpath,"del",$sid).'">delete</A>',
	 'it and free up space in the database.','</P>',
	 << 'ENDHERE'
<P>
To run a search, enter the criteria below and press either the
"Search (REPLACE)" or "Search (MERGE)" buttons below. REPLACE
overwrites any old search results with the results of the search
(probably what you want). MERGE adds new runs which fall within
the criteria to those already present, this can be slow and may not
have the effect you expect..
</P>
ENDHERE
);

    if($search_have_results eq SQLTRUE)
      {
	my $db_searchres=$db->SearchResults;
	if(not $db_searchres->summary($sid))
	  {
	    my $err=$db_searchres->errstr;
	    chomp $err;
	    $r->log_error("run_search: error getting search results:".$err);
	
	    my $HTML=
	      join("\n",
		   '<H2 class="warn">Database Error</H2>',
		   '<P class="warn">Could not get search results !',
		   'Error returned was: <SAMP>'.$err.'</SAMP></P>');
	
	    $r->no_cache(1);
	    $r->err_headers_out->add("Content-type","text/html");
	    $r->custom_response(SERVER_ERROR,
				whipple_page(-title   => "Query Error",
					     -body    => $HTML));
	
	    return SERVER_ERROR;
	  }
	$db->commit;

	my $row=$db_searchres->fetchrow_arrayref;
	my $nfound=$row->[0];
	my $duration=$row->[1];

	if($duration > 120)
	  {
	    $duration = sprintf("%.1f hrs", $duration/60);
	  }
	else
	  {
	    $duration .= " mins";
	  };

	push(@HTML,
	     "<H3>Search Results</H3>",
	     '<P>Found <B>'.$nfound.'</B> runs matching your criteria,',
	     'a total of <B>'.$duration.'</B> of observations.');

	if($nfound > 100)
	  {
	    push(@HTML,
		 '<A href="'.
		 join("/",$searchpath,"export",$sid,"html/limit0+100").
		 '">View first 100 results</A>.');
	  }
	else
	  {
	    push(@HTML,
		 '<A href="'.join("/",$searchpath,"export",$sid,"html").
		 '">View results</A>.');
	  }

	push(@HTML,
	     'The search results can also be returned in a number',
	     'of different',
	     '<A href="'.join("/",$searchpath,"export",$sid).'">formats</A>.',
	     '</P>');
      }

    if($search_have_criteria eq SQLTRUE)
      {
	my ($ok,$form)=generate_criteria_form($db,$r,$uid,$sid);
	return $ok if ( $ok != OK );

	push(@HTML,
	     "<H3>Search Criteria</H3>",
	     '<FORM action="'.join("/",$searchpath,$invocation,$sid).
	     '" method="post" class="noindent">',
	     $form,
	     '</FORM>');
      }

    $r->no_cache(1) if($r->method_number != M_POST);

    $r->content_type("text/html");
    $r->print(whipple_page(-title   => "Search: criteria",
			   -body    => join("\n",@HTML)));
    return OK;
  }

###############################################################################
################### SHOW THE RESULTS AS A ANALYZE SCRIPT ######################
###############################################################################

sub expand_format_command
  {
    my $command=shift;
    my $runentry=shift;

    my $runno     = $runentry->{"run_no"};
    my $utc_date  = $runentry->{"utc_date"};

    my $offrunno   = $runentry->{"off_run_no"};
    my $n2runno    = $runentry->{"n2_run_no"};

    my $onrun      = sprintf("%6.6d",$runno);
    my $offrun     = sprintf("%6.6d",$offrunno);
    my $n2run      = sprintf("%6.6d",$n2runno);

    my $n2short   = substr($n2run,-4,4);

    my $year      = substr($utc_date,0,4);
    my $month     = substr($utc_date,5,2);
    my $day       = substr($utc_date,8,2);
    my $yearshort = substr($year,2,2);

    my $utshort   = $yearshort.$month.$day;

    $command =~ s/\$ONRUNNO/$runno/ge;
    $command =~ s/\$ONRUNNO/$runno/ge;
    $command =~ s/\$OFFRUNNO/$offrunno/ge;
    $command =~ s/\$N2RUNNO/$n2runno/ge;

    $command =~ s/\$ONRUN/$onrun/ge;
    $command =~ s/\$OFFRUN/$offrun/ge;
    $command =~ s/\$N2RUN/$n2run/ge;
    $command =~ s/\$N2SHORT/$n2short/ge;

    $command =~ s/\$YEARSHORT/$yearshort/ge;
    $command =~ s/\$YEAR/$year/ge;
    $command =~ s/\$MONTH/$month/ge;
    $command =~ s/\$DAY/$day/ge;
    $command =~ s/\$UTSHORT/$utshort/ge;
    $command =~ s/\$UTLONG/$utc_date/ge;

    $command =~ s/\$\{(.*?)\}/$runentry->{$1}/ge;

    return $command;
  }

my %QLformats =
  (
   "quicklook" => {
		   "n2"   => 'n2 gt$N2RUN $UTSHORT',
		   "trk"  => 'pr gt$ONRUN none $N2SHORT $UTSHORT',
		   "pair" => 'pr gt$ONRUN gt$OFFRUN $N2SHORT $UTSHORT',
		  },

   "sanalyze" => {
		   "n2"   => 'n2 gt$N2RUN $UTSHORT',
		   "trk"  => 'tr gt$ONRUN gt$N2RUN $UTSHORT',
		   "pair" => 'pr gt$ONRUN gt$OFFRUN gt$N2RUN $UTSHORT',
		  },

   "canalyze"  => {
		   "trk"  => 'tr gt$ONRUN none gt$N2RUN $UTSHORT',
		   "pair" => 'pr gt$ONRUN gt$OFFRUN gt$N2RUN $UTSHORT',
		  },

   "logsheets"  => {
		   "trk"  => 'lynx -auth=$auth -source http://veritas.astro.ucla.edu/db/redirect/log/logs_$YEAR/d$YEARSHORT$MONTH/d$UTSHORT.log_10 >> output.txt',
		   "pair" => 'lynx -auth=$auth -source http://veritas.astro.ucla.edu/db/redirect/log/logs_$YEAR/d$YEARSHORT$MONTH/d$UTSHORT.log_10 >> output.txt',
		  },

   "download"   => {
		    "n2"   => 'd$UTSHORT gt$N2RUN.fz.bz2 # N2',
		    "pair" => 'd$UTSHORT gt$ONRUN.fz.bz2 # ON'."\n".'d$UTSHORT gt$OFFRUN.fz.bz2 # OFF',
		    "trk"  => 'd$UTSHORT gt$ONRUN.fz.bz2'
		   },

   "csv"        => {
		    "pair" => 'YOU MUST SELECT "Treat off files as tracking runs"',
		    "trk"  => '${source_id},${run_no},${source_name},${utc_date},${utc_time},${mode_text},${duration},${sky_q_text},${starting_el},${off_run_no},${n2_run_no}'
		   }
  );

sub export_results_as_textlist
  {
    my $db=shift;
    my $r=shift;
    my $uid=shift;
    my $sid=shift;
    my $search_save=shift;
    my $search_name=shift;
    my $info=shift;
    my $export_sid_path=shift;
    my $invocation=shift;
    my $export_options=shift;

    my @args=@_;

    my $Format=$QLformats{$invocation};
    if(not defined $Format)
      {
	my $HTML=join("\n",
		      '<H2 class="warn">Path not found</H2>',
		      '<P class="warn">The url "'.$r->uri,
		      '" was not found on this server. Sorry.</P>');
	
	$r->no_cache(1);
	$r->err_headers_out->add("Content-type","text/html");
	$r->custom_response(NOT_FOUND,whipple_page(-title   => "Not Found",
						   -body    => $HTML));
	
	return NOT_FOUND;
      }

    my $codes_mo=$db->Descriptor->get("Mode_Codes");
    my $codes_sq=$db->Descriptor->get("Sky_Codes");

    my $include_off = $export_options->{"include_off"};
    my $include_n2  = $export_options->{"include_n2"};;
    my $off_as_trk  = $export_options->{"off_as_trk"};;

    my @RunList=
      sort({ ( $info->{$a}->{"utc_date"} eq $info->{$b}->{"utc_date"} ) ?
	     ( $info->{$a}->{"run_no"} <=> $info->{$b}->{"run_no"} ) :
	     ( $info->{$a}->{"utc_date"} cmp $info->{$b}->{"utc_date"} ) }
	   keys %{$info});

    my @ResTable;

    if(($include_n2)&&(exists($Format->{"n2"})))
      {
	my %N2Files;
	
	my $run_id;
	foreach $run_id ( @RunList )
	  {
	    my $i=$info->{$run_id};
	    my $date=$i->{'utc_date'};
	    $N2Files{$date}->{$i->{'n2_run_no'}}=$i
	      if(exists $i->{'n2_run_no'});
	  }

	my $ut;
	foreach $ut ( sort { $a cmp $b } keys %N2Files )
	  {
	    my $run;
	    foreach $run ( sort { $a <=> $b } keys %{$N2Files{$ut}} )
	      {
		my $rinfo=$N2Files{$ut}->{$run};
		my $format=expand_format_command($Format->{"n2"},$rinfo);
		push @ResTable,$format;
	      }
	  }
      }

    my %Files;
    my $run_id;
    foreach $run_id ( @RunList )
      {
	my $i=$info->{$run_id};
	my $date=$i->{'utc_date'};
	$Files{$date}->{$i->{'run_no'}}=$i
	  if(exists $i->{'run_no'});
      }

    my $ut;
    foreach $ut ( sort { $a cmp $b } keys %Files )
      {
	my $run;
	foreach $run ( sort { $a <=> $b } keys %{$Files{$ut}} )
	  {
	    next if (not exists $Files{$ut}->{$run});

	    my $rinfo=$Files{$ut}->{$run};
	    my $format=undef;

	    $rinfo->{'mode_text'} = $codes_mo->Val($rinfo->{'mode'});
	    $rinfo->{'sky_q_text'} = $codes_sq->Val($rinfo->{'sky_q'});

	    if((not $off_as_trk) && (exists $rinfo->{'off_run_no'}) &&
	       (exists $Files{$ut}->{$rinfo->{'off_run_no'}}) &&
	       (exists($Format->{"pair"})))
	      {
		$format=expand_format_command($Format->{"pair"},$rinfo);
		delete $Files{$ut}->{$rinfo->{'off_run_no'}};
	      }
	    elsif((not $off_as_trk) && (exists $rinfo->{'off_run_no'}) &&
		  ($include_off) && (exists($Format->{"pair"})))
	      {
		$format=expand_format_command($Format->{"pair"},$rinfo);
	      }
	    elsif(exists($Format->{"trk"}))
	      {
		$format=expand_format_command($Format->{"trk"},$rinfo);
	      }
	
	    $Files{$ut}->{$run}->{"__command"} = $format;
#	    push @ResTable,$format if (defined $format)
	  }
      }

    foreach $ut ( sort { $a cmp $b } keys %Files )
      {
	my $run;
	foreach $run ( sort { $a <=> $b } keys %{$Files{$ut}} )
	  {
	    push(@ResTable,$Files{$ut}->{$run}->{"__command"})
	      if (exists $Files{$ut}->{$run}->{"__command"});
	  }
      }

    my $TEXT=join("\n", @ResTable);

    $r->no_cache(1);
    $r->content_type("text/plain");
    $r->print($TEXT);
    return OK;
  }

###############################################################################
########################### SHOW THE RESULTS AS HTML ##########################
###############################################################################

sub export_results_as_html
  {
    my $db=shift;
    my $r=shift;
    my $uid=shift;
    my $sid=shift;
    my $search_save=shift;
    my $search_name=shift;
    my $info=shift;
    my $export_sid_path=shift;
    my $invocation=shift;
    my $export_options=shift;

    my @args=@_;

    my $limit=undef;
    my $offset=undef;

    foreach ( @args )
      {
	if ( /^limit(\d+)[+](\d+)$/ ) # limit
	  {
	    $offset=$1;
	    $limit=$2;
	  }
      }

    my $codes_mo=$db->Descriptor->get("Mode_Codes");
    my $codes_sq=$db->Descriptor->get("Sky_Codes");

    my @ResTable;
    push(@ResTable,
	 '<TR align="center">',
	 map ( { '<TH>'.escape_html($_).'</TH>' }
	       "ID","Run No","Source","UTC Date","UTC Time","Mode","Duration",
	       "Sky","El","Off No","N2 No","Comment"),
	 '</TR>');

    my @RunList=
      sort({ ( $info->{$a}->{"utc_date"} eq $info->{$b}->{"utc_date"} ) ?
	     ( $info->{$a}->{"run_no"} <=> $info->{$b}->{"run_no"} ) :
	     ( $info->{$a}->{"utc_date"} cmp $info->{$b}->{"utc_date"} ) }
	   keys %{$info});

    my $total_time_inmin=0;
    my $total_runs_found=0;

    my $elevation_sum=0;
    my $elevation_found=0;

    my $trodd=1;
    my $run_id;
    foreach $run_id ( @RunList )
      {
	my $i=$info->{$run_id};

	my ($y,$m,$d)=split(/-/,$i->{'utc_date'});
	my $sy=substr($y,2,2);
	my $logurl=join("/",
			get_base,
			get_redirect,
			"log",
			"logs_".$y,
			"d".$sy.$m,
			"d".$sy.$m.$d.".log_10");

	$logurl =~ s/_10$// if ( $i->{'utc_date'} lt '1995-10-19' );

	my @Fields=($i->{'source_id'},
		    $i->{'run_no'},
		    $i->{'source_name'},
		    $i->{'utc_date'},
		    $i->{'utc_time'},
		    $codes_mo->Val($i->{'mode'}),
		    $i->{'duration'},
		    $codes_sq->Val($i->{'sky_q'}),
		    $i->{'starting_el'},
		    $i->{'off_run_no'},
		    $i->{'n2_run_no'},
		    $i->{'comment'},
		   );

	@Fields = ( map { escape_html($_) } @Fields );
	
	$Fields[3]='<A HREF="'.$logurl.'">'.$Fields[3].'</A>';

	push(@ResTable,
	     '<TR align="center" class="'.(($trodd)?"trodd":"treven").'">',
	     map ( { '<TD>'.$_.'</TD>' } @Fields ),
	     '</TR>');
	$trodd = not $trodd;

	if(defined $i->{'starting_el'})
	{
	    $elevation_sum += $i->{'starting_el'};
	    $elevation_found ++;
	}

	$total_time_inmin += $i->{'duration'};
	$total_runs_found ++;
      }

    if($total_runs_found)
      {
	my $time=$total_time_inmin;

	if($time>120)
	  {
	    $time=sprintf("%.1f hrs",$time/60);
	  }
	else
	  {
	    $time=sprintf("%d mins",$time);
	  }
	
	my $elevation_avg = "";
	if($elevation_found)
	{
	    $elevation_avg=sprintf("%d",floor($elevation_sum/$elevation_found+0.5));
	}

	my @Fields=("",$total_runs_found." runs","","","","",
		    $time,"",$elevation_avg,"","","");

	push(@ResTable,
	     '<TR align="center">',
	     map ( { '<TH>'.escape_html($_).'</TH>' } @Fields ),
	     '</TR>');
      }

    my $title;
    if($search_save eq SQLTRUE)
      {
	$title =
	  '<H2>Search Results - <I>'.escape_html($search_name).'</I></H2>';
      }
    else
      {
	$title = '<H2>Search Results</H2>';
      }

    my $HTML = join("\n",$title,
		    '<DIV align="center">',
		    '<TABLE align="center" cellpadding=4 cellspacing=0>',
		    @ResTable,
		    '</TABLE>','</DIV>');

    if((defined $limit) and (defined $offset))
      {
	my $url=join("/",$export_sid_path,$invocation);

	my $db_searchres=$db->SearchResults;
	if(not $db_searchres->summary($sid))
	  {
	    my $err=$db_searchres->errstr;
	    chomp $err;
	    $r->log_error("run_search: error getting search results:".$err);
	
	    my $HTML=
	      join("\n",
		   '<H2 class="warn">Database Error</H2>',
		   '<P class="warn">Could not get search results !',
		   'Error returned was: <SAMP>'.$err.'</SAMP></P>');
	
	    $r->no_cache(1);
	    $r->err_headers_out->add("Content-type","text/html");
	    $r->custom_response(SERVER_ERROR,
				whipple_page(-title   => "Query Error",
					     -body    => $HTML));
	
	    return SERVER_ERROR;
	  }
	$db->commit;
	
	my $row=$db_searchres->fetchrow_arrayref;
	my $nfound=$row->[0];
	my $duration=$row->[1];

	my @pages;
	if($offset != 0)
	  {
	    my $prevoffset=$offset-$limit;
	    $prevoffset=0 if ( $prevoffset < 0 );
	    my $lurl=$url."/limit".$prevoffset."+".$limit;
	    push @pages,'<A HREF="'.$lurl.'">[Prev]</A>';
	  }

	my $page;
	my $npages=int(($nfound+$limit-1)/$limit);
	for($page=0;$page<$npages;$page++)
	  {
	    my $pageoffset=$page*$limit;
	    my $lurl=$url."/limit".$pageoffset."+".$limit;
	    push @pages,'<A HREF="'.$lurl.'">['.($page+1).']</A>';
	  }

	push @pages,'<A HREF="'.$url.'">[All]</A>';

	if($offset+$limit < $nfound)
	  {
	    my $nextoffset=$offset+$limit;
	    my $lurl=$url."/limit".$nextoffset."+".$limit;
	    push @pages,'<A HREF="'.$lurl.'">[Next]</A>';
	  }
	
	$HTML .= "\n<H3>Go to page:</H3>\n".'<P align="center">' . join("",@pages) . '</P>';
      }

    $r->no_cache(1);
    $r->content_type("text/html");
    $r->print(whipple_page(-title   => "Search: results",
			   -body    => $HTML));
    return OK;
  }

###############################################################################
############################### EXPORT RESULTS ################################
###############################################################################

sub merge_table
  {
    my $db=shift;
    my $r=shift;
    my $uid=shift;
    my $sid=shift;
    my $results_hash=shift;
    my $extend=shift;
    my $query=shift;

    my $limit=shift;
    my $offset=shift;

    my $db_sr=$db->SearchResults;

    my $query_result;

    if((not defined $limit) or (not defined $offset))
      {
	$query="sel_".$query."_sid";
	$query_result=$db_sr->$query($sid);
      }
    else
      {
	$query="lim_".$query."_sid";
	$query_result=$db_sr->$query($sid,int($limit),int($offset));
      }

    if((not($query_result)) and ($db_sr->err))
      {
	my $err=$db_sr->errstr;
	chomp $err;
	$r->log_error("start_results: ".$query." error getting search results:".$err);
	
	my $HTML=
	  join("\n",
	       '<H2 class="warn">Database Error</H2>',
	       '<P class="warn">Could not get search results !',
	       'Error returned was: <SAMP>'.$err.'</SAMP></P>');
	
	$r->no_cache(1);
	$r->err_headers_out->add("Content-type","text/html");
	$r->custom_response(SERVER_ERROR,
			    whipple_page(-title   => "Query Error",
					 -body    => $HTML));
	
	return SERVER_ERROR;
      }
    $db->commit;

    my $row;
    while(defined($row=$db_sr->fetchrow_hashref))
      {
	my $run_id=delete $row->{"run_id"};
	my $key;

	next if ((not exists $results_hash->{$run_id}) && (not $extend));

	foreach $key ( keys %$row )
	  {
	    $results_hash->{$run_id}->{$key} = $row->{$key};
	  }
      }

    return OK;
  }

my %export_handlers =
  (
   "html"      => \&export_results_as_html,
   "download"  => \&export_results_as_textlist,
   "quicklook" => \&export_results_as_textlist,
   "canalyze"  => \&export_results_as_textlist,
   "sanalyze"  => \&export_results_as_textlist,
   "logsheets" => \&export_results_as_textlist,
   "csv"       => \&export_results_as_textlist,
  );

sub export_results
  {
    my $db=shift;
    my $r=shift;
    my $uid=shift;
    my $searchpath=shift;
    my @args=@_;

    my $invocation=shift @args;
    my $sid=shift @args;

    my $format=shift(@args);

    my $limit=undef;
    my $offset=undef;

    if(not $sid)
      {
	my $HTML=join("\n",
		      '<H2 class="warn">No search ID</H2>',
		      '<P class="warn">Must supply search id !</P>');
	
	$r->no_cache(1);
	$r->err_headers_out->add("Content-type","text/html");
	$r->custom_response(NOT_FOUND,
			    whipple_page(-title   => "Error",
					 -body    => $HTML));
	return NOT_FOUND;
      }

    my $db_search=$db->Search;

    my ($valsearch,$row)=validate_search($db,$r,$uid,$sid);
    return $valsearch if ( $valsearch != OK );

    my $search_name=$row->{"search_name"};
    my $search_have_criteria=$row->{"have_criteria"};
    my $search_have_results=$row->{"have_results"};
    my $search_save=$row->{"save"};
    my $search_timestamp=$row->{"timestamp"};

    my %exportoptions=( "include_off" => 1,
			"include_n2"  => 1,
			"off_as_trk"  => 0 );

    if ( not $format )
      {
	if($r->method_number == M_POST)
	  {
	    my %form_fields=get_content($r);
	
	    if((not exists $form_fields{"format"}) or
	       (not exists $form_fields{"include_n2"}) or
	       (not exists $form_fields{"include_off"}) or
	       (not exists $form_fields{"off_as_trk"}))
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

	    $format=$form_fields{"format"};
	    $format=~s/^\s*//;
	    $format=~s/\s*$//;

	    $exportoptions{"include_n2"}=$form_fields{"include_n2"};
	    $exportoptions{"include_off"}=$form_fields{"include_off"};
	    $exportoptions{"off_as_trk"}=$form_fields{"off_as_trk"};
	  }
	else
	  {
	    my @HTML;
	
	    if($search_save eq SQLTRUE)
	      {
		push(@HTML,
		     '<H2>Export Results - <I>'.
		     escape_html($search_name).'</I></H2>');
	      }
	    else
	      {
		push(@HTML,"<H2>Export Results</H2>");
	      }
	
	    push(@HTML,
		 '<FORM action="'.join("/",$searchpath,$invocation,$sid).
		 '" method="post" class="noindent">',
		 '<TABLE>',
		 '<TR>','<TD>',
		 '<LABEL for="format">Format</LABEL>',
		 '</TD>','<TD>',
		 '<SELECT name="format">',
		 '<OPTION value="quicklook">Quicklook Script</OPTION>',
		 '<OPTION value="canalyze">CAnalyze Script</OPTION>',
		 '<OPTION value="download">Download Script</OPTION>',
		 '<OPTION value="sanalyze">CAnalyze++ Script</OPTION>',
		 '<OPTION value="csv">Comma Seperated Values (CSV)</OPTION>',
		 '</SELECT>',
		 '</TD>','</TR>',

		 '<TR>','<TD>',
		 '<LABEL for="include_n2">Automatically include '.
		 'N2 files</LABEL>',
		 '</TD>','<TD>',
		 '<SELECT name="include_n2">',
		 '<OPTION value="1">Yes</OPTION>',
		 '<OPTION value="0">No</OPTION>',
		 '</SELECT>',
		 '</TD>','</TR>',

		 '<TR>','<TD>',
		 '<LABEL for="include_off">Automatically include '.
		 'OFF files</LABEL>',
		 '</TD>','<TD>',
		 '<SELECT name="include_off">',
		 '<OPTION value="1">Yes</OPTION>',
		 '<OPTION value="0">No</OPTION>',
		 '</SELECT>',
		 '</TD>','</TR>',

		 '<TR>','<TD>',
		 '<LABEL for="off_as_trk">Treat off files as tracking runs'.
		 '</LABEL>',
		 '</TD>','<TD>',
		 '<SELECT name="off_as_trk">',
		 '<OPTION value="0">No</OPTION>',
		 '<OPTION value="1">Yes</OPTION>',
		 '</SELECT>',
		 '</TD>','</TR>',

		 '<TR>','<TD>','</TD>','<TD>',
		 '<INPUT name="Export" type="submit" value="Export" />',
		 '</TD>','</TR>',

		 '</TABLE>',
		 '</FORM>');

	    #$r->no_cache(1);
	    $r->content_type("text/html");
	    $r->print(whipple_page(-title   => "Export Results",
				   -body    => join("\n",@HTML)));
	    return OK;
	  }
      }

    my $handler=$export_handlers{$format};
    if(not defined $handler)
      {
	my $HTML=join("\n",
		      '<H2 class="warn">Path not found</H2>',
		      '<P class="warn">The url "'.$r->uri,
		      '" was not found on this server. Sorry.</P>');
	
	$r->no_cache(1);
	$r->err_headers_out->add("Content-type","text/html");
	$r->custom_response(NOT_FOUND,whipple_page(-title   => "Not Found",
						   -body    => $HTML));
	
	return NOT_FOUND;
      }

    foreach ( @args )
      {
	if ( /^limit(\d+)[+](\d+)$/ ) # limit
	  {
	    $offset=$1;
	    $limit=$2;
	  }

	if ( /^includeOFF=(\d)$/ )
	  {
	    $exportoptions{"include_off"}=$1;
	  }

	if ( /^includeN2=(\d)$/ )
	  {
	    $exportoptions{"include_n2"}=$1;
	  }

	if ( /^OFFasTRK=(\d)$/ )
	  {
	    $exportoptions{"off_as_trk"}=$1;
	  }
      }

    if ( $search_have_results ne SQLTRUE )
      {
	my $title;
	if($search_save eq SQLTRUE)
	  {
	    $title =
	      '<H2>Search Results - <I>'.escape_html($search_name).'</I></H2>';
	  }
	else
	  {
	    $title = '<H2>Search Results</H2>';
	  }
	
	my $HTML = join("\n",$title,
			'<P>No results yet associated with this',
			'search.</P>');
	
	$r->no_cache(1);
	$r->content_type("text/html");
	$r->print(whipple_page(-title   => "Search: results",
			       -body    => $HTML));
	return OK;
      }

    my %info;
    my $result;

    $result=merge_table($db,$r,$uid,$sid,\%info,1,"results",$limit,$offset);
    return $result if ( $result != OK );

    $result=merge_table($db,$r,$uid,$sid,\%info,0,"runident",$limit,$offset);
    return $result if ( $result != OK );

    $result=merge_table($db,$r,$uid,$sid,\%info,0,"runinfo",$limit,$offset);
    return $result if ( $result != OK );

    $result=merge_table($db,$r,$uid,$sid,\%info,0,"rlinkoff",$limit,$offset);
    return $result if ( $result != OK );

    $result=merge_table($db,$r,$uid,$sid,\%info,0,"rlinkn2",$limit,$offset);
    return $result if ( $result != OK );

    my $export_sid_path=join("/",$searchpath,$invocation,$sid);

    return &$handler($db,$r,$uid,$sid,$search_save,$search_name,
		     \%info,$export_sid_path,
		     $format,\%exportoptions,@args);
  }

###############################################################################
############################### SEARCH PROFILE ################################
###############################################################################

sub parse_profile_form
  {
    my $db=shift;
    my $r=shift;
    my $uid=shift;
    my $sid=shift;

    my $db_search=$db->Search;

    my %form_fields=get_content($r);

    if((not exists $form_fields{"searchname"}) or
#       (not exists $form_fields{"searchsave"}) or
       (not exists $form_fields{"updateprofile"}))
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

    my $name=$form_fields{"searchname"};
    $name=~s/^\s*//;
    $name=~s/\s*$//;

    my $save=$form_fields{"searchsave"};
    $save=SQLFALSE if ( not defined $save );

    if(not $db_search->update_name_save_suid($name,$save,$sid,$uid))
      {
	my $err=$db_search->errstr;
	chomp $err;
	$r->log_error("parse_profile_form: database returned error".$err);
	
	my $HTML=join("\n",
		      '<H2 class="warn">Database Error</H2>',
		      '<P class="warn">Could not enter search profile !',
		      'Error returned was: <SAMP>'.$err.'</SAMP></P>');
	
	$r->no_cache(1);
	$r->err_headers_out->add("Content-type","text/html");
	$r->custom_response(SERVER_ERROR,
			    whipple_page(-title   => "Database Error",
					 -body    => $HTML));
	
	$db->rollback;
	return SERVER_ERROR;
      }

    $db->commit;

    return OK;
  }

sub search_profile
  {
    my $db=shift;
    my $r=shift;
    my $uid=shift;
    my $searchpath=shift;
    my @args=@_;

    my $invocation=shift @args;
    my $sid=shift @args;

    if(not $sid)
      {
	my $HTML=join("\n",
		      '<H2 class="warn">No search ID</H2>',
		      '<P class="warn">Must supply search id !</P>');
	
	$r->no_cache(1);
	$r->err_headers_out->add("Content-type","text/html");
	$r->custom_response(NOT_FOUND,
			    whipple_page(-title   => "Error",
					 -body    => $HTML));
	return NOT_FOUND;
      }

    my $db_search=$db->Search;

    if($r->method_number == M_POST)
      {
	my ($valsearch,$row)=validate_search($db,$r,$uid,$sid);
	return $valsearch if ( $valsearch != OK );
	
	my ($result,$replace)=parse_profile_form($db,$r,$uid,$sid);
	return $result if ( $result != OK );

	$r->no_cache(1);
	$r->headers_out->add("Location",$searchpath."/run/".$sid);
	return REDIRECT;
      }

    my ($valsearch,$row)=validate_search($db,$r,$uid,$sid);
    return $valsearch if ( $valsearch != OK );

    my $search_name=$row->{"search_name"};
    my $search_have_criteria=$row->{"have_criteria"};
    my $search_have_results=$row->{"have_results"};
    my $search_save=$row->{"save"};
    my $search_timestamp=$row->{"timestamp"};

    my @HTML;
    push(@HTML,'<H2>Search</H2>',
	 '<P>',
	 'Searches which are not tagged to be saved (ie. those for which',
	 'the "save search" button, below, is not clicked) will be deleted',
	 'periodically. If you want to keep this search, to re-run at a later',
	 'date then make sure to click the button below.',
	 '</P>'
	);

    if(1)
      {
	push(@HTML,
	     "<H3>Search Profile</H3>",
	     '<FORM action="'.join("/",$searchpath,"profile",$sid).
	     '" method="post" class="noindent">',
	     '<TABLE>',
	     '<TR>','<TD>',
	     '<LABEL for="searchname">Search Name</LABEL>',
	     '</TD>','<TD>',
	     '<INPUT type="text" size="40" maxlength="40" name="searchname" '.
	     'value="'.escape_html($search_name).'" />',
	     '</TD>','</TR>',
	     '<TR>','<TD>',
	     '<LABEL for="searchsave">Save search:</LABEL>',
	     '</TD>','<TD>',
	     '<INPUT type="checkbox" name="searchsave" value="'.SQLTRUE.'" '.
	     (($search_save == SQLTRUE)?"checked ":"").' />',
	     '</TD>','</TR>',
	     '<TR>','<TD>','</TD>','<TD>',
	     '<INPUT name="updateprofile" type="submit" '.
	     'value="Update Profile" />',
	     '</TD>','</TR>',
	     '</TABLE>',
	     '</FORM>');
      }
	
    $r->no_cache(1);
    $r->content_type("text/html");
    $r->print(whipple_page(-title   => "Search: profile",
			   -body    => join("\n",@HTML)));
    return OK;
  }

#
#
#

my %SearchHandlers=
  (
   "new"     => \&new_search,
   "del"     => \&del_search,
   "run"     => \&run_search,
   "show"    => \&show_results,
   "export"  => \&export_results,
   "profile" => \&search_profile,
  );

sub handler
  {
    my $db=shift;
    my $r=shift;
    my $uid=shift;
    my $basepath=shift;
    my @args=@_;

    my $invocation=shift @args;

    my $action=$args[0];

    if(defined $action)
      {
	my $actionhandle=$SearchHandlers{$action};

	if(not defined $actionhandle)
 	  {
	    my $HTML=join("\n",
			  '<H2 class="warn">Path not found</H2>',
			  '<P class="warn">The url "'.$r->uri,
			  '" was not found on this server. Sorry.</P>');
	
	    $r->no_cache(1);
	    $r->err_headers_out->add("Content-type","text/html");
	    $r->custom_response(NOT_FOUND,whipple_page(-title   => "Not Found",
						       -body    => $HTML));
	
	    return NOT_FOUND;
	  }

	return &$actionhandle($db,$r,$uid,join("/",$basepath,$invocation),
			      @args) if ( defined $actionhandle );
      }

    return list_searches($db,$r,$uid,join("/",$basepath,$invocation),@args);
  }

1;
