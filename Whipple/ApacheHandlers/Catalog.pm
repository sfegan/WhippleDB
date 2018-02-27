package Whipple::ApacheHandlers::Catalog;

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

use Whipple::Misc qw(get_content);
use Whipple::WhipDB;
use Whipple::HTML qw(:funcs);

sub handler
  {
    my $db=shift;
    my $r=shift;
    my $uid=shift;
    my $basepath=shift;
    my @args=@_;

    my $invocation=shift @args;

    my $db_rinfo=$db->RunInfo;
    
    if((not($db_rinfo->source_catalog())) and ($db_rinfo->err))
      {
	my $err=$db_rinfo->errstr;
	chomp $err;
	$r->log_error("Catalog::handler: error getting source catalog:".$err);
	
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
    $db->commit;
    
    my @results;

    my $row;
    while(defined($row=$db_rinfo->fetchrow_hashref))
      {
	push @results,$row;
      }

    my $COLUMNS=1;

    my $nfields=scalar @results;
    my $span=int(($nfields+$COLUMNS-1)/$COLUMNS);
    
    my @HTML;

    my $column;
    push(@HTML,'<COLGROUP>');
    for($column=0;$column<$COLUMNS;$column++)
      {
	push(@HTML,'<COL width="100" />') if ($column != 0);
	push(@HTML,'<COL span="4">');
      }
    push(@HTML,'</COLGROUP>');

    push(@HTML,'<TR align="center">');
    for($column=0;$column<$COLUMNS;$column++)
      {
	push(@HTML,'<TH></TH>') if ($column != 0);
	push(@HTML,
	     '<TH>Source ID</TH>',
	     '<TH>Source Name</TH>',
	     '<TH>Runs</TH>',
	     '<TH>Time (hr)</TH>');
      }
    push(@HTML,'</TR>');
    
    my $trodd=1;
    my $row;
    for($row=0;$row<$span;$row++)
      {
	push(@HTML,'<TR align="center" class="'.(($trodd)?"trodd":"treven").'">');
	for($column=0;$column<$COLUMNS;$column++)
	  {
	    my $el=($row*$COLUMNS)+$column;
	    next if($el >= $nfields);
	    push(@HTML,'<TD></TD>') if ($column != 0);
	    push(@HTML,
		 '<TD>',escape_html($results[$el]->{"source_id"}),'</TD>',
		 '<TD>',escape_html($results[$el]->{"source_name"}),'</TD>',
		 '<TD>',escape_html($results[$el]->{"count"}),'</TD>',
		 '<TD>',escape_html(sprintf("%.2f",$results[$el]->{"duration"}/60)),'</TD>');
	  }
	push(@HTML,'</TR>');
	$trodd = not $trodd;
      }

    my $HTML=
      join("\n",
	   '<H2>Source Catalog</H2>',
	   '<DIV align="center">',
	   '<TABLE align="center" cellpadding=4 cellspacing=0>',
	   @HTML,
	   '</TABLE>',
	   '</DIV>',
	   '</FORM>');
    
    $r->no_cache(1);
    $r->content_type("text/html");
    $r->print(whipple_page(-title   => "Source catalog",
			   -body    => $HTML));
    return OK;
  }

1;
