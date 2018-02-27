package Whipple::ApacheHandlers::Coordinates;

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

use Whipple::Misc qw(get_args);
use Whipple::WhipDB;
use Whipple::HTML qw(:funcs);

sub hms
  {
    my $hms=shift;
    my $h=int(abs($hms)/10000);
    my $m=int(fmod(abs($hms/100),100));
    my $s=int(fmod(abs($hms),100));
    my $f=int(fmod(abs($hms)*10,10));
    return sprintf("%02dh%02dm%02d.%01ds",$h,$m,$s,$f);
  }

sub dms
  {
    my $dms=shift;
    my $p=$dms>0?'+':'-';
    my $d=int(abs($dms)/10000);
    my $m=int(fmod(abs($dms)/100,100));
    my $s=fmod(abs($dms),100);
    return sprintf("%s%02dd%02dm%02ds",$p,$d,$m,$s);
  }

my $sort_field;
my $sort_order;

sub mysort($$)
  {
    my $a = shift;
    my $b = shift;
    my $A = $a->{$sort_field};
    my $B = $b->{$sort_field};
    my $is_num =
      ( $A =~ /^([+-]?)(?=\d|\.\d)\d*(\.\d*)?([Ee]([+-]?\d+))?$/ ) &&
	( $B =~ /^([+-]?)(?=\d|\.\d)\d*(\.\d*)?([Ee]([+-]?\d+))?$/ );

    if($is_num)
      {
	return $A <=> $B if ( $sort_order eq "asc" );
	return $B <=> $A;
      }

    return $A cmp $B if ( $sort_order eq "asc" );
    return $B cmp $A;
  }

sub handler
  {
    my $db=shift;
    my $r=shift;
    my $uid=shift;
    my $basepath=shift;
    my @args=@_;

    my $invocation=shift @args;

    my %form_fields=get_args($r);

    my $result = $db->SourceCatalog->select_all;
    if((not($result)) and ($db->SourceCatalog->err))
      {
	my $err=$db->SourceCatalog->errstr;
	chomp $err;
	$r->log_error("coordianates handler: error getting results: ".$err);
	
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

    my @rows;
    my $row;
    push @rows,$row
      while(defined($row=$db->SourceCatalog->fetchrow_hashref));

    if(not @rows)
      {
	my $HTML=
	  join("\n",
	       '<H2 class="warn">No entries found</H2>',
	       '<P class="warn">No entries found in the source catalog.</P>');
	
	$r->no_cache(1);
	$r->err_headers_out->add("Content-type","text/html");
	$r->custom_response(SERVER_ERROR,
			    whipple_page(-title   => "No Results",
					 -body    => $HTML));

	return OK;
      }

    $sort_field = "source_id";
    $sort_order = "asc";

    $sort_field = $form_fields{"sort_field"}
      if ( ( exists $form_fields{"sort_field"} ) and
	   ( exists $rows[0]->{$form_fields{"sort_field"}} ));
    $sort_order = "dec"
      if ( ( exists $form_fields{"sort_order"} ) and
	   ( $form_fields{"sort_order"} eq "dec" ) );

    my @TABLE;
    push(@TABLE,
	 '<COLGROUP span="1"/>',
	 '<COLGROUP span="1"/>',
	 '<COLGROUP span="3"/>',
	);

    my $base = $basepath.'/'.$invocation;
    my @sortopt =
      ( "source_id", "source_name", "right_ascention", "declination" );
    my %sorturl;
    foreach my $opt ( @sortopt )
      {
	my $order = "asc";
	$order = "dec" if ( ( $sort_field eq $opt ) &&
			    ( $sort_order eq "asc" ) );
	$sorturl{$opt} = $base."?sort_field=".$opt."&sort_order=".$order;
      }

    push(@TABLE,
	 '<THEAD>',
	 '<TR align="center">',
	 '<TH rowspan="2"><A href="'.$sorturl{"source_id"}.'">Source ID</A></TH>',
	 '<TH rowspan="2"><A href="'.$sorturl{"source_name"}.'">Catalog Name</A></TH>',
	 '<TH colspan="3">Coordinates</TH>',
	 '</TR>'.
	 '<TR align="center">',
	 '<TH><A href="'.$sorturl{"right_ascention"}.'">Right Ascention</A></TH>',
	 '<TH><A href="'.$sorturl{"declination"}.'">Declination</A></TH>',
	 '<TH>Epoch</TH>',
	 '</TR>',
	 '</THEAD>',
	 '<TBODY>'
	);

    my $trodd=1;
    foreach my $entry ( sort mysort @rows )
      {
	my @Fields=("", "", "-", "-", "-");

	$Fields[0] = $entry->{"source_id"};
	$Fields[1] = $entry->{"source_name"};
	$Fields[2] = hms($entry->{"right_ascention"})
	  if ( defined $entry->{"right_ascention"} );
	$Fields[3] = dms($entry->{"declination"})
	  if ( defined $entry->{"declination"} );
	$Fields[4] = $entry->{"epoch"}
	  if ( defined $entry->{"epoch"} );

	push(@TABLE,
	     '<TR align="center" class="'.(($trodd)?"trodd":"treven").'">',
	     map ( { '<TD>'.$_.'</TD>' } @Fields ),
	     '</TR>');
	$trodd = not $trodd;
      }

    push(@TABLE,
	 '</TBODY>');


    my $HTML=
      join("\n",
	   '<H2>Source Coordinates Table</H2>',
	   '<DIV align="center">',
#	   '<TABLE border="1" frame="none" rules="groups" align="center" cellpadding=3>',
	   '<TABLE align="center" cellpadding=4 cellspacing=0>',
	   @TABLE,
	   '</TABLE>','</DIV>');

    $r->no_cache(1);
    $r->content_type("text/html");
    $r->print(whipple_page(-title   => "Source Coordinates Table",
			   -body    => $HTML));

    return OK;
  }

1;
