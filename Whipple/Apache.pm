package Whipple::Apache;

use strict;

use APR;
use APR::Bucket;
use APR::Brigade;
use APR::Const qw(SUCCESS BLOCK_READ);

use Apache2::Log;
use Apache2::ServerUtil;
use Apache2::RequestRec ();
use Apache2::RequestIO ();
use Apache2::Response ();
use Apache2::SubRequest ();
use Apache2::Connection ();
use Apache2::Filter ();
use Apache2::Const qw(:common :methods
		      HTTP_MOVED_TEMPORARILY REDIRECT
		      MODE_READBYTES);

use Whipple::Misc qw(get_content);

use Whipple::ApacheHandlers::Search;
use Whipple::ApacheHandlers::Catalog;
use Whipple::ApacheHandlers::Summary;
use Whipple::ApacheHandlers::Coordinates;

use CGI::Cookie;
use Digest::MD5;

use Whipple::WhipDB;
use Whipple::HTML qw(:funcs);

#
# DataBase handle maintainence
#

my $db;

sub ConnectDB
  {
    return 1 if((defined $db) and ($db->ping));
    undef $db;
    $db=new Whipple::WhipDB(0);
    return 1 if(defined $db);
    my $r=Apache2::RequestUtil->request;
    $r->log_reason("Cannot connect to database... we're screwed!");
    $r->custom_response(SERVER_ERROR,"Cannot connect to database");
    return undef;
  }

#
# Get the login name / user id from the user (cookie) and validate them
#

use constant COOKIE           => 'login';
use constant COOKIEUSER       => 'user';
use constant COOKIEUID        => 'uid';
use constant COOKIEVALIDATION => 'valid';

sub validate_uid
  {
    my ($user,$uid)=@_;
    return Digest::MD5->md5_hex("GrumpleWaldUser".$user."hasUID".$uid);
  }

sub get_user
  {
    my $r=shift;

    my $cookieline=$r->headers_in->get('Cookie');
    return "mustlogin" if(not $cookieline);
    my $cookiejar=CGI::Cookie->parse($cookieline);

    return "mustlogin" if(not exists $cookiejar->{COOKIE()});
    my $cookie=$cookiejar->{COOKIE()};

    my ($user,$uid,$valid)=$cookie->value;
    return "mustlogin" if((not $user)or(not $uid)or(not $valid));

    my $mvalid=validate_uid($user,$uid);
    if($mvalid ne $valid)
      {
	$r->log_error("Found invalid credentials, user=".$user." uid=".$uid);
	return "mustlogin";
      }

    return ($user,$uid);
  }

sub set_user
  {
    my $r=shift;

    my $user=shift;
    my $uid=shift;

    my $valid=validate_uid($user,$uid);

    my $cookie=new CGI::Cookie( '-name'      => COOKIE,
				'-value'     => [$user, $uid, $valid],
				'-path'      => "/",
				'-expires'   => '+6M',
			      );

    $r->headers_out->add('Set-Cookie',$cookie);
  }

#
# Generate the HTML for the login page
#

sub login_page
  {
    my $LoginURI=shift;
    my $PrevURI=shift;
    my $User=shift;
    my $NewUser=shift;

    $PrevURI="" if ( not defined $PrevURI );
    $User="" if ( not defined $User );
    $NewUser="" if ( not defined $NewUser );

    $PrevURI=escape_html($PrevURI);

    my $UserInput=
      tagme('INPUT type="text" name="username" maxlength="40" size="40"',
	    qopt("value",escape_html($User)),'/');

    my $NewUserInput="";
    $NewUserInput=" checked " if ( $NewUser eq "yes" );

    my $BODY;
    $BODY=join("\n",
	       '<H2>Login</H2>',
	       '<P>',
	       <<'ENDHERE' .
To login, please enter your username in the box below, and press
the "Login" button. If you do not have a login name or wish to create
a new one then check the "Create new user" box. Logging in allows you
to save your searches and search results and retreive them at a later
date. This is only inteneded as a convenience and not as a method of
security, no password is required for any username.
ENDHERE
	       '</P>',
	       '<P>',
	       <<'ENDHERE' .
Your username will be stored by your browser as a cookie and will be
automatically used the next time you visit. This means that you should
never have to see this Login page again. If, however, you wish to
change your username, you can get this login page back at any time by
clicking on the "login" link on the bottom bar. You <B>must</B> have
cookies enabled on your browser to use this database.
ENDHERE
	       '</P>',
	       '<FORM action="'.$LoginURI.'" method="post">',
	       ' <LABEL for="username">User Name:</LABEL>',
	       ' '.$UserInput,
	       ' (<LABEL for="username">Create new user: </LABEL>',
	       ' <INPUT type="checkbox" name="createuser" value="yes" />)<BR>',
	       ' <INPUT type="hidden" name="prevuri" value="'.$PrevURI.'">',
	       ' <INPUT type="submit" value="Login">',
	       "</FORM>",
	       "");

    return $BODY;
  }

sub login_handler
  {
    my $db=shift;
    my $r=shift;
    my $u=shift;
    my $basepath=shift;
    my @args=@_;

    my $URI=$basepath;
    my $USER=undef;
    my $NEWUSER=undef;

    if($r->method_number == M_POST)
      {
    if(0)
      {
	$r->content_type("text/plain");
	$r->print($r->the_request,"\n");
	$r->headers_in->do(sub { $r->print($_[0],": ",$_[1],"\n"); return 1; });
	$r->print("\n");
	my $a=get_content($r);
	$r->print($a);
	return OK;
      }

	my %content=get_content($r);

# SJF PRINT
#foreach ( keys %content ) { Apache2::ServerUtil->server->log_error($_,": ",$content{$_}); }

	$USER=$content{"username"};
	$NEWUSER=$content{"createuser"};
	my $prevuri=$content{"prevuri"};
	$URI=$prevuri if (defined $prevuri);

	if($USER)
	  {
	    $USER =~ s/^\s+//;
	    $USER =~ s/\s+$//;
	    $USER =~ s/\s+/ /;
	    $USER = lc $USER;
	    $USER =~ s/^(.{40}).*$/$1/ if ( length $USER > 40 );
	  }

	if(($USER)and($prevuri))
	  {
	    my $uid;
	    if($NEWUSER eq 'yes')
	      {
		$uid=$db->UserProfile->GetOrCreateUID($USER);
	      }
	    else
	      {
		$uid=$db->UserProfile->GetUID($USER);
	      }

	    if(not defined $uid)
	      {
		my $HTML;
		if($db->UserProfile->err)
		   {
		     my $err=$db->UserProfile->errstr;
		     chomp $err;
		     $db->rollback;
		     $HTML=join("\n",
				'<H2 class="warn">Database Error</H2>',
				'<P class="warn">The database returned an',
				'error while finding/creating the',
				'user profile for',
				'"'.$USER.'" ! Error returned was:',
				'<SAMP>'.$err.'</SAMP></P>');
		     $r->log_error("Database Error: UserProfile: ".$err);
		   }
		else
		  {
		    $HTML=join("\n",
			       '<H2 class="warn">Unknown user</H2>',
			       '<P class="warn">User "'.$USER.'" not found',
			       'in the user profile database. </P>');
		  }
		
		$r->no_cache(1);
		$r->content_type("text/html");
		$r->print(whipple_page(-title   => "Login",
				       -body    => $HTML));
		return OK;
	      }

	    $db->commit;

	    my $HTML=join("\n",
			  '<H2>Logged In !</H2>',
			  '<P>You have been logged in as user "'.
			  escape_html($USER).'".',
			  'Continue to '.tagme("A",qopt("HREF",$URI)).$URI.
			  "</A></P>"
			  );

	    set_user($r,$USER,$uid);
	    $r->no_cache(1);
	    $r->content_type("text/html");
	    $r->print(whipple_page(-title   => "Login",
				   -body    => $HTML));
	    return OK;
	  }
      }
    else
      {
	$URI=$r->prev->uri if ( defined $r->prev );
      }

    $r->no_cache(1);
    $r->content_type("text/html");
    $r->print(whipple_page(-title   => "Login",
			   -body    => login_page($basepath."/login",
						  $URI,$USER,$NEWUSER)));
    return OK;
  }

sub logout_handler
  {
    my $db=shift;
    my $r=shift;
    my $u=shift;
    my $basepath=shift;
    my @args=@_;

    my $cookie=new CGI::Cookie( '-name'      => COOKIE,
				'-value'     => "",
				'-path'      => "/",
				'-expires'   => '-1d',
			      );

    $r->headers_out->add('Set-Cookie',$cookie);

    my $HTML=join("\n",
		  '<H2>Logged out</H2>',
		  '<P>You have been logged out. Bye.</P>'
		 );

    $r->no_cache(1);
    $r->content_type("text/html");
    $r->print(whipple_page(-title   => "Login",
			   -body    => $HTML));
    return OK;
  }


my %Handlers=(
	      "login"     => \&login_handler,
	      "logout"    => \&logout_handler,
	      "search"    => \&Whipple::ApacheHandlers::Search::handler,
	      "catalog"   => \&Whipple::ApacheHandlers::Catalog::handler,
	      "summary"   => \&Whipple::ApacheHandlers::Summary::handler,
	      "coordinates" => \&Whipple::ApacheHandlers::Coordinates::handler,
	     );


my @HandlerDescriptions=
  (
#   [
#    "login",
#    '<P>Log into the database. If this is your first time here you can choose
#a login name. Your login name is stored as a cookie by your browser and is
#presented automatically when you return. This allows the database to keep
#track of searches and search results that you make and to make them available
#to you upon your return.</P>'
#   ],
   [
    "search",
    '<P>Search the database for runs that match your criteria. Search criteria
and results are saved in the database for when you return. You can search for
runs by source id, name, duration, sky quality, starting elevation and date.
You can also request that runs have a corresponding off run of at least a
certain duration. Results can be displayed in a variety of different formats.
</P>'
   ],
   [
    "summary",
    '<P>A list of the time spent on each source under each weather condition.
The summary is generated for a given set of dates. This is useful for seeing
how long each source was observed during a dark-run. It can also be used to
characterize the weather conditions during the season.'
   ],
   [
    "coordinates",
    "<P>Display the coordinates of all sources in the source catalog. The
coordinates have been entered into the database manually from the tracking 
computer list. As such, the coordinates may not reflect the recent changes
on the tracking computer. The list can be sorted in a number of different 
ways.</P>"
   ],
   [
    "catalog",
    '<P>A list of all source id and name combinations, along with how many
runs each combination has and the total duration of the runs. This is useful
for tracking the id code of a particular source down or seeing wheather any
runs were taken on a source with the wrong id code.</P>'
   ],
   [
    "logout",
    '<P>Only logoff the database if you want to change your user name. This
deletes the cookie stored by your browser which means you will have to login
before you use the database again. You do <B>not</B> need to logoff when you
are finished using the database for today, only do so if you want.'
   ],
 );

sub intro
  {
    my $db=shift;
    my $r=shift;
    my $uid=shift;
    my $basepath=shift;
    my @args=@_;

    my $invocation=shift @args;

    my @HTML=( '<H2>Welcome!</H2>',
	       '<P>Welcome to the VERITAS Log Sheet Database.</P>',
	       '<P>The log sheet database allows you to search for data runs
using a variety of different criteria. Any searches you make are stored by
the database and available to you whenever you return. Hence you are able to
setup searches for your favourite sources and replay them at a later date,
as more runs are taken. There are also a number of summaries of the data
available.</P>',
	     );

    my $latest_run = $db->RunInfo->getLatestRun();
    push(@HTML,
	 '<P>Latest run in database is from date: <B>'.$latest_run.'</B>.',
	 'The database is not updated automatically from the log sheets',
	 'and therefore may not yet contain the latest observations.</P>')
      if(defined $latest_run);

    my $desc;
    foreach $desc (@HandlerDescriptions)
      {
	push(@HTML,
	     '<P><H3><A HREF="'.$basepath.'/'.$desc->[0].'/">',
	     ucfirst $desc->[0],'</A></H3></P>',
	     $desc->[1]);
      }

    push(@HTML,'</UL>');

    $r->no_cache(1);
    $r->content_type("text/html");
    $r->print(whipple_page(-title   => "VERITAS Log Sheet Database",
			   -body    => join("\n",@HTML)));
    return OK;
  }

sub handler
  {
    my $r=shift;

    return SERVER_ERROR if(not defined ConnectDB());
    my @uricpts=grep { $_ ne "" } split "/",$r->uri;
    my @pathcpts=grep { $_ ne "" } split "/",$r->path_info;
    my $mypath="";
    $mypath = "/".join("/",@uricpts[0..(scalar(@uricpts)-scalar(@pathcpts)-1)])
      if((scalar(@uricpts)-scalar(@pathcpts))>0);
    my $handle=$pathcpts[0];

    set_base($mypath);

    if($handle eq get_redirect)
      {
	shift @pathcpts;
	$r->method(M_GET);
	$r->internal_redirect(join("/",undef,@pathcpts));
	return OK;
      }

    my ($user,$uid)=(undef,undef);
    if((not defined $handle)or($handle ne "login"))
      {
	($user,$uid)=get_user($r);
	return SERVER_ERROR if(not defined $user);
	
	if($user eq "mustlogin")
	  {
	    $r->no_cache(1);
	    $r->method(M_GET);
	    $r->internal_redirect($mypath."/login");
	    return OK;
	  }
      }

    return intro($db,$r,$uid,$mypath,@pathcpts) if(not defined $handle);

    my $handler=undef;
    $handler=$Handlers{$handle} if ( exists $Handlers{$handle} );

    return &$handler($db,$r,$uid,$mypath,@pathcpts) if ( defined $handler );

    my $HTML=join("\n",
		  '<H2 class="warn">Path not found</H2>',
		  '<P class="warn">The url "'.$r->uri.'" was not found',
		  'on this server. Sorry.</P>');

    $r->no_cache(1);
    $r->err_headers_out->add("Content-type","text/html");
    $r->custom_response(NOT_FOUND,whipple_page(-title   => "Not Found",
					       -body    => $HTML));
    return NOT_FOUND;
  }

1;
