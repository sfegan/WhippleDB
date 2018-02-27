package Whipple::HTML;

use strict;

use Exporter;
use vars qw(@ISA @EXPORT_OK %EXPORT_TAGS);

@ISA = qw(Exporter);

@EXPORT_OK = qw(qopt tagme escape_html unescape_html whipple_page
		set_base get_base get_redirect);
%EXPORT_TAGS = ( 'funcs' => [qw(qopt tagme escape_html unescape_html
				whipple_page set_base get_base get_redirect)],
	       );

#
# Some handy HTML routines
#

sub qopt
  {
    return shift().'="'.shift().'"';
  }

sub tagme
  {
    return "<".join(" ",@_).">";
  }

sub escape_html
  {
    my $html=shift;
    $html =~ s/([^\w\s])/sprintf ("&#%d;", ord ($1))/ge;
    return $html;
  }

sub unescape_html
  {
    my $ehtml=shift;
    $ehtml =~ s/\&\#(\d+);/pack "c",$1/ge;
    return $ehtml;
  }

my $Base;
sub set_base
  {
    $Base=shift;
  }

sub get_base
  {
    return $Base;
  }

sub get_redirect
  {
    return "redirect";
  }

sub whipple_page
  {
    my %Tags=@_;

    my @barlinks=(["ucla" => "/"],
		  ["sao" => "http://veritas.sao.arizona.edu/"],
		  ["observer" => "http://veritas.sao.arizona.edu/private/Observer/"],
		  ["home" => $Base ],
		  ["login" => $Base."/login" ],
		  ["search" => $Base."/search" ],
		  ["summary" => $Base."/summary" ],
		  ["coordinates" => $Base."/coordinates" ],
		  ["catalog" => $Base."/catalog" ],
		  ["logout" => $Base."/logout" ],
		  );

    my @BODY_TAGS=('BODY');
    push(@BODY_TAGS,qopt("class",$Tags{"-class"})) if (exists $Tags{"-class"});

    my $TITLE='Untitled Document';
    $TITLE=$Tags{-title} if (exists $Tags{-title});

    my @ltbits=localtime;
    my $year=$ltbits[5]+1900;

    my $redirect_base = join("/",get_base,get_redirect);

    my @HTML;
    push @HTML,
    '<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "DTD/xhtml1-strict.dtd">',
#    '<?xml-stylesheet href="style/test.css" type="text/css"?>',
    '<HTML xmlns="http://www.w3.org/1999/xhtml" lang="en" xml:lang="en">',
    '<HEAD>',
    '<TITLE>',$TITLE,'</TITLE>',
    '<LINK href="'.$redirect_base.'/style/whipple.css" rel="stylesheet" type="text/css" />',
    '<LINK rel="icon" href="'.$redirect_base.'/favicon.ico" type="image/x-icon" />',
    '<LINK rel="shortcut icon" href="'.$redirect_base.'/favicon.ico" type="image/x-icon" />';

    push @HTML,'</HEAD>';

    push @HTML,tagme(@BODY_TAGS);

    push @HTML,'<H1 class="bar">VERITAS Log Sheet Database</H1>','';

    push @HTML,$Tags{"-body"};

    push @HTML,
    '<H2 class="bar">',
    join(" | ",map({ '<A href="'.$_->[1].'">'.$_->[0].'</A>' } 
	@barlinks)),
    '</H2>','',
    '<ADDRESS>','Comments, suggestions and corrections to',
    '<A href="mailto:sfegan@astro.ucla.edu">',
    'sfegan@astro.ucla.edu</A>',
    '<BR />Copyright 2000-'.$year.', The VERITAS Collaboration','</ADDRESS>';

    push @HTML,'</BODY>','</HTML>';

    return join("\n",@HTML);
  }

1;
