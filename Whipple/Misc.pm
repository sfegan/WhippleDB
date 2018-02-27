package Whipple::Misc;

use strict;

use Exporter;
use vars qw(@ISA @EXPORT_OK %EXPORT_TAGS);

@ISA = qw(Exporter);

@EXPORT_OK = qw(get_content get_args);
%EXPORT_TAGS = ( 'funcs' => [qw(get_content get_args)],
	       );

use APR;
use APR::Bucket;
use APR::Brigade;
use APR::Const qw(SUCCESS BLOCK_READ);

use Apache2::Log;
use Apache2::ServerUtil;
use Apache2::RequestRec ();
use Apache2::RequestIO ();
use Apache2::SubRequest ();
use Apache2::Connection ();
use Apache2::Filter ();
use Apache2::Const qw(:common :methods 
		      HTTP_MOVED_TEMPORARILY REDIRECT
		      MODE_READBYTES);

#
# get_content -- this routine does not really belong here
#

use constant IOBUFSIZE => 8192;

sub parse_args
  {
    my $data=shift;

    return () unless defined $data and $data;

    return map {
      tr/+/ /;
      s/%([0-9a-fA-F]{2})/pack("C",hex($1))/ge;
      $_;
    } split /[=&;]/, $data, -1;
  }

sub get_content
  {
    my $r = shift;

    my $bb = APR::Brigade->new($r->pool,
                               $r->connection->bucket_alloc);

    my $data = '';
    my $seen_eos = 0;
    do {
        $r->input_filters->get_brigade($bb, Apache2::Const::MODE_READBYTES,
                                       APR::Const::BLOCK_READ, IOBUFSIZE);
        while (!$bb->is_empty) {
            my $b = $bb->first;

            if ($b->is_eos) {
                $seen_eos++;
                last;
            }

            if ($b->read(my $buf)) {
                $data .= $buf;
            }

            $b->delete;
        }
    } while (!$seen_eos);

    $bb->destroy;

    return $data unless wantarray;
    return parse_args($data);
}

sub get_args
  {
    my $r = shift;
    my $data = $r->args;
    return $data unless wantarray;
    return parse_args($data);
  }


1;
