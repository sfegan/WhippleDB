package Whipple::WhipDB::TableAccess;

use strict;
use DBI;
use Data::Dumper;

use vars qw(@ISA);
@ISA=qw();

sub new
  {
    my $class=shift;
    my $db=shift;
    my $self={};

    die "Cannot clone type ".bless($class) if ref($class);
    bless $self,$class;
    
    $self->{"db"}=$db;

    my $Methods=$self->SQLMethods;
    my $method_name;
    foreach $method_name ( keys %{$Methods} )
      {
	$self->{"method"}->{$method_name}=
	  $db->prepare($Methods->{$method_name}) || die $db->errstr;
      }
    
    return $self;
  }

sub SQLMethods
  {
    return {};
  }

sub AUTOLOAD
  {
    no strict 'vars';
    my $self=shift;
    my $name=$AUTOLOAD;
    $name =~ s/.*://;
    
    return if $name eq "DESTROY";

    my $cursors=$self->{"
  }

;
