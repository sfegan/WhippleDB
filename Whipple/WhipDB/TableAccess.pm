package Whipple::WhipDB::TableAccess;

use strict;
use DBI;
use Data::Dumper;

use vars qw(@ISA %Handles);
@ISA=qw();
%Handles=();

sub new
  {
    my $class=shift;
    my $db=shift;
    my $self={};

    die "Cannot clone type ".bless($class) if ref($class);
    bless $self,$class;

    $self->{"db"}=$db;

    return $self;
  }

sub SQLMethods
  {
    return {};
  }

sub Handle
  {
    my $self=shift;
    my $handle=shift;

    $Handles{ref $self}=$handle;
    return int $handle->execute(@_);
  }

sub AUTOLOAD
  {
    use vars qw($AUTOLOAD);
    my $self=shift;
    my $name=$AUTOLOAD;
    $name =~ s/.*://;

    my $db=$self->{"db"};

    return if $name eq "DESTROY";

    my $methods=$self->SQLMethods;
    if(exists $methods->{$name})
      {
#print STDERR $methods->{$name},"\n";
	my $handle;
	if(not exists $self->{"method"}->{$name})
	  {
	    $handle=$self->{"method"}->{$name}=
	      $db->prepare($methods->{$name}) || die $db->errstr;
	  }
	else { $handle=$self->{"method"}->{$name} };
	
	my $retval=Handle($self,$handle,@_);
	return $retval;
      }

    return $Handles{ref $self}->$name(@_)
      if ( defined $Handles{ref $self} );

    return $db->$name(@_);
  }

1;
