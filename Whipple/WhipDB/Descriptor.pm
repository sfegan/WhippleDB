package Whipple::WhipDB::Descriptor::OBJ;

use strict;

sub new
  {
    my $class=shift;
    my $dbhandle=shift;
    my $self={};

    die "Cannot clone type ".bless($class) if ref($class);
    bless $self,$class;

    my $line;
    while(defined($line=$dbhandle->fetchrow_hashref))
      {
	my $ID=$line->{"id"};
	my $Val=$line->{"val"};

	$self->{"ID"}->[$ID]=$Val;
	$self->{"Val"}->{$Val}=$ID;
	foreach ( keys ( %{$line} ) )
	  {
	    $self->{"Item"}->[$ID]->{$_}=$line->{$_};
	  }
      }
   
    if($dbhandle->err)
      {
	$dbhandle->rollback;
	return undef;
      }

    $dbhandle->finish;
    $dbhandle->commit;
    return $self;
  }

sub ID
  {
    my $self=shift;
    my $Val=shift;
    return undef unless (exists $self->{"Val"}->{$Val});
    return $self->{"Val"}->{$Val};
  }

sub Val
  {
    my $self=shift;
    my $ID=shift;
    return undef unless (defined $self->{"ID"}->[$ID]);
    return $self->{"ID"}->[$ID];
  }
    
sub Item
  {
    my $self=shift;
    my $ID=shift;
    return undef unless (defined $self->{"Item"}->[$ID]);
    return $self->{"Item"}->[$ID];
  }
    
sub Vals
  {
    my $self=shift;
    return keys %{$self->{"Val"}};
  }

package Whipple::WhipDB::Descriptor;

use strict;
use DBI;
use Data::Dumper;

use Whipple::WhipDB::TableAccess;

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

sub get
  {
    my $self=shift;
    my $descriptor=shift;
    $self->{"handle"}=
      $self->{"db"}->prepare("SELECT * FROM ".$descriptor);
    return undef if(not $self->{"handle"});
    return undef if(not $self->{"handle"}->execute);
    return new Whipple::WhipDB::Descriptor::OBJ $self;
  }

sub commit
{
  my $self=shift;
  $self->{"db"}->commit;
}

sub rollback
{
  my $self=shift;
  $self->{"db"}->commit;
}

sub AUTOLOAD
  {
    use vars qw($AUTOLOAD);
    use Data::Dumper;
    my $self=shift;
    my $name=$AUTOLOAD;
    $name =~ s/.*://;
    
    my $db=$self->{"db"};
    
    return if $name eq "DESTROY";

    return $self->{"handle"}->$name(@_)
      if ( defined $self->{"handle"} );
    
    return $db->$name(@_);
  }

1;
