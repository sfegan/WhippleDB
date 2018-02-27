package Whipple::WhipDB::Misc;

# CREATE TABLE Dark_Runs (
# 	First		DATE		NOT NULL,
# 	Last		DATE		NOT NULL
# );
#
# CREATE TABLE ObservingSeasons (
# 	First		DATE		NOT NULL,
# 	Last		DATE		NOT NULL
# );

use strict;
use DBI;
use Data::Dumper;

use Whipple::WhipDB::TableAccess;

use vars qw(@ISA $SQLMethods);
@ISA=qw(Whipple::WhipDB::TableAccess);

$SQLMethods=
  {
   "select_dr"        => q{SELECT * FROM Dark_Runs ORDER BY First},
   "select_os"        => q{SELECT * FROM Observing_Seasons ORDER BY First},
  };

sub SQLMethods
  {
    return $SQLMethods;
  }


sub GetDarkRuns
  {
    my $self=shift;

    return undef if(not $self->select_dr());
    my @dates;
    my $stuff;
    while(defined($stuff=$self->fetchrow_arrayref))
      {
	push @dates,[@$stuff];
      }
    
    return \@dates;
  }

sub GetObservingSeasons
  {
    my $self=shift;

    return undef if(not $self->select_os());
    my @dates;
    my $stuff;
    while(defined($stuff=$self->fetchrow_arrayref))
      {
	push @dates,[@$stuff];
      }
    
    return \@dates;
  }

1;
