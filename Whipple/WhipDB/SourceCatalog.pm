package Whipple::WhipDB::SourceCatalog;

# CREATE TABLE Source_Catalog (
# 	Source_ID	VARCHAR(5)	PRIMARY_KEY,
# 	Source_Name	VARCHAR(30)	NOT NULL,
# 	Right_Ascention	FLOAT,
# 	Declination	FLOAT,
# 	Epoch		FLOAT
# );

use strict;
use DBI;
use Data::Dumper;

use Whipple::WhipDB::TableAccess;

use vars qw(@ISA $SQLMethods);
@ISA=qw(Whipple::WhipDB::TableAccess);

$SQLMethods=
  {
   "insert"           => q{INSERT INTO Source_Catalog 
			     VALUES ( ?, ?, ?, ?, ? )},
   "select_all"       => q{SELECT * FROM Source_Catalog},
   "select_source_id" => q{SELECT * FROM Source_Catalog
			     WHERE Source_ID = ?},
  };

sub SQLMethods
  {
    return $SQLMethods;
  }


sub GetCatalog
  {
    my $self=shift;
    my $run_no=shift;
    my $utc_date=shift;

    return undef if(not $self->select_all());
    my %catalog;
    my $stuff;
    while(defined($stuff=$self->fetchrow_hashref))
      {
	$catalog{$stuff->{"source_id"}} = $stuff;
      }

    return \%catalog;
  }

1;
