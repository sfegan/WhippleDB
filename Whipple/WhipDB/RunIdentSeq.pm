package Whipple::WhipDB::RunIdentSeq;

# CREATE SEQUENCE Run_Ident_Seq;

use strict;
use DBI;
use Data::Dumper;

use Whipple::WhipDB::TableAccess;

use vars qw(@ISA $SQLMethods);
@ISA=qw(Whipple::WhipDB::TableAccess);

$SQLMethods=
  {
   "nextval"          => q{SELECT NEXTVAL('Run_Ident_Seq')},
   "currval"          => q{SELECT CURRVAL('Run_Ident_Seq')},
  };


sub SQLMethods
  {
    return $SQLMethods;
  }

1;
