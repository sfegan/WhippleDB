package Whipple::WhipDB::SearchTerm;

# CREATE TABLE Search_Term (
# 	Search_ID	INTEGER		NOT NULL,
# 	Term_ID		INTEGER		NOT NULL,
# 	Field		INTEGER		NOT NULL,
# 	Op_Code		INTEGER		NOT NULL,
# 	Value_Rep	VARCHAR(80),
# 	IncludeNulls	BOOLEAN		DEFAULT TRUE NOT NULL,
# 	PRIMARY KEY ( Search_ID, Term_ID )
# );

use strict;
use DBI;
use Data::Dumper;

use Whipple::WhipDB::TableAccess;

use vars qw(@ISA $SQLMethods);
@ISA=qw(Whipple::WhipDB::TableAccess);

$SQLMethods=
  {
   "insert"            => q{INSERT INTO Search_Term 
			      VALUES ( ?, ?, ?, ?, ?, ? )},
   "select_sid"        => q{SELECT * FROM Search_Term WHERE Search_ID = ?},
   "delete_sid"        => q{DELETE FROM Search_Term WHERE Search_ID = ?},

   "delete_unmatched"  => q{DELETE from Search_Term WHERE NOT EXISTS
			      ( SELECT Search_ID FROM Search st
				WHERE st.Search_ID = Search_Term.Search_ID )},
  };

sub SQLMethods
  {
    return $SQLMethods;
  }
