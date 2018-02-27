package Whipple::WhipDB::Search;

# CREATE TABLE Search (
# 	Search_ID	INTEGER		NOT NULL PRIMARY KEY,
# 	UID		INTEGER		NOT NULL,
# 	Time_Stamp	TIMESTAMP	NOT NULL,
# 	Search_Name	VARCHAR(40)	NOT NULL,
# 	Have_Criteria	BOOLEAN		DEFAULT TRUE NOT NULL,
# 	Have_Results	BOOLEAN		DEFAULT FALSE NOT NULL,
# 	Save		BOOLEAN		DEFAULT FALSE NOT NULL,
# );

use strict;
use DBI;
use Data::Dumper;

use Whipple::WhipDB::TableAccess;

use vars qw(@ISA $SQLMethods);
@ISA=qw(Whipple::WhipDB::TableAccess);

$SQLMethods=
  {
   "insert_full"       => q{INSERT INTO Search VALUES ( ?, ?, ?, ?, ?, ?, ? )},
   "insert_simple"     => q{INSERT INTO Search
			     SELECT ?, ?, CURRENT_TIMESTAMP, ? },
   "select_nextid"     => q{SELECT NEXTVAL('Search_Seq')},
   "select_suid"       => q{SELECT * FROM Search 
			     WHERE Search_ID = ? and UID = ?},
   "select_uid"        => q{SELECT * FROM Search WHERE UID = ? 
			     ORDER BY Time_Stamp DESC},
   "update_stamp_suid" => q{UPDATE Search SET Time_Stamp=CURRENT_TIMESTAMP
			     WHERE Search_ID = ? and UID = ?},
   "update_name_suid"  => q{UPDATE Search SET Search_Name=?
			     WHERE Search_ID = ? and UID = ?},
   "update_havec_suid" => q{UPDATE Search SET Have_Criteria=?
			     WHERE Search_ID = ? and UID = ?},
   "update_haver_suid" => q{UPDATE Search SET Have_Results=?
			     WHERE Search_ID = ? and UID = ?},
   "update_save_suid"  => q{UPDATE Search SET Save=?
			     WHERE Search_ID = ? and UID = ?},
   "update_name_save_suid"  => 
                          q{UPDATE Search SET Search_Name=?, Save=?
			     WHERE Search_ID = ? and UID = ?},
   "delete_suid"       => q{DELETE FROM Search
			     WHERE Search_ID = ? and UID = ?},
   "delete_old"        => q{DELETE from Search WHERE Search_ID IN 
			     ( SELECT Search_ID FROM Search WHERE Time_Stamp < 
			       ( SELECT CURRENT_TIMESTAMP - Age FROM 
				 Search_Results_Maxage ) AND Save='false' )}
  };

sub SQLMethods
  {
    return $SQLMethods;
  }
