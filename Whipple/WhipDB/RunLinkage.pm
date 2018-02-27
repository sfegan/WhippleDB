package Whipple::WhipDB::RunLinkage;

# CREATE TABLE Run_Linkage (
# 	Run_ID		INTEGER		NOT NULL PRIMARY KEY,
# 	Off_ID		INTEGER,
# 	N2_ID		INTEGER
# );

use strict;
use DBI;
use Data::Dumper;

use Whipple::WhipDB::TableAccess;

use vars qw(@ISA $SQLMethods);
@ISA=qw(Whipple::WhipDB::TableAccess);

$SQLMethods=
  {
   "insert"           => q{INSERT INTO Run_Linkage 
			     VALUES ( ?, ?, ? )},
   "insert_in_seq"    => q{INSERT INTO Run_Linkage
			     SELECT NEXTVAL('Run_Ident_Seq'), ?, ?},
   "select_runid"     => q{SELECT * FROM Run_Linkage WHERE Run_ID = ?},
   "select_run"       => q{SELECT Run_No, UTC_Date, rl.* 
			     FROM Run_Ident rid, Run_Linkage rl
			       WHERE rid.RunIdent=rl.RunIdent AND
				 Run_No = ? AND UTC_Date = ?},
   "select_date"      => q{SELECT Run_No, UTC_Date, rl.* 
			     FROM Run_Ident rid, Run_Linkage rl
			       WHERE rid.RunIdent=rl.RunIdent AND
				 UTC_Date = ?},
   "delete_runid"     => q{DELETE FROM Run_Linkage WHERE Run_ID = ?},
   "update_offid_runid" => 
                         q{UPDATE Run_Linkage SET Off_ID = ? WHERE Run_ID = ?},
   "update_n2id_runid" => 
                         q{UPDATE Run_Linkage SET N2_ID = ? WHERE Run_ID = ?},
  };


sub SQLMethods
  {
    return $SQLMethods;
  }

1;
