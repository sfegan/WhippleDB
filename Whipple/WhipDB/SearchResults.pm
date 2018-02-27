package Whipple::WhipDB::SearchResults;

# CREATE TABLE Search_Results (
# 	Search_ID	INTEGER		NOT NULL,
# 	Run_ID		INTEGER		NOT NULL,
# 	Accepted	BOOLEAN		DEFAULT TRUE NOT NULL,
# 	PRIMARY KEY ( Search_ID, Run_ID )
# );


use strict;
use DBI;
use Data::Dumper;

use Whipple::WhipDB::TableAccess;

use vars qw(@ISA $SQLMethods);
@ISA=qw(Whipple::WhipDB::TableAccess);

$SQLMethods=
  {
   "select_sid"        => q{SELECT * FROM Search_Results WHERE Search_ID = ?},
   "delete_sid"        => q{DELETE FROM Search_Results WHERE Search_ID = ?},
   "delete_srid"       => q{DELETE FROM Search_Results 
			      WHERE Search_ID = ? AND Run_ID = ? },
   "update_acc_srid"   => q{UDATE Search_Results SET Accepted = ? 
			      WHERE Search_ID = ? AND Run_ID = ? },

   # full selections

   "sel_results_sid"   => q{SELECT Run_ID, Accepted FROM Search_Results 
			      WHERE Search_ID = ?},
   "sel_runident_sid"  => q{SELECT me.* FROM Run_Ident me, Search_Results sr
			      WHERE me.Run_ID=sr.Run_ID AND Search_ID = ?},
   "sel_runinfo_sid"   => q{SELECT me.* FROM Run_Info me, Search_Results sr
			      WHERE me.Run_ID=sr.Run_ID AND Search_ID = ?},
   "sel_archfile_sid"  => q{SELECT me.* FROM Arch_File me, Search_Results sr
			      WHERE me.Run_ID=sr.Run_ID AND Search_ID = ?},
   "sel_datafile_sid"  => q{SELECT me.* FROM Data_File me, Search_Results sr
			      WHERE me.Run_ID=sr.Run_ID AND Search_ID = ?},
   "sel_rlinkoff_sid"  => q{SELECT me.Run_ID, me.Off_id, 
			      offr.Run_No AS Off_Run_No FROM Run_Linkage me,
			      Search_Results sr, Run_Ident offr
				WHERE me.Run_ID=sr.Run_ID AND Search_ID = ? AND
				  me.Off_ID=offr.Run_ID},
   "sel_rlinkn2_sid"   => q{SELECT me.Run_ID, me.N2_id, 
			      n2.Run_No AS N2_Run_No FROM Run_Linkage me,
			      Search_Results sr, Run_Ident n2
				WHERE me.Run_ID=sr.Run_ID AND Search_ID = ? AND
				  me.N2_ID=n2.Run_ID},

   # limited selections

   "lim_results_sid"   => q{SELECT Run_ID, Accepted FROM Search_Results 
			      WHERE Search_ID = ? ORDER BY Run_ID
				LIMIT ? OFFSET ? },
   "lim_runident_sid"  => q{SELECT me.* FROM Run_Ident me, Search_Results sr
			      WHERE me.Run_ID=sr.Run_ID AND Search_ID = ?
				ORDER BY Run_ID LIMIT ? OFFSET ? },
   "lim_runinfo_sid"   => q{SELECT me.* FROM Run_Info me, Search_Results sr
			      WHERE me.Run_ID=sr.Run_ID AND Search_ID = ?
				ORDER BY Run_ID LIMIT ? OFFSET ? },
   "lim_archfile_sid"  => q{SELECT me.* FROM Arch_File me, Search_Results sr
			      WHERE me.Run_ID=sr.Run_ID AND Search_ID = ?
				ORDER BY Run_ID LIMIT ? OFFSET ? },
   "lim_datafile_sid"  => q{SELECT me.* FROM Data_File me, Search_Results sr
			      WHERE me.Run_ID=sr.Run_ID AND Search_ID = ?
				ORDER BY Run_ID LIMIT ? OFFSET ? },
   "lim_rlinkoff_sid"  => q{SELECT me.Run_ID, me.Off_id, 
			      offr.Run_No AS Off_Run_No FROM Run_Linkage me,
			      Search_Results sr, Run_Ident offr
				WHERE me.Run_ID=sr.Run_ID AND Search_ID = ? AND
				  me.Off_ID=offr.Run_ID
				    ORDER BY Run_ID LIMIT ? OFFSET ? },
   "lim_rlinkn2_sid"   => q{SELECT me.Run_ID, me.N2_id, 
			      n2.Run_No AS N2_Run_No FROM Run_Linkage me,
			      Search_Results sr, Run_Ident n2
				WHERE me.Run_ID=sr.Run_ID AND Search_ID = ? AND
				  me.N2_ID=n2.Run_ID
				    ORDER BY Run_ID LIMIT ? OFFSET ? },

   "count"             => q{SELECT Search_ID, count(Search_ID)
                              FROM Search_Results JOIN Search USING (Search_ID)
                                WHERE UID = ? GROUP BY Search_ID},

   "summary"           => q{SELECT count(sr.Run_ID), sum(Duration) 
			      FROM Search_Results sr, Run_Info ri
				WHERE Search_ID = ? AND Accepted = 't' AND
				  sr.Run_ID=ri.Run_ID },
   
   "delete_unmatched"  => q{DELETE from Search_Results WHERE NOT EXISTS
			      ( SELECT Search_ID FROM Search st
				WHERE st.Search_ID = Search_Results.Search_ID )},
  };

sub SQLMethods
  {
    return $SQLMethods;
  }
