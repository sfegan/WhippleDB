package Whipple::WhipDB::DataFile;

use strict;
use DBI;
use Data::Dumper;

use Whipple::WhipDB::TableAccess;

use vars qw(@ISA $SQLMethods);
@ISA=qw(Whipple::WhipDB::TableAccess);

$SQLMethods=
  {
   "insert"           => q{INSERT INTO Data_File VALUES ( ?, ?, ?, ?, ? )},
   "select_runid"     => q{SELECT * FROM Data_File WHERE Run_ID = ?},
   "select_runid_fmt" => q{SELECT * FROM Data_File WHERE 
			     Run_ID = ? AND
			       File_Type = ? AND Compress = ? },
   "select_run"       => q{SELECT * FROM Data_File WHERE Run_ID =
			     ( SELECT Run_ID FROM Run_Ident WHERE
			       Run_No = ? AND UTC_Date = ? )},
   "select_run_fmt"   => q{SELECT * FROM Data_File WHERE Run_ID =
			     ( SELECT Run_ID FROM Run_Ident WHERE
			       Run_No = ? AND UTC_Date = ? ) AND 
				 File_Type = ? AND Compress = ? },
   "select_date"      => q{SELECT ri.Run_No, ri.UTC_DATE, me.* 
			     FROM Data_File me, Run_Ident ri
			       WHERE me.Run_ID = ri.Run_ID AND
				 ri.UTC_Date = ?},
   "select_runno"     => q{SELECT ri.Run_No, ri.UTC_DATE, me.* 
			     FROM Data_File me, Run_Ident ri
			       WHERE me.Run_ID = ri.Run_ID AND
				 ri.Run_No = ?},
   "select_archid"    => q{SELECT me.* FROM Data_File me, Arch_File af
			     WHERE me.Run_ID = af.Run_ID AND
			       me.File_Type=af.File_Type AND
				 me.Compress=af.Compress AND
				   af.Arch_ID = ?},
   "delete_runid"     => q{DELETE FROM Data_File WHERE Run_ID = ?},
   "delete_runid_fmt" => q{DELETE FROM Data_File WHERE 
			     Run_ID = ? AND 
			       File_Type = ? AND Compress = ? },
   "delete_run"       => q{DELETE FROM Data_File WHERE Run_ID =
			     ( SELECT Run_ID FROM Run_Ident WHERE
			       Run_No = ? AND UTC_Date = ? )},
   "delete_run_fmt"   => q{DELETE FROM Data_File WHERE Run_ID =
			     ( SELECT Run_ID FROM Run_Ident WHERE
			       Run_No = ? AND UTC_Date = ? ) AND 
				 File_Type = ? AND Compress = ? },
   "delete_date"      => q{DELETE FROM Data_File WHERE Run_ID IN
			     ( SELECT Run_ID FROM Run_Ident WHERE
			       UTC_Date = ? )},
   "update_runid_fmt" => q{UPDATE Data_File SET Bytes = ?, MD5_Sum = ?
			     WHERE Run_ID = ? AND
			       File_Type = ? AND Compress = ? },
   "update_run_fmt"   => q{UPDATE Data_File SET Bytes = ?, MD5_Sum = ?
			     WHERE Run_ID IN
			       ( SELECT Run_ID FROM Run_Ident WHERE
				 UTC_Date = ? )},
   "get_runs_without_archive" =>
     q{SELECT * FROM Data_File df WHERE NOT EXISTS 
	 ( SELECT Run_ID FROM Arch_File af WHERE af.Run_ID=df.Run_ID )},
  };

sub SQLMethods
  {
    return $SQLMethods;
  }

1;
