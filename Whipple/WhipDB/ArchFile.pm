package Whipple::WhipDB::ArchFile;

# CREATE TABLE Arch_File (
# 	Seq_No		INT		NOT NULL PRIMARY KEY,
# 	Format		VARCHAR(15)	NOT NULL,
# 	CD_ident	INT		NOT NULL
# );

use strict;
use DBI;
use Data::Dumper;

use Whipple::WhipDB::TableAccess;

use vars qw(@ISA $SQLMethods);
@ISA=qw(Whipple::WhipDB::TableAccess);

$SQLMethods=
  {
   "insert"           => q{INSERT INTO Arch_File VALUES ( ?, ?, ?, ? )},
   "select_runid"     => q{SELECT * FROM Arch_File WHERE Run_ID = ?},
   "select_run"       => q{SELECT * FROM Arch_File WHERE Run_ID =
			     ( SELECT Run_ID FROM Run_Ident WHERE
			       Run_No = ? AND UTC_Date = ? )},
   "select_date"      => q{SELECT ri.Run_No, ri.UTC_DATE, me.* 
			     FROM Arch_File me, Run_Ident ri
			       WHERE me.Run_ID = ri.Run_ID AND
				 ri.UTC_Date = ?},
   "select_runno"     => q{SELECT ri.Run_No, ri.UTC_DATE, me.* 
			     FROM Arch_File me, Run_Ident ri
			       WHERE me.Run_ID = ri.Run_ID AND
				 ri.Run_No = ?},
   "select_archid"    => q{SELECT ri.Run_No, ri.UTC_DATE, me.* 
			     FROM Arch_File me, Run_Ident ri
			       WHERE me.Run_ID = ri.Run_ID AND
				 Arch_ID = ?},
   "delete_runid"     => q{DELETE FROM Arch_File WHERE Run_ID = ?},
   "delete_run"       => q{DELETE FROM Arch_File WHERE Run_ID =
			     ( SELECT Run_ID FROM Run_Ident WHERE
			       Run_No = ? AND UTC_Date = ? )},
   "delete_date"      => q{DELETE FROM Arch_File WHERE Run_ID IN
			     ( SELECT Run_ID FROM Run_Ident WHERE
			       UTC_Date = ? )},
   "update_fmt_runid" => q{UPDATE Arch_File 
			     SET File_Type = ? , Compress = ?
			       WHERE Run_ID = ? },
   "update_fmt_run"   => q{UPDATE Arch_File 
			     SET File_Type = ? , Compress = ?
			       WHERE Run_ID =
				 ( SELECT Run_ID FROM Run_Ident WHERE
				   Run_No = ? AND UTC_Date = ? )},
   "update_fmt_date"  => q{UPDATE Arch_File 
			     SET File_Type = ? , Compress = ?
			       FROM Run_Ident ri
				 WHERE Arch_File.Run_ID = ri.Run_ID AND
				   ri.UTC_Date = ?},
   "update_arch_runid" => q{UPDATE Arch_File SET Arch_ID = ?
			      WHERE Run_ID = ? },
   "update_arch_run"   => q{UPDATE Arch_File SET Arch_ID = ?
			      WHERE Run_ID =
				( SELECT Run_ID FROM Run_Ident WHERE
				  Run_No = ? AND UTC_Date = ? )},
   "update_arch_date"  => q{UPDATE Arch_File SET Arch_ID = ? 
			      WHERE Run_ID IN
				( SELECT Run_ID FROM Run_Ident WHERE
				  UTC_Date = ? )},
   "archfileinfo_run"  => q{SELECT af.*, ai.medium, ai.fs_format
			      FROM Arch_File af, Arch_Info ai 
				WHERE af.Arch_ID = ai.Arch_ID AND Run_ID =
				  ( SELECT Run_ID FROM Run_Ident WHERE
				    Run_No = ? AND UTC_Date = ? )},
  };

sub SQLMethods
  {
    return $SQLMethods;
  }

1;
