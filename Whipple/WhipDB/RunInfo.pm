package Whipple::WhipDB::RunInfo;

# CREATE TABLE Run_Info (
# 	Run_ID		INTEGER		NOT NULL PRIMARY KEY,
# 	Source_ID	VARCHAR(5)	NOT NULL,
# 	Source_Name	VARCHAR(30)	NOT NULL,
# 	UTC_Time	TIME,
# 	Duration	INTEGER,
# 	Mode		INTEGER,
# 	Sky_Q		INTEGER,
# 	Starting_El	FLOAT4,
# 	Comment		VARCHAR(40)
# );

use strict;
use DBI;
use Data::Dumper;

use Whipple::WhipDB::TableAccess;

use vars qw(@ISA $SQLMethods);
@ISA=qw(Whipple::WhipDB::TableAccess);

$SQLMethods=
  {
   "insert"           => q{INSERT INTO Run_Info 
			     VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?)},
   "insert_in_seq"    => q{INSERT INTO Run_Info
			     SELECT NEXTVAL('Run_Ident_Seq'),
			     ?, ?, ?, ?, ?, ?, ?, ?},
   "select_runid"     => q{SELECT * FROM Run_Info WHERE Run_ID= ?},
   "select_run"       => q{SELECT Run_No, UTC_Date, rinf.* 
			     FROM Run_Ident rid, Run_Info rinf
			       WHERE rid.Run_ID=rinf.Run_ID AND
				 Run_No = ? AND UTC_Date = ?},
   "select_all_source_id_source_name" =>
                         q{SELECT Source_ID, Source_Name FROM Run_Info
			     GROUP BY Source_ID, Source_Name
			       ORDER BY Source_ID, Source_Name},
   "delete_runid"     => q{DELETE FROM Run_Info WHERE Run_ID = ?},
   "update_utc_time_runid" => 
                         q{UPDATE Run_Info SET UTC_Time = ? WHERE Run_ID = ?},
   "update_source_id_runid" => 
                         q{UPDATE Run_Info SET Source_ID = ? WHERE Run_ID = ?},
   "update_source_name_runid" => 
                         q{UPDATE Run_Info SET Source_Name = ? 
			     WHERE Run_ID = ?},
   "update_duration_runid" => 
                         q{UPDATE Run_Info SET Duration = ? WHERE Run_ID = ?},
   "update_mode_runid" => 
                         q{UPDATE Run_Info SET Mode = ? WHERE Run_ID = ?},
   "update_sky_q_runid" => 
                         q{UPDATE Run_Info SET Sky_Q = ? WHERE Run_ID = ?},
   "update_starting_el_runid" => 
                         q{UPDATE Run_Info SET Starting_El = ? 
			     WHERE Run_ID = ?},
   "update_scope_config_runid" => 
                         q{UPDATE Run_Info SET Scope_Config = ? 
			     WHERE Run_ID = ?},
   "update_comment_runid" => 
                         q{UPDATE Run_Info SET Comment = ? WHERE Run_ID = ?},
   "update_source_name_source_id" => 
                         q{UPDATE Run_Info SET Source_Name = ? 
			     WHERE Source_ID = ?},
   "update_source_id_source_id" => 
                         q{UPDATE Run_Info SET Source_ID = ? 
			     WHERE Source_ID = ?},
   "source_catalog"   => q{SELECT Source_ID, Source_Name, 
			     count(Source_ID) AS "count",
			     sum(Duration) AS "duration" 
			       FROM Run_Info GROUP BY Source_ID, Source_Name},
   "summary_dates" =>    q{SELECT Source_ID, Sky_Q, 
			     count(Source_ID) AS runs, sum(Duration) AS time
			       FROM Run_Ident rid, Run_Info ri WHERE 
				 rid.Run_ID = ri.run_ID AND
				   UTC_Date >= ? AND UTC_Date <= ? 
				     GROUP BY Source_ID, Sky_Q},
   "latest_run"  =>      q{SELECT max(UTC_Date)
			     FROM Run_Ident rid, Run_Info ri WHERE
			       rid.Run_ID = ri.run_ID },
  };


sub SQLMethods
  {
    return $SQLMethods;
  }

sub getLatestRun
  {
    my $self=shift;

    return undef if(not $self->latest_run());
    my $stuff=$self->fetchrow_arrayref;
    return undef if(not defined $stuff);
    return $stuff->[0];
  }

1;
