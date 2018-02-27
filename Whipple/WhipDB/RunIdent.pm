package Whipple::WhipDB::RunIdent;

# CREATE TABLE Run_Ident (
# 	Run_No		INT		NOT NULL,
# 	UTC_Date	DATE		NOT NULL,
# 	Seq_No		INT		NOT NULL,
# 	PRIMARY KEY ( Run_No, UTC_Date )
# );

use strict;
use DBI;
use Data::Dumper;

use Whipple::WhipDB::TableAccess;

use vars qw(@ISA $SQLMethods);
@ISA=qw(Whipple::WhipDB::TableAccess);

$SQLMethods=
  {
   "insert"           => q{INSERT INTO Run_Ident VALUES ( ?, ?, ? )},
   "insert_in_seq"    => q{INSERT INTO Run_Ident
			     SELECT ?, ?, NEXTVAL('Run_Ident_Seq')},
   "select_runid"     => q{SELECT Run_No, UTC_Date 
			     FROM Run_Ident WHERE Run_ID = ?},
   "select_run"       => q{SELECT Run_ID FROM Run_Ident WHERE 
			     Run_No = ? AND UTC_Date = ?},
   "select_date"      => q{SELECT * FROM Run_Ident WHERE 
			     UTC_Date = ?},
   "select_runno"     => q{SELECT * FROM Run_Ident WHERE 
			     Run_No = ?},
   "select_archid"    => q{SELECT me.* FROM Run_Ident me, Arch_File af
			     WHERE me.Run_ID = af.Run_ID
			       AND af.Arch_ID = ? },
   "delete_runid"     => q{DELETE FROM Run_Ident WHERE Run_ID = ?},
   "delete_run"       => q{DELETE FROM Run_Ident WHERE
			     Run_No = ? AND UTC_Date = ?},
   "update_runid"     => q{UPDATE Run_Ident SET
			     Run_No = ? AND UTC_Date = ?
			       WHERE Run_ID = ?},
   "get_runs_without_archive" =>
     q{SELECT * FROM Run_Ident ri WHERE NOT EXISTS 
	 ( SELECT Run_ID FROM Arch_File af WHERE af.Run_ID=ri.Run_ID )
	   ORDER BY UTC_Date, Run_NO},
   "get_dates_without_archive" =>
     q{SELECT UTC_Date, count(Run_ID) 
	 FROM Run_Ident ri WHERE NOT EXISTS 
	   ( SELECT Run_ID FROM Arch_File af WHERE af.Run_ID=ri.Run_ID )
	     GROUP BY UTC_Date ORDER BY UTC_Date},
   "get_runs_without_datafile" =>
     q{SELECT * FROM Run_Ident ri WHERE NOT EXISTS 
	 ( SELECT Run_ID FROM Data_File df WHERE df.Run_ID=ri.Run_ID )
	   ORDER BY UTC_Date, Run_NO},
   "get_runs_without_runinfo" =>
     q{SELECT * FROM Run_Ident ri WHERE NOT EXISTS 
	 ( SELECT Run_ID FROM Run_Info inf WHERE inf.Run_ID=ri.Run_ID )
	   ORDER BY UTC_Date, Run_NO},
  };

sub SQLMethods
  {
    return $SQLMethods;
  }

sub GetID
  {
    my $self=shift;
    my $run_no=shift;
    my $utc_date=shift;

    return undef if(not $self->select_run($run_no,$utc_date));
    my $stuff=$self->fetchrow_arrayref;
    return undef if(not defined $stuff);
    return $stuff->[0];
  }

sub GetOrCreateID
  {
    my $self=shift;
    my $run_no=shift;
    my $utc_date=shift;

    my $ID=$self->GetID($run_no,$utc_date);
    return $ID if(defined $ID);
    return undef if($self->err);
    
    return undef if(not $self->insert_in_seq($run_no,$utc_date));
    return $self->GetID($run_no,$utc_date);
  }

1;
