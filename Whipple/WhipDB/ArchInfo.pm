package Whipple::WhipDB::ArchInfo;

# CREATE TABLE Arch_Info (
# 	Arch_ID		INTEGER		NOT NULL PRIMARY KEY,
# 	Medium		INTEGER		NOT NULL,
# 	FS_Format	INTEGER		NOT NULL
# );

use strict;
use DBI;
use Data::Dumper;

use Whipple::WhipDB::TableAccess;

use vars qw(@ISA $SQLMethods);
@ISA=qw(Whipple::WhipDB::TableAccess);

$SQLMethods=
  {
   "insert"           => q{INSERT INTO Arch_Info VALUES ( ?, ?, ? )},
   "select_archid"    => q{SELECT * FROM Arch_Info WHERE Arch_ID = ?},
   "get_last_archive" => q{SELECT max(Arch_ID) AS Arch_ID FROM Arch_Info},
   "delete_archid"    => q{DELETE FROM Arch_Info WHERE Arch_ID = ?},
   "update_archid"    => q{UPDATE Arch_Info SET Medium = ?, FS_Format = ?
			     WHERE Arch_ID = ?},
  };

sub SQLMethods
  {
    return $SQLMethods;
  }

sub GetLastArchive
  {
    my $self=shift;
    return undef if(not $self->get_last_archive);
    my $stuff=$self->fetchrow_arrayref;
    return undef if(not defined $stuff);
    return $stuff->[0];
  }

1;
