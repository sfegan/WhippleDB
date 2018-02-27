package Whipple::WhipDB::UserProfile;

# CREATE TABLE User_Profile (
# 	Login		VARCHAR(40)	NOT NULL PRIMARY KEY,
# 	Password	VARCHAR(36),
# 	UID		INTEGER		NOT NULL UNIQUE
# );

use strict;
use DBI;
use Data::Dumper;

use Whipple::WhipDB::TableAccess;

use vars qw(@ISA $SQLMethods);
@ISA=qw(Whipple::WhipDB::TableAccess);

$SQLMethods=
  {
   "insert"           => q{INSERT INTO User_Profile VALUES ( ?, ?, ? )},
   "insert_in_seq"    => q{INSERT INTO User_Profile
			     SELECT ?, ?, NEXTVAL('UID_Seq')},
   "select_login"     => q{SELECT * FROM User_Profile WHERE Login = ?},
   "select_uid"       => q{SELECT * FROM User_Profile WHERE UID = ?},
  };

sub SQLMethods
  {
    return $SQLMethods;
  }

sub GetUID
  {
    my $self=shift;
    my $login=shift;

    return undef if(not $self->select_login($login));
    my $stuff=$self->fetchrow_hashref;
    return undef if(not defined $stuff);
    return $stuff->{"uid"};
  }

sub GetOrCreateUID
  {
    my $self=shift;
    my $login=shift;

    my $ID=$self->GetUID($login);
    return $ID if(defined $ID);
    return undef if($self->err);
    
    return undef if(not $self->insert_in_seq($login,undef));
    return $self->GetUID($login);
  }

1;
