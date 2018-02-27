package Whipple::WhipDB;

use strict;
use DBI;
use Data::Dumper;

@Whipple::WhipDB::ISA=qw();

use constant DB_SOURCE => 'dbi:Pg:dbname=whipple';
use constant DB_USER   => 'sfegan';
use constant DB_AUTH   => '';

use Whipple::WhipDB::RunIdentSeq;
use Whipple::WhipDB::RunIdent;
use Whipple::WhipDB::DataFile;
use Whipple::WhipDB::ArchFile;
use Whipple::WhipDB::ArchInfo;
use Whipple::WhipDB::RunInfo;
use Whipple::WhipDB::RunLinkage;
use Whipple::WhipDB::UserProfile;
use Whipple::WhipDB::Search;
use Whipple::WhipDB::SearchResults;
use Whipple::WhipDB::SearchTerm;
use Whipple::WhipDB::SourceCatalog;
use Whipple::WhipDB::Descriptor;
use Whipple::WhipDB::Misc;

use vars qw(%SubMethods);
%SubMethods=( "RunIdentSeq"   => 1, 
	      "RunIdent"      => 1,
	      "DataFile"      => 1,
	      "ArchFile"      => 1,
	      "ArchInfo"      => 1,
	      "RunInfo"       => 1,
	      "RunLinkage"    => 1,
	      "UserProfile"   => 1,
	      "Search"        => 1,
	      "SearchResults" => 1,
	      "SearchTerm"    => 1,
	      "SourceCatalog" => 1,
	      "Descriptor"    => 1, 
	      "Misc"          => 1, 
	    );

sub new
  {
    my $class=shift;
    my $self={};
    my $trace=shift;

    die "Cannot clone type ".bless($class) if ref($class);
    bless $self,$class;
    
    DBI->trace($trace) if ( defined $trace );

    my $dbh = DBI->connect(DB_SOURCE, DB_USER, DB_AUTH,
			   { PrintError => 1, AutoCommit => 0 });
    if(not defined $dbh)
      {
	print STDERR "Could not connect to DB\n";
	die $DBI::errstr;
      }

    if( not $dbh->do(q{SET DateStyle TO 'ISO'}) )
      {
	print STDERR "Could not SET DateStyle TO 'ISO'\n";
	die $DBI::errstr;
      }
    $dbh->commit;

    if( not $dbh->do(q{SET TimeZone TO 'UTC'}) )
      {
	print STDERR "Could not SET TimeZone TO 'UTC'\n";
	die $DBI::errstr;
      }
    $dbh->commit;
    
    $self->{"dbh"}=$dbh;
    $self->{"derived"}={};
    
    return $self;
  }

sub dbh
  {
    my $self=shift;
    return $self->{"dbh"};
  }

sub ping
  {
    my $self=shift;
    return $self->{"dbh"}->ping;
  }

sub DESTROY
  {
  }    

sub AUTOLOAD
  {
    use vars qw($AUTOLOAD);
    my $self=shift;
    my $name=$AUTOLOAD;
    $name =~ s/.*://;
    
    return if $name eq "DESTROY";

    if(exists $SubMethods{$name} )
      {
	my $class="Whipple::WhipDB::".$name;
	$self->{"derived"}->{$name} = $class->new($self)
	  if ( not exists $self->{"derived"}->{$name} );
	return $self->{"derived"}->{$name};
      }
    
    return $self->{"dbh"}->$name(@_);
  }

1;
