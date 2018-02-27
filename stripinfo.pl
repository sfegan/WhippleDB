$enter_into_cd_db=1;
if ($ARGV[0] eq '-n')
  {
    $enter_into_cd_db=0;
    shift @ARGV;
  }

$cd=$ARGV[0];


$fn="< cdinfo/cd.".$cd.".dat";
open FP,$fn || die $fn.":".$!;

$fs='iso9660/rr';

while((defined($L1=<FP>)) and (defined($L2=<FP>)))
  {
    chomp $L1; $L1 =~ s/^\s+//;
    chomp $L2; $L2 =~ s/^\s+//;

    ($inode, $KB, $mode, $lnks, $owner, $group, $bytes, $d1, $d2, $d3, $file)
      = split /\s+/,$L1,11;
    ( $md5, $file2)  
      = split /\s+/,$L2,2;

    if($file2 ne $file)
      {
	print STDERR $file," != ",$file1,"\n";
	die;
      }

    if(not $file =~ /[gd]([0-9]{6})\/gt([0-9]*)[.](.*)/)
      {
	print STDERR "Dont recognise file ",$file,"\n";
	next;
      }

    $date=$1; $date += 19000000;
    $runno=$2;
    $type=$3;

    if    ( lc $type eq "fz.bz2" ) { $type = "gdf"; $comp="bzip2" }
    elsif ( lc $type eq "fz.gz" )  { $type = "gdf"; $comp="gzip"  }
    elsif ( lc $type eq "fz.z" )   { $type = "gdf"; $comp="compress" }
    elsif ( lc $type eq "fz" )     { $type = "gdf"; $comp="none"   }
    elsif ( lc $type eq "fzg" )    { $type = "gdf";$comp="gzip";$fs='iso9660';}
    elsif ( lc $type eq "fzz" )    { $type = "gdf";$comp="compress";$fs='iso9660';}
    else { print STDERR "Unknown type: '",$type,"'\n"; next; }

    printf("INSERT INTO Run_Ident SELECT %d, '%d', NEXTVAL('Run_Ident_Seq')\n",
	   $runno, $date);

    printf("INSERT INTO Arch_File_v SELECT CURRVAL('Run_Ident_Seq'), ".
	   "'%s', '%s', %d\n", $type, %comp, $cd ) 
      if ( $enter_into_cd_db == 1 );

    printf("INSERT INTO Data_File_v SELECT CURRVAL('Run_Ident_Seq'), ".
	   "'%s', '%s', %d, '%s'\n", $type, $comp, $bytes, $md5);
  }

printf("INSERT INTO CD_Arch_Info_v VALUES ( %d, 'cd', '%s' )\n",$cd,$fs)
  if ( $enter_into_cd_db == 1 );
