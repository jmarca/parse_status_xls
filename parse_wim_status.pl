#!/usr/bin/perl -w

use warnings;
use strict;
use version; our $VERSION = qv('0.2.1');
use English qw(-no_match_vars);
use Carp;
use Data::Dumper;

use CalVAD::WIM::ParseStatusSpreadsheeets 0.2.3;
use CalVAD::WIM::StoreStatusSpreadsheeets 0.2.4;


use Getopt::Long;
use Pod::Usage;


##################################################
# initialize with command line options
##################################################
my @files = ();
my $db    = 'spatialvds';
my $host  = 'metis.its.uci.edu';
my $port  = 5432;
my $pattern = '.*status.*\.xlsx?$';
my $dbuser;
my $dbpass = '';
my $year;
my $path;
my $help;
my $write_undefined = 0;

my $result = GetOptions(
    'files=s'    => \@files,
    'year=i'     => \$year,
    'database=s' => \$db,
    'user=s'     => \$dbuser,
    'host=s'     => \$host,
    'port=i'     => \$port,
    'pattern=s'  => \$pattern,
    'write_undefined'  => \$write_undefined,
    'path=s'     => \$path,
    'help|?'     => \$help
);
if ( !$result || $help ) {
    pod2usage(1);
}

if((!@files && !$path) ||
   (@files && $path)){
    pod2usage(1);
}

if(!@files){
    # read files according to passed command line arguments

    carp "directory path is $path, looking for $pattern";

    sub loadfiles {
        if (-f) {
            push @files, grep { /$pattern/isxm } $File::Find::name;
        }
        return;
    }
    File::Find::find( \&loadfiles, $path );

    @files = sort { $a cmp $b } @files;

}
carp 'going to process ', scalar @files, ' files:' ,Dumper @files;

foreach my $file (@files) {
    my $yr=$year;
    if(!$year){
        # extract from filename
        if($file =~ /(19\d{2})|(2\d{3})/){
            $yr = $1 || $2;
            #carp "regex saw $1 $2"
        }
    }
    if(!$year && !$yr){
        croak "no year for file $file.  You'll need to pass the year (using --year) and perhaps this file (using --file) on the command line";
    }
    carp "processing $file with year $yr";
    my $obj = CalVAD::WIM::ParseStatusSpreadsheeets->new(
        'write_undefined'=>$write_undefined,
         'past_month'=>0,
         'file'=>$file,
         'year'=>$yr,
        );
    my $data = $obj->data;
    # look at past month as well to catch any quirks
    carp 'got ', scalar @{$data},' rows of data';

    $obj = CalVAD::WIM::ParseStatusSpreadsheeets->new(
        'past_month'=>1,
        'file'=>$file,
        'year'=>$yr,
        );
    my $moredata = $obj->data;

    carp 'got ', scalar @{$moredata},' rows of data in second pass (look at past month)';

    push @{$data}, @{$moredata};
    carp 'got ', scalar @{$data},' rows of data in second pass (look at past month)';

    my $saver = CalVAD::WIM::StoreStatusSpreadsheeets->new
        ('host_psql'=>$host,
         'port_psql'=>$port,
         'dbname_psql'=>$db,
         'username_psql'=>$dbuser,
         'password_psql'=>$dbpass,
         'data'=>$data
        );
    $saver->save_data();
}


1;


__END__


=head1 NAME

    parse_wim_status.pl - parse status spreasheets and write the contents to a database table

=head1 DESCRIPTION

    Bparse_wim_status.pl will read the given input file(s) and save the data to the specified
    db.

=head1 VERSION

    this is the 3rd version

=head1 USAGE

    perl -w parse_wim_status.pl --path /data/wim/raw/data/ -y 2013

=head1 REQUIRED ARGUMENTS

       -path     the root directory below which can be found the target files

       -year     the year (for example, 2007) you want to process.  If left blank, the program will try to guess based on the higher level directories


=head1 OPTIONS


       -help     brief help message
       -username optional, username for the pg database
       -password optional, password for the pg database
       -host     optional, host to use for postgresql
       -db       optional, database to use for postgresql, defaults to spatialvds
       -port     optional, defaults to pg standard

       -write_undefined boolean, defaults to false.  If set, then
           values that are not defined in the spreadsheet will be set
           to "UNDEFINED" in the database.  This isn't a good idea on
           the first pass, because maybe the next month has a valid
           value for that entry?

       and other options I am too lazy to document
