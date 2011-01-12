#!/usr/bin/perl -w

use warnings;
use strict;
use Data::Dumper;
use version; our $VERSION = qv('0.0.3');
use English qw(-no_match_vars);
use Carp;

use Getopt::Long;
use Pod::Usage;

use FindBin;
use lib "$FindBin::Bin/lib";
use OSM::NumRoutes;            # to hook into osm.newtempseg schema
use NewCTMLMap::ExtractOut;    # for spatialvds.newctmlmap schema

use IO::File;
use Text::CSV;
use File::Find;

use DateTime;
use DateTime::Format::DateParse;
use DateTime::Format::Pg;

#### This is the part where options are set

my $user        = $ENV{PSQL_USER} || q{};
my $pass        = $ENV{PSQL_PASS} || q{};
my $host        = $ENV{PSQL_HOST} || q{};
my $eventdbname = $ENV{PSQL_DB}   || 'spatialvds';
my $mapdbname   = $ENV{PSQL_DB}   || 'osm';
my $port        = $ENV{PSQL_PORT} || 5432;
my $path;
my $help;
my $cdb_user   = $ENV{COUCHDB_USER} || q{};
my $cdb_pass   = $ENV{COUCHDB_PASS} || q{};
my $cdb_host   = $ENV{COUCHDB_HOST} || '127.0.0.1';
my $cdb_dbname = $ENV{COUCHDB_DB}   || 'versioned_detector_segments';
my $cdb_port   = $ENV{COUCHDB_PORT} || '5984';

my $deletedb;
my $dumpsize = 1000;

my $reparse;

my $result = GetOptions(
    'username:s'  => \$user,
    'password:s'  => \$pass,
    'host:s'      => \$host,
    'db:s'        => \$eventdbname,
    'osmdb:s'     => \$mapdbname,
    'port:i'      => \$port,
    'cusername:s' => \$cdb_user,
    'cpassword:s' => \$cdb_pass,
    'chost:s'     => \$cdb_host,
    'cdb:s'       => \$cdb_dbname,
    'cport:i'     => \$cdb_port,
    'path=s'      => \$path,
    'delete'      => \$deletedb,
    'reparse'     => \$reparse,
    'bulksize=i'  => \$dumpsize,
    'help|?'      => \$help
);

if ( !$result || $help ) {
    pod2usage(1);
}

# logic: query the db for all timestamps in the events table (start
# timestamps) that do not yet have an associated entry in ... hmm, the
# couchdb tracking table? or perhaps join the query with the events_segements table and pick ones without entries?  And then iterate over the timestamps.  For each timestamp, send a request to the stored procedure generates a list of detectors that are active for that timestamp (where ts <= $qtime and endts > $qtime).  With that list in hand, send those points to a function that calls a sql routine that loads up the versioned_detector_segment table.

# access the event db table
my $ctmlmap = 'NewCTMLMap::ExtractOut'->new(

    # first the sql role
    'host_psql'     => $host,
    'port_psql'     => $port,
    'dbname_psql'   => $eventdbname,
    'username_psql' => $user,
    'password_psql' => $pass,

    # now the couchdb role
    'host_couchdb'     => $cdb_host,
    'port_couchdb'     => $cdb_port,
    'dbname_couchdb'   => $cdb_dbname,
    'username_couchdb' => $cdb_user,
    'password_couchdb' => $cdb_pass,
    'create'           => 1,

);
my $tempseg = 'OSM::NumRoutes'->new(

    # first the sql role
    'host_psql'     => $host,
    'port_psql'     => $port,
    'dbname_psql'   => $mapdbname,
    'username_psql' => $user,
    'password_psql' => $pass,

    # now the couchdb role
    'host_couchdb'     => $cdb_host,
    'port_couchdb'     => $cdb_port,
    'dbname_couchdb'   => $cdb_dbname,
    'username_couchdb' => $cdb_user,
    'password_couchdb' => $cdb_pass,
    'create'           => 1,

);

# make sure the tracking CouchDB db has been created

$tempseg->create_db();

# get the timestamps I care about
# my $event_rs      = $ctmlmap->seg_detector_event_rs();
# my $segment_event = $event_rs->next;
# while ($segment_event) {
#    my $ts      = $segment_event->ts;
my $ts = '2008-06-01 08:00:00';
    my $friends = $tempseg->automated_versioned_segment_insert($ts);
    carp "$friends rows inserted for time $ts";
    # $segment_event = $event_rs->next;
    # my $nextts;
    # if ($segment_event) {
    #     $nextts = $segment_event->ts;
    # }
my $nextts= '2008-06-01 09:00:00';
    my $components = $tempseg->automated_versioned_segment_components_insert($ts,$nextts);

    carp
"$components rows inserted into component set for time $ts to time $nextts";
    croak 'die ugly';

# }

1;

__END__


=head1 NAME

    parse_wim_csv_bulkload - write the wim data to postgresql via bulk loading

=head1 VERSION

    this is the 3rd version

=head1 USAGE

    parse_wim_csv_bulkload.pl --path /data/wim/raw/data/ --site 6 -y 2007 -m 12

=head1 REQUIRED ARGUMENTS

       -path     the directory in which the target raw PeMS data files reside


=head1 OPTIONS

       -help     brief help message
       -username optional, username for the pg database
       -password optional, password for the pg database
       -host     optional, host to use for postgres
       -db       optional, database to use for postgres, defaults to spatialvds
       -port     optional, defaults to pg standard

       -cusername  optional,  couchdb user
       -cpassword  optional,  couchdb pass
       -chost      optional,  couchdb host, default localhost
       -cdb        optional,  couchdb dbname, default pemsrawdocs
       -cport      optional,  couchdb port, default couchdb-standard 5984

       and other options I am too lazy to document


=head1 DIAGNOSTICS

=head1 EXIT STATUS

1

=head1 CONFIGURATION AND ENVIRONMENT

   I'm just here for the free cheese.

=head1 DEPENDENCIES

Text::CSV;
IO::File;
IO::Uncompress::Gunzip
DB::CouchDB;
DateTime::Format::DateParse;

and others I am too lazy to document

=head1 INCOMPATIBILITIES

none known

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

James E. Marca, UC Irvine ITS
jmarca@translab.its.uci.edu

=head1 LICENSE AND COPYRIGHT

This program is free software, (c) 2009 James E Marca under the same terms as Perl itself.

=head1 DESCRIPTION

    B<This program> will read the given input file(s) and save the data to the specified
    couchdb as documents (at the moment, one document per file).

=head1 CONFIGURATION
    you need some environment variables set for usernames and passwords for couchdb and psql
