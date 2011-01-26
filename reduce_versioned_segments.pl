#!/usr/bin/perl -w

use warnings;
use strict;
use Data::Dumper;
use version; our $VERSION = qv('0.0.4');
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
my $help;
my $cdb_user   = $ENV{COUCHDB_USER} || q{};
my $cdb_pass   = $ENV{COUCHDB_PASS} || q{};
my $cdb_host   = $ENV{COUCHDB_HOST} || '127.0.0.1';
my $cdb_dbname = $ENV{COUCHDB_DB}   || 'versioned_detector_segments';
my $cdb_port   = $ENV{COUCHDB_PORT} || '5984';

my $startyear;
my $startmonth = 1;
my $startday   = 1;
my $endyear;
my $event;
my $detector_pattern;

my $result = GetOptions(
    'startyear:i'        => \$startyear,
    'startmonth:i'       => \$startmonth,
    'startday:i'         => \$startday,
    'endyear:i'          => \$endyear,
    'event:s'            => \$event,
    'detector_pattern:s' => \$detector_pattern,
    'username:s'         => \$user,
    'password:s'         => \$pass,
    'host:s'             => \$host,
    'db:s'               => \$eventdbname,
    'osmdb:s'            => \$mapdbname,
    'port:i'             => \$port,
    'cusername:s'        => \$cdb_user,
    'cpassword:s'        => \$cdb_pass,
    'chost:s'            => \$cdb_host,
    'cdb:s'              => \$cdb_dbname,
    'cport:i'            => \$cdb_port,
    'help|?'             => \$help
);

if ( !$result || $help ) {
    pod2usage(1);
}

# logic: query the db for all timestamps in the events table (start
# timestamps) that do not yet have an associated entry in ... hmm, the
# couchdb tracking table? or perhaps join the query with the
# events_segements table and pick ones without entries?  And then
# iterate over the timestamps.  For each timestamp, send a request to
# the stored procedure generates a list of detectors that are active
# for that timestamp (where ts <= $qtime and endts > $qtime).  With
# that list in hand, send those points to a function that calls a sql
# routine that loads up the versioned_detector_segment table.

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

  # make a csv dumper
  my $csv = Text::CSV->new();

my $push_reduced_segment_conditions = sub {
    my ( $storage, $dbh, $data ) = @_;
    $dbh->do('COPY tempseg.reduced_detector_segment_conditions (components,ts,endts,condition) from STDIN with csv' );
    while ( my $line = shift @{$data} ) {
        my @d =  map {$line->{$_}} qw{components ts endts condition};
        $csv->combine( @d );
        $dbh->pg_putcopydata( $csv->string() . "\n" );
    }
    $dbh->pg_putcopyend();
    return;
};


# make sure the tracking CouchDB db has been created

$tempseg->create_db();

# get a hook into the components I need to simplify
my $rs        = $tempseg->detector_segment_conditions_components_rs();
my $component = $rs->next;
while ($component) {
    my $comp    = $component->components;
    my $cond    = $component->conditions;
    croak "$comp, $cond";
    my $friends = $tempseg->fetch_segment_conditions(
        'component' => $comp,
        'condition' => $cond,
    );
    my $row = $friends->next;
    # copy
    my $copy = [ map {$row->$_} qw{components ts endts condition}];
    my $newrecords = [ $copy ];
    while ( $row = $friends->next ) {
        if ( $newrecords->[-1]->endts == $row->ts ) {
            $newrecords->[-1]->endts = $row->endts;
        }
        else {
          $copy = [ map {$row->$_} qw{components ts endts condition}];
            push @{$newrecords}, $copy;
        }
    }
    my $inserts = $tempseg->storage->dbh_do( $push_reduced_segment_conditions,$newrecords);
    carp "$inserts rows inserted into reduced event set for $comp and $cond";
}

1;

__END__


=head1 NAME

    generate_versioned_segments.pl - for each event time, generate complete set of events and segment to detector mappings

=head1 VERSION

    this is the 4th version

=head1 USAGE

    perl -w generate_versioned_segments.pl


=head1 REQUIRED ARGUMENTS

    none.  use environment variables


=head1 OPTIONS

    -startyear        optional, the start year for the analysis, will get dates greater than year-01-01 00:00:00
    -startmonth        optional, the start month.  useful if a program is killed
    -startday          optional, the start day.  useful if a program is killed
    -endyear          optional, the end year for the analysis, will get dates less than (year+1)-01-01 00:00:00
    -event            optional, the event of interest, default specified in OSM::NumRoutes, example 'imputed|observed'
    -detector_pattern optional, the detector pattern,  default specified in OSM::NumRoutes, example 'vds|wim'

       -username optional, username for the pg database
       -password optional, password for the pg database
       -host     optional, host to use for postgres
       -db       optional, database to use for the db containing the newctmlmap schema and tables, defaults to spatialvds
       -osmdb    optional, database to use for the db containing the osm tempseg schema and tables, defaults to osm
       -port     optional, defaults to pg standard

       -cusername  optional,  couchdb user
       -cpassword  optional,  couchdb pass
       -chost      optional,  couchdb host, default localhost
       -cdb        optional,  couchdb dbname, default versioned_detector_segments, for tracking stuff.  Kindof unused right now
       -cport      optional,  couchdb port, default couchdb-standard 5984


    'help|?'             => \$help
       -help     brief help message

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
