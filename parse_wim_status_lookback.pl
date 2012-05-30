#!/usr/bin/perl -w

use warnings;
use strict;
use Data::Dumper;
use version; our $VERSION = qv('0.0.2');
use English qw(-no_match_vars);
use Carp;

use Spreadsheet::Read;

use Getopt::Long;
use DateTime::Format::DateParse;
use DateTime::Format::Pg;
use Testbed::Spatial::VDS::Schema::Public;

my @files = ();
my $db    = 'spatialvds';
my $host  = 'metis.its.uci.edu';
my $dbuser;
my $dbpass;
my $year   = 2007;
my $result = GetOptions(
    'files:s'    => \@files,
    'year:i'     => \$year,
    'database:s' => \$db,
    'user=s'     => \$dbuser,
    'pass=s'     => \$dbpass,
);

if (@files) {
    my $temp_string = join q{,}, @files;

    @files = split /,/sxm, $temp_string;
}
if ( !@files ) {
    croak
'need to have files passed on the command line with option --files="somefile.xls,anotherfile.xls" --files="yetanother.xls"';
}

my $vdb =
  Testbed::Spatial::VDS::Schema::Public->connect( "dbi:Pg:dbname=$db;host=$host",
    $dbuser, $dbpass );

my @bulk;

sub checkbulk {
    my $args    = shift;
    my $datarow = $vdb->resultset('WimStatus')->find($args);
    if ($datarow) {
      carp 'already have entry for ', Dumper $args;
        return 0;
    }
    else {
        return 1;
    }
}
my @created_codes = ();

sub check_status_code {
    my $code    = shift;
    my $datarow = $vdb->resultset('WimStatusCodes')->find($code);
    if ( !$datarow ) {

        # need to enter status code into table
        $vdb->resultset('WimStatusCodes')->create( { 'status' => $code } );
        push @created_codes, $code;
    }
    return;
}

sub bulksave {

    # take the arguments, create 24 entries in the bulk save list
    my $args = shift;
    my $ts   = $args->{'ts'};
    if (
        checkbulk(
            {
                'site_no' => $args->{'site_no'},
                'ts'      => $ts,
            }
        )
      )
    {
        check_status_code( $args->{'class_status'} );
        check_status_code( $args->{'weight_status'} );

        push @bulk,
          {
            'site_no'       => $args->{'site_no'},
            'ts'            => $ts,
            'class_status'  => $args->{'class_status'},
            'class_notes'   => $args->{'class_notes'},
            'weight_status' => $args->{'weight_status'},
            'weight_notes'  => $args->{'weight_notes'},
          };
    }
    return scalar @bulk;
}

# parse all files in the list of files to parse.

# this version will parse the monthly IRD and PAT xls files

foreach my $file (@files) {
    carp "processing $file";
    my $ref = ReadData($file);

# row 1 is headers.  Parse from Row 2 onwards.  Bail when no more data in col 1, row n
    my $month = $ref->[1]->{'D1'};
    my $ts    = DateTime::Format::DateParse->parse_datetime("$month 1, $year");
    my $row   = 2;
    # this version looks at the prior month
    while ( $ref->[1]->{"A$row"} ) {
        my $site          = $ref->[1]->{"A$row"};
        my $class_status  = $ref->[1]->{"D$row"};
        my $class_notes   = $ref->[1]->{"F$row"};
        my $weight_status = $ref->[1]->{"G$row"};
        my $weight_notes  = $ref->[1]->{"I$row"};
	carp "( $site, $class_status, $class_notes, $weight_status, $weight_notes )";
        foreach ( $site, $class_status, $class_notes, $weight_status,
            $weight_notes )
        {
            s/^\s+//sxm;
            s/\s+$//sxm;
        }
        if ( !$class_status || !$weight_status ) {

            # possible mistake
            if (   ( $class_status || $weight_status )
                || ( $class_notes || $weight_notes ) )
            {

                # um, oops!
                carp Dumper {
                    'site_no'       => $site,
                    'ts'            => DateTime::Format::Pg->format_date($ts),
                    'class_status'  => $class_status,
                    'class_notes'   => $class_notes,
                    'weight_status' => $weight_status,
                    'weight_notes'  => $weight_notes,
                };
                croak 'inconsistent data';
            }

            # otherwise, nothing to see here.  move along
	    carp 'nada';
            next;
        }

    # 	carp Dumper {
    #                 'site_no'       => $site,
    #                 'ts'            => DateTime::Format::Pg->format_date($ts),
    #                 'class_status'  => $class_status,
    #                 'class_notes'    => $class_notes,
    #                 'weight_status' => $weight_status,
    #                 'weight_notes'   => $weight_notes,
    #             };
        bulksave(
            {
                'site_no'       => $site,
                'ts'            => DateTime::Format::Pg->format_date($ts),
                'class_status'  => $class_status,
                'class_notes'   => $class_notes,
                'weight_status' => $weight_status,
                'weight_notes'  => $weight_notes,
            }
        );
    }
    continue {

        # increment the row
        $row++;
    }

    #done parsing this report
    if (@bulk) {
        my $output = $vdb->resultset('WimStatus')->populate( \@bulk );
        @bulk = ();
    }

    # look at the next file
}

#all done.  spit any status files that were created
carp 'created status fields in db', Dumper( \@created_codes );

#buh-bye
1;

__END__

