use strict;
use warnings;
use MooseX::Declare;

class PEMS::ExtractOut {

    use version; our $VERSION = qv('0.0.1');

    with 'CouchDB::Trackable';

    use Holiday::California;
    # use Testbed::Spatial::VDS::Schema;
    use Testbed::Spatial::VDS::Schema::Public;
    use DateTime::Format::Pg;
    use DateTime;
    # use MooseX::Types::DateTime qw(DateTime);

    use Carp;
    has 'sth_caching' => ('is'=>'ro','isa'=>'Bool','default'=>1);
    has 'district'=> ( 'is' => 'rw', 'isa' => 'Int' );
    has 'query'   => ( 'is' => 'rw', 'isa' => 'HashRef', 'default' => undef );
    has 'vdsid'   => ( 'is' => 'rw', 'isa' => 'Int', 'default' => undef );
    has 'start_dt' => ('is'=>'rw','isa'=>'DateTime','lazy_build'=>1);
    has 'curr_dt' => ('is'=>'rw','isa'=>'DateTime','lazy_build'=>1);
    has 'end_dt' => ('is'=>'rw','isa'=>'DateTime','lazy_build'=>1);
    has 'duration' => ('is'=>'rw','isa'=>'DateTime::Duration','lazy_build'=>1);

    method _build_start_dt {
        my $rs = $self->resultset('PemsRaw')->search(
            {},
            {
                select => [ { MIN => 'ts' } ],
                as     => [qw/ min_ts /],
            }
        );
        my $record = $rs->first;
        my $min_ts = $record->get_column('min_ts');
        my $dt     = DateTime::Format::Pg->parse_datetime($min_ts);
        return $dt;
    }

    method _build_curr_dt {
        return $self->start_dt;
    }

    method _build_end_dt {
        my $rs = $self->resultset('PemsRaw')->search(
            {},
            {
                select => [ { MAX => 'ts' } ],
                as     => [qw/ max_ts /],
            }
        );
        my $record = $rs->first;
        my $max_ts = $record->get_column('max_ts');
        my $dt     = DateTime::Format::Pg->parse_datetime($max_ts);
        return $dt;
    }

    method _build_duration {
      my $dur = DateTime::Duration->new( hours => 1 );
      return $dur;
    }

    has 'couchdb_bulkdocs_limit' =>
      ( 'is' => 'rw', 'isa' => 'Int', 'default' => 100000 );
    has 'sql_page_size' =>
      ( 'is' => 'rw', 'isa' => 'Int', 'default' => 1000000 );
    has '_big_update' =>
      ( 'is' => 'rw', 'isa' => 'ArrayRef', 'lazy_build'=>1,);
    method _build__big_update {
        return [];
    }

    has '_weekend_checker' => (
        'is'  => 'ro',
        'isa' => 'Holiday::California',
        'builder'=>'_build__weekend_checker',
        'handles' => [qw( is_holiday_or_weekend )],
    );
    method _build__weekend_checker {
        return Holiday::California->new();
    }

    my $param = 'psql';
    method _build__connection_psql {

        # process my passed options for psql attributes
        my ( $host, $port, $dbname, $username, $password ) =
          map { $self->$_ }
          map { join q{_}, $_, $param }
          qw/ host port dbname username password /;
        my $vdb = Testbed::Spatial::VDS::Schema::Public->connect(
            "dbi:Pg:dbname=$dbname;host=$host;port=$port",
            $username, $password, {},
            { 'disable_sth_caching' => $self->sth_caching },
        );
        return $vdb;
    }

    with 'DB::Connection' => {
        'name'                  => 'psql',
        'connection_type'       => 'Testbed::Spatial::VDS::Schema::Public',
        'connection_delegation' => qr/^(.*)/sxm,
    };

    method write_vds_to_couch {
        my $vds_rs = $self->_vds_current_view_rs();
        while ( my $vds = $vds_rs->next() ) {
            my $data = { 'proj' => 4326 };
            $data->{'id'}     = $vds->id - 0;
            $data->{'name'}   = $vds->name;
            $data->{'cal_pm'} = $vds->cal_pm;
            $data->{'abs_pm'} = $vds->abs_pm - 0;
            my $geojson = $self->json()->decode( $vds->get_column('geojson') );
            $geojson->{'coordinates'} = [
                $geojson->{'coordinates'}->[0] - 0,
                $geojson->{'coordinates'}->[1] - 0
            ];
            $data->{'geometry'} = $geojson;
            $data->{'lat'}      = $vds->latitude - 0;
            $data->{'lon'}      = $vds->longitude - 0;
            $data->{'lanes'}    = $vds->lanes - 0;

            if ( defined $vds->segment_length ) {
                $data->{'segment_length'} = $vds->segment_length - 0;
            }

            my $dt = DateTime::Format::Pg->parse_datetime( $vds->version );
            $data->{'version'} = $dt->ymd;

            $data->{'freeway'}  = $vds->freeway_id;
            $data->{'dir'}      = $vds->freeway_dir;
            $data->{'type'}     = $vds->vdstype;
            $data->{'district'} = $vds->district - 0;

            #$data->{'geom'}           = $vds->geom;
            $data->{'cal_pm_numeric'} = $vds->cal_pm_numeric - 0;

            push @{ $self->_big_update }, $data;

            # don't let things get out of hand
            if (
                scalar @{ $self->_big_update } > $self->couchdb_bulkdocs_limit )
            {
                $self->bulk_save();
            }

        }

        # make sure to save the leftovers
        if ( scalar @{ $self->_big_update() } ) {

            # there are some results left to save in this page,
            $self->bulk_save();
        }
        return;

    }

    method extract_out {
        my $records_saved = 0;
        carp 'getting rs for vdsids';
        my $vds_rs  = $self->_vds_rs();
        my $data_rs = $self->rs_query();
        while ( my $vds = $vds_rs->next() ) {
            $self->clear_curr_dt;
            while ( DateTime->compare( $self->curr_dt, $self->end_dt ) < 0 ) {
                my $query = {
                    'vds_id'  => $vds->id,
                    'fivemin' => {
                        -between => [
                            DateTime::Format::Pg->format_datetime(
                                $self->curr_dt
                            ),
                            DateTime::Format::Pg->format_datetime(
                                $self->curr_dt + $self->duration
                            ),

                        ]
                    },
                };
                $self->curr_dt( $self->curr_dt + $self->duration );
                my $rs         = $data_rs->search($query);
                my $cursor = $rs->cursor;
                while ( my @vals = $cursor->next ) {
                    $self->inner_loop_method( [@vals] );
                }
            }
        }
        # make sure to save the leftovers
        if ( scalar @{ $self->_big_update() } ) {

            # there are some results left to save in this page,
            $self->bulk_save();
        }
        return;
    }

    # let calling code use around or after type modifiers if necessary


    method inner_loop_method (ArrayRef $vals) {
        if ( !defined $vals->[3] || !defined $vals->[4] ) {
            return;
        }

# key to array: select => [qw{vds_id fivemin intervals nsum oave nlanes olanes}]

        # break up fivemin timestamp

        my $dt = DateTime::Format::Pg->parse_datetime( $vals->[1] );

        # make a hash of the data, push

        my $id = join q{_}, @{$vals}[ 0 .. 1 ];
        $id =~ s/\s/_/gsxm;

        # output data consists of site values (combining all lanes) and lane values
        my $data =[];

        # As data is stored, en passant make the values numbers, not
        # characters, so json does the right thing first the site
        # values
        push @{$data}, map{$_ - 0} $vals->[ 2 ] ;
        # for occupancy, round to 5 decimal points
        push @{$data}, map{sprintf( '%.5f', $_ ) - 0} $vals->[ 3 ] ;
        # the number of intervals used in the calculation
        push @{$data}, map{$_ - 0} $vals->[ 4 ] ;

        # the number of lanes at the site
        push @{$data}, scalar @{$vals->[ 5 ]} ;

        # then the lane values append _unrolled_ nlanes, olanes (makes
        # for faster map/reduce, I think.
        push @{$data},map{$_ - 0} @{$vals->[5]};
        # for occupancy, round to 5 decimal points
        push @{$data}, map{sprintf( '%.5f', $_ ) - 0} @{$vals->[6]};

        # append some ones for the "count" variables, again, to speed up map/reduce
        #foreach ( @{ $vals->[6] } ) {
        #    push @{$data}, 1 - 0;
        #}
        my $dt_info = [
            $dt->hour(),$dt->minute(), $dt->day_abbr(), $dt->day(), $dt->month_abbr(),
            $dt->year(),
        ];
        my $row = {
            '_id'  => $id,
            'vdsid'=>$vals->[0],
            '5min' => $data,
            'dt'   => $dt_info,
        };
        if ( $self->is_holiday_or_weekend($dt) ) {
            $row->{'we_h'} = 1;
        }

        # croak Dumper $row;
        push @{ $self->_big_update }, $row;

        # don't let things get out of hand
        if ( scalar @{ $self->_big_update } > $self->couchdb_bulkdocs_limit ){
            $self->bulk_save();
        }

        return;
    }

    method _vds_rs {
        my $rs;
        if ( $self->vdsid ) {
            $rs =
              $self->resultset('Vds')->search( { vds_id => $self->vdsid, } );
        }
        else {
            $rs = $self->resultset('Vds')->search(
                {
                    id => {
                        -between => [
                            $self->district * 100000, ( $self->district + 1 ) * 100000 - 1
                        ]
                    }
                }
            );
        }
        return $rs;
    }

    method _vds_current_view_rs {
        my $rs;
        if ( $self->vdsid ) {
            $rs = $self->resultset('VdsCurrentView')->search(
                { vds_id => $self->vdsid, },
                {
                    '+select' => [ { 'ST_AsGeoJSON' => 'geom' }, ],
                    '+as'     => [qw/geojson/],
                }
            );
        }
        else {
	  if($self->district){
            $rs = $self->resultset('VdsCurrentView')->search(
                { district => $self->district, },
                {
                    '+select' => [ { 'ST_AsGeoJSON' => 'geom' }, ],
                    '+as'     => [qw/geojson/],
                }

            );
	  }else{
	    $rs = $self->resultset('VdsCurrentView')->search(
                {  },
                {
                    '+select' => [ { 'ST_AsGeoJSON' => 'geom' }, ],
                    '+as'     => [qw/geojson/],
                }

            );
	  }
        }
        return $rs;
    }

    method rs_query {

        my $rs = $self->resultset('PemsRaw5minuteAggregatesFour')->search(
            {},
            {
                'select' =>
                  [qw{vds_id fivemin intervals nsum oave nlanes olanes}],
            }
        );

        return $rs;
    }

      use Data::Dumper;
    method bulk_save {
        carp 'bulk_save ', scalar @{ $self->_big_update() }, ' records';
	carp 'sample', Dumper $self->_big_update->[0];
        my $rs = $self->bulk_docs( $self->_big_update );
	carp Dumper $rs;

        $self->_big_update( [] );

        # $self->_clear_big_update();
        return $rs;
    }

}

1;    # Magic true value required at end of module

__END__

=head1 NAME

PEMS::ExtractOut - [One line description of module's purpose here]


=head1 VERSION

This document describes PEMS::ExtractOut version 0.0.1


=head1 SYNOPSIS

    use PEMS::ExtractOut;

=head1 DESCRIPTION


=head1 SUBROUTINES/METHODS

=head2  extract_out

move data defined by the rs_query from sql storage to the couchdb database

=head2  rs_query

set the query to send to the sql db

=head2 bulk_save

save everything so far to couchdb


=head1 DIAGNOSTICS


=head1 CONFIGURATION AND ENVIRONMENT

Spatialvds::CopyIn requires no configuration files or environment variables.


=head1 DEPENDENCIES

None.

=head1 INCOMPATIBILITIES

None reported.

=head1 BUGS AND LIMITATIONS

No bugs have been reported.

Please report any bugs or feature requests to
C<bug-spatialvds-copyin@rt.cpan.org>, or through the web interface at
L<http://rt.cpan.org>.  Or email them to me, as it is likely this will
never get posted to CPAN.


=head1 AUTHOR

James E. Marca  C<< <jmarca@translab.its.uci.edu> >>


=head1 LICENSE AND COPYRIGHT

Copyright (c) 2009, James E. Marca C<< <jmarca@translab.its.uci.edu> >>. All rights reserved.

This module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself. See L<perlartistic>.


=head1 DISCLAIMER OF WARRANTY

BECAUSE THIS SOFTWARE IS LICENSED FREE OF CHARGE, THERE IS NO WARRANTY
FOR THE SOFTWARE, TO THE EXTENT PERMITTED BY APPLICABLE LAW. EXCEPT WHEN
OTHERWISE STATED IN WRITING THE COPYRIGHT HOLDERS AND/OR OTHER PARTIES
PROVIDE THE SOFTWARE "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER
EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THE SOFTWARE IS WITH
YOU. SHOULD THE SOFTWARE PROVE DEFECTIVE, YOU ASSUME THE COST OF ALL
NECESSARY SERVICING, REPAIR, OR CORRECTION.

IN NO EVENT UNLESS REQUIRED BY APPLICABLE LAW OR AGREED TO IN WRITING
WILL ANY COPYRIGHT HOLDER, OR ANY OTHER PARTY WHO MAY MODIFY AND/OR
REDISTRIBUTE THE SOFTWARE AS PERMITTED BY THE ABOVE LICENCE, BE
LIABLE TO YOU FOR DAMAGES, INCLUDING ANY GENERAL, SPECIAL, INCIDENTAL,
OR CONSEQUENTIAL DAMAGES ARISING OUT OF THE USE OR INABILITY TO USE
THE SOFTWARE (INCLUDING BUT NOT LIMITED TO LOSS OF DATA OR DATA BEING
RENDERED INACCURATE OR LOSSES SUSTAINED BY YOU OR THIRD PARTIES OR A
FAILURE OF THE SOFTWARE TO OPERATE WITH ANY OTHER SOFTWARE), EVEN IF
SUCH HOLDER OR OTHER PARTY HAS BEEN ADVISED OF THE POSSIBILITY OF
SUCH DAMAGES.
