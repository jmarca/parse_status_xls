use strict;
use warnings;
use MooseX::Declare;

class PEMS::AggregateToCouchDB {

    use version; our $VERSION = qv('0.0.1');

    with 'CouchDB::Trackable';

    use Holiday::California;
    # use Testbed::Spatial::VDS::Schema;
    use Testbed::Spatial::VDS::Schema::Public;
    use DateTime::Format::Pg;
    use DateTime;
    # use MooseX::Types::DateTime qw(DateTime);
    use Data::Dumper;

    use Carp;
    has 'sth_caching' => ('is'=>'ro','isa'=>'Bool','default'=>1);
    has 'district'=> ( 'is' => 'rw', 'isa' => 'Int', 'default' => 12 );
    has 'query'   => ( 'is' => 'rw', 'isa' => 'HashRef', 'default' => undef );
    has 'table'   => ( 'is' => 'rw', 'isa' => 'Str', 'default' => undef );
    has 'vdsid'   => ( 'is' => 'rw', 'isa' => 'Int', 'default' => undef );
    has 'start_dt' => ('is'=>'rw','isa'=>'DateTime','lazy_build'=>1);
    has 'curr_dt' => ('is'=>'rw','isa'=>'DateTime','lazy_build'=>1);
    has 'end_dt' => ('is'=>'rw','isa'=>'DateTime','lazy_build'=>1);
    has 'duration' => ('is'=>'rw','isa'=>'DateTime::Duration','lazy_build'=>1);
    has 'couchdb_bulkdocs_limit' =>
      ( 'is' => 'rw', 'isa' => 'Int', 'default' => 100000 );
    has 'sql_page_size' =>
      ( 'is' => 'rw', 'isa' => 'Int', 'default' => 1000000 );

    has '_sqla' => ('is'=>'ro','isa'=>'SQL::Abstract','lazy_build'=>1);

    has '_big_update' =>
      ( 'is' => 'rw', 'isa' => 'ArrayRef', 'lazy_build'=>1,);
    has '_weekend_checker' => (
        'is'  => 'ro',
        'isa' => 'Holiday::California',
        'builder'=>'_build__weekend_checker',
        'handles' => [qw( is_holiday_or_weekend )],
    );
    has '_stmt'=>('is'=>'ro','isa'=>'Str','lazy_build'=>1);
    method _build__stmt {
        my $stmt;
        $stmt = <<'FINIS';

select vds_id, date_trunc('hour'::text, ts) as tshour,
        corr(n1,o1) as corr_lane1,
        corr(n2,o2) as corr_lane2,
        corr(n3,o3) as corr_lane3,
        corr(n4,o4) as corr_lane4,
        corr(n5,o5) as corr_lane5,
        corr(n6,o6) as corr_lane6,
        corr(n7,o7) as corr_lane7,
        corr(n8,o8) as corr_lane8
    from pems_raw 
    where vds_id= ? and 
          ((date_trunc('hour'::text, ts) + 
            floor(date_part('minutes'::text, ts) / 5::double precision) 
             * '00:05:00'::interval) between ? and ?)
    group by vds_id,tshour
FINIS

        $stmt =~ s/\s+/ /sxgm;
        return $stmt;
    }

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
      my $dur = DateTime::Duration->new( months => 1 );
      return $dur;
    }
    method _build__big_update {
        return [];
    }
    method _build__weekend_checker {
        return Holiday::California->new();
    }
    method _build__sqla {
        return SQL::Abstract->new();
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

    method extract_out {
        my $records_saved = 0;
        carp 'getting rs for vdsids';
        my $vds_rs = $self->_vds_rs();

        my $sth;
        my $stmt       = $self->_stmt;
        my $storage    = $self->storage();
        my $dbh        = $storage->dbh();
        my $one_second = new DateTime::Duration( 'seconds' => 1 );


       while ( my $vds = $vds_rs->next() ) {
            $self->clear_curr_dt;
            while ( DateTime->compare( $self->curr_dt, $self->end_dt ) < 0 ) {

                my @bind = (
                    $vds->id,
                    DateTime::Format::Pg->format_datetime( $self->curr_dt ),
                    DateTime::Format::Pg->format_datetime(
                        ( $self->curr_dt + $self->duration ) - $one_second
                    )
                );
                if ( !$sth ) {
                    $sth = $dbh->prepare($stmt);
                }
                $self->curr_dt( $self->curr_dt + $self->duration );
                $sth->execute(@bind);
                while ( my $vals = $sth->fetchrow_arrayref ) {
                    $self->inner_loop_method($vals);
                }

                # make sure to save the leftovers
                if ( scalar @{ $self->_big_update() } ) {

                    # there are some results left to save in this page,
                    $self->bulk_save();
                }
            }
        }
	return;

    }

    # let calling code use around or after type modifiers if necessary
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



    method inner_loop_method (ArrayRef $vals) {
        # break up timestamp

        my $dt = DateTime::Format::Pg->parse_datetime( $vals->[1] );

        my $id = join q{_}, @{$vals}[ 0 .. 1 ];
        $id =~ s/\s/_/gsxm;

        # output data
        my $data =[];

        # As data is stored, en passant make the values numbers, not
        # characters, so json does the right thing first the site
        # values
        for my $i (1..8){
          if(defined $vals->[2+$i]){
            # round to 5 decimal points
            push @{$data}, map{sprintf( '%.5f', $_ ) - 0} $vals->[ 2+$i ] ;
          }else{
            last;
          }
        }
        my $dt_info = [
            $dt->hour(),$dt->minute(), $dt->day_abbr(), $dt->day(), $dt->month_abbr(),
            $dt->year(),
        ];
        my $row = {
            '_id'  => $id,
            'vdsid'=> $vals->[0],
            'corr' => $data,
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


    method bulk_save {
        carp 'bulk_save';
        my $rs = $self->bulk_docs( $self->_big_update );
        $self->_big_update( [] );

        # $self->_clear_big_update();
        return $rs;
    }

}

1;    # Magic true value required at end of module

__END__

=head1 NAME

PEMS::AggregateToCouchDB - [One line description of module's purpose here]


=head1 VERSION

This document describes PEMS::AggregateToCouchDB version 0.0.1


=head1 SYNOPSIS

    use PEMS::AggregateToCouchDB;

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
