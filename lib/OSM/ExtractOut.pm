use strict;
use warnings;
use MooseX::Declare;

class OSM::ExtractOut {

    use version; our $VERSION = qv('0.0.1');

    with 'CouchDB::Trackable';

    # use Testbed::Spatial::VDS::Schema;
    use Testbed::Spatial::VDS::Schema::NewCTMLMap;
    use DateTime::Format::Pg;
    use DateTime;
    # use MooseX::Types::DateTime qw(DateTime);

    use Carp;
    has 'sth_caching' => ('is'=>'ro','isa'=>'Bool','default'=>1);


    has 'couchdb_bulkdocs_limit' =>
      ( 'is' => 'rw', 'isa' => 'Int', 'default' => 100000 );
    has 'sql_page_size' =>
      ( 'is' => 'rw', 'isa' => 'Int', 'default' => 1000000 );
    has '_big_update' =>
      ( 'is' => 'rw', 'isa' => 'ArrayRef', 'lazy_build'=>1,);
    method _build__big_update {
        return [];
    }

    my $param = 'psql';
    method _build__connection_psql {

        # process my passed options for psql attributes
        my ( $host, $port, $dbname, $username, $password ) =
          map { $self->$_ }
          map { join q{_}, $_, $param }
          qw/ host port dbname username password /;
        my $vdb = Testbed::Spatial::VDS::Schema::NewCTMLMap->connect(
            "dbi:Pg:dbname=$dbname;host=$host;port=$port",
            $username, $password, {},
            { 'disable_sth_caching' => $self->sth_caching },
        );
        return $vdb;
    }

    with 'DB::Connection' => {
        'name'                  => 'psql',
        'connection_type'       => 'Testbed::Spatial::VDS::Schema::NewCTMLMap',
        'connection_delegation' => qr/^(.*)/sxm,
    };

# Table "newctmlmap.vds_segment_geometry"
#   Column   |   Type   | Modifiers 
# -----------+----------+-----------
#  vds_id    | integer  | not null
#  adj_pm    | numeric  | 
#  refnum    | integer  | 
#  direction | text     | 
#  seggeom   | geometry | 

    method _vds_seg_geom_rs {
        my $rs;

	$rs = $self->resultset('VdsSegmentGeometry')->search(
							 {  },
							 {
		 '+select' => [ { 'ST_AsGeoJSON' => 'seggeom' }, ],
		 '+as'     => [qw/geojson/],
                }
							);
        return $rs;
    }


      use Data::Dumper;
    method bulk_save {
        carp 'bulk_save ', scalar @{ $self->_big_update() }, ' records';
	# carp 'sample', Dumper $self->_big_update->[0];
        my $rs = $self->bulk_docs( $self->_big_update );
	# carp Dumper $rs;

        $self->_big_update( [] );

        # $self->_clear_big_update();
        return $rs;
    }

    method getnum (Str $str){
	return $str * 1.0;
    }

    method write_vds_to_couch {
       $self->json_shrink(0);
       $self->json_pretty(0);
       $self->handle_blessed(0);
        my $vds_rs = $self->_vds_seg_geom_rs();
        while ( my $vds = $vds_rs->next() ) {
            my $data = { 'proj' => 4326 };
            $data->{'vds_id'}     = $vds->vds_id - 0;
            $data->{'freeway'}   = $vds->refnum - 0;
            $data->{'dir'} = $vds->direction;
            my $geojson = $self->json()->decode( $vds->get_column('geojson') );
	    ## **SOMETIMES** but not always, perw writes text, not numbers 
	    ## so I must touch every number here and subtract zero
	    my $coords = [];
            for my $pair (@{$geojson->{'coordinates'}}){
	      my $lat = $pair->[0] - 0;
	      my $lon = $pair->[1] - 0;
	      push @{$coords}, [ $lat,$lon ];
	    }
	    $geojson->{'coordinates'} = $coords;
            $data->{'geometry'} = $geojson;
            $data->{'type'}     = 'ML';

            push @{ $self->_big_update }, $data;


            # don't let things get out of hand
            if (
                scalar @{ $self->_big_update } > 10 )
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

}

1;    # Magic true value required at end of module

__END__

=head1 NAME

OSM::ExtractOut - [One line description of module's purpose here]


=head1 VERSION

This document describes OSM::ExtractOut version 0.0.1


=head1 SYNOPSIS

    use OSM::ExtractOut;

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
