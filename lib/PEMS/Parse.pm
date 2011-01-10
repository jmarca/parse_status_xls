use strict;
use warnings;
use MooseX::Declare;

class PEMS::Parse {

    use version; our $VERSION = qv('0.0.1');
    use Carp;
    use Data::Dumper;
    use DateTime;
    use File::Path qw(make_path);
    use Testbed::Spatial::VDS::Schema;

    #use FindBin;
    #use lib "$FindBin::Bin/..";
    use MooseX::Types::DateTime qw(DateTime);
    use Utils::Types  qw(CouchdbResult);

    with 'CouchDB::Trackable';

    method _build__connection_psql {

        my $param = 'psql';
        my ( $host, $port, $dbname, $username, $password ) =
          map { $self->$_ }
          map { join q{_}, $_, $param }
          qw/ host port dbname username password /;
        my $vdb = Testbed::Spatial::VDS::Schema->connect(
            "dbi:Pg:dbname=$dbname;host=$host;port=$port",
            $username, $password, {}, { 'disable_sth_caching' => 1 } );
        return $vdb;
    }

    with 'DB::Connection' => {
        'name'                  => 'psql',
        'connection_type'       => 'Testbed::Spatial::VDS::Schema',
        'connection_delegation' => qr/^(.*)/sxm,
    };


    has 'output_dir' => ('is'=>'rw','isa'=>'Str','default'=>'./downloads');
    has 'year' => ('is'=>'rw','isa'=>'Int','required'=>1);
    has 'district'  => ('is'=>'rw','isa'=>'Str','required'=>1);

    has 'inner_loop_method' =>
      ( is => 'ro', 'isa' => 'CodeRef', 'required' => 1 );

    has '_stmt'=>('is'=>'ro','isa'=>'Str','lazy_build'=>1);

    has 'store'=>('is'=>'ro','isa'=>'HashRef','lazy_build'=>1);
    has 'vds_info'=>('is'=>'ro','isa'=>'HashRef','lazy_build'=>1);
    method _build_store {
      my $hashref={};
      return $hashref;
    }
    method _build_vds_info {
      my $hashref={};
      return $hashref;
    }

    method _build__stmt {

      my $stmt = <<'FINIS';
    SELECT v.id, v.name, v.cal_pm, v.abs_pm, v.latitude, v.longitude,
           vv.lanes, vv.segment_length, vv.version, vf.freeway_id,
           vf.freeway_dir, vt.type_id AS vdstype, vd.district_id AS district,
           ST_AsEWKT(g.geom) as geom,
   regexp_replace(v.cal_pm,E'[^[:digit:]^\\.]','','g')::numeric as cal_pm_numeric
    FROM vds_id_all v
    JOIN (
        SELECT vds_versioned.* from vds_versioned join (select id,max(version) as version from vds_versioned group by id )vmax USING (id,version)
    ) vv USING (id)
    JOIN vds_points_4326  ON v.id = vds_points_4326.vds_id
    JOIN vds_vdstype vt USING (vds_id)
    JOIN vds_district vd USING (vds_id)
    JOIN vds_freeway vf USING (vds_id)
    JOIN geom_points_4326 g USING (gid)
FINIS
        $stmt =~ s/\s+/ /sxgm;
        return $stmt;
    }




    method BUILD {

        if ( !-e $self->output_dir ) {
            carp
'Going to create a destination directory ' ,  $self->output_dir;
            make_path( $self->output_dir );
        }
        elsif ( !-d $self->output_dir ) {
            confess 'Need to pass a directory to output_dir.  ',
              $self->output_dir,
              ' exists but is not a directory.';
        }

    }

    method _populate_vdsinfo ( ArrayRef $vals ) {
        $self->vds_info->{ $vals->[0] } = {
            'id'             => $vals->[0],
            'name'           => $vals->[1],
            'cal_pm'         => $vals->[2],
            'abs_pm'         => $vals->[3],
            'latitude'       => $vals->[4],
            'longitude'      => $vals->[5],
            'lanes_fromPeMS' => $vals->[6],
            'segment_length' => $vals->[7],
            'version'        => $vals->[8],
            'freeway_id'     => $vals->[9],
            'freeway_dir'    => $vals->[10],
            'vdstype'        => $vals->[11],
            'district'       => $vals->[12],
            'geom'           => $vals->[13],
            'cal_pm_numeric' => $vals->[14],

        };
        return;
    }

    method _fetch_and_strip (Str $url){

      my $doc = $self->get_doc($url);
      if($doc->err){
        return;
      }
      delete $doc->{'_id'};
      delete $doc->{'_rev'};
      delete $doc->{'row'};
      return $doc;
    }


    method copy_in($fh) {
        $self->clear_store;
        $self->inner_loop_method->( $fh,$self->store );
      }

    method fetch_vds_metadata  ( DateTime :$dt )  {
        $self->clear_vds_info;    # clear the decks first
        my $stmt    = $self->_stmt;
        my $storage = $self->storage();
        my $dbh     = $storage->dbh();

        # my @bind = ( DateTime::Format::Pg->format_date( $dt ), ) ;
        my $sth = $dbh->prepare($stmt);
        # $sth->execute(@bind);
        $sth->execute();
        while ( my $vals = $sth->fetchrow_arrayref ) {
              $self->_populate_vdsinfo($vals);
        }
        return;

    }

    method sanitize_name  (Int $id)  {

        if ( !$self->vds_info->{$id}->{'sanitized_name'} ) {
            my $sanitized_name = $self->vds_info->{$id}->{'name'};

            # change 'n/o' to 'n of', etc
            $sanitized_name =~ s/(n|s|e|w)\/o/$1 of/ixm;

            # change slashes to dashes
            $sanitized_name =~ s/\//-/sxm;

            # strip quotes
            $sanitized_name =~ s/"//sxm;
            # most single quotes are feet references
            $sanitized_name =~ s/' /ft /xm;
            $sanitized_name =~ s/'//sxm;

            # make @ at
            $sanitized_name =~ s/@/ at /xm;

            # make * +
            $sanitized_name =~ s/\*/+/xm;

            # convert to upper case
            $sanitized_name = uc $sanitized_name;

            # # regular use of Mi/MI/M, etc
            # $sanitized_name =~ s/(\dM)I(\s+N|S|E|W)/$1 /xm;

            # make spaces underscores
            $sanitized_name =~ s/\s+/_/gsxm;

            $self->vds_info->{$id}->{'sanitized_name'} = $sanitized_name;
        }
        return $self->vds_info->{$id}->{'sanitized_name'};
    }

    method breakup {
        my $store = $self->store;
        for my $id ( keys %{$store} ) {

            # make path from vds metadata
            # pattern:  district/freeway/direction/name/vdsid_vdstype_year.txt
            my $info           = $self->vds_info->{$id};
            my $sanitized_name = $self->sanitize_name($id);
            my $d =
              $self->district < 10
              ? 'D0' . $self->district
              : 'D' . $self->district;
            my $path = join q{/}, $self->output_dir,
              $d,
              $info->{'freeway_id'}, $info->{'freeway_dir'},
              $sanitized_name;
            if ( !-e $path ) {
                make_path($path);
            }
            my $filename = join q{_}, $id, $info->{'vdstype'}, $self->year;
            $filename .= '.txt';
            my $absname = join q{/}, $path, $filename;

            #open for appending
            my $fh = IO::File->new( $absname, '>>' );
            if ( defined $fh ) {
                for my $line ( @{ $store->{$id} } ) {
                    my $p_res = print {$fh} $line;
                }
            }
        }
        $self->clear_store;    # aaand the next
    }


}


1;

__END__


=head1 NAME

PEMS::DAMatrix - Code that reads and processes PeMS bulk download web pages


=head1 VERSION

This document describes PEMS::DAMatrix version 0.0.1


=head1 SYNOPSIS

    use PEMS::DAMatrix;

    //then later,

    my $tracker = PEMS::DAMatrix->new(
        'host_couchdb'     => $cdb_host,
        'port_couchdb'     => $cdb_port,
        'dbname_couchdb'   => $cdb_dbname,
        'username_couchdb' => $cdb_user,
        'password_couchdb' => $cdb_pass,
        'create'           => 1,
        'start'            => $start,
        'end'              => $end,
        'output_dir'       => $outdir,

    );
    $tracker->create_db();


=head1 DESCRIPTION

This module comes about as a refactor of a really long script.  In
doing the refactor, I also decided to pull in my couchdb tracker
class.  The reasons for doing this are as follows.

First, PeMS made it a conditions of using their site that we limit the
downloads to every ten seconds.  I've put that into the script, but
that also made it unbearably long waiting to parse through the web
pages to get to where I wanted to be.  For example, if I wanted to
extract 2008 data, I'd have to sit around waiting while the script
checked every link from the earliest years through 2008 until it found
files that landed withint my requested time period.

Instead, the new script caches the responses from PeMS, and tries to
decode the web pages a bit more intelligently.  The cache is stored in
a CouchDB database on a page by page basis, with the page URL acting
as the document id.  Before hitting the PeMS site, the script can
first look into the $tracker object for the url in question.  If it is
there, then it can carry on just as if it had actually hit the PeMS
site, thus saving a trip to PeMS and the mandatory 10+ seconds of
sleep.

=head1 SUBROUTINES/METHODS

This module uses the new and possibly unstable but thoroughly
awesome MooseX::Declare.  Therefore most methods have type
qualifiers on the passed parameters.

=head2  parse_data_availability_matrix

     arguments: (Str :$url, Str :$content)

This is the reason why this module is called DAMatrix...Data
Availability Matrix.  This method accepts a string for the url of
the data availability matrix (for storage), and the content from
the query.  It should be called with code like:

    my $datamap =
      $tracker->fetch_data_availability_matrix( 'url' => $dwnldsumm, );
    if ( !$datamap ) {
        my $summresp = $ua->get($dwnldsumm);
        $datamap = $tracker->parse_data_availability_matrix(
            'url'     => $dwnldsumm,
            'content' => $summresp->content,
        );
    }

Notice the initial call to "fetch" the data availability matrix is how
you get around having to download the same data more than once


=head2  fetch_data_availability_matrix

     arguments: (Str :$url)

A call to this method will return an existing data availability matrix
for the passed URL, or nothing.


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
