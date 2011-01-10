use strict;
use warnings;
use MooseX::Declare;

class PEMS::DAMatrix {

    use version; our $VERSION = qv('0.0.1');
    use Carp;
    use Data::Dumper;
    use DateTime;
    use HTTP::Response;
    use HTTP::Request;
    use MooseX::Types::HTTP  qw(HttpResponse);
    use File::Path qw(make_path);

    # use FindBin;
    # use lib "$FindBin::Bin/..";
    use Utils::Types  qw(CouchdbResult);

    with 'CouchDB::Trackable';

    has 'start' => ('is'=>'rw','isa'=>'DateTime','lazy_build'=>1);
    has 'end' => ('is'=>'rw','isa'=>'DateTime','lazy_build'=>1);
    has 'output_dir' => ('is'=>'rw','isa'=>'Str','default'=>'./downloads');

    has '_month_hack'=>('is'=>'ro','isa'=>'HashRef','lazy_build'=>1);

    method _build__month_hack {
        my $ref =  {
            'Jan' => 1, 'Feb' => 2,  'Mar' => 3,  'Apr' => 4,
            'May' => 5, 'Jun' => 6,  'Jul' => 7,  'Aug' => 8,
            'Sep' => 9, 'Oct' => 10, 'Nov' => 11, 'Dec' => 12,
        };
        return $ref;
    }

    method BUILD {

        if ( !$self->output_dir eq './downloads' ) {
            carp
'No output_dir specified.  Going to create a \'downloads\' directory in the current directory';
        }
        if ( !-e $self->output_dir ) {
            make_path( $self->output_dir );
        }
        elsif ( !-d $self->output_dir ) {
            confess 'Need to pass a directory to output_dir.  ',
              $self->output_dir,
              ' exists but is not a directory.';
        }

    }
# old comment from Craig describing what he did here, and why
# summresp should now have a bunch of onClicks that have calls to
# bts_load_element(), where the first parameter gets shoved in the
# draw field below:
#
# http://pems.eecs.berkeley.edu/?dnode=State&content=dbx&tab=dbx_download&draw=397973&element=detail
#
# I would have liked to use XML::Twig to get it, but PeMS sends
# poorly formated HTML
#
# Regular expressions are probably faster anyway...

    method parse_data_availability_matrix (Str :$url, Str :$content) {
        my @rows = split /\n/sxm, $content;
        # find and parse the header row
        my $row;
        $row = shift @rows;
        while ( scalar @rows && $row !~ /Data\s+Availability\s+Matrix/sxm ) {
            $row = shift @rows;

        }

        # header of table
        $row = shift @rows;
        my @months;
        if ($row) {
            @months =
              map { /th.*?&quot;&gt;([a-zA-Z_]{3})&lt;\/th&gt;/gsxm } $row;
        }

        # next row is everything.  try to slot stuff away by months and years
        $row = shift @rows;

        # split that row into lines of table rows
        my @lines;
        if ($row) {
            @lines = map { /&lt;tr&gt;(.*?)&lt;\/tr&gt;/gsxm } $row;
        }
        my $bighash = {};
        for my $line (@lines) {

            # the th element is the year
            my ($year) = map { /&lt;th&gt;(.*?)&lt;\/th&gt;/sxm } $line;

            # now grab the params on this line, match up with the months;
            my @param_elements =
              map { /&lt;td.*?&gt;(.*?)&lt;\/td&gt;/gsxm } $line;
            my $month_index = 0;
            $bighash->{$year} = {};
            for my $param_element (@param_elements) {
                if ( $param_element =~ /bts_load_element\((.*?),/sxm ) {
                    $bighash->{$year}
                      ->{ $self->_month_hack->{ $months[$month_index] } } = $1;
                }
                $month_index++;
            }
            if ( !keys %{ $bighash->{$year} } ) {
                delete $bighash->{$year};
            }
        }
        $self->track( 'id' => $url, 'otherdata' => $bighash );
        return $bighash;
    }

  method parse_datafiles_response (Str :$content, Str :$url){

      my $content_rows = [split /\n/sxm, $content];

      my $days = $self->filter_datainfo('content'=>$content_rows);
      $self->track( 'id' => $self->_datafiles_url($url), 'otherdata' => $days );

      my $vdsfiles = $self->filter_stationinfo('content'=>$content_rows);
      $self->track( 'id' => $self->_stationfiles_url($url), 'otherdata' => {'vdsfiles'=>$vdsfiles} );

      return $days;
  }

  method get_datafiles_between_dates ( HashRef|CouchdbResult :$days ) {

    # grep out files that violate the date criteria
    my $files = {};
    foreach my $fkey ( keys %{$days} ) {
        my $val = $days->{$fkey};
        if (   ( $val =~ /.*?_(\d\d\d\d)_(\d\d).txt.gz/sxm )
            || ( $val =~ /.*?_(\d\d\d\d)_(\d\d)_(\d\d).txt.gz/sxm )
            || ( $val =~ /.*?_(\d\d\d\d)_(\d\d).VOL.gz/sxm )
            || ( $val =~ /.*?_(\d\d\d\d)_(\d\d).STA.gz/sxm ) )
        {
            my ( $y, $m, $d );
            if ( !$3 ) {
                $d = 1;
                $y = $1;
                $m = $2;
            }
            else {
                $y = $1;
                $m = $2;
                $d = $3;
            }
            my $dt = DateTime->new( 'year' => $y, 'month' => $m, 'day' => $d );
            if (   ( ( not defined $self->start ) || $dt >= $self->start )
                && ( ( not defined $self->end ) || $dt <= $self->end ) )
            {
                $files->{$fkey} = $val;

                # use val for the filename
            }
        }
        else {
            croak "$val failed to pass regex";
        }
    }
    return $files;
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


    method _datafiles_url($url){
      return $url . '_data';
    }
    method _stationfiles_url($url){
      return $url . '_vds';
    }

    method fetch_data_availability_matrix (Str :$url){
      return $self->_fetch_and_strip($url);
    }

    method fetch_datafiles (Str :$url){

      return $self->_fetch_and_strip($self->_datafiles_url($url));
    }

    method fetch_stationfiles (Str :$url) {
      my $doc = $self->_fetch_and_strip($self->_stationfiles_url($url));
      return $doc->{'vdsfiles'};
    }

    method have_data_file (Str :$url){

      return $self->_fetch_and_strip($self->_datafiles_url($url));
    }

    method have_station_file ( Str :$url ){

      return $self->_fetch_and_strip($self->_stationfiles_url($url));
    }

    method track_data_file (Str :$url, HashRef :$content){

      return $self->track('id'=>$self->_datafiles_url($url),
                         'otherdata'=>$content);
    }

    method track_station_file ( Str :$url, HashRef  :$content ){

      return $self->track('id'=>$self->_stationfiles_url($url),
                         'otherdata'=>$content);

    }

    method dump_response (Str :$url, HttpResponse :$response){
        my $filename;
        if ( $response->header('content-disposition') && $response->header('content-disposition') =~ /filename=(.+txt)/sxm )
        {
            $filename = $1;
        }
        if ( !defined $filename ) {
            # instead try the last bit of the url
          if($url =~ /(.*\/)?(.*)\.?(.*)$/sxm){
            if($3){
              $filename= join q{.},$2,$3;
            }else{
              $filename=$2;
            }
          }
        }

        # only write if not yet written
        my $target = join q{/}, $self->output_dir, $filename;

        my $information = {
            'localfile'      => $target,
            'content_length' => $response->content_length,
            'uri'            => $response->request->uri->as_string,
        };

        if ( not( -e $target && -s $target ) ) {

            # file does not exist or else is empty
            my $fh = IO::File->new("> $target");
            if ( defined $fh ) {
                my $p_res = print {$fh} $response->content;
                if ( !$p_res ) {
                    croak "couldn't print out content to $target";
                }
                $fh->close;
            }
            else {
                croak "FAILED!!!!  couldn't open filehandle on $target";
            }

        }
        # track that for future reference
        my $doc = $self->track_station_file(
            'url'     => $url,
            'content' => $information,
        );

        return;
    }


    method filter_datainfo  (ArrayRef :$content) {

        # coerce array into hash: param keys datafile name
        my $days =
          { map { /download=(.*?)&.*?(d\d\d_.*?\....\.gz)/gsxm }
              @{$content} };
        return $days;
    }

    method filter_stationinfo (ArrayRef :$content) {

        # coerce array into another hash for vds descriptions
        my %vdsdefs =
          map { /TMDD.*?download=(.*?)&.*?(Text)/igsxm } @{$content};
        my @vdsfiles = keys %vdsdefs;
        return [@vdsfiles];
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
