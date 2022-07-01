# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2023: Open PostgreSQL Monitoring Development Group


=pod

=head1 NAME

pgNode - facet class extending PostgresNode for check_pgactivity tests

=head1 DESCRIPTION

This class should not be used directly to create objects. Its only aim is to
extend the existing PostgresNode class, imported from PostgreSQL source code,
without editing it so we can import new versions easily.

See PostgresNode documentation for original methods.

=cut

package pgNode;

use strict;
use warnings;

use Test::More;
use Time::HiRes qw(usleep);
use Cwd 'cwd';

use Config;

use PostgresNode;

BEGIN {
    # set environment vars
    $ENV{'TESTDIR'}    = cwd;
    delete $ENV{'PAGER'};

    # Look for command 'true'
    # It's under /bin on various Linux, under /usr/bin for macosx
    if ( -x '/bin/true' ) {
        $ENV{'PG_REGRESS'} = '/bin/true';
    }
    else {
        $ENV{'PG_REGRESS'} = '/usr/bin/true';
    }
}

sub new {
    my $class = shift;
    my $self = {};

    $self->{'node'} = PostgresNode->get_new_node(@_);

    bless $self, $class;

    BAIL_OUT( "TAP tests does not support versions older than 8.2" )
        if $self->version < 8.2;

    $ENV{'CHECK_PGA_OLD_PSQL'} = 1 if $self->version < 8.4;

    note('Node "', $self->{'node'}->name, '" uses version: ', $self->version);

    return $self;
}

sub AUTOLOAD {
    our $AUTOLOAD;
    my $subname = $AUTOLOAD;
    my $self    = shift;

    $subname =~ s/^pgNode:://;

    return if $subname eq "DESTROY" and not $self->{'node'}->can("DESTROY");

    return $self->{'node'}->$subname(@_);
}

# Overload wait_for_catchup to pass the PostgresNode object as param
sub wait_for_catchup {
    my $self = shift;
    my $stb = shift;

    $self->{'node'}->wait_for_catchup($stb->{'node'}, @_);
}

=pod

=head1 METHODS

Below the changes and new methods implemented in this facet.

=over

=item $node->version()

Return the PostgreSQL backend version.

=cut

sub version {
    return $_[0]->{'node'}->{_pg_version};
    #die "pgNode must not be used directly to create an object"
}

=item $node->switch_wal()

Force WAL rotation.

Return the old segment filename.

=cut

sub switch_wal {
    my $self = shift;
    my $result;

    if ($self->version >= '10') {
        $result = $self->safe_psql('postgres',
            'SELECT pg_walfile_name(pg_switch_wal())');
    }
    else {
        $result = $self->safe_psql('postgres',
            'SELECT pg_xlogfile_name(pg_switch_xlog())');
    }

    chomp $result;

    return if $result eq '';
    return $result;
}

=item $node->wait_for_archive($wal)

Wait for given C<$wal> to be archived.

Timeout is 30s before bailing out.

=cut

sub wait_for_archive {
    my $self         = shift;
    my $wal          = shift;
    my $sleep_time   = 100_000; # 0.1s
    my $max_attempts = 300; # 300 * 0.1s = 30s
    my $walfile      = $self->archive_dir() ."/$wal";

    print "# waiting for archive $walfile\n";

    while ($max_attempts and not -f $walfile) {
        $max_attempts--;
        usleep($sleep_time);
    }

    if (not -f $walfile) {
        print "# timeout waiting for archive $wal\n";
        print TestLib::slurp_file($self->logfile);
        BAIL_OUT("achiving timeout or failure");
        return 0;
    }

    return 1;
}

=pod

=back

=head1 SEE ALSO

The original L<PostgresNode> class with further methods.

The L<TestLib> class with testing helper functions.


=cut

1;
