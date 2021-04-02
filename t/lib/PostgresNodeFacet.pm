# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2021: Open PostgreSQL Monitoring Development Group


=pod

=head1 NAME

PostgresNodeFacet - facet class extending PostgresNode for check_pgactivity tests

=head1 DESCRIPTION

This class should not be used directly to create objects. Its only aim is to
extend the existing PostgresNode class, imported from PostgreSQL source code,
without editing it so we can import new versions easily.

See PostgresNode documentation for original methods.

=cut

package PostgresNodeFacet;

use strict;
use warnings;

use Test::More;
use Time::HiRes qw(usleep);
use parent 'PostgresNode';

sub new {
    my $class = shift;
    my $self;

    $self = $class->SUPER::new(@_);

    $self->{test_pghost}    = $PostgresNode::test_pghost;
    $self->{test_localhost} = $PostgresNode::test_localhost;
    $self->{use_tcp}        = $PostgresNode::use_tcp;

    return $self;
}


=pod

=head1 METHODS

Bellow the changes and new methods implemented in this facet.

=over

=item $node->is_default_host()

Return true if the instance host is the default class one.

By default, the class create and access the instance through a unix socket with
a pseudo-randomly chosen port.

This methode return false if the instance has been created with specific
host and port.

=cut

sub is_default_host { return $_[0]->host eq $_[0]->{test_pghost} }

=pod

=item $node->test_localhost()

Getter method returning the C<test_localhost> attribute of the PostgresNode
class.

This attribute is just the localhost address used for tests when sensible.

=cut

sub test_localhost { return $_[0]->{test_localhost} }

=pod

=item $node->use_tcp()

Getter method returning the C<use_tcp> attribute of the PostgresNode class.

Return true if the node is listening on TCP, false if listening on unix
sockets.

=cut

sub use_tcp { return $_[0]->{use_tcp} }

=pod

=item $node->version()

Return the PostgreSQL backend version.

=cut

sub version {
    die "PostgresNodeFacet must not be used directly to create an object"
}

=item $node->switch_wal()

Force WAL rotation.

Return the old segment filename.

=cut

sub switch_wal {
    my $self = shift;

    my $result = $self->safe_psql('postgres',
        'SELECT pg_walfile_name(pg_switch_wal())');

    chomp $result;

    return if $result eq '';
    return $result;
}

=item $node->switch_wal($wal)

Wait for given C<$wal> to be archived.

Timeout is 30s before bailing out.

=cut

sub wait_for_archive {
    my $self         = shift;
    my $wal          = shift;
    my $max_attempts = 300; # 30s
    my $walfile      = $self->archive_dir() ."/$wal";

    print "# waiting for archive $walfile\n";

    while ($max_attempts and not -f $walfile) {
        $max_attempts--;
        # Wait 0.1 second before retrying.
        usleep(100_000);
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
