# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2020: Open PostgreSQL Monitoring Development Group

package PostgresNode13;

use strict;
use warnings;

use Test::More;
use Time::HiRes qw(usleep);
use parent 'PostgresNode';

sub new
{
    my $class = shift;
    my $self;

    $self = $class->SUPER::new(@_);

    $self->{test_pghost}    = $PostgresNode::test_pghost;
    $self->{test_localhost} = $PostgresNode::test_localhost;
    $self->{use_tcp}        = $PostgresNode::use_tcp;

    return $self;
}

sub switch_wal {
    my $self = shift;

    my $result = $self->safe_psql('postgres',
        'SELECT pg_walfile_name(pg_switch_wal())');

    chomp($result);

    return if $result eq '';
    return $result;
}

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

sub is_default_host { return $_[0]->host eq $_[0]->{test_pghost} }
sub test_localhost  { return $_[0]->{test_localhost} }
sub use_tcp         { return $_[0]->{use_tcp} }
sub version         { return 13 }

1
