#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2022: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't/lib';
use pgNode;
use Test::More;

my $node      = pgNode->get_new_node('prod');
my $num_tests = 12;
my $wal;

# we have $num_tests normal tests + three tests for incompatible pg versions
plan tests => $num_tests + 3;

### Beginning of tests ###

$node->init;
    
# Tests for PostreSQL 8.0 and before
SKIP: {
    skip "testing incompatibility with PostgreSQL 8.0 and before", 3
        unless $node->version <= 8.0;

    $node->start;
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'autovacuum',
                              '--username' => getlogin,
                              '--format'   => 'human'
        ],
        1,
        [ qr/^$/ ],
        [ qr/^Service autovacuum is not compatible with host/ ],
        'non compatible PostgreSQL version'
    );
}

# Tests for PostreSQL 8.1 and after
SKIP: {
    my @stdout;
    skip "these tests requires PostgreSQL 8.1 and after", $num_tests
        unless $node->version >= 8.1;

    if ($node->version < 8.3) {
        $node->append_conf('postgresql.conf',
             qq{autovacuum = on\n}
            .qq{stats_row_level = on}
        );
    }

    $node->start;

    @stdout = (
        qr/^Service  *: POSTGRES_AUTOVACUUM$/m,
        qr/^Returns  *: 0 \(OK\)$/m,
        qr/^Message  *: Number of autovacuum: [0-3]$/m,
        qr/^Perfdata *: VACUUM_FREEZE=[0-3]$/m,
        qr/^Perfdata *: VACUUM_ANALYZE=[0-3]$/m,
        qr/^Perfdata *: VACUUM=[0-3]$/m,
        qr/^Perfdata *: ANALYZE=[0-3]$/m,
        qr/^Perfdata *: oldest_autovacuum=(NaN|\d+)s$/m,
    );

    SKIP: {
        skip "No max_worker before PgSQL 8.3", 1
            if $node->version < 8.3;
        push @stdout, qr/^Perfdata *: max_workers=3$/m;
    }

    SKIP: {
        skip "No autovacuum brin summarize before PgSQL 10", 1
            if $node->version < 10;
        push @stdout, qr/^Perfdata *: BRIN_SUMMARIZE=[0-3]$/m;
    }

    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'autovacuum',
                              '--username' => getlogin,
                              '--format'   => 'human'
        ],
        0,
        \@stdout,
        [ qr/^$/ ],
        'basic check without thresholds'
    );

    $node->stop;
}

### End of tests ###
