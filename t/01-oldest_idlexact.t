#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2026: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't/lib';
use pgNode;
use pgSession;
use Test::More tests => 40;

my $node = pgNode->get_new_node('prod');
my $proc;

$node->init;
$node->start;

### Beginning of tests ###

# failing without thresholds
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'oldest_idlexact',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human'
    ],
    127,
    [ qr/^$/ ],
    [ qr/^FATAL: you must specify critical and warning thresholds.$/m ],
    'failing without thresholds'
);

# Tests for PostreSQL 8.2 and before
SKIP: {
    skip "testing incompatibility with PostgreSQL 8.2 and before", 3
        if $node->version >= 8.3;

    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'oldest_idlexact',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
                              '--warning'  => '30s',
                              '--critical' => '1h'
        ],
        1,
        [ qr/^$/ ],
        [ qr/^Service oldest_idlexact is not compatible with host/ ],
        'non compatible PostgreSQL version'
    );
}

SKIP: {
    skip "incompatible tests with PostgreSQL < 8.3", 34 if $node->version < 8.3;

    # basic check
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'oldest_idlexact',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
                              '--warning'  => '30s',
                              '--critical' => '1h'
        ],
        0,
        [ qr/^Service  *: POSTGRES_OLDEST_IDLEXACT$/m,
          qr/^Returns  *: 0 \(OK\)$/m,
          qr/^Message  *: 0 idle transaction\(s\)$/m,
          qr/^Perfdata *: template1 # idle xact=0$/m
        ],
        [ qr/^$/ ],
        'basic check'
    );

    # unit check
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'oldest_idlexact',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
                              '--warning'  => '30m',
                              '--critical' => '1h'
        ],
        0,
        [ qr/^Service  *: POSTGRES_OLDEST_IDLEXACT$/m,
          qr/^Perfdata       : template1 avg=NaNs warn=1800 crit=3600$/m,
        ],
        [ qr/^$/ ],
        'unit check'
    );

    $proc = pgSession->new( $node, 'postgres' );

    $proc->query( 'BEGIN', 1 );
    $proc->query( 'SELECT txid_current()', 1 );

    # OK check
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'oldest_idlexact',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
                              '--dbname'   => 'postgres',
                              '--warning'  => '3s',
                              '--critical' => '1h'
        ],
        0,
        [ qr/^Service  *: POSTGRES_OLDEST_IDLEXACT$/m,
          qr/^Returns  *: 0 \(OK\)$/m,
          qr/^Message  *: 1 idle transaction\(s\)$/m,
          qr/^Perfdata *: postgres # idle xact=1$/m
        ],
        [ qr/^$/ ],
        'OK check'
    );

    # wait for transaction to be idle for more than 3 seconds
    $node->poll_query_until( 'template1', q{
        SELECT current_timestamp - xact_start > interval '3s'
        FROM pg_catalog.pg_stat_activity
        WHERE datname = 'postgres'
          AND xact_start IS NOT NULL
        LIMIT 1
    });

    # warning check
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'oldest_idlexact',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
                              '--dbname'   => 'postgres',
                              '--warning'  => '2s',
                              '--critical' => '1h'
        ],
        1,
        [ qr/^Service  *: POSTGRES_OLDEST_IDLEXACT$/m,
          qr/^Returns  *: 1 \(WARNING\)$/m,
          qr/^Message  *: 1 idle transaction\(s\)$/m,
          qr/^Perfdata *: postgres # idle xact=1$/m
        ],
        [ qr/^$/ ],
        'warning check'
    );

    # critical check
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'oldest_idlexact',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
                              '--dbname'   => 'template1',
                              '--warning'  => '1s',
                              '--critical' => '2s'
        ],
        2,
        [ qr/^Service  *: POSTGRES_OLDEST_IDLEXACT$/m,
          qr/^Returns  *: 2 \(CRITICAL\)$/m,
          qr/^Message  *: 1 idle transaction\(s\)$/m,
          qr/^Perfdata *: postgres # idle xact=1$/m
        ],
        [ qr/^$/ ],
        'critical check'
    );

    SKIP: {
        skip "active xact are not detected before 9.2", 6 if $node->version < 9.2;

        # Emit one query and check that check_pga does not emit a warning or critical
        $proc->query( 'SELECT count(*) FROM pg_class', 1 );

        # active transaction check
        $node->command_checks_all( [
            './check_pgactivity', '--service'  => 'oldest_idlexact',
                                  '--username' => $ENV{'USER'} || 'postgres',
                                  '--format'   => 'human',
                                  '--dbname'   => 'template1',
                                  '--warning'  => '2s',
                                  '--critical' => '1h'
            ],
            0,
            [ qr/^Service  *: POSTGRES_OLDEST_IDLEXACT$/m,
              qr/^Returns  *: 0 \(OK\)$/m,
              qr/^Message  *: 1 idle transaction\(s\)$/m,
              qr/^Perfdata *: postgres # idle xact=1$/m
            ],
            [ qr/^$/ ],
            'active transaction check'
        );
    }
}

### End of tests ###

# stop immediate to kill any remaining backends
$node->stop( 'immediate' );
