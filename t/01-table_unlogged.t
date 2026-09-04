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
use Time::HiRes qw(usleep gettimeofday tv_interval);
use Test::More tests => 15;

my $node = pgNode->get_new_node('prod');

$node->init;
$node->start;

### Beginning of tests ###

# This service can run without thresholds

# Tests for PostreSQL 9.4 and before
SKIP: {
    skip "testing incompatibility with PostgreSQL 9.4 and before", 3
        if $node->version >= 9.5;

    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'table_unlogged',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
        ],
        1,
        [ qr/^$/ ],
        [ qr/^Service table_unlogged is not compatible with host/ ],
        'non compatible PostgreSQL version'
    );
}

SKIP: {
    skip "incompatible tests with PostgreSQL < 9.5", 34 if $node->version < 9.5;

    # basic check => Returns OK
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'table_unlogged',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
        ],
        0,
        [ qr/^Service  *: POSTGRES_TABLE_UNLOGGED$/m,
          qr/^Returns  *: 0 \(OK\)$/m,
          qr/^Message  *: No unlogged table$/m,
        ],
        [ qr/^$/ ],
        'basic check'
    );

    $node->psql('postgres', 'CREATE DATABASE unlogged;');
    $node->psql('unlogged', 'CREATE UNLOGGED TABLE test1(x text);');

    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'table_unlogged',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
        ],
        2,
        [ qr/^Service  *: POSTGRES_TABLE_UNLOGGED$/m,
          qr/^Returns  *: 2 \(CRITICAL\)$/m,
          qr/^Message  *: 1\/1 table\(s\) unlogged$/m,
          qr/^Long message *: unlogged.public.test1 \(unlogged\);$/m,
          qr/^Perfdata  *: table unlogged in unlogged=1$/m,
        ],
        [ qr/^$/ ],
        'basic check with unlogged table'
    );

}

### End of tests ###

# stop immediate to kill any remaining backends
$node->stop( 'immediate' );

