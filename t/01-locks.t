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
use TestLib ();
use IPC::Run ();
use Test::More tests => 13;

my $node = pgNode->get_new_node('prod');
my @timer;
my @in;
my @out;
my @procs;

$node->init;
$node->start;

### Beginning of tests ###

# failing without thresholds
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'locks',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human'
    ],
    127,
    [ qr/^$/ ],
    [ qr/^FATAL: you must specify critical and warning thresholds.$/m ],
    'failing without thresholds'
);

# basic check
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'locks',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '4',
                          '--critical' => '5'
    ],
    0,
    [ qr/^Service  *: POSTGRES_LOCKS$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: . locks, . predicate locks, 0 waiting locks$/m,
    ],
    [ qr/^$/ ],
    'basic check'
);

# two sessions on two different db

TestLib::system_or_bail('createdb',
    '--host' => $node->host,
    '--port' => $node->port,
    'testdb'
);

push @procs, pgSession->new($node, 'testdb') for 1..3;

$procs[0]->query('CREATE TABLE tstlocks (i integer primary key)', 0);
$procs[0]->query('BEGIN', 0);
$procs[1]->query('BEGIN', 0);

$procs[0]->query('INSERT INTO tstlocks (i) VALUES (1)', 0);
$procs[1]->query('INSERT INTO tstlocks (i) VALUES (1)', 0);

# wait for backend to be connected and active
$node->poll_query_until('template1', q{
    SELECT query_start IS NOT NULL -- < now()
    FROM pg_catalog.pg_stat_activity
    WHERE datname = 'testdb'
    LIMIT 1
});

# Triggers a WARNING
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'locks',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '1',
                          '--critical' => '100'
    ],
    1,
    [ qr/^Service  *: POSTGRES_LOCKS$/m,
      qr/^Returns  *: 1 \(WARNING\)$/m,
      qr/^Message  *: .+ locks, . predicate locks, 1 waiting locks$/m,
    ],
    [ qr/^$/ ],
    'check for locks with warning'
);

### End of tests ###

# stop immediate to kill any remaining backends
$node->stop('immediate');
