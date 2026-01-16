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
use Test::More tests => 17;

my $node = pgNode->get_new_node('prod');
my @timer;
my @in;
my @out;
my @procs;

$node->init;
$node->append_conf('postgresql.conf', 'max_connections=8');
$node->start;

### Beginning of tests ###

# failing without thresholds
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'longest_query',
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
    './check_pgactivity', '--service'  => 'longest_query',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '4',
                          '--critical' => '5'
    ],
    0,
    [ qr/^Service  *: POSTGRES_LONGEST_QUERY$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: 1 running querie\(s\)$/m,
      qr/^Perfdata *: template1 #queries=1$/m,
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

push @procs, pgSession->new($node, 'testdb');

$procs[0]->query('select pg_sleep(60)', 60);

# wait for backend to be connected and active
$node->poll_query_until('template1', q{
    SELECT (query_start + interval '10 seconds') < now()
    FROM pg_catalog.pg_stat_activity
    WHERE datname = 'testdb'
    LIMIT 1
});

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'longest_query',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '4s',
                          '--critical' => '20s'
    ],
    1,
    [ qr/^Service  *: POSTGRES_LONGEST_QUERY$/m,
      qr/^Returns  *: 1 \(WARNING\)$/m,
      qr/^Message  *: testdb: 10s$/m,
      qr/^Perfdata *: testdb max=10[.0-9]*s warn=4 crit=20$/m,
      qr/^Perfdata *: testdb avg=10s warn=4 crit=20$/m,
      qr/^Perfdata *: testdb #queries=1$/m,
    ],
    [ qr/^$/ ],
    '1 warning'
);

### End of tests ###

# stop immediate to kill any remaining backends
$node->stop('immediate');
