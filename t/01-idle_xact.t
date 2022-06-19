#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2022: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't/lib';
use pgNode;
use pgSession;
use TestLib ();
use IPC::Run ();
use Test::More tests => 33;

my $node = pgNode->get_new_node('prod');
my @timer;
my @in;
my @out;
my @procs;

$node->init;
$node->append_conf('postgresql.conf', 'max_connections=8');
$node->start;

### Begin of tests ###

# failing without thresholds
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'oldest_idlexact',
                          '--username' => getlogin,
                          '--format'   => 'human'
    ],
    127,
    [ qr/^$/ ],
    [ qr/^FATAL: you must specify critical and warning thresholds.$/m ],
    'failing without thresholds'
);

# basic check
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'oldest_idlexact',
                          '--username' => getlogin,
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

# Add a new session and start a idle transaction
TestLib::system_or_bail('createdb',
    '--host' => $node->host,
    '--port' => $node->port,
    'testdb'
);

push @procs, pgSession->new($node, 'testdb');

$procs[0]->query('BEGIN');
$procs[0]->query('SELECT txid_current()');

# OK check
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'oldest_idlexact',
                          '--username' => getlogin,
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '3s',
                          '--critical' => '1h'
    ],
    0,
    [ qr/^Service  *: POSTGRES_OLDEST_IDLEXACT$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: 1 idle transaction\(s\)$/m,
      qr/^Perfdata *: testdb # idle xact=1$/m
    ],
    [ qr/^$/ ],
    'OK check'
);

# wait for transaction to be idle for more than 3 seconds
$node->poll_query_until('template1', q{
    SELECT current_timestamp - xact_start > interval '3s'
    FROM pg_catalog.pg_stat_activity
    WHERE datname = 'testdb' 
      AND xact_start IS NOT NULL
    LIMIT 1
});

# warning check
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'oldest_idlexact',
                          '--username' => getlogin,
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '2s',
                          '--critical' => '1h'
    ],
    1,
    [ qr/^Service  *: POSTGRES_OLDEST_IDLEXACT$/m,
      qr/^Returns  *: 1 \(WARNING\)$/m,
      qr/^Message  *: 1 idle transaction\(s\)$/m,
      qr/^Perfdata *: testdb # idle xact=1$/m
    ],
    [ qr/^$/ ],
    'warning check'
);

# critical check
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'oldest_idlexact',
                          '--username' => getlogin,
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '1s',
                          '--critical' => '2s'
    ],
    2,
    [ qr/^Service  *: POSTGRES_OLDEST_IDLEXACT$/m,
      qr/^Returns  *: 2 \(CRITICAL\)$/m,
      qr/^Message  *: 1 idle transaction\(s\)$/m,
      qr/^Perfdata *: testdb # idle xact=1$/m
    ],
    [ qr/^$/ ],
    'critical check'
);

# Emit one query and check that check_pga does not emit a warning or critical
$procs[0]->query('SELECT 1');

# active transaction check
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'oldest_idlexact',
                          '--username' => getlogin,
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '3s',
                          '--critical' => '1h'
    ],
    0,
    [ qr/^Service  *: POSTGRES_OLDEST_IDLEXACT$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: 1 idle transaction\(s\)$/m,
      qr/^Perfdata *: testdb # idle xact=1$/m
    ],
    [ qr/^$/ ],
    'active transaction check'
);

### End of tests ###

# stop immediate to kill any remaining backends
$node->stop('immediate');
