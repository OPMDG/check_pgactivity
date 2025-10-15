#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2025: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't/lib';
use pgNode;
use pgSession;
use TestLib ();
use IPC::Run ();
use Test::More tests => 14;

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
    './check_pgactivity', '--service'  => 'table_bloat',
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
    './check_pgactivity', '--service'  => 'table_bloat',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '40%',
                          '--critical' => '20%'
    ],
    0,
    [ qr/^Service  *: POSTGRES_TABLE_BLOAT$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: Table bloat ok$/m,
    ],
    [ qr/^$/ ],
    'basic check'
);

# add a bloated table
TestLib::system_or_bail('createdb',
    '--host' => $node->host,
    '--port' => $node->port,
    'testdb'
);

$node->psql('testdb', 'CREATE TABLE bloated (i serial, x text) WITH (autovacuum_enabled = off);');
$node->psql('testdb', 'INSERT INTO bloated (x) SELECT md5(i::text) FROM generate_series(1, 10000) i;');
$node->psql('testdb', 'DELETE FROM bloated WHERE i < 9000;');
$node->psql('testdb', 'ANALYZE bloated;');

# basic check
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'table_bloat',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '40%',
                          '--critical' => '20%'
    ],
    2,
    [ qr/^Service  *: POSTGRES_TABLE_BLOAT$/m,
      qr/^Returns  *: 2 \(CRITICAL\)$/m,
      qr/^Message  *: 1\/(.*) table\(s\) bloated$/m,
      qr/^Perfdata *: table bloated in testdb=1$/m,
    ],
    [ qr/^$/ ],
    'check for bloated table'
);

### End of tests ###

# stop immediate to kill any remaining backends
$node->stop('immediate');
