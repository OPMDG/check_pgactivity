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
use Test::More;

my $node = pgNode->get_new_node('prod');
my @timer;
my @in;
my @out;
my @procs;
my @stdout;

$node->init;
$node->append_conf('postgresql.conf', 'max_connections=8');
$node->start;

### Beginning of tests ###

# basic check without thresholds
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'backends_status',
                          '--username' => getlogin,
                          '--format'   => 'human'
    ],
    0,
    [ qr/^Service  *: POSTGRES_BACKENDS_STATUS$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
    ],
    [ qr/^$/ ],
    'OK without thresholds'
);

# basic check with thresholds
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'backends_status',
                          '--username' => getlogin,
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => 'idle_xact=4s',
                          '--critical' => 'idle_xact=5s'
    ],
    0,
    [
      qr/^Service  *: POSTGRES_BACKENDS_STATUS$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: 1 backend connected$/m,
      qr/^Perfdata *: active=1$/m,
      qr/^Perfdata *: oldest active=0s$/m,
      qr/^Perfdata *: disabled=0$/m,
      qr/^Perfdata *: fastpath function call=0$/m,
      qr/^Perfdata *: oldest fastpath function call=0s$/m,
      qr/^Perfdata *: idle=0$/m,
      qr/^Perfdata *: oldest idle=0s$/m,
      qr/^Perfdata *: idle in transaction=0$/m,
      qr/^Perfdata *: oldest idle in transaction=0s warn=4s crit=5s min=\d max=\d$/m,
      qr/^Perfdata *: idle in transaction \(aborted\)=0$/m,
      qr/^Perfdata *: oldest idle in transaction \(aborted\)=0s$/m,
      qr/^Perfdata *: insufficient privilege=0$/m,
      qr/^Perfdata *: undefined=0$/m,
      qr/^Perfdata *: waiting for lock=0$/m,
      qr/^Perfdata *: oldest waiting for lock=0s$/m,
    ],
    [ qr/^$/ ],
    'basic check with threshold and check presence of all perfdata'
);

# two sessions on two different db

TestLib::system_or_bail('createdb',
    '--host' => $node->host,
    '--port' => $node->port,
    'testdb'
);

push @procs, pgSession->new($node, 'testdb') for 1..3;

$procs[0]->query('select pg_sleep(60)', 60);
$procs[1]->query('BEGIN',0);

# wait for backend to be connected and active
$node->poll_query_until('template1', q{
    SELECT query_start IS NOT NULL -- < now()
    FROM pg_catalog.pg_stat_activity
    WHERE datname = 'testdb'
    LIMIT 1
});

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'backends_status',
                          '--username' => getlogin,
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => 'active=3',
                          '--critical' => 'active=4'
    ],
    0,
    [ qr/^Service  *: POSTGRES_BACKENDS_STATUS$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: 4 backend connected$/m,
      qr/^Perfdata *: idle=1$/m,
      qr/^Perfdata *: idle in transaction=1$/m,
      qr/^Perfdata *: active=2 warn=3 crit=4 min=\d max=\d$/m,
    ],
    [ qr/^$/ ],
    'three sessions, one active, one idlexact, one idle, OK'
);

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'backends_status',
                          '--username' => getlogin,
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => 'active=1',
                          '--critical' => 'active=2'
    ],
    2,
    [ qr/^Service  *: POSTGRES_BACKENDS_STATUS$/m,
      qr/^Returns  *: 2 \(CRITICAL\)$/m,
      qr/^Message  *: 2 active$/m,
      qr/^Perfdata *: idle=1$/m,
      qr/^Perfdata *: idle in transaction=1$/m,
      qr/^Perfdata *: active=2 warn=1 crit=2 min=\d max=\d$/m,
    ],
    [ qr/^$/ ],
    'three sessions, one active, one idlexact, one idle, Critical'
);


done_testing();
exit;

### End of tests ###

# stop immediate to kill any remaining backends
$node->stop('immediate');
