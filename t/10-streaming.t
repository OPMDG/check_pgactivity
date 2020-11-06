#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2020: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't';
use PostgresNode;
use Test::More;
use TestLib ( 'command_checks_all' );

# declare instance named "prim"
my $prim   = PostgresNode->get_new_node('prim');
# declare standby instance named "sec"
my $stby   = PostgresNode->get_new_node('sec');
my $backup = 'backup';
my %ans;

# create primary and start it
$prim->init(allows_streaming => 1);
$prim->start;
note("primary started");
# create backup
$prim->backup($backup);
note("backup done");

# create standby from backup and start it
$stby->init_from_backup($prim, $backup, has_streaming => 1);
$stby->start;
note("standby started");

# checkpoint to avoid waiting long time for the standby to catchup
$prim->safe_psql('template1', 'checkpoint');
# wait for standby to catchup
$prim->wait_for_catchup($stby, 'write', $prim->lsn('insert'));
note("standby catchup");

### Begin of tests ###

command_checks_all( [
    './check_pgactivity', '--service'  => 'streaming_delta',
                          '--host'     => $prim->host,
                          '--port'     => $prim->port,
                          '--username' => getlogin,
                          '--format'   => 'human'
    ],
    0,
    [
        qr/Service  *: POSTGRES_STREAMING_DELTA$/m,
        qr/Returns  *: 0 \(OK\)$/m,
        qr/Message  *: 1 slaves checked$/m,
        qr/Perfdata *: sent delta sec@=0B$/m,
        qr/Perfdata *: wrote delta sec@=0B$/m,
        qr/Perfdata *: flushed delta sec@=0B$/m,
        qr/Perfdata *: pid sec@=\d+$/m,
        qr/Perfdata *: replay delta sec@=0B$/m,
        qr/Perfdata *: # of excluded slaves=0$/m,
        qr/Perfdata *: # of slaves=1$/m
    ],
    undef,
    'check_streaming'
);

### End of tests ###

$stby->stop;
$prim->stop;
done_testing;
