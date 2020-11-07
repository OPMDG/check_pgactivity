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

# declare instance named "prim"
my $prim   = PostgresNode->get_new_node('prim');
# declare standby instance named "sec"
my $stb1   = PostgresNode->get_new_node('sec1');
my $stb2   = PostgresNode->get_new_node('sec2');
my $backup = 'backup';

# create primary and start it
$prim->init(allows_streaming => 1);
$prim->start;
note("primary started");
# create backup
$prim->backup($backup);
note("backup done");

# create standby from backup and start it
$stb1->init_from_backup($prim, $backup, has_streaming => 1);
$stb1->start;
note("standby 1 started");

# create standby from backup and start it
$stb2->init_from_backup($prim, $backup, has_streaming => 1);
$stb2->start;
note("standby 2 started");

# checkpoint to avoid waiting long time for the standby to catchup
$prim->safe_psql('template1', 'checkpoint');
# wait for standby to catchup
$prim->wait_for_catchup($stb1, 'write', $prim->lsn('insert'));
note("standby catchup");

### Begin of tests ###

# Normal check with two standby
note "Normal check with two standby";
$prim->command_checks_all( [
    './check_pgactivity', '--service'  => 'streaming_delta',
                          '--username' => getlogin,
                          '--format'   => 'human'
    ],
    0,
    [
        qr/Service  *: POSTGRES_STREAMING_DELTA$/m,
        qr/Returns  *: 0 \(OK\)$/m,
        qr/Message  *: 2 slaves checked$/m,
        qr/Perfdata *: sent delta sec1@=0B$/m,
        qr/Perfdata *: wrote delta sec1@=0B$/m,
        qr/Perfdata *: flushed delta sec1@=0B$/m,
        qr/Perfdata *: replay delta sec1@=0B$/m,
        qr/Perfdata *: pid sec1@=\d+$/m,
        qr/Perfdata *: sent delta sec2@=0B$/m,
        qr/Perfdata *: wrote delta sec2@=0B$/m,
        qr/Perfdata *: flushed delta sec2@=0B$/m,
        qr/Perfdata *: replay delta sec2@=0B$/m,
        qr/Perfdata *: pid sec2@=\d+$/m,
        qr/Perfdata *: # of excluded slaves=0$/m,
        qr/Perfdata *: # of slaves=2$/m
    ],
    undef,
    'one standby streaming'
);

# Normal check excluding one
note "Normal check with two standby, excluding one";
$prim->command_checks_all( [
    './check_pgactivity', '--service'  => 'streaming_delta',
                          '--username' => getlogin,
                          '--exclude'  => 'sec1',
                          '--format'   => 'human'
    ],
    0,
    [
        qr/Service  *: POSTGRES_STREAMING_DELTA$/m,
        qr/Returns  *: 0 \(OK\)$/m,
        qr/Message  *: 1 slaves checked$/m,
        qr/Perfdata *: sent delta sec2@=0B$/m,
        qr/Perfdata *: wrote delta sec2@=0B$/m,
        qr/Perfdata *: flushed delta sec2@=0B$/m,
        qr/Perfdata *: replay delta sec2@=0B$/m,
        qr/Perfdata *: pid sec2@=\d+$/m,
        qr/Perfdata *: # of excluded slaves=1$/m,
        qr/Perfdata *: # of slaves=2$/m
    ],
    undef,
    'excluding one standby'
);

# Normal check excluding both
note "Normal check excluding both standby";
$prim->command_checks_all( [
    './check_pgactivity', '--service'  => 'streaming_delta',
                          '--username' => getlogin,
                          '--exclude'  => 'sec[12]',
                          '--format'   => 'human'
    ],
    0,
    [
        qr/Service  *: POSTGRES_STREAMING_DELTA$/m,
        qr/Returns  *: 0 \(OK\)$/m,
        qr/Message  *: 0 slaves checked$/m,
        qr/Perfdata *: # of excluded slaves=2$/m,
        qr/Perfdata *: # of slaves=2$/m
    ],
    undef,
    'excluding one standby'
);

# normal check with one explicit standby
note "normal check with one explicit standby";
$prim->command_checks_all( [
    './check_pgactivity', '--service'  => 'streaming_delta',
                          '--username' => getlogin,
                          '--slave'    => 'sec1 ',
                          '--format'   => 'human'
    ],
    0,
    [
        qr/Service  *: POSTGRES_STREAMING_DELTA$/m,
        qr/Returns  *: 0 \(OK\)$/m,
        qr/Message  *: 2 slaves checked$/m,
        qr/Perfdata *: sent delta sec1@=0B$/m,
        qr/Perfdata *: wrote delta sec1@=0B$/m,
        qr/Perfdata *: flushed delta sec1@=0B$/m,
        qr/Perfdata *: replay delta sec1@=0B$/m,
        qr/Perfdata *: pid sec1@=\d+$/m,
        qr/Perfdata *: sent delta sec2@=0B$/m,
        qr/Perfdata *: wrote delta sec2@=0B$/m,
        qr/Perfdata *: flushed delta sec2@=0B$/m,
        qr/Perfdata *: replay delta sec2@=0B$/m,
        qr/Perfdata *: pid sec2@=\d+$/m,
        qr/Perfdata *: # of excluded slaves=0$/m,
        qr/Perfdata *: # of slaves=2$/m
    ],
    undef,
    'one explicit standby'
);

# failing check when called with an explicit standby not connected
note "failing check when called with an explicit standby not connected";
$stb1->stop;
$prim->command_checks_all( [
    './check_pgactivity', '--service'  => 'streaming_delta',
                          '--username' => getlogin,
                          '--slave'    => 'sec1 ',
                          '--format'   => 'human'
    ],
    2,
    [
        qr/Service  *: POSTGRES_STREAMING_DELTA$/m,
        qr/Returns  *: 2 \(CRITICAL\)$/m,
        qr/Message  *: sec1  not connected$/m,
        qr/Perfdata *: sent delta sec2@=0B$/m,
        qr/Perfdata *: wrote delta sec2@=0B$/m,
        qr/Perfdata *: flushed delta sec2@=0B$/m,
        qr/Perfdata *: replay delta sec2@=0B$/m,
        qr/Perfdata *: pid sec2@=\d+$/m,
        qr/Perfdata *: # of excluded slaves=0$/m,
        qr/Perfdata *: # of slaves=1$/m
    ],
    undef,
    'one failing explicit standby'
);

# no standby connected
note "no standby connected";
$stb2->stop;
$prim->command_checks_all( [
    './check_pgactivity', '--service'  => 'streaming_delta',
                          '--username' => getlogin,
                          '--format'   => 'human'
    ],
    3,
    [
        qr/Service  *: POSTGRES_STREAMING_DELTA$/m,
        qr/Returns  *: 3 \(UNKNOWN\)$/m,
        qr/Message  *: No slaves connected$/m,
    ],
    undef,
    'no standby connected'
);

### End of tests ###

$stb1->stop;
$stb2->stop;
$prim->stop;
done_testing;
