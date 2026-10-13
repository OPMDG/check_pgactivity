#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2026: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't/lib';
use pgNode;
use Test::More tests => 10;

my $prim      = pgNode->get_new_node('prim');
my $stb1      = pgNode->get_new_node('sec1');
my $backup    = 'backup';
my $pgversion = $prim->version;

note "testing on version $pgversion";

# Tests for PostreSQL 9.0 and after
SKIP: {
    skip "these tests requires PostgreSQL 9.1 and after", 10
        unless $pgversion >= '9.0';

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

# checkpoint to avoid waiting long time for the standby to catchup
$prim->safe_psql('template1', 'checkpoint');
$prim->wait_for_catchup($stb1, 'replay', $prim->lsn('insert'));

### Beginning of tests ###

# test on primary => Returns FALSE

$prim->command_checks_all( [
        './check_pgactivity', '--service'  => 'is_hot_standby',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
                              '--host'     => $prim->host,
                              '--port'     => $prim->port,
    ],
    2,
    [ qr/^Service  *: POSTGRES_IS_HOT_STANDBY$/m,
      qr/^Returns  *: 2 \(CRITICAL\)$/m,
      qr/^Message  *: Cluster is not hot standby/m,
    ],
    [ qr/^$/ ],
    'basic check'
);

# test on secondary => Returns FALSE
$stb1->command_checks_all( [
        './check_pgactivity', '--service'  => 'is_hot_standby',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
                              '--host'     => $stb1->host,
                              '--port'     => $stb1->port
    ],
    0,
    [ qr/^Service  *: POSTGRES_IS_HOT_STANDBY$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: Cluster is hot standby/m,
    ],
    [ qr/^$/ ],
    'basic check not master'
);


# stop immediate to kill any remaining backends
$prim->stop('immediate');
$stb1->stop('immediate');

}

### End of tests ###
done_testing();

