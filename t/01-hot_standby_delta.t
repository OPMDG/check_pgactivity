#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2026: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't/lib';
use pgNode;
use Test::More;

plan tests => 14;

# declare instance named "prim"
my $prim      = pgNode->get_new_node('prim');
# declare standby instances "sec1" and "sec2"
my $stb1      = pgNode->get_new_node('sec1');
my $stb2      = pgNode->get_new_node('sec2');
my $backup    = 'backup'; # backup name
my $pgversion = $prim->version;

note "testing on version $pgversion";

# Tests for PostreSQL 9.0 and after
SKIP: {
    skip "these tests requires PostgreSQL 9.1 and after", 14
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

    # create standby from backup and start it
    $stb2->init_from_backup($prim, $backup, has_streaming => 1);
    $stb2->start;
    note("standby 2 started");

    # checkpoint to avoid waiting long time for the standby to catchup
    $prim->safe_psql('template1', 'checkpoint');

    # wait for standbys to catchup
    $prim->wait_for_catchup($stb1, 'replay', $prim->lsn('insert'));
    $prim->wait_for_catchup($stb2, 'replay', $prim->lsn('insert'));
    note("standbys caught up");

    ### Beginning of tests ###

    # Normal check with one standby
    note "Normal check with one standby";
    $prim->command_checks_all( [
        './check_pgactivity', '--service'  => 'hot_standby_delta',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
                              '--host'     => $prim->host . ',' . $stb1->host,
                              '--port'     => $prim->port . ',' . $stb1->port,
        ],
        0,
        [
            qr/Service  *: POSTGRES_HOT_STANDBY_DELTA$/m,
            qr/Returns  *: 0 \(OK\)$/m,
            qr/Message  *: 1 Hot standby checked$/m,
            qr/Perfdata *: receive delta host:.* port:[1-9][0-9]+ db:postgres=0B$/m,
            qr/Perfdata *: replay delta host:.* port:[1-9][0-9]+ db:postgres=0B$/m,
        ],
        [ qr/^$/ ],
        'two standbys streaming'
    );

    # Normal check with two standby
    note "Normal check with two standby";
    $prim->command_checks_all( [
        './check_pgactivity', '--service'  => 'hot_standby_delta',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
                              '--host'     => $prim->host .','. $stb1->host .','. $stb2->host,
                              '--port'     => $prim->port .','. $stb1->port .','. $stb2->port,
        ],
        0,
        [
            qr/Service  *: POSTGRES_HOT_STANDBY_DELTA$/m,
            qr/Returns  *: 0 \(OK\)$/m,
            qr/Message  *: 2 Hot standby checked$/m,
            qr/Perfdata *: receive delta host:.* port:[1-9][0-9]+ db:postgres=0B$/m,
            qr/Perfdata *: replay delta host:.* port:[1-9][0-9]+ db:postgres=0B$/m,
        ],
        [ qr/^$/ ],
        'two standbys streaming'
    );

} # end of SKIP

### End of tests ###
done_testing();

# stop immediate to kill any remaining backends
$prim->stop('immediate');
$stb1->stop('immediate');
$stb2->stop('immediate');
