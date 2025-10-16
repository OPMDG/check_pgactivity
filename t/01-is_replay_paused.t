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
use Time::HiRes qw(usleep gettimeofday tv_interval);
use Test::More tests => 10;

my $node = pgNode->get_new_node('prod');

$node->init;
$node->append_conf('postgresql.conf', 'wal_level=replica');

$node->start;

### Beginning of tests ###

# check on master => Returns UNKNOWN
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'is_replay_paused',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
    ],
    3,
    [ qr/^Service  *: POSTGRES_REPLICATION_PAUSED$/m,
      qr/^Returns  *: 3 \(UNKNOWN\)$/m,
      qr/^Message  *: Server is not standby./m,
    ],
    [ qr/^$/ ],
    'check on master'
);

$node->stop( 'immediate' );
$node->set_standby_mode;
$node->start;

# replay not paused => Returns OK
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'is_replay_paused',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
    ],
    0,
    [ qr/^Service  *: POSTGRES_REPLICATION_PAUSED$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *:  replay is not paused/m,
    ],
    [ qr/^$/ ],
    'replay not paused'
);

### End of tests ###

# stop immediate to kill any remaining backends
$node->stop( 'immediate' );

