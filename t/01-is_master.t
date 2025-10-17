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

# basic check => Returns OK
$node->command_checks_all( [
        './check_pgactivity', '--service'  => 'is_master',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
        ],
        0,
        [ qr/^Service  *: POSTGRES_IS_MASTER$/m,
          qr/^Returns  *: 0 \(OK\)$/m,
          qr/^Message  *: Cluster is master/m,
        ],
        [ qr/^$/ ],
        'basic check'
);

$node->stop( 'immediate' );
$node->set_standby_mode;
$node->start;

# basic check => Returns CRITICAL
$node->command_checks_all( [
        './check_pgactivity', '--service'  => 'is_master',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
        ],
        2,
        [ qr/^Service  *: POSTGRES_IS_MASTER$/m,
          qr/^Returns  *: 2 \(CRITICAL\)$/m,
          qr/^Message  *: Cluster is not master/m,
        ],
        [ qr/^$/ ],
        'basic check not master'
);

$node->stop( 'immediate' );
### End of tests ###

# stop immediate to kill any remaining backends
$node->stop( 'immediate' );

