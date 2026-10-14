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
use Test::More tests => 12;

my $node = pgNode->get_new_node('prod');

$node->init;
$node->append_conf('postgresql.conf', "work_mem='64kB'");
$node->start;

### Beginning of tests ###

# basic check with CRITICAL
#
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'configuration',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
    ],
    2,
    [ qr/^Service  *: POSTGRES_CONFIGURATION$/m,
      qr/^Returns  *: 2 \(CRITICAL\)$/m,
      qr/^Message  *: fsync=off$/m,
      qr/^Message  *: data_checksums=off$/m,
      qr/^Message  *: work_mem=64$/m,
    ],
    [ qr/^$/ ],
    'critical'
);

$node->append_conf('postgresql.conf', 'fsync=on');
$node->restart();

# WARNING
#
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'configuration',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--exclude'  => 'data_checksums',
    ],
    1,
    [ qr/^Service  *: POSTGRES_CONFIGURATION$/m,
      qr/^Returns  *: 1 \(WARNING\)$/m,
      qr/^Message  *: work_mem=64$/m,
    ],
    [ qr/^$/ ],
    'only a warning'
);

### End of tests ###

# stop immediate to kill any remaining backends
$node->stop( 'immediate' );
