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
use Test::More tests => 5;

my $node = pgNode->get_new_node('prod');

$node->init;
$node->start;

### Beginning of tests ###

# basic check => Returns OK
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'invalid_indexes',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--warning'  => '1',
                          '--critical' => '2',
    ],
    0,
    [ qr/^Service  *: POSTGRES_INVALID_INDEXES$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: No invalid index$/m,
    ],
    [ qr/^$/ ],
    'basic check'
);

### End of tests ###

# stop immediate to kill any remaining backends
$node->stop( 'immediate' );
