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
use Test::More tests => 6;

my $node = pgNode->get_new_node('prod');
my @timer;
my @in;
my @out;
my @procs;

$node->init;
$node->start;

### Beginning of tests ###

# basic check
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'max_freeze_age',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
    ],
    0,
    [ qr/^Service  *: POSTGRES_MAX_FREEZE_AGE$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: oldest database is .* with age .*$/m,
      qr/^Perfdata *: postgres=.*$/m,
    ],
    [ qr/^$/ ],
    'basic check'
);



### End of tests ###

# stop immediate to kill any remaining backends
$node->stop('immediate');
