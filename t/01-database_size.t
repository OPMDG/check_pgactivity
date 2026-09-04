#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2026: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't/lib';
use pgNode;
use TestLib ();
use Test::More tests => 6;

my $node = pgNode->get_new_node('prod');
my $pga_data = "$TestLib::tmp_check/pga.data";

$node->init;
$node->start;

### Beginning of tests ###

# First check. Returns no perfdata
$node->command_checks_all( [
    './check_pgactivity', '--service'     => 'database_size',
                          '--username'    => $ENV{'USER'} || 'postgres',
                          '--format'      => 'human',
                          '--status-file' => $pga_data,
    ],
    0,
    [
      qr/^Service  *: POSTGRES_DB_SIZE$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: [0-9]+ database\(s\) checked$/m,
      qr/^Perfdata *: template1=.*$/m,
    ],
    [ qr/^$/ ],
    'first basic check'
);

### End of tests ###

# stop immediate to kill any remaining backends
$node->stop('immediate');
