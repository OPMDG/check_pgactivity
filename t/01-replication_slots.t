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
use Test::More tests => 5;

my $node = pgNode->get_new_node('prod');

$node->init;
$node->start;

### Beginning of tests ###


# First check.
$node->command_checks_all( [
    './check_pgactivity', '--service'     => 'replication_slots',
                          '--username'    => $ENV{'USER'} || 'postgres',
                          '--format'      => 'human',
    ],
    0,
    [
      qr/^Service  *: POSTGRES_REPLICATION_SLOTS$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: Replication slots OK$/m,
    ],
    [ qr/^$/ ],
    'first basic check'
);

$node->stop;
if ( $node->version >= 9.6 ) {
    $node->append_conf('postgresql.conf', "wal_level = replica");
}
elsif ( $node->version >= 9.0 ) {
    $node->append_conf('postgresql.conf', "wal_level = archive");
}
$node->start;



### End of tests ###

# stop immediate to kill any remaining backends
$node->stop('immediate');
