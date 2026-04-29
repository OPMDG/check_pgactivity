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
use Time::HiRes qw(usleep gettimeofday tv_interval);
use Test::More tests => 15;

my $node = pgNode->get_new_node('prod');

$node->init;
$node->start;

### Beginning of tests ###

# basic check => Returns OK
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'unused_indexes',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
    ],
    0,
    [ qr/^Service  *: POSTGRES_UNUSED_INDEXES$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: No unused index$/m,
    ],
    [ qr/^$/ ],
    'basic check'
);

# create table and index
$node->psql('postgres', 'CREATE DATABASE b1;');
$node->psql('b1', 'CREATE TABLE foo(id integer);');
$node->psql('b1', 'CREATE INDEX ON foo(id);');

# unused index check => Returns WARNING
$node->command_checks_all( [
    './check_pgactivity', '--service'   => 'unused_indexes',
                          '--username'  => $ENV{'USER'} || 'postgres',
                          '--format'    => 'human',
                          '--dbinclude' => 'b1',
    ],
    1,
    [ qr/^Service  *: POSTGRES_UNUSED_INDEXES$/m,
      qr/^Returns  *: 1 \(WARNING\)$/m,
      qr/^Message  *: 1\/1 index\(es\) unused$/m,
    ],
    [ qr/^$/ ],
    'unused index check'
);

$node->psql('b1', 'INSERT INTO foo SELECT generate_series(1, 1000000);');
$node->psql('b1', 'ANALYZE foo;');
$node->psql('b1', 'SELECT * FROM foo WHERE id=100;');

# used index check => Returns OK
$node->command_checks_all( [
    './check_pgactivity', '--service'   => 'unused_indexes',
                          '--username'  => $ENV{'USER'} || 'postgres',
                          '--format'    => 'human',
                          '--dbinclude' => 'b1',
    ],
    0,
    [ qr/^Service  *: POSTGRES_UNUSED_INDEXES$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: No unused index$/m,
    ],
    [ qr/^$/ ],
    'used index check'
);

### End of tests ###

# stop immediate to kill any remaining backends
$node->stop( 'immediate' );
