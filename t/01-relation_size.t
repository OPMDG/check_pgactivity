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
use Test::More tests => 25;

my $node = pgNode->get_new_node('prod');

$node->init;
$node->start;

### Beginning of tests ###

# create tables
$node->psql('postgres', 'CREATE DATABASE b1;');
$node->psql('b1', 'CREATE TABLE t1(id integer);');
$node->psql('b1', 'CREATE TABLE t2(id integer);');
$node->psql('b1', 'CREATE TABLE t3(id integer);');
$node->psql('b1', 'INSERT INTO t1 SELECT generate_series(1, 100000);');
$node->psql('b1', 'INSERT INTO t2 SELECT generate_series(1, 1000000);');
$node->psql('b1', 'INSERT INTO t3 SELECT generate_series(1, 5000000);');

# basic check => Returns OK
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'relation_size',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbinclude' => 'b1',
    ],
    0,
    [ qr/^Service  *: POSTGRES_RELATION_SIZE$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: 3 relation\(s\)$/m,
      qr/^Perfdata *: .*=[1-9][.0-9]*[kMGTPE]B$/m,
    ],
    [ qr/^$/ ],
    'basic check'
);


# Returns WARNING
$node->command_checks_all( [
    './check_pgactivity', '--service'   => 'relation_size',
                          '--username'  => $ENV{'USER'} || 'postgres',
                          '--format'    => 'human',
                          '--dbinclude' => 'b1',
                          '--warning'   => '100MB',
                          '--critical'  => '200MB',
    ],
    1,
    [ qr/^Service  *: POSTGRES_RELATION_SIZE$/m,
      qr/^Returns  *: 1 \(WARNING\)$/m,
      qr/^Message  *: 1\/3 oversized relation\(s\)$/m,
      qr/^Perfdata *: .*=[1-9][.0-9]*[kMGTPE]B$/m,
    ],
    [ qr/^$/ ],
    'warning check'
);

# Returns CRITICAL
$node->command_checks_all( [
    './check_pgactivity', '--service'   => 'relation_size',
                          '--username'  => $ENV{'USER'} || 'postgres',
                          '--format'    => 'human',
                          '--dbinclude' => 'b1',
                          '--warning'   => '100MB',
                          '--critical'  => '150MB',
    ],
    2,
    [ qr/^Service  *: POSTGRES_RELATION_SIZE$/m,
      qr/^Returns  *: 2 \(CRITICAL\)$/m,
      qr/^Message  *: 1\/3 oversized relation\(s\)$/m,
      qr/^Perfdata *: .*=[1-9][.0-9]*[kMGTPE]B$/m,
    ],
    [ qr/^$/ ],
    'critical check'
);

# Returns OK if exclude
$node->command_checks_all( [
    './check_pgactivity', '--service'   => 'relation_size',
                          '--username'  => $ENV{'USER'} || 'postgres',
                          '--format'    => 'human',
                          '--dbinclude' => 'b1',
                          '--warning'   => '100MB',
                          '--critical'  => '150MB',
                          '--exclude'   => 't2',
                          '--exclude'   => 't3',
    ],
    0,
    [ qr/^Service  *: POSTGRES_RELATION_SIZE$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Long message  *: 2 excluded relation\(s\) from check$/m,
      qr/^Message  *: 1 relation\(s\)$/m,
      qr/^Perfdata *: .*=[1-9][.0-9]*[kMGTPE]B$/m,
    ],
    [ qr/^$/ ],
    'exclude check'
);

### End of tests ###

# stop immediate to kill any remaining backends
$node->stop( 'immediate' );
