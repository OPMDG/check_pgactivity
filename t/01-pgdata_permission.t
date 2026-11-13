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
use Test::More tests => 22;

my $node = pgNode->get_new_node('prod');

$node->init;
$node->start;

### Beginning of tests ###
# First check.
$node->command_checks_all( [
    './check_pgactivity', '--service'     => 'pgdata_permission',
                          '--username'    => $ENV{'USER'} || 'postgres',
                          '--format'      => 'human',
    ],
    0,
    [
      qr/^Service  *: POSTGRES_CHECK_PGDATA_PERMISSION$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: Permission is ok on .*$/m,
      qr/^Message  *: Owner of .* is \(.*\)$/m,
    ],
    [ qr/^$/ ],
    'first basic check'
);

# unit test with a tablespace => Returns OK
mkdir $node->basedir . '/tablespace1';
$node->psql('postgres', 'CREATE TABLESPACE myts1 LOCATION \'' . $node->basedir . '/tablespace1\';');

# second check
$node->command_checks_all( [
    './check_pgactivity', '--service'     => 'pgdata_permission',
                          '--username'    => $ENV{'USER'} || 'postgres',
                          '--format'      => 'human',
    ],
    0,
    [
      qr/^Service  *: POSTGRES_CHECK_PGDATA_PERMISSION$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: Permission is ok on .*$/m,
      qr/^Message  *: Owner of .* is \(.*\)$/m,
      qr/^Message  *: Permission is ok on .*$/m,
      qr/^Message  *: Owner of .* is \(.*\)$/m,
    ],
    [ qr/^$/ ],
    'basic check with one "ok" tablespace'
);

chmod 0777, $node->basedir . '/tablespace1';

# third check
$node->command_checks_all( [
    './check_pgactivity', '--service'     => 'pgdata_permission',
                          '--username'    => $ENV{'USER'} || 'postgres',
                          '--format'      => 'human',
    ],
    2,
    [
      qr/^Service  *: POSTGRES_CHECK_PGDATA_PERMISSION$/m,
      qr/^Returns  *: 2 \(CRITICAL\)$/m,
      qr/^Message  *: Permission is ok on .*$/m,
      qr/^Message  *: Owner of .* is \(.*\)$/m,
      qr/^Message  *: Permission is 0777 on .* \(tablespace myts1\)$/m,
      qr/^Message  *: Owner of .* is \(.*\)$/m,
    ],
    [ qr/^$/ ],
    'basic check with one "bad" tablespace'
);

### End of tests ###

# stop immediate to kill any remaining backends
$node->stop('immediate');
