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
use File::Copy;
use Time::HiRes qw(usleep gettimeofday tv_interval);
use Test::More tests => 29;

my $node = pgNode->get_new_node('prod');
my $proc;

$node->init;
$node->start;

### Beginning of tests ###

# Find the OID of the postgres database
my ($cmdret, $dboid, $stderr) =
      $node->psql('postgres', "SELECT oid FROM pg_database WHERE datname='postgres'");

# This service can run without thresholds

# basic check => Returns OK
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'orphan_files',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
    ],
    0,
    [ qr/^Service  *: POSTGRES_ORPHAN_FILES$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: No orphan files found!$/m,
    ],
    [ qr/^$/ ],
    'basic check'
);

# basic check in error
#
copy($node->data_dir . '/base/' . $dboid . '/1247', $node->data_dir . '/base/' . $dboid . '/124712471');

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'orphan_files',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
    ],
    0,
    [ qr/^Service  *: POSTGRES_ORPHAN_FILES$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: 1 orphan files found, total size .*$/m,
    ],
    [ qr/^$/ ],
    'basic check with an orphan file'
);

copy($node->data_dir . '/base/' . $dboid . '/1247', $node->data_dir . '/base/' . $dboid . '/124712472');
copy($node->data_dir . '/base/' . $dboid . '/1247', $node->data_dir . '/base/' . $dboid . '/124712473');

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'orphan_files',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '2',
                          '--critical' => '4',
    ],
    1,
    [ qr/^Service  *: POSTGRES_ORPHAN_FILES$/m,
      qr/^Returns  *: 1 \(WARNING\)$/m,
      qr/^Message  *: 3 orphan files found, total size .*$/m,
    ],
    [ qr/^$/ ],
    'check ending in warning for number of files'
);

copy($node->data_dir . '/base/' . $dboid . '/1247', $node->data_dir . '/base/' . $dboid . '/124712474');

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'orphan_files',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '2',
                          '--critical' => '4',
    ],
    2,
    [ qr/^Service  *: POSTGRES_ORPHAN_FILES$/m,
      qr/^Returns  *: 2 \(CRITICAL\)$/m,
      qr/^Message  *: 4 orphan files found, total size .*$/m,
    ],
    [ qr/^$/ ],
    'check ending in critical for number of files'
);

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'orphan_files',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--warning'  => '2',
                          '--critical' => '4',
                          '--detailed',
    ],
    2,
    [ qr/^Service  *: POSTGRES_ORPHAN_FILES$/m,
      qr/^Returns  *: 2 \(CRITICAL\)$/m,
      qr/^Message  *: 4 orphan files found, total size .*$/m,
      qr/^Long message  *: File .*, size \d+.\d\d.*$/m,
      qr/^Long message  *: File .*, size \d+.\d\d.*$/m,
      qr/^Long message  *: File .*, size \d+.\d\d.*$/m,
      qr/^Long message  *: File .*, size \d+.\d\d.*$/m,
    ],
    [ qr/^$/ ],
    'detailed check ending in critical for number of files'
);

### End of tests ###

# stop immediate to kill any remaining backends
$node->stop( 'immediate' );
