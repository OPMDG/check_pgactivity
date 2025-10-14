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
use Test::More tests => 2;

my $node = pgNode->get_new_node('prod');

$node->init( data_checksums => 1 );
$node->start;

### Beginning of tests ###

$node->psql( 'postgres', 'CREATE TABLE corruptme (x text)' );
$node->psql( 'postgres', '
    INSERT INTO corruptme (x)
    SELECT md5(i::text) FROM generate_series(1, 10000) i
');
my $file = $node->safe_psql( 'postgres', q{
    SELECT pg_relation_filepath('corruptme')
});
note("==> Corrupted file : $file");

subtest pg11 => sub {

    # Tests for PostreSQL 11 and before
  SKIP: {
        skip "testing incompatibility with PostgreSQL 11 and before", 3
          if $node->version >= 12;
        plan tests => 3;

        $node->command_checks_all(
            [
                './check_pgactivity',
                '--service'  => 'checksum_errors',
                '--username' => $ENV{'USER'} || 'postgres',
                '--format'   => 'human',
            ],
            1,
            [qr/^$/],
            [qr/^Service checksum_errors is not compatible with host/],
            'non compatible PostgreSQL version'
        );
    }
};

subtest pg12 => sub {
  SKIP: {
        skip "incompatible tests with PostgreSQL < 12", 34
          if $node->version < 12;

        plan tests => 12;

        # basic check => Returns OK
        $node->command_checks_all(
            [
                './check_pgactivity',
                '--service'  => 'checksum_errors',
                '--username' => $ENV{'USER'} || 'postgres',
                '--format'   => 'human',
            ],
            0,
            [
                qr/^Service  *: POSTGRES_CHECKSUM_ERRORS$/m,
                qr/^Message  *: 4 database\(s\) checked$/m,
                qr/^Returns  *: 0 \(OK\)$/m,
            ],
            [qr/^$/],
            'basic check'
        );

     # Make sure the data is written on disk before Postgres is stopped
     # If this checkpoint is skipped, PG will overwrite the corrupted page after
     # starting WAL replay at startup.
        $node->psql( 'postgres', 'CHECKPOINT' );
        $node->stop('immediate');

        # Corrupt silently checksum of first page of table corruptme
        # Postgres is stopped to avoid any caching
        $node->corrupt_page_checksum( $file, 0 );

        $node->start;

        # Some debug output
        $node->psql( 'postgres', "VACUUM corruptme" );
        sleep(2);

        # corruption check => Returns CRITICAL
        $node->command_checks_all(
            [
                './check_pgactivity',
                '--service'  => 'checksum_errors',
                '--username' => $ENV{'USER'} || 'postgres',
                '--format'   => 'human',
            ],
            2,
            [
                qr/^Service  *: POSTGRES_CHECKSUM_ERRORS$/m,
                qr/^Message  *: postgres: 1 error\(s\)$/m,
                qr/^Perfdata *: postgres=1 warn=1 crit=1$/m,
                qr/^Perfdata *: template1=0 warn=1 crit=1$/m,
                qr/^Returns  *: 2 \(CRITICAL\)$/m,
            ],
            [qr/^$/],
            'basic check'
        );
    }
};

### End of tests ###

# stop immediate to kill any remaining backends
$node->stop('immediate');

