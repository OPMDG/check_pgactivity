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
use TestLib ();
use IPC::Run ();
use Test::More tests => 2;
use PostgresVersion;

my $node = pgNode->get_new_node('prod');
my @timer;
my @in;
my @out;
my @procs;
my $pgversion;

$node->init;

$node->start;
$pgversion=PostgresVersion->new($node->version());
$node->stop('immediate');

$node->start;

### Beginning of tests ###
subtest pg15 => sub {

    # Tests for PostreSQL 11 and before
  SKIP: {
        skip "testing incompatibility with PostgreSQL 15 and later", 3
          if $node->version < 15;
        plan tests => 3;

        $node->command_checks_all(
            [
                './check_pgactivity',
                '--service'  => 'stat_snapshot_age',
                '--username' => $ENV{'USER'} || 'postgres',
                '--format'   => 'human',
            ],
            1,
            [qr/^$/],
            [qr/^Service stat_snapshot_age is not compatible with host/],
            'non compatible PostgreSQL version'
        );
  }
};

subtest pg14 => sub {
  SKIP: {
        skip "incompatible tests with PostgreSQL between 9.4 and 14", 34
          if $node->version > 14;

        plan tests => 4;

        # basic check => Returns OK
        $node->command_checks_all(
            [
                './check_pgactivity',
                '--service'  => 'stat_snapshot_age',
                '--username' => $ENV{'USER'} || 'postgres',
                '--format'   => 'human',
            ],
            0,
            [
                qr/^Service  *: POSTGRES_STAT_SNAPSHOT_AGE$/m,
                qr/^Returns  *: 0 \(OK\)$/m,
            ],
            [qr/^$/],
            'basic check'
        );
  }
};

# stop immediate to kill any remaining backends
$node->stop('immediate');

done_testing();

