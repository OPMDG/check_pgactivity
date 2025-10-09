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
use Test::More tests => 15;

my $node = pgNode->get_new_node('prod');

$node->init;
$node->start;

### Beginning of tests ###

# This service can run without thresholds

# Tests for PostreSQL 16 and before
SKIP: {
    skip "testing incompatibility with PostgreSQL 16 and before", 3
        if $node->version >= 17;

    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'hugepages',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
        ],
        1,
        [ qr/^$/ ],
        [ qr/^Service hugepages is not compatible with host/ ],
        'non compatible PostgreSQL version'
    );
}

SKIP: {
    skip "incompatible tests with PostgreSQL < 17", 34 if $node->version < 17;

    # Note: this service is only tested without huge pages because for
    # PostgreSQL to allocate huge page, the system must have huge pages
    # available. Stock OSes usually dont have either vm.nr_hugepages or
    # vm.nr_overcommit_hugepages configured, thus the tests would fail in most
    # cases. Modifying the sysctl config would require a superuser privileges,
    # we cannot expect it to be available everywhere either.
    $node->append_conf('postgresql.conf', "huge_pages = off");
    $node->restart();

    # basic check => Returns OK
    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'hugepages',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
        ],
        0,
        [ qr/^Service  *: POSTGRES_HUGEPAGES$/m,
          qr/^Returns  *: 0 \(OK\)$/m,
          qr/^Message  *: The huge pages are in the expected state: off.$/m,
          qr/^Perfdata *: huge_page_status=0$/m,
        ],
        [ qr/^$/ ],
        'basic check'
    );

    $node->command_checks_all( [
        './check_pgactivity', '--service'  => 'hugepages',
                              '--username' => $ENV{'USER'} || 'postgres',
                              '--format'   => 'human',
                              '--with-hugepages',
        ],
        2,
        [ qr/^Service  *: POSTGRES_HUGEPAGES$/m,
          qr/^Returns  *: 2 \(CRITICAL\)$/m,
          qr/^Message  *: The huge pages are: off, the expected state was: on.$/m,
          qr/^Perfdata *: huge_page_status=0$/m,
        ],
        [ qr/^$/ ],
        'failed check'
    );
}

### End of tests ###

# stop immediate to kill any remaining backends
$node->stop( 'immediate' );

