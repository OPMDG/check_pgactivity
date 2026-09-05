#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2026: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't/lib';
use pgNode;
use Test::More tests => 2;

my $node        = pgNode->new('prod'); # declare instance named "prod"

# create the instance and start it
$node->init();
$node->start;

### Beginning of tests ###

# Tests for PostreSQL 14 and before
subtest pg14 => sub {
    plan tests => 3;

    SKIP: {
        skip "testing incompatibility with PostgreSQL 14 and before", 3
            unless $node->version <= 15.0;

        $node->command_checks_all( [
            './check_pgactivity', '--service'  => 'ident_settings',
                                  '--username' => $ENV{'USER'} || 'postgres',
                                  '--format'   => 'human'
            ],
            1,
            [ qr/^$/ ],
            [ qr/^Service ident_settings is not compatible with host/ ],
            'non compatible PostgreSQL version'
        );
    }
};

subtest pg15 => sub {
    plan tests => 12;

    SKIP: {
        skip "incompatible tests with PostgreSQL < 15", 12
            unless $node->version >= 15.0;

        # simple check
        $node->command_checks_all( [
            './check_pgactivity', '--service'  => 'ident_settings',
                                  '--username' => $ENV{'USER'} || 'postgres',
                                  '--format'   => 'human'
            ],
            0,
            [
                qr/^Service  *: POSTGRES_IDENT_SETTINGS$/m,
                qr/^Returns  *: 0 \(OK\)$/m,
                qr/^Message  *: No errors in ident configuration files$/m,
            ],
            [ qr/^$/ ],
            'simple check with no error'
        );

        # Add a simple one with an error
        $node->append_conf('pg_ident.conf', "foobar");

        $node->command_checks_all( [
            './check_pgactivity', '--service'  => 'ident_settings',
                                  '--username' => $ENV{'USER'} || 'postgres',
                                  '--format'   => 'human'
            ],
            2,
            [
                qr/^Service  *: POSTGRES_IDENT_SETTINGS$/m,
                qr/^Returns  *: 2 \(CRITICAL\)$/m,
                qr/^Message  *: 1 error\(s\)$/m,
                qr/^Long message *: file .*, line \d+, error: .*$/m,
                qr/^Perfdata *: errors=1$/m,
            ],
            [ qr/^$/ ],
            'simple check with error'
        );
    }
};

done_testing;

### End of tests ###

$node->stop( 'immediate' );
