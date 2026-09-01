#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2026: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't/lib';
use pgNode;
use Test::More tests => 12;

my $node        = pgNode->new('prod'); # declare instance named "prod"

# create the instance and start it
$node->init();
$node->start;

### Beginning of tests ###

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

### End of tests ###

$node->stop( 'immediate' );
