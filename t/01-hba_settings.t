#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2026: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't/lib';
use pgNode;
use Test::More tests => 27;

my $node        = pgNode->new('prod'); # declare instance named "prod"

# create the instance and start it
$node->init();
$node->start;

### Beginning of tests ###

# simple check (trust everywhere, with initdb -A trust)
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'hba_settings',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human'
    ],
    2,
    [
        qr/^Service  *: POSTGRES_HBA_SETTINGS$/m,
        qr/^Returns  *: 2 \(CRITICAL\)$/m,
        qr/^Message  *: 1 critical issue\(s\) found, 0 warning issue\(s\) found$/m,
        qr/^Perfdata *: trust=6 warn=1 crit=1$/m,
    ],
    [ qr/^$/ ],
    'simple issues check'
);

# Rip all the HBA config
truncate $node->data_dir . '/' . 'pg_hba.conf', 0;

# Add a simple one without error but a critical issue
$node->append_conf('pg_hba.conf', "local all all trust");
$node->reload;
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'hba_settings',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human'
    ],
    2,
    [
        qr/^Service  *: POSTGRES_HBA_SETTINGS$/m,
        qr/^Returns  *: 2 \(CRITICAL\)$/m,
        qr/^Message  *: 1 critical issue\(s\) found, 0 warning issue\(s\) found$/m,
        qr/^Perfdata *: trust=1 warn=1 crit=1$/m,
    ],
    [ qr/^$/ ],
    'simple issues check'
);

# Add another simple one without error but another critical issue
$node->append_conf('pg_hba.conf', "local all all md5");
$node->reload;
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'hba_settings',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human'
    ],
    2,
    [
        qr/^Service  *: POSTGRES_HBA_SETTINGS$/m,
        qr/^Returns  *: 2 \(CRITICAL\)$/m,
        qr/^Message  *: 2 critical issue\(s\) found, 0 warning issue\(s\) found$/m,
        qr/^Perfdata *: trust=1 warn=1 crit=1$/m,
        qr/^Perfdata *: md5=1 warn=1 crit=1$/m,
    ],
    [ qr/^$/ ],
    'simple issues check'
);

# Add yet another one with an error
$node->append_conf('pg_hba.conf', "host all all all 255.255.255.255 scram-sha-256");
$node->reload;
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'hba_settings',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human'
    ],
    2,
    [
        qr/^Service  *: POSTGRES_HBA_SETTINGS$/m,
        qr/^Returns  *: 2 \(CRITICAL\)$/m,
        qr/^Message  *: 3 critical issue\(s\) found, 0 warning issue\(s\) found$/m,
        qr/^Perfdata *: error=1 warn=1 crit=1$/m,
        qr/^Perfdata *: trust=1 warn=1 crit=1$/m,
        qr/^Perfdata *: md5=1 warn=1 crit=1$/m,
    ],
    [ qr/^$/ ],
    'simple issues check'
);

### End of tests ###

$node->stop( 'immediate' );
