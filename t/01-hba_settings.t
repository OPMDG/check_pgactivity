#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2026: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't/lib';
use pgNode;
use Test::More tests => 42;

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
        qr/^Message  *: 6 trust method\(s\)$/m,
        qr/^Perfdata *: trust=6 warn=1 crit=1$/m,
    ],
    [ qr/^$/ ],
    'simple issues check'
);


# Add a simple one without error but a warning issue (md5)
truncate $node->data_dir . '/' . 'pg_hba.conf', 0;
$node->append_conf('pg_hba.conf', "local all all peer");
$node->append_conf('pg_hba.conf', "local all all md5");
$node->reload;
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'hba_settings',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human'
    ],
    1,
    [
        qr/^Service  *: POSTGRES_HBA_SETTINGS$/m,
        qr/^Returns  *: 1 \(WARNING\)$/m,
        qr/^Message  *: 1 md5 method\(s\)$/m,
        qr/^Perfdata *: md5=1 warn=1 crit=1$/m,
    ],
    [ qr/^$/ ],
    'simple warning-md5 check'
);

# Add another simple one without error but a critical issue (password)
truncate $node->data_dir . '/' . 'pg_hba.conf', 0;
$node->append_conf('pg_hba.conf', "local all all peer");
$node->append_conf('pg_hba.conf', "local all all password");
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
        qr/^Message  *: 1 password method\(s\)$/m,
        qr/^Perfdata *: password=1 warn=1 crit=1$/m,
    ],
    [ qr/^$/ ],
    'simple critical-password check'
);

# Add another simple one without error but a critical issue (trust)
truncate $node->data_dir . '/' . 'pg_hba.conf', 0;
$node->append_conf('pg_hba.conf', "local all all peer");
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
        qr/^Message  *: 1 trust method\(s\)$/m,
        qr/^Perfdata *: trust=1 warn=1 crit=1$/m,
    ],
    [ qr/^$/ ],
    'simple critical-password check'
);

# Add yet another one with an error
truncate $node->data_dir . '/' . 'pg_hba.conf', 0;
$node->append_conf('pg_hba.conf', "local all all peer");
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
        qr/^Message  *: 1 error\(s\)$/m,
        qr/^Perfdata *: error=1 warn=1 crit=1$/m,
    ],
    [ qr/^$/ ],
    'simple issues check'
);

# Let's try a complete one
truncate $node->data_dir . '/' . 'pg_hba.conf', 0;
$node->append_conf('pg_hba.conf', "local all all peer");
$node->append_conf('pg_hba.conf', "local all all trust");
$node->append_conf('pg_hba.conf', "local all all md5");
$node->append_conf('pg_hba.conf', "local all all md5");
$node->append_conf('pg_hba.conf', "local all all password");
$node->append_conf('pg_hba.conf', "local all all trust");
$node->append_conf('pg_hba.conf', "local all all md5");
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
        qr/^Message  *: 1 error\(s\)$/m,
        qr/^Message  *: 3 md5 method\(s\)$/m,
        qr/^Message  *: 1 password method\(s\)$/m,
        qr/^Message  *: 2 trust method\(s\)$/m,
        qr/^Perfdata *: error=1 warn=1 crit=1$/m,
        qr/^Perfdata *: md5=3 warn=1 crit=1$/m,
        qr/^Perfdata *: password=1 warn=1 crit=1$/m,
        qr/^Perfdata *: trust=2 warn=1 crit=1$/m,
    ],
    [ qr/^$/ ],
    'complete example'
);

### End of tests ###

$node->stop( 'immediate' );
