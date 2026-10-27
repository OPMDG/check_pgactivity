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
use Test::More tests => 53;

my $node = pgNode->get_new_node('prod');
my $pga_data = "$TestLib::tmp_check/pga.data";

$node->init;
$node->start;

### Beginning of tests ###

# First check. Returns no perfdata
$node->command_checks_all( [
    './check_pgactivity', '--service'     => 'settings',
                          '--username'    => $ENV{'USER'} || 'postgres',
                          '--format'      => 'human',
                          '--status-file' => $pga_data,
    ],
    0,
    [
      qr/^Service  *: POSTGRES_SETTINGS$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: Setting saved$/m,
    ],
    [ qr/^$/ ],
    'first basic check'
);

# Second check. Returns OK
$node->command_checks_all( [
    './check_pgactivity', '--service'     => 'settings',
                          '--username'    => $ENV{'USER'} || 'postgres',
                          '--format'      => 'human',
                          '--status-file' => $pga_data,
    ],
    0,
    [
      qr/^Service  *: POSTGRES_SETTINGS$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: Setting OK$/m,
    ],
    [ qr/^$/ ],
    'second basic check'
);

SKIP: {
    skip "checking with non superuser role is not supported before v10", 5
        if $node->version < 10;

    # Check with a non superuser role with the documented privileges
    $node->safe_psql('postgres', 'CREATE ROLE check_pga LOGIN');
    $node->safe_psql('postgres', 'GRANT pg_monitor TO check_pga');
    $node->safe_psql('postgres', 'GRANT SELECT ON pg_file_settings TO check_pga');
    $node->safe_psql('postgres',
        'GRANT EXECUTE ON FUNCTION pg_show_all_file_settings() TO check_pga');

    $node->command_checks_all( [
        './check_pgactivity', '--service'     => 'settings',
                              '--username'    => 'check_pga',
                              '--dbname'      => 'postgres',
                              '--format'      => 'human',
                              '--status-file' => "$pga_data.check_pga",
        ],
        0,
        [
          qr/^Service  *: POSTGRES_SETTINGS$/m,
          qr/^Returns  *: 0 \(OK\)$/m,
          qr/^Message  *: Setting saved$/m,
        ],
        [ qr/^$/ ],
        'check with a non superuser role'
    );
}

$node->append_conf('postgresql.conf', "work_mem=ahah");

SKIP: {
    skip "testing incompatibility with PostgreSQL 9.4 and before", 38
        unless $node->version >= 9.5;

    # Third check. Returns CRITICAL
    $node->command_checks_all( [
        './check_pgactivity', '--service'     => 'settings',
                              '--username'    => $ENV{'USER'} || 'postgres',
                              '--format'      => 'human',
                              '--status-file' => $pga_data,
        ],
        2,
        [
          qr/^Service  *: POSTGRES_SETTINGS$/m,
          qr/^Returns  *: 2 \(CRITICAL\)$/m,
          qr/^Message  *: Setting in error!$/m,
          qr/^Long message  *: work_mem: setting could not be applied in .*, line \d+$/m,
        ],
        [ qr/^$/ ],
        'third basic check with an error'
    );

    # Fix work_mem and save the settings
    $node->append_conf('postgresql.conf', "work_mem=4MB");
    $node->reload;

    $node->command_checks_all( [
        './check_pgactivity', '--service'     => 'settings',
                              '--username'    => $ENV{'USER'} || 'postgres',
                              '--format'      => 'human',
                              '--status-file' => $pga_data,
                              '--save',
        ],
        0,
        [
          qr/^Service  *: POSTGRES_SETTINGS$/m,
          qr/^Returns  *: 0 \(OK\)$/m,
          qr/^Message  *: Setting saved$/m,
        ],
        [ qr/^$/ ],
        'save settings after fixing the error'
    );

    # A postmaster parameter changed in postgresql.conf and reloaded is only
    # pending restart: returns WARNING, not CRITICAL
    $node->append_conf('postgresql.conf', "max_connections=20");
    $node->reload;
    $node->poll_query_until('postgres', q{
        SELECT pending_restart FROM pg_settings WHERE name = 'max_connections'
    });

    $node->command_checks_all( [
        './check_pgactivity', '--service'     => 'settings',
                              '--username'    => $ENV{'USER'} || 'postgres',
                              '--format'      => 'human',
                              '--status-file' => $pga_data,
        ],
        1,
        [
          qr/^Service  *: POSTGRES_SETTINGS$/m,
          qr/^Returns  *: 1 \(WARNING\)$/m,
          qr/^Message  *: Setting changed and pending restart!$/m,
          qr/^Long message  *: max_connections is pending restart!$/m,
          qr/\A(?!.*could not be applied)/s,
        ],
        [ qr/^$/ ],
        'pending restart from postgresql.conf'
    );

    # An invalid value is still an error while a parameter is pending restart
    $node->append_conf('postgresql.conf', "work_mem=ahah");
    $node->reload;

    $node->command_checks_all( [
        './check_pgactivity', '--service'     => 'settings',
                              '--username'    => $ENV{'USER'} || 'postgres',
                              '--format'      => 'human',
                              '--status-file' => $pga_data,
        ],
        2,
        [
          qr/^Service  *: POSTGRES_SETTINGS$/m,
          qr/^Returns  *: 2 \(CRITICAL\)$/m,
          qr/^Message  *: Setting in error!$/m,
          qr/^Long message  *: max_connections is pending restart!$/m,
          qr/^Long message  *: work_mem: setting could not be applied in .*, line \d+$/m,
          qr/\A(?!.*max_connections: setting could not be applied)/s,
        ],
        [ qr/^$/ ],
        'error while a parameter is pending restart'
    );

    # Fix work_mem, restart and save the settings
    $node->append_conf('postgresql.conf', "work_mem=4MB");
    $node->restart;

    $node->command_checks_all( [
        './check_pgactivity', '--service'     => 'settings',
                              '--username'    => $ENV{'USER'} || 'postgres',
                              '--format'      => 'human',
                              '--status-file' => $pga_data,
                              '--save',
        ],
        0,
        [
          qr/^Service  *: POSTGRES_SETTINGS$/m,
          qr/^Returns  *: 0 \(OK\)$/m,
          qr/^Message  *: Setting saved$/m,
        ],
        [ qr/^$/ ],
        'save settings after restart'
    );

    # Same with ALTER SYSTEM
    $node->safe_psql('postgres', 'ALTER SYSTEM SET max_connections = 30');
    $node->reload;
    $node->poll_query_until('postgres', q{
        SELECT pending_restart FROM pg_settings WHERE name = 'max_connections'
    });

    $node->command_checks_all( [
        './check_pgactivity', '--service'     => 'settings',
                              '--username'    => $ENV{'USER'} || 'postgres',
                              '--format'      => 'human',
                              '--status-file' => $pga_data,
        ],
        1,
        [
          qr/^Service  *: POSTGRES_SETTINGS$/m,
          qr/^Returns  *: 1 \(WARNING\)$/m,
          qr/^Message  *: Setting changed and pending restart!$/m,
          qr/^Long message  *: max_connections is pending restart!$/m,
          qr/\A(?!.*could not be applied)/s,
        ],
        [ qr/^$/ ],
        'pending restart from ALTER SYSTEM'
    );
}

### End of tests ###

# stop immediate to kill any remaining backends
$node->stop('immediate');
