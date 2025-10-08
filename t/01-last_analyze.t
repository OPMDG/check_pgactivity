#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2023: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't/lib';
use pgNode;
use TestLib ();
use Test::More tests => 33;

my $node = pgNode->get_new_node('prod');
my $pga_data = "$TestLib::tmp_check/pga.data";
my $stdout;
my @stdout;

$node->init;

$node->append_conf('postgresql.conf', 'stats_row_level = on')
    if $node->version < 8.3;

$node->start;

### Beginning of tests ###

# failing without thresholds
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'last_analyze',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human'
    ],
    127,
    [ qr/^$/ ],
    [ qr/^FATAL: you must specify critical and warning thresholds.$/m ],
    'failing without thresholds'
);

TestLib::system_or_bail('createdb',
    '--host' => $node->host,
    '--port' => $node->port,
    'testdb'
);

# test database with no tables

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'last_analyze',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
                          '--status-file' => $pga_data,
                          '--warning'  => '1h',
                          '--critical' => '10d'
    ],
    0,
    [ qr/^Service  *: POSTGRES_LAST_ANALYZE$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: 1 database\(s\) checked$/m,
      qr/^Perfdata *: testdb=NaNs warn=3600 crit=864000$/m,
    ],
    [ qr/^$/ ],
    'database with no tables'
);

# test database with one table never analyzed

# we must track the stat activity on pg_class to make sure there was some stat
# activity to avoid the check_pga shortcut when no activity.
($stdout) = $node->psql('testdb', q{
    SELECT n_tup_ins
    FROM pg_stat_sys_tables
    WHERE relname = 'pg_class'
});

$node->psql('testdb', 'CREATE TABLE foo (bar INT PRIMARY KEY)');

$node->poll_query_until('testdb', qq{
    SELECT n_tup_ins > $stdout
    FROM pg_stat_sys_tables
    WHERE relname = 'pg_class'
});

@stdout = (
    qr/^Service  *: POSTGRES_LAST_ANALYZE$/m,
    qr/^Returns  *: 2 \(CRITICAL\)$/m,
    qr/^Message  *: testdb: Infinity$/m,
    qr/^Perfdata *: testdb=Infinitys warn=3600 crit=864000$/m
);

SKIP: {
    # skip **all** the tests in this files about analyze counts if < 9.1,
    # not just the two following below, so we avoid repeating this SKIP block.
    skip "No analyze counts PgSQL 9.1", 6
        if $node->version < 9.1;

    push @stdout, (
        qr/^Perfdata *: testdb analyze=0$/m,
        qr/^Perfdata *: testdb autoanalyze=0$/m
    );
}

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'last_analyze',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'testdb',
                          '--status-file' => $pga_data,
                          '--warning'  => '1h',
                          '--critical' => '10d'
    ],
    2,
    \@stdout,
    [ qr/^$/ ],
    'database with one table never analyzed'
);

# test database with two tables, only one never analyzed

$node->psql('testdb', 'CREATE TABLE titi (grosminet INT PRIMARY KEY)');
$node->psql('testdb', 'INSERT INTO titi SELECT generate_series(1,1000)');
$node->psql('testdb', 'ANALYZE titi');

$node->poll_query_until('testdb', q{
    SELECT last_analyze IS NOT NULL
    FROM pg_catalog.pg_stat_user_tables
    WHERE relname = 'titi'
});

@stdout = (
    qr/^Service  *: POSTGRES_LAST_ANALYZE$/m,
    qr/^Returns  *: 2 \(CRITICAL\)$/m,
    qr/^Message  *: testdb: Infinity$/m,
    qr/^Perfdata *: testdb=Infinitys warn=3600 crit=864000$/m
);

push @stdout, (
    qr/^Perfdata *: testdb analyze=1$/m,
    qr/^Perfdata *: testdb autoanalyze=0$/m
) if $node->version >= 9.1;

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'last_analyze',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'testdb',
                          '--status-file' => $pga_data,
                          '--warning'  => '1h',
                          '--critical' => '10d'
    ],
    2,
    \@stdout,
    [ qr/^$/ ],
    'database with two tables, one never analyzed'
);

# test database with two tables, both analyzed

$node->psql('testdb', 'ANALYZE foo');

$node->poll_query_until('testdb', q{
    SELECT count(last_analyze) = 2
    FROM pg_catalog.pg_stat_user_tables
    WHERE relname IN ('foo', 'titi')
});

@stdout = (
    qr/^Service  *: POSTGRES_LAST_ANALYZE$/m,
    qr/^Returns  *: 0 \(OK\)$/m,
    qr/^Message  *: 1 database\(s\) checked$/m,
    qr/^Perfdata *: testdb=.*s warn=3600 crit=864000$/m
);

push @stdout, (
    qr/^Perfdata *: testdb analyze=1$/m,
    qr/^Perfdata *: testdb autoanalyze=0$/m
) if $node->version >= 9.1;

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'last_analyze',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--dbname'   => 'testdb',
                          '--status-file' => $pga_data,
                          '--warning'  => '1h',
                          '--critical' => '10d'
    ],
    0,
    \@stdout,
    [ qr/^$/ ],
    'test database with two tables, both analyzed'
);

### End of tests ###

$node->stop( 'immediate' );
