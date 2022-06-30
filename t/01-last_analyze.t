#!/usr/bin/perl
# This program is open source, licensed under the PostgreSQL License.
# For license terms, see the LICENSE file.
#
# Copyright (C) 2012-2022: Open PostgreSQL Monitoring Development Group

use strict;
use warnings;

use lib 't/lib';
use pgNode;
use TestLib ();
use Test::More tests => 33;

my $node = pgNode->get_new_node('prod');

$node->init;
$node->start;

### Begin of tests ###

# failing without thresholds
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'last_analyze',
                          '--username' => getlogin,
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
                          '--username' => getlogin,
                          '--format'   => 'human',
                          '--dbname'   => 'template1',
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

$node->psql('testdb', 'CREATE TABLE foo (bar INT PRIMARY KEY)');

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'last_analyze',
                          '--username' => getlogin,
                          '--format'   => 'human',
                          '--dbname'   => 'testdb',
                          '--warning'  => '1h',
                          '--critical' => '10d'
    ],
    2,
    [ qr/^Service  *: POSTGRES_LAST_ANALYZE$/m,
      qr/^Returns  *: 2 \(CRITICAL\)$/m,
      qr/^Message  *: testdb: Infinity$/m,
      qr/^Perfdata *: testdb=Infinitys warn=3600 crit=864000$/m,
      qr/^Perfdata *: testdb analyze=0$/m,
      qr/^Perfdata *: testdb autoanalyze=0$/m,
    ],
    [ qr/^$/ ],
    'database with one table never analyzed'
);

# test database with two tables, only one never analyzed

$node->psql('testdb', 'CREATE TABLE titi (grosminet INT PRIMARY KEY)');
$node->psql('testdb', 'INSERT INTO titi SELECT generate_series(1,1000)');
$node->psql('testdb', 'ANALYZE titi');

$node->poll_query_until('testdb', q{
    SELECT analyze_count > 0
    FROM pg_catalog.pg_stat_user_tables
    WHERE relname = 'titi'
});

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'last_analyze',
                          '--username' => getlogin,
                          '--format'   => 'human',
                          '--dbname'   => 'testdb',
                          '--warning'  => '1h',
                          '--critical' => '10d'
    ],
    2,
    [ qr/^Service  *: POSTGRES_LAST_ANALYZE$/m,
      qr/^Returns  *: 2 \(CRITICAL\)$/m,
      qr/^Message  *: testdb: Infinity$/m,
      qr/^Perfdata *: testdb=Infinitys warn=3600 crit=864000$/m,
      qr/^Perfdata *: testdb analyze=1$/m,
      qr/^Perfdata *: testdb autoanalyze=0$/m,
    ],
    [ qr/^$/ ],
    'database with two tables, one never analyzed'
);

# test database with two tables, both analyzed

$node->psql('testdb', 'ANALYZE foo');

$node->poll_query_until('testdb', q{
    SELECT sum(analyze_count) = 2
    FROM pg_catalog.pg_stat_user_tables
    WHERE relname IN ('foo', 'titi')
});

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'last_analyze',
                          '--username' => getlogin,
                          '--format'   => 'human',
                          '--dbname'   => 'testdb',
                          '--warning'  => '1h',
                          '--critical' => '10d'
    ],
    0,
    [ qr/^Service  *: POSTGRES_LAST_ANALYZE$/m,
      qr/^Returns  *: 0 \(OK\)$/m,
      qr/^Message  *: 1 database\(s\) checked$/m,
      qr/^Perfdata *: testdb=.*s warn=3600 crit=864000$/m,
      qr/^Perfdata *: testdb analyze=1$/m,
      qr/^Perfdata *: testdb autoanalyze=0$/m,
    ],
    [ qr/^$/ ],
    'test database with two tables, both analyzed'
);

### End of tests ###

$node->stop('fast');
