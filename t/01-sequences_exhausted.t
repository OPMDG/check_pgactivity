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
use TestLib ();
use IPC::Run ();
use Test::More;
use PostgresVersion;

my $node = pgNode->get_new_node('prod');
my @timer;
my @in;
my @out;
my @procs;
my $pgversion;

$node->init;
$node->start;

### Beginning of tests ###


# basic check without thresholds
$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'sequences_exhausted',
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

$node->psql('testdb', 'CREATE TABLE test1 (i smallint PRIMARY KEY)');
$node->psql('testdb', 'CREATE SEQUENCE test1seq INCREMENT BY 8000 START WITH 16000 OWNED BY test1.i');

# As the sequence is new, set its first value
$node->psql('testdb', "SELECT nextval('test1seq')") ;

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'sequences_exhausted',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--warning'  => '50%',
                          '--critical' => '90%'
    ],
    0,
    [
        qr/^Service *: POSTGRES_CHECK_SEQ_EXHAUSTED$/m,
        qr/^Returns *: 0 \(OK\)$/m,
    ],
    [ qr/^$/ ],
    'basic check'
);

$node->psql('testdb', "SELECT nextval('test1seq')");

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'sequences_exhausted',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--warning'  => '50%',
                          '--critical' => '90%'
    ],
    1,
    [
        qr/^Service *: POSTGRES_CHECK_SEQ_EXHAUSTED$/m,
        qr/^Returns *: 1 \(WARNING\)$/m,
    ],
    [ qr/^$/ ],
    'check warning'
);

$node->psql('testdb', "SELECT nextval('test1seq')");

$node->command_checks_all( [
    './check_pgactivity', '--service'  => 'sequences_exhausted',
                          '--username' => $ENV{'USER'} || 'postgres',
                          '--format'   => 'human',
                          '--warning'  => '50%',
                          '--critical' => '90%'
    ],
    2,
    [
        qr/^Service *: POSTGRES_CHECK_SEQ_EXHAUSTED$/m,
        qr/^Returns *: 2 \(CRITICAL\)$/m,
    ],
    [ qr/^$/ ],
    'check critical'
);

# stop immediate to kill any remaining backends
$node->stop('immediate');

done_testing();

